import Foundation

struct ResourceStructuredLogFieldSampleSearch: Equatable, Sendable {
    let value: String
    let query: String
}

struct ResourceStructuredLogField: Equatable, Sendable {
    let key: String
    let title: String
    let nonEmptyCount: Int
    let sampleValues: [String]
    let sampleSearchQueries: [ResourceStructuredLogFieldSampleSearch]

    func sampleSearchQuery(for value: String) -> String? {
        sampleSearchQueries.first { $0.value == value }?.query
    }
}

struct ResourceDuplicateLogLine: Equatable, Sendable {
    let fingerprint: String
    let count: Int
    let sampleLine: String
}

struct ResourceStructuredLogSummary: Equatable, Sendable {
    let totalLineCount: Int
    let jsonLineCount: Int
    let isStructured: Bool
    let fields: [ResourceStructuredLogField]
    let duplicateLines: [ResourceDuplicateLogLine]

    var structuredRatio: Double {
        guard totalLineCount > 0 else { return 0 }
        return Double(jsonLineCount) / Double(totalLineCount)
    }

    func field(_ key: String) -> ResourceStructuredLogField? {
        fields.first { $0.key == key }
    }
}

enum ResourceStructuredLogFieldSearch {
    static func query(field: ResourceStructuredLogField, value: String) -> String {
        field.sampleSearchQuery(for: value) ?? query(fieldKey: field.key, value: value)
    }

    static func query(fieldKey: String, value: String) -> String {
        let escapedKey = fieldKey.replacingOccurrences(of: "\"", with: "\\\"")
        let escapedValue = value.replacingOccurrences(of: "\"", with: "\\\"")
        return "\"\(escapedKey)\":\"\(escapedValue)\""
    }
}

enum ResourceStructuredLogAnalyzer {
    private static let sampleLineLimit = 200
    private static let structuredSampleThreshold = 0.6
    private static let sampleValueLimit = 4

    private static let fieldDefinitions: [(key: String, title: String, aliases: [String])] = [
        ("timestamp", "Timestamp", ["timestamp", "time", "ts", "@timestamp"]),
        ("level", "Level", ["level", "severity", "log_level", "logLevel"]),
        ("message", "Message", ["message", "msg", "log", "body"]),
        ("pod", "Pod", ["pod", "pod_name", "podName", "podName"]),
        ("container", "Container", ["container", "container_name", "containerName"]),
        ("requestID", "Request ID", ["requestID", "requestId", "request_id", "req_id", "correlationId", "correlation_id"]),
        ("traceID", "Trace ID", ["traceID", "traceId", "trace_id", "trace", "spanTraceId"]),
        ("namespace", "Namespace", ["namespace", "namespace_name", "namespaceName"])
    ]
    private static let keyFingerprintIndexes: [UInt64: Int] = {
        var indexes: [UInt64: Int] = [:]
        for (index, definition) in fieldDefinitions.enumerated() {
            for alias in definition.aliases {
                indexes[normalizedKeyHash(alias.utf8)] = index
            }
        }
        return indexes
    }()

    static func analyze(text: String, duplicateLimit: Int = 8) -> ResourceStructuredLogSummary {
        let bytes = Array(text.utf8)
        guard !bytes.isEmpty else {
            return ResourceStructuredLogSummary(
                totalLineCount: 0,
                jsonLineCount: 0,
                isStructured: false,
                fields: [],
                duplicateLines: []
            )
        }

        guard looksStructured(bytes: bytes) else {
            let lines = logLines(in: text)
            return ResourceStructuredLogSummary(
                totalLineCount: lines.count,
                jsonLineCount: 0,
                isStructured: false,
                fields: [],
                duplicateLines: duplicateLines(in: lines, duplicateLimit: duplicateLimit)
            )
        }

        return analyzeStructuredBytes(bytes, duplicateLimit: duplicateLimit)
    }

    private static func looksStructured(bytes: [UInt8]) -> Bool {
        var sampled = 0
        var jsonCandidates = 0
        forEachLineRange(in: bytes) { start, end, shouldStop in
            guard !isBlank(bytes, start: start, end: end) else { return }
            sampled += 1
            if jsonRange(in: bytes, start: start, end: end) != nil {
                jsonCandidates += 1
            }
            if sampled == sampleLineLimit {
                shouldStop = true
            }
        }
        guard sampled > 0 else { return false }
        return Double(jsonCandidates) / Double(sampled) >= structuredSampleThreshold
    }

    private static func analyzeStructuredBytes(_ bytes: [UInt8], duplicateLimit: Int) -> ResourceStructuredLogSummary {
        var totalLineCount = 0
        var jsonLineCount = 0
        var fieldCounts = Array(repeating: 0, count: fieldDefinitions.count)
        var fieldSamples = Array(repeating: [String](), count: fieldDefinitions.count)
        var fieldSampleQueries = Array(repeating: [ResourceStructuredLogFieldSampleSearch](), count: fieldDefinitions.count)
        var duplicateCounts: [UInt64: (count: Int, sampleLine: String, fingerprint: String)] = [:]

        forEachLineRange(in: bytes) { lineStart, lineEnd, _ in
            totalLineCount += 1
            guard let jsonRange = jsonRange(in: bytes, start: lineStart, end: lineEnd) else { return }
            jsonLineCount += 1

            var levelRange: Range<Int>?
            var messageRange: Range<Int>?
            scanJSONFields(in: bytes, start: jsonRange.lowerBound, end: jsonRange.upperBound) { fieldIndex, keyStart, keyEnd, valueStart, valueEnd in
                fieldCounts[fieldIndex] += 1
                if fieldSamples[fieldIndex].count < sampleValueLimit {
                    let value = String(decoding: bytes[valueStart..<valueEnd], as: UTF8.self)
                    guard !value.isEmpty else { return }
                    if !fieldSamples[fieldIndex].contains(value) {
                        fieldSamples[fieldIndex].append(value)
                        let key = String(decoding: bytes[keyStart..<keyEnd], as: UTF8.self)
                        fieldSampleQueries[fieldIndex].append(
                            ResourceStructuredLogFieldSampleSearch(
                                value: value,
                                query: ResourceStructuredLogFieldSearch.query(fieldKey: key, value: value)
                            )
                        )
                    }
                }
                if fieldIndex == 1 {
                    levelRange = valueStart..<valueEnd
                } else if fieldIndex == 2 {
                    messageRange = valueStart..<valueEnd
                }
            }

            let fingerprintHash: UInt64
            let fingerprintRanges: (level: Range<Int>?, message: Range<Int>)?
            let fallbackFingerprintRange: Range<Int>?
            if let messageRange {
                fingerprintHash = structuredFingerprintHash(
                    bytes: bytes,
                    levelRange: levelRange,
                    messageRange: messageRange
                )
                fingerprintRanges = (levelRange, messageRange)
                fallbackFingerprintRange = nil
            } else {
                let trimmed = trimASCIIWhitespace(bytes, start: lineStart, end: lineEnd)
                guard !trimmed.isEmpty else { return }
                fingerprintHash = hashBytes(bytes, range: trimmed)
                fingerprintRanges = nil
                fallbackFingerprintRange = trimmed
            }

            if var duplicate = duplicateCounts[fingerprintHash] {
                duplicate.count += 1
                duplicateCounts[fingerprintHash] = duplicate
            } else {
                let fingerprint: String
                if let fingerprintRanges {
                    if let levelRange = fingerprintRanges.level, !levelRange.isEmpty {
                        fingerprint = String(decoding: bytes[levelRange], as: UTF8.self)
                            + "|"
                            + String(decoding: bytes[fingerprintRanges.message], as: UTF8.self)
                    } else {
                        fingerprint = String(decoding: bytes[fingerprintRanges.message], as: UTF8.self)
                    }
                } else if let fallbackFingerprintRange {
                    fingerprint = String(decoding: bytes[fallbackFingerprintRange], as: UTF8.self)
                } else {
                    fingerprint = ""
                }
                guard !fingerprint.isEmpty else { return }
                duplicateCounts[fingerprintHash] = (
                    count: 1,
                    sampleLine: String(decoding: bytes[lineStart..<lineEnd], as: UTF8.self),
                    fingerprint: fingerprint
                )
            }
        }

        let fields = fieldDefinitions.enumerated().compactMap { index, definition -> ResourceStructuredLogField? in
            let count = fieldCounts[index]
            guard count > 0 else { return nil }
            return ResourceStructuredLogField(
                key: definition.key,
                title: definition.title,
                nonEmptyCount: count,
                sampleValues: fieldSamples[index],
                sampleSearchQueries: fieldSampleQueries[index]
            )
        }

        let duplicates = duplicateCounts
            .filter { $0.value.count > 1 }
            .sorted {
                if $0.value.count != $1.value.count {
                    return $0.value.count > $1.value.count
                }
                return $0.key < $1.key
            }
            .prefix(max(0, duplicateLimit))
            .map {
                ResourceDuplicateLogLine(
                    fingerprint: $0.value.fingerprint,
                    count: $0.value.count,
                    sampleLine: $0.value.sampleLine
                )
            }

        return ResourceStructuredLogSummary(
            totalLineCount: totalLineCount,
            jsonLineCount: jsonLineCount,
            isStructured: Double(jsonLineCount) / Double(max(1, totalLineCount)) >= structuredSampleThreshold,
            fields: fields,
            duplicateLines: duplicates
        )
    }

    private static func logLines(in text: String) -> [String] {
        var lines: [String] = []
        lines.reserveCapacity(max(1, text.utf8.count / 96))
        text.enumerateLines { line, _ in
            lines.append(line)
        }
        return lines
    }

    private static func forEachLineRange(
        in bytes: [UInt8],
        _ body: (_ start: Int, _ end: Int, _ shouldStop: inout Bool) -> Void
    ) {
        var start = 0
        var index = 0
        var shouldStop = false
        while index < bytes.count, !shouldStop {
            if bytes[index] == UInt8(ascii: "\n") {
                let end = index > start && bytes[index - 1] == UInt8(ascii: "\r") ? index - 1 : index
                body(start, end, &shouldStop)
                start = index + 1
            }
            index += 1
        }
        if start < bytes.count, !shouldStop {
            body(start, bytes.count, &shouldStop)
        }
    }

    private static func lineRanges(in bytes: [UInt8]) -> [(start: Int, end: Int)] {
        var ranges: [(Int, Int)] = []
        ranges.reserveCapacity(max(1, bytes.count / 96))
        var start = 0
        var index = 0
        while index < bytes.count {
            if bytes[index] == UInt8(ascii: "\n") {
                let end = index > start && bytes[index - 1] == UInt8(ascii: "\r") ? index - 1 : index
                ranges.append((start, end))
                start = index + 1
            }
            index += 1
        }
        if start < bytes.count {
            ranges.append((start, bytes.count))
        }
        return ranges
    }

    private static func jsonObjectRange(in line: String) -> Range<String.Index>? {
        guard let start = line.firstIndex(of: "{"),
              let end = line.lastIndex(of: "}"),
              start <= end else {
            return nil
        }
        return start..<line.index(after: end)
    }

    private static func jsonRange(in bytes: [UInt8], start: Int, end: Int) -> Range<Int>? {
        var objectStart: Int?
        var objectEnd: Int?
        var cursor = start
        while cursor < end {
            if bytes[cursor] == UInt8(ascii: "{"), objectStart == nil {
                objectStart = cursor
            }
            if bytes[cursor] == UInt8(ascii: "}") {
                objectEnd = cursor + 1
            }
            cursor += 1
        }
        guard let objectStart, let objectEnd, objectStart < objectEnd else { return nil }
        return objectStart..<objectEnd
    }

    private static func isBlank(_ bytes: [UInt8], start: Int, end: Int) -> Bool {
        guard start < end else { return true }
        for index in start..<end {
            switch bytes[index] {
            case UInt8(ascii: " "), UInt8(ascii: "\t"), UInt8(ascii: "\r"):
                continue
            default:
                return false
            }
        }
        return true
    }

    private static func scanJSONFields(
        in bytes: [UInt8],
        start: Int,
        end: Int,
        onField: (Int, Int, Int, Int, Int) -> Void
    ) {
        var cursor = start
        while cursor < end {
            guard bytes[cursor] == UInt8(ascii: "\"") else {
                cursor += 1
                continue
            }

            let keyStart = cursor + 1
            guard let keyEnd = quotedStringEnd(in: bytes, start: keyStart, end: end) else { return }
            cursor = keyEnd + 1
            skipWhitespace(in: bytes, cursor: &cursor, end: end)
            guard cursor < end, bytes[cursor] == UInt8(ascii: ":") else { continue }
            cursor += 1
            skipWhitespace(in: bytes, cursor: &cursor, end: end)
            guard cursor < end else { return }

            let fieldIndex = fieldIndex(forKeyBytes: bytes, start: keyStart, end: keyEnd)
            if bytes[cursor] == UInt8(ascii: "\"") {
                let valueStart = cursor + 1
                guard let valueEnd = quotedStringEnd(in: bytes, start: valueStart, end: end) else { return }
                if let fieldIndex {
                    onField(fieldIndex, keyStart, keyEnd, valueStart, valueEnd)
                }
                cursor = valueEnd + 1
            } else if bytes[cursor] == UInt8(ascii: "{") || bytes[cursor] == UInt8(ascii: "[") {
                cursor += 1
            } else {
                let valueStart = cursor
                while cursor < end,
                      bytes[cursor] != UInt8(ascii: ","),
                      bytes[cursor] != UInt8(ascii: "}") {
                    cursor += 1
                }
                let trimmed = trimASCIIWhitespace(bytes, start: valueStart, end: cursor)
                if let fieldIndex, !trimmed.isEmpty {
                    onField(fieldIndex, keyStart, keyEnd, trimmed.lowerBound, trimmed.upperBound)
                }
            }
        }
    }

    private static func duplicateLines(in lines: [String], duplicateLimit: Int) -> [ResourceDuplicateLogLine] {
        var counts: [String: (count: Int, sampleLine: String)] = [:]
        for line in lines {
            let fingerprint = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !fingerprint.isEmpty else { continue }
            var entry = counts[fingerprint, default: (0, line)]
            entry.count += 1
            counts[fingerprint] = entry
        }
        return counts
            .filter { $0.value.count > 1 }
            .sorted {
                if $0.value.count != $1.value.count {
                    return $0.value.count > $1.value.count
                }
                return $0.key < $1.key
            }
            .prefix(max(0, duplicateLimit))
            .map {
                ResourceDuplicateLogLine(
                    fingerprint: $0.key,
                    count: $0.value.count,
                    sampleLine: $0.value.sampleLine
                )
            }
    }

    private static func quotedStringEnd(in bytes: [UInt8], start: Int, end: Int) -> Int? {
        var cursor = start
        var isEscaped = false
        while cursor < end {
            let byte = bytes[cursor]
            if isEscaped {
                isEscaped = false
            } else if byte == UInt8(ascii: "\\") {
                isEscaped = true
            } else if byte == UInt8(ascii: "\"") {
                return cursor
            }
            cursor += 1
        }
        return nil
    }

    private static func skipWhitespace(in bytes: [UInt8], cursor: inout Int, end: Int) {
        while cursor < end {
            switch bytes[cursor] {
            case UInt8(ascii: " "), UInt8(ascii: "\t"), UInt8(ascii: "\n"), UInt8(ascii: "\r"):
                cursor += 1
            default:
                return
            }
        }
    }

    private static func trimASCIIWhitespace(_ bytes: [UInt8], start initialStart: Int, end initialEnd: Int) -> Range<Int> {
        var start = initialStart
        var end = initialEnd
        while start < end, isASCIIWhitespace(bytes[start]) {
            start += 1
        }
        while end > start {
            let previous = end - 1
            guard isASCIIWhitespace(bytes[previous]) else { break }
            end = previous
        }
        return start..<end
    }

    private static func isASCIIWhitespace(_ byte: UInt8) -> Bool {
        byte == UInt8(ascii: " ")
            || byte == UInt8(ascii: "\t")
            || byte == UInt8(ascii: "\n")
            || byte == UInt8(ascii: "\r")
    }

    private static func fieldIndex(forKeyBytes bytes: [UInt8], start: Int, end: Int) -> Int? {
        keyFingerprintIndexes[normalizedKeyHash(bytes, start: start, end: end)]
    }

    private static func isASCIIAlphaNumeric(_ byte: UInt8) -> Bool {
        (byte >= UInt8(ascii: "a") && byte <= UInt8(ascii: "z"))
            || (byte >= UInt8(ascii: "A") && byte <= UInt8(ascii: "Z"))
            || (byte >= UInt8(ascii: "0") && byte <= UInt8(ascii: "9"))
    }

    private static func normalizedKeyHash<S: Sequence>(_ bytes: S) -> UInt64 where S.Element == UInt8 {
        var hash = fnvOffsetBasis
        var count: UInt64 = 0
        for byte in bytes where isASCIIAlphaNumeric(byte) {
            let normalizedByte = byte >= UInt8(ascii: "A") && byte <= UInt8(ascii: "Z") ? byte + 32 : byte
            hashByte(normalizedByte, into: &hash)
            count += 1
        }
        hash ^= count &* 0x9E37_79B9_7F4A_7C15
        return hash
    }

    private static func normalizedKeyHash(_ bytes: [UInt8], start: Int, end: Int) -> UInt64 {
        var hash = fnvOffsetBasis
        var count: UInt64 = 0
        var index = start
        while index < end {
            let byte = bytes[index]
            if isASCIIAlphaNumeric(byte) {
                let normalizedByte = byte >= UInt8(ascii: "A") && byte <= UInt8(ascii: "Z") ? byte + 32 : byte
                hashByte(normalizedByte, into: &hash)
                count += 1
            }
            index += 1
        }
        hash ^= count &* 0x9E37_79B9_7F4A_7C15
        return hash
    }

    private static func structuredFingerprintHash(
        bytes: [UInt8],
        levelRange: Range<Int>?,
        messageRange: Range<Int>
    ) -> UInt64 {
        var hash = fnvOffsetBasis
        if let levelRange, !levelRange.isEmpty {
            hashBytes(bytes, range: levelRange, into: &hash)
            hashByte(UInt8(ascii: "|"), into: &hash)
        }
        hashBytes(bytes, range: messageRange, into: &hash)
        return hash
    }

    private static func hashBytes(_ bytes: [UInt8], range: Range<Int>) -> UInt64 {
        var hash = fnvOffsetBasis
        hashBytes(bytes, range: range, into: &hash)
        return hash
    }

    private static func hashBytes(_ bytes: [UInt8], range: Range<Int>, into hash: inout UInt64) {
        var index = range.lowerBound
        while index < range.upperBound {
            hashByte(bytes[index], into: &hash)
            index += 1
        }
    }

    private static func hashByte(_ byte: UInt8, into hash: inout UInt64) {
        hash ^= UInt64(byte)
        hash &*= fnvPrime
    }

    private static let fnvOffsetBasis: UInt64 = 14_695_981_039_346_656_037
    private static let fnvPrime: UInt64 = 1_099_511_628_211
}
