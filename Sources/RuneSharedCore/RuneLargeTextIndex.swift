import Foundation

public struct RuneLargeTextLine: Equatable, Sendable {
    public let number: Int
    public let range: NSRange
    public let contentRange: NSRange
    public let text: String

    public init(number: Int, range: NSRange, contentRange: NSRange, text: String) {
        self.number = number
        self.range = range
        self.contentRange = contentRange
        self.text = text
    }
}

public struct RuneLargeTextViewport: Equatable, Sendable {
    public let startLine: Int
    public let lineLimit: Int
    public let totalLineCount: Int
    public let lines: [RuneLargeTextLine]

    public init(startLine: Int, lineLimit: Int, totalLineCount: Int, lines: [RuneLargeTextLine]) {
        self.startLine = startLine
        self.lineLimit = lineLimit
        self.totalLineCount = totalLineCount
        self.lines = lines
    }
}

public struct RuneLargeTextMatch: Equatable, Sendable {
    public let index: Int
    public let range: NSRange
    public let lineNumber: Int

    public init(index: Int, range: NSRange, lineNumber: Int) {
        self.index = index
        self.range = range
        self.lineNumber = lineNumber
    }
}

public struct RuneLargeTextSearchResult: Equatable, Sendable {
    public let query: String
    public let totalLineCount: Int
    public let matches: [RuneLargeTextMatch]

    public init(query: String, totalLineCount: Int, matches: [RuneLargeTextMatch]) {
        self.query = query
        self.totalLineCount = totalLineCount
        self.matches = matches
    }

    public var matchingLineCount: Int {
        matches.count
    }

    public func clampedMatchIndex(_ index: Int) -> Int {
        guard !matches.isEmpty else { return 0 }
        return min(max(index, 0), matches.count - 1)
    }

    public func nextMatchIndex(from index: Int) -> Int {
        guard !matches.isEmpty else { return 0 }
        return (clampedMatchIndex(index) + 1) % matches.count
    }

    public func previousMatchIndex(from index: Int) -> Int {
        guard !matches.isEmpty else { return 0 }
        return (clampedMatchIndex(index) - 1 + matches.count) % matches.count
    }
}

public struct RuneLargeTextIndex: Equatable, Sendable {
    public let text: String
    public let lineStartUTF16Offsets: [Int]
    public let lineContentEndUTF16Offsets: [Int]
    public let utf16Length: Int

    public init(text: String) {
        self.text = text
        let nsText = text as NSString
        self.utf16Length = nsText.length
        let indexed = Self.buildLineIndex(nsText)
        self.lineStartUTF16Offsets = indexed.starts
        self.lineContentEndUTF16Offsets = indexed.contentEnds
    }

    public var lineCount: Int {
        lineStartUTF16Offsets.count
    }

    public func line(number oneBasedLineNumber: Int) -> RuneLargeTextLine? {
        guard oneBasedLineNumber >= 1, oneBasedLineNumber <= lineCount else { return nil }
        let lineIndex = oneBasedLineNumber - 1
        let start = lineStartUTF16Offsets[lineIndex]
        let nextStart = lineIndex + 1 < lineStartUTF16Offsets.count
            ? lineStartUTF16Offsets[lineIndex + 1]
            : utf16Length
        let contentEnd = lineContentEndUTF16Offsets[lineIndex]
        let fullRange = NSRange(location: start, length: max(0, nextStart - start))
        let contentRange = NSRange(location: start, length: max(0, contentEnd - start))
        let nsText = text as NSString
        return RuneLargeTextLine(
            number: oneBasedLineNumber,
            range: fullRange,
            contentRange: contentRange,
            text: nsText.substring(with: contentRange)
        )
    }

    public func viewport(startLine: Int, lineLimit: Int) -> RuneLargeTextViewport {
        guard lineLimit > 0, lineCount > 0 else {
            return RuneLargeTextViewport(
                startLine: max(1, startLine),
                lineLimit: max(0, lineLimit),
                totalLineCount: lineCount,
                lines: []
            )
        }

        let clampedStart = min(max(1, startLine), lineCount)
        let endLine = min(lineCount, clampedStart + lineLimit - 1)
        let lines = (clampedStart...endLine).compactMap { line(number: $0) }
        return RuneLargeTextViewport(
            startLine: clampedStart,
            lineLimit: lineLimit,
            totalLineCount: lineCount,
            lines: lines
        )
    }

    public func lineNumber(containingUTF16Location location: Int) -> Int {
        guard !lineStartUTF16Offsets.isEmpty else { return 0 }
        let clampedLocation = min(max(0, location), utf16Length)
        var lower = 0
        var upper = lineStartUTF16Offsets.count
        while lower < upper {
            let middle = lower + (upper - lower) / 2
            if lineStartUTF16Offsets[middle] <= clampedLocation {
                lower = middle + 1
            } else {
                upper = middle
            }
        }
        return max(1, lower)
    }

    public func search(
        query: String,
        options: NSString.CompareOptions = [.caseInsensitive, .diacriticInsensitive]
    ) -> RuneLargeTextSearchResult {
        let normalizedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedQuery.isEmpty, utf16Length > 0 else {
            return RuneLargeTextSearchResult(query: "", totalLineCount: lineCount, matches: [])
        }

        let nsText = text as NSString
        var matches: [RuneLargeTextMatch] = []
        matches.reserveCapacity(min(utf16Length / max(1, normalizedQuery.utf16.count * 8), 4_096))
        var searchLocation = 0
        while searchLocation < utf16Length {
            let searchRange = NSRange(location: searchLocation, length: utf16Length - searchLocation)
            let matchRange = nsText.range(of: normalizedQuery, options: options, range: searchRange)
            guard matchRange.location != NSNotFound else { break }

            matches.append(RuneLargeTextMatch(
                index: matches.count,
                range: matchRange,
                lineNumber: lineNumber(containingUTF16Location: matchRange.location)
            ))
            searchLocation = max(matchRange.location + max(matchRange.length, 1), matchRange.location + 1)
        }

        return RuneLargeTextSearchResult(
            query: normalizedQuery,
            totalLineCount: lineCount,
            matches: matches
        )
    }

    private static func buildLineIndex(_ nsText: NSString) -> (starts: [Int], contentEnds: [Int]) {
        guard nsText.length > 0 else { return ([], []) }

        var starts: [Int] = []
        var contentEnds: [Int] = []
        let estimatedLineCount = min(max(1, nsText.length / 80), 50_000)
        starts.reserveCapacity(estimatedLineCount)
        contentEnds.reserveCapacity(estimatedLineCount)

        starts.append(0)
        var location = 0
        while location < nsText.length {
            let lineRange = nsText.lineRange(for: NSRange(location: location, length: 0))
            let lineEnd = NSMaxRange(lineRange)
            contentEnds.append(contentEnd(in: nsText, lineRange: lineRange))

            location = lineEnd
            if location < nsText.length {
                starts.append(location)
            } else if lineRange.length > 0, lineEndsWithNewline(nsText, lineRange: lineRange) {
                starts.append(nsText.length)
                contentEnds.append(nsText.length)
            }
        }

        return (starts, contentEnds)
    }

    private static func contentEnd(in nsText: NSString, lineRange: NSRange) -> Int {
        var end = NSMaxRange(lineRange)
        guard end > lineRange.location else { return end }
        let last = nsText.character(at: end - 1)
        if last == 10 {
            end -= 1
            if end > lineRange.location, nsText.character(at: end - 1) == 13 {
                end -= 1
            }
        } else if last == 13 {
            end -= 1
        }
        return end
    }

    private static func lineEndsWithNewline(_ nsText: NSString, lineRange: NSRange) -> Bool {
        guard lineRange.length > 0 else { return false }
        let last = nsText.character(at: NSMaxRange(lineRange) - 1)
        return last == 10 || last == 13
    }
}
