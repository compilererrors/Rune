import Foundation
import RuneCore

enum YAMLHighlightKind {
    case key
    case string
    case number
    case boolean
    case comment
    case directive
    case anchor
    case alias
}

struct YAMLHighlightSpan {
    let range: NSRange
    let kind: YAMLHighlightKind
}

struct YAMLTextAnalysis {
    let highlights: [YAMLHighlightSpan]
    let validationIssues: [YAMLValidationIssue]
}

enum YAMLLanguageService {
    /// Full analysis is cheap for normal Kubernetes manifests. Above this size the editor should
    /// render the visible area first and let debounced validation catch up outside the keystroke path.
    static let interactiveFullAnalysisCharacterLimit = 90_000
    static let interactiveFullAnalysisLineLimit = 2_500

    static func analyze(_ source: String) -> YAMLTextAnalysis {
        cache.analysis(for: source)
    }

    static func prefersViewportAnalysis(_ source: String) -> Bool {
        if source.utf16.count > interactiveFullAnalysisCharacterLimit {
            return true
        }

        return lineIndexCache.index(for: source).lineCount > interactiveFullAnalysisLineLimit
    }

    static func analyzeFragment(_ source: String, range requestedRange: NSRange) -> YAMLTextAnalysis {
        let nsSource = source as NSString
        guard nsSource.length > 0 else {
            return YAMLTextAnalysis(highlights: [], validationIssues: [])
        }

        let boundedRange = NSIntersectionRange(
            requestedRange,
            NSRange(location: 0, length: nsSource.length)
        )
        guard boundedRange.length > 0 else {
            return YAMLTextAnalysis(highlights: [], validationIssues: [])
        }

        let lineAlignedRange = nsSource.lineRange(for: boundedRange)
        let fragment = nsSource.substring(with: lineAlignedRange)
        let lineOffset = max(0, lineNumber(for: lineAlignedRange.location, in: source) - 1)
        let analysis = analyzeUncached(fragment)

        return YAMLTextAnalysis(
            highlights: analysis.highlights.map {
                YAMLHighlightSpan(
                    range: NSRange(location: $0.range.location + lineAlignedRange.location, length: $0.range.length),
                    kind: $0.kind
                )
            },
            validationIssues: analysis.validationIssues.map {
                shiftedIssue($0, locationOffset: lineAlignedRange.location, lineOffset: lineOffset)
            }
        )
    }

    static func softTabWhitespace(forColumn column: Int, indentWidth: Int = 2) -> String {
        let width = max(1, indentWidth)
        let remainder = column % width
        let count = remainder == 0 ? width : width - remainder
        return String(repeating: " ", count: count)
    }

    static func suggestedIndentation(after line: String) -> String {
        let nsLine = line as NSString
        let indentation = leadingWhitespace(in: nsLine)
        let contentEnd = commentStart(in: nsLine) ?? nsLine.length
        let first = firstNonWhitespace(in: nsLine, from: 0, to: contentEnd)
        guard first < contentEnd else {
            return nsLine.substring(with: NSRange(location: 0, length: indentation))
        }

        let content = nsLine.substring(with: NSRange(location: first, length: contentEnd - first))
            .trimmingCharacters(in: .whitespaces)
        let currentIndentation = nsLine.substring(with: NSRange(location: 0, length: indentation))

        if content == "-" || content.hasSuffix(":") || content.hasSuffix("|") || content.hasSuffix(">") {
            return currentIndentation + "  "
        }

        if content.hasPrefix("- ") {
            let remainder = String(content.dropFirst(2)).trimmingCharacters(in: .whitespaces)
            if remainder.isEmpty || remainder.hasSuffix(":") || remainder.hasSuffix("|") || remainder.hasSuffix(">") {
                return currentIndentation + "  "
            }
        }

        return currentIndentation
    }

    private static let cache = YAMLAnalysisCache()
    private static let lineIndexCache = YAMLLineIndexCache()

    fileprivate static func analyzeUncached(_ source: String) -> YAMLTextAnalysis {
        let model = YAMLDocumentModel(source: source)
        let highlights = model.tokens.map {
            YAMLHighlightSpan(range: $0.range, kind: YAMLHighlightKind(tokenKind: $0.kind))
        }
        let issues = deduplicated(
            model.diagnostics
                + flowDelimiterDiagnostics(in: source)
                + kubernetesManagedFieldsDiagnostics(in: source)
        )
        return YAMLTextAnalysis(highlights: highlights, validationIssues: issues)
    }

    private static func kubernetesManagedFieldsDiagnostics(in source: String) -> [YAMLValidationIssue] {
        guard source.contains("managedFields:"), source.contains("fieldsV1:") else {
            return []
        }

        struct SourceLine {
            let text: String
            let trimmed: String
            let indent: Int
            let lineNumber: Int
            let offset: Int
        }

        var lines: [SourceLine] = []
        var offset = 0
        for (index, line) in source.split(separator: "\n", omittingEmptySubsequences: false).map(String.init).enumerated() {
            lines.append(SourceLine(
                text: line,
                trimmed: line.trimmingCharacters(in: .whitespaces),
                indent: line.prefix { $0 == " " }.count,
                lineNumber: index + 1,
                offset: offset
            ))
            offset += (line as NSString).length + 1
        }

        var issues: [YAMLValidationIssue] = []
        var managedFieldsIndent: Int?

        func nextMeaningfulLine(after index: Int) -> SourceLine? {
            guard index + 1 < lines.count else { return nil }
            for candidate in lines[(index + 1)...] {
                if !candidate.trimmed.isEmpty, !candidate.trimmed.hasPrefix("#") {
                    return candidate
                }
            }
            return nil
        }

        for index in lines.indices {
            let line = lines[index]
            guard !line.trimmed.isEmpty, !line.trimmed.hasPrefix("#") else { continue }

            if let indent = managedFieldsIndent,
               line.indent < indent || (line.indent == indent && !line.trimmed.hasPrefix("-")) {
                managedFieldsIndent = nil
            }

            if line.trimmed == "managedFields:" || line.trimmed.hasPrefix("managedFields: ") {
                managedFieldsIndent = line.indent
                continue
            }

            guard managedFieldsIndent != nil, line.trimmed == "fieldsV1:" else { continue }
            guard let next = nextMeaningfulLine(after: index) else { continue }
            if next.indent == line.indent, isManagedFieldsOwnershipKey(next.trimmed) {
                issues.append(YAMLValidationIssue(
                    source: .kubernetes,
                    severity: .error,
                    message: "ManagedFields ownership key is aligned with fieldsV1. Indent it under fieldsV1 or hide managed fields before editing.",
                    line: next.lineNumber,
                    column: next.indent + 1,
                    range: YAMLValidationRange(
                        location: next.offset + next.indent,
                        length: min((next.trimmed as NSString).length, max(1, next.text.count - next.indent))
                    )
                ))
            }
        }

        return issues
    }

    private static func isManagedFieldsOwnershipKey(_ trimmed: String) -> Bool {
        trimmed.hasPrefix("f:")
            || trimmed.hasPrefix("k:")
            || trimmed == ".:"
            || trimmed.hasPrefix(".: ")
    }

    private static func flowDelimiterDiagnostics(in source: String) -> [YAMLValidationIssue] {
        guard source.rangeOfCharacter(from: CharacterSet(charactersIn: "[]{}")) != nil else {
            return []
        }

        struct StackItem {
            let character: unichar
            let offset: Int
        }

        let nsSource = source as NSString
        var stack: [StackItem] = []
        var issues: [YAMLValidationIssue] = []
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false
        var inComment = false

        for index in 0..<nsSource.length {
            let character = nsSource.character(at: index)

            if inComment {
                if character == YAMLServiceCharacter.newline || character == YAMLServiceCharacter.carriageReturn {
                    inComment = false
                }
                continue
            }

            if escaped {
                escaped = false
                continue
            }

            if character == YAMLServiceCharacter.backslash && inDoubleQuotes {
                escaped = true
                continue
            }

            if character == YAMLServiceCharacter.singleQuote && !inDoubleQuotes {
                if inSingleQuotes,
                   index + 1 < nsSource.length,
                   nsSource.character(at: index + 1) == YAMLServiceCharacter.singleQuote {
                    continue
                }
                inSingleQuotes.toggle()
                continue
            }

            if character == YAMLServiceCharacter.doubleQuote && !inSingleQuotes {
                inDoubleQuotes.toggle()
                continue
            }

            if character == YAMLServiceCharacter.hash && !inSingleQuotes && !inDoubleQuotes {
                inComment = true
                continue
            }

            guard !inSingleQuotes, !inDoubleQuotes else { continue }

            if character == YAMLServiceCharacter.leftBracket || character == YAMLServiceCharacter.leftBrace {
                stack.append(StackItem(character: character, offset: index))
                continue
            }

            if character == YAMLServiceCharacter.rightBracket || character == YAMLServiceCharacter.rightBrace {
                guard let last = stack.last else {
                    issues.append(delimiterIssue(source: source, offset: index, message: "Unexpected closing '\(String(Character(UnicodeScalar(character)!)))'."))
                    continue
                }

                let expected = character == YAMLServiceCharacter.rightBracket
                    ? YAMLServiceCharacter.leftBracket
                    : YAMLServiceCharacter.leftBrace
                if last.character == expected {
                    stack.removeLast()
                } else {
                    issues.append(delimiterIssue(source: source, offset: index, message: "Mismatched closing '\(String(Character(UnicodeScalar(character)!)))'."))
                }
            }
        }

        for item in stack {
            issues.append(
                delimiterIssue(
                    source: source,
                    offset: item.offset,
                    message: "Unclosed '\(String(Character(UnicodeScalar(item.character)!)))' flow collection."
                )
            )
        }

        return issues
    }

    private static func delimiterIssue(source: String, offset: Int, message: String) -> YAMLValidationIssue {
        YAMLValidationIssue(
            source: .syntax,
            severity: .error,
            message: message,
            line: lineNumber(for: offset, in: source),
            column: columnNumber(for: offset, in: source),
            range: YAMLValidationRange(location: offset, length: 1)
        )
    }

    private static func lineNumber(for offset: Int, in source: String) -> Int {
        lineIndexCache.index(for: source).lineNumber(for: offset)
    }

    private static func columnNumber(for offset: Int, in source: String) -> Int {
        let nsSource = source as NSString
        let boundedOffset = max(0, min(offset, nsSource.length))
        let lineRange = nsSource.lineRange(for: NSRange(location: boundedOffset, length: 0))
        return boundedOffset - lineRange.location + 1
    }

    private static func deduplicated(_ issues: [YAMLValidationIssue]) -> [YAMLValidationIssue] {
        var seen: Set<String> = []
        return issues.filter { seen.insert($0.id).inserted }
    }

    private static func shiftedIssue(
        _ issue: YAMLValidationIssue,
        locationOffset: Int,
        lineOffset: Int
    ) -> YAMLValidationIssue {
        let shiftedRange = issue.range.map {
            YAMLValidationRange(location: $0.location + locationOffset, length: $0.length)
        }
        return YAMLValidationIssue(
            source: issue.source,
            severity: issue.severity,
            message: issue.message,
            line: issue.line.map { $0 + lineOffset },
            column: issue.column,
            range: shiftedRange
        )
    }

    private static func leadingWhitespace(in line: NSString) -> Int {
        firstNonWhitespace(in: line, from: 0, to: line.length)
    }

    private static func firstNonWhitespace(in line: NSString, from start: Int, to end: Int) -> Int {
        var index = start
        while index < end {
            let character = line.character(at: index)
            guard character == YAMLServiceCharacter.space || character == YAMLServiceCharacter.tab else { break }
            index += 1
        }
        return index
    }

    private static func commentStart(in line: NSString) -> Int? {
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false

        for index in 0..<line.length {
            let character = line.character(at: index)
            if escaped {
                escaped = false
                continue
            }
            if character == YAMLServiceCharacter.backslash && inDoubleQuotes {
                escaped = true
                continue
            }
            if character == YAMLServiceCharacter.singleQuote && !inDoubleQuotes {
                inSingleQuotes.toggle()
                continue
            }
            if character == YAMLServiceCharacter.doubleQuote && !inSingleQuotes {
                inDoubleQuotes.toggle()
                continue
            }
            if character == YAMLServiceCharacter.hash && !inSingleQuotes && !inDoubleQuotes {
                if index == 0 || line.character(at: index - 1) == YAMLServiceCharacter.space || line.character(at: index - 1) == YAMLServiceCharacter.tab {
                    return index
                }
            }
        }

        return nil
    }
}

private extension YAMLHighlightKind {
    init(tokenKind: YAMLSyntaxTokenKind) {
        switch tokenKind {
        case .key:
            self = .key
        case .string:
            self = .string
        case .number:
            self = .number
        case .boolean:
            self = .boolean
        case .comment:
            self = .comment
        case .directive:
            self = .directive
        case .anchor:
            self = .anchor
        case .alias:
            self = .alias
        }
    }
}

private final class YAMLAnalysisCache: @unchecked Sendable {
    private let lock = NSLock()
    private var cachedSource: String?
    private var cachedAnalysis: YAMLTextAnalysis?

    func analysis(for source: String) -> YAMLTextAnalysis {
        lock.lock()
        if cachedSource == source, let cachedAnalysis {
            lock.unlock()
            return cachedAnalysis
        }
        lock.unlock()

        let analysis = YAMLLanguageService.analyzeUncached(source)

        lock.lock()
        cachedSource = source
        cachedAnalysis = analysis
        lock.unlock()

        return analysis
    }
}

private struct YAMLLineIndex {
    let utf16Length: Int
    let lineStartOffsets: [Int]

    var lineCount: Int {
        lineStartOffsets.count
    }

    func lineNumber(for offset: Int) -> Int {
        let boundedOffset = max(0, min(offset, utf16Length))
        var lowerBound = 0
        var upperBound = lineStartOffsets.count

        while lowerBound < upperBound {
            let midpoint = (lowerBound + upperBound) / 2
            if lineStartOffsets[midpoint] <= boundedOffset {
                lowerBound = midpoint + 1
            } else {
                upperBound = midpoint
            }
        }

        return max(1, lowerBound)
    }
}

private final class YAMLLineIndexCache: @unchecked Sendable {
    private let lock = NSLock()
    private var cachedSource: String?
    private var cachedIndex: YAMLLineIndex?

    func index(for source: String) -> YAMLLineIndex {
        lock.lock()
        if cachedSource == source, let cachedIndex {
            lock.unlock()
            return cachedIndex
        }
        lock.unlock()

        let index = makeIndex(for: source)

        lock.lock()
        cachedSource = source
        cachedIndex = index
        lock.unlock()

        return index
    }

    private func makeIndex(for source: String) -> YAMLLineIndex {
        let nsSource = source as NSString
        var lineStartOffsets = [0]
        var offset = 0

        while offset < nsSource.length {
            let character = nsSource.character(at: offset)
            if character == YAMLServiceCharacter.newline {
                lineStartOffsets.append(offset + 1)
            } else if character == YAMLServiceCharacter.carriageReturn {
                if offset + 1 < nsSource.length,
                   nsSource.character(at: offset + 1) == YAMLServiceCharacter.newline {
                    offset += 1
                }
                lineStartOffsets.append(offset + 1)
            }
            offset += 1
        }

        return YAMLLineIndex(utf16Length: nsSource.length, lineStartOffsets: lineStartOffsets)
    }
}

private enum YAMLServiceCharacter {
    static let tab: unichar = 9
    static let newline: unichar = 10
    static let carriageReturn: unichar = 13
    static let space: unichar = 32
    static let hash: unichar = 35
    static let doubleQuote: unichar = 34
    static let singleQuote: unichar = 39
    static let backslash: unichar = 92
    static let leftBracket: unichar = 91
    static let rightBracket: unichar = 93
    static let leftBrace: unichar = 123
    static let rightBrace: unichar = 125
}
