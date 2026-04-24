import Foundation
import RuneCore

enum YAMLSyntaxTokenKind: Equatable {
    case key
    case string
    case number
    case boolean
    case comment
    case directive
    case anchor
    case alias
}

struct YAMLSyntaxToken: Equatable {
    let range: NSRange
    let kind: YAMLSyntaxTokenKind
}

struct YAMLDocumentLine: Equatable {
    enum Shape: Equatable {
        case blank
        case comment
        case directive
        case mapping(opensBlock: Bool)
        case sequence(opensBlock: Bool)
        case scalar
    }

    let number: Int
    let range: NSRange
    let text: String
    let indentationColumns: Int
    let indentationRange: NSRange
    let indentationTabOffsets: [Int]
    let contentWithoutComment: String
    let shape: Shape

    var isMeaningful: Bool {
        switch shape {
        case .blank, .comment:
            return false
        case .directive, .mapping, .sequence, .scalar:
            return true
        }
    }

    var opensNestedBlock: Bool {
        switch shape {
        case let .mapping(opensBlock), let .sequence(opensBlock):
            return opensBlock
        case .blank, .comment, .directive, .scalar:
            return false
        }
    }
}

struct YAMLDocumentModel: Equatable {
    let source: String
    let lines: [YAMLDocumentLine]
    let tokens: [YAMLSyntaxToken]
    let diagnostics: [YAMLValidationIssue]

    init(source: String) {
        self.source = source

        let parser = YAMLDocumentParser(source: source)
        let parsed = parser.parse()
        self.lines = parsed.lines
        self.tokens = parsed.tokens
        self.diagnostics = parsed.diagnostics
    }
}

private struct YAMLDocumentParser {
    private let source: String
    private let nsSource: NSString

    init(source: String) {
        self.source = source
        self.nsSource = source as NSString
    }

    func parse() -> (lines: [YAMLDocumentLine], tokens: [YAMLSyntaxToken], diagnostics: [YAMLValidationIssue]) {
        var lines: [YAMLDocumentLine] = []
        var tokens: [YAMLSyntaxToken] = []
        var diagnostics: [YAMLValidationIssue] = []
        var previousMeaningfulLine: YAMLDocumentLine?
        var lineNumber = 1

        nsSource.enumerateSubstrings(
            in: NSRange(location: 0, length: nsSource.length),
            options: [.byLines, .substringNotRequired]
        ) { _, substringRange, _, _ in
            let line = nsSource.substring(with: substringRange) as NSString
            let parsedLine = parseLine(line, number: lineNumber, absoluteLocation: substringRange.location)

            tokens.append(contentsOf: parsedLine.tokens)
            diagnostics.append(contentsOf: parsedLine.diagnostics)

            let documentLine = parsedLine.line
            if documentLine.isMeaningful {
                if let previous = previousMeaningfulLine,
                   documentLine.indentationColumns > previous.indentationColumns,
                   !previous.opensNestedBlock,
                   !isPlainScalarContinuation(documentLine, after: previous) {
                    diagnostics.append(
                        YAMLValidationIssue(
                            source: .syntax,
                            severity: .error,
                            message: "Unexpected indentation. The previous line does not start a nested YAML block.",
                            line: documentLine.number,
                            column: 1,
                            range: YAMLValidationRange(
                                location: documentLine.indentationRange.location,
                                length: max(1, documentLine.indentationRange.length)
                            )
                        )
                    )
                }

                previousMeaningfulLine = documentLine
            }

            lines.append(documentLine)
            lineNumber += 1
        }

        return (lines, tokens, suppressWarningsShadowedByErrors(diagnostics))
    }

    private func isPlainScalarContinuation(_ line: YAMLDocumentLine, after previous: YAMLDocumentLine) -> Bool {
        guard case .scalar = line.shape else { return false }
        guard case .mapping(let opensBlock) = previous.shape, !opensBlock else { return false }
        return true
    }

    private func suppressWarningsShadowedByErrors(_ diagnostics: [YAMLValidationIssue]) -> [YAMLValidationIssue] {
        let syntaxErrorLocations = Set(
            diagnostics
                .filter { $0.source == .syntax && $0.severity == .error }
                .map(Self.issueLocationKey)
        )

        guard !syntaxErrorLocations.isEmpty else { return diagnostics }

        return diagnostics.filter { issue in
            guard issue.source == .syntax, issue.severity == .warning else { return true }
            return !syntaxErrorLocations.contains(Self.issueLocationKey(issue))
        }
    }

    private static func issueLocationKey(_ issue: YAMLValidationIssue) -> String {
        [
            issue.line.map(String.init) ?? "-",
            issue.column.map(String.init) ?? "-",
            issue.range.map { "\($0.location):\($0.length)" } ?? "-"
        ].joined(separator: "|")
    }

    private func parseLine(
        _ line: NSString,
        number: Int,
        absoluteLocation: Int
    ) -> (line: YAMLDocumentLine, tokens: [YAMLSyntaxToken], diagnostics: [YAMLValidationIssue]) {
        let indentation = indentationInfo(in: line, absoluteLocation: absoluteLocation)
        let commentStart = commentStart(in: line)
        let contentEnd = commentStart ?? line.length
        let content = line.substring(with: NSRange(location: 0, length: contentEnd))
        let trimmedContent = content.trimmingCharacters(in: .whitespaces)
        let shape = lineShape(line, contentEnd: contentEnd, indentationLength: indentation.length)

        var tokens: [YAMLSyntaxToken] = []
        var diagnostics: [YAMLValidationIssue] = []

        for offset in indentation.tabOffsets {
            diagnostics.append(
                YAMLValidationIssue(
                    source: .syntax,
                    severity: .error,
                    message: "Tabs are not allowed in YAML indentation.",
                    line: number,
                    column: offset + 1,
                    range: YAMLValidationRange(location: absoluteLocation + offset, length: 1)
                )
            )
        }

        if indentation.tabOffsets.isEmpty,
           indentation.columns > 0,
           indentation.columns % 2 != 0 {
            diagnostics.append(
                YAMLValidationIssue(
                    source: .syntax,
                    severity: .warning,
                    message: "Indentation is not aligned to a two-space YAML level.",
                    line: number,
                    column: 1,
                    range: YAMLValidationRange(
                        location: absoluteLocation,
                        length: max(1, indentation.length)
                    )
                )
            )
        }

        let syntax = syntaxTokens(
            in: line,
            lineNumber: number,
            absoluteLocation: absoluteLocation,
            contentEnd: contentEnd
        )
        tokens.append(contentsOf: syntax.tokens)
        diagnostics.append(contentsOf: syntax.diagnostics)

        if let commentStart {
            tokens.append(
                YAMLSyntaxToken(
                    range: NSRange(location: absoluteLocation + commentStart, length: line.length - commentStart),
                    kind: .comment
                )
            )
        }

        return (
            YAMLDocumentLine(
                number: number,
                range: NSRange(location: absoluteLocation, length: line.length),
                text: line as String,
                indentationColumns: indentation.columns,
                indentationRange: NSRange(location: absoluteLocation, length: indentation.length),
                indentationTabOffsets: indentation.tabOffsets,
                contentWithoutComment: content,
                shape: trimmedContent.isEmpty ? .blank : shape
            ),
            tokens,
            diagnostics
        )
    }

    private func indentationInfo(in line: NSString, absoluteLocation: Int) -> (columns: Int, length: Int, tabOffsets: [Int]) {
        var columns = 0
        var length = 0
        var tabOffsets: [Int] = []

        while length < line.length {
            let character = line.character(at: length)
            if character == YAMLCharacter.space {
                columns += 1
                length += 1
                continue
            }

            if character == YAMLCharacter.tab {
                columns += 2
                tabOffsets.append(length)
                length += 1
                continue
            }

            break
        }

        return (columns, length, tabOffsets)
    }

    private func lineShape(_ line: NSString, contentEnd: Int, indentationLength: Int) -> YAMLDocumentLine.Shape {
        let first = firstNonWhitespace(in: line, from: indentationLength, to: contentEnd)
        guard first < contentEnd else { return .blank }

        if line.character(at: first) == YAMLCharacter.hash {
            return .comment
        }

        let trimmed = line.substring(with: NSRange(location: first, length: contentEnd - first))
            .trimmingCharacters(in: .whitespaces)

        if trimmed.hasPrefix("---") || trimmed.hasPrefix("...") || trimmed.hasPrefix("%") {
            return .directive
        }

        if line.character(at: first) == YAMLCharacter.hyphen,
           first + 1 >= contentEnd || isWhitespace(line.character(at: first + 1)) {
            let itemStart = firstNonWhitespace(in: line, from: min(first + 1, contentEnd), to: contentEnd)
            guard itemStart < contentEnd else { return .sequence(opensBlock: true) }
            if mappingKeyRange(in: line, contentEnd: contentEnd) != nil {
                return .sequence(opensBlock: true)
            }
            return .sequence(opensBlock: valueOpensBlock(in: line, start: itemStart, end: contentEnd))
        }

        if mappingKeyRange(in: line, contentEnd: contentEnd) != nil {
            return .mapping(opensBlock: valueOpensBlockAfterMappingSeparator(in: line, contentEnd: contentEnd))
        }

        return .scalar
    }

    private func syntaxTokens(
        in line: NSString,
        lineNumber: Int,
        absoluteLocation: Int,
        contentEnd: Int
    ) -> (tokens: [YAMLSyntaxToken], diagnostics: [YAMLValidationIssue]) {
        var tokens: [YAMLSyntaxToken] = []
        var diagnostics: [YAMLValidationIssue] = []

        if let directive = directiveRange(in: line, contentEnd: contentEnd) {
            tokens.append(YAMLSyntaxToken(range: shifted(directive, by: absoluteLocation), kind: .directive))
        }

        if let keyRange = mappingKeyRange(in: line, contentEnd: contentEnd) {
            tokens.append(YAMLSyntaxToken(range: shifted(keyRange, by: absoluteLocation), kind: .key))
        }

        var index = 0
        while index < contentEnd {
            let character = line.character(at: index)

            if character == YAMLCharacter.doubleQuote {
                let consumed = consumeDoubleQuotedString(in: line, from: index, end: contentEnd)
                tokens.append(YAMLSyntaxToken(range: shifted(consumed.range, by: absoluteLocation), kind: .string))
                if !consumed.closed {
                    diagnostics.append(
                        YAMLValidationIssue(
                            source: .syntax,
                            severity: .error,
                            message: "Unclosed double-quoted string.",
                            line: lineNumber,
                            column: consumed.range.location + 1,
                            range: YAMLValidationRange(
                                location: absoluteLocation + consumed.range.location,
                                length: consumed.range.length
                            )
                        )
                    )
                }
                index = consumed.range.location + consumed.range.length
                continue
            }

            if character == YAMLCharacter.singleQuote {
                let consumed = consumeSingleQuotedString(in: line, from: index, end: contentEnd)
                tokens.append(YAMLSyntaxToken(range: shifted(consumed.range, by: absoluteLocation), kind: .string))
                if !consumed.closed {
                    diagnostics.append(
                        YAMLValidationIssue(
                            source: .syntax,
                            severity: .error,
                            message: "Unclosed single-quoted string.",
                            line: lineNumber,
                            column: consumed.range.location + 1,
                            range: YAMLValidationRange(
                                location: absoluteLocation + consumed.range.location,
                                length: consumed.range.length
                            )
                        )
                    )
                }
                index = consumed.range.location + consumed.range.length
                continue
            }

            if (character == YAMLCharacter.ampersand || character == YAMLCharacter.asterisk),
               hasScalarBoundary(before: line, at: index) {
                let nameRange = consumeAnchorName(in: line, from: index + 1, end: contentEnd)
                if nameRange.length > 0 {
                    tokens.append(
                        YAMLSyntaxToken(
                            range: NSRange(location: absoluteLocation + index, length: 1 + nameRange.length),
                            kind: character == YAMLCharacter.ampersand ? .anchor : .alias
                        )
                    )
                    index = nameRange.location + nameRange.length
                    continue
                }
            }

            if isScalarStart(character),
               hasScalarBoundary(before: line, at: index) {
                let scalarRange = consumePlainScalar(in: line, from: index, end: contentEnd)
                let scalar = line.substring(with: scalarRange)
                if isBooleanScalar(scalar) {
                    tokens.append(YAMLSyntaxToken(range: shifted(scalarRange, by: absoluteLocation), kind: .boolean))
                } else if isNumberScalar(scalar) {
                    tokens.append(YAMLSyntaxToken(range: shifted(scalarRange, by: absoluteLocation), kind: .number))
                }
                index = scalarRange.location + scalarRange.length
                continue
            }

            index += 1
        }

        return (tokens, diagnostics)
    }

    private func directiveRange(in line: NSString, contentEnd: Int) -> NSRange? {
        let first = firstNonWhitespace(in: line, from: 0, to: contentEnd)
        guard first < contentEnd else { return nil }

        let trimmed = line.substring(with: NSRange(location: first, length: contentEnd - first))
            .trimmingCharacters(in: .whitespaces)

        if trimmed.hasPrefix("---") || trimmed.hasPrefix("...") {
            return NSRange(location: first, length: min(3, contentEnd - first))
        }

        if trimmed.hasPrefix("%") {
            return NSRange(location: first, length: contentEnd - first)
        }

        return nil
    }

    private func mappingKeyRange(in line: NSString, contentEnd: Int) -> NSRange? {
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false
        var index = 0

        while index < contentEnd {
            let character = line.character(at: index)

            if escaped {
                escaped = false
                index += 1
                continue
            }

            if character == YAMLCharacter.backslash && inDoubleQuotes {
                escaped = true
                index += 1
                continue
            }

            if character == YAMLCharacter.singleQuote && !inDoubleQuotes {
                if inSingleQuotes, index + 1 < contentEnd, line.character(at: index + 1) == YAMLCharacter.singleQuote {
                    index += 2
                    continue
                }
                inSingleQuotes.toggle()
            } else if character == YAMLCharacter.doubleQuote && !inSingleQuotes {
                inDoubleQuotes.toggle()
            } else if character == YAMLCharacter.colon && !inSingleQuotes && !inDoubleQuotes {
                let next = index + 1
                guard next >= contentEnd || isWhitespace(line.character(at: next)) else {
                    index += 1
                    continue
                }

                var start = firstNonWhitespace(in: line, from: 0, to: index)
                if start < index,
                   line.character(at: start) == YAMLCharacter.hyphen,
                   start + 1 < index,
                   isWhitespace(line.character(at: start + 1)) {
                    start = firstNonWhitespace(in: line, from: start + 2, to: index)
                }

                if start < index,
                   line.character(at: start) == YAMLCharacter.question,
                   start + 1 < index,
                   isWhitespace(line.character(at: start + 1)) {
                    start = firstNonWhitespace(in: line, from: start + 2, to: index)
                }

                var end = index
                while end > start, isWhitespace(line.character(at: end - 1)) {
                    end -= 1
                }

                guard end > start else { return nil }
                return NSRange(location: start, length: end - start)
            }

            index += 1
        }

        return nil
    }

    private func valueOpensBlockAfterMappingSeparator(in line: NSString, contentEnd: Int) -> Bool {
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false
        var index = 0

        while index < contentEnd {
            let character = line.character(at: index)

            if escaped {
                escaped = false
                index += 1
                continue
            }

            if character == YAMLCharacter.backslash && inDoubleQuotes {
                escaped = true
                index += 1
                continue
            }

            if character == YAMLCharacter.singleQuote && !inDoubleQuotes {
                inSingleQuotes.toggle()
            } else if character == YAMLCharacter.doubleQuote && !inSingleQuotes {
                inDoubleQuotes.toggle()
            } else if character == YAMLCharacter.colon && !inSingleQuotes && !inDoubleQuotes {
                let next = firstNonWhitespace(in: line, from: index + 1, to: contentEnd)
                guard next < contentEnd else { return true }
                return valueOpensBlock(in: line, start: next, end: contentEnd)
            }

            index += 1
        }

        return false
    }

    private func valueOpensBlock(in line: NSString, start: Int, end: Int) -> Bool {
        guard start < end else { return true }
        let value = line.substring(with: NSRange(location: start, length: end - start))
            .trimmingCharacters(in: .whitespaces)
        guard !value.isEmpty else { return true }
        return value == "|" || value == ">" || value.hasSuffix(":")
    }

    private func commentStart(in line: NSString) -> Int? {
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false
        var index = 0

        while index < line.length {
            let character = line.character(at: index)

            if escaped {
                escaped = false
                index += 1
                continue
            }

            if character == YAMLCharacter.backslash && inDoubleQuotes {
                escaped = true
                index += 1
                continue
            }

            if character == YAMLCharacter.singleQuote && !inDoubleQuotes {
                if inSingleQuotes, index + 1 < line.length, line.character(at: index + 1) == YAMLCharacter.singleQuote {
                    index += 2
                    continue
                }
                inSingleQuotes.toggle()
            } else if character == YAMLCharacter.doubleQuote && !inSingleQuotes {
                inDoubleQuotes.toggle()
            } else if character == YAMLCharacter.hash && !inSingleQuotes && !inDoubleQuotes {
                if index == 0 || isWhitespace(line.character(at: index - 1)) {
                    return index
                }
            }

            index += 1
        }

        return nil
    }

    private func consumeDoubleQuotedString(in line: NSString, from start: Int, end: Int) -> (range: NSRange, closed: Bool) {
        var index = start + 1
        var escaped = false
        while index < end {
            let character = line.character(at: index)
            if escaped {
                escaped = false
                index += 1
                continue
            }
            if character == YAMLCharacter.backslash {
                escaped = true
                index += 1
                continue
            }
            if character == YAMLCharacter.doubleQuote {
                return (NSRange(location: start, length: index - start + 1), true)
            }
            index += 1
        }
        return (NSRange(location: start, length: end - start), false)
    }

    private func consumeSingleQuotedString(in line: NSString, from start: Int, end: Int) -> (range: NSRange, closed: Bool) {
        var index = start + 1
        while index < end {
            if line.character(at: index) == YAMLCharacter.singleQuote {
                if index + 1 < end, line.character(at: index + 1) == YAMLCharacter.singleQuote {
                    index += 2
                    continue
                }
                return (NSRange(location: start, length: index - start + 1), true)
            }
            index += 1
        }
        return (NSRange(location: start, length: end - start), false)
    }

    private func consumeAnchorName(in line: NSString, from start: Int, end: Int) -> NSRange {
        var index = start
        while index < end, isAnchorCharacter(line.character(at: index)) {
            index += 1
        }
        return NSRange(location: start, length: index - start)
    }

    private func consumePlainScalar(in line: NSString, from start: Int, end: Int) -> NSRange {
        var index = start
        while index < end {
            let character = line.character(at: index)
            if isWhitespace(character) || isPlainScalarTerminator(character) {
                break
            }
            index += 1
        }
        return NSRange(location: start, length: index - start)
    }

    private func firstNonWhitespace(in line: NSString, from start: Int, to end: Int) -> Int {
        var index = start
        while index < end, isWhitespace(line.character(at: index)) {
            index += 1
        }
        return index
    }

    private func shifted(_ range: NSRange, by offset: Int) -> NSRange {
        NSRange(location: offset + range.location, length: range.length)
    }

    private func hasScalarBoundary(before line: NSString, at index: Int) -> Bool {
        guard index > 0 else { return true }
        let previous = line.character(at: index - 1)
        return isWhitespace(previous)
            || previous == YAMLCharacter.colon
            || previous == YAMLCharacter.comma
            || previous == YAMLCharacter.leftBracket
            || previous == YAMLCharacter.leftBrace
    }

    private func isScalarStart(_ character: unichar) -> Bool {
        !isWhitespace(character)
            && character != YAMLCharacter.hash
            && character != YAMLCharacter.colon
            && character != YAMLCharacter.ampersand
            && character != YAMLCharacter.asterisk
            && character != YAMLCharacter.comma
            && character != YAMLCharacter.singleQuote
            && character != YAMLCharacter.doubleQuote
            && character != YAMLCharacter.leftBracket
            && character != YAMLCharacter.rightBracket
            && character != YAMLCharacter.leftBrace
            && character != YAMLCharacter.rightBrace
    }

    private func isPlainScalarTerminator(_ character: unichar) -> Bool {
        character == YAMLCharacter.hash
            || character == YAMLCharacter.colon
            || character == YAMLCharacter.comma
            || character == YAMLCharacter.leftBracket
            || character == YAMLCharacter.rightBracket
            || character == YAMLCharacter.leftBrace
            || character == YAMLCharacter.rightBrace
    }

    private func isAnchorCharacter(_ character: unichar) -> Bool {
        isLetter(character) || isDigit(character) || character == YAMLCharacter.underscore || character == YAMLCharacter.hyphen || character == YAMLCharacter.period
    }

    private func isBooleanScalar(_ text: String) -> Bool {
        switch text.lowercased() {
        case "true", "false", "yes", "no", "on", "off", "null", "~":
            return true
        default:
            return false
        }
    }

    private func isNumberScalar(_ text: String) -> Bool {
        guard !text.isEmpty else { return false }
        let nsText = text as NSString
        var index = 0
        if nsText.character(at: index) == YAMLCharacter.plus || nsText.character(at: index) == YAMLCharacter.hyphen {
            index += 1
        }
        guard index < nsText.length else { return false }

        var hasDigit = false
        var hasDecimalSeparator = false
        while index < nsText.length {
            let character = nsText.character(at: index)
            if isDigit(character) {
                hasDigit = true
            } else if character == YAMLCharacter.period, !hasDecimalSeparator {
                hasDecimalSeparator = true
            } else {
                return false
            }
            index += 1
        }
        return hasDigit
    }

    private func isWhitespace(_ character: unichar) -> Bool {
        character == YAMLCharacter.space || character == YAMLCharacter.tab
    }

    private func isLetter(_ character: unichar) -> Bool {
        (65...90).contains(character) || (97...122).contains(character)
    }

    private func isDigit(_ character: unichar) -> Bool {
        (48...57).contains(character)
    }
}

private enum YAMLCharacter {
    static let tab: unichar = 9
    static let space: unichar = 32
    static let hash: unichar = 35
    static let doubleQuote: unichar = 34
    static let singleQuote: unichar = 39
    static let backslash: unichar = 92
    static let colon: unichar = 58
    static let comma: unichar = 44
    static let hyphen: unichar = 45
    static let question: unichar = 63
    static let plus: unichar = 43
    static let period: unichar = 46
    static let underscore: unichar = 95
    static let ampersand: unichar = 38
    static let asterisk: unichar = 42
    static let leftBracket: unichar = 91
    static let rightBracket: unichar = 93
    static let leftBrace: unichar = 123
    static let rightBrace: unichar = 125
}
