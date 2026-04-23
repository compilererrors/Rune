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
    static func analyze(_ source: String) -> YAMLTextAnalysis {
        let nsSource = source as NSString
        var highlights: [YAMLHighlightSpan] = []
        var validationIssues: [YAMLValidationIssue] = []

        nsSource.enumerateSubstrings(
            in: NSRange(location: 0, length: nsSource.length),
            options: [.byLines, .substringNotRequired]
        ) { _, substringRange, _, _ in
            let line = nsSource.substring(with: substringRange)
            analyzeLine(
                line,
                source: source,
                absoluteLocation: substringRange.location,
                highlights: &highlights,
                validationIssues: &validationIssues
            )
        }

        validationIssues.append(contentsOf: unmatchedFlowDelimiterDiagnostics(in: source))
        return YAMLTextAnalysis(highlights: highlights, validationIssues: validationIssues)
    }

    static func suggestedIndentation(after line: String) -> String {
        let currentIndentation = String(line.prefix { $0.isWhitespace })
        let content = lineContentWithoutComment(line).trimmingCharacters(in: .whitespaces)
        guard !content.isEmpty else { return currentIndentation }

        if content == "-" {
            return currentIndentation + "  "
        }

        if content.hasSuffix(":") || content.hasSuffix("|") || content.hasSuffix(">") {
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

    private static func analyzeLine(
        _ line: String,
        source: String,
        absoluteLocation: Int,
        highlights: inout [YAMLHighlightSpan],
        validationIssues: inout [YAMLValidationIssue]
    ) {
        let scalars = Array(line)
        let sourceLine = lineNumber(for: absoluteLocation, in: source)

        for (offset, character) in scalars.enumerated() where character == "\t" {
            validationIssues.append(
                YAMLValidationIssue(
                    source: .syntax,
                    severity: .error,
                    message: "Tabs are not allowed in YAML indentation.",
                    line: sourceLine,
                    column: offset + 1,
                    range: YAMLValidationRange(location: absoluteLocation + offset, length: 1)
                )
            )
        }

        if let directiveRange = directiveRange(in: line) {
            highlights.append(
                YAMLHighlightSpan(
                    range: NSRange(location: absoluteLocation + directiveRange.location, length: directiveRange.length),
                    kind: .directive
                )
            )
        }

        if let keyRange = mappingKeyRange(in: line) {
            highlights.append(
                YAMLHighlightSpan(
                    range: NSRange(location: absoluteLocation + keyRange.location, length: keyRange.length),
                    kind: .key
                )
            )
        }

        var index = 0
        while index < scalars.count {
            let character = scalars[index]

            if character == "#" {
                highlights.append(
                    YAMLHighlightSpan(
                        range: NSRange(location: absoluteLocation + index, length: scalars.count - index),
                        kind: .comment
                    )
                )
                break
            }

            if character == "\"" {
                let stringRange = consumeDoubleQuotedString(in: scalars, from: index)
                highlights.append(
                    YAMLHighlightSpan(
                        range: NSRange(location: absoluteLocation + stringRange.location, length: stringRange.length),
                        kind: .string
                    )
                )

                if !stringRange.isClosed {
                    validationIssues.append(
                        YAMLValidationIssue(
                            source: .syntax,
                            severity: .error,
                            message: "Unclosed double-quoted string.",
                            line: sourceLine,
                            column: stringRange.location + 1,
                            range: YAMLValidationRange(
                                location: absoluteLocation + stringRange.location,
                                length: stringRange.length
                            )
                        )
                    )
                }

                index = stringRange.location + stringRange.length
                continue
            }

            if character == "'" {
                let stringRange = consumeSingleQuotedString(in: scalars, from: index)
                highlights.append(
                    YAMLHighlightSpan(
                        range: NSRange(location: absoluteLocation + stringRange.location, length: stringRange.length),
                        kind: .string
                    )
                )

                if !stringRange.isClosed {
                    validationIssues.append(
                        YAMLValidationIssue(
                            source: .syntax,
                            severity: .error,
                            message: "Unclosed single-quoted string.",
                            line: sourceLine,
                            column: stringRange.location + 1,
                            range: YAMLValidationRange(
                                location: absoluteLocation + stringRange.location,
                                length: stringRange.length
                            )
                        )
                    )
                }

                index = stringRange.location + stringRange.length
                continue
            }

            if (character == "&" || character == "*"),
               hasScalarBoundary(before: scalars, at: index) {
                let nameRange = consumeAnchorName(in: scalars, from: index + 1)
                if nameRange.length > 0 {
                    highlights.append(
                        YAMLHighlightSpan(
                            range: NSRange(location: absoluteLocation + index, length: 1 + nameRange.length),
                            kind: character == "&" ? .anchor : .alias
                        )
                    )
                    index = nameRange.location + nameRange.length
                    continue
                }
            }

            if isScalarStart(character),
               hasScalarBoundary(before: scalars, at: index) {
                let scalarRange = consumePlainScalar(in: scalars, from: index)
                let scalarText = String(scalars[scalarRange.location..<(scalarRange.location + scalarRange.length)])

                if isBooleanScalar(scalarText) {
                    highlights.append(
                        YAMLHighlightSpan(
                            range: NSRange(location: absoluteLocation + scalarRange.location, length: scalarRange.length),
                            kind: .boolean
                        )
                    )
                } else if isNumberScalar(scalarText) {
                    highlights.append(
                        YAMLHighlightSpan(
                            range: NSRange(location: absoluteLocation + scalarRange.location, length: scalarRange.length),
                            kind: .number
                        )
                    )
                }

                index = scalarRange.location + scalarRange.length
                continue
            }

            index += 1
        }
    }

    private static func directiveRange(in line: String) -> NSRange? {
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty else { return nil }

        let leadingWhitespace = line.prefix { $0.isWhitespace }.count
        if trimmed.hasPrefix("---") || trimmed.hasPrefix("...") {
            return NSRange(location: leadingWhitespace, length: min(3, trimmed.count))
        }

        if trimmed.hasPrefix("%") {
            let length = lineContentWithoutComment(line).count - leadingWhitespace
            guard length > 0 else { return nil }
            return NSRange(location: leadingWhitespace, length: length)
        }

        return nil
    }

    private static func mappingKeyRange(in line: String) -> NSRange? {
        let scalars = Array(line)
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false

        for index in scalars.indices {
            let character = scalars[index]

            if escaped {
                escaped = false
                continue
            }

            switch character {
            case "\\" where inDoubleQuotes:
                escaped = true
            case "'" where !inDoubleQuotes:
                if inSingleQuotes, index + 1 < scalars.count, scalars[index + 1] == "'" {
                    continue
                }
                inSingleQuotes.toggle()
            case "\"" where !inSingleQuotes:
                inDoubleQuotes.toggle()
            case "#" where !inSingleQuotes && !inDoubleQuotes:
                return nil
            case ":" where !inSingleQuotes && !inDoubleQuotes:
                let nextIndex = index + 1
                guard nextIndex >= scalars.count || scalars[nextIndex].isWhitespace else { continue }

                var start = 0
                while start < index, scalars[start].isWhitespace {
                    start += 1
                }

                if start < index, scalars[start] == "-", start + 1 < index, scalars[start + 1].isWhitespace {
                    start += 2
                    while start < index, scalars[start].isWhitespace {
                        start += 1
                    }
                }

                if start < index, scalars[start] == "?", start + 1 < index, scalars[start + 1].isWhitespace {
                    start += 2
                    while start < index, scalars[start].isWhitespace {
                        start += 1
                    }
                }

                var end = index
                while end > start, scalars[end - 1].isWhitespace {
                    end -= 1
                }

                guard end > start else { return nil }
                return NSRange(location: start, length: end - start)
            default:
                break
            }
        }

        return nil
    }

    private static func lineContentWithoutComment(_ line: String) -> String {
        let scalars = Array(line)
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false

        for index in scalars.indices {
            let character = scalars[index]

            if escaped {
                escaped = false
                continue
            }

            switch character {
            case "\\" where inDoubleQuotes:
                escaped = true
            case "'" where !inDoubleQuotes:
                if inSingleQuotes, index + 1 < scalars.count, scalars[index + 1] == "'" {
                    continue
                }
                inSingleQuotes.toggle()
            case "\"" where !inSingleQuotes:
                inDoubleQuotes.toggle()
            case "#" where !inSingleQuotes && !inDoubleQuotes:
                return String(scalars[..<index])
            default:
                break
            }
        }

        return line
    }

    private static func consumeDoubleQuotedString(in scalars: [Character], from start: Int) -> ConsumedRange {
        var index = start + 1
        var escaped = false

        while index < scalars.count {
            let character = scalars[index]
            if escaped {
                escaped = false
                index += 1
                continue
            }

            if character == "\\" {
                escaped = true
                index += 1
                continue
            }

            if character == "\"" {
                return ConsumedRange(location: start, length: index - start + 1, isClosed: true)
            }

            index += 1
        }

        return ConsumedRange(location: start, length: scalars.count - start, isClosed: false)
    }

    private static func consumeSingleQuotedString(in scalars: [Character], from start: Int) -> ConsumedRange {
        var index = start + 1

        while index < scalars.count {
            if scalars[index] == "'" {
                if index + 1 < scalars.count, scalars[index + 1] == "'" {
                    index += 2
                    continue
                }
                return ConsumedRange(location: start, length: index - start + 1, isClosed: true)
            }

            index += 1
        }

        return ConsumedRange(location: start, length: scalars.count - start, isClosed: false)
    }

    private static func consumeAnchorName(in scalars: [Character], from start: Int) -> NSRange {
        var index = start
        while index < scalars.count, isAnchorCharacter(scalars[index]) {
            index += 1
        }
        return NSRange(location: start, length: index - start)
    }

    private static func consumePlainScalar(in scalars: [Character], from start: Int) -> NSRange {
        var index = start
        while index < scalars.count {
            let character = scalars[index]
            if character.isWhitespace || "#:,[]{}".contains(character) {
                break
            }
            index += 1
        }
        return NSRange(location: start, length: index - start)
    }

    private static func hasScalarBoundary(before scalars: [Character], at index: Int) -> Bool {
        guard index > 0 else { return true }
        let previous = scalars[index - 1]
        return previous.isWhitespace || "[:,[{".contains(previous)
    }

    private static func isScalarStart(_ character: Character) -> Bool {
        !character.isWhitespace && !"#:&*,'\"[]{}".contains(character)
    }

    private static func isAnchorCharacter(_ character: Character) -> Bool {
        isLetter(character) || isDigit(character) || character == "_" || character == "-" || character == "."
    }

    private static func isBooleanScalar(_ text: String) -> Bool {
        switch text.lowercased() {
        case "true", "false", "yes", "no", "on", "off", "null", "~":
            return true
        default:
            return false
        }
    }

    private static func isNumberScalar(_ text: String) -> Bool {
        guard !text.isEmpty else { return false }
        var scalars = Array(text)
        if scalars.first == "+" || scalars.first == "-" {
            scalars.removeFirst()
        }
        guard !scalars.isEmpty else { return false }

        var hasDigit = false
        var hasDecimalSeparator = false

        for character in scalars {
            if isDigit(character) {
                hasDigit = true
                continue
            }

            if character == ".", !hasDecimalSeparator {
                hasDecimalSeparator = true
                continue
            }

            return false
        }

        return hasDigit
    }

    private static func isLetter(_ character: Character) -> Bool {
        character.unicodeScalars.allSatisfy { CharacterSet.letters.contains($0) }
    }

    private static func isDigit(_ character: Character) -> Bool {
        character.unicodeScalars.allSatisfy { CharacterSet.decimalDigits.contains($0) }
    }

    private static func unmatchedFlowDelimiterDiagnostics(in source: String) -> [YAMLValidationIssue] {
        struct StackItem {
            let character: Character
            let offset: Int
        }

        var stack: [StackItem] = []
        var validationIssues: [YAMLValidationIssue] = []
        let scalars = Array(source)
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false
        var inComment = false

        for index in scalars.indices {
            let character = scalars[index]

            if inComment {
                if character == "\n" {
                    inComment = false
                }
                continue
            }

            if escaped {
                escaped = false
                continue
            }

            switch character {
            case "\\" where inDoubleQuotes:
                escaped = true
            case "'" where !inDoubleQuotes:
                if inSingleQuotes, index + 1 < scalars.count, scalars[index + 1] == "'" {
                    continue
                }
                inSingleQuotes.toggle()
            case "\"" where !inSingleQuotes:
                inDoubleQuotes.toggle()
            case "#" where !inSingleQuotes && !inDoubleQuotes:
                inComment = true
            case "[", "{":
                guard !inSingleQuotes && !inDoubleQuotes else { continue }
                stack.append(StackItem(character: character, offset: index))
            case "]", "}":
                guard !inSingleQuotes && !inDoubleQuotes else { continue }
                guard let last = stack.last else {
                    validationIssues.append(
                        issueForDelimiter(
                            source: source,
                            offset: index,
                            message: "Unexpected closing '\(character)'."
                        )
                    )
                    continue
                }
                let expected: Character = character == "]" ? "[" : "{"
                if last.character == expected {
                    stack.removeLast()
                } else {
                    validationIssues.append(
                        issueForDelimiter(
                            source: source,
                            offset: index,
                            message: "Mismatched closing '\(character)'."
                        )
                    )
                }
            default:
                break
            }
        }

        for item in stack {
            validationIssues.append(
                issueForDelimiter(
                    source: source,
                    offset: item.offset,
                    message: "Unclosed '\(item.character)' flow collection."
                )
            )
        }

        return validationIssues
    }

    private static func issueForDelimiter(
        source: String,
        offset: Int,
        message: String
    ) -> YAMLValidationIssue {
        let line = lineNumber(for: offset, in: source)
        let column = columnNumber(for: offset, in: source)
        return YAMLValidationIssue(
            source: .syntax,
            severity: .error,
            message: message,
            line: line,
            column: column,
            range: YAMLValidationRange(location: offset, length: 1)
        )
    }

    private static func lineNumber(for offset: Int, in source: String) -> Int {
        let nsSource = source as NSString
        let boundedOffset = max(0, min(offset, nsSource.length))
        let prefix = nsSource.substring(to: boundedOffset)
        return prefix.reduce(into: 1) { count, character in
            if character == "\n" {
                count += 1
            }
        }
    }

    private static func columnNumber(for offset: Int, in source: String) -> Int {
        let nsSource = source as NSString
        let boundedOffset = max(0, min(offset, nsSource.length))
        let lineRange = nsSource.lineRange(for: NSRange(location: boundedOffset, length: 0))
        return boundedOffset - lineRange.location + 1
    }
}

private struct ConsumedRange {
    let location: Int
    let length: Int
    let isClosed: Bool
}
