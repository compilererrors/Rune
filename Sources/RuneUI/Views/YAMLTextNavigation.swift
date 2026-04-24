import Foundation
import RuneCore

struct YAMLTextNavigationRequest: Equatable {
    let issueID: String
    let sequence: Int
    let range: YAMLValidationRange?
    let line: Int?
    let column: Int?

    init(issue: YAMLValidationIssue, sequence: Int) {
        self.issueID = issue.id
        self.sequence = sequence
        self.range = issue.range
        self.line = issue.line
        self.column = issue.column
    }
}

enum YAMLTextNavigation {
    static func targetRange(in text: String, request: YAMLTextNavigationRequest) -> NSRange? {
        let nsText = text as NSString
        if let range = clampedRange(request.range?.nsRange, textLength: nsText.length) {
            return range
        }

        guard let line = request.line else { return nil }
        return rangeForLineColumn(
            in: nsText,
            line: line,
            column: request.column ?? 1
        )
    }

    private static func clampedRange(_ range: NSRange?, textLength: Int) -> NSRange? {
        guard let range else { return nil }
        guard textLength >= 0 else { return nil }
        let location = min(max(0, range.location), textLength)
        let maxLength = max(0, textLength - location)
        let length = min(max(1, range.length), maxLength)
        return NSRange(location: location, length: length)
    }

    private static func rangeForLineColumn(in text: NSString, line: Int, column: Int) -> NSRange? {
        guard line > 0 else { return nil }
        var currentLine = 1
        var location = 0

        while location <= text.length {
            let lineRange = text.lineRange(for: NSRange(location: location, length: 0))
            if currentLine == line {
                let trimmed = trimmedLineRange(lineRange, in: text)
                let boundedColumn = max(1, column)
                let targetLocation = min(trimmed.location + boundedColumn - 1, trimmed.location + trimmed.length)
                return NSRange(location: targetLocation, length: min(1, max(0, text.length - targetLocation)))
            }

            let next = lineRange.location + lineRange.length
            guard next > location else { break }
            location = next
            currentLine += 1
        }

        return nil
    }

    private static func trimmedLineRange(_ lineRange: NSRange, in text: NSString) -> NSRange {
        var length = lineRange.length
        while length > 0 {
            let character = text.character(at: lineRange.location + length - 1)
            if character == 10 || character == 13 {
                length -= 1
            } else {
                break
            }
        }
        return NSRange(location: lineRange.location, length: length)
    }
}
