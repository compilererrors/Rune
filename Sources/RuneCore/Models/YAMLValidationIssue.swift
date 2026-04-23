import Foundation

public struct YAMLValidationRange: Hashable, Codable, Sendable {
    public let location: Int
    public let length: Int

    public init(location: Int, length: Int) {
        self.location = location
        self.length = length
    }

    public var nsRange: NSRange {
        NSRange(location: location, length: length)
    }
}

public struct YAMLValidationIssue: Identifiable, Hashable, Sendable {
    public enum Severity: String, Codable, Sendable {
        case error
        case warning
    }

    public enum Source: String, Codable, Sendable {
        case syntax
        case kubernetes
        case transport
    }

    public let source: Source
    public let severity: Severity
    public let message: String
    public let line: Int?
    public let column: Int?
    public let range: YAMLValidationRange?

    public init(
        source: Source,
        severity: Severity,
        message: String,
        line: Int? = nil,
        column: Int? = nil,
        range: YAMLValidationRange? = nil
    ) {
        self.source = source
        self.severity = severity
        self.message = message
        self.line = line
        self.column = column
        self.range = range
    }

    public var id: String {
        [
            source.rawValue,
            severity.rawValue,
            message,
            line.map(String.init) ?? "-",
            column.map(String.init) ?? "-",
            range.map { "\($0.location):\($0.length)" } ?? "-"
        ].joined(separator: "|")
    }
}
