import Foundation

public enum LogTimeFilter: Equatable, Codable, Sendable {
    case all
    case tailLines(Int)
    case lastMinutes(Int)
    case lastHours(Int)
    case lastDays(Int)
    case since(Date)

    public var kubectlSinceArgument: String? {
        switch self {
        case .all, .tailLines:
            return nil
        case let .lastMinutes(value):
            return "\(value)m"
        case let .lastHours(value):
            return "\(value)h"
        case let .lastDays(value):
            return "\(value * 24)h"
        case let .since(date):
            return ISO8601DateFormatter().string(from: date)
        }
    }

    public var kubectlTailArgument: String? {
        switch self {
        case let .tailLines(lines):
            return String(max(1, lines))
        case .all, .lastMinutes, .lastHours, .lastDays, .since:
            return nil
        }
    }

    public var usesSinceTime: Bool {
        if case .since = self {
            return true
        }
        return false
    }
}
