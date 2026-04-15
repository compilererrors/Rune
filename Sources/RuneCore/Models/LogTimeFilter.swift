import Foundation

public enum LogTimeFilter: Equatable, Codable, Sendable {
    case lastMinutes(Int)
    case lastHours(Int)
    case lastDays(Int)
    case since(Date)

    public var kubectlArgument: String {
        switch self {
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

    public var usesSinceTime: Bool {
        if case .since = self {
            return true
        }
        return false
    }
}
