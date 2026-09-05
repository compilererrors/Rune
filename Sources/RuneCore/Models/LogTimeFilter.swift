import Foundation

public enum LogTimeFilter: Equatable, Codable, Sendable {
    case all
    case tailLines(Int)
    case lastMinutes(Int)
    case lastHours(Int)
    case lastDays(Int)
    case since(Date)

    public var kubernetesSinceArgument: String? {
        switch self {
        case .all, .tailLines:
            return nil
        case let .lastMinutes(value):
            return "\(Self.normalizedPositive(value))m"
        case let .lastHours(value):
            return "\(Self.normalizedPositive(value))h"
        case let .lastDays(value):
            return "\(Self.saturatedPositiveProduct(value, multiplier: 24))h"
        case let .since(date):
            return ISO8601DateFormatter().string(from: date)
        }
    }

    public var kubernetesSinceSeconds: Int? {
        switch self {
        case .all, .tailLines, .since:
            return nil
        case let .lastMinutes(value):
            return Self.saturatedPositiveProduct(value, multiplier: 60)
        case let .lastHours(value):
            return Self.saturatedPositiveProduct(value, multiplier: 60 * 60)
        case let .lastDays(value):
            return Self.saturatedPositiveProduct(value, multiplier: 24 * 60 * 60)
        }
    }

    public var kubernetesTailArgument: String? {
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

    private static func normalizedPositive(_ value: Int) -> Int {
        max(1, value)
    }

    private static func saturatedPositiveProduct(_ value: Int, multiplier: Int) -> Int {
        let normalized = normalizedPositive(value)
        let result = normalized.multipliedReportingOverflow(by: multiplier)
        return result.overflow ? Int.max : result.partialValue
    }
}
