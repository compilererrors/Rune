import Foundation

public enum RuneCustomLogPresetSlot: String, CaseIterable, Sendable {
    case one
    case two

    public var ordinal: Int {
        switch self {
        case .one: return 1
        case .two: return 2
        }
    }
}

public enum RuneCustomLogPresetMode: String, CaseIterable, Sendable {
    case lines
    case time

    public var title: String {
        switch self {
        case .lines: return "Line count"
        case .time: return "Time window"
        }
    }
}

public enum RuneCustomLogPresetTimeUnit: String, CaseIterable, Sendable {
    case minutes
    case hours
    case days

    public var title: String {
        switch self {
        case .minutes: return "Minutes"
        case .hours: return "Hours"
        case .days: return "Days"
        }
    }

    public func makeFilter(amount: Int) -> LogTimeFilter {
        let normalized = max(1, amount)
        switch self {
        case .minutes:
            return .lastMinutes(normalized)
        case .hours:
            return .lastHours(normalized)
        case .days:
            return .lastDays(normalized)
        }
    }

    public func shortTitle(amount: Int) -> String {
        let normalized = max(1, amount)
        switch self {
        case .minutes:
            return "\(normalized)m"
        case .hours:
            return "\(normalized)h"
        case .days:
            return "\(normalized)d"
        }
    }
}

public struct RuneCustomLogPresetConfig: Equatable, Sendable {
    public var mode: RuneCustomLogPresetMode
    public var lines: Int
    public var timeValue: Int
    public var timeUnit: RuneCustomLogPresetTimeUnit

    public init(
        mode: RuneCustomLogPresetMode,
        lines: Int,
        timeValue: Int,
        timeUnit: RuneCustomLogPresetTimeUnit
    ) {
        self.mode = mode
        self.lines = max(1, lines)
        self.timeValue = max(1, timeValue)
        self.timeUnit = timeUnit
    }

    public static func defaultValue(for slot: RuneCustomLogPresetSlot) -> Self {
        switch slot {
        case .one:
            return Self(
                mode: .lines,
                lines: 5_000,
                timeValue: 15,
                timeUnit: .minutes
            )
        case .two:
            return Self(
                mode: .time,
                lines: 99_999,
                timeValue: 6,
                timeUnit: .hours
            )
        }
    }

    /// Matches the Settings text fields: blank, non-decimal, or overflowing values resolve to one.
    public static func normalizedPositiveInteger(_ rawValue: String) -> Int {
        max(1, Int(rawValue) ?? 1)
    }

    public var filter: LogTimeFilter {
        switch mode {
        case .lines:
            return .tailLines(lines)
        case .time:
            return timeUnit.makeFilter(amount: timeValue)
        }
    }

    public func title(slot: RuneCustomLogPresetSlot) -> String {
        switch mode {
        case .lines:
            return "Custom \(slot.ordinal) (\(lines) lines)"
        case .time:
            return "Custom \(slot.ordinal) (Last \(timeUnit.shortTitle(amount: timeValue)))"
        }
    }
}
