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

    public var filter: LogTimeFilter {
        switch mode {
        case .lines:
            if lines >= 99_999 {
                return .all
            }
            return .tailLines(max(1, lines))
        case .time:
            return timeUnit.makeFilter(amount: timeValue)
        }
    }

    public func title(slot: RuneCustomLogPresetSlot) -> String {
        switch mode {
        case .lines:
            if lines >= 99_999 {
                return "Custom \(slot.ordinal) (Since beginning)"
            }
            return "Custom \(slot.ordinal) (\(lines) lines)"
        case .time:
            return "Custom \(slot.ordinal) (Last \(timeUnit.shortTitle(amount: timeValue)))"
        }
    }
}
