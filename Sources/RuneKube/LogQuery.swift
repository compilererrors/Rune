import Foundation
import RuneCore

public enum LogQueryProfile: Sendable {
    case pod
    case unifiedPerPod
}

struct ResolvedLogQuery: Equatable, Sendable {
    let sinceSeconds: Int?
    let sinceTime: Date?
    let tailLines: Int?
}

extension LogTimeFilter {
    func resolvedLogQuery(profile _: LogQueryProfile) -> ResolvedLogQuery {
        switch self {
        case .all:
            return ResolvedLogQuery(sinceSeconds: nil, sinceTime: nil, tailLines: nil)
        case let .tailLines(lines):
            return ResolvedLogQuery(sinceSeconds: nil, sinceTime: nil, tailLines: max(1, lines))
        case .lastMinutes, .lastHours, .lastDays:
            return ResolvedLogQuery(
                sinceSeconds: kubernetesSinceSeconds,
                sinceTime: nil,
                tailLines: nil
            )
        case let .since(date):
            return ResolvedLogQuery(sinceSeconds: nil, sinceTime: date, tailLines: nil)
        }
    }
}
