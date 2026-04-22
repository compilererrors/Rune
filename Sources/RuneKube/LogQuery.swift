import Foundation
import RuneCore

public enum LogQueryProfile: Sendable {
    case pod
    case unifiedPerPod
}

struct ResolvedLogQuery: Equatable, Sendable {
    let since: String?
    let usesSinceTime: Bool
    let tailLines: Int
}

extension LogTimeFilter {
    func resolvedLogQuery(profile: LogQueryProfile) -> ResolvedLogQuery {
        switch self {
        case .all:
            return ResolvedLogQuery(since: nil, usesSinceTime: false, tailLines: 200)
        case let .tailLines(lines):
            return ResolvedLogQuery(since: nil, usesSinceTime: false, tailLines: max(1, lines))
        case .lastMinutes, .lastHours, .lastDays, .since:
            let cappedTail: Int
            switch profile {
            case .pod:
                cappedTail = 2_000
            case .unifiedPerPod:
                cappedTail = 400
            }
            return ResolvedLogQuery(
                since: kubectlSinceArgument,
                usesSinceTime: usesSinceTime,
                tailLines: cappedTail
            )
        }
    }
}
