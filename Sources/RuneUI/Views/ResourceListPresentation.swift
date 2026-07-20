import Foundation
import RuneCore

enum ResourceListStateAction: Sendable, Equatable {
    case clearFilter
    case retry
}

enum ResourceListPresentation: Sendable, Equatable {
    case content
    case state(RuneContentState, action: ResourceListStateAction?)

    static func project(
        isLoading: Bool,
        visibleCount: Int,
        kindTitle: String,
        filterQuery: String,
        scopeDescription: String,
        freshness: RuneResourceListFreshness? = nil
    ) -> ResourceListPresentation {
        if visibleCount > 0 {
            return .content
        }

        let resourceName = kindTitle.lowercased()
        if isLoading || freshness?.status == .refreshing || freshness?.status == .reconnecting {
            return .state(
                .loading(
                    title: "Loading \(kindTitle)",
                    message: "Refreshing \(resourceName) for \(scopeDescription)."
                ),
                action: nil
            )
        }

        if freshness?.status == .failed {
            let failureMessage = freshness?.message.trimmingCharacters(in: .whitespacesAndNewlines)
            let message = failureMessage.flatMap { $0.isEmpty ? nil : $0 }
                ?? "Refresh failed for \(scopeDescription). Try again."
            return .state(
                .retryableError(
                    title: "Could not load \(resourceName)",
                    message: message
                ),
                action: .retry
            )
        }

        let query = filterQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        if !query.isEmpty {
            return .state(
                .filteredEmpty(
                    title: "No \(resourceName) match \"\(query)\"",
                    message: "This filter searches \(resourceName) loaded for \(scopeDescription). Clear the filter, switch scope, or choose another kind."
                ),
                action: .clearFilter
            )
        }

        return .state(
            .empty(
                title: "No \(resourceName) loaded",
                message: "Rune has no \(resourceName) to show for \(scopeDescription)."
            ),
            action: nil
        )
    }
}
