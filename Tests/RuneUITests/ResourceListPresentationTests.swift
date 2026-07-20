@testable import RuneUI
import RuneCore
import XCTest

final class ResourceListPresentationTests: XCTestCase {
    func testLoadingWinsOverFilteredAndTrueEmptyStatesForEveryResourceFamily() {
        let families = [
            "Pods",
            "Deployments",
            "Services",
            "StatefulSets",
            "ConfigMaps",
            "Nodes",
            "RBAC resources",
            "Events",
            "Helm releases",
            "Operator resources"
        ]

        for family in families {
            for filterQuery in ["", "synthetic-missing"] {
                let presentation = ResourceListPresentation.project(
                    isLoading: true,
                    visibleCount: 0,
                    kindTitle: family,
                    filterQuery: filterQuery,
                    scopeDescription: "the synthetic scope"
                )

                guard case let .state(state, action) = presentation else {
                    return XCTFail("Expected a loading state for \(family)")
                }
                guard case let .loading(title, message) = state else {
                    return XCTFail("Loading must precede empty state projection for \(family)")
                }
                XCTAssertEqual(title, "Loading \(family)")
                XCTAssertEqual(message, "Refreshing \(family.lowercased()) for the synthetic scope.")
                XCTAssertNil(action)
            }
        }
    }

    func testLoadedRowsRemainVisibleWhileARefreshRuns() {
        XCTAssertEqual(
            ResourceListPresentation.project(
                isLoading: true,
                visibleCount: 3,
                kindTitle: "Pods",
                filterQuery: "",
                scopeDescription: "namespace synthetic"
            ),
            .content
        )
    }

    func testFilteredEmptyStateOffersClearWithoutChangingTrueEmptyState() {
        let filtered = ResourceListPresentation.project(
            isLoading: false,
            visibleCount: 0,
            kindTitle: "Services",
            filterQuery: "  missing  ",
            scopeDescription: "namespace synthetic"
        )
        guard case let .state(filteredState, action) = filtered else {
            return XCTFail("Expected filtered-empty presentation")
        }
        XCTAssertEqual(
            filteredState,
            .filteredEmpty(
                title: "No services match \"missing\"",
                message: "This filter searches services loaded for namespace synthetic. Clear the filter, switch scope, or choose another kind."
            )
        )
        XCTAssertEqual(action, .clearFilter)

        let empty = ResourceListPresentation.project(
            isLoading: false,
            visibleCount: 0,
            kindTitle: "Events",
            filterQuery: "   ",
            scopeDescription: "the current namespace scope"
        )
        guard case let .state(emptyState, emptyAction) = empty else {
            return XCTFail("Expected true-empty presentation")
        }
        XCTAssertEqual(
            emptyState,
            .empty(
                title: "No events loaded",
                message: "Rune has no events to show for the current namespace scope."
            )
        )
        XCTAssertNil(emptyAction)
    }

    func testFailedFirstLoadOffersRetryWhileCachedRowsRemainVisible() {
        let failedFreshness = RuneResourceListFreshness(
            status: .failed,
            message: "Synthetic request failed."
        )
        let failed = ResourceListPresentation.project(
            isLoading: false,
            visibleCount: 0,
            kindTitle: "ConfigMaps",
            filterQuery: "",
            scopeDescription: "the synthetic scope",
            freshness: failedFreshness
        )

        guard case let .state(state, action) = failed else {
            return XCTFail("Expected failed first load to render a state")
        }
        XCTAssertEqual(
            state,
            .retryableError(
                title: "Could not load configmaps",
                message: "Synthetic request failed."
            )
        )
        XCTAssertEqual(action, .retry)

        XCTAssertEqual(
            ResourceListPresentation.project(
                isLoading: false,
                visibleCount: 3,
                kindTitle: "ConfigMaps",
                filterQuery: "",
                scopeDescription: "the synthetic scope",
                freshness: failedFreshness
            ),
            .content
        )
    }

    func testFreshnessRefreshAndReconnectStatesCannotFallThroughToEmpty() {
        for status in [RuneSnapshotFreshnessStatus.refreshing, .reconnecting] {
            let presentation = ResourceListPresentation.project(
                isLoading: false,
                visibleCount: 0,
                kindTitle: "Secrets",
                filterQuery: "synthetic-missing",
                scopeDescription: "the synthetic scope",
                freshness: RuneResourceListFreshness(status: status, message: "Synthetic refresh")
            )
            guard case let .state(state, action) = presentation else {
                return XCTFail("Expected \(status) to render loading")
            }
            guard case .loading = state else {
                return XCTFail("Expected \(status) to win over filtered empty")
            }
            XCTAssertNil(action)
        }
    }

    func testResourceListPresentationProjectionBenchmarkKPI() {
        let families = ["Pods", "Deployments", "Services", "Events", "Helm releases", "Operator resources"]
        var stateCount = 0
        let start = ProcessInfo.processInfo.systemUptime

        for index in 0..<50_000 {
            let presentation = ResourceListPresentation.project(
                isLoading: index.isMultiple(of: 3),
                visibleCount: index.isMultiple(of: 5) ? 1 : 0,
                kindTitle: families[index % families.count],
                filterQuery: index.isMultiple(of: 2) ? "" : "synthetic",
                scopeDescription: "the synthetic scope"
            )
            if case .state = presentation {
                stateCount += 1
            }
        }

        let elapsed = ProcessInfo.processInfo.systemUptime - start
        XCTAssertGreaterThan(stateCount, 0)
        XCTAssertLessThan(
            elapsed,
            0.25,
            "KPI: 50k loading/content/filtered/empty list projections must stay below 250ms in debug."
        )
    }
}
