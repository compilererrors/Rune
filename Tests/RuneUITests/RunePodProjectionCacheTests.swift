import RuneCore
@testable import RuneUI
import XCTest

@MainActor
final class RunePodProjectionCacheTests: XCTestCase {
    func testPodFavoriteMatcherAgreesWithPerPodFavoriteLookup() {
        let suiteName = TestDefaultsSuite.name("RunePodProjectionCacheTests.favorites.\(UUID().uuidString)")
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removeTestSuite(named: suiteName) }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context")
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: .inMemory(),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: defaults)
        )
        let pods = [
            PodSummary(name: "alpha", namespace: "default", status: "Running"),
            PodSummary(name: "bravo", namespace: "team-a", status: "Running"),
            PodSummary(name: "charlie", namespace: "team-a", status: "Pending"),
            PodSummary(name: "delta", namespace: "", status: "Running")
        ]
        state.setPods(pods)
        XCTAssertTrue(pods.allSatisfy { !viewModel.podFavoriteMatcher()($0) })

        viewModel.toggleFavoriteResource(kind: .pod, namespace: "team-a", name: "bravo")
        viewModel.toggleFavoriteResource(kind: .pod, namespace: "", name: "delta")
        viewModel.toggleFavoriteResource(kind: .deployment, namespace: "default", name: "alpha")

        let matcher = viewModel.podFavoriteMatcher()
        for pod in pods {
            XCTAssertEqual(
                matcher(pod),
                viewModel.isFavoriteResource(kind: .pod, namespace: pod.namespace, name: pod.name),
                pod.name
            )
        }
        XCTAssertEqual(viewModel.visiblePods.prefix(2).map(\.name).sorted(), ["bravo", "delta"])
    }

    func testPodOrderIsReusedOnlyWhileSortKeysAreUnchanged() {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context")
        let viewModel = RuneAppViewModel(state: state, bookmarkManager: .inMemory())
        var pods = (0..<200).map {
            PodSummary(name: "pod-\($0)", namespace: "default", status: $0 % 3 == 0 ? "Pending" : "Running", totalRestarts: $0 % 5)
        }
        state.setPods(pods)
        let nameOrder = viewModel.visiblePods.map(\.name)

        pods[7] = PodSummary(name: "pod-7", namespace: "default", status: "Failed", totalRestarts: 9)
        state.setPods(pods)
        XCTAssertEqual(viewModel.visiblePods.map(\.name), nameOrder, "Changing an unsorted field keeps the name order.")
        XCTAssertEqual(viewModel.visiblePods.first { $0.name == "pod-7" }?.status, "Failed", "Reused order must carry fresh values.")

        viewModel.togglePodSort(.restarts)
        let expected = pods.sorted { lhs, rhs in
            lhs.totalRestarts != rhs.totalRestarts
                ? lhs.totalRestarts > rhs.totalRestarts
                : lhs.name.localizedStandardCompare(rhs.name) == .orderedAscending
        }.map(\.name)
        XCTAssertEqual(viewModel.visiblePods.map(\.name), expected)

        pods[3] = PodSummary(name: "pod-3", namespace: "default", status: "Pending", totalRestarts: 20)
        state.setPods(pods)
        XCTAssertEqual(viewModel.visiblePods.first?.name, "pod-3", "A changed sort key must re-sort.")
    }
}
