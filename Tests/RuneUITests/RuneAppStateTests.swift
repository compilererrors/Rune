import XCTest
@testable import RuneCore
@testable import RuneUI

final class RuneAppStateTests: XCTestCase {
    @MainActor
    func testOverviewClusterUsageCanUpdateWithoutReplacingOverviewSnapshot() {
        let state = RuneAppState()
        state.setOverviewSnapshot(
            pods: [],
            deploymentsCount: 3,
            servicesCount: 2,
            ingressesCount: 1,
            configMapsCount: 4,
            cronJobsCount: 5,
            nodesCount: 6,
            clusterCPUPercent: nil,
            clusterMemoryPercent: nil,
            events: []
        )

        state.setOverviewClusterUsage(cpuPercent: 17, memoryPercent: 42)

        XCTAssertEqual(state.overviewClusterCPUPercent, 17)
        XCTAssertEqual(state.overviewClusterMemoryPercent, 42)
        XCTAssertEqual(state.overviewDeploymentsCount, 3)
        XCTAssertEqual(state.overviewServicesCount, 2)
        XCTAssertEqual(state.overviewNodesCount, 6)
    }

    @MainActor
    func testNamespaceOptionsAreAlphabetical() {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "cluster")
        state.setNamespaces(["zeta", "default", "Alpha", "beta"])
        state.selectedNamespace = "zeta"
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.namespaceOptions, ["Alpha", "beta", "default", "zeta"])
    }

    @MainActor
    func testContextMenuOptionsAreAlphabeticalWithoutFavoriteGrouping() {
        let state = RuneAppState()
        state.setContexts([
            KubeContext(name: "prod"),
            KubeContext(name: "alpha"),
            KubeContext(name: "Beta")
        ])
        let viewModel = RuneAppViewModel(state: state)
        state.setFavoriteContextNames(["prod"])

        XCTAssertEqual(viewModel.contextMenuOptions.map(\.name), ["alpha", "Beta", "prod"])
        XCTAssertEqual(viewModel.visibleContexts.map(\.name), ["prod", "alpha", "Beta"])
    }
}
