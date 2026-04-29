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

    @MainActor
    func testHistoryBackAndForwardWorksAfterFirstTrackedNavigation() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(viewModel.canNavigateBack)

        viewModel.setSection(.workloads)

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertTrue(viewModel.canNavigateBack)

        viewModel.navigateBack()

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(viewModel.canNavigateBack)
        XCTAssertTrue(viewModel.canNavigateForward)

        viewModel.navigateForward()

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertTrue(viewModel.canNavigateBack)
        XCTAssertFalse(viewModel.canNavigateForward)
    }

    @MainActor
    func testCommandPaletteCompositeNavigationKeepsInitialBackTarget() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let item = CommandPaletteItem(
            id: "kind:service",
            title: "Services",
            subtitle: "Networking",
            symbolName: "network",
            action: .resourceKind(section: .networking, kind: .service)
        )

        XCTAssertEqual(state.selectedSection, .overview)

        viewModel.executeCommandPaletteItem(item)

        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .service)
        XCTAssertTrue(viewModel.canNavigateBack)

        viewModel.navigateBack()

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(viewModel.canNavigateBack)
        XCTAssertTrue(viewModel.canNavigateForward)
    }

    @MainActor
    func testStoppingPortForwardMarksStartingSessionStoppedImmediately() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let session = PortForwardSession(
            id: "pf-1",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1",
            status: .starting,
            lastMessage: "Starting"
        )
        state.isStartingPortForward = true
        state.upsertPortForwardSession(session)

        viewModel.stopPortForward(session)

        XCTAssertEqual(state.portForwardSessions.first?.status, .stopped)
        XCTAssertEqual(state.portForwardSessions.first?.lastMessage, "Port-forward stopped.")
        XCTAssertFalse(state.isStartingPortForward)
    }

    @MainActor
    func testOpenPortForwardInBrowserOpensActiveLocalURL() {
        let state = RuneAppState()
        let browserOpener = RecordingPortForwardBrowserOpener()
        let viewModel = RuneAppViewModel(state: state, portForwardBrowserOpener: browserOpener)
        let session = PortForwardSession(
            id: "pf-1",
            contextName: "fake",
            namespace: "default",
            targetKind: .service,
            targetName: "web",
            localPort: 8080,
            remotePort: 80,
            address: "0.0.0.0",
            status: .active,
            lastMessage: "Connected"
        )

        XCTAssertEqual(session.browserURL?.absoluteString, "http://127.0.0.1:8080/")

        viewModel.openPortForwardInBrowser(session)

        XCTAssertEqual(browserOpener.openedURLs.map(\.absoluteString), ["http://127.0.0.1:8080/"])
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testOpenPortForwardInBrowserRejectsDisconnectedSession() {
        let state = RuneAppState()
        let browserOpener = RecordingPortForwardBrowserOpener()
        let viewModel = RuneAppViewModel(state: state, portForwardBrowserOpener: browserOpener)
        let session = PortForwardSession(
            id: "pf-1",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1",
            status: .starting,
            lastMessage: "Starting"
        )

        viewModel.openPortForwardInBrowser(session)

        XCTAssertTrue(browserOpener.openedURLs.isEmpty)
        XCTAssertEqual(state.lastError, "Invalid input: Port-forward is not connected yet.")
    }

    @MainActor
    func testSessionLogCacheKeepsReadSegmentsWithResourceBreaks() {
        let state = RuneAppState()
        let firstDate = Date(timeIntervalSince1970: 1_776_000_000)
        let secondDate = Date(timeIntervalSince1970: 1_776_000_030)

        state.appendPodLogRead(
            "first line\n",
            contextName: "aks-prod",
            namespace: "backend",
            podName: "api-0",
            loadedAt: firstDate
        )
        state.appendPodLogRead(
            "second line\n",
            contextName: "aks-prod",
            namespace: "backend",
            podName: "api-0",
            loadedAt: secondDate
        )

        XCTAssertTrue(state.podLogs.contains("Pod  backend/api-0"))
        XCTAssertTrue(state.podLogs.contains("Context: aks-prod"))
        XCTAssertTrue(state.podLogs.contains("first line"))
        XCTAssertTrue(state.podLogs.contains("second line"))
        XCTAssertGreaterThanOrEqual(state.podLogs.components(separatedBy: "────────────────").count, 5)

        state.setPodLogs("")
        state.showCachedPodLogs(contextName: "aks-prod", namespace: "backend", podName: "api-0")
        XCTAssertTrue(state.podLogs.contains("first line"))
        XCTAssertTrue(state.podLogs.contains("second line"))
    }
}

@MainActor
private final class RecordingPortForwardBrowserOpener: PortForwardBrowserOpening {
    private(set) var openedURLs: [URL] = []

    func open(_ url: URL) {
        openedURLs.append(url)
    }
}
