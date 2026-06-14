import XCTest
@testable import RuneCore
@testable import RuneUI

final class TerminalLogTabsWorkflowTests: XCTestCase {
    func testLogTabStateAddsSelectsClosesAndFallsBackBetweenPods() throws {
        let pods = [
            pod("api-0"),
            pod("worker-0"),
            pod("job-0", status: "Succeeded")
        ]
        var state = TerminalPodLogTabState()

        state.ensureTab(for: pods[0])
        XCTAssertEqual(state.tabs.map(\.podName), ["api-0"])
        XCTAssertEqual(state.activePod(in: pods, fallback: nil)?.name, "api-0")

        state.add(preferredPod: pods[1])
        XCTAssertEqual(state.tabs.map(\.podName), ["api-0", "worker-0"])
        XCTAssertEqual(state.activePod(in: pods, fallback: nil)?.name, "worker-0")

        let firstTabID = state.tabs[0].id
        let selectedPod = state.select(id: firstTabID, availablePods: pods)
        XCTAssertEqual(selectedPod?.name, "api-0")
        XCTAssertEqual(state.activeTabID, firstTabID)
        XCTAssertEqual(state.selectedPodID, pods[0].id)

        let fallbackAfterClose = state.close(id: firstTabID, availablePods: pods, fallbackPod: pods[2])
        XCTAssertEqual(fallbackAfterClose?.name, "worker-0")
        XCTAssertEqual(state.tabs.map(\.podName), ["worker-0"])
        XCTAssertEqual(state.activePod(in: pods, fallback: nil)?.name, "worker-0")

        let remainingTabID = try XCTUnwrap(state.tabs.first?.id)
        let fallbackWhenEmpty = state.close(id: remainingTabID, availablePods: pods, fallbackPod: pods[2])
        XCTAssertEqual(fallbackWhenEmpty?.name, "job-0")
        XCTAssertTrue(state.tabs.isEmpty)
        XCTAssertNil(state.activeTabID)
        XCTAssertEqual(state.selectedPodID, pods[2].id)
    }

    func testLogTabStateReconcilesRemovedPodsWithoutLeakingDeadTabs() {
        let pods = [pod("api-0"), pod("worker-0"), pod("job-0")]
        var state = TerminalPodLogTabState()
        state.add(preferredPod: pods[0])
        state.add(preferredPod: pods[1])
        state.add(preferredPod: pods[2])

        let activeBeforeRemoval = state.activeTabID
        XCTAssertEqual(state.activePod(in: pods, fallback: nil)?.name, "job-0")

        state.reconcile(availablePods: [pods[0], pods[1]], fallbackPod: pods[0])

        XCTAssertFalse(state.tabs.contains { $0.podName == "job-0" })
        XCTAssertNotEqual(state.activeTabID, activeBeforeRemoval)
        XCTAssertEqual(state.activePod(in: [pods[0], pods[1]], fallback: nil)?.name, "api-0")
        XCTAssertEqual(state.selectedPodID, pods[0].id)
    }

    func testPreferredPodForNewTabUsesFallbackThenFavoritesThenNameOrder() {
        let pods = [
            pod("api-0"),
            pod("worker-0"),
            pod("batch-0")
        ]
        var state = TerminalPodLogTabState()

        XCTAssertEqual(
            state.preferredPodForNewTab(pods: pods, fallbackPod: pods[1], isFavorite: { _ in false })?.name,
            "worker-0"
        )

        state.add(preferredPod: pods[1])

        XCTAssertEqual(
            state.preferredPodForNewTab(pods: pods, fallbackPod: pods[1], isFavorite: { $0.name == "api-0" })?.name,
            "api-0"
        )

        state.add(preferredPod: pods[0])

        XCTAssertEqual(
            state.preferredPodForNewTab(pods: pods, fallbackPod: pods[1], isFavorite: { $0.name == "api-0" })?.name,
            "batch-0"
        )
    }

    func testLogTabPresentationsPreserveClosedOverPodNamesAndFavoriteState() {
        let pods = [pod("api-0"), pod("worker-0")]
        var state = TerminalPodLogTabState()
        state.add(preferredPod: pods[0])
        state.add(preferredPod: pods[1])

        let presentations = state.presentations(pods: pods, isFavorite: { $0.name == "worker-0" })

        XCTAssertEqual(presentations.map(\.title), ["api-0", "worker-0"])
        XCTAssertEqual(presentations.map(\.subtitle), ["default", "default"])
        XCTAssertEqual(presentations.map(\.isFavorite), [false, true])
        XCTAssertEqual(presentations[1].accessibilityLabel, "worker-0, namespace default")
        XCTAssertEqual(presentations[1].helpText, "Logs for default/worker-0")
    }

    func testUpdatingActiveTabChangesOnlyThatTabTarget() {
        let pods = [pod("api-0"), pod("worker-0"), pod("job-0")]
        var state = TerminalPodLogTabState()
        state.add(preferredPod: pods[0])
        state.add(preferredPod: pods[1])

        state.updateActive(to: pods[2])

        XCTAssertEqual(state.tabs.map(\.podName), ["api-0", "job-0"])
        XCTAssertEqual(state.activePod(in: pods, fallback: nil)?.name, "job-0")
        XCTAssertEqual(state.selectedPodID, pods[2].id)
    }

    func testActiveLogTabDoesNotFollowShellPodFallbackChanges() {
        let pods = [pod("api-0"), pod("worker-0")]
        var state = TerminalPodLogTabState()
        state.ensureTab(for: pods[0])

        XCTAssertEqual(state.activePod(in: pods, fallback: pods[1])?.name, "api-0")

        state.reconcile(availablePods: pods, fallbackPod: pods[1])

        XCTAssertEqual(state.activePod(in: pods, fallback: pods[1])?.name, "api-0")
        XCTAssertEqual(state.selectedPodID, pods[0].id)
    }

    private func pod(_ name: String, status: String = "Running") -> PodSummary {
        PodSummary(name: name, namespace: "default", status: status, containerNamesLine: "app")
    }
}
