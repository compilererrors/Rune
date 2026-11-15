import XCTest
@testable import RuneCore
@testable import RuneUI

/// Drag-and-drop reordering and pinning for shell and log tabs.
final class TerminalTabReorderTests: XCTestCase {
    private struct Item: Identifiable, Equatable { let id: String }

    func testMovingOntoATabTakesItsSlotInBothDirections() {
        var items = ["a", "b", "c", "d"].map(Item.init)
        XCTAssertTrue(items.moveElement(id: "a", onto: "c"))
        XCTAssertEqual(items.map(\.id), ["b", "c", "a", "d"])
        XCTAssertTrue(items.moveElement(id: "d", onto: "b"))
        XCTAssertEqual(items.map(\.id), ["d", "b", "c", "a"])
        XCTAssertFalse(items.moveElement(id: "b", onto: "b"))
        XCTAssertFalse(items.moveElement(id: "missing", onto: "b"))
        XCTAssertEqual(items.map(\.id), ["d", "b", "c", "a"])
    }

    @MainActor
    func testShellTabsReorderWithoutChangingTheActiveSession() {
        let state = RuneAppState()
        for name in ["one", "two", "three"] {
            state.setTerminalSession(session(name))
        }
        state.selectTerminalSession(id: "two")
        state.moveTerminalSession(id: "three", onto: "one")
        XCTAssertEqual(state.terminalSessions.map(\.id), ["three", "one", "two"])
        XCTAssertEqual(state.activeTerminalSessionID, "two")
        XCTAssertEqual(state.terminalSession?.id, "two")
    }

    func testPinnedLogTabsSitFirstAndStayAheadWhenDragged() {
        let pods = ["api-0", "worker-0", "job-0", "db-0"].map(pod)
        var state = TerminalPodLogTabState()
        for pod in pods { state.add(preferredPod: pod) }
        let pinned: Set<String> = [pods[2].id]
        let isPinned: (TerminalPodLogTab) -> Bool = { pinned.contains($0.podID) }

        let shown = state.presentations(pods: pods) { pinned.contains($0.id) }
        XCTAssertEqual(shown.map(\.title), ["job-0", "api-0", "worker-0", "db-0"])

        state.groupPinnedFirst(isPinned: isPinned)
        XCTAssertEqual(state.tabs.map(\.podName), ["job-0", "api-0", "worker-0", "db-0"])

        // An unpinned tab dropped on the pinned one lands right after it.
        state.move(id: state.tabs[3].id, onto: state.tabs[0].id, isPinned: isPinned)
        XCTAssertEqual(state.tabs.map(\.podName), ["job-0", "db-0", "api-0", "worker-0"])

        // Reordering among unpinned tabs works as usual.
        state.move(id: state.tabs[1].id, onto: state.tabs[3].id, isPinned: isPinned)
        XCTAssertEqual(state.tabs.map(\.podName), ["job-0", "api-0", "worker-0", "db-0"])
    }

    func testPinSymbolSettingMapsToSymbols() {
        XCTAssertEqual(RuneTabPinSymbol.pin.systemImage(isPinned: true), "pin.fill")
        XCTAssertEqual(RuneTabPinSymbol.pin.systemImage(isPinned: false), "pin")
        XCTAssertEqual(RuneTabPinSymbol.star.systemImage(isPinned: true), "star.fill")
        XCTAssertEqual(RuneTabPinSymbol(rawValue: "unknown"), nil)
    }

    private func session(_ id: String) -> PodTerminalSession {
        PodTerminalSession(
            id: id, contextName: "synthetic-context", namespace: "default",
            podName: "pod-\(id)", shell: "sh", transcript: "", status: .connected
        )
    }

    private func pod(_ name: String) -> PodSummary {
        PodSummary(name: name, namespace: "default", status: "Running", containerNamesLine: "app")
    }
}
