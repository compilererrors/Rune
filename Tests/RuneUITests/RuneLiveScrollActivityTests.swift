import AppKit
import RuneCore
@testable import RuneUI
import SwiftUI
import XCTest

@MainActor
final class RuneLiveScrollActivityTests: XCTestCase {
    private final class PodInput: ObservableObject {
        @Published var pods: [PodSummary]
        init(pods: [PodSummary]) { self.pods = pods }
    }

    private struct PodTableHost: View {
        @ObservedObject var input: PodInput

        var body: some View {
            AppKitPodTableView(
                pods: input.pods,
                selectedPodID: "default/sample-pod-3",
                selectedPodIDs: [],
                sortColumn: .name,
                sortAscending: true,
                nameColumnWidth: 220,
                canApplyClusterMutations: false,
                isFavorite: { _ in false },
                onSelectPod: { _ in },
                onToggleBulkSelection: { _ in },
                onToggleSort: { _ in },
                onNameColumnWidthChanged: { _ in },
                onToggleFavorite: { _ in },
                onOpenLogs: { _ in },
                onOpenExec: { _ in },
                onOpenDescribe: { _ in },
                onOpenYAML: { _ in },
                onDelete: { _ in }
            )
        }
    }

    override func tearDown() {
        RuneLiveScrollActivity.shared.endScrolling()
        RuneLiveScrollActivity.shared.idleDelay = 0.3
        super.tearDown()
    }

    func testTableHoldsLiveUpdatesUntilScrollingEnds() throws {
        let activity = RuneLiveScrollActivity.shared
        activity.idleDelay = 30
        let pods = (0..<40).map { PodSummary(name: "sample-pod-\($0)", namespace: "default", status: "Running") }
        let input = PodInput(pods: pods)
        let host = NSHostingView(rootView: PodTableHost(input: input).frame(width: 900, height: 400))
        host.frame = NSRect(x: 0, y: 0, width: 900, height: 400)
        pump(host)
        let tableView = try XCTUnwrap(firstTableView(in: host))
        XCTAssertEqual(tableView.numberOfRows, 40)
        XCTAssertEqual(tableView.selectedRow, 3)

        activity.noteScrollEvent()
        for count in [41, 42, 43] {
            input.pods = (0..<count).map { PodSummary(name: "sample-pod-\($0)", namespace: "default", status: "Pending") }
            pump(host)
            XCTAssertEqual(tableView.numberOfRows, 40, "Updates must wait while the user scrolls.")
        }

        activity.endScrolling()
        pump(host)
        XCTAssertEqual(tableView.numberOfRows, 43, "The latest update applies once scrolling ends.")
        XCTAssertEqual(tableView.selectedRow, 3, "Selection survives the deferred update.")
    }

    func testScrollEndsAfterIdleDelayAndReleasesWaiters() async {
        let activity = RuneLiveScrollActivity.shared
        activity.idleDelay = 0.05
        var released = false
        activity.noteScrollEvent()
        XCTAssertTrue(activity.isScrolling)
        let waiter = Task { @MainActor in
            await activity.waitUntilIdle()
            released = true
        }
        try? await Task.sleep(nanoseconds: 20_000_000)
        XCTAssertFalse(released)
        await waiter.value
        XCTAssertTrue(released)
        XCTAssertFalse(activity.isScrolling)
    }

    func testScrollWheelOnResourceTableMarksScrolling() throws {
        let activity = RuneLiveScrollActivity.shared
        activity.idleDelay = 30
        let host = NSHostingView(rootView: PodTableHost(input: PodInput(pods: [])).frame(width: 600, height: 300))
        host.frame = NSRect(x: 0, y: 0, width: 600, height: 300)
        pump(host)
        let scrollView = try XCTUnwrap(resourceScrollView(in: host))
        let event = try XCTUnwrap(CGEvent(scrollWheelEvent2Source: nil, units: .pixel, wheelCount: 1, wheel1: -4, wheel2: 0, wheel3: 0))
        scrollView.scrollWheel(with: try XCTUnwrap(NSEvent(cgEvent: event)))
        XCTAssertTrue(activity.isScrolling)
    }

    private func pump(_ view: NSView) {
        for _ in 0..<4 {
            view.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.02))
        }
    }

    private func firstTableView(in view: NSView) -> NSTableView? {
        if let tableView = view as? NSTableView { return tableView }
        for subview in view.subviews {
            if let found = firstTableView(in: subview) { return found }
        }
        return nil
    }

    private func resourceScrollView(in view: NSView) -> RuneAppKitResourceListScrollView? {
        if let scrollView = view as? RuneAppKitResourceListScrollView { return scrollView }
        for subview in view.subviews {
            if let found = resourceScrollView(in: subview) { return found }
        }
        return nil
    }
}
