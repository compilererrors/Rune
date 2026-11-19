import AppKit
import RuneCore
@testable import RuneUI
import SwiftUI
import XCTest

/// Scrolls large tables and documents one frame step at a time in a real
/// window and records the main-thread cost of each step, including display.
@MainActor
final class RuneScrollSmoothnessBenchmarkTests: XCTestCase {
    private final class PodInput: ObservableObject {
        @Published var pods: [PodSummary]
        init(pods: [PodSummary]) { self.pods = pods }
    }

    private struct PodTableHost: View {
        @ObservedObject var input: PodInput

        var body: some View {
            AppKitPodTableView(
                pods: input.pods,
                selectedPodID: input.pods.first?.id,
                selectedPodIDs: [],
                sortColumn: .name,
                sortAscending: true,
                nameColumnWidth: 260,
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

    private struct FrameStats {
        let p50: Double
        let p95: Double
        let max: Double
        let hitches: Int
        let frames: Int
        let samples: [Double]

        init(_ samples: [Double]) {
            self.samples = samples
            let sorted = samples.sorted()
            func percentile(_ value: Double) -> Double {
                sorted[min(sorted.count - 1, Int((Double(sorted.count - 1) * value).rounded()))]
            }
            p50 = percentile(0.5)
            p95 = percentile(0.95)
            max = sorted.last ?? 0
            hitches = samples.filter { $0 > 1.0 / 60.0 }.count
            frames = samples.count
        }

        var summary: String {
            String(format: "p50 %.2fms p95 %.2fms max %.2fms hitches %d/%d", p50 * 1000, p95 * 1000, max * 1000, hitches, frames)
        }
    }

    func testScrollingLargeTablesAndDocumentsStaysInsideFrameBudgetKPI() throws {
        let statuses = ["Running", "Pending", "Failed", "Succeeded"]
        let reasons: [String?] = [nil, nil, "CrashLoopBackOff", "ContainerStatusUnknown", nil, "Init:0/2"]
        var pods = (0..<5_000).map { index in
            PodSummary(
                name: "sample-workload-\(index)-7c9db",
                namespace: "team-\(index % 7)",
                status: statuses[index % statuses.count],
                statusReason: reasons[index % reasons.count],
                totalRestarts: index % 9,
                ageDescription: "\(index % 30 + 1)d",
                cpuUsage: "\(index % 400)m",
                memoryUsage: "\(64 + index % 512)Mi"
            )
        }
        let podInput = PodInput(pods: pods)
        let profileSteps = Int(ProcessInfo.processInfo.environment["RUNE_SCROLL_PROFILE_STEPS"] ?? "") ?? 240
        let podStats = try scroll(view: PodTableHost(input: podInput), steps: profileSteps, pixelsPerStep: 42, beforeStep: { step in
            guard step % 6 == 5 else { return }
            let row = (step * 37) % pods.count
            pods[row] = PodSummary(
                name: pods[row].name,
                namespace: pods[row].namespace,
                status: step % 12 == 5 ? "Running" : "Pending",
                totalRestarts: pods[row].totalRestarts + 1,
                ageDescription: pods[row].ageDescription
            )
            podInput.pods = pods
        })

        let rootState = RuneAppState()
        let rootContext = KubeContext(name: "sample-context")
        rootState.setContexts([rootContext])
        rootState.selectedContext = rootContext
        rootState.selectedSection = .workloads
        rootState.selectedWorkloadKind = .pod
        rootState.setPods(pods)
        rootState.selectedPod = pods[0]
        let rootViewModel = RuneAppViewModel(state: rootState, bookmarkManager: .inMemory())
        rootViewModel.isSidebarVisible = true
        rootViewModel.isDetailPaneVisible = true
        let rootSteps = Int(ProcessInfo.processInfo.environment["RUNE_SCROLL_ROOT_STEPS"] ?? "") ?? 240
        let rootStats = try scroll(
            view: RuneRootView(viewModel: rootViewModel, onLayoutSnapshotChange: nil, debugDisableBootstrap: true),
            steps: rootSteps,
            pixelsPerStep: 42,
            width: 1440,
            height: 900,
            findScrollView: { self.resourceTableScrollView(in: $0) },
            beforeStep: { step in
            guard step % 6 == 5 else { return }
            let row = (step * 37) % pods.count
            pods[row] = PodSummary(
                name: pods[row].name,
                namespace: pods[row].namespace,
                status: step % 12 == 5 ? "Running" : "Pending",
                totalRestarts: pods[row].totalRestarts + 1,
                ageDescription: pods[row].ageDescription
            )
            let snapshot = pods
            RuneLiveScrollActivity.shared.performWhenIdle(owner: rootState) {
                rootState.setPods(snapshot)
            }
            }
        )
        XCTAssertEqual(rootState.pods, pods, "Updates held during the scroll apply once it ends.")

        let events = (0..<3_000).map { index in
            EventSummary(
                eventIdentifier: "event-\(index)",
                eventNamespace: "team-\(index % 5)",
                type: index % 9 == 0 ? "Warning" : "Normal",
                reason: index % 9 == 0 ? "BackOff" : "Scheduled",
                objectName: "sample-workload-\(index)-7c9db",
                message: "Synthetic event message \(index) for scroll measurement with a longer tail of text",
                lastTimestamp: "\(index % 59 + 1)m",
                involvedKind: "Pod",
                involvedNamespace: "team-\(index % 5)"
            )
        }
        let eventStats = try scroll(
            view: AppKitEventListView(
                events: events,
                selectedEventID: nil,
                sortColumn: .lastSeen,
                sortAscending: false,
                onSelectEvent: { _ in },
                onToggleSort: { _ in }
            ),
            steps: 240,
            pixelsPerStep: 42
        )

        let yaml = (["apiVersion: v1", "kind: ConfigMap", "metadata:", "  name: sample", "data:"]
            + (0..<12_000).map { "  key-\($0): \"synthetic value \($0) with enough text to wrap the viewport\"" })
            .joined(separator: "\n")
        let yamlStats = try scroll(
            view: AppKitManifestTextView(text: .constant(yaml), isEditable: false, contentStyle: .yaml, showsLineNumbers: true),
            steps: 240,
            pixelsPerStep: 60
        )

        print("KPI scroll pods: \(podStats.summary)")
        print("KPI scroll pods without updates: \(FrameStats(podStats.samples.enumerated().filter { $0.offset % 6 != 5 }.map { $0.element }).summary)")
        print("KPI scroll pods update frames: \(FrameStats(podStats.samples.enumerated().filter { $0.offset % 6 == 5 }.map { $0.element }).summary)")
        print("KPI scroll pods in root view: \(rootStats.summary)")
        print("KPI scroll pods in root view without updates: \(FrameStats(rootStats.samples.enumerated().filter { $0.offset % 6 != 5 }.map { $0.element }).summary)")
        print("KPI scroll pods in root view update frames: \(FrameStats(rootStats.samples.enumerated().filter { $0.offset % 6 == 5 }.map { $0.element }).summary)")
        print("KPI scroll events: \(eventStats.summary)")
        print("KPI scroll yaml: \(yamlStats.summary)")

        #if DEBUG
        let p95Budget = 0.030
        let hitchBudget = 0.20
        #else
        let p95Budget = 0.014
        let hitchBudget = 0.02
        #endif
        for (name, stats) in [("pods", podStats), ("root pods", rootStats), ("events", eventStats), ("yaml", yamlStats)] {
            XCTAssertLessThan(stats.p95, p95Budget, "KPI: \(name) scroll p95 frame cost \(stats.summary)")
            XCTAssertLessThanOrEqual(
                Double(stats.hitches) / Double(stats.frames),
                hitchBudget,
                "KPI: \(name) scroll hitch rate \(stats.summary)"
            )
        }
    }

    private func scroll<V: View>(
        view: V,
        steps: Int,
        pixelsPerStep: CGFloat,
        width: CGFloat = 1100,
        height: CGFloat = 760,
        findScrollView: ((NSView) -> NSScrollView?)? = nil,
        beforeStep: (Int) -> Void = { _ in }
    ) throws -> FrameStats {
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: width, height: height),
            styleMask: [.titled, .resizable],
            backing: .buffered,
            defer: false
        )
        let host = NSHostingView(rootView: view)
        host.frame = NSRect(x: 0, y: 0, width: width, height: height)
        window.contentView = host
        window.orderFront(nil)
        defer { RuneTestWindow.close(window) }
        settle(window, seconds: findScrollView == nil ? 0.6 : 1.5)

        let scrollView = try XCTUnwrap((findScrollView ?? { self.firstScrollView(in: $0) })(host))
        let clipView = scrollView.contentView
        let maximumY = max(0, (scrollView.documentView?.frame.height ?? 0) - clipView.bounds.height)
        XCTAssertGreaterThan(maximumY, 2_000, "document must be long enough to measure scrolling")

        var samples: [Double] = []
        samples.reserveCapacity(steps)
        var y: CGFloat = 0
        var direction: CGFloat = 1
        for step in 0..<steps {
            RuneLiveScrollActivity.shared.noteScrollEvent()
            beforeStep(step)
            let started = ContinuousClock.now
            y += pixelsPerStep * direction
            if y >= maximumY || y <= 0 {
                direction *= -1
                y = min(max(y, 0), maximumY)
            }
            clipView.scroll(to: NSPoint(x: clipView.bounds.origin.x, y: y))
            scrollView.reflectScrolledClipView(clipView)
            RunLoop.main.run(mode: .default, before: Date())
            window.contentView?.layoutSubtreeIfNeeded()
            window.displayIfNeeded()
            CATransaction.flush()
            samples.append(started.duration(to: .now).frameSeconds)
        }
        RuneLiveScrollActivity.shared.endScrolling()
        settle(window, seconds: 0.2)
        return FrameStats(samples)
    }

    private func settle(_ window: NSWindow, seconds: TimeInterval) {
        let deadline = Date().addingTimeInterval(seconds)
        while Date() < deadline {
            window.contentView?.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.01))
        }
        window.displayIfNeeded()
    }

    private func resourceTableScrollView(in view: NSView) -> NSScrollView? {
        if let scrollView = view as? RuneAppKitResourceListScrollView, scrollView.documentView is NSTableView { return scrollView }
        for subview in view.subviews {
            if let found = resourceTableScrollView(in: subview) { return found }
        }
        return nil
    }

    private func firstScrollView(in view: NSView) -> NSScrollView? {
        if let scrollView = view as? NSScrollView, scrollView.documentView != nil { return scrollView }
        for subview in view.subviews {
            if let found = firstScrollView(in: subview) { return found }
        }
        return nil
    }
}

private extension Duration {
    var frameSeconds: Double {
        let parts = components
        return Double(parts.seconds) + Double(parts.attoseconds) / 1e18
    }
}
