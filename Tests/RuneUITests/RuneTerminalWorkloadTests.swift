import AppKit
import Darwin
import RuneCore
import RuneKube
@testable import RuneUI
import SwiftUI
import XCTest

/// The Terminal section under realistic load: several shell tabs with long
/// transcripts, pod logs in the inspector, and output arriving. The app must
/// settle to idle (no layout or render loop), open a tab without stalling,
/// and handle streamed output in time proportional to the new text.
@MainActor
final class RuneTerminalWorkloadTests: XCTestCase {
    // MARK: Idle (no layout or render loops)

    func testTerminalSectionSettlesToIdle() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 2_000)
        defer { harness.window.orderOut(nil) }
        try await assertIdle("the Terminal section with three tabs and logs")
    }

    func testOpeningANewTabWithPlusSettlesToIdle() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 2_000)
        defer { harness.window.orderOut(nil) }
        let sessionsBefore = harness.viewModel.state.terminalSessions.count
        let started = ContinuousClock.now
        try clickShellPlus(in: harness.window)
        try await settle()
        XCTAssertGreaterThan(harness.viewModel.state.terminalSessions.count, sessionsBefore, "+ opened a shell tab")
        XCTAssertLessThan(started.duration(to: .now), .milliseconds(1_500), "+ responds at once")
        try await assertIdle("after + opened a new shell tab")
    }

    /// The user's path: press + again and again (each opens and connects a
    /// shell for the next pod), switching tabs in between, with logs open.
    func testPressingPlusRepeatedlyNeverLoops() async throws {
        let harness = try await makeHarness(sessionCount: 1, transcriptLines: 200, podCount: 8)
        defer { harness.window.orderOut(nil) }
        for round in 0..<6 {
            try clickShellPlus(in: harness.window)
            try await settle(seconds: 0.6)
            try await assertIdle("after + number \(round + 1)", window: 0.6)
        }
        XCTAssertGreaterThanOrEqual(harness.viewModel.state.terminalSessions.count, 6)
        XCTAssertTrue(harness.viewModel.state.terminalSessions.allSatisfy { $0.status == .connected })
    }

    func testStartingAndSwitchingTabsSettlesToIdle() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 2_000)
        defer { harness.window.orderOut(nil) }
        let viewModel = harness.viewModel
        let pods = viewModel.state.pods
        viewModel.startTerminalSession(for: pods[2], container: nil)
        try await settle()
        for session in viewModel.state.terminalSessions {
            viewModel.selectTerminalSession(id: session.id)
            try await settle(seconds: 0.2)
        }
        try await assertIdle("after opening and switching shell tabs")
    }

    /// The log header picks its form from its width. Around each breakpoint
    /// a form change must not resize the pane and flip the form back: that
    /// loop pins the CPU and AppKit aborts ("more Update Constraints passes
    /// than there are views"). Sweeps inspector widths across both breakpoints.
    func testLogHeaderBreakpointsNeverOscillate() async throws {
        let defaults = UserDefaults.standard
        let previous = defaults.object(forKey: RuneSettingsKeys.layoutDetailWidth)
        defer { if let previous { defaults.set(previous, forKey: RuneSettingsKeys.layoutDetailWidth) } else { defaults.removeObject(forKey: RuneSettingsKeys.layoutDetailWidth) } }
        let previousScrollBars = defaults.object(forKey: "AppleShowScrollBars")
        defaults.set("Always", forKey: "AppleShowScrollBars")
        defer { if let previousScrollBars { defaults.set(previousScrollBars, forKey: "AppleShowScrollBars") } else { defaults.removeObject(forKey: "AppleShowScrollBars") } }
        var busyWidths: [Int] = []
        for detailWidth in stride(from: 420, through: 660, by: 5) {
            defaults.set(Double(detailWidth), forKey: RuneSettingsKeys.layoutDetailWidth)
            let harness = try await makeHarness(sessionCount: 2, transcriptLines: 50, settleSeconds: 0.6, windowSize: NSSize(width: 1728, height: 1000))
            let cpuStart = Self.processCPUSeconds()
            try await Task.sleep(nanoseconds: 400_000_000)
            if (Self.processCPUSeconds() - cpuStart) / 0.4 > 0.3 { busyWidths.append(detailWidth) }
            harness.window.orderOut(nil)
        }
        XCTAssertEqual(busyWidths, [], "the log header keeps re-laying out at these inspector widths")
    }

    /// With a mouse attached, macOS shows scroll bars that take space, so a
    /// scroll view that gains or loses one changes its content width. The
    /// shell transcript, the log output and the tab strips must not turn
    /// that into a layout loop. User-sized window, user-sized inspector.
    func testPressingPlusWithAlwaysVisibleScrollBarsNeverLoops() async throws {
        let defaults = UserDefaults.standard
        let previousScrollBars = defaults.object(forKey: "AppleShowScrollBars")
        let previousDetail = defaults.object(forKey: RuneSettingsKeys.layoutDetailWidth)
        defaults.set("Always", forKey: "AppleShowScrollBars")
        defaults.set(680.0, forKey: RuneSettingsKeys.layoutDetailWidth)
        defer {
            if let previousScrollBars { defaults.set(previousScrollBars, forKey: "AppleShowScrollBars") } else { defaults.removeObject(forKey: "AppleShowScrollBars") }
            if let previousDetail { defaults.set(previousDetail, forKey: RuneSettingsKeys.layoutDetailWidth) } else { defaults.removeObject(forKey: RuneSettingsKeys.layoutDetailWidth) }
        }
        print("SCROLLER STYLE", NSScroller.preferredScrollerStyle.rawValue)
        let harness = try await makeHarness(sessionCount: 2, transcriptLines: 300, podCount: 8, windowSize: NSSize(width: 1728, height: 1080))
        defer { harness.window.orderOut(nil) }
        for round in 0..<5 {
            try clickShellPlus(in: harness.window)
            try await settle(seconds: 0.6)
            for session in harness.viewModel.state.terminalSessions.suffix(1) {
                harness.viewModel.terminalSessionInput = "burst"
                harness.viewModel.selectTerminalSession(id: session.id)
                harness.viewModel.sendTerminalSessionInput()
            }
            try await settle(seconds: 0.4)
            try await assertIdle("legacy scroll bars, after + number \(round + 1)", window: 0.8)
        }
    }

    /// The state the app crashed in: many shell tabs and many log tabs, so
    /// both strips overflow and scroll, then one more tab in each.
    func testOverflowingShellAndLogTabStripsNeverLoop() async throws {
        let defaults = UserDefaults.standard
        let previousScrollBars = defaults.object(forKey: "AppleShowScrollBars")
        let previousDetail = defaults.object(forKey: RuneSettingsKeys.layoutDetailWidth)
        defer {
            if let previousScrollBars { defaults.set(previousScrollBars, forKey: "AppleShowScrollBars") } else { defaults.removeObject(forKey: "AppleShowScrollBars") }
            if let previousDetail { defaults.set(previousDetail, forKey: RuneSettingsKeys.layoutDetailWidth) } else { defaults.removeObject(forKey: RuneSettingsKeys.layoutDetailWidth) }
        }
        for scrollBars in ["Always", "WhenScrolling"] {
            for detailWidth in [520.0, 680.0] {
                defaults.set(scrollBars, forKey: "AppleShowScrollBars")
                defaults.set(detailWidth, forKey: RuneSettingsKeys.layoutDetailWidth)
                let harness = try await makeHarness(sessionCount: 1, transcriptLines: 300, settleSeconds: 1.0, podCount: 10, windowSize: NSSize(width: 1728, height: 1080))
                for round in 0..<7 {
                    try clickShellPlus(in: harness.window)
                    try await settle(seconds: 0.35)
                    try clickLogPlus(in: harness.window)
                    try await settle(seconds: 0.35)
                    try await assertIdle("\(scrollBars) scroll bars, inspector \(Int(detailWidth)), round \(round + 1)", window: 0.5)
                }
                harness.window.orderOut(nil)
            }
        }
    }

    // MARK: Output

    /// Output into a long transcript costs time for the new text, not for
    /// the whole transcript on every chunk.
    func testStreamedOutputIntoALongTranscriptStaysFast() async throws {
        let harness = try await makeHarness(sessionCount: 1, transcriptLines: 0)
        defer { harness.window.orderOut(nil) }
        let viewModel = harness.viewModel
        let id = try XCTUnwrap(viewModel.state.terminalSessions.first?.id)
        let long = (0..<60_000).map { "line \($0) of a long build log output for the session\r\n" }.joined()
        viewModel.enqueueTerminalSessionOutput(id: id, text: long)
        try await settle(seconds: 0.3)

        let started = ContinuousClock.now
        for chunk in 0..<200 {
            viewModel.enqueueTerminalSessionOutput(id: id, text: "chunk \(chunk)\r\n")
        }
        let elapsed = started.duration(to: .now)
        #if DEBUG
        let budget = Duration.milliseconds(250)
        #else
        let budget = Duration.milliseconds(60)
        #endif
        XCTAssertLessThan(elapsed, budget, "200 small chunks into a 60,000-line transcript")
        try await settle(seconds: 0.3)
        XCTAssertTrue(viewModel.state.terminalSessions.first?.transcript.hasSuffix("chunk 199\n") ?? false)
    }

    /// The saved workspace holds tabs, not transcripts, so output must not
    /// write it to disk.
    func testOutputDoesNotRewriteTheSavedWorkspace() async throws {
        let store = CountingTerminalWorkspaceStore()
        let harness = try await makeHarness(sessionCount: 2, transcriptLines: 100, store: store, persists: true)
        defer { harness.window.orderOut(nil) }
        let viewModel = harness.viewModel
        let id = try XCTUnwrap(viewModel.state.terminalSessions.first?.id)
        try await settle(seconds: 0.5)
        let savesBefore = store.saveCount
        for chunk in 0..<30 {
            viewModel.enqueueTerminalSessionOutput(id: id, text: "output \(chunk)\r\n")
            try await Task.sleep(nanoseconds: 40_000_000)
        }
        try await settle(seconds: 0.3)
        XCTAssertEqual(store.saveCount, savesBefore, "streamed output wrote the saved workspace \(store.saveCount - savesBefore) times")
    }

    // MARK: Harness

    private struct Harness {
        let window: NSWindow
        let viewModel: RuneAppViewModel
    }

    private func makeHarness(
        sessionCount: Int,
        transcriptLines: Int,
        store: TerminalWorkspaceStateStoring = NoopTerminalWorkspaceStateStore(),
        persists: Bool = false,
        settleSeconds: Double = 1.5,
        podCount: Int = 4,
        windowSize: NSSize = NSSize(width: 1500, height: 900)
    ) async throws -> Harness {
        let defaults = UserDefaults.standard
        let previousSave = defaults.object(forKey: RuneSettingsKeys.saveLastAppState)
        defaults.set(persists, forKey: RuneSettingsKeys.saveLastAppState)
        addTeardownBlock {
            if let previousSave { defaults.set(previousSave, forKey: RuneSettingsKeys.saveLastAppState) } else { defaults.removeObject(forKey: RuneSettingsKeys.saveLastAppState) }
        }

        let state = RuneAppState()
        state.setContexts([KubeContext(name: "sample-cluster")])
        state.selectedContext = KubeContext(name: "sample-cluster")
        state.setNamespaces(["team-alpha"])
        state.selectedNamespace = "team-alpha"
        state.selectedSection = .terminal
        let pods = (0..<podCount).map {
            PodSummary(name: "sample-api-\($0)-7c9db", namespace: "team-alpha", status: "Running", ageDescription: "2h", containerNamesLine: "app")
        }
        state.setPods(pods)
        state.selectedPod = pods[0]
        state.setPodLogs((0..<2_000).map { "2026-01-01T10:00:00Z level=info message=\"request \($0)\"" }.joined(separator: "\n"))
        let transcript = (0..<transcriptLines).map { "$ echo \($0)\n\($0)\n" }.joined() + "$ "
        for index in 0..<sessionCount {
            state.setTerminalSession(PodTerminalSession(
                id: "shell-\(index)",
                contextName: "sample-cluster",
                namespace: "team-alpha",
                podName: pods[index % 2].name,
                containerName: index < 2 ? nil : "app",
                shell: "sh",
                transcript: transcript,
                status: .connected
            ))
        }
        state.selectTerminalSession(id: "shell-0")
        let viewModel = RuneAppViewModel(state: state, terminalBackend: FakePodShellBackend(), terminalWorkspaceStateStore: store)
        viewModel.isSidebarVisible = true
        viewModel.isDetailPaneVisible = true

        let host = NSHostingController(rootView: RuneRootView(
            viewModel: viewModel,
            onLayoutSnapshotChange: { _ in },
            debugDisableBootstrap: true,
            debugDisableLayoutPersistence: !persists
        ))
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1500, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        host.sizingOptions = []
        window.contentViewController = host
        window.setContentSize(windowSize)
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
        try await settle(seconds: settleSeconds)
        return Harness(window: window, viewModel: viewModel)
    }

    private func settle(seconds: Double = 0.8) async throws {
        try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
    }

    /// The app is idle when it uses little CPU over a quiet second: a layout
    /// or render loop keeps the main thread busy without any input.
    private func assertIdle(_ situation: String, window: Double = 1.5, file: StaticString = #filePath, line: UInt = #line) async throws {
        try await settle(seconds: 0.5)
        let cpuStart = Self.processCPUSeconds()
        let wallStart = ContinuousClock.now
        try await Task.sleep(nanoseconds: UInt64(window * 1_000_000_000))
        let cpu = Self.processCPUSeconds() - cpuStart
        let wall = wallStart.duration(to: .now)
        let wallSeconds = Double(wall.components.seconds) + Double(wall.components.attoseconds) / 1e18
        let share = cpu / wallSeconds
        XCTAssertLessThan(share, 0.15, "\(situation): \(Int(share * 100))% CPU while idle (a layout or render loop)", file: file, line: line)
    }

    private static func processCPUSeconds() -> Double {
        var usage = rusage()
        getrusage(RUSAGE_SELF, &usage)
        func seconds(_ time: timeval) -> Double { Double(time.tv_sec) + Double(time.tv_usec) / 1_000_000 }
        return seconds(usage.ru_utime) + seconds(usage.ru_stime)
    }

    /// Clicks the shell tab bar's "+": the trailing slot of the strip that
    /// sits on the top edge of the shell's transcript.
    private func clickShellPlus(in window: NSWindow) throws {
        let transcript = try XCTUnwrap(
            scrollViews(in: window.contentView!)
                .filter { ($0.documentView as? NSTextView)?.isEditable == false }
                .min { $0.convert($0.bounds, to: nil).minX < $1.convert($1.bounds, to: nil).minX },
            "the shell transcript"
        )
        let frame = transcript.convert(transcript.bounds, to: nil)
        let point = NSPoint(x: frame.maxX - TerminalTabLayoutMetrics.addSlotWidth / 2, y: frame.maxY + TerminalTabLayoutMetrics.tabHeight / 2)
        for type in [NSEvent.EventType.leftMouseDown, .leftMouseUp] {
            let event = try XCTUnwrap(NSEvent.mouseEvent(
                with: type, location: point, modifierFlags: [], timestamp: ProcessInfo.processInfo.systemUptime,
                windowNumber: window.windowNumber, context: nil, eventNumber: 0, clickCount: 1, pressure: 1
            ))
            window.sendEvent(event)
        }
    }

    /// Clicks the log tab bar's "+", on the top edge of the log output.
    private func clickLogPlus(in window: NSWindow) throws {
        let output = try XCTUnwrap(
            scrollViews(in: window.contentView!)
                .filter { ($0.documentView as? NSTextView)?.isEditable == false }
                .max { $0.convert($0.bounds, to: nil).minX < $1.convert($1.bounds, to: nil).minX },
            "the log output"
        )
        try click(at: NSPoint(
            x: output.convert(output.bounds, to: nil).maxX - TerminalTabLayoutMetrics.addSlotWidth / 2,
            y: output.convert(output.bounds, to: nil).maxY + TerminalTabLayoutMetrics.tabHeight / 2
        ), in: window)
    }

    private func click(at point: NSPoint, in window: NSWindow) throws {
        for type in [NSEvent.EventType.leftMouseDown, .leftMouseUp] {
            let event = try XCTUnwrap(NSEvent.mouseEvent(
                with: type, location: point, modifierFlags: [], timestamp: ProcessInfo.processInfo.systemUptime,
                windowNumber: window.windowNumber, context: nil, eventNumber: 0, clickCount: 1, pressure: 1
            ))
            window.sendEvent(event)
        }
    }

    private func scrollViews(in view: NSView) -> [NSScrollView] {
        var result: [NSScrollView] = []
        if let scroll = view as? NSScrollView { result.append(scroll) }
        for subview in view.subviews { result += scrollViews(in: subview) }
        return result
    }

    private func pressCommandT(in window: NSWindow) throws {
        let event = try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown, location: .zero, modifierFlags: [.command],
            timestamp: ProcessInfo.processInfo.systemUptime, windowNumber: window.windowNumber,
            context: nil, characters: "t", charactersIgnoringModifiers: "t", isARepeat: false, keyCode: 17
        ))
        NSApp.sendEvent(event)
    }

    private func accessibilityElement(labelled label: String, in view: NSView?) -> NSAccessibilityProtocol? {
        guard let view else { return nil }
        func search(_ element: Any, depth: Int) -> NSAccessibilityProtocol? {
            guard depth < 60, let element = element as? NSAccessibilityProtocol else { return nil }
            if element.accessibilityLabel() == label { return element }
            for child in element.accessibilityChildren() ?? [] {
                if let found = search(child, depth: depth + 1) { return found }
            }
            return nil
        }
        return search(view, depth: 0)
    }
}

final class CountingTerminalWorkspaceStore: TerminalWorkspaceStateStoring, @unchecked Sendable {
    private(set) var saveCount = 0
    func loadTerminalWorkspaceState() -> TerminalWorkspaceStateSnapshot? { nil }
    func saveTerminalWorkspaceState(_ snapshot: TerminalWorkspaceStateSnapshot) { saveCount += 1 }
    func clearTerminalWorkspaceState() {}
}

/// A stand-in shell: connects after a short delay, prints a prompt, echoes
/// each line it is sent and answers "burst" with a screenful of output.
final class FakePodShellBackend: PodTerminalSessionBackend, @unchecked Sendable {
    private let lock = NSLock()
    private var outputs: [String: @Sendable (String) -> Void] = [:]

    func startPodTerminalSession(
        id sessionID: String,
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        container: String?,
        shellCommand: [String],
        onOutput: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) -> Void
    ) async throws {
        try await Task.sleep(nanoseconds: 30_000_000)
        lock.withLock { outputs[sessionID] = onOutput }
        onOutput("\(podName):/app$ ")
    }

    func writeToPodTerminalSession(id: String, text: String) async throws {
        guard let output = lock.withLock({ outputs[id] }) else { return }
        let command = text.trimmingCharacters(in: .newlines)
        if command == "burst" {
            output((0..<300).map { "line \($0)\r\n" }.joined() + "$ ")
        } else {
            output(text.replacingOccurrences(of: "\n", with: "\r\n") + "$ ")
        }
    }

    func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws {}

    func stopPodTerminalSession(id: String) async {
        _ = lock.withLock { outputs.removeValue(forKey: id) }
    }
}

/// The saved terminal workspace holds tabs (pod, container, shell), never
/// transcripts. Output must not count as a change worth saving: saving on
/// every output flush encoded and wrote the workspace file on the main
/// thread many times a second.
@MainActor
final class TerminalWorkspacePersistenceTriggerTests: XCTestCase {
    func testOutputAndStatusDoNotChangeThePersistenceKey() {
        let session = PodTerminalSession(
            id: "shell-a", contextName: "sample-cluster", namespace: "team-alpha",
            podName: "sample-api-0", shell: "sh", transcript: "$ ", status: .connecting
        )
        let later = PodTerminalSession(
            id: "shell-a", contextName: "sample-cluster", namespace: "team-alpha",
            podName: "sample-api-0", shell: "sh", transcript: "$ ls\napp\n$ ", status: .connected
        )
        XCTAssertEqual(
            TerminalWorkspacePersistenceLifecycleModifier.persistenceKey(for: [session]),
            TerminalWorkspacePersistenceLifecycleModifier.persistenceKey(for: [later])
        )
    }

    func testOpeningClosingAndReorderingTabsChangeThePersistenceKey() {
        let a = PodTerminalSession(id: "a", contextName: "c", namespace: "n", podName: "p-a", shell: "sh", transcript: "", status: .connected)
        let b = PodTerminalSession(id: "b", contextName: "c", namespace: "n", podName: "p-b", shell: "sh", transcript: "", status: .connected)
        let key = TerminalWorkspacePersistenceLifecycleModifier.persistenceKey(for:)
        XCTAssertNotEqual(key([a]), key([a, b]))
        XCTAssertNotEqual(key([a, b]), key([b, a]))
    }
}
