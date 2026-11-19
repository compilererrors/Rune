import AppKit
import Combine
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
        defer { RuneTestWindow.close(harness.window) }
        try await assertIdle("the Terminal section with three tabs and logs")
    }

    func testOpeningANewTabWithPlusSettlesToIdle() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 2_000)
        defer { RuneTestWindow.close(harness.window) }
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
        defer { RuneTestWindow.close(harness.window) }
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
        defer { RuneTestWindow.close(harness.window) }
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
        let defaults = VolatileTestDefaults(self)
        try await defaults.setScrollBars("Always")
        var busyWidths: [Int] = []
        for detailWidth in stride(from: 420, through: 660, by: 5) {
            defaults.set(Double(detailWidth), forKey: RuneSettingsKeys.layoutDetailWidth)
            let harness = try await makeHarness(sessionCount: 2, transcriptLines: 50, settleSeconds: 0.6, windowSize: NSSize(width: 1728, height: 1000))
            // A layout loop pins the CPU for as long as the window is up;
            // other work in the test process is over in a moment. A width
            // counts only if it is still busy in a second window.
            var isBusy = true
            for _ in 0..<2 where isBusy {
                let cpuStart = Self.processCPUSeconds()
                try await Task.sleep(nanoseconds: 400_000_000)
                isBusy = (Self.processCPUSeconds() - cpuStart) / 0.4 > 0.3
            }
            if isBusy { busyWidths.append(detailWidth) }
            RuneTestWindow.close(harness.window)
        }
        XCTAssertEqual(busyWidths, [], "the log header keeps re-laying out at these inspector widths")
    }

    // MARK: User-flow KPIs

    /// Shell output from the pod to the screen: 1,500 chunks through the
    /// emulator, the throttled flush and the transcript view.
    func testTerminalOutputEndToEndKPI() async throws {
        let harness = try await makeHarness(sessionCount: 1, transcriptLines: 50)
        defer { RuneTestWindow.close(harness.window) }
        let transcript = try XCTUnwrap(shellTranscriptTextView(in: harness.window))
        var samples: [Double] = []
        for round in 0..<3 {
            let marker = "end-of-round-\(round)"
            let started = ContinuousClock.now
            for chunk in 0..<500 {
                harness.viewModel.enqueueTerminalSessionOutput(id: "shell-0", text: "round \(round) chunk \(chunk): build step finished in \(chunk % 13) ms\r\n")
            }
            harness.viewModel.enqueueTerminalSessionOutput(id: "shell-0", text: marker + "\r\n$ ")
            let deadline = Date().addingTimeInterval(15)
            while !transcript.string.contains(marker), Date() < deadline {
                try await Task.sleep(nanoseconds: 2_000_000)
            }
            harness.window.displayIfNeeded()
            XCTAssertTrue(transcript.string.contains(marker), "round \(round) reached the screen")
            samples.append(RuneBenchmarkRecord.seconds(started.duration(to: .now)))
        }
        RuneBenchmarkRecord.emit("terminal_output_end_to_end", samplesSeconds: samples, iterations: 501)
        let median = samples.sorted()[samples.count / 2]
        print("KPI terminal output end to end ms per 501 chunks:", samples.map { Int($0 * 1000) })
        XCTAssertLessThan(median, 0.3, "501 chunks reached the screen in \(median) s")
    }

    /// Idle: the Terminal section with tabs and logs open uses next to no CPU
    /// and does not keep waking up.
    func testIdleCPUAndWakeupsKPI() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 2_000)
        defer { RuneTestWindow.close(harness.window) }
        try await settle(seconds: 1.0)
        var shares: [Double] = []
        var wakeupsPerSecond: [Double] = []
        for _ in 0..<3 {
            let cpuStart = Self.processCPUSeconds()
            let wakeupsStart = Self.wakeups()
            let started = ContinuousClock.now
            try await Task.sleep(nanoseconds: 1_000_000_000)
            let wall = RuneBenchmarkRecord.seconds(started.duration(to: .now))
            shares.append((Self.processCPUSeconds() - cpuStart) / wall)
            wakeupsPerSecond.append(Double(Self.wakeups() - wakeupsStart) / wall)
        }
        RuneBenchmarkRecord.emit("idle_cpu_seconds_per_second", samplesSeconds: shares)
        let cpu = shares.sorted()[1]
        let wakeups = wakeupsPerSecond.sorted()[1]
        print("KPI idle CPU share:", String(format: "%.3f", cpu), "wakeups/s:", Int(wakeups))
        XCTAssertLessThan(cpu, 0.05, "idle CPU \(Int(cpu * 100))%")
        XCTAssertLessThan(wakeups, 100, "idle wakeups \(Int(wakeups))/s")
    }

    /// Switching between ten shell tabs with long transcripts.
    func testShellTabSwitchKPI() async throws {
        let harness = try await makeHarness(sessionCount: 10, transcriptLines: 2_000)
        defer { RuneTestWindow.close(harness.window) }
        for index in 0..<10 {
            harness.viewModel.enqueueTerminalSessionOutput(id: "shell-\(index)", text: "tab-marker-\(index)\r\n$ ")
        }
        try await settle(seconds: 0.8)
        let transcript = { self.shellTranscriptTextView(in: harness.window) }
        var samples: [Double] = []
        for step in 0..<20 {
            let index = (step * 3) % 10
            let started = ContinuousClock.now
            harness.viewModel.selectTerminalSession(id: "shell-\(index)")
            let deadline = Date().addingTimeInterval(5)
            while transcript()?.string.contains("tab-marker-\(index)\n") != true, Date() < deadline {
                try await Task.sleep(nanoseconds: 1_000_000)
            }
            harness.window.contentView?.layoutSubtreeIfNeeded()
            harness.window.displayIfNeeded()
            samples.append(RuneBenchmarkRecord.seconds(started.duration(to: .now)))
        }
        RuneBenchmarkRecord.emit("shell_tab_switch", samplesSeconds: samples)
        let median = samples.sorted()[samples.count / 2] * 1000
        print("KPI shell tab switch ms:", samples.map { Int($0 * 1000) })
        // About 45 ms in release, most of it SwiftUI updating the window for
        // the new selection. Laying out the whole transcript again, as
        // TextKit 2 did, took 110–140 ms.
        XCTAssertLessThan(median, 65, "switching tabs takes \(Int(median)) ms")
    }

    /// Resizing the whole window with the Terminal section and logs shown.
    func testWindowLiveResizeKPI() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 2_000, windowSize: NSSize(width: 1500, height: 900))
        defer { RuneTestWindow.close(harness.window) }
        var samples: [Double] = []
        for step in 0..<40 {
            let width = 1500 - CGFloat(step < 20 ? step : 40 - step) * 12
            let started = ContinuousClock.now
            harness.window.setContentSize(NSSize(width: width, height: 900))
            harness.window.contentView?.layoutSubtreeIfNeeded()
            harness.window.displayIfNeeded()
            samples.append(RuneBenchmarkRecord.seconds(started.duration(to: .now)))
            try await Task.sleep(nanoseconds: 16_000_000)
        }
        RuneBenchmarkRecord.emit("window_live_resize_step", samplesSeconds: samples)
        let median = samples.sorted()[samples.count / 2] * 1000
        print("KPI window resize step ms median:", Int(median), "max:", Int((samples.max() ?? 0) * 1000))
        XCTAssertLessThan(median, 50, "a window resize step takes \(Int(median)) ms")
    }

    /// Memory for ten shell tabs with 5,000-line transcripts and a long log.
    func testMemoryWithManyTabsAndLongLogsKPI() async throws {
        let before = Self.physicalFootprintMegabytes()
        let harness = try await makeHarness(sessionCount: 10, transcriptLines: 5_000)
        defer { RuneTestWindow.close(harness.window) }
        harness.viewModel.state.setPodLogs((0..<20_000).map { "2026-01-01T10:00:00Z level=info message=\"request \($0)\"" }.joined(separator: "\n"))
        for index in 0..<10 { harness.viewModel.selectTerminalSession(id: "shell-\(index)"); try await settle(seconds: 0.1) }
        try await settle(seconds: 1.0)
        let growth = Self.physicalFootprintMegabytes() - before
        print("KPI memory growth MB:", Int(growth))
        XCTAssertLessThan(growth, 250, "ten long tabs and a 20,000-line log grew memory by \(Int(growth)) MB")
    }

    /// Interrupt and timer wakeups of this process, as Activity Monitor's
    /// Idle Wake Ups column counts them.
    private static func wakeups() -> Int {
        var info = task_power_info_data_t()
        var count = mach_msg_type_number_t(MemoryLayout<task_power_info_data_t>.size / MemoryLayout<integer_t>.size)
        let result = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                task_info(mach_task_self_, task_flavor_t(TASK_POWER_INFO), $0, &count)
            }
        }
        return result == KERN_SUCCESS ? Int(info.task_interrupt_wakeups + info.task_platform_idle_wakeups) : 0
    }

    private static func physicalFootprintMegabytes() -> Double {
        var info = task_vm_info_data_t()
        var count = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size)
        let result = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count)
            }
        }
        return result == KERN_SUCCESS ? Double(info.phys_footprint) / 1_048_576 : 0
    }

    // MARK: Cmd-W

    func testCommandWClosesTheActiveShellTabWhereTheShellHasFocus() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 20)
        defer { RuneTestWindow.close(harness.window) }
        let transcript = try XCTUnwrap(shellTranscriptTextView(in: harness.window))
        harness.window.makeFirstResponder(transcript)
        harness.viewModel.selectTerminalSession(id: "shell-1")
        try await settle(seconds: 0.3)

        try pressCommandW(in: harness.window)
        try await settle(seconds: 0.3)
        XCTAssertEqual(harness.viewModel.state.terminalSessions.map(\.id), ["shell-0", "shell-2"], "Cmd-W closed the active shell tab")
        XCTAssertTrue(harness.window.isVisible, "and not the window")

        try pressCommandW(in: harness.window)
        try await settle(seconds: 0.3)
        XCTAssertEqual(harness.viewModel.state.terminalSessions.count, 1)
        try pressCommandW(in: harness.window)
        try await settle(seconds: 0.3)
        XCTAssertEqual(harness.viewModel.state.terminalSessions.count, 1, "the last tab stays; Cmd-W is the window's again")
    }

    func testCommandWInTheLogsLeavesTheShellTabsAlone() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 20)
        defer { RuneTestWindow.close(harness.window) }
        // Focus the shell first, then click into the inspector, as a user would.
        harness.window.makeFirstResponder(try XCTUnwrap(shellTranscriptTextView(in: harness.window)))
        let splitView = try XCTUnwrap(findSplitView(in: harness.window.contentView!))
        let inspector = splitView.arrangedSubviews[2].convert(splitView.arrangedSubviews[2].bounds, to: nil)
        try appClick(at: NSPoint(x: inspector.midX, y: inspector.minY + 40), in: harness.window)
        try await settle(seconds: 0.3)

        try pressCommandW(in: harness.window)
        try await settle(seconds: 0.3)
        XCTAssertEqual(harness.viewModel.state.terminalSessions.count, 3, "Cmd-W in the logs is about the log tabs")
    }

    // MARK: Terminal shortcuts

    func testCommandKClearsTheShellOnlyWhenTheShellHasFocus() async throws {
        let harness = try await makeHarness(sessionCount: 2, transcriptLines: 20)
        defer { RuneTestWindow.close(harness.window) }
        harness.window.makeFirstResponder(try XCTUnwrap(shellTranscriptTextView(in: harness.window)))
        try press("k", 40, .command, in: harness.window)
        try await settle(seconds: 0.3)
        XCTAssertEqual(harness.viewModel.state.terminalSession?.transcript, "", "Cmd-K clears the shell")
        XCTAssertFalse(harness.viewModel.state.isCommandPalettePresented, "and does not open the palette")

        harness.window.makeFirstResponder(nil)
        harness.viewModel.selectTerminalSession(id: "shell-1")
        try press("k", 40, .command, in: harness.window)
        try await settle(seconds: 0.3)
        XCTAssertFalse(harness.viewModel.state.terminalSession?.transcript.isEmpty ?? true, "without shell focus Cmd-K is the palette's")
    }

    func testCommandPlusMinusAndZeroSizeTheTerminalText() async throws {
        let defaults = UserDefaults.standard
        let previous = defaults.persistedTestValue(forKey: RuneSettingsKeys.terminalFontSize)
        defer { if let previous { defaults.set(previous, forKey: RuneSettingsKeys.terminalFontSize) } else { defaults.removeObject(forKey: RuneSettingsKeys.terminalFontSize) } }
        defaults.set(12.0, forKey: RuneSettingsKeys.terminalFontSize)
        let harness = try await makeHarness(sessionCount: 1, transcriptLines: 20)
        defer { RuneTestWindow.close(harness.window) }
        harness.window.makeFirstResponder(try XCTUnwrap(shellTranscriptTextView(in: harness.window)))

        try press("=", 24, .command, in: harness.window)
        try press("=", 24, .command, in: harness.window)
        XCTAssertEqual(defaults.double(forKey: RuneSettingsKeys.terminalFontSize), 14)
        try press("-", 27, .command, in: harness.window)
        XCTAssertEqual(defaults.double(forKey: RuneSettingsKeys.terminalFontSize), 13)
        try press("0", 29, .command, in: harness.window)
        XCTAssertEqual(defaults.double(forKey: RuneSettingsKeys.terminalFontSize), RuneSettingsKeys.terminalFontSizeDefault)
    }

    func testControlTabCyclesTheShellTabs() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 20)
        defer { RuneTestWindow.close(harness.window) }
        harness.window.makeFirstResponder(try XCTUnwrap(shellTranscriptTextView(in: harness.window)))
        harness.viewModel.selectTerminalSession(id: "shell-2")
        try press("\t", 48, .control, in: harness.window)
        XCTAssertEqual(harness.viewModel.state.activeTerminalSessionID, "shell-0", "wraps to the first tab")
        try press("\t", 48, [.control, .shift], in: harness.window)
        XCTAssertEqual(harness.viewModel.state.activeTerminalSessionID, "shell-2")
    }

    func testCommandShiftTReopensTheShellTabJustClosed() async throws {
        let harness = try await makeHarness(sessionCount: 3, transcriptLines: 20)
        defer { RuneTestWindow.close(harness.window) }
        harness.window.makeFirstResponder(try XCTUnwrap(shellTranscriptTextView(in: harness.window)))
        harness.viewModel.selectTerminalSession(id: "shell-1")
        let closedPod = try XCTUnwrap(harness.viewModel.state.terminalSession?.podName)
        try pressCommandW(in: harness.window)
        try await settle(seconds: 0.3)
        XCTAssertEqual(harness.viewModel.state.terminalSessions.count, 2)

        try press("T", 17, [.command, .shift], in: harness.window)
        try await settle(seconds: 0.6)
        XCTAssertEqual(harness.viewModel.state.terminalSessions.count, 3, "Cmd-Shift-T brought the tab back")
        XCTAssertEqual(harness.viewModel.state.terminalSession?.podName, closedPod, "for the same pod, and selected it")
    }

    func testCommandTInTheLogsOpensALogTabNotAShell() async throws {
        let harness = try await makeHarness(sessionCount: 2, transcriptLines: 20)
        defer { RuneTestWindow.close(harness.window) }
        let splitView = try XCTUnwrap(findSplitView(in: harness.window.contentView!))
        let inspector = splitView.arrangedSubviews[2].convert(splitView.arrangedSubviews[2].bounds, to: nil)
        try appClick(at: NSPoint(x: inspector.midX, y: inspector.minY + 40), in: harness.window)
        try await settle(seconds: 0.3)
        let podBefore = harness.viewModel.state.selectedPod?.id

        try press("t", 17, .command, in: harness.window)
        try await settle(seconds: 0.4)
        XCTAssertEqual(harness.viewModel.state.terminalSessions.count, 2, "no shell tab")
        XCTAssertNotEqual(harness.viewModel.state.selectedPod?.id, podBefore, "a log tab for another pod")
    }

    private func press(_ characters: String, _ keyCode: UInt16, _ flags: NSEvent.ModifierFlags, in window: NSWindow) throws {
        let event = try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown, location: .zero, modifierFlags: flags, timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber, context: nil, characters: characters, charactersIgnoringModifiers: characters,
            isARepeat: false, keyCode: keyCode
        ))
        NSApp.sendEvent(event)
    }

    private func pressCommandW(in window: NSWindow) throws {
        let event = try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown, location: .zero, modifierFlags: .command, timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber, context: nil, characters: "w", charactersIgnoringModifiers: "w",
            isARepeat: false, keyCode: 13
        ))
        NSApp.sendEvent(event)
    }

    private func shellTranscriptTextView(in window: NSWindow) -> NSTextView? {
        scrollViews(in: window.contentView!)
            .compactMap { $0.documentView as? NSTextView }
            .first { String(describing: type(of: $0)).contains("TerminalTranscript") }
    }

    /// A click through the app, so the root view's event monitors see it.
    private func appClick(at point: NSPoint, in window: NSWindow) throws {
        for type in [NSEvent.EventType.leftMouseDown, .leftMouseUp] {
            let event = try XCTUnwrap(NSEvent.mouseEvent(
                with: type, location: point, modifierFlags: [], timestamp: ProcessInfo.processInfo.systemUptime,
                windowNumber: window.windowNumber, context: nil, eventNumber: 0, clickCount: 1, pressure: 1
            ))
            NSApp.sendEvent(event)
        }
    }

    // MARK: Resizing the inspector

    /// Dragging the inspector's divider must stay smooth with data in it:
    /// each step only lays the panes out at the new width. Nothing may
    /// publish app state, re-render the transcript or message the shell
    /// per step; the terminal learns its new size once the drag settles.
    func testDraggingTheInspectorDividerOnlyLaysOut() async throws {
        let harness = try await makeHarness(sessionCount: 2, transcriptLines: 400, windowSize: NSSize(width: 1500, height: 900))
        defer { RuneTestWindow.close(harness.window) }
        harness.viewModel.enqueueTerminalSessionOutput(id: "shell-0", text: (0..<200).map { "line \($0)\r\n" }.joined() + "$ ")
        try await settle(seconds: 0.6)

        for section in [RuneSection.terminal, .workloads] {
            harness.viewModel.state.selectedSection = section
            try await settle(seconds: 0.8)
            try await waitUntilQuiet(harness)
            let result = try await dragInspectorDivider(in: harness)
            print("INSPECTOR DRAG \(section): publishes=\(result.publishes) shellResizes=\(result.shellResizes) newControls=\(result.newControls) medianMs=\(result.medianMs) maxMs=\(result.maxMs)")
            XCTAssertEqual(result.publishes, 0, "\(section): app state published \(result.publishes) times during the drag")
            XCTAssertEqual(result.shellResizes, 0, "\(section): the shell was resized \(result.shellResizes) times during the drag")
            XCTAssertEqual(result.newControls, [], "\(section): native controls were recreated during the drag")
            // Timing varies with machine load; the checks above are the guard.
            // This only catches a gross regression (two frames at 60 Hz).
            XCTAssertLessThan(result.medianMs, 33, "\(section): a drag step takes \(result.medianMs) ms")
        }
    }

    private func dragInspectorDivider(in harness: Harness) async throws -> (publishes: Int, shellResizes: Int, newControls: [String], medianMs: Double, maxMs: Double) {
        func controls() -> [ObjectIdentifier: String] {
            var found: [ObjectIdentifier: String] = [:]
            func walk(_ view: NSView) {
                if (view is NSControl && !(view is NSScroller)) || view is NSTextView { found[ObjectIdentifier(view)] = String(describing: type(of: view)) }
                view.subviews.forEach(walk)
            }
            walk(harness.window.contentView!)
            return found
        }
        let controlsBefore = controls()
        let splitView = try XCTUnwrap(findSplitView(in: harness.window.contentView!), "the three-pane split view")
        var publishes = 0
        let stateSink = harness.viewModel.state.objectWillChange.sink { _ in publishes += 1 }
        let modelSink = harness.viewModel.objectWillChange.sink { _ in publishes += 1 }
        defer { stateSink.cancel(); modelSink.cancel() }
        let resizesBefore = harness.backend.resizes
        let start = splitView.arrangedSubviews[2].frame.minX - splitView.dividerThickness
        var stepMs: [Double] = []
        for step in 0..<40 {
            let offset = CGFloat(step < 20 ? -step : step - 40) * 8
            let began = ContinuousClock.now
            splitView.setPosition(start + offset, ofDividerAt: 1)
            harness.window.contentView?.layoutSubtreeIfNeeded()
            harness.window.displayIfNeeded()
            let took = began.duration(to: .now)
            stepMs.append(Double(took.components.attoseconds) / 1e15 + Double(took.components.seconds) * 1000)
            try await Task.sleep(nanoseconds: 16_000_000)
        }
        let newControls = controls().filter { controlsBefore[$0.key] == nil }.map(\.value).sorted()
        let drag = (publishes, harness.backend.resizes - resizesBefore)
        // Once the drag settles the shell learns its final size, once.
        try await Task.sleep(nanoseconds: 400_000_000)
        XCTAssertLessThanOrEqual(harness.backend.resizes - resizesBefore, 1, "the settled size is reported once")
        RuneBenchmarkRecord.emit("inspector_drag_step", samplesSeconds: stepMs.map { $0 / 1000 })
        stepMs.sort()
        return (drag.0, drag.1, newControls, stepMs[stepMs.count / 2], stepMs.last ?? 0)
    }

    /// Switching section starts loads that can land a burst of updates
    /// late; the drag starts on a view with no updates for half a second.
    private func waitUntilQuiet(_ harness: Harness) async throws {
        var last = ContinuousClock.now
        let stateSink = harness.viewModel.state.objectWillChange.sink { _ in last = .now }
        let modelSink = harness.viewModel.objectWillChange.sink { _ in last = .now }
        defer { stateSink.cancel(); modelSink.cancel() }
        let deadline = ContinuousClock.now + .seconds(5)
        while last.duration(to: .now) < .milliseconds(500), ContinuousClock.now < deadline {
            try await Task.sleep(nanoseconds: 50_000_000)
        }
    }

    private func findSplitView(in view: NSView) -> NSSplitView? {
        if let split = view as? NSSplitView, split.isVertical, split.arrangedSubviews.count == 3 { return split }
        for subview in view.subviews {
            if let found = findSplitView(in: subview) { return found }
        }
        return nil
    }

    /// With a mouse attached, macOS shows scroll bars that take space, so a
    /// scroll view that gains or loses one changes its content width. The
    /// shell transcript, the log output and the tab strips must not turn
    /// that into a layout loop. User-sized window, user-sized inspector.
    func testPressingPlusWithAlwaysVisibleScrollBarsNeverLoops() async throws {
        let defaults = VolatileTestDefaults(self)
        try await defaults.setScrollBars("Always")
        defaults.set(680.0, forKey: RuneSettingsKeys.layoutDetailWidth)
        let harness = try await makeHarness(sessionCount: 2, transcriptLines: 300, podCount: 8, windowSize: NSSize(width: 1728, height: 1080))
        defer { RuneTestWindow.close(harness.window) }
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
        let defaults = VolatileTestDefaults(self)
        for scrollBars in ["Always", "WhenScrolling"] {
            try await defaults.setScrollBars(scrollBars)
            for detailWidth in [520.0, 680.0] {
                defaults.set(detailWidth, forKey: RuneSettingsKeys.layoutDetailWidth)
                let harness = try await makeHarness(sessionCount: 1, transcriptLines: 300, settleSeconds: 1.0, podCount: 10, windowSize: NSSize(width: 1728, height: 1080))
                for round in 0..<7 {
                    try clickShellPlus(in: harness.window)
                    try await settle(seconds: 0.35)
                    try clickLogPlus(in: harness.window)
                    try await settle(seconds: 0.35)
                    try await assertIdle("\(scrollBars) scroll bars, inspector \(Int(detailWidth)), round \(round + 1)", window: 0.5)
                }
                RuneTestWindow.close(harness.window)
            }
        }
    }

    // MARK: Output

    /// Output into a long transcript costs time for the new text, not for
    /// the whole transcript on every chunk.
    func testStreamedOutputIntoALongTranscriptStaysFast() async throws {
        let harness = try await makeHarness(sessionCount: 1, transcriptLines: 0)
        defer { RuneTestWindow.close(harness.window) }
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
        defer { RuneTestWindow.close(harness.window) }
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
        let backend: FakePodShellBackend
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
        let previousSave = defaults.persistedTestValue(forKey: RuneSettingsKeys.saveLastAppState)
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
        let backend = FakePodShellBackend()
        let viewModel = RuneAppViewModel(state: state, terminalBackend: backend, bookmarkManager: .inMemory(), terminalWorkspaceStateStore: store)
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
        // Take the app down with the test. A window that is only ordered out
        // keeps its whole view tree, which goes on answering every settings
        // change and slows the tests that run after this one.
        addTeardownBlock { @MainActor in
            window.contentViewController = nil
            window.close()
        }
        try await settle(seconds: settleSeconds)
        return Harness(window: window, viewModel: viewModel, backend: backend)
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

    private var resizeCount = 0
    var resizes: Int { lock.withLock { resizeCount } }

    func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws {
        lock.withLock { resizeCount += 1 }
    }

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
