import AppKit
import RuneCore
@testable import RuneUI
import SwiftUI
import XCTest

/// Safeguards for the Pod Shell's typing path, to run before any terminal
/// change reaches the user: every printable key typed into the shell's input
/// arrives (none is taken by Rune's single-letter shortcuts), and each
/// keystroke is handled within a small, bounded time.
@MainActor
final class RuneTerminalTypingSafeguardTests: XCTestCase {
    private static let printable = (32...126).map { String(UnicodeScalar(UInt8($0))) }

    func testEveryPrintableKeyReachesTheShellInputThroughTheAppKeyPath() async throws {
        let harness = try await makeHarness()
        defer { RuneTestWindow.close(harness.window) }
        let prompt = harness.prompt

        for character in Self.printable {
            prompt.string = ""
            harness.viewModel.terminalSessionInput = ""
            harness.window.sendEvent(try keyEvent(character, window: harness.window))
            XCTAssertEqual(prompt.string, character, "“\(character)” must reach the shell input")
        }
    }

    /// Rune's single-letter shortcuts (l = Logs, d, s, e, y, /) stand aside
    /// whenever the key window's first responder is an editable text view.
    /// The shell input must be one, or those letters are swallowed (the
    /// round-16 terminal surface was not, which is why "l" could not be typed).
    func testTheShellInputSuspendsRuneShortcuts() async throws {
        let harness = try await makeHarness()
        defer { RuneTestWindow.close(harness.window) }
        let responder = harness.window.firstResponder as? NSTextView
        XCTAssertTrue(responder === harness.prompt)
        let context = RuneRootKeyboardNavigationContext(
            commandPalettePresented: false,
            yamlEditorSheetPresented: false,
            yamlManifestIsEditing: false,
            kubeConfigImportReviewPresented: false,
            addClusterPopoverPresented: false,
            addClusterProviderPresented: false,
            manualNamespaceSheetPresented: false,
            pendingWriteConfirmationPresented: false,
            launchExperiencePresented: false,
            appSheetAttached: false,
            editableTextResponderActive: responder?.isEditable == true
        )
        XCTAssertTrue(context.isSuspended, "typing in the shell input suspends single-letter shortcuts")
    }

    func testKeystrokesStayFastKPI() async throws {
        let harness = try await makeHarness()
        defer { RuneTestWindow.close(harness.window) }
        harness.prompt.string = ""

        let text = String(repeating: "kubectl get pods -L app ", count: 8)
        var slowest = Duration.zero
        let started = ContinuousClock.now
        for character in text {
            let keyStarted = ContinuousClock.now
            harness.window.sendEvent(try keyEvent(String(character), window: harness.window))
            slowest = max(slowest, keyStarted.duration(to: .now))
        }
        let total = started.duration(to: .now)
        XCTAssertEqual(harness.prompt.string, text)
        let perKeyMilliseconds = Double(total.components.attoseconds) / 1e15 / Double(text.count)
            + Double(total.components.seconds) * 1_000 / Double(text.count)
        let slowestMilliseconds = Double(slowest.components.attoseconds) / 1e15 + Double(slowest.components.seconds) * 1_000
        #if DEBUG
        let averageBudget = 8.0
        let slowestBudget = 50.0
        #else
        let averageBudget = 3.0
        let slowestBudget = 20.0
        #endif
        XCTAssertLessThan(perKeyMilliseconds, averageBudget, "KPI: a keystroke is handled in under \(averageBudget) ms on average")
        XCTAssertLessThan(slowestMilliseconds, slowestBudget, "KPI: no keystroke takes longer than \(slowestBudget) ms")
    }

    /// What "+" does: open a tab for the pod, which connects at once
    /// (auto-connect). Here there is no cluster, so each attempt fails fast;
    /// the app must stay responsive and typable through many opens,
    /// switches and closes.
    func testOpeningSwitchingAndClosingTabsStaysResponsiveAndTypable() async throws {
        let harness = try await makeHarness()
        defer { RuneTestWindow.close(harness.window) }
        let viewModel = harness.viewModel
        let pod = try XCTUnwrap(viewModel.state.selectedPod)
        let started = ContinuousClock.now

        for round in 0..<12 {
            viewModel.startTerminalSession(for: pod, container: nil)
            try await Task.sleep(nanoseconds: 80_000_000)
            let ids = viewModel.state.terminalSessions.map(\.id)
            if let other = ids.dropLast().last {
                viewModel.selectTerminalSession(id: other)
                try await Task.sleep(nanoseconds: 30_000_000)
            }
            if let newest = ids.last {
                viewModel.selectTerminalSession(id: newest)
            }
            if round % 3 == 2, let oldest = ids.first(where: { $0 != "demo-shell" }) {
                viewModel.closeTerminalSession(id: oldest)
            }
            try await Task.sleep(nanoseconds: 60_000_000)

            // The main thread answers within a frame or two after each round.
            let probeStarted = ContinuousClock.now
            await Task.yield()
            XCTAssertLessThan(probeStarted.duration(to: .now), .milliseconds(250), "no hang after round \(round)")
        }

        // Back on the connected demo tab: typing still works.
        viewModel.selectTerminalSession(id: "demo-shell")
        try await Task.sleep(nanoseconds: 200_000_000)
        if let prompt = textViews(in: harness.window.contentView!).first(where: \.isEditable) {
            harness.window.makeFirstResponder(prompt)
            prompt.string = ""
            for character in "ls -L" {
                harness.window.sendEvent(try keyEvent(String(character), window: harness.window))
            }
            XCTAssertEqual(prompt.string, "ls -L")
        } else {
            XCTFail("the shell input is available again on the connected tab")
        }
        XCTAssertLessThan(started.duration(to: .now), .seconds(15), "12 open/switch/close rounds stay responsive")
    }

    private func accessibilityElement(labelled label: String, in view: NSView?) -> NSAccessibilityProtocol? {
        guard let view else { return nil }
        func search(_ element: Any, depth: Int) -> NSAccessibilityProtocol? {
            guard depth < 40, let element = element as? NSAccessibilityProtocol else { return nil }
            if element.accessibilityLabel() == label {
                return element
            }
            for child in element.accessibilityChildren() ?? [] {
                if let found = search(child, depth: depth + 1) { return found }
            }
            return nil
        }
        return search(view, depth: 0)
    }

    // MARK: Harness

    private struct Harness {
        let window: NSWindow
        let viewModel: RuneAppViewModel
        let prompt: NSTextView
    }

    private func makeHarness() async throws -> Harness {
        let state = RuneAppState()
        state.setContexts([KubeContext(name: "sample-cluster")])
        state.selectedContext = KubeContext(name: "sample-cluster")
        state.setNamespaces(["team-alpha"])
        state.selectedNamespace = "team-alpha"
        state.selectedSection = .terminal
        let pod = PodSummary(name: "sample-api-0-7c9db", namespace: "team-alpha", status: "Running", ageDescription: "2h")
        state.setPods([pod])
        state.selectedPod = pod
        state.setTerminalSession(PodTerminalSession(
            id: "demo-shell",
            contextName: "sample-cluster",
            namespace: "team-alpha",
            podName: pod.name,
            containerName: nil,
            shell: "sh",
            transcript: "$ ",
            status: .connected,
            lastExitCode: nil,
            lastDiagnostic: nil
        ))
        let viewModel = RuneAppViewModel(state: state)
        viewModel.isSidebarVisible = true
        viewModel.isDetailPaneVisible = true

        let host = NSHostingController(rootView: RuneRootView(viewModel: viewModel, onLayoutSnapshotChange: { _ in }, debugDisableBootstrap: true))
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1280, height: 800),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
        try await Task.sleep(nanoseconds: 1_200_000_000)

        let prompt = try XCTUnwrap(
            textViews(in: host.view).first(where: \.isEditable),
            "the shell input is an editable text view"
        )
        window.makeFirstResponder(prompt)
        try await Task.sleep(nanoseconds: 200_000_000)
        XCTAssertTrue(window.firstResponder === prompt)
        return Harness(window: window, viewModel: viewModel, prompt: prompt)
    }

    private func textViews(in view: NSView) -> [NSTextView] {
        var result: [NSTextView] = []
        if let textView = view as? NSTextView { result.append(textView) }
        for subview in view.subviews { result += textViews(in: subview) }
        return result
    }

    private func keyEvent(_ character: String, window: NSWindow) throws -> NSEvent {
        let isUppercase = character.uppercased() == character && character.lowercased() != character
        return try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown,
            location: .zero,
            modifierFlags: isUppercase ? [.shift] : [],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            characters: character,
            charactersIgnoringModifiers: character.lowercased(),
            isARepeat: false,
            keyCode: Self.keyCode(for: character)
        ))
    }

    /// US key codes for letters (the shortcuts match on characters, but give
    /// AppKit realistic events); other characters use a neutral code.
    private static func keyCode(for character: String) -> UInt16 {
        let codes: [Character: UInt16] = [
            "a": 0, "s": 1, "d": 2, "f": 3, "h": 4, "g": 5, "z": 6, "x": 7, "c": 8, "v": 9, "b": 11, "q": 12,
            "w": 13, "e": 14, "r": 15, "y": 16, "t": 17, "o": 31, "u": 32, "i": 34, "p": 35, "l": 37,
            "j": 38, "k": 40, "n": 45, "m": 46, " ": 49
        ]
        guard let first = character.lowercased().first, let code = codes[first] else { return 50 }
        return code
    }
}
