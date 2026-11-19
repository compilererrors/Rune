import AppKit
import RuneCore
import RuneKube
@testable import RuneUI
import XCTest

/// What makes the pod shell usable as a terminal: the keys people edit
/// commands with, full-screen tools' cursor keys, a sensible container in
/// multi-container pods, and the demo shell behaving like a shell.
@MainActor
final class TerminalShellUsabilityTests: XCTestCase {
    // MARK: Key encoding

    private func key(_ keyCode: UInt16, _ characters: String, ignoring: String? = nil, _ flags: NSEvent.ModifierFlags = []) -> NSEvent {
        NSEvent.keyEvent(
            with: .keyDown, location: .zero, modifierFlags: flags, timestamp: 0, windowNumber: 0, context: nil,
            characters: characters, charactersIgnoringModifiers: ignoring ?? characters, isARepeat: false, keyCode: keyCode
        )!
    }

    func testKeysEncodeLikeTerminalApp() {
        let arrow = "\u{F700}"
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(126, arrow)), "\u{1B}[A")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(123, "\u{F702}", [.option])), "\u{1B}b", "Option-Left: back a word")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(124, "\u{F703}", [.option])), "\u{1B}f", "Option-Right: forward a word")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(123, "\u{F702}", [.control])), "\u{1B}[1;5D")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(126, arrow, [.shift])), "\u{1B}[1;2A")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(48, "\t", [.shift])), "\u{1B}[Z", "Shift-Tab")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(51, "\u{7F}", [.option])), "\u{1B}\u{7F}", "Option-Delete: delete a word")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(122, "\u{F704}")), "\u{1B}OP", "F1")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(96, "\u{F708}")), "\u{1B}[15~", "F5")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(111, "\u{F70F}")), "\u{1B}[24~", "F12")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(0, "a")), "a")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(8, "\u{3}", ignoring: "c", [.control])), "\u{3}", "Ctrl-C")
    }

    func testOptionTypesTheCharacterItMakesOnTheKeyboard() {
        // Swedish and many other layouts type @ | [ ] { } \ $ with Option.
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(19, "@", ignoring: "2", [.option])), "@")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(26, "|", ignoring: "7", [.option])), "|")
        XCTAssertEqual(TerminalKeyEncoding.sequence(for: key(21, "$", ignoring: "4", [.option])), "$")
    }

    func testKeysLeftForTheAppAreNotSent() {
        XCTAssertNil(TerminalKeyEncoding.sequence(for: key(13, "w", [.command])))
        XCTAssertNil(TerminalKeyEncoding.sequence(for: key(105, "\u{F710}")), "F13 has no terminal meaning here")
    }

    func testApplicationCursorModeSendsSS3CursorKeys() {
        XCTAssertEqual(TerminalKeyEncoding.applyingCursorKeyMode("\u{1B}[A", application: true), "\u{1B}OA")
        XCTAssertEqual(TerminalKeyEncoding.applyingCursorKeyMode("\u{1B}[D", application: true), "\u{1B}OD")
        XCTAssertEqual(TerminalKeyEncoding.applyingCursorKeyMode("\u{1B}[H", application: true), "\u{1B}OH")
        XCTAssertEqual(TerminalKeyEncoding.applyingCursorKeyMode("\u{1B}[1;5A", application: true), "\u{1B}[1;5A", "modified keys keep CSI")
        XCTAssertEqual(TerminalKeyEncoding.applyingCursorKeyMode("\u{1B}[A", application: false), "\u{1B}[A")
        XCTAssertEqual(TerminalKeyEncoding.applyingCursorKeyMode("ls", application: true), "ls")
    }

    func testCursorKeysFollowTheShellsMode() async throws {
        let backend = RecordingShell()
        let viewModel = try connectedViewModel(backend: backend)
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "\u{1B}[?1h")
        viewModel.sendTerminalControlSequence("\u{1B}[A")
        try await waitUntil { backend.writes.count == 1 }
        XCTAssertEqual(backend.writes.last, "\u{1B}OA", "vim asked for application cursor keys")

        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "\u{1B}[?1l")
        viewModel.sendTerminalControlSequence("\u{1B}[A")
        try await waitUntil { backend.writes.count == 2 }
        XCTAssertEqual(backend.writes.last, "\u{1B}[A")
    }

    // MARK: Containers

    func testAShellInAMultiContainerPodUsesTheFirstContainerAndSaysSo() async throws {
        let backend = RecordingShell()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, terminalBackend: backend, bookmarkManager: .inMemory())
        state.setContexts([KubeContext(name: "sample-cluster")])
        state.selectedContext = KubeContext(name: "sample-cluster")
        state.setNamespaces(["team-alpha"])
        state.selectedNamespace = "team-alpha"
        let pod = PodSummary(name: "api-0", namespace: "team-alpha", status: "Running", containerNamesLine: "app, proxy")
        state.setPods([pod])

        viewModel.startTerminalSession(for: pod)
        try await waitUntil { backend.startedContainers.count == 1 }
        XCTAssertEqual(backend.startedContainers.first, "app")
        XCTAssertEqual(state.terminalSession?.containerName, "app")
        XCTAssertTrue(state.terminalSession?.transcript.contains("app") == true && state.terminalSession?.transcript.contains("proxy") == true,
                      "the shell says which container it chose out of which")

        let single = PodSummary(name: "api-1", namespace: "team-alpha", status: "Running", containerNamesLine: "app")
        state.setPods([pod, single])
        viewModel.startTerminalSession(for: single)
        try await waitUntil { backend.startedContainers.count == 2 }
        XCTAssertNil(backend.startedContainers.last ?? nil, "one container: the server picks it, no note needed")
    }

    func testAShellUsesThePodDefaultContainerAnnotationBeforeTheFirstContainer() async throws {
        let backend = RecordingShell()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, terminalBackend: backend, bookmarkManager: .inMemory())
        state.setContexts([KubeContext(name: "sample-cluster")])
        state.selectedContext = KubeContext(name: "sample-cluster")
        state.setNamespaces(["team-alpha"])
        state.selectedNamespace = "team-alpha"
        let pod = PodSummary(
            name: "api-0", namespace: "team-alpha", status: "Running",
            containerNamesLine: "proxy, app", defaultContainerName: "app"
        )
        state.setPods([pod])

        viewModel.startTerminalSession(for: pod)
        try await waitUntil { backend.startedContainers.count == 1 }
        XCTAssertEqual(backend.startedContainers.first, "app", "as kubectl exec picks it")
        XCTAssertEqual(state.terminalSession?.containerName, "app")
    }

    /// A one-off command names no container. The API server answers that
    /// with HTTP 400 when the pod has several, so Rune picks the one
    /// `kubectl exec` would.
    func testAOneOffCommandRunsInTheContainerKubectlWouldPick() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, bookmarkManager: .inMemory())
        state.setPods([
            PodSummary(name: "api-0", namespace: "team-alpha", status: "Running", containerNamesLine: "proxy, app", defaultContainerName: "app"),
            PodSummary(name: "web-0", namespace: "team-alpha", status: "Running", containerNamesLine: "proxy, web"),
            PodSummary(name: "job-0", namespace: "team-alpha", status: "Running", containerNamesLine: "run"),
            PodSummary(name: "api-0", namespace: "team-beta", status: "Running", containerNamesLine: "first, second")
        ])

        XCTAssertEqual(viewModel.defaultExecContainerName(podName: "api-0", namespace: "team-alpha"), "app", "the annotated container")
        XCTAssertEqual(viewModel.defaultExecContainerName(podName: "web-0", namespace: "team-alpha"), "proxy", "else the first")
        XCTAssertNil(viewModel.defaultExecContainerName(podName: "job-0", namespace: "team-alpha"), "one container: the server picks it")
        XCTAssertEqual(viewModel.defaultExecContainerName(podName: "api-0", namespace: "team-beta"), "first", "the pod of that namespace")
        XCTAssertNil(viewModel.defaultExecContainerName(podName: "gone-0", namespace: "team-alpha"))
    }

    /// A session kept from before Rune chose a container has none. It could
    /// never connect to a multi-container pod, so Reconnect must move it to
    /// the default container instead of calling it another target.
    func testReconnectingASessionWithoutAContainerUsesTheDefaultContainer() async throws {
        let backend = RecordingShell()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, terminalBackend: backend, bookmarkManager: .inMemory())
        state.setContexts([KubeContext(name: "sample-cluster")])
        state.selectedContext = KubeContext(name: "sample-cluster")
        state.setNamespaces(["team-alpha"])
        state.selectedNamespace = "team-alpha"
        let pod = PodSummary(name: "api-0", namespace: "team-alpha", status: "Running", containerNamesLine: "app, proxy")
        state.setPods([pod])
        state.setTerminalSession(PodTerminalSession(
            id: "shell-1", contextName: "sample-cluster", namespace: "team-alpha", podName: "api-0",
            shell: "sh", transcript: "", status: .failed
        ))
        state.selectTerminalSession(id: "shell-1")

        viewModel.startTerminalSession(for: pod, container: nil, replacingSessionID: "shell-1")
        try await waitUntil { backend.startedContainers.count == 1 }
        XCTAssertEqual(backend.startedContainers.first, "app")
        XCTAssertEqual(state.terminalSessions.map(\.id), ["shell-1"], "the same tab reconnected")
        XCTAssertEqual(state.terminalSession?.containerName, "app")
        XCTAssertNil(state.lastError, "not reported as a changed target")
    }

    // MARK: Reopening tabs

    func testAClosedShellTabSurvivesAPodListThatIsStillLoading() async throws {
        let backend = RecordingShell()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, terminalBackend: backend, bookmarkManager: .inMemory())
        state.setContexts([KubeContext(name: "sample-cluster")])
        state.selectedContext = KubeContext(name: "sample-cluster")
        state.setNamespaces(["team-alpha"])
        state.selectedNamespace = "team-alpha"
        let pod = PodSummary(name: "api-0", namespace: "team-alpha", status: "Running", containerNamesLine: "app")
        state.setPods([pod])
        viewModel.startTerminalSession(for: pod)
        try await waitUntil { backend.startedContainers.count == 1 }
        viewModel.closeTerminalSession(id: try XCTUnwrap(state.terminalSession?.id))

        state.setPods([])
        XCTAssertFalse(viewModel.reopenLastClosedTerminalSession(), "no pods listed yet")
        state.setPods([pod])
        XCTAssertTrue(viewModel.reopenLastClosedTerminalSession(), "the closed tab was kept")
        XCTAssertEqual(state.terminalSession?.podName, "api-0")
        XCTAssertFalse(viewModel.reopenLastClosedTerminalSession(), "and reopens once")
    }

    // MARK: Demo shell

    func testTheDemoShellEditsAndRecallsLinesLikeAShell() async throws {
        let shell = RuneDemoPodShell()
        let screen = ScreenRecorder()
        try await shell.startPodTerminalSession(
            id: "demo", from: [], context: KubeContext(name: "rune-demo"), namespace: "demo", podName: "api-0",
            container: nil, shellCommand: ["sh"], onOutput: { screen.feed($0) }, onTermination: { _ in }
        )
        try await waitUntil { screen.text.contains(":/app#") }

        for input in ["echo ac", "\u{1B}[D", "b", "\r"] { try await shell.writeToPodTerminalSession(id: "demo", text: input) }
        XCTAssertTrue(screen.lines.contains("abc"), "Left moved the cursor; b landed mid-line. Screen: \(screen.text)")

        for input in ["\u{1B}[A", "\r"] { try await shell.writeToPodTerminalSession(id: "demo", text: input) }
        XCTAssertEqual(screen.lines.filter { $0 == "abc" }.count, 2, "Up recalled the last command. Screen: \(screen.text)")

        for input in ["echo xyz", "\u{1B}[D", "\u{7F}", "\r"] { try await shell.writeToPodTerminalSession(id: "demo", text: input) }
        XCTAssertTrue(screen.lines.contains("xz"), "Delete removed the character before the cursor. Screen: \(screen.text)")

        for input in ["\u{1B}[A", "\u{1B}[A", "\u{1B}[B", "\r"] { try await shell.writeToPodTerminalSession(id: "demo", text: input) }
        XCTAssertEqual(screen.lines.filter { $0 == "xz" }.count, 2, "Up twice then Down returns to the newest. Screen: \(screen.text)")
    }

    // MARK: Helpers

    private func connectedViewModel(backend: RecordingShell) throws -> RuneAppViewModel {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, terminalBackend: backend, bookmarkManager: .inMemory())
        state.setTerminalSession(PodTerminalSession(
            id: "shell-1", contextName: "sample-cluster", namespace: "team-alpha", podName: "api-0",
            shell: "sh", transcript: "$ ", status: .connected
        ))
        state.selectTerminalSession(id: "shell-1")
        return viewModel
    }

    private func waitUntil(timeout: TimeInterval = 3, _ condition: () -> Bool, file: StaticString = #filePath, line: UInt = #line) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition() {
            if Date() > deadline { return XCTFail("timed out", file: file, line: line) }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }
}

/// Renders a shell's output as a terminal would.
private final class ScreenRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var emulator = TerminalScreenEmulator(columns: 80, rows: 24)
    func feed(_ text: String) { lock.withLock { emulator.feed(text) } }
    var text: String { lock.withLock { emulator.renderedText } }
    var lines: [String] { text.components(separatedBy: "\n") }
}

/// A pod shell that records what it is asked to do.
private final class RecordingShell: PodTerminalSessionBackend, @unchecked Sendable {
    private let lock = NSLock()
    private var writeLog: [String] = []
    private var containerLog: [String?] = []
    var writes: [String] { lock.withLock { writeLog } }
    var startedContainers: [String?] { lock.withLock { containerLog } }

    func startPodTerminalSession(
        id sessionID: String, from sources: [KubeConfigSource], context: KubeContext, namespace: String,
        podName: String, container: String?, shellCommand: [String],
        onOutput: @escaping @Sendable (String) -> Void, onTermination: @escaping @Sendable (Int32) -> Void
    ) async throws {
        lock.withLock { containerLog.append(container) }
    }

    func writeToPodTerminalSession(id: String, text: String) async throws { lock.withLock { writeLog.append(text) } }
    func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws {}
    func stopPodTerminalSession(id: String) async {}
}
