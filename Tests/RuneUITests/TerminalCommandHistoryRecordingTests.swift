import Combine
import RuneCore
import RuneKube
@testable import RuneUI
import XCTest

/// Commands run in a pod shell land in the Commands tab's history, whether
/// sent from the prompt or typed in the terminal, once the shell echoes
/// them, and survive a restart.
@MainActor
final class TerminalCommandHistoryRecordingTests: XCTestCase {
    private let suiteName = TestDefaultsSuite.name("rune.tests.terminal-command-history")
    private var defaults: UserDefaults!

    override func setUp() async throws {
        defaults = UserDefaults(suiteName: suiteName)
        defaults.removeTestSuite(named: suiteName)
        defaults.set(50, forKey: RuneSettingsKeys.terminalCommandHistoryLimit)
    }

    override func tearDown() async throws {
        defaults.removeTestSuite(named: suiteName)
        defaults = nil
    }

    func testPromptCommandsAreRecordedWithTheirPod() {
        let viewModel = connectedViewModel()
        run("ls -la", in: viewModel)
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["ls -la"])
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.first?.podName, "api-0")
    }

    func testCommandsTypedInTheTerminalAreRecorded() {
        let viewModel = connectedViewModel()
        type(["d", "f", " ", "-", "x", "\u{7F}", "h", "\r"], in: viewModel)
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["df -h"], "Delete removed the x")
    }

    func testLinesEditedWithCursorKeysOrTabAreNotGuessed() {
        let viewModel = connectedViewModel()
        type(["l", "s", "\u{1B}[D", "\r"], in: viewModel)
        type(["c", "a", "\t", "\r"], in: viewModel)
        type(["\u{1B}[A", "\r"], in: viewModel)
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty, "what the shell ran is unknown, so nothing is recorded")
        type(["p", "w", "d", "\r"], in: viewModel)
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["pwd"], "the next plain line is recorded again")
    }

    /// A prompt that hides its input (psql, sudo, read -s) echoes nothing:
    /// what is typed there is not a command and must never be saved.
    func testAPasswordTypedAtAHiddenPromptIsNotRecorded() {
        let viewModel = connectedViewModel()
        type(["p", "s", "q", "l", "\r"], in: viewModel, response: "Password: ")
        type(["h", "u", "n", "t", "e", "r", "2", "\r"], in: viewModel, echoes: false, response: "psql (16.2)\r\ndb=# ")
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["psql"])
        XCTAssertNil(savedHistoryText().range(of: "hunter2"), "the password is not on disk")

        type(["\\", "q", "\r"], in: viewModel)
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["\\q", "psql"], "echoed lines are recorded again")
    }

    func testAPasswordSentFromThePromptFieldIsNotRecorded() {
        let viewModel = connectedViewModel()
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "sudo -s\r\nPassword: ")
        viewModel.terminalSessionInput = "hunter2"
        viewModel.sendTerminalSessionInput()
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "\r\n# ")
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty)
        XCTAssertNil(savedHistoryText().range(of: "hunter2"))
    }

    func testTextTypedInAFullScreenToolIsNotRecorded() {
        let viewModel = connectedViewModel()
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "\u{1B}[?1049h")
        type(["i", "t", "o", "k", "e", "n", "\r"], in: viewModel, response: "")
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty, "text typed into an editor is not a command")

        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "\u{1B}[?1049l$ ")
        type(["p", "w", "d", "\r"], in: viewModel)
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["pwd"])
    }

    func testAPastedLineIsRecordedOnceTheShellEchoesIt() {
        let viewModel = connectedViewModel()
        viewModel.sendTerminalControlSequence("ls -la\r")
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty, "not before the shell shows it")
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "ls -la\r\ntotal 0\r\n$ ")
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["ls -la"])
    }

    /// A shell reads pasted lines one at a time and shows each at its own
    /// prompt, after the output of the one before.
    func testSeveralPastedLinesAreEachRecordedAsTheShellReadsThem() {
        let viewModel = connectedViewModel()
        viewModel.sendTerminalControlSequence("date\runame -a\r")
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "date\r\nMon Jan  5 10:00:00 UTC 2026\r\n$ uname -a\r\nLinux\r\n$ ")
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["uname -a", "date"])
    }

    /// As in a shell's own history (and so in Terminal and iTerm2): what a
    /// program asks for is not a command, even when the terminal shows it.
    func testAnAnswerToAProgramsQuestionIsNotRecorded() {
        let viewModel = connectedViewModel()
        type(["l", "o", "g", "i", "n", "\r"], in: viewModel, response: "Token: ")
        type(["a", "b", "c", "1", "2", "3", "\r"], in: viewModel, response: "Continue? [y/N] ")
        type(["y", "\r"], in: viewModel, response: "")
        type(["v", "a", "l", "u", "e", "\r"], in: viewModel)
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["login"], "only the line typed at the shell prompt")
        XCTAssertNil(savedHistoryText().range(of: "abc123"))

        type(["p", "w", "d", "\r"], in: viewModel)
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["pwd", "login"], "back at the prompt, commands are recorded again")
    }

    func testShellPromptsAreRecognisedByTheirLastCharacter() {
        for prompt in ["$ ", "# ", "% ", "/ # ", "root@api-0:/app# ", "user@host:~$ ", "host% ", "$"] {
            XCTAssertTrue(RuneAppViewModel.looksLikeShellPrompt(prompt), prompt)
        }
        for prompt in ["", "Token: ", "Password: ", "Continue? [y/N] ", ">>> ", "mysql> ", "> "] {
            XCTAssertFalse(RuneAppViewModel.looksLikeShellPrompt(prompt), prompt)
        }
    }

    /// A command typed while another is still printing: the terminal shows
    /// it at once, between the output, and the shell shows it again at its
    /// prompt when it reads it. That second showing makes it a command.
    func testACommandTypedWhileAnotherIsRunningIsRecordedWhenTheShellReadsIt() {
        let viewModel = connectedViewModel()
        viewModel.sendTerminalControlSequence("ls -la\r")
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "build step 41\r\nbuild step 42\r\n")
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "ls -la\r\nbuild step 43\r\n")
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty, "shown, but not yet at a prompt")
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "$ ls -la\r\ntotal 0\r\n$ ")
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["ls -la"])
    }

    /// A hidden line does not wait for ever: once enough output has gone
    /// by, a later line that happens to end with it is not taken for its echo.
    func testAHiddenLineStopsWaitingAfterEnoughOutput() {
        let viewModel = connectedViewModel()
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "Password: ")
        type(["h", "u", "n", "t", "e", "r", "2", "\r"], in: viewModel, echoes: false, response: "")
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: (0..<80).map { "line \($0)\r\n" }.joined())
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "the value was hunter2\r\n$ ")
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty)
        XCTAssertNil(savedHistoryText().range(of: "hunter2"))
    }

    func testACommandWrappedOverSeveralRowsIsRecorded() {
        let viewModel = connectedViewModel()
        let command = "echo " + (1...40).map { "word\($0)" }.joined(separator: " ")
        XCTAssertGreaterThan(command.count, 160, "longer than two rows of the terminal")
        run(command, in: viewModel)
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), [command])
    }

    func testHistorySurvivesARestartAndCanBeCleared() {
        let viewModel = connectedViewModel()
        run("printenv | sort", in: viewModel)

        let restarted = newViewModel(state: RuneAppState())
        XCTAssertEqual(restarted.terminalCommandHistory.entries.map(\.command), ["printenv | sort"])
        restarted.clearTerminalCommandHistory()
        XCTAssertTrue(restarted.terminalCommandHistory.entries.isEmpty)
        XCTAssertTrue(newViewModel(state: RuneAppState()).terminalCommandHistory.entries.isEmpty)
    }

    func testTheLimitFromSettingsApplies() {
        defaults.set(2, forKey: RuneSettingsKeys.terminalCommandHistoryLimit)
        let viewModel = connectedViewModel()
        for command in ["one", "two", "three"] { run(command, in: viewModel) }
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["three", "two"])

        defaults.set(0, forKey: RuneSettingsKeys.terminalCommandHistoryLimit)
        viewModel.applyTerminalCommandHistoryLimit()
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty, "Off keeps nothing")
        run("four", in: viewModel)
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty)
    }

    func testTurningHistoryOffClearsItAndSavesNothing() {
        let viewModel = connectedViewModel()
        run("env", in: viewModel)
        XCTAssertNotNil(defaults.data(forKey: RuneSettingsKeys.terminalCommandHistory))

        defaults.runeTerminalCommandHistoryEnabled = false
        XCTAssertNil(defaults.data(forKey: RuneSettingsKeys.terminalCommandHistory), "turning it off clears what was saved")
        viewModel.applyTerminalCommandHistoryLimit()
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty, "and what was kept in memory")

        run("cat secret.txt", in: viewModel)
        type(["i", "d", "\r"], in: viewModel)
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty, "nothing is recorded while off")
        XCTAssertNil(defaults.data(forKey: RuneSettingsKeys.terminalCommandHistory), "nothing is saved while off")

        defaults.runeTerminalCommandHistoryEnabled = true
        run("uptime", in: viewModel)
        XCTAssertEqual(viewModel.terminalCommandHistory.entries.map(\.command), ["uptime"], "only commands after turning it back on")
    }

    func testHistorySavedBeforeTurningOffIsNotLoadedWhileOff() throws {
        let saved = TerminalCommandHistory(entries: [
            .init(command: "ls", contextName: "sample-cluster", namespace: "team-alpha", podName: "api-0", date: Date()),
        ])
        defaults.set(try JSONEncoder().encode(saved), forKey: RuneSettingsKeys.terminalCommandHistory)
        defaults.set(false, forKey: RuneSettingsKeys.terminalCommandHistoryEnabled)

        let viewModel = newViewModel(state: RuneAppState())
        XCTAssertTrue(viewModel.terminalCommandHistory.entries.isEmpty)
        XCTAssertNil(defaults.data(forKey: RuneSettingsKeys.terminalCommandHistory), "left over history is removed from disk")
    }

    /// An unrelated settings change must not publish the history again:
    /// every publish redraws the window.
    func testAnUnrelatedSettingsChangeDoesNotPublishTheHistory() {
        let viewModel = connectedViewModel()
        run("ls", in: viewModel)
        var publishes = 0
        let observation = viewModel.$terminalCommandHistory.dropFirst().sink { _ in publishes += 1 }
        defaults.set(true, forKey: "rune.tests.unrelated")
        viewModel.applyTerminalCommandHistoryLimit()
        XCTAssertEqual(publishes, 0)
        observation.cancel()
    }

    // MARK: Helpers

    /// Sends a command from the prompt field; the shell echoes it and prompts again.
    private func run(_ command: String, in viewModel: RuneAppViewModel) {
        viewModel.terminalSessionInput = command
        viewModel.sendTerminalSessionInput()
        viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: command + "\r\n$ ")
    }

    /// Types keys in the terminal. A shell echoes each key as it is typed and
    /// answers Return with a line break and `response`.
    private func type(_ keys: [String], in viewModel: RuneAppViewModel, echoes: Bool = true, response: String = "$ ") {
        for key in keys {
            viewModel.sendTerminalControlSequence(key)
            switch key {
            case "\r": viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "\r\n" + response)
            case "\u{7F}": if echoes { viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: "\u{8} \u{8}") }
            case "\u{1B}[D": if echoes { viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: key) }
            case "\t", "\u{1B}[A": break
            default: if echoes { viewModel.enqueueTerminalSessionOutput(id: "shell-1", text: key) }
            }
        }
    }

    private func savedHistoryText() -> String {
        defaults.data(forKey: RuneSettingsKeys.terminalCommandHistory).map { String(decoding: $0, as: UTF8.self) } ?? ""
    }

    private func newViewModel(state: RuneAppState) -> RuneAppViewModel {
        RuneAppViewModel(state: state, terminalBackend: SilentShell(), bookmarkManager: .inMemory(), terminalCommandHistoryDefaults: defaults)
    }

    private func connectedViewModel() -> RuneAppViewModel {
        let state = RuneAppState()
        let viewModel = newViewModel(state: state)
        state.setTerminalSession(PodTerminalSession(
            id: "shell-1", contextName: "sample-cluster", namespace: "team-alpha", podName: "api-0",
            shell: "sh", transcript: "$ ", status: .connected
        ))
        state.selectTerminalSession(id: "shell-1")
        return viewModel
    }
}

private final class SilentShell: PodTerminalSessionBackend, @unchecked Sendable {
    func startPodTerminalSession(
        id sessionID: String, from sources: [KubeConfigSource], context: KubeContext, namespace: String,
        podName: String, container: String?, shellCommand: [String],
        onOutput: @escaping @Sendable (String) -> Void, onTermination: @escaping @Sendable (Int32) -> Void
    ) async throws {}
    func writeToPodTerminalSession(id: String, text: String) async throws {}
    func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws {}
    func stopPodTerminalSession(id: String) async {}
}
