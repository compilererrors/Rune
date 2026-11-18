import AppKit
import RuneCore
@testable import RuneUI
import XCTest

/// The Terminal section's tab and text shortcuts, as in Terminal, iTerm2,
/// Ghostty and Safari: which key is which command, and the tab models that
/// back "reopen closed tab" and "next/previous tab".
@MainActor
final class TerminalKeyCommandTests: XCTestCase {
    private func key(_ characters: String, _ keyCode: UInt16, _ flags: NSEvent.ModifierFlags) -> NSEvent {
        NSEvent.keyEvent(
            with: .keyDown, location: .zero, modifierFlags: flags, timestamp: 0, windowNumber: 0, context: nil,
            characters: characters, charactersIgnoringModifiers: characters, isARepeat: false, keyCode: keyCode
        )!
    }

    func testKeysMapToTerminalCommands() {
        XCTAssertEqual(TerminalKeyCommand(key("w", 13, .command)), .closeTab)
        XCTAssertEqual(TerminalKeyCommand(key("t", 17, .command)), .newTab)
        XCTAssertEqual(TerminalKeyCommand(key("T", 17, [.command, .shift])), .reopenClosedTab)
        XCTAssertEqual(TerminalKeyCommand(key("\t", 48, .control)), .nextTab)
        XCTAssertEqual(TerminalKeyCommand(key("\t", 48, [.control, .shift])), .previousTab)
        XCTAssertEqual(TerminalKeyCommand(key("}", 30, [.command, .shift])), .nextTab)
        XCTAssertEqual(TerminalKeyCommand(key("{", 33, [.command, .shift])), .previousTab)
        XCTAssertEqual(TerminalKeyCommand(key("k", 40, .command)), .clear)
        XCTAssertEqual(TerminalKeyCommand(key("=", 24, .command)), .biggerText)
        XCTAssertEqual(TerminalKeyCommand(key("+", 24, [.command, .shift])), .biggerText)
        XCTAssertEqual(TerminalKeyCommand(key("-", 27, .command)), .smallerText)
        XCTAssertEqual(TerminalKeyCommand(key("0", 29, .command)), .defaultTextSize)
    }

    func testOtherKeysAreLeftAlone() {
        XCTAssertNil(TerminalKeyCommand(key("w", 13, [])), "typing w")
        XCTAssertNil(TerminalKeyCommand(key("w", 13, .control)), "Ctrl-W deletes a word in the shell")
        XCTAssertNil(TerminalKeyCommand(key("k", 40, [.command, .option])))
        XCTAssertNil(TerminalKeyCommand(key("1", 18, .command)), "Cmd-1 to Cmd-9 switch sections")
        XCTAssertNil(TerminalKeyCommand(key("\t", 48, [])), "Tab moves between panes")
        XCTAssertNil(TerminalKeyCommand(key("t", 17, [.command, .option])))
    }

    func testTerminalTextSizeStepsWithinItsLimits() {
        XCTAssertEqual(TerminalKeyCommand.textSize(after: .biggerText, from: 12), 13)
        XCTAssertEqual(TerminalKeyCommand.textSize(after: .smallerText, from: 12), 11)
        XCTAssertEqual(TerminalKeyCommand.textSize(after: .defaultTextSize, from: 17), RuneSettingsKeys.terminalFontSizeDefault)
        XCTAssertEqual(TerminalKeyCommand.textSize(after: .biggerText, from: RuneSettingsKeys.terminalFontSizeMaximum), RuneSettingsKeys.terminalFontSizeMaximum)
        XCTAssertEqual(TerminalKeyCommand.textSize(after: .smallerText, from: RuneSettingsKeys.terminalFontSizeMinimum), RuneSettingsKeys.terminalFontSizeMinimum)
    }

    // MARK: Log tabs

    private let pods = ["api-0", "api-1", "worker-0"].map { PodSummary(name: $0, namespace: "team-alpha", status: "Running") }

    private func logTabs() -> TerminalPodLogTabState {
        var state = TerminalPodLogTabState()
        for pod in pods { state.add(preferredPod: pod) }
        return state
    }

    func testLogTabsCycleAndWrap() {
        var state = logTabs()
        XCTAssertEqual(state.activeTabID, "pod:\(pods[2].id)")
        XCTAssertEqual(state.selectAdjacent(offset: 1, availablePods: pods)?.name, "api-0", "wraps to the first tab")
        XCTAssertEqual(state.selectAdjacent(offset: -1, availablePods: pods)?.name, "worker-0", "and back")
    }

    func testClosedLogTabsReopenNewestFirst() {
        var state = logTabs()
        _ = state.close(id: "pod:\(pods[0].id)", availablePods: pods, fallbackPod: nil)
        _ = state.close(id: "pod:\(pods[2].id)", availablePods: pods, fallbackPod: nil)
        XCTAssertEqual(state.tabs.map(\.podName), ["api-1"])

        XCTAssertEqual(state.reopenLastClosed(availablePods: pods)?.name, "worker-0")
        XCTAssertEqual(state.activeTabID, "pod:\(pods[2].id)")
        XCTAssertEqual(state.reopenLastClosed(availablePods: pods)?.name, "api-0")
        XCTAssertNil(state.reopenLastClosed(availablePods: pods), "nothing left to reopen")
        XCTAssertEqual(Set(state.tabs.map(\.podName)), ["api-0", "api-1", "worker-0"])
    }

    func testAClosedLogTabWhosePodIsGoneIsSkipped() {
        var state = logTabs()
        _ = state.close(id: "pod:\(pods[1].id)", availablePods: pods, fallbackPod: nil)
        _ = state.close(id: "pod:\(pods[2].id)", availablePods: pods, fallbackPod: nil)
        let remaining = [pods[0], pods[1]]
        XCTAssertEqual(state.reopenLastClosed(availablePods: remaining)?.name, "api-1", "worker-0 is gone")
    }

    func testClosedLogTabsSurviveAPodListThatIsStillLoading() {
        var state = logTabs()
        _ = state.close(id: "pod:\(pods[0].id)", availablePods: pods, fallbackPod: nil)
        _ = state.close(id: "pod:\(pods[2].id)", availablePods: pods, fallbackPod: nil)
        XCTAssertNil(state.reopenLastClosed(availablePods: []), "no pods listed yet")
        XCTAssertEqual(state.reopenLastClosed(availablePods: pods)?.name, "worker-0", "the closed tabs were kept")
        XCTAssertEqual(state.reopenLastClosed(availablePods: pods)?.name, "api-0")
    }
}
