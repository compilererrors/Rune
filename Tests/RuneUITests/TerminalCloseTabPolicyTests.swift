@testable import RuneUI
import XCTest

/// Cmd-W in the Terminal section closes the active tab of the strip you
/// are in, and only while that strip has more than one tab.
final class TerminalCloseTabPolicyTests: XCTestCase {
    private typealias Policy = TerminalCloseTabPolicy

    private func strips(logs: [String] = ["log-a", "log-b"], activeLog: String? = "log-b",
                        shells: [String] = ["shell-1", "shell-2"], activeShell: String? = "shell-2",
                        draft: Bool = false) -> Policy.Strips {
        Policy.Strips(logTabIDs: logs, activeLogTabID: activeLog, shellSessionIDs: shells,
                      activeShellSessionID: activeShell, isComposingShellDraft: draft)
    }

    func testInTheShellItClosesTheActiveShellTab() {
        XCTAssertEqual(Policy.target(logsFocused: false, logsVisible: true, strips: strips()), .shellSession("shell-2"))
    }

    func testInTheLogsItClosesTheActiveLogTab() {
        XCTAssertEqual(Policy.target(logsFocused: true, logsVisible: true, strips: strips()), .logTab("log-b"))
    }

    func testWithOneTabLeftTheWindowClosesAsUsual() {
        XCTAssertNil(Policy.target(logsFocused: false, logsVisible: true, strips: strips(shells: ["shell-1"], activeShell: "shell-1")))
        XCTAssertNil(Policy.target(logsFocused: true, logsVisible: true, strips: strips(logs: ["log-a"], activeLog: "log-a")))
    }

    func testLogsOnlyCountWhileTheLogsTabIsShown() {
        XCTAssertEqual(Policy.target(logsFocused: true, logsVisible: false, strips: strips()), .shellSession("shell-2"))
    }

    func testANewShellTabDraftIsClosedFirst() {
        XCTAssertEqual(Policy.target(logsFocused: false, logsVisible: true, strips: strips(shells: ["shell-1"], activeShell: "shell-1", draft: true)), .shellDraft)
        XCTAssertNil(Policy.target(logsFocused: false, logsVisible: true, strips: strips(shells: [], activeShell: nil, draft: true)), "the draft alone is the only tab")
    }

    func testFocusFollowsTheResponderElseThePaneLastUsed() {
        XCTAssertTrue(Policy.logsHaveFocus(responderIsInInspector: true, paneFocusIsInspector: false))
        XCTAssertFalse(Policy.logsHaveFocus(responderIsInInspector: false, paneFocusIsInspector: true))
        XCTAssertTrue(Policy.logsHaveFocus(responderIsInInspector: nil, paneFocusIsInspector: true))
        XCTAssertFalse(Policy.logsHaveFocus(responderIsInInspector: nil, paneFocusIsInspector: false))
    }
}
