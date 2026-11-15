import RuneUI
import XCTest

/// Minimizing and restoring the main window, as from the Dock. The window
/// must always come back: never hidden while it deminiaturizes, never a
/// helper window picked instead of the main one.
final class RuneWindowPresentationPolicyTests: XCTestCase {
    private typealias Policy = RuneWindowPresentationPolicy
    private typealias Window = RuneWindowPresentationPolicy.Window

    func testDockClickRestoresAMinimizedWindowOnlyByDeminiaturizing() {
        // A minimized window reports itself as not on the active Space.
        let windows = [Window(title: "Rune", isVisible: false, isMiniaturized: true, isOnActiveSpace: false)]
        XCTAssertEqual(Policy.reopen(windows: windows, title: "Rune"), .deminiaturize(index: 0))
    }

    func testActivatingTheAppLeavesAMinimizedWindowMinimized() {
        let windows = [Window(title: "Rune", isVisible: false, isMiniaturized: true, isOnActiveSpace: false)]
        XCTAssertEqual(Policy.activate(windows: windows, title: "Rune"), .none)
    }

    func testActivatingTheAppDoesNotMoveOrReorderAVisibleWindow() {
        let windows = [Window(title: "Rune", isVisible: true, isOnActiveSpace: false)]
        XCTAssertEqual(Policy.activate(windows: windows, title: "Rune"), .none)
    }

    func testDockClickMovesAWindowShownOnAnotherSpace() {
        let windows = [Window(title: "Rune", isVisible: true, isOnActiveSpace: false)]
        XCTAssertEqual(Policy.reopen(windows: windows, title: "Rune"), .moveToActiveSpace(index: 0))
    }

    func testHelperWindowsAreNeverTakenForTheMainWindow() {
        let windows = [
            Window(title: "", isPanel: true, isVisible: true),
            Window(title: "", canBecomeMain: false, isVisible: false),
            Window(title: "Rune", isVisible: false, isMiniaturized: true, isOnActiveSpace: false)
        ]
        XCTAssertEqual(Policy.reopen(windows: windows, title: "Rune"), .deminiaturize(index: 2))
    }

    func testAMainWindowWithAnotherTitleIsStillFound() {
        let windows = [
            Window(title: "", canBecomeMain: false, isVisible: true),
            Window(title: "aks-sample · Terminal", isVisible: false, isMiniaturized: true, isOnActiveSpace: false)
        ]
        XCTAssertEqual(Policy.reopen(windows: windows, title: "Rune"), .deminiaturize(index: 1))
    }

    func testTheSettingsWindowIsNeverTakenForTheMainWindow() {
        let windows = [
            Window(title: "General", isAuxiliary: true, isVisible: true),
            Window(title: "aks-sample · Terminal", isVisible: false, isMiniaturized: true, isOnActiveSpace: false)
        ]
        XCTAssertEqual(Policy.reopen(windows: windows, title: "Rune"), .deminiaturize(index: 1))
        XCTAssertEqual(Policy.reopen(windows: [windows[0]], title: "Rune"), .openMainWindow)
    }

    func testWithNoMainWindowOneIsOpened() {
        XCTAssertEqual(Policy.reopen(windows: [], title: "Rune"), .openMainWindow)
        XCTAssertEqual(Policy.activate(windows: [Window(title: "", isPanel: true, isVisible: true)], title: "Rune"), .openMainWindow)
    }

    func testRepeatedMinimizeAndRestoreAlwaysEndsWithTheWindowBack() {
        var window = Window(title: "Rune", isVisible: true)
        for _ in 0..<10 {
            window.isMiniaturized = true
            window.isVisible = false
            window.isOnActiveSpace = false
            // Activation first (as clicking the Dock activates), then reopen.
            XCTAssertEqual(Policy.activate(windows: [window], title: "Rune"), .none)
            XCTAssertEqual(Policy.reopen(windows: [window], title: "Rune"), .deminiaturize(index: 0))
            window.isMiniaturized = false
            window.isVisible = true
            window.isOnActiveSpace = true
            XCTAssertEqual(Policy.reopen(windows: [window], title: "Rune"), .makeKey(index: 0))
        }
    }
}
