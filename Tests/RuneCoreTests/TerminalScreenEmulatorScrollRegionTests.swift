import RuneCore
import XCTest

/// Full-screen tools (vim, less, top) scroll part of the screen with a
/// scroll region and ask for application cursor keys; both must be honoured
/// or their screens tear.
final class TerminalScreenEmulatorScrollRegionTests: XCTestCase {
    /// The screen's rows; a line break after the last one only marks where
    /// the cursor is.
    private func shown(_ emulator: TerminalScreenEmulator) -> String {
        var text = emulator.renderedText
        if text.hasSuffix("\n") { text.removeLast() }
        return text
    }

    private func screen(_ text: String, rows: Int = 5) -> TerminalScreenEmulator {
        var emulator = TerminalScreenEmulator(columns: 20, rows: rows)
        emulator.feed(text)
        return emulator
    }

    func testLineFeedsScrollOnlyInsideTheRegion() {
        let emulator = screen("\u{1B}[1;1Htop\u{1B}[5;1Hbottom\u{1B}[2;4r\u{1B}[2;1Ha\r\nb\r\nc\r\nd")
        XCTAssertEqual(shown(emulator), "top\nb\nc\nd\nbottom", "rows 2 to 4 scrolled; the first and last stayed put")
    }

    func testReverseIndexAtTheRegionTopScrollsTheRegionDown() {
        let emulator = screen("\u{1B}[1;1Htop\u{1B}[2;1Ha\u{1B}[3;1Hb\u{1B}[4;1Hc\u{1B}[5;1Hbottom\u{1B}[2;4r\u{1B}[2;1H\u{1B}M")
        XCTAssertEqual(shown(emulator), "top\n\na\nb\nbottom", "a blank row at the top of the region; c dropped off its bottom")
    }

    func testInsertAndDeleteLinesStayInsideTheRegion() {
        var emulator = screen("\u{1B}[1;1Htop\u{1B}[2;1Ha\u{1B}[3;1Hb\u{1B}[4;1Hc\u{1B}[5;1Hbottom\u{1B}[2;4r\u{1B}[2;1H\u{1B}[M")
        XCTAssertEqual(shown(emulator), "top\nb\nc\n\nbottom", "deleting a line pulls the region up, not the status line")
        emulator.feed("\u{1B}[2;1H\u{1B}[L")
        XCTAssertEqual(shown(emulator), "top\n\nb\nc\nbottom")
    }

    func testScrollingInsideARegionKeepsItOutOfTheScrollback() {
        let emulator = screen("\u{1B}[2;4r\u{1B}[4;1Hone\r\ntwo\r\nthree\r\nfour")
        XCTAssertFalse(emulator.renderedText.hasPrefix("one"), "lines scrolled out of a region are not history")
        XCTAssertTrue(emulator.renderedText.contains("four"))
    }

    func testARegionFromTheTopRowKeepsItsScrolledLinesAsHistory() {
        // A status line on the last row (apt's progress bar): rows 1 to 4 scroll.
        let emulator = screen("\u{1B}[1;4r\u{1B}[1;1Hone\r\ntwo\r\nthree\r\nfour\r\nfive\r\nsix")
        XCTAssertTrue(emulator.renderedText.hasPrefix("one\ntwo\nthree"), "output above a status line stays in the scrollback")
        XCTAssertTrue(emulator.renderedText.contains("six"))
    }

    func testARegionFromTheTopRowOfTheAlternateScreenIsNotHistory() {
        let emulator = screen("\u{1B}[?1049h\u{1B}[1;4r\u{1B}[1;1Hone\r\ntwo\r\nthree\r\nfour\r\nfive\u{1B}[r\u{1B}[?1049l")
        XCTAssertFalse(emulator.renderedText.contains("one"), "a full-screen tool's lines are not history")
    }

    func testResizingInTheAlternateScreenFitsThePrimaryScreenToo() {
        // The primary screen has a region to row 9 and ten rows of text when
        // a full-screen tool starts and the window shrinks to four rows.
        var emulator = TerminalScreenEmulator(columns: 20, rows: 10)
        emulator.feed((1...10).map { "line\($0)" }.joined(separator: "\r\n"))
        emulator.feed("\u{1B}[2;9r\u{1B}[?1049h")
        XCTAssertTrue(emulator.isUsingAlternateScreen)
        emulator.resize(columns: 20, rows: 4)
        emulator.feed("\u{1B}[?1049l")
        XCTAssertFalse(emulator.isUsingAlternateScreen)
        emulator.feed("\u{1B}[4;1H\r\nafter\r\nmore\r\n\u{1B}M\u{1B}[L\u{1B}[M\u{1B}[S\u{1B}[T")
        let text = emulator.renderedText
        XCTAssertTrue(text.hasPrefix("line1\nline2"), "rows that no longer fit became history")
        XCTAssertTrue(text.contains("after"), "scrolling, inserting and deleting lines work at the new size")
    }

    func testResettingTheRegionRestoresFullScreenScrolling() {
        let emulator = screen("\u{1B}[2;4r\u{1B}[r\u{1B}[5;1Hlast\r\nnext", rows: 5)
        XCTAssertTrue(emulator.renderedText.hasSuffix("last\nnext"))
    }

    func testApplicationCursorKeysFollowTheMode() {
        var emulator = TerminalScreenEmulator()
        XCTAssertFalse(emulator.applicationCursorKeys)
        emulator.feed("\u{1B}[?1h")
        XCTAssertTrue(emulator.applicationCursorKeys)
        emulator.feed("\u{1B}[?1l")
        XCTAssertFalse(emulator.applicationCursorKeys)
        emulator.feed("\u{1B}[?1;1049h")
        XCTAssertTrue(emulator.applicationCursorKeys, "several modes in one sequence")
        emulator.feed("\u{1B}c")
        XCTAssertFalse(emulator.applicationCursorKeys, "a full reset clears it")
    }
}
