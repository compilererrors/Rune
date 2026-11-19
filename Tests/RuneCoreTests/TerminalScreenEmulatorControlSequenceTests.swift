import Foundation
import RuneCore
import XCTest

/// Shells and editors send sequences Rune does not implement. Each must be
/// skipped whole, and never be run as the plain command that happens to
/// share its final byte.
final class TerminalScreenEmulatorControlSequenceTests: XCTestCase {
    private func shown(_ emulator: TerminalScreenEmulator) -> String {
        var text = emulator.renderedText
        if text.hasSuffix("\n") { text.removeLast() }
        return text
    }

    private func screen(_ text: String, columns: Int = 20, rows: Int = 5) -> TerminalScreenEmulator {
        var emulator = TerminalScreenEmulator(columns: columns, rows: rows)
        emulator.feed(text)
        return emulator
    }

    func testKittyKeyboardSequencesDoNotRestoreTheCursor() {
        // fish, Neovim and Helix push, query and pop the kitty keyboard
        // protocol with `u`; plain `CSI u` is restore-cursor.
        for sequence in ["\u{1B}[>1u", "\u{1B}[?u", "\u{1B}[<u", "\u{1B}[=5;1u", "\u{1B}[<1u"] {
            let emulator = screen("one\r\ntwo\r\n$ " + sequence + "ls")
            XCTAssertEqual(shown(emulator), "one\ntwo\n$ ls", "\(sequence.debugDescription) moved the cursor")
        }
    }

    func testPlainSaveAndRestoreCursorStillWork() {
        let emulator = screen("ab\u{1B}[scd\u{1B}[uX")
        XCTAssertEqual(shown(emulator), "abXd")
    }

    func testPrivateFormsOfScrollAndSaveAreNotTheirPlainCommands() {
        // `CSI > T` resets title modes; `CSI ? s` and `CSI ? r` save and
        // restore private modes. None scrolls or moves the cursor.
        let scrolled = screen("one\r\ntwo\u{1B}[>2T!")
        XCTAssertEqual(shown(scrolled), "one\ntwo!")

        let saved = screen("ab\u{1B}[?1049s\r\ncd\u{1B}[?1049r\u{1B}[uX")
        XCTAssertEqual(shown(saved), "Xb\ncd", "the only cursor saved is the default home position")
    }

    func testSequencesWithAnIntermediateByteAreSkipped() {
        // Soft reset, cursor style, scroll left: `!`, space and `$` come
        // before the final byte.
        let emulator = screen("ab\u{1B}[!p\u{1B}[2 q\u{1B}[3 @\u{1B}[?2026$pcd")
        XCTAssertEqual(shown(emulator), "abcd")
    }

    func testPrivateModesAndErasesStillApply() {
        var emulator = screen("shell$ \u{1B}[?1049h\u{1B}[?1hfull screen")
        XCTAssertTrue(emulator.isUsingAlternateScreen)
        XCTAssertTrue(emulator.applicationCursorKeys)
        emulator.feed("\u{1B}[?1l\u{1B}[?1049l")
        XCTAssertFalse(emulator.isUsingAlternateScreen)
        XCTAssertFalse(emulator.applicationCursorKeys)
        XCTAssertEqual(shown(emulator), "shell$")

        let erased = screen("abcdef\u{1B}[3D\u{1B}[?K")
        XCTAssertEqual(shown(erased), "abc")
    }

    func testEscapeSequencesWithIntermediatesTakeTheirFinalByte() {
        // Select UTF-8, the alignment test, 7-bit controls and a character
        // set: the last character is part of the sequence, not text.
        let emulator = screen("a\u{1B}%Gb\u{1B}#8c\u{1B} Fd\u{1B}(Be\u{1B}$(Df")
        XCTAssertEqual(shown(emulator), "abcdef")
    }

    func testAnEscapeSequenceSplitAfterItsIntermediateWaitsForTheFinalByte() {
        var emulator = TerminalScreenEmulator(columns: 20, rows: 5)
        emulator.feed("a\u{1B}%")
        emulator.feed("Gb")
        XCTAssertEqual(shown(emulator), "ab")
    }

    func testEraseCharactersStopsAtTheRightEdge() {
        // A line padded past the screen width would look like a wrapped
        // line to whatever reads the rows back.
        var emulator = screen("abcdefgh\r\u{1B}[3C\u{1B}[999X", columns: 8, rows: 3)
        XCTAssertEqual(shown(emulator), "abc")
        emulator.feed("\r\nnext")
        XCTAssertNil(
            emulator.textPreceding("next").flatMap { $0.isEmpty ? nil : $0 },
            "the row above was full of erased cells, so the text read as its continuation"
        )
        XCTAssertEqual(shown(emulator), "abc\nnext")
    }
}
