import Foundation
import RuneCore
import XCTest

final class UTF8StreamDecoderTests: XCTestCase {
    private func decode(_ pieces: [[UInt8]]) -> String {
        var decoder = UTF8StreamDecoder()
        return pieces.map { decoder.decode(Data($0)) }.joined() + decoder.finish()
    }

    func testACharacterSplitBetweenTwoPiecesIsDecodedWhole() {
        let bytes = Array("räksmörgås 🚀 日本".utf8)
        for split in 0...bytes.count {
            XCTAssertEqual(
                decode([Array(bytes[..<split]), Array(bytes[split...])]),
                "räksmörgås 🚀 日本",
                "split at byte \(split)"
            )
        }
    }

    func testOneByteAtATime() {
        let text = "å🚀é漢"
        XCTAssertEqual(decode(text.utf8.map { [$0] }), text)
    }

    func testNothingIsHeldBackForCompleteText() {
        var decoder = UTF8StreamDecoder()
        XCTAssertEqual(decoder.decode(Data("plain ascii".utf8)), "plain ascii")
        XCTAssertEqual(decoder.decode(Data("åäö".utf8)), "åäö")
        XCTAssertEqual(decoder.finish(), "")
    }

    func testInvalidBytesAreReplacedWhereTheyStand() {
        // A lone continuation byte and an impossible lead byte are not the
        // start of a character; they are not kept for the next piece.
        var decoder = UTF8StreamDecoder()
        XCTAssertEqual(decoder.decode(Data([0x61, 0x80])), "a\u{FFFD}")
        XCTAssertEqual(decoder.decode(Data([0xFF])), "\u{FFFD}")
        XCTAssertEqual(decoder.decode(Data([0x62])), "b")
        XCTAssertEqual(decoder.finish(), "")
    }

    func testACharacterThatNeverCompletesIsReplacedAtTheEnd() {
        var decoder = UTF8StreamDecoder()
        XCTAssertEqual(decoder.decode(Data([0x61, 0xF0, 0x9F])), "a")
        XCTAssertEqual(decoder.finish(), "\u{FFFD}")
        XCTAssertEqual(decoder.finish(), "")
    }

    func testAnUnfinishedCharacterFollowedByOtherTextIsReplacedOnce() {
        var decoder = UTF8StreamDecoder()
        XCTAssertEqual(decoder.decode(Data([0xC3])), "")
        XCTAssertEqual(decoder.decode(Data("ok".utf8)), "\u{FFFD}ok")
    }

    func testSlicesOfLargerDataDecode() {
        // Frames arrive as slices that do not start at index zero.
        let frame = Data([0x01] + Array("åäö".utf8))
        var decoder = UTF8StreamDecoder()
        XCTAssertEqual(decoder.decode(frame.dropFirst()), "åäö")
        let split = Data([0x01, 0xC3])
        XCTAssertEqual(decoder.decode(split.dropFirst()), "")
        XCTAssertEqual(decoder.decode(Data([0xA5])), "å")
    }
}
