import AppKit
import XCTest
@testable import RuneUI

final class ResourceLogANSIFormatterTests: XCTestCase {
    func testANSIFormatterStripsEscapeSequencesForDisplayedAndCopiedText() {
        let text = "ok \u{1B}[31mfailed\u{1B}[0m done"

        let plain = ResourceLogANSIFormatter.plainText(from: text)
        let attributed = ResourceLogANSIFormatter.attributedString(
            from: text,
            font: NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)
        )

        XCTAssertEqual(plain, "ok failed done")
        XCTAssertEqual(attributed.string, "ok failed done")
        XCTAssertFalse(attributed.string.contains("\u{1B}[31m"))
    }

    func testANSIFormatterAppliesColorOnlyInsideSequenceRange() {
        let text = "plain \u{1B}[32mgreen\u{1B}[0m plain"
        let attributed = ResourceLogANSIFormatter.attributedString(
            from: text,
            font: NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)
        )
        let nsText = attributed.string as NSString
        let greenRange = nsText.range(of: "green")
        let plainRange = nsText.range(of: "plain")

        let greenColor = attributed.attribute(.foregroundColor, at: greenRange.location, effectiveRange: nil) as? NSColor
        let plainColor = attributed.attribute(.foregroundColor, at: plainRange.location, effectiveRange: nil) as? NSColor

        XCTAssertEqual(greenColor, NSColor.systemGreen)
        XCTAssertEqual(plainColor, NSColor.labelColor)
    }
}
