import XCTest
@testable import RuneUI

/// Nested corners are concentric (Apple HIG, macOS 26 design: an outer
/// radius equals the inner radius plus the padding between the shapes).
/// Loose nesting (12 pt or more) caps the step, but an outer shape is always
/// rounder than what it holds.
final class RuneConcentricRadiusTests: XCTestCase {
    func testScaleIsStrictlyIncreasing() {
        XCTAssertEqual(RuneRadius.scale, RuneRadius.scale.sorted())
        XCTAssertEqual(Set(RuneRadius.scale).count, RuneRadius.scale.count)
    }

    func testControlCardIsConcentricWithItsControls() {
        // A 28 pt control in a 44 pt card sits 8 pt from the card edge.
        let inset = (RuneUILayoutMetrics.inspectorToolbarActionGroupHeight - RuneControlHeight.toolbar) / 2
        XCTAssertEqual(RuneUILayoutMetrics.controlCardCornerRadius, RuneRadius.concentric(inner: RuneRadius.control, inset: inset))
        XCTAssertEqual(RuneRadius.group, RuneRadius.control + inset)
    }

    func testFocusOutlineIsConcentricWithSidebarRows() {
        // The outline is 4 pt inside the pane; rows start at the 12 pt sidebar padding.
        let inset = RuneUILayoutMetrics.sidebarPadding - RuneSpacing.xxs
        XCTAssertEqual(RuneRadius.concentric(inner: RuneRadius.control, inset: inset), RuneRadius.group)
    }

    func testTerminalTabStripIsConcentricWithItsTabs() {
        let inset: CGFloat = (38 - 28) / 2
        XCTAssertEqual(RuneUILayoutMetrics.terminalTabStripCornerRadius, RuneRadius.tab + inset)
    }

    func testFindBarIsConcentricWithItsCompactControls() {
        XCTAssertEqual(RuneUILayoutMetrics.findBarCornerRadius, RuneRadius.chip + RuneUILayoutMetrics.inspectorControlChromeVerticalPadding)
    }

    func testOuterShapesStayRounderThanWhatTheyHold() {
        // Pane shell → cards, card → document frame, card → control, control → chip.
        XCTAssertGreaterThan(RuneUILayoutMetrics.paneShellCornerRadius, RuneUILayoutMetrics.controlCardCornerRadius)
        XCTAssertGreaterThan(RuneUILayoutMetrics.groupedContentCornerRadius, RuneRadius.card)
        XCTAssertGreaterThan(RuneRadius.card, RuneRadius.control)
        XCTAssertEqual(RuneRadius.chip, RuneRadius.control - 2, "shapes inside a control sit 2 pt in")
        XCTAssertEqual(RuneUILayoutMetrics.groupedContentCornerRadius, RuneRadius.group, "grouped containers hold controls")
    }
}
