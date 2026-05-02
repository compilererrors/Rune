import XCTest
@testable import RuneCore
@testable import RuneUI

final class RuneUILayoutMetricsTests: XCTestCase {
    func testResolvedWindowContentTopInsetUsesDefaultWhenNil() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: nil),
            RuneUILayoutMetrics.windowContentTopInset
        )
    }

    func testResolvedWindowContentTopInsetIgnoresLowerOutliers() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: -12),
            RuneUILayoutMetrics.windowContentTopInset
        )
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 3),
            RuneUILayoutMetrics.windowContentTopInset
        )
    }

    func testResolvedWindowContentTopInsetIgnoresUpperOutliers() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 200),
            RuneUILayoutMetrics.windowContentTopInset
        )
    }

    func testResolvedWindowContentTopInsetDoesNotChangeAfterMeasuredInsetArrives() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 18),
            RuneUILayoutMetrics.windowContentTopInset
        )
    }

    func testSharedPaneMetricsStayConsistent() {
        XCTAssertGreaterThan(RuneUILayoutMetrics.paneOuterPadding, RuneUILayoutMetrics.paneInnerPadding)
        XCTAssertGreaterThan(RuneUILayoutMetrics.headerChipHeight, 24)
        XCTAssertGreaterThan(RuneUILayoutMetrics.headerChipHorizontalPadding, 0)
        XCTAssertGreaterThanOrEqual(
            RuneUILayoutMetrics.windowContentTopInset,
            RuneUILayoutMetrics.minWindowContentTopInset
        )
        XCTAssertLessThanOrEqual(
            RuneUILayoutMetrics.windowContentTopInset,
            RuneUILayoutMetrics.maxWindowContentTopInset
        )
    }

    func testDetailPaneCanExpandFurtherWhenSidebarIsHidden() {
        XCTAssertGreaterThan(
            RuneUILayoutMetrics.splitDetailColumnExpandedMaxWidth,
            RuneUILayoutMetrics.splitDetailColumnMaxWidth
        )
    }
}
