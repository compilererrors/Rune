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

    func testResolvedWindowContentTopInsetClampsLowerBound() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: -12),
            RuneUILayoutMetrics.windowContentTopInset
        )
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 3),
            RuneUILayoutMetrics.windowContentTopInset
        )
    }

    func testResolvedWindowContentTopInsetClampsUpperBound() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 200),
            RuneUILayoutMetrics.maxWindowContentTopInset
        )
    }

    func testResolvedWindowContentTopInsetKeepsMeasuredInRange() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 18),
            18
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
}
