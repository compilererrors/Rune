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

    func testResolvedWindowContentTopInsetKeepsLargeMeasuredInset() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 200),
            200
        )
    }

    func testResolvedWindowContentTopInsetKeepsMeasuredInsetAboveFallback() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 60),
            60
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
    }
}
