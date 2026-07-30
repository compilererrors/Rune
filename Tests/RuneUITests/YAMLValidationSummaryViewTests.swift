import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class YAMLValidationSummaryViewTests: XCTestCase {
    func testMixedIssueCountsUseCorrectSingularAndPluralForms() {
        let cases = [
            (errors: 1, warnings: 1, expected: "1 error, 1 warning"),
            (errors: 1, warnings: 2, expected: "1 error, 2 warnings"),
            (errors: 2, warnings: 1, expected: "2 errors, 1 warning"),
            (errors: 2, warnings: 2, expected: "2 errors, 2 warnings"),
        ]

        for testCase in cases {
            XCTAssertEqual(
                YAMLValidationSummaryView.summaryTitle(
                    errorCount: testCase.errors,
                    warningCount: testCase.warnings,
                    isValidating: false
                ),
                testCase.expected
            )
        }
    }

    func testSummaryPreservesEmptyValidationAndSingleCategoryWording() {
        XCTAssertEqual(
            YAMLValidationSummaryView.summaryTitle(
                errorCount: 0,
                warningCount: 0,
                isValidating: false
            ),
            "No local YAML problems"
        )
        XCTAssertEqual(
            YAMLValidationSummaryView.summaryTitle(
                errorCount: 0,
                warningCount: 0,
                isValidating: true
            ),
            "Checking local YAML…"
        )
        XCTAssertEqual(
            YAMLValidationSummaryView.summaryTitle(
                errorCount: 1,
                warningCount: 0,
                isValidating: false
            ),
            "1 YAML error"
        )
        XCTAssertEqual(
            YAMLValidationSummaryView.summaryTitle(
                errorCount: 2,
                warningCount: 0,
                isValidating: false
            ),
            "2 YAML errors"
        )
        XCTAssertEqual(
            YAMLValidationSummaryView.summaryTitle(
                errorCount: 0,
                warningCount: 1,
                isValidating: false
            ),
            "1 validation warning"
        )
        XCTAssertEqual(
            YAMLValidationSummaryView.summaryTitle(
                errorCount: 0,
                warningCount: 2,
                isValidating: false
            ),
            "2 validation warnings"
        )
    }

    func testExpandedSingleIssueSizesToItsContentInsteadOfScrollCap() {
        let host = NSHostingView(
            rootView: YAMLValidationSummaryView(
                issues: [
                    YAMLValidationIssue(
                        source: .syntax,
                        severity: .error,
                        message: "Unexpected indentation.",
                        line: 4,
                        column: 1
                    ),
                ],
                isValidating: false,
                expandedListMaxHeight: RuneUILayoutMetrics.yamlSheetValidationListMaxHeight,
                initiallyExpanded: true,
                onSelectIssue: { _ in }
            )
            .frame(width: 520)
        )

        host.layoutSubtreeIfNeeded()

        XCTAssertLessThan(
            host.fittingSize.height,
            RuneUILayoutMetrics.yamlSheetValidationListMaxHeight,
            "A single expanded issue should leave most of the sheet height for the YAML editor."
        )
        XCTAssertFalse(containsScrollView(in: host))
    }

    func testExpandedLargeIssueListRemainsBoundedAndScrollable() {
        let issues = (1...12).map { index in
            YAMLValidationIssue(
                source: .syntax,
                severity: index.isMultiple(of: 2) ? .warning : .error,
                message: "Synthetic YAML issue \(index).",
                line: index,
                column: 2
            )
        }
        let host = NSHostingView(
            rootView: YAMLValidationSummaryView(
                issues: issues,
                isValidating: false,
                expandedListMaxHeight: RuneUILayoutMetrics.yamlSheetValidationListMaxHeight,
                initiallyExpanded: true,
                onSelectIssue: { _ in }
            )
            .frame(width: 520)
        )

        host.layoutSubtreeIfNeeded()

        XCTAssertLessThanOrEqual(
            host.fittingSize.height,
            RuneUILayoutMetrics.yamlSheetValidationListMaxHeight + 70
        )
        XCTAssertTrue(containsScrollView(in: host))
    }

    private func containsScrollView(in view: NSView) -> Bool {
        if view is NSScrollView {
            return true
        }
        return view.subviews.contains(where: containsScrollView)
    }
}
