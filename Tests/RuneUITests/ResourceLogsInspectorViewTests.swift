import XCTest
@testable import RuneUI

final class ResourceLogsInspectorViewTests: XCTestCase {
    func testLogSearchReturnsOriginalTextWhenQueryIsBlank() {
        let result = ResourceLogSearchResult.make(
            text: "alpha\nbeta\ngamma",
            query: "   "
        )

        XCTAssertEqual(result.displayedText, "alpha\nbeta\ngamma")
        XCTAssertFalse(result.isFiltering)
    }

    func testBlankLogSearchCountsCommonLineEndings() {
        let result = ResourceLogSearchResult.make(
            text: "alpha\r\nbeta\rgamma\n",
            query: ""
        )

        XCTAssertEqual(result.totalLineCount, 4)
        XCTAssertEqual(result.matchingLineCount, 4)
    }

    func testLogSearchFiltersMatchingLinesCaseInsensitively() {
        let result = ResourceLogSearchResult.make(
            text: "INFO started\nwarn slow query\nERROR failed\nsecond error\n",
            query: "error"
        )

        XCTAssertEqual(result.matchingLineCount, 2)
        XCTAssertEqual(result.displayedText, "ERROR failed\nsecond error")
        XCTAssertEqual(result.summaryText, "Showing 2 matching lines out of 5.")
    }

    func testLogScrollIdentityIgnoresTailContentChanges() {
        let first = ResourceLogSearchResult.make(
            text: "INFO started",
            query: ""
        )
        let second = ResourceLogSearchResult.make(
            text: "INFO started\nINFO ready",
            query: ""
        )

        XCTAssertEqual(first.scrollIdentityToken, second.scrollIdentityToken)
    }

    func testLogScrollIdentityChangesWhenSearchQueryChanges() {
        let first = ResourceLogSearchResult.make(
            text: "INFO started\nERROR failed",
            query: ""
        )
        let second = ResourceLogSearchResult.make(
            text: "INFO started\nERROR failed",
            query: "error"
        )

        XCTAssertNotEqual(first.scrollIdentityToken, second.scrollIdentityToken)
    }

    func testLargeUnfilteredLogsDeferInitialTextMount() {
        let text = String(repeating: "INFO synthetic benchmark line\n", count: 12_000)
        let result = ResourceLogSearchResult.make(text: text, query: "")

        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }

    func testSmallUnfilteredLogsRenderImmediately() {
        let result = ResourceLogSearchResult.make(text: "INFO ready\nINFO steady", query: "")

        XCTAssertFalse(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }

    func testFilteredLogsRenderMatchesImmediately() {
        let text = (0..<12_000)
            .map { index in
                index.isMultiple(of: 1_000)
                    ? "ERROR synthetic line \(index)"
                    : "INFO synthetic line \(index)"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.make(text: text, query: "error")

        XCTAssertTrue(result.isFiltering)
        XCTAssertFalse(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }
}
