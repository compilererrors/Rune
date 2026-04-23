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

    func testLogSearchFiltersMatchingLinesCaseInsensitively() {
        let result = ResourceLogSearchResult.make(
            text: "INFO started\nwarn slow query\nERROR failed\nsecond error\n",
            query: "error"
        )

        XCTAssertEqual(result.matchingLineCount, 2)
        XCTAssertEqual(result.displayedText, "ERROR failed\nsecond error")
        XCTAssertEqual(result.summaryText, "Showing 2 matching lines out of 5.")
    }
}
