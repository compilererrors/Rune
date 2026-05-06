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

    func testLogSearchFindsMatchesWithoutFilteringOutput() {
        let result = ResourceLogSearchResult.make(
            text: "INFO started\nwarn slow query\nERROR failed\nsecond error\n",
            query: "error"
        )

        XCTAssertEqual(result.matchingLineCount, 2)
        XCTAssertEqual(result.displayedText, "INFO started\nwarn slow query\nERROR failed\nsecond error\n")
        XCTAssertEqual(result.summaryText, "2 matches in 5 total lines.")
        XCTAssertEqual(result.matchRanges.count, 2)
        XCTAssertEqual((result.displayedText as NSString).substring(with: result.matchRanges[0]), "ERROR")
        XCTAssertEqual((result.displayedText as NSString).substring(with: result.matchRanges[1]), "error")
    }

    func testLogSearchNavigationWrapsAndBuildsRangeRequest() {
        let result = ResourceLogSearchResult.make(
            text: "INFO started\nERROR failed\nsecond error\n",
            query: "error"
        )

        XCTAssertEqual(result.nextMatchIndex(from: 0), 1)
        XCTAssertEqual(result.nextMatchIndex(from: 1), 0)
        XCTAssertEqual(result.previousMatchIndex(from: 0), 1)

        let request = result.navigationRequest(selectedIndex: 1, sequence: 3)
        XCTAssertEqual(request?.sequence, 3)
        XCTAssertEqual(request?.range?.location, result.matchRanges[1].location)
        XCTAssertEqual(request?.range?.length, result.matchRanges[1].length)
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

    func testLogScrollIdentityDoesNotResetWhenSearchQueryChanges() {
        let first = ResourceLogSearchResult.make(
            text: "INFO started\nERROR failed",
            query: ""
        )
        let second = ResourceLogSearchResult.make(
            text: "INFO started\nERROR failed",
            query: "error"
        )

        XCTAssertEqual(first.scrollIdentityToken, second.scrollIdentityToken)
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

    func testLargeSearchedLogsStillDeferBecauseOutputIsNotFiltered() {
        let text = (0..<12_000)
            .map { index in
                index.isMultiple(of: 1_000)
                    ? "ERROR synthetic line \(index)"
                    : "INFO synthetic line \(index)"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.make(text: text, query: "error")

        XCTAssertTrue(result.isFiltering)
        XCTAssertTrue(result.displayedText.contains("INFO synthetic line 1"))
        XCTAssertTrue(result.displayedText.contains("INFO synthetic line 11999"))
        XCTAssertEqual(result.matchRanges.count, 12)
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }

    func testLargeInspectorSearchDoesNotCapRenderedLogData() {
        let text = (0..<12_000)
            .map { index in
                index.isMultiple(of: 1_500)
                    ? "ERROR full output line \(index)"
                    : "INFO full output line \(index)"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "error")

        XCTAssertEqual(result.displayedText, text)
        XCTAssertTrue(result.displayedText.contains("INFO full output line 11999"))
        XCTAssertEqual(result.matchRanges.count, 8)
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }
}
