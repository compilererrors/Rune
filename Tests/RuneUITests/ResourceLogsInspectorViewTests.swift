import XCTest
@testable import RuneUI

final class ResourceLogsInspectorViewTests: XCTestCase {
    func testLogToolbarUsesStableRowsInsteadOfBreakpointWrapping() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertFalse(source.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(source.contains("LogToolbarScrollRow"))
        XCTAssertTrue(source.contains("private var sourceControls: some View"))
        XCTAssertTrue(source.contains("private var modeControls: some View"))
        XCTAssertTrue(source.contains("private var primaryControls: some View"))
        XCTAssertTrue(source.contains("private var toolbarActions: some View"))
        XCTAssertTrue(source.contains("LogToolbarGroup"))
        XCTAssertTrue(source.contains("LogToolbarPickerField(title: \"Window\")"))
        XCTAssertTrue(source.contains("LogToolbarPickerField(title: \"Container\")"))
        XCTAssertTrue(source.contains("ResourceLogsStatusPanel("))
        XCTAssertTrue(source.contains("private struct ResourceLogsStatusPanel"))
        XCTAssertTrue(source.contains("ResourceLogsSourcePanel(title: \"Pods\""))
        XCTAssertTrue(source.contains(".toggleStyle(.button)"))
        XCTAssertTrue(source.contains(".fixedSize(horizontal: true, vertical: false)"))
        XCTAssertTrue(source.contains(".frame(minHeight: 34)"))
        XCTAssertTrue(source.contains("Label(\"Save Logs\", systemImage: \"square.and.arrow.down\")"))
        XCTAssertTrue(source.contains(".buttonStyle(.borderedProminent)"))
        XCTAssertTrue(source.contains("Label(\"More\", systemImage: \"ellipsis.circle\")"))
        XCTAssertTrue(source.contains("Export Visible Results ZIP"))
        XCTAssertTrue(source.contains("Copy Selection"))
        XCTAssertTrue(source.contains(".controlSize(.small)"))
        XCTAssertTrue(source.contains(".logToolbarButtonFrame()"))
    }

    func testLogToolbarStatusAndControlsUseGroupedNativeChrome() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("LogToolbarGroup"))
        XCTAssertTrue(source.contains("ResourceLogsStatusPanel"))
        XCTAssertTrue(source.contains("private struct ResourceLogsSourcePanel"))
        XCTAssertTrue(source.contains("statusColor"))
        XCTAssertTrue(source.contains("RoundedRectangle(cornerRadius: 8, style: .continuous)"))
        XCTAssertTrue(source.contains("Text(title)"))
        XCTAssertTrue(source.contains("VStack(alignment: .leading, spacing: 4)"))
        XCTAssertTrue(source.contains("Text(title.uppercased())"))
        XCTAssertFalse(source.contains("Circle()\\n                    .fill(statusText.lowercased()"))
    }

    func testLogSearchUsesDraftInputAndExternalSearchPulseFeedback() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("@State private var searchPulseID = 0"))
        XCTAssertTrue(source.contains("applyLogSearchQuery"))
        XCTAssertTrue(source.contains("searchPulseID: searchPulseID"))
        XCTAssertTrue(source.contains("@State private var draftQuery = \"\""))
        XCTAssertTrue(source.contains("@State private var queryCommitTask: Task<Void, Never>?"))
        XCTAssertTrue(source.contains("try? await Task.sleep(nanoseconds: 85_000_000)"))
        XCTAssertTrue(source.contains("LogSearchPulseOverlay"))
    }

    func testLogInspectorShowsInterruptedStreamStateInsideOutputSurface() throws {
        let logsViewSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(logsViewSource.contains("ResourceLogsErrorView(message: errorMessage, onReload: onReload)"))
        XCTAssertTrue(logsViewSource.contains("Button(\"Retry\", action: onReload)"))
        XCTAssertTrue(logsViewSource.contains("Label(\"Reload\", systemImage: \"arrow.clockwise\")"))
        XCTAssertTrue(rootViewSource.contains("errorMessage: viewModel.state.lastLogFetchError"))
        XCTAssertTrue(rootViewSource.contains("return \"Reconnect failed\""))
    }

    func testLogStatusUsesCompactChipInsteadOfFullWidthBanner() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        let statusPanelSource = try XCTUnwrap(source.slice(
            from: "private struct ResourceLogsStatusPanel",
            to: "private struct ResourceLogsSourcePanel"
        ))

        XCTAssertTrue(statusPanelSource.contains(".fixedSize(horizontal: false, vertical: true)"))
        XCTAssertTrue(statusPanelSource.contains(".lineLimit(1)"))
        XCTAssertTrue(statusPanelSource.contains(".font(.caption.weight(.semibold))"))
        XCTAssertFalse(statusPanelSource.contains(".frame(maxWidth: .infinity"))
        XCTAssertFalse(statusPanelSource.contains("statusColor.opacity(0.08)"))
        XCTAssertFalse(statusPanelSource.contains(".strokeBorder(statusColor.opacity(0.22)"))
    }

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
        XCTAssertEqual(result.matchLineNumbers, [3, 4])
        XCTAssertEqual(result.matchPositionText(selectedIndex: 0), "1 of 2")
        XCTAssertEqual(result.matchPositionText(selectedIndex: 1), "2 of 2")
        XCTAssertEqual(result.matchPositionText(selectedIndex: 99), "2 of 2")
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

    func testStructuredFieldSearchQueryMatchesRawJSONWithoutFilteringOutput() {
        let text = """
        {"level":"info","message":"started","pod":"api-0"}
        {"level":"error","message":"failed","pod":"api-1"}
        {"level":"info","message":"recovered","pod":"api-1"}
        """
        let query = ResourceStructuredLogFieldSearch.query(fieldKey: "level", value: "error")
        let result = ResourceLogSearchResult.make(text: text, query: query)

        XCTAssertEqual(query, #""level":"error""#)
        XCTAssertEqual(result.matchingLineCount, 1)
        XCTAssertEqual(result.displayedText, text)
        XCTAssertTrue(result.displayedText.contains(#""level":"info""#))
        XCTAssertTrue(result.displayedText.contains(#""level":"error""#))
    }

    func testStructuredFieldAliasSearchQueryMatchesRawJSONWithoutFilteringOutput() throws {
        let text = """
        {"severity":"error","msg":"failed","requestId":"req-1"}
        {"level":"info","message":"recovered","request_id":"req-2"}
        """
        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)
        let field = try XCTUnwrap(summary.field("level"))
        let query = ResourceStructuredLogFieldSearch.query(field: field, value: "error")
        let result = ResourceLogSearchResult.make(text: text, query: query)

        XCTAssertEqual(query, #""severity":"error""#)
        XCTAssertEqual(result.matchingLineCount, 1)
        XCTAssertEqual(result.displayedText, text)
        XCTAssertTrue(result.displayedText.contains(#""level":"info""#))
        XCTAssertTrue(result.displayedText.contains(#""severity":"error""#))
    }

    func testStructuredLogSummaryPanelIsWiredIntoLogInspector() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("ResourceStructuredLogAnalyzer.analyze(text:"))
        XCTAssertTrue(source.contains("ResourceStructuredLogSummaryPanel("))
        XCTAssertTrue(source.contains("summary: structuredLogSummary"))
        XCTAssertTrue(source.contains("ResourceStructuredLogFieldSearch.query(field: field, value: value)"))
        XCTAssertTrue(source.contains("onSearchFieldSample"))
        XCTAssertTrue(source.contains("onSearchDuplicate"))
    }

    func testLogSearchUIShowsCurrentMatchPosition() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("matchPositionText(selectedIndex: selectedMatchIndex)"))
        XCTAssertTrue(source.contains("ResourceLogsSearchSummaryBar("))
        XCTAssertTrue(source.contains("selectedMatchIndex: selectedSearchMatchIndex"))
        XCTAssertTrue(source.contains(".monospacedDigit()"))
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

    private var resourceLogsInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceLogsInspectorView.swift").path
    }

    private var runeRootViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
    }
}

private extension String {
    func slice(from start: String, to end: String) -> String? {
        guard let startRange = range(of: start),
              let endRange = range(of: end, range: startRange.upperBound..<endIndex) else {
            return nil
        }
        return String(self[startRange.lowerBound..<endRange.lowerBound])
    }
}
