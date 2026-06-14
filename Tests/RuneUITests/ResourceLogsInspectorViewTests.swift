import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
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
        XCTAssertTrue(source.contains("enum ResourceLogsPresentationStyle"))
        XCTAssertTrue(source.contains("case terminalCompact"))
        XCTAssertTrue(source.contains("private var terminalCompactBody: some View"))
        XCTAssertTrue(source.contains("presentationStyle == .terminalCompact ? 8 : 10"))
        XCTAssertTrue(source.contains("LogToolbarGroup"))
        XCTAssertTrue(source.contains("LogToolbarGroup(role: .source)"))
        XCTAssertTrue(source.contains("LogToolbarGroup(role: .source, spacing: 4)"))
        XCTAssertTrue(source.contains("LogToolbarPickerField(title: t(.window), role: .window)"))
        XCTAssertTrue(source.contains("LogToolbarPickerField(title: t(.container), role: .container)"))
        XCTAssertTrue(source.contains("LogToolbarStatusIndicator("))
        XCTAssertTrue(source.contains("private struct LogToolbarStatusIndicator"))
        XCTAssertTrue(source.contains("ResourceLogsSourcePanel(title: \"Pods\""))
        XCTAssertTrue(source.contains(".toggleStyle(.button)"))
        XCTAssertTrue(source.contains(".fixedSize(horizontal: true, vertical: false)"))
        XCTAssertTrue(source.contains(".frame(height: role.height)"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorToolbarSourceGroupHeight"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorToolbarActionGroupHeight"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorToolbarGroupSpacing"))
        XCTAssertTrue(source.contains("RuneUILayoutMetrics.inspectorToolbarControlSpacing"))
        XCTAssertTrue(source.contains("Label(t(.saveLogs), systemImage: \"square.and.arrow.down\")"))
        XCTAssertTrue(source.contains(".buttonStyle(.borderedProminent)"))
        XCTAssertTrue(source.contains("Label(t(.more), systemImage: \"ellipsis.circle\")"))
        XCTAssertTrue(source.contains("t(.exportVisibleResultsZip)"))
        XCTAssertTrue(source.contains("t(.copySelection)"))
        XCTAssertTrue(source.contains(".controlSize(.small)"))
        XCTAssertTrue(source.contains(".logToolbarButtonFrame()"))
        XCTAssertTrue(source.contains(".logToolbarIconButtonFrame()"))
        XCTAssertTrue(source.contains("idealWidth: width"))
        XCTAssertTrue(source.contains("maxHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight"))
        XCTAssertTrue(source.contains(".font(.caption.weight(.semibold))"))
    }

    func testLogToolbarStatusAndControlsUseGroupedNativeChrome() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let toolbarGroupSource = try XCTUnwrap(source.slice(
            from: "private struct LogToolbarGroup",
            to: "private struct LogToolbarScrollRow"
        ))

        XCTAssertTrue(source.contains("LogToolbarGroup"))
        XCTAssertTrue(source.contains("LogToolbarStatusIndicator"))
        XCTAssertTrue(source.contains("private struct ResourceLogsSourcePanel"))
        XCTAssertTrue(source.contains("statusColor"))
        XCTAssertTrue(source.contains("@Environment(\\.runeThemePalette) private var runeThemePalette"))
        XCTAssertTrue(source.contains("RoundedRectangle(cornerRadius: 8, style: .continuous)"))
        XCTAssertTrue(source.contains("RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)"))
        XCTAssertTrue(toolbarGroupSource.contains("runeThemePalette?.row.opacity(0.42)"))
        XCTAssertTrue(toolbarGroupSource.contains("runeThemePalette?.stroke.opacity(0.28)"))
        XCTAssertFalse(toolbarGroupSource.contains("RoundedRectangle(cornerRadius: 9, style: .continuous)"))
        XCTAssertTrue(source.contains("Text(title)"))
        XCTAssertTrue(source.contains("VStack(alignment: .leading, spacing: 5)"))
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
        XCTAssertTrue(source.contains("@FocusState private var isSearchFocused"))
        XCTAssertTrue(source.contains(".keyboardShortcut(\"f\", modifiers: [.command])"))
        XCTAssertTrue(source.contains("matchCase.toggle()"))
        XCTAssertTrue(source.contains("Text(\"Aa\")"))
        XCTAssertTrue(source.contains("LogSearchPulseOverlay"))
    }

    func testLogSearchSupportsMatchCase() {
        let insensitive = ResourceLogSearchResult.make(
            text: "ERROR failed\nerror lower\n",
            query: "error",
            matchCase: false
        )
        let sensitive = ResourceLogSearchResult.make(
            text: "ERROR failed\nerror lower\n",
            query: "error",
            matchCase: true
        )

        XCTAssertEqual(insensitive.matchRanges.count, 2)
        XCTAssertEqual(sensitive.matchRanges.count, 1)
        XCTAssertFalse(insensitive.matchCase)
        XCTAssertTrue(sensitive.matchCase)
    }

    func testLogInspectorShowsInterruptedStreamStateInsideOutputSurface() throws {
        let logsViewSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(logsViewSource.contains("ResourceLogsErrorView(message: errorMessage, onReload: onReload)"))
        XCTAssertTrue(logsViewSource.contains("Button(\"Retry\", action: onReload)"))
        XCTAssertTrue(logsViewSource.contains("Label(t(.reload), systemImage: \"arrow.clockwise\")"))
        XCTAssertTrue(rootViewSource.contains("errorMessage: viewModel.state.lastLogFetchError"))
        XCTAssertTrue(rootViewSource.contains("return \"Reconnect failed\""))
    }

    func testLogStatusUsesCompactChipInsteadOfFullWidthBanner() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        let statusPanelSource = try XCTUnwrap(source.slice(
            from: "private struct LogToolbarStatusIndicator",
            to: "private struct ResourceLogsSourcePanel"
        ))

        XCTAssertTrue(statusPanelSource.contains(".frame(width: 20, height: RuneUILayoutMetrics.inspectorToolbarControlMinHeight)"))
        XCTAssertTrue(statusPanelSource.contains("statusColor.opacity(0.18)"))
        XCTAssertTrue(statusPanelSource.contains(".help(helpText)"))
        XCTAssertTrue(statusPanelSource.contains(".accessibilityLabel(\"Log status: \\(statusText)\")"))
        XCTAssertFalse(statusPanelSource.contains("Text(compactText)"))
        XCTAssertFalse(statusPanelSource.contains("private var compactText"))
        XCTAssertFalse(statusPanelSource.contains(".frame(maxWidth: .infinity"))
        XCTAssertFalse(statusPanelSource.contains(".strokeBorder(statusColor.opacity(0.22)"))
    }

    func testPodLogToolbarUsesEmbeddedFavoritePicker() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let sourceControls = try XCTUnwrap(source.slice(
            from: "private var sourceControls: some View",
            to: "private var primaryControls: some View"
        ))

        XCTAssertTrue(sourceControls.contains("FavoritePodPicker("))
        XCTAssertTrue(sourceControls.contains("rowTitle: { $0.name }"))
        XCTAssertTrue(sourceControls.contains("rowDetail: { \"\\($0.namespace) - \\($0.status)\" }"))
        XCTAssertTrue(sourceControls.contains("isFavoritePod: isFavoritePod"))
        XCTAssertTrue(sourceControls.contains("onToggleFavoritePod: onToggleFavoritePod"))
        XCTAssertTrue(sourceControls.contains("selection: selectedPodID"))
        XCTAssertTrue(sourceControls.contains(".accessibilityIdentifier(\"pod-log-favorite-picker\")"))
        XCTAssertFalse(sourceControls.contains("podPickerTitle(pod)"))
        XCTAssertFalse(sourceControls.contains("pod-log-favorite-toggle"))
    }

    func testFavoritePodPickerSelectionSurvivesFavoriteSorting() {
        let pods = [
            PodSummary(name: "api", namespace: "default", status: "Running"),
            PodSummary(name: "worker", namespace: "default", status: "Running"),
            PodSummary(name: "queue", namespace: "default", status: "Running")
        ]
        var favorites = Set([pods[1].id])

        XCTAssertEqual(
            FavoritePodPickerPresentation.sortedPods(pods, isFavoritePod: { favorites.contains($0.id) }).map(\.id),
            [pods[1].id, pods[0].id, pods[2].id]
        )
        XCTAssertEqual(
            FavoritePodPickerPresentation.selectedPod(in: pods, selection: pods[0].id)?.id,
            pods[0].id
        )

        favorites.insert(pods[0].id)

        XCTAssertEqual(
            FavoritePodPickerPresentation.selectedPod(in: pods, selection: pods[2].id)?.id,
            pods[2].id,
            "Changing favorites must not pin the picker to the favorite pod; selection remains an independent pod id."
        )
        XCTAssertEqual(
            FavoritePodPickerPresentation.rowIcon(for: pods[2], selection: pods[2].id, isFavoritePod: { favorites.contains($0.id) }),
            "checkmark"
        )
        XCTAssertEqual(
            FavoritePodPickerPresentation.rowIcon(for: pods[0], selection: pods[2].id, isFavoritePod: { favorites.contains($0.id) }),
            "star.fill"
        )
    }

    func testTailControlDoesNotAddPauseButtonBesideTail() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let modeControls = try XCTUnwrap(source.slice(
            from: "private var modeControls: some View",
            to: "private var toolbarActions: some View"
        ))

        XCTAssertTrue(modeControls.contains("tailControl"))
        XCTAssertTrue(modeControls.contains("LogToolbarStatusIndicator("))
        XCTAssertTrue(modeControls.contains("statusText: statusText"))
        XCTAssertTrue(modeControls.contains("showsPreviousHint: includePreviousLogs"))
        XCTAssertTrue(modeControls.contains("if isStreamPaused"))
        XCTAssertTrue(modeControls.contains("Label(t(.previous), systemImage: \"clock.arrow.circlepath\")"))
        XCTAssertTrue(modeControls.contains("Label(t(.resume), systemImage: \"play.fill\")"))
        XCTAssertTrue(modeControls.contains("Label(t(.pause), systemImage: \"pause.fill\")"))
        XCTAssertTrue(modeControls.contains(".buttonStyle(.bordered)"))
        XCTAssertTrue(modeControls.contains(".buttonStyle(.borderedProminent)"))
        XCTAssertTrue(modeControls.contains(".logToolbarIconButtonFrame()"))
        XCTAssertTrue(modeControls.contains("Button(t(.stopTail))"))
        XCTAssertFalse(modeControls.contains("Toggle(\"Tail\""))
        XCTAssertFalse(modeControls.contains("Button(isStreamPaused ? \"Resume\" : \"Pause\""))
    }

    func testTerminalCompactLogsToolbarKeepsModeControlsOnFirstRowAndActionsOnSecondRow() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let compactBody = try XCTUnwrap(source.slice(
            from: "private var terminalCompactBody: some View",
            to: "private var sourceControls: some View"
        ))

        XCTAssertTrue(compactBody.contains("LogToolbarScrollRow {\n                primaryControls"))
        XCTAssertTrue(compactBody.contains("LogToolbarScrollRow {\n                toolbarActions"))
        XCTAssertFalse(compactBody.contains("LogToolbarScrollRow {\n                modeControls"))
        XCTAssertFalse(compactBody.contains("ResourceLogsStatusPanel("))
        XCTAssertFalse(compactBody.contains("statusText: statusText,\n                showsPreviousHint: includePreviousLogs"))
    }

    func testLogToolbarStatusIndicatorLivesWithModeControlsAndHasHelp() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let indicatorSource = try XCTUnwrap(source.slice(
            from: "private struct LogToolbarStatusIndicator",
            to: "private struct ResourceLogsSourcePanel"
        ))

        XCTAssertTrue(indicatorSource.contains(".help(helpText)"))
        XCTAssertTrue(indicatorSource.contains(".accessibilityLabel(\"Log status: \\(statusText)\")"))
        XCTAssertTrue(indicatorSource.contains("Previous logs only exist for restarted containers."))
        XCTAssertTrue(indicatorSource.contains("RuneUILayoutMetrics.inspectorToolbarControlMinHeight"))
        XCTAssertFalse(indicatorSource.contains("Text(compactText)"))
    }

    func testTerminalLogTabsExposePodNamesOnHover() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let tabBarSource = try XCTUnwrap(source.slice(
            from: "struct TerminalLogTabBar: View",
            to: "private extension View"
        ))

        XCTAssertTrue(tabBarSource.contains("Text(\"\\(number) \\(tab.title)\")"))
        XCTAssertTrue(tabBarSource.contains(".help(tab.helpText)"))
        XCTAssertTrue(tabBarSource.contains("Text(subtitle)"))
        XCTAssertTrue(tabBarSource.contains(".help(\"Close log tab for \\(tab.title)\")"))
    }

    func testTerminalLogTabsUseRuneSurfaceChrome() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let tabBarSource = try XCTUnwrap(source.slice(
            from: "struct TerminalLogTabBar: View",
            to: "private extension View"
        ))

        XCTAssertTrue(tabBarSource.contains("RuneSurfaceBackground(kind: .inset)"))
        XCTAssertTrue(tabBarSource.contains("RuneSurfaceBackground(kind: .listRow(isSelected: isActive))"))
        XCTAssertTrue(tabBarSource.contains("Capsule()"))
        XCTAssertTrue(tabBarSource.contains(".frame(width: 3, height: 16)"))
        XCTAssertFalse(tabBarSource.contains("RuneSurfaceBackground(kind: .editor)"))
        XCTAssertFalse(tabBarSource.contains(".frame(height: 2)"))
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

    func testSimpleModeSkipsStructuredLogSummaryAnalysis() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("@AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false"))
        XCTAssertTrue(source.contains("if !simpleMode"))
        XCTAssertTrue(source.contains(".task(id: \"\\(simpleMode):\\(logText)\")"))
        XCTAssertTrue(source.contains("guard !simpleMode else"))
        XCTAssertTrue(source.contains("structuredLogSummary = ResourceStructuredLogAnalyzer.analyze(text: \"\")"))
    }

    func testLogSearchUIShowsCurrentMatchPosition() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("matchPositionText(selectedIndex: selectedMatchIndex)"))
        XCTAssertTrue(source.contains("ResourceLogsSearchSummaryBar("))
        XCTAssertTrue(source.contains("selectedMatchIndex: selectedSearchMatchIndex"))
        XCTAssertTrue(source.contains(".monospacedDigit()"))
        XCTAssertTrue(source.contains("@State private var isJumpPopoverPresented = false"))
        XCTAssertTrue(source.contains("prepareJumpPopover(for: searchSummary)"))
        XCTAssertTrue(source.contains("jumpToMatchPopover(searchSummary)"))
        XCTAssertTrue(source.contains("private func commitJump(to searchSummary: ResourceLogSearchResult)"))
        XCTAssertTrue(source.contains("Button(\"Go\")"))
        XCTAssertTrue(source.contains("Go to match"))
        XCTAssertTrue(source.contains("RoundedRectangle(cornerRadius: 9, style: .continuous)"))
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

    func testLargeUnfilteredLogsUseVirtualizedTextSurface() {
        let text = String(repeating: "INFO synthetic benchmark line\n", count: 12_000)
        let result = ResourceLogSearchResult.make(text: text, query: "")

        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }

    func testSmallUnfilteredLogsRenderImmediately() {
        let result = ResourceLogSearchResult.make(text: "INFO ready\nINFO steady", query: "")

        XCTAssertFalse(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
    }

    func testLargeSearchedLogsUseVirtualizedTextSurfaceWithoutCappingData() {
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

    func testWideFewLineLogsRenderWithRegularTextSurfaceInsteadOfVirtualizedSurface() throws {
        let widePayload = String(repeating: " payload=synthetic-wide-log-field", count: 140)
        let text = (0..<80)
            .map { index in "line-\(String(format: "%06d", index))\(widePayload)" }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "")

        XCTAssertGreaterThan(result.displayedText.utf8.count, ResourceLogsDeferredRenderingPolicy.deferredOutputThreshold)
        XCTAssertEqual(result.textIndex.lineCount, 80)
        XCTAssertFalse(
            ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result),
            "A small number of very wide log lines should not use the virtualized surface; that path can look clipped or blank because it optimizes for deep line counts."
        )

        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        XCTAssertTrue(source.contains("allowsAutomaticLargeTextSurface: false"))
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

    func testLargeLogSearchNavigationKeepsFocusWhenNextMatchStaysOnSameLine() throws {
        let text = "ERROR first ERROR second\nINFO recovered\n"
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "error")

        XCTAssertEqual(result.matchLineNumber(selectedIndex: 0), 1)
        XCTAssertEqual(result.matchLineNumber(selectedIndex: 1), 1)
        XCTAssertNotEqual(
            result.largeTextNavigationRevision(selectedIndex: 0, sequence: 1),
            result.largeTextNavigationRevision(selectedIndex: 1, sequence: 1)
        )

        let inspectorSource = try String(contentsOfFile: inspectorTextViewsPath, encoding: .utf8)
        let logsSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let sharedSource = try String(contentsOfFile: largeTextSurfacePath, encoding: .utf8)

        XCTAssertTrue(inspectorSource.contains("largeTextScrollTargetRevision"))
        XCTAssertTrue(logsSource.contains("largeTextNavigationRevision"))
        XCTAssertTrue(sharedSource.contains("scrollTargetRevision"))
        XCTAssertTrue(
            sharedSource.contains("matchScrollTargetID"),
            "Virtualized log search must use a per-navigation scroll identity so “Next match” still scrolls when the line number is unchanged."
        )
    }

    /// Regression guard for pod logs that look like long single-line Spring-style rows: the inspector must not stop
    /// laying out after roughly one viewport worth of rows (~80 on common laptop heights), leaving a blank panel.
    @MainActor
    func testWideSyntheticPodLogsLayOutTranscriptTallerThanEightyLineStride() async throws {
        let lineCount = 150
        let text = Self.makeMockWidePodLogTranscript(lineCount: lineCount)
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "")

        XCTAssertGreaterThan(text.utf8.count, ResourceLogsDeferredRenderingPolicy.deferredOutputThreshold)
        XCTAssertEqual(result.textIndex.lineCount, lineCount)
        XCTAssertFalse(
            ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result),
            "This fixture targets the non-virtualized AppKit log surface (few logical lines, very wide UTF-8)."
        )

        let host = NSHostingController(
            rootView: PodLogsInspectorPane(
                selectedLogPreset: .constant(.largeTail),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                isLoadingLogs: false,
                isLoadingResources: false,
                errorMessage: nil,
                statusText: "Tail off",
                containerOptions: ["mock"],
                logText: text,
                readOnlyResetID: "mock-wide-pod-log-layout",
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )
            .frame(width: 920, height: 680)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 920, height: 680),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)

        let textViews = findTextViews(in: host.view)
        let logTextView = try XCTUnwrap(
            textViews.filter({ $0.string == text }).max(by: { $0.bounds.width * $0.bounds.height < $1.bounds.width * $1.bounds.height }),
            "Expected a read-only NSTextView containing the full mock transcript."
        )

        logTextView.layoutSubtreeIfNeeded()
        guard let layoutManager = logTextView.layoutManager,
              let textContainer = logTextView.textContainer else {
            XCTFail("Missing layoutManager/textContainer")
            return
        }

        layoutManager.ensureLayout(for: textContainer)
        let usedHeight = layoutManager.usedRect(for: textContainer).height
        let lineHeight = layoutManager.defaultLineHeight(for: logTextView.font ?? NSFont.monospacedSystemFont(ofSize: 12, weight: .regular))

        XCTAssertGreaterThan(
            usedHeight,
            lineHeight * 90,
            "Mock transcript has \(lineCount) logical newlines; layout height must exceed a ~80-line band or the log panel looks clipped with empty space below."
        )
    }

    /// Many lines plus large UTF-8 use the virtualized log surface; scrollable content must still span the full
    /// line count (not an ~80-line-tall blank cut-off).
    @MainActor
    func testDeepSyntheticPodLogsScrollExceedsEightyLineStride() async throws {
        let lineCount = 1_100
        let text = Self.makeMockDeepPodLogTranscript(lineCount: lineCount, minUTF8PerLine: 240)
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "")

        XCTAssertGreaterThan(text.utf8.count, ResourceLogsDeferredRenderingPolicy.deferredOutputThreshold)
        XCTAssertGreaterThanOrEqual(result.textIndex.lineCount, ResourceLogsDeferredRenderingPolicy.deferredLineCountThreshold)
        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))

        let host = NSHostingController(
            rootView: PodLogsInspectorPane(
                selectedLogPreset: .constant(.largeTail),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                isLoadingLogs: false,
                isLoadingResources: false,
                errorMessage: nil,
                statusText: "Tail off",
                containerOptions: ["mock"],
                logText: text,
                readOnlyResetID: "mock-deep-pod-log-layout",
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )
            .frame(width: 920, height: 680)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 920, height: 680),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)

        let scrollViews = findScrollViews(in: host.view)
        let docHeights = scrollViews.compactMap { $0.documentView?.bounds.height }
        let maxDocHeight = try XCTUnwrap(docHeights.max(), "Expected at least one scrollable document surface.")
        let expectedMinHeight = CGFloat(lineCount) * 14

        XCTAssertGreaterThan(
            maxDocHeight,
            expectedMinHeight,
            "Deep mock logs with \(lineCount) lines must size scroll content near one row per logical line; an ~80-row-tall document with thousands of lines is the reported pod log clip-off bug."
        )
    }

    @MainActor
    func testWideFewLinePodLogsFillInspectorOutputHeight() async throws {
        let longPayload = String(repeating: " payload=synthetic-wide-log-field", count: 140)
        let text = (0..<80)
            .map { index in "line-\(String(format: "%06d", index))\(longPayload)" }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "")

        XCTAssertFalse(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))
        XCTAssertEqual(result.textIndex.lineCount, 80)

        let host = NSHostingController(
            rootView: PodLogsInspectorPane(
                selectedLogPreset: .constant(.largeTail),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                isLoadingLogs: false,
                isLoadingResources: false,
                errorMessage: nil,
                statusText: "Tail off",
                containerOptions: ["app"],
                logText: text,
                readOnlyResetID: "large-few-line-log-layout",
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )
            .frame(width: 920, height: 680)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 920, height: 680),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { closeTestWindow(window) }

        try await settle(window: window)

        let scrollViews = findScrollViews(in: host.view)
        let outputScrollView = try XCTUnwrap(
            scrollViews.max(by: { $0.frame.height < $1.frame.height }),
            "The large log output should mount a scroll view."
        )

        XCTAssertGreaterThanOrEqual(
            outputScrollView.frame.height,
            360,
            "Large few-line logs must use the remaining inspector height; otherwise only a few lines render and the rest of the panel looks blank."
        )
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

    private var inspectorTextViewsPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/InspectorTextViews.swift").path
    }

    private var largeTextSurfacePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneSharedUI/RuneSharedComponents.swift").path
    }

    @MainActor
    private func settle(window: NSWindow) async throws {
        for _ in 0..<8 {
            window.contentView?.layoutSubtreeIfNeeded()
            try await Task.sleep(nanoseconds: 25_000_000)
        }
    }

    @MainActor
    private func closeTestWindow(_ window: NSWindow) {
        window.orderOut(nil)
        window.contentViewController = nil
        window.contentView = nil
    }

    @MainActor
    private func findScrollViews(in view: NSView) -> [NSScrollView] {
        var matches: [NSScrollView] = []
        if let scrollView = view as? NSScrollView {
            matches.append(scrollView)
        }
        for subview in view.subviews {
            matches.append(contentsOf: findScrollViews(in: subview))
        }
        return matches
    }

    @MainActor
    private func findTextViews(in view: NSView) -> [NSTextView] {
        var matches: [NSTextView] = []
        if let textView = view as? NSTextView {
            matches.append(textView)
        }
        for subview in view.subviews {
            matches.append(contentsOf: findTextViews(in: subview))
        }
        return matches
    }

    private static func makeMockWidePodLogTranscript(lineCount: Int) -> String {
        // Keep each row wide enough that the merged UTF-8 crosses the inspector’s “large payload” threshold
        // (same shape as multi-field service logs) without referencing any real workload.
        let filler = String(repeating: " f=mock", count: 260)
        return (0..<lineCount)
            .map { idx in
                "MOCK-\(String(format: "%04d", idx))Z MOCK-LEVEL 0 --- [mock-app] mock-thread mock.MockClass\(filler)"
            }
            .joined(separator: "\n")
    }

    private static func makeMockDeepPodLogTranscript(lineCount: Int, minUTF8PerLine: Int) -> String {
        let prefix = "MOCK-"
        let padLength = max(1, minUTF8PerLine - prefix.count - 8)
        let pad = String(repeating: "x", count: padLength)
        return (0..<lineCount)
            .map { idx in
                "\(prefix)\(String(format: "%05d", idx))-\(pad)"
            }
            .joined(separator: "\n")
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
