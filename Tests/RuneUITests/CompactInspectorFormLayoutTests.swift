import AppKit
import RuneCore
import SwiftUI
import XCTest
@testable import RuneUI

final class CompactInspectorFormLayoutTests: XCTestCase {
    @MainActor
    func testMatchCaseControlKeepsSharedIconActionHitTarget() {
        let size = NSHostingView(
            rootView: RuneMatchCaseButton(isSelected: .constant(false))
        ).fittingSize

        XCTAssertGreaterThanOrEqual(size.width, RuneUILayoutMetrics.iconButtonSize)
        XCTAssertGreaterThanOrEqual(size.height, RuneUILayoutMetrics.iconButtonSize)
        XCTAssertLessThanOrEqual(size.width, RuneUILayoutMetrics.iconButtonSize + 1)
        XCTAssertLessThanOrEqual(size.height, RuneUILayoutMetrics.iconButtonSize + 1)
    }

    @MainActor
    func testTerminalSelectorRowsStackAtInspectorMinimumWithoutHidingActions() {
        let pod = PodSummary(
            name: "synthetic-api-0",
            namespace: "sample",
            status: "Running"
        )

        let selectorCompact = fittingSize(
            TerminalPodSelectorRow(
                title: "Port-forward pod",
                systemImage: "point.3.connected.trianglepath.dotted",
                pods: [pod],
                selection: .constant(pod.id)
            ),
            width: TerminalPodControlLayoutMetrics.supportedInspectorWidth
        )
        let selectorWide = fittingSize(
            TerminalPodSelectorRow(
                title: "Port-forward pod",
                systemImage: "point.3.connected.trianglepath.dotted",
                pods: [pod],
                selection: .constant(pod.id)
            ),
            width: 760
        )

        let sessionCompact = fittingSize(
            sessionRow(pod: pod),
            width: TerminalPodControlLayoutMetrics.supportedInspectorWidth
        )
        let sessionWide = fittingSize(sessionRow(pod: pod), width: 760)

        XCTAssertEqual(selectorCompact.width, TerminalPodControlLayoutMetrics.supportedInspectorWidth, accuracy: 0.5)
        XCTAssertGreaterThan(selectorCompact.height, selectorWide.height)
        XCTAssertEqual(sessionCompact.width, TerminalPodControlLayoutMetrics.supportedInspectorWidth, accuracy: 0.5)
        XCTAssertGreaterThan(sessionCompact.height, sessionWide.height)
        XCTAssertLessThanOrEqual(
            TerminalPodControlLayoutMetrics.compactPickerWidth + 20,
            TerminalPodControlLayoutMetrics.supportedInspectorWidth
        )
    }

    @MainActor
    func testInspectorAndTerminalFindBarsUseTwoRowsAt320Points() {
        let inspectorIndex = InspectorFindIndex(
            text: "alpha\nbeta alpha",
            query: "alpha",
            matchCase: false
        )
        let terminalIndex = TerminalTranscriptSearchIndex(
            text: "alpha\nbeta alpha",
            query: "alpha",
            matchCase: false
        )

        let inspectorCompact = fittingSize(
            InspectorFindBar(
                placeholder: "Find in YAML",
                searchIndex: inspectorIndex,
                query: .constant("alpha"),
                matchCase: .constant(false),
                selectedMatchIndex: .constant(0),
                isPresented: .constant(true)
            ),
            width: RuneFindBarMetrics.supportedInspectorWidth
        )
        let inspectorWide = fittingSize(
            InspectorFindBar(
                placeholder: "Find in YAML",
                searchIndex: inspectorIndex,
                query: .constant("alpha"),
                matchCase: .constant(false),
                selectedMatchIndex: .constant(0),
                isPresented: .constant(true)
            ),
            width: 760
        )

        let terminalCompact = fittingSize(
            terminalSearchBar(index: terminalIndex),
            width: RuneFindBarMetrics.supportedInspectorWidth
        )
        let terminalWide = fittingSize(terminalSearchBar(index: terminalIndex), width: 760)

        XCTAssertEqual(inspectorCompact.width, RuneFindBarMetrics.supportedInspectorWidth, accuracy: 0.5)
        XCTAssertGreaterThan(inspectorCompact.height, inspectorWide.height)
        XCTAssertEqual(terminalCompact.width, RuneFindBarMetrics.supportedInspectorWidth, accuracy: 0.5)
        XCTAssertGreaterThan(terminalCompact.height, terminalWide.height)
    }

    @MainActor
    func testLogFindBarHeightIsInvariantAcrossAsyncMatchStatesAndWidths() {
        let manyMatchText = String(repeating: "alpha ", count: 9_999)
        let states: [(query: String, result: ResourceLogSearchResult?)] = [
            ("", ResourceLogSearchResult.make(text: manyMatchText, query: "")),
            ("alpha", nil),
            ("alpha", ResourceLogSearchResult.make(text: "alpha", query: "alpha")),
            ("alpha", ResourceLogSearchResult.make(text: manyMatchText, query: "alpha")),
            ("missing", ResourceLogSearchResult.make(text: manyMatchText, query: "missing"))
        ]

        for width in [CGFloat(248), 320, 420, 820] {
            let sizes = states.map { state in
                fittingSize(logSearchBar(query: state.query, result: state.result), width: width)
            }
            let heights = sizes.map(\.height)
            let minimumHeight = heights.min() ?? 0
            let maximumHeight = heights.max() ?? 0

            XCTAssertLessThanOrEqual(maximumHeight - minimumHeight, 0.5)
            XCTAssertLessThanOrEqual(maximumHeight, 36)
        }
    }

    @MainActor
    func testSearchAndRepeatedLinesStayInOneCompactTwoRowPanel() {
        let repeatedText = (0..<15).map { _ in "synthetic repeated line" }.joined(separator: "\n")
            + "\nsynthetic unique line"
        let summary = ResourceStructuredLogAnalyzer.analyze(text: repeatedText)
        let result = ResourceLogSearchResult.make(text: repeatedText, query: "synthetic")
        XCTAssertFalse(summary.duplicateLines.isEmpty)

        for width in [CGFloat(248), 320, 420, 820] {
            let pending = fittingSize(
                logExplorePanel(query: "synthetic", result: nil, summary: summary),
                width: width
            )
            let current = fittingSize(
                logExplorePanel(query: "synthetic", result: result, summary: summary),
                width: width
            )
            let searchOnly = fittingSize(
                logExplorePanel(
                    query: "synthetic",
                    result: result,
                    summary: ResourceStructuredLogAnalyzer.analyze(text: "")
                ),
                width: width
            )
            let repeatedOnly = fittingSize(
                ResourceStructuredLogSummaryPanel(
                    summary: summary,
                    onSearchFieldSample: { _, _ in },
                    onSearchDuplicate: { _ in }
                ),
                width: width
            )

            XCTAssertEqual(pending.height, current.height, accuracy: 0.5)
            XCTAssertLessThanOrEqual(current.height, 80)
            XCTAssertLessThanOrEqual(searchOnly.height, 44)
            XCTAssertLessThanOrEqual(repeatedOnly.height, ResourceLogsLayoutMetrics.insightsRowHeight + 1)
        }
    }

    @MainActor
    func testRolloutHistorySwitchesFromColumnsToCompactRowsAt320Points() {
        let rows = [
            DeploymentRolloutHistoryRow(
                revision: "42",
                replicaSet: "synthetic-api-7956c7c8db",
                changeCause: "image updated by synthetic release"
            ),
            DeploymentRolloutHistoryRow(
                revision: "43",
                replicaSet: "synthetic-api-5fb7796f65",
                changeCause: "configuration updated"
            )
        ]

        let compact = fittingSize(
            DeploymentRolloutHistoryTable(rows: rows),
            width: DeploymentRolloutHistoryLayoutMetrics.supportedInspectorWidth
        )
        let wide = fittingSize(DeploymentRolloutHistoryTable(rows: rows), width: 700)

        XCTAssertEqual(compact.width, DeploymentRolloutHistoryLayoutMetrics.supportedInspectorWidth, accuracy: 0.5)
        XCTAssertGreaterThan(compact.height, wide.height)
        XCTAssertGreaterThan(
            DeploymentRolloutHistoryLayoutMetrics.minimumTableWidth,
            DeploymentRolloutHistoryLayoutMetrics.supportedInspectorWidth
        )
    }

    func testAdoptersKeepExistingScrollableDocumentAndToolbarSurfaces() throws {
        let terminal = try source("Sources/RuneUI/Views/TerminalTranscriptSurface.swift")
        let logs = try source("Sources/RuneUI/Views/ResourceLogsInspectorView.swift")
        let inspector = try source("Sources/RuneUI/Views/InspectorFind.swift")
        let rollout = try source("Sources/RuneUI/Views/DeploymentRolloutHistoryView.swift")

        XCTAssertTrue(terminal.contains("InspectorTextSurface(minHeight: height)"))
        XCTAssertTrue(terminal.contains("RuneFindBarChrome(\"Terminal find controls\")"))
        XCTAssertTrue(inspector.contains("RuneFindBarChrome(\"Inspector find controls\")"))
        XCTAssertTrue(logs.contains("struct ResourceLogsExplorePanel"))
        XCTAssertTrue(logs.contains(".background(RuneSurfaceBackground(kind: .inset))"))
        XCTAssertTrue(logs.contains("struct LogToolbarScrollRow"))
        XCTAssertTrue(logs.contains("ScrollView(.horizontal, showsIndicators: false)"))
        XCTAssertTrue(rollout.contains("ScrollView(.vertical)"))
        XCTAssertFalse(rollout.contains("ScrollView([.vertical, .horizontal])"))
    }

    @MainActor
    private func sessionRow(pod: PodSummary) -> some View {
        TerminalSessionControlRow(
            title: "Session",
            systemImage: "terminal",
            pods: [pod],
            terminalSessions: [],
            primaryActionTitle: "Connect",
            primaryActionSystemImage: "bolt.horizontal",
            isPrimaryActionDisabled: false,
            isClearDisabled: false,
            isFavoritePod: { _ in false },
            onToggleFavoritePod: { _ in },
            onPrimaryAction: {},
            onClear: {},
            selection: .constant(pod.id)
        )
    }

    @MainActor
    private func terminalSearchBar(index: TerminalTranscriptSearchIndex) -> some View {
        TerminalTranscriptSearchBar(
            searchIndex: index,
            resolvedSearchMatchIndex: 0,
            query: .constant("alpha"),
            matchCase: .constant(false),
            onPrevious: {},
            onNext: {},
            onClose: {}
        )
    }

    @MainActor
    private func logSearchBar(query: String, result: ResourceLogSearchResult?) -> some View {
        ResourceLogsSearchBar(
            query: .constant(query),
            matchCase: .constant(false),
            selectedMatchIndex: .constant(0),
            focusRequestID: 0,
            searchSummary: result,
            placeholder: "Search logs",
            findHelp: "Find in logs",
            matchCaseHelp: "Match case"
        )
    }

    @MainActor
    private func logExplorePanel(
        query: String,
        result: ResourceLogSearchResult?,
        summary: ResourceStructuredLogSummary
    ) -> some View {
        ResourceLogsExplorePanel(
            searchQuery: .constant(query),
            searchMatchCase: .constant(false),
            selectedSearchMatchIndex: .constant(0),
            searchFocusRequestID: 0,
            searchResult: result,
            structuredSummary: summary,
            showsInsights: true,
            presentationStyle: .regular,
            placeholder: "Search logs",
            findHelp: "Find in logs",
            matchCaseHelp: "Match case",
            onSearchFieldSample: { _, _ in },
            onSearchDuplicate: { _ in }
        )
    }

    @MainActor
    private func fittingSize<Content: View>(_ content: Content, width: CGFloat) -> CGSize {
        let host = NSHostingView(rootView: content.frame(width: width))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    private func source(_ relativePath: String) throws -> String {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return try String(contentsOf: root.appendingPathComponent(relativePath), encoding: .utf8)
    }
}
