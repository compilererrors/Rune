import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class TerminalStandaloneLayoutTests: XCTestCase {
    @MainActor
    func testMultilinePasteNoticeReservesHeightOnlyWhileActive() {
        let inactiveSize = fittingSize(
            of: TerminalMultilinePasteNotice(isActive: false),
            width: 320
        )
        let activeSize = fittingSize(
            of: TerminalMultilinePasteNotice(isActive: true),
            width: 320
        )

        XCTAssertLessThanOrEqual(inactiveSize.height, 0.5)
        XCTAssertEqual(activeSize.height, 18, accuracy: 1)
    }

    @MainActor
    func testCommonCommandRowFillsWidthAndDispatchesOneInsertAndOneCopyAction() {
        var insertedCommands: [String] = []
        var copyCount = 0
        let row = ResourceTerminalCommonCommandRow(
            command: "printf synthetic-output",
            copyHelp: "Copy shell command",
            onInsert: { insertedCommands.append($0) },
            onCopy: { copyCount += 1 }
        )

        let size = fittingSize(of: row, width: 320)
        row.insertCommand()
        row.copyCommand()

        XCTAssertEqual(size.width, 320, accuracy: 1)
        XCTAssertGreaterThanOrEqual(size.height, ResourceTerminalCommonCommandRow.minimumHeight)
        XCTAssertLessThanOrEqual(size.height, 40)
        XCTAssertEqual(insertedCommands, ["printf synthetic-output"])
        XCTAssertEqual(copyCount, 1)
    }

    func testCompactLogLayoutUsesReducedOutputMinimumAndCollapsedSummary() {
        XCTAssertEqual(ResourceLogsLayoutMetrics.regularOutputMinimumHeight, 280)
        XCTAssertEqual(ResourceLogsLayoutMetrics.terminalCompactOutputMinimumHeight, 210)
        XCTAssertEqual(
            ResourceLogsLayoutMetrics.outputMinimumHeight(for: .regular),
            ResourceLogsLayoutMetrics.regularOutputMinimumHeight
        )
        XCTAssertEqual(
            ResourceLogsLayoutMetrics.outputMinimumHeight(for: .terminalCompact),
            ResourceLogsLayoutMetrics.terminalCompactOutputMinimumHeight
        )
        XCTAssertFalse(ResourceLogsLayoutMetrics.startsWithStructuredSummaryCollapsed(for: .regular))
        XCTAssertTrue(ResourceLogsLayoutMetrics.startsWithStructuredSummaryCollapsed(for: .terminalCompact))
    }

    @MainActor
    func testCompactLogOutputRendersAtTheReducedMinimumHeight() {
        let compactSize = fittingSize(
            of: makeLogOutput(presentationStyle: .terminalCompact),
            width: 420
        )
        let regularSize = fittingSize(
            of: makeLogOutput(presentationStyle: .regular),
            width: 420
        )

        XCTAssertGreaterThanOrEqual(
            compactSize.height,
            ResourceLogsLayoutMetrics.terminalCompactOutputMinimumHeight
        )
        XCTAssertLessThanOrEqual(compactSize.height, 220)
        XCTAssertGreaterThanOrEqual(
            regularSize.height,
            ResourceLogsLayoutMetrics.regularOutputMinimumHeight
        )
        XCTAssertGreaterThan(regularSize.height, compactSize.height)
    }

    @MainActor
    func testRegularStructuredSummaryUsesOneDenseRowWhileTerminalSummaryStartsCollapsed() {
        let summary = ResourceStructuredLogSummary(
            totalLineCount: 8,
            jsonLineCount: 8,
            isStructured: true,
            fields: [
                ResourceStructuredLogField(
                    key: "level",
                    title: "Level",
                    nonEmptyCount: 8,
                    sampleValues: ["info", "warning"],
                    sampleSearchQueries: [
                        ResourceStructuredLogFieldSampleSearch(
                            value: "info",
                            query: "\"level\":\"info\""
                        )
                    ]
                )
            ],
            duplicateLines: [
                ResourceDuplicateLogLine(
                    fingerprint: "synthetic repeated line",
                    count: 3,
                    sampleLine: "synthetic repeated line"
                )
            ]
        )
        let regularSize = fittingSize(
            of: ResourceStructuredLogSummaryPanel(
                summary: summary,
                onSearchFieldSample: { _, _ in },
                onSearchDuplicate: { _ in },
                presentationStyle: .regular
            ),
            width: 420
        )
        let compactSize = fittingSize(
            of: ResourceStructuredLogSummaryPanel(
                summary: summary,
                onSearchFieldSample: { _, _ in },
                onSearchDuplicate: { _ in },
                presentationStyle: .terminalCompact
            ),
            width: 420
        )

        XCTAssertEqual(
            regularSize.height,
            ResourceLogsLayoutMetrics.insightsRowHeight,
            accuracy: 1
        )
        XCTAssertLessThanOrEqual(regularSize.height, compactSize.height)
        XCTAssertLessThanOrEqual(compactSize.height, 34)
    }

    func testStandaloneTerminalSourceKeepsSingleInsertTargetAndSelfManagedLogOutput() throws {
        let terminalSource = try String(contentsOfFile: resourceTerminalInspectorPath, encoding: .utf8)
        let commandRow = try XCTUnwrap(terminalSource.slice(
            from: "struct ResourceTerminalCommonCommandRow: View",
            to: "struct ResourceTerminalDetailsView: View"
        ))
        let shellSource = try String(contentsOfFile: terminalShellPanelPath, encoding: .utf8)
        let pasteNotice = try XCTUnwrap(shellSource.slice(
            from: "struct TerminalMultilinePasteNotice: View",
            to: "struct TerminalPromptLayoutMetrics"
        ))
        let logsSource = try String(contentsOfFile: resourceLogsInspectorPath, encoding: .utf8)
        let outputSurface = try XCTUnwrap(logsSource.slice(
            from: "struct ResourceLogsOutputSurface: View",
            to: "enum ResourceLogsDeferredRenderingPolicy"
        ))

        XCTAssertEqual(commandRow.occurrences(of: "Button(action:"), 2)
        XCTAssertTrue(commandRow.contains("Button(action: insertCommand)"))
        XCTAssertTrue(commandRow.contains("Button(action: copyCommand)"))
        XCTAssertFalse(commandRow.contains("paperplane"))
        XCTAssertTrue(terminalSource.contains("ResourceTerminalCommonCommandRow("))

        XCTAssertTrue(pasteNotice.contains("if isActive"))
        XCTAssertFalse(pasteNotice.contains(".opacity("))
        XCTAssertFalse(pasteNotice.contains("accessibilityHidden"))

        XCTAssertTrue(outputSurface.contains("InspectorTextSurface("))
        XCTAssertTrue(outputSurface.contains("InspectorReadOnlyTextView("))
        XCTAssertTrue(outputSurface.contains("ResourceLogsLayoutMetrics.outputMinimumHeight"))
        XCTAssertFalse(outputSurface.contains("ScrollView("))
        XCTAssertTrue(logsSource.contains("RuneDisclosureSection("))
        XCTAssertTrue(logsSource.contains("isExpanded: $isCompactSummaryExpanded"))
    }

    @MainActor
    private func fittingSize<Content: View>(of content: Content, width: CGFloat) -> CGSize {
        let host = NSHostingView(rootView: content.frame(width: width))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    @MainActor
    private func makeLogOutput(
        presentationStyle: ResourceLogsPresentationStyle
    ) -> ResourceLogsOutputSurface {
        ResourceLogsOutputSurface(
            isLoadingLogs: false,
            isLoadingResources: false,
            errorMessage: nil,
            logText: "",
            renderSearchResult: ResourceLogSearchResult.make(text: "", query: ""),
            navigationSearchResult: nil,
            selectedSearchMatchIndex: 0,
            searchNavigationSequence: 0,
            emptyTitle: "No synthetic output",
            emptyMessage: "The synthetic source returned no lines.",
            noMatchesMessage: "No synthetic lines matched.",
            readOnlyResetID: "synthetic-terminal-log-output",
            onReload: {},
            presentationStyle: presentationStyle
        )
    }

    private var resourceTerminalInspectorPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceTerminalInspectorView.swift").path
    }

    private var terminalShellPanelPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalShellPanelView.swift").path
    }

    private var resourceLogsInspectorPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceLogsInspectorView.swift").path
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
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

    func occurrences(of value: String) -> Int {
        components(separatedBy: value).count - 1
    }
}
