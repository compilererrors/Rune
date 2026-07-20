import XCTest
import struct RuneSharedCore.RuneLargeTextIndex
import struct RuneSharedCore.RuneLargeTextViewportLayout
@testable import RuneUI

final class RuneLargeTextIndexTests: XCTestCase {
    func testLineIndexCountsMixedLineEndingsAndTrailingBlankLine() {
        let index = RuneLargeTextIndex(text: "alpha\r\nbeta\rgamma\n")

        XCTAssertEqual(index.lineCount, 4)
        XCTAssertEqual(index.line(number: 1)?.text, "alpha")
        XCTAssertEqual(index.line(number: 2)?.text, "beta")
        XCTAssertEqual(index.line(number: 3)?.text, "gamma")
        XCTAssertEqual(index.line(number: 4)?.text, "")
    }

    func testMockKeyValueConfigDumpIndexesEveryLogicalLine() {
        let lineCount = 500
        let text = (0..<lineCount)
            .map { "mock.framework.mockSetting.\($0) = mockValue" }
            .joined(separator: "\n")
        let index = RuneLargeTextIndex(text: text)

        XCTAssertEqual(index.lineCount, lineCount)
        XCTAssertEqual(index.line(number: 1)?.text, "mock.framework.mockSetting.0 = mockValue")
        XCTAssertEqual(index.line(number: lineCount)?.text, "mock.framework.mockSetting.\(lineCount - 1) = mockValue")
    }

    func testViewportReadsOnlyRequestedWindowDeepInLargeText() {
        let text = (0..<80_000)
            .map { "line-\(String(format: "%05d", $0)) value=synthetic" }
            .joined(separator: "\n")
        let index = RuneLargeTextIndex(text: text)
        let viewport = index.viewport(startLine: 70_001, lineLimit: 5)

        XCTAssertEqual(index.lineCount, 80_000)
        XCTAssertEqual(viewport.startLine, 70_001)
        XCTAssertEqual(viewport.lines.map(\.number), [70_001, 70_002, 70_003, 70_004, 70_005])
        XCTAssertEqual(viewport.lines.first?.text, "line-70000 value=synthetic")
        XCTAssertEqual(viewport.lines.last?.text, "line-70004 value=synthetic")
    }

    func testLargeTextViewportClampsStaleOffsetSoLinesRemainRenderable() {
        let layout = RuneLargeTextViewportLayout(
            lineCount: 4_356,
            rowHeight: 18,
            verticalPadding: 8,
            viewportHeight: 620
        )
        let staleOffsetPastContent = layout.contentHeight * 3

        XCTAssertGreaterThan(staleOffsetPastContent, layout.maxVerticalOffset)
        XCTAssertEqual(layout.clampedVerticalOffset(staleOffsetPastContent), layout.maxVerticalOffset)

        let startLine = layout.viewportStartLine(
            verticalOffset: staleOffsetPastContent,
            overscan: 24
        )
        let viewport = RuneLargeTextIndex(text: (0..<4_356).map { "line-\($0)" }.joined(separator: "\n"))
            .viewport(startLine: startLine, lineLimit: 60)

        XCTAssertFalse(viewport.lines.isEmpty, "Existing log lines must still render when a reused large text surface has a stale scroll offset.")
        XCTAssertGreaterThanOrEqual(viewport.lines.count, 30)
        XCTAssertEqual(layout.scrollTargetLineForClampedOffset(staleOffsetPastContent), 4_356)
    }

    func testSearchFindsEveryMatchWithoutFilteringOrCapping() {
        let text = (0..<60_000)
            .map { index in
                index.isMultiple(of: 5)
                    ? "line-\(index) status=error"
                    : "line-\(index) status=ok"
            }
            .joined(separator: "\n")
        let index = RuneLargeTextIndex(text: text)
        let result = index.search(query: "status=error")

        XCTAssertEqual(result.matches.count, 12_000)
        XCTAssertEqual(result.matches.first?.lineNumber, 1)
        XCTAssertEqual(result.matches.last?.lineNumber, 59_996)
    }

    func testLargeTextIndexBuildCancelsCooperatively() {
        enum SyntheticCancellation: Error {
            case requested
        }

        let text = (0..<20_000)
            .map { "line-\($0) value=synthetic" }
            .joined(separator: "\n")
        var cancellationChecks = 0

        XCTAssertThrowsError(
            try RuneLargeTextIndex(
                text: text,
                cancellationCheck: {
                    cancellationChecks += 1
                    if cancellationChecks == 4 {
                        throw SyntheticCancellation.requested
                    }
                }
            )
        ) { error in
            XCTAssertTrue(error is SyntheticCancellation)
        }
        XCTAssertEqual(cancellationChecks, 4)
    }

    func testLargeTextSearchCancelsCooperativelyBeforePublishingPartialMatches() {
        enum SyntheticCancellation: Error {
            case requested
        }

        let text = (0..<40_000)
            .map { "line-\($0) status=error" }
            .joined(separator: "\n")
        let index = RuneLargeTextIndex(text: text)
        var cancellationChecks = 0

        XCTAssertThrowsError(
            try index.search(
                query: "status=error",
                cancellationCheck: {
                    cancellationChecks += 1
                    if cancellationChecks == 12 {
                        throw SyntheticCancellation.requested
                    }
                }
            )
        ) { error in
            XCTAssertTrue(error is SyntheticCancellation)
        }
        XCTAssertEqual(cancellationChecks, 12)
    }

    func testTerminalTranscriptSearchFindsAllMatchesPastOldHighlightLimit() {
        let transcript = (0..<60_000)
            .map { index in
                index.isMultiple(of: 5)
                    ? "line=\(index) status=error"
                    : "line=\(index) status=ok"
            }
            .joined(separator: "\n")
        let index = TerminalTranscriptSearchIndex(
            text: transcript,
            query: "status=error",
            matchCase: false
        )

        XCTAssertEqual(index.ranges.count, 12_000)
        XCTAssertEqual(index.statusText(selectedIndex: 11_999), "12000 of 12000")
        XCTAssertEqual(index.matchLineNumber(selectedIndex: 11_999), 59_996)
    }

    func testTerminalTranscriptRenderModelTargetsBottomWhenSearchIsBlank() {
        let transcript = (0..<120)
            .map { "line=\($0) status=ok" }
            .joined(separator: "\n")
        let model = TerminalTranscriptRenderModel(
            text: transcript,
            query: "",
            matchCase: false,
            usesLargeTextSurface: true
        )

        XCTAssertEqual(model.textIndex?.lineCount, 120)
        XCTAssertEqual(model.scrollTargetLine(selectedIndex: 0), 120)
    }

    func testTerminalTranscriptRenderModelPreservesManualScrollWhenSearchIsBlank() {
        let transcript = (0..<120)
            .map { "line=\($0) status=ok" }
            .joined(separator: "\n")
        let model = TerminalTranscriptRenderModel(
            text: transcript,
            query: "",
            matchCase: false,
            usesLargeTextSurface: true
        )

        XCTAssertNil(model.scrollTargetLine(selectedIndex: 0, isPinnedToBottom: false))
    }

    func testTerminalTranscriptRenderModelTargetsSearchMatchWhenQueryIsPresent() {
        let transcript = """
        line=0 status=ok
        line=1 status=error
        line=2 status=ok
        line=3 status=error
        """
        let model = TerminalTranscriptRenderModel(
            text: transcript,
            query: "status=error",
            matchCase: false,
            usesLargeTextSurface: true
        )

        XCTAssertEqual(model.searchIndex.ranges.count, 2)
        XCTAssertEqual(model.scrollTargetLine(selectedIndex: 1, isPinnedToBottom: false), 4)
    }
}
