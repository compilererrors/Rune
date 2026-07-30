import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class InspectorControlGridTests: XCTestCase {
    func testControlRowsShareGridAtCompactAndWideInspectorWidths() throws {
        for width in [CGFloat(320), 760] {
            let recorder = InspectorControlGridFrameRecorder()
            let host = NSHostingView(
                rootView: InspectorControlGridHarness(
                    width: width,
                    recorder: recorder
                )
            )
            host.frame = NSRect(x: 0, y: 0, width: width, height: 100)

            settle(host, until: {
                recorder.frames.count == InspectorControlGridRegion.allCases.count
            })

            let firstRow = try frame(.firstRow, in: recorder, width: width)
            let secondRow = try frame(.secondRow, in: recorder, width: width)
            let firstContent = try frame(.firstContent, in: recorder, width: width)
            let secondContent = try frame(.secondContent, in: recorder, width: width)
            let expectedContentX = firstRow.minX
                + RuneUILayoutMetrics.inspectorControlLeadingAccessoryWidth
                + RuneUILayoutMetrics.inspectorControlColumnSpacing

            XCTAssertEqual(firstRow.minX, secondRow.minX, accuracy: 0.5)
            XCTAssertEqual(firstRow.maxX, secondRow.maxX, accuracy: 0.5)
            XCTAssertEqual(firstRow.width, width, accuracy: 0.5)
            XCTAssertEqual(secondRow.width, width, accuracy: 0.5)
            XCTAssertEqual(
                firstRow.height,
                RuneUILayoutMetrics.inspectorControlRowHeight,
                accuracy: 0.5
            )
            XCTAssertEqual(
                secondRow.height,
                RuneUILayoutMetrics.inspectorControlRowHeight,
                accuracy: 0.5
            )
            XCTAssertEqual(firstContent.minX, expectedContentX, accuracy: 0.5)
            XCTAssertEqual(secondContent.minX, expectedContentX, accuracy: 0.5)
        }
    }

    func testInspectorSurfacesAdoptSharedControlGridMetrics() throws {
        let logs = try source("Sources/RuneUI/Views/ResourceLogsInspectorView.swift")
        let find = try source("Sources/RuneUI/Views/InspectorFind.swift")
        let terminal = try source("Sources/RuneUI/Views/TerminalTranscriptSurface.swift")
        let manifest = try source("Sources/RuneUI/Views/ResourceManifestInspectorLayout.swift")

        let logSearch = try sourceSection(
            logs,
            from: "struct ResourceLogsSearchBar",
            to: "struct ResourceLogSearchResult"
        )
        let logSummary = try sourceSection(
            logs,
            from: "struct ResourceStructuredLogSummaryPanel",
            to: "struct ResourceLogsPaneActions"
        )
        let logExplore = try sourceSection(
            logs,
            from: "struct ResourceLogsExplorePanel",
            to: "actor ResourceLogsLatestWorkLane"
        )
        let inspectorSearch = try sourceSection(
            find,
            from: "private var searchField: some View",
            to: "private var navigationControls: some View"
        )
        let terminalSearch = try sourceSection(
            terminal,
            from: "private var searchField: some View",
            to: "private var navigationControls: some View"
        )

        XCTAssertTrue(logSearch.contains("RuneInspectorControlGridRow"))
        XCTAssertTrue(logSummary.contains("RuneInspectorControlGridRow"))
        XCTAssertTrue(logExplore.contains("RuneUILayoutMetrics.inspectorControlRowSpacing"))
        XCTAssertTrue(logExplore.contains("RuneUILayoutMetrics.inspectorControlContentInset"))
        XCTAssertTrue(logExplore.contains("RuneUILayoutMetrics.inspectorControlSurfaceVerticalPadding"))
        XCTAssertFalse(logExplore.contains("spacing: 5"))
        XCTAssertFalse(logExplore.contains(".padding(.horizontal, 8)"))
        XCTAssertFalse(logExplore.contains(".padding(.horizontal, 10)"))
        XCTAssertFalse(logExplore.contains(".padding(.vertical, 6)"))

        XCTAssertTrue(inspectorSearch.contains("RuneInspectorControlGridRow"))
        XCTAssertTrue(terminalSearch.contains("RuneInspectorControlGridRow"))
        XCTAssertFalse(inspectorSearch.contains(".padding(.horizontal"))
        XCTAssertFalse(terminalSearch.contains(".padding(.horizontal"))

        XCTAssertTrue(manifest.contains("RuneUILayoutMetrics.inspectorSectionSpacing"))
        XCTAssertEqual(
            manifest.occurrences(of: ".frame(maxWidth: .infinity, alignment: .leading)"),
            5
        )
        XCTAssertFalse(manifest.contains("spacing: 10"))
        XCTAssertFalse(manifest.contains(".padding(.horizontal, 10)"))
        XCTAssertFalse(manifest.contains(".padding(.vertical, 10)"))
    }

    private func frame(
        _ region: InspectorControlGridRegion,
        in recorder: InspectorControlGridFrameRecorder,
        width: CGFloat
    ) throws -> CGRect {
        try XCTUnwrap(
            recorder.frames[region],
            "Missing \(region) frame at \(width)pt."
        )
    }

    private func settle(
        _ host: NSView,
        until condition: () -> Bool
    ) {
        for _ in 0..<60 {
            host.layoutSubtreeIfNeeded()
            if condition() { return }
            RunLoop.main.run(until: Date().addingTimeInterval(0.005))
        }
    }

    private func source(_ relativePath: String) throws -> String {
        let repositoryRoot = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return try String(
            contentsOf: repositoryRoot.appendingPathComponent(relativePath),
            encoding: .utf8
        )
    }

    private func sourceSection(
        _ source: String,
        from startMarker: String,
        to endMarker: String
    ) throws -> String {
        let start = try XCTUnwrap(source.range(of: startMarker))
        let end = try XCTUnwrap(
            source.range(of: endMarker, range: start.upperBound..<source.endIndex)
        )
        return String(source[start.lowerBound..<end.lowerBound])
    }
}

@MainActor
private final class InspectorControlGridFrameRecorder: ObservableObject {
    @Published var frames: [InspectorControlGridRegion: CGRect] = [:]
}

private enum InspectorControlGridRegion: CaseIterable, Hashable {
    case firstRow
    case firstContent
    case secondRow
    case secondContent
}

private struct InspectorControlGridFramePreferenceKey: PreferenceKey {
    static let defaultValue: [InspectorControlGridRegion: CGRect] = [:]

    static func reduce(
        value: inout [InspectorControlGridRegion: CGRect],
        nextValue: () -> [InspectorControlGridRegion: CGRect]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private struct InspectorControlGridFrameProbe: View {
    let region: InspectorControlGridRegion

    var body: some View {
        GeometryReader { proxy in
            Color.clear.preference(
                key: InspectorControlGridFramePreferenceKey.self,
                value: [
                    region: proxy.frame(in: .named(InspectorControlGridHarness.coordinateSpaceName))
                ]
            )
        }
    }
}

private struct InspectorControlGridHarness: View {
    static let coordinateSpaceName = "InspectorControlGridTests"

    let width: CGFloat
    @ObservedObject var recorder: InspectorControlGridFrameRecorder

    var body: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorControlRowSpacing) {
            gridRow(
                accessory: Circle().frame(width: 6, height: 6),
                rowRegion: .firstRow,
                contentRegion: .firstContent
            )

            gridRow(
                accessory: Text("Aa").font(.system(size: 18, weight: .bold)),
                rowRegion: .secondRow,
                contentRegion: .secondContent
            )
        }
        .frame(width: width, height: 100, alignment: .topLeading)
        .coordinateSpace(name: Self.coordinateSpaceName)
        .onPreferenceChange(InspectorControlGridFramePreferenceKey.self) { frames in
            recorder.frames = frames
        }
    }

    private func gridRow<Accessory: View>(
        accessory: Accessory,
        rowRegion: InspectorControlGridRegion,
        contentRegion: InspectorControlGridRegion
    ) -> some View {
        RuneInspectorControlGridRow {
            accessory
        } content: {
            RoundedRectangle(cornerRadius: 2)
                .frame(width: 80, height: 12)
                .background(InspectorControlGridFrameProbe(region: contentRegion))
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(InspectorControlGridFrameProbe(region: rowRegion))
    }
}

private extension String {
    func occurrences(of substring: String) -> Int {
        components(separatedBy: substring).count - 1
    }
}
