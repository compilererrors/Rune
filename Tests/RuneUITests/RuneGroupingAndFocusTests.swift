import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

/// Grouping and focus (Design/README.md): one header block per inspector,
/// facts as one quiet grid, actions attached to the surface they act on, and
/// no card inside a card.
final class RuneGroupingAndFocusTests: XCTestCase {
    func testEveryResourceInspectorHeaderHasAKindSubtitle() throws {
        let source = try RuneRootViewSource.text()
        let blocks = source.components(separatedBy: "RuneInspectorScaffold(").dropFirst()
        XCTAssertFalse(blocks.isEmpty)
        for block in blocks {
            let header = String(block.prefix(400))
            XCTAssertTrue(header.contains("subtitle:"), "every inspector header names its kind and namespace: \(header.prefix(80))")
        }
    }

    func testStatusIsAPillNotAFactRow() throws {
        let source = try RuneRootViewSource.text()
        XCTAssertFalse(source.contains("inspectorInfoRow(\"Status\""), "status is the header pill")
        XCTAssertFalse(source.contains("inspectorInfoRow(\"Namespace\""), "the namespace is in the subtitle")
        XCTAssertTrue(source.contains("status: RuneInspectorStatus(pod.displayStatus)"))
        XCTAssertTrue(source.contains("status: deploymentInspectorStatus(deployment)"))
    }

    func testFactRowsCopyOnHoverAndFromTheContextMenu() throws {
        let design = try sourceText("Sources/RuneUI/Design/Components/RuneDesignComponents.swift")
        let row = try XCTUnwrap(design.slice(from: "struct RuneInspectorInfoRow<Value: View>: View {", to: "struct RuneSectionLabel"))
        XCTAssertTrue(row.contains(".opacity(isHovered ? 1 : 0)"), "the copy button shows on hover only")
        XCTAssertTrue(row.contains(".allowsHitTesting(isHovered)"))
        XCTAssertTrue(row.contains(".contextMenu {"), "and copy is always in the context menu")
        XCTAssertTrue(row.contains("RuneHairline()"), "rows are divided by hairlines")
        XCTAssertFalse(row.contains("Image(systemName: systemImage)"), "no icon per label")
        XCTAssertFalse(row.contains("RuneCardSurface"), "no card per row")
    }

    func testEventsInspectorFocusesOnTheMessageAndLinksTheObject() throws {
        let source = try RuneRootViewSource.text()
        let events = try XCTUnwrap(source.slice(from: "var eventDetails: some View {", to: "func eventGoToResourceButtonTitle"))
        XCTAssertTrue(events.contains("RuneSectionLabel(\"Message\")"))
        XCTAssertTrue(events.contains(".runeText(.body)"), "the message is readable body text, not a code strip")
        XCTAssertTrue(events.contains("RuneFactLink(object"), "the object is a link that opens it")
        XCTAssertTrue(events.contains("showsActions: false"), "no separate Go to button")
        let message = try XCTUnwrap(events.range(of: "RuneSectionLabel(\"Message\")")).lowerBound
        let facts = try XCTUnwrap(events.range(of: "eventInspectorCoreInfo(event)")).lowerBound
        XCTAssertLessThan(message, facts, "the message comes first")
        XCTAssertFalse(source.contains("Label(\"Event stream\""), "no label standing in for a kind picker")
    }

    func testScopeShowsInOnePlace() throws {
        let source = try RuneRootViewSource.text()
        XCTAssertFalse(source.contains("RuneHeaderCapsule(\n                        currentResourceListScopeLabel"))
        XCTAssertTrue(source.contains("if let note = currentResourceListScopeNote"))
        XCTAssertTrue(source.contains("case .events, .helm, .overview, .terminal:\n            return nil"))
    }

    func testDocumentsHaveTheirActionsAttachedAndNoCardsInside() throws {
        let manifest = try sourceText("Sources/RuneUI/Views/ResourceManifestInspectorLayout.swift")
        XCTAssertTrue(manifest.contains("RuneDocumentFrame(showsStatus: showsStatus)"))
        let logs = try sourceText("Sources/RuneUI/Views/ResourceLogsInspectorView.swift")
        XCTAssertTrue(logs.contains("RuneDocumentFrame(\n            showsTabs: surfaceTabs != nil,\n            showsStatus: showsInsightsLine\n        ) {"))
        let shell = try sourceText("Sources/RuneUI/Views/TerminalShellPanelView.swift")
        XCTAssertTrue(shell.contains("RuneDocumentFrame(role: .terminal)"))
        XCTAssertFalse(shell.contains(".runePanelCard("), "the Pod Shell is not a card around a framed console")

        for path in [
            "Sources/RuneUI/Views/ResourceManifestPresentation.swift",
            "Sources/RuneUI/Views/ResourceLogsInspectorView.swift",
            "Sources/RuneUI/Views/ResourceYAMLInspectorView.swift",
            "Sources/RuneUI/Views/InspectorTextViews.swift",
            "Sources/RuneUI/Views/TerminalTabStrip.swift",
            "Sources/RuneUI/Views/TerminalPodSelectorRow.swift"
        ] {
            XCTAssertTrue(try sourceText(path).contains("runeDocumentFrameEmbedded"), "\(path) drops its own chrome inside a document frame")
        }
    }

    func testInspectorActionRowIsNotABoxedCard() throws {
        let design = try sourceText("Sources/RuneUI/Design/Components/RuneDesignComponents.swift")
        let row = try XCTUnwrap(design.slice(from: "struct RuneInspectorActionRow<Content: View>: View {", to: "/// Short forms share one wrapping rule"))
        XCTAssertFalse(row.contains("RuneControlCard"))
        XCTAssertTrue(row.contains("ScrollView(.horizontal, showsIndicators: false)"), "it still never wraps, so resizing never jumps")
    }

    func testTabsThatSwitchASurfaceSitOnItsTopEdge() throws {
        let shell = try sourceText("Sources/RuneUI/Views/TerminalShellPanelView.swift")
        let shellTabs = try XCTUnwrap(shell.slice(from: "} tabs: {", to: "} content: {"))
        XCTAssertTrue(shellTabs.contains("TerminalSessionTabBar("), "shell tabs sit on the transcript's top edge")
        let root = try RuneRootViewSource.text()
        XCTAssertTrue(root.contains(".environment(\\.runeSurfaceTabs, AnyView(terminalLogTabBar))"), "pod log tabs sit on the log surface")
        let frame = try sourceText("Sources/RuneUI/Design/Components/RuneDocumentFrame.swift")
        XCTAssertTrue(frame.contains(".background(role.tabStripFill(palette: palette, colorScheme: colorScheme))"))
        XCTAssertTrue(frame.contains(".environment(\\.runeSurfaceTabRole, role)"))
        let tabBar = try sourceText("Sources/RuneUI/Views/TerminalSessionTabBar.swift")
        XCTAssertTrue(tabBar.contains("KeyboardShortcut(\"t\", modifiers: [.command])"))
        XCTAssertTrue(tabBar.contains(".keyboardShortcut(\"[\", modifiers: [.command, .shift])"))
        XCTAssertTrue(tabBar.contains(".keyboardShortcut(\"]\", modifiers: [.command, .shift])"))
        XCTAssertFalse(tabBar.contains("keyboardShortcut(\"w\""), "⌘W stays with the window")
    }

    func testDenseViewsFoldRareActionsIntoOneOverflowMenu() throws {
        let yaml = try sourceText("Sources/RuneUI/Views/ResourceYAMLInspectorView.swift")
        let pane = try XCTUnwrap(yaml.slice(from: "struct ResourceYAMLInspectorPane", to: "struct ManifestUnsavedEditsChip"))
        XCTAssertTrue(pane.contains("ManifestYAMLMoreMenu("))
        XCTAssertFalse(pane.contains("ManifestYAMLActionMenus("))
        XCTAssertFalse(pane.contains("ManifestManagedFieldsToggle("))
        let describe = try sourceText("Sources/RuneUI/Views/ResourceDescribeInspectorView.swift")
        XCTAssertFalse(describe.contains("ManifestManagedFieldsToggle("))
        XCTAssertTrue(describe.contains("Label(\"More\", systemImage: \"ellipsis.circle\")"))
        let shell = try sourceText("Sources/RuneUI/Views/TerminalShellPanelView.swift")
        XCTAssertFalse(shell.contains("RuneBorderedIconButton("), "the shell header has one overflow menu, not a row of icons")
        let portForward = try sourceText("Sources/RuneUI/Views/TerminalPortForwardPanelView.swift")
        XCTAssertFalse(portForward.contains("TerminalActionButtonLabel(\n                \"Copy kubectl\""))
    }

    func testLogViewerHeaderIsTwoLabelledRowsWithNothingAboveIt() throws {
        let logs = try sourceText("Sources/RuneUI/Views/ResourceLogsInspectorView.swift")
        let header = try XCTUnwrap(logs.slice(from: "private var logViewerHeader: some View {", to: "private var hasSourcePickers: Bool {"))
        XCTAssertTrue(header.contains("sourceHeaderRow(showsCaptions: true)"))
        XCTAssertTrue(header.contains("actionHeaderRow(showsLabels: true)"))
        XCTAssertTrue(header.contains("actionHeaderRow(showsLabels: false)"))
        XCTAssertTrue(header.contains("LogHeaderLabeledField(\"Range\""))
        XCTAssertTrue(header.contains("showsFavoriteButton: false"), "pinning lives in the picker list")
        XCTAssertTrue(logs.contains("RuneStableLabel(followTitle, reserving: [\"Follow\", \"Following\", \"Paused\"])"))
        XCTAssertTrue(logs.contains("Label(\"Save\", systemImage: \"square.and.arrow.down\")"))
        XCTAssertTrue(logs.contains("Label(\"Include Previous Container\", systemImage: \"clock.arrow.circlepath\")"))
        let root = try RuneRootViewSource.text()
        let terminal = try XCTUnwrap(root.slice(from: "var terminalDetails: some View {", to: "var terminalInspectorPod: PodSummary? {"))
        XCTAssertTrue(terminal.contains("showsActions: false"), "no floating Use in Pod Shell button")
    }

    func testDocumentFrameCornersAreConcentricWithTheBarControls() {
        XCTAssertEqual(
            RuneUILayoutMetrics.documentFrameCornerRadius,
            RuneRadius.control + RuneUILayoutMetrics.documentFrameBarInset
        )
    }

    @MainActor
    func testFactGridAlignsValuesOnOneColumn() throws {
        var frames: [CGRect] = []
        let grid = RuneFactGrid("Details") {
            RuneInspectorInfoRow("Chart") { Text("sample-chart-1.0.0").background(FrameProbe { frames.append($0) }) }
            RuneInspectorInfoRow("App version") { Text("1.0.0").background(FrameProbe { frames.append($0) }) }
            RuneInspectorInfoRow("Updated") { Text("2026-01-01").background(FrameProbe { frames.append($0) }) }
        }
        .frame(width: 360)
        let host = NSHostingView(rootView: grid)
        host.frame = NSRect(x: 0, y: 0, width: 360, height: 200)
        host.layoutSubtreeIfNeeded()
        RunLoop.main.run(until: Date().addingTimeInterval(0.2))
        host.layoutSubtreeIfNeeded()

        let latest = Array(frames.suffix(3))
        XCTAssertEqual(latest.count, 3)
        for frame in latest {
            XCTAssertEqual(frame.minX, latest[0].minX, accuracy: 0.5, "values share one column")
        }
    }

    private func sourceText(_ relativePath: String) throws -> String {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return try String(contentsOf: root.appendingPathComponent(relativePath), encoding: .utf8)
    }
}

private struct FrameProbe: View {
    let onFrame: (CGRect) -> Void

    var body: some View {
        GeometryReader { proxy in
            Color.clear
                .onAppear { onFrame(proxy.frame(in: .global)) }
        }
    }
}

private extension String {
    func slice(from start: String, to end: String) -> String? {
        guard let lower = range(of: start)?.upperBound,
              let upper = range(of: end, range: lower..<endIndex)?.lowerBound else { return nil }
        return String(self[lower..<upper])
    }
}
