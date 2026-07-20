import AppKit
import RuneCore
import SwiftUI
@testable import RuneUI
import XCTest

final class AppKitResourceTableInfrastructureTests: XCTestCase {
    @MainActor
    func testSharedSelectionPlumbingSelectsAndClearsRows() {
        let rows = ["resource-alpha", "resource-beta", "resource-gamma"]
        let dataSource = ResourceTableTestDataSource(rowCount: rows.count)
        let tableView = makeTableView(dataSource: dataSource)

        applyResourceTableSelection(
            selectedID: "resource-beta",
            rows: rows,
            rowID: { $0 },
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 1)

        applyResourceTableSelection(
            selectedID: "missing-resource",
            rows: rows,
            rowID: { $0 },
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, -1)

        applyResourceTableSelection(
            selectedID: nil,
            rows: rows,
            rowID: { $0 },
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, -1)
    }

    @MainActor
    func testCustomThemeResolvesThroughTableHeaderHierarchy() {
        let tableView = RuneAppKitResourceTableView()
        let customTheme = RuneAppearanceTheme.fjord.resolvedTheme
        tableView.resolvedTheme = customTheme
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))

        let headerView = NSTableHeaderView()
        tableView.headerView = headerView
        let nestedHeaderContent = NSView()
        headerView.addSubview(nestedHeaderContent)

        XCTAssertTrue(headerView.tableView === tableView)
        XCTAssertEqual(resolvedRuneResourceTableTheme(for: headerView).id, customTheme.id)
        XCTAssertEqual(resolvedRuneResourceTableTheme(for: nestedHeaderContent).id, customTheme.id)
        XCTAssertEqual(
            resolvedRuneResourceTableTheme(for: NSView()).id,
            RuneAppearanceTheme.native.resolvedTheme.id
        )
    }

    func testResourceTablesUseOneSharedHostAndInteractionSubclass() throws {
        let source = try String(contentsOfFile: appKitResourceTablePath, encoding: .utf8)

        XCTAssertTrue(source.contains("private enum RuneAppKitResourceTableHost"))
        XCTAssertEqual(source.components(separatedBy: "RuneAppKitResourceTableHost.make(").count - 1, 7)
        XCTAssertEqual(source.components(separatedBy: "let scrollView = RuneAppKitResourceListScrollView()").count - 1, 1)
        XCTAssertEqual(source.components(separatedBy: "RuneAppKitResourceTableStyle.apply(to: tableView").count - 1, 1)
        XCTAssertEqual(source.components(separatedBy: "final class RuneAppKitResourceTableView: NSTableView").count - 1, 1)
        XCTAssertEqual(source.components(separatedBy: "override func menu(for event: NSEvent)").count - 1, 1)
        XCTAssertEqual(source.components(separatedBy: "override func keyDown(with event: NSEvent)").count - 1, 1)
        XCTAssertTrue(source.contains("headerView.tableView as? RuneAppKitResourceTableView"))
        XCTAssertFalse(source.contains("private final class PodNSTableView"))
        XCTAssertFalse(source.contains("private final class DeploymentNSTableView"))
        XCTAssertFalse(source.contains("private final class ServiceNSTableView"))
        XCTAssertTrue(source.contains("static let headerHeight: CGFloat = 24"))
        XCTAssertTrue(source.contains("static let rowHeight: CGFloat = 34"))
        XCTAssertTrue(source.contains("button.widthAnchor.constraint(equalToConstant: RuneUILayoutMetrics.iconButtonSize)"))
        XCTAssertTrue(source.contains("button.heightAnchor.constraint(equalToConstant: RuneUILayoutMetrics.iconButtonSize)"))
    }

    func testHorizontalOverflowIndicatorsFollowViewportPosition() {
        let fitting = RuneHorizontalTableOverflowState.resolve(
            visibleOriginX: 0,
            visibleWidth: 520,
            documentWidth: 520
        )
        XCTAssertFalse(fitting.hasOverflow)
        XCTAssertFalse(fitting.showsLeadingIndicator)
        XCTAssertFalse(fitting.showsTrailingIndicator)

        let leadingEdge = RuneHorizontalTableOverflowState.resolve(
            visibleOriginX: 0,
            visibleWidth: 320,
            documentWidth: 800
        )
        XCTAssertTrue(leadingEdge.hasOverflow)
        XCTAssertFalse(leadingEdge.showsLeadingIndicator)
        XCTAssertTrue(leadingEdge.showsTrailingIndicator)

        let middle = RuneHorizontalTableOverflowState.resolve(
            visibleOriginX: 240,
            visibleWidth: 320,
            documentWidth: 800
        )
        XCTAssertTrue(middle.hasOverflow)
        XCTAssertTrue(middle.showsLeadingIndicator)
        XCTAssertTrue(middle.showsTrailingIndicator)

        let trailingEdge = RuneHorizontalTableOverflowState.resolve(
            visibleOriginX: 480,
            visibleWidth: 320,
            documentWidth: 800
        )
        XCTAssertTrue(trailingEdge.hasOverflow)
        XCTAssertTrue(trailingEdge.showsLeadingIndicator)
        XCTAssertFalse(trailingEdge.showsTrailingIndicator)
    }

    func testAllSevenResourceTablesShareHorizontalScrollerPolicyKPI() throws {
        let source = try String(contentsOfFile: appKitResourceTablePath, encoding: .utf8)

        XCTAssertEqual(
            source.components(separatedBy: "RuneAppKitResourceTableHost.make(").count - 1,
            7,
            "KPI: all seven resource-table families must use the shared AppKit host."
        )
        XCTAssertEqual(
            source.components(separatedBy: "scrollView.hasHorizontalScroller = true").count - 1,
            1,
            "KPI: horizontal scrolling must be configured once in the shared host."
        )
        XCTAssertFalse(
            source.contains("hasHorizontalScroller:"),
            "Individual table families must not opt out of the shared overflow policy."
        )
        XCTAssertTrue(source.contains("scrollView.autohidesScrollers = true"))
        XCTAssertTrue(source.contains("scrollView.horizontalScrollElasticity = .automatic"))
        XCTAssertTrue(source.contains("RuneHorizontalTableOverflowState.resolve("))
        XCTAssertTrue(source.contains("More columns are available. Scroll horizontally to reveal them."))
    }

    @MainActor
    func testNarrowGenericResourceTableCanScrollToHiddenColumns() throws {
        let resource = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-config",
            namespace: "synthetic-namespace",
            primaryText: "3 keys",
            secondaryText: "1 KiB"
        )
        let host = NSHostingView(rootView: AppKitGenericResourceListView(
            resources: [resource],
            selectedResourceID: nil,
            selectedResourceIDs: [],
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: false,
            isFavorite: { _ in false },
            onSelectResource: { _ in },
            onToggleBulkSelection: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        ).frame(width: 360, height: 220))
        host.frame = NSRect(x: 0, y: 0, width: 360, height: 220)
        settle(host)

        let scrollView = try XCTUnwrap(findResourceTableScrollView(in: host))
        scrollView.layoutSubtreeIfNeeded()
        let tableView = try XCTUnwrap(scrollView.documentView as? RuneAppKitResourceTableView)
        let viewportWidth = scrollView.contentView.documentVisibleRect.width

        XCTAssertTrue(scrollView.hasHorizontalScroller)
        XCTAssertTrue(scrollView.autohidesScrollers)
        XCTAssertGreaterThan(tableView.frame.width, viewportWidth)

        let maximumOffset = tableView.frame.width - viewportWidth
        scrollView.contentView.scroll(to: NSPoint(x: maximumOffset, y: 0))
        scrollView.reflectScrolledClipView(scrollView.contentView)

        XCTAssertGreaterThan(
            scrollView.contentView.documentVisibleRect.minX,
            1,
            "A narrow table must allow navigation to columns outside the initial viewport."
        )
    }

    func testAppKitTextSurfacesOwnTheirScrolling() throws {
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let start = try XCTUnwrap(rootSource.range(of: "private func exportableTextPane"))
        let end = try XCTUnwrap(rootSource.range(of: "private func execPane", range: start.upperBound..<rootSource.endIndex))
        let exportPane = String(rootSource[start.lowerBound..<end.lowerBound])

        XCTAssertTrue(exportPane.contains("InspectorTextSurface(minHeight: 220)"))
        XCTAssertTrue(exportPane.contains("InspectorReadOnlyTextView("))
        XCTAssertFalse(exportPane.contains("ScrollView"))
    }

    @MainActor
    func testResourceTableSelectionProjectionBenchmarkKPI() {
        let rows = (0..<2_500).map { "resource-\($0)" }
        let dataSource = ResourceTableTestDataSource(rowCount: rows.count)
        let tableView = makeTableView(dataSource: dataSource)

        let start = ProcessInfo.processInfo.systemUptime
        for pass in 0..<50 {
            let selectedID = pass.isMultiple(of: 2) ? rows[0] : rows[rows.count - 1]
            applyResourceTableSelection(
                selectedID: selectedID,
                rows: rows,
                rowID: { $0 },
                in: tableView
            )
        }
        let elapsed = ProcessInfo.processInfo.systemUptime - start

        XCTAssertEqual(tableView.selectedRow, rows.count - 1)
        XCTAssertLessThan(
            elapsed,
            0.10,
            "KPI: 50 shared AppKit selection projections across 2,500 rows must stay below 100ms in debug."
        )
    }

    @MainActor
    private func makeTableView(dataSource: ResourceTableTestDataSource) -> NSTableView {
        let tableView = NSTableView(frame: NSRect(x: 0, y: 0, width: 480, height: 320))
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))
        tableView.dataSource = dataSource
        tableView.noteNumberOfRowsChanged()
        return tableView
    }

    @MainActor
    private func settle(_ view: NSView) {
        for _ in 0..<3 {
            view.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.02))
        }
    }

    @MainActor
    private func findResourceTableScrollView(in view: NSView) -> NSScrollView? {
        if let scrollView = view as? NSScrollView,
           scrollView.documentView is RuneAppKitResourceTableView {
            return scrollView
        }
        for subview in view.subviews {
            if let match = findResourceTableScrollView(in: subview) {
                return match
            }
        }
        return nil
    }

    private var appKitResourceTablePath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/AppKitPodTableView.swift").path
    }

    private var runeRootViewPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}

private final class ResourceTableTestDataSource: NSObject, NSTableViewDataSource {
    let rowCount: Int

    init(rowCount: Int) {
        self.rowCount = rowCount
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        rowCount
    }
}
