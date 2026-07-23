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

    @MainActor
    func testColumnContentWidthIgnoresHiddenColumns() {
        let tableView = NSTableView()
        tableView.columnAutoresizingStyle = .noColumnAutoresizing
        let visibleColumn = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("visible"))
        let hiddenColumn = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("hidden"))
        tableView.addTableColumn(visibleColumn)
        tableView.addTableColumn(hiddenColumn)
        visibleColumn.width = 180
        hiddenColumn.minWidth = 120
        hiddenColumn.width = 0
        hiddenColumn.isHidden = true

        XCTAssertEqual(hiddenColumn.width, 120)
        XCTAssertEqual(RuneAppKitResourceTableStyle.columnContentWidth(in: tableView), 180)

        hiddenColumn.isHidden = false

        XCTAssertEqual(RuneAppKitResourceTableStyle.columnContentWidth(in: tableView), 300)
    }

    func testSortIndicatorGeometryIsVisibleAndClearOfResizeHandle() {
        let columnRect = NSRect(x: 24, y: 0, width: 180, height: 24)
        let indicatorRect = RuneAppKitResourceListLayout.sortIndicatorRect(in: columnRect)

        XCTAssertEqual(indicatorRect.size, RuneAppKitResourceListLayout.sortIndicatorSize)
        XCTAssertEqual(indicatorRect.midY, columnRect.midY, accuracy: 0.01)
        XCTAssertEqual(
            columnRect.maxX - indicatorRect.maxX,
            RuneAppKitResourceListLayout.sortIndicatorTrailingInset,
            accuracy: 0.01
        )
        let resizeHitSlop = RuneAppKitResourceListLayout.sortIndicatorTrailingInset
            - RuneAppKitResourceListLayout.sortIndicatorResizeSafetyGap
        XCTAssertGreaterThanOrEqual(
            indicatorRect.maxX,
            columnRect.maxX - resizeHitSlop - RuneAppKitResourceListLayout.sortIndicatorResizeSafetyGap
        )
        XCTAssertLessThanOrEqual(indicatorRect.maxX, columnRect.maxX - resizeHitSlop)
    }

    @MainActor
    func testHostedTableShowsSortStateOnTheActiveColumnAndPreviewsSortableHeaders() throws {
        let resource = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-config",
            namespace: "synthetic-namespace",
            primaryText: "2 keys",
            secondaryText: "2 text values · 0 binary values"
        )
        var toggledColumn: GenericResourceListSortColumn?
        let host = NSHostingView(rootView: genericResourceView(
            kind: .configMap,
            resource: resource,
            sortColumn: .secondary,
            sortAscending: false,
            onToggleSort: { toggledColumn = $0 }
        ).frame(width: 760, height: 220))
        host.frame = NSRect(x: 0, y: 0, width: 760, height: 220)
        settle(host)

        let scrollView = try XCTUnwrap(findResourceTableScrollView(in: host))
        let tableView = try XCTUnwrap(scrollView.documentView as? RuneAppKitResourceTableView)
        let headerView = try XCTUnwrap(tableView.headerView as? RuneAppKitResourceTableHeaderView)
        let nameColumn = try XCTUnwrap(tableView.tableColumns.first { $0.identifier.rawValue == "name" })
        let primaryColumn = try XCTUnwrap(tableView.tableColumns.first { $0.identifier.rawValue == "primary" })
        let secondaryColumn = try XCTUnwrap(tableView.tableColumns.first { $0.identifier.rawValue == "secondary" })
        let nameCell = try XCTUnwrap(nameColumn.headerCell as? RuneAppKitResourceTableHeaderCell)
        let secondaryCell = try XCTUnwrap(secondaryColumn.headerCell as? RuneAppKitResourceTableHeaderCell)

        XCTAssertEqual(tableView.sortDescriptors.first?.key, GenericResourceListSortColumn.secondary.rawValue)
        XCTAssertEqual(tableView.sortDescriptors.first?.ascending, false)
        XCTAssertFalse(nameCell.isSorted)
        XCTAssertTrue(secondaryCell.isSorted)
        XCTAssertFalse(secondaryCell.sortAscending)
        XCTAssertEqual(secondaryCell.accessibilityValue() as? String, "Sorted descending")
        XCTAssertEqual(secondaryCell.accessibilitySortDirection(), .descending)
        XCTAssertNil(tableView.indicatorImage(in: nameColumn))
        XCTAssertNotNil(tableView.indicatorImage(in: secondaryColumn))

        let secondaryIndex = try XCTUnwrap(tableView.tableColumns.firstIndex(of: secondaryColumn))
        let secondaryRect = headerView.headerRect(ofColumn: secondaryIndex)
        let indicatorRect = RuneAppKitResourceListLayout.sortIndicatorRect(in: secondaryRect)
        XCTAssertTrue(secondaryRect.contains(indicatorRect))
        XCTAssertLessThan(indicatorRect.maxX, secondaryRect.maxX)

        let primaryIndex = try XCTUnwrap(tableView.tableColumns.firstIndex(of: primaryColumn))
        let primaryRect = headerView.headerRect(ofColumn: primaryIndex)
        headerView.updateHoveredSortableColumnForTesting(
            at: NSPoint(x: primaryRect.midX, y: primaryRect.midY)
        )
        XCTAssertEqual(headerView.hoveredSortableColumnIdentifierForTesting, "primary")
        headerView.updateHoveredSortableColumnForTesting(
            at: NSPoint(x: primaryRect.maxX - 1, y: primaryRect.midY)
        )
        XCTAssertNil(
            headerView.hoveredSortableColumnIdentifierForTesting,
            "The hover affordance must stay clear of the resize target."
        )

        tableView.delegate?.tableView?(tableView, didClick: primaryColumn)
        XCTAssertEqual(toggledColumn, .primary)
    }

    func testAllResourceFamiliesFillTheUsableWideViewport() {
        let visibleWidth: CGFloat = 1_320
        let expected = RuneAppKitResourceListLayout.availableColumnWidth(visibleWidth: visibleWidth)
        let pod = RuneAppKitResourceListLayout.podColumnWidths(
            visibleWidth: visibleWidth,
            minimumNameWidth: PodTableLayout.nameColumnDefaultWidth
        )
        let deployment = RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: visibleWidth)
        let service = RuneAppKitResourceListLayout.serviceColumnWidths(visibleWidth: visibleWidth)
        let generic = RuneAppKitResourceListLayout.genericColumnWidths(visibleWidth: visibleWidth)
        let helm = RuneAppKitResourceListLayout.helmColumnWidths(visibleWidth: visibleWidth)
        let event = RuneAppKitResourceListLayout.eventColumnWidths(visibleWidth: visibleWidth)
        let operatorResource = RuneAppKitResourceListLayout.operatorColumnWidths(visibleWidth: visibleWidth)

        XCTAssertEqual(
            pod.selection + pod.name + pod.cpu + pod.memory + pod.restarts + pod.age + pod.status + pod.favorite,
            expected,
            accuracy: 0.5
        )
        XCTAssertEqual(deployment.name + deployment.replicas + deployment.favorite, expected, accuracy: 0.5)
        XCTAssertEqual(service.name + service.type + service.clusterIP + service.favorite, expected, accuracy: 0.5)
        XCTAssertEqual(
            generic.selection + generic.name + generic.primary + generic.secondary + generic.namespace + generic.favorite,
            expected,
            accuracy: 0.5
        )
        XCTAssertEqual(
            helm.name + helm.status + helm.namespace + helm.revision + helm.chart + helm.appVersion,
            expected,
            accuracy: 0.5
        )
        XCTAssertEqual(
            event.reason + event.type + event.object + event.namespace + event.lastSeen + event.message,
            expected,
            accuracy: 0.5
        )
        XCTAssertEqual(
            operatorResource.name + operatorResource.family + operatorResource.kind + operatorResource.namespace
                + operatorResource.status + operatorResource.printerColumns + operatorResource.apiPath + operatorResource.favorite,
            expected,
            accuracy: 0.5
        )
    }

    func testGenericColumnPresentationsAreSemanticAndStoredPerKind() {
        let configMap = RuneGenericResourceColumnPresentation.resolve(for: .configMap)
        XCTAssertEqual(configMap.primaryTitle, "Keys")
        XCTAssertEqual(configMap.secondaryTitle, "Data")

        let networkPolicy = RuneGenericResourceColumnPresentation.resolve(for: .networkPolicy)
        XCTAssertEqual(networkPolicy.primaryTitle, "Policy Types")
        XCTAssertEqual(networkPolicy.secondaryTitle, "Pod Selector")

        let job = RuneGenericResourceColumnPresentation.resolve(for: .job)
        XCTAssertEqual(job.primaryTitle, "Status")
        XCTAssertEqual(job.secondaryTitle, "Progress")

        XCTAssertNotEqual(
            RuneGenericResourceColumnPresentation.tableID(for: .configMap),
            RuneGenericResourceColumnPresentation.tableID(for: .secret)
        )
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
        XCTAssertFalse(scrollView.horizontalOverflowStateForTesting.showsLeadingIndicator)
        XCTAssertTrue(scrollView.horizontalOverflowStateForTesting.showsTrailingIndicator)

        let middleOffset = (tableView.frame.width - viewportWidth) / 2
        scrollView.contentView.scroll(to: NSPoint(x: middleOffset, y: 0))
        scrollView.reflectScrolledClipView(scrollView.contentView)
        XCTAssertTrue(scrollView.horizontalOverflowStateForTesting.showsLeadingIndicator)
        XCTAssertTrue(scrollView.horizontalOverflowStateForTesting.showsTrailingIndicator)

        let maximumOffset = tableView.frame.width - viewportWidth
        scrollView.contentView.scroll(to: NSPoint(x: maximumOffset, y: 0))
        scrollView.reflectScrolledClipView(scrollView.contentView)

        XCTAssertGreaterThan(
            scrollView.contentView.documentVisibleRect.minX,
            1,
            "A narrow table must allow navigation to columns outside the initial viewport."
        )
        XCTAssertTrue(scrollView.horizontalOverflowStateForTesting.showsLeadingIndicator)
        XCTAssertFalse(scrollView.horizontalOverflowStateForTesting.showsTrailingIndicator)
    }

    @MainActor
    func testGenericKindUpdateRefreshesColumnMetadataAndSortState() throws {
        let configMap = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-config",
            namespace: "synthetic-namespace",
            primaryText: "2 keys",
            secondaryText: "2 text values · 0 binary values"
        )
        let host = NSHostingView(rootView: genericResourceView(kind: .configMap, resource: configMap)
            .frame(width: 760, height: 220))
        host.frame = NSRect(x: 0, y: 0, width: 760, height: 220)
        settle(host)

        let scrollView = try XCTUnwrap(findResourceTableScrollView(in: host))
        let tableView = try XCTUnwrap(scrollView.documentView as? RuneAppKitResourceTableView)
        let primary = try XCTUnwrap(tableView.tableColumns.first { $0.identifier.rawValue == "primary" })
        XCTAssertEqual(primary.title, "Keys")
        XCTAssertTrue(primary.headerToolTip?.contains("Keys") == true)
        XCTAssertEqual(tableView.sortDescriptors.first?.key, GenericResourceListSortColumn.name.rawValue)
        XCTAssertEqual(tableView.sortDescriptors.first?.ascending, true)

        let policy = ClusterResourceSummary(
            kind: .networkPolicy,
            name: "synthetic-policy",
            namespace: "synthetic-namespace",
            primaryText: "Ingress",
            secondaryText: "app=sample"
        )
        host.rootView = genericResourceView(kind: .networkPolicy, resource: policy)
            .frame(width: 760, height: 220)
        settle(host)

        XCTAssertEqual(primary.title, "Policy Types")
        XCTAssertTrue(primary.headerToolTip?.contains("Policy Types") == true)
        let headerCell = try XCTUnwrap(primary.headerCell as? RuneAppKitResourceTableHeaderCell)
        XCTAssertEqual(headerCell.accessibilityLabel(), "Policy Types")
        XCTAssertEqual(tableView.sortDescriptors.first?.key, GenericResourceListSortColumn.name.rawValue)
        XCTAssertEqual(tableView.sortDescriptors.first?.ascending, true)
        let nameColumn = try XCTUnwrap(tableView.tableColumns.first { $0.identifier.rawValue == "name" })
        let nameHeaderCell = try XCTUnwrap(nameColumn.headerCell as? RuneAppKitResourceTableHeaderCell)
        XCTAssertTrue(nameHeaderCell.isSorted)
        XCTAssertEqual(nameHeaderCell.accessibilitySortDirection(), .ascending)
    }

    @MainActor
    func testAutomaticGenericColumnProjectionAdaptsWithoutPersistingUserWidths() throws {
        let columnIDs = ["name", "primary", "secondary", "namespace"]
        let keys = columnIDs.map {
            "rune.settings.layout.resourceColumnWidths.genericResources.configMap.\($0)"
        }
        let savedValues = Dictionary(uniqueKeysWithValues: keys.map { ($0, UserDefaults.standard.object(forKey: $0)) })
        defer {
            for key in keys {
                if let savedValue = savedValues[key] ?? nil {
                    UserDefaults.standard.set(savedValue, forKey: key)
                } else {
                    UserDefaults.standard.removeObject(forKey: key)
                }
            }
        }
        keys.forEach(UserDefaults.standard.removeObject(forKey:))

        let resource = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-config",
            namespace: "synthetic-namespace",
            primaryText: "2 keys",
            secondaryText: "2 text values · 0 binary values"
        )
        let host = NSHostingView(rootView: genericResourceView(kind: .configMap, resource: resource)
            .frame(maxWidth: .infinity, maxHeight: .infinity))
        host.frame = NSRect(x: 0, y: 0, width: 760, height: 220)
        settle(host)

        let scrollView = try XCTUnwrap(findResourceTableScrollView(in: host))
        let tableView = try XCTUnwrap(scrollView.documentView as? RuneAppKitResourceTableView)
        let nameColumn = try XCTUnwrap(tableView.tableColumns.first { $0.identifier.rawValue == "name" })
        let narrowNameWidth = nameColumn.width
        XCTAssertTrue(keys.allSatisfy { UserDefaults.standard.object(forKey: $0) == nil })

        host.frame = NSRect(x: 0, y: 0, width: 1_120, height: 220)
        settle(host)

        XCTAssertGreaterThan(nameColumn.width, narrowNameWidth)
        XCTAssertEqual(
            tableView.tableColumns.reduce(CGFloat(0)) { $0 + $1.width },
            RuneAppKitResourceListLayout.availableColumnWidth(
                visibleWidth: scrollView.contentView.documentVisibleRect.width
            ),
            accuracy: 1
        )
        XCTAssertTrue(keys.allSatisfy { UserDefaults.standard.object(forKey: $0) == nil })
    }

    @MainActor
    func testPersistedGenericWidthsStillFillAWiderViewport() throws {
        let storedWidths: [String: Double] = [
            "name": 300,
            "primary": 128,
            "secondary": 128,
            "namespace": 120
        ]
        let keys = storedWidths.keys.reduce(into: [String: String]()) { result, columnID in
            result[columnID] = "rune.settings.layout.resourceColumnWidths.genericResources.configMap.\(columnID)"
        }
        let savedValues = Dictionary(uniqueKeysWithValues: keys.values.map {
            ($0, UserDefaults.standard.object(forKey: $0))
        })
        defer {
            for key in keys.values {
                if let savedValue = savedValues[key] ?? nil {
                    UserDefaults.standard.set(savedValue, forKey: key)
                } else {
                    UserDefaults.standard.removeObject(forKey: key)
                }
            }
        }
        for (columnID, width) in storedWidths {
            UserDefaults.standard.set(width, forKey: try XCTUnwrap(keys[columnID]))
        }

        let resource = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-config",
            namespace: "synthetic-namespace",
            primaryText: "2 keys",
            secondaryText: "2 text values · 0 binary values"
        )
        let host = NSHostingView(rootView: genericResourceView(kind: .configMap, resource: resource)
            .frame(maxWidth: .infinity, maxHeight: .infinity))
        host.frame = NSRect(x: 0, y: 0, width: 1_120, height: 220)
        settle(host)

        let scrollView = try XCTUnwrap(findResourceTableScrollView(in: host))
        let tableView = try XCTUnwrap(scrollView.documentView as? RuneAppKitResourceTableView)
        let nameColumn = try XCTUnwrap(tableView.tableColumns.first { $0.identifier.rawValue == "name" })
        let expectedWidth = RuneAppKitResourceListLayout.availableColumnWidth(
            visibleWidth: scrollView.contentView.documentVisibleRect.width
        )

        XCTAssertGreaterThan(nameColumn.width, CGFloat(try XCTUnwrap(storedWidths["name"])))
        XCTAssertEqual(
            tableView.tableColumns.reduce(CGFloat(0)) { $0 + $1.width },
            expectedWidth,
            accuracy: 1,
            "Saved widths should remain preferences, not create a blank trailing strip after the viewport grows."
        )
    }

    func testEveryResourceFamilyReconcilesSavedWidthsWithTheViewport() throws {
        let source = try String(contentsOfFile: appKitResourceTablePath, encoding: .utf8)

        XCTAssertEqual(
            source.components(separatedBy: "viewportFillingFlexibleWidth(").count - 1,
            8,
            "The shared helper plus all seven resource families must reconcile persisted widths with the viewport."
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
    private func findResourceTableScrollView(in view: NSView) -> RuneAppKitResourceListScrollView? {
        if let scrollView = view as? RuneAppKitResourceListScrollView,
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

    @MainActor
    private func genericResourceView(
        kind: KubeResourceKind,
        resource: ClusterResourceSummary,
        sortColumn: GenericResourceListSortColumn = .name,
        sortAscending: Bool = true,
        onToggleSort: @escaping (GenericResourceListSortColumn) -> Void = { _ in }
    ) -> AppKitGenericResourceListView {
        AppKitGenericResourceListView(
            kind: kind,
            resources: [resource],
            selectedResourceID: nil,
            selectedResourceIDs: [],
            sortColumn: sortColumn,
            sortAscending: sortAscending,
            canApplyClusterMutations: false,
            isFavorite: { _ in false },
            onSelectResource: { _ in },
            onToggleBulkSelection: { _ in },
            onToggleSort: onToggleSort,
            onToggleFavorite: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
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
