import AppKit
import RuneCore
import SwiftUI
@testable import RuneUI
import XCTest

final class AppKitResourceTableInfrastructureTests: XCTestCase {
    func testRefreshPlanSkipsUnchangedRowsAndTargetsOnlyChangedRows() {
        let baseline = (0..<2_500).map { index in
            RuneAppKitResourceTableRowSnapshot(
                id: "resource-\(index)",
                value: "value-\(index)"
            )
        }

        XCTAssertEqual(
            RuneAppKitResourceTableRefreshPlan.resolve(
                previous: baseline,
                current: baseline
            ),
            .none
        )

        var updated = baseline
        updated[11] = RuneAppKitResourceTableRowSnapshot(
            id: "resource-11",
            value: "updated-11"
        )
        updated[1_700] = RuneAppKitResourceTableRowSnapshot(
            id: "resource-1700",
            value: "updated-1700",
            cellState: 1
        )
        XCTAssertEqual(
            RuneAppKitResourceTableRefreshPlan.resolve(
                previous: baseline,
                current: updated
            ),
            .rows(IndexSet([11, 1_700]))
        )

        var appended = baseline
        appended.append(RuneAppKitResourceTableRowSnapshot(id: "resource-2500", value: "value-2500"))
        XCTAssertEqual(
            RuneAppKitResourceTableRefreshPlan.resolve(
                previous: baseline,
                current: appended
            ),
            .reloadAll
        )

        var reordered = baseline
        reordered.swapAt(0, 1)
        XCTAssertEqual(
            RuneAppKitResourceTableRefreshPlan.resolve(
                previous: baseline,
                current: reordered
            ),
            .reloadAll
        )
    }

    @MainActor
    func testNoOpGeometryUpdateDoesNotInvalidateTableOrHeader() {
        let tableView = NSTableView()
        let first = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("first"))
        let second = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("second"))
        first.width = 180
        second.width = 220
        tableView.addTableColumn(first)
        tableView.addTableColumn(second)

        let header = NSTableHeaderView()
        tableView.headerView = header
        let renderedWidth: CGFloat = 418
        tableView.frame = NSRect(x: 0, y: 0, width: renderedWidth, height: 200)
        header.frame = NSRect(
            x: 0,
            y: 0,
            width: renderedWidth,
            height: RuneAppKitResourceTableStyle.headerHeight
        )
        tableView.needsDisplay = false
        tableView.needsLayout = false
        header.needsDisplay = false
        header.needsLayout = false

        RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)

        XCTAssertFalse(tableView.needsDisplay)
        XCTAssertFalse(tableView.needsLayout)
        XCTAssertFalse(header.needsDisplay)
        XCTAssertFalse(header.needsLayout)
    }

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
    func testResourceTableDisallowsUserClearingButPublishedNilStillClears() {
        let rows = ["resource-alpha", "resource-beta"]
        let dataSource = ResourceTableTestDataSource(rowCount: rows.count)
        let tableView = RuneAppKitResourceTableView(
            frame: NSRect(x: 0, y: 0, width: 480, height: 320)
        )
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))
        tableView.dataSource = dataSource
        RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: false)
        tableView.noteNumberOfRowsChanged()

        XCTAssertFalse(
            tableView.allowsEmptySelection,
            "Command-click must not create a transient empty selection that state immediately snaps back."
        )

        applyResourceTableSelection(
            selectedID: "resource-beta",
            rows: rows,
            rowID: { $0 },
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 1)

        applyResourceTableSelection(
            selectedID: nil,
            rows: rows,
            rowID: { $0 },
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, -1)
    }

    @MainActor
    func testSelectionBridgeKeepsUserClickAheadOfStalePublishedSelection() {
        var bridge = RuneAppKitResourceTableSelectionBridge()
        let rows = ["resource-alpha", "resource-beta", "resource-gamma"]
        let dataSource = ResourceTableTestDataSource(rowCount: rows.count)
        let tableView = makeTableView(dataSource: dataSource)

        applyResourceTableSelection(
            selectedID: "resource-alpha",
            rows: rows,
            rowID: { $0 },
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 0)

        // User clicks beta before SwiftUI publishes the new selection.
        tableView.selectRowIndexes(IndexSet(integer: 1), byExtendingSelection: false)
        bridge.noteUserSelectedID(
            "resource-beta",
            publishedSelectedID: "resource-alpha",
            staleThroughApplyGeneration: 7
        )

        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-alpha",
            rows: rows,
            rowID: { $0 },
            applyGeneration: 7,
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 1)
        XCTAssertEqual(bridge.pendingUserSelectedID, "resource-beta")

        // SwiftUI catches up — pending clears and selection stays on beta.
        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-beta",
            rows: rows,
            rowID: { $0 },
            applyGeneration: 8,
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 1)
        XCTAssertNil(bridge.pendingUserSelectedID)
    }

    @MainActor
    func testSelectionBridgeYieldsToExternalPublishedSelection() {
        var bridge = RuneAppKitResourceTableSelectionBridge()
        let rows = ["resource-alpha", "resource-beta", "resource-gamma"]
        let dataSource = ResourceTableTestDataSource(rowCount: rows.count)
        let tableView = makeTableView(dataSource: dataSource)

        tableView.selectRowIndexes(IndexSet(integer: 1), byExtendingSelection: false)
        bridge.noteUserSelectedID(
            "resource-beta",
            publishedSelectedID: "resource-alpha",
            staleThroughApplyGeneration: 4
        )

        // External navigation publishes gamma while AppKit still shows the pending click.
        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-gamma",
            rows: rows,
            rowID: { $0 },
            applyGeneration: 5,
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 2)
        XCTAssertNil(bridge.pendingUserSelectedID)
    }

    @MainActor
    func testSelectionBridgeDropsPendingSelectionWhenClickedRowDisappears() {
        var bridge = RuneAppKitResourceTableSelectionBridge()
        let initialRows = ["resource-alpha", "resource-beta"]
        let dataSource = ResourceTableTestDataSource(rowCount: initialRows.count)
        let tableView = makeTableView(dataSource: dataSource)

        tableView.selectRowIndexes(IndexSet(integer: 1), byExtendingSelection: false)
        bridge.noteUserSelectedID(
            "resource-beta",
            publishedSelectedID: "resource-alpha",
            staleThroughApplyGeneration: 3
        )

        let updatedRows = ["resource-alpha"]
        dataSource.rowCount = updatedRows.count
        tableView.reloadData()
        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-alpha",
            rows: updatedRows,
            rowID: { $0 },
            applyGeneration: 3,
            in: tableView
        )

        XCTAssertEqual(tableView.selectedRow, 0)
        XCTAssertNil(bridge.pendingUserSelectedID)
    }

    @MainActor
    func testSelectionBridgeTreatsNewerAAfterClickingBAsAuthoritative() {
        var bridge = RuneAppKitResourceTableSelectionBridge()
        let rows = ["resource-alpha", "resource-beta"]
        let dataSource = ResourceTableTestDataSource(rowCount: rows.count)
        let tableView = makeTableView(dataSource: dataSource)

        tableView.selectRowIndexes(IndexSet(integer: 1), byExtendingSelection: false)
        bridge.noteUserSelectedID(
            "resource-beta",
            publishedSelectedID: "resource-alpha",
            staleThroughApplyGeneration: 11
        )

        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-alpha",
            rows: rows,
            rowID: { $0 },
            applyGeneration: 11,
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 1, "The apply already queued before the click must not snap back.")

        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-alpha",
            rows: rows,
            rowID: { $0 },
            applyGeneration: 12,
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 0, "A newer A publication must win in an intentional A → B → A transition.")
        XCTAssertNil(bridge.pendingUserSelectedID)
    }

    @MainActor
    func testSelectionBridgeTracksPendingIDAcrossRenderedRowReorder() {
        var bridge = RuneAppKitResourceTableSelectionBridge()
        let initialRows = ["resource-alpha", "resource-beta"]
        let reorderedRows = ["resource-beta", "resource-alpha"]
        let dataSource = ResourceTableTestDataSource(rowCount: initialRows.count)
        let tableView = makeTableView(dataSource: dataSource)

        tableView.selectRowIndexes(IndexSet(integer: 1), byExtendingSelection: false)
        bridge.noteUserSelectedID(
            "resource-beta",
            publishedSelectedID: "resource-alpha",
            staleThroughApplyGeneration: 9
        )

        tableView.reloadData()
        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-alpha",
            rows: reorderedRows,
            rowID: { $0 },
            applyGeneration: 9,
            in: tableView
        )

        XCTAssertEqual(tableView.selectedRow, 0)
        XCTAssertEqual(bridge.pendingUserSelectedID, "resource-beta")
    }

    @MainActor
    func testSelectionBridgeRejectsSupersededDeferredContextMenuIntent() {
        var bridge = RuneAppKitResourceTableSelectionBridge()

        let staleIntent = bridge.noteUserSelectedID(
            "resource-beta",
            publishedSelectedID: "resource-alpha",
            staleThroughApplyGeneration: 2
        )
        let currentIntent = bridge.noteUserSelectedID(
            "resource-gamma",
            publishedSelectedID: "resource-alpha",
            staleThroughApplyGeneration: 2
        )

        XCTAssertFalse(
            bridge.prepareToPublish(
                staleIntent,
                displayedSelectedID: "resource-beta",
                staleThroughApplyGeneration: 2
            )
        )
        XCTAssertTrue(
            bridge.prepareToPublish(
                currentIntent,
                displayedSelectedID: "resource-gamma",
                staleThroughApplyGeneration: 3
            )
        )
    }

    @MainActor
    func testSelectionBridgeRejectsDeferredIntentAfterNewerParentRevision() {
        var bridge = RuneAppKitResourceTableSelectionBridge()
        let intent = bridge.noteUserSelectedID(
            "resource-beta",
            publishedSelectedID: "resource-alpha",
            staleThroughApplyGeneration: 2,
            publishedSelectionRevision: 1
        )

        XCTAssertFalse(
            bridge.prepareToPublish(
                intent,
                displayedSelectedID: "resource-beta",
                staleThroughApplyGeneration: 3,
                publishedSelectedID: "resource-gamma",
                publishedSelectionRevision: 2
            ),
            "A deferred mouse intent must not overwrite a selection that advanced while it was queued."
        )
        XCTAssertNil(bridge.pendingUserSelectedID)
    }

    @MainActor
    func testVersionedSelectionBridgeUsesActualPostClickRevisionAcrossIntermediateProjection() {
        var bridge = RuneAppKitResourceTableSelectionBridge()
        let rows = ["resource-alpha", "resource-beta", "resource-gamma"]
        let dataSource = ResourceTableTestDataSource(rowCount: rows.count)
        let tableView = makeTableView(dataSource: dataSource)

        tableView.selectRowIndexes(IndexSet(integer: 1), byExtendingSelection: false)
        let intent = bridge.noteUserSelectedID(
            "resource-beta",
            publishedSelectedID: "resource-alpha",
            staleThroughApplyGeneration: 4,
            publishedSelectionRevision: 1
        )
        bridge.confirmPublishedUserSelection(intent, selectionRevision: 5)

        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-gamma",
            rows: rows,
            rowID: { $0 },
            applyGeneration: 5,
            publishedSelectionRevision: 4,
            in: tableView
        )
        XCTAssertEqual(
            tableView.selectedRow,
            1,
            "A projection newer than the parent baseline can still predate the click's actual state revision."
        )

        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-beta",
            rows: rows,
            rowID: { $0 },
            applyGeneration: 6,
            publishedSelectionRevision: 5,
            in: tableView
        )
        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-gamma",
            rows: rows,
            rowID: { $0 },
            applyGeneration: 7,
            publishedSelectionRevision: 4,
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 1)

        applyBridgedResourceTableSelection(
            bridge: &bridge,
            publishedSelectedID: "resource-alpha",
            rows: rows,
            rowID: { $0 },
            applyGeneration: 8,
            publishedSelectionRevision: 6,
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRow, 0)
    }

    @MainActor
    func testSelectionBridgeResetAcceptsLowerRevisionFromAnotherResourceFamily() {
        var bridge = RuneAppKitResourceTableSelectionBridge()
        let intent = bridge.noteUserSelectedID(
            "pod-beta",
            publishedSelectedID: "pod-alpha",
            staleThroughApplyGeneration: 2,
            publishedSelectionRevision: 9
        )
        bridge.confirmPublishedUserSelection(intent, selectionRevision: 10)

        bridge.reset()

        XCTAssertNil(bridge.pendingUserSelectedID)
        XCTAssertEqual(
            bridge.projectedSelectedID(
                publishedSelectedID: "service-alpha",
                pendingSelectionIsAvailable: true,
                applyGeneration: 3,
                publishedSelectionRevision: 0
            ),
            "service-alpha",
            "A generic table reused for another resource kind must not carry the previous kind's revision high-water mark."
        )
    }

    func testDisplayedRowSnapshotKeepsRowEventsBoundToRenderedOrder() {
        let displayedRows = [
            RuneAppKitResourceTableRowSnapshot(id: "resource-alpha", value: "rendered-alpha"),
            RuneAppKitResourceTableRowSnapshot(id: "resource-beta", value: "rendered-beta")
        ]
        let newerParentOrder = ["resource-beta", "resource-alpha"]

        XCTAssertEqual(newerParentOrder[0], "resource-beta")
        XCTAssertEqual(displayedResourceTableRow(at: 0, in: displayedRows)?.id, "resource-alpha")
        XCTAssertEqual(displayedResourceTableRow(at: 0, in: displayedRows)?.value, "rendered-alpha")
        XCTAssertNil(displayedResourceTableRow(at: 2, in: displayedRows))
    }

    @MainActor
    func testCoordinatorClickUsesRenderedSnapshotWhileReorderApplyIsPending() {
        let alpha = ServiceSummary(
            name: "service-alpha",
            namespace: "synthetic",
            type: "ClusterIP",
            clusterIP: "192.0.2.10"
        )
        let beta = ServiceSummary(
            name: "service-beta",
            namespace: "synthetic",
            type: "ClusterIP",
            clusterIP: "192.0.2.11"
        )
        var selectedIDs: [String] = []

        func view(services: [ServiceSummary]) -> AppKitServiceListView {
            AppKitServiceListView(
                services: services,
                selectedServiceID: beta.id,
                sortColumn: .name,
                sortAscending: true,
                canApplyClusterMutations: false,
                isFavorite: { _ in false },
                onSelectService: { selectedIDs.append($0.id) },
                onToggleSort: { _ in },
                onToggleFavorite: { _ in },
                onOpenUnifiedLogs: { _ in },
                onOpenPortForward: { _ in },
                onOpenDescribe: { _ in },
                onOpenYAML: { _ in },
                onDelete: { _ in }
            )
        }

        let initialView = view(services: [alpha, beta])
        let coordinator = initialView.makeCoordinator()
        let tableView = RuneAppKitResourceTableView(
            frame: NSRect(x: 0, y: 0, width: 480, height: 240)
        )
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))
        tableView.dataSource = coordinator
        tableView.delegate = coordinator
        coordinator.tableView = tableView
        coordinator.apply(parent: initialView)
        settle(tableView)
        XCTAssertEqual(tableView.selectedRow, 1)

        let reorderedView = view(services: [beta, alpha])
        coordinator.parent = reorderedView
        coordinator.apply(parent: reorderedView)

        // AppKit still renders [alpha, beta] until the queued apply runs.
        tableView.selectRowIndexes(IndexSet(integer: 0), byExtendingSelection: false)
        XCTAssertEqual(selectedIDs.last, alpha.id)

        settle(tableView)
        XCTAssertEqual(
            tableView.selectedRow,
            1,
            "The pending alpha click must follow alpha's ID when the rendered rows reorder."
        )
    }

    @MainActor
    func testCoordinatorRejectsClickForDisplayedRowRemovedFromLatestParent() {
        let services = ["alpha", "beta", "gamma"].enumerated().map { index, name in
            ServiceSummary(
                name: "service-\(name)",
                namespace: "synthetic",
                type: "ClusterIP",
                clusterIP: "192.0.2.\(40 + index)"
            )
        }
        var selectedIDs: [String] = []

        func view(services: [ServiceSummary]) -> AppKitServiceListView {
            AppKitServiceListView(
                services: services,
                selectedServiceID: services[0].id,
                sortColumn: .name,
                sortAscending: true,
                canApplyClusterMutations: false,
                isFavorite: { _ in false },
                onSelectService: { selectedIDs.append($0.id) },
                onToggleSort: { _ in },
                onToggleFavorite: { _ in },
                onOpenUnifiedLogs: { _ in },
                onOpenPortForward: { _ in },
                onOpenDescribe: { _ in },
                onOpenYAML: { _ in },
                onDelete: { _ in }
            )
        }

        let initialView = view(services: services)
        let coordinator = initialView.makeCoordinator()
        let tableView = RuneAppKitResourceTableView(
            frame: NSRect(x: 0, y: 0, width: 480, height: 240)
        )
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))
        tableView.dataSource = coordinator
        tableView.delegate = coordinator
        coordinator.tableView = tableView
        coordinator.apply(parent: initialView)
        settle(tableView)
        XCTAssertEqual(tableView.numberOfRows, 3)
        XCTAssertEqual(tableView.selectedRow, 0)

        let updatedView = view(services: Array(services.prefix(2)))
        coordinator.apply(parent: updatedView)

        // The parent already contains A/B, but AppKit still exposes the old A/B/C snapshot.
        XCTAssertEqual(tableView.numberOfRows, 3)
        tableView.selectRowIndexes(IndexSet(integer: 2), byExtendingSelection: false)

        XCTAssertTrue(
            selectedIDs.isEmpty,
            "A click on removed C must not publish its stale rendered model."
        )

        settle(tableView)
        XCTAssertEqual(tableView.numberOfRows, 2)
        XCTAssertEqual(tableView.selectedRow, 0)
        XCTAssertLessThan(tableView.selectedRow, updatedView.services.count)
    }

    @MainActor
    func testCoordinatorKeepsLatestRapidClickAcrossRepeatedStaleParentProjection() {
        let services = ["alpha", "beta", "gamma"].map { name in
            ServiceSummary(
                name: "service-\(name)",
                namespace: "synthetic",
                type: "ClusterIP",
                clusterIP: "192.0.2.30"
            )
        }
        var selectedIDs: [String] = []
        var liveSelectionRevision: UInt64 = 1

        func view(selectedID: String, revision: UInt64) -> AppKitServiceListView {
            AppKitServiceListView(
                services: services,
                selectedServiceID: selectedID,
                selectionRevision: revision,
                selectionRevisionAfterSelect: { liveSelectionRevision },
                sortColumn: .name,
                sortAscending: true,
                canApplyClusterMutations: false,
                isFavorite: { _ in false },
                onSelectService: {
                    selectedIDs.append($0.id)
                    liveSelectionRevision += 1
                },
                onToggleSort: { _ in },
                onToggleFavorite: { _ in },
                onOpenUnifiedLogs: { _ in },
                onOpenPortForward: { _ in },
                onOpenDescribe: { _ in },
                onOpenYAML: { _ in },
                onDelete: { _ in }
            )
        }

        let initialView = view(selectedID: services[0].id, revision: 1)
        let coordinator = initialView.makeCoordinator()
        let tableView = RuneAppKitResourceTableView(
            frame: NSRect(x: 0, y: 0, width: 480, height: 240)
        )
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))
        tableView.dataSource = coordinator
        tableView.delegate = coordinator
        coordinator.tableView = tableView
        coordinator.apply(parent: initialView)
        settle(tableView)

        tableView.selectRowIndexes(IndexSet(integer: 1), byExtendingSelection: false)
        XCTAssertEqual(selectedIDs.last, services[1].id)
        XCTAssertEqual(liveSelectionRevision, 2)

        let betaProjection = view(selectedID: services[1].id, revision: 2)
        coordinator.apply(parent: betaProjection)
        settle(tableView)
        coordinator.apply(parent: betaProjection)
        settle(tableView)

        tableView.selectRowIndexes(IndexSet(integer: 2), byExtendingSelection: false)
        XCTAssertEqual(selectedIDs.last, services[2].id)
        XCTAssertEqual(liveSelectionRevision, 3)

        // A SwiftUI value built while beta was current can reach updateNSView
        // after the gamma click. It must not overwrite the newer user intent.
        coordinator.apply(parent: betaProjection)
        settle(tableView)

        XCTAssertEqual(tableView.selectedRow, 2)
        XCTAssertEqual(selectedIDs, [services[1].id, services[2].id])

        let gammaProjection = view(selectedID: services[2].id, revision: 3)
        coordinator.apply(parent: gammaProjection)
        settle(tableView)
        coordinator.apply(parent: betaProjection)
        settle(tableView)
        XCTAssertEqual(
            tableView.selectedRow,
            2,
            "A stale beta projection must stay rejected after gamma was acknowledged."
        )

        // The coordinator's parent is still the stale beta projection while
        // AppKit correctly displays gamma. Right-clicking beta must therefore
        // compare with the displayed selection, not skip publication because
        // the stale parent already says beta.
        coordinator.selectRowForContextMenu(1, in: tableView)
        settle(tableView)
        XCTAssertEqual(selectedIDs.last, services[1].id)
        XCTAssertEqual(liveSelectionRevision, 4)
        XCTAssertEqual(tableView.selectedRow, 1)

        liveSelectionRevision = 5
        let newerExternalAlphaProjection = view(selectedID: services[0].id, revision: 5)
        coordinator.apply(parent: newerExternalAlphaProjection)
        settle(tableView)
        XCTAssertEqual(
            tableView.selectedRow,
            0,
            "A genuinely newer external selection revision must remain authoritative."
        )
    }

    @MainActor
    func testCoordinatorProtectsProposedRowBeforeMouseUpFromQueuedStaleApply() {
        let services = ["alpha", "beta", "gamma"].map { name in
            ServiceSummary(
                name: "service-\(name)",
                namespace: "synthetic",
                type: "ClusterIP",
                clusterIP: "192.0.2.50"
            )
        }
        var selectedIDs: [String] = []
        var liveSelectionRevision: UInt64 = 2

        func view(selectedID: String, revision: UInt64) -> AppKitServiceListView {
            AppKitServiceListView(
                services: services,
                selectedServiceID: selectedID,
                selectionRevision: revision,
                selectionRevisionAfterSelect: { liveSelectionRevision },
                sortColumn: .name,
                sortAscending: true,
                canApplyClusterMutations: false,
                isFavorite: { _ in false },
                onSelectService: {
                    selectedIDs.append($0.id)
                    liveSelectionRevision += 1
                },
                onToggleSort: { _ in },
                onToggleFavorite: { _ in },
                onOpenUnifiedLogs: { _ in },
                onOpenPortForward: { _ in },
                onOpenDescribe: { _ in },
                onOpenYAML: { _ in },
                onDelete: { _ in }
            )
        }

        let betaProjection = view(selectedID: services[1].id, revision: 2)
        let coordinator = betaProjection.makeCoordinator()
        let tableView = RuneAppKitResourceTableView(
            frame: NSRect(x: 0, y: 0, width: 480, height: 240)
        )
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))
        tableView.dataSource = coordinator
        tableView.delegate = coordinator
        coordinator.tableView = tableView
        coordinator.apply(parent: betaProjection)
        settle(tableView)
        XCTAssertEqual(tableView.selectedRow, 1)

        // This is the real AppKit race window: the proposed row is known at
        // mouse-down, but selectionDidChange does not arrive until mouse-up.
        XCTAssertEqual(
            coordinator.tableView(
                tableView,
                selectionIndexesForProposedSelection: IndexSet(integer: 2)
            ),
            IndexSet(integer: 2)
        )
        coordinator.apply(parent: betaProjection)
        settle(tableView)

        XCTAssertEqual(
            tableView.selectedRow,
            2,
            "The apply queued during mouse tracking must preserve the proposed gamma row."
        )
        XCTAssertTrue(selectedIDs.isEmpty, "Selection must publish once, when the gesture completes.")

        tableView.finishMouseSelectionTracking()

        XCTAssertEqual(selectedIDs, [services[2].id])
        XCTAssertEqual(liveSelectionRevision, 3)
        XCTAssertEqual(tableView.selectedRow, 2)

        let gammaProjection = view(selectedID: services[2].id, revision: 3)
        coordinator.apply(parent: gammaProjection)
        settle(tableView)
        _ = coordinator.tableView(
            tableView,
            selectionIndexesForProposedSelection: IndexSet(integer: 0)
        )

        // An external selection at exactly the click's expected next
        // revision is authoritative; it must not be mistaken for stale state.
        liveSelectionRevision = 4
        let externalBetaProjection = view(selectedID: services[1].id, revision: 4)
        coordinator.apply(parent: externalBetaProjection)
        settle(tableView)
        XCTAssertEqual(tableView.selectedRow, 1)

        tableView.finishMouseSelectionTracking()
        XCTAssertEqual(selectedIDs, [services[2].id])
        XCTAssertEqual(tableView.selectedRow, 1)
    }

    @MainActor
    func testCoordinatorRejectsDeferredContextSelectionAfterNewerExternalNavigation() {
        let services = ["alpha", "beta", "gamma"].map { name in
            ServiceSummary(
                name: "service-\(name)",
                namespace: "synthetic",
                type: "ClusterIP",
                clusterIP: "192.0.2.51"
            )
        }
        var selectedIDs: [String] = []
        var liveSelectionRevision: UInt64 = 1

        func view(
            services: [ServiceSummary],
            selectedID: String,
            revision: UInt64
        ) -> AppKitServiceListView {
            AppKitServiceListView(
                services: services,
                selectedServiceID: selectedID,
                selectionRevision: revision,
                selectionRevisionAfterSelect: { liveSelectionRevision },
                sortColumn: .name,
                sortAscending: true,
                canApplyClusterMutations: false,
                isFavorite: { _ in false },
                onSelectService: {
                    selectedIDs.append($0.id)
                    liveSelectionRevision += 1
                },
                onToggleSort: { _ in },
                onToggleFavorite: { _ in },
                onOpenUnifiedLogs: { _ in },
                onOpenPortForward: { _ in },
                onOpenDescribe: { _ in },
                onOpenYAML: { _ in },
                onDelete: { _ in }
            )
        }

        let initialView = view(
            services: services,
            selectedID: services[0].id,
            revision: 1
        )
        let coordinator = initialView.makeCoordinator()
        let tableView = RuneAppKitResourceTableView(
            frame: NSRect(x: 0, y: 0, width: 480, height: 240)
        )
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))
        tableView.dataSource = coordinator
        tableView.delegate = coordinator
        coordinator.tableView = tableView
        coordinator.apply(parent: initialView)
        settle(tableView)

        coordinator.selectRowForContextMenu(1, in: tableView)

        liveSelectionRevision = 3
        let reorderedServices = [services[2], services[1], services[0]]
        coordinator.apply(
            parent: view(
                services: reorderedServices,
                selectedID: services[2].id,
                revision: 3
            )
        )
        settle(tableView)

        XCTAssertTrue(
            selectedIDs.isEmpty,
            "The older deferred beta intent must not republish over external gamma."
        )
        XCTAssertEqual(tableView.selectedRow, 0)
        XCTAssertEqual(reorderedServices[tableView.selectedRow].id, services[2].id)
    }

    @MainActor
    func testCoordinatorKeepsEveryOneOfSixtyFourRapidProposedSelections() {
        let services = (0..<8).map { index in
            ServiceSummary(
                name: "service-\(index)",
                namespace: "synthetic",
                type: "ClusterIP",
                clusterIP: "192.0.2.\(60 + index)"
            )
        }
        var selectedIDs: [String] = []
        var authoritativeSelectedID = services[0].id
        var liveSelectionRevision: UInt64 = 1

        func view(
            services: [ServiceSummary],
            selectedID: String,
            revision: UInt64
        ) -> AppKitServiceListView {
            AppKitServiceListView(
                services: services,
                selectedServiceID: selectedID,
                selectionRevision: revision,
                selectionRevisionAfterSelect: { liveSelectionRevision },
                sortColumn: .name,
                sortAscending: true,
                canApplyClusterMutations: false,
                isFavorite: { _ in false },
                onSelectService: {
                    authoritativeSelectedID = $0.id
                    selectedIDs.append($0.id)
                    liveSelectionRevision += 1
                },
                onToggleSort: { _ in },
                onToggleFavorite: { _ in },
                onOpenUnifiedLogs: { _ in },
                onOpenPortForward: { _ in },
                onOpenDescribe: { _ in },
                onOpenYAML: { _ in },
                onDelete: { _ in }
            )
        }

        let initialView = view(
            services: services,
            selectedID: authoritativeSelectedID,
            revision: liveSelectionRevision
        )
        let coordinator = initialView.makeCoordinator()
        let tableView = RuneAppKitResourceTableView(
            frame: NSRect(x: 0, y: 0, width: 480, height: 240)
        )
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))
        tableView.dataSource = coordinator
        tableView.delegate = coordinator
        coordinator.tableView = tableView
        coordinator.apply(parent: initialView)
        settle(tableView)

        var renderedServices = services
        for click in 0..<64 {
            let target = services[(click * 5 + 1) % services.count]
            let targetRow = try! XCTUnwrap(
                renderedServices.firstIndex(where: { $0.id == target.id })
            )
            let staleSelectedID = authoritativeSelectedID
            let staleRevision = liveSelectionRevision

            _ = coordinator.tableView(
                tableView,
                selectionIndexesForProposedSelection: IndexSet(integer: targetRow)
            )

            if click.isMultiple(of: 8) {
                let temporarilyFiltered = renderedServices.filter { $0.id != target.id }
                coordinator.apply(
                    parent: view(
                        services: temporarilyFiltered,
                        selectedID: staleSelectedID,
                        revision: staleRevision
                    )
                )
                settle(tableView)
                XCTAssertEqual(
                    selectedIDs.count,
                    click,
                    "A stale filtered projection must not complete or discard click \(click)."
                )
            }

            let shift = (click + 1) % services.count
            var reordered = Array(services[shift...] + services[..<shift])
            if !click.isMultiple(of: 2) {
                reordered.reverse()
            }
            coordinator.apply(
                parent: view(
                    services: reordered,
                    selectedID: staleSelectedID,
                    revision: staleRevision
                )
            )
            settle(tableView)
            renderedServices = reordered

            XCTAssertEqual(
                renderedServices[tableView.selectedRow].id,
                target.id,
                "Click \(click) snapped back before mouse-up."
            )
            tableView.finishMouseSelectionTracking()
            XCTAssertEqual(selectedIDs.last, target.id, "Click \(click) published the wrong row ID.")
            XCTAssertEqual(authoritativeSelectedID, target.id)
        }

        coordinator.apply(
            parent: view(
                services: renderedServices,
                selectedID: authoritativeSelectedID,
                revision: liveSelectionRevision
            )
        )
        settle(tableView)
        let penultimateID = selectedIDs[selectedIDs.count - 2]
        coordinator.apply(
            parent: view(
                services: renderedServices,
                selectedID: penultimateID,
                revision: liveSelectionRevision - 1
            )
        )
        settle(tableView)

        XCTAssertEqual(selectedIDs.count, 64)
        XCTAssertEqual(
            renderedServices[tableView.selectedRow].id,
            authoritativeSelectedID,
            "A stale projection after the stress run must not undo the sixty-fourth click."
        )
    }

    @MainActor
    func testCoordinatorDropsSupersededDeferredContextMenuSelection() {
        let services = ["alpha", "beta", "gamma"].map { name in
            ServiceSummary(
                name: "service-\(name)",
                namespace: "synthetic",
                type: "ClusterIP",
                clusterIP: "192.0.2.20"
            )
        }
        var selectedIDs: [String] = []
        let view = AppKitServiceListView(
            services: services,
            selectedServiceID: services[0].id,
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: false,
            isFavorite: { _ in false },
            onSelectService: { selectedIDs.append($0.id) },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenUnifiedLogs: { _ in },
            onOpenPortForward: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let coordinator = view.makeCoordinator()
        let tableView = RuneAppKitResourceTableView(
            frame: NSRect(x: 0, y: 0, width: 480, height: 240)
        )
        tableView.addTableColumn(NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name")))
        tableView.dataSource = coordinator
        tableView.delegate = coordinator
        coordinator.tableView = tableView
        coordinator.apply(parent: view)
        settle(tableView)

        coordinator.selectRowForContextMenu(1, in: tableView)
        coordinator.selectRowForContextMenu(2, in: tableView)
        settle(tableView)

        XCTAssertEqual(selectedIDs, [services[2].id])
        XCTAssertEqual(tableView.selectedRow, 2)
    }

    @MainActor
    func testApplyResourceTableSelectionSkipsNoOpReselect() {
        let rows = ["resource-alpha", "resource-beta"]
        let dataSource = ResourceTableTestDataSource(rowCount: rows.count)
        let tableView = makeTableView(dataSource: dataSource)

        applyResourceTableSelection(
            selectedID: "resource-beta",
            rows: rows,
            rowID: { $0 },
            in: tableView
        )
        let selectedIndexes = tableView.selectedRowIndexes
        applyResourceTableSelection(
            selectedID: "resource-beta",
            rows: rows,
            rowID: { $0 },
            in: tableView
        )
        XCTAssertEqual(tableView.selectedRowIndexes, selectedIndexes)
    }

    func testResourceTableSelectionInvalidationIsWiredForCustomHighlights() throws {
        let source = try String(contentsOfFile: appKitResourceTablePath, encoding: .utf8)
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("func invalidateResourceTableSelectionDisplay("))
        XCTAssertTrue(source.contains("struct RuneAppKitResourceTableSelectionBridge"))
        XCTAssertTrue(source.contains("func applyBridgedResourceTableSelection<Row>("))
        XCTAssertTrue(source.contains("override func mouseDown(with event: NSEvent)"))
        XCTAssertTrue(
            source.contains(
                "super.mouseDown(with: event)\n        finishMouseSelectionTracking()"
            )
        )
        XCTAssertTrue(
            source.contains(
                "(delegate as? RuneAppKitResourceTableSelectionTrackingDelegate)?"
            ),
            "The shared table must finish a captured gesture even if an interleaved programmatic selection suppresses AppKit's usual notification."
        )
        XCTAssertTrue(source.contains("override func selectRowIndexes(_ indexes: IndexSet, byExtendingSelection extend: Bool)"))
        XCTAssertTrue(source.contains("override func deselectAll(_ sender: Any?)"))
        XCTAssertTrue(source.contains("allowsMultipleSelection = false"))
        XCTAssertTrue(source.contains("allowsEmptySelection = false"))
        XCTAssertTrue(source.contains("selectionHighlightStyle = .none"))
        XCTAssertTrue(source.contains("action: #selector(toggleBulkSelection(_:))"))
        XCTAssertEqual(
            source.components(separatedBy: "private var selectionBridge = RuneAppKitResourceTableSelectionBridge()").count - 1,
            7
        )
        XCTAssertEqual(
            source.components(separatedBy: "applyBridgedResourceTableSelection(\n").count - 1,
            7,
            "Every resource-table coordinator must project selection through the shared bridge."
        )
        XCTAssertEqual(
            source.components(separatedBy: "displayedRows?.count ?? 0").count - 1,
            7,
            "The data source row count must come from the snapshot currently rendered by AppKit."
        )
        XCTAssertEqual(
            source.components(separatedBy: "self.selectionBridge.prepareToPublish(").count - 1,
            7,
            "Every deferred context-menu selection must verify that its user intent is still current."
        )
        XCTAssertEqual(
            source.components(separatedBy: "var selectionRevisionAfterSelect: (() -> UInt64)? = nil").count - 1,
            7,
            "All seven resource tables must read the actual state revision after a click."
        )
        XCTAssertEqual(
            source.components(separatedBy: "confirmPublishedUserSelection(").count - 1,
            15,
            "The bridge declaration plus direct and context-menu paths must confirm post-click revisions."
        )
        XCTAssertEqual(
            rootSource.components(separatedBy: "resourceSelectionRevision(").count - 1,
            14,
            "Every production middle-panel table must use a family-scoped revision for both projection and its live post-click acknowledgement."
        )
        XCTAssertTrue(source.contains("selectionRevisionKind != parent.resolvedResourceKind"))
        XCTAssertTrue(source.contains("selectionBridge.reset()"))
        XCTAssertEqual(
            source.components(separatedBy: "first(where: { $0.id == intent.id })").count - 1,
            7,
            "Every direct-click path must resolve its captured intent ID against the latest parent rows."
        )
        XCTAssertEqual(
            source.components(
                separatedBy: "selectionIndexesForProposedSelection proposedSelectionIndexes"
            ).count - 1,
            7,
            "Every resource table must capture mouse intent before selectionDidChange at mouse-up."
        )
        XCTAssertEqual(
            source.components(separatedBy: "private var proposedUserSelectionIntent:").count - 1,
            7,
            "Every resource table must retain the proposed row ID across the AppKit mouse-tracking window."
        )
        XCTAssertEqual(
            source.components(
                separatedBy: "func resourceTableDidFinishMouseSelection(_ tableView: NSTableView)"
            ).count - 1,
            8,
            "The tracking protocol plus all seven coordinators must finalize a gesture after mouseDown returns."
        )
        XCTAssertFalse(source.contains("parent.pods[tableView.selectedRow]"))
        XCTAssertFalse(source.contains("parent.deployments[tableView.selectedRow]"))
        XCTAssertFalse(source.contains("parent.services[tableView.selectedRow]"))
        XCTAssertFalse(source.contains("parent.releases[tableView.selectedRow]"))
        XCTAssertFalse(source.contains("parent.events[tableView.selectedRow]"))
        XCTAssertFalse(source.contains("parent.resources[tableView.selectedRow]"))
        XCTAssertTrue(source.contains("func applyBridgedResourceTableSelection<Row>("))
        XCTAssertTrue(
            source.contains("let rowsToRedraw = selectedRowIndexes.union(indexes)"),
            "Native clicks must invalidate the previous and next custom selection fills."
        )
        XCTAssertTrue(
            source.contains("invalidateResourceTableSelectionDisplay(\n        previousSelectedRows.union(IndexSet(integer: selectedRow)),\n        in: tableView\n    )")
                || source.contains("previousSelectedRows.union(IndexSet(integer: selectedRow))"),
            "Programmatic selection projection must clear stale custom row fills."
        )
    }

    @MainActor
    func testHostedPodTableSingleSelectionKeepsOnlyOneSelectedRow() throws {
        let pods = (0..<8).map { index in
            PodSummary(
                name: "pod-\(index)",
                namespace: "default",
                status: "Running"
            )
        }
        var selectedPodID: String? = pods[0].id
        let selectedPodIDs: Set<String> = []
        let host = NSHostingView(rootView: AppKitPodTableView(
            pods: pods,
            selectedPodID: selectedPodID,
            selectedPodIDs: selectedPodIDs,
            sortColumn: .name,
            sortAscending: true,
            nameColumnWidth: 280,
            canApplyClusterMutations: false,
            isFavorite: { _ in false },
            onSelectPod: { selectedPodID = $0.id },
            onToggleBulkSelection: { _ in },
            onToggleSort: { _ in },
            onNameColumnWidthChanged: { _ in },
            onToggleFavorite: { _ in },
            onOpenLogs: { _ in },
            onOpenExec: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        ).frame(width: 960, height: 280))
        host.frame = NSRect(x: 0, y: 0, width: 960, height: 280)
        settle(host)

        let scrollView = try XCTUnwrap(findResourceTableScrollView(in: host))
        let tableView = try XCTUnwrap(scrollView.documentView as? RuneAppKitResourceTableView)
        XCTAssertEqual(tableView.allowsMultipleSelection, false)

        applyResourceTableSelection(
            selectedID: pods[0].id,
            rows: pods,
            rowID: \.id,
            in: tableView
        )
        applyResourceTableSelection(
            selectedID: pods[3].id,
            rows: pods,
            rowID: \.id,
            in: tableView
        )

        XCTAssertEqual(tableView.selectedRowIndexes, IndexSet(integer: 3))
        XCTAssertEqual(tableView.numberOfSelectedRows, 1)

        var selectedCount = 0
        for row in 0..<tableView.numberOfRows {
            guard let rowView = tableView.rowView(atRow: row, makeIfNecessary: false) else { continue }
            if rowView.isSelected {
                selectedCount += 1
                XCTAssertEqual(row, 3)
            }
        }
        XCTAssertEqual(selectedCount, 1)
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

    func testAllResourceTablesUseSharedIdentifierBackedCellAndRowReuse() throws {
        let source = try String(contentsOfFile: appKitResourceTablePath, encoding: .utf8)
        let viewProviderSignature =
            "func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView?"
        let viewProviderBodies = source
            .components(separatedBy: viewProviderSignature)
            .dropFirst()
            .compactMap { remainder -> String? in
                guard let end = remainder.range(of: "func tableViewSelectionDidChange") else {
                    return nil
                }
                return String(remainder[..<end.lowerBound])
            }

        XCTAssertTrue(source.contains("private func resourceTableCellIdentifier(for tableColumn: NSTableColumn)"))
        XCTAssertTrue(source.contains(#""rune.resource-cell.\(tableColumn.identifier.rawValue)""#))
        XCTAssertTrue(source.contains("private func dequeueResourceTableCell<Cell: NSView>("))
        XCTAssertTrue(source.contains("tableView.makeView(withIdentifier: identifier, owner: tableView.delegate) as? Cell"))
        XCTAssertTrue(source.contains("cell.identifier = identifier"))
        XCTAssertTrue(source.contains("private func dequeueResourceTableRow(from tableView: NSTableView)"))
        XCTAssertTrue(source.contains(#"NSUserInterfaceItemIdentifier("rune.resource-row")"#))
        XCTAssertTrue(source.contains("rowView.identifier = identifier"))
        XCTAssertTrue(source.contains("private final class RuneAppKitCheckboxCell: NSView"))

        XCTAssertEqual(
            viewProviderBodies.count,
            7,
            "Every shared resource-table family must expose one AppKit view provider."
        )
        for body in viewProviderBodies {
            XCTAssertTrue(
                body.contains("resourceTableLabelCell(")
                    || body.contains("resourceTablePillCell(")
                    || body.contains("resourceTableFavoriteCell(")
                    || body.contains("resourceTableCheckboxCell("),
                "Every resource-table view provider must route cell creation through the shared dequeue/configure path."
            )
        }
        XCTAssertEqual(
            source.components(separatedBy: "dequeueResourceTableRow(from: tableView)").count - 1,
            7,
            "Every resource-table family must reuse identifier-backed row views."
        )
    }

    @MainActor
    func testHostedResourceTableActuallyReusesCellsAndRowsAcrossDistantViewports() throws {
        let resources = (0..<160).map { index in
            ClusterResourceSummary(
                kind: .configMap,
                name: String(format: "synthetic-config-%03d", index),
                namespace: "synthetic-namespace",
                primaryText: "\(index % 12 + 1) keys",
                secondaryText: "\(index % 5 + 1) KiB"
            )
        }
        let host = NSHostingView(rootView: AppKitGenericResourceListView(
            kind: .configMap,
            resources: resources,
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
        ).frame(width: 1_120, height: 240))
        host.frame = NSRect(x: 0, y: 0, width: 1_120, height: 240)
        settle(host)

        let scrollView = try XCTUnwrap(findResourceTableScrollView(in: host))
        let tableView = try XCTUnwrap(scrollView.documentView as? RuneAppKitResourceTableView)
        XCTAssertEqual(tableView.numberOfRows, resources.count)

        tableView.scrollRowToVisible(0)
        settle(host)
        let leadingViewport = identifierBackedVisibleViews(in: tableView)
        XCTAssertFalse(leadingViewport.cells.isEmpty)
        XCTAssertFalse(leadingViewport.rows.isEmpty)

        tableView.scrollRowToVisible(120)
        settle(host)
        let distantViewport = identifierBackedVisibleViews(in: tableView)
        XCTAssertFalse(distantViewport.cells.isEmpty)
        XCTAssertFalse(distantViewport.rows.isEmpty)

        XCTAssertFalse(
            leadingViewport.cells.isDisjoint(with: distantViewport.cells),
            "Scrolling to distant rows must reuse cell objects instead of rebuilding constraints and controls."
        )
        XCTAssertFalse(
            leadingViewport.rows.isDisjoint(with: distantViewport.rows),
            "Scrolling to distant rows must reuse the shared identifier-backed row views."
        )
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
        XCTAssertTrue(source.contains("scrollView.usesPredominantAxisScrolling = true"))
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
    func testHorizontalOverflowEdgeGlowCanBeDisabledWithoutDisablingScrolling() throws {
        let resource = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-config",
            namespace: "synthetic-namespace",
            primaryText: "3 keys",
            secondaryText: "1 KiB"
        )
        let host = NSHostingView(rootView: genericResourceView(kind: .configMap, resource: resource)
            .frame(width: 360, height: 220))
        host.frame = NSRect(x: 0, y: 0, width: 360, height: 220)
        settle(host)

        let scrollView = try XCTUnwrap(findResourceTableScrollView(in: host))
        scrollView.layoutSubtreeIfNeeded()

        XCTAssertTrue(scrollView.hasHorizontalScroller)
        XCTAssertTrue(scrollView.horizontalOverflowStateForTesting.hasOverflow)
        XCTAssertTrue(scrollView.horizontalOverflowStateForTesting.showsTrailingIndicator)
        XCTAssertTrue(scrollView.isHorizontalOverflowEdgeGlowVisibleForTesting)

        scrollView.showsHorizontalOverflowEdgeGlow = false

        XCTAssertFalse(scrollView.isHorizontalOverflowEdgeGlowVisibleForTesting)
        XCTAssertTrue(scrollView.hasHorizontalScroller)
        XCTAssertTrue(scrollView.horizontalOverflowStateForTesting.hasOverflow)
        XCTAssertNotNil(scrollView.accessibilityHelp())

        scrollView.showsHorizontalOverflowEdgeGlow = true

        XCTAssertTrue(scrollView.isHorizontalOverflowEdgeGlowVisibleForTesting)
    }

    @MainActor
    func testRepeatedScrollReflectionSkipsUnchangedOverflowWork() throws {
        let resource = ClusterResourceSummary(
            kind: .configMap,
            name: "synthetic-config",
            namespace: "synthetic-namespace",
            primaryText: "3 keys",
            secondaryText: "1 KiB"
        )
        let host = NSHostingView(rootView: genericResourceView(kind: .configMap, resource: resource)
            .frame(width: 360, height: 220))
        host.frame = NSRect(x: 0, y: 0, width: 360, height: 220)
        settle(host)

        let scrollView = try XCTUnwrap(findResourceTableScrollView(in: host))
        scrollView.layoutSubtreeIfNeeded()
        scrollView.reflectScrolledClipView(scrollView.contentView)
        let resolutionCount = scrollView.horizontalOverflowResolutionCountForTesting
        let accessibilityCount = scrollView.accessibilityOverflowUpdateCountForTesting

        for _ in 0..<1_000 {
            scrollView.reflectScrolledClipView(scrollView.contentView)
        }

        XCTAssertEqual(scrollView.horizontalOverflowResolutionCountForTesting, resolutionCount)
        XCTAssertEqual(scrollView.accessibilityOverflowUpdateCountForTesting, accessibilityCount)
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
        discardResourceColumnWidthStateForTesting(storageKeys: keys)
        defer {
            discardResourceColumnWidthStateForTesting(storageKeys: keys)
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
        discardResourceColumnWidthStateForTesting(storageKeys: Array(keys.values))
        defer {
            discardResourceColumnWidthStateForTesting(storageKeys: Array(keys.values))
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
    private func identifierBackedVisibleViews(
        in tableView: NSTableView
    ) -> (cells: Set<ObjectIdentifier>, rows: Set<ObjectIdentifier>) {
        tableView.layoutSubtreeIfNeeded()
        let visibleRows = tableView.rows(in: tableView.visibleRect)
        guard visibleRows.location != NSNotFound, visibleRows.length > 0 else {
            return ([], [])
        }

        var cells: Set<ObjectIdentifier> = []
        var rows: Set<ObjectIdentifier> = []
        let upperBound = min(tableView.numberOfRows, visibleRows.location + visibleRows.length)
        for row in visibleRows.location..<upperBound {
            if let rowView = tableView.rowView(atRow: row, makeIfNecessary: true) {
                XCTAssertEqual(rowView.identifier?.rawValue, "rune.resource-row")
                rows.insert(ObjectIdentifier(rowView))
            }
            for columnIndex in 0..<tableView.numberOfColumns {
                guard let cell = tableView.view(
                    atColumn: columnIndex,
                    row: row,
                    makeIfNecessary: true
                ) else { continue }
                let columnID = tableView.tableColumns[columnIndex].identifier.rawValue
                XCTAssertEqual(cell.identifier?.rawValue, "rune.resource-cell.\(columnID)")
                cells.insert(ObjectIdentifier(cell))
            }
        }
        return (cells, rows)
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
    var rowCount: Int

    init(rowCount: Int) {
        self.rowCount = rowCount
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        rowCount
    }
}
