import AppKit
import RuneCore
import SwiftUI

fileprivate func copyLabelSelector(_ selector: [String: String]) -> String {
    selector
        .sorted { lhs, rhs in lhs.key.localizedStandardCompare(rhs.key) == .orderedAscending }
        .map { "\($0.key)=\($0.value)" }
        .joined(separator: ",")
}

enum RuneAppKitResourceListLayout {
    static let sortIndicatorReservedWidth: CGFloat = 22
    static let sortIndicatorSize = NSSize(width: 8, height: 8)
    static let resourceMaximumContentWidth: CGFloat = 860
    static let deploymentReplicaColumnWidth: CGFloat = 88
    static let deploymentFavoriteColumnWidth: CGFloat = PodTableLayout.favoriteColumnWidth
    static let deploymentMinimumNameColumnWidth: CGFloat = 260
    static let deploymentTrailingBreathingRoom: CGFloat = 14
    static let deploymentMaximumContentWidth: CGFloat = 620
    static let serviceTypeColumnWidth: CGFloat = 140
    static let serviceClusterIPColumnWidth: CGFloat = 172
    static let serviceFavoriteColumnWidth = PodTableLayout.favoriteColumnWidth
    static let serviceMinimumNameColumnWidth: CGFloat = 260
    static let serviceTrailingBreathingRoom: CGFloat = 14
    static let serviceMaximumContentWidth: CGFloat = 740
    static let genericSelectionColumnWidth = PodTableLayout.selectionColumnWidth
    static let genericPrimaryColumnWidth: CGFloat = 136
    static let genericSecondaryColumnWidth: CGFloat = 152
    static let genericNamespaceColumnWidth: CGFloat = 124
    static let genericPrimaryMinimumColumnWidth: CGFloat = 96
    static let genericSecondaryMinimumColumnWidth: CGFloat = 72
    static let genericNamespaceMinimumColumnWidth: CGFloat = 104
    static let genericFavoriteColumnWidth = PodTableLayout.favoriteColumnWidth
    static let genericMinimumNameColumnWidth: CGFloat = 300
    static let genericTrailingBreathingRoom: CGFloat = 14
    static let genericMaximumContentWidth: CGFloat = 820
    static let helmStatusColumnWidth: CGFloat = 100
    static let helmNamespaceColumnWidth: CGFloat = 150
    static let helmRevisionColumnWidth: CGFloat = 76
    static let helmChartColumnWidth: CGFloat = 220
    static let helmAppVersionColumnWidth: CGFloat = 120
    static let helmStatusMinimumColumnWidth: CGFloat = 84
    static let helmNamespaceMinimumColumnWidth: CGFloat = 104
    static let helmRevisionMinimumColumnWidth: CGFloat = 58
    static let helmChartMinimumColumnWidth: CGFloat = 112
    static let helmAppVersionMinimumColumnWidth: CGFloat = 80
    static let helmMinimumNameColumnWidth: CGFloat = 300
    static let helmTrailingBreathingRoom: CGFloat = 14
    static let helmMaximumContentWidth: CGFloat = 980
    static let eventTypeColumnWidth: CGFloat = 108
    static let eventObjectColumnWidth: CGFloat = 210
    static let eventNamespaceColumnWidth: CGFloat = 132
    static let eventLastSeenColumnWidth: CGFloat = 166
    static let eventMessageColumnWidth: CGFloat = 260
    static let eventTypeMinimumColumnWidth: CGFloat = 84
    static let eventObjectMinimumColumnWidth: CGFloat = 130
    static let eventNamespaceMinimumColumnWidth: CGFloat = 104
    static let eventLastSeenMinimumColumnWidth: CGFloat = 120
    static let eventMessageMinimumColumnWidth: CGFloat = 160
    static let eventMinimumReasonColumnWidth: CGFloat = 180
    static let eventTrailingBreathingRoom: CGFloat = 14
    static let eventMaximumContentWidth: CGFloat = 1_080
    static let operatorFavoriteColumnWidth = PodTableLayout.favoriteColumnWidth
    static let operatorFamilyColumnWidth: CGFloat = 132
    static let operatorKindColumnWidth: CGFloat = 150
    static let operatorNamespaceColumnWidth: CGFloat = 126
    static let operatorStatusColumnWidth: CGFloat = 120
    static let operatorPrinterColumnsColumnWidth: CGFloat = 170
    static let operatorAPIPathColumnWidth: CGFloat = 260
    static let operatorFamilyMinimumColumnWidth: CGFloat = 96
    static let operatorKindMinimumColumnWidth: CGFloat = 110
    static let operatorNamespaceMinimumColumnWidth: CGFloat = 104
    static let operatorStatusMinimumColumnWidth: CGFloat = 90
    static let operatorAPIPathMinimumColumnWidth: CGFloat = 150
    static let operatorMinimumNameColumnWidth: CGFloat = 240
    static let operatorTrailingBreathingRoom: CGFloat = 14
    static let operatorMaximumContentWidth: CGFloat = 1_080

    static func minimumHeaderColumnWidth(title: String, reservesSortIndicator: Bool) -> CGFloat {
        let font = NSFont.systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold)
        let textWidth = ceil((title as NSString).size(withAttributes: [.font: font]).width)
        let trailingWidth: CGFloat = reservesSortIndicator ? sortIndicatorReservedWidth : 6
        return ceil(textWidth + 8 + trailingWidth)
    }

    static func deploymentColumnWidths(visibleWidth: CGFloat) -> (name: CGFloat, replicas: CGFloat, favorite: CGFloat) {
        let visibleWidth = min(
            deploymentMaximumContentWidth,
            max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - deploymentTrailingBreathingRoom)
        )
        let fixedWidth = deploymentReplicaColumnWidth + deploymentFavoriteColumnWidth
        return (
            name: max(deploymentMinimumNameColumnWidth, visibleWidth - fixedWidth),
            replicas: deploymentReplicaColumnWidth,
            favorite: deploymentFavoriteColumnWidth
        )
    }

    static func serviceColumnWidths(visibleWidth: CGFloat) -> (name: CGFloat, type: CGFloat, clusterIP: CGFloat, favorite: CGFloat) {
        let visibleWidth = min(
            serviceMaximumContentWidth,
            max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - serviceTrailingBreathingRoom)
        )
        let fixedWidth = serviceTypeColumnWidth + serviceClusterIPColumnWidth + serviceFavoriteColumnWidth
        return (
            name: max(serviceMinimumNameColumnWidth, visibleWidth - fixedWidth),
            type: serviceTypeColumnWidth,
            clusterIP: serviceClusterIPColumnWidth,
            favorite: serviceFavoriteColumnWidth
        )
    }

    static func genericColumnWidths(
        visibleWidth: CGFloat
    ) -> (selection: CGFloat, name: CGFloat, primary: CGFloat, secondary: CGFloat, namespace: CGFloat, favorite: CGFloat) {
        let visibleWidth = min(
            genericMaximumContentWidth,
            max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - genericTrailingBreathingRoom)
        )
        let fixedWidth = genericSelectionColumnWidth
            + genericPrimaryColumnWidth
            + genericSecondaryColumnWidth
            + genericNamespaceColumnWidth
            + genericFavoriteColumnWidth
        return (
            selection: genericSelectionColumnWidth,
            name: max(genericMinimumNameColumnWidth, visibleWidth - fixedWidth),
            primary: genericPrimaryColumnWidth,
            secondary: genericSecondaryColumnWidth,
            namespace: genericNamespaceColumnWidth,
            favorite: genericFavoriteColumnWidth
        )
    }

    static func helmColumnWidths(
        visibleWidth: CGFloat
    ) -> (name: CGFloat, status: CGFloat, namespace: CGFloat, revision: CGFloat, chart: CGFloat, appVersion: CGFloat) {
        let visibleWidth = min(
            helmMaximumContentWidth,
            max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - helmTrailingBreathingRoom)
        )
        let fixedWidth = helmStatusColumnWidth
            + helmNamespaceColumnWidth
            + helmRevisionColumnWidth
            + helmChartColumnWidth
            + helmAppVersionColumnWidth
        return (
            name: max(helmMinimumNameColumnWidth, visibleWidth - fixedWidth),
            status: helmStatusColumnWidth,
            namespace: helmNamespaceColumnWidth,
            revision: helmRevisionColumnWidth,
            chart: helmChartColumnWidth,
            appVersion: helmAppVersionColumnWidth
        )
    }

    static func eventColumnWidths(
        visibleWidth: CGFloat
    ) -> (reason: CGFloat, type: CGFloat, object: CGFloat, namespace: CGFloat, lastSeen: CGFloat, message: CGFloat) {
        let visibleWidth = min(
            eventMaximumContentWidth,
            max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - eventTrailingBreathingRoom)
        )
        let fixedWidth = eventTypeColumnWidth
            + eventObjectColumnWidth
            + eventNamespaceColumnWidth
            + eventLastSeenColumnWidth
            + eventMessageColumnWidth
        return (
            reason: max(eventMinimumReasonColumnWidth, visibleWidth - fixedWidth),
            type: eventTypeColumnWidth,
            object: eventObjectColumnWidth,
            namespace: eventNamespaceColumnWidth,
            lastSeen: eventLastSeenColumnWidth,
            message: eventMessageColumnWidth
        )
    }

    static func operatorColumnWidths(
        visibleWidth: CGFloat
    ) -> (name: CGFloat, family: CGFloat, kind: CGFloat, namespace: CGFloat, status: CGFloat, printerColumns: CGFloat, apiPath: CGFloat, favorite: CGFloat) {
        let visibleWidth = min(
            operatorMaximumContentWidth,
            max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - operatorTrailingBreathingRoom)
        )
        let fixedWidth = operatorFamilyColumnWidth
            + operatorKindColumnWidth
            + operatorNamespaceColumnWidth
            + operatorStatusColumnWidth
            + operatorPrinterColumnsColumnWidth
            + operatorAPIPathColumnWidth
            + operatorFavoriteColumnWidth
        return (
            name: max(operatorMinimumNameColumnWidth, visibleWidth - fixedWidth),
            family: operatorFamilyColumnWidth,
            kind: operatorKindColumnWidth,
            namespace: operatorNamespaceColumnWidth,
            status: operatorStatusColumnWidth,
            printerColumns: operatorPrinterColumnsColumnWidth,
            apiPath: operatorAPIPathColumnWidth,
            favorite: operatorFavoriteColumnWidth
        )
    }
}

private extension NSColor {
    static func runeTableHex(_ hex: String) -> NSColor {
        let cleaned = hex.trimmingCharacters(in: CharacterSet(charactersIn: "#"))
        let value = UInt64(cleaned, radix: 16) ?? 0
        let red: CGFloat
        let green: CGFloat
        let blue: CGFloat
        let alpha: CGFloat

        switch cleaned.count {
        case 8:
            red = CGFloat((value >> 24) & 0xff) / 255
            green = CGFloat((value >> 16) & 0xff) / 255
            blue = CGFloat((value >> 8) & 0xff) / 255
            alpha = CGFloat(value & 0xff) / 255
        default:
            red = CGFloat((value >> 16) & 0xff) / 255
            green = CGFloat((value >> 8) & 0xff) / 255
            blue = CGFloat(value & 0xff) / 255
            alpha = 1
        }

        return NSColor(srgbRed: red, green: green, blue: blue, alpha: alpha)
    }
}

@MainActor
private final class RuneAppKitColumnWidthStore {
    static let shared = RuneAppKitColumnWidthStore()

    private let defaults: UserDefaults
    private let keyPrefix = "rune.settings.layout.resourceColumnWidths."

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
    }

    func width(tableID: String, columnID: String) -> CGFloat? {
        let value = defaults.double(forKey: key(tableID: tableID, columnID: columnID))
        guard value > 0 else { return nil }
        return CGFloat(value)
    }

    func setWidth(_ width: CGFloat, tableID: String, columnID: String) {
        guard width > 0 else { return }
        defaults.set(Double(width.rounded(.toNearestOrAwayFromZero)), forKey: key(tableID: tableID, columnID: columnID))
    }

    func removeWidth(tableID: String, columnID: String) {
        defaults.removeObject(forKey: key(tableID: tableID, columnID: columnID))
    }

    private func key(tableID: String, columnID: String) -> String {
        keyPrefix + tableID + "." + columnID
    }
}

@MainActor
func applyImmediateResourceContextMenuSelection(
    row: Int,
    in tableView: NSTableView
) {
    tableView.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
    if let rowView = tableView.rowView(atRow: row, makeIfNecessary: false) {
        rowView.needsDisplay = true
        rowView.displayIfNeeded()
    } else {
        tableView.setNeedsDisplay(tableView.rect(ofRow: row))
    }
}

@MainActor
private enum RuneAppKitResourceTableStyle {
    static let headerHeight: CGFloat = 24
    static let rowHeight: CGFloat = 34
    static let rowGap: CGFloat = 4
    static let rowHorizontalInset: CGFloat = 6
    static let actionColumnTrailingPadding: CGFloat = 18
    static let contentLeadingInset: CGFloat = 10
    static let contentTrailingInset: CGFloat = 10
    static let sortIndicatorGap: CGFloat = 4

    static func apply(to tableView: NSTableView, allowsColumnResizing: Bool) {
        tableView.usesAlternatingRowBackgroundColors = false
        tableView.backgroundColor = .clear
        tableView.gridStyleMask = []
        tableView.rowSizeStyle = .medium
        tableView.rowHeight = rowHeight
        tableView.intercellSpacing = NSSize(width: 0, height: rowGap)
        tableView.allowsMultipleSelection = false
        tableView.allowsColumnResizing = allowsColumnResizing
        tableView.columnAutoresizingStyle = .noColumnAutoresizing
        tableView.selectionHighlightStyle = .none
        tableView.focusRingType = .none
        tableView.autoresizingMask = [.width]
    }

    static func apply(to headerView: RuneAppKitResourceTableHeaderView) {
        headerView.horizontalInset = rowHorizontalInset
        headerView.frame = NSRect(x: 0, y: 0, width: 0, height: headerHeight)
        headerView.autoresizingMask = [.width]
    }

    static func columnContentWidth(in tableView: NSTableView) -> CGFloat {
        tableView.tableColumns.reduce(CGFloat(0)) { $0 + $1.width }
    }

    static func renderedTableWidth(in tableView: NSTableView) -> CGFloat {
        columnContentWidth(in: tableView) + (rowHorizontalInset * 2) + actionColumnTrailingPadding
    }

    static func updateRenderedTableWidth(on tableView: NSTableView?) {
        guard let tableView else { return }
        let tableWidth = renderedTableWidth(in: tableView)
        if abs(tableView.frame.width - tableWidth) >= 1 {
            tableView.setFrameSize(NSSize(width: tableWidth, height: tableView.frame.height))
        }
        if let headerView = tableView.headerView {
            let headerSize = NSSize(width: tableWidth, height: headerHeight)
            if abs(headerView.frame.width - headerSize.width) >= 1
                || abs(headerView.frame.height - headerSize.height) >= 1 {
                headerView.setFrameSize(headerSize)
            }
        }
        tableView.headerView?.needsDisplay = true
        tableView.needsLayout = true
        tableView.needsDisplay = true
        tableView.layoutSubtreeIfNeeded()
        tableView.headerView?.needsLayout = true
        tableView.headerView?.layoutSubtreeIfNeeded()
        if let scrollView = tableView.enclosingScrollView {
            scrollView.contentView.needsDisplay = true
            scrollView.contentView.needsLayout = true
            scrollView.contentView.layoutSubtreeIfNeeded()
            scrollView.reflectScrolledClipView(scrollView.contentView)
            scrollView.tile()
        }
        let visibleRows = tableView.rows(in: tableView.visibleRect)
        guard visibleRows.location != NSNotFound else { return }
        let upperBound = min(tableView.numberOfRows, visibleRows.location + visibleRows.length)
        guard visibleRows.location < upperBound else { return }
        for row in visibleRows.location..<upperBound {
            if let rowView = tableView.rowView(atRow: row, makeIfNecessary: false) {
                rowView.needsLayout = true
                rowView.needsDisplay = true
                rowView.layoutSubtreeIfNeeded()
            }
            for column in 0..<tableView.numberOfColumns {
                guard let cellView = tableView.view(atColumn: column, row: row, makeIfNecessary: false) else { continue }
                cellView.needsLayout = true
                cellView.needsDisplay = true
                cellView.layoutSubtreeIfNeeded()
            }
        }
        tableView.displayIfNeeded()
        tableView.headerView?.displayIfNeeded()
    }

    static func invalidateTheme(in scrollView: NSScrollView) {
        guard let tableView = scrollView.documentView as? NSTableView else { return }
        tableView.headerView?.needsDisplay = true
        tableView.needsDisplay = true
        for row in 0..<tableView.numberOfRows {
            tableView.rowView(atRow: row, makeIfNecessary: false)?.needsDisplay = true
        }
    }
}

private struct RuneAppKitResourceTableTheme {
    let headerFill: NSColor
    let headerText: NSColor
    let headerDivider: NSColor
    let columnDivider: NSColor
    let columnDividerResizable: NSColor
    let rowFill: NSColor
    let selectedRowFill: NSColor
    let rowStroke: NSColor

    static var current: RuneAppKitResourceTableTheme {
        let theme = RuneAppearanceTheme.resolved(UserDefaults.standard.string(forKey: RuneSettingsKeys.appearanceTheme) ?? RuneSettingsKeys.appearanceThemeDefault)
        if theme.isNative {
            return RuneAppKitResourceTableTheme(
                headerFill: NSColor.controlBackgroundColor.withAlphaComponent(0.42),
                headerText: .headerTextColor,
                headerDivider: NSColor.separatorColor.withAlphaComponent(0.24),
                columnDivider: NSColor.gridColor.withAlphaComponent(0.28),
                columnDividerResizable: NSColor.gridColor.withAlphaComponent(0.48),
                rowFill: NSColor.controlBackgroundColor.withAlphaComponent(0.42),
                selectedRowFill: NSColor.controlAccentColor.withAlphaComponent(0.11),
                rowStroke: NSColor.separatorColor.withAlphaComponent(0.20)
            )
        }
        guard let appKit = theme.appKitPalette else {
            return themed(text: "#1f2933", stroke: "#d4d2c9", row: "#f1f0ea", selected: "#3d70b2", selectedAlpha: 0.14)
        }
        return themed(
            text: appKit.foreground,
            stroke: appKit.stroke,
            row: appKit.row,
            selected: appKit.accent,
            selectedAlpha: appKit.selectedAlpha
        )
    }

    private static func themed(
        text: String,
        stroke: String,
        row: String,
        selected: String,
        selectedAlpha: CGFloat = 0.18
    ) -> RuneAppKitResourceTableTheme {
        let strokeColor = NSColor.runeTableHex(stroke)
        return RuneAppKitResourceTableTheme(
            headerFill: NSColor.runeTableHex(row).withAlphaComponent(0.62),
            headerText: NSColor.runeTableHex(text).withAlphaComponent(0.92),
            headerDivider: strokeColor.withAlphaComponent(0.40),
            columnDivider: strokeColor.withAlphaComponent(0.32),
            columnDividerResizable: strokeColor.withAlphaComponent(0.56),
            rowFill: NSColor.runeTableHex(row).withAlphaComponent(0.62),
            selectedRowFill: NSColor.runeTableHex(selected).withAlphaComponent(selectedAlpha),
            rowStroke: strokeColor.withAlphaComponent(0.30)
        )
    }
}

struct AppKitPodTableView: NSViewRepresentable {
    let pods: [PodSummary]
    let selectedPodID: String?
    let selectedPodIDs: Set<String>
    let sortColumn: PodListSortColumn
    let sortAscending: Bool
    let nameColumnWidth: CGFloat
    let canApplyClusterMutations: Bool
    let isFavorite: (PodSummary) -> Bool
    let onSelectPod: (PodSummary) -> Void
    let onToggleBulkSelection: (PodSummary) -> Void
    let onToggleSort: (PodListSortColumn) -> Void
    let onNameColumnWidthChanged: (CGFloat) -> Void
    let onToggleFavorite: (PodSummary) -> Void
    let onOpenLogs: (PodSummary) -> Void
    let onOpenExec: (PodSummary) -> Void
    let onOpenDescribe: (PodSummary) -> Void
    let onOpenYAML: (PodSummary) -> Void
    let onDelete: (PodSummary) -> Void
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let tableView = PodNSTableView()
        tableView.coordinator = context.coordinator
        tableView.delegate = context.coordinator
        tableView.dataSource = context.coordinator
        RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: true)
        let headerView = RuneAppKitResourceTableHeaderView()
        RuneAppKitResourceTableStyle.apply(to: headerView)
        headerView.resizableColumnIdentifiers = Set(PodColumn.allCases.filter(\.isUserResizable).map(\.rawValue))
        headerView.onResetColumn = { [weak coordinator = context.coordinator] columnID in
            coordinator?.resetColumnWidth(columnID)
        }
        tableView.headerView = headerView

        for column in PodColumn.allCases {
            tableView.addTableColumn(column.tableColumn(nameColumnWidth: nameColumnWidth))
        }

        let scrollView = RuneAppKitResourceListScrollView()
        scrollView.onVisibleWidthChanged = { [weak coordinator = context.coordinator] in
            coordinator?.updateTableGeometry()
        }
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.documentView = tableView
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? PodNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
    }

    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        var parent: AppKitPodTableView
        weak var tableView: NSTableView?
        private var isApplyingSelection = false
        private var nameColumnPersistWorkItem: DispatchWorkItem?
        private var applyGeneration = 0
        private let tableID = "pods"

        init(_ parent: AppKitPodTableView) {
            self.parent = parent
        }

        func apply(parent: AppKitPodTableView) {
            guard let tableView else { return }
            applyGeneration += 1
            let generation = applyGeneration
            DispatchQueue.main.async { [weak self, weak tableView] in
                guard let self,
                      let tableView,
                      generation == self.applyGeneration else { return }
                self.updateNameColumnWidthIfNeeded(on: tableView, width: self.parent.nameColumnWidth)
                self.updateStoredColumnWidthsIfNeeded(on: tableView)
                self.updateTableWidth(on: tableView)
                tableView.reloadData()
                self.applySelection(on: tableView)
                self.updateSortIndicator(on: tableView)
            }
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            parent.pods.count
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard row >= 0, row < parent.pods.count, let tableColumn else { return nil }
            let pod = parent.pods[row]
            guard let column = PodColumn(rawValue: tableColumn.identifier.rawValue) else { return nil }

            switch column {
            case .selection:
                let container = NSView()
                let checkbox = NSButton(checkboxWithTitle: "", target: self, action: #selector(toggleBulkSelection(_:)))
                checkbox.tag = row
                checkbox.state = parent.selectedPodIDs.contains(pod.id) ? .on : .off
                checkbox.toolTip = parent.selectedPodIDs.contains(pod.id) ? "Remove from bulk selection" : "Add to bulk selection"
                checkbox.controlSize = .small
                checkbox.setAccessibilityLabel("Select \(pod.name)")
                checkbox.translatesAutoresizingMaskIntoConstraints = false
                container.addSubview(checkbox)
                NSLayoutConstraint.activate([
                    checkbox.centerXAnchor.constraint(equalTo: container.centerXAnchor),
                    checkbox.centerYAnchor.constraint(equalTo: container.centerYAnchor)
                ])
                return container

            case .name:
                return label(
                    pod.name,
                    font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium),
                    alignment: .left,
                    tooltip: pod.name,
                    lineBreakMode: .byTruncatingMiddle
                )

            case .cpu:
                return label(pod.cpuDisplay, font: metricsFont, alignment: .right)

            case .memory:
                return label(pod.memoryDisplay, font: metricsFont, alignment: .right)

            case .restarts:
                return label("\(pod.totalRestarts)", font: metricsFont, alignment: .right)

            case .age:
                return label(pod.ageDescription, font: metricsFont, alignment: .right)

            case .status:
                return statusCell(for: pod)

            case .favorite:
                return favoriteCell(isFavorite: parent.isFavorite(pod), row: row)
            }
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard !isApplyingSelection,
                  let tableView = notification.object as? NSTableView,
                  tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.pods.count else { return }
            parent.onSelectPod(parent.pods[tableView.selectedRow])
        }

        func tableView(_ tableView: NSTableView, didClick tableColumn: NSTableColumn) {
            guard let column = PodColumn(rawValue: tableColumn.identifier.rawValue),
                  let sortColumn = column.sortColumn else { return }
            parent.onToggleSort(sortColumn)
        }

        func selectRowForContextMenu(_ row: Int, in tableView: NSTableView) {
            guard row >= 0, row < parent.pods.count else { return }
            let pod = parent.pods[row]
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyImmediateResourceContextMenuSelection(row: row, in: tableView)
            guard pod.id != parent.selectedPodID else { return }
            DispatchQueue.main.async { [weak self] in
                guard let self,
                      let current = self.parent.pods.first(where: { $0.id == pod.id })
                else { return }
                self.parent.onSelectPod(current)
            }
        }

        func tableViewColumnDidResize(_ notification: Notification) {
            guard let tableColumn = notification.userInfo?["NSTableColumn"] as? NSTableColumn,
                  let column = PodColumn(rawValue: tableColumn.identifier.rawValue),
                  column.isUserResizable else { return }
            let width = min(max(tableColumn.width, tableColumn.minWidth), tableColumn.maxWidth)
            tableColumn.width = width
            RuneAppKitColumnWidthStore.shared.setWidth(width, tableID: tableID, columnID: column.rawValue)
            updateTableWidth(on: tableView)

            guard column == .name else { return }

            nameColumnPersistWorkItem?.cancel()
            let workItem = DispatchWorkItem { [weak self] in
                self?.parent.onNameColumnWidthChanged(width)
            }
            nameColumnPersistWorkItem = workItem
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18, execute: workItem)
        }

        @objc private func toggleBulkSelection(_ sender: NSButton) {
            guard sender.tag >= 0, sender.tag < parent.pods.count else { return }
            parent.onToggleBulkSelection(parent.pods[sender.tag])
        }

        func makeMenu(forRow row: Int) -> NSMenu? {
            guard row >= 0, row < parent.pods.count else { return nil }
            let pod = parent.pods[row]
            let menu = NSMenu()
            menu.addItem(menuItem(parent.isFavorite(pod) ? "Remove Favorite" : "Favorite Resource", action: #selector(toggleFavoriteFromMenu(_:)), pod: pod))
            menu.addItem(.separator())
            menu.addItem(menuItem("Open Logs", action: #selector(openLogsFromMenu(_:)), pod: pod))
            menu.addItem(menuItem("Exec Shell", action: #selector(openExecFromMenu(_:)), pod: pod))
            menu.addItem(menuItem("Describe", action: #selector(openDescribeFromMenu(_:)), pod: pod))
            menu.addItem(menuItem("Open YAML", action: #selector(openYAMLFromMenu(_:)), pod: pod))
            menu.addItem(.separator())
            menu.addItem(menuItem("Copy pod name", action: #selector(copyPodNameFromMenu(_:)), pod: pod))
            menu.addItem(menuItem("Copy namespace", action: #selector(copyNamespaceFromMenu(_:)), pod: pod))
            if pod.containerNamesLine != nil {
                menu.addItem(menuItem("Copy container names", action: #selector(copyContainerNamesFromMenu(_:)), pod: pod))
            }
            if pod.containerImagesLine != nil {
                menu.addItem(menuItem("Copy images", action: #selector(copyImagesFromMenu(_:)), pod: pod))
            }
            if !pod.labels.isEmpty {
                menu.addItem(menuItem("Copy labels", action: #selector(copyLabelsFromMenu(_:)), pod: pod))
            }
            if pod.ownerReferencesLine != nil {
                menu.addItem(menuItem("Copy owner reference", action: #selector(copyOwnerReferenceFromMenu(_:)), pod: pod))
            }
            if pod.nodeName != nil {
                menu.addItem(menuItem("Copy node name", action: #selector(copyNodeNameFromMenu(_:)), pod: pod))
            }
            if pod.podIP != nil {
                menu.addItem(menuItem("Copy pod IP", action: #selector(copyPodIPFromMenu(_:)), pod: pod))
            }
            menu.addItem(.separator())
            menu.addItem(menuItem("Delete Pod", action: #selector(deleteFromMenu(_:)), pod: pod, isEnabled: parent.canApplyClusterMutations))
            return menu
        }

        @objc private func toggleFavoriteFromMenu(_ sender: NSMenuItem) {
            withPod(sender, parent.onToggleFavorite)
        }

        func toggleFavoriteForSelectedRow(in tableView: NSTableView) {
            guard tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.pods.count else { return }
            parent.onToggleFavorite(parent.pods[tableView.selectedRow])
        }

        @objc private func openLogsFromMenu(_ sender: NSMenuItem) {
            withPod(sender, parent.onOpenLogs)
        }

        @objc private func openExecFromMenu(_ sender: NSMenuItem) {
            withPod(sender, parent.onOpenExec)
        }

        @objc private func openDescribeFromMenu(_ sender: NSMenuItem) {
            withPod(sender, parent.onOpenDescribe)
        }

        @objc private func openYAMLFromMenu(_ sender: NSMenuItem) {
            withPod(sender, parent.onOpenYAML)
        }

        @objc private func deleteFromMenu(_ sender: NSMenuItem) {
            withPod(sender, parent.onDelete)
        }

        @objc private func copyPodNameFromMenu(_ sender: NSMenuItem) {
            withPod(sender) { pod in copyToClipboard(pod.name) }
        }

        @objc private func copyNamespaceFromMenu(_ sender: NSMenuItem) {
            withPod(sender) { pod in copyToClipboard(pod.namespace) }
        }

        @objc private func copyContainerNamesFromMenu(_ sender: NSMenuItem) {
            withPod(sender) { pod in
                if let containerNamesLine = pod.containerNamesLine {
                    copyToClipboard(containerNamesLine)
                }
            }
        }

        @objc private func copyImagesFromMenu(_ sender: NSMenuItem) {
            withPod(sender) { pod in
                if let containerImagesLine = pod.containerImagesLine {
                    copyToClipboard(containerImagesLine)
                }
            }
        }

        @objc private func copyLabelsFromMenu(_ sender: NSMenuItem) {
            withPod(sender) { pod in
                copyToClipboard(copyLabelSelector(pod.labels))
            }
        }

        @objc private func copyOwnerReferenceFromMenu(_ sender: NSMenuItem) {
            withPod(sender) { pod in
                if let ownerReferencesLine = pod.ownerReferencesLine {
                    copyToClipboard(ownerReferencesLine)
                }
            }
        }

        @objc private func copyNodeNameFromMenu(_ sender: NSMenuItem) {
            withPod(sender) { pod in
                if let nodeName = pod.nodeName {
                    copyToClipboard(nodeName)
                }
            }
        }

        @objc private func copyPodIPFromMenu(_ sender: NSMenuItem) {
            withPod(sender) { pod in
                if let podIP = pod.podIP {
                    copyToClipboard(podIP)
                }
            }
        }

        private func updateNameColumnWidthIfNeeded(on tableView: NSTableView, width: CGFloat) {
            guard let column = tableView.tableColumns.first(where: { $0.identifier.rawValue == PodColumn.name.rawValue }) else { return }
            let clamped = PodTableLayout.clampedNameColumnWidth(
                RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: PodColumn.name.rawValue) ?? width
            )
            column.minWidth = PodTableLayout.nameColumnMinimumWidth
            column.maxWidth = PodTableLayout.nameColumnMaximumWidth
            if abs(column.width - clamped) >= 1 {
                column.width = clamped
            }
        }

        func resetColumnWidth(_ columnID: String) {
            guard let columnKind = PodColumn(rawValue: columnID) else { return }
            RuneAppKitColumnWidthStore.shared.removeWidth(tableID: tableID, columnID: columnID)
            guard columnKind == .name else {
                guard let tableView,
                      let tableColumn = tableView.tableColumns.first(where: { $0.identifier.rawValue == columnID }) else { return }
                let width = columnKind.width(nameColumnWidth: PodTableLayout.nameColumnDefaultWidth)
                tableColumn.width = width
                updateTableWidth(on: tableView)
                return
            }

            nameColumnPersistWorkItem?.cancel()
            guard let tableView,
                  let column = tableView.tableColumns.first(where: { $0.identifier.rawValue == PodColumn.name.rawValue }) else {
                parent.onNameColumnWidthChanged(PodTableLayout.nameColumnDefaultWidth)
                return
            }
            let width = PodTableLayout.nameColumnDefaultWidth
            column.width = width
            updateTableWidth(on: tableView)
            parent.onNameColumnWidthChanged(width)
        }

        private func updateStoredColumnWidthsIfNeeded(on tableView: NSTableView) {
            for tableColumn in tableView.tableColumns {
                guard tableColumn.identifier.rawValue != PodColumn.name.rawValue,
                      let column = PodColumn(rawValue: tableColumn.identifier.rawValue),
                      column.isUserResizable,
                      let width = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: column.rawValue) else { continue }
                let clamped = min(max(width, tableColumn.minWidth), tableColumn.maxWidth)
                if abs(tableColumn.width - clamped) >= 1 {
                    tableColumn.width = clamped
                }
            }
        }

        private func updateTableWidth(on tableView: NSTableView?) {
            RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)
        }

        func updateTableGeometry() {
            updateTableWidth(on: tableView)
        }

        private func applySelection(on tableView: NSTableView) {
            let selectedRow = parent.pods.firstIndex { $0.id == parent.selectedPodID }
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            if let selectedRow {
                tableView.selectRowIndexes(IndexSet(integer: selectedRow), byExtendingSelection: false)
            } else {
                tableView.deselectAll(nil)
            }
        }

        private func updateSortIndicator(on tableView: NSTableView) {
            for tableColumn in tableView.tableColumns {
                guard let column = PodColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                if let headerCell = tableColumn.headerCell as? RuneAppKitResourceTableHeaderCell {
                    headerCell.configure(
                        title: column.title,
                        isSorted: column.sortColumn == parent.sortColumn,
                        ascending: parent.sortAscending,
                        reservesSortIndicator: column.sortColumn != nil
                    )
                }
                if column.sortColumn == parent.sortColumn {
                    let imageName = NSImage.Name(parent.sortAscending ? "NSAscendingSortIndicator" : "NSDescendingSortIndicator")
                    tableView.setIndicatorImage(NSImage(named: imageName), in: tableColumn)
                } else {
                    tableView.setIndicatorImage(nil, in: tableColumn)
                }
            }
            tableView.headerView?.needsDisplay = true
        }

        private var metricsFont: NSFont {
            .monospacedSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular)
        }

        private func label(
            _ text: String,
            font: NSFont,
            alignment: NSTextAlignment,
            textColor: NSColor = .labelColor,
            tooltip: String? = nil,
            lineBreakMode: NSLineBreakMode = .byTruncatingTail
        ) -> NSView {
            let container = NSView()
            let label = NSTextField(labelWithString: text)
            label.font = font
            label.alignment = alignment
            label.textColor = textColor
            label.lineBreakMode = lineBreakMode
            label.maximumNumberOfLines = 1
            label.toolTip = tooltip
            label.translatesAutoresizingMaskIntoConstraints = false
            container.addSubview(label)
            NSLayoutConstraint.activate([
                label.leadingAnchor.constraint(equalTo: container.leadingAnchor, constant: alignment == .left ? RuneAppKitResourceTableStyle.contentLeadingInset : 0),
                label.trailingAnchor.constraint(equalTo: container.trailingAnchor, constant: alignment == .right ? -RuneAppKitResourceTableStyle.contentTrailingInset : 0),
                label.centerYAnchor.constraint(equalTo: container.centerYAnchor)
            ])
            return container
        }

        private func statusCell(for pod: PodSummary) -> NSView {
            let container = NSView()

            let pill = RuneAppKitResourceStatusPillView(text: pod.status, color: statusColor(for: pod.status))
            pill.toolTip = "Pod phase from the cluster"
            pill.translatesAutoresizingMaskIntoConstraints = false
            container.addSubview(pill)

            NSLayoutConstraint.activate([
                pill.centerXAnchor.constraint(equalTo: container.centerXAnchor),
                pill.centerYAnchor.constraint(equalTo: container.centerYAnchor)
            ])
            return container
        }

        private func favoriteCell(isFavorite: Bool, row: Int) -> NSView {
            let container = NSView()
            let button = NSButton(
                image: NSImage(systemSymbolName: isFavorite ? "star.fill" : "star", accessibilityDescription: "Favorite Resource") ?? NSImage(),
                target: self,
                action: #selector(toggleFavoriteButton(_:))
            )
            button.tag = row
            button.isBordered = false
            button.imagePosition = .imageOnly
            button.contentTintColor = isFavorite ? .systemYellow : .secondaryLabelColor
            button.symbolConfiguration = .init(pointSize: NSFont.smallSystemFontSize, weight: .semibold)
            button.toolTip = isFavorite ? "Remove favorite" : "Favorite resource"
            button.setAccessibilityLabel(isFavorite ? "Remove Favorite" : "Favorite Resource")
            button.translatesAutoresizingMaskIntoConstraints = false
            container.addSubview(button)
            NSLayoutConstraint.activate([
                button.centerXAnchor.constraint(equalTo: container.centerXAnchor),
                button.centerYAnchor.constraint(equalTo: container.centerYAnchor),
                button.widthAnchor.constraint(equalToConstant: 22),
                button.heightAnchor.constraint(equalToConstant: 22)
            ])
            return container
        }

        @objc private func toggleFavoriteButton(_ sender: NSButton) {
            guard sender.tag >= 0, sender.tag < parent.pods.count else { return }
            parent.onToggleFavorite(parent.pods[sender.tag])
        }

        private func statusColor(for status: String) -> NSColor {
            switch status.lowercased() {
            case "running", "succeeded", "ready":
                return .systemGreen
            case "pending", "terminating":
                return .systemOrange
            case "failed", "error", "crashloopbackoff":
                return .systemRed
            default:
                return .secondaryLabelColor
            }
        }

        private func menuItem(_ title: String, action: Selector, pod: PodSummary, isEnabled: Bool = true) -> NSMenuItem {
            let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
            item.target = self
            item.representedObject = pod
            item.isEnabled = isEnabled
            return item
        }

        private func withPod(_ sender: NSMenuItem, _ action: (PodSummary) -> Void) {
            guard let pod = sender.representedObject as? PodSummary else { return }
            action(pod)
        }

        private func copyToClipboard(_ value: String) {
            let pasteboard = NSPasteboard.general
            pasteboard.clearContents()
            pasteboard.setString(value, forType: .string)
        }
    }
}

struct AppKitDeploymentListView: NSViewRepresentable {
    let deployments: [DeploymentSummary]
    let selectedDeploymentID: String?
    let sortColumn: DeploymentListSortColumn
    let sortAscending: Bool
    let canApplyClusterMutations: Bool
    let isFavorite: (DeploymentSummary) -> Bool
    let onSelectDeployment: (DeploymentSummary) -> Void
    let onToggleSort: (DeploymentListSortColumn) -> Void
    let onToggleFavorite: (DeploymentSummary) -> Void
    let onOpenUnifiedLogs: (DeploymentSummary) -> Void
    let onOpenRollout: (DeploymentSummary) -> Void
    let onOpenDescribe: (DeploymentSummary) -> Void
    let onOpenYAML: (DeploymentSummary) -> Void
    let onDelete: (DeploymentSummary) -> Void
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let tableView = DeploymentNSTableView()
        tableView.coordinator = context.coordinator
        tableView.delegate = context.coordinator
        tableView.dataSource = context.coordinator
        RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: true)
        let headerView = RuneAppKitResourceTableHeaderView()
        RuneAppKitResourceTableStyle.apply(to: headerView)
        headerView.resizableColumnIdentifiers = Set(DeploymentColumn.allCases.filter(\.isUserResizable).map(\.rawValue))
        headerView.onResetColumn = { [weak coordinator = context.coordinator] columnID in
            coordinator?.resetColumnWidth(columnID)
        }
        tableView.headerView = headerView

        for column in DeploymentColumn.allCases {
            tableView.addTableColumn(column.tableColumn())
        }

        let scrollView = RuneAppKitResourceListScrollView()
        scrollView.onVisibleWidthChanged = { [weak coordinator = context.coordinator] in
            coordinator?.updateColumnWidths()
        }
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.documentView = tableView
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? DeploymentNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
    }

    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        var parent: AppKitDeploymentListView
        weak var tableView: NSTableView?
        private var isApplyingSelection = false
        private var applyGeneration = 0
        private let tableID = "deployments"

        init(_ parent: AppKitDeploymentListView) {
            self.parent = parent
        }

        func apply(parent: AppKitDeploymentListView) {
            guard let tableView else { return }
            applyGeneration += 1
            let generation = applyGeneration
            DispatchQueue.main.async { [weak self, weak tableView] in
                guard let self,
                      let tableView,
                      generation == self.applyGeneration else { return }
                self.updateColumnWidths(on: tableView)
                tableView.reloadData()
                self.applySelection(on: tableView)
                self.updateDeploymentSortIndicator(on: tableView)
            }
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            parent.deployments.count
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard row >= 0, row < parent.deployments.count,
                  let tableColumn,
                  let column = DeploymentColumn(rawValue: tableColumn.identifier.rawValue) else { return nil }
            let deployment = parent.deployments[row]
            switch column {
            case .name:
                return RuneAppKitCenteredLabelCell(
                    text: deployment.name,
                    font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium),
                    alignment: .left,
                    tooltip: deployment.name,
                    lineBreakMode: .byTruncatingMiddle
                )
            case .replicas:
                return RuneAppKitPillCell(text: deployment.replicaText, color: .systemBlue)
            case .favorite:
                return RuneAppKitFavoriteButtonCell(
                    isFavorite: parent.isFavorite(deployment),
                    row: row,
                    target: self,
                    action: #selector(toggleFavoriteButton(_:))
                )
            }
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard !isApplyingSelection,
                  let tableView = notification.object as? NSTableView,
                  tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.deployments.count else { return }
            parent.onSelectDeployment(parent.deployments[tableView.selectedRow])
        }

        func tableView(_ tableView: NSTableView, didClick tableColumn: NSTableColumn) {
            guard let column = DeploymentColumn(rawValue: tableColumn.identifier.rawValue),
                  let sortColumn = column.sortColumn else { return }
            parent.onToggleSort(sortColumn)
        }

        func selectRowForContextMenu(_ row: Int, in tableView: NSTableView) {
            guard row >= 0, row < parent.deployments.count else { return }
            let deployment = parent.deployments[row]
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyImmediateResourceContextMenuSelection(row: row, in: tableView)
            guard deployment.id != parent.selectedDeploymentID else { return }
            DispatchQueue.main.async { [weak self] in
                guard let self,
                      let current = self.parent.deployments.first(where: { $0.id == deployment.id })
                else { return }
                self.parent.onSelectDeployment(current)
            }
        }

        func tableViewColumnDidResize(_ notification: Notification) {
            guard let tableColumn = notification.userInfo?["NSTableColumn"] as? NSTableColumn,
                  let column = DeploymentColumn(rawValue: tableColumn.identifier.rawValue),
                  column.isUserResizable else { return }
            let width = min(max(tableColumn.width, tableColumn.minWidth), tableColumn.maxWidth)
            tableColumn.width = width
            RuneAppKitColumnWidthStore.shared.setWidth(width, tableID: tableID, columnID: column.rawValue)
            updateTableWidth(on: tableView)
        }

        func makeMenu(forRow row: Int) -> NSMenu? {
            guard row >= 0, row < parent.deployments.count else { return nil }
            let deployment = parent.deployments[row]
            let menu = NSMenu()
            menu.addItem(menuItem(parent.isFavorite(deployment) ? "Remove Favorite" : "Favorite Resource", action: #selector(toggleFavoriteFromMenu(_:)), deployment: deployment))
            menu.addItem(.separator())
            menu.addItem(menuItem("Open Unified Logs", action: #selector(openUnifiedLogsFromMenu(_:)), deployment: deployment))
            menu.addItem(menuItem("Open Rollout", action: #selector(openRolloutFromMenu(_:)), deployment: deployment))
            menu.addItem(menuItem("Describe", action: #selector(openDescribeFromMenu(_:)), deployment: deployment))
            menu.addItem(menuItem("Open YAML", action: #selector(openYAMLFromMenu(_:)), deployment: deployment))
            menu.addItem(.separator())
            menu.addItem(menuItem("Copy deployment name", action: #selector(copyDeploymentNameFromMenu(_:)), deployment: deployment))
            menu.addItem(menuItem("Copy namespace", action: #selector(copyNamespaceFromMenu(_:)), deployment: deployment))
            if deployment.selector?.isEmpty == false {
                menu.addItem(menuItem("Copy selector", action: #selector(copySelectorFromMenu(_:)), deployment: deployment))
            }
            menu.addItem(.separator())
            menu.addItem(menuItem("Delete Deployment", action: #selector(deleteFromMenu(_:)), deployment: deployment, isEnabled: parent.canApplyClusterMutations))
            return menu
        }

        @objc private func toggleFavoriteFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender, parent.onToggleFavorite)
        }

        func toggleFavoriteForSelectedRow(in tableView: NSTableView) {
            guard tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.deployments.count else { return }
            parent.onToggleFavorite(parent.deployments[tableView.selectedRow])
        }

        @objc private func toggleFavoriteButton(_ sender: NSButton) {
            guard sender.tag >= 0, sender.tag < parent.deployments.count else { return }
            parent.onToggleFavorite(parent.deployments[sender.tag])
        }

        @objc private func openUnifiedLogsFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender, parent.onOpenUnifiedLogs)
        }

        @objc private func openRolloutFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender, parent.onOpenRollout)
        }

        @objc private func openDescribeFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender, parent.onOpenDescribe)
        }

        @objc private func openYAMLFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender, parent.onOpenYAML)
        }

        @objc private func deleteFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender, parent.onDelete)
        }

        @objc private func copyDeploymentNameFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender) { deployment in copyToClipboard(deployment.name) }
        }

        @objc private func copyNamespaceFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender) { deployment in copyToClipboard(deployment.namespace) }
        }

        @objc private func copySelectorFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender) { deployment in
                if let selector = deployment.selector, !selector.isEmpty {
                    copyToClipboard(copyLabelSelector(selector))
                }
            }
        }

        private func applySelection(on tableView: NSTableView) {
            let selectedRow = parent.deployments.firstIndex { $0.id == parent.selectedDeploymentID }
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            if let selectedRow {
                tableView.selectRowIndexes(IndexSet(integer: selectedRow), byExtendingSelection: false)
            } else {
                tableView.deselectAll(nil)
            }
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = min(
                RuneAppKitResourceListLayout.deploymentMaximumContentWidth,
                max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.deploymentTrailingBreathingRoom)
            )
            let storedReplicas = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: DeploymentColumn.replicas.rawValue)
            let replicasWidth = min(max(storedReplicas ?? widths.replicas, DeploymentColumn.replicas.minimumWidth), DeploymentColumn.replicas.maximumWidth)
            let dynamicNameWidth = max(
                RuneAppKitResourceListLayout.deploymentMinimumNameColumnWidth,
                projectedWidth - replicasWidth - widths.favorite
            )
            let storedName = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: DeploymentColumn.name.rawValue)
            let nameWidth = min(max(storedName ?? dynamicNameWidth, DeploymentColumn.name.minimumWidth), DeploymentColumn.name.maximumWidth)
            for tableColumn in tableView.tableColumns {
                guard let column = DeploymentColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                switch column {
                case .name: tableColumn.width = nameWidth
                case .replicas: tableColumn.width = replicasWidth
                case .favorite: tableColumn.width = widths.favorite
                }
            }
            updateTableWidth(on: tableView)
        }

        func resetColumnWidth(_ columnID: String) {
            RuneAppKitColumnWidthStore.shared.removeWidth(tableID: tableID, columnID: columnID)
            updateColumnWidths()
        }

        private func updateTableWidth(on tableView: NSTableView?) {
            RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)
        }

        private func updateDeploymentSortIndicator(on tableView: NSTableView) {
            for tableColumn in tableView.tableColumns {
                guard let column = DeploymentColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                if let headerCell = tableColumn.headerCell as? RuneAppKitResourceTableHeaderCell {
                    headerCell.configure(
                        title: column.title,
                        isSorted: column.sortColumn == parent.sortColumn,
                        ascending: parent.sortAscending,
                        reservesSortIndicator: column.sortColumn != nil
                    )
                }
                if column.sortColumn == parent.sortColumn {
                    let imageName = NSImage.Name(parent.sortAscending ? "NSAscendingSortIndicator" : "NSDescendingSortIndicator")
                    tableView.setIndicatorImage(NSImage(named: imageName), in: tableColumn)
                } else {
                    tableView.setIndicatorImage(nil, in: tableColumn)
                }
            }
            tableView.headerView?.needsDisplay = true
        }

        private func menuItem(_ title: String, action: Selector, deployment: DeploymentSummary, isEnabled: Bool = true) -> NSMenuItem {
            let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
            item.target = self
            item.representedObject = deployment
            item.isEnabled = isEnabled
            return item
        }

        private func withDeployment(_ sender: NSMenuItem, _ action: (DeploymentSummary) -> Void) {
            guard let deployment = sender.representedObject as? DeploymentSummary else { return }
            action(deployment)
        }

        private func copyToClipboard(_ value: String) {
            let pasteboard = NSPasteboard.general
            pasteboard.clearContents()
            pasteboard.setString(value, forType: .string)
        }
    }
}

struct AppKitServiceListView: NSViewRepresentable {
    let services: [ServiceSummary]
    let selectedServiceID: String?
    let sortColumn: ServiceListSortColumn
    let sortAscending: Bool
    let canApplyClusterMutations: Bool
    let isFavorite: (ServiceSummary) -> Bool
    let onSelectService: (ServiceSummary) -> Void
    let onToggleSort: (ServiceListSortColumn) -> Void
    let onToggleFavorite: (ServiceSummary) -> Void
    let onOpenUnifiedLogs: (ServiceSummary) -> Void
    let onOpenPortForward: (ServiceSummary) -> Void
    let onOpenDescribe: (ServiceSummary) -> Void
    let onOpenYAML: (ServiceSummary) -> Void
    let onDelete: (ServiceSummary) -> Void
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let tableView = ServiceNSTableView()
        tableView.coordinator = context.coordinator
        tableView.delegate = context.coordinator
        tableView.dataSource = context.coordinator
        RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: true)
        let headerView = RuneAppKitResourceTableHeaderView()
        RuneAppKitResourceTableStyle.apply(to: headerView)
        headerView.resizableColumnIdentifiers = Set(ServiceColumn.allCases.filter(\.isUserResizable).map(\.rawValue))
        headerView.onResetColumn = { [weak coordinator = context.coordinator] columnID in
            coordinator?.resetColumnWidth(columnID)
        }
        tableView.headerView = headerView

        for column in ServiceColumn.allCases {
            tableView.addTableColumn(column.tableColumn())
        }

        let scrollView = RuneAppKitResourceListScrollView()
        scrollView.onVisibleWidthChanged = { [weak coordinator = context.coordinator] in
            coordinator?.updateColumnWidths()
        }
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.documentView = tableView
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? ServiceNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
    }

    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        var parent: AppKitServiceListView
        weak var tableView: NSTableView?
        private var isApplyingSelection = false
        private var applyGeneration = 0
        private let tableID = "services"

        init(_ parent: AppKitServiceListView) {
            self.parent = parent
        }

        func apply(parent: AppKitServiceListView) {
            guard let tableView else { return }
            applyGeneration += 1
            let generation = applyGeneration
            DispatchQueue.main.async { [weak self, weak tableView] in
                guard let self,
                      let tableView,
                      generation == self.applyGeneration else { return }
                self.updateColumnWidths(on: tableView)
                tableView.reloadData()
                self.applySelection(on: tableView)
                self.updateServiceSortIndicator(on: tableView)
            }
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            parent.services.count
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard row >= 0, row < parent.services.count,
                  let tableColumn,
                  let column = ServiceColumn(rawValue: tableColumn.identifier.rawValue) else { return nil }
            let service = parent.services[row]
            switch column {
            case .name:
                return RuneAppKitCenteredLabelCell(
                    text: service.name,
                    font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium),
                    alignment: .left,
                    tooltip: service.name,
                    lineBreakMode: .byTruncatingMiddle
                )
            case .type:
                return RuneAppKitPillCell(text: service.type, color: .systemPurple)
            case .clusterIP:
                return RuneAppKitCenteredLabelCell(text: service.clusterIP, font: .monospacedSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular), alignment: .right, tooltip: service.clusterIP)
            case .favorite:
                return RuneAppKitFavoriteButtonCell(
                    isFavorite: parent.isFavorite(service),
                    row: row,
                    target: self,
                    action: #selector(toggleFavoriteButton(_:))
                )
            }
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard !isApplyingSelection,
                  let tableView = notification.object as? NSTableView,
                  tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.services.count else { return }
            parent.onSelectService(parent.services[tableView.selectedRow])
        }

        func tableView(_ tableView: NSTableView, didClick tableColumn: NSTableColumn) {
            guard let column = ServiceColumn(rawValue: tableColumn.identifier.rawValue),
                  let sortColumn = column.sortColumn else { return }
            parent.onToggleSort(sortColumn)
        }

        func selectRowForContextMenu(_ row: Int, in tableView: NSTableView) {
            guard row >= 0, row < parent.services.count else { return }
            let service = parent.services[row]
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyImmediateResourceContextMenuSelection(row: row, in: tableView)
            guard service.id != parent.selectedServiceID else { return }
            DispatchQueue.main.async { [weak self] in
                guard let self,
                      let current = self.parent.services.first(where: { $0.id == service.id })
                else { return }
                self.parent.onSelectService(current)
            }
        }

        func tableViewColumnDidResize(_ notification: Notification) {
            guard let tableColumn = notification.userInfo?["NSTableColumn"] as? NSTableColumn,
                  let column = ServiceColumn(rawValue: tableColumn.identifier.rawValue),
                  column.isUserResizable else { return }
            let width = min(max(tableColumn.width, tableColumn.minWidth), tableColumn.maxWidth)
            tableColumn.width = width
            RuneAppKitColumnWidthStore.shared.setWidth(width, tableID: tableID, columnID: column.rawValue)
            updateTableWidth(on: tableView)
        }

        func makeMenu(forRow row: Int) -> NSMenu? {
            guard row >= 0, row < parent.services.count else { return nil }
            let service = parent.services[row]
            let menu = NSMenu()
            menu.addItem(menuItem(parent.isFavorite(service) ? "Remove Favorite" : "Favorite Resource", action: #selector(toggleFavoriteFromMenu(_:)), service: service))
            menu.addItem(.separator())
            menu.addItem(menuItem("Open Unified Logs", action: #selector(openUnifiedLogsFromMenu(_:)), service: service))
            menu.addItem(menuItem("Port Forward", action: #selector(openPortForwardFromMenu(_:)), service: service))
            menu.addItem(menuItem("Describe", action: #selector(openDescribeFromMenu(_:)), service: service))
            menu.addItem(menuItem("Open YAML", action: #selector(openYAMLFromMenu(_:)), service: service))
            menu.addItem(.separator())
            menu.addItem(menuItem("Copy service name", action: #selector(copyServiceNameFromMenu(_:)), service: service))
            menu.addItem(menuItem("Copy namespace", action: #selector(copyNamespaceFromMenu(_:)), service: service))
            menu.addItem(menuItem("Copy cluster IP", action: #selector(copyClusterIPFromMenu(_:)), service: service))
            if service.selector?.isEmpty == false {
                menu.addItem(menuItem("Copy selector", action: #selector(copySelectorFromMenu(_:)), service: service))
            }
            menu.addItem(.separator())
            menu.addItem(menuItem("Delete Service", action: #selector(deleteFromMenu(_:)), service: service, isEnabled: parent.canApplyClusterMutations))
            return menu
        }

        @objc private func toggleFavoriteFromMenu(_ sender: NSMenuItem) {
            withService(sender, parent.onToggleFavorite)
        }

        func toggleFavoriteForSelectedRow(in tableView: NSTableView) {
            guard tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.services.count else { return }
            parent.onToggleFavorite(parent.services[tableView.selectedRow])
        }

        @objc private func toggleFavoriteButton(_ sender: NSButton) {
            guard sender.tag >= 0, sender.tag < parent.services.count else { return }
            parent.onToggleFavorite(parent.services[sender.tag])
        }

        @objc private func openUnifiedLogsFromMenu(_ sender: NSMenuItem) {
            withService(sender, parent.onOpenUnifiedLogs)
        }

        @objc private func openPortForwardFromMenu(_ sender: NSMenuItem) {
            withService(sender, parent.onOpenPortForward)
        }

        @objc private func openDescribeFromMenu(_ sender: NSMenuItem) {
            withService(sender, parent.onOpenDescribe)
        }

        @objc private func openYAMLFromMenu(_ sender: NSMenuItem) {
            withService(sender, parent.onOpenYAML)
        }

        @objc private func deleteFromMenu(_ sender: NSMenuItem) {
            withService(sender, parent.onDelete)
        }

        @objc private func copyServiceNameFromMenu(_ sender: NSMenuItem) {
            withService(sender) { service in copyToClipboard(service.name) }
        }

        @objc private func copyNamespaceFromMenu(_ sender: NSMenuItem) {
            withService(sender) { service in copyToClipboard(service.namespace) }
        }

        @objc private func copyClusterIPFromMenu(_ sender: NSMenuItem) {
            withService(sender) { service in copyToClipboard(service.clusterIP) }
        }

        @objc private func copySelectorFromMenu(_ sender: NSMenuItem) {
            withService(sender) { service in
                if let selector = service.selector, !selector.isEmpty {
                    copyToClipboard(copyLabelSelector(selector))
                }
            }
        }

        private func applySelection(on tableView: NSTableView) {
            let selectedRow = parent.services.firstIndex { $0.id == parent.selectedServiceID }
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            if let selectedRow {
                tableView.selectRowIndexes(IndexSet(integer: selectedRow), byExtendingSelection: false)
            } else {
                tableView.deselectAll(nil)
            }
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.serviceColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = min(
                RuneAppKitResourceListLayout.serviceMaximumContentWidth,
                max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.serviceTrailingBreathingRoom)
            )
            let storedType = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: ServiceColumn.type.rawValue)
            let typeWidth = min(max(storedType ?? widths.type, ServiceColumn.type.minimumWidth), ServiceColumn.type.maximumWidth)
            let storedClusterIP = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: ServiceColumn.clusterIP.rawValue)
            let clusterIPWidth = min(max(storedClusterIP ?? widths.clusterIP, ServiceColumn.clusterIP.minimumWidth), ServiceColumn.clusterIP.maximumWidth)
            let dynamicNameWidth = max(
                RuneAppKitResourceListLayout.serviceMinimumNameColumnWidth,
                projectedWidth - typeWidth - clusterIPWidth - widths.favorite
            )
            let storedName = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: ServiceColumn.name.rawValue)
            let nameWidth = min(max(storedName ?? dynamicNameWidth, ServiceColumn.name.minimumWidth), ServiceColumn.name.maximumWidth)
            for tableColumn in tableView.tableColumns {
                guard let column = ServiceColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                switch column {
                case .name: tableColumn.width = nameWidth
                case .type: tableColumn.width = typeWidth
                case .clusterIP: tableColumn.width = clusterIPWidth
                case .favorite: tableColumn.width = widths.favorite
                }
            }
            updateTableWidth(on: tableView)
        }

        func resetColumnWidth(_ columnID: String) {
            RuneAppKitColumnWidthStore.shared.removeWidth(tableID: tableID, columnID: columnID)
            updateColumnWidths()
        }

        private func updateTableWidth(on tableView: NSTableView?) {
            RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)
        }

        private func updateServiceSortIndicator(on tableView: NSTableView) {
            for tableColumn in tableView.tableColumns {
                guard let column = ServiceColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                if let headerCell = tableColumn.headerCell as? RuneAppKitResourceTableHeaderCell {
                    headerCell.configure(
                        title: column.title,
                        isSorted: column.sortColumn == parent.sortColumn,
                        ascending: parent.sortAscending,
                        reservesSortIndicator: column.sortColumn != nil
                    )
                }
                if column.sortColumn == parent.sortColumn {
                    let imageName = NSImage.Name(parent.sortAscending ? "NSAscendingSortIndicator" : "NSDescendingSortIndicator")
                    tableView.setIndicatorImage(NSImage(named: imageName), in: tableColumn)
                } else {
                    tableView.setIndicatorImage(nil, in: tableColumn)
                }
            }
            tableView.headerView?.needsDisplay = true
        }

        private func menuItem(_ title: String, action: Selector, service: ServiceSummary, isEnabled: Bool = true) -> NSMenuItem {
            let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
            item.target = self
            item.representedObject = service
            item.isEnabled = isEnabled
            return item
        }

        private func withService(_ sender: NSMenuItem, _ action: (ServiceSummary) -> Void) {
            guard let service = sender.representedObject as? ServiceSummary else { return }
            action(service)
        }

        private func copyToClipboard(_ value: String) {
            let pasteboard = NSPasteboard.general
            pasteboard.clearContents()
            pasteboard.setString(value, forType: .string)
        }
    }
}

struct AppKitGenericResourceListView: NSViewRepresentable {
    let resources: [ClusterResourceSummary]
    let selectedResourceID: String?
    let selectedResourceIDs: Set<String>
    let sortColumn: GenericResourceListSortColumn
    let sortAscending: Bool
    let canApplyClusterMutations: Bool
    let isFavorite: (ClusterResourceSummary) -> Bool
    let onSelectResource: (ClusterResourceSummary) -> Void
    let onToggleBulkSelection: (ClusterResourceSummary) -> Void
    let onToggleSort: (GenericResourceListSortColumn) -> Void
    let onToggleFavorite: (ClusterResourceSummary) -> Void
    let onOpenDescribe: (ClusterResourceSummary) -> Void
    let onOpenYAML: (ClusterResourceSummary) -> Void
    let onDelete: (ClusterResourceSummary) -> Void
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let tableView = GenericResourceNSTableView()
        tableView.coordinator = context.coordinator
        tableView.delegate = context.coordinator
        tableView.dataSource = context.coordinator
        RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: true)
        let headerView = RuneAppKitResourceTableHeaderView()
        RuneAppKitResourceTableStyle.apply(to: headerView)
        headerView.resizableColumnIdentifiers = Set(GenericResourceColumn.allCases.filter(\.isUserResizable).map(\.rawValue))
        headerView.onResetColumn = { [weak coordinator = context.coordinator] columnID in
            coordinator?.resetColumnWidth(columnID)
        }
        tableView.headerView = headerView

        for column in GenericResourceColumn.allCases {
            tableView.addTableColumn(column.tableColumn())
        }

        let scrollView = RuneAppKitResourceListScrollView()
        scrollView.onVisibleWidthChanged = { [weak coordinator = context.coordinator] in
            coordinator?.updateColumnWidths()
        }
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.documentView = tableView
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? GenericResourceNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
    }

    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        var parent: AppKitGenericResourceListView
        weak var tableView: NSTableView?
        private var isApplyingSelection = false
        private var applyGeneration = 0
        private let tableID = "genericResources"

        init(_ parent: AppKitGenericResourceListView) {
            self.parent = parent
        }

        func apply(parent: AppKitGenericResourceListView) {
            guard let tableView else { return }
            applyGeneration += 1
            let generation = applyGeneration
            DispatchQueue.main.async { [weak self, weak tableView] in
                guard let self,
                      let tableView,
                      generation == self.applyGeneration else { return }
                self.updateColumnWidths(on: tableView)
                tableView.reloadData()
                self.applySelection(on: tableView)
                self.updateGenericSortIndicator(on: tableView)
            }
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            parent.resources.count
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard row >= 0, row < parent.resources.count,
                  let tableColumn,
                  let column = GenericResourceColumn(rawValue: tableColumn.identifier.rawValue) else { return nil }
            let resource = parent.resources[row]
            switch column {
            case .selection:
                let container = NSView()
                let checkbox = NSButton(checkboxWithTitle: "", target: self, action: #selector(toggleBulkSelection(_:)))
                checkbox.tag = row
                checkbox.state = parent.selectedResourceIDs.contains(resource.id) ? .on : .off
                checkbox.toolTip = parent.selectedResourceIDs.contains(resource.id) ? "Remove from bulk selection" : "Add to bulk selection"
                checkbox.controlSize = .small
                checkbox.setAccessibilityLabel("Select \(resource.name)")
                checkbox.translatesAutoresizingMaskIntoConstraints = false
                container.addSubview(checkbox)
                NSLayoutConstraint.activate([
                    checkbox.centerXAnchor.constraint(equalTo: container.centerXAnchor),
                    checkbox.centerYAnchor.constraint(equalTo: container.centerYAnchor)
                ])
                return container
            case .name:
                return RuneAppKitCenteredLabelCell(
                    text: resource.name,
                    font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium),
                    alignment: .left,
                    tooltip: resource.name,
                    lineBreakMode: .byTruncatingMiddle
                )
            case .primary:
                return RuneAppKitCenteredLabelCell(text: resource.primaryText, font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold), alignment: .right, tooltip: resource.primaryText)
            case .secondary:
                return RuneAppKitCenteredLabelCell(text: resource.secondaryText, font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular), alignment: .left, tooltip: resource.secondaryText)
            case .namespace:
                return RuneAppKitCenteredLabelCell(text: resource.namespace ?? "Cluster", font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular), alignment: .left, tooltip: resource.namespace ?? "Cluster scoped")
            case .favorite:
                return RuneAppKitFavoriteButtonCell(
                    isFavorite: parent.isFavorite(resource),
                    row: row,
                    target: self,
                    action: #selector(toggleFavoriteButton(_:))
                )
            }
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard !isApplyingSelection,
                  let tableView = notification.object as? NSTableView,
                  tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.resources.count else { return }
            parent.onSelectResource(parent.resources[tableView.selectedRow])
        }

        func tableView(_ tableView: NSTableView, didClick tableColumn: NSTableColumn) {
            guard let column = GenericResourceColumn(rawValue: tableColumn.identifier.rawValue),
                  let sortColumn = column.sortColumn else { return }
            parent.onToggleSort(sortColumn)
        }

        func selectRowForContextMenu(_ row: Int, in tableView: NSTableView) {
            guard row >= 0, row < parent.resources.count else { return }
            let resource = parent.resources[row]
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyImmediateResourceContextMenuSelection(row: row, in: tableView)
            guard resource.id != parent.selectedResourceID else { return }
            DispatchQueue.main.async { [weak self] in
                guard let self,
                      let current = self.parent.resources.first(where: { $0.id == resource.id })
                else { return }
                self.parent.onSelectResource(current)
            }
        }

        func tableViewColumnDidResize(_ notification: Notification) {
            guard let tableColumn = notification.userInfo?["NSTableColumn"] as? NSTableColumn,
                  let column = GenericResourceColumn(rawValue: tableColumn.identifier.rawValue),
                  column.isUserResizable else { return }
            let width = min(max(tableColumn.width, tableColumn.minWidth), tableColumn.maxWidth)
            tableColumn.width = width
            RuneAppKitColumnWidthStore.shared.setWidth(width, tableID: tableID, columnID: column.rawValue)
            updateTableWidth(on: tableView)
        }

        func makeMenu(forRow row: Int) -> NSMenu? {
            guard row >= 0, row < parent.resources.count else { return nil }
            let resource = parent.resources[row]
            let menu = NSMenu()
            menu.addItem(menuItem(parent.isFavorite(resource) ? "Remove Favorite" : "Favorite Resource", action: #selector(toggleFavoriteFromMenu(_:)), resource: resource))
            menu.addItem(.separator())
            menu.addItem(menuItem("Describe", action: #selector(openDescribeFromMenu(_:)), resource: resource))
            menu.addItem(menuItem("Open YAML", action: #selector(openYAMLFromMenu(_:)), resource: resource))
            menu.addItem(.separator())
            menu.addItem(menuItem("Copy \(resource.kind.singularTypeName) name", action: #selector(copyResourceNameFromMenu(_:)), resource: resource))
            if resource.namespace != nil {
                menu.addItem(menuItem("Copy namespace", action: #selector(copyNamespaceFromMenu(_:)), resource: resource))
            }
            if !resource.primaryText.isEmpty {
                menu.addItem(menuItem("Copy primary detail", action: #selector(copyPrimaryTextFromMenu(_:)), resource: resource))
            }
            if !resource.secondaryText.isEmpty {
                menu.addItem(menuItem("Copy secondary detail", action: #selector(copySecondaryTextFromMenu(_:)), resource: resource))
            }
            if resource.ownerReferencesLine != nil {
                menu.addItem(menuItem("Copy owner reference", action: #selector(copyOwnerReferenceFromMenu(_:)), resource: resource))
            }
            menu.addItem(.separator())
            menu.addItem(menuItem("Delete \(resource.kind.singularTypeName)", action: #selector(deleteFromMenu(_:)), resource: resource, isEnabled: parent.canApplyClusterMutations))
            return menu
        }

        @objc private func toggleBulkSelection(_ sender: NSButton) {
            guard sender.tag >= 0, sender.tag < parent.resources.count else { return }
            parent.onToggleBulkSelection(parent.resources[sender.tag])
        }

        @objc private func toggleFavoriteButton(_ sender: NSButton) {
            guard sender.tag >= 0, sender.tag < parent.resources.count else { return }
            parent.onToggleFavorite(parent.resources[sender.tag])
        }

        func toggleFavoriteForSelectedRow(in tableView: NSTableView) {
            guard tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.resources.count else { return }
            parent.onToggleFavorite(parent.resources[tableView.selectedRow])
        }

        @objc private func toggleFavoriteFromMenu(_ sender: NSMenuItem) {
            withResource(sender, parent.onToggleFavorite)
        }

        @objc private func openDescribeFromMenu(_ sender: NSMenuItem) {
            withResource(sender, parent.onOpenDescribe)
        }

        @objc private func openYAMLFromMenu(_ sender: NSMenuItem) {
            withResource(sender, parent.onOpenYAML)
        }

        @objc private func deleteFromMenu(_ sender: NSMenuItem) {
            withResource(sender, parent.onDelete)
        }

        @objc private func copyResourceNameFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in copyToClipboard(resource.name) }
        }

        @objc private func copyNamespaceFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in
                if let namespace = resource.namespace {
                    copyToClipboard(namespace)
                }
            }
        }

        @objc private func copyPrimaryTextFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in copyToClipboard(resource.primaryText) }
        }

        @objc private func copySecondaryTextFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in copyToClipboard(resource.secondaryText) }
        }

        @objc private func copyOwnerReferenceFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in
                if let ownerReferencesLine = resource.ownerReferencesLine {
                    copyToClipboard(ownerReferencesLine)
                }
            }
        }

        private func applySelection(on tableView: NSTableView) {
            let selectedRow = parent.resources.firstIndex { $0.id == parent.selectedResourceID }
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            if let selectedRow {
                tableView.selectRowIndexes(IndexSet(integer: selectedRow), byExtendingSelection: false)
            } else {
                tableView.deselectAll(nil)
            }
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.genericColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = min(
                RuneAppKitResourceListLayout.genericMaximumContentWidth,
                max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.genericTrailingBreathingRoom)
            )
            let storedPrimary = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: GenericResourceColumn.primary.rawValue)
            let primaryWidth = min(max(storedPrimary ?? widths.primary, GenericResourceColumn.primary.minimumWidth), GenericResourceColumn.primary.maximumWidth)
            let storedSecondary = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: GenericResourceColumn.secondary.rawValue)
            let secondaryWidth = min(max(storedSecondary ?? widths.secondary, GenericResourceColumn.secondary.minimumWidth), GenericResourceColumn.secondary.maximumWidth)
            let storedNamespace = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: GenericResourceColumn.namespace.rawValue)
            let namespaceWidth = min(max(storedNamespace ?? widths.namespace, GenericResourceColumn.namespace.minimumWidth), GenericResourceColumn.namespace.maximumWidth)
            let dynamicNameWidth = max(
                RuneAppKitResourceListLayout.genericMinimumNameColumnWidth,
                projectedWidth - widths.selection - primaryWidth - secondaryWidth - namespaceWidth - widths.favorite
            )
            let storedName = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: GenericResourceColumn.name.rawValue)
            let nameWidth = min(max(storedName ?? dynamicNameWidth, GenericResourceColumn.name.minimumWidth), GenericResourceColumn.name.maximumWidth)
            for tableColumn in tableView.tableColumns {
                guard let column = GenericResourceColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                switch column {
                case .selection: tableColumn.width = widths.selection
                case .name: tableColumn.width = nameWidth
                case .primary: tableColumn.width = primaryWidth
                case .secondary: tableColumn.width = secondaryWidth
                case .namespace: tableColumn.width = namespaceWidth
                case .favorite: tableColumn.width = widths.favorite
                }
            }
            updateTableWidth(on: tableView)
        }

        func resetColumnWidth(_ columnID: String) {
            RuneAppKitColumnWidthStore.shared.removeWidth(tableID: tableID, columnID: columnID)
            updateColumnWidths()
        }

        private func updateTableWidth(on tableView: NSTableView?) {
            RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)
        }

        private func updateGenericSortIndicator(on tableView: NSTableView) {
            for tableColumn in tableView.tableColumns {
                guard let column = GenericResourceColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                if let headerCell = tableColumn.headerCell as? RuneAppKitResourceTableHeaderCell {
                    headerCell.configure(
                        title: column.title,
                        isSorted: column.sortColumn == parent.sortColumn,
                        ascending: parent.sortAscending,
                        reservesSortIndicator: column.sortColumn != nil
                    )
                }
                if column.sortColumn == parent.sortColumn {
                    let imageName = NSImage.Name(parent.sortAscending ? "NSAscendingSortIndicator" : "NSDescendingSortIndicator")
                    tableView.setIndicatorImage(NSImage(named: imageName), in: tableColumn)
                } else {
                    tableView.setIndicatorImage(nil, in: tableColumn)
                }
            }
            tableView.headerView?.needsDisplay = true
        }

        private func menuItem(_ title: String, action: Selector, resource: ClusterResourceSummary, isEnabled: Bool = true) -> NSMenuItem {
            let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
            item.target = self
            item.representedObject = resource
            item.isEnabled = isEnabled
            return item
        }

        private func withResource(_ sender: NSMenuItem, _ action: (ClusterResourceSummary) -> Void) {
            guard let resource = sender.representedObject as? ClusterResourceSummary else { return }
            action(resource)
        }

        private func copyToClipboard(_ value: String) {
            let pasteboard = NSPasteboard.general
            pasteboard.clearContents()
            pasteboard.setString(value, forType: .string)
        }
    }
}

struct AppKitHelmReleaseListView: NSViewRepresentable {
    let releases: [HelmReleaseSummary]
    let selectedReleaseID: String?
    let sortColumn: HelmReleaseListSortColumn
    let sortAscending: Bool
    let onSelectRelease: (HelmReleaseSummary) -> Void
    let onToggleSort: (HelmReleaseListSortColumn) -> Void
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let tableView = HelmReleaseNSTableView()
        tableView.coordinator = context.coordinator
        tableView.delegate = context.coordinator
        tableView.dataSource = context.coordinator
        RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: true)
        let headerView = RuneAppKitResourceTableHeaderView()
        RuneAppKitResourceTableStyle.apply(to: headerView)
        headerView.resizableColumnIdentifiers = Set(HelmReleaseColumn.allCases.filter(\.isUserResizable).map(\.rawValue))
        headerView.onResetColumn = { [weak coordinator = context.coordinator] columnID in
            coordinator?.resetColumnWidth(columnID)
        }
        tableView.headerView = headerView

        for column in HelmReleaseColumn.allCases {
            tableView.addTableColumn(column.tableColumn())
        }

        let scrollView = RuneAppKitResourceListScrollView()
        scrollView.onVisibleWidthChanged = { [weak coordinator = context.coordinator] in
            coordinator?.updateColumnWidths()
        }
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.documentView = tableView
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? HelmReleaseNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
    }

    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        var parent: AppKitHelmReleaseListView
        weak var tableView: NSTableView?
        private var isApplyingSelection = false
        private var applyGeneration = 0
        private let tableID = "helmReleases"

        init(_ parent: AppKitHelmReleaseListView) {
            self.parent = parent
        }

        func apply(parent: AppKitHelmReleaseListView) {
            guard let tableView else { return }
            applyGeneration += 1
            let generation = applyGeneration
            DispatchQueue.main.async { [weak self, weak tableView] in
                guard let self,
                      let tableView,
                      generation == self.applyGeneration else { return }
                self.updateColumnWidths(on: tableView)
                tableView.reloadData()
                self.applySelection(on: tableView)
                self.updateHelmSortIndicator(on: tableView)
            }
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            parent.releases.count
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard row >= 0, row < parent.releases.count,
                  let tableColumn,
                  let column = HelmReleaseColumn(rawValue: tableColumn.identifier.rawValue) else { return nil }
            let release = parent.releases[row]
            switch column {
            case .name:
                return RuneAppKitCenteredLabelCell(
                    text: release.name,
                    font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium),
                    alignment: .left,
                    tooltip: release.name,
                    lineBreakMode: .byTruncatingMiddle
                )
            case .status:
                return RuneAppKitPillCell(text: release.status.capitalized, color: statusColor(for: release.status))
            case .namespace:
                return RuneAppKitCenteredLabelCell(
                    text: release.namespace,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: release.namespace
                )
            case .revision:
                return RuneAppKitCenteredLabelCell(
                    text: "\(release.revision)",
                    font: .monospacedDigitSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .right
                )
            case .chart:
                return RuneAppKitCenteredLabelCell(
                    text: release.chart,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: release.chart
                )
            case .appVersion:
                return RuneAppKitCenteredLabelCell(
                    text: release.appVersion,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: release.appVersion
                )
            }
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard !isApplyingSelection,
                  let tableView = notification.object as? NSTableView,
                  tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.releases.count else { return }
            parent.onSelectRelease(parent.releases[tableView.selectedRow])
        }

        func tableView(_ tableView: NSTableView, didClick tableColumn: NSTableColumn) {
            guard let column = HelmReleaseColumn(rawValue: tableColumn.identifier.rawValue) else { return }
            parent.onToggleSort(column.sortColumn)
        }

        func selectRowForContextMenu(_ row: Int, in tableView: NSTableView) {
            guard row >= 0, row < parent.releases.count else { return }
            let release = parent.releases[row]
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyImmediateResourceContextMenuSelection(row: row, in: tableView)
            guard release.id != parent.selectedReleaseID else { return }
            DispatchQueue.main.async { [weak self] in
                guard let self,
                      let current = self.parent.releases.first(where: { $0.id == release.id })
                else { return }
                self.parent.onSelectRelease(current)
            }
        }

        func tableViewColumnDidResize(_ notification: Notification) {
            guard let tableColumn = notification.userInfo?["NSTableColumn"] as? NSTableColumn,
                  let column = HelmReleaseColumn(rawValue: tableColumn.identifier.rawValue),
                  column.isUserResizable else { return }
            let width = min(max(tableColumn.width, tableColumn.minWidth), tableColumn.maxWidth)
            tableColumn.width = width
            RuneAppKitColumnWidthStore.shared.setWidth(width, tableID: tableID, columnID: column.rawValue)
            updateTableWidth(on: tableView)
        }

        func makeMenu(forRow row: Int) -> NSMenu? {
            guard row >= 0, row < parent.releases.count else { return nil }
            let release = parent.releases[row]
            let menu = NSMenu()
            menu.addItem(menuItem("Copy Helm release name", action: #selector(copyReleaseNameFromMenu(_:)), release: release))
            menu.addItem(menuItem("Copy namespace", action: #selector(copyNamespaceFromMenu(_:)), release: release))
            menu.addItem(menuItem("Copy chart", action: #selector(copyChartFromMenu(_:)), release: release))
            menu.addItem(menuItem("Copy app version", action: #selector(copyAppVersionFromMenu(_:)), release: release))
            return menu
        }

        @objc private func copyReleaseNameFromMenu(_ sender: NSMenuItem) {
            withRelease(sender) { release in copyToClipboard(release.name) }
        }

        @objc private func copyNamespaceFromMenu(_ sender: NSMenuItem) {
            withRelease(sender) { release in copyToClipboard(release.namespace) }
        }

        @objc private func copyChartFromMenu(_ sender: NSMenuItem) {
            withRelease(sender) { release in copyToClipboard(release.chart) }
        }

        @objc private func copyAppVersionFromMenu(_ sender: NSMenuItem) {
            withRelease(sender) { release in copyToClipboard(release.appVersion) }
        }

        private func applySelection(on tableView: NSTableView) {
            let selectedRow = parent.releases.firstIndex { $0.id == parent.selectedReleaseID }
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            if let selectedRow {
                tableView.selectRowIndexes(IndexSet(integer: selectedRow), byExtendingSelection: false)
            } else {
                tableView.deselectAll(nil)
            }
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.helmColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = min(
                RuneAppKitResourceListLayout.helmMaximumContentWidth,
                max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.helmTrailingBreathingRoom)
            )
            let storedStatus = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: HelmReleaseColumn.status.rawValue)
            let statusWidth = min(max(storedStatus ?? widths.status, HelmReleaseColumn.status.minimumWidth), HelmReleaseColumn.status.maximumWidth)
            let storedNamespace = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: HelmReleaseColumn.namespace.rawValue)
            let namespaceWidth = min(max(storedNamespace ?? widths.namespace, HelmReleaseColumn.namespace.minimumWidth), HelmReleaseColumn.namespace.maximumWidth)
            let storedRevision = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: HelmReleaseColumn.revision.rawValue)
            let revisionWidth = min(max(storedRevision ?? widths.revision, HelmReleaseColumn.revision.minimumWidth), HelmReleaseColumn.revision.maximumWidth)
            let storedChart = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: HelmReleaseColumn.chart.rawValue)
            let chartWidth = min(max(storedChart ?? widths.chart, HelmReleaseColumn.chart.minimumWidth), HelmReleaseColumn.chart.maximumWidth)
            let storedAppVersion = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: HelmReleaseColumn.appVersion.rawValue)
            let appVersionWidth = min(max(storedAppVersion ?? widths.appVersion, HelmReleaseColumn.appVersion.minimumWidth), HelmReleaseColumn.appVersion.maximumWidth)
            let dynamicNameWidth = max(
                RuneAppKitResourceListLayout.helmMinimumNameColumnWidth,
                projectedWidth - statusWidth - namespaceWidth - revisionWidth - chartWidth - appVersionWidth
            )
            let storedName = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: HelmReleaseColumn.name.rawValue)
            let nameWidth = min(max(storedName ?? dynamicNameWidth, HelmReleaseColumn.name.minimumWidth), HelmReleaseColumn.name.maximumWidth)
            for tableColumn in tableView.tableColumns {
                guard let column = HelmReleaseColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                switch column {
                case .name: tableColumn.width = nameWidth
                case .status: tableColumn.width = statusWidth
                case .namespace: tableColumn.width = namespaceWidth
                case .revision: tableColumn.width = revisionWidth
                case .chart: tableColumn.width = chartWidth
                case .appVersion: tableColumn.width = appVersionWidth
                }
            }
            updateTableWidth(on: tableView)
        }

        func resetColumnWidth(_ columnID: String) {
            RuneAppKitColumnWidthStore.shared.removeWidth(tableID: tableID, columnID: columnID)
            updateColumnWidths()
        }

        private func updateTableWidth(on tableView: NSTableView?) {
            RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)
        }

        private func updateHelmSortIndicator(on tableView: NSTableView) {
            for tableColumn in tableView.tableColumns {
                guard let column = HelmReleaseColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                if let headerCell = tableColumn.headerCell as? RuneAppKitResourceTableHeaderCell {
                    headerCell.configure(
                        title: column.title,
                        isSorted: column.sortColumn == parent.sortColumn,
                        ascending: parent.sortAscending,
                        reservesSortIndicator: true
                    )
                }
                if column.sortColumn == parent.sortColumn {
                    let imageName = NSImage.Name(parent.sortAscending ? "NSAscendingSortIndicator" : "NSDescendingSortIndicator")
                    tableView.setIndicatorImage(NSImage(named: imageName), in: tableColumn)
                } else {
                    tableView.setIndicatorImage(nil, in: tableColumn)
                }
            }
            tableView.headerView?.needsDisplay = true
        }

        private func statusColor(for status: String) -> NSColor {
            switch status.lowercased() {
            case "deployed", "running", "succeeded", "ready":
                return .systemGreen
            case "pending", "superseded", "uninstalling":
                return .systemOrange
            case "failed", "error", "uninstalled":
                return .systemRed
            default:
                return .secondaryLabelColor
            }
        }

        private func menuItem(_ title: String, action: Selector, release: HelmReleaseSummary) -> NSMenuItem {
            let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
            item.target = self
            item.representedObject = release
            return item
        }

        private func withRelease(_ sender: NSMenuItem, _ action: (HelmReleaseSummary) -> Void) {
            guard let release = sender.representedObject as? HelmReleaseSummary else { return }
            action(release)
        }

        private func copyToClipboard(_ value: String) {
            let pasteboard = NSPasteboard.general
            pasteboard.clearContents()
            pasteboard.setString(value, forType: .string)
        }
    }
}

struct AppKitEventListView: NSViewRepresentable {
    let events: [EventSummary]
    let selectedEventID: String?
    let sortColumn: EventListSortColumn
    let sortAscending: Bool
    let onSelectEvent: (EventSummary) -> Void
    let onToggleSort: (EventListSortColumn) -> Void
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let tableView = EventNSTableView()
        tableView.coordinator = context.coordinator
        tableView.delegate = context.coordinator
        tableView.dataSource = context.coordinator
        RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: true)
        let headerView = RuneAppKitResourceTableHeaderView()
        RuneAppKitResourceTableStyle.apply(to: headerView)
        headerView.resizableColumnIdentifiers = Set(EventColumn.allCases.filter(\.isUserResizable).map(\.rawValue))
        headerView.onResetColumn = { [weak coordinator = context.coordinator] columnID in
            coordinator?.resetColumnWidth(columnID)
        }
        tableView.headerView = headerView

        for column in EventColumn.allCases {
            tableView.addTableColumn(column.tableColumn())
        }

        let scrollView = RuneAppKitResourceListScrollView()
        scrollView.onVisibleWidthChanged = { [weak coordinator = context.coordinator] in
            coordinator?.updateColumnWidths()
        }
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.documentView = tableView
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? EventNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
    }

    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        var parent: AppKitEventListView
        weak var tableView: NSTableView?
        private var isApplyingSelection = false
        private var applyGeneration = 0
        private let tableID = "events"

        init(_ parent: AppKitEventListView) {
            self.parent = parent
        }

        func apply(parent: AppKitEventListView) {
            guard let tableView else { return }
            applyGeneration += 1
            let generation = applyGeneration
            DispatchQueue.main.async { [weak self, weak tableView] in
                guard let self,
                      let tableView,
                      generation == self.applyGeneration else { return }
                self.updateColumnWidths(on: tableView)
                tableView.reloadData()
                self.applySelection(on: tableView)
                self.updateEventSortIndicator(on: tableView)
            }
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            parent.events.count
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard row >= 0, row < parent.events.count,
                  let tableColumn,
                  let column = EventColumn(rawValue: tableColumn.identifier.rawValue) else { return nil }
            let event = parent.events[row]
            switch column {
            case .reason:
                return RuneAppKitCenteredLabelCell(
                    text: event.reason,
                    font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium),
                    alignment: .left,
                    tooltip: event.reason,
                    lineBreakMode: .byTruncatingMiddle
                )
            case .type:
                return RuneAppKitPillCell(text: event.type, color: eventColor(for: event.type))
            case .object:
                return RuneAppKitCenteredLabelCell(
                    text: event.objectName,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: event.objectName,
                    lineBreakMode: .byTruncatingMiddle
                )
            case .namespace:
                let namespace = event.involvedNamespace ?? "Cluster"
                return RuneAppKitCenteredLabelCell(
                    text: namespace,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: namespace
                )
            case .lastSeen:
                return RuneAppKitCenteredLabelCell(
                    text: event.lastTimestamp ?? "-",
                    font: .monospacedDigitSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .right,
                    tooltip: event.lastTimestamp
                )
            case .message:
                return RuneAppKitCenteredLabelCell(
                    text: event.message,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: event.message
                )
            }
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard !isApplyingSelection,
                  let tableView = notification.object as? NSTableView,
                  tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.events.count else { return }
            parent.onSelectEvent(parent.events[tableView.selectedRow])
        }

        func tableView(_ tableView: NSTableView, didClick tableColumn: NSTableColumn) {
            guard let column = EventColumn(rawValue: tableColumn.identifier.rawValue),
                  let sortColumn = column.sortColumn else { return }
            parent.onToggleSort(sortColumn)
        }

        func selectRowForContextMenu(_ row: Int, in tableView: NSTableView) {
            guard row >= 0, row < parent.events.count else { return }
            let event = parent.events[row]
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyImmediateResourceContextMenuSelection(row: row, in: tableView)
            guard event.id != parent.selectedEventID else { return }
            DispatchQueue.main.async { [weak self] in
                guard let self,
                      let current = self.parent.events.first(where: { $0.id == event.id })
                else { return }
                self.parent.onSelectEvent(current)
            }
        }

        func tableViewColumnDidResize(_ notification: Notification) {
            guard let tableColumn = notification.userInfo?["NSTableColumn"] as? NSTableColumn,
                  let column = EventColumn(rawValue: tableColumn.identifier.rawValue),
                  column.isUserResizable else { return }
            let width = min(max(tableColumn.width, tableColumn.minWidth), tableColumn.maxWidth)
            tableColumn.width = width
            RuneAppKitColumnWidthStore.shared.setWidth(width, tableID: tableID, columnID: column.rawValue)
            updateTableWidth(on: tableView)
        }

        func makeMenu(forRow row: Int) -> NSMenu? {
            guard row >= 0, row < parent.events.count else { return nil }
            let event = parent.events[row]
            let menu = NSMenu()
            menu.addItem(menuItem("Copy event reason", action: #selector(copyReasonFromMenu(_:)), event: event))
            if event.involvedKind != nil {
                menu.addItem(menuItem("Copy object kind", action: #selector(copyObjectKindFromMenu(_:)), event: event))
            }
            menu.addItem(menuItem("Copy object name", action: #selector(copyObjectNameFromMenu(_:)), event: event))
            if event.involvedNamespace != nil {
                menu.addItem(menuItem("Copy namespace", action: #selector(copyNamespaceFromMenu(_:)), event: event))
            }
            menu.addItem(menuItem("Copy event message", action: #selector(copyMessageFromMenu(_:)), event: event))
            return menu
        }

        @objc private func copyReasonFromMenu(_ sender: NSMenuItem) {
            withEvent(sender) { event in copyToClipboard(event.reason) }
        }

        @objc private func copyObjectKindFromMenu(_ sender: NSMenuItem) {
            withEvent(sender) { event in
                if let kind = event.involvedKind {
                    copyToClipboard(kind)
                }
            }
        }

        @objc private func copyObjectNameFromMenu(_ sender: NSMenuItem) {
            withEvent(sender) { event in copyToClipboard(event.objectName) }
        }

        @objc private func copyNamespaceFromMenu(_ sender: NSMenuItem) {
            withEvent(sender) { event in
                if let namespace = event.involvedNamespace {
                    copyToClipboard(namespace)
                }
            }
        }

        @objc private func copyMessageFromMenu(_ sender: NSMenuItem) {
            withEvent(sender) { event in copyToClipboard(event.message) }
        }

        private func applySelection(on tableView: NSTableView) {
            let selectedRow = parent.events.firstIndex { $0.id == parent.selectedEventID }
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            if let selectedRow {
                tableView.selectRowIndexes(IndexSet(integer: selectedRow), byExtendingSelection: false)
            } else {
                tableView.deselectAll(nil)
            }
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.eventColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = min(
                RuneAppKitResourceListLayout.eventMaximumContentWidth,
                max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.eventTrailingBreathingRoom)
            )
            let storedType = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: EventColumn.type.rawValue)
            let typeWidth = min(max(storedType ?? widths.type, EventColumn.type.minimumWidth), EventColumn.type.maximumWidth)
            let storedObject = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: EventColumn.object.rawValue)
            let objectWidth = min(max(storedObject ?? widths.object, EventColumn.object.minimumWidth), EventColumn.object.maximumWidth)
            let storedNamespace = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: EventColumn.namespace.rawValue)
            let namespaceWidth = min(max(storedNamespace ?? widths.namespace, EventColumn.namespace.minimumWidth), EventColumn.namespace.maximumWidth)
            let storedLastSeen = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: EventColumn.lastSeen.rawValue)
            let lastSeenWidth = min(max(storedLastSeen ?? widths.lastSeen, EventColumn.lastSeen.minimumWidth), EventColumn.lastSeen.maximumWidth)
            let storedMessage = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: EventColumn.message.rawValue)
            let messageWidth = min(max(storedMessage ?? widths.message, EventColumn.message.minimumWidth), EventColumn.message.maximumWidth)
            let dynamicReasonWidth = max(
                RuneAppKitResourceListLayout.eventMinimumReasonColumnWidth,
                projectedWidth - typeWidth - objectWidth - namespaceWidth - lastSeenWidth - messageWidth
            )
            let storedReason = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: EventColumn.reason.rawValue)
            let reasonWidth = min(max(storedReason ?? dynamicReasonWidth, EventColumn.reason.minimumWidth), EventColumn.reason.maximumWidth)
            for tableColumn in tableView.tableColumns {
                guard let column = EventColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                switch column {
                case .reason: tableColumn.width = reasonWidth
                case .type: tableColumn.width = typeWidth
                case .object: tableColumn.width = objectWidth
                case .namespace: tableColumn.width = namespaceWidth
                case .lastSeen: tableColumn.width = lastSeenWidth
                case .message: tableColumn.width = messageWidth
                }
            }
            updateTableWidth(on: tableView)
        }

        func resetColumnWidth(_ columnID: String) {
            RuneAppKitColumnWidthStore.shared.removeWidth(tableID: tableID, columnID: columnID)
            updateColumnWidths()
        }

        private func updateTableWidth(on tableView: NSTableView?) {
            RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)
        }

        private func updateEventSortIndicator(on tableView: NSTableView) {
            for tableColumn in tableView.tableColumns {
                guard let column = EventColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                if let headerCell = tableColumn.headerCell as? RuneAppKitResourceTableHeaderCell {
                    headerCell.configure(
                        title: column.title,
                        isSorted: column.sortColumn == parent.sortColumn,
                        ascending: parent.sortAscending,
                        reservesSortIndicator: column.sortColumn != nil
                    )
                }
                if column.sortColumn == parent.sortColumn {
                    let imageName = NSImage.Name(parent.sortAscending ? "NSAscendingSortIndicator" : "NSDescendingSortIndicator")
                    tableView.setIndicatorImage(NSImage(named: imageName), in: tableColumn)
                } else {
                    tableView.setIndicatorImage(nil, in: tableColumn)
                }
            }
            tableView.headerView?.needsDisplay = true
        }

        private func eventColor(for type: String) -> NSColor {
            type.lowercased() == "warning" ? .systemOrange : .systemGreen
        }

        private func menuItem(_ title: String, action: Selector, event: EventSummary) -> NSMenuItem {
            let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
            item.target = self
            item.representedObject = event
            return item
        }

        private func withEvent(_ sender: NSMenuItem, _ action: (EventSummary) -> Void) {
            guard let event = sender.representedObject as? EventSummary else { return }
            action(event)
        }

        private func copyToClipboard(_ value: String) {
            let pasteboard = NSPasteboard.general
            pasteboard.clearContents()
            pasteboard.setString(value, forType: .string)
        }
    }
}

struct AppKitOperatorResourceListView: NSViewRepresentable {
    let resources: [OperatorResourceSummary]
    let selectedResourceID: String?
    let sortColumn: OperatorResourceListSortColumn
    let sortAscending: Bool
    let showsPrinterColumns: Bool
    let isFavorite: (OperatorResourceSummary) -> Bool
    let onSelectResource: (OperatorResourceSummary) -> Void
    let onToggleSort: (OperatorResourceListSortColumn) -> Void
    let onToggleFavorite: (OperatorResourceSummary) -> Void
    let onOpenDescribe: (OperatorResourceSummary) -> Void
    let onOpenYAML: (OperatorResourceSummary) -> Void
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let tableView = OperatorResourceNSTableView()
        tableView.coordinator = context.coordinator
        tableView.delegate = context.coordinator
        tableView.dataSource = context.coordinator
        RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: true)
        let headerView = RuneAppKitResourceTableHeaderView()
        RuneAppKitResourceTableStyle.apply(to: headerView)
        headerView.resizableColumnIdentifiers = Set(OperatorResourceColumn.allCases.filter(\.isUserResizable).map(\.rawValue))
        headerView.onResetColumn = { [weak coordinator = context.coordinator] columnID in
            coordinator?.resetColumnWidth(columnID)
        }
        tableView.headerView = headerView

        for column in OperatorResourceColumn.allCases {
            tableView.addTableColumn(column.tableColumn())
        }

        let scrollView = RuneAppKitResourceListScrollView()
        scrollView.onVisibleWidthChanged = { [weak coordinator = context.coordinator] in
            coordinator?.updateColumnWidths()
        }
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.documentView = tableView
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? OperatorResourceNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
    }

    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        var parent: AppKitOperatorResourceListView
        weak var tableView: NSTableView?
        private var isApplyingSelection = false
        private var applyGeneration = 0
        private let tableID = "operatorResources"

        init(_ parent: AppKitOperatorResourceListView) {
            self.parent = parent
        }

        func apply(parent: AppKitOperatorResourceListView) {
            guard let tableView else { return }
            applyGeneration += 1
            let generation = applyGeneration
            DispatchQueue.main.async { [weak self, weak tableView] in
                guard let self,
                      let tableView,
                      generation == self.applyGeneration else { return }
                self.updateColumnWidths(on: tableView)
                tableView.reloadData()
                self.applySelection(on: tableView)
                self.updateOperatorResourceSortIndicator(on: tableView)
            }
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            parent.resources.count
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard row >= 0, row < parent.resources.count,
                  let tableColumn,
                  let column = OperatorResourceColumn(rawValue: tableColumn.identifier.rawValue) else { return nil }
            let resource = parent.resources[row]
            switch column {
            case .name:
                return RuneAppKitCenteredLabelCell(
                    text: resource.name,
                    font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium),
                    alignment: .left,
                    tooltip: resource.name,
                    lineBreakMode: .byTruncatingMiddle
                )
            case .family:
                return RuneAppKitCenteredLabelCell(
                    text: resource.family,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: resource.family
                )
            case .kind:
                return RuneAppKitCenteredLabelCell(
                    text: resource.kind,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: resource.kind
                )
            case .namespace:
                let namespace = resource.namespace ?? "Cluster"
                return RuneAppKitCenteredLabelCell(
                    text: namespace,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: namespace
                )
            case .status:
                return RuneAppKitPillCell(text: resource.status, color: statusColor(for: resource.status))
            case .printerColumns:
                let text = resource.printerColumns
                    .map { "\($0.title): \($0.value)" }
                    .joined(separator: "  ")
                return RuneAppKitCenteredLabelCell(
                    text: text.isEmpty ? "—" : text,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: text.isEmpty ? "No custom columns" : text,
                    lineBreakMode: .byTruncatingTail
                )
            case .apiPath:
                return RuneAppKitCenteredLabelCell(
                    text: resource.apiPath,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: .left,
                    tooltip: resource.apiPath,
                    lineBreakMode: .byTruncatingMiddle
                )
            case .favorite:
                return RuneAppKitFavoriteButtonCell(
                    isFavorite: parent.isFavorite(resource),
                    row: row,
                    target: self,
                    action: #selector(toggleFavoriteButton(_:))
                )
            }
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard !isApplyingSelection,
                  let tableView = notification.object as? NSTableView,
                  tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.resources.count else { return }
            parent.onSelectResource(parent.resources[tableView.selectedRow])
        }

        func tableView(_ tableView: NSTableView, didClick tableColumn: NSTableColumn) {
            guard let column = OperatorResourceColumn(rawValue: tableColumn.identifier.rawValue),
                  let sortColumn = column.sortColumn else { return }
            parent.onToggleSort(sortColumn)
        }

        func selectRowForContextMenu(_ row: Int, in tableView: NSTableView) {
            guard row >= 0, row < parent.resources.count else { return }
            let resource = parent.resources[row]
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyImmediateResourceContextMenuSelection(row: row, in: tableView)
            guard resource.id != parent.selectedResourceID else { return }
            DispatchQueue.main.async { [weak self] in
                guard let self,
                      let current = self.parent.resources.first(where: { $0.id == resource.id })
                else { return }
                self.parent.onSelectResource(current)
            }
        }

        func tableViewColumnDidResize(_ notification: Notification) {
            guard let tableColumn = notification.userInfo?["NSTableColumn"] as? NSTableColumn,
                  let column = OperatorResourceColumn(rawValue: tableColumn.identifier.rawValue),
                  column.isUserResizable else { return }
            let width = min(max(tableColumn.width, tableColumn.minWidth), tableColumn.maxWidth)
            tableColumn.width = width
            RuneAppKitColumnWidthStore.shared.setWidth(width, tableID: tableID, columnID: column.rawValue)
            updateTableWidth(on: tableView)
        }

        func makeMenu(forRow row: Int) -> NSMenu? {
            guard row >= 0, row < parent.resources.count else { return nil }
            let resource = parent.resources[row]
            let menu = NSMenu()
            menu.addItem(menuItem(parent.isFavorite(resource) ? "Remove Favorite" : "Favorite Resource", action: #selector(toggleFavoriteFromMenu(_:)), resource: resource))
            menu.addItem(.separator())
            menu.addItem(menuItem("Describe", action: #selector(openDescribeFromMenu(_:)), resource: resource))
            menu.addItem(menuItem("Open YAML", action: #selector(openYAMLFromMenu(_:)), resource: resource))
            menu.addItem(.separator())
            menu.addItem(menuItem("Copy resource name", action: #selector(copyNameFromMenu(_:)), resource: resource))
            menu.addItem(menuItem("Copy family", action: #selector(copyFamilyFromMenu(_:)), resource: resource))
            menu.addItem(menuItem("Copy kind", action: #selector(copyKindFromMenu(_:)), resource: resource))
            if resource.namespace != nil {
                menu.addItem(menuItem("Copy namespace", action: #selector(copyNamespaceFromMenu(_:)), resource: resource))
            }
            menu.addItem(menuItem("Copy status", action: #selector(copyStatusFromMenu(_:)), resource: resource))
            menu.addItem(menuItem("Copy API path", action: #selector(copyAPIPathFromMenu(_:)), resource: resource))
            return menu
        }

        @objc private func toggleFavoriteFromMenu(_ sender: NSMenuItem) {
            withResource(sender, parent.onToggleFavorite)
        }

        func toggleFavoriteForSelectedRow(in tableView: NSTableView) {
            guard tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.resources.count else { return }
            parent.onToggleFavorite(parent.resources[tableView.selectedRow])
        }

        @objc private func toggleFavoriteButton(_ sender: NSButton) {
            guard sender.tag >= 0, sender.tag < parent.resources.count else { return }
            parent.onToggleFavorite(parent.resources[sender.tag])
        }

        @objc private func openDescribeFromMenu(_ sender: NSMenuItem) {
            withResource(sender, parent.onOpenDescribe)
        }

        @objc private func openYAMLFromMenu(_ sender: NSMenuItem) {
            withResource(sender, parent.onOpenYAML)
        }

        @objc private func copyNameFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in copyToClipboard(resource.name) }
        }

        @objc private func copyFamilyFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in copyToClipboard(resource.family) }
        }

        @objc private func copyKindFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in copyToClipboard(resource.kind) }
        }

        @objc private func copyNamespaceFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in
                if let namespace = resource.namespace {
                    copyToClipboard(namespace)
                }
            }
        }

        @objc private func copyStatusFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in copyToClipboard(resource.status) }
        }

        @objc private func copyAPIPathFromMenu(_ sender: NSMenuItem) {
            withResource(sender) { resource in copyToClipboard(resource.apiPath) }
        }

        private func applySelection(on tableView: NSTableView) {
            let selectedRow = parent.resources.firstIndex { $0.id == parent.selectedResourceID }
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            if let selectedRow {
                tableView.selectRowIndexes(IndexSet(integer: selectedRow), byExtendingSelection: false)
            } else {
                tableView.deselectAll(nil)
            }
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.operatorColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = min(
                RuneAppKitResourceListLayout.operatorMaximumContentWidth,
                max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.operatorTrailingBreathingRoom)
            )
            let storedFamily = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: OperatorResourceColumn.family.rawValue)
            let familyWidth = min(max(storedFamily ?? widths.family, OperatorResourceColumn.family.minimumWidth), OperatorResourceColumn.family.maximumWidth)
            let storedKind = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: OperatorResourceColumn.kind.rawValue)
            let kindWidth = min(max(storedKind ?? widths.kind, OperatorResourceColumn.kind.minimumWidth), OperatorResourceColumn.kind.maximumWidth)
            let storedNamespace = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: OperatorResourceColumn.namespace.rawValue)
            let namespaceWidth = min(max(storedNamespace ?? widths.namespace, OperatorResourceColumn.namespace.minimumWidth), OperatorResourceColumn.namespace.maximumWidth)
            let storedStatus = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: OperatorResourceColumn.status.rawValue)
            let statusWidth = min(max(storedStatus ?? widths.status, OperatorResourceColumn.status.minimumWidth), OperatorResourceColumn.status.maximumWidth)
            let storedPrinterColumns = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: OperatorResourceColumn.printerColumns.rawValue)
            let printerColumnsWidth = parent.showsPrinterColumns
                ? min(max(storedPrinterColumns ?? widths.printerColumns, OperatorResourceColumn.printerColumns.minimumWidth), OperatorResourceColumn.printerColumns.maximumWidth)
                : 0
            let storedAPIPath = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: OperatorResourceColumn.apiPath.rawValue)
            let apiPathWidth = min(max(storedAPIPath ?? widths.apiPath, OperatorResourceColumn.apiPath.minimumWidth), OperatorResourceColumn.apiPath.maximumWidth)
            let dynamicNameWidth = max(
                RuneAppKitResourceListLayout.operatorMinimumNameColumnWidth,
                projectedWidth - familyWidth - kindWidth - namespaceWidth - statusWidth - printerColumnsWidth - apiPathWidth - widths.favorite
            )
            let storedName = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: OperatorResourceColumn.name.rawValue)
            let nameWidth = min(max(storedName ?? dynamicNameWidth, OperatorResourceColumn.name.minimumWidth), OperatorResourceColumn.name.maximumWidth)
            for tableColumn in tableView.tableColumns {
                guard let column = OperatorResourceColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                switch column {
                case .name: tableColumn.width = nameWidth
                case .family: tableColumn.width = familyWidth
                case .kind: tableColumn.width = kindWidth
                case .namespace: tableColumn.width = namespaceWidth
                case .status: tableColumn.width = statusWidth
                case .printerColumns:
                    tableColumn.isHidden = !parent.showsPrinterColumns
                    tableColumn.width = printerColumnsWidth
                case .apiPath: tableColumn.width = apiPathWidth
                case .favorite: tableColumn.width = widths.favorite
                }
            }
            updateTableWidth(on: tableView)
        }

        func resetColumnWidth(_ columnID: String) {
            RuneAppKitColumnWidthStore.shared.removeWidth(tableID: tableID, columnID: columnID)
            updateColumnWidths()
        }

        private func updateTableWidth(on tableView: NSTableView?) {
            RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)
        }

        private func updateOperatorResourceSortIndicator(on tableView: NSTableView) {
            for tableColumn in tableView.tableColumns {
                guard let column = OperatorResourceColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                if let headerCell = tableColumn.headerCell as? RuneAppKitResourceTableHeaderCell {
                    headerCell.configure(
                        title: column.title,
                        isSorted: column.sortColumn == parent.sortColumn,
                        ascending: parent.sortAscending,
                        reservesSortIndicator: column.sortColumn != nil
                    )
                }
                if column.sortColumn == parent.sortColumn {
                    let imageName = NSImage.Name(parent.sortAscending ? "NSAscendingSortIndicator" : "NSDescendingSortIndicator")
                    tableView.setIndicatorImage(NSImage(named: imageName), in: tableColumn)
                } else {
                    tableView.setIndicatorImage(nil, in: tableColumn)
                }
            }
            tableView.headerView?.needsDisplay = true
        }

        private func statusColor(for status: String) -> NSColor {
            switch status.lowercased() {
            case let value where value.contains("ready") || value.contains("synced") || value.contains("healthy") || value.contains("true"):
                return .systemGreen
            case let value where value.contains("progress") || value.contains("pending") || value.contains("unknown"):
                return .systemOrange
            case let value where value.contains("error") || value.contains("false") || value.contains("failed"):
                return .systemRed
            default:
                return .secondaryLabelColor
            }
        }

        private func menuItem(_ title: String, action: Selector, resource: OperatorResourceSummary) -> NSMenuItem {
            let item = NSMenuItem(title: title, action: action, keyEquivalent: "")
            item.target = self
            item.representedObject = resource
            return item
        }

        private func withResource(_ sender: NSMenuItem, _ action: (OperatorResourceSummary) -> Void) {
            guard let resource = sender.representedObject as? OperatorResourceSummary else { return }
            action(resource)
        }

        private func copyToClipboard(_ value: String) {
            let pasteboard = NSPasteboard.general
            pasteboard.clearContents()
            pasteboard.setString(value, forType: .string)
        }
    }
}

@MainActor
private final class PodNSTableView: NSTableView {
    weak var coordinator: AppKitPodTableView.Coordinator?

    override func keyDown(with event: NSEvent) {
        if runeAppKitEventIsFavoriteToggle(event) {
            coordinator?.toggleFavoriteForSelectedRow(in: self)
            return
        }
        super.keyDown(with: event)
    }

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        coordinator?.selectRowForContextMenu(row, in: self)
        return coordinator?.makeMenu(forRow: row)
    }
}

@MainActor
private final class DeploymentNSTableView: NSTableView {
    weak var coordinator: AppKitDeploymentListView.Coordinator?

    override func keyDown(with event: NSEvent) {
        if runeAppKitEventIsFavoriteToggle(event) {
            coordinator?.toggleFavoriteForSelectedRow(in: self)
            return
        }
        super.keyDown(with: event)
    }

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        coordinator?.selectRowForContextMenu(row, in: self)
        return coordinator?.makeMenu(forRow: row)
    }
}

@MainActor
private final class ServiceNSTableView: NSTableView {
    weak var coordinator: AppKitServiceListView.Coordinator?

    override func keyDown(with event: NSEvent) {
        if runeAppKitEventIsFavoriteToggle(event) {
            coordinator?.toggleFavoriteForSelectedRow(in: self)
            return
        }
        super.keyDown(with: event)
    }

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        coordinator?.selectRowForContextMenu(row, in: self)
        return coordinator?.makeMenu(forRow: row)
    }
}

@MainActor
private final class GenericResourceNSTableView: NSTableView {
    weak var coordinator: AppKitGenericResourceListView.Coordinator?

    override func keyDown(with event: NSEvent) {
        if runeAppKitEventIsFavoriteToggle(event) {
            coordinator?.toggleFavoriteForSelectedRow(in: self)
            return
        }
        super.keyDown(with: event)
    }

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        coordinator?.selectRowForContextMenu(row, in: self)
        return coordinator?.makeMenu(forRow: row)
    }
}

@MainActor
private final class HelmReleaseNSTableView: NSTableView {
    weak var coordinator: AppKitHelmReleaseListView.Coordinator?

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        coordinator?.selectRowForContextMenu(row, in: self)
        return coordinator?.makeMenu(forRow: row)
    }
}

@MainActor
private final class EventNSTableView: NSTableView {
    weak var coordinator: AppKitEventListView.Coordinator?

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        coordinator?.selectRowForContextMenu(row, in: self)
        return coordinator?.makeMenu(forRow: row)
    }
}

@MainActor
private final class OperatorResourceNSTableView: NSTableView {
    weak var coordinator: AppKitOperatorResourceListView.Coordinator?

    override func keyDown(with event: NSEvent) {
        if runeAppKitEventIsFavoriteToggle(event) {
            coordinator?.toggleFavoriteForSelectedRow(in: self)
            return
        }
        super.keyDown(with: event)
    }

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        coordinator?.selectRowForContextMenu(row, in: self)
        return coordinator?.makeMenu(forRow: row)
    }
}

private func runeAppKitEventIsFavoriteToggle(_ event: NSEvent) -> Bool {
    let disallowedModifiers: NSEvent.ModifierFlags = [.command, .control, .option]
    guard event.modifierFlags.intersection(disallowedModifiers).isEmpty else { return false }
    return event.charactersIgnoringModifiers?.lowercased() == "f"
}

private final class RuneAppKitResourceTableHeaderView: NSTableHeaderView {
    var resizableColumnIdentifiers: Set<String> = []
    var onResetColumn: ((String) -> Void)?
    var horizontalInset: CGFloat = 0

    override func mouseDown(with event: NSEvent) {
        if event.clickCount == 2, let columnID = resizableColumnIdentifier(near: event) {
            onResetColumn?(columnID)
            return
        }
        super.mouseDown(with: event)
    }

    override func draw(_ dirtyRect: NSRect) {
        NSColor.clear.setFill()
        dirtyRect.fill()
        drawHeaderBackground(in: dirtyRect)

        if let tableView {
            for columnIndex in 0..<tableView.numberOfColumns {
                let rect = headerRect(ofColumn: columnIndex)
                guard rect.intersects(dirtyRect) else { continue }
                let column = tableView.tableColumns[columnIndex]
                column.headerCell.draw(withFrame: rect, in: self)
                if let indicatorImage = tableView.indicatorImage(in: column) {
                    drawSortIndicator(indicatorImage, in: rect, column: column)
                }
                drawColumnDivider(at: rect.maxX, isResizable: resizableColumnIdentifiers.contains(column.identifier.rawValue))
            }
        }

        RuneAppKitResourceTableTheme.current.headerDivider.setFill()
        let renderedWidth = tableView.map(RuneAppKitResourceTableStyle.renderedTableWidth(in:)) ?? bounds.width
        NSRect(
            x: bounds.minX + horizontalInset,
            y: bounds.minY,
            width: max(0, min(bounds.width, renderedWidth) - (horizontalInset * 2)),
            height: 1
        ).fill()
    }

    private func drawHeaderBackground(in dirtyRect: NSRect) {
        let renderedWidth = tableView.map(RuneAppKitResourceTableStyle.renderedTableWidth(in:)) ?? bounds.width
        let rect = NSRect(
            x: bounds.minX + horizontalInset,
            y: bounds.minY + 1,
            width: max(0, min(bounds.width, renderedWidth) - (horizontalInset * 2)),
            height: max(0, bounds.height - 2)
        )
        guard rect.intersects(dirtyRect), rect.width > 0, rect.height > 0 else { return }
        RuneAppKitResourceTableTheme.current.headerFill.setFill()
        NSBezierPath(
            roundedRect: rect,
            xRadius: RuneUILayoutMetrics.compactGlyphCornerRadius,
            yRadius: RuneUILayoutMetrics.compactGlyphCornerRadius
        ).fill()
    }

    private func resizableColumnIdentifier(near event: NSEvent) -> String? {
        guard let tableView else { return nil }
        let point = convert(event.locationInWindow, from: nil)
        for columnIndex in 0..<tableView.tableColumns.count {
            let column = tableView.tableColumns[columnIndex]
            guard resizableColumnIdentifiers.contains(column.identifier.rawValue) else { continue }
            let dividerX = headerRect(ofColumn: columnIndex).maxX
            if abs(point.x - dividerX) <= 7 {
                return column.identifier.rawValue
            }
        }
        return nil
    }

    private func drawColumnDivider(at x: CGFloat, isResizable: Bool) {
        guard x > bounds.minX + horizontalInset, x < bounds.maxX - horizontalInset else { return }
        let tableTheme = RuneAppKitResourceTableTheme.current
        let color = isResizable ? tableTheme.columnDividerResizable : tableTheme.columnDivider
        color.setFill()
        NSRect(x: x.rounded(.down), y: bounds.minY + 4, width: 1, height: max(0, bounds.height - 8)).fill()
    }

    private func drawSortIndicator(_ image: NSImage, in rect: NSRect, column: NSTableColumn) {
        guard column.identifier.rawValue != PodColumn.selection.rawValue,
              column.identifier.rawValue != PodColumn.favorite.rawValue else { return }
        let imageSize = RuneAppKitResourceListLayout.sortIndicatorSize
        let x = rect.maxX - RuneAppKitResourceTableStyle.contentTrailingInset - imageSize.width
        let y = rect.midY - (imageSize.height / 2)
        image.draw(in: NSRect(x: x, y: y, width: imageSize.width, height: imageSize.height))
    }
}

private final class RuneAppKitResourceTableHeaderCell: NSTableHeaderCell {
    private let textAlignment: NSTextAlignment
    private var baseTitle = ""
    private var isSorted = false
    private var sortAscending = true
    private var reservesSortIndicator = false

    init(alignment: NSTextAlignment) {
        self.textAlignment = alignment
        super.init(textCell: "")
        self.alignment = alignment
    }

    required init(coder: NSCoder) {
        self.textAlignment = .left
        super.init(coder: coder)
    }

    func configure(title: String, isSorted: Bool, ascending: Bool, reservesSortIndicator: Bool) {
        self.baseTitle = title
        self.isSorted = isSorted
        self.sortAscending = ascending
        self.reservesSortIndicator = reservesSortIndicator
        stringValue = title
    }

    override func draw(withFrame cellFrame: NSRect, in controlView: NSView) {
        drawInterior(withFrame: cellFrame, in: controlView)
    }

    override func drawInterior(withFrame cellFrame: NSRect, in controlView: NSView) {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = textAlignment
        paragraph.lineBreakMode = .byTruncatingTail
        let attributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold),
            .foregroundColor: RuneAppKitResourceTableTheme.current.headerText,
            .paragraphStyle: paragraph
        ]
        let title = NSAttributedString(string: baseTitle.isEmpty ? stringValue : baseTitle, attributes: attributes)
        let leadingInset = RuneAppKitResourceTableStyle.contentLeadingInset
        let trailingInset = RuneAppKitResourceTableStyle.contentTrailingInset
            + (reservesSortIndicator ? RuneAppKitResourceListLayout.sortIndicatorSize.width + RuneAppKitResourceTableStyle.sortIndicatorGap : 0)
        let textSize = title.size()
        let textRect = NSRect(
            x: cellFrame.minX + leadingInset,
            y: cellFrame.midY - (textSize.height / 2),
            width: max(0, cellFrame.width - leadingInset - trailingInset),
            height: textSize.height
        )
        title.draw(in: textRect)
    }
}

private final class RuneAppKitResourceTableRowView: NSTableRowView {
    private let horizontalInset: CGFloat

    init(horizontalInset: CGFloat = 4) {
        self.horizontalInset = horizontalInset
        super.init(frame: .zero)
    }

    required init?(coder: NSCoder) {
        self.horizontalInset = 4
        super.init(coder: coder)
    }

    override var isEmphasized: Bool {
        get { false }
        set {}
    }

    override func drawSelection(in dirtyRect: NSRect) {}

    override func drawBackground(in dirtyRect: NSRect) {
        let contentWidth = (superview as? NSTableView)
            .map(RuneAppKitResourceTableStyle.renderedTableWidth(in:)) ?? bounds.width
        let rowRect = NSRect(
            x: bounds.minX + horizontalInset,
            y: bounds.minY + 1,
            width: max(0, min(bounds.width, contentWidth) - (horizontalInset * 2)),
            height: max(0, bounds.height - 2)
        )
        let path = NSBezierPath(roundedRect: rowRect, xRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, yRadius: RuneUILayoutMetrics.compactGlyphCornerRadius)
        let tableTheme = RuneAppKitResourceTableTheme.current
        let fill = isSelected
            ? tableTheme.selectedRowFill
            : tableTheme.rowFill
        fill.setFill()
        path.fill()

        if !isSelected {
            tableTheme.rowStroke.setStroke()
            path.lineWidth = 1
            path.stroke()
        }
    }
}

@MainActor
private final class RuneAppKitResourceListScrollView: NSScrollView {
    var onVisibleWidthChanged: (() -> Void)?
    private var lastVisibleWidth: CGFloat = -1
    private var isSendingVisibleWidthChange = false

    override func layout() {
        super.layout()
        notifyVisibleWidthChangedIfNeeded()
    }

    override func setFrameSize(_ newSize: NSSize) {
        super.setFrameSize(newSize)
        notifyVisibleWidthChangedIfNeeded()
    }

    override func setBoundsSize(_ newSize: NSSize) {
        super.setBoundsSize(newSize)
        notifyVisibleWidthChangedIfNeeded()
    }

    override func resizeSubviews(withOldSize oldSize: NSSize) {
        super.resizeSubviews(withOldSize: oldSize)
        notifyVisibleWidthChangedIfNeeded()
    }

    override func viewWillStartLiveResize() {
        super.viewWillStartLiveResize()
        notifyVisibleWidthChangedIfNeeded(force: true)
    }

    override func viewDidEndLiveResize() {
        super.viewDidEndLiveResize()
        notifyVisibleWidthChangedIfNeeded(force: true)
    }

    private func notifyVisibleWidthChangedIfNeeded(force: Bool = false) {
        guard !isSendingVisibleWidthChange else { return }
        let visibleWidth = contentView.bounds.width.rounded(.toNearestOrAwayFromZero)
        guard force || abs(visibleWidth - lastVisibleWidth) >= 1 else { return }
        lastVisibleWidth = visibleWidth
        isSendingVisibleWidthChange = true
        onVisibleWidthChanged?()
        isSendingVisibleWidthChange = false
    }
}

private final class RuneAppKitCenteredLabelCell: NSView {
    init(
        text: String,
        font: NSFont,
        alignment: NSTextAlignment,
        tooltip: String? = nil,
        lineBreakMode: NSLineBreakMode = .byTruncatingTail
    ) {
        super.init(frame: .zero)
        let label = NSTextField(labelWithString: text)
        label.font = font
        label.alignment = alignment
        label.lineBreakMode = lineBreakMode
        label.maximumNumberOfLines = 1
        label.toolTip = tooltip
        label.translatesAutoresizingMaskIntoConstraints = false
        addSubview(label)
        NSLayoutConstraint.activate([
            label.leadingAnchor.constraint(equalTo: leadingAnchor, constant: alignment == .left ? RuneAppKitResourceTableStyle.contentLeadingInset : 0),
            label.trailingAnchor.constraint(equalTo: trailingAnchor, constant: alignment == .right ? -RuneAppKitResourceTableStyle.contentTrailingInset : 0),
            label.centerYAnchor.constraint(equalTo: centerYAnchor)
        ])
    }

    required init?(coder: NSCoder) {
        nil
    }
}

private final class RuneAppKitPillCell: NSView {
    init(text: String, color: NSColor) {
        super.init(frame: .zero)
        let pill = RuneAppKitResourceStatusPillView(text: text, color: color, minimumWidth: 34)
        pill.translatesAutoresizingMaskIntoConstraints = false
        addSubview(pill)
        NSLayoutConstraint.activate([
            pill.centerXAnchor.constraint(equalTo: centerXAnchor),
            pill.centerYAnchor.constraint(equalTo: centerYAnchor)
        ])
    }

    required init?(coder: NSCoder) {
        nil
    }
}

private final class RuneAppKitFavoriteButtonCell: NSView {
    init(isFavorite: Bool, row: Int, target: AnyObject, action: Selector) {
        super.init(frame: .zero)
        let button = NSButton(
            image: NSImage(systemSymbolName: isFavorite ? "star.fill" : "star", accessibilityDescription: "Favorite Resource") ?? NSImage(),
            target: target,
            action: action
        )
        button.tag = row
        button.isBordered = false
        button.imagePosition = .imageOnly
        button.contentTintColor = isFavorite ? .systemYellow : .secondaryLabelColor
        button.symbolConfiguration = .init(pointSize: NSFont.smallSystemFontSize, weight: .semibold)
        button.toolTip = isFavorite ? "Remove favorite" : "Favorite resource"
        button.setAccessibilityLabel(isFavorite ? "Remove Favorite" : "Favorite Resource")
        button.translatesAutoresizingMaskIntoConstraints = false
        addSubview(button)
        NSLayoutConstraint.activate([
            button.centerXAnchor.constraint(equalTo: centerXAnchor),
            button.centerYAnchor.constraint(equalTo: centerYAnchor),
            button.widthAnchor.constraint(equalToConstant: 22),
            button.heightAnchor.constraint(equalToConstant: 22)
        ])
    }

    required init?(coder: NSCoder) {
        nil
    }
}

private final class RuneAppKitSingleLineResourceCell: NSView {
    init(title: String, badgeText: String, badgeColor: NSColor, isFavorite: Bool) {
        super.init(frame: .zero)

        let titleLabel = NSTextField(labelWithString: title)
        titleLabel.font = .systemFont(ofSize: NSFont.systemFontSize, weight: .medium)
        titleLabel.lineBreakMode = .byTruncatingMiddle
        titleLabel.maximumNumberOfLines = 1
        titleLabel.toolTip = title

        let badge = RuneAppKitResourceStatusPillView(text: badgeText, color: badgeColor, minimumWidth: 34)
        let favorite = NSImageView()
        favorite.image = NSImage(systemSymbolName: "star.fill", accessibilityDescription: "Favorite")
        favorite.contentTintColor = .systemYellow
        favorite.symbolConfiguration = .init(pointSize: NSFont.smallSystemFontSize, weight: .semibold)
        favorite.imageScaling = .scaleProportionallyDown
        favorite.setAccessibilityLabel("Favorite")
        favorite.isHidden = !isFavorite

        for view in [titleLabel, badge, favorite] {
            view.translatesAutoresizingMaskIntoConstraints = false
            addSubview(view)
        }

        NSLayoutConstraint.activate([
            titleLabel.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 10),
            titleLabel.centerYAnchor.constraint(equalTo: centerYAnchor),
            badge.trailingAnchor.constraint(equalTo: favorite.leadingAnchor, constant: -8),
            badge.centerYAnchor.constraint(equalTo: centerYAnchor),
            favorite.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -12),
            favorite.centerYAnchor.constraint(equalTo: centerYAnchor),
            favorite.widthAnchor.constraint(equalToConstant: 14),
            favorite.heightAnchor.constraint(equalToConstant: 14),
            titleLabel.trailingAnchor.constraint(lessThanOrEqualTo: badge.leadingAnchor, constant: -12)
        ])
    }

    required init?(coder: NSCoder) {
        nil
    }
}

private final class RuneAppKitResourceStatusPillView: NSView {
    private let label: NSTextField
    private let color: NSColor
    private let minimumWidth: CGFloat

    init(text: String, color: NSColor, minimumWidth: CGFloat = 86) {
        self.color = color
        self.minimumWidth = minimumWidth
        self.label = NSTextField(labelWithString: text)
        super.init(frame: .zero)
        wantsLayer = true
        translatesAutoresizingMaskIntoConstraints = false
        label.font = .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold)
        label.textColor = color
        label.alignment = .center
        label.lineBreakMode = .byTruncatingTail
        label.maximumNumberOfLines = 1
        label.translatesAutoresizingMaskIntoConstraints = false
        addSubview(label)
        NSLayoutConstraint.activate([
            label.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 8),
            label.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -8),
            label.topAnchor.constraint(equalTo: topAnchor, constant: 2),
            label.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -2),
            widthAnchor.constraint(greaterThanOrEqualToConstant: minimumWidth)
        ])
    }

    required init?(coder: NSCoder) {
        nil
    }

    override func layout() {
        super.layout()
        layer?.cornerRadius = bounds.height / 2
        layer?.backgroundColor = color.withAlphaComponent(0.22).cgColor
    }
}

private enum DeploymentColumn: String, CaseIterable {
    case name
    case replicas
    case favorite

    @MainActor
    func tableColumn() -> NSTableColumn {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(rawValue))
        let headerCell = RuneAppKitResourceTableHeaderCell(alignment: alignment)
        headerCell.configure(title: title, isSorted: false, ascending: true, reservesSortIndicator: sortColumn != nil)
        column.headerCell = headerCell
        column.title = title
        if let sortColumn {
            column.sortDescriptorPrototype = NSSortDescriptor(key: sortColumn.rawValue, ascending: true)
        }
        column.resizingMask = isUserResizable ? .userResizingMask : []
        column.width = width
        column.minWidth = minimumWidth
        column.maxWidth = maximumWidth
        return column
    }

    var sortColumn: DeploymentListSortColumn? {
        switch self {
        case .name: return .name
        case .replicas: return .replicas
        case .favorite: return nil
        }
    }

    var title: String {
        switch self {
        case .name: return "Name"
        case .replicas: return "Ready"
        case .favorite: return ""
        }
    }

    var alignment: NSTextAlignment {
        switch self {
        case .name, .favorite: return .left
        case .replicas: return .center
        }
    }

    var width: CGFloat {
        switch self {
        case .name: return 320
        case .replicas: return RuneAppKitResourceListLayout.deploymentReplicaColumnWidth
        case .favorite: return PodTableLayout.favoriteColumnWidth
        }
    }

    var minimumWidth: CGFloat {
        switch self {
        case .name: return RuneAppKitResourceListLayout.deploymentMinimumNameColumnWidth
        case .replicas: return RuneAppKitResourceListLayout.deploymentReplicaColumnWidth
        case .favorite: return width
        }
    }

    var maximumWidth: CGFloat {
        switch self {
        case .name: return 10_000
        case .replicas: return 180
        case .favorite: return width
        }
    }

    var isUserResizable: Bool {
        switch self {
        case .favorite: return false
        case .name, .replicas: return true
        }
    }
}

private enum ServiceColumn: String, CaseIterable {
    case name
    case type
    case clusterIP
    case favorite

    @MainActor
    func tableColumn() -> NSTableColumn {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(rawValue))
        let headerCell = RuneAppKitResourceTableHeaderCell(alignment: alignment)
        headerCell.configure(title: title, isSorted: false, ascending: true, reservesSortIndicator: sortColumn != nil)
        column.headerCell = headerCell
        column.title = title
        if let sortColumn {
            column.sortDescriptorPrototype = NSSortDescriptor(key: sortColumn.rawValue, ascending: true)
        }
        column.resizingMask = isUserResizable ? .userResizingMask : []
        column.width = width
        column.minWidth = minimumWidth
        column.maxWidth = maximumWidth
        return column
    }

    var sortColumn: ServiceListSortColumn? {
        switch self {
        case .name: return .name
        case .type: return .type
        case .clusterIP: return .clusterIP
        case .favorite: return nil
        }
    }

    var title: String {
        switch self {
        case .name: return "Name"
        case .type: return "Type"
        case .clusterIP: return "Cluster IP"
        case .favorite: return ""
        }
    }

    var alignment: NSTextAlignment {
        switch self {
        case .name, .favorite: return .left
        case .type: return .center
        case .clusterIP: return .right
        }
    }

    var width: CGFloat {
        switch self {
        case .name: return 300
        case .type: return RuneAppKitResourceListLayout.serviceTypeColumnWidth
        case .clusterIP: return RuneAppKitResourceListLayout.serviceClusterIPColumnWidth
        case .favorite: return RuneAppKitResourceListLayout.serviceFavoriteColumnWidth
        }
    }

    var minimumWidth: CGFloat {
        switch self {
        case .name: return RuneAppKitResourceListLayout.serviceMinimumNameColumnWidth
        case .type: return RuneAppKitResourceListLayout.serviceTypeColumnWidth
        case .clusterIP: return RuneAppKitResourceListLayout.serviceClusterIPColumnWidth
        case .favorite: return width
        }
    }

    var maximumWidth: CGFloat {
        switch self {
        case .name: return 10_000
        case .type: return 220
        case .clusterIP: return 260
        case .favorite: return width
        }
    }

    var isUserResizable: Bool {
        switch self {
        case .favorite: return false
        case .name, .type, .clusterIP: return true
        }
    }
}

private enum GenericResourceColumn: String, CaseIterable {
    case selection
    case name
    case primary
    case secondary
    case namespace
    case favorite

    @MainActor
    func tableColumn() -> NSTableColumn {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(rawValue))
        let headerCell = RuneAppKitResourceTableHeaderCell(alignment: alignment)
        headerCell.configure(title: title, isSorted: false, ascending: true, reservesSortIndicator: sortColumn != nil)
        column.headerCell = headerCell
        column.title = title
        if let sortColumn {
            column.sortDescriptorPrototype = NSSortDescriptor(key: sortColumn.rawValue, ascending: true)
        }
        column.resizingMask = isUserResizable ? .userResizingMask : []
        column.width = width
        column.minWidth = minimumWidth
        column.maxWidth = maximumWidth
        return column
    }

    var sortColumn: GenericResourceListSortColumn? {
        switch self {
        case .name: return .name
        case .primary: return .primary
        case .secondary: return .secondary
        case .namespace: return .namespace
        case .selection, .favorite: return nil
        }
    }

    var title: String {
        switch self {
        case .selection: return ""
        case .name: return "Name"
        case .primary: return "Detail"
        case .secondary: return "Info"
        case .namespace: return "Namespace"
        case .favorite: return ""
        }
    }

    var alignment: NSTextAlignment {
        switch self {
        case .primary: return .right
        case .secondary, .namespace, .selection, .favorite, .name: return .left
        }
    }

    var width: CGFloat {
        switch self {
        case .selection: return RuneAppKitResourceListLayout.genericSelectionColumnWidth
        case .name: return 260
        case .primary: return RuneAppKitResourceListLayout.genericPrimaryColumnWidth
        case .secondary: return RuneAppKitResourceListLayout.genericSecondaryColumnWidth
        case .namespace: return RuneAppKitResourceListLayout.genericNamespaceColumnWidth
        case .favorite: return RuneAppKitResourceListLayout.genericFavoriteColumnWidth
        }
    }

    var minimumWidth: CGFloat {
        switch self {
        case .name: return RuneAppKitResourceListLayout.genericMinimumNameColumnWidth
        case .primary:
            return RuneAppKitResourceListLayout.genericPrimaryMinimumColumnWidth
        case .secondary:
            return RuneAppKitResourceListLayout.genericSecondaryMinimumColumnWidth
        case .namespace:
            return RuneAppKitResourceListLayout.genericNamespaceMinimumColumnWidth
        case .selection, .favorite:
            return width
        }
    }

    var maximumWidth: CGFloat {
        switch self {
        case .name: return 10_000
        case .primary: return 260
        case .secondary: return 360
        case .namespace: return 280
        case .selection, .favorite: return width
        }
    }

    var isUserResizable: Bool {
        switch self {
        case .selection, .favorite: return false
        case .name, .primary, .secondary, .namespace: return true
        }
    }
}

private enum HelmReleaseColumn: String, CaseIterable {
    case name
    case status
    case namespace
    case revision
    case chart
    case appVersion

    @MainActor
    func tableColumn() -> NSTableColumn {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(rawValue))
        let headerCell = RuneAppKitResourceTableHeaderCell(alignment: alignment)
        headerCell.configure(title: title, isSorted: false, ascending: true, reservesSortIndicator: true)
        column.headerCell = headerCell
        column.title = title
        column.sortDescriptorPrototype = NSSortDescriptor(key: sortColumn.rawValue, ascending: true)
        column.resizingMask = isUserResizable ? .userResizingMask : []
        column.width = width
        column.minWidth = minimumWidth
        column.maxWidth = maximumWidth
        return column
    }

    var sortColumn: HelmReleaseListSortColumn {
        switch self {
        case .name: return .name
        case .status: return .status
        case .namespace: return .namespace
        case .revision: return .revision
        case .chart: return .chart
        case .appVersion: return .appVersion
        }
    }

    var title: String {
        switch self {
        case .name: return "Name"
        case .status: return "Status"
        case .namespace: return "Namespace"
        case .revision: return "Rev"
        case .chart: return "Chart"
        case .appVersion: return "App"
        }
    }

    var alignment: NSTextAlignment {
        switch self {
        case .name, .namespace, .chart, .appVersion: return .left
        case .status: return .center
        case .revision: return .right
        }
    }

    var width: CGFloat {
        switch self {
        case .name: return 260
        case .status: return RuneAppKitResourceListLayout.helmStatusColumnWidth
        case .namespace: return RuneAppKitResourceListLayout.helmNamespaceColumnWidth
        case .revision: return RuneAppKitResourceListLayout.helmRevisionColumnWidth
        case .chart: return RuneAppKitResourceListLayout.helmChartColumnWidth
        case .appVersion: return RuneAppKitResourceListLayout.helmAppVersionColumnWidth
        }
    }

    var minimumWidth: CGFloat {
        switch self {
        case .name: return RuneAppKitResourceListLayout.helmMinimumNameColumnWidth
        case .status:
            return RuneAppKitResourceListLayout.helmStatusMinimumColumnWidth
        case .namespace:
            return RuneAppKitResourceListLayout.helmNamespaceMinimumColumnWidth
        case .revision:
            return RuneAppKitResourceListLayout.helmRevisionMinimumColumnWidth
        case .chart:
            return RuneAppKitResourceListLayout.helmChartMinimumColumnWidth
        case .appVersion:
            return RuneAppKitResourceListLayout.helmAppVersionMinimumColumnWidth
        }
    }

    var maximumWidth: CGFloat {
        switch self {
        case .name: return 10_000
        case .status: return 180
        case .namespace: return 300
        case .revision: return 120
        case .chart: return 340
        case .appVersion: return 220
        }
    }

    var isUserResizable: Bool {
        true
    }
}

private enum EventColumn: String, CaseIterable {
    case reason
    case type
    case object
    case namespace
    case lastSeen
    case message

    @MainActor
    func tableColumn() -> NSTableColumn {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(rawValue))
        let headerCell = RuneAppKitResourceTableHeaderCell(alignment: alignment)
        headerCell.configure(title: title, isSorted: false, ascending: true, reservesSortIndicator: sortColumn != nil)
        column.headerCell = headerCell
        column.title = title
        if let sortColumn {
            column.sortDescriptorPrototype = NSSortDescriptor(key: sortColumn.rawValue, ascending: true)
        }
        column.resizingMask = isUserResizable ? .userResizingMask : []
        column.width = width
        column.minWidth = minimumWidth
        column.maxWidth = maximumWidth
        return column
    }

    var sortColumn: EventListSortColumn? {
        switch self {
        case .reason: return .reason
        case .type: return .type
        case .object: return .object
        case .namespace: return .namespace
        case .lastSeen: return .lastSeen
        case .message: return nil
        }
    }

    var title: String {
        switch self {
        case .reason: return "Reason"
        case .type: return "Type"
        case .object: return "Object"
        case .namespace: return "Namespace"
        case .lastSeen: return "Last Seen"
        case .message: return "Message"
        }
    }

    var alignment: NSTextAlignment {
        switch self {
        case .type: return .center
        case .lastSeen: return .right
        case .reason, .object, .namespace, .message: return .left
        }
    }

    var width: CGFloat {
        switch self {
        case .reason: return 220
        case .type: return RuneAppKitResourceListLayout.eventTypeColumnWidth
        case .object: return RuneAppKitResourceListLayout.eventObjectColumnWidth
        case .namespace: return RuneAppKitResourceListLayout.eventNamespaceColumnWidth
        case .lastSeen: return RuneAppKitResourceListLayout.eventLastSeenColumnWidth
        case .message: return RuneAppKitResourceListLayout.eventMessageColumnWidth
        }
    }

    var minimumWidth: CGFloat {
        switch self {
        case .reason:
            return RuneAppKitResourceListLayout.eventMinimumReasonColumnWidth
        case .type:
            return RuneAppKitResourceListLayout.eventTypeMinimumColumnWidth
        case .object:
            return RuneAppKitResourceListLayout.eventObjectMinimumColumnWidth
        case .namespace:
            return RuneAppKitResourceListLayout.eventNamespaceMinimumColumnWidth
        case .lastSeen:
            return RuneAppKitResourceListLayout.eventLastSeenMinimumColumnWidth
        case .message:
            return RuneAppKitResourceListLayout.eventMessageMinimumColumnWidth
        }
    }

    var maximumWidth: CGFloat {
        switch self {
        case .reason: return 10_000
        case .type: return 180
        case .object: return 360
        case .namespace: return 260
        case .lastSeen: return 260
        case .message: return 520
        }
    }

    var isUserResizable: Bool {
        true
    }
}

private enum OperatorResourceColumn: String, CaseIterable {
    case name
    case family
    case kind
    case namespace
    case status
    case printerColumns
    case apiPath
    case favorite

    @MainActor
    func tableColumn() -> NSTableColumn {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(rawValue))
        let headerCell = RuneAppKitResourceTableHeaderCell(alignment: alignment)
        headerCell.configure(title: title, isSorted: false, ascending: true, reservesSortIndicator: sortColumn != nil)
        column.headerCell = headerCell
        column.title = title
        if let sortColumn {
            column.sortDescriptorPrototype = NSSortDescriptor(key: sortColumn.rawValue, ascending: true)
        }
        column.resizingMask = isUserResizable ? .userResizingMask : []
        column.width = width
        column.minWidth = minimumWidth
        column.maxWidth = maximumWidth
        return column
    }

    var sortColumn: OperatorResourceListSortColumn? {
        switch self {
        case .name: return .name
        case .family: return .family
        case .kind: return .kind
        case .namespace: return .namespace
        case .status: return .status
        case .printerColumns: return nil
        case .apiPath: return .apiPath
        case .favorite: return nil
        }
    }

    var title: String {
        switch self {
        case .name: return "Name"
        case .family: return "Family"
        case .kind: return "Kind"
        case .namespace: return "Namespace"
        case .status: return "Status"
        case .printerColumns: return "Columns"
        case .apiPath: return "API Path"
        case .favorite: return ""
        }
    }

    var alignment: NSTextAlignment {
        switch self {
        case .status: return .center
        case .favorite, .name, .family, .kind, .namespace, .printerColumns, .apiPath: return .left
        }
    }

    var width: CGFloat {
        switch self {
        case .name: return 260
        case .family: return RuneAppKitResourceListLayout.operatorFamilyColumnWidth
        case .kind: return RuneAppKitResourceListLayout.operatorKindColumnWidth
        case .namespace: return RuneAppKitResourceListLayout.operatorNamespaceColumnWidth
        case .status: return RuneAppKitResourceListLayout.operatorStatusColumnWidth
        case .printerColumns: return RuneAppKitResourceListLayout.operatorPrinterColumnsColumnWidth
        case .apiPath: return RuneAppKitResourceListLayout.operatorAPIPathColumnWidth
        case .favorite: return RuneAppKitResourceListLayout.operatorFavoriteColumnWidth
        }
    }

    var minimumWidth: CGFloat {
        switch self {
        case .name:
            return RuneAppKitResourceListLayout.operatorMinimumNameColumnWidth
        case .family:
            return RuneAppKitResourceListLayout.operatorFamilyMinimumColumnWidth
        case .kind:
            return RuneAppKitResourceListLayout.operatorKindMinimumColumnWidth
        case .namespace:
            return RuneAppKitResourceListLayout.operatorNamespaceMinimumColumnWidth
        case .status:
            return RuneAppKitResourceListLayout.operatorStatusMinimumColumnWidth
        case .printerColumns:
            return 120
        case .apiPath:
            return RuneAppKitResourceListLayout.operatorAPIPathMinimumColumnWidth
        case .favorite:
            return width
        }
    }

    var maximumWidth: CGFloat {
        switch self {
        case .name: return 10_000
        case .family: return 260
        case .kind: return 300
        case .namespace: return 260
        case .status: return 220
        case .printerColumns: return 280
        case .apiPath: return 520
        case .favorite: return width
        }
    }

    var isUserResizable: Bool {
        switch self {
        case .favorite: return false
        case .name, .family, .kind, .namespace, .status, .printerColumns, .apiPath: return true
        }
    }
}

private enum PodColumn: String, CaseIterable {
    case selection
    case name
    case cpu
    case memory
    case restarts
    case age
    case status
    case favorite

    init?(sortColumn: PodListSortColumn) {
        switch sortColumn {
        case .name: self = .name
        case .cpu: self = .cpu
        case .memory: self = .memory
        case .restarts: self = .restarts
        case .age: self = .age
        case .status: self = .status
        }
    }

    var sortColumn: PodListSortColumn? {
        switch self {
        case .name: return .name
        case .cpu: return .cpu
        case .memory: return .memory
        case .restarts: return .restarts
        case .age: return .age
        case .status: return .status
        case .selection, .favorite: return nil
        }
    }

    @MainActor
    func tableColumn(nameColumnWidth: CGFloat) -> NSTableColumn {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(rawValue))
        let headerCell = RuneAppKitResourceTableHeaderCell(alignment: alignment)
        headerCell.configure(title: title, isSorted: false, ascending: true, reservesSortIndicator: sortColumn != nil)
        column.headerCell = headerCell
        column.title = title
        if let sortColumn {
            column.sortDescriptorPrototype = NSSortDescriptor(key: sortColumn.rawValue, ascending: true)
        }
        column.width = width(nameColumnWidth: nameColumnWidth)
        column.minWidth = minWidth
        column.maxWidth = maxWidth
        column.resizingMask = isUserResizable ? .userResizingMask : []
        return column
    }

    var title: String {
        switch self {
        case .selection: return ""
        case .name: return "Name"
        case .cpu: return "CPU"
        case .memory: return "MEM"
        case .restarts: return "Restarts"
        case .age: return "Age"
        case .status: return "Status"
        case .favorite: return ""
        }
    }

    private var alignment: NSTextAlignment {
        switch self {
        case .cpu, .memory, .restarts, .age:
            return .right
        case .status:
            return .center
        case .selection, .favorite, .name:
            return .left
        }
    }

    func width(nameColumnWidth: CGFloat) -> CGFloat {
        switch self {
        case .selection: return PodTableLayout.selectionColumnWidth
        case .name: return PodTableLayout.clampedNameColumnWidth(nameColumnWidth)
        case .cpu: return PodTableLayout.cpuWidth
        case .memory: return PodTableLayout.memoryWidth
        case .restarts: return PodTableLayout.restartsWidth
        case .age: return PodTableLayout.ageWidth
        case .status: return PodTableLayout.statusTotalWidth
        case .favorite: return PodTableLayout.favoriteColumnWidth
        }
    }

    private var minWidth: CGFloat {
        switch self {
        case .name: return PodTableLayout.nameColumnMinimumWidth
        case .cpu, .memory, .restarts, .age, .status:
            return RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: title, reservesSortIndicator: true)
        case .selection, .favorite:
            return width(nameColumnWidth: PodTableLayout.nameColumnDefaultWidth)
        }
    }

    private var maxWidth: CGFloat {
        switch self {
        case .name: return PodTableLayout.nameColumnMaximumWidth
        case .cpu: return 160
        case .memory: return 180
        case .restarts: return 180
        case .age: return 140
        case .status: return 240
        case .selection, .favorite:
            return width(nameColumnWidth: PodTableLayout.nameColumnDefaultWidth)
        }
    }

    var isUserResizable: Bool {
        switch self {
        case .selection, .favorite: return false
        case .name, .cpu, .memory, .restarts, .age, .status: return true
        }
    }
}
