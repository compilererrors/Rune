import AppKit
import RuneCore
import SwiftUI

enum RuneAppKitResourceListLayout {
    static let sortIndicatorReservedWidth: CGFloat = 22
    static let sortIndicatorSize = NSSize(width: 8, height: 8)
    static let resourceMaximumContentWidth: CGFloat = 1_200
    static let deploymentReplicaColumnWidth: CGFloat = 88
    static let deploymentFavoriteColumnWidth: CGFloat = PodTableLayout.favoriteColumnWidth
    static let deploymentMinimumNameColumnWidth: CGFloat = 260
    static let deploymentTrailingBreathingRoom: CGFloat = 14
    static let deploymentMaximumContentWidth = resourceMaximumContentWidth
    static let serviceTypeColumnWidth: CGFloat = 140
    static let serviceClusterIPColumnWidth: CGFloat = 172
    static let serviceFavoriteColumnWidth = PodTableLayout.favoriteColumnWidth
    static let serviceMinimumNameColumnWidth: CGFloat = 260
    static let serviceTrailingBreathingRoom: CGFloat = 14
    static let serviceMaximumContentWidth = resourceMaximumContentWidth
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
    static let genericMaximumContentWidth = resourceMaximumContentWidth
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
    static let helmMaximumContentWidth = resourceMaximumContentWidth

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
private enum RuneAppKitResourceTableStyle {
    static let rowHeight: CGFloat = 34
    static let rowGap: CGFloat = 4
    static let rowHorizontalInset: CGFloat = 6
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

        let scrollView = NSScrollView()
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.documentView = tableView
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? PodNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
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

        func tableViewColumnDidResize(_ notification: Notification) {
            guard let tableColumn = notification.userInfo?["NSTableColumn"] as? NSTableColumn,
                  let column = PodColumn(rawValue: tableColumn.identifier.rawValue),
                  column.isUserResizable else { return }
            let width = min(max(tableColumn.width, tableColumn.minWidth), tableColumn.maxWidth)
            tableColumn.width = width
            RuneAppKitColumnWidthStore.shared.setWidth(width, tableID: tableID, columnID: column.rawValue)

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
            menu.addItem(.separator())
            menu.addItem(menuItem("Delete Pod", action: #selector(deleteFromMenu(_:)), pod: pod, isEnabled: parent.canApplyClusterMutations))
            return menu
        }

        @objc private func toggleFavoriteFromMenu(_ sender: NSMenuItem) {
            withPod(sender, parent.onToggleFavorite)
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
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? DeploymentNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
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
            menu.addItem(.separator())
            menu.addItem(menuItem("Delete Deployment", action: #selector(deleteFromMenu(_:)), deployment: deployment, isEnabled: parent.canApplyClusterMutations))
            return menu
        }

        @objc private func toggleFavoriteFromMenu(_ sender: NSMenuItem) {
            withDeployment(sender, parent.onToggleFavorite)
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
            guard let tableView else { return }
            let tableWidth = tableView.tableColumns.reduce(CGFloat(0)) { $0 + $1.width }
            if abs(tableView.frame.width - tableWidth) >= 1 {
                tableView.setFrameSize(NSSize(width: tableWidth, height: tableView.frame.height))
            }
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
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? ServiceNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
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
            menu.addItem(.separator())
            menu.addItem(menuItem("Delete Service", action: #selector(deleteFromMenu(_:)), service: service, isEnabled: parent.canApplyClusterMutations))
            return menu
        }

        @objc private func toggleFavoriteFromMenu(_ sender: NSMenuItem) {
            withService(sender, parent.onToggleFavorite)
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
            guard let tableView else { return }
            let tableWidth = tableView.tableColumns.reduce(CGFloat(0)) { $0 + $1.width }
            if abs(tableView.frame.width - tableWidth) >= 1 {
                tableView.setFrameSize(NSSize(width: tableWidth, height: tableView.frame.height))
            }
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
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? GenericResourceNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
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
            guard let tableView else { return }
            let tableWidth = tableView.tableColumns.reduce(CGFloat(0)) { $0 + $1.width }
            if abs(tableView.frame.width - tableWidth) >= 1 {
                tableView.setFrameSize(NSSize(width: tableWidth, height: tableView.frame.height))
            }
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
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = scrollView.documentView as? HelmReleaseNSTableView else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
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
            guard let tableView else { return }
            let tableWidth = tableView.tableColumns.reduce(CGFloat(0)) { $0 + $1.width }
            if abs(tableView.frame.width - tableWidth) >= 1 {
                tableView.setFrameSize(NSSize(width: tableWidth, height: tableView.frame.height))
            }
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

@MainActor
private final class PodNSTableView: NSTableView {
    weak var coordinator: AppKitPodTableView.Coordinator?

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        return coordinator?.makeMenu(forRow: row)
    }
}

@MainActor
private final class DeploymentNSTableView: NSTableView {
    weak var coordinator: AppKitDeploymentListView.Coordinator?

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        return coordinator?.makeMenu(forRow: row)
    }
}

@MainActor
private final class ServiceNSTableView: NSTableView {
    weak var coordinator: AppKitServiceListView.Coordinator?

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        return coordinator?.makeMenu(forRow: row)
    }
}

@MainActor
private final class GenericResourceNSTableView: NSTableView {
    weak var coordinator: AppKitGenericResourceListView.Coordinator?

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
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
        selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        return coordinator?.makeMenu(forRow: row)
    }
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

        NSColor.separatorColor.withAlphaComponent(0.24).setFill()
        NSRect(
            x: bounds.minX + horizontalInset,
            y: bounds.minY,
            width: max(0, bounds.width - (horizontalInset * 2)),
            height: 1
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
        let color = isResizable
            ? NSColor.gridColor.withAlphaComponent(0.48)
            : NSColor.gridColor.withAlphaComponent(0.28)
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
            .foregroundColor: NSColor.headerTextColor,
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
        let rowRect = bounds.insetBy(dx: horizontalInset, dy: 1)
        let path = NSBezierPath(roundedRect: rowRect, xRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, yRadius: RuneUILayoutMetrics.compactGlyphCornerRadius)
        let fill = isSelected
            ? NSColor.controlAccentColor.withAlphaComponent(0.11)
            : NSColor.controlBackgroundColor.withAlphaComponent(0.42)
        fill.setFill()
        path.fill()

        if !isSelected {
            NSColor.separatorColor.withAlphaComponent(0.20).setStroke()
            path.lineWidth = 1
            path.stroke()
        }
    }
}

@MainActor
private final class RuneAppKitResourceListScrollView: NSScrollView {
    var onVisibleWidthChanged: (() -> Void)?
    private var lastVisibleWidth: CGFloat = -1

    override func layout() {
        super.layout()
        let visibleWidth = contentView.bounds.width.rounded(.toNearestOrAwayFromZero)
        guard abs(visibleWidth - lastVisibleWidth) >= 1 else { return }
        lastVisibleWidth = visibleWidth
        onVisibleWidthChanged?()
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
