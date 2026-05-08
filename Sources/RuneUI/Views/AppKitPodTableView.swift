import AppKit
import RuneCore
import SwiftUI

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
        tableView.usesAlternatingRowBackgroundColors = false
        tableView.backgroundColor = .clear
        tableView.gridStyleMask = []
        let headerView = RuneAppKitResourceTableHeaderView()
        headerView.resizableColumnIdentifier = PodColumn.name.rawValue
        headerView.onResetResizableColumn = { [weak coordinator = context.coordinator] in
            coordinator?.resetNameColumnWidth()
        }
        tableView.headerView = headerView
        tableView.rowHeight = 34
        tableView.intercellSpacing = NSSize(width: 0, height: 4)
        tableView.allowsMultipleSelection = false
        tableView.allowsColumnResizing = true
        tableView.columnAutoresizingStyle = .noColumnAutoresizing
        tableView.selectionHighlightStyle = .none

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

        init(_ parent: AppKitPodTableView) {
            self.parent = parent
        }

        func apply(parent: AppKitPodTableView) {
            guard let tableView else { return }
            updateNameColumnWidthIfNeeded(on: tableView, width: parent.nameColumnWidth)
            tableView.reloadData()
            applySelection(on: tableView)
            updateSortIndicator(on: tableView)
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
                let checkbox = NSButton(checkboxWithTitle: "", target: self, action: #selector(toggleBulkSelection(_:)))
                checkbox.tag = row
                checkbox.state = parent.selectedPodIDs.contains(pod.id) ? .on : .off
                checkbox.toolTip = parent.selectedPodIDs.contains(pod.id) ? "Remove from bulk selection" : "Add to bulk selection"
                checkbox.controlSize = .small
                checkbox.setAccessibilityLabel("Select \(pod.name)")
                return checkbox

            case .name:
                return label(pod.name, font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium), alignment: .left, tooltip: pod.name)

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
            }
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: 6)
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
                  tableColumn.identifier.rawValue == PodColumn.name.rawValue else { return }
            let width = PodTableLayout.clampedNameColumnWidth(tableColumn.width)
            tableColumn.width = width
            tableColumn.minWidth = PodTableLayout.nameColumnMinimumWidth
            tableColumn.maxWidth = PodTableLayout.nameColumnMaximumWidth

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
            let clamped = PodTableLayout.clampedNameColumnWidth(width)
            column.minWidth = PodTableLayout.nameColumnMinimumWidth
            column.maxWidth = PodTableLayout.nameColumnMaximumWidth
            if abs(column.width - clamped) >= 1 {
                column.width = clamped
            }
        }

        func resetNameColumnWidth() {
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
                        ascending: parent.sortAscending
                    )
                }
                tableView.setIndicatorImage(nil, in: tableColumn)
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
            tooltip: String? = nil
        ) -> NSTextField {
            let label = NSTextField(labelWithString: text)
            label.font = font
            label.alignment = alignment
            label.textColor = textColor
            label.lineBreakMode = .byTruncatingTail
            label.maximumNumberOfLines = 1
            label.toolTip = tooltip
            return label
        }

        private func statusCell(for pod: PodSummary) -> NSView {
            let container = NSView()

            let pill = RuneAppKitResourceStatusPillView(text: pod.status, color: statusColor(for: pod.status))
            pill.toolTip = "Pod phase from the cluster"
            pill.translatesAutoresizingMaskIntoConstraints = false
            container.addSubview(pill)

            var constraints = [
                pill.centerXAnchor.constraint(equalTo: container.centerXAnchor),
                pill.centerYAnchor.constraint(equalTo: container.centerYAnchor)
            ]
            if parent.isFavorite(pod) {
                let favorite = symbolImageView(systemName: "star.fill", accessibilityLabel: "Favorite")
                favorite.translatesAutoresizingMaskIntoConstraints = false
                container.addSubview(favorite)
                constraints += [
                    favorite.trailingAnchor.constraint(equalTo: container.trailingAnchor, constant: -8),
                    favorite.centerYAnchor.constraint(equalTo: container.centerYAnchor),
                    favorite.widthAnchor.constraint(equalToConstant: 14),
                    favorite.heightAnchor.constraint(equalToConstant: 14)
                ]
            }
            NSLayoutConstraint.activate(constraints)
            return container
        }

        private func symbolImageView(systemName: String, accessibilityLabel: String) -> NSImageView {
            let imageView = NSImageView()
            imageView.image = NSImage(systemSymbolName: systemName, accessibilityDescription: accessibilityLabel)
            imageView.contentTintColor = .systemYellow
            imageView.symbolConfiguration = .init(pointSize: NSFont.smallSystemFontSize, weight: .semibold)
            imageView.imageScaling = .scaleProportionallyDown
            imageView.setAccessibilityLabel(accessibilityLabel)
            return imageView
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
    let canApplyClusterMutations: Bool
    let isFavorite: (DeploymentSummary) -> Bool
    let onSelectDeployment: (DeploymentSummary) -> Void
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
        tableView.usesAlternatingRowBackgroundColors = false
        tableView.backgroundColor = .clear
        tableView.gridStyleMask = []
        tableView.headerView = nil
        tableView.rowHeight = 34
        tableView.intercellSpacing = NSSize(width: 0, height: 4)
        tableView.allowsMultipleSelection = false
        tableView.allowsColumnResizing = false
        tableView.columnAutoresizingStyle = .uniformColumnAutoresizingStyle
        tableView.selectionHighlightStyle = .none

        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("deployment"))
        column.resizingMask = .autoresizingMask
        column.minWidth = 200
        tableView.addTableColumn(column)

        let scrollView = NSScrollView()
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

        init(_ parent: AppKitDeploymentListView) {
            self.parent = parent
        }

        func apply(parent: AppKitDeploymentListView) {
            guard let tableView else { return }
            if let column = tableView.tableColumns.first {
                column.width = tableView.bounds.width
            }
            tableView.reloadData()
            applySelection(on: tableView)
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            parent.deployments.count
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: 10)
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard row >= 0, row < parent.deployments.count else { return nil }
            return deploymentCell(for: parent.deployments[row])
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard !isApplyingSelection,
                  let tableView = notification.object as? NSTableView,
                  tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.deployments.count else { return }
            parent.onSelectDeployment(parent.deployments[tableView.selectedRow])
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

        private func deploymentCell(for deployment: DeploymentSummary) -> NSView {
            RuneAppKitSingleLineResourceCell(
                title: deployment.name,
                badgeText: deployment.replicaText,
                badgeColor: .systemBlue,
                isFavorite: parent.isFavorite(deployment)
            )
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
    let canApplyClusterMutations: Bool
    let isFavorite: (ServiceSummary) -> Bool
    let onSelectService: (ServiceSummary) -> Void
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
        tableView.usesAlternatingRowBackgroundColors = false
        tableView.backgroundColor = .clear
        tableView.gridStyleMask = []
        tableView.headerView = nil
        tableView.rowHeight = 34
        tableView.intercellSpacing = NSSize(width: 0, height: 4)
        tableView.allowsMultipleSelection = false
        tableView.allowsColumnResizing = false
        tableView.columnAutoresizingStyle = .uniformColumnAutoresizingStyle
        tableView.selectionHighlightStyle = .none

        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("service"))
        column.resizingMask = .autoresizingMask
        column.minWidth = 200
        tableView.addTableColumn(column)

        let scrollView = NSScrollView()
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

        init(_ parent: AppKitServiceListView) {
            self.parent = parent
        }

        func apply(parent: AppKitServiceListView) {
            guard let tableView else { return }
            if let column = tableView.tableColumns.first {
                column.width = tableView.bounds.width
            }
            tableView.reloadData()
            applySelection(on: tableView)
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            parent.services.count
        }

        func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
            RuneAppKitResourceTableRowView(horizontalInset: 10)
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard row >= 0, row < parent.services.count else { return nil }
            return serviceCell(for: parent.services[row])
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard !isApplyingSelection,
                  let tableView = notification.object as? NSTableView,
                  tableView.selectedRow >= 0,
                  tableView.selectedRow < parent.services.count else { return }
            parent.onSelectService(parent.services[tableView.selectedRow])
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

        private func serviceCell(for service: ServiceSummary) -> NSView {
            RuneAppKitSingleLineResourceCell(
                title: service.name,
                badgeText: service.type,
                badgeColor: .systemPurple,
                isFavorite: parent.isFavorite(service)
            )
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

private final class RuneAppKitResourceTableHeaderView: NSTableHeaderView {
    var resizableColumnIdentifier: String?
    var onResetResizableColumn: (() -> Void)?

    override func mouseDown(with event: NSEvent) {
        if event.clickCount == 2, isEventNearResizableColumnDivider(event) {
            onResetResizableColumn?()
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
                tableView.tableColumns[columnIndex].headerCell.draw(withFrame: rect, in: self)
                drawColumnDivider(at: rect.maxX, isResizable: tableView.tableColumns[columnIndex].identifier.rawValue == resizableColumnIdentifier)
            }
        }

        NSColor.separatorColor.withAlphaComponent(0.24).setFill()
        NSRect(x: bounds.minX, y: bounds.minY, width: bounds.width, height: 1).fill()
    }

    private func isEventNearResizableColumnDivider(_ event: NSEvent) -> Bool {
        guard let tableView,
              let resizableColumnIdentifier,
              let columnIndex = tableView.tableColumns.firstIndex(where: { $0.identifier.rawValue == resizableColumnIdentifier }) else { return false }
        let point = convert(event.locationInWindow, from: nil)
        let dividerX = headerRect(ofColumn: columnIndex).maxX
        return abs(point.x - dividerX) <= 7
    }

    private func drawColumnDivider(at x: CGFloat, isResizable: Bool) {
        guard x > bounds.minX, x < bounds.maxX else { return }
        let color = isResizable
            ? NSColor.separatorColor.withAlphaComponent(0.46)
            : NSColor.separatorColor.withAlphaComponent(0.22)
        color.setFill()
        NSRect(x: x.rounded(.down), y: bounds.minY + 5, width: 1, height: max(0, bounds.height - 10)).fill()
        if isResizable {
            NSColor.controlAccentColor.withAlphaComponent(0.22).setFill()
            NSRect(x: x.rounded(.down) + 2, y: bounds.midY - 7, width: 2, height: 14).fill()
        }
    }
}

private final class RuneAppKitResourceTableHeaderCell: NSTableHeaderCell {
    private let textAlignment: NSTextAlignment
    private var baseTitle = ""
    private var isSorted = false
    private var sortAscending = true

    init(alignment: NSTextAlignment) {
        self.textAlignment = alignment
        super.init(textCell: "")
        self.alignment = alignment
    }

    required init(coder: NSCoder) {
        self.textAlignment = .left
        super.init(coder: coder)
    }

    func configure(title: String, isSorted: Bool, ascending: Bool) {
        self.baseTitle = title
        self.isSorted = isSorted
        self.sortAscending = ascending
        stringValue = title
    }

    override func draw(withFrame cellFrame: NSRect, in controlView: NSView) {
        drawInterior(withFrame: cellFrame.insetBy(dx: 2, dy: 0), in: controlView)
    }

    override func drawInterior(withFrame cellFrame: NSRect, in controlView: NSView) {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = textAlignment
        paragraph.lineBreakMode = .byTruncatingTail
        let attributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold),
            .foregroundColor: NSColor.secondaryLabelColor,
            .paragraphStyle: paragraph
        ]
        let title = NSAttributedString(string: baseTitle.isEmpty ? stringValue : baseTitle, attributes: attributes)
        let textRect = cellFrame.insetBy(dx: textAlignment == .right ? 0 : 4, dy: 0)
        title.draw(in: textRect)
        drawSortIndicator(in: cellFrame)
    }

    private func drawSortIndicator(in cellFrame: NSRect) {
        guard isSorted else { return }
        let symbolName = sortAscending ? "chevron.up" : "chevron.down"
        guard let image = NSImage(systemSymbolName: symbolName, accessibilityDescription: nil) else { return }
        image.isTemplate = true
        NSColor.secondaryLabelColor.set()

        let imageSize = NSSize(width: 8, height: 8)
        let originX: CGFloat
        switch textAlignment {
        case .right:
            originX = cellFrame.minX + 2
        case .center:
            originX = cellFrame.midX + 28
        default:
            let titleWidth = (baseTitle as NSString).size(withAttributes: [
                .font: NSFont.systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold)
            ]).width
            originX = min(cellFrame.maxX - imageSize.width - 4, cellFrame.minX + 8 + titleWidth)
        }
        let rect = NSRect(
            x: originX,
            y: cellFrame.midY - (imageSize.height / 2),
            width: imageSize.width,
            height: imageSize.height
        )
        image.draw(in: rect)
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

private final class RuneAppKitSingleLineResourceCell: NSView {
    init(title: String, badgeText: String, badgeColor: NSColor, isFavorite: Bool) {
        super.init(frame: .zero)

        let titleLabel = NSTextField(labelWithString: title)
        titleLabel.font = .systemFont(ofSize: NSFont.systemFontSize, weight: .medium)
        titleLabel.lineBreakMode = .byTruncatingTail
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

private enum PodColumn: String, CaseIterable {
    case selection
    case name
    case cpu
    case memory
    case restarts
    case age
    case status

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
        case .selection: return nil
        }
    }

    @MainActor
    func tableColumn(nameColumnWidth: CGFloat) -> NSTableColumn {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(rawValue))
        let headerCell = RuneAppKitResourceTableHeaderCell(alignment: alignment)
        headerCell.configure(title: title, isSorted: false, ascending: true)
        column.headerCell = headerCell
        column.title = title
        column.width = width(nameColumnWidth: nameColumnWidth)
        column.minWidth = minWidth
        column.maxWidth = maxWidth
        column.resizingMask = self == .name ? .userResizingMask : []
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
        }
    }

    private var alignment: NSTextAlignment {
        switch self {
        case .cpu, .memory, .restarts, .age:
            return .right
        case .status:
            return .center
        case .selection, .name:
            return .left
        }
    }

    private func width(nameColumnWidth: CGFloat) -> CGFloat {
        switch self {
        case .selection: return PodTableLayout.selectionColumnWidth
        case .name: return PodTableLayout.clampedNameColumnWidth(nameColumnWidth)
        case .cpu: return PodTableLayout.cpuWidth
        case .memory: return PodTableLayout.memoryWidth
        case .restarts: return PodTableLayout.restartsWidth
        case .age: return PodTableLayout.ageWidth
        case .status: return PodTableLayout.statusTotalWidth + PodTableLayout.favoriteColumnWidth
        }
    }

    private var minWidth: CGFloat {
        switch self {
        case .name: return PodTableLayout.nameColumnMinimumWidth
        default: return width(nameColumnWidth: PodTableLayout.nameColumnDefaultWidth)
        }
    }

    private var maxWidth: CGFloat {
        switch self {
        case .name: return PodTableLayout.nameColumnMaximumWidth
        default: return width(nameColumnWidth: PodTableLayout.nameColumnDefaultWidth)
        }
    }
}
