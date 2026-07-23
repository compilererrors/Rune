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
    static let sortIndicatorTrailingInset: CGFloat = 10
    static let sortIndicatorResizeSafetyGap: CGFloat = 3
    static let sortIndicatorSize = NSSize(width: 10, height: 8)
    static let sortIndicatorReservedWidth: CGFloat = sortIndicatorTrailingInset + sortIndicatorSize.width + 4
    static let viewportTrailingPadding: CGFloat = 18
    static let deploymentReplicaColumnWidth: CGFloat = 88
    static let deploymentFavoriteColumnWidth: CGFloat = PodTableLayout.favoriteColumnWidth
    static let deploymentMinimumNameColumnWidth: CGFloat = 260
    static let serviceTypeColumnWidth: CGFloat = 140
    static let serviceClusterIPColumnWidth: CGFloat = 172
    static let serviceFavoriteColumnWidth = PodTableLayout.favoriteColumnWidth
    static let serviceMinimumNameColumnWidth: CGFloat = 260
    static let genericSelectionColumnWidth = PodTableLayout.selectionColumnWidth
    static let genericPrimaryColumnWidth: CGFloat = 136
    static let genericSecondaryColumnWidth: CGFloat = 152
    static let genericNamespaceColumnWidth: CGFloat = 124
    static let genericPrimaryMinimumColumnWidth: CGFloat = 128
    static let genericSecondaryMinimumColumnWidth: CGFloat = 128
    static let genericNamespaceMinimumColumnWidth: CGFloat = 120
    static let genericFavoriteColumnWidth = PodTableLayout.favoriteColumnWidth
    static let genericMinimumNameColumnWidth: CGFloat = 300
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

    private struct ColumnRule {
        let minimum: CGFloat
        let ideal: CGFloat
        let maximum: CGFloat
        let growthWeight: CGFloat

        init(minimum: CGFloat, ideal: CGFloat, maximum: CGFloat, growthWeight: CGFloat) {
            self.minimum = minimum
            self.ideal = max(minimum, ideal)
            self.maximum = max(self.ideal, maximum)
            self.growthWeight = max(0, growthWeight)
        }

        static func fixed(_ width: CGFloat) -> ColumnRule {
            ColumnRule(minimum: width, ideal: width, maximum: width, growthWeight: 0)
        }
    }

    static func availableColumnWidth(visibleWidth: CGFloat) -> CGFloat {
        max(0, visibleWidth.rounded(.toNearestOrAwayFromZero) - viewportTrailingPadding)
    }

    static func viewportFillingFlexibleWidth(
        baseWidth: CGFloat,
        occupiedWidth: CGFloat,
        visibleWidth: CGFloat,
        minimumWidth: CGFloat,
        maximumWidth: CGFloat
    ) -> CGFloat {
        let residualFillWidth = availableColumnWidth(visibleWidth: visibleWidth) - occupiedWidth
        return min(max(max(baseWidth, residualFillWidth), minimumWidth), maximumWidth)
    }

    static func sortIndicatorRect(in columnRect: NSRect) -> NSRect {
        NSRect(
            x: columnRect.maxX - sortIndicatorTrailingInset - sortIndicatorSize.width,
            y: columnRect.midY - (sortIndicatorSize.height / 2),
            width: sortIndicatorSize.width,
            height: sortIndicatorSize.height
        )
    }

    private static func solveColumnWidths(visibleWidth: CGFloat, rules: [ColumnRule]) -> [CGFloat] {
        let targetWidth = availableColumnWidth(visibleWidth: visibleWidth)
        var widths: [CGFloat] = []
        widths.reserveCapacity(rules.count)
        var minimumWidth: CGFloat = 0
        for rule in rules {
            widths.append(rule.minimum)
            minimumWidth += rule.minimum
        }
        guard targetWidth > minimumWidth else { return widths }

        var remaining = targetWidth - minimumWidth
        var idealGrowthTotal: CGFloat = 0
        for index in rules.indices {
            idealGrowthTotal += max(0, rules[index].ideal - widths[index])
        }
        if idealGrowthTotal > 0 {
            let budget = min(remaining, idealGrowthTotal)
            for index in rules.indices {
                let idealGrowth = max(0, rules[index].ideal - widths[index])
                if idealGrowth > 0 {
                    widths[index] += budget * (idealGrowth / idealGrowthTotal)
                }
            }
            remaining -= budget
        }

        while remaining > 0.01 {
            var totalWeight: CGFloat = 0
            for index in rules.indices
            where rules[index].growthWeight > 0 && widths[index] < rules[index].maximum - 0.01 {
                totalWeight += rules[index].growthWeight
            }
            guard totalWeight > 0 else { break }

            var consumed: CGFloat = 0
            for index in rules.indices
            where rules[index].growthWeight > 0 && widths[index] < rules[index].maximum - 0.01 {
                let requested = remaining * (rules[index].growthWeight / totalWeight)
                let growth = min(requested, rules[index].maximum - widths[index])
                widths[index] += growth
                consumed += growth
            }
            guard consumed > 0.01 else { break }
            remaining -= consumed
        }

        return widths
    }

    static func minimumHeaderColumnWidth(title: String, reservesSortIndicator: Bool) -> CGFloat {
        let font = NSFont.systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold)
        let textWidth = ceil((title as NSString).size(withAttributes: [.font: font]).width)
        let contentInset: CGFloat = 10
        let trailingWidth = reservesSortIndicator
            ? sortIndicatorSize.width + 4
            : 0
        // Reserve the same chrome on both sides. This keeps centered labels optically
        // centered and prevents a hover/active chevron from truncating short headers.
        return ceil(textWidth + ((contentInset + trailingWidth) * 2))
    }

    private static let podHeaderMinimums = (
        cpu: minimumHeaderColumnWidth(title: "CPU", reservesSortIndicator: true),
        memory: minimumHeaderColumnWidth(title: "MEM", reservesSortIndicator: true),
        restarts: minimumHeaderColumnWidth(title: "Restarts", reservesSortIndicator: true),
        age: minimumHeaderColumnWidth(title: "Age", reservesSortIndicator: true),
        status: minimumHeaderColumnWidth(title: "Status", reservesSortIndicator: true)
    )
    private static let helmHeaderMinimums = (
        status: minimumHeaderColumnWidth(title: "Status", reservesSortIndicator: true),
        namespace: minimumHeaderColumnWidth(title: "Namespace", reservesSortIndicator: true),
        revision: minimumHeaderColumnWidth(title: "Rev", reservesSortIndicator: true),
        chart: minimumHeaderColumnWidth(title: "Chart", reservesSortIndicator: true),
        appVersion: minimumHeaderColumnWidth(title: "App", reservesSortIndicator: true)
    )
    private static let eventHeaderMinimums = (
        type: minimumHeaderColumnWidth(title: "Type", reservesSortIndicator: true),
        object: minimumHeaderColumnWidth(title: "Object", reservesSortIndicator: true),
        namespace: minimumHeaderColumnWidth(title: "Namespace", reservesSortIndicator: true),
        lastSeen: minimumHeaderColumnWidth(title: "Last Seen", reservesSortIndicator: true),
        message: minimumHeaderColumnWidth(title: "Message", reservesSortIndicator: false)
    )
    private static let operatorHeaderMinimums = (
        family: minimumHeaderColumnWidth(title: "Family", reservesSortIndicator: true),
        kind: minimumHeaderColumnWidth(title: "Kind", reservesSortIndicator: true),
        namespace: minimumHeaderColumnWidth(title: "Namespace", reservesSortIndicator: true),
        status: minimumHeaderColumnWidth(title: "Status", reservesSortIndicator: true),
        printerColumns: minimumHeaderColumnWidth(title: "Columns", reservesSortIndicator: false),
        apiPath: minimumHeaderColumnWidth(title: "API Path", reservesSortIndicator: true)
    )
    private static let deploymentColumnRules: [ColumnRule] = [
        ColumnRule(minimum: deploymentMinimumNameColumnWidth, ideal: 360, maximum: 10_000, growthWeight: 3),
        ColumnRule(minimum: deploymentReplicaColumnWidth, ideal: deploymentReplicaColumnWidth, maximum: 180, growthWeight: 1),
        .fixed(deploymentFavoriteColumnWidth)
    ]
    private static let serviceColumnRules: [ColumnRule] = [
        ColumnRule(minimum: serviceMinimumNameColumnWidth, ideal: 360, maximum: 10_000, growthWeight: 2.6),
        ColumnRule(minimum: serviceTypeColumnWidth, ideal: serviceTypeColumnWidth, maximum: 220, growthWeight: 1),
        ColumnRule(minimum: serviceClusterIPColumnWidth, ideal: serviceClusterIPColumnWidth, maximum: 260, growthWeight: 1.2),
        .fixed(serviceFavoriteColumnWidth)
    ]
    private static let genericColumnRules: [ColumnRule] = [
        .fixed(genericSelectionColumnWidth),
        ColumnRule(minimum: genericMinimumNameColumnWidth, ideal: 380, maximum: 10_000, growthWeight: 2.8),
        ColumnRule(minimum: genericPrimaryMinimumColumnWidth, ideal: genericPrimaryColumnWidth, maximum: 260, growthWeight: 1.1),
        ColumnRule(minimum: genericSecondaryMinimumColumnWidth, ideal: genericSecondaryColumnWidth, maximum: 360, growthWeight: 1.3),
        ColumnRule(minimum: genericNamespaceMinimumColumnWidth, ideal: genericNamespaceColumnWidth, maximum: 280, growthWeight: 1.1),
        .fixed(genericFavoriteColumnWidth)
    ]
    private static let helmColumnRules: [ColumnRule] = [
        ColumnRule(minimum: helmMinimumNameColumnWidth, ideal: 380, maximum: 10_000, growthWeight: 2.4),
        ColumnRule(minimum: max(helmStatusMinimumColumnWidth, helmHeaderMinimums.status), ideal: helmStatusColumnWidth, maximum: 220, growthWeight: 0.9),
        ColumnRule(minimum: max(helmNamespaceMinimumColumnWidth, helmHeaderMinimums.namespace), ideal: helmNamespaceColumnWidth, maximum: 280, growthWeight: 1.1),
        ColumnRule(minimum: max(helmRevisionMinimumColumnWidth, helmHeaderMinimums.revision), ideal: helmRevisionColumnWidth, maximum: 150, growthWeight: 0.6),
        ColumnRule(minimum: max(helmChartMinimumColumnWidth, helmHeaderMinimums.chart), ideal: helmChartColumnWidth, maximum: 420, growthWeight: 1.7),
        ColumnRule(minimum: max(helmAppVersionMinimumColumnWidth, helmHeaderMinimums.appVersion), ideal: helmAppVersionColumnWidth, maximum: 240, growthWeight: 1)
    ]
    private static let eventColumnRules: [ColumnRule] = [
        ColumnRule(minimum: eventMinimumReasonColumnWidth, ideal: 240, maximum: 10_000, growthWeight: 1.5),
        ColumnRule(minimum: max(eventTypeMinimumColumnWidth, eventHeaderMinimums.type), ideal: eventTypeColumnWidth, maximum: 220, growthWeight: 0.8),
        ColumnRule(minimum: max(eventObjectMinimumColumnWidth, eventHeaderMinimums.object), ideal: eventObjectColumnWidth, maximum: 420, growthWeight: 1.4),
        ColumnRule(minimum: max(eventNamespaceMinimumColumnWidth, eventHeaderMinimums.namespace), ideal: eventNamespaceColumnWidth, maximum: 280, growthWeight: 1),
        ColumnRule(minimum: max(eventLastSeenMinimumColumnWidth, eventHeaderMinimums.lastSeen), ideal: eventLastSeenColumnWidth, maximum: 320, growthWeight: 1.1),
        ColumnRule(minimum: max(eventMessageMinimumColumnWidth, eventHeaderMinimums.message), ideal: eventMessageColumnWidth, maximum: 10_000, growthWeight: 2.2)
    ]
    private static let operatorColumnRules: [ColumnRule] = [
        ColumnRule(minimum: operatorMinimumNameColumnWidth, ideal: 340, maximum: 10_000, growthWeight: 2.2),
        ColumnRule(minimum: max(operatorFamilyMinimumColumnWidth, operatorHeaderMinimums.family), ideal: operatorFamilyColumnWidth, maximum: 280, growthWeight: 1),
        ColumnRule(minimum: max(operatorKindMinimumColumnWidth, operatorHeaderMinimums.kind), ideal: operatorKindColumnWidth, maximum: 300, growthWeight: 1.1),
        ColumnRule(minimum: max(operatorNamespaceMinimumColumnWidth, operatorHeaderMinimums.namespace), ideal: operatorNamespaceColumnWidth, maximum: 280, growthWeight: 1),
        ColumnRule(minimum: max(operatorStatusMinimumColumnWidth, operatorHeaderMinimums.status), ideal: operatorStatusColumnWidth, maximum: 260, growthWeight: 0.9),
        ColumnRule(minimum: max(120, operatorHeaderMinimums.printerColumns), ideal: operatorPrinterColumnsColumnWidth, maximum: 360, growthWeight: 1.3),
        ColumnRule(minimum: max(operatorAPIPathMinimumColumnWidth, operatorHeaderMinimums.apiPath), ideal: operatorAPIPathColumnWidth, maximum: 10_000, growthWeight: 2),
        .fixed(operatorFavoriteColumnWidth)
    ]

    static func podColumnWidths(
        visibleWidth: CGFloat,
        minimumNameWidth: CGFloat
    ) -> (
        selection: CGFloat,
        name: CGFloat,
        cpu: CGFloat,
        memory: CGFloat,
        restarts: CGFloat,
        age: CGFloat,
        status: CGFloat,
        favorite: CGFloat
    ) {
        let cpuMinimum = max(
            PodTableLayout.cpuWidth,
            podHeaderMinimums.cpu
        )
        let memoryMinimum = max(
            PodTableLayout.memoryWidth,
            podHeaderMinimums.memory
        )
        let restartsMinimum = max(
            PodTableLayout.restartsWidth,
            podHeaderMinimums.restarts
        )
        let ageMinimum = max(
            PodTableLayout.ageWidth,
            podHeaderMinimums.age
        )
        let statusMinimum = max(
            PodTableLayout.statusTotalWidth,
            podHeaderMinimums.status
        )
        let widths = solveColumnWidths(
            visibleWidth: visibleWidth,
            rules: [
                .fixed(PodTableLayout.selectionColumnWidth),
                ColumnRule(
                    minimum: PodTableLayout.clampedNameColumnWidth(minimumNameWidth),
                    ideal: 380,
                    maximum: PodTableLayout.nameColumnMaximumWidth,
                    growthWeight: 3
                ),
                ColumnRule(minimum: cpuMinimum, ideal: max(cpuMinimum, 78), maximum: 160, growthWeight: 0.7),
                ColumnRule(minimum: memoryMinimum, ideal: max(memoryMinimum, 86), maximum: 180, growthWeight: 0.8),
                ColumnRule(minimum: restartsMinimum, ideal: max(restartsMinimum, 108), maximum: 180, growthWeight: 0.9),
                ColumnRule(minimum: ageMinimum, ideal: max(ageMinimum, 82), maximum: 140, growthWeight: 0.6),
                ColumnRule(minimum: statusMinimum, ideal: max(statusMinimum, 118), maximum: 240, growthWeight: 1),
                .fixed(PodTableLayout.favoriteColumnWidth)
            ]
        )
        return (
            selection: widths[0],
            name: widths[1],
            cpu: widths[2],
            memory: widths[3],
            restarts: widths[4],
            age: widths[5],
            status: widths[6],
            favorite: widths[7]
        )
    }

    static func deploymentColumnWidths(visibleWidth: CGFloat) -> (name: CGFloat, replicas: CGFloat, favorite: CGFloat) {
        let widths = solveColumnWidths(
            visibleWidth: visibleWidth,
            rules: deploymentColumnRules
        )
        return (
            name: widths[0],
            replicas: widths[1],
            favorite: widths[2]
        )
    }

    static func serviceColumnWidths(visibleWidth: CGFloat) -> (name: CGFloat, type: CGFloat, clusterIP: CGFloat, favorite: CGFloat) {
        let widths = solveColumnWidths(
            visibleWidth: visibleWidth,
            rules: serviceColumnRules
        )
        return (
            name: widths[0],
            type: widths[1],
            clusterIP: widths[2],
            favorite: widths[3]
        )
    }

    static func genericColumnWidths(
        visibleWidth: CGFloat
    ) -> (selection: CGFloat, name: CGFloat, primary: CGFloat, secondary: CGFloat, namespace: CGFloat, favorite: CGFloat) {
        let widths = solveColumnWidths(
            visibleWidth: visibleWidth,
            rules: genericColumnRules
        )
        return (
            selection: widths[0],
            name: widths[1],
            primary: widths[2],
            secondary: widths[3],
            namespace: widths[4],
            favorite: widths[5]
        )
    }

    static func helmColumnWidths(
        visibleWidth: CGFloat
    ) -> (name: CGFloat, status: CGFloat, namespace: CGFloat, revision: CGFloat, chart: CGFloat, appVersion: CGFloat) {
        let widths = solveColumnWidths(
            visibleWidth: visibleWidth,
            rules: helmColumnRules
        )
        return (
            name: widths[0],
            status: widths[1],
            namespace: widths[2],
            revision: widths[3],
            chart: widths[4],
            appVersion: widths[5]
        )
    }

    static func eventColumnWidths(
        visibleWidth: CGFloat
    ) -> (reason: CGFloat, type: CGFloat, object: CGFloat, namespace: CGFloat, lastSeen: CGFloat, message: CGFloat) {
        let widths = solveColumnWidths(
            visibleWidth: visibleWidth,
            rules: eventColumnRules
        )
        return (
            reason: widths[0],
            type: widths[1],
            object: widths[2],
            namespace: widths[3],
            lastSeen: widths[4],
            message: widths[5]
        )
    }

    static func operatorColumnWidths(
        visibleWidth: CGFloat
    ) -> (name: CGFloat, family: CGFloat, kind: CGFloat, namespace: CGFloat, status: CGFloat, printerColumns: CGFloat, apiPath: CGFloat, favorite: CGFloat) {
        let widths = solveColumnWidths(
            visibleWidth: visibleWidth,
            rules: operatorColumnRules
        )
        return (
            name: widths[0],
            family: widths[1],
            kind: widths[2],
            namespace: widths[3],
            status: widths[4],
            printerColumns: widths[5],
            apiPath: widths[6],
            favorite: widths[7]
        )
    }
}

struct RuneGenericResourceColumnPresentation {
    let primaryTitle: String
    let secondaryTitle: String
    let primaryAlignment: NSTextAlignment
    let secondaryAlignment: NSTextAlignment

    static func resolve(for kind: KubeResourceKind?) -> RuneGenericResourceColumnPresentation {
        switch kind {
        case .statefulSet, .daemonSet, .replicaSet:
            return presentation("Ready", "Selector", primary: .right)
        case .job:
            return presentation("Status", "Progress")
        case .cronJob:
            return presentation("Schedule", "Status")
        case .endpoint:
            return presentation("Ready", "Ports", primary: .right)
        case .ingress:
            return presentation("Host", "Backend")
        case .configMap:
            return presentation("Keys", "Data", primary: .right)
        case .secret:
            return presentation("Type", "Values", secondary: .right)
        case .node:
            return presentation("Status", "Version")
        case .serviceAccount:
            return presentation("Secrets", "Token Policy", primary: .right)
        case .role, .clusterRole:
            return presentation("Rules", "Scope", primary: .right)
        case .roleBinding, .clusterRoleBinding:
            return presentation("Role", "Subjects", secondary: .right)
        case .persistentVolumeClaim:
            return presentation("Status", "Volume")
        case .persistentVolume:
            return presentation("Status", "Capacity", secondary: .right)
        case .storageClass:
            return presentation("Provisioner", "Default")
        case .horizontalPodAutoscaler:
            return presentation("Replicas", "Target", primary: .right)
        case .networkPolicy:
            return presentation("Policy Types", "Pod Selector")
        case .pod:
            return presentation("Status", "Node")
        case .deployment:
            return presentation("Ready", "Status", primary: .right)
        case .service:
            return presentation("Type", "Cluster IP", secondary: .right)
        case .event:
            return presentation("Type", "Message")
        case nil:
            return presentation("Detail", "Info")
        }
    }

    static func tableID(for kind: KubeResourceKind?) -> String {
        "genericResources.\(kind?.rawValue ?? "unknown")"
    }

    private static func presentation(
        _ primaryTitle: String,
        _ secondaryTitle: String,
        primary primaryAlignment: NSTextAlignment = .left,
        secondary secondaryAlignment: NSTextAlignment = .left
    ) -> RuneGenericResourceColumnPresentation {
        RuneGenericResourceColumnPresentation(
            primaryTitle: primaryTitle,
            secondaryTitle: secondaryTitle,
            primaryAlignment: primaryAlignment,
            secondaryAlignment: secondaryAlignment
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
func applyResourceTableSelection<Row>(
    selectedID: String?,
    rows: [Row],
    rowID: (Row) -> String,
    in tableView: NSTableView
) {
    guard let selectedID,
          let selectedRow = rows.firstIndex(where: { rowID($0) == selectedID }) else {
        tableView.deselectAll(nil)
        return
    }
    tableView.selectRowIndexes(IndexSet(integer: selectedRow), byExtendingSelection: false)
}

@MainActor
enum RuneAppKitResourceTableStyle {
    static let headerHeight: CGFloat = 24
    static let rowHeight: CGFloat = 34
    static let rowGap: CGFloat = 0
    static let rowHorizontalInset: CGFloat = 0
    static let actionColumnTrailingPadding = RuneAppKitResourceListLayout.viewportTrailingPadding
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
        tableView.tableColumns.reduce(CGFloat(0)) { width, column in
            column.isHidden ? width : width + column.width
        }
    }

    static func renderedTableWidth(in tableView: NSTableView) -> CGFloat {
        let minimumContentWidth = columnContentWidth(in: tableView)
            + (rowHorizontalInset * 2)
            + actionColumnTrailingPadding
        let viewportWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? 0
        return max(minimumContentWidth, viewportWidth.rounded(.toNearestOrAwayFromZero))
    }

    static func updateRenderedTableWidth(on tableView: NSTableView?, updatesVisibleCellsImmediately: Bool = true) {
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
        tableView.needsDisplay = updatesVisibleCellsImmediately
        tableView.headerView?.needsLayout = true
        guard updatesVisibleCellsImmediately else {
            synchronizeVisibleResizeFrames(on: tableView)
            tableView.headerView?.displayIfNeeded()
            return
        }
        if let scrollView = tableView.enclosingScrollView {
            scrollView.contentView.needsLayout = true
            scrollView.contentView.needsDisplay = true
            scrollView.contentView.layoutSubtreeIfNeeded()
            scrollView.reflectScrolledClipView(scrollView.contentView)
            scrollView.tile()
        }
        tableView.layoutSubtreeIfNeeded()
        tableView.headerView?.layoutSubtreeIfNeeded()
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

    static func synchronizeVisibleResizeFrames(on tableView: NSTableView) {
        tableView.enclosingScrollView?.contentView.needsLayout = true
        let visibleRows = tableView.rows(in: tableView.visibleRect)
        guard visibleRows.location != NSNotFound else { return }
        let upperBound = min(tableView.numberOfRows, visibleRows.location + visibleRows.length)
        guard visibleRows.location < upperBound else { return }

        for row in visibleRows.location..<upperBound {
            if let rowView = tableView.rowView(atRow: row, makeIfNecessary: false) {
                let rowFrame = tableView.rect(ofRow: row)
                if abs(rowView.frame.minX - rowFrame.minX) >= 1
                    || abs(rowView.frame.width - rowFrame.width) >= 1 {
                    rowView.frame = rowFrame
                }
                rowView.needsDisplay = true
                rowView.displayIfNeeded()
            }
            for column in 0..<tableView.numberOfColumns {
                guard let cellView = tableView.view(atColumn: column, row: row, makeIfNecessary: false) else { continue }
                let cellFrame = tableView.frameOfCell(atColumn: column, row: row)
                let targetFrame = cellView.superview.map { superview in
                    superview === tableView ? cellFrame : superview.convert(cellFrame, from: tableView)
                } ?? cellFrame
                if abs(cellView.frame.minX - targetFrame.minX) >= 1
                    || abs(cellView.frame.width - targetFrame.width) >= 1 {
                    cellView.frame = targetFrame
                }
            }
        }
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

@MainActor
private enum RuneAppKitResourceTableHost {
    static func make<Coordinator>(
        coordinator: Coordinator,
        resolvedTheme: RuneResolvedTheme,
        columns: [NSTableColumn],
        resizableColumnIdentifiers: Set<String>,
        onResetColumn: @escaping (String) -> Void,
        onColumnResize: @escaping (Notification) -> Void,
        onVisibleWidthChanged: @escaping () -> Void,
        onContextMenu: @escaping (Int, NSTableView) -> NSMenu?,
        onFavoriteToggle: (() -> Void)? = nil
    ) -> NSScrollView where Coordinator: NSObject, Coordinator: NSTableViewDelegate, Coordinator: NSTableViewDataSource {
        let tableView = RuneAppKitResourceTableView()
        tableView.resolvedTheme = resolvedTheme
        tableView.delegate = coordinator
        tableView.dataSource = coordinator
        RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: true)

        let headerView = RuneAppKitResourceTableHeaderView()
        RuneAppKitResourceTableStyle.apply(to: headerView)
        headerView.resizableColumnIdentifiers = resizableColumnIdentifiers
        headerView.onResetColumn = onResetColumn
        headerView.onColumnResizeCommitted = { [weak tableView] tableColumn in
            onColumnResize(
                Notification(
                    name: NSTableView.columnDidResizeNotification,
                    object: tableView,
                    userInfo: ["NSTableColumn": tableColumn]
                )
            )
        }
        tableView.headerView = headerView
        columns.forEach(tableView.addTableColumn)
        tableView.onContextMenu = onContextMenu
        tableView.onFavoriteToggle = onFavoriteToggle

        let scrollView = RuneAppKitResourceListScrollView()
        scrollView.onVisibleWidthChanged = onVisibleWidthChanged
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.horizontalScrollElasticity = .automatic
        scrollView.documentView = tableView
        return scrollView
    }

    static func tableView(in scrollView: NSScrollView) -> NSTableView? {
        scrollView.documentView as? RuneAppKitResourceTableView
    }

    static func invalidateTheme(in scrollView: NSScrollView, resolvedTheme: RuneResolvedTheme) {
        (scrollView.documentView as? RuneAppKitResourceTableView)?.resolvedTheme = resolvedTheme
        RuneAppKitResourceTableStyle.invalidateTheme(in: scrollView)
    }

    static func configureHorizontalOverflowEdgeGlow(in scrollView: NSScrollView, isEnabled: Bool) {
        (scrollView as? RuneAppKitResourceListScrollView)?.showsHorizontalOverflowEdgeGlow = isEnabled
    }
}

@MainActor
private enum RuneAppKitResourceColumnResizeNotificationGate {
    private static var suppressionDepthByTableID: [ObjectIdentifier: Int] = [:]

    static func withSuppressedNotifications<T>(for tableView: NSTableView, _ work: () throws -> T) rethrows -> T {
        let tableID = ObjectIdentifier(tableView)
        suppressionDepthByTableID[tableID, default: 0] += 1
        defer {
            let nextDepth = (suppressionDepthByTableID[tableID] ?? 1) - 1
            if nextDepth > 0 {
                suppressionDepthByTableID[tableID] = nextDepth
            } else {
                suppressionDepthByTableID.removeValue(forKey: tableID)
            }
        }
        return try work()
    }

    static func isSuppressed(_ notification: Notification) -> Bool {
        guard let tableView = notification.object as? NSTableView else { return false }
        return (suppressionDepthByTableID[ObjectIdentifier(tableView)] ?? 0) > 0
    }
}

@MainActor
func isSuppressedSynchronizedResourceColumnResize(_ notification: Notification) -> Bool {
    RuneAppKitResourceColumnResizeNotificationGate.isSuppressed(notification)
}

@MainActor
private func synchronizeResourceTableSortDescriptors(
    on tableView: NSTableView,
    key: String,
    ascending: Bool
) {
    if tableView.sortDescriptors.count == 1,
       let current = tableView.sortDescriptors.first,
       current.key == key,
       current.ascending == ascending {
        return
    }
    tableView.sortDescriptors = [NSSortDescriptor(key: key, ascending: ascending)]
}

@MainActor
@discardableResult
func applySynchronizedResourceColumnResize(
    _ proposedWidth: CGFloat,
    for tableColumn: NSTableColumn,
    in tableView: NSTableView
) -> CGFloat {
    let width = min(
        max(proposedWidth.rounded(.toNearestOrAwayFromZero), tableColumn.minWidth),
        tableColumn.maxWidth
    )
    guard abs(tableColumn.width - width) >= 1 else { return tableColumn.width }
    RuneAppKitResourceColumnResizeNotificationGate.withSuppressedNotifications(for: tableView) {
        tableColumn.width = width
    }
    RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView, updatesVisibleCellsImmediately: false)
    return width
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

    static func resolved(_ theme: RuneResolvedTheme) -> RuneAppKitResourceTableTheme {
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

    @MainActor
    static func resolved(for view: NSView) -> RuneAppKitResourceTableTheme {
        resolved(resolvedRuneResourceTableTheme(for: view))
    }

    private static func themed(
        text: String,
        stroke: String,
        row: String,
        selected: String,
        selectedAlpha: CGFloat = 0.18
    ) -> RuneAppKitResourceTableTheme {
        let strokeColor = NSColor.runeHex(stroke)
        return RuneAppKitResourceTableTheme(
            headerFill: NSColor.runeHex(row).withAlphaComponent(0.62),
            headerText: NSColor.runeHex(text).withAlphaComponent(0.92),
            headerDivider: strokeColor.withAlphaComponent(0.40),
            columnDivider: strokeColor.withAlphaComponent(0.32),
            columnDividerResizable: strokeColor.withAlphaComponent(0.56),
            rowFill: NSColor.runeHex(row).withAlphaComponent(0.62),
            selectedRowFill: NSColor.runeHex(selected).withAlphaComponent(selectedAlpha),
            rowStroke: strokeColor.withAlphaComponent(0.30)
        )
    }
}

/// Resolves the SwiftUI-provided Rune theme from either table content or its
/// separate AppKit header hierarchy. `NSTableHeaderView` is hosted in the
/// scroll view's header clip view, so walking only `superview` cannot reach the
/// document table.
@MainActor
func resolvedRuneResourceTableTheme(for view: NSView) -> RuneResolvedTheme {
    var candidate: NSView? = view
    while let current = candidate {
        if let tableView = current as? RuneAppKitResourceTableView {
            return tableView.resolvedTheme
        }
        if let headerView = current as? NSTableHeaderView,
           let tableView = headerView.tableView as? RuneAppKitResourceTableView {
            return tableView.resolvedTheme
        }
        candidate = current.superview
    }
    return RuneAppearanceTheme.native.resolvedTheme
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
    @Environment(\.runeResolvedTheme) private var resolvedTheme
    @AppStorage(RuneSettingsKeys.showResourceTableScrollEdgeGlow) private var showsHorizontalOverflowEdgeGlow = true

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = RuneAppKitResourceTableHost.make(
            coordinator: context.coordinator,
            resolvedTheme: resolvedTheme,
            columns: PodColumn.allCases.map { $0.tableColumn(nameColumnWidth: nameColumnWidth) },
            resizableColumnIdentifiers: Set(PodColumn.allCases.filter(\.isUserResizable).map(\.rawValue)),
            onResetColumn: { [weak coordinator = context.coordinator] in coordinator?.resetColumnWidth($0) },
            onColumnResize: { [weak coordinator = context.coordinator] in coordinator?.tableViewColumnDidResize($0) },
            onVisibleWidthChanged: { [weak coordinator = context.coordinator] in coordinator?.updateTableGeometry() },
            onContextMenu: { [weak coordinator = context.coordinator] row, tableView in
                coordinator?.selectRowForContextMenu(row, in: tableView)
                return coordinator?.makeMenu(forRow: row)
            },
            onFavoriteToggle: { [weak coordinator = context.coordinator] in
                guard let tableView = coordinator?.tableView else { return }
                coordinator?.toggleFavoriteForSelectedRow(in: tableView)
            }
        )
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return scrollView }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
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
                self.updateColumnWidths(on: tableView)
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
            if isSuppressedSynchronizedResourceColumnResize(notification) { return }
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

        func resetColumnWidth(_ columnID: String) {
            guard let columnKind = PodColumn(rawValue: columnID) else { return }
            RuneAppKitColumnWidthStore.shared.removeWidth(tableID: tableID, columnID: columnID)
            if columnKind == .name {
                nameColumnPersistWorkItem?.cancel()
                parent.onNameColumnWidthChanged(PodTableLayout.nameColumnDefaultWidth)
            }
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateTableWidth(on tableView: NSTableView?) {
            RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)
        }

        func updateTableGeometry() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.podColumnWidths(
                visibleWidth: visibleWidth,
                minimumNameWidth: parent.nameColumnWidth
            )
            var projected: [PodColumn: CGFloat] = [
                .selection: widths.selection,
                .name: widths.name,
                .cpu: widths.cpu,
                .memory: widths.memory,
                .restarts: widths.restarts,
                .age: widths.age,
                .status: widths.status,
                .favorite: widths.favorite
            ]

            for column in PodColumn.allCases where column.isUserResizable {
                guard projected[column] != nil,
                      let storedWidth = RuneAppKitColumnWidthStore.shared.width(
                          tableID: tableID,
                          columnID: column.rawValue
                      ) else { continue }
                projected[column] = min(
                    max(storedWidth, column.minimumWidth),
                    column.maximumWidth
                )
            }
            let occupiedWidth = projected.reduce(CGFloat(0)) { partialResult, element in
                element.key == .name ? partialResult : partialResult + element.value
            }
            projected[.name] = RuneAppKitResourceListLayout.viewportFillingFlexibleWidth(
                baseWidth: projected[.name] ?? widths.name,
                occupiedWidth: occupiedWidth,
                visibleWidth: visibleWidth,
                minimumWidth: PodColumn.name.minimumWidth,
                maximumWidth: PodColumn.name.maximumWidth
            )

            RuneAppKitResourceColumnResizeNotificationGate.withSuppressedNotifications(for: tableView) {
                for tableColumn in tableView.tableColumns {
                    guard let column = PodColumn(rawValue: tableColumn.identifier.rawValue),
                          let projectedWidth = projected[column] else { continue }
                    let width = min(
                        max(projectedWidth, tableColumn.minWidth),
                        tableColumn.maxWidth
                    )
                    if abs(tableColumn.width - width) >= 1 {
                        tableColumn.width = width
                    }
                }
            }
            updateTableWidth(on: tableView)
        }

        private func applySelection(on tableView: NSTableView) {
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyResourceTableSelection(selectedID: parent.selectedPodID, rows: parent.pods, rowID: \.id, in: tableView)
        }

        private func updateSortIndicator(on tableView: NSTableView) {
            synchronizeResourceTableSortDescriptors(
                on: tableView,
                key: parent.sortColumn.rawValue,
                ascending: parent.sortAscending
            )
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
            container.wantsLayer = true
            container.layer?.masksToBounds = true

            let pill = RuneAppKitResourceStatusPillView(text: pod.status, color: statusColor(for: pod.status))
            pill.toolTip = "\(pod.status) - pod status from the cluster"
            pill.translatesAutoresizingMaskIntoConstraints = false
            container.addSubview(pill)
            let maxPillWidth = pill.widthAnchor.constraint(lessThanOrEqualTo: container.widthAnchor, constant: -8)
            maxPillWidth.priority = .defaultHigh

            NSLayoutConstraint.activate([
                pill.leadingAnchor.constraint(greaterThanOrEqualTo: container.leadingAnchor, constant: 4),
                pill.trailingAnchor.constraint(lessThanOrEqualTo: container.trailingAnchor, constant: -4),
                maxPillWidth,
                pill.centerXAnchor.constraint(equalTo: container.centerXAnchor),
                pill.centerYAnchor.constraint(equalTo: container.centerYAnchor)
            ])
            return container
        }

        private func favoriteCell(isFavorite: Bool, row: Int) -> NSView {
            RuneAppKitFavoriteButtonCell(
                isFavorite: isFavorite,
                row: row,
                target: self,
                action: #selector(toggleFavoriteButton(_:))
            )
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
    @Environment(\.runeResolvedTheme) private var resolvedTheme
    @AppStorage(RuneSettingsKeys.showResourceTableScrollEdgeGlow) private var showsHorizontalOverflowEdgeGlow = true

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = RuneAppKitResourceTableHost.make(
            coordinator: context.coordinator,
            resolvedTheme: resolvedTheme,
            columns: DeploymentColumn.allCases.map { $0.tableColumn() },
            resizableColumnIdentifiers: Set(DeploymentColumn.allCases.filter(\.isUserResizable).map(\.rawValue)),
            onResetColumn: { [weak coordinator = context.coordinator] in coordinator?.resetColumnWidth($0) },
            onColumnResize: { [weak coordinator = context.coordinator] in coordinator?.tableViewColumnDidResize($0) },
            onVisibleWidthChanged: { [weak coordinator = context.coordinator] in coordinator?.updateColumnWidths() },
            onContextMenu: { [weak coordinator = context.coordinator] row, tableView in
                coordinator?.selectRowForContextMenu(row, in: tableView)
                return coordinator?.makeMenu(forRow: row)
            },
            onFavoriteToggle: { [weak coordinator = context.coordinator] in
                guard let tableView = coordinator?.tableView else { return }
                coordinator?.toggleFavoriteForSelectedRow(in: tableView)
            }
        )
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return scrollView }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
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
            if isSuppressedSynchronizedResourceColumnResize(notification) { return }
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
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyResourceTableSelection(selectedID: parent.selectedDeploymentID, rows: parent.deployments, rowID: \.id, in: tableView)
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = RuneAppKitResourceListLayout.availableColumnWidth(visibleWidth: visibleWidth)
            let storedReplicas = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: DeploymentColumn.replicas.rawValue)
            let replicasWidth = min(max(storedReplicas ?? widths.replicas, DeploymentColumn.replicas.minimumWidth), DeploymentColumn.replicas.maximumWidth)
            let dynamicNameWidth = max(
                RuneAppKitResourceListLayout.deploymentMinimumNameColumnWidth,
                projectedWidth - replicasWidth - widths.favorite
            )
            let storedName = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: DeploymentColumn.name.rawValue)
            let nameWidth = RuneAppKitResourceListLayout.viewportFillingFlexibleWidth(
                baseWidth: storedName ?? dynamicNameWidth,
                occupiedWidth: replicasWidth + widths.favorite,
                visibleWidth: visibleWidth,
                minimumWidth: DeploymentColumn.name.minimumWidth,
                maximumWidth: DeploymentColumn.name.maximumWidth
            )
            RuneAppKitResourceColumnResizeNotificationGate.withSuppressedNotifications(for: tableView) {
                for tableColumn in tableView.tableColumns {
                    guard let column = DeploymentColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                    switch column {
                    case .name: tableColumn.width = nameWidth
                    case .replicas: tableColumn.width = replicasWidth
                    case .favorite: tableColumn.width = widths.favorite
                    }
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
            synchronizeResourceTableSortDescriptors(
                on: tableView,
                key: parent.sortColumn.rawValue,
                ascending: parent.sortAscending
            )
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
    @Environment(\.runeResolvedTheme) private var resolvedTheme
    @AppStorage(RuneSettingsKeys.showResourceTableScrollEdgeGlow) private var showsHorizontalOverflowEdgeGlow = true

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = RuneAppKitResourceTableHost.make(
            coordinator: context.coordinator,
            resolvedTheme: resolvedTheme,
            columns: ServiceColumn.allCases.map { $0.tableColumn() },
            resizableColumnIdentifiers: Set(ServiceColumn.allCases.filter(\.isUserResizable).map(\.rawValue)),
            onResetColumn: { [weak coordinator = context.coordinator] in coordinator?.resetColumnWidth($0) },
            onColumnResize: { [weak coordinator = context.coordinator] in coordinator?.tableViewColumnDidResize($0) },
            onVisibleWidthChanged: { [weak coordinator = context.coordinator] in coordinator?.updateColumnWidths() },
            onContextMenu: { [weak coordinator = context.coordinator] row, tableView in
                coordinator?.selectRowForContextMenu(row, in: tableView)
                return coordinator?.makeMenu(forRow: row)
            },
            onFavoriteToggle: { [weak coordinator = context.coordinator] in
                guard let tableView = coordinator?.tableView else { return }
                coordinator?.toggleFavoriteForSelectedRow(in: tableView)
            }
        )
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return scrollView }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
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
            if isSuppressedSynchronizedResourceColumnResize(notification) { return }
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
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyResourceTableSelection(selectedID: parent.selectedServiceID, rows: parent.services, rowID: \.id, in: tableView)
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.serviceColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = RuneAppKitResourceListLayout.availableColumnWidth(visibleWidth: visibleWidth)
            let storedType = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: ServiceColumn.type.rawValue)
            let typeWidth = min(max(storedType ?? widths.type, ServiceColumn.type.minimumWidth), ServiceColumn.type.maximumWidth)
            let storedClusterIP = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: ServiceColumn.clusterIP.rawValue)
            let clusterIPWidth = min(max(storedClusterIP ?? widths.clusterIP, ServiceColumn.clusterIP.minimumWidth), ServiceColumn.clusterIP.maximumWidth)
            let dynamicNameWidth = max(
                RuneAppKitResourceListLayout.serviceMinimumNameColumnWidth,
                projectedWidth - typeWidth - clusterIPWidth - widths.favorite
            )
            let storedName = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: ServiceColumn.name.rawValue)
            let nameWidth = RuneAppKitResourceListLayout.viewportFillingFlexibleWidth(
                baseWidth: storedName ?? dynamicNameWidth,
                occupiedWidth: typeWidth + clusterIPWidth + widths.favorite,
                visibleWidth: visibleWidth,
                minimumWidth: ServiceColumn.name.minimumWidth,
                maximumWidth: ServiceColumn.name.maximumWidth
            )
            RuneAppKitResourceColumnResizeNotificationGate.withSuppressedNotifications(for: tableView) {
                for tableColumn in tableView.tableColumns {
                    guard let column = ServiceColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                    switch column {
                    case .name: tableColumn.width = nameWidth
                    case .type: tableColumn.width = typeWidth
                    case .clusterIP: tableColumn.width = clusterIPWidth
                    case .favorite: tableColumn.width = widths.favorite
                    }
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
            synchronizeResourceTableSortDescriptors(
                on: tableView,
                key: parent.sortColumn.rawValue,
                ascending: parent.sortAscending
            )
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
    var kind: KubeResourceKind? = nil
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
    @Environment(\.runeResolvedTheme) private var resolvedTheme
    @AppStorage(RuneSettingsKeys.showResourceTableScrollEdgeGlow) private var showsHorizontalOverflowEdgeGlow = true

    var resolvedResourceKind: KubeResourceKind? {
        kind ?? resources.first?.kind
    }

    var columnPresentation: RuneGenericResourceColumnPresentation {
        RuneGenericResourceColumnPresentation.resolve(for: resolvedResourceKind)
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = RuneAppKitResourceTableHost.make(
            coordinator: context.coordinator,
            resolvedTheme: resolvedTheme,
            columns: GenericResourceColumn.allCases.map { $0.tableColumn(presentation: columnPresentation) },
            resizableColumnIdentifiers: Set(GenericResourceColumn.allCases.filter(\.isUserResizable).map(\.rawValue)),
            onResetColumn: { [weak coordinator = context.coordinator] in coordinator?.resetColumnWidth($0) },
            onColumnResize: { [weak coordinator = context.coordinator] in coordinator?.tableViewColumnDidResize($0) },
            onVisibleWidthChanged: { [weak coordinator = context.coordinator] in coordinator?.updateColumnWidths() },
            onContextMenu: { [weak coordinator = context.coordinator] row, tableView in
                coordinator?.selectRowForContextMenu(row, in: tableView)
                return coordinator?.makeMenu(forRow: row)
            },
            onFavoriteToggle: { [weak coordinator = context.coordinator] in
                guard let tableView = coordinator?.tableView else { return }
                coordinator?.toggleFavoriteForSelectedRow(in: tableView)
            }
        )
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return scrollView }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
    }

    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        var parent: AppKitGenericResourceListView
        weak var tableView: NSTableView?
        private var isApplyingSelection = false
        private var applyGeneration = 0
        private var tableID: String {
            RuneGenericResourceColumnPresentation.tableID(for: parent.resolvedResourceKind)
        }

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
                self.updateGenericSortIndicator(on: tableView)
                self.updateColumnWidths(on: tableView)
                tableView.reloadData()
                self.applySelection(on: tableView)
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
                return RuneAppKitCenteredLabelCell(
                    text: resource.primaryText,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold),
                    alignment: parent.columnPresentation.primaryAlignment,
                    tooltip: resource.primaryText
                )
            case .secondary:
                return RuneAppKitCenteredLabelCell(
                    text: resource.secondaryText,
                    font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    alignment: parent.columnPresentation.secondaryAlignment,
                    tooltip: resource.secondaryText
                )
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
            if isSuppressedSynchronizedResourceColumnResize(notification) { return }
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
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyResourceTableSelection(selectedID: parent.selectedResourceID, rows: parent.resources, rowID: \.id, in: tableView)
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.genericColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = RuneAppKitResourceListLayout.availableColumnWidth(visibleWidth: visibleWidth)
            let presentation = parent.columnPresentation
            let storedPrimary = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: GenericResourceColumn.primary.rawValue)
            let primaryWidth = min(
                max(storedPrimary ?? widths.primary, GenericResourceColumn.primary.minimumWidth(presentation: presentation)),
                GenericResourceColumn.primary.maximumWidth
            )
            let storedSecondary = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: GenericResourceColumn.secondary.rawValue)
            let secondaryWidth = min(
                max(storedSecondary ?? widths.secondary, GenericResourceColumn.secondary.minimumWidth(presentation: presentation)),
                GenericResourceColumn.secondary.maximumWidth
            )
            let storedNamespace = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: GenericResourceColumn.namespace.rawValue)
            let namespaceWidth = min(
                max(storedNamespace ?? widths.namespace, GenericResourceColumn.namespace.minimumWidth(presentation: presentation)),
                GenericResourceColumn.namespace.maximumWidth
            )
            let dynamicNameWidth = max(
                RuneAppKitResourceListLayout.genericMinimumNameColumnWidth,
                projectedWidth - widths.selection - primaryWidth - secondaryWidth - namespaceWidth - widths.favorite
            )
            let storedName = RuneAppKitColumnWidthStore.shared.width(tableID: tableID, columnID: GenericResourceColumn.name.rawValue)
            let nameWidth = RuneAppKitResourceListLayout.viewportFillingFlexibleWidth(
                baseWidth: storedName ?? dynamicNameWidth,
                occupiedWidth: widths.selection + primaryWidth + secondaryWidth + namespaceWidth + widths.favorite,
                visibleWidth: visibleWidth,
                minimumWidth: GenericResourceColumn.name.minimumWidth(presentation: presentation),
                maximumWidth: GenericResourceColumn.name.maximumWidth
            )
            RuneAppKitResourceColumnResizeNotificationGate.withSuppressedNotifications(for: tableView) {
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
            synchronizeResourceTableSortDescriptors(
                on: tableView,
                key: parent.sortColumn.rawValue,
                ascending: parent.sortAscending
            )
            let presentation = parent.columnPresentation
            for tableColumn in tableView.tableColumns {
                guard let column = GenericResourceColumn(rawValue: tableColumn.identifier.rawValue) else { continue }
                let title = column.title(presentation: presentation)
                tableColumn.title = title
                tableColumn.headerToolTip = column.headerToolTip(presentation: presentation)
                tableColumn.minWidth = column.minimumWidth(presentation: presentation)
                if let headerCell = tableColumn.headerCell as? RuneAppKitResourceTableHeaderCell {
                    headerCell.configure(
                        title: title,
                        isSorted: column.sortColumn == parent.sortColumn,
                        ascending: parent.sortAscending,
                        reservesSortIndicator: column.sortColumn != nil,
                        alignment: column.alignment(presentation: presentation)
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
    @Environment(\.runeResolvedTheme) private var resolvedTheme
    @AppStorage(RuneSettingsKeys.showResourceTableScrollEdgeGlow) private var showsHorizontalOverflowEdgeGlow = true

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = RuneAppKitResourceTableHost.make(
            coordinator: context.coordinator,
            resolvedTheme: resolvedTheme,
            columns: HelmReleaseColumn.allCases.map { $0.tableColumn() },
            resizableColumnIdentifiers: Set(HelmReleaseColumn.allCases.filter(\.isUserResizable).map(\.rawValue)),
            onResetColumn: { [weak coordinator = context.coordinator] in coordinator?.resetColumnWidth($0) },
            onColumnResize: { [weak coordinator = context.coordinator] in coordinator?.tableViewColumnDidResize($0) },
            onVisibleWidthChanged: { [weak coordinator = context.coordinator] in coordinator?.updateColumnWidths() },
            onContextMenu: { [weak coordinator = context.coordinator] row, tableView in
                coordinator?.selectRowForContextMenu(row, in: tableView)
                return coordinator?.makeMenu(forRow: row)
            }
        )
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return scrollView }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
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
            if isSuppressedSynchronizedResourceColumnResize(notification) { return }
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
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyResourceTableSelection(selectedID: parent.selectedReleaseID, rows: parent.releases, rowID: \.id, in: tableView)
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.helmColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = RuneAppKitResourceListLayout.availableColumnWidth(visibleWidth: visibleWidth)
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
            let nameWidth = RuneAppKitResourceListLayout.viewportFillingFlexibleWidth(
                baseWidth: storedName ?? dynamicNameWidth,
                occupiedWidth: statusWidth + namespaceWidth + revisionWidth + chartWidth + appVersionWidth,
                visibleWidth: visibleWidth,
                minimumWidth: HelmReleaseColumn.name.minimumWidth,
                maximumWidth: HelmReleaseColumn.name.maximumWidth
            )
            RuneAppKitResourceColumnResizeNotificationGate.withSuppressedNotifications(for: tableView) {
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
            synchronizeResourceTableSortDescriptors(
                on: tableView,
                key: parent.sortColumn.rawValue,
                ascending: parent.sortAscending
            )
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
    @Environment(\.runeResolvedTheme) private var resolvedTheme
    @AppStorage(RuneSettingsKeys.showResourceTableScrollEdgeGlow) private var showsHorizontalOverflowEdgeGlow = true

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = RuneAppKitResourceTableHost.make(
            coordinator: context.coordinator,
            resolvedTheme: resolvedTheme,
            columns: EventColumn.allCases.map { $0.tableColumn() },
            resizableColumnIdentifiers: Set(EventColumn.allCases.filter(\.isUserResizable).map(\.rawValue)),
            onResetColumn: { [weak coordinator = context.coordinator] in coordinator?.resetColumnWidth($0) },
            onColumnResize: { [weak coordinator = context.coordinator] in coordinator?.tableViewColumnDidResize($0) },
            onVisibleWidthChanged: { [weak coordinator = context.coordinator] in coordinator?.updateColumnWidths() },
            onContextMenu: { [weak coordinator = context.coordinator] row, tableView in
                coordinator?.selectRowForContextMenu(row, in: tableView)
                return coordinator?.makeMenu(forRow: row)
            }
        )
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return scrollView }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
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
            if isSuppressedSynchronizedResourceColumnResize(notification) { return }
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
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyResourceTableSelection(selectedID: parent.selectedEventID, rows: parent.events, rowID: \.id, in: tableView)
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.eventColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = RuneAppKitResourceListLayout.availableColumnWidth(visibleWidth: visibleWidth)
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
            let reasonWidth = RuneAppKitResourceListLayout.viewportFillingFlexibleWidth(
                baseWidth: storedReason ?? dynamicReasonWidth,
                occupiedWidth: typeWidth + objectWidth + namespaceWidth + lastSeenWidth + messageWidth,
                visibleWidth: visibleWidth,
                minimumWidth: EventColumn.reason.minimumWidth,
                maximumWidth: EventColumn.reason.maximumWidth
            )
            RuneAppKitResourceColumnResizeNotificationGate.withSuppressedNotifications(for: tableView) {
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
            synchronizeResourceTableSortDescriptors(
                on: tableView,
                key: parent.sortColumn.rawValue,
                ascending: parent.sortAscending
            )
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
    @Environment(\.runeResolvedTheme) private var resolvedTheme
    @AppStorage(RuneSettingsKeys.showResourceTableScrollEdgeGlow) private var showsHorizontalOverflowEdgeGlow = true

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = RuneAppKitResourceTableHost.make(
            coordinator: context.coordinator,
            resolvedTheme: resolvedTheme,
            columns: OperatorResourceColumn.allCases.map { $0.tableColumn() },
            resizableColumnIdentifiers: Set(OperatorResourceColumn.allCases.filter(\.isUserResizable).map(\.rawValue)),
            onResetColumn: { [weak coordinator = context.coordinator] in coordinator?.resetColumnWidth($0) },
            onColumnResize: { [weak coordinator = context.coordinator] in coordinator?.tableViewColumnDidResize($0) },
            onVisibleWidthChanged: { [weak coordinator = context.coordinator] in coordinator?.updateColumnWidths() },
            onContextMenu: { [weak coordinator = context.coordinator] row, tableView in
                coordinator?.selectRowForContextMenu(row, in: tableView)
                return coordinator?.makeMenu(forRow: row)
            },
            onFavoriteToggle: { [weak coordinator = context.coordinator] in
                guard let tableView = coordinator?.tableView else { return }
                coordinator?.toggleFavoriteForSelectedRow(in: tableView)
            }
        )
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return scrollView }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let tableView = RuneAppKitResourceTableHost.tableView(in: scrollView) else { return }
        context.coordinator.tableView = tableView
        context.coordinator.apply(parent: self)
        RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)
        RuneAppKitResourceTableHost.configureHorizontalOverflowEdgeGlow(
            in: scrollView,
            isEnabled: showsHorizontalOverflowEdgeGlow
        )
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
            if isSuppressedSynchronizedResourceColumnResize(notification) { return }
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
            isApplyingSelection = true
            defer { isApplyingSelection = false }
            applyResourceTableSelection(selectedID: parent.selectedResourceID, rows: parent.resources, rowID: \.id, in: tableView)
        }

        func updateColumnWidths() {
            guard let tableView else { return }
            updateColumnWidths(on: tableView)
        }

        private func updateColumnWidths(on tableView: NSTableView) {
            let visibleWidth = tableView.enclosingScrollView?.contentView.bounds.width ?? tableView.bounds.width
            let widths = RuneAppKitResourceListLayout.operatorColumnWidths(visibleWidth: visibleWidth)
            let projectedWidth = RuneAppKitResourceListLayout.availableColumnWidth(visibleWidth: visibleWidth)
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
            let nameWidth = RuneAppKitResourceListLayout.viewportFillingFlexibleWidth(
                baseWidth: storedName ?? dynamicNameWidth,
                occupiedWidth: familyWidth + kindWidth + namespaceWidth + statusWidth + printerColumnsWidth + apiPathWidth + widths.favorite,
                visibleWidth: visibleWidth,
                minimumWidth: OperatorResourceColumn.name.minimumWidth,
                maximumWidth: OperatorResourceColumn.name.maximumWidth
            )
            RuneAppKitResourceColumnResizeNotificationGate.withSuppressedNotifications(for: tableView) {
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
            synchronizeResourceTableSortDescriptors(
                on: tableView,
                key: parent.sortColumn.rawValue,
                ascending: parent.sortAscending
            )
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
final class RuneAppKitResourceTableView: NSTableView {
    var resolvedTheme = RuneAppearanceTheme.native.resolvedTheme
    var onContextMenu: ((Int, NSTableView) -> NSMenu?)?
    var onFavoriteToggle: (() -> Void)?

    override func keyDown(with event: NSEvent) {
        if runeAppKitEventIsFavoriteToggle(event), let onFavoriteToggle {
            onFavoriteToggle()
            return
        }
        super.keyDown(with: event)
    }

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        return onContextMenu?(row, self)
    }
}

private func runeAppKitEventIsFavoriteToggle(_ event: NSEvent) -> Bool {
    let disallowedModifiers: NSEvent.ModifierFlags = [.command, .control, .option]
    guard event.modifierFlags.intersection(disallowedModifiers).isEmpty else { return false }
    return event.charactersIgnoringModifiers?.lowercased() == "f"
}

final class RuneAppKitResourceTableHeaderView: NSTableHeaderView {
    var resizableColumnIdentifiers: Set<String> = []
    var onResetColumn: ((String) -> Void)?
    var onColumnResizeCommitted: ((NSTableColumn) -> Void)?
    var horizontalInset: CGFloat = 0
    private var hoverTrackingArea: NSTrackingArea?
    private var hoveredSortableColumnIdentifier: String?

    var hoveredSortableColumnIdentifierForTesting: String? {
        hoveredSortableColumnIdentifier
    }

    func updateHoveredSortableColumnForTesting(at point: NSPoint) {
        updateHoveredColumn(at: point)
    }

    override func updateTrackingAreas() {
        if let hoverTrackingArea {
            removeTrackingArea(hoverTrackingArea)
        }
        let trackingArea = NSTrackingArea(
            rect: .zero,
            options: [.mouseEnteredAndExited, .mouseMoved, .activeInKeyWindow, .inVisibleRect],
            owner: self,
            userInfo: nil
        )
        addTrackingArea(trackingArea)
        hoverTrackingArea = trackingArea
        super.updateTrackingAreas()
    }

    override func mouseEntered(with event: NSEvent) {
        updateHoveredColumn(for: event)
    }

    override func mouseMoved(with event: NSEvent) {
        updateHoveredColumn(for: event)
    }

    override func mouseExited(with event: NSEvent) {
        setHoveredSortableColumnIdentifier(nil)
    }

    override func mouseDown(with event: NSEvent) {
        if event.clickCount == 2, let columnID = resizableColumnIdentifier(near: event) {
            onResetColumn?(columnID)
            return
        }
        if let column = resizableColumnResizeTarget(near: event) {
            trackColumnResize(column, from: event)
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
                if let headerCell = column.headerCell as? RuneAppKitResourceTableHeaderCell {
                    let showsHoverPreview = hoveredSortableColumnIdentifier == column.identifier.rawValue
                        && headerCell.reservesSortIndicator
                        && !headerCell.isSorted
                    if headerCell.isSorted || showsHoverPreview {
                        drawSortIndicator(
                            ascending: headerCell.isSorted ? headerCell.sortAscending : true,
                            active: headerCell.isSorted,
                            in: rect,
                            column: column
                        )
                    }
                }
                drawColumnDivider(at: rect.maxX, isResizable: resizableColumnIdentifiers.contains(column.identifier.rawValue))
            }
        }

        RuneAppKitResourceTableTheme.resolved(for: self).headerDivider.setFill()
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
        RuneAppKitResourceTableTheme.resolved(for: self).headerFill.setFill()
        NSBezierPath(
            roundedRect: rect,
            xRadius: RuneUILayoutMetrics.compactGlyphCornerRadius,
            yRadius: RuneUILayoutMetrics.compactGlyphCornerRadius
        ).fill()
    }

    private func resizableColumnIdentifier(near event: NSEvent) -> String? {
        resizableColumnResizeTarget(near: event)?.identifier.rawValue
    }

    private func resizableColumnResizeTarget(near event: NSEvent) -> NSTableColumn? {
        guard let tableView else { return nil }
        let point = convert(event.locationInWindow, from: nil)
        let resizeHitSlop = RuneAppKitResourceListLayout.sortIndicatorTrailingInset
            - RuneAppKitResourceListLayout.sortIndicatorResizeSafetyGap
        for columnIndex in 0..<tableView.tableColumns.count {
            let column = tableView.tableColumns[columnIndex]
            guard resizableColumnIdentifiers.contains(column.identifier.rawValue) else { continue }
            let dividerX = headerRect(ofColumn: columnIndex).maxX
            if abs(point.x - dividerX) <= resizeHitSlop {
                return column
            }
        }
        return nil
    }

    private func trackColumnResize(_ column: NSTableColumn, from event: NSEvent) {
        guard let window, let tableView else { return }
        let initialX = convert(event.locationInWindow, from: nil).x
        let initialWidth = column.width
        var didResize = false

        func applyWidth(from nextEvent: NSEvent) {
            let currentX = convert(nextEvent.locationInWindow, from: nil).x
            let proposedWidth = initialWidth + currentX - initialX
            let width = applySynchronizedResourceColumnResize(proposedWidth, for: column, in: tableView)
            if abs(width - initialWidth) >= 1 {
                didResize = true
            }
        }

        func latestResizeEvent(startingWith event: NSEvent) -> NSEvent {
            var latestEvent = event
            while latestEvent.type == .leftMouseDragged,
                  let pendingEvent = window.nextEvent(
                    matching: [.leftMouseDragged, .leftMouseUp],
                    until: Date(),
                    inMode: .eventTracking,
                    dequeue: true
                  ) {
                latestEvent = pendingEvent
            }
            return latestEvent
        }

        while true {
            guard let nextEvent = window.nextEvent(matching: [.leftMouseDragged, .leftMouseUp]) else { break }
            let resizeEvent = latestResizeEvent(startingWith: nextEvent)
            switch resizeEvent.type {
            case .leftMouseDragged:
                applyWidth(from: resizeEvent)
            case .leftMouseUp:
                applyWidth(from: resizeEvent)
                if didResize {
                    onColumnResizeCommitted?(column)
                }
                return
            default:
                break
            }
        }
    }

    private func drawColumnDivider(at x: CGFloat, isResizable: Bool) {
        guard x > bounds.minX + horizontalInset, x < bounds.maxX - horizontalInset else { return }
        let tableTheme = RuneAppKitResourceTableTheme.resolved(for: self)
        let color = isResizable ? tableTheme.columnDividerResizable : tableTheme.columnDivider
        color.setFill()
        NSRect(x: x.rounded(.down), y: bounds.minY + 4, width: 1, height: max(0, bounds.height - 8)).fill()
    }

    private func updateHoveredColumn(for event: NSEvent) {
        updateHoveredColumn(at: convert(event.locationInWindow, from: nil))
    }

    private func updateHoveredColumn(at point: NSPoint) {
        guard let tableView else {
            setHoveredSortableColumnIdentifier(nil)
            return
        }
        let resizeHitSlop = RuneAppKitResourceListLayout.sortIndicatorTrailingInset
            - RuneAppKitResourceListLayout.sortIndicatorResizeSafetyGap
        for columnIndex in 0..<tableView.tableColumns.count {
            let column = tableView.tableColumns[columnIndex]
            let rect = headerRect(ofColumn: columnIndex)
            guard rect.contains(point) else { continue }
            let nearResizeHandle = resizableColumnIdentifiers.contains(column.identifier.rawValue)
                && abs(point.x - rect.maxX) <= resizeHitSlop
            let isSortable = (column.headerCell as? RuneAppKitResourceTableHeaderCell)?.reservesSortIndicator == true
            setHoveredSortableColumnIdentifier(isSortable && !nearResizeHandle ? column.identifier.rawValue : nil)
            return
        }
        setHoveredSortableColumnIdentifier(nil)
    }

    private func setHoveredSortableColumnIdentifier(_ identifier: String?) {
        guard hoveredSortableColumnIdentifier != identifier else { return }
        hoveredSortableColumnIdentifier = identifier
        needsDisplay = true
    }

    private func drawSortIndicator(ascending: Bool, active: Bool, in rect: NSRect, column: NSTableColumn) {
        guard column.identifier.rawValue != PodColumn.selection.rawValue,
              column.identifier.rawValue != PodColumn.favorite.rawValue else { return }
        let indicatorRect = RuneAppKitResourceListLayout.sortIndicatorRect(in: rect)
        let verticalDirection: CGFloat = isFlipped ? -1 : 1
        let apexDirection = ascending ? verticalDirection : -verticalDirection
        let apexY = indicatorRect.midY + (apexDirection * 2)
        let baseY = indicatorRect.midY - (apexDirection * 2)

        let path = NSBezierPath()
        path.move(to: NSPoint(x: indicatorRect.minX + 1, y: baseY))
        path.line(to: NSPoint(x: indicatorRect.midX, y: apexY))
        path.line(to: NSPoint(x: indicatorRect.maxX - 1, y: baseY))
        path.lineWidth = 1.5
        path.lineCapStyle = .round
        path.lineJoinStyle = .round
        let headerColor = RuneAppKitResourceTableTheme.resolved(for: self).headerText
        (active ? headerColor : headerColor.withAlphaComponent(0.48)).setStroke()
        path.stroke()
    }
}

final class RuneAppKitResourceTableHeaderCell: NSTableHeaderCell {
    private var textAlignment: NSTextAlignment
    private var baseTitle = ""
    private(set) var isSorted = false
    private(set) var sortAscending = true
    private(set) var reservesSortIndicator = false

    init(alignment: NSTextAlignment) {
        self.textAlignment = alignment
        super.init(textCell: "")
        self.alignment = alignment
    }

    required init(coder: NSCoder) {
        self.textAlignment = .left
        super.init(coder: coder)
    }

    func configure(
        title: String,
        isSorted: Bool,
        ascending: Bool,
        reservesSortIndicator: Bool,
        alignment: NSTextAlignment? = nil
    ) {
        self.baseTitle = title
        self.isSorted = isSorted
        self.sortAscending = ascending
        self.reservesSortIndicator = reservesSortIndicator
        if let alignment {
            textAlignment = alignment
            self.alignment = alignment
        }
        stringValue = title
        setAccessibilityLabel(title)
        if reservesSortIndicator {
            setAccessibilityHelp("Click to sort by \(title)")
            setAccessibilityValue(isSorted ? (ascending ? "Sorted ascending" : "Sorted descending") : "Not sorted")
            setAccessibilitySortDirection(isSorted ? (ascending ? .ascending : .descending) : .unknown)
        } else {
            setAccessibilityHelp(nil)
            setAccessibilityValue(nil)
            setAccessibilitySortDirection(.unknown)
        }
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
            .foregroundColor: RuneAppKitResourceTableTheme.resolved(for: controlView).headerText,
            .paragraphStyle: paragraph
        ]
        let title = NSAttributedString(string: baseTitle.isEmpty ? stringValue : baseTitle, attributes: attributes)
        let leadingInset = RuneAppKitResourceTableStyle.contentLeadingInset
            + (textAlignment == .center && reservesSortIndicator
                ? RuneAppKitResourceListLayout.sortIndicatorSize.width + RuneAppKitResourceTableStyle.sortIndicatorGap
                : 0)
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

final class RuneAppKitResourceTableRowView: NSTableRowView {
    private let horizontalInset: CGFloat

    init(horizontalInset: CGFloat = RuneAppKitResourceTableStyle.rowHorizontalInset) {
        self.horizontalInset = horizontalInset
        super.init(frame: .zero)
    }

    required init?(coder: NSCoder) {
        self.horizontalInset = RuneAppKitResourceTableStyle.rowHorizontalInset
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
            y: bounds.minY,
            width: max(0, min(bounds.width, contentWidth) - (horizontalInset * 2)),
            height: bounds.height
        )
        let tableTheme = RuneAppKitResourceTableTheme.resolved(for: self)
        let fill = isSelected
            ? tableTheme.selectedRowFill
            : tableTheme.rowFill
        fill.setFill()
        rowRect.fill()

        if !isSelected {
            tableTheme.rowStroke.setFill()
            NSRect(
                x: rowRect.minX,
                y: max(rowRect.minY, rowRect.maxY - 1),
                width: rowRect.width,
                height: 1
            ).fill()
        }
    }
}

struct RuneHorizontalTableOverflowState: Equatable, Sendable {
    let hasOverflow: Bool
    let showsLeadingIndicator: Bool
    let showsTrailingIndicator: Bool

    static func resolve(
        visibleOriginX: CGFloat,
        visibleWidth: CGFloat,
        documentWidth: CGFloat,
        tolerance: CGFloat = 1
    ) -> RuneHorizontalTableOverflowState {
        let maximumOffset = max(0, documentWidth - visibleWidth)
        guard maximumOffset > tolerance else {
            return RuneHorizontalTableOverflowState(
                hasOverflow: false,
                showsLeadingIndicator: false,
                showsTrailingIndicator: false
            )
        }

        let offset = min(max(visibleOriginX, 0), maximumOffset)
        return RuneHorizontalTableOverflowState(
            hasOverflow: true,
            showsLeadingIndicator: offset > tolerance,
            showsTrailingIndicator: offset < maximumOffset - tolerance
        )
    }
}

final class RuneHorizontalTableOverflowIndicatorView: NSView {
    var isEnabled = true {
        didSet {
            guard isEnabled != oldValue else { return }
            updateVisibility()
            needsDisplay = true
        }
    }

    var state = RuneHorizontalTableOverflowState.resolve(
        visibleOriginX: 0,
        visibleWidth: 0,
        documentWidth: 0
    ) {
        didSet {
            guard state != oldValue else { return }
            updateVisibility()
            needsDisplay = true
        }
    }

    override var isOpaque: Bool { false }

    override func hitTest(_ point: NSPoint) -> NSView? {
        nil
    }

    override func draw(_ dirtyRect: NSRect) {
        if state.showsLeadingIndicator {
            drawIndicator(atLeadingEdge: true)
        }
        if state.showsTrailingIndicator {
            drawIndicator(atLeadingEdge: false)
        }
    }

    override func isAccessibilityElement() -> Bool {
        false
    }

    private func updateVisibility() {
        isHidden = !isEnabled || !(state.showsLeadingIndicator || state.showsTrailingIndicator)
    }

    private func drawIndicator(atLeadingEdge: Bool) {
        let indicatorWidth: CGFloat = 18
        let rect = NSRect(
            x: atLeadingEdge ? bounds.minX : bounds.maxX - indicatorWidth,
            y: bounds.minY,
            width: indicatorWidth,
            height: bounds.height
        )
        let edgeColor = NSColor.separatorColor.withAlphaComponent(0.22)
        let colors = atLeadingEdge
            ? [edgeColor, NSColor.clear]
            : [NSColor.clear, edgeColor]
        NSGradient(colors: colors)?.draw(in: rect, angle: 0)
    }
}

@MainActor
final class RuneAppKitResourceListScrollView: NSScrollView {
    var onVisibleWidthChanged: (() -> Void)?
    var showsHorizontalOverflowEdgeGlow = true {
        didSet {
            guard showsHorizontalOverflowEdgeGlow != oldValue else { return }
            horizontalOverflowIndicator.isEnabled = showsHorizontalOverflowEdgeGlow
        }
    }
    var horizontalOverflowStateForTesting: RuneHorizontalTableOverflowState {
        horizontalOverflowIndicator.state
    }
    var isHorizontalOverflowEdgeGlowVisibleForTesting: Bool {
        !horizontalOverflowIndicator.isHidden
    }
    private var lastVisibleWidth: CGFloat = -1
    private var isSendingVisibleWidthChange = false
    private let horizontalOverflowIndicator = RuneHorizontalTableOverflowIndicatorView()

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        installHorizontalOverflowIndicator()
    }

    required init?(coder: NSCoder) {
        super.init(coder: coder)
        installHorizontalOverflowIndicator()
    }

    override func layout() {
        super.layout()
        horizontalOverflowIndicator.frame = contentView.frame
        updateHorizontalOverflowIndicator()
        notifyVisibleWidthChangedIfNeeded()
    }

    override func reflectScrolledClipView(_ clipView: NSClipView) {
        super.reflectScrolledClipView(clipView)
        updateHorizontalOverflowIndicator()
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

    private func installHorizontalOverflowIndicator() {
        horizontalOverflowIndicator.isHidden = true
        addSubview(horizontalOverflowIndicator, positioned: .above, relativeTo: contentView)
    }

    private func updateHorizontalOverflowIndicator() {
        let visibleRect = contentView.documentVisibleRect
        let state = RuneHorizontalTableOverflowState.resolve(
            visibleOriginX: visibleRect.minX,
            visibleWidth: visibleRect.width,
            documentWidth: documentView?.frame.width ?? visibleRect.width
        )
        horizontalOverflowIndicator.state = state
        setAccessibilityHelp(
            state.hasOverflow
                ? "More columns are available. Scroll horizontally to reveal them."
                : nil
        )
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
        button.setAccessibilityValue(isFavorite ? "Selected" : "Not selected")
        button.setAccessibilityHelp(button.toolTip)
        button.translatesAutoresizingMaskIntoConstraints = false
        addSubview(button)
        NSLayoutConstraint.activate([
            button.centerXAnchor.constraint(equalTo: centerXAnchor),
            button.centerYAnchor.constraint(equalTo: centerYAnchor),
            button.widthAnchor.constraint(equalToConstant: RuneUILayoutMetrics.iconButtonSize),
            button.heightAnchor.constraint(equalToConstant: RuneUILayoutMetrics.iconButtonSize)
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
        layer?.masksToBounds = true
        translatesAutoresizingMaskIntoConstraints = false
        label.font = .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold)
        label.textColor = color
        label.alignment = .center
        label.lineBreakMode = .byTruncatingTail
        label.maximumNumberOfLines = 1
        label.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
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
        case .name:
            return max(
                RuneAppKitResourceListLayout.deploymentMinimumNameColumnWidth,
                RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: title, reservesSortIndicator: true)
            )
        case .replicas:
            return max(
                RuneAppKitResourceListLayout.deploymentReplicaColumnWidth,
                RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: title, reservesSortIndicator: true)
            )
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
        case .name:
            return max(
                RuneAppKitResourceListLayout.serviceMinimumNameColumnWidth,
                RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: title, reservesSortIndicator: true)
            )
        case .type, .clusterIP:
            return max(
                width,
                RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: title, reservesSortIndicator: true)
            )
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
    func tableColumn(presentation: RuneGenericResourceColumnPresentation) -> NSTableColumn {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(rawValue))
        let alignment = alignment(presentation: presentation)
        let title = title(presentation: presentation)
        let headerCell = RuneAppKitResourceTableHeaderCell(alignment: alignment)
        headerCell.configure(title: title, isSorted: false, ascending: true, reservesSortIndicator: sortColumn != nil)
        column.headerCell = headerCell
        column.title = title
        column.headerToolTip = headerToolTip(presentation: presentation)
        if let sortColumn {
            column.sortDescriptorPrototype = NSSortDescriptor(key: sortColumn.rawValue, ascending: true)
        }
        column.resizingMask = isUserResizable ? .userResizingMask : []
        column.width = width
        column.minWidth = minimumWidth(presentation: presentation)
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

    func title(presentation: RuneGenericResourceColumnPresentation) -> String {
        switch self {
        case .selection: return ""
        case .name: return "Name"
        case .primary: return presentation.primaryTitle
        case .secondary: return presentation.secondaryTitle
        case .namespace: return "Namespace"
        case .favorite: return ""
        }
    }

    func alignment(presentation: RuneGenericResourceColumnPresentation) -> NSTextAlignment {
        switch self {
        case .primary: return presentation.primaryAlignment
        case .secondary: return presentation.secondaryAlignment
        case .namespace, .selection, .favorite, .name: return .left
        }
    }

    func headerToolTip(presentation: RuneGenericResourceColumnPresentation) -> String? {
        switch self {
        case .selection:
            return "Select resources for bulk actions"
        case .name:
            return "Resource name. Click to sort."
        case .primary:
            return "\(presentation.primaryTitle) for each resource. Click to sort."
        case .secondary:
            return "\(presentation.secondaryTitle) for each resource. Click to sort."
        case .namespace:
            return "Kubernetes namespace. Click to sort."
        case .favorite:
            return "Favorite resource"
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

    func minimumWidth(presentation: RuneGenericResourceColumnPresentation) -> CGFloat {
        guard sortColumn != nil else { return minimumWidth }
        return max(
            minimumWidth,
            RuneAppKitResourceListLayout.minimumHeaderColumnWidth(
                title: title(presentation: presentation),
                reservesSortIndicator: true
            )
        )
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
        let base: CGFloat = switch self {
        case .name: RuneAppKitResourceListLayout.helmMinimumNameColumnWidth
        case .status:
            RuneAppKitResourceListLayout.helmStatusMinimumColumnWidth
        case .namespace:
            RuneAppKitResourceListLayout.helmNamespaceMinimumColumnWidth
        case .revision:
            RuneAppKitResourceListLayout.helmRevisionMinimumColumnWidth
        case .chart:
            RuneAppKitResourceListLayout.helmChartMinimumColumnWidth
        case .appVersion:
            RuneAppKitResourceListLayout.helmAppVersionMinimumColumnWidth
        }
        return max(
            base,
            RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: title, reservesSortIndicator: true)
        )
    }

    var maximumWidth: CGFloat {
        switch self {
        case .name: return 10_000
        case .status: return 220
        case .namespace: return 280
        case .revision: return 150
        case .chart: return 420
        case .appVersion: return 240
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
        let base: CGFloat = switch self {
        case .reason:
            RuneAppKitResourceListLayout.eventMinimumReasonColumnWidth
        case .type:
            RuneAppKitResourceListLayout.eventTypeMinimumColumnWidth
        case .object:
            RuneAppKitResourceListLayout.eventObjectMinimumColumnWidth
        case .namespace:
            RuneAppKitResourceListLayout.eventNamespaceMinimumColumnWidth
        case .lastSeen:
            RuneAppKitResourceListLayout.eventLastSeenMinimumColumnWidth
        case .message:
            RuneAppKitResourceListLayout.eventMessageMinimumColumnWidth
        }
        return max(
            base,
            RuneAppKitResourceListLayout.minimumHeaderColumnWidth(
                title: title,
                reservesSortIndicator: sortColumn != nil
            )
        )
    }

    var maximumWidth: CGFloat {
        switch self {
        case .reason: return 10_000
        case .type: return 220
        case .object: return 420
        case .namespace: return 280
        case .lastSeen: return 320
        case .message: return 10_000
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
        let base: CGFloat = switch self {
        case .name:
            RuneAppKitResourceListLayout.operatorMinimumNameColumnWidth
        case .family:
            RuneAppKitResourceListLayout.operatorFamilyMinimumColumnWidth
        case .kind:
            RuneAppKitResourceListLayout.operatorKindMinimumColumnWidth
        case .namespace:
            RuneAppKitResourceListLayout.operatorNamespaceMinimumColumnWidth
        case .status:
            RuneAppKitResourceListLayout.operatorStatusMinimumColumnWidth
        case .printerColumns:
            120
        case .apiPath:
            RuneAppKitResourceListLayout.operatorAPIPathMinimumColumnWidth
        case .favorite:
            width
        }
        guard self != .favorite else { return base }
        return max(
            base,
            RuneAppKitResourceListLayout.minimumHeaderColumnWidth(
                title: title,
                reservesSortIndicator: sortColumn != nil
            )
        )
    }

    var maximumWidth: CGFloat {
        switch self {
        case .name: return 10_000
        case .family: return 280
        case .kind: return 300
        case .namespace: return 280
        case .status: return 260
        case .printerColumns: return 360
        case .apiPath: return 10_000
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
        column.minWidth = minimumWidth
        column.maxWidth = maximumWidth
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

    var minimumWidth: CGFloat {
        switch self {
        case .name:
            return max(
                PodTableLayout.nameColumnMinimumWidth,
                RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: title, reservesSortIndicator: true)
            )
        case .cpu, .memory, .restarts, .age, .status:
            return max(
                width(nameColumnWidth: PodTableLayout.nameColumnDefaultWidth),
                RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: title, reservesSortIndicator: true)
            )
        case .selection, .favorite:
            return width(nameColumnWidth: PodTableLayout.nameColumnDefaultWidth)
        }
    }

    var maximumWidth: CGFloat {
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
