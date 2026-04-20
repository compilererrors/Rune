import AppKit
import SwiftUI

struct AppKitTripleSplitWidthState {
    var desiredSidebarWidth: CGFloat = RuneUILayoutMetrics.splitSidebarMinWidth
    var desiredDetailWidth: CGFloat = RuneUILayoutMetrics.splitDetailColumnIdealWidth
    var hasAppliedInitialRestore = false
    var needsWidthRestore = true
    var lastAppliedContainerWidth: CGFloat = 0

    private var pendingUserDrivenSidebarWidth: CGFloat?
    private var pendingUserDrivenDetailWidth: CGFloat?

    mutating func shouldApplyOnLayout(containerWidth: CGFloat) -> Bool {
        needsWidthRestore || !hasAppliedInitialRestore || abs(containerWidth - lastAppliedContainerWidth) > 1
    }

    mutating func registerRequestedWidths(
        sidebarWidth: CGFloat,
        detailWidth: CGFloat,
        actualSidebarWidth: CGFloat,
        actualDetailWidth: CGFloat
    ) -> Bool {
        let resolvedSidebarWidth = Self.resolveRequestedWidth(
            sidebarWidth,
            pendingUserDrivenWidth: &pendingUserDrivenSidebarWidth,
            actualWidth: actualSidebarWidth,
            minWidth: RuneUILayoutMetrics.splitSidebarMinWidth,
            maxWidth: RuneUILayoutMetrics.splitSidebarMaxWidth
        )
        let resolvedDetailWidth = Self.resolveRequestedWidth(
            detailWidth,
            pendingUserDrivenWidth: &pendingUserDrivenDetailWidth,
            actualWidth: actualDetailWidth,
            minWidth: RuneUILayoutMetrics.splitDetailColumnMinWidth,
            maxWidth: RuneUILayoutMetrics.splitDetailColumnMaxWidth
        )
        let widthsChanged = abs(desiredSidebarWidth - resolvedSidebarWidth) > 1
            || abs(desiredDetailWidth - resolvedDetailWidth) > 1

        desiredSidebarWidth = resolvedSidebarWidth
        desiredDetailWidth = resolvedDetailWidth

        if widthsChanged || !hasAppliedInitialRestore {
            needsWidthRestore = true
        }

        return needsWidthRestore || !hasAppliedInitialRestore
    }

    mutating func noteUserResize(
        actualSidebarWidth: CGFloat,
        actualDetailWidth: CGFloat,
        containerWidth: CGFloat
    ) {
        guard hasAppliedInitialRestore else { return }
        guard abs(containerWidth - lastAppliedContainerWidth) <= 1 else { return }

        desiredSidebarWidth = actualSidebarWidth
        desiredDetailWidth = actualDetailWidth
        pendingUserDrivenSidebarWidth = actualSidebarWidth
        pendingUserDrivenDetailWidth = actualDetailWidth
        needsWidthRestore = false
    }

    mutating func noteRestoreAttempt(containerWidth: CGFloat) {
        lastAppliedContainerWidth = containerWidth
    }

    mutating func noteRestoreSettled() {
        hasAppliedInitialRestore = true
        needsWidthRestore = false
    }

    private static func resolveRequestedWidth(
        _ width: CGFloat,
        pendingUserDrivenWidth: inout CGFloat?,
        actualWidth: CGFloat,
        minWidth: CGFloat,
        maxWidth: CGFloat
    ) -> CGFloat {
        let clampedWidth = min(max(width, minWidth), maxWidth)
        guard let pendingWidth = pendingUserDrivenWidth else {
            return clampedWidth
        }

        if abs(clampedWidth - pendingWidth) <= 1 {
            pendingUserDrivenWidth = nil
            return clampedWidth
        }

        if abs(actualWidth - pendingWidth) <= 1 {
            return pendingWidth
        }

        pendingUserDrivenWidth = nil
        return clampedWidth
    }
}

struct AppKitTripleSplitView: NSViewControllerRepresentable {
    let sidebar: AnyView
    let content: AnyView
    let detail: AnyView
    let sidebarWidth: CGFloat
    let detailWidth: CGFloat
    let onSidebarWidthChange: (CGFloat) -> Void
    let onDetailWidthChange: (CGFloat) -> Void

    func makeNSViewController(context: Context) -> Controller {
        Controller()
    }

    func updateNSViewController(_ controller: Controller, context: Context) {
        controller.update(
            sidebar: sidebar,
            content: content,
            detail: detail,
            sidebarWidth: sidebarWidth,
            detailWidth: detailWidth,
            onSidebarWidthChange: onSidebarWidthChange,
            onDetailWidthChange: onDetailWidthChange
        )
    }
}

extension AppKitTripleSplitView {
    @MainActor
    final class Controller: NSSplitViewController {
        private let sidebarController = NSHostingController(rootView: AnyView(EmptyView()))
        private let contentController = NSHostingController(rootView: AnyView(EmptyView()))
        private let detailController = NSHostingController(rootView: AnyView(EmptyView()))

        private lazy var sidebarItem: NSSplitViewItem = {
            let item = NSSplitViewItem(viewController: sidebarController)
            item.minimumThickness = RuneUILayoutMetrics.splitSidebarMinWidth
            item.maximumThickness = RuneUILayoutMetrics.splitSidebarMaxWidth
            item.canCollapse = false
            return item
        }()

        private lazy var contentItem: NSSplitViewItem = {
            let item = NSSplitViewItem(viewController: contentController)
            item.minimumThickness = RuneUILayoutMetrics.splitContentColumnMinWidth
            item.canCollapse = false
            return item
        }()

        private lazy var detailItem: NSSplitViewItem = {
            let item = NSSplitViewItem(viewController: detailController)
            item.minimumThickness = RuneUILayoutMetrics.splitDetailColumnMinWidth
            item.maximumThickness = RuneUILayoutMetrics.splitDetailColumnMaxWidth
            item.canCollapse = false
            return item
        }()

        private var widthState = AppKitTripleSplitWidthState()
        private var onSidebarWidthChange: ((CGFloat) -> Void)?
        private var onDetailWidthChange: ((CGFloat) -> Void)?
        private var hasReceivedInitialConfiguration = false
        private var pendingRestore = false

        override func viewDidLoad() {
            super.viewDidLoad()
            splitView.isVertical = true
            splitView.delegate = self
            addSplitViewItem(sidebarItem)
            addSplitViewItem(contentItem)
            addSplitViewItem(detailItem)
        }

        override func viewDidLayout() {
            super.viewDidLayout()
            guard hasReceivedInitialConfiguration else { return }
            guard widthState.shouldApplyOnLayout(containerWidth: splitView.bounds.width) else { return }
            applyDesiredWidthsIfNeeded()
        }

        func update(
            sidebar: AnyView,
            content: AnyView,
            detail: AnyView,
            sidebarWidth: CGFloat,
            detailWidth: CGFloat,
            onSidebarWidthChange: @escaping (CGFloat) -> Void,
            onDetailWidthChange: @escaping (CGFloat) -> Void
        ) {
            sidebarController.rootView = sidebar
            contentController.rootView = content
            detailController.rootView = detail
            self.onSidebarWidthChange = onSidebarWidthChange
            self.onDetailWidthChange = onDetailWidthChange
            hasReceivedInitialConfiguration = true

            guard widthState.registerRequestedWidths(
                sidebarWidth: sidebarWidth,
                detailWidth: detailWidth,
                actualSidebarWidth: sidebarController.view.frame.width,
                actualDetailWidth: detailController.view.frame.width
            ) else { return }

            applyDesiredWidthsIfNeeded()
        }

        override func splitViewDidResizeSubviews(_ notification: Notification) {
            widthState.noteUserResize(
                actualSidebarWidth: sidebarController.view.frame.width,
                actualDetailWidth: detailController.view.frame.width,
                containerWidth: splitView.bounds.width
            )
            guard widthState.hasAppliedInitialRestore else { return }
            reportWidthsIfNeeded()
        }

        private func applyDesiredWidthsIfNeeded() {
            guard splitViewItems.count == 3 else { return }

            let totalWidth = splitView.bounds.width
            guard totalWidth > 0 else { return }
            widthState.noteRestoreAttempt(containerWidth: totalWidth)

            let dividerThickness = splitView.dividerThickness
            let availableWidth = totalWidth - (dividerThickness * 2)
            guard availableWidth > 0 else { return }

            let sidebar = targetSidebarWidth(for: availableWidth)
            let detail = targetDetailWidth(for: availableWidth, sidebarWidth: sidebar)
            sidebarItem.preferredThicknessFraction = sidebar / totalWidth
            detailItem.preferredThicknessFraction = detail / totalWidth

            let actualSidebar = sidebarController.view.frame.width
            let actualDetail = detailController.view.frame.width

            guard abs(actualSidebar - sidebar) > 1 || abs(actualDetail - detail) > 1 else {
                widthState.noteRestoreSettled()
                reportWidthsIfNeeded()
                return
            }

            guard !pendingRestore else { return }
            pendingRestore = true
            splitView.setPosition(sidebar, ofDividerAt: 0)
            splitView.setPosition(totalWidth - dividerThickness - detail, ofDividerAt: 1)
            splitView.adjustSubviews()

            DispatchQueue.main.async { [weak self] in
                self?.pendingRestore = false
                self?.applyDesiredWidthsIfNeeded()
            }
        }

        private func targetSidebarWidth(for availableWidth: CGFloat) -> CGFloat {
            let requested = min(
                max(widthState.desiredSidebarWidth, RuneUILayoutMetrics.splitSidebarMinWidth),
                RuneUILayoutMetrics.splitSidebarMaxWidth
            )
            let maxSidebar = min(
                RuneUILayoutMetrics.splitSidebarMaxWidth,
                max(
                    RuneUILayoutMetrics.splitSidebarMinWidth,
                    availableWidth - RuneUILayoutMetrics.splitContentColumnMinWidth - RuneUILayoutMetrics.splitDetailColumnMinWidth
                )
            )
            return min(requested, maxSidebar)
        }

        private func targetDetailWidth(for availableWidth: CGFloat, sidebarWidth: CGFloat) -> CGFloat {
            let requested = min(
                max(widthState.desiredDetailWidth, RuneUILayoutMetrics.splitDetailColumnMinWidth),
                RuneUILayoutMetrics.splitDetailColumnMaxWidth
            )
            let maxDetail = min(
                RuneUILayoutMetrics.splitDetailColumnMaxWidth,
                max(
                    RuneUILayoutMetrics.splitDetailColumnMinWidth,
                    availableWidth - RuneUILayoutMetrics.splitContentColumnMinWidth - sidebarWidth
                )
            )
            return min(requested, maxDetail)
        }

        private func reportWidthsIfNeeded() {
            onSidebarWidthChange?(sidebarController.view.frame.width)
            onDetailWidthChange?(detailController.view.frame.width)
        }
    }
}
