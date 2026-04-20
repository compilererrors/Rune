import AppKit
import SwiftUI

/// Restores divider positions for SwiftUI `HSplitView` by talking to the backing `NSSplitView` directly.
struct AppKitSplitViewRestorer: NSViewRepresentable {
    let sidebarWidth: CGFloat
    let detailWidth: CGFloat
    let onInitialRestore: () -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator(onInitialRestore: onInitialRestore)
    }

    func makeNSView(context: Context) -> TrackingView {
        let view = TrackingView(frame: .zero)
        view.coordinator = context.coordinator
        return view
    }

    func updateNSView(_ nsView: TrackingView, context: Context) {
        context.coordinator.onInitialRestore = onInitialRestore
        context.coordinator.desiredSidebarWidth = sidebarWidth
        context.coordinator.desiredDetailWidth = detailWidth
        context.coordinator.scheduleApply(from: nsView)
    }
}

extension AppKitSplitViewRestorer {
    final class TrackingView: NSView {
        weak var coordinator: Coordinator?

        override func viewDidMoveToWindow() {
            super.viewDidMoveToWindow()
            coordinator?.scheduleApply(from: self)
        }

        override func viewDidMoveToSuperview() {
            super.viewDidMoveToSuperview()
            coordinator?.scheduleApply(from: self)
        }

        override func layout() {
            super.layout()
            coordinator?.scheduleApply(from: self)
        }
    }

    @MainActor
    final class Coordinator {
        var desiredSidebarWidth: CGFloat = RuneUILayoutMetrics.splitSidebarMinWidth
        var desiredDetailWidth: CGFloat = RuneUILayoutMetrics.splitDetailColumnIdealWidth
        var onInitialRestore: () -> Void

        private var didReportInitialRestore = false
        private var applyScheduled = false

        init(onInitialRestore: @escaping () -> Void) {
            self.onInitialRestore = onInitialRestore
        }

        func scheduleApply(from view: NSView) {
            guard !applyScheduled else { return }
            applyScheduled = true

            DispatchQueue.main.async { [weak self, weak view] in
                guard let self, let view else { return }
                self.applyScheduled = false
                self.applyIfPossible(from: view)
            }
        }

        private func applyIfPossible(from view: NSView) {
            guard let splitView = enclosingSplitView(from: view) else { return }
            guard splitView.isVertical, splitView.arrangedSubviews.count == 3 else {
                reportInitialRestoreIfNeeded()
                return
            }

            let totalWidth = splitView.bounds.width
            guard totalWidth > 0 else { return }

            let dividerThickness = splitView.dividerThickness
            let availableWidth = totalWidth - (dividerThickness * 2)
            guard availableWidth > 0 else { return }

            let requestedSidebar = clampedSidebarWidth(desiredSidebarWidth)
            let requestedDetail = clampedDetailWidth(desiredDetailWidth)

            let maxSidebar = min(
                RuneUILayoutMetrics.splitSidebarMaxWidth,
                max(
                    RuneUILayoutMetrics.splitSidebarMinWidth,
                    availableWidth - RuneUILayoutMetrics.splitContentColumnMinWidth - RuneUILayoutMetrics.splitDetailColumnMinWidth
                )
            )
            let sidebar = min(requestedSidebar, maxSidebar)

            let maxDetail = min(
                RuneUILayoutMetrics.splitDetailColumnMaxWidth,
                max(
                    RuneUILayoutMetrics.splitDetailColumnMinWidth,
                    availableWidth - RuneUILayoutMetrics.splitContentColumnMinWidth - sidebar
                )
            )
            let detail = min(requestedDetail, maxDetail)

            let actualSidebar = splitView.arrangedSubviews[0].frame.width
            let actualDetail = splitView.arrangedSubviews[2].frame.width

            if abs(actualSidebar - sidebar) > 1 || abs(actualDetail - detail) > 1 {
                splitView.setPosition(sidebar, ofDividerAt: 0)
                splitView.setPosition(totalWidth - dividerThickness - detail, ofDividerAt: 1)
                splitView.adjustSubviews()
                scheduleApply(from: view)
                return
            }

            reportInitialRestoreIfNeeded()
        }

        private func clampedSidebarWidth(_ width: CGFloat) -> CGFloat {
            min(max(width, RuneUILayoutMetrics.splitSidebarMinWidth), RuneUILayoutMetrics.splitSidebarMaxWidth)
        }

        private func clampedDetailWidth(_ width: CGFloat) -> CGFloat {
            min(max(width, RuneUILayoutMetrics.splitDetailColumnMinWidth), RuneUILayoutMetrics.splitDetailColumnMaxWidth)
        }

        private func reportInitialRestoreIfNeeded() {
            guard !didReportInitialRestore else { return }
            didReportInitialRestore = true
            DispatchQueue.main.async { [onInitialRestore] in
                onInitialRestore()
            }
        }

        private func enclosingSplitView(from view: NSView) -> NSSplitView? {
            var current: NSView? = view
            while let unwrapped = current {
                if let splitView = unwrapped as? NSSplitView {
                    return splitView
                }
                current = unwrapped.superview
            }

            if let rootView = view.window?.contentView,
               let splitView = firstSplitView(in: rootView) {
                return splitView
            }

            return nil
        }

        private func firstSplitView(in view: NSView) -> NSSplitView? {
            if let splitView = view as? NSSplitView,
               splitView.isVertical,
               splitView.arrangedSubviews.count == 3 {
                return splitView
            }

            for subview in view.subviews {
                if let splitView = firstSplitView(in: subview) {
                    return splitView
                }
            }

            return nil
        }
    }
}
