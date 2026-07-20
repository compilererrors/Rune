import AppKit
import SwiftUI

struct WindowChromeConfigurator: NSViewRepresentable {
    let onMeasuredTopInsetChange: (CGFloat) -> Void
    let onOwningWindowChange: (Int?) -> Void

    final class Coordinator {
        var configuredWindowNumber: Int?
        var lastMeasuredTopInset: CGFloat?
        var lastReportedWindowNumber: Int?
    }

    final class TrackingView: NSView {
        var onGeometryChange: ((TrackingView) -> Void)?

        override func viewDidMoveToWindow() {
            super.viewDidMoveToWindow()
            reportGeometryChange()
        }

        override func layout() {
            super.layout()
            reportGeometryChange()
        }

        private func reportGeometryChange() {
            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                self.onGeometryChange?(self)
            }
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeNSView(context: Context) -> TrackingView {
        TrackingView(frame: .zero)
    }

    func updateNSView(_ nsView: TrackingView, context: Context) {
        nsView.onGeometryChange = { trackingView in
            let resolvedWindowNumber = trackingView.window?.windowNumber
            if context.coordinator.lastReportedWindowNumber != resolvedWindowNumber {
                context.coordinator.lastReportedWindowNumber = resolvedWindowNumber
                onOwningWindowChange(resolvedWindowNumber)
            }

            guard let window = trackingView.window else { return }
            let windowNumber = window.windowNumber
            if context.coordinator.configuredWindowNumber != windowNumber {
                context.coordinator.configuredWindowNumber = windowNumber

                // Keep root content consistently below titlebar/toolbar while using the
                // modern unified toolbar style instead of full-size-content overlays.
                window.titlebarAppearsTransparent = false
                window.styleMask.remove(.fullSizeContentView)
                window.titleVisibility = .hidden
                window.toolbarStyle = .unified
                window.toolbar?.showsBaselineSeparator = false
            }

            let inset = trackingView.safeAreaInsets.top
            guard context.coordinator.lastMeasuredTopInset != inset else { return }

            context.coordinator.lastMeasuredTopInset = inset
            onMeasuredTopInsetChange(inset)
        }
    }
}
