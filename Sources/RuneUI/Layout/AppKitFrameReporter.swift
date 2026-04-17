import AppKit
import SwiftUI

/// Ground-truth `NSView` frames for layout debugging. Enable with `RUNE_DEBUG_APPKIT_FRAMES=1`.
enum RuneAppKitFrameDebug {
    static var isEnabled: Bool {
        ProcessInfo.processInfo.environment["RUNE_DEBUG_APPKIT_FRAMES"] == "1"
    }
}

struct AppKitFrameReporterRepresentable: NSViewRepresentable {
    let label: String

    final class ReportingView: NSView {
        var label: String = ""
        private var lastLogSignature: String?

        override func layout() {
            super.layout()
            guard RuneAppKitFrameDebug.isEnabled else { return }
            guard window != nil else { return }
            let bounds = self.bounds
            let inWindow = convert(bounds, to: nil)
            let sig = String(
                format: "%@|%.2f|%.2f|%.2f|%.2f",
                label, inWindow.origin.x, inWindow.origin.y, inWindow.size.width, inWindow.size.height
            )
            guard sig != lastLogSignature else { return }
            lastLogSignature = sig
            NSLog(
                "[Rune][AppKitFrame] label=%@ bounds=(%.1f,%.1f,%.1fx%.1f) inWindow=(%.1f,%.1f,%.1fx%.1f)",
                label,
                bounds.origin.x, bounds.origin.y, bounds.size.width, bounds.size.height,
                inWindow.origin.x, inWindow.origin.y, inWindow.size.width, inWindow.size.height
            )
        }
    }

    func makeNSView(context: Context) -> ReportingView {
        let v = ReportingView(frame: .zero)
        v.wantsLayer = true
        v.layer?.backgroundColor = NSColor.clear.cgColor
        return v
    }

    func updateNSView(_ nsView: ReportingView, context: Context) {
        nsView.label = label
    }
}

extension View {
    /// Attaches an invisible `NSView` that logs stable window-space frames when `RUNE_DEBUG_APPKIT_FRAMES=1`.
    @ViewBuilder
    func runeAppKitFrameReporter(_ label: String) -> some View {
        if RuneAppKitFrameDebug.isEnabled {
            background(AppKitFrameReporterRepresentable(label: label))
        } else {
            self
        }
    }
}
