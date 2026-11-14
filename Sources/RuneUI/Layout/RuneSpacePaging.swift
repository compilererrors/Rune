import AppKit
import SwiftUI

/// Keyboard scrolling for read-only inspector documents (Logs, YAML, Describe
/// and other inspector content), the way a browser or Preview does:
/// Space / Shift+Space page, Up / Down move one line, Page Up / Page Down
/// page, Home / End jump to the ends. It runs in the focused text view's
/// `keyDown`, or in the window's key monitor when the inspector pane has
/// keyboard focus, so it costs nothing per frame. Editable text, fields,
/// focused buttons and terminals never route here, so typing is unchanged.
@MainActor
enum RuneSpacePaging {
    enum Direction: Equatable {
        case down
        case up
    }

    enum Command: Equatable {
        case pageDown
        case pageUp
        case lineDown
        case lineUp
        case top
        case bottom
    }

    /// The paging direction for an unmodified Space (down) or Shift+Space
    /// (up), or nil for any other key or modifier.
    static func direction(for event: NSEvent) -> Direction? {
        switch command(for: event) {
        case .pageDown where event.keyCode == 49: return .down
        case .pageUp where event.keyCode == 49: return .up
        default: return nil
        }
    }

    /// The scroll command for a key, or nil when the key should keep its
    /// normal meaning (any Command/Option/Control combination, Shift+arrows
    /// for selection, and every other key).
    static func command(for event: NSEvent) -> Command? {
        guard event.type == .keyDown else { return nil }
        let modifiers = event.modifierFlags
            .intersection(.deviceIndependentFlagsMask)
            .subtracting([.capsLock, .numericPad, .function])
        if event.charactersIgnoringModifiers == " " {
            if modifiers.isEmpty { return .pageDown }
            if modifiers == .shift { return .pageUp }
            return nil
        }
        guard modifiers.isEmpty else { return nil }
        switch event.keyCode {
        case 125: return .lineDown
        case 126: return .lineUp
        case 121: return .pageDown
        case 116: return .pageUp
        case 115: return .top
        case 119: return .bottom
        default: return nil
        }
    }

    /// The new top edge after one page: a visible page minus one line of
    /// overlap, clamped to the document.
    static func targetOffset(
        current: CGFloat,
        visibleHeight: CGFloat,
        documentHeight: CGFloat,
        lineHeight: CGFloat,
        direction: Direction
    ) -> CGFloat {
        targetOffset(
            current: current,
            visibleHeight: visibleHeight,
            documentHeight: documentHeight,
            lineHeight: lineHeight,
            command: direction == .down ? .pageDown : .pageUp
        )
    }

    static func targetOffset(
        current: CGFloat,
        visibleHeight: CGFloat,
        documentHeight: CGFloat,
        lineHeight: CGFloat,
        command: Command
    ) -> CGFloat {
        let page = max(lineHeight, visibleHeight - lineHeight)
        let maxOffset = max(0, documentHeight - visibleHeight)
        let proposed: CGFloat
        switch command {
        case .pageDown: proposed = current + page
        case .pageUp: proposed = current - page
        case .lineDown: proposed = current + lineHeight
        case .lineUp: proposed = current - lineHeight
        case .top: proposed = 0
        case .bottom: proposed = maxOffset
        }
        return min(max(0, proposed), maxOffset)
    }

    /// Pages `scrollView` and returns true when the key was handled. A Space
    /// at the end of the document is still consumed, so it never beeps or
    /// falls through to typing.
    @discardableResult
    static func page(_ scrollView: NSScrollView, direction: Direction, lineHeight: CGFloat) -> Bool {
        scroll(scrollView, command: direction == .down ? .pageDown : .pageUp, lineHeight: lineHeight)
    }

    /// Scrolls `scrollView` for `command`. Line steps move at once; page and
    /// end jumps animate briefly unless Reduce Motion is on.
    @discardableResult
    static func scroll(_ scrollView: NSScrollView, command: Command, lineHeight: CGFloat) -> Bool {
        guard let documentView = scrollView.documentView else { return false }
        let clipView = scrollView.contentView
        let bounds = clipView.bounds
        let documentHeight = documentView.frame.height
        let current = documentView.isFlipped ? bounds.minY : documentHeight - bounds.maxY
        let target = targetOffset(
            current: current,
            visibleHeight: bounds.height,
            documentHeight: documentHeight,
            lineHeight: lineHeight,
            command: command
        )
        guard abs(target - current) > 0.5 else { return true }
        let originY = documentView.isFlipped ? target : documentHeight - target - bounds.height
        let origin = NSPoint(x: bounds.minX, y: originY)
        let isLineStep = command == .lineDown || command == .lineUp
        if isLineStep || NSWorkspace.shared.accessibilityDisplayShouldReduceMotion {
            clipView.setBoundsOrigin(origin)
            scrollView.reflectScrolledClipView(clipView)
        } else {
            NSAnimationContext.runAnimationGroup { context in
                context.duration = 0.15
                context.allowsImplicitAnimation = true
                clipView.animator().setBoundsOrigin(origin)
            } completionHandler: {
                MainActor.assumeIsolated { scrollView.reflectScrolledClipView(clipView) }
            }
        }
        return true
    }

    /// One line of the document in `scrollView`.
    static func lineHeight(for scrollView: NSScrollView) -> CGFloat {
        if let textView = scrollView.documentView as? NSTextView,
           let font = textView.font,
           let layoutManager = textView.layoutManager {
            return layoutManager.defaultLineHeight(for: font)
        }
        return 18
    }
}

/// Text surfaces that take typed input although they are not editable text
/// views (the exec terminal transcript forwards keys to its prompt). Keyboard
/// scrolling never routes through them.
@MainActor
protocol RuneKeyboardTypingSurface: AnyObject {}

/// Marks the inspector pane, so the window's key monitor can find the
/// inspector document to scroll when the pane has keyboard focus.
struct RuneInspectorDocumentAnchor: NSViewRepresentable {
    func makeNSView(context: Context) -> AnchorView {
        AnchorView()
    }

    func updateNSView(_ nsView: AnchorView, context: Context) {}

    final class AnchorView: NSView {
        private static var anchors: [ObjectIdentifier: WeakAnchor] = [:]

        private struct WeakAnchor {
            weak var view: AnchorView?
        }

        override func hitTest(_ point: NSPoint) -> NSView? { nil }

        override func viewDidMoveToWindow() {
            super.viewDidMoveToWindow()
            Self.anchors = Self.anchors.filter { $0.value.view != nil }
            if let window {
                Self.anchors[ObjectIdentifier(window)] = WeakAnchor(view: self)
            }
        }

        static func anchor(in window: NSWindow) -> AnchorView? {
            anchors[ObjectIdentifier(window)]?.view.flatMap { $0.window === window ? $0 : nil }
        }

        /// The inspector pane's frame in window coordinates.
        var paneRectInWindow: NSRect {
            convert(bounds, to: nil)
        }
    }
}

@MainActor
enum RuneInspectorKeyboardScrolling {
    /// Scrolls the inspector document for `event` when the inspector pane has
    /// keyboard focus and the key isn't meant for something else. Returns
    /// true when the event was handled.
    static func handle(_ event: NSEvent, in window: NSWindow) -> Bool {
        guard let command = RuneSpacePaging.command(for: event),
              let anchor = RuneInspectorDocumentAnchor.AnchorView.anchor(in: window) else { return false }
        let paneRect = anchor.paneRectInWindow
        guard allowsScrolling(firstResponder: window.firstResponder, paneRect: paneRect, event: event) else { return false }
        guard let scrollView = documentScrollView(in: window, paneRect: paneRect) else { return false }
        return RuneSpacePaging.scroll(scrollView, command: command, lineHeight: RuneSpacePaging.lineHeight(for: scrollView))
    }

    /// Keys stay with the first responder when it is editable, a control
    /// (a focused button presses on Space), a terminal, or outside the
    /// inspector.
    static func allowsScrolling(firstResponder: NSResponder?, paneRect: NSRect, event: NSEvent) -> Bool {
        guard let responder = firstResponder else { return true }
        if responder is NSWindow { return true }
        if responder is RuneKeyboardTypingSurface { return false }
        if let textView = responder as? NSTextView {
            guard !textView.isEditable, !textView.isFieldEditor else { return false }
        } else if responder is NSControl {
            return false
        }
        guard let view = responder as? NSView else { return true }
        let frame = view.convert(view.bounds, to: nil)
        // A container of the whole inspector (SwiftUI's hosting view takes
        // first responder after a click) is neutral; a view inside the
        // inspector is fine; anything beside it (the table) keeps its keys.
        if frame.contains(NSPoint(x: paneRect.midX, y: paneRect.midY)) { return true }
        return paneRect.insetBy(dx: -1, dy: -1).contains(NSPoint(x: frame.midX, y: frame.midY))
    }

    /// The largest scrollable vertical scroll view inside the inspector pane:
    /// the YAML, Describe or Logs document, or the inspector's own scroll
    /// view on tabs without a document.
    static func documentScrollView(in window: NSWindow, paneRect: NSRect) -> NSScrollView? {
        guard let root = window.contentView else { return nil }
        var best: (view: NSScrollView, area: CGFloat)?
        func visit(_ view: NSView) {
            if view.isHidden { return }
            if let scrollView = view as? NSScrollView, let documentView = scrollView.documentView {
                let frame = scrollView.convert(scrollView.bounds, to: nil)
                let isInside = paneRect.insetBy(dx: -1, dy: -1).contains(frame)
                let scrollsVertically = documentView.frame.height > scrollView.contentView.bounds.height + 1
                if isInside, scrollsVertically, frame.width > 1, frame.height > 1 {
                    let area = frame.width * frame.height
                    if area > (best?.area ?? 0) { best = (scrollView, area) }
                }
            }
            view.subviews.forEach(visit)
        }
        visit(root)
        return best?.view
    }
}
