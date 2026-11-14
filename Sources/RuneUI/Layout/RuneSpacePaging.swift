import AppKit

/// Space and Shift+Space page through read-only inspector documents (Logs,
/// YAML, Describe), the way a browser or Preview does. It runs in the focused
/// view's `keyDown`, so it only applies when that view is first responder
/// and costs nothing per frame. Editable text, fields and terminals never
/// route here, so Space keeps typing there.
@MainActor
enum RuneSpacePaging {
    enum Direction: Equatable {
        case down
        case up
    }

    /// The paging direction for an unmodified Space (down) or Shift+Space
    /// (up), or nil for any other key or modifier.
    static func direction(for event: NSEvent) -> Direction? {
        guard event.type == .keyDown, event.charactersIgnoringModifiers == " " else { return nil }
        let modifiers = event.modifierFlags
            .intersection(.deviceIndependentFlagsMask)
            .subtracting([.capsLock, .numericPad, .function])
        if modifiers.isEmpty { return .down }
        if modifiers == .shift { return .up }
        return nil
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
        let step = max(lineHeight, visibleHeight - lineHeight)
        let maxOffset = max(0, documentHeight - visibleHeight)
        let proposed = direction == .down ? current + step : current - step
        return min(max(0, proposed), maxOffset)
    }

    /// Pages `scrollView` and returns true when the key was handled. A Space
    /// at the end of the document is still consumed, so it never beeps or
    /// falls through to typing.
    @discardableResult
    static func page(_ scrollView: NSScrollView, direction: Direction, lineHeight: CGFloat) -> Bool {
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
            direction: direction
        )
        guard abs(target - current) > 0.5 else { return true }
        let originY = documentView.isFlipped ? target : documentHeight - target - bounds.height
        let origin = NSPoint(x: bounds.minX, y: originY)
        if NSWorkspace.shared.accessibilityDisplayShouldReduceMotion {
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
}
