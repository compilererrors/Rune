import AppKit
import SwiftUI

/// A grabber under a resizable surface (the terminal): a short capsule that
/// says "drag me", the up-down resize cursor on hover, and double-click to
/// go back to the automatic size. Reports the vertical drag distance; the
/// owner clamps and stores the height.
struct RuneResizeGrip: View {
    var help: String = "Drag to resize. Double-click to reset."
    let onDragChanged: (_ translation: CGFloat) -> Void
    let onDragEnded: () -> Void
    let onReset: () -> Void
    @State private var isHovered = false
    @State private var isDragging = false

    var body: some View {
        Capsule()
            .fill(isHovered || isDragging ? AnyShapeStyle(.runeAccent) : AnyShapeStyle(.runeTertiary))
            .frame(width: RuneResizeGripMetrics.capsuleWidth, height: RuneResizeGripMetrics.capsuleHeight)
            .frame(maxWidth: .infinity, minHeight: RuneResizeGripMetrics.hitHeight)
            .contentShape(Rectangle())
            .onHover { hovering in
                guard hovering != isHovered else { return }
                isHovered = hovering
                if hovering {
                    NSCursor.resizeUpDown.push()
                } else if !isDragging {
                    NSCursor.pop()
                }
            }
            .gesture(
                DragGesture(minimumDistance: 1, coordinateSpace: .global)
                    .onChanged { value in
                        isDragging = true
                        onDragChanged(value.translation.height)
                    }
                    .onEnded { _ in
                        isDragging = false
                        if !isHovered { NSCursor.pop() }
                        onDragEnded()
                    }
            )
            .simultaneousGesture(TapGesture(count: 2).onEnded(onReset))
            .help(help)
            .accessibilityElement()
            .accessibilityLabel("Resize")
            .accessibilityHint(help)
    }
}

enum RuneResizeGripMetrics {
    static let capsuleWidth: CGFloat = 36
    static let capsuleHeight: CGFloat = 4
    static let hitHeight: CGFloat = 12
}
