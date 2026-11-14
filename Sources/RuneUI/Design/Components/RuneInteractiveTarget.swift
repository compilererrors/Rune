import SwiftUI

private struct RuneMinimumInteractiveTargetModifier: ViewModifier {
    let minimumWidth: CGFloat?
    let alignment: Alignment

    func body(content: Content) -> some View {
        content
            .frame(
                minWidth: minimumWidth,
                minHeight: RuneUILayoutMetrics.iconButtonSize,
                alignment: alignment
            )
            .contentShape(Rectangle())
    }
}

extension View {
    /// Keeps compact labels dense while giving their owning control a reliable macOS target.
    func runeMinimumInteractiveTarget(
        minWidth: CGFloat? = nil,
        alignment: Alignment = .center
    ) -> some View {
        modifier(RuneMinimumInteractiveTargetModifier(
            minimumWidth: minWidth,
            alignment: alignment
        ))
    }
}
