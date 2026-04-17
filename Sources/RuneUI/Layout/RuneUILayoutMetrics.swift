import CoreGraphics
import RuneCore

enum RuneUILayoutMetrics {
    // Shared spacing primitives for root panes so all sections stay aligned.
    static let windowContentTopInset: CGFloat = 8
    static let paneOuterPadding: CGFloat = 16
    static let paneInnerPadding: CGFloat = 12
    static let sidebarPadding: CGFloat = 14
    static let headerChipHeight: CGFloat = 28
    static let headerChipHorizontalPadding: CGFloat = 10

    static let minWindowContentTopInset: CGFloat = 0
    static let maxWindowContentTopInset: CGFloat = 28

    static func resolvedWindowContentTopInset(measuredInset: CGFloat?) -> CGFloat {
        guard let measuredInset else {
            return windowContentTopInset
        }

        let clamped = min(max(measuredInset, minWindowContentTopInset), maxWindowContentTopInset)
        return max(windowContentTopInset, clamped)
    }
}
