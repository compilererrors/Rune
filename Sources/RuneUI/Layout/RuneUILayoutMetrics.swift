import CoreGraphics
import RuneCore

enum RuneUILayoutMetrics {
    // Shared spacing primitives for root panes so all sections stay aligned.
    //
    // Corner radii — one continuous shape family (`.docs/rune-design-plan.md` §6): shells vs grouped inset vs rows/chips.
    /// Outer rounded rect for the three main columns (sidebar, content, inspector shell).
    static let paneShellCornerRadius: CGFloat = 12
    /// Inset cards inside a pane (inspector body, empty states, table row chrome).
    static let groupedContentCornerRadius: CGFloat = 10
    /// Tight interactive rows (sidebar section/context picks), log/YAML editor chrome.
    static let interactiveRowCornerRadius: CGFloat = 8
    /// Small metadata chips and inline badges.
    static let compactGlyphCornerRadius: CGFloat = 6
    /// Vertical inset for the split grabber overlay so it doesn’t sit flush under the window chrome.
    static var splitDividerVerticalInset: CGFloat { paneShellCornerRadius }

    static let windowContentTopInset: CGFloat = 8
    static let paneOuterPadding: CGFloat = 16
    static let paneInnerPadding: CGFloat = 12
    static let sidebarPadding: CGFloat = 14
    /// Horizontal padding between split columns. Use 0 so the system divider sits flush against pane shells (no dark gap).
    static let splitColumnGutter: CGFloat = 0
    /// Minimum width for the resource list column (`HSplitView`); protects tables when the inspector is narrow.
    static let splitContentColumnMinWidth: CGFloat = 560
    /// Minimum inspector width. Slightly wider default so YAML/describe toolbars and monospaced text stay usable at the split minimum (window min ~1280pt).
    static let splitDetailColumnMinWidth: CGFloat = 460
    /// Extra space on the inspector’s leading edge so content doesn’t hug the split divider when the column is at its minimum.
    static let inspectorLeadingInset: CGFloat = 4
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
