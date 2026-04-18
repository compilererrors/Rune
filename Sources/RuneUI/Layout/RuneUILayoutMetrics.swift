import CoreGraphics
import RuneCore

enum RuneUILayoutMetrics {
    // Shared spacing primitives for root panes so all sections stay aligned.
    //
    // Corner radii — one continuous shape family (`.docs/rune-design-plan.md` §6): shells vs grouped inset vs rows/chips.
    /// Outer rounded rect for inset cards and grouped panels (not full split columns — those use `NavigationSplitView` system chrome).
    static let paneShellCornerRadius: CGFloat = 12
    /// Inset cards inside a pane (inspector body, empty states, table row chrome).
    static let groupedContentCornerRadius: CGFloat = 10
    /// Tight interactive rows (sidebar section/context picks), log/YAML editor chrome.
    static let interactiveRowCornerRadius: CGFloat = 8
    /// Small metadata chips and inline badges.
    static let compactGlyphCornerRadius: CGFloat = 6

    static let windowContentTopInset: CGFloat = 8
    static let paneOuterPadding: CGFloat = 16
    static let paneInnerPadding: CGFloat = 12
    static let sidebarPadding: CGFloat = 14
    /// Minimum width for the resource list column (`NavigationSplitView` content).
    static let splitContentColumnMinWidth: CGFloat = 560
    /// Matches first-ship column cap (`49c6517` First draft).
    static let splitContentColumnMaxWidth: CGFloat = 1200
    /// Minimum / ideal / max inspector column (`49c6517` First draft).
    static let splitDetailColumnMinWidth: CGFloat = 340
    static let splitDetailColumnIdealWidth: CGFloat = 440
    static let splitDetailColumnMaxWidth: CGFloat = 820
    /// Sidebar max width (`49c6517`).
    static let splitSidebarMaxWidth: CGFloat = 460
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
