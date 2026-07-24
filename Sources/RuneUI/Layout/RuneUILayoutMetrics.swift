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
    /// Compact terminal/log tab chrome; deliberately distinct from general interactive rows.
    static let tabCornerRadius: CGFloat = 7

    /// Root content lives in the window content area below the unified toolbar. Keep this at zero:
    /// adding a synthetic inset makes the workspace drift down and hides the bottom of sidebars,
    /// terminals, and inspector cards on smaller windows.
    static let windowContentTopInset: CGFloat = 0
    static let paneOuterPadding: CGFloat = 16
    static let paneInnerPadding: CGFloat = 12
    static let sidebarPadding: CGFloat = 14
    /// Middle-pane cadence from `.docs/rune-design-plan.md` §6.6.
    static let contentControlSpacing: CGFloat = 8
    static let contentModuleSpacing: CGFloat = 12
    static let contentSectionSpacing: CGFloat = 16
    /// Shared chrome immediately above resource tables.
    static let resourceListToolbarMinimumHeight: CGFloat = 30
    static let resourceListToolbarAccessibilityMinimumHeight: CGFloat = 44
    static let resourceFilterFieldMinimumWidth: CGFloat = 160
    static let resourceFilterFieldIdealWidth: CGFloat = 240
    static let resourceFilterFieldMaximumWidth: CGFloat = 280
    static let resourceFilterControlsMaximumWidth: CGFloat = 312
    static let resourceListActionsRailMinimumWidth: CGFloat = 320
    static let resourceListCompactRowSpacing: CGFloat = 6
    /// Minimum width for the resource list column (`NavigationSplitView` content).
    static let splitContentColumnMinWidth: CGFloat = 360
    /// Matches first-ship column cap (`49c6517` First draft).
    static let splitContentColumnMaxWidth: CGFloat = 1200
    /// Minimum / ideal / max inspector column (`49c6517` First draft).
    static let splitDetailColumnMinWidth: CGFloat = 320
    static let splitDetailColumnIdealWidth: CGFloat = 520
    static let splitDetailColumnMaxWidth: CGFloat = 1280
    static let splitDetailColumnExpandedMaxWidth: CGFloat = 1800
    /// Sidebar min / max width (`49c6517`).
    static let splitSidebarMinWidth: CGFloat = 220
    static let splitSidebarMaxWidth: CGFloat = 460
    static let headerChipHeight: CGFloat = 28
    static let headerChipHorizontalPadding: CGFloat = 10
    static let headerCapsuleMinimumHeight: CGFloat = 28
    static let headerCapsuleHorizontalPadding: CGFloat = 10
    static let headerCapsuleVerticalPadding: CGFloat = 4
    static let headerCapsuleAccessibilityVerticalPadding: CGFloat = 8
    static let contentHeaderMinimumScrollableWidth: CGFloat = 640
    static let inspectorToolbarGroupSpacing: CGFloat = 8
    static let inspectorToolbarControlSpacing: CGFloat = 8
    static let inspectorToolbarGroupHorizontalPadding: CGFloat = 10
    static let inspectorToolbarGroupVerticalPadding: CGFloat = 8
    static let inspectorToolbarSourceGroupHeight: CGFloat = 58
    static let inspectorToolbarActionGroupHeight: CGFloat = 44
    static let inspectorToolbarControlMinHeight: CGFloat = 30
    static let yamlSheetValidationListMaxHeight: CGFloat = 150
    static let iconButtonSize: CGFloat = 28

    // Modal sheets and custom dialogs. Native alerts, confirmation dialogs and
    // open/save panels keep the system-provided macOS metrics.
    static let dialogContentPadding: CGFloat = 20
    static let dialogSectionSpacing: CGFloat = 14
    static let dialogControlSpacing: CGFloat = 8
    static let dialogButtonMinHeight: CGFloat = 30
    static let dialogButtonLabelMinHeight: CGFloat = 22
    static let dialogFooterButtonLabelMinWidth: CGFloat = 72
    static let dialogIconButtonSize: CGFloat = iconButtonSize
    static let compactDialogWidth: CGFloat = 420
    static let standardDialogWidth: CGFloat = 560
    static let wideDialogWidth: CGFloat = 760
    static let wideDialogHeight: CGFloat = 560
    static let commandPaletteMinWidth: CGFloat = 560
    static let commandPaletteIdealWidth: CGFloat = 720
    static let commandPaletteMaxWidth: CGFloat = 880
    static let commandPaletteMinHeight: CGFloat = 420
    static let commandPaletteIdealHeight: CGFloat = 520
    static let commandPaletteMaxHeight: CGFloat = 600
    static let providerDialogBodyMinHeight: CGFloat = 220
    static let providerDialogBodyIdealHeight: CGFloat = 300
    static let providerDialogBodyMaxHeight: CGFloat = 430
    static let providerDialogMaxHeight: CGFloat = 600
    static let addClusterPopoverWidth: CGFloat = 400
    static let addClusterPopoverPadding: CGFloat = 14
    static let addClusterActionCardMinHeight: CGFloat = 62
    static let addClusterPopoverMaxHeight: CGFloat = 560

    static let minWindowContentTopInset: CGFloat = 0
    static let maxWindowContentTopInset: CGFloat = 0

    static func resolvedWindowContentTopInset(measuredInset: CGFloat?) -> CGFloat {
        windowContentTopInset
    }
}

public enum RuneWindowLayoutDefaults {
    public static let minimumWidth: CGFloat = 980
    public static let minimumHeight: CGFloat = 640
}
