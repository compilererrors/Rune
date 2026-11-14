import CoreGraphics
import RuneCore

enum RuneUILayoutMetrics {
    // Named feature metrics, expressed through the grid in RuneDesignTokens.swift.
    /// Outer rounded rect for inset cards and grouped panels (not full split columns — those use `NavigationSplitView` system chrome).
    static let paneShellCornerRadius: CGFloat = RuneRadius.panel
    /// Inset cards inside a pane (inspector body, empty states, table row chrome).
    static let groupedContentCornerRadius: CGFloat = RuneRadius.group
    /// Tight interactive rows (sidebar section/context picks), log/YAML editor chrome.
    static let interactiveRowCornerRadius: CGFloat = RuneRadius.control
    static let controlCardCornerRadius: CGFloat = RuneRadius.group
    /// The find bar holds 24 pt controls (6 pt corners) 8 pt from its edge.
    static let findBarCornerRadius: CGFloat = RuneRadius.concentric(inner: RuneRadius.chip, inset: RuneSpacing.xs)
    /// Terminal session tabs (7 pt corners) sit 5 pt inside their 38 pt strip.
    static let terminalTabStripCornerRadius: CGFloat = RuneRadius.concentric(inner: RuneRadius.tab, inset: 5)
    /// A framed document with its attached header bar: the bar's controls sit
    /// `documentFrameBarInset` in from the frame, so the frame's corners are
    /// concentric with the control corners.
    static let documentFrameBarInset: CGFloat = RuneSpacing.tight
    /// The expanded YAML editor opens large and resizes freely.
    static let yamlEditorIdealWidth: CGFloat = 1_120
    static let yamlEditorIdealHeight: CGFloat = 820
    static let documentFrameBarVerticalInset: CGFloat = RuneSpacing.tight
    static let documentFrameStatusVerticalInset: CGFloat = RuneSpacing.xxs
    static let documentFrameCornerRadius: CGFloat = RuneRadius.concentric(inner: RuneRadius.control, inset: documentFrameBarInset)
    /// Small metadata chips and inline badges.
    static let compactGlyphCornerRadius: CGFloat = RuneRadius.chip
    /// Compact terminal/log tab chrome; deliberately distinct from general interactive rows.
    static let tabCornerRadius: CGFloat = RuneRadius.tab

    /// Root content lives in the window content area below the unified toolbar. Keep this at zero:
    /// adding a synthetic inset makes the workspace drift down and hides the bottom of sidebars,
    /// terminals, and inspector cards on smaller windows.
    static let windowContentTopInset: CGFloat = 0
    static let paneOuterPadding: CGFloat = RuneSpacing.md
    static let paneInnerPadding: CGFloat = RuneSpacing.sm
    static let sidebarPadding: CGFloat = RuneSpacing.sm
    /// Middle-pane cadence from `.docs/rune-design-plan.md` §6.6.
    static let contentControlSpacing: CGFloat = RuneSpacing.xs
    static let contentModuleSpacing: CGFloat = RuneSpacing.sm
    static let contentSectionSpacing: CGFloat = RuneSpacing.md
    /// Shared inspector grid. Full sections use the regular/compact cadence,
    /// while search, insight, and find rows share one leading accessory column.
    static let inspectorSectionSpacing: CGFloat = RuneSpacing.sm
    /// Between the sections of an inspector overview (facts, relationships):
    /// wider than the space inside a section, so space alone groups them.
    static let inspectorGroupSpacing: CGFloat = RuneSpacing.lg
    static let inspectorCompactSectionSpacing: CGFloat = RuneSpacing.xs
    static let inspectorControlRowSpacing: CGFloat = RuneSpacing.xs
    static let inspectorWrappedActionRowSpacing: CGFloat = RuneSpacing.xxs
    static let inspectorControlRowHeight: CGFloat = RuneControlHeight.toolbar
    static let inspectorControlContentInset: CGFloat = RuneSpacing.inset
    static let inspectorControlChromeVerticalPadding: CGFloat = RuneSpacing.xs
    static let inspectorControlSurfaceVerticalPadding: CGFloat = RuneSpacing.tight
    static let inspectorControlLeadingAccessoryWidth: CGFloat = RuneControlHeight.regular
    static let inspectorControlColumnSpacing: CGFloat = RuneSpacing.hairline
    static let inspectorOverlayInset: CGFloat = RuneSpacing.inset
    /// Shared chrome immediately above resource tables.
    static let resourceListToolbarMinimumHeight: CGFloat = RuneControlHeight.toolbar
    static let resourceListToolbarAccessibilityMinimumHeight: CGFloat = RuneControlHeight.accessibility
    static let resourceFilterFieldMinimumWidth: CGFloat = 160
    static let resourceFilterFieldIdealWidth: CGFloat = 240
    static let resourceFilterFieldMaximumWidth: CGFloat = 280
    static let resourceFilterControlsMaximumWidth: CGFloat = 312
    /// The filter gives up width (down to this) before toolbar actions lose
    /// their labels.
    static let resourceFilterControlsMinimumWidth: CGFloat = 200
    static let resourceListActionsRailMinimumWidth: CGFloat = 320
    static let resourceListCompactRowSpacing: CGFloat = RuneSpacing.tight
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
    static let headerChipHeight: CGFloat = RuneControlHeight.regular
    static let headerChipHorizontalPadding: CGFloat = RuneSpacing.inset
    /// Header capsules are badges (`RuneBadgeSurface`): small control size.
    static let headerCapsuleMinimumHeight: CGFloat = RuneControlSize.small.baseHeight
    static let headerCapsuleHorizontalPadding: CGFloat = RuneSpacing.xs
    static let headerCapsuleVerticalPadding: CGFloat = 0
    static let headerCapsuleAccessibilityVerticalPadding: CGFloat = RuneSpacing.xxs
    static let contentHeaderMinimumScrollableWidth: CGFloat = 640
    static let inspectorToolbarGroupSpacing: CGFloat = RuneSpacing.xs
    static let inspectorToolbarControlSpacing: CGFloat = RuneSpacing.xs
    static let inspectorToolbarGroupHorizontalPadding: CGFloat = inspectorControlContentInset
    static let inspectorToolbarGroupVerticalPadding: CGFloat = RuneSpacing.xs
    static let inspectorToolbarSourceGroupHeight: CGFloat = 58
    static let inspectorToolbarActionGroupHeight: CGFloat = 44
    static let inspectorToolbarControlMinHeight: CGFloat = RuneControlHeight.toolbar
    /// Content inset shared by inspector document surfaces (YAML, Describe, logs, and terminal output).
    static let inspectorDocumentHorizontalInset: CGFloat = RuneSpacing.inset
    static let inspectorDocumentVerticalInset: CGFloat = RuneSpacing.inset
    static let yamlSheetValidationListMaxHeight: CGFloat = 150
    static let iconButtonSize: CGFloat = RuneControlHeight.regular
    /// Fact grids: one label column wide enough for "App version" or
    /// "Cluster IP", and rows tall enough to read without boxes.
    static let factLabelWidth: CGFloat = 96
    static let factRowVerticalPadding: CGFloat = RuneSpacing.tight

    // Modal sheets and custom dialogs. Native alerts, confirmation dialogs and
    // open/save panels keep the system-provided macOS metrics.
    static let dialogContentPadding: CGFloat = RuneSpacing.lg
    static let dialogSectionSpacing: CGFloat = 14
    static let dialogControlSpacing: CGFloat = RuneSpacing.xs
    static let dialogButtonMinHeight: CGFloat = RuneControlHeight.toolbar
    static let dialogButtonLabelMinHeight: CGFloat = RuneControlHeight.compact
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
    static let providerDialogBodyCompactHeight: CGFloat = 172
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
