import AppKit
import RuneCore
import SwiftUI

enum RuneSurfaceKind {
    case panel
    case inset
    case editor
    case listRow(isSelected: Bool)
    case sidebarSelection(isSelected: Bool)

    var cornerRadius: CGFloat {
        switch self {
        case .panel, .inset:
            return RuneUILayoutMetrics.groupedContentCornerRadius
        case .listRow:
            return RuneUILayoutMetrics.compactGlyphCornerRadius
        case .editor, .sidebarSelection:
            return RuneUILayoutMetrics.interactiveRowCornerRadius
        }
    }

    func fill(theme: RuneResolvedTheme) -> Color {
        if let palette = theme.palette {
            switch self {
            case .panel:
                return palette.panel
            case .inset:
                return palette.inset
            case .editor:
                return palette.editor
            case let .listRow(isSelected):
                return isSelected ? palette.rowSelected.opacity(0.72) : palette.row.opacity(0.62)
            case let .sidebarSelection(isSelected):
                return isSelected ? palette.selectionFill : Color.clear
            }
        }

        switch self {
        case .panel:
            return Color(nsColor: .controlBackgroundColor).opacity(0.72)
        case .inset:
            return Color(nsColor: .textBackgroundColor).opacity(0.58)
        case .editor:
            return Color(nsColor: .textBackgroundColor).opacity(0.92)
        case let .listRow(isSelected):
            return isSelected
                ? Color.accentColor.opacity(0.11)
                : Color(nsColor: .controlBackgroundColor).opacity(0.42)
        case let .sidebarSelection(isSelected):
            return isSelected ? Color.accentColor.opacity(0.16) : Color.clear
        }
    }

    func stroke(theme: RuneResolvedTheme) -> Color? {
        if let palette = theme.palette {
            switch self {
            case .panel:
                return palette.stroke.opacity(0.36)
            case .inset:
                return palette.stroke.opacity(0.50)
            case .editor:
                return palette.stroke.opacity(0.36)
            case let .listRow(isSelected):
                return isSelected ? nil : palette.stroke.opacity(0.30)
            case .sidebarSelection:
                return nil
            }
        }

        switch self {
        case .panel:
            return Color(nsColor: .separatorColor).opacity(0.24)
        case .inset:
            return Color(nsColor: .separatorColor).opacity(0.45)
        case .editor:
            return Color(nsColor: .separatorColor).opacity(0.24)
        case let .listRow(isSelected):
            if isSelected { return nil }
            return Color(nsColor: .separatorColor).opacity(0.2)
        case .sidebarSelection:
            return nil
        }
    }
}

struct RuneSurfaceBackground: View {
    let kind: RuneSurfaceKind
    @Environment(\.runeResolvedTheme) private var resolvedTheme

    var body: some View {
        RoundedRectangle(cornerRadius: kind.cornerRadius, style: .continuous)
            .fill(kind.fill(theme: resolvedTheme))
            .overlay {
                if let stroke = kind.stroke(theme: resolvedTheme) {
                    RoundedRectangle(cornerRadius: kind.cornerRadius, style: .continuous)
                        .strokeBorder(stroke, lineWidth: 1)
                }
            }
    }
}

/// One shared leading column for compact inspector controls. Search fields,
/// summaries, and similar rows keep their main content aligned even when the
/// leading symbols have different intrinsic sizes.
struct RuneInspectorControlGridRow<LeadingAccessory: View, Content: View>: View {
    @ViewBuilder let leadingAccessory: LeadingAccessory
    @ViewBuilder let content: Content

    init(
        @ViewBuilder leadingAccessory: () -> LeadingAccessory,
        @ViewBuilder content: () -> Content
    ) {
        self.leadingAccessory = leadingAccessory()
        self.content = content()
    }

    var body: some View {
        HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorControlColumnSpacing) {
            leadingAccessory
                .frame(
                    width: RuneUILayoutMetrics.inspectorControlLeadingAccessoryWidth,
                    height: RuneUILayoutMetrics.inspectorControlLeadingAccessoryWidth,
                    alignment: .center
                )

            content
        }
        .frame(
            minHeight: RuneUILayoutMetrics.inspectorControlRowHeight,
            alignment: .leading
        )
    }
}

struct RuneChip<Content: View>: View {
    let horizontalPadding: CGFloat
    let verticalPadding: CGFloat
    let fill: Color?
    let cornerRadius: CGFloat
    @ViewBuilder var content: Content
    @Environment(\.runeThemePalette) private var runeThemePalette

    init(
        horizontalPadding: CGFloat = RuneSpacing.xs,
        verticalPadding: CGFloat = RuneSpacing.xxs,
        fill: Color? = nil,
        cornerRadius: CGFloat = RuneUILayoutMetrics.compactGlyphCornerRadius,
        @ViewBuilder content: () -> Content
    ) {
        self.horizontalPadding = horizontalPadding
        self.verticalPadding = verticalPadding
        self.fill = fill
        self.cornerRadius = cornerRadius
        self.content = content()
    }

    var body: some View {
        content
            .padding(.horizontal, horizontalPadding)
            .padding(.vertical, verticalPadding)
            .background(
                RoundedRectangle(cornerRadius: cornerRadius, style: .continuous)
                    .fill(fill ?? runeThemePalette?.chipFill ?? Color.secondary.opacity(0.12))
            )
    }
}

/// Compact icon affordance with a stable macOS hit target and semantic states.
struct RuneIconButton: View {
    let systemImage: String
    let accessibilityLabel: String
    let helpText: String
    let isSelected: Bool?
    let isDisabled: Bool
    let selectedTint: Color?
    let action: () -> Void
    @Environment(\.runeThemePalette) private var runeThemePalette

    init(
        _ accessibilityLabel: String,
        systemImage: String,
        help: String? = nil,
        isSelected: Bool? = nil,
        isDisabled: Bool = false,
        selectedTint: Color? = nil,
        action: @escaping () -> Void
    ) {
        self.systemImage = systemImage
        self.accessibilityLabel = accessibilityLabel
        helpText = help ?? accessibilityLabel
        self.isSelected = isSelected
        self.isDisabled = isDisabled
        self.selectedTint = selectedTint
        self.action = action
    }

    var body: some View {
        Button(action: action) {
            Image(systemName: systemImage)
                .frame(
                    width: RuneUILayoutMetrics.iconButtonSize,
                    height: RuneUILayoutMetrics.iconButtonSize
                )
                .contentShape(Rectangle())
        }
        .buttonStyle(RuneCompactButtonStyle(isSelected: isSelected == true, tint: selectedTint))
        .disabled(isDisabled)
        .help(helpText)
        .accessibilityLabel(accessibilityLabel)
        .modifier(RuneIconButtonSelectionAccessibilityModifier(isSelected: isSelected))
    }
}

/// Bordered toolbar action for a compact destination or command that needs
/// stronger visual presence than the plain `RuneIconButton`.
struct RuneBorderedIconButton: View {
    let systemImage: String
    let accessibilityLabel: String
    let helpText: String
    let isDisabled: Bool
    let action: () -> Void

    init(
        _ accessibilityLabel: String,
        systemImage: String,
        help: String? = nil,
        isDisabled: Bool = false,
        action: @escaping () -> Void
    ) {
        self.systemImage = systemImage
        self.accessibilityLabel = accessibilityLabel
        helpText = help ?? accessibilityLabel
        self.isDisabled = isDisabled
        self.action = action
    }

    var body: some View {
        Button(action: action) {
            Image(systemName: systemImage)
                .runeInterfaceFont(relativeSize: -1, weight: .medium)
                .contentShape(Rectangle())
        }
        .buttonStyle(RuneToolbarButtonStyle(isIconOnly: true))
        .contentShape(Rectangle())
        .disabled(isDisabled)
        .help(helpText)
        .accessibilityLabel(accessibilityLabel)
    }
}

private struct RuneIconButtonSelectionAccessibilityModifier: ViewModifier {
    let isSelected: Bool?

    @ViewBuilder
    func body(content: Content) -> some View {
        if let isSelected {
            content
                .accessibilityValue(isSelected ? "Selected" : "Not selected")
                .accessibilityAddTraits(isSelected ? .isSelected : [])
        } else {
            content
        }
    }
}

struct RuneBulkSelectionBar<Actions: View>: View {
    let selectedCount: Int
    let visibleCount: Int
    let allVisibleSelected: Bool
    let showsActions: Bool
    let onToggleVisibleSelection: () -> Void
    let onClearSelection: () -> Void
    @ViewBuilder var actions: Actions
    @Environment(\.runeThemePalette) private var runeThemePalette
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeInterfaceFontSize) private var interfaceFontSize

    init(
        selectedCount: Int,
        visibleCount: Int,
        allVisibleSelected: Bool,
        showsActions: Bool = true,
        onToggleVisibleSelection: @escaping () -> Void,
        onClearSelection: @escaping () -> Void,
        @ViewBuilder actions: () -> Actions
    ) {
        self.selectedCount = selectedCount
        self.visibleCount = visibleCount
        self.allVisibleSelected = allVisibleSelected
        self.showsActions = showsActions
        self.onToggleVisibleSelection = onToggleVisibleSelection
        self.onClearSelection = onClearSelection
        self.actions = actions()
    }

    /// The count, Select/Deselect Visible and clear are one joined control
    /// that is always present and always the same size: titles reserve the
    /// width of their alternatives and clear is disabled, not hidden, with
    /// nothing selected. Selecting rows therefore never moves the filter,
    /// Sort or the actions (macOS HIG: change state in place, and dim
    /// controls that don't apply instead of removing them).
    var body: some View {
        HStack(spacing: RuneSpacing.xs) {
            RuneToolbarButtonGroup(isSelected: selectedCount > 0) {
                RuneToolbarGroupLabel {
                    Label {
                        RuneStableLabel(selectedCountText, reserving: ["888 selected"])
                            .monospacedDigit()
                    } icon: {
                        Image(systemName: selectedCount > 0 ? "checkmark.circle.fill" : "circle")
                    }
                    .labelStyle(.titleAndIcon)
                }
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(selectedCountText)
                RuneToolbarGroupDivider()
                // An icon segment: the count already says what is selected, and
                // a fixed-width segment keeps the group compact and stable.
                Button(action: onToggleVisibleSelection) {
                    Image(systemName: "checklist")
                }
                .buttonStyle(RuneToolbarSegmentButtonStyle())
                .disabled(visibleCount == 0)
                .help(allVisibleSelected ? "Deselect all visible rows" : "Select all visible rows")
                .accessibilityLabel(toggleTitle)
                RuneToolbarGroupDivider()
                Button(action: onClearSelection) {
                    Image(systemName: "xmark")
                }
                .buttonStyle(RuneToolbarSegmentButtonStyle())
                .disabled(selectedCount == 0)
                .help("Clear all selected rows")
                .accessibilityLabel("Clear selection")
            }
            if showsActions {
                actions
            }
        }
        .accessibilityElement(children: .contain)
    }

    private var toggleTitle: String {
        allVisibleSelected ? "Deselect Visible" : "Select Visible"
    }

    private var selectedCountText: String {
        "\(selectedCount) selected"
    }
}

/// One stable control band shared by every resource list. The primary control
/// keeps enough width for filtering while secondary controls own their compact
/// overflow behavior, avoiding per-resource wrapping and table-position jumps.
private struct RuneResourceListToolbarLayout: Layout {
    let minimumHeight: CGFloat

    private var inlineMinimumWidth: CGFloat {
        RuneUILayoutMetrics.resourceFilterControlsMinimumWidth
            + RuneUILayoutMetrics.contentControlSpacing
            + RuneUILayoutMetrics.resourceListActionsRailMinimumWidth
    }

    func sizeThatFits(
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) -> CGSize {
        guard subviews.count == 2 else { return .zero }
        let width = proposal.width ?? inlineMinimumWidth
        let measurements = toolbarMeasurements(width: width, subviews: subviews)
        return CGSize(width: width, height: measurements.height)
    }

    func placeSubviews(
        in bounds: CGRect,
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) {
        guard subviews.count == 2 else { return }
        let measurements = toolbarMeasurements(width: bounds.width, subviews: subviews)

        if measurements.isInline {
            subviews[0].place(
                at: CGPoint(
                    x: bounds.minX,
                    y: bounds.minY + (measurements.height - measurements.primarySize.height) / 2
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(
                    width: measurements.primaryWidth,
                    height: measurements.primarySize.height
                )
            )
            subviews[1].place(
                at: CGPoint(
                    x: bounds.minX
                        + measurements.primaryWidth
                        + RuneUILayoutMetrics.contentControlSpacing,
                    y: bounds.minY + (measurements.height - measurements.actionsSize.height) / 2
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(
                    width: measurements.actionsWidth,
                    height: measurements.actionsSize.height
                )
            )
            return
        }

        subviews[0].place(
            at: bounds.origin,
            anchor: .topLeading,
            proposal: ProposedViewSize(
                width: bounds.width,
                height: measurements.primarySize.height
            )
        )
        subviews[1].place(
            at: CGPoint(
                x: bounds.minX,
                y: bounds.minY
                    + measurements.primarySize.height
                    + RuneUILayoutMetrics.resourceListCompactRowSpacing
            ),
            anchor: .topLeading,
            proposal: ProposedViewSize(
                width: bounds.width,
                height: measurements.actionsSize.height
            )
        )
    }

    private func toolbarMeasurements(width: CGFloat, subviews: Subviews) -> (
        isInline: Bool,
        primaryWidth: CGFloat,
        actionsWidth: CGFloat,
        primarySize: CGSize,
        actionsSize: CGSize,
        height: CGFloat
    ) {
        let isInline = width >= inlineMinimumWidth
        if isInline {
            // The filter fills the row up to the actions with the standard
            // control gap, and only shrinks toward its minimum before the
            // actions drop their labels. The actions keep one width in every
            // state, so the filter never resizes when rows are selected.
            let actionsIdealWidth = subviews[1].sizeThatFits(.unspecified).width
            let spacing = RuneUILayoutMetrics.contentControlSpacing
            let primaryWidth = max(
                RuneUILayoutMetrics.resourceFilterControlsMinimumWidth,
                width - spacing - actionsIdealWidth
            )
            let actionsWidth = max(
                RuneUILayoutMetrics.resourceListActionsRailMinimumWidth,
                width - primaryWidth - spacing
            )
            let primarySize = subviews[0].sizeThatFits(
                ProposedViewSize(width: primaryWidth, height: nil)
            )
            let actionsSize = subviews[1].sizeThatFits(
                ProposedViewSize(width: actionsWidth, height: nil)
            )
            return (
                true,
                primaryWidth,
                actionsWidth,
                primarySize,
                actionsSize,
                max(minimumHeight, max(primarySize.height, actionsSize.height))
            )
        }

        let compactProposal = ProposedViewSize(width: width, height: nil)
        let primarySize = subviews[0].sizeThatFits(compactProposal)
        let actionsSize = subviews[1].sizeThatFits(compactProposal)
        return (
            false,
            width,
            width,
            primarySize,
            actionsSize,
            primarySize.height
                + RuneUILayoutMetrics.resourceListCompactRowSpacing
                + max(minimumHeight, actionsSize.height)
        )
    }
}

struct RuneResourceListToolbar<Primary: View, Actions: View>: View {
    let accessibilityLabel: String
    @ViewBuilder let primary: Primary
    @ViewBuilder let actions: Actions
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeInterfaceFontSize) private var interfaceFontSize

    init(
        _ accessibilityLabel: String = "Resource list controls",
        @ViewBuilder primary: () -> Primary,
        @ViewBuilder actions: () -> Actions
    ) {
        self.accessibilityLabel = accessibilityLabel
        self.primary = primary()
        self.actions = actions()
    }

    var body: some View {
        RuneResourceListToolbarLayout(minimumHeight: minimumHeight) {
            primary
            actionsRail
        }
        .frame(
            maxWidth: .infinity,
            minHeight: minimumHeight,
            alignment: .leading
        )
        .accessibilityElement(children: .contain)
        .accessibilityLabel(accessibilityLabel)
        .accessibilityIdentifier("resource-list-toolbar")
    }

    /// `EmptyView` deliberately has no layout footprint. Keep a transparent,
    /// non-interactive rail behind the contextual actions so resource families
    /// without actions still use the same grid and compact breakpoint.
    private var actionsRail: some View {
        ZStack(alignment: .leading) {
            Color.clear
                .frame(height: minimumHeight)
                .allowsHitTesting(false)
                .accessibilityHidden(true)
            actions
        }
    }

    private var minimumHeight: CGFloat {
        usesRegularControls
            ? RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight
            : RuneUILayoutMetrics.resourceListToolbarMinimumHeight
    }

    private var usesRegularControls: Bool {
        dynamicTypeSize.isAccessibilitySize
            || interfaceFontSize > RuneInterfaceTypography.standardMenuFontSize + 1
    }
}

struct RuneNoticeBanner: View {
    let notice: RuneUserNotice
    var onOpenFolder: ((URL) -> Void)? = nil
    var onOpenFile: ((URL) -> Void)? = nil
    let onDismiss: () -> Void
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        HStack(alignment: .top, spacing: RuneSpacing.xs) {
            Image(systemName: symbolName)
                .runeText(.sectionTitle)
                .foregroundStyle(tint)
                .frame(width: 18, height: 18)
                .padding(.top, RuneSpacing.hairline)

            VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                Text(notice.title)
                    .runeText(.microEmphasis)
                    .foregroundStyle(.runePrimary)
                Text(notice.message)
                    .runeText(.micro)
                    .foregroundStyle(.runeSecondary)
                    .fixedSize(horizontal: false, vertical: true)
                    .textSelection(.enabled)

                if let savedFileURL = notice.savedFileURL,
                   onOpenFolder != nil || onOpenFile != nil {
                    ViewThatFits(in: .horizontal) {
                        HStack(spacing: RuneSpacing.tight) {
                            savedFileActions(for: savedFileURL)
                        }
                        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
                            savedFileActions(for: savedFileURL)
                        }
                    }
                    .buttonStyle(RuneToolbarButtonStyle())
                    .controlSize(.small)
                    .runeInterfaceFont(relativeSize: -1)
                    .padding(.top, RuneSpacing.xxs)
                }
            }

            Spacer(minLength: 8)

            RuneIconButton(
                "Dismiss notice",
                systemImage: "xmark",
                help: "Dismiss",
                action: onDismiss
            )
        }
        .padding(.horizontal, RuneSpacing.sm)
        .padding(.vertical, RuneSpacing.xs)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .fill(tint.opacity(0.11))
        )
        .overlay {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .strokeBorder(tint.opacity(0.28), lineWidth: 1)
        }
        .accessibilityElement(children: .contain)
    }

    @ViewBuilder
    private func savedFileActions(for savedFileURL: URL) -> some View {
        if let onOpenFolder {
            Button {
                onOpenFolder(savedFileURL)
            } label: {
                Label("Open Folder", systemImage: "folder")
                    .fixedSize()
            }
            .help("Open the folder containing the saved log file.")
            .accessibilityIdentifier("saved-log-open-folder")
        }
        if let onOpenFile {
            Button {
                onOpenFile(savedFileURL)
            } label: {
                Label("Open File", systemImage: "doc.text")
                    .fixedSize()
            }
            .help("Open the saved log file in its default application.")
            .accessibilityIdentifier("saved-log-open-file")
        }
    }

    private var symbolName: String {
        switch notice.severity {
        case .info:
            return "info.circle.fill"
        case .warning:
            return "exclamationmark.triangle.fill"
        case .error:
            return "xmark.octagon.fill"
        }
    }

    private var tint: Color {
        switch notice.severity {
        case .info:
            return RuneSemanticColorRole.info.color(in: runeThemePalette)
        case .warning:
            return RuneSemanticColorRole.warning.color(in: runeThemePalette)
        case .error:
            return RuneSemanticColorRole.danger.color(in: runeThemePalette)
        }
    }
}

struct RuneInspectorInfoRow<Value: View>: View {
    let title: String
    let systemImage: String
    let fixedLabelWidth: CGFloat?
    @ViewBuilder var value: Value

    init(
        _ title: String,
        systemImage: String,
        fixedLabelWidth: CGFloat? = 118,
        @ViewBuilder value: () -> Value
    ) {
        self.title = title
        self.systemImage = systemImage
        self.fixedLabelWidth = fixedLabelWidth
        self.value = value()
    }

    var body: some View {
        ViewThatFits(in: .horizontal) {
            HStack(alignment: .firstTextBaseline, spacing: RuneSpacing.xs) {
                label
                    .frame(width: fixedLabelWidth, alignment: .leading)
                value
            }

            VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                label
                value
            }
        }
    }

    private var label: some View {
        HStack(spacing: RuneSpacing.tight) {
            Image(systemName: systemImage)
                .foregroundStyle(.runeSecondary)
                .frame(width: 14, alignment: .center)
            Text(title)
                .runeText(.micro)
                .foregroundStyle(.runeSecondary)
        }
    }
}

struct RuneInspectorActionRow<Content: View>: View {
    @ViewBuilder var content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    /// The inspector's actions sit on one quiet control card. The row scrolls
    /// sideways when the pane is narrow instead of wrapping, so resizing the
    /// pane never makes the inspector jump vertically.
    var body: some View {
        RuneControlCard {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarControlSpacing) {
                    content
                }
                .fixedSize(horizontal: true, vertical: false)
            }
            .controlSize(.regular)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

/// Short forms share one wrapping rule, including enlarged accessibility text.
struct RuneAdaptiveFormRow<Content: View>: View {
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @ViewBuilder let content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    var body: some View {
        Group {
            if dynamicTypeSize.isAccessibilitySize {
                stackedContent
            } else {
                ViewThatFits(in: .horizontal) {
                    HStack(alignment: .center, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
                        content
                    }
                    .fixedSize(horizontal: true, vertical: false)
                    stackedContent
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .runeInterfaceControlSize()
    }

    private var stackedContent: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
            content
        }
    }
}

/// Consistent bottom action area for custom macOS sheets. Actions remain
/// right-aligned, use regular controls, and are separated from body content.
struct RuneDialogActionBar<Actions: View>: View {
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @ViewBuilder var actions: Actions

    init(@ViewBuilder actions: () -> Actions) {
        self.actions = actions()
    }

    var body: some View {
        VStack(spacing: RuneUILayoutMetrics.dialogControlSpacing) {
            Divider()
            Group {
                if dynamicTypeSize.isAccessibilitySize {
                    stackedActions
                } else {
                    ViewThatFits(in: .horizontal) {
                        HStack(spacing: RuneUILayoutMetrics.dialogControlSpacing) {
                            actions
                        }
                        .fixedSize(horizontal: true, vertical: false)
                        stackedActions
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .trailing)
            .runeInterfaceControlSize()
        }
        .padding(.top, RuneSpacing.hairline)
    }

    private var stackedActions: some View {
        VStack(alignment: .trailing, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
            actions
        }
    }
}

/// Gives a text action a stable visual width without inflating compact dialogs.
struct RuneDialogButtonLabel: View {
    let title: String

    init(_ title: String) {
        self.title = title
    }

    var body: some View {
        Text(title)
            .fixedSize(horizontal: false, vertical: true)
            .frame(
                minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
            )
    }
}

/// Standard close affordance for custom sheet headers with a full 28-point hit target.
struct RuneDialogCloseButton: View {
    let accessibilityLabel: String
    let action: () -> Void

    init(_ accessibilityLabel: String = "Close", action: @escaping () -> Void) {
        self.accessibilityLabel = accessibilityLabel
        self.action = action
    }

    var body: some View {
        RuneIconButton(accessibilityLabel, systemImage: "xmark", action: action)
        .keyboardShortcut(.cancelAction)
    }
}

extension View {
    func runePanelCard(padding: CGFloat = RuneSpacing.sm, alignment: Alignment = .leading) -> some View {
        self
            .padding(padding)
            .frame(maxWidth: .infinity, alignment: alignment)
            .background(RuneSurfaceBackground(kind: .panel))
    }

    func runeInsetCard(padding: CGFloat = RuneSpacing.sm, alignment: Alignment = .leading) -> some View {
        self
            .padding(padding)
            .frame(maxWidth: .infinity, alignment: alignment)
            .background(RuneSurfaceBackground(kind: .inset))
    }

    func runeSidebarSelection(isSelected: Bool) -> some View {
        self.background(RuneSurfaceBackground(kind: .sidebarSelection(isSelected: isSelected)))
    }
}
