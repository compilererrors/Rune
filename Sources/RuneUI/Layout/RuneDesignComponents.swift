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
                return palette.panel.opacity(0.92)
            case .inset:
                return palette.inset.opacity(0.92)
            case .editor:
                return palette.editor.opacity(0.96)
            case let .listRow(isSelected):
                return isSelected ? palette.rowSelected.opacity(0.72) : palette.row.opacity(0.62)
            case let .sidebarSelection(isSelected):
                return isSelected ? palette.accent.opacity(0.18) : Color.clear
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
        horizontalPadding: CGFloat = 8,
        verticalPadding: CGFloat = 3,
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
                .font(.system(size: 11, weight: .semibold))
                .frame(
                    width: RuneUILayoutMetrics.iconButtonSize,
                    height: RuneUILayoutMetrics.iconButtonSize
                )
                .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .foregroundStyle(foregroundColor)
        .background {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                .fill(isSelected == true ? selectionColor.opacity(0.16) : Color.clear)
        }
        .contentShape(Rectangle())
        .disabled(isDisabled)
        .opacity(isDisabled ? 0.52 : 1)
        .help(helpText)
        .accessibilityLabel(accessibilityLabel)
        .modifier(RuneIconButtonSelectionAccessibilityModifier(isSelected: isSelected))
    }

    private var selectionColor: Color {
        selectedTint ?? runeThemePalette?.accent ?? Color.accentColor
    }

    private var foregroundColor: Color {
        if isDisabled {
            return runeThemePalette?.mutedText ?? Color.secondary
        }
        if isSelected == true {
            return selectionColor
        }
        return runeThemePalette?.secondaryText ?? Color.secondary
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
                .font(.system(size: 11, weight: .semibold))
                .frame(maxWidth: .infinity)
                .contentShape(Rectangle())
        }
        .buttonStyle(.bordered)
        .controlSize(.small)
        .frame(width: RuneUILayoutMetrics.borderedIconButtonWidth)
        .frame(minHeight: RuneUILayoutMetrics.inspectorToolbarControlMinHeight)
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

    var body: some View {
        ViewThatFits(in: .horizontal) {
            regularControls
            compactControls
        }
        .buttonStyle(.bordered)
        .runeInterfaceFont(relativeSize: -1, weight: .semibold)
        .controlSize(usesRegularControls ? .regular : .small)
        .frame(
            maxWidth: .infinity,
            minHeight: dynamicTypeSize.isAccessibilitySize
                ? RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight
                : RuneUILayoutMetrics.resourceListToolbarMinimumHeight,
            alignment: .leading
        )
        .accessibilityElement(children: .contain)
    }

    private var regularControls: some View {
        HStack(spacing: 8) {
            if selectedCount > 0 {
                selectionSummary
            }
            selectVisibleButton
            if showsActions {
                separator
                actions
            }
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var compactControls: some View {
        HStack(spacing: 6) {
            if selectedCount > 0 {
                selectionSummary
            }
            compactSelectVisibleButton
            if showsActions {
                actions
            }
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var compactSelectVisibleButton: some View {
        Button {
            onToggleVisibleSelection()
        } label: {
            Image(systemName: allVisibleSelected ? "xmark.square" : "checklist")
        }
        .frame(width: RuneUILayoutMetrics.iconButtonSize)
        .runeMinimumInteractiveTarget()
        .disabled(visibleCount == 0)
        .help(allVisibleSelected ? "Deselect all visible rows" : "Select all visible rows")
        .accessibilityLabel(toggleTitle)
    }

    private var selectionCountChip: some View {
        RuneChip(
            horizontalPadding: 8,
            verticalPadding: 3,
            fill: selectedCount > 0
                ? (runeThemePalette?.selectionFill ?? Color.accentColor.opacity(0.14))
                : (runeThemePalette?.chipFill ?? Color.secondary.opacity(0.10))
        ) {
            Label(selectedCountText, systemImage: selectedCount > 0 ? "checkmark.square.fill" : "square")
                .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                .labelStyle(.titleAndIcon)
                .imageScale(.small)
                .foregroundStyle(
                    selectedCount > 0
                        ? (runeThemePalette?.accent ?? Color.accentColor)
                        : (runeThemePalette?.secondaryText ?? Color.secondary)
                )
        }
        .frame(height: RuneUILayoutMetrics.headerChipHeight)
        .accessibilityLabel(selectedCountText)
    }

    private var selectionSummary: some View {
        HStack(spacing: 2) {
            selectionCountChip
            RuneIconButton(
                "Clear selection",
                systemImage: "xmark",
                help: "Clear all selected rows"
            ) {
                onClearSelection()
            }
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var selectVisibleButton: some View {
        Button {
            onToggleVisibleSelection()
        } label: {
            Label(toggleTitle, systemImage: allVisibleSelected ? "xmark.square" : "checklist")
        }
        .runeMinimumInteractiveTarget()
        .disabled(visibleCount == 0)
        .help(allVisibleSelected ? "Deselect all visible rows" : "Select all visible rows")
        .accessibilityLabel(toggleTitle)
    }

    private var separator: some View {
        Rectangle()
            .fill(runeThemePalette?.divider ?? Color(nsColor: .separatorColor).opacity(0.45))
            .frame(width: 1, height: RuneUILayoutMetrics.headerChipHeight - 8)
            .accessibilityHidden(true)
    }

    private var toggleTitle: String {
        allVisibleSelected ? "Deselect Visible" : "Select Visible"
    }

    private var selectedCountText: String {
        "\(selectedCount) selected"
    }

    private var usesRegularControls: Bool {
        dynamicTypeSize.isAccessibilitySize
            || interfaceFontSize > RuneInterfaceTypography.standardMenuFontSize + 1
    }
}

/// One stable control band shared by every resource list. The primary control
/// keeps enough width for filtering while secondary controls own their compact
/// overflow behavior, avoiding per-resource wrapping and table-position jumps.
private struct RuneResourceListToolbarLayout: Layout {
    let minimumHeight: CGFloat

    private var inlineMinimumWidth: CGFloat {
        RuneUILayoutMetrics.resourceFilterControlsMaximumWidth
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
            let primaryWidth = RuneUILayoutMetrics.resourceFilterControlsMaximumWidth
            let actionsWidth = max(
                RuneUILayoutMetrics.resourceListActionsRailMinimumWidth,
                width - primaryWidth - RuneUILayoutMetrics.contentControlSpacing
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
        .runeInterfaceFont(relativeSize: -1, weight: .medium)
        .controlSize(usesRegularControls ? .regular : .small)
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
    let onDismiss: () -> Void
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        HStack(alignment: .top, spacing: 10) {
            Image(systemName: symbolName)
                .font(.system(size: 14, weight: .semibold))
                .foregroundStyle(tint)
                .frame(width: 18, height: 18)
                .padding(.top, 1)

            VStack(alignment: .leading, spacing: 3) {
                Text(notice.title)
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.primary)
                Text(notice.message)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                    .textSelection(.enabled)
            }

            Spacer(minLength: 8)

            RuneIconButton(
                "Dismiss notice",
                systemImage: "xmark",
                help: "Dismiss",
                action: onDismiss
            )
        }
        .padding(.horizontal, 11)
        .padding(.vertical, 9)
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
            HStack(alignment: .firstTextBaseline, spacing: 10) {
                label
                    .frame(width: fixedLabelWidth, alignment: .leading)
                value
            }

            VStack(alignment: .leading, spacing: 4) {
                label
                value
            }
        }
    }

    private var label: some View {
        HStack(spacing: 6) {
            Image(systemName: systemImage)
                .foregroundStyle(.secondary)
                .frame(width: 14, alignment: .center)
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }
}

struct RuneInspectorActionRow<Content: View>: View {
    @ViewBuilder var content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarGroupSpacing) {
                content
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .runeInterfaceFont(relativeSize: -1, weight: .medium)
        .controlSize(.regular)
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

/// Consistent bottom action area for custom macOS sheets. Actions remain
/// right-aligned, use regular controls, and are separated from body content.
struct RuneDialogActionBar<Actions: View>: View {
    @ViewBuilder var actions: Actions

    init(@ViewBuilder actions: () -> Actions) {
        self.actions = actions()
    }

    var body: some View {
        VStack(spacing: RuneUILayoutMetrics.dialogControlSpacing) {
            Divider()
            HStack(spacing: RuneUILayoutMetrics.dialogControlSpacing) {
                Spacer(minLength: 0)
                actions
            }
            .controlSize(.regular)
        }
        .padding(.top, 2)
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
    func runePanelCard(padding: CGFloat = 12, alignment: Alignment = .leading) -> some View {
        self
            .padding(padding)
            .frame(maxWidth: .infinity, alignment: alignment)
            .background(RuneSurfaceBackground(kind: .panel))
    }

    func runeInsetCard(padding: CGFloat = 14, alignment: Alignment = .leading) -> some View {
        self
            .padding(padding)
            .frame(maxWidth: .infinity, alignment: alignment)
            .background(RuneSurfaceBackground(kind: .inset))
    }

    func runeSidebarSelection(isSelected: Bool) -> some View {
        self.background(RuneSurfaceBackground(kind: .sidebarSelection(isSelected: isSelected)))
    }
}
