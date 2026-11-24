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
    /// A concentric radius for a container whose inset differs from the
    /// kind's default (for example the terminal tab strip).
    var cornerRadius: CGFloat? = nil
    @Environment(\.runeResolvedTheme) private var resolvedTheme

    var body: some View {
        let radius = cornerRadius ?? kind.cornerRadius
        RoundedRectangle(cornerRadius: radius, style: .continuous)
            .fill(kind.fill(theme: resolvedTheme))
            .overlay {
                if let stroke = kind.stroke(theme: resolvedTheme) {
                    RoundedRectangle(cornerRadius: radius, style: .continuous)
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
                if notice.severity == .info {
                    Text(notice.message)
                        .runeText(.micro)
                        .foregroundStyle(.runeSecondary)
                        .fixedSize(horizontal: false, vertical: true)
                        .textSelection(.enabled)
                } else {
                    // Warnings and errors carry what the cluster or client said: shown word for word, copyable.
                    RuneErrorDetail(Self.readableErrorMessage(notice.message))
                }

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

    /// A partial load joins one error per resource with " | "; each goes on its own line.
    static func readableErrorMessage(_ message: String) -> String {
        message.replacingOccurrences(of: " | ", with: "\n")
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

/// One row of a fact grid: a label in a fixed column and its value, with a
/// hairline above it. Copy lives in the context menu and in a button that
/// appears on hover, as in Finder's Get Info and Xcode's inspectors, so the
/// grid stays quiet until you reach for a value.
struct RuneInspectorInfoRow<Value: View>: View {
    let title: String
    let fixedLabelWidth: CGFloat?
    let copyValue: String?
    let onCopy: ((String) -> Void)?
    @ViewBuilder var value: Value
    @State private var isHovered = false

    init(
        _ title: String,
        fixedLabelWidth: CGFloat? = RuneUILayoutMetrics.factLabelWidth,
        copyValue: String? = nil,
        onCopy: ((String) -> Void)? = nil,
        @ViewBuilder value: () -> Value
    ) {
        self.title = title
        self.fixedLabelWidth = fixedLabelWidth
        self.copyValue = copyValue
        self.onCopy = onCopy
        self.value = value()
    }

    var body: some View {
        ViewThatFits(in: .horizontal) {
            HStack(alignment: .firstTextBaseline, spacing: RuneSpacing.sm) {
                label
                    .frame(width: fixedLabelWidth, alignment: .leading)
                valueColumn
            }

            VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                label
                valueColumn
            }
        }
        .padding(.vertical, RuneUILayoutMetrics.factRowVerticalPadding)
        .padding(.trailing, canCopy ? RuneUILayoutMetrics.iconButtonSize : 0)
        .frame(maxWidth: .infinity, alignment: .leading)
        .overlay(alignment: .top) {
            RuneHairline()
        }
        .overlay(alignment: .trailing) {
            if canCopy, let copyValue, let onCopy {
                // Always laid out, shown on hover: the row never shifts.
                RuneIconButton("Copy \(title.lowercased())", systemImage: "doc.on.doc") {
                    onCopy(copyValue)
                }
                .opacity(isHovered ? 1 : 0)
                .allowsHitTesting(isHovered)
            }
        }
        .contentShape(Rectangle())
        .onHover { isHovered = $0 }
        .contextMenu {
            if canCopy, let copyValue, let onCopy {
                Button("Copy \(title)") { onCopy(copyValue) }
            }
        }
        .accessibilityElement(children: .combine)
    }

    private var canCopy: Bool {
        copyValue != nil && onCopy != nil
    }

    private var valueColumn: some View {
        value
            .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var label: some View {
        Text(title)
            .runeText(.caption)
            .foregroundStyle(.runeSecondary)
            .lineLimit(2)
            .fixedSize(horizontal: false, vertical: true)
    }
}

/// The quiet label above an inspector section. Sections are separated by
/// space and this header, never by a card around them.
struct RuneSectionLabel<Accessory: View>: View {
    let title: String
    @ViewBuilder var accessory: Accessory

    init(_ title: String, @ViewBuilder accessory: () -> Accessory) {
        self.title = title
        self.accessory = accessory()
    }

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: RuneSpacing.xs) {
            Text(title)
                .runeText(.microEmphasis)
                .foregroundStyle(.runeSecondary)
                .accessibilityAddTraits(.isHeader)
            Spacer(minLength: 0)
            accessory
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

extension RuneSectionLabel where Accessory == EmptyView {
    init(_ title: String) {
        self.init(title) { EmptyView() }
    }
}

/// A hairline between rows of one section.
struct RuneHairline: View {
    var body: some View {
        Rectangle()
            .fill(Color(nsColor: .separatorColor))
            .frame(height: 1)
            .accessibilityHidden(true)
    }
}

/// Key/value facts as one aligned grid: an optional section header, then
/// rows with a hairline between each. No card; the header, the shared label
/// column and the hairlines are the grouping.
struct RuneFactGrid<Rows: View>: View {
    let title: String?
    @ViewBuilder var rows: Rows

    init(_ title: String? = nil, @ViewBuilder rows: () -> Rows) {
        self.title = title
        self.rows = rows()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
            if let title {
                RuneSectionLabel(title)
            }
            VStack(alignment: .leading, spacing: 0) {
                rows
            }
            // Each row draws the hairline above it; masking the first one
            // leaves hairlines only between rows.
            .mask {
                Rectangle().padding(.top, 1)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .contain)
    }
}

/// A fact value that opens what it names, styled as a link so it reads as a
/// value first. Replaces a separate "Go to" button next to the facts.
struct RuneFactLink: View {
    let title: String
    let help: String
    let action: () -> Void

    init(_ title: String, help: String, action: @escaping () -> Void) {
        self.title = title
        self.help = help
        self.action = action
    }

    var body: some View {
        Button(action: action) {
            HStack(alignment: .firstTextBaseline, spacing: RuneSpacing.xxs) {
                Text(title)
                    .runeText(.body)
                    .lineLimit(1)
                    .truncationMode(.middle)
                Image(systemName: "arrow.right.circle")
                    .runeText(.micro)
            }
        }
        .buttonStyle(.link)
        .help(help)
    }
}

/// The status of the inspected object, shown as a pill beside the subtitle.
struct RuneInspectorStatus: Equatable, Sendable {
    let text: String
    let tone: RuneBadgeTone

    init(_ text: String, tone: RuneBadgeTone) {
        self.text = text
        self.tone = tone
    }

    /// Maps common Kubernetes and Helm status words to a tone.
    init(_ text: String) {
        self.init(text, tone: Self.tone(for: text))
    }

    static func tone(for status: String) -> RuneBadgeTone {
        let value = status.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let success = ["running", "deployed", "ready", "active", "bound", "available", "succeeded", "completed", "complete", "normal", "healthy", "true"]
        let warning = ["pending", "warning", "progressing", "pending-install", "pending-upgrade", "pending-rollback", "uninstalling", "terminating", "containercreating", "unknown", "superseded"]
        let danger = ["failed", "error", "crashloopbackoff", "backoff", "imagepullbackoff", "errimagepull", "evicted", "oomkilled", "lost", "false"]
        if success.contains(value) { return .success }
        if warning.contains(value) { return .warning }
        if danger.contains(value) { return .danger }
        return .neutral
    }
}

struct RuneInspectorActionRow<Content: View>: View {
    @ViewBuilder var content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    /// The inspector's actions sit in a plain row under the tabs, with no
    /// card around them: a box around one or two buttons adds a level without
    /// adding meaning. The row scrolls sideways when the pane is narrow
    /// instead of wrapping, so resizing the pane never makes the inspector
    /// jump vertically.
    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarControlSpacing) {
                content
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .controlSize(.regular)
        .frame(maxWidth: .infinity, minHeight: RuneControlHeight.toolbar, alignment: .leading)
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
