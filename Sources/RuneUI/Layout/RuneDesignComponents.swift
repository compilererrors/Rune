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
            return Color(nsColor: .controlBackgroundColor).opacity(0.72)
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
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault

    var body: some View {
        let theme = RuneAppearanceTheme.resolved(appearanceThemeRaw)
        RoundedRectangle(cornerRadius: kind.cornerRadius, style: .continuous)
            .fill(kind.fill(theme: theme))
            .overlay {
                if let stroke = kind.stroke(theme: theme) {
                    RoundedRectangle(cornerRadius: kind.cornerRadius, style: .continuous)
                        .strokeBorder(stroke, lineWidth: 1)
                }
            }
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

struct RuneSelectionCheckboxButton: View {
    let isSelected: Bool
    let accessibilityLabel: String
    let selectedHelp: String
    let deselectedHelp: String
    let onToggle: () -> Void

    init(
        isSelected: Bool,
        accessibilityLabel: String,
        selectedHelp: String,
        deselectedHelp: String,
        onToggle: @escaping () -> Void
    ) {
        self.isSelected = isSelected
        self.accessibilityLabel = accessibilityLabel
        self.selectedHelp = selectedHelp
        self.deselectedHelp = deselectedHelp
        self.onToggle = onToggle
    }

    var body: some View {
        Toggle(
            isOn: Binding(
                get: { isSelected },
                set: { _ in onToggle() }
            )
        ) {
            Text(accessibilityLabel)
        }
        .toggleStyle(.checkbox)
        .labelsHidden()
        .controlSize(.small)
        .frame(width: 22, height: 22, alignment: .center)
        .contentShape(Rectangle())
        .accessibilityLabel(accessibilityLabel)
        .accessibilityValue(isSelected ? "Selected" : "Not selected")
        .help(isSelected ? selectedHelp : deselectedHelp)
    }
}

struct RuneBulkSelectionBar<Actions: View>: View {
    let selectedCount: Int
    let visibleCount: Int
    let allVisibleSelected: Bool
    let onToggleVisibleSelection: () -> Void
    @ViewBuilder var actions: Actions
    @Environment(\.runeThemePalette) private var runeThemePalette

    init(
        selectedCount: Int,
        visibleCount: Int,
        allVisibleSelected: Bool,
        onToggleVisibleSelection: @escaping () -> Void,
        @ViewBuilder actions: () -> Actions
    ) {
        self.selectedCount = selectedCount
        self.visibleCount = visibleCount
        self.allVisibleSelected = allVisibleSelected
        self.onToggleVisibleSelection = onToggleVisibleSelection
        self.actions = actions()
    }

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 8) {
                selectionCountChip
                selectVisibleButton
                separator
                actions
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .buttonStyle(.bordered)
        .controlSize(.small)
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .contain)
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
                .font(.caption.weight(.semibold))
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

    private var selectVisibleButton: some View {
        Button {
            onToggleVisibleSelection()
        } label: {
            Label(toggleTitle, systemImage: allVisibleSelected ? "xmark.square" : "checklist")
        }
        .disabled(visibleCount == 0)
        .help(allVisibleSelected ? "Deselect all visible pods" : "Select all visible pods")
        .accessibilityLabel(toggleTitle)
    }

    private var separator: some View {
        Rectangle()
            .fill(runeThemePalette?.divider ?? Color(nsColor: .separatorColor).opacity(0.45))
            .frame(width: 1, height: RuneUILayoutMetrics.headerChipHeight - 8)
            .accessibilityHidden(true)
    }

    private var toggleTitle: String {
        allVisibleSelected ? "Deselect All" : "Select All"
    }

    private var selectedCountText: String {
        "\(selectedCount) selected"
    }
}

struct RuneNoticeBanner: View {
    let notice: RuneUserNotice
    let onDismiss: () -> Void

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

            Button {
                onDismiss()
            } label: {
                Image(systemName: "xmark")
                    .font(.system(size: 10, weight: .bold))
                    .frame(width: 18, height: 18)
            }
            .buttonStyle(.plain)
            .foregroundStyle(.secondary)
            .help("Dismiss")
            .accessibilityLabel("Dismiss notice")
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
        .accessibilityElement(children: .combine)
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
            return .blue
        case .warning:
            return .orange
        case .error:
            return .red
        }
    }
}

struct RuneInspectorSection<Content: View>: View {
    let padding: CGFloat
    @ViewBuilder var content: Content

    init(
        padding: CGFloat = 14,
        @ViewBuilder content: () -> Content
    ) {
        self.padding = padding
        self.content = content()
    }

    var body: some View {
        content
            .padding(padding)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(RuneSurfaceBackground(kind: .inset))
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
                .font(.caption.weight(.semibold))
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
        .controlSize(.regular)
        .frame(maxWidth: .infinity, alignment: .leading)
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

    func runeEditorCard(padding: CGFloat = 10, alignment: Alignment = .leading) -> some View {
        self
            .padding(padding)
            .frame(maxWidth: .infinity, alignment: alignment)
            .background(RuneSurfaceBackground(kind: .editor))
    }

    func runeListRowCard(
        isSelected: Bool,
        horizontalPadding: CGFloat = 10,
        verticalPadding: CGFloat = 6,
        alignment: Alignment = .leading
    ) -> some View {
        self
            .padding(.horizontal, horizontalPadding)
            .padding(.vertical, verticalPadding)
            .frame(maxWidth: .infinity, alignment: alignment)
            .background(RuneSurfaceBackground(kind: .listRow(isSelected: isSelected)))
            .contentShape(Rectangle())
    }

    func runeSidebarSelection(isSelected: Bool) -> some View {
        self.background(RuneSurfaceBackground(kind: .sidebarSelection(isSelected: isSelected)))
    }
}
