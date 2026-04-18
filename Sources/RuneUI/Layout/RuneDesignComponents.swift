import AppKit
import SwiftUI

enum RuneSurfaceKind {
    case panel
    case inset
    case editor
    case listRow(isSelected: Bool)
    case sidebarSelection(isSelected: Bool)

    var cornerRadius: CGFloat {
        switch self {
        case .panel, .inset, .listRow:
            return RuneUILayoutMetrics.groupedContentCornerRadius
        case .editor, .sidebarSelection:
            return RuneUILayoutMetrics.interactiveRowCornerRadius
        }
    }

    var fill: Color {
        switch self {
        case .panel:
            return Color(nsColor: .controlBackgroundColor).opacity(0.72)
        case .inset:
            return Color(nsColor: .controlBackgroundColor)
        case .editor:
            return Color(nsColor: .textBackgroundColor).opacity(0.92)
        case let .listRow(isSelected):
            return isSelected
                ? Color.accentColor.opacity(0.16)
                : Color(nsColor: .controlBackgroundColor).opacity(0.72)
        case let .sidebarSelection(isSelected):
            return isSelected ? Color.accentColor.opacity(0.16) : Color.clear
        }
    }

    var stroke: Color? {
        switch self {
        case .panel:
            return Color(nsColor: .separatorColor).opacity(0.24)
        case .inset:
            return Color(nsColor: .separatorColor).opacity(0.45)
        case .editor:
            return Color(nsColor: .separatorColor).opacity(0.24)
        case let .listRow(isSelected):
            return isSelected
                ? Color.accentColor.opacity(0.28)
                : Color(nsColor: .separatorColor).opacity(0.32)
        case .sidebarSelection:
            return nil
        }
    }
}

struct RuneSurfaceBackground: View {
    let kind: RuneSurfaceKind

    var body: some View {
        RoundedRectangle(cornerRadius: kind.cornerRadius, style: .continuous)
            .fill(kind.fill)
            .overlay {
                if let stroke = kind.stroke {
                    RoundedRectangle(cornerRadius: kind.cornerRadius, style: .continuous)
                        .strokeBorder(stroke, lineWidth: 1)
                }
            }
    }
}

struct RuneChip<Content: View>: View {
    let horizontalPadding: CGFloat
    let verticalPadding: CGFloat
    let fill: Color
    let cornerRadius: CGFloat
    @ViewBuilder var content: Content

    init(
        horizontalPadding: CGFloat = 8,
        verticalPadding: CGFloat = 3,
        fill: Color = Color.secondary.opacity(0.12),
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
                    .fill(fill)
            )
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
        horizontalPadding: CGFloat = 12,
        verticalPadding: CGFloat = 10,
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
