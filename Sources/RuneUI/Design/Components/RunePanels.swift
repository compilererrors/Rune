import SwiftUI

/// Grouped content card inside a pane: one fill, a hairline stroke and the
/// shared panel radius. Use it for module-sized groups, not for every row.
struct RunePanel<Content: View>: View {
    let padding: CGFloat
    let kind: RuneSurfaceKind
    @ViewBuilder var content: Content

    init(
        padding: CGFloat = RuneSpacing.sm,
        kind: RuneSurfaceKind = .panel,
        @ViewBuilder content: () -> Content
    ) {
        self.padding = padding
        self.kind = kind
        self.content = content()
    }

    var body: some View {
        content
            .padding(padding)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(RuneSurfaceBackground(kind: kind))
    }
}

/// Small uppercase group label with an optional trailing accessory, used for
/// sidebar groups and inspector sections.
struct RuneGroupLabel<Accessory: View>: View {
    let title: String
    let systemImage: String?
    let isEmphasized: Bool
    @ViewBuilder var accessory: Accessory

    init(
        _ title: String,
        systemImage: String? = nil,
        isEmphasized: Bool = false,
        @ViewBuilder accessory: () -> Accessory
    ) {
        self.title = title
        self.systemImage = systemImage
        self.isEmphasized = isEmphasized
        self.accessory = accessory()
    }

    var body: some View {
        HStack(spacing: RuneSpacing.tight) {
            if let systemImage {
                Image(systemName: systemImage)
                    .runeText(.micro)
                    .accessibilityHidden(true)
            }
            Text(title)
                .runeText(.groupLabel)
                .lineLimit(1)
            Spacer(minLength: RuneSpacing.xs)
            accessory
        }
        .foregroundStyle(isEmphasized ? AnyShapeStyle(RuneTextStyle.accent) : AnyShapeStyle(.runeSecondary))
        .frame(minHeight: RuneControlHeight.compact, alignment: .center)
        .accessibilityElement(children: .combine)
        .accessibilityAddTraits(.isHeader)
    }
}

extension RuneGroupLabel where Accessory == EmptyView {
    init(_ title: String, systemImage: String? = nil, isEmphasized: Bool = false) {
        self.init(title, systemImage: systemImage, isEmphasized: isEmphasized) { EmptyView() }
    }
}

/// Rounded, tinted symbol tile used to anchor page titles and module headers.
struct RuneSymbolTile: View {
    let systemImage: String
    let tint: Color
    let size: CGFloat

    init(_ systemImage: String, tint: Color, size: CGFloat = RuneControlHeight.regular) {
        self.systemImage = systemImage
        self.tint = tint
        self.size = size
    }

    var body: some View {
        Image(systemName: systemImage)
            .font(.system(size: size * 0.5, weight: .semibold))
            .foregroundStyle(tint)
            .frame(width: size, height: size)
            .background(
                RoundedRectangle(cornerRadius: RuneRadius.control, style: .continuous)
                    .fill(tint.opacity(0.14))
            )
            .overlay(
                RoundedRectangle(cornerRadius: RuneRadius.control, style: .continuous)
                    .strokeBorder(tint.opacity(0.22), lineWidth: 1)
            )
            .accessibilityHidden(true)
    }
}

/// The quiet card behind one cluster of related controls (an inspector's
/// actions, a command field with its buttons, port-forward endpoints). It
/// gives the cluster context without heavy chrome: the row fill, a faint
/// border and the control radius, with 8 pt inside a row of 28 pt controls
/// (44 pt in all). Buttons inside default to the toolbar style.
struct RuneControlCard<Content: View>: View {
    var minHeight: CGFloat = RuneUILayoutMetrics.inspectorToolbarActionGroupHeight
    var verticalPadding: CGFloat = RuneUILayoutMetrics.inspectorControlSurfaceVerticalPadding
    var fillsWidth = false
    @ViewBuilder let content: Content
    @Environment(\.runeThemePalette) private var palette

    init(
        minHeight: CGFloat = RuneUILayoutMetrics.inspectorToolbarActionGroupHeight,
        verticalPadding: CGFloat = RuneUILayoutMetrics.inspectorControlSurfaceVerticalPadding,
        fillsWidth: Bool = false,
        @ViewBuilder content: () -> Content
    ) {
        self.minHeight = minHeight
        self.verticalPadding = verticalPadding
        self.fillsWidth = fillsWidth
        self.content = content()
    }

    var body: some View {
        let shape = RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
        content
            .buttonStyle(RuneToolbarButtonStyle())
            .padding(.horizontal, RuneUILayoutMetrics.inspectorToolbarGroupHorizontalPadding)
            .padding(.vertical, verticalPadding)
            .frame(maxWidth: fillsWidth ? .infinity : nil, minHeight: minHeight, alignment: .leading)
            .background(shape.fill(palette?.row.opacity(0.42) ?? Color.primary.opacity(0.04)))
            .overlay {
                shape
                    .strokeBorder(palette?.stroke.opacity(0.28) ?? Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
                    .allowsHitTesting(false)
            }
    }
}
