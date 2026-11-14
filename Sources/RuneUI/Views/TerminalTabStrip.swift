import SwiftUI

enum TerminalTabChromeEmphasis {
    case standard
    case draft
}

/// Native tab-bar proportions (macOS window tabs, as Ghostty and iTerm2
/// use them): one compact row whose tabs share the full width.
enum TerminalTabLayoutMetrics {
    static let tabHeight: CGFloat = 28
    /// Tabs share the strip's width equally; below this width each they stop
    /// shrinking and the strip scrolls instead.
    static let minimumTabWidth: CGFloat = 140
    /// The leading close and trailing favourite slots, so a centred title
    /// never runs under them.
    static let accessorySlotWidth: CGFloat = 24
    static let addSlotWidth: CGFloat = 36
}

/// Lays tabs out at equal widths that share `availableWidth` (one tab spans
/// all of it), never narrower than `minimumTabWidth`; past that the row is
/// wider than the strip and the strip's scroll view scrolls. One layout, no
/// breakpoints, so the strip's height never changes.
struct RuneEqualWidthTabsLayout: Layout {
    var availableWidth: CGFloat
    var minimumTabWidth: CGFloat = TerminalTabLayoutMetrics.minimumTabWidth

    func tabWidth(count: Int) -> CGFloat {
        guard count > 0 else { return 0 }
        return max(minimumTabWidth, availableWidth / CGFloat(count))
    }

    func sizeThatFits(proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) -> CGSize {
        CGSize(
            width: max(availableWidth, tabWidth(count: subviews.count) * CGFloat(subviews.count)),
            height: TerminalTabLayoutMetrics.tabHeight
        )
    }

    func placeSubviews(in bounds: CGRect, proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) {
        let width = tabWidth(count: subviews.count)
        for (index, subview) in subviews.enumerated() {
            subview.place(
                at: CGPoint(x: bounds.minX + CGFloat(index) * width, y: bounds.minY),
                anchor: .topLeading,
                proposal: ProposedViewSize(width: width, height: bounds.height)
            )
        }
    }
}

/// A tab title: centred, middle-truncated, with optional secondary text
/// that gives way first when space runs out.
struct TerminalTabTitle: View {
    let primaryTitle: String
    let secondaryTitle: String?
    var leadingSymbol: String? = nil
    var leadingTint: Color? = nil
    var statusDotColor: Color? = nil
    var trailingNote: String? = nil
    var trailingNoteColor: Color? = nil

    var body: some View {
        HStack(spacing: RuneSpacing.tight) {
            if let leadingSymbol {
                Image(systemName: leadingSymbol)
                    .runeText(.micro)
                    .foregroundStyle(leadingTint ?? Color.secondary)
            }
            if let statusDotColor {
                TerminalStatusDot(color: statusDotColor)
            }
            Text(primaryTitle)
                .runeText(.microEmphasis)
                .lineLimit(1)
                .truncationMode(.middle)
                .layoutPriority(2)
            if let secondaryTitle, !secondaryTitle.isEmpty {
                Text(secondaryTitle)
                    .runeText(.micro)
                    .foregroundStyle(.runeSecondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
            }
            if let trailingNote {
                Text(trailingNote)
                    .runeText(.micro)
                    .foregroundStyle(trailingNoteColor ?? Color.secondary)
                    .lineLimit(1)
                    .layoutPriority(1)
            }
        }
        .padding(.horizontal, TerminalTabLayoutMetrics.accessorySlotWidth)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)
        .contentShape(Rectangle())
    }
}

/// Shared label for empty and draft tabs.
struct TerminalPlaceholderTabLabel: View {
    let primaryTitle: String
    let secondaryTitle: String
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        TerminalTabTitle(
            primaryTitle: primaryTitle,
            secondaryTitle: secondaryTitle,
            leadingSymbol: "plus.circle.fill",
            leadingTint: runeThemePalette?.accent ?? Color.accentColor
        )
    }
}

private struct TerminalTabIsHoveredKey: EnvironmentKey {
    static let defaultValue = false
}

extension EnvironmentValues {
    /// True while the pointer is over the enclosing tab, so its close button
    /// (and other secondary tab controls) can appear on hover only.
    var terminalTabIsHovered: Bool {
        get { self[TerminalTabIsHoveredKey.self] }
        set { self[TerminalTabIsHoveredKey.self] = newValue }
    }
}

/// A tab in a native-style tab bar on the top edge of a surface, as in macOS
/// window tabs (Ghostty, iTerm2, Terminal, Safari): the selected tab is the
/// surface's own fill with no border, so it flows into the content; the
/// others sit on the recessed strip with hairline dividers and secondary text.
struct TerminalTabChromeModifier: ViewModifier {
    let isActive: Bool
    let emphasis: TerminalTabChromeEmphasis
    @Environment(\.runeThemePalette) private var runeThemePalette
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.runeSurfaceTabRole) private var surfaceRole
    @State private var isHovered = false

    func body(content: Content) -> some View {
        content
            .frame(maxWidth: .infinity, minHeight: TerminalTabLayoutMetrics.tabHeight, maxHeight: TerminalTabLayoutMetrics.tabHeight)
            .foregroundStyle(isActive ? .runePrimary : .runeSecondary)
            .environment(\.terminalTabIsHovered, isHovered)
            .background(fill)
            .overlay(alignment: .trailing) {
                if !isActive {
                    TerminalTabDivider()
                }
            }
            .contentShape(Rectangle())
            .onHover { isHovered = $0 }
    }

    private var fill: Color {
        if isActive {
            return surfaceRole.fill(palette: runeThemePalette, colorScheme: colorScheme)
        }
        if isHovered {
            return (runeThemePalette?.foreground ?? Color.primary).opacity(0.05)
        }
        return .clear
    }
}

/// The hairline between tabs, inset from the strip's edges as in native tab bars.
struct TerminalTabDivider: View {
    var body: some View {
        Rectangle()
            .fill(Color(nsColor: .separatorColor))
            .frame(width: 1)
            .padding(.vertical, RuneSpacing.tight)
            .accessibilityHidden(true)
    }
}

/// Secondary tab controls (close, favorite) show while the tab is hovered,
/// keeping their space so the title never shifts.
struct TerminalTabHoverReveal: ViewModifier {
    var alwaysVisible = false
    @Environment(\.terminalTabIsHovered) private var isHovered

    func body(content: Content) -> some View {
        content
            .opacity(isHovered || alwaysVisible ? 1 : 0)
            .allowsHitTesting(isHovered || alwaysVisible)
    }
}

/// The small close or favourite button in a tab's accessory slot.
struct TerminalTabAccessoryButton: View {
    let accessibilityLabel: String
    let systemImage: String
    var tint: Color? = nil
    /// For toggles (the favourite star): exposed as a selected state.
    var isSelected: Bool? = nil
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Image(systemName: systemImage)
                .runeText(.micro, weight: .semibold)
                .foregroundStyle(tint ?? Color.secondary)
                .frame(
                    width: TerminalTabLayoutMetrics.accessorySlotWidth - RuneSpacing.xxs,
                    height: TerminalTabLayoutMetrics.accessorySlotWidth - RuneSpacing.xxs
                )
                .contentShape(Rectangle())
        }
        .buttonStyle(RuneCompactButtonStyle())
        .help(accessibilityLabel)
        .accessibilityLabel(accessibilityLabel)
        .accessibilityValue(isSelected.map { $0 ? "Selected" : "Not selected" } ?? "")
        .accessibilityAddTraits(isSelected == true ? .isSelected : [])
    }
}

extension View {
    func terminalTabChrome(
        isActive: Bool,
        emphasis: TerminalTabChromeEmphasis = .standard
    ) -> some View {
        modifier(TerminalTabChromeModifier(isActive: isActive, emphasis: emphasis))
    }

    func terminalTabRevealOnHover(alwaysVisible: Bool = false) -> some View {
        modifier(TerminalTabHoverReveal(alwaysVisible: alwaysVisible))
    }
}

/// A native-style tab bar for one surface: tabs share the full width (one
/// tab spans all of it), scroll only when there are too many to fit, and
/// "+" sits in its own fixed slot at the trailing end. The strip's recessed
/// background comes from the document frame it sits on.
struct TerminalTabStrip<Content: View>: View {
    let canAddTab: Bool
    let addHelp: String
    let addAccessibilityLabel: String
    var addShortcut: KeyboardShortcut? = nil
    let onAddTab: () -> Void
    @ViewBuilder let content: () -> Content
    @Environment(\.runeDocumentFrameEmbedded) private var isEmbedded

    var body: some View {
        HStack(spacing: 0) {
            // The strip reads its own width once per resize (it is one fixed-
            // height bar, not a list row) so tabs can share it exactly.
            GeometryReader { proxy in
                ScrollView(.horizontal, showsIndicators: false) {
                    RuneEqualWidthTabsLayout(availableWidth: proxy.size.width) {
                        content()
                    }
                }
            }
            .frame(maxWidth: .infinity, maxHeight: TerminalTabLayoutMetrics.tabHeight, alignment: .leading)

            TerminalTabDivider()

            Button(action: onAddTab) {
                Image(systemName: "plus")
                    .runeText(.caption, weight: .semibold)
                    .frame(width: TerminalTabLayoutMetrics.addSlotWidth, height: TerminalTabLayoutMetrics.tabHeight)
                    .contentShape(Rectangle())
            }
            .buttonStyle(RuneCompactButtonStyle())
            .disabled(!canAddTab)
            .keyboardShortcut(addShortcut)
            .help(addHelp)
            .accessibilityLabel(addAccessibilityLabel)
        }
        .frame(height: TerminalTabLayoutMetrics.tabHeight)
        .background(RuneSurfaceBackground(kind: .inset, cornerRadius: RuneUILayoutMetrics.terminalTabStripCornerRadius).opacity(isEmbedded ? 0 : 1))
    }
}
