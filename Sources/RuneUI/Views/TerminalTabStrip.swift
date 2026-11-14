import SwiftUI

enum TerminalTabChromeEmphasis {
    case standard
    case draft
}

enum TerminalTabLayoutMetrics {
    static let tabWidth: CGFloat = 216
    static let tabHeight: CGFloat = 28
    static let placeholderHorizontalPadding: CGFloat = 8
}

/// Shared label for empty and draft tabs. Padding is constrained inside the
/// fixed tab frame so placeholders never grow wider than established tabs.
struct TerminalPlaceholderTabLabel: View {
    let primaryTitle: String
    let secondaryTitle: String
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        HStack(spacing: RuneSpacing.tight) {
            Image(systemName: "plus.circle.fill")
                .runeText(.microEmphasis)
                .foregroundStyle(runeThemePalette?.accent ?? Color.accentColor)
            Text(primaryTitle)
                .runeText(.microEmphasis)
                .lineLimit(1)
            Text(secondaryTitle)
                .runeText(.microEmphasis)
                .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                .lineLimit(1)
        }
        .padding(.horizontal, TerminalTabLayoutMetrics.placeholderHorizontalPadding)
        .frame(
            width: TerminalTabLayoutMetrics.tabWidth,
            height: TerminalTabLayoutMetrics.tabHeight,
            alignment: .leading
        )
        .contentShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.tabCornerRadius, style: .continuous))
    }
}

struct TerminalTabChromeModifier: ViewModifier {
    let isActive: Bool
    let emphasis: TerminalTabChromeEmphasis
    @Environment(\.runeThemePalette) private var runeThemePalette

    func body(content: Content) -> some View {
        content
            .background {
                RuneSurfaceBackground(kind: .listRow(isSelected: isActive))
                    .clipShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.tabCornerRadius, style: .continuous))
            }
            .overlay {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.tabCornerRadius, style: .continuous)
                    .stroke(borderColor, lineWidth: 1)
            }
            .overlay(alignment: .leading) {
                if isActive {
                    Capsule()
                        .fill(runeThemePalette?.accent ?? Color.accentColor)
                        .frame(width: 3, height: 16)
                        .padding(.leading, RuneSpacing.xxs)
                }
            }
    }

    private var borderColor: Color {
        if isActive {
            let paletteOpacity = emphasis == .draft ? 0.58 : 0.55
            let fallbackOpacity = emphasis == .draft ? 0.48 : 0.42
            return runeThemePalette?.selectionStroke.opacity(paletteOpacity)
                ?? Color.accentColor.opacity(fallbackOpacity)
        }
        return runeThemePalette?.stroke.opacity(0.26) ?? Color.primary.opacity(0.12)
    }
}

extension View {
    func terminalTabChrome(
        isActive: Bool,
        emphasis: TerminalTabChromeEmphasis = .standard
    ) -> some View {
        modifier(TerminalTabChromeModifier(isActive: isActive, emphasis: emphasis))
    }
}

struct TerminalTabStrip<Content: View>: View {
    let canAddTab: Bool
    let addHelp: String
    let addAccessibilityLabel: String
    let onAddTab: () -> Void
    @ViewBuilder let content: () -> Content
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        HStack(spacing: 0) {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: RuneSpacing.tight) {
                    content()
                }
                .padding(.horizontal, RuneSpacing.tight)
            }
            .overlay(alignment: .trailing) {
                Rectangle()
                    .fill(runeThemePalette?.divider ?? Color.primary.opacity(0.10))
                    .frame(width: 1, height: 22)
            }

            Button(action: onAddTab) {
                Image(systemName: "plus")
                    .runeText(.microEmphasis)
                    .frame(width: 38, height: 28)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .controlSize(.small)
            .disabled(!canAddTab)
            .terminalTabChrome(isActive: false)
            .padding(.leading, RuneSpacing.tight)
            .help(addHelp)
            .accessibilityLabel(addAccessibilityLabel)
        }
        .frame(height: 38)
        .padding(.horizontal, RuneSpacing.tight)
        .background(RuneSurfaceBackground(kind: .inset))
    }
}
