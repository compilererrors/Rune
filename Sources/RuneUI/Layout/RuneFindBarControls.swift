import SwiftUI

enum RuneFindBarMetrics {
    static let supportedInspectorWidth: CGFloat = 320
    static let minimumSearchFieldWidth: CGFloat = 160
    static let idealSearchFieldWidth: CGFloat = 240
}

/// Shared find-bar chrome. Indexing and navigation remain owned by each
/// surface, while the controls use the same wide and compact layout policy.
struct RuneFindBarChrome<Primary: View, Secondary: View>: View {
    let accessibilityLabel: String
    @ViewBuilder let primary: Primary
    @ViewBuilder let secondary: Secondary

    init(
        _ accessibilityLabel: String,
        @ViewBuilder primary: () -> Primary,
        @ViewBuilder secondary: () -> Secondary
    ) {
        self.accessibilityLabel = accessibilityLabel
        self.primary = primary()
        self.secondary = secondary()
    }

    var body: some View {
        RuneAdaptiveToolbar(accessibilityLabel) {
            primary
        } secondary: {
            secondary
        }
        .controlSize(.small)
        .padding(.horizontal, RuneUILayoutMetrics.inspectorControlContentInset)
        .padding(.vertical, RuneUILayoutMetrics.inspectorControlChromeVerticalPadding)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
        .overlay {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .stroke(Color.primary.opacity(0.16), lineWidth: 1)
        }
        .shadow(color: .black.opacity(0.16), radius: 10, x: 0, y: 5)
        .contentShape(Rectangle())
        .runePointerCursor()
    }
}

struct RuneMatchCaseButton: View {
    @Binding var isSelected: Bool
    let helpText: String
    @Environment(\.runeThemePalette) private var runeThemePalette

    init(isSelected: Binding<Bool>, help: String = "Match case") {
        _isSelected = isSelected
        helpText = help
    }

    var body: some View {
        Button {
            isSelected.toggle()
        } label: {
            Text("Aa")
                .font(.caption.weight(.semibold))
                .frame(
                    width: RuneUILayoutMetrics.iconButtonSize,
                    height: RuneUILayoutMetrics.iconButtonSize
                )
                .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .foregroundStyle(
            isSelected
                ? (runeThemePalette?.accent ?? Color.accentColor)
                : (runeThemePalette?.secondaryText ?? Color.secondary)
        )
        .background {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                .fill(
                    isSelected
                        ? (runeThemePalette?.accent ?? Color.accentColor).opacity(0.22)
                        : Color.clear
                )
        }
        .help(helpText)
        .accessibilityLabel(helpText)
        .accessibilityValue(isSelected ? "Selected" : "Not selected")
        .accessibilityAddTraits(isSelected ? .isSelected : [])
    }
}
