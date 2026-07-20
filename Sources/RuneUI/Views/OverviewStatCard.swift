import SwiftUI

/// Reusable overview metric card. RootView keeps module ordering and navigation;
/// this component owns only the card's visual, loading, focus, and accessibility states.
struct OverviewStatCard: View {
    let title: String
    let count: Int
    let symbol: String
    let tint: Color
    let isLoading: Bool
    let isKeyboardFocused: Bool
    let help: String?
    let showsHoverHelp: Bool
    let onOpen: () -> Void

    @Environment(\.runeResolvedTheme) private var resolvedTheme

    init(
        title: String,
        count: Int,
        symbol: String,
        tint: Color,
        isLoading: Bool = false,
        isKeyboardFocused: Bool = false,
        help: String? = nil,
        showsHoverHelp: Bool,
        onOpen: @escaping () -> Void
    ) {
        self.title = title
        self.count = count
        self.symbol = symbol
        self.tint = tint
        self.isLoading = isLoading
        self.isKeyboardFocused = isKeyboardFocused
        self.help = help
        self.showsHoverHelp = showsHoverHelp
        self.onOpen = onOpen
    }

    var body: some View {
        Button(action: onOpen) {
            VStack(alignment: .leading, spacing: 7) {
                HStack {
                    Image(systemName: symbol)
                        .foregroundStyle(tint)
                    Spacer()
                    Image(systemName: "arrow.up.right.square")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                countContent

                Text(title)
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)
            }
            .padding(12)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(
                RuneSurfaceKind.panel.fill(theme: resolvedTheme),
                in: RoundedRectangle(
                    cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius,
                    style: .continuous
                )
            )
            .overlay {
                RoundedRectangle(
                    cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius,
                    style: .continuous
                )
                .stroke(
                    isKeyboardFocused ? Color.accentColor.opacity(0.8) : Color.clear,
                    lineWidth: 1.5
                )
            }
        }
        .buttonStyle(.plain)
        .runeHelp(help ?? "Open \(title)", enabled: showsHoverHelp)
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(title)
        .accessibilityValue(accessibilityValueText)
        .accessibilityHint(help ?? "Open \(title)")
        .accessibilityAddTraits(isKeyboardFocused ? .isSelected : [])
    }

    @ViewBuilder
    private var countContent: some View {
        if isLoading && count == 0 {
            ProgressView()
                .controlSize(.small)
                .scaleEffect(0.85)
                .padding(.vertical, 5)
                .accessibilityHidden(true)
        } else {
            HStack(spacing: 6) {
                Text("\(count)")
                    .font(.title2.weight(.bold))
                if isLoading {
                    ProgressView()
                        .controlSize(.small)
                        .scaleEffect(0.75)
                        .accessibilityHidden(true)
                }
            }
        }
    }

    var accessibilityValueText: String {
        if isLoading && count == 0 {
            return "Loading"
        }
        if isLoading {
            return "\(count), refreshing"
        }
        return "\(count)"
    }
}
