import SwiftUI

struct TerminalLogTabPresentation: Identifiable, Hashable, Sendable {
    let id: String
    let podID: String
    let title: String
    let subtitle: String?
    let isFavorite: Bool
    let accessibilityLabel: String
    let helpText: String
}

struct TerminalLogTabBar: View {
    let tabs: [TerminalLogTabPresentation]
    let activeTabID: String?
    let canAddTab: Bool
    let onSelectTab: (String) -> Void
    let onCloseTab: (String) -> Void
    let onToggleFavoriteTab: (String) -> Void
    let onAddTab: () -> Void
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        TerminalTabStrip(
            canAddTab: canAddTab,
            addHelp: "Open another pod log tab",
            addAccessibilityLabel: "New Log Tab",
            onAddTab: onAddTab
        ) {
            if tabs.isEmpty {
                emptyTab
            } else {
                ForEach(Array(tabs.enumerated()), id: \.element.id) { index, tab in
                    tabButton(tab, number: index + 1)
                }
            }
        }
    }

    private var emptyTab: some View {
        Button(action: onAddTab) {
            TerminalPlaceholderTabLabel(
                primaryTitle: "New Logs",
                secondaryTitle: "Ready"
            )
        }
        .buttonStyle(.plain)
        .disabled(!canAddTab)
        .terminalTabChrome(isActive: true)
        .help("Open a pod log tab")
    }

    private func tabButton(_ tab: TerminalLogTabPresentation, number: Int) -> some View {
        ZStack {
            Button {
                onSelectTab(tab.id)
            } label: {
                HStack(spacing: 6) {
                    Text("\(number) \(tab.title)")
                        .font(.caption.weight(.semibold))
                        .lineLimit(1)
                    if let subtitle = tab.subtitle {
                        Text(subtitle)
                            .font(.caption2.weight(.semibold))
                            .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                            .lineLimit(1)
                    }
                    Spacer(minLength: 0)
                }
                .padding(.leading, 40)
                .padding(.trailing, 28)
                .frame(width: 216, height: 28, alignment: .leading)
                .contentShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.tabCornerRadius, style: .continuous))
            }
            .buttonStyle(.plain)
            .help(tab.helpText)
            .accessibilityLabel(tab.accessibilityLabel)
            .accessibilityAddTraits(tab.id == activeTabID ? [.isSelected, .isButton] : .isButton)

            HStack(spacing: 0) {
                RuneIconButton(
                    tab.isFavorite ? "Remove Log Target Favorite" : "Favorite Log Target",
                    systemImage: tab.isFavorite ? "star.fill" : "star",
                    help: tab.isFavorite ? "Remove log target favorite" : "Favorite log target",
                    isSelected: tab.isFavorite,
                    selectedTint: .yellow
                ) {
                    onToggleFavoriteTab(tab.id)
                }
                .padding(.leading, 4)
                Spacer(minLength: 0)

                RuneIconButton(
                    "Close log tab for \(tab.title)",
                    systemImage: "xmark"
                ) {
                    onCloseTab(tab.id)
                }
            }
            .frame(width: 216, height: 28)
        }
        .frame(width: 216, height: 28, alignment: .leading)
        .terminalTabChrome(isActive: tab.id == activeTabID)
        .help(tab.helpText)
        .accessibilityElement(children: .contain)
    }
}
