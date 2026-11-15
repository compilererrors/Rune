import SwiftUI
import RuneCore

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
    var onMoveTab: ((String, String) -> Void)? = nil
    @AppStorage(RuneSettingsKeys.tabPinSymbol) private var pinSymbolRaw = RuneTabPinSymbol.pin.rawValue
    @Environment(\.runeThemePalette) private var runeThemePalette

    private var pinSymbol: RuneTabPinSymbol {
        RuneTabPinSymbol(rawValue: pinSymbolRaw) ?? .pin
    }

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
        Button {
            onSelectTab(tab.id)
        } label: {
            TerminalTabTitle(primaryTitle: tab.title, secondaryTitle: tab.subtitle)
        }
        .buttonStyle(.plain)
        .help(tab.helpText)
        .accessibilityLabel(tab.accessibilityLabel)
        .accessibilityAddTraits(tab.id == activeTabID ? [.isSelected, .isButton] : .isButton)
        .overlay {
            HStack(spacing: 0) {
                // Close at the leading edge, pin at the trailing edge, both
                // on hover; a pinned tab keeps its mark and sits first.
                TerminalTabAccessoryButton(accessibilityLabel: "Close log tab for \(tab.title)", systemImage: "xmark") {
                    onCloseTab(tab.id)
                }
                .terminalTabRevealOnHover()
                Spacer(minLength: 0)
                TerminalTabAccessoryButton(
                    accessibilityLabel: tab.isFavorite ? "Unpin Tab" : "Pin Tab",
                    systemImage: pinSymbol.systemImage(isPinned: tab.isFavorite),
                    tint: tab.isFavorite && pinSymbol == .star ? .yellow : nil,
                    isSelected: tab.isFavorite
                ) {
                    onToggleFavoriteTab(tab.id)
                }
                .terminalTabRevealOnHover(alwaysVisible: tab.isFavorite)
            }
            .padding(.horizontal, RuneSpacing.hairline)
        }
        .terminalTabChrome(isActive: tab.id == activeTabID)
        .terminalTabReorderable(scope: "logs", id: tab.id, onMove: onMoveTab)
        .contextMenu {
            Button(tab.isFavorite ? "Unpin Tab" : "Pin Tab") { onToggleFavoriteTab(tab.id) }
            Button("Close Tab") { onCloseTab(tab.id) }
        }
        .accessibilityElement(children: .contain)
    }
}
