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
        HStack(spacing: 0) {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 6) {
                    if tabs.isEmpty {
                        emptyTab
                    } else {
                        ForEach(Array(tabs.enumerated()), id: \.element.id) { index, tab in
                            tabButton(tab, number: index + 1)
                        }
                    }
                }
                .padding(.horizontal, 6)
            }
            .overlay(alignment: .trailing) {
                Rectangle()
                    .fill(runeThemePalette?.divider ?? Color.primary.opacity(0.10))
                    .frame(width: 1, height: 22)
            }

            Button(action: onAddTab) {
                Image(systemName: "plus")
                    .font(.caption.weight(.semibold))
                    .frame(width: 38, height: 28)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .controlSize(.small)
            .disabled(!canAddTab)
            .background(RuneSurfaceBackground(kind: .listRow(isSelected: false)))
            .overlay(tabBorder(isActive: false))
            .padding(.leading, 6)
            .help("Open another pod log tab")
            .accessibilityLabel("New Log Tab")
        }
        .frame(height: 38)
        .padding(.horizontal, 6)
        .background(RuneSurfaceBackground(kind: .inset))
    }

    private var emptyTab: some View {
        Button(action: onAddTab) {
            HStack(spacing: 6) {
                Image(systemName: "plus.circle.fill")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(runeThemePalette?.accent ?? Color.accentColor)
                Text("New Logs")
                    .font(.caption.weight(.semibold))
                    .lineLimit(1)
                Text("Ready")
                    .font(.caption2.weight(.semibold))
                    .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
            }
            .frame(width: 216, height: 28, alignment: .leading)
            .padding(.horizontal, 8)
            .contentShape(RoundedRectangle(cornerRadius: 7, style: .continuous))
        }
        .buttonStyle(.plain)
        .disabled(!canAddTab)
        .background(tabBackground(isActive: true))
        .overlay(tabBorder(isActive: true))
        .overlay(alignment: .leading) {
            Capsule()
                .fill(runeThemePalette?.accent ?? Color.accentColor)
                .frame(width: 3, height: 16)
                .padding(.leading, 3)
        }
        .help("Open a pod log tab")
    }

    private func tabButton(_ tab: TerminalLogTabPresentation, number: Int) -> some View {
        ZStack(alignment: .trailing) {
            HStack(spacing: 6) {
                Button {
                    onToggleFavoriteTab(tab.id)
                } label: {
                    Image(systemName: tab.isFavorite ? "star.fill" : "star")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(tab.isFavorite ? Color.yellow : Color.secondary)
                        .frame(width: 20, height: 24)
                        .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .help(tab.isFavorite ? "Remove log target favorite" : "Favorite log target")
                .accessibilityLabel(tab.isFavorite ? "Remove Log Target Favorite" : "Favorite Log Target")
                Text("\(number) \(tab.title)")
                    .font(.caption.weight(.semibold))
                    .lineLimit(1)
                    .help(tab.helpText)
                if let subtitle = tab.subtitle {
                    Text(subtitle)
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                        .lineLimit(1)
                        .help(tab.helpText)
                }
                Spacer(minLength: 0)
            }
            .padding(.leading, 8)
            .padding(.trailing, 28)

            Button {
                onCloseTab(tab.id)
            } label: {
                Image(systemName: "xmark")
                    .font(.caption2.weight(.bold))
                    .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                    .frame(width: 28, height: 28)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .help("Close log tab for \(tab.title)")
        }
        .frame(width: 216, height: 28, alignment: .leading)
        .contentShape(RoundedRectangle(cornerRadius: 7, style: .continuous))
        .onTapGesture {
            onSelectTab(tab.id)
        }
        .background(tabBackground(isActive: tab.id == activeTabID))
        .overlay(tabBorder(isActive: tab.id == activeTabID))
        .overlay(alignment: .leading) {
            if tab.id == activeTabID {
                Capsule()
                    .fill(runeThemePalette?.accent ?? Color.accentColor)
                    .frame(width: 3, height: 16)
                    .padding(.leading, 3)
            }
        }
        .help(tab.helpText)
        .accessibilityElement(children: .combine)
        .accessibilityLabel(tab.accessibilityLabel)
        .accessibilityAddTraits(tab.id == activeTabID ? [.isSelected, .isButton] : .isButton)
    }

    private func tabBackground(isActive: Bool) -> some View {
        RuneSurfaceBackground(kind: .listRow(isSelected: isActive))
            .clipShape(RoundedRectangle(cornerRadius: 7, style: .continuous))
    }

    private func tabBorder(isActive: Bool) -> some View {
        RoundedRectangle(cornerRadius: 7, style: .continuous)
            .stroke(
                isActive
                    ? (runeThemePalette?.selectionStroke.opacity(0.55) ?? Color.accentColor.opacity(0.42))
                    : (runeThemePalette?.stroke.opacity(0.26) ?? Color.primary.opacity(0.12)),
                lineWidth: 1
            )
    }
}
