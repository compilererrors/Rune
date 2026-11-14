import SwiftUI

struct ContextSidebarRow: View {
    let displayName: String
    let rawName: String
    let secondaryText: String?
    let iconName: String?
    let isSelected: Bool
    let isFavorite: Bool
    let isProduction: Bool
    let isManuallyMarkedProduction: Bool
    let canRemove: Bool
    let onSelect: () -> Void
    let onToggleProduction: () -> Void
    let onToggleFavorite: () -> Void
    let onRemove: () -> Void
    @Environment(\.runeThemePalette) private var runeThemePalette
    @Environment(\.runeInterfaceFontSize) private var interfaceFontSize

    var body: some View {
        HStack(spacing: RuneSpacing.xs) {
            Button(action: onSelect) {
                HStack(spacing: RuneSpacing.xs) {
                    leadingIcon
                    titleStack
                    if isProduction {
                        Image(systemName: "exclamationmark.shield.fill")
                            .runeText(.micro)
                            .foregroundStyle(RuneSemanticColorRole.danger.color(in: runeThemePalette))
                            .help(isManuallyMarkedProduction ? "Marked as production" : "Production context detected")
                    }
                    Spacer()
                }
                .padding(.horizontal, RuneSpacing.xs)
                .padding(.vertical, RuneSpacing.xxs)
                .frame(minHeight: RuneControlHeight.regular)
                .contentShape(Rectangle())
                .runeSidebarSelection(isSelected: isSelected)
            }
            .buttonStyle(.plain)
            .accessibilityIdentifier("rune.context.\(rawName)")
            .accessibilityValue(isSelected ? "Selected" : "Not selected")
            .contextMenu {
                Button(action: onToggleProduction) {
                    Label(
                        isManuallyMarkedProduction ? "Unmark Production" : "Mark as Production",
                        systemImage: isManuallyMarkedProduction ? "shield.slash" : "exclamationmark.shield"
                    )
                }
                if canRemove {
                    Divider()
                    Button(role: .destructive, action: onRemove) {
                        Label("Remove from Rune…", systemImage: "minus.circle")
                    }
                    .accessibilityIdentifier("rune.context.remove.\(rawName)")
                }
            }

            RuneIconButton(
                isFavorite ? "Remove context from favorites" : "Add context to favorites",
                systemImage: isFavorite ? "star.fill" : "star",
                isSelected: isFavorite,
                selectedTint: .yellow,
                action: onToggleFavorite
            )
        }
    }

    @ViewBuilder
    private var leadingIcon: some View {
        if let iconName {
            Image(systemName: iconName)
                .runeText(.microEmphasis)
                .foregroundStyle(isSelected ? RuneTextStyle.accent : .secondary)
                .frame(width: 18, height: 14)
        } else {
            RuneStatusDot(isSelected ? (runeThemePalette?.accent ?? Color.accentColor) : (runeThemePalette?.mutedText ?? Color.secondary))
                .frame(width: 18)
        }
    }

    private var titleStack: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
            Text(displayName)
                .runeInterfaceFont(weight: isSelected ? .semibold : .medium)
                .lineLimit(1)
            if let secondaryLine {
                Text(secondaryLine)
                    .runeText(.micro)
                    .foregroundStyle(.runeSecondary)
                    .lineLimit(1)
            }
        }
        .help(displayName == rawName ? rawName : "\(displayName) (\(rawName))")
    }

    private var secondaryLine: String? {
        guard let secondaryText else { return nil }
        return displayName == rawName ? secondaryText : "\(rawName) • \(secondaryText)"
    }
}
