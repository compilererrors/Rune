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
    let onSelect: () -> Void
    let onToggleProduction: () -> Void
    let onToggleFavorite: () -> Void

    var body: some View {
        HStack(spacing: 8) {
            Button(action: onSelect) {
                HStack(spacing: 8) {
                    leadingIcon
                    titleStack
                    if isProduction {
                        Image(systemName: "exclamationmark.shield.fill")
                            .font(.caption)
                            .foregroundStyle(.red)
                            .help(isManuallyMarkedProduction ? "Marked as production" : "Production context detected")
                    }
                    Spacer()
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 6)
                .runeSidebarSelection(isSelected: isSelected)
            }
            .buttonStyle(.plain)
            .contextMenu {
                Button(action: onToggleProduction) {
                    Label(
                        isManuallyMarkedProduction ? "Unmark Production" : "Mark as Production",
                        systemImage: isManuallyMarkedProduction ? "shield.slash" : "exclamationmark.shield"
                    )
                }
            }

            Button(action: onToggleFavorite) {
                Image(systemName: isFavorite ? "star.fill" : "star")
                    .foregroundStyle(isFavorite ? Color.yellow : Color.gray)
            }
            .buttonStyle(.plain)
        }
    }

    @ViewBuilder
    private var leadingIcon: some View {
        if let iconName {
            Image(systemName: iconName)
                .font(.caption.weight(.semibold))
                .foregroundStyle(isSelected ? Color.accentColor : Color.secondary)
                .frame(width: 14, height: 14)
        } else {
            Circle()
                .fill(isSelected ? Color.accentColor : Color.gray.opacity(0.6))
                .frame(width: 7, height: 7)
        }
    }

    private var titleStack: some View {
        VStack(alignment: .leading, spacing: 1) {
            Text(displayName)
                .font(.body.weight(.medium))
                .lineLimit(1)
            if let secondaryLine {
                Text(secondaryLine)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
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
