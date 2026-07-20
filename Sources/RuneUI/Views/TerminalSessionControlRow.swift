import SwiftUI
import RuneCore

struct TerminalSessionControlRow: View {
    let title: String
    let systemImage: String
    let pods: [PodSummary]
    let terminalSessions: [PodTerminalSession]
    let primaryActionTitle: String
    let primaryActionSystemImage: String
    let isPrimaryActionDisabled: Bool
    let isClearDisabled: Bool
    let isFavoritePod: (PodSummary) -> Bool
    let onToggleFavoritePod: (PodSummary) -> Void
    let onPrimaryAction: () -> Void
    let onClear: () -> Void
    @Binding var selection: String

    var body: some View {
        TerminalPodControlLayout(
            accessibilityLabel: "\(title) controls",
            title: title,
            systemImage: systemImage
        ) { width in
            TerminalPodSelectorControl(
                title: title,
                pods: pods,
                terminalSessions: terminalSessions,
                width: width,
                isFavoritePod: isFavoritePod,
                onToggleFavoritePod: onToggleFavoritePod,
                selection: $selection
            )
        } actions: {
            actionButtons
        }
    }

    private var actionButtons: some View {
        HStack(spacing: 8) {
            Button(action: onPrimaryAction) {
                Label(primaryActionTitle, systemImage: primaryActionSystemImage)
                    .lineLimit(1)
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
            .frame(width: 112)
            .disabled(isPrimaryActionDisabled)
            .help(primaryActionTitle)

            Button("Clear", action: onClear)
                .buttonStyle(.bordered)
                .controlSize(.small)
                .frame(width: 72)
                .disabled(isClearDisabled)
                .help("Clear active terminal output")
                .keyboardShortcut("k", modifiers: [.command])
        }
        .fixedSize(horizontal: true, vertical: false)
    }
}
