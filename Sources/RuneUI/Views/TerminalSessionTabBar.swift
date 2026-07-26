import SwiftUI
import RuneCore

struct TerminalSessionTabPresentation: Hashable, Sendable {
    let primaryTitle: String
    let secondaryTitle: String?
    let accessibilityLabel: String
    let helpText: String

    static func make(session: PodTerminalSession, number: Int) -> TerminalSessionTabPresentation {
        let trimmedContainer = session.containerName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let containerTitle = trimmedContainer.isEmpty ? nil : trimmedContainer
        let statusTitle = TerminalStatusStyling.title(session.status)
        var helpParts = [
            "\(session.namespace)/\(session.podName)",
            "Shell: \(session.shell)",
            "Status: \(statusTitle)"
        ]
        if let containerTitle {
            helpParts.insert("Container: \(containerTitle)", at: 1)
        }
        if let exitCode = session.lastExitCode {
            helpParts.append("Last exit code: \(exitCode)")
        }

        let accessibility = [
            "\(number) \(session.podName)",
            containerTitle.map { "container \($0)" },
            statusTitle
        ].compactMap { $0 }.joined(separator: ", ")

        return TerminalSessionTabPresentation(
            primaryTitle: "\(number) \(session.podName)",
            secondaryTitle: containerTitle,
            accessibilityLabel: accessibility,
            helpText: helpParts.joined(separator: " - ")
        )
    }
}

struct TerminalSessionTabBar: View {
    let sessions: [PodTerminalSession]
    let activeSessionID: String?
    let isComposingNewSession: Bool
    let selectedPod: PodSummary?
    let canApplyMutations: Bool
    let onSelectSession: (String) -> Void
    let onCloseSession: (String) -> Void
    let onComposeNewSession: () -> Void
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        TerminalTabStrip(
            canAddTab: canApplyMutations && !isComposingNewSession,
            addHelp: "Prepare a new shell tab. Pick a pod, then connect.",
            addAccessibilityLabel: "New Shell",
            onAddTab: onComposeNewSession
        ) {
            ForEach(Array(sessions.enumerated()), id: \.element.id) { index, session in
                tab(session, number: index + 1)
            }

            if isComposingNewSession || sessions.isEmpty {
                draftTab(number: sessions.count + 1)
            }
        }
    }

    private func draftTab(number: Int) -> some View {
        Button(action: onComposeNewSession) {
            TerminalPlaceholderTabLabel(
                primaryTitle: "\(number) New Shell",
                secondaryTitle: selectedPod?.name ?? "Ready"
            )
        }
        .buttonStyle(.plain)
        .disabled(!canApplyMutations)
        .terminalTabChrome(isActive: true, emphasis: .draft)
        .help(selectedPod.map { "New shell tab for \($0.namespace)/\($0.name)" } ?? "Select a pod to connect this new shell tab")
    }

    private func tab(_ session: PodTerminalSession, number: Int) -> some View {
        let presentation = TerminalSessionTabPresentation.make(session: session, number: number)
        return ZStack {
            Button {
                activateSession(session)
            } label: {
                HStack(spacing: 6) {
                    TerminalStatusDot(
                        color: TerminalStatusStyling.color(session.status, palette: runeThemePalette)
                    )
                    Text(presentation.primaryTitle)
                        .font(.caption.weight(.semibold))
                        .lineLimit(1)
                    if let secondaryTitle = presentation.secondaryTitle {
                        Text(secondaryTitle)
                            .font(.caption2.weight(.semibold))
                            .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                            .lineLimit(1)
                    }
                    if session.status != .connected {
                        Text(TerminalStatusStyling.title(session.status))
                            .font(.caption2.weight(.semibold))
                            .foregroundStyle(
                                TerminalStatusStyling.color(session.status, palette: runeThemePalette)
                            )
                            .lineLimit(1)
                    }
                    Spacer(minLength: 0)
                }
                .padding(.leading, 8)
                .padding(.trailing, 28)
                .frame(width: 216, height: 28, alignment: .leading)
                .contentShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.tabCornerRadius, style: .continuous))
            }
            .buttonStyle(.plain)
            .help(presentation.helpText)
            .accessibilityLabel(presentation.accessibilityLabel)
            .accessibilityAddTraits(session.id == activeSessionID ? [.isSelected, .isButton] : .isButton)

            HStack(spacing: 0) {
                Spacer(minLength: 0)
                RuneIconButton("Close terminal tab", systemImage: "xmark") {
                    onCloseSession(session.id)
                }
            }
            .frame(width: 216, height: 28, alignment: .trailing)
        }
        .frame(width: 216, height: 28, alignment: .leading)
        .terminalTabChrome(isActive: session.id == activeSessionID)
        .help(presentation.helpText)
        .accessibilityElement(children: .contain)
    }

    func activateSession(_ session: PodTerminalSession) {
        onSelectSession(session.id)
    }

}
