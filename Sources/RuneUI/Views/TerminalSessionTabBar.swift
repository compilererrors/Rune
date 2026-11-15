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

        // Titles name the pod, as native tabs name the document; the number
        // stays in the accessibility label for VoiceOver.
        return TerminalSessionTabPresentation(
            primaryTitle: session.podName,
            secondaryTitle: containerTitle ?? session.namespace,
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
    var onMoveSession: ((String, String) -> Void)? = nil
    let onComposeNewSession: () -> Void
    var onDisconnectSession: ((String) -> Void)? = nil
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        TerminalTabStrip(
            canAddTab: canApplyMutations && !isComposingNewSession,
            addHelp: "Prepare a new shell tab. Pick a pod, then connect. (⌘T)",
            addAccessibilityLabel: "New Shell",
            addShortcut: KeyboardShortcut("t", modifiers: [.command]),
            onAddTab: onComposeNewSession
        ) {
            ForEach(Array(sessions.enumerated()), id: \.element.id) { index, session in
                tab(session, number: index + 1)
            }

            if isComposingNewSession || sessions.isEmpty {
                draftTab(number: sessions.count + 1)
            }
        }
        .background {
            // Previous and next shell tab, as in Terminal and iTerm2.
            HStack(spacing: 0) {
                Button("Previous Shell Tab") { selectAdjacentSession(offset: -1) }
                    .keyboardShortcut("[", modifiers: [.command, .shift])
                Button("Next Shell Tab") { selectAdjacentSession(offset: 1) }
                    .keyboardShortcut("]", modifiers: [.command, .shift])
            }
            .frame(width: 0, height: 0)
            .opacity(0)
            .accessibilityHidden(true)
            .disabled(sessions.count < 2)
        }
    }

    func selectAdjacentSession(offset: Int) {
        guard !sessions.isEmpty else { return }
        let currentIndex = sessions.firstIndex { $0.id == activeSessionID } ?? 0
        let nextIndex = (currentIndex + offset + sessions.count) % sessions.count
        onSelectSession(sessions[nextIndex].id)
    }

    private func draftTab(number: Int) -> some View {
        Button(action: onComposeNewSession) {
            TerminalPlaceholderTabLabel(
                primaryTitle: "New Shell",
                secondaryTitle: selectedPod?.name ?? "Ready"
            )
        }
        .buttonStyle(.plain)
        .disabled(!canApplyMutations)
        .terminalTabChrome(isActive: true, emphasis: .draft)
        .help(selectedPod.map { "New shell tab for \($0.namespace)/\($0.name)" } ?? "Select a pod to connect this new shell tab")
        .accessibilityLabel("\(number) New Shell")
    }

    private func tab(_ session: PodTerminalSession, number: Int) -> some View {
        let presentation = TerminalSessionTabPresentation.make(session: session, number: number)
        return Button {
            activateSession(session)
        } label: {
            TerminalTabTitle(
                primaryTitle: presentation.primaryTitle,
                secondaryTitle: presentation.secondaryTitle,
                statusDotColor: TerminalStatusStyling.color(session.status, palette: runeThemePalette),
                trailingNote: session.status == .connected ? nil : TerminalStatusStyling.title(session.status),
                trailingNoteColor: TerminalStatusStyling.color(session.status, palette: runeThemePalette)
            )
        }
        .buttonStyle(.plain)
        .accessibilityLabel(presentation.accessibilityLabel)
        .accessibilityAddTraits(session.id == activeSessionID ? [.isSelected, .isButton] : .isButton)
        .overlay(alignment: .leading) {
            // Close sits at the leading edge on hover, as in macOS tabs.
            TerminalTabAccessoryButton(accessibilityLabel: "Close terminal tab", systemImage: "xmark") {
                onCloseSession(session.id)
            }
            .terminalTabRevealOnHover()
            .padding(.leading, RuneSpacing.hairline)
        }
        .terminalTabChrome(isActive: session.id == activeSessionID)
        .terminalTabReorderable(scope: "shell", id: session.id, onMove: onMoveSession)
        .contextMenu {
            if let onDisconnectSession, session.status == .connected || session.status == .connecting {
                Button("Disconnect") { onDisconnectSession(session.id) }
            }
            Button("Close Tab") { onCloseSession(session.id) }
        }
        .help(presentation.helpText)
        .accessibilityElement(children: .contain)
    }

    func activateSession(_ session: PodTerminalSession) {
        onSelectSession(session.id)
    }

}
