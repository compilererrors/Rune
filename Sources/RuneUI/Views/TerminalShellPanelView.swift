import SwiftUI
import RuneCore

struct TerminalShellPanelView: View {
    let session: PodTerminalSession?
    let sessions: [PodTerminalSession]
    let activeSessionID: String?
    let isComposingNewSession: Bool
    let selectedPod: PodSummary?
    let availablePods: [PodSummary]
    let canApplyMutations: Bool
    let transcriptHeight: CGFloat
    @Binding var selectedShellPodID: String
    @Binding var terminalInput: String
    let onStartSession: (PodSummary) -> Void
    let onReconnectSession: (PodTerminalSession, PodSummary) -> Void
    let onSend: () -> Void
    let onDisconnect: () -> Void
    let onSelectSession: (String) -> Void
    let onCloseSession: (String) -> Void
    let onComposeNewSession: () -> Void
    let onClearTranscript: () -> Void
    @AppStorage(RuneSettingsKeys.terminalFontSize) private var storedTerminalFontSize = RuneSettingsKeys.terminalFontSizeDefault

    private var terminalFontSize: CGFloat {
        CGFloat(RuneSettingsKeys.clampedTerminalFontSize(storedTerminalFontSize))
    }

    private var activeTabLabel: String? {
        guard let session else { return nil }
        var parts = [session.namespace, "shell \(session.shell)"]
        if let exitCode = session.lastExitCode {
            parts.append("exit \(exitCode)")
        }
        return parts.joined(separator: " - ")
    }

    private var canSendInput: Bool {
        session?.status == .connected
    }

    private var canStopActiveSession: Bool {
        session?.status == .connected || session?.status == .connecting
    }

    private var selectedPodExistingSession: PodTerminalSession? {
        guard session == nil, let selectedPod else { return nil }
        return sessions.first { $0.namespace == selectedPod.namespace && $0.podName == selectedPod.name }
    }

    private var canOpenSession: Bool {
        session == nil || session?.status == .disconnected || session?.status == .failed
    }

    private var primaryActionTitle: String {
        guard let session else {
            return selectedPodExistingSession == nil ? "Connect" : "Open Tab"
        }
        switch session.status {
        case .connecting:
            return "Cancel"
        case .connected:
            return "Disconnect"
        case .disconnected, .failed:
            return "Reconnect"
        }
    }

    private var primaryActionSystemImage: String {
        guard let session else {
            return selectedPodExistingSession == nil ? "play.fill" : "arrowshape.turn.up.right"
        }
        switch session.status {
        case .connecting:
            return "xmark"
        case .connected:
            return "stop.fill"
        case .disconnected, .failed:
            return "arrow.clockwise"
        }
    }

    private var primaryActionDisabled: Bool {
        if canStopActiveSession {
            return false
        }
        if selectedPodExistingSession != nil {
            return false
        }
        return selectedPod == nil || !canApplyMutations || !canOpenSession
    }

    private var transcriptPlaceholder: String {
        if selectedPod == nil && session == nil {
            return "Select a pod in this namespace, then connect the new shell tab."
        }
        if isComposingNewSession { return "New shell tab. Choose a pod and connect." }
        return "No shell session yet."
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            header

            TerminalSessionTabBar(
                sessions: sessions,
                activeSessionID: activeSessionID,
                isComposingNewSession: isComposingNewSession,
                selectedPod: selectedPod,
                canApplyMutations: canApplyMutations,
                selectedShellPodID: $selectedShellPodID,
                onStartSession: onStartSession,
                onSelectSession: onSelectSession,
                onCloseSession: onCloseSession,
                onComposeNewSession: onComposeNewSession
            )

            TerminalSessionControlRow(
                title: "Session",
                systemImage: "terminal",
                pods: availablePods,
                terminalSessions: sessions,
                primaryActionTitle: primaryActionTitle,
                primaryActionSystemImage: primaryActionSystemImage,
                isPrimaryActionDisabled: primaryActionDisabled,
                isClearDisabled: session?.transcript.isEmpty ?? true,
                onPrimaryAction: performPrimaryAction,
                onClear: onClearTranscript,
                selection: $selectedShellPodID
            )

            TerminalTranscriptSurface(
                text: session?.transcript.isEmpty == false ? session?.transcript ?? "" : transcriptPlaceholder,
                height: transcriptHeight,
                resetID: "terminal:\(session?.id ?? "empty")",
                fontSize: terminalFontSize
            )

            inputRow
        }
        .runePanelCard(padding: RuneUILayoutMetrics.paneInnerPadding)
        .onAppear(perform: syncShellPodSelectionToActiveSession)
        .onChange(of: activeSessionID) { _, _ in
            syncShellPodSelectionToActiveSession()
        }
        .onChange(of: selectedShellPodID) { _, newValue in
            handleShellPodSelectionChange(newValue)
        }
    }

    private var header: some View {
        titleBlock
    }

    private var titleBlock: some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack(spacing: 8) {
                Label("Pod Shell", systemImage: "terminal")
                    .font(.headline)
                if let session {
                    statusBadge(session.status)
                } else if isComposingNewSession {
                    draftBadge
                }
            }

            if let activeTabLabel {
                Text(activeTabLabel)
                    .font(.footnote.weight(.semibold))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .help(activeTabLabel)
            } else {
                Text(isComposingNewSession ? "New shell tab" : "No active shell tab")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var inputRow: some View {
        HStack(spacing: 8) {
            Text("$")
                .font(.system(size: terminalFontSize, weight: .semibold, design: .monospaced))
                .foregroundStyle(canSendInput ? Color.accentColor : .secondary)

            TextField("Type a shell command and press Return", text: $terminalInput)
                .textFieldStyle(.roundedBorder)
                .font(.system(size: terminalFontSize, weight: .regular, design: .monospaced))
                .onSubmit(onSend)
                .disabled(!canSendInput)

            Button("Send") {
                onSend()
            }
            .disabled(!canSendInput || terminalInput.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        }
        .controlSize(.small)
    }

    private func statusBadge(_ status: PodTerminalSessionStatus) -> some View {
        Text(TerminalStatusStyling.title(status))
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(TerminalStatusStyling.color(status).opacity(0.16), in: Capsule())
            .foregroundStyle(TerminalStatusStyling.color(status))
    }

    private var draftBadge: some View {
        Text("Ready")
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(Color.accentColor.opacity(0.14), in: Capsule())
            .foregroundStyle(Color.accentColor)
    }

    private func syncShellPodSelectionToActiveSession() {
        guard !isComposingNewSession else { return }
        guard let session else { return }
        let id = "\(session.namespace)/\(session.podName)"
        if availablePods.contains(where: { $0.id == id }) {
            selectedShellPodID = id
        }
    }

    private func handleShellPodSelectionChange(_ podID: String) {
        guard !podID.isEmpty else { return }
        if let session, podID == "\(session.namespace)/\(session.podName)" {
            return
        }
        if let matchingSession = sessions.first(where: { "\($0.namespace)/\($0.podName)" == podID }) {
            onSelectSession(matchingSession.id)
        } else {
            onComposeNewSession()
        }
    }

    private func performPrimaryAction() {
        if canStopActiveSession {
            onDisconnect()
            return
        }
        guard let selectedPod else { return }
        if let existing = selectedPodExistingSession {
            onSelectSession(existing.id)
            return
        }
        if let session, session.status == .disconnected || session.status == .failed {
            onReconnectSession(session, selectedPod)
        } else {
            onStartSession(selectedPod)
        }
    }

}
