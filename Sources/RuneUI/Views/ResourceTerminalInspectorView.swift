import SwiftUI
import RuneCore

struct TerminalShellPodSelectionPolicy {
    static func sessions(
        in sessions: [PodTerminalSession],
        contextName: String?,
        namespace: String
    ) -> [PodTerminalSession] {
        guard let contextName else { return [] }
        return sessions.filter { $0.contextName == contextName && $0.namespace == namespace }
    }

    static func preferredPodIDForNewShell(
        selectedPod: PodSummary?,
        availablePods: [PodSummary],
        sessions: [PodTerminalSession],
        currentSelectionID: String
    ) -> String {
        if let current = availablePods.first(where: { $0.id == currentSelectionID }),
           !hasShellSession(for: current, in: sessions) {
            return current.id
        }
        if let selectedPod, !hasShellSession(for: selectedPod, in: sessions) {
            return selectedPod.id
        }
        if let available = availablePods.first(where: { !hasShellSession(for: $0, in: sessions) }) {
            return available.id
        }
        return selectedPod?.id ?? availablePods.first?.id ?? ""
    }

    static func hasShellSession(for pod: PodSummary, in sessions: [PodTerminalSession]) -> Bool {
        sessions.contains { $0.namespace == pod.namespace && $0.podName == pod.name && $0.containerName == nil }
    }

    static func preferredSessionIDForHandoff(
        pod: PodSummary,
        sessions: [PodTerminalSession],
        contextName: String?,
        preferredContainer: String?
    ) -> String? {
        guard let contextName else { return nil }
        let candidates = sessions.filter {
            $0.contextName == contextName
                && $0.namespace == pod.namespace
                && $0.podName == pod.name
        }
        let container = preferredContainer?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        if let container, !container.isEmpty,
           let exact = candidates.first(where: { $0.containerName == container }) {
            return exact.id
        }
        return candidates.first(where: { $0.containerName == nil })?.id
            ?? candidates.first?.id
    }
}

struct ResourceTerminalWorkspaceView: View {
    let session: PodTerminalSession?
    let sessions: [PodTerminalSession]
    let activeSessionID: String?
    let contextName: String?
    let namespace: String
    let selectedPod: PodSummary?
    let availablePods: [PodSummary]
    let portForwardSessions: [PortForwardSession]
    let canApplyMutations: Bool
    @Binding var selectedShellPodID: String
    @Binding var selectedPortForwardPodID: String
    @Binding var terminalInput: String
    @Binding var portForwardLocalPort: String
    @Binding var portForwardRemotePort: String
    @Binding var portForwardAddress: String
    let onStartSession: (PodSummary, String?) -> Void
    let onReconnectSession: (PodTerminalSession, PodSummary, String?) -> Void
    let onStartPortForward: (PodSummary) -> Void
    let onStopPortForward: (PortForwardSession) -> Void
    let onOpenPortForwardInBrowser: (PortForwardSession) -> Void
    let onRetryPortForward: (PortForwardSession) -> Void
    let onClearPortForward: (PortForwardSession) -> Void
    let onClearInactivePortForwards: () -> Void
    let onSend: () -> Void
    let onSendControlSequence: (String) -> Void
    let onResizeSession: (String, Int, Int) -> Void
    let onDisconnect: () -> Void
    let onSelectSession: (String) -> Void
    let onCloseSession: (String) -> Void
    let onClearTranscript: () -> Void
    let onSaveActiveTerminalTranscript: () -> Void
    let onSaveAllTerminalTranscripts: () -> Void
    let onSaveActiveTerminalTranscriptToExportFolder: () -> Void
    let onSaveActiveTerminalTranscriptAndOpen: () -> Void
    let onSaveAllTerminalTranscriptsToExportFolder: () -> Void
    let onSaveAllTerminalTranscriptsAndOpen: () -> Void
    let isFavoritePod: (PodSummary) -> Bool
    let onToggleFavoritePod: (PodSummary) -> Void
    var onOpenSelectedPodLogs: ((PodSummary) -> Void)? = nil
    @State private var isPortForwardExpanded = false
    @State private var isComposingNewShellTab = false

    private var scopedSessions: [PodTerminalSession] {
        TerminalShellPodSelectionPolicy.sessions(
            in: sessions,
            contextName: contextName,
            namespace: namespace
        )
    }

    private var scopedSession: PodTerminalSession? {
        guard let session,
              session.contextName == contextName,
              session.namespace == namespace else { return nil }
        return session
    }

    private var scopedActiveSessionID: String? {
        guard let activeSessionID,
              scopedSessions.contains(where: { $0.id == activeSessionID }) else { return nil }
        return activeSessionID
    }

    private var shellPod: PodSummary? {
        pod(for: selectedShellPodID) ?? selectedPod ?? availablePods.first
    }

    private var isShowingNewShellDraft: Bool {
        isComposingNewShellTab || scopedSessions.isEmpty
    }

    private var visibleShellSession: PodTerminalSession? {
        isShowingNewShellDraft ? nil : scopedSession
    }

    private var visibleActiveSessionID: String? {
        isShowingNewShellDraft ? nil : scopedActiveSessionID
    }

    private var portForwardPod: PodSummary? {
        pod(for: selectedPortForwardPodID) ?? selectedPod ?? availablePods.first
    }

    var body: some View {
        GeometryReader { proxy in
            ScrollView {
                VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorSectionSpacing) {
                    TerminalPortForwardPanelView(
                        isExpanded: $isPortForwardExpanded,
                        contextName: contextName,
                        selectedPod: portForwardPod,
                        availablePods: availablePods,
                        portForwardSessions: portForwardSessions,
                        canApplyMutations: canApplyMutations,
                        selectedPortForwardPodID: $selectedPortForwardPodID,
                        localPort: $portForwardLocalPort,
                        remotePort: $portForwardRemotePort,
                        address: $portForwardAddress,
                        isFavoritePod: isFavoritePod,
                        onToggleFavoritePod: onToggleFavoritePod,
                        onStartPortForward: onStartPortForward,
                        onStopPortForward: onStopPortForward,
                        onOpenPortForwardInBrowser: onOpenPortForwardInBrowser,
                        onRetryPortForward: onRetryPortForward,
                        onClearPortForward: onClearPortForward,
                        onClearInactivePortForwards: onClearInactivePortForwards
                    )

                    TerminalShellPanelView(
                        session: visibleShellSession,
                        sessions: scopedSessions,
                        activeSessionID: visibleActiveSessionID,
                        isComposingNewSession: isShowingNewShellDraft,
                        selectedPod: shellPod,
                        availablePods: availablePods,
                        canApplyMutations: canApplyMutations,
                        transcriptHeight: terminalHeight(
                            availableHeight: proxy.size.height,
                            portForwardExpanded: isPortForwardExpanded
                        ),
                        selectedShellPodID: $selectedShellPodID,
                        terminalInput: $terminalInput,
                        onStartSession: startShellSession,
                        onReconnectSession: reconnectShellSession,
                        onSend: onSend,
                        onSendControlSequence: onSendControlSequence,
                        onResizeSession: onResizeSession,
                        onDisconnect: onDisconnect,
                        onSelectSession: selectShellSession,
                        onCloseSession: closeShellSession,
                        onComposeNewSession: composeNewShellTab,
                        onClearTranscript: onClearTranscript,
                        onSaveActiveTranscript: onSaveActiveTerminalTranscript,
                        onSaveAllTranscripts: onSaveAllTerminalTranscripts,
                        onSaveActiveTranscriptToExportFolder: onSaveActiveTerminalTranscriptToExportFolder,
                        onSaveActiveTranscriptAndOpen: onSaveActiveTerminalTranscriptAndOpen,
                        onSaveAllTranscriptsToExportFolder: onSaveAllTerminalTranscriptsToExportFolder,
                        onSaveAllTranscriptsAndOpen: onSaveAllTerminalTranscriptsAndOpen,
                        isFavoritePod: isFavoritePod,
                        onToggleFavoritePod: onToggleFavoritePod,
                        onOpenSelectedPodLogs: onOpenSelectedPodLogs
                    )
                }
                .frame(maxWidth: .infinity, alignment: .topLeading)
            }
            .id("terminal")
            .scrollContentBackground(.hidden)
            .background(Color.clear)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .onChange(of: scopedActiveSessionID) { _, newValue in
            if newValue != nil {
                isComposingNewShellTab = false
            }
        }
    }

    private func terminalHeight(availableHeight: CGFloat, portForwardExpanded: Bool) -> CGFloat {
        let reservedForControls: CGFloat = portForwardExpanded ? 410 : 236
        let target = availableHeight - reservedForControls
        let upperBound: CGFloat = portForwardExpanded ? 330 : 430
        return min(upperBound, max(220, target))
    }

    private func pod(for id: String) -> PodSummary? {
        availablePods.first { $0.id == id }
    }

    private func startShellSession(_ pod: PodSummary, containerName: String?) {
        isComposingNewShellTab = false
        onStartSession(pod, containerName)
    }

    private func reconnectShellSession(_ session: PodTerminalSession, pod: PodSummary, containerName: String?) {
        isComposingNewShellTab = false
        onReconnectSession(session, pod, containerName)
    }

    private func selectShellSession(_ id: String) {
        isComposingNewShellTab = false
        onSelectSession(id)
    }

    private func closeShellSession(_ id: String) {
        if scopedSessions.count <= 1 {
            isComposingNewShellTab = true
        }
        onCloseSession(id)
    }

    private func composeNewShellTab() {
        isComposingNewShellTab = true
        terminalInput = ""
        selectedShellPodID = preferredPodIDForNewShell()
    }

    private func preferredPodIDForNewShell() -> String {
        TerminalShellPodSelectionPolicy.preferredPodIDForNewShell(
            selectedPod: selectedPod,
            availablePods: availablePods,
            sessions: scopedSessions,
            currentSelectionID: selectedShellPodID
        )
    }
}

struct TerminalSessionDetailPresentation: Hashable, Sendable {
    let targetTitle: String
    let containerTitle: String?
    let shellTitle: String
    let statusTitle: String
    let lastExitCodeTitle: String?

    static func make(session: PodTerminalSession) -> TerminalSessionDetailPresentation {
        let trimmedContainer = session.containerName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return TerminalSessionDetailPresentation(
            targetTitle: "\(session.namespace)/\(session.podName)",
            containerTitle: trimmedContainer.isEmpty ? nil : trimmedContainer,
            shellTitle: session.shell,
            statusTitle: session.status.rawValue.capitalized,
            lastExitCodeTitle: session.lastExitCode.map(String.init)
        )
    }
}

struct ResourceTerminalCommonCommandRow: View {
    static let minimumHeight: CGFloat = 28

    let command: String
    let copyHelp: String
    let onInsert: (String) -> Void
    let onCopy: () -> Void

    var body: some View {
        HStack(spacing: 8) {
            Button(action: insertCommand) {
                HStack(spacing: 8) {
                    Text(command)
                        .font(.system(size: 12, weight: .regular, design: .monospaced))
                        .lineLimit(1)
                    Spacer(minLength: 8)
                    Image(systemName: "arrow.turn.down.left")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                }
                .frame(
                    maxWidth: .infinity,
                    minHeight: Self.minimumHeight,
                    alignment: .leading
                )
                .contentShape(Rectangle())
            }
            .buttonStyle(.bordered)
            .help("Insert into terminal prompt: \(command)")
            .accessibilityLabel("Insert \(command) into terminal prompt")

            Button(action: copyCommand) {
                Image(systemName: "doc.on.doc")
                    .frame(width: 14)
            }
            .buttonStyle(.bordered)
            .help(copyHelp)
            .accessibilityLabel(copyHelp)
        }
        .controlSize(.small)
    }

    func insertCommand() {
        onInsert(command)
    }

    func copyCommand() {
        onCopy()
    }
}

struct ResourceTerminalDetailsView: View {
    let session: PodTerminalSession?
    let selectedPod: PodSummary?
    let portForwardSessions: [PortForwardSession]
    let onFillCommand: (String) -> Void

    private let commonCommands = [
        "pwd",
        "printenv | sort",
        "ls -la",
        "cat /etc/os-release",
        "df -h",
        "ps -ef"
    ]

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            if let session {
                let presentation = TerminalSessionDetailPresentation.make(session: session)
                Label(presentation.targetTitle, systemImage: "terminal")
                    .font(.subheadline.weight(.medium))
                if let containerTitle = presentation.containerTitle {
                    Text("Container: \(containerTitle)")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }
                Text("Shell: \(presentation.shellTitle)")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                Text("Status: \(presentation.statusTitle)")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                if let lastExitCodeTitle = presentation.lastExitCodeTitle {
                    Text("Last exit code: \(lastExitCodeTitle)")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }
            } else if let selectedPod {
                Label("Ready for \(selectedPod.namespace)/\(selectedPod.name)", systemImage: "terminal")
                    .font(.subheadline.weight(.medium))
                Text("Start a shell session to use the terminal.")
                    .foregroundStyle(.secondary)
            } else {
                Text("Select a pod in Workloads > Pods to start an interactive shell.")
                    .foregroundStyle(.secondary)
            }

            if let active = portForwardSessions.first(where: { $0.status == .active || $0.status == .starting }) {
                Label("\(active.resourceLabel) \(active.localPort):\(active.remotePort)", systemImage: "point.3.connected.trianglepath.dotted")
                    .font(.subheadline.weight(.medium))
            }

            Divider()

            VStack(alignment: .leading, spacing: 8) {
                Text("Common Commands")
                    .font(.headline)

                ForEach(commonCommands, id: \.self) { command in
                    ResourceTerminalCommonCommandRow(
                        command: command,
                        copyHelp: session == nil ? "Copy shell command" : "Copy kubectl exec command",
                        onInsert: onFillCommand,
                        onCopy: { copySuggestedCommand(command) }
                    )
                }
            }

            Divider()

            VStack(alignment: .leading, spacing: 6) {
                Text("Notes")
                    .font(.headline)
                Text("CPU/MEM chips in the top header are cluster-level overview metrics, not pod-shell metrics.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                Text("Select a command row to prefill the prompt, then edit before sending if needed.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private func copySuggestedCommand(_ command: String) {
        guard let session else {
            TerminalKubectlCommandBuilder.copyToPasteboard(command)
            return
        }
        TerminalKubectlCommandBuilder.copyToPasteboard(
            TerminalKubectlCommandBuilder.exec(
                contextName: session.contextName,
                namespace: session.namespace,
                podName: session.podName,
                containerName: session.containerName,
                command: command
            )
        )
    }
}
