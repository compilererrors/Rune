import SwiftUI
import RuneCore

struct ResourceTerminalWorkspaceView: View {
    let session: PodTerminalSession?
    let sessions: [PodTerminalSession]
    let activeSessionID: String?
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
    let onStartSession: (PodSummary) -> Void
    let onReconnectSession: (PodTerminalSession, PodSummary) -> Void
    let onStartPortForward: (PodSummary) -> Void
    let onStopPortForward: (PortForwardSession) -> Void
    let onOpenPortForwardInBrowser: (PortForwardSession) -> Void
    let onSend: () -> Void
    let onDisconnect: () -> Void
    let onSelectSession: (String) -> Void
    let onCloseSession: (String) -> Void
    let onClearTranscript: () -> Void
    @State private var isPortForwardExpanded = false
    @State private var isComposingNewShellTab = false

    private var shellPod: PodSummary? {
        pod(for: selectedShellPodID) ?? selectedPod ?? availablePods.first
    }

    private var isShowingNewShellDraft: Bool {
        isComposingNewShellTab || sessions.isEmpty
    }

    private var visibleShellSession: PodTerminalSession? {
        isShowingNewShellDraft ? nil : session
    }

    private var visibleActiveSessionID: String? {
        isShowingNewShellDraft ? nil : activeSessionID
    }

    private var portForwardPod: PodSummary? {
        pod(for: selectedPortForwardPodID) ?? selectedPod ?? availablePods.first
    }

    var body: some View {
        GeometryReader { proxy in
            ScrollView {
                VStack(alignment: .leading, spacing: 10) {
                    TerminalPortForwardPanelView(
                        isExpanded: $isPortForwardExpanded,
                        selectedPod: portForwardPod,
                        availablePods: availablePods,
                        portForwardSessions: portForwardSessions,
                        canApplyMutations: canApplyMutations,
                        selectedPortForwardPodID: $selectedPortForwardPodID,
                        localPort: $portForwardLocalPort,
                        remotePort: $portForwardRemotePort,
                        address: $portForwardAddress,
                        onStartPortForward: onStartPortForward,
                        onStopPortForward: onStopPortForward,
                        onOpenPortForwardInBrowser: onOpenPortForwardInBrowser
                    )

                    TerminalShellPanelView(
                        session: visibleShellSession,
                        sessions: sessions,
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
                        onDisconnect: onDisconnect,
                        onSelectSession: selectShellSession,
                        onCloseSession: closeShellSession,
                        onComposeNewSession: composeNewShellTab,
                        onClearTranscript: onClearTranscript
                    )
                }
                .frame(maxWidth: .infinity, alignment: .topLeading)
            }
            .id("terminal")
            .scrollContentBackground(.hidden)
            .background(Color.clear)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .onChange(of: activeSessionID) { _, newValue in
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

    private func startShellSession(_ pod: PodSummary) {
        isComposingNewShellTab = false
        onStartSession(pod)
    }

    private func reconnectShellSession(_ session: PodTerminalSession, pod: PodSummary) {
        isComposingNewShellTab = false
        onReconnectSession(session, pod)
    }

    private func selectShellSession(_ id: String) {
        isComposingNewShellTab = false
        onSelectSession(id)
    }

    private func closeShellSession(_ id: String) {
        if sessions.count <= 1 {
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
        if let selectedPod, !hasShellSession(for: selectedPod) {
            return selectedPod.id
        }
        if let available = availablePods.first(where: { !hasShellSession(for: $0) }) {
            return available.id
        }
        return selectedPod?.id ?? availablePods.first?.id ?? ""
    }

    private func hasShellSession(for pod: PodSummary) -> Bool {
        sessions.contains { $0.namespace == pod.namespace && $0.podName == pod.name }
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
            Text("Terminal")
                .font(.title2.weight(.bold))

            if let session {
                Label("\(session.namespace)/\(session.podName)", systemImage: "terminal")
                    .font(.subheadline.weight(.medium))
                Text("Shell: \(session.shell)")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                Text("Status: \(session.status.rawValue.capitalized)")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                if let exitCode = session.lastExitCode {
                    Text("Last exit code: \(exitCode)")
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
                    HStack(spacing: 8) {
                        Button {
                            onFillCommand(command)
                        } label: {
                            Text(command)
                                .font(.system(size: 12, weight: .regular, design: .monospaced))
                                .frame(maxWidth: .infinity, alignment: .leading)
                        }
                        .buttonStyle(.bordered)

                        Button {
                            onFillCommand(command)
                        } label: {
                            Image(systemName: "paperplane")
                        }
                        .buttonStyle(.bordered)
                        .help("Insert into terminal prompt")
                    }
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
                Text("Use the command buttons to prefill the prompt, then edit before sending if needed.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}
