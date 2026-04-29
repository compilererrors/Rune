import AppKit
import SwiftUI
import RuneCore

struct ResourceTerminalWorkspaceView: View {
    let session: PodTerminalSession?
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
    let onStartPortForward: (PodSummary) -> Void
    let onStopPortForward: (PortForwardSession) -> Void
    let onOpenPortForwardInBrowser: (PortForwardSession) -> Void
    let onSend: () -> Void
    let onDisconnect: () -> Void
    let onClearTranscript: () -> Void
    @State private var isPortForwardExpanded = true

    private var shellPod: PodSummary? {
        pod(for: selectedShellPodID) ?? selectedPod ?? availablePods.first
    }

    private var portForwardPod: PodSummary? {
        pod(for: selectedPortForwardPodID) ?? selectedPod ?? availablePods.first
    }

    private var activeOrStartingPortForwardSessions: [PortForwardSession] {
        portForwardSessions.filter { $0.status == .active || $0.status == .starting }
    }

    private var primaryPortForwardSession: PortForwardSession? {
        activeOrStartingPortForwardSessions.first ?? portForwardSessions.first
    }

    private var selectedStoppablePortForwardSession: PortForwardSession? {
        guard let portForwardPod else { return nil }
        return portForwardSessions.first {
            $0.targetKind == .pod
                && $0.targetName == portForwardPod.name
                && $0.namespace == portForwardPod.namespace
                && ($0.status == .starting || $0.status == .active || $0.status == .failed)
        }
    }

    private var targetPodLabel: String? {
        if let session {
            return "\(session.namespace)/\(session.podName)"
        }
        if let shellPod {
            return "\(shellPod.namespace)/\(shellPod.name)"
        }
        return nil
    }

    private var canSendInput: Bool {
        session?.status == .connected
    }

    private var transcriptPlaceholder: String {
        if shellPod == nil && session == nil {
            return "Select a pod in this namespace, then start an interactive shell."
        }
        return "No shell session yet."
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                portForwardCard
                terminalCard
            }
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .id("terminal")
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var terminalCard: some View {
        VStack(alignment: .leading, spacing: 12) {
            ViewThatFits(in: .horizontal) {
                HStack(alignment: .center, spacing: 10) {
                    terminalTitleBlock
                    Spacer(minLength: 12)
                    terminalActions
                }

                VStack(alignment: .leading, spacing: 10) {
                    terminalTitleBlock
                    terminalActions
                }
            }

            TerminalPodSelectorRow(
                title: "Shell pod",
                systemImage: "terminal",
                pods: availablePods,
                selection: $selectedShellPodID
            )

            TerminalTranscriptSurface(
                text: session?.transcript.isEmpty == false ? session?.transcript ?? "" : transcriptPlaceholder,
                minHeight: 460,
                resetID: "terminal:\(session?.id ?? "empty")"
            )

            terminalInputRow
        }
        .runePanelCard(padding: RuneUILayoutMetrics.paneInnerPadding)
    }

    private var terminalTitleBlock: some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack(spacing: 8) {
                Label("Pod Shell", systemImage: "terminal")
                    .font(.headline)
                if let session {
                    terminalStatusBadge(session.status)
                }
            }

            if let targetPodLabel {
                Text(targetPodLabel)
                    .font(.footnote.weight(.semibold))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .help(targetPodLabel)
            } else {
                Text("No pod selected")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var terminalActions: some View {
        HStack(spacing: 8) {
            Button(session == nil ? "Connect Shell" : "Reconnect") {
                if let shellPod {
                    onStartSession(shellPod)
                }
            }
            .disabled(shellPod == nil || !canApplyMutations)

            Button("Disconnect") {
                onDisconnect()
            }
            .disabled(session == nil)

            Button("Clear") {
                onClearTranscript()
            }
            .disabled(session?.transcript.isEmpty ?? true)
        }
        .controlSize(.small)
    }

    private var terminalInputRow: some View {
        HStack(spacing: 8) {
            Text("$")
                .font(.system(size: 12, weight: .semibold, design: .monospaced))
                .foregroundStyle(canSendInput ? Color.accentColor : .secondary)

            TextField("Type a shell command and press Return", text: $terminalInput)
                .textFieldStyle(.roundedBorder)
                .font(.system(size: 12, weight: .regular, design: .monospaced))
                .onSubmit(onSend)
                .disabled(!canSendInput)

            Button("Send") {
                onSend()
            }
            .disabled(!canSendInput || terminalInput.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        }
        .controlSize(.small)
    }

    private var portForwardCard: some View {
        VStack(alignment: .leading, spacing: 12) {
            ViewThatFits(in: .horizontal) {
                HStack(alignment: .center, spacing: 10) {
                    portForwardTitleBlock
                    Spacer(minLength: 12)
                    portForwardHeaderActions
                }

                VStack(alignment: .leading, spacing: 10) {
                    portForwardTitleBlock
                    portForwardHeaderActions
                }
            }

            if isPortForwardExpanded {
                TerminalPodSelectorRow(
                    title: "Port-forward pod",
                    systemImage: "point.3.connected.trianglepath.dotted",
                    pods: availablePods,
                    selection: $selectedPortForwardPodID
                )

                ViewThatFits(in: .horizontal) {
                    portForwardEndpointFields
                    VStack(alignment: .leading, spacing: 8) {
                        portForwardEndpointFields
                    }
                }

                activePortForwardList
            } else {
                compactPortForwardStatus
            }
        }
        .runePanelCard(padding: RuneUILayoutMetrics.paneInnerPadding)
    }

    private var portForwardTitleBlock: some View {
        VStack(alignment: .leading, spacing: 5) {
            Label("Port Forward", systemImage: "point.3.connected.trianglepath.dotted")
                .font(.headline)

            Text(portForwardPod.map { "\($0.namespace)/\($0.name)" } ?? "No pod selected")
                .font(.footnote.weight(.semibold))
                .foregroundStyle(.secondary)
                .lineLimit(1)
                .help(portForwardPod.map { "\($0.namespace)/\($0.name)" } ?? "No pod selected")
        }
    }

    private var portForwardStartButton: some View {
        Group {
            if let session = selectedStoppablePortForwardSession {
                Button(session.status == .starting ? "Cancel" : "Stop") {
                    onStopPortForward(session)
                }
                .help(session.status == .starting ? "Cancel this port-forward" : "Stop this port-forward")
            } else {
                Button("Start") {
                    if let portForwardPod {
                        onStartPortForward(portForwardPod)
                    }
                }
                .disabled(portForwardPod == nil || !canApplyMutations)
            }
        }
        .controlSize(.small)
    }

    private var portForwardHeaderActions: some View {
        HStack(spacing: 6) {
            portForwardStartButton

            Button {
                isPortForwardExpanded.toggle()
            } label: {
                Label(isPortForwardExpanded ? "Minimize" : "Expand", systemImage: isPortForwardExpanded ? "chevron.up" : "chevron.down")
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
            .help(isPortForwardExpanded ? "Minimize port-forward controls" : "Expand port-forward controls")
        }
    }

    private var compactPortForwardStatus: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(alignment: .firstTextBaseline, spacing: 8) {
                if let session = primaryPortForwardSession {
                    portForwardStatusDot(session.status)
                    Text("\(session.resourceLabel) \(session.localPort):\(session.remotePort)")
                        .font(.subheadline.weight(.semibold))
                        .lineLimit(1)
                        .help("\(session.resourceLabel) \(session.localPort):\(session.remotePort)")
                    Spacer(minLength: 0)
                    Text(session.status.rawValue.capitalized)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(portForwardStatusColor(session.status))
                    if session.status == .active, session.browserURL != nil {
                        portForwardOpenInBrowserButton(session)
                    }
                    if session.status == .starting || session.status == .active || session.status == .failed {
                        portForwardStopButton(session)
                    }
                } else {
                    portForwardStatusDot(.stopped)
                    Text("No active port-forward")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Spacer(minLength: 0)
                    Text("\(portForwardLocalPort) -> \(portForwardRemotePort)")
                        .font(.caption.monospacedDigit())
                        .foregroundStyle(.secondary)
                }
            }

            HStack(spacing: 8) {
                Text(portForwardPod.map { $0.name } ?? "No pod selected")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .help(portForwardPod.map { "\($0.namespace)/\($0.name)" } ?? "No pod selected")

                if activeOrStartingPortForwardSessions.count > 1 {
                    RuneChip(verticalPadding: 2) {
                        Text("+\(activeOrStartingPortForwardSessions.count - 1) more")
                            .font(.caption2.weight(.semibold))
                    }
                }
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 6)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private var portForwardEndpointFields: some View {
        HStack(spacing: 8) {
            terminalField("Local", text: $portForwardLocalPort, minWidth: 74, idealWidth: 92)
            Text("->")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            terminalField("Remote", text: $portForwardRemotePort, minWidth: 74, idealWidth: 92)
            terminalField("Address", text: $portForwardAddress, minWidth: 120, idealWidth: 150)
            Spacer(minLength: 0)
        }
        .controlSize(.small)
    }

    private var activePortForwardList: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Active Port Forwards")
                .font(.subheadline.weight(.semibold))

            if portForwardSessions.isEmpty {
                Text("No port-forward sessions started yet.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            } else {
                ForEach(portForwardSessions) { session in
                    HStack(alignment: .firstTextBaseline, spacing: 8) {
                        portForwardStatusDot(session.status)
                        VStack(alignment: .leading, spacing: 3) {
                            Text("\(session.resourceLabel)  \(session.localPort):\(session.remotePort)")
                                .font(.subheadline.weight(.semibold))
                                .lineLimit(1)
                                .help("\(session.resourceLabel) \(session.localPort):\(session.remotePort)")
                            Text("\(session.contextName) • \(session.namespace) • \(session.status.rawValue.capitalized)")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                                .help("\(session.contextName) • \(session.namespace) • \(session.status.rawValue.capitalized)")
                        }
                        Spacer(minLength: 0)
                        if session.status == .starting || session.status == .active || session.status == .failed {
                            portForwardStopButton(session)
                        }
                        if session.status == .active, session.browserURL != nil {
                            portForwardOpenInBrowserButton(session)
                        }
                    }
                    .padding(.horizontal, 8)
                    .padding(.vertical, 6)
                    .background(RuneSurfaceBackground(kind: .listRow(isSelected: false)))
                }
            }
        }
    }

    private func portForwardOpenInBrowserButton(_ session: PortForwardSession) -> some View {
        Button {
            onOpenPortForwardInBrowser(session)
        } label: {
            Label("Open in Browser", systemImage: "safari")
        }
        .buttonStyle(.bordered)
        .controlSize(.small)
        .help(session.browserURL.map { "Open \($0.absoluteString)" } ?? "Open local port-forward URL")
    }

    private func portForwardStopButton(_ session: PortForwardSession) -> some View {
        Button(session.status == .starting ? "Cancel" : "Stop") {
            onStopPortForward(session)
        }
        .buttonStyle(.bordered)
        .controlSize(.small)
        .help(session.status == .starting ? "Cancel this port-forward" : "Stop this port-forward")
    }

    private func terminalField(_ placeholder: String, text: Binding<String>, minWidth: CGFloat, idealWidth: CGFloat) -> some View {
        TextField(placeholder, text: text)
            .textFieldStyle(.roundedBorder)
            .font(.system(size: 12, weight: .regular, design: .monospaced))
            .frame(minWidth: minWidth, idealWidth: idealWidth, maxWidth: idealWidth + 44)
    }

    private func pod(for id: String) -> PodSummary? {
        availablePods.first { $0.id == id }
    }

    private func portForwardStatusDot(_ status: PortForwardStatus) -> some View {
        Circle()
            .fill(portForwardStatusColor(status))
            .frame(width: 8, height: 8)
            .padding(.top, 4)
    }

    private func portForwardStatusColor(_ status: PortForwardStatus) -> Color {
        switch status {
        case .starting:
            return .orange
        case .active:
            return .green
        case .stopped:
            return .secondary
        case .failed:
            return .red
        }
    }

    private func terminalStatusBadge(_ status: PodTerminalSessionStatus) -> some View {
        Text(statusTitle(status))
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(statusColor(status).opacity(0.16), in: Capsule())
            .foregroundStyle(statusColor(status))
    }

    private func statusTitle(_ status: PodTerminalSessionStatus) -> String {
        switch status {
        case .connecting: return "Connecting"
        case .connected: return "Connected"
        case .disconnected: return "Disconnected"
        case .failed: return "Failed"
        }
    }

    private func statusColor(_ status: PodTerminalSessionStatus) -> Color {
        switch status {
        case .connecting: return .orange
        case .connected: return .green
        case .disconnected: return .secondary
        case .failed: return .red
        }
    }
}

private struct TerminalPodSelectorRow: View {
    let title: String
    let systemImage: String
    let pods: [PodSummary]
    @Binding var selection: String

    var body: some View {
        ViewThatFits(in: .horizontal) {
            HStack(spacing: 10) {
                selectorLabel
                    .frame(width: 128, alignment: .leading)
                picker
            }

            VStack(alignment: .leading, spacing: 6) {
                selectorLabel
                picker
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private var selectorLabel: some View {
        Label(title, systemImage: systemImage)
            .font(.footnote.weight(.semibold))
            .foregroundStyle(.secondary)
            .lineLimit(1)
    }

    private var picker: some View {
        Picker(title, selection: $selection) {
            if pods.isEmpty {
                Text("No pods in namespace").tag("")
            } else {
                ForEach(pods) { pod in
                    Text(podTitle(pod)).tag(pod.id)
                }
            }
        }
        .labelsHidden()
        .disabled(pods.isEmpty)
        .controlSize(.small)
        .frame(minWidth: 180, idealWidth: 320, maxWidth: .infinity, minHeight: 24, idealHeight: 26, maxHeight: 28, alignment: .leading)
    }

    private func podTitle(_ pod: PodSummary) -> String {
        "\(pod.name)  \(pod.status)"
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

private struct TerminalTranscriptSurface: View {
    let text: String
    let minHeight: CGFloat
    let resetID: String

    var body: some View {
        InspectorTextSurface(minHeight: minHeight) {
            TerminalTranscriptTextView(text: text)
                .id(resetID)
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }
}

private struct TerminalTranscriptTextView: NSViewRepresentable {
    let text: String

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = NSScrollView(frame: .zero)
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.borderType = .noBorder
        scrollView.drawsBackground = false
        scrollView.automaticallyAdjustsContentInsets = false
        scrollView.scrollerInsets = NSEdgeInsets()
        scrollView.contentInsets = NSEdgeInsets()
        scrollView.horizontalScrollElasticity = .none
        scrollView.verticalScrollElasticity = .allowed

        let textView = NSTextView(frame: .zero)
        textView.isEditable = false
        textView.isSelectable = true
        textView.isRichText = false
        textView.importsGraphics = false
        textView.usesFindBar = true
        textView.usesFontPanel = false
        textView.allowsUndo = false
        textView.isAutomaticQuoteSubstitutionEnabled = false
        textView.isAutomaticDashSubstitutionEnabled = false
        textView.isAutomaticDataDetectionEnabled = false
        textView.isAutomaticLinkDetectionEnabled = false
        textView.isAutomaticSpellingCorrectionEnabled = false
        textView.isAutomaticTextCompletionEnabled = false
        textView.isGrammarCheckingEnabled = false
        textView.isContinuousSpellCheckingEnabled = false
        textView.isHorizontallyResizable = false
        textView.isVerticallyResizable = true
        textView.minSize = .zero
        textView.maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        textView.autoresizingMask = [.width]
        textView.textContainerInset = NSSize(width: 10, height: 10)
        textView.backgroundColor = .clear
        textView.drawsBackground = false
        textView.font = NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)
        textView.textColor = .labelColor

        if let container = textView.textContainer {
            container.widthTracksTextView = true
            container.heightTracksTextView = false
            container.containerSize = NSSize(width: 0, height: CGFloat.greatestFiniteMagnitude)
            container.lineFragmentPadding = 0
        }

        textView.string = text
        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        guard let textView = scrollView.documentView as? NSTextView else { return }

        if let container = textView.textContainer {
            let contentWidth = max(0, scrollView.contentSize.width - textView.textContainerInset.width * 2)
            if abs(container.containerSize.width - contentWidth) > 1 {
                container.containerSize = NSSize(width: contentWidth, height: CGFloat.greatestFiniteMagnitude)
            }
        }

        let shouldStickToBottom = isNearBottom(scrollView)
        if textView.string != text {
            if text.hasPrefix(textView.string),
               let textStorage = textView.textStorage {
                let suffix = String(text.dropFirst(textView.string.count))
                textStorage.append(
                    NSAttributedString(
                        string: suffix,
                        attributes: [
                            .font: textView.font ?? NSFont.monospacedSystemFont(ofSize: 12, weight: .regular),
                            .foregroundColor: textView.textColor ?? NSColor.labelColor
                        ]
                    )
                )
            } else {
                textView.string = text
            }
            textView.layoutManager?.ensureLayout(for: textView.textContainer!)
            textView.invalidateIntrinsicContentSize()
            textView.layoutSubtreeIfNeeded()
            if shouldStickToBottom {
                let range = NSRange(location: max(0, text.utf16.count - 1), length: 0)
                textView.scrollRangeToVisible(range)
            }
        } else {
            textView.layoutManager?.ensureLayout(for: textView.textContainer!)
        }
    }

    private func isNearBottom(_ scrollView: NSScrollView) -> Bool {
        guard let documentView = scrollView.documentView else { return true }
        let clipBounds = scrollView.contentView.bounds
        let maxOffset = max(0, documentView.frame.maxY - clipBounds.height)
        return maxOffset - clipBounds.origin.y < 28
    }
}
