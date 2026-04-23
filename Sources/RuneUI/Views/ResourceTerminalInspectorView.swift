import AppKit
import SwiftUI
import RuneCore

struct ResourceTerminalWorkspaceView: View {
    let session: PodTerminalSession?
    let selectedPod: PodSummary?
    let portForwardSessions: [PortForwardSession]
    let canApplyMutations: Bool
    @Binding var terminalInput: String
    let onStartSession: () -> Void
    let onSend: () -> Void
    let onDisconnect: () -> Void
    let onClearTranscript: () -> Void

    private var targetPodLabel: String? {
        if let session {
            return "\(session.namespace)/\(session.podName)"
        }
        if let selectedPod {
            return "\(selectedPod.namespace)/\(selectedPod.name)"
        }
        return nil
    }

    private var canSendInput: Bool {
        session?.status == .connected
    }

    private var transcriptPlaceholder: String {
        if selectedPod == nil && session == nil {
            return "Select a pod in Workloads > Pods, then start an interactive shell."
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
            HStack(alignment: .center, spacing: 10) {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Pod Shell")
                        .font(.headline)
                    if let targetPodLabel {
                        Text(targetPodLabel)
                            .font(.footnote.weight(.semibold))
                            .foregroundStyle(.secondary)
                    } else {
                        Text("No pod selected")
                            .font(.footnote)
                            .foregroundStyle(.secondary)
                    }
                }

                Spacer()

                if let session {
                    terminalStatusBadge(session.status)
                }

                Button(session == nil ? "Connect Shell" : "Reconnect") {
                    onStartSession()
                }
                .disabled(targetPodLabel == nil || !canApplyMutations)

                Button("Disconnect") {
                    onDisconnect()
                }
                .disabled(session == nil)

                Button("Clear") {
                    onClearTranscript()
                }
                .disabled(session?.transcript.isEmpty ?? true)
            }

            TerminalTranscriptSurface(
                text: session?.transcript.isEmpty == false ? session?.transcript ?? "" : transcriptPlaceholder,
                minHeight: 420,
                resetID: "terminal:\(session?.id ?? "empty")"
            )

            HStack(spacing: 8) {
                Text("$")
                    .font(.system(size: 12, weight: .semibold, design: .monospaced))
                    .foregroundStyle(canSendInput ? Color.accentColor : .secondary)

                TextField("Type a shell command and press Return", text: $terminalInput)
                    .textFieldStyle(.roundedBorder)
                    .onSubmit(onSend)
                    .disabled(!canSendInput)

                Button("Send") {
                    onSend()
                }
                .disabled(!canSendInput || terminalInput.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
        }
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous)
                .fill(.regularMaterial)
        )
        .frame(maxWidth: .infinity, alignment: .topLeading)
    }

    private var portForwardCard: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Active Port Forwards")
                .font(.headline)

            if portForwardSessions.isEmpty {
                Text("No port-forward sessions started yet.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(portForwardSessions) { session in
                    VStack(alignment: .leading, spacing: 4) {
                        Text("\(session.resourceLabel)  \(session.localPort):\(session.remotePort)")
                            .font(.subheadline.weight(.semibold))
                        Text("\(session.contextName) • \(session.namespace) • \(session.status.rawValue.capitalized)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous)
                .fill(.regularMaterial)
        )
        .frame(maxWidth: .infinity, alignment: .topLeading)
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
            textView.string = text
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
