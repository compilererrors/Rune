import SwiftUI
import RuneCore

struct TerminalPortForwardPanelView: View {
    @Binding var isExpanded: Bool
    let selectedPod: PodSummary?
    let availablePods: [PodSummary]
    let portForwardSessions: [PortForwardSession]
    let canApplyMutations: Bool
    @Binding var selectedPortForwardPodID: String
    @Binding var localPort: String
    @Binding var remotePort: String
    @Binding var address: String
    let onStartPortForward: (PodSummary) -> Void
    let onStopPortForward: (PortForwardSession) -> Void
    let onOpenPortForwardInBrowser: (PortForwardSession) -> Void
    private let compactStatusHeight: CGFloat = 54
    private let activeSessionListHeight: CGFloat = 128

    private var activeOrStartingSessions: [PortForwardSession] {
        portForwardSessions.filter { $0.status == .active || $0.status == .starting }
    }

    private var primarySession: PortForwardSession? {
        activeOrStartingSessions.first ?? portForwardSessions.first
    }

    private var selectedStoppableSession: PortForwardSession? {
        guard let selectedPod else { return nil }
        return portForwardSessions.first {
            $0.targetKind == .pod
                && $0.targetName == selectedPod.name
                && $0.namespace == selectedPod.namespace
                && ($0.status == .starting || $0.status == .active || $0.status == .failed)
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            header

            if isExpanded {
                expandedControls
            } else {
                compactStatus
            }
        }
        .runePanelCard(padding: RuneUILayoutMetrics.paneInnerPadding)
    }

    private var header: some View {
        ViewThatFits(in: .horizontal) {
            HStack(alignment: .center, spacing: 10) {
                titleBlock
                Spacer(minLength: 12)
                headerActions
            }

            VStack(alignment: .leading, spacing: 10) {
                titleBlock
                headerActions
            }
        }
    }

    private var titleBlock: some View {
        VStack(alignment: .leading, spacing: 5) {
            Label("Port Forward", systemImage: "point.3.connected.trianglepath.dotted")
                .font(.headline)

            Text(selectedPod.map { "\($0.namespace)/\($0.name)" } ?? "No pod selected")
                .font(.footnote.weight(.semibold))
                .foregroundStyle(.secondary)
                .lineLimit(1)
                .help(selectedPod.map { "\($0.namespace)/\($0.name)" } ?? "No pod selected")
        }
    }

    private var headerActions: some View {
        HStack(spacing: 6) {
            startButton

            Button {
                isExpanded.toggle()
            } label: {
                Label(isExpanded ? "Minimize" : "Expand", systemImage: isExpanded ? "chevron.up" : "chevron.down")
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
            .frame(width: 104)
            .help(isExpanded ? "Minimize port-forward controls" : "Expand port-forward controls")
        }
    }

    private var startButton: some View {
        Group {
            if let session = selectedStoppableSession {
                Button(session.status == .starting ? "Cancel" : "Stop") {
                    onStopPortForward(session)
                }
                .help(session.status == .starting ? "Cancel this port-forward" : "Stop this port-forward")
            } else {
                Button("Start") {
                    if let selectedPod {
                        onStartPortForward(selectedPod)
                    }
                }
                .disabled(selectedPod == nil || !canApplyMutations)
            }
        }
        .controlSize(.small)
        .frame(width: 74)
    }

    private var expandedControls: some View {
        VStack(alignment: .leading, spacing: 10) {
            TerminalPodSelectorRow(
                title: "Port-forward pod",
                systemImage: "point.3.connected.trianglepath.dotted",
                pods: availablePods,
                selection: $selectedPortForwardPodID
            )

            ViewThatFits(in: .horizontal) {
                endpointFields
                VStack(alignment: .leading, spacing: 8) {
                    endpointFields
                }
            }

            activeSessionList
        }
    }

    private var compactStatus: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(alignment: .firstTextBaseline, spacing: 8) {
                if let session = primarySession {
                    statusDot(session.status)
                    Text("\(session.resourceLabel) \(session.localPort):\(session.remotePort)")
                        .font(.subheadline.weight(.semibold))
                        .lineLimit(1)
                        .help("\(session.resourceLabel) \(session.localPort):\(session.remotePort)")
                    Spacer(minLength: 0)
                    Text(session.status.rawValue.capitalized)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(TerminalStatusStyling.color(session.status))
                    if session.status == .active, session.browserURL != nil {
                        openInBrowserButton(session)
                    }
                    if session.status == .starting || session.status == .active || session.status == .failed {
                        stopButton(session)
                    }
                } else {
                    statusDot(.stopped)
                    Text("No active port-forward")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Spacer(minLength: 0)
                    Text("\(localPort) -> \(remotePort)")
                        .font(.caption.monospacedDigit())
                        .foregroundStyle(.secondary)
                }
            }

            HStack(spacing: 8) {
                Text(selectedPod.map(\.name) ?? "No pod selected")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .help(selectedPod.map { "\($0.namespace)/\($0.name)" } ?? "No pod selected")

                if activeOrStartingSessions.count > 1 {
                    RuneChip(verticalPadding: 2) {
                        Text("+\(activeOrStartingSessions.count - 1) more")
                            .font(.caption2.weight(.semibold))
                    }
                }
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 6)
        .frame(height: compactStatusHeight)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private var endpointFields: some View {
        HStack(spacing: 8) {
            field("Local", text: $localPort, minWidth: 74, idealWidth: 92)
            Text("->")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            field("Remote", text: $remotePort, minWidth: 74, idealWidth: 92)
            field("Address", text: $address, minWidth: 120, idealWidth: 150)
            Spacer(minLength: 0)
        }
        .controlSize(.small)
    }

    private var activeSessionList: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Active Port Forwards")
                .font(.subheadline.weight(.semibold))

            ScrollView {
                VStack(alignment: .leading, spacing: 6) {
                    if portForwardSessions.isEmpty {
                        Text("No port-forward sessions started yet.")
                            .font(.footnote)
                            .foregroundStyle(.secondary)
                            .frame(maxWidth: .infinity, minHeight: activeSessionListHeight - 8, alignment: .topLeading)
                    } else {
                        ForEach(portForwardSessions) { session in
                            HStack(alignment: .firstTextBaseline, spacing: 8) {
                                statusDot(session.status)
                                VStack(alignment: .leading, spacing: 3) {
                                    Text("\(session.resourceLabel)  \(session.localPort):\(session.remotePort)")
                                        .font(.subheadline.weight(.semibold))
                                        .lineLimit(1)
                                        .help("\(session.resourceLabel) \(session.localPort):\(session.remotePort)")
                                    Text("\(session.contextName) - \(session.namespace) - \(session.status.rawValue.capitalized)")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                        .lineLimit(1)
                                        .help("\(session.contextName) - \(session.namespace) - \(session.status.rawValue.capitalized)")
                                }
                                Spacer(minLength: 0)
                                if session.status == .starting || session.status == .active || session.status == .failed {
                                    stopButton(session)
                                }
                                if session.status == .active, session.browserURL != nil {
                                    openInBrowserButton(session)
                                }
                            }
                            .padding(.horizontal, 8)
                            .padding(.vertical, 6)
                            .background(RuneSurfaceBackground(kind: .listRow(isSelected: false)))
                        }
                    }
                }
            }
            .frame(height: activeSessionListHeight)
            .scrollContentBackground(.hidden)
        }
    }

    private func field(_ placeholder: String, text: Binding<String>, minWidth: CGFloat, idealWidth: CGFloat) -> some View {
        TextField(placeholder, text: text)
            .textFieldStyle(.roundedBorder)
            .font(.system(size: 12, weight: .regular, design: .monospaced))
            .frame(minWidth: minWidth, idealWidth: idealWidth, maxWidth: idealWidth + 44)
    }

    private func openInBrowserButton(_ session: PortForwardSession) -> some View {
        Button {
            onOpenPortForwardInBrowser(session)
        } label: {
            Label("Open in Browser", systemImage: "safari")
        }
        .buttonStyle(.bordered)
        .controlSize(.small)
        .help(session.browserURL.map { "Open \($0.absoluteString)" } ?? "Open local port-forward URL")
    }

    private func stopButton(_ session: PortForwardSession) -> some View {
        Button(session.status == .starting ? "Cancel" : "Stop") {
            onStopPortForward(session)
        }
        .buttonStyle(.bordered)
        .controlSize(.small)
        .help(session.status == .starting ? "Cancel this port-forward" : "Stop this port-forward")
    }

    private func statusDot(_ status: PortForwardStatus) -> some View {
        TerminalStatusDot(color: TerminalStatusStyling.color(status), topPadding: 4)
    }
}
