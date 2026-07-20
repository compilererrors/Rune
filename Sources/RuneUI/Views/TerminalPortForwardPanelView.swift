import SwiftUI
import RuneCore

struct TerminalPortForwardPanelView: View {
    @Binding var isExpanded: Bool
    let contextName: String?
    let selectedPod: PodSummary?
    let availablePods: [PodSummary]
    let portForwardSessions: [PortForwardSession]
    let canApplyMutations: Bool
    @Binding var selectedPortForwardPodID: String
    @Binding var localPort: String
    @Binding var remotePort: String
    @Binding var address: String
    var isFavoritePod: (PodSummary) -> Bool = { _ in false }
    var onToggleFavoritePod: (PodSummary) -> Void = { _ in }
    let onStartPortForward: (PodSummary) -> Void
    let onStopPortForward: (PortForwardSession) -> Void
    let onOpenPortForwardInBrowser: (PortForwardSession) -> Void
    let onRetryPortForward: (PortForwardSession) -> Void
    let onClearPortForward: (PortForwardSession) -> Void
    let onClearInactivePortForwards: () -> Void
    @Environment(\.runeThemePalette) private var runeThemePalette
    private let compactStatusHeight: CGFloat = 54
    private let activeSessionListHeight: CGFloat = 128

    private var activeOrStartingSessions: [PortForwardSession] {
        portForwardSessions.filter(\.isActiveOrStarting)
    }

    private var inactiveSessions: [PortForwardSession] {
        portForwardSessions.filter(\.isInactive)
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
                && $0.contextName == contextName
                && $0.isActiveOrStarting
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
        RuneAdaptiveToolbar("Port-forward controls") {
            titleBlock
        } secondary: {
            headerActions
        }
        .fixedSize(horizontal: false, vertical: true)
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
        PortForwardPrimaryActionLayout {
            primaryActionButton
        } utilities: {
            ViewThatFits(in: .horizontal) {
                HStack(spacing: 6) {
                    copyDraftKubectlButton
                    expansionButton
                }
                .fixedSize(horizontal: true, vertical: false)

                VStack(alignment: .leading, spacing: 6) {
                    copyDraftKubectlButton
                    expansionButton
                }
            }
        }
    }

    private var primaryActionButton: some View {
        PortForwardPrimaryActionButton(
            activeSession: selectedStoppableSession,
            startTitle: "Start",
            isStartDisabled: selectedPod == nil || !canApplyMutations,
            onStart: {
                if let selectedPod {
                    onStartPortForward(selectedPod)
                }
            },
            onStop: onStopPortForward
        )
    }

    private var expansionButton: some View {
        Button {
            isExpanded.toggle()
        } label: {
            Label(
                isExpanded ? "Minimize" : "Expand",
                systemImage: isExpanded ? "chevron.up" : "chevron.down"
            )
        }
        .buttonStyle(.bordered)
        .controlSize(.regular)
        .help(isExpanded ? "Minimize port-forward controls" : "Expand port-forward controls")
    }

    private var copyDraftKubectlButton: some View {
        Button {
            copyDraftPortForwardCommand()
        } label: {
            Label("Copy kubectl", systemImage: "doc.on.doc")
        }
        .buttonStyle(.bordered)
        .controlSize(.regular)
        .disabled(selectedPod == nil)
        .help("Copy kubectl port-forward command")
    }

    private var expandedControls: some View {
        VStack(alignment: .leading, spacing: 10) {
            TerminalPodSelectorRow(
                title: "Port-forward pod",
                systemImage: "point.3.connected.trianglepath.dotted",
                pods: availablePods,
                isFavoritePod: isFavoritePod,
                onToggleFavoritePod: onToggleFavoritePod,
                selection: $selectedPortForwardPodID
            )

            PortForwardEndpointFields(
                localPort: $localPort,
                remotePort: $remotePort,
                address: $address
            )

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
                        .foregroundStyle(TerminalStatusStyling.color(session.status, palette: runeThemePalette))
                } else {
                    statusDot(.stopped)
                    Text("No active port-forward")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Spacer(minLength: 0)
                    Text("\(localPort) → \(remotePort)")
                        .font(.caption.monospacedDigit())
                        .foregroundStyle(.secondary)
                }
            }

            if let session = primarySession {
                ViewThatFits(in: .horizontal) {
                    HStack(spacing: 8) {
                        selectedPodStatusLabel
                        sessionUtilityButtons(session)
                    }
                    .fixedSize(horizontal: true, vertical: false)

                    VStack(alignment: .leading, spacing: 6) {
                        selectedPodStatusLabel
                        sessionUtilityButtons(session)
                    }
                }
            } else {
                selectedPodStatusLabel
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 6)
        .frame(minHeight: compactStatusHeight)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private var selectedPodStatusLabel: some View {
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

    private var activeSessionList: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(activeOrStartingSessions.isEmpty && !inactiveSessions.isEmpty ? "Recent Port Forwards" : "Port Forwards")
                    .font(.subheadline.weight(.semibold))
                Spacer(minLength: 0)
                if !inactiveSessions.isEmpty {
                    Button("Clear Inactive") {
                        onClearInactivePortForwards()
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)
                    .help("Remove stopped and failed port-forward rows from this list")
                }
            }

            ScrollView {
                VStack(alignment: .leading, spacing: 6) {
                    if portForwardSessions.isEmpty {
                        Text("No port-forward sessions started yet.")
                            .font(.footnote)
                            .foregroundStyle(.secondary)
                            .frame(maxWidth: .infinity, minHeight: activeSessionListHeight - 8, alignment: .topLeading)
                    } else {
                        ForEach(portForwardSessions) { session in
                            VStack(alignment: .leading, spacing: 6) {
                                HStack(alignment: .firstTextBaseline, spacing: 8) {
                                    statusDot(session.status)
                                    VStack(alignment: .leading, spacing: 3) {
                                        Text("\(session.resourceLabel)  \(session.localPort):\(session.remotePort)")
                                            .font(.subheadline.weight(.semibold))
                                            .lineLimit(1)
                                            .help("\(session.resourceLabel) \(session.localPort):\(session.remotePort)")
                                        Text("\(session.contextName) - \(session.namespace)")
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                            .lineLimit(1)
                                            .help("\(session.contextName) - \(session.namespace)")
                                    }
                                    Spacer(minLength: 0)
                                    Text(session.status.rawValue.capitalized)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(TerminalStatusStyling.color(session.status, palette: runeThemePalette))
                                }

                                sessionUtilityButtons(session)
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

    private func sessionUtilityButtons(_ session: PortForwardSession) -> some View {
        ViewThatFits(in: .horizontal) {
            HStack(spacing: 6) {
                sessionUtilityButtonContent(session)
            }
            .fixedSize(horizontal: true, vertical: false)

            VStack(alignment: .leading, spacing: 6) {
                sessionUtilityButtonContent(session)
            }
        }
        .accessibilityElement(children: .contain)
    }

    @ViewBuilder
    private func sessionUtilityButtonContent(_ session: PortForwardSession) -> some View {
        if session.status == .active, session.browserURL != nil {
            openInBrowserButton(session)
        }
        if session.status == .failed {
            retryButton(session)
        }
        copyPortForwardCommandButton(session)
        if session.isInactive {
            clearButton(session)
        }
    }

    private func openInBrowserButton(_ session: PortForwardSession) -> some View {
        Button {
            onOpenPortForwardInBrowser(session)
        } label: {
            Label("Open in Browser", systemImage: "safari")
        }
        .buttonStyle(.bordered)
        .controlSize(.regular)
        .help(session.browserURL.map { "Open \($0.absoluteString)" } ?? "Open local port-forward URL")
    }

    private func retryButton(_ session: PortForwardSession) -> some View {
        Button {
            onRetryPortForward(session)
        } label: {
            Label("Retry", systemImage: "arrow.clockwise")
        }
        .buttonStyle(.bordered)
        .controlSize(.regular)
        .help("Try this port-forward again")
    }

    private func clearButton(_ session: PortForwardSession) -> some View {
        Button {
            onClearPortForward(session)
        } label: {
            Label("Clear", systemImage: "xmark.circle")
        }
        .buttonStyle(.bordered)
        .controlSize(.regular)
        .help("Remove this inactive port-forward row")
    }

    private func copyPortForwardCommandButton(_ session: PortForwardSession) -> some View {
        Button {
            TerminalKubectlCommandBuilder.copyToPasteboard(
                TerminalKubectlCommandBuilder.portForward(
                    contextName: session.contextName,
                    namespace: session.namespace,
                    targetKind: session.targetKind,
                    targetName: session.targetName,
                    localPort: session.localPort,
                    remotePort: session.remotePort,
                    address: session.address
                )
            )
        } label: {
            Label("Copy kubectl", systemImage: "doc.on.doc")
        }
        .buttonStyle(.bordered)
        .controlSize(.regular)
        .help("Copy kubectl port-forward command")
    }

    private func copyDraftPortForwardCommand() {
        guard let selectedPod else { return }
        TerminalKubectlCommandBuilder.copyToPasteboard(
            TerminalKubectlCommandBuilder.portForward(
                contextName: contextName,
                namespace: selectedPod.namespace,
                targetKind: .pod,
                targetName: selectedPod.name,
                localPort: localPort,
                remotePort: remotePort,
                address: address
            )
        )
    }

    private func statusDot(_ status: PortForwardStatus) -> some View {
        TerminalStatusDot(
            color: TerminalStatusStyling.color(status, palette: runeThemePalette),
            topPadding: 4
        )
    }
}
