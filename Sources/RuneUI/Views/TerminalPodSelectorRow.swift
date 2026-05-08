import SwiftUI
import RuneCore

struct TerminalPodSelectorRow: View {
    let title: String
    let systemImage: String
    let pods: [PodSummary]
    let terminalSessions: [PodTerminalSession]
    let actionTitle: String?
    let actionSystemImage: String?
    let isActionDisabled: Bool
    let onAction: (() -> Void)?
    @Binding var selection: String

    init(
        title: String,
        systemImage: String,
        pods: [PodSummary],
        terminalSessions: [PodTerminalSession] = [],
        actionTitle: String? = nil,
        actionSystemImage: String? = nil,
        isActionDisabled: Bool = false,
        onAction: (() -> Void)? = nil,
        selection: Binding<String>
    ) {
        self.title = title
        self.systemImage = systemImage
        self.pods = pods
        self.terminalSessions = terminalSessions
        self.actionTitle = actionTitle
        self.actionSystemImage = actionSystemImage
        self.isActionDisabled = isActionDisabled
        self.onAction = onAction
        self._selection = selection
    }

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 10) {
                selectorLabel
                    .frame(width: 128, alignment: .leading)
                picker
                actionButton
            }
            .fixedSize(horizontal: true, vertical: false)
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
        .frame(width: 320, height: 26, alignment: .leading)
    }

    @ViewBuilder
    private var actionButton: some View {
        if let actionTitle, let actionSystemImage, let onAction {
            Button(action: onAction) {
                Label(actionTitle, systemImage: actionSystemImage)
                    .lineLimit(1)
                    .frame(width: 94)
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
            .disabled(isActionDisabled)
            .help(actionTitle)
        }
    }

    private func podTitle(_ pod: PodSummary) -> String {
        guard let status = terminalSessionStatus(for: pod) else {
            return "\(pod.name)  \(pod.status)"
        }
        return "\(pod.name)  \(pod.status)  -  \(status)"
    }

    private func terminalSessionStatus(for pod: PodSummary) -> String? {
        let matching = terminalSessions.filter { $0.namespace == pod.namespace && $0.podName == pod.name }
        if matching.contains(where: { $0.status == .connected }) { return "Connected" }
        if matching.contains(where: { $0.status == .connecting }) { return "Connecting" }
        if matching.contains(where: { $0.status == .failed }) { return "Failed" }
        if matching.contains(where: { $0.status == .disconnected }) { return "Disconnected" }
        return nil
    }
}
