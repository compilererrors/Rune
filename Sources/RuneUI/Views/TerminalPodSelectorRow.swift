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
    let isFavoritePod: (PodSummary) -> Bool
    let onToggleFavoritePod: (PodSummary) -> Void
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
        isFavoritePod: @escaping (PodSummary) -> Bool = { _ in false },
        onToggleFavoritePod: @escaping (PodSummary) -> Void = { _ in },
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
        self.isFavoritePod = isFavoritePod
        self.onToggleFavoritePod = onToggleFavoritePod
        self.onAction = onAction
        self._selection = selection
    }

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 10) {
                selectorLabel
                    .frame(width: 112, alignment: .leading)
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
        FavoritePodPicker(
            title: title,
            pods: pods,
            width: 320,
            rowTitle: podTitle,
            rowDetail: podDetail,
            isFavoritePod: isFavoritePod,
            onToggleFavoritePod: onToggleFavoritePod,
            selection: $selection
        )
    }

    @ViewBuilder
    private var actionButton: some View {
        if let actionTitle, let actionSystemImage, let onAction {
            Button(action: onAction) {
                Label(actionTitle, systemImage: actionSystemImage)
                    .lineLimit(1)
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
            .frame(width: 104)
            .disabled(isActionDisabled)
            .help(actionTitle)
        }
    }

    private func podTitle(_ pod: PodSummary) -> String {
        podDetail(pod).map { "\(pod.name)  \($0)" } ?? pod.name
    }

    private func podDetail(_ pod: PodSummary) -> String? {
        guard let status = terminalSessionStatus(for: pod) else {
            return pod.status
        }
        return "\(pod.status) - \(status)"
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
