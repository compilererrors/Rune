import SwiftUI
import RuneCore

enum TerminalPodControlLayoutMetrics {
    static let supportedInspectorWidth: CGFloat = 320
    static let labelWidth: CGFloat = 104
    static let widePickerWidth: CGFloat = 320
    static let compactPickerWidth: CGFloat = 280
    static let controlSpacing: CGFloat = 10
    static let compactSpacing: CGFloat = 6
}

struct TerminalPodSelectorControl: View {
    let title: String
    let pods: [PodSummary]
    let terminalSessions: [PodTerminalSession]
    let width: CGFloat
    let isFavoritePod: (PodSummary) -> Bool
    let onToggleFavoritePod: (PodSummary) -> Void
    @Binding var selection: String

    var body: some View {
        FavoritePodPicker(
            title: title,
            pods: pods,
            width: width,
            rowTitle: podTitle,
            rowDetail: podDetail,
            isFavoritePod: isFavoritePod,
            onToggleFavoritePod: onToggleFavoritePod,
            selection: $selection
        )
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

struct TerminalPodControlLayout<Actions: View>: View {
    let accessibilityLabel: String
    let title: String
    let systemImage: String
    let hasActions: Bool
    let selector: (CGFloat) -> TerminalPodSelectorControl
    @ViewBuilder let actions: Actions

    init(
        accessibilityLabel: String,
        title: String,
        systemImage: String,
        hasActions: Bool = true,
        selector: @escaping (CGFloat) -> TerminalPodSelectorControl,
        @ViewBuilder actions: () -> Actions
    ) {
        self.accessibilityLabel = accessibilityLabel
        self.title = title
        self.systemImage = systemImage
        self.hasActions = hasActions
        self.selector = selector
        self.actions = actions()
    }

    var body: some View {
        Group {
            if hasActions {
                RuneAdaptiveToolbar(accessibilityLabel) {
                    selectorGroup
                } secondary: {
                    actions
                }
            } else {
                selectorGroup
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private var selectorGroup: some View {
        ViewThatFits(in: .horizontal) {
            HStack(spacing: TerminalPodControlLayoutMetrics.controlSpacing) {
                selectorLabel
                    .frame(width: TerminalPodControlLayoutMetrics.labelWidth, alignment: .leading)
                selector(TerminalPodControlLayoutMetrics.widePickerWidth)
            }

            VStack(alignment: .leading, spacing: TerminalPodControlLayoutMetrics.compactSpacing) {
                selectorLabel
                selector(TerminalPodControlLayoutMetrics.compactPickerWidth)
            }
        }
    }

    private var selectorLabel: some View {
        Label(title, systemImage: systemImage)
            .font(.footnote.weight(.semibold))
            .foregroundStyle(.secondary)
            .lineLimit(1)
    }
}

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
        TerminalPodControlLayout(
            accessibilityLabel: "\(title) controls",
            title: title,
            systemImage: systemImage,
            hasActions: actionTitle != nil && actionSystemImage != nil && onAction != nil
        ) { width in
            TerminalPodSelectorControl(
                title: title,
                pods: pods,
                terminalSessions: terminalSessions,
                width: width,
                isFavoritePod: isFavoritePod,
                onToggleFavoritePod: onToggleFavoritePod,
                selection: $selection
            )
        } actions: {
            actionButton
        }
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
}
