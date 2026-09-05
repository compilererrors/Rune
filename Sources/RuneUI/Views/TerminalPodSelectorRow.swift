import SwiftUI
import RuneCore

enum TerminalPodControlLayoutMetrics {
    static let supportedInspectorWidth: CGFloat = 320
    static let labelWidth: CGFloat = 104
    static let widePickerWidth: CGFloat = 360
    static let compactPickerWidth: CGFloat = 300
    static let controlSpacing: CGFloat = 10
    static let compactSpacing: CGFloat = 6
    static let actionSpacing: CGFloat = RuneAdaptiveToolbarMetrics.groupSpacing
    static let rowSpacing: CGFloat = RuneAdaptiveToolbarMetrics.rowSpacing
    static let minimumRowHeight: CGFloat = RuneUILayoutMetrics.iconButtonSize

    static let wideSelectorWidth = labelWidth + controlSpacing + widePickerWidth
    static let compactSelectorWidth = compactPickerWidth

    static func wideInlineMinimumWidth(actionWidth: CGFloat) -> CGFloat {
        wideSelectorWidth + actionSpacing + actionWidth
    }

    static func compactInlineMinimumWidth(actionWidth: CGFloat) -> CGFloat {
        compactSelectorWidth + actionSpacing + actionWidth
    }
}

private enum TerminalPodActionRailMode {
    case wideInline
    case compactInline
    case stacked
}

private struct TerminalPodActionRailLayout: Layout {
    let minimumRowHeight: CGFloat
    let forcesStackedLayout: Bool

    func sizeThatFits(
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) -> CGSize {
        guard subviews.count == 2 else { return .zero }

        let actionSize = subviews[1].sizeThatFits(.unspecified)
        let wideInlineMinimumWidth = TerminalPodControlLayoutMetrics.wideInlineMinimumWidth(
            actionWidth: actionSize.width
        )
        let width = proposal.width ?? wideInlineMinimumWidth
        let mode = layoutMode(width: width, actionWidth: actionSize.width)

        switch mode {
        case .wideInline, .compactInline:
            let selectorWidth = selectorWidth(for: mode)
            let selectorSize = subviews[0].sizeThatFits(ProposedViewSize(
                width: selectorWidth,
                height: nil
            ))
            return CGSize(
                width: width,
                height: max(minimumRowHeight, selectorSize.height, actionSize.height)
            )
        case .stacked:
            let selectorSize = subviews[0].sizeThatFits(ProposedViewSize(width: width, height: nil))
            let fittedActionSize = subviews[1].sizeThatFits(ProposedViewSize(width: width, height: nil))
            return CGSize(
                width: width,
                height: max(minimumRowHeight, selectorSize.height)
                    + TerminalPodControlLayoutMetrics.rowSpacing
                    + max(minimumRowHeight, fittedActionSize.height)
            )
        }
    }

    func placeSubviews(
        in bounds: CGRect,
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) {
        guard subviews.count == 2 else { return }

        let actionSize = subviews[1].sizeThatFits(.unspecified)
        let mode = layoutMode(width: bounds.width, actionWidth: actionSize.width)

        switch mode {
        case .wideInline, .compactInline:
            let selectorWidth = selectorWidth(for: mode)
            let selectorSize = subviews[0].sizeThatFits(ProposedViewSize(
                width: selectorWidth,
                height: nil
            ))
            let rowHeight = max(minimumRowHeight, selectorSize.height, actionSize.height)
            subviews[0].place(
                at: CGPoint(
                    x: bounds.minX,
                    y: bounds.minY + (rowHeight - selectorSize.height) / 2
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(
                    width: selectorWidth,
                    height: selectorSize.height
                )
            )
            subviews[1].place(
                at: CGPoint(
                    x: bounds.maxX - actionSize.width,
                    y: bounds.minY + (rowHeight - actionSize.height) / 2
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(width: actionSize.width, height: actionSize.height)
            )
        case .stacked:
            let selectorSize = subviews[0].sizeThatFits(ProposedViewSize(width: bounds.width, height: nil))
            let fittedActionSize = subviews[1].sizeThatFits(ProposedViewSize(width: bounds.width, height: nil))
            let selectorRowHeight = max(minimumRowHeight, selectorSize.height)
            let actionRowHeight = max(minimumRowHeight, fittedActionSize.height)
            subviews[0].place(
                at: CGPoint(
                    x: bounds.minX,
                    y: bounds.minY + (selectorRowHeight - selectorSize.height) / 2
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(width: bounds.width, height: selectorSize.height)
            )
            subviews[1].place(
                at: CGPoint(
                    x: bounds.minX,
                    y: bounds.minY
                        + selectorRowHeight
                        + TerminalPodControlLayoutMetrics.rowSpacing
                        + (actionRowHeight - fittedActionSize.height) / 2
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(width: bounds.width, height: fittedActionSize.height)
            )
        }
    }

    private func layoutMode(width: CGFloat, actionWidth: CGFloat) -> TerminalPodActionRailMode {
        guard !forcesStackedLayout else { return .stacked }
        if width >= TerminalPodControlLayoutMetrics.wideInlineMinimumWidth(actionWidth: actionWidth) {
            return .wideInline
        }
        if width >= TerminalPodControlLayoutMetrics.compactInlineMinimumWidth(actionWidth: actionWidth) {
            return .compactInline
        }
        return .stacked
    }

    private func selectorWidth(for mode: TerminalPodActionRailMode) -> CGFloat {
        switch mode {
        case .wideInline:
            return TerminalPodControlLayoutMetrics.wideSelectorWidth
        case .compactInline:
            return TerminalPodControlLayoutMetrics.compactSelectorWidth
        case .stacked:
            return 0
        }
    }
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
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

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
                TerminalPodActionRailLayout(
                    minimumRowHeight: minimumRowHeight,
                    forcesStackedLayout: dynamicTypeSize.isAccessibilitySize
                ) {
                    selectorGroup
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
        .accessibilityElement(children: .contain)
        .accessibilityLabel(accessibilityLabel)
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

    private var minimumRowHeight: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? RuneAdaptiveToolbarMetrics.accessibilityMinimumRowHeight
            : TerminalPodControlLayoutMetrics.minimumRowHeight
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
