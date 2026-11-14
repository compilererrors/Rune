import SwiftUI
import RuneCore

enum FavoritePodPickerMetrics {
    static let popoverMinimumWidth: CGFloat = 320
}

struct FavoritePodPickerPresentation {
    static func selectedPod(in pods: [PodSummary], selection: String) -> PodSummary? {
        pods.first { $0.id == selection }
    }

    static func sortedPods(_ pods: [PodSummary], isFavoritePod: (PodSummary) -> Bool) -> [PodSummary] {
        pods.sorted { lhs, rhs in
            let lhsFavorite = isFavoritePod(lhs)
            let rhsFavorite = isFavoritePod(rhs)
            if lhsFavorite != rhsFavorite {
                return lhsFavorite && !rhsFavorite
            }
            return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
        }
    }

    static func selectedFavoriteIcon(
        in pods: [PodSummary],
        selection: String,
        isFavoritePod: (PodSummary) -> Bool
    ) -> String {
        selectedPod(in: pods, selection: selection).map(isFavoritePod) == true ? "star.fill" : "star"
    }
}

enum FavoritePodFavoriteActionPlacement: Sendable, Equatable {
    case selectedPod
    case podRow
}

/// Shared favorite affordance for the selected-pod control and each popover row.
/// The glyph remains compact while `RuneIconButton` owns the 28-point target and
/// selected-state accessibility semantics.
struct FavoritePodFavoriteActionButton: View {
    let podName: String?
    let isFavorite: Bool
    let placement: FavoritePodFavoriteActionPlacement
    let action: () -> Void

    var accessibilityLabel: String {
        guard let podName, !podName.isEmpty else {
            return "Favorite selected pod"
        }

        let operation = isFavorite ? "Remove" : "Add"
        switch placement {
        case .selectedPod:
            return "\(operation) selected pod \(podName) \(isFavorite ? "from" : "to") favorites"
        case .podRow:
            return "\(operation) pod \(podName) \(isFavorite ? "from" : "to") favorites"
        }
    }

    var body: some View {
        RuneIconButton(
            accessibilityLabel,
            systemImage: isFavorite ? "star.fill" : "star",
            isSelected: isFavorite,
            isDisabled: podName == nil,
            selectedTint: .yellow,
            action: action
        )
    }
}

struct FavoritePodPicker: View {
    let title: String
    let pods: [PodSummary]
    let width: CGFloat
    let rowTitle: (PodSummary) -> String
    let rowDetail: (PodSummary) -> String?
    let isFavoritePod: (PodSummary) -> Bool
    let onToggleFavoritePod: (PodSummary) -> Void
    @Binding var selection: String
    /// Fills the width it is offered (down to `width` as a minimum is not
    /// required) instead of a fixed width, for pickers that share a bar.
    var fillsWidth = false
    /// Off where pinning lives only in the picker's list (the log header).
    var showsFavoriteButton = true
    @State private var isPopoverPresented = false

    var body: some View {
        HStack(spacing: 0) {
            if showsFavoriteButton {
                selectedFavoriteButton
            }
            selectedPodButton
        }
        .frame(width: fillsWidth ? nil : width, alignment: .leading)
        .frame(maxWidth: fillsWidth ? .infinity : nil, alignment: .leading)
        .frame(minHeight: RuneUILayoutMetrics.iconButtonSize, alignment: .leading)
        .background(RuneSurfaceBackground(kind: .listRow(isSelected: false)))
        .overlay {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
        }
        .help(selectedPod.map(podHelpText) ?? "Choose a pod in the current namespace.")
    }

    private var selectedFavoriteButton: some View {
        FavoritePodFavoriteActionButton(
            podName: selectedPod?.name,
            isFavorite: selectedPodIsFavorite,
            placement: .selectedPod
        ) {
            if let selectedPod {
                onToggleFavoritePod(selectedPod)
            }
        }
    }

    private var selectedPodButton: some View {
        Button {
            isPopoverPresented = true
        } label: {
            selectedPodButtonLabel
        }
        .buttonStyle(.plain)
        .disabled(sortedPods.isEmpty)
        .popover(isPresented: $isPopoverPresented, arrowEdge: .bottom) {
            podPopover
        }
        .help(selectedPodHelpText)
        .accessibilityLabel(title)
        .accessibilityValue(selectedPodTitle)
    }

    private var selectedPodButtonLabel: some View {
        HStack(spacing: RuneSpacing.xs) {
            Text(selectedPodTitle)
                .runeInterfaceFont(weight: .medium)
                .lineLimit(1)
                .truncationMode(.middle)
                .help(selectedPodHelpText)
            Spacer(minLength: 0)
            Image(systemName: "chevron.up.chevron.down")
                .runeInterfaceFont(relativeSize: -3, weight: .semibold)
                .foregroundStyle(.runeSecondary)
        }
        .padding(.leading, RuneSpacing.xs)
        .padding(.trailing, RuneSpacing.tight)
        .frame(
            width: fillsWidth ? nil : max(width - RuneUILayoutMetrics.iconButtonSize, 140),
            alignment: .leading
        )
        .frame(maxWidth: fillsWidth ? .infinity : nil, alignment: .leading)
        .frame(minHeight: RuneUILayoutMetrics.iconButtonSize, alignment: .leading)
        .contentShape(Rectangle())
    }

    private var podPopover: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.xs) {
            Text(title)
                .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                .foregroundStyle(.runeSecondary)
                .padding(.horizontal, RuneSpacing.inset)
                .padding(.top, RuneSpacing.xs)

            if sortedPods.isEmpty {
                Text("No pods in namespace")
                    .runeText(.micro)
                    .foregroundStyle(.runeSecondary)
                    .padding(RuneSpacing.inset)
            } else {
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                        ForEach(sortedPods) { pod in
                            podRow(pod)
                        }
                    }
                    .padding(RuneSpacing.tight)
                }
                .frame(maxHeight: 260)
            }
        }
        .frame(width: max(width, FavoritePodPickerMetrics.popoverMinimumWidth))
        .runePointerCursor()
    }

    private func podRow(_ pod: PodSummary) -> some View {
        HStack(spacing: RuneSpacing.tight) {
            FavoritePodFavoriteActionButton(
                podName: pod.name,
                isFavorite: isFavoritePod(pod),
                placement: .podRow
            ) {
                onToggleFavoritePod(pod)
            }

            Button {
                selection = pod.id
                isPopoverPresented = false
            } label: {
                HStack(spacing: RuneSpacing.xs) {
                    Image(systemName: pod.id == selection ? "checkmark" : "circle")
                        .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                        .foregroundStyle(pod.id == selection ? RuneTextStyle.accent : .secondary)
                        .frame(width: 14)

                    VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                        Text(rowTitle(pod))
                            .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                            .foregroundStyle(.runePrimary)
                            .lineLimit(1)
                            .truncationMode(.middle)
                            .help(rowTitle(pod))

                        if let detail = rowDetail(pod), !detail.isEmpty {
                            Text(detail)
                                .runeInterfaceFont(relativeSize: -2)
                                .foregroundStyle(.runeSecondary)
                                .lineLimit(1)
                                .truncationMode(.middle)
                                .help(detail)
                        }
                    }

                    Spacer(minLength: 0)
                }
                .padding(.horizontal, RuneSpacing.tight)
                .padding(.vertical, RuneSpacing.xxs)
                .contentShape(RoundedRectangle(
                    cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius,
                    style: .continuous
                ))
            }
            .buttonStyle(.plain)
            .background(RuneSurfaceBackground(kind: .listRow(isSelected: pod.id == selection)))
            .clipShape(RoundedRectangle(
                cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius,
                style: .continuous
            ))
            .help(podHelpText(pod))
            .accessibilityLabel("Select pod \(pod.name)")
        }
    }

    private var selectedPod: PodSummary? {
        FavoritePodPickerPresentation.selectedPod(in: pods, selection: selection)
    }

    private var selectedPodTitle: String {
        selectedPod.map(rowTitle) ?? "No pods in namespace"
    }

    private var selectedPodHelpText: String {
        selectedPod.map(podHelpText) ?? "Choose a pod in the current namespace."
    }

    private var selectedPodIsFavorite: Bool {
        FavoritePodPickerPresentation.selectedFavoriteIcon(
            in: pods,
            selection: selection,
            isFavoritePod: isFavoritePod
        ) == "star.fill"
    }

    private var sortedPods: [PodSummary] {
        FavoritePodPickerPresentation.sortedPods(pods, isFavoritePod: isFavoritePod)
    }

    private func podHelpText(_ pod: PodSummary) -> String {
        if let detail = rowDetail(pod), !detail.isEmpty {
            return "\(pod.namespace)/\(pod.name) - \(detail)"
        }
        return "\(pod.namespace)/\(pod.name)"
    }
}
