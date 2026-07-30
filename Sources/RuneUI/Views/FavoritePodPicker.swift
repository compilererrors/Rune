import SwiftUI
import RuneCore

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

    static func rowIcon(for pod: PodSummary, selection: String, isFavoritePod: (PodSummary) -> Bool) -> String {
        if pod.id == selection { return "checkmark" }
        return isFavoritePod(pod) ? "star.fill" : "circle"
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
    @State private var isPopoverPresented = false

    var body: some View {
        HStack(spacing: 0) {
            selectedFavoriteButton
            selectedPodButton
        }
        .frame(width: width, alignment: .leading)
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
        .accessibilityLabel(title)
        .accessibilityValue(selectedPodTitle)
    }

    private var selectedPodButtonLabel: some View {
        HStack(spacing: 8) {
            Text(selectedPodTitle)
                .runeInterfaceFont(weight: .medium)
                .lineLimit(1)
                .truncationMode(.middle)
            Spacer(minLength: 0)
            Image(systemName: "chevron.up.chevron.down")
                .runeInterfaceFont(relativeSize: -3, weight: .semibold)
                .foregroundStyle(.secondary)
        }
        .padding(.leading, 8)
        .padding(.trailing, 7)
        .frame(
            width: max(width - RuneUILayoutMetrics.iconButtonSize, 140),
            alignment: .leading
        )
        .frame(minHeight: RuneUILayoutMetrics.iconButtonSize, alignment: .leading)
        .contentShape(Rectangle())
    }

    private var podPopover: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                .foregroundStyle(.secondary)
                .padding(.horizontal, 10)
                .padding(.top, 8)

            if sortedPods.isEmpty {
                Text("No pods in namespace")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .padding(10)
            } else {
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 3) {
                        ForEach(sortedPods) { pod in
                            podRow(pod)
                        }
                    }
                    .padding(6)
                }
                .frame(maxHeight: 260)
            }
        }
        .frame(width: max(width, 280))
        .runePointerCursor()
    }

    private func podRow(_ pod: PodSummary) -> some View {
        HStack(spacing: 6) {
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
                HStack(spacing: 8) {
                    Image(systemName: pod.id == selection ? "checkmark" : "circle")
                        .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                        .foregroundStyle(pod.id == selection ? Color.accentColor : Color.secondary.opacity(0.55))
                        .frame(width: 14)

                    VStack(alignment: .leading, spacing: 2) {
                        Text(rowTitle(pod))
                            .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                            .foregroundStyle(.primary)
                            .lineLimit(1)
                            .truncationMode(.middle)

                        if let detail = rowDetail(pod), !detail.isEmpty {
                            Text(detail)
                                .runeInterfaceFont(relativeSize: -2)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                                .truncationMode(.middle)
                        }
                    }

                    Spacer(minLength: 0)
                }
                .padding(.horizontal, 7)
                .padding(.vertical, 5)
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
