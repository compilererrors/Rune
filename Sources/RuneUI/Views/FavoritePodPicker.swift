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

            Button {
                isPopoverPresented = true
            } label: {
                HStack(spacing: 8) {
                    Text(selectedPod.map(rowTitle) ?? "No pods in namespace")
                        .lineLimit(1)
                        .truncationMode(.middle)
                    Spacer(minLength: 0)
                    Image(systemName: "chevron.up.chevron.down")
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(.secondary)
                }
                .padding(.leading, 8)
                .padding(.trailing, 7)
                .frame(width: max(width - 30, 140), height: 26, alignment: .leading)
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .disabled(sortedPods.isEmpty)
            .popover(isPresented: $isPopoverPresented, arrowEdge: .bottom) {
                podPopover
            }
            .accessibilityLabel(title)
            .accessibilityValue(selectedPod.map(rowTitle) ?? "No pods in namespace")
        }
        .frame(width: width, height: 26, alignment: .leading)
        .background(RuneSurfaceBackground(kind: .listRow(isSelected: false)))
        .overlay {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
        }
        .help(selectedPod.map(podHelpText) ?? "Choose a pod in the current namespace.")
    }

    private var selectedFavoriteButton: some View {
        Button {
            if let selectedPod {
                onToggleFavoritePod(selectedPod)
            }
        } label: {
            Image(systemName: FavoritePodPickerPresentation.selectedFavoriteIcon(
                in: pods,
                selection: selection,
                isFavoritePod: isFavoritePod
            ))
                .frame(width: 30, height: 26)
                .foregroundStyle(selectedPod.map(isFavoritePod) == true ? Color.yellow : Color.secondary)
                .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .disabled(selectedPod == nil)
        .help(selectedPod.map(isFavoritePod) == true ? "Remove pod favorite" : "Favorite selected pod")
        .accessibilityLabel(selectedPod.map(isFavoritePod) == true ? "Remove Pod Favorite" : "Favorite Pod")
    }

    private var podPopover: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.caption.weight(.semibold))
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
    }

    private func podRow(_ pod: PodSummary) -> some View {
        HStack(spacing: 6) {
            Button {
                onToggleFavoritePod(pod)
            } label: {
                Image(systemName: isFavoritePod(pod) ? "star.fill" : "star")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(isFavoritePod(pod) ? Color.yellow : Color.secondary)
                    .frame(width: 24, height: 24)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .help(isFavoritePod(pod) ? "Remove pod favorite" : "Favorite pod")
            .accessibilityLabel(isFavoritePod(pod) ? "Remove Pod Favorite" : "Favorite Pod")

            Button {
                selection = pod.id
                isPopoverPresented = false
            } label: {
                HStack(spacing: 8) {
                    Image(systemName: pod.id == selection ? "checkmark" : "circle")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(pod.id == selection ? Color.accentColor : Color.secondary.opacity(0.55))
                        .frame(width: 14)

                    VStack(alignment: .leading, spacing: 2) {
                        Text(rowTitle(pod))
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.primary)
                            .lineLimit(1)
                            .truncationMode(.middle)

                        if let detail = rowDetail(pod), !detail.isEmpty {
                            Text(detail)
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                                .truncationMode(.middle)
                        }
                    }

                    Spacer(minLength: 0)
                }
                .padding(.horizontal, 7)
                .padding(.vertical, 5)
                .contentShape(RoundedRectangle(cornerRadius: 7, style: .continuous))
            }
            .buttonStyle(.plain)
            .background(RuneSurfaceBackground(kind: .listRow(isSelected: pod.id == selection)))
            .clipShape(RoundedRectangle(cornerRadius: 7, style: .continuous))
            .help(podHelpText(pod))
            .accessibilityLabel("Select pod \(pod.name)")
        }
    }

    private var selectedPod: PodSummary? {
        FavoritePodPickerPresentation.selectedPod(in: pods, selection: selection)
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
