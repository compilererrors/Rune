import SwiftUI
import RuneCore

struct TerminalSessionControlRow: View {
    let title: String
    let systemImage: String
    let pods: [PodSummary]
    let terminalSessions: [PodTerminalSession]
    let primaryActionTitle: String
    let primaryActionSystemImage: String
    let isPrimaryActionDisabled: Bool
    let isClearDisabled: Bool
    let isFavoritePod: (PodSummary) -> Bool
    let onToggleFavoritePod: (PodSummary) -> Void
    let onPrimaryAction: () -> Void
    let onClear: () -> Void
    @Binding var selection: String

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 10) {
                selectorLabel
                    .frame(width: 96, alignment: .leading)
                picker
                actionButtons
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

    private var actionButtons: some View {
        HStack(spacing: 8) {
            Button(action: onPrimaryAction) {
                Label(primaryActionTitle, systemImage: primaryActionSystemImage)
                    .lineLimit(1)
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
            .frame(width: 112)
            .disabled(isPrimaryActionDisabled)
            .help(primaryActionTitle)

            Button("Clear", action: onClear)
                .buttonStyle(.bordered)
                .controlSize(.small)
                .frame(width: 72)
                .disabled(isClearDisabled)
                .help("Clear active terminal output")
                .keyboardShortcut("k", modifiers: [.command])
        }
        .fixedSize(horizontal: true, vertical: false)
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
