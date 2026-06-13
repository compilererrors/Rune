import SwiftUI
import RuneCore

struct TerminalSessionTabPresentation: Hashable, Sendable {
    let primaryTitle: String
    let secondaryTitle: String?
    let accessibilityLabel: String
    let helpText: String

    static func make(session: PodTerminalSession, number: Int) -> TerminalSessionTabPresentation {
        let trimmedContainer = session.containerName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let containerTitle = trimmedContainer.isEmpty ? nil : trimmedContainer
        let statusTitle = TerminalStatusStyling.title(session.status)
        var helpParts = [
            "\(session.namespace)/\(session.podName)",
            "Shell: \(session.shell)",
            "Status: \(statusTitle)"
        ]
        if let containerTitle {
            helpParts.insert("Container: \(containerTitle)", at: 1)
        }
        if let exitCode = session.lastExitCode {
            helpParts.append("Last exit code: \(exitCode)")
        }

        let accessibility = [
            "\(number) \(session.podName)",
            containerTitle.map { "container \($0)" },
            statusTitle
        ].compactMap { $0 }.joined(separator: ", ")

        return TerminalSessionTabPresentation(
            primaryTitle: "\(number) \(session.podName)",
            secondaryTitle: containerTitle,
            accessibilityLabel: accessibility,
            helpText: helpParts.joined(separator: " - ")
        )
    }
}

struct TerminalSessionTabBar: View {
    let sessions: [PodTerminalSession]
    let activeSessionID: String?
    let isComposingNewSession: Bool
    let selectedPod: PodSummary?
    let canApplyMutations: Bool
    @Binding var selectedShellPodID: String
    let onStartSession: (PodSummary) -> Void
    let onSelectSession: (String) -> Void
    let onCloseSession: (String) -> Void
    let onComposeNewSession: () -> Void

    var body: some View {
        HStack(spacing: 0) {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 6) {
                    ForEach(Array(sessions.enumerated()), id: \.element.id) { index, session in
                        tab(session, number: index + 1)
                    }

                    if isComposingNewSession || sessions.isEmpty {
                        draftTab(number: sessions.count + 1)
                    }
                }
                .padding(.horizontal, 6)
            }
            .overlay(alignment: .trailing) {
                Rectangle()
                    .fill(Color.primary.opacity(0.10))
                    .frame(width: 1, height: 22)
            }

            Button(action: onComposeNewSession) {
                Image(systemName: "plus")
                    .font(.caption.weight(.semibold))
                    .frame(width: 40, height: 28)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .controlSize(.small)
            .disabled(!canApplyMutations || isComposingNewSession)
            .background(
                RoundedRectangle(cornerRadius: 6, style: .continuous)
                    .fill(Color(nsColor: .controlBackgroundColor).opacity(0.34))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 6, style: .continuous)
                    .stroke(Color.primary.opacity(0.16), lineWidth: 1)
            )
            .padding(.leading, 6)
            .help("Prepare a new shell tab. Pick a pod, then connect.")
            .accessibilityLabel("New Shell")
        }
        .frame(height: 36)
        .padding(.horizontal, 6)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private func draftTab(number: Int) -> some View {
        Button(action: onComposeNewSession) {
            HStack(spacing: 6) {
                Image(systemName: "plus.circle.fill")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(Color.accentColor)
                Text("\(number) New Shell")
                    .font(.caption.weight(.semibold))
                    .lineLimit(1)
                if let selectedPod {
                    Text(selectedPod.name)
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                } else {
                    Text("Ready")
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(.secondary)
                }
            }
            .frame(width: 216, height: 28, alignment: .leading)
            .padding(.horizontal, 8)
            .contentShape(RoundedRectangle(cornerRadius: 6, style: .continuous))
        }
        .buttonStyle(.plain)
        .disabled(!canApplyMutations)
        .background(tabBackground(isActive: true, isDraft: true))
        .overlay(tabBorder(isActive: true, isDraft: true))
        .overlay(alignment: .bottom) {
            Rectangle()
                .fill(Color.accentColor)
                .frame(height: 2)
        }
        .help(selectedPod.map { "New shell tab for \($0.namespace)/\($0.name)" } ?? "Select a pod to connect this new shell tab")
    }

    private func tab(_ session: PodTerminalSession, number: Int) -> some View {
        let presentation = TerminalSessionTabPresentation.make(session: session, number: number)
        return ZStack(alignment: .trailing) {
            HStack(spacing: 6) {
                TerminalStatusDot(color: TerminalStatusStyling.color(session.status))
                Text(presentation.primaryTitle)
                    .font(.caption.weight(.semibold))
                    .lineLimit(1)
                if let secondaryTitle = presentation.secondaryTitle {
                    Text(secondaryTitle)
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
                if session.status != .connected {
                    Text(TerminalStatusStyling.title(session.status))
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(TerminalStatusStyling.color(session.status))
                        .lineLimit(1)
                }
                Spacer(minLength: 0)
            }
            .padding(.leading, 8)
            .padding(.trailing, 28)

            Button {
                onCloseSession(session.id)
            } label: {
                Image(systemName: "xmark")
                    .font(.caption2.weight(.bold))
                    .foregroundStyle(.secondary)
                    .frame(width: 28, height: 28)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .help("Close terminal tab")
        }
        .frame(width: 216, height: 28, alignment: .leading)
        .contentShape(RoundedRectangle(cornerRadius: 6, style: .continuous))
        .onTapGesture {
            select(session)
        }
        .background(tabBackground(isActive: session.id == activeSessionID, isDraft: false))
        .overlay(tabBorder(isActive: session.id == activeSessionID, isDraft: false))
        .overlay(alignment: .bottom) {
            if session.id == activeSessionID {
                Rectangle()
                    .fill(Color.accentColor)
                    .frame(height: 2)
            }
        }
        .help(presentation.helpText)
        .accessibilityElement(children: .combine)
        .accessibilityLabel(presentation.accessibilityLabel)
        .accessibilityAddTraits(session.id == activeSessionID ? [.isSelected, .isButton] : .isButton)
    }

    private func select(_ session: PodTerminalSession) {
        selectedShellPodID = "\(session.namespace)/\(session.podName)"
        onSelectSession(session.id)
    }

    private func tabBackground(isActive: Bool, isDraft: Bool) -> some View {
        RoundedRectangle(cornerRadius: 6, style: .continuous)
            .fill(
                isActive
                    ? Color.accentColor.opacity(isDraft ? 0.18 : 0.20)
                    : Color(nsColor: .controlBackgroundColor).opacity(0.34)
            )
    }

    private func tabBorder(isActive: Bool, isDraft: Bool) -> some View {
        RoundedRectangle(cornerRadius: 6, style: .continuous)
            .stroke(
                isActive
                    ? Color.accentColor.opacity(isDraft ? 0.58 : 0.50)
                    : Color.primary.opacity(0.16),
                lineWidth: 1
            )
    }

}
