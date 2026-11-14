import SwiftUI
import RuneCore

struct OverviewRecentEventsPanelView: View {
    @Environment(\.runeThemePalette) private var runeThemePalette
    let events: [EventSummary]
    let onOpenEventSource: (EventSummary) -> Void
    @AppStorage(RuneSettingsKeys.showHoverTooltips) private var showHoverTooltips = true

    var body: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.xs) {
            RuneSectionHeader(
                "Recent Events",
                systemImage: "bolt.badge.clock",
                tint: events.contains(where: { $0.type.lowercased() == "warning" })
                    ? RuneSemanticColorRole.warning.color(in: runeThemePalette)
                    : (runeThemePalette?.secondaryText ?? Color.secondary)
            ) {
                RuneHeaderCapsule(
                    events.isEmpty ? "None" : "\(min(events.count, 8)) shown",
                    role: .value
                )
            }
                .runeHelp(overviewRecentEventsHelp, enabled: showHoverTooltips)

            if events.isEmpty {
                RuneContentStateView(
                    .empty(
                        title: "No events loaded",
                        message: "Events from the current snapshot will appear here."
                    ),
                    variant: .inline
                )
            } else {
                ForEach(Array(events.prefix(8))) { event in
                    Button {
                        onOpenEventSource(event)
                    } label: {
                        HStack(alignment: .top, spacing: RuneSpacing.xs) {
                            Image(systemName: event.type.lowercased() == "warning" ? "exclamationmark.triangle.fill" : "checkmark.circle.fill")
                                .foregroundStyle(event.type.lowercased() == "warning" ? RuneTextStyle.warning : .success)
                                .frame(width: 16)

                            VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                                if let ts = event.lastTimestamp?.trimmingCharacters(in: .whitespacesAndNewlines), !ts.isEmpty {
                                    Text(ts)
                                        .runeText(.micro, design: .monospaced)
                                        .foregroundStyle(.runeTertiary)
                                }
                                Text(event.reason + " - " + event.objectName)
                                    .runeText(.microEmphasis)
                                    .foregroundStyle(.runePrimary)
                                    .multilineTextAlignment(.leading)
                                Text(event.message)
                                    .runeText(.micro)
                                    .foregroundStyle(.runeSecondary)
                                    .multilineTextAlignment(.leading)
                                    .lineLimit(2)
                            }

                            Spacer(minLength: 0)

                            Image(systemName: "chevron.right")
                                .runeText(.microEmphasis)
                                .foregroundStyle(.runeTertiary)
                                .padding(.top, RuneSpacing.hairline)
                        }
                        .padding(.vertical, RuneSpacing.xxs)
                        .padding(.horizontal, RuneSpacing.hairline)
                        .contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                    .runeHelp(overviewEventRowHelp(event), enabled: showHoverTooltips)
                }
            }
        }
        .runePanelCard()
    }

    private var overviewRecentEventsHelp: String {
        "Recent Events shows raw Kubernetes Event objects from the current snapshot. Cluster Signals may also show warning events in Incident Timeline when they are useful for triage."
    }

    private func overviewEventRowHelp(_ event: EventSummary) -> String {
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if kind.isEmpty {
            return "Shows details in the inspector. Use the action button to open the involved resource when listed."
        }
        return "Shows details in the inspector. \"Go to ...\" switches section and selects \(kind) \(event.objectName) when present."
    }
}
