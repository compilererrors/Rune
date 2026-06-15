import RuneCore
import SwiftUI

struct ResourceRelationshipSection<Content: View>: View {
    let title: String
    @ViewBuilder var content: Content

    init(title: String, @ViewBuilder content: () -> Content) {
        self.title = title
        self.content = content()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            VStack(alignment: .leading, spacing: 6) {
                content
            }
        }
        .padding(10)
        .background(RuneSurfaceBackground(kind: .editor))
    }
}

struct RelatedPodsRelationshipSection: View {
    let pods: [PodSummary]
    let open: (PodSummary) -> Void

    var body: some View {
        ResourceRelationshipSection(title: "Related Pods") {
            ForEach(pods) { pod in
                ResourceRelationshipLinkButton(
                    title: pod.name,
                    subtitle: "\(pod.namespace) · \(pod.status)",
                    symbol: "cube.box"
                ) {
                    open(pod)
                }
            }
        }
    }
}

struct RelatedEventsRelationshipSection: View {
    let events: [EventSummary]
    let open: (EventSummary) -> Void

    var body: some View {
        ResourceRelationshipSection(title: "Related Events") {
            ForEach(events) { event in
                ResourceRelationshipLinkButton(
                    title: event.reason,
                    subtitle: subtitle(for: event),
                    symbol: event.type.caseInsensitiveCompare("Warning") == .orderedSame ? "exclamationmark.triangle" : "clock.badge"
                ) {
                    open(event)
                }
            }
        }
    }

    private func subtitle(for event: EventSummary) -> String {
        let timestamp = event.lastTimestamp?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if timestamp.isEmpty {
            return "\(event.type) · \(event.objectName)"
        }
        return "\(event.type) · \(event.objectName) · \(timestamp)"
    }
}

struct ResourceRelationshipLinkButton: View {
    let title: String
    let subtitle: String
    let symbol: String
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            HStack(spacing: 8) {
                Image(systemName: symbol)
                    .frame(width: 16)
                    .foregroundStyle(.secondary)
                VStack(alignment: .leading, spacing: 2) {
                    Text(title)
                        .font(.caption.weight(.semibold))
                        .lineLimit(1)
                    Text(subtitle)
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
                Spacer(minLength: 0)
                Image(systemName: "chevron.right")
                    .font(.caption2.weight(.semibold))
                    .foregroundStyle(.tertiary)
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .help("Open \(title)")
    }
}

struct ResourceRelationshipEmptyRow: View {
    let title: String
    let subtitle: String

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: "tray")
                .frame(width: 16)
                .foregroundStyle(.secondary)
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.caption.weight(.semibold))
                Text(subtitle)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }
            Spacer(minLength: 0)
        }
        .padding(.vertical, 4)
    }
}
