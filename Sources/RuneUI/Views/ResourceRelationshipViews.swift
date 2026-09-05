import RuneCore
import SwiftUI

enum ResourceRelationshipLayoutMetrics {
    static let rowSpacing: CGFloat = 6
    static let minimumRowHeight: CGFloat = 34
    static let maximumVisibleRows = 3

    static var maximumListHeight: CGFloat {
        minimumRowHeight * CGFloat(maximumVisibleRows)
            + rowSpacing * CGFloat(maximumVisibleRows - 1)
    }
}

struct ResourceRelationshipSection<Content: View>: View {
    let title: String
    let rowCount: Int?
    @ViewBuilder var content: Content

    init(
        title: String,
        rowCount: Int? = nil,
        @ViewBuilder content: () -> Content
    ) {
        self.title = title
        self.rowCount = rowCount
        self.content = content()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)

            if usesBoundedInnerScroll {
                ScrollView(.vertical) {
                    relationshipRows
                }
                .frame(
                    maxWidth: .infinity,
                    maxHeight: ResourceRelationshipLayoutMetrics.maximumListHeight,
                    alignment: .topLeading
                )
            } else {
                relationshipRows
            }
        }
        .padding(10)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private var usesBoundedInnerScroll: Bool {
        guard let rowCount else { return false }
        return rowCount > ResourceRelationshipLayoutMetrics.maximumVisibleRows
    }

    private var relationshipRows: some View {
        LazyVStack(alignment: .leading, spacing: ResourceRelationshipLayoutMetrics.rowSpacing) {
            content
        }
        .frame(maxWidth: .infinity, alignment: .topLeading)
    }
}

struct RelatedPodsRelationshipSection: View {
    let pods: [PodSummary]
    let open: (PodSummary) -> Void

    var body: some View {
        ResourceRelationshipSection(title: "Related Pods", rowCount: pods.count) {
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
        ResourceRelationshipSection(title: "Related Events", rowCount: events.count) {
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
    static let minimumHeight = ResourceRelationshipLayoutMetrics.minimumRowHeight

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
            .frame(maxWidth: .infinity, minHeight: Self.minimumHeight, alignment: .leading)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .accessibilityLabel(title)
        .accessibilityValue(subtitle)
        .accessibilityHint("Opens the related resource in the inspector")
        .help("Open \(title) — \(subtitle)")
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
        .frame(
            maxWidth: .infinity,
            minHeight: ResourceRelationshipLinkButton.minimumHeight,
            alignment: .leading
        )
        .accessibilityElement(children: .combine)
    }
}
