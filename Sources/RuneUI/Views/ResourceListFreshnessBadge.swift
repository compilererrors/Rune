import RuneCore
import SwiftUI

struct ResourceListFreshnessBadge: View {
    let freshness: RuneResourceListFreshness

    var body: some View {
        HStack(spacing: 5) {
            Circle()
                .fill(ResourceListFreshnessPresentation.color(for: freshness.status))
                .frame(width: 7, height: 7)
            Text("List: \(ResourceListFreshnessPresentation.text(for: freshness.status))")
                .font(.caption.weight(.semibold))
                .lineLimit(1)
        }
        .padding(.horizontal, RuneUILayoutMetrics.headerChipHorizontalPadding)
        .frame(height: RuneUILayoutMetrics.headerChipHeight)
        .background(.thinMaterial, in: Capsule())
        .help(freshness.message)
    }
}

enum ResourceListFreshnessPresentation {
    static func text(for status: RuneSnapshotFreshnessStatus) -> String {
        switch status {
        case .idle: return "No data"
        case .refreshing: return "Refreshing"
        case .reconnecting: return "Reconnecting"
        case .live: return "Live"
        case .stale: return "Stale"
        case .failed: return "Failed"
        }
    }

    static func color(for status: RuneSnapshotFreshnessStatus) -> Color {
        switch status {
        case .idle: return .secondary
        case .refreshing: return .blue
        case .reconnecting: return .blue
        case .live: return .green
        case .stale: return .orange
        case .failed: return .red
        }
    }
}
