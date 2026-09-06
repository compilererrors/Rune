import RuneCore
import SwiftUI

struct ResourceListFreshnessBadge: View {
    let freshness: RuneResourceListFreshness
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        RuneHeaderCapsule(
            "List: \(ResourceListFreshnessPresentation.text(for: freshness.status))",
            role: .status,
            indicatorColor: ResourceListFreshnessPresentation.color(
                for: freshness.status,
                palette: runeThemePalette
            ),
            helpText: freshness.message,
            accessibilityLabel: "Resource list status: \(ResourceListFreshnessPresentation.text(for: freshness.status))"
        )
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

    static func color(for status: RuneSnapshotFreshnessStatus, palette: RuneThemePalette? = nil) -> Color {
        switch status {
        case .idle: return .secondary
        case .refreshing, .reconnecting: return RuneSemanticColorRole.info.color(in: palette)
        case .live: return RuneSemanticColorRole.success.color(in: palette)
        case .stale: return RuneSemanticColorRole.warning.color(in: palette)
        case .failed: return RuneSemanticColorRole.danger.color(in: palette)
        }
    }
}
