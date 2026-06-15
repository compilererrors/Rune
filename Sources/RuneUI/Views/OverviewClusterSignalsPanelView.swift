import SwiftUI
import RuneCore

enum OverviewInsightPanelID: Hashable {
    case unhealthy
    case gitOps
    case incidents
    case dependencies
}

struct OverviewClusterSignalsPanelView: View {
    let unhealthy: [OverviewSignalItem]
    let gitOpsRollups: [OverviewGitOpsRollupItem]
    let incidents: [OverviewSignalItem]
    let dependencies: [OverviewDependencyItem]
    @Binding var expandedPanels: Set<OverviewInsightPanelID>
    let onOpenSignal: (OverviewSignalItem) -> Void
    let onOpenGitOpsRollup: (OverviewGitOpsRollupItem) -> Void
    let onOpenDependency: (OverviewDependencyItem) -> Void
    @AppStorage(RuneSettingsKeys.showHoverTooltips) private var showHoverTooltips = true

    private var activeCount: Int {
        unhealthy.count + gitOpsRollups.count + incidents.count + dependencies.count
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 8) {
                Image(systemName: activeCount == 0 ? "checkmark.seal" : "exclamationmark.triangle")
                    .foregroundStyle(activeCount == 0 ? Color.secondary : Color.orange)
                    .frame(width: 18)
                Text("Cluster Signals")
                    .font(.headline.weight(.semibold))
                Spacer(minLength: 8)
                Text(activeCount == 0 ? "Clear" : "\(activeCount) item\(activeCount == 1 ? "" : "s")")
                    .font(.caption.weight(.bold))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .padding(.horizontal, 8)
                    .frame(height: 22)
                    .background(Color.secondary.opacity(0.12), in: Capsule())
            }
            .runeHelp(overviewClusterSignalsHelp, enabled: showHoverTooltips)

            VStack(spacing: 0) {
                signalSection(
                    id: .unhealthy,
                    title: "Unhealthy",
                    symbol: "exclamationmark.octagon",
                    emptyText: "No unhealthy pods or deployments in the current snapshot.",
                    items: unhealthy
                )

                panelDivider

                gitOpsSection(id: .gitOps, items: gitOpsRollups)

                panelDivider

                signalSection(
                    id: .incidents,
                    title: "Incident Timeline",
                    symbol: "waveform.path.ecg",
                    emptyText: "No warning events in the current snapshot.",
                    items: incidents
                )

                panelDivider

                dependencySection(id: .dependencies, items: dependencies)
            }
        }
        .frame(maxWidth: .infinity, alignment: .topLeading)
        .runePanelCard(padding: 12)
    }

    private var panelDivider: some View {
        Divider()
            .overlay(Color.white.opacity(0.04))
            .padding(.leading, 28)
    }

    private func signalSection(
        id: OverviewInsightPanelID,
        title: String,
        symbol: String,
        emptyText: String,
        items: [OverviewSignalItem]
    ) -> some View {
        let isExpanded = expandedPanels.contains(id)
        let severity = items.first?.severity ?? .info
        let summary = items.first.map { $0.title + " - " + $0.detail } ?? emptyText

        return VStack(alignment: .leading, spacing: 8) {
            disclosureHeader(
                id: id,
                title: title,
                symbol: symbol,
                badge: items.isEmpty ? "Clean" : "\(items.count)",
                summary: summary,
                severity: severity,
                isExpanded: isExpanded,
                help: overviewInsightHelp(for: id)
            )

            if isExpanded {
                if items.isEmpty {
                    emptyTextView(emptyText)
                } else {
                    VStack(alignment: .leading, spacing: 6) {
                        ForEach(items) { item in
                            signalRow(item) {
                                onOpenSignal(item)
                            }
                            .runeHelp(overviewSignalRowHelp(item), enabled: showHoverTooltips)
                        }
                    }
                    .padding(.leading, 28)
                }
            }
        }
        .padding(.vertical, 8)
    }

    private func gitOpsSection(
        id: OverviewInsightPanelID,
        items: [OverviewGitOpsRollupItem]
    ) -> some View {
        let isExpanded = expandedPanels.contains(id)
        let summary = items.first?.detail ?? "No Flux or ArgoCD resources loaded."
        let severity = items.first?.severity ?? .info

        return VStack(alignment: .leading, spacing: 8) {
            disclosureHeader(
                id: id,
                title: "GitOps",
                symbol: "arrow.triangle.2.circlepath",
                badge: items.isEmpty ? "None" : "\(items.count)",
                summary: summary,
                severity: severity,
                isExpanded: isExpanded,
                help: overviewInsightHelp(for: id)
            )

            if isExpanded {
                if items.isEmpty {
                    emptyTextView("No Flux or ArgoCD resources loaded.")
                } else {
                    VStack(alignment: .leading, spacing: 6) {
                        ForEach(items) { item in
                            gitOpsRow(item) {
                                onOpenGitOpsRollup(item)
                            }
                            .runeHelp(overviewGitOpsRowHelp(item), enabled: showHoverTooltips)
                        }
                    }
                    .padding(.leading, 28)
                }
            }
        }
        .padding(.vertical, 8)
    }

    private func dependencySection(
        id: OverviewInsightPanelID,
        items: [OverviewDependencyItem]
    ) -> some View {
        let isExpanded = expandedPanels.contains(id)
        let summary = items.first.map { $0.source + " to " + $0.target } ?? "No service or workload relationships loaded."

        return VStack(alignment: .leading, spacing: 8) {
            disclosureHeader(
                id: id,
                title: "Dependency Map",
                symbol: "point.3.connected.trianglepath.dotted",
                badge: items.isEmpty ? "None" : "\(items.count)",
                summary: summary,
                severity: .info,
                isExpanded: isExpanded,
                help: overviewInsightHelp(for: id)
            )

            if isExpanded {
                if items.isEmpty {
                    emptyTextView("No service or workload relationships loaded.")
                } else {
                    VStack(alignment: .leading, spacing: 6) {
                        ForEach(items) { item in
                            dependencyRow(item) {
                                onOpenDependency(item)
                            }
                            .runeHelp(overviewDependencyRowHelp(item), enabled: showHoverTooltips)
                        }
                    }
                    .padding(.leading, 28)
                }
            }
        }
        .padding(.vertical, 8)
    }

    private func disclosureHeader(
        id: OverviewInsightPanelID,
        title: String,
        symbol: String,
        badge: String,
        summary: String,
        severity: OverviewSignalSeverity,
        isExpanded: Bool,
        help: String
    ) -> some View {
        Button {
            if isExpanded {
                expandedPanels.remove(id)
            } else {
                expandedPanels.insert(id)
            }
        } label: {
            HStack(alignment: .center, spacing: 8) {
                Image(systemName: isExpanded ? "chevron.down" : "chevron.right")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.tertiary)
                    .frame(width: 10)
                Image(systemName: symbol)
                    .foregroundStyle(signalColor(severity))
                    .frame(width: 18)
                VStack(alignment: .leading, spacing: 2) {
                    Text(title)
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.primary)
                        .lineLimit(1)
                    Text(summary)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
                Spacer(minLength: 8)
                Text(badge)
                    .font(.caption.weight(.bold))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .padding(.horizontal, 8)
                    .frame(height: 22)
                    .background(Color.secondary.opacity(0.12), in: Capsule())
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .runeHelp(help, enabled: showHoverTooltips)
    }

    private func emptyTextView(_ text: String) -> some View {
        Text(text)
            .font(.caption)
            .foregroundStyle(.secondary)
            .fixedSize(horizontal: false, vertical: true)
            .padding(.leading, 28)
    }

    private func signalRow(_ item: OverviewSignalItem, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack(alignment: .top, spacing: 7) {
                Circle()
                    .fill(signalColor(item.severity))
                    .frame(width: 7, height: 7)
                    .padding(.top, 5)
                VStack(alignment: .leading, spacing: 1) {
                    HStack(spacing: 6) {
                        Text(item.title)
                            .font(.caption.weight(.semibold))
                            .lineLimit(1)
                        Text(item.badge)
                            .font(.caption2.weight(.bold))
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                    Text(item.detail)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(2)
                }
                Spacer(minLength: 6)
                Image(systemName: "chevron.right")
                    .font(.caption2.weight(.semibold))
                    .foregroundStyle(.tertiary)
                    .padding(.top, 4)
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }

    private func gitOpsRow(_ item: OverviewGitOpsRollupItem, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack(alignment: .top, spacing: 7) {
                Circle()
                    .fill(signalColor(item.severity))
                    .frame(width: 7, height: 7)
                    .padding(.top, 5)
                VStack(alignment: .leading, spacing: 1) {
                    HStack(spacing: 6) {
                        Text(item.title)
                            .font(.caption.weight(.semibold))
                            .lineLimit(1)
                        Text(item.badge)
                            .font(.caption2.weight(.bold))
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                    Text(item.detail)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(2)
                }
                Spacer(minLength: 6)
                Image(systemName: "line.3.horizontal.decrease.circle")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.tertiary)
                    .padding(.top, 3)
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }

    private func dependencyRow(_ item: OverviewDependencyItem, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack(alignment: .top, spacing: 7) {
                Circle()
                    .fill(Color.secondary.opacity(0.75))
                    .frame(width: 7, height: 7)
                    .padding(.top, 5)
                VStack(alignment: .leading, spacing: 2) {
                    HStack(spacing: 6) {
                        Text(item.source)
                            .font(.caption.weight(.semibold))
                            .lineLimit(1)
                        Image(systemName: "chevron.right")
                            .font(.caption2.weight(.bold))
                            .foregroundStyle(.tertiary)
                        Text(item.target)
                            .font(.caption.weight(.semibold))
                            .lineLimit(1)
                    }
                    Text(item.relation + " - " + item.detail)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
                Spacer(minLength: 6)
                Image(systemName: "chevron.right")
                    .font(.caption2.weight(.semibold))
                    .foregroundStyle(.tertiary)
                    .padding(.top, 4)
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }

    private func signalColor(_ severity: OverviewSignalSeverity) -> Color {
        switch severity {
        case .critical:
            return .red
        case .warning:
            return .orange
        case .info:
            return .secondary
        }
    }

    private var overviewClusterSignalsHelp: String {
        "Cluster Signals is a triage summary from the current snapshot. Unhealthy shows current resource health, GitOps summarizes Flux and ArgoCD controllers, Incident Timeline promotes warning events, and Dependency Map shows loaded service/workload relationships."
    }

    private func overviewInsightHelp(for id: OverviewInsightPanelID) -> String {
        switch id {
        case .unhealthy:
            return "Current-state health signals from pods and deployments, such as failed status, readiness gaps, or restarts. These are not raw Kubernetes Events."
        case .gitOps:
            return "Flux and ArgoCD rollups from loaded custom resources. Open a row to focus the operator browser on all GitOps, Flux, ArgoCD, or unhealthy GitOps resources."
        case .incidents:
            return "Warning Kubernetes Events promoted into a short incident timeline. Use Events for the full raw event list."
        case .dependencies:
            return "Loaded relationships between Services, Deployments, and Pods. This is a snapshot-based map, not a live network trace."
        }
    }

    private func overviewSignalRowHelp(_ item: OverviewSignalItem) -> String {
        let targetText = item.target.map { "\($0.kind.singularTypeName) \($0.name)" } ?? "the related resource"
        return "Open \(targetText). This row is derived from current health state or a warning Event, depending on the section."
    }

    private func overviewGitOpsRowHelp(_ item: OverviewGitOpsRollupItem) -> String {
        "Open the operator browser focused on \(item.title). This rollup is derived from loaded Flux and ArgoCD custom resources."
    }

    private func overviewDependencyRowHelp(_ item: OverviewDependencyItem) -> String {
        let targetText = item.primaryTarget.map { "\($0.kind.singularTypeName) \($0.name)" } ?? item.target
        return "Open \(targetText). This row explains a loaded relationship: \(item.source) \(item.relation) \(item.target)."
    }
}
