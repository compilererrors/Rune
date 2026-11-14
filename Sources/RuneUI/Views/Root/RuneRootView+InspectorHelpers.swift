import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    func contextUsageBadge(label: String, value: String) -> some View {
        RuneHeaderCapsule(
            "\(label): \(value)",
            role: .value,
            systemImage: label == "CPU" ? "cpu" : "memorychip",
            accessibilityLabel: "\(label) usage: \(value)"
        )
    }

    func contextUsageValue(_ value: Int?) -> String {
        if let value {
            return "\(value)%"
        }
        return viewModel.state.isLoading ? "..." : "n/a"
    }

    func healthBadge(label: String, value: Int, tone: RuneBadgeTone) -> some View {
        HStack(spacing: RuneSpacing.tight) {
            RuneStatusDot(tone.tint(in: activeAppearanceTheme.palette) ?? activeAppearanceTheme.palette?.secondaryText ?? Color.secondary)
            Text(label + ": \(value)")
        }
        .runeBadgeSurface(tone)
    }

    func inspectorActionButtonRow<Content: View>(@ViewBuilder content: () -> Content) -> some View {
        RuneInspectorActionRow(content: content)
    }

    func normalizedCopyValue(_ value: String) -> String? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, trimmed != "—", trimmed.lowercased() != "n/a" else {
            return nil
        }
        return trimmed
    }

    func copyToClipboard(_ value: String) {
        guard let copyValue = normalizedCopyValue(value) else { return }
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(copyValue, forType: .string)
    }

    func copySelectedTextFromFocusedTextView() {
        guard let textView = NSApp.keyWindow?.firstResponder as? NSTextView else { return }
        let text = textView.string as NSString
        let selectedRange = textView.selectedRange()
        guard selectedRange.length > 0, NSMaxRange(selectedRange) <= text.length else { return }
        let lineRange = text.lineRange(for: selectedRange)
        let selectedLines = text.substring(with: lineRange).trimmingCharacters(in: .newlines)
        copyToClipboard(selectedLines)
    }

    @ViewBuilder
    func copyMenuItem(value: String, label: String) -> some View {
        if normalizedCopyValue(value) != nil {
            Button("Copy \(label)") {
                copyToClipboard(value)
            }
        }
    }

    var detailRefreshButton: some View {
        RuneIconButton(
            "Refresh inspector",
            systemImage: "arrow.clockwise"
        ) {
            refreshDetailPane()
        }
    }

    func refreshDetailPane() {
        let route = RuneInspectorRefreshRouting.route(
            section: viewModel.state.selectedSection,
            workloadKind: viewModel.state.selectedWorkloadKind,
            podTab: podInspectorTab,
            deploymentTab: deploymentInspectorTab,
            serviceTab: serviceInspectorTab,
            genericTab: genericResourceManifestTab,
            helmTab: helmInspectorTab,
            helmMode: selectedHelmInspectorMode
        )

        switch route {
        case .currentView:
            viewModel.refreshCurrentView(debounced: false)
        case .resourceInspector:
            viewModel.refreshResourceInspectorOnly()
        case .logs:
            viewModel.reloadLogsForSelection()
        case .helmInspector:
            viewModel.refreshSelectedHelmInspector()
        }
    }

    func deploymentReplicaStatusText(_ deployment: DeploymentSummary) -> String {
        if deployment.desiredReplicas == 0 {
            return "Scaled to zero"
        }
        return "\(deployment.readyReplicas) of \(deployment.desiredReplicas) ready"
    }

    func deploymentScaleIsDirty(_ deployment: DeploymentSummary) -> Bool {
        viewModel.scaleReplicaInput != deployment.desiredReplicas
    }

    /// Facts first as aligned grids, then the relationships; no cards.
    func podOverviewSection(pod: PodSummary) -> some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorGroupSpacing) {
            podInspectorCoreInfo(pod)

            RuneFactGrid("Placement") {
                if let node = viewModel.selectedPodRelatedNode {
                    RuneInspectorInfoRow("Node", copyValue: node.name, onCopy: copyToClipboard) {
                        RuneFactLink(node.name, help: "Show this node in Nodes.") {
                            viewModel.openPodRelatedNode(node)
                        }
                    }
                } else {
                    podOverviewRow(title: "Node", value: pod.nodeName ?? "—")
                }
                podOverviewRow(title: "Pod IP", value: pod.podIP ?? "—")
                podOverviewRow(title: "Host IP", value: pod.hostIP ?? "—")
                podOverviewRow(title: "QoS class", value: pod.qosClass ?? "—")
                if let containers = pod.containerNamesLine, !containers.isEmpty {
                    RuneInspectorInfoRow("Containers", copyValue: containers, onCopy: copyToClipboard) {
                        Text(containers)
                            .runeText(.code)
                            .textSelection(.enabled)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
            }

            let relatedEvents = viewModel.selectedPodRelatedEvents
            if !relatedEvents.isEmpty {
                RelatedEventsRelationshipSection(events: relatedEvents, open: viewModel.openRelatedEvent)
            }
        }
    }

    /// The pod's state as one fact grid. Status itself is the header pill.
    func podInspectorCoreInfo(_ pod: PodSummary) -> some View {
        RuneFactGrid("Status") {
            if pod.statusReason != nil {
                inspectorInfoRow("Phase", value: pod.status)
            }
            podOverviewRow(title: "Ready", value: pod.containersReady ?? "—")
            podOverviewRow(title: "Restarts", value: "\(pod.totalRestarts)")
            podOverviewRow(title: "Age", value: pod.ageDescription)
            podOverviewRow(title: "CPU", value: pod.cpuDisplay)
            podOverviewRow(title: "Memory", value: pod.memoryDisplay)
        }
    }

    func podOverviewRow(title: String, value: String) -> some View {
        inspectorInfoRow(title, value: value)
    }

    /// "Kind · namespace", or the kind alone for cluster-scoped objects.
    func inspectorSubtitle(_ kind: String, namespace: String?) -> String {
        let namespace = namespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return namespace.isEmpty ? kind : "\(kind) · \(namespace)"
    }

    /// One fact: the value in body text, copy on hover or from the context menu.
    func inspectorInfoRow(_ title: String, value: String) -> some View {
        RuneInspectorInfoRow(
            title,
            copyValue: normalizedCopyValue(value) == nil ? nil : value,
            onCopy: copyToClipboard
        ) {
            Text(value)
                .runeText(.body)
                .textSelection(.enabled)
                .fixedSize(horizontal: false, vertical: true)
        }
    }

    func deploymentOverviewSection(deployment: DeploymentSummary) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.sm) {
            WorkloadReplicaScaleControlsView(
                label: "Replicas",
                isDirty: deploymentScaleIsDirty(deployment),
                canMutate: viewModel.canApplyClusterMutations,
                replicas: $viewModel.scaleReplicaInput,
                action: viewModel.requestScaleSelectedDeployment
            )

            Divider()
                .opacity(0.45)

            let relatedReplicaSets = viewModel.selectedDeploymentRelatedReplicaSets
            let shouldOfferReplicaSetHistoryLoad = simpleMode && relatedReplicaSets.isEmpty
            if !relatedReplicaSets.isEmpty || shouldOfferReplicaSetHistoryLoad {
                let historicalReplicaSetCount = relatedReplicaSets.filter(isHistoricalDeploymentReplicaSet).count
                let visibleReplicaSets = showsHistoricalDeploymentReplicaSets
                    ? relatedReplicaSets
                    : relatedReplicaSets.filter { !isHistoricalDeploymentReplicaSet($0) }

                ResourceRelationshipSection(
                    title: "Related ReplicaSets",
                    rowCount: (shouldOfferReplicaSetHistoryLoad ? 1 : visibleReplicaSets.count)
                        + ((historicalReplicaSetCount > 0 || shouldOfferReplicaSetHistoryLoad) ? 1 : 0)
                ) {
                    if shouldOfferReplicaSetHistoryLoad {
                        ResourceRelationshipEmptyRow(
                            title: "History not loaded",
                            subtitle: "Simple mode loads ReplicaSets only when you ask for rollout history."
                        )
                    } else {
                        ForEach(visibleReplicaSets) { replicaSet in
                            ResourceRelationshipLinkButton(
                                title: replicaSet.name,
                                subtitle: "\(replicaSet.namespace ?? deployment.namespace) · \(replicaSet.primaryText)",
                                symbol: "rectangle.stack"
                            ) {
                                viewModel.openDeploymentRelatedReplicaSet(replicaSet)
                            }
                        }
                    }

                    if historicalReplicaSetCount > 0 || shouldOfferReplicaSetHistoryLoad {
                        RuneDisclosureRow(
                            shouldOfferReplicaSetHistoryLoad
                                ? "Deployment history, not loaded"
                                : "Deployment history, \(historicalReplicaSetCount) inactive ReplicaSets",
                            isExpanded: showsHistoricalDeploymentReplicaSets,
                            help: shouldOfferReplicaSetHistoryLoad
                                ? "Load ReplicaSets for rollout history and debugging."
                                : showsHistoricalDeploymentReplicaSets
                                ? "Hide inactive ReplicaSets with 0/0 ready replicas."
                                : "Show inactive ReplicaSets kept for rollout history and debugging.",
                            action: {
                                if shouldOfferReplicaSetHistoryLoad {
                                    showsHistoricalDeploymentReplicaSets = true
                                    viewModel.refreshReplicaSetsForCurrentNamespace()
                                } else {
                                    showsHistoricalDeploymentReplicaSets.toggle()
                                }
                            }
                        ) {
                            HStack(spacing: RuneSpacing.xs) {
                                Image(systemName: "clock.arrow.circlepath")
                                    .frame(width: 16)
                                    .foregroundStyle(.runeSecondary)
                                Text(showsHistoricalDeploymentReplicaSets ? "Hide history" : "Show history")
                                    .runeText(.microEmphasis)
                                if historicalReplicaSetCount > 0 {
                                    Text("(\(historicalReplicaSetCount))")
                                        .runeText(.micro)
                                        .foregroundStyle(.runeSecondary)
                                }
                                Spacer(minLength: 0)
                            }
                        }
                    }
                }
            }

            let relatedPods = viewModel.selectedDeploymentRelatedPods
            ResourceRelationshipSection(title: "Related Pods", rowCount: max(relatedPods.count, 1)) {
                if relatedPods.isEmpty {
                    ResourceRelationshipEmptyRow(
                        title: "No related pods in snapshot",
                        subtitle: "Rune has no loaded pod that matches this deployment or its ReplicaSets."
                    )
                } else {
                    ForEach(relatedPods) { pod in
                        ResourceRelationshipLinkButton(
                            title: pod.name,
                            subtitle: "\(pod.namespace) · \(pod.displayStatus)",
                            symbol: "cube.box"
                        ) {
                            viewModel.openDeploymentRelatedPod(pod)
                        }
                    }
                }
            }

            let relatedEvents = viewModel.selectedDeploymentRelatedEvents
            if !relatedEvents.isEmpty {
                RelatedEventsRelationshipSection(events: relatedEvents, open: viewModel.openRelatedEvent)
            }

        }
    }

    /// The replica state is the header pill; the overview starts with the scaler.
    func deploymentInspectorStatus(_ deployment: DeploymentSummary) -> RuneInspectorStatus {
        let tone: RuneBadgeTone
        if deployment.desiredReplicas == 0 {
            tone = .neutral
        } else if deployment.readyReplicas >= deployment.desiredReplicas {
            tone = .success
        } else if deployment.readyReplicas > 0 {
            tone = .warning
        } else {
            tone = .danger
        }
        return RuneInspectorStatus(deploymentReplicaStatusText(deployment), tone: tone)
    }

    @ViewBuilder
    var deploymentInspectorActions: some View {
        Button("Restart Rollout") {
            viewModel.requestRolloutRestartSelectedDeployment()
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .disabled(!viewModel.canApplyClusterMutations)

        Button(appString(.applyYAML)) {
            viewModel.requestApplySelectedResourceYAML()
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .disabled(!viewModel.canApplyClusterMutations)

        RuneToolbarMenu {
            Button("Export Pod Logs ZIP") {
                viewModel.saveDeploymentPodLogsZip()
            }
            Button("Save Pod Logs ZIP to Export Folder") {
                viewModel.saveDeploymentPodLogsZipToExportFolder(openAfterSave: false)
            }
            Button("Save Pod Logs ZIP and Open") {
                viewModel.saveDeploymentPodLogsZipToExportFolder(openAfterSave: true)
            }
        } label: {
            Label("Export Pod Logs ZIP", systemImage: "archivebox")
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .disabled(viewModel.state.isLoadingLogs)

        Button("Export Pod YAML ZIP") {
            viewModel.saveDeploymentPodYAMLZip()
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .disabled(viewModel.state.isLoadingResourceDetails)

        Button("Delete", role: .destructive) {
            viewModel.requestDeleteSelectedResource()
        }
        .disabled(!viewModel.canApplyClusterMutations)
    }

    func isHistoricalDeploymentReplicaSet(_ replicaSet: ClusterResourceSummary) -> Bool {
        replicaSet.primaryText
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
            .hasPrefix("0/0 ready")
    }

    func inspectorEmptyState(_ state: RuneContentState, symbol: String) -> some View {
        RunePaneContentStateView(
            state,
            style: .plain,
            graphicSystemImage: symbol
        )
    }

    var namespaceSuggestions: [String] {
        viewModel.namespaceOptions
    }

    var hasAvailableKubernetesContexts: Bool {
        !viewModel.contextMenuOptions.isEmpty
    }

    var manualNamespaceMenuOptions: [String] {
        viewModel.manualNamespaceOptions
    }

    var namespaceMenuTitle: String {
        let trimmed = viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "Namespace" : trimmed
    }

    func openSettingsWindow() {
        openSettings()
    }

    /// When the toolbar already scopes to a namespace, omit duplicate namespace chips/labels for resources in that namespace.
    func shouldShowResourceNamespaceLabel(_ resourceNamespace: String?) -> Bool {
        guard let ns = resourceNamespace?.trimmingCharacters(in: .whitespacesAndNewlines), !ns.isEmpty else {
            return false
        }
        let selected = viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        if selected.isEmpty { return true }
        return ns.caseInsensitiveCompare(selected) != .orderedSame
    }

    var panelFill: some ShapeStyle {
        RuneSurfaceKind.panel.fill(theme: activeAppearanceTheme)
    }

    var editorFill: Color {
        RuneSurfaceKind.editor.fill(theme: activeAppearanceTheme)
    }

    var overviewEventsCardHelp: String {
        "Open the Events view. This is the raw Kubernetes Event list for the namespace, while Cluster Signals only promotes selected warning events into triage."
    }

    func eventHint(for event: EventSummary) -> String {
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if kind.isEmpty {
            return "Shows details in the inspector. Use the action button to open the involved resource when listed."
        }
        return "Shows details in the inspector. \"Go to …\" switches section and selects \(kind) \(event.objectName) when present."
    }

    func podStatusCount(_ status: String) -> Int {
        viewModel.state.overviewPods.filter { $0.status.lowercased() == status }.count
    }

    func statusColor(for status: String) -> Color {
        switch status.lowercased() {
        case "running": return RuneSemanticColorRole.success.color(in: activeAppearanceTheme.palette)
        case "pending": return RuneSemanticColorRole.warning.color(in: activeAppearanceTheme.palette)
        case "failed": return RuneSemanticColorRole.danger.color(in: activeAppearanceTheme.palette)
        case "succeeded": return RuneSemanticColorRole.info.color(in: activeAppearanceTheme.palette)
        default: return .gray
        }
    }

    func portForwardStatusColor(_ status: PortForwardStatus) -> Color {
        switch status {
        case .starting: return RuneSemanticColorRole.warning.color(in: activeAppearanceTheme.palette)
        case .active: return RuneSemanticColorRole.success.color(in: activeAppearanceTheme.palette)
        case .stopped: return .secondary
        case .failed: return RuneSemanticColorRole.danger.color(in: activeAppearanceTheme.palette)
        }
    }

    func execOutputText(for result: PodExecResult) -> String {
        let stdout = result.stdout.isEmpty ? "" : result.stdout
        let stderr = result.stderr.isEmpty ? "" : "\n[stderr]\n\(result.stderr)"
        let merged = stdout + stderr
        return merged.isEmpty ? "No output" : merged
    }
}
