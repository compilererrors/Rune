import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var detailPane: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.sm) {
            ZStack(alignment: .topLeading) {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)
                    .fill(panelFill)

                Group {
                    switch viewModel.state.selectedSection {
                    case .overview:
                        overviewDetails
                    case .workloads:
                        workloadDetails
                    case .networking:
                        networkingDetails
                    case .config:
                        configDetails
                    case .storage:
                        storageDetails
                    case .events:
                        eventDetails
                    case .helm:
                        helmDetails
                    case .rbac:
                        rbacDetails
                    case .terminal:
                        terminalDetails
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                .padding(RuneSpacing.md)
                // Every inspector button without an explicit role uses the
                // toolbar family, never the native gray bezel.
                .buttonStyle(RuneToolbarButtonStyle())
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .clipShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)
                    .strokeBorder(Color(nsColor: .separatorColor).opacity(0.28), lineWidth: 1)
            }
        }
        // `minWidth: 0` lets split columns shrink correctly; without it, nested scroll views can force odd horizontal alignment.
        // `minHeight: 0` keeps the pane exactly as tall as its column, so content that reports a taller
        // minimum never grows the pane and gets it re-centred (the pane top would jump between tabs).
        .frame(minWidth: 0, maxWidth: .infinity, minHeight: 0, maxHeight: .infinity, alignment: .topLeading)
        .padding(RuneSpacing.sm)
        .background {
            ZStack(alignment: .leading) {
                RuneGlassPaneSurface(role: .inspector)
                RuneGlassPaneBorder(role: .inspector)
                RuneRootLayoutProbe(kind: .detail, generation: layoutGeneration)
                paneFocusOutline(isFocused: keyboardPaneFocus == .detail)
            }
        }
    }

    var overviewDetails: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.sm) {
            Text("Overview")
                .runeText(.title, weight: .bold)

            VStack(alignment: .leading, spacing: RuneSpacing.xs) {
                inspectorInfoRow("Context", value: viewModel.state.selectedContext?.name ?? "-", symbol: "network")
                inspectorInfoRow("Namespace", value: viewModel.state.selectedNamespace, symbol: "square.stack.3d.up")
                inspectorInfoRow("Mode", value: viewModel.state.isReadOnlyMode ? "Read-only" : "Read/Write", symbol: "lock.shield")
            }

            inspectorActionButtonRow {
                Button("Open Workloads") {
                    viewModel.setSection(.workloads)
                }

                Button("Open Events") {
                    viewModel.setSection(.events)
                }

                Button("Open Helm") {
                    viewModel.setSection(.helm)
                }

                Button("Reload") {
                    viewModel.refreshCurrentView(debounced: false)
                }

                if !simpleMode {
                    RuneToolbarMenu {
                        Button("Save Bundle") {
                            viewModel.saveSupportBundle()
                        }

                        Button("Save Bundle to Export Folder") {
                            viewModel.saveSupportBundleToExportFolder(openAfterSave: false)
                        }

                        Button("Save Bundle and Open") {
                            viewModel.saveSupportBundleToExportFolder(openAfterSave: true)
                        }
                    } label: {
                        Label("Save Bundle", systemImage: "square.and.arrow.down")
                    }
                }
            }

            Divider()

            VStack(alignment: .leading, spacing: RuneSpacing.xs) {
                HStack(spacing: RuneSpacing.xs) {
                    Text("Write Audit")
                        .runeText(.headline)
                    Spacer(minLength: 0)
                    RuneToolbarMenu {
                        Button("Export JSON…") {
                            viewModel.saveVisibleWriteAuditLog()
                        }

                        Button("Save JSON to Export Folder") {
                            viewModel.saveVisibleWriteAuditLogToExportFolder(openAfterSave: false)
                        }

                        Button("Save JSON and Open") {
                            viewModel.saveVisibleWriteAuditLogToExportFolder(openAfterSave: true)
                        }
                    } label: {
                        Label("Export", systemImage: "square.and.arrow.up")
                    }
                    .buttonStyle(RuneToolbarButtonStyle())
                    .controlSize(.small)
                    .disabled(viewModel.state.writeAuditLog.isEmpty)
                    .help("Export visible write audit entries")
                }

                if viewModel.state.writeAuditLog.isEmpty {
                    Text("No write actions in this session.")
                        .runeText(.micro)
                        .foregroundStyle(.runeSecondary)
                } else {
                    TextField("Search audit", text: $viewModel.writeAuditSearchQuery)
                        .textFieldStyle(RuneControlTextFieldStyle())

                    if viewModel.visibleWriteAuditEntries.isEmpty {
                        Text("No audit entries match the current search.")
                            .runeText(.micro)
                            .foregroundStyle(.runeSecondary)
                    }

                    ForEach(viewModel.visibleWriteAuditEntries.prefix(8)) { entry in
                        VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                            HStack {
                                Text(entry.action)
                                    .runeText(.microEmphasis)
                                Spacer(minLength: 8)
                                Text(entry.status)
                                    .runeText(.microEmphasis)
                                    .foregroundStyle(entry.status == "Succeeded" ? RuneTextStyle.success : .danger)
                            }
                            Text(entry.resource)
                                .runeText(.micro)
                                .lineLimit(1)
                                .help(entry.resource)
                            Text("\(entry.contextName) · \(entry.namespace) · \(entry.timestamp.formatted(date: .omitted, time: .standard))")
                                .runeText(.micro)
                                .foregroundStyle(.runeSecondary)
                        }
                        .padding(.vertical, RuneSpacing.xxs)
                    }
                }
            }

            Spacer(minLength: 0)
        }
    }

    var workloadDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                podDetails
            case .deployment:
                deploymentDetails
            case .statefulSet:
                statefulSetDetails
            case .daemonSet:
                daemonSetDetails
            case .job:
                jobDetails
            case .cronJob:
                cronJobInspectorContent
            case .replicaSet:
                replicaSetDetails
            case .horizontalPodAutoscaler:
                horizontalPodAutoscalerDetails
            case .service, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()
            case .event:
                EmptyView()
            }
        }
    }

    var replicaSetDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedReplicaSet) {
            let relatedPods = viewModel.selectedReplicaSetRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openReplicaSetRelatedPod)
            }
        }
    }

    var statefulSetDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedStatefulSet) {
            if viewModel.state.selectedStatefulSet != nil,
               let replicas = viewModel.selectedStatefulSetReplicaCounts {
                HStack(spacing: RuneSpacing.xs) {
                    RuneStatusDot((replicas.ready >= replicas.desired ? RuneSemanticColorRole.success : replicas.ready > 0 ? .warning : .danger).color(in: activeAppearanceTheme.palette))
                    Text("\(replicas.ready) of \(replicas.desired) ready")
                        .runeText(.body, weight: .semibold)
                }

                WorkloadReplicaScaleControlsView(
                    label: "Replicas",
                    isDirty: viewModel.scaleReplicaInput != replicas.desired,
                    canMutate: viewModel.canApplyClusterMutations,
                    replicas: $viewModel.scaleReplicaInput,
                    action: viewModel.requestScaleSelectedStatefulSet
                )

                Button("Restart Rollout") {
                    viewModel.requestRolloutRestartSelectedStatefulSet()
                }
                .buttonStyle(RuneToolbarButtonStyle())
                .disabled(!viewModel.canApplyClusterMutations)

                Divider()
                    .opacity(0.45)
            }

            let relatedPods = viewModel.selectedStatefulSetRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openStatefulSetRelatedPod)
            }
        }
    }

    var daemonSetDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedDaemonSet) {
            let relatedPods = viewModel.selectedDaemonSetRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openDaemonSetRelatedPod)
            }
        }
    }

    var cronJobInspectorContent: some View {
        genericResourceDetails(resource: viewModel.state.selectedCronJob) {
            if viewModel.state.selectedCronJob != nil {
                HStack(spacing: RuneSpacing.inset) {
                    if viewModel.state.selectedCronJob?.secondaryText == "Suspended" {
                        Button("Resume") {
                            viewModel.setSelectedCronJobSuspended(false)
                        }
                        .disabled(!viewModel.canApplyClusterMutations)
                    } else {
                        Button("Suspend") {
                            viewModel.setSelectedCronJobSuspended(true)
                        }
                        .disabled(!viewModel.canApplyClusterMutations)
                    }
                    Button("Create job now") {
                        viewModel.createManualJobFromSelectedCronJob()
                    }
                    .disabled(!viewModel.canApplyClusterMutations)
                }
            }
            let relatedJobs = viewModel.selectedCronJobRelatedJobs
            if !relatedJobs.isEmpty {
                ResourceRelationshipSection(title: "Related Jobs", rowCount: relatedJobs.count) {
                    ForEach(relatedJobs) { job in
                        ResourceRelationshipLinkButton(
                            title: job.name,
                            subtitle: "\(job.namespace ?? viewModel.state.selectedNamespace) · \(job.primaryText)",
                            symbol: "checklist"
                        ) {
                            viewModel.openCronJobRelatedJob(job)
                        }
                    }
                }
            }
        }
    }

    var jobDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedJob) {
            let relatedPods = viewModel.selectedJobRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openJobRelatedPod)
            }
        }
    }

    var horizontalPodAutoscalerDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedHorizontalPodAutoscaler) {
            if let target = viewModel.selectedHorizontalPodAutoscalerScaleTarget {
                ResourceRelationshipSection(title: "Scale Target") {
                    ResourceRelationshipLinkButton(
                        title: target.name,
                        subtitle: target.subtitle,
                        symbol: target.symbol
                    ) {
                        viewModel.openHorizontalPodAutoscalerScaleTarget(target)
                    }
                }
            }
        }
    }

    var networkingDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                serviceDetails
            case .ingress:
                ingressDetails
            case .endpoint:
                genericResourceDetails(resource: viewModel.state.selectedEndpoint)
            case .networkPolicy:
                genericResourceDetails(resource: viewModel.state.selectedNetworkPolicy)
            case .pod, .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()
            }
        }
    }

    var ingressDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedIngress) {
            let relatedServices = viewModel.selectedIngressRelatedServices
            if !relatedServices.isEmpty {
                ResourceRelationshipSection(title: "Related Services", rowCount: relatedServices.count) {
                    ForEach(relatedServices) { service in
                        ResourceRelationshipLinkButton(
                            title: service.name,
                            subtitle: "\(service.namespace) · \(service.type) · \(service.clusterIP)",
                            symbol: "point.3.connected.trianglepath.dotted"
                        ) {
                            viewModel.openIngressRelatedService(service)
                        }
                    }
                }
            }
        }
    }

    var configDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                genericResourceDetails(resource: viewModel.state.selectedConfigMap)
            case .secret:
                genericResourceDetails(resource: viewModel.state.selectedSecret)
            case .pod, .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .service, .endpoint, .ingress, .networkPolicy, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()
            }
        }
    }

    var storageDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                persistentVolumeClaimDetails
            case .persistentVolume:
                persistentVolumeDetails
            case .storageClass:
                genericResourceDetails(resource: viewModel.state.selectedStorageClass)
            case .node:
                nodeDetails
            default:
                EmptyView()
            }
        }
    }

    var persistentVolumeClaimDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedPersistentVolumeClaim) {
            if let persistentVolume = viewModel.selectedPersistentVolumeClaimRelatedPersistentVolume {
                ResourceRelationshipSection(title: "Related PersistentVolume") {
                    ResourceRelationshipLinkButton(
                        title: persistentVolume.name,
                        subtitle: "\(persistentVolume.primaryText) · \(persistentVolume.secondaryText)",
                        symbol: "externaldrive.badge.checkmark"
                    ) {
                        viewModel.openPersistentVolumeClaimRelatedPersistentVolume(persistentVolume)
                    }
                }
            }
        }
    }

    var persistentVolumeDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedPersistentVolume) {
            let relatedClaims = viewModel.selectedPersistentVolumeRelatedPersistentVolumeClaims
            if !relatedClaims.isEmpty {
                ResourceRelationshipSection(title: "Related PVCs", rowCount: relatedClaims.count) {
                    ForEach(relatedClaims) { pvc in
                        ResourceRelationshipLinkButton(
                            title: pvc.name,
                            subtitle: "\(pvc.namespace ?? viewModel.state.selectedNamespace) · \(pvc.primaryText)",
                            symbol: "externaldrive.badge.person.crop"
                        ) {
                            viewModel.openPersistentVolumeRelatedPersistentVolumeClaim(pvc)
                        }
                    }
                }
            }
        }
    }

    var nodeDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedNode) {
            let relatedPods = viewModel.selectedNodeRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openNodeRelatedPod)
            }
        }
    }

    var rbacDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedRBACResource) {
            if let role = viewModel.selectedRBACBindingReferencedRole {
                ResourceRelationshipSection(title: "Referenced Role") {
                    ResourceRelationshipLinkButton(
                        title: role.name,
                        subtitle: "\(role.namespace ?? "Cluster") · \(role.primaryText)",
                        symbol: role.kind == .clusterRole ? "lock.shield" : "person.badge.key"
                    ) {
                        viewModel.openRBACBindingReferencedRole(role)
                    }
                }
            }

            let relatedBindings = viewModel.selectedRBACRoleRelatedBindings
            if !relatedBindings.isEmpty {
                ResourceRelationshipSection(title: "Related Bindings", rowCount: relatedBindings.count) {
                    ForEach(relatedBindings) { binding in
                        ResourceRelationshipLinkButton(
                            title: binding.name,
                            subtitle: "\(binding.namespace ?? "Cluster") · \(binding.secondaryText)",
                            symbol: binding.kind == .clusterRoleBinding ? "person.2.badge.key" : "person.badge.key"
                        ) {
                            viewModel.openRBACRoleRelatedBinding(binding)
                        }
                    }
                }
            }

            if !simpleMode {
                RBACCanISimulatorPanel(viewModel: viewModel)
            }

            Divider()
        }
    }

    var helmDetails: some View {
        Group {
            if let resource = viewModel.state.selectedOperatorResource {
                operatorResourceDetails(resource: resource)
            } else if let release = viewModel.state.selectedHelmRelease {
                RuneInspectorScaffold(
                    title: release.name,
                    copyAccessibilityLabel: "Copy Helm release name",
                    bodyScrollBehavior: helmInspectorTab == .overview || helmInspectorTab == .history
                        ? .vertical
                        : .selfManaged,
                    showsActions: helmInspectorTab != .overview,
                    onCopy: { copyToClipboard(release.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        helmReleaseCoreInfo(release)
                    },
                    tabs: {
                        RuneTabBar("", selection: $helmInspectorTab, tabs: HelmInspectorTab.allCases) { $0.localizedTitle(appString) }
                        .accessibilityLabel("Helm inspector")
                    },
                    actions: {
                        helmInspectorActions
                    },
                    content: {
                        switch helmInspectorTab {
                        case .overview:
                            inspectorInfoRow("Updated", value: release.updated, symbol: "calendar.badge.clock")
                        case .values:
                            exportableTextPane(
                                text: viewModel.state.helmValues,
                                emptyText: "No values loaded"
                            )
                        case .manifest:
                            exportableTextPane(
                                text: viewModel.state.helmManifest,
                                emptyText: "No manifest loaded"
                            )
                        case .history:
                            helmHistoryPane
                        }
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select a Helm release",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "ferry"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    func helmReleaseCoreInfo(_ release: HelmReleaseSummary) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
            if shouldShowResourceNamespaceLabel(release.namespace) {
                inspectorInfoRow("Namespace", value: release.namespace, symbol: "square.stack.3d.up")
            }
            inspectorInfoRow("Status", value: release.status.capitalized, symbol: "checkmark.seal")
            inspectorInfoRow("Chart", value: release.chart, symbol: "shippingbox")
            inspectorInfoRow("App Version", value: release.appVersion, symbol: "tag")
            inspectorInfoRow("Revision", value: String(release.revision), symbol: "clock.arrow.trianglehead.counterclockwise.rotate.90")
        }
    }

    @ViewBuilder
    var helmInspectorActions: some View {
        switch helmInspectorTab {
        case .values:
            Button("Save Values…", action: viewModel.saveCurrentHelmValues)
                .buttonStyle(RuneToolbarButtonStyle())
        case .manifest:
            Button("Save Manifest…", action: viewModel.saveCurrentHelmManifest)
                .buttonStyle(RuneToolbarButtonStyle())
        case .history:
            Button("Save History…", action: viewModel.saveCurrentHelmHistory)
                .buttonStyle(RuneToolbarButtonStyle())
        case .overview:
            EmptyView()
        }
    }

    @ViewBuilder
    var helmHistoryPane: some View {
        if viewModel.state.helmHistory.isEmpty {
            inspectorEmptyState(
                .empty(
                    title: "No history loaded",
                    message: "Revision history will appear after it is returned by the cluster."
                ),
                symbol: "clock.arrow.circlepath"
            )
        } else {
            VStack(alignment: .leading, spacing: RuneSpacing.inset) {
                HelmRollbackOptionsView(
                    wait: $viewModel.helmRollbackWait,
                    timeout: $viewModel.helmRollbackTimeoutInput,
                    cleanupOnFail: $viewModel.helmRollbackCleanupOnFail
                )
                .padding(RuneSpacing.inset)
                .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))

                LazyVStack(alignment: .leading, spacing: RuneSpacing.inset) {
                    ForEach(viewModel.state.helmHistory) { entry in
                        VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                            HStack {
                                Text("Revision \(entry.revision)")
                                    .runeText(.microEmphasis)
                                Spacer()
                                Button("Rollback") {
                                    viewModel.requestHelmRollback(revision: entry.revision)
                                }
                                .disabled(!viewModel.canApplyClusterMutations)
                                Text(entry.status.capitalized)
                                    .runeText(.microEmphasis)
                                    .foregroundStyle(statusColor(for: entry.status))
                            }
                            Text(entry.chart + " • " + entry.appVersion)
                                .runeText(.micro)
                                .foregroundStyle(.runeSecondary)
                            Text(entry.description)
                                .runeText(.micro)
                            Text(entry.updated)
                                .runeText(.micro)
                                .foregroundStyle(.runeSecondary)
                        }
                        .padding(RuneSpacing.inset)
                        .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                    }
                }
            }
        }
    }

    func operatorResourceDetails(resource: OperatorResourceSummary) -> some View {
        RuneInspectorScaffold(
            title: resource.name,
            copyAccessibilityLabel: "Copy \(resource.kind) name",
            bodyScrollBehavior: genericResourceManifestTab == .overview ? .vertical : .selfManaged,
            showsActions: false,
            onCopy: { copyToClipboard(resource.name) },
            onRefresh: refreshDetailPane,
            info: {
                VStack(alignment: .leading, spacing: RuneSpacing.tight) {
                    inspectorInfoRow("Family", value: resource.family, symbol: "shippingbox")
                    if let namespace = resource.namespace, shouldShowResourceNamespaceLabel(namespace) {
                        inspectorInfoRow("Namespace", value: namespace, symbol: "square.stack.3d.up")
                    } else if resource.namespace == nil {
                        inspectorInfoRow("Scope", value: "Cluster", symbol: "square.stack.3d.up")
                    }
                    inspectorInfoRow("Status", value: resource.status, symbol: "checkmark.seal")
                    inspectorInfoRow("API Path", value: resource.apiPath, symbol: "curlybraces")
                }
            },
            tabs: {
                RuneTabBar(appString(.manifest), selection: $genericResourceManifestTab, tabs: GenericResourceManifestTab.allCases) { $0.localizedTitle(appString) }
                .accessibilityLabel("Inspector")
            },
            actions: {
                EmptyView()
            },
            content: {
                switch genericResourceManifestTab {
                case .overview:
                    operatorResourceOverview(resource)
                case .describe, .yaml:
                    manifestInspectorPane(activeTab: genericResourceManifestTab)
                }
            }
        )
    }

    func operatorResourceOverview(_ resource: OperatorResourceSummary) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.inset) {
            if !resource.message.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                Text(resource.message)
                    .runeText(.caption)
                    .textSelection(.enabled)
                    .fixedSize(horizontal: false, vertical: true)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(RuneSpacing.inset)
                    .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
            }

            ForEach(Array(resource.printerColumns.enumerated()), id: \.offset) { _, column in
                inspectorInfoRow(column.title, value: column.value, symbol: "list.bullet.rectangle")
            }
        }
    }
}
