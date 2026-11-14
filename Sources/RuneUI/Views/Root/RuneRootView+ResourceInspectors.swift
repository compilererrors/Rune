import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var podDetails: some View {
        Group {
            if let pod = viewModel.state.selectedPod {
                RuneInspectorScaffold(
                    title: pod.name,
                    copyAccessibilityLabel: "Copy Pod name",
                    bodyScrollBehavior: podInspectorTab == .overview ? .vertical : .selfManaged,
                    showsActions: podInspectorTab == .overview,
                    onCopy: { copyToClipboard(pod.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        podInspectorCoreInfo(pod)
                    },
                    tabs: {
                    RuneTabBar("", selection: $podInspectorTab, tabs: PodInspectorTab.allCases) { $0.localizedTitle(appString) }
                    .accessibilityLabel("Inspector")
                    },
                    actions: {
                        Button("Delete", role: .destructive) {
                            viewModel.requestDeleteSelectedResource()
                        }
                        .disabled(!viewModel.canApplyClusterMutations)
                    },
                    content: {
                    Group {
                        switch podInspectorTab {
                        case .overview:
                            podOverviewSection(pod: pod)

                        case .logs:
                            PodLogsInspectorPane(
                                selectedLogPreset: $viewModel.selectedLogPreset,
                                includePreviousLogs: $viewModel.includePreviousLogs,
                                selectedContainer: $viewModel.selectedLogContainer,
                                isTailModeEnabled: $viewModel.isLogTailModeEnabled,
                                isStreamPaused: $viewModel.isLogStreamPaused,
                                isLoadingLogs: viewModel.state.isLoadingLogs,
                                isLoadingResources: viewModel.state.isLoading,
                                errorMessage: viewModel.state.lastLogFetchError,
                                statusText: logStatusText,
                                containerOptions: viewModel.podLogContainerOptions,
                                logText: viewModel.state.podLogs,
                                readOnlyResetID: "podlogs:\(viewModel.state.selectedPod?.name ?? ""):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs):\(viewModel.selectedLogContainer)",
                                onReload: { viewModel.reloadLogsForSelection() },
                                onSave: { viewModel.saveCurrentLogs() },
                                onSaveToExportFolder: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: false) },
                                onSaveAndOpen: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: true) },
                                onSaveVisibleZip: { viewModel.saveVisibleLogsZip(visibleText: $0) },
                                onSaveVisibleZipToExportFolder: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: false) },
                                onSaveVisibleZipAndOpen: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: true) },
                                onSaveFullZip: { viewModel.saveCurrentLogsZip() },
                                onSaveFullZipToExportFolder: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: false) },
                                onSaveFullZipAndOpen: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: true) },
                                onSaveAllPodsZip: { viewModel.saveAllPodsLogsZip() },
                                onSaveAllPodsZipToExportFolder: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: false) },
                                onSaveAllPodsZipAndOpen: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: true) },
                                onCopySelection: { copySelectedTextFromFocusedTextView() },
                                onCopyAll: { viewModel.copyCurrentLogsToClipboard() },
                                onToggleStreamPause: { viewModel.toggleLogStreamPause() }
                            )

                        case .exec:
                            execPane(for: pod)

                        case .portForward:
                            portForwardPane(targetKind: .pod, targetName: pod.name)

                        case .describe:
                            manifestInspectorPane(activeTab: .describe)

                        case .yaml:
                            manifestInspectorPane(activeTab: .yaml)
                        }
                    }
                    .transaction { transaction in
                        transaction.animation = nil
                    }
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select a pod",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "cube.box"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    var deploymentDetails: some View {
        Group {
            if let deployment = viewModel.state.selectedDeployment {
                RuneInspectorScaffold(
                    title: deployment.name,
                    copyAccessibilityLabel: "Copy Deployment name",
                    bodyScrollBehavior: deploymentInspectorTab == .overview ? .vertical : .selfManaged,
                    showsActions: deploymentInspectorTab == .overview,
                    onCopy: { copyToClipboard(deployment.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        deploymentInspectorCoreInfo(deployment)
                    },
                    tabs: {
                    RuneTabBar("", selection: $deploymentInspectorTab, tabs: DeploymentInspectorTab.allCases) { $0.localizedTitle(appString) }
                    .accessibilityLabel("Inspector")
                    },
                    actions: {
                        deploymentInspectorActions
                    },
                    content: {
                    Group {
                        switch deploymentInspectorTab {
                        case .overview:
                            deploymentOverviewSection(deployment: deployment)

                        case .unifiedLogs:
                            UnifiedResourceLogsInspectorPane(
                                selectedLogPreset: $viewModel.selectedLogPreset,
                                includePreviousLogs: $viewModel.includePreviousLogs,
                                isTailModeEnabled: $viewModel.isLogTailModeEnabled,
                                isStreamPaused: $viewModel.isLogStreamPaused,
                                isLoadingLogs: viewModel.state.isLoadingLogs,
                                isLoadingResources: viewModel.state.isLoading,
                                errorMessage: viewModel.state.lastLogFetchError,
                                statusText: logStatusText,
                                podNames: viewModel.state.unifiedServiceLogPods,
                                logText: viewModel.state.unifiedServiceLogs,
                                readOnlyResetID: "unifiedlogs:\(viewModel.state.selectedDeployment?.name ?? ""):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs)",
                                onReload: { viewModel.reloadLogsForSelection() },
                                onSave: { viewModel.saveCurrentLogs() },
                                onSaveToExportFolder: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: false) },
                                onSaveAndOpen: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: true) },
                                onSaveVisibleZip: { viewModel.saveVisibleLogsZip(visibleText: $0) },
                                onSaveVisibleZipToExportFolder: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: false) },
                                onSaveVisibleZipAndOpen: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: true) },
                                onSaveFullZip: { viewModel.saveCurrentLogsZip() },
                                onSaveFullZipToExportFolder: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: false) },
                                onSaveFullZipAndOpen: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: true) },
                                onSaveAllPodsZip: { viewModel.saveAllPodsLogsZip() },
                                onSaveAllPodsZipToExportFolder: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: false) },
                                onSaveAllPodsZipAndOpen: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: true) },
                                onCopySelection: { copySelectedTextFromFocusedTextView() },
                                onCopyAll: { viewModel.copyCurrentLogsToClipboard() },
                                onToggleStreamPause: { viewModel.toggleLogStreamPause() }
                            )

                        case .rollout:
                            VStack(alignment: .leading, spacing: RuneSpacing.inset) {
                                RuneAdaptiveToolbar("Deployment rollout actions") {
                                    TextField("Revision (optional)", text: $viewModel.rolloutRevisionInput)
                                        .textFieldStyle(RuneControlTextFieldStyle())
                                        .frame(maxWidth: 160)
                                } secondary: {
                                    HStack(spacing: RuneSpacing.xs) {
                                        Button("Rollback") {
                                            viewModel.requestRolloutUndoSelectedDeployment()
                                        }
                                        .disabled(!viewModel.canApplyClusterMutations)

                                        Button("Save History") {
                                            viewModel.saveCurrentRolloutHistory()
                                        }
                                    }
                                }

                                DeploymentRolloutHistoryView(history: viewModel.state.deploymentRolloutHistory)
                            }

                        case .describe:
                            manifestInspectorPane(activeTab: .describe)

                        case .yaml:
                            manifestInspectorPane(activeTab: .yaml)
                        }
                    }
                    .transaction { transaction in
                        transaction.animation = nil
                    }
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select a deployment",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "shippingbox"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    var serviceDetails: some View {
        Group {
            if let service = viewModel.state.selectedService {
                RuneInspectorScaffold(
                    title: service.name,
                    copyAccessibilityLabel: "Copy Service name",
                    bodyScrollBehavior: serviceInspectorTab == .overview ? .vertical : .selfManaged,
                    showsActions: serviceInspectorTab == .overview,
                    onCopy: { copyToClipboard(service.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        serviceInspectorCoreInfo(service)
                    },
                    tabs: {
                    RuneTabBar("", selection: $serviceInspectorTab, tabs: ServiceInspectorTab.allCases) { $0.localizedTitle(appString) }
                    .accessibilityLabel("Inspector")
                    },
                    actions: {
                        serviceInspectorActions
                    },
                    content: {
                    Group {
                        switch serviceInspectorTab {
                        case .overview:
                            VStack(alignment: .leading, spacing: RuneSpacing.sm) {
                                let relatedPods = viewModel.selectedServiceRelatedPods
                                if !relatedPods.isEmpty {
                                    RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openServiceRelatedPod)
                                }
                                let relatedIngresses = viewModel.selectedServiceRelatedIngresses
                                if !relatedIngresses.isEmpty {
                                    ResourceRelationshipSection(title: "Related Ingresses", rowCount: relatedIngresses.count) {
                                        ForEach(relatedIngresses) { ingress in
                                            ResourceRelationshipLinkButton(
                                                title: ingress.name,
                                                subtitle: "\(ingress.namespace ?? service.namespace) · \(ingress.primaryText)",
                                                symbol: "point.3.filled.connected.trianglepath.dotted"
                                            ) {
                                                viewModel.openServiceRelatedIngress(ingress)
                                            }
                                        }
                                    }
                                }
                                let relatedEvents = viewModel.selectedServiceRelatedEvents
                                if !relatedEvents.isEmpty {
                                    RelatedEventsRelationshipSection(events: relatedEvents, open: viewModel.openRelatedEvent)
                                }
                            }

                        case .unifiedLogs:
                            UnifiedResourceLogsInspectorPane(
                                selectedLogPreset: $viewModel.selectedLogPreset,
                                includePreviousLogs: $viewModel.includePreviousLogs,
                                isTailModeEnabled: $viewModel.isLogTailModeEnabled,
                                isStreamPaused: $viewModel.isLogStreamPaused,
                                isLoadingLogs: viewModel.state.isLoadingLogs,
                                isLoadingResources: viewModel.state.isLoading,
                                errorMessage: viewModel.state.lastLogFetchError,
                                statusText: logStatusText,
                                podNames: viewModel.state.unifiedServiceLogPods,
                                logText: viewModel.state.unifiedServiceLogs,
                                readOnlyResetID: "unifiedlogs:\(viewModel.state.selectedService?.name ?? ""):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs)",
                                onReload: { viewModel.reloadLogsForSelection() },
                                onSave: { viewModel.saveCurrentLogs() },
                                onSaveToExportFolder: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: false) },
                                onSaveAndOpen: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: true) },
                                onSaveVisibleZip: { viewModel.saveVisibleLogsZip(visibleText: $0) },
                                onSaveVisibleZipToExportFolder: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: false) },
                                onSaveVisibleZipAndOpen: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: true) },
                                onSaveFullZip: { viewModel.saveCurrentLogsZip() },
                                onSaveFullZipToExportFolder: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: false) },
                                onSaveFullZipAndOpen: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: true) },
                                onSaveAllPodsZip: { viewModel.saveAllPodsLogsZip() },
                                onSaveAllPodsZipToExportFolder: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: false) },
                                onSaveAllPodsZipAndOpen: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: true) },
                                onCopySelection: { copySelectedTextFromFocusedTextView() },
                                onCopyAll: { viewModel.copyCurrentLogsToClipboard() },
                                onToggleStreamPause: { viewModel.toggleLogStreamPause() }
                            )

                        case .portForward:
                            portForwardPane(targetKind: .service, targetName: service.name)

                        case .describe:
                            manifestInspectorPane(activeTab: .describe)

                        case .yaml:
                            manifestInspectorPane(activeTab: .yaml)
                        }
                    }
                    .transaction { transaction in
                        transaction.animation = nil
                    }
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select a service",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "point.3.connected.trianglepath.dotted"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    func serviceInspectorCoreInfo(_ service: ServiceSummary) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
            if shouldShowResourceNamespaceLabel(service.namespace) {
                inspectorInfoRow("Namespace", value: service.namespace, symbol: "square.stack.3d.up")
            }
            inspectorInfoRow("Type", value: service.type, symbol: "point.3.connected.trianglepath.dotted")
            inspectorInfoRow("Cluster IP", value: service.clusterIP, symbol: "network")
        }
    }

    @ViewBuilder
    var serviceInspectorActions: some View {
        Button(appString(.applyYAML)) {
            viewModel.requestApplySelectedResourceYAML()
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .disabled(!viewModel.canApplyClusterMutations)

        Button("Export…") {
            viewModel.saveCurrentResourceYAML()
        }
        .buttonStyle(RuneToolbarButtonStyle())

        Button("Delete", role: .destructive) {
            viewModel.requestDeleteSelectedResource()
        }
        .disabled(!viewModel.canApplyClusterMutations)
    }

    var eventDetails: some View {
        Group {
            if let event = viewModel.state.selectedEvent {
                RuneInspectorScaffold(
                    title: event.reason,
                    copyAccessibilityLabel: "Copy event reason",
                    bodyScrollBehavior: .vertical,
                    showsTabs: false,
                    showsActions: !event.objectName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
                    onCopy: { copyToClipboard(event.reason) },
                    onRefresh: refreshDetailPane,
                    info: {
                        eventInspectorCoreInfo(event)
                    },
                    tabs: {
                        EmptyView()
                    },
                    actions: {
                        Button(eventGoToResourceButtonTitle(for: event)) {
                            viewModel.openEventSource(event)
                        }
                        .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                        .help("Switches section and selects the involved object when it appears in the list.")
                    },
                    content: {
                        Text(event.message.isEmpty ? "No event message" : event.message)
                            .runeText(.code)
                            .textSelection(.enabled)
                            .fixedSize(horizontal: false, vertical: true)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(RuneSpacing.inset)
                            .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select an event",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "bolt.badge.clock"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    func eventInspectorCoreInfo(_ event: EventSummary) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
            if let timestamp = event.lastTimestamp?.trimmingCharacters(in: .whitespacesAndNewlines), !timestamp.isEmpty {
                inspectorInfoRow("Time", value: timestamp, symbol: "clock")
            }
            inspectorInfoRow("Type", value: event.type, symbol: "exclamationmark.bubble")

            let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            let object = kind.isEmpty ? event.objectName : "\(kind) / \(event.objectName)"
            inspectorInfoRow("Object", value: object, symbol: "cube")

            if let namespace = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines), !namespace.isEmpty {
                inspectorInfoRow("Namespace", value: namespace, symbol: "square.stack.3d.up")
            }
        }
    }

    func eventGoToResourceButtonTitle(for event: EventSummary) -> String {
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        switch kind {
        case "pod": return "Go to Pod"
        case "deployment": return "Go to Deployment"
        case "statefulset": return "Go to StatefulSet"
        case "daemonset": return "Go to DaemonSet"
        case "service": return "Go to Service"
        case "ingress": return "Go to Ingress"
        case "configmap": return "Go to ConfigMap"
        case "secret": return "Go to Secret"
        case "node": return "Go to Node"
        case "": return "Go to resource"
        default: return "Go to resource"
        }
    }

    func genericResourceDetails(resource: ClusterResourceSummary?) -> some View {
        genericResourceDetails(resource: resource) {
            EmptyView()
        }
    }

    func genericResourceDetails<Supplementary: View>(
        resource: ClusterResourceSummary?,
        @ViewBuilder supplementary: () -> Supplementary
    ) -> some View {
        Group {
            if let resource {
                RuneInspectorScaffold(
                    title: resource.name,
                    copyAccessibilityLabel: "Copy \(resource.kind.localizedTitle(appString)) name",
                    bodyScrollBehavior: genericResourceManifestTab == .overview ? .vertical : .selfManaged,
                    showsActions: genericResourceManifestTab == .overview && resource.kind == .configMap,
                    onCopy: { copyToClipboard(resource.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
                            if let namespace = resource.namespace, shouldShowResourceNamespaceLabel(namespace) {
                                inspectorInfoRow("Namespace", value: namespace, symbol: "square.stack.3d.up")
                            }
                            let labels = resource.kind.summaryFieldLabels
                            inspectorInfoRow(labels.primary.title, value: resource.primaryText, symbol: labels.primary.symbol)
                            inspectorInfoRow(labels.secondary.title, value: resource.secondaryText, symbol: labels.secondary.symbol)
                        }
                    },
                    tabs: {
                        RuneTabBar(appString(.manifest), selection: $genericResourceManifestTab, tabs: GenericResourceManifestTab.allCases) { $0.localizedTitle(appString) }
                        .accessibilityLabel("Inspector")
                    },
                    actions: {
                        if resource.kind == .configMap {
                            Button("Edit ConfigMap YAML") {
                                genericResourceManifestTab = .yaml
                                yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
                            }
                            .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                            .disabled(!viewModel.canApplyClusterMutations)
                            .help("Switches to YAML and enables inline editing for this ConfigMap.")
                        }
                    },
                    content: {
                        switch genericResourceManifestTab {
                        case .overview:
                            genericResourceOverview(resource: resource, supplementary: supplementary)
                        case .describe, .yaml:
                            manifestInspectorPane(activeTab: genericResourceManifestTab)
                        }
                    }
                )
            } else {
                genericResourceEmptyState
            }
        }
    }

    func genericResourceOverview<Supplementary: View>(
        resource: ClusterResourceSummary,
        @ViewBuilder supplementary: () -> Supplementary
    ) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.sm) {
            supplementary()

            let relatedEvents = viewModel.relatedEvents(for: resource)
            if !relatedEvents.isEmpty {
                RelatedEventsRelationshipSection(events: relatedEvents, open: viewModel.openRelatedEvent)
            }

            if resource.kind == .statefulSet || resource.kind == .daemonSet {
                HStack(spacing: RuneSpacing.inset) {
                    TextField("Revision (optional)", text: $viewModel.rolloutRevisionInput)
                        .textFieldStyle(RuneControlTextFieldStyle())
                        .frame(maxWidth: 160)

                    Button("Rollback") {
                        viewModel.requestRolloutUndoSelectedController()
                    }
                    .disabled(!viewModel.canApplyClusterMutations)

                    Spacer(minLength: 0)
                }
            }
        }
    }
}
