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
                    subtitle: inspectorSubtitle("Pod", namespace: pod.namespace),
                    status: RuneInspectorStatus(pod.displayStatus),
                    copyAccessibilityLabel: "Copy Pod name",
                    bodyScrollBehavior: podInspectorTab == .overview ? .vertical : .selfManaged,
                    showsActions: podInspectorTab == .overview,
                    onCopy: { copyToClipboard(pod.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        EmptyView()
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
                    subtitle: inspectorSubtitle("Deployment", namespace: deployment.namespace),
                    status: deploymentInspectorStatus(deployment),
                    copyAccessibilityLabel: "Copy Deployment name",
                    bodyScrollBehavior: deploymentInspectorTab == .overview ? .vertical : .selfManaged,
                    showsActions: deploymentInspectorTab == .overview,
                    onCopy: { copyToClipboard(deployment.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        EmptyView()
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
                    subtitle: inspectorSubtitle("Service", namespace: service.namespace),
                    copyAccessibilityLabel: "Copy Service name",
                    bodyScrollBehavior: serviceInspectorTab == .overview ? .vertical : .selfManaged,
                    showsActions: serviceInspectorTab == .overview,
                    onCopy: { copyToClipboard(service.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        EmptyView()
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
                            VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorGroupSpacing) {
                                serviceInspectorCoreInfo(service)
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
        RuneFactGrid("Details") {
            inspectorInfoRow("Type", value: service.type)
            inspectorInfoRow("Cluster IP", value: service.clusterIP)
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

    /// The message is the focus: facts as a short grid with the object as a
    /// link, then the message as readable, wrapping text.
    var eventDetails: some View {
        Group {
            if let event = viewModel.state.selectedEvent {
                RuneInspectorScaffold(
                    title: event.reason,
                    subtitle: eventInspectorSubtitle(event),
                    status: RuneInspectorStatus(event.type, tone: event.type.lowercased() == "warning" ? .warning : .neutral),
                    copyAccessibilityLabel: "Copy event reason",
                    bodyScrollBehavior: .vertical,
                    showsTabs: false,
                    showsActions: false,
                    onCopy: { copyToClipboard(event.reason) },
                    onRefresh: refreshDetailPane,
                    info: {
                        EmptyView()
                    },
                    tabs: {
                        EmptyView()
                    },
                    actions: {
                        EmptyView()
                    },
                    content: {
                        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorGroupSpacing) {
                            VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                                RuneSectionLabel("Message")
                                Text(event.message.isEmpty ? "No event message" : event.message)
                                    .runeText(.body)
                                    .foregroundStyle(event.message.isEmpty ? .runeSecondary : .runePrimary)
                                    .textSelection(.enabled)
                                    .lineSpacing(RuneSpacing.hairline)
                                    .fixedSize(horizontal: false, vertical: true)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .accessibilityIdentifier("event-inspector-message")
                            }
                            eventInspectorCoreInfo(event)
                        }
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

    func eventInspectorSubtitle(_ event: EventSummary) -> String {
        inspectorSubtitle("Event · \(eventInvolvedObjectLabel(event))", namespace: event.involvedNamespace)
    }

    func eventInvolvedObjectLabel(_ event: EventSummary) -> String {
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return kind.isEmpty ? event.objectName : "\(kind)/\(event.objectName)"
    }

    func eventInspectorCoreInfo(_ event: EventSummary) -> some View {
        RuneFactGrid("Details") {
            let object = eventInvolvedObjectLabel(event)
            if !event.objectName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                RuneInspectorInfoRow("Object", copyValue: object, onCopy: copyToClipboard) {
                    RuneFactLink(object, help: "\(eventGoToResourceButtonTitle(for: event)): switches section and selects it when it appears in the list.") {
                        viewModel.openEventSource(event)
                    }
                    .accessibilityLabel(eventGoToResourceButtonTitle(for: event))
                }
            }
            if let timestamp = event.lastTimestamp?.trimmingCharacters(in: .whitespacesAndNewlines), !timestamp.isEmpty {
                inspectorInfoRow("Time", value: timestamp)
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
                    subtitle: inspectorSubtitle(resource.kind.singularTypeName, namespace: resource.namespace),
                    status: genericResourceStatus(resource),
                    copyAccessibilityLabel: "Copy \(resource.kind.localizedTitle(appString)) name",
                    bodyScrollBehavior: genericResourceManifestTab == .overview ? .vertical : .selfManaged,
                    showsActions: genericResourceManifestTab == .overview && resource.kind == .configMap,
                    onCopy: { copyToClipboard(resource.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        EmptyView()
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

    /// Kinds whose first summary field is their status show it as the pill.
    func genericResourceStatus(_ resource: ClusterResourceSummary) -> RuneInspectorStatus? {
        guard resource.kind.summaryFieldLabels.primary.title == "Status" else { return nil }
        let text = resource.primaryText.trimmingCharacters(in: .whitespacesAndNewlines)
        return text.isEmpty ? nil : RuneInspectorStatus(text)
    }

    func genericResourceOverview<Supplementary: View>(
        resource: ClusterResourceSummary,
        @ViewBuilder supplementary: () -> Supplementary
    ) -> some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorGroupSpacing) {
            RuneFactGrid("Details") {
                let labels = resource.kind.summaryFieldLabels
                // A status field is the header pill, not a row.
                if genericResourceStatus(resource) == nil {
                    inspectorInfoRow(labels.primary.title, value: resource.primaryText)
                }
                inspectorInfoRow(labels.secondary.title, value: resource.secondaryText)
            }

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
