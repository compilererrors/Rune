import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var yamlManifestDocumentState: ManifestDocumentState {
        ManifestDocumentState.resolved(
            content: viewModel.state.resourceYAML,
            isLoading: viewModel.state.isLoadingResourceDetails,
            error: viewModel.state.lastResourceYAMLError,
            loadingTitle: "Loading YAML",
            loadingMessage: "Fetching \(manifestResourceReference) from the cluster.",
            failureTitle: "YAML could not be refreshed",
            emptyTitle: "No YAML available",
            emptyMessage: "The cluster returned no manifest for \(manifestResourceReference)."
        )
    }

    var yamlFooterText: String {
        if viewModel.state.isLoadingResourceDetails {
            return "Loading resource YAML from the cluster."
        }
        if viewModel.state.lastResourceYAMLError != nil {
            return "YAML could not be loaded for the current selection. Check context, namespace, and cluster access in Settings."
        }
        return "No YAML was returned for the current selection yet. You can also use Import… to paste YAML from a file."
    }

    var describeManifestDocumentState: ManifestDocumentState {
        ManifestDocumentState.resolved(
            content: viewModel.state.resourceDescribe,
            isLoading: viewModel.state.isLoadingResourceDetails,
            error: viewModel.state.lastResourceDescribeError,
            loadingTitle: "Loading describe output",
            loadingMessage: "Fetching \(manifestResourceReference) from the cluster.",
            failureTitle: "Describe could not be refreshed",
            emptyTitle: "No describe output",
            emptyMessage: "The cluster returned no describe content for \(manifestResourceReference)."
        )
    }

    var logStatusText: String {
        if viewModel.state.isLoadingLogs {
            return "Refreshing logs"
        }
        if let error = viewModel.state.lastLogFetchError?.trimmingCharacters(in: .whitespacesAndNewlines), !error.isEmpty {
            return "Reconnect failed"
        }
        if let date = viewModel.state.lastLogUpdatedAt {
            if viewModel.isLogTailModeEnabled {
                if viewModel.isLogStreamPaused {
                    return "Paused - last updated \(date.formatted(date: .omitted, time: .standard))"
                }
                return "Live tailing - updated \(date.formatted(date: .omitted, time: .standard))"
            }
            return "Tail off - last updated \(date.formatted(date: .omitted, time: .standard))"
        }
        if viewModel.isLogTailModeEnabled {
            if viewModel.isLogStreamPaused {
                return "Paused"
            }
            return "Live tailing"
        }
        if viewModel.includePreviousLogs {
            return "Previous logs requested"
        }
        return "No log read yet"
    }

    var manifestStatusText: String {
        if viewModel.state.isLoadingResourceDetails {
            return "Refreshing details"
        }
        if viewModel.state.lastResourceYAMLError != nil || viewModel.state.lastResourceDescribeError != nil {
            return "Stale details"
        }
        if let date = viewModel.state.lastResourceDetailsUpdatedAt {
            return "Last updated \(date.formatted(date: .omitted, time: .standard))"
        }
        return "No detail read yet"
    }

    var yamlDraftBinding: Binding<String> {
        Binding(
            get: { viewModel.state.resourceYAML },
            set: { viewModel.state.updateResourceYAMLDraft($0) }
        )
    }

    var manifestResourceReference: String {
        if viewModel.state.selectedSection == .helm,
           let resource = viewModel.state.selectedOperatorResource {
            return "\(resource.kind) \(resource.name)"
        }

        switch viewModel.state.selectedWorkloadKind {
        case .pod:
            return viewModel.state.selectedPod.map { "pod \($0.name)" } ?? "the selected pod"
        case .deployment:
            return viewModel.state.selectedDeployment.map { "deployment \($0.name)" } ?? "the selected deployment"
        case .service:
            return viewModel.state.selectedService.map { "service \($0.name)" } ?? "the selected service"
        case .statefulSet:
            return viewModel.state.selectedStatefulSet.map { "statefulset \($0.name)" } ?? "the selected statefulset"
        case .daemonSet:
            return viewModel.state.selectedDaemonSet.map { "daemonset \($0.name)" } ?? "the selected daemonset"
        case .job:
            return viewModel.state.selectedJob.map { "job \($0.name)" } ?? "the selected job"
        case .cronJob:
            return viewModel.state.selectedCronJob.map { "cronjob \($0.name)" } ?? "the selected cronjob"
        case .replicaSet:
            return viewModel.state.selectedReplicaSet.map { "replicaset \($0.name)" } ?? "the selected replicaset"
        case .ingress:
            return viewModel.state.selectedIngress.map { "ingress \($0.name)" } ?? "the selected ingress"
        case .endpoint:
            return viewModel.state.selectedEndpoint.map { "endpoints \($0.name)" } ?? "the selected endpoints"
        case .configMap:
            return viewModel.state.selectedConfigMap.map { "configmap \($0.name)" } ?? "the selected configmap"
        case .secret:
            return viewModel.state.selectedSecret.map { "secret \($0.name)" } ?? "the selected secret"
        case .node:
            return viewModel.state.selectedNode.map { "node \($0.name)" } ?? "the selected node"
        case .persistentVolumeClaim:
            return viewModel.state.selectedPersistentVolumeClaim.map { "pvc \($0.name)" } ?? "the selected PVC"
        case .persistentVolume:
            return viewModel.state.selectedPersistentVolume.map { "pv \($0.name)" } ?? "the selected PV"
        case .storageClass:
            return viewModel.state.selectedStorageClass.map { "storageclass \($0.name)" } ?? "the selected StorageClass"
        case .horizontalPodAutoscaler:
            return viewModel.state.selectedHorizontalPodAutoscaler.map { "hpa \($0.name)" } ?? "the selected HPA"
        case .networkPolicy:
            return viewModel.state.selectedNetworkPolicy.map { "networkpolicy \($0.name)" } ?? "the selected NetworkPolicy"
        case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            return viewModel.state.selectedRBACResource.map { "\($0.kind.kubernetesResourceName) \($0.name)" } ?? "the selected RBAC resource"
        case .event:
            return "the selected event"
        }
    }

    var genericResourceEmptyState: some View {
        if viewModel.state.isLoading || viewModel.state.isLoadingResourceDetails {
            return AnyView(
                inspectorEmptyState(
                    .loading(
                        title: "Loading \(viewModel.state.selectedWorkloadKind.title)",
                        message: "Loading resources and manifest details for the active namespace."
                    ),
                    symbol: "hourglass"
                )
            )
        }

        let availableCount: Int = {
            switch viewModel.state.selectedWorkloadKind {
            case .statefulSet: return viewModel.visibleStatefulSets.count
            case .daemonSet: return viewModel.visibleDaemonSets.count
            case .job: return viewModel.visibleJobs.count
            case .cronJob: return viewModel.visibleCronJobs.count
            case .replicaSet: return viewModel.visibleReplicaSets.count
            case .horizontalPodAutoscaler: return viewModel.visibleHorizontalPodAutoscalers.count
            case .ingress: return viewModel.visibleIngresses.count
            case .endpoint: return viewModel.visibleEndpoints.count
            case .networkPolicy: return viewModel.visibleNetworkPolicies.count
            case .persistentVolumeClaim: return viewModel.visiblePersistentVolumeClaims.count
            case .persistentVolume: return viewModel.visiblePersistentVolumes.count
            case .storageClass: return viewModel.visibleStorageClasses.count
            case .configMap: return viewModel.visibleConfigMaps.count
            case .secret: return viewModel.visibleSecrets.count
            case .node: return viewModel.visibleNodes.count
            case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding: return viewModel.visibleRBACResources.count
            default: return 0
            }
        }()

        if availableCount == 0 {
            return AnyView(
                inspectorEmptyState(
                    .empty(
                        title: "No \(viewModel.state.selectedWorkloadKind.title.lowercased()) found",
                        message: "Nothing is available in the current namespace yet. YAML and describe will appear here when a resource is selected."
                    ),
                    symbol: "tray"
                )
            )
        }

        return AnyView(
            inspectorEmptyState(
                .unselected(
                    title: "Select a resource",
                    message: "Select an item in the center list to inspect details and actions here."
                ),
                symbol: "list.bullet.rectangle"
            )
        )
    }

    func openYAMLEditorSheet() {
        yamlManifestIsEditing = false
        isYAMLEditorSheetPresented = true
    }

    func yamlManifestEditorSheet() -> some View {
        ResourceYAMLEditorSheetView(
            resourceReference: manifestResourceReference,
            yamlText: yamlDraftBinding,
            yamlFooterText: yamlFooterText,
            canApplyMutations: viewModel.canApplyClusterMutations && viewModel.state.selectedOperatorResource == nil,
            hasUnsavedEdits: viewModel.state.resourceYAMLHasUnsavedEdits,
            validationIssues: viewModel.state.resourceYAMLValidationIssues,
            isValidating: viewModel.state.isValidatingResourceYAML,
            canUndoEdit: viewModel.state.canUndoResourceYAMLEdit,
            canReapplySnapshot: viewModel.canReapplyResourceYAMLBaseline,
            canDryRun: viewModel.canDryRunSelectedResourceYAML,
            isRunningDryRun: viewModel.isRunningResourceYAMLDryRun,
            dryRunStatus: viewModel.resourceYAMLDryRunStatus,
            onApply: { viewModel.requestApplySelectedResourceYAML() },
            onDryRun: { viewModel.requestDryRunSelectedResourceYAML() },
            onReapplySnapshot: { viewModel.requestReapplyResourceYAMLBaseline() },
            onUndoEdit: { viewModel.undoResourceYAMLEdit() },
            onRevert: { viewModel.revertResourceYAMLDraft() },
            onImport: { viewModel.importResourceYAMLFromFile() },
            onExport: { viewModel.saveCurrentResourceYAML() },
            onExportToExportFolder: { viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: false) },
            onExportAndOpen: { viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: true) },
            onClose: { isYAMLEditorSheetPresented = false },
            notice: viewModel.state.activeNotice,
            onDismissNotice: { viewModel.state.clearError() },
            onOpenSavedFolder: viewModel.openSavedLogFolder,
            onOpenSavedFile: viewModel.openSavedLogFile,
            documentState: yamlManifestDocumentState,
            editorRestorationRequest: viewModel.state.resourceYAMLEditorRestorationRequest,
            onEditorEdit: { yaml, presentation in
                viewModel.updateResourceYAMLDraft(
                    yaml,
                    editorPresentation: presentation
                )
            }
        )
    }

    var yamlBlock: some View {
        ResourceYAMLInspectorPane(
            resourceReference: manifestResourceReference,
            documentIdentity: viewModel.state.resourceDetailScope,
            yamlText: yamlDraftBinding,
            yamlDisplayText: viewModel.state.resourceYAML,
            yamlFooterText: yamlFooterText,
            baseline: viewModel.state.resourceYAMLBaseline,
            hasUnsavedEdits: viewModel.state.resourceYAMLHasUnsavedEdits,
            canApplyMutations: viewModel.canApplyClusterMutations && viewModel.state.selectedOperatorResource == nil,
            validationIssues: viewModel.state.resourceYAMLValidationIssues,
            isValidating: viewModel.state.isValidatingResourceYAML,
            statusText: manifestStatusText,
            canUndoEdit: viewModel.state.canUndoResourceYAMLEdit,
            canReapplySnapshot: viewModel.canReapplyResourceYAMLBaseline,
            isInlineEditing: $yamlManifestIsEditing,
            inlineEditorImplementation: resolvedManifestInlineEditorImplementation,
            onApply: { viewModel.requestApplySelectedResourceYAML() },
            onReapplySnapshot: { viewModel.requestReapplyResourceYAMLBaseline() },
            onOpenEditor: { openYAMLEditorSheet() },
            onUndoEdit: { viewModel.undoResourceYAMLEdit() },
            onRevert: { viewModel.revertResourceYAMLDraft() },
            onImport: { viewModel.importResourceYAMLFromFile() },
            onExport: { viewModel.saveCurrentResourceYAML() },
            onExportToExportFolder: { viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: false) },
            onExportAndOpen: { viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: true) },
            readOnlyResetID: "yaml:\(manifestResourceReference):\(viewModel.state.selectedSection.rawValue):\(viewModel.state.selectedWorkloadKind.kubernetesResourceName)",
            documentState: yamlManifestDocumentState,
            editorRestorationRequest: viewModel.state.resourceYAMLEditorRestorationRequest,
            onEditorEdit: { yaml, presentation in
                viewModel.updateResourceYAMLDraft(
                    yaml,
                    editorPresentation: presentation
                )
            }
        )
    }

    /// One pane at a time — avoids `ZStack` + opacity (both branches still participated in layout, causing width drift and editor jumping when switching YAML/Describe).
    @ViewBuilder
    func manifestInspectorPane(activeTab: GenericResourceManifestTab) -> some View {
        switch activeTab {
        case .overview:
            EmptyView()
        case .yaml:
            yamlBlock
        case .describe:
            describeBlock
        }
    }

    /// Describe tab: read-only describe output; cluster updates use the YAML manifest (same buffer as the YAML tab) and Apply.
    var describeBlock: some View {
        ResourceDescribeInspectorPane(
            describeText: viewModel.state.resourceDescribe,
            resourceReference: manifestResourceReference,
            canApplyMutations: viewModel.canApplyClusterMutations && viewModel.state.selectedOperatorResource == nil,
            yamlText: viewModel.state.resourceYAML,
            hasUnsavedEdits: viewModel.state.resourceYAMLHasUnsavedEdits,
            validationIssues: viewModel.state.resourceYAMLValidationIssues,
            statusText: manifestStatusText,
            onApply: { viewModel.requestApplySelectedResourceYAML() },
            onOpenYAMLEditor: { openYAMLEditorSheet() },
            onExport: { viewModel.saveCurrentResourceDescribe() },
            onExportToExportFolder: { viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: false) },
            onExportAndOpen: { viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: true) },
            readOnlyResetID: "describe:\(manifestResourceReference):\(viewModel.state.selectedSection.rawValue):\(viewModel.state.selectedWorkloadKind.kubernetesResourceName)",
            documentState: describeManifestDocumentState
        )
    }

    func exportableTextPane(text: String, emptyText: String) -> some View {
        InspectorTextSurface(minHeight: 220) {
            InspectorReadOnlyTextView(
                text: text.isEmpty ? emptyText : text,
                resetID: "export:\(emptyText):\((text.isEmpty ? emptyText : text).count)"
            )
        }
    }

    func execPane(for pod: PodSummary) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.sm) {
            // Command and its actions are one cluster on a context card. The
            // progress spinner keeps its slot, so running never moves a button.
            RuneControlCard(fillsWidth: true) {
                VStack(alignment: .leading, spacing: RuneSpacing.xs) {
                    TextField("Command", text: $viewModel.execCommandInput)
                        .textFieldStyle(RuneControlTextFieldStyle())

                    HStack(spacing: RuneSpacing.xs) {
                        Button("Run in Pod") {
                            viewModel.requestExecInSelectedPod()
                        }
                        .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                        .disabled(viewModel.state.isExecutingCommand || !viewModel.canApplyClusterMutations)

                        Button("Open Terminal") {
                            viewModel.startTerminalSession(for: pod)
                        }
                        .disabled(!viewModel.canApplyClusterMutations)

                        ProgressView()
                            .controlSize(.small)
                            .opacity(viewModel.state.isExecutingCommand ? 1 : 0)
                            .accessibilityHidden(!viewModel.state.isExecutingCommand)

                        Spacer()

                        if let result = viewModel.state.lastExecResult, result.podName == pod.name {
                            Text("Exit code: \(result.exitCode)")
                                .runeText(.microEmphasis)
                                .foregroundStyle(result.exitCode == 0 ? RuneTextStyle.secondary : .danger)
                        }
                    }
                }
            }

            if let result = viewModel.state.lastExecResult, result.podName == pod.name {
                VStack(alignment: .leading, spacing: RuneSpacing.xs) {
                    Text(result.command.joined(separator: " "))
                        .runeText(.microEmphasis)
                        .foregroundStyle(.runeSecondary)

                    InspectorReadOnlyTextSurface(
                        text: execOutputText(for: result),
                        minHeight: 220,
                        resetID: "exec:\(result.podName):\(result.command.joined(separator: " ")):\(result.exitCode)"
                    )
                }
            } else {
                Text("Run a command to see stdout/stderr here.")
                    .foregroundStyle(.runeSecondary)
            }
        }
    }

    func portForwardPane(targetKind: PortForwardTargetKind, targetName: String) -> some View {
        let matchingNamespace = viewModel.state.selectedNamespace
        let matchingSessions = viewModel.state.portForwardSessions.filter {
            $0.targetKind == targetKind
                && $0.targetName == targetName
                && $0.namespace == matchingNamespace
        }
        let activeSession = matchingSessions.first {
            $0.contextName == viewModel.state.selectedContext?.name && $0.isActiveOrStarting
        }

        return VStack(alignment: .leading, spacing: RuneSpacing.sm) {
            // Endpoints and their actions are one cluster on a context card.
            RuneControlCard(fillsWidth: true) {
                VStack(alignment: .leading, spacing: RuneSpacing.xs) {
                    PortForwardEndpointFields(
                        localPort: $viewModel.portForwardLocalPortInput,
                        remotePort: $viewModel.portForwardRemotePortInput,
                        address: $viewModel.portForwardAddressInput
                    )

                    PortForwardPrimaryActionLayout {
                        PortForwardPrimaryActionButton(
                            activeSession: activeSession,
                            startTitle: "Start Port Forward",
                            isStartDisabled: viewModel.state.isStartingPortForward,
                            onStart: viewModel.startPortForwardForSelection,
                            onStop: viewModel.stopPortForward
                        )
                    } utilities: {
                        HStack(spacing: RuneSpacing.xs) {
                            Button("Open Terminal") {
                                viewModel.setSection(.terminal)
                            }

                            ProgressView()
                                .controlSize(.small)
                                .opacity(viewModel.state.isStartingPortForward ? 1 : 0)
                                .accessibilityHidden(!viewModel.state.isStartingPortForward)
                        }
                    }
                }
            }

            if matchingSessions.isEmpty {
                Text("No port-forward sessions for this \(targetKind.title.lowercased()).")
                    .foregroundStyle(.runeSecondary)
            } else {
                HStack {
                    Text(matchingSessions.contains(where: \.isActiveOrStarting) ? "Port forwards for this \(targetKind.title.lowercased())" : "Recent port forwards for this \(targetKind.title.lowercased())")
                        .runeText(.microEmphasis)
                    Spacer(minLength: 0)
                    if matchingSessions.contains(where: \.isInactive) {
                        Button("Clear Inactive") {
                            viewModel.clearInactivePortForwardSessions(
                                targetKind: targetKind,
                                targetName: targetName,
                                namespace: matchingNamespace
                            )
                        }
                        .controlSize(.small)
                        .help("Remove stopped and failed port-forward rows")
                    }
                }
                ForEach(matchingSessions) { session in
                    portForwardSessionRow(session)
                }
            }
        }
    }
}
