import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    func startLiveDebugScenarioIfNeeded() {
        guard RuneRootLayoutDebug.liveScenarioEnabled else { return }
        guard !liveDebugScenarioStarted else { return }
        liveDebugScenarioStarted = true

        Task { @MainActor in
            await runLiveDebugScenario()
        }
    }

    @MainActor
    func runLiveDebugScenario() async {
        RuneRootLayoutDebug.logScenario(
            .overview,
            status: "start",
            detail: "shell=\(resolvedShellVariant.debugLabel) editor=\(resolvedManifestInlineEditorImplementation.debugLabel)"
        )

        await waitForLiveScenarioReady()

        for step in RuneRootLayoutDebug.liveScenarioSteps {
            await performLiveDebugScenarioStep(step)
        }

        RuneRootLayoutDebug.logScenario(.overview, status: "finished")

        if RuneRootLayoutDebug.liveScenarioExitWhenDone {
            NSApp.terminate(nil)
        }
    }

    @MainActor
    func waitForLiveScenarioReady() async {
        let timeout = Date().addingTimeInterval(20)
        while Date() < timeout {
            if viewModel.state.selectedContext != nil, !viewModel.visibleContexts.isEmpty {
                var appliedOverride = false

                if let contextName = RuneRootLayoutDebug.liveScenarioContextName, !contextName.isEmpty,
                   let context = viewModel.visibleContexts.first(where: { $0.name == contextName }),
                   viewModel.state.selectedContext != context {
                    viewModel.setContext(context)
                    appliedOverride = true
                }
                if let namespace = RuneRootLayoutDebug.liveScenarioNamespace, !namespace.isEmpty,
                   viewModel.state.selectedNamespace != namespace {
                    viewModel.setNamespace(namespace)
                    appliedOverride = true
                }

                let contextSettled =
                    RuneRootLayoutDebug.liveScenarioContextName == nil
                    || RuneRootLayoutDebug.liveScenarioContextName?.isEmpty == true
                    || viewModel.state.selectedContext?.name == RuneRootLayoutDebug.liveScenarioContextName
                let namespaceSettled =
                    RuneRootLayoutDebug.liveScenarioNamespace == nil
                    || RuneRootLayoutDebug.liveScenarioNamespace?.isEmpty == true
                    || viewModel.state.selectedNamespace == RuneRootLayoutDebug.liveScenarioNamespace

                if appliedOverride, contextSettled, namespaceSettled {
                    viewModel.refreshCurrentView()
                }

                if contextSettled && namespaceSettled {
                    try? await Task.sleep(nanoseconds: 700_000_000)
                    return
                }
            }
            try? await Task.sleep(nanoseconds: 200_000_000)
        }
    }

    @MainActor
    func performLiveDebugScenarioStep(_ step: RuneRootLiveDebugScenarioStep) async {
        RuneRootLayoutDebug.logScenario(step, status: "begin")

        let applied = await waitUntilLiveDebugScenarioStepCanApply(step)
        if !applied {
            RuneRootLayoutDebug.logScenario(step, status: "skip-timeout")
            return
        }

        let dwellNanoseconds = liveDebugScenarioDwellNanoseconds(for: step)
        RuneRootLayoutDebug.logScenario(step, status: "settling", detail: "dwellMs=\(dwellNanoseconds / 1_000_000)")
        try? await Task.sleep(nanoseconds: dwellNanoseconds)
        await waitUntilLiveDebugScenarioStepSettled(step)

        if let snapshot = lastLayoutSnapshot {
            RuneRootLayoutDebug.logScenario(
                step,
                status: "snapshot",
                detail: "content=(\(snapshot.contentMinX ?? -1),\(snapshot.contentMinY ?? -1)) detail=(\(snapshot.detailMinX ?? -1),\(snapshot.detailMinY ?? -1))"
            )
        } else {
            RuneRootLayoutDebug.logScenario(step, status: "snapshot-missing")
        }
        await waitForLiveDebugScenarioInteractionAcknowledgement(step)
    }

    @MainActor
    func waitForLiveDebugScenarioInteractionAcknowledgement(
        _ step: RuneRootLiveDebugScenarioStep
    ) async {
        guard step.requiresExternalInteractionAcknowledgement,
              let directory = RuneRootLayoutDebug.liveScenarioInteractionAcknowledgementDirectory,
              !directory.isEmpty else {
            try? await Task.sleep(nanoseconds: RuneRootLayoutDebug.liveScenarioSnapshotHoldNanoseconds)
            return
        }

        let acknowledgementURL = URL(fileURLWithPath: directory, isDirectory: true)
            .appendingPathComponent("\(step.rawValue).ack", isDirectory: false)
        let timeout = Date().addingTimeInterval(45)
        while Date() < timeout {
            if FileManager.default.fileExists(atPath: acknowledgementURL.path) {
                RuneRootLayoutDebug.logScenario(step, status: "interaction-acknowledged")
                return
            }
            try? await Task.sleep(nanoseconds: 50_000_000)
        }
        RuneRootLayoutDebug.logScenario(step, status: "interaction-ack-timeout")
    }

    @MainActor
    func waitUntilLiveDebugScenarioStepSettled(_ step: RuneRootLiveDebugScenarioStep) async {
        let timeout = Date().addingTimeInterval(20)
        while Date() < timeout {
            if isLiveDebugScenarioStepSettled(step) {
                return
            }
            try? await Task.sleep(nanoseconds: 250_000_000)
        }
        RuneRootLayoutDebug.logScenario(step, status: "settle-timeout")
    }

    func isLiveDebugScenarioStepSettled(_ step: RuneRootLiveDebugScenarioStep) -> Bool {
        switch step {
        case .authDoctor:
            return !viewModel.state.isRunningAuthDoctor
                && !viewModel.state.authDoctorChecks.isEmpty
        case .workloadPodLogs, .workloadDeploymentUnifiedLogs, .networkingServiceUnifiedLogs:
            return !viewModel.state.isLoading && !viewModel.state.isLoadingLogs
        case .terminalLogs:
            return !viewModel.state.isLoading
                && !viewModel.state.isLoadingLogs
                && viewModel.state.terminalSession?.status == .connected
        case .workloadDeploymentYAMLValidation:
            return !viewModel.state.isLoadingResourceDetails
                && !viewModel.state.isValidatingResourceYAML
                && !viewModel.state.resourceYAMLBaseline.isEmpty
                && viewModel.state.resourceDetailScope?.kind == KubeResourceKind.deployment.rawValue
        case .rbacCanI:
            return !viewModel.state.isLoadingResourceDetails
                && !viewModel.isRunningRBACCanI
                && viewModel.rbacCanIResult != nil
        case .workloadPodYAMLReadOnly,
             .workloadPodYAMLQuickEdit,
             .workloadPodYAMLEditorSheet,
             .workloadDeploymentYAMLReadOnly,
             .workloadDeploymentYAMLQuickEdit,
             .networkingServiceYAMLReadOnly,
             .networkingServiceYAMLQuickEdit,
             .configConfigMapYAMLReadOnly,
             .configConfigMapYAMLQuickEdit,
             .storagePVCYAML:
            return !viewModel.state.isLoadingResourceDetails
                && (!viewModel.state.resourceYAML.isEmpty || viewModel.state.lastResourceYAMLError != nil)
        case .workloadPodDescribe,
             .workloadDeploymentDescribe,
             .networkingServiceDescribe,
             .configConfigMapDescribe,
             .storagePVCDescribe,
             .rbacRole:
            return !viewModel.state.isLoadingResourceDetails
                && (!viewModel.state.resourceDescribe.isEmpty || viewModel.state.lastResourceDescribeError != nil)
        default:
            return !viewModel.state.isLoading
        }
    }

    @MainActor
    func waitUntilLiveDebugScenarioStepCanApply(_ step: RuneRootLiveDebugScenarioStep) async -> Bool {
        let timeout = Date().addingTimeInterval(25)
        repeat {
            // Re-selecting the section invalidates its snapshot request. Let
            // a pending read finish before retrying a fixture-dependent step.
            if !viewModel.state.isLoading {
                if applyLiveDebugScenarioStep(step) {
                    return true
                }
                viewModel.refreshCurrentView()
            }
            try? await Task.sleep(nanoseconds: 500_000_000)
        } while Date() < timeout
        return false
    }

    func liveDebugScenarioDwellNanoseconds(for step: RuneRootLiveDebugScenarioStep) -> UInt64 {
        switch step {
        case .workloadPodOverview,
             .workloadPodLogs,
             .workloadPodExec,
             .workloadPodPortForward,
             .workloadPodYAMLReadOnly,
             .workloadPodYAMLQuickEdit,
             .workloadPodYAMLEditorSheet,
             .workloadPodDescribe:
            return RuneRootLayoutDebug.liveScenarioPodDwellNanoseconds
        case .workloadDeploymentUnifiedLogs, .networkingServiceUnifiedLogs:
            return max(RuneRootLayoutDebug.liveScenarioPodDwellNanoseconds, 2_500_000_000)
        default:
            return 900_000_000
        }
    }

    @MainActor
    func applyLiveDebugScenarioStep(_ step: RuneRootLiveDebugScenarioStep) -> Bool {
        if !step.presentsYAMLEditorSheet {
            isYAMLEditorSheetPresented = false
        }

        switch step {
        case .overview:
            viewModel.setSection(.overview)
            return true
        case .authDoctor:
            viewModel.setSection(.overview)
            isAuthDoctorPanelExpanded = true
            if viewModel.state.authDoctorChecks.isEmpty,
               !viewModel.state.isRunningAuthDoctor {
                viewModel.runAuthDoctor()
            }
            return true
        case .workloadPodOverview:
            guard let pod = viewModel.visiblePods.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.selectPod(pod)
            podInspectorTab = .overview
            yamlManifestIsEditing = false
            return true
        case .workloadPodLogs:
            guard let pod = viewModel.state.selectedPod ?? viewModel.visiblePods.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.pod)
            viewModel.selectPod(pod)
            podInspectorTab = .logs
            yamlManifestIsEditing = false
            viewModel.reloadLogsForSelection()
            return true
        case .workloadPodExec:
            guard let pod = viewModel.state.selectedPod ?? viewModel.visiblePods.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.pod)
            viewModel.selectPod(pod)
            podInspectorTab = .exec
            yamlManifestIsEditing = false
            return true
        case .workloadPodPortForward:
            guard let pod = viewModel.state.selectedPod ?? viewModel.visiblePods.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.pod)
            viewModel.selectPod(pod)
            podInspectorTab = .portForward
            yamlManifestIsEditing = false
            return true
        case .workloadPodYAMLReadOnly:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.pod)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .workloadPodYAMLQuickEdit:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.pod)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .workloadPodYAMLEditorSheet:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.pod)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .yaml
            yamlManifestIsEditing = false
            isYAMLEditorSheetPresented = true
            return true
        case .workloadPodDescribe:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.pod)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .describe
            yamlManifestIsEditing = false
            return true
        case .workloadDeploymentOverview:
            guard let deployment = viewModel.visibleDeployments.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.selectDeployment(deployment)
            deploymentInspectorTab = .overview
            yamlManifestIsEditing = false
            return true
        case .workloadDeploymentUnifiedLogs:
            guard let deployment = viewModel.state.selectedDeployment ?? viewModel.visibleDeployments.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.deployment)
            viewModel.selectDeployment(deployment)
            deploymentInspectorTab = .unifiedLogs
            yamlManifestIsEditing = false
            viewModel.reloadLogsForSelection()
            return true
        case .workloadDeploymentRollout:
            guard let deployment = viewModel.state.selectedDeployment ?? viewModel.visibleDeployments.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.deployment)
            viewModel.selectDeployment(deployment)
            deploymentInspectorTab = .rollout
            yamlManifestIsEditing = false
            return true
        case .workloadDeploymentYAMLReadOnly:
            guard viewModel.state.selectedDeployment != nil || viewModel.visibleDeployments.first != nil else { return false }
            if viewModel.state.selectedDeployment == nil, let deployment = viewModel.visibleDeployments.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.deployment)
                viewModel.selectDeployment(deployment)
            }
            deploymentInspectorTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .workloadDeploymentYAMLQuickEdit:
            guard viewModel.state.selectedDeployment != nil || viewModel.visibleDeployments.first != nil else { return false }
            if viewModel.state.selectedDeployment == nil, let deployment = viewModel.visibleDeployments.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.deployment)
                viewModel.selectDeployment(deployment)
            }
            deploymentInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .workloadDeploymentYAMLValidation:
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.deployment)
            guard let deployment = viewModel.state.selectedDeployment
                    ?? viewModel.visibleDeployments.first else {
                return false
            }
            if viewModel.state.selectedDeployment?.id != deployment.id {
                viewModel.selectDeployment(deployment)
            }
            deploymentInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            let expectedScope = ResourceDetailScope(
                contextName: viewModel.state.selectedContext?.name ?? "",
                namespace: deployment.namespace,
                kind: .deployment,
                name: deployment.name
            )
            guard viewModel.state.resourceDetailScope == expectedScope,
                  !viewModel.state.isLoadingResourceDetails,
                  !viewModel.state.resourceYAMLBaseline.isEmpty else {
                if !viewModel.state.isLoadingResourceDetails {
                    viewModel.selectDeployment(deployment)
                }
                return false
            }
            return true
        case .workloadDeploymentDescribe:
            guard viewModel.state.selectedDeployment != nil || viewModel.visibleDeployments.first != nil else { return false }
            if viewModel.state.selectedDeployment == nil, let deployment = viewModel.visibleDeployments.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.deployment)
                viewModel.selectDeployment(deployment)
            }
            deploymentInspectorTab = .describe
            yamlManifestIsEditing = false
            return true
        case .networkingServiceOverview:
            viewModel.setSection(.networking)
            viewModel.setWorkloadKind(.service)
            guard let service = viewModel.visibleServices.first else { return false }
            viewModel.selectService(service)
            serviceInspectorTab = .overview
            yamlManifestIsEditing = false
            return true
        case .networkingServiceUnifiedLogs:
            guard let service = viewModel.state.selectedService ?? viewModel.visibleServices.first else { return false }
            viewModel.setSection(.networking)
            viewModel.setWorkloadKind(.service)
            viewModel.selectService(service)
            serviceInspectorTab = .unifiedLogs
            yamlManifestIsEditing = false
            viewModel.reloadLogsForSelection()
            return true
        case .networkingServicePortForward:
            guard let service = viewModel.state.selectedService ?? viewModel.visibleServices.first else { return false }
            viewModel.setSection(.networking)
            viewModel.setWorkloadKind(.service)
            viewModel.selectService(service)
            serviceInspectorTab = .portForward
            yamlManifestIsEditing = false
            return true
        case .networkingServiceYAMLReadOnly:
            guard viewModel.state.selectedService != nil || viewModel.visibleServices.first != nil else { return false }
            if viewModel.state.selectedService == nil, let service = viewModel.visibleServices.first {
                viewModel.setSection(.networking)
                viewModel.setWorkloadKind(.service)
                viewModel.selectService(service)
            }
            serviceInspectorTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .networkingServiceYAMLQuickEdit:
            guard viewModel.state.selectedService != nil || viewModel.visibleServices.first != nil else { return false }
            if viewModel.state.selectedService == nil, let service = viewModel.visibleServices.first {
                viewModel.setSection(.networking)
                viewModel.setWorkloadKind(.service)
                viewModel.selectService(service)
            }
            serviceInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .networkingServiceDescribe:
            guard viewModel.state.selectedService != nil || viewModel.visibleServices.first != nil else { return false }
            if viewModel.state.selectedService == nil, let service = viewModel.visibleServices.first {
                viewModel.setSection(.networking)
                viewModel.setWorkloadKind(.service)
                viewModel.selectService(service)
            }
            serviceInspectorTab = .describe
            yamlManifestIsEditing = false
            return true
        case .configConfigMapPrepare:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            if let configMap = viewModel.visibleConfigMaps.first {
                viewModel.selectConfigMap(configMap)
            } else {
                viewModel.refreshCurrentView()
            }
            yamlManifestIsEditing = false
            return true
        case .configConfigMapYAMLReadOnly:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            guard let configMap = viewModel.visibleConfigMaps.first else { return false }
            viewModel.selectConfigMap(configMap)
            genericResourceManifestTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .configConfigMapYAMLQuickEdit:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            guard let configMap = viewModel.visibleConfigMaps.first else { return false }
            viewModel.selectConfigMap(configMap)
            genericResourceManifestTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .configConfigMapDescribe:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            guard viewModel.state.selectedConfigMap != nil || viewModel.visibleConfigMaps.first != nil else { return false }
            if viewModel.state.selectedConfigMap == nil, let configMap = viewModel.visibleConfigMaps.first {
                viewModel.selectConfigMap(configMap)
            }
            genericResourceManifestTab = .describe
            yamlManifestIsEditing = false
            return true
        case .storagePVCDescribe:
            viewModel.setSection(.storage)
            viewModel.setWorkloadKind(.persistentVolumeClaim)
            guard let pvc = viewModel.visiblePersistentVolumeClaims.first else { return false }
            viewModel.selectPersistentVolumeClaim(pvc)
            genericResourceManifestTab = .describe
            yamlManifestIsEditing = false
            return true
        case .storagePVCYAML:
            viewModel.setSection(.storage)
            viewModel.setWorkloadKind(.persistentVolumeClaim)
            guard let pvc = viewModel.visiblePersistentVolumeClaims.first else { return false }
            viewModel.selectPersistentVolumeClaim(pvc)
            genericResourceManifestTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .eventsDetail:
            viewModel.setSection(.events)
            guard let event = viewModel.visibleEvents.first else { return false }
            viewModel.selectEvent(event)
            yamlManifestIsEditing = false
            return true
        case .rbacRole:
            viewModel.setSection(.rbac)
            viewModel.setWorkloadKind(.role)
            guard let resource = viewModel.visibleRBACResources.first else { return false }
            viewModel.selectRBACResource(resource)
            genericResourceManifestTab = .describe
            yamlManifestIsEditing = false
            return true
        case .rbacCanI:
            viewModel.setSection(.rbac)
            viewModel.setWorkloadKind(.role)
            guard let resource = viewModel.state.selectedRBACResource
                    ?? viewModel.visibleRBACResources.first else {
                return false
            }
            viewModel.selectRBACResource(resource)
            genericResourceManifestTab = .overview
            yamlManifestIsEditing = false
            viewModel.useRBACCanIPreset(
                verb: "get",
                resource: "pods",
                apiGroup: nil,
                subresource: "log",
                scope: .namespace
            )
            viewModel.runRBACCanISimulator()
            return true
        case .terminal:
            viewModel.setSection(.terminal)
            terminalInspectorTab = .commands
            yamlManifestIsEditing = false
            return true
        case .terminalLogs:
            viewModel.setSection(.terminal)
            terminalInspectorTab = .logs
            yamlManifestIsEditing = false
            guard let pod = terminalLogActivePod ?? terminalInitialLogPod else { return false }
            ensureTerminalLogTabs(for: pod)
            let additionalPods = viewModel.state.pods
                .filter {
                    $0.id != pod.id
                        && $0.status.localizedCaseInsensitiveCompare("Running") == .orderedSame
                }
                .prefix(2)
            for additionalPod in additionalPods
            where !terminalLogTabState.tabs.contains(where: { $0.podID == additionalPod.id }) {
                terminalLogTabState.add(preferredPod: additionalPod)
            }
            if let primaryTabID = terminalLogTabState.tabs.first(where: { $0.podID == pod.id })?.id {
                _ = terminalLogTabState.select(id: primaryTabID, availablePods: viewModel.state.pods)
            }
            viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
            let shellPods = [pod] + Array(additionalPods.prefix(1))
            for shellPod in shellPods {
                viewModel.startTerminalSession(for: shellPod)
            }
            return true
        }
    }
}
