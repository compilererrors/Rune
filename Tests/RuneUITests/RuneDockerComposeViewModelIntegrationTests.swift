import Foundation
import XCTest
@testable import RuneCore
@testable import RuneDiagnostics
@testable import RuneExport
@testable import RuneKube
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

@MainActor
final class RuneDockerComposeViewModelIntegrationTests: XCTestCase {
    func testDockerComposeViewModelCoversPrimarySectionsInspectorsAndTerminalPodLogs() async throws {
        let harness = try makeHarness()
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = false
        defer { restoreDemoSetting(previousDemoSetting) }

        try await harness.viewModel.reloadContexts()
        try await selectOrbitContext(in: harness)
        try await waitUntil(timeout: 20) {
            harness.state.contexts.map(\.name).contains("fake-orbit-mesh")
                && harness.state.contexts.map(\.name).contains("fake-lattice-spark")
                && harness.state.selectedContext?.name == "fake-orbit-mesh"
                && harness.state.selectedNamespace == "alpha-zone"
                && !harness.state.overviewPods.isEmpty
                && !harness.state.isLoading
        }

        XCTAssertFalse(harness.viewModel.visibleContexts.map(\.name).contains("rune-demo"))
        XCTAssertGreaterThanOrEqual(harness.state.overviewDeploymentsCount, 2)
        XCTAssertGreaterThanOrEqual(harness.state.overviewServicesCount, 1)
        XCTAssertGreaterThanOrEqual(harness.state.overviewNodesCount, 1)

        harness.viewModel.setSection(.workloads)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .workloads
                && harness.state.selectedWorkloadKind == .pod
                && harness.state.pods.contains { $0.name.hasPrefix("orbit-lens-") }
                && !harness.state.isLoading
                && !harness.state.isLoadingResourceDetails
        }

        let pod = try XCTUnwrap(harness.state.pods.first { $0.name.hasPrefix("orbit-lens-") })
        harness.viewModel.selectPod(pod)
        harness.viewModel.reloadLogsForSelection()
        try await waitUntil(timeout: 20) {
            harness.state.selectedPod?.id == pod.id
                && harness.state.resourceYAML.contains(pod.name)
                && harness.state.resourceDescribe.contains(pod.name)
                && !harness.state.podLogs.isEmpty
                && !harness.state.isLoadingLogs
                && !harness.state.isLoadingResourceDetails
        }

        harness.viewModel.setWorkloadKind(.deployment)
        try await waitUntil(timeout: 20) {
            harness.state.selectedWorkloadKind == .deployment
                && harness.state.deployments.map(\.name).contains("orbit-lens")
                && !harness.state.isLoading
        }
        harness.viewModel.selectDeployment(harness.state.deployments.first { $0.name == "orbit-lens" })
        try await waitUntil(timeout: 20) {
            harness.state.selectedDeployment?.name == "orbit-lens"
                && harness.state.resourceYAML.contains("orbit-lens")
                && harness.state.resourceDescribe.contains("orbit-lens")
                && !harness.state.isLoadingResourceDetails
        }

        harness.viewModel.setSection(.networking)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .networking
                && harness.state.selectedWorkloadKind == .service
                && harness.state.services.map(\.name).contains("orbit-lens")
                && !harness.state.isLoading
        }
        harness.viewModel.selectService(harness.state.services.first { $0.name == "orbit-lens" })
        try await waitUntil(timeout: 20) {
            harness.state.selectedService?.name == "orbit-lens"
                && harness.state.resourceYAML.contains("orbit-lens")
                && !harness.state.isLoadingResourceDetails
        }

        harness.viewModel.setSection(.config)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .config
                && harness.state.configMaps.map(\.name).contains("orbit-grid")
                && !harness.state.isLoading
        }
        harness.viewModel.selectConfigMap(harness.state.configMaps.first { $0.name == "orbit-grid" })
        try await waitUntil(timeout: 20) {
            harness.state.selectedConfigMap?.name == "orbit-grid"
                && harness.state.resourceYAML.contains("orbit-grid")
                && !harness.state.isLoadingResourceDetails
        }

        harness.viewModel.setSection(.rbac)
        try await waitUntil(timeout: 30) {
            harness.state.selectedSection == .rbac
                && harness.state.rbacRoles.map(\.name).contains("alpha-reader")
                && harness.state.rbacRoleBindings.map(\.name).contains("alpha-reader-binding")
                && !harness.state.rbacClusterRoles.isEmpty
                && !harness.state.rbacClusterRoleBindings.isEmpty
                && !harness.state.isLoading
        }

        harness.viewModel.setSection(.storage)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .storage
                && !harness.state.persistentVolumeClaims.isEmpty
                && !harness.state.isLoading
        }

        harness.viewModel.setSection(.events)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .events
                && !harness.state.events.isEmpty
                && !harness.state.isLoading
        }
        XCTAssertTrue(harness.state.events.contains { event in
            harness.state.pods.contains { $0.name == event.objectName }
        })

        harness.viewModel.setSection(.workloads)
        harness.viewModel.setWorkloadKind(.pod)
        try await waitUntil(timeout: 30) {
            harness.state.selectedSection == .workloads
                && harness.state.selectedWorkloadKind == .pod
                && harness.state.pods.contains { $0.name.hasPrefix("orbit-lens-") }
                && !harness.state.isLoading
        }
        let terminalPod = try XCTUnwrap(harness.state.pods.first { $0.name.hasPrefix("orbit-lens-") })

        harness.viewModel.setSection(.helm)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .helm
                && harness.state.helmReleases.map(\.name).contains("orbit-lens")
                && !harness.state.isLoading
        }

        harness.viewModel.setSection(.terminal)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .terminal
                && harness.state.pods.contains { $0.id == terminalPod.id }
                && !harness.state.isLoading
                && !harness.state.isLoadingResourceDetails
        }
        harness.viewModel.focusTerminalPodInspector(terminalPod, reloadLogs: true, loadDetails: false)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .terminal
                && harness.state.selectedPod?.id == terminalPod.id
                && !harness.state.podLogs.isEmpty
                && !harness.state.isLoadingLogs
        }

        XCTAssertNil(harness.state.lastError)
        XCTAssertNil(harness.state.lastLogFetchError)
        XCTAssertNil(harness.state.lastResourceYAMLError)
        XCTAssertNil(harness.state.lastResourceDescribeError)
    }

    func testDockerComposeViewModelCoversResourceKindTabsWithoutDemoFallback() async throws {
        let harness = try makeHarness()
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = false
        defer { restoreDemoSetting(previousDemoSetting) }

        try await harness.viewModel.reloadContexts()
        try await selectOrbitContext(in: harness)
        try await waitUntil(timeout: 20) {
            harness.state.selectedContext?.name == "fake-orbit-mesh"
                && harness.state.selectedNamespace == "alpha-zone"
                && !harness.state.isLoading
        }

        try await assertKindLoads(.workloads, .pod, in: harness) { !$0.state.pods.isEmpty }
        try await assertKindLoads(.workloads, .deployment, in: harness) { !$0.state.deployments.isEmpty }
        try await assertKindLoads(.workloads, .statefulSet, in: harness) { $0.state.statefulSets.map(\.name).contains("orbit-vault") }
        try await assertKindLoads(.workloads, .daemonSet, in: harness) { $0.state.daemonSets.map(\.name).contains("alpha-node-shadow") }
        try await assertKindLoads(.workloads, .job, in: harness) { $0.state.jobs.map(\.name).contains("orbit-smoke-once") }
        try await assertKindLoads(.workloads, .cronJob, in: harness) { $0.state.cronJobs.map(\.name).contains("orbit-sweep-cycle") }
        try await assertKindLoads(.workloads, .replicaSet, in: harness) { !$0.state.replicaSets.isEmpty }
        try await assertKindLoads(.workloads, .horizontalPodAutoscaler, in: harness) { $0.state.horizontalPodAutoscalers.map(\.name).contains("orbit-lens") }

        try await assertKindLoads(.networking, .service, in: harness) { $0.state.services.map(\.name).contains("orbit-lens") }
        try await assertKindLoads(.networking, .ingress, in: harness) { $0.state.ingresses.map(\.name).contains("orbit-lens") }
        try await assertKindLoads(.networking, .networkPolicy, in: harness) { $0.state.networkPolicies.map(\.name).contains("alpha-default-deny") }

        try await assertKindLoads(.storage, .persistentVolumeClaim, in: harness) { !$0.state.persistentVolumeClaims.isEmpty }
        try await assertKindLoads(.storage, .persistentVolume, in: harness) { !$0.state.persistentVolumes.isEmpty }
        try await assertKindLoads(.storage, .storageClass, in: harness) { !$0.state.storageClasses.isEmpty }
        try await assertKindLoads(.storage, .node, in: harness) { !$0.state.nodes.isEmpty }

        try await assertKindLoads(.config, .configMap, in: harness) { $0.state.configMaps.map(\.name).contains("orbit-grid") }
        try await assertKindLoads(.config, .secret, in: harness) { $0.state.secrets.map(\.name).contains("orbit-seal") }

        try await assertKindLoads(.rbac, .role, in: harness) { $0.state.rbacRoles.map(\.name).contains("alpha-reader") }
        try await assertKindLoads(.rbac, .roleBinding, in: harness) { $0.state.rbacRoleBindings.map(\.name).contains("alpha-reader-binding") }
        try await assertKindLoads(.rbac, .clusterRole, in: harness) { !$0.state.rbacClusterRoles.isEmpty }
        try await assertKindLoads(.rbac, .clusterRoleBinding, in: harness) { !$0.state.rbacClusterRoleBindings.isEmpty }

        XCTAssertFalse(harness.viewModel.visibleContexts.map(\.name).contains("rune-demo"))
        XCTAssertNil(harness.state.lastError)
    }

    func testDockerComposeTerminalRightPanelLogWorkflowDoesNotFollowShellPodFallback() async throws {
        let harness = try makeHarness()
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = false
        defer { restoreDemoSetting(previousDemoSetting) }

        try await harness.viewModel.reloadContexts()
        try await selectOrbitContext(in: harness)
        harness.viewModel.setSection(.terminal)

        try await waitUntil(timeout: 30) {
            harness.state.selectedSection == .terminal
                && harness.state.pods.contains { $0.name.hasPrefix("orbit-lens-") }
                && harness.state.pods.contains { $0.name.hasPrefix("ember-gate-") }
                && !harness.state.isLoading
        }

        let logPod = try XCTUnwrap(harness.state.pods.first { $0.name.hasPrefix("orbit-lens-") })
        let shellPod = try XCTUnwrap(harness.state.pods.first { $0.name.hasPrefix("ember-gate-") })
        var logTabs = TerminalPodLogTabState()
        logTabs.ensureTab(for: logPod)

        harness.viewModel.focusTerminalPodInspector(logPod, reloadLogs: true, loadDetails: false)

        try await waitUntil(timeout: 30) {
            harness.state.selectedSection == .terminal
                && harness.state.selectedPod?.id == logPod.id
                && !harness.state.isLoadingLogs
        }

        let requestCountAfterLogLoad = await harness.kubeClient.restRequestMetricsSummary().requestCount

        logTabs.reconcile(availablePods: harness.state.pods, fallbackPod: shellPod)

        XCTAssertEqual(logTabs.activePod(in: harness.state.pods, fallback: shellPod)?.id, logPod.id)
        XCTAssertEqual(logTabs.selectedPodID, logPod.id)
        XCTAssertEqual(harness.state.selectedPod?.id, logPod.id)
        let requestCountAfterShellFallback = await harness.kubeClient.restRequestMetricsSummary().requestCount
        XCTAssertEqual(requestCountAfterShellFallback, requestCountAfterLogLoad)

        logTabs.updateActive(to: shellPod)
        harness.viewModel.focusTerminalPodInspector(shellPod, reloadLogs: true, loadDetails: false)

        try await waitUntil(timeout: 30) {
            harness.state.selectedSection == .terminal
                && harness.state.selectedPod?.id == shellPod.id
                && !harness.state.isLoadingLogs
        }
        try await waitUntilRequestCountExceeds(requestCountAfterLogLoad, in: harness)

        let requestCountAfterExplicitLogSwitch = await harness.kubeClient.restRequestMetricsSummary().requestCount
        XCTAssertGreaterThan(requestCountAfterExplicitLogSwitch, requestCountAfterLogLoad)

        if harness.viewModel.isFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name) {
            harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name)
        }
        if harness.viewModel.isFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name) {
            harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name)
        }

        harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name)
        harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name)
        XCTAssertTrue(harness.viewModel.isFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name))
        XCTAssertTrue(harness.viewModel.isFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name))

        harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name)
        XCTAssertFalse(harness.viewModel.isFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name))
        XCTAssertTrue(harness.viewModel.isFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name))
        XCTAssertNil(harness.state.lastLogFetchError)
        XCTAssertNil(harness.state.lastError)
    }

    func testDockerComposeRollbackSafetyFlowAgainstFakeCluster() async throws {
        let harness = try makeHarness()
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        let previousRolloutDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        let previousHelmDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        let previousRollbackPlan = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
        let previousProductionConfirmation = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeEnableDemoCluster = false
        UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun = true
        UserDefaults.standard.runeWriteSafetyRequireHelmDryRun = true
        UserDefaults.standard.runeWriteSafetyShowRollbackPlan = true
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = true
        defer {
            restoreDemoSetting(previousDemoSetting)
            restoreUserDefault(previousRolloutDryRun, forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
            restoreUserDefault(previousHelmDryRun, forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
            restoreUserDefault(previousRollbackPlan, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
            restoreUserDefault(previousProductionConfirmation, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let client = KubernetesClient(commandTimeout: 10)
        let context = KubeContext(name: "fake-orbit-mesh")
        let namespace = "alpha-zone"
        let deploymentName = "rune-it-rollback-\(Self.shortTestID())"
        defer {
            Task {
                try? await client.deleteResource(
                    from: [KubeConfigSource(url: harness.kubeconfigURL)],
                    context: context,
                    namespace: namespace,
                    kind: .deployment,
                    name: deploymentName
                )
            }
        }

        try await client.applyYAML(
            from: [KubeConfigSource(url: harness.kubeconfigURL)],
            context: context,
            namespace: namespace,
            yaml: Self.rollbackDeploymentYAML(
                name: deploymentName,
                namespace: namespace,
                marker: "rollback-baseline"
            )
        )
        try await waitForDeploymentReady(
            client: client,
            sources: [KubeConfigSource(url: harness.kubeconfigURL)],
            context: context,
            namespace: namespace,
            deploymentName: deploymentName
        )
        try await client.applyYAML(
            from: [KubeConfigSource(url: harness.kubeconfigURL)],
            context: context,
            namespace: namespace,
            yaml: Self.rollbackDeploymentYAML(
                name: deploymentName,
                namespace: namespace,
                marker: "rollback-edited"
            )
        )
        try await waitForDeploymentReady(
            client: client,
            sources: [KubeConfigSource(url: harness.kubeconfigURL)],
            context: context,
            namespace: namespace,
            deploymentName: deploymentName
        )
        try await waitForRolloutHistory(
            client: client,
            sources: [KubeConfigSource(url: harness.kubeconfigURL)],
            context: context,
            namespace: namespace,
            deploymentName: deploymentName,
            revisions: [1, 2]
        )

        try await harness.viewModel.reloadContexts()
        try await selectOrbitContext(in: harness)
        harness.viewModel.setSection(.workloads)
        harness.viewModel.setWorkloadKind(.deployment)
        try await waitUntil(timeout: 30) {
            harness.state.deployments.contains { $0.name == deploymentName }
                && !harness.state.isLoading
        }

        let deployment = try XCTUnwrap(harness.state.deployments.first { $0.name == deploymentName })
        harness.viewModel.selectDeployment(deployment)
        try await waitUntil(timeout: 30) {
            harness.state.selectedDeployment?.name == deploymentName
                && harness.state.deploymentRolloutHistory.contains("1")
                && harness.state.deploymentRolloutHistory.contains("2")
                && !harness.state.isLoadingResourceDetails
        }

        if !harness.viewModel.isProductionContext(context) {
            harness.viewModel.toggleProductionMark(for: context)
        }
        harness.viewModel.rolloutRevisionInput = "1"
        harness.viewModel.requestRolloutUndoSelectedDeployment()

        try await waitUntil(timeout: 30) {
            harness.viewModel.pendingWriteDryRunStatus?.contains("Server accepted rollback dry-run") == true
        }
        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Review Production Action")
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Rollback plan:"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Target resource: deployment/\(deploymentName)"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Target revision: 1"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Server accepted rollback dry-run"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionKubectlCommand.contains("rollout undo deployment \(deploymentName) --to-revision=1"))

        harness.viewModel.confirmPendingWriteAction()

        XCTAssertEqual(
            harness.viewModel.pendingProductionDestructiveConfirmation,
            .rolloutUndo(deploymentName: deploymentName, revision: 1)
        )
        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Rollback")
        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil(timeout: 60) {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Rollout Undo"
                    && entry.resource.contains("deployment/\(deploymentName)")
                    && entry.status == "Succeeded"
                    && entry.message.contains("server dry-run")
            }
        }
        try await waitForDeploymentYAML(
            client: client,
            sources: [KubeConfigSource(url: harness.kubeconfigURL)],
            context: context,
            namespace: namespace,
            deploymentName: deploymentName,
            contains: "rollback-baseline"
        )

        harness.viewModel.setSection(.helm)
        try await waitUntil(timeout: 30) {
            harness.state.selectedSection == .helm
                && harness.state.helmReleases.contains { $0.name == "orbit-lens" }
                && !harness.state.isLoading
        }
        let release = try XCTUnwrap(harness.state.helmReleases.first { $0.name == "orbit-lens" })
        harness.viewModel.selectHelmRelease(release)
        try await waitUntil(timeout: 30) {
            harness.state.selectedHelmRelease?.name == "orbit-lens"
                && !harness.state.helmHistory.isEmpty
        }

        let targetRevision = try XCTUnwrap(harness.state.helmHistory.map(\.revision).min())
        harness.viewModel.requestHelmRollback(revision: targetRevision)

        try await waitUntil(timeout: 10) {
            harness.viewModel.pendingWriteDryRunStatus?.contains("Helm accepted rollback dry-run") == true
        }

        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Review Production Action")
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Rune will run Helm rollback after confirmation"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Rollback plan:"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Target release: \(release.namespace)/\(release.name)"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Helm accepted rollback dry-run"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionKubectlCommand.contains("helm --kube-context fake-orbit-mesh --namespace \(release.namespace) rollback \(release.name) \(targetRevision)"))
        XCTAssertEqual(harness.helmRunner.requests.map(\.dryRun), [true])

        harness.viewModel.confirmPendingWriteAction()
        XCTAssertEqual(
            harness.viewModel.pendingProductionDestructiveConfirmation,
            .helmRollback(
                releaseName: release.name,
                namespace: release.namespace,
                revision: targetRevision,
                wait: true,
                timeout: "5m",
                cleanupOnFail: true
            )
        )
        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil(timeout: 10) {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Helm Rollback"
                    && entry.resource.contains("helmrelease/\(release.namespace)/\(release.name)")
                    && entry.status == "Succeeded"
                    && entry.message.contains("Helm rollback completed after Helm dry-run")
            }
        }
        XCTAssertEqual(harness.helmRunner.requests.map(\.dryRun), [true, true, false])

        XCTAssertNil(harness.state.lastError)
    }

    func testDockerComposeOverviewSignalsAreDistinctClickableAndBounded() async throws {
        let harness = try makeHarness()
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = false
        defer { restoreDemoSetting(previousDemoSetting) }

        try await harness.viewModel.reloadContexts()
        try await selectOrbitContext(in: harness)
        harness.viewModel.setSection(.overview)

        try await waitUntil(timeout: 30) {
            harness.state.selectedSection == .overview
                && !harness.state.overviewPods.isEmpty
                && !harness.state.overviewEvents.isEmpty
                && !harness.state.isLoading
        }

        let unhealthy = harness.viewModel.overviewUnhealthyItems
        let incidents = harness.viewModel.overviewIncidentTimelineItems
        let dependencies = harness.viewModel.overviewDependencyItems

        XCTAssertLessThanOrEqual(unhealthy.count, 8)
        XCTAssertLessThanOrEqual(incidents.count, 8)
        XCTAssertLessThanOrEqual(dependencies.count, 8)
        XCTAssertFalse(unhealthy.contains { $0.badge == "Event" })
        XCTAssertTrue(incidents.allSatisfy { $0.target?.kind == .event })

        if let signal = unhealthy.first(where: { $0.target?.kind == .pod }) {
            harness.viewModel.openOverviewSignal(signal)
            try await waitUntil(timeout: 20) {
                harness.state.selectedSection == .workloads
                    && harness.state.selectedWorkloadKind == .pod
                    && harness.state.selectedPod?.name == signal.target?.name
            }
        }

        if let dependency = dependencies.first(where: { $0.primaryTarget?.kind == .deployment }) {
            harness.viewModel.setSection(.overview)
            harness.viewModel.openOverviewDependency(dependency)
            try await waitUntil(timeout: 20) {
                harness.state.selectedSection == .workloads
                    && harness.state.selectedWorkloadKind == .deployment
                    && harness.state.selectedDeployment?.name == dependency.primaryTarget?.name
            }
        }

        if let incident = incidents.first {
            let objectName = incident.title.components(separatedBy: " • ").last ?? ""
            harness.viewModel.setSection(.overview)
            harness.viewModel.openOverviewSignal(incident)
            try await waitUntil(timeout: 20) {
                harness.state.selectedSection == .events
                    || harness.state.selectedPod?.name == objectName
            }
        }

        XCTAssertNil(harness.state.lastError)
    }

    func testDockerComposeAuthDoctorAndSupportBundleAreNonDestructive() async throws {
        let exporter = RecordingFileExporter()
        let harness = try makeHarness(exporter: exporter)
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = false
        defer { restoreDemoSetting(previousDemoSetting) }

        try await harness.viewModel.reloadContexts()
        try await selectOrbitContext(in: harness)
        harness.viewModel.setSection(.workloads)
        harness.viewModel.setWorkloadKind(.pod)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .workloads
                && harness.state.selectedWorkloadKind == .pod
                && !harness.state.pods.isEmpty
                && !harness.state.isLoading
                && !harness.state.isLoadingResourceDetails
        }

        let sources = [KubeConfigSource(url: harness.kubeconfigURL)]
        let context = KubeContext(name: "fake-orbit-mesh")
        let namespace = "alpha-zone"
        let clusterBefore = try await clusterSnapshot(
            client: harness.kubeClient,
            sources: sources,
            context: context,
            namespace: namespace
        )
        let stateBeforeAuthDoctor = ViewModelResourceStateSnapshot(state: harness.state)

        harness.viewModel.runAuthDoctor()

        try await waitUntil(timeout: 30) {
            !harness.state.isRunningAuthDoctor
                && harness.state.authDoctorChecks.contains { $0.id == "pod-list" }
                && harness.state.authDoctorChecks.contains { $0.id == "pod-logs" }
        }

        let clusterAfterAuthDoctor = try await clusterSnapshot(
            client: harness.kubeClient,
            sources: sources,
            context: context,
            namespace: namespace
        )
        XCTAssertEqual(clusterAfterAuthDoctor, clusterBefore)
        XCTAssertEqual(ViewModelResourceStateSnapshot(state: harness.state), stateBeforeAuthDoctor)
        XCTAssertEqual(harness.state.writeAuditLog.count, 0)
        XCTAssertFalse(harness.state.authDoctorChecks.contains { $0.id == "helm-rollback-dry-run" })

        let stateBeforeSupportBundle = ViewModelResourceStateSnapshot(state: harness.state)
        let requestCountBeforeSupportBundle = await harness.kubeClient.restRequestMetricsSummary().requestCount

        harness.viewModel.saveSupportBundle()

        try await waitUntil(timeout: 10) {
            exporter.saves.count == 1
        }

        let requestCountAfterSupportBundle = await harness.kubeClient.restRequestMetricsSummary().requestCount
        XCTAssertEqual(requestCountAfterSupportBundle, requestCountBeforeSupportBundle)
        XCTAssertEqual(ViewModelResourceStateSnapshot(state: harness.state), stateBeforeSupportBundle)
        XCTAssertEqual(harness.state.writeAuditLog.count, 0)

        let save = try XCTUnwrap(exporter.saves.first)
        XCTAssertEqual(save.allowedFileTypes, ["json"])
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: save.data)
        XCTAssertEqual(decoded.contextName, "<context-name>")
        XCTAssertEqual(decoded.namespace, namespace)
        XCTAssertEqual(decoded.resourceCounts["pods"], stateBeforeSupportBundle.podNames.count)
        XCTAssertFalse(decoded.requestMetrics.isEmpty)
        XCTAssertNil(harness.state.lastError)
    }

    func testDockerComposeProductionDeleteRequiresSecondConfirmationBeforeMutatingCluster() async throws {
        let harness = try makeHarness()
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        let previousProductionConfirmation = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        let context = KubeContext(name: "fake-orbit-mesh")
        var markedProductionInTest = false
        UserDefaults.standard.runeEnableDemoCluster = false
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = true
        defer {
            if markedProductionInTest {
                harness.viewModel.toggleProductionMark(for: context)
            }
            restoreDemoSetting(previousDemoSetting)
            restoreUserDefault(previousProductionConfirmation, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let sources = [KubeConfigSource(url: harness.kubeconfigURL)]
        let namespace = "alpha-zone"
        let configName = "rune-it-delete-guard-\(Self.shortTestID())"
        defer {
            Task {
                try? await harness.kubeClient.deleteResource(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    kind: .configMap,
                    name: configName
                )
            }
        }

        try await harness.kubeClient.applyYAML(
            from: sources,
            context: context,
            namespace: namespace,
            yaml: Self.configMapYAML(name: configName, namespace: namespace, value: "delete-guard")
        )
        try await waitForConfigMap(
            client: harness.kubeClient,
            sources: sources,
            context: context,
            namespace: namespace,
            name: configName
        )

        try await harness.viewModel.reloadContexts()
        try await selectOrbitContext(in: harness)
        harness.viewModel.setSection(.config)
        harness.viewModel.setWorkloadKind(.configMap)
        try await waitUntil(timeout: 30) {
            harness.state.configMaps.contains { $0.name == configName }
                && !harness.state.isLoading
                && !harness.state.isLoadingResourceDetails
        }

        let configMap = try XCTUnwrap(harness.state.configMaps.first { $0.name == configName })
        harness.viewModel.selectConfigMap(configMap)
        try await waitUntil(timeout: 30) {
            harness.state.selectedConfigMap?.name == configName
                && harness.state.resourceYAML.contains(configName)
                && !harness.state.isLoadingResourceDetails
        }

        if !harness.viewModel.isProductionContext(context) {
            harness.viewModel.toggleProductionMark(for: context)
            markedProductionInTest = true
        }
        XCTAssertTrue(harness.viewModel.isProductionContext(context))

        harness.viewModel.requestDeleteSelectedResource()
        XCTAssertEqual(
            harness.viewModel.pendingWriteAction,
            .delete(kind: .configMap, name: configName)
        )
        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Review Production Action")
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Destructive production actions require a second confirmation"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionKubectlCommand.contains("delete configmap \(configName)"))

        let requestCountBeforeFirstConfirm = await harness.kubeClient.restRequestMetricsSummary().requestCount
        harness.viewModel.confirmPendingWriteAction()
        XCTAssertEqual(
            harness.viewModel.pendingProductionDestructiveConfirmation,
            .delete(kind: .configMap, name: configName)
        )
        XCTAssertEqual(
            harness.viewModel.pendingWriteAction,
            .delete(kind: .configMap, name: configName)
        )
        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Delete")
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Final confirmation required"))

        let requestCountAfterFirstConfirm = await harness.kubeClient.restRequestMetricsSummary().requestCount
        XCTAssertEqual(requestCountAfterFirstConfirm, requestCountBeforeFirstConfirm)
        try await assertConfigMapExists(
            client: harness.kubeClient,
            sources: sources,
            context: context,
            namespace: namespace,
            name: configName
        )
        XCTAssertFalse(harness.state.writeAuditLog.contains { $0.resource.contains(configName) })

        harness.viewModel.confirmPendingWriteAction()
        try await waitUntil(timeout: 30) {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Delete"
                    && entry.resource.contains("configmap/\(configName)")
                    && entry.status == "Succeeded"
            }
        }
        try await waitForConfigMapDeletion(
            client: harness.kubeClient,
            sources: sources,
            context: context,
            namespace: namespace,
            name: configName
        )

        XCTAssertNil(harness.viewModel.pendingWriteAction)
        XCTAssertNil(harness.viewModel.pendingProductionDestructiveConfirmation)
        XCTAssertNil(harness.state.lastError)
    }

    private struct Harness {
        let kubeconfigURL: URL
        let kubeClient: KubernetesClient
        let state: RuneAppState
        let viewModel: RuneAppViewModel
        let helmRunner: DockerComposeRecordingHelmCommandRunner
    }

    private func makeHarness(exporter: FileExporting = NoopFileExporter()) throws -> Harness {
        guard ProcessInfo.processInfo.environment["RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS"] == "1" else {
            throw XCTSkip("Set RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 to run Docker Compose fake-cluster UI integration tests.")
        }

        let kubeconfig = repoRoot.appendingPathComponent("docker-compose/generated/rune-fake-kubeconfig.yaml")
        guard FileManager.default.fileExists(atPath: kubeconfig.path) else {
            throw XCTSkip("Docker Compose fake kubeconfig is missing. Run scripts/run-local-k8s-integration-report.sh with RUNE_SKIP_DOCKER_FAKE_K8S=0.")
        }

        let contents = try String(contentsOf: kubeconfig, encoding: .utf8)
        guard contents.contains("name: fake-orbit-mesh"),
              contents.contains("name: fake-lattice-spark"),
              contents.contains("server: https://127.0.0.1:16443"),
              contents.contains("server: https://127.0.0.1:17443") else {
            throw XCTSkip("Docker Compose kubeconfig failed local-only fake-cluster safety checks.")
        }

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfig)])
        let kubeClient = KubernetesClient(commandTimeout: 10)
        let helmRunner = DockerComposeRecordingHelmCommandRunner()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: kubeClient,
            exporter: exporter,
            helmCommandRunner: helmRunner,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        return Harness(
            kubeconfigURL: kubeconfig,
            kubeClient: kubeClient,
            state: state,
            viewModel: viewModel,
            helmRunner: helmRunner
        )
    }

    private func selectOrbitContext(in harness: Harness) async throws {
        try await waitUntil(timeout: 20) {
            harness.state.contexts.map(\.name).contains("fake-orbit-mesh")
        }

        if harness.state.selectedContext?.name != "fake-orbit-mesh" {
            harness.viewModel.setContext(KubeContext(name: "fake-orbit-mesh"))
        }

        try await waitUntil(timeout: 20) {
            harness.state.selectedContext?.name == "fake-orbit-mesh"
                && harness.state.selectedNamespace == "alpha-zone"
                && !harness.state.overviewPods.isEmpty
                && !harness.state.isLoading
        }
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }

    private func assertKindLoads(
        _ section: RuneSection,
        _ kind: KubeResourceKind,
        in harness: Harness,
        file: StaticString = #filePath,
        line: UInt = #line,
        predicate: @escaping @MainActor (Harness) -> Bool
    ) async throws {
        harness.viewModel.setSection(section)
        harness.viewModel.setWorkloadKind(kind)
        try await waitUntil(timeout: 20, file: file, line: line) {
            harness.state.selectedSection == section
                && harness.state.selectedWorkloadKind == kind
                && predicate(harness)
                && !harness.state.isLoading
        }
    }

    private func restoreDemoSetting(_ value: Any?) {
        restoreUserDefault(value, forKey: RuneSettingsKeys.enableDemoCluster)
    }

    private func restoreUserDefault(_ value: Any?, forKey key: String) {
        if let value {
            UserDefaults.standard.set(value, forKey: key)
        } else {
            UserDefaults.standard.removeObject(forKey: key)
        }
    }

    private func waitUntil(
        timeout: TimeInterval = 2,
        file: StaticString = #filePath,
        line: UInt = #line,
        predicate: @escaping @MainActor () -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !predicate() {
            if Date() >= deadline {
                XCTFail("Timed out waiting for condition", file: file, line: line)
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    private func waitUntilRequestCountExceeds(
        _ requestCount: Int,
        in harness: Harness,
        timeout: TimeInterval = 10,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while await harness.kubeClient.restRequestMetricsSummary().requestCount <= requestCount {
            if Date() >= deadline {
                XCTFail("Timed out waiting for REST request count to increase", file: file, line: line)
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    private func waitForDeploymentReady(
        client: KubernetesClient,
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String,
        timeout: TimeInterval = 120
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        var lastMessage = ""
        while Date() < deadline {
            do {
                let result = try await client.verifyDeploymentRollout(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    deploymentName: deploymentName,
                    timeout: min(15, max(1, deadline.timeIntervalSinceNow))
                )
                lastMessage = result.message
                if result.status == .ready {
                    return
                }
            } catch {
                lastMessage = String(describing: error)
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        }
        throw RuneError.commandFailed(
            command: "wait for deployment rollout",
            message: "Timed out waiting for Deployment \(deploymentName) readiness. Last result: \(lastMessage)"
        )
    }

    private func waitForRolloutHistory(
        client: KubernetesClient,
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String,
        revisions: Set<Int>,
        timeout: TimeInterval = 120
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        var lastHistory = ""
        while Date() < deadline {
            do {
                lastHistory = try await client.deploymentRolloutHistory(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    deploymentName: deploymentName
                )
                let seen = Set(lastHistory.split(separator: "\n").compactMap { line -> Int? in
                    line.split(whereSeparator: \.isWhitespace).first.flatMap { Int($0) }
                })
                if revisions.isSubset(of: seen) {
                    return
                }
            } catch {
                lastHistory = String(describing: error)
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        }
        throw RuneError.commandFailed(
            command: "wait for rollout history",
            message: "Timed out waiting for rollout history \(revisions). Last history: \(lastHistory)"
        )
    }

    private func waitForDeploymentYAML(
        client: KubernetesClient,
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String,
        contains marker: String,
        timeout: TimeInterval = 60
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        var lastYAML = ""
        while Date() < deadline {
            do {
                lastYAML = try await client.resourceYAML(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    kind: .deployment,
                    name: deploymentName
                )
                if lastYAML.contains(marker) {
                    return
                }
            } catch {
                lastYAML = String(describing: error)
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        }
        throw RuneError.commandFailed(
            command: "wait for deployment YAML",
            message: "Timed out waiting for deployment YAML to contain \(marker). Last YAML: \(lastYAML)"
        )
    }

    private func waitForConfigMap(
        client: KubernetesClient,
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        name: String,
        timeout: TimeInterval = 30
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        var lastConfigMaps: [String] = []
        while Date() < deadline {
            let configMaps = try await client.listConfigMaps(from: sources, context: context, namespace: namespace)
            lastConfigMaps = configMaps.map(\.name)
            if lastConfigMaps.contains(name) {
                return
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        }
        throw RuneError.commandFailed(
            command: "wait for configmap",
            message: "Timed out waiting for ConfigMap \(name). Last ConfigMaps: \(lastConfigMaps.joined(separator: ", "))"
        )
    }

    private func assertConfigMapExists(
        client: KubernetesClient,
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        name: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let configMaps = try await client.listConfigMaps(from: sources, context: context, namespace: namespace)
        XCTAssertTrue(configMaps.contains { $0.name == name }, "Expected ConfigMap \(name) to still exist", file: file, line: line)
    }

    private func waitForConfigMapDeletion(
        client: KubernetesClient,
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        name: String,
        timeout: TimeInterval = 30
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        var lastConfigMaps: [String] = []
        while Date() < deadline {
            let configMaps = try await client.listConfigMaps(from: sources, context: context, namespace: namespace)
            lastConfigMaps = configMaps.map(\.name)
            if !lastConfigMaps.contains(name) {
                return
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        }
        throw RuneError.commandFailed(
            command: "wait for configmap deletion",
            message: "Timed out waiting for ConfigMap \(name) deletion. Last ConfigMaps: \(lastConfigMaps.joined(separator: ", "))"
        )
    }

    private func clusterSnapshot(
        client: KubernetesClient,
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> DockerComposeClusterSnapshot {
        async let pods = client.listPods(from: sources, context: context, namespace: namespace)
        async let deployments = client.listDeployments(from: sources, context: context, namespace: namespace)
        async let services = client.listServices(from: sources, context: context, namespace: namespace)
        async let configMaps = client.listConfigMaps(from: sources, context: context, namespace: namespace)
        async let events = client.listEvents(from: sources, context: context, namespace: namespace)

        return try await DockerComposeClusterSnapshot(
            podNames: pods.map(\.name).sorted(),
            deploymentNames: deployments.map(\.name).sorted(),
            serviceNames: services.map(\.name).sorted(),
            configMapNames: configMaps.map(\.name).sorted(),
            eventObjectNames: events.map(\.objectName).sorted()
        )
    }

    private static func shortTestID() -> String {
        String(UUID().uuidString.prefix(8)).lowercased()
    }

    private static func configMapYAML(name: String, namespace: String, value: String) -> String {
        """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: \(name)
          namespace: \(namespace)
          labels:
            app.kubernetes.io/managed-by: rune-integration-test
        data:
          delete-guard: \(value)
        """
    }

    private static func rollbackDeploymentYAML(name: String, namespace: String, marker: String) -> String {
        """
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: \(name)
          namespace: \(namespace)
          labels:
            app: \(name)
            app.kubernetes.io/managed-by: rune-integration-test
          annotations:
            kubernetes.io/change-cause: \(marker)
        spec:
          replicas: 1
          selector:
            matchLabels:
              app: \(name)
          template:
            metadata:
              labels:
                app: \(name)
              annotations:
                rune.test/rollback-marker: \(marker)
            spec:
              containers:
                - name: app
                  image: nginx:1.27-alpine
                  command: ["/bin/sh", "-ec"]
                  args:
                    - |
                      echo \(marker) >/usr/share/nginx/html/index.html
                      nginx -g 'daemon off;'
        """
    }
}

private final class DockerComposeRecordingHelmCommandRunner: HelmCommandRunning, @unchecked Sendable {
    private(set) var requests: [HelmRollbackRequest] = []

    func rollback(_ request: HelmRollbackRequest, timeout: TimeInterval) async throws -> HelmCommandResult {
        requests.append(request)
        return HelmCommandResult(exitCode: 0, stdout: "ok\n", stderr: "")
    }
}

private struct DockerComposeClusterSnapshot: Equatable {
    let podNames: [String]
    let deploymentNames: [String]
    let serviceNames: [String]
    let configMapNames: [String]
    let eventObjectNames: [String]
}

private struct ViewModelResourceStateSnapshot: Equatable {
    let selectedContextName: String?
    let selectedNamespace: String
    let selectedSection: RuneSection
    let selectedWorkloadKind: KubeResourceKind
    let podNames: [String]
    let deploymentNames: [String]
    let serviceNames: [String]
    let configMapNames: [String]
    let eventObjectNames: [String]

    @MainActor
    init(state: RuneAppState) {
        selectedContextName = state.selectedContext?.name
        selectedNamespace = state.selectedNamespace
        selectedSection = state.selectedSection
        selectedWorkloadKind = state.selectedWorkloadKind
        podNames = state.pods.map(\.name)
        deploymentNames = state.deployments.map(\.name)
        serviceNames = state.services.map(\.name)
        configMapNames = state.configMaps.map(\.name)
        eventObjectNames = state.events.map(\.objectName)
    }
}

private final class NoopFileExporter: FileExporting {
    @MainActor
    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        FileManager.default.temporaryDirectory.appendingPathComponent(suggestedName)
    }
}

private final class RecordingFileExporter: FileExporting {
    struct Save {
        let data: Data
        let suggestedName: String
        let allowedFileTypes: [String]
    }

    private(set) var saves: [Save] = []

    @MainActor
    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        saves.append(Save(data: data, suggestedName: suggestedName, allowedFileTypes: allowedFileTypes))
        return FileManager.default.temporaryDirectory.appendingPathComponent(suggestedName)
    }
}
