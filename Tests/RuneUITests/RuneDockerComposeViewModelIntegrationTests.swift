import Foundation
import XCTest
@testable import RuneCore
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
        try await waitUntil(timeout: 20) {
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

        harness.viewModel.setSection(.helm)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .helm
                && harness.state.helmReleases.map(\.name).contains("orbit-lens")
                && !harness.state.isLoading
        }

        harness.viewModel.setSection(.terminal)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .terminal
                && harness.state.pods.contains { $0.id == pod.id }
                && !harness.state.isLoading
                && !harness.state.isLoadingResourceDetails
        }
        harness.viewModel.focusTerminalPodInspector(pod, reloadLogs: true, loadDetails: false)
        try await waitUntil(timeout: 20) {
            harness.state.selectedSection == .terminal
                && harness.state.selectedPod?.id == pod.id
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

    private struct Harness {
        let kubeconfigURL: URL
        let state: RuneAppState
        let viewModel: RuneAppViewModel
    }

    private func makeHarness() throws -> Harness {
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
        let viewModel = RuneAppViewModel(
            state: state,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        return Harness(kubeconfigURL: kubeconfig, state: state, viewModel: viewModel)
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
        if let value {
            UserDefaults.standard.set(value, forKey: RuneSettingsKeys.enableDemoCluster)
        } else {
            UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
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
}
