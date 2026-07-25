import XCTest
import RuneCore
import RuneStore
@testable import RuneUI

final class RuneViewModelSelectionRaceTests: XCTestCase {
    @MainActor
    func testUserSelectionCancelsDelayedNavigationRestore() async throws {
        let state = podSelectionState()
        let viewModel = makeViewModel(state: state)
        let pods = state.pods

        viewModel.selectPod(pods[1])
        viewModel.navigateBack()
        XCTAssertEqual(state.selectedPod?.name, pods[0].name)

        viewModel.selectPod(pods[2])
        try await Task.sleep(nanoseconds: 350_000_000)

        XCTAssertEqual(state.selectedPod?.name, pods[2].name)
    }

    @MainActor
    func testRapidBackThenForwardKeepsLatestNavigationCheckpoint() async throws {
        let state = podSelectionState()
        let viewModel = makeViewModel(state: state)
        let pods = state.pods

        viewModel.selectPod(pods[1])
        viewModel.navigateBack()
        viewModel.navigateForward()
        try await Task.sleep(nanoseconds: 350_000_000)

        XCTAssertEqual(state.selectedPod?.name, pods[1].name)
    }

    @MainActor
    func testUserSelectionCancelsDelayedSavedWorkspaceRestore() async throws {
        let state = podSelectionState()
        let viewModel = makeViewModel(state: state)
        let pods = state.pods
        let workspace = SavedWorkspaceSnapshot(
            name: "Synthetic workload",
            contextName: nil,
            namespace: "scope-a",
            section: .workloads,
            workloadKind: .pod,
            resourceKind: "pod",
            resourceName: pods[0].name,
            resourceNamespace: pods[0].namespace
        )

        viewModel.openSavedWorkspace(workspace)
        XCTAssertEqual(state.selectedPod?.name, pods[0].name)

        viewModel.selectPod(pods[1])
        try await Task.sleep(nanoseconds: 350_000_000)

        XCTAssertEqual(state.selectedPod?.name, pods[1].name)
    }

    @MainActor
    func testHelmLatestScopeWinsRapidAllNamespaceChange() async throws {
        let state = RuneAppState()
        let context = KubeContext(name: "synthetic-context-a")
        state.selectedContext = context
        state.selectedNamespace = "scope-a"
        state.selectedSection = .helm
        state.isHelmAllNamespaces = false
        let recorder = HelmListInvocationRecorder()
        let staleRelease = helmRelease(name: "stale-release", namespace: "scope-a")
        let currentRelease = helmRelease(name: "current-release", namespace: "scope-a")
        let viewModel = makeViewModel(
            state: state,
            helmReleaseList: { _, _, _, allNamespaces in
                recorder.record(allNamespaces: allNamespaces)
                if allNamespaces {
                    try await Task.sleep(nanoseconds: 220_000_000)
                    return [staleRelease]
                }
                try await Task.sleep(nanoseconds: 20_000_000)
                return [currentRelease]
            }
        )

        viewModel.setHelmAllNamespaces(true)
        try await waitUntil { recorder.callCount == 1 }
        viewModel.setHelmAllNamespaces(false)
        try await waitUntil { recorder.callCount == 2 && state.helmReleases == [currentRelease] }
        try await Task.sleep(nanoseconds: 260_000_000)

        XCTAssertEqual(state.helmReleases, [currentRelease])
        XCTAssertEqual(recorder.allNamespacesArguments, [true, false])
    }

    @MainActor
    func testHelmResultIsDiscardedAfterContextAndNamespaceChange() async throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .helm
        state.isHelmAllNamespaces = false
        let baselineRelease = helmRelease(name: "baseline-release", namespace: "scope-a")
        let staleRelease = helmRelease(name: "stale-release", namespace: "scope-a")
        state.setHelmReleases([baselineRelease])
        let recorder = HelmListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            helmReleaseList: { _, _, _, _ in
                recorder.record(allNamespaces: true)
                try await Task.sleep(nanoseconds: 180_000_000)
                return [staleRelease]
            }
        )

        viewModel.setHelmAllNamespaces(true)
        try await waitUntil { recorder.callCount == 1 }
        state.selectedContext = KubeContext(name: "synthetic-context-b")
        state.selectedNamespace = "scope-b"
        try await Task.sleep(nanoseconds: 240_000_000)

        XCTAssertEqual(state.helmReleases, [baselineRelease])
    }

    @MainActor
    func testSimpleModeOperatorFamilyInvalidatesPendingHelmReleaseList() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .helm
        state.isHelmAllNamespaces = false
        let baselineRelease = helmRelease(name: "baseline-release", namespace: "scope-a")
        let staleRelease = helmRelease(name: "stale-release", namespace: "scope-a")
        state.setHelmReleases([baselineRelease])
        let recorder = HelmListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            helmReleaseList: { _, _, _, allNamespaces in
                recorder.record(allNamespaces: allNamespaces)
                try await Task.sleep(nanoseconds: 180_000_000)
                return [staleRelease]
            }
        )

        viewModel.setHelmAllNamespaces(false)
        try await waitUntil { recorder.callCount == 1 }
        viewModel.setHelmBrowserResourceFamily(.operatorResources)
        try await Task.sleep(nanoseconds: 240_000_000)

        XCTAssertEqual(viewModel.helmBrowserResourceFamily, .operatorResources)
        XCTAssertEqual(state.helmReleases, [baselineRelease])
        XCTAssertEqual(state.selectedHelmRelease, baselineRelease)
        XCTAssertFalse(state.isLoading)

        viewModel.setHelmAllNamespaces(true)
        try await Task.sleep(nanoseconds: 50_000_000)

        XCTAssertEqual(recorder.callCount, 1)
        XCTAssertEqual(state.helmReleases, [baselineRelease])
    }

    @MainActor
    func testRBACCacheDoesNotCrossNamespaceOrContextScope() {
        let contextA = KubeContext(name: "synthetic-context-a")
        let contextB = KubeContext(name: "synthetic-context-b")
        let store = ResourceStore()
        cacheServiceAccounts(
            [rbacResource(kind: .serviceAccount, name: "service-scope-b", namespace: "scope-b")],
            context: contextA,
            namespace: "scope-b",
            store: store
        )
        cacheServiceAccounts(
            [rbacResource(kind: .serviceAccount, name: "service-context-b", namespace: "scope-b")],
            context: contextB,
            namespace: "scope-b",
            store: store
        )
        store.cacheNamespaces(["scope-b"], context: contextB)

        let state = RuneAppState()
        state.selectedContext = contextA
        state.selectedNamespace = "scope-a"
        state.selectedSection = .rbac
        state.selectedWorkloadKind = .role
        state.setRBACData(
            roles: [rbacResource(kind: .role, name: "role-scope-a", namespace: "scope-a")],
            serviceAccounts: [rbacResource(kind: .serviceAccount, name: "service-scope-a", namespace: "scope-a")],
            roleBindings: [rbacResource(kind: .roleBinding, name: "binding-scope-a", namespace: "scope-a")],
            clusterRoles: [rbacResource(kind: .clusterRole, name: "cluster-role-a", namespace: nil)],
            clusterRoleBindings: [rbacResource(kind: .clusterRoleBinding, name: "cluster-binding-a", namespace: nil)]
        )
        let viewModel = makeViewModel(state: state, store: store)

        viewModel.setNamespace("scope-b")

        XCTAssertTrue(state.rbacRoles.isEmpty)
        XCTAssertTrue(state.rbacRoleBindings.isEmpty)
        XCTAssertTrue(state.rbacClusterRoles.isEmpty)
        XCTAssertTrue(state.rbacClusterRoleBindings.isEmpty)
        XCTAssertEqual(state.serviceAccounts.map(\.name), ["service-scope-b"])

        state.setRBACData(
            roles: [rbacResource(kind: .role, name: "role-context-a", namespace: "scope-b")],
            serviceAccounts: state.serviceAccounts,
            roleBindings: [],
            clusterRoles: [rbacResource(kind: .clusterRole, name: "cluster-role-context-a", namespace: nil)],
            clusterRoleBindings: []
        )
        viewModel.setContext(contextB)

        XCTAssertTrue(state.rbacRoles.isEmpty)
        XCTAssertTrue(state.rbacClusterRoles.isEmpty)
        XCTAssertEqual(state.serviceAccounts.map(\.name), ["service-context-b"])
    }

    @MainActor
    func testOperatorSearchResetsAndClampsPagination() {
        let state = RuneAppState()
        state.setOperatorResources((0..<85).map { index in
            OperatorResourceSummary(
                family: "synthetic-family",
                kind: "SyntheticKind",
                apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
                name: index < 7 ? "match-\(index)" : "item-\(index)",
                namespace: "scope-a",
                status: "Ready",
                message: ""
            )
        })
        let viewModel = makeViewModel(state: state)

        viewModel.pageOperatorResourcesForward()
        viewModel.pageOperatorResourcesForward()
        XCTAssertEqual(viewModel.operatorResourcePage, 2)
        XCTAssertEqual(viewModel.operatorResourcePageSummary, "81-85 of 85")

        viewModel.setResourceSearchQuery("match-")

        XCTAssertEqual(viewModel.operatorResourcePage, 0)
        XCTAssertEqual(viewModel.pagedOperatorResources.count, 7)
        XCTAssertEqual(viewModel.operatorResourcePageSummary, "1-7 of 7")

        viewModel.setResourceSearchQuery("")
        viewModel.pageOperatorResourcesForward()
        state.resourceSearchQuery = "match-"

        XCTAssertEqual(viewModel.operatorResourcePage, 0)
        XCTAssertEqual(viewModel.operatorResourcePageSummary, "1-7 of 7")
    }

    @MainActor
    private func podSelectionState() -> RuneAppState {
        let state = RuneAppState()
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.setPods([
            PodSummary(name: "pod-a", namespace: "scope-a", status: "Running"),
            PodSummary(name: "pod-b", namespace: "scope-a", status: "Running"),
            PodSummary(name: "pod-c", namespace: "scope-a", status: "Running")
        ])
        return state
    }

    @MainActor
    private func makeViewModel(
        state: RuneAppState,
        store: ResourceStore = ResourceStore(),
        helmReleaseList: HelmReleaseListing? = nil
    ) -> RuneAppViewModel {
        RuneAppViewModel(
            state: state,
            store: store,
            contextPreferences: EmptyContextPreferences(),
            savedWorkspaceStore: InMemorySavedWorkspaceStore(),
            helmReleaseList: helmReleaseList
        )
    }

    private func helmRelease(name: String, namespace: String) -> HelmReleaseSummary {
        HelmReleaseSummary(
            name: name,
            namespace: namespace,
            revision: 1,
            updated: "2026-01-01T00:00:00Z",
            status: "deployed",
            chart: "synthetic-chart-1.0.0",
            appVersion: "1.0.0"
        )
    }

    private func rbacResource(
        kind: KubeResourceKind,
        name: String,
        namespace: String?
    ) -> ClusterResourceSummary {
        ClusterResourceSummary(
            kind: kind,
            name: name,
            namespace: namespace,
            primaryText: "Synthetic",
            secondaryText: ""
        )
    }

    @MainActor
    private func cacheServiceAccounts(
        _ serviceAccounts: [ClusterResourceSummary],
        context: KubeContext,
        namespace: String,
        store: ResourceStore
    ) {
        store.cacheSnapshot(
            context: context,
            namespace: namespace,
            pods: [],
            deployments: [],
            statefulSets: [],
            daemonSets: [],
            jobs: [],
            cronJobs: [],
            replicaSets: [],
            persistentVolumeClaims: [],
            horizontalPodAutoscalers: [],
            networkPolicies: [],
            services: [],
            endpoints: [],
            ingresses: [],
            configMaps: [],
            secrets: [],
            serviceAccounts: serviceAccounts,
            events: []
        )
    }

    @MainActor
    private func waitUntil(
        timeoutNanoseconds: UInt64 = 1_000_000_000,
        condition: @escaping @MainActor () -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(Double(timeoutNanoseconds) / 1_000_000_000)
        while !condition() {
            if Date() >= deadline {
                XCTFail("Timed out waiting for asynchronous selection state.")
                return
            }
            try await Task.sleep(nanoseconds: 5_000_000)
        }
    }
}

@MainActor
private final class HelmListInvocationRecorder {
    private(set) var allNamespacesArguments: [Bool] = []

    var callCount: Int {
        allNamespacesArguments.count
    }

    func record(allNamespaces: Bool) {
        allNamespacesArguments.append(allNamespaces)
    }
}

private final class EmptyContextPreferences: ContextPreferencesStoring {
    func loadFavoriteContextNames() -> Set<String> {
        []
    }

    func saveFavoriteContextNames(_ names: Set<String>) {}
}

private final class InMemorySavedWorkspaceStore: SavedWorkspaceStoring {
    private var workspaces: [SavedWorkspaceSnapshot] = []

    func loadSavedWorkspaces() -> [SavedWorkspaceSnapshot] {
        workspaces
    }

    func saveSavedWorkspaces(_ workspaces: [SavedWorkspaceSnapshot]) {
        self.workspaces = workspaces
    }
}
