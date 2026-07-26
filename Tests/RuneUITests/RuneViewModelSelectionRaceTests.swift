import XCTest
import RuneCore
import RuneFakeK8sSupport
import RuneKube
import RuneSecurity
import RuneStore
@testable import RuneUI

final class RuneViewModelSelectionRaceTests: XCTestCase {
    @MainActor
    func testDemoRefreshDoesNotSnapRapidPodSelectionBackToFirstRow() async throws {
        let previousDemoSetting = UserDefaults.standard.object(
            forKey: RuneSettingsKeys.enableDemoCluster
        )
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(
                    previousDemoSetting,
                    forKey: RuneSettingsKeys.enableDemoCluster
                )
            } else {
                UserDefaults.standard.removeObject(
                    forKey: RuneSettingsKeys.enableDemoCluster
                )
            }
        }

        let state = RuneAppState()
        let viewModel = makeViewModel(state: state)
        viewModel.loadDemoCluster()
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        let pods = state.pods
        XCTAssertEqual(pods.count, 3)

        viewModel.selectPod(pods[0])
        viewModel.selectPod(pods[1])
        viewModel.selectPod(pods[2])

        viewModel.refreshCurrentView(debounced: false)
        XCTAssertEqual(state.selectedPod?.id, pods[2].id)

        try await Task.sleep(nanoseconds: 50_000_000)

        XCTAssertEqual(state.pods.map(\.id), pods.map(\.id))
        XCTAssertEqual(state.selectedPod?.id, pods[2].id)
    }

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
    func testContextReloadCannotRestoreOlderContextOrNamespaceAfterRapidNavigation() async throws {
        let contextA = KubeContext(name: "synthetic-context-a")
        let contextB = KubeContext(name: "synthetic-context-b")
        let state = RuneAppState()
        state.setContexts([contextA, contextB])
        state.selectedContext = contextA
        state.selectedNamespace = "scope-a"
        let store = ResourceStore()
        store.cacheNamespaces(["scope-b"], context: contextB)
        let gate = SelectionRaceAsyncGate()
        let recorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            store: store,
            kubeContextList: { _ in
                recorder.record()
                await gate.wait()
                return [contextA, contextB]
            }
        )

        let reloadTask = Task {
            try await viewModel.reloadContexts()
        }
        try await waitUntil { recorder.callCount == 1 }

        viewModel.setContext(contextB)
        viewModel.setNamespace("scope-b")
        gate.open()

        do {
            try await reloadTask.value
            XCTFail("A context reload superseded by navigation must be cancelled.")
        } catch is CancellationError {
            // Expected: the later context/namespace intent owns the selection.
        }

        XCTAssertEqual(state.selectedContext, contextB)
        XCTAssertEqual(state.selectedNamespace, "scope-b")
    }

    @MainActor
    func testSavedWorkspaceDelayedRetryDoesNotReplayInspectorOrLogPresentation() async throws {
        let state = podSelectionState()
        let viewModel = makeViewModel(state: state)
        let savedInspectorState = SavedWorkspaceInspectorState(
            podTabID: "logs",
            deploymentTabID: "rollout",
            isYAMLInlineEditing: true
        )
        let workspace = SavedWorkspaceSnapshot(
            name: "Synthetic presentation",
            contextName: nil,
            namespace: "scope-a",
            section: .workloads,
            workloadKind: .pod,
            resourceKind: "pod",
            resourceName: state.pods[0].name,
            resourceNamespace: state.pods[0].namespace,
            logPresetID: PodLogPreset.last15Minutes.rawValue,
            inspectorState: savedInspectorState
        )

        viewModel.openSavedWorkspace(workspace)
        let initialRestoreRequestID = try XCTUnwrap(
            viewModel.savedWorkspaceInspectorRestoreRequest?.id
        )
        XCTAssertEqual(viewModel.selectedLogPreset, .last15Minutes)

        viewModel.selectedLogPreset = .recentLines
        viewModel.updateSavedWorkspaceInspectorState(
            SavedWorkspaceInspectorState(
                podTabID: "overview",
                deploymentTabID: "overview",
                isYAMLInlineEditing: false
            )
        )
        try await Task.sleep(nanoseconds: 350_000_000)

        XCTAssertEqual(viewModel.selectedLogPreset, .recentLines)
        XCTAssertEqual(
            viewModel.savedWorkspaceInspectorRestoreRequest?.id,
            initialRestoreRequestID,
            "The delayed resource-ID retry must not publish a second presentation restore."
        )
    }

    @MainActor
    func testLateResourceInspectorRefreshPreservesNewerYAMLDraftState() async throws {
        let context = KubeContext(name: "synthetic-context-a")
        let deployment = DeploymentSummary(
            name: "synthetic-deployment",
            namespace: "scope-a",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        let scope = ResourceDetailScope(
            contextName: context.name,
            namespace: deployment.namespace,
            kind: .deployment,
            name: deployment.name
        )
        let initialYAML = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: initial\n"
        let refreshedYAML = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: refreshed\n"
        let firstDraft = "metadata: [\n# first local draft\n"
        let latestDraft = "metadata: [\n# latest local draft\n"
        let validationIssue = YAMLValidationIssue(
            source: .syntax,
            severity: .error,
            message: "Synthetic local validation issue"
        )

        let state = RuneAppState()
        state.selectedContext = context
        state.selectedNamespace = deployment.namespace
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .deployment
        state.setDeployments([deployment])
        state.setSelectedDeployment(deployment)
        state.beginResourceDetailLoad(scope: scope)
        state.setResourceYAML(initialYAML)
        state.setResourceDescribe("initial describe")
        state.finishResourceDetailLoad()

        let gate = SelectionRaceAsyncGate()
        let recorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            resourceManifestLoad: { _, _, _, _, _ in
                recorder.record()
                await gate.wait()
                return (refreshedYAML, "refreshed describe")
            }
        )

        viewModel.refreshResourceInspectorOnly()
        try await waitUntil { recorder.callCount == 1 }

        state.updateResourceYAMLDraft(firstDraft)
        state.updateResourceYAMLDraft(latestDraft)
        state.setResourceYAMLValidationIssues([validationIssue])
        let revisionAfterEdit = state.resourceYAMLDraftRevision
        let undoSnapshotAfterEdit = state.resourceYAMLUndoSnapshot

        gate.open()
        try await waitUntil { !state.isLoadingResourceDetails }

        XCTAssertEqual(state.resourceYAML, latestDraft)
        XCTAssertEqual(state.resourceYAMLBaseline, initialYAML)
        XCTAssertEqual(state.resourceYAMLUndoSnapshot, undoSnapshotAfterEdit)
        XCTAssertEqual(state.resourceYAMLUndoSnapshot, firstDraft)
        XCTAssertTrue(state.canUndoResourceYAMLEdit)
        XCTAssertEqual(state.resourceYAMLValidationIssues, [validationIssue])
        XCTAssertEqual(state.resourceYAMLDraftRevision, revisionAfterEdit)
        XCTAssertEqual(state.resourceDescribe, "refreshed describe")
        XCTAssertTrue(state.lastResourceYAMLError?.contains("kept the newer local YAML draft") == true)

        state.revertResourceYAMLToClusterSnapshot()
        XCTAssertFalse(state.resourceYAMLHasUnsavedEdits)
        viewModel.refreshResourceInspectorOnly()
        try await waitUntil {
            recorder.callCount == 2
                && !state.isLoadingResourceDetails
                && state.resourceYAML == refreshedYAML
        }

        XCTAssertEqual(state.resourceYAMLBaseline, refreshedYAML)
        XCTAssertFalse(state.canUndoResourceYAMLEdit)
        XCTAssertTrue(state.resourceYAMLValidationIssues.isEmpty)
        XCTAssertNil(state.lastResourceYAMLError)
    }

    @MainActor
    func testLateOperatorInspectorRefreshPreservesNewerYAMLDraftState() async throws {
        let context = KubeContext(name: "synthetic-context-a")
        let resource = OperatorResourceSummary(
            family: "synthetic-family",
            kind: "SyntheticResource",
            apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
            name: "synthetic-operator-resource",
            namespace: "scope-a",
            status: "Ready",
            message: ""
        )
        let scope = ResourceDetailScope(
            contextName: context.name,
            namespace: resource.namespace,
            kind: resource.kind,
            name: resource.name
        )
        let initialYAML = "apiVersion: example.test/v1\nkind: SyntheticResource\nmetadata:\n  name: initial\n"
        let refreshedYAML = "apiVersion: example.test/v1\nkind: SyntheticResource\nmetadata:\n  name: refreshed\n"
        let localDraft = "metadata: [\n# local operator draft\n"
        let validationIssue = YAMLValidationIssue(
            source: .syntax,
            severity: .error,
            message: "Synthetic operator validation issue"
        )

        let state = RuneAppState()
        state.selectedContext = context
        state.selectedNamespace = resource.namespace ?? "scope-a"
        state.selectedSection = .helm
        state.setOperatorResources([resource])
        state.setSelectedOperatorResource(resource)
        state.beginResourceDetailLoad(scope: scope)
        state.setResourceYAML(initialYAML)
        state.setResourceDescribe("initial operator describe")
        state.finishResourceDetailLoad()

        let gate = SelectionRaceAsyncGate()
        let recorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            operatorResourceManifestLoad: { _, _, _ in
                recorder.record()
                await gate.wait()
                return (refreshedYAML, "refreshed operator describe")
            }
        )

        viewModel.refreshSelectedHelmInspector()
        try await waitUntil { recorder.callCount == 1 }

        state.updateResourceYAMLDraft(localDraft)
        state.setResourceYAMLValidationIssues([validationIssue])
        let revisionAfterEdit = state.resourceYAMLDraftRevision
        let undoSnapshotAfterEdit = state.resourceYAMLUndoSnapshot

        gate.open()
        try await waitUntil { !state.isLoadingResourceDetails }

        XCTAssertEqual(state.resourceYAML, localDraft)
        XCTAssertEqual(state.resourceYAMLBaseline, initialYAML)
        XCTAssertEqual(state.resourceYAMLUndoSnapshot, undoSnapshotAfterEdit)
        XCTAssertEqual(state.resourceYAMLUndoSnapshot, initialYAML)
        XCTAssertTrue(state.canUndoResourceYAMLEdit)
        XCTAssertEqual(state.resourceYAMLValidationIssues, [validationIssue])
        XCTAssertEqual(state.resourceYAMLDraftRevision, revisionAfterEdit)
        XCTAssertEqual(state.resourceDescribe, "refreshed operator describe")
        XCTAssertTrue(state.lastResourceYAMLError?.contains("kept the newer local YAML draft") == true)
        XCTAssertNotEqual(state.resourceYAMLBaseline, refreshedYAML)
    }

    @MainActor
    func testHelmAndOperatorSelectionsStayMutuallyExclusiveAcrossListRefreshes() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = false
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let context = KubeContext(name: "synthetic-context-a")
        let release = helmRelease(name: "release-a", namespace: "scope-a")
        let operatorResource = OperatorResourceSummary(
            family: "synthetic-family",
            kind: "SyntheticKind",
            apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
            name: "operator-a",
            namespace: "scope-a",
            status: "Ready",
            message: ""
        )
        let state = RuneAppState()
        state.selectedContext = context
        state.selectedNamespace = "scope-a"
        state.selectedSection = .helm
        state.setHelmReleases([release])
        state.setOperatorResources([operatorResource])
        state.setSelectedOperatorResource(operatorResource)
        XCTAssertNil(state.selectedHelmRelease)

        let gate = SelectionRaceAsyncGate()
        let recorder = HelmListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            helmReleaseList: { _, _, _, allNamespaces in
                recorder.record(allNamespaces: allNamespaces)
                await gate.wait()
                return [release]
            }
        )
        viewModel.setHelmBrowserResourceFamily(.operatorResources)
        viewModel.setHelmAllNamespaces(true)
        try await waitUntil { recorder.callCount == 1 }
        gate.open()
        try await waitUntil {
            state.resourceListFreshness[.helmReleases]?.status == .live
        }

        XCTAssertEqual(state.selectedOperatorResource?.id, operatorResource.id)
        XCTAssertNil(state.selectedHelmRelease)

        state.setOperatorResources([], selectFallback: true)
        XCTAssertNil(state.selectedOperatorResource)
        XCTAssertNil(
            state.selectedHelmRelease,
            "Removing the operator must not reveal a release auto-selected by a sibling list refresh."
        )
    }

    @MainActor
    func testHelmFamilyPickerActivatesOnlyTheChosenSelectionFamily() {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = false
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let release = helmRelease(name: "release-a", namespace: "scope-a")
        let operatorResource = OperatorResourceSummary(
            family: "synthetic-family",
            kind: "SyntheticKind",
            apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
            name: "operator-a",
            namespace: "scope-a",
            status: "Ready",
            message: ""
        )
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .helm
        state.setHelmReleases([release])
        state.setOperatorResources([operatorResource])
        let viewModel = makeViewModel(state: state)

        viewModel.setHelmBrowserResourceFamily(.operatorResources)

        XCTAssertEqual(state.selectedOperatorResource?.id, operatorResource.id)
        XCTAssertNil(state.selectedHelmRelease)

        viewModel.setHelmBrowserResourceFamily(.helmReleases)

        XCTAssertEqual(state.selectedHelmRelease?.id, release.id)
        XCTAssertNil(state.selectedOperatorResource)
    }

    @MainActor
    func testScopeChangesSynchronouslyRetireSpecializedRowsAndSelections() {
        let contextA = KubeContext(name: "synthetic-context-a")
        let contextB = KubeContext(name: "synthetic-context-b")
        let release = helmRelease(name: "release-a", namespace: "scope-a")
        let operatorResource = OperatorResourceSummary(
            family: "synthetic-family",
            kind: "SyntheticKind",
            apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
            name: "operator-a",
            namespace: "scope-a",
            status: "Ready",
            message: ""
        )
        let state = RuneAppState()
        state.setContexts([contextA, contextB])
        state.selectedContext = contextA
        state.selectedNamespace = "scope-a"
        state.selectedSection = .helm
        state.setHelmReleases([release])
        state.setOperatorResources([operatorResource])
        let store = ResourceStore()
        store.cacheNamespaces(["scope-b"], context: contextB)
        let viewModel = makeViewModel(state: state, store: store)

        viewModel.setContext(contextB)

        XCTAssertTrue(state.helmReleases.isEmpty)
        XCTAssertNil(state.selectedHelmRelease)
        XCTAssertTrue(state.operatorResources.isEmpty)
        XCTAssertNil(state.selectedOperatorResource)

        let sameContextRelease = helmRelease(name: "release-all", namespace: "scope-other")
        state.isHelmAllNamespaces = true
        state.setHelmReleases([sameContextRelease])
        viewModel.setNamespace("scope-b")

        XCTAssertEqual(state.helmReleases, [sameContextRelease])
        XCTAssertEqual(state.selectedHelmRelease?.id, sameContextRelease.id)

        state.setOperatorResources([operatorResource])
        state.setSelectedOperatorResource(operatorResource)
        viewModel.setNamespace("scope-c")

        XCTAssertTrue(state.operatorResources.isEmpty)
        XCTAssertNil(state.selectedOperatorResource)
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
        XCTAssertNil(state.selectedHelmRelease)
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
    func testRBACCanICheckCannotPublishAfterScopeOrInputChanges() async throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        let scopeGate = SelectionRaceAsyncGate()
        let inputGate = SelectionRaceAsyncGate()
        let recorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            rbacCanICheck: { _, _, _, _, _, _, _ in
                let invocation = recorder.record()
                if invocation == 1 {
                    await scopeGate.wait()
                } else {
                    await inputGate.wait()
                }
                return true
            }
        )

        viewModel.runRBACCanISimulator()
        try await waitUntil { recorder.callCount == 1 }

        state.selectedNamespace = "scope-b"
        XCTAssertFalse(viewModel.isRunningRBACCanI)
        XCTAssertNil(viewModel.rbacCanIResult)

        scopeGate.open()
        try await Task.sleep(nanoseconds: 20_000_000)
        XCTAssertNil(
            viewModel.rbacCanIResult,
            "A Can-I response for the previous namespace must be discarded."
        )

        viewModel.runRBACCanISimulator()
        try await waitUntil { recorder.callCount == 2 }

        viewModel.rbacCanIVerb = "watch"
        XCTAssertFalse(viewModel.isRunningRBACCanI)
        XCTAssertNil(viewModel.rbacCanIResult)

        inputGate.open()
        try await Task.sleep(nanoseconds: 20_000_000)
        XCTAssertNil(
            viewModel.rbacCanIResult,
            "A Can-I response for an edited request must be discarded."
        )
    }

    @MainActor
    func testParallelPortForwardStartsKeepBusyStateAndCannotNavigateAfterUserLeaves() async throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        let firstGate = SelectionRaceAsyncGate()
        let secondGate = SelectionRaceAsyncGate()
        let recorder = PortForwardStartInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            portForwardStart: { _, context, namespace, targetKind, targetName, localPort, remotePort, address in
                recorder.record(localPort: localPort)
                if targetName == "pod-a" {
                    await firstGate.wait()
                } else {
                    await secondGate.wait()
                }
                return PortForwardSession(
                    id: "session-\(targetName)",
                    contextName: context.name,
                    namespace: namespace,
                    targetKind: targetKind,
                    targetName: targetName,
                    localPort: localPort,
                    remotePort: remotePort,
                    address: address,
                    status: .active
                )
            }
        )

        viewModel.portForwardLocalPortInput = "18080"
        viewModel.portForwardRemotePortInput = "8080"
        viewModel.startPortForward(targetKind: .pod, targetName: "pod-a")
        try await waitUntil { recorder.callCount == 1 }

        viewModel.portForwardLocalPortInput = "18081"
        viewModel.startPortForward(targetKind: .pod, targetName: "pod-b")
        try await waitUntil { recorder.callCount == 2 }
        XCTAssertTrue(state.isStartingPortForward)

        viewModel.setSection(.networking)

        firstGate.open()
        try await waitUntil {
            state.portForwardSessions.contains { $0.id == "session-pod-a" }
        }
        XCTAssertTrue(
            state.isStartingPortForward,
            "Completing one start must not clear the busy state while another start remains."
        )
        XCTAssertEqual(state.selectedSection, .networking)

        secondGate.open()
        try await waitUntil {
            !state.isStartingPortForward
                && state.portForwardSessions.contains { $0.id == "session-pod-b" }
        }

        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(recorder.localPorts, [18080, 18081])
    }

    @MainActor
    func testCronJobSuspendFreezesRequestScopeAndRejectsStaleFailure() async throws {
        let kubeConfigURL = try writeSyntheticKubeConfig("synthetic-config-a")
        defer {
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }
        let source = KubeConfigSource(url: kubeConfigURL)
        let contextA = KubeContext(name: "synthetic-context-a")
        let contextB = KubeContext(name: "synthetic-context-b")
        let cronJobA = rbacResource(kind: .cronJob, name: "cron-a", namespace: "scope-a")
        let cronJobB = rbacResource(kind: .cronJob, name: "cron-b", namespace: "scope-b")
        let state = RuneAppState()
        state.setSources([source])
        state.selectedContext = contextA
        state.selectedNamespace = "scope-a"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .cronJob
        state.setCronJobs([cronJobA])
        let gate = SelectionRaceAsyncGate()
        let recorder = CronJobSuspendInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            cronJobSuspendPatch: { sources, context, namespace, name, suspend in
                recorder.record(
                    sources: sources,
                    context: context,
                    namespace: namespace,
                    name: name,
                    suspend: suspend
                )
                await gate.wait()
                recorder.markFinished()
                throw RuneError.invalidInput(message: "Synthetic stale CronJob failure.")
            }
        )

        viewModel.setSelectedCronJobSuspended(true)
        state.selectedContext = contextB
        state.selectedNamespace = "scope-b"
        state.setCronJobs([cronJobB])
        try await waitUntil { recorder.callCount == 1 }

        let invocation = try XCTUnwrap(recorder.invocations.first)
        XCTAssertEqual(invocation.sources, [source])
        XCTAssertEqual(invocation.context, contextA)
        XCTAssertEqual(invocation.namespace, "scope-a")
        XCTAssertEqual(invocation.name, "cron-a")
        XCTAssertTrue(invocation.suspend)

        gate.open()
        try await waitUntil { recorder.didFinish }

        XCTAssertNil(
            state.lastError,
            "A failure from the old CronJob scope must not replace current-scope UI state."
        )
    }

    @MainActor
    func testCronJobSuspendCompletionIsDiscardedAfterSamePathKubeConfigRotation() async throws {
        let kubeConfigURL = try writeSyntheticKubeConfig("synthetic-config-a")
        defer {
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }
        let source = KubeConfigSource(url: kubeConfigURL)
        let cronJob = rbacResource(kind: .cronJob, name: "cron-a", namespace: "scope-a")
        let state = RuneAppState()
        state.setSources([source])
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .cronJob
        state.setCronJobs([cronJob])
        let gate = SelectionRaceAsyncGate()
        let recorder = CronJobSuspendInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            cronJobSuspendPatch: { sources, context, namespace, name, suspend in
                recorder.record(
                    sources: sources,
                    context: context,
                    namespace: namespace,
                    name: name,
                    suspend: suspend
                )
                await gate.wait()
                recorder.markFinished()
                throw RuneError.invalidInput(message: "Synthetic rotated-source failure.")
            }
        )

        viewModel.setSelectedCronJobSuspended(true)
        try await waitUntil { recorder.callCount == 1 }
        try advanceSyntheticKubeConfigFingerprint(at: kubeConfigURL)
        gate.open()
        try await waitUntil { recorder.didFinish }

        XCTAssertNil(
            state.lastError,
            "A completion from an older same-path kubeconfig revision must be discarded."
        )
    }

    @MainActor
    func testIdenticalCronJobSuspendIntentIsAdmittedAfterSamePathKubeConfigRotation() async throws {
        let kubeConfigURL = try writeSyntheticKubeConfig("synthetic-config-a")
        defer {
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }
        let source = KubeConfigSource(url: kubeConfigURL)
        let cronJob = rbacResource(kind: .cronJob, name: "cron-a", namespace: "scope-a")
        let state = RuneAppState()
        state.setSources([source])
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .cronJob
        state.setCronJobs([cronJob])
        let firstGate = SelectionRaceAsyncGate()
        let secondGate = SelectionRaceAsyncGate()
        let recorder = CronJobSuspendInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            cronJobSuspendPatch: { sources, context, namespace, name, suspend in
                recorder.record(
                    sources: sources,
                    context: context,
                    namespace: namespace,
                    name: name,
                    suspend: suspend
                )
                if recorder.callCount == 1 {
                    await firstGate.wait()
                } else {
                    await secondGate.wait()
                }
                recorder.markFinished()
            }
        )

        viewModel.setSelectedCronJobSuspended(true)
        try await waitUntil { recorder.callCount == 1 }

        try advanceSyntheticKubeConfigFingerprint(at: kubeConfigURL)
        viewModel.setSelectedCronJobSuspended(true)
        try await waitUntil { recorder.callCount == 2 }

        XCTAssertEqual(recorder.invocations.map(\.suspend), [true, true])

        firstGate.open()
        secondGate.open()
        try await waitUntil { recorder.finishedCount == 2 }
    }

    @MainActor
    func testRapidCronJobSuspendIntentsReachServerInAdmissionOrder() async throws {
        let cronJob = rbacResource(kind: .cronJob, name: "cron-a", namespace: "scope-a")
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .cronJob
        state.setCronJobs([cronJob])
        let firstGate = SelectionRaceAsyncGate()
        let secondGate = SelectionRaceAsyncGate()
        let recorder = CronJobSuspendInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            cronJobSuspendPatch: { sources, context, namespace, name, suspend in
                recorder.record(
                    sources: sources,
                    context: context,
                    namespace: namespace,
                    name: name,
                    suspend: suspend
                )
                if suspend {
                    await firstGate.wait()
                } else {
                    await secondGate.wait()
                }
                recorder.markFinished()
            }
        )

        viewModel.setSelectedCronJobSuspended(true)
        try await waitUntil { recorder.callCount == 1 }
        viewModel.setSelectedCronJobSuspended(false)
        await Task.yield()

        XCTAssertEqual(
            recorder.callCount,
            1,
            "The newer intent must wait until the earlier server patch has completed."
        )

        firstGate.open()
        try await waitUntil { recorder.callCount == 2 }
        XCTAssertEqual(recorder.invocations.map(\.suspend), [true, false])

        state.selectedNamespace = "scope-b"
        secondGate.open()
        try await waitUntil { recorder.finishedCount == 2 }
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testIdenticalRapidCronJobSuspendIntentsAreDeduplicated() async throws {
        let cronJob = rbacResource(kind: .cronJob, name: "cron-a", namespace: "scope-a")
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .cronJob
        state.setCronJobs([cronJob])
        let gate = SelectionRaceAsyncGate()
        let recorder = CronJobSuspendInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            cronJobSuspendPatch: { sources, context, namespace, name, suspend in
                recorder.record(
                    sources: sources,
                    context: context,
                    namespace: namespace,
                    name: name,
                    suspend: suspend
                )
                await gate.wait()
                recorder.markFinished()
            }
        )

        viewModel.setSelectedCronJobSuspended(true)
        try await waitUntil { recorder.callCount == 1 }
        for _ in 0..<63 {
            viewModel.setSelectedCronJobSuspended(true)
        }
        await Task.yield()

        XCTAssertEqual(recorder.callCount, 1)
        state.selectedNamespace = "scope-b"
        gate.open()
        try await waitUntil { recorder.finishedCount == 1 }
        XCTAssertEqual(recorder.callCount, 1)
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
    func testAsyncOperatorRefreshPreservesTheLatestValidUserPage() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let resources = (0..<85).map { index in
            OperatorResourceSummary(
                family: "synthetic-family",
                kind: "SyntheticKind",
                apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
                name: "operator-\(index)",
                namespace: "scope-a",
                status: "Ready",
                message: ""
            )
        }
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .helm
        state.setOperatorResources(resources)
        let gate = SelectionRaceAsyncGate()
        let recorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            operatorResourceList: { _, _, _ in
                recorder.record()
                await gate.wait()
                return resources
            }
        )

        viewModel.setHelmBrowserResourceFamily(.operatorResources)
        try await waitUntil { recorder.callCount == 1 }
        viewModel.pageOperatorResourcesForward()
        XCTAssertEqual(viewModel.operatorResourcePage, 1)

        gate.open()
        try await waitUntil {
            state.resourceListFreshness[.operatorResources]?.status == .live
        }

        XCTAssertEqual(viewModel.operatorResourcePage, 1)
        XCTAssertEqual(viewModel.operatorResourcePageSummary, "41-80 of 85")
    }

    @MainActor
    func testOlderReplicaSetRefreshCannotSnapRapidSelectionFromCBackToA() async throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "scope-a"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .replicaSet
        let replicaSets = ["a", "b", "c"].map { name in
            ClusterResourceSummary(
                kind: .replicaSet,
                name: "replica-\(name)",
                namespace: "scope-a",
                primaryText: "1/1",
                secondaryText: "Synthetic"
            )
        }
        state.setReplicaSets(replicaSets)
        let recorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            replicaSetList: { _, _, _ in
                let invocation = recorder.record()
                if invocation == 1 {
                    try await Task.sleep(nanoseconds: 220_000_000)
                    return Array(replicaSets.prefix(2))
                }
                try await Task.sleep(nanoseconds: 20_000_000)
                return replicaSets
            }
        )

        viewModel.refreshReplicaSetsForCurrentNamespace()
        try await waitUntil { recorder.callCount == 1 }
        viewModel.refreshReplicaSetsForCurrentNamespace()
        try await waitUntil { recorder.callCount == 2 && state.replicaSets == replicaSets }

        viewModel.selectReplicaSet(replicaSets[0])
        viewModel.selectReplicaSet(replicaSets[1])
        viewModel.selectReplicaSet(replicaSets[2])
        try await Task.sleep(nanoseconds: 260_000_000)

        XCTAssertEqual(state.replicaSets, replicaSets)
        XCTAssertEqual(state.selectedReplicaSet?.id, replicaSets[2].id)
    }

    @MainActor
    func testOlderOperatorRefreshCannotClearRapidSelectionAfterAThenBThenC() async throws {
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
        let resources = ["a", "b", "c"].map { name in
            OperatorResourceSummary(
                family: "synthetic-family",
                kind: "SyntheticKind",
                apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
                name: "operator-\(name)",
                namespace: "scope-a",
                status: "Ready",
                message: ""
            )
        }
        state.setOperatorResources(resources)
        let recorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            helmReleaseList: { _, _, _, _ in [] },
            operatorResourceList: { _, _, _ in
                let invocation = recorder.record()
                if invocation == 1 {
                    try await Task.sleep(nanoseconds: 220_000_000)
                    return Array(resources.prefix(2))
                }
                try await Task.sleep(nanoseconds: 20_000_000)
                return resources
            }
        )

        viewModel.setHelmBrowserResourceFamily(.operatorResources)
        try await waitUntil { recorder.callCount == 1 }
        viewModel.setHelmBrowserResourceFamily(.helmReleases)
        viewModel.setHelmBrowserResourceFamily(.operatorResources)
        try await waitUntil { recorder.callCount == 2 && state.operatorResources == resources }

        viewModel.selectOperatorResource(resources[0])
        viewModel.selectOperatorResource(resources[1])
        viewModel.selectOperatorResource(resources[2])
        try await Task.sleep(nanoseconds: 260_000_000)

        XCTAssertEqual(state.operatorResources, resources)
        XCTAssertEqual(state.selectedOperatorResource?.id, resources[2].id)
    }

    @MainActor
    func testOperatorRefreshFailurePreservesExistingListAndSelection() async throws {
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
        let resources = ["a", "b", "c"].map { name in
            OperatorResourceSummary(
                family: "synthetic-family",
                kind: "SyntheticKind",
                apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
                name: "operator-\(name)",
                namespace: "scope-a",
                status: "Ready",
                message: ""
            )
        }
        state.setOperatorResources(resources)
        state.setSelectedOperatorResource(resources[2])
        let recorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            operatorResourceList: { _, _, _ in
                recorder.record()
                throw RuneError.invalidInput(message: "Synthetic operator list failure")
            }
        )

        viewModel.setHelmBrowserResourceFamily(.operatorResources)
        try await waitUntil {
            recorder.callCount == 1
                && state.resourceListFreshness[.operatorResources]?.status == .failed
        }

        XCTAssertEqual(state.operatorResources, resources)
        XCTAssertEqual(state.selectedOperatorResource?.id, resources[2].id)
    }

    @MainActor
    func testSpecializedListResultsFromChangedKubeConfigFingerprintAreDiscarded() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let kubeConfigURL = try writeSyntheticKubeConfig("apiVersion: v1\nkind: Config\n")
        defer {
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }
        let source = KubeConfigSource(url: kubeConfigURL)
        let context = KubeContext(name: "synthetic-context-a")

        let helmState = RuneAppState()
        helmState.setSources([source])
        helmState.selectedContext = context
        helmState.selectedNamespace = "scope-a"
        helmState.selectedSection = .helm
        let baselineRelease = helmRelease(name: "baseline-release", namespace: "scope-a")
        let staleRelease = helmRelease(name: "stale-release", namespace: "scope-a")
        helmState.setHelmReleases([baselineRelease])
        let helmRecorder = HelmListInvocationRecorder()
        let helmViewModel = makeViewModel(
            state: helmState,
            helmReleaseList: { _, _, _, allNamespaces in
                helmRecorder.record(allNamespaces: allNamespaces)
                if helmRecorder.callCount == 1 {
                    try await Task.sleep(nanoseconds: 180_000_000)
                    return [staleRelease]
                }
                try await Task.sleep(nanoseconds: 20_000_000)
                return [baselineRelease]
            }
        )

        let operatorResources = ["a", "b", "c"].map { name in
            OperatorResourceSummary(
                family: "synthetic-family",
                kind: "SyntheticKind",
                apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
                name: "operator-\(name)",
                namespace: "scope-a",
                status: "Ready",
                message: ""
            )
        }
        let operatorState = RuneAppState()
        operatorState.setSources([source])
        operatorState.selectedContext = context
        operatorState.selectedNamespace = "scope-a"
        operatorState.selectedSection = .helm
        operatorState.setOperatorResources(operatorResources)
        operatorState.setSelectedOperatorResource(operatorResources[2])
        let operatorRecorder = ListInvocationRecorder()
        let operatorViewModel = makeViewModel(
            state: operatorState,
            operatorResourceList: { _, _, _ in
                let invocation = operatorRecorder.record()
                if invocation == 1 {
                    try await Task.sleep(nanoseconds: 180_000_000)
                    return Array(operatorResources.prefix(2))
                }
                try await Task.sleep(nanoseconds: 20_000_000)
                return operatorResources
            }
        )

        let replicaSets = ["a", "b", "c"].map { name in
            ClusterResourceSummary(
                kind: .replicaSet,
                name: "replica-\(name)",
                namespace: "scope-a",
                primaryText: "1/1",
                secondaryText: "Synthetic"
            )
        }
        let replicaState = RuneAppState()
        replicaState.setSources([source])
        replicaState.selectedContext = context
        replicaState.selectedNamespace = "scope-a"
        replicaState.selectedSection = .workloads
        replicaState.selectedWorkloadKind = .replicaSet
        replicaState.setReplicaSets(replicaSets)
        replicaState.setSelectedReplicaSet(replicaSets[2])
        let replicaRecorder = ListInvocationRecorder()
        let replicaViewModel = makeViewModel(
            state: replicaState,
            replicaSetList: { _, _, _ in
                let invocation = replicaRecorder.record()
                if invocation == 1 {
                    try await Task.sleep(nanoseconds: 180_000_000)
                    return Array(replicaSets.prefix(2))
                }
                try await Task.sleep(nanoseconds: 20_000_000)
                return replicaSets
            }
        )

        helmViewModel.setHelmAllNamespaces(true)
        operatorViewModel.setHelmBrowserResourceFamily(.operatorResources)
        replicaViewModel.refreshReplicaSetsForCurrentNamespace()
        try await waitUntil {
            helmRecorder.callCount == 1
                && operatorRecorder.callCount == 1
                && replicaRecorder.callCount == 1
        }
        try advanceSyntheticKubeConfigFingerprint(at: kubeConfigURL)
        try await waitUntil {
            helmRecorder.callCount == 2
                && operatorRecorder.callCount == 2
                && replicaRecorder.callCount == 2
                && helmState.helmReleases == [baselineRelease]
                && operatorState.operatorResources == operatorResources
                && replicaState.replicaSets == replicaSets
                && helmState.resourceListFreshness[.helmReleases]?.status == .live
                && operatorState.resourceListFreshness[.operatorResources]?.status == .live
                && replicaState.resourceListFreshness[.replicaSets]?.status == .live
        }

        XCTAssertEqual(helmState.helmReleases, [baselineRelease])
        XCTAssertEqual(helmState.selectedHelmRelease, baselineRelease)
        XCTAssertEqual(operatorState.operatorResources, operatorResources)
        XCTAssertEqual(operatorState.selectedOperatorResource?.id, operatorResources[2].id)
        XCTAssertEqual(replicaState.replicaSets, replicaSets)
        XCTAssertEqual(replicaState.selectedReplicaSet?.id, replicaSets[2].id)
    }

    @MainActor
    func testScheduledRefreshImmediatelyInvalidatesSpecializedListsItWillReplace() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let context = KubeContext(name: "synthetic-context-a")
        let baselineRelease = helmRelease(name: "baseline-release", namespace: "scope-a")
        let staleRelease = helmRelease(name: "stale-release", namespace: "scope-a")
        let helmState = RuneAppState()
        helmState.selectedContext = context
        helmState.selectedNamespace = "scope-a"
        helmState.selectedSection = .helm
        helmState.setHelmReleases([baselineRelease])
        let helmRecorder = HelmListInvocationRecorder()
        let helmViewModel = makeViewModel(
            state: helmState,
            helmReleaseList: { _, _, _, allNamespaces in
                helmRecorder.record(allNamespaces: allNamespaces)
                try await Task.sleep(nanoseconds: 60_000_000)
                return [staleRelease]
            }
        )

        let operatorResources = ["a", "b", "c"].map { name in
            OperatorResourceSummary(
                family: "synthetic-family",
                kind: "SyntheticKind",
                apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
                name: "operator-\(name)",
                namespace: "scope-a",
                status: "Ready",
                message: ""
            )
        }
        let operatorState = RuneAppState()
        operatorState.selectedContext = context
        operatorState.selectedNamespace = "scope-a"
        operatorState.selectedSection = .helm
        operatorState.setOperatorResources(operatorResources)
        operatorState.setSelectedOperatorResource(operatorResources[2])
        let operatorRecorder = ListInvocationRecorder()
        let operatorViewModel = makeViewModel(
            state: operatorState,
            operatorResourceList: { _, _, _ in
                operatorRecorder.record()
                try await Task.sleep(nanoseconds: 60_000_000)
                return Array(operatorResources.prefix(2))
            }
        )

        let replicaSets = ["a", "b", "c"].map { name in
            ClusterResourceSummary(
                kind: .replicaSet,
                name: "replica-\(name)",
                namespace: "scope-a",
                primaryText: "1/1",
                secondaryText: "Synthetic"
            )
        }
        let replicaStore = ResourceStore()
        cacheReplicaSets(replicaSets, context: context, namespace: "scope-a", store: replicaStore)
        let replicaState = RuneAppState()
        replicaState.selectedContext = context
        replicaState.selectedNamespace = "scope-a"
        replicaState.selectedSection = .workloads
        replicaState.selectedWorkloadKind = .replicaSet
        replicaState.setReplicaSets(replicaSets)
        replicaState.setSelectedReplicaSet(replicaSets[2])
        let replicaRecorder = ListInvocationRecorder()
        let replicaViewModel = makeViewModel(
            state: replicaState,
            store: replicaStore,
            replicaSetList: { _, _, _ in
                replicaRecorder.record()
                try await Task.sleep(nanoseconds: 60_000_000)
                return Array(replicaSets.prefix(2))
            }
        )

        helmViewModel.setHelmAllNamespaces(true)
        operatorViewModel.setHelmBrowserResourceFamily(.operatorResources)
        replicaViewModel.refreshReplicaSetsForCurrentNamespace()
        try await waitUntil {
            helmRecorder.callCount == 1
                && operatorRecorder.callCount == 1
                && replicaRecorder.callCount == 1
        }

        helmViewModel.refreshCurrentView(debounced: true)
        operatorViewModel.refreshCurrentView(debounced: true)
        replicaViewModel.refreshCurrentView(debounced: true)
        try await Task.sleep(nanoseconds: 90_000_000)

        XCTAssertEqual(helmState.helmReleases, [baselineRelease])
        XCTAssertEqual(operatorState.operatorResources, operatorResources)
        XCTAssertEqual(operatorState.selectedOperatorResource?.id, operatorResources[2].id)
        XCTAssertEqual(replicaState.replicaSets, replicaSets)
        XCTAssertEqual(replicaState.selectedReplicaSet?.id, replicaSets[2].id)

        // Prevent the delayed general refreshes from doing unrelated network work
        // after this test has proved the pre-debounce invalidation.
        helmState.selectedContext = nil
        operatorState.selectedContext = nil
        replicaState.selectedContext = nil
        try await Task.sleep(nanoseconds: 50_000_000)
    }

    @MainActor
    func testDiscardedSnapshotDoesNotRunHelmFollowUpOrReportLive() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let namespacePath = "/api/v1/namespaces"
        let fixture = RuneFakeK8sFixture(
            delayedResponseTargets: [namespacePath: 180_000_000]
        )
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        let kubeConfigURL = try writeSyntheticKubeConfig(server.kubeconfigYAML())
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }

        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let baselineRelease = helmRelease(name: "baseline-release", namespace: "alpha-zone")
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeConfigURL)])
        state.selectedContext = context
        state.selectedNamespace = "alpha-zone"
        state.selectedSection = .helm
        state.setHelmReleases([baselineRelease])
        let helmRecorder = HelmListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 2),
            helmReleaseList: { _, _, _, allNamespaces in
                helmRecorder.record(allNamespaces: allNamespaces)
                return []
            }
        )

        viewModel.refreshCurrentView(debounced: false)
        try await waitUntil {
            server.requestLines().contains { $0.contains("GET \(namespacePath) ") }
        }
        try advanceSyntheticKubeConfigFingerprint(at: kubeConfigURL)
        try await waitUntil {
            !state.isLoading
                && state.snapshotFreshness.status == .reconnecting
                && state.resourceListFreshness[.helmReleases]?.status == .reconnecting
        }

        XCTAssertEqual(helmRecorder.callCount, 0)
        XCTAssertEqual(state.helmReleases, [baselineRelease])
        XCTAssertNotEqual(state.snapshotFreshness.status, .live)
    }

    @MainActor
    func testCancelledHelmFollowUpFinishesAlreadyAppliedSnapshotAsLive() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let server = try await RuneFakeK8sRESTServer.start(fixture: RuneFakeK8sFixture())
        let kubeConfigURL = try writeSyntheticKubeConfig(server.kubeconfigYAML())
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }

        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let baselineRelease = helmRelease(name: "baseline-release", namespace: "alpha-zone")
        let staleRelease = helmRelease(name: "stale-release", namespace: "alpha-zone")
        let operatorResource = OperatorResourceSummary(
            family: "synthetic-family",
            kind: "SyntheticKind",
            apiPath: "/apis/example.test/v1/namespaces/alpha-zone/syntheticresources",
            name: "operator-current",
            namespace: "alpha-zone",
            status: "Ready",
            message: ""
        )
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeConfigURL)])
        state.selectedContext = context
        state.selectedNamespace = "alpha-zone"
        state.selectedSection = .helm
        state.setHelmReleases([baselineRelease])
        let helmRecorder = HelmListInvocationRecorder()
        let operatorRecorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 2),
            helmReleaseList: { _, _, _, allNamespaces in
                helmRecorder.record(allNamespaces: allNamespaces)
                try await Task.sleep(nanoseconds: 180_000_000)
                return [staleRelease]
            },
            operatorResourceList: { _, _, _ in
                operatorRecorder.record()
                return [operatorResource]
            }
        )

        viewModel.refreshCurrentView(debounced: false)
        try await waitUntil { helmRecorder.callCount == 1 }
        viewModel.setHelmBrowserResourceFamily(.operatorResources)
        try await waitUntil {
            operatorRecorder.callCount == 1
                && state.operatorResources == [operatorResource]
                && state.snapshotFreshness.status == .live
        }

        XCTAssertEqual(state.helmReleases, [baselineRelease])
        XCTAssertEqual(state.snapshotFreshness.status, .live)
    }

    @MainActor
    func testSupersededOperatorFollowUpCannotMarkReplacementSnapshotLive() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = false
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let server = try await RuneFakeK8sRESTServer.start(fixture: RuneFakeK8sFixture())
        let kubeConfigURL = try writeSyntheticKubeConfig(server.kubeconfigYAML())
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }

        let firstFollowUpGate = SelectionRaceAsyncGate()
        let replacementFollowUpGate = SelectionRaceAsyncGate()
        defer {
            firstFollowUpGate.open()
            replacementFollowUpGate.open()
        }
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let operatorResource = OperatorResourceSummary(
            family: "synthetic-family",
            kind: "SyntheticKind",
            apiPath: "/apis/example.test/v1/namespaces/alpha-zone/syntheticresources",
            name: "operator-current",
            namespace: "alpha-zone",
            status: "Ready",
            message: ""
        )
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeConfigURL)])
        state.selectedContext = context
        state.selectedNamespace = "alpha-zone"
        state.selectedSection = .helm
        let startedRecorder = ListInvocationRecorder()
        let finishedRecorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 2),
            helmReleaseList: { _, _, _, _ in [] },
            operatorResourceList: { _, _, _ in
                let invocation = startedRecorder.record()
                if invocation == 1 {
                    await firstFollowUpGate.wait()
                } else {
                    await replacementFollowUpGate.wait()
                }
                finishedRecorder.record()
                return [operatorResource]
            }
        )

        viewModel.refreshCurrentView(debounced: false)
        try await waitUntil { startedRecorder.callCount == 1 }
        viewModel.refreshCurrentView(debounced: false)
        try await waitUntil {
            startedRecorder.callCount == 2
                && state.snapshotFreshness.status == .refreshing
        }

        firstFollowUpGate.open()
        try await waitUntil { finishedRecorder.callCount == 1 }
        await Task.yield()

        XCTAssertEqual(state.snapshotFreshness.status, .refreshing)

        replacementFollowUpGate.open()
        try await waitUntil {
            finishedRecorder.callCount == 2
                && state.snapshotFreshness.status == .live
        }
    }

    @MainActor
    func testEmptySourceSyncClearsAndInvalidatesSpecializedResourceLists() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let kubeConfigURL = try writeSyntheticKubeConfig("apiVersion: v1\nkind: Config\n")
        defer {
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }
        let context = KubeContext(name: "synthetic-context-a")
        let release = helmRelease(name: "current-release", namespace: "scope-a")
        let staleRelease = helmRelease(name: "stale-release", namespace: "scope-a")
        let operatorResource = OperatorResourceSummary(
            family: "synthetic-family",
            kind: "SyntheticKind",
            apiPath: "/apis/example.test/v1/namespaces/scope-a/syntheticresources",
            name: "operator-current",
            namespace: "scope-a",
            status: "Ready",
            message: ""
        )
        let replicaSet = ClusterResourceSummary(
            kind: .replicaSet,
            name: "replica-current",
            namespace: "scope-a",
            primaryText: "1/1",
            secondaryText: "Synthetic"
        )

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeConfigURL)])
        state.selectedContext = context
        state.selectedNamespace = "scope-a"
        state.selectedSection = .helm
        state.setHelmReleases([release])
        state.setSelectedHelmRelease(release)
        state.setOperatorResources([operatorResource])
        state.setSelectedOperatorResource(operatorResource)
        state.setReplicaSets([replicaSet])
        state.setSelectedReplicaSet(replicaSet)
        let helmRecorder = HelmListInvocationRecorder()
        let replicaRecorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            helmReleaseList: { _, _, _, allNamespaces in
                helmRecorder.record(allNamespaces: allNamespaces)
                try await Task.sleep(nanoseconds: 120_000_000)
                return [staleRelease]
            },
            replicaSetList: { _, _, _ in
                replicaRecorder.record()
                try await Task.sleep(nanoseconds: 120_000_000)
                return [replicaSet]
            }
        )

        viewModel.setHelmAllNamespaces(true)
        viewModel.refreshReplicaSetsForCurrentNamespace()
        try await waitUntil {
            helmRecorder.callCount == 1 && replicaRecorder.callCount == 1
        }

        let changed = try await viewModel.syncKubeConfigSourcesFromDiscovery(reason: "synthetic-empty")
        XCTAssertTrue(changed)
        try await Task.sleep(nanoseconds: 150_000_000)

        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertTrue(state.namespaces.isEmpty)
        XCTAssertTrue(state.helmReleases.isEmpty)
        XCTAssertNil(state.selectedHelmRelease)
        XCTAssertTrue(state.operatorResources.isEmpty)
        XCTAssertNil(state.selectedOperatorResource)
        XCTAssertTrue(state.replicaSets.isEmpty)
        XCTAssertNil(state.selectedReplicaSet)
        XCTAssertTrue(state.resourceListFreshness.isEmpty)
        XCTAssertFalse(state.isLoading)
    }

    @MainActor
    func testContextListResultCannotRestoreContextsAfterSourcesBecomeEmpty() async throws {
        let kubeConfigURL = try writeSyntheticKubeConfig("apiVersion: v1\nkind: Config\n")
        defer {
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }

        let staleContext = KubeContext(name: "synthetic-context-a")
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeConfigURL)])
        state.setContexts([staleContext])
        state.selectedContext = staleContext
        state.selectedNamespace = "scope-a"
        let recorder = ListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            kubeContextList: { _ in
                recorder.record()
                try await Task.sleep(nanoseconds: 120_000_000)
                return [staleContext]
            }
        )

        let reloadTask = Task {
            try await viewModel.reloadContexts()
        }
        try await waitUntil { recorder.callCount == 1 && state.isLoading }

        let changed = try await viewModel.syncKubeConfigSourcesFromDiscovery(reason: "synthetic-empty")
        XCTAssertTrue(changed)
        do {
            try await reloadTask.value
            XCTFail("The superseded context request should be cancelled.")
        } catch is CancellationError {
            // Expected: empty-source cleanup invalidates the captured context scope.
        }

        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertNil(state.selectedContext)
        XCTAssertTrue(state.namespaces.isEmpty)
        XCTAssertFalse(state.isLoading)
    }

    @MainActor
    func testFinishingContextReloadDoesNotHidePendingHelmLoad() async throws {
        let context = KubeContext(name: "synthetic-context-a")
        let state = RuneAppState()
        state.selectedContext = context
        state.selectedNamespace = "scope-a"
        state.selectedSection = .helm
        let contextRecorder = ListInvocationRecorder()
        let helmRecorder = HelmListInvocationRecorder()
        let viewModel = makeViewModel(
            state: state,
            kubeContextList: { _ in
                contextRecorder.record()
                try await Task.sleep(nanoseconds: 60_000_000)
                return []
            },
            helmReleaseList: { _, _, _, allNamespaces in
                helmRecorder.record(allNamespaces: allNamespaces)
                try await Task.sleep(nanoseconds: 220_000_000)
                return []
            }
        )

        let reloadTask = Task {
            try await viewModel.reloadContexts()
        }
        try await waitUntil { contextRecorder.callCount == 1 && state.isLoading }
        viewModel.setHelmAllNamespaces(true)
        try await waitUntil { helmRecorder.callCount == 1 }

        try await reloadTask.value

        XCTAssertTrue(state.isLoading)
        try await waitUntil { !state.isLoading }
    }

    @MainActor
    func testCancelledSnapshotCannotReplaceNewerSameScopePodListDuringDebouncedRefresh() async throws {
        let podListPath = "/api/v1/namespaces/alpha-zone/pods"
        let fixture = RuneFakeK8sFixture(
            delayedResponseTargets: [podListPath: 600_000_000]
        )
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        let kubeConfigURL = try writeSyntheticKubeConfig(server.kubeconfigYAML())
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }

        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let stalePods = Array(snapshotRacePods.prefix(2))
        let currentPods = snapshotRacePods
        let store = ResourceStore()
        store.cacheNamespaces(["alpha-zone"], context: context)
        cachePods(stalePods, context: context, namespace: "alpha-zone", store: store)

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeConfigURL)])
        state.selectedContext = context
        state.selectedNamespace = "alpha-zone"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        let viewModel = makeViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 2),
            store: store
        )

        viewModel.refreshCurrentView(debounced: false)
        try await waitUntil {
            server.requestLines().contains { $0.contains("GET \(podListPath) ") }
        }

        state.setPods(currentPods)
        state.setSelectedPod(currentPods[2])
        cachePods(currentPods, context: context, namespace: "alpha-zone", store: store)

        // Let the replacement refresh load a different family so its own pod
        // request cannot hide whether the canceled, already in-flight pod read
        // was rejected after its delayed response arrives.
        state.selectedSection = .networking
        state.selectedWorkloadKind = .service
        viewModel.refreshCurrentView(debounced: true)
        try await waitUntil(timeoutNanoseconds: 2_000_000_000) {
            state.snapshotFreshness.status == .live
        }
        try await Task.sleep(nanoseconds: 50_000_000)

        XCTAssertEqual(state.pods.map(\.id), currentPods.map(\.id))
        XCTAssertEqual(state.selectedPod?.id, currentPods[2].id)
        XCTAssertEqual(state.snapshotFreshness.status, .live)
    }

    @MainActor
    func testSnapshotFromChangedKubeConfigFingerprintCannotReplaceCurrentPodSelection() async throws {
        let podListPath = "/api/v1/namespaces/alpha-zone/pods"
        let fixture = RuneFakeK8sFixture(
            delayedResponseTargets: [podListPath: 350_000_000]
        )
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        let kubeConfigURL = try writeSyntheticKubeConfig(server.kubeconfigYAML())
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeConfigURL.deletingLastPathComponent())
        }

        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let currentPods = snapshotRacePods
        let store = ResourceStore()
        store.cacheNamespaces(["alpha-zone"], context: context)
        cachePods(currentPods, context: context, namespace: "alpha-zone", store: store)

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeConfigURL)])
        state.selectedContext = context
        state.selectedNamespace = "alpha-zone"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.setPods(currentPods)
        state.setSelectedPod(currentPods[2])
        let viewModel = makeViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 2),
            store: store
        )

        viewModel.refreshCurrentView(debounced: false)
        try await waitUntil {
            server.requestLines().contains { $0.contains("GET \(podListPath) ") }
        }

        try advanceSyntheticKubeConfigFingerprint(at: kubeConfigURL)

        try await waitUntil { !state.isLoading }

        XCTAssertEqual(state.pods.map(\.id), currentPods.map(\.id))
        XCTAssertEqual(state.selectedPod?.id, currentPods[2].id)
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
        kubeClient: KubernetesClient = KubernetesClient(),
        store: ResourceStore = ResourceStore(),
        bookmarkManager: BookmarkManager = BookmarkManager(store: EmptySelectionRaceBookmarkStore()),
        kubeConfigDiscoverer: KubeConfigDiscovering = EmptySelectionRaceKubeConfigDiscoverer(),
        kubeContextList: KubeContextListing? = nil,
        helmReleaseList: HelmReleaseListing? = nil,
        operatorResourceList: OperatorResourceListing? = nil,
        replicaSetList: ReplicaSetListing? = nil,
        rbacCanICheck: RBACCanIChecking? = nil,
        portForwardStart: PortForwardStarting? = nil,
        cronJobSuspendPatch: CronJobSuspendPatching? = nil,
        resourceManifestLoad: ResourceManifestLoading? = nil,
        operatorResourceManifestLoad: OperatorResourceManifestLoading? = nil
    ) -> RuneAppViewModel {
        RuneAppViewModel(
            state: state,
            kubeClient: kubeClient,
            bookmarkManager: bookmarkManager,
            kubeConfigDiscoverer: kubeConfigDiscoverer,
            store: store,
            contextPreferences: EmptyContextPreferences(),
            savedWorkspaceStore: InMemorySavedWorkspaceStore(),
            kubeContextList: kubeContextList,
            helmReleaseList: helmReleaseList,
            operatorResourceList: operatorResourceList,
            replicaSetList: replicaSetList,
            rbacCanICheck: rbacCanICheck,
            portForwardStart: portForwardStart,
            cronJobSuspendPatch: cronJobSuspendPatch,
            resourceManifestLoad: resourceManifestLoad,
            operatorResourceManifestLoad: operatorResourceManifestLoad
        )
    }

    private var snapshotRacePods: [PodSummary] {
        [
            PodSummary(
                name: "orbit-lens-6f58d7d89b-hx9q2",
                namespace: "alpha-zone",
                status: "Running"
            ),
            PodSummary(
                name: "ember-gate-75c9f746b8-kq2wm",
                namespace: "alpha-zone",
                status: "Running"
            ),
            PodSummary(
                name: "synthetic-current-pod",
                namespace: "alpha-zone",
                status: "Running"
            )
        ]
    }

    private func writeSyntheticKubeConfig(_ yaml: String) throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-selection-race-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let url = directory.appendingPathComponent("config.yaml")
        try yaml.write(to: url, atomically: true, encoding: .utf8)
        return url
    }

    private func advanceSyntheticKubeConfigFingerprint(at url: URL) throws {
        let original = try String(contentsOf: url, encoding: .utf8)
        try (original + "\n# synthetic fingerprint revision\n").write(
            to: url,
            atomically: true,
            encoding: .utf8
        )
        try FileManager.default.setAttributes(
            [.modificationDate: Date(timeIntervalSinceNow: 2)],
            ofItemAtPath: url.path
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
    private func cachePods(
        _ pods: [PodSummary],
        context: KubeContext,
        namespace: String,
        store: ResourceStore
    ) {
        store.cacheSnapshot(
            context: context,
            namespace: namespace,
            pods: pods,
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
            serviceAccounts: [],
            events: []
        )
    }

    @MainActor
    private func cacheReplicaSets(
        _ replicaSets: [ClusterResourceSummary],
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
            replicaSets: replicaSets,
            persistentVolumeClaims: [],
            horizontalPodAutoscalers: [],
            networkPolicies: [],
            services: [],
            endpoints: [],
            ingresses: [],
            configMaps: [],
            secrets: [],
            serviceAccounts: [],
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

@MainActor
private final class ListInvocationRecorder {
    private(set) var callCount = 0

    @discardableResult
    func record() -> Int {
        callCount += 1
        return callCount
    }
}

@MainActor
private final class PortForwardStartInvocationRecorder {
    private(set) var localPorts: [Int] = []

    var callCount: Int {
        localPorts.count
    }

    func record(localPort: Int) {
        localPorts.append(localPort)
    }
}

@MainActor
private final class CronJobSuspendInvocationRecorder {
    struct Invocation {
        let sources: [KubeConfigSource]
        let context: KubeContext
        let namespace: String
        let name: String
        let suspend: Bool
    }

    private(set) var invocations: [Invocation] = []
    private(set) var finishedCount = 0

    var callCount: Int {
        invocations.count
    }

    var didFinish: Bool {
        finishedCount > 0
    }

    func record(
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        name: String,
        suspend: Bool
    ) {
        invocations.append(
            Invocation(
                sources: sources,
                context: context,
                namespace: namespace,
                name: name,
                suspend: suspend
            )
        )
    }

    func markFinished() {
        finishedCount += 1
    }
}

@MainActor
private final class SelectionRaceAsyncGate {
    private var continuation: CheckedContinuation<Void, Never>?
    private var isOpen = false

    func wait() async {
        guard !isOpen else { return }
        await withCheckedContinuation { continuation in
            self.continuation = continuation
        }
    }

    func open() {
        isOpen = true
        continuation?.resume()
        continuation = nil
    }
}

private struct EmptySelectionRaceKubeConfigDiscoverer: KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL] {
        []
    }
}

private final class EmptySelectionRaceBookmarkStore: BookmarkStore, @unchecked Sendable {
    func loadRecords() throws -> [BookmarkRecord] {
        []
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {}
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
