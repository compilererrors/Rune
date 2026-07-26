import XCTest
@testable import RuneCore

final class RuneSelectionReconciliationTests: XCTestCase {
    @MainActor
    func testSelectionRevisionIsScopedAndTracksIdentityChangesButNotSameIDRefreshes() {
        let state = RuneAppState()
        let first = PodSummary(name: "pod-a", namespace: "selection-test", status: "Pending")
        let refreshedFirst = PodSummary(name: "pod-a", namespace: "selection-test", status: "Running")
        let second = PodSummary(name: "pod-b", namespace: "selection-test", status: "Running")

        state.setPods([first, second])
        let podChannel: RuneResourceSelectionChannel = .resource(.pod)
        let initialSelectionRevision = state.resourceSelectionRevision(for: podChannel)

        state.setPods([refreshedFirst, second])
        XCTAssertEqual(state.selectedPod, refreshedFirst)
        XCTAssertEqual(
            state.resourceSelectionRevision(for: podChannel),
            initialSelectionRevision,
            "Refreshing the selected model with the same stable ID must not create a new selection revision."
        )

        let service = ServiceSummary(
            name: "service-a",
            namespace: "selection-test",
            type: "ClusterIP",
            clusterIP: "192.0.2.1"
        )
        state.setServices([service])
        XCTAssertEqual(
            state.resourceSelectionRevision(for: podChannel),
            initialSelectionRevision,
            "A background selection change in another resource family must not supersede a pending pod click."
        )
        XCTAssertEqual(state.resourceSelectionRevision(for: .resource(.service)), 1)

        state.setSelectedPod(second)
        XCTAssertEqual(state.resourceSelectionRevision(for: podChannel), initialSelectionRevision + 1)

        state.selectedPod = refreshedFirst
        XCTAssertEqual(
            state.resourceSelectionRevision(for: podChannel),
            initialSelectionRevision + 2,
            "Direct selection mutations must carry a newer revision for AppKit projections."
        )
    }

    @MainActor
    func testTypedSelectionsUseLatestModelsWithTheSameID() {
        let state = RuneAppState()

        let firstDeployment = DeploymentSummary(
            name: "first",
            namespace: "selection-test",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        let oldDeployment = DeploymentSummary(
            name: "selected",
            namespace: "selection-test",
            readyReplicas: 0,
            desiredReplicas: 2
        )
        let refreshedDeployment = DeploymentSummary(
            name: "selected",
            namespace: "selection-test",
            readyReplicas: 2,
            desiredReplicas: 2
        )
        state.setDeployments([firstDeployment, oldDeployment])
        state.setSelectedDeployment(oldDeployment)
        state.setDeployments([firstDeployment, refreshedDeployment])
        XCTAssertEqual(state.selectedDeployment, refreshedDeployment)

        let firstService = ServiceSummary(
            name: "first",
            namespace: "selection-test",
            type: "ClusterIP",
            clusterIP: "192.0.2.1"
        )
        let oldService = ServiceSummary(
            name: "selected",
            namespace: "selection-test",
            type: "ClusterIP",
            clusterIP: "192.0.2.2"
        )
        let refreshedService = ServiceSummary(
            name: "selected",
            namespace: "selection-test",
            type: "LoadBalancer",
            clusterIP: "192.0.2.3"
        )
        state.setServices([firstService, oldService])
        state.setSelectedService(oldService)
        state.setServices([firstService, refreshedService])
        XCTAssertEqual(state.selectedService, refreshedService)

        let firstEvent = EventSummary(
            eventIdentifier: "first-event",
            type: "Normal",
            reason: "Started",
            objectName: "first",
            message: "Started",
            involvedKind: "Pod",
            involvedNamespace: "selection-test"
        )
        let oldEvent = EventSummary(
            eventIdentifier: "selected-event",
            type: "Normal",
            reason: "Scheduled",
            objectName: "selected",
            message: "Scheduled",
            lastTimestamp: "2026-01-01T00:00:00Z",
            involvedKind: "Pod",
            involvedNamespace: "selection-test"
        )
        let refreshedEvent = EventSummary(
            eventIdentifier: "selected-event",
            type: "Warning",
            reason: "BackOff",
            objectName: "selected",
            message: "Retrying",
            lastTimestamp: "2026-01-01T00:01:00Z",
            involvedKind: "Pod",
            involvedNamespace: "selection-test"
        )
        state.setEvents([firstEvent, oldEvent])
        state.setSelectedEvent(oldEvent)
        state.setEvents([firstEvent, refreshedEvent])
        XCTAssertEqual(state.selectedEvent, refreshedEvent)

        let firstRelease = HelmReleaseSummary(
            name: "first",
            namespace: "selection-test",
            revision: 1,
            updated: "earlier",
            status: "deployed",
            chart: "first-1.0.0",
            appVersion: "1.0.0"
        )
        let oldRelease = HelmReleaseSummary(
            name: "selected",
            namespace: "selection-test",
            revision: 1,
            updated: "earlier",
            status: "pending-upgrade",
            chart: "selected-1.0.0",
            appVersion: "1.0.0"
        )
        let refreshedRelease = HelmReleaseSummary(
            name: "selected",
            namespace: "selection-test",
            revision: 2,
            updated: "later",
            status: "deployed",
            chart: "selected-2.0.0",
            appVersion: "2.0.0"
        )
        state.setHelmReleases([firstRelease, oldRelease])
        state.setSelectedHelmRelease(oldRelease)
        state.setHelmReleases([firstRelease, refreshedRelease])
        XCTAssertEqual(state.selectedHelmRelease, refreshedRelease)

        let firstOperatorResource = OperatorResourceSummary(
            family: "Synthetic",
            kind: "Widgets",
            apiPath: "/apis/synthetic.invalid/v1/namespaces/selection-test/widgets",
            name: "first",
            namespace: "selection-test",
            status: "Ready",
            message: "Ready"
        )
        let oldOperatorResource = OperatorResourceSummary(
            family: "Synthetic",
            kind: "Widgets",
            apiPath: "/apis/synthetic.invalid/v1/namespaces/selection-test/widgets",
            name: "selected",
            namespace: "selection-test",
            status: "Progressing",
            message: "Waiting"
        )
        let refreshedOperatorResource = OperatorResourceSummary(
            family: "Synthetic",
            kind: "Widgets",
            apiPath: "/apis/synthetic.invalid/v1/namespaces/selection-test/widgets",
            name: "selected",
            namespace: "selection-test",
            status: "Ready",
            message: "Reconciled"
        )
        state.setOperatorResources([firstOperatorResource, oldOperatorResource])
        state.setSelectedOperatorResource(oldOperatorResource)
        state.setOperatorResources([firstOperatorResource, refreshedOperatorResource])
        XCTAssertEqual(state.selectedOperatorResource, refreshedOperatorResource)
    }

    @MainActor
    func testMissingTypedSelectionUsesTheFamilyFallback() {
        let state = RuneAppState()
        let selectedDeployment = DeploymentSummary(
            name: "selected",
            namespace: "selection-test",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        let remainingDeployment = DeploymentSummary(
            name: "remaining",
            namespace: "selection-test",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        state.setDeployments([selectedDeployment])
        state.setSelectedDeployment(selectedDeployment)
        state.setDeployments([remainingDeployment])
        XCTAssertEqual(state.selectedDeployment, remainingDeployment)

        let selectedOperatorResource = OperatorResourceSummary(
            family: "Synthetic",
            kind: "Widgets",
            apiPath: "/apis/synthetic.invalid/v1/namespaces/selection-test/widgets",
            name: "selected",
            namespace: "selection-test",
            status: "Ready",
            message: "Ready"
        )
        let remainingOperatorResource = OperatorResourceSummary(
            family: "Synthetic",
            kind: "Widgets",
            apiPath: "/apis/synthetic.invalid/v1/namespaces/selection-test/widgets",
            name: "remaining",
            namespace: "selection-test",
            status: "Ready",
            message: "Ready"
        )
        state.setOperatorResources([selectedOperatorResource])
        state.setSelectedOperatorResource(selectedOperatorResource)
        state.setOperatorResources([remainingOperatorResource])
        XCTAssertNil(state.selectedOperatorResource)
    }

    @MainActor
    func testEveryGenericSelectionUsesLatestModelWithTheSameID() {
        let kinds: [KubeResourceKind] = [
            .statefulSet,
            .daemonSet,
            .job,
            .cronJob,
            .replicaSet,
            .persistentVolumeClaim,
            .persistentVolume,
            .storageClass,
            .horizontalPodAutoscaler,
            .networkPolicy,
            .endpoint,
            .ingress,
            .configMap,
            .secret,
            .node
        ]

        for kind in kinds {
            let state = RuneAppState()
            let namespace = kind.isNamespaced ? "selection-test" : nil
            let first = ClusterResourceSummary(
                kind: kind,
                name: "first",
                namespace: namespace,
                primaryText: "unchanged",
                secondaryText: "first"
            )
            let oldSelection = ClusterResourceSummary(
                kind: kind,
                name: "selected",
                namespace: namespace,
                primaryText: "old",
                secondaryText: "before"
            )
            let refreshedSelection = ClusterResourceSummary(
                kind: kind,
                name: "selected",
                namespace: namespace,
                primaryText: "updated",
                secondaryText: "after"
            )

            setGenericResources([first, oldSelection], kind: kind, state: state)
            setGenericSelection(oldSelection, kind: kind, state: state)
            setGenericResources([first, refreshedSelection], kind: kind, state: state)

            XCTAssertEqual(
                genericSelection(kind: kind, state: state),
                refreshedSelection,
                "Expected \(kind.rawValue) selection to use the refreshed row"
            )
        }
    }

    @MainActor
    func testGenericBulkSelectionPrunesOnlyIDsMissingFromTheWholeResourceUniverse() {
        let state = RuneAppState()
        let selectedConfigMap = ClusterResourceSummary(
            kind: .configMap,
            name: "selected-config",
            namespace: "selection-test",
            primaryText: "1 key",
            secondaryText: "old"
        )
        let replacementConfigMap = ClusterResourceSummary(
            kind: .configMap,
            name: "replacement-config",
            namespace: "selection-test",
            primaryText: "1 key",
            secondaryText: "new"
        )
        let selectedSecret = ClusterResourceSummary(
            kind: .secret,
            name: "selected-secret",
            namespace: "selection-test",
            primaryText: "Opaque",
            secondaryText: "1 key"
        )
        let selectedRole = ClusterResourceSummary(
            kind: .role,
            name: "selected-role",
            namespace: "selection-test",
            primaryText: "1 rule",
            secondaryText: "Namespaced"
        )

        state.setConfigMaps([selectedConfigMap])
        state.setSecrets([selectedSecret])
        state.setRBACData(
            roles: [selectedRole],
            serviceAccounts: [],
            roleBindings: [],
            clusterRoles: [],
            clusterRoleBindings: []
        )
        state.setSelectedGenericResourceIDs(
            [selectedConfigMap.id, selectedSecret.id, selectedRole.id],
            validIDs: [selectedConfigMap.id, selectedSecret.id, selectedRole.id]
        )

        state.setConfigMaps([replacementConfigMap])

        XCTAssertEqual(
            state.selectedGenericResourceIDs,
            [selectedSecret.id, selectedRole.id],
            "Refreshing one family must retain valid selections from other families"
        )

        state.setRBACData(
            roles: [],
            serviceAccounts: [],
            roleBindings: [],
            clusterRoles: [],
            clusterRoleBindings: []
        )

        XCTAssertEqual(state.selectedGenericResourceIDs, [selectedSecret.id])

        state.setSecrets([])

        XCTAssertTrue(state.selectedGenericResourceIDs.isEmpty)
    }

    @MainActor
    private func setGenericResources(
        _ resources: [ClusterResourceSummary],
        kind: KubeResourceKind,
        state: RuneAppState
    ) {
        switch kind {
        case .statefulSet: state.setStatefulSets(resources)
        case .daemonSet: state.setDaemonSets(resources)
        case .job: state.setJobs(resources)
        case .cronJob: state.setCronJobs(resources)
        case .replicaSet: state.setReplicaSets(resources)
        case .persistentVolumeClaim: state.setPersistentVolumeClaims(resources)
        case .persistentVolume: state.setPersistentVolumes(resources)
        case .storageClass: state.setStorageClasses(resources)
        case .horizontalPodAutoscaler: state.setHorizontalPodAutoscalers(resources)
        case .networkPolicy: state.setNetworkPolicies(resources)
        case .endpoint: state.setEndpoints(resources)
        case .ingress: state.setIngresses(resources)
        case .configMap: state.setConfigMaps(resources)
        case .secret: state.setSecrets(resources)
        case .node: state.setNodes(resources)
        default: XCTFail("Unsupported generic resource kind: \(kind.rawValue)")
        }
    }

    @MainActor
    private func setGenericSelection(
        _ resource: ClusterResourceSummary,
        kind: KubeResourceKind,
        state: RuneAppState
    ) {
        switch kind {
        case .statefulSet: state.setSelectedStatefulSet(resource)
        case .daemonSet: state.setSelectedDaemonSet(resource)
        case .job: state.setSelectedJob(resource)
        case .cronJob: state.setSelectedCronJob(resource)
        case .replicaSet: state.setSelectedReplicaSet(resource)
        case .persistentVolumeClaim: state.setSelectedPersistentVolumeClaim(resource)
        case .persistentVolume: state.setSelectedPersistentVolume(resource)
        case .storageClass: state.setSelectedStorageClass(resource)
        case .horizontalPodAutoscaler: state.setSelectedHorizontalPodAutoscaler(resource)
        case .networkPolicy: state.setSelectedNetworkPolicy(resource)
        case .endpoint: state.setSelectedEndpoint(resource)
        case .ingress: state.setSelectedIngress(resource)
        case .configMap: state.setSelectedConfigMap(resource)
        case .secret: state.setSelectedSecret(resource)
        case .node: state.setSelectedNode(resource)
        default: XCTFail("Unsupported generic resource kind: \(kind.rawValue)")
        }
    }

    @MainActor
    private func genericSelection(
        kind: KubeResourceKind,
        state: RuneAppState
    ) -> ClusterResourceSummary? {
        switch kind {
        case .statefulSet: return state.selectedStatefulSet
        case .daemonSet: return state.selectedDaemonSet
        case .job: return state.selectedJob
        case .cronJob: return state.selectedCronJob
        case .replicaSet: return state.selectedReplicaSet
        case .persistentVolumeClaim: return state.selectedPersistentVolumeClaim
        case .persistentVolume: return state.selectedPersistentVolume
        case .storageClass: return state.selectedStorageClass
        case .horizontalPodAutoscaler: return state.selectedHorizontalPodAutoscaler
        case .networkPolicy: return state.selectedNetworkPolicy
        case .endpoint: return state.selectedEndpoint
        case .ingress: return state.selectedIngress
        case .configMap: return state.selectedConfigMap
        case .secret: return state.selectedSecret
        case .node: return state.selectedNode
        default:
            XCTFail("Unsupported generic resource kind: \(kind.rawValue)")
            return nil
        }
    }
}
