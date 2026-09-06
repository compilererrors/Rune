import Foundation
import XCTest
@testable import RuneCore
@testable import RuneUI

final class RuneResourceSortingTests: XCTestCase {
    @MainActor
    func testResourceNamesUseNaturalNumericOrder() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.setPods([10, 2, 1].map { PodSummary(name: "sample-\($0)", namespace: "alpha", status: "Running") })
        XCTAssertEqual(viewModel.visiblePods.map(\.name), ["sample-1", "sample-2", "sample-10"])
        viewModel.togglePodSort(.name)
        XCTAssertEqual(viewModel.visiblePods.map(\.name), ["sample-10", "sample-2", "sample-1"])
    }

    @MainActor
    func testEventDatesSortChronologicallyAcrossOffsetsAndKeepUnknownDatesLast() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.setEvents([
            ("later", "2026-01-01T09:30:00.250Z"),
            ("earlier", "2026-01-01T10:00:00+01:00"),
            ("missing", nil),
            ("invalid", "unknown")
        ].map { name, timestamp in
            EventSummary(type: "Normal", reason: name, objectName: "sample", message: "Synthetic event", lastTimestamp: timestamp)
        })
        viewModel.toggleEventSort(.lastSeen)
        XCTAssertEqual(viewModel.visibleEvents.map(\.reason), ["later", "earlier", "invalid", "missing"])
        viewModel.toggleEventSort(.lastSeen)
        XCTAssertEqual(viewModel.visibleEvents.map(\.reason), ["earlier", "later", "invalid", "missing"])
    }

    @MainActor
    func testResourceComparatorsAreIrreflexiveInBothDirections() {
        let viewModel = RuneAppViewModel(state: RuneAppState())
        let pod = PodSummary(name: "sample", namespace: "alpha", status: "Running")
        let deployment = DeploymentSummary(
            name: "sample",
            namespace: "alpha",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        let service = ServiceSummary(
            name: "sample",
            namespace: "alpha",
            type: "ClusterIP",
            clusterIP: "10.0.0.1"
        )
        let generic = ClusterResourceSummary(
            kind: .configMap,
            name: "sample",
            namespace: "alpha",
            primaryText: "1 key",
            secondaryText: "Ready"
        )
        let helm = HelmReleaseSummary(
            name: "sample",
            namespace: "alpha",
            revision: 1,
            updated: "2026-01-01T00:00:00Z",
            status: "deployed",
            chart: "sample-1.0.0",
            appVersion: "1.0.0"
        )
        let event = EventSummary(
            type: "Normal",
            reason: "Started",
            objectName: "sample",
            message: "Synthetic event",
            lastTimestamp: "2026-01-01T00:00:00Z",
            involvedKind: "Pod",
            involvedNamespace: "alpha"
        )
        let operatorResource = OperatorResourceSummary(
            family: "Sample",
            kind: "Widgets",
            apiPath: "/apis/sample.invalid/v1/namespaces/alpha/widgets",
            name: "sample",
            namespace: "alpha",
            status: "Ready",
            message: ""
        )

        assertComparatorsAreIrreflexive(
            viewModel: viewModel,
            pod: pod,
            deployment: deployment,
            service: service,
            generic: generic,
            helm: helm,
            event: event,
            operatorResource: operatorResource
        )

        viewModel.togglePodSort(.name)
        viewModel.toggleDeploymentSort(.name)
        viewModel.toggleServiceSort(.name)
        viewModel.toggleGenericResourceSort(.name)
        viewModel.toggleHelmReleaseSort(.name)
        viewModel.toggleEventSort(.reason)
        viewModel.toggleOperatorResourceSort(.family)

        assertComparatorsAreIrreflexive(
            viewModel: viewModel,
            pod: pod,
            deployment: deployment,
            service: service,
            generic: generic,
            helm: helm,
            event: event,
            operatorResource: operatorResource
        )
    }

    @MainActor
    func testDuplicateNamesUseDeterministicNamespaceTieBreakInBothDirections() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)

        state.setPods([
            PodSummary(name: "shared", namespace: "zeta", status: "Running"),
            PodSummary(name: "bravo", namespace: "middle", status: "Running"),
            PodSummary(name: "shared", namespace: "alpha", status: "Running")
        ])
        state.setDeployments([
            DeploymentSummary(name: "shared", namespace: "zeta", readyReplicas: 1, desiredReplicas: 1),
            DeploymentSummary(name: "bravo", namespace: "middle", readyReplicas: 1, desiredReplicas: 1),
            DeploymentSummary(name: "shared", namespace: "alpha", readyReplicas: 1, desiredReplicas: 1)
        ])
        state.setServices([
            ServiceSummary(name: "shared", namespace: "zeta", type: "ClusterIP", clusterIP: "10.0.0.3"),
            ServiceSummary(name: "bravo", namespace: "middle", type: "ClusterIP", clusterIP: "10.0.0.2"),
            ServiceSummary(name: "shared", namespace: "alpha", type: "ClusterIP", clusterIP: "10.0.0.1")
        ])
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "shared", namespace: "zeta", primaryText: "1 key", secondaryText: "Ready"),
            ClusterResourceSummary(kind: .configMap, name: "bravo", namespace: "middle", primaryText: "1 key", secondaryText: "Ready"),
            ClusterResourceSummary(kind: .configMap, name: "shared", namespace: "alpha", primaryText: "1 key", secondaryText: "Ready")
        ])
        state.setHelmReleases([
            helmRelease(name: "shared", namespace: "zeta"),
            helmRelease(name: "bravo", namespace: "middle"),
            helmRelease(name: "shared", namespace: "alpha")
        ])
        state.setEvents([
            event(reason: "Same", namespace: "zeta", objectName: "shared"),
            event(reason: "Alpha", namespace: "middle", objectName: "bravo"),
            event(reason: "Same", namespace: "alpha", objectName: "shared")
        ])
        state.setOperatorResources([
            operatorResource(family: "Zulu", namespace: "zeta", name: "shared"),
            operatorResource(family: "Alpha", namespace: "middle", name: "bravo"),
            operatorResource(family: "Zulu", namespace: "alpha", name: "shared")
        ])

        XCTAssertEqual(viewModel.visiblePods.map(\.id), ["middle/bravo", "alpha/shared", "zeta/shared"])
        XCTAssertEqual(viewModel.visibleDeployments.map(\.id), ["middle/bravo", "alpha/shared", "zeta/shared"])
        XCTAssertEqual(viewModel.visibleServices.map(\.id), ["middle/bravo", "alpha/shared", "zeta/shared"])
        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.id), ["configMap|middle|bravo", "configMap|alpha|shared", "configMap|zeta|shared"])
        XCTAssertEqual(viewModel.visibleHelmReleases.map(\.id), ["middle/bravo", "alpha/shared", "zeta/shared"])
        XCTAssertEqual(eventKeys(viewModel.visibleEvents), ["middle/Alpha/bravo", "alpha/Same/shared", "zeta/Same/shared"])
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.id), [
            "Alpha|Widgets|middle|bravo",
            "Zulu|Widgets|alpha|shared",
            "Zulu|Widgets|zeta|shared"
        ])

        viewModel.togglePodSort(.name)
        viewModel.toggleDeploymentSort(.name)
        viewModel.toggleServiceSort(.name)
        viewModel.toggleGenericResourceSort(.name)
        viewModel.toggleHelmReleaseSort(.name)
        viewModel.toggleEventSort(.reason)
        viewModel.toggleOperatorResourceSort(.family)

        XCTAssertEqual(viewModel.visiblePods.map(\.id), ["alpha/shared", "zeta/shared", "middle/bravo"])
        XCTAssertEqual(viewModel.visibleDeployments.map(\.id), ["alpha/shared", "zeta/shared", "middle/bravo"])
        XCTAssertEqual(viewModel.visibleServices.map(\.id), ["alpha/shared", "zeta/shared", "middle/bravo"])
        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.id), ["configMap|alpha|shared", "configMap|zeta|shared", "configMap|middle|bravo"])
        XCTAssertEqual(viewModel.visibleHelmReleases.map(\.id), ["alpha/shared", "zeta/shared", "middle/bravo"])
        XCTAssertEqual(eventKeys(viewModel.visibleEvents), ["alpha/Same/shared", "zeta/Same/shared", "middle/Alpha/bravo"])
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.id), [
            "Zulu|Widgets|alpha|shared",
            "Zulu|Widgets|zeta|shared",
            "Alpha|Widgets|middle|bravo"
        ])
    }

    @MainActor
    func testEquivalentPodSnapshotsKeepInputOrderAcrossSortDirections() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.setPods([
            PodSummary(name: "same", namespace: "alpha", status: "Second snapshot"),
            PodSummary(name: "same", namespace: "alpha", status: "First snapshot")
        ])

        XCTAssertEqual(viewModel.visiblePods.map(\.status), ["Second snapshot", "First snapshot"])
        XCTAssertEqual(viewModel.visiblePods.map(\.status), ["Second snapshot", "First snapshot"])

        viewModel.togglePodSort(.name)

        XCTAssertEqual(viewModel.visiblePods.map(\.status), ["Second snapshot", "First snapshot"])
        XCTAssertEqual(viewModel.visiblePods.map(\.status), ["Second snapshot", "First snapshot"])
    }

    @MainActor
    func testFavoritesRemainFirstDuringDescendingSorts() {
        let suiteName = "RuneResourceSortingTests.favorites.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-sort-context")
        let viewModel = RuneAppViewModel(
            state: state,
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: defaults)
        )

        state.setPods([
            PodSummary(name: "alpha", namespace: "default", status: "Running"),
            PodSummary(name: "zulu", namespace: "default", status: "Running")
        ])
        state.setDeployments([
            DeploymentSummary(name: "alpha", namespace: "default", readyReplicas: 1, desiredReplicas: 1),
            DeploymentSummary(name: "zulu", namespace: "default", readyReplicas: 1, desiredReplicas: 1)
        ])
        state.setServices([
            ServiceSummary(name: "alpha", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.1"),
            ServiceSummary(name: "zulu", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.2")
        ])
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "alpha", namespace: "default", primaryText: "1 key", secondaryText: "Ready"),
            ClusterResourceSummary(kind: .configMap, name: "zulu", namespace: "default", primaryText: "1 key", secondaryText: "Ready")
        ])
        state.setOperatorResources([
            operatorResource(family: "Alpha", namespace: "default", name: "alpha"),
            operatorResource(family: "Zulu", namespace: "default", name: "zulu")
        ])

        viewModel.toggleFavoriteResource(kind: .pod, namespace: "default", name: "alpha")
        viewModel.toggleFavoriteResource(kind: .deployment, namespace: "default", name: "alpha")
        viewModel.toggleFavoriteResource(kind: .service, namespace: "default", name: "alpha")
        viewModel.toggleFavoriteResource(kind: .configMap, namespace: "default", name: "alpha")
        viewModel.toggleFavoriteOperatorResource(state.operatorResources[0])
        viewModel.togglePodSort(.name)
        viewModel.toggleDeploymentSort(.name)
        viewModel.toggleServiceSort(.name)
        viewModel.toggleGenericResourceSort(.name)
        viewModel.toggleOperatorResourceSort(.family)

        XCTAssertEqual(viewModel.visiblePods.first?.name, "alpha")
        XCTAssertEqual(viewModel.visibleDeployments.first?.name, "alpha")
        XCTAssertEqual(viewModel.visibleServices.first?.name, "alpha")
        XCTAssertEqual(viewModel.visibleConfigMaps.first?.name, "alpha")
        XCTAssertEqual(viewModel.visibleOperatorResources.first?.name, "alpha")
    }

    @MainActor
    private func assertComparatorsAreIrreflexive(
        viewModel: RuneAppViewModel,
        pod: PodSummary,
        deployment: DeploymentSummary,
        service: ServiceSummary,
        generic: ClusterResourceSummary,
        helm: HelmReleaseSummary,
        event: EventSummary,
        operatorResource: OperatorResourceSummary,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertFalse(viewModel.podComparator(pod, pod), file: file, line: line)
        XCTAssertFalse(viewModel.deploymentComparator(deployment, deployment), file: file, line: line)
        XCTAssertFalse(viewModel.serviceComparator(service, service), file: file, line: line)
        XCTAssertFalse(viewModel.genericResourceComparator(generic, generic), file: file, line: line)
        XCTAssertFalse(viewModel.helmReleaseComparator(helm, helm), file: file, line: line)
        XCTAssertFalse(viewModel.eventComparator(event, event), file: file, line: line)
        XCTAssertFalse(viewModel.operatorResourceComparator(operatorResource, operatorResource), file: file, line: line)
    }

    private func helmRelease(name: String, namespace: String) -> HelmReleaseSummary {
        HelmReleaseSummary(
            name: name,
            namespace: namespace,
            revision: 1,
            updated: "2026-01-01T00:00:00Z",
            status: "deployed",
            chart: "sample-1.0.0",
            appVersion: "1.0.0"
        )
    }

    private func event(reason: String, namespace: String, objectName: String) -> EventSummary {
        EventSummary(
            type: "Normal",
            reason: reason,
            objectName: objectName,
            message: "Synthetic event",
            lastTimestamp: "2026-01-01T00:00:00Z",
            involvedKind: "Pod",
            involvedNamespace: namespace
        )
    }

    private func operatorResource(family: String, namespace: String, name: String) -> OperatorResourceSummary {
        OperatorResourceSummary(
            family: family,
            kind: "Widgets",
            apiPath: "/apis/sample.invalid/v1/namespaces/\(namespace)/widgets",
            name: name,
            namespace: namespace,
            status: "Ready",
            message: ""
        )
    }

    private func eventKeys(_ events: [EventSummary]) -> [String] {
        events.map { "\($0.involvedNamespace ?? "")/\($0.reason)/\($0.objectName)" }
    }
}
