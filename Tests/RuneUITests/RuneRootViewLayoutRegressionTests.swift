import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class RuneRootViewLayoutRegressionTests: XCTestCase {
    func testWorkloadPodDescribeTabRemainsTopAligned() async throws {
        let pod = PodSummary(name: "orders-api-7c9db", namespace: "example-backend", status: "Running", totalRestarts: 1, ageDescription: "5m")
        let baseline = try await hostSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview
                )
            },
            section: .workloads,
            kind: .pod
        )
        let describe = try await hostSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .describe
                )
            },
            section: .workloads,
            kind: .pod
        )

        assertAligned(baseline: baseline, candidate: describe)
    }

    func testWorkloadPodYAMLTabRemainsTopAligned() async throws {
        let pod = PodSummary(name: "orders-api-7c9db", namespace: "example-backend", status: "Running", totalRestarts: 1, ageDescription: "5m")
        let baseline = try await hostSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .overview
                )
            },
            section: .workloads,
            kind: .pod
        )
        let yaml = try await hostSnapshot(
            viewModel: makePodViewModel(pod: pod),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialPodInspectorTab: .yaml
                )
            },
            section: .workloads,
            kind: .pod
        )

        assertAligned(baseline: baseline, candidate: yaml)
    }

    func testWorkloadDeploymentDescribeTabRemainsTopAligned() async throws {
        let deployment = DeploymentSummary(name: "orders-api", namespace: "example-backend", readyReplicas: 2, desiredReplicas: 2)
        let baseline = try await hostSnapshot(
            viewModel: makeDeploymentViewModel(deployment: deployment),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialDeploymentInspectorTab: .overview
                )
            },
            section: .workloads,
            kind: .deployment
        )
        let describe = try await hostSnapshot(
            viewModel: makeDeploymentViewModel(deployment: deployment),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialDeploymentInspectorTab: .describe
                )
            },
            section: .workloads,
            kind: .deployment
        )

        assertAligned(baseline: baseline, candidate: describe)
    }

    func testWorkloadDeploymentYAMLTabRemainsTopAligned() async throws {
        let deployment = DeploymentSummary(name: "orders-api", namespace: "example-backend", readyReplicas: 2, desiredReplicas: 2)
        let baseline = try await hostSnapshot(
            viewModel: makeDeploymentViewModel(deployment: deployment),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialDeploymentInspectorTab: .overview
                )
            },
            section: .workloads,
            kind: .deployment
        )
        let yaml = try await hostSnapshot(
            viewModel: makeDeploymentViewModel(deployment: deployment),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialDeploymentInspectorTab: .yaml
                )
            },
            section: .workloads,
            kind: .deployment
        )

        assertAligned(baseline: baseline, candidate: yaml)
    }

    func testNetworkingDescribeTabRemainsTopAligned() async throws {
        let service = ServiceSummary(name: "orders-api", namespace: "example-backend", type: "ClusterIP", clusterIP: "10.0.0.10")
        let baseline = try await hostSnapshot(
            viewModel: makeServiceViewModel(service: service),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialServiceInspectorTab: .overview
                )
            },
            section: .networking,
            kind: .service
        )
        let describe = try await hostSnapshot(
            viewModel: makeServiceViewModel(service: service),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialServiceInspectorTab: .describe
                )
            },
            section: .networking,
            kind: .service
        )

        assertAligned(baseline: baseline, candidate: describe)
    }

    func testNetworkingYAMLTabRemainsTopAligned() async throws {
        let service = ServiceSummary(name: "orders-api", namespace: "example-backend", type: "ClusterIP", clusterIP: "10.0.0.10")
        let baseline = try await hostSnapshot(
            viewModel: makeServiceViewModel(service: service),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialServiceInspectorTab: .overview
                )
            },
            section: .networking,
            kind: .service
        )
        let yaml = try await hostSnapshot(
            viewModel: makeServiceViewModel(service: service),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialServiceInspectorTab: .yaml
                )
            },
            section: .networking,
            kind: .service
        )

        assertAligned(baseline: baseline, candidate: yaml)
    }

    func testConfigYAMLAndDescribeRemainTopAligned() async throws {
        let resource = ClusterResourceSummary(
            kind: .configMap,
            name: "orders-config",
            namespace: "example-backend",
            primaryText: "ConfigMap",
            secondaryText: "12 keys"
        )
        let yaml = try await hostSnapshot(
            viewModel: makeConfigViewModel(resource: resource),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialGenericResourceManifestTab: .yaml
                )
            },
            section: .config,
            kind: .configMap
        )
        let describe = try await hostSnapshot(
            viewModel: makeConfigViewModel(resource: resource),
            rootView: { viewModel, capture in
                RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: capture,
                    debugDisableBootstrap: true,
                    initialGenericResourceManifestTab: .describe
                )
            },
            section: .config,
            kind: .configMap
        )

        assertAligned(baseline: yaml, candidate: describe)
    }

    func testConfigAndWorkloadsRemainTopAligned() async throws {
        let viewModel = RuneAppViewModel()
        var snapshots: [RuneRootLayoutSnapshot] = []

        let host = NSHostingController(
            rootView: RuneRootView(viewModel: viewModel) { snapshot in
                snapshots.append(snapshot)
            }
        )

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        viewModel.state.selectedSection = .workloads
        viewModel.state.selectedWorkloadKind = .pod
        let workloadsSnapshot = try await waitForSnapshot(in: window, snapshots: { snapshots }) {
            $0.section == .workloads
                && $0.workloadKind == .pod
                && $0.contentMinY != nil
                && $0.headerMinY != nil
                && $0.detailMinY != nil
        }

        viewModel.state.selectedSection = .config
        viewModel.state.selectedWorkloadKind = .configMap
        let configSnapshot = try await waitForSnapshot(in: window, snapshots: { snapshots }) {
            $0.section == .config
                && $0.workloadKind == .configMap
                && $0.contentMinY != nil
                && $0.headerMinY != nil
                && $0.detailMinY != nil
        }

        XCTAssertEqual(
            workloadsSnapshot.resolvedWindowTopInset,
            configSnapshot.resolvedWindowTopInset,
            accuracy: 0.5
        )
        XCTAssertEqual(workloadsSnapshot.contentMinY ?? 0, configSnapshot.contentMinY ?? 0, accuracy: 1.5)
        XCTAssertEqual(workloadsSnapshot.headerMinY ?? 0, configSnapshot.headerMinY ?? 0, accuracy: 1.5)
        XCTAssertEqual(workloadsSnapshot.detailMinY ?? 0, configSnapshot.detailMinY ?? 0, accuracy: 1.5)
    }

    func testSectionTransitionsDoNotDriftAfterLayoutSettles() async throws {
        let viewModel = RuneAppViewModel()
        var snapshots: [RuneRootLayoutSnapshot] = []

        let host = NSHostingController(
            rootView: RuneRootView(viewModel: viewModel) { snapshot in
                snapshots.append(snapshot)
            }
        )

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await assertStableTransition(
            in: window,
            snapshots: { snapshots },
            viewModel: viewModel,
            section: .workloads,
            kind: .pod
        )
        try await assertStableTransition(
            in: window,
            snapshots: { snapshots },
            viewModel: viewModel,
            section: .config,
            kind: .configMap
        )
        try await assertStableTransition(
            in: window,
            snapshots: { snapshots },
            viewModel: viewModel,
            section: .rbac,
            kind: .role
        )
        try await assertStableTransition(
            in: window,
            snapshots: { snapshots },
            viewModel: viewModel,
            section: .workloads,
            kind: .pod
        )
    }

    private func waitForSnapshot(
        in window: NSWindow,
        snapshots: @escaping () -> [RuneRootLayoutSnapshot],
        matching predicate: (RuneRootLayoutSnapshot) -> Bool
    ) async throws -> RuneRootLayoutSnapshot {
        let timeout = Date().addingTimeInterval(2.0)
        while Date() < timeout {
            window.contentView?.layoutSubtreeIfNeeded()
            await Task.yield()

            if let snapshot = snapshots().last(where: predicate) {
                return snapshot
            }

            try await Task.sleep(nanoseconds: 20_000_000)
        }

        XCTFail("Timed out waiting for layout snapshot")
        throw CancellationError()
    }

    private func assertStableTransition(
        in window: NSWindow,
        snapshots: @escaping () -> [RuneRootLayoutSnapshot],
        viewModel: RuneAppViewModel,
        section: RuneSection,
        kind: KubeResourceKind
    ) async throws {
        viewModel.state.selectedSection = section
        viewModel.state.selectedWorkloadKind = kind

        let settled = try await waitForSnapshot(in: window, snapshots: snapshots) {
            $0.section == section
                && $0.workloadKind == kind
                && $0.contentMinY != nil
                && $0.headerMinY != nil
                && $0.detailMinY != nil
        }

        let settledSnapshotCount = snapshots().count
        let baselineContent = settled.contentMinY ?? 0
        let baselineDetail = settled.detailMinY ?? 0
        let timeout = Date().addingTimeInterval(0.35)

        while Date() < timeout {
            window.contentView?.layoutSubtreeIfNeeded()
            await Task.yield()

            let recentSnapshots = snapshots().dropFirst(settledSnapshotCount).filter {
                $0.section == section
                    && $0.workloadKind == kind
                    && $0.contentMinY != nil
                    && $0.headerMinY != nil
                    && $0.detailMinY != nil
            }

            for snapshot in recentSnapshots.suffix(6) {
                XCTAssertEqual(snapshot.contentMinY ?? 0, baselineContent, accuracy: 2.0)
                XCTAssertEqual(snapshot.detailMinY ?? 0, baselineDetail, accuracy: 2.0)
                XCTAssertGreaterThanOrEqual((snapshot.headerMinY ?? 0) - (snapshot.contentMinY ?? 0), 12.0)
                XCTAssertLessThanOrEqual((snapshot.headerMinY ?? 0) - (snapshot.contentMinY ?? 0), 64.0)
            }

            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    private func makeServiceViewModel(service: ServiceSummary) -> RuneAppViewModel {
        let state = RuneAppState()
        state.selectedSection = .networking
        state.selectedWorkloadKind = .service
        state.selectedNamespace = service.namespace
        state.setServices([service])
        state.selectedService = service
        state.setResourceYAML(sampleYAML(named: service.name))
        state.setResourceDescribe(sampleDescribe(named: service.name))
        return RuneAppViewModel(state: state)
    }

    private func makePodViewModel(pod: PodSummary) -> RuneAppViewModel {
        let state = RuneAppState()
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedNamespace = pod.namespace
        state.setPods([pod])
        state.selectedPod = pod
        state.setResourceYAML(sampleYAML(named: pod.name))
        state.setResourceDescribe(sampleDescribe(named: pod.name))
        return RuneAppViewModel(state: state)
    }

    private func makeDeploymentViewModel(deployment: DeploymentSummary) -> RuneAppViewModel {
        let state = RuneAppState()
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .deployment
        state.selectedNamespace = deployment.namespace
        state.setDeployments([deployment])
        state.selectedDeployment = deployment
        state.setResourceYAML(sampleYAML(named: deployment.name))
        state.setResourceDescribe(sampleDescribe(named: deployment.name))
        return RuneAppViewModel(state: state)
    }

    private func makeConfigViewModel(resource: ClusterResourceSummary) -> RuneAppViewModel {
        let state = RuneAppState()
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.selectedNamespace = resource.namespace ?? "default"
        state.setConfigMaps([resource])
        state.selectedConfigMap = resource
        state.setResourceYAML(sampleYAML(named: resource.name))
        state.setResourceDescribe(sampleDescribe(named: resource.name))
        return RuneAppViewModel(state: state)
    }

    private func hostSnapshot(
        viewModel: RuneAppViewModel,
        rootView: (RuneAppViewModel, @escaping (RuneRootLayoutSnapshot) -> Void) -> RuneRootView,
        section: RuneSection,
        kind: KubeResourceKind
    ) async throws -> RuneRootLayoutSnapshot {
        var snapshots: [RuneRootLayoutSnapshot] = []
        let host = NSHostingController(
            rootView: rootView(viewModel) { snapshot in
                snapshots.append(snapshot)
            }
        )

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        return try await waitForSnapshot(in: window, snapshots: { snapshots }) {
            $0.section == section
                && $0.workloadKind == kind
                && $0.contentMinY != nil
                && $0.headerMinY != nil
                && $0.detailMinY != nil
        }
    }

    private func assertAligned(baseline: RuneRootLayoutSnapshot, candidate: RuneRootLayoutSnapshot) {
        XCTAssertEqual(
            baseline.resolvedWindowTopInset,
            candidate.resolvedWindowTopInset,
            accuracy: 0.5
        )
        XCTAssertEqual(baseline.contentMinY ?? 0, candidate.contentMinY ?? 0, accuracy: 1.5)
        XCTAssertEqual(baseline.headerMinY ?? 0, candidate.headerMinY ?? 0, accuracy: 1.5)
        XCTAssertEqual(baseline.detailMinY ?? 0, candidate.detailMinY ?? 0, accuracy: 1.5)
    }

    private func sampleYAML(named name: String) -> String {
        Array(repeating: "kind: ConfigMap\nmetadata:\n  name: \(name)\n  namespace: example-backend\n", count: 24)
            .joined(separator: "---\n")
    }

    private func sampleDescribe(named name: String) -> String {
        Array(repeating: "Name: \(name)\nNamespace: example-backend\nLabels: app=orders\nEvents: <none>\n", count: 32)
            .joined(separator: "\n")
    }
}
