import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneStore
@testable import RuneUI

@MainActor
final class RuneFakeClusterViewModelIntegrationTests: XCTestCase {
    func testOverviewStartupLoadsFakeClusterSnapshot() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()

        XCTAssertEqual(harness.state.contexts.map(\.name), [RuneFakeK8sFixture.defaultContextName])
        XCTAssertEqual(harness.state.selectedNamespace, "alpha-zone")
        XCTAssertEqual(harness.state.namespaces, ["alpha-zone", "bravo-zone"])
        XCTAssertEqual(harness.state.overviewPods.map(\.name), [
            "ember-gate-75c9f746b8-kq2wm",
            "orbit-lens-6f58d7d89b-hx9q2"
        ])
        XCTAssertEqual(harness.state.overviewDeploymentsCount, 2)
        XCTAssertEqual(harness.state.overviewServicesCount, 2)
        XCTAssertEqual(harness.state.overviewIngressesCount, 1)
        XCTAssertEqual(harness.state.overviewConfigMapsCount, 2)
        XCTAssertEqual(harness.state.overviewCronJobsCount, 1)
        XCTAssertEqual(harness.state.overviewNodesCount, 3)
        XCTAssertEqual(harness.state.overviewEvents.count, 2)
        XCTAssertNil(harness.state.lastError)
    }

    func testTerminalStartupLoadsPodsWithoutVisitingWorkloadsFirst() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }
        harness.state.selectedSection = .terminal

        try await harness.viewModel.reloadContexts()

        XCTAssertEqual(harness.state.selectedSection, .terminal)
        XCTAssertEqual(harness.state.selectedNamespace, "alpha-zone")
        XCTAssertEqual(harness.state.pods.map(\.name), [
            "ember-gate-75c9f746b8-kq2wm",
            "orbit-lens-6f58d7d89b-hx9q2"
        ])
        XCTAssertNil(harness.state.lastError)
    }

    func testTerminalNamespaceSwitchReloadsPodsForShellAndPortForwardSelectors() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }
        harness.state.selectedSection = .terminal
        try await harness.viewModel.reloadContexts()

        harness.viewModel.setNamespace("bravo-zone")

        try await waitUntil {
            harness.state.selectedNamespace == "bravo-zone"
                && harness.state.pods.map(\.name) == ["bravo-spoke-59fd6dfb4b-s9n2p"]
        }

        XCTAssertEqual(harness.state.selectedSection, .terminal)
        XCTAssertEqual(harness.state.pods.first?.status, "Pending")
        XCTAssertNil(harness.state.lastError)
    }

    func testSectionNavigationLoadsExpectedFakeClusterData() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()

        harness.viewModel.setSection(.workloads)
        try await waitUntil {
            harness.state.pods.map(\.name) == [
                "ember-gate-75c9f746b8-kq2wm",
                "orbit-lens-6f58d7d89b-hx9q2"
            ]
        }

        harness.viewModel.setWorkloadKind(.deployment)
        try await waitUntil {
            harness.state.deployments.map(\.name) == ["ember-gate", "orbit-lens"]
        }

        harness.viewModel.setSection(.networking)
        try await waitUntil {
            harness.state.services.map(\.name) == ["ember-gate", "orbit-lens"]
        }

        harness.viewModel.setSection(.config)
        try await waitUntil {
            harness.state.configMaps.map(\.name) == ["ember-gate-settings", "orbit-lens-settings"]
        }

        harness.viewModel.setSection(.events)
        try await waitUntil {
            harness.state.events.map(\.objectName) == [
                "ember-gate-75c9f746b8-kq2wm",
                "orbit-lens-6f58d7d89b-hx9q2"
            ]
        }

        XCTAssertNil(harness.state.lastError)
    }

    private struct Harness {
        let server: RuneFakeK8sRESTServer
        let kubeconfigURL: URL
        let state: RuneAppState
        let viewModel: RuneAppViewModel

        func cleanup() {
            server.stop()
            try? FileManager.default.removeItem(at: kubeconfigURL)
        }
    }

    private func makeHarness() async throws -> Harness {
        let server = try await RuneFakeK8sRESTServer.start()
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfig)])
        let viewModel = RuneAppViewModel(
            state: state,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        return Harness(server: server, kubeconfigURL: kubeconfig, state: state, viewModel: viewModel)
    }

    private func writeKubeconfig(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-ui-fake-cluster-\(UUID().uuidString).yaml")
        try contents.write(to: url, atomically: true, encoding: .utf8)
        return url
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
