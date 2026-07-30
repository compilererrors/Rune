import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneStore
@testable import RuneUI

@MainActor
final class RuneResourceYAMLLocalValidationTests: XCTestCase {
    func testEditingValidYAMLDoesNotContactKubernetes() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }
        harness.server.resetRequestLines()

        let editedYAML = Self.validYAML.replacingOccurrences(
            of: "MODE: synthetic",
            with: "MODE: edited-locally"
        )
        harness.state.updateResourceYAMLDraft(editedYAML)

        try await waitUntil(timeout: 3) {
            harness.state.resourceYAML == editedYAML
                && harness.state.resourceYAMLValidationIssues.isEmpty
                && !harness.state.isValidatingResourceYAML
        }
        try await Task.sleep(nanoseconds: 450_000_000)

        XCTAssertEqual(harness.state.resourceYAML, editedYAML)
        XCTAssertTrue(harness.state.resourceYAMLValidationIssues.isEmpty)
        XCTAssertFalse(
            harness.server.requestLines().contains(where: isValidationRequest),
            "Editing a local YAML draft must not start a server-side dry-run."
        )
    }

    func testInvalidEditReportsLocalSyntaxIssueWithoutContactingKubernetes() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }
        harness.server.resetRequestLines()

        let invalidYAML = Self.validYAML.replacingOccurrences(
            of: "\ndata:\n",
            with: "\ndata:\n\tBROKEN: value\n"
        )
        harness.state.updateResourceYAMLDraft(invalidYAML)

        try await waitUntil {
            !harness.state.isValidatingResourceYAML
                && harness.state.resourceYAMLValidationIssues.contains(where: {
                    $0.severity == .error
                        && $0.message == "Tabs are not allowed in YAML indentation."
                })
        }
        try await Task.sleep(nanoseconds: 450_000_000)

        XCTAssertEqual(harness.state.resourceYAML, invalidYAML)
        XCTAssertTrue(
            harness.state.resourceYAMLValidationIssues.contains(where: {
                $0.severity == .error
                    && $0.message == "Tabs are not allowed in YAML indentation."
            })
        )
        XCTAssertFalse(
            harness.server.requestLines().contains(where: isValidationRequest),
            "Local syntax validation must not contact Kubernetes."
        )
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
        let kubeconfigURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-yaml-local-validation-\(UUID().uuidString).yaml")
        try server.kubeconfigYAML().write(
            to: kubeconfigURL,
            atomically: true,
            encoding: .utf8
        )

        let resource = ClusterResourceSummary(
            kind: .configMap,
            name: "orbit-lens-settings",
            namespace: "alpha-zone",
            primaryText: "Synthetic settings",
            secondaryText: ""
        )
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfigURL)])
        state.selectedContext = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        state.selectedNamespace = "alpha-zone"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setConfigMaps([resource])
        state.setSelectedConfigMap(resource)
        state.setResourceYAML(Self.validYAML)

        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: KubernetesClient(),
            store: ResourceStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        return Harness(
            server: server,
            kubeconfigURL: kubeconfigURL,
            state: state,
            viewModel: viewModel
        )
    }

    private func waitUntil(
        timeout: TimeInterval = 2,
        file: StaticString = #filePath,
        line: UInt = #line,
        predicate: @escaping @MainActor () -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !predicate() {
            guard Date() < deadline else {
                XCTFail("Timed out waiting for local YAML validation.", file: file, line: line)
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    private static let validYAML = """
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: orbit-lens-settings
      namespace: alpha-zone
    data:
      MODE: synthetic
    """
}

private func isValidationRequest(_ requestLine: String) -> Bool {
    requestLine.contains("fieldManager=rune")
        && requestLine.contains("dryRun=All")
}
