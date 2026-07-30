import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneStore
@testable import RuneUI

@MainActor
final class RuneFakeClusterYAMLWorkflowIntegrationTests: XCTestCase {
    func testNonDefaultNamespaceLongLineValidationUndoSearchAndApplyWorkflow() async throws {
        let previousDryRunSetting = UserDefaults.standard.object(
            forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun
        )
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = true
        defer {
            restoreSetting(
                previousDryRunSetting,
                forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun
            )
        }

        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await selectBravoConfigMap(in: harness)

        let searchToken = "synthetic-long-line-target"
        let longValue = searchToken + "-" + String(repeating: "x", count: 4_096)
        let validDraft = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: bravo-spoke-settings
          namespace: bravo-zone
        data:
          LONG_VALUE: "\(longValue)"
        """
        harness.state.updateResourceYAMLDraft(validDraft)

        try await waitUntil(timeout: 4) {
            !harness.state.isValidatingResourceYAML
                && !harness.state.resourceYAMLValidationIssues.contains(where: { $0.severity == .error })
        }
        try await Task.sleep(nanoseconds: 450_000_000)
        XCTAssertFalse(
            harness.server.requestLines().contains(where: {
                isConfigMapDryRunRequest($0, namespace: "bravo-zone")
            }),
            "Editing a valid local draft must not contact Kubernetes."
        )

        let searchIndex = InspectorFindIndex(
            text: harness.state.resourceYAML,
            query: searchToken,
            matchCase: true
        )
        XCTAssertEqual(searchIndex.ranges.count, 1)
        XCTAssertEqual(searchIndex.matchLineNumber(selectedIndex: 0), 7)

        let dryRunCountBeforeInvalidEdit = harness.server.requestLines().filter {
            isConfigMapDryRunRequest($0, namespace: "bravo-zone")
        }.count
        let invalidDraft = validDraft.replacingOccurrences(
            of: "\ndata:\n",
            with: "\ndata:\n\tBROKEN: value\n"
        )
        harness.state.updateResourceYAMLDraft(invalidDraft)

        try await waitUntil {
            !harness.state.isValidatingResourceYAML
                && harness.state.resourceYAMLValidationIssues.contains(where: {
                    $0.severity == .error
                        && $0.message == "Tabs are not allowed in YAML indentation."
                })
        }

        harness.viewModel.requestApplySelectedResourceYAML()

        XCTAssertNil(harness.viewModel.pendingWriteAction)
        XCTAssertEqual(
            harness.state.lastError,
            "Invalid input: Fix YAML errors before applying."
        )
        XCTAssertEqual(
            harness.server.requestLines().filter {
                isConfigMapDryRunRequest($0, namespace: "bravo-zone")
            }.count,
            dryRunCountBeforeInvalidEdit,
            "A locally invalid draft must not reach server validation or apply."
        )

        harness.state.clearError()
        harness.viewModel.undoResourceYAMLEdit()

        try await waitUntil(timeout: 4) {
            harness.state.resourceYAML == validDraft
                && !harness.state.isValidatingResourceYAML
                && !harness.state.resourceYAMLValidationIssues.contains(where: { $0.severity == .error })
        }

        harness.viewModel.requestApplySelectedResourceYAML()

        try await waitUntil(timeout: 4) {
            harness.viewModel.pendingWriteDryRunStatus
                == "Passed. Kubernetes accepted the server-side dry-run."
        }

        guard case let .apply(kind, name, yaml, _)? = harness.viewModel.pendingWriteAction else {
            return XCTFail("Expected a pending ConfigMap apply after undo restored the valid draft.")
        }
        XCTAssertEqual(kind, .configMap)
        XCTAssertEqual(name, "bravo-spoke-settings")
        XCTAssertEqual(yaml, validDraft)
        XCTAssertTrue(
            harness.viewModel.pendingWriteActionKubectlCommand.contains(
                "--namespace bravo-zone apply --server-side"
            )
        )

        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil(timeout: 4) {
            harness.state.writeAuditLog.contains(where: {
                $0.action == "Apply YAML"
                    && $0.namespace == "bravo-zone"
                    && $0.resource == "configmap/bravo-spoke-settings"
                    && $0.status == "Succeeded"
            })
        }

        let requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains {
            isConfigMapDryRunRequest($0, namespace: "bravo-zone")
        })
        XCTAssertTrue(requestLines.contains {
            isConfigMapApplyRequest($0, namespace: "bravo-zone")
        })
        XCTAssertFalse(requestLines.contains {
            $0.hasPrefix("PATCH /api/v1/namespaces/alpha-zone/configmaps/bravo-spoke-settings")
        })
    }

    func testCancellingPendingApplyAfterDryRunNeverWritesToCluster() async throws {
        let previousDryRunSetting = UserDefaults.standard.object(
            forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun
        )
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = true
        defer {
            restoreSetting(
                previousDryRunSetting,
                forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun
            )
        }

        let harness = try await makeHarness()
        defer { harness.cleanup() }
        try await selectBravoConfigMap(in: harness)

        let validDraft = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: bravo-spoke-settings
          namespace: bravo-zone
        data:
          CANCEL_PROBE: "synthetic"
        """
        harness.state.updateResourceYAMLDraft(validDraft)

        try await waitUntil(timeout: 4) {
            !harness.state.isValidatingResourceYAML
                && !harness.state.resourceYAMLValidationIssues.contains(where: { $0.severity == .error })
        }
        try await Task.sleep(nanoseconds: 450_000_000)
        XCTAssertFalse(
            harness.server.requestLines().contains(where: {
                isConfigMapDryRunRequest($0, namespace: "bravo-zone")
            }),
            "Server dry-run must start only after Apply is requested."
        )

        harness.server.resetRequestLines()
        harness.viewModel.requestApplySelectedResourceYAML()

        try await waitUntil(timeout: 4) {
            harness.viewModel.pendingWriteDryRunStatus
                == "Passed. Kubernetes accepted the server-side dry-run."
        }
        XCTAssertNotNil(harness.viewModel.pendingWriteAction)
        XCTAssertTrue(harness.server.requestLines().contains {
            isConfigMapDryRunRequest($0, namespace: "bravo-zone")
        })

        harness.viewModel.cancelPendingWriteAction()

        XCTAssertNil(harness.viewModel.pendingWriteAction)
        XCTAssertNil(harness.viewModel.pendingWriteDryRunStatus)
        XCTAssertFalse(harness.server.requestLines().contains {
            isConfigMapApplyRequest($0, namespace: "bravo-zone")
        })
        XCTAssertTrue(harness.state.writeAuditLog.isEmpty)
    }

    func testManualYAMLDryRunPassesWithoutPreparingOrPerformingWrite() async throws {
        let previousDryRunSetting = UserDefaults.standard.object(
            forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun
        )
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = false
        defer {
            restoreSetting(
                previousDryRunSetting,
                forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun
            )
        }

        let harness = try await makeHarness()
        defer { harness.cleanup() }
        try await selectBravoConfigMap(in: harness)

        let baseline = harness.state.resourceYAMLBaseline
        let validDraft = Self.bravoConfigMapDraft(value: "manual-check")
        harness.state.updateResourceYAMLDraft(validDraft)
        try await waitUntil {
            harness.state.resourceYAML == validDraft
                && harness.state.resourceYAMLValidationIssues.isEmpty
        }

        harness.server.resetRequestLines()
        harness.viewModel.requestDryRunSelectedResourceYAML()

        try await waitUntil(timeout: 4) {
            harness.viewModel.resourceYAMLDryRunStatus
                == "Dry run passed. Nothing was applied."
        }

        let dryRunRequests = harness.server.requests().filter {
            isConfigMapDryRunRequest($0.requestLine, namespace: "bravo-zone")
        }
        XCTAssertEqual(dryRunRequests.count, 1)
        XCTAssertEqual(dryRunRequests.first?.body, validDraft)
        XCTAssertFalse(harness.server.requestLines().contains {
            isConfigMapApplyRequest($0, namespace: "bravo-zone")
        })
        XCTAssertNil(harness.viewModel.pendingWriteAction)
        XCTAssertTrue(harness.state.writeAuditLog.isEmpty)
        XCTAssertEqual(harness.state.resourceYAMLBaseline, baseline)
        XCTAssertEqual(harness.state.resourceYAML, validDraft)
        XCTAssertTrue(harness.state.resourceYAMLHasUnsavedEdits)
    }

    func testManualYAMLDryRunServerFailureIsUnderstandableAndNeverWrites() async throws {
        let fixture = RuneFakeK8sFixture(
            transientFailureTargets: [Self.bravoConfigMapDryRunTarget]
        )
        let harness = try await makeHarness(fixture: fixture)
        defer { harness.cleanup() }
        try await selectBravoConfigMap(in: harness)

        harness.state.updateResourceYAMLDraft(
            Self.bravoConfigMapDraft(value: "server-failure")
        )
        try await waitUntil {
            harness.state.resourceYAMLValidationIssues.isEmpty
        }

        harness.server.resetRequestLines()
        harness.viewModel.requestDryRunSelectedResourceYAML()

        try await waitUntil(timeout: 4) {
            harness.viewModel.resourceYAMLDryRunStatus?.contains(
                "Synthetic transient failure"
            ) == true
        }

        let status = try XCTUnwrap(harness.viewModel.resourceYAMLDryRunStatus)
        XCTAssertTrue(status.hasPrefix("Dry run could not complete:"))
        XCTAssertFalse(status.contains(#""kind":"Status""#))
        XCTAssertFalse(status.contains("safe to retry"))
        XCTAssertEqual(
            harness.server.requestLines().filter {
                isConfigMapDryRunRequest($0, namespace: "bravo-zone")
            }.count,
            1
        )
        XCTAssertFalse(harness.server.requestLines().contains {
            isConfigMapApplyRequest($0, namespace: "bravo-zone")
        })
        XCTAssertNil(harness.viewModel.pendingWriteAction)
        XCTAssertTrue(harness.state.writeAuditLog.isEmpty)
    }

    func testManualYAMLDryRunDropsDelayedResultAfterDraftChanges() async throws {
        let fixture = RuneFakeK8sFixture(
            delayedResponseTargets: [Self.bravoConfigMapDryRunTarget: 600_000_000]
        )
        let harness = try await makeHarness(fixture: fixture)
        defer { harness.cleanup() }
        try await selectBravoConfigMap(in: harness)

        let firstDraft = Self.bravoConfigMapDraft(value: "before-delay")
        harness.state.updateResourceYAMLDraft(firstDraft)
        try await waitUntil {
            harness.state.resourceYAMLValidationIssues.isEmpty
        }

        harness.server.resetRequestLines()
        harness.viewModel.requestDryRunSelectedResourceYAML()
        try await waitUntil {
            harness.server.requestLines().contains {
                isConfigMapDryRunRequest($0, namespace: "bravo-zone")
            }
                && harness.viewModel.isRunningResourceYAMLDryRun
        }

        let changedDraft = Self.bravoConfigMapDraft(value: "after-delay")
        harness.state.updateResourceYAMLDraft(changedDraft)
        try await waitUntil {
            harness.state.resourceYAML == changedDraft
                && harness.viewModel.resourceYAMLDryRunStatus == nil
                && !harness.viewModel.isRunningResourceYAMLDryRun
        }
        try await Task.sleep(nanoseconds: 800_000_000)

        XCTAssertNil(harness.viewModel.resourceYAMLDryRunStatus)
        XCTAssertFalse(harness.viewModel.isRunningResourceYAMLDryRun)
        XCTAssertTrue(harness.state.resourceYAMLValidationIssues.isEmpty)
        XCTAssertNil(harness.viewModel.pendingWriteAction)
        XCTAssertFalse(harness.server.requestLines().contains {
            isConfigMapApplyRequest($0, namespace: "bravo-zone")
        })
    }

    func testApplyRunsFreshDryRunAfterSuccessfulManualDryRun() async throws {
        let previousDryRunSetting = UserDefaults.standard.object(
            forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun
        )
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = true
        defer {
            restoreSetting(
                previousDryRunSetting,
                forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun
            )
        }

        let harness = try await makeHarness()
        defer { harness.cleanup() }
        try await selectBravoConfigMap(in: harness)

        harness.state.updateResourceYAMLDraft(
            Self.bravoConfigMapDraft(value: "fresh-apply-check")
        )
        try await waitUntil {
            harness.state.resourceYAMLValidationIssues.isEmpty
        }

        harness.server.resetRequestLines()
        harness.viewModel.requestDryRunSelectedResourceYAML()
        try await waitUntil(timeout: 4) {
            harness.viewModel.resourceYAMLDryRunStatus
                == "Dry run passed. Nothing was applied."
        }
        XCTAssertEqual(
            harness.server.requestLines().filter {
                isConfigMapDryRunRequest($0, namespace: "bravo-zone")
            }.count,
            1
        )

        harness.viewModel.requestApplySelectedResourceYAML()
        try await waitUntil(timeout: 4) {
            harness.viewModel.pendingWriteDryRunStatus
                == "Passed. Kubernetes accepted the server-side dry-run."
                && harness.server.requestLines().filter {
                    isConfigMapDryRunRequest($0, namespace: "bravo-zone")
                }.count == 2
        }

        XCTAssertNotNil(harness.viewModel.pendingWriteAction)
        XCTAssertFalse(harness.server.requestLines().contains {
            isConfigMapApplyRequest($0, namespace: "bravo-zone")
        })
        harness.viewModel.cancelPendingWriteAction()
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

    private func makeHarness(
        fixture: RuneFakeK8sFixture = RuneFakeK8sFixture()
    ) async throws -> Harness {
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        let kubeconfigURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-yaml-workflow-\(UUID().uuidString).yaml")
        try server.kubeconfigYAML().write(
            to: kubeconfigURL,
            atomically: true,
            encoding: .utf8
        )

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfigURL)])
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

    private func selectBravoConfigMap(in harness: Harness) async throws {
        try await harness.viewModel.reloadContexts()
        harness.viewModel.setNamespace("bravo-zone")
        harness.viewModel.setSection(.config)
        harness.viewModel.setWorkloadKind(.configMap)

        try await waitUntil {
            harness.state.selectedNamespace == "bravo-zone"
                && harness.state.configMaps.contains(where: { $0.name == "bravo-spoke-settings" })
                && !harness.state.isLoading
        }

        let configMap = try XCTUnwrap(
            harness.state.configMaps.first(where: { $0.name == "bravo-spoke-settings" })
        )
        harness.viewModel.selectConfigMap(configMap)

        try await waitUntil {
            harness.state.resourceDetailScope == ResourceDetailScope(
                contextName: RuneFakeK8sFixture.defaultContextName,
                namespace: "bravo-zone",
                kind: .configMap,
                name: "bravo-spoke-settings"
            )
                && harness.state.resourceYAML.contains("bravo-spoke-settings")
                && !harness.state.isLoadingResourceDetails
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
            guard Date() < deadline else {
                XCTFail("Timed out waiting for YAML workflow condition.", file: file, line: line)
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    private func restoreSetting(_ value: Any?, forKey key: String) {
        if let value {
            UserDefaults.standard.set(value, forKey: key)
        } else {
            UserDefaults.standard.removeObject(forKey: key)
        }
    }

    private static func bravoConfigMapDraft(value: String) -> String {
        """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: bravo-spoke-settings
          namespace: bravo-zone
        data:
          MODE: "\(value)"
        """
    }

    private static let bravoConfigMapDryRunTarget =
        "/api/v1/namespaces/bravo-zone/configmaps/bravo-spoke-settings?fieldManager=rune&force=true&dryRun=All"
}

private func isConfigMapDryRunRequest(_ requestLine: String, namespace: String) -> Bool {
    requestLine.hasPrefix(
        "PATCH /api/v1/namespaces/\(namespace)/configmaps/bravo-spoke-settings?"
    )
        && requestLine.contains("dryRun=All")
}

private func isConfigMapApplyRequest(_ requestLine: String, namespace: String) -> Bool {
    requestLine.hasPrefix(
        "PATCH /api/v1/namespaces/\(namespace)/configmaps/bravo-spoke-settings?"
    )
        && requestLine.contains("fieldManager=rune")
        && !requestLine.contains("dryRun=All")
}
