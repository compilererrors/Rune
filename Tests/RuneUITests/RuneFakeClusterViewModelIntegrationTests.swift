import Foundation
import XCTest
@testable import RuneCore
@testable import RuneDiagnostics
@testable import RuneExport
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneStore
@testable import RuneUI

@MainActor
final class RuneFakeClusterViewModelIntegrationTests: XCTestCase {
    func testDemoContextIsVisibleWithoutHijackingFakeClusterStartup() async throws {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer { restoreDemoSetting(previousDemoSetting) }

        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()

        XCTAssertEqual(harness.state.contexts.map(\.name), [RuneFakeK8sFixture.defaultContextName])
        XCTAssertEqual(harness.state.selectedContext?.name, RuneFakeK8sFixture.defaultContextName)
        XCTAssertEqual(harness.state.selectedNamespace, "alpha-zone")
        XCTAssertTrue(harness.viewModel.visibleContexts.map(\.name).contains("rune-demo"))
        XCTAssertTrue(harness.viewModel.contextMenuOptions.map(\.name).contains("rune-demo"))
        XCTAssertEqual(harness.state.overviewPods.map(\.name), [
            "ember-gate-75c9f746b8-kq2wm",
            "orbit-lens-6f58d7d89b-hx9q2"
        ])
        XCTAssertNil(harness.state.lastError)
    }

    func testSelectingDemoContextFromFakeClusterUsesInMemorySnapshotAndCanReturnToFakeCluster() async throws {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer { restoreDemoSetting(previousDemoSetting) }

        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.server.resetRequestLines()

        harness.viewModel.setContext(KubeContext(name: "rune-demo"))

        XCTAssertEqual(harness.state.selectedContext?.name, "rune-demo")
        XCTAssertEqual(harness.state.selectedNamespace, "demo")
        XCTAssertEqual(harness.state.contexts.map(\.name), [RuneFakeK8sFixture.defaultContextName, "rune-demo"])
        XCTAssertFalse(harness.state.pods.isEmpty)
        XCTAssertTrue(harness.state.resourceYAML.contains("kind: Pod"))
        XCTAssertTrue(harness.server.requestLines().isEmpty)
        XCTAssertNil(harness.state.lastError)

        harness.viewModel.setContext(KubeContext(name: RuneFakeK8sFixture.defaultContextName))

        try await waitUntil {
            harness.state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                && harness.state.selectedNamespace == "alpha-zone"
                && harness.state.pods.map(\.name) == [
                    "ember-gate-75c9f746b8-kq2wm",
                    "orbit-lens-6f58d7d89b-hx9q2"
                ]
                && !harness.state.isLoading
        }

        XCTAssertNil(harness.state.lastError)
    }

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
        XCTAssertEqual(harness.state.deployments.count, 2)
        XCTAssertEqual(harness.state.services.count, 2)
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

    func testFakeClusterLoadsPodLogsYAMLAndDescribeThroughViewModel() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.viewModel.setSection(.workloads)

        try await waitUntil {
            harness.state.selectedSection == .workloads
                && harness.state.selectedWorkloadKind == .pod
                && harness.state.selectedPod?.name == "ember-gate-75c9f746b8-kq2wm"
                && harness.state.resourceYAML.contains("Pod")
                && harness.state.resourceDescribe.contains("Name:")
                && harness.state.resourceDescribe.contains("ember-gate-75c9f746b8-kq2wm")
        }

        harness.viewModel.reloadLogsForSelection()

        try await waitUntil {
            !harness.state.isLoadingLogs
                && harness.state.podLogs.contains("synthetic REST fake log")
                && harness.state.lastLogFetchError == nil
        }

        XCTAssertNil(harness.state.lastResourceYAMLError)
        XCTAssertNil(harness.state.lastResourceDescribeError)
        XCTAssertNil(harness.state.lastError)
    }

    func testFakeClusterLoadsUnifiedDeploymentAndServiceLogsThroughViewModel() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.viewModel.setSection(.workloads)
        harness.viewModel.setWorkloadKind(.deployment)

        try await waitUntil {
            harness.state.selectedSection == .workloads
                && harness.state.selectedWorkloadKind == .deployment
                && harness.state.deployments.contains { $0.name == "orbit-lens" }
                && !harness.state.isLoading
        }

        let deployment = try XCTUnwrap(harness.state.deployments.first { $0.name == "orbit-lens" })
        harness.server.resetRequestLines()
        harness.viewModel.selectDeployment(deployment)
        harness.viewModel.reloadLogsForSelection()

        try await waitUntil {
            let requestLines = harness.server.requestLines()
            return !harness.state.isLoadingLogs
                && harness.state.selectedDeployment?.name == "orbit-lens"
                && harness.state.unifiedServiceLogPods == ["orbit-lens-6f58d7d89b-hx9q2"]
                && harness.state.unifiedServiceLogs.contains("[orbit-lens-6f58d7d89b-hx9q2]")
                && harness.state.unifiedServiceLogs.contains("synthetic REST fake log")
                && requestLines.contains { $0.contains("/pods?labelSelector=") && $0.contains("orbit-lens") }
                && requestLines.contains { $0.contains("/pods/orbit-lens-6f58d7d89b-hx9q2/log") }
        }

        var requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.contains("/pods?labelSelector=") && $0.contains("orbit-lens") })
        XCTAssertTrue(requestLines.contains { $0.contains("/pods/orbit-lens-6f58d7d89b-hx9q2/log") })
        XCTAssertNil(harness.state.lastLogFetchError)

        harness.viewModel.setSection(.networking)
        try await waitUntil {
            harness.state.selectedSection == .networking
                && harness.state.selectedWorkloadKind == .service
                && harness.state.services.contains { $0.name == "orbit-lens" }
                && !harness.state.isLoading
        }

        let service = try XCTUnwrap(harness.state.services.first { $0.name == "orbit-lens" })
        harness.server.resetRequestLines()
        harness.viewModel.selectService(service)
        harness.viewModel.reloadLogsForSelection()

        try await waitUntil {
            let requestLines = harness.server.requestLines()
            return !harness.state.isLoadingLogs
                && harness.state.selectedService?.name == "orbit-lens"
                && harness.state.unifiedServiceLogPods == ["orbit-lens-6f58d7d89b-hx9q2"]
                && harness.state.unifiedServiceLogs.contains("[orbit-lens-6f58d7d89b-hx9q2]")
                && harness.state.unifiedServiceLogs.contains("synthetic REST fake log")
                && requestLines.contains { $0.contains("/pods?labelSelector=") && $0.contains("orbit-lens") }
                && requestLines.contains { $0.contains("/pods/orbit-lens-6f58d7d89b-hx9q2/log") }
        }

        requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.contains("/pods?labelSelector=") && $0.contains("orbit-lens") })
        XCTAssertTrue(requestLines.contains { $0.contains("/pods/orbit-lens-6f58d7d89b-hx9q2/log") })
        XCTAssertNil(harness.state.lastLogFetchError)
        XCTAssertNil(harness.state.lastError)
    }

    func testTerminalPodInspectorLoadsLogsAndYAMLAgainstFakeClusterWithoutLeavingTerminal() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }
        harness.state.selectedSection = .terminal

        try await harness.viewModel.reloadContexts()
        let pod = try XCTUnwrap(harness.state.pods.first)
        harness.server.resetRequestLines()

        harness.viewModel.focusTerminalPodInspector(pod, reloadLogs: true, loadDetails: false)

        try await waitUntil {
            harness.state.selectedSection == .terminal
                && harness.state.selectedWorkloadKind == .pod
                && harness.state.selectedPod?.id == pod.id
                && harness.state.podLogs.contains("synthetic REST fake log")
                && !harness.state.isLoadingLogs
        }

        harness.viewModel.focusTerminalPodInspector(pod, reloadLogs: false, loadDetails: true)

        try await waitUntil {
            harness.state.selectedSection == .terminal
                && harness.state.resourceYAML.contains(pod.name)
                && harness.state.resourceDescribe.contains(pod.name)
                && !harness.state.isLoadingResourceDetails
        }

        let requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.contains("/pods/\(pod.name)/log") })
        XCTAssertTrue(requestLines.contains { $0.contains("/pods/\(pod.name)") })
        XCTAssertNil(harness.state.lastLogFetchError)
        XCTAssertNil(harness.state.lastResourceYAMLError)
        XCTAssertNil(harness.state.lastResourceDescribeError)
        XCTAssertNil(harness.state.lastError)
    }

    func testTerminalRightPanelLogWorkflowDoesNotFollowShellPodFallbackAgainstFakeCluster() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }
        harness.state.selectedSection = .terminal

        try await harness.viewModel.reloadContexts()
        let logPod = try XCTUnwrap(harness.state.pods.first { $0.name == "ember-gate-75c9f746b8-kq2wm" })
        let shellPod = try XCTUnwrap(harness.state.pods.first { $0.name == "orbit-lens-6f58d7d89b-hx9q2" })
        var logTabs = TerminalPodLogTabState()
        logTabs.ensureTab(for: logPod)

        harness.server.resetRequestLines()
        harness.viewModel.focusTerminalPodInspector(logPod, reloadLogs: true, loadDetails: false)

        try await waitUntil {
            harness.state.selectedSection == .terminal
                && harness.state.selectedPod?.id == logPod.id
                && harness.state.podLogs.contains("synthetic REST fake log")
                && !harness.state.isLoadingLogs
        }

        XCTAssertTrue(harness.server.requestLines().contains { $0.contains("/pods/\(logPod.name)/log") })

        harness.server.resetRequestLines()
        logTabs.reconcile(availablePods: harness.state.pods, fallbackPod: shellPod)

        XCTAssertEqual(logTabs.activePod(in: harness.state.pods, fallback: shellPod)?.id, logPod.id)
        XCTAssertEqual(logTabs.selectedPodID, logPod.id)
        XCTAssertEqual(harness.state.selectedPod?.id, logPod.id)
        XCTAssertTrue(harness.server.requestLines().isEmpty)

        logTabs.updateActive(to: shellPod)
        harness.viewModel.focusTerminalPodInspector(shellPod, reloadLogs: true, loadDetails: false)

        try await waitUntil {
            harness.state.selectedSection == .terminal
                && harness.state.selectedPod?.id == shellPod.id
                && !harness.state.isLoadingLogs
                && harness.server.requestLines().contains { $0.contains("/pods/\(shellPod.name)/log") }
        }

        XCTAssertTrue(harness.server.requestLines().contains { $0.contains("/pods/\(shellPod.name)/log") })

        if harness.viewModel.isFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name) {
            harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name)
        }
        if harness.viewModel.isFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name) {
            harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name)
        }

        harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name)
        XCTAssertTrue(harness.viewModel.isFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name))
        XCTAssertFalse(harness.viewModel.isFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name))

        harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name)
        XCTAssertTrue(harness.viewModel.isFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name))
        XCTAssertTrue(harness.viewModel.isFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name))

        harness.viewModel.toggleFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name)
        XCTAssertFalse(harness.viewModel.isFavoriteResource(kind: .pod, namespace: logPod.namespace, name: logPod.name))
        XCTAssertTrue(harness.viewModel.isFavoriteResource(kind: .pod, namespace: shellPod.namespace, name: shellPod.name))
        XCTAssertNil(harness.state.lastLogFetchError)
        XCTAssertNil(harness.state.lastError)
    }

    func testTerminalPodLogEndpointFailureStaysInInspectorAndPreservesCachedLogs() async throws {
        let failingPodName = "ember-gate-75c9f746b8-kq2wm"
        let fixture = RuneFakeK8sFixture(
            contexts: RuneFakeK8sFixture.defaultContexts.map { cluster in
                RuneFakeK8sCluster(
                    contextName: cluster.contextName,
                    defaultNamespace: cluster.defaultNamespace,
                    namespaces: cluster.namespaces.map { namespace in
                        RuneFakeK8sNamespace(
                            name: namespace.name,
                            pods: namespace.pods,
                            deployments: namespace.deployments,
                            services: namespace.services,
                            failingLogPodNames: namespace.name == "alpha-zone" ? [failingPodName] : []
                        )
                    },
                    nodes: cluster.nodes,
                    operatorResources: cluster.operatorResources
                )
            }
        )
        let harness = try await makeHarness(fixture: fixture)
        defer { harness.cleanup() }
        harness.state.selectedSection = .terminal

        try await harness.viewModel.reloadContexts()
        let pod = try XCTUnwrap(harness.state.pods.first { $0.name == failingPodName })
        harness.state.appendPodLogRead(
            "cached log before forced endpoint failure\n",
            contextName: RuneFakeK8sFixture.defaultContextName,
            namespace: "alpha-zone",
            podName: failingPodName
        )
        harness.server.resetRequestLines()

        harness.viewModel.focusTerminalPodInspector(pod, reloadLogs: true, loadDetails: false)

        try await waitUntil {
            harness.state.selectedSection == .terminal
                && harness.state.selectedWorkloadKind == .pod
                && harness.state.selectedPod?.id == pod.id
                && !harness.state.isLoadingLogs
                && harness.state.lastLogFetchError?.contains("Synthetic forced pod log failure") == true
        }

        let requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.contains("/pods/\(failingPodName)/log") })
        XCTAssertTrue(harness.state.podLogs.contains("cached log before forced endpoint failure"))
        XCTAssertNil(harness.state.lastError)
    }

    func testRapidViewSwitchCoalescesFinalInspectorRequests() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.server.resetRequestLines()

        harness.viewModel.setSection(.workloads)
        harness.viewModel.setWorkloadKind(.deployment)
        harness.viewModel.setSection(.networking)
        harness.viewModel.setWorkloadKind(.service)
        harness.viewModel.setSection(.config)
        harness.viewModel.setWorkloadKind(.configMap)

        try await waitUntil {
            harness.state.selectedSection == .config
                && harness.state.selectedWorkloadKind == .configMap
                && harness.state.configMaps.map(\.name) == ["ember-gate-settings", "orbit-lens-settings"]
                && !harness.state.isLoading
                && !harness.state.isLoadingResourceDetails
                && harness.state.lastResourceYAMLError == nil
                && harness.state.lastResourceDescribeError == nil
        }

        let requestLines = harness.server.requestLines()
        let resourcePath = "/api/v1/namespaces/alpha-zone/configmaps/ember-gate-settings"
        let finalResourceGETs = requestLines.filter { line in
            line.hasPrefix("GET \(resourcePath)") || line.hasPrefix("GET \(resourcePath)?")
        }

        XCTAssertEqual(harness.state.selectedConfigMap?.name, "ember-gate-settings")
        XCTAssertEqual(finalResourceGETs.count, 2)
        XCTAssertFalse(requestLines.contains { $0.contains("/deployments/ember-gate") })
        XCTAssertNil(harness.state.lastError)
    }

    func testRollbackDryRunFailureBlocksRealRollbackAndAuditsFailureAgainstFakeCluster() async throws {
        let previousRolloutDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        let previousRollbackPlan = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
        let previousProductionConfirmation = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun = true
        UserDefaults.standard.runeWriteSafetyShowRollbackPlan = true
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = false
        defer {
            restoreSetting(previousRolloutDryRun, forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
            restoreSetting(previousRollbackPlan, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
            restoreSetting(previousProductionConfirmation, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.viewModel.setSection(.workloads)
        harness.viewModel.setWorkloadKind(.deployment)

        try await waitUntil {
            harness.state.deployments.map(\.name) == ["ember-gate", "orbit-lens"]
                && !harness.state.isLoading
        }

        let deployment = try XCTUnwrap(harness.state.deployments.first { $0.name == "orbit-lens" })
        harness.viewModel.selectDeployment(deployment)

        try await waitUntil {
            harness.state.selectedDeployment?.name == "orbit-lens"
                && harness.state.deploymentRolloutHistory.contains("REVISION")
                && harness.state.deploymentRolloutHistory.contains("1")
                && harness.state.deploymentRolloutHistory.contains("2")
                && !harness.state.isLoadingResourceDetails
        }

        harness.server.resetRequestLines()
        harness.viewModel.rolloutRevisionInput = "99"
        harness.viewModel.requestRolloutUndoSelectedDeployment()

        try await waitUntil {
            harness.viewModel.pendingWriteDryRunStatus?.contains("No matching ReplicaSet revision") == true
        }

        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Could not complete"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("Rollback plan:"))

        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Rollout Undo"
                    && entry.status == "Failed"
                    && entry.resource == "deployment/orbit-lens revision=99"
                    && entry.message.contains("No matching ReplicaSet revision")
            }
        }

        let auditEntry = try XCTUnwrap(harness.state.writeAuditLog.first { entry in
            entry.action == "Rollout Undo"
                && entry.resource == "deployment/orbit-lens revision=99"
        })
        let auditText = [
            auditEntry.contextName,
            auditEntry.namespace,
            auditEntry.resource,
            auditEntry.status,
            auditEntry.message
        ].joined(separator: "\n")
        XCTAssertEqual(auditEntry.contextName, RuneFakeK8sFixture.defaultContextName)
        XCTAssertEqual(auditEntry.namespace, "alpha-zone")
        XCTAssertFalse(auditText.contains(harness.kubeconfigURL.path))
        XCTAssertFalse(auditText.localizedCaseInsensitiveContains("token"))

        let requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("GET /apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens") })
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("GET /apis/apps/v1/namespaces/alpha-zone/replicasets") })
        XCTAssertFalse(requestLines.contains { $0.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens") })
    }

    func testRollbackSuccessAuditsPostActionReadinessVerificationAgainstFakeCluster() async throws {
        let previousRolloutDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        let previousRollbackPlan = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
        let previousPostActionVerification = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequirePostActionVerification)
        let previousProductionConfirmation = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun = true
        UserDefaults.standard.runeWriteSafetyShowRollbackPlan = true
        UserDefaults.standard.runeWriteSafetyRequirePostActionVerification = true
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = false
        defer {
            restoreSetting(previousRolloutDryRun, forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
            restoreSetting(previousRollbackPlan, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
            restoreSetting(previousPostActionVerification, forKey: RuneSettingsKeys.writeSafetyRequirePostActionVerification)
            restoreSetting(previousProductionConfirmation, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.viewModel.setSection(.workloads)
        harness.viewModel.setWorkloadKind(.deployment)

        try await waitUntil {
            harness.state.deployments.map(\.name) == ["ember-gate", "orbit-lens"]
                && !harness.state.isLoading
        }

        let deployment = try XCTUnwrap(harness.state.deployments.first { $0.name == "orbit-lens" })
        harness.viewModel.selectDeployment(deployment)

        try await waitUntil {
            harness.state.selectedDeployment?.name == "orbit-lens"
                && harness.state.deploymentRolloutHistory.contains("1")
                && harness.state.deploymentRolloutHistory.contains("2")
                && !harness.state.isLoadingResourceDetails
        }

        harness.server.resetRequestLines()
        harness.viewModel.rolloutRevisionInput = "1"
        harness.viewModel.requestRolloutUndoSelectedDeployment()

        try await waitUntil {
            harness.viewModel.pendingWriteDryRunStatus == "Server accepted rollback dry-run."
        }

        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Rollout Undo"
                    && entry.status == "Succeeded"
                    && entry.resource == "deployment/orbit-lens revision=1"
                    && entry.message.contains("Rollback completed after server dry-run")
                    && entry.message.contains("Post-action verification: Deployment orbit-lens is ready 2/2.")
            }
        }

        let requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens?dryRun=All") })
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens ") })
    }

    func testRollbackPostActionVerificationTimesOutWhenDeploymentDoesNotBecomeReadyAgainstFakeCluster() async throws {
        let previousRolloutDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        let previousRollbackPlan = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
        let previousPostActionVerification = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequirePostActionVerification)
        let previousProductionConfirmation = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun = true
        UserDefaults.standard.runeWriteSafetyShowRollbackPlan = true
        UserDefaults.standard.runeWriteSafetyRequirePostActionVerification = true
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = false
        defer {
            restoreSetting(previousRolloutDryRun, forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
            restoreSetting(previousRollbackPlan, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
            restoreSetting(previousPostActionVerification, forKey: RuneSettingsKeys.writeSafetyRequirePostActionVerification)
            restoreSetting(previousProductionConfirmation, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let harness = try await makeHarness(kubeClient: KubernetesClient(commandTimeout: 0.15))
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.viewModel.setSection(.workloads)
        harness.viewModel.setWorkloadKind(.deployment)

        try await waitUntil {
            harness.state.deployments.map(\.name) == ["ember-gate", "orbit-lens"]
                && !harness.state.isLoading
        }

        let deployment = try XCTUnwrap(harness.state.deployments.first { $0.name == "ember-gate" })
        harness.viewModel.selectDeployment(deployment)

        try await waitUntil {
            harness.state.selectedDeployment?.name == "ember-gate"
                && harness.state.deploymentRolloutHistory.contains("1")
                && harness.state.deploymentRolloutHistory.contains("2")
                && !harness.state.isLoadingResourceDetails
        }

        harness.server.resetRequestLines()
        harness.viewModel.rolloutRevisionInput = "1"
        harness.viewModel.requestRolloutUndoSelectedDeployment()

        try await waitUntil {
            harness.viewModel.pendingWriteDryRunStatus == "Server accepted rollback dry-run."
        }

        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil(timeout: 3) {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Rollout Undo"
                    && entry.status == "Succeeded"
                    && entry.resource == "deployment/ember-gate revision=1"
                    && entry.message.contains("Rollback completed after server dry-run")
                    && entry.message.contains("Post-action verification: Timed out waiting for Deployment ember-gate rollout readiness.")
                    && entry.message.contains("Last observed readiness was 1/2.")
            }
        }

        let requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/deployments/ember-gate?dryRun=All") })
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/deployments/ember-gate ") })
    }

    func testAuthDoctorDoesNotReportHelmRollbackAsAuthFailure() async throws {
        let previousHelmDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        UserDefaults.standard.runeWriteSafetyRequireHelmDryRun = true
        defer {
            restoreSetting(previousHelmDryRun, forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        }

        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.server.resetRequestLines()

        harness.viewModel.runAuthDoctor()

        try await waitUntil {
            !harness.state.isRunningAuthDoctor
                && harness.state.authDoctorChecks.contains { $0.id == "contexts" }
        }

        XCTAssertFalse(harness.state.authDoctorChecks.contains { $0.id == "helm-rollback-dry-run" })
        XCTAssertFalse(harness.state.authDoctorChecks.contains { check in
            check.message.contains("Native Helm rollback dry-run is not available")
                || check.message.contains("does not run Helm automatically")
        })
        XCTAssertFalse(harness.server.requestLines().contains { $0.localizedCaseInsensitiveContains("helm") })
    }

    func testAuthDoctorIsReadOnlyAgainstFakeCluster() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        let podsBefore = harness.state.pods.map(\.name)
        let deploymentsBefore = harness.state.deployments.map(\.name)
        let servicesBefore = harness.state.services.map(\.name)
        let configMapsBefore = harness.state.configMaps.map(\.name)
        let eventsBefore = harness.state.events.map(\.objectName)
        let yamlBefore = harness.state.resourceYAML
        let describeBefore = harness.state.resourceDescribe
        let logsBefore = harness.state.podLogs
        let writeAuditCountBefore = harness.state.writeAuditLog.count
        harness.server.resetRequestLines()

        harness.viewModel.runAuthDoctor()

        try await waitUntil {
            !harness.state.isRunningAuthDoctor
                && harness.state.authDoctorChecks.contains { $0.id == "pod-list" }
                && harness.state.authDoctorChecks.contains { $0.id == "pod-logs" }
        }

        XCTAssertEqual(harness.state.pods.map(\.name), podsBefore)
        XCTAssertEqual(harness.state.deployments.map(\.name), deploymentsBefore)
        XCTAssertEqual(harness.state.services.map(\.name), servicesBefore)
        XCTAssertEqual(harness.state.configMaps.map(\.name), configMapsBefore)
        XCTAssertEqual(harness.state.events.map(\.objectName), eventsBefore)
        XCTAssertEqual(harness.state.resourceYAML, yamlBefore)
        XCTAssertEqual(harness.state.resourceDescribe, describeBefore)
        XCTAssertEqual(harness.state.podLogs, logsBefore)
        XCTAssertEqual(harness.state.writeAuditLog.count, writeAuditCountBefore)

        let requestLines = harness.server.requestLines()
        XCTAssertFalse(requestLines.isEmpty)
        XCTAssertTrue(requestLines.allSatisfy { line in
            line.hasPrefix("GET ")
                || line.contains("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews")
        })
        XCTAssertFalse(requestLines.contains { line in
            line.hasPrefix("PATCH ")
                || line.hasPrefix("PUT ")
                || line.hasPrefix("DELETE ")
                || (line.hasPrefix("POST ") && !line.contains("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"))
                || line.contains("/exec")
                || line.contains("/portforward")
                || line.contains("/scale")
        })
        XCTAssertNil(harness.state.lastError)
    }

    func testSupportBundleExportDoesNotTouchClusterOrMutateResourceState() async throws {
        let exporter = RecordingFileExporter()
        let harness = try await makeHarness(exporter: exporter)
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        let podsBefore = harness.state.pods.map(\.name)
        let deploymentsBefore = harness.state.deployments.map(\.name)
        let servicesBefore = harness.state.services.map(\.name)
        let configMapsBefore = harness.state.configMaps.map(\.name)
        let eventsBefore = harness.state.events.map(\.objectName)
        let yamlBefore = harness.state.resourceYAML
        let describeBefore = harness.state.resourceDescribe
        let logsBefore = harness.state.podLogs
        let writeAuditCountBefore = harness.state.writeAuditLog.count
        harness.server.resetRequestLines()

        harness.viewModel.saveSupportBundle()

        try await waitUntil {
            exporter.saves.count == 1
        }

        XCTAssertTrue(harness.server.requestLines().isEmpty)
        XCTAssertEqual(harness.state.pods.map(\.name), podsBefore)
        XCTAssertEqual(harness.state.deployments.map(\.name), deploymentsBefore)
        XCTAssertEqual(harness.state.services.map(\.name), servicesBefore)
        XCTAssertEqual(harness.state.configMaps.map(\.name), configMapsBefore)
        XCTAssertEqual(harness.state.events.map(\.objectName), eventsBefore)
        XCTAssertEqual(harness.state.resourceYAML, yamlBefore)
        XCTAssertEqual(harness.state.resourceDescribe, describeBefore)
        XCTAssertEqual(harness.state.podLogs, logsBefore)
        XCTAssertEqual(harness.state.writeAuditLog.count, writeAuditCountBefore)

        let save = try XCTUnwrap(exporter.saves.first)
        XCTAssertTrue(save.suggestedName.hasPrefix("support-bundle-"))
        XCTAssertEqual(save.allowedFileTypes, ["json"])
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: save.data)
        XCTAssertEqual(decoded.resourceCounts["pods"], podsBefore.count)
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

    private func makeHarness(
        fixture: RuneFakeK8sFixture = RuneFakeK8sFixture(),
        kubeClient: KubernetesClient = KubernetesClient(),
        exporter: FileExporting = NoopFileExporter()
    ) async throws -> Harness {
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfig)])
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: kubeClient,
            exporter: exporter,
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

    private func restoreDemoSetting(_ value: Any?) {
        restoreSetting(value, forKey: RuneSettingsKeys.enableDemoCluster)
    }

    private func restoreSetting(_ value: Any?, forKey key: String) {
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
