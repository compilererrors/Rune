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

    func testK9sServiceAccountAndEndpointCommandsLoadRealResourceLists() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreSetting(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }

        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()

        harness.viewModel.setSection(.networking)
        harness.viewModel.setWorkloadKind(.endpoint)
        try await waitUntil {
            harness.state.selectedSection == .networking
                && harness.state.selectedWorkloadKind == .endpoint
                && harness.state.endpoints.map(\.name).contains("orbit-lens")
                && !harness.state.isLoading
        }

        let endpointItem = try XCTUnwrap(harness.viewModel.commandPaletteItems(query: ":ep orbit").first)
        XCTAssertEqual(endpointItem.title, "orbit-lens")
        harness.state.isCommandPalettePresented = true
        harness.viewModel.executeCommandPaletteQuery(":ep orbit")
        XCTAssertFalse(harness.state.isCommandPalettePresented)
        XCTAssertEqual(harness.state.selectedEndpoint?.name, "orbit-lens")

        harness.viewModel.setSection(.rbac)
        harness.viewModel.setWorkloadKind(.serviceAccount)
        try await waitUntil {
            harness.state.selectedSection == .rbac
                && harness.state.selectedWorkloadKind == .serviceAccount
                && harness.state.serviceAccounts.map(\.name).contains("orbit-lens-runner")
                && !harness.state.isLoading
        }

        let serviceAccountItem = try XCTUnwrap(harness.viewModel.commandPaletteItems(query: ":sa orbit").first)
        XCTAssertEqual(serviceAccountItem.title, "orbit-lens-runner")
        harness.state.isCommandPalettePresented = true
        harness.viewModel.executeCommandPaletteQuery(":sa orbit")
        XCTAssertFalse(harness.state.isCommandPalettePresented)
        XCTAssertEqual(harness.state.selectedRBACResource?.name, "orbit-lens-runner")
        XCTAssertNil(harness.state.lastError)
    }

    func testSimpleModeOverviewStillLoadsCoreClusterSnapshot() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreSetting(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }

        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()

        XCTAssertEqual(harness.state.selectedNamespace, "alpha-zone")
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
        XCTAssertTrue(harness.state.overviewEvents.isEmpty)
        XCTAssertNil(harness.state.lastError)
    }

    func testOverviewIgnoresEmptyWarmSnapshotAndFetchesLiveCoreData() async throws {
        let emptyWarmSnapshot = PersistedOverviewSnapshot(
            contextName: RuneFakeK8sFixture.defaultContextName,
            namespace: "alpha-zone",
            fetchedAt: Date(),
            lastAccessedAt: Date(),
            pods: [],
            deploymentsCount: 0,
            servicesCount: 0,
            ingressesCount: 0,
            configMapsCount: 0,
            cronJobsCount: 0,
            nodesCount: 0,
            events: []
        )
        let harness = try await makeHarness(
            overviewSnapshotPersistence: SingleOverviewSnapshotCacheStore(snapshot: emptyWarmSnapshot)
        )
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()

        XCTAssertEqual(harness.state.overviewPods.map(\.name), [
            "ember-gate-75c9f746b8-kq2wm",
            "orbit-lens-6f58d7d89b-hx9q2"
        ])
        XCTAssertEqual(harness.state.overviewDeploymentsCount, 2)
        XCTAssertEqual(harness.state.overviewServicesCount, 2)
        XCTAssertEqual(harness.state.overviewNodesCount, 3)
        XCTAssertNil(harness.state.lastError)
    }

    func testSimpleModeWorkloadsStillLoadsPodsAndDeployments() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreSetting(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }

        let harness = try await makeHarness()
        defer { harness.cleanup() }
        harness.state.selectedSection = .workloads
        harness.state.selectedWorkloadKind = .pod

        try await harness.viewModel.reloadContexts()

        XCTAssertEqual(harness.state.pods.map(\.name), [
            "ember-gate-75c9f746b8-kq2wm",
            "orbit-lens-6f58d7d89b-hx9q2"
        ])

        harness.viewModel.setWorkloadKind(.deployment)

        try await waitUntil {
            harness.state.selectedWorkloadKind == .deployment
                && harness.state.deployments.map(\.name) == ["ember-gate", "orbit-lens"]
                && !harness.state.isLoading
        }

        XCTAssertEqual(harness.state.pods.map(\.name), [
            "ember-gate-75c9f746b8-kq2wm",
            "orbit-lens-6f58d7d89b-hx9q2"
        ])
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

    func testLiveResourceWatchFollowsNamespaceAndStopsWhenDisabled() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }
        harness.state.selectedSection = .workloads
        harness.state.selectedWorkloadKind = .pod
        try await harness.viewModel.reloadContexts()
        harness.server.resetRequestLines()

        harness.viewModel.isLiveStatusUpdatesEnabled = true
        try await waitUntil {
            harness.server.requestLines().contains { line in
                line.contains("/api/v1/namespaces/alpha-zone/pods?") && line.contains("watch=1")
            }
        }

        harness.viewModel.setNamespace("bravo-zone")
        try await waitUntil {
            harness.server.requestLines().contains { line in
                line.contains("/api/v1/namespaces/bravo-zone/pods?") && line.contains("watch=1")
            }
        }

        harness.viewModel.isLiveStatusUpdatesEnabled = false
        let watchCountAfterDisable = harness.server.requestLines().filter { $0.contains("watch=1") }.count
        try await Task.sleep(nanoseconds: 750_000_000)
        XCTAssertEqual(
            harness.server.requestLines().filter { $0.contains("watch=1") }.count,
            watchCountAfterDisable
        )
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
                && harness.state.pods.map(\.name) == [
                    "ember-gate-75c9f746b8-kq2wm",
                    "orbit-lens-6f58d7d89b-hx9q2"
                ]
                && harness.state.replicaSets.map(\.name) == [
                    "ember-gate-rs1",
                    "ember-gate-rs2",
                    "orbit-lens-rs1",
                    "orbit-lens-rs2"
                ]
                && harness.viewModel.selectedDeploymentRelatedPods.map(\.name) == ["ember-gate-75c9f746b8-kq2wm"]
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

    func testEventSourceNavigationHydratesAndSelectsPodAfterOneClick() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.viewModel.setSection(.events)
        try await waitUntil {
            !harness.state.events.isEmpty && !harness.state.isLoading
        }

        let event = try XCTUnwrap(
            harness.state.events.first(where: { $0.involvedKind?.lowercased() == "pod" })
        )
        let context = try XCTUnwrap(harness.state.selectedContext)
        harness.store.clearContext(context)
        harness.state.setPods([])
        harness.state.setOverviewSnapshot(
            pods: [],
            deploymentsCount: 0,
            servicesCount: 0,
            ingressesCount: 0,
            configMapsCount: 0,
            cronJobsCount: 0,
            nodesCount: 0,
            events: []
        )
        harness.state.resourceSearchQuery = event.reason
        harness.server.resetRequestLines()

        harness.viewModel.openEventSource(event)

        try await waitUntil {
            harness.state.selectedSection == .workloads
                && harness.state.selectedWorkloadKind == .pod
                && harness.state.selectedPod?.name == event.objectName
        }

        XCTAssertTrue(
            harness.server.requestLines().contains {
                $0.contains("/api/v1/namespaces/\(harness.state.selectedNamespace)/pods")
            }
        )
        XCTAssertEqual(harness.state.selectedPod?.namespace, event.involvedNamespace)
        XCTAssertTrue(harness.state.resourceSearchQuery.isEmpty)
        XCTAssertNil(harness.state.lastError)
    }

    func testManualNavigationCancelsDelayedEventSourceNavigation() async throws {
        let podListPath = "/api/v1/namespaces/alpha-zone/pods"
        let fixture = RuneFakeK8sFixture(
            delayedResponseTargets: [podListPath: 600_000_000]
        )
        let harness = try await makeHarness(fixture: fixture)
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.viewModel.setSection(.events)
        try await waitUntil {
            !harness.state.events.isEmpty && !harness.state.isLoading
        }

        let event = try XCTUnwrap(
            harness.state.events.first(where: { $0.involvedKind?.lowercased() == "pod" })
        )
        let context = try XCTUnwrap(harness.state.selectedContext)
        harness.store.clearContext(context)
        harness.state.setPods([])
        harness.server.resetRequestLines()

        harness.viewModel.openEventSource(event)
        try await waitUntil {
            harness.server.requestLines().contains { $0.contains("GET \(podListPath) ") }
        }

        harness.viewModel.setSection(.config)
        try await Task.sleep(nanoseconds: 750_000_000)

        XCTAssertEqual(harness.state.selectedSection, .config)
        XCTAssertEqual(harness.state.selectedWorkloadKind, .configMap)
        XCTAssertNil(harness.state.selectedPod)
    }

    func testEventSourceNavigationHydratesPodInEventNamespaceAfterOneClick() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        let event = EventSummary(
            type: "Warning",
            reason: "Pending",
            objectName: "bravo-spoke-59fd6dfb4b-s9n2p",
            message: "Pod is waiting to be scheduled.",
            lastTimestamp: "2026-07-24T00:00:00Z",
            involvedKind: "Pod",
            involvedNamespace: "bravo-zone"
        )
        harness.state.selectedSection = .events
        harness.state.setEvents([event])
        let context = try XCTUnwrap(harness.state.selectedContext)
        harness.store.clearContext(context)
        harness.state.setPods([])
        harness.server.resetRequestLines()

        harness.viewModel.openEventSource(event)

        try await waitUntil {
            harness.state.selectedNamespace == "bravo-zone"
                && harness.state.selectedSection == .workloads
                && harness.state.selectedWorkloadKind == .pod
                && harness.state.selectedPod?.name == event.objectName
        }

        XCTAssertTrue(
            harness.server.requestLines().contains {
                $0.contains("/api/v1/namespaces/bravo-zone/pods")
            }
        )
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

        let searchResult = ResourceLogSearchResult.makeForInspector(
            text: harness.state.podLogs,
            query: "synthetic REST fake log"
        )

        XCTAssertGreaterThan(searchResult.matchRanges.count, 0)
        XCTAssertTrue(searchResult.displayedText.contains("synthetic REST fake log"))
        XCTAssertTrue(harness.state.podLogs.contains("[gate]"))
        XCTAssertTrue(harness.state.podLogs.contains("[metrics]"))
        let requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.contains("/log?") && $0.contains("container=gate") })
        XCTAssertTrue(requestLines.contains { $0.contains("/log?") && $0.contains("container=metrics") })
        XCTAssertFalse(requestLines.contains { $0.contains("allContainers") })
        XCTAssertNil(harness.state.lastResourceYAMLError)
        XCTAssertNil(harness.state.lastResourceDescribeError)
        XCTAssertNil(harness.state.lastError)
    }

    func testConfiguredLogExportsUseLogsLoadedFromFakeCluster() async throws {
        let configuredExporter = RecordingConfiguredExporter()
        let harness = try await makeHarness(configuredExporter: configuredExporter)
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.viewModel.setSection(.workloads)

        try await waitUntil {
            harness.state.selectedSection == .workloads
                && harness.state.selectedWorkloadKind == .pod
                && harness.state.selectedPod?.name == "ember-gate-75c9f746b8-kq2wm"
                && !harness.state.isLoading
        }

        harness.viewModel.reloadLogsForSelection()

        try await waitUntil {
            !harness.state.isLoadingLogs
                && harness.state.podLogs.contains("synthetic REST fake log")
                && harness.state.lastLogFetchError == nil
        }

        harness.server.resetRequestLines()
        harness.viewModel.saveCurrentLogsToExportFolder(openAfterSave: true)
        harness.viewModel.saveVisibleLogsZipToExportFolder(
            visibleText: "visible synthetic REST fake log\n",
            openAfterSave: false
        )

        XCTAssertEqual(configuredExporter.saves.count, 2)

        let currentLogsSave = configuredExporter.saves[0]
        XCTAssertEqual(String(data: currentLogsSave.data, encoding: .utf8), harness.state.podLogs)
        XCTAssertTrue(currentLogsSave.suggestedName.hasPrefix("pod-ember-gate-75c9f746b8-kq2wm-logs-"))
        XCTAssertEqual(currentLogsSave.allowedFileTypes, ["log", "txt"])
        XCTAssertEqual(currentLogsSave.kind, .plainText)
        XCTAssertTrue(currentLogsSave.openAfterSave)

        let visibleZipSave = configuredExporter.saves[1]
        XCTAssertTrue(visibleZipSave.suggestedName.hasPrefix("pod-ember-gate-75c9f746b8-kq2wm-visible-logs-"))
        XCTAssertEqual(visibleZipSave.allowedFileTypes, ["zip"])
        XCTAssertEqual(visibleZipSave.kind, .archive)
        XCTAssertFalse(visibleZipSave.openAfterSave)
        let visibleZipEntries = try ZipArchiveTestSupport.entries(from: visibleZipSave.data)
        XCTAssertTrue(visibleZipEntries.keys.contains { $0.hasSuffix(".log") })
        XCTAssertTrue(visibleZipEntries.values.contains {
            String(data: $0, encoding: .utf8)?.contains("visible synthetic REST fake log") == true
        })

        XCTAssertNil(harness.state.lastLogFetchError)
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

    func testPendingProductionWriteKeepsArmedSourceContextAndNamespaceAfterActiveScopeChanges() async throws {
        let previousProductionConfirmation = UserDefaults.standard.object(
            forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation
        )
        let previousRolloutDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        let previousPostActionVerification = UserDefaults.standard.object(
            forKey: RuneSettingsKeys.writeSafetyRequirePostActionVerification
        )
        let previousCopyableCommand = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireCopyableCommand)
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = true
        UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun = false
        UserDefaults.standard.runeWriteSafetyRequirePostActionVerification = false
        UserDefaults.standard.runeWriteSafetyRequireCopyableCommand = true
        defer {
            restoreSetting(previousProductionConfirmation, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
            restoreSetting(previousRolloutDryRun, forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
            restoreSetting(previousPostActionVerification, forKey: RuneSettingsKeys.writeSafetyRequirePostActionVerification)
            restoreSetting(previousCopyableCommand, forKey: RuneSettingsKeys.writeSafetyRequireCopyableCommand)
        }

        let armedContextName = "synthetic-production-a"
        let activeContextName = "synthetic-dev-b"
        let armedServer = try await RuneFakeK8sRESTServer.start(
            fixture: renamedDefaultFixture(contextName: armedContextName),
            contextName: armedContextName
        )
        let activeServer = try await RuneFakeK8sRESTServer.start(
            fixture: renamedDefaultFixture(contextName: activeContextName),
            contextName: activeContextName
        )
        let armedKubeconfig = try writeKubeconfig(armedServer.kubeconfigYAML())
        let activeKubeconfig = try writeKubeconfig(activeServer.kubeconfigYAML())
        defer {
            armedServer.stop()
            activeServer.stop()
            try? FileManager.default.removeItem(at: armedKubeconfig)
            try? FileManager.default.removeItem(at: activeKubeconfig)
        }

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: armedKubeconfig)])
        state.selectedContext = KubeContext(name: "synthetic-production-a")
        state.selectedNamespace = "alpha-zone"
        state.selectedWorkloadKind = .deployment
        state.setSelectedDeployment(DeploymentSummary(
            name: "orbit-lens",
            namespace: "alpha-zone",
            readyReplicas: 2,
            desiredReplicas: 2,
            selector: ["app": "orbit-lens"]
        ))
        let viewModel = RuneAppViewModel(
            state: state,
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        viewModel.rolloutRevisionInput = "1"

        viewModel.requestRolloutUndoSelectedDeployment()

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Review Production Action")
        XCTAssertEqual(
            viewModel.pendingWriteActionKubectlCommand,
            "kubectl --context synthetic-production-a --namespace alpha-zone rollout undo deployment orbit-lens --to-revision=1"
        )

        state.setSources([KubeConfigSource(url: activeKubeconfig)])
        state.selectedContext = KubeContext(name: "synthetic-dev-b")
        state.selectedNamespace = "bravo-zone"

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Review Production Action")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("PRODUCTION CONTEXT"))
        XCTAssertTrue(viewModel.pendingWriteActionKubectlCommand.contains("synthetic-production-a --namespace alpha-zone"))

        viewModel.confirmPendingWriteAction()

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Rollback")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Final confirmation required"))
        XCTAssertTrue(armedServer.requestLines().isEmpty)
        XCTAssertTrue(activeServer.requestLines().isEmpty)

        viewModel.confirmPendingWriteAction()

        try await waitUntil {
            state.writeAuditLog.contains { entry in
                entry.action == "Rollout Undo"
                    && entry.contextName == "synthetic-production-a"
                    && entry.namespace == "alpha-zone"
                    && entry.status == "Succeeded"
            }
        }

        XCTAssertTrue(armedServer.requestLines().contains { line in
            line.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens ")
        })
        XCTAssertTrue(activeServer.requestLines().isEmpty)
        XCTAssertEqual(state.selectedContext?.name, "synthetic-dev-b")
        XCTAssertEqual(state.selectedNamespace, "bravo-zone")
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

        try await waitUntil(timeout: 15) {
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

    func testScaleAndRolloutRestartAuditAgainstFakeCluster() async throws {
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
        harness.server.resetRequestLines()

        harness.viewModel.scaleReplicaInput = 4
        harness.viewModel.requestScaleSelectedDeployment()

        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Scale")
        XCTAssertTrue(harness.viewModel.pendingWriteActionKubectlCommand.contains("scale deployment orbit-lens --replicas 4"))

        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Scale"
                    && entry.status == "Succeeded"
                    && entry.resource == "deployment/orbit-lens replicas=4"
            }
        }

        var requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens/scale ") })

        harness.server.resetRequestLines()
        harness.viewModel.requestRolloutRestartSelectedDeployment()

        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Restart")
        XCTAssertTrue(harness.viewModel.pendingWriteActionKubectlCommand.contains("rollout restart deployment orbit-lens"))

        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Rollout Restart"
                    && entry.status == "Succeeded"
                    && entry.resource == "deployment/orbit-lens"
            }
        }

        requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens ") })

        harness.server.resetRequestLines()
        harness.viewModel.setWorkloadKind(.statefulSet)

        try await waitUntil {
            harness.state.statefulSets.map(\.name) == ["ledger-store"]
                && !harness.state.isLoading
        }

        let statefulSet = try XCTUnwrap(harness.state.statefulSets.first)
        harness.viewModel.selectStatefulSet(statefulSet)
        XCTAssertEqual(harness.viewModel.scaleReplicaInput, 2)

        harness.viewModel.scaleReplicaInput = 3
        harness.viewModel.requestScaleSelectedStatefulSet()

        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Scale")
        XCTAssertTrue(harness.viewModel.pendingWriteActionKubectlCommand.contains("scale statefulset ledger-store --replicas 3"))

        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Scale"
                    && entry.status == "Succeeded"
                    && entry.resource == "statefulset/ledger-store replicas=3"
            }
        }

        requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/statefulsets/ledger-store/scale ") })

        harness.server.resetRequestLines()
        harness.viewModel.requestRolloutRestartSelectedStatefulSet()

        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Restart")
        XCTAssertTrue(harness.viewModel.pendingWriteActionKubectlCommand.contains("rollout restart statefulset ledger-store"))

        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Rollout Restart"
                    && entry.status == "Succeeded"
                    && entry.resource == "statefulset/ledger-store"
            }
        }

        requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("PATCH /apis/apps/v1/namespaces/alpha-zone/statefulsets/ledger-store ") })
    }

    func testCronJobManualTriggerAuditsCreateJobAgainstFakeCluster() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.viewModel.setSection(.workloads)
        harness.viewModel.setWorkloadKind(.cronJob)

        try await waitUntil {
            harness.state.cronJobs.map(\.name) == ["ember-gate-report"]
                && !harness.state.isLoading
        }

        let cronJob = try XCTUnwrap(harness.state.cronJobs.first)
        harness.viewModel.selectCronJob(cronJob)
        harness.server.resetRequestLines()

        harness.viewModel.createManualJobFromSelectedCronJob()

        XCTAssertEqual(harness.viewModel.pendingWriteActionConfirmLabel, "Create Job")
        XCTAssertTrue(harness.viewModel.pendingWriteActionMessage.contains("ember-gate-report-manual-"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionKubectlCommand.contains("create job ember-gate-report-manual-"))
        XCTAssertTrue(harness.viewModel.pendingWriteActionKubectlCommand.contains("--from=cronjob/ember-gate-report"))

        harness.viewModel.confirmPendingWriteAction()

        try await waitUntil {
            harness.state.writeAuditLog.contains { entry in
                entry.action == "Create Job"
                    && entry.status == "Succeeded"
                    && entry.resource.contains("cronjob/ember-gate-report -> job/ember-gate-report-manual-")
            }
        }

        let requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("GET /apis/batch/v1/namespaces/alpha-zone/cronjobs/ember-gate-report ") })
        XCTAssertTrue(requestLines.contains { $0.hasPrefix("POST /apis/batch/v1/namespaces/alpha-zone/jobs ") })
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

    func testAuthDoctorScopeChangesCancelOldRunWithoutClearingNewRun() async throws {
        let fixture = RuneFakeK8sFixture(
            delayedResponseTargets: ["/api/v1/namespaces": 600_000_000]
        )
        let harness = try await makeHarness(fixture: fixture)
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()
        harness.server.resetRequestLines()
        harness.viewModel.runAuthDoctor()
        try await waitUntil {
            harness.server.requestLines().contains { $0.hasPrefix("GET /api/v1/namespaces ") }
        }

        harness.state.selectedContext = KubeContext(name: "synthetic-other-context")
        await Task.yield()

        XCTAssertFalse(harness.state.isRunningAuthDoctor)
        XCTAssertTrue(harness.state.authDoctorChecks.isEmpty)

        harness.state.selectedContext = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        harness.state.selectedNamespace = "alpha-zone"
        harness.server.resetRequestLines()
        harness.viewModel.runAuthDoctor()
        try await waitUntil {
            harness.server.requestLines().contains { $0.hasPrefix("GET /api/v1/namespaces ") }
        }

        harness.state.selectedNamespace = "bravo-zone"
        await Task.yield()
        XCTAssertFalse(harness.state.isRunningAuthDoctor)
        XCTAssertTrue(harness.state.authDoctorChecks.isEmpty)

        harness.viewModel.runAuthDoctor()
        XCTAssertTrue(harness.state.isRunningAuthDoctor)
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertTrue(
            harness.state.isRunningAuthDoctor,
            "A cancelled older run must not clear the replacement run's progress state."
        )

        try await waitUntil(timeout: 5) {
            !harness.state.isRunningAuthDoctor
                && harness.state.authDoctorChecks.contains {
                    $0.id == "namespace" && $0.message == "bravo-zone"
                }
        }
        XCTAssertTrue(harness.state.authDoctorChecks.contains {
            $0.id == "pod-list" && $0.message.contains("bravo-zone")
        })

        harness.state.selectedContext = KubeContext(name: "synthetic-completed-run-context")
        await Task.yield()
        XCTAssertTrue(
            harness.state.authDoctorChecks.isEmpty,
            "A completed Auth Doctor run must not remain visible or exportable after its scope changes."
        )
    }

    func testAuthDoctorLocalInspectionUsesSelectedContextInsteadOfKubeconfigCurrentContext() async throws {
        let harness = try await makeHarness()
        defer { harness.cleanup() }
        let endpoint = "http://127.0.0.1:\(harness.server.port)"
        try """
        apiVersion: v1
        kind: Config
        current-context: current-exec
        clusters:
        - name: current-cluster
          cluster:
            server: \(endpoint)
        - name: selected-cluster
          cluster:
            server: \(endpoint)
        contexts:
        - name: current-exec
          context:
            cluster: current-cluster
            namespace: alpha-zone
            user: exec-user
        - name: selected-static
          context:
            cluster: selected-cluster
            namespace: alpha-zone
            user: static-user
        users:
        - name: exec-user
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: synthetic-inactive-credential-helper
              interactiveMode: Never
        - name: static-user
          user:
            token: fake-token
        """.write(to: harness.kubeconfigURL, atomically: true, encoding: .utf8)
        harness.state.selectedContext = KubeContext(name: "selected-static")

        harness.viewModel.runAuthDoctor()

        try await waitUntil {
            !harness.state.isRunningAuthDoctor
                && harness.state.authDoctorChecks.contains { $0.id == "selected-context" }
        }

        XCTAssertEqual(
            harness.state.authDoctorChecks.first { $0.id == "selected-context" }?.message,
            "selected-static"
        )
        XCTAssertFalse(harness.state.authDoctorChecks.contains { $0.id == "exec-auth-profile" })
        XCTAssertFalse(harness.state.authDoctorChecks.contains { $0.id == "exec-auth-tools" })
        XCTAssertFalse(harness.state.authDoctorChecks.map(\.message).joined(separator: "\n")
            .contains("synthetic-inactive-credential-helper"))
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

    func testAuthDoctorReportsPartialRBACWithoutBreakingCoreClusterLoad() async throws {
        let fixture = RuneFakeK8sFixture(selfSubjectAccessReviewDenials: [
            RuneFakeK8sRBACRule(namespace: "alpha-zone", verb: "get", resource: "pods", subresource: "log"),
            RuneFakeK8sRBACRule(namespace: "alpha-zone", verb: "create", resource: "pods", subresource: "exec"),
            RuneFakeK8sRBACRule(namespace: "alpha-zone", verb: "create", resource: "pods", subresource: "portforward")
        ])
        let harness = try await makeHarness(fixture: fixture)
        defer { harness.cleanup() }

        try await harness.viewModel.reloadContexts()

        XCTAssertEqual(harness.state.selectedNamespace, "alpha-zone")
        XCTAssertEqual(harness.state.pods.map(\.name), [
            "ember-gate-75c9f746b8-kq2wm",
            "orbit-lens-6f58d7d89b-hx9q2"
        ])
        XCTAssertEqual(harness.state.deployments.map(\.name), ["ember-gate", "orbit-lens"])
        harness.server.resetRequestLines()

        harness.viewModel.runAuthDoctor()

        try await waitUntil {
            !harness.state.isRunningAuthDoctor
                && harness.state.authDoctorChecks.contains { $0.id == "rbac-access-summary" }
                && harness.state.authDoctorChecks.contains { $0.id == "pod-logs" }
        }

        let checksByID = harness.state.authDoctorChecks.reduce(into: [String: RuneHealthCheck]()) { checks, check in
            checks[check.id] = checks[check.id] ?? check
        }
        XCTAssertEqual(checksByID["rbac-pods-list"]?.status, .passed)
        XCTAssertEqual(checksByID["rbac-pod-logs"]?.status, .warning)
        XCTAssertEqual(checksByID["rbac-pod-exec"]?.status, .warning)
        XCTAssertEqual(checksByID["rbac-port-forward"]?.status, .warning)
        XCTAssertEqual(checksByID["pod-logs"]?.status, .warning)
        XCTAssertTrue(checksByID["rbac-access-summary"]?.message.contains("Partial pod access") == true)
        XCTAssertEqual(harness.state.pods.map(\.name), [
            "ember-gate-75c9f746b8-kq2wm",
            "orbit-lens-6f58d7d89b-hx9q2"
        ])
        XCTAssertEqual(harness.state.deployments.map(\.name), ["ember-gate", "orbit-lens"])
        XCTAssertNil(harness.state.lastError)

        let requestLines = harness.server.requestLines()
        XCTAssertTrue(requestLines.contains { $0.contains("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews") })
        XCTAssertTrue(requestLines.contains { $0.contains("/pods/ember-gate-75c9f746b8-kq2wm/log") })
        XCTAssertFalse(requestLines.contains { $0.hasPrefix("PATCH ") || $0.contains("/exec") || $0.contains("/scale") })
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
        let store: ResourceStore
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
        exporter: FileExporting = NoopFileExporter(),
        configuredExporter: ConfiguredExporting = NoopConfiguredExporter(),
        overviewSnapshotPersistence: any OverviewSnapshotCacheStoring = NoopOverviewSnapshotCacheStore()
    ) async throws -> Harness {
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfig)])
        let store = ResourceStore()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: kubeClient,
            store: store,
            exporter: exporter,
            configuredExporter: configuredExporter,
            overviewSnapshotPersistence: overviewSnapshotPersistence,
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        return Harness(
            server: server,
            kubeconfigURL: kubeconfig,
            store: store,
            state: state,
            viewModel: viewModel
        )
    }

    private actor SingleOverviewSnapshotCacheStore: OverviewSnapshotCacheStoring {
        private let snapshot: PersistedOverviewSnapshot

        init(snapshot: PersistedOverviewSnapshot) {
            self.snapshot = snapshot
        }

        func loadSnapshot(contextName: String, namespace: String, maxAge: TimeInterval) async -> PersistedOverviewSnapshot? {
            guard snapshot.contextName == contextName,
                  snapshot.namespace == namespace else {
                return nil
            }
            return snapshot
        }

        func saveSnapshot(_ snapshot: PersistedOverviewSnapshot) async {}
    }

    private func writeKubeconfig(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-ui-fake-cluster-\(UUID().uuidString).yaml")
        try contents.write(to: url, atomically: true, encoding: .utf8)
        return url
    }

    private func renamedDefaultFixture(contextName: String) -> RuneFakeK8sFixture {
        let source = RuneFakeK8sFixture.defaultContexts[0]
        return RuneFakeK8sFixture(contexts: [
            RuneFakeK8sCluster(
                contextName: contextName,
                defaultNamespace: source.defaultNamespace,
                namespaces: source.namespaces,
                nodes: source.nodes,
                operatorResources: source.operatorResources
            )
        ])
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

private final class NoopConfiguredExporter: ConfiguredExporting {
    @MainActor
    func save(
        data: Data,
        suggestedName: String,
        allowedFileTypes: [String],
        kind: ConfiguredExportFileKind,
        openAfterSave: Bool
    ) throws -> URL {
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

@MainActor
private final class RecordingConfiguredExporter: ConfiguredExporting {
    struct Save {
        let data: Data
        let suggestedName: String
        let allowedFileTypes: [String]
        let kind: ConfiguredExportFileKind
        let openAfterSave: Bool
    }

    private(set) var saves: [Save] = []

    func save(
        data: Data,
        suggestedName: String,
        allowedFileTypes: [String],
        kind: ConfiguredExportFileKind,
        openAfterSave: Bool
    ) throws -> URL {
        saves.append(Save(
            data: data,
            suggestedName: suggestedName,
            allowedFileTypes: allowedFileTypes,
            kind: kind,
            openAfterSave: openAfterSave
        ))
        return FileManager.default.temporaryDirectory.appendingPathComponent(suggestedName)
    }
}
