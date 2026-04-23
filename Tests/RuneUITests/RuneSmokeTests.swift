import Foundation
import XCTest
@testable import RuneCore
@testable import RuneDiagnostics
@testable import RuneExport
@testable import RuneHelm
@testable import RuneKube
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

@MainActor
final class RuneSmokeTests: XCTestCase {
    nonisolated(unsafe) private var originalDiagnosticsLogging: Any?

    override func setUp() {
        super.setUp()
        originalDiagnosticsLogging = UserDefaults.standard.object(forKey: RuneSettingsKeys.diagnosticsLogging)
        UserDefaults.standard.set(false, forKey: RuneSettingsKeys.diagnosticsLogging)
    }

    override func tearDown() {
        if let originalDiagnosticsLogging {
            UserDefaults.standard.set(originalDiagnosticsLogging, forKey: RuneSettingsKeys.diagnosticsLogging)
        } else {
            UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.diagnosticsLogging)
        }
        super.tearDown()
    }

    func testKubeConfigDiscovererUsesKubeconfigAndDefaultPath() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("rune-discovery-\(UUID().uuidString)", isDirectory: true)
        let kubeDirectory = root.appendingPathComponent(".kube", isDirectory: true)
        let envConfig = root.appendingPathComponent("env-config.yaml")
        let defaultConfig = kubeDirectory.appendingPathComponent("config")

        try FileManager.default.createDirectory(at: kubeDirectory, withIntermediateDirectories: true, attributes: nil)
        try "apiVersion: v1\n".write(to: envConfig, atomically: true, encoding: .utf8)
        try "apiVersion: v1\n".write(to: defaultConfig, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(at: root) }

        let discoverer = KubeConfigDiscoverer(
            environmentProvider: { ["KUBECONFIG": "\(envConfig.path):\(envConfig.path)"] },
            homeDirectoryProvider: { root },
            fileExists: { path in FileManager.default.fileExists(atPath: path) }
        )

        let paths = discoverer.discoverCandidateFiles().map(\.path)
        XCTAssertEqual(paths, [defaultConfig.path, envConfig.path].sorted())
    }

    func testBootstrapAutoLoadsDiscoveredKubeconfig() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(
            runner: runner,
            kubeConfigDiscoverer: StaticKubeConfigDiscoverer(urls: [kubeconfigURL])
        )

        viewModel.bootstrap()

        try await waitUntil {
            !viewModel.state.contexts.isEmpty
        }

        XCTAssertEqual(
            viewModel.state.kubeConfigSources.map { URL(fileURLWithPath: $0.path).standardizedFileURL.path },
            [kubeconfigURL.standardizedFileURL.path]
        )
        XCTAssertEqual(viewModel.state.contexts.first?.name, "prod-main")
    }

    func testBootstrapFallsBackToDirectDiscoveredSourcesWhenBookmarkSaveFails() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(
            runner: runner,
            bookmarkManager: BookmarkManager(store: FailingSaveBookmarkStore()),
            kubeConfigDiscoverer: StaticKubeConfigDiscoverer(urls: [kubeconfigURL])
        )

        viewModel.bootstrap()

        try await waitUntil {
            !viewModel.state.contexts.isEmpty
        }

        XCTAssertEqual(viewModel.state.kubeConfigSources.count, 1)
        XCTAssertEqual(viewModel.state.contexts.first?.name, "prod-main")
    }

    func testBootstrapFallsBackToDirectDiscoveredSourcesWhenBookmarkLoadFails() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(
            runner: runner,
            bookmarkManager: BookmarkManager(store: FailingLoadBookmarkStore()),
            kubeConfigDiscoverer: StaticKubeConfigDiscoverer(urls: [kubeconfigURL])
        )

        viewModel.bootstrap()

        try await waitUntil {
            !viewModel.state.contexts.isEmpty
        }

        XCTAssertEqual(viewModel.state.kubeConfigSources.count, 1)
        XCTAssertEqual(viewModel.state.contexts.first?.name, "prod-main")
    }

    func testBootstrapWithoutDiscoveredConfigsDoesNotSetMissingConfigError() async throws {
        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(
            runner: runner,
            kubeConfigDiscoverer: StaticKubeConfigDiscoverer(urls: [])
        )

        viewModel.bootstrap()
        try await Task.sleep(nanoseconds: 100_000_000)

        XCTAssertNil(viewModel.state.lastError)
        XCTAssertTrue(viewModel.state.kubeConfigSources.isEmpty)
        XCTAssertTrue(viewModel.state.contexts.isEmpty)
    }

    func testUnifiedServiceLogsSmokeFlow() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let service = viewModel.state.services.first(where: { $0.name == "api-svc" }) else {
            XCTFail("Expected api-svc")
            return
        }

        viewModel.setWorkloadKind(.service)
        viewModel.selectService(service)

        try await waitUntil {
            !viewModel.state.unifiedServiceLogs.isEmpty
        }

        XCTAssertTrue(viewModel.state.unifiedServiceLogs.contains("[api-0]"))
        XCTAssertTrue(viewModel.state.unifiedServiceLogs.contains("[api-1]"))
        XCTAssertEqual(viewModel.state.unifiedServiceLogPods, ["api-0", "api-1"])

        let unifiedArgs = ["kubectl"] + builder.podsByLabelSelectorArguments(context: "prod-main", namespace: "default", selector: "app=api")
        let calledSelector = await runner.didRun(arguments: unifiedArgs)
        XCTAssertTrue(calledSelector)
    }

    func testPreviousLogsUnavailableShowsFriendlyMessage() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.podLogsArguments(
            context: "prod-main",
            namespace: "default",
            podName: "api-0",
            container: nil,
            filter: .tailLines(200),
            previous: true,
            follow: false
        ))] = CommandResult(
            stdout: "",
            stderr: "Error from server (BadRequest): previous terminated container \"api\" in pod \"api-0\" not found",
            exitCode: 1
        )

        let runner = ScriptedCommandRunner(script: script)
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let pod = viewModel.state.pods.first(where: { $0.name == "api-0" }) else {
            XCTFail("Expected api-0 pod")
            return
        }

        viewModel.setWorkloadKind(.pod)
        viewModel.selectPod(pod)
        viewModel.includePreviousLogs = true

        try await waitUntil {
            viewModel.state.podLogs.contains("No previous logs available")
        }

        XCTAssertNil(viewModel.state.lastError)
    }

    func testChangingLogPresetCancelsInFlightFetchAndCommitsOnlyLatestLogs() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let slowArgs = ["kubectl"] + builder.podLogsArguments(
            context: "prod-main",
            namespace: "default",
            podName: "api-0",
            container: nil,
            filter: .lastMinutes(5),
            previous: false,
            follow: false
        )
        let latestArgs = ["kubectl"] + builder.podLogsArguments(
            context: "prod-main",
            namespace: "default",
            podName: "api-0",
            container: nil,
            filter: .lastMinutes(15),
            previous: false,
            follow: false
        )
        let runner = CancellableLogCommandRunner(
            script: baseScript(builder: builder),
            slowLogArguments: slowArgs,
            latestLogArguments: latestArgs
        )
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let pod = viewModel.state.pods.first(where: { $0.name == "api-0" }) else {
            XCTFail("Expected api-0 pod")
            return
        }

        viewModel.setWorkloadKind(.pod)
        viewModel.selectPod(pod)
        viewModel.selectedLogPreset = .last5Minutes

        try await waitUntil {
            await runner.didStartSlowLogFetch()
        }

        viewModel.selectedLogPreset = .last15Minutes

        try await waitUntil(timeout: 3.0) {
            await runner.didCancelSlowLogFetch()
                && viewModel.state.podLogs.contains("latest 15m logs")
        }

        let didRunLatestFetch = await runner.didRun(arguments: latestArgs)
        XCTAssertFalse(viewModel.state.podLogs.contains("stale 5m logs"))
        XCTAssertTrue(didRunLatestFetch)
    }

    func testAdditionalResourceTypesLoadIntoState() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.statefulSet)
        try await waitUntil {
            viewModel.state.statefulSets.first?.name == "api-db"
        }

        viewModel.setWorkloadKind(.daemonSet)
        try await waitUntil {
            viewModel.state.daemonSets.first?.name == "node-agent"
        }

        viewModel.setSection(.config)
        viewModel.setWorkloadKind(.configMap)
        try await waitUntil {
            viewModel.state.configMaps.first?.name == "app-config"
        }

        viewModel.setWorkloadKind(.secret)
        try await waitUntil {
            viewModel.state.secrets.first?.name == "db-credentials"
        }

        viewModel.setSection(.storage)
        viewModel.setWorkloadKind(.node)
        try await waitUntil {
            viewModel.state.nodes.first?.name == "worker-1"
        }

        XCTAssertEqual(viewModel.state.statefulSets.first?.name, "api-db")
        XCTAssertEqual(viewModel.state.daemonSets.first?.name, "node-agent")
        XCTAssertEqual(viewModel.state.configMaps.first?.name, "app-config")
        XCTAssertEqual(viewModel.state.secrets.first?.name, "db-credentials")
        XCTAssertEqual(viewModel.state.nodes.first?.name, "worker-1")
    }

    func testOverviewLoadsNamespaceScopedCounts() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        XCTAssertEqual(viewModel.state.overviewPods.count, 2)
        XCTAssertEqual(viewModel.state.overviewDeploymentsCount, 1)
        XCTAssertEqual(viewModel.state.overviewServicesCount, 1)
        XCTAssertEqual(viewModel.state.overviewIngressesCount, 1)
        XCTAssertEqual(viewModel.state.overviewConfigMapsCount, 1)
        XCTAssertEqual(viewModel.state.overviewNodesCount, 1)
        XCTAssertEqual(viewModel.state.overviewEvents.count, 1)
    }

    func testOverviewFromEventsColdStartRefreshesPodsInsteadOfKeepingZero() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        // Reproduces startup in Events section where pod rows are not part of the first snapshot.
        viewModel.state.selectedSection = .events
        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        viewModel.setSection(.overview)

        try await waitUntil {
            viewModel.state.selectedSection == .overview
                && viewModel.state.overviewPods.count == 2
                && viewModel.state.overviewDeploymentsCount == 1
                && viewModel.state.overviewServicesCount == 1
        }
    }

    func testNavigationBackForwardRestoresView() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        try await waitUntil {
            viewModel.state.selectedSection == .overview
        }

        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.deployment)
        try await waitUntil {
            viewModel.state.selectedSection == .workloads
                && viewModel.state.selectedWorkloadKind == .deployment
        }

        viewModel.setSection(.networking)
        viewModel.setWorkloadKind(.service)
        try await waitUntil {
            viewModel.state.selectedSection == .networking
                && viewModel.state.selectedWorkloadKind == .service
        }

        XCTAssertTrue(viewModel.canNavigateBack)
        viewModel.navigateBack()
        try await waitUntil {
            viewModel.state.selectedSection == .workloads
                && viewModel.state.selectedWorkloadKind == .deployment
        }

        XCTAssertTrue(viewModel.canNavigateForward)
        viewModel.navigateForward()
        try await waitUntil {
            viewModel.state.selectedSection == .networking
                && viewModel.state.selectedWorkloadKind == .service
        }
    }

    func testNamespacesLoadFromCluster() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        XCTAssertEqual(viewModel.state.namespaces, ["default", "kube-system", "platform"])
        XCTAssertEqual(viewModel.namespaceOptions, ["default", "kube-system", "platform"])
    }

    func testContextSwitchResolvesNamespaceBeforeNamespacedLoads() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)

        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "prod-main\nqa-main\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespaceListArguments(context: "qa-main"))] = CommandResult(
            stdout: "kube-system\nqa\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: """
            {"items":[{"metadata":{"name":"qa-api-0","namespace":"qa","creationTimestamp":"2024-06-01T12:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}}]}
            """,
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "qa-api-0   Running\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podTopArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "qa-api-0   2m   8Mi\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.deploymentListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.statefulSetListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.daemonSetListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.serviceListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.ingressListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.configMapListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.secretListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.persistentVolumeClaimListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.persistentVolumeListArguments(context: "qa-main"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.storageClassListArguments(context: "qa-main"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.horizontalPodAutoscalerListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.networkPolicyListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.eventListArguments(context: "qa-main", namespace: "qa"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "qa-main", namespace: "qa", resource: "deployments"))] = CommandResult(
            stdout: "",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "qa-main", namespace: "qa", resource: "services"))] = CommandResult(
            stdout: "",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "qa-main", namespace: "qa", resource: "ingresses"))] = CommandResult(
            stdout: "",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "qa-main", namespace: "qa", resource: "configmaps"))] = CommandResult(
            stdout: "",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "qa-main", namespace: "qa", resource: "cronjobs"))] = CommandResult(
            stdout: "",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.clusterResourceCountArguments(context: "qa-main", resource: "nodes"))] = CommandResult(
            stdout: "worker-qa-1\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.nodeListArguments(context: "qa-main"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podListAllNamespacesArguments(context: "qa-main"))] = CommandResult(
            stdout: """
            {"items":[{"metadata":{"name":"qa-api-0","namespace":"qa","creationTimestamp":"2024-06-01T12:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}}]}
            """,
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podTopAllNamespacesArguments(context: "qa-main"))] = CommandResult(
            stdout: "qa   qa-api-0   2m   8Mi\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.deploymentListAllNamespacesArguments(context: "qa-main"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.serviceListAllNamespacesArguments(context: "qa-main"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.ingressListAllNamespacesArguments(context: "qa-main"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.configMapListAllNamespacesArguments(context: "qa-main"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.eventListAllNamespacesArguments(context: "qa-main"))] = CommandResult(
            stdout: "{\"items\":[]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podLogsArguments(
            context: "qa-main",
            namespace: "qa",
            podName: "qa-api-0",
            container: nil,
            filter: .tailLines(200),
            previous: false,
            follow: false
        ))] = CommandResult(
            stdout: "2026-04-16T00:00:03Z started qa-api-0",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.resourceYAMLArguments(context: "qa-main", namespace: "qa", kind: .pod, name: "qa-api-0"))] = CommandResult(
            stdout: "kind: Pod\nmetadata:\n  name: qa-api-0\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.describeResourceArguments(context: "qa-main", namespace: "qa", kind: .pod, name: "qa-api-0"))] = CommandResult(
            stdout: "Name: qa-api-0\nNamespace: qa\n",
            stderr: "",
            exitCode: 0
        )

        let runner = ScriptedCommandRunner(script: script)
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        viewModel.setContext(KubeContext(name: "qa-main"))

        try await waitUntil {
            viewModel.state.selectedContext?.name == "qa-main"
                && viewModel.state.selectedNamespace == "qa"
                && viewModel.state.namespaces == ["kube-system", "qa"]
                && viewModel.state.pods.first?.name == "qa-api-0"
        }

        let staleNamespaceArgs = ["kubectl"] + builder.podListArguments(context: "qa-main", namespace: "default")
        let didRunWithStaleNamespace = await runner.didRun(arguments: staleNamespaceArgs)
        XCTAssertFalse(didRunWithStaleNamespace)
    }

    func testContextSwitchDoesNotLeakOldContextResourcesWhenLoadsFail() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "prod-main\nqa-main\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespaceListArguments(context: "qa-main"))] = CommandResult(
            stdout: "qa\n",
            stderr: "",
            exitCode: 0
        )

        let runner = ScriptedCommandRunner(script: script)
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        XCTAssertFalse(viewModel.state.pods.isEmpty)

        viewModel.setContext(KubeContext(name: "qa-main"))

        try await waitUntil {
            viewModel.state.selectedContext?.name == "qa-main"
                && viewModel.state.selectedNamespace == "qa"
        }

        XCTAssertTrue(viewModel.state.pods.isEmpty)
        XCTAssertEqual(viewModel.state.overviewPods.count, 0)
    }

    func testSnapshotKeepsPodsWhenOneResourceFails() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script.removeValue(forKey: key(["kubectl"] + builder.serviceListArguments(context: "prod-main", namespace: "default")))

        let runner = ScriptedCommandRunner(script: script)
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        XCTAssertEqual(viewModel.state.pods.count, 2)
        XCTAssertEqual(viewModel.state.pods.first?.name, "api-0")
        XCTAssertTrue(viewModel.state.lastError?.contains("Partial load") == true)
        XCTAssertTrue(viewModel.state.lastError?.contains("services") == true)
    }

    func testSnapshotDoesNotReuseOldNamespacesWhenNamespaceLoadFails() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script.removeValue(forKey: key(["kubectl"] + builder.namespaceListArguments(context: "prod-main")))

        let runner = ScriptedCommandRunner(script: script)
        let viewModel = makeViewModel(runner: runner)
        viewModel.state.setNamespaces(["old-ns"])

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        XCTAssertTrue(viewModel.state.namespaces.isEmpty)
        XCTAssertTrue(viewModel.namespaceOptions.contains("default"))
        XCTAssertTrue(viewModel.state.lastError?.contains("namespaces") == true)
    }

    func testSnapshotDoesNotExposeDiskHydratedNamespacesWhenLiveNamespaceFetchFails() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "lattice-zone\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.contextNamespaceArguments(context: "lattice-zone"))] = CommandResult(
            stdout: "delta-zone",
            stderr: "",
            exitCode: 0
        )
        script.removeValue(forKey: key(["kubectl"] + builder.namespaceListArguments(context: "lattice-zone")))

        let runner = ScriptedCommandRunner(script: script)
        let namespacePersistence = InMemoryNamespaceListPersistenceStore(
            values: ["lattice-zone": ["echo-zone", "delta-zone", "default"]]
        )
        let viewModel = makeViewModel(
            runner: runner,
            namespaceListPersistence: namespacePersistence
        )

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        XCTAssertEqual(viewModel.state.selectedNamespace, "delta-zone")
        XCTAssertTrue(viewModel.state.namespaces.isEmpty)
        XCTAssertFalse(viewModel.namespaceOptions.contains("echo-zone"))
    }

    func testContextSwitchPrefersContextDefaultWhenNamespaceListUnavailable() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "prod-main\nqa-main\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.contextNamespaceArguments(context: "qa-main"))] = CommandResult(
            stdout: "delta-zone",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "qa-main", namespace: "delta-zone"))] = CommandResult(
            stdout: "delta-pod-0   Running\n",
            stderr: "",
            exitCode: 0
        )

        let runner = ScriptedCommandRunner(script: script)
        let preferences = InMemoryContextPreferencesStore(
            preferredNamespaces: ["qa-main": "foxtrot-zone"]
        )
        let viewModel = makeViewModel(runner: runner, contextPreferences: preferences)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        viewModel.setContext(KubeContext(name: "qa-main"))

        try await waitUntil {
            viewModel.state.selectedContext?.name == "qa-main"
                && viewModel.state.selectedNamespace == "delta-zone"
        }

        let staleNamespaceArgs = ["kubectl"] + builder.podStatusListArguments(
            context: "qa-main",
            namespace: "foxtrot-zone"
        )
        let usedStaleNamespace = await runner.didRun(arguments: staleNamespaceArgs)
        XCTAssertFalse(usedStaleNamespace)

        let usedContextDefaultArgs = ["kubectl"] + builder.podStatusListArguments(
            context: "qa-main",
            namespace: "delta-zone"
        )
        let usedContextDefaultNamespace = await runner.didRun(arguments: usedContextDefaultArgs)
        XCTAssertTrue(usedContextDefaultNamespace)
    }

    func testReloadContextsUsesContextDefaultInsteadOfSavedNamespaceForNewSelection() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "lattice-zone\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.contextNamespaceArguments(context: "lattice-zone"))] = CommandResult(
            stdout: "delta-zone",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespaceListArguments(context: "lattice-zone"))] = CommandResult(
            stdout: "default\ndelta-zone\necho-zone\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "lattice-zone", namespace: "delta-zone"))] = CommandResult(
            stdout: "delta-pod-0   Running\n",
            stderr: "",
            exitCode: 0
        )

        let runner = ScriptedCommandRunner(script: script)
        let preferences = InMemoryContextPreferencesStore(
            preferredNamespaces: ["lattice-zone": "echo-zone"]
        )
        let viewModel = makeViewModel(runner: runner, contextPreferences: preferences)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        try await waitUntil {
            viewModel.state.selectedContext?.name == "lattice-zone"
                && viewModel.state.selectedNamespace == "delta-zone"
        }

        let staleNamespaceArgs = ["kubectl"] + builder.podStatusListArguments(
            context: "lattice-zone",
            namespace: "echo-zone"
        )
        let usedStaleNamespace = await runner.didRun(arguments: staleNamespaceArgs)
        XCTAssertFalse(usedStaleNamespace)

        XCTAssertEqual(viewModel.namespaceOptions.first, "delta-zone")
        XCTAssertTrue(viewModel.namespaceOptions.contains("echo-zone"))
    }

    func testContextSwitchPrefersContextDefaultOverSavedNamespaceWhenNamespacesLoad() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "prod-main\nqa-main\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.contextNamespaceArguments(context: "qa-main"))] = CommandResult(
            stdout: "delta-zone",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespaceListArguments(context: "qa-main"))] = CommandResult(
            stdout: "default\ndelta-zone\nfoxtrot-zone\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "qa-main", namespace: "delta-zone"))] = CommandResult(
            stdout: "delta-pod-0   Running\n",
            stderr: "",
            exitCode: 0
        )

        let runner = ScriptedCommandRunner(script: script)
        let preferences = InMemoryContextPreferencesStore(
            preferredNamespaces: ["qa-main": "foxtrot-zone"]
        )
        let viewModel = makeViewModel(runner: runner, contextPreferences: preferences)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        viewModel.setContext(KubeContext(name: "qa-main"))

        try await waitUntil {
            viewModel.state.selectedContext?.name == "qa-main"
                && viewModel.state.selectedNamespace == "delta-zone"
        }

        let staleNamespaceArgs = ["kubectl"] + builder.podStatusListArguments(
            context: "qa-main",
            namespace: "foxtrot-zone"
        )
        let usedStaleNamespace = await runner.didRun(arguments: staleNamespaceArgs)
        XCTAssertFalse(usedStaleNamespace)
    }

    func testContextSwitchRevalidatesNamespaceAndAvoidsStaleSavedNamespaceWhenContextDefaultIsEmpty() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "prod-main\nqa-main\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.contextNamespaceArguments(context: "qa-main"))] = CommandResult(
            stdout: "",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespaceListArguments(context: "qa-main"))] = CommandResult(
            stdout: "default\ndelta-zone\nfoxtrot-zone\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "qa-main", namespace: "delta-zone"))] = CommandResult(
            stdout: "delta-pod-0   Running\n",
            stderr: "",
            exitCode: 0
        )

        let runner = ScriptedCommandRunner(script: script)
        let preferences = InMemoryContextPreferencesStore(
            preferredNamespaces: ["qa-main": "foxtrot-zone"]
        )
        let viewModel = makeViewModel(runner: runner, contextPreferences: preferences)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        viewModel.setContext(KubeContext(name: "qa-main"))

        try await waitUntil {
            viewModel.state.selectedContext?.name == "qa-main"
                && viewModel.state.selectedNamespace == "delta-zone"
        }

        let staleNamespaceArgs = ["kubectl"] + builder.podStatusListArguments(
            context: "qa-main",
            namespace: "foxtrot-zone"
        )
        let usedStaleNamespace = await runner.didRun(arguments: staleNamespaceArgs)
        XCTAssertFalse(usedStaleNamespace)
    }

    func testContextSwitchPrefersContextSuffixOverMismatchedContextDefault() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "prod-main\nvector-delta-zone\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.contextNamespaceArguments(context: "vector-delta-zone"))] = CommandResult(
            stdout: "echo-zone",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespaceListArguments(context: "vector-delta-zone"))] = CommandResult(
            stdout: "default\ndelta-zone\necho-zone\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "vector-delta-zone", namespace: "delta-zone"))] = CommandResult(
            stdout: "delta-pod-0   Running\n",
            stderr: "",
            exitCode: 0
        )

        let runner = ScriptedCommandRunner(script: script)
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        viewModel.setContext(KubeContext(name: "vector-delta-zone"))

        try await waitUntil {
            viewModel.state.selectedContext?.name == "vector-delta-zone"
                && viewModel.state.selectedNamespace == "delta-zone"
        }

        let wrongNamespaceArgs = ["kubectl"] + builder.podStatusListArguments(
            context: "vector-delta-zone",
            namespace: "echo-zone"
        )
        let usedWrongNamespace = await runner.didRun(arguments: wrongNamespaceArgs)
        XCTAssertFalse(usedWrongNamespace)
    }

    func testNamespaceOptionsPreferCurrentSelectionWhenNamespaceListUnavailable() async throws {
        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let preferences = InMemoryContextPreferencesStore(
            preferredNamespaces: ["example-context": "example-namespace"]
        )
        let viewModel = makeViewModel(runner: runner, contextPreferences: preferences)

        viewModel.state.selectedContext = KubeContext(name: "example-context")
        viewModel.state.selectedNamespace = "old-selection"

        XCTAssertEqual(viewModel.namespaceOptions, ["old-selection"])
    }

    func testUnifiedDeploymentLogsSmokeFlow() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let deployment = viewModel.state.deployments.first(where: { $0.name == "api" }) else {
            XCTFail("Expected api deployment")
            return
        }

        viewModel.setWorkloadKind(.deployment)
        viewModel.selectDeployment(deployment)

        try await waitUntil {
            !viewModel.state.unifiedServiceLogs.isEmpty
        }

        XCTAssertTrue(viewModel.state.unifiedServiceLogs.contains("[api-0]"))
        XCTAssertTrue(viewModel.state.unifiedServiceLogs.contains("[api-1]"))
        XCTAssertEqual(viewModel.state.unifiedServiceLogPods, ["api-0", "api-1"])

        let deploymentJSONArgs = ["kubectl"] + builder.deploymentJSONArguments(context: "prod-main", namespace: "default", deploymentName: "api")
        let calledDeploymentJSON = await runner.didRun(arguments: deploymentJSONArgs)
        XCTAssertFalse(calledDeploymentJSON)
    }

    func testProductionGuardScaleSmokeFlow() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let deployment = viewModel.state.deployments.first(where: { $0.name == "api" }) else {
            XCTFail("Expected deployment api")
            return
        }

        viewModel.setWorkloadKind(.deployment)
        viewModel.selectDeployment(deployment)
        viewModel.scaleReplicaInput = 5

        XCTAssertTrue(viewModel.isProductionContext)

        viewModel.requestScaleSelectedDeployment()
        XCTAssertNotNil(viewModel.pendingWriteAction)
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("PRODUCTION CONTEXT"))

        viewModel.confirmPendingWriteAction()

        let scaleArgs = ["kubectl"] + builder.scaleDeploymentArguments(context: "prod-main", namespace: "default", deploymentName: "api", replicas: 5)
        try await waitUntil {
            await runner.didRun(arguments: scaleArgs)
        }

        let calledScale = await runner.didRun(arguments: scaleArgs)
        XCTAssertTrue(calledScale)
    }

    func testReadOnlyModeBlocksWriteActions() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let deployment = viewModel.state.deployments.first(where: { $0.name == "api" }) else {
            XCTFail("Expected deployment api")
            return
        }

        viewModel.setWorkloadKind(.deployment)
        viewModel.selectDeployment(deployment)

        viewModel.setReadOnlyMode(true)
        viewModel.requestScaleSelectedDeployment()

        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertEqual(viewModel.state.lastError, RuneError.readOnlyMode.localizedDescription)

        let scaleArgs = ["kubectl"] + builder.scaleDeploymentArguments(context: "prod-main", namespace: "default", deploymentName: "api", replicas: 3)
        let calledScale = await runner.didRun(arguments: scaleArgs)
        XCTAssertFalse(calledScale)
    }

    func testRolloutRestartSmokeFlow() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let deployment = viewModel.state.deployments.first(where: { $0.name == "api" }) else {
            XCTFail("Expected deployment api")
            return
        }

        viewModel.setWorkloadKind(.deployment)
        viewModel.selectDeployment(deployment)
        viewModel.requestRolloutRestartSelectedDeployment()

        XCTAssertNotNil(viewModel.pendingWriteAction)
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("PRODUCTION CONTEXT"))

        viewModel.confirmPendingWriteAction()

        let restartArgs = ["kubectl"] + builder.rolloutRestartArguments(context: "prod-main", namespace: "default", deploymentName: "api")
        try await waitUntil {
            await runner.didRun(arguments: restartArgs)
        }

        let calledRestart = await runner.didRun(arguments: restartArgs)
        XCTAssertTrue(calledRestart)
    }

    func testRolloutHistoryAndUndoSmokeFlow() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let deployment = viewModel.state.deployments.first(where: { $0.name == "api" }) else {
            XCTFail("Expected deployment api")
            return
        }

        viewModel.setWorkloadKind(.deployment)
        viewModel.selectDeployment(deployment)

        try await waitUntil {
            !viewModel.state.deploymentRolloutHistory.isEmpty
        }

        XCTAssertTrue(viewModel.state.deploymentRolloutHistory.contains("REVISION"))

        viewModel.rolloutRevisionInput = "2"
        viewModel.requestRolloutUndoSelectedDeployment()
        XCTAssertNotNil(viewModel.pendingWriteAction)

        viewModel.confirmPendingWriteAction()

        let undoArgs = ["kubectl"] + builder.rolloutUndoArguments(
            context: "prod-main",
            namespace: "default",
            deploymentName: "api",
            revision: 2
        )

        try await waitUntil {
            await runner.didRun(arguments: undoArgs)
        }

        let didUndo = await runner.didRun(arguments: undoArgs)
        XCTAssertTrue(didUndo)
    }

    func testHelmSmokeFlowLoadsDetailsAndRollback() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        viewModel.setSection(.helm)

        try await waitUntil {
            !viewModel.state.helmReleases.isEmpty && !viewModel.state.helmManifest.isEmpty
        }

        XCTAssertEqual(viewModel.state.selectedHelmRelease?.name, "platform")
        XCTAssertTrue(viewModel.state.helmManifest.contains("kind: Deployment"))
        XCTAssertEqual(viewModel.state.helmHistory.first?.revision, 3)

        viewModel.helmRollbackRevisionInput = "2"
        viewModel.requestRollbackSelectedHelmRelease()
        XCTAssertNotNil(viewModel.pendingWriteAction)

        viewModel.confirmPendingWriteAction()

        let helmBuilder = HelmCommandBuilder()
        let rollbackArgs = ["helm"] + helmBuilder.rollbackArguments(
            context: "prod-main",
            namespace: "platform",
            releaseName: "platform",
            revision: 2
        )

        try await waitUntil {
            await runner.didRun(arguments: rollbackArgs)
        }

        let didRollback = await runner.didRun(arguments: rollbackArgs)
        XCTAssertTrue(didRollback)
    }

    func testSupportBundleExportUsesExporter() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let exporter = RecordingExporter()
        let viewModel = makeViewModel(runner: runner, exporter: exporter)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        viewModel.saveSupportBundle()

        let saved = exporter.lastSaved
        XCTAssertNotNil(saved)
        XCTAssertEqual(saved?.allowedFileTypes, ["json"])
        XCTAssertTrue(saved?.suggestedName.hasPrefix("support-bundle-") == true)

        let payload = String(decoding: saved?.data ?? Data(), as: UTF8.self)
        XCTAssertTrue(payload.contains("\"contextName\""))
        XCTAssertTrue(payload.contains("\"resourceCounts\""))
    }

    func testCommandPaletteSupportsK9sStyleQueries() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        let podItems = viewModel.commandPaletteItems(query: ":po api")
        XCTAssertEqual(podItems.first?.title, "api-0")

        viewModel.executeCommandPaletteQuery(":svc api")
        XCTAssertEqual(viewModel.state.selectedSection, .networking)
        XCTAssertEqual(viewModel.state.selectedService?.name, "api-svc")
    }

    func testCommandPaletteNamespaceSwitchWorksInSyntheticContext() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "vector-prod\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.contextNamespaceArguments(context: "vector-prod"))] = CommandResult(
            stdout: "gamma-hub",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespaceListArguments(context: "vector-prod"))] = CommandResult(
            stdout: "gamma-hub\ncinder-zone\ndefault\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "vector-prod", namespace: "gamma-hub"))] = CommandResult(
            stdout: "gamma-hub-0   Running\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "vector-prod", namespace: "cinder-zone"))] = CommandResult(
            stdout: "cinder-node-0   Running\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podListArguments(context: "vector-prod", namespace: "gamma-hub"))] = CommandResult(
            stdout: """
            {"items":[
            {"metadata":{"name":"gamma-hub-0","namespace":"gamma-hub","creationTimestamp":"2026-04-16T10:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}}
            ]}
            """,
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podListArguments(context: "vector-prod", namespace: "cinder-zone"))] = CommandResult(
            stdout: """
            {"items":[
            {"metadata":{"name":"cinder-node-0","namespace":"cinder-zone","creationTimestamp":"2026-04-16T10:01:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}}
            ]}
            """,
            stderr: "",
            exitCode: 0
        )

        let runner = ScriptedCommandRunner(script: script)
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        try await waitUntil {
            viewModel.state.selectedContext?.name == "vector-prod"
                && viewModel.state.selectedNamespace == "gamma-hub"
        }

        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.pod)

        let namespaceRows = viewModel.commandPaletteItems(query: ":ns")
        XCTAssertTrue(namespaceRows.contains(where: { $0.title == "gamma-hub" }))
        XCTAssertTrue(namespaceRows.contains(where: { $0.title == "cinder-zone" }))

        guard let targetItem = namespaceRows.first(where: { $0.title == "cinder-zone" }) else {
            XCTFail("Expected synthetic namespace row in command palette")
            return
        }

        viewModel.executeCommandPaletteItem(targetItem)

        try await waitUntil {
            viewModel.state.selectedNamespace == "cinder-zone"
        }

        let targetArgs = ["kubectl"] + builder.podListArguments(
            context: "vector-prod",
            namespace: "cinder-zone"
        )
        try await waitUntil {
            await runner.didRun(arguments: targetArgs)
        }
        let calledTargetNamespace = await runner.didRun(arguments: targetArgs)
        XCTAssertTrue(calledTargetNamespace)
        try await waitUntil { !viewModel.state.isLoading }
    }

    func testCommandPaletteContextThenNamespaceQueryUsesCurrentContextNamespaces() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        var script = baseScript(builder: builder)
        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(
            stdout: "prod-main\nvector-prod\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.contextNamespaceArguments(context: "vector-prod"))] = CommandResult(
            stdout: "gamma-hub",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespaceListArguments(context: "vector-prod"))] = CommandResult(
            stdout: "gamma-hub\ncinder-zone\ndefault\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "vector-prod", namespace: "gamma-hub"))] = CommandResult(
            stdout: "gamma-hub-0   Running\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "vector-prod", namespace: "cinder-zone"))] = CommandResult(
            stdout: "cinder-node-0   Running\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podListArguments(context: "vector-prod", namespace: "gamma-hub"))] = CommandResult(
            stdout: """
            {"items":[
            {"metadata":{"name":"gamma-hub-0","namespace":"gamma-hub","creationTimestamp":"2026-04-16T10:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}}
            ]}
            """,
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podListArguments(context: "vector-prod", namespace: "cinder-zone"))] = CommandResult(
            stdout: """
            {"items":[
            {"metadata":{"name":"cinder-node-0","namespace":"cinder-zone","creationTimestamp":"2026-04-16T10:01:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}}
            ]}
            """,
            stderr: "",
            exitCode: 0
        )

        let runner = ScriptedCommandRunner(script: script)
        let viewModel = makeViewModel(runner: runner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        viewModel.executeCommandPaletteQuery(":ctx vector-prod")

        try await waitUntil {
            viewModel.state.selectedContext?.name == "vector-prod"
                && viewModel.state.selectedNamespace == "gamma-hub"
        }

        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.pod)

        let namespaceRows = viewModel.commandPaletteItems(query: ":ns")
        XCTAssertFalse(namespaceRows.contains(where: { $0.title == "platform" }))
        XCTAssertTrue(namespaceRows.contains(where: { $0.title == "cinder-zone" }))

        viewModel.executeCommandPaletteQuery(":ns cinder-zone")

        try await waitUntil {
            viewModel.state.selectedNamespace == "cinder-zone"
        }

        let targetArgs = ["kubectl"] + builder.podListArguments(
            context: "vector-prod",
            namespace: "cinder-zone"
        )
        try await waitUntil {
            await runner.didRun(arguments: targetArgs)
        }
        let calledTargetNamespace = await runner.didRun(arguments: targetArgs)
        XCTAssertTrue(calledTargetNamespace)
        try await waitUntil { !viewModel.state.isLoading }
    }

    func testSaveVisibleEventsUsesExporter() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let exporter = RecordingExporter()
        let viewModel = makeViewModel(runner: runner, exporter: exporter)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()
        viewModel.setSection(.events)
        viewModel.saveVisibleEvents()

        let saved = exporter.lastSaved
        XCTAssertNotNil(saved)
        XCTAssertTrue(saved?.suggestedName.hasPrefix("events-default-") == true)
        XCTAssertEqual(saved?.allowedFileTypes, ["txt", "log"])
        XCTAssertTrue(String(decoding: saved?.data ?? Data(), as: UTF8.self).contains("Container started"))
    }

    func testExecSmokeFlow() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let longRunningRunner = ScriptedLongRunningCommandRunner()
        let viewModel = makeViewModel(runner: runner, longRunningRunner: longRunningRunner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let pod = viewModel.state.pods.first(where: { $0.name == "api-0" }) else {
            XCTFail("Expected pod api-0")
            return
        }

        viewModel.setWorkloadKind(.pod)
        viewModel.selectPod(pod)
        viewModel.execCommandInput = "printenv HOSTNAME"
        viewModel.requestExecInSelectedPod()

        XCTAssertNotNil(viewModel.pendingWriteAction)
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("PRODUCTION CONTEXT"))

        viewModel.confirmPendingWriteAction()

        let execArgs = ["kubectl"] + builder.podExecArguments(
            context: "prod-main",
            namespace: "default",
            podName: "api-0",
            container: nil,
            command: ["printenv", "HOSTNAME"]
        )

        try await waitUntil {
            await runner.didRun(arguments: execArgs)
        }

        XCTAssertEqual(viewModel.state.lastExecResult?.podName, "api-0")
        XCTAssertTrue(viewModel.state.lastExecResult?.stdout.contains("api-0") == true)
        XCTAssertEqual(viewModel.state.selectedSection, .terminal)
    }

    func testPortForwardSmokeFlow() async throws {
        let kubeconfigURL = try makeTempKubeconfigFile()
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: baseScript(builder: builder))
        let longRunningRunner = ScriptedLongRunningCommandRunner()
        let viewModel = makeViewModel(runner: runner, longRunningRunner: longRunningRunner)

        viewModel.state.setSources([KubeConfigSource(url: kubeconfigURL)])
        try await viewModel.reloadContexts()

        guard let service = viewModel.state.services.first(where: { $0.name == "api-svc" }) else {
            XCTFail("Expected service api-svc")
            return
        }

        viewModel.setWorkloadKind(.service)
        viewModel.selectService(service)
        viewModel.portForwardLocalPortInput = "8080"
        viewModel.portForwardRemotePortInput = "80"
        viewModel.portForwardAddressInput = "127.0.0.1"
        viewModel.startPortForwardForSelection()

        let startArgs = ["kubectl"] + builder.portForwardArguments(
            context: "prod-main",
            namespace: "default",
            targetKind: .service,
            targetName: "api-svc",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1"
        )

        try await waitUntil {
            longRunningRunner.didStart(arguments: startArgs)
        }

        XCTAssertEqual(viewModel.state.portForwardSessions.first?.targetName, "api-svc")
        XCTAssertEqual(viewModel.state.portForwardSessions.first?.status, .active)
        XCTAssertEqual(viewModel.state.selectedSection, .terminal)

        if let session = viewModel.state.portForwardSessions.first {
            viewModel.stopPortForward(session)
        } else {
            XCTFail("Expected port-forward session")
            return
        }

        try await waitUntil {
            viewModel.state.portForwardSessions.first?.status == .stopped
        }
    }

    private func makeViewModel(
        runner: any CommandRunning,
        bookmarkManager: BookmarkManager = BookmarkManager(store: InMemoryBookmarkStore()),
        kubeConfigDiscoverer: KubeConfigDiscovering = StaticKubeConfigDiscoverer(urls: []),
        exporter: FileExporting = NoopExporter(),
        longRunningRunner: LongRunningCommandRunning = ScriptedLongRunningCommandRunner(),
        contextPreferences: ContextPreferencesStoring = InMemoryContextPreferencesStore(),
        namespaceListPersistence: NamespaceListPersisting = NoopNamespaceListPersistenceStore(),
        overviewSnapshotPersistence: any OverviewSnapshotCacheStoring = NoopOverviewSnapshotCacheStore()
    ) -> RuneAppViewModel {
        let kubeClient = KubectlClient(
            runner: runner,
            longRunningRunner: longRunningRunner,
            parser: KubectlOutputParser(),
            builder: KubectlCommandBuilder(),
            kubectlPath: "/usr/bin/env",
            access: SecurityScopedAccess()
        )
        let helmClient = HelmClient(
            runner: runner,
            parser: HelmOutputParser(),
            builder: HelmCommandBuilder(),
            helmPath: "/usr/bin/env",
            access: SecurityScopedAccess()
        )

        return RuneAppViewModel(
            state: RuneAppState(),
            kubeClient: kubeClient,
            helmClient: helmClient,
            bookmarkManager: bookmarkManager,
            picker: NoopPicker(),
            kubeConfigDiscoverer: kubeConfigDiscoverer,
            exporter: exporter,
            supportBundleBuilder: JSONSupportBundleBuilder(),
            contextPreferences: contextPreferences,
            overviewSnapshotPersistence: overviewSnapshotPersistence,
            namespaceListPersistence: namespaceListPersistence
        )
    }

    private func makeTempKubeconfigFile() throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent("rune-smoke-kubeconfig-\(UUID().uuidString)")
        try "apiVersion: v1\nkind: Config\nclusters: []\ncontexts: []\nusers: []\n".write(to: url, atomically: true, encoding: .utf8)
        return url
    }

    private func baseScript(builder: KubectlCommandBuilder) -> [CommandKey: CommandResult] {
        var script: [CommandKey: CommandResult] = [:]
        let helmBuilder = HelmCommandBuilder()

        script[key(["kubectl"] + builder.contextListArguments())] = CommandResult(stdout: "prod-main\n", stderr: "", exitCode: 0)
        script[key(["kubectl"] + builder.contextNamespaceArguments(context: "prod-main"))] = CommandResult(stdout: "default", stderr: "", exitCode: 0)
        script[key(["kubectl"] + builder.namespaceListArguments(context: "prod-main"))] = CommandResult(
            stdout: "default\nkube-system\nplatform\n",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.podListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: """
            {"items":[
            {"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2024-06-01T12:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}},
            {"metadata":{"name":"api-1","namespace":"default","creationTimestamp":"2024-06-01T11:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":1}]}}
            ]}
            """,
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podStatusListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: """
            api-0   Running
            api-1   Running
            """,
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podTopArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: """
            api-0   5m   10Mi
            api-1   3m   8Mi
            """,
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.podListAllNamespacesArguments(context: "prod-main"))] = CommandResult(
            stdout: """
            {"items":[
            {"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2024-06-01T12:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}},
            {"metadata":{"name":"api-1","namespace":"default","creationTimestamp":"2024-06-01T11:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}},
            {"metadata":{"name":"jobs-0","namespace":"platform","creationTimestamp":"2024-06-01T10:00:00Z"},"status":{"phase":"Pending","containerStatuses":[]}}
            ]}
            """,
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.podTopAllNamespacesArguments(context: "prod-main"))] = CommandResult(
            stdout: """
            default   api-0   5m   10Mi
            default   api-1   3m   8Mi
            platform   jobs-0   1m   4Mi
            """,
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.deploymentListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"api\"},\"spec\":{\"replicas\":3,\"selector\":{\"matchLabels\":{\"app\":\"api\"}}},\"status\":{\"readyReplicas\":2}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.deploymentListAllNamespacesArguments(context: "prod-main"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"api\",\"namespace\":\"default\"},\"spec\":{\"replicas\":3,\"selector\":{\"matchLabels\":{\"app\":\"api\"}}},\"status\":{\"readyReplicas\":2}},{\"metadata\":{\"name\":\"worker\",\"namespace\":\"platform\"},\"spec\":{\"replicas\":1},\"status\":{\"readyReplicas\":1}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.statefulSetListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"api-db\"},\"spec\":{\"replicas\":2},\"status\":{\"readyReplicas\":2}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.daemonSetListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"node-agent\"},\"status\":{\"numberReady\":3,\"desiredNumberScheduled\":3}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.serviceListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"api-svc\"},\"spec\":{\"type\":\"ClusterIP\",\"clusterIP\":\"10.96.0.21\"}}]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "prod-main", namespace: "default", resource: "deployments"))] = CommandResult(
            stdout: "api\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "prod-main", namespace: "default", resource: "services"))] = CommandResult(
            stdout: "api-svc\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "prod-main", namespace: "default", resource: "ingresses"))] = CommandResult(
            stdout: "public-api\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "prod-main", namespace: "default", resource: "configmaps"))] = CommandResult(
            stdout: "app-config\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.namespacedResourceCountArguments(context: "prod-main", namespace: "default", resource: "cronjobs"))] = CommandResult(
            stdout: "",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.clusterResourceCountArguments(context: "prod-main", resource: "nodes"))] = CommandResult(
            stdout: "worker-1\n",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.serviceListAllNamespacesArguments(context: "prod-main"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"api-svc\",\"namespace\":\"default\"},\"spec\":{\"type\":\"ClusterIP\",\"clusterIP\":\"10.96.0.21\"}},{\"metadata\":{\"name\":\"jobs-svc\",\"namespace\":\"platform\"},\"spec\":{\"type\":\"ClusterIP\",\"clusterIP\":\"10.96.0.44\"}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.ingressListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"public-api\"},\"spec\":{\"rules\":[{\"host\":\"api.example.com\"}]},\"status\":{\"loadBalancer\":{\"ingress\":[{\"hostname\":\"lb.example.com\"}]}}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.ingressListAllNamespacesArguments(context: "prod-main"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"public-api\",\"namespace\":\"default\"},\"spec\":{\"rules\":[{\"host\":\"api.example.com\"}]},\"status\":{\"loadBalancer\":{\"ingress\":[{\"hostname\":\"lb.example.com\"}]}}},{\"metadata\":{\"name\":\"jobs-api\",\"namespace\":\"platform\"},\"spec\":{\"rules\":[{\"host\":\"jobs.example.com\"}]},\"status\":{\"loadBalancer\":{\"ingress\":[{\"hostname\":\"jobs-lb.example.com\"}]}}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.configMapListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"app-config\"},\"data\":{\"LOG_LEVEL\":\"info\",\"FEATURE_FLAG\":\"true\"}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.configMapListAllNamespacesArguments(context: "prod-main"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"app-config\",\"namespace\":\"default\"},\"data\":{\"LOG_LEVEL\":\"info\",\"FEATURE_FLAG\":\"true\"}},{\"metadata\":{\"name\":\"jobs-config\",\"namespace\":\"platform\"},\"data\":{\"QUEUE\":\"payments\"}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.secretListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"db-credentials\"},\"type\":\"Opaque\",\"data\":{\"username\":\"dXNlcg==\",\"password\":\"c2VjcmV0\"}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.nodeListArguments(context: "prod-main"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"worker-1\"},\"status\":{\"conditions\":[{\"type\":\"Ready\",\"status\":\"True\"}],\"nodeInfo\":{\"kubeletVersion\":\"v1.31.0\"}}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.persistentVolumeClaimListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"data-pvc\",\"namespace\":\"default\"},\"spec\":{\"resources\":{\"requests\":{\"storage\":\"1Gi\"}}},\"status\":{\"phase\":\"Bound\",\"capacity\":{\"storage\":\"1Gi\"}}}]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.persistentVolumeListArguments(context: "prod-main"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"pv-1\"},\"spec\":{\"capacity\":{\"storage\":\"10Gi\"}},\"status\":{\"phase\":\"Available\"}}]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.storageClassListArguments(context: "prod-main"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"standard\",\"annotations\":{\"storageclass.kubernetes.io/is-default-class\":\"true\"}},\"provisioner\":\"kubernetes.io/aws-ebs\"}]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.horizontalPodAutoscalerListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"api-hpa\",\"namespace\":\"default\"},\"spec\":{\"minReplicas\":1,\"maxReplicas\":5,\"scaleTargetRef\":{\"kind\":\"Deployment\",\"name\":\"api\"}},\"status\":{\"currentReplicas\":2}}]}",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.networkPolicyListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"metadata\":{\"name\":\"deny-all\",\"namespace\":\"default\"},\"spec\":{\"policyTypes\":[\"Ingress\",\"Egress\"]}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.eventListArguments(context: "prod-main", namespace: "default"))] = CommandResult(
            stdout: "{\"items\":[{\"type\":\"Normal\",\"reason\":\"Started\",\"message\":\"Container started\",\"involvedObject\":{\"name\":\"api-0\"}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.eventListAllNamespacesArguments(context: "prod-main"))] = CommandResult(
            stdout: "{\"items\":[{\"type\":\"Warning\",\"reason\":\"BackOff\",\"message\":\"Back-off restarting failed container\",\"involvedObject\":{\"name\":\"jobs-0\"}},{\"type\":\"Normal\",\"reason\":\"Started\",\"message\":\"Container started\",\"involvedObject\":{\"name\":\"api-0\"}}]}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.podLogsArguments(
            context: "prod-main",
            namespace: "default",
            podName: "api-0",
            container: nil,
            filter: .tailLines(200),
            previous: false,
            follow: false
        ))] = CommandResult(
            stdout: "2026-04-16T00:00:01Z started api-0",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.podLogsArguments(
            context: "prod-main",
            namespace: "default",
            podName: "api-1",
            container: nil,
            filter: .tailLines(200),
            previous: false,
            follow: false
        ))] = CommandResult(
            stdout: "2026-04-16T00:00:02Z started api-1",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.podExecArguments(
            context: "prod-main",
            namespace: "default",
            podName: "api-0",
            container: nil,
            command: ["printenv", "HOSTNAME"]
        ))] = CommandResult(
            stdout: "api-0\n",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.resourceYAMLArguments(context: "prod-main", namespace: "default", kind: .pod, name: "api-0"))] = CommandResult(
            stdout: "kind: Pod\nmetadata:\n  name: api-0\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.describeResourceArguments(context: "prod-main", namespace: "default", kind: .pod, name: "api-0"))] = CommandResult(
            stdout: "Name: api-0\n",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.resourceYAMLArguments(context: "prod-main", namespace: "default", kind: .deployment, name: "api"))] = CommandResult(
            stdout: "kind: Deployment\nmetadata:\n  name: api\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.describeResourceArguments(context: "prod-main", namespace: "default", kind: .deployment, name: "api"))] = CommandResult(
            stdout: "Name: api\n",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.resourceYAMLArguments(context: "prod-main", namespace: "default", kind: .service, name: "api-svc"))] = CommandResult(
            stdout: "kind: Service\nmetadata:\n  name: api-svc\n",
            stderr: "",
            exitCode: 0
        )
        script[key(["kubectl"] + builder.describeResourceArguments(context: "prod-main", namespace: "default", kind: .service, name: "api-svc"))] = CommandResult(
            stdout: "Name: api-svc\n",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.serviceJSONArguments(context: "prod-main", namespace: "default", serviceName: "api-svc"))] = CommandResult(
            stdout: "{\"metadata\":{\"name\":\"api-svc\"},\"spec\":{\"selector\":{\"app\":\"api\"}}}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.deploymentJSONArguments(context: "prod-main", namespace: "default", deploymentName: "api"))] = CommandResult(
            stdout: "{\"metadata\":{\"name\":\"api\"},\"spec\":{\"selector\":{\"matchLabels\":{\"app\":\"api\"}}}}",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.podsByLabelSelectorArguments(context: "prod-main", namespace: "default", selector: "app=api"))] = CommandResult(
            stdout: """
            {"items":[
            {"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2024-06-01T12:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}},
            {"metadata":{"name":"api-1","namespace":"default","creationTimestamp":"2024-06-01T11:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0}]}}
            ]}
            """,
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.scaleDeploymentArguments(context: "prod-main", namespace: "default", deploymentName: "api", replicas: 5))] = CommandResult(
            stdout: "scaled",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.scaleDeploymentArguments(context: "prod-main", namespace: "default", deploymentName: "api", replicas: 3))] = CommandResult(
            stdout: "scaled",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.rolloutRestartArguments(context: "prod-main", namespace: "default", deploymentName: "api"))] = CommandResult(
            stdout: "deployment.apps/api restarted",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.rolloutHistoryArguments(context: "prod-main", namespace: "default", deploymentName: "api"))] = CommandResult(
            stdout: "REVISION  CHANGE-CAUSE\n1         <none>\n2         config update\n",
            stderr: "",
            exitCode: 0
        )

        script[key(["kubectl"] + builder.rolloutUndoArguments(context: "prod-main", namespace: "default", deploymentName: "api", revision: 2))] = CommandResult(
            stdout: "deployment.apps/api rolled back",
            stderr: "",
            exitCode: 0
        )

        script[key(["helm"] + helmBuilder.listArguments(context: "prod-main", namespace: nil, allNamespaces: true))] = CommandResult(
            stdout: "[{\"name\":\"platform\",\"namespace\":\"platform\",\"revision\":\"3\",\"updated\":\"2026-04-16 10:00:00.000000 +0000 UTC\",\"status\":\"deployed\",\"chart\":\"platform-1.4.0\",\"app_version\":\"2.8.1\"}]",
            stderr: "",
            exitCode: 0
        )

        script[key(["helm"] + helmBuilder.valuesArguments(context: "prod-main", namespace: "platform", releaseName: "platform"))] = CommandResult(
            stdout: "replicaCount: 3\nimage:\n  tag: 2.8.1\n",
            stderr: "",
            exitCode: 0
        )

        script[key(["helm"] + helmBuilder.manifestArguments(context: "prod-main", namespace: "platform", releaseName: "platform"))] = CommandResult(
            stdout: "kind: Deployment\nmetadata:\n  name: platform\n",
            stderr: "",
            exitCode: 0
        )

        script[key(["helm"] + helmBuilder.historyArguments(context: "prod-main", namespace: "platform", releaseName: "platform"))] = CommandResult(
            stdout: "[{\"revision\":3,\"updated\":\"2026-04-16 10:00:00.000000 +0000 UTC\",\"status\":\"deployed\",\"chart\":\"platform-1.4.0\",\"app_version\":\"2.8.1\",\"description\":\"Upgrade complete\"},{\"revision\":2,\"updated\":\"2026-04-15 08:00:00.000000 +0000 UTC\",\"status\":\"superseded\",\"chart\":\"platform-1.3.0\",\"app_version\":\"2.8.0\",\"description\":\"Upgrade complete\"}]",
            stderr: "",
            exitCode: 0
        )

        script[key(["helm"] + helmBuilder.rollbackArguments(context: "prod-main", namespace: "platform", releaseName: "platform", revision: 2))] = CommandResult(
            stdout: "Rollback was a success",
            stderr: "",
            exitCode: 0
        )

        return script
    }

    private func key(_ arguments: [String]) -> CommandKey {
        CommandKey(executable: "/usr/bin/env", arguments: arguments)
    }

    private func waitUntil(timeout: TimeInterval = 2.0, condition: @escaping @MainActor () async -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)

        while Date() < deadline {
            if await condition() {
                return
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        XCTFail("Condition not met before timeout")
    }
}

private struct CommandKey: Hashable, Sendable {
    let executable: String
    let arguments: [String]
}

private actor ScriptedCommandRunner: CommandRunning {
    private let script: [CommandKey: CommandResult]
    private var calls: [CommandKey] = []

    init(script: [CommandKey: CommandResult]) {
        self.script = script
    }

    func run(
        executable: String,
        arguments: [String],
        environment: [String: String],
        timeout: TimeInterval?
    ) async throws -> CommandResult {
        let key = CommandKey(executable: executable, arguments: arguments)
        calls.append(key)

        guard let result = script[key] else {
            throw RuneError.commandFailed(command: "missing scripted command", message: arguments.joined(separator: " "))
        }

        return result
    }

    func didRun(arguments: [String]) -> Bool {
        calls.contains(where: { $0.arguments == arguments })
    }
}

private actor CancellableLogCommandRunner: CommandRunning {
    private let script: [CommandKey: CommandResult]
    private let slowLogArguments: [String]
    private let latestLogArguments: [String]
    private var calls: [CommandKey] = []
    private var slowLogStarted = false
    private var slowLogCancelled = false

    init(
        script: [CommandKey: CommandResult],
        slowLogArguments: [String],
        latestLogArguments: [String]
    ) {
        self.script = script
        self.slowLogArguments = slowLogArguments
        self.latestLogArguments = latestLogArguments
    }

    func run(
        executable: String,
        arguments: [String],
        environment: [String: String],
        timeout: TimeInterval?
    ) async throws -> CommandResult {
        let key = CommandKey(executable: executable, arguments: arguments)
        calls.append(key)

        if arguments == slowLogArguments {
            slowLogStarted = true
            do {
                try await Task.sleep(nanoseconds: 5_000_000_000)
                return CommandResult(stdout: "stale 5m logs", stderr: "", exitCode: 0)
            } catch {
                slowLogCancelled = true
                throw CancellationError()
            }
        }

        if arguments == latestLogArguments {
            return CommandResult(stdout: "latest 15m logs", stderr: "", exitCode: 0)
        }

        guard let result = script[key] else {
            throw RuneError.commandFailed(command: "missing scripted command", message: arguments.joined(separator: " "))
        }

        return result
    }

    func didStartSlowLogFetch() -> Bool {
        slowLogStarted
    }

    func didCancelSlowLogFetch() -> Bool {
        slowLogCancelled
    }

    func didRun(arguments: [String]) -> Bool {
        calls.contains(where: { $0.arguments == arguments })
    }
}

private final class InMemoryBookmarkStore: BookmarkStore {
    private var records: [BookmarkRecord] = []

    func loadRecords() throws -> [BookmarkRecord] {
        records
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        self.records = records
    }
}

private final class FailingSaveBookmarkStore: BookmarkStore {
    func loadRecords() throws -> [BookmarkRecord] {
        []
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        throw NSError(domain: "RuneTests", code: 17, userInfo: [NSLocalizedDescriptionKey: "save failed"])
    }
}

private final class FailingLoadBookmarkStore: BookmarkStore {
    func loadRecords() throws -> [BookmarkRecord] {
        throw NSError(domain: "RuneTests", code: 18, userInfo: [NSLocalizedDescriptionKey: "load failed"])
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {}
}

private final class NoopPicker: KubeConfigPicking {
    @MainActor
    func pickFiles() throws -> [URL] {
        []
    }
}

private final class NoopExporter: FileExporting {
    @MainActor
    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        FileManager.default.temporaryDirectory.appendingPathComponent(suggestedName)
    }
}

private final class ScriptedLongRunningCommandRunner: LongRunningCommandRunning, @unchecked Sendable {
    private let lock = NSLock()
    private var startedArguments: [[String]] = []

    func start(
        executable: String,
        arguments: [String],
        environment: [String : String],
        onStdout: @escaping @Sendable (String) -> Void,
        onStderr: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) -> Void
    ) throws -> any RunningCommandControlling {
        lock.lock()
        startedArguments.append(arguments)
        lock.unlock()

        onStderr("Forwarding from 127.0.0.1:8080 -> 80\n")
        return ScriptedRunningCommandHandle {
            onTermination(0)
        }
    }

    func didStart(arguments: [String]) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return startedArguments.contains(arguments)
    }
}

private final class ScriptedRunningCommandHandle: RunningCommandControlling, @unchecked Sendable {
    let id = UUID()
    private let onTerminate: @Sendable () -> Void
    private let lock = NSLock()
    private var hasTerminated = false

    init(onTerminate: @escaping @Sendable () -> Void) {
        self.onTerminate = onTerminate
    }

    func terminate() {
        lock.lock()
        let shouldTerminate = !hasTerminated
        hasTerminated = true
        lock.unlock()

        if shouldTerminate {
            onTerminate()
        }
    }
}

@MainActor
private final class RecordingExporter: FileExporting {
    struct SavedFile {
        let data: Data
        let suggestedName: String
        let allowedFileTypes: [String]
    }

    private(set) var lastSaved: SavedFile?

    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        lastSaved = SavedFile(data: data, suggestedName: suggestedName, allowedFileTypes: allowedFileTypes)
        return FileManager.default.temporaryDirectory.appendingPathComponent(suggestedName)
    }
}

private struct InMemoryContextPreferencesStore: ContextPreferencesStoring {
    var preferredNamespaces: [String: String] = [:]

    func loadFavoriteContextNames() -> Set<String> { [] }
    func saveFavoriteContextNames(_ names: Set<String>) {}
    func loadPreferredNamespace(for contextName: String) -> String? { preferredNamespaces[contextName] }
}

private struct StaticKubeConfigDiscoverer: KubeConfigDiscovering {
    let urls: [URL]

    func discoverCandidateFiles() -> [URL] {
        urls
    }
}

private struct InMemoryNamespaceListPersistenceStore: NamespaceListPersisting {
    var values: [String: [String]] = [:]

    func load(contextName: String) -> [String]? {
        values[contextName]
    }

    func save(names: [String], contextName: String) {}
}
