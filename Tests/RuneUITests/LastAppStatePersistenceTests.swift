import Foundation
import RuneCore
import RuneFakeK8sSupport
import RuneSecurity
import RuneStore
@testable import RuneUI
import XCTest

final class LastAppStatePersistenceTests: XCTestCase {
    func testJSONStoreRoundTripsVersionedSafeStateAndClears() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStatePersistenceTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let url = directory.appendingPathComponent("last-app-state.json")
        let store = JSONLastAppStateStore(url: url)
        let snapshot = LastAppStateSnapshot(
            sourceScopeID: "synthetic-source-scope",
            contextName: "synthetic-context-b",
            namespace: "synthetic-scope-b",
            sectionID: RuneSection.workloads.rawValue,
            workloadKindID: KubeResourceKind.pod.rawValue,
            resourceID: "synthetic-resource-id",
            resourceKind: "pod",
            resourceName: "synthetic-pod-b",
            resourceNamespace: "synthetic-scope-b",
            logPresetID: PodLogPreset.largeTail.rawValue,
            logContainer: "synthetic-sidecar",
            includePreviousLogs: true,
            isHelmAllNamespaces: false,
            podSortColumnID: PodListSortColumn.restarts.rawValue,
            podSortAscending: false,
            inspectorState: SavedWorkspaceInspectorState(
                podTabID: "logs",
                helmBrowserTabID: "operatorResources",
                terminalTabID: "logs"
            )
        )

        store.saveLastAppState(snapshot)

        XCTAssertEqual(store.loadLastAppState(), snapshot)
        let json = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(json.contains("\"schemaVersion\""))
        XCTAssertFalse(json.contains("searchQuery"))
        XCTAssertFalse(json.contains("logOutput"))
        XCTAssertFalse(json.contains("yamlDraft"))
        XCTAssertFalse(json.contains("transcript"))
        XCTAssertFalse(json.contains("shellCommand"))
        XCTAssertFalse(json.contains("credential"))

        store.clearLastAppState()
        XCTAssertNil(store.loadLastAppState())

        try Data(#"{"schemaVersion":2,"snapshot":{}}"#.utf8).write(to: url)
        XCTAssertNil(
            store.loadLastAppState(),
            "A newer unknown schema must not be interpreted as current app state."
        )
    }

    func testInspectorStateDecodesSnapshotsWrittenBeforeNewSessionFields() throws {
        let legacyJSON = Data(
            #"{"podTabID":"logs","terminalTabID":"yaml","isYAMLInlineEditing":false}"#.utf8
        )

        let decoded = try JSONDecoder().decode(
            SavedWorkspaceInspectorState.self,
            from: legacyJSON
        )

        XCTAssertEqual(decoded.podTabID, "logs")
        XCTAssertEqual(decoded.terminalTabID, "yaml")
        XCTAssertEqual(decoded.isYAMLInlineEditing, false)
        XCTAssertNil(decoded.helmBrowserTabID)
        XCTAssertNil(decoded.showsHistoricalDeploymentReplicaSets)
    }

    @MainActor
    func testViewModelRestoresSafeSelectionsAndAllLogsWhenEnabled() {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let inspectorState = SavedWorkspaceInspectorState(
            podTabID: "logs",
            deploymentTabID: "rollout",
            helmBrowserTabID: "operatorResources",
            terminalTabID: "yaml",
            isYAMLInlineEditing: nil,
            showsHistoricalDeploymentReplicaSets: true
        )
        let store = RecordingLastAppStateStore(
            snapshot: LastAppStateSnapshot(
                contextName: "synthetic-context-b",
                namespace: "synthetic-scope-b",
                sectionID: RuneSection.workloads.rawValue,
                workloadKindID: KubeResourceKind.deployment.rawValue,
                logPresetID: PodLogPreset.largeTail.rawValue,
                includePreviousLogs: true,
                isHelmAllNamespaces: false,
                podSortColumnID: PodListSortColumn.cpu.rawValue,
                podSortAscending: false,
                operatorResourceFocusID: OperatorResourceFocus.unhealthy.rawValue,
                inspectorState: inspectorState
            )
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, lastAppStateStore: store)

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .deployment)
        XCTAssertEqual(viewModel.selectedLogPreset, .largeTail)
        XCTAssertTrue(viewModel.includePreviousLogs)
        XCTAssertFalse(state.isHelmAllNamespaces)
        XCTAssertEqual(viewModel.podSortColumn, .cpu)
        XCTAssertFalse(viewModel.podSortAscending)
        XCTAssertEqual(viewModel.operatorResourceFocus, .unhealthy)
        XCTAssertEqual(
            viewModel.savedWorkspaceInspectorRestoreRequest?.inspectorState,
            inspectorState
        )
        XCTAssertFalse(viewModel.isLogTailModeEnabled)
        XCTAssertFalse(viewModel.isLogStreamPaused)
    }

    @MainActor
    func testInvalidStoredChoiceIDsFallBackWithoutEnablingTransientTailState() {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let store = RecordingLastAppStateStore(
            snapshot: LastAppStateSnapshot(
                sectionID: "removed-section",
                workloadKindID: "removed-kind",
                logPresetID: "removed-log-preset",
                podSortColumnID: "removed-sort-column",
                operatorResourceFocusID: "removed-focus"
            )
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, lastAppStateStore: store)

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(viewModel.selectedLogPreset, .recentLines)
        XCTAssertEqual(viewModel.podSortColumn, .name)
        XCTAssertEqual(viewModel.operatorResourceFocus, .all)
        XCTAssertFalse(viewModel.isLogTailModeEnabled)
        XCTAssertFalse(viewModel.isLogStreamPaused)
    }

    @MainActor
    func testDisabledSettingSkipsRestoreAndClearsOldSnapshot() {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = false
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let store = RecordingLastAppStateStore(
            snapshot: LastAppStateSnapshot(
                sectionID: RuneSection.terminal.rawValue,
                logPresetID: PodLogPreset.largeTail.rawValue,
                includePreviousLogs: true
            )
        )
        let terminalStore = RecordingTerminalWorkspaceStateStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            lastAppStateStore: store,
            terminalWorkspaceStateStore: terminalStore
        )

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertEqual(viewModel.selectedLogPreset, .recentLines)
        XCTAssertFalse(viewModel.includePreviousLogs)
        XCTAssertEqual(store.clearCount, 1)
        XCTAssertNil(store.snapshot)
        XCTAssertEqual(terminalStore.clearCount, 1)
    }

    @MainActor
    func testTurningSettingOffClearsBothStoresAndCancelsPendingWrite() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let store = RecordingLastAppStateStore()
        let terminalStore = RecordingTerminalWorkspaceStateStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            lastAppStateStore: store,
            terminalWorkspaceStateStore: terminalStore
        )
        state.selectedSection = .workloads

        UserDefaults.standard.runeSaveLastAppState = false
        NotificationCenter.default.post(
            name: UserDefaults.didChangeNotification,
            object: UserDefaults.standard
        )
        state.selectedSection = .terminal
        try await Task.sleep(nanoseconds: 200_000_000)

        XCTAssertEqual(viewModel.selectedLogPreset, .recentLines)
        XCTAssertGreaterThanOrEqual(store.clearCount, 1)
        XCTAssertGreaterThanOrEqual(terminalStore.clearCount, 1)
        XCTAssertEqual(store.saveCount, 0)
    }

    @MainActor
    func testSelectionChangesPersistWithoutWaitingForAppTermination() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let store = RecordingLastAppStateStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, lastAppStateStore: store)
        let pod = PodSummary(
            name: "synthetic-pod-a",
            namespace: "synthetic-scope-a",
            status: "Running",
            containerNamesLine: "synthetic-main, synthetic-sidecar"
        )

        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "synthetic-scope-a"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.setPods([pod])
        state.setSelectedPod(pod)
        viewModel.selectedLogPreset = .largeTail
        viewModel.selectedLogContainer = "synthetic-sidecar"
        viewModel.includePreviousLogs = true

        try await Task.sleep(nanoseconds: 300_000_000)

        let saved = try XCTUnwrap(store.snapshot)
        XCTAssertGreaterThan(store.saveCount, 0)
        XCTAssertEqual(saved.contextName, "synthetic-context-a")
        XCTAssertEqual(saved.namespace, "synthetic-scope-a")
        XCTAssertEqual(saved.sectionID, RuneSection.workloads.rawValue)
        XCTAssertEqual(saved.workloadKindID, KubeResourceKind.pod.rawValue)
        XCTAssertEqual(saved.resourceName, "synthetic-pod-a")
        XCTAssertEqual(saved.logPresetID, PodLogPreset.largeTail.rawValue)
        XCTAssertEqual(saved.logContainer, "synthetic-sidecar")
        XCTAssertEqual(saved.includePreviousLogs, true)
    }

    @MainActor
    func testImmediateFlushKeepsLatestLogChoiceAndScopesContainerToSelectedPod() throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let store = RecordingLastAppStateStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, lastAppStateStore: store)
        let pod = PodSummary(
            name: "synthetic-pod-a",
            namespace: "synthetic-scope-a",
            status: "Running",
            containerNamesLine: "synthetic-main, synthetic-sidecar"
        )
        let deployment = DeploymentSummary(
            name: "synthetic-deployment-a",
            namespace: "synthetic-scope-a",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        state.selectedContext = KubeContext(name: "synthetic-context-a")
        state.selectedNamespace = "synthetic-scope-a"
        state.selectedSection = .workloads
        state.setPods([pod])
        state.setDeployments([deployment])
        state.selectedWorkloadKind = .pod
        viewModel.selectedLogPreset = .largeTail
        viewModel.selectedLogContainer = "synthetic-sidecar"
        viewModel.includePreviousLogs = true

        viewModel.persistLastAppStateNow()

        XCTAssertEqual(store.snapshot?.logPresetID, PodLogPreset.largeTail.rawValue)
        XCTAssertEqual(store.snapshot?.logContainer, "synthetic-sidecar")
        XCTAssertEqual(store.snapshot?.includePreviousLogs, true)

        state.selectedWorkloadKind = .deployment
        viewModel.persistLastAppStateNow()

        XCTAssertEqual(store.snapshot?.resourceName, deployment.name)
        XCTAssertNil(
            store.snapshot?.logContainer,
            "A container from a previously selected pod must not leak into another resource."
        )
    }

    @MainActor
    func testRBACRestoreRequiresExactResourceKindWhenNamesCollide() {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let namespace = "synthetic-scope-a"
        let serviceAccount = ClusterResourceSummary(
            kind: .serviceAccount,
            name: "synthetic-shared-name",
            namespace: namespace,
            primaryText: "",
            secondaryText: ""
        )
        let role = ClusterResourceSummary(
            kind: .role,
            name: serviceAccount.name,
            namespace: namespace,
            primaryText: "",
            secondaryText: ""
        )
        let state = RuneAppState()
        state.setRBACData(
            roles: [role],
            serviceAccounts: [serviceAccount],
            roleBindings: [],
            clusterRoles: [],
            clusterRoleBindings: []
        )
        let viewModel = RuneAppViewModel(state: state)

        viewModel.openSavedWorkspace(
            SavedWorkspaceSnapshot(
                name: "Synthetic RBAC selection",
                contextName: nil,
                namespace: namespace,
                section: .rbac,
                workloadKind: .role,
                resourceKind: "role",
                resourceName: role.name,
                resourceNamespace: namespace
            )
        )

        XCTAssertEqual(state.selectedRBACResource?.kind, .role)
    }

    @MainActor
    func testColdStartRestoresExistingContextAndValidatedNamespace() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let fallbackScope = RuneFakeK8sNamespace(
            name: "synthetic-fallback-scope",
            pods: [],
            deployments: [],
            services: []
        )
        let restoredScope = RuneFakeK8sNamespace(
            name: "synthetic-restored-scope",
            pods: [],
            deployments: [],
            services: []
        )
        let firstContext = RuneFakeK8sCluster(
            contextName: "synthetic-context-a",
            defaultNamespace: fallbackScope.name,
            namespaces: [fallbackScope, restoredScope],
            nodes: []
        )
        let restoredContext = RuneFakeK8sCluster(
            contextName: "synthetic-context-b",
            defaultNamespace: restoredScope.name,
            namespaces: [fallbackScope, restoredScope],
            nodes: []
        )
        let fixture = RuneFakeK8sFixture(contexts: [firstContext, restoredContext])
        let server = try await RuneFakeK8sRESTServer.start(
            fixture: fixture,
            contextName: firstContext.contextName
        )
        defer { server.stop() }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStatePersistenceTests.bootstrap.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let defaultsSuite = "LastAppStatePersistenceTests.contextPreferences.\(UUID().uuidString)"
        let contextDefaults = try XCTUnwrap(UserDefaults(suiteName: defaultsSuite))
        defer { contextDefaults.removePersistentDomain(forName: defaultsSuite) }
        let store = RecordingLastAppStateStore()
        do {
            let persistedState = RuneAppState()
            persistedState.setSources([KubeConfigSource(url: kubeConfigURL)])
            persistedState.selectedContext = KubeContext(name: restoredContext.contextName)
            persistedState.selectedNamespace = restoredScope.name
            persistedState.selectedSection = .terminal
            persistedState.selectedWorkloadKind = .pod
            let writer = RuneAppViewModel(
                state: persistedState,
                contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
                savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
                lastAppStateStore: store,
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )
            writer.selectedLogPreset = .largeTail
            writer.persistLastAppStateNow()
        }
        XCTAssertNotNil(store.snapshot?.sourceScopeID)

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: EmptyLastStateBookmarkStore()),
            kubeConfigDiscoverer: FixedLastStateKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
            savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
            lastAppStateStore: store,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            state.selectedContext?.name == restoredContext.contextName
                && state.selectedNamespace == restoredScope.name
                && state.namespaces.contains(restoredScope.name)
        }

        XCTAssertEqual(state.selectedContext?.name, restoredContext.contextName)
        XCTAssertEqual(state.selectedNamespace, restoredScope.name)
        XCTAssertEqual(state.selectedSection, .terminal)
        XCTAssertEqual(viewModel.selectedLogPreset, .largeTail)
    }

    @MainActor
    func testColdStartNeverAppliesStaleNamespaceToFallbackContext() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStatePersistenceTests.fallback.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let defaultsSuite = "LastAppStatePersistenceTests.fallbackPreferences.\(UUID().uuidString)"
        let contextDefaults = try XCTUnwrap(UserDefaults(suiteName: defaultsSuite))
        defer { contextDefaults.removePersistentDomain(forName: defaultsSuite) }
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: EmptyLastStateBookmarkStore()),
            kubeConfigDiscoverer: FixedLastStateKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
            savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
            lastAppStateStore: RecordingLastAppStateStore(
                snapshot: LastAppStateSnapshot(
                    contextName: "synthetic-removed-context",
                    namespace: "synthetic-stale-scope",
                    sectionID: RuneSection.terminal.rawValue
                )
            ),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                && !state.namespaces.isEmpty
        }

        XCTAssertEqual(state.selectedContext?.name, RuneFakeK8sFixture.defaultContextName)
        XCTAssertEqual(state.selectedNamespace, "alpha-zone")
        XCTAssertNotEqual(state.selectedNamespace, "synthetic-stale-scope")
    }

    @MainActor
    func testColdStartDoesNotRestoreClusterScopeFromDifferentKubeConfigSource() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStatePersistenceTests.sourceScope.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let defaultsSuite = "LastAppStatePersistenceTests.sourceScopePreferences.\(UUID().uuidString)"
        let contextDefaults = try XCTUnwrap(UserDefaults(suiteName: defaultsSuite))
        defer { contextDefaults.removePersistentDomain(forName: defaultsSuite) }
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: EmptyLastStateBookmarkStore()),
            kubeConfigDiscoverer: FixedLastStateKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
            savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
            lastAppStateStore: RecordingLastAppStateStore(
                snapshot: LastAppStateSnapshot(
                    sourceScopeID: "synthetic-different-source-scope",
                    contextName: RuneFakeK8sFixture.defaultContextName,
                    namespace: "bravo-zone",
                    sectionID: RuneSection.terminal.rawValue
                )
            ),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                && state.namespaces.contains("bravo-zone")
        }

        XCTAssertEqual(state.selectedNamespace, "alpha-zone")
    }

    @MainActor
    func testTransientContextFailurePreservesLastGoodSnapshot() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStatePersistenceTests.failure.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try """
        apiVersion: v1
        kind: Config
        clusters:
        - name: synthetic-cluster-a
          cluster:
            server: https://127.0.0.1:6443
            insecure-skip-tls-verify: true
        contexts:
        - name: synthetic-context-a
          context:
            cluster: synthetic-cluster-a
            namespace: synthetic-scope-a
        current-context: synthetic-context-a
        users: []
        """.write(
            to: kubeConfigURL,
            atomically: true,
            encoding: .utf8
        )

        let writerStore = RecordingLastAppStateStore()
        let writerState = RuneAppState()
        writerState.setSources([KubeConfigSource(url: kubeConfigURL)])
        writerState.selectedContext = KubeContext(name: "synthetic-context-a")
        writerState.selectedNamespace = "synthetic-scope-a"
        writerState.selectedSection = .workloads
        writerState.selectedWorkloadKind = .pod
        writerState.setPods([
            PodSummary(
                name: "synthetic-pod-a",
                namespace: "synthetic-scope-a",
                status: "Running"
            )
        ])
        let writer = RuneAppViewModel(
            state: writerState,
            savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
            lastAppStateStore: writerStore,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        writer.persistLastAppStateNow()
        let original = try XCTUnwrap(writerStore.snapshot)
        XCTAssertNotNil(original.sourceScopeID)

        let store = RecordingLastAppStateStore(snapshot: original)
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: EmptyLastStateBookmarkStore()),
            kubeConfigDiscoverer: FixedLastStateKubeConfigDiscoverer(urls: [kubeConfigURL]),
            savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
            lastAppStateStore: store,
            kubeContextList: { _ in
                throw LastAppStateTestError.transientContextFailure
            },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil { state.lastError != nil }
        try await Task.sleep(nanoseconds: 200_000_000)

        XCTAssertEqual(store.snapshot, original)
        XCTAssertEqual(store.saveCount, 0)
    }

    @MainActor
    func testColdStartHydratesHelmListBeforeRestoringRelease() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSaveLastAppState = true
        UserDefaults.standard.runeSimpleMode = false
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
            restoreDefault(previousSimpleMode, key: RuneSettingsKeys.simpleMode)
        }

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStatePersistenceTests.helm.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let release = HelmReleaseSummary(
            name: "synthetic-release-a",
            namespace: "alpha-zone",
            revision: 3,
            updated: "2026-07-30T12:00:00Z",
            status: "deployed",
            chart: "synthetic-chart-1.0.0",
            appVersion: "1.0.0"
        )
        let defaultsSuite = "LastAppStatePersistenceTests.helmPreferences.\(UUID().uuidString)"
        let contextDefaults = try XCTUnwrap(UserDefaults(suiteName: defaultsSuite))
        defer { contextDefaults.removePersistentDomain(forName: defaultsSuite) }
        let store = RecordingLastAppStateStore()
        do {
            let persistedState = RuneAppState()
            persistedState.setSources([KubeConfigSource(url: kubeConfigURL)])
            persistedState.selectedContext = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
            persistedState.selectedNamespace = release.namespace
            persistedState.selectedSection = .helm
            persistedState.isHelmAllNamespaces = false
            persistedState.setHelmReleases([release])
            let writer = RuneAppViewModel(
                state: persistedState,
                contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
                savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
                lastAppStateStore: store,
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )
            writer.updateSavedWorkspaceInspectorState(
                SavedWorkspaceInspectorState(helmBrowserTabID: "releases")
            )
            writer.persistLastAppStateNow()
        }
        XCTAssertNotNil(store.snapshot?.sourceScopeID)

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: EmptyLastStateBookmarkStore()),
            kubeConfigDiscoverer: FixedLastStateKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
            savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
            lastAppStateStore: store,
            helmReleaseList: { _, _, namespace, allNamespaces in
                XCTAssertFalse(allNamespaces)
                XCTAssertEqual(namespace, release.namespace)
                return [release]
            },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            state.selectedHelmRelease?.id == release.id
        }

        XCTAssertEqual(state.selectedSection, .helm)
        XCTAssertEqual(state.selectedHelmRelease, release)
    }

    @MainActor
    func testColdStartRestoresResourceOnlyAfterItsScopeHydrates() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let fixture = RuneFakeK8sFixture()
        let expectedPod = try XCTUnwrap(
            fixture.contexts.first?.namespaces.first?.pods.last
        )
        let expectedContainer = try XCTUnwrap(expectedPod.containers.last)
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStatePersistenceTests.resource.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let defaultsSuite = "LastAppStatePersistenceTests.resourcePreferences.\(UUID().uuidString)"
        let contextDefaults = try XCTUnwrap(UserDefaults(suiteName: defaultsSuite))
        defer { contextDefaults.removePersistentDomain(forName: defaultsSuite) }
        let store = RecordingLastAppStateStore()
        do {
            let persistedPod = PodSummary(
                name: expectedPod.name,
                namespace: "alpha-zone",
                status: expectedPod.phase,
                containerNamesLine: expectedPod.containers.joined(separator: ", ")
            )
            let persistedState = RuneAppState()
            persistedState.setSources([KubeConfigSource(url: kubeConfigURL)])
            persistedState.selectedContext = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
            persistedState.selectedNamespace = "alpha-zone"
            persistedState.selectedSection = .workloads
            persistedState.selectedWorkloadKind = .pod
            persistedState.setPods([persistedPod])
            let writer = RuneAppViewModel(
                state: persistedState,
                contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
                savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
                lastAppStateStore: store,
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )
            writer.selectedLogPreset = .largeTail
            writer.selectedLogContainer = expectedContainer
            writer.includePreviousLogs = true
            writer.persistLastAppStateNow()
        }
        XCTAssertNotNil(store.snapshot?.sourceScopeID)

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: EmptyLastStateBookmarkStore()),
            kubeConfigDiscoverer: FixedLastStateKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
            savedWorkspaceStore: EmptyLastStateSavedWorkspaceStore(),
            lastAppStateStore: store,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        XCTAssertNil(state.selectedPod)
        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            state.selectedPod?.name == expectedPod.name
                && viewModel.selectedLogContainer == expectedContainer
                && !state.resourceYAML.isEmpty
                && server.requestLines().contains {
                    $0.contains("/pods/\(expectedPod.name)/log?")
                }
        }

        XCTAssertEqual(state.selectedContext?.name, RuneFakeK8sFixture.defaultContextName)
        XCTAssertEqual(state.selectedNamespace, "alpha-zone")
        XCTAssertEqual(state.selectedPod?.name, expectedPod.name)
        XCTAssertEqual(viewModel.selectedLogContainer, expectedContainer)
        XCTAssertFalse(state.resourceDescribe.isEmpty)
        let logRequest = try XCTUnwrap(
            server.requestLines().last {
                $0.contains("/pods/\(expectedPod.name)/log?")
            }
        )
        XCTAssertTrue(logRequest.contains("container=\(expectedContainer)"))
        XCTAssertTrue(logRequest.contains("previous=true"))
        XCTAssertFalse(logRequest.contains("tailLines="))
        XCTAssertFalse(logRequest.contains("sinceSeconds="))
    }

    private func restoreDefault(_ value: Any?, key: String) {
        if let value {
            UserDefaults.standard.set(value, forKey: key)
        } else {
            UserDefaults.standard.removeObject(forKey: key)
        }
    }

    @MainActor
    private func waitUntil(
        timeoutNanoseconds: UInt64 = 5_000_000_000,
        condition: @escaping @MainActor () -> Bool
    ) async throws {
        let start = ContinuousClock.now
        while !condition() {
            if start.duration(to: .now) > .nanoseconds(Int64(timeoutNanoseconds)) {
                XCTFail("Timed out waiting for last app state restoration.")
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }
}

private enum LastAppStateTestError: Error {
    case transientContextFailure
}

private final class RecordingLastAppStateStore: LastAppStateStoring {
    private(set) var snapshot: LastAppStateSnapshot?
    private(set) var saveCount = 0
    private(set) var clearCount = 0

    init(snapshot: LastAppStateSnapshot? = nil) {
        self.snapshot = snapshot
    }

    func loadLastAppState() -> LastAppStateSnapshot? {
        snapshot
    }

    func saveLastAppState(_ snapshot: LastAppStateSnapshot) {
        self.snapshot = snapshot
        saveCount += 1
    }

    func clearLastAppState() {
        snapshot = nil
        clearCount += 1
    }
}

private final class RecordingTerminalWorkspaceStateStore: TerminalWorkspaceStateStoring {
    private(set) var clearCount = 0

    func loadTerminalWorkspaceState() -> TerminalWorkspaceStateSnapshot? { nil }
    func saveTerminalWorkspaceState(_ snapshot: TerminalWorkspaceStateSnapshot) {}

    func clearTerminalWorkspaceState() {
        clearCount += 1
    }
}

private struct FixedLastStateKubeConfigDiscoverer: KubeConfigDiscovering {
    let urls: [URL]

    func discoverCandidateFiles() -> [URL] {
        urls
    }
}

private struct EmptyLastStateBookmarkStore: BookmarkStore {
    func loadRecords() throws -> [BookmarkRecord] { [] }
    func saveRecords(_ records: [BookmarkRecord]) throws {}
}

private struct EmptyLastStateSavedWorkspaceStore: SavedWorkspaceStoring {
    func loadSavedWorkspaces() -> [SavedWorkspaceSnapshot] { [] }
    func saveSavedWorkspaces(_ workspaces: [SavedWorkspaceSnapshot]) {}
}
