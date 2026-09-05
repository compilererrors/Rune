import Foundation
import Network
import RuneCore
import RuneFakeK8sSupport
import RuneKube
import RuneSecurity
import RuneStore
@testable import RuneUI
import XCTest

final class LastAppStateFailureRecoveryTests: XCTestCase {
    @MainActor
    func testImmediateFlushAfterTransientContextFailurePreservesPreviousSnapshot() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStateFailureRecoveryTests.context.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try syntheticKubeConfigYAML(
            server: "https://synthetic-context-failure.invalid",
            contextName: "synthetic-context-a",
            namespace: "synthetic-scope-a"
        ).write(
            to: kubeConfigURL,
            atomically: true,
            encoding: .utf8
        )

        let previousSnapshot = try scopedPodSnapshot(
            kubeConfigURL: kubeConfigURL,
            contextName: "synthetic-context-a",
            namespace: "synthetic-scope-a",
            podName: "synthetic-pod-a"
        )
        let store = FailureRecoveryLastAppStateStore(snapshot: previousSnapshot)
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: FailureRecoveryEmptyBookmarkStore()),
            kubeConfigDiscoverer: FailureRecoveryKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: store,
            kubeContextList: { _ in
                throw FailureRecoveryTestError.transientContextListFailure
            },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            state.lastError != nil
        }

        viewModel.persistLastAppStateNow()

        XCTAssertEqual(store.snapshot, previousSnapshot)
        XCTAssertEqual(
            store.saveCount,
            0,
            "A quit-style immediate flush must not replace the last good snapshot after a transient context failure."
        )
    }

    @MainActor
    func testPodListFailurePreservesStoredResourceSelectionDuringImmediateFlush() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let contextName = "synthetic-context-b"
        let namespace = "synthetic-scope-b"
        let podName = "synthetic-pod-b"
        let server = try FailureRecoveryPodListServer.start(namespace: namespace)
        defer { server.stop() }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStateFailureRecoveryTests.pods.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try FailureRecoveryPodListServer.kubeConfigYAML(
            serverPort: server.port,
            contextName: contextName,
            namespace: namespace
        ).write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let previousSnapshot = try scopedPodSnapshot(
            kubeConfigURL: kubeConfigURL,
            contextName: contextName,
            namespace: namespace,
            podName: podName
        )
        let store = FailureRecoveryLastAppStateStore(snapshot: previousSnapshot)
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 2),
            bookmarkManager: BookmarkManager(store: FailureRecoveryEmptyBookmarkStore()),
            kubeConfigDiscoverer: FailureRecoveryKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: store,
            kubeContextList: { _ in [KubeContext(name: contextName)] },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil(timeoutNanoseconds: 8_000_000_000) {
            state.selectedContext?.name == contextName
                && state.selectedNamespace == namespace
                && state.lastError?.contains("Synthetic transient pod list failure") == true
                && server.podListRequestCount > 0
        }
        try await Task.sleep(nanoseconds: 200_000_000)

        viewModel.persistLastAppStateNow()

        XCTAssertGreaterThan(server.namespaceListRequestCount, 0)
        XCTAssertGreaterThan(server.podListRequestCount, 0)
        XCTAssertEqual(store.snapshot, previousSnapshot)
        XCTAssertEqual(store.snapshot?.resourceName, podName)
        XCTAssertEqual(
            store.saveCount,
            0,
            "A failed pod-family hydration must not persist a fallback without the stored resource selection."
        )
    }

    @MainActor
    func testColdHelmSectionWithoutSavedResourceStillHydratesReleaseList() async throws {
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
            .appendingPathComponent("LastAppStateFailureRecoveryTests.helm.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let release = HelmReleaseSummary(
            name: "synthetic-release-c",
            namespace: "alpha-zone",
            revision: 4,
            updated: "2026-07-30T12:00:00Z",
            status: "deployed",
            chart: "synthetic-chart-1.0.0",
            appVersion: "1.0.0"
        )
        let helmRecorder = FailureRecoveryHelmListRecorder(releases: [release])
        let seedStore = FailureRecoveryLastAppStateStore()
        do {
            let persistedState = RuneAppState()
            persistedState.setSources([KubeConfigSource(url: kubeConfigURL)])
            persistedState.selectedContext = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
            persistedState.selectedNamespace = release.namespace
            persistedState.selectedSection = .helm
            persistedState.isHelmAllNamespaces = false
            let writer = RuneAppViewModel(
                state: persistedState,
                contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
                savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
                lastAppStateStore: seedStore,
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )
            writer.updateSavedWorkspaceInspectorState(
                SavedWorkspaceInspectorState(helmBrowserTabID: "releases")
            )
            writer.persistLastAppStateNow()
        }
        let previousSnapshot = try XCTUnwrap(seedStore.snapshot)
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: FailureRecoveryEmptyBookmarkStore()),
            kubeConfigDiscoverer: FailureRecoveryKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: FailureRecoveryLastAppStateStore(snapshot: previousSnapshot),
            helmReleaseList: { _, _, namespace, allNamespaces in
                helmRecorder.record(namespace: namespace, allNamespaces: allNamespaces)
            },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                && state.selectedNamespace == release.namespace
                && state.namespaces.contains(release.namespace)
        }
        try await Task.sleep(nanoseconds: 200_000_000)

        XCTAssertEqual(helmRecorder.callCount, 1)
        XCTAssertEqual(helmRecorder.lastNamespace, release.namespace)
        XCTAssertFalse(helmRecorder.lastAllNamespaces)
        XCTAssertEqual(state.helmReleases, [release])
    }

    @MainActor
    func testScopedHelmRestoreRetriesAfterFailureRestoresExactReleaseAndUnlocksImmediatePersistence() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSaveLastAppState = true
        UserDefaults.standard.runeSimpleMode = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
            restoreDefault(previousSimpleMode, key: RuneSettingsKeys.simpleMode)
        }

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LastAppStateFailureRecoveryTests.helm-retry.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let fallbackRelease = HelmReleaseSummary(
            name: "synthetic-release-fallback",
            namespace: "alpha-zone",
            revision: 1,
            updated: "2026-07-30T12:00:00Z",
            status: "deployed",
            chart: "synthetic-chart-1.0.0",
            appVersion: "1.0.0"
        )
        let expectedRelease = HelmReleaseSummary(
            name: "synthetic-release-restored",
            namespace: fallbackRelease.namespace,
            revision: 7,
            updated: "2026-07-30T12:05:00Z",
            status: "deployed",
            chart: "synthetic-chart-2.0.0",
            appVersion: "2.0.0"
        )
        let previousSnapshot = try scopedHelmSnapshot(
            kubeConfigURL: kubeConfigURL,
            contextName: RuneFakeK8sFixture.defaultContextName,
            namespace: expectedRelease.namespace,
            releases: [fallbackRelease, expectedRelease],
            selectedRelease: expectedRelease
        )
        XCTAssertNotNil(previousSnapshot.sourceScopeID)
        XCTAssertEqual(previousSnapshot.resourceName, expectedRelease.name)

        let store = FailureRecoveryLastAppStateStore(snapshot: previousSnapshot)
        let helmRecorder = FailureRecoverySequencedHelmListRecorder(
            failuresBeforeSuccess: 1,
            releases: [fallbackRelease, expectedRelease]
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: FailureRecoveryEmptyBookmarkStore()),
            kubeConfigDiscoverer: FailureRecoveryKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: store,
            helmReleaseList: { _, _, namespace, allNamespaces in
                try helmRecorder.record(namespace: namespace, allNamespaces: allNamespaces)
            },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            helmRecorder.callCount == 1
                && state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                && state.selectedNamespace == expectedRelease.namespace
        }

        viewModel.persistLastAppStateNow()
        XCTAssertEqual(store.saveCount, 0)
        XCTAssertEqual(store.snapshot, previousSnapshot)

        viewModel.refreshCurrentView(debounced: false)
        try await waitUntil {
            helmRecorder.callCount >= 2
                && state.selectedHelmRelease == expectedRelease
        }

        XCTAssertEqual(state.helmReleases, [fallbackRelease, expectedRelease])
        XCTAssertEqual(
            state.selectedHelmRelease,
            expectedRelease,
            "A successful retry must restore the saved release, not the first fallback row."
        )

        let saveCountBeforeImmediateFlush = store.saveCount
        viewModel.persistLastAppStateNow()

        XCTAssertEqual(store.saveCount, saveCountBeforeImmediateFlush + 1)
        XCTAssertEqual(store.snapshot?.resourceKind, "helmrelease")
        XCTAssertEqual(store.snapshot?.resourceName, expectedRelease.name)
        XCTAssertEqual(store.snapshot?.resourceNamespace, expectedRelease.namespace)
    }

    @MainActor
    func testAdvancedModeColdHelmRestoreHydratesReleaseAndOperatorFamilies() async throws {
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
            .appendingPathComponent("LastAppStateFailureRecoveryTests.helm-advanced.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let release = HelmReleaseSummary(
            name: "synthetic-release-advanced",
            namespace: "alpha-zone",
            revision: 2,
            updated: "2026-07-30T12:10:00Z",
            status: "deployed",
            chart: "synthetic-chart-3.0.0",
            appVersion: "3.0.0"
        )
        let operatorResource = OperatorResourceSummary(
            family: "synthetic.example.invalid",
            kind: "SyntheticResource",
            apiPath: "syntheticresources.synthetic.example.invalid",
            name: "synthetic-operator-resource",
            namespace: release.namespace,
            status: "Ready",
            message: "Synthetic operator resource is ready."
        )
        let previousSnapshot = try scopedHelmSnapshot(
            kubeConfigURL: kubeConfigURL,
            contextName: RuneFakeK8sFixture.defaultContextName,
            namespace: release.namespace,
            releases: [],
            selectedRelease: nil
        )
        let helmRecorder = FailureRecoveryHelmListRecorder(releases: [release])
        let operatorRecorder = FailureRecoveryOperatorListRecorder(resources: [operatorResource])
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: FailureRecoveryEmptyBookmarkStore()),
            kubeConfigDiscoverer: FailureRecoveryKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: FailureRecoveryLastAppStateStore(snapshot: previousSnapshot),
            helmReleaseList: { _, _, namespace, allNamespaces in
                helmRecorder.record(namespace: namespace, allNamespaces: allNamespaces)
            },
            operatorResourceList: { _, _, namespace in
                operatorRecorder.record(namespace: namespace)
            },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            helmRecorder.callCount == 1
                && operatorRecorder.callCount == 1
                && state.helmReleases == [release]
                && state.operatorResources == [operatorResource]
        }

        XCTAssertEqual(helmRecorder.lastNamespace, release.namespace)
        XCTAssertEqual(operatorRecorder.lastNamespace, release.namespace)
        XCTAssertEqual(viewModel.helmBrowserResourceFamily, .helmReleases)
        XCTAssertEqual(state.selectedHelmRelease, release)
        XCTAssertNil(state.selectedOperatorResource)
    }

    @MainActor
    func testAdvancedColdRestoreKeepsPendingUntilExactSecondOperatorResourceIsSelected() async throws {
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
            .appendingPathComponent("LastAppStateFailureRecoveryTests.operator-exact.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let namespace = "alpha-zone"
        let fallbackResource = syntheticOperatorResource(
            name: "synthetic-operator-fallback",
            namespace: namespace
        )
        let expectedResource = syntheticOperatorResource(
            name: "synthetic-operator-restored",
            namespace: namespace
        )
        let previousSnapshot = try scopedOperatorSnapshot(
            kubeConfigURL: kubeConfigURL,
            contextName: RuneFakeK8sFixture.defaultContextName,
            namespace: namespace,
            resources: [fallbackResource, expectedResource],
            selectedResource: expectedResource
        )
        XCTAssertNotNil(previousSnapshot.sourceScopeID)
        XCTAssertEqual(previousSnapshot.resourceName, expectedResource.name)

        let store = FailureRecoveryLastAppStateStore(snapshot: previousSnapshot)
        let release = HelmReleaseSummary(
            name: "synthetic-release-during-operator-restore",
            namespace: namespace,
            revision: 1,
            updated: "2026-07-30T12:20:00Z",
            status: "deployed",
            chart: "synthetic-chart-4.0.0",
            appVersion: "4.0.0"
        )
        let helmRecorder = FailureRecoveryHelmListRecorder(releases: [release])
        let operatorRecorder = FailureRecoverySuspendedOperatorListRecorder(
            resources: [fallbackResource, expectedResource]
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: FailureRecoveryEmptyBookmarkStore()),
            kubeConfigDiscoverer: FailureRecoveryKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: store,
            helmReleaseList: { _, _, namespace, allNamespaces in
                helmRecorder.record(namespace: namespace, allNamespaces: allNamespaces)
            },
            operatorResourceList: { _, _, namespace in
                await operatorRecorder.record(namespace: namespace)
            },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            operatorRecorder.callCount == 1
                && helmRecorder.callCount == 1
                && state.selectedHelmRelease == release
        }

        viewModel.persistLastAppStateNow()
        XCTAssertEqual(store.saveCount, 0)
        XCTAssertEqual(store.snapshot, previousSnapshot)

        operatorRecorder.succeed()
        try await waitUntil {
            operatorRecorder.didComplete
                && state.selectedOperatorResource == expectedResource
        }

        XCTAssertEqual(state.operatorResources, [fallbackResource, expectedResource])
        XCTAssertEqual(
            state.selectedOperatorResource,
            expectedResource,
            "Cold restore must select the saved second operator row, not the first fallback."
        )
        XCTAssertNil(state.selectedHelmRelease)

        let saveCountBeforeImmediateFlush = store.saveCount
        viewModel.persistLastAppStateNow()

        XCTAssertEqual(store.saveCount, saveCountBeforeImmediateFlush + 1)
        XCTAssertEqual(store.snapshot?.resourceKind, expectedResource.apiPath)
        XCTAssertEqual(store.snapshot?.resourceName, expectedResource.name)
        XCTAssertEqual(store.snapshot?.resourceNamespace, expectedResource.namespace)
    }

    @MainActor
    func testAdvancedOperatorRetrySucceedsWhileReleaseFamilyStillFailsAndUnlocksPersistence() async throws {
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
            .appendingPathComponent("LastAppStateFailureRecoveryTests.operator-retry.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try server.kubeconfigYAML().write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let namespace = "alpha-zone"
        let fallbackResource = syntheticOperatorResource(
            name: "synthetic-retry-fallback",
            namespace: namespace
        )
        let expectedResource = syntheticOperatorResource(
            name: "synthetic-retry-restored",
            namespace: namespace
        )
        let previousSnapshot = try scopedOperatorSnapshot(
            kubeConfigURL: kubeConfigURL,
            contextName: RuneFakeK8sFixture.defaultContextName,
            namespace: namespace,
            resources: [fallbackResource, expectedResource],
            selectedResource: expectedResource
        )
        let store = FailureRecoveryLastAppStateStore(snapshot: previousSnapshot)
        let helmRecorder = FailureRecoverySequencedHelmListRecorder(
            failuresBeforeSuccess: 10,
            releases: []
        )
        let operatorRecorder = FailureRecoverySequencedOperatorListRecorder(
            failuresBeforeSuccess: 1,
            resources: [fallbackResource, expectedResource]
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: FailureRecoveryEmptyBookmarkStore()),
            kubeConfigDiscoverer: FailureRecoveryKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: store,
            helmReleaseList: { _, _, namespace, allNamespaces in
                try helmRecorder.record(namespace: namespace, allNamespaces: allNamespaces)
            },
            operatorResourceList: { _, _, namespace in
                try operatorRecorder.record(namespace: namespace)
            },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            helmRecorder.callCount == 1
                && operatorRecorder.callCount == 1
        }

        viewModel.persistLastAppStateNow()
        XCTAssertEqual(store.saveCount, 0)
        XCTAssertEqual(store.snapshot, previousSnapshot)

        viewModel.refreshCurrentView(debounced: false)
        try await waitUntil(timeoutNanoseconds: 8_000_000_000) {
            helmRecorder.callCount >= 2
                && operatorRecorder.callCount >= 2
                && state.selectedOperatorResource == expectedResource
        }

        XCTAssertTrue(state.helmReleases.isEmpty)
        XCTAssertEqual(state.operatorResources, [fallbackResource, expectedResource])
        XCTAssertEqual(
            state.selectedOperatorResource,
            expectedResource,
            "A release-family failure must not prevent exact operator restoration."
        )

        let saveCountBeforeImmediateFlush = store.saveCount
        viewModel.persistLastAppStateNow()

        XCTAssertEqual(store.saveCount, saveCountBeforeImmediateFlush + 1)
        XCTAssertEqual(store.snapshot?.resourceKind, expectedResource.apiPath)
        XCTAssertEqual(store.snapshot?.resourceName, expectedResource.name)
        XCTAssertEqual(store.snapshot?.resourceNamespace, expectedResource.namespace)
    }

    @MainActor
    private func scopedPodSnapshot(
        kubeConfigURL: URL,
        contextName: String,
        namespace: String,
        podName: String
    ) throws -> LastAppStateSnapshot {
        let pod = PodSummary(
            name: podName,
            namespace: namespace,
            status: "Running",
            containerNamesLine: "synthetic-container"
        )
        let seedStore = FailureRecoveryLastAppStateStore()
        let persistedState = RuneAppState()
        persistedState.setSources([KubeConfigSource(url: kubeConfigURL)])
        persistedState.selectedContext = KubeContext(name: contextName)
        persistedState.selectedNamespace = namespace
        persistedState.selectedSection = .workloads
        persistedState.selectedWorkloadKind = .pod
        persistedState.setPods([pod])
        let writer = RuneAppViewModel(
            state: persistedState,
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: seedStore,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        writer.persistLastAppStateNow()
        return try XCTUnwrap(seedStore.snapshot)
    }

    @MainActor
    private func scopedOperatorSnapshot(
        kubeConfigURL: URL,
        contextName: String,
        namespace: String,
        resources: [OperatorResourceSummary],
        selectedResource: OperatorResourceSummary
    ) throws -> LastAppStateSnapshot {
        let seedStore = FailureRecoveryLastAppStateStore()
        let persistedState = RuneAppState()
        persistedState.setSources([KubeConfigSource(url: kubeConfigURL)])
        persistedState.selectedContext = KubeContext(name: contextName)
        persistedState.selectedNamespace = namespace
        persistedState.selectedSection = .helm
        persistedState.isHelmAllNamespaces = false
        persistedState.setOperatorResources(resources, selectFallback: false)
        persistedState.setSelectedOperatorResource(selectedResource)
        let writer = RuneAppViewModel(
            state: persistedState,
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: seedStore,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        writer.updateSavedWorkspaceInspectorState(
            SavedWorkspaceInspectorState(helmBrowserTabID: "operatorResources")
        )
        writer.persistLastAppStateNow()
        return try XCTUnwrap(seedStore.snapshot)
    }

    private func syntheticOperatorResource(
        name: String,
        namespace: String
    ) -> OperatorResourceSummary {
        OperatorResourceSummary(
            family: "synthetic.example.invalid",
            kind: "SyntheticResource",
            apiPath: "syntheticresources.synthetic.example.invalid",
            name: name,
            namespace: namespace,
            status: "Ready",
            message: "Synthetic operator resource is ready."
        )
    }

    @MainActor
    private func scopedHelmSnapshot(
        kubeConfigURL: URL,
        contextName: String,
        namespace: String,
        releases: [HelmReleaseSummary],
        selectedRelease: HelmReleaseSummary?
    ) throws -> LastAppStateSnapshot {
        let seedStore = FailureRecoveryLastAppStateStore()
        let persistedState = RuneAppState()
        persistedState.setSources([KubeConfigSource(url: kubeConfigURL)])
        persistedState.selectedContext = KubeContext(name: contextName)
        persistedState.selectedNamespace = namespace
        persistedState.selectedSection = .helm
        persistedState.isHelmAllNamespaces = false
        persistedState.setHelmReleases(releases)
        persistedState.setSelectedHelmRelease(selectedRelease)
        let writer = RuneAppViewModel(
            state: persistedState,
            contextPreferences: FailureRecoveryEmptyContextPreferencesStore(),
            savedWorkspaceStore: FailureRecoveryEmptySavedWorkspaceStore(),
            lastAppStateStore: seedStore,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        writer.updateSavedWorkspaceInspectorState(
            SavedWorkspaceInspectorState(helmBrowserTabID: "releases")
        )
        writer.persistLastAppStateNow()
        return try XCTUnwrap(seedStore.snapshot)
    }

    private func syntheticKubeConfigYAML(
        server: String,
        contextName: String,
        namespace: String
    ) -> String {
        """
        apiVersion: v1
        kind: Config
        clusters:
        - name: synthetic-cluster
          cluster:
            server: \(server)
        contexts:
        - name: \(contextName)
          context:
            cluster: synthetic-cluster
            namespace: \(namespace)
            user: synthetic-user
        current-context: \(contextName)
        users:
        - name: synthetic-user
          user: {}
        """
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
                XCTFail("Timed out waiting for synthetic last-state recovery condition.")
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }
}

private enum FailureRecoveryTestError: Error {
    case transientContextListFailure
    case transientHelmReleaseListFailure
    case transientOperatorResourceListFailure
}

private final class FailureRecoveryLastAppStateStore: LastAppStateStoring {
    private(set) var snapshot: LastAppStateSnapshot?
    private(set) var saveCount = 0

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
    }
}

private struct FailureRecoveryKubeConfigDiscoverer: KubeConfigDiscovering {
    let urls: [URL]

    func discoverCandidateFiles() -> [URL] {
        urls
    }
}

private struct FailureRecoveryEmptyBookmarkStore: BookmarkStore {
    func loadRecords() throws -> [BookmarkRecord] { [] }
    func saveRecords(_ records: [BookmarkRecord]) throws {}
}

private struct FailureRecoveryEmptyContextPreferencesStore: ContextPreferencesStoring {
    func loadFavoriteContextNames() -> Set<String> { [] }
    func saveFavoriteContextNames(_ names: Set<String>) {}
}

private struct FailureRecoveryEmptySavedWorkspaceStore: SavedWorkspaceStoring {
    func loadSavedWorkspaces() -> [SavedWorkspaceSnapshot] { [] }
    func saveSavedWorkspaces(_ workspaces: [SavedWorkspaceSnapshot]) {}
}

@MainActor
private final class FailureRecoveryHelmListRecorder {
    private let releases: [HelmReleaseSummary]
    private(set) var callCount = 0
    private(set) var lastNamespace: String?
    private(set) var lastAllNamespaces = false

    init(releases: [HelmReleaseSummary]) {
        self.releases = releases
    }

    func record(namespace: String?, allNamespaces: Bool) -> [HelmReleaseSummary] {
        callCount += 1
        lastNamespace = namespace
        lastAllNamespaces = allNamespaces
        return releases
    }
}

@MainActor
private final class FailureRecoverySequencedHelmListRecorder {
    private let failuresBeforeSuccess: Int
    private let releases: [HelmReleaseSummary]
    private(set) var callCount = 0
    private(set) var lastNamespace: String?
    private(set) var lastAllNamespaces = false

    init(
        failuresBeforeSuccess: Int,
        releases: [HelmReleaseSummary]
    ) {
        self.failuresBeforeSuccess = failuresBeforeSuccess
        self.releases = releases
    }

    func record(namespace: String?, allNamespaces: Bool) throws -> [HelmReleaseSummary] {
        callCount += 1
        lastNamespace = namespace
        lastAllNamespaces = allNamespaces
        if callCount <= failuresBeforeSuccess {
            throw FailureRecoveryTestError.transientHelmReleaseListFailure
        }
        return releases
    }
}

@MainActor
private final class FailureRecoveryOperatorListRecorder {
    private let resources: [OperatorResourceSummary]
    private(set) var callCount = 0
    private(set) var lastNamespace: String?

    init(resources: [OperatorResourceSummary]) {
        self.resources = resources
    }

    func record(namespace: String) -> [OperatorResourceSummary] {
        callCount += 1
        lastNamespace = namespace
        return resources
    }
}

@MainActor
private final class FailureRecoverySequencedOperatorListRecorder {
    private let failuresBeforeSuccess: Int
    private let resources: [OperatorResourceSummary]
    private(set) var callCount = 0
    private(set) var lastNamespace: String?

    init(
        failuresBeforeSuccess: Int,
        resources: [OperatorResourceSummary]
    ) {
        self.failuresBeforeSuccess = failuresBeforeSuccess
        self.resources = resources
    }

    func record(namespace: String) throws -> [OperatorResourceSummary] {
        callCount += 1
        lastNamespace = namespace
        if callCount <= failuresBeforeSuccess {
            throw FailureRecoveryTestError.transientOperatorResourceListFailure
        }
        return resources
    }
}

@MainActor
private final class FailureRecoverySuspendedOperatorListRecorder {
    private let resources: [OperatorResourceSummary]
    private var continuation: CheckedContinuation<[OperatorResourceSummary], Never>?
    private(set) var callCount = 0
    private(set) var lastNamespace: String?
    private(set) var didComplete = false

    init(resources: [OperatorResourceSummary]) {
        self.resources = resources
    }

    func record(namespace: String) async -> [OperatorResourceSummary] {
        callCount += 1
        lastNamespace = namespace
        let resources = await withCheckedContinuation { continuation in
            self.continuation = continuation
        }
        didComplete = true
        return resources
    }

    func succeed() {
        continuation?.resume(returning: resources)
        continuation = nil
    }
}

private final class FailureRecoveryPodListServer: @unchecked Sendable {
    let port: UInt16

    private let listener: NWListener
    private let queue = DispatchQueue(label: "rune.tests.last-state-pod-failure")
    private let lock = NSLock()
    private var namespaceRequests = 0
    private var podRequests = 0
    private let namespace: String

    private init(listener: NWListener, port: UInt16, namespace: String) {
        self.listener = listener
        self.port = port
        self.namespace = namespace
    }

    var namespaceListRequestCount: Int {
        lock.withLock { namespaceRequests }
    }

    var podListRequestCount: Int {
        lock.withLock { podRequests }
    }

    static func start(namespace: String) throws -> FailureRecoveryPodListServer {
        let listener = try NWListener(using: .tcp, on: .any)
        let startResult = FailureRecoveryServerStartResult()
        let serverBox = FailureRecoveryServerReference()

        listener.newConnectionHandler = { connection in
            serverBox.server?.receive(connection: connection)
        }
        listener.stateUpdateHandler = { state in
            switch state {
            case .ready:
                guard let port = listener.port?.rawValue else {
                    startResult.resolve(.failure(URLError(.cannotConnectToHost)))
                    return
                }
                let server = FailureRecoveryPodListServer(
                    listener: listener,
                    port: port,
                    namespace: namespace
                )
                serverBox.server = server
                startResult.resolve(.success(server))
            case let .failed(error):
                startResult.resolve(.failure(error))
            default:
                break
            }
        }
        listener.start(queue: DispatchQueue(label: "rune.tests.last-state-pod-failure-listener"))
        return try startResult.wait()
    }

    static func kubeConfigYAML(
        serverPort: UInt16,
        contextName: String,
        namespace: String
    ) -> String {
        """
        apiVersion: v1
        kind: Config
        clusters:
        - name: synthetic-cluster
          cluster:
            server: http://127.0.0.1:\(serverPort)
        contexts:
        - name: \(contextName)
          context:
            cluster: synthetic-cluster
            namespace: \(namespace)
            user: synthetic-user
        current-context: \(contextName)
        users:
        - name: synthetic-user
          user: {}
        """
    }

    func stop() {
        listener.cancel()
    }

    private func receive(connection: NWConnection) {
        connection.start(queue: queue)
        receive(connection: connection, accumulated: Data())
    }

    private func receive(connection: NWConnection, accumulated: Data) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] data, _, isComplete, error in
            guard let self else {
                connection.cancel()
                return
            }
            var request = accumulated
            if let data {
                request.append(data)
            }
            guard request.range(of: Data("\r\n\r\n".utf8)) != nil else {
                if isComplete || error != nil {
                    connection.cancel()
                } else {
                    self.receive(connection: connection, accumulated: request)
                }
                return
            }

            let target = String(decoding: request, as: UTF8.self)
                .components(separatedBy: "\r\n")
                .first?
                .split(separator: " ")
                .dropFirst()
                .first
                .map(String.init) ?? "/"
            connection.send(
                content: self.responseData(for: target),
                completion: .contentProcessed { _ in
                    connection.cancel()
                }
            )
        }
    }

    private func responseData(for target: String) -> Data {
        let path = target.split(separator: "?", maxSplits: 1).first.map(String.init) ?? target
        if path == "/api/v1/namespaces" {
            lock.withLock {
                namespaceRequests += 1
            }
            return Self.httpResponse(
                status: 200,
                reason: "OK",
                body: """
                {"apiVersion":"v1","kind":"NamespaceList","metadata":{},"items":[{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"\(namespace)"}}]}
                """
            )
        }
        if path == "/api/v1/namespaces/\(namespace)/pods" {
            lock.withLock {
                podRequests += 1
            }
            return Self.httpResponse(
                status: 503,
                reason: "Service Unavailable",
                body: """
                {"apiVersion":"v1","kind":"Status","status":"Failure","message":"Synthetic transient pod list failure","reason":"ServiceUnavailable","code":503}
                """
            )
        }
        return Self.httpResponse(
            status: 404,
            reason: "Not Found",
            body: """
            {"apiVersion":"v1","kind":"Status","status":"Failure","message":"Synthetic route not found","reason":"NotFound","code":404}
            """
        )
    }

    private static func httpResponse(status: Int, reason: String, body: String) -> Data {
        let bodyData = Data(body.utf8)
        let header = [
            "HTTP/1.1 \(status) \(reason)",
            "Content-Type: application/json",
            "Content-Length: \(bodyData.count)",
            "Connection: close",
            "",
            ""
        ].joined(separator: "\r\n")
        var response = Data(header.utf8)
        response.append(bodyData)
        return response
    }
}

private final class FailureRecoveryServerReference: @unchecked Sendable {
    var server: FailureRecoveryPodListServer?
}

private final class FailureRecoveryServerStartResult: @unchecked Sendable {
    private let condition = NSCondition()
    private var result: Result<FailureRecoveryPodListServer, Error>?

    func resolve(_ result: Result<FailureRecoveryPodListServer, Error>) {
        condition.lock()
        guard self.result == nil else {
            condition.unlock()
            return
        }
        self.result = result
        condition.signal()
        condition.unlock()
    }

    func wait() throws -> FailureRecoveryPodListServer {
        condition.lock()
        while result == nil {
            condition.wait()
        }
        let resolved = result
        condition.unlock()
        return try resolved!.get()
    }
}
