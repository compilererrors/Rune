import Foundation
import RuneCore
import RuneFakeK8sSupport
import RuneSecurity
import RuneStore
@testable import RuneUI
import XCTest

final class TerminalWorkspaceSourceScopeTests: XCTestCase {
    @MainActor
    func testTerminalWorkspaceSnapshotIsRejectedWhenKubeConfigSourceScopeChanges() throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("TerminalWorkspaceSourceScopeTests.terminal.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try syntheticSingleContextKubeConfig(server: "https://synthetic-old.invalid")
            .write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let context = KubeContext(name: "synthetic-shared-context")
        let namespace = "synthetic-shared-scope"
        let podName = "synthetic-shared-pod"
        let store = SourceScopeTerminalWorkspaceStore()
        let snapshot = TerminalWorkspaceStateSnapshot(
            sessions: [
                TerminalWorkspaceSessionSnapshot(
                    id: "synthetic-session",
                    contextName: context.name,
                    namespace: namespace,
                    podName: podName,
                    containerName: "synthetic-container",
                    shell: "sh"
                )
            ],
            activeSessionID: "synthetic-session",
            logTabs: [
                TerminalWorkspaceLogTabSnapshot(
                    id: "synthetic-log-tab",
                    podID: "\(namespace)/\(podName)",
                    namespace: namespace,
                    podName: podName
                )
            ],
            activeLogTabID: "synthetic-log-tab",
            selectedLogPodID: "\(namespace)/\(podName)",
            shellPodID: "\(namespace)/\(podName)",
            portForwardPodID: nil,
            inspectorTabID: "logs"
        )

        do {
            let writerState = RuneAppState()
            writerState.setSources([KubeConfigSource(url: kubeConfigURL)])
            writerState.selectedContext = context
            writerState.selectedNamespace = namespace
            let writer = RuneAppViewModel(
                state: writerState,
                terminalWorkspaceStateStore: store
            )
            writer.persistTerminalWorkspaceState(snapshot)
        }

        try syntheticSingleContextKubeConfig(server: "https://synthetic-new.invalid")
            .write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let readerState = RuneAppState()
        readerState.setSources([KubeConfigSource(url: kubeConfigURL)])
        readerState.selectedContext = context
        readerState.selectedNamespace = namespace
        let reader = RuneAppViewModel(
            state: readerState,
            terminalWorkspaceStateStore: store
        )

        XCTAssertNil(
            reader.loadPersistedTerminalWorkspaceState(),
            "Terminal workspace state from another kubeconfig source scope must not be restored even when context, namespace, and pod names collide."
        )
    }

    @MainActor
    func testTerminalWorkspaceScopeSurvivesCredentialRotationForSameCluster() throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("TerminalWorkspaceSourceScopeTests.credentials.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        let server = "https://synthetic-stable.invalid"
        try syntheticSingleContextKubeConfig(server: server, token: "synthetic-token-a")
            .write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let context = KubeContext(name: "synthetic-shared-context")
        let namespace = "synthetic-shared-scope"
        let store = SourceScopeTerminalWorkspaceStore()
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeConfigURL)])
        state.selectedContext = context
        state.selectedNamespace = namespace
        let writer = RuneAppViewModel(
            state: state,
            terminalWorkspaceStateStore: store
        )
        writer.persistTerminalWorkspaceState(
            TerminalWorkspaceStateSnapshot(
                sessions: [],
                activeSessionID: nil,
                logTabs: [
                    TerminalWorkspaceLogTabSnapshot(
                        id: "synthetic-log-tab",
                        podID: "\(namespace)/synthetic-pod",
                        namespace: namespace,
                        podName: "synthetic-pod"
                    )
                ],
                activeLogTabID: "synthetic-log-tab",
                selectedLogPodID: "\(namespace)/synthetic-pod",
                shellPodID: nil,
                portForwardPodID: nil,
                inspectorTabID: "logs"
            )
        )

        try syntheticSingleContextKubeConfig(server: server, token: "synthetic-token-b")
            .write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let readerState = RuneAppState()
        readerState.setSources([KubeConfigSource(url: kubeConfigURL)])
        readerState.selectedContext = context
        readerState.selectedNamespace = namespace
        let reader = RuneAppViewModel(
            state: readerState,
            terminalWorkspaceStateStore: store
        )

        XCTAssertNotNil(
            reader.loadPersistedTerminalWorkspaceState(),
            "Credential rotation must not look like a different cluster."
        )
    }

    @MainActor
    func testDuplicateImportChangingServerRetiresTerminalSessionBeforeNewScopeCanPersistIt() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let context = KubeContext(name: "synthetic-shared-context")
        let namespaceFixture = try XCTUnwrap(RuneFakeK8sFixture.defaultContexts.first?.namespaces.first)
        let namespace = namespaceFixture.name
        let cluster = RuneFakeK8sCluster(
            contextName: context.name,
            defaultNamespace: namespace,
            namespaces: [namespaceFixture],
            nodes: []
        )
        let replacementServer = try await RuneFakeK8sRESTServer.start(
            fixture: RuneFakeK8sFixture(contexts: [cluster]),
            contextName: context.name
        )
        defer { replacementServer.stop() }
        let replacementServerURL = try XCTUnwrap(
            serverURL(in: replacementServer.kubeconfigYAML())
        )

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "TerminalWorkspaceSourceScopeTests.runtimeImport.\(UUID().uuidString)",
                isDirectory: true
            )
        let importsDirectory = directory.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let originalSourceURL = directory.appendingPathComponent("synthetic-original-kubeconfig")
        try syntheticSingleContextKubeConfig(
            server: "https://synthetic-original.invalid",
            namespace: namespace
        )
        .write(to: originalSourceURL, atomically: true, encoding: .utf8)

        let defaultsSuite = "TerminalWorkspaceSourceScopeTests.runtimeImportPreferences.\(UUID().uuidString)"
        let contextDefaults = try XCTUnwrap(UserDefaults(suiteName: defaultsSuite))
        defer { contextDefaults.removePersistentDomain(forName: defaultsSuite) }

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: originalSourceURL)])
        state.selectedContext = context
        state.selectedNamespace = namespace
        state.setTerminalSession(PodTerminalSession(
            id: "synthetic-runtime-session",
            contextName: context.name,
            namespace: namespace,
            podName: namespaceFixture.pods[0].name,
            containerName: nil,
            shell: "sh",
            status: .connected
        ))

        let terminalStore = SourceScopeTerminalWorkspaceStore()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: SourceScopeEmptyBookmarkStore()),
            kubeConfigDiscoverer: SourceScopeFixedKubeConfigDiscoverer(urls: [originalSourceURL]),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
            savedWorkspaceStore: SourceScopeEmptySavedWorkspaceStore(),
            terminalWorkspaceStateStore: terminalStore,
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: importsDirectory),
            kubeContextList: { _ in [context] },
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        viewModel.persistTerminalWorkspaceState(
            terminalSnapshot(
                sessions: state.terminalSessions,
                activeSessionID: state.activeTerminalSessionID
            )
        )
        XCTAssertNotNil(viewModel.loadPersistedTerminalWorkspaceState())

        viewModel.kubeConfigDuplicateHandlingChoice = .updateExisting
        viewModel.importKubeConfig(
            raw: syntheticSingleContextKubeConfig(
                server: replacementServerURL,
                namespace: namespace
            ),
            sourceName: "synthetic-replacement.yaml"
        )
        try await waitUntil {
            viewModel.isKubeConfigImportConfirmationPending && viewModel.canConfirmKubeConfigImport
        }
        viewModel.confirmKubeConfigImport()
        try await waitUntil {
            state.kubeConfigSources.count == 2 && !viewModel.isCommittingKubeConfigImport
        }

        XCTAssertTrue(
            state.terminalSessions.isEmpty,
            "Changing the server behind a same-named context must retire live sessions from the previous source scope."
        )
        XCTAssertNil(
            viewModel.loadPersistedTerminalWorkspaceState(),
            "The terminal workspace written for the previous server must be rejected in the replacement source scope."
        )

        viewModel.persistTerminalWorkspaceState(
            terminalSnapshot(
                sessions: state.terminalSessions,
                activeSessionID: state.activeTerminalSessionID
            )
        )
        XCTAssertNil(
            viewModel.loadPersistedTerminalWorkspaceState(),
            "A runtime source change must clear sessions before the persistence lifecycle can stamp them with the replacement scope."
        )
    }

    @MainActor
    func testUnscopedLegacyLastAppSnapshotCannotRestoreClusterBoundState() async throws {
        let previousSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.saveLastAppState)
        UserDefaults.standard.runeSaveLastAppState = true
        defer {
            restoreDefault(previousSetting, key: RuneSettingsKeys.saveLastAppState)
        }

        let fixtureNamespaces = try XCTUnwrap(RuneFakeK8sFixture.defaultContexts.first?.namespaces)
        let currentNamespace = try XCTUnwrap(fixtureNamespaces.first)
        let legacyNamespace = try XCTUnwrap(fixtureNamespaces.dropFirst().first)
        let currentContext = RuneFakeK8sCluster(
            contextName: "synthetic-a-current-context",
            defaultNamespace: currentNamespace.name,
            namespaces: [currentNamespace],
            nodes: []
        )
        let legacyContext = RuneFakeK8sCluster(
            contextName: "synthetic-z-legacy-context",
            defaultNamespace: legacyNamespace.name,
            namespaces: [legacyNamespace],
            nodes: []
        )
        let fixture = RuneFakeK8sFixture(contexts: [currentContext, legacyContext])
        let currentServer = try await RuneFakeK8sRESTServer.start(
            fixture: fixture,
            contextName: currentContext.contextName
        )
        let legacyServer = try await RuneFakeK8sRESTServer.start(
            fixture: fixture,
            contextName: legacyContext.contextName
        )
        defer {
            currentServer.stop()
            legacyServer.stop()
        }

        let currentServerURL = try XCTUnwrap(serverURL(in: currentServer.kubeconfigYAML()))
        let legacyServerURL = try XCTUnwrap(serverURL(in: legacyServer.kubeconfigYAML()))
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("TerminalWorkspaceSourceScopeTests.legacy.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeConfigURL = directory.appendingPathComponent("synthetic-kubeconfig")
        try syntheticTwoContextKubeConfig(
            currentContext: currentContext,
            currentServerURL: currentServerURL,
            legacyContext: legacyContext,
            legacyServerURL: legacyServerURL
        )
        .write(to: kubeConfigURL, atomically: true, encoding: .utf8)

        let defaultsSuite = "TerminalWorkspaceSourceScopeTests.preferences.\(UUID().uuidString)"
        let contextDefaults = try XCTUnwrap(UserDefaults(suiteName: defaultsSuite))
        defer { contextDefaults.removePersistentDomain(forName: defaultsSuite) }

        let legacySnapshot = LastAppStateSnapshot(
            contextName: legacyContext.contextName,
            namespace: legacyNamespace.name,
            sectionID: RuneSection.workloads.rawValue,
            workloadKindID: KubeResourceKind.pod.rawValue,
            resourceKind: KubeResourceKind.pod.kubernetesResourceName,
            resourceName: legacyNamespace.pods[0].name,
            resourceNamespace: legacyNamespace.name
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: SourceScopeEmptyBookmarkStore()),
            kubeConfigDiscoverer: SourceScopeFixedKubeConfigDiscoverer(urls: [kubeConfigURL]),
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
            savedWorkspaceStore: SourceScopeEmptySavedWorkspaceStore(),
            lastAppStateStore: SourceScopeLastAppStateStore(snapshot: legacySnapshot),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.bootstrapIfNeeded()
        try await waitUntil {
            state.selectedPod != nil && !state.selectedNamespace.isEmpty
        }

        XCTAssertEqual(state.selectedContext?.name, currentContext.contextName)
        XCTAssertEqual(state.selectedNamespace, currentNamespace.name)
        XCTAssertTrue(
            currentNamespace.pods.contains { $0.name == state.selectedPod?.name },
            "Fallback selection must come from the live current namespace."
        )
        XCTAssertNotEqual(state.selectedPod?.name, legacyNamespace.pods[0].name)
    }

    private func restoreDefault(_ value: Any?, key: String) {
        if let value {
            UserDefaults.standard.set(value, forKey: key)
        } else {
            UserDefaults.standard.removeObject(forKey: key)
        }
    }

    private func syntheticSingleContextKubeConfig(
        server: String,
        namespace: String = "synthetic-shared-scope",
        token: String? = nil
    ) -> String {
        let userBlock = token.map { "    token: \($0)" } ?? "    {}"
        return """
        apiVersion: v1
        kind: Config
        current-context: synthetic-shared-context
        clusters:
        - name: synthetic-shared-cluster
          cluster:
            server: \(server)
        contexts:
        - name: synthetic-shared-context
          context:
            cluster: synthetic-shared-cluster
            namespace: \(namespace)
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
        \(userBlock)
        """
    }

    private func terminalSnapshot(
        sessions: [PodTerminalSession],
        activeSessionID: String?
    ) -> TerminalWorkspaceStateSnapshot {
        TerminalWorkspaceStateSnapshot(
            sessions: sessions.map(TerminalWorkspaceSessionSnapshot.init(session:)),
            activeSessionID: activeSessionID,
            logTabs: [],
            activeLogTabID: nil,
            selectedLogPodID: nil,
            shellPodID: nil,
            portForwardPodID: nil,
            inspectorTabID: nil
        )
    }

    private func syntheticTwoContextKubeConfig(
        currentContext: RuneFakeK8sCluster,
        currentServerURL: String,
        legacyContext: RuneFakeK8sCluster,
        legacyServerURL: String
    ) -> String {
        """
        apiVersion: v1
        kind: Config
        current-context: \(currentContext.contextName)
        clusters:
        - name: synthetic-current-cluster
          cluster:
            server: \(currentServerURL)
        - name: synthetic-legacy-cluster
          cluster:
            server: \(legacyServerURL)
        contexts:
        - name: \(currentContext.contextName)
          context:
            cluster: synthetic-current-cluster
            namespace: \(currentContext.defaultNamespace)
            user: synthetic-user
        - name: \(legacyContext.contextName)
          context:
            cluster: synthetic-legacy-cluster
            namespace: \(legacyContext.defaultNamespace)
            user: synthetic-user
        users:
        - name: synthetic-user
          user: {}
        """
    }

    private func serverURL(in kubeConfig: String) -> String? {
        kubeConfig
            .split(whereSeparator: \.isNewline)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .first { $0.hasPrefix("server: ") }?
            .dropFirst("server: ".count)
            .description
    }

    @MainActor
    private func waitUntil(
        timeoutNanoseconds: UInt64 = 5_000_000_000,
        condition: @escaping @MainActor () -> Bool
    ) async throws {
        let start = ContinuousClock.now
        while !condition() {
            if start.duration(to: .now) > .nanoseconds(Int64(timeoutNanoseconds)) {
                XCTFail("Timed out waiting for synthetic source-scope state.")
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }
}

private final class SourceScopeTerminalWorkspaceStore: TerminalWorkspaceStateStoring {
    private var snapshot: TerminalWorkspaceStateSnapshot?

    func loadTerminalWorkspaceState() -> TerminalWorkspaceStateSnapshot? {
        snapshot
    }

    func saveTerminalWorkspaceState(_ snapshot: TerminalWorkspaceStateSnapshot) {
        self.snapshot = snapshot.isEmpty ? nil : snapshot
    }

    func clearTerminalWorkspaceState() {
        snapshot = nil
    }
}

private final class SourceScopeLastAppStateStore: LastAppStateStoring {
    private let snapshot: LastAppStateSnapshot

    init(snapshot: LastAppStateSnapshot) {
        self.snapshot = snapshot
    }

    func loadLastAppState() -> LastAppStateSnapshot? {
        snapshot
    }

    func saveLastAppState(_ snapshot: LastAppStateSnapshot) {}
    func clearLastAppState() {}
}

private struct SourceScopeFixedKubeConfigDiscoverer: KubeConfigDiscovering {
    let urls: [URL]

    func discoverCandidateFiles() -> [URL] {
        urls
    }
}

private struct SourceScopeEmptyBookmarkStore: BookmarkStore {
    func loadRecords() throws -> [BookmarkRecord] { [] }
    func saveRecords(_ records: [BookmarkRecord]) throws {}
}

private struct SourceScopeEmptySavedWorkspaceStore: SavedWorkspaceStoring {
    func loadSavedWorkspaces() -> [SavedWorkspaceSnapshot] { [] }
    func saveSavedWorkspaces(_ workspaces: [SavedWorkspaceSnapshot]) {}
}
