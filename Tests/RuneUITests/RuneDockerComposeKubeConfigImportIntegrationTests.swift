import Foundation
import XCTest
@testable import RuneCore
@testable import RuneKube
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

/// Exercises the same staged Add Cluster transaction used by the UI against both
/// local k3s APIs. The other Docker ViewModel tests intentionally inject their
/// source directly; this test guards the import boundary itself.
@MainActor
final class RuneDockerComposeKubeConfigImportIntegrationTests: XCTestCase {
    func testAddClusterImportPublishesAndActivatesBothDockerComposeContexts() async throws {
        guard ProcessInfo.processInfo.environment["RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS"] == "1" else {
            throw XCTSkip("Set RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 to run Docker Compose import integration tests.")
        }

        let kubeconfig = repoRoot.appendingPathComponent("docker-compose/generated/rune-fake-kubeconfig.yaml")
        let raw = try String(contentsOf: kubeconfig, encoding: .utf8)
        try assertLocalOnlyFixture(raw)

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneDockerComposeImport.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeEnableDemoCluster = false
        UserDefaults.standard.runeSimpleMode = true
        defer {
            restore(previousDemoSetting, key: RuneSettingsKeys.enableDemoCluster)
            restore(previousSimpleMode, key: RuneSettingsKeys.simpleMode)
        }

        let bookmarks = DockerImportBookmarkStore()
        let state = RuneAppState()
        let client = KubernetesClient(commandTimeout: 10)
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: client,
            bookmarkManager: BookmarkManager(store: bookmarks),
            kubeConfigDiscoverer: KubeConfigDiscoverer(
                environmentProvider: { ["RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY": "1"] },
                homeDirectoryProvider: { directory },
                fileExists: { _ in false }
            ),
            contextPreferences: FileBackedContextPreferencesStore(
                url: directory.appendingPathComponent("context-preferences.json")
            ),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                rootDirectory: directory.appendingPathComponent("imports", isDirectory: true)
            ),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)

        viewModel.importKubeConfig(raw: raw, sourceName: "local-compose-kubeconfig.yaml")

        try await waitUntil {
            viewModel.isKubeConfigImportConfirmationPending || state.lastError != nil
        }
        XCTAssertNil(state.lastError)
        XCTAssertTrue(viewModel.canConfirmKubeConfigImport)
        XCTAssertEqual(viewModel.kubeConfigImportReviewMode, .preflight)
        XCTAssertEqual(
            Set(viewModel.kubeConfigImportReviews.flatMap { $0.contexts.map(\.name) }),
            ["fake-lattice-spark", "fake-orbit-mesh"]
        )
        XCTAssertTrue(state.kubeConfigSources.isEmpty, "Preflight must not publish a source.")
        XCTAssertTrue(bookmarks.snapshot.isEmpty, "Preflight must not publish a bookmark.")
        for token in bearerTokens(in: raw) {
            XCTAssertFalse(
                viewModel.kubeConfigImportReviews.contains { $0.redactedPreview.contains(token) },
                "The review must redact bearer tokens."
            )
        }

        viewModel.confirmKubeConfigImport()

        try await waitUntil(timeout: 30) {
            !viewModel.isKubeConfigImportConfirmationPending
                && !viewModel.isCommittingKubeConfigImport
                && state.kubeConfigSources.count == 1
                && Set(state.contexts.map(\.name)) == ["fake-lattice-spark", "fake-orbit-mesh"]
                && state.selectedContext?.name == "fake-orbit-mesh"
                && !state.isLoading
        }
        XCTAssertNil(state.lastError)
        XCTAssertEqual(bookmarks.snapshot.count, 1)
        let importedSource = try XCTUnwrap(state.kubeConfigSources.first?.url)
        XCTAssertNotEqual(importedSource.standardizedFileURL, kubeconfig.standardizedFileURL)
        XCTAssertTrue(importedSource.standardizedFileURL.path.hasPrefix(directory.standardizedFileURL.path))
        XCTAssertEqual(
            bookmarks.snapshot.first.map { URL(fileURLWithPath: $0.path).standardizedFileURL },
            importedSource.standardizedFileURL
        )
        XCTAssertTrue(FileManager.default.fileExists(atPath: importedSource.path))

        try await assertDeployment(
            "orbit-lens",
            context: "fake-orbit-mesh",
            namespace: "alpha-zone",
            viewModel: viewModel,
            state: state
        )
        try await assertDeployment(
            "aurora-signal-weaver",
            context: "fake-lattice-spark",
            namespace: "delta-zone",
            viewModel: viewModel,
            state: state
        )

        XCTAssertNil(state.lastError)
        XCTAssertEqual(state.kubeConfigSources.map(\.url), [importedSource])
    }

    private func assertDeployment(
        _ deploymentName: String,
        context contextName: String,
        namespace: String,
        viewModel: RuneAppViewModel,
        state: RuneAppState
    ) async throws {
        viewModel.setContext(KubeContext(name: contextName))
        try await waitUntil(timeout: 30) {
            state.selectedContext?.name == contextName
                && state.namespaces.contains(namespace)
                && !state.isLoading
        }

        viewModel.setNamespace(namespace)
        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.deployment)
        try await waitUntil(timeout: 30) {
            state.selectedContext?.name == contextName
                && state.selectedNamespace == namespace
                && state.selectedSection == .workloads
                && state.selectedWorkloadKind == .deployment
                && state.deployments.contains { $0.name == deploymentName }
                && !state.isLoading
        }
    }

    private func assertLocalOnlyFixture(_ raw: String) throws {
        let requiredMarkers = [
            "name: fake-orbit-mesh",
            "name: fake-lattice-spark",
            "server: https://127.0.0.1:16443",
            "server: https://127.0.0.1:17443",
        ]
        guard requiredMarkers.allSatisfy(raw.contains) else {
            throw DockerImportFixtureError.unsafeKubeconfig
        }
        let serverLines = raw.split(whereSeparator: \.isNewline)
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { $0.hasPrefix("server:") }
        guard serverLines.count == 2,
              serverLines.allSatisfy({ $0.contains("https://127.0.0.1:") }) else {
            throw DockerImportFixtureError.unsafeKubeconfig
        }
    }

    private func bearerTokens(in raw: String) -> [String] {
        raw.split(whereSeparator: \.isNewline).compactMap { line in
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard trimmed.hasPrefix("token:") else { return nil }
            let token = trimmed.dropFirst("token:".count).trimmingCharacters(in: .whitespaces)
            return token.isEmpty ? nil : token
        }
    }

    private func waitUntil(
        timeout: TimeInterval = 5,
        file: StaticString = #filePath,
        line: UInt = #line,
        predicate: @escaping @MainActor () -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !predicate() {
            if Date() >= deadline {
                XCTFail("Timed out waiting for Docker Compose import condition", file: file, line: line)
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    private func restore(_ value: Any?, key: String) {
        if let value {
            UserDefaults.standard.set(value, forKey: key)
        } else {
            UserDefaults.standard.removeObject(forKey: key)
        }
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}

private enum DockerImportFixtureError: LocalizedError {
    case unsafeKubeconfig

    var errorDescription: String? {
        "Docker Compose import test refused a kubeconfig that was not exactly the two local fake clusters."
    }
}

private final class DockerImportBookmarkStore: BookmarkStore, @unchecked Sendable {
    private let lock = NSLock()
    private var records: [BookmarkRecord] = []

    var snapshot: [BookmarkRecord] {
        lock.withLock { records }
    }

    func loadRecords() throws -> [BookmarkRecord] {
        snapshot
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        lock.withLock {
            self.records = records
        }
    }
}
