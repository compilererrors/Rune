import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

@MainActor
final class KubeConfigImportReuseWorkflowTests: XCTestCase {
    func testConfirmedReimportReusesOwnedCopyAndBookmarkThenReadsSyntheticCluster() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let root = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }
        let store = AppOwnedKubeConfigImportStore(rootDirectory: root.appendingPathComponent("imports"))
        let raw = server.kubeconfigYAML()
        let existing = try store.saveImportedKubeConfig(raw: raw, sourceName: "first.yaml")
        let record = try store.record(forImportedKubeConfigAt: existing)
        let bookmarks = ReuseWorkflowBookmarks()
        let manager = BookmarkManager(store: bookmarks)
        try manager.addKubeConfig(url: existing)
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: existing)])
        let viewModel = makeViewModel(state: state, bookmarks: manager, store: store)
        viewModel.kubeConfigDuplicateHandlingChoice = .updateExisting

        viewModel.importKubeConfig(raw: raw, sourceName: "second.yaml")
        try await waitUntil { viewModel.isKubeConfigImportConfirmationPending && !viewModel.isPreparingKubeConfigImport }
        XCTAssertEqual(bookmarks.records.count, 1)
        XCTAssertEqual(try store.record(forImportedKubeConfigAt: existing), record)
        viewModel.confirmKubeConfigImport()
        try await waitUntil { !viewModel.isKubeConfigImportConfirmationPending && !viewModel.isCommittingKubeConfigImport }

        XCTAssertNil(state.lastError)
        XCTAssertEqual(state.kubeConfigSources.map(\.url), [existing])
        XCTAssertEqual(bookmarks.records.map(\.path), [existing.path])
        XCTAssertEqual(try store.record(forImportedKubeConfigAt: existing), record)
        XCTAssertEqual(record.origin.source, .importedFile)
        XCTAssertEqual(state.activeNotice?.title, "Connections reused")
        XCTAssertFalse(state.contexts.isEmpty)
        XCTAssertFalse(state.namespaces.isEmpty)
    }

    func testBookmarkFailureInMixedBatchPreservesPreviouslyWorkingReusedCopy() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let root = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }
        let imports = root.appendingPathComponent("imports")
        let store = AppOwnedKubeConfigImportStore(rootDirectory: imports)
        let raw = server.kubeconfigYAML()
        let existing = try store.saveImportedKubeConfig(raw: raw, sourceName: "existing.yaml")
        let record = try store.record(forImportedKubeConfigAt: existing)
        let originalDirectories = try FileManager.default.contentsOfDirectory(atPath: imports.path)
        let bookmarks = ReuseWorkflowBookmarks()
        let manager = BookmarkManager(store: bookmarks)
        try manager.addKubeConfig(url: existing)
        bookmarks.rejectWrites = true
        let first = root.appendingPathComponent("first.yaml")
        let second = root.appendingPathComponent("second.yaml")
        try raw.write(to: first, atomically: true, encoding: .utf8)
        try raw.replacingOccurrences(of: "fake-orbit-mesh", with: "synthetic-second-context").write(to: second, atomically: true, encoding: .utf8)
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: existing)])
        let viewModel = makeViewModel(state: state, bookmarks: manager, store: store, picker: ReuseWorkflowPicker(urls: [first, second]))
        viewModel.kubeConfigDuplicateHandlingChoice = .updateExisting
        viewModel.importKubeConfig()
        try await waitUntil { viewModel.isKubeConfigImportConfirmationPending && !viewModel.isPreparingKubeConfigImport }
        viewModel.confirmKubeConfigImport()
        try await waitUntil { state.lastError != nil && !viewModel.isCommittingKubeConfigImport }

        XCTAssertTrue(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertTrue(viewModel.canConfirmKubeConfigImport)
        XCTAssertEqual(state.kubeConfigSources.map(\.url), [existing])
        XCTAssertEqual(bookmarks.records.map(\.path), [existing.path])
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: imports.path), originalDirectories)
        XCTAssertEqual(try store.record(forImportedKubeConfigAt: existing), record)
        XCTAssertEqual(try String(contentsOf: first, encoding: .utf8), raw)
        XCTAssertTrue(FileManager.default.fileExists(atPath: second.path))
        viewModel.cancelKubeConfigImport()
    }

    private func makeViewModel(state: RuneAppState, bookmarks: BookmarkManager, store: AppOwnedKubeConfigImportStore, picker: ReuseWorkflowPicker = .init(urls: [])) -> RuneAppViewModel {
        RuneAppViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 2),
            bookmarkManager: bookmarks,
            picker: picker,
            kubeConfigDiscoverer: ReuseWorkflowDiscoverer(),
            kubeConfigImportStore: store,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
    }

    private func temporaryDirectory() throws -> URL {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("RuneImportReuseWorkflowTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }

    private func waitUntil(_ condition: () -> Bool) async throws {
        let deadline = ContinuousClock.now + .seconds(5)
        while !condition() {
            guard ContinuousClock.now < deadline else { throw RuneError.invalidInput(message: "Synthetic import workflow timed out.") }
            try await Task.sleep(for: .milliseconds(5))
        }
    }
}

private final class ReuseWorkflowBookmarks: BookmarkStore, @unchecked Sendable {
    private let lock = NSLock()
    private var storedRecords: [BookmarkRecord] = []
    private var rejects = false
    var records: [BookmarkRecord] { lock.withLock { storedRecords } }
    var rejectWrites: Bool {
        get { lock.withLock { rejects } }
        set { lock.withLock { rejects = newValue } }
    }
    func loadRecords() throws -> [BookmarkRecord] { records }
    func saveRecords(_ records: [BookmarkRecord]) throws {
        try lock.withLock {
            if rejects { throw RuneError.invalidInput(message: "Synthetic bookmark failure") }
            storedRecords = records
        }
    }
}

private struct ReuseWorkflowDiscoverer: KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL] { [] }
}

@MainActor
private struct ReuseWorkflowPicker: KubeConfigPicking {
    let urls: [URL]
    func pickFiles() throws -> [URL] { urls }
    func pickFolder() throws -> URL? { nil }
    func pickDefaultKubeConfig(at defaultURL: URL) throws -> URL? { urls.first }
}
