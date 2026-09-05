import Foundation
import XCTest
@testable import RuneCore
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

@MainActor
final class PendingWriteOperationLifecycleTests: XCTestCase {
    func testChangedKubeConfigInvalidatesPreviouslyReviewedWrite() async throws {
        let harness = try makeHarness()
        defer { harness.cleanup() }
        harness.viewModel.pendingWriteAction = rollback

        try Data("apiVersion: v1\n# synthetic replacement\n".utf8).write(to: harness.config)
        harness.viewModel.confirmPendingWriteAction()

        XCTAssertNil(harness.viewModel.pendingWriteAction)
        XCTAssertTrue(harness.state.lastError?.contains("kubeconfig changed") == true)
        let requests = await harness.runner.requests
        XCTAssertTrue(requests.isEmpty)
        XCTAssertTrue(harness.state.writeAuditLog.isEmpty)
    }

    func testSameSizeKubeConfigReplacementWithPreservedDateInvalidatesReview() async throws {
        let harness = try makeHarness()
        defer { harness.cleanup() }
        harness.viewModel.pendingWriteAction = rollback
        let attributes = try FileManager.default.attributesOfItem(atPath: harness.config.path)

        // Change content without changing the metadata used by background refresh fingerprints.
        try Data("apiVersion: v2\n".utf8).write(to: harness.config)
        try FileManager.default.setAttributes(
            [.modificationDate: try XCTUnwrap(attributes[.modificationDate])],
            ofItemAtPath: harness.config.path
        )
        harness.viewModel.confirmPendingWriteAction()

        XCTAssertNil(harness.viewModel.pendingWriteAction)
        XCTAssertTrue(harness.state.lastError?.contains("kubeconfig changed") == true)
        let requests = await harness.runner.requests
        XCTAssertTrue(requests.isEmpty)
    }

    func testUnreadableReviewedSourceCannotBeConfirmed() async throws {
        let harness = try makeHarness()
        defer { harness.cleanup() }
        try FileManager.default.removeItem(at: harness.config)
        harness.viewModel.pendingWriteAction = rollback
        harness.viewModel.confirmPendingWriteAction()

        XCTAssertNil(harness.viewModel.pendingWriteAction)
        XCTAssertNotNil(harness.state.lastError)
        let requests = await harness.runner.requests
        XCTAssertTrue(requests.isEmpty)
    }

    func testSymlinkedKubeConfigCanBeReviewedAndExecuted() async throws {
        try await withHelmDryRun(false) {
            let harness = try makeHarness()
            defer { harness.cleanup() }
            let linkedConfig = harness.directory.appendingPathComponent("linked-config.yaml")
            try FileManager.default.createSymbolicLink(at: linkedConfig, withDestinationURL: harness.config)
            harness.state.setSources([KubeConfigSource(url: linkedConfig)])
            harness.viewModel.pendingWriteAction = rollback
            harness.viewModel.confirmPendingWriteAction()
            try await waitUntil { await harness.runner.isSuspended }

            let requests = await harness.runner.requests
            XCTAssertEqual(requests.first?.sources, [KubeConfigSource(url: linkedConfig)])
            XCTAssertNil(harness.state.lastError)
            await harness.runner.complete(failing: true)
            try await waitUntil { !harness.state.writeAuditLog.isEmpty }
        }
    }

    func testReadOnlyEnabledDuringDryRunPreventsRollback() async throws {
        try await withHelmDryRun(true) {
            let harness = try makeHarness()
            defer { harness.cleanup() }
            harness.viewModel.pendingWriteAction = rollback
            harness.viewModel.confirmPendingWriteAction()
            try await waitUntil { await harness.runner.isSuspended }

            harness.state.isReadOnlyMode = true
            await harness.runner.complete()
            try await waitUntil { !harness.state.writeAuditLog.isEmpty }

            let requests = await harness.runner.requests
            XCTAssertEqual(requests.map(\.dryRun), [true])
            XCTAssertEqual(harness.state.writeAuditLog.first?.status, "Failed")
            XCTAssertEqual(harness.state.lastError, RuneError.readOnlyMode.localizedDescription)
        }
    }

    func testKubeConfigRotationDuringDryRunPreventsRollback() async throws {
        try await withHelmDryRun(true) {
            let harness = try makeHarness()
            defer { harness.cleanup() }
            harness.viewModel.pendingWriteAction = rollback
            harness.viewModel.confirmPendingWriteAction()
            try await waitUntil { await harness.runner.isSuspended }

            try Data("apiVersion: v1\n# synthetic new target\n".utf8).write(to: harness.config)
            await harness.runner.complete()
            try await waitUntil { !harness.state.writeAuditLog.isEmpty }

            let requests = await harness.runner.requests
            XCTAssertEqual(requests.map(\.dryRun), [true])
            XCTAssertEqual(harness.state.writeAuditLog.first?.status, "Failed")
            XCTAssertTrue(harness.state.writeAuditLog.first?.message.contains("kubeconfig changed") == true)
        }
    }

    func testLateWriteFailureDoesNotReplaceErrorInNewContext() async throws {
        try await withHelmDryRun(false) {
            let harness = try makeHarness()
            defer { harness.cleanup() }
            harness.viewModel.pendingWriteAction = rollback
            harness.viewModel.confirmPendingWriteAction()
            try await waitUntil { await harness.runner.isSuspended }

            harness.state.selectedContext = KubeContext(name: "synthetic-current")
            harness.state.selectedNamespace = "synthetic-current-namespace"
            let currentError = RuneError.invalidInput(message: "Synthetic current-context error")
            harness.state.setError(currentError)
            await harness.runner.complete(failing: true)
            try await waitUntil { !harness.state.writeAuditLog.isEmpty }

            XCTAssertEqual(harness.state.lastError, currentError.localizedDescription)
            XCTAssertEqual(harness.state.writeAuditLog.first?.status, "Failed")
            XCTAssertEqual(harness.state.writeAuditLog.first?.contextName, "synthetic-reviewed")
        }
    }

    func testFailureFromRotatedSourceCannotReplaceCurrentError() async throws {
        try await withHelmDryRun(false) {
            let harness = try makeHarness()
            defer { harness.cleanup() }
            harness.viewModel.pendingWriteAction = rollback
            harness.viewModel.confirmPendingWriteAction()
            try await waitUntil { await harness.runner.isSuspended }

            try Data("apiVersion: v1\n# synthetic source rotation\n".utf8).write(to: harness.config)
            let currentError = RuneError.invalidInput(message: "Synthetic replacement-source error")
            harness.state.setError(currentError)
            await harness.runner.complete(failing: true)
            try await waitUntil { !harness.state.writeAuditLog.isEmpty }

            XCTAssertEqual(harness.state.lastError, currentError.localizedDescription)
            XCTAssertEqual(harness.state.writeAuditLog.first?.status, "Failed")
        }
    }

    func testHelmRollbackAuditUsesReleaseNamespaceFromAllNamespaceSelection() async throws {
        try await withHelmDryRun(false) {
            let harness = try makeHarness()
            defer { harness.cleanup() }
            harness.state.selectedNamespace = "synthetic-active-namespace"
            harness.state.isHelmAllNamespaces = true
            harness.viewModel.pendingWriteAction = rollback
            harness.viewModel.confirmPendingWriteAction()
            try await waitUntil { await harness.runner.isSuspended }
            await harness.runner.complete(failing: true)
            try await waitUntil { !harness.state.writeAuditLog.isEmpty }

            let requests = await harness.runner.requests
            XCTAssertEqual(requests.first?.namespace, "synthetic-namespace")
            XCTAssertEqual(harness.state.writeAuditLog.first?.namespace, "synthetic-namespace")
        }
    }

    func testReviewTargetUsesCapturedScopeAndActualResourceNamespace() throws {
        let harness = try makeHarness()
        defer { harness.cleanup() }
        harness.viewModel.pendingWriteAction = rollback
        harness.state.selectedContext = KubeContext(name: "synthetic-other")
        harness.state.selectedNamespace = "synthetic-other-namespace"
        XCTAssertEqual(
            harness.viewModel.pendingWriteActionTargetSummary,
            "Context: synthetic-reviewed\nNamespace: synthetic-namespace"
        )

        harness.viewModel.pendingWriteAction = .delete(kind: .node, name: "synthetic-node")
        XCTAssertEqual(
            harness.viewModel.pendingWriteActionTargetSummary,
            "Context: synthetic-other\nCluster-wide resource"
        )
        harness.viewModel.pendingWriteAction = .deleteMany([
            .init(kind: .pod, name: "synthetic-pod", namespace: "synthetic-b"),
            .init(kind: .configMap, name: "synthetic-config", namespace: "synthetic-a"),
            .init(kind: .node, name: "synthetic-node", namespace: "")
        ])
        XCTAssertEqual(
            harness.viewModel.pendingWriteActionTargetSummary,
            "Context: synthetic-other\nNamespaces: synthetic-a, synthetic-b\nIncludes cluster-wide resources"
        )
    }

    func testContextRemovalRejectsChangedSourceSet() throws {
        let remover = RecordingWriteLifecycleContextRemover()
        let harness = try makeHarness(contextRemover: remover)
        defer { harness.cleanup() }
        harness.viewModel.requestKubeConfigContextRemoval(KubeContext(name: "synthetic-reviewed"))
        let replacement = harness.directory.appendingPathComponent("replacement.yaml")
        try Data("apiVersion: v1\n".utf8).write(to: replacement)
        harness.state.setSources([KubeConfigSource(url: replacement)])

        harness.viewModel.confirmKubeConfigContextRemoval()

        XCTAssertTrue(remover.removalSources.isEmpty)
        XCTAssertNotNil(harness.viewModel.pendingKubeConfigContextRemoval)
        XCTAssertTrue(harness.viewModel.kubeConfigContextRemovalError?.contains("changed after this review") == true)
        XCTAssertFalse(harness.viewModel.isRemovingKubeConfigContext)
    }

    func testContextRemovalRejectsSameSizeContentRotationAfterConfirmation() async throws {
        let remover = RecordingWriteLifecycleContextRemover()
        let harness = try makeHarness(contextRemover: remover)
        defer { harness.cleanup() }
        harness.viewModel.requestKubeConfigContextRemoval(KubeContext(name: "synthetic-reviewed"))
        let attributes = try FileManager.default.attributesOfItem(atPath: harness.config.path)
        harness.viewModel.confirmKubeConfigContextRemoval()
        XCTAssertTrue(harness.viewModel.isRemovingKubeConfigContext)

        try Data("apiVersion: v2\n".utf8).write(to: harness.config)
        try FileManager.default.setAttributes(
            [.modificationDate: try XCTUnwrap(attributes[.modificationDate])],
            ofItemAtPath: harness.config.path
        )
        try await waitUntil { !harness.viewModel.isRemovingKubeConfigContext }

        XCTAssertTrue(remover.removalSources.isEmpty)
        XCTAssertTrue(harness.viewModel.kubeConfigContextRemovalError?.contains("changed after this review") == true)
    }

    func testContextRemovalFailureIsVisibleInSheetAndClearedOnCancel() async throws {
        let remover = RecordingWriteLifecycleContextRemover(failsRemoval: true)
        let harness = try makeHarness(contextRemover: remover)
        defer { harness.cleanup() }
        harness.viewModel.requestKubeConfigContextRemoval(KubeContext(name: "synthetic-reviewed"))
        harness.viewModel.confirmKubeConfigContextRemoval()
        // A competing request cannot replace the review while removal owns it.
        harness.viewModel.requestKubeConfigContextRemoval(KubeContext(name: "synthetic-competing"))
        try await waitUntil { !harness.viewModel.isRemovingKubeConfigContext }

        XCTAssertEqual(harness.viewModel.pendingKubeConfigContextRemoval?.contextName, "synthetic-reviewed")
        let message = try XCTUnwrap(harness.viewModel.kubeConfigContextRemovalError)
        XCTAssertTrue(message.contains("Check file access"))
        XCTAssertFalse(message.contains("synthetic-private-error-detail"))
        XCTAssertEqual(remover.removalSources, [[KubeConfigSource(url: harness.config)]])

        harness.viewModel.cancelKubeConfigContextRemoval()
        XCTAssertNil(harness.viewModel.pendingKubeConfigContextRemoval)
        XCTAssertNil(harness.viewModel.kubeConfigContextRemovalError)
        harness.viewModel.requestKubeConfigContextRemoval(KubeContext(name: "synthetic-reviewed"))
        XCTAssertNil(harness.viewModel.kubeConfigContextRemovalError)
    }

    private var rollback: PendingWriteAction {
        .helmRollback(
            releaseName: "synthetic-release",
            namespace: "synthetic-namespace",
            revision: 2,
            wait: true,
            timeout: "5m",
            cleanupOnFail: false
        )
    }

    private func makeHarness(
        contextRemover: any KubeConfigContextRemoving = KubeConfigContextRemover()
    ) throws -> WriteLifecycleHarness {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneWriteLifecycle.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let config = directory.appendingPathComponent("config.yaml")
        try Data("apiVersion: v1\n".utf8).write(to: config)
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: config)])
        state.selectedContext = KubeContext(name: "synthetic-reviewed")
        state.selectedNamespace = "synthetic-namespace"
        let runner = SuspendedWriteLifecycleHelmRunner()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigContextRemover: contextRemover,
            helmCommandRunner: runner,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        return WriteLifecycleHarness(
            directory: directory, config: config, state: state, viewModel: viewModel, runner: runner
        )
    }

    private func withHelmDryRun(
        _ enabled: Bool,
        operation: () async throws -> Void
    ) async rethrows {
        let key = RuneSettingsKeys.writeSafetyRequireHelmDryRun
        let previous = UserDefaults.standard.object(forKey: key)
        UserDefaults.standard.runeWriteSafetyRequireHelmDryRun = enabled
        defer {
            if let previous {
                UserDefaults.standard.set(previous, forKey: key)
            } else {
                UserDefaults.standard.removeObject(forKey: key)
            }
        }
        try await operation()
    }

    private func waitUntil(_ condition: () async -> Bool) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(3))
        while !(await condition()) {
            guard ContinuousClock.now < deadline else {
                throw RuneError.invalidInput(message: "Synthetic write operation timed out")
            }
            try await Task.sleep(for: .milliseconds(5))
        }
    }
}

private final class RecordingWriteLifecycleContextRemover: KubeConfigContextRemoving, @unchecked Sendable {
    private let lock = NSLock()
    private var capturedSources: [[KubeConfigSource]] = []
    private let failsRemoval: Bool

    init(failsRemoval: Bool = false) { self.failsRemoval = failsRemoval }

    var removalSources: [[KubeConfigSource]] { lock.withLock { capturedSources } }

    func previewRemoval(of contextName: String, from sources: [KubeConfigSource]) throws -> KubeConfigContextRemovalPreview {
        .init(
            contextName: contextName,
            affectedSourceDisplayNames: sources.map { $0.url.lastPathComponent },
            removedClusterCount: 1,
            removedUserCount: 1
        )
    }

    func removeContext(named contextName: String, from sources: [KubeConfigSource]) throws -> KubeConfigContextRemovalPreview {
        lock.withLock { capturedSources.append(sources) }
        if failsRemoval {
            throw RuneError.invalidInput(message: "synthetic-private-error-detail")
        }
        return try previewRemoval(of: contextName, from: sources)
    }
}

@MainActor
private struct WriteLifecycleHarness {
    let directory: URL
    let config: URL
    let state: RuneAppState
    let viewModel: RuneAppViewModel
    let runner: SuspendedWriteLifecycleHelmRunner

    func cleanup() { try? FileManager.default.removeItem(at: directory) }
}

private actor SuspendedWriteLifecycleHelmRunner: HelmCommandRunning {
    private(set) var requests: [HelmRollbackRequest] = []
    private var continuation: CheckedContinuation<HelmCommandResult, any Error>?

    var isSuspended: Bool { continuation != nil }

    func rollback(_ request: HelmRollbackRequest, timeout _: TimeInterval) async throws -> HelmCommandResult {
        requests.append(request)
        return try await withCheckedThrowingContinuation { continuation = $0 }
    }

    func complete(failing: Bool = false) {
        let pending = continuation
        continuation = nil
        if failing {
            pending?.resume(throwing: RuneError.invalidInput(message: "Synthetic earlier operation failed"))
        } else {
            pending?.resume(returning: HelmCommandResult(exitCode: 0, stdout: "", stderr: ""))
        }
    }
}
