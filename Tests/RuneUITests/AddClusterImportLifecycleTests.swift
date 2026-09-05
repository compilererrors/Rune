import Foundation
import XCTest
@testable import RuneCore
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

@MainActor
final class AddClusterImportLifecycleTests: XCTestCase {
    func testCancelledCLIImportDiscardsLateSuccessBeforeReview() async throws {
        let importer = SuspendedLifecycleImporter()
        let viewModel = makeViewModel(importer: importer)
        viewModel.runCloudKubeConfigImport(request)
        try await waitUntil { await importer.isSuspended }

        viewModel.cancelCloudKubeConfigImport()
        await importer.complete()
        try await waitUntil { !viewModel.isRunningCloudKubeConfigImport }

        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, "Import cancelled.")
        XCTAssertFalse(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertTrue(viewModel.kubeConfigImportReviews.isEmpty)
        XCTAssertTrue(viewModel.state.kubeConfigSources.isEmpty)
        XCTAssertNil(viewModel.cloudKubeConfigImportDiagnostic)
    }

    func testCancelledCLIImportTreatsLateProcessFailureAsCancellation() async throws {
        let importer = SuspendedLifecycleImporter()
        let viewModel = makeViewModel(importer: importer)
        viewModel.runCloudKubeConfigImport(request)
        try await waitUntil { await importer.isSuspended }

        viewModel.cancelCloudKubeConfigImport()
        await importer.complete(failing: true)
        try await waitUntil { !viewModel.isRunningCloudKubeConfigImport }

        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, "Import cancelled.")
        XCTAssertNil(viewModel.cloudKubeConfigImportDiagnostic)
        XCTAssertTrue(viewModel.state.authDoctorChecks.isEmpty)
    }

    func testLateOutputCannotRepopulateClearedSheetOrNewImport() async throws {
        let importer = SuspendedLifecycleImporter()
        let viewModel = makeViewModel(importer: importer)
        viewModel.runCloudKubeConfigImport(request)
        try await waitUntil { await importer.isSuspended }
        await importer.emit("first-attempt", attempt: 0)
        try await waitUntil { viewModel.cloudKubeConfigImportOutput.contains("first-attempt") }
        await importer.complete(failing: true)
        try await waitUntil { !viewModel.isRunningCloudKubeConfigImport }

        viewModel.clearCloudKubeConfigImportStatus()
        await importer.emit("late-after-close", attempt: 0)
        // A MainActor barrier lets the output callback's queued task run.
        await drainOutputCallbacks()
        XCTAssertEqual(viewModel.cloudKubeConfigImportOutput, "")

        viewModel.runCloudKubeConfigImport(request)
        try await waitUntil { await importer.isSuspended }
        await importer.emit("late-old-attempt", attempt: 0)
        await importer.emit("current-attempt", attempt: 1)
        try await waitUntil { viewModel.cloudKubeConfigImportOutput.contains("current-attempt") }
        XCTAssertFalse(viewModel.cloudKubeConfigImportOutput.contains("late-old-attempt"))
        viewModel.cancelCloudKubeConfigImport()
        await importer.complete(failing: true)
        try await waitUntil { !viewModel.isRunningCloudKubeConfigImport }
    }

    func testAzureSignInOwnsPresentationAndRejectsCompetingImports() async throws {
        let importer = SuspendedLifecycleImporter()
        let signIn = SuspendedLifecycleSignIn()
        let viewModel = makeViewModel(importer: importer, signIn: signIn)
        viewModel.runCloudKubeConfigImport(request)
        try await waitUntil { await importer.isSuspended }
        await importer.complete(failing: true)
        try await waitUntil { !viewModel.isRunningCloudKubeConfigImport }
        XCTAssertNotNil(viewModel.cloudKubeConfigImportDiagnostic)

        viewModel.signInToAzureAndRetry(request)
        try await waitUntil { await signIn.isSuspended }
        let status = viewModel.cloudKubeConfigImportStatus
        let output = viewModel.cloudKubeConfigImportOutput
        XCTAssertNil(viewModel.cloudKubeConfigImportDiagnostic, "Old errors must not hide sign-in progress.")

        viewModel.clearCloudKubeConfigImportStatus()
        viewModel.runCloudKubeConfigImport(request)
        viewModel.runNativeEKSClusterImport(
            clusterName: "", region: "", accessKeyID: "", secretAccessKey: ""
        )
        viewModel.chooseAndRunNativeGKEClusterImport(projectID: "", location: "", clusterName: "")
        viewModel.connectSelectedEKSNativeAuth(accessKeyID: "", secretAccessKey: "")
        await drainOutputCallbacks()

        XCTAssertTrue(viewModel.isSigningInToAzure)
        XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
        XCTAssertFalse(viewModel.isConnectingNativeKubernetesAuth)
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, status)
        XCTAssertEqual(viewModel.cloudKubeConfigImportOutput, output)
        XCTAssertNil(viewModel.cloudKubeConfigImportDiagnostic)
        XCTAssertNil(viewModel.nativeKubernetesAuthStatus)
        let importCount = await importer.attemptCount
        XCTAssertEqual(importCount, 1)

        viewModel.cancelAzureCLISignIn()
        await signIn.complete()
        await drainOutputCallbacks()
        XCTAssertFalse(viewModel.isSigningInToAzure)
        XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, "Azure sign-in cancelled.")
    }

    func testAzureSignInCannotReplacePendingImportReview() async throws {
        let importer = SuspendedLifecycleImporter()
        let signIn = SuspendedLifecycleSignIn()
        let viewModel = makeViewModel(importer: importer, signIn: signIn)
        viewModel.runCloudKubeConfigImport(request)
        try await waitUntil { await importer.isSuspended }
        await importer.complete()
        try await waitUntil { !viewModel.isRunningCloudKubeConfigImport }
        XCTAssertTrue(viewModel.isKubeConfigImportConfirmationPending)
        let reviews = viewModel.kubeConfigImportReviews
        let status = viewModel.cloudKubeConfigImportStatus

        viewModel.signInToAzureAndRetry(request)
        XCTAssertFalse(viewModel.isSigningInToAzure)
        XCTAssertEqual(viewModel.kubeConfigImportReviews, reviews)
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, status)
        viewModel.cancelKubeConfigImport()
    }

    private var request: CloudKubeConfigImportRequest {
        .init(provider: .aks, clusterName: "synthetic-cluster", resourceGroup: "synthetic-group")
    }

    private func makeViewModel(
        importer: SuspendedLifecycleImporter,
        signIn: SuspendedLifecycleSignIn = SuspendedLifecycleSignIn()
    ) -> RuneAppViewModel {
        RuneAppViewModel(
            state: RuneAppState(),
            kubeConfigDiscoverer: LifecycleEmptyDiscoverer(),
            cloudKubeConfigImporter: importer,
            azureCLISignInRunner: signIn,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
    }

    private func waitUntil(_ condition: () async -> Bool) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(3))
        while !(await condition()) {
            guard ContinuousClock.now < deadline else {
                throw RuneError.invalidInput(message: "Synthetic lifecycle operation timed out.")
            }
            try await Task.sleep(for: .milliseconds(5))
        }
    }

    private func drainOutputCallbacks() async {
        await withCheckedContinuation { continuation in
            DispatchQueue.main.async { continuation.resume() }
        }
    }
}

private actor SuspendedLifecycleImporter: CloudKubeConfigImporting {
    private var continuation: CheckedContinuation<CloudKubeConfigImportResult, Error>?
    private var result: CloudKubeConfigImportResult?
    private var outputs: [@Sendable (CloudKubeConfigCommandOutput) -> Void] = []
    var isSuspended: Bool { continuation != nil }
    var attemptCount: Int { outputs.count }

    nonisolated func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        .init(executable: "az", arguments: [], displayCommand: "az aks get-credentials")
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        try await importCluster(request, onOutput: { _ in })
    }

    func importCluster(
        _ request: CloudKubeConfigImportRequest,
        onOutput: @escaping @Sendable (CloudKubeConfigCommandOutput) -> Void
    ) async throws -> CloudKubeConfigImportResult {
        let url = URL(fileURLWithPath: request.targetKubeconfigPath)
        let raw = """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://cluster.example.invalid
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
            token: synthetic-token
        """
        try raw.write(to: url, atomically: true, encoding: .utf8)
        result = CloudKubeConfigImportResult(
            command: try commandPreview(for: request),
            commandResult: .init(exitCode: 0, stdout: "", stderr: ""),
            discoveredURLs: [url],
            reviews: [KubeConfigImportValidator().validate(raw: raw, sourceName: "synthetic.yaml")]
        )
        outputs.append(onOutput)
        // Deliberately ignores cancellation to exercise delayed process callbacks.
        return try await withCheckedThrowingContinuation { continuation = $0 }
    }

    func complete(failing: Bool = false) {
        let pending = continuation
        continuation = nil
        if failing {
            pending?.resume(throwing: CloudKubeConfigImportError.commandFailed(
                command: "az", exitCode: 1, message: "AADSTS50173"
            ))
        } else if let result {
            pending?.resume(returning: result)
        }
    }

    func emit(_ text: String, attempt: Int) {
        outputs[attempt](.init(stream: .stdout, text: text))
    }
}

private actor SuspendedLifecycleSignIn: AzureCLISignInRunning {
    private var continuation: CheckedContinuation<Void, Never>?
    var isSuspended: Bool { continuation != nil }

    func signIn() async throws {
        await withCheckedContinuation { continuation = $0 }
    }

    func complete() {
        continuation?.resume()
        continuation = nil
    }
}

private struct LifecycleEmptyDiscoverer: KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL] { [] }
}
