import Foundation
import XCTest
@testable import RuneCore
@testable import RuneDiagnostics
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

@MainActor
final class NativeCloudImportParityTests: XCTestCase {
    func testHeadlessAKSImportReviewsBeforeBindingThenPublishesAndBinds() async throws {
        _ = try await exerciseImport(provider: .aks)
    }

    func testInjectedGKECredentialPickerReviewsBeforeBindingThenPublishesAndBinds() async throws {
        _ = try await exerciseImport(provider: .gke)
    }

    func testCancellingInjectedGKECredentialSelectionRejectsStaleCompletion() async throws {
        let picker = DeferredGKECredentialFilePicker()
        let importer = ParityNativeCloudClusterImporter(
            aksResult: NativeCloudClusterImportResult(
                provider: .aks,
                rawKubeConfig: "",
                sourceName: "unused-aks.yaml"
            ),
            gkeResult: NativeCloudClusterImportResult(
                provider: .gke,
                rawKubeConfig: "",
                sourceName: "unused-gke.yaml"
            )
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            gkeCredentialFilePicker: picker,
            nativeCloudClusterImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.chooseAndRunNativeGKEClusterImport(
            projectID: "synthetic-project",
            location: "europe-west1",
            clusterName: "synthetic-cluster"
        )
        XCTAssertTrue(viewModel.isConnectingNativeKubernetesAuth)
        XCTAssertTrue(viewModel.isRunningNativeCloudClusterImport)
        XCTAssertEqual(picker.selectionCount, 1)

        viewModel.cancelNativeCloudClusterImport()
        picker.complete(.selected(Data(#"{"type":"service_account"}"#.utf8)))
        await Task.yield()

        XCTAssertEqual(picker.cancellationCount, 1)
        XCTAssertFalse(viewModel.isConnectingNativeKubernetesAuth)
        XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
        XCTAssertFalse(viewModel.isRunningNativeCloudClusterImport)
        XCTAssertEqual(
            viewModel.cloudKubeConfigImportStatus,
            AddClusterCloudImportWorkflow.nativeCancelledStatus(for: .gke)
        )
        XCTAssertFalse(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertTrue(viewModel.kubeConfigImportReviews.isEmpty)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        let importCall = await importer.gkeCall()
        XCTAssertNil(importCall)
    }

    func testHeadlessAKSAndGKEImportReviewBindingReleaseKPI() async throws {
        let aksElapsed = try await exerciseImport(provider: .aks)
        let gkeElapsed = try await exerciseImport(provider: .gke)
        let elapsed = aksElapsed + gkeElapsed

        #if DEBUG
        let maximumSeconds = 1.2
        #else
        let maximumSeconds = 0.6
        #endif
        XCTAssertLessThan(
            elapsed,
            maximumSeconds,
            "KPI: headless AKS and injected-picker GKE import, review, publish, and credential binding should stay below 1.2s in debug and 600ms in release."
        )
    }

    private func exerciseImport(provider: CloudKubeConfigProvider) async throws -> TimeInterval {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneNativeCloudParity.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let credentials = ParityNativeCredentialConfigurator()
        let serviceAccountJSON = Data(#"{"type":"service_account","project_id":"synthetic-project"}"#.utf8)
        let clientSecret = "synthetic-client-secret"
        let importer = ParityNativeCloudClusterImporter(
            aksResult: NativeCloudClusterImportResult(
                provider: .aks,
                rawKubeConfig: providerKubeconfig(from: server.kubeconfigYAML(), provider: .aks),
                sourceName: "synthetic-aks.yaml"
            ),
            gkeResult: NativeCloudClusterImportResult(
                provider: .gke,
                rawKubeConfig: providerKubeconfig(from: server.kubeconfigYAML(), provider: .gke),
                sourceName: "synthetic-gke.yaml"
            )
        )
        let picker = ImmediateGKECredentialFilePicker(selection: .selected(serviceAccountJSON))
        let metrics = KubernetesRESTRequestMetricsRecorder()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: KubernetesClient(
                commandTimeout: 2,
                restClient: KubernetesRESTClient(
                    requestMetricsRecorder: metrics,
                    nativeCredentialProvider: credentials
                ),
                requestMetricsRecorder: metrics
            ),
            bookmarkManager: BookmarkManager(store: ParityBookmarkStore()),
            gkeCredentialFilePicker: picker,
            kubeConfigDiscoverer: ParityEmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                rootDirectory: directory.appendingPathComponent("imports", isDirectory: true)
            ),
            nativeCloudClusterImporter: importer,
            nativeAuthConfigurator: credentials,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        let clock = ContinuousClock()
        let started = clock.now
        switch provider {
        case .aks:
            viewModel.runNativeAKSClusterImport(
                subscriptionID: "11111111-1111-4111-8111-111111111111",
                resourceGroup: "synthetic-group",
                clusterName: "synthetic-cluster",
                tenantID: "22222222-2222-4222-8222-222222222222",
                clientID: "33333333-3333-4333-8333-333333333333",
                clientSecret: clientSecret
            )
        case .gke:
            viewModel.chooseAndRunNativeGKEClusterImport(
                projectID: "synthetic-project",
                location: "europe-west1",
                clusterName: "synthetic-cluster"
            )
        case .eks:
            XCTFail("EKS has its own established parity coverage.")
            return 0
        }

        try await waitUntil {
            viewModel.isKubeConfigImportConfirmationPending
                && !viewModel.isRunningNativeCloudClusterImport
        }
        XCTAssertTrue(viewModel.canConfirmKubeConfigImport)
        XCTAssertEqual(viewModel.kubeConfigImportReviews.count, 1)
        XCTAssertEqual(
            viewModel.cloudKubeConfigImportStatus,
            AddClusterCloudImportWorkflow.nativeReadyForReviewStatus(for: provider)
        )
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        let bindingCountBeforeConfirmation = await credentials.bindingCount()
        XCTAssertEqual(bindingCountBeforeConfirmation, 0)
        XCTAssertEqual(picker.selectionCount, provider == .gke ? 1 : 0)

        viewModel.confirmKubeConfigImport()
        try await waitUntil {
            await credentials.bindingCount() == 1
                && !viewModel.isCommittingKubeConfigImport
                && viewModel.nativeKubernetesAuthStatus == "Cluster imported and native credentials connected."
        }

        XCTAssertEqual(
            viewModel.cloudKubeConfigImportStatus,
            AddClusterCloudImportWorkflow.importedStatus(for: provider)
        )
        XCTAssertEqual(state.selectedContext?.name, RuneFakeK8sFixture.defaultContextName)
        XCTAssertFalse(state.kubeConfigSources.isEmpty)
        XCTAssertNil(state.lastError)

        switch provider {
        case .aks:
            let recordedImportCall = await importer.aksCall()
            let importCall = try XCTUnwrap(recordedImportCall)
            XCTAssertEqual(importCall.clientSecret, clientSecret)
            let recordedBinding = await credentials.aksBinding()
            let binding = try XCTUnwrap(recordedBinding)
            XCTAssertEqual(binding.request.provider, .azureKubelogin)
            XCTAssertEqual(binding.clientSecret, clientSecret)
        case .gke:
            let recordedImportCall = await importer.gkeCall()
            let importCall = try XCTUnwrap(recordedImportCall)
            XCTAssertEqual(importCall.serviceAccountJSON, serviceAccountJSON)
            let recordedBinding = await credentials.gkeBinding()
            let binding = try XCTUnwrap(recordedBinding)
            XCTAssertEqual(binding.request.provider, .googleGKE)
            XCTAssertEqual(binding.serviceAccountJSON, serviceAccountJSON)
        case .eks:
            break
        }

        for source in state.kubeConfigSources {
            let stored = try String(contentsOf: source.url, encoding: .utf8)
            XCTAssertFalse(stored.contains(clientSecret))
            XCTAssertFalse(stored.contains(String(decoding: serviceAccountJSON, as: UTF8.self)))
        }
        return durationSeconds(started.duration(to: clock.now))
    }

    private func providerKubeconfig(
        from base: String,
        provider: CloudKubeConfigProvider
    ) -> String {
        let execConfig: String
        switch provider {
        case .aks:
            execConfig = """
                exec:
                  apiVersion: client.authentication.k8s.io/v1beta1
                  command: kubelogin
                  args:
                  - get-token
                  - --environment
                  - AzurePublicCloud
                  - --server-id
                  - 44444444-4444-4444-8444-444444444444
                  - --client-id
                  - 33333333-3333-4333-8333-333333333333
                  - --tenant-id
                  - 22222222-2222-4222-8222-222222222222
                  - --login
                  - spn
                  interactiveMode: Never
            """
        case .gke:
            execConfig = """
                exec:
                  apiVersion: client.authentication.k8s.io/v1beta1
                  command: gke-gcloud-auth-plugin
                  args:
                  - --use_application_default_credentials
                  provideClusterInfo: true
                  interactiveMode: Never
            """
        case .eks:
            execConfig = ""
        }
        return base.replacingOccurrences(of: "    token: fake-token", with: execConfig)
    }

    private func waitUntil(
        timeout: TimeInterval = 5,
        predicate: @escaping @MainActor () async -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !(await predicate()) {
            if Date() >= deadline {
                XCTFail("Timed out waiting for native cloud import parity workflow.")
                return
            }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
    }

    private func durationSeconds(_ duration: Duration) -> TimeInterval {
        let components = duration.components
        return TimeInterval(components.seconds)
            + TimeInterval(components.attoseconds) / 1_000_000_000_000_000_000
    }
}

@MainActor
private final class ImmediateGKECredentialFilePicker: GKECredentialFilePicking {
    let selection: GKECredentialFileSelection
    private(set) var selectionCount = 0

    init(selection: GKECredentialFileSelection) {
        self.selection = selection
    }

    func beginSelection(
        completion: @escaping @MainActor (GKECredentialFileSelection) -> Void
    ) {
        selectionCount += 1
        completion(selection)
    }

    func cancelSelection() {}
}

@MainActor
private final class DeferredGKECredentialFilePicker: GKECredentialFilePicking {
    private var completion: (@MainActor (GKECredentialFileSelection) -> Void)?
    private(set) var selectionCount = 0
    private(set) var cancellationCount = 0

    func beginSelection(
        completion: @escaping @MainActor (GKECredentialFileSelection) -> Void
    ) {
        selectionCount += 1
        self.completion = completion
    }

    func cancelSelection() {
        cancellationCount += 1
    }

    func complete(_ selection: GKECredentialFileSelection) {
        completion?(selection)
        completion = nil
    }
}

private actor ParityNativeCloudClusterImporter: NativeCloudClusterImporting {
    struct AKSCall: Sendable {
        let request: AKSNativeClusterImportRequest
        let clientSecret: String
    }

    struct GKECall: Sendable {
        let request: GKENativeClusterImportRequest
        let serviceAccountJSON: Data
    }

    let aksResult: NativeCloudClusterImportResult
    let gkeResult: NativeCloudClusterImportResult
    private var recordedAKSCall: AKSCall?
    private var recordedGKECall: GKECall?

    init(
        aksResult: NativeCloudClusterImportResult,
        gkeResult: NativeCloudClusterImportResult
    ) {
        self.aksResult = aksResult
        self.gkeResult = gkeResult
    }

    func importAKS(
        _ request: AKSNativeClusterImportRequest,
        clientSecret: String
    ) async throws -> NativeCloudClusterImportResult {
        recordedAKSCall = AKSCall(request: request, clientSecret: clientSecret)
        return aksResult
    }

    func importEKS(
        _: AWSEKSClusterImportRequest,
        credentials _: AWSEKSCredentials
    ) async throws -> NativeCloudClusterImportResult {
        throw ParityUnexpectedProviderError()
    }

    func importGKE(
        _ request: GKENativeClusterImportRequest,
        serviceAccountJSON: Data
    ) async throws -> NativeCloudClusterImportResult {
        recordedGKECall = GKECall(
            request: request,
            serviceAccountJSON: serviceAccountJSON
        )
        return gkeResult
    }

    func aksCall() -> AKSCall? {
        recordedAKSCall
    }

    func gkeCall() -> GKECall? {
        recordedGKECall
    }
}

private actor ParityNativeCredentialConfigurator:
    KubernetesNativeAuthConfiguring,
    KubernetesNativeCredentialProviding {
    struct AKSBinding: Sendable {
        let request: KubernetesNativeCredentialRequest
        let clientSecret: String
    }

    struct GKEBinding: Sendable {
        let request: KubernetesNativeCredentialRequest
        let serviceAccountJSON: Data
    }

    private var recordedAKSBinding: AKSBinding?
    private var recordedGKEBinding: GKEBinding?

    func status(
        for request: KubernetesNativeCredentialRequest
    ) async throws -> KubernetesNativeAuthProfileStatus {
        KubernetesNativeAuthProfileStatus(
            bindingID: request.bindingID,
            provider: request.provider,
            isConnected: recordedAKSBinding?.request.bindingID == request.bindingID
                || recordedGKEBinding?.request.bindingID == request.bindingID,
            expiresAt: nil
        )
    }

    func bindAWSCredentials(
        to _: KubernetesNativeCredentialRequest,
        credentials _: AWSEKSCredentials,
        displayName _: String
    ) async throws {
        throw ParityUnexpectedProviderError()
    }

    func bindAKSServicePrincipal(
        to request: KubernetesNativeCredentialRequest,
        clientSecret: String,
        displayName _: String
    ) async throws {
        recordedAKSBinding = AKSBinding(request: request, clientSecret: clientSecret)
    }

    func bindGCPServiceAccount(
        to request: KubernetesNativeCredentialRequest,
        serviceAccountJSON: Data,
        displayName _: String
    ) async throws {
        recordedGKEBinding = GKEBinding(
            request: request,
            serviceAccountJSON: serviceAccountJSON
        )
    }

    func removeProfile(for bindingID: String) async throws {
        if recordedAKSBinding?.request.bindingID == bindingID {
            recordedAKSBinding = nil
        }
        if recordedGKEBinding?.request.bindingID == bindingID {
            recordedGKEBinding = nil
        }
    }

    func credential(
        for request: KubernetesNativeCredentialRequest
    ) async throws -> KubernetesNativeCredential? {
        guard recordedAKSBinding?.request.bindingID == request.bindingID
            || recordedGKEBinding?.request.bindingID == request.bindingID else {
            return nil
        }
        return KubernetesNativeCredential(
            bearerToken: "fake-token",
            expiresAt: Date().addingTimeInterval(300)
        )
    }

    func bindingCount() -> Int {
        (recordedAKSBinding == nil ? 0 : 1)
            + (recordedGKEBinding == nil ? 0 : 1)
    }

    func aksBinding() -> AKSBinding? {
        recordedAKSBinding
    }

    func gkeBinding() -> GKEBinding? {
        recordedGKEBinding
    }
}

private final class ParityBookmarkStore: BookmarkStore, @unchecked Sendable {
    private var records: [BookmarkRecord] = []

    func loadRecords() throws -> [BookmarkRecord] {
        records
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        self.records = records
    }
}

private struct ParityEmptyKubeConfigDiscoverer: KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL] {
        []
    }
}

private struct ParityUnexpectedProviderError: Error {}
