import Foundation
import XCTest
@testable import RuneCore
@testable import RuneDiagnostics
@testable import RuneExport
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

@MainActor
final class NativeCloudClusterImportViewModelTests: XCTestCase {
    func testNativeEKSImportReviewsBeforeBindingThenPublishesAndConnects() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneNativeImportTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let raw = server.kubeconfigYAML().replacingOccurrences(
            of: "    token: fake-token",
            with: "    exec:\n"
                + "      apiVersion: client.authentication.k8s.io/v1beta1\n"
                + "      command: aws\n"
                + "      args: [eks, get-token, --cluster-name, synthetic-cluster, --region, eu-north-1]\n"
                + "      interactiveMode: Never"
        )
        let nativeImporter = FixedNativeCloudClusterImporter(result: NativeCloudClusterImportResult(
            provider: .eks,
            rawKubeConfig: raw,
            sourceName: "synthetic-eks.yaml"
        ))
        let credentials = RecordingNativeCloudCredentialConfigurator(
            bindingDelayNanoseconds: 200_000_000
        )
        let metrics = KubernetesRESTRequestMetricsRecorder()
        let kubeClient = KubernetesClient(
            commandTimeout: 2,
            restClient: KubernetesRESTClient(
                requestMetricsRecorder: metrics,
                nativeCredentialProvider: credentials
            ),
            requestMetricsRecorder: metrics
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: kubeClient,
            bookmarkManager: BookmarkManager(store: NativeImportBookmarkStore()),
            kubeConfigDiscoverer: NativeImportEmptyDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                rootDirectory: directory.appendingPathComponent("imports", isDirectory: true)
            ),
            nativeCloudClusterImporter: nativeImporter,
            nativeAuthConfigurator: credentials,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        let secret = "synthetic-secret-material"

        viewModel.runNativeEKSClusterImport(
            clusterName: "synthetic-cluster",
            region: "eu-north-1",
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: secret
        )

        try await waitUntil {
            viewModel.isKubeConfigImportConfirmationPending
                && !viewModel.isRunningCloudKubeConfigImport
        }
        let readyStatus = viewModel.cloudKubeConfigImportStatus
        let readyNativeStatus = viewModel.nativeKubernetesAuthStatus
        let reviewCount = viewModel.kubeConfigImportReviews.count
        viewModel.runNativeEKSClusterImport(
            clusterName: "",
            region: "",
            accessKeyID: "",
            secretAccessKey: ""
        )
        viewModel.runNativeAKSClusterImport(
            subscriptionID: "",
            resourceGroup: "",
            clusterName: "",
            tenantID: "",
            clientID: "",
            clientSecret: ""
        )
        viewModel.chooseAndRunNativeGKEClusterImport(
            projectID: "",
            location: "",
            clusterName: ""
        )

        XCTAssertTrue(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertFalse(viewModel.isConnectingNativeKubernetesAuth)
        XCTAssertEqual(viewModel.kubeConfigImportReviews.count, reviewCount)
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, readyStatus)
        XCTAssertEqual(viewModel.nativeKubernetesAuthStatus, readyNativeStatus)
        XCTAssertNil(viewModel.cloudKubeConfigImportDiagnostic)
        let callBeforeConfirmation = await credentials.awsCall()
        XCTAssertNil(callBeforeConfirmation)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)

        viewModel.confirmKubeConfigImport()
        try await waitUntil { await credentials.hasStartedBinding() }
        XCTAssertTrue(viewModel.isCommittingKubeConfigImport)
        try await waitUntil {
            let call = await credentials.awsCall()
            return call != nil && !viewModel.isCommittingKubeConfigImport
        }

        let recordedCall = await credentials.awsCall()
        let call = try XCTUnwrap(recordedCall)
        XCTAssertEqual(call.request.provider, .awsEKS)
        XCTAssertEqual(call.request.contextName, RuneFakeK8sFixture.defaultContextName)
        XCTAssertEqual(call.credentials.secretAccessKey, secret)
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, "Imported EKS kubeconfig context.")
        XCTAssertTrue(state.kubeConfigSources.allSatisfy { $0.url.path.contains("/imports/") })
        XCTAssertFalse(state.kubeConfigSources.isEmpty)
        XCTAssertFalse(viewModel.nativeKubernetesAuthStatus?.contains(secret) == true)
        XCTAssertFalse(state.lastError?.contains(secret) == true)
        for source in state.kubeConfigSources {
            let stored = try String(contentsOf: source.url, encoding: .utf8)
            XCTAssertFalse(stored.contains(secret))
        }
    }

    func testCancellingNativeImportReviewDoesNotBindCredentials() async throws {
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
            exec:
              apiVersion: client.authentication.k8s.io/v1beta1
              command: aws
              args: [eks, get-token, --cluster-name, synthetic-cluster, --region, eu-north-1]
        """
        let credentials = RecordingNativeCloudCredentialConfigurator()
        let viewModel = RuneAppViewModel(
            state: RuneAppState(),
            nativeCloudClusterImporter: FixedNativeCloudClusterImporter(result: NativeCloudClusterImportResult(
                provider: .eks,
                rawKubeConfig: raw,
                sourceName: "synthetic-eks.yaml"
            )),
            nativeAuthConfigurator: credentials,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runNativeEKSClusterImport(
            clusterName: "synthetic-cluster",
            region: "eu-north-1",
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: "synthetic-secret-material"
        )
        try await waitUntil {
            viewModel.isKubeConfigImportConfirmationPending
        }
        viewModel.cancelKubeConfigImport()

        XCTAssertFalse(viewModel.isKubeConfigImportConfirmationPending)
        let recordedCall = await credentials.awsCall()
        XCTAssertNil(recordedCall)
        XCTAssertNil(viewModel.cloudKubeConfigImportStatus)
        XCTAssertEqual(
            viewModel.nativeKubernetesAuthStatus,
            "Import cancelled. Credentials were not stored."
        )
    }

    func testCancellingInFlightNativeImportNeverCreatesReviewOrFailure() async throws {
        let importer = HangingNativeCloudClusterImporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            nativeCloudClusterImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runNativeEKSClusterImport(
            clusterName: "synthetic-cluster",
            region: "eu-north-1",
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: "synthetic-secret-material"
        )
        try await waitUntil { await importer.hasStarted() }
        XCTAssertTrue(viewModel.isRunningNativeCloudClusterImport)

        viewModel.cancelNativeCloudClusterImport()
        try await waitUntil { !viewModel.isRunningNativeCloudClusterImport }

        XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
        XCTAssertFalse(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertNil(state.lastError)
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, "Amazon EKS import cancelled.")
        XCTAssertNil(viewModel.nativeKubernetesAuthStatus)
    }

    func testInvalidNativeDuplicateEntriesCannotOverwriteInFlightImportPresentation() async throws {
        let importer = HangingNativeCloudClusterImporter()
        let state = RuneAppState()
        state.setAuthDoctorChecks([
            RuneHealthCheck(
                id: "synthetic-baseline",
                title: "Synthetic baseline",
                status: .passed,
                message: "Synthetic baseline remains unchanged."
            )
        ])
        let viewModel = RuneAppViewModel(
            state: state,
            nativeCloudClusterImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runNativeEKSClusterImport(
            clusterName: "synthetic-cluster",
            region: "eu-north-1",
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: "synthetic-secret-material"
        )
        try await waitUntil { await importer.hasStarted() }
        let statusBeforeDuplicates = viewModel.cloudKubeConfigImportStatus
        let nativeStatusBeforeDuplicates = viewModel.nativeKubernetesAuthStatus
        let checksBeforeDuplicates = state.authDoctorChecks.map { "\($0.id)|\($0.status)|\($0.message)" }

        viewModel.runNativeEKSClusterImport(
            clusterName: "",
            region: "",
            accessKeyID: "",
            secretAccessKey: ""
        )
        viewModel.runNativeAKSClusterImport(
            subscriptionID: "",
            resourceGroup: "",
            clusterName: "",
            tenantID: "",
            clientID: "",
            clientSecret: ""
        )
        viewModel.chooseAndRunNativeGKEClusterImport(
            projectID: "",
            location: "",
            clusterName: ""
        )

        let importerCallCount = await importer.eksCallCount()
        XCTAssertEqual(importerCallCount, 1)
        XCTAssertTrue(viewModel.isRunningNativeCloudClusterImport)
        XCTAssertFalse(viewModel.isConnectingNativeKubernetesAuth)
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, statusBeforeDuplicates)
        XCTAssertEqual(viewModel.nativeKubernetesAuthStatus, nativeStatusBeforeDuplicates)
        XCTAssertNil(viewModel.cloudKubeConfigImportDiagnostic)
        XCTAssertNil(state.lastError)
        XCTAssertEqual(
            state.authDoctorChecks.map { "\($0.id)|\($0.status)|\($0.message)" },
            checksBeforeDuplicates
        )

        viewModel.cancelNativeCloudClusterImport()
        try await waitUntil { !viewModel.isRunningNativeCloudClusterImport }
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, "Amazon EKS import cancelled.")
        XCTAssertNil(viewModel.cloudKubeConfigImportDiagnostic)
        XCTAssertNil(state.lastError)
    }

    func testNativeImportRejectsCredentialConnectAndDisconnectWithoutPresentationMutation() async throws {
        let importer = HangingNativeCloudClusterImporter()
        let configurator = RecordingNativeCloudCredentialConfigurator()
        let state = RuneAppState()
        state.setAuthDoctorChecks([
            RuneHealthCheck(
                id: "synthetic-baseline",
                title: "Synthetic baseline",
                status: .passed,
                message: "Synthetic diagnostic remains unchanged."
            )
        ])
        let viewModel = RuneAppViewModel(
            state: state,
            nativeCloudClusterImporter: importer,
            nativeAuthConfigurator: configurator,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        let request = KubernetesNativeCredentialRequest(
            bindingID: "synthetic-binding",
            provider: .awsEKS,
            contextName: "synthetic-context",
            clusterName: "synthetic-cluster",
            userName: "synthetic-user",
            server: "https://cluster.example.invalid",
            exec: KubernetesNativeAuthExecDescriptor(command: "synthetic-auth-plugin"),
            authProvider: nil
        )

        viewModel.runNativeEKSClusterImport(
            clusterName: "synthetic-cluster",
            region: "eu-north-1",
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: "synthetic-secret-material"
        )
        try await waitUntil { await importer.hasStarted() }
        let cloudStatus = viewModel.cloudKubeConfigImportStatus
        let nativeStatus = viewModel.nativeKubernetesAuthStatus
        let checks = state.authDoctorChecks
        let error = state.lastError

        viewModel.connectEKSNativeAuth(
            request: request,
            accessKeyID: "",
            secretAccessKey: ""
        )
        viewModel.disconnectNativeAuth(request: request, expectedProvider: .awsEKS)

        XCTAssertTrue(viewModel.isRunningNativeCloudClusterImport)
        XCTAssertFalse(viewModel.isConnectingNativeKubernetesAuth)
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, cloudStatus)
        XCTAssertEqual(viewModel.nativeKubernetesAuthStatus, nativeStatus)
        XCTAssertEqual(state.authDoctorChecks, checks)
        XCTAssertEqual(state.lastError, error)
        let awsCall = await configurator.awsCall()
        let removedBindingIDs = await configurator.removedBindingIDs()
        XCTAssertNil(awsCall)
        XCTAssertEqual(removedBindingIDs, [])

        viewModel.cancelNativeCloudClusterImport()
        try await waitUntil { !viewModel.isRunningNativeCloudClusterImport }
    }

    func testGKEFileSelectionRejectsCredentialConnectAndDisconnectWithoutPresentationMutation() async {
        let picker = HoldingGKECredentialFilePicker()
        let configurator = RecordingNativeCloudCredentialConfigurator()
        let state = RuneAppState()
        state.setAuthDoctorChecks([
            RuneHealthCheck(
                id: "synthetic-baseline",
                title: "Synthetic baseline",
                status: .passed,
                message: "Synthetic diagnostic remains unchanged."
            )
        ])
        let viewModel = RuneAppViewModel(
            state: state,
            gkeCredentialFilePicker: picker,
            nativeAuthConfigurator: configurator,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        let request = KubernetesNativeCredentialRequest(
            bindingID: "synthetic-binding",
            provider: .awsEKS,
            contextName: "synthetic-context",
            clusterName: "synthetic-cluster",
            userName: "synthetic-user",
            server: "https://cluster.example.invalid",
            exec: KubernetesNativeAuthExecDescriptor(command: "synthetic-auth-plugin"),
            authProvider: nil
        )

        viewModel.chooseAndRunNativeGKEClusterImport(
            projectID: "synthetic-project",
            location: "synthetic-location",
            clusterName: "synthetic-cluster"
        )
        XCTAssertEqual(picker.beginCount, 1)
        XCTAssertTrue(viewModel.isRunningNativeCloudClusterImport)
        let cloudStatus = viewModel.cloudKubeConfigImportStatus
        let nativeStatus = viewModel.nativeKubernetesAuthStatus
        let checks = state.authDoctorChecks
        let error = state.lastError

        viewModel.connectEKSNativeAuth(
            request: request,
            accessKeyID: "",
            secretAccessKey: ""
        )
        viewModel.disconnectNativeAuth(request: request, expectedProvider: .awsEKS)
        await Task.yield()
        let awsCall = await configurator.awsCall()
        let removedBindingIDs = await configurator.removedBindingIDs()

        XCTAssertTrue(viewModel.isRunningNativeCloudClusterImport)
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, cloudStatus)
        XCTAssertEqual(viewModel.nativeKubernetesAuthStatus, nativeStatus)
        XCTAssertEqual(state.authDoctorChecks, checks)
        XCTAssertEqual(state.lastError, error)
        XCTAssertNil(awsCall)
        XCTAssertEqual(removedBindingIDs, [])

        viewModel.cancelNativeCloudClusterImport()
        XCTAssertEqual(picker.cancelCount, 1)
        XCTAssertFalse(viewModel.isRunningNativeCloudClusterImport)
    }

    func testNativeImportFailurePublishesActionablePrivacySafeDiagnostic() async throws {
        let sensitiveValue = "synthetic-sensitive-native-import-payload"
        let state = RuneAppState()
        let exporter = NativeImportRecordingExporter()
        let viewModel = RuneAppViewModel(
            state: state,
            exporter: exporter,
            nativeCloudClusterImporter: SensitiveFailingNativeCloudClusterImporter(
                message: sensitiveValue
            ),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runNativeEKSClusterImport(
            clusterName: "synthetic-cluster",
            region: "eu-north-1",
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: "synthetic-secret-material"
        )
        try await waitUntil {
            viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                && !viewModel.isRunningNativeCloudClusterImport
        }

        let diagnostic = try XCTUnwrap(viewModel.cloudKubeConfigImportDiagnostic)
        let surfaced = [
            diagnostic.title,
            diagnostic.classification,
            diagnostic.message,
            diagnostic.operationShape,
            diagnostic.nextAction,
            viewModel.cloudKubeConfigImportStatus,
            viewModel.nativeKubernetesAuthStatus,
            state.lastError,
            state.authDoctorChecks.map(\.message).joined(separator: "\n")
        ].compactMap { $0 }.joined(separator: "\n")

        XCTAssertEqual(diagnostic.title, "Native import failed")
        XCTAssertTrue(diagnostic.operationShape.contains("Amazon EKS HTTPS API"))
        XCTAssertTrue(diagnostic.nextAction.contains("EKS access"))
        XCTAssertTrue(state.authDoctorChecks.contains { $0.id == "cloud-login-eks" })
        XCTAssertEqual(state.activeNotice?.severity, .error)
        XCTAssertEqual(state.activeNotice?.title, "Action failed")
        XCTAssertFalse(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertFalse(surfaced.contains(sensitiveValue))
        XCTAssertFalse(surfaced.contains("synthetic-secret-material"))

        let authDoctorMessage = try XCTUnwrap(
            state.authDoctorChecks.first { $0.id == "cloud-login-eks" }?.message
        )
        XCTAssertTrue(authDoctorMessage.contains(diagnostic.classification))
        XCTAssertTrue(authDoctorMessage.contains(diagnostic.nextAction))

        viewModel.saveSupportBundle()
        try await waitUntil { exporter.saves.count == 1 }
        let bundle = try JSONDecoder().decode(
            SupportBundleRequest.self,
            from: try XCTUnwrap(exporter.saves.first)
        )
        XCTAssertEqual(bundle.cloudImportDiagnostic?.classification, diagnostic.classification)
        XCTAssertEqual(bundle.cloudImportDiagnostic?.nextAction, diagnostic.nextAction)
        XCTAssertFalse(String(decoding: exporter.saves[0], as: UTF8.self).contains(sensitiveValue))
        XCTAssertFalse(String(decoding: exporter.saves[0], as: UTF8.self).contains("synthetic-secret-material"))
    }

    func testClassifiedNativeFailureSurvivesAuthDoctorAndSupportBundleProjection() async throws {
        let state = RuneAppState()
        let exporter = NativeImportRecordingExporter()
        let viewModel = RuneAppViewModel(
            state: state,
            exporter: exporter,
            nativeCloudClusterImporter: DeniedEKSNativeCloudClusterImporter(),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runNativeEKSClusterImport(
            clusterName: "synthetic-cluster",
            region: "eu-north-1",
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: "synthetic-secret-material"
        )
        try await waitUntil {
            viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                && !viewModel.isRunningNativeCloudClusterImport
        }

        let diagnostic = try XCTUnwrap(viewModel.cloudKubeConfigImportDiagnostic)
        XCTAssertEqual(diagnostic.classification, "Authorization failed")
        XCTAssertTrue(diagnostic.nextAction.contains("eks:DescribeCluster"))
        let authDoctorMessage = try XCTUnwrap(
            state.authDoctorChecks.first { $0.id == "cloud-login-eks" }?.message
        )
        XCTAssertTrue(authDoctorMessage.contains("Classification: Authorization failed"))
        XCTAssertTrue(authDoctorMessage.contains(diagnostic.nextAction))

        viewModel.runAuthDoctor()
        try await waitUntil {
            !state.isRunningAuthDoctor
                && state.authDoctorChecks.contains { $0.id == "kubeconfig" }
        }
        let diagnosticAfterAuthDoctor = try XCTUnwrap(viewModel.cloudKubeConfigImportDiagnostic)
        XCTAssertEqual(diagnosticAfterAuthDoctor, diagnostic)

        viewModel.saveSupportBundle()
        try await waitUntil { exporter.saves.count == 1 }
        let bundle = try JSONDecoder().decode(
            SupportBundleRequest.self,
            from: try XCTUnwrap(exporter.saves.first)
        )
        XCTAssertEqual(bundle.cloudImportDiagnostic?.classification, "Authorization failed")
        XCTAssertEqual(bundle.cloudImportDiagnostic?.nextAction, diagnosticAfterAuthDoctor.nextAction)
        XCTAssertTrue(bundle.authDoctorChecks.contains { $0.id == "kubeconfig" })
        let bundleJSON = String(decoding: exporter.saves[0], as: UTF8.self)
        XCTAssertFalse(bundleJSON.contains("SYNTHETICACCESSKEY"))
        XCTAssertFalse(bundleJSON.contains("synthetic-secret-material"))
    }

    func testImportAsCopyBindsCredentialsToFinalRenamedContext() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneNativeImportCopyTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
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
            exec:
              apiVersion: client.authentication.k8s.io/v1beta1
              command: aws
              args: [eks, get-token, --cluster-name, synthetic-cluster, --region, eu-north-1]
        """
        let existing = directory.appendingPathComponent("existing.yaml")
        try raw.write(to: existing, atomically: true, encoding: .utf8)
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: existing)])
        let credentials = RecordingNativeCloudCredentialConfigurator()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: NativeImportBookmarkStore()),
            kubeConfigDiscoverer: NativeImportEmptyDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                rootDirectory: directory.appendingPathComponent("imports", isDirectory: true)
            ),
            nativeCloudClusterImporter: FixedNativeCloudClusterImporter(result: NativeCloudClusterImportResult(
                provider: .eks,
                rawKubeConfig: raw,
                sourceName: "synthetic-eks.yaml"
            )),
            nativeAuthConfigurator: credentials,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runNativeEKSClusterImport(
            clusterName: "synthetic-cluster",
            region: "eu-north-1",
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: "synthetic-secret-material"
        )
        try await waitUntil { viewModel.isKubeConfigImportConfirmationPending }
        viewModel.kubeConfigDuplicateHandlingChoice = .importAsCopy
        viewModel.confirmKubeConfigImport()
        try await waitUntil { await credentials.awsCall() != nil }

        let recordedCall = await credentials.awsCall()
        XCTAssertEqual(recordedCall?.request.contextName, "synthetic-context-copy-2")
        XCTAssertEqual(recordedCall?.request.clusterName, "synthetic-cluster-copy-2")
        XCTAssertEqual(recordedCall?.request.userName, "synthetic-user-copy-2")
        XCTAssertEqual(recordedCall?.request.exec?.optionValue(for: "--cluster-name"), "synthetic-cluster")
    }

    func testKeychainBindingFailureKeepsActionableRecoveryStatusAfterActivation() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneNativeImportBindFailureTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
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
            exec:
              apiVersion: client.authentication.k8s.io/v1beta1
              command: aws
              args: [eks, get-token, --cluster-name, synthetic-cluster, --region, eu-north-1]
        """
        let credentials = RecordingNativeCloudCredentialConfigurator(shouldFailBinding: true)
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: NativeImportBookmarkStore()),
            kubeConfigDiscoverer: NativeImportEmptyDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                rootDirectory: directory.appendingPathComponent("imports", isDirectory: true)
            ),
            nativeCloudClusterImporter: FixedNativeCloudClusterImporter(result: NativeCloudClusterImportResult(
                provider: .eks,
                rawKubeConfig: raw,
                sourceName: "synthetic-eks.yaml"
            )),
            nativeAuthConfigurator: credentials,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runNativeEKSClusterImport(
            clusterName: "synthetic-cluster",
            region: "eu-north-1",
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: "synthetic-secret-material"
        )
        try await waitUntil { viewModel.isKubeConfigImportConfirmationPending }
        viewModel.confirmKubeConfigImport()
        try await waitUntil { !viewModel.isCommittingKubeConfigImport }

        XCTAssertEqual(
            viewModel.nativeKubernetesAuthStatus,
            "Cluster imported, but Keychain storage failed. Import again and choose Update existing in the review to retry."
        )
        XCTAssertFalse(state.kubeConfigSources.isEmpty)
        XCTAssertNotNil(state.lastError)
    }

    private func waitUntil(
        timeout: TimeInterval = 5,
        predicate: @escaping @MainActor () async -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !(await predicate()) {
            if Date() >= deadline {
                XCTFail("Timed out waiting for native cloud import")
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }
}

private struct FixedNativeCloudClusterImporter: NativeCloudClusterImporting {
    let result: NativeCloudClusterImportResult

    func importAKS(
        _: AKSNativeClusterImportRequest,
        clientSecret _: String
    ) async throws -> NativeCloudClusterImportResult {
        result
    }

    func importEKS(
        _: AWSEKSClusterImportRequest,
        credentials _: AWSEKSCredentials
    ) async throws -> NativeCloudClusterImportResult {
        result
    }

    func importGKE(
        _: GKENativeClusterImportRequest,
        serviceAccountJSON _: Data
    ) async throws -> NativeCloudClusterImportResult {
        result
    }
}

private actor HangingNativeCloudClusterImporter: NativeCloudClusterImporting {
    private var started = false
    private var eksCalls = 0

    func importAKS(
        _: AKSNativeClusterImportRequest,
        clientSecret _: String
    ) async throws -> NativeCloudClusterImportResult {
        throw CancellationError()
    }

    func importEKS(
        _: AWSEKSClusterImportRequest,
        credentials _: AWSEKSCredentials
    ) async throws -> NativeCloudClusterImportResult {
        eksCalls += 1
        started = true
        try await Task.sleep(nanoseconds: 30_000_000_000)
        throw CancellationError()
    }

    func importGKE(
        _: GKENativeClusterImportRequest,
        serviceAccountJSON _: Data
    ) async throws -> NativeCloudClusterImportResult {
        throw CancellationError()
    }

    func hasStarted() -> Bool { started }
    func eksCallCount() -> Int { eksCalls }
}

private struct SensitiveFailingNativeCloudClusterImporter: NativeCloudClusterImporting {
    let message: String

    func importAKS(
        _: AKSNativeClusterImportRequest,
        clientSecret _: String
    ) async throws -> NativeCloudClusterImportResult {
        throw SensitiveNativeImportFailure(message: message)
    }

    func importEKS(
        _: AWSEKSClusterImportRequest,
        credentials _: AWSEKSCredentials
    ) async throws -> NativeCloudClusterImportResult {
        throw SensitiveNativeImportFailure(message: message)
    }

    func importGKE(
        _: GKENativeClusterImportRequest,
        serviceAccountJSON _: Data
    ) async throws -> NativeCloudClusterImportResult {
        throw SensitiveNativeImportFailure(message: message)
    }
}

private struct SensitiveNativeImportFailure: LocalizedError, Sendable {
    let message: String

    var errorDescription: String? { message }
}

private struct DeniedEKSNativeCloudClusterImporter: NativeCloudClusterImporting {
    func importAKS(
        _: AKSNativeClusterImportRequest,
        clientSecret _: String
    ) async throws -> NativeCloudClusterImportResult {
        throw CancellationError()
    }

    func importEKS(
        _: AWSEKSClusterImportRequest,
        credentials _: AWSEKSCredentials
    ) async throws -> NativeCloudClusterImportResult {
        throw AWSEKSClusterImportError.accessDenied
    }

    func importGKE(
        _: GKENativeClusterImportRequest,
        serviceAccountJSON _: Data
    ) async throws -> NativeCloudClusterImportResult {
        throw CancellationError()
    }
}

private actor RecordingNativeCloudCredentialConfigurator:
    KubernetesNativeAuthConfiguring,
    KubernetesNativeCredentialProviding {
    struct AWSCall: Sendable {
        let request: KubernetesNativeCredentialRequest
        let credentials: AWSEKSCredentials
    }

    private var recordedAWSCall: AWSCall?
    private var recordedRemovedBindingIDs: [String] = []
    private let bindingDelayNanoseconds: UInt64
    private let shouldFailBinding: Bool
    private var bindingStarted = false

    init(
        bindingDelayNanoseconds: UInt64 = 0,
        shouldFailBinding: Bool = false
    ) {
        self.bindingDelayNanoseconds = bindingDelayNanoseconds
        self.shouldFailBinding = shouldFailBinding
    }

    func status(for request: KubernetesNativeCredentialRequest) async throws -> KubernetesNativeAuthProfileStatus {
        KubernetesNativeAuthProfileStatus(
            bindingID: request.bindingID,
            provider: request.provider,
            isConnected: recordedAWSCall?.request.bindingID == request.bindingID,
            expiresAt: nil
        )
    }

    func bindAWSCredentials(
        to request: KubernetesNativeCredentialRequest,
        credentials: AWSEKSCredentials,
        displayName _: String
    ) async throws {
        bindingStarted = true
        if bindingDelayNanoseconds > 0 {
            try await Task.sleep(nanoseconds: bindingDelayNanoseconds)
        }
        if shouldFailBinding {
            throw SyntheticNativeCredentialBindingError()
        }
        recordedAWSCall = AWSCall(request: request, credentials: credentials)
    }

    func bindAKSServicePrincipal(
        to _: KubernetesNativeCredentialRequest,
        clientSecret _: String,
        displayName _: String
    ) async throws {}

    func bindGCPServiceAccount(
        to _: KubernetesNativeCredentialRequest,
        serviceAccountJSON _: Data,
        displayName _: String
    ) async throws {}

    func removeProfile(for bindingID: String) async throws {
        recordedRemovedBindingIDs.append(bindingID)
        recordedAWSCall = nil
    }

    func credential(for request: KubernetesNativeCredentialRequest) async throws -> KubernetesNativeCredential? {
        guard recordedAWSCall?.request.bindingID == request.bindingID else { return nil }
        return KubernetesNativeCredential(
            bearerToken: "fake-token",
            expiresAt: Date().addingTimeInterval(300)
        )
    }

    func invalidateCredential(for _: String) async {}

    func awsCall() -> AWSCall? { recordedAWSCall }
    func removedBindingIDs() -> [String] { recordedRemovedBindingIDs }
    func hasStartedBinding() -> Bool { bindingStarted }
}

@MainActor
private final class NativeImportRecordingExporter: FileExporting {
    private(set) var saves: [Data] = []

    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        saves.append(data)
        return FileManager.default.temporaryDirectory.appendingPathComponent(suggestedName)
    }
}

@MainActor
private final class HoldingGKECredentialFilePicker: GKECredentialFilePicking {
    private(set) var beginCount = 0
    private(set) var cancelCount = 0
    private var completion: (@MainActor (GKECredentialFileSelection) -> Void)?

    func beginSelection(
        completion: @escaping @MainActor (GKECredentialFileSelection) -> Void
    ) {
        beginCount += 1
        self.completion = completion
    }

    func cancelSelection() {
        cancelCount += 1
        completion = nil
    }
}

private struct SyntheticNativeCredentialBindingError: LocalizedError {
    var errorDescription: String? { "Synthetic Keychain binding failure." }
}

private final class NativeImportBookmarkStore: BookmarkStore, @unchecked Sendable {
    private var records: [BookmarkRecord] = []

    func loadRecords() throws -> [BookmarkRecord] { records }
    func saveRecords(_ records: [BookmarkRecord]) throws { self.records = records }
}

private struct NativeImportEmptyDiscoverer: KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL] { [] }
}
