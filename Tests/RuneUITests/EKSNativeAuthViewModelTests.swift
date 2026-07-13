import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneSecurity
@testable import RuneUI

@MainActor
final class EKSNativeAuthViewModelTests: XCTestCase {
    func testConnectSelectedEKSNativeAuthBindsSelectedDescriptorAndVerifiesContext() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreSetting(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfigURL = try writeKubeconfig(eksKubeconfig(from: server.kubeconfigYAML()))
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }

        let source = KubeConfigSource(url: kubeconfigURL)
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let state = RuneAppState()
        state.setSources([source])
        state.setContexts([context])
        state.selectedContext = context
        let configurator = RecordingNativeAuthConfigurator()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: makeKubeClient(nativeProvider: configurator),
            nativeAuthConfigurator: configurator
        )

        let accessKeyID = "SYNTHETICACCESSKEY"
        let secretAccessKey = "synthetic-secret-material"
        let sessionToken = "synthetic-session-material"
        let expiration = Date(timeIntervalSince1970: 2_000_000_000)
        viewModel.connectSelectedEKSNativeAuth(
            accessKeyID: "  \(accessKeyID)  ",
            secretAccessKey: "  \(secretAccessKey)  ",
            sessionToken: "  \(sessionToken)  ",
            expiration: expiration
        )

        try await waitUntil { !viewModel.isConnectingNativeKubernetesAuth }
        let recordedCall = await configurator.recordedCall()
        let call = try XCTUnwrap(recordedCall)
        let expectedRequest = try XCTUnwrap(
            KubeConfigNativeAuthAnalyzer()
                .analyze(source: source)
                .contexts
                .first(where: { $0.contextName == context.name })?
                .credentialRequest
        )

        XCTAssertEqual(call.request, expectedRequest)
        XCTAssertEqual(call.request.provider, .awsEKS)
        XCTAssertEqual(call.request.contextName, RuneFakeK8sFixture.defaultContextName)
        XCTAssertEqual(call.request.exec?.optionValue(for: "--cluster-name"), "synthetic-cluster")
        XCTAssertEqual(call.request.exec?.optionValue(for: "--region"), "eu-north-1")
        XCTAssertEqual(call.credentials.accessKeyID, accessKeyID)
        XCTAssertEqual(call.credentials.secretAccessKey, secretAccessKey)
        XCTAssertEqual(call.credentials.sessionToken, sessionToken)
        XCTAssertEqual(call.credentials.expiration, expiration)
        XCTAssertEqual(call.displayName, "AWS EKS")
        XCTAssertEqual(viewModel.nativeKubernetesAuthStatus, "AWS native authentication is connected.")
        XCTAssertNil(state.lastError)
        assertSecretsAreAbsent(
            [accessKeyID, secretAccessKey, sessionToken],
            from: [viewModel.nativeKubernetesAuthStatus, state.lastError]
        )
    }

    func testConnectSelectedEKSNativeAuthRejectsNonEKSContextWithActionableProviderError() async throws {
        let kubeconfigURL = try writeKubeconfig(gkeKubeconfig)
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }
        let context = KubeContext(name: "synthetic-gke")
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfigURL)])
        state.setContexts([context])
        state.selectedContext = context
        let configurator = RecordingNativeAuthConfigurator()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: makeKubeClient(nativeProvider: configurator),
            nativeAuthConfigurator: configurator
        )

        let secretAccessKey = "must-not-appear-secret"
        viewModel.connectSelectedEKSNativeAuth(
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: secretAccessKey
        )

        try await waitUntil { !viewModel.isConnectingNativeKubernetesAuth }

        let recordedCall = await configurator.recordedCall()
        XCTAssertNil(recordedCall)
        XCTAssertEqual(viewModel.nativeKubernetesAuthStatus, "AWS native authentication could not be connected.")
        XCTAssertEqual(
            state.lastError,
            "Invalid input: The selected context uses Google GKE, not Amazon EKS."
        )
        assertSecretsAreAbsent(
            ["SYNTHETICACCESSKEY", secretAccessKey],
            from: [viewModel.nativeKubernetesAuthStatus, state.lastError]
        )
    }

    func testConnectSelectedEKSNativeAuthExplainsUnsupportedContext() async throws {
        let kubeconfigURL = try writeKubeconfig(staticTokenKubeconfig)
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }
        let context = KubeContext(name: "synthetic-static")
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfigURL)])
        state.setContexts([context])
        state.selectedContext = context
        let configurator = RecordingNativeAuthConfigurator()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: makeKubeClient(nativeProvider: configurator),
            nativeAuthConfigurator: configurator
        )

        viewModel.connectSelectedEKSNativeAuth(
            accessKeyID: "SYNTHETICACCESSKEY",
            secretAccessKey: "synthetic-secret-material"
        )

        try await waitUntil { !viewModel.isConnectingNativeKubernetesAuth }

        let recordedCall = await configurator.recordedCall()
        XCTAssertNil(recordedCall)
        XCTAssertEqual(
            state.lastError,
            "Invalid input: The selected context does not contain a supported native authentication configuration."
        )
    }

    func testAppStoreEKSFlowUsesSecureFieldsClearsDraftAndKeepsImportAvailable() throws {
        let source = try String(contentsOf: runeRootViewURL, encoding: .utf8)

        XCTAssertTrue(source.contains("if !RuneExternalCommandPolicy.allowsExternalCommands"))
        XCTAssertTrue(source.contains("SecureField(\"AWS access key ID\""))
        XCTAssertTrue(source.contains("SecureField(\"AWS secret access key\""))
        XCTAssertTrue(source.contains("SecureField(\"AWS session token (optional)\""))
        XCTAssertTrue(source.contains("viewModel.connectSelectedEKSNativeAuth("))
        XCTAssertTrue(source.contains("cloudCredentialDraft.nativeAWSAccessKeyID = \"\""))
        XCTAssertTrue(source.contains("cloudCredentialDraft.nativeAWSSecretAccessKey = \"\""))
        XCTAssertTrue(source.contains("cloudCredentialDraft.nativeAWSSessionToken = \"\""))
        XCTAssertTrue(source.contains("Label(\"Import…\", systemImage: \"doc.badge.plus\")"))
        XCTAssertTrue(source.contains("Import a provider kubeconfig before connecting native credentials."))
    }

    func testConnectSelectedAKSNativeAuthBindsOnlySecretToKubeloginDescriptor() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreSetting(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfigURL = try writeKubeconfig(aksKubeconfig(from: server.kubeconfigYAML()))
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }
        let source = KubeConfigSource(url: kubeconfigURL)
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let state = RuneAppState()
        state.setSources([source])
        state.setContexts([context])
        state.selectedContext = context
        let configurator = RecordingNativeAuthConfigurator()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: makeKubeClient(nativeProvider: configurator),
            nativeAuthConfigurator: configurator
        )
        let secret = "synthetic-azure-secret"

        viewModel.connectSelectedAKSNativeAuth(clientSecret: secret)
        try await waitUntil { !viewModel.isConnectingNativeKubernetesAuth }

        let recordedCall = await configurator.recordedAKSCall()
        let call = try XCTUnwrap(recordedCall)
        XCTAssertEqual(call.request.provider, .azureKubelogin)
        XCTAssertEqual(call.clientSecret, secret)
        XCTAssertEqual(call.displayName, "Azure AKS")
        XCTAssertEqual(viewModel.nativeKubernetesAuthStatus, "Azure native authentication is connected.")
        assertSecretsAreAbsent([secret], from: [viewModel.nativeKubernetesAuthStatus, state.lastError])
    }

    func testConnectSelectedGKENativeAuthBindsSelectedContextWithoutPersistingJSONInStatus() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreSetting(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfigURL = try writeKubeconfig(gkeKubeconfig(from: server.kubeconfigYAML()))
        defer { try? FileManager.default.removeItem(at: kubeconfigURL) }
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfigURL)])
        state.setContexts([context])
        state.selectedContext = context
        let configurator = RecordingNativeAuthConfigurator()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: makeKubeClient(nativeProvider: configurator),
            nativeAuthConfigurator: configurator
        )
        let json = Data(#"{"type":"service_account","synthetic_secret":"must-not-appear"}"#.utf8)

        viewModel.connectSelectedGKENativeAuth(serviceAccountJSON: json)
        try await waitUntil { !viewModel.isConnectingNativeKubernetesAuth }

        let recordedCall = await configurator.recordedGCPCall()
        let call = try XCTUnwrap(recordedCall)
        XCTAssertEqual(call.request.provider, .googleGKE)
        XCTAssertEqual(call.serviceAccountJSON, json)
        XCTAssertEqual(call.displayName, "Google GKE")
        XCTAssertEqual(viewModel.nativeKubernetesAuthStatus, "Google native authentication is connected.")
        assertSecretsAreAbsent(["must-not-appear"], from: [viewModel.nativeKubernetesAuthStatus, state.lastError])
    }

    func testAppStoreAzureAndGoogleFlowsUseUserMediatedSecretsAndClearDrafts() throws {
        let source = try String(contentsOf: runeRootViewURL, encoding: .utf8)
        let viewModelSource = try String(contentsOf: runeAppViewModelURL, encoding: .utf8)

        XCTAssertTrue(source.contains("SecureField(\"Azure service-principal secret\""))
        XCTAssertTrue(source.contains("cloudCredentialDraft.nativeAKSClientSecret = \"\""))
        XCTAssertTrue(source.contains("viewModel.connectSelectedAKSNativeAuth(clientSecret: secret)"))
        XCTAssertTrue(source.contains("viewModel.chooseAndConnectSelectedGKENativeAuth()"))
        XCTAssertTrue(viewModelSource.contains("let panel = NSOpenPanel()"))
        XCTAssertTrue(viewModelSource.contains("panel.allowedContentTypes = [.json]"))
        XCTAssertTrue(viewModelSource.contains("read(upToCount: 1_048_577)"))
        XCTAssertTrue(viewModelSource.contains("startAccessingSecurityScopedResource()"))
    }

    private func eksKubeconfig(from base: String) -> String {
        base.replacingOccurrences(
            of: "    token: fake-token",
            with: "    exec:\n"
                + "      apiVersion: client.authentication.k8s.io/v1\n"
                + "      command: aws\n"
                + "      args: [eks, get-token, --cluster-name, synthetic-cluster, --region, eu-north-1]\n"
                + "      interactiveMode: Never"
        )
    }

    private func aksKubeconfig(from base: String) -> String {
        base.replacingOccurrences(
            of: "    token: fake-token",
            with: "    exec:\n"
                + "      apiVersion: client.authentication.k8s.io/v1\n"
                + "      command: kubelogin\n"
                + "      args: [get-token, --tenant-id, 11111111-1111-4111-8111-111111111111, --server-id, 22222222-2222-4222-8222-222222222222, --client-id, 33333333-3333-4333-8333-333333333333, -l, spn]\n"
                + "      interactiveMode: Never"
        )
    }

    private func gkeKubeconfig(from base: String) -> String {
        base.replacingOccurrences(
            of: "    token: fake-token",
            with: "    exec:\n"
                + "      apiVersion: client.authentication.k8s.io/v1\n"
                + "      command: gke-gcloud-auth-plugin\n"
                + "      interactiveMode: Never"
        )
    }

    private var gkeKubeconfig: String {
        """
        apiVersion: v1
        kind: Config
        current-context: synthetic-gke
        clusters:
        - name: synthetic-gke-cluster
          cluster:
            server: https://cluster.example.invalid
        contexts:
        - name: synthetic-gke
          context:
            cluster: synthetic-gke-cluster
            user: synthetic-gke-user
        users:
        - name: synthetic-gke-user
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1beta1
              command: gke-gcloud-auth-plugin
        """
    }

    private var staticTokenKubeconfig: String {
        """
        apiVersion: v1
        kind: Config
        current-context: synthetic-static
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://cluster.example.invalid
        contexts:
        - name: synthetic-static
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
            token: synthetic-placeholder-token
        """
    }

    private func writeKubeconfig(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-native-auth-ui-\(UUID().uuidString).yaml")
        try contents.write(to: url, atomically: true, encoding: .utf8)
        return url
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
                XCTFail("Timed out waiting for native authentication", file: file, line: line)
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    private func makeKubeClient(
        nativeProvider: any KubernetesNativeCredentialProviding
    ) -> KubernetesClient {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        return KubernetesClient(
            commandTimeout: 2,
            restClient: KubernetesRESTClient(
                requestMetricsRecorder: recorder,
                nativeCredentialProvider: nativeProvider
            ),
            requestMetricsRecorder: recorder
        )
    }

    private func assertSecretsAreAbsent(
        _ secrets: [String],
        from messages: [String?],
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        for message in messages.compactMap({ $0 }) {
            for secret in secrets {
                XCTAssertFalse(message.contains(secret), file: file, line: line)
            }
        }
    }

    private func restoreSetting(_ value: Any?, forKey key: String) {
        if let value {
            UserDefaults.standard.set(value, forKey: key)
        } else {
            UserDefaults.standard.removeObject(forKey: key)
        }
    }

    private var runeRootViewURL: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift")
    }

    private var runeAppViewModelURL: URL {
        runeRootViewURL.deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("ViewModels/RuneAppViewModel.swift")
    }
}

private actor RecordingNativeAuthConfigurator: KubernetesNativeAuthConfiguring, KubernetesNativeCredentialProviding {
    struct Call: Sendable {
        let request: KubernetesNativeCredentialRequest
        let credentials: AWSEKSCredentials
        let displayName: String
    }

    struct AKSCall: Sendable {
        let request: KubernetesNativeCredentialRequest
        let clientSecret: String
        let displayName: String
    }

    struct GCPCall: Sendable {
        let request: KubernetesNativeCredentialRequest
        let serviceAccountJSON: Data
        let displayName: String
    }

    private var call: Call?
    private var aksCall: AKSCall?
    private var gcpCall: GCPCall?

    func status(for request: KubernetesNativeCredentialRequest) async throws -> KubernetesNativeAuthProfileStatus {
        KubernetesNativeAuthProfileStatus(
            bindingID: request.bindingID,
            provider: request.provider,
            isConnected: call?.request.bindingID == request.bindingID
                || aksCall?.request.bindingID == request.bindingID
                || gcpCall?.request.bindingID == request.bindingID,
            expiresAt: call?.credentials.expiration
        )
    }

    func bindAKSServicePrincipal(
        to request: KubernetesNativeCredentialRequest,
        clientSecret: String,
        displayName: String
    ) async throws {
        aksCall = AKSCall(request: request, clientSecret: clientSecret, displayName: displayName)
    }

    func bindGCPServiceAccount(
        to request: KubernetesNativeCredentialRequest,
        serviceAccountJSON: Data,
        displayName: String
    ) async throws {
        gcpCall = GCPCall(request: request, serviceAccountJSON: serviceAccountJSON, displayName: displayName)
    }

    func credential(for request: KubernetesNativeCredentialRequest) async throws -> KubernetesNativeCredential? {
        let isBound = call?.request.bindingID == request.bindingID
            || aksCall?.request.bindingID == request.bindingID
            || gcpCall?.request.bindingID == request.bindingID
        guard isBound else { return nil }
        return KubernetesNativeCredential(
            bearerToken: "fake-token",
            expiresAt: Date().addingTimeInterval(300)
        )
    }

    func invalidateCredential(for _: String) async {}

    func bindAWSCredentials(
        to request: KubernetesNativeCredentialRequest,
        credentials: AWSEKSCredentials,
        displayName: String
    ) async throws {
        call = Call(request: request, credentials: credentials, displayName: displayName)
    }

    func removeProfile(for bindingID: String) async throws {
        if call?.request.bindingID == bindingID {
            call = nil
        }
        if aksCall?.request.bindingID == bindingID { aksCall = nil }
        if gcpCall?.request.bindingID == bindingID { gcpCall = nil }
    }

    func recordedCall() -> Call? {
        call
    }

    func recordedAKSCall() -> AKSCall? { aksCall }
    func recordedGCPCall() -> GCPCall? { gcpCall }
}
