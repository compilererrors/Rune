import Foundation
import XCTest
@preconcurrency import Security
@testable import RuneSecurity

final class DefaultKubernetesNativeCredentialProviderTests: XCTestCase {
    func testAWSProfileBindingProducesNativeEKSCredentialAndNeverPersistsBearerToken() async throws {
        let secretStore = LockedMemorySecretStore()
        let profileStore = KeychainKubernetesNativeAuthProfileStore(
            secretStore: secretStore,
            keyPrefix: "synthetic.native-auth"
        )
        let now = Date(timeIntervalSince1970: 1_893_456_000)
        let provider = DefaultKubernetesNativeCredentialProvider(
            profileStore: profileStore,
            now: { now }
        )
        let request = awsRequest()
        let credentials = try AWSEKSCredentials(
            accessKeyID: "AKIDSYNTHETIC00000000",
            secretAccessKey: "synthetic-secret-material",
            sessionToken: "synthetic-session-material"
        )

        try await provider.bindAWSCredentials(
            to: request,
            credentials: credentials,
            displayName: "Synthetic AWS"
        )
        let resolvedCredential = try await provider.credential(for: request)
        let credential = try XCTUnwrap(resolvedCredential)
        let status = try await provider.status(for: request)

        XCTAssertTrue(credential.bearerToken.hasPrefix("k8s-aws-v1."))
        XCTAssertEqual(credential.expiresAt, now.addingTimeInterval(14 * 60))
        XCTAssertTrue(status.isConnected)
        XCTAssertFalse(secretStore.snapshot().values.contains { data in
            String(decoding: data, as: UTF8.self).contains("k8s-aws-v1.")
        })

        await provider.invalidateCredential(for: request.bindingID)
        let refreshedCredential = try await provider.credential(for: request)
        XCTAssertNotNil(refreshedCredential)
        try await provider.removeProfile(for: request.bindingID)
        let removedCredential = try await provider.credential(for: request)
        XCTAssertNil(removedCredential)
    }

    func testLateInvalidationCannotEvictNewerCredentialRevision() async throws {
        let provider = DefaultKubernetesNativeCredentialProvider(
            profileStore: KeychainKubernetesNativeAuthProfileStore(
                secretStore: LockedMemorySecretStore(),
                keyPrefix: "synthetic.revision"
            ),
            now: { Date(timeIntervalSince1970: 1_893_456_000) }
        )
        let request = awsRequest()
        try await provider.bindAWSCredentials(
            to: request,
            credentials: try AWSEKSCredentials(
                accessKeyID: "AKIDSYNTHETIC00000000",
                secretAccessKey: "synthetic-secret-material",
                sessionToken: "synthetic-session-material"
            ),
            displayName: "Synthetic AWS"
        )

        let staleResult = try await provider.credential(for: request)
        let stale = try XCTUnwrap(staleResult)
        await provider.invalidateCredential(for: request.bindingID)
        let freshResult = try await provider.credential(for: request)
        let fresh = try XCTUnwrap(freshResult)
        XCTAssertNotEqual(stale.revision, fresh.revision)

        await provider.invalidateCredential(
            for: request.bindingID,
            matchingRevision: stale.revision
        )
        let retainedResult = try await provider.credential(for: request)
        let retained = try XCTUnwrap(retainedResult)

        XCTAssertEqual(retained.revision, fresh.revision)

        await provider.invalidateCredential(
            for: request.bindingID,
            matchingRevision: fresh.revision
        )
        let refreshedResult = try await provider.credential(for: request)
        let refreshed = try XCTUnwrap(refreshedResult)
        XCTAssertNotEqual(refreshed.revision, fresh.revision)
    }

    func testEmbeddedOIDCAuthProviderUsesValidIDTokenWithoutNetworkOrKeychainProfile() async throws {
        let now = Date(timeIntervalSince1970: 1_893_456_000)
        let token = try makeJWT(
            issuer: "https://issuer.example.invalid",
            audience: "synthetic-kubernetes",
            expiration: now.addingTimeInterval(600)
        )
        let http = CountingFailingOIDCClient()
        let provider = DefaultKubernetesNativeCredentialProvider(
            profileStore: KeychainKubernetesNativeAuthProfileStore(
                secretStore: LockedMemorySecretStore(),
                keyPrefix: "synthetic.oidc"
            ),
            oidcClient: OIDCNativeAuthClient(httpClient: http, clock: FixedNativeOIDCClock(now: now)),
            now: { now }
        )
        let request = oidcRequest(idToken: token)

        let resolvedCredential = try await provider.credential(for: request)
        let credential = try XCTUnwrap(resolvedCredential)
        let requestCount = await http.requestCount()

        XCTAssertEqual(credential.bearerToken, token)
        XCTAssertEqual(credential.expiresAt, now.addingTimeInterval(600))
        XCTAssertEqual(requestCount, 0)
    }

    func testKnownAWSExecWithoutBindingReturnsNilForDirectFallback() async throws {
        let provider = DefaultKubernetesNativeCredentialProvider(
            profileStore: KeychainKubernetesNativeAuthProfileStore(
                secretStore: LockedMemorySecretStore(),
                keyPrefix: "synthetic.missing"
            )
        )

        let credential = try await provider.credential(for: awsRequest())
        XCTAssertNil(credential)
    }

    func testAKSBindingResolvesBearerThroughInjectedNativeTransportAndKeepsTokenOutOfKeychain() async throws {
        let secretStore = LockedMemorySecretStore()
        let http = NativeAKSHTTPClient()
        let provider = DefaultKubernetesNativeCredentialProvider(
            profileStore: KeychainKubernetesNativeAuthProfileStore(
                secretStore: secretStore,
                keyPrefix: "synthetic.aks"
            ),
            aksHTTPClient: http
        )
        let request = aksRequest()

        try await provider.bindAKSServicePrincipal(
            to: request,
            clientSecret: "synthetic-client-secret",
            displayName: "Synthetic AKS"
        )
        let resolvedCredential = try await provider.credential(for: request)
        let credential = try XCTUnwrap(resolvedCredential)
        let status = try await provider.status(for: request)
        let requestCount = await http.requestCount()

        XCTAssertEqual(credential.bearerToken, "synthetic-aks-access-token")
        XCTAssertTrue(status.isConnected)
        XCTAssertEqual(requestCount, 1)
        XCTAssertFalse(secretStore.snapshot().values.contains {
            String(decoding: $0, as: UTF8.self).contains("synthetic-aks-access-token")
        })
    }

    func testGCPBindingResolvesBearerThroughInjectedNativeTransportAndKeepsTokenOutOfKeychain() async throws {
        let secretStore = LockedMemorySecretStore()
        let http = NativeGCPHTTPClient()
        let provider = DefaultKubernetesNativeCredentialProvider(
            profileStore: KeychainKubernetesNativeAuthProfileStore(
                secretStore: secretStore,
                keyPrefix: "synthetic.gcp"
            ),
            gcpHTTPClient: http
        )
        let request = gcpRequest()
        let json = try makeServiceAccountJSON()

        try await provider.bindGCPServiceAccount(
            to: request,
            serviceAccountJSON: json,
            displayName: "Synthetic GKE"
        )
        let resolvedCredential = try await provider.credential(for: request)
        let credential = try XCTUnwrap(resolvedCredential)
        let status = try await provider.status(for: request)
        let requestCount = await http.requestCount()

        XCTAssertEqual(credential.bearerToken, "synthetic-gcp-access-token")
        XCTAssertTrue(status.isConnected)
        XCTAssertEqual(requestCount, 1)
        XCTAssertFalse(secretStore.snapshot().values.contains {
            String(decoding: $0, as: UTF8.self).contains("synthetic-gcp-access-token")
        })
    }

    func testGCPBindingRejectsInvalidJSONBeforeKeychainWrite() async throws {
        let secretStore = LockedMemorySecretStore()
        let provider = DefaultKubernetesNativeCredentialProvider(
            profileStore: KeychainKubernetesNativeAuthProfileStore(
                secretStore: secretStore,
                keyPrefix: "synthetic.gcp-invalid"
            )
        )

        do {
            try await provider.bindGCPServiceAccount(
                to: self.gcpRequest(),
                serviceAccountJSON: Data(#"{"type":"authorized_user"}"#.utf8),
                displayName: "Synthetic GKE"
            )
            XCTFail("Expected unsupported GCP credential type")
        } catch {
            XCTAssertEqual(error as? GCPServiceAccountAuthError, .unsupportedCredentialType)
        }
        XCTAssertTrue(secretStore.snapshot().isEmpty)
    }

    func testDisconnectDuringInFlightRefreshCannotPublishOrCacheOldCredential() async throws {
        let profileStore = KeychainKubernetesNativeAuthProfileStore(
            secretStore: LockedMemorySecretStore(),
            keyPrefix: "synthetic.aks-race-remove"
        )
        let http = BlockingFirstAKSHTTPClient()
        let provider = DefaultKubernetesNativeCredentialProvider(
            profileStore: profileStore,
            aksHTTPClient: http
        )
        let request = aksRequest()
        try await provider.bindAKSServicePrincipal(
            to: request,
            clientSecret: "synthetic-old-secret",
            displayName: "Synthetic AKS"
        )
        let pending = Task { try await provider.credential(for: request) }
        try await http.waitUntilFirstRequestStarts()

        try await provider.removeProfile(for: request.bindingID)
        await http.releaseFirstRequest(token: "synthetic-obsolete-token")

        do {
            _ = try await pending.value
            XCTFail("An obsolete in-flight credential must not be published after disconnect")
        } catch is CancellationError {
        } catch {
            XCTFail("Expected cancellation for obsolete credential, got \(error)")
        }
        let removedCredential = try await provider.credential(for: request)
        let removedStatus = try await provider.status(for: request)
        XCTAssertNil(removedCredential)
        XCTAssertFalse(removedStatus.isConnected)
    }

    func testRebindDuringInFlightRefreshCannotRestoreOldToken() async throws {
        let http = BlockingFirstAKSHTTPClient()
        let provider = DefaultKubernetesNativeCredentialProvider(
            profileStore: KeychainKubernetesNativeAuthProfileStore(
                secretStore: LockedMemorySecretStore(),
                keyPrefix: "synthetic.aks-race-rebind"
            ),
            aksHTTPClient: http
        )
        let request = aksRequest()
        try await provider.bindAKSServicePrincipal(
            to: request,
            clientSecret: "synthetic-old-secret",
            displayName: "Synthetic AKS"
        )
        let obsolete = Task { try await provider.credential(for: request) }
        try await http.waitUntilFirstRequestStarts()

        try await provider.bindAKSServicePrincipal(
            to: request,
            clientSecret: "synthetic-new-secret",
            displayName: "Synthetic AKS replacement"
        )
        await http.releaseFirstRequest(token: "synthetic-obsolete-token")
        do {
            _ = try await obsolete.value
            XCTFail("An obsolete in-flight credential must not be published after rebind")
        } catch is CancellationError {
        } catch {
            XCTFail("Expected cancellation for obsolete credential, got \(error)")
        }

        let refreshed = try await provider.credential(for: request)
        let requestCount = await http.requestCount()
        XCTAssertEqual(refreshed?.bearerToken, "synthetic-current-token")
        XCTAssertEqual(requestCount, 2)
    }

    func testStatusRejectsExpiredAWSAndCorruptedProviderProfiles() async throws {
        let store = KeychainKubernetesNativeAuthProfileStore(
            secretStore: LockedMemorySecretStore(),
            keyPrefix: "synthetic.status-validation"
        )
        let now = Date(timeIntervalSince1970: 2_000_000_000)
        let provider = DefaultKubernetesNativeCredentialProvider(profileStore: store, now: { now })
        let request = awsRequest()
        try await provider.bindAWSCredentials(
            to: request,
            credentials: try AWSEKSCredentials(
                accessKeyID: "AKIDSYNTHETIC00000000",
                secretAccessKey: "synthetic-expired-secret",
                expiration: now.addingTimeInterval(-1)
            ),
            displayName: "Expired AWS"
        )
        let expiredStatus = try await provider.status(for: request)
        XCTAssertFalse(expiredStatus.isConnected)

        try await store.save(
            profile: KubernetesNativeAuthProfile(
                bindingID: request.bindingID,
                provider: .awsEKS,
                displayName: "Corrupted AWS"
            ),
            secret: Data("not-json".utf8)
        )
        let corruptedStatus = try await provider.status(for: request)
        XCTAssertFalse(corruptedStatus.isConnected)
    }

    private func awsRequest() -> KubernetesNativeCredentialRequest {
        KubernetesNativeCredentialRequest(
            bindingID: "native-k8s-v1:synthetic-aws-binding",
            provider: .awsEKS,
            contextName: "synthetic-context",
            clusterName: "synthetic-cluster",
            userName: "synthetic-user",
            server: "https://127.0.0.1:6443",
            exec: KubernetesNativeAuthExecDescriptor(
                apiVersion: "client.authentication.k8s.io/v1",
                command: "aws",
                arguments: ["eks", "get-token", "--region", "eu-north-1", "--cluster-name", "synthetic-cluster"],
                interactiveMode: "Never"
            ),
            authProvider: nil
        )
    }

    private func oidcRequest(idToken: String) -> KubernetesNativeCredentialRequest {
        KubernetesNativeCredentialRequest(
            bindingID: "native-k8s-v1:synthetic-oidc-binding",
            provider: .oidc,
            contextName: "synthetic-context",
            clusterName: "synthetic-cluster",
            userName: "synthetic-user",
            server: "https://127.0.0.1:6443",
            exec: nil,
            authProvider: KubernetesNativeAuthProviderDescriptor(
                name: "oidc",
                configuration: [
                    "idp-issuer-url": "https://issuer.example.invalid",
                    "client-id": "synthetic-kubernetes",
                    "id-token": idToken
                ],
                sensitiveConfigurationKeys: ["id-token"]
            )
        )
    }

    private func aksRequest() -> KubernetesNativeCredentialRequest {
        KubernetesNativeCredentialRequest(
            bindingID: "native-k8s-v1:synthetic-aks-binding",
            provider: .azureKubelogin,
            contextName: "synthetic-aks-context",
            clusterName: "synthetic-aks-cluster",
            userName: "synthetic-aks-user",
            server: "https://127.0.0.1:6443",
            exec: KubernetesNativeAuthExecDescriptor(
                apiVersion: "client.authentication.k8s.io/v1",
                command: "kubelogin",
                arguments: [
                    "get-token",
                    "--tenant-id", "11111111-1111-4111-8111-111111111111",
                    "--server-id", "22222222-2222-4222-8222-222222222222",
                    "--client-id", "33333333-3333-4333-8333-333333333333",
                    "-l", "spn"
                ],
                interactiveMode: "Never"
            ),
            authProvider: nil
        )
    }

    private func gcpRequest() -> KubernetesNativeCredentialRequest {
        KubernetesNativeCredentialRequest(
            bindingID: "native-k8s-v1:synthetic-gcp-binding",
            provider: .googleGKE,
            contextName: "synthetic-gke-context",
            clusterName: "synthetic-gke-cluster",
            userName: "synthetic-gke-user",
            server: "https://127.0.0.1:6443",
            exec: KubernetesNativeAuthExecDescriptor(
                apiVersion: "client.authentication.k8s.io/v1",
                command: "gke-gcloud-auth-plugin",
                interactiveMode: "Never"
            ),
            authProvider: nil
        )
    }

    private func makeServiceAccountJSON() throws -> Data {
        let attributes: [CFString: Any] = [
            kSecAttrKeyType: kSecAttrKeyTypeRSA,
            kSecAttrKeySizeInBits: 2_048,
            kSecAttrIsPermanent: false
        ]
        var keyError: Unmanaged<CFError>?
        guard let key = SecKeyCreateRandomKey(attributes as CFDictionary, &keyError) else {
            throw XCTSkip("Could not generate synthetic RSA fixture")
        }
        var exportError: Unmanaged<CFError>?
        guard let pkcs1 = SecKeyCopyExternalRepresentation(key, &exportError) as Data? else {
            throw XCTSkip("Could not export synthetic RSA fixture")
        }
        let algorithm = Data([0x30, 0x0D, 0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01, 0x05, 0x00])
        let pkcs8 = derElement(
            tag: 0x30,
            value: Data([0x02, 0x01, 0x00]) + algorithm + derElement(tag: 0x04, value: pkcs1)
        )
        let encoded = pkcs8.base64EncodedString()
        let lines = stride(from: 0, to: encoded.count, by: 64).map { offset -> String in
            let start = encoded.index(encoded.startIndex, offsetBy: offset)
            let end = encoded.index(start, offsetBy: min(64, encoded.count - offset))
            return String(encoded[start..<end])
        }
        let pem = (["-----BEGIN PRIVATE KEY-----"] + lines + ["-----END PRIVATE KEY-----"])
            .joined(separator: "\n")
        return try JSONSerialization.data(withJSONObject: [
            "type": "service_account",
            "private_key": pem,
            "client_email": "rune-test@synthetic-project.iam.gserviceaccount.com",
            "token_uri": "https://oauth2.googleapis.com/token"
        ], options: [.sortedKeys])
    }

    private func derElement(tag: UInt8, value: Data) -> Data {
        var output = Data([tag])
        if value.count < 128 {
            output.append(UInt8(value.count))
        } else {
            var bytes: [UInt8] = []
            var length = value.count
            while length > 0 {
                bytes.insert(UInt8(length & 0xFF), at: 0)
                length >>= 8
            }
            output.append(0x80 | UInt8(bytes.count))
            output.append(contentsOf: bytes)
        }
        output.append(value)
        return output
    }

    private func makeJWT(issuer: String, audience: String, expiration: Date) throws -> String {
        let header = try JSONSerialization.data(withJSONObject: ["alg": "none", "typ": "JWT"], options: [.sortedKeys])
        let payload = try JSONSerialization.data(withJSONObject: [
            "iss": issuer,
            "aud": audience,
            "exp": Int(expiration.timeIntervalSince1970)
        ], options: [.sortedKeys])
        return [header, payload, Data("synthetic-signature".utf8)]
            .map(base64URL)
            .joined(separator: ".")
    }

    private func base64URL(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}

private actor NativeAKSHTTPClient: AKSServicePrincipalTokenHTTPClient {
    private var count = 0

    func send(_ request: AKSServicePrincipalTokenHTTPRequest) async throws -> AKSServicePrincipalTokenHTTPResponse {
        count += 1
        XCTAssertEqual(request.url.scheme, "https")
        return AKSServicePrincipalTokenHTTPResponse(
            statusCode: 200,
            body: Data(#"{"access_token":"synthetic-aks-access-token","token_type":"Bearer","expires_in":3600}"#.utf8)
        )
    }

    func requestCount() -> Int { count }
}

private actor BlockingFirstAKSHTTPClient: AKSServicePrincipalTokenHTTPClient {
    private var count = 0
    private var firstContinuation: CheckedContinuation<AKSServicePrincipalTokenHTTPResponse, Never>?

    func send(_ request: AKSServicePrincipalTokenHTTPRequest) async throws -> AKSServicePrincipalTokenHTTPResponse {
        count += 1
        if count == 1 {
            return await withCheckedContinuation { continuation in
                firstContinuation = continuation
            }
        }
        let body = String(decoding: request.body, as: UTF8.self)
        XCTAssertTrue(body.contains("client_secret=synthetic-new-secret"))
        return response(token: "synthetic-current-token")
    }

    func waitUntilFirstRequestStarts() async throws {
        let deadline = ContinuousClock.now + .seconds(2)
        while count == 0 {
            guard ContinuousClock.now < deadline else {
                throw XCTSkip("Timed out waiting for synthetic AKS request")
            }
            try await Task.sleep(for: .milliseconds(5))
        }
    }

    func releaseFirstRequest(token: String) {
        firstContinuation?.resume(returning: response(token: token))
        firstContinuation = nil
    }

    func requestCount() -> Int { count }

    private func response(token: String) -> AKSServicePrincipalTokenHTTPResponse {
        AKSServicePrincipalTokenHTTPResponse(
            statusCode: 200,
            body: Data("{\"access_token\":\"\(token)\",\"token_type\":\"Bearer\",\"expires_in\":3600}".utf8)
        )
    }
}

private actor NativeGCPHTTPClient: GCPServiceAccountHTTPClient {
    private var count = 0

    func send(_ request: GCPServiceAccountHTTPRequest) async throws -> GCPServiceAccountHTTPResponse {
        count += 1
        XCTAssertEqual(request.url.absoluteString, "https://oauth2.googleapis.com/token")
        return GCPServiceAccountHTTPResponse(
            statusCode: 200,
            body: Data(#"{"access_token":"synthetic-gcp-access-token","token_type":"Bearer","expires_in":3600}"#.utf8)
        )
    }

    func requestCount() -> Int { count }
}

private final class LockedMemorySecretStore: SecretStore, @unchecked Sendable {
    private let lock = NSLock()
    private var values: [String: Data] = [:]

    func set(_ value: Data, for key: String) throws {
        lock.lock()
        values[key] = value
        lock.unlock()
    }

    func get(for key: String) throws -> Data? {
        lock.lock()
        defer { lock.unlock() }
        return values[key]
    }

    func delete(for key: String) throws {
        lock.lock()
        values.removeValue(forKey: key)
        lock.unlock()
    }

    func snapshot() -> [String: Data] {
        lock.lock()
        defer { lock.unlock() }
        return values
    }
}

private struct FixedNativeOIDCClock: OIDCClock {
    let nowValue: Date
    init(now: Date) { self.nowValue = now }
    func now() -> Date { nowValue }
}

private actor CountingFailingOIDCClient: OIDCHTTPClient {
    private var count = 0
    nonisolated let supportsCustomCertificateAuthorities = false

    func send(_ request: OIDCHTTPRequest) async throws -> OIDCHTTPResponse {
        count += 1
        throw OIDCNativeAuthError.transport(stage: .request)
    }

    func requestCount() -> Int { count }
}
