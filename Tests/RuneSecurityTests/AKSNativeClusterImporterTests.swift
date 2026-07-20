import Foundation
import XCTest
@testable import RuneSecurity

final class AKSNativeClusterImporterTests: XCTestCase {
    private let subscriptionID = "11111111-1111-4111-8111-111111111111"
    private let tenantID = "22222222-2222-4222-8222-222222222222"
    private let clientID = "33333333-3333-4333-8333-333333333333"
    private let generatedClientID = "44444444-4444-4444-8444-444444444444"
    private let serverID = "55555555-5555-4555-8555-555555555555"

    func testImportUsesARMContractsAndRewritesCurrentKubeloginUserToSPN() async throws {
        let rawProviderKubeConfig = generatedKubeConfig()
        let tokenHTTP = RecordingAKSNativeTokenHTTPClient { [tenantID, clientID] request in
            XCTAssertEqual(request.url.absoluteString, "https://login.microsoftonline.com/\(tenantID)/oauth2/v2.0/token")
            XCTAssertEqual(request.headers["Accept"], "application/json")
            XCTAssertEqual(request.headers["Content-Type"], "application/x-www-form-urlencoded")
            let fields = Self.formFields(request.body)
            XCTAssertEqual(fields["client_id"], clientID)
            XCTAssertEqual(fields["client_secret"], "synthetic+/= secret")
            XCTAssertEqual(fields["grant_type"], "client_credentials")
            XCTAssertEqual(fields["scope"], "https://management.azure.com/.default")
            return Self.tokenResponse()
        }
        let clusterHTTP = RecordingAKSClusterHTTPClient { [subscriptionID, rawProviderKubeConfig] request in
            XCTAssertEqual(request.method, "POST")
            XCTAssertEqual(request.url.scheme, "https")
            XCTAssertEqual(request.url.host, "management.azure.com")
            XCTAssertEqual(
                request.url.path,
                "/subscriptions/\(subscriptionID)/resourceGroups/synthetic-group/providers/Microsoft.ContainerService/managedClusters/synthetic-cluster/listClusterUserCredential"
            )
            let query = Dictionary(uniqueKeysWithValues: URLComponents(
                url: request.url,
                resolvingAgainstBaseURL: false
            )!.queryItems!.map { ($0.name, $0.value ?? "") })
            XCTAssertEqual(query["api-version"], "2026-04-01")
            XCTAssertEqual(query["format"], "exec")
            XCTAssertEqual(request.headers["Accept"], "application/json")
            XCTAssertEqual(request.headers["Authorization"], "Bearer synthetic-arm-token")
            XCTAssertTrue(request.body.isEmpty)
            return Self.clusterResponse(kubeConfig: rawProviderKubeConfig)
        }
        let importer = AKSNativeClusterImporter(
            httpClient: clusterHTTP,
            tokenHTTPClient: tokenHTTP
        )

        let result = try await importer.importCluster(
            request(),
            clientSecret: "synthetic+/= secret"
        )

        XCTAssertEqual(result.sourceName, "synthetic-cluster-kubeconfig.yaml")
        XCTAssertFalse(result.rawKubeConfig.contains("synthetic+/= secret"))
        XCTAssertFalse(result.rawKubeConfig.contains(generatedClientID))
        XCTAssertFalse(result.rawKubeConfig.contains("devicecode"))
        XCTAssertFalse(result.description.contains(result.rawKubeConfig))

        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(raw: result.rawKubeConfig)
        XCTAssertEqual(analysis.currentContext, "synthetic-context")
        let context = try XCTUnwrap(analysis.contexts.first)
        XCTAssertEqual(context.provider, .azureKubelogin)
        XCTAssertEqual(context.exec?.command, "kubelogin")
        XCTAssertEqual(context.exec?.interactiveMode, "Never")
        XCTAssertTrue(context.exec?.environment.isEmpty == true)
        let descriptor = try XCTUnwrap(try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: try XCTUnwrap(context.exec?.command),
            arguments: try XCTUnwrap(context.exec?.arguments)
        ))
        XCTAssertEqual(descriptor.tenantID, tenantID)
        XCTAssertEqual(descriptor.clientID, clientID)
        XCTAssertEqual(descriptor.serverID, serverID)
        XCTAssertEqual(descriptor.environment, .publicCloud)
        XCTAssertNotNil(context.credentialRequest)
        let tokenRequestCount = await tokenHTTP.requestCount()
        let clusterRequestCount = await clusterHTTP.requestCount()
        XCTAssertEqual(tokenRequestCount, 1)
        XCTAssertEqual(clusterRequestCount, 1)
    }

    func testRequestValidationFailsBeforeCredentialsOrNetwork() async throws {
        XCTAssertThrowsError(try AKSNativeClusterImportRequest(
            subscriptionID: "not-a-subscription",
            resourceGroup: "synthetic-group",
            clusterName: "synthetic-cluster",
            tenantID: tenantID,
            clientID: clientID
        )) { error in
            XCTAssertEqual(error as? AKSNativeClusterImportError, .invalidRequest(field: "subscription-id"))
        }
        XCTAssertThrowsError(try AKSNativeClusterImportRequest(
            subscriptionID: subscriptionID,
            resourceGroup: "../synthetic-group",
            clusterName: "synthetic-cluster",
            tenantID: tenantID,
            clientID: clientID
        )) { error in
            XCTAssertEqual(error as? AKSNativeClusterImportError, .invalidRequest(field: "resource-group"))
        }
        XCTAssertThrowsError(try AKSNativeClusterImportRequest(
            subscriptionID: subscriptionID,
            resourceGroup: "synthetic-group",
            clusterName: "-synthetic-cluster",
            tenantID: tenantID,
            clientID: clientID
        )) { error in
            XCTAssertEqual(error as? AKSNativeClusterImportError, .invalidRequest(field: "cluster-name"))
        }

        let tokenHTTP = RecordingAKSNativeTokenHTTPClient { _ in
            XCTFail("Invalid credentials must fail before HTTP")
            return Self.tokenResponse()
        }
        let clusterHTTP = RecordingAKSClusterHTTPClient { _ in
            XCTFail("Invalid credentials must fail before HTTP")
            return Self.clusterResponse(kubeConfig: "unused")
        }
        let importer = AKSNativeClusterImporter(httpClient: clusterHTTP, tokenHTTPClient: tokenHTTP)
        do {
            _ = try await importer.importCluster(request(), clientSecret: "secret\nmaterial")
            XCTFail("Expected invalid credentials")
        } catch {
            XCTAssertEqual(error as? AKSNativeClusterImportError, .invalidCredentials)
        }
        let tokenRequestCount = await tokenHTTP.requestCount()
        let clusterRequestCount = await clusterHTTP.requestCount()
        XCTAssertEqual(tokenRequestCount, 0)
        XCTAssertEqual(clusterRequestCount, 0)
    }

    func testTenantMismatchFailsAfterFetchWithoutLeakingSecret() async throws {
        let otherTenantID = "66666666-6666-4666-8666-666666666666"
        let rawProviderKubeConfig = generatedKubeConfig(tenantID: otherTenantID)
        let tokenHTTP = RecordingAKSNativeTokenHTTPClient { _ in Self.tokenResponse() }
        let clusterHTTP = RecordingAKSClusterHTTPClient { [rawProviderKubeConfig] _ in
            Self.clusterResponse(kubeConfig: rawProviderKubeConfig)
        }
        let importer = AKSNativeClusterImporter(httpClient: clusterHTTP, tokenHTTPClient: tokenHTTP)

        do {
            _ = try await importer.importCluster(request(), clientSecret: "never-expose-this-secret")
            XCTFail("Expected tenant mismatch")
        } catch {
            XCTAssertEqual(error as? AKSNativeClusterImportError, .tenantMismatch)
            XCTAssertFalse(error.localizedDescription.contains("never-expose-this-secret"))
            XCTAssertFalse(error.localizedDescription.contains(otherTenantID))
        }
    }

    func testUnexpectedKubeloginOptionsFailClosed() async throws {
        let rawProviderKubeConfig = generatedKubeConfig()
        let tokenHTTP = RecordingAKSNativeTokenHTTPClient { _ in Self.tokenResponse() }
        let clusterHTTP = RecordingAKSClusterHTTPClient { [rawProviderKubeConfig] _ in
            let raw = rawProviderKubeConfig.replacingOccurrences(
                of: "      - --server-id",
                with: "      - --legacy\n      - --server-id"
            )
            return Self.clusterResponse(kubeConfig: raw)
        }
        let importer = AKSNativeClusterImporter(httpClient: clusterHTTP, tokenHTTPClient: tokenHTTP)

        do {
            _ = try await importer.importCluster(request(), clientSecret: "synthetic-secret")
            XCTFail("Expected incompatible kubeconfig")
        } catch {
            XCTAssertEqual(error as? AKSNativeClusterImportError, .incompatibleKubeConfig)
        }
    }

    func testMalformedCredentialResponsesFailClosed() async throws {
        let malformedBodies: [Data] = [
            Data(#"{}"#.utf8),
            Self.jsonData(["kubeconfigs": []]),
            Self.jsonData(["kubeconfigs": [["value": "not-base64"]]]),
            Self.jsonData(["kubeconfigs": [["value": Data([0xFF, 0xFE]).base64EncodedString()]]])
        ]

        for body in malformedBodies {
            let tokenHTTP = RecordingAKSNativeTokenHTTPClient { _ in Self.tokenResponse() }
            let clusterHTTP = RecordingAKSClusterHTTPClient { _ in
                AKSClusterHTTPResponse(statusCode: 200, body: body)
            }
            let importer = AKSNativeClusterImporter(httpClient: clusterHTTP, tokenHTTPClient: tokenHTTP)
            do {
                _ = try await importer.importCluster(request(), clientSecret: "synthetic-secret")
                XCTFail("Expected invalid provider response")
            } catch {
                XCTAssertEqual(error as? AKSNativeClusterImportError, .invalidProviderResponse)
            }
        }
    }

    func testProviderErrorsAndTransportErrorsAreSanitized() async throws {
        let rejectedTokenHTTP = RecordingAKSNativeTokenHTTPClient { _ in
            AKSServicePrincipalTokenHTTPResponse(
                statusCode: 401,
                body: Self.jsonData([
                    "error": "invalid_client<script>",
                    "error_description": "client_secret=never-expose-this-secret"
                ])
            )
        }
        let unusedClusterHTTP = RecordingAKSClusterHTTPClient { _ in
            XCTFail("Cluster request must not run after authentication rejection")
            return Self.clusterResponse(kubeConfig: "unused")
        }
        let rejectedImporter = AKSNativeClusterImporter(
            httpClient: unusedClusterHTTP,
            tokenHTTPClient: rejectedTokenHTTP
        )
        do {
            _ = try await rejectedImporter.importCluster(request(), clientSecret: "never-expose-this-secret")
            XCTFail("Expected authentication rejection")
        } catch {
            XCTAssertEqual(
                error as? AKSNativeClusterImportError,
                .authenticationFailed(statusCode: 401, code: "invalid_clientscript")
            )
            XCTAssertFalse(error.localizedDescription.contains("never-expose-this-secret"))
        }

        let acceptedTokenHTTP = RecordingAKSNativeTokenHTTPClient { _ in Self.tokenResponse() }
        let rejectedClusterHTTP = RecordingAKSClusterHTTPClient { _ in
            AKSClusterHTTPResponse(
                statusCode: 403,
                body: Self.jsonData([
                    "error": [
                        "code": "AuthorizationFailed<script>",
                        "message": "Bearer never-expose-this-token"
                    ]
                ])
            )
        }
        let clusterRejectedImporter = AKSNativeClusterImporter(
            httpClient: rejectedClusterHTTP,
            tokenHTTPClient: acceptedTokenHTTP
        )
        do {
            _ = try await clusterRejectedImporter.importCluster(request(), clientSecret: "never-expose-this-secret")
            XCTFail("Expected cluster rejection")
        } catch {
            XCTAssertEqual(
                error as? AKSNativeClusterImportError,
                .clusterRequestFailed(statusCode: 403, code: "AuthorizationFailedscript")
            )
            XCTAssertFalse(error.localizedDescription.contains("never-expose"))
        }

        let transportTokenHTTP = RecordingAKSNativeTokenHTTPClient { _ in
            throw SyntheticAKSNativeImportError.secret("client_secret=never-expose-this-secret")
        }
        let transportImporter = AKSNativeClusterImporter(
            httpClient: unusedClusterHTTP,
            tokenHTTPClient: transportTokenHTTP
        )
        do {
            _ = try await transportImporter.importCluster(request(), clientSecret: "never-expose-this-secret")
            XCTFail("Expected transport failure")
        } catch {
            XCTAssertEqual(error as? AKSNativeClusterImportError, .transport)
            XCTAssertFalse(error.localizedDescription.contains("never-expose-this-secret"))
        }
    }

    func testCancellationIsPreservedAndStopsBeforeClusterRequest() async throws {
        let tokenHTTP = RecordingAKSNativeTokenHTTPClient { _ in throw CancellationError() }
        let clusterHTTP = RecordingAKSClusterHTTPClient { _ in
            XCTFail("Cluster request must not run after cancellation")
            return Self.clusterResponse(kubeConfig: "unused")
        }
        let importer = AKSNativeClusterImporter(httpClient: clusterHTTP, tokenHTTPClient: tokenHTTP)

        do {
            _ = try await importer.importCluster(request(), clientSecret: "synthetic-secret")
            XCTFail("Expected cancellation")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }
        let clusterRequestCount = await clusterHTTP.requestCount()
        XCTAssertEqual(clusterRequestCount, 0)
    }

    func testDefaultClusterTransportRejectsNonARMHostsBeforeNetwork() async throws {
        let transport = URLSessionAKSClusterHTTPClient()
        do {
            _ = try await transport.send(AKSClusterHTTPRequest(
                url: URL(string: "https://synthetic.example.invalid/credential")!,
                headers: [:]
            ))
            XCTFail("Expected fixed-host validation")
        } catch {
            XCTAssertEqual(error as? AKSNativeClusterImportError, .invalidEndpoint)
        }
    }

    private func request() throws -> AKSNativeClusterImportRequest {
        try AKSNativeClusterImportRequest(
            subscriptionID: subscriptionID,
            resourceGroup: "synthetic-group",
            clusterName: "synthetic-cluster",
            tenantID: tenantID,
            clientID: clientID
        )
    }

    private func generatedKubeConfig(tenantID: String? = nil) -> String {
        """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://synthetic-api.example.invalid
            certificate-authority-data: c3ludGhldGljLWNh
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
              command: kubelogin
              interactiveMode: IfAvailable
              args:
              - get-token
              - --environment
              - AzurePublicCloud
              - --server-id
              - \(serverID)
              - --client-id
              - \(generatedClientID)
              - --tenant-id
              - \(tenantID ?? self.tenantID)
              - --login
              - devicecode
        """
    }

    private static func tokenResponse() -> AKSServicePrincipalTokenHTTPResponse {
        AKSServicePrincipalTokenHTTPResponse(
            statusCode: 200,
            body: jsonData([
                "access_token": "synthetic-arm-token",
                "token_type": "Bearer",
                "expires_in": 3_600
            ])
        )
    }

    private static func clusterResponse(kubeConfig: String) -> AKSClusterHTTPResponse {
        AKSClusterHTTPResponse(
            statusCode: 200,
            body: jsonData([
                "kubeconfigs": [[
                    "name": "synthetic-user-credential",
                    "value": Data(kubeConfig.utf8).base64EncodedString()
                ]]
            ])
        )
    }

    private static func jsonData(_ value: Any) -> Data {
        try! JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
    }

    private static func formFields(_ body: Data) -> [String: String] {
        var components = URLComponents()
        components.percentEncodedQuery = String(decoding: body, as: UTF8.self)
        return Dictionary(uniqueKeysWithValues: (components.queryItems ?? []).map {
            ($0.name, $0.value ?? "")
        })
    }
}

private actor RecordingAKSNativeTokenHTTPClient: AKSServicePrincipalTokenHTTPClient {
    private let handler: @Sendable (
        AKSServicePrincipalTokenHTTPRequest
    ) async throws -> AKSServicePrincipalTokenHTTPResponse
    private var requests: [AKSServicePrincipalTokenHTTPRequest] = []

    init(handler: @escaping @Sendable (
        AKSServicePrincipalTokenHTTPRequest
    ) async throws -> AKSServicePrincipalTokenHTTPResponse) {
        self.handler = handler
    }

    func send(_ request: AKSServicePrincipalTokenHTTPRequest) async throws -> AKSServicePrincipalTokenHTTPResponse {
        requests.append(request)
        return try await handler(request)
    }

    func requestCount() -> Int { requests.count }
}

private actor RecordingAKSClusterHTTPClient: AKSClusterHTTPClient {
    private let handler: @Sendable (AKSClusterHTTPRequest) async throws -> AKSClusterHTTPResponse
    private var requests: [AKSClusterHTTPRequest] = []

    init(handler: @escaping @Sendable (AKSClusterHTTPRequest) async throws -> AKSClusterHTTPResponse) {
        self.handler = handler
    }

    func send(_ request: AKSClusterHTTPRequest) async throws -> AKSClusterHTTPResponse {
        requests.append(request)
        return try await handler(request)
    }

    func requestCount() -> Int { requests.count }
}

private enum SyntheticAKSNativeImportError: Error {
    case secret(String)
}
