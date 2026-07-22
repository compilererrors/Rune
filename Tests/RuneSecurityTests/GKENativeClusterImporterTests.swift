import Foundation
@testable import RuneSecurity
@preconcurrency import Security
import XCTest

final class GKENativeClusterImporterTests: XCTestCase {
    func testImportFetchesFixedGKEEndpointAndBuildsAnalyzerCompatibleKubeconfig() async throws {
        let fixture = try makeServiceAccountFixture()
        let oauthClient = RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        let certificateAuthority = Data("synthetic cluster CA".utf8).base64EncodedString()
        let clusterClient = RecordingGKEClusterHTTPClient(response: GKEClusterHTTPResponse(
            statusCode: 200,
            headers: ["Content-Type": "application/json"],
            body: try clusterResponse(
                endpoint: "cluster.example.invalid",
                certificateAuthority: certificateAuthority
            )
        ))
        let importer = GKENativeClusterImporter(
            httpClient: clusterClient,
            oauthHTTPClient: oauthClient
        )

        let result = try await importer.importCluster(
            GKENativeClusterImportRequest(
                projectID: "synthetic-project",
                location: "europe-west1-b",
                clusterName: "synthetic-cluster"
            ),
            serviceAccountJSON: fixture
        )

        XCTAssertEqual(result.sourceName, "gke_synthetic-project_europe-west1-b_synthetic-cluster.yaml")
        XCTAssertFalse(result.rawKubeConfig.contains("synthetic-access-token"))
        XCTAssertFalse(result.rawKubeConfig.contains("PRIVATE KEY"))
        XCTAssertFalse(result.rawKubeConfig.contains("rune-test@example.invalid"))

        let clusterRequests = await clusterClient.requests()
        let request = try XCTUnwrap(clusterRequests.first)
        XCTAssertEqual(request.method, "GET")
        XCTAssertEqual(request.url.scheme, "https")
        XCTAssertEqual(request.url.host, "container.googleapis.com")
        XCTAssertEqual(
            request.url.path,
            "/v1/projects/synthetic-project/locations/europe-west1-b/clusters/synthetic-cluster"
        )
        XCTAssertEqual(request.headers["Accept"], "application/json")
        XCTAssertEqual(request.headers["Authorization"], "Bearer synthetic-access-token")
        XCTAssertEqual(request.description, "GKEClusterHTTPRequest(<redacted>)")
        let fields = try XCTUnwrap(
            URLComponents(url: request.url, resolvingAgainstBaseURL: false)?
                .queryItems?
                .first(where: { $0.name == "fields" })?
                .value
        )
        XCTAssertEqual(
            fields,
            "name,location,endpoint,masterAuth/clusterCaCertificate,controlPlaneEndpointsConfig/dnsEndpointConfig/endpoint"
        )

        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(raw: result.rawKubeConfig)
        XCTAssertEqual(analysis.currentContext, "gke_synthetic-project_europe-west1-b_synthetic-cluster")
        let context = try XCTUnwrap(analysis.contexts.first)
        XCTAssertEqual(context.provider, .googleGKE)
        XCTAssertEqual(context.cluster.server, "https://cluster.example.invalid")
        XCTAssertEqual(context.cluster.certificateAuthorityData, certificateAuthority)
        XCTAssertEqual(context.exec?.command, "gke-gcloud-auth-plugin")
        XCTAssertEqual(context.exec?.arguments, ["--use_application_default_credentials"])
        XCTAssertEqual(context.exec?.provideClusterInfo, true)
        XCTAssertEqual(context.exec?.interactiveMode, "Never")
        XCTAssertNotNil(context.credentialRequest)

        let oauthRequestCount = await oauthClient.requestCount()
        let clusterRequestCount = await clusterClient.requestCount()
        XCTAssertEqual(oauthRequestCount, 1)
        XCTAssertEqual(clusterRequestCount, 1)
    }

    func testDNSOnlyClusterUsesSystemTrustWithoutClusterCertificateAuthority() async throws {
        let dnsEndpoint = "synthetic-id.europe-west1.gke.goog"
        let clusterClient = RecordingGKEClusterHTTPClient(response: GKEClusterHTTPResponse(
            statusCode: 200,
            body: try JSONSerialization.data(withJSONObject: [
                "name": "synthetic-cluster",
                "location": "europe-west1",
                "controlPlaneEndpointsConfig": [
                    "dnsEndpointConfig": ["endpoint": dnsEndpoint]
                ]
            ], options: [.sortedKeys])
        ))
        let importer = GKENativeClusterImporter(
            httpClient: clusterClient,
            oauthHTTPClient: RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        )

        let result = try await importer.importCluster(
            GKENativeClusterImportRequest(
                projectID: "synthetic-project",
                location: "europe-west1",
                clusterName: "synthetic-cluster"
            ),
            serviceAccountJSON: try makeServiceAccountFixture()
        )

        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(raw: result.rawKubeConfig)
        let context = try XCTUnwrap(analysis.contexts.first)
        XCTAssertEqual(context.cluster.server, "https://\(dnsEndpoint)")
        XCTAssertNil(context.cluster.certificateAuthorityData)
        XCTAssertFalse(result.rawKubeConfig.contains("certificate-authority-data"))
        XCTAssertEqual(context.provider, .googleGKE)
        XCTAssertNotNil(context.credentialRequest)
    }

    func testImportTrimsResourceIdentifiersBeforeBuildingRequest() async throws {
        let oauthClient = RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        let clusterClient = RecordingGKEClusterHTTPClient(response: try successfulClusterHTTPResponse())
        let importer = GKENativeClusterImporter(
            httpClient: clusterClient,
            oauthHTTPClient: oauthClient
        )

        _ = try await importer.importCluster(
            GKENativeClusterImportRequest(
                projectID: " synthetic-project ",
                location: " europe-west1-b ",
                clusterName: " synthetic-cluster "
            ),
            serviceAccountJSON: try makeServiceAccountFixture()
        )

        let requests = await clusterClient.requests()
        let request = try XCTUnwrap(requests.first)
        XCTAssertEqual(
            request.url.path,
            "/v1/projects/synthetic-project/locations/europe-west1-b/clusters/synthetic-cluster"
        )
    }

    func testInvalidResourceIdentifiersFailBeforeOAuthOrClusterRequests() async throws {
        let oauthClient = RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        let clusterClient = RecordingGKEClusterHTTPClient(response: try successfulClusterHTTPResponse())
        let importer = GKENativeClusterImporter(
            httpClient: clusterClient,
            oauthHTTPClient: oauthClient
        )
        let fixture = try makeServiceAccountFixture()

        for request in [
            GKENativeClusterImportRequest(projectID: "", location: "europe-west1-b", clusterName: "synthetic-cluster"),
            GKENativeClusterImportRequest(projectID: "synthetic/project", location: "europe-west1-b", clusterName: "synthetic-cluster"),
            GKENativeClusterImportRequest(projectID: "synthetic-project", location: "../location", clusterName: "synthetic-cluster"),
            GKENativeClusterImportRequest(projectID: "synthetic-project", location: "europe-west1-b", clusterName: "cluster?query")
        ] {
            do {
                _ = try await importer.importCluster(request, serviceAccountJSON: fixture)
                XCTFail("Expected invalid resource identifier")
            } catch let error as GKENativeClusterImportError {
                switch error {
                case .missingRequiredField, .invalidResourceIdentifier:
                    break
                default:
                    XCTFail("Unexpected error: \(error)")
                }
            }
        }

        let oauthRequestCount = await oauthClient.requestCount()
        let clusterRequestCount = await clusterClient.requestCount()
        XCTAssertEqual(oauthRequestCount, 0)
        XCTAssertEqual(clusterRequestCount, 0)
    }

    func testForbiddenClusterResponseReportsAuthorizationWithoutExposingProviderBody() async throws {
        let sensitiveDiagnostic = "synthetic-sensitive-diagnostic"
        let clusterClient = RecordingGKEClusterHTTPClient(response: GKEClusterHTTPResponse(
            statusCode: 403,
            body: Data("{\"error\":\"\(sensitiveDiagnostic)\"}".utf8)
        ))
        let importer = GKENativeClusterImporter(
            httpClient: clusterClient,
            oauthHTTPClient: RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        )

        do {
            _ = try await importer.importCluster(validRequest(), serviceAccountJSON: try makeServiceAccountFixture())
            XCTFail("Expected request rejection")
        } catch {
            XCTAssertEqual(error as? GKENativeClusterImportError, .requestRejected(403))
            XCTAssertFalse(error.localizedDescription.contains(sensitiveDiagnostic))
        }
    }

    func testUnauthorizedClusterResponseReportsAuthenticationWithoutExposingProviderBody() async throws {
        let sensitiveDiagnostic = "synthetic-sensitive-authentication-diagnostic"
        let clusterClient = RecordingGKEClusterHTTPClient(response: GKEClusterHTTPResponse(
            statusCode: 401,
            body: Data("{\"error\":\"\(sensitiveDiagnostic)\"}".utf8)
        ))
        let importer = GKENativeClusterImporter(
            httpClient: clusterClient,
            oauthHTTPClient: RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        )

        do {
            _ = try await importer.importCluster(validRequest(), serviceAccountJSON: try makeServiceAccountFixture())
            XCTFail("Expected authentication failure")
        } catch {
            XCTAssertEqual(error as? GKENativeClusterImportError, .authenticationFailed)
            XCTAssertFalse(error.localizedDescription.contains(sensitiveDiagnostic))
        }
    }

    func testMalformedEndpointAndCertificateAuthorityFailClosed() async throws {
        let fixture = try makeServiceAccountFixture()
        let invalidEndpoint = RecordingGKEClusterHTTPClient(response: GKEClusterHTTPResponse(
            statusCode: 200,
            body: try clusterResponse(
                endpoint: "https://redirect.example.invalid/path",
                certificateAuthority: Data("synthetic CA".utf8).base64EncodedString()
            )
        ))
        let invalidEndpointImporter = GKENativeClusterImporter(
            httpClient: invalidEndpoint,
            oauthHTTPClient: RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        )
        do {
            _ = try await invalidEndpointImporter.importCluster(validRequest(), serviceAccountJSON: fixture)
            XCTFail("Expected endpoint validation failure")
        } catch {
            XCTAssertEqual(error as? GKENativeClusterImportError, .invalidClusterEndpoint)
        }

        let invalidCA = RecordingGKEClusterHTTPClient(response: GKEClusterHTTPResponse(
            statusCode: 200,
            body: try clusterResponse(
                endpoint: "cluster.example.invalid",
                certificateAuthority: "not-base64%%%"
            )
        ))
        let invalidCAImporter = GKENativeClusterImporter(
            httpClient: invalidCA,
            oauthHTTPClient: RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        )
        do {
            _ = try await invalidCAImporter.importCluster(validRequest(), serviceAccountJSON: fixture)
            XCTFail("Expected certificate-authority validation failure")
        } catch {
            XCTAssertEqual(error as? GKENativeClusterImportError, .invalidCertificateAuthority)
        }
    }

    func testOversizedClusterResponseIsRejectedBeforeDecoding() async throws {
        let importer = GKENativeClusterImporter(
            httpClient: RecordingGKEClusterHTTPClient(response: GKEClusterHTTPResponse(
                statusCode: 200,
                body: Data(repeating: 0x41, count: 1_048_577)
            )),
            oauthHTTPClient: RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        )

        do {
            _ = try await importer.importCluster(validRequest(), serviceAccountJSON: try makeServiceAccountFixture())
            XCTFail("Expected response-size rejection")
        } catch {
            XCTAssertEqual(error as? GKENativeClusterImportError, .responseTooLarge)
        }
    }

    func testUnknownClusterTransportFailureIsSanitized() async throws {
        let importer = GKENativeClusterImporter(
            httpClient: FailingGKEClusterHTTPClient(),
            oauthHTTPClient: RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        )

        do {
            _ = try await importer.importCluster(validRequest(), serviceAccountJSON: try makeServiceAccountFixture())
            XCTFail("Expected transport failure")
        } catch {
            XCTAssertEqual(error as? GKENativeClusterImportError, .networkFailure)
            XCTAssertFalse(error.localizedDescription.contains("synthetic-sensitive-transport-detail"))
        }
    }

    func testClusterTransportCancellationIsPreserved() async throws {
        let importer = GKENativeClusterImporter(
            httpClient: CancellingGKEClusterHTTPClient(),
            oauthHTTPClient: RecordingGKEOAuthHTTPClient(response: oauthTokenResponse())
        )

        do {
            _ = try await importer.importCluster(validRequest(), serviceAccountJSON: try makeServiceAccountFixture())
            XCTFail("Expected cancellation")
        } catch is CancellationError {
            // Expected.
        } catch {
            XCTFail("Expected CancellationError, got \(error)")
        }
    }

    func testOAuthTransportCancellationIsPreserved() async throws {
        let importer = GKENativeClusterImporter(
            httpClient: RecordingGKEClusterHTTPClient(response: try successfulClusterHTTPResponse()),
            oauthHTTPClient: CancellingGKEOAuthHTTPClient()
        )

        do {
            _ = try await importer.importCluster(validRequest(), serviceAccountJSON: try makeServiceAccountFixture())
            XCTFail("Expected cancellation")
        } catch is CancellationError {
            // Expected.
        } catch {
            XCTFail("Expected CancellationError, got \(error)")
        }
    }

    func testCallerCancellationStopsInFlightOAuthPromptly() async throws {
        let oauthClient = HangingGKEOAuthHTTPClient()
        let importer = GKENativeClusterImporter(
            httpClient: RecordingGKEClusterHTTPClient(response: try successfulClusterHTTPResponse()),
            oauthHTTPClient: oauthClient
        )
        let fixture = try makeServiceAccountFixture()
        let request = validRequest()
        let task = Task {
            try await importer.importCluster(request, serviceAccountJSON: fixture)
        }
        let deadline = Date().addingTimeInterval(2)
        while !(await oauthClient.hasStarted()) && Date() < deadline {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        let didStart = await oauthClient.hasStarted()
        XCTAssertTrue(didStart)

        let cancelledAt = Date()
        task.cancel()
        do {
            _ = try await task.value
            XCTFail("Expected caller cancellation")
        } catch is CancellationError {
            XCTAssertLessThan(Date().timeIntervalSince(cancelledAt), 1)
        } catch {
            XCTFail("Expected CancellationError, got \(error)")
        }
    }

    func testProductionTransportRejectsNonGoogleOriginAndNonGETWithoutNetworkAccess() async {
        let client = GKEClusterURLSessionHTTPClient()
        let invalidOrigin = GKEClusterHTTPRequest(
            url: URL(string: "https://container.example.invalid/v1/projects/synthetic/locations/test/clusters/test")!,
            method: "GET",
            headers: [:]
        )
        do {
            _ = try await client.send(invalidOrigin)
            XCTFail("Expected fixed-origin rejection")
        } catch {
            XCTAssertEqual(error as? GKENativeClusterImportError, .invalidHTTPResponse)
        }

        let invalidMethod = GKEClusterHTTPRequest(
            url: URL(string: "https://container.googleapis.com/v1/projects/synthetic/locations/test/clusters/test")!,
            method: "POST",
            headers: [:]
        )
        do {
            _ = try await client.send(invalidMethod)
            XCTFail("Expected method rejection")
        } catch {
            XCTAssertEqual(error as? GKENativeClusterImportError, .invalidHTTPResponse)
        }
    }

    private func validRequest() -> GKENativeClusterImportRequest {
        GKENativeClusterImportRequest(
            projectID: "synthetic-project",
            location: "europe-west1-b",
            clusterName: "synthetic-cluster"
        )
    }

    private func successfulClusterHTTPResponse() throws -> GKEClusterHTTPResponse {
        GKEClusterHTTPResponse(
            statusCode: 200,
            body: try clusterResponse(
                endpoint: "cluster.example.invalid",
                certificateAuthority: Data("synthetic cluster CA".utf8).base64EncodedString()
            )
        )
    }

    private func clusterResponse(endpoint: String, certificateAuthority: String) throws -> Data {
        try JSONSerialization.data(withJSONObject: [
            "name": "synthetic-cluster",
            "location": "europe-west1-b",
            "endpoint": endpoint,
            "masterAuth": [
                "clusterCaCertificate": certificateAuthority
            ]
        ], options: [.sortedKeys])
    }

    private func oauthTokenResponse() -> GCPServiceAccountHTTPResponse {
        GCPServiceAccountHTTPResponse(
            statusCode: 200,
            headers: ["Content-Type": "application/json"],
            body: Data("{\"access_token\":\"synthetic-access-token\",\"expires_in\":3600,\"token_type\":\"Bearer\"}".utf8)
        )
    }

    private func makeServiceAccountFixture() throws -> Data {
        let attributes: [CFString: Any] = [
            kSecAttrKeyType: kSecAttrKeyTypeRSA,
            kSecAttrKeySizeInBits: 2_048,
            kSecAttrIsPermanent: false
        ]
        var keyError: Unmanaged<CFError>?
        let privateKey = try XCTUnwrap(SecKeyCreateRandomKey(attributes as CFDictionary, &keyError))
        var exportError: Unmanaged<CFError>?
        let pkcs1 = try XCTUnwrap(SecKeyCopyExternalRepresentation(privateKey, &exportError) as Data?)
        let pkcs8 = derElement(tag: 0x30, value:
            Data([0x02, 0x01, 0x00])
            + Data([0x30, 0x0D, 0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01, 0x05, 0x00])
            + derElement(tag: 0x04, value: pkcs1)
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
            "project_id": "synthetic-project",
            "private_key_id": "synthetic-key-id",
            "private_key": pem,
            "client_email": "rune-test@example.invalid",
            "client_id": "000000000000000000000",
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
}

private actor RecordingGKEOAuthHTTPClient: GCPServiceAccountHTTPClient {
    private let response: GCPServiceAccountHTTPResponse
    private var recordedRequests: [GCPServiceAccountHTTPRequest] = []

    init(response: GCPServiceAccountHTTPResponse) {
        self.response = response
    }

    func send(_ request: GCPServiceAccountHTTPRequest) async throws -> GCPServiceAccountHTTPResponse {
        recordedRequests.append(request)
        return response
    }

    func requestCount() -> Int { recordedRequests.count }
}

private struct CancellingGKEOAuthHTTPClient: GCPServiceAccountHTTPClient {
    func send(_: GCPServiceAccountHTTPRequest) async throws -> GCPServiceAccountHTTPResponse {
        throw CancellationError()
    }
}

private actor HangingGKEOAuthHTTPClient: GCPServiceAccountHTTPClient {
    private var started = false

    func send(_: GCPServiceAccountHTTPRequest) async throws -> GCPServiceAccountHTTPResponse {
        started = true
        try await Task.sleep(nanoseconds: 30_000_000_000)
        throw CancellationError()
    }

    func hasStarted() -> Bool { started }
}

private actor RecordingGKEClusterHTTPClient: GKEClusterHTTPClient {
    private let response: GKEClusterHTTPResponse
    private var recordedRequests: [GKEClusterHTTPRequest] = []

    init(response: GKEClusterHTTPResponse) {
        self.response = response
    }

    func send(_ request: GKEClusterHTTPRequest) async throws -> GKEClusterHTTPResponse {
        recordedRequests.append(request)
        return response
    }

    func requests() -> [GKEClusterHTTPRequest] { recordedRequests }
    func requestCount() -> Int { recordedRequests.count }
}

private struct FailingGKEClusterHTTPClient: GKEClusterHTTPClient {
    func send(_: GKEClusterHTTPRequest) async throws -> GKEClusterHTTPResponse {
        throw SyntheticGKETransportError(message: "synthetic-sensitive-transport-detail")
    }

    private struct SyntheticGKETransportError: Error {
        let message: String
    }
}

private struct CancellingGKEClusterHTTPClient: GKEClusterHTTPClient {
    func send(_: GKEClusterHTTPRequest) async throws -> GKEClusterHTTPResponse {
        throw CancellationError()
    }
}
