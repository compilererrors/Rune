import Foundation
@testable import RuneSecurity
import XCTest

final class AWSEKSClusterImporterTests: XCTestCase {
    private let accessKeyID = "AKIASYNTHETIC000001"
    private let secretAccessKey = "syntheticSecretKeyForOfflineGoldenTestOnly0001"
    private let clusterName = "synthetic-cluster"
    private let region = "eu-north-1"

    func testDescribeClusterUsesFixedRegionalEndpointAndSigV4GoldenSignature() async throws {
        let client = RecordingAWSEKSClusterHTTPClient(response: try successfulResponse())
        let result = try await importer(client: client).importCluster(
            request(),
            credentials: credentials()
        )

        let requests = await client.requests()
        let captured = try XCTUnwrap(requests.first)
        XCTAssertEqual(requests.count, 1)
        XCTAssertEqual(captured.method, "GET")
        XCTAssertEqual(
            captured.url.absoluteString,
            "https://eks.eu-north-1.amazonaws.com/clusters/synthetic-cluster"
        )
        XCTAssertEqual(captured.headers["Accept"], "application/json")
        XCTAssertEqual(captured.headers["Host"], "eks.eu-north-1.amazonaws.com")
        XCTAssertEqual(captured.headers["X-Amz-Date"], "20250102T030405Z")
        XCTAssertEqual(
            captured.headers["Authorization"],
            "AWS4-HMAC-SHA256 Credential=AKIASYNTHETIC000001/20250102/eu-north-1/eks/aws4_request, "
                + "SignedHeaders=host;x-amz-date, "
                + "Signature=9d7949dd113d934a6901d90a3946becda55c8844096a7762587034b7ea8d19e4"
        )
        XCTAssertEqual(result.sourceName, "eks-synthetic-cluster.yaml")
        XCTAssertFalse(captured.description.contains(accessKeyID))
        XCTAssertFalse(captured.description.contains(secretAccessKey))
        XCTAssertFalse(result.rawKubeConfig.contains(accessKeyID))
        XCTAssertFalse(result.rawKubeConfig.contains(secretAccessKey))
    }

    func testSessionCredentialHeaderIsIncludedInSignature() async throws {
        let sessionToken = "synthetic/session+token=value"
        let client = RecordingAWSEKSClusterHTTPClient(response: try successfulResponse())
        _ = try await importer(client: client).importCluster(
            request(),
            credentials: AWSEKSCredentials(
                accessKeyID: accessKeyID,
                secretAccessKey: secretAccessKey,
                sessionToken: sessionToken
            )
        )

        let requests = await client.requests()
        let captured = try XCTUnwrap(requests.first)
        XCTAssertEqual(captured.headers["X-Amz-Security-Token"], sessionToken)
        XCTAssertTrue(captured.headers["Authorization"]?.contains(
            "SignedHeaders=host;x-amz-date;x-amz-security-token"
        ) == true)
        XCTAssertFalse(captured.headers["Authorization"]?.contains(sessionToken) == true)
    }

    func testGeneratedKubeConfigIsAnalyzerCompatibleWithNativeEKSAuth() async throws {
        let certificateAuthority = Data("synthetic-ca".utf8).base64EncodedString()
        let endpoint = "https://synthetic-cluster.invalid"
        let client = RecordingAWSEKSClusterHTTPClient(response: try successfulResponse(
            endpoint: endpoint,
            certificateAuthority: certificateAuthority
        ))

        let result = try await importer(client: client).importCluster(
            request(),
            credentials: credentials()
        )
        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(raw: result.rawKubeConfig)
        let context = try XCTUnwrap(analysis.contexts.first)
        let credentialRequest = try XCTUnwrap(context.credentialRequest)

        XCTAssertEqual(analysis.contexts.count, 1)
        XCTAssertTrue(analysis.issues.isEmpty)
        XCTAssertEqual(analysis.currentContext, syntheticARN())
        XCTAssertEqual(context.cluster.server, endpoint)
        XCTAssertEqual(context.cluster.certificateAuthorityData, certificateAuthority)
        XCTAssertEqual(credentialRequest.provider, .awsEKS)
        XCTAssertEqual(context.exec?.apiVersion, "client.authentication.k8s.io/v1")
        XCTAssertEqual(context.exec?.interactiveMode, "Never")
        XCTAssertEqual(context.exec?.optionValue(for: "--region"), region)
        XCTAssertEqual(context.exec?.optionValue(for: "--cluster-name"), clusterName)
    }

    func testUpdatingClusterIsAccepted() async throws {
        let client = RecordingAWSEKSClusterHTTPClient(response: try successfulResponse(status: "UPDATING"))

        let result = try await importer(client: client).importCluster(
            request(),
            credentials: credentials()
        )

        XCTAssertTrue(result.rawKubeConfig.contains("--cluster-name"))
    }

    func testLocalClusterOnOutpostsUsesClusterIDLikeAWSCLI() async throws {
        let client = RecordingAWSEKSClusterHTTPClient(response: try successfulResponse(
            clusterID: "synthetic-outpost-cluster-id",
            outpostConfig: [:]
        ))

        let result = try await importer(client: client).importCluster(
            request(),
            credentials: credentials()
        )
        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(raw: result.rawKubeConfig)
        let exec = try XCTUnwrap(analysis.contexts.first?.exec)

        XCTAssertEqual(exec.optionValue(for: "--cluster-id"), "synthetic-outpost-cluster-id")
        XCTAssertNil(exec.optionValue(for: "--cluster-name"))
    }

    func testRegionalOriginsAndARNPartitionsCoverCommercialGovCloudAndChina() async throws {
        let cases = [
            ("us-west-2", "aws", "eks.us-west-2.amazonaws.com"),
            ("us-gov-west-1", "aws-us-gov", "eks.us-gov-west-1.amazonaws.com"),
            ("cn-north-1", "aws-cn", "eks.cn-north-1.amazonaws.com.cn")
        ]

        for (region, partition, expectedHost) in cases {
            let arn = syntheticARN(partition: partition, region: region)
            let client = RecordingAWSEKSClusterHTTPClient(response: try successfulResponse(
                arn: arn,
                region: region
            ))
            _ = try await importer(client: client).importCluster(
                AWSEKSClusterImportRequest(clusterName: clusterName, region: region),
                credentials: credentials()
            )

            let requests = await client.requests()
            XCTAssertEqual(requests.first?.url.host, expectedHost)
            XCTAssertTrue(requests.first?.headers["Authorization"]?.contains(
                "/\(region)/eks/aws4_request"
            ) == true)
        }
    }

    func testInvalidNamesRegionsAndUnsupportedPartitionsFailBeforeNetwork() async throws {
        let cases: [(AWSEKSClusterImportRequest, AWSEKSClusterImportError)] = [
            (AWSEKSClusterImportRequest(clusterName: "", region: region), .invalidClusterName),
            (AWSEKSClusterImportRequest(clusterName: "invalid/name", region: region), .invalidClusterName),
            (AWSEKSClusterImportRequest(clusterName: clusterName, region: "invalid"), .invalidRegion),
            (
                AWSEKSClusterImportRequest(clusterName: clusterName, region: "us-iso-east-1"),
                .unsupportedPartition
            )
        ]

        for (request, expected) in cases {
            let client = RecordingAWSEKSClusterHTTPClient(response: try successfulResponse())
            do {
                _ = try await importer(client: client).importCluster(request, credentials: credentials())
                XCTFail("Expected validation failure")
            } catch {
                XCTAssertEqual(error as? AWSEKSClusterImportError, expected)
            }
            let requestCount = await client.requestCount()
            XCTAssertEqual(requestCount, 0)
        }
    }

    func testDescribeClusterResponseIsBoundToRequestedClusterRegionAndPartition() async throws {
        let invalidResponses = [
            try successfulResponse(name: "different-synthetic-cluster"),
            try successfulResponse(arn: syntheticARN(region: "eu-west-1")),
            try successfulResponse(arn: syntheticARN(partition: "aws-cn"))
        ]

        for response in invalidResponses {
            let error = await capturedError(response: response)
            XCTAssertEqual(error as? AWSEKSClusterImportError, .invalidClusterResponse)
        }
    }

    func testClusterStatusEndpointAndCertificateAuthorityAreStrictlyValidated() async throws {
        let cases: [(AWSEKSClusterHTTPResponse, AWSEKSClusterImportError)] = [
            (try successfulResponse(status: "CREATING"), .clusterNotReady),
            (try successfulResponse(endpoint: "http://synthetic-cluster.invalid"), .invalidClusterEndpoint),
            (try successfulResponse(endpoint: "https://user@synthetic-cluster.invalid"), .invalidClusterEndpoint),
            (try successfulResponse(certificateAuthority: "not-base64"), .invalidCertificateAuthority),
            (try successfulResponse(certificateAuthority: ""), .invalidCertificateAuthority)
        ]

        for (response, expected) in cases {
            let error = await capturedError(response: response)
            XCTAssertEqual(error as? AWSEKSClusterImportError, expected)
        }
    }

    func testHTTPFailuresResponseBoundsAndDiagnosticsAreSanitized() async throws {
        let privateDiagnostic = "synthetic-sensitive-provider-diagnostic"
        let denied = AWSEKSClusterHTTPResponse(
            statusCode: 403,
            body: Data("{\"message\":\"\(privateDiagnostic)\"}".utf8)
        )
        let deniedError = await capturedError(response: denied)
        XCTAssertEqual(deniedError as? AWSEKSClusterImportError, .accessDenied)
        XCTAssertFalse(deniedError.localizedDescription.contains(privateDiagnostic))

        let oversized = AWSEKSClusterHTTPResponse(
            statusCode: 200,
            body: Data(repeating: 65, count: 1_048_577)
        )
        let oversizedError = await capturedError(response: oversized)
        XCTAssertEqual(oversizedError as? AWSEKSClusterImportError, .responseTooLarge)

        let failingClient = FailingAWSEKSClusterHTTPClient()
        do {
            _ = try await importer(client: failingClient).importCluster(request(), credentials: credentials())
            XCTFail("Expected network failure")
        } catch {
            XCTAssertEqual(error as? AWSEKSClusterImportError, .networkFailure)
            XCTAssertFalse(error.localizedDescription.contains(privateDiagnostic))
            XCTAssertFalse(error.localizedDescription.contains(accessKeyID))
            XCTAssertFalse(error.localizedDescription.contains(secretAccessKey))
        }
    }

    func testCancellationPropagatesWithoutBeingMappedToNetworkFailure() async throws {
        let client = CancellingAWSEKSClusterHTTPClient()

        do {
            _ = try await importer(client: client).importCluster(request(), credentials: credentials())
            XCTFail("Expected cancellation")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }
    }

    func testExpiredCredentialsFailBeforeNetwork() async throws {
        let date = try fixedDate()
        let client = RecordingAWSEKSClusterHTTPClient(response: try successfulResponse())
        let expiredCredentials = try AWSEKSCredentials(
            accessKeyID: accessKeyID,
            secretAccessKey: secretAccessKey,
            sessionToken: "synthetic-session-token",
            expiration: date.addingTimeInterval(20)
        )

        do {
            _ = try await importer(client: client).importCluster(request(), credentials: expiredCredentials)
            XCTFail("Expected expired credentials")
        } catch {
            XCTAssertEqual(error as? AWSEKSNativeAuthError, .expiredCredentials)
        }
        let requestCount = await client.requestCount()
        XCTAssertEqual(requestCount, 0)
    }

    private func importer(client: any AWSEKSClusterHTTPClient) throws -> AWSEKSClusterImporter {
        let date = try fixedDate()
        return AWSEKSClusterImporter(httpClient: client, signingDate: { date })
    }

    private func request() -> AWSEKSClusterImportRequest {
        AWSEKSClusterImportRequest(clusterName: clusterName, region: region)
    }

    private func credentials() throws -> AWSEKSCredentials {
        try AWSEKSCredentials(accessKeyID: accessKeyID, secretAccessKey: secretAccessKey)
    }

    private func fixedDate() throws -> Date {
        let components = DateComponents(
            calendar: Calendar(identifier: .gregorian),
            timeZone: TimeZone(secondsFromGMT: 0),
            year: 2025,
            month: 1,
            day: 2,
            hour: 3,
            minute: 4,
            second: 5
        )
        return try XCTUnwrap(components.date)
    }

    private func syntheticARN(
        partition: String = "aws",
        region: String? = nil,
        name: String? = nil
    ) -> String {
        "arn:\(partition):eks:\(region ?? self.region):000000000000:cluster/\(name ?? clusterName)"
    }

    private func successfulResponse(
        name: String? = nil,
        arn: String? = nil,
        region: String? = nil,
        status: String = "ACTIVE",
        endpoint: String = "https://synthetic-cluster.invalid",
        certificateAuthority: String? = nil,
        clusterID: String? = nil,
        outpostConfig: [String: Any]? = nil
    ) throws -> AWSEKSClusterHTTPResponse {
        let resolvedName = name ?? clusterName
        let resolvedRegion = region ?? self.region
        var cluster: [String: Any] = [
            "name": resolvedName,
            "arn": arn ?? syntheticARN(region: resolvedRegion, name: resolvedName),
            "status": status,
            "endpoint": endpoint,
            "certificateAuthority": [
                "data": certificateAuthority ?? Data("synthetic-ca".utf8).base64EncodedString()
            ]
        ]
        if let clusterID {
            cluster["id"] = clusterID
        }
        if let outpostConfig {
            cluster["outpostConfig"] = outpostConfig
        }
        return AWSEKSClusterHTTPResponse(
            statusCode: 200,
            body: try JSONSerialization.data(withJSONObject: ["cluster": cluster])
        )
    }

    private func capturedError(response: AWSEKSClusterHTTPResponse) async -> Error {
        let client = RecordingAWSEKSClusterHTTPClient(response: response)
        do {
            _ = try await importer(client: client).importCluster(request(), credentials: credentials())
            return SyntheticMissingError()
        } catch {
            return error
        }
    }
}

private actor RecordingAWSEKSClusterHTTPClient: AWSEKSClusterHTTPClient {
    private let response: AWSEKSClusterHTTPResponse
    private var recordedRequests: [AWSEKSClusterHTTPRequest] = []

    init(response: AWSEKSClusterHTTPResponse) {
        self.response = response
    }

    func send(_ request: AWSEKSClusterHTTPRequest) async throws -> AWSEKSClusterHTTPResponse {
        recordedRequests.append(request)
        return response
    }

    func requests() -> [AWSEKSClusterHTTPRequest] { recordedRequests }
    func requestCount() -> Int { recordedRequests.count }
}

private struct FailingAWSEKSClusterHTTPClient: AWSEKSClusterHTTPClient {
    func send(_: AWSEKSClusterHTTPRequest) async throws -> AWSEKSClusterHTTPResponse {
        throw SyntheticNetworkError(diagnostic: "synthetic-sensitive-provider-diagnostic")
    }

    private struct SyntheticNetworkError: Error, Sendable {
        let diagnostic: String
    }
}

private struct CancellingAWSEKSClusterHTTPClient: AWSEKSClusterHTTPClient {
    func send(_: AWSEKSClusterHTTPRequest) async throws -> AWSEKSClusterHTTPResponse {
        throw CancellationError()
    }
}

private struct SyntheticMissingError: Error, Sendable {}
