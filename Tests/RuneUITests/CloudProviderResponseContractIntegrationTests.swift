import Foundation
import Network
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneSecurity

/// Contract coverage for the provider-CLI import boundary.
///
/// The loopback server mirrors only the documented response fields consumed by
/// provider CLIs when they create kubeconfig. It deliberately does not replace
/// the opt-in live-cloud test in `CloudKubeConfigImporterTests`.
final class CloudProviderResponseContractIntegrationTests: XCTestCase {
    func testAKSDocumentedCredentialResponseImportsAndReachesLoopbackCluster() async throws {
        try await verifyImport(
            request: CloudKubeConfigImportRequest(
                provider: .aks,
                clusterName: "synthetic-aks",
                resourceGroup: "synthetic-group",
                profileOrSubscription: "00000000-0000-4000-8000-000000000000"
            ),
            expectedExecutable: "az",
            expectedArgumentsBeforeTarget: [
                "aks", "get-credentials",
                "--resource-group", "synthetic-group",
                "--name", "synthetic-aks",
                "--overwrite-existing"
            ],
            expectedProviderRequest: "POST /subscriptions/00000000-0000-4000-8000-000000000000/resourceGroups/synthetic-group/providers/Microsoft.ContainerService/managedClusters/synthetic-aks/listClusterUserCredential?api-version=2026-03-01 HTTP/1.1"
        )
    }

    func testEKSDocumentedDescribeClusterResponseImportsAndReachesLoopbackCluster() async throws {
        try await verifyImport(
            request: CloudKubeConfigImportRequest(
                provider: .eks,
                clusterName: "synthetic-eks",
                regionOrLocation: "eu-north-1",
                profileOrSubscription: "synthetic-profile",
                roleARN: "arn:aws:iam::000000000000:role/synthetic"
            ),
            expectedExecutable: "aws",
            expectedArgumentsBeforeTarget: [
                "eks", "update-kubeconfig",
                "--region", "eu-north-1",
                "--name", "synthetic-eks"
            ],
            expectedProviderRequest: "GET /clusters/synthetic-eks HTTP/1.1"
        )
    }

    func testGKEDocumentedClusterResponseImportsAndReachesLoopbackCluster() async throws {
        try await verifyImport(
            request: CloudKubeConfigImportRequest(
                provider: .gke,
                clusterName: "synthetic-gke",
                regionOrLocation: "europe-north1",
                projectID: "synthetic-project"
            ),
            expectedExecutable: "gcloud",
            expectedArgumentsBeforeTarget: [
                "container", "clusters", "get-credentials",
                "synthetic-gke",
                "--location", "europe-north1",
                "--project", "synthetic-project"
            ],
            expectedProviderRequest: "GET /v1/projects/synthetic-project/locations/europe-north1/clusters/synthetic-gke HTTP/1.1"
        )
    }

    func testAKSEmptyCredentialFailsClosedWithoutWritingKubeconfig() async throws {
        try await verifyRejectedImport(
            request: CloudKubeConfigImportRequest(
                provider: .aks,
                clusterName: "synthetic-aks",
                resourceGroup: "synthetic-group",
                profileOrSubscription: "00000000-0000-4000-8000-000000000000"
            ),
            responseMode: .aksEmptyCredential,
            expectedErrorCode: .cannotParseResponse
        )
    }

    func testAKSInvalidBase64CredentialFailsClosedWithoutWritingKubeconfig() async throws {
        try await verifyRejectedImport(
            request: CloudKubeConfigImportRequest(
                provider: .aks,
                clusterName: "synthetic-aks",
                resourceGroup: "synthetic-group",
                profileOrSubscription: "00000000-0000-4000-8000-000000000000"
            ),
            responseMode: .aksInvalidBase64Credential,
            expectedErrorCode: .cannotParseResponse
        )
    }

    func testEKSNonActiveClusterFailsClosedWithoutWritingKubeconfig() async throws {
        try await verifyRejectedImport(
            request: CloudKubeConfigImportRequest(
                provider: .eks,
                clusterName: "synthetic-eks",
                regionOrLocation: "eu-north-1"
            ),
            responseMode: .eksNonActive,
            expectedErrorCode: .cannotParseResponse
        )
    }

    func testEKSInvalidCertificateAuthorityFailsClosedWithoutWritingKubeconfig() async throws {
        try await verifyRejectedImport(
            request: CloudKubeConfigImportRequest(
                provider: .eks,
                clusterName: "synthetic-eks",
                regionOrLocation: "eu-north-1"
            ),
            responseMode: .eksInvalidCertificateAuthority,
            expectedErrorCode: .cannotParseResponse
        )
    }

    func testGKEInvalidCertificateAuthorityFailsClosedWithoutWritingKubeconfig() async throws {
        try await verifyRejectedImport(
            request: CloudKubeConfigImportRequest(
                provider: .gke,
                clusterName: "synthetic-gke",
                regionOrLocation: "europe-north1",
                projectID: "synthetic-project"
            ),
            responseMode: .gkeInvalidCertificateAuthority,
            expectedErrorCode: .cannotParseResponse
        )
    }

    func testGKENotFoundFailsClosedWithoutWritingKubeconfig() async throws {
        try await verifyRejectedImport(
            request: CloudKubeConfigImportRequest(
                provider: .gke,
                clusterName: "synthetic-gke",
                regionOrLocation: "europe-north1",
                projectID: "synthetic-project"
            ),
            responseMode: .gkeNotFound,
            expectedErrorCode: .badServerResponse
        )
    }

    private func verifyImport(
        request: CloudKubeConfigImportRequest,
        expectedExecutable: String,
        expectedArgumentsBeforeTarget: [String],
        expectedProviderRequest: String
    ) async throws {
        let kubernetesServer = try await RuneFakeK8sRESTServer.start()
        defer { kubernetesServer.stop() }
        let providerServer = try CloudProviderContractServer.start(
            kubeconfigYAML: kubernetesServer.kubeconfigYAML(),
            kubernetesEndpoint: "http://127.0.0.1:\(kubernetesServer.port)"
        )
        defer { providerServer.stop() }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("CloudProviderContract.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let target = directory.appendingPathComponent("config")
        let targetedRequest = request.withContractTarget(target.path)
        let runner = CloudProviderContractRunner(baseURL: providerServer.baseURL)
        let importer = CloudKubeConfigCLIImporter(runner: runner, timeout: 5)

        let result = try await importer.importCluster(targetedRequest)

        XCTAssertEqual(result.command.executable, expectedExecutable)
        XCTAssertEqual(
            Array(result.command.arguments.prefix(expectedArgumentsBeforeTarget.count)),
            expectedArgumentsBeforeTarget
        )
        XCTAssertEqual(result.discoveredURLs, [target])
        XCTAssertEqual(result.reviews.count, 1)
        XCTAssertTrue(
            result.reviews[0].isValid,
            result.reviews[0].issues.map(\.message).joined(separator: "\n")
        )
        XCTAssertEqual(result.reviews[0].contexts.map(\.name), [RuneFakeK8sFixture.defaultContextName])
        if request.provider != .aks {
            let imported = try String(contentsOf: target, encoding: .utf8)
            XCTAssertTrue(imported.contains(
                "certificate-authority-data: \(CloudProviderContractFixture.certificateAuthorityData)"
            ))
        }

        let namespaces = try await KubernetesClient(commandTimeout: 2).listNamespaces(
            from: [KubeConfigSource(url: target)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        )
        XCTAssertEqual(namespaces, ["alpha-zone", "bravo-zone"])
        XCTAssertEqual(providerServer.requestLines(), [expectedProviderRequest])
        XCTAssertTrue(kubernetesServer.requestLines().contains { $0.contains("/api/v1/namespaces") })
    }

    private func verifyRejectedImport(
        request: CloudKubeConfigImportRequest,
        responseMode: CloudProviderContractResponseMode,
        expectedErrorCode: URLError.Code
    ) async throws {
        let kubernetesServer = try await RuneFakeK8sRESTServer.start()
        defer { kubernetesServer.stop() }
        let providerServer = try CloudProviderContractServer.start(
            kubeconfigYAML: kubernetesServer.kubeconfigYAML(),
            kubernetesEndpoint: "http://127.0.0.1:\(kubernetesServer.port)",
            responseMode: responseMode
        )
        defer { providerServer.stop() }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("CloudProviderContractRejected.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let target = directory.appendingPathComponent("config")
        let importer = CloudKubeConfigCLIImporter(
            runner: CloudProviderContractRunner(baseURL: providerServer.baseURL),
            timeout: 5
        )

        var caughtError: Error?
        do {
            _ = try await importer.importCluster(request.withContractTarget(target.path))
        } catch {
            caughtError = error
        }

        XCTAssertNotNil(caughtError, "A malformed provider response must fail the import")
        XCTAssertFalse(
            FileManager.default.fileExists(atPath: target.path),
            "A rejected provider response must never write or publish kubeconfig"
        )
        if let urlError = caughtError as? URLError {
            XCTAssertEqual(urlError.code, expectedErrorCode)
        } else {
            XCTFail("Expected a fail-closed provider response error, got \(String(describing: caughtError))")
        }
        XCTAssertEqual(providerServer.requestLines().count, 1)
        XCTAssertTrue(kubernetesServer.requestLines().isEmpty)
    }
}

private extension CloudKubeConfigImportRequest {
    func withContractTarget(_ path: String) -> CloudKubeConfigImportRequest {
        CloudKubeConfigImportRequest(
            provider: provider,
            clusterName: clusterName,
            regionOrLocation: regionOrLocation,
            resourceGroup: resourceGroup,
            projectID: projectID,
            profileOrSubscription: profileOrSubscription,
            roleARN: roleARN,
            targetKubeconfigPath: path,
            overwriteExisting: overwriteExisting
        )
    }
}

/// Test-only stand-in for the installed provider CLI. The production importer
/// still builds the real documented `az`, `aws`, and `gcloud` commands; this
/// adapter consumes documented provider REST payloads and writes the same
/// isolated kubeconfig output boundary Rune reviews in production.
private struct CloudProviderContractRunner: CloudKubeConfigCommandRunning {
    let baseURL: URL

    func run(
        _ command: CloudKubeConfigCommandPreview,
        timeout _: TimeInterval
    ) async throws -> CloudKubeConfigCommandResult {
        let kubeconfig: String
        let target: String
        switch command.executable {
        case "az":
            target = try requiredOption("--file", in: command.arguments)
            let subscription = try requiredOption("--subscription", in: command.arguments)
            let resourceGroup = try requiredOption("--resource-group", in: command.arguments)
            let cluster = try requiredOption("--name", in: command.arguments)
            var components = URLComponents(
                url: baseURL.appendingPathComponent(
                    "subscriptions/\(subscription)/resourceGroups/\(resourceGroup)/providers/Microsoft.ContainerService/managedClusters/\(cluster)/listClusterUserCredential"
                ),
                resolvingAgainstBaseURL: false
            )!
            components.queryItems = [URLQueryItem(name: "api-version", value: "2026-03-01")]
            var request = URLRequest(url: components.url!)
            request.httpMethod = "POST"
            let response: AKSCredentialResults = try await decodedResponse(for: request)
            guard let encoded = response.kubeconfigs.first?.value,
                  !encoded.isEmpty,
                  let bytes = Data(base64Encoded: encoded),
                  !bytes.isEmpty,
                  let value = String(data: bytes, encoding: .utf8),
                  !value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                throw URLError(.cannotParseResponse)
            }
            kubeconfig = value

        case "aws":
            target = try requiredOption("--kubeconfig", in: command.arguments)
            _ = try requiredOption("--region", in: command.arguments)
            let clusterName = try requiredOption("--name", in: command.arguments)
            let response: EKSDescribeClusterResponse = try await decodedResponse(
                for: URLRequest(url: baseURL.appendingPathComponent("clusters/\(clusterName)"))
            )
            guard response.cluster.name == clusterName,
                  response.cluster.status == "ACTIVE",
                  let certificateAuthority = Data(base64Encoded: response.cluster.certificateAuthority.data),
                  !certificateAuthority.isEmpty else {
                throw URLError(.cannotParseResponse)
            }
            kubeconfig = Self.loopbackKubeconfig(
                server: response.cluster.endpoint,
                certificateAuthorityData: response.cluster.certificateAuthority.data
            )

        case "gcloud":
            guard let configuredTarget = command.environment["KUBECONFIG"], !configuredTarget.isEmpty else {
                throw URLError(.badURL)
            }
            target = configuredTarget
            let project = try requiredOption("--project", in: command.arguments)
            let location = try requiredOption("--location", in: command.arguments)
            guard command.arguments.count >= 4 else { throw URLError(.badURL) }
            let clusterName = command.arguments[3]
            let response: GKEClusterResponse = try await decodedResponse(
                for: URLRequest(url: baseURL.appendingPathComponent(
                    "v1/projects/\(project)/locations/\(location)/clusters/\(clusterName)"
                ))
            )
            guard response.name == clusterName,
                  let certificateAuthority = Data(base64Encoded: response.masterAuth.clusterCaCertificate),
                  !certificateAuthority.isEmpty else {
                throw URLError(.cannotParseResponse)
            }
            kubeconfig = Self.loopbackKubeconfig(
                server: "http://\(response.endpoint)",
                certificateAuthorityData: response.masterAuth.clusterCaCertificate
            )

        default:
            throw URLError(.unsupportedURL)
        }

        try kubeconfig.write(toFile: target, atomically: true, encoding: .utf8)
        return CloudKubeConfigCommandResult(
            exitCode: 0,
            stdout: "Updated isolated kubeconfig.\n",
            stderr: ""
        )
    }

    private func decodedResponse<Value: Decodable>(for request: URLRequest) async throws -> Value {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.urlCache = nil
        let (data, response) = try await URLSession(configuration: configuration).data(for: request)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw URLError(.badServerResponse)
        }
        return try JSONDecoder().decode(Value.self, from: data)
    }

    private func requiredOption(_ option: String, in arguments: [String]) throws -> String {
        guard let index = arguments.firstIndex(of: option), arguments.indices.contains(index + 1) else {
            throw URLError(.badURL)
        }
        return arguments[index + 1]
    }

    private static func loopbackKubeconfig(server: String, certificateAuthorityData: String) -> String {
        """
        apiVersion: v1
        kind: Config
        current-context: \(RuneFakeK8sFixture.defaultContextName)
        clusters:
        - name: \(RuneFakeK8sFixture.defaultContextName)
          cluster:
            server: \(server)
            certificate-authority-data: \(certificateAuthorityData)
        contexts:
        - name: \(RuneFakeK8sFixture.defaultContextName)
          context:
            cluster: \(RuneFakeK8sFixture.defaultContextName)
            namespace: alpha-zone
            user: synthetic-provider-user
        users:
        - name: synthetic-provider-user
          user:
            token: synthetic-provider-token
        """
    }
}

private enum CloudProviderContractFixture {
    static let certificateAuthorityData = Data("synthetic-ca".utf8).base64EncodedString()
}

private enum CloudProviderContractResponseMode: Equatable {
    case valid
    case aksEmptyCredential
    case aksInvalidBase64Credential
    case eksNonActive
    case eksInvalidCertificateAuthority
    case gkeInvalidCertificateAuthority
    case gkeNotFound
}

private struct AKSCredentialResults: Codable {
    struct Kubeconfig: Codable {
        let name: String
        let value: String
    }

    let kubeconfigs: [Kubeconfig]
}

private struct EKSDescribeClusterResponse: Codable {
    struct Cluster: Codable {
        struct CertificateAuthority: Codable {
            let data: String
        }

        let name: String
        let endpoint: String
        let status: String
        let certificateAuthority: CertificateAuthority
    }

    let cluster: Cluster
}

private struct GKEClusterResponse: Codable {
    struct MasterAuth: Codable {
        let clusterCaCertificate: String
    }

    let name: String
    let location: String
    let endpoint: String
    let masterAuth: MasterAuth
}

/// Local HTTP response server whose routes and JSON fields follow these official
/// contracts:
/// - https://learn.microsoft.com/rest/api/aks/managed-clusters/list-cluster-user-credentials
/// - https://docs.aws.amazon.com/eks/latest/APIReference/API_DescribeCluster.html
/// - https://docs.cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters/get
private final class CloudProviderContractServer: @unchecked Sendable {
    let port: UInt16
    let baseURL: URL

    private let listener: NWListener
    private let recorder: CloudProviderRequestRecorder

    private init(listener: NWListener, port: UInt16, recorder: CloudProviderRequestRecorder) {
        self.listener = listener
        self.port = port
        self.baseURL = URL(string: "http://127.0.0.1:\(port)/")!
        self.recorder = recorder
    }

    static func start(
        kubeconfigYAML: String,
        kubernetesEndpoint: String,
        responseMode: CloudProviderContractResponseMode = .valid
    ) throws -> CloudProviderContractServer {
        let listener = try NWListener(using: .tcp, on: .any)
        let recorder = CloudProviderRequestRecorder()
        let router = CloudProviderContractRouter(
            kubeconfigYAML: kubeconfigYAML,
            kubernetesEndpoint: kubernetesEndpoint,
            responseMode: responseMode,
            recorder: recorder
        )
        listener.newConnectionHandler = { connection in
            router.receive(connection)
        }
        let result = CloudProviderServerStartResult()
        listener.stateUpdateHandler = { state in
            switch state {
            case .ready:
                guard let port = listener.port?.rawValue else {
                    result.resolve(.failure(URLError(.cannotConnectToHost)))
                    return
                }
                result.resolve(.success(CloudProviderContractServer(
                    listener: listener,
                    port: port,
                    recorder: recorder
                )))
            case .failed(let error):
                result.resolve(.failure(error))
            default:
                break
            }
        }
        listener.start(queue: DispatchQueue(label: "rune.provider-contract.listener"))
        return try result.wait()
    }

    func requestLines() -> [String] { recorder.snapshot() }
    func stop() { listener.cancel() }
}

private final class CloudProviderContractRouter: @unchecked Sendable {
    private let kubeconfigYAML: String
    private let kubernetesEndpoint: String
    private let responseMode: CloudProviderContractResponseMode
    private let recorder: CloudProviderRequestRecorder

    init(
        kubeconfigYAML: String,
        kubernetesEndpoint: String,
        responseMode: CloudProviderContractResponseMode,
        recorder: CloudProviderRequestRecorder
    ) {
        self.kubeconfigYAML = kubeconfigYAML
        self.kubernetesEndpoint = kubernetesEndpoint
        self.responseMode = responseMode
        self.recorder = recorder
    }

    func receive(_ connection: NWConnection) {
        connection.start(queue: DispatchQueue(label: "rune.provider-contract.connection"))
        connection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [self] data, _, _, _ in
            guard let data,
                  let request = String(data: data, encoding: .utf8),
                  let line = request.split(separator: "\r\n", maxSplits: 1).first.map(String.init) else {
                connection.cancel()
                return
            }
            recorder.append(line)
            connection.sendHTTP(route(line))
        }
    }

    private func route(_ requestLine: String) -> CloudProviderHTTPResponse {
        if requestLine == "POST /subscriptions/00000000-0000-4000-8000-000000000000/resourceGroups/synthetic-group/providers/Microsoft.ContainerService/managedClusters/synthetic-aks/listClusterUserCredential?api-version=2026-03-01 HTTP/1.1" {
            let value = switch responseMode {
            case .aksEmptyCredential: ""
            case .aksInvalidBase64Credential: "%%%"
            default: Data(kubeconfigYAML.utf8).base64EncodedString()
            }
            return json(AKSCredentialResults(kubeconfigs: [
                .init(name: "clusterUser", value: value)
            ]))
        }
        if requestLine == "GET /clusters/synthetic-eks HTTP/1.1" {
            let status = responseMode == .eksNonActive ? "CREATING" : "ACTIVE"
            let certificateAuthority = responseMode == .eksInvalidCertificateAuthority
                ? "%%%"
                : CloudProviderContractFixture.certificateAuthorityData
            return json(EKSDescribeClusterResponse(cluster: .init(
                name: "synthetic-eks",
                endpoint: kubernetesEndpoint,
                status: status,
                certificateAuthority: .init(data: certificateAuthority)
            )))
        }
        if requestLine == "GET /v1/projects/synthetic-project/locations/europe-north1/clusters/synthetic-gke HTTP/1.1" {
            if responseMode == .gkeNotFound {
                return CloudProviderHTTPResponse(
                    status: 404,
                    body: Data(#"{"error":{"code":404,"status":"NOT_FOUND"}}"#.utf8)
                )
            }
            let endpoint = kubernetesEndpoint.replacingOccurrences(of: "http://", with: "")
            let certificateAuthority = responseMode == .gkeInvalidCertificateAuthority
                ? "%%%"
                : CloudProviderContractFixture.certificateAuthorityData
            return json(GKEClusterResponse(
                name: "synthetic-gke",
                location: "europe-north1",
                endpoint: endpoint,
                masterAuth: .init(clusterCaCertificate: certificateAuthority)
            ))
        }
        return CloudProviderHTTPResponse(status: 404, body: Data(#"{"error":{"code":"not_found"}}"#.utf8))
    }

    private func json<Value: Encodable>(_ value: Value) -> CloudProviderHTTPResponse {
        do {
            return CloudProviderHTTPResponse(status: 200, body: try JSONEncoder().encode(value))
        } catch {
            return CloudProviderHTTPResponse(status: 500, body: Data(#"{"error":{"code":"encoding_failed"}}"#.utf8))
        }
    }
}

private struct CloudProviderHTTPResponse {
    let status: Int
    let body: Data
}

private extension NWConnection {
    func sendHTTP(_ response: CloudProviderHTTPResponse) {
        let reason = response.status == 200 ? "OK" : response.status == 404 ? "Not Found" : "Internal Server Error"
        var data = Data(
            "HTTP/1.1 \(response.status) \(reason)\r\nContent-Type: application/json\r\nContent-Length: \(response.body.count)\r\nConnection: close\r\n\r\n".utf8
        )
        data.append(response.body)
        send(content: data, completion: .contentProcessed { [weak self] _ in self?.cancel() })
    }
}

private final class CloudProviderRequestRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var lines: [String] = []

    func append(_ line: String) {
        lock.lock()
        lines.append(line)
        lock.unlock()
    }

    func snapshot() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return lines
    }
}

private final class CloudProviderServerStartResult: @unchecked Sendable {
    private let lock = NSLock()
    private let semaphore = DispatchSemaphore(value: 0)
    private var result: Result<CloudProviderContractServer, Error>?

    func resolve(_ value: Result<CloudProviderContractServer, Error>) {
        lock.lock()
        guard result == nil else {
            lock.unlock()
            return
        }
        result = value
        lock.unlock()
        semaphore.signal()
    }

    func wait() throws -> CloudProviderContractServer {
        guard semaphore.wait(timeout: .now() + 5) == .success else {
            throw URLError(.timedOut)
        }
        lock.lock()
        defer { lock.unlock() }
        return try result!.get()
    }
}
