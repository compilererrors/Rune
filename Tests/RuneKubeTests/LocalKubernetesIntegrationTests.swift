import Foundation
import Network
import XCTest
@testable import RuneCore
@testable import RuneKube

final class LocalKubernetesIntegrationTests: XCTestCase {
    func testNativeRESTClientReadsPodsMetricsAndLogsFromLocalFixture() async throws {
        let server = try await LocalKubernetesAPIServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(serverURL: "http://127.0.0.1:\(server.port)")
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: "local-fixture")

        let namespaces = try await client.listNamespaces(from: sources, context: context)
        XCTAssertEqual(namespaces, ["default"])

        let pods = try await client.listPods(from: sources, context: context, namespace: "default")
        XCTAssertEqual(pods.map(\.name), ["api-0"])
        XCTAssertEqual(pods.first?.status, "Running")
        XCTAssertEqual(pods.first?.cpuUsage, "42m")
        XCTAssertEqual(pods.first?.memoryUsage, "64Mi")

        let logs = try await client.podLogs(
            from: sources,
            context: context,
            namespace: "default",
            podName: "api-0",
            filter: .tailLines(200),
            previous: false
        )
        XCTAssertEqual(logs, "line one\nline two\n")
    }

    func testNativeHelmReleaseReaderDecodesV3StorageWithoutHelmBinary() async throws {
        let server = try await LocalKubernetesAPIServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(serverURL: "http://127.0.0.1:\(server.port)")
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: "local-fixture")

        let releases = try await client.listReleases(
            from: sources,
            context: context,
            namespace: "default",
            allNamespaces: false
        )

        XCTAssertEqual(releases.count, 1)
        XCTAssertEqual(releases.first?.name, "orbit")
        XCTAssertEqual(releases.first?.namespace, "default")
        XCTAssertEqual(releases.first?.revision, 3)
        XCTAssertEqual(releases.first?.status, "deployed")
        XCTAssertEqual(releases.first?.chart, "orbit-chart-1.2.3")
        XCTAssertEqual(releases.first?.appVersion, "4.5.6")

        let values = try await client.releaseValues(
            from: sources,
            context: context,
            namespace: "default",
            releaseName: "orbit"
        )
        XCTAssertTrue(values.contains("replicaCount: 2"))
        XCTAssertTrue(values.contains("tag: 4.5.6"))

        let manifest = try await client.releaseManifest(
            from: sources,
            context: context,
            namespace: "default",
            releaseName: "orbit"
        )
        XCTAssertTrue(manifest.contains("kind: ConfigMap"))

        let history = try await client.releaseHistory(
            from: sources,
            context: context,
            namespace: "default",
            releaseName: "orbit"
        )
        XCTAssertEqual(history.map(\.revision), [3])
    }

    private func writeKubeconfig(serverURL: String) throws -> URL {
        let kubeconfig = """
        apiVersion: v1
        kind: Config
        current-context: local-fixture
        clusters:
        - name: local-cluster
          cluster:
            server: \(serverURL)
        contexts:
        - name: local-fixture
          context:
            cluster: local-cluster
            user: local-user
            namespace: default
        users:
        - name: local-user
          user:
            token: local-token
        """
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-local-k8s-\(UUID().uuidString).yaml")
        try kubeconfig.write(to: url, atomically: true, encoding: .utf8)
        return url
    }
}

private final class LocalKubernetesAPIServer: @unchecked Sendable {
    let port: UInt16
    private let listener: NWListener
    private let queue = DispatchQueue(label: "rune.local-k8s-fixture")

    private init(listener: NWListener, port: UInt16) {
        self.listener = listener
        self.port = port
    }

    static func start() async throws -> LocalKubernetesAPIServer {
        let listener = try NWListener(using: .tcp, on: 0)
        listener.newConnectionHandler = { connection in
            connection.start(queue: DispatchQueue(label: "rune.local-k8s-fixture.connection"))
            connection.receive(minimumIncompleteLength: 1, maximumLength: 65_536) { data, _, _, _ in
                guard let data,
                      let request = String(data: data, encoding: .utf8),
                      let requestLine = request.split(separator: "\r\n").first else {
                    connection.cancel()
                    return
                }
                let parts = requestLine.split(separator: " ")
                let target = parts.count > 1 ? String(parts[1]) : "/"
                let (status, contentType, body) = response(for: target)
                let reason = status == 200 ? "OK" : "Not Found"
                let header = [
                    "HTTP/1.1 \(status) \(reason)",
                    "Content-Type: \(contentType)",
                    "Content-Length: \(body.utf8.count)",
                    "Connection: close",
                    "",
                    ""
                ].joined(separator: "\r\n")
                connection.send(content: Data((header + body).utf8), completion: .contentProcessed { _ in
                    connection.cancel()
                })
            }
        }

        return try await withCheckedThrowingContinuation { continuation in
            listener.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    guard let port = listener.port?.rawValue else {
                        continuation.resume(throwing: URLError(.cannotConnectToHost))
                        return
                    }
                    continuation.resume(returning: LocalKubernetesAPIServer(listener: listener, port: port))
                case let .failed(error):
                    continuation.resume(throwing: error)
                default:
                    break
                }
            }
            listener.start(queue: DispatchQueue(label: "rune.local-k8s-fixture.listener"))
        }
    }

    func stop() {
        listener.cancel()
    }

    private static func response(for target: String) -> (Int, String, String) {
        let path = target.split(separator: "?", maxSplits: 1).first.map(String.init) ?? target
        switch path {
        case "/api/v1/namespaces":
            return (200, "application/json", #"{"items":[{"metadata":{"name":"default"}}]}"#)
        case "/api/v1/namespaces/default/pods":
            return (200, "application/json", podListJSON)
        case "/apis/metrics.k8s.io/v1beta1/namespaces/default/pods":
            return (200, "application/json", podMetricsJSON)
        case "/api/v1/namespaces/default/pods/api-0/log":
            return (200, "text/plain", "line one\nline two\n")
        case "/api/v1/namespaces/default/pods/api-0":
            return (200, "application/json", podJSON)
        case "/api/v1/namespaces/default/secrets", "/api/v1/secrets":
            return (200, "application/json", helmSecretListJSON)
        default:
            return (404, "application/json", #"{"kind":"Status","status":"Failure","message":"not found"}"#)
        }
    }

    private static let podJSON = """
    {"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2026-04-26T10:00:00Z"},"status":{"phase":"Running","podIP":"10.42.0.10","containerStatuses":[{"ready":true,"restartCount":1}]}}
    """

    private static let podListJSON = """
    {"items":[\(podJSON)]}
    """

    private static let podMetricsJSON = """
    {"items":[{"metadata":{"name":"api-0","namespace":"default"},"containers":[{"usage":{"cpu":"42m","memory":"64Mi"}}]}]}
    """

    private static let helmReleasePayload = "SDRzSUFBQUFBQUFDLzFXUHNXN0RJQkNHWHdYZGJGdUdwQjVZTTNkck8xU1dvcXM1dTZqMmdRQkhpaUsvZThGTnEzUTdmbjYrKzdnQjQwS2d3WVVQbTZEYWo5SGpVREpESTY1elNTOFVvblVNK2xDQjVkR0J2c0dNTVowTitkbGR5ZVMyYWxWWHQ4ZGFkUyt5MVZKcXFkN3owNWd3clhHbjNhdFZIdU1RckU4N0VsNzlGTkNRR056aVowb0VXd1hESjRaVXRpeVUwR0RDTWorcTFqK05CeldRaldvT09VSHYzLzdDWS9QVWRMQVZwT1BSVG9VVHNva2Q4T1JXemp0VS90S0NFNVdiaE5PL053dXlIU25tVnFiYU8xV0xpK3o1eTdMUjRyUkRuOUgzL0d1cWV4YWlxR3F4bS9ZTTJ6ZG1tYy95WndFQUFBPT0="

    private static var helmSecretListJSON: String {
        """
        {"items":[{"metadata":{"name":"sh.helm.release.v1.orbit.v3","namespace":"default","labels":{"owner":"helm","name":"orbit","status":"deployed","version":"3"}},"data":{"release":"\(helmReleasePayload)"}}]}
        """
    }
}
