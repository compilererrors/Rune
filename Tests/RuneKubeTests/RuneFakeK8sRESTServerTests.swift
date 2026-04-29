import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube

final class RuneFakeK8sRESTServerTests: XCTestCase {
    func testNativeClientReadsScriptlessRESTFakeCluster() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        let contexts = try await client.listContexts(from: sources)
        XCTAssertEqual(contexts, [context])
        let defaultNamespace = try await client.contextNamespace(from: sources, context: context)
        XCTAssertEqual(defaultNamespace, "alpha-zone")
        let namespaces = try await client.listNamespaces(from: sources, context: context)
        XCTAssertEqual(namespaces, ["alpha-zone", "bravo-zone"])

        let pods = try await client.listPods(from: sources, context: context, namespace: "alpha-zone")
        XCTAssertEqual(pods.map(\.name), ["ember-gate-75c9f746b8-kq2wm", "orbit-lens-6f58d7d89b-hx9q2"])
        XCTAssertEqual(pods.first(where: { $0.name.hasPrefix("orbit-lens") })?.cpuUsage, "42m")
        XCTAssertEqual(pods.first(where: { $0.name.hasPrefix("orbit-lens") })?.memoryUsage, "96Mi")

        let deployments = try await client.listDeployments(from: sources, context: context, namespace: "alpha-zone")
        XCTAssertEqual(deployments.map(\.name), ["ember-gate", "orbit-lens"])
        XCTAssertEqual(deployments.first(where: { $0.name == "orbit-lens" })?.readyReplicas, 2)

        let services = try await client.listServices(from: sources, context: context, namespace: "alpha-zone")
        XCTAssertEqual(services.map(\.name), ["ember-gate", "orbit-lens"])
        XCTAssertEqual(services.first(where: { $0.name == "orbit-lens" })?.selector, ["app": "orbit-lens"])

        let count = try await client.countNamespacedResources(
            from: sources,
            context: context,
            namespace: "alpha-zone",
            resource: "pods"
        )
        XCTAssertEqual(count, 2)

        let logs = try await client.podLogs(
            from: sources,
            context: context,
            namespace: "alpha-zone",
            podName: "orbit-lens-6f58d7d89b-hx9q2",
            filter: .tailLines(50),
            previous: false
        )
        XCTAssertTrue(logs.contains("synthetic REST fake log"))
    }

    func testRESTFakeSupportsKubernetesPaginationMetadata() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }

        let url = URL(string: "http://127.0.0.1:\(server.port)/api/v1/namespaces/alpha-zone/pods?limit=1")!
        let (data, response) = try await URLSession.shared.data(from: url)
        XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 200)

        let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        let metadata = object?["metadata"] as? [String: Any]
        let items = object?["items"] as? [[String: Any]]

        XCTAssertEqual(metadata?["remainingItemCount"] as? Int, 1)
        XCTAssertEqual(metadata?["continue"] as? String, "1")
        XCTAssertEqual(items?.count, 1)
    }

    private func writeKubeconfig(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-rest-fake-kubeconfig-\(UUID().uuidString).yaml")
        try contents.write(to: url, atomically: true, encoding: .utf8)
        return url
    }
}
