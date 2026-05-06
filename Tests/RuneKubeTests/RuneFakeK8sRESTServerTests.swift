import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube

final class RuneFakeK8sRESTServerTests: XCTestCase {
    func testSelfSubjectAccessReviewAllowedParserReadsStatusAllowed() throws {
        let allowed = try KubernetesRESTClient.selfSubjectAccessReviewAllowed(
            from: #"{"apiVersion":"authorization.k8s.io/v1","status":{"allowed":true}}"#
        )
        let denied = try KubernetesRESTClient.selfSubjectAccessReviewAllowed(
            from: #"{"apiVersion":"authorization.k8s.io/v1","status":{"allowed":false}}"#
        )

        XCTAssertTrue(allowed)
        XCTAssertFalse(denied)
    }

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

    func testOperatorResourceProbeReturnsEmptyWhenCRDsAreAbsent() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let resources = try await client.listOperatorResources(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone"
        )

        XCTAssertTrue(resources.isEmpty)
    }

    func testOperatorResourceProbeReadsCertManagerFluxAndArgoCDResources() async throws {
        let base = RuneFakeK8sFixture.defaultContexts[0]
        let fixture = RuneFakeK8sFixture(contexts: [
            RuneFakeK8sCluster(
                contextName: base.contextName,
                defaultNamespace: base.defaultNamespace,
                namespaces: base.namespaces,
                nodes: base.nodes,
                operatorResources: [
                    RuneFakeK8sOperatorResource(
                        apiGroup: "cert-manager.io",
                        apiVersion: "v1",
                        plural: "certificates",
                        kind: "Certificate",
                        name: "web-tls",
                        namespace: "alpha-zone",
                        conditionType: "Ready",
                        conditionStatus: "True",
                        reason: "Issued",
                        message: "Certificate is up to date"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "source.toolkit.fluxcd.io",
                        apiVersion: "v1",
                        plural: "gitrepositories",
                        kind: "GitRepository",
                        name: "platform",
                        namespace: "alpha-zone",
                        conditionType: "Ready",
                        conditionStatus: "False",
                        reason: "FetchFailed",
                        message: "Waiting for repository access"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "argoproj.io",
                        apiVersion: "v1alpha1",
                        plural: "applications",
                        kind: "Application",
                        name: "control-plane",
                        namespace: "alpha-zone",
                        conditionType: "Synced",
                        conditionStatus: "True",
                        reason: "Healthy",
                        message: "Application is synced"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "external-secrets.io",
                        apiVersion: "v1beta1",
                        plural: "externalsecrets",
                        kind: "ExternalSecret",
                        name: "payments-api",
                        namespace: "alpha-zone",
                        conditionType: "Ready",
                        conditionStatus: "True",
                        reason: "SecretSynced",
                        message: "Secret is synced"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "apiextensions.crossplane.io",
                        apiVersion: "v1",
                        plural: "compositions",
                        kind: "Composition",
                        name: "postgresql",
                        namespace: nil,
                        conditionType: "Established",
                        conditionStatus: "True",
                        reason: "Active",
                        message: "Composition is available"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "gateway.networking.k8s.io",
                        apiVersion: "v1",
                        plural: "gateways",
                        kind: "Gateway",
                        name: "edge",
                        namespace: "alpha-zone",
                        conditionType: "Programmed",
                        conditionStatus: "True",
                        reason: "ListenersValid",
                        message: "Gateway listeners are programmed"
                    )
                ]
            )
        ])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let resources = try await client.listOperatorResources(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone"
        )

        XCTAssertEqual(Set(resources.map(\.family)), Set(["ArgoCD", "Crossplane", "External Secrets", "Flux", "Gateway API", "cert-manager"]))
        XCTAssertEqual(resources.first(where: { $0.name == "web-tls" })?.kind, "Certificates")
        XCTAssertEqual(resources.first(where: { $0.name == "web-tls" })?.status, "Ready True")
        XCTAssertEqual(resources.first(where: { $0.name == "platform" })?.message, "Waiting for repository access")
        XCTAssertEqual(resources.first(where: { $0.name == "control-plane" })?.apiPath, "/apis/argoproj.io/v1alpha1/namespaces/alpha-zone/applications")
        XCTAssertEqual(resources.first(where: { $0.name == "payments-api" })?.family, "External Secrets")
        XCTAssertEqual(resources.first(where: { $0.name == "postgresql" })?.namespace, nil)
        XCTAssertEqual(resources.first(where: { $0.name == "edge" })?.status, "Programmed True")
    }

    private func writeKubeconfig(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-rest-fake-kubeconfig-\(UUID().uuidString).yaml")
        try contents.write(to: url, atomically: true, encoding: .utf8)
        return url
    }
}
