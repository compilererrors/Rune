import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube

final class RuneFakeK8sCompatibilityTests: XCTestCase {
    private let namespace = "compat-zone"

    func testPodListReportsKubectlStatusForSidecarInitCrashLoopAndTerminatingPods() async throws {
        let pods = [
            pod("plain-app"),
            pod("mesh-app", sidecars: ["mesh-proxy"]),
            pod("init-blocked-app", blockingInit: "wait-for-db"),
            pod("crashing-app", waiting: "CrashLoopBackOff", restarts: 7),
            pod("draining-app", terminating: true)
        ]
        let (server, kubeconfig) = try await startServer(pods: pods)
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeconfig)
        }

        let listed = try await KubernetesClient(commandTimeout: 2).listPods(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: namespace
        )
        let byName = Dictionary(uniqueKeysWithValues: listed.map { ($0.name, $0) })

        let plain = try XCTUnwrap(byName["plain-app"])
        XCTAssertEqual(plain.displayStatus, "Running")
        XCTAssertNil(plain.statusReason)
        XCTAssertEqual(plain.containersReady, "1/1")

        let mesh = try XCTUnwrap(byName["mesh-app"])
        XCTAssertEqual(mesh.displayStatus, "Running")
        XCTAssertEqual(mesh.containersReady, "2/2", "A native sidecar counts toward READY like in kubectl.")
        XCTAssertEqual(mesh.initContainerNames, ["mesh-proxy"])

        let initBlocked = try XCTUnwrap(byName["init-blocked-app"])
        XCTAssertEqual(initBlocked.status, "Pending")
        XCTAssertEqual(initBlocked.displayStatus, "Init:0/1")
        XCTAssertEqual(initBlocked.containersReady, "0/1")

        let crashing = try XCTUnwrap(byName["crashing-app"])
        XCTAssertEqual(crashing.status, "Running", "The phase stays Running; health counts rely on it.")
        XCTAssertEqual(crashing.displayStatus, "CrashLoopBackOff")
        XCTAssertEqual(crashing.containersReady, "0/1")
        XCTAssertEqual(crashing.totalRestarts, 7)

        let draining = try XCTUnwrap(byName["draining-app"])
        XCTAssertEqual(draining.status, "Running")
        XCTAssertEqual(draining.displayStatus, "Terminating")
    }

    func testPodDetailKeepsKubectlStatusAfterInspectorMerge() async throws {
        let (server, kubeconfig) = try await startServer(pods: [pod("crashing-app", waiting: "CrashLoopBackOff")])
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeconfig)
        }
        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        let rows = try await client.listPods(from: sources, context: context, namespace: namespace)
        let row = try XCTUnwrap(rows.first)
        let merged = row.mergingInspectorDetail(row)
        XCTAssertEqual(merged.displayStatus, "CrashLoopBackOff")
        XCTAssertEqual(merged.status, "Running")
    }

    func testExternalSecretsAreReadThroughServedV1WithoutProbingRemovedVersions() async throws {
        let (server, kubeconfig) = try await startServer(operatorResources: [
            externalSecret(version: "v1", name: "payments-sync"),
            certificate(name: "edge-tls")
        ])
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeconfig)
        }

        let resources = try await KubernetesClient(commandTimeout: 2).listOperatorResources(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: namespace
        )

        let secret = try XCTUnwrap(resources.first { $0.name == "payments-sync" })
        XCTAssertEqual(secret.family, "External Secrets")
        XCTAssertEqual(secret.apiPath, "/apis/external-secrets.io/v1/namespaces/\(namespace)/externalsecrets")
        XCTAssertEqual(secret.status, "Ready True")
        XCTAssertTrue(resources.contains { $0.name == "edge-tls" && $0.family == "cert-manager" })

        let operatorRequests = server.requestLines().filter { $0.contains(" /apis/") }
        XCTAssertFalse(operatorRequests.contains { $0.contains("external-secrets.io/v1beta1") },
                       "A version the cluster doesn't serve isn't requested.")
        XCTAssertFalse(operatorRequests.contains { $0.contains("argoproj.io") || $0.contains("fluxcd.io") || $0.contains("crossplane.io") },
                       "Groups the cluster doesn't serve aren't requested.")
    }

    func testOlderExternalSecretsInstallStillListsThroughV1beta1() async throws {
        let (server, kubeconfig) = try await startServer(operatorResources: [
            externalSecret(version: "v1beta1", name: "legacy-sync")
        ])
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeconfig)
        }

        let resources = try await KubernetesClient(commandTimeout: 2).listOperatorResources(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: namespace
        )

        let secret = try XCTUnwrap(resources.first { $0.name == "legacy-sync" })
        XCTAssertEqual(secret.apiPath, "/apis/external-secrets.io/v1beta1/namespaces/\(namespace)/externalsecrets")
        XCTAssertFalse(server.requestLines().contains { $0.contains("/apis/external-secrets.io/v1/") })
    }

    private func pod(
        _ name: String,
        sidecars: [String] = [],
        blockingInit: String? = nil,
        waiting: String? = nil,
        restarts: Int = 0,
        terminating: Bool = false
    ) -> RuneFakeK8sPod {
        var pod = RuneFakeK8sPod(
            name: name,
            deploymentName: name,
            phase: "Running",
            restarts: restarts,
            cpu: "1m",
            memory: "1Mi",
            podIP: "10.42.0.9",
            nodeName: "synthetic-node",
            labels: ["app": name],
            containers: ["main"]
        )
        pod.nativeSidecars = sidecars
        pod.blockingInitContainer = blockingInit
        pod.containerWaitingReason = waiting
        pod.isTerminating = terminating
        return pod
    }

    private func externalSecret(version: String, name: String) -> RuneFakeK8sOperatorResource {
        RuneFakeK8sOperatorResource(
            apiGroup: "external-secrets.io",
            apiVersion: version,
            plural: "externalsecrets",
            kind: "ExternalSecret",
            name: name,
            namespace: namespace,
            conditionType: "Ready",
            conditionStatus: "True",
            reason: "SecretSynced",
            message: "Secret was synced"
        )
    }

    private func certificate(name: String) -> RuneFakeK8sOperatorResource {
        RuneFakeK8sOperatorResource(
            apiGroup: "cert-manager.io",
            apiVersion: "v1",
            plural: "certificates",
            kind: "Certificate",
            name: name,
            namespace: namespace,
            conditionType: "Ready",
            conditionStatus: "True",
            reason: "Issued",
            message: "Certificate is up to date"
        )
    }

    private func startServer(
        pods: [RuneFakeK8sPod] = [],
        operatorResources: [RuneFakeK8sOperatorResource] = []
    ) async throws -> (RuneFakeK8sRESTServer, URL) {
        let fixture = RuneFakeK8sFixture(contexts: [
            RuneFakeK8sCluster(
                contextName: RuneFakeK8sFixture.defaultContextName,
                defaultNamespace: namespace,
                namespaces: [RuneFakeK8sNamespace(name: namespace, pods: pods, deployments: [], services: [])],
                nodes: [],
                operatorResources: operatorResources
            )
        ])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-compat-fake-kubeconfig-\(UUID().uuidString).yaml")
        try server.kubeconfigYAML().write(to: url, atomically: true, encoding: .utf8)
        return (server, url)
    }
}
