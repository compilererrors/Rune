import Foundation
import XCTest
@testable import RuneCore
@testable import RuneKube

final class KubernetesClientTests: XCTestCase {
    func testRESTClientLoadsContextsDirectlyFromKubeconfig() async throws {
        let kubeconfig = try writeKubeconfig(
            """
            apiVersion: v1
            kind: Config
            current-context: dev
            clusters:
            - name: dev-cluster
              cluster:
                server: http://127.0.0.1:65535
            contexts:
            - name: dev
              context:
                cluster: dev-cluster
                user: dev-user
                namespace: default
            users:
            - name: dev-user
              user:
                token: test-token
            """
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient()
        let contexts = try await client.listContexts(from: [KubeConfigSource(url: kubeconfig)])

        XCTAssertEqual(contexts, [KubeContext(name: "dev")])
    }

    func testRESTClientLoadsDefaultNamespaceDirectlyFromKubeconfig() async throws {
        let kubeconfig = try writeKubeconfig(
            """
            apiVersion: v1
            kind: Config
            current-context: prod
            clusters:
            - name: prod-cluster
              cluster:
                server: http://127.0.0.1:65535
            contexts:
            - name: prod
              context:
                cluster: prod-cluster
                user: prod-user
                namespace: platform
            users:
            - name: prod-user
              user:
                token: test-token
            """
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient()
        let namespace = try await client.contextNamespace(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: "prod")
        )

        XCTAssertEqual(namespace, "platform")
    }

    func testOutputParserParsesKubernetesPodJSON() throws {
        let raw = """
        {"items":[{"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2026-04-26T10:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":2}]}}]}
        """

        let pods = try KubernetesOutputParser().parsePodsListJSON(namespace: "default", from: raw)

        XCTAssertEqual(pods.count, 1)
        XCTAssertEqual(pods.first?.name, "api-0")
        XCTAssertEqual(pods.first?.status, "Running")
        XCTAssertEqual(pods.first?.totalRestarts, 2)
    }

    func testKubernetesListJSONReadsRemainingItemCount() {
        let raw = #"{"metadata":{"continue":"next","remainingItemCount":41},"items":[{"metadata":{"name":"one"}}]}"#

        XCTAssertEqual(KubernetesListJSON.collectionListTotal(from: raw), 42)
        XCTAssertEqual(KubernetesListJSON.collectionPageInfo(from: raw)?.continueToken, "next")
    }

    func testPreferredPortForwardPodChoosesRunningPodDeterministically() {
        let pods = [
            PodSummary(name: "api-b", namespace: "default", status: "Pending"),
            PodSummary(name: "api-c", namespace: "default", status: "Running"),
            PodSummary(name: "api-a", namespace: "default", status: "Running")
        ]

        XCTAssertEqual(KubernetesClient.preferredPortForwardPod(from: pods)?.name, "api-a")
    }

    func testServerSideApplyYAMLOmitsManagedFieldsFromFetchedManifest() {
        let yaml = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: api-0
          namespace: default
          managedFields:
          - apiVersion: v1
            fieldsType: FieldsV1
            fieldsV1:
              f:metadata:
                f:labels: {}
          labels:
            app: api
        spec:
          containers:
          - name: api
            image: api:latest
        """

        let sanitized = KubernetesRESTClient._testServerSideApplyYAML(from: yaml)

        XCTAssertFalse(sanitized.contains("managedFields"))
        XCTAssertFalse(sanitized.contains("fieldsType"))
        XCTAssertTrue(sanitized.contains("  labels:"))
        XCTAssertTrue(sanitized.contains("spec:"))
    }

    func testServerSideApplyYAMLOmitsInlineEmptyManagedFields() {
        let yaml = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: settings
          managedFields: []
          labels:
            app: settings
        data:
          key: value
        """

        let sanitized = KubernetesRESTClient._testServerSideApplyYAML(from: yaml)

        XCTAssertFalse(sanitized.contains("managedFields"))
        XCTAssertTrue(sanitized.contains("  labels:"))
        XCTAssertTrue(sanitized.contains("data:"))
    }

    private func writeKubeconfig(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-native-kubeconfig-\(UUID().uuidString).yaml")
        try contents.write(to: url, atomically: true, encoding: .utf8)
        return url
    }
}
