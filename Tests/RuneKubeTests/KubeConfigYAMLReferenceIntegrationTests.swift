import Foundation
import XCTest
@testable import RuneCore
@testable import RuneKube

final class KubeConfigYAMLReferenceIntegrationTests: XCTestCase {
    func testListContextsResolvesAnchorsAliasesAndMergeKeys() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneKubeYAMLReferences.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        kind: Config
        current-context: merged-context
        clusters:
        - &cluster-entry
          name: anchored-cluster
          cluster: &cluster-values
            server: https://cluster.example.invalid
        - <<: *cluster-entry
          name: merged-cluster
        contexts:
        - &context-entry
          name: alias-context
          context: &context-values
            cluster: anchored-cluster
            user: anchored-user
            namespace: synthetic-namespace
        - <<: *context-entry
          name: merged-context
          context:
            <<: *context-values
            cluster: merged-cluster
            user: merged-user
        - *context-entry
        users:
        - &user-entry
          name: anchored-user
          user: &user-values
            token: synthetic-token
        - <<: *user-entry
          name: merged-user
          user:
            <<: *user-values
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let client = KubernetesClient()
        let sources = [KubeConfigSource(url: kubeconfig)]
        let contexts = try await client.listContexts(from: sources)
        let mergedNamespace = try await client.contextNamespace(
            from: sources,
            context: KubeContext(name: "merged-context")
        )

        XCTAssertEqual(contexts.map(\.name), ["alias-context", "merged-context"])
        XCTAssertEqual(mergedNamespace, "synthetic-namespace")
    }
}
