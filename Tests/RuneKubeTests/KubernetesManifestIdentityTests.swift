import XCTest
@testable import RuneKube

final class KubernetesManifestIdentityTests: XCTestCase {
    func testNamespacedManifestUsesExplicitOrDefaultNamespace() throws {
        let explicit = try KubernetesManifestIdentity.parse(
            yaml: """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: "settings"
              namespace: test-space
            """,
            defaultNamespace: "fallback-space"
        )
        XCTAssertEqual(explicit.apiVersion, "v1")
        XCTAssertEqual(explicit.kind, .configMap)
        XCTAssertEqual(explicit.name, "settings")
        XCTAssertEqual(explicit.namespace, "test-space")

        let fallback = try KubernetesManifestIdentity.parse(
            yaml: """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings # synthetic fixture
            """,
            defaultNamespace: "fallback-space"
        )
        XCTAssertEqual(fallback.namespace, "fallback-space")
    }

    func testClusterScopedManifestDoesNotInheritSelectedNamespace() throws {
        let identity = try KubernetesManifestIdentity.parse(
            yaml: """
            apiVersion: v1
            kind: Node
            metadata:
              name: worker-one
            """,
            defaultNamespace: "test-space"
        )

        XCTAssertEqual(identity.kind, .node)
        XCTAssertEqual(identity.name, "worker-one")
        XCTAssertEqual(identity.namespace, "")
    }

    func testNestedMetadataFieldsNeverOverrideResourceIdentity() throws {
        let identity = try KubernetesManifestIdentity.parse(
            yaml: """
            apiVersion: v1
            kind: Pod
            metadata:
              name: synthetic-pod
              namespace: test-space
              labels:
                name: nested-label-value
                namespace: nested-label-namespace
              ownerReferences:
                - apiVersion: apps/v1
                  kind: ReplicaSet
                  name: synthetic-owner
            spec:
              containers:
                - name: synthetic-container
                  image: example.invalid/synthetic:1
            """,
            defaultNamespace: "fallback-space"
        )

        XCTAssertEqual(identity.kind, .pod)
        XCTAssertEqual(identity.name, "synthetic-pod")
        XCTAssertEqual(identity.namespace, "test-space")
    }

    func testNestedOwnerNameDoesNotSatisfyMissingMetadataName() {
        XCTAssertThrowsError(
            try KubernetesManifestIdentity.parse(
                yaml: """
                apiVersion: v1
                kind: Pod
                metadata:
                  namespace: test-space
                  ownerReferences:
                    - apiVersion: apps/v1
                      kind: ReplicaSet
                      name: synthetic-owner
                """,
                defaultNamespace: "fallback-space"
            )
        ) { error in
            XCTAssertTrue(error.localizedDescription.contains("missing metadata.name"))
        }
    }

    func testFlowMetadataAndQuotedKeysUseDecodedIdentity() throws {
        let identity = try KubernetesManifestIdentity.parse(
            yaml: """
            "apiVersion": v1
            'kind': Pod
            metadata: {"name": "synthetic-\\u0070od", 'namespace': test-space}
            spec:
              containers: []
            """,
            defaultNamespace: "fallback-space"
        )

        XCTAssertEqual(identity.apiVersion, "v1")
        XCTAssertEqual(identity.kind, .pod)
        XCTAssertEqual(identity.name, "synthetic-pod")
        XCTAssertEqual(identity.namespace, "test-space")
    }

    func testLargeManifestIdentityParsingStaysWithinInteractiveBudget() throws {
        let data = (0..<20_000)
            .map { "  synthetic-key-\($0): synthetic-value-\($0)" }
            .joined(separator: "\n")
        let yaml = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: synthetic-large
          namespace: test-space
        data:
        \(data)
        """

        let startedAt = Date()
        let identity = try KubernetesManifestIdentity.parse(
            yaml: yaml,
            defaultNamespace: "fallback-space"
        )
        let elapsed = Date().timeIntervalSince(startedAt)

        XCTAssertEqual(identity.name, "synthetic-large")
        XCTAssertLessThan(
            elapsed,
            1,
            "Extracting the target from a large manifest must not add a multi-second editor stall."
        )
    }

    func testUnsupportedManifestKindIsRejectedBeforeRequestConstruction() {
        XCTAssertThrowsError(
            try KubernetesManifestIdentity.parse(
                yaml: """
                apiVersion: example.invalid/v1
                kind: SyntheticWidget
                metadata:
                  name: sample
                """,
                defaultNamespace: "test-space"
            )
        )
    }
}
