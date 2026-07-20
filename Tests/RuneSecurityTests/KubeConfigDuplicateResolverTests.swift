import XCTest
@testable import RuneSecurity

final class KubeConfigDuplicateResolverTests: XCTestCase {
    func testSkipDuplicateKeepsFirstDefinitions() throws {
        let resolved = try KubeConfigDuplicateResolver().resolve(
            raw: duplicateKubeConfig,
            choice: .skipDuplicate
        )
        let review = KubeConfigImportValidator().validate(raw: resolved)

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.map(\.name), ["synthetic-context"])
        XCTAssertEqual(review.contexts.first?.serverHost, "first.example.invalid")
        XCTAssertTrue(resolved.contains("first-token"))
        XCTAssertFalse(resolved.contains("second-token"))
    }

    func testUpdateExistingKeepsLastDefinitions() throws {
        let resolved = try KubeConfigDuplicateResolver().resolve(
            raw: duplicateKubeConfig,
            choice: .updateExisting
        )
        let review = KubeConfigImportValidator().validate(raw: resolved)

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.map(\.name), ["synthetic-context"])
        XCTAssertEqual(review.contexts.first?.serverHost, "second.example.invalid")
        XCTAssertFalse(resolved.contains("first-token"))
        XCTAssertTrue(resolved.contains("second-token"))
    }

    func testImportAsCopyKeepsDefinitionsAndRewritesReferences() throws {
        let resolved = try KubeConfigDuplicateResolver().resolve(
            raw: duplicateKubeConfig,
            choice: .importAsCopy
        )
        let review = KubeConfigImportValidator().validate(raw: resolved)

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.map(\.name), ["synthetic-context", "synthetic-context-copy-2"])
        XCTAssertEqual(
            review.contexts.compactMap(\.serverHost).sorted(),
            ["first.example.invalid", "second.example.invalid"]
        )
        XCTAssertTrue(resolved.contains("synthetic-cluster-copy-2"))
        XCTAssertTrue(resolved.contains("synthetic-user-copy-2"))
        XCTAssertTrue(resolved.contains("first-token"))
        XCTAssertTrue(resolved.contains("second-token"))
    }

    func testImportAsCopyAvoidsExistingSuffixCollisionsDeterministically() throws {
        let raw = duplicateKubeConfig.replacingOccurrences(
            of:
            """
            clusters:
            - name: synthetic-cluster
            """,
            with:
            """
            clusters:
            - name: synthetic-cluster-copy-2
              cluster:
                server: https://existing-copy.example.invalid
            - name: synthetic-cluster
            """
        )

        let first = try KubeConfigDuplicateResolver().resolve(raw: raw, choice: .importAsCopy)
        let second = try KubeConfigDuplicateResolver().resolve(raw: raw, choice: .importAsCopy)

        XCTAssertEqual(first, second)
        XCTAssertTrue(first.contains("synthetic-cluster-copy-3"))
        XCTAssertTrue(KubeConfigImportValidator().validate(raw: first).isValid)
    }

    func testPayloadWithoutDuplicatesIsReturnedByteForByte() throws {
        let raw = duplicateKubeConfig
            .replacingOccurrences(
                of:
                """
                - name: synthetic-cluster
                  cluster:
                    server: https://second.example.invalid
                """,
                with: ""
            )
            .replacingOccurrences(
                of:
                """
                - name: synthetic-user
                  user:
                    token: second-token
                """,
                with: ""
            )
            .replacingOccurrences(
                of:
                """
                - name: synthetic-context
                  context:
                    cluster: synthetic-cluster
                    user: synthetic-user
                    namespace: second
                """,
                with: ""
            )

        XCTAssertEqual(
            try KubeConfigDuplicateResolver().resolve(raw: raw, choice: .importAsCopy),
            raw
        )
    }

    func testImportAsCopyRenamesConflictsAgainstReservedSourcesAndCurrentContext() throws {
        let single = duplicateKubeConfig
            .replacingOccurrences(
                of:
                """
                - name: synthetic-cluster
                  cluster:
                    server: https://second.example.invalid
                """,
                with: ""
            )
            .replacingOccurrences(
                of:
                """
                - name: synthetic-user
                  user:
                    token: second-token
                """,
                with: ""
            )
            .replacingOccurrences(
                of:
                """
                - name: synthetic-context
                  context:
                    cluster: synthetic-cluster
                    user: synthetic-user
                    namespace: second
                """,
                with: ""
            )
        let reserved = KubeConfigNameRegistry(
            contextNames: ["synthetic-context"],
            clusterNames: ["synthetic-cluster"],
            userNames: ["synthetic-user"]
        )

        let resolution = try KubeConfigDuplicateResolver().resolve(
            raw: single,
            choice: .importAsCopy,
            reservedNames: reserved
        )
        let review = KubeConfigImportValidator().validate(raw: resolution.raw)

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.map(\.name), ["synthetic-context-copy-2"])
        XCTAssertEqual(review.contexts.first?.clusterName, "synthetic-cluster-copy-2")
        XCTAssertEqual(review.contexts.first?.userName, "synthetic-user-copy-2")
        XCTAssertTrue(resolution.raw.contains("current-context: synthetic-context-copy-2"))
        XCTAssertEqual(resolution.contextNameVariants["synthetic-context"], ["synthetic-context-copy-2"])
        XCTAssertEqual(resolution.currentContextName, "synthetic-context-copy-2")
    }

    private var duplicateKubeConfig: String {
        """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://first.example.invalid
        - name: synthetic-cluster
          cluster:
            server: https://second.example.invalid
        users:
        - name: synthetic-user
          user:
            token: first-token
        - name: synthetic-user
          user:
            token: second-token
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
            namespace: first
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
            namespace: second
        """
    }
}
