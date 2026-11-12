import XCTest
@testable import RuneSecurity
@testable import RuneUI

final class KubeConfigImportTransactionTests: XCTestCase {
    func testDuplicatePoliciesPreserveExplicitProviderOrigin() throws {
        let origin = KubeConfigImportOrigin(source: .guidedCLI, provider: .azure)
        let transaction = makeTransaction()
        let sourced = KubeConfigImportTransaction(
            payloads: transaction.payloads.map { .init(raw: $0.raw, sourceName: $0.sourceName, sourceURL: $0.sourceURL, origin: origin) },
            logLabel: "synthetic origin",
            existingNames: KubeConfigNameRegistry(),
            validator: KubeConfigImportValidator(),
            resolver: KubeConfigDuplicateResolver()
        )
        for choice in KubeConfigDuplicateHandlingChoice.allCases {
            let resolution = try sourced.resolvingDuplicates(choice: choice, resolver: KubeConfigDuplicateResolver(), validator: KubeConfigImportValidator())
            XCTAssertTrue(resolution.payloads.allSatisfy { $0.origin == origin })
        }
    }

    func testCopyPolicyResolvesCrossFileConflictsAndProjectsEveryReview() throws {
        let transaction = makeTransaction()

        let resolution = try transaction.resolvingDuplicates(
            choice: .importAsCopy,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )

        XCTAssertEqual(resolution.payloads.count, 2)
        XCTAssertEqual(resolution.reviews.count, 2)
        XCTAssertEqual(resolution.reviews[0].contexts.map(\.name), ["synthetic-context"])
        XCTAssertEqual(resolution.reviews[1].contexts.map(\.name), ["synthetic-context-copy-2"])
        XCTAssertTrue(resolution.payloads[1].raw.contains("current-context: synthetic-context-copy-2"))
        XCTAssertTrue(resolution.payloads[1].raw.contains("synthetic-cluster-copy-2"))
        XCTAssertTrue(resolution.payloads[1].raw.contains("synthetic-user-copy-2"))
        XCTAssertEqual(
            resolution.contextNamesForPreferences,
            ["synthetic-context", "synthetic-context-copy-2"]
        )
        XCTAssertEqual(resolution.sourcePlacement, .append)
        XCTAssertEqual(resolution.preferredContextName, "synthetic-context-copy-2")
        XCTAssertTrue(resolution.reviews.allSatisfy(\.isValid))
        XCTAssertTrue(resolution.reviews[1].hasDuplicateConflicts)
        XCTAssertFalse(resolution.reviews.contains { $0.redactedPreview.contains("synthetic-token") })
    }

    func testSkipPolicyProjectsOnlyFirstCrossFileContextForPreferences() throws {
        let resolution = try makeTransaction().resolvingDuplicates(
            choice: .skipDuplicate,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )

        XCTAssertEqual(resolution.reviews[0].contexts.map(\.name), ["synthetic-context"])
        XCTAssertTrue(resolution.reviews[1].contexts.isEmpty)
        XCTAssertEqual(resolution.contextNamesForPreferences, ["synthetic-context"])
        XCTAssertEqual(resolution.sourcePlacement, .append)
        XCTAssertEqual(resolution.preferredContextName, "synthetic-context")
        XCTAssertTrue(resolution.reviews.allSatisfy(\.isValid))
    }

    func testUpdatePolicyRequestsNewestFirstSourcePlacement() throws {
        let resolution = try makeTransaction().resolvingDuplicates(
            choice: .updateExisting,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )

        XCTAssertEqual(resolution.reviews.flatMap(\.contexts).map(\.name), [
            "synthetic-context",
            "synthetic-context"
        ])
        XCTAssertEqual(resolution.contextNamesForPreferences, ["synthetic-context"])
        XCTAssertEqual(resolution.sourcePlacement, .prependNewestFirst)
        XCTAssertEqual(resolution.preferredContextName, "synthetic-context")
        XCTAssertTrue(resolution.reviews.allSatisfy(\.isValid))
    }

    func testSkipPolicyBlocksNewContextThatRedefinesLoadedClusterName() throws {
        let transaction = makePartialCollisionTransaction(
            clusterName: "shared-cluster",
            userName: "incoming-user",
            existingNames: KubeConfigNameRegistry(clusterNames: ["shared-cluster"])
        )

        let resolution = try transaction.resolvingDuplicates(
            choice: .skipDuplicate,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )

        let review = try XCTUnwrap(resolution.reviews.first)
        let issue = try XCTUnwrap(review.issues.first { $0.id == "skip-duplicate-dependent-name-conflict" })
        XCTAssertEqual(review.contexts.map(\.name), ["incoming-context"])
        XCTAssertEqual(issue.severity, .error)
        XCTAssertTrue(issue.message.contains("earlier loaded cluster or user definitions"))
        XCTAssertFalse(review.isValid)
        XCTAssertNotNil(AddClusterCloudImportWorkflow.blockingFailure(in: resolution.reviews))
    }

    func testSkipPolicyBlocksNewContextThatRedefinesLoadedUserName() throws {
        let transaction = makePartialCollisionTransaction(
            clusterName: "incoming-cluster",
            userName: "shared-user",
            existingNames: KubeConfigNameRegistry(userNames: ["shared-user"])
        )

        let resolution = try transaction.resolvingDuplicates(
            choice: .skipDuplicate,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )

        let review = try XCTUnwrap(resolution.reviews.first)
        XCTAssertNotNil(review.issues.first { $0.id == "skip-duplicate-dependent-name-conflict" })
        XCTAssertFalse(review.isValid)
    }

    func testPartialDependencyCollisionRemainsValidWithExplicitUpdateOrCopyPolicy() throws {
        let transaction = makePartialCollisionTransaction(
            clusterName: "shared-cluster",
            userName: "shared-user",
            existingNames: KubeConfigNameRegistry(
                clusterNames: ["shared-cluster"],
                userNames: ["shared-user"]
            )
        )

        let updateResolution = try transaction.resolvingDuplicates(
            choice: .updateExisting,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )
        let updateReview = try XCTUnwrap(updateResolution.reviews.first)
        XCTAssertTrue(updateReview.isValid)
        XCTAssertEqual(updateReview.contexts.first?.clusterName, "shared-cluster")
        XCTAssertEqual(updateReview.contexts.first?.userName, "shared-user")
        XCTAssertEqual(updateReview.contexts.first?.serverHost, "incoming.example.invalid")
        XCTAssertEqual(updateResolution.sourcePlacement, .prependNewestFirst)

        let copyResolution = try transaction.resolvingDuplicates(
            choice: .importAsCopy,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )
        let copyReview = try XCTUnwrap(copyResolution.reviews.first)
        XCTAssertTrue(copyReview.isValid)
        XCTAssertEqual(copyReview.contexts.first?.name, "incoming-context")
        XCTAssertEqual(copyReview.contexts.first?.clusterName, "shared-cluster-copy-2")
        XCTAssertEqual(copyReview.contexts.first?.userName, "shared-user-copy-2")
        XCTAssertTrue(copyResolution.payloads[0].raw.contains("shared-cluster-copy-2"))
        XCTAssertTrue(copyResolution.payloads[0].raw.contains("shared-user-copy-2"))
    }

    func testSkipPolicyDoesNotFlagReferenceToLoadedUserWhenPayloadDoesNotRedefineIt() throws {
        let transaction = KubeConfigImportTransaction(
            payloads: [
                .init(
                    raw: kubeConfigReferencingLoadedUser,
                    sourceName: "incoming.yaml",
                    sourceURL: nil
                )
            ],
            logLabel: "syntheticReference",
            existingNames: KubeConfigNameRegistry(userNames: ["shared-user"]),
            validator: KubeConfigImportValidator(),
            resolver: KubeConfigDuplicateResolver()
        )

        let resolution = try transaction.resolvingDuplicates(
            choice: .skipDuplicate,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )

        let review = try XCTUnwrap(resolution.reviews.first)
        XCTAssertTrue(review.isValid)
        XCTAssertNil(review.issues.first { $0.id == "skip-duplicate-dependent-name-conflict" })
        XCTAssertEqual(review.contexts.first?.userName, "shared-user")
    }

    func testReviewAggregatorIncludesEveryFileWithoutLeakingSecrets() throws {
        let resolution = try makeTransaction().resolvingDuplicates(
            choice: .importAsCopy,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )

        let aggregate = try XCTUnwrap(KubeConfigImportReviewAggregator.aggregate(resolution.reviews))

        XCTAssertEqual(aggregate.contexts.count, 2)
        XCTAssertEqual(aggregate.sourceName, "2 kubeconfig files")
        XCTAssertTrue(aggregate.redactedPreview.contains("# first.yaml"))
        XCTAssertTrue(aggregate.redactedPreview.contains("# second.yaml"))
        XCTAssertFalse(aggregate.redactedPreview.contains("synthetic-token"))
        XCTAssertTrue(aggregate.hasDuplicateConflicts)
        XCTAssertEqual(aggregate.duplicateHandlingChoices, KubeConfigDuplicateHandlingChoice.allCases)
    }

    func testDuplicatePoliciesReserveDefinitionsNotReferencedByEarlierContexts() throws {
        let firstRaw = """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://first.example.invalid
        - name: reserved-cluster
          cluster:
            server: https://unused.example.invalid
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
            token: synthetic-token
        - name: reserved-user
          user:
            token: synthetic-unused-token
        """
        let transaction = KubeConfigImportTransaction(
            payloads: [
                .init(raw: firstRaw, sourceName: "first.yaml", sourceURL: nil),
                .init(
                    raw: partialCollisionKubeConfig(clusterName: "reserved-cluster", userName: "reserved-user"),
                    sourceName: "second.yaml",
                    sourceURL: nil
                ),
            ],
            logLabel: "syntheticUnusedDefinitions",
            existingNames: KubeConfigNameRegistry(),
            validator: KubeConfigImportValidator(),
            resolver: KubeConfigDuplicateResolver()
        )

        let skipped = try transaction.resolvingDuplicates(
            choice: .skipDuplicate,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )
        XCTAssertFalse(skipped.reviews[1].isValid)
        XCTAssertTrue(skipped.reviews[1].issues.contains { $0.id == "skip-duplicate-dependent-name-conflict" })

        let resolution = try transaction.resolvingDuplicates(
            choice: .importAsCopy,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator()
        )

        XCTAssertTrue(resolution.reviews.allSatisfy(\.isValid))
        XCTAssertEqual(resolution.reviews[1].contexts.first?.clusterName, "reserved-cluster-copy-2")
        XCTAssertEqual(resolution.reviews[1].contexts.first?.userName, "reserved-user-copy-2")
        XCTAssertEqual(resolution.reviews[1].contexts.first?.serverHost, "incoming.example.invalid")
        XCTAssertTrue(resolution.reviews[1].hasDuplicateConflicts)
        XCTAssertEqual(resolution.contextNamesForPreferences, ["synthetic-context", "incoming-context"])
    }

    func testResolutionPreservesErrorsForPayloadsThatFailedNamePreflight() {
        let fixtures: [(String, KubeConfigDuplicateResolutionError)] = [
            ("clusters: [", .malformedKubeConfig),
            ("{}\n---\n{}", .multipleDocuments),
        ]
        for (raw, expectedError) in fixtures {
            let transaction = KubeConfigImportTransaction(
                payloads: [
                    .init(raw: kubeConfig(server: "first.example.invalid"), sourceName: "first.yaml", sourceURL: nil),
                    .init(raw: raw, sourceName: "invalid.yaml", sourceURL: nil),
                ],
                logLabel: "syntheticInvalidDefinitions",
                existingNames: KubeConfigNameRegistry(),
                validator: KubeConfigImportValidator(),
                resolver: KubeConfigDuplicateResolver()
            )

            XCTAssertEqual(transaction.reviews.count, 2)
            for choice in KubeConfigDuplicateHandlingChoice.allCases {
                XCTAssertThrowsError(try transaction.resolvingDuplicates(
                    choice: choice,
                    resolver: KubeConfigDuplicateResolver(),
                    validator: KubeConfigImportValidator()
                )) { error in
                    XCTAssertEqual(error as? KubeConfigDuplicateResolutionError, expectedError)
                }
            }
        }
    }

    func testChangingDuplicatePolicyDoesNotCarryForwardResolvedNames() throws {
        let transaction = makeTransaction()
        let originalReviews = transaction.reviews
        let choices: [KubeConfigDuplicateHandlingChoice] = [
            .importAsCopy, .skipDuplicate, .importAsCopy, .updateExisting, .importAsCopy,
        ]

        for choice in choices {
            let resolution = try transaction.resolvingDuplicates(
                choice: choice,
                resolver: KubeConfigDuplicateResolver(),
                validator: KubeConfigImportValidator()
            )

            let expectedNames: Set<String> = choice == .importAsCopy
                ? ["synthetic-context", "synthetic-context-copy-2"]
                : ["synthetic-context"]
            XCTAssertEqual(resolution.contextNamesForPreferences, expectedNames)
            XCTAssertEqual(Set(resolution.reviews.flatMap(\.contexts).map(\.name)), expectedNames)
            XCTAssertTrue(resolution.reviews.allSatisfy(\.isValid))
            XCTAssertEqual(transaction.reviews, originalReviews)
        }
    }

    func testResolutionRevalidatesExecutableAvailabilityAfterPreflight() throws {
        let raw = kubeConfig(server: "first.example.invalid").replacingOccurrences(
            of: "token: synthetic-token",
            with: "exec:\n      command: synthetic-auth-plugin"
        )
        let transaction = KubeConfigImportTransaction(
            payloads: [.init(raw: raw, sourceName: "synthetic.yaml", sourceURL: nil)],
            logLabel: "syntheticExecutableAvailability",
            existingNames: KubeConfigNameRegistry(),
            validator: KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]),
            resolver: KubeConfigDuplicateResolver()
        )
        XCTAssertFalse(transaction.reviews.flatMap(\.issues).contains { $0.id.hasPrefix("missing-exec-plugin-") })

        let resolution = try transaction.resolvingDuplicates(
            choice: .importAsCopy,
            resolver: KubeConfigDuplicateResolver(),
            validator: KubeConfigImportValidator(fileExists: { _ in false }, executableSearchPaths: ["/synthetic/bin"])
        )

        XCTAssertTrue(resolution.reviews.flatMap(\.issues).contains { $0.id == "missing-exec-plugin-synthetic-context" })
    }

    private func makeTransaction() -> KubeConfigImportTransaction {
        let resolver = KubeConfigDuplicateResolver()
        return KubeConfigImportTransaction(
            payloads: [
                .init(raw: kubeConfig(server: "first.example.invalid"), sourceName: "first.yaml", sourceURL: nil),
                .init(raw: kubeConfig(server: "second.example.invalid"), sourceName: "second.yaml", sourceURL: nil)
            ],
            logLabel: "syntheticBatch",
            existingNames: KubeConfigNameRegistry(),
            validator: KubeConfigImportValidator(),
            resolver: resolver
        )
    }

    private func makePartialCollisionTransaction(
        clusterName: String,
        userName: String,
        existingNames: KubeConfigNameRegistry
    ) -> KubeConfigImportTransaction {
        KubeConfigImportTransaction(
            payloads: [
                .init(
                    raw: partialCollisionKubeConfig(clusterName: clusterName, userName: userName),
                    sourceName: "incoming.yaml",
                    sourceURL: nil
                )
            ],
            logLabel: "syntheticPartialCollision",
            existingNames: existingNames,
            validator: KubeConfigImportValidator(),
            resolver: KubeConfigDuplicateResolver()
        )
    }

    private func partialCollisionKubeConfig(clusterName: String, userName: String) -> String {
        """
        apiVersion: v1
        kind: Config
        current-context: incoming-context
        clusters:
        - name: \(clusterName)
          cluster:
            server: https://incoming.example.invalid
        contexts:
        - name: incoming-context
          context:
            cluster: \(clusterName)
            user: \(userName)
        users:
        - name: \(userName)
          user:
            token: synthetic-incoming-token
        """
    }

    private var kubeConfigReferencingLoadedUser: String {
        """
        apiVersion: v1
        kind: Config
        current-context: incoming-context
        clusters:
        - name: incoming-cluster
          cluster:
            server: https://incoming.example.invalid
        contexts:
        - name: incoming-context
          context:
            cluster: incoming-cluster
            user: shared-user
        users: []
        """
    }

    private func kubeConfig(server: String) -> String {
        """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://\(server)
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
            token: synthetic-token
        """
    }
}
