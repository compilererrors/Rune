import XCTest
@testable import RuneCore
@testable import RuneSecurity
@testable import RuneUI

final class AddClusterCloudImportWorkflowTests: XCTestCase {
    func testProjectsProviderStatusesWithoutViewModelState() {
        XCTAssertEqual(AddClusterCloudImportWorkflow.runningStatus(for: .eks), "Running EKS import...")
        XCTAssertEqual(AddClusterCloudImportWorkflow.importedStatus(for: .aks), "Imported AKS kubeconfig context.")
        XCTAssertEqual(AddClusterCloudImportWorkflow.failedStatus(), "Cloud import failed.")
        XCTAssertEqual(
            AddClusterCloudImportWorkflow.nativeReadyForReviewStatus(for: .eks),
            "Amazon EKS cluster access is ready for review."
        )
        XCTAssertEqual(
            AddClusterCloudImportWorkflow.nativeCancelledStatus(for: .gke),
            "Google GKE import cancelled."
        )
    }

    func testNativeFailureChecksNeverReferToProviderCLI() throws {
        for provider in [CloudKubeConfigProvider.aks, .eks, .gke] {
            let check = try XCTUnwrap(
                AddClusterCloudImportWorkflow.nativeImportFailureChecks(for: provider).first
            )
            XCTAssertFalse(check.message.localizedCaseInsensitiveContains("CLI"))
            XCTAssertTrue(check.message.localizedCaseInsensitiveContains("Native"))
        }
    }

    func testNativeImportDiagnosticClassifiesProviderFailures() {
        let aks = AddClusterCloudImportWorkflow.nativeDiagnostic(
            for: AKSNativeClusterImportError.clusterRequestFailed(
                statusCode: 403,
                code: "SyntheticAuthorizationFailure"
            ),
            provider: .aks
        )
        let eks = AddClusterCloudImportWorkflow.nativeDiagnostic(
            for: AWSEKSClusterImportError.accessDenied,
            provider: .eks
        )
        let gke = AddClusterCloudImportWorkflow.nativeDiagnostic(
            for: GKENativeClusterImportError.requestRejected(403),
            provider: .gke
        )
        let aksUnauthorized = AddClusterCloudImportWorkflow.nativeDiagnostic(
            for: AKSNativeClusterImportError.clusterRequestFailed(
                statusCode: 401,
                code: "SyntheticUnauthorized"
            ),
            provider: .aks
        )
        let entraThrottled = AddClusterCloudImportWorkflow.nativeDiagnostic(
            for: AKSNativeClusterImportError.authenticationFailed(
                statusCode: 429,
                code: "SyntheticThrottled"
            ),
            provider: .aks
        )
        let googleUnavailable = AddClusterCloudImportWorkflow.nativeDiagnostic(
            for: GCPServiceAccountAuthError.tokenEndpointRejected(503),
            provider: .gke
        )

        XCTAssertEqual(aks.title, "AKS permission denied")
        XCTAssertEqual(aks.classification, "Authorization failed")
        XCTAssertTrue(aks.nextAction.contains("AKS Cluster User"))
        XCTAssertTrue(aks.operationShape.contains("Azure Resource Manager HTTPS API"))
        XCTAssertEqual(eks.title, "EKS permission denied")
        XCTAssertTrue(eks.nextAction.contains("eks:DescribeCluster"))
        XCTAssertTrue(eks.operationShape.contains("Amazon EKS HTTPS API"))
        XCTAssertEqual(gke.title, "GKE permission denied")
        XCTAssertTrue(gke.nextAction.contains("container.clusters.get"))
        XCTAssertTrue(gke.operationShape.contains("GKE HTTPS API"))
        XCTAssertEqual(aksUnauthorized.title, "Azure authentication failed")
        XCTAssertEqual(aksUnauthorized.classification, "Authentication failed")
        XCTAssertFalse(aksUnauthorized.nextAction.contains("AKS Cluster User"))
        XCTAssertEqual(entraThrottled.title, "Provider request throttled")
        XCTAssertEqual(entraThrottled.classification, "HTTP 429")
        XCTAssertEqual(googleUnavailable.title, "Provider temporarily unavailable")
        XCTAssertEqual(googleUnavailable.classification, "HTTP 503")
        XCTAssertTrue([aks, eks, gke].allSatisfy { !$0.operationShape.contains("<secret>") })
    }

    func testNativeImportDiagnosticRejectsSensitiveErrorPayloads() {
        let sensitiveValue = "synthetic-sensitive-provider-payload"
        let diagnostics = [
            AddClusterCloudImportWorkflow.nativeDiagnostic(
                for: AKSNativeClusterImportError.clusterRequestFailed(
                    statusCode: 9_999,
                    code: sensitiveValue
                ),
                provider: .aks
            ),
            AddClusterCloudImportWorkflow.nativeDiagnostic(
                for: AWSEKSNativeAuthError.unsupportedOption(sensitiveValue),
                provider: .eks
            ),
            AddClusterCloudImportWorkflow.nativeDiagnostic(
                for: GCPServiceAccountAuthError.missingRequiredField(sensitiveValue),
                provider: .gke
            ),
            AddClusterCloudImportWorkflow.nativeDiagnostic(
                for: SyntheticSensitiveNativeImportError(message: sensitiveValue),
                provider: .gke
            )
        ]
        let rendered = diagnostics.map { diagnostic in
            [
                diagnostic.title,
                diagnostic.classification,
                diagnostic.message,
                diagnostic.operationShape,
                diagnostic.nextAction,
                diagnostic.documentationTitle,
                diagnostic.documentationURL.absoluteString
            ].joined(separator: "\n")
        }.joined(separator: "\n")

        XCTAssertFalse(rendered.contains(sensitiveValue))
        XCTAssertEqual(diagnostics[0].classification, "HTTP 0")
        XCTAssertEqual(diagnostics[3].title, "Native import failed")
    }

    func testCloudImportDiagnosticClassifiesCommonProviderFailures() {
        let failed = AddClusterCloudImportWorkflow.diagnostic(
            for: CloudKubeConfigImportError.commandFailed(
                command: "aws eks update-kubeconfig --region synthetic-region --name synthetic-cluster",
                exitCode: 17,
                message: "synthetic provider stderr"
            ),
            provider: .eks
        )
        let timedOut = AddClusterCloudImportWorkflow.diagnostic(
            for: CloudKubeConfigImportError.commandTimedOut(
                command: "az aks get-credentials --resource-group synthetic-group --name synthetic-cluster",
                timeoutSeconds: 7,
                message: "synthetic timeout stderr"
            ),
            provider: .aks
        )
        let missingKubeconfig = AddClusterCloudImportWorkflow.diagnostic(
            for: CloudKubeConfigImportError.noKubeconfigDiscovered(
                command: "gcloud container clusters get-credentials synthetic-cluster --location synthetic-location --project synthetic-project"
            ),
            provider: .gke
        )
        let unavailable = AddClusterCloudImportWorkflow.diagnostic(
            for: CloudKubeConfigImportError.externalCommandsUnavailable(
                message: RuneExternalCommandPolicy.disabledMessage
            ),
            provider: .gke
        )

        XCTAssertEqual(failed.title, "Provider CLI failed")
        XCTAssertEqual(failed.classification, "Exit code 17")
        XCTAssertEqual(failed.documentationTitle, "EKS Login Docs")
        XCTAssertEqual(timedOut.title, "Provider CLI timed out")
        XCTAssertEqual(timedOut.classification, "Timeout after 7s")
        XCTAssertEqual(timedOut.documentationTitle, "AKS Login Docs")
        XCTAssertEqual(missingKubeconfig.title, "No kubeconfig found")
        XCTAssertEqual(missingKubeconfig.classification, "No kubeconfig discovered")
        XCTAssertEqual(missingKubeconfig.documentationTitle, "GKE Login Docs")
        XCTAssertEqual(unavailable.title, "Provider CLI unavailable")
        XCTAssertEqual(unavailable.classification, "CLI unavailable")
        XCTAssertTrue(unavailable.nextAction.contains("optional credentials"))
    }

    func testCloudImportDiagnosticExplainsMissingProviderCLI() {
        for provider in [CloudKubeConfigProvider.aks, .eks, .gke] {
            let diagnostic = AddClusterCloudImportWorkflow.diagnostic(
                for: CloudKubeConfigImportError.commandFailed(
                    command: "synthetic command",
                    exitCode: 127,
                    message: "Required provider CLI not found: synthetic"
                ),
                provider: provider
            )

            XCTAssertEqual(diagnostic.title, "Provider CLI not installed")
            XCTAssertEqual(diagnostic.classification, "CLI not found")
            XCTAssertTrue(diagnostic.nextAction.contains("Help & tools"))
            XCTAssertFalse(diagnostic.message.contains("synthetic"))
        }
    }

    func testCloudImportDiagnosticClassifiesAKSAuthorizationFailures() {
        let diagnostic = AddClusterCloudImportWorkflow.diagnostic(
            for: CloudKubeConfigImportError.commandFailed(
                command: "az aks get-credentials --resource-group synthetic-group --name synthetic-cluster",
                exitCode: 1,
                message: "ERROR: (AuthorizationFailed) The client does not have authorization to perform action 'Microsoft.ContainerService/managedClusters/listClusterUserCredential/action' over scope '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/synthetic-group/providers/Microsoft.ContainerService/managedClusters/synthetic-cluster'."
            ),
            provider: .aks
        )

        XCTAssertEqual(diagnostic.title, "Azure authorization failed")
        XCTAssertEqual(diagnostic.classification, "AKS permission denied")
        XCTAssertTrue(diagnostic.message.contains("cannot fetch AKS user credentials"))
        XCTAssertEqual(diagnostic.nextAction, "Ask for AKS Cluster User access on this cluster, then retry import.")
        XCTAssertFalse(diagnostic.message.contains("00000000"))
        XCTAssertFalse(diagnostic.nextAction.contains("synthetic-cluster"))
    }

    func testCloudImportDiagnosticClassifiesExpiredAzureSignInWithoutLeakingProviderMetadata() {
        let sensitiveTenant = "00000000-0000-0000-0000-000000000000"
        let diagnostic = AddClusterCloudImportWorkflow.diagnostic(
            for: CloudKubeConfigImportError.commandFailed(
                command: "az aks get-credentials --resource-group synthetic-group --name synthetic-cluster",
                exitCode: 1,
                message: "AADSTS50173: The provided grant has expired; a fresh auth token is needed. Run az login --tenant \(sensitiveTenant)."
            ),
            provider: .aks
        )

        XCTAssertEqual(diagnostic.title, "Azure sign-in expired")
        XCTAssertEqual(diagnostic.classification, "Sign-in required")
        XCTAssertEqual(diagnostic.recoveryAction, .signInToAzure)
        XCTAssertTrue(diagnostic.nextAction.contains("retry this AKS import automatically"))
        XCTAssertFalse(diagnostic.message.contains(sensitiveTenant))
        XCTAssertFalse(diagnostic.nextAction.contains(sensitiveTenant))
    }

    func testCloudImportDiagnosticClassifiesEKSAndGKEFailuresPrecisely() {
        let eksAuth = cliDiagnostic(.eks, output: "ExpiredToken: The security token included in the request is expired")
        let eksPermission = cliDiagnostic(.eks, output: "AccessDeniedException: not authorized to perform eks:DescribeCluster")
        let eksMissing = cliDiagnostic(.eks, output: "ResourceNotFoundException: No cluster found")
        let gkeAuth = cliDiagnostic(.gke, output: "invalid_grant: reauthentication is needed")
        let gkePermission = cliDiagnostic(.gke, output: "PERMISSION_DENIED: required 'container.clusters.get' permission")
        let gkeMissing = cliDiagnostic(.gke, output: "NOT_FOUND: cluster not found")

        XCTAssertEqual(eksAuth.classification, "Authentication failed")
        XCTAssertEqual(eksPermission.title, "EKS permission denied")
        XCTAssertEqual(eksMissing.classification, "Cluster not found")
        XCTAssertEqual(gkeAuth.title, "Google authentication failed")
        XCTAssertEqual(gkePermission.title, "GKE permission denied")
        XCTAssertEqual(gkeMissing.classification, "Cluster not found")
        XCTAssertTrue([eksAuth, eksPermission, eksMissing, gkeAuth, gkePermission, gkeMissing].allSatisfy {
            !$0.message.localizedCaseInsensitiveContains("synthetic-sensitive")
        })
    }

    func testSheetOwnedDiagnosticsCoverSignInAndBlockedReviewWithoutRawErrors() {
        let signIn = AddClusterCloudImportWorkflow.azureSignInDiagnostic(
            for: AzureCLISignInError.timedOut(timeoutSeconds: 600)
        )
        let blocked = AddClusterCloudImportWorkflow.blockedReviewDiagnostic(
            for: .gke,
            failure: AddClusterImportReviewFailure(
                message: "Kubeconfig is missing current-context.",
                checks: []
            )
        )

        XCTAssertEqual(signIn.title, "Azure sign-in timed out")
        XCTAssertEqual(signIn.recoveryAction, .signInToAzure)
        XCTAssertEqual(blocked.title, "Kubeconfig was rejected")
        XCTAssertEqual(blocked.classification, "Import review failed")
        XCTAssertTrue(blocked.message.contains("current-context"))
    }

    func testCloudImportDiagnosticUsesSafeCommandShapeWithoutRawProviderOutput() {
        let rawCommand = "aws eks update-kubeconfig --region private-region --name private-cluster --profile private-profile --role-arn arn:aws:iam::123456789012:role/private"
        let rawOutput = "token=synthetic-secret server=https://private.example.invalid kubeconfig=/private/tmp/config"
        let diagnostic = AddClusterCloudImportWorkflow.diagnostic(
            for: CloudKubeConfigImportError.commandFailed(
                command: rawCommand,
                exitCode: 42,
                message: rawOutput
            ),
            provider: .eks
        )
        let rendered = [
            diagnostic.title,
            diagnostic.classification,
            diagnostic.message,
            diagnostic.operationShape,
            diagnostic.nextAction,
            diagnostic.documentationTitle,
            diagnostic.documentationURL.absoluteString
        ].joined(separator: "\n")

        XCTAssertTrue(diagnostic.operationShape.contains("<cluster-name>"))
        XCTAssertTrue(diagnostic.operationShape.contains("<region>"))
        XCTAssertFalse(rendered.contains(rawCommand))
        XCTAssertFalse(rendered.contains(rawOutput))
        XCTAssertFalse(rendered.contains("private-cluster"))
        XCTAssertFalse(rendered.contains("private-profile"))
        XCTAssertFalse(rendered.contains("arn:aws"))
        XCTAssertFalse(rendered.contains("synthetic-secret"))
        XCTAssertFalse(rendered.contains("private.example.invalid"))
        XCTAssertFalse(rendered.contains("/private/tmp"))
    }

    func testBlockingIssuesUseOnlyErrorsAndPreserveReviewOrder() {
        let reviews = [
            review(issues: [
                issue("warning-only", .warning),
                issue("missing-current-context", .error, message: "Current context is missing.")
            ]),
            review(issues: [
                issue("missing-server-synthetic", .error, message: "Cluster server is missing.")
            ])
        ]

        let blocking = AddClusterCloudImportWorkflow.blockingIssues(in: reviews)

        XCTAssertEqual(blocking.map(\.id), ["missing-current-context", "missing-server-synthetic"])
        XCTAssertEqual(
            AddClusterCloudImportWorkflow.blockingImportErrorMessage(for: blocking),
            "Kubeconfig is missing current-context. A kubeconfig context is missing a valid cluster server URL."
        )
    }

    func testBlockingFailureProjectsBoundedChecksAndMessageInOnePass() throws {
        let reviews = [
            review(issues: [
                issue("warning-only", .warning),
                issue("missing-current-context", .error, message: "Current context is missing."),
                issue("missing-server-synthetic", .error, message: "Cluster server is missing.")
            ]),
            review(issues: (0..<6).map { index in
                issue("synthetic-\(index)", .error, message: "Synthetic import issue \(index)")
            })
        ]

        let failure = try XCTUnwrap(AddClusterCloudImportWorkflow.blockingFailure(in: reviews))

        XCTAssertEqual(
            failure.message,
            "Kubeconfig is missing current-context. A kubeconfig context is missing a valid cluster server URL. Kubeconfig import review found a blocking issue."
        )
        XCTAssertEqual(failure.checks.count, 6)
        XCTAssertEqual(failure.checks.dropFirst().map(\.id), [
            "kubeconfig-import-missing-current-context",
            "kubeconfig-import-missing-server-synthetic",
            "kubeconfig-import-synthetic-0",
            "kubeconfig-import-synthetic-1",
            "kubeconfig-import-synthetic-2"
        ])
        XCTAssertFalse(failure.checks.map(\.message).joined(separator: " ").contains("Synthetic import issue"))
    }

    func testBlockingFailureReturnsNilForWarningOnlyReviews() {
        let failure = AddClusterCloudImportWorkflow.blockingFailure(in: [
            review(issues: [
                issue("warning-only", .warning)
            ])
        ])

        XCTAssertNil(failure)
    }

    func testImportReviewFailureChecksStayBoundedAndPrivacySafe() {
        let issues = (0..<7).map { index in
            issue("synthetic-\(index)", .error, message: "Synthetic import issue \(index)")
        }

        let checks = AddClusterCloudImportWorkflow.importReviewFailureChecks(for: issues)

        XCTAssertEqual(checks.count, 6)
        XCTAssertEqual(checks.first?.id, "kubeconfig-import")
        XCTAssertEqual(checks.dropFirst().map(\.id), (0..<5).map { "kubeconfig-import-synthetic-\($0)" })
        XCTAssertTrue(checks.allSatisfy { $0.status == .failed })
        XCTAssertFalse(checks.map(\.message).joined(separator: " ").contains("Synthetic import issue"))
    }

    func testImportReviewFailureChecksHashUnsafeIssueIDs() {
        let unsafeID = "/private/tmp/token-secret/current-context"
        let checks = AddClusterCloudImportWorkflow.importReviewFailureChecks(for: [
            issue(unsafeID, .error, message: "Synthetic import issue")
        ])

        let checkID = checks[1].id
        XCTAssertTrue(checkID.hasPrefix("kubeconfig-import-issue-"))
        XCTAssertFalse(checkID.contains("/private"))
        XCTAssertFalse(checkID.contains("token-secret"))
        XCTAssertFalse(checkID.contains("current-context"))
        XCTAssertEqual(
            checkID,
            AddClusterCloudImportWorkflow.importReviewFailureChecks(for: [
                issue(unsafeID, .error, message: "Synthetic import issue")
            ])[1].id
        )
    }

    func testBlockingFailureHashesUnsafeIDsAndBoundsMessagesAcrossReviews() throws {
        let unsafeID = "/private/tmp/token-secret/current-context"
        let reviews = [
            review(issues: [
                issue(unsafeID, .error, message: "First synthetic import issue."),
                issue("safe-id", .warning, message: "Warning should not block.")
            ]),
            review(issues: [
                issue("cluster-server-url", .error, message: "Second synthetic import issue."),
                issue(String(repeating: "long-sensitive-id-", count: 8), .error, message: "Third synthetic import issue."),
                issue("fourth", .error, message: "Fourth synthetic import issue.")
            ])
        ]

        let failure = try XCTUnwrap(AddClusterCloudImportWorkflow.blockingFailure(in: reviews))
        let joinedIDs = failure.checks.map(\.id).joined(separator: " ")
        let joinedMessages = failure.checks.map(\.message).joined(separator: " ")

        XCTAssertEqual(failure.checks.count, 5)
        XCTAssertEqual(
            failure.message,
            "Kubeconfig import review found a blocking issue. Kubeconfig import review found a blocking issue. Kubeconfig import review found a blocking issue."
        )
        XCTAssertTrue(failure.checks.contains { $0.id.hasPrefix("kubeconfig-import-issue-") })
        XCTAssertTrue(failure.checks.contains { $0.id == "kubeconfig-import-cluster-server-url" })
        XCTAssertFalse(joinedIDs.contains("/private"))
        XCTAssertFalse(joinedIDs.contains("token-secret"))
        XCTAssertFalse(joinedIDs.contains("current-context"))
        XCTAssertFalse(joinedIDs.contains("long-sensitive-id"))
        XCTAssertFalse(joinedMessages.contains("Warning should not block."))
        XCTAssertFalse(joinedMessages.contains("synthetic import issue"))
    }

    func testImportReviewFailureChecksHashLongIssueIDs() {
        let longID = String(repeating: "synthetic-long-id-", count: 8)
        let checks = AddClusterCloudImportWorkflow.importReviewFailureChecks(for: [
            issue(longID, .error, message: "Synthetic import issue")
        ])

        let checkID = checks[1].id
        XCTAssertTrue(checkID.hasPrefix("kubeconfig-import-issue-"))
        XCTAssertLessThan(checkID.count, longID.count)
    }

    func testCloudLoginFailureCheckIsScopedToProvider() {
        let checks = AddClusterCloudImportWorkflow.cloudLoginFailureChecks(for: .gke)

        XCTAssertEqual(checks.count, 1)
        XCTAssertEqual(checks.first?.id, "cloud-login-gke")
        XCTAssertEqual(checks.first?.title, "GKE cloud login")
        XCTAssertEqual(checks.first?.status, .failed)
        XCTAssertTrue(checks.first?.message.contains("provider CLI") == true)
    }

    func testCloudLoginFailureChecksUseStableProviderIDsAndTitles() {
        XCTAssertEqual(
            AddClusterCloudImportWorkflow.cloudLoginFailureChecks(for: .aks).first?.id,
            "cloud-login-aks"
        )
        XCTAssertEqual(
            AddClusterCloudImportWorkflow.cloudLoginFailureChecks(for: .aks).first?.title,
            "AKS cloud login"
        )
        XCTAssertEqual(
            AddClusterCloudImportWorkflow.cloudLoginFailureChecks(for: .eks).first?.id,
            "cloud-login-eks"
        )
        XCTAssertEqual(
            AddClusterCloudImportWorkflow.cloudLoginFailureChecks(for: .eks).first?.title,
            "EKS cloud login"
        )
        XCTAssertEqual(
            AddClusterCloudImportWorkflow.cloudLoginFailureChecks(for: .gke).first?.id,
            "cloud-login-gke"
        )
        XCTAssertEqual(
            AddClusterCloudImportWorkflow.cloudLoginFailureChecks(for: .gke).first?.title,
            "GKE cloud login"
        )
    }

    private func review(issues: [KubeConfigImportIssue]) -> KubeConfigImportReview {
        KubeConfigImportReview(contexts: [], issues: issues, redactedPreview: "")
    }

    private func cliDiagnostic(
        _ provider: CloudKubeConfigProvider,
        output: String
    ) -> AddClusterCloudImportDiagnostic {
        AddClusterCloudImportWorkflow.diagnostic(
            for: CloudKubeConfigImportError.commandFailed(
                command: "synthetic command",
                exitCode: 1,
                message: output
            ),
            provider: provider
        )
    }

    private func issue(
        _ id: String,
        _ severity: KubeConfigImportIssueSeverity,
        message: String? = nil
    ) -> KubeConfigImportIssue {
        KubeConfigImportIssue(
            id: id,
            severity: severity,
            message: message ?? "Issue \(id)"
        )
    }
}

private struct SyntheticSensitiveNativeImportError: LocalizedError {
    let message: String

    var errorDescription: String? { message }
}
