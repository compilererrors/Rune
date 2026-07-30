import AppKit
import Combine
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneDiagnostics
@testable import RuneExport
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

final class RuneAppStateTests: XCTestCase {
    @MainActor
    func testUserDefaultsNotificationsPostedOffMainThreadAreDeliveredSafely() async {
        let viewModel = RuneAppViewModel()

        await Task.detached {
            NotificationCenter.default.post(
                name: UserDefaults.didChangeNotification,
                object: UserDefaults.standard
            )
        }.value

        await withCheckedContinuation { continuation in
            DispatchQueue.main.async {
                continuation.resume()
            }
        }
        withExtendedLifetime(viewModel) {}
    }

    @MainActor
    func testResourceSettersDoNotPublishWhenValuesAreUnchanged() {
        let state = RuneAppState()
        let pod = PodSummary(
            name: "api-0",
            namespace: "default",
            status: "Running",
            containersReady: "1/1",
            containerNamesLine: "app"
        )
        let deployment = DeploymentSummary(
            name: "api",
            namespace: "default",
            readyReplicas: 1,
            desiredReplicas: 1,
            selector: ["app": "api"]
        )

        state.setPods([pod])
        state.setDeployments([deployment])
        state.setOverviewSnapshot(
            pods: [pod],
            deploymentsCount: 1,
            servicesCount: 1,
            ingressesCount: 0,
            configMapsCount: 0,
            cronJobsCount: 0,
            nodesCount: 1,
            clusterCPUPercent: 20,
            clusterMemoryPercent: 30,
            events: []
        )

        var publishCount = 0
        let cancellable = state.objectWillChange.sink { _ in
            publishCount += 1
        }
        defer { cancellable.cancel() }

        state.setPods([pod])
        state.setDeployments([deployment])
        state.setOverviewSnapshot(
            pods: [pod],
            deploymentsCount: 1,
            servicesCount: 1,
            ingressesCount: 0,
            configMapsCount: 0,
            cronJobsCount: 0,
            nodesCount: 1,
            clusterCPUPercent: 20,
            clusterMemoryPercent: 30,
            events: []
        )

        XCTAssertEqual(publishCount, 0)
    }

    @MainActor
    func testYAMLValidationAndEmptyFreshnessMutationsPublishOnlyRealChanges() {
        let state = RuneAppState()
        var publishCount = 0
        let cancellable = state.objectWillChange.sink { _ in
            publishCount += 1
        }
        defer { cancellable.cancel() }

        state.clearResourceListFreshness()
        state.setResourceYAMLValidationIssues([])
        state.finishResourceYAMLValidation()
        XCTAssertEqual(publishCount, 0)

        let issue = YAMLValidationIssue(
            source: .syntax,
            severity: .error,
            message: "Synthetic YAML error"
        )
        state.beginResourceYAMLValidation()
        state.beginResourceYAMLValidation()
        state.setResourceYAMLValidationIssues([issue])
        state.setResourceYAMLValidationIssues([issue])
        state.finishResourceYAMLValidation()
        state.finishResourceYAMLValidation()

        XCTAssertEqual(publishCount, 3)
    }

    func testPodJSONEnrichmentIsSkippedWhenPodListAlreadyHasJSONDetails() {
        let detailedPods = [
            PodSummary(
                name: "api-0",
                namespace: "default",
                status: "Running",
                podIP: "10.0.0.12",
                nodeName: "node-a",
                qosClass: "Burstable",
                containersReady: "1/1",
                containerNamesLine: "app",
                labels: ["app": "api"],
                containerImagesLine: "example.invalid/app:v1",
                ownerReferencesLine: "ReplicaSet/api-123"
            )
        ]

        XCTAssertFalse(RuneAppViewModel.podListNeedsJSONEnrichment(detailedPods))
    }

    func testPodJSONEnrichmentStillRunsForSparsePodRows() {
        let sparsePods = [
            PodSummary(name: "api-0", namespace: "default", status: "Running")
        ]

        XCTAssertTrue(RuneAppViewModel.podListNeedsJSONEnrichment(sparsePods))
        XCTAssertFalse(RuneAppViewModel.podListNeedsJSONEnrichment([]))
    }

    func testKubeConfigDiscovererUsesKUBECONFIGAndDefaultPathWithoutWritingFiles() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.kubeconfigDiscovery.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let first = directory.appendingPathComponent("first.yaml")
        let second = directory.appendingPathComponent("second.yaml")
        let home = directory.appendingPathComponent("home", isDirectory: true)
        let defaultConfig = home.appendingPathComponent(".kube", isDirectory: true).appendingPathComponent("config")
        try FileManager.default.createDirectory(at: defaultConfig.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("apiVersion: v1\n".utf8).write(to: first)
        try Data("apiVersion: v1\n".utf8).write(to: second)
        try Data("apiVersion: v1\n".utf8).write(to: defaultConfig)

        let discoverer = KubeConfigDiscoverer(
            environmentProvider: { ["KUBECONFIG": "\(second.path):\(first.path):\(second.path)"] },
            homeDirectoryProvider: { home },
            fileExists: { FileManager.default.fileExists(atPath: $0) }
        )

        XCTAssertEqual(
            discoverer.discoverCandidateFiles().map(\.path),
            [second.path, first.path, defaultConfig.path]
        )
        XCTAssertTrue(FileManager.default.fileExists(atPath: defaultConfig.path))
    }

    func testKubeConfigDiscovererLetsRuneKubeconfigOverrideTerminalKubeconfig() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.runeKubeconfigDiscovery.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let runeFirst = directory.appendingPathComponent("rune-first.yaml")
        let runeSecond = directory.appendingPathComponent("rune-second.yaml")
        let terminalOnly = directory.appendingPathComponent("terminal-only.yaml")
        let home = directory.appendingPathComponent("home", isDirectory: true)
        let defaultConfig = home.appendingPathComponent(".kube", isDirectory: true).appendingPathComponent("config")
        try FileManager.default.createDirectory(at: defaultConfig.deletingLastPathComponent(), withIntermediateDirectories: true)
        for file in [runeFirst, runeSecond, terminalOnly, defaultConfig] {
            try Data("apiVersion: v1\n".utf8).write(to: file)
        }

        let discoverer = KubeConfigDiscoverer(
            environmentProvider: {
                [
                    "RUNE_KUBECONFIG": "\(runeSecond.path):\(runeFirst.path)",
                    "KUBECONFIG": terminalOnly.path
                ]
            },
            homeDirectoryProvider: { home },
            fileExists: { FileManager.default.fileExists(atPath: $0) }
        )

        XCTAssertEqual(
            discoverer.discoverCandidateFiles().map(\.path),
            [runeSecond.path, runeFirst.path, defaultConfig.path]
        )
        XCTAssertFalse(discoverer.discoverCandidateFiles().map(\.path).contains(terminalOnly.path))
    }

    func testKubeConfigImportValidatorBuildsRedactedReview() {
        let validator = KubeConfigImportValidator(fileExists: { $0.hasSuffix("/aws") }, executableSearchPaths: ["/synthetic/bin"])
        let review = validator.validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://example.invalid
                certificate-authority-data: public-ca
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
                namespace: default
            users:
            - name: user-alpha
              user:
                token: secret-token
                client-certificate-data: secret-cert
                client-key-data: secret-key
                extensions:
                - token: secret-list-token
                exec:
                  command: aws
            """
        )

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.map(\.name), ["context-alpha"])
        XCTAssertEqual(review.contexts.first?.serverHost, "example.invalid")
        XCTAssertEqual(review.contexts.first?.authType, "Exec plugin")
        XCTAssertEqual(review.contexts.first?.providerHint, "EKS")
        XCTAssertFalse(review.redactedPreview.contains("secret-token"))
        XCTAssertFalse(review.redactedPreview.contains("secret-list-token"))
        XCTAssertFalse(review.redactedPreview.contains("secret-cert"))
        XCTAssertFalse(review.redactedPreview.contains("secret-key"))
        XCTAssertTrue(review.redactedPreview.contains("token: <redacted>"))
        XCTAssertTrue(review.redactedPreview.contains("- token: <redacted>"))
        XCTAssertTrue(review.redactedPreview.contains("client-key-data: <redacted>"))
    }

    func testKubeConfigImportValidatorRedactsTokenBlockScalarContinuationLines() {
        let review = KubeConfigImportValidator().validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://example.invalid
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            users:
            - name: user-alpha
              user:
                token: |-
                  synthetic-block-secret-line-one
                  synthetic-block-secret-line-two
            """
        )

        XCTAssertFalse(review.redactedPreview.contains("synthetic-block-secret-line-one"))
        XCTAssertFalse(review.redactedPreview.contains("synthetic-block-secret-line-two"))
        XCTAssertTrue(review.redactedPreview.contains("<redacted>"))
    }

    func testKubeConfigImportValidatorRedactsTokenInsideFlowMapping() {
        let review = KubeConfigImportValidator().validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - name: cluster-alpha
              cluster: {server: https://example.invalid}
            contexts:
            - name: context-alpha
              context: {cluster: cluster-alpha, user: user-alpha}
            users:
            - name: user-alpha
              user: {token: synthetic-flow-secret}
            """
        )

        XCTAssertFalse(review.redactedPreview.contains("synthetic-flow-secret"))
        XCTAssertTrue(review.redactedPreview.contains("<redacted>"))
    }

    func testKubeConfigImportValidatorRedactsLocalCredentialPaths() {
        let review = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://example.invalid
                certificate-authority: /synthetic/kube/ca.crt
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            users:
            - name: user-alpha
              user:
                client-certificate: /synthetic/kube/client.crt
                client-key: /synthetic/kube/client.key
            """
        )

        XCTAssertTrue(review.isValid)
        XCTAssertFalse(review.redactedPreview.contains("/synthetic/kube/ca.crt"))
        XCTAssertFalse(review.redactedPreview.contains("/synthetic/kube/client.crt"))
        XCTAssertFalse(review.redactedPreview.contains("/synthetic/kube/client.key"))
        XCTAssertTrue(review.redactedPreview.contains("certificate-authority: <redacted>"))
        XCTAssertTrue(review.redactedPreview.contains("client-certificate: <redacted>"))
        XCTAssertTrue(review.redactedPreview.contains("client-key: <redacted>"))
    }

    func testKubeConfigImportValidatorReportsDuplicateContextAndMissingCurrentContext() {
        let review = KubeConfigImportValidator().validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://example.invalid
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            users:
            - name: user-alpha
              user:
                token: test-token
            """
        )

        XCTAssertFalse(review.isValid)
        XCTAssertTrue(review.issues.contains { $0.id == "missing-current-context" && $0.severity == .error })
        XCTAssertTrue(review.issues.contains { $0.id == "duplicate-context-context-alpha" && $0.severity == .error })
        XCTAssertEqual(
            review.duplicateHandlingChoices,
            [.updateExisting, .importAsCopy, .skipDuplicate]
        )
    }

    @MainActor
    func testKubeConfigDuplicateHandlingChoiceDefaultsToSkipAndCanChange() {
        let viewModel = RuneAppViewModel(state: RuneAppState())

        XCTAssertEqual(viewModel.kubeConfigDuplicateHandlingChoice, .skipDuplicate)

        viewModel.kubeConfigDuplicateHandlingChoice = .importAsCopy

        XCTAssertEqual(viewModel.kubeConfigDuplicateHandlingChoice, .importAsCopy)
    }

    func testKubeConfigImportValidatorReportsDuplicateClustersAndUsersWithoutCrashing() {
        let review = KubeConfigImportValidator().validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://first.example.invalid
            - name: cluster-alpha
              cluster:
                server: https://second.example.invalid
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            users:
            - name: user-alpha
              user:
                token: first-token
            - name: user-alpha
              user:
                token: second-token
            """
        )

        XCTAssertFalse(review.isValid)
        XCTAssertEqual(review.contexts.first?.serverHost, "first.example.invalid")
        XCTAssertTrue(review.issues.contains { $0.id == "duplicate-cluster-cluster-alpha" && $0.severity == .error })
        XCTAssertTrue(review.issues.contains { $0.id == "duplicate-user-user-alpha" && $0.severity == .error })
        XCTAssertFalse(review.redactedPreview.contains("first-token"))
        XCTAssertFalse(review.redactedPreview.contains("second-token"))
    }

    func testKubeConfigImportValidatorReportsMalformedYAML() {
        let review = KubeConfigImportValidator().validate(
            raw:
            """
            apiVersion: v1
            this line is not valid kubeconfig yaml
            current-context: context-alpha
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://example.invalid
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            users:
            - name: user-alpha
              user:
                token: test-token
            """
        )

        XCTAssertFalse(review.isValid)
        XCTAssertTrue(review.issues.contains { $0.id == "malformed-yaml" && $0.severity == .error })
    }

    func testKubeConfigImportValidatorWarnsForMissingExecPlugin() {
        let review = KubeConfigImportValidator(fileExists: { _ in false }, executableSearchPaths: ["/synthetic/bin"]).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://example.invalid
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            users:
            - name: user-alpha
              user:
                exec:
                  command: missing-plugin
            """
        )

        XCTAssertTrue(review.isValid)
        XCTAssertTrue(review.issues.contains { $0.id == "missing-exec-plugin-context-alpha" && $0.severity == .warning })
        XCTAssertEqual(review.contexts.first?.authType, "Exec plugin")
    }

    func testKubeConfigImportValidatorParsesTypeFirstClusterContextAndUser() {
        let review = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - cluster:
                server: https://example.invalid
              name: cluster-alpha
            users:
            - user:
                token: test-token
              name: user-alpha
            contexts:
            - context:
                cluster: cluster-alpha
                user: user-alpha
                namespace: k3s-namespace
              name: context-alpha
            """
        )

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.map(\.name), ["context-alpha"])
        XCTAssertEqual(review.contexts.first?.clusterName, "cluster-alpha")
        XCTAssertEqual(review.contexts.first?.userName, "user-alpha")
        XCTAssertEqual(review.contexts.first?.namespace, "k3s-namespace")
        XCTAssertEqual(review.contexts.first?.serverHost, "example.invalid")
        XCTAssertEqual(review.contexts.first?.authType, "Token")
        XCTAssertFalse(review.issues.contains { $0.id == "missing-contexts" })
    }

    func testKubeConfigImportValidatorParsesTypeFirstExecUserAndWarnsForMissingPlugin() {
        let review = KubeConfigImportValidator(fileExists: { _ in false }, executableSearchPaths: ["/synthetic/bin"]).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - cluster:
                server: https://eks.example.invalid
              name: cluster-alpha
            users:
            - user:
                exec:
                  command: aws
              name: user-alpha
            contexts:
            - context:
                cluster: cluster-alpha
                user: user-alpha
                namespace: eks-namespace
              name: context-alpha
            """
        )

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.first?.authType, "Exec plugin")
        XCTAssertEqual(review.contexts.first?.providerHint, "EKS")
        XCTAssertTrue(review.issues.contains { $0.id == "missing-exec-plugin-context-alpha" && $0.severity == .warning })
    }

    func testKubeConfigImportValidatorDoesNotTreatExecArgsOrEnvAsUsers() {
        let review = KubeConfigImportValidator(
            fileExists: { $0 == "/synthetic/bin/aws" },
            executableSearchPaths: ["/synthetic/bin"]
        ).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://eks.example.invalid
            users:
            - name: user-alpha
              user:
                exec:
                  apiVersion: client.authentication.k8s.io/v1
                  command: aws
                  args:
                    - eks
                    - get-token
                    - --region
                    - eu-north-1
                    - --cluster-name
                    - synthetic-eks
                  env:
                    - name: AWS_PROFILE
                      value: synthetic-profile
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
                namespace: eks-namespace
            """
        )

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.first?.authType, "Exec plugin")
        XCTAssertEqual(review.contexts.first?.providerHint, "EKS")
        XCTAssertFalse(review.issues.contains { $0.id == "user-missing-name" })
        XCTAssertFalse(review.issues.contains { $0.id == "unknown-user-context-alpha" })
    }

    func testKubeConfigImportValidatorHandlesCommentsBOMAndCRLFInTypeFirstItems() {
        let raw = "\u{FEFF}apiVersion: v1\r\nkind: Config\r\ncurrent-context: context-alpha\r\nclusters:\r\n- cluster: # generated by local tooling\r\n    server: https://example.invalid\r\n  name: cluster-alpha # sibling name after cluster\r\nusers:\r\n- user:\r\n    token: test-token\r\n  name: user-alpha\r\ncontexts:\r\n- context: # type-first context\r\n    cluster: cluster-alpha\r\n    user: user-alpha\r\n    namespace: default\r\n  name: context-alpha\r\n"

        let review = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(raw: raw)

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.first?.name, "context-alpha")
        XCTAssertEqual(review.contexts.first?.namespace, "default")
        XCTAssertEqual(review.contexts.first?.serverHost, "example.invalid")
    }

    func testKubeConfigImportValidatorHandlesQuotedScalarsInlineCommentsAndMultipleContexts() {
        let review = KubeConfigImportValidator(
            fileExists: { _ in true },
            executableSearchPaths: ["/synthetic/bin"]
        ).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: "context-beta" # selected by user
            clusters:
            - name: "cluster-alpha"
              cluster:
                server: "https://alpha.example.invalid:6443" # generated comment
            - name: 'cluster-beta'
              cluster:
                server: 'https://beta.example.invalid'
            contexts:
            - name: "context-beta"
              context:
                cluster: "cluster-beta"
                user: "user-beta"
                namespace: "team-beta"
            - name: 'context-alpha'
              context:
                cluster: 'cluster-alpha'
                user: 'user-alpha'
                namespace: 'team-alpha'
            users:
            - name: "user-alpha"
              user:
                tokenFile: "/synthetic/private/token-alpha.txt"
            - name: "user-beta"
              user:
                auth-provider:
                  name: oidc
                  config:
                    id-token: "synthetic-id-token-with#hash"
                    client-id: "synthetic-client-id"
            """
        )

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.map(\.name), ["context-alpha", "context-beta"])
        XCTAssertEqual(review.contexts.map(\.namespace), ["team-alpha", "team-beta"])
        XCTAssertEqual(review.contexts.map(\.serverHost), ["alpha.example.invalid", "beta.example.invalid"])
        XCTAssertEqual(review.contexts.map(\.authType), ["Token file", "OIDC"])
        XCTAssertEqual(review.contexts.last?.providerHint, "OIDC")
        XCTAssertFalse(review.issues.contains { $0.severity == .error })
        XCTAssertFalse(review.redactedPreview.contains("/synthetic/private/token-alpha.txt"))
        XCTAssertFalse(review.redactedPreview.contains("synthetic-id-token-with#hash"))
        XCTAssertTrue(review.redactedPreview.contains("tokenFile: <redacted>"))
        XCTAssertTrue(review.redactedPreview.contains("id-token: <redacted>"))
    }

    func testKubeConfigImportValidatorReportsMissingDelayedNames() {
        let review = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - cluster:
                server: https://example.invalid
            users:
            - user:
                token: test-token
            contexts:
            - context:
                cluster: cluster-alpha
                user: user-alpha
            """
        )

        XCTAssertFalse(review.isValid)
        XCTAssertTrue(review.issues.contains { $0.id == "cluster-missing-name" && $0.severity == .error })
        XCTAssertTrue(review.issues.contains { $0.id == "context-missing-name" && $0.severity == .error })
        XCTAssertTrue(review.issues.contains { $0.id == "user-missing-name" && $0.severity == .error })
    }

    func testKubeConfigImportValidatorReportsUnknownCurrentContext() {
        let review = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: missing-context
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://example.invalid
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            users:
            - name: user-alpha
              user:
                token: test-token
            """
        )

        XCTAssertFalse(review.isValid)
        XCTAssertTrue(review.issues.contains { $0.id == "missing-current-context-reference" && $0.severity == .error })
    }

    func testKubeConfigImportValidatorResolvesYAMLAnchorsAliasesAndMergeKeys() {
        let scalarAliasReview = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            x-server: &server https://example.invalid
            current-context: context-alpha
            clusters:
            - cluster:
                server: *server
              name: cluster-alpha
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            users:
            - name: user-alpha
              user:
                token: test-token
            """
        )

        let mergeReview = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            x-cluster-base: &clusterBase
              server: https://merged.example.invalid
            x-context-base: &contextBase
              cluster: cluster-alpha
              user: user-alpha
              namespace: merged-namespace
            x-user-base: &userBase
              token: merged-token
            current-context: context-alpha
            clusters:
            - cluster:
                <<: *clusterBase
              name: cluster-alpha
            contexts:
            - name: context-alpha
              context:
                <<: [*contextBase]
            users:
            - name: user-alpha
              user:
                <<: *userBase
            """
        )

        let listAliasReview = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
            raw:
            """
            apiVersion: v1
            kind: Config
            x-cluster-entry: &clusterEntry
              name: cluster-alpha
              cluster:
                server: https://list-alias.example.invalid
            x-context-entry: &contextEntry
              name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
                namespace: alias-namespace
            x-user-entry: &userEntry
              name: user-alpha
              user:
                exec:
                  command: synthetic-auth
            current-context: context-alpha
            clusters:
            - *clusterEntry
            contexts:
            - *contextEntry
            users:
            - *userEntry
            """
        )

        XCTAssertTrue(scalarAliasReview.isValid)
        XCTAssertEqual(scalarAliasReview.contexts.first?.serverHost, "example.invalid")
        XCTAssertFalse(scalarAliasReview.issues.contains { $0.id == "unsupported-yaml-feature" })

        XCTAssertTrue(mergeReview.isValid)
        XCTAssertEqual(mergeReview.contexts.first?.serverHost, "merged.example.invalid")
        XCTAssertEqual(mergeReview.contexts.first?.namespace, "merged-namespace")
        XCTAssertEqual(mergeReview.contexts.first?.authType, "Token")
        XCTAssertFalse(mergeReview.redactedPreview.contains("merged-token"))

        XCTAssertTrue(listAliasReview.isValid)
        XCTAssertEqual(listAliasReview.contexts.first?.namespace, "alias-namespace")
        XCTAssertEqual(listAliasReview.contexts.first?.serverHost, "list-alias.example.invalid")
        XCTAssertEqual(listAliasReview.contexts.first?.authType, "Exec plugin")
        XCTAssertFalse(listAliasReview.issues.contains { $0.id == "unsupported-yaml-feature" })
    }

    func testKubeConfigImportValidatorRecognizesSupportedProviderKubeconfigShapes() {
        let cases: [(context: String, server: String, command: String?, expectedHint: String?)] = [
            ("synthetic-eks", "https://eks.example.invalid", "aws", "EKS"),
            ("synthetic-aks", "https://aks.example.invalid", "kubelogin", "AKS"),
            ("synthetic-gke", "https://gke.example.invalid", "gke-gcloud-auth-plugin", "GKE"),
            ("synthetic-doks", "https://doks.example.invalid", "doctl", "DOKS"),
            ("synthetic-rancher", "https://rancher.example.invalid", nil, "Rancher"),
            ("synthetic-openshift", "https://openshift.example.invalid", "oc", "OpenShift"),
            ("crc", "https://crc.example.invalid", nil, "OpenShift"),
            ("kind-synthetic", "https://kind.example.invalid", nil, "kind"),
            ("minikube", "https://minikube.example.invalid", nil, "Minikube"),
            ("synthetic-k3d", "https://k3d.example.invalid", nil, "k3d"),
            ("synthetic-k3s", "https://k3s.example.invalid", nil, "K3s"),
            ("docker-desktop", "https://docker.example.invalid", nil, "Docker Desktop"),
            ("orbstack", "https://orbstack.example.invalid", nil, "OrbStack"),
            ("generic-cluster", "https://generic.example.invalid", nil, nil)
        ]

        for item in cases {
            let review = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
                raw: providerKubeconfig(
                    context: item.context,
                    server: item.server,
                    execCommand: item.command
                )
            )

            XCTAssertTrue(review.isValid, item.context)
            XCTAssertEqual(review.contexts.first?.name, item.context)
            XCTAssertEqual(review.contexts.first?.namespace, "synthetic-namespace")
            XCTAssertEqual(review.contexts.first?.providerHint, item.expectedHint, item.context)
        }
    }

    func testKubeConfigImportValidatorRecognizesAdditionalAuthShapesFromMobileReference() {
        let cases: [(name: String, userBlock: String, expectedAuthType: String, expectedHint: String?)] = [
            (
                "oidc-auth-provider",
                """
                auth-provider:
                      name: oidc
                      config:
                        issuer-url: https://issuer.example.invalid
                        client-id: synthetic-client
                        id-token: synthetic-id-token
                """,
                "OIDC",
                "OIDC"
            ),
            (
                "token-file",
                "tokenFile: /synthetic/token.txt",
                "Token file",
                nil
            ),
            (
                "basic-auth",
                """
                username: synthetic-user
                    password: synthetic-password
                """,
                "Basic",
                nil
            ),
            (
                "client-certificate-files",
                """
                client-certificate: /synthetic/client.crt
                    client-key: /synthetic/client.key
                """,
                "Client certificate",
                nil
            )
        ]

        for item in cases {
            let review = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
                raw:
                """
                apiVersion: v1
                kind: Config
                current-context: \(item.name)-context
                clusters:
                - name: \(item.name)-cluster
                  cluster:
                    server: https://example.invalid
                contexts:
                - name: \(item.name)-context
                  context:
                    cluster: \(item.name)-cluster
                    user: \(item.name)-user
                users:
                - name: \(item.name)-user
                  user:
                    \(item.userBlock)
                """
            )

            XCTAssertTrue(review.isValid, item.name)
            XCTAssertEqual(review.contexts.first?.authType, item.expectedAuthType, item.name)
            XCTAssertEqual(review.contexts.first?.providerHint, item.expectedHint, item.name)
            XCTAssertFalse(review.redactedPreview.contains("synthetic-id-token"), item.name)
            XCTAssertFalse(review.redactedPreview.contains("synthetic-password"), item.name)
            XCTAssertFalse(review.redactedPreview.contains("/synthetic/token.txt"), item.name)
            XCTAssertFalse(review.redactedPreview.contains("/synthetic/client.key"), item.name)
        }
    }

    private func providerKubeconfig(context: String, server: String, execCommand: String?) -> String {
        let cluster = "\(context)-cluster"
        let user = "\(context)-user"
        let userBlock = execCommand.map {
            """
            exec:
                  command: \($0)
            """
        } ?? "token: synthetic-token"

        return """
        apiVersion: v1
        kind: Config
        current-context: \(context)
        clusters:
        - cluster:
            server: \(server)
          name: \(cluster)
        users:
        - user:
            \(userBlock)
          name: \(user)
        contexts:
        - context:
            cluster: \(cluster)
            user: \(user)
            namespace: synthetic-namespace
          name: \(context)
        """
    }

    @MainActor
    func testViewModelStoresKubeConfigImportReviewWithoutLeakingSecrets() {
        let viewModel = RuneAppViewModel(
            state: RuneAppState(),
            kubeConfigImportValidator: KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"])
        )

        let review = viewModel.reviewKubeConfigImport(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: context-alpha
            clusters:
            - name: cluster-alpha
              cluster:
                server: https://example.invalid
            contexts:
            - name: context-alpha
              context:
                cluster: cluster-alpha
                user: user-alpha
            users:
            - name: user-alpha
              user:
                token: secret-token
            """
        )

        XCTAssertEqual(viewModel.kubeConfigImportReviews, [review])
        XCTAssertTrue(review.isValid)
        XCTAssertFalse(review.redactedPreview.contains("secret-token"))

        viewModel.clearKubeConfigImportReviews()
        XCTAssertTrue(viewModel.kubeConfigImportReviews.isEmpty)
    }

    @MainActor
    func testViewModelReviewsLoadedKubeConfigSourcesForAuthDoctorAction() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.loadedReview.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let kubeconfig = directory.appendingPathComponent("synthetic-kubeconfig.yaml")
        try Data(
            """
            apiVersion: v1
            kind: Config
            current-context: synthetic-context
            clusters:
            - name: synthetic-cluster
              cluster:
                server: https://example.invalid
            contexts:
            - name: synthetic-context
              context:
                cluster: synthetic-cluster
                user: synthetic-user
                namespace: synthetic-namespace
            users:
            - name: synthetic-user
              user:
                token: secret-token
            """.utf8
        ).write(to: kubeconfig, options: .atomic)

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfig)])
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigImportValidator: KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"])
        )

        let reviews = viewModel.reviewLoadedKubeConfigSources()

        XCTAssertEqual(reviews.count, 1)
        XCTAssertEqual(viewModel.kubeConfigImportReviews, reviews)
        XCTAssertEqual(reviews.first?.sourceName, kubeconfig.lastPathComponent)
        XCTAssertEqual(reviews.first?.contexts.first?.name, "synthetic-context")
        XCTAssertTrue(reviews.first?.isValid == true)
        XCTAssertFalse(reviews.first?.redactedPreview.contains("secret-token") == true)
    }

    @MainActor
    func testImportKubeConfigValidatesBeforeSavingBookmark() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importValidation.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("broken-kubeconfig.yaml")
        try Data(
            """
            apiVersion: v1
            kind: Config
            clusters: []
            users: []
            contexts: []
            """.utf8
        ).write(to: kubeconfig, options: .atomic)
        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            picker: FixedKubeConfigPicker(urls: [kubeconfig]),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer()
        )

        viewModel.importKubeConfig()

        try await waitUntilForRuneAppState {
            state.lastError != nil
        }
        XCTAssertTrue(state.lastError?.contains("current-context") == true)
        XCTAssertTrue(bookmarkStore.records.isEmpty)
        XCTAssertEqual(viewModel.kubeConfigImportReviews.count, 1)
        XCTAssertFalse(viewModel.kubeConfigImportReviews[0].isValid)
        XCTAssertTrue(state.authDoctorChecks.contains { $0.id == "kubeconfig-import" && $0.status == .failed })
        XCTAssertTrue(state.authDoctorChecks.contains { $0.id == "kubeconfig-import-missing-current-context" && $0.status == .failed })
    }

    @MainActor
    func testImportKubeConfigStagesValidPayloadUntilExplicitConfirmation() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importPreflight.\(UUID().uuidString)", isDirectory: true)
        let imports = directory.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: imports)
        )

        viewModel.importKubeConfig(raw: syntheticImportKubeConfig, sourceName: "synthetic.yaml")

        try await waitUntilForRuneAppState {
            viewModel.isKubeConfigImportConfirmationPending
        }
        XCTAssertTrue(viewModel.canConfirmKubeConfigImport)
        XCTAssertEqual(viewModel.kubeConfigImportReviewMode, .preflight)
        XCTAssertTrue(bookmarkStore.records.isEmpty)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertFalse(FileManager.default.fileExists(atPath: imports.path))

        viewModel.confirmKubeConfigImport()

        try await waitUntilForRuneAppState {
            !bookmarkStore.records.isEmpty
        }
        XCTAssertFalse(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertEqual(viewModel.kubeConfigImportReviewMode, .report)
        XCTAssertEqual(try String(contentsOfFile: bookmarkStore.records[0].path, encoding: .utf8), syntheticImportKubeConfig)
    }

    @MainActor
    func testCancelKubeConfigImportDiscardsPayloadAndDoesNotPersistMetadataOrFavorites() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.cancelImportPreflight.\(UUID().uuidString)", isDirectory: true)
        let preferences = FileBackedContextPreferencesStore(
            url: directory.appendingPathComponent("context-preferences.json")
        )
        let imports = directory.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            contextPreferences: preferences,
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: imports)
        )
        viewModel.favoriteImportedKubeConfigContexts = true
        viewModel.importKubeConfig(raw: syntheticImportKubeConfig, sourceName: "synthetic.yaml")

        try await waitUntilForRuneAppState {
            viewModel.isKubeConfigImportConfirmationPending
        }
        viewModel.setKubeConfigImportContextMetadata(
            contextName: "synthetic-context",
            alias: "Synthetic Alias",
            tags: ["synthetic"]
        )
        viewModel.cancelKubeConfigImport()

        XCTAssertFalse(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertFalse(viewModel.canConfirmKubeConfigImport)
        XCTAssertNil(viewModel.kubeConfigImportReviewMode)
        XCTAssertTrue(viewModel.kubeConfigImportReviews.isEmpty)
        XCTAssertTrue(bookmarkStore.records.isEmpty)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.favoriteContextNames.isEmpty)
        XCTAssertNil(preferences.loadContextDisplayMetadata(for: "synthetic-context"))
        XCTAssertNil(preferences.loadPreferredNamespace(for: "synthetic-context"))
        XCTAssertFalse(FileManager.default.fileExists(atPath: imports.path))
    }

    @MainActor
    func testBookmarkFailureRollsBackPublishedBatchAndLeavesPreferencesUnchanged() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importBookmarkRollback.\(UUID().uuidString)", isDirectory: true)
        let imports = directory.appendingPathComponent("imports", isDirectory: true)
        let preferences = FileBackedContextPreferencesStore(
            url: directory.appendingPathComponent("context-preferences.json")
        )
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: RejectingBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            contextPreferences: preferences,
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: imports)
        )

        viewModel.importKubeConfig(raw: syntheticImportKubeConfig, sourceName: "synthetic.yaml")
        try await waitUntilForRuneAppState {
            viewModel.isKubeConfigImportConfirmationPending
        }
        viewModel.setKubeConfigImportContextMetadata(
            contextName: "synthetic-context",
            alias: "Must Not Persist"
        )
        viewModel.confirmKubeConfigImport()

        try await waitUntilForRuneAppState {
            state.lastError != nil && !viewModel.isCommittingKubeConfigImport
        }
        XCTAssertTrue(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertTrue(viewModel.canConfirmKubeConfigImport)
        XCTAssertNil(preferences.loadContextDisplayMetadata(for: "synthetic-context"))
        XCTAssertNil(preferences.loadPreferredNamespace(for: "synthetic-context"))
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: imports.path), [])
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
    }

    @MainActor
    func testDoubleConfirmAndCancelDuringCommitPublishExactlyOnce() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importCommitGuard.\(UUID().uuidString)", isDirectory: true)
        let imports = directory.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let bookmarks = InMemoryBookmarkStore()
        let viewModel = RuneAppViewModel(
            state: RuneAppState(),
            bookmarkManager: BookmarkManager(store: bookmarks),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: imports)
        )
        viewModel.importKubeConfig(raw: syntheticImportKubeConfig, sourceName: "synthetic.yaml")
        try await waitUntilForRuneAppState {
            viewModel.isKubeConfigImportConfirmationPending
        }

        viewModel.confirmKubeConfigImport()
        viewModel.confirmKubeConfigImport()
        viewModel.cancelKubeConfigImport()

        try await waitUntilForRuneAppState {
            bookmarks.records.count == 1 && !viewModel.isKubeConfigImportConfirmationPending
        }
        XCTAssertEqual(bookmarks.records.count, 1)
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: imports.path).count, 1)
    }

    @MainActor
    func testConfirmRequiresFreshExplicitConfirmationWhenLoadedSourcesChangeAfterPreflight() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importRegistryReconfirm.\(UUID().uuidString)", isDirectory: true)
        let imports = directory.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let bookmarks = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarks),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: imports)
        )
        viewModel.importKubeConfig(raw: syntheticImportKubeConfig, sourceName: "incoming.yaml")
        try await waitUntilForRuneAppState {
            viewModel.isKubeConfigImportConfirmationPending && viewModel.canConfirmKubeConfigImport
        }

        let newlyLoadedSource = directory.appendingPathComponent("newly-loaded.yaml")
        try """
        apiVersion: v1
        kind: Config
        current-context: other-context
        clusters:
        - name: other-cluster
          cluster:
            server: https://other.example.invalid
        contexts:
        - name: other-context
          context:
            cluster: other-cluster
        users: []
        """.write(to: newlyLoadedSource, atomically: true, encoding: .utf8)
        state.setSources([KubeConfigSource(url: newlyLoadedSource)])

        viewModel.confirmKubeConfigImport()
        try await waitUntilForRuneAppState {
            !viewModel.isCommittingKubeConfigImport
                && viewModel.kubeConfigImportReviews.flatMap(\.issues).contains {
                    $0.id == "loaded-registry-changed-reconfirmation-required"
                }
        }

        XCTAssertTrue(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertTrue(viewModel.canConfirmKubeConfigImport)
        XCTAssertTrue(bookmarks.records.isEmpty)
        XCTAssertFalse(FileManager.default.fileExists(atPath: imports.path))

        viewModel.confirmKubeConfigImport()
        try await waitUntilForRuneAppState {
            bookmarks.records.count == 1 && !viewModel.isKubeConfigImportConfirmationPending
        }
        XCTAssertEqual(bookmarks.records.count, 1)
    }

    @MainActor
    func testConfirmRevalidatesChangedNamesAndBlocksStaleDuplicateResolution() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importRegistryTOCTOU.\(UUID().uuidString)", isDirectory: true)
        let imports = directory.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let loadedSource = directory.appendingPathComponent("loaded.yaml")
        func loadedKubeConfig(clusterName: String) -> String {
            """
            apiVersion: v1
            kind: Config
            current-context: loaded-context
            clusters:
            - name: \(clusterName)
              cluster:
                server: https://loaded.example.invalid
            contexts:
            - name: loaded-context
              context:
                cluster: \(clusterName)
            users: []
            """
        }
        try loadedKubeConfig(clusterName: "initial-cluster").write(
            to: loadedSource,
            atomically: true,
            encoding: .utf8
        )
        let bookmarks = InMemoryBookmarkStore()
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: loadedSource)])
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarks),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: imports)
        )
        viewModel.importKubeConfig(raw: syntheticImportKubeConfig, sourceName: "incoming.yaml")
        try await waitUntilForRuneAppState {
            viewModel.isKubeConfigImportConfirmationPending && viewModel.canConfirmKubeConfigImport
        }

        // The source path and selected source remain unchanged; only its registry names
        // change after preflight. Confirm must hash and reparse the current bytes.
        try loadedKubeConfig(clusterName: "synthetic-cluster").write(
            to: loadedSource,
            atomically: true,
            encoding: .utf8
        )
        viewModel.confirmKubeConfigImport()
        try await waitUntilForRuneAppState {
            !viewModel.isCommittingKubeConfigImport
                && viewModel.kubeConfigImportReviews.flatMap(\.issues).contains {
                    $0.id == "skip-duplicate-dependent-name-conflict"
                }
        }

        XCTAssertTrue(viewModel.isKubeConfigImportConfirmationPending)
        XCTAssertFalse(viewModel.canConfirmKubeConfigImport)
        XCTAssertTrue(bookmarks.records.isEmpty)
        XCTAssertFalse(FileManager.default.fileExists(atPath: imports.path))
        XCTAssertTrue(viewModel.kubeConfigImportReviews.flatMap(\.issues).contains {
            $0.id == "loaded-registry-changed-reconfirmation-required"
        })

        viewModel.kubeConfigDuplicateHandlingChoice = .importAsCopy
        XCTAssertTrue(viewModel.canConfirmKubeConfigImport)
        viewModel.confirmKubeConfigImport()
        try await waitUntilForRuneAppState {
            bookmarks.records.count == 1 && !viewModel.isKubeConfigImportConfirmationPending
        }
        let saved = try String(contentsOfFile: try XCTUnwrap(bookmarks.records.first?.path), encoding: .utf8)
        XCTAssertTrue(saved.contains("name: synthetic-cluster-copy-2"))
    }

    @MainActor
    func testStartingAnotherImportCannotReplaceACommittingTransaction() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importReplacementGuard.\(UUID().uuidString)", isDirectory: true)
        let imports = directory.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let bookmarks = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarks),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: imports)
        )
        viewModel.importKubeConfig(raw: syntheticImportKubeConfig, sourceName: "first.yaml")
        try await waitUntilForRuneAppState {
            viewModel.isKubeConfigImportConfirmationPending
        }

        viewModel.confirmKubeConfigImport()
        viewModel.importKubeConfig(
            raw: syntheticImportKubeConfig.replacingOccurrences(of: "synthetic-context", with: "replacement-context"),
            sourceName: "replacement.yaml"
        )

        XCTAssertTrue(state.lastError?.contains("Wait for the current kubeconfig import") == true)
        try await waitUntilForRuneAppState {
            bookmarks.records.count == 1 && !viewModel.isKubeConfigImportConfirmationPending
        }
        let saved = try String(contentsOfFile: bookmarks.records[0].path, encoding: .utf8)
        XCTAssertTrue(saved.contains("synthetic-context"))
        XCTAssertFalse(saved.contains("replacement-context"))
    }

    @MainActor
    func testSkipDuplicateAgainstLoadedSourceKeepsExistingSourceOrderAndMetadata() async throws {
        let fixture = try makeLoadedDuplicateImportFixture(label: "skip")
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        fixture.preferences.saveContextDisplayMetadata(
            ContextDisplayMetadata(alias: "Existing Alias"),
            for: "synthetic-context"
        )

        fixture.viewModel.importKubeConfig(
            raw: duplicateSourceKubeConfig(server: "incoming.example.invalid", namespace: "incoming"),
            sourceName: "incoming.yaml"
        )
        try await confirmPendingKubeConfigImport(fixture.viewModel)

        try await waitUntilForRuneAppState {
            (fixture.bookmarks.records.count == 2 && !fixture.viewModel.isKubeConfigImportConfirmationPending)
                || fixture.state.lastError != nil
        }
        XCTAssertNil(fixture.state.lastError)
        XCTAssertEqual(fixture.bookmarks.records.first?.path, fixture.existingSource.path)
        XCTAssertEqual(
            fixture.preferences.loadContextDisplayMetadata(for: "synthetic-context")?.alias,
            "Existing Alias"
        )
        XCTAssertTrue(fixture.viewModel.kubeConfigImportReviews.first?.contexts.isEmpty == true)
    }

    @MainActor
    func testSkipDuplicatePreservesLoadedSourceThatWasNotBookmarked() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.unbookmarkedDuplicateImport.\(UUID().uuidString)", isDirectory: true)
        let imports = directory.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let existingSource = directory.appendingPathComponent("existing.yaml")
        try duplicateSourceKubeConfig(
            server: "existing.example.invalid",
            namespace: "existing"
        ).write(to: existingSource, atomically: true, encoding: .utf8)
        let bookmarks = InMemoryBookmarkStore()
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: existingSource)])
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarks),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: imports)
        )

        viewModel.importKubeConfig(
            raw: duplicateSourceKubeConfig(server: "incoming.example.invalid", namespace: "incoming"),
            sourceName: "incoming.yaml"
        )
        try await confirmPendingKubeConfigImport(viewModel)
        try await waitUntilForRuneAppState {
            bookmarks.records.count == 1 && !viewModel.isKubeConfigImportConfirmationPending
        }

        XCTAssertEqual(state.kubeConfigSources.first?.url.standardizedFileURL, existingSource.standardizedFileURL)
        XCTAssertEqual(state.kubeConfigSources.count, 2)
        XCTAssertTrue(viewModel.kubeConfigImportReviews.first?.contexts.isEmpty == true)
    }

    @MainActor
    func testUpdateDuplicateAgainstLoadedSourcePrependsIncomingSourceAndPersistsMetadata() async throws {
        let fixture = try makeLoadedDuplicateImportFixture(label: "update")
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        fixture.preferences.saveContextDisplayMetadata(
            ContextDisplayMetadata(alias: "Existing Alias"),
            for: "synthetic-context"
        )
        fixture.viewModel.kubeConfigDuplicateHandlingChoice = .updateExisting
        fixture.viewModel.importKubeConfig(
            raw: duplicateSourceKubeConfig(server: "incoming.example.invalid", namespace: "incoming"),
            sourceName: "incoming.yaml"
        )
        try await waitUntilForRuneAppState {
            fixture.viewModel.isKubeConfigImportConfirmationPending
        }
        fixture.viewModel.setKubeConfigImportContextMetadata(
            contextName: "synthetic-context",
            alias: "Updated Alias"
        )
        fixture.viewModel.confirmKubeConfigImport()

        try await waitUntilForRuneAppState {
            fixture.bookmarks.records.count == 2 && !fixture.viewModel.isKubeConfigImportConfirmationPending
        }
        XCTAssertNotEqual(fixture.bookmarks.records.first?.path, fixture.existingSource.path)
        XCTAssertEqual(fixture.bookmarks.records.last?.path, fixture.existingSource.path)
        XCTAssertEqual(
            fixture.preferences.loadContextDisplayMetadata(for: "synthetic-context")?.alias,
            "Updated Alias"
        )
    }

    @MainActor
    func testImportAsCopyAgainstLoadedSourceRewritesNamesAndPersistsMetadataUnderFinalContext() async throws {
        let fixture = try makeLoadedDuplicateImportFixture(label: "copy")
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        fixture.viewModel.importKubeConfig(
            raw: duplicateSourceKubeConfig(server: "incoming.example.invalid", namespace: "incoming"),
            sourceName: "incoming.yaml"
        )
        try await waitUntilForRuneAppState {
            fixture.viewModel.isKubeConfigImportConfirmationPending
        }
        fixture.viewModel.kubeConfigDuplicateHandlingChoice = .importAsCopy
        XCTAssertEqual(
            fixture.viewModel.kubeConfigImportReviews.first?.contexts.map(\.name),
            ["synthetic-context-copy-2"]
        )
        XCTAssertTrue(
            fixture.viewModel.canConfirmKubeConfigImport,
            "Resolved issues: \(fixture.viewModel.kubeConfigImportReviews.flatMap(\.issues))"
        )
        fixture.viewModel.setKubeConfigImportContextMetadata(
            contextName: "synthetic-context-copy-2",
            alias: "Copied Alias"
        )
        fixture.viewModel.confirmKubeConfigImport()

        try await waitUntilForRuneAppState {
            (fixture.bookmarks.records.count == 2 && !fixture.viewModel.isKubeConfigImportConfirmationPending)
                || fixture.state.lastError != nil
        }
        XCTAssertNil(fixture.state.lastError)
        let importedPath = try XCTUnwrap(
            fixture.bookmarks.records.map(\.path).first { $0 != fixture.existingSource.path }
        )
        let saved = try String(contentsOfFile: importedPath, encoding: .utf8)
        XCTAssertTrue(saved.contains("current-context: synthetic-context-copy-2"))
        XCTAssertTrue(saved.contains("name: synthetic-cluster-copy-2"))
        XCTAssertTrue(saved.contains("name: synthetic-user-copy-2"))
        XCTAssertEqual(
            fixture.preferences.loadContextDisplayMetadata(for: "synthetic-context-copy-2")?.alias,
            "Copied Alias"
        )
        XCTAssertNil(fixture.preferences.loadContextDisplayMetadata(for: "synthetic-context"))
    }

    @MainActor
    private func makeLoadedDuplicateImportFixture(label: String) throws -> (
        directory: URL,
        existingSource: URL,
        bookmarks: InMemoryBookmarkStore,
        preferences: FileBackedContextPreferencesStore,
        state: RuneAppState,
        viewModel: RuneAppViewModel
    ) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.duplicateImport.\(label).\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let existingSource = directory.appendingPathComponent("existing.yaml")
        try duplicateSourceKubeConfig(
            server: "existing.example.invalid",
            namespace: "existing"
        ).write(to: existingSource, atomically: true, encoding: .utf8)
        let bookmarks = InMemoryBookmarkStore()
        let bookmarkManager = BookmarkManager(store: bookmarks)
        try bookmarkManager.addKubeConfig(url: existingSource)
        let preferences = FileBackedContextPreferencesStore(
            url: directory.appendingPathComponent("context-preferences.json")
        )
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: existingSource)])
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: bookmarkManager,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            contextPreferences: preferences,
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                rootDirectory: directory.appendingPathComponent("imports", isDirectory: true)
            )
        )
        return (directory, existingSource, bookmarks, preferences, state, viewModel)
    }

    private func duplicateSourceKubeConfig(server: String, namespace: String) -> String {
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
            namespace: \(namespace)
        users:
        - name: synthetic-user
          user:
            token: synthetic-token
        """
    }

    private var syntheticImportKubeConfig: String {
        """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://example.invalid
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            namespace: synthetic-namespace
        users: []
        """
    }

    @MainActor
    func testImportKubeConfigCopiesValidFileIntoAppOwnedStorageBeforeBookmarking() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importCopy.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let source = directory.appendingPathComponent("selected-config.yaml")
        let appOwnedDirectory = directory.appendingPathComponent("app-owned-imports", isDirectory: true)
        let kubeconfig = """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://example.invalid
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            namespace: default
        users: []
        """
        try kubeconfig.write(to: source, atomically: true, encoding: .utf8)

        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            picker: FixedKubeConfigPicker(urls: [source]),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: appOwnedDirectory)
        )

        viewModel.importKubeConfig()
        try await confirmPendingKubeConfigImport(viewModel)

        try await waitUntilForRuneAppState {
            !bookmarkStore.records.isEmpty
        }

        let record = try XCTUnwrap(bookmarkStore.records.first)
        XCTAssertNotEqual(record.path, source.path)
        XCTAssertTrue(record.path.hasPrefix(appOwnedDirectory.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: record.path))
        XCTAssertEqual(try String(contentsOfFile: record.path, encoding: .utf8), kubeconfig)
    }

    @MainActor
    func testImportKubeConfigCarriesSourceURLThroughViewModelAndPreservesRelativeCredentialFiles() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importRelativeCredentials.\(UUID().uuidString)", isDirectory: true)
        let sourceDirectory = directory.appendingPathComponent("selected", isDirectory: true)
        let credentialsDirectory = sourceDirectory.appendingPathComponent("credentials", isDirectory: true)
        let appOwnedDirectory = directory.appendingPathComponent("app-owned-imports", isDirectory: true)
        try FileManager.default.createDirectory(at: credentialsDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let referencedFiles: [(String, String)] = [
            ("ca.pem", "synthetic-ca"),
            ("client.pem", "synthetic-client-certificate"),
            ("client-key.pem", "synthetic-client-key"),
            ("token", "synthetic-token")
        ]
        for (name, contents) in referencedFiles {
            try contents.write(
                to: credentialsDirectory.appendingPathComponent(name),
                atomically: true,
                encoding: .utf8
            )
        }

        let source = sourceDirectory.appendingPathComponent("selected-config.yaml")
        let kubeconfig = """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://example.invalid
            certificate-authority: credentials/ca.pem
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
            tokenFile: credentials/token
            client-certificate: credentials/client.pem
            client-key: credentials/client-key.pem
        """
        try kubeconfig.write(to: source, atomically: true, encoding: .utf8)

        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            picker: FixedKubeConfigPicker(urls: [source]),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: appOwnedDirectory)
        )

        viewModel.importKubeConfig()
        try await confirmPendingKubeConfigImport(viewModel)

        try await waitUntilForRuneAppState {
            !bookmarkStore.records.isEmpty
        }

        let record = try XCTUnwrap(bookmarkStore.records.first)
        let imported = URL(fileURLWithPath: record.path)
        let saved = try String(contentsOf: imported, encoding: .utf8)
        let assetsDirectory = imported.deletingLastPathComponent().appendingPathComponent("assets", isDirectory: true)
        let assets = try FileManager.default.contentsOfDirectory(
            at: assetsDirectory,
            includingPropertiesForKeys: nil
        )
        XCTAssertEqual(assets.count, 4)
        XCTAssertTrue(saved.contains("assets/000-certificate-authority.pem"))
        XCTAssertTrue(saved.contains("assets/001-client-certificate.pem"))
        XCTAssertTrue(saved.contains("assets/002-client-key.pem"))
        XCTAssertTrue(saved.contains("assets/003-token"))
        XCTAssertEqual(state.kubeConfigSources.first?.url.standardizedFileURL.path, imported.standardizedFileURL.path)

        try FileManager.default.removeItem(at: sourceDirectory)
        for asset in assets {
            XCTAssertFalse(try Data(contentsOf: asset).isEmpty)
        }
    }

    @MainActor
    func testImportKubeConfigRawCopiesValidPasteIntoAppOwnedStorageBeforeBookmarking() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importPaste.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let appOwnedDirectory = directory.appendingPathComponent("app-owned-imports", isDirectory: true)
        let kubeconfig = """
        apiVersion: v1
        kind: Config
        current-context: pasted-context
        clusters:
        - name: pasted-cluster
          cluster:
            server: https://example.invalid
        contexts:
        - name: pasted-context
          context:
            cluster: pasted-cluster
            namespace: default
        users: []
        """

        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: appOwnedDirectory)
        )

        viewModel.importKubeConfig(raw: kubeconfig, sourceName: "pasted-kubeconfig.yaml")
        try await confirmPendingKubeConfigImport(viewModel)

        try await waitUntilForRuneAppState {
            !bookmarkStore.records.isEmpty
        }

        let record = try XCTUnwrap(bookmarkStore.records.first)
        XCTAssertTrue(record.path.hasPrefix(appOwnedDirectory.path))
        XCTAssertTrue(record.path.hasSuffix(".yaml"))
        XCTAssertEqual(try String(contentsOfFile: record.path, encoding: .utf8), kubeconfig)
        XCTAssertEqual(viewModel.kubeConfigImportReviews.first?.contexts.first?.name, "pasted-context")
    }

    @MainActor
    func testImportKubeConfigPersistsFavoriteAndPreferredNamespacePreferences() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importPreferences.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let suiteName = "RuneAppStateTests.importPreferences.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            contextPreferences: store,
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: directory.appendingPathComponent("imports", isDirectory: true))
        )
        viewModel.favoriteImportedKubeConfigContexts = true

        viewModel.importKubeConfig(
            raw:
            """
            apiVersion: v1
            kind: Config
            current-context: imported-context
            clusters:
            - name: imported-cluster
              cluster:
                server: https://example.invalid
            contexts:
            - name: imported-context
              context:
                cluster: imported-cluster
                namespace: imported-namespace
            users: []
            """,
            sourceName: "imported-kubeconfig.yaml"
        )
        try await confirmPendingKubeConfigImport(viewModel)

        try await waitUntilForRuneAppState {
            !bookmarkStore.records.isEmpty
        }

        XCTAssertTrue(state.favoriteContextNames.contains("imported-context"))
        XCTAssertTrue(store.loadFavoriteContextNames().contains("imported-context"))
        XCTAssertEqual(store.loadPreferredNamespace(for: "imported-context"), "imported-namespace")
    }

    @MainActor
    func testImportKubeConfigPersistsContextDisplayMetadataDraftsWithoutSensitiveValues() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importContextMetadata.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = FileBackedContextPreferencesStore(url: directory.appendingPathComponent("context-preferences.json"))
        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            contextPreferences: store,
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: directory.appendingPathComponent("imports", isDirectory: true))
        )
        let raw = """
        apiVersion: v1
        kind: Config
        current-context: synthetic-import
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://synthetic.eks.amazonaws.com
        contexts:
        - name: synthetic-import
          context:
            cluster: synthetic-cluster
            user: synthetic-user
            namespace: synthetic-namespace
        users:
        - name: synthetic-user
          user:
            token: synthetic-token-value
        """

        let review = viewModel.reviewKubeConfigImport(raw: raw, sourceName: "synthetic.yaml")
        XCTAssertEqual(review.contexts.first?.providerHint, "EKS")
        XCTAssertEqual(
            viewModel.kubeConfigImportContextMetadataDrafts["synthetic-import"],
            ContextDisplayMetadata(
                colorKey: "eks",
                iconName: "cloud",
                tags: ["EKS", "Token"],
                group: "Provider clusters"
            )
        )

        viewModel.setKubeConfigImportContextMetadata(
            contextName: "synthetic-import",
            alias: "Synthetic Import",
            colorKey: "blue",
            iconName: "server.rack",
            tags: ["platform", "team-synthetic", "platform"],
            group: "Synthetic Group"
        )
        viewModel.importKubeConfig(raw: raw, sourceName: "synthetic.yaml")
        try await confirmPendingKubeConfigImport(viewModel)

        try await waitUntilForRuneAppState {
            !bookmarkStore.records.isEmpty
        }

        XCTAssertEqual(
            store.loadContextDisplayMetadata(for: "synthetic-import"),
            ContextDisplayMetadata(
                alias: "Synthetic Import",
                colorKey: "blue",
                iconName: "server.rack",
                tags: ["platform", "team-synthetic"],
                group: "Synthetic Group"
            )
        )
        XCTAssertEqual(viewModel.contextDisplayMetadata(for: "synthetic-import")?.alias, "Synthetic Import")
        let json = try String(contentsOf: directory.appendingPathComponent("context-preferences.json"), encoding: .utf8)
        XCTAssertFalse(json.contains("synthetic-token-value"))
        XCTAssertFalse(json.contains("synthetic.eks.amazonaws.com"))
        XCTAssertFalse(json.contains("synthetic-user"))
    }

    @MainActor
    func testImportKubeConfigFolderImportsOnlyDirectKubeConfigCandidates() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importFolder.\(UUID().uuidString)", isDirectory: true)
        let folder = directory.appendingPathComponent("selected-folder", isDirectory: true)
        let nested = folder.appendingPathComponent("nested", isDirectory: true)
        let appOwnedDirectory = directory.appendingPathComponent("app-owned-imports", isDirectory: true)
        try FileManager.default.createDirectory(at: nested, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        func kubeconfig(context: String) -> String {
            """
            apiVersion: v1
            kind: Config
            current-context: \(context)
            clusters:
            - name: \(context)-cluster
              cluster:
                server: https://example.invalid
            contexts:
            - name: \(context)
              context:
                cluster: \(context)-cluster
            users: []
            """
        }

        try kubeconfig(context: "folder-config").write(
            to: folder.appendingPathComponent("config"),
            atomically: true,
            encoding: .utf8
        )
        try kubeconfig(context: "folder-extra").write(
            to: folder.appendingPathComponent("extra.yaml"),
            atomically: true,
            encoding: .utf8
        )
        try "not a kubeconfig".write(
            to: folder.appendingPathComponent("notes.txt"),
            atomically: true,
            encoding: .utf8
        )
        try kubeconfig(context: "nested-context").write(
            to: nested.appendingPathComponent("nested.yaml"),
            atomically: true,
            encoding: .utf8
        )

        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            picker: FixedKubeConfigPicker(urls: [], folderURL: folder),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: appOwnedDirectory)
        )

        viewModel.importKubeConfigFolder()
        try await confirmPendingKubeConfigImport(viewModel)

        try await waitUntilForRuneAppState {
            bookmarkStore.records.count == 2
        }

        XCTAssertEqual(Set(viewModel.kubeConfigImportReviews.flatMap { $0.contexts.map(\.name) }), ["folder-config", "folder-extra"])
        XCTAssertTrue(bookmarkStore.records.allSatisfy { $0.path.hasPrefix(appOwnedDirectory.path) })
        XCTAssertFalse(bookmarkStore.records.contains { $0.path.contains("notes") || $0.path.contains("nested") })
        XCTAssertEqual(state.kubeConfigSources.map(\.displayName), ["config.yaml", "extra.yaml"])
    }

    @MainActor
    func testImportManualTokenKubeConfigBuildsRedactedAppOwnedConfig() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.manualTokenImport.\(UUID().uuidString)", isDirectory: true)
        let appOwnedDirectory = directory.appendingPathComponent("app-owned-imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let bookmarkStore = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarkStore),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: appOwnedDirectory)
        )
        viewModel.manualKubeConfigName = "Manual Token Cluster"
        viewModel.manualKubeConfigServer = "https://example.invalid:6443"
        viewModel.manualKubeConfigNamespace = "manual-namespace"
        viewModel.manualKubeConfigToken = "synthetic-manual-token"

        viewModel.importManualTokenKubeConfig()
        try await confirmPendingKubeConfigImport(viewModel)

        try await waitUntilForRuneAppState {
            !bookmarkStore.records.isEmpty
        }

        let record = try XCTUnwrap(bookmarkStore.records.first)
        let saved = try String(contentsOfFile: record.path, encoding: .utf8)
        let review = try XCTUnwrap(viewModel.kubeConfigImportReviews.first)
        XCTAssertTrue(record.path.hasPrefix(appOwnedDirectory.path))
        XCTAssertTrue(saved.contains("server: \"https://example.invalid:6443\""))
        XCTAssertTrue(saved.contains("namespace: \"manual-namespace\""))
        XCTAssertTrue(saved.contains("token: \"synthetic-manual-token\""))
        XCTAssertEqual(review.contexts.first?.name, "Manual-Token-Cluster")
        XCTAssertEqual(review.contexts.first?.namespace, "manual-namespace")
        XCTAssertEqual(review.contexts.first?.authType, "Token")
        XCTAssertFalse(review.redactedPreview.contains("synthetic-manual-token"))
        XCTAssertEqual(viewModel.manualKubeConfigToken, "")
    }

    @MainActor
    func testManualKubeConfigSecretCanBeClearedWithoutChangingNonSecretDraftFields() {
        let viewModel = RuneAppViewModel(
            state: RuneAppState(),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer()
        )
        viewModel.manualKubeConfigName = "Synthetic Cluster"
        viewModel.manualKubeConfigServer = "https://example.invalid:6443"
        viewModel.manualKubeConfigNamespace = "synthetic-namespace"
        viewModel.manualKubeConfigToken = "synthetic-bearer-secret"

        viewModel.clearManualKubeConfigSecret()

        XCTAssertEqual(viewModel.manualKubeConfigToken, "")
        XCTAssertEqual(viewModel.manualKubeConfigName, "Synthetic Cluster")
        XCTAssertEqual(viewModel.manualKubeConfigServer, "https://example.invalid:6443")
        XCTAssertEqual(viewModel.manualKubeConfigNamespace, "synthetic-namespace")
    }

    @MainActor
    func testMockedAddClusterCloudImportLoadsCoreFakeClusterDataInSimpleMode() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreUserDefaultsValue(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }

        let server = try await RuneFakeK8sRESTServer.start(fixture: RuneFakeK8sFixture())
        defer { server.stop() }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.mockedAddCluster.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let kubeconfig = directory.appendingPathComponent("synthetic-cloud-kubeconfig.yaml")
        let rawKubeconfig = server.kubeconfigYAML()
        try rawKubeconfig.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let review = KubeConfigImportValidator(
            fileExists: { _ in true },
            executableSearchPaths: ["/synthetic/bin"]
        ).validate(raw: rawKubeconfig, sourceName: kubeconfig.lastPathComponent)
        let preview = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )
        let importer = MockCloudKubeConfigImporter(result: .success(CloudKubeConfigImportResult(
            command: preview,
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
            discoveredURLs: [kubeconfig],
            reviews: [review]
        )))
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                rootDirectory: directory.appendingPathComponent("imports", isDirectory: true)
            ),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1"
        ))

        try await waitUntilForRuneAppState(timeout: 10) {
            viewModel.cloudKubeConfigImportStatus == AddClusterCloudImportWorkflow.readyForReviewStatus(for: .eks)
                && viewModel.isKubeConfigImportConfirmationPending
        }
        viewModel.confirmKubeConfigImport()
        try await waitUntilForRuneAppState(timeout: 10) {
            viewModel.cloudKubeConfigImportStatus == "Imported EKS kubeconfig context."
                && state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                && state.selectedNamespace == "alpha-zone"
                && state.pods.contains { $0.name == "orbit-lens-6f58d7d89b-hx9q2" }
                && state.deployments.contains { $0.name == "orbit-lens" }
        }

        XCTAssertEqual(viewModel.kubeConfigImportReviews.count, 1)
        XCTAssertTrue(viewModel.kubeConfigImportReviews[0].isValid)
        XCTAssertTrue(state.kubeConfigSources.allSatisfy { $0.url.path.contains("/imports/") })
        XCTAssertFalse(state.kubeConfigSources.contains { $0.url.standardizedFileURL == kubeconfig.standardizedFileURL })
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testMockedAddClusterCloudImportLoadsFakeClusterForEveryRunnableProvider() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreUserDefaultsValue(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }

        let server = try await RuneFakeK8sRESTServer.start(fixture: RuneFakeK8sFixture())
        defer { server.stop() }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.mockedAddClusterAllProviders.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let kubeconfig = directory.appendingPathComponent("synthetic-cloud-kubeconfig.yaml")
        let rawKubeconfig = server.kubeconfigYAML()
        try rawKubeconfig.write(to: kubeconfig, atomically: true, encoding: .utf8)
        let review = KubeConfigImportValidator(
            fileExists: { _ in true },
            executableSearchPaths: ["/synthetic/bin"]
        ).validate(raw: rawKubeconfig, sourceName: kubeconfig.lastPathComponent)

        let cases: [(CloudKubeConfigImportRequest, CloudKubeConfigCommandPreview, String)] = [
            (
                CloudKubeConfigImportRequest(
                    provider: .aks,
                    clusterName: "synthetic-aks",
                    resourceGroup: "synthetic-group"
                ),
                CloudKubeConfigCommandPreview(
                    executable: "az",
                    arguments: ["aks", "get-credentials", "--resource-group", "synthetic-group", "--name", "synthetic-aks"],
                    displayCommand: "az aks get-credentials --resource-group synthetic-group --name synthetic-aks"
                ),
                "Imported AKS kubeconfig context."
            ),
            (
                CloudKubeConfigImportRequest(
                    provider: .eks,
                    clusterName: "synthetic-eks",
                    regionOrLocation: "eu-north-1"
                ),
                CloudKubeConfigCommandPreview(
                    executable: "aws",
                    arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-eks"],
                    displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-eks"
                ),
                "Imported EKS kubeconfig context."
            ),
            (
                CloudKubeConfigImportRequest(
                    provider: .gke,
                    clusterName: "synthetic-gke",
                    regionOrLocation: "europe-north1",
                    projectID: "synthetic-project"
                ),
                CloudKubeConfigCommandPreview(
                    executable: "gcloud",
                    arguments: [
                        "container", "clusters", "get-credentials",
                        "synthetic-gke",
                        "--location", "europe-north1",
                        "--project", "synthetic-project"
                    ],
                    displayCommand: "gcloud container clusters get-credentials synthetic-gke --location europe-north1 --project synthetic-project"
                ),
                "Imported GKE kubeconfig context."
            )
        ]

        for (request, preview, expectedStatus) in cases {
            let importer = MockCloudKubeConfigImporter(
                preview: preview,
                result: .success(CloudKubeConfigImportResult(
                    command: preview,
                    commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
                    discoveredURLs: [kubeconfig],
                    reviews: [review]
                ))
            )
            let state = RuneAppState()
            let viewModel = RuneAppViewModel(
                state: state,
                bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
                kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
                kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                    rootDirectory: directory.appendingPathComponent("imports-\(request.provider.rawValue)", isDirectory: true)
                ),
                cloudKubeConfigImporter: importer,
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )

            viewModel.runCloudKubeConfigImport(request)

            try await waitUntilForRuneAppState(timeout: 10) {
                viewModel.cloudKubeConfigImportStatus == AddClusterCloudImportWorkflow.readyForReviewStatus(for: request.provider)
                    && viewModel.isKubeConfigImportConfirmationPending
            }
            viewModel.confirmKubeConfigImport()
            try await waitUntilForRuneAppState(timeout: 10) {
                viewModel.cloudKubeConfigImportStatus == expectedStatus
                    && state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                    && state.selectedNamespace == "alpha-zone"
                    && state.pods.contains { $0.name == "orbit-lens-6f58d7d89b-hx9q2" }
                    && state.deployments.contains { $0.name == "orbit-lens" }
            }

            XCTAssertTrue(state.kubeConfigSources.allSatisfy { $0.url.path.contains("/imports-") })
            XCTAssertNil(state.lastError)
            XCTAssertEqual(viewModel.kubeConfigImportReviews, [review])
        }
    }

    @MainActor
    func testCloudImportUsesIsolatedTargetAndCancelLeavesRequestedKubeconfigUnchanged() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.cloudIsolation.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let requestedTarget = directory.appendingPathComponent("existing-default.yaml")
        let existingContents = "synthetic existing kubeconfig sentinel\n"
        try existingContents.write(to: requestedTarget, atomically: true, encoding: .utf8)
        let importer = TargetWritingCloudKubeConfigImporter()
        let bookmarks = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarks),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                rootDirectory: directory.appendingPathComponent("imports", isDirectory: true)
            ),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1",
            targetKubeconfigPath: requestedTarget.path
        ))

        try await waitUntilForRuneAppState {
            viewModel.isKubeConfigImportConfirmationPending && !viewModel.isRunningCloudKubeConfigImport
        }
        let isolatedTarget = try XCTUnwrap(importer.targetPaths.first)
        XCTAssertNotEqual(isolatedTarget, requestedTarget.path)
        XCTAssertTrue(FileManager.default.fileExists(atPath: isolatedTarget))
        XCTAssertEqual(try String(contentsOf: requestedTarget, encoding: .utf8), existingContents)
        XCTAssertTrue(bookmarks.records.isEmpty)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)

        viewModel.cancelKubeConfigImport()

        XCTAssertFalse(FileManager.default.fileExists(atPath: isolatedTarget))
        XCTAssertFalse(FileManager.default.fileExists(
            atPath: URL(fileURLWithPath: isolatedTarget).deletingLastPathComponent().path
        ))
        XCTAssertEqual(try String(contentsOf: requestedTarget, encoding: .utf8), existingContents)
        XCTAssertTrue(bookmarks.records.isEmpty)
    }

    @MainActor
    func testColdStartLeavesAddClusterCloudImportIdleUntilUserRunsProvider() {
        let importer = CountingCloudKubeConfigImporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
        XCTAssertNil(viewModel.cloudKubeConfigImportStatus)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertEqual(importer.previewCallCount, 0)
        XCTAssertEqual(importer.importCallCount, 0)

        let command = viewModel.cloudKubeConfigCommandPreview(for: CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1"
        ))

        XCTAssertEqual(command, "aws eks update-kubeconfig")
        XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
        XCTAssertNil(viewModel.cloudKubeConfigImportStatus)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertEqual(importer.previewCallCount, 1)
        XCTAssertEqual(importer.importCallCount, 0)
    }

    @MainActor
    func testColdStartRootShellDoesNotPreviewOrRunAddClusterProvider() {
        let importer = CountingCloudKubeConfigImporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        _ = RuneRootView(
            viewModel: viewModel,
            onLayoutSnapshotChange: nil,
            debugDisableBootstrap: true
        )

        XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
        XCTAssertNil(viewModel.cloudKubeConfigImportStatus)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertEqual(importer.previewCallCount, 0)
        XCTAssertEqual(importer.importCallCount, 0)
    }

    @MainActor
    func testColdStartLaunchShellInitialMountDoesNotPreviewOrRunAddClusterProvider() {
        let importer = CountingCloudKubeConfigImporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        let controller = NSHostingController(
            rootView: RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: nil,
                debugDisableBootstrap: true
            )
        )

        controller.view.frame = NSRect(x: 0, y: 0, width: 1440, height: 900)
        controller.view.layoutSubtreeIfNeeded()

        XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
        XCTAssertNil(viewModel.cloudKubeConfigImportStatus)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertEqual(importer.previewCallCount, 0)
        XCTAssertEqual(importer.importCallCount, 0)
    }

    @MainActor
    func testMockedAddClusterCloudImportFailureDoesNotLoadClusterData() async throws {
        let preview = CloudKubeConfigCommandPreview(
            executable: "gcloud",
            arguments: ["container", "clusters", "get-credentials", "synthetic-gke"],
            displayCommand: "gcloud container clusters get-credentials synthetic-gke"
        )
        let importer = MockCloudKubeConfigImporter(
            preview: preview,
            result: .failure(.commandFailed(
                command: preview.displayCommand,
                exitCode: 42,
                message: "synthetic login required"
            ))
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
            provider: .gke,
            clusterName: "synthetic-gke",
            regionOrLocation: "europe-north1",
            projectID: "synthetic-project"
        ))

        try await waitUntilForRuneAppState {
            viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                && state.lastError?.contains("Cloud import command failed") == true
        }

        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertTrue(state.pods.isEmpty)
        XCTAssertTrue(state.authDoctorChecks.contains {
            $0.id == "cloud-login-gke"
                && $0.status == .failed
                && !$0.message.contains("synthetic login required")
        })
        XCTAssertFalse(state.lastError?.contains("synthetic login required") == true)
        XCTAssertFalse(state.lastError?.contains(preview.displayCommand) == true)
        XCTAssertFalse(state.activeNotice?.message.contains("synthetic login required") == true)
        XCTAssertFalse(state.activeNotice?.message.contains(preview.displayCommand) == true)
    }

    @MainActor
    func testAddClusterCloudImportFailureKeepsUserFacingStatusAndChecksGeneric() async throws {
        let preview = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )
        let sensitiveProviderOutput = "synthetic-token-from-provider stderr"
        let importer = MockCloudKubeConfigImporter(
            preview: preview,
            result: .failure(.commandFailed(
                command: preview.displayCommand,
                exitCode: 42,
                message: sensitiveProviderOutput
            ))
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1"
        ))

        try await waitUntilForRuneAppState {
            viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                && state.authDoctorChecks.contains { $0.id == "cloud-login-eks" && $0.status == .failed }
        }

        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, "Cloud import failed.")
        let diagnostic = try XCTUnwrap(viewModel.cloudKubeConfigImportDiagnostic)
        let diagnosticText = [
            diagnostic.title,
            diagnostic.classification,
            diagnostic.message,
            diagnostic.operationShape,
            diagnostic.nextAction,
            diagnostic.documentationTitle,
            diagnostic.documentationURL.absoluteString
        ].joined(separator: "\n")
        XCTAssertEqual(diagnostic.title, "Provider CLI failed")
        XCTAssertEqual(diagnostic.classification, "Exit code 42")
        XCTAssertTrue(diagnostic.operationShape.contains("<cluster-name>"))
        XCTAssertTrue(diagnostic.operationShape.contains("<region>"))
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertFalse(state.authDoctorChecks.map(\.message).joined(separator: "\n").contains(sensitiveProviderOutput))
        XCTAssertFalse(viewModel.cloudKubeConfigImportStatus?.contains(sensitiveProviderOutput) == true)
        XCTAssertFalse(state.lastError?.contains(sensitiveProviderOutput) == true)
        XCTAssertFalse(state.lastError?.contains(preview.displayCommand) == true)
        XCTAssertFalse(state.activeNotice?.message.contains(sensitiveProviderOutput) == true)
        XCTAssertFalse(state.activeNotice?.message.contains(preview.displayCommand) == true)
        XCTAssertFalse(diagnosticText.contains(sensitiveProviderOutput))
        XCTAssertFalse(diagnosticText.contains(preview.displayCommand))
        XCTAssertFalse(diagnosticText.contains("synthetic-cloud"))
        XCTAssertFalse(diagnosticText.contains("eu-north-1"))

        let supportBundle = SupportBundleRequest.snapshot(
            state: state,
            generatedAt: "2026-06-21T00:00:00Z",
            resourceCounts: [:],
            selectedResourceKind: nil,
            selectedResourceName: nil
        )
        let data = try JSONSupportBundleBuilder().buildBundle(from: supportBundle)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))
        XCTAssertFalse(json.contains(sensitiveProviderOutput))
        XCTAssertFalse(json.contains(preview.displayCommand))
    }

    @MainActor
    func testAddClusterCloudImportBlockingReviewKeepsImportReviewAuthDoctorChecks() async throws {
        let preview = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )
        let review = KubeConfigImportReview(
            contexts: [],
            issues: [
                KubeConfigImportIssue(
                    id: "missing-current-context",
                    severity: .error,
                    message: "Synthetic current context is missing."
                )
            ],
            redactedPreview: "apiVersion: v1\ncontexts: []\n"
        )
        let importer = MockCloudKubeConfigImporter(result: .success(CloudKubeConfigImportResult(
            command: preview,
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
            discoveredURLs: [],
            reviews: [review]
        )))
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1"
        ))

        try await waitUntilForRuneAppState {
            viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                && !viewModel.isRunningCloudKubeConfigImport
                && state.lastError?.contains("Kubeconfig is missing current-context.") == true
        }

        XCTAssertEqual(viewModel.kubeConfigImportReviews, [review])
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertTrue(state.authDoctorChecks.contains {
            $0.id == "kubeconfig-import-missing-current-context"
                && $0.status == .failed
                && $0.message == "Kubeconfig is missing current-context."
        })
        XCTAssertFalse(state.authDoctorChecks.contains { $0.id == "cloud-login-eks" })
    }

    @MainActor
    func testAddClusterCloudImportBlockingReviewDoesNotReplaceExistingKubeconfigSources() async throws {
        let existingSource = KubeConfigSource(url: URL(fileURLWithPath: "/tmp/rune-existing-synthetic-kubeconfig"))
        let preview = CloudKubeConfigCommandPreview(
            executable: "az",
            arguments: ["aks", "get-credentials", "--resource-group", "synthetic-group", "--name", "synthetic-aks"],
            displayCommand: "az aks get-credentials --resource-group synthetic-group --name synthetic-aks"
        )
        let review = KubeConfigImportReview(
            contexts: [],
            issues: [
                KubeConfigImportIssue(
                    id: "missing-current-context",
                    severity: .error,
                    message: "Synthetic current context is missing."
                )
            ],
            redactedPreview: "apiVersion: v1\ncontexts: []\n"
        )
        let importer = MockCloudKubeConfigImporter(result: .success(CloudKubeConfigImportResult(
            command: preview,
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
            discoveredURLs: [],
            reviews: [review]
        )))
        let state = RuneAppState()
        state.setSources([existingSource])
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
            provider: .aks,
            clusterName: "synthetic-aks",
            resourceGroup: "synthetic-group"
        ))

        try await waitUntilForRuneAppState {
            viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                && state.lastError?.contains("Kubeconfig is missing current-context.") == true
        }

        XCTAssertEqual(state.kubeConfigSources, [existingSource])
        XCTAssertEqual(viewModel.kubeConfigImportReviews, [review])
        XCTAssertTrue(state.authDoctorChecks.contains {
            $0.id == "kubeconfig-import-missing-current-context"
                && $0.status == .failed
        })
    }

    @MainActor
    func testAddClusterCloudImportIgnoresDuplicateRunWhileImportIsInFlight() async throws {
        let preview = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )
        let importer = BlockingCloudKubeConfigImporter(result: CloudKubeConfigImportResult(
            command: preview,
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
            discoveredURLs: [],
            reviews: []
        ))
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        let request = CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1"
        )

        viewModel.runCloudKubeConfigImport(request)
        viewModel.runCloudKubeConfigImport(request)

        try await waitUntilForRuneAppState {
            importer.importCallCount == 1
                && importer.hasSuspendedImport
                && viewModel.isRunningCloudKubeConfigImport
        }
        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, "Running EKS import...")

        importer.resume()

        try await waitUntilForRuneAppState {
            !viewModel.isRunningCloudKubeConfigImport
        }
        XCTAssertEqual(importer.importCallCount, 1)
    }

    @MainActor
    func testAddClusterCloudImportStatusCanBeClearedAfterFailure() async throws {
        let preview = CloudKubeConfigCommandPreview(
            executable: "gcloud",
            arguments: ["container", "clusters", "get-credentials", "synthetic-gke"],
            displayCommand: "gcloud container clusters get-credentials synthetic-gke"
        )
        let importer = MockCloudKubeConfigImporter(
            preview: preview,
            result: .failure(.commandFailed(
                command: preview.displayCommand,
                exitCode: 42,
                message: "synthetic login required"
            ))
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
            provider: .gke,
            clusterName: "synthetic-gke",
            regionOrLocation: "europe-north1",
            projectID: "synthetic-project"
        ))

        try await waitUntilForRuneAppState {
            viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                && !viewModel.isRunningCloudKubeConfigImport
        }

        viewModel.clearCloudKubeConfigImportStatus()

        XCTAssertNil(viewModel.cloudKubeConfigImportStatus)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertTrue(state.contexts.isEmpty)
    }

    @MainActor
    func testAddClusterCloudImportStreamsProviderOutputInsideRune() async throws {
        let preview = CloudKubeConfigCommandPreview(
            executable: "gcloud",
            arguments: ["container", "clusters", "get-credentials", "synthetic-gke"],
            displayCommand: "gcloud container clusters get-credentials synthetic-gke"
        )
        let importer = OutputtingCloudKubeConfigImporter(
            preview: preview,
            chunks: [
                CloudKubeConfigCommandOutput(stream: .stdout, text: "Open browser login\n"),
                CloudKubeConfigCommandOutput(stream: .stderr, text: "Waiting for provider auth\n")
            ],
            result: .failure(.commandFailed(
                command: preview.displayCommand,
                exitCode: 42,
                message: "synthetic login required"
            ))
        )
        let viewModel = RuneAppViewModel(
            state: RuneAppState(),
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
            provider: .gke,
            clusterName: "synthetic-gke",
            regionOrLocation: "europe-north1",
            projectID: "synthetic-project"
        ))

        try await waitUntilForRuneAppState {
            viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                && viewModel.cloudKubeConfigImportOutput.contains("Open browser login")
                && viewModel.cloudKubeConfigImportOutput.contains("Waiting for provider auth")
        }

        XCTAssertTrue(viewModel.cloudKubeConfigImportOutput.hasPrefix("$ gcloud container clusters get-credentials"))

        viewModel.clearCloudKubeConfigImportStatus()

        XCTAssertEqual(viewModel.cloudKubeConfigImportOutput, "")
    }

    @MainActor
    func testAddClusterCloudImportStatusClearKeepsRunningStatusWhileImportIsInFlight() async throws {
        let preview = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )
        let importer = BlockingCloudKubeConfigImporter(result: CloudKubeConfigImportResult(
            command: preview,
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
            discoveredURLs: [],
            reviews: []
        ))
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1"
        ))

        try await waitUntilForRuneAppState {
            importer.hasSuspendedImport
                && viewModel.isRunningCloudKubeConfigImport
        }

        viewModel.clearCloudKubeConfigImportStatus()

        XCTAssertEqual(viewModel.cloudKubeConfigImportStatus, "Running EKS import...")
        XCTAssertEqual(importer.importCallCount, 1)

        importer.resume()

        try await waitUntilForRuneAppState {
            !viewModel.isRunningCloudKubeConfigImport
        }
    }

    @MainActor
    func testAddClusterCloudImportFailureUnlocksRetryAndClearsStaleError() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreUserDefaultsValue(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }

        let server = try await RuneFakeK8sRESTServer.start(fixture: RuneFakeK8sFixture())
        defer { server.stop() }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.addClusterRetry.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let kubeconfig = directory.appendingPathComponent("synthetic-retry-kubeconfig.yaml")
        let rawKubeconfig = server.kubeconfigYAML()
        try rawKubeconfig.write(to: kubeconfig, atomically: true, encoding: .utf8)
        let review = KubeConfigImportValidator(
            fileExists: { _ in true },
            executableSearchPaths: ["/synthetic/bin"]
        ).validate(raw: rawKubeconfig, sourceName: kubeconfig.lastPathComponent)
        let preview = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )
        let importer = SequencedCloudKubeConfigImporter(
            preview: preview,
            results: [
                .failure(.commandFailed(
                    command: preview.displayCommand,
                    exitCode: 42,
                    message: "synthetic login required"
                )),
                .success(CloudKubeConfigImportResult(
                    command: preview,
                    commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
                    discoveredURLs: [kubeconfig],
                    reviews: [review]
                ))
            ]
        )
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(
                rootDirectory: directory.appendingPathComponent("imports", isDirectory: true)
            ),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        let request = CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1"
        )

        viewModel.runCloudKubeConfigImport(request)

        try await waitUntilForRuneAppState {
            viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                && !viewModel.isRunningCloudKubeConfigImport
                && state.lastError?.contains("Cloud import command failed") == true
        }

        viewModel.runCloudKubeConfigImport(request)

        XCTAssertNil(state.lastError)
        try await waitUntilForRuneAppState {
            viewModel.cloudKubeConfigImportStatus == AddClusterCloudImportWorkflow.readyForReviewStatus(for: .eks)
                && !viewModel.isRunningCloudKubeConfigImport
                && viewModel.isKubeConfigImportConfirmationPending
        }
        viewModel.confirmKubeConfigImport()
        try await waitUntilForRuneAppState {
            viewModel.cloudKubeConfigImportStatus == "Imported EKS kubeconfig context."
                && state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                && state.pods.contains { $0.name == "orbit-lens-6f58d7d89b-hx9q2" }
        }
        XCTAssertNil(state.lastError)
        XCTAssertEqual(importer.importCallCount, 2)
    }

    @MainActor
    func testAddClusterRefreshContextsLoadsExternallyWrittenKubeconfigAgainstFakeCluster() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreUserDefaultsValue(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }

        let server = try await RuneFakeK8sRESTServer.start(fixture: RuneFakeK8sFixture())
        defer { server.stop() }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.addClusterRefresh.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let kubeconfig = directory.appendingPathComponent("synthetic-cli-written-kubeconfig.yaml")
        try server.kubeconfigYAML().write(to: kubeconfig, atomically: true, encoding: .utf8)

        let discoverer = MutableKubeConfigDiscoverer()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: InMemoryBookmarkStore()),
            kubeConfigDiscoverer: discoverer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.refreshKubeConfigSourcesFromDiscovery()

        try await waitUntilForRuneAppState {
            state.contexts.isEmpty
        }

        discoverer.urls = [kubeconfig]
        viewModel.refreshKubeConfigSourcesFromDiscovery()

        try await waitUntilForRuneAppState(timeout: 10) {
            state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                && state.selectedNamespace == "alpha-zone"
                && state.pods.contains { $0.name == "orbit-lens-6f58d7d89b-hx9q2" }
                && state.deployments.contains { $0.name == "orbit-lens" }
        }

        XCTAssertEqual(state.kubeConfigSources.map(\.url.standardizedFileURL.path), [kubeconfig.standardizedFileURL.path])
        XCTAssertEqual(discoverer.callCount, 2)
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testSamePathKubeconfigContentChangeClearsCompletedAuthDoctorScope() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer { restoreUserDefaultsValue(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode) }

        let server = try await RuneFakeK8sRESTServer.start(fixture: RuneFakeK8sFixture())
        defer { server.stop() }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.authDoctorConfigRevision.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let kubeconfig = directory.appendingPathComponent("config.yaml")
        let initialConfig = server.kubeconfigYAML()
        try initialConfig.write(to: kubeconfig, atomically: true, encoding: .utf8)
        let discoverer = MutableKubeConfigDiscoverer()
        discoverer.urls = [kubeconfig]
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: RejectingBookmarkStore()),
            kubeConfigDiscoverer: discoverer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        _ = try await viewModel.syncKubeConfigSourcesFromDiscovery(reason: "test-initial")
        try await waitUntilForRuneAppState {
            state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                && !state.selectedNamespace.isEmpty
        }

        viewModel.runAuthDoctor()
        try await waitUntilForRuneAppState(timeout: 10) {
            !state.isRunningAuthDoctor
                && state.authDoctorChecks.contains { $0.id == "selected-context" }
        }

        let sourcesBeforeChange = state.kubeConfigSources
        try (initialConfig + "\n# synthetic revision\n")
            .write(to: kubeconfig, atomically: true, encoding: .utf8)
        let changed = try await viewModel.syncKubeConfigSourcesFromDiscovery(
            reason: "test-content-change"
        )

        XCTAssertTrue(changed)
        XCTAssertEqual(state.kubeConfigSources, sourcesBeforeChange)
        XCTAssertTrue(
            state.authDoctorChecks.isEmpty,
            "Diagnostics from the previous kubeconfig bytes must not remain visible or exportable."
        )
    }

    @MainActor
    func testSameContextNameSourceRotationNeverPublishesOldNamespaceScope() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        let previousPersistence = UserDefaults.standard.object(forKey: RuneSettingsKeys.persistNamespaceListCache)
        UserDefaults.standard.runeSimpleMode = true
        UserDefaults.standard.runePersistNamespaceListCache = true
        defer {
            restoreUserDefaultsValue(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            restoreUserDefaultsValue(previousPersistence, forKey: RuneSettingsKeys.persistNamespaceListCache)
        }

        let contextName = "synthetic-shared-context"
        func fixture(namespace: String, delayed: Bool = false) -> RuneFakeK8sFixture {
            RuneFakeK8sFixture(
                contexts: [
                    RuneFakeK8sCluster(
                        contextName: contextName,
                        defaultNamespace: namespace,
                        namespaces: [
                            RuneFakeK8sNamespace(
                                name: namespace,
                                pods: [],
                                deployments: [],
                                services: []
                            )
                        ],
                        nodes: []
                    )
                ],
                delayedResponseTargets: delayed ? ["/api/v1/namespaces": 150_000_000] : [:]
            )
        }

        let oldServer = try await RuneFakeK8sRESTServer.start(
            fixture: fixture(namespace: "old-scope-only"),
            contextName: contextName
        )
        let newServer = try await RuneFakeK8sRESTServer.start(
            fixture: fixture(namespace: "new-scope-only", delayed: true),
            contextName: contextName
        )
        defer {
            oldServer.stop()
            newServer.stop()
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.namespaceSourceRotation.\(UUID().uuidString)", isDirectory: true)
        let cacheDirectory = directory.appendingPathComponent("namespace-cache", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let suiteName = "RuneAppStateTests.namespaceSourceRotationPreferences.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let contextPreferences = UserDefaultsContextPreferencesStore(defaults: defaults)
        contextPreferences.saveManualNamespaces(["old-scope-only"], for: contextName)

        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try oldServer.kubeconfigYAML()
            .write(to: kubeconfig, atomically: true, encoding: .utf8)
        let discoverer = MutableKubeConfigDiscoverer()
        discoverer.urls = [kubeconfig]
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: RejectingBookmarkStore()),
            kubeConfigDiscoverer: discoverer,
            contextPreferences: contextPreferences,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: JSONNamespaceListPersistenceStore(directoryURL: cacheDirectory)
        )

        _ = try await viewModel.syncKubeConfigSourcesFromDiscovery(reason: "synthetic-initial")
        try await waitUntilForRuneAppState(timeout: 10) {
            state.selectedContext?.name == contextName
                && state.namespaces == ["old-scope-only"]
                && state.selectedNamespace == "old-scope-only"
        }

        var rotationStarted = false
        var observedRotatedScopes: [(namespaces: [String], selectedNamespace: String)] = []
        let observation = Publishers.CombineLatest3(
            state.$selectedContext,
            state.$namespaces,
            state.$selectedNamespace
        )
        .sink { context, namespaces, selectedNamespace in
            guard rotationStarted, context?.name == contextName else { return }
            observedRotatedScopes.append((namespaces, selectedNamespace))
        }
        defer { observation.cancel() }

        try newServer.kubeconfigYAML()
            .write(to: kubeconfig, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes(
            [.modificationDate: Date(timeIntervalSinceNow: 2)],
            ofItemAtPath: kubeconfig.path
        )
        state.setResourceYAML("metadata:\n  name: synthetic-baseline\n")
        state.updateResourceYAMLDraft("metadata:\n  name: synthetic-local-edit\n")
        rotationStarted = true
        let deferred = try await viewModel.syncKubeConfigSourcesFromDiscovery(
            reason: "synthetic-source-rotation"
        )

        XCTAssertTrue(deferred)
        XCTAssertTrue(viewModel.isKubeConfigScopeReloadPending)
        XCTAssertFalse(viewModel.writeActionsEnabled)
        XCTAssertEqual(state.namespaces, ["old-scope-only"])

        state.revertResourceYAMLToClusterSnapshot()
        let resumed = try await viewModel.syncKubeConfigSourcesFromDiscovery(
            reason: "synthetic-source-rotation-resumed"
        )

        XCTAssertTrue(resumed)
        XCTAssertFalse(viewModel.isKubeConfigScopeReloadPending)
        try await waitUntilForRuneAppState(timeout: 10) {
            state.selectedContext?.name == contextName
                && state.namespaces == ["new-scope-only"]
                && state.selectedNamespace == "new-scope-only"
        }
        XCTAssertFalse(
            observedRotatedScopes.contains {
                $0.namespaces.contains("old-scope-only")
                    || $0.selectedNamespace == "old-scope-only"
            },
            "A reused context name must have an empty boundary until the new source revision is verified."
        )
        XCTAssertFalse(viewModel.namespaceOptions.contains("old-scope-only"))
        XCTAssertTrue(
            contextPreferences.loadManualNamespaces(for: contextName).isEmpty,
            "A successful authoritative list should remove stale manual entries from an older source scope."
        )
    }

    @MainActor
    func testDiscoverySyncRetainsConfirmedSessionImportWhenBookmarkReloadIsUnavailable() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.sessionImportSync.\(UUID().uuidString)", isDirectory: true)
        let imports = directory.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let bookmarks = InMemoryBookmarkStore()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: bookmarks),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            kubeConfigImportStore: AppOwnedKubeConfigImportStore(rootDirectory: imports),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        viewModel.importKubeConfig(raw: syntheticImportKubeConfig, sourceName: "synthetic.yaml")
        try await confirmPendingKubeConfigImport(viewModel)
        try await waitUntilForRuneAppState {
            bookmarks.records.count == 1 && !viewModel.isKubeConfigImportConfirmationPending
        }
        let importedPath = try XCTUnwrap(state.kubeConfigSources.first?.url.standardizedFileURL.path)

        bookmarks.removeAllRecords()
        _ = try await viewModel.syncKubeConfigSourcesFromDiscovery(reason: "test-bookmark-unavailable")

        XCTAssertEqual(state.kubeConfigSources.map { $0.url.standardizedFileURL.path }, [importedPath])
        XCTAssertTrue(FileManager.default.fileExists(atPath: importedPath))
    }

    func testFileBackedContextPreferencesStoreRoundTripsVersionedPreferences() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.fileBacked.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("context-preferences.json")

        let store = FileBackedContextPreferencesStore(url: url)
        store.saveFavoriteContextNames(["beta", "alpha", "alpha"])
        store.saveFavoriteResourceIDs(["context-alpha|pod|default|api"])
        store.saveFavoriteNamespaceIDs(["context-alpha|namespace|default"])
        store.saveManualProductionContextIDs(["context-alpha"])
        store.saveManualNamespaces(["zeta", "alpha", "alpha"], for: "context-alpha")
        store.savePreferredNamespace("zeta", for: "context-alpha")
        store.saveContextDisplayMetadata(
            ContextDisplayMetadata(
                alias: " Synthetic Alpha ",
                colorKey: " eks ",
                iconName: " cloud ",
                tags: [" EKS ", "Token", "EKS"],
                group: " Provider clusters "
            ),
            for: "context-alpha"
        )
        store.saveHiddenOperatorPrinterColumnFamilies(["Flux", "cert-manager", "Flux"])

        let reloaded = FileBackedContextPreferencesStore(url: url)

        XCTAssertEqual(reloaded.loadFavoriteContextNames(), ["alpha", "beta"])
        XCTAssertEqual(reloaded.loadFavoriteResourceIDs(), ["context-alpha|pod|default|api"])
        XCTAssertEqual(reloaded.loadFavoriteNamespaceIDs(), ["context-alpha|namespace|default"])
        XCTAssertEqual(reloaded.loadManualProductionContextIDs(), ["context-alpha"])
        XCTAssertEqual(reloaded.loadManualNamespaces(for: "context-alpha"), ["alpha", "zeta"])
        XCTAssertEqual(reloaded.loadPreferredNamespace(for: "context-alpha"), "zeta")
        XCTAssertEqual(
            reloaded.loadContextDisplayMetadata(for: "context-alpha"),
            ContextDisplayMetadata(
                alias: "Synthetic Alpha",
                colorKey: "eks",
                iconName: "cloud",
                tags: ["EKS", "Token"],
                group: "Provider clusters"
            )
        )
        XCTAssertEqual(reloaded.loadHiddenOperatorPrinterColumnFamilies(), ["Flux", "cert-manager"])

        let json = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(json.contains("\"schemaVersion\""))
        XCTAssertTrue(json.contains("\(FileBackedContextPreferencesStore.currentSchemaVersion)"))
    }

    func testFileBackedContextPreferencesAppliesImportMetadataAsOneDocumentUpdate() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.importPreferenceBatch.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("context-preferences.json")
        let backupURL = url.appendingPathExtension("bak")
        let store = FileBackedContextPreferencesStore(url: url, backupURL: backupURL)
        store.saveFavoriteContextNames(["existing-context"])

        store.applyKubeConfigImportPreferenceBatch(KubeConfigImportPreferenceBatch(
            preferredNamespaces: ["synthetic-context": "synthetic-namespace"],
            contextDisplayMetadata: [
                "synthetic-context": ContextDisplayMetadata(alias: "Synthetic", tags: ["test"])
            ],
            favoriteContextNames: ["existing-context", "synthetic-context"]
        ))

        let reloaded = FileBackedContextPreferencesStore(url: url)
        XCTAssertEqual(reloaded.loadPreferredNamespace(for: "synthetic-context"), "synthetic-namespace")
        XCTAssertEqual(
            reloaded.loadContextDisplayMetadata(for: "synthetic-context"),
            ContextDisplayMetadata(alias: "Synthetic", tags: ["test"])
        )
        XCTAssertEqual(reloaded.loadFavoriteContextNames(), ["existing-context", "synthetic-context"])

        let previousDocument = FileBackedContextPreferencesStore(url: backupURL)
        XCTAssertEqual(previousDocument.loadFavoriteContextNames(), ["existing-context"])
        XCTAssertNil(previousDocument.loadPreferredNamespace(for: "synthetic-context"))
        XCTAssertNil(previousDocument.loadContextDisplayMetadata(for: "synthetic-context"))
    }

    @MainActor
    func testSavedWorkspaceStorePersistsSyntheticSnapshotsWithoutSensitiveMetadata() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.savedWorkspaces.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("saved-workspaces.json")
        let store = JSONSavedWorkspaceStore(url: url)

        store.saveSavedWorkspaces([
            SavedWorkspaceSnapshot(
                id: "workspace-beta",
                name: "  Workload Drilldown  ",
                contextName: "context-beta",
                namespace: "observability",
                section: .workloads,
                workloadKind: .deployment,
                isFavorite: true,
                resourceKind: "deployment",
                resourceName: "api",
                resourceNamespace: "observability",
                inspectorState: SavedWorkspaceInspectorState(
                    podTabID: "logs",
                    serviceTabID: "unifiedLogs",
                    deploymentTabID: "rollout",
                    genericManifestTabID: "yaml",
                    helmTabID: "history",
                    terminalTabID: "yaml",
                    isYAMLInlineEditing: true
                )
            ),
            SavedWorkspaceSnapshot(
                id: "workspace-alpha",
                name: "Cluster Overview",
                contextName: "context-alpha",
                namespace: "default",
                section: .overview,
                workloadKind: .pod,
                resourceKind: nil,
                resourceName: nil,
                resourceNamespace: nil
            )
        ])

        let reloaded = store.loadSavedWorkspaces()

        XCTAssertEqual(reloaded.map(\.id), ["workspace-beta", "workspace-alpha"])
        XCTAssertEqual(reloaded.map(\.name), ["Workload Drilldown", "Cluster Overview"])
        XCTAssertEqual(reloaded.first?.isFavorite, true)
        XCTAssertEqual(reloaded.first?.contextName, "context-beta")
        XCTAssertEqual(reloaded.first?.resourceName, "api")
        XCTAssertEqual(reloaded.first?.inspectorState?.podTabID, "logs")
        XCTAssertEqual(reloaded.first?.inspectorState?.deploymentTabID, "rollout")
        XCTAssertEqual(reloaded.first?.inspectorState?.isYAMLInlineEditing, true)

        let json = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(json.contains("\"schemaVersion\""))
        XCTAssertFalse(json.contains("/Users/"))
        XCTAssertFalse(json.localizedCaseInsensitiveContains("token"))
        XCTAssertFalse(json.localizedCaseInsensitiveContains("client-certificate-data"))
        XCTAssertFalse(json.localizedCaseInsensitiveContains("client-key-data"))
        XCTAssertFalse(json.localizedCaseInsensitiveContains("server:"))
        XCTAssertFalse(json.localizedCaseInsensitiveContains("certificate-authority-data"))
    }

    @MainActor
    func testTerminalWorkspaceStateStorePersistsTargetsWithoutTranscriptPayloads() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.terminalWorkspace.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("terminal-workspace-state.json")
        let store = JSONTerminalWorkspaceStateStore(url: url)
        let session = PodTerminalSession(
            id: "shell-a",
            contextName: "context-alpha",
            namespace: "default",
            podName: "api-0",
            containerName: "app",
            shell: "sh",
            transcript: "synthetic-sensitive-marker\nshell command payload\n",
            status: .connected
        )
        let snapshot = TerminalWorkspaceStateSnapshot(
            sessions: [TerminalWorkspaceSessionSnapshot(session: session)],
            activeSessionID: "shell-a",
            logTabs: [
                TerminalWorkspaceLogTabSnapshot(
                    id: "log-a",
                    podID: "default/api-0",
                    namespace: "default",
                    podName: "api-0"
                )
            ],
            activeLogTabID: "log-a",
            selectedLogPodID: "default/api-0",
            shellPodID: "default/api-0",
            portForwardPodID: "default/api-0",
            inspectorTabID: "logs"
        )

        store.saveTerminalWorkspaceState(snapshot)

        let reloaded = try XCTUnwrap(store.loadTerminalWorkspaceState())
        XCTAssertEqual(reloaded.sessions.first?.podName, "api-0")
        XCTAssertEqual(reloaded.logTabs.first?.podID, "default/api-0")
        XCTAssertEqual(reloaded.activeSessionID, "shell-a")
        let restored = try XCTUnwrap(reloaded.sessions.first?.restoredSession)
        XCTAssertEqual(restored.status, .disconnected)
        XCTAssertEqual(restored.transcript, "")

        let json = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(json.contains("\"schemaVersion\""))
        XCTAssertFalse(json.localizedCaseInsensitiveContains("synthetic-sensitive-marker"))
        XCTAssertFalse(json.localizedCaseInsensitiveContains("shell command payload"))
        XCTAssertFalse(json.localizedCaseInsensitiveContains("transcript"))
        XCTAssertFalse(json.contains("/Users/"))
    }

    @MainActor
    func testViewModelSavesAndOpensWorkspaceFromCurrentSelection() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.viewModelSavedWorkspaces.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = JSONSavedWorkspaceStore(url: directory.appendingPathComponent("saved-workspaces.json"))
        let contextAlpha = KubeContext(name: "context-alpha")
        let contextBeta = KubeContext(name: "context-beta")
        let pod = PodSummary(
            name: "api",
            namespace: "observability",
            status: "Running",
            containerNamesLine: "api, sidecar"
        )
        let state = RuneAppState()
        state.setContexts([contextAlpha, contextBeta])
        state.setNamespaces(["default", "observability"])
        state.selectedContext = contextBeta
        state.selectedNamespace = "observability"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.setPods([pod])
        state.setSelectedPod(pod)
        let resourceStore = ResourceStore()
        resourceStore.cacheNamespaces(["default", "observability"], context: contextBeta)
        resourceStore.cacheSnapshot(
            context: contextBeta,
            namespace: "observability",
            pods: [pod],
            deployments: [],
            statefulSets: [],
            daemonSets: [],
            jobs: [],
            cronJobs: [],
            replicaSets: [],
            persistentVolumeClaims: [],
            horizontalPodAutoscalers: [],
            networkPolicies: [],
            services: [],
            endpoints: [],
            ingresses: [],
            configMaps: [],
            secrets: [],
            serviceAccounts: [],
            events: []
        )
        let viewModel = RuneAppViewModel(state: state, store: resourceStore, savedWorkspaceStore: store)
        viewModel.selectedLogPreset = .last15Minutes
        viewModel.includePreviousLogs = true
        viewModel.selectedLogContainer = "sidecar"
        viewModel.isLogTailModeEnabled = true
        viewModel.isSidebarVisible = false
        viewModel.isDetailPaneVisible = false
        viewModel.updateSavedWorkspaceInspectorState(
            SavedWorkspaceInspectorState(
                podTabID: "logs",
                serviceTabID: "unifiedLogs",
                deploymentTabID: "rollout",
                genericManifestTabID: "yaml",
                helmTabID: "history",
                terminalTabID: "yaml",
                isYAMLInlineEditing: true
            )
        )

        let saveCommand = try XCTUnwrap(viewModel.commandPaletteItems(query: ":savews Observability API").first)
        XCTAssertEqual(saveCommand.title, "Save Workspace: Observability API")
        viewModel.executeCommandPaletteItem(saveCommand)

        let saved = try XCTUnwrap(viewModel.savedWorkspaces.first)
        XCTAssertEqual(saved.name, "Observability API")
        XCTAssertEqual(saved.contextName, "context-beta")
        XCTAssertEqual(saved.namespace, "observability")
        XCTAssertEqual(saved.section, .workloads)
        XCTAssertEqual(saved.workloadKind, .pod)
        XCTAssertEqual(saved.resourceKind, "pod")
        XCTAssertEqual(saved.resourceName, "api")
        XCTAssertEqual(saved.resourceNamespace, "observability")
        XCTAssertEqual(saved.logPresetID, PodLogPreset.last15Minutes.rawValue)
        XCTAssertEqual(saved.logContainer, "sidecar")
        XCTAssertEqual(saved.includePreviousLogs, true)
        XCTAssertEqual(saved.isLogTailModeEnabled, true)
        XCTAssertEqual(saved.isSidebarVisible, false)
        XCTAssertEqual(saved.isDetailPaneVisible, false)
        XCTAssertEqual(saved.inspectorState?.podTabID, "logs")
        XCTAssertEqual(saved.inspectorState?.serviceTabID, "unifiedLogs")
        XCTAssertEqual(saved.inspectorState?.deploymentTabID, "rollout")
        XCTAssertEqual(saved.inspectorState?.genericManifestTabID, "yaml")
        XCTAssertEqual(saved.inspectorState?.helmTabID, "history")
        XCTAssertEqual(saved.inspectorState?.terminalTabID, "yaml")
        XCTAssertEqual(saved.inspectorState?.isYAMLInlineEditing, true)

        let favoriteCommand = try XCTUnwrap(viewModel.commandPaletteItems(query: ":favws api").first)
        XCTAssertEqual(favoriteCommand.title, "Favorite Workspace: Observability API")
        viewModel.executeCommandPaletteItem(favoriteCommand)
        XCTAssertEqual(viewModel.savedWorkspaces.first?.isFavorite, true)

        state.selectedContext = contextAlpha
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .deployment
        state.setSelectedPod(nil)
        viewModel.selectedLogPreset = .recentLines
        viewModel.includePreviousLogs = false
        viewModel.selectedLogContainer = ""
        viewModel.isLogTailModeEnabled = false
        viewModel.isSidebarVisible = true
        viewModel.isDetailPaneVisible = true

        let commandItem = try XCTUnwrap(viewModel.commandPaletteItems(query: ":ws api").first)
        XCTAssertEqual(commandItem.title, "Observability API")
        viewModel.executeCommandPaletteItem(commandItem)

        XCTAssertEqual(state.selectedContext, contextBeta)
        XCTAssertEqual(state.selectedNamespace, "observability")
        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod, pod)
        XCTAssertEqual(viewModel.selectedLogPreset, .last15Minutes)
        XCTAssertEqual(viewModel.includePreviousLogs, true)
        XCTAssertEqual(viewModel.selectedLogContainer, "sidecar")
        XCTAssertEqual(viewModel.isLogTailModeEnabled, true)
        XCTAssertEqual(viewModel.isSidebarVisible, false)
        XCTAssertEqual(viewModel.isDetailPaneVisible, false)
        XCTAssertEqual(viewModel.savedWorkspaceInspectorRestoreRequest?.inspectorState.podTabID, "logs")
        XCTAssertEqual(viewModel.savedWorkspaceInspectorRestoreRequest?.inspectorState.deploymentTabID, "rollout")
        XCTAssertEqual(viewModel.savedWorkspaceInspectorRestoreRequest?.inspectorState.isYAMLInlineEditing, true)
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":ws").first?.symbolName, "star.fill")
        XCTAssertEqual(store.loadSavedWorkspaces().map(\.name), ["Observability API"])
        XCTAssertEqual(store.loadSavedWorkspaces().first?.isFavorite, true)
    }

    @MainActor
    func testRootSynchronizesHelmBrowserFamilyWhenOpeningLegacySavedWorkspaces() async throws {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            restoreUserDefaultsValue(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        viewModel.loadDemoCluster()
        let release = try XCTUnwrap(state.helmReleases.first)
        let operatorResource = try XCTUnwrap(state.operatorResources.first)
        let controller = NSHostingController(
            rootView: RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: nil,
                debugDisableBootstrap: true
            )
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1_280, height: 800),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = controller
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }
        controller.view.layoutSubtreeIfNeeded()

        let operatorWorkspace = SavedWorkspaceSnapshot(
            name: "Synthetic operator workspace",
            contextName: nil,
            namespace: operatorResource.namespace ?? "default",
            section: .helm,
            workloadKind: .pod,
            resourceKind: operatorResource.apiPath,
            resourceName: operatorResource.name,
            resourceNamespace: operatorResource.namespace
        )
        viewModel.openSavedWorkspace(operatorWorkspace)
        XCTAssertEqual(state.selectedOperatorResource, operatorResource)
        XCTAssertEqual(viewModel.helmBrowserResourceFamily, .operatorResources)

        try await waitUntilForRuneAppState {
            state.selectedOperatorResource == operatorResource
                && viewModel.helmBrowserResourceFamily == .operatorResources
        }
        XCTAssertEqual(state.selectedOperatorResource, operatorResource)
        XCTAssertEqual(viewModel.helmBrowserResourceFamily, .operatorResources)
        XCTAssertEqual(state.operatorResources, [operatorResource])
        XCTAssertNil(state.selectedHelmRelease)

        let releaseWorkspace = SavedWorkspaceSnapshot(
            name: "Synthetic release workspace",
            contextName: nil,
            namespace: release.namespace,
            section: .helm,
            workloadKind: .pod,
            resourceKind: "helmrelease",
            resourceName: release.name,
            resourceNamespace: release.namespace
        )
        viewModel.openSavedWorkspace(releaseWorkspace)

        try await waitUntilForRuneAppState {
            state.selectedHelmRelease == release
        }
        XCTAssertEqual(viewModel.helmBrowserResourceFamily, .helmReleases)

        XCTAssertNil(operatorWorkspace.inspectorState)
        XCTAssertNil(releaseWorkspace.inspectorState)
        XCTAssertGreaterThan(window.frame.width, 0)
    }

    @MainActor
    func testSavedWorkspaceCommandPaletteSearchUsesContextDisplayMetadata() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.workspaceContextMetadata.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let workspaceStore = JSONSavedWorkspaceStore(url: directory.appendingPathComponent("saved-workspaces.json"))
        workspaceStore.saveSavedWorkspaces([
            SavedWorkspaceSnapshot(
                id: "workspace-payments",
                name: "API Triage",
                contextName: "context-alpha",
                namespace: "observability",
                section: .workloads,
                workloadKind: .pod,
                resourceKind: "pod",
                resourceName: "api",
                resourceNamespace: "observability"
            )
        ])
        let contextPreferences = FileBackedContextPreferencesStore(url: directory.appendingPathComponent("context-preferences.json"))
        contextPreferences.saveContextDisplayMetadata(
            ContextDisplayMetadata(
                alias: "Checkout Production",
                iconName: "cloud",
                tags: ["payments", "platform"],
                group: "Provider clusters"
            ),
            for: "context-alpha"
        )
        let state = RuneAppState()
        state.setContexts([KubeContext(name: "context-alpha")])
        let viewModel = RuneAppViewModel(
            state: state,
            contextPreferences: contextPreferences,
            savedWorkspaceStore: workspaceStore
        )

        let commandItem = try XCTUnwrap(viewModel.commandPaletteItems(query: ":ws payments").first)
        XCTAssertEqual(commandItem.title, "API Triage")
        XCTAssertTrue(commandItem.subtitle.contains("Checkout Production"))
        XCTAssertTrue(commandItem.subtitle.contains("context-alpha"))
        XCTAssertTrue(commandItem.subtitle.contains("payments"))

        let globalItem = try XCTUnwrap(viewModel.commandPaletteItems(query: "Provider clusters").first { $0.id == "workspace:workspace-payments" })
        XCTAssertEqual(globalItem.title, "API Triage")
    }

    func testFileBackedContextPreferencesStoreRecoversFromBackupWhenPrimaryIsCorrupt() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.fileBackedRecovery.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("context-preferences.json")
        let backupURL = directory.appendingPathComponent("context-preferences.json.bak")
        let corruptURL = directory.appendingPathComponent("context-preferences.json.corrupt")

        let store = FileBackedContextPreferencesStore(url: url, backupURL: backupURL, corruptURL: corruptURL)
        store.saveFavoriteContextNames(["context-alpha"])
        store.saveFavoriteContextNames(["context-beta"])
        try Data("not-json".utf8).write(to: url, options: .atomic)

        let recovered = FileBackedContextPreferencesStore(url: url, backupURL: backupURL, corruptURL: corruptURL)

        XCTAssertEqual(recovered.loadFavoriteContextNames(), ["context-alpha"])
        XCTAssertTrue(FileManager.default.fileExists(atPath: corruptURL.path))
        XCTAssertEqual(try String(contentsOf: corruptURL, encoding: .utf8), "not-json")
        XCTAssertTrue(try String(contentsOf: url, encoding: .utf8).contains("context-alpha"))
    }

    func testFileBackedContextPreferencesStoreMigratesLegacyDefaultsWithoutSensitivePayloadFields() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.fileBackedMigration.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("context-preferences.json")

        let suiteName = "RuneAppStateTests.legacyPreferences.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let legacy = UserDefaultsContextPreferencesStore(defaults: defaults)
        legacy.saveFavoriteContextNames(["context-alpha"])
        legacy.saveFavoriteResourceIDs(["context-alpha|deployment|default|api"])
        legacy.saveFavoriteNamespaceIDs(["context-alpha|namespace|default"])
        legacy.saveManualProductionContextIDs(["context-alpha"])
        legacy.saveManualNamespaces(["default"], for: "context-alpha")
        legacy.savePreferredNamespace("default", for: "context-alpha")

        let migrated = FileBackedContextPreferencesStore(url: url, legacyStore: legacy)

        XCTAssertEqual(migrated.loadFavoriteContextNames(), ["context-alpha"])
        XCTAssertEqual(migrated.loadManualNamespaces(for: "context-alpha"), ["default"])
        XCTAssertEqual(migrated.loadPreferredNamespace(for: "context-alpha"), "default")
        XCTAssertTrue(FileManager.default.fileExists(atPath: url.path))

        let json = try String(contentsOf: url, encoding: .utf8).lowercased()
        XCTAssertFalse(json.contains("kubeconfig"))
        XCTAssertFalse(json.contains("bearer"))
        XCTAssertFalse(json.contains("token"))
        XCTAssertFalse(json.contains("client-certificate-data"))
        XCTAssertFalse(json.contains("client-key-data"))
        XCTAssertFalse(json.contains("secretvalue"))
    }

    func testFileBackedContextPreferencesStoreMigratesOlderSchemaDocument() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.fileBackedOlderSchema.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("context-preferences.json")
        try Data(
            """
            {
              "favoriteContextNames": ["context-beta", "context-alpha"],
              "favoriteResourceIDs": ["context-alpha|service|default|api"],
              "manualNamespaces": {
                "context-alpha": ["zeta", "default"]
              }
            }
            """.utf8
        ).write(to: url, options: .atomic)

        let migrated = FileBackedContextPreferencesStore(url: url)

        XCTAssertEqual(migrated.loadFavoriteContextNames(), ["context-alpha", "context-beta"])
        XCTAssertEqual(migrated.loadFavoriteResourceIDs(), ["context-alpha|service|default|api"])
        XCTAssertEqual(migrated.loadManualNamespaces(for: "context-alpha"), ["default", "zeta"])

        migrated.savePreferredNamespace("default", for: "context-alpha")

        let json = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(json.contains("\"schemaVersion\""))
        XCTAssertTrue(json.contains("\(FileBackedContextPreferencesStore.currentSchemaVersion)"))
    }

    func testWriteSafetySettingsDefaultOnAndPersistAsUserPreferences() {
        let suiteName = "RuneAppStateTests.writeSafety.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertTrue(defaults.runeWriteSafetyRequireApplyDryRun)
        XCTAssertTrue(defaults.runeWriteSafetyRequireRolloutDryRun)
        XCTAssertTrue(defaults.runeWriteSafetyRequireHelmDryRun)
        XCTAssertTrue(defaults.runeWriteSafetyShowRollbackPlan)
        XCTAssertTrue(defaults.runeWriteSafetyRequireCopyableCommand)
        XCTAssertTrue(defaults.runeWriteSafetyRequirePostActionVerification)
        XCTAssertTrue(defaults.runeWriteSafetyRequireProductionSecondConfirmation)
        XCTAssertFalse(defaults.runeWriteSafetyShowDestructiveCommandsInCommandPalette)

        defaults.runeWriteSafetyRequireApplyDryRun = false
        defaults.runeWriteSafetyRequireProductionSecondConfirmation = false
        defaults.runeWriteSafetyShowDestructiveCommandsInCommandPalette = true

        XCTAssertFalse(defaults.runeWriteSafetyRequireApplyDryRun)
        XCTAssertFalse(defaults.runeWriteSafetyRequireProductionSecondConfirmation)
        XCTAssertTrue(defaults.runeWriteSafetyShowDestructiveCommandsInCommandPalette)
    }

    func testManagedFieldsDisplaySettingDefaultsOnAndPersists() {
        let suiteName = "RuneAppStateTests.managedFields.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertTrue(defaults.runeHideManagedFieldsByDefault)

        defaults.runeHideManagedFieldsByDefault = false

        XCTAssertFalse(defaults.runeHideManagedFieldsByDefault)
    }

    @MainActor
    func testViewModelRestoresAndPersistsSplitPaneVisibility() {
        let defaults = UserDefaults.standard
        let previousSidebar = defaults.object(forKey: RuneSettingsKeys.layoutSidebarVisible)
        let previousDetail = defaults.object(forKey: RuneSettingsKeys.layoutDetailPaneVisible)
        defer {
            if let previousSidebar {
                defaults.set(previousSidebar, forKey: RuneSettingsKeys.layoutSidebarVisible)
            } else {
                defaults.removeObject(forKey: RuneSettingsKeys.layoutSidebarVisible)
            }

            if let previousDetail {
                defaults.set(previousDetail, forKey: RuneSettingsKeys.layoutDetailPaneVisible)
            } else {
                defaults.removeObject(forKey: RuneSettingsKeys.layoutDetailPaneVisible)
            }
        }

        defaults.runeLayoutSidebarVisible = false
        defaults.runeLayoutDetailPaneVisible = true

        let viewModel = RuneAppViewModel(state: RuneAppState())

        XCTAssertFalse(viewModel.isSidebarVisible)
        XCTAssertTrue(viewModel.isDetailPaneVisible)

        viewModel.toggleSidebarVisibility()
        viewModel.toggleDetailPaneVisibility()

        XCTAssertTrue(defaults.runeLayoutSidebarVisible)
        XCTAssertFalse(defaults.runeLayoutDetailPaneVisible)
    }

    @MainActor
    func testLoadDemoClusterPopulatesFullInMemorySnapshot() {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let state = RuneAppState()
        state.isLoading = true
        state.isLoadingLogs = true
        state.setError(RuneError.invalidInput(message: "Previous startup error"))
        let viewModel = RuneAppViewModel(state: state)

        viewModel.loadDemoCluster()

        XCTAssertEqual(state.contexts.map(\.name), ["rune-demo"])
        XCTAssertEqual(state.selectedContext?.name, "rune-demo")
        XCTAssertEqual(state.selectedNamespace, "demo")
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
        XCTAssertFalse(state.isLoading)
        XCTAssertFalse(state.isLoadingLogs)
        XCTAssertNil(state.lastError)
        XCTAssertEqual(state.pods.count, 3)
        XCTAssertNotNil(state.pods.first { $0.name == "checkout-5d79f6c8b9-vx4lp" && $0.status == "CrashLoopBackOff" })
        XCTAssertFalse(state.deployments.isEmpty)
        XCTAssertNotNil(state.deployments.first { $0.name == "checkout" && $0.readyReplicas == 0 && $0.desiredReplicas == 2 })
        XCTAssertFalse(state.statefulSets.isEmpty)
        XCTAssertFalse(state.daemonSets.isEmpty)
        XCTAssertFalse(state.jobs.isEmpty)
        XCTAssertFalse(state.cronJobs.isEmpty)
        XCTAssertFalse(state.replicaSets.isEmpty)
        XCTAssertFalse(state.horizontalPodAutoscalers.isEmpty)
        XCTAssertFalse(state.services.isEmpty)
        XCTAssertFalse(state.ingresses.isEmpty)
        XCTAssertFalse(state.networkPolicies.isEmpty)
        XCTAssertFalse(state.configMaps.isEmpty)
        XCTAssertFalse(state.secrets.isEmpty)
        XCTAssertFalse(state.persistentVolumeClaims.isEmpty)
        XCTAssertFalse(state.persistentVolumes.isEmpty)
        XCTAssertFalse(state.storageClasses.isEmpty)
        XCTAssertFalse(state.nodes.isEmpty)
        XCTAssertFalse(state.rbacRoles.isEmpty)
        XCTAssertFalse(state.rbacRoleBindings.isEmpty)
        XCTAssertFalse(state.rbacClusterRoles.isEmpty)
        XCTAssertFalse(state.rbacClusterRoleBindings.isEmpty)
        XCTAssertFalse(state.helmReleases.isEmpty)
        XCTAssertFalse(state.operatorResources.isEmpty)
        XCTAssertNotNil(state.events.first { $0.type == "Warning" && $0.objectName == "checkout-5d79f6c8b9-vx4lp" })
        XCTAssertTrue(state.resourceYAML.contains("kind: Pod"))
        XCTAssertTrue(state.resourceDescribe.contains("Name:"))
        XCTAssertTrue(state.podLogs.contains("demo API"))
    }

    @MainActor
    func testEventsSortBySelectedColumn() {
        let state = RuneAppState()
        state.setEvents([
            EventSummary(
                type: "Normal",
                reason: "Started",
                objectName: "api-2",
                message: "Started container api",
                lastTimestamp: "2026-05-05T10:02:00Z",
                involvedKind: "Pod",
                involvedNamespace: "backend"
            ),
            EventSummary(
                type: "Warning",
                reason: "BackOff",
                objectName: "api-1",
                message: "Back-off restarting container",
                lastTimestamp: "2026-05-05T10:03:00Z",
                involvedKind: "Pod",
                involvedNamespace: "backend"
            ),
            EventSummary(
                type: "Normal",
                reason: "Scheduled",
                objectName: "web-0",
                message: "Assigned pod to node",
                lastTimestamp: "2026-05-05T10:01:00Z",
                involvedKind: "Pod",
                involvedNamespace: "frontend"
            )
        ])
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.visibleEvents.map(\.reason), ["BackOff", "Scheduled", "Started"])

        viewModel.toggleEventSort(.lastSeen)
        XCTAssertEqual(viewModel.visibleEvents.map(\.objectName), ["api-1", "api-2", "web-0"])

        viewModel.toggleEventSort(.lastSeen)
        XCTAssertEqual(viewModel.visibleEvents.map(\.objectName), ["web-0", "api-2", "api-1"])

        viewModel.toggleEventSort(.namespace)
        XCTAssertEqual(viewModel.visibleEvents.map(\.objectName), ["api-1", "api-2", "web-0"])
    }

    @MainActor
    func testEventSourceNavigationUsesInvolvedNamespaceWhenNamesCollide() {
        let state = RuneAppState()
        state.selectedNamespace = "frontend"
        state.setPods([
            PodSummary(name: "api-0", namespace: "backend", status: "Running"),
            PodSummary(name: "api-0", namespace: "frontend", status: "CrashLoopBackOff")
        ])
        let event = EventSummary(
            type: "Warning",
            reason: "BackOff",
            objectName: "api-0",
            message: "Back-off restarting container",
            lastTimestamp: "2026-05-05T10:03:00Z",
            involvedKind: "Pod",
            involvedNamespace: "frontend"
        )
        state.setEvents([event])
        state.selectedSection = .events
        let viewModel = RuneAppViewModel(state: state)

        viewModel.openEventSource(event)

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.namespace, "frontend")
        XCTAssertEqual(state.selectedPod?.status, "CrashLoopBackOff")
    }

    @MainActor
    func testOverviewIncidentUsesAlreadyLoadedOverviewPodWithoutWaitingForAnotherSnapshot() throws {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        state.selectedSection = .overview
        let pod = PodSummary(
            name: "synthetic-api-abc123",
            namespace: "synthetic",
            status: "CrashLoopBackOff"
        )
        let event = EventSummary(
            type: "Warning",
            reason: "BackOff",
            objectName: pod.name,
            message: "Back-off restarting container",
            lastTimestamp: "2026-05-05T10:03:00Z",
            involvedKind: "Pod",
            involvedNamespace: pod.namespace
        )
        state.setPods([])
        state.setOverviewSnapshot(
            pods: [pod],
            deploymentsCount: 0,
            servicesCount: 0,
            ingressesCount: 0,
            configMapsCount: 0,
            cronJobsCount: 0,
            nodesCount: 0,
            events: [event]
        )
        let viewModel = RuneAppViewModel(state: state)
        let incident = try XCTUnwrap(viewModel.overviewIncidentTimelineItems.first)

        viewModel.openOverviewSignal(incident)

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod, pod)
    }

    @MainActor
    func testLoadDemoClusterWinsOverPendingBootstrap() async throws {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let state = RuneAppState()
        state.setContexts([KubeContext(name: "previous-context")])
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer()
        )

        viewModel.bootstrap()
        viewModel.loadDemoCluster()
        try await Task.sleep(nanoseconds: 50_000_000)

        XCTAssertEqual(state.contexts.map(\.name), ["previous-context", "rune-demo"])
        XCTAssertEqual(state.selectedContext?.name, "rune-demo")
        XCTAssertEqual(state.selectedNamespace, "demo")
        XCTAssertFalse(viewModel.isLaunchExperienceVisible)
        XCTAssertTrue(state.kubeConfigSources.isEmpty)
    }

    @MainActor
    func testResetDemoClusterRestoresInMemorySnapshot() {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)

        viewModel.loadDemoCluster()
        state.setPods([])
        state.setEvents([])
        state.selectedSection = .terminal
        state.setPodLogs("mutated demo logs")

        viewModel.resetDemoCluster()

        XCTAssertEqual(state.selectedContext?.name, "rune-demo")
        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertEqual(state.pods.count, 3)
        XCTAssertNotNil(state.pods.first { $0.status == "CrashLoopBackOff" })
        XCTAssertNotNil(state.events.first { $0.type == "Warning" })
        XCTAssertTrue(state.podLogs.contains("started demo API"))
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testDemoClusterDetailsAndLogsStayInMemoryWhenSwitchingSelections() async throws {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, kubeClient: KubernetesClient(commandTimeout: 0.01))
        viewModel.loadDemoCluster()

        viewModel.setSection(RuneSection.workloads)
        viewModel.setWorkloadKind(KubeResourceKind.deployment)
        try await waitUntilForRuneAppState {
            state.resourceYAML.contains("kind: Deployment")
                && state.resourceDescribe.contains("Deployment")
                && state.unifiedServiceLogs.contains("deployment rollout")
                && !state.isLoadingResourceDetails
                && !state.isLoadingLogs
        }

        viewModel.setSection(RuneSection.helm)
        try await waitUntilForRuneAppState {
            state.helmValues.contains("replicaCount")
                && state.helmManifest.contains("kind: Deployment")
                && state.helmHistory.map(\.revision) == [1, 2]
        }

        viewModel.selectOperatorResource(state.operatorResources.first)
        try await waitUntilForRuneAppState {
            state.resourceYAML.contains("apiVersion: operators.coreos.com")
                && state.resourceDescribe.contains("Operator")
                && !state.isLoadingResourceDetails
        }

        XCTAssertNil(state.lastResourceYAMLError)
        XCTAssertNil(state.lastResourceDescribeError)
        XCTAssertNil(state.lastLogFetchError)
    }

    @MainActor
    func testResourceInspectorRefreshRetainsDocumentsDuringSameScopeReload() async throws {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            restoreUserDefaultsValue(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        viewModel.loadDemoCluster()
        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.deployment)

        try await waitUntilForRuneAppState {
            state.resourceYAML.contains("kind: Deployment")
                && state.resourceDescribe.contains("Deployment")
                && !state.isLoadingResourceDetails
        }

        state.setResourceYAML("visible YAML while refreshing")
        state.setResourceDescribe("visible describe while refreshing")
        viewModel.refreshResourceInspectorOnly()

        XCTAssertEqual(state.resourceYAML, "visible YAML while refreshing")
        XCTAssertEqual(state.resourceDescribe, "visible describe while refreshing")
        XCTAssertTrue(state.isLoadingResourceDetails)

        try await waitUntilForRuneAppState {
            state.resourceYAML.contains("kind: Deployment")
                && state.resourceDescribe.contains("Deployment")
                && !state.isLoadingResourceDetails
        }
        XCTAssertNil(state.lastResourceYAMLError)
        XCTAssertNil(state.lastResourceDescribeError)
    }

    @MainActor
    func testResourceInspectorRefreshFailureRetainsSameScopeDocuments() async throws {
        let context = KubeContext(name: "context-synthetic")
        let deployment = DeploymentSummary(
            name: "deployment-synthetic",
            namespace: "namespace-synthetic",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        let state = RuneAppState()
        state.setContexts([context])
        state.selectedContext = context
        state.selectedNamespace = deployment.namespace
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .deployment
        state.setDeployments([deployment])
        state.setSelectedDeployment(deployment)
        state.beginResourceDetailLoad(scope: ResourceDetailScope(
            contextName: context.name,
            namespace: deployment.namespace,
            kind: .deployment,
            name: deployment.name
        ))
        state.setResourceYAML("cached YAML document")
        state.setResourceDescribe("cached describe document")
        state.finishResourceDetailLoad()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 0.01)
        )

        viewModel.refreshResourceInspectorOnly()

        XCTAssertTrue(state.isLoadingResourceDetails)
        XCTAssertEqual(state.resourceYAML, "cached YAML document")
        XCTAssertEqual(state.resourceDescribe, "cached describe document")

        try await waitUntilForRuneAppState {
            !state.isLoadingResourceDetails
                && state.lastResourceYAMLError != nil
                && state.lastResourceDescribeError != nil
        }

        XCTAssertEqual(state.resourceYAML, "cached YAML document")
        XCTAssertEqual(state.resourceDescribe, "cached describe document")
        XCTAssertTrue(state.resourceYAMLHasUnsavedEdits == false)
    }

    @MainActor
    func testResourceInspectorRefreshScopeChangePerformsFullReset() async throws {
        let context = KubeContext(name: "context-synthetic")
        let previous = DeploymentSummary(
            name: "deployment-previous",
            namespace: "namespace-synthetic",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        let current = DeploymentSummary(
            name: "deployment-current",
            namespace: "namespace-synthetic",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        let state = RuneAppState()
        state.setContexts([context])
        state.selectedContext = context
        state.selectedNamespace = current.namespace
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .deployment
        state.setDeployments([previous, current])
        state.setSelectedDeployment(current)
        state.beginResourceDetailLoad(scope: ResourceDetailScope(
            contextName: context.name,
            namespace: previous.namespace,
            kind: .deployment,
            name: previous.name
        ))
        state.setResourceYAML("stale YAML from previous selection")
        state.setResourceDescribe("stale describe from previous selection")
        state.finishResourceDetailLoad()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 0.01)
        )

        viewModel.refreshResourceInspectorOnly()

        XCTAssertTrue(state.isLoadingResourceDetails)
        XCTAssertTrue(state.resourceYAML.isEmpty)
        XCTAssertTrue(state.resourceDescribe.isEmpty)
        XCTAssertEqual(state.resourceDetailScope?.name, current.name)

        try await waitUntilForRuneAppState {
            !state.isLoadingResourceDetails
        }
    }

    @MainActor
    func testSelectedHelmInspectorRefreshReloadsReleaseAndOperatorDocuments() async throws {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            restoreUserDefaultsValue(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
        }

        let exporter = RecordingFileExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        viewModel.loadDemoCluster()
        viewModel.setSection(.helm)

        try await waitUntilForRuneAppState {
            state.helmValues.contains("replicaCount")
                && state.helmManifest.contains("kind: Deployment")
                && state.helmHistory.map(\.revision) == [1, 2]
        }

        state.setHelmValues("stale values")
        state.setHelmManifest("stale manifest")
        state.setHelmHistory([])
        viewModel.refreshSelectedHelmInspector()

        try await waitUntilForRuneAppState {
            state.helmValues.contains("replicaCount")
                && state.helmManifest.contains("kind: Deployment")
                && state.helmHistory.map(\.revision) == [1, 2]
        }

        let operatorResource = try XCTUnwrap(state.operatorResources.first)
        viewModel.selectOperatorResource(operatorResource)
        try await waitUntilForRuneAppState {
            state.resourceYAML.contains("apiVersion: operators.coreos.com")
                && state.resourceDescribe.contains("Operator")
                && !state.isLoadingResourceDetails
        }

        state.setResourceYAML("stale operator YAML")
        state.setResourceDescribe("stale operator describe")
        viewModel.refreshSelectedHelmInspector()

        XCTAssertEqual(state.resourceYAML, "stale operator YAML")
        XCTAssertEqual(state.resourceDescribe, "stale operator describe")
        XCTAssertTrue(state.isLoadingResourceDetails)

        try await waitUntilForRuneAppState {
            state.resourceYAML.contains("apiVersion: operators.coreos.com")
                && state.resourceDescribe.contains(operatorResource.name)
                && !state.isLoadingResourceDetails
        }

        viewModel.saveCurrentResourceYAML()
        viewModel.saveCurrentResourceDescribe()

        XCTAssertEqual(exporter.saves.count, 2)
        XCTAssertTrue(exporter.saves[0].suggestedName.hasSuffix(".yaml"))
        XCTAssertTrue(exporter.saves[1].suggestedName.contains("-describe-"))
        XCTAssertEqual(String(data: exporter.saves[0].data, encoding: .utf8), state.resourceYAML)
        XCTAssertEqual(String(data: exporter.saves[1].data, encoding: .utf8), state.resourceDescribe)
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testOperatorInspectorRefreshFailureRetainsSameScopeDocuments() async throws {
        let context = KubeContext(name: "context-synthetic")
        let resource = OperatorResourceSummary(
            family: "Synthetic operator",
            kind: "SyntheticResource",
            apiPath: "/apis/synthetic.example/v1/namespaces/namespace-synthetic/resources",
            name: "resource-synthetic",
            namespace: "namespace-synthetic",
            status: "Ready",
            message: "Synthetic test resource"
        )
        let state = RuneAppState()
        state.setContexts([context])
        state.selectedContext = context
        state.selectedNamespace = resource.namespace ?? "default"
        state.selectedSection = .helm
        state.setOperatorResources([resource])
        state.setSelectedOperatorResource(resource)
        state.beginResourceDetailLoad(scope: ResourceDetailScope(
            contextName: context.name,
            namespace: resource.namespace,
            kind: resource.kind,
            name: resource.name
        ))
        state.setResourceYAML("cached operator YAML")
        state.setResourceDescribe("cached operator describe")
        state.finishResourceDetailLoad()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: KubernetesClient(commandTimeout: 0.01)
        )

        viewModel.refreshSelectedHelmInspector()

        XCTAssertTrue(state.isLoadingResourceDetails)
        XCTAssertEqual(state.resourceYAML, "cached operator YAML")
        XCTAssertEqual(state.resourceDescribe, "cached operator describe")

        try await waitUntilForRuneAppState {
            !state.isLoadingResourceDetails
                && state.lastResourceYAMLError != nil
                && state.lastResourceDescribeError != nil
        }

        XCTAssertEqual(state.resourceYAML, "cached operator YAML")
        XCTAssertEqual(state.resourceDescribe, "cached operator describe")
    }

    @MainActor
    func testTerminalPodInspectorCanLoadLogsAndYAMLWithoutLeavingTerminal() async throws {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        viewModel.loadDemoCluster()
        state.selectedSection = .terminal

        let pod = try XCTUnwrap(state.pods.last)
        viewModel.focusTerminalPodInspector(pod, reloadLogs: true, loadDetails: true)

        XCTAssertEqual(state.selectedSection, .terminal)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.id, pod.id)
        try await waitUntilForRuneAppState {
            state.resourceYAML.contains("name: \(pod.name)")
                && state.podLogs.contains("demo API")
                && !state.isLoadingResourceDetails
                && !state.isLoadingLogs
        }
    }

    @MainActor
    func testTerminalPodInspectorDefaultsLogContainerForMultiContainerPods() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let pod = PodSummary(
            name: "alpha-log-matrix",
            namespace: "alpha-zone",
            status: "Running",
            containerNamesLine: "main, sidecar"
        )
        state.setPods([pod])
        state.selectedSection = .terminal

        viewModel.focusTerminalPodInspector(pod, reloadLogs: true, loadDetails: false)

        XCTAssertEqual(state.selectedSection, .terminal)
        XCTAssertEqual(state.selectedPod?.id, pod.id)
        XCTAssertEqual(viewModel.selectedLogContainer, "main")
    }

    @MainActor
    func testOverviewClusterUsageCanUpdateWithoutReplacingOverviewSnapshot() {
        let state = RuneAppState()
        state.setOverviewSnapshot(
            pods: [],
            deploymentsCount: 3,
            servicesCount: 2,
            ingressesCount: 1,
            configMapsCount: 4,
            cronJobsCount: 5,
            nodesCount: 6,
            clusterCPUPercent: nil,
            clusterMemoryPercent: nil,
            events: []
        )

        state.setOverviewClusterUsage(cpuPercent: 17, memoryPercent: 42)

        XCTAssertEqual(state.overviewClusterCPUPercent, 17)
        XCTAssertEqual(state.overviewClusterMemoryPercent, 42)
        XCTAssertEqual(state.overviewDeploymentsCount, 3)
        XCTAssertEqual(state.overviewServicesCount, 2)
        XCTAssertEqual(state.overviewNodesCount, 6)
    }

    @MainActor
    func testOverviewIncidentAndDependencyProjectionsUseCurrentSnapshot() throws {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let pods = [
            PodSummary(
                name: "synthetic-api-7f6d9c7b5c-a1b2c",
                namespace: "namespace-a",
                status: "Running",
                totalRestarts: 3,
                containersReady: "1/2"
            ),
            PodSummary(
                name: "synthetic-worker-5f6d7c8b9d-d4e5f",
                namespace: "namespace-a",
                status: "CrashLoopBackOff",
                totalRestarts: 7,
                containersReady: "0/1"
            )
        ]
        let deployments = [
            DeploymentSummary(
                name: "synthetic-api",
                namespace: "namespace-a",
                readyReplicas: 1,
                desiredReplicas: 2,
                selector: ["app": "synthetic-api"]
            )
        ]
        let services = [
            ServiceSummary(
                name: "synthetic-api",
                namespace: "namespace-a",
                type: "ClusterIP",
                clusterIP: "10.0.0.1",
                selector: ["app": "synthetic-api"]
            )
        ]
        let events = [
            EventSummary(
                type: "Warning",
                reason: "BackOff",
                objectName: "synthetic-worker-5f6d7c8b9d-d4e5f",
                message: "Back-off restarting synthetic container",
                lastTimestamp: "2026-05-13T10:00:00Z",
                involvedKind: "Pod",
                involvedNamespace: "namespace-a"
            )
        ]
        state.setPods(pods)
        state.setDeployments(deployments)
        state.setServices(services)
        state.setOverviewSnapshot(
            pods: pods,
            deploymentsCount: deployments.count,
            servicesCount: services.count,
            ingressesCount: 0,
            configMapsCount: 0,
            cronJobsCount: 0,
            nodesCount: 1,
            events: events
        )

        XCTAssertTrue(viewModel.overviewUnhealthyItems.contains { $0.title == "synthetic-worker-5f6d7c8b9d-d4e5f" })
        XCTAssertTrue(viewModel.overviewUnhealthyItems.contains { $0.detail == "1/2 containers ready" })
        XCTAssertFalse(viewModel.overviewUnhealthyItems.contains { $0.badge == "Event" })
        XCTAssertTrue(viewModel.overviewIncidentTimelineItems.contains { $0.title.contains("BackOff") })
        XCTAssertFalse(viewModel.overviewIncidentTimelineItems.contains { $0.title.contains("Restart") })
        XCTAssertEqual(viewModel.overviewDependencyItems.first?.source, "Service/synthetic-api")
        XCTAssertEqual(viewModel.overviewDependencyItems.first?.target, "Deployment/synthetic-api")

        let unhealthyPod = try XCTUnwrap(viewModel.overviewUnhealthyItems.first { $0.title == "synthetic-worker-5f6d7c8b9d-d4e5f" })
        viewModel.openOverviewSignal(unhealthyPod)
        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.name, "synthetic-worker-5f6d7c8b9d-d4e5f")
    }

    @MainActor
    func testOverviewOperatorSignalNavigationOpensOperatorResource() throws {
        let state = RuneAppState()
        state.selectedSection = .overview
        let certificate = OperatorResourceSummary(
            family: "cert-manager",
            kind: "Certificates",
            apiPath: "/apis/cert-manager.io/v1/namespaces/synthetic/certificates",
            name: "api-tls",
            namespace: "synthetic",
            status: "Ready False",
            message: "Certificate renewal failed"
        )
        state.setOperatorResources([certificate])
        let viewModel = RuneAppViewModel(state: state, kubeClient: KubernetesClient(commandTimeout: 0.01))

        let signal = try XCTUnwrap(viewModel.overviewUnhealthyItems.first { $0.title == "api-tls" })
        XCTAssertEqual(signal.operatorResourceID, certificate.id)

        viewModel.openOverviewSignal(signal)

        XCTAssertEqual(state.selectedSection, .helm)
        XCTAssertEqual(viewModel.operatorResourceFocus, .all)
        XCTAssertEqual(state.selectedOperatorResource, certificate)
        XCTAssertNil(state.selectedHelmRelease)
    }

    @MainActor
    func testOverviewGitOpsSignalNavigationOpensOperatorResourceWithGitOpsFocus() throws {
        let state = RuneAppState()
        state.selectedSection = .overview
        let application = OperatorResourceSummary(
            family: "ArgoCD",
            kind: "Applications",
            apiPath: "/apis/argoproj.io/v1alpha1/namespaces/synthetic/applications",
            name: "payments",
            namespace: "synthetic",
            status: "OutOfSync Degraded",
            message: "Application health is degraded"
        )
        state.setOperatorResources([application])
        let viewModel = RuneAppViewModel(state: state, kubeClient: KubernetesClient(commandTimeout: 0.01))

        let signal = try XCTUnwrap(viewModel.overviewUnhealthyItems.first { $0.title == "payments" })
        XCTAssertEqual(signal.operatorResourceID, application.id)

        viewModel.openOverviewSignal(signal)

        XCTAssertEqual(state.selectedSection, .helm)
        XCTAssertEqual(viewModel.operatorResourceFocus, .gitOps)
        XCTAssertEqual(state.selectedOperatorResource, application)
    }

    @MainActor
    func testOverviewGitOpsRollupSummarizesControllersAndOpensFocus() throws {
        let state = RuneAppState()
        state.selectedSection = .overview
        state.setOperatorResources([
            OperatorResourceSummary(
                family: "Flux",
                kind: "Kustomizations",
                apiPath: "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/synthetic/kustomizations",
                name: "apps",
                namespace: "synthetic",
                status: "Ready False",
                message: "Reconciliation failed"
            ),
            OperatorResourceSummary(
                family: "Flux",
                kind: "HelmReleases",
                apiPath: "/apis/helm.toolkit.fluxcd.io/v2/namespaces/synthetic/helmreleases",
                name: "frontend",
                namespace: "synthetic",
                status: "Ready True",
                message: "Release is in sync"
            ),
            OperatorResourceSummary(
                family: "ArgoCD",
                kind: "Applications",
                apiPath: "/apis/argoproj.io/v1alpha1/namespaces/synthetic/applications",
                name: "payments",
                namespace: "synthetic",
                status: "OutOfSync Degraded",
                message: "Application health is degraded"
            ),
            OperatorResourceSummary(
                family: "cert-manager",
                kind: "Certificates",
                apiPath: "/apis/cert-manager.io/v1/namespaces/synthetic/certificates",
                name: "api-tls",
                namespace: "synthetic",
                status: "Ready True",
                message: "Certificate is ready"
            )
        ])
        let viewModel = RuneAppViewModel(state: state, kubeClient: KubernetesClient(commandTimeout: 0.01))

        let rollups = viewModel.overviewGitOpsRollupItems

        XCTAssertEqual(rollups.map(\.title), ["GitOps resources", "Flux", "ArgoCD", "Unhealthy GitOps"])
        XCTAssertEqual(rollups[0].detail, "3 loaded • Flux 2 • ArgoCD 1")
        XCTAssertEqual(rollups[1].detail, "2 resources • 1 unhealthy")
        XCTAssertEqual(rollups[2].detail, "1 resource • 1 unhealthy")
        XCTAssertEqual(rollups[3].detail, "2 resources need attention")
        XCTAssertEqual(rollups[0].severity, .warning)
        XCTAssertEqual(rollups[3].severity, .critical)

        viewModel.openOverviewGitOpsRollup(try XCTUnwrap(rollups.first { $0.controller == .flux }))
        XCTAssertEqual(state.selectedSection, .helm)
        XCTAssertEqual(viewModel.operatorResourceFocus, .flux)

        viewModel.openOverviewGitOpsRollup(try XCTUnwrap(rollups.first { $0.controller == .unhealthy }))
        XCTAssertEqual(state.selectedSection, .helm)
        XCTAssertEqual(viewModel.operatorResourceFocus, .unhealthy)
    }

    @MainActor
    func testDeploymentRelationshipNavigationOpensLikelyOwnedPodWithoutReload() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pods = [
            PodSummary(name: "api-7c9d8f6b5c-abc12", namespace: "synthetic", status: "Running"),
            PodSummary(name: "api-7c9d8f6b5c-def34", namespace: "synthetic", status: "Pending"),
            PodSummary(name: "worker-7c9d8f6b5c-def34", namespace: "synthetic", status: "Running")
        ]
        let replicaSets = [
            ClusterResourceSummary(kind: .replicaSet, name: "api-7c9d8f6b5c", namespace: "synthetic", primaryText: "1/2 ready", secondaryText: "Owned by Deployment/api"),
            ClusterResourceSummary(kind: .replicaSet, name: "worker-7c9d8f6b5c", namespace: "synthetic", primaryText: "1/1 ready", secondaryText: "Owned by Deployment/worker")
        ]
        let deployment = DeploymentSummary(
            name: "api",
            namespace: "synthetic",
            readyReplicas: 1,
            desiredReplicas: 1,
            selector: ["app": "api"]
        )
        state.setPods(pods)
        state.setReplicaSets(replicaSets)
        state.setDeployments([deployment])
        state.setSelectedDeployment(deployment)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .deployment
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedDeploymentRelatedPods.map(\.name), ["api-7c9d8f6b5c-abc12", "api-7c9d8f6b5c-def34"])
        XCTAssertEqual(viewModel.selectedDeploymentRelatedReplicaSets.map(\.name), ["api-7c9d8f6b5c"])

        viewModel.openDeploymentRelatedPod(pods[1])

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.name, "api-7c9d8f6b5c-def34")

        viewModel.openDeploymentRelatedReplicaSet(replicaSets[0])

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .replicaSet)
        XCTAssertEqual(state.selectedReplicaSet?.name, "api-7c9d8f6b5c")
    }

    @MainActor
    func testOwnerReferenceRelationshipNavigationDoesNotRequireNamePrefixOrWriteAudit() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let deployment = DeploymentSummary(
            name: "api",
            namespace: "synthetic",
            readyReplicas: 1,
            desiredReplicas: 1
        )
        let replicaSet = ClusterResourceSummary(
            kind: .replicaSet,
            name: "generated-rs",
            namespace: "synthetic",
            primaryText: "1/1 ready",
            secondaryText: "ReplicaSet",
            ownerReferencesLine: "Deployment/api"
        )
        let pod = PodSummary(
            name: "generated-pod",
            namespace: "synthetic",
            status: "Running",
            ownerReferencesLine: "ReplicaSet/generated-rs"
        )
        state.setDeployments([deployment])
        state.setReplicaSets([replicaSet])
        state.setPods([pod])
        state.setSelectedDeployment(deployment)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .deployment
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedDeploymentRelatedReplicaSets.map(\.name), ["generated-rs"])
        XCTAssertEqual(viewModel.selectedDeploymentRelatedPods.map(\.name), ["generated-pod"])

        viewModel.openDeploymentRelatedPod(pod)

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.name, "generated-pod")
        XCTAssertTrue(state.writeAuditLog.isEmpty)
    }

    @MainActor
    func testDeploymentRelationshipPodsUseRelatedReplicaSetPrefixBeforeDeploymentPrefix() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pods = [
            PodSummary(name: "api-v2-7c9d8f6b5c-abc12", namespace: "synthetic", status: "Running"),
            PodSummary(name: "api-legacy-abc12", namespace: "synthetic", status: "Running")
        ]
        let replicaSet = ClusterResourceSummary(
            kind: .replicaSet,
            name: "api-v2-7c9d8f6b5c",
            namespace: "synthetic",
            primaryText: "1/1 ready",
            secondaryText: "Owned by Deployment/api"
        )
        let deployment = DeploymentSummary(
            name: "api",
            namespace: "synthetic",
            readyReplicas: 1,
            desiredReplicas: 1,
            selector: ["app": "api"]
        )
        state.setPods(pods)
        state.setReplicaSets([replicaSet])
        state.setDeployments([deployment])
        state.setSelectedDeployment(deployment)
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(
            viewModel.selectedDeploymentRelatedPods.map(\.name),
            ["api-v2-7c9d8f6b5c-abc12", "api-legacy-abc12"]
        )
    }

    @MainActor
    func testServiceRelationshipNavigationUsesMatchingDeploymentSelectorBeforePodPrefixFallback() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pods = [
            PodSummary(name: "api-7c9d8f6b5c-abc12", namespace: "synthetic", status: "Running"),
            PodSummary(name: "api-7c9d8f6b5c-def34", namespace: "synthetic", status: "Pending"),
            PodSummary(name: "service-name-only-abc12", namespace: "synthetic", status: "Running")
        ]
        let deployment = DeploymentSummary(
            name: "api",
            namespace: "synthetic",
            readyReplicas: 1,
            desiredReplicas: 1,
            selector: ["app": "api", "tier": "web"]
        )
        let service = ServiceSummary(
            name: "service-name-only",
            namespace: "synthetic",
            type: "ClusterIP",
            clusterIP: "10.96.0.10",
            selector: ["app": "api"]
        )
        state.setPods(pods)
        state.setDeployments([deployment])
        state.setServices([service])
        state.setSelectedService(service)
        state.selectedSection = .networking
        state.selectedWorkloadKind = .service
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedServiceRelatedPods.map(\.name), ["api-7c9d8f6b5c-abc12", "api-7c9d8f6b5c-def34"])

        viewModel.openServiceRelatedPod(pods[1])

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.name, "api-7c9d8f6b5c-def34")
    }

    @MainActor
    func testIngressRelationshipNavigationOpensBackendService() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let api = ServiceSummary(
            name: "api",
            namespace: "synthetic",
            type: "ClusterIP",
            clusterIP: "10.96.0.10",
            selector: ["app": "api"]
        )
        let metrics = ServiceSummary(
            name: "metrics",
            namespace: "synthetic",
            type: "ClusterIP",
            clusterIP: "10.96.0.11",
            selector: ["app": "metrics"]
        )
        let ingress = ClusterResourceSummary(
            kind: .ingress,
            name: "api-public",
            namespace: "synthetic",
            primaryText: "api.synthetic.example",
            secondaryText: "Service api:80, metrics:9090"
        )
        state.setServices([api, metrics])
        state.setIngresses([ingress])
        state.setSelectedIngress(ingress)
        state.setSelectedService(api)
        state.selectedSection = .networking
        state.selectedWorkloadKind = .ingress
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedIngressRelatedServices.map(\.name), ["api", "metrics"])
        XCTAssertEqual(viewModel.selectedServiceRelatedIngresses.map(\.name), ["api-public"])

        viewModel.openIngressRelatedService(metrics)

        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .service)
        XCTAssertEqual(state.selectedService?.name, "metrics")

        viewModel.openServiceRelatedIngress(ingress)

        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .ingress)
        XCTAssertEqual(state.selectedIngress?.name, "api-public")
    }

    @MainActor
    func testOverviewDependencyProjectionIncludesIngressBackendServices() throws {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        state.selectedSection = .overview
        let api = ServiceSummary(
            name: "api",
            namespace: "synthetic",
            type: "ClusterIP",
            clusterIP: "10.96.0.10",
            selector: ["app": "api"]
        )
        let metrics = ServiceSummary(
            name: "metrics",
            namespace: "synthetic",
            type: "ClusterIP",
            clusterIP: "10.96.0.11",
            selector: ["app": "metrics"]
        )
        let ingress = ClusterResourceSummary(
            kind: .ingress,
            name: "api-public",
            namespace: "synthetic",
            primaryText: "api.synthetic.example",
            secondaryText: "Services api:80, metrics:9090"
        )
        state.setServices([api, metrics])
        state.setIngresses([ingress])
        let viewModel = RuneAppViewModel(state: state)

        let dependencies = viewModel.overviewDependencyItems.filter { $0.source == "Ingress/api-public" }

        XCTAssertEqual(dependencies.map(\.target), ["Service/api", "Service/metrics"])
        XCTAssertEqual(dependencies.map(\.relation), ["routes to", "routes to"])
        let metricsDependency = try XCTUnwrap(dependencies.first { $0.target == "Service/metrics" })
        XCTAssertEqual(metricsDependency.detail, "api.synthetic.example")

        viewModel.openOverviewDependency(metricsDependency)

        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .service)
        XCTAssertEqual(state.selectedService?.name, "metrics")
    }

    @MainActor
    func testOverviewDependencyProjectionIncludesPersistentVolumeClaimBinding() throws {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        state.selectedSection = .overview
        let pvc = ClusterResourceSummary(
            kind: .persistentVolumeClaim,
            name: "postgres-data",
            namespace: "synthetic",
            primaryText: "Bound",
            secondaryText: "PV pv-postgres-data · 20Gi"
        )
        let pv = ClusterResourceSummary(
            kind: .persistentVolume,
            name: "pv-postgres-data",
            namespace: nil,
            primaryText: "Bound",
            secondaryText: "20Gi"
        )
        state.setPersistentVolumeClaims([pvc])
        state.setPersistentVolumes([pv])
        let viewModel = RuneAppViewModel(state: state)

        let dependency = try XCTUnwrap(viewModel.overviewDependencyItems.first { $0.source == "PVC/postgres-data" })

        XCTAssertEqual(dependency.relation, "binds to")
        XCTAssertEqual(dependency.target, "PV/pv-postgres-data")
        XCTAssertEqual(dependency.detail, "Bound")

        viewModel.openOverviewDependency(dependency)

        XCTAssertEqual(state.selectedSection, .storage)
        XCTAssertEqual(state.selectedWorkloadKind, .persistentVolume)
        XCTAssertEqual(state.selectedPersistentVolume?.name, "pv-postgres-data")
    }

    @MainActor
    func testPersistentVolumeClaimRelationshipNavigationOpensBoundPersistentVolume() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pvc = ClusterResourceSummary(
            kind: .persistentVolumeClaim,
            name: "postgres-data",
            namespace: "synthetic",
            primaryText: "Bound",
            secondaryText: "PV pv-postgres-data · 20Gi"
        )
        let pv = ClusterResourceSummary(
            kind: .persistentVolume,
            name: "pv-postgres-data",
            namespace: nil,
            primaryText: "Bound",
            secondaryText: "20Gi"
        )
        state.setPersistentVolumeClaims([pvc])
        state.setPersistentVolumes([pv])
        state.setSelectedPersistentVolumeClaim(pvc)
        state.selectedSection = .storage
        state.selectedWorkloadKind = .persistentVolumeClaim
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedPersistentVolumeClaimRelatedPersistentVolume?.name, "pv-postgres-data")

        viewModel.openPersistentVolumeClaimRelatedPersistentVolume(pv)

        XCTAssertEqual(state.selectedSection, .storage)
        XCTAssertEqual(state.selectedWorkloadKind, .persistentVolume)
        XCTAssertEqual(state.selectedPersistentVolume?.name, "pv-postgres-data")
    }

    @MainActor
    func testPersistentVolumeRelationshipNavigationOpensBoundClaims() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pvc = ClusterResourceSummary(
            kind: .persistentVolumeClaim,
            name: "postgres-data",
            namespace: "synthetic",
            primaryText: "Bound",
            secondaryText: "PV pv-postgres-data · 20Gi"
        )
        let otherPVC = ClusterResourceSummary(
            kind: .persistentVolumeClaim,
            name: "cache-data",
            namespace: "synthetic",
            primaryText: "Bound",
            secondaryText: "PV pv-cache-data · 5Gi"
        )
        let pv = ClusterResourceSummary(
            kind: .persistentVolume,
            name: "pv-postgres-data",
            namespace: nil,
            primaryText: "Bound",
            secondaryText: "20Gi"
        )
        state.setPersistentVolumeClaims([pvc, otherPVC])
        state.setPersistentVolumes([pv])
        state.setSelectedPersistentVolume(pv)
        state.selectedSection = .storage
        state.selectedWorkloadKind = .persistentVolume
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedPersistentVolumeRelatedPersistentVolumeClaims.map(\.name), ["postgres-data"])

        viewModel.openPersistentVolumeRelatedPersistentVolumeClaim(pvc)

        XCTAssertEqual(state.selectedSection, .storage)
        XCTAssertEqual(state.selectedWorkloadKind, .persistentVolumeClaim)
        XCTAssertEqual(state.selectedPersistentVolumeClaim?.name, "postgres-data")
    }

    @MainActor
    func testReplicaSetRelationshipNavigationOpensOwnedPod() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pods = [
            PodSummary(name: "api-7c9d8f6b5c-abc12", namespace: "synthetic", status: "Running"),
            PodSummary(name: "worker-7c9d8f6b5c-def34", namespace: "synthetic", status: "Running")
        ]
        let replicaSet = ClusterResourceSummary(
            kind: .replicaSet,
            name: "api-7c9d8f6b5c",
            namespace: "synthetic",
            primaryText: "1/1 ready",
            secondaryText: "Owned by Deployment/api"
        )
        state.setPods(pods)
        state.setReplicaSets([replicaSet])
        state.setSelectedReplicaSet(replicaSet)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .replicaSet
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedReplicaSetRelatedPods.map(\.name), ["api-7c9d8f6b5c-abc12"])

        viewModel.openReplicaSetRelatedPod(pods[0])

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.name, "api-7c9d8f6b5c-abc12")
    }

    @MainActor
    func testStatefulSetRelationshipNavigationOpensOrdinalPod() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pods = [
            PodSummary(name: "database-0", namespace: "synthetic", status: "Running"),
            PodSummary(name: "database-1", namespace: "synthetic", status: "Pending"),
            PodSummary(name: "api-0", namespace: "synthetic", status: "Running")
        ]
        let statefulSet = ClusterResourceSummary(
            kind: .statefulSet,
            name: "database",
            namespace: "synthetic",
            primaryText: "1/2 ready",
            secondaryText: "Stateful workload"
        )
        state.setPods(pods)
        state.setStatefulSets([statefulSet])
        state.setSelectedStatefulSet(statefulSet)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .statefulSet
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedStatefulSetRelatedPods.map(\.name), ["database-0", "database-1"])

        viewModel.openStatefulSetRelatedPod(pods[1])

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.name, "database-1")
    }

    @MainActor
    func testDaemonSetRelationshipNavigationOpensMatchingPodPrefix() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pods = [
            PodSummary(name: "node-agent-abc12", namespace: "synthetic", status: "Running"),
            PodSummary(name: "node-agent-def34", namespace: "synthetic", status: "Running"),
            PodSummary(name: "other-agent-abc12", namespace: "synthetic", status: "Running")
        ]
        let daemonSet = ClusterResourceSummary(
            kind: .daemonSet,
            name: "node-agent",
            namespace: "synthetic",
            primaryText: "2/2 ready",
            secondaryText: "Daemon workload"
        )
        state.setPods(pods)
        state.setDaemonSets([daemonSet])
        state.setSelectedDaemonSet(daemonSet)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .daemonSet
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedDaemonSetRelatedPods.map(\.name), ["node-agent-abc12", "node-agent-def34"])

        viewModel.openDaemonSetRelatedPod(pods[0])

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.name, "node-agent-abc12")
    }

    @MainActor
    func testJobRelationshipNavigationOpensCreatedPod() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pods = [
            PodSummary(name: "backup-28600123-x4k9p", namespace: "synthetic", status: "Succeeded"),
            PodSummary(name: "backup-28600123-z8m2q", namespace: "synthetic", status: "Running"),
            PodSummary(name: "other-28600123-x4k9p", namespace: "synthetic", status: "Succeeded")
        ]
        let job = ClusterResourceSummary(
            kind: .job,
            name: "backup-28600123",
            namespace: "synthetic",
            primaryText: "1/1 complete",
            secondaryText: "Age 2m"
        )
        state.setPods(pods)
        state.setJobs([job])
        state.setSelectedJob(job)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .job
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedJobRelatedPods.map(\.name), ["backup-28600123-x4k9p", "backup-28600123-z8m2q"])

        viewModel.openJobRelatedPod(pods[0])

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.name, "backup-28600123-x4k9p")
    }

    @MainActor
    func testCronJobRelationshipNavigationOpensCreatedJob() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let jobs = [
            ClusterResourceSummary(kind: .job, name: "backup-28600123", namespace: "synthetic", primaryText: "Complete", secondaryText: "Age 2m"),
            ClusterResourceSummary(kind: .job, name: "backup-28600124", namespace: "synthetic", primaryText: "Running", secondaryText: "Age 1m"),
            ClusterResourceSummary(kind: .job, name: "restore-28600124", namespace: "synthetic", primaryText: "Complete", secondaryText: "Age 1m")
        ]
        let cronJob = ClusterResourceSummary(
            kind: .cronJob,
            name: "backup",
            namespace: "synthetic",
            primaryText: "*/5 * * * *",
            secondaryText: "Active"
        )
        state.setJobs(jobs)
        state.setCronJobs([cronJob])
        state.setSelectedCronJob(cronJob)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .cronJob
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedCronJobRelatedJobs.map(\.name), ["backup-28600123", "backup-28600124"])

        viewModel.openCronJobRelatedJob(jobs[1])

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .job)
        XCTAssertEqual(state.selectedJob?.name, "backup-28600124")
    }

    @MainActor
    func testHorizontalPodAutoscalerRelationshipNavigationOpensScaleTarget() throws {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let deployment = DeploymentSummary(
            name: "api",
            namespace: "synthetic",
            readyReplicas: 2,
            desiredReplicas: 3,
            selector: ["app": "api"]
        )
        let hpa = ClusterResourceSummary(
            kind: .horizontalPodAutoscaler,
            name: "api",
            namespace: "synthetic",
            primaryText: "2-6 replicas (current 3)",
            secondaryText: "Deployment/api"
        )
        state.setDeployments([deployment])
        state.setHorizontalPodAutoscalers([hpa])
        state.setSelectedHorizontalPodAutoscaler(hpa)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .horizontalPodAutoscaler
        let viewModel = RuneAppViewModel(state: state)

        let target = try XCTUnwrap(viewModel.selectedHorizontalPodAutoscalerScaleTarget)
        XCTAssertEqual(target.kind, .deployment)
        XCTAssertEqual(target.name, "api")
        XCTAssertEqual(target.subtitle, "synthetic · 2/3 replicas")

        viewModel.openHorizontalPodAutoscalerScaleTarget(target)

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .deployment)
        XCTAssertEqual(state.selectedDeployment?.name, "api")
    }

    @MainActor
    func testNodeRelationshipNavigationOpensScheduledPod() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pods = [
            PodSummary(name: "api-0", namespace: "synthetic", status: "Running", nodeName: "node-a"),
            PodSummary(name: "worker-0", namespace: "synthetic", status: "Running", nodeName: "node-b"),
            PodSummary(name: "api-1", namespace: "synthetic", status: "Pending")
        ]
        let node = ClusterResourceSummary(
            kind: .node,
            name: "node-a",
            namespace: nil,
            primaryText: "Ready",
            secondaryText: "v1.30.0"
        )
        state.setPods(pods)
        state.setNodes([node])
        state.setSelectedNode(node)
        state.selectedSection = .storage
        state.selectedWorkloadKind = .node
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedNodeRelatedPods.map(\.name), ["api-0"])

        viewModel.openNodeRelatedPod(pods[0])

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertEqual(state.selectedPod?.name, "api-0")
    }

    @MainActor
    func testPodRelationshipNavigationOpensScheduledNode() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pod = PodSummary(name: "api-0", namespace: "synthetic", status: "Running", nodeName: "node-a")
        let node = ClusterResourceSummary(
            kind: .node,
            name: "node-a",
            namespace: nil,
            primaryText: "Ready",
            secondaryText: "v1.30.0"
        )
        state.setPods([pod])
        state.setNodes([node])
        state.setSelectedPod(pod)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedPodRelatedNode?.name, "node-a")

        viewModel.openPodRelatedNode(node)

        XCTAssertEqual(state.selectedSection, .storage)
        XCTAssertEqual(state.selectedWorkloadKind, .node)
        XCTAssertEqual(state.selectedNode?.name, "node-a")
    }

    @MainActor
    func testRelatedEventsNavigationFiltersNamespaceAndDeduplicatesOverviewEvents() {
        let state = RuneAppState()
        state.selectedNamespace = "synthetic"
        let pod = PodSummary(name: "api-0", namespace: "synthetic", status: "Running", nodeName: "node-a")
        let deployment = DeploymentSummary(name: "api", namespace: "synthetic", readyReplicas: 1, desiredReplicas: 1)
        let service = ServiceSummary(name: "api", namespace: "synthetic", type: "ClusterIP", clusterIP: "10.96.0.10")
        let matchingEvent = EventSummary(
            type: "Warning",
            reason: "BackOff",
            objectName: "api-0",
            message: "Back-off restarting failed container",
            lastTimestamp: "2026-05-05T10:03:00Z",
            involvedKind: "Pod",
            involvedNamespace: "synthetic"
        )
        let otherNamespaceEvent = EventSummary(
            type: "Warning",
            reason: "FailedScheduling",
            objectName: "api-0",
            message: "Unschedulable in another namespace",
            lastTimestamp: "2026-05-05T10:04:00Z",
            involvedKind: "Pod",
            involvedNamespace: "other"
        )
        let otherKindEvent = EventSummary(
            type: "Normal",
            reason: "ScalingReplicaSet",
            objectName: "api",
            message: "Deployment changed",
            lastTimestamp: "2026-05-05T10:05:00Z",
            involvedKind: "Deployment",
            involvedNamespace: "synthetic"
        )
        let serviceEvent = EventSummary(
            type: "Normal",
            reason: "UpdatedLoadBalancer",
            objectName: "api",
            message: "Service updated",
            lastTimestamp: "2026-05-05T10:06:00Z",
            involvedKind: "Service",
            involvedNamespace: "synthetic"
        )
        state.setPods([pod])
        state.setDeployments([deployment])
        state.setServices([service])
        state.setEvents([matchingEvent, otherNamespaceEvent, otherKindEvent, serviceEvent])
        state.setOverviewSnapshot(
            pods: [],
            deploymentsCount: 0,
            servicesCount: 0,
            ingressesCount: 0,
            configMapsCount: 0,
            cronJobsCount: 0,
            nodesCount: 0,
            events: [matchingEvent]
        )
        state.setSelectedPod(pod)
        state.setSelectedDeployment(deployment)
        state.setSelectedService(service)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedPodRelatedEvents.map(\.reason), ["BackOff"])
        state.setSelectedDeployment(deployment)
        XCTAssertEqual(viewModel.selectedDeploymentRelatedEvents.map(\.reason), ["ScalingReplicaSet"])
        state.setSelectedService(service)
        XCTAssertEqual(viewModel.selectedServiceRelatedEvents.map(\.reason), ["UpdatedLoadBalancer"])
        state.setSelectedPod(pod)

        viewModel.openRelatedEvent(matchingEvent)

        XCTAssertEqual(state.selectedSection, .events)
        XCTAssertEqual(state.selectedEvent?.reason, "BackOff")
    }

    @MainActor
    func testOverviewUnhealthyIgnoresSucceededCronJobPodsWithNotReadyContainers() {
        let projector = OverviewInsightsProjector(
            pods: [
                PodSummary(
                    name: "nightly-report-29600123-x4k9p",
                    namespace: "namespace-a",
                    status: "Succeeded",
                    totalRestarts: 1,
                    containersReady: "0/1"
                ),
                PodSummary(
                    name: "failed-report-29600124-z8m2q",
                    namespace: "namespace-a",
                    status: "Failed",
                    totalRestarts: 0,
                    containersReady: "0/1"
                )
            ],
            deployments: [],
            services: [],
            events: []
        )

        let unhealthy = projector.unhealthyItems()
        XCTAssertFalse(unhealthy.contains { $0.title == "nightly-report-29600123-x4k9p" })
        XCTAssertTrue(unhealthy.contains { $0.title == "failed-report-29600124-z8m2q" })
    }

    @MainActor
    func testOverviewUnhealthyIgnoresSingleRestartOnOtherwiseHealthyPod() {
        let projector = OverviewInsightsProjector(
            pods: [
                PodSummary(
                    name: "api-0",
                    namespace: "namespace-a",
                    status: "Running",
                    totalRestarts: 1,
                    containersReady: "1/1"
                ),
                PodSummary(
                    name: "worker-0",
                    namespace: "namespace-a",
                    status: "Running",
                    totalRestarts: 3,
                    containersReady: "1/1"
                )
            ],
            deployments: [],
            services: [],
            events: []
        )

        let unhealthy = projector.unhealthyItems()
        XCTAssertFalse(unhealthy.contains { $0.title == "api-0" })
        XCTAssertTrue(unhealthy.contains { $0.title == "worker-0" && $0.badge == "Restart" })
    }

    @MainActor
    func testOverviewUnhealthyIncludesCertificateExpiryAndGatewayReadinessSignals() throws {
        let now = try XCTUnwrap(ISO8601DateFormatter().date(from: "2026-06-15T10:00:00Z"))
        let projector = OverviewInsightsProjector(
            pods: [],
            deployments: [],
            services: [],
            events: [],
            operatorResources: [
                OperatorResourceSummary(
                    family: "cert-manager",
                    kind: "Certificates",
                    apiPath: "/apis/cert-manager.io/v1/namespaces/synthetic/certificates",
                    name: "api-tls",
                    namespace: "synthetic",
                    status: "Ready True",
                    message: "Certificate is up to date",
                    printerColumns: [
                        OperatorResourceSummary.PrinterColumn(title: "Not After", value: "2026-06-20T00:00:00Z")
                    ]
                ),
                OperatorResourceSummary(
                    family: "cert-manager",
                    kind: "Certificates",
                    apiPath: "/apis/cert-manager.io/v1/namespaces/synthetic/certificates",
                    name: "old-tls",
                    namespace: "synthetic",
                    status: "Ready True",
                    message: "Certificate is stale",
                    printerColumns: [
                        OperatorResourceSummary.PrinterColumn(title: "Expires", value: "2026-06-01")
                    ]
                ),
                OperatorResourceSummary(
                    family: "Gateway API",
                    kind: "Gateways",
                    apiPath: "/apis/gateway.networking.k8s.io/v1/namespaces/synthetic/gateways",
                    name: "edge",
                    namespace: "synthetic",
                    status: "Programmed False",
                    message: "Listener failed to attach"
                )
            ],
            now: now
        )

        let unhealthy = projector.unhealthyItems()
        XCTAssertTrue(unhealthy.contains { $0.title == "api-tls" && $0.badge == "Cert" && $0.detail == "Certificate expires 2026-06-20" && $0.severity == .warning && $0.operatorResourceID != nil })
        XCTAssertTrue(unhealthy.contains { $0.title == "old-tls" && $0.badge == "Cert" && $0.detail == "Certificate expired 2026-06-01" && $0.severity == .critical && $0.operatorResourceID != nil })
        XCTAssertTrue(unhealthy.contains { $0.title == "edge" && $0.badge == "Gateway" && $0.detail == "Listener failed to attach" && $0.severity == .critical && $0.operatorResourceID != nil })
    }

    @MainActor
    func testOverviewUnhealthyIncludesGitOpsDriftSignals() {
        let projector = OverviewInsightsProjector(
            pods: [],
            deployments: [],
            services: [],
            events: [],
            operatorResources: [
                OperatorResourceSummary(
                    family: "Flux",
                    kind: "Kustomizations",
                    apiPath: "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/synthetic/kustomizations",
                    name: "apps",
                    namespace: "synthetic",
                    status: "Ready False",
                    message: "Reconciliation failed"
                ),
                OperatorResourceSummary(
                    family: "ArgoCD",
                    kind: "Applications",
                    apiPath: "/apis/argoproj.io/v1alpha1/namespaces/synthetic/applications",
                    name: "payments",
                    namespace: "synthetic",
                    status: "OutOfSync Degraded",
                    message: "Application health is degraded"
                ),
                OperatorResourceSummary(
                    family: "Flux",
                    kind: "HelmReleases",
                    apiPath: "/apis/helm.toolkit.fluxcd.io/v2/namespaces/synthetic/helmreleases",
                    name: "healthy",
                    namespace: "synthetic",
                    status: "Ready True",
                    message: "Release is in sync"
                )
            ]
        )

        let unhealthy = projector.unhealthyItems()
        XCTAssertTrue(unhealthy.contains { $0.title == "apps" && $0.badge == "Flux" && $0.detail == "Reconciliation failed" && $0.severity == .critical && $0.operatorResourceID != nil })
        XCTAssertTrue(unhealthy.contains { $0.title == "payments" && $0.badge == "ArgoCD" && $0.detail == "Application health is degraded" && $0.severity == .critical && $0.operatorResourceID != nil })
        XCTAssertFalse(unhealthy.contains { $0.title == "healthy" })
    }

    @MainActor
    func testOverviewUnhealthyIncludesNodeAndJobSignals() {
        let projector = OverviewInsightsProjector(
            pods: [],
            deployments: [],
            services: [],
            events: [],
            jobs: [
                ClusterResourceSummary(
                    kind: .job,
                    name: "nightly-backup",
                    namespace: "synthetic",
                    primaryText: "Failed",
                    secondaryText: "BackoffLimitExceeded"
                ),
                ClusterResourceSummary(
                    kind: .job,
                    name: "report-rollup",
                    namespace: "synthetic",
                    primaryText: "Complete",
                    secondaryText: "Succeeded"
                )
            ],
            nodes: [
                ClusterResourceSummary(
                    kind: .node,
                    name: "node-a",
                    namespace: nil,
                    primaryText: "NotReady",
                    secondaryText: "Kubelet unreachable"
                ),
                ClusterResourceSummary(
                    kind: .node,
                    name: "node-b",
                    namespace: nil,
                    primaryText: "Ready",
                    secondaryText: "Healthy"
                )
            ]
        )

        let unhealthy = projector.unhealthyItems()
        XCTAssertTrue(unhealthy.contains { item in
            item.title == "nightly-backup"
                && item.badge == "Job"
                && item.severity == .critical
                && item.target == OverviewResourceReference(kind: .job, namespace: "synthetic", name: "nightly-backup")
        })
        XCTAssertTrue(unhealthy.contains { item in
            item.title == "node-a"
                && item.badge == "Node"
                && item.severity == .critical
                && item.target == OverviewResourceReference(kind: .node, namespace: nil, name: "node-a")
        })
        XCTAssertFalse(unhealthy.contains { $0.title == "report-rollup" })
        XCTAssertFalse(unhealthy.contains { $0.title == "node-b" })
    }

    func testAuthDoctorLogProbePrefersRunningReadyPodOverTerminalOrPendingPods() {
        let pods = [
            PodSummary(name: "finished-job", namespace: "namespace-a", status: "Succeeded", containersReady: "0/1"),
            PodSummary(name: "pending-job", namespace: "namespace-a", status: "Pending", containersReady: "0/1"),
            PodSummary(name: "running-but-not-ready", namespace: "namespace-a", status: "Running", containersReady: "0/1"),
            PodSummary(name: "running-ready", namespace: "namespace-a", status: "Running", containersReady: "1/1")
        ]

        XCTAssertEqual(RuneAppViewModel.authDoctorLogProbePod(from: pods)?.name, "running-ready")
    }

    func testAuthDoctorLogProbeFallsBackToSucceededPodWhenNoRunningPodsExist() {
        let pods = [
            PodSummary(name: "pending-job", namespace: "namespace-a", status: "Pending", containersReady: "0/1"),
            PodSummary(name: "finished-job", namespace: "namespace-a", status: "Succeeded", containersReady: "0/1")
        ]

        XCTAssertEqual(RuneAppViewModel.authDoctorLogProbePod(from: pods)?.name, "finished-job")
    }

    @MainActor
    func testOverviewInsightsProjectorLimitsLargeSignalsAndKeepsDependenciesBounded() {
        let pods = (0..<30).map { index in
            PodSummary(
                name: "synthetic-api-\(String(format: "%02d", index))-7d8c9f6b5-\(String(format: "%05d", index))",
                namespace: "namespace-a",
                status: index.isMultiple(of: 2) ? "CrashLoopBackOff" : "Running",
                totalRestarts: index.isMultiple(of: 3) ? 6 : 0,
                containersReady: index.isMultiple(of: 5) ? "0/1" : "1/1"
            )
        }
        let deployments = (0..<12).map { index in
            DeploymentSummary(
                name: "synthetic-api-\(String(format: "%02d", index))",
                namespace: "namespace-a",
                readyReplicas: index.isMultiple(of: 4) ? 0 : 1,
                desiredReplicas: 2,
                selector: ["app": "synthetic-api-\(String(format: "%02d", index))"]
            )
        }
        let services = (0..<12).map { index in
            ServiceSummary(
                name: "synthetic-api-\(String(format: "%02d", index))",
                namespace: "namespace-a",
                type: "ClusterIP",
                clusterIP: "10.0.0.\(index + 1)",
                selector: ["app": "synthetic-api-\(String(format: "%02d", index))"]
            )
        }
        let events = (0..<12).map { index in
            EventSummary(
                type: "Warning",
                reason: "BackOff",
                objectName: "synthetic-api-\(String(format: "%02d", index))",
                message: "Synthetic warning \(index)",
                lastTimestamp: "2026-05-13T10:00:00Z",
                involvedKind: "Pod",
                involvedNamespace: "namespace-a"
            )
        }
        let projector = OverviewInsightsProjector(
            pods: pods,
            deployments: deployments,
            services: services,
            events: events
        )

        XCTAssertEqual(projector.unhealthyItems().count, 8)
        XCTAssertEqual(projector.incidentTimelineItems().count, 8)
        XCTAssertLessThanOrEqual(projector.dependencyItems().count, 8)
        XCTAssertEqual(projector.dependencyItems().first?.source, "Service/synthetic-api-00")
        XCTAssertEqual(projector.dependencyItems().first?.target, "Deployment/synthetic-api-00")
    }

    @MainActor
    func testNamespaceOptionsAreAlphabetical() {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "cluster")
        state.setNamespaces(["zeta", "default", "Alpha", "beta"])
        state.selectedNamespace = "zeta"
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.namespaceOptions, ["Alpha", "beta", "default", "zeta"])
    }

    @MainActor
    func testContextMenuOptionsUseFavoriteFirstOrdering() {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = false
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let state = RuneAppState()
        state.setContexts([
            KubeContext(name: "prod"),
            KubeContext(name: "alpha"),
            KubeContext(name: "Beta")
        ])
        let viewModel = RuneAppViewModel(state: state)
        state.setFavoriteContextNames(["prod"])

        XCTAssertEqual(viewModel.contextMenuOptions.map(\.name), ["prod", "alpha", "Beta"])
        XCTAssertEqual(viewModel.visibleContexts.map(\.name), ["prod", "alpha", "Beta"])
    }

    @MainActor
    func testVisibleContextSearchUsesDisplayMetadataAliasTagsAndGroup() {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = false
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.contextSearchMetadata.\(UUID().uuidString)", isDirectory: true)
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = FileBackedContextPreferencesStore(url: directory.appendingPathComponent("context-preferences.json"))
        store.saveContextDisplayMetadata(
            ContextDisplayMetadata(
                alias: "Checkout Production",
                colorKey: "blue",
                iconName: "cloud",
                tags: ["payments", "platform"],
                group: "Provider clusters"
            ),
            for: "context-alpha"
        )
        store.saveContextDisplayMetadata(
            ContextDisplayMetadata(tags: ["observability"], group: "Internal tools"),
            for: "context-beta"
        )
        store.saveFavoriteContextNames(["context-beta"])

        let state = RuneAppState()
        state.setContexts([
            KubeContext(name: "context-alpha"),
            KubeContext(name: "context-beta"),
            KubeContext(name: "context-gamma")
        ])
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)

        viewModel.setContextSearchQuery("payments")
        XCTAssertEqual(viewModel.visibleContexts.map(\.name), ["context-alpha"])

        viewModel.setContextSearchQuery("internal")
        XCTAssertEqual(viewModel.visibleContexts.map(\.name), ["context-beta"])

        viewModel.setContextSearchQuery("context")
        XCTAssertEqual(viewModel.visibleContexts.map(\.name), ["context-beta", "context-alpha", "context-gamma"])
    }

    @MainActor
    func testCommandPaletteContextItemsUseDisplayMetadata() {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = false
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.commandPaletteContextMetadata.\(UUID().uuidString)", isDirectory: true)
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = FileBackedContextPreferencesStore(url: directory.appendingPathComponent("context-preferences.json"))
        store.saveContextDisplayMetadata(
            ContextDisplayMetadata(
                alias: "Checkout Production",
                iconName: "cloud",
                tags: ["payments", "platform"],
                group: "Provider clusters"
            ),
            for: "context-alpha"
        )

        let state = RuneAppState()
        state.setContexts([KubeContext(name: "context-alpha"), KubeContext(name: "context-beta")])
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)

        let commandItem = viewModel.commandPaletteItems(query: ":ctx payments").first
        XCTAssertEqual(commandItem?.title, "Checkout Production")
        XCTAssertTrue(commandItem?.subtitle.contains("context-alpha") == true)
        XCTAssertTrue(commandItem?.subtitle.contains("payments") == true)
        XCTAssertEqual(commandItem?.symbolName, "cloud")

        let globalItem = viewModel.commandPaletteItems(query: "Provider clusters").first { $0.id == "context:context-alpha" }
        XCTAssertEqual(globalItem?.title, "Checkout Production")
    }

    @MainActor
    func testCommandPaletteSearchKeepsUnicodeCaseFoldingAndInvalidatesContextMetadataCache() {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = false
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let suiteName = "RuneAppStateTests.commandPaletteUnicode.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        store.saveContextDisplayMetadata(
            ContextDisplayMetadata(alias: "München Produktion", tags: ["plattform"]),
            for: "synthetic-context"
        )

        let state = RuneAppState()
        let context = KubeContext(name: "synthetic-context")
        state.setContexts([context])
        state.selectedContext = context
        state.selectedNamespace = "first"
        state.setNamespaces(["first"])
        state.setPods([PodSummary(name: "initial-pod", namespace: "first", status: "Running")])
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)

        XCTAssertEqual(
            viewModel.commandPaletteItems(query: "MÜNCHEN").first { $0.id == "context:synthetic-context" }?.title,
            "München Produktion"
        )

        store.saveContextDisplayMetadata(
            ContextDisplayMetadata(alias: "Zürich Plattform", tags: ["drift"]),
            for: context.name
        )

        XCTAssertEqual(
            viewModel.commandPaletteItems(query: "zürich").first { $0.id == "context:synthetic-context" }?.title,
            "Zürich Plattform"
        )

        viewModel.toggleFavorite(for: context)
        XCTAssertEqual(
            viewModel.commandPaletteItems(query: "ZÜRICH").first { $0.id == "context:synthetic-context" }?.symbolName,
            "star.fill"
        )

        XCTAssertEqual(viewModel.commandPaletteItems(query: ":ns FIRST").first?.id, "cmd:namespace:first")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":po INITIAL").first?.title, "initial-pod")

        state.selectedNamespace = "second"
        state.setNamespaces(["second"])
        state.setPods([PodSummary(name: "fresh-pod", namespace: "second", status: "Running")])
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":ns SECOND").first?.id, "cmd:namespace:second")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":po FRESH").first?.title, "fresh-pod")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":po INITIAL").first?.id, "nav:pod")
    }

    @MainActor
    func testCommandPaletteGlobalSearchIsBoundedButResourceAliasesStillReachRows() {
        let state = RuneAppState()
        state.setContexts((0..<250).map { KubeContext(name: "synthetic-context-\($0)") })
        state.setPods([
            PodSummary(name: "needle-pod", namespace: "default", status: "Running")
        ])
        state.setDeployments([
            DeploymentSummary(name: "needle-deployment", namespace: "default", readyReplicas: 1, desiredReplicas: 1)
        ])
        state.setServices([
            ServiceSummary(name: "needle-service", namespace: "default", type: "ClusterIP", clusterIP: "synthetic-ip")
        ])
        func resource(_ kind: KubeResourceKind, _ name: String, namespace: String? = "default") -> ClusterResourceSummary {
            ClusterResourceSummary(kind: kind, name: name, namespace: namespace, primaryText: "ready", secondaryText: "synthetic")
        }
        state.setStatefulSets([resource(.statefulSet, "needle-statefulset")])
        state.setDaemonSets([resource(.daemonSet, "needle-daemonset")])
        state.setReplicaSets([resource(.replicaSet, "needle-replicaset")])
        state.setEndpoints([resource(.endpoint, "needle-endpoints")])
        state.setIngresses([resource(.ingress, "needle-ingress")])
        state.setPersistentVolumeClaims([resource(.persistentVolumeClaim, "needle-pvc")])
        state.setPersistentVolumes([resource(.persistentVolume, "needle-pv", namespace: nil)])
        state.setStorageClasses([resource(.storageClass, "needle-storageclass", namespace: nil)])
        state.setHorizontalPodAutoscalers([resource(.horizontalPodAutoscaler, "needle-hpa")])
        state.setNetworkPolicies([resource(.networkPolicy, "needle-networkpolicy")])
        state.setConfigMaps([resource(.configMap, "needle-configmap")])
        state.setSecrets([resource(.secret, "needle-secret")])
        state.setNodes([resource(.node, "needle-node", namespace: nil)])
        state.setCronJobs([resource(.cronJob, "needle-cronjob")])
        state.setJobs([resource(.job, "needle-job")])
        state.setRBACData(
            roles: [],
            serviceAccounts: [resource(.serviceAccount, "needle-serviceaccount")],
            roleBindings: [],
            clusterRoles: [],
            clusterRoleBindings: []
        )
        let viewModel = RuneAppViewModel(state: state)

        let globalItems = viewModel.commandPaletteItems(query: "synthetic")
        XCTAssertEqual(globalItems.count, 160)
        XCTAssertTrue(globalItems.allSatisfy { $0.id.hasPrefix("context:") })

        let podItems = viewModel.commandPaletteItems(query: ":po needle")
        XCTAssertEqual(podItems.first?.id, "cmd:pod:default/needle-pod")
        XCTAssertEqual(podItems.first?.title, "needle-pod")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":deploy needle").first?.title, "needle-deployment")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":svc needle").first?.title, "needle-service")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":sts needle").first?.title, "needle-statefulset")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":ds needle").first?.title, "needle-daemonset")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":rs needle").first?.title, "needle-replicaset")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":ep needle").first?.title, "needle-endpoints")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":ing needle").first?.title, "needle-ingress")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":pvc needle").first?.title, "needle-pvc")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":pv needle").first?.title, "needle-pv")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":sc needle").first?.title, "needle-storageclass")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":hpa needle").first?.title, "needle-hpa")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":np needle").first?.title, "needle-networkpolicy")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":cm needle").first?.title, "needle-configmap")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":sec needle").first?.title, "needle-secret")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":no needle").first?.title, "needle-node")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":cj needle").first?.title, "needle-cronjob")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":job needle").first?.title, "needle-job")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":sa needle").first?.title, "needle-serviceaccount")

        XCTAssertEqual(viewModel.commandPaletteItems(query: "po needle").first?.id, "cmd:pod:default/needle-pod")
        XCTAssertEqual(viewModel.commandPaletteItems(query: "deploy needle").first?.title, "needle-deployment")
        XCTAssertEqual(viewModel.commandPaletteItems(query: "svc needle").first?.title, "needle-service")
        XCTAssertEqual(viewModel.commandPaletteItems(query: "ep needle").first?.title, "needle-endpoints")
        XCTAssertEqual(viewModel.commandPaletteItems(query: "cm needle").first?.title, "needle-configmap")
        XCTAssertEqual(viewModel.commandPaletteItems(query: "sa needle").first?.title, "needle-serviceaccount")
    }

    @MainActor
    func testCommandPaletteBareNamespaceCommandMatchesColonCommand() {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-dev")
        state.selectedNamespace = "default"
        state.setNamespaces(["default", "payments"])
        state.isCommandPalettePresented = true
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.commandPaletteItems(query: ":ns pay").first?.id, "cmd:namespace:payments")
        XCTAssertEqual(viewModel.commandPaletteItems(query: "ns pay").first?.id, "cmd:namespace:payments")

        viewModel.executeCommandPaletteQuery("ns payments")

        XCTAssertFalse(state.isCommandPalettePresented)
        XCTAssertEqual(state.selectedNamespace, "payments")
    }

    @MainActor
    func testCommandPaletteColonShowsBuiltInCommandCheatSheet() {
        let viewModel = RuneAppViewModel(state: RuneAppState())

        let items = viewModel.commandPaletteItems(query: ":")

        XCTAssertFalse(items.isEmpty)
        XCTAssertTrue(items.allSatisfy { $0.id.hasPrefix("help:") })
        XCTAssertTrue(items.contains { $0.title == ":po <name>" && $0.subtitle == "Pods" })
        XCTAssertTrue(items.contains { $0.title == ":deploy <name>" && $0.subtitle == "Deployments" })
        XCTAssertTrue(items.contains { $0.title == ":svc / :service <name>" && $0.subtitle == "Services" })
        XCTAssertTrue(items.contains { $0.title == ":ns <namespace>" && $0.subtitle == "Switch namespace" })
        XCTAssertTrue(items.contains { $0.id == "help:logs" && $0.title == ":logs / :log / :l" })
        XCTAssertTrue(items.contains { $0.id == "help:save-view" && $0.title == ":save-view / :sv" })
        XCTAssertTrue(items.contains { $0.id == "help:save-folder" && $0.title == ":save-folder / :sf" })
        XCTAssertTrue(items.contains { $0.id == "help:save-open" && $0.title == ":save-open / :save-and-open / :so" })
        XCTAssertTrue(items.contains { $0.id == "help:save-logs" && $0.title == ":save-logs / :sl" })
        XCTAssertTrue(items.contains { $0.id == "help:export-logs" && $0.title == ":export-logs / :el" })
        XCTAssertTrue(items.contains { $0.id == "help:save-and-open-logs" && $0.title == ":save-and-open-logs / :sol" })
    }

    @MainActor
    func testCommandPaletteServiceAccountCommandNavigatesWithoutStub() {
        let viewModel = RuneAppViewModel(state: RuneAppState())
        let items = viewModel.commandPaletteItems(query: ":sa")

        XCTAssertFalse(items.contains { $0.id.hasPrefix("stub:") })
        XCTAssertFalse(items.contains { $0.subtitle.contains("Not in Rune yet") })
        XCTAssertEqual(items.first?.id, "nav:sa")
        XCTAssertEqual(items.first?.title, "ServiceAccounts")
    }

    @MainActor
    func testCommandPaletteResourceKindShortcutsExecuteExpectedNavigation() throws {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let cases: [(query: String, section: RuneSection, kind: KubeResourceKind)] = [
            (":po", .workloads, .pod),
            ("po", .workloads, .pod),
            (":deploy", .workloads, .deployment),
            (":sts", .workloads, .statefulSet),
            (":ds", .workloads, .daemonSet),
            (":job", .workloads, .job),
            (":cj", .workloads, .cronJob),
            (":rs", .workloads, .replicaSet),
            (":hpa", .workloads, .horizontalPodAutoscaler),
            (":svc", .networking, .service),
            (":ep", .networking, .endpoint),
            ("ep", .networking, .endpoint),
            (":ing", .networking, .ingress),
            (":np", .networking, .networkPolicy),
            (":cm", .config, .configMap),
            ("cm", .config, .configMap),
            (":sec", .config, .secret),
            (":pvc", .storage, .persistentVolumeClaim),
            (":pv", .storage, .persistentVolume),
            (":sc", .storage, .storageClass),
            (":no", .storage, .node),
            (":sa", .rbac, .serviceAccount),
            ("sa", .rbac, .serviceAccount),
            (":role", .rbac, .role),
            (":rb", .rbac, .roleBinding),
            (":cr", .rbac, .clusterRole),
            (":crb", .rbac, .clusterRoleBinding)
        ]

        for entry in cases {
            let item = try XCTUnwrap(viewModel.commandPaletteItems(query: entry.query).first, "Missing item for \(entry.query)")
            viewModel.executeCommandPaletteItem(item)
            XCTAssertEqual(state.selectedSection, entry.section, entry.query)
            XCTAssertEqual(state.selectedWorkloadKind, entry.kind, entry.query)
        }
    }

    @MainActor
    func testCommandPaletteEndpointAndServiceAccountQueriesSelectRowsEndToEnd() {
        let state = RuneAppState()
        state.selectedNamespace = "default"
        let endpoint = ClusterResourceSummary(
            kind: .endpoint,
            name: "needle-endpoints",
            namespace: "default",
            primaryText: "2 addresses",
            secondaryText: "Ready"
        )
        let serviceAccount = ClusterResourceSummary(
            kind: .serviceAccount,
            name: "needle-runner",
            namespace: "default",
            primaryText: "0 secrets",
            secondaryText: "ServiceAccount"
        )
        state.setEndpoints([endpoint])
        state.setRBACData(
            roles: [],
            serviceAccounts: [serviceAccount],
            roleBindings: [],
            clusterRoles: [],
            clusterRoleBindings: []
        )
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.commandPaletteItems(query: ":ep needle").first?.id, "cmd:ep:endpoint|default|needle-endpoints")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":sa runner").first?.id, "cmd:sa:serviceAccount|default|needle-runner")
        XCTAssertEqual(viewModel.commandPaletteItems(query: "ep needle").first?.id, "cmd:ep:endpoint|default|needle-endpoints")
        XCTAssertEqual(viewModel.commandPaletteItems(query: "sa runner").first?.id, "cmd:sa:serviceAccount|default|needle-runner")

        state.isCommandPalettePresented = true
        viewModel.executeCommandPaletteQuery(":ep needle")

        XCTAssertFalse(state.isCommandPalettePresented)
        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .endpoint)
        XCTAssertEqual(state.selectedEndpoint?.name, "needle-endpoints")

        state.isCommandPalettePresented = true
        viewModel.executeCommandPaletteQuery(":sa runner")

        XCTAssertFalse(state.isCommandPalettePresented)
        XCTAssertEqual(state.selectedSection, .rbac)
        XCTAssertEqual(state.selectedWorkloadKind, .serviceAccount)
        XCTAssertEqual(state.selectedRBACResource?.name, "needle-runner")

        state.isCommandPalettePresented = true
        viewModel.executeCommandPaletteQuery("ep needle")

        XCTAssertFalse(state.isCommandPalettePresented)
        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .endpoint)
        XCTAssertEqual(state.selectedEndpoint?.name, "needle-endpoints")

        state.isCommandPalettePresented = true
        viewModel.executeCommandPaletteQuery("sa runner")

        XCTAssertFalse(state.isCommandPalettePresented)
        XCTAssertEqual(state.selectedSection, .rbac)
        XCTAssertEqual(state.selectedWorkloadKind, .serviceAccount)
        XCTAssertEqual(state.selectedRBACResource?.name, "needle-runner")
    }

    @MainActor
    func testCommandPaletteDoesNotExposeClusterWriteOrDestructiveCommands() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette = false
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        }

        let state = RuneAppState()
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.setSelectedPod(PodSummary(name: "api-0", namespace: "default", status: "Running"))
        let viewModel = RuneAppViewModel(state: state)
        let writeQueries = [
            "delete",
            ":delete",
            ":del",
            ":rm",
            ":apply",
            ":scale",
            ":restart",
            ":rollout",
            ":rollback",
            ":exec"
        ]
        let blockedTerms = [
            "delete",
            "remove",
            "apply yaml",
            "scale",
            "restart rollout",
            "rollback",
            "exec"
        ]

        for query in writeQueries {
            let indexedText = viewModel.commandPaletteItems(query: query)
                .map { "\($0.id) \($0.title) \($0.subtitle)".lowercased() }
                .joined(separator: "\n")

            for term in blockedTerms {
                XCTAssertFalse(indexedText.contains(term), "\(query) exposed \(term)")
            }
        }
    }

    @MainActor
    func testCommandPaletteDestructiveDeleteRequiresExplicitOptInAndOnlyOpensConfirmation() throws {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette = true
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-dev")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.setSelectedPod(PodSummary(name: "api-0", namespace: "default", status: "Running"))
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.commandPaletteItems(query: "delete").first?.title, "Delete Pod: api-0")
        let item = try XCTUnwrap(viewModel.commandPaletteItems(query: ":delete").first)
        XCTAssertEqual(item.title, "Delete Pod: api-0")
        XCTAssertTrue(item.subtitle.contains("opens confirmation only"))

        viewModel.executeCommandPaletteItem(item)

        XCTAssertEqual(viewModel.pendingWriteAction, .delete(kind: .pod, name: "api-0"))
        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Delete")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("This cannot be undone."))
        XCTAssertTrue(state.writeAuditLog.isEmpty)
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testCommandPaletteDeleteQueryWorkflowOnlyArmsConfirmationAndDismissesPalette() throws {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette = true
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-dev")
        state.selectedNamespace = "default"
        state.selectedSection = .networking
        state.selectedWorkloadKind = .service
        state.isCommandPalettePresented = true
        state.setSelectedService(ServiceSummary(name: "api", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.10"))
        let viewModel = RuneAppViewModel(state: state)

        viewModel.executeCommandPaletteQuery(":delete")

        XCTAssertFalse(state.isCommandPalettePresented)
        XCTAssertEqual(viewModel.pendingWriteAction, .delete(kind: .service, name: "api"))
        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Delete")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("api"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("This cannot be undone."))
        XCTAssertTrue(state.writeAuditLog.isEmpty)
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testCommandPaletteDestructiveDeleteUsesProductionSecondConfirmation() throws {
        let previousPalette = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        let previousProduction = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette = true
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = true
        defer {
            restoreUserDefaultsValue(previousPalette, forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
            restoreUserDefaultsValue(previousProduction, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "prod")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.setSelectedPod(PodSummary(name: "api-0", namespace: "default", status: "Running"))
        let viewModel = RuneAppViewModel(state: state)

        let item = try XCTUnwrap(viewModel.commandPaletteItems(query: ":rm").first)
        viewModel.executeCommandPaletteItem(item)

        XCTAssertEqual(viewModel.pendingWriteAction, .delete(kind: .pod, name: "api-0"))
        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Review Production Action")
        XCTAssertNil(viewModel.pendingProductionDestructiveConfirmation)
        XCTAssertTrue(state.writeAuditLog.isEmpty)

        viewModel.confirmPendingWriteAction()

        XCTAssertEqual(viewModel.pendingWriteAction, .delete(kind: .pod, name: "api-0"))
        XCTAssertEqual(viewModel.pendingProductionDestructiveConfirmation, .delete(kind: .pod, name: "api-0"))
        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Delete")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Final confirmation required"))
        XCTAssertTrue(state.writeAuditLog.isEmpty)
    }

    @MainActor
    func testCommandPaletteDeleteActionCannotBypassDisabledSettingWhenConstructedDirectly() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette = false
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-dev")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.setSelectedPod(PodSummary(name: "api-0", namespace: "default", status: "Running"))
        let viewModel = RuneAppViewModel(state: state)
        let item = CommandPaletteItem(
            id: "synthetic-delete-bypass",
            title: "Delete Pod: api-0",
            subtitle: "Synthetic direct action",
            symbolName: "trash",
            action: .deleteSelectedResource
        )

        viewModel.executeCommandPaletteItem(item)

        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertNil(viewModel.pendingProductionDestructiveConfirmation)
        XCTAssertTrue(state.writeAuditLog.isEmpty)
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testCommandPaletteDestructiveDeleteRequiresSelectedResourceEvenWhenOptedIn() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette = true
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-dev")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.isCommandPalettePresented = true
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertTrue(viewModel.commandPaletteItems(query: ":delete").isEmpty)
        XCTAssertTrue(viewModel.commandPaletteItems(query: ":rm").isEmpty)

        viewModel.executeCommandPaletteQuery(":delete")

        XCTAssertTrue(state.isCommandPalettePresented)
        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertNil(viewModel.pendingProductionDestructiveConfirmation)
        XCTAssertTrue(state.writeAuditLog.isEmpty)
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testCommandPaletteDestructiveCommandsStayHiddenAcrossSelectedResourceKindsWhenOptedOut() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette = false
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        }

        let cases: [(String, (RuneAppState) -> Void)] = [
            ("pod", { state in
                state.selectedSection = .workloads
                state.selectedWorkloadKind = .pod
                state.setSelectedPod(PodSummary(name: "api-0", namespace: "default", status: "Running"))
            }),
            ("deployment", { state in
                state.selectedSection = .workloads
                state.selectedWorkloadKind = .deployment
                state.setSelectedDeployment(DeploymentSummary(name: "api", namespace: "default", readyReplicas: 1, desiredReplicas: 1))
            }),
            ("service", { state in
                state.selectedSection = .networking
                state.selectedWorkloadKind = .service
                state.setSelectedService(ServiceSummary(name: "api", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.10"))
            }),
            ("configmap", { state in
                state.selectedSection = .config
                state.selectedWorkloadKind = .configMap
                state.setSelectedConfigMap(ClusterResourceSummary(kind: .configMap, name: "settings", namespace: "default", primaryText: "2 keys", secondaryText: "Data"))
            })
        ]

        for (label, configure) in cases {
            let state = RuneAppState()
            state.selectedContext = KubeContext(name: "synthetic-dev")
            state.selectedNamespace = "default"
            configure(state)
            let viewModel = RuneAppViewModel(state: state)

            XCTAssertTrue(viewModel.commandPaletteItems(query: ":delete").isEmpty, label)
            XCTAssertTrue(viewModel.commandPaletteItems(query: ":rm").isEmpty, label)

            viewModel.executeCommandPaletteQuery(":delete")

            XCTAssertNil(viewModel.pendingWriteAction, label)
            XCTAssertNil(viewModel.pendingProductionDestructiveConfirmation, label)
            XCTAssertTrue(state.writeAuditLog.isEmpty, label)
            XCTAssertNil(state.lastError, label)
        }
    }

    @MainActor
    func testCommandPaletteDestructiveDeleteHonorsReadOnlyModeBeforeConfirmation() throws {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette = true
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)
        }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-dev")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.isReadOnlyMode = true
        state.setSelectedPod(PodSummary(name: "api-0", namespace: "default", status: "Running"))
        let viewModel = RuneAppViewModel(state: state)

        let item = try XCTUnwrap(viewModel.commandPaletteItems(query: ":del").first)
        viewModel.executeCommandPaletteItem(item)

        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertNil(viewModel.pendingProductionDestructiveConfirmation)
        XCTAssertEqual(state.lastError, "Read-only mode is on; write actions are blocked.")
        XCTAssertTrue(state.writeAuditLog.isEmpty)
    }

    @MainActor
    func testDemoContextIsVisibleWhenDemoClusterIsEnabled() {
        let previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
        defer {
            if let previousDemoSetting {
                UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
            }
        }

        let state = RuneAppState()
        state.setContexts([
            KubeContext(name: "prod"),
            KubeContext(name: "qa")
        ])
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertTrue(viewModel.visibleContexts.map(\.name).contains("rune-demo"))
        XCTAssertTrue(viewModel.contextMenuOptions.map(\.name).contains("rune-demo"))

        viewModel.setContext(KubeContext(name: "rune-demo"))

        XCTAssertEqual(state.selectedContext?.name, "rune-demo")
        XCTAssertEqual(state.selectedNamespace, "demo")
        XCTAssertTrue(state.contexts.map(\.name).contains("prod"))
        XCTAssertTrue(state.contexts.map(\.name).contains("rune-demo"))
        XCTAssertFalse(state.pods.isEmpty)
    }

    @MainActor
    func testHistoryBackAndForwardWorksAfterFirstTrackedNavigation() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(viewModel.canNavigateBack)

        viewModel.setSection(.workloads)

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertTrue(viewModel.canNavigateBack)

        viewModel.navigateBack()

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(viewModel.canNavigateBack)
        XCTAssertTrue(viewModel.canNavigateForward)

        viewModel.navigateForward()

        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertTrue(viewModel.canNavigateBack)
        XCTAssertFalse(viewModel.canNavigateForward)
    }

    @MainActor
    func testCommandPaletteCompositeNavigationKeepsInitialBackTarget() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let item = CommandPaletteItem(
            id: "kind:service",
            title: "Services",
            subtitle: "Networking",
            symbolName: "network",
            action: .resourceKind(section: .networking, kind: .service)
        )

        XCTAssertEqual(state.selectedSection, .overview)

        viewModel.executeCommandPaletteItem(item)

        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .service)
        XCTAssertTrue(viewModel.canNavigateBack)

        viewModel.navigateBack()

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertFalse(viewModel.canNavigateBack)
        XCTAssertTrue(viewModel.canNavigateForward)
    }

    @MainActor
    func testWorkloadKindSwitchDefersDetailsWhenSnapshotReloadIsNeeded() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod

        viewModel.setWorkloadKind(.deployment)

        XCTAssertEqual(state.selectedWorkloadKind, .deployment)
        XCTAssertFalse(state.isLoadingResourceDetails)
    }

    @MainActor
    func testCompositeSectionAndKindNavigationDefersDetailsUntilRefreshCompletes() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "synthetic")
        state.selectedNamespace = "default"
        state.setServices([
            ServiceSummary(
                name: "sample-service",
                namespace: "default",
                type: "ClusterIP",
                clusterIP: "10.0.0.10"
            )
        ])

        viewModel.setSection(.networking)
        viewModel.setWorkloadKind(.service)

        XCTAssertEqual(state.selectedSection, .networking)
        XCTAssertEqual(state.selectedWorkloadKind, .service)
        XCTAssertFalse(state.isLoadingResourceDetails)
    }

    @MainActor
    func testWorkloadKindNavigationSkipsDetailsForSectionsWithoutInspectors() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedSection = .overview
        state.isLoadingResourceDetails = true

        viewModel.setWorkloadKind(.pod)

        XCTAssertEqual(state.selectedSection, .overview)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)
        XCTAssertFalse(state.isLoadingResourceDetails)
    }

    @MainActor
    func testEmptySelectionDoesNotStartInspectorDetailsLoad() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.isLoadingResourceDetails = true

        viewModel.selectPod(nil)

        XCTAssertNil(state.selectedPod)
        XCTAssertFalse(state.isLoadingResourceDetails)
    }

    @MainActor
    func testNavigationClearsObsoleteInspectorAndLogLoadingState() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.isLoadingResourceDetails = true
        state.isLoadingLogs = true

        viewModel.selectPod(nil)

        XCTAssertNil(state.selectedPod)
        XCTAssertFalse(state.isLoadingResourceDetails)
        XCTAssertFalse(state.isLoadingLogs)
    }

    @MainActor
    func testStoppingPortForwardMarksStartingSessionStoppedImmediately() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let session = PortForwardSession(
            id: "pf-1",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1",
            status: .starting,
            lastMessage: "Starting"
        )
        state.isStartingPortForward = true
        state.upsertPortForwardSession(session)

        viewModel.stopPortForward(session)

        XCTAssertEqual(state.portForwardSessions.first?.status, .stopped)
        XCTAssertEqual(state.portForwardSessions.first?.lastMessage, "Port-forward stopped.")
        XCTAssertFalse(state.isStartingPortForward)
    }

    @MainActor
    func testClearingPortForwardSessionsOnlyRemovesInactiveRows() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let active = PortForwardSession(
            id: "pf-active",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1",
            status: .active
        )
        let stopped = PortForwardSession(
            id: "pf-stopped",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8081,
            remotePort: 80,
            address: "127.0.0.1",
            status: .stopped
        )
        let failed = PortForwardSession(
            id: "pf-failed",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8082,
            remotePort: 80,
            address: "127.0.0.1",
            status: .failed
        )
        let otherNamespaceFailed = PortForwardSession(
            id: "pf-other-namespace",
            contextName: "fake",
            namespace: "other",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8083,
            remotePort: 80,
            address: "127.0.0.1",
            status: .failed
        )
        state.setPortForwardSessions([active, stopped, failed, otherNamespaceFailed])

        viewModel.clearPortForwardSession(active)

        XCTAssertEqual(state.portForwardSessions.map(\.id), ["pf-active", "pf-stopped", "pf-failed", "pf-other-namespace"])

        viewModel.clearPortForwardSession(stopped)

        XCTAssertEqual(state.portForwardSessions.map(\.id), ["pf-active", "pf-failed", "pf-other-namespace"])

        viewModel.clearInactivePortForwardSessions(targetKind: .pod, targetName: "api-0", namespace: "default")

        XCTAssertEqual(state.portForwardSessions.map(\.id), ["pf-active", "pf-other-namespace"])

        viewModel.clearInactivePortForwardSessions()

        XCTAssertEqual(state.portForwardSessions.map(\.id), ["pf-active"])
    }

    @MainActor
    func testOpenPortForwardInBrowserOpensActiveLocalURL() {
        let state = RuneAppState()
        let browserOpener = RecordingPortForwardBrowserOpener()
        let viewModel = RuneAppViewModel(state: state, portForwardBrowserOpener: browserOpener)
        let session = PortForwardSession(
            id: "pf-1",
            contextName: "fake",
            namespace: "default",
            targetKind: .service,
            targetName: "web",
            localPort: 8080,
            remotePort: 80,
            address: "0.0.0.0",
            status: .active,
            lastMessage: "Connected"
        )

        XCTAssertEqual(session.browserURL?.absoluteString, "http://127.0.0.1:8080/")

        viewModel.openPortForwardInBrowser(session)

        XCTAssertEqual(browserOpener.openedURLs.map(\.absoluteString), ["http://127.0.0.1:8080/"])
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testOpenPortForwardInBrowserRejectsDisconnectedSession() {
        let state = RuneAppState()
        let browserOpener = RecordingPortForwardBrowserOpener()
        let viewModel = RuneAppViewModel(state: state, portForwardBrowserOpener: browserOpener)
        let session = PortForwardSession(
            id: "pf-1",
            contextName: "fake",
            namespace: "default",
            targetKind: .pod,
            targetName: "api-0",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1",
            status: .starting,
            lastMessage: "Starting"
        )

        viewModel.openPortForwardInBrowser(session)

        XCTAssertTrue(browserOpener.openedURLs.isEmpty)
        XCTAssertEqual(state.lastError, "Invalid input: Port-forward is not connected yet.")
    }

    @MainActor
    func testCancelledLogExportDoesNotShowGlobalError() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: CancelledFileExporter())
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setPodLogs("line\n")

        viewModel.saveCurrentLogs()

        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testCurrentLogsConfiguredExportUsesExportFolderWorkflow() {
        let state = RuneAppState()
        let configuredExporter = RecordingConfiguredExporter()
        let viewModel = RuneAppViewModel(state: state, configuredExporter: configuredExporter)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setPodLogs("line\n")

        viewModel.saveCurrentLogsToExportFolder(openAfterSave: true)

        XCTAssertEqual(configuredExporter.saves.count, 1)
        XCTAssertEqual(String(data: configuredExporter.saves[0].data, encoding: .utf8), "line\n")
        XCTAssertTrue(configuredExporter.saves[0].suggestedName.hasPrefix("pod-api-0-logs-"))
        XCTAssertEqual(configuredExporter.saves[0].allowedFileTypes, ["log", "txt"])
        XCTAssertEqual(configuredExporter.saves[0].kind, .plainText)
        XCTAssertTrue(configuredExporter.saves[0].openAfterSave)
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testHelmAndRolloutConfiguredExportsUseTextWorkflow() {
        let state = RuneAppState()
        let configuredExporter = RecordingConfiguredExporter()
        let viewModel = RuneAppViewModel(state: state, configuredExporter: configuredExporter)
        let release = HelmReleaseSummary(
            name: "synthetic-release",
            namespace: "synthetic",
            revision: 2,
            updated: "now",
            status: "deployed",
            chart: "synthetic-1.0.0",
            appVersion: "1.0.0"
        )
        state.setSelectedHelmRelease(release)
        state.setHelmValues("replicas: 2\n")
        state.setHelmManifest("kind: Deployment\n")
        state.setHelmHistory([
            HelmReleaseRevision(
                revision: 2,
                updated: "now",
                status: "deployed",
                chart: release.chart,
                appVersion: release.appVersion,
                description: "Synthetic upgrade"
            )
        ])
        state.setSelectedDeployment(
            DeploymentSummary(
                name: "synthetic-api",
                namespace: "synthetic",
                readyReplicas: 2,
                desiredReplicas: 2
            )
        )
        state.setDeploymentRolloutHistory("REVISION\tCHANGE-CAUSE\n2\tSynthetic upgrade\n")

        viewModel.saveCurrentHelmValuesToExportFolder(openAfterSave: false)
        viewModel.saveCurrentHelmManifestToExportFolder(openAfterSave: true)
        viewModel.saveCurrentHelmHistoryToExportFolder(openAfterSave: false)
        viewModel.saveCurrentRolloutHistoryToExportFolder(openAfterSave: true)

        XCTAssertEqual(configuredExporter.saves.count, 4)
        XCTAssertTrue(configuredExporter.saves.allSatisfy { $0.kind == .plainText })
        XCTAssertEqual(configuredExporter.saves.map { $0.openAfterSave }, [false, true, false, true])
        XCTAssertTrue(configuredExporter.saves[0].suggestedName.contains("-values-"))
        XCTAssertTrue(configuredExporter.saves[1].suggestedName.contains("-manifest-"))
        XCTAssertTrue(configuredExporter.saves[2].suggestedName.contains("-history-"))
        XCTAssertTrue(configuredExporter.saves[3].suggestedName.contains("-rollout-history-"))
        XCTAssertEqual(String(decoding: configuredExporter.saves[0].data, as: UTF8.self), "replicas: 2\n")
        XCTAssertEqual(String(decoding: configuredExporter.saves[1].data, as: UTF8.self), "kind: Deployment\n")
        XCTAssertTrue(String(decoding: configuredExporter.saves[2].data, as: UTF8.self).contains("Synthetic upgrade"))
        XCTAssertTrue(String(decoding: configuredExporter.saves[3].data, as: UTF8.self).contains("Synthetic upgrade"))
    }

    @MainActor
    func testVisibleLogsZipConfiguredExportUsesArchiveWorkflow() {
        let state = RuneAppState()
        let configuredExporter = RecordingConfiguredExporter()
        let viewModel = RuneAppViewModel(state: state, configuredExporter: configuredExporter)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")

        viewModel.saveVisibleLogsZipToExportFolder(visibleText: "matched line\n", openAfterSave: true)

        XCTAssertEqual(configuredExporter.saves.count, 1)
        XCTAssertTrue(configuredExporter.saves[0].suggestedName.hasPrefix("pod-api-0-visible-logs-"))
        XCTAssertEqual(configuredExporter.saves[0].allowedFileTypes, ["zip"])
        XCTAssertEqual(configuredExporter.saves[0].kind, .archive)
        XCTAssertTrue(configuredExporter.saves[0].openAfterSave)
        XCTAssertTrue(configuredExporter.saves[0].data.count > 0)
        XCTAssertNil(state.lastError)
    }

    @MainActor
    func testUserCancelledErrorsDoNotCreateGlobalNotice() {
        let state = RuneAppState()

        state.setError(RuneError.userCancelled)

        XCTAssertNil(state.lastError)
        XCTAssertNil(state.activeNotice)
    }

    @MainActor
    func testGlobalErrorsBecomeStructuredNotices() {
        let state = RuneAppState()

        state.setError(RuneError.invalidInput(message: "Choose a namespace."))

        XCTAssertEqual(state.lastError, "Invalid input: Choose a namespace.")
        XCTAssertEqual(state.activeNotice?.severity, .warning)
        XCTAssertEqual(state.activeNotice?.title, "Check the action")

        state.setError(RuneError.commandFailed(command: "kubectl get pods", message: "forbidden"))

        XCTAssertEqual(state.activeNotice?.severity, .error)
        XCTAssertEqual(state.activeNotice?.title, "Kubernetes command failed")

        state.clearError()

        XCTAssertNil(state.lastError)
        XCTAssertNil(state.activeNotice)
    }

    @MainActor
    func testApplyYAMLRequiresUnsavedEdits() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
            """
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertNil(viewModel.pendingWriteAction)

        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
              labels:
                app: api
            """
        )
        viewModel.requestApplySelectedResourceYAML()

        XCTAssertNotNil(viewModel.pendingWriteAction)
    }

    @MainActor
    func testApplyYAMLRejectsChangesThatTargetAnotherResource() {
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = false
        defer {
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "synthetic")
        state.selectedNamespace = "test-space"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setSelectedConfigMap(ClusterResourceSummary(
            kind: .configMap,
            name: "settings",
            namespace: "test-space",
            primaryText: "1 key",
            secondaryText: "ConfigMap"
        ))

        let baseline = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: settings
          namespace: test-space
        data:
          mode: baseline
        """
        let changedTargets = [
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: other-settings
              namespace: test-space
            data:
              mode: edited
            """,
            """
            apiVersion: v1
            kind: Secret
            metadata:
              name: settings
              namespace: test-space
            stringData:
              mode: edited
            """,
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
              namespace: other-space
            data:
              mode: edited
            """
        ]

        for changedTarget in changedTargets {
            state.clearError()
            state.setResourceYAML(baseline)
            state.updateResourceYAMLDraft(changedTarget)

            viewModel.requestApplySelectedResourceYAML()

            XCTAssertNil(viewModel.pendingWriteAction)
            XCTAssertTrue(
                state.lastError?.contains(
                    "Keep kind, metadata.name, and metadata.namespace unchanged."
                ) == true
            )
        }
    }

    @MainActor
    func testApplyYAMLAcceptsMatchingExplicitTargetIdentity() {
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = false
        defer {
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "synthetic")
        state.selectedNamespace = "test-space"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setSelectedConfigMap(ClusterResourceSummary(
            kind: .configMap,
            name: "settings",
            namespace: "test-space",
            primaryText: "1 key",
            secondaryText: "ConfigMap"
        ))
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
              namespace: test-space
            data:
              mode: baseline
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
              namespace: test-space
            data:
              mode: edited
            """
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertEqual(
            viewModel.pendingWriteAction,
            .apply(
                kind: .configMap,
                name: "settings",
                yaml: state.resourceYAML,
                baseline: state.resourceYAMLBaseline
            )
        )
    }

    @MainActor
    func testCurrentResourceYAMLExportRequiresMatchingDetailScope() {
        let exporter = RecordingFileExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.beginResourceDetailLoad(scope: ResourceDetailScope(
            contextName: "fake",
            namespace: "default",
            kind: .pod,
            name: "api-0"
        ))
        state.setResourceYAML("kind: Pod\nmetadata:\n  name: api-0\n")
        state.finishResourceDetailLoad()

        viewModel.saveCurrentResourceYAML()

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertTrue(exporter.saves[0].suggestedName.hasPrefix("pod-api-0-"))
    }

    @MainActor
    func testCurrentResourceYAMLConfiguredExportUsesTextWorkflow() {
        let configuredExporter = RecordingConfiguredExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, configuredExporter: configuredExporter)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.beginResourceDetailLoad(scope: ResourceDetailScope(
            contextName: "fake",
            namespace: "default",
            kind: .pod,
            name: "api-0"
        ))
        state.setResourceYAML("kind: Pod\nmetadata:\n  name: api-0\n")
        state.finishResourceDetailLoad()

        viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: true)

        XCTAssertEqual(configuredExporter.saves.count, 1)
        XCTAssertTrue(configuredExporter.saves[0].suggestedName.hasPrefix("pod-api-0-"))
        XCTAssertTrue(configuredExporter.saves[0].suggestedName.hasSuffix(".yaml"))
        XCTAssertEqual(configuredExporter.saves[0].allowedFileTypes, ["yaml", "yml"])
        XCTAssertEqual(configuredExporter.saves[0].kind, .plainText)
        XCTAssertTrue(configuredExporter.saves[0].openAfterSave)
        XCTAssertEqual(String(decoding: configuredExporter.saves[0].data, as: UTF8.self), state.resourceYAML)
    }

    @MainActor
    func testStaleResourceYAMLExportIsBlockedWhenDetailScopeDoesNotMatchSelection() {
        let exporter = RecordingFileExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.beginResourceDetailLoad(scope: ResourceDetailScope(
            contextName: "fake",
            namespace: "other",
            kind: .pod,
            name: "api-0"
        ))
        state.setResourceYAML("kind: Pod\nmetadata:\n  name: api-0\n")
        state.finishResourceDetailLoad()

        viewModel.saveCurrentResourceYAML()

        XCTAssertTrue(exporter.saves.isEmpty)
    }

    @MainActor
    func testStaleResourceYAMLApplyIsBlockedWhenDetailScopeDoesNotMatchSelection() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.beginResourceDetailLoad(scope: ResourceDetailScope(
            contextName: "fake",
            namespace: "other",
            kind: .pod,
            name: "api-0"
        ))
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
            """
        )
        state.finishResourceDetailLoad()
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
              labels:
                app: api
            """
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertNil(viewModel.pendingWriteAction)
    }

    @MainActor
    func testStaleOperatorDetailScopeDoesNotBypassCurrentResourceYAMLExportGuard() {
        let exporter = RecordingFileExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setSelectedOperatorResource(OperatorResourceSummary(
            family: "operators",
            kind: "Subscription",
            apiPath: "/apis/operators.coreos.com/v1alpha1/namespaces/default/subscriptions",
            name: "demo-operator",
            namespace: "default",
            status: "Ready",
            message: "Ready"
        ))
        state.beginResourceDetailLoad(scope: ResourceDetailScope(
            contextName: "fake",
            namespace: "default",
            kind: "Subscription",
            name: "demo-operator"
        ))
        state.setResourceYAML("kind: Subscription\nmetadata:\n  name: demo-operator\n")
        state.finishResourceDetailLoad()

        viewModel.saveCurrentResourceYAML()

        XCTAssertTrue(exporter.saves.isEmpty)
    }

    @MainActor
    func testApplyYAMLRejectsValidationErrorsBeforeConfirm() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: Pod
            metadata:
              name: api-0
              labels:
                app: api
            """
        )
        state.setResourceYAMLValidationIssues([
            YAMLValidationIssue(source: .syntax, severity: .error, message: "bad yaml")
        ])

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertEqual(state.lastError, "Invalid input: Fix YAML errors before applying.")
    }

    @MainActor
    func testApplyYAMLImmediatelyRechecksTheCurrentDraftForLocalErrors() {
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = false
        defer {
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "synthetic")
        state.selectedNamespace = "test-space"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setSelectedConfigMap(ClusterResourceSummary(
            kind: .configMap,
            name: "settings",
            namespace: "test-space",
            primaryText: "1 key",
            secondaryText: "ConfigMap"
        ))
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
              namespace: test-space
            data:
              mode: baseline
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
              namespace: test-space
            data:
            \tBROKEN: value
            """
        )

        XCTAssertTrue(
            state.resourceYAMLValidationIssues.isEmpty,
            "This exercises Apply before the deferred validation publisher has run."
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertTrue(state.resourceYAMLValidationIssues.contains {
            $0.severity == .error
                && $0.message == "Tabs are not allowed in YAML indentation."
        })
        XCTAssertEqual(state.lastError, "Invalid input: Fix YAML errors before applying.")
    }

    @MainActor
    func testReapplyResourceYAMLBaselineUsesInSessionSnapshotForNonSecrets() {
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = false
        defer {
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setSelectedConfigMap(
            ClusterResourceSummary(
                kind: .configMap,
                name: "settings",
                namespace: "default",
                primaryText: "2 keys",
                secondaryText: "ConfigMap"
            )
        )
        let baseline = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: settings
        data:
          mode: old
        """
        let edited = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: settings
        data:
          mode: edited
        """
        state.setResourceYAML(baseline)
        state.updateResourceYAMLDraft(edited)

        XCTAssertTrue(viewModel.canReapplyResourceYAMLBaseline)

        viewModel.requestReapplyResourceYAMLBaseline()

        guard case let .apply(kind, name, yaml, actionBaseline)? = viewModel.pendingWriteAction else {
            return XCTFail("Expected pending apply action")
        }
        XCTAssertEqual(kind, .configMap)
        XCTAssertEqual(name, "settings")
        XCTAssertEqual(yaml, baseline)
        XCTAssertEqual(actionBaseline, edited)
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("YAML diff preview"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("-   mode: edited"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("+   mode: old"))
    }

    @MainActor
    func testReapplyResourceYAMLBaselineIsDisabledForSecrets() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedSection = .config
        state.selectedWorkloadKind = .secret
        state.setSelectedSecret(
            ClusterResourceSummary(
                kind: .secret,
                name: "api-token",
                namespace: "default",
                primaryText: "2 keys",
                secondaryText: "Opaque"
            )
        )
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: Secret
            metadata:
              name: api-token
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: Secret
            metadata:
              name: api-token
              labels:
                app: api
            """
        )

        XCTAssertFalse(viewModel.canReapplyResourceYAMLBaseline)

        viewModel.requestReapplyResourceYAMLBaseline()

        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertEqual(
            state.lastError,
            "Invalid input: Apply last fetched YAML is disabled for Secrets. Review the diff and apply YAML explicitly."
        )
    }

    @MainActor
    func testReplacePodLogReadOverwritesCachedSnapshotWithoutMergingPriorFetch() {
        let state = RuneAppState()
        let firstDate = Date(timeIntervalSince1970: 1_776_000_000)
        let secondDate = Date(timeIntervalSince1970: 1_776_000_060)

        state.replacePodLogRead(
            "narrow-window-line\n",
            contextName: "ctx-a",
            namespace: "ns-a",
            podName: "pod-a",
            loadedAt: firstDate
        )
        XCTAssertTrue(state.podLogs.contains("narrow-window-line"))
        XCTAssertFalse(state.podLogs.contains("wide-window-line"))

        state.replacePodLogRead(
            "wide-window-line\n",
            contextName: "ctx-a",
            namespace: "ns-a",
            podName: "pod-a",
            loadedAt: secondDate
        )
        XCTAssertTrue(state.podLogs.contains("wide-window-line"))
        XCTAssertFalse(state.podLogs.contains("narrow-window-line"))
    }

    @MainActor
    func testSessionLogCacheKeepsReadSegmentsWithResourceBreaks() {
        let state = RuneAppState()
        let firstDate = Date(timeIntervalSince1970: 1_776_000_000)
        let secondDate = Date(timeIntervalSince1970: 1_776_000_030)

        state.appendPodLogRead(
            "first line\n",
            contextName: "demo-cluster",
            namespace: "demo-namespace",
            podName: "api-0",
            loadedAt: firstDate
        )
        state.appendPodLogRead(
            "second line\n",
            contextName: "demo-cluster",
            namespace: "demo-namespace",
            podName: "api-0",
            loadedAt: secondDate
        )

        XCTAssertTrue(state.podLogs.contains("Pod  demo-namespace/api-0"))
        XCTAssertTrue(state.podLogs.contains("Context: demo-cluster"))
        XCTAssertTrue(state.podLogs.contains("first line"))
        XCTAssertTrue(state.podLogs.contains("second line"))
        XCTAssertGreaterThanOrEqual(state.podLogs.components(separatedBy: "────────────────").count, 5)

        state.setPodLogs("")
        state.showCachedPodLogs(contextName: "demo-cluster", namespace: "demo-namespace", podName: "api-0")
        XCTAssertTrue(state.podLogs.contains("first line"))
        XCTAssertTrue(state.podLogs.contains("second line"))
    }

    @MainActor
    func testLogFetchErrorPreservesCachedLogsUntilSuccessfulRead() {
        let state = RuneAppState()

        state.appendPodLogRead(
            "ready\n",
            contextName: "demo",
            namespace: "default",
            podName: "api-0",
            loadedAt: Date(timeIntervalSince1970: 1_776_000_000)
        )

        state.setLastLogFetchError("stream interrupted")

        XCTAssertEqual(state.lastLogFetchError, "stream interrupted")
        XCTAssertTrue(state.podLogs.contains("ready"))

        state.appendPodLogRead(
            "recovered\n",
            contextName: "demo",
            namespace: "default",
            podName: "api-0",
            loadedAt: Date(timeIntervalSince1970: 1_776_000_030)
        )

        XCTAssertNil(state.lastLogFetchError)
        XCTAssertTrue(state.podLogs.contains("recovered"))
    }

    @MainActor
    func testUnifiedLogsCanBeScopedToSelectedPods() {
        let result = RuneAppViewModel.scopedUnifiedLogResult(
            mergedText: """
            [api-0] first
            [api-1] second
            [api-0] third
            """,
            podNames: ["api-0", "api-1"],
            selectedPodNames: ["api-0"]
        )

        XCTAssertEqual(result.podNames, ["api-0"])
        XCTAssertTrue(result.mergedText.contains("[api-0] first"))
        XCTAssertTrue(result.mergedText.contains("[api-0] third"))
        XCTAssertFalse(result.mergedText.contains("[api-1] second"))
    }

    @MainActor
    func testResourceYAMLUndoWalksDraftHistoryOneStepAtATime() {
        let state = RuneAppState()
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            """
        )

        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
              labels:
                app: demo
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
              labels:
                app: demo
            data:
              enabled: "true"
            """
        )

        XCTAssertTrue(state.canUndoResourceYAMLEdit)

        state.undoResourceYAMLEdit()

        XCTAssertTrue(state.resourceYAML.contains("labels:"))
        XCTAssertFalse(state.resourceYAML.contains("enabled:"))
        XCTAssertTrue(state.canUndoResourceYAMLEdit)

        state.undoResourceYAMLEdit()

        XCTAssertFalse(state.resourceYAML.contains("labels:"))
        XCTAssertFalse(state.canUndoResourceYAMLEdit)

        state.undoResourceYAMLEdit()

        XCTAssertFalse(state.resourceYAML.contains("labels:"))
    }

    @MainActor
    func testResourceYAMLUndoRestoresPresentationWithMatchingTextEntry() {
        let state = RuneAppState()
        let firstPresentation = ResourceYAMLEditorPresentation(
            selections: [ResourceYAMLEditorSelection(location: 2, length: 0)],
            viewportX: 120,
            viewportY: 40,
            hadKeyboardFocus: true
        )
        let secondPresentation = ResourceYAMLEditorPresentation(
            selections: [ResourceYAMLEditorSelection(location: 5, length: 3)],
            viewportX: 260,
            viewportY: 90,
            hadKeyboardFocus: false
        )

        state.setResourceYAML("first")
        state.updateResourceYAMLDraft(
            "second",
            undoPresentation: firstPresentation
        )
        state.updateResourceYAMLDraft(
            "third",
            undoPresentation: secondPresentation
        )

        state.undoResourceYAMLEdit()

        XCTAssertEqual(state.resourceYAML, "second")
        XCTAssertEqual(state.resourceYAMLUndoSnapshot, "first")
        XCTAssertEqual(
            state.resourceYAMLEditorRestorationRequest?.presentation,
            secondPresentation
        )
        let firstSequence = state.resourceYAMLEditorRestorationRequest?.sequence

        state.undoResourceYAMLEdit()

        XCTAssertEqual(state.resourceYAML, "first")
        XCTAssertNil(state.resourceYAMLUndoSnapshot)
        XCTAssertEqual(
            state.resourceYAMLEditorRestorationRequest?.presentation,
            firstPresentation
        )
        XCTAssertNotEqual(
            state.resourceYAMLEditorRestorationRequest?.sequence,
            firstSequence
        )

        state.updateResourceYAMLDraft("new edit")
        XCTAssertNil(state.resourceYAMLEditorRestorationRequest)
    }

    @MainActor
    func testPendingApplyMessageIncludesYAMLDiffPreview() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setSelectedConfigMap(
            ClusterResourceSummary(
                kind: .configMap,
                name: "settings",
                namespace: "default",
                primaryText: "1 data key",
                secondaryText: "2m"
            )
        )
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: old
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: new
            """
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("YAML diff preview"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("-   mode: old"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("+   mode: new"))
    }

    @MainActor
    func testPendingApplyMessageIncludesServerDryRunStatus() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = true
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setSelectedConfigMap(
            ClusterResourceSummary(
                kind: .configMap,
                name: "settings",
                namespace: "default",
                primaryText: "1 data key",
                secondaryText: "2m"
            )
        )
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: old
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: new
            """
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertEqual(viewModel.pendingWriteDryRunStatus, "Checking with Kubernetes API...")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Server dry-run:"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Checking with Kubernetes API..."))
    }

    @MainActor
    func testDisabledApplyDryRunSettingSkipsApplyDryRunPreview() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        UserDefaults.standard.runeWriteSafetyRequireApplyDryRun = false
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setSelectedConfigMap(
            ClusterResourceSummary(
                kind: .configMap,
                name: "settings",
                namespace: "default",
                primaryText: "1 data key",
                secondaryText: "2m"
            )
        )
        state.setResourceYAML(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: old
            """
        )
        state.updateResourceYAMLDraft(
            """
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: settings
            data:
              mode: new
            """
        )

        viewModel.requestApplySelectedResourceYAML()

        XCTAssertNotNil(viewModel.pendingWriteAction)
        XCTAssertNil(viewModel.pendingWriteDryRunStatus)
        XCTAssertFalse(viewModel.pendingWriteActionMessage.contains("Server dry-run:"))
    }

    @MainActor
    func testMultipleTerminalSessionsSwitchAndCloseIndependently() {
        let state = RuneAppState()
        let first = PodTerminalSession(
            id: "shell-a",
            contextName: "demo",
            namespace: "default",
            podName: "api-0",
            shell: "sh",
            transcript: "first",
            status: .connected
        )
        let second = PodTerminalSession(
            id: "shell-b",
            contextName: "demo",
            namespace: "default",
            podName: "worker-0",
            shell: "sh",
            transcript: "second",
            status: .connected
        )

        state.setTerminalSession(first)
        state.setTerminalSession(second)

        XCTAssertEqual(state.terminalSessions.map(\.id), ["shell-a", "shell-b"])
        XCTAssertEqual(state.terminalSession?.id, "shell-b")

        state.selectTerminalSession(id: "shell-a")
        state.appendTerminalSessionOutput(id: "shell-b", text: "\nbackground")

        XCTAssertEqual(state.terminalSession?.id, "shell-a")
        XCTAssertEqual(state.terminalSession?.transcript, "first")
        XCTAssertTrue(state.terminalSessions.first(where: { $0.id == "shell-b" })?.transcript.contains("background") == true)

        state.setTerminalSession(nil)

        XCTAssertEqual(state.terminalSessions.map(\.id), ["shell-b"])
        XCTAssertEqual(state.terminalSession?.id, "shell-b")
    }

    @MainActor
    func testClosingBackgroundTerminalTabNeverActivatesItOrClearsActiveDraft() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let first = PodTerminalSession(
            id: "shell-a",
            contextName: "synthetic",
            namespace: "default",
            podName: "api-0",
            shell: "sh",
            status: .connected
        )
        let second = PodTerminalSession(
            id: "shell-b",
            contextName: "synthetic",
            namespace: "default",
            podName: "worker-0",
            shell: "sh",
            status: .connected
        )
        state.setTerminalSession(first)
        state.setTerminalSession(second)
        state.selectTerminalSession(id: first.id)
        viewModel.terminalSessionInput = "unfinished command"

        viewModel.closeTerminalSession(id: second.id)

        XCTAssertEqual(state.terminalSessions.map(\.id), [first.id])
        XCTAssertEqual(state.activeTerminalSessionID, first.id)
        XCTAssertEqual(state.terminalSession?.id, first.id)
        XCTAssertEqual(viewModel.terminalSessionInput, "unfinished command")
    }

    @MainActor
    func testTerminalSessionMutationsStayScopedToActiveTab() {
        let state = RuneAppState()
        state.setTerminalSession(PodTerminalSession(
            id: "shell-a",
            contextName: "demo",
            namespace: "default",
            podName: "api-0",
            shell: "sh",
            transcript: "alpha",
            status: .connected
        ))
        state.setTerminalSession(PodTerminalSession(
            id: "shell-b",
            contextName: "demo",
            namespace: "default",
            podName: "worker-0",
            shell: "sh",
            transcript: "beta",
            status: .connected
        ))

        state.selectTerminalSession(id: "shell-a")
        state.clearTerminalSessionTranscript()
        state.updateTerminalSessionStatus(id: "shell-b", status: .failed, exitCode: 137)
        state.appendTerminalSessionCommandEcho(id: "shell-b", command: "date")

        XCTAssertEqual(state.terminalSession?.id, "shell-a")
        XCTAssertEqual(state.terminalSession?.transcript, "")
        let background = state.terminalSessions.first { $0.id == "shell-b" }
        XCTAssertEqual(background?.status, .failed)
        XCTAssertEqual(background?.lastExitCode, 137)
        XCTAssertTrue(background?.transcript.contains("$ date") == true)
    }

    @MainActor
    func testActiveTerminalTranscriptExportSavesCurrentShellOnly() {
        let exporter = RecordingFileExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.setTerminalSession(PodTerminalSession(
            id: "shell-a",
            contextName: "demo-context",
            namespace: "demo",
            podName: "api-0",
            shell: "sh",
            transcript: "active line\n",
            status: .connected
        ))
        state.setTerminalSession(PodTerminalSession(
            id: "shell-b",
            contextName: "demo-context",
            namespace: "demo",
            podName: "worker-0",
            shell: "sh",
            transcript: "background line\n",
            status: .connected
        ))
        state.selectTerminalSession(id: "shell-a")

        viewModel.saveActiveTerminalTranscript()

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertTrue(exporter.saves[0].suggestedName.hasPrefix("terminal-demo-api-0-transcript-"))
        XCTAssertEqual(exporter.saves[0].allowedFileTypes, ["log", "txt"])
        let payload = String(decoding: exporter.saves[0].data, as: UTF8.self)
        XCTAssertTrue(payload.contains("Context: demo-context"))
        XCTAssertTrue(payload.contains("Namespace: demo"))
        XCTAssertTrue(payload.contains("Pod: api-0"))
        XCTAssertTrue(payload.contains("Status: connected"))
        XCTAssertTrue(payload.contains("active line"))
        XCTAssertFalse(payload.contains("background line"))
    }

    @MainActor
    func testActiveTerminalTranscriptConfiguredExportUsesTextWorkflow() {
        let configuredExporter = RecordingConfiguredExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, configuredExporter: configuredExporter)
        state.setTerminalSession(PodTerminalSession(
            id: "shell-a",
            contextName: "demo-context",
            namespace: "demo",
            podName: "api-0",
            shell: "sh",
            transcript: "active line\n",
            status: .connected
        ))

        viewModel.saveActiveTerminalTranscriptToExportFolder(openAfterSave: true)

        XCTAssertEqual(configuredExporter.saves.count, 1)
        XCTAssertTrue(configuredExporter.saves[0].suggestedName.hasPrefix("terminal-demo-api-0-transcript-"))
        XCTAssertEqual(configuredExporter.saves[0].allowedFileTypes, ["log", "txt"])
        XCTAssertEqual(configuredExporter.saves[0].kind, .plainText)
        XCTAssertTrue(configuredExporter.saves[0].openAfterSave)
        let payload = String(decoding: configuredExporter.saves[0].data, as: UTF8.self)
        XCTAssertTrue(payload.contains("Context: demo-context"))
        XCTAssertTrue(payload.contains("active line"))
    }

    @MainActor
    func testAllTerminalTranscriptExportBuildsOneArchiveForEveryShellTab() throws {
        let exporter = RecordingFileExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.setTerminalSession(PodTerminalSession(
            id: "shell-a",
            contextName: "demo-context",
            namespace: "demo",
            podName: "api-0",
            shell: "sh",
            transcript: "alpha\n",
            status: .disconnected,
            lastExitCode: 0
        ))
        state.setTerminalSession(PodTerminalSession(
            id: "shell-b",
            contextName: "demo-context",
            namespace: "demo",
            podName: "worker-0",
            shell: "sh",
            transcript: "beta\n",
            status: .failed,
            lastExitCode: 137
        ))

        viewModel.saveAllTerminalTranscriptsZip()

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertTrue(exporter.saves[0].suggestedName.hasPrefix("terminal-transcripts-"))
        XCTAssertEqual(exporter.saves[0].allowedFileTypes, ["zip"])
        let entries = try ZipArchiveTestSupport.entries(from: exporter.saves[0].data)
        let readme = String(decoding: try XCTUnwrap(entries["terminal-transcripts/README.txt"]), as: UTF8.self)
        let apiEntry = try XCTUnwrap(entries.first { key, _ in
            key.hasPrefix("terminal-transcripts/session-1-demo-api-0-") && key.hasSuffix(".log")
        })
        let workerEntry = try XCTUnwrap(entries.first { key, _ in
            key.hasPrefix("terminal-transcripts/session-2-demo-worker-0-") && key.hasSuffix(".log")
        })
        XCTAssertTrue(readme.contains("Sessions: 2"))
        XCTAssertTrue(String(decoding: apiEntry.value, as: UTF8.self).contains("alpha"))
        XCTAssertTrue(String(decoding: workerEntry.value, as: UTF8.self).contains("beta"))
        XCTAssertTrue(String(decoding: workerEntry.value, as: UTF8.self).contains("Exit Code: 137"))
    }

    @MainActor
    func testAllTerminalTranscriptConfiguredExportUsesArchiveWorkflow() throws {
        let configuredExporter = RecordingConfiguredExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, configuredExporter: configuredExporter)
        state.setTerminalSession(PodTerminalSession(
            id: "shell-a",
            contextName: "demo-context",
            namespace: "demo",
            podName: "api-0",
            shell: "sh",
            transcript: "alpha\n",
            status: .disconnected,
            lastExitCode: 0
        ))
        state.setTerminalSession(PodTerminalSession(
            id: "shell-b",
            contextName: "demo-context",
            namespace: "demo",
            podName: "worker-0",
            shell: "sh",
            transcript: "beta\n",
            status: .failed,
            lastExitCode: 137
        ))

        viewModel.saveAllTerminalTranscriptsZipToExportFolder(openAfterSave: true)

        XCTAssertEqual(configuredExporter.saves.count, 1)
        XCTAssertTrue(configuredExporter.saves[0].suggestedName.hasPrefix("terminal-transcripts-"))
        XCTAssertEqual(configuredExporter.saves[0].allowedFileTypes, ["zip"])
        XCTAssertEqual(configuredExporter.saves[0].kind, .archive)
        XCTAssertTrue(configuredExporter.saves[0].openAfterSave)
        let entries = try ZipArchiveTestSupport.entries(from: configuredExporter.saves[0].data)
        XCTAssertNotNil(entries["terminal-transcripts/README.txt"])
        XCTAssertTrue(entries.values.contains { String(decoding: $0, as: UTF8.self).contains("alpha") })
        XCTAssertTrue(entries.values.contains { String(decoding: $0, as: UTF8.self).contains("beta") })
    }

    @MainActor
    func testTerminalTranscriptExportSkipsWhenNoTranscriptExists() {
        let exporter = RecordingFileExporter()
        let configuredExporter = RecordingConfiguredExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter, configuredExporter: configuredExporter)
        state.setTerminalSession(PodTerminalSession(
            id: "shell-empty",
            contextName: "demo-context",
            namespace: "demo",
            podName: "api-0",
            shell: "sh",
            transcript: "",
            status: .connected
        ))

        viewModel.saveActiveTerminalTranscript()
        viewModel.saveAllTerminalTranscriptsZip()
        viewModel.saveActiveTerminalTranscriptToExportFolder(openAfterSave: true)
        viewModel.saveAllTerminalTranscriptsZipToExportFolder(openAfterSave: true)

        XCTAssertTrue(exporter.saves.isEmpty)
        XCTAssertTrue(configuredExporter.saves.isEmpty)
    }

    @MainActor
    func testScopeHardeningTerminalTargetIdentityIncludesContextNamespacePodAndContainer() {
        let session = PodTerminalSession(
            id: "terminal-scope",
            contextName: "context-alpha",
            namespace: "namespace-alpha",
            podName: "pod-alpha",
            containerName: "container-alpha",
            shell: "sh",
            status: .disconnected
        )

        XCTAssertTrue(session.targets(
            contextName: "context-alpha",
            namespace: "namespace-alpha",
            podName: "pod-alpha",
            containerName: "container-alpha"
        ))
        XCTAssertFalse(session.targets(
            contextName: "context-beta",
            namespace: "namespace-alpha",
            podName: "pod-alpha",
            containerName: "container-alpha"
        ))
        XCTAssertFalse(session.targets(
            contextName: "context-alpha",
            namespace: "namespace-beta",
            podName: "pod-alpha",
            containerName: "container-alpha"
        ))
        XCTAssertFalse(session.targets(
            contextName: "context-alpha",
            namespace: "namespace-alpha",
            podName: "pod-beta",
            containerName: "container-alpha"
        ))
        XCTAssertFalse(session.targets(
            contextName: "context-alpha",
            namespace: "namespace-alpha",
            podName: "pod-alpha",
            containerName: "container-beta"
        ))
    }

    @MainActor
    func testScopeHardeningTerminalReconnectRejectsEveryCrossScopeDimension() {
        let cases: [(String, String, String, String?, String, String, String, String?)] = [
            ("context-alpha", "namespace-alpha", "pod-alpha", "container-alpha", "context-beta", "namespace-alpha", "pod-alpha", "container-alpha"),
            ("context-alpha", "namespace-alpha", "pod-alpha", "container-alpha", "context-alpha", "namespace-beta", "pod-alpha", "container-alpha"),
            ("context-alpha", "namespace-alpha", "pod-alpha", "container-alpha", "context-alpha", "namespace-alpha", "pod-beta", "container-alpha"),
            ("context-alpha", "namespace-alpha", "pod-alpha", "container-alpha", "context-alpha", "namespace-alpha", "pod-alpha", "container-beta")
        ]

        for (index, item) in cases.enumerated() {
            let state = RuneAppState()
            state.selectedContext = KubeContext(name: item.4)
            state.selectedNamespace = item.5
            let viewModel = RuneAppViewModel(state: state)
            state.setTerminalSession(PodTerminalSession(
                id: "terminal-\(index)",
                contextName: item.0,
                namespace: item.1,
                podName: item.2,
                containerName: item.3,
                shell: "sh",
                transcript: "preserved",
                status: .disconnected
            ))
            let pod = PodSummary(
                name: item.6,
                namespace: item.5,
                status: "Running",
                containerNamesLine: [item.3, item.7].compactMap { $0 }.joined(separator: ", ")
            )

            viewModel.startTerminalSession(
                for: pod,
                container: item.7,
                replacingSessionID: "terminal-\(index)"
            )

            XCTAssertEqual(state.terminalSessions.count, 1, "case \(index)")
            XCTAssertEqual(state.terminalSession?.transcript, "preserved", "case \(index)")
            XCTAssertEqual(state.terminalSession?.status, .disconnected, "case \(index)")
            XCTAssertTrue(state.lastError?.contains("terminal target changed") == true, "case \(index)")
        }
    }

    @MainActor
    func testScopeHardeningContextSwitchStopsAndClearsEveryTerminalSessionInLeavingContext() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
        let leaving = KubeContext(name: "context-leaving")
        let target = KubeContext(name: "context-target")
        state.setContexts([leaving, target])
        state.selectedContext = leaving
        state.selectedNamespace = "namespace-alpha"
        state.setTerminalSession(PodTerminalSession(
            id: "target-session",
            contextName: target.name,
            namespace: "namespace-target",
            podName: "pod-target",
            shell: "sh",
            status: .disconnected
        ))
        for (id, namespace) in [("leaving-a", "namespace-alpha"), ("leaving-b", "namespace-beta")] {
            state.setTerminalSession(PodTerminalSession(
                id: id,
                contextName: leaving.name,
                namespace: namespace,
                podName: "pod-\(id)",
                shell: "sh",
                status: .connected
            ))
        }

        viewModel.setContext(target)

        XCTAssertEqual(state.terminalSessions.map(\.id), ["target-session"])
        XCTAssertEqual(state.terminalSession?.id, "target-session")
    }

    @MainActor
    func testScopeHardeningNamespaceSwitchStopsAndClearsOnlyTerminalSessionsInLeavingNamespace() {
        let state = RuneAppState()
        let context = KubeContext(name: "context-alpha")
        state.selectedContext = context
        state.selectedNamespace = "namespace-leaving"
        let viewModel = RuneAppViewModel(state: state, kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
        state.setTerminalSession(PodTerminalSession(
            id: "target-session",
            contextName: context.name,
            namespace: "namespace-target",
            podName: "pod-target",
            shell: "sh",
            status: .disconnected
        ))
        state.setTerminalSession(PodTerminalSession(
            id: "leaving-session",
            contextName: context.name,
            namespace: "namespace-leaving",
            podName: "pod-leaving",
            shell: "sh",
            status: .connected
        ))

        viewModel.setNamespace("namespace-target")

        XCTAssertEqual(state.terminalSessions.map(\.id), ["target-session"])
        XCTAssertEqual(state.terminalSession?.id, "target-session")
    }

    @MainActor
    func testScopeHardeningPortForwardScopesAreValueSnapshotsAndRetryRequiresOriginalActiveScope() {
        let state = RuneAppState()
        let source = KubeConfigSource(url: URL(fileURLWithPath: "/tmp/rune-synthetic-kubeconfig"))
        let contextAlpha = KubeContext(name: "context-alpha")
        let contextBeta = KubeContext(name: "context-beta")
        state.setSources([source])
        state.setContexts([contextAlpha, contextBeta])
        state.selectedContext = contextAlpha
        state.selectedNamespace = "namespace-alpha"
        let viewModel = RuneAppViewModel(state: state)

        let startScope = viewModel.currentPortForwardStartScope()
        state.selectedContext = contextBeta
        state.selectedNamespace = "namespace-beta-current"
        let retrySession = PortForwardSession(
            id: "port-forward-retry",
            contextName: contextAlpha.name,
            namespace: "namespace-alpha-original",
            targetKind: .pod,
            targetName: "pod-alpha",
            localPort: 18080,
            remotePort: 8080,
            address: "127.0.0.1",
            status: .failed
        )
        XCTAssertNil(viewModel.portForwardRetryScope(for: retrySession))

        state.selectedContext = contextAlpha
        state.selectedNamespace = "namespace-alpha-original"
        let retryScope = viewModel.portForwardRetryScope(for: retrySession)

        XCTAssertEqual(startScope?.context, contextAlpha)
        XCTAssertEqual(startScope?.namespace, "namespace-alpha")
        XCTAssertEqual(startScope?.kubeConfigSources, [source])
        XCTAssertEqual(retryScope?.context, contextAlpha)
        XCTAssertEqual(retryScope?.namespace, "namespace-alpha-original")
        XCTAssertEqual(retryScope?.kubeConfigSources, [source])
    }

    @MainActor
    func testScopeHardeningPortForwardRetryKeepsFailedRowWhenOriginalScopeCannotBeResolved() {
        let state = RuneAppState()
        state.setContexts([KubeContext(name: "context-current")])
        state.selectedContext = KubeContext(name: "context-current")
        state.selectedNamespace = "namespace-current"
        let viewModel = RuneAppViewModel(state: state)
        let session = PortForwardSession(
            id: "port-forward-unresolved",
            contextName: "context-removed",
            namespace: "namespace-original",
            targetKind: .service,
            targetName: "service-alpha",
            localPort: 18081,
            remotePort: 8081,
            address: "127.0.0.1",
            status: .failed
        )
        state.upsertPortForwardSession(session)

        viewModel.retryPortForward(session)

        XCTAssertEqual(state.portForwardSessions, [session])
        XCTAssertTrue(state.lastError?.contains("original active context and namespace") == true)
    }

    @MainActor
    func testStartingTerminalSessionForConnectedPodReusesExistingTab() {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "benchmark")
        state.selectedNamespace = "default"
        let viewModel = RuneAppViewModel(state: state)
        state.setTerminalSession(PodTerminalSession(
            id: "shell-a",
            contextName: "benchmark",
            namespace: "default",
            podName: "pod-0",
            shell: "sh",
            transcript: "already connected",
            status: .connected
        ))

        viewModel.startTerminalSession(for: PodSummary(name: "pod-0", namespace: "default", status: "Running"))

        XCTAssertEqual(state.terminalSessions.map(\.id), ["shell-a"])
        XCTAssertEqual(state.terminalSession?.id, "shell-a")
        XCTAssertEqual(state.terminalSession?.transcript, "already connected")
    }

    @MainActor
    func testTerminalSessionsAreScopedBySelectedContainer() {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "benchmark")
        state.selectedNamespace = "default"
        let viewModel = RuneAppViewModel(state: state)
        let pod = PodSummary(
            name: "pod-0",
            namespace: "default",
            status: "Running",
            containerNamesLine: "app, sidecar"
        )

        viewModel.startTerminalSession(for: pod, container: "app")
        viewModel.startTerminalSession(for: pod, container: "sidecar")
        viewModel.startTerminalSession(for: pod, container: "app")

        XCTAssertEqual(state.terminalSessions.count, 2)
        XCTAssertEqual(state.terminalSessions.map(\.containerName), ["app", "sidecar"])
        XCTAssertEqual(state.terminalSession?.containerName, "app")
    }

    @MainActor
    func testTerminalFailureDiagnosticIsStoredAndClearedAfterReconnect() {
        let state = RuneAppState()
        state.setTerminalSession(PodTerminalSession(
            id: "shell",
            contextName: "demo",
            namespace: "default",
            podName: "pod-0",
            shell: "sh",
            status: .connecting
        ))
        let diagnostic = PodTerminalSessionDiagnostic(
            category: .transportDisconnected,
            summary: "Terminal stream disconnected for pod `pod-0`.",
            recoveryHint: "Reconnect when the cluster connection is healthy."
        )

        state.updateTerminalSessionStatus(id: "shell", status: .failed, diagnostic: diagnostic)

        XCTAssertEqual(state.terminalSession?.lastDiagnostic, diagnostic)

        state.updateTerminalSessionStatus(id: "shell", status: .connected)

        XCTAssertNil(state.terminalSession?.lastDiagnostic)
    }

    @MainActor
    func testTerminalSessionAppendAppliesScrollbackLimitFromSettings() {
        let defaults = UserDefaults.standard
        let oldValue = defaults.object(forKey: RuneSettingsKeys.terminalScrollbackLineLimit)
        defer {
            if let oldValue {
                defaults.set(oldValue, forKey: RuneSettingsKeys.terminalScrollbackLineLimit)
            } else {
                defaults.removeObject(forKey: RuneSettingsKeys.terminalScrollbackLineLimit)
            }
        }
        defaults.runeTerminalScrollbackLineLimit = RuneSettingsKeys.terminalScrollbackLineLimitMinimum

        let state = RuneAppState()
        state.setTerminalSession(PodTerminalSession(
            id: "shell",
            contextName: "demo",
            namespace: "default",
            podName: "pod-0",
            shell: "sh",
            status: .connected
        ))

        state.appendTerminalSessionOutput(
            id: "shell",
            text: (0..<(RuneSettingsKeys.terminalScrollbackLineLimitMinimum + 5)).map { "line \($0)" }.joined(separator: "\n")
        )

        let transcript = state.terminalSession?.transcript ?? ""
        XCTAssertTrue(transcript.hasPrefix(TerminalScrollbackRetention.truncationMarker))
        XCTAssertFalse(transcript.contains("line 0"))
        XCTAssertTrue(transcript.contains("line \(RuneSettingsKeys.terminalScrollbackLineLimitMinimum + 4)"))
    }

    @MainActor
    func testUnifiedLogCacheKeepsPodDeploymentAndServiceReadsSeparate() {
        let state = RuneAppState()
        let loadedAt = Date(timeIntervalSince1970: 1_776_000_000)

        state.appendPodLogRead(
            "pod line",
            contextName: "demo",
            namespace: "default",
            podName: "api",
            loadedAt: loadedAt
        )
        state.appendUnifiedServiceLogRead(
            "deployment line",
            pods: ["api-6f9"],
            contextName: "demo",
            namespace: "default",
            kind: .deployment,
            resourceName: "api",
            loadedAt: loadedAt
        )
        state.appendUnifiedServiceLogRead(
            "service line",
            pods: ["api-6f9"],
            contextName: "demo",
            namespace: "default",
            kind: .service,
            resourceName: "api",
            loadedAt: loadedAt
        )

        XCTAssertTrue(state.cachedLogs(contextName: "demo", namespace: "default", kind: .pod, resourceName: "api").contains("pod line"))
        XCTAssertTrue(state.cachedLogs(contextName: "demo", namespace: "default", kind: .deployment, resourceName: "api").contains("deployment line"))
        XCTAssertFalse(state.cachedLogs(contextName: "demo", namespace: "default", kind: .deployment, resourceName: "api").contains("service line"))
        XCTAssertTrue(state.cachedLogs(contextName: "demo", namespace: "default", kind: .service, resourceName: "api").contains("service line"))
    }

    @MainActor
    func testFavoriteResourcesSortBeforeOtherResources() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.favoriteResources.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let viewModel = RuneAppViewModel(
            state: state,
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: defaults)
        )
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "beta", namespace: "default", primaryText: "1 key", secondaryText: "1m"),
            ClusterResourceSummary(kind: .configMap, name: "alpha", namespace: "default", primaryText: "1 key", secondaryText: "1m")
        ])

        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["alpha", "beta"])

        viewModel.toggleFavoriteResource(kind: .configMap, namespace: "default", name: "beta")

        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["beta", "alpha"])
    }

    @MainActor
    func testGenericResourceListSortsByHeaderMetadata() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "sort-contract")
        state.selectedNamespace = "default"
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "beta", namespace: "default", primaryText: "1 key", secondaryText: "runtime"),
            ClusterResourceSummary(kind: .configMap, name: "alpha", namespace: "default", primaryText: "3 keys", secondaryText: "feature-flags"),
            ClusterResourceSummary(kind: .configMap, name: "gamma", namespace: "platform", primaryText: "2 keys", secondaryText: "settings")
        ])

        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["alpha", "beta", "gamma"])

        viewModel.toggleGenericResourceSort(.primary)
        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["alpha", "gamma", "beta"])

        viewModel.toggleGenericResourceSort(.secondary)
        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["alpha", "beta", "gamma"])

        viewModel.toggleGenericResourceSort(.namespace)
        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["alpha", "beta", "gamma"])

        viewModel.toggleGenericResourceSort(.namespace)
        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["gamma", "alpha", "beta"])
    }

    @MainActor
    func testDeploymentListSortsByHeaderMetadata() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "sort-contract")
        state.selectedNamespace = "default"
        state.setDeployments([
            DeploymentSummary(name: "api", namespace: "default", readyReplicas: 1, desiredReplicas: 3),
            DeploymentSummary(name: "worker", namespace: "default", readyReplicas: 3, desiredReplicas: 3),
            DeploymentSummary(name: "checkout", namespace: "default", readyReplicas: 0, desiredReplicas: 2)
        ])

        XCTAssertEqual(viewModel.visibleDeployments.map(\.name), ["api", "checkout", "worker"])

        viewModel.toggleDeploymentSort(.replicas)

        XCTAssertEqual(viewModel.visibleDeployments.map(\.name), ["worker", "api", "checkout"])

        viewModel.toggleDeploymentSort(.replicas)

        XCTAssertEqual(viewModel.visibleDeployments.map(\.name), ["checkout", "api", "worker"])
    }

    @MainActor
    func testServiceListSortsByHeaderMetadata() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "sort-contract")
        state.selectedNamespace = "default"
        state.setServices([
            ServiceSummary(name: "api", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.30"),
            ServiceSummary(name: "gateway", namespace: "default", type: "LoadBalancer", clusterIP: "10.0.0.10"),
            ServiceSummary(name: "worker", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.20")
        ])

        XCTAssertEqual(viewModel.visibleServices.map(\.name), ["api", "gateway", "worker"])

        viewModel.toggleServiceSort(.type)

        XCTAssertEqual(viewModel.visibleServices.map(\.name), ["api", "worker", "gateway"])

        viewModel.toggleServiceSort(.clusterIP)

        XCTAssertEqual(viewModel.visibleServices.map(\.name), ["gateway", "worker", "api"])
    }

    @MainActor
    func testHelmReleaseListSortsByHeaderMetadata() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "sort-contract")
        state.selectedNamespace = "default"
        state.setHelmReleases([
            HelmReleaseSummary(name: "worker", namespace: "default", revision: 2, updated: "2026-05-01 10:00:00", status: "deployed", chart: "worker-1.4.0", appVersion: "1.4.0"),
            HelmReleaseSummary(name: "api", namespace: "default", revision: 4, updated: "2026-05-01 10:01:00", status: "failed", chart: "api-1.6.0", appVersion: "1.6.0"),
            HelmReleaseSummary(name: "gateway", namespace: "platform", revision: 1, updated: "2026-05-01 10:02:00", status: "deployed", chart: "gateway-1.2.0", appVersion: "1.2.0")
        ])

        XCTAssertEqual(viewModel.visibleHelmReleases.map(\.name), ["api", "gateway", "worker"])

        viewModel.toggleHelmReleaseSort(.status)

        XCTAssertEqual(viewModel.visibleHelmReleases.map(\.name), ["gateway", "worker", "api"])

        viewModel.toggleHelmReleaseSort(.revision)

        XCTAssertEqual(viewModel.visibleHelmReleases.map(\.name), ["api", "worker", "gateway"])

        viewModel.toggleHelmReleaseSort(.namespace)

        XCTAssertEqual(viewModel.visibleHelmReleases.map(\.name), ["api", "worker", "gateway"])
    }

    @MainActor
    func testFavoriteResourcesPersistAndAreScopedByKindNamespaceAndName() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.favoriteResourceScope.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")

        viewModel.toggleFavoriteResource(kind: .pod, namespace: "default", name: "api")
        viewModel.toggleFavoriteResource(kind: .service, namespace: "default", name: "api")
        viewModel.toggleFavoriteResource(kind: .pod, namespace: "other", name: "api")

        XCTAssertTrue(viewModel.isFavoriteResource(kind: .pod, namespace: "default", name: "api"))
        XCTAssertTrue(viewModel.isFavoriteResource(kind: .service, namespace: "default", name: "api"))
        XCTAssertTrue(viewModel.isFavoriteResource(kind: .pod, namespace: "other", name: "api"))
        XCTAssertFalse(viewModel.isFavoriteResource(kind: .deployment, namespace: "default", name: "api"))

        let reloadedState = RuneAppState()
        let reloadedViewModel = RuneAppViewModel(state: reloadedState, contextPreferences: store)
        reloadedState.selectedContext = KubeContext(name: "demo")
        XCTAssertTrue(reloadedViewModel.isFavoriteResource(kind: .pod, namespace: "default", name: "api"))
        XCTAssertTrue(reloadedViewModel.isFavoriteResource(kind: .service, namespace: "default", name: "api"))
        XCTAssertTrue(reloadedViewModel.isFavoriteResource(kind: .pod, namespace: "other", name: "api"))
    }

    @MainActor
    func testFavoriteNamespacesPersistAndSortFirst() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.favoriteNamespaceScope.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")
        state.setNamespaces(["zeta", "alpha", "bravo"])
        state.selectedNamespace = "alpha"

        XCTAssertEqual(viewModel.namespaceOptions, ["alpha", "bravo", "zeta"])
        viewModel.toggleFavoriteNamespace("zeta")

        XCTAssertTrue(viewModel.isFavoriteNamespace("zeta"))
        XCTAssertEqual(viewModel.namespaceOptions, ["zeta", "alpha", "bravo"])

        let reloadedState = RuneAppState()
        let reloadedViewModel = RuneAppViewModel(state: reloadedState, contextPreferences: store)
        reloadedState.selectedContext = KubeContext(name: "demo")
        reloadedState.setNamespaces(["alpha", "zeta"])
        reloadedState.selectedNamespace = "alpha"
        XCTAssertEqual(reloadedViewModel.namespaceOptions, ["zeta", "alpha"])
    }

    @MainActor
    func testManualNamespacesPersistPerContextAndAppearWithoutListPermission() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.manualNamespaces.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")
        state.setNamespaces([])

        viewModel.setNamespace("team-a")

        XCTAssertEqual(store.loadManualNamespaces(for: "demo"), ["team-a"])
        XCTAssertEqual(viewModel.manualNamespaceOptions, ["team-a"])

        let reloadedState = RuneAppState()
        let reloadedViewModel = RuneAppViewModel(state: reloadedState, contextPreferences: store)
        reloadedState.selectedContext = KubeContext(name: "demo")
        reloadedState.selectedNamespace = ""
        reloadedState.setNamespaces([])

        XCTAssertEqual(reloadedViewModel.namespaceOptions, ["team-a"])
        XCTAssertEqual(reloadedViewModel.manualNamespaceOptions, ["team-a"])
    }

    @MainActor
    func testContextSwitchCarriesCurrentNamespaceWhenTargetContextHasItCached() {
        let state = RuneAppState()
        let store = ResourceStore()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            store: store
        )
        let source = KubeContext(name: "synthetic-source")
        let target = KubeContext(name: "synthetic-target")
        state.selectedContext = source
        state.selectedNamespace = "namespace-blue"
        store.cacheNamespaces(["default", "namespace-blue", "namespace-green"], context: target)

        viewModel.setContext(target)

        XCTAssertEqual(state.selectedContext?.name, "synthetic-target")
        XCTAssertEqual(state.selectedNamespace, "namespace-blue")
    }

    @MainActor
    func testContextSwitchNeverPublishesOrPersistsUnverifiedLeavingNamespace() async throws {
        let state = RuneAppState()
        let resourceStore = ResourceStore()
        let suiteName = "RuneAppStateTests.contextNamespaceIsolation.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let preferences = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            store: resourceStore,
            contextPreferences: preferences,
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        let source = KubeContext(name: "synthetic-source")
        let target = KubeContext(name: "synthetic-target")
        state.setContexts([source, target])
        state.selectedContext = source
        state.selectedNamespace = "source-only"
        state.setNamespaces(["source-only"])

        var observedTargetScopes: [(namespaces: [String], selectedNamespace: String)] = []
        let observation = Publishers.CombineLatest3(
            state.$selectedContext,
            state.$namespaces,
            state.$selectedNamespace
        )
        .sink { context, namespaces, selectedNamespace in
            guard context?.name == target.name else { return }
            observedTargetScopes.append((namespaces, selectedNamespace))
        }
        defer { observation.cancel() }

        viewModel.setContext(target)

        XCTAssertEqual(state.selectedContext, target)
        XCTAssertTrue(state.namespaces.isEmpty)
        XCTAssertTrue(state.selectedNamespace.isEmpty)
        XCTAssertFalse(viewModel.namespaceOptions.contains("source-only"))
        XCTAssertFalse(
            observedTargetScopes.contains {
                $0.namespaces.contains("source-only") || $0.selectedNamespace == "source-only"
            },
            "No observable target-context state may contain a namespace owned only by the leaving context."
        )

        try await waitUntilForRuneAppState {
            state.isManualNamespaceMode
        }

        XCTAssertNil(preferences.loadPreferredNamespace(for: target.name))
        XCTAssertTrue(preferences.loadManualNamespaces(for: target.name).isEmpty)
        XCTAssertFalse(viewModel.namespaceOptions.contains("source-only"))
    }

    @MainActor
    func testContextSwitchFallsBackToTargetSavedNamespaceWhenCarriedNamespaceIsMissing() {
        let state = RuneAppState()
        let store = ResourceStore()
        let suiteName = "RuneAppStateTests.contextNamespaceCarry.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let preferences = UserDefaultsContextPreferencesStore(defaults: defaults)
        preferences.savePreferredNamespace("namespace-green", for: "synthetic-target")

        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            store: store,
            contextPreferences: preferences
        )
        let source = KubeContext(name: "synthetic-source")
        let target = KubeContext(name: "synthetic-target")
        state.selectedContext = source
        state.selectedNamespace = "namespace-blue"
        store.cacheNamespaces(["default", "namespace-green"], context: target)

        viewModel.setContext(target)

        XCTAssertEqual(state.selectedContext?.name, "synthetic-target")
        XCTAssertEqual(state.selectedNamespace, "namespace-green")
    }

    @MainActor
    func testManualProductionContextMarkPersistsAndTriggersProductionConfirmationForSyntheticContext() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = true
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let suiteName = "RuneAppStateTests.manualProduction.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let context = KubeContext(name: "staging-west")

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = context
        state.selectedNamespace = "default"

        XCTAssertFalse(viewModel.isProductionContext)
        viewModel.toggleProductionMark(for: context)
        XCTAssertTrue(viewModel.isManuallyMarkedProduction(context))

        let reloadedState = RuneAppState()
        let reloadedViewModel = RuneAppViewModel(state: reloadedState, contextPreferences: store)
        reloadedState.selectedContext = context
        reloadedState.selectedNamespace = "default"
        reloadedViewModel.pendingWriteAction = .delete(kind: .pod, name: "api")

        XCTAssertTrue(reloadedViewModel.isProductionContext)
        XCTAssertTrue(reloadedViewModel.pendingWriteActionMessage.contains("Destructive production actions require a second confirmation"))
    }

    @MainActor
    func testUnmarkProductionContextRemovesManualOverrideWithoutDisablingAutoDetection() {
        let suiteName = "RuneAppStateTests.manualProductionUnmark.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: RuneAppState(), contextPreferences: store)
        let staging = KubeContext(name: "staging-west")

        viewModel.toggleProductionMark(for: staging)
        XCTAssertTrue(viewModel.isManuallyMarkedProduction(staging))
        XCTAssertTrue(viewModel.isProductionContext(staging))

        viewModel.toggleProductionMark(for: staging)
        XCTAssertFalse(viewModel.isManuallyMarkedProduction(staging))
        XCTAssertFalse(viewModel.isProductionContext(staging))
        XCTAssertTrue(viewModel.isProductionContext(KubeContext(name: "production-blue")))
    }

    @MainActor
    func testManualProductionContextPersistenceDoesNotStoreKubeconfigPayloads() {
        let suiteName = "RuneAppStateTests.manualProductionPrivacy.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let storageKey = "manualProductionContexts"
        let store = UserDefaultsContextPreferencesStore(
            defaults: defaults,
            manualProductionContextsKey: storageKey
        )
        let viewModel = RuneAppViewModel(state: RuneAppState(), contextPreferences: store)

        viewModel.toggleProductionMark(for: KubeContext(name: "staging-synthetic"))

        let stored = defaults.stringArray(forKey: storageKey) ?? []
        XCTAssertEqual(stored, ["staging-synthetic"])
        let serialized = stored.joined(separator: "\n")
        XCTAssertFalse(serialized.contains("apiVersion"))
        XCTAssertFalse(serialized.contains("clusters:"))
        XCTAssertFalse(serialized.contains("certificate-authority-data"))
        XCTAssertFalse(serialized.contains("token"))
        XCTAssertFalse(serialized.contains("users:"))
        XCTAssertFalse(serialized.contains("KUBECONFIG"))
    }

    func testPodSummaryExposesContainerNamesForLogSelection() {
        let pod = PodSummary(
            name: "api",
            namespace: "default",
            status: "Running",
            containerNamesLine: "api, metrics , sidecar",
            initContainerNamesLine: "setup",
            ephemeralContainerNamesLine: "debugger"
        )

        XCTAssertEqual(pod.containerNames, ["api", "metrics", "sidecar"])
        XCTAssertEqual(pod.logContainerNames, ["api", "metrics", "sidecar", "setup", "debugger"])
        XCTAssertEqual(RuneAppViewModel.normalizedTerminalContainerSelection("api", pod: pod), "api")
        XCTAssertNil(RuneAppViewModel.normalizedTerminalContainerSelection("setup", pod: pod))
        XCTAssertNil(RuneAppViewModel.normalizedTerminalContainerSelection("debugger", pod: pod))
    }

    @MainActor
    func testPodBulkSelectionReconcilesWhenPodListChanges() {
        let state = RuneAppState()
        let api = PodSummary(name: "api-0", namespace: "default", status: "Running")
        let worker = PodSummary(name: "worker-0", namespace: "default", status: "Running")
        let old = PodSummary(name: "old-0", namespace: "default", status: "Failed")
        state.setPods([api, worker, old])

        state.setSelectedPodIDs([api.id, old.id, "missing"])

        XCTAssertEqual(state.selectedPodIDs, [api.id, old.id])

        state.setPods([api, worker])

        XCTAssertEqual(state.selectedPodIDs, [api.id])
    }

    @MainActor
    func testSelectedPodsForBulkActionsPreserveHiddenSelectionsAcrossFilter() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "benchmark")
        state.selectedNamespace = "default"
        let pods = [
            PodSummary(name: "worker-0", namespace: "default", status: "Running"),
            PodSummary(name: "api-0", namespace: "default", status: "Running"),
            PodSummary(name: "api-1", namespace: "default", status: "Pending")
        ]
        state.setPods(pods)

        viewModel.togglePodBulkSelection(pods[0])
        viewModel.togglePodBulkSelection(pods[1])
        viewModel.togglePodBulkSelection(pods[2])

        XCTAssertEqual(viewModel.selectedPodsForBulkActions.map(\.name), ["api-0", "api-1", "worker-0"])

        viewModel.setResourceSearchQuery("api")

        XCTAssertEqual(viewModel.selectedPodsForBulkActions.map(\.name), ["api-0", "api-1", "worker-0"])
        XCTAssertEqual(viewModel.selectedPodCount, 3)

        viewModel.toggleAllVisiblePodsForBulkActions()

        XCTAssertEqual(viewModel.selectedPodsForBulkActions.map(\.name), ["worker-0"])
        XCTAssertFalse(viewModel.areAllVisiblePodsSelectedForBulkActions)

        viewModel.toggleAllVisiblePodsForBulkActions()

        XCTAssertEqual(viewModel.selectedPodsForBulkActions.map(\.name), ["api-0", "api-1", "worker-0"])
        XCTAssertTrue(viewModel.areAllVisiblePodsSelectedForBulkActions)
    }

    @MainActor
    func testPodFavoritesSortFirstWhilePreservingMetricSortWithinGroups() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.podMetricSort.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let viewModel = RuneAppViewModel(
            state: state,
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: defaults)
        )
        state.selectedContext = KubeContext(name: "sort-contract")
        state.selectedNamespace = "default"
        state.setPods([
            PodSummary(name: "favorite-low-cpu-old", namespace: "default", status: "Running", ageDescription: "13d", cpuUsage: "1m"),
            PodSummary(name: "favorite-high-cpu-middle", namespace: "default", status: "Running", ageDescription: "1d", cpuUsage: "42m"),
            PodSummary(name: "mid-cpu-new", namespace: "default", status: "Running", ageDescription: "4h", cpuUsage: "3m"),
            PodSummary(name: "missing-cpu", namespace: "default", status: "Running", ageDescription: "2d")
        ])
        viewModel.toggleFavoriteResource(kind: .pod, namespace: "default", name: "favorite-low-cpu-old")
        viewModel.toggleFavoriteResource(kind: .pod, namespace: "default", name: "favorite-high-cpu-middle")

        viewModel.togglePodSort(.cpu)

        XCTAssertEqual(
            viewModel.visiblePods.map(\.name),
            ["favorite-high-cpu-middle", "favorite-low-cpu-old", "mid-cpu-new", "missing-cpu"]
        )

        viewModel.togglePodSort(.age)

        XCTAssertEqual(
            viewModel.visiblePods.map(\.name),
            ["favorite-high-cpu-middle", "favorite-low-cpu-old", "mid-cpu-new", "missing-cpu"]
        )
    }

    @MainActor
    func testDeploymentAndServiceFavoritesSortFirstWithoutDroppingActiveSort() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.workloadFavoriteSort.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let viewModel = RuneAppViewModel(
            state: state,
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: defaults)
        )
        state.selectedContext = KubeContext(name: "sort-contract")
        state.selectedNamespace = "default"
        state.setDeployments([
            DeploymentSummary(name: "api", namespace: "default", readyReplicas: 1, desiredReplicas: 3),
            DeploymentSummary(name: "worker", namespace: "default", readyReplicas: 3, desiredReplicas: 3),
            DeploymentSummary(name: "checkout", namespace: "default", readyReplicas: 0, desiredReplicas: 2)
        ])
        state.setServices([
            ServiceSummary(name: "api", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.30"),
            ServiceSummary(name: "gateway", namespace: "default", type: "LoadBalancer", clusterIP: "10.0.0.10"),
            ServiceSummary(name: "worker", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.20")
        ])

        viewModel.toggleFavoriteResource(kind: .deployment, namespace: "default", name: "checkout")
        viewModel.toggleDeploymentSort(.replicas)

        XCTAssertEqual(viewModel.visibleDeployments.map(\.name), ["checkout", "worker", "api"])

        viewModel.toggleFavoriteResource(kind: .service, namespace: "default", name: "gateway")
        viewModel.toggleServiceSort(.type)

        XCTAssertEqual(viewModel.visibleServices.map(\.name), ["gateway", "api", "worker"])
    }

    @MainActor
    func testResourceSearchQueryNormalizesPastedLineBreaksBeforeFiltering() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.setPods([
            PodSummary(name: "api-0", namespace: "default", status: "Pending"),
            PodSummary(name: "api-1", namespace: "default", status: "Running"),
            PodSummary(name: "worker-0", namespace: "default", status: "Running")
        ])

        viewModel.setResourceSearchQuery("api\nPending\n")

        XCTAssertEqual(state.resourceSearchQuery, "api Pending")
        XCTAssertEqual(viewModel.visiblePods.map(\.name), ["api-0"])
    }

    @MainActor
    func testResourceSearchQueryKeepsNormalTypingButSanitizesPasteSeparators() {
        XCTAssertEqual(
            RuneAppViewModel.normalizedResourceSearchQueryInput("api  worker"),
            "api  worker"
        )
        XCTAssertEqual(
            RuneAppViewModel.normalizedResourceSearchQueryInput("api\tworker\r\npending\n"),
            "api worker pending"
        )
    }

    @MainActor
    func testGenericResourceBulkSelectionBuildsDeleteConfirmation() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .configMap
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "settings", namespace: "default", primaryText: "2 keys", secondaryText: "Data"),
            ClusterResourceSummary(kind: .configMap, name: "feature-flags", namespace: "default", primaryText: "1 key", secondaryText: "Data")
        ])

        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[0])
        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[1])

        XCTAssertEqual(viewModel.selectedGenericResourceCount, 2)
        XCTAssertTrue(viewModel.areAllVisibleGenericResourcesSelectedForBulkActions)

        viewModel.requestDeleteSelectedGenericResources()

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Delete 2")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("2 selected resources"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("configmap/feature-flags"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("configmap/settings"))
        XCTAssertTrue(viewModel.pendingWriteActionIsDestructive)
    }

    @MainActor
    func testGenericSelectVisiblePreservesAndActsOnHiddenSelections() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "synthetic-context")
        state.selectedNamespace = "synthetic-namespace"
        state.selectedWorkloadKind = .configMap
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "api-a", namespace: "synthetic-namespace", primaryText: "1 key", secondaryText: "1 text value · 0 binary values"),
            ClusterResourceSummary(kind: .configMap, name: "api-b", namespace: "synthetic-namespace", primaryText: "1 key", secondaryText: "1 text value · 0 binary values"),
            ClusterResourceSummary(kind: .configMap, name: "worker", namespace: "synthetic-namespace", primaryText: "1 key", secondaryText: "1 text value · 0 binary values")
        ])

        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[2])
        viewModel.setResourceSearchQuery("api")
        viewModel.toggleAllVisibleGenericResourcesForBulkActions()

        XCTAssertEqual(viewModel.selectedGenericResourceCount, 3)
        XCTAssertEqual(
            Set(viewModel.selectedGenericResourcesForBulkActions.map(\.name)),
            ["api-a", "api-b", "worker"]
        )
        XCTAssertTrue(viewModel.areAllVisibleGenericResourcesSelectedForBulkActions)

        viewModel.toggleAllVisibleGenericResourcesForBulkActions()

        XCTAssertEqual(viewModel.selectedGenericResourcesForBulkActions.map(\.name), ["worker"])
        XCTAssertFalse(viewModel.areAllVisibleGenericResourcesSelectedForBulkActions)
    }

    @MainActor
    func testBulkSelectionsClearWhenResourceScopeChangesEvenIfTargetHasMatchingIDs() {
        let state = RuneAppState()
        let store = ResourceStore()
        let source = KubeContext(name: "synthetic-source")
        let target = KubeContext(name: "synthetic-target")
        let namespace = "synthetic-namespace"
        let pod = PodSummary(name: "api-0", namespace: namespace, status: "Running")
        let configMap = ClusterResourceSummary(
            kind: .configMap,
            name: "settings",
            namespace: namespace,
            primaryText: "1 key",
            secondaryText: "1 text value · 0 binary values"
        )
        state.selectedContext = source
        state.selectedNamespace = namespace
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setPods([pod])
        state.setConfigMaps([configMap])
        state.setSelectedPodIDs([pod.id])
        state.setSelectedGenericResourceIDs([configMap.id], validIDs: [configMap.id])

        store.cacheNamespaces([namespace, "next-namespace"], context: target)
        store.cacheSnapshot(
            context: target,
            namespace: namespace,
            pods: [pod],
            deployments: [],
            statefulSets: [],
            daemonSets: [],
            jobs: [],
            cronJobs: [],
            replicaSets: [],
            persistentVolumeClaims: [],
            horizontalPodAutoscalers: [],
            networkPolicies: [],
            services: [],
            endpoints: [],
            ingresses: [],
            configMaps: [configMap],
            secrets: [],
            serviceAccounts: [],
            events: []
        )
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            store: store
        )

        viewModel.setContext(target)

        XCTAssertEqual(state.pods.map(\.id), [pod.id])
        XCTAssertEqual(state.configMaps.map(\.id), [configMap.id])
        XCTAssertTrue(state.selectedPodIDs.isEmpty)
        XCTAssertTrue(state.selectedGenericResourceIDs.isEmpty)

        state.setSelectedPodIDs([pod.id])
        state.setSelectedGenericResourceIDs([configMap.id], validIDs: [configMap.id])
        viewModel.setNamespace("next-namespace")

        XCTAssertTrue(state.selectedPodIDs.isEmpty)
        XCTAssertTrue(state.selectedGenericResourceIDs.isEmpty)

        state.setConfigMaps([configMap])
        state.setSelectedGenericResourceIDs([configMap.id], validIDs: [configMap.id])
        viewModel.setWorkloadKind(.secret)

        XCTAssertTrue(state.selectedGenericResourceIDs.isEmpty)

        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setPods([pod])
        state.setConfigMaps([configMap])
        state.setSelectedPodIDs([pod.id])
        state.setSelectedGenericResourceIDs([configMap.id], validIDs: [configMap.id])

        viewModel.setSection(.events)

        XCTAssertEqual(state.selectedSection, .events)
        XCTAssertEqual(
            state.selectedWorkloadKind,
            .configMap,
            "The regression must exercise a section-only scope change without an implicit kind change."
        )
        XCTAssertTrue(state.selectedPodIDs.isEmpty)
        XCTAssertTrue(state.selectedGenericResourceIDs.isEmpty)
    }

    @MainActor
    func testBulkSelectionsClearWhenWorkspaceResolvesAnotherNamespaceInSameContext() {
        let state = RuneAppState()
        let store = ResourceStore()
        let context = KubeContext(name: "synthetic-context")
        let namespaceA = "namespace-a"
        let namespaceB = "namespace-b"
        let configMapA = ClusterResourceSummary(
            kind: .configMap,
            name: "settings-a",
            namespace: namespaceA,
            primaryText: "1 key",
            secondaryText: "1 text value · 0 binary values"
        )
        let configMapB = ClusterResourceSummary(
            kind: .configMap,
            name: "settings-b",
            namespace: namespaceB,
            primaryText: "1 key",
            secondaryText: "1 text value · 0 binary values"
        )
        state.setContexts([context])
        state.selectedContext = context
        state.selectedNamespace = namespaceA
        state.selectedSection = .config
        state.selectedWorkloadKind = .configMap
        state.setConfigMaps([configMapA])
        state.setSelectedGenericResourceIDs([configMapA.id], validIDs: [configMapA.id])
        store.cacheNamespaces([namespaceA, namespaceB], context: context)
        cacheSyntheticSnapshot(configMaps: [configMapA], context: context, namespace: namespaceA, store: store)
        cacheSyntheticSnapshot(configMaps: [configMapB], context: context, namespace: namespaceB, store: store)
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            store: store
        )
        let workspaceA = SavedWorkspaceSnapshot(
            name: "Namespace A",
            contextName: context.name,
            namespace: namespaceA,
            section: .config,
            workloadKind: .configMap,
            resourceKind: nil,
            resourceName: nil,
            resourceNamespace: nil
        )
        let workspaceB = SavedWorkspaceSnapshot(
            name: "Namespace B",
            contextName: context.name,
            namespace: namespaceB,
            section: .config,
            workloadKind: .configMap,
            resourceKind: nil,
            resourceName: nil,
            resourceNamespace: nil
        )

        viewModel.openSavedWorkspace(workspaceB)

        XCTAssertEqual(state.selectedNamespace, namespaceB)
        XCTAssertTrue(state.selectedGenericResourceIDs.isEmpty)

        viewModel.openSavedWorkspace(workspaceA)

        XCTAssertEqual(state.selectedNamespace, namespaceA)
        XCTAssertTrue(state.selectedGenericResourceIDs.isEmpty)
        XCTAssertEqual(viewModel.selectedGenericResourceCount, 0)
    }

    @MainActor
    func testSelectedGenericResourceComparisonIncludesFilterHiddenSelection() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .configMap
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "settings", namespace: "default", primaryText: "2 keys", secondaryText: "Data"),
            ClusterResourceSummary(kind: .configMap, name: "feature-flags", namespace: "default", primaryText: "1 key", secondaryText: "Data")
        ])

        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[0])
        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[1])
        viewModel.setResourceSearchQuery("settings")

        let comparison = viewModel.selectedGenericResourceComparisonText

        XCTAssertEqual(viewModel.visibleConfigMaps.map(\.name), ["settings"])
        XCTAssertTrue(viewModel.canCopySelectedGenericResourceComparison)
        XCTAssertTrue(comparison.contains("Selected ConfigMaps Compare"))
        XCTAssertTrue(comparison.contains("Namespace: default"))
        XCTAssertTrue(comparison.contains("feature-flags"))
        XCTAssertTrue(comparison.contains("settings"))
        XCTAssertTrue(comparison.contains("Primary: 1 key"))
        XCTAssertTrue(comparison.contains("Primary: 2 keys"))
        XCTAssertFalse(comparison.contains("token"))
        XCTAssertFalse(comparison.contains("kubeconfig"))
    }

    @MainActor
    func testSelectedGenericResourceComparisonRequiresAtLeastTwoResources() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .configMap
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "settings", namespace: "default", primaryText: "2 keys", secondaryText: "Data")
        ])

        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[0])

        XCTAssertFalse(viewModel.canCopySelectedGenericResourceComparison)
        XCTAssertTrue(viewModel.selectedGenericResourceComparisonText.contains("settings"))
    }

    @MainActor
    func testSelectedGenericResourceComparisonCopiesToRequestedPasteboard() throws {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "synthetic-context")
        state.selectedNamespace = "synthetic-namespace"
        state.selectedWorkloadKind = .configMap
        state.setConfigMaps([
            ClusterResourceSummary(kind: .configMap, name: "app-settings", namespace: "synthetic-namespace", primaryText: "2 keys", secondaryText: "Data"),
            ClusterResourceSummary(kind: .configMap, name: "feature-flags", namespace: "synthetic-namespace", primaryText: "1 key", secondaryText: "Data")
        ])
        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[0])
        viewModel.toggleGenericResourceBulkSelection(viewModel.visibleConfigMaps[1])
        let pasteboard = NSPasteboard(name: NSPasteboard.Name("RuneComparisonTest-(UUID().uuidString)"))

        viewModel.copySelectedGenericResourceComparisonToClipboard(to: pasteboard)

        let copiedText = try XCTUnwrap(pasteboard.string(forType: .string))
        XCTAssertEqual(copiedText, viewModel.selectedGenericResourceComparisonText)
        XCTAssertTrue(copiedText.contains("app-settings"))
        XCTAssertTrue(copiedText.contains("feature-flags"))
    }

    @MainActor
    func testOperatorResourcesParticipateInSearchAcrossFamilyKindStatusAndMessage() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.setOperatorResources([
            OperatorResourceSummary(
                family: "Flux",
                kind: "Kustomizations",
                apiPath: "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/default/kustomizations",
                name: "frontend",
                namespace: "default",
                status: "Ready True",
                message: "Applied revision main@sha1",
                printerColumns: [
                    OperatorResourceSummary.PrinterColumn(title: "Revision", value: "main@sha1"),
                    OperatorResourceSummary.PrinterColumn(title: "Age", value: "8m")
                ]
            ),
            OperatorResourceSummary(
                family: "ArgoCD",
                kind: "Applications",
                apiPath: "/apis/argoproj.io/v1alpha1/namespaces/default/applications",
                name: "payments",
                namespace: "default",
                status: "SyncError False",
                message: "Waiting for health"
            )
        ])

        state.resourceSearchQuery = "syncerror"
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["payments"])

        state.resourceSearchQuery = "revision"
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["frontend"])

        state.resourceSearchQuery = "flux"
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["frontend"])

        state.resourceSearchQuery = "8m"
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["frontend"])
    }

    @MainActor
    func testOperatorResourceGitOpsFocusFiltersFluxAndArgoWithoutDroppingSortOrFavorites() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.operatorGitOpsFocus.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")
        state.setOperatorResources([
            OperatorResourceSummary(
                family: "cert-manager",
                kind: "Certificates",
                apiPath: "/apis/cert-manager.io/v1/namespaces/default/certificates",
                name: "api-tls",
                namespace: "default",
                status: "Ready",
                message: ""
            ),
            OperatorResourceSummary(
                family: "Flux",
                kind: "Kustomizations",
                apiPath: "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/default/kustomizations",
                name: "frontend",
                namespace: "default",
                status: "Ready",
                message: ""
            ),
            OperatorResourceSummary(
                family: "ArgoCD",
                kind: "Applications",
                apiPath: "/apis/argoproj.io/v1alpha1/namespaces/default/applications",
                name: "payments",
                namespace: "default",
                status: "OutOfSync",
                message: ""
            ),
            OperatorResourceSummary(
                family: "Gateway API",
                kind: "Gateways",
                apiPath: "/apis/gateway.networking.k8s.io/v1/namespaces/default/gateways",
                name: "edge",
                namespace: "default",
                status: "Programmed",
                message: ""
            )
        ])

        XCTAssertEqual(viewModel.gitOpsOperatorResourceCount, 2)
        XCTAssertEqual(viewModel.fluxOperatorResourceCount, 1)
        XCTAssertEqual(viewModel.argoCDOperatorResourceCount, 1)
        XCTAssertEqual(viewModel.unhealthyGitOpsOperatorResourceCount, 1)
        XCTAssertEqual(viewModel.operatorResourceFocusSummary, "4 operator resources")

        viewModel.pageOperatorResourcesForward()
        viewModel.toggleFavoriteOperatorResource(state.operatorResources[1])
        viewModel.setOperatorResourceFocus(.gitOps)

        XCTAssertEqual(viewModel.operatorResourcePage, 0)
        XCTAssertEqual(viewModel.operatorResourceFocusSummary, "2 GitOps resources • Flux 1 • ArgoCD 1 • Unhealthy 1")
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["frontend", "payments"])

        state.resourceSearchQuery = "outofsync"
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["payments"])
        state.resourceSearchQuery = ""
        viewModel.setOperatorResourceFocus(.flux)
        XCTAssertEqual(viewModel.operatorResourceFocusSummary, "1 Flux resource • Unhealthy 0")
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["frontend"])

        viewModel.setOperatorResourceFocus(.argoCD)
        XCTAssertEqual(viewModel.operatorResourceFocusSummary, "1 ArgoCD resource • Unhealthy 1")
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["payments"])

        viewModel.setOperatorResourceFocus(.unhealthy)
        XCTAssertEqual(viewModel.operatorResourceFocusSummary, "1 unhealthy GitOps resource")
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["payments"])
        XCTAssertTrue(RuneAppViewModel.isGitOpsOperatorResource(state.operatorResources[1]))
        XCTAssertTrue(RuneAppViewModel.isGitOpsOperatorResource(state.operatorResources[2]))
        XCTAssertFalse(RuneAppViewModel.isGitOpsOperatorResource(state.operatorResources[0]))
    }

    @MainActor
    func testOperatorResourceFavoritesSortFirst() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.operatorFavorites.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")
        state.setOperatorResources([
            OperatorResourceSummary(
                family: "Custom Resources",
                kind: "Widgets",
                apiPath: "/apis/example.io/v1/namespaces/default/widgets",
                name: "alpha",
                namespace: "default",
                status: "Ready",
                message: ""
            ),
            OperatorResourceSummary(
                family: "Custom Resources",
                kind: "Widgets",
                apiPath: "/apis/example.io/v1/namespaces/default/widgets",
                name: "beta",
                namespace: "default",
                status: "Ready",
                message: ""
            )
        ])

        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["alpha", "beta"])
        viewModel.toggleFavoriteOperatorResource(state.operatorResources[1])
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["beta", "alpha"])
    }

    @MainActor
    func testOperatorPrinterColumnsCanBeHiddenPerFamilyWithoutChangingFavoriteOrder() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.operatorPrinterColumns.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")
        state.setOperatorResources([
            OperatorResourceSummary(
                family: "Custom Resources",
                kind: "Widgets",
                apiPath: "/apis/example.io/v1/namespaces/default/widgets",
                name: "alpha",
                namespace: "default",
                status: "Ready",
                message: "",
                printerColumns: [OperatorResourceSummary.PrinterColumn(title: "Age", value: "1m")]
            ),
            OperatorResourceSummary(
                family: "Custom Resources",
                kind: "Widgets",
                apiPath: "/apis/example.io/v1/namespaces/default/widgets",
                name: "beta",
                namespace: "default",
                status: "Ready",
                message: "",
                printerColumns: [OperatorResourceSummary.PrinterColumn(title: "Age", value: "2m")]
            )
        ])
        viewModel.toggleFavoriteOperatorResource(state.operatorResources[1])

        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["beta", "alpha"])
        XCTAssertTrue(viewModel.showsOperatorPrinterColumnsForCurrentFamily)

        viewModel.toggleOperatorPrinterColumnsForCurrentFamily()

        XCTAssertFalse(viewModel.showsOperatorPrinterColumnsForCurrentFamily)
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["beta", "alpha"])
        XCTAssertEqual(store.loadHiddenOperatorPrinterColumnFamilies(), ["Custom Resources"])

        let reloaded = RuneAppViewModel(state: state, contextPreferences: store)
        XCTAssertFalse(reloaded.showsOperatorPrinterColumnsForCurrentFamily)
    }

    @MainActor
    func testOperatorResourcesSortBySelectedColumnWithFavoritesFirst() {
        let state = RuneAppState()
        let suiteName = "RuneAppStateTests.operatorSort.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = UserDefaultsContextPreferencesStore(defaults: defaults)
        let viewModel = RuneAppViewModel(state: state, contextPreferences: store)
        state.selectedContext = KubeContext(name: "demo")
        state.setOperatorResources([
            OperatorResourceSummary(
                family: "Flux",
                kind: "Kustomizations",
                apiPath: "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/default/kustomizations",
                name: "frontend",
                namespace: "default",
                status: "Ready",
                message: ""
            ),
            OperatorResourceSummary(
                family: "ArgoCD",
                kind: "Applications",
                apiPath: "/apis/argoproj.io/v1alpha1/namespaces/default/applications",
                name: "payments",
                namespace: "default",
                status: "Progressing",
                message: ""
            ),
            OperatorResourceSummary(
                family: "Custom Resources",
                kind: "Widgets",
                apiPath: "/apis/example.io/v1/widgets",
                name: "cluster-widget",
                namespace: nil,
                status: "Unknown",
                message: ""
            )
        ])

        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["payments", "cluster-widget", "frontend"])

        viewModel.toggleOperatorResourceSort(.status)
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["payments", "frontend", "cluster-widget"])

        viewModel.toggleOperatorResourceSort(.status)
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name), ["cluster-widget", "frontend", "payments"])

        viewModel.toggleFavoriteOperatorResource(state.operatorResources[1])
        XCTAssertEqual(viewModel.visibleOperatorResources.map(\.name).first, "payments")
    }

    @MainActor
    func testOperatorResourcesArePagedForLargeCRDBrowsing() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.setOperatorResources((0..<45).map { index in
            OperatorResourceSummary(
                family: "Custom Resources",
                kind: "Widgets",
                apiPath: "/apis/example.io/v1/namespaces/default/widgets",
                name: String(format: "widget-%02d", index),
                namespace: "default",
                status: "Ready",
                message: ""
            )
        })

        XCTAssertEqual(viewModel.pagedOperatorResources.count, 40)
        XCTAssertEqual(viewModel.operatorResourcePageSummary, "1-40 of 45")
        XCTAssertTrue(viewModel.canPageOperatorResourcesForward)

        viewModel.pageOperatorResourcesForward()

        XCTAssertEqual(viewModel.pagedOperatorResources.map(\.name), ["widget-40", "widget-41", "widget-42", "widget-43", "widget-44"])
        XCTAssertEqual(viewModel.operatorResourcePageSummary, "41-45 of 45")
        XCTAssertTrue(viewModel.canPageOperatorResourcesBackward)
        XCTAssertFalse(viewModel.canPageOperatorResourcesForward)
    }

    @MainActor
    func testOperatorResourceSelectionIsSeparateFromHelmReleaseForDrilldown() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let release = HelmReleaseSummary(
            name: "sample-release",
            namespace: "default",
            revision: 1,
            updated: "now",
            status: "deployed",
            chart: "sample-1.0.0",
            appVersion: "1.0.0"
        )
        let resource = OperatorResourceSummary(
            family: "Custom Resources",
            kind: "Widgets",
            apiPath: "/apis/example.test/v1/namespaces/default/widgets",
            name: "widget-a",
            namespace: "default",
            status: "Ready True",
            message: "Ready"
        )

        state.setHelmReleases([release])
        state.setOperatorResources([resource])
        viewModel.selectHelmRelease(release)
        XCTAssertEqual(state.selectedHelmRelease, release)
        XCTAssertNil(state.selectedOperatorResource)

        viewModel.selectOperatorResource(resource)

        XCTAssertEqual(state.selectedOperatorResource, resource)
        XCTAssertNil(state.selectedHelmRelease)
    }

    @MainActor
    func testManualNamespaceModeIsExplicitNonBlockingState() {
        let state = RuneAppState()

        state.setManualNamespaceMode(true, warning: "You cannot list namespaces, but you can work in a namespace manually.")

        XCTAssertTrue(state.isManualNamespaceMode)
        XCTAssertEqual(state.namespaceAccessWarning, "You cannot list namespaces, but you can work in a namespace manually.")

        state.clearManualNamespaceMode()
        XCTAssertFalse(state.isManualNamespaceMode)
        XCTAssertNil(state.namespaceAccessWarning)
    }

    @MainActor
    func testLogAndResourceDetailUpdatesExposeTimestamps() {
        let state = RuneAppState()
        let loadedAt = Date(timeIntervalSince1970: 1_700_000_000)

        state.appendPodLogRead(
            "ready",
            contextName: "demo",
            namespace: "default",
            podName: "api",
            loadedAt: loadedAt
        )
        state.setResourceYAML("kind: Pod\nmetadata:\n  name: api\n")

        XCTAssertEqual(state.lastLogUpdatedAt, loadedAt)
        XCTAssertNotNil(state.lastResourceDetailsUpdatedAt)
    }

    @MainActor
    func testLogStreamPauseIsDistinctFromTailMode() {
        let viewModel = RuneAppViewModel(state: RuneAppState())

        XCTAssertFalse(viewModel.isLogTailModeEnabled)
        viewModel.toggleLogStreamPause()
        XCTAssertFalse(viewModel.isLogStreamPaused)

        viewModel.isLogTailModeEnabled = true
        viewModel.toggleLogStreamPause()

        XCTAssertTrue(viewModel.isLogTailModeEnabled)
        XCTAssertTrue(viewModel.isLogStreamPaused)

        viewModel.toggleLogStreamPause()

        XCTAssertTrue(viewModel.isLogTailModeEnabled)
        XCTAssertFalse(viewModel.isLogStreamPaused)

        viewModel.isLogTailModeEnabled = false

        XCTAssertFalse(viewModel.isLogTailModeEnabled)
        XCTAssertFalse(viewModel.isLogStreamPaused)
    }

    @MainActor
    func testCreateManualJobFromCronJobRequiresPendingWriteConfirmation() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.setSelectedCronJob(ClusterResourceSummary(
            kind: .cronJob,
            name: "nightly",
            namespace: "default",
            primaryText: "0 2 * * *",
            secondaryText: "Active"
        ))

        viewModel.createManualJobFromSelectedCronJob()

        XCTAssertEqual(viewModel.pendingWriteActionTitle, "Create a Job from CronJob nightly?")
        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Create Job")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("nightly-manual-"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("selected CronJob template"))
    }

    @MainActor
    func testReadOnlyModeBlocksPendingWriteBeforeKubernetesCallAndAuditAppend() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.isReadOnlyMode = true
        viewModel.pendingWriteAction = .createJobFromCronJob(cronJobName: "nightly", jobName: "nightly-manual-1")

        viewModel.confirmPendingWriteAction()

        XCTAssertNil(viewModel.pendingWriteAction)
        XCTAssertEqual(state.lastError, "Read-only mode is on; write actions are blocked.")
        XCTAssertTrue(state.writeAuditLog.isEmpty)
    }

    @MainActor
    func testProductionDestructiveWriteRequiresSecondConfirmation() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = true
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "prod")
        state.selectedNamespace = "default"
        viewModel.pendingWriteAction = .delete(kind: .pod, name: "api")

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Review Production Action")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Destructive production actions require a second confirmation"))

        viewModel.confirmPendingWriteAction()

        XCTAssertEqual(viewModel.pendingProductionDestructiveConfirmation, .delete(kind: .pod, name: "api"))
        XCTAssertEqual(viewModel.pendingWriteAction, .delete(kind: .pod, name: "api"))
        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Delete")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Final confirmation required"))
        XCTAssertTrue(state.writeAuditLog.isEmpty)

        viewModel.cancelPendingWriteAction()

        XCTAssertNil(viewModel.pendingProductionDestructiveConfirmation)
        XCTAssertNil(viewModel.pendingWriteAction)
    }

    @MainActor
    func testProductionRolloutRollbackRequiresSecondConfirmation() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = true
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "prod")
        state.selectedNamespace = "default"
        viewModel.pendingWriteAction = .rolloutUndo(deploymentName: "api", revision: 2)

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Review Production Action")

        viewModel.confirmPendingWriteAction()

        XCTAssertEqual(viewModel.pendingProductionDestructiveConfirmation, .rolloutUndo(deploymentName: "api", revision: 2))
        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Rollback")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("Final confirmation required"))
    }

    @MainActor
    func testProductionHelmRollbackRequiresSecondConfirmationBeforeRunningHelm() {
        let previousProduction = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = true
        UserDefaults.standard.runeWriteSafetyRequireHelmDryRun = true
        defer {
            restoreUserDefaultsValue(previousProduction, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        }

        let state = RuneAppState()
        let helmRunner = RecordingHelmCommandRunner()
        let viewModel = RuneAppViewModel(state: state, helmCommandRunner: helmRunner)
        state.selectedContext = KubeContext(name: "prod")
        state.selectedNamespace = "payments"
        state.setSelectedHelmRelease(
            HelmReleaseSummary(
                name: "api",
                namespace: "payments",
                revision: 3,
                updated: "2026-05-05 10:00:00",
                status: "deployed",
                chart: "api-1.2.0",
                appVersion: "1.2.0"
            )
        )

        viewModel.requestHelmRollback(revision: 2)

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Review Production Action")
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("No kubeconfig selected"))

        viewModel.confirmPendingWriteAction()

        XCTAssertEqual(
            viewModel.pendingProductionDestructiveConfirmation,
            .helmRollback(releaseName: "api", namespace: "payments", revision: 2, wait: true, timeout: "5m", cleanupOnFail: true)
        )
        XCTAssertTrue(helmRunner.requests.isEmpty)
        XCTAssertTrue(state.writeAuditLog.isEmpty)
    }

    @MainActor
    func testDisabledProductionSecondConfirmationSettingUsesSingleConfirmationLabel() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = false
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "prod")
        state.selectedNamespace = "default"
        viewModel.pendingWriteAction = .delete(kind: .pod, name: "api")

        XCTAssertEqual(viewModel.pendingWriteActionConfirmLabel, "Delete")
        XCTAssertFalse(viewModel.pendingWriteActionMessage.contains("Destructive production actions require a second confirmation"))
    }

    @MainActor
    func testDisabledCopyableCommandSettingHidesPendingWriteCommandPreview() {
        let previous = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireCopyableCommand)
        UserDefaults.standard.runeWriteSafetyRequireCopyableCommand = false
        defer {
            restoreUserDefaultsValue(previous, forKey: RuneSettingsKeys.writeSafetyRequireCopyableCommand)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        viewModel.pendingWriteAction = .delete(kind: .pod, name: "api")

        XCTAssertEqual(viewModel.pendingWriteActionKubectlCommand, "")
    }

    @MainActor
    func testPendingApplyDiffPreviewTruncatesLargeYAML() {
        let baseline = (0..<40).map { "key\($0): old" }.joined(separator: "\n")
        let edited = (0..<40).map { "key\($0): new" }.joined(separator: "\n")
        let action = PendingWriteAction.apply(kind: .configMap, name: "settings", yaml: edited, baseline: baseline)

        let message = action.message

        XCTAssertTrue(message.contains("YAML diff preview"))
        XCTAssertTrue(message.contains("- key0: old"))
        XCTAssertTrue(message.contains("+ key0: new"))
        XCTAssertTrue(message.contains("… diff truncated"))
    }

    @MainActor
    func testPendingWriteActionBuildsCopyableKubectlCommand() {
        let action = PendingWriteAction.scale(deploymentName: "api service", replicas: 3)

        let command = action.kubectlCommand(contextName: "prod west", namespace: "payments")

        XCTAssertEqual(command, "kubectl --context 'prod west' --namespace payments scale deployment 'api service' --replicas 3")
        XCTAssertEqual(
            PendingWriteAction.scale(deploymentName: "api;service", replicas: 3)
                .kubectlCommand(contextName: "dev", namespace: "default"),
            "kubectl --context dev --namespace default scale deployment 'api;service' --replicas 3"
        )
        XCTAssertEqual(
            PendingWriteAction.scaleStatefulSet(name: "ledger store", replicas: 2)
                .kubectlCommand(contextName: "dev", namespace: "data"),
            "kubectl --context dev --namespace data scale statefulset 'ledger store' --replicas 2"
        )
        XCTAssertEqual(
            PendingWriteAction.rolloutRestartStatefulSet(name: "ledger store")
                .kubectlCommand(contextName: "dev", namespace: "data"),
            "kubectl --context dev --namespace data rollout restart statefulset 'ledger store'"
        )

        XCTAssertEqual(
            PendingWriteAction.rolloutUndo(deploymentName: "api", revision: nil)
                .kubectlCommand(contextName: "dev", namespace: "default"),
            "kubectl --context dev --namespace default rollout undo deployment api"
        )
        XCTAssertEqual(
            PendingWriteAction.rolloutUndo(deploymentName: "api", revision: 7)
                .kubectlCommand(contextName: "dev", namespace: "default"),
            "kubectl --context dev --namespace default rollout undo deployment api --to-revision=7"
        )
        XCTAssertEqual(
            PendingWriteAction.controllerRolloutUndo(kind: .statefulSet, name: "postgres data", revision: 4)
                .kubectlCommand(contextName: "dev", namespace: "storage"),
            "kubectl --context dev --namespace storage rollout undo statefulset 'postgres data' --to-revision=4"
        )
        XCTAssertEqual(
            PendingWriteAction.controllerRolloutUndo(kind: .daemonSet, name: "node-agent", revision: nil)
                .kubectlCommand(contextName: "dev", namespace: "kube-system"),
            "kubectl --context dev --namespace kube-system rollout undo daemonset node-agent"
        )
        XCTAssertEqual(
            PendingWriteAction.helmRollback(
                releaseName: "api service",
                namespace: "payments",
                revision: 4,
                wait: true,
                timeout: "10m",
                cleanupOnFail: true
            )
            .kubectlCommand(contextName: "prod west", namespace: "ignored"),
            "helm --kube-context 'prod west' --namespace payments rollback 'api service' 4 --wait --timeout 10m --cleanup-on-fail"
        )
    }

    @MainActor
    func testRolloutRollbackConfirmationShowsPlanAndDryRunStatusWhenSafetySettingsAreEnabled() {
        let previousPlan = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        UserDefaults.standard.runeWriteSafetyShowRollbackPlan = true
        UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun = true
        defer {
            restoreUserDefaultsValue(previousPlan, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "payments"
        state.setSelectedDeployment(
            DeploymentSummary(
                name: "api",
                namespace: "payments",
                readyReplicas: 2,
                desiredReplicas: 3,
                selector: ["app": "api", "tier": "web"]
            )
        )
        state.setDeploymentRolloutHistory(
            """
            REVISION\tREPLICASET\tCHANGE-CAUSE
            1\tapi-5f4d\tbootstrap
            2\tapi-6d7c\trollout
            """
        )
        viewModel.rolloutRevisionInput = "1"

        viewModel.requestRolloutUndoSelectedDeployment()

        let message = viewModel.pendingWriteActionMessage
        XCTAssertTrue(message.contains("Rollback plan:"))
        XCTAssertTrue(message.contains("Target resource: deployment/api"))
        XCTAssertTrue(message.contains("Namespace: payments"))
        XCTAssertTrue(message.contains("Current revision: 2"))
        XCTAssertTrue(message.contains("Target revision: 1"))
        XCTAssertTrue(message.contains("Affected selector/pods: app=api, tier=web"))
        XCTAssertTrue(message.contains("kubectl --context demo --namespace payments rollout undo deployment api --to-revision=1"))
        XCTAssertTrue(message.contains("Server dry-run:"))
    }

    @MainActor
    func testDisabledRollbackPlanAndDryRunSettingsUseLowerFrictionRollbackConfirmation() {
        let previousPlan = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        UserDefaults.standard.runeWriteSafetyShowRollbackPlan = false
        UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun = false
        defer {
            restoreUserDefaultsValue(previousPlan, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "payments"
        state.setSelectedDeployment(
            DeploymentSummary(name: "api", namespace: "payments", readyReplicas: 2, desiredReplicas: 3)
        )

        viewModel.requestRolloutUndoSelectedDeployment()

        XCTAssertFalse(viewModel.pendingWriteActionMessage.contains("Rollback plan:"))
        XCTAssertFalse(viewModel.pendingWriteActionMessage.contains("Server dry-run:"))
        XCTAssertNil(viewModel.pendingRollbackPlan)
        XCTAssertNil(viewModel.pendingWriteDryRunStatus)
    }

    @MainActor
    func testControllerRollbackConfirmationShowsPlanAndBlocksAutomaticExecution() async throws {
        let previousPlan = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        UserDefaults.standard.runeWriteSafetyShowRollbackPlan = true
        UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun = true
        defer {
            restoreUserDefaultsValue(previousPlan, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun)
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "storage"
        state.selectedWorkloadKind = .statefulSet
        state.setSelectedStatefulSet(
            ClusterResourceSummary(
                kind: .statefulSet,
                name: "postgres",
                namespace: "storage",
                primaryText: "2/3 ready",
                secondaryText: "app=postgres"
            )
        )
        viewModel.rolloutRevisionInput = "4"

        viewModel.requestRolloutUndoSelectedController()

        let message = viewModel.pendingWriteActionMessage
        XCTAssertTrue(message.contains("Rollout rollback command preview only"))
        XCTAssertTrue(message.contains("Rollback plan:"))
        XCTAssertTrue(message.contains("Target resource: statefulset/postgres"))
        XCTAssertTrue(message.contains("Namespace: storage"))
        XCTAssertTrue(message.contains("Target revision: 4"))
        XCTAssertTrue(message.contains("kubectl --context demo --namespace storage rollout undo statefulset postgres --to-revision=4"))
        XCTAssertTrue(message.contains("StatefulSet rollback dry-run is not available"))

        viewModel.confirmPendingWriteAction()

        XCTAssertNil(viewModel.pendingWriteAction)
        try await waitUntilForRuneAppState {
            !state.writeAuditLog.isEmpty
        }
        XCTAssertEqual(state.writeAuditLog.first?.action, "Controller Rollout Undo")
        XCTAssertEqual(state.writeAuditLog.first?.resource, "statefulset/postgres revision=4")
        XCTAssertEqual(state.writeAuditLog.first?.status, "Blocked")
        XCTAssertTrue(state.writeAuditLog.first?.message.contains("did not run this rollback automatically") == true)
    }

    @MainActor
    func testHelmRollbackRunsDryRunThenRollbackAfterConfirmation() async throws {
        let previousPlan = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        UserDefaults.standard.runeWriteSafetyShowRollbackPlan = true
        UserDefaults.standard.runeWriteSafetyRequireHelmDryRun = true
        defer {
            restoreUserDefaultsValue(previousPlan, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        }

        let state = RuneAppState()
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try Data("apiVersion: v1\n".utf8).write(to: kubeconfig)
        state.setSources([KubeConfigSource(url: kubeconfig)])
        let helmRunner = RecordingHelmCommandRunner()
        let viewModel = RuneAppViewModel(state: state, helmCommandRunner: helmRunner)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "payments"
        state.setSelectedHelmRelease(
            HelmReleaseSummary(
                name: "api",
                namespace: "payments",
                revision: 3,
                updated: "2026-05-05 10:00:00",
                status: "deployed",
                chart: "api-1.2.0",
                appVersion: "1.2.0"
            )
        )

        viewModel.requestHelmRollback(revision: 2)

        try await waitUntilForRuneAppState {
            viewModel.pendingWriteDryRunStatus == "Helm accepted rollback dry-run."
        }

        let message = viewModel.pendingWriteActionMessage
        XCTAssertTrue(message.contains("Rune will run Helm rollback after confirmation"))
        XCTAssertTrue(message.contains("Rollback plan:"))
        XCTAssertTrue(message.contains("Target release: payments/api"))
        XCTAssertTrue(message.contains("Current revision: 3"))
        XCTAssertTrue(message.contains("Target revision: 2"))
        XCTAssertTrue(message.contains("helm --kube-context demo --namespace payments rollback api 2 --wait --timeout 5m --cleanup-on-fail"))
        XCTAssertTrue(message.contains("Helm accepted rollback dry-run"))
        XCTAssertEqual(helmRunner.requests.map(\.dryRun), [true])

        viewModel.confirmPendingWriteAction()

        XCTAssertNil(viewModel.pendingWriteAction)
        try await waitUntilForRuneAppState {
            state.writeAuditLog.contains { $0.action == "Helm Rollback" && $0.status == "Succeeded" }
        }
        XCTAssertEqual(helmRunner.requests.map(\.dryRun), [true, true, false])
        XCTAssertEqual(state.writeAuditLog.first?.action, "Helm Rollback")
        XCTAssertEqual(state.writeAuditLog.first?.status, "Succeeded")
        XCTAssertTrue(state.writeAuditLog.first?.message.contains("Helm rollback completed after Helm dry-run") == true)
    }

    @MainActor
    func testHelmRollbackExecutionUsesArmedScopeAfterActiveScopeChanges() async throws {
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        let previousProductionConfirmation = UserDefaults.standard.object(
            forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation
        )
        UserDefaults.standard.runeWriteSafetyRequireHelmDryRun = false
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = false
        defer {
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
            restoreUserDefaultsValue(previousProductionConfirmation, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        }

        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let armedKubeconfig = directory.appendingPathComponent("armed.yaml")
        let activeKubeconfig = directory.appendingPathComponent("active.yaml")
        try Data("apiVersion: v1\n".utf8).write(to: armedKubeconfig)
        try Data("apiVersion: v1\n".utf8).write(to: activeKubeconfig)

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: armedKubeconfig)])
        state.selectedContext = KubeContext(name: "synthetic-armed")
        state.selectedNamespace = "payments-a"
        state.setSelectedHelmRelease(HelmReleaseSummary(
            name: "api",
            namespace: "payments-a",
            revision: 3,
            updated: "2026-05-05 10:00:00",
            status: "deployed",
            chart: "api-1.2.0",
            appVersion: "1.2.0"
        ))
        let helmRunner = RecordingHelmCommandRunner()
        let viewModel = RuneAppViewModel(state: state, helmCommandRunner: helmRunner)

        viewModel.requestHelmRollback(revision: 2)

        state.setSources([KubeConfigSource(url: activeKubeconfig)])
        state.selectedContext = KubeContext(name: "synthetic-active")
        state.selectedNamespace = "payments-b"
        viewModel.confirmPendingWriteAction()

        try await waitUntilForRuneAppState {
            state.writeAuditLog.contains { $0.action == "Helm Rollback" && $0.status == "Succeeded" }
        }

        let request = try XCTUnwrap(helmRunner.requests.first)
        XCTAssertEqual(helmRunner.requests.count, 1)
        XCTAssertEqual(request.sources, [KubeConfigSource(url: armedKubeconfig)])
        XCTAssertEqual(request.contextName, "synthetic-armed")
        XCTAssertEqual(request.namespace, "payments-a")
        XCTAssertEqual(state.writeAuditLog.first?.contextName, "synthetic-armed")
        XCTAssertEqual(state.writeAuditLog.first?.namespace, "payments-a")
        XCTAssertEqual(state.selectedContext?.name, "synthetic-active")
        XCTAssertEqual(state.selectedNamespace, "payments-b")
    }

    @MainActor
    func testHelmRollbackUsesEditableOptionsForPreviewAndExecution() async throws {
        let previousPlan = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
        let previousDryRun = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        UserDefaults.standard.runeWriteSafetyShowRollbackPlan = true
        UserDefaults.standard.runeWriteSafetyRequireHelmDryRun = true
        defer {
            restoreUserDefaultsValue(previousPlan, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan)
            restoreUserDefaultsValue(previousDryRun, forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun)
        }

        let state = RuneAppState()
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try Data("apiVersion: v1\n".utf8).write(to: kubeconfig)
        state.setSources([KubeConfigSource(url: kubeconfig)])
        let helmRunner = RecordingHelmCommandRunner()
        let viewModel = RuneAppViewModel(state: state, helmCommandRunner: helmRunner)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "payments"
        state.setSelectedHelmRelease(
            HelmReleaseSummary(
                name: "api service",
                namespace: "payments",
                revision: 5,
                updated: "2026-05-05 10:00:00",
                status: "deployed",
                chart: "api-1.2.0",
                appVersion: "1.2.0"
            )
        )
        viewModel.helmRollbackWait = false
        viewModel.helmRollbackTimeoutInput = " 10m "
        viewModel.helmRollbackCleanupOnFail = false

        viewModel.requestHelmRollback(revision: 4)

        try await waitUntilForRuneAppState {
            viewModel.pendingWriteDryRunStatus == "Helm accepted rollback dry-run."
        }

        XCTAssertEqual(
            viewModel.pendingWriteAction,
            .helmRollback(
                releaseName: "api service",
                namespace: "payments",
                revision: 4,
                wait: false,
                timeout: "10m",
                cleanupOnFail: false
            )
        )
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("not wait for Kubernetes resources"))
        XCTAssertTrue(viewModel.pendingWriteActionMessage.contains("cleanup-on-fail disabled"))
        XCTAssertEqual(
            viewModel.pendingWriteActionKubectlCommand,
            "helm --kube-context demo --namespace payments rollback 'api service' 4 --timeout 10m"
        )
        XCTAssertEqual(helmRunner.requests.map(\.wait), [false])
        XCTAssertEqual(helmRunner.requests.map(\.timeout), ["10m"])
        XCTAssertEqual(helmRunner.requests.map(\.cleanupOnFail), [false])

        viewModel.confirmPendingWriteAction()

        try await waitUntilForRuneAppState {
            state.writeAuditLog.contains { $0.action == "Helm Rollback" && $0.status == "Succeeded" }
        }
        XCTAssertEqual(helmRunner.requests.map(\.dryRun), [true, true, false])
        XCTAssertEqual(helmRunner.requests.map(\.wait), [false, false, false])
        XCTAssertEqual(helmRunner.requests.map(\.timeout), ["10m", "10m", "10m"])
        XCTAssertEqual(helmRunner.requests.map(\.cleanupOnFail), [false, false, false])
    }

    @MainActor
    func testWriteAuditLogCapsEntriesAndKeepsNewestFirst() {
        let state = RuneAppState()

        for index in 0..<205 {
            state.appendWriteAuditEntry(
                WriteAuditEntry(
                    action: "Apply YAML",
                    contextName: "demo",
                    namespace: "default",
                    resource: "configmap/settings-\(index)",
                    status: "Succeeded",
                    message: "ok"
                )
            )
        }

        XCTAssertEqual(state.writeAuditLog.count, 200)
        XCTAssertEqual(state.writeAuditLog.first?.resource, "configmap/settings-204")
        XCTAssertEqual(state.writeAuditLog.last?.resource, "configmap/settings-5")
    }

    @MainActor
    func testWriteAuditSearchAndExportUsesVisibleEntries() throws {
        let state = RuneAppState()
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: "Apply YAML",
                contextName: "demo",
                namespace: "default",
                resource: "configmap/settings",
                status: "Succeeded",
                message: "Write action completed"
            )
        )
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: "Delete",
                contextName: "demo",
                namespace: "default",
                resource: "pod/api-0",
                status: "Failed",
                message: "forbidden"
            )
        )

        viewModel.writeAuditSearchQuery = "failed pod/api-0"

        XCTAssertEqual(viewModel.visibleWriteAuditEntries.map(\.resource), ["pod/api-0"])

        viewModel.saveVisibleWriteAuditLog()

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertEqual(exporter.saves.first?.allowedFileTypes, ["json"])
        let data = try XCTUnwrap(exporter.saves.first?.data)
        let entries = try JSONDecoder().decode([WriteAuditEntry].self, from: data)
        XCTAssertEqual(entries.map(\.resource), ["pod/api-0"])
    }

    @MainActor
    func testWriteAuditConfiguredExportUsesVisibleEntriesAndTextWorkflow() throws {
        let state = RuneAppState()
        let configuredExporter = RecordingConfiguredExporter()
        let viewModel = RuneAppViewModel(state: state, configuredExporter: configuredExporter)
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: "Apply YAML",
                contextName: "demo",
                namespace: "default",
                resource: "configmap/settings",
                status: "Succeeded",
                message: "Write action completed"
            )
        )
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: "Delete",
                contextName: "demo",
                namespace: "default",
                resource: "pod/api-0",
                status: "Failed",
                message: "forbidden"
            )
        )

        viewModel.writeAuditSearchQuery = "failed pod/api-0"
        viewModel.saveVisibleWriteAuditLogToExportFolder(openAfterSave: true)

        XCTAssertEqual(configuredExporter.saves.count, 1)
        XCTAssertTrue(configuredExporter.saves[0].suggestedName.hasPrefix("write-audit-"))
        XCTAssertEqual(configuredExporter.saves[0].allowedFileTypes, ["json"])
        XCTAssertEqual(configuredExporter.saves[0].kind, .plainText)
        XCTAssertTrue(configuredExporter.saves[0].openAfterSave)
        let entries = try JSONDecoder().decode([WriteAuditEntry].self, from: configuredExporter.saves[0].data)
        XCTAssertEqual(entries.map(\.resource), ["pod/api-0"])
    }

    @MainActor
    func testVisibleLogZipExportUsesDisplayedText() throws {
        let state = RuneAppState()
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")

        viewModel.saveVisibleLogsZip(visibleText: "matched line\n")

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertEqual(exporter.saves.first?.allowedFileTypes, ["zip"])
        XCTAssertTrue(exporter.saves.first?.suggestedName.contains("visible-logs") == true)
        let entries = try ZipArchiveTestSupport.entries(from: try XCTUnwrap(exporter.saves.first?.data))
        XCTAssertTrue(entries.keys.contains { $0.hasSuffix(".log") })
        XCTAssertTrue(entries.values.contains { String(data: $0, encoding: .utf8) == "matched line\n" })
    }

    @MainActor
    func testCommandPaletteWorkspaceCommandAliasesPublishTypedRequests() throws {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let cases: [(queries: [String], id: String, command: WorkspaceCommand)] = [
            ([":logs", ":log", ":l", "logs"], "cmd:workspace:open-logs", .openLogs),
            ([":save-view", ":sv", "save-view"], "cmd:workspace:save-current-detail", .saveCurrentDetail),
            ([":save-folder", ":sf", "save-folder"], "cmd:workspace:save-current-detail-to-export-folder", .saveCurrentDetailToExportFolder),
            ([":save-open", ":save-and-open", ":so", "save-open", "save-and-open"], "cmd:workspace:save-and-open-current-detail", .saveAndOpenCurrentDetail)
        ]

        for entry in cases {
            var resolvedIDs: Set<String> = []
            for query in entry.queries {
                let item = try XCTUnwrap(viewModel.commandPaletteItems(query: query).first)
                resolvedIDs.insert(item.id)
                guard case let .workspaceCommand(command) = item.action else {
                    return XCTFail("Expected workspace command for \(query)")
                }
                XCTAssertEqual(command, entry.command)

                let previousRequestID = viewModel.workspaceCommandRequest?.id
                state.isCommandPalettePresented = true
                viewModel.executeCommandPaletteItem(item)

                XCTAssertEqual(viewModel.workspaceCommandRequest?.command, entry.command)
                XCTAssertNotEqual(viewModel.workspaceCommandRequest?.id, previousRequestID)
                XCTAssertFalse(state.isCommandPalettePresented)
            }
            XCTAssertEqual(resolvedIDs, [entry.id])
        }

        let shortBareAliases = [
            ("l", "cmd:workspace:open-logs"),
            ("sf", "cmd:workspace:save-current-detail-to-export-folder"),
            ("so", "cmd:workspace:save-and-open-current-detail"),
            ("el", "cmd:logs:save-to-export-folder"),
            ("sol", "cmd:logs:save-and-open")
        ]
        for (query, commandID) in shortBareAliases {
            XCTAssertNotEqual(viewModel.commandPaletteItems(query: query).map(\.id), [commandID])
        }
    }

    @MainActor
    func testCommandPaletteLogExportAliasesUseDistinctDestinations() throws {
        let state = RuneAppState()
        let exporter = RecordingFileExporter()
        let configuredExporter = RecordingConfiguredExporter()
        let viewModel = RuneAppViewModel(
            state: state,
            exporter: exporter,
            configuredExporter: configuredExporter
        )
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setPodLogs("ready\n")

        let cases: [(queries: [String], id: String, action: LogExportAction)] = [
            ([":save-logs", ":sl", "save-logs"], "cmd:logs:save-as", .saveAs),
            ([":export-logs", ":el", "export-logs"], "cmd:logs:save-to-export-folder", .saveToExportFolder),
            ([":save-and-open-logs", ":sol", "save-and-open-logs"], "cmd:logs:save-and-open", .saveAndOpen)
        ]
        for entry in cases {
            var resolvedIDs: Set<String> = []
            for query in entry.queries {
                let item = try XCTUnwrap(viewModel.commandPaletteItems(query: query).first)
                resolvedIDs.insert(item.id)
                guard case let .logExport(action) = item.action else {
                    return XCTFail("Expected log export command for \(query)")
                }
                XCTAssertEqual(action, entry.action)
            }
            XCTAssertEqual(resolvedIDs, [entry.id])
        }

        viewModel.executeCommandPaletteQuery(":sl")
        viewModel.executeCommandPaletteQuery(":el")
        viewModel.executeCommandPaletteQuery(":sol")

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertEqual(exporter.saves.first?.allowedFileTypes, ["log", "txt"])
        XCTAssertTrue(exporter.saves.first?.suggestedName.contains("pod-api-0-logs") == true)
        XCTAssertEqual(String(data: try XCTUnwrap(exporter.saves.first?.data), encoding: .utf8), "ready\n")
        XCTAssertEqual(configuredExporter.saves.count, 2)
        XCTAssertEqual(configuredExporter.saves.map { $0.openAfterSave }, [false, true])
        XCTAssertTrue(configuredExporter.saves.allSatisfy { $0.allowedFileTypes == ["log", "txt"] })
    }

    @MainActor
    func testCommandPalettePodNamedLogsIsNotCapturedByLogExportAlias() throws {
        let state = RuneAppState()
        state.selectedNamespace = "default"
        let pod = PodSummary(name: "logs", namespace: "default", status: "Running")
        state.setPods([pod])
        let viewModel = RuneAppViewModel(state: state)

        let item = try XCTUnwrap(viewModel.commandPaletteItems(query: ":po logs").first)

        XCTAssertEqual(item.id, "cmd:pod:default/logs")
        guard case let .pod(resolvedPod) = item.action else {
            return XCTFail("Expected a pod command")
        }
        XCTAssertEqual(resolvedPod, pod)

        viewModel.executeCommandPaletteItem(item)

        XCTAssertEqual(state.selectedPod, pod)
    }

    @MainActor
    func testResourceDescribeExportSavesCurrentDescribeText() throws {
        let state = RuneAppState()
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setResourceDescribe("Name: api-0\nManaged Fields:\n  manager: kube-controller\n")

        viewModel.saveCurrentResourceDescribe()

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertEqual(exporter.saves.first?.allowedFileTypes, ["txt", "log"])
        XCTAssertTrue(exporter.saves.first?.suggestedName.contains("pod-api-0-describe") == true)
        XCTAssertEqual(String(data: try XCTUnwrap(exporter.saves.first?.data), encoding: .utf8), state.resourceDescribe)
    }

    @MainActor
    func testResourceDescribeConfiguredExportUsesTextWorkflow() {
        let state = RuneAppState()
        let configuredExporter = RecordingConfiguredExporter()
        let viewModel = RuneAppViewModel(state: state, configuredExporter: configuredExporter)
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setResourceDescribe("Name: api-0\nManaged Fields:\n  manager: kube-controller\n")

        viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: true)

        XCTAssertEqual(configuredExporter.saves.count, 1)
        XCTAssertEqual(configuredExporter.saves.first?.allowedFileTypes, ["txt", "log"])
        XCTAssertEqual(configuredExporter.saves.first?.kind, .plainText)
        XCTAssertEqual(configuredExporter.saves.first?.openAfterSave, true)
        XCTAssertTrue(configuredExporter.saves.first?.suggestedName.contains("pod-api-0-describe") == true)
        XCTAssertEqual(String(decoding: configuredExporter.saves[0].data, as: UTF8.self), state.resourceDescribe)
    }

    func testPodContainerLogArchiveIncludesDeploymentMergedAndContainerFiles() throws {
        let data = try LogArchiveBuilder.buildPodContainerZip(
            records: [
                PodLogArchiveRecord(podName: "api-0", containerName: "app", logs: "ready\nserved request"),
                PodLogArchiveRecord(podName: "api-0", containerName: "sidecar", logs: "proxy ready"),
                PodLogArchiveRecord(podName: "api-1", containerName: nil, logs: "single container")
            ],
            baseName: "deployment-api-pod-logs",
            generatedAt: "20260506T100000Z"
        )

        XCTAssertEqual(Array(data.prefix(2)), [0x50, 0x4b])
        XCTAssertGreaterThan(data.count, 0)
        let entries = try ZipArchiveTestSupport.entries(from: data)
        XCTAssertEqual(
            Set(entries.keys),
            [
                "deployment-api-pod-logs/merged-20260506T100000Z.log",
                "deployment-api-pod-logs/pods/api-0/app-20260506T100000Z.log",
                "deployment-api-pod-logs/pods/api-0/sidecar-20260506T100000Z.log",
                "deployment-api-pod-logs/pods/api-1/api-1-20260506T100000Z.log"
            ]
        )
        XCTAssertEqual(
            String(decoding: try XCTUnwrap(entries["deployment-api-pod-logs/merged-20260506T100000Z.log"]), as: UTF8.self),
            "[api-0/app] ready\n[api-0/app] served request\n[api-0/sidecar] proxy ready\n[api-1] single container"
        )
        XCTAssertEqual(
            String(decoding: try XCTUnwrap(entries["deployment-api-pod-logs/pods/api-0/app-20260506T100000Z.log"]), as: UTF8.self),
            "ready\nserved request"
        )
        XCTAssertEqual(
            String(decoding: try XCTUnwrap(entries["deployment-api-pod-logs/pods/api-0/sidecar-20260506T100000Z.log"]), as: UTF8.self),
            "proxy ready"
        )
    }

    @MainActor
    func testFullPodLogArchivePreservesSuccessfulContainerWhenAnotherContainerFails() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.partialLogArchive.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("synthetic-kubeconfig.yaml")
        try server.kubeconfigYAML().write(to: kubeconfig, atomically: true, encoding: .utf8)

        let podName = "orbit-lens-6f58d7d89b-hx9q2"
        let viewModel = RuneAppViewModel(state: RuneAppState())
        let data = try await viewModel.fullPodLogsZipData(
            pods: [PodSummary(
                name: podName,
                namespace: "alpha-zone",
                status: "Running",
                containerNamesLine: "lens, synthetic-missing"
            )],
            sources: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone",
            baseName: "synthetic-partial-logs",
            generatedAt: "20260718T200000Z",
            previous: false
        )

        let entries = try ZipArchiveTestSupport.entries(from: data)
        let successfulPath = "synthetic-partial-logs/pods/\(podName)/lens-20260718T200000Z.log"
        let warningPath = "synthetic-partial-logs/pods/\(podName)/synthetic-missing-20260718T200000Z.log"
        XCTAssertTrue(String(decoding: try XCTUnwrap(entries[successfulPath]), as: UTF8.self).contains("synthetic REST fake log"))
        XCTAssertTrue(
            String(decoding: try XCTUnwrap(entries[warningPath]), as: UTF8.self)
                .contains("⚠ Logs unavailable for container synthetic-missing:")
        )
    }

    func testLogArchiveIncludesMetadataWithoutSecretFields() throws {
        let metadata = LogArchiveMetadata(
            context: "demo",
            namespace: "default",
            workloadKind: "deployment",
            workloadName: "api",
            selectedPods: ["api-0", "api-1"],
            timeWindow: "recent-200-lines",
            previous: true,
            tail: false,
            exportedAt: "2026-05-07T10:00:00Z",
            scope: "full"
        )

        let data = try LogArchiveBuilder.buildZip(
            mergedText: "[api-0] ready\n[api-1] served",
            podNames: ["api-0", "api-1"],
            baseName: "deployment-api-full-logs",
            generatedAt: "20260507T100000Z",
            metadata: metadata
        )

        let entries = try ZipArchiveTestSupport.entries(from: data)
        let metadataData = try XCTUnwrap(entries["deployment-api-full-logs/metadata-20260507T100000Z.json"])
        let decodedMetadata = try JSONDecoder().decode(LogArchiveMetadata.self, from: metadataData)
        let metadataText = String(decoding: metadataData, as: UTF8.self)
        XCTAssertEqual(decodedMetadata, metadata)
        XCTAssertEqual(
            String(decoding: try XCTUnwrap(entries["deployment-api-full-logs/merged-20260507T100000Z.log"]), as: UTF8.self),
            "[api-0] ready\n[api-1] served"
        )
        XCTAssertFalse(metadataText.localizedCaseInsensitiveContains("token"))
        XCTAssertFalse(metadataText.localizedCaseInsensitiveContains("bearer"))
        XCTAssertFalse(metadataText.localizedCaseInsensitiveContains("kubeconfig"))
        XCTAssertFalse(metadataText.contains("/Users/"))
    }

    func testLogArchiveSplitsMergedLogsByKnownPodWithoutUnknownLines() throws {
        let data = try LogArchiveBuilder.buildZip(
            mergedText: "[api-0] ready\n[api-1] served\n[api-0]\n[unknown] ignored\nplain line",
            podNames: ["api-0", "api-1", "api-2"],
            baseName: "deployment-api-full-logs",
            generatedAt: "20260507T100000Z"
        )

        let entries = try ZipArchiveTestSupport.entries(from: data)
        XCTAssertEqual(
            String(decoding: try XCTUnwrap(entries["deployment-api-full-logs/pods/api-0-20260507T100000Z.log"]), as: UTF8.self),
            "ready\n"
        )
        XCTAssertEqual(
            String(decoding: try XCTUnwrap(entries["deployment-api-full-logs/pods/api-1-20260507T100000Z.log"]), as: UTF8.self),
            "served"
        )
        XCTAssertEqual(
            String(decoding: try XCTUnwrap(entries["deployment-api-full-logs/pods/api-2-20260507T100000Z.log"]), as: UTF8.self),
            ""
        )
    }

    func testSinglePodLogArchiveUsesMergedTextWhenLogsHaveNoPodPrefix() throws {
        let data = try LogArchiveBuilder.buildZip(
            mergedText: "ready\nserved request",
            podNames: ["api-0"],
            baseName: "pod-api-0-full-logs",
            generatedAt: "20260507T100000Z"
        )

        let entries = try ZipArchiveTestSupport.entries(from: data)
        XCTAssertEqual(
            String(decoding: try XCTUnwrap(entries["pod-api-0-full-logs/pods/api-0-20260507T100000Z.log"]), as: UTF8.self),
            "ready\nserved request"
        )
    }

    func testPodContainerLogArchiveIncludesMetadataForSelectedPods() throws {
        let metadata = LogArchiveMetadata(
            context: "demo",
            namespace: "default",
            workloadKind: "pod",
            workloadName: "selected-pods",
            selectedPods: ["api-0"],
            timeWindow: "all",
            previous: false,
            tail: true,
            exportedAt: "2026-05-07T10:01:00Z",
            scope: "selected"
        )

        let data = try LogArchiveBuilder.buildPodContainerZip(
            records: [
                PodLogArchiveRecord(podName: "api-0", containerName: "app", logs: "ready")
            ],
            baseName: "selected-pod-full-logs",
            generatedAt: "20260507T100100Z",
            metadata: metadata
        )

        let entries = try ZipArchiveTestSupport.entries(from: data)
        let metadataData = try XCTUnwrap(entries["selected-pod-full-logs/metadata-20260507T100100Z.json"])
        let decodedMetadata = try JSONDecoder().decode(LogArchiveMetadata.self, from: metadataData)
        XCTAssertEqual(decodedMetadata, metadata)
        XCTAssertEqual(
            String(decoding: try XCTUnwrap(entries["selected-pod-full-logs/pods/api-0/app-20260507T100100Z.log"]), as: UTF8.self),
            "ready"
        )
    }

    @MainActor
    func testSupportBundleIncludesAuthDoctorAndWriteAudit() throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "demo")
        state.selectedNamespace = "default"
        state.setAuthDoctorChecks([
            RuneHealthCheck(
                id: "namespace-list",
                title: "Namespace list",
                status: .warning,
                message: "Namespace listing is forbidden for /Users/example/.kube/config; manual namespace mode is available."
            )
        ])
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: "Apply YAML",
                contextName: "demo",
                namespace: "default",
                resource: "configmap/settings",
                status: "Succeeded",
                message: "Server dry-run passed."
            )
        )

        let request = SupportBundleRequest.snapshot(
            state: state,
            generatedAt: "2026-05-06T00:00:00Z",
            resourceCounts: ["pods": 0],
            selectedResourceKind: "ConfigMap",
            selectedResourceName: "settings"
        )
        let data = try JSONSupportBundleBuilder().buildBundle(from: request)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: data)

        XCTAssertEqual(decoded.authDoctorChecks.map(\.id), ["namespace-list"])
        XCTAssertEqual(decoded.writeAuditLog.map(\.resource), ["configmap/settings"])
        XCTAssertEqual(decoded.authDoctorChecks.first?.message.contains("<local-path>"), true)
        XCTAssertFalse(String(data: data, encoding: .utf8)?.contains("/Users/") == true)
    }

    @MainActor
    func testSupportBundleIncludesPrivacySafeRequestMetrics() throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context")
        state.selectedNamespace = "default"
        let loadedAt = Date(timeIntervalSince1970: 1_700)
        state.markResourceListsLive(
            [.pods],
            updatedAt: loadedAt,
            message: "Live for synthetic-context / default"
        )
        state.markResourceListsFailed(
            [.services],
            message: "Partial load for synthetic-context / default: services forbidden"
        )

        let request = SupportBundleRequest.snapshot(
            state: state,
            generatedAt: "2026-05-06T00:00:00Z",
            resourceCounts: ["pods": 2],
            selectedResourceKind: "Pod",
            selectedResourceName: "api-0",
            requestMetrics: [
                SupportBundleRequestMetric(
                    sourcePath: "swift-rest",
                    method: "GET",
                    apiPath: "/api/v1/namespaces/<namespace>/pods?continue=<redacted>",
                    statusCode: 200,
                    responseBytes: 512,
                    durationSeconds: 0.012,
                    attempt: 1,
                    outcome: "success",
                    cancellationReason: nil
                )
            ],
            requestMetricsSummary: SupportBundleRequestMetricsSummary(
                requestCount: 4,
                successCount: 3,
                failureCount: 1,
                cancelledCount: 0,
                responseBytes: 2_048,
                totalDurationSeconds: 0.10,
                retainedMetricCount: 1,
                omittedMetricCount: 3
            ),
            requestMetricGroups: [
                SupportBundleRequestMetricGroup(
                    sourcePath: "swift-rest",
                    method: "GET",
                    apiPath: "/api/v1/namespaces/<namespace>/pods?continue=<redacted>",
                    requestCount: 4,
                    successCount: 3,
                    failureCount: 1,
                    cancelledCount: 0,
                    responseBytes: 2_048,
                    totalDurationSeconds: 0.10,
                    maxDurationSeconds: 0.04,
                    latestStatusCode: 503,
                    latestOutcome: "httpError"
                )
            ]
        )
        let data = try JSONSupportBundleBuilder().buildBundle(from: request)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: data)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))

        XCTAssertEqual(decoded.requestMetrics.count, 1)
        XCTAssertEqual(decoded.requestMetrics.first?.sourcePath, "swift-rest")
        XCTAssertEqual(decoded.requestMetrics.first?.apiPath, "/api/v1/namespaces/<namespace>/pods?continue=<redacted>")
        XCTAssertEqual(decoded.requestMetricsSummary?.requestCount, 4)
        XCTAssertEqual(decoded.requestMetricsSummary?.retainedMetricCount, 1)
        XCTAssertEqual(decoded.requestMetricsSummary?.omittedMetricCount, 3)
        XCTAssertEqual(decoded.requestMetricGroups.count, 1)
        XCTAssertEqual(decoded.requestMetricGroups.first?.requestCount, 4)
        XCTAssertEqual(decoded.requestMetricGroups.first?.latestOutcome, "httpError")
        XCTAssertEqual(decoded.resourceListFreshness.map(\.family), ["pods", "services"])
        XCTAssertEqual(decoded.resourceListFreshness.map(\.status), ["live", "failed"])
        XCTAssertEqual(decoded.resourceListFreshness.first?.updatedAt, loadedAt)
        XCTAssertEqual(decoded.resourceListFreshness.first?.message, "Live for <context-name> namespace <namespace>")
        XCTAssertEqual(decoded.resourceListFreshness.last?.message, "Partial load for <context-name> namespace <namespace>: services forbidden")
        XCTAssertFalse(json.contains("synthetic-namespace"))
        XCTAssertFalse(json.contains("synthetic-context"))
    }

    @MainActor
    func testSupportBundleRedactsClusterScopedRequestMetricNamesWithoutChangingResourceShape() throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context")
        state.selectedNamespace = "synthetic-namespace"
        let paths = [
            "/api/v1/nodes",
            "/api/v1/nodes/synthetic-private-node",
            "/api/v1/nodes/synthetic-private-node/status",
            "/api/v1/nodes/synthetic-private-node/proxy/synthetic-private-route/health",
            "/api/v1/watch/nodes/synthetic-private-node",
            "/api/v1/pods?synthetic-private-bare-value&limit=10&synthetic-private-key=synthetic-private-value",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/synthetic-private-role",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/synthetic-private-role/status",
            "/apis/rbac.authorization.k8s.io/v1/watch/clusterroles/synthetic-private-role"
        ]
        let expectedPaths = [
            "/api/v1/nodes",
            "/api/v1/nodes/<name>",
            "/api/v1/nodes/<name>/status",
            "/api/v1/nodes/<name>/proxy/<path>",
            "/api/v1/watch/nodes/<name>",
            "/api/v1/pods?<redacted>&limit=<redacted>&<redacted>",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/<name>",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/<name>/status",
            "/apis/rbac.authorization.k8s.io/v1/watch/clusterroles/<name>"
        ]
        let requestMetrics = paths.map { path in
            SupportBundleRequestMetric(
                sourcePath: "swift-rest",
                method: "GET",
                apiPath: path,
                statusCode: 200,
                responseBytes: 128,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: "success",
                cancellationReason: nil
            )
        }
        let requestMetricGroups = paths.map { path in
            SupportBundleRequestMetricGroup(
                sourcePath: "swift-rest",
                method: "GET",
                apiPath: path,
                requestCount: 1,
                successCount: 1,
                failureCount: 0,
                cancelledCount: 0,
                responseBytes: 128,
                totalDurationSeconds: 0.01,
                maxDurationSeconds: 0.01,
                latestStatusCode: 200,
                latestOutcome: "success"
            )
        }

        let request = SupportBundleRequest.snapshot(
            state: state,
            generatedAt: "2026-07-22T00:00:00Z",
            resourceCounts: ["nodes": 1, "clusterRoles": 1],
            selectedResourceKind: nil,
            selectedResourceName: nil,
            requestMetrics: requestMetrics,
            requestMetricGroups: requestMetricGroups
        )
        let data = try JSONSupportBundleBuilder().buildBundle(from: request)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: data)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))

        XCTAssertEqual(decoded.requestMetrics.map(\.apiPath), expectedPaths)
        XCTAssertEqual(decoded.requestMetricGroups.map(\.apiPath), expectedPaths)
        XCTAssertFalse(json.contains("synthetic-private-node"))
        XCTAssertFalse(json.contains("synthetic-private-route"))
        XCTAssertFalse(json.contains("synthetic-private-role"))
        XCTAssertFalse(json.contains("synthetic-private-bare-value"))
        XCTAssertFalse(json.contains("synthetic-private-key"))
        XCTAssertFalse(json.contains("synthetic-private-value"))
    }

    func testSupportBundleDecodesOlderSnapshotsWithoutOptionalDiagnostics() throws {
        let json = """
        {
          "generatedAt": "2026-05-06T00:00:00Z",
          "namespace": "<namespace>",
          "sectionTitle": "Workloads",
          "readOnlyMode": false,
          "resourceCounts": {"pods": 2},
          "resourceYAML": "",
          "resourceDescribe": "",
          "podLogs": "",
          "unifiedLogs": "",
          "unifiedLogPods": [],
          "deploymentRolloutHistory": "",
          "recentEvents": [],
          "portForwardSessions": [],
          "lastExecResult": null,
          "authDoctorChecks": [],
          "writeAuditLog": []
        }
        """

        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: Data(json.utf8))

        XCTAssertTrue(decoded.requestMetrics.isEmpty)
        XCTAssertNil(decoded.requestMetricsSummary)
        XCTAssertTrue(decoded.requestMetricGroups.isEmpty)
        XCTAssertTrue(decoded.resourceListFreshness.isEmpty)
    }

    func testSnapshotRefreshConcurrencyLimiterBoundsConcurrentOperations() async throws {
        let limiter = SnapshotRefreshConcurrencyLimiter(limit: 3)
        let tracker = SnapshotRefreshConcurrencyTracker()

        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<12 {
                group.addTask {
                    try? await limiter.withPermit {
                        await tracker.started()
                        try await Task.sleep(nanoseconds: 15_000_000)
                        await tracker.finished()
                    }
                }
            }
        }

        let maxActive = await tracker.maxActive
        let completed = await tracker.completed
        XCTAssertLessThanOrEqual(maxActive, 3)
        XCTAssertEqual(completed, 12)
    }

    func testSnapshotRefreshConcurrencyLimiterCancelsWaitingOperations() async throws {
        let limiter = SnapshotRefreshConcurrencyLimiter(limit: 1)
        let tracker = SnapshotRefreshConcurrencyTracker()

        let first = Task {
            try await limiter.withPermit {
                await tracker.started()
                try await Task.sleep(nanoseconds: 40_000_000)
                await tracker.finished()
            }
        }

        for _ in 0..<100 where await tracker.maxActive == 0 {
            try await Task.sleep(nanoseconds: 2_000_000)
        }

        let waiting = Task {
            try await limiter.withPermit {
                await tracker.started()
                await tracker.finished()
            }
        }
        waiting.cancel()

        do {
            try await waiting.value
            XCTFail("Expected cancelled waiter to throw before running its operation.")
        } catch is CancellationError {
        } catch {
            XCTFail("Expected CancellationError, got \(error).")
        }

        try await first.value

        let completed = await tracker.completed
        XCTAssertEqual(completed, 1)
    }

    @MainActor
    func testSaveSupportBundleIncludesKubernetesRequestMetrics() async throws {
        let contextName = "synthetic-context"
        let fixture = try makeSyntheticMetricsKubeconfig(contextName: contextName)
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: fixture.kubeconfig)])
        state.selectedContext = KubeContext(name: contextName)
        state.selectedNamespace = "default"
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
        let scopeIdentity = try await restClient.requestMetricsScopeIdentity(
            environment: ["KUBECONFIG": fixture.kubeconfig.path],
            contextName: contextName
        )
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic-namespace/pods?continue=synthetic-token",
                statusCode: 200,
                responseBytes: 256,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .success
            ),
            contextName: contextName,
            scopeIdentity: scopeIdentity
        )
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic-namespace/pods?continue=retry-token",
                statusCode: 503,
                responseBytes: 64,
                durationSeconds: 0.02,
                attempt: 1,
                outcome: .httpError
            ),
            contextName: contextName,
            scopeIdentity: scopeIdentity
        )
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic-other/secrets?synthetic-private-key=synthetic-private-value",
                statusCode: nil,
                responseBytes: 0,
                durationSeconds: 0.03,
                attempt: 1,
                outcome: .networkError
            ),
            contextName: "synthetic-other-context"
        )
        let client = KubernetesClient(restClient: restClient, requestMetricsRecorder: recorder)
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(state: state, kubeClient: client, exporter: exporter)

        viewModel.saveSupportBundle()
        try await waitUntilForRuneAppState {
            exporter.saves.count == 1
        }

        let data = try XCTUnwrap(exporter.saves.first?.data)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: data)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))

        XCTAssertEqual(decoded.requestMetrics.count, 2)
        XCTAssertEqual(decoded.requestMetrics.map(\.apiPath), [
            "/api/v1/namespaces/<namespace>/pods?continue=<redacted>",
            "/api/v1/namespaces/<namespace>/pods?continue=<redacted>"
        ])
        XCTAssertEqual(decoded.requestMetrics.map(\.outcome), ["success", "httpError"])
        XCTAssertEqual(decoded.requestMetricsSummary?.requestCount, 2)
        XCTAssertEqual(decoded.requestMetricsSummary?.successCount, 1)
        XCTAssertEqual(decoded.requestMetricsSummary?.failureCount, 1)
        XCTAssertEqual(decoded.requestMetricsSummary?.retainedMetricCount, 2)
        XCTAssertEqual(decoded.requestMetricsSummary?.omittedMetricCount, 0)
        XCTAssertTrue(json.contains("\"omittedMetricCount\""))
        XCTAssertEqual(decoded.requestMetricsSummary?.retainedMetricCount, decoded.requestMetrics.count)
        XCTAssertEqual(decoded.requestMetricGroups.count, 1)
        XCTAssertEqual(decoded.requestMetricGroups.first?.apiPath, "/api/v1/namespaces/<namespace>/pods?continue=<redacted>")
        XCTAssertEqual(decoded.requestMetricGroups.first?.requestCount, 2)
        XCTAssertEqual(decoded.requestMetricGroups.first?.failureCount, 1)
        XCTAssertEqual(decoded.requestMetricGroups.first?.latestOutcome, "httpError")
        XCTAssertFalse(json.contains("synthetic-namespace"))
        XCTAssertFalse(json.contains("synthetic-token"))
        XCTAssertFalse(json.contains("retry-token"))
        XCTAssertFalse(json.contains("/secrets"))
        XCTAssertFalse(json.contains("synthetic-other-context"))
        XCTAssertFalse(json.contains("synthetic-private-key"))
        XCTAssertFalse(json.contains("synthetic-private-value"))
    }

    @MainActor
    func testRequestMetricsSummaryRefreshesFromKubeClientRecorder() async throws {
        let contextName = "synthetic-metrics-context"
        let fixture = try makeSyntheticMetricsKubeconfig(contextName: contextName)
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
        let scopeIdentity = try await restClient.requestMetricsScopeIdentity(
            environment: ["KUBECONFIG": fixture.kubeconfig.path],
            contextName: contextName
        )
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic-namespace/pods",
                statusCode: 200,
                responseBytes: 1_024,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .success
            ),
            contextName: contextName,
            scopeIdentity: scopeIdentity
        )
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic-namespace/services",
                statusCode: 504,
                responseBytes: 128,
                durationSeconds: 0.20,
                attempt: 2,
                outcome: .httpError
            ),
            contextName: contextName,
            scopeIdentity: scopeIdentity
        )
        let client = KubernetesClient(restClient: restClient, requestMetricsRecorder: recorder)
        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: fixture.kubeconfig)])
        state.selectedContext = KubeContext(name: contextName)
        let viewModel = RuneAppViewModel(state: state, kubeClient: client)

        viewModel.refreshKubernetesRequestMetricsSummary()

        try await waitUntilForRuneAppState {
            !viewModel.isRefreshingKubernetesRequestMetricsSummary
                && viewModel.kubernetesRequestMetricsSummary.requestCountText == "2 API attempts"
        }
        XCTAssertEqual(viewModel.kubernetesRequestMetricsSummary.outcomeText, "1 ok • 1 failed • 0 cancelled")
        XCTAssertEqual(viewModel.kubernetesRequestMetricsSummary.transferText, "1.1 KB • 210 ms total")
        XCTAssertEqual(viewModel.kubernetesRequestMetricsSummary.retainedText, "2 retained")
        XCTAssertTrue(viewModel.kubernetesRequestMetricsSummary.hasFailures)
        XCTAssertEqual(viewModel.kubernetesRequestMetricsSummary.endpointHighlights.map(\.apiPath), [
            "/api/v1/namespaces/<namespace>/services",
            "/api/v1/namespaces/<namespace>/pods"
        ])
        XCTAssertEqual(viewModel.kubernetesRequestMetricsSummary.endpointHighlights.map(\.hasIssues), [true, false])
    }

    @MainActor
    func testMetricsUIAndSupportBundleRejectPreviousScopeAfterSamePathKubeconfigReplacement() async throws {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let firstServer = try await RuneFakeK8sRESTServer.start()
        let secondServer = try await RuneFakeK8sRESTServer.start()
        defer {
            firstServer.stop()
            secondServer.stop()
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneAppStateTests.metricsScope.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try firstServer.kubeconfigYAML().write(
            to: kubeconfig,
            atomically: true,
            encoding: .utf8
        )

        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
        let client = KubernetesClient(
            commandTimeout: 2,
            restClient: restClient,
            requestMetricsRecorder: recorder
        )
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        _ = try await client.listPodStatuses(
            from: sources,
            context: context,
            namespace: "alpha-zone"
        )

        let state = RuneAppState()
        state.setSources(sources)
        state.selectedContext = context
        state.selectedNamespace = "alpha-zone"
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: client,
            exporter: exporter
        )

        viewModel.refreshKubernetesRequestMetricsSummary()
        try await waitUntilForRuneAppState {
            !viewModel.isRefreshingKubernetesRequestMetricsSummary
                && viewModel.kubernetesRequestMetricsSummary.requestCountText == "1 API attempt"
        }

        try secondServer.kubeconfigYAML().write(
            to: kubeconfig,
            atomically: true,
            encoding: .utf8
        )
        try FileManager.default.setAttributes(
            [.modificationDate: Date(timeIntervalSinceNow: 2)],
            ofItemAtPath: kubeconfig.path
        )

        viewModel.refreshKubernetesRequestMetricsSummary()
        try await waitUntilForRuneAppState {
            !viewModel.isRefreshingKubernetesRequestMetricsSummary
                && viewModel.kubernetesRequestMetricsSummary == .empty
        }

        viewModel.saveSupportBundle()
        try await waitUntilForRuneAppState {
            exporter.saves.count == 1
        }
        let data = try XCTUnwrap(exporter.saves.first?.data)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: data)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))
        XCTAssertTrue(decoded.requestMetrics.isEmpty)
        XCTAssertTrue(decoded.requestMetricGroups.isEmpty)
        XCTAssertEqual(decoded.requestMetricsSummary?.requestCount, 0)
        XCTAssertEqual(decoded.contextName, "<context-name>")
        XCTAssertFalse(json.contains(context.name))
    }

    @MainActor
    func testAuthDoctorDoesNotReuseNameOnlyMetricsWhenSelectedContextHasNoSource() async throws {
        let selectedContextName = "synthetic-selected-context"
        let recorder = KubernetesRESTRequestMetricsRecorder()
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/pods",
                statusCode: 200,
                responseBytes: 256,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .success
            ),
            contextName: selectedContextName
        )
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/secrets",
                statusCode: 503,
                responseBytes: 64,
                durationSeconds: 0.02,
                attempt: 1,
                outcome: .httpError
            ),
            contextName: "synthetic-other-context"
        )
        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
        let client = KubernetesClient(restClient: restClient, requestMetricsRecorder: recorder)
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: selectedContextName)
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeClient: client,
            exporter: exporter
        )

        viewModel.runAuthDoctor()

        try await waitUntilForRuneAppState {
            !state.isRunningAuthDoctor
                && !viewModel.isRefreshingKubernetesRequestMetricsSummary
        }
        XCTAssertEqual(viewModel.kubernetesRequestMetricsSummary, .empty)
        XCTAssertTrue(state.authDoctorChecks.contains { $0.id == "kubeconfig" && $0.status == .failed })

        viewModel.saveSupportBundle()
        try await waitUntilForRuneAppState {
            exporter.saves.count == 1
        }
        let data = try XCTUnwrap(exporter.saves.first?.data)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: data)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))
        XCTAssertTrue(decoded.requestMetrics.isEmpty)
        XCTAssertEqual(decoded.requestMetricsSummary?.requestCount, 0)
        XCTAssertFalse(json.contains(selectedContextName))
    }

    @MainActor
    func testMetricsUIAndSupportBundleStayEmptyWithoutSelectedContext() async throws {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic-private/secrets?synthetic-key=synthetic-value",
                statusCode: 503,
                responseBytes: 64,
                durationSeconds: 0.02,
                attempt: 1,
                outcome: .httpError
            ),
            contextName: "synthetic-unselected-context"
        )
        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
        let client = KubernetesClient(restClient: restClient, requestMetricsRecorder: recorder)
        let state = RuneAppState()
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(state: state, kubeClient: client, exporter: exporter)

        viewModel.refreshKubernetesRequestMetricsSummary()
        try await waitUntilForRuneAppState {
            !viewModel.isRefreshingKubernetesRequestMetricsSummary
        }

        XCTAssertEqual(viewModel.kubernetesRequestMetricsSummary, .empty)

        viewModel.saveSupportBundle()
        try await waitUntilForRuneAppState {
            exporter.saves.count == 1
        }

        let data = try XCTUnwrap(exporter.saves.first?.data)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: data)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))
        XCTAssertTrue(decoded.requestMetrics.isEmpty)
        XCTAssertTrue(decoded.requestMetricGroups.isEmpty)
        XCTAssertEqual(decoded.requestMetricsSummary?.requestCount, 0)
        XCTAssertEqual(decoded.requestMetricsSummary?.retainedMetricCount, 0)
        XCTAssertEqual(decoded.requestMetricsSummary?.omittedMetricCount, 0)
        XCTAssertFalse(json.contains("/secrets"))
        XCTAssertFalse(json.contains("synthetic-unselected-context"))
        XCTAssertFalse(json.contains("synthetic-key"))
        XCTAssertFalse(json.contains("synthetic-value"))
    }

    @MainActor
    func testSaveSupportBundleConfiguredExportUsesTextWorkflow() async throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context")
        state.selectedNamespace = "default"
        let configuredExporter = RecordingConfiguredExporter()
        let viewModel = RuneAppViewModel(state: state, configuredExporter: configuredExporter)

        viewModel.saveSupportBundleToExportFolder(openAfterSave: true)
        try await waitUntilForRuneAppState {
            configuredExporter.saves.count == 1
        }

        XCTAssertTrue(configuredExporter.saves[0].suggestedName.hasPrefix("support-bundle-"))
        XCTAssertEqual(configuredExporter.saves[0].allowedFileTypes, ["json"])
        XCTAssertEqual(configuredExporter.saves[0].kind, .plainText)
        XCTAssertTrue(configuredExporter.saves[0].openAfterSave)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: configuredExporter.saves[0].data)
        XCTAssertEqual(decoded.contextName, "<context-name>")
        XCTAssertEqual(decoded.namespace, "default")
    }

    @MainActor
    func testSupportBundleRedactsSensitiveValuesLocalPathsAndContextName() throws {
        let defaults = UserDefaults.standard
        let previousExportFolderDisplayName = defaults.object(forKey: RuneSettingsKeys.exportFolderDisplayName)
        let previousTextOpener = defaults.object(forKey: RuneSettingsKeys.exportTextOpenerBundleIdentifier)
        let previousArchiveOpener = defaults.object(forKey: RuneSettingsKeys.exportArchiveOpenerBundleIdentifier)
        let previousPrivacySafeFilenames = defaults.object(forKey: RuneSettingsKeys.exportUsesPrivacySafeFilenames)
        defer {
            restoreUserDefaultsValue(previousExportFolderDisplayName, forKey: RuneSettingsKeys.exportFolderDisplayName)
            restoreUserDefaultsValue(previousTextOpener, forKey: RuneSettingsKeys.exportTextOpenerBundleIdentifier)
            restoreUserDefaultsValue(previousArchiveOpener, forKey: RuneSettingsKeys.exportArchiveOpenerBundleIdentifier)
            restoreUserDefaultsValue(previousPrivacySafeFilenames, forKey: RuneSettingsKeys.exportUsesPrivacySafeFilenames)
        }
        defaults.runeExportFolderDisplayName = "Synthetic Export Folder"
        defaults.runeExportTextOpenerBundleIdentifier = "com.example.SyntheticTextViewer"
        defaults.runeExportArchiveOpenerBundleIdentifier = "com.example.SyntheticArchiveViewer"
        defaults.runeExportUsesPrivacySafeFilenames = true

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-production-context")
        state.selectedNamespace = "synthetic-namespace"
        state.setResourceYAML("""
        apiVersion: v1
        kind: Secret
        metadata:
          name: app-secret
        data:
          token: synthetic-token-value
          client-key: /synthetic/home/user/client.key
        """)
        state.setResourceDescribe("Loaded synthetic-production-context from /synthetic/home/user/.kube/config")
        state.setPodLogs("Authorization: Bearer synthetic-bearer-value\npassword: synthetic-password-value")
        state.setUnifiedServiceLogs("context synthetic-production-context id-token: synthetic-id-token-value", pods: ["api-0"])
        state.setDeploymentRolloutHistory("client-certificate-data: synthetic-cert-value")
        state.setEvents([
            EventSummary(
                type: "Warning",
                reason: "Failed",
                objectName: "api-0",
                message: "Read /synthetic/home/user/manifest.yaml for synthetic-production-context"
            )
        ])
        state.setPortForwardSessions([
            PortForwardSession(
                id: "pf-1",
                contextName: "synthetic-production-context",
                namespace: "synthetic-namespace",
                targetKind: .pod,
                targetName: "api-0",
                localPort: 8080,
                remotePort: 80,
                address: "127.0.0.1",
                status: .failed,
                lastMessage: "socket path /synthetic/home/user/pf.sock"
            )
        ])
        state.setLastExecResult(
            PodExecResult(
                podName: "api-0",
                namespace: "synthetic-namespace",
                command: ["cat", "/synthetic/home/user/.kube/config"],
                stdout: "refresh-token: synthetic-refresh-token-value",
                stderr: "client-key-data: synthetic-key-data-value",
                exitCode: 1
            )
        )
        state.setAuthDoctorChecks([
            RuneHealthCheck(
                id: "kubeconfig",
                title: "Kubeconfig",
                status: .warning,
                message: "synthetic-production-context uses token synthetic-health-token-value from /synthetic/home/user/.kube/config"
            )
        ])
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: "Apply YAML",
                contextName: "synthetic-production-context",
                namespace: "synthetic-namespace",
                resource: "secret/app-secret",
                status: "Failed",
                message: "token: synthetic-audit-token-value"
            )
        )

        let request = SupportBundleRequest.snapshot(
            state: state,
            generatedAt: "2026-05-06T00:00:00Z",
            resourceCounts: ["pods": 1],
            selectedResourceKind: "Secret",
            selectedResourceName: "app-secret"
        )
        let data = try JSONSupportBundleBuilder().buildBundle(from: request)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))

        XCTAssertFalse(json.contains("synthetic-production-context"))
        XCTAssertFalse(json.contains("/synthetic/home/user"))
        XCTAssertFalse(json.contains("synthetic-token-value"))
        XCTAssertFalse(json.contains("synthetic-bearer-value"))
        XCTAssertFalse(json.contains("synthetic-password-value"))
        XCTAssertFalse(json.contains("synthetic-id-token-value"))
        XCTAssertFalse(json.contains("synthetic-cert-value"))
        XCTAssertFalse(json.contains("synthetic-refresh-token-value"))
        XCTAssertFalse(json.contains("synthetic-key-data-value"))
        XCTAssertFalse(json.contains("synthetic-health-token-value"))
        XCTAssertFalse(json.contains("synthetic-audit-token-value"))
        XCTAssertFalse(json.contains("Synthetic Export Folder"))
        XCTAssertFalse(json.contains("com.example.SyntheticTextViewer"))
        XCTAssertFalse(json.contains("com.example.SyntheticArchiveViewer"))
        XCTAssertTrue(json.contains("<context-name>"))
        XCTAssertTrue(json.contains("<local-path>"))
        XCTAssertTrue(json.contains("<redacted>"))
    }

    @MainActor
    func testAuthDoctorKubeconfigInspectorDetectsManagedAuthWithoutExportingArguments() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        clusters:
        - name: demo
          cluster:
            server: https://example.invalid
            certificate-authority-data: REDACTED
            proxy-url: http://proxy.invalid
        users:
        - name: demo
          user:
            exec:
              command: kubelogin
              args:
              - get-token
              - --environment
              - AzurePublicCloud
        contexts:
        - name: demo
          context:
            cluster: demo
            user: demo
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let checks = AuthDoctorKubeconfigInspector(
            fileExists: { path in
                path.hasSuffix("/az") || path.hasSuffix("/kubelogin")
            },
            executableSearchPaths: ["/synthetic/bin"]
        ).inspect(sources: [KubeConfigSource(url: kubeconfig)])
        let messages = checks.map(\.message).joined(separator: " ")

        XCTAssertTrue(messages.contains("AKS/kubelogin auth hints detected."))
        XCTAssertTrue(messages.contains("All kubeconfig exec auth commands were found on PATH."))
        XCTAssertTrue(messages.contains("Detected cloud provider CLI tools are available on PATH."))
        XCTAssertTrue(messages.contains("Proxy configuration was detected."))
        XCTAssertTrue(messages.contains("Custom certificate authority configuration was detected."))
        XCTAssertFalse(messages.contains("get-token"))
        XCTAssertFalse(messages.contains(kubeconfig.path))
    }

    @MainActor
    func testAuthDoctorKubeconfigInspectorAddsProviderLifecycleChecksWithoutLeakingValues() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        clusters:
        - name: synthetic-eks
          cluster:
            server: https://synthetic.eks.amazonaws.com
        - name: synthetic-gke
          cluster:
            server: https://container.googleapis.com
        - name: synthetic-aks
          cluster:
            server: https://synthetic-aks.example.invalid
        - name: synthetic-oidc
          cluster:
            server: https://synthetic-oidc.example.invalid
        users:
        - name: synthetic-eks
          user:
            exec:
              command: aws
              args:
              - eks
              - get-token
              - --role-arn
              - arn:aws:iam::000000000000:role/synthetic-private-role
        - name: synthetic-gke
          user:
            exec:
              command: gke-gcloud-auth-plugin
        - name: synthetic-aks
          user:
            exec:
              command: kubelogin
        - name: synthetic-oidc
          user:
            auth-provider:
              name: oidc
              config:
                id-token: synthetic-id-token-value
                expiry: "2099-01-01T00:00:00Z"
        contexts: []
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let checks = AuthDoctorKubeconfigInspector(
            fileExists: { path in
                path.hasSuffix("/aws")
                    || path.hasSuffix("/gke-gcloud-auth-plugin")
                    || path.hasSuffix("/kubelogin")
            },
            executableSearchPaths: ["/synthetic/bin"]
        ).inspect(sources: [KubeConfigSource(url: kubeconfig)])
        let messages = checks.map(\.message).joined(separator: " ")

        XCTAssertEqual(checks.first { $0.id == "eks-role-profile" }?.status, .passed)
        XCTAssertEqual(checks.first { $0.id == "gke-auth-plugin-profile" }?.status, .passed)
        XCTAssertEqual(checks.first { $0.id == "aks-kubelogin-profile" }?.status, .passed)
        XCTAssertEqual(checks.first { $0.id == "oidc-token-profile" }?.status, .passed)
        XCTAssertFalse(messages.contains("synthetic-private-role"))
        XCTAssertFalse(messages.contains("synthetic-id-token-value"))
        XCTAssertFalse(messages.contains("2099-01-01"))
        XCTAssertFalse(messages.contains(kubeconfig.path))
    }

    @MainActor
    func testAuthDoctorKubeconfigInspectorWarnsForExpiredOIDCProfile() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        users:
        - name: synthetic-oidc
          user:
            auth-provider:
              name: oidc
              config:
                id-token: synthetic-id-token-value
                expiry: "2000-01-01T00:00:00Z"
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let checks = AuthDoctorKubeconfigInspector(
            fileExists: { _ in false },
            executableSearchPaths: ["/synthetic/bin"]
        ).inspect(sources: [KubeConfigSource(url: kubeconfig)])
        let oidc = try XCTUnwrap(checks.first { $0.id == "oidc-token-profile" })

        XCTAssertEqual(oidc.status, .warning)
        XCTAssertTrue(oidc.message.contains("appears expired"))
        XCTAssertFalse(oidc.message.contains("synthetic-id-token-value"))
        XCTAssertFalse(oidc.message.contains("2000-01-01"))
    }

    @MainActor
    func testAuthDoctorKubeconfigInspectorUsesMacFallbackSearchPathsForCloudTools() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        users:
        - name: demo
          user:
            exec:
              command: kubelogin
        clusters:
        - name: demo
          cluster:
            server: https://example.invalid
        contexts:
        - name: demo
          context:
            cluster: demo
            user: demo
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let checks = AuthDoctorKubeconfigInspector(
            fileExists: { path in
                path == "/opt/homebrew/bin/az" || path == "/opt/homebrew/bin/kubelogin"
            },
            executableSearchPaths: RuneExecutableSearchPath.directories(from: ["PATH": "/usr/bin"])
        ).inspect(sources: [KubeConfigSource(url: kubeconfig)])
        let cloudTools = try XCTUnwrap(checks.first { $0.id == "cloud-login-tools" })
        let execTools = try XCTUnwrap(checks.first { $0.id == "exec-auth-tools" })

        XCTAssertEqual(cloudTools.status, .passed)
        XCTAssertEqual(execTools.status, .passed)
    }

    @MainActor
    func testAuthDoctorKubeconfigInspectorWarnsForMissingCloudAuthToolsWithoutLeakingArgs() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        clusters:
        - name: demo
          cluster:
            server: https://demo.eks.amazonaws.com
        users:
        - name: demo
          user:
            exec:
              command: aws
              args:
              - eks
              - get-token
              - --cluster-name
              - synthetic-cluster
        contexts:
        - name: demo
          context:
            cluster: demo
            user: demo
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let checks = AuthDoctorKubeconfigInspector(
            fileExists: { _ in false },
            executableSearchPaths: ["/synthetic/bin"]
        ).inspect(sources: [KubeConfigSource(url: kubeconfig)])
        let execTools = try XCTUnwrap(checks.first { $0.id == "exec-auth-tools" })
        let cloudTools = try XCTUnwrap(checks.first { $0.id == "cloud-login-tools" })

        XCTAssertEqual(execTools.status, .warning)
        XCTAssertEqual(cloudTools.status, .warning)
        XCTAssertTrue(execTools.message.contains("aws"))
        XCTAssertTrue(cloudTools.message.contains("aws"))
        XCTAssertFalse(execTools.message.contains("synthetic-cluster"))
        XCTAssertFalse(cloudTools.message.contains("synthetic-cluster"))
        XCTAssertFalse(execTools.message.contains(kubeconfig.path))
        XCTAssertFalse(cloudTools.message.contains(kubeconfig.path))
    }

    @MainActor
    func testAuthDoctorKubeconfigInspectorExplainsDisabledExternalExecAuth() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        clusters:
        - name: demo
          cluster:
            server: https://demo.eks.amazonaws.com
        users:
        - name: demo
          user:
            exec:
              command: aws
              args:
              - eks
              - get-token
              - --cluster-name
              - synthetic-cluster
        contexts:
        - name: demo
          context:
            cluster: demo
            user: demo
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let checks = AuthDoctorKubeconfigInspector(
            fileExists: { _ in true },
            executableSearchPaths: ["/synthetic/bin"],
            externalCommandsAllowed: { false }
        ).inspect(sources: [KubeConfigSource(url: kubeconfig)])
        let execProfile = try XCTUnwrap(checks.first { $0.id == "exec-auth-profile" })
        let execTools = try XCTUnwrap(checks.first { $0.id == "exec-auth-tools" })
        let cloudTools = try XCTUnwrap(checks.first { $0.id == "cloud-login-tools" })
        let rendered = checks.map(\.message).joined(separator: "\n")

        XCTAssertEqual(execProfile.status, .warning)
        XCTAssertEqual(execTools.status, .warning)
        XCTAssertEqual(cloudTools.status, .warning)
        XCTAssertTrue(execProfile.message.contains("CLI-backed auth"))
        XCTAssertTrue(execTools.message.contains("cannot run external auth plugins"))
        XCTAssertTrue(cloudTools.message.contains("cannot run external provider commands"))
        XCTAssertFalse(rendered.contains("synthetic-cluster"))
        XCTAssertFalse(rendered.contains(kubeconfig.path))
    }

    @MainActor
    func testAuthDoctorKubeconfigInspectorDetectsAdditionalProviderTooling() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        clusters:
        - name: synthetic-doks
          cluster:
            server: https://doks.example.invalid
        - name: synthetic-rancher
          cluster:
            server: https://rancher.example.invalid
        - name: synthetic-openshift
          cluster:
            server: https://openshift.example.invalid
        users:
        - name: synthetic-doks
          user:
            exec:
              command: doctl
        - name: synthetic-rancher
          user:
            exec:
              command: rancher
        - name: synthetic-openshift
          user:
            exec:
              command: oc
        contexts:
        - name: synthetic-doks
          context:
            cluster: synthetic-doks
            user: synthetic-doks
        - name: synthetic-rancher
          context:
            cluster: synthetic-rancher
            user: synthetic-rancher
        - name: synthetic-openshift
          context:
            cluster: synthetic-openshift
            user: synthetic-openshift
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)

        let checks = AuthDoctorKubeconfigInspector(
            fileExists: { _ in false },
            executableSearchPaths: ["/synthetic/bin"]
        ).inspect(sources: [KubeConfigSource(url: kubeconfig)])
        let messages = checks.map(\.message).joined(separator: " ")
        let cloudTools = try XCTUnwrap(checks.first { $0.id == "cloud-login-tools" })
        let doksProfile = try XCTUnwrap(checks.first { $0.id == "doks-doctl-profile" })
        let rancherProfile = try XCTUnwrap(checks.first { $0.id == "rancher-cli-profile" })
        let openShiftProfile = try XCTUnwrap(checks.first { $0.id == "openshift-cli-profile" })

        XCTAssertEqual(cloudTools.status, .warning)
        XCTAssertEqual(doksProfile.status, .warning)
        XCTAssertEqual(rancherProfile.status, .warning)
        XCTAssertEqual(openShiftProfile.status, .warning)
        XCTAssertTrue(messages.contains("DOKS auth hints detected."))
        XCTAssertTrue(messages.contains("Rancher auth hints detected."))
        XCTAssertTrue(messages.contains("OpenShift auth hints detected."))
        XCTAssertTrue(cloudTools.message.contains("doctl"))
        XCTAssertTrue(cloudTools.message.contains("rancher"))
        XCTAssertTrue(cloudTools.message.contains("oc"))
        XCTAssertTrue(doksProfile.message.contains("doctl"))
        XCTAssertTrue(rancherProfile.message.contains("rancher"))
        XCTAssertTrue(openShiftProfile.message.contains("oc"))
        XCTAssertFalse(messages.contains(kubeconfig.path))
    }

    @MainActor
    func testAuthDoctorOutputCanBeClearedWhenIdle() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.setAuthDoctorChecks([
            RuneHealthCheck(id: "namespace-list", title: "Namespace list", status: .warning, message: "Synthetic warning")
        ])

        viewModel.clearAuthDoctorOutput()

        XCTAssertTrue(state.authDoctorChecks.isEmpty)
    }

    @MainActor
    func testRBACCanISimulatorRunsReadOnlyNamespaceScopedRequest() async throws {
        let state = RuneAppState()
        state.setContexts([KubeContext(name: "synthetic-context")])
        state.selectedNamespace = "payments"
        state.setSources([KubeConfigSource(url: URL(fileURLWithPath: "/tmp/rune-synthetic-kubeconfig"))])
        let checker = RecordingRBACCanIChecker(result: true)
        let viewModel = RuneAppViewModel(state: state, rbacCanICheck: checker.check)
        viewModel.rbacCanIVerb = " create "
        viewModel.rbacCanIResource = " pods "
        viewModel.rbacCanIApiGroup = " "
        viewModel.rbacCanISubresource = " exec "
        viewModel.rbacCanIScope = .namespace

        viewModel.runRBACCanISimulator()

        try await waitUntilForRuneAppState {
            viewModel.rbacCanIResult != nil && !viewModel.isRunningRBACCanI
        }
        let request = try XCTUnwrap(checker.requests.first)
        XCTAssertEqual(request.contextName, "synthetic-context")
        XCTAssertEqual(request.namespace, "payments")
        XCTAssertEqual(request.verb, "create")
        XCTAssertEqual(request.resource, "pods")
        XCTAssertNil(request.apiGroup)
        XCTAssertEqual(request.subresource, "exec")
        XCTAssertEqual(viewModel.rbacCanIResult?.allowed, true)
        XCTAssertTrue(state.writeAuditLog.isEmpty)
        XCTAssertNil(viewModel.pendingWriteAction)
    }

    @MainActor
    func testRBACCanISimulatorUsesSelectedClusterScopedResourceDefaults() async throws {
        let state = RuneAppState()
        state.setContexts([KubeContext(name: "synthetic-context")])
        state.selectedNamespace = "payments"
        state.setSources([KubeConfigSource(url: URL(fileURLWithPath: "/tmp/rune-synthetic-kubeconfig"))])
        let clusterRole = ClusterResourceSummary(
            kind: .clusterRole,
            name: "operator-view",
            namespace: nil,
            primaryText: "Rules: 4",
            secondaryText: "ClusterRole"
        )
        state.setRBACData(roles: [], serviceAccounts: [], roleBindings: [], clusterRoles: [clusterRole], clusterRoleBindings: [])
        let checker = RecordingRBACCanIChecker(result: false)
        let viewModel = RuneAppViewModel(state: state, rbacCanICheck: checker.check)

        viewModel.selectRBACResource(clusterRole)

        XCTAssertEqual(viewModel.rbacCanIVerb, "list")
        XCTAssertEqual(viewModel.rbacCanIResource, "clusterroles")
        XCTAssertEqual(viewModel.rbacCanIApiGroup, "rbac.authorization.k8s.io")
        XCTAssertEqual(viewModel.rbacCanIScope, .cluster)

        viewModel.runRBACCanISimulator()

        try await waitUntilForRuneAppState {
            viewModel.rbacCanIResult != nil && !viewModel.isRunningRBACCanI
        }
        let request = try XCTUnwrap(checker.requests.first)
        XCTAssertNil(request.namespace)
        XCTAssertEqual(request.verb, "list")
        XCTAssertEqual(request.resource, "clusterroles")
        XCTAssertEqual(request.apiGroup, "rbac.authorization.k8s.io")
        XCTAssertEqual(viewModel.rbacCanIResult?.allowed, false)
        XCTAssertTrue(state.writeAuditLog.isEmpty)
        XCTAssertNil(viewModel.pendingWriteAction)
    }

    @MainActor
    func testRBACCanIPresetPopulatesSimulatorFields() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)

        viewModel.useRBACCanIPreset(
            verb: " list ",
            resource: " deployments ",
            apiGroup: " apps ",
            subresource: nil,
            scope: .namespace
        )

        XCTAssertEqual(viewModel.rbacCanIVerb, "list")
        XCTAssertEqual(viewModel.rbacCanIResource, "deployments")
        XCTAssertEqual(viewModel.rbacCanIApiGroup, "apps")
        XCTAssertEqual(viewModel.rbacCanISubresource, "")
        XCTAssertEqual(viewModel.rbacCanIScope, .namespace)
        XCTAssertNil(viewModel.rbacCanIResult)
    }

    @MainActor
    func testRBACBindingRelationshipNavigationOpensReferencedRole() throws {
        let state = RuneAppState()
        state.selectedNamespace = "payments"
        let binding = ClusterResourceSummary(
            kind: .roleBinding,
            name: "api-readers",
            namespace: "payments",
            primaryText: "→ ClusterRole/view",
            secondaryText: "1 subject(s)"
        )
        let clusterRole = ClusterResourceSummary(
            kind: .clusterRole,
            name: "view",
            namespace: nil,
            primaryText: "6 rules",
            secondaryText: "Cluster role"
        )
        state.setRBACData(roles: [], serviceAccounts: [], roleBindings: [binding], clusterRoles: [clusterRole], clusterRoleBindings: [])
        state.setSelectedRBACResource(binding)
        state.selectedSection = .rbac
        state.selectedWorkloadKind = .roleBinding
        let viewModel = RuneAppViewModel(state: state)

        let referencedRole = try XCTUnwrap(viewModel.selectedRBACBindingReferencedRole)
        XCTAssertEqual(referencedRole.kind, .clusterRole)
        XCTAssertEqual(referencedRole.name, "view")

        viewModel.openRBACBindingReferencedRole(referencedRole)

        XCTAssertEqual(state.selectedSection, .rbac)
        XCTAssertEqual(state.selectedWorkloadKind, .clusterRole)
        XCTAssertEqual(state.selectedRBACResource?.name, "view")
    }

    @MainActor
    func testRBACRoleRelationshipNavigationOpensRelatedBinding() {
        let state = RuneAppState()
        state.selectedNamespace = "payments"
        let role = ClusterResourceSummary(
            kind: .role,
            name: "api-reader",
            namespace: "payments",
            primaryText: "2 rules",
            secondaryText: "Namespaced role"
        )
        let binding = ClusterResourceSummary(
            kind: .roleBinding,
            name: "api-reader-binding",
            namespace: "payments",
            primaryText: "→ Role/api-reader",
            secondaryText: "1 subject(s)"
        )
        let otherNamespaceBinding = ClusterResourceSummary(
            kind: .roleBinding,
            name: "api-reader-other",
            namespace: "other",
            primaryText: "→ Role/api-reader",
            secondaryText: "1 subject(s)"
        )
        state.setRBACData(
            roles: [role],
            serviceAccounts: [],
            roleBindings: [binding, otherNamespaceBinding],
            clusterRoles: [],
            clusterRoleBindings: []
        )
        state.setSelectedRBACResource(role)
        state.selectedSection = .rbac
        state.selectedWorkloadKind = .role
        let viewModel = RuneAppViewModel(state: state)

        XCTAssertEqual(viewModel.selectedRBACRoleRelatedBindings.map(\.name), ["api-reader-binding"])

        viewModel.openRBACRoleRelatedBinding(binding)

        XCTAssertEqual(state.selectedSection, .rbac)
        XCTAssertEqual(state.selectedWorkloadKind, .roleBinding)
        XCTAssertEqual(state.selectedRBACResource?.name, "api-reader-binding")
    }

    @MainActor
    func testRBACCanISimulatorReportsValidationErrorsWithoutRunningChecker() {
        let state = RuneAppState()
        state.selectedNamespace = "payments"
        let checker = RecordingRBACCanIChecker(result: true)
        let viewModel = RuneAppViewModel(state: state, rbacCanICheck: checker.check)

        viewModel.runRBACCanISimulator()

        XCTAssertEqual(viewModel.rbacCanIResult?.errorMessage, "Select a Kubernetes context before checking RBAC.")
        XCTAssertTrue(checker.requests.isEmpty)

        state.setContexts([KubeContext(name: "synthetic-context")])
        viewModel.rbacCanIVerb = " "
        viewModel.runRBACCanISimulator()

        XCTAssertEqual(viewModel.rbacCanIResult?.errorMessage, "Enter a verb to check.")
        XCTAssertTrue(checker.requests.isEmpty)

        viewModel.rbacCanIVerb = "list"
        viewModel.rbacCanIResource = " "
        viewModel.runRBACCanISimulator()

        XCTAssertEqual(viewModel.rbacCanIResult?.errorMessage, "Enter a resource to check.")
        XCTAssertTrue(checker.requests.isEmpty)

        viewModel.rbacCanIResource = "pods"
        state.selectedNamespace = " "
        viewModel.rbacCanIScope = .namespace
        viewModel.runRBACCanISimulator()

        XCTAssertEqual(viewModel.rbacCanIResult?.errorMessage, "Select or enter a namespace, or switch scope to Cluster.")
        XCTAssertTrue(checker.requests.isEmpty)
    }

    @MainActor
    func testRBACCanISimulatorReportsCheckerFailure() async throws {
        let state = RuneAppState()
        state.setContexts([KubeContext(name: "synthetic-context")])
        state.selectedNamespace = "payments"
        state.setSources([KubeConfigSource(url: URL(fileURLWithPath: "/tmp/rune-synthetic-kubeconfig"))])
        let checker = RecordingRBACCanIChecker(error: RuneError.invalidInput(message: "Synthetic RBAC failure"))
        let viewModel = RuneAppViewModel(state: state, rbacCanICheck: checker.check)

        viewModel.runRBACCanISimulator()

        try await waitUntilForRuneAppState {
            viewModel.rbacCanIResult != nil && !viewModel.isRunningRBACCanI
        }
        XCTAssertEqual(checker.requests.count, 1)
        XCTAssertEqual(viewModel.rbacCanIResult?.allowed, nil)
        XCTAssertEqual(viewModel.rbacCanIResult?.errorMessage, "Invalid input: Synthetic RBAC failure")
    }

    func testKubeResourceKindProjectsRBACAPIGroupForAccessReviews() {
        XCTAssertEqual(KubeResourceKind.deployment.rbacAPIGroup, "apps")
        XCTAssertEqual(KubeResourceKind.cronJob.rbacAPIGroup, "batch")
        XCTAssertEqual(KubeResourceKind.ingress.rbacAPIGroup, "networking.k8s.io")
        XCTAssertEqual(KubeResourceKind.storageClass.rbacAPIGroup, "storage.k8s.io")
        XCTAssertEqual(KubeResourceKind.clusterRoleBinding.rbacAPIGroup, "rbac.authorization.k8s.io")
        XCTAssertNil(KubeResourceKind.pod.rbacAPIGroup)
        XCTAssertNil(KubeResourceKind.persistentVolume.rbacAPIGroup)
    }
}

@MainActor
private final class RecordingPortForwardBrowserOpener: PortForwardBrowserOpening {
    private(set) var openedURLs: [URL] = []

    func open(_ url: URL) {
        openedURLs.append(url)
    }
}

private struct CancelledFileExporter: FileExporting {
    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        throw FileExportError.userCancelled
    }
}

private func makeSyntheticMetricsKubeconfig(
    contextName: String
) throws -> (directory: URL, kubeconfig: URL) {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("RuneAppStateTests.metricsFixture.\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    let kubeconfig = directory.appendingPathComponent("config.yaml")
    let yaml = """
    apiVersion: v1
    kind: Config
    current-context: \(contextName)
    contexts:
    - name: \(contextName)
      context:
        cluster: synthetic-cluster
        user: synthetic-user
        namespace: default
    clusters:
    - name: synthetic-cluster
      cluster:
        server: https://api.synthetic.invalid
    users:
    - name: synthetic-user
      user:
        token: synthetic-token
    """
    do {
        try yaml.write(to: kubeconfig, atomically: true, encoding: .utf8)
        return (directory, kubeconfig)
    } catch {
        try? FileManager.default.removeItem(at: directory)
        throw error
    }
}

private struct EmptyKubeConfigDiscoverer: KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL] {
        []
    }
}

private final class MutableKubeConfigDiscoverer: KubeConfigDiscovering, @unchecked Sendable {
    var urls: [URL] = []
    private(set) var callCount = 0

    func discoverCandidateFiles() -> [URL] {
        callCount += 1
        return urls
    }
}

@MainActor
private struct FixedKubeConfigPicker: KubeConfigPicking {
    let urls: [URL]
    var folderURL: URL? = nil

    func pickFiles() throws -> [URL] {
        urls
    }

    func pickFolder() throws -> URL? {
        folderURL
    }

    func pickDefaultKubeConfig(at defaultURL: URL) throws -> URL? {
        urls.first
    }
}

private struct MockCloudKubeConfigImporter: CloudKubeConfigImporting {
    let preview: CloudKubeConfigCommandPreview
    let result: Result<CloudKubeConfigImportResult, CloudKubeConfigImportError>

    init(
        preview: CloudKubeConfigCommandPreview = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig"],
            displayCommand: "aws eks update-kubeconfig"
        ),
        result: Result<CloudKubeConfigImportResult, CloudKubeConfigImportError>
    ) {
        self.preview = preview
        self.result = result
    }

    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        preview
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        try result.get()
    }
}

private struct OutputtingCloudKubeConfigImporter: CloudKubeConfigImporting {
    let preview: CloudKubeConfigCommandPreview
    let chunks: [CloudKubeConfigCommandOutput]
    let result: Result<CloudKubeConfigImportResult, CloudKubeConfigImportError>

    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        preview
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        try result.get()
    }

    func importCluster(
        _ request: CloudKubeConfigImportRequest,
        onOutput: @escaping @Sendable (CloudKubeConfigCommandOutput) -> Void
    ) async throws -> CloudKubeConfigImportResult {
        for chunk in chunks {
            onOutput(chunk)
        }
        return try result.get()
    }
}

private final class TargetWritingCloudKubeConfigImporter: CloudKubeConfigImporting, @unchecked Sendable {
    private let lock = NSLock()
    private var storedTargetPaths: [String] = []

    var targetPaths: [String] {
        lock.withLock { storedTargetPaths }
    }

    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        try CloudKubeConfigCommandBuilder().preview(for: request)
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        let target = request.targetKubeconfigPath
        lock.withLock {
            storedTargetPaths.append(target)
        }
        let url = URL(fileURLWithPath: target)
        let raw =
        """
        apiVersion: v1
        kind: Config
        current-context: synthetic-cloud-context
        clusters:
        - name: synthetic-cloud-cluster
          cluster:
            server: https://example.invalid
        contexts:
        - name: synthetic-cloud-context
          context:
            cluster: synthetic-cloud-cluster
        users: []
        """
        try raw.write(to: url, atomically: true, encoding: .utf8)
        let preview = try commandPreview(for: request)
        return CloudKubeConfigImportResult(
            command: preview,
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
            discoveredURLs: [url],
            reviews: [KubeConfigImportValidator().validate(raw: raw, sourceName: url.lastPathComponent)]
        )
    }
}

private final class CountingCloudKubeConfigImporter: CloudKubeConfigImporting, @unchecked Sendable {
    private let lock = NSLock()
    private let preview = CloudKubeConfigCommandPreview(
        executable: "aws",
        arguments: ["eks", "update-kubeconfig"],
        displayCommand: "aws eks update-kubeconfig"
    )
    private var storedPreviewCallCount = 0
    private var storedImportCallCount = 0

    var previewCallCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return storedPreviewCallCount
    }

    var importCallCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return storedImportCallCount
    }

    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        lock.withLock {
            storedPreviewCallCount += 1
        }
        return preview
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        lock.withLock {
            storedImportCallCount += 1
        }
        return CloudKubeConfigImportResult(
            command: preview,
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "", stderr: ""),
            discoveredURLs: [],
            reviews: []
        )
    }
}

private final class SequencedCloudKubeConfigImporter: CloudKubeConfigImporting, @unchecked Sendable {
    private let lock = NSLock()
    private let preview: CloudKubeConfigCommandPreview
    private var results: [Result<CloudKubeConfigImportResult, CloudKubeConfigImportError>]
    private var storedImportCallCount = 0

    var importCallCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return storedImportCallCount
    }

    init(
        preview: CloudKubeConfigCommandPreview,
        results: [Result<CloudKubeConfigImportResult, CloudKubeConfigImportError>]
    ) {
        self.preview = preview
        self.results = results
    }

    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        preview
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        let result = lock.withLock {
            storedImportCallCount += 1
            guard !results.isEmpty else {
                return Result<CloudKubeConfigImportResult, CloudKubeConfigImportError>.failure(.missingRequiredField("Result"))
            }
            return results.removeFirst()
        }
        return try result.get()
    }
}

private final class BlockingCloudKubeConfigImporter: CloudKubeConfigImporting, @unchecked Sendable {
    private let lock = NSLock()
    private let result: CloudKubeConfigImportResult
    private var continuation: CheckedContinuation<CloudKubeConfigImportResult, Error>?
    private var storedImportCallCount = 0

    var importCallCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return storedImportCallCount
    }

    var hasSuspendedImport: Bool {
        lock.lock()
        defer { lock.unlock() }
        return continuation != nil
    }

    init(result: CloudKubeConfigImportResult) {
        self.result = result
    }

    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        result.command
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        lock.withLock {
            storedImportCallCount += 1
        }

        return try await withCheckedThrowingContinuation { continuation in
            lock.withLock {
                self.continuation = continuation
            }
        }
    }

    func resume() {
        lock.lock()
        let continuation = continuation
        self.continuation = nil
        lock.unlock()
        continuation?.resume(returning: result)
    }
}

private final class InMemoryBookmarkStore: BookmarkStore, @unchecked Sendable {
    private(set) var records: [BookmarkRecord] = []

    func loadRecords() throws -> [BookmarkRecord] {
        records
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        self.records = records
    }

    func removeAllRecords() {
        records = []
    }
}

private final class RejectingBookmarkStore: BookmarkStore, @unchecked Sendable {
    private enum Failure: Error {
        case rejected
    }

    func loadRecords() throws -> [BookmarkRecord] {
        []
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        throw Failure.rejected
    }
}

private func restoreUserDefaultsValue(_ value: Any?, forKey key: String) {
    if let value {
        UserDefaults.standard.set(value, forKey: key)
    } else {
        UserDefaults.standard.removeObject(forKey: key)
    }
}

@MainActor
private func cacheSyntheticSnapshot(
    configMaps: [ClusterResourceSummary],
    context: KubeContext,
    namespace: String,
    store: ResourceStore
) {
    store.cacheSnapshot(
        context: context,
        namespace: namespace,
        pods: [],
        deployments: [],
        statefulSets: [],
        daemonSets: [],
        jobs: [],
        cronJobs: [],
        replicaSets: [],
        persistentVolumeClaims: [],
        horizontalPodAutoscalers: [],
        networkPolicies: [],
        services: [],
        endpoints: [],
        ingresses: [],
        configMaps: configMaps,
        secrets: [],
        serviceAccounts: [],
        events: []
    )
}

private func waitUntilForRuneAppState(
    timeout: TimeInterval = 2,
    file: StaticString = #filePath,
    line: UInt = #line,
    predicate: @escaping @MainActor () -> Bool
) async throws {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if await MainActor.run(body: predicate) {
            return
        }
        try await Task.sleep(nanoseconds: 20_000_000)
    }
    XCTFail("Timed out waiting for condition", file: file, line: line)
}

@MainActor
private func confirmPendingKubeConfigImport(
    _ viewModel: RuneAppViewModel,
    file: StaticString = #filePath,
    line: UInt = #line
) async throws {
    try await waitUntilForRuneAppState(file: file, line: line) {
        viewModel.isKubeConfigImportConfirmationPending && viewModel.canConfirmKubeConfigImport
    }
    viewModel.confirmKubeConfigImport()
}

private actor SnapshotRefreshConcurrencyTracker {
    private var activeCount = 0
    private(set) var maxActive = 0
    private(set) var completed = 0

    func started() {
        activeCount += 1
        maxActive = max(maxActive, activeCount)
    }

    func finished() {
        activeCount -= 1
        completed += 1
    }
}

@MainActor
private final class RecordingFileExporter: FileExporting {
    private(set) var saves: [(data: Data, suggestedName: String, allowedFileTypes: [String])] = []

    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        saves.append((data, suggestedName, allowedFileTypes))
        return URL(fileURLWithPath: "/tmp/\(suggestedName)")
    }
}

@MainActor
private final class RecordingConfiguredExporter: ConfiguredExporting {
    private(set) var saves: [
        (
            data: Data,
            suggestedName: String,
            allowedFileTypes: [String],
            kind: ConfiguredExportFileKind,
            openAfterSave: Bool
        )
    ] = []

    func save(
        data: Data,
        suggestedName: String,
        allowedFileTypes: [String],
        kind: ConfiguredExportFileKind,
        openAfterSave: Bool
    ) throws -> URL {
        saves.append((data, suggestedName, allowedFileTypes, kind, openAfterSave))
        return URL(fileURLWithPath: "/tmp/\(suggestedName)")
    }
}

private final class RecordingHelmCommandRunner: HelmCommandRunning, @unchecked Sendable {
    private(set) var requests: [HelmRollbackRequest] = []
    var result = HelmCommandResult(exitCode: 0, stdout: "ok\n", stderr: "")

    func rollback(_ request: HelmRollbackRequest, timeout: TimeInterval) async throws -> HelmCommandResult {
        requests.append(request)
        return result
    }
}

@MainActor
private final class RecordingRBACCanIChecker {
    struct Request: Equatable {
        let sourcesCount: Int
        let contextName: String
        let namespace: String?
        let verb: String
        let resource: String
        let apiGroup: String?
        let subresource: String?
    }

    private let result: Bool
    private let error: Error?
    private(set) var requests: [Request] = []

    init(result: Bool = true, error: Error? = nil) {
        self.result = result
        self.error = error
    }

    func check(
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String?,
        verb: String,
        resource: String,
        apiGroup: String?,
        subresource: String?
    ) async throws -> Bool {
        requests.append(Request(
            sourcesCount: sources.count,
            contextName: context.name,
            namespace: namespace,
            verb: verb,
            resource: resource,
            apiGroup: apiGroup,
            subresource: subresource
        ))
        if let error {
            throw error
        }
        return result
    }
}
