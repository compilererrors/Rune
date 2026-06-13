import XCTest
@testable import RuneCore
@testable import RuneDiagnostics
@testable import RuneExport
@testable import RuneKube
@testable import RuneSecurity
@testable import RuneStore
@testable import RuneUI

final class RuneAppStateTests: XCTestCase {
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
            [first.path, second.path, defaultConfig.path].sorted()
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
            [runeFirst.path, runeSecond.path, defaultConfig.path].sorted()
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

    func testKubeConfigImportValidatorRejectsUnsupportedYAMLAliasAndMergeFeatures() {
        let aliasReview = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"]).validate(
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
            current-context: context-alpha
            clusters:
            - cluster:
                <<: *base
                server: https://example.invalid
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

        XCTAssertFalse(aliasReview.isValid)
        XCTAssertTrue(aliasReview.issues.contains { $0.id == "unsupported-yaml-feature" && $0.severity == .error })
        XCTAssertFalse(mergeReview.isValid)
        XCTAssertTrue(mergeReview.issues.contains { $0.id == "unsupported-yaml-feature" && $0.severity == .error })
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

        try await waitUntilForRuneAppState {
            !bookmarkStore.records.isEmpty
        }

        XCTAssertTrue(state.favoriteContextNames.contains("imported-context"))
        XCTAssertTrue(store.loadFavoriteContextNames().contains("imported-context"))
        XCTAssertEqual(store.loadPreferredNamespace(for: "imported-context"), "imported-namespace")
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

        try await waitUntilForRuneAppState {
            bookmarkStore.records.count == 2
        }

        XCTAssertEqual(Set(viewModel.kubeConfigImportReviews.flatMap { $0.contexts.map(\.name) }), ["folder-config", "folder-extra"])
        XCTAssertTrue(bookmarkStore.records.allSatisfy { $0.path.hasPrefix(appOwnedDirectory.path) })
        XCTAssertFalse(bookmarkStore.records.contains { $0.path.contains("notes") || $0.path.contains("nested") })
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

        let reloaded = FileBackedContextPreferencesStore(url: url)

        XCTAssertEqual(reloaded.loadFavoriteContextNames(), ["alpha", "beta"])
        XCTAssertEqual(reloaded.loadFavoriteResourceIDs(), ["context-alpha|pod|default|api"])
        XCTAssertEqual(reloaded.loadFavoriteNamespaceIDs(), ["context-alpha|namespace|default"])
        XCTAssertEqual(reloaded.loadManualProductionContextIDs(), ["context-alpha"])
        XCTAssertEqual(reloaded.loadManualNamespaces(for: "context-alpha"), ["alpha", "zeta"])
        XCTAssertEqual(reloaded.loadPreferredNamespace(for: "context-alpha"), "zeta")

        let json = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(json.contains("\"schemaVersion\""))
        XCTAssertTrue(json.contains("\(FileBackedContextPreferencesStore.currentSchemaVersion)"))
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

        defaults.runeWriteSafetyRequireApplyDryRun = false
        defaults.runeWriteSafetyRequireProductionSecondConfirmation = false

        XCTAssertFalse(defaults.runeWriteSafetyRequireApplyDryRun)
        XCTAssertFalse(defaults.runeWriteSafetyRequireProductionSecondConfirmation)
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
        let archiveBytes = String(decoding: exporter.saves[0].data, as: UTF8.self)
        XCTAssertTrue(archiveBytes.contains("terminal-transcripts/README.txt"))
        XCTAssertTrue(archiveBytes.contains("terminal-transcripts/session-1-demo-api-0-"))
        XCTAssertTrue(archiveBytes.contains("terminal-transcripts/session-2-demo-worker-0-"))
        XCTAssertTrue(archiveBytes.contains("alpha"))
        XCTAssertTrue(archiveBytes.contains("beta"))
        XCTAssertTrue(archiveBytes.contains("Exit Code: 137"))
    }

    @MainActor
    func testTerminalTranscriptExportSkipsWhenNoTranscriptExists() {
        let exporter = RecordingFileExporter()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
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

        XCTAssertTrue(exporter.saves.isEmpty)
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

        let reloadedState = RuneAppState()
        let reloadedViewModel = RuneAppViewModel(state: reloadedState, contextPreferences: store)
        reloadedState.selectedContext = KubeContext(name: "demo")
        reloadedState.selectedNamespace = ""
        reloadedState.setNamespaces([])

        XCTAssertEqual(reloadedViewModel.namespaceOptions, ["team-a"])
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
            containerNamesLine: "api, metrics , sidecar"
        )

        XCTAssertEqual(pod.containerNames, ["api", "metrics", "sidecar"])
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
    func testSelectedPodsForBulkActionsFollowVisibleOrderingAndFilter() {
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

        XCTAssertEqual(viewModel.selectedPodsForBulkActions.map(\.name), ["api-0", "api-1"])
        XCTAssertEqual(viewModel.selectedPodCount, 3)
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
        XCTAssertTrue(viewModel.pendingWriteActionIsDestructive)
    }

    @MainActor
    func testSelectedGenericResourceComparisonSummarizesVisibleSelection() {
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

        let comparison = viewModel.selectedGenericResourceComparisonText

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
                message: "Applied revision main@sha1"
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
        XCTAssertGreaterThan(try XCTUnwrap(exporter.saves.first?.data.count), 0)
    }

    @MainActor
    func testCommandPalettePodLogsAliasSavesCurrentLogs() throws {
        let state = RuneAppState()
        let exporter = RecordingFileExporter()
        let viewModel = RuneAppViewModel(state: state, exporter: exporter)
        state.selectedContext = KubeContext(name: "fake")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .pod
        state.selectedPod = PodSummary(name: "api-0", namespace: "default", status: "Running")
        state.setPodLogs("ready\n")

        let items = viewModel.commandPaletteItems(query: ":po logs")
        XCTAssertEqual(items.first?.title, "Save Logs")

        viewModel.executeCommandPaletteQuery(":po logs")

        XCTAssertEqual(exporter.saves.count, 1)
        XCTAssertEqual(exporter.saves.first?.allowedFileTypes, ["log", "txt"])
        XCTAssertTrue(exporter.saves.first?.suggestedName.contains("pod-api-0-logs") == true)
        XCTAssertEqual(String(data: try XCTUnwrap(exporter.saves.first?.data), encoding: .utf8), "ready\n")
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
        let archiveText = String(decoding: data, as: UTF8.self)
        XCTAssertTrue(archiveText.contains("deployment-api-pod-logs/merged-20260506T100000Z.log"))
        XCTAssertTrue(archiveText.contains("deployment-api-pod-logs/pods/api-0/app-20260506T100000Z.log"))
        XCTAssertTrue(archiveText.contains("deployment-api-pod-logs/pods/api-0/sidecar-20260506T100000Z.log"))
        XCTAssertTrue(archiveText.contains("deployment-api-pod-logs/pods/api-1/api-1-20260506T100000Z.log"))
        XCTAssertTrue(archiveText.contains("[api-0/app] ready"))
        XCTAssertTrue(archiveText.contains("[api-0/sidecar] proxy ready"))
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

        let archiveText = String(decoding: data, as: UTF8.self)
        XCTAssertTrue(archiveText.contains("deployment-api-full-logs/metadata-20260507T100000Z.json"))
        XCTAssertTrue(archiveText.contains(#""context":"demo""#))
        XCTAssertTrue(archiveText.contains(#""namespace":"default""#))
        XCTAssertTrue(archiveText.contains(#""workloadKind":"deployment""#))
        XCTAssertTrue(archiveText.contains(#""workloadName":"api""#))
        XCTAssertTrue(archiveText.contains(#""selectedPods":["api-0","api-1"]"#))
        XCTAssertTrue(archiveText.contains(#""timeWindow":"recent-200-lines""#))
        XCTAssertTrue(archiveText.contains(#""previous":true"#))
        XCTAssertTrue(archiveText.contains(#""tail":false"#))
        XCTAssertTrue(archiveText.contains(#""scope":"full""#))
        XCTAssertFalse(archiveText.localizedCaseInsensitiveContains("token"))
        XCTAssertFalse(archiveText.localizedCaseInsensitiveContains("bearer"))
        XCTAssertFalse(archiveText.localizedCaseInsensitiveContains("kubeconfig"))
        XCTAssertFalse(archiveText.contains("/Users/"))
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

        let archiveText = String(decoding: data, as: UTF8.self)
        XCTAssertTrue(archiveText.contains("selected-pod-full-logs/metadata-20260507T100100Z.json"))
        XCTAssertTrue(archiveText.contains(#""workloadKind":"pod""#))
        XCTAssertTrue(archiveText.contains(#""selectedPods":["api-0"]"#))
        XCTAssertTrue(archiveText.contains(#""scope":"selected""#))
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
                retainedMetricCount: 1
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
        XCTAssertEqual(decoded.requestMetricGroups.count, 1)
        XCTAssertEqual(decoded.requestMetricGroups.first?.requestCount, 4)
        XCTAssertEqual(decoded.requestMetricGroups.first?.latestOutcome, "httpError")
        XCTAssertFalse(json.contains("synthetic-namespace"))
        XCTAssertFalse(json.contains("synthetic-context"))
    }

    func testSupportBundleDecodesOlderSnapshotsWithoutRequestMetrics() throws {
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
    }

    @MainActor
    func testSaveSupportBundleIncludesKubernetesRequestMetrics() async throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-context")
        state.selectedNamespace = "default"
        let recorder = KubernetesRESTRequestMetricsRecorder()
        await recorder.record(KubernetesRESTRequestMetric(
            method: "GET",
            apiPath: "/api/v1/namespaces/synthetic-namespace/pods?continue=synthetic-token",
            statusCode: 200,
            responseBytes: 256,
            durationSeconds: 0.01,
            attempt: 1,
            outcome: .success
        ))
        await recorder.record(KubernetesRESTRequestMetric(
            method: "GET",
            apiPath: "/api/v1/namespaces/synthetic-namespace/pods?continue=retry-token",
            statusCode: 503,
            responseBytes: 64,
            durationSeconds: 0.02,
            attempt: 1,
            outcome: .httpError
        ))
        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
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
        XCTAssertEqual(decoded.requestMetricGroups.count, 1)
        XCTAssertEqual(decoded.requestMetricGroups.first?.apiPath, "/api/v1/namespaces/<namespace>/pods?continue=<redacted>")
        XCTAssertEqual(decoded.requestMetricGroups.first?.requestCount, 2)
        XCTAssertEqual(decoded.requestMetricGroups.first?.failureCount, 1)
        XCTAssertEqual(decoded.requestMetricGroups.first?.latestOutcome, "httpError")
        XCTAssertFalse(json.contains("synthetic-namespace"))
        XCTAssertFalse(json.contains("synthetic-token"))
        XCTAssertFalse(json.contains("retry-token"))
    }

    @MainActor
    func testSupportBundleRedactsSensitiveValuesLocalPathsAndContextName() throws {
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

        XCTAssertEqual(cloudTools.status, .warning)
        XCTAssertTrue(messages.contains("DOKS auth hints detected."))
        XCTAssertTrue(messages.contains("Rancher auth hints detected."))
        XCTAssertTrue(messages.contains("OpenShift auth hints detected."))
        XCTAssertTrue(cloudTools.message.contains("doctl"))
        XCTAssertTrue(cloudTools.message.contains("rancher"))
        XCTAssertTrue(cloudTools.message.contains("oc"))
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

private struct EmptyKubeConfigDiscoverer: KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL] {
        []
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

private final class InMemoryBookmarkStore: BookmarkStore, @unchecked Sendable {
    private(set) var records: [BookmarkRecord] = []

    func loadRecords() throws -> [BookmarkRecord] {
        records
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        self.records = records
    }
}

private func restoreUserDefaultsValue(_ value: Any?, forKey key: String) {
    if let value {
        UserDefaults.standard.set(value, forKey: key)
    } else {
        UserDefaults.standard.removeObject(forKey: key)
    }
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
private final class RecordingFileExporter: FileExporting {
    private(set) var saves: [(data: Data, suggestedName: String, allowedFileTypes: [String])] = []

    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        saves.append((data, suggestedName, allowedFileTypes))
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
