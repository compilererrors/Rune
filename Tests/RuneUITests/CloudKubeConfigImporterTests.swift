import XCTest
@testable import RuneCore
@testable import RuneKube
@testable import RuneSecurity

final class CloudKubeConfigImporterTests: XCTestCase {
    func testHelmCommandRunnerRejectsMissingKubeconfigBeforeRunningProcess() async throws {
        do {
            _ = try await ProcessHelmCommandRunner().rollback(
                HelmRollbackRequest(
                    sources: [],
                    contextName: "synthetic-context",
                    namespace: "synthetic-namespace",
                    releaseName: "synthetic-release",
                    revision: 1,
                    wait: true,
                    timeout: "5m",
                    cleanupOnFail: true,
                    dryRun: true
                ),
                timeout: 1
            )
            XCTFail("Expected missing kubeconfig error")
        } catch {
            XCTAssertEqual(error as? HelmCommandError, .missingKubeConfig)
        }
    }

    func testCloudImporterBuildsProviderCommandsWithoutStoringCredentials() throws {
        let importer = CloudKubeConfigCLIImporter(
            runner: RecordingCloudCommandRunner(),
            discoverer: StaticKubeConfigDiscoverer(urls: [])
        )

        let eks = try importer.commandPreview(for: CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cluster",
            regionOrLocation: "eu-north-1",
            profileOrSubscription: "synthetic-profile",
            roleARN: "arn:aws:iam::000000000000:role/synthetic"
        ))
        XCTAssertEqual(eks.executable, "aws")
        XCTAssertEqual(eks.arguments, [
            "eks", "update-kubeconfig",
            "--region", "eu-north-1",
            "--name", "synthetic-cluster",
            "--profile", "synthetic-profile",
            "--role-arn", "arn:aws:iam::000000000000:role/synthetic"
        ])

        let aks = try importer.commandPreview(for: CloudKubeConfigImportRequest(
            provider: .aks,
            clusterName: "synthetic-aks",
            resourceGroup: "synthetic-group",
            profileOrSubscription: "synthetic-subscription"
        ))
        XCTAssertEqual(aks.executable, "az")
        XCTAssertTrue(aks.arguments.contains("--overwrite-existing"))
        XCTAssertTrue(aks.displayCommand.contains("az aks get-credentials"))

        let gke = try importer.commandPreview(for: CloudKubeConfigImportRequest(
            provider: .gke,
            clusterName: "synthetic-gke",
            regionOrLocation: "europe-north1",
            projectID: "synthetic-project"
        ))
        XCTAssertEqual(gke.executable, "gcloud")
        XCTAssertTrue(gke.displayCommand.contains("gcloud container clusters get-credentials"))
    }

    func testCloudImporterBuildsExactCommandsForEveryRunnableProvider() throws {
        let importer = CloudKubeConfigCLIImporter(
            runner: RecordingCloudCommandRunner(),
            discoverer: StaticKubeConfigDiscoverer(urls: [])
        )

        let cases: [(CloudKubeConfigImportRequest, String, [String], String)] = [
            (
                CloudKubeConfigImportRequest(
                    provider: .aks,
                    clusterName: "synthetic-aks",
                    resourceGroup: "synthetic-group",
                    profileOrSubscription: "synthetic-subscription",
                    overwriteExisting: false
                ),
                "az",
                [
                    "aks", "get-credentials",
                    "--resource-group", "synthetic-group",
                    "--name", "synthetic-aks",
                    "--subscription", "synthetic-subscription"
                ],
                "az aks get-credentials --resource-group synthetic-group --name synthetic-aks --subscription synthetic-subscription"
            ),
            (
                CloudKubeConfigImportRequest(
                    provider: .eks,
                    clusterName: "synthetic-eks",
                    regionOrLocation: "eu-north-1",
                    profileOrSubscription: "synthetic-profile",
                    roleARN: "arn:aws:iam::000000000000:role/synthetic"
                ),
                "aws",
                [
                    "eks", "update-kubeconfig",
                    "--region", "eu-north-1",
                    "--name", "synthetic-eks",
                    "--profile", "synthetic-profile",
                    "--role-arn", "arn:aws:iam::000000000000:role/synthetic"
                ],
                "aws eks update-kubeconfig --region eu-north-1 --name synthetic-eks --profile synthetic-profile --role-arn arn:aws:iam::000000000000:role/synthetic"
            ),
            (
                CloudKubeConfigImportRequest(
                    provider: .gke,
                    clusterName: "synthetic-gke",
                    regionOrLocation: "europe-north1",
                    projectID: "synthetic-project"
                ),
                "gcloud",
                [
                    "container", "clusters", "get-credentials",
                    "synthetic-gke",
                    "--location", "europe-north1",
                    "--project", "synthetic-project"
                ],
                "gcloud container clusters get-credentials synthetic-gke --location europe-north1 --project synthetic-project"
            )
        ]

        for (request, executable, arguments, displayCommand) in cases {
            let preview = try importer.commandPreview(for: request)
            XCTAssertEqual(preview.executable, executable)
            XCTAssertEqual(preview.arguments, arguments)
            XCTAssertEqual(preview.displayCommand, displayCommand)
        }
    }

    func testCloudImporterCanTargetIsolatedKubeconfigFilesForLiveProviderRuns() throws {
        let importer = CloudKubeConfigCLIImporter(
            runner: RecordingCloudCommandRunner(),
            discoverer: StaticKubeConfigDiscoverer(urls: [])
        )
        let targetPath = "/tmp/rune-synthetic-kubeconfig"

        let aks = try importer.commandPreview(for: CloudKubeConfigImportRequest(
            provider: .aks,
            clusterName: "synthetic-aks",
            resourceGroup: "synthetic-group",
            targetKubeconfigPath: targetPath
        ))
        XCTAssertEqual(aks.arguments.suffix(3), ["--overwrite-existing", "--file", targetPath])
        XCTAssertTrue(aks.environment.isEmpty)

        let eks = try importer.commandPreview(for: CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-eks",
            regionOrLocation: "eu-north-1",
            targetKubeconfigPath: targetPath
        ))
        XCTAssertEqual(eks.arguments, [
            "eks", "update-kubeconfig",
            "--region", "eu-north-1",
            "--name", "synthetic-eks",
            "--kubeconfig", targetPath
        ])
        XCTAssertTrue(eks.environment.isEmpty)

        let gke = try importer.commandPreview(for: CloudKubeConfigImportRequest(
            provider: .gke,
            clusterName: "synthetic-gke",
            regionOrLocation: "europe-north1",
            projectID: "synthetic-project",
            targetKubeconfigPath: targetPath
        ))
        XCTAssertEqual(gke.arguments, [
            "container", "clusters", "get-credentials",
            "synthetic-gke",
            "--location", "europe-north1",
            "--project", "synthetic-project"
        ])
        XCTAssertEqual(gke.environment["KUBECONFIG"], targetPath)
    }

    func testCloudImporterRequiresProviderSpecificFields() throws {
        let importer = CloudKubeConfigCLIImporter(
            runner: RecordingCloudCommandRunner(),
            discoverer: StaticKubeConfigDiscoverer(urls: [])
        )

        XCTAssertThrowsError(try importer.commandPreview(for: CloudKubeConfigImportRequest(
            provider: .aks,
            clusterName: "synthetic-aks"
        ))) { error in
            XCTAssertEqual(error as? CloudKubeConfigImportError, .missingRequiredField("Resource group"))
        }

        XCTAssertThrowsError(try importer.commandPreview(for: CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-eks"
        ))) { error in
            XCTAssertEqual(error as? CloudKubeConfigImportError, .missingRequiredField("Region"))
        }

        XCTAssertThrowsError(try importer.commandPreview(for: CloudKubeConfigImportRequest(
            provider: .gke,
            clusterName: "synthetic-gke",
            regionOrLocation: "europe-north1"
        ))) { error in
            XCTAssertEqual(error as? CloudKubeConfigImportError, .missingRequiredField("Project ID"))
        }
    }

    func testCloudImporterRunsCommandDiscoversKubeconfigAndValidatesReview() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("CloudKubeConfigImporterTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("config")
        try Self.syntheticKubeconfig.write(to: kubeconfig, atomically: true, encoding: .utf8)
        let runner = RecordingCloudCommandRunner(result: .init(exitCode: 0, stdout: "updated\n", stderr: ""))
        let importer = CloudKubeConfigCLIImporter(
            runner: runner,
            discoverer: StaticKubeConfigDiscoverer(urls: [kubeconfig]),
            validator: KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"])
        )

        let result = try await importer.importCluster(CloudKubeConfigImportRequest(
            provider: .aks,
            clusterName: "synthetic-aks",
            resourceGroup: "synthetic-group"
        ))

        XCTAssertEqual(runner.commands.map(\.executable), ["az"])
        XCTAssertEqual(result.discoveredURLs, [kubeconfig])
        XCTAssertEqual(result.reviews.count, 1)
        XCTAssertTrue(result.reviews[0].isValid)
        XCTAssertEqual(result.reviews[0].contexts.first?.name, "synthetic-context")
        XCTAssertFalse(result.reviews[0].redactedPreview.contains("synthetic-token"))
    }

    func testCloudImporterPrefersExplicitTargetKubeconfigForReviewDiscovery() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("CloudKubeConfigImporterTargetTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let targetKubeconfig = directory.appendingPathComponent("isolated-config")
        try Self.syntheticKubeconfig.write(to: targetKubeconfig, atomically: true, encoding: .utf8)

        let importer = CloudKubeConfigCLIImporter(
            runner: RecordingCloudCommandRunner(result: .init(exitCode: 0, stdout: "updated\n", stderr: "")),
            discoverer: StaticKubeConfigDiscoverer(urls: [targetKubeconfig]),
            validator: KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"])
        )

        let result = try await importer.importCluster(CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-eks",
            regionOrLocation: "eu-north-1",
            targetKubeconfigPath: targetKubeconfig.path
        ))

        XCTAssertEqual(result.discoveredURLs, [targetKubeconfig])
        XCTAssertEqual(result.reviews.count, 1)
        XCTAssertTrue(result.reviews[0].isValid)
    }

    func testLiveCloudLoginGeneratesIsolatedKubeconfigAndReachesKubernetesAPIWhenExplicitlyEnabled() async throws {
        guard let request = Self.liveCloudRequestFromEnvironment() else {
            throw XCTSkip("""
            Set RUNE_LIVE_CLOUD_PROVIDER plus provider fields to run against a real test cluster.
            Required fields:
            AKS: RUNE_LIVE_AKS_CLUSTER, RUNE_LIVE_AKS_RESOURCE_GROUP
            EKS: RUNE_LIVE_EKS_CLUSTER, RUNE_LIVE_EKS_REGION
            GKE: RUNE_LIVE_GKE_CLUSTER, RUNE_LIVE_GKE_LOCATION, RUNE_LIVE_GKE_PROJECT
            Optional: RUNE_LIVE_CLOUD_PROFILE_OR_SUBSCRIPTION, RUNE_LIVE_EKS_ROLE_ARN
            """)
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("CloudKubeConfigImporterLive.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let targetKubeconfig = directory.appendingPathComponent("config")

        let importer = CloudKubeConfigCLIImporter(timeout: 180)
        let result = try await importer.importCluster(request.withTargetKubeconfigPath(targetKubeconfig.path))

        XCTAssertEqual(result.discoveredURLs.first?.standardizedFileURL.path, targetKubeconfig.standardizedFileURL.path)
        XCTAssertFalse(result.reviews.isEmpty)
        XCTAssertTrue(result.reviews.contains { $0.isValid }, result.reviews.flatMap(\.issues).map(\.message).joined(separator: "\n"))

        let contextName = try XCTUnwrap(result.reviews.flatMap(\.contexts).first?.name)
        let namespaces = try await KubernetesClient(commandTimeout: 20).listNamespaces(
            from: [KubeConfigSource(url: targetKubeconfig)],
            context: KubeContext(name: contextName)
        )
        XCTAssertFalse(namespaces.isEmpty)
    }

    func testCloudImporterReportsProviderCommandFailure() async throws {
        let runner = RecordingCloudCommandRunner(result: .init(exitCode: 42, stdout: "", stderr: "not logged in"))
        let importer = CloudKubeConfigCLIImporter(
            runner: runner,
            discoverer: StaticKubeConfigDiscoverer(urls: [])
        )

        do {
            _ = try await importer.importCluster(CloudKubeConfigImportRequest(
                provider: .gke,
                clusterName: "synthetic-gke",
                regionOrLocation: "europe-north1",
                projectID: "synthetic-project"
            ))
            XCTFail("Expected cloud command failure")
        } catch let error as CloudKubeConfigImportError {
            XCTAssertEqual(error, .commandFailed(
                command: "gcloud container clusters get-credentials synthetic-gke --location europe-north1 --project synthetic-project",
                exitCode: 42,
                message: "not logged in"
            ))
        }
    }

    private static let syntheticKubeconfig = """
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
        token: synthetic-token
    """

    private static func liveCloudRequestFromEnvironment() -> CloudKubeConfigImportRequest? {
        let env = ProcessInfo.processInfo.environment
        guard let provider = env["RUNE_LIVE_CLOUD_PROVIDER"]?.lowercased(),
              !provider.isEmpty else {
            return nil
        }
        let profileOrSubscription = env["RUNE_LIVE_CLOUD_PROFILE_OR_SUBSCRIPTION"] ?? ""
        switch provider {
        case "aks":
            guard let cluster = nonEmpty(env["RUNE_LIVE_AKS_CLUSTER"]),
                  let resourceGroup = nonEmpty(env["RUNE_LIVE_AKS_RESOURCE_GROUP"]) else {
                return nil
            }
            return CloudKubeConfigImportRequest(
                provider: .aks,
                clusterName: cluster,
                resourceGroup: resourceGroup,
                profileOrSubscription: profileOrSubscription
            )
        case "eks":
            guard let cluster = nonEmpty(env["RUNE_LIVE_EKS_CLUSTER"]),
                  let region = nonEmpty(env["RUNE_LIVE_EKS_REGION"]) else {
                return nil
            }
            return CloudKubeConfigImportRequest(
                provider: .eks,
                clusterName: cluster,
                regionOrLocation: region,
                profileOrSubscription: profileOrSubscription,
                roleARN: env["RUNE_LIVE_EKS_ROLE_ARN"] ?? ""
            )
        case "gke":
            guard let cluster = nonEmpty(env["RUNE_LIVE_GKE_CLUSTER"]),
                  let location = nonEmpty(env["RUNE_LIVE_GKE_LOCATION"]),
                  let project = nonEmpty(env["RUNE_LIVE_GKE_PROJECT"]) else {
                return nil
            }
            return CloudKubeConfigImportRequest(
                provider: .gke,
                clusterName: cluster,
                regionOrLocation: location,
                projectID: project
            )
        default:
            return nil
        }
    }

    private static func nonEmpty(_ value: String?) -> String? {
        let trimmed = value?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return trimmed.isEmpty ? nil : trimmed
    }
}

private extension CloudKubeConfigImportRequest {
    func withTargetKubeconfigPath(_ path: String) -> CloudKubeConfigImportRequest {
        CloudKubeConfigImportRequest(
            provider: provider,
            clusterName: clusterName,
            regionOrLocation: regionOrLocation,
            resourceGroup: resourceGroup,
            projectID: projectID,
            profileOrSubscription: profileOrSubscription,
            roleARN: roleARN,
            targetKubeconfigPath: path,
            overwriteExisting: overwriteExisting
        )
    }
}

private final class RecordingCloudCommandRunner: CloudKubeConfigCommandRunning, @unchecked Sendable {
    private(set) var commands: [CloudKubeConfigCommandPreview] = []
    private let result: CloudKubeConfigCommandResult

    init(result: CloudKubeConfigCommandResult = .init(exitCode: 0, stdout: "", stderr: "")) {
        self.result = result
    }

    func run(_ command: CloudKubeConfigCommandPreview, timeout: TimeInterval) async throws -> CloudKubeConfigCommandResult {
        commands.append(command)
        return result
    }
}

private struct StaticKubeConfigDiscoverer: KubeConfigDiscovering {
    let urls: [URL]

    func discoverCandidateFiles() -> [URL] {
        urls
    }
}
