import XCTest
@testable import RuneSecurity

final class CloudKubeConfigImporterTests: XCTestCase {
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
