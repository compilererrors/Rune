import Foundation
import XCTest
@testable import RuneCore
@testable import RuneSecurity
@testable import RuneUI

@MainActor
final class TerminalCloudKubeConfigCommandRunnerTests: XCTestCase {
    func testRunsAllowlistedCloudCommandAndStreamsItsResult() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("config")
        let launcher = CompletingTerminalLauncher(
            stdout: "synthetic-output\n",
            stderr: "synthetic-warning\n",
            exitCode: 0
        )
        let runner = TerminalCloudKubeConfigCommandRunner(
            launcher: launcher,
            pollNanoseconds: 1_000_000
        )
        let recorder = TerminalCloudOutputRecorder()

        let result = try await runner.run(
            CloudKubeConfigCommandPreview(
                executable: "gcloud",
                arguments: [
                    "container", "clusters", "get-credentials", "synthetic's-cluster",
                    "--location", "example-location", "--project", "synthetic-project"
                ],
                displayCommand: "synthetic preview",
                environment: ["KUBECONFIG": kubeconfig.path]
            ),
            timeout: 1,
            onOutput: { recorder.append($0) }
        )

        XCTAssertEqual(result.exitCode, 0)
        XCTAssertEqual(result.stdout, "synthetic-output\n")
        XCTAssertEqual(result.stderr, "synthetic-warning\n")
        XCTAssertFalse(result.timedOut)
        XCTAssertTrue(recorder.chunks.contains(.init(stream: .stdout, text: "synthetic-output\n")))
        XCTAssertTrue(recorder.chunks.contains(.init(stream: .stderr, text: "synthetic-warning\n")))
        XCTAssertTrue(try XCTUnwrap(launcher.launchedScript).contains("'synthetic'\"'\"'s-cluster'"))
    }

    func testGeneratedCommandDocumentsExecuteAllProvidersAndWriteIsolatedKubeconfigs() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let fakeScript = """
        #!/bin/zsh
        target="$KUBECONFIG"
        while [[ $# -gt 0 ]]; do
          case "$1" in
            --file|--kubeconfig) target="$2"; shift 2 ;;
            *) shift ;;
          esac
        done
        print -r -- "synthetic-${0:t}-output"
        print -r -- 'apiVersion: v1' > "$target"
        """
        for executable in ["az", "aws", "gcloud"] {
            let fakeCLI = directory.appendingPathComponent(executable)
            try Data(fakeScript.utf8).write(to: fakeCLI)
            try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: fakeCLI.path)
        }
        let runner = TerminalCloudKubeConfigCommandRunner(
            launcher: ExecutingTerminalLauncher(),
            pollNanoseconds: 1_000_000,
            cliSearchPath: "\(directory.path):/usr/bin:/bin"
        )
        let cases: [(CloudKubeConfigImportRequest, String)] = [
            (
                CloudKubeConfigImportRequest(
                    provider: .aks,
                    clusterName: "synthetic-aks",
                    resourceGroup: "synthetic-group",
                    targetKubeconfigPath: directory.appendingPathComponent("aks-config").path
                ),
                "az"
            ),
            (
                CloudKubeConfigImportRequest(
                    provider: .eks,
                    clusterName: "synthetic-eks",
                    regionOrLocation: "example-region",
                    targetKubeconfigPath: directory.appendingPathComponent("eks-config").path
                ),
                "aws"
            ),
            (
                CloudKubeConfigImportRequest(
                    provider: .gke,
                    clusterName: "synthetic-gke",
                    regionOrLocation: "example-location",
                    projectID: "synthetic-project",
                    targetKubeconfigPath: directory.appendingPathComponent("gke-config").path
                ),
                "gcloud"
            )
        ]

        for (request, executable) in cases {
            let preview = try CloudKubeConfigCommandBuilder().preview(for: request)
            let result = try await runner.run(preview, timeout: 2)

            XCTAssertEqual(result.exitCode, 0, executable)
            XCTAssertEqual(result.stdout, "synthetic-\(executable)-output\n", executable)
            XCTAssertEqual(
                try String(contentsOfFile: request.targetKubeconfigPath, encoding: .utf8),
                "apiVersion: v1\n",
                executable
            )
        }
    }

    func testRejectsExecutableAndCommandFamiliesOutsideCloudImportAllowlist() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("config")
        let launcher = CompletingTerminalLauncher(stdout: "", stderr: "", exitCode: 0)
        let runner = TerminalCloudKubeConfigCommandRunner(launcher: launcher)
        let unsupportedCommands = [
            CloudKubeConfigCommandPreview(
                executable: "sh",
                arguments: ["-c", "true"],
                displayCommand: "unsupported",
                environment: ["KUBECONFIG": kubeconfig.path]
            ),
            CloudKubeConfigCommandPreview(
                executable: "az",
                arguments: ["account", "show"],
                displayCommand: "unsupported",
                environment: ["KUBECONFIG": kubeconfig.path]
            )
        ]

        for command in unsupportedCommands {
            do {
                _ = try await runner.run(command, timeout: 1)
                XCTFail("Expected unsupported command to be rejected")
            } catch let error as RuneError {
                XCTAssertEqual(error, .invalidInput(message: "Unsupported cloud import command."))
            }
        }
        XCTAssertNil(launcher.launchedScript)
    }

    func testRequiresAnIsolatedDestinationForClusterImports() async throws {
        let launcher = CompletingTerminalLauncher(stdout: "", stderr: "", exitCode: 0)
        let runner = TerminalCloudKubeConfigCommandRunner(launcher: launcher)

        do {
            _ = try await runner.run(
                CloudKubeConfigCommandPreview(
                    executable: "aws",
                    arguments: ["eks", "update-kubeconfig", "--name", "synthetic-cluster"],
                    displayCommand: "synthetic preview"
                ),
                timeout: 1
            )
            XCTFail("Expected a missing destination error")
        } catch let error as RuneError {
            XCTAssertEqual(
                error,
                .invalidInput(message: "Cloud import requires an isolated kubeconfig destination.")
            )
        }
        XCTAssertNil(launcher.launchedScript)
    }

    func testSupportsOnlyTheExactInteractiveAzureSignInCommandWithoutKubeconfig() async throws {
        let launcher = CompletingTerminalLauncher(stdout: "", stderr: "", exitCode: 0)
        let runner = TerminalCloudKubeConfigCommandRunner(
            launcher: launcher,
            pollNanoseconds: 1_000_000
        )

        let result = try await runner.run(
            CloudKubeConfigCommandPreview(
                executable: "az",
                arguments: ["login", "--only-show-errors", "--output", "none"],
                displayCommand: "az login --only-show-errors --output none"
            ),
            timeout: 1
        )

        XCTAssertEqual(result.exitCode, 0)
        XCTAssertNotNil(launcher.launchedScript)
    }

    func testCancellationCreatesARequestForTheTerminalChildCommand() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let launcher = NonCompletingTerminalLauncher()
        let runner = TerminalCloudKubeConfigCommandRunner(
            launcher: launcher,
            pollNanoseconds: 1_000_000
        )
        let task = Task {
            try await runner.run(
                CloudKubeConfigCommandPreview(
                    executable: "aws",
                    arguments: ["eks", "update-kubeconfig", "--name", "synthetic-cluster"],
                    displayCommand: "synthetic preview",
                    environment: ["KUBECONFIG": directory.appendingPathComponent("config").path]
                ),
                timeout: 5
            )
        }

        while launcher.launchedScript == nil {
            try await Task.sleep(nanoseconds: 1_000_000)
        }
        task.cancel()

        do {
            _ = try await task.value
            XCTFail("Expected cancellation")
        } catch is CancellationError {
        }
        XCTAssertTrue(FileManager.default.fileExists(
            atPath: directory.appendingPathComponent(".rune-cloud-import.cancel").path
        ))
    }

    func testCancellationBeforeTerminalStartsNeverLaunchesProviderCLI() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let fakeCLI = directory.appendingPathComponent("aws")
        try Data("#!/bin/zsh\nprint -r -- 'unexpected-provider-run' > \"$KUBECONFIG\"\n".utf8)
            .write(to: fakeCLI)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: fakeCLI.path)
        let target = directory.appendingPathComponent("config")
        let runner = TerminalCloudKubeConfigCommandRunner(
            launcher: CancelledBeforeExecutionTerminalLauncher(),
            pollNanoseconds: 1_000_000,
            cliSearchPath: "\(directory.path):/usr/bin:/bin"
        )

        let result = try await runner.run(
            .init(
                executable: "aws",
                arguments: ["eks", "update-kubeconfig", "--name", "synthetic-cluster"],
                displayCommand: "synthetic preview",
                environment: ["KUBECONFIG": target.path]
            ),
            timeout: 2
        )

        XCTAssertEqual(result.exitCode, 130)
        XCTAssertFalse(FileManager.default.fileExists(atPath: target.path))
    }

    private func makeTemporaryDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneTerminalRunnerTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false)
        return directory
    }
}

@MainActor
private final class CompletingTerminalLauncher: TerminalCommandDocumentLaunching {
    private let stdout: String
    private let stderr: String
    private let exitCode: Int32
    private(set) var launchedScript: String?

    init(stdout: String, stderr: String, exitCode: Int32) {
        self.stdout = stdout
        self.stderr = stderr
        self.exitCode = exitCode
    }

    func launch(commandFileURL: URL) async throws {
        launchedScript = try String(contentsOf: commandFileURL, encoding: .utf8)
        let directory = commandFileURL.deletingLastPathComponent()
        try Data(stdout.utf8).write(to: directory.appendingPathComponent(".rune-cloud-import.stdout"))
        try Data(stderr.utf8).write(to: directory.appendingPathComponent(".rune-cloud-import.stderr"))
        try Data("\(exitCode)\n".utf8).write(to: directory.appendingPathComponent(".rune-cloud-import.status"))
    }
}

@MainActor
private final class NonCompletingTerminalLauncher: TerminalCommandDocumentLaunching {
    private(set) var launchedScript: String?

    func launch(commandFileURL: URL) async throws {
        launchedScript = try String(contentsOf: commandFileURL, encoding: .utf8)
    }
}

@MainActor
private struct CancelledBeforeExecutionTerminalLauncher: TerminalCommandDocumentLaunching {
    func launch(commandFileURL: URL) async throws {
        let marker = commandFileURL.deletingLastPathComponent()
            .appendingPathComponent(".rune-cloud-import.cancel")
        try Data().write(to: marker)
        try await ExecutingTerminalLauncher().launch(commandFileURL: commandFileURL)
    }
}

@MainActor
private struct ExecutingTerminalLauncher: TerminalCommandDocumentLaunching {
    func launch(commandFileURL: URL) async throws {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/bin/zsh")
        process.arguments = [commandFileURL.path]
        try process.run()
        process.waitUntilExit()
    }
}

private final class TerminalCloudOutputRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [CloudKubeConfigCommandOutput] = []

    var chunks: [CloudKubeConfigCommandOutput] {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }

    func append(_ chunk: CloudKubeConfigCommandOutput) {
        lock.lock()
        storage.append(chunk)
        lock.unlock()
    }
}
