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

    func testCancellationCleansUpANonAcknowledgingTerminalLaunch() async throws {
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
        XCTAssertFalse(FileManager.default.fileExists(atPath: try XCTUnwrap(launcher.commandFileURL).deletingLastPathComponent().path))
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

    func testStreamsSplitUTF8AndDrainsTheFinalOutputBeforeReturning() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let recorder = TerminalCloudOutputRecorder()
        let launcher = SplitOutputTerminalLauncher(recorder: recorder)
        let runner = TerminalCloudKubeConfigCommandRunner(launcher: launcher, pollNanoseconds: 1_000_000)

        let result = try await runner.run(command(in: directory), timeout: 2, onOutput: { recorder.append($0) })

        XCTAssertTrue(launcher.streamedBeforeCompletion)
        XCTAssertEqual(result.stdout, "prefix å🙂 final\n")
        XCTAssertEqual(recorder.chunks.filter { $0.stream == .stdout }.map(\.text).joined(), result.stdout)
        XCTAssertEqual(result.stderr, "last warning\n")
        XCTAssertEqual(recorder.chunks.filter { $0.stream == .stderr }.map(\.text).joined(), result.stderr)
    }

    func testBoundsReaderMemoryAndDoesNotSplitUTF8AtTheCaptureLimit() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let launcher = CompletingTerminalLauncher(stdout: "abc🙂" + String(repeating: "x", count: 200_000), stderr: "", exitCode: 0)
        let recorder = TerminalCloudOutputRecorder()
        let runner = TerminalCloudKubeConfigCommandRunner(launcher: launcher, pollNanoseconds: 1_000_000, outputByteLimit: 5)

        let result = try await runner.run(command(in: directory), timeout: 2, onOutput: { recorder.append($0) })

        XCTAssertEqual(result.stdout, "abc\n[Additional command output omitted.]\n")
        XCTAssertEqual(recorder.chunks.map(\.text).joined(), result.stdout)
    }

    func testGeneratedScriptBoundsDiskOutputAndContinuesDrainingBothStreams() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        try writeFakeCLI(in: directory, script: """
        /usr/bin/yes x | /usr/bin/head -c 131072
        /usr/bin/yes y | /usr/bin/head -c 131072 >&2
        print -r -- 'completed' > "$KUBECONFIG"
        /bin/sleep 0.1
        """)
        let launcher = InspectingExecutingTerminalLauncher()
        let runner = TerminalCloudKubeConfigCommandRunner(
            launcher: launcher,
            pollNanoseconds: 50_000_000,
            cliSearchPath: "\(directory.path):/usr/bin:/bin",
            outputByteLimit: 128
        )

        let result = try await runner.run(command(in: directory), timeout: 3)

        XCTAssertEqual(result.exitCode, 0)
        XCTAssertEqual(launcher.outputFileSizes, [129, 129])
        XCTAssertTrue(result.stdout.hasSuffix("[Additional command output omitted.]\n"))
        XCTAssertTrue(result.stderr.hasSuffix("[Additional command output omitted.]\n"))
        XCTAssertEqual(try String(contentsOf: directory.appendingPathComponent("config"), encoding: .utf8), "completed\n")
    }

    func testGeneratedScriptStreamsShortLoginOutputBeforeTheProviderCompletes() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        try writeFakeCLI(in: directory, script: """
        print -r -- 'Use synthetic sign-in code'
        for attempt in {1..100}; do
          [[ -e "$KUBECONFIG.ready" ]] && exit 0
          /bin/sleep 0.01
        done
        exit 9
        """)
        let recorder = TerminalCloudOutputRecorder()
        let acknowledgement = directory.appendingPathComponent("config.ready")
        let runner = TerminalCloudKubeConfigCommandRunner(
            launcher: ExecutingTerminalLauncher(),
            pollNanoseconds: 1_000_000,
            cliSearchPath: "\(directory.path):/usr/bin:/bin"
        )

        let result = try await runner.run(command(in: directory), timeout: 3, onOutput: { chunk in
            recorder.append(chunk)
            if recorder.chunks.map(\.text).joined().contains("Use synthetic sign-in code") {
                try? Data().write(to: acknowledgement)
            }
        })

        XCTAssertEqual(result.exitCode, 0, "The CLI must receive acknowledgement while it is still running")
        XCTAssertEqual(result.stdout, "Use synthetic sign-in code\n")
        XCTAssertTrue(FileManager.default.fileExists(atPath: acknowledgement.path))
    }

    func testTimeoutIncludesTheTerminalLaunchAndRemovesItsPrivateDirectory() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let launcher = SuspendedTerminalLauncher()
        let runner = TerminalCloudKubeConfigCommandRunner(launcher: launcher, pollNanoseconds: 1_000_000)
        let start = ContinuousClock.now

        let result = try await runner.run(command(in: directory), timeout: 0.02)

        XCTAssertTrue(result.timedOut)
        XCTAssertEqual(result.exitCode, 124)
        XCTAssertLessThan(start.duration(to: .now), .seconds(2))
        XCTAssertFalse(FileManager.default.fileExists(atPath: try XCTUnwrap(launcher.commandFileURL).deletingLastPathComponent().path))
        launcher.resume()
    }

    func testTimedOutQueuedScriptCannotStartAfterCleanupEvenIfTerminalAlreadyReadIt() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        try writeFakeCLI(in: directory, script: "print -r -- 'unexpected' > \"$KUBECONFIG\"")
        let launcher = NonCompletingTerminalLauncher()
        let runner = TerminalCloudKubeConfigCommandRunner(
            launcher: launcher,
            pollNanoseconds: 1_000_000,
            cliSearchPath: "\(directory.path):/usr/bin:/bin"
        )

        let result = try await runner.run(command(in: directory), timeout: 0.02)
        let delayedScript = directory.appendingPathComponent("delayed.command")
        try Data(try XCTUnwrap(launcher.launchedScript).utf8).write(to: delayedScript)
        try await ExecutingTerminalLauncher().launch(commandFileURL: delayedScript)

        XCTAssertTrue(result.timedOut)
        XCTAssertFalse(FileManager.default.fileExists(atPath: directory.appendingPathComponent("config").path))
    }

    func testCancellationAndTimeoutTerminateTermIgnoringProviderAndItsChild() async throws {
        for cancel in [false, true] {
            let directory = try makeTemporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            try writeFakeCLI(in: directory, script: """
            trap '' TERM
            /bin/sleep 30 &
            print -r -- "$$ $!" > "$KUBECONFIG"
            wait
            """)
            let launcher = NonblockingExecutingTerminalLauncher()
            let runner = TerminalCloudKubeConfigCommandRunner(
                launcher: launcher,
                pollNanoseconds: 1_000_000,
                cliSearchPath: "\(directory.path):/usr/bin:/bin"
            )
            let preview = command(in: directory)
            let task = Task { try await runner.run(preview, timeout: cancel ? 10 : 0.5) }
            let target = directory.appendingPathComponent("config")
            let startedDeadline = ContinuousClock.now.advanced(by: .seconds(2))
            while !FileManager.default.fileExists(atPath: target.path), ContinuousClock.now < startedDeadline {
                try await Task.sleep(for: .milliseconds(5))
            }
            let pids = try String(contentsOf: target, encoding: .utf8)
                .split(whereSeparator: \.isWhitespace).compactMap { Int32($0) }
            XCTAssertEqual(pids.count, 2)
            if cancel { task.cancel() }

            do {
                let result = try await task.value
                XCTAssertFalse(cancel, "Expected cancellation")
                XCTAssertTrue(result.timedOut)
            } catch is CancellationError {
                XCTAssertTrue(cancel)
            }

            let stoppedDeadline = ContinuousClock.now.advanced(by: .seconds(2))
            while pids.contains(where: { kill($0, 0) == 0 }), ContinuousClock.now < stoppedDeadline {
                try await Task.sleep(for: .milliseconds(10))
            }
            for pid in pids {
                XCTAssertNotEqual(kill(pid, 0), 0, "Provider process \(pid) survived shutdown")
            }
            XCTAssertFalse(FileManager.default.fileExists(atPath: try XCTUnwrap(launcher.commandFileURL).deletingLastPathComponent().path))
        }
    }

    func testAlreadyCancelledTaskDoesNotLaunchTerminalOrCreateHelpers() async throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let launcher = NonCompletingTerminalLauncher()
        let runner = TerminalCloudKubeConfigCommandRunner(launcher: launcher)
        let preview = command(in: directory)
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await runner.run(preview, timeout: 1)
        }

        do {
            _ = try await task.value
            XCTFail("Expected cancellation")
        } catch is CancellationError { }
        XCTAssertNil(launcher.launchedScript)
        XCTAssertTrue(try FileManager.default.contentsOfDirectory(atPath: directory.path).isEmpty)
    }

    private func command(in directory: URL) -> CloudKubeConfigCommandPreview {
        .init(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--name", "synthetic-cluster"],
            displayCommand: "synthetic preview",
            environment: ["KUBECONFIG": directory.appendingPathComponent("config").path]
        )
    }

    private func writeFakeCLI(in directory: URL, script: String) throws {
        let url = directory.appendingPathComponent("aws")
        try Data("#!/bin/zsh\n\(script)\n".utf8).write(to: url)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: url.path)
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
    private(set) var commandFileURL: URL?

    func launch(commandFileURL: URL) async throws {
        self.commandFileURL = commandFileURL
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
        let process = terminalFixtureProcess(commandFileURL: commandFileURL)
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            process.terminationHandler = { _ in continuation.resume() }
            do {
                try process.run()
            } catch {
                process.terminationHandler = nil
                continuation.resume(throwing: error)
            }
        }
    }
}

@MainActor
private final class NonblockingExecutingTerminalLauncher: TerminalCommandDocumentLaunching {
    private var process: Process?
    private(set) var commandFileURL: URL?

    func launch(commandFileURL: URL) async throws {
        self.commandFileURL = commandFileURL
        let process = terminalFixtureProcess(commandFileURL: commandFileURL)
        self.process = process
        try process.run()
    }
}

@MainActor
private final class InspectingExecutingTerminalLauncher: TerminalCommandDocumentLaunching {
    private(set) var outputFileSizes = [0, 0]

    func launch(commandFileURL: URL) async throws {
        let process = terminalFixtureProcess(commandFileURL: commandFileURL)
        let directory = commandFileURL.deletingLastPathComponent()
        try process.run()
        while process.isRunning {
            for (index, name) in ["stdout", "stderr"].enumerated() {
                let path = directory.appendingPathComponent(".rune-cloud-import.\(name)").path
                if let size = (try? FileManager.default.attributesOfItem(atPath: path)[.size]) as? NSNumber {
                    outputFileSizes[index] = max(outputFileSizes[index], size.intValue)
                }
            }
            try await Task.sleep(for: .milliseconds(1))
        }
    }
}

@MainActor
private final class SuspendedTerminalLauncher: TerminalCommandDocumentLaunching {
    private(set) var commandFileURL: URL?
    private var continuation: CheckedContinuation<Void, Never>?

    func launch(commandFileURL: URL) async throws {
        self.commandFileURL = commandFileURL
        await withCheckedContinuation { continuation = $0 }
    }

    func resume() {
        continuation?.resume()
        continuation = nil
    }
}

@MainActor
private final class SplitOutputTerminalLauncher: TerminalCommandDocumentLaunching {
    let recorder: TerminalCloudOutputRecorder
    private(set) var streamedBeforeCompletion = false

    init(recorder: TerminalCloudOutputRecorder) {
        self.recorder = recorder
    }

    func launch(commandFileURL: URL) async throws {
        let directory = commandFileURL.deletingLastPathComponent()
        let output = directory.appendingPathComponent(".rune-cloud-import.stdout")
        try Data("prefix ".utf8).write(to: output)
        let handle = try FileHandle(forWritingTo: output)
        defer { try? handle.close() }
        try handle.seekToEnd()
        for byte in "å🙂".utf8 {
            try handle.write(contentsOf: Data([byte]))
            try await Task.sleep(for: .milliseconds(15))
        }
        streamedBeforeCompletion = recorder.chunks.map(\.text).joined() == "prefix å🙂"
        try handle.write(contentsOf: Data(" final\n".utf8))
        try Data("last warning\n".utf8).write(to: directory.appendingPathComponent(".rune-cloud-import.stderr"))
        try Data("0\n".utf8).write(to: directory.appendingPathComponent(".rune-cloud-import.status"))
    }
}

private func terminalFixtureProcess(commandFileURL: URL) -> Process {
    let process = Process()
    // Terminal launches command documents with a controlling TTY. Reproduce that
    // locally so process-group cancellation is tested without opening Terminal.
    process.executableURL = URL(fileURLWithPath: "/usr/bin/script")
    process.arguments = ["-q", "/dev/null", "/bin/zsh", commandFileURL.path]
    process.standardInput = FileHandle.nullDevice
    process.standardOutput = FileHandle.nullDevice
    process.standardError = FileHandle.nullDevice
    return process
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
