import AppKit
import Foundation
import RuneCore
import RuneSecurity

/// Uses a normal child process in direct builds and the user-trusted Terminal app
/// in App Store builds, where App Sandbox intentionally blocks host CLI execution.
public struct RuneCloudKubeConfigCommandRunner: CloudKubeConfigCommandRunning {
    private let processRunner = ProcessCloudKubeConfigCommandRunner()
    private let terminalRunner = TerminalCloudKubeConfigCommandRunner()

    public init() {}

    public func run(
        _ command: CloudKubeConfigCommandPreview,
        timeout: TimeInterval
    ) async throws -> CloudKubeConfigCommandResult {
        try await run(command, timeout: timeout, onOutput: { _ in })
    }

    public func run(
        _ command: CloudKubeConfigCommandPreview,
        timeout: TimeInterval,
        onOutput: @escaping @Sendable (CloudKubeConfigCommandOutput) -> Void
    ) async throws -> CloudKubeConfigCommandResult {
        if RuneExternalCommandPolicy.allowsExternalCommands {
            return try await processRunner.run(command, timeout: timeout, onOutput: onOutput)
        }
        return try await terminalRunner.run(command, timeout: timeout, onOutput: onOutput)
    }
}

protocol TerminalCommandDocumentLaunching: Sendable {
    @MainActor
    func launch(commandFileURL: URL) async throws
}

struct WorkspaceTerminalCommandDocumentLauncher: TerminalCommandDocumentLaunching {
    @MainActor
    func launch(commandFileURL: URL) async throws {
        guard let terminalURL = NSWorkspace.shared.urlForApplication(
            withBundleIdentifier: "com.apple.Terminal"
        ) else {
            throw RuneError.commandFailed(
                command: "Terminal",
                message: "Terminal could not be found on this Mac."
            )
        }

        let configuration = NSWorkspace.OpenConfiguration()
        configuration.activates = false
        configuration.addsToRecentItems = false

        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            NSWorkspace.shared.open(
                [commandFileURL],
                withApplicationAt: terminalURL,
                configuration: configuration
            ) { _, error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: ())
                }
            }
        }
    }
}

/// Runs the narrowly allow-listed cloud login command in Terminal rather than as
/// an App Sandbox child process. Terminal stays in the background and writes only
/// bounded command output, an exit status, and the isolated kubeconfig back to the
/// per-import temporary directory owned by Rune.
struct TerminalCloudKubeConfigCommandRunner: CloudKubeConfigCommandRunning {
    private static let defaultCLISearchPath = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

    private let launcher: any TerminalCommandDocumentLaunching
    private let pollNanoseconds: UInt64
    private let cliSearchPath: String

    init(
        launcher: any TerminalCommandDocumentLaunching = WorkspaceTerminalCommandDocumentLauncher(),
        pollNanoseconds: UInt64 = 100_000_000,
        cliSearchPath: String = Self.defaultCLISearchPath
    ) {
        self.launcher = launcher
        self.pollNanoseconds = pollNanoseconds
        self.cliSearchPath = cliSearchPath
    }

    func run(
        _ command: CloudKubeConfigCommandPreview,
        timeout: TimeInterval
    ) async throws -> CloudKubeConfigCommandResult {
        try await run(command, timeout: timeout, onOutput: { _ in })
    }

    func run(
        _ command: CloudKubeConfigCommandPreview,
        timeout: TimeInterval,
        onOutput: @escaping @Sendable (CloudKubeConfigCommandOutput) -> Void
    ) async throws -> CloudKubeConfigCommandResult {
        try Task.checkCancellation()
        guard Self.isAllowed(command) else {
            throw RuneError.invalidInput(message: "Unsupported cloud import command.")
        }

        let files = try executionFiles(for: command)
        do {
            try prepare(files: files, command: command)
        } catch {
            cleanup(files: files)
            throw error
        }

        return try await withTaskCancellationHandler {
            do {
                try await launcher.launch(commandFileURL: files.command)
            } catch {
                cleanup(files: files)
                throw RuneError.commandFailed(
                    command: command.executable,
                    message: "Terminal could not start the cloud import command."
                )
            }

            return try await waitForResult(
                files: files,
                timeout: timeout,
                onOutput: onOutput
            )
        } onCancel: {
            files.requestCancellation()
        }
    }

    private func executionFiles(
        for command: CloudKubeConfigCommandPreview
    ) throws -> TerminalCloudImportExecutionFiles {
        let targetPath = command.environment["KUBECONFIG"]
            ?? value(after: "--file", in: command.arguments)
            ?? value(after: "--kubeconfig", in: command.arguments)
        if command.executable == "az", command.arguments == ["login", "--only-show-errors", "--output", "none"] {
            let directory = FileManager.default.temporaryDirectory
                .appendingPathComponent("RuneCloudCommand.\(UUID().uuidString)", isDirectory: true)
            try FileManager.default.createDirectory(
                at: directory,
                withIntermediateDirectories: false,
                attributes: [.posixPermissions: 0o700]
            )
            return TerminalCloudImportExecutionFiles(directory: directory, ownsDirectory: true)
        }

        guard let targetPath,
              !targetPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw RuneError.invalidInput(
                message: "Cloud import requires an isolated kubeconfig destination."
            )
        }

        let directory = URL(fileURLWithPath: targetPath)
            .standardizedFileURL
            .deletingLastPathComponent()
        return TerminalCloudImportExecutionFiles(directory: directory)
    }

    private static func isAllowed(_ command: CloudKubeConfigCommandPreview) -> Bool {
        switch command.executable {
        case "az":
            return command.arguments == ["login", "--only-show-errors", "--output", "none"]
                || command.arguments.starts(with: ["aks", "get-credentials"])
        case "aws":
            return command.arguments.starts(with: ["eks", "update-kubeconfig"])
        case "gcloud":
            return command.arguments.starts(with: ["container", "clusters", "get-credentials"])
        default:
            return false
        }
    }

    private func value(after flag: String, in arguments: [String]) -> String? {
        guard let index = arguments.firstIndex(of: flag),
              arguments.indices.contains(index + 1) else { return nil }
        return arguments[index + 1]
    }

    private func prepare(
        files: TerminalCloudImportExecutionFiles,
        command: CloudKubeConfigCommandPreview
    ) throws {
        for url in files.helperFiles {
            try? FileManager.default.removeItem(at: url)
        }

        let script = shellScript(files: files, command: command)
        try Data(script.utf8).write(to: files.command, options: .atomic)
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o700],
            ofItemAtPath: files.command.path
        )
    }

    private func shellScript(
        files: TerminalCloudImportExecutionFiles,
        command: CloudKubeConfigCommandPreview
    ) -> String {
        let executable = shellQuote(command.executable)
        let arguments = command.arguments.map(shellQuote).joined(separator: " ")
        let environment = command.environment
            .filter { key, _ in key.unicodeScalars.allSatisfy { scalar in
                CharacterSet.uppercaseLetters.contains(scalar)
                    || CharacterSet.decimalDigits.contains(scalar)
                    || scalar == "_"
            } }
            .sorted { $0.key < $1.key }
            .map { "export \($0.key)=\(shellQuote($0.value))" }
            .joined(separator: "\n")

        return """
        #!/bin/zsh
        set +e
        umask 077
        export PATH=\(shellQuote(cliSearchPath))
        \(environment)
        stdout_file=\(shellQuote(files.stdout.path))
        stderr_file=\(shellQuote(files.stderr.path))
        status_file=\(shellQuote(files.status.path))
        status_tmp=\(shellQuote(files.statusTemporary.path))
        cancel_file=\(shellQuote(files.cancel.path))
        : > "$stdout_file"
        : > "$stderr_file"
        rm -f "$status_file" "$status_tmp"
        if [[ -e "$cancel_file" ]]; then
          print -r -- '130' > "$status_tmp"
          mv -f "$status_tmp" "$status_file"
          exit 130
        fi
        if ! command -v \(executable) >/dev/null 2>&1; then
          print -r -- "Required provider CLI not found: \(command.executable)" > "$stderr_file"
          print -r -- '127' > "$status_tmp"
          mv -f "$status_tmp" "$status_file"
          exit 127
        fi
        \(executable) \(arguments) > "$stdout_file" 2> "$stderr_file" &
        child_pid=$!
        while kill -0 "$child_pid" 2>/dev/null; do
          if [[ -e "$cancel_file" ]]; then
            kill -TERM "$child_pid" 2>/dev/null
            wait "$child_pid" 2>/dev/null
            print -r -- '130' > "$status_tmp"
            mv -f "$status_tmp" "$status_file"
            exit 130
          fi
          sleep 0.1
        done
        wait "$child_pid"
        exit_code=$?
        print -r -- "$exit_code" > "$status_tmp"
        mv -f "$status_tmp" "$status_file"
        exit "$exit_code"
        """
    }

    private func shellQuote(_ value: String) -> String {
        "'" + value.replacingOccurrences(of: "'", with: "'\"'\"'") + "'"
    }

    private func waitForResult(
        files: TerminalCloudImportExecutionFiles,
        timeout: TimeInterval,
        onOutput: @escaping @Sendable (CloudKubeConfigCommandOutput) -> Void
    ) async throws -> CloudKubeConfigCommandResult {
        let deadline = Date().addingTimeInterval(timeout)
        var streamedStdoutBytes = 0
        var streamedStderrBytes = 0

        while Date() < deadline {
            try Task.checkCancellation()
            streamNewOutput(
                from: files.stdout,
                stream: .stdout,
                previousByteCount: &streamedStdoutBytes,
                onOutput: onOutput
            )
            streamNewOutput(
                from: files.stderr,
                stream: .stderr,
                previousByteCount: &streamedStderrBytes,
                onOutput: onOutput
            )

            if let exitCode = readExitCode(from: files.status) {
                let result = CloudKubeConfigCommandResult(
                    exitCode: exitCode,
                    stdout: readText(from: files.stdout),
                    stderr: readText(from: files.stderr),
                    timedOut: false
                )
                cleanup(files: files)
                return result
            }

            try await Task.sleep(nanoseconds: pollNanoseconds)
        }

        files.requestCancellation()
        let cancellationDeadline = Date().addingTimeInterval(0.75)
        while Date() < cancellationDeadline, readExitCode(from: files.status) == nil {
            try? await Task.sleep(nanoseconds: min(pollNanoseconds, 100_000_000))
        }
        let result = CloudKubeConfigCommandResult(
            exitCode: 124,
            stdout: readText(from: files.stdout),
            stderr: readText(from: files.stderr),
            timedOut: true
        )
        cleanup(files: files, preservingCancellationMarker: true)
        return result
    }

    private func streamNewOutput(
        from url: URL,
        stream: CloudKubeConfigCommandOutputStream,
        previousByteCount: inout Int,
        onOutput: @escaping @Sendable (CloudKubeConfigCommandOutput) -> Void
    ) {
        guard let data = try? Data(contentsOf: url), data.count > previousByteCount else { return }
        let newData = data.subdata(in: previousByteCount..<data.count)
        previousByteCount = data.count
        guard let text = String(data: newData, encoding: .utf8), !text.isEmpty else { return }
        onOutput(CloudKubeConfigCommandOutput(stream: stream, text: text))
    }

    private func readExitCode(from url: URL) -> Int32? {
        guard let raw = try? String(contentsOf: url, encoding: .utf8),
              let value = Int32(raw.trimmingCharacters(in: .whitespacesAndNewlines)) else {
            return nil
        }
        return value
    }

    private func readText(from url: URL) -> String {
        (try? String(contentsOf: url, encoding: .utf8)) ?? ""
    }

    private func cleanup(
        files: TerminalCloudImportExecutionFiles,
        preservingCancellationMarker: Bool = false
    ) {
        for url in files.helperFiles where !preservingCancellationMarker || url != files.cancel {
            try? FileManager.default.removeItem(at: url)
        }
        if files.ownsDirectory, !preservingCancellationMarker {
            try? FileManager.default.removeItem(at: files.directory)
        }
    }
}

private struct TerminalCloudImportExecutionFiles: Sendable {
    let directory: URL
    let command: URL
    let stdout: URL
    let stderr: URL
    let status: URL
    let statusTemporary: URL
    let cancel: URL
    let ownsDirectory: Bool

    init(directory: URL, ownsDirectory: Bool = false) {
        self.directory = directory
        self.ownsDirectory = ownsDirectory
        command = directory.appendingPathComponent("rune-cloud-import.command")
        stdout = directory.appendingPathComponent(".rune-cloud-import.stdout")
        stderr = directory.appendingPathComponent(".rune-cloud-import.stderr")
        status = directory.appendingPathComponent(".rune-cloud-import.status")
        statusTemporary = directory.appendingPathComponent(".rune-cloud-import.status.tmp")
        cancel = directory.appendingPathComponent(".rune-cloud-import.cancel")
    }

    var helperFiles: [URL] {
        [command, stdout, stderr, status, statusTemporary, cancel]
    }

    func requestCancellation() {
        FileManager.default.createFile(atPath: cancel.path, contents: Data(), attributes: [
            .posixPermissions: 0o600
        ])
    }
}
