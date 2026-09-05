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
    private let outputByteLimit: Int

    init(
        launcher: any TerminalCommandDocumentLaunching = WorkspaceTerminalCommandDocumentLauncher(),
        pollNanoseconds: UInt64 = 100_000_000,
        cliSearchPath: String = Self.defaultCLISearchPath,
        outputByteLimit: Int = 1_048_576
    ) {
        self.launcher = launcher
        self.pollNanoseconds = pollNanoseconds
        self.cliSearchPath = cliSearchPath
        self.outputByteLimit = min(max(outputByteLimit, 4), 1_048_576)
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
        guard timeout.isFinite, timeout > 0 else {
            return CloudKubeConfigCommandResult(exitCode: 124, stdout: "", stderr: "", timedOut: true)
        }
        let deadline = ContinuousClock.now.advanced(by: .seconds(timeout))

        let files = try executionFiles(for: command)
        do {
            try prepare(files: files, command: command)
        } catch {
            cleanup(files: files)
            throw error
        }

        return try await withTaskCancellationHandler {
            let launchState = TerminalCloudImportLaunchState()
            let launchTask = Task {
                do {
                    try Task.checkCancellation()
                    guard !files.isCancellationRequested else { return }
                    try await launcher.launch(commandFileURL: files.command)
                } catch {
                    launchState.markFailed()
                }
            }
            defer {
                launchTask.cancel()
                cleanup(files: files)
            }
            do {
                return try await waitForResult(
                    files: files,
                    command: command,
                    launchState: launchState,
                    deadline: deadline,
                    onOutput: onOutput
                )
            } catch {
                files.requestCancellation()
                await waitForCancellation(files: files)
                throw error
            }
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
            return try makeExecutionFiles(in: FileManager.default.temporaryDirectory)
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
        return try makeExecutionFiles(in: directory)
    }

    private func makeExecutionFiles(in parent: URL) throws -> TerminalCloudImportExecutionFiles {
        let directory = parent.appendingPathComponent(".rune-cloud-import.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: false,
            attributes: [.posixPermissions: 0o700]
        )
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
        work_dir=\(shellQuote(files.directory.path))
        [[ -d "$work_dir" ]] || exit 130
        export PATH=\(shellQuote(cliSearchPath))
        \(environment)
        stdout_file=\(shellQuote(files.stdout.path))
        stderr_file=\(shellQuote(files.stderr.path))
        status_file=\(shellQuote(files.status.path))
        status_tmp=\(shellQuote(files.statusTemporary.path))
        cancel_file=\(shellQuote(files.cancel.path))
        stdout_pipe=\(shellQuote(files.stdoutPipe.path))
        stderr_pipe=\(shellQuote(files.stderrPipe.path))
        child_pid=''
        stdout_pid=''
        stderr_pid=''
        capture_output() {
          local remaining=\(outputByteLimit + 1) count
          while (( remaining > 0 )); do
            sysread -s "$(( remaining < 16384 ? remaining : 16384 ))" -c count -o 1 || return
            (( remaining -= count ))
          done
          /bin/cat > /dev/null
        }
        # Terminal provides a controlling TTY. Job control gives the provider and
        # all of its descendants their own process group for bounded shutdown.
        stop_child() {
          if [[ -n "$child_pid" ]]; then
            if kill -TERM -- -"$child_pid" 2>/dev/null; then
              /bin/sleep 0.2
              kill -KILL -- -"$child_pid" 2>/dev/null
            fi
            wait "$child_pid" 2>/dev/null
          fi
        }
        finish() {
          local result="$1"
          [[ -n "$stdout_pid" ]] && wait "$stdout_pid" 2>/dev/null
          [[ -n "$stderr_pid" ]] && wait "$stderr_pid" 2>/dev/null
          /bin/rm -f "$stdout_pipe" "$stderr_pipe"
          if [[ -d "$work_dir" ]]; then
            print -r -- "$result" > "$status_tmp"
            /bin/mv -f "$status_tmp" "$status_file"
          fi
          exit "$result"
        }
        trap 'stop_child; finish 130' HUP INT TERM
        : > "$stdout_file"
        : > "$stderr_file"
        /bin/rm -f "$status_file" "$status_tmp"
        if [[ -e "$cancel_file" ]]; then
          finish 130
        fi
        if ! setopt MONITOR NO_BG_NICE 2>/dev/null || ! zmodload zsh/system; then
          print -r -- 'Terminal could not create an isolated command job.' > "$stderr_file"
          finish 125
        fi
        if ! command -v \(executable) >/dev/null 2>&1; then
          print -r -- "Required provider CLI not found: \(command.executable)" > "$stderr_file"
          finish 127
        fi
        if ! /usr/bin/mkfifo "$stdout_pipe" "$stderr_pipe"; then
          print -r -- 'Terminal could not prepare command output.' > "$stderr_file"
          finish 125
        fi
        # Keep one extra byte to detect truncation. Drain everything beyond the
        # cap so a verbose CLI cannot fill the disk or fail with a broken pipe.
        capture_output > "$stdout_file" < "$stdout_pipe" &
        stdout_pid=$!
        capture_output > "$stderr_file" < "$stderr_pipe" &
        stderr_pid=$!
        if [[ ! -d "$work_dir" || -e "$cancel_file" ]]; then
          kill -TERM -- -"$stdout_pid" -"$stderr_pid" 2>/dev/null
          finish 130
        fi
        \(executable) \(arguments) > "$stdout_pipe" 2> "$stderr_pipe" &
        child_pid=$!
        while kill -0 "$child_pid" 2>/dev/null; do
          if [[ ! -d "$work_dir" || -e "$cancel_file" ]]; then
            stop_child
            finish 130
          fi
          /bin/sleep 0.1
        done
        wait "$child_pid"
        exit_code=$?
        stop_child
        finish "$exit_code"
        """
    }

    private func shellQuote(_ value: String) -> String {
        "'" + value.replacingOccurrences(of: "'", with: "'\"'\"'") + "'"
    }

    private func waitForResult(
        files: TerminalCloudImportExecutionFiles,
        command: CloudKubeConfigCommandPreview,
        launchState: TerminalCloudImportLaunchState,
        deadline: ContinuousClock.Instant,
        onOutput: @escaping @Sendable (CloudKubeConfigCommandOutput) -> Void
    ) async throws -> CloudKubeConfigCommandResult {
        var stdout = TerminalCloudImportOutputReader(limit: outputByteLimit)
        var stderr = TerminalCloudImportOutputReader(limit: outputByteLimit)

        while ContinuousClock.now < deadline {
            try Task.checkCancellation()
            if let exitCode = readExitCode(from: files.status) {
                stdout.read(from: files.stdout, stream: .stdout, final: true, onOutput: onOutput)
                stderr.read(from: files.stderr, stream: .stderr, final: true, onOutput: onOutput)
                return CloudKubeConfigCommandResult(
                    exitCode: exitCode,
                    stdout: stdout.text,
                    stderr: stderr.text,
                    timedOut: false
                )
            }
            if launchState.hasFailed {
                throw RuneError.commandFailed(
                    command: command.executable,
                    message: "Terminal could not start the cloud import command."
                )
            }
            stdout.read(from: files.stdout, stream: .stdout, onOutput: onOutput)
            stderr.read(from: files.stderr, stream: .stderr, onOutput: onOutput)
            try await Task.sleep(for: min(.nanoseconds(pollNanoseconds), ContinuousClock.now.duration(to: deadline)))
        }

        try Task.checkCancellation()
        files.requestCancellation()
        await waitForCancellation(files: files)
        try Task.checkCancellation()
        stdout.read(from: files.stdout, stream: .stdout, final: true, onOutput: onOutput)
        stderr.read(from: files.stderr, stream: .stderr, final: true, onOutput: onOutput)
        return CloudKubeConfigCommandResult(
            exitCode: 124,
            stdout: stdout.text,
            stderr: stderr.text,
            timedOut: true
        )
    }

    private func waitForCancellation(files: TerminalCloudImportExecutionFiles) async {
        // A cancelled task cannot use Task.sleep itself: it immediately throws.
        // Give Terminal time to acknowledge TERM/KILL before removing its files.
        await Task.detached {
            let deadline = ContinuousClock.now.advanced(by: .seconds(1))
            while ContinuousClock.now < deadline, readExitCode(from: files.status) == nil {
                try? await Task.sleep(for: .milliseconds(20))
            }
        }.value
    }

    private func readExitCode(from url: URL) -> Int32? {
        guard let handle = try? FileHandle(forReadingFrom: url) else { return nil }
        defer { try? handle.close() }
        guard let data = try? handle.read(upToCount: 32),
              let raw = String(data: data, encoding: .utf8),
              let value = Int32(raw.trimmingCharacters(in: .whitespacesAndNewlines)) else {
            return nil
        }
        return value
    }

    private func cleanup(files: TerminalCloudImportExecutionFiles) {
        // Each invocation owns a unique directory. Removing it also makes an
        // already queued Terminal document fail closed before starting the CLI.
        try? FileManager.default.removeItem(at: files.directory)
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
    let stdoutPipe: URL
    let stderrPipe: URL

    init(directory: URL) {
        self.directory = directory
        command = directory.appendingPathComponent("rune-cloud-import.command")
        stdout = directory.appendingPathComponent(".rune-cloud-import.stdout")
        stderr = directory.appendingPathComponent(".rune-cloud-import.stderr")
        status = directory.appendingPathComponent(".rune-cloud-import.status")
        statusTemporary = directory.appendingPathComponent(".rune-cloud-import.status.tmp")
        cancel = directory.appendingPathComponent(".rune-cloud-import.cancel")
        stdoutPipe = directory.appendingPathComponent(".rune-cloud-import.stdout.pipe")
        stderrPipe = directory.appendingPathComponent(".rune-cloud-import.stderr.pipe")
    }

    var helperFiles: [URL] {
        [command, stdout, stderr, status, statusTemporary, cancel, stdoutPipe, stderrPipe]
    }

    var isCancellationRequested: Bool {
        !FileManager.default.fileExists(atPath: directory.path)
            || FileManager.default.fileExists(atPath: cancel.path)
    }

    func requestCancellation() {
        FileManager.default.createFile(atPath: cancel.path, contents: Data(), attributes: [
            .posixPermissions: 0o600
        ])
    }
}

private final class TerminalCloudImportLaunchState: @unchecked Sendable {
    private let lock = NSLock()
    private var failed = false

    var hasFailed: Bool {
        lock.withLock { failed }
    }

    func markFailed() {
        lock.withLock { failed = true }
    }
}

private struct TerminalCloudImportOutputReader {
    private static let truncationMessage = "\n[Additional command output omitted.]\n"
    let limit: Int
    private var offset = 0
    private var pendingUTF8 = Data()
    private var truncated = false
    private(set) var text = ""

    init(limit: Int) {
        self.limit = limit
    }

    mutating func read(
        from url: URL,
        stream: CloudKubeConfigCommandOutputStream,
        final: Bool = false,
        onOutput: @escaping @Sendable (CloudKubeConfigCommandOutput) -> Void
    ) {
        if !truncated, let handle = try? FileHandle(forReadingFrom: url) {
            defer { try? handle.close() }
            try? handle.seek(toOffset: UInt64(offset))
            while offset <= limit,
                  let data = try? handle.read(upToCount: min(16_384, limit + 1 - offset)),
                  !data.isEmpty {
                let accepted = min(data.count, limit - offset)
                pendingUTF8.append(data.prefix(accepted))
                offset += data.count
                let split = completeUTF8PrefixLength(pendingUTF8)
                emit(String(decoding: pendingUTF8.prefix(split), as: UTF8.self), stream: stream, onOutput: onOutput)
                pendingUTF8 = Data(pendingUTF8.dropFirst(split))
                if offset > limit {
                    truncated = true
                    // Do not produce a replacement character for a valid scalar
                    // whose final bytes happened to fall beyond the capture cap.
                    pendingUTF8.removeAll()
                    emit(Self.truncationMessage, stream: stream, onOutput: onOutput)
                }
            }
        }
        if final, !pendingUTF8.isEmpty {
            emit(String(decoding: pendingUTF8, as: UTF8.self), stream: stream, onOutput: onOutput)
            pendingUTF8.removeAll()
        }
    }

    private func completeUTF8PrefixLength(_ data: Data) -> Int {
        let count = data.count
        for index in stride(from: count - 1, through: max(0, count - 4), by: -1) {
            let byte = data[index]
            if byte & 0xC0 == 0x80 { continue }
            let length: Int
            switch byte {
            case 0xC2...0xDF: length = 2
            case 0xE0...0xEF: length = 3
            case 0xF0...0xF4: length = 4
            default: length = 1
            }
            return count - index < length ? index : count
        }
        return count
    }

    private mutating func emit(
        _ value: String,
        stream: CloudKubeConfigCommandOutputStream,
        onOutput: @escaping @Sendable (CloudKubeConfigCommandOutput) -> Void
    ) {
        guard !value.isEmpty else { return }
        text += value
        onOutput(CloudKubeConfigCommandOutput(stream: stream, text: value))
    }
}
