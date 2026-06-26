import Foundation
import RuneCore
#if os(macOS) || os(iOS) || os(tvOS) || os(watchOS)
import Darwin
#else
import Glibc
#endif

struct ProcessCommandExecutionResult: Sendable, Equatable {
    let exitCode: Int32
    let stdout: String
    let stderr: String
    let timedOut: Bool
}

enum ProcessCommandOutputStream: Sendable, Equatable {
    case stdout
    case stderr
}

struct ProcessCommandOutputChunk: Sendable, Equatable {
    let stream: ProcessCommandOutputStream
    let text: String
}

protocol ProcessCommandExecuting: Sendable {
    func run(
        executable: String,
        arguments: [String],
        environment additionalEnvironment: [String: String],
        timeout: TimeInterval
    ) async throws -> ProcessCommandExecutionResult

    func run(
        executable: String,
        arguments: [String],
        environment additionalEnvironment: [String: String],
        timeout: TimeInterval,
        onOutput: @escaping @Sendable (ProcessCommandOutputChunk) -> Void
    ) async throws -> ProcessCommandExecutionResult
}

extension ProcessCommandExecuting {
    func run(
        executable: String,
        arguments: [String],
        environment additionalEnvironment: [String: String],
        timeout: TimeInterval,
        onOutput _: @escaping @Sendable (ProcessCommandOutputChunk) -> Void
    ) async throws -> ProcessCommandExecutionResult {
        try await run(
            executable: executable,
            arguments: arguments,
            environment: additionalEnvironment,
            timeout: timeout
        )
    }
}

struct ProcessCommandExecutor: ProcessCommandExecuting {
    private let terminationGracePeriod: TimeInterval
    private let externalCommandsAllowed: @Sendable () -> Bool

    init(
        terminationGracePeriod: TimeInterval = 2,
        externalCommandsAllowed: @escaping @Sendable () -> Bool = { RuneExternalCommandPolicy.allowsExternalCommands }
    ) {
        self.terminationGracePeriod = terminationGracePeriod
        self.externalCommandsAllowed = externalCommandsAllowed
    }

    func run(
        executable: String,
        arguments: [String],
        environment additionalEnvironment: [String: String],
        timeout: TimeInterval
    ) async throws -> ProcessCommandExecutionResult {
        try await run(
            executable: executable,
            arguments: arguments,
            environment: additionalEnvironment,
            timeout: timeout,
            onOutput: { _ in }
        )
    }

    func run(
        executable: String,
        arguments: [String],
        environment additionalEnvironment: [String: String],
        timeout: TimeInterval,
        onOutput: @escaping @Sendable (ProcessCommandOutputChunk) -> Void
    ) async throws -> ProcessCommandExecutionResult {
        guard externalCommandsAllowed() else {
            throw RuneError.invalidInput(message: RuneExternalCommandPolicy.disabledMessage)
        }
        return try await Task.detached(priority: .userInitiated) {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
            process.arguments = [executable] + arguments

            var environment = ProcessInfo.processInfo.environment
            environment["PATH"] = RuneExecutableSearchPath.pathValue(from: environment)
            for (key, value) in additionalEnvironment {
                environment[key] = value
            }
            process.environment = environment

            let stdoutPipe = Pipe()
            let stderrPipe = Pipe()
            process.standardOutput = stdoutPipe
            process.standardError = stderrPipe

            let outputCapture = ProcessCommandOutputCapture()

            stdoutPipe.fileHandleForReading.readabilityHandler = { handle in
                outputCapture.append(
                    data: handle.availableData,
                    stream: .stdout,
                    onOutput: onOutput
                )
            }
            stderrPipe.fileHandleForReading.readabilityHandler = { handle in
                outputCapture.append(
                    data: handle.availableData,
                    stream: .stderr,
                    onOutput: onOutput
                )
            }
            defer {
                stdoutPipe.fileHandleForReading.readabilityHandler = nil
                stderrPipe.fileHandleForReading.readabilityHandler = nil
            }

            try process.run()
            let deadline = Date().addingTimeInterval(timeout)
            while process.isRunning, Date() < deadline {
                try await Task.sleep(nanoseconds: 50_000_000)
            }
            let didTimeout = process.isRunning
            if process.isRunning {
                process.terminate()
                let terminationDeadline = Date().addingTimeInterval(terminationGracePeriod)
                while process.isRunning, Date() < terminationDeadline {
                    try await Task.sleep(nanoseconds: 25_000_000)
                }
                if process.isRunning {
                    kill(process.processIdentifier, SIGKILL)
                }
            }
            process.waitUntilExit()

            outputCapture.append(
                data: stdoutPipe.fileHandleForReading.readDataToEndOfFile(),
                stream: .stdout,
                onOutput: onOutput
            )
            outputCapture.append(
                data: stderrPipe.fileHandleForReading.readDataToEndOfFile(),
                stream: .stderr,
                onOutput: onOutput
            )
            let captured = outputCapture.snapshot()

            return ProcessCommandExecutionResult(
                exitCode: process.terminationStatus,
                stdout: captured.stdout,
                stderr: captured.stderr,
                timedOut: didTimeout
            )
        }.value
    }
}

private final class ProcessCommandOutputCapture: @unchecked Sendable {
    private let lock = NSLock()
    private var stdout = ""
    private var stderr = ""

    func append(
        data: Data,
        stream: ProcessCommandOutputStream,
        onOutput: @escaping @Sendable (ProcessCommandOutputChunk) -> Void
    ) {
        guard !data.isEmpty,
              let text = String(data: data, encoding: .utf8),
              !text.isEmpty else {
            return
        }
        lock.lock()
        switch stream {
        case .stdout:
            stdout.append(text)
        case .stderr:
            stderr.append(text)
        }
        lock.unlock()
        onOutput(ProcessCommandOutputChunk(stream: stream, text: text))
    }

    func snapshot() -> (stdout: String, stderr: String) {
        lock.lock()
        defer { lock.unlock() }
        return (stdout, stderr)
    }
}
