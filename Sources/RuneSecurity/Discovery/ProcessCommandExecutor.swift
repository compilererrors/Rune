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

protocol ProcessCommandExecuting: Sendable {
    func run(
        executable: String,
        arguments: [String],
        environment additionalEnvironment: [String: String],
        timeout: TimeInterval
    ) async throws -> ProcessCommandExecutionResult
}

struct ProcessCommandExecutor: ProcessCommandExecuting {
    private let terminationGracePeriod: TimeInterval

    init(terminationGracePeriod: TimeInterval = 2) {
        self.terminationGracePeriod = terminationGracePeriod
    }

    func run(
        executable: String,
        arguments: [String],
        environment additionalEnvironment: [String: String],
        timeout: TimeInterval
    ) async throws -> ProcessCommandExecutionResult {
        try await Task.detached(priority: .userInitiated) {
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

            let stdout = String(data: stdoutPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
            let stderr = String(data: stderrPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
            return ProcessCommandExecutionResult(
                exitCode: process.terminationStatus,
                stdout: stdout,
                stderr: stderr,
                timedOut: didTimeout
            )
        }.value
    }
}
