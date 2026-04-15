import Foundation
import RuneCore

public struct CommandResult: Sendable {
    public let stdout: String
    public let stderr: String
    public let exitCode: Int32

    public init(stdout: String, stderr: String, exitCode: Int32) {
        self.stdout = stdout
        self.stderr = stderr
        self.exitCode = exitCode
    }
}

public protocol CommandRunning: Sendable {
    func run(
        executable: String,
        arguments: [String],
        environment: [String: String]
    ) async throws -> CommandResult
}

public protocol RunningCommandControlling: AnyObject, Sendable {
    var id: UUID { get }
    func terminate()
}

public protocol LongRunningCommandRunning: Sendable {
    func start(
        executable: String,
        arguments: [String],
        environment: [String: String],
        onStdout: @escaping @Sendable (String) -> Void,
        onStderr: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) -> Void
    ) throws -> any RunningCommandControlling
}

public final class ProcessCommandRunner: CommandRunning {
    public init() {}

    public func run(
        executable: String,
        arguments: [String],
        environment: [String: String] = [:]
    ) async throws -> CommandResult {
        try await withCheckedThrowingContinuation { continuation in
            let process = Process()
            process.executableURL = URL(fileURLWithPath: executable)
            process.arguments = arguments

            var mergedEnvironment = ProcessInfo.processInfo.environment
            environment.forEach { key, value in
                mergedEnvironment[key] = value
            }
            process.environment = mergedEnvironment

            let stdoutPipe = Pipe()
            let stderrPipe = Pipe()
            process.standardOutput = stdoutPipe
            process.standardError = stderrPipe

            process.terminationHandler = { _ in
                let stdout = String(data: stdoutPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
                let stderr = String(data: stderrPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
                let result = CommandResult(stdout: stdout, stderr: stderr, exitCode: process.terminationStatus)
                continuation.resume(returning: result)
            }

            do {
                try process.run()
            } catch {
                continuation.resume(throwing: RuneError.commandFailed(command: executable, message: error.localizedDescription))
            }
        }
    }
}

public final class ProcessLongRunningCommandRunner: LongRunningCommandRunning {
    public init() {}

    public func start(
        executable: String,
        arguments: [String],
        environment: [String: String] = [:],
        onStdout: @escaping @Sendable (String) -> Void,
        onStderr: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) -> Void
    ) throws -> any RunningCommandControlling {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments

        var mergedEnvironment = ProcessInfo.processInfo.environment
        environment.forEach { key, value in
            mergedEnvironment[key] = value
        }
        process.environment = mergedEnvironment

        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe

        stdoutPipe.fileHandleForReading.readabilityHandler = { handle in
            let data = handle.availableData
            guard !data.isEmpty, let text = String(data: data, encoding: .utf8) else { return }
            onStdout(text)
        }

        stderrPipe.fileHandleForReading.readabilityHandler = { handle in
            let data = handle.availableData
            guard !data.isEmpty, let text = String(data: data, encoding: .utf8) else { return }
            onStderr(text)
        }

        process.terminationHandler = { process in
            stdoutPipe.fileHandleForReading.readabilityHandler = nil
            stderrPipe.fileHandleForReading.readabilityHandler = nil
            onTermination(process.terminationStatus)
        }

        do {
            try process.run()
        } catch {
            stdoutPipe.fileHandleForReading.readabilityHandler = nil
            stderrPipe.fileHandleForReading.readabilityHandler = nil
            throw RuneError.commandFailed(command: executable, message: error.localizedDescription)
        }

        return ProcessCommandHandle(process: process)
    }
}

public final class ProcessCommandHandle: RunningCommandControlling, @unchecked Sendable {
    public let id = UUID()

    private let process: Process

    public init(process: Process) {
        self.process = process
    }

    public func terminate() {
        if process.isRunning {
            process.terminate()
        }
    }
}
