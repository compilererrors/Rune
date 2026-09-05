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
    private let outputByteLimit: Int

    init(
        terminationGracePeriod: TimeInterval = 2,
        externalCommandsAllowed: @escaping @Sendable () -> Bool = { RuneExternalCommandPolicy.allowsExternalCommands },
        outputByteLimit: Int = 1_048_576
    ) {
        self.terminationGracePeriod = terminationGracePeriod.isFinite ? max(0.025, terminationGracePeriod) : 2
        self.externalCommandsAllowed = externalCommandsAllowed
        self.outputByteLimit = min(max(outputByteLimit, 4), 1_048_576)
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
        try Task.checkCancellation()
        guard timeout.isFinite, timeout > 0 else {
            return ProcessCommandExecutionResult(exitCode: 124, stdout: "", stderr: "", timedOut: true)
        }
        let executionState = ProcessCommandExecutionState()
        return try await withTaskCancellationHandler {
            try await runProcess(
                executable: executable,
                arguments: arguments,
                environment: additionalEnvironment,
                timeout: timeout,
                onOutput: onOutput,
                executionState: executionState
            )
        } onCancel: {
            executionState.cancel()
        }
    }

    private func runProcess(
        executable: String,
        arguments: [String],
        environment additionalEnvironment: [String: String],
        timeout: TimeInterval,
        onOutput: @escaping @Sendable (ProcessCommandOutputChunk) -> Void,
        executionState: ProcessCommandExecutionState
    ) async throws -> ProcessCommandExecutionResult {
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
        process.standardInput = FileHandle.nullDevice
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe

        defer {
            try? stdoutPipe.fileHandleForReading.close()
            try? stderrPipe.fileHandleForReading.close()
            try? stdoutPipe.fileHandleForWriting.close()
            try? stderrPipe.fileHandleForWriting.close()
        }
        try makeNonblocking(stdoutPipe.fileHandleForReading)
        try makeNonblocking(stderrPipe.fileHandleForReading)
        var stdout = ProcessCommandOutputCapture(limit: outputByteLimit)
        var stderr = ProcessCommandOutputCapture(limit: outputByteLimit)

        try Task.checkCancellation()
        let deadline = ContinuousClock.now.advanced(by: .seconds(timeout))
        try process.run()
        let pid = process.processIdentifier
        // Foundation creates an isolated process group on macOS. Verify ownership
        // before using group signals; never signal Rune's own process group.
        let processGroup: pid_t? = getpgid(pid) == pid && pid != getpgrp() ? pid : nil
        while process.isRunning, ContinuousClock.now < deadline, !executionState.isCancelled {
            stdout.drain(stdoutPipe.fileHandleForReading, stream: .stdout, onOutput: onOutput)
            stderr.drain(stderrPipe.fileHandleForReading, stream: .stderr, onOutput: onOutput)
            try? await Task.sleep(nanoseconds: 50_000_000)
        }
        let didCancel = executionState.isCancelled || Task.isCancelled
        let didTimeout = process.isRunning && !didCancel
        let terminationOutput: @Sendable (ProcessCommandOutputChunk) -> Void = { chunk in
            if !didCancel { onOutput(chunk) }
        }
        let active = { process.isRunning || processGroup.map { kill(-$0, 0) == 0 } == true }
        let signal: (Int32) -> Void = { signal in
            if let processGroup {
                kill(-processGroup, signal)
            } else if process.isRunning {
                kill(pid, signal)
            }
        }
        if active() {
            signal(SIGTERM)
            let terminationDeadline = ContinuousClock.now.advanced(by: .seconds(terminationGracePeriod))
            while active(), ContinuousClock.now < terminationDeadline {
                stdout.drain(stdoutPipe.fileHandleForReading, stream: .stdout, onOutput: terminationOutput)
                stderr.drain(stderrPipe.fileHandleForReading, stream: .stderr, onOutput: terminationOutput)
                await cancellationResistantSleep(nanoseconds: 25_000_000)
            }
            if active() {
                signal(SIGKILL)
                let killDeadline = ContinuousClock.now.advanced(by: .seconds(terminationGracePeriod))
                while active(), ContinuousClock.now < killDeadline {
                    await cancellationResistantSleep(nanoseconds: 25_000_000)
                }
            }
        }
        guard !process.isRunning else {
            if executionState.isCancelled || Task.isCancelled {
                throw CancellationError()
            }
            throw RuneError.commandFailed(
                command: executable,
                message: "The command did not terminate after its timeout."
            )
        }
        if executionState.isCancelled || Task.isCancelled {
            throw CancellationError()
        }

        // A descendant can inherit a pipe after its parent exits. Read only
        // available bytes with a finite budget; waiting for EOF can hang forever.
        stdout.drain(stdoutPipe.fileHandleForReading, stream: .stdout, final: true, onOutput: onOutput)
        stderr.drain(stderrPipe.fileHandleForReading, stream: .stderr, final: true, onOutput: onOutput)
        try Task.checkCancellation()

        return ProcessCommandExecutionResult(
            exitCode: process.terminationStatus,
            stdout: stdout.text,
            stderr: stderr.text,
            timedOut: didTimeout
        )
    }

    private func makeNonblocking(_ handle: FileHandle) throws {
        let flags = fcntl(handle.fileDescriptor, F_GETFL)
        guard flags >= 0, fcntl(handle.fileDescriptor, F_SETFL, flags | O_NONBLOCK) == 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
    }

    private func cancellationResistantSleep(nanoseconds: UInt64) async {
        await Task.detached {
            try? await Task.sleep(nanoseconds: nanoseconds)
        }.value
    }
}

private final class ProcessCommandExecutionState: @unchecked Sendable {
    private let lock = NSLock()
    private var cancelled = false

    var isCancelled: Bool {
        lock.lock()
        defer { lock.unlock() }
        return cancelled
    }

    func cancel() {
        lock.lock()
        cancelled = true
        lock.unlock()
    }
}

private struct ProcessCommandOutputCapture {
    private let limit: Int
    private var capturedBytes = 0
    private var pendingUTF8 = Data()
    private var truncated = false
    private(set) var text = ""

    init(limit: Int) {
        self.limit = limit
    }

    mutating func drain(
        _ handle: FileHandle,
        stream: ProcessCommandOutputStream,
        final: Bool = false,
        onOutput: @escaping @Sendable (ProcessCommandOutputChunk) -> Void
    ) {
        var buffer = [UInt8](repeating: 0, count: 16_384)
        // Even continuously writing descendants cannot monopolize the executor
        // or bypass its timeout. Retention and draining have separate bounds.
        for _ in 0..<(final ? 64 : 16) {
            let count = buffer.withUnsafeMutableBytes {
                read(handle.fileDescriptor, $0.baseAddress, $0.count)
            }
            if count < 0, errno == EINTR { continue }
            guard count > 0 else { break }
            guard !truncated else { continue }
            let accepted = min(count, limit - capturedBytes)
            pendingUTF8.append(contentsOf: buffer.prefix(accepted))
            capturedBytes += accepted
            let split = completeUTF8PrefixLength(pendingUTF8)
            emit(String(decoding: pendingUTF8.prefix(split), as: UTF8.self), stream: stream, onOutput: onOutput)
            pendingUTF8 = Data(pendingUTF8.dropFirst(split))
            if count > accepted {
                truncated = true
                pendingUTF8.removeAll()
                emit("\n[Additional command output omitted.]\n", stream: stream, onOutput: onOutput)
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
        stream: ProcessCommandOutputStream,
        onOutput: @escaping @Sendable (ProcessCommandOutputChunk) -> Void
    ) {
        guard !value.isEmpty else { return }
        text += value
        onOutput(ProcessCommandOutputChunk(stream: stream, text: value))
    }
}
