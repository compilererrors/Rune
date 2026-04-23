import Foundation
import OSLog
import RuneCore
import RuneDiagnostics

private func mirrorCommandNSLog(_ message: String) {
    guard UserDefaults.standard.runeDiagnosticsLogging else { return }
    NSLog("[Rune][Command] %@", message)
}

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
        environment: [String: String],
        timeout: TimeInterval?
    ) async throws -> CommandResult
}

public extension CommandRunning {
    func run(
        executable: String,
        arguments: [String],
        environment: [String: String]
    ) async throws -> CommandResult {
        try await run(
            executable: executable,
            arguments: arguments,
            environment: environment,
            timeout: nil
        )
    }
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
        environment: [String: String] = [:],
        timeout: TimeInterval? = nil
    ) async throws -> CommandResult {
        let command = renderCommand(executable: executable, arguments: arguments)
        let timeoutValue = timeout.map { max(1, $0) }
        let state = ProcessCommandState()
        let started = Date()

        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let process = Process()
                state.setProcess(process)
                process.executableURL = URL(fileURLWithPath: executable)
                process.arguments = arguments
                process.environment = mergedEnvironment(overrides: environment)

                commandLogger.log("Starting command: \(command, privacy: .public)")
                mirrorCommandNSLog("Starting command: \(command)")

                let stdoutPipe = Pipe()
                let stderrPipe = Pipe()
                process.standardOutput = stdoutPipe
                process.standardError = stderrPipe
                let stdoutBuffer = ProcessOutputBuffer()
                let stderrBuffer = ProcessOutputBuffer()

                stdoutPipe.fileHandleForReading.readabilityHandler = { handle in
                    let data = handle.availableData
                    if !data.isEmpty {
                        stdoutBuffer.append(data)
                    }
                }
                stderrPipe.fileHandleForReading.readabilityHandler = { handle in
                    let data = handle.availableData
                    if !data.isEmpty {
                        stderrBuffer.append(data)
                    }
                }

                if let timeoutValue {
                    let timeoutItem = DispatchWorkItem {
                        guard let activeProcess = state.process(), activeProcess.isRunning else { return }

                        commandLogger.error("Command timed out after \(timeoutValue, privacy: .public)s: \(command, privacy: .public)")
                        mirrorCommandNSLog("Command timed out after \(timeoutValue)s: \(command)")
                        activeProcess.terminate()
                        let err = RuneError.commandFailed(
                            command: command,
                            message: "Timed out after \(Int(timeoutValue)) seconds"
                        )
                        K8sWireTrace.logKubectlCompletion(
                            command: command,
                            timeout: timeout,
                            duration: Date().timeIntervalSince(started),
                            exitCode: nil,
                            stdoutBytes: nil,
                            stderrBytes: nil,
                            error: err
                        )
                        state.resume(
                            continuation,
                            result: .failure(err)
                        )
                    }
                    state.setTimeoutItem(timeoutItem)
                    DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + timeoutValue, execute: timeoutItem)
                }

                process.terminationHandler = { process in
                    state.cancelTimeoutItem()
                    stdoutPipe.fileHandleForReading.readabilityHandler = nil
                    stderrPipe.fileHandleForReading.readabilityHandler = nil
                    stdoutBuffer.append(stdoutPipe.fileHandleForReading.readDataToEndOfFile())
                    stderrBuffer.append(stderrPipe.fileHandleForReading.readDataToEndOfFile())
                    let stdoutData = stdoutBuffer.data()
                    let stderrData = stderrBuffer.data()
                    if state.hasAlreadyResumed() {
                        return
                    }

                    let stdout = String(data: stdoutData, encoding: .utf8) ?? ""
                    let stderr = String(data: stderrData, encoding: .utf8) ?? ""
                    let duration = Date().timeIntervalSince(started)

                    if state.takeTerminatedByCancellation() {
                        commandLogger.log("Command cancelled: \(command, privacy: .public)")
                        mirrorCommandNSLog("Command cancelled: \(command)")
                        let cancel = CancellationError()
                        K8sWireTrace.logKubectlCompletion(
                            command: command,
                            timeout: timeout,
                            duration: duration,
                            exitCode: process.terminationStatus,
                            stdoutBytes: stdout.utf8.count,
                            stderrBytes: stderr.utf8.count,
                            error: cancel
                        )
                        state.resume(continuation, result: .failure(cancel))
                        return
                    }

                    let result = CommandResult(stdout: stdout, stderr: stderr, exitCode: process.terminationStatus)

                    commandLogger.log("Finished command (exit \(process.terminationStatus)): \(command, privacy: .public)")
                    mirrorCommandNSLog("Finished command (exit \(process.terminationStatus)): \(command)")
                    if process.terminationStatus != 0 {
                        commandLogger.error("Command stderr: \(stderr, privacy: .public)")
                        mirrorCommandNSLog("Command stderr: \(stderr)")
                    }

                    K8sWireTrace.logKubectlCompletion(
                        command: command,
                        timeout: timeout,
                        duration: duration,
                        exitCode: result.exitCode,
                        stdoutBytes: result.stdout.utf8.count,
                        stderrBytes: result.stderr.utf8.count,
                        error: nil
                    )

                    state.resume(continuation, result: .success(result))
                }

                do {
                    try process.run()
                } catch {
                    state.cancelTimeoutItem()
                    stdoutPipe.fileHandleForReading.readabilityHandler = nil
                    stderrPipe.fileHandleForReading.readabilityHandler = nil
                    commandLogger.error("Failed to start command: \(command, privacy: .public) :: \(error.localizedDescription, privacy: .public)")
                    mirrorCommandNSLog("Failed to start command: \(command) :: \(error.localizedDescription)")
                    K8sWireTrace.logKubectlCompletion(
                        command: command,
                        timeout: timeout,
                        duration: Date().timeIntervalSince(started),
                        exitCode: nil,
                        stdoutBytes: nil,
                        stderrBytes: nil,
                        error: error
                    )
                    state.resume(
                        continuation,
                        result: .failure(
                            RuneError.commandFailed(
                                command: executable,
                                message: error.localizedDescription
                            )
                        )
                    )
                }
            }
        } onCancel: {
            state.markTerminatedByCancellation()
            state.cancelTimeoutItem()
            if let process = state.process(), process.isRunning {
                process.terminate()
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

        process.environment = mergedEnvironment(overrides: environment)
        let renderedLong = renderCommand(executable: executable, arguments: arguments)
        commandLogger.log("Starting long-running command: \(renderedLong, privacy: .public)")
        mirrorCommandNSLog("Starting long-running command: \(renderedLong)")

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
            let renderedEnd = renderCommand(executable: executable, arguments: arguments)
            commandLogger.log(
                "Long-running command ended (exit \(process.terminationStatus)): \(renderedEnd, privacy: .public)"
            )
            mirrorCommandNSLog("Long-running command ended (exit \(process.terminationStatus)): \(renderedEnd)")
            onTermination(process.terminationStatus)
        }

        do {
            try process.run()
        } catch {
            stdoutPipe.fileHandleForReading.readabilityHandler = nil
            stderrPipe.fileHandleForReading.readabilityHandler = nil
            let renderedFail = renderCommand(executable: executable, arguments: arguments)
            commandLogger.error("Failed to start long-running command: \(renderedFail, privacy: .public) :: \(error.localizedDescription, privacy: .public)")
            mirrorCommandNSLog("Failed to start long-running command: \(renderedFail) :: \(error.localizedDescription)")
            throw RuneError.commandFailed(command: executable, message: error.localizedDescription)
        }

        return ProcessCommandHandle(process: process)
    }
}

private final class ProcessOutputBuffer: @unchecked Sendable {
    private let lock = NSLock()
    private var storage = Data()

    func append(_ data: Data) {
        guard !data.isEmpty else { return }
        lock.lock()
        storage.append(data)
        lock.unlock()
    }

    func data() -> Data {
        lock.lock()
        let value = storage
        lock.unlock()
        return value
    }
}

private final class ProcessCommandState: @unchecked Sendable {
    private let lock = NSLock()
    private var processRef: Process?
    private var timeoutItem: DispatchWorkItem?
    private var didResume = false
    private var terminatedByCancellation = false

    func setProcess(_ process: Process) {
        lock.lock()
        processRef = process
        lock.unlock()
    }

    func markTerminatedByCancellation() {
        lock.lock()
        terminatedByCancellation = true
        lock.unlock()
    }

    func takeTerminatedByCancellation() -> Bool {
        lock.lock()
        let v = terminatedByCancellation
        terminatedByCancellation = false
        lock.unlock()
        return v
    }

    func hasAlreadyResumed() -> Bool {
        lock.lock()
        let v = didResume
        lock.unlock()
        return v
    }

    func process() -> Process? {
        lock.lock()
        let process = processRef
        lock.unlock()
        return process
    }

    func setTimeoutItem(_ item: DispatchWorkItem) {
        lock.lock()
        timeoutItem = item
        lock.unlock()
    }

    func cancelTimeoutItem() {
        lock.lock()
        let item = timeoutItem
        timeoutItem = nil
        lock.unlock()
        item?.cancel()
    }

    func resume(
        _ continuation: CheckedContinuation<CommandResult, Error>,
        result: Result<CommandResult, Error>
    ) {
        lock.lock()
        let shouldResume = !didResume
        didResume = true
        lock.unlock()

        guard shouldResume else { return }
        continuation.resume(with: result)
    }
}

private func mergedEnvironment(overrides: [String: String]) -> [String: String] {
    var environment = ProcessInfo.processInfo.environment
    overrides.forEach { key, value in
        environment[key] = value
    }
    environment["PATH"] = augmentedPath(current: environment["PATH"])
    return environment
}

private func augmentedPath(current: String?) -> String {
    let preferredSegments = [
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin"
    ]

    var orderedSegments = (current ?? "")
        .split(separator: ":")
        .map(String.init)
        .filter { !$0.isEmpty }

    var seen = Set(orderedSegments)
    for segment in preferredSegments where !seen.contains(segment) {
        orderedSegments.append(segment)
        seen.insert(segment)
    }

    return orderedSegments.joined(separator: ":")
}

private func renderCommand(executable: String, arguments: [String]) -> String {
    ([executable] + arguments).joined(separator: " ")
}

private let commandLogger = Logger(subsystem: "com.rune.desktop", category: "CommandRunner")

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
