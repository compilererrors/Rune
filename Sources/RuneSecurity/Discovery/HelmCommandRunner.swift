import Foundation
import RuneCore

public struct HelmRollbackRequest: Sendable, Equatable {
    public let sources: [KubeConfigSource]
    public let contextName: String
    public let namespace: String
    public let releaseName: String
    public let revision: Int
    public let wait: Bool
    public let timeout: String
    public let cleanupOnFail: Bool
    public let dryRun: Bool

    public init(
        sources: [KubeConfigSource],
        contextName: String,
        namespace: String,
        releaseName: String,
        revision: Int,
        wait: Bool,
        timeout: String,
        cleanupOnFail: Bool,
        dryRun: Bool
    ) {
        self.sources = sources
        self.contextName = contextName
        self.namespace = namespace
        self.releaseName = releaseName
        self.revision = revision
        self.wait = wait
        self.timeout = timeout
        self.cleanupOnFail = cleanupOnFail
        self.dryRun = dryRun
    }
}

public struct HelmCommandResult: Sendable, Equatable {
    public let exitCode: Int32
    public let stdout: String
    public let stderr: String
    public let timedOut: Bool

    public init(exitCode: Int32, stdout: String, stderr: String, timedOut: Bool = false) {
        self.exitCode = exitCode
        self.stdout = stdout
        self.stderr = stderr
        self.timedOut = timedOut
    }
}

public struct HelmCommandPreview: Sendable, Equatable {
    public let executable: String
    public let arguments: [String]
    public let displayCommand: String
    public let environment: [String: String]

    public init(
        executable: String,
        arguments: [String],
        displayCommand: String,
        environment: [String: String]
    ) {
        self.executable = executable
        self.arguments = arguments
        self.displayCommand = displayCommand
        self.environment = environment
    }
}

public enum HelmCommandError: Error, LocalizedError, Sendable, Equatable {
    case missingKubeConfig
    case failed(exitCode: Int32, message: String)
    case timedOut(timeoutSeconds: Int, message: String)

    public var errorDescription: String? {
        switch self {
        case .missingKubeConfig:
            return "No kubeconfig source is loaded."
        case let .failed(exitCode, message):
            return "Helm command failed with exit code \(exitCode). \(message)"
        case let .timedOut(timeoutSeconds, message):
            let suffix = message.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "" : " \(message)"
            return "Helm command timed out after \(timeoutSeconds) seconds.\(suffix)"
        }
    }
}

public protocol HelmCommandRunning: Sendable {
    func rollback(_ request: HelmRollbackRequest, timeout: TimeInterval) async throws -> HelmCommandResult
}

public struct HelmRollbackCommandBuilder: Sendable {
    public init() {}

    public func preview(for request: HelmRollbackRequest) throws -> HelmCommandPreview {
        guard !request.sources.isEmpty else {
            throw HelmCommandError.missingKubeConfig
        }

        var arguments = [
            "--kube-context", request.contextName,
            "--namespace", request.namespace,
            "rollback",
            request.releaseName,
            "\(request.revision)"
        ]
        if request.dryRun {
            arguments.append("--dry-run")
        }
        if request.wait {
            arguments.append("--wait")
        }
        let trimmedTimeout = request.timeout.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmedTimeout.isEmpty {
            arguments.append("--timeout")
            arguments.append(trimmedTimeout)
        }
        if request.cleanupOnFail {
            arguments.append("--cleanup-on-fail")
        }

        let executable = "helm"
        let environment = ["KUBECONFIG": request.sources.map(\.url.path).joined(separator: ":")]
        return HelmCommandPreview(
            executable: executable,
            arguments: arguments,
            displayCommand: ShellCommandFormatting.displayCommand(executable: executable, arguments: arguments),
            environment: environment
        )
    }
}

public struct ProcessHelmCommandRunner: HelmCommandRunning {
    private let commandBuilder: HelmRollbackCommandBuilder
    private let executor: any ProcessCommandExecuting

    public init(commandBuilder: HelmRollbackCommandBuilder = HelmRollbackCommandBuilder()) {
        self.commandBuilder = commandBuilder
        self.executor = ProcessCommandExecutor()
    }

    init(commandBuilder: HelmRollbackCommandBuilder = HelmRollbackCommandBuilder(), executor: any ProcessCommandExecuting) {
        self.commandBuilder = commandBuilder
        self.executor = executor
    }

    public func rollback(_ request: HelmRollbackRequest, timeout: TimeInterval = 120) async throws -> HelmCommandResult {
        let command = try commandBuilder.preview(for: request)
        let result = try await executor.run(
            executable: command.executable,
            arguments: command.arguments,
            environment: command.environment,
            timeout: timeout
        )
        if result.timedOut {
            let message = result.stderr.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                : result.stderr.trimmingCharacters(in: .whitespacesAndNewlines)
            throw HelmCommandError.timedOut(timeoutSeconds: Int(timeout.rounded(.up)), message: message)
        }
        guard result.exitCode == 0 else {
            let message = result.stderr.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                : result.stderr.trimmingCharacters(in: .whitespacesAndNewlines)
            throw HelmCommandError.failed(exitCode: result.exitCode, message: message)
        }
        return HelmCommandResult(
            exitCode: result.exitCode,
            stdout: result.stdout,
            stderr: result.stderr,
            timedOut: result.timedOut
        )
    }
}
