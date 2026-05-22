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

    public init(exitCode: Int32, stdout: String, stderr: String) {
        self.exitCode = exitCode
        self.stdout = stdout
        self.stderr = stderr
    }
}

public enum HelmCommandError: Error, LocalizedError, Sendable, Equatable {
    case missingKubeConfig
    case failed(exitCode: Int32, message: String)

    public var errorDescription: String? {
        switch self {
        case .missingKubeConfig:
            return "No kubeconfig source is loaded."
        case let .failed(exitCode, message):
            return "Helm command failed with exit code \(exitCode). \(message)"
        }
    }
}

public protocol HelmCommandRunning: Sendable {
    func rollback(_ request: HelmRollbackRequest, timeout: TimeInterval) async throws -> HelmCommandResult
}

public struct ProcessHelmCommandRunner: HelmCommandRunning {
    public init() {}

    public func rollback(_ request: HelmRollbackRequest, timeout: TimeInterval = 120) async throws -> HelmCommandResult {
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

        let result = try await runHelm(arguments: arguments, sources: request.sources, timeout: timeout)
        guard result.exitCode == 0 else {
            let message = result.stderr.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                : result.stderr.trimmingCharacters(in: .whitespacesAndNewlines)
            throw HelmCommandError.failed(exitCode: result.exitCode, message: message)
        }
        return result
    }

    private func runHelm(
        arguments: [String],
        sources: [KubeConfigSource],
        timeout: TimeInterval
    ) async throws -> HelmCommandResult {
        try await Task.detached(priority: .userInitiated) {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
            process.arguments = ["helm"] + arguments

            var environment = ProcessInfo.processInfo.environment
            environment["PATH"] = RuneExecutableSearchPath.pathValue(from: environment)
            environment["KUBECONFIG"] = sources.map(\.url.path).joined(separator: ":")
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
            if process.isRunning {
                process.terminate()
            }
            process.waitUntilExit()

            let stdout = String(data: stdoutPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
            let stderr = String(data: stderrPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
            return HelmCommandResult(exitCode: process.terminationStatus, stdout: stdout, stderr: stderr)
        }.value
    }
}
