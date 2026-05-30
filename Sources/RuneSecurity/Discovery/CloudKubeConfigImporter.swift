import Foundation
import RuneCore

public enum CloudKubeConfigProvider: String, Sendable, CaseIterable {
    case aks
    case eks
    case gke
}

public struct CloudKubeConfigImportRequest: Sendable, Equatable {
    public let provider: CloudKubeConfigProvider
    public let clusterName: String
    public let regionOrLocation: String
    public let resourceGroup: String
    public let projectID: String
    public let profileOrSubscription: String
    public let roleARN: String
    public let targetKubeconfigPath: String
    public let overwriteExisting: Bool

    public init(
        provider: CloudKubeConfigProvider,
        clusterName: String,
        regionOrLocation: String = "",
        resourceGroup: String = "",
        projectID: String = "",
        profileOrSubscription: String = "",
        roleARN: String = "",
        targetKubeconfigPath: String = "",
        overwriteExisting: Bool = true
    ) {
        self.provider = provider
        self.clusterName = clusterName
        self.regionOrLocation = regionOrLocation
        self.resourceGroup = resourceGroup
        self.projectID = projectID
        self.profileOrSubscription = profileOrSubscription
        self.roleARN = roleARN
        self.targetKubeconfigPath = targetKubeconfigPath
        self.overwriteExisting = overwriteExisting
    }
}

public struct CloudKubeConfigCommandPreview: Sendable, Equatable {
    public let executable: String
    public let arguments: [String]
    public let displayCommand: String
    public let environment: [String: String]

    public init(
        executable: String,
        arguments: [String],
        displayCommand: String,
        environment: [String: String] = [:]
    ) {
        self.executable = executable
        self.arguments = arguments
        self.displayCommand = displayCommand
        self.environment = environment
    }
}

public struct CloudKubeConfigCommandResult: Sendable, Equatable {
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

public struct CloudKubeConfigImportResult: Sendable, Equatable {
    public let command: CloudKubeConfigCommandPreview
    public let commandResult: CloudKubeConfigCommandResult
    public let discoveredURLs: [URL]
    public let reviews: [KubeConfigImportReview]
}

public enum CloudKubeConfigImportError: Error, LocalizedError, Sendable, Equatable {
    case missingRequiredField(String)
    case commandFailed(command: String, exitCode: Int32, message: String)
    case commandTimedOut(command: String, timeoutSeconds: Int, message: String)
    case noKubeconfigDiscovered(command: String)

    public var errorDescription: String? {
        switch self {
        case .missingRequiredField(let name):
            return "\(name) is required."
        case .commandFailed(let command, let exitCode, let message):
            return "Cloud import command failed with exit code \(exitCode): \(command). \(message)"
        case .commandTimedOut(let command, let timeoutSeconds, let message):
            let suffix = message.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "" : " \(message)"
            return "Cloud import command timed out after \(timeoutSeconds) seconds: \(command).\(suffix)"
        case .noKubeconfigDiscovered(let command):
            return "Cloud import command completed, but Rune could not find a kubeconfig to review: \(command)."
        }
    }
}

public protocol CloudKubeConfigCommandRunning: Sendable {
    func run(_ command: CloudKubeConfigCommandPreview, timeout: TimeInterval) async throws -> CloudKubeConfigCommandResult
}

public protocol CloudKubeConfigImporting: Sendable {
    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview
    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult
}

public struct CloudKubeConfigCLIImporter: CloudKubeConfigImporting {
    private let commandBuilder: CloudKubeConfigCommandBuilder
    private let runner: CloudKubeConfigCommandRunning
    private let discoverer: KubeConfigDiscovering
    private let validator: KubeConfigImportValidator
    private let timeout: TimeInterval

    public init(
        commandBuilder: CloudKubeConfigCommandBuilder = CloudKubeConfigCommandBuilder(),
        runner: CloudKubeConfigCommandRunning = ProcessCloudKubeConfigCommandRunner(),
        discoverer: KubeConfigDiscovering = KubeConfigDiscoverer(),
        validator: KubeConfigImportValidator = KubeConfigImportValidator(),
        timeout: TimeInterval = 120
    ) {
        self.commandBuilder = commandBuilder
        self.runner = runner
        self.discoverer = discoverer
        self.validator = validator
        self.timeout = timeout
    }

    public func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        try commandBuilder.preview(for: request)
    }

    public func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        let command = try commandPreview(for: request)
        let commandResult = try await runner.run(command, timeout: timeout)
        if commandResult.timedOut {
            throw CloudKubeConfigImportError.commandTimedOut(
                command: command.displayCommand,
                timeoutSeconds: Int(timeout.rounded(.up)),
                message: commandResult.stderr.isEmpty ? commandResult.stdout : commandResult.stderr
            )
        }
        guard commandResult.exitCode == 0 else {
            throw CloudKubeConfigImportError.commandFailed(
                command: command.displayCommand,
                exitCode: commandResult.exitCode,
                message: commandResult.stderr.isEmpty ? commandResult.stdout : commandResult.stderr
            )
        }

        let discoveredURLs = discoveredCandidateFiles(for: request)
        guard !discoveredURLs.isEmpty else {
            throw CloudKubeConfigImportError.noKubeconfigDiscovered(command: command.displayCommand)
        }
        let reviews = discoveredURLs.map { url in
            let raw = (try? String(contentsOf: url, encoding: .utf8)) ?? ""
            return validator.validate(raw: raw, sourceName: url.lastPathComponent)
        }
        return CloudKubeConfigImportResult(
            command: command,
            commandResult: commandResult,
            discoveredURLs: discoveredURLs,
            reviews: reviews
        )
    }

    private func discoveredCandidateFiles(for request: CloudKubeConfigImportRequest) -> [URL] {
        var urls: [URL] = []
        let target = request.targetKubeconfigPath.trimmingCharacters(in: .whitespacesAndNewlines)
        if !target.isEmpty {
            urls.append(URL(fileURLWithPath: NSString(string: target).expandingTildeInPath))
        }
        urls.append(contentsOf: discoverer.discoverCandidateFiles())

        var seen = Set<String>()
        return urls.filter { url in
            let key = url.standardizedFileURL.path
            guard seen.insert(key).inserted else { return false }
            return FileManager.default.fileExists(atPath: key)
        }
    }

}

public struct ProcessCloudKubeConfigCommandRunner: CloudKubeConfigCommandRunning {
    private let executor: any ProcessCommandExecuting

    public init() {
        self.executor = ProcessCommandExecutor()
    }

    init(executor: any ProcessCommandExecuting) {
        self.executor = executor
    }

    public func run(_ command: CloudKubeConfigCommandPreview, timeout: TimeInterval) async throws -> CloudKubeConfigCommandResult {
        let result = try await executor.run(
            executable: command.executable,
            arguments: command.arguments,
            environment: command.environment,
            timeout: timeout
        )
        return CloudKubeConfigCommandResult(
            exitCode: result.exitCode,
            stdout: result.stdout,
            stderr: result.stderr,
            timedOut: result.timedOut
        )
    }
}
