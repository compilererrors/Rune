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

    public init(exitCode: Int32, stdout: String, stderr: String) {
        self.exitCode = exitCode
        self.stdout = stdout
        self.stderr = stderr
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

    public var errorDescription: String? {
        switch self {
        case .missingRequiredField(let name):
            return "\(name) is required."
        case .commandFailed(let command, let exitCode, let message):
            return "Cloud import command failed with exit code \(exitCode): \(command). \(message)"
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
    private let runner: CloudKubeConfigCommandRunning
    private let discoverer: KubeConfigDiscovering
    private let validator: KubeConfigImportValidator
    private let timeout: TimeInterval

    public init(
        runner: CloudKubeConfigCommandRunning = ProcessCloudKubeConfigCommandRunner(),
        discoverer: KubeConfigDiscovering = KubeConfigDiscoverer(),
        validator: KubeConfigImportValidator = KubeConfigImportValidator(),
        timeout: TimeInterval = 120
    ) {
        self.runner = runner
        self.discoverer = discoverer
        self.validator = validator
        self.timeout = timeout
    }

    public func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        let clusterName = try required(request.clusterName, "Cluster name")
        let targetKubeconfigPath = request.targetKubeconfigPath.trimmingCharacters(in: .whitespacesAndNewlines)
        let executable: String
        var arguments: [String]
        var environment: [String: String] = [:]

        switch request.provider {
        case .aks:
            executable = "az"
            let resourceGroup = try required(request.resourceGroup, "Resource group")
            arguments = [
                "aks", "get-credentials",
                "--resource-group", resourceGroup,
                "--name", clusterName
            ]
            if request.overwriteExisting {
                arguments.append("--overwrite-existing")
            }
            appendOptional("--file", targetKubeconfigPath, to: &arguments)
            appendOptional("--subscription", request.profileOrSubscription, to: &arguments)

        case .eks:
            executable = "aws"
            let region = try required(request.regionOrLocation, "Region")
            arguments = [
                "eks", "update-kubeconfig",
                "--region", region,
                "--name", clusterName
            ]
            appendOptional("--kubeconfig", targetKubeconfigPath, to: &arguments)
            appendOptional("--profile", request.profileOrSubscription, to: &arguments)
            appendOptional("--role-arn", request.roleARN, to: &arguments)

        case .gke:
            executable = "gcloud"
            let location = try required(request.regionOrLocation, "Location")
            let projectID = try required(request.projectID, "Project ID")
            arguments = [
                "container", "clusters", "get-credentials",
                clusterName,
                "--location", location,
                "--project", projectID
            ]
            if !targetKubeconfigPath.isEmpty {
                environment["KUBECONFIG"] = targetKubeconfigPath
            }
        }

        let parts = [executable] + arguments
        return CloudKubeConfigCommandPreview(
            executable: executable,
            arguments: arguments,
            displayCommand: parts.map(Self.shellQuoted).joined(separator: " "),
            environment: environment
        )
    }

    public func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        let command = try commandPreview(for: request)
        let commandResult = try await runner.run(command, timeout: timeout)
        guard commandResult.exitCode == 0 else {
            throw CloudKubeConfigImportError.commandFailed(
                command: command.displayCommand,
                exitCode: commandResult.exitCode,
                message: commandResult.stderr.isEmpty ? commandResult.stdout : commandResult.stderr
            )
        }

        let discoveredURLs = discoveredCandidateFiles(for: request)
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
            return seen.insert(key).inserted
        }
    }

    private func required(_ value: String, _ name: String) throws -> String {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            throw CloudKubeConfigImportError.missingRequiredField(name)
        }
        return trimmed
    }

    private func appendOptional(_ flag: String, _ value: String, to arguments: inout [String]) {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        arguments.append(flag)
        arguments.append(trimmed)
    }

    private static func shellQuoted(_ value: String) -> String {
        guard value.rangeOfCharacter(from: .whitespacesAndNewlines) != nil
            || value.contains("'")
            || value.contains("\"") else {
            return value
        }
        return "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
    }
}

public struct ProcessCloudKubeConfigCommandRunner: CloudKubeConfigCommandRunning {
    public init() {}

    public func run(_ command: CloudKubeConfigCommandPreview, timeout: TimeInterval) async throws -> CloudKubeConfigCommandResult {
        try await Task.detached(priority: .userInitiated) {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
            process.arguments = [command.executable] + command.arguments
            var environment = ProcessInfo.processInfo.environment
            environment["PATH"] = RuneExecutableSearchPath.pathValue(from: environment)
            for (key, value) in command.environment {
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
            if process.isRunning {
                process.terminate()
            }
            process.waitUntilExit()

            let stdout = String(data: stdoutPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
            let stderr = String(data: stderrPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
            return CloudKubeConfigCommandResult(exitCode: process.terminationStatus, stdout: stdout, stderr: stderr)
        }.value
    }
}
