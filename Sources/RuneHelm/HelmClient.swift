import Foundation
import RuneCore
import RuneKube
import RuneSecurity

public final class HelmClient: HelmReleaseService, @unchecked Sendable {
    private let runner: CommandRunning
    private let parser: HelmOutputParser
    private let builder: HelmCommandBuilder
    private let helmPath: String
    private let commandTimeout: TimeInterval
    private let access: SecurityScopedAccess

    public init(
        runner: CommandRunning = ProcessCommandRunner(),
        parser: HelmOutputParser = HelmOutputParser(),
        builder: HelmCommandBuilder = HelmCommandBuilder(),
        helmPath: String = "/usr/bin/env",
        commandTimeout: TimeInterval = 30,
        access: SecurityScopedAccess = SecurityScopedAccess()
    ) {
        self.runner = runner
        self.parser = parser
        self.builder = builder
        self.helmPath = helmPath
        self.commandTimeout = commandTimeout
        self.access = access
    }

    public func listReleases(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String?,
        allNamespaces: Bool
    ) async throws -> [HelmReleaseSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runHelm(
            arguments: builder.listArguments(context: context.name, namespace: namespace, allNamespaces: allNamespaces),
            environment: env
        )

        do {
            return try parser.parseReleases(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "helm release list could not be parsed")
        }
    }

    public func releaseValues(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runHelm(
            arguments: builder.valuesArguments(context: context.name, namespace: namespace, releaseName: releaseName),
            environment: env
        )
        return result.stdout
    }

    public func releaseManifest(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runHelm(
            arguments: builder.manifestArguments(context: context.name, namespace: namespace, releaseName: releaseName),
            environment: env
        )
        return result.stdout
    }

    public func releaseHistory(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> [HelmReleaseRevision] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runHelm(
            arguments: builder.historyArguments(context: context.name, namespace: namespace, releaseName: releaseName),
            environment: env
        )

        do {
            return try parser.parseHistory(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "helm history could not be parsed")
        }
    }

    public func rollbackRelease(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String,
        revision: Int
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        _ = try await runHelm(
            arguments: builder.rollbackArguments(context: context.name, namespace: namespace, releaseName: releaseName, revision: revision),
            environment: env
        )
    }

    private func runHelm(arguments: [String], environment: [String: String]) async throws -> CommandResult {
        let result = try await runner.run(
            executable: helmPath,
            arguments: ["helm"] + arguments,
            environment: environment,
            timeout: commandTimeout
        )

        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(command: "helm \(arguments.joined(separator: " "))", message: result.stderr)
        }

        return result
    }

    private func ensureSources(_ sources: [KubeConfigSource]) throws {
        guard !sources.isEmpty else {
            throw RuneError.missingKubeConfig
        }
    }

    private func kubeconfigEnvironment(from sources: [KubeConfigSource]) throws -> [String: String] {
        try ensureSources(sources)

        let urls = sources.map(\.url)

        for url in urls {
            _ = try access.withAccess(to: url) {
                try FileManager.default.attributesOfItem(atPath: url.path)
            }
        }

        return [
            "KUBECONFIG": urls.map(\.path).joined(separator: ":")
        ]
    }
}
