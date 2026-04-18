import Foundation
import RuneCore

/// Locates the `rune-k8s-agent` helper (Go + client-go) bundled next to the app or via `RUNE_K8S_AGENT`.
enum RuneK8sAgentLocator {
    static func resolvedExecutablePath() -> String? {
        if let env = ProcessInfo.processInfo.environment["RUNE_K8S_AGENT"], !env.isEmpty,
           FileManager.default.isExecutableFile(atPath: env) {
            return env
        }
        let bundle = Bundle.main.bundlePath
        guard !bundle.isEmpty else { return nil }
        let candidate = URL(fileURLWithPath: bundle).appendingPathComponent("Contents/MacOS/rune-k8s-agent")
        if FileManager.default.isExecutableFile(atPath: candidate.path) {
            return candidate.path
        }
        return nil
    }
}

/// Lists batch workloads via the bundled `rune-k8s-agent` helper (stdout JSON). Uses the same kubeconfig environment as the rest of Rune.
enum RuneK8sAgentWorkloadClient {
    static func listJobs(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let result = try await runner.run(
            executable: executablePath,
            arguments: ["list", "jobs", "--context", contextName, "--namespace", namespace],
            environment: environment,
            timeout: timeout
        )
        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: "rune-k8s-agent list jobs",
                message: result.stderr.isEmpty ? "exit \(result.exitCode)" : result.stderr
            )
        }
        return try decodeSummaries(from: result.stdout)
    }

    static func listCronJobs(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let result = try await runner.run(
            executable: executablePath,
            arguments: ["list", "cronjobs", "--context", contextName, "--namespace", namespace],
            environment: environment,
            timeout: timeout
        )
        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: "rune-k8s-agent list cronjobs",
                message: result.stderr.isEmpty ? "exit \(result.exitCode)" : result.stderr
            )
        }
        return try decodeSummaries(from: result.stdout)
    }

    static func listDaemonSets(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let result = try await runner.run(
            executable: executablePath,
            arguments: ["list", "daemonsets", "--context", contextName, "--namespace", namespace],
            environment: environment,
            timeout: timeout
        )
        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: "rune-k8s-agent list daemonsets",
                message: result.stderr.isEmpty ? "exit \(result.exitCode)" : result.stderr
            )
        }
        return try decodeSummaries(from: result.stdout)
    }

    static func listStatefulSets(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let result = try await runner.run(
            executable: executablePath,
            arguments: ["list", "statefulsets", "--context", contextName, "--namespace", namespace],
            environment: environment,
            timeout: timeout
        )
        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: "rune-k8s-agent list statefulsets",
                message: result.stderr.isEmpty ? "exit \(result.exitCode)" : result.stderr
            )
        }
        return try decodeSummaries(from: result.stdout)
    }

    static func listDeployments(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [DeploymentSummary] {
        let result = try await runner.run(
            executable: executablePath,
            arguments: ["list", "deployments", "--context", contextName, "--namespace", namespace],
            environment: environment,
            timeout: timeout
        )
        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: "rune-k8s-agent list deployments",
                message: result.stderr.isEmpty ? "exit \(result.exitCode)" : result.stderr
            )
        }
        return try decodeDeployments(from: result.stdout)
    }

    static func listReplicaSets(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let result = try await runner.run(
            executable: executablePath,
            arguments: ["list", "replicasets", "--context", contextName, "--namespace", namespace],
            environment: environment,
            timeout: timeout
        )
        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: "rune-k8s-agent list replicasets",
                message: result.stderr.isEmpty ? "exit \(result.exitCode)" : result.stderr
            )
        }
        return try decodeSummaries(from: result.stdout)
    }

    private static func decodeDeployments(from stdout: String) throws -> [DeploymentSummary] {
        let trimmed = stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            return []
        }
        let data = Data(trimmed.utf8)
        do {
            return try JSONDecoder().decode([DeploymentSummary].self, from: data)
        } catch {
            throw RuneError.parseError(message: "rune-k8s-agent deployments JSON kunde inte tolkas")
        }
    }

    private static func decodeSummaries(from stdout: String) throws -> [ClusterResourceSummary] {
        let trimmed = stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            return []
        }
        let data = Data(trimmed.utf8)
        do {
            return try JSONDecoder().decode([ClusterResourceSummary].self, from: data)
        } catch {
            throw RuneError.parseError(message: "rune-k8s-agent JSON kunde inte tolkas")
        }
    }
}
