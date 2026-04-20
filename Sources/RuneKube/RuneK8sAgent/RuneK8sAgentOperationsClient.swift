import Foundation
import RuneCore

/// Operation-focused wrapper over `rune-k8s-agent` (logs/top/exec/mutations/rollout/port-forward).
enum RuneK8sAgentOperationsClient {
    private struct NodeTopPercentRow: Decodable {
        let cpuPercent: Int?
        let memoryPercent: Int?
    }

    private struct PodTopRow: Decodable {
        let namespace: String?
        let name: String
        let cpu: String
        let memory: String
    }

    private struct AgentExecResult: Decodable {
        let stdout: String
        let stderr: String
        let exitCode: Int32
    }

    static func clusterUsagePercent(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> (cpuPercent: Int?, memoryPercent: Int?) {
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["top", "nodes", "--context", contextName],
            timeout: timeout,
            commandName: "rune-k8s-agent top nodes"
        )
        let row = try decodeJSON(NodeTopPercentRow.self, from: stdout, parseError: "rune-k8s-agent top nodes JSON kunde inte tolkas")
        return (row.cpuPercent, row.memoryPercent)
    }

    static func podTopByName(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [String: (cpu: String, memory: String)] {
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["top", "pods", "--context", contextName, "--namespace", namespace],
            timeout: timeout,
            commandName: "rune-k8s-agent top pods"
        )
        let rows = try decodeJSON([PodTopRow].self, from: stdout, parseError: "rune-k8s-agent top pods JSON kunde inte tolkas")
        return Dictionary(uniqueKeysWithValues: rows.map { ($0.name, (cpu: $0.cpu, memory: $0.memory)) })
    }

    static func podTopByNamespaceAndName(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [String: (cpu: String, memory: String)] {
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["top", "pods", "--context", contextName, "--all-namespaces"],
            timeout: timeout,
            commandName: "rune-k8s-agent top pods --all-namespaces"
        )
        let rows = try decodeJSON([PodTopRow].self, from: stdout, parseError: "rune-k8s-agent top pods all-namespaces JSON kunde inte tolkas")
        var out: [String: (cpu: String, memory: String)] = [:]
        out.reserveCapacity(rows.count)
        for row in rows {
            guard let ns = row.namespace, !ns.isEmpty else { continue }
            out["\(ns)/\(row.name)"] = (cpu: row.cpu, memory: row.memory)
        }
        return out
    }

    static func podLogs(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        podName: String,
        filter: LogTimeFilter,
        previous: Bool,
        timeout: TimeInterval
    ) async throws -> String {
        var args: [String] = [
            "logs",
            "--context", contextName,
            "--namespace", namespace,
            "--pod", podName
        ]

        switch filter {
        case .all:
            args += ["--tail", "200"]
        case let .tailLines(lines):
            args += ["--tail", String(max(1, lines))]
        case .lastMinutes, .lastHours, .lastDays, .since:
            if let since = filter.kubectlSinceArgument {
                if filter.usesSinceTime {
                    args += ["--since-time", since]
                } else {
                    args += ["--since", since]
                }
            }
            args += ["--tail", "5000"]
        }

        if previous {
            args.append("--previous")
        }

        return try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: args,
            timeout: timeout,
            commandName: "rune-k8s-agent logs"
        )
    }

    static func serviceSelector(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        serviceName: String,
        timeout: TimeInterval
    ) async throws -> [String: String] {
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "selector", "service",
                "--context", contextName,
                "--namespace", namespace,
                "--name", serviceName
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent selector service"
        )
        return try decodeJSON([String: String].self, from: stdout, parseError: "rune-k8s-agent service selector JSON kunde inte tolkas")
    }

    static func deploymentSelector(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        timeout: TimeInterval
    ) async throws -> [String: String] {
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "selector", "deployment",
                "--context", contextName,
                "--namespace", namespace,
                "--name", deploymentName
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent selector deployment"
        )
        return try decodeJSON([String: String].self, from: stdout, parseError: "rune-k8s-agent deployment selector JSON kunde inte tolkas")
    }

    static func podsBySelector(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        selector: String,
        timeout: TimeInterval
    ) async throws -> [PodSummary] {
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "selector", "pods",
                "--context", contextName,
                "--namespace", namespace,
                "--label-selector", selector
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent selector pods"
        )
        return try decodeJSON([PodSummary].self, from: stdout, parseError: "rune-k8s-agent selector pods JSON kunde inte tolkas")
    }

    static func execInPod(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        podName: String,
        container: String?,
        command: [String],
        timeout: TimeInterval
    ) async throws -> PodExecResult {
        var args: [String] = [
            "exec",
            "--context", contextName,
            "--namespace", namespace,
            "--pod", podName
        ]
        if let container, !container.isEmpty {
            args += ["--container", container]
        }
        args.append("--")
        args += command

        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: args,
            timeout: timeout,
            commandName: "rune-k8s-agent exec"
        )
        let decoded = try decodeJSON(AgentExecResult.self, from: stdout, parseError: "rune-k8s-agent exec JSON kunde inte tolkas")
        return PodExecResult(
            podName: podName,
            namespace: namespace,
            command: command,
            stdout: decoded.stdout,
            stderr: decoded.stderr,
            exitCode: decoded.exitCode
        )
    }

    static func deleteResource(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        kind: KubeResourceKind,
        name: String,
        timeout: TimeInterval
    ) async throws {
        _ = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "delete",
                "--context", contextName,
                "--namespace", namespace,
                "--kind", kind.kubectlName,
                "--name", name
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent delete"
        )
    }

    static func scaleDeployment(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        replicas: Int,
        timeout: TimeInterval
    ) async throws {
        _ = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "scale", "deployment",
                "--context", contextName,
                "--namespace", namespace,
                "--name", deploymentName,
                "--replicas", String(replicas)
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent scale deployment"
        )
    }

    static func restartDeploymentRollout(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        timeout: TimeInterval
    ) async throws {
        _ = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "rollout", "restart", "deployment",
                "--context", contextName,
                "--namespace", namespace,
                "--name", deploymentName
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent rollout restart deployment"
        )
    }

    static func deploymentRolloutHistory(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        timeout: TimeInterval
    ) async throws -> String {
        try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "rollout", "history", "deployment",
                "--context", contextName,
                "--namespace", namespace,
                "--name", deploymentName
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent rollout history deployment"
        )
    }

    static func rollbackDeploymentRollout(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        revision: Int?,
        timeout: TimeInterval
    ) async throws {
        var args: [String] = [
            "rollout", "undo", "deployment",
            "--context", contextName,
            "--namespace", namespace,
            "--name", deploymentName
        ]
        if let revision {
            args += ["--to-revision", String(revision)]
        }
        _ = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: args,
            timeout: timeout,
            commandName: "rune-k8s-agent rollout undo deployment"
        )
    }

    static func applyFile(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        filePath: String,
        timeout: TimeInterval
    ) async throws {
        _ = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "apply",
                "--context", contextName,
                "--namespace", namespace,
                "--file", filePath
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent apply"
        )
    }

    static func patchCronJobSuspend(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        name: String,
        suspend: Bool,
        timeout: TimeInterval
    ) async throws {
        _ = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "patch", "cronjob-suspend",
                "--context", contextName,
                "--namespace", namespace,
                "--name", name,
                "--suspend", suspend ? "true" : "false"
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent patch cronjob-suspend"
        )
    }

    static func createJobFromCronJob(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        cronJobName: String,
        jobName: String,
        timeout: TimeInterval
    ) async throws {
        _ = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: [
                "create", "job-from-cronjob",
                "--context", contextName,
                "--namespace", namespace,
                "--cronjob", cronJobName,
                "--job", jobName
            ],
            timeout: timeout,
            commandName: "rune-k8s-agent create job-from-cronjob"
        )
    }

    static func portForwardArguments(
        contextName: String,
        namespace: String,
        targetKind: PortForwardTargetKind,
        targetName: String,
        localPort: Int,
        remotePort: Int,
        address: String
    ) -> [String] {
        [
            "port-forward",
            "--context", contextName,
            "--namespace", namespace,
            "--target-kind", targetKind.kubectlResourceName,
            "--target-name", targetName,
            "--local-port", String(localPort),
            "--remote-port", String(remotePort),
            "--address", address
        ]
    }

    private static func runAgentCommand(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        arguments: [String],
        timeout: TimeInterval,
        commandName: String
    ) async throws -> String {
        let quickAttemptTimeout: TimeInterval = 12
        let result: CommandResult
        if timeout > quickAttemptTimeout {
            do {
                result = try await runner.run(
                    executable: executablePath,
                    arguments: arguments,
                    environment: environment,
                    timeout: quickAttemptTimeout
                )
            } catch {
                guard isProcessTimeoutError(error) else { throw error }
                result = try await runner.run(
                    executable: executablePath,
                    arguments: arguments,
                    environment: environment,
                    timeout: timeout
                )
            }
        } else {
            result = try await runner.run(
                executable: executablePath,
                arguments: arguments,
                environment: environment,
                timeout: timeout
            )
        }

        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: commandName,
                message: result.stderr.isEmpty ? result.stdout : result.stderr
            )
        }
        return result.stdout
    }

    private static func isProcessTimeoutError(_ error: Error) -> Bool {
        guard case let RuneError.commandFailed(_, message) = error else { return false }
        return message.localizedCaseInsensitiveContains("timed out")
    }

    private static func decodeJSON<T: Decodable>(
        _ type: T.Type,
        from stdout: String,
        parseError: String
    ) throws -> T {
        let trimmed = stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        let data = Data(trimmed.utf8)
        do {
            return try JSONDecoder().decode(T.self, from: data)
        } catch {
            throw RuneError.parseError(message: parseError)
        }
    }
}
