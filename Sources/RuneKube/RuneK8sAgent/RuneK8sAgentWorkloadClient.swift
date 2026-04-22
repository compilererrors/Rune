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
    static func listContexts(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        timeout: TimeInterval
    ) async throws -> [KubeContext] {
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["contexts"],
            timeout: timeout,
            commandName: "rune-k8s-agent contexts"
        )
        let names: [String] = try decodeJSON([String].self, from: stdout, parseError: "rune-k8s-agent contexts JSON could not be parsed")
        return names.map(KubeContext.init(name:))
    }

    static func listNamespaces(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [String] {
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["namespaces", "--context", contextName],
            timeout: timeout,
            commandName: "rune-k8s-agent namespaces"
        )
        return try decodeJSON([String].self, from: stdout, parseError: "rune-k8s-agent namespaces JSON could not be parsed")
    }

    static func contextNamespace(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> String? {
        struct ContextNamespaceRow: Decodable {
            let namespace: String
        }
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["context-namespace", "--context", contextName],
            timeout: timeout,
            commandName: "rune-k8s-agent context-namespace"
        )
        let row = try decodeJSON(
            ContextNamespaceRow.self,
            from: stdout,
            parseError: "rune-k8s-agent context-namespace JSON could not be parsed"
        )
        let trimmed = row.namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    static func listPods(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [PodSummary] {
        struct AgentPodSummary: Decodable {
            let name: String
            let namespace: String
            let status: String
            let totalRestarts: Int
            let creationTimestamp: String?
            let podIP: String?
            let hostIP: String?
            let nodeName: String?
            let qosClass: String?
            let containersReady: String?
            let containerNamesLine: String?
        }
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "pods",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        let rows = try decodeJSON([AgentPodSummary].self, from: stdout, parseError: "rune-k8s-agent pods JSON could not be parsed")
        return rows.map { row in
            PodSummary(
                name: row.name,
                namespace: row.namespace,
                status: row.status,
                totalRestarts: row.totalRestarts,
                ageDescription: KubernetesAgeFormatting.describe(creationISO8601: row.creationTimestamp),
                cpuUsage: nil,
                memoryUsage: nil,
                podIP: row.podIP,
                hostIP: row.hostIP,
                nodeName: row.nodeName,
                qosClass: row.qosClass,
                containersReady: row.containersReady,
                containerNamesLine: row.containerNamesLine
            )
        }
    }

    static func listPodsAllNamespaces(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [PodSummary] {
        struct AgentPodSummary: Decodable {
            let name: String
            let namespace: String
            let status: String
            let totalRestarts: Int
            let creationTimestamp: String?
            let podIP: String?
            let hostIP: String?
            let nodeName: String?
            let qosClass: String?
            let containersReady: String?
            let containerNamesLine: String?
        }
        let stdout = try await runListAllNamespacesCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "pods",
            contextName: contextName,
            timeout: timeout
        )
        let rows = try decodeJSON([AgentPodSummary].self, from: stdout, parseError: "rune-k8s-agent pods JSON could not be parsed")
        return rows.map { row in
            PodSummary(
                name: row.name,
                namespace: row.namespace,
                status: row.status,
                totalRestarts: row.totalRestarts,
                ageDescription: KubernetesAgeFormatting.describe(creationISO8601: row.creationTimestamp),
                cpuUsage: nil,
                memoryUsage: nil,
                podIP: row.podIP,
                hostIP: row.hostIP,
                nodeName: row.nodeName,
                qosClass: row.qosClass,
                containersReady: row.containersReady,
                containerNamesLine: row.containerNamesLine
            )
        }
    }

    static func listServices(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ServiceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "services",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ServiceSummary].self, from: stdout, parseError: "rune-k8s-agent services JSON could not be parsed")
    }

    static func listServicesAllNamespaces(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [ServiceSummary] {
        let stdout = try await runListAllNamespacesCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "services",
            contextName: contextName,
            timeout: timeout
        )
        return try decodeJSON([ServiceSummary].self, from: stdout, parseError: "rune-k8s-agent services JSON could not be parsed")
    }

    static func listIngresses(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "ingresses",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent ingresses JSON could not be parsed")
    }

    static func listIngressesAllNamespaces(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListAllNamespacesCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "ingresses",
            contextName: contextName,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent ingresses JSON could not be parsed")
    }

    static func listConfigMaps(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "configmaps",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent configmaps JSON could not be parsed")
    }

    static func listConfigMapsAllNamespaces(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListAllNamespacesCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "configmaps",
            contextName: contextName,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent configmaps JSON could not be parsed")
    }

    static func listSecrets(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "secrets",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent secrets JSON could not be parsed")
    }

    static func listRoles(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "roles",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent roles JSON could not be parsed")
    }

    static func listRoleBindings(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "rolebindings",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent rolebindings JSON could not be parsed")
    }

    static func listPersistentVolumeClaims(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "persistentvolumeclaims",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent persistentvolumeclaims JSON could not be parsed")
    }

    static func listHorizontalPodAutoscalers(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "horizontalpodautoscalers",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent horizontalpodautoscalers JSON could not be parsed")
    }

    static func listNetworkPolicies(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "networkpolicies",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent networkpolicies JSON could not be parsed")
    }

    static func listNodes(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["list", "nodes", "--context", contextName],
            timeout: timeout,
            commandName: "rune-k8s-agent list nodes"
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent nodes JSON could not be parsed")
    }

    static func listPersistentVolumes(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runClusterListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "persistentvolumes",
            contextName: contextName,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent persistentvolumes JSON could not be parsed")
    }

    static func listStorageClasses(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runClusterListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "storageclasses",
            contextName: contextName,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent storageclasses JSON could not be parsed")
    }

    static func listClusterRoles(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runClusterListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "clusterroles",
            contextName: contextName,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent clusterroles JSON could not be parsed")
    }

    static func listClusterRoleBindings(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runClusterListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "clusterrolebindings",
            contextName: contextName,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent clusterrolebindings JSON could not be parsed")
    }

    static func listEvents(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [EventSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "events",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([EventSummary].self, from: stdout, parseError: "rune-k8s-agent events JSON could not be parsed")
    }

    static func listEventsAllNamespaces(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [EventSummary] {
        let stdout = try await runListAllNamespacesCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "events",
            contextName: contextName,
            timeout: timeout
        )
        return try decodeJSON([EventSummary].self, from: stdout, parseError: "rune-k8s-agent events JSON could not be parsed")
    }

    static func listJobs(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "jobs",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent JSON could not be parsed")
    }

    static func listCronJobs(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "cronjobs",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent JSON could not be parsed")
    }

    static func listDaemonSets(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "daemonsets",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent JSON could not be parsed")
    }

    static func listStatefulSets(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "statefulsets",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent JSON could not be parsed")
    }

    static func listDeployments(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [DeploymentSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "deployments",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([DeploymentSummary].self, from: stdout, parseError: "rune-k8s-agent deployments JSON could not be parsed")
    }

    static func listDeploymentsAllNamespaces(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        timeout: TimeInterval
    ) async throws -> [DeploymentSummary] {
        let stdout = try await runListAllNamespacesCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "deployments",
            contextName: contextName,
            timeout: timeout
        )
        return try decodeJSON([DeploymentSummary].self, from: stdout, parseError: "rune-k8s-agent deployments JSON could not be parsed")
    }

    static func listReplicaSets(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> [ClusterResourceSummary] {
        let stdout = try await runListCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            resource: "replicasets",
            contextName: contextName,
            namespace: namespace,
            timeout: timeout
        )
        return try decodeJSON([ClusterResourceSummary].self, from: stdout, parseError: "rune-k8s-agent JSON could not be parsed")
    }

    private static func runListCommand(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        resource: String,
        contextName: String,
        namespace: String,
        timeout: TimeInterval
    ) async throws -> String {
        try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["list", resource, "--context", contextName, "--namespace", namespace],
            timeout: timeout,
            commandName: "rune-k8s-agent list \(resource)"
        )
    }

    private static func runListAllNamespacesCommand(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        resource: String,
        contextName: String,
        timeout: TimeInterval
    ) async throws -> String {
        try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["list", resource, "--context", contextName, "--all-namespaces"],
            timeout: timeout,
            commandName: "rune-k8s-agent list \(resource) --all-namespaces"
        )
    }

    private static func runClusterListCommand(
        executablePath: String,
        runner: CommandRunning,
        environment: [String: String],
        resource: String,
        contextName: String,
        timeout: TimeInterval
    ) async throws -> String {
        try await runAgentCommand(
            executablePath: executablePath,
            runner: runner,
            environment: environment,
            arguments: ["list", resource, "--context", contextName],
            timeout: timeout,
            commandName: "rune-k8s-agent list \(resource)"
        )
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
                message: result.stderr.isEmpty ? "exit \(result.exitCode)" : result.stderr
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
