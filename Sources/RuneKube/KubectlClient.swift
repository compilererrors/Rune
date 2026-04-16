import Foundation
import RuneCore
import RuneSecurity

public final class KubectlClient: ContextListingService, NamespaceListingService, PodListingService, DeploymentListingService, ServiceListingService, EventListingService, GenericResourceListingService, PodLogService, UnifiedServiceLogService, UnifiedDeploymentLogService, ManifestService, ResourceWriteService, @unchecked Sendable {
    private let runner: CommandRunning
    private let longRunningRunner: LongRunningCommandRunning
    private let parser: KubectlOutputParser
    private let builder: KubectlCommandBuilder
    private let kubectlPath: String
    private let commandTimeout: TimeInterval
    private let access: SecurityScopedAccess
    private let portForwardRegistry = PortForwardRegistry()

    public init(
        runner: CommandRunning = ProcessCommandRunner(),
        longRunningRunner: LongRunningCommandRunning = ProcessLongRunningCommandRunner(),
        parser: KubectlOutputParser = KubectlOutputParser(),
        builder: KubectlCommandBuilder = KubectlCommandBuilder(),
        kubectlPath: String = "/usr/bin/env",
        commandTimeout: TimeInterval = 30,
        access: SecurityScopedAccess = SecurityScopedAccess()
    ) {
        self.runner = runner
        self.longRunningRunner = longRunningRunner
        self.parser = parser
        self.builder = builder
        self.kubectlPath = kubectlPath
        self.commandTimeout = commandTimeout
        self.access = access
    }

    public func listContexts(from sources: [KubeConfigSource]) async throws -> [KubeContext] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(arguments: builder.contextListArguments(), environment: env)
        return parser.parseContexts(from: result.stdout)
    }

    public func listNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [String] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.namespaceListArguments(context: context.name),
            environment: env
        )
        return parser.parseNamespaces(from: result.stdout)
    }

    public func contextNamespace(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> String? {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.contextNamespaceArguments(context: context.name),
            environment: env
        )

        let trimmed = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    public func listPods(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [PodSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.podListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        return parser.parsePods(namespace: namespace, from: result.stdout)
    }

    public func listPodsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [PodSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.podListAllNamespacesArguments(context: context.name),
            environment: env
        )

        return parser.parsePodsAllNamespaces(from: result.stdout)
    }

    public func listDeployments(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [DeploymentSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.deploymentListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseDeployments(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "deployments JSON kunde inte tolkas")
        }
    }

    public func listDeploymentsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [DeploymentSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.deploymentListAllNamespacesArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseDeployments(namespace: "", from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "deployments JSON kunde inte tolkas")
        }
    }

    public func listServices(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ServiceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.serviceListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseServices(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "services JSON kunde inte tolkas")
        }
    }

    public func listServicesAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ServiceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.serviceListAllNamespacesArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseServices(namespace: "", from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "services JSON kunde inte tolkas")
        }
    }

    public func listStatefulSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.statefulSetListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseStatefulSets(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "statefulsets JSON kunde inte tolkas")
        }
    }

    public func listDaemonSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.daemonSetListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseDaemonSets(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "daemonsets JSON kunde inte tolkas")
        }
    }

    public func listIngresses(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.ingressListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseIngresses(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "ingresses JSON kunde inte tolkas")
        }
    }

    public func listIngressesAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.ingressListAllNamespacesArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseIngresses(namespace: "", from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "ingresses JSON kunde inte tolkas")
        }
    }

    public func listConfigMaps(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.configMapListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseConfigMaps(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "configmaps JSON kunde inte tolkas")
        }
    }

    public func listConfigMapsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.configMapListAllNamespacesArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseConfigMaps(namespace: "", from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "configmaps JSON kunde inte tolkas")
        }
    }

    public func listSecrets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.secretListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseSecrets(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "secrets JSON kunde inte tolkas")
        }
    }

    public func listNodes(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.nodeListArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseNodes(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "nodes JSON kunde inte tolkas")
        }
    }

    public func listEvents(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [EventSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.eventListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseEvents(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "events JSON kunde inte tolkas")
        }
    }

    public func listEventsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [EventSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.eventListAllNamespacesArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseEvents(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "events JSON kunde inte tolkas")
        }
    }

    public func countNamespacedResources(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        resource: String
    ) async throws -> Int {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.namespacedResourceCountArguments(
                context: context.name,
                namespace: namespace,
                resource: resource
            ),
            environment: env
        )
        return Self.parseLineCount(from: result.stdout)
    }

    public func countClusterResources(
        from sources: [KubeConfigSource],
        context: KubeContext,
        resource: String
    ) async throws -> Int {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.clusterResourceCountArguments(
                context: context.name,
                resource: resource
            ),
            environment: env
        )
        return Self.parseLineCount(from: result.stdout)
    }

    public func podLogs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        let arguments = builder.podLogsArguments(
            context: context.name,
            namespace: namespace,
            podName: podName,
            container: nil,
            filter: filter,
            previous: previous,
            follow: false
        )

        let result: CommandResult
        do {
            result = try await runKubectl(arguments: arguments, environment: env)
        } catch {
            if previous, isMissingPreviousLogsError(error) {
                return "No previous logs available for \(podName)."
            }
            throw error
        }

        return result.stdout
    }

    public func unifiedLogsForService(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        service: ServiceSummary,
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> UnifiedServiceLogs {
        let env = try kubeconfigEnvironment(from: sources)

        let serviceJSON = try await runKubectl(
            arguments: builder.serviceJSONArguments(context: context.name, namespace: namespace, serviceName: service.name),
            environment: env
        )

        let selectorMap: [String: String]
        do {
            selectorMap = try parser.parseServiceSelector(from: serviceJSON.stdout)
        } catch {
            throw RuneError.parseError(message: "service selector kunde inte tolkas")
        }

        guard !selectorMap.isEmpty else {
            throw RuneError.parseError(message: "Service \(service.name) saknar selector och kan inte användas för unified logs")
        }

        let selector = selectorMap
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: ",")

        let podResult = try await runKubectl(
            arguments: builder.podsByLabelSelectorArguments(context: context.name, namespace: namespace, selector: selector),
            environment: env
        )

        let pods = parser.parsePods(namespace: namespace, from: podResult.stdout)
        guard !pods.isEmpty else {
            return UnifiedServiceLogs(service: service, podNames: [], mergedText: "No pods found for service selector: \(selector)")
        }

        var collectedLines: [TaggedLogLine] = []

        try await withThrowingTaskGroup(of: [TaggedLogLine].self) { group in
            for pod in pods {
                group.addTask {
                    let logs = try await self.podLogs(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        podName: pod.name,
                        filter: filter,
                        previous: previous
                    )

                    return self.taggedLines(from: logs, podName: pod.name)
                }
            }

            for try await podLines in group {
                collectedLines.append(contentsOf: podLines)
            }
        }

        let merged = collectedLines
            .sorted(by: Self.taggedLineSort)
            .map { line in
                "[\(line.podName)] \(line.text)"
            }
            .joined(separator: "\n")

        return UnifiedServiceLogs(
            service: service,
            podNames: pods.map(\.name).sorted(),
            mergedText: merged
        )
    }

    public func unifiedLogsForDeployment(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deployment: DeploymentSummary,
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> UnifiedDeploymentLogs {
        let env = try kubeconfigEnvironment(from: sources)

        let deploymentJSON = try await runKubectl(
            arguments: builder.deploymentJSONArguments(context: context.name, namespace: namespace, deploymentName: deployment.name),
            environment: env
        )

        let selectorMap: [String: String]
        do {
            selectorMap = try parser.parseDeploymentSelector(from: deploymentJSON.stdout)
        } catch {
            throw RuneError.parseError(message: "deployment selector kunde inte tolkas")
        }

        guard !selectorMap.isEmpty else {
            throw RuneError.parseError(message: "Deployment \(deployment.name) saknar matchLabels-selector och kan inte användas för unified logs")
        }

        let selector = selectorMap
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: ",")

        let podResult = try await runKubectl(
            arguments: builder.podsByLabelSelectorArguments(context: context.name, namespace: namespace, selector: selector),
            environment: env
        )

        let pods = parser.parsePods(namespace: namespace, from: podResult.stdout)
        guard !pods.isEmpty else {
            return UnifiedDeploymentLogs(deployment: deployment, podNames: [], mergedText: "No pods found for deployment selector: \(selector)")
        }

        var collectedLines: [TaggedLogLine] = []

        try await withThrowingTaskGroup(of: [TaggedLogLine].self) { group in
            for pod in pods {
                group.addTask {
                    let logs = try await self.podLogs(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        podName: pod.name,
                        filter: filter,
                        previous: previous
                    )

                    return self.taggedLines(from: logs, podName: pod.name)
                }
            }

            for try await podLines in group {
                collectedLines.append(contentsOf: podLines)
            }
        }

        let merged = collectedLines
            .sorted(by: Self.taggedLineSort)
            .map { line in
                "[\(line.podName)] \(line.text)"
            }
            .joined(separator: "\n")

        return UnifiedDeploymentLogs(
            deployment: deployment,
            podNames: pods.map(\.name).sorted(),
            mergedText: merged
        )
    }

    public func resourceYAML(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.resourceYAMLArguments(context: context.name, namespace: namespace, kind: kind, name: name),
            environment: env
        )

        return result.stdout
    }

    public func execInPod(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        container: String?,
        command: [String]
    ) async throws -> PodExecResult {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.podExecArguments(
                context: context.name,
                namespace: namespace,
                podName: podName,
                container: container,
                command: command
            ),
            environment: env
        )

        return PodExecResult(
            podName: podName,
            namespace: namespace,
            command: command,
            stdout: result.stdout,
            stderr: result.stderr,
            exitCode: result.exitCode
        )
    }

    public func deleteResource(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        _ = try await runKubectl(
            arguments: builder.deleteResourceArguments(context: context.name, namespace: namespace, kind: kind, name: name),
            environment: env
        )
    }

    public func scaleDeployment(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String,
        replicas: Int
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        _ = try await runKubectl(
            arguments: builder.scaleDeploymentArguments(context: context.name, namespace: namespace, deploymentName: deploymentName, replicas: replicas),
            environment: env
        )
    }

    public func restartDeploymentRollout(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        _ = try await runKubectl(
            arguments: builder.rolloutRestartArguments(context: context.name, namespace: namespace, deploymentName: deploymentName),
            environment: env
        )
    }

    public func deploymentRolloutHistory(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.rolloutHistoryArguments(context: context.name, namespace: namespace, deploymentName: deploymentName),
            environment: env
        )

        return result.stdout
    }

    public func rollbackDeploymentRollout(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String,
        revision: Int?
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        _ = try await runKubectl(
            arguments: builder.rolloutUndoArguments(
                context: context.name,
                namespace: namespace,
                deploymentName: deploymentName,
                revision: revision
            ),
            environment: env
        )
    }

    public func startPortForward(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        targetKind: PortForwardTargetKind,
        targetName: String,
        localPort: Int,
        remotePort: Int,
        address: String,
        onEvent: @escaping @Sendable (PortForwardSession) -> Void
    ) async throws -> PortForwardSession {
        let env = try kubeconfigEnvironment(from: sources)
        let sessionID = UUID().uuidString
        let baseSession = PortForwardSession(
            id: sessionID,
            contextName: context.name,
            namespace: namespace,
            targetKind: targetKind,
            targetName: targetName,
            localPort: localPort,
            remotePort: remotePort,
            address: address,
            status: .starting
        )

        let handle = try longRunningRunner.start(
            executable: kubectlPath,
            arguments: ["kubectl"] + builder.portForwardArguments(
                context: context.name,
                namespace: namespace,
                targetKind: targetKind,
                targetName: targetName,
                localPort: localPort,
                remotePort: remotePort,
                address: address
            ),
            environment: env,
            onStdout: { output in
                let message = output.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !message.isEmpty else { return }
                onEvent(
                    PortForwardSession(
                        id: sessionID,
                        contextName: context.name,
                        namespace: namespace,
                        targetKind: targetKind,
                        targetName: targetName,
                        localPort: localPort,
                        remotePort: remotePort,
                        address: address,
                        status: .active,
                        lastMessage: message
                    )
                )
            },
            onStderr: { output in
                let message = output.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !message.isEmpty else { return }
                let status: PortForwardStatus = message.localizedCaseInsensitiveContains("Forwarding from") ? .active : .starting
                onEvent(
                    PortForwardSession(
                        id: sessionID,
                        contextName: context.name,
                        namespace: namespace,
                        targetKind: targetKind,
                        targetName: targetName,
                        localPort: localPort,
                        remotePort: remotePort,
                        address: address,
                        status: status,
                        lastMessage: message
                    )
                )
            },
            onTermination: { exitCode in
                let status: PortForwardStatus = exitCode == 0 ? .stopped : .failed
                let message = exitCode == 0 ? "Port-forward stopped" : "Port-forward exited with code \(exitCode)"
                onEvent(
                    PortForwardSession(
                        id: sessionID,
                        contextName: context.name,
                        namespace: namespace,
                        targetKind: targetKind,
                        targetName: targetName,
                        localPort: localPort,
                        remotePort: remotePort,
                        address: address,
                        status: status,
                        lastMessage: message
                    )
                )
            }
        )

        await portForwardRegistry.insert(handle: handle, id: sessionID)
        return baseSession
    }

    public func stopPortForward(sessionID: String) async {
        if let handle = await portForwardRegistry.remove(id: sessionID) {
            handle.terminate()
        }
    }

    public func applyYAML(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        yaml: String
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent("rune-apply-\(UUID().uuidString).yaml")

        try yaml.write(to: tempURL, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        _ = try await runKubectl(
            arguments: builder.applyFileArguments(context: context.name, namespace: namespace, filePath: tempURL.path),
            environment: env
        )
    }

    private func runKubectl(arguments: [String], environment: [String: String]) async throws -> CommandResult {
        let result = try await runner.run(
            executable: kubectlPath,
            arguments: ["kubectl"] + arguments,
            environment: environment,
            timeout: commandTimeout
        )

        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(command: "kubectl \(arguments.joined(separator: " "))", message: result.stderr)
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

    private func taggedLines(from logs: String, podName: String) -> [TaggedLogLine] {
        logs
            .split(whereSeparator: \.isNewline)
            .map { String($0) }
            .filter { !$0.isEmpty }
            .map { line in
                TaggedLogLine(podName: podName, text: line, timestamp: parseTimestamp(line))
            }
    }

    private func parseTimestamp(_ line: String) -> Date? {
        let token = line.split(maxSplits: 1, omittingEmptySubsequences: true, whereSeparator: \.isWhitespace).first
        guard let timestampToken = token else { return nil }

        let fractionalFormatter = ISO8601DateFormatter()
        fractionalFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        if let date = fractionalFormatter.date(from: String(timestampToken)) {
            return date
        }

        let formatter = ISO8601DateFormatter()
        return formatter.date(from: String(timestampToken))
    }

    private static func taggedLineSort(lhs: TaggedLogLine, rhs: TaggedLogLine) -> Bool {
        switch (lhs.timestamp, rhs.timestamp) {
        case let (left?, right?):
            if left != right {
                return left < right
            }
        case (nil, .some):
            return false
        case (.some, nil):
            return true
        case (nil, nil):
            break
        }

        if lhs.podName != rhs.podName {
            return lhs.podName < rhs.podName
        }

        return lhs.text < rhs.text
    }

    private struct TaggedLogLine: Sendable {
        let podName: String
        let text: String
        let timestamp: Date?
    }

    private static func parseLineCount(from raw: String) -> Int {
        raw
            .split(whereSeparator: \.isNewline)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { line in
                !line.isEmpty && !line.lowercased().hasPrefix("no resources found")
            }
            .count
    }

    private func isMissingPreviousLogsError(_ error: Error) -> Bool {
        let text = String(describing: error).lowercased()
        return text.contains("previous terminated container")
            || text.contains("no previous terminated container")
            || text.contains("previous container not found")
    }
}

private actor PortForwardRegistry {
    private var handles: [String: any RunningCommandControlling] = [:]

    func insert(handle: any RunningCommandControlling, id: String) {
        handles[id] = handle
    }

    func remove(id: String) -> (any RunningCommandControlling)? {
        handles.removeValue(forKey: id)
    }
}
