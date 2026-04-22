import Foundation
import RuneCore
import RuneDiagnostics
import RuneSecurity

public final class KubectlClient: ContextListingService, NamespaceListingService, PodListingService, DeploymentListingService, ServiceListingService, EventListingService, GenericResourceListingService, PodLogService, UnifiedServiceLogService, UnifiedDeploymentLogService, ManifestService, ResourceWriteService, @unchecked Sendable {
    private let runner: CommandRunning
    private let longRunningRunner: LongRunningCommandRunning
    private let parser: KubectlOutputParser
    private let builder: KubectlCommandBuilder
    private let kubectlPath: String
    private let restClient: KubernetesRESTClient
    /// Optional path to `rune-k8s-agent` (Go + client-go). When nil, ``RuneK8sAgentLocator`` is used.
    private let k8sAgentPath: String?
    private let commandTimeout: TimeInterval
    private let access: SecurityScopedAccess
    private let portForwardRegistry = PortForwardRegistry()

    /// First attempt when using default process timeout; a second attempt uses `retryTimeoutAfterQuickFailure`.
    private let quickKubectlAttemptTimeout: TimeInterval = 12
    /// Second attempt after a timeout on the quick attempt (only when `timeout` parameter is nil).
    private let retryTimeoutAfterQuickFailure: TimeInterval = 90
    /// Explicit ceiling for slow namespaced `kubectl get … -o json` on large / high-latency clusters (skips quick+retry when passed to `runKubectl`).
    private let slowNamespacedJSONListTimeout: TimeInterval = 120
    /// Page size for raw list count pagination (`kubectl get --raw ...?limit=N`).
    private let pagedCountLimit: Int = 250
    /// Hard stop for paged counts to avoid infinite loops on broken continue tokens.
    private let pagedCountMaxPages: Int = 500
    /// Keep pod metrics merge opportunistic so `Workloads > Pods` does not stall on metrics hiccups.
    private let opportunisticPodTopTimeout: TimeInterval = 0.8
    /// Unified logs should stay responsive in large namespaces with many historical pods.
    private let unifiedLogsMaxPods: Int = 8
    private let unifiedLogsMaxConcurrentPodFetches: Int = 3
    /// Keep per-pod log fetch short so one slow pod does not block the whole merged view.
    private let unifiedLogsPerPodTimeout: TimeInterval = 8
    /// One merged backend call can use a slightly larger budget than the per-pod fallback.
    private let unifiedLogsAggregateTimeout: TimeInterval = 20
    /// Selector/pod-discovery for unified logs should fail fast; stale workloads should not block the inspector for minutes.
    private let unifiedLogsSelectorTimeout: TimeInterval = 12

    public init(
        runner: CommandRunning = ProcessCommandRunner(),
        longRunningRunner: LongRunningCommandRunning = ProcessLongRunningCommandRunner(),
        parser: KubectlOutputParser = KubectlOutputParser(),
        builder: KubectlCommandBuilder = KubectlCommandBuilder(),
        kubectlPath: String = "/usr/bin/env",
        k8sAgentPath: String? = nil,
        commandTimeout: TimeInterval = 30,
        access: SecurityScopedAccess = SecurityScopedAccess()
    ) {
        self.runner = runner
        self.longRunningRunner = longRunningRunner
        self.parser = parser
        self.builder = builder
        self.kubectlPath = kubectlPath
        self.restClient = KubernetesRESTClient(runner: runner, kubectlPath: kubectlPath)
        self.k8sAgentPath = k8sAgentPath
        self.commandTimeout = commandTimeout
        self.access = access
    }

    public func listContexts(from sources: [KubeConfigSource]) async throws -> [KubeContext] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(arguments: builder.contextListArguments(), environment: env)
        let parsed = parser.parseContexts(from: result.stdout)
        if !parsed.isEmpty {
            return parsed
        }
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listContexts(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    timeout: commandTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let contexts = try await contextsViaREST(environment: env) {
            return contexts
        }
        return parsed
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
        let parsed = parser.parseNamespaces(from: result.stdout)
        if !parsed.isEmpty {
            return parsed
        }
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listNamespaces(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: commandTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let namespaces = try await namespacesViaREST(environment: env, context: context) {
            return namespaces
        }
        return parsed
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
        if !trimmed.isEmpty {
            return trimmed
        }
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.contextNamespace(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: commandTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let namespace = try await contextNamespaceViaREST(environment: env, context: context) {
            return namespace
        }
        return nil
    }

    public func listPods(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [PodSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listPods(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        let pods: [PodSummary]
        if let restPods = try await listViaREST(
            environment: env,
            context: context,
            resource: "pods",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in
                let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
                return try self.parser.parsePodsListJSON(namespace: namespace, from: trimmed)
            }
        ) {
            pods = restPods
        } else {
            let fallback = try await runKubectl(
                arguments: builder.podListArguments(context: context.name, namespace: namespace),
                environment: env
            )
            let trimmed = fallback.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
            pods = try parser.parsePodsListJSON(namespace: namespace, from: trimmed)
        }

        var merged = pods
        if let agent = resolvedK8sAgentPath() {
            if let metrics = try? await RuneK8sAgentOperationsClient.podTopByName(
                executablePath: agent,
                runner: runner,
                environment: env,
                contextName: context.name,
                namespace: namespace,
                timeout: opportunisticPodTopTimeout
            ) {
                merged = mergePodNameMetrics(merged, metrics)
            }
        } else if let top = await runKubectlAllowFailure(
            arguments: builder.podTopArguments(context: context.name, namespace: namespace),
            environment: env,
            timeout: opportunisticPodTopTimeout
        ),
            top.exitCode == 0 {
            let metrics = parser.parsePodTopByName(from: top.stdout)
            merged = mergePodNameMetrics(merged, metrics)
        }

        return merged
    }

    /// Full JSON list merged into `base` by pod id — keeps status/restarts/age/CPU/mem from `base`, fills IP/node/QoS/ready from JSON.
    public func enrichPodsWithJSONList(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        merging base: [PodSummary]
    ) async throws -> [PodSummary] {
        // Agent list pods already includes the extended fields this enrichment adds.
        // Avoid an extra kubectl round-trip when agent mode is available.
        if resolvedK8sAgentPath() != nil {
            return base
        }
        let env = try kubeconfigEnvironment(from: sources)
        let detailed: [PodSummary]
        if let restPods = try await listViaREST(
            environment: env,
            context: context,
            resource: "pods",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in
                let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
                return try self.parser.parsePodsListJSON(namespace: namespace, from: trimmed)
            }
        ) {
            detailed = restPods
        } else {
            let list = try await runKubectl(
                arguments: builder.podListArguments(context: context.name, namespace: namespace),
                environment: env
            )
            let trimmed = list.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
            detailed = try parser.parsePodsListJSON(namespace: namespace, from: trimmed)
        }
        return Self.mergePodSummariesPreservingMetrics(base: base, detail: detailed)
    }

    /// Single-pod JSON for the inspector overview (IP, node, QoS, ready) — lighter than listing all pods.
    public func fetchPodSummaryForInspector(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String
    ) async throws -> PodSummary {
        let env = try kubeconfigEnvironment(from: sources)
        if let summary = try await resourceViaREST(
            environment: env,
            context: context,
            kind: .pod,
            namespace: namespace,
            name: podName,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in
                let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
                return try self.parser.parseSinglePodJSON(namespace: namespace, from: trimmed)
            }
        ) {
            return summary
        }
        let result = try await runKubectl(
            arguments: builder.resourceJSONArguments(
                context: context.name,
                namespace: namespace,
                kind: .pod,
                name: podName
            ),
            environment: env
        )
        let trimmed = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        return try parser.parseSinglePodJSON(namespace: namespace, from: trimmed)
    }

    public func listPodStatuses(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [PodSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                let pods = try await RuneK8sAgentWorkloadClient.listPods(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
                return pods.map { pod in
                    PodSummary(
                        name: pod.name,
                        namespace: pod.namespace,
                        status: pod.status,
                        totalRestarts: pod.totalRestarts,
                        ageDescription: pod.ageDescription
                    )
                }
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with REST fallback.
            }
        }
        if let restPods = try await listViaREST(
            environment: env,
            context: context,
            resource: "pods",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in
                let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
                return try self.parser.parsePodsListJSON(namespace: namespace, from: trimmed)
            }
        ) {
            return restPods.map { pod in
                PodSummary(
                    name: pod.name,
                    namespace: pod.namespace,
                    status: pod.status,
                    totalRestarts: pod.totalRestarts,
                    ageDescription: pod.ageDescription
                )
            }
        }

        let result = try await runKubectl(
            arguments: builder.podStatusListArguments(context: context.name, namespace: namespace),
            environment: env
        )
        return parser.parsePods(namespace: namespace, from: result.stdout)
    }

    public func listPodsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [PodSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listPodsAllNamespaces(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        let pods: [PodSummary]
        if let restPods = try await listViaREST(
            environment: env,
            context: context,
            resource: "pods",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in
                let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
                return try self.parser.parsePodsListJSONAllNamespaces(from: trimmed)
            }
        ) {
            pods = restPods
        } else {
            let fallback = try await runKubectl(
                arguments: builder.podListAllNamespacesArguments(context: context.name),
                environment: env
            )
            let trimmed = fallback.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
            pods = try parser.parsePodsListJSONAllNamespaces(from: trimmed)
        }

        var merged = pods
        if let agent = resolvedK8sAgentPath() {
            if let metrics = try? await RuneK8sAgentOperationsClient.podTopByNamespaceAndName(
                executablePath: agent,
                runner: runner,
                environment: env,
                contextName: context.name,
                timeout: opportunisticPodTopTimeout
            ) {
                merged = mergePodNamespacedMetrics(merged, metrics)
            }
        } else if let top = await runKubectlAllowFailure(
            arguments: builder.podTopAllNamespacesArguments(context: context.name),
            environment: env,
            timeout: opportunisticPodTopTimeout
        ),
            top.exitCode == 0 {
            let metrics = parser.parsePodTopByNamespaceAndName(from: top.stdout)
            merged = mergePodNamespacedMetrics(merged, metrics)
        }

        return merged
    }

    public func listDeployments(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [DeploymentSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listDeployments(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent (client-go) failed; continue with REST.
            }
        }
        if let deployments = try await listViaREST(
            environment: env,
            context: context,
            resource: "deployments",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseDeployments(namespace: namespace, from: raw) }
        ) {
            return deployments
        }
        let fallback = try await runKubectl(
            arguments: builder.deploymentListArguments(context: context.name, namespace: namespace),
            environment: env
        )
        return try parser.parseDeployments(namespace: namespace, from: fallback.stdout)
    }

    public func listDeploymentsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [DeploymentSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listDeploymentsAllNamespaces(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let deployments = try await listViaREST(
            environment: env,
            context: context,
            resource: "deployments",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseDeployments(namespace: "", from: raw) }
        ) {
            return deployments
        }
        let fallback = try await runKubectl(
            arguments: builder.deploymentListAllNamespacesArguments(context: context.name),
            environment: env
        )
        return try parser.parseDeployments(namespace: "", from: fallback.stdout)
    }

    public func listServices(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ServiceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listServices(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let services = try await listViaREST(
            environment: env,
            context: context,
            resource: "services",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseServices(namespace: namespace, from: raw) }
        ) {
            return services
        }
        let result = try await runKubectl(
            arguments: builder.serviceListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseServices(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "services JSON could not be parsed")
        }
    }

    public func listServicesAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ServiceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listServicesAllNamespaces(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let services = try await listViaREST(
            environment: env,
            context: context,
            resource: "services",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseServices(namespace: "", from: raw) }
        ) {
            return services
        }
        let result = try await runKubectl(
            arguments: builder.serviceListAllNamespacesArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseServices(namespace: "", from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "services JSON could not be parsed")
        }
    }

    public func listStatefulSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listStatefulSets(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent (client-go) failed; continue with REST.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "statefulsets",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseStatefulSets(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.statefulSetListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseStatefulSets(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "statefulsets JSON could not be parsed")
        }
    }

    public func listDaemonSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listDaemonSets(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent (client-go) failed; continue with REST.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "daemonsets",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseDaemonSets(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.daemonSetListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseDaemonSets(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "daemonsets JSON could not be parsed")
        }
    }

    public func listJobs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listJobs(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent (client-go) failed; continue with REST.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "jobs",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseJobs(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let fallback = try await runKubectl(
            arguments: builder.jobListArguments(context: context.name, namespace: namespace),
            environment: env,
            timeout: slowNamespacedJSONListTimeout
        )
        do {
            return try parser.parseJobs(namespace: namespace, from: fallback.stdout)
        } catch {
            throw RuneError.parseError(message: "jobs could not be parsed")
        }
    }

    public func listCronJobs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listCronJobs(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent (client-go) failed; continue with REST.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "cronjobs",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseCronJobs(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let fallback = try await runKubectl(
            arguments: builder.cronJobListArguments(context: context.name, namespace: namespace),
            environment: env,
            timeout: slowNamespacedJSONListTimeout
        )
        do {
            return try parser.parseCronJobs(namespace: namespace, from: fallback.stdout)
        } catch {
            throw RuneError.parseError(message: "cronjobs could not be parsed")
        }
    }

    public func listReplicaSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listReplicaSets(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent (client-go) failed; continue with REST.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "replicasets",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseReplicaSets(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.replicaSetListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseReplicaSets(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "replicasets JSON could not be parsed")
        }
    }

    public func listPersistentVolumeClaims(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listPersistentVolumeClaims(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "persistentvolumeclaims",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parsePersistentVolumeClaims(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.persistentVolumeClaimListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parsePersistentVolumeClaims(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "PVC JSON could not be parsed")
        }
    }

    public func listPersistentVolumes(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listPersistentVolumes(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "persistentvolumes",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parsePersistentVolumes(from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.persistentVolumeListArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parsePersistentVolumes(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "PV JSON could not be parsed")
        }
    }

    public func listStorageClasses(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listStorageClasses(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "storageclasses",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseStorageClasses(from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.storageClassListArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseStorageClasses(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "StorageClass JSON could not be parsed")
        }
    }

    public func listHorizontalPodAutoscalers(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listHorizontalPodAutoscalers(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "horizontalpodautoscalers",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseHorizontalPodAutoscalers(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.horizontalPodAutoscalerListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseHorizontalPodAutoscalers(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "HPA JSON could not be parsed")
        }
    }

    public func listNetworkPolicies(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listNetworkPolicies(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "networkpolicies",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseNetworkPolicies(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.networkPolicyListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseNetworkPolicies(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "NetworkPolicy JSON could not be parsed")
        }
    }

    public func listIngresses(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listIngresses(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "ingresses",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseIngresses(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.ingressListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseIngresses(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "ingresses JSON could not be parsed")
        }
    }

    public func listIngressesAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listIngressesAllNamespaces(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "ingresses",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseIngresses(namespace: "", from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.ingressListAllNamespacesArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseIngresses(namespace: "", from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "ingresses JSON could not be parsed")
        }
    }

    public func listConfigMaps(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listConfigMaps(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "configmaps",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseConfigMaps(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.configMapListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseConfigMaps(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "configmaps JSON could not be parsed")
        }
    }

    public func listConfigMapsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listConfigMapsAllNamespaces(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "configmaps",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseConfigMaps(namespace: "", from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.configMapListAllNamespacesArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseConfigMaps(namespace: "", from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "configmaps JSON could not be parsed")
        }
    }

    public func listSecrets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listSecrets(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "secrets",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseSecrets(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.secretListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseSecrets(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "secrets JSON could not be parsed")
        }
    }

    public func listNodes(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listNodes(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "nodes",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseNodes(from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.nodeListArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseNodes(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "nodes JSON could not be parsed")
        }
    }

    public func listEvents(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [EventSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listEvents(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: 20
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "events",
            namespace: namespace,
            timeout: 20,
            parse: { raw in try self.parser.parseEvents(from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.eventListArguments(context: context.name, namespace: namespace),
            environment: env,
            timeout: 20
        )

        do {
            return try parser.parseEvents(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "events JSON could not be parsed")
        }
    }

    public func listEventsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [EventSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listEventsAllNamespaces(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: 20
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "events",
            namespace: nil,
            timeout: 20,
            parse: { raw in try self.parser.parseEvents(from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.eventListAllNamespacesArguments(context: context.name),
            environment: env,
            timeout: 20
        )

        do {
            return try parser.parseEvents(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "events JSON could not be parsed")
        }
    }

    public func listRoles(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listRoles(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "roles",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseRoles(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.roleListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseRoles(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "roles JSON could not be parsed")
        }
    }

    public func listRoleBindings(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listRoleBindings(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "rolebindings",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseRoleBindings(namespace: namespace, from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.roleBindingListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseRoleBindings(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "rolebindings JSON could not be parsed")
        }
    }

    public func listClusterRoles(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listClusterRoles(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "clusterroles",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseClusterRoles(from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.clusterRoleListArguments(context: context.name),
            environment: env,
            timeout: 90
        )

        do {
            return try parser.parseClusterRoles(from: result.stdout)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.clusterRoleListTextArguments(context: context.name),
                environment: env,
                timeout: 75
            )
            return parser.parseClusterRoleNames(from: fallback.stdout)
        }
    }

    public func listClusterRoleBindings(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentWorkloadClient.listClusterRoleBindings(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Go agent failed; continue with REST fallback.
            }
        }
        if let items = try await listViaREST(
            environment: env,
            context: context,
            resource: "clusterrolebindings",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseClusterRoleBindings(from: raw) }
        ) {
            return items
        }
        let result = try await runKubectl(
            arguments: builder.clusterRoleBindingListArguments(context: context.name),
            environment: env,
            timeout: 90
        )

        do {
            return try parser.parseClusterRoleBindings(from: result.stdout)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.clusterRoleBindingListTextArguments(context: context.name),
                environment: env,
                timeout: 75
            )
            return parser.parseClusterRoleBindingNames(from: fallback.stdout)
        }
    }

    public func countNamespacedResources(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        resource: String,
        progress: ((Int) -> Void)? = nil
    ) async throws -> Int {
        let env = try kubeconfigEnvironment(from: sources)
        if let agentCount = await namespacedResourceCountViaAgentIfPossible(
            context: context,
            namespace: namespace,
            resource: resource,
            environment: env
        ) {
            progress?(agentCount)
            return agentCount
        }
        if let apiPath = builder.namespacedResourceListMetadataAPIPath(namespace: namespace, resource: resource),
           let total = await collectionListTotalFromMetadataProbe(
                context: context,
                environment: env,
                apiPath: apiPath
           ) {
            progress?(total)
            return total
        }
        if let paged = await pagedNamespacedCollectionCount(
            context: context,
            namespace: namespace,
            resource: resource,
            environment: env,
            progress: progress
        ) {
            return paged
        }

        let result = try await runKubectl(
            arguments: builder.namespacedResourceCountArguments(
                context: context.name,
                namespace: namespace,
                resource: resource
            ),
            environment: env,
            timeout: 90
        )
        let total = Self.parseLineCount(from: result.stdout)
        progress?(total)
        return total
    }

    /// Prefer Go helper (client-go) counts where supported; return `nil` for unsupported resources.
    /// Returning `nil` keeps existing kubectl-based count fallbacks intact.
    private func namespacedResourceCountViaAgentIfPossible(
        context: KubeContext,
        namespace: String,
        resource: String,
        environment: [String: String]
    ) async -> Int? {
        guard let agent = resolvedK8sAgentPath() else { return nil }

        let normalized = resource.lowercased()
        let timeout = slowNamespacedJSONListTimeout
        do {
            switch normalized {
            case "pods", "pod":
                let rows = try await RuneK8sAgentWorkloadClient.listPods(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "services", "service":
                let rows = try await RuneK8sAgentWorkloadClient.listServices(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "ingresses", "ingress":
                let rows = try await RuneK8sAgentWorkloadClient.listIngresses(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "configmaps", "configmap":
                let rows = try await RuneK8sAgentWorkloadClient.listConfigMaps(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "events", "event":
                let rows = try await RuneK8sAgentWorkloadClient.listEvents(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "secrets", "secret":
                let rows = try await RuneK8sAgentWorkloadClient.listSecrets(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "roles", "role":
                let rows = try await RuneK8sAgentWorkloadClient.listRoles(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "rolebindings", "rolebinding":
                let rows = try await RuneK8sAgentWorkloadClient.listRoleBindings(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "persistentvolumeclaims", "persistentvolumeclaim", "pvc":
                let rows = try await RuneK8sAgentWorkloadClient.listPersistentVolumeClaims(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "horizontalpodautoscalers", "horizontalpodautoscaler", "hpa":
                let rows = try await RuneK8sAgentWorkloadClient.listHorizontalPodAutoscalers(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "networkpolicies", "networkpolicy":
                let rows = try await RuneK8sAgentWorkloadClient.listNetworkPolicies(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "deployments", "deployment":
                let rows = try await RuneK8sAgentWorkloadClient.listDeployments(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "statefulsets", "statefulset":
                let rows = try await RuneK8sAgentWorkloadClient.listStatefulSets(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "daemonsets", "daemonset":
                let rows = try await RuneK8sAgentWorkloadClient.listDaemonSets(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "jobs", "job":
                let rows = try await RuneK8sAgentWorkloadClient.listJobs(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "cronjobs", "cronjob":
                let rows = try await RuneK8sAgentWorkloadClient.listCronJobs(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            case "replicasets", "replicaset":
                let rows = try await RuneK8sAgentWorkloadClient.listReplicaSets(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.count
            default:
                return nil
            }
        } catch {
            return nil
        }
    }

    public func countClusterResources(
        from sources: [KubeConfigSource],
        context: KubeContext,
        resource: String,
        progress: ((Int) -> Void)? = nil
    ) async throws -> Int {
        let env = try kubeconfigEnvironment(from: sources)
        if let agentCount = await clusterResourceCountViaAgentIfPossible(
            context: context,
            resource: resource,
            environment: env
        ) {
            progress?(agentCount)
            return agentCount
        }
        if let apiPath = builder.clusterResourceListMetadataAPIPath(resource: resource),
           let total = await collectionListTotalFromMetadataProbe(
                context: context,
                environment: env,
                apiPath: apiPath
           ) {
            progress?(total)
            return total
        }
        if let paged = await pagedClusterCollectionCount(
            context: context,
            resource: resource,
            environment: env,
            progress: progress
        ) {
            return paged
        }

        let result = try await runKubectl(
            arguments: builder.clusterResourceCountArguments(
                context: context.name,
                resource: resource
            ),
            environment: env,
            timeout: 90
        )
        let total = Self.parseLineCount(from: result.stdout)
        progress?(total)
        return total
    }

    private func clusterResourceCountViaAgentIfPossible(
        context: KubeContext,
        resource: String,
        environment: [String: String]
    ) async -> Int? {
        guard let agent = resolvedK8sAgentPath() else { return nil }
        switch resource.lowercased() {
        case "nodes", "node":
            do {
                let rows = try await RuneK8sAgentWorkloadClient.listNodes(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
                return rows.count
            } catch {
                return nil
            }
        case "persistentvolumes", "persistentvolume", "pv":
            do {
                let rows = try await RuneK8sAgentWorkloadClient.listPersistentVolumes(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
                return rows.count
            } catch {
                return nil
            }
        case "storageclasses", "storageclass":
            do {
                let rows = try await RuneK8sAgentWorkloadClient.listStorageClasses(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
                return rows.count
            } catch {
                return nil
            }
        case "clusterroles", "clusterrole":
            do {
                let rows = try await RuneK8sAgentWorkloadClient.listClusterRoles(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
                return rows.count
            } catch {
                return nil
            }
        case "clusterrolebindings", "clusterrolebinding":
            do {
                let rows = try await RuneK8sAgentWorkloadClient.listClusterRoleBindings(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    timeout: slowNamespacedJSONListTimeout
                )
                return rows.count
            } catch {
                return nil
            }
        default:
            return nil
        }
    }

    /// Chunked raw list count: iterate `limit` pages and accumulate `items.count` from each response.
    /// Uses `remainingItemCount` for early finish when the apiserver provides it.
    private func pagedNamespacedCollectionCount(
        context: KubeContext,
        namespace: String,
        resource: String,
        environment: [String: String],
        progress: ((Int) -> Void)?
    ) async -> Int? {
        guard let first = KubernetesRESTPath.namespacedCollectionRequest(
            namespace: namespace,
            resource: resource,
            options: KubernetesListOptions(limit: pagedCountLimit)
        ) else {
            return nil
        }

        return await pagedCollectionCount(
            context: context,
            firstRequest: first,
            nextRequest: { token in
                KubernetesRESTPath.namespacedCollectionRequest(
                    namespace: namespace,
                    resource: resource,
                    options: KubernetesListOptions(limit: pagedCountLimit, continueToken: token)
                )
            },
            environment: environment,
            progress: progress
        )
    }

    private func pagedClusterCollectionCount(
        context: KubeContext,
        resource: String,
        environment: [String: String],
        progress: ((Int) -> Void)?
    ) async -> Int? {
        guard let first = KubernetesRESTPath.clusterCollectionRequest(
            resource: resource,
            options: KubernetesListOptions(limit: pagedCountLimit)
        ) else {
            return nil
        }

        return await pagedCollectionCount(
            context: context,
            firstRequest: first,
            nextRequest: { token in
                KubernetesRESTPath.clusterCollectionRequest(
                    resource: resource,
                    options: KubernetesListOptions(limit: pagedCountLimit, continueToken: token)
                )
            },
            environment: environment,
            progress: progress
        )
    }

    private func pagedCollectionCount(
        context: KubeContext,
        firstRequest: KubernetesRESTRequest,
        nextRequest: (String) -> KubernetesRESTRequest?,
        environment: [String: String],
        progress: ((Int) -> Void)?
    ) async -> Int? {
        var request = firstRequest
        var total = 0

        for _ in 0..<pagedCountMaxPages {
            let raw: String
            do {
                raw = try await restClient.rawGET(
                    environment: environment,
                    contextName: context.name,
                    apiPath: request.apiPath,
                    timeout: 45
                )
            } catch {
                return nil
            }

            guard let page = KubectlListJSON.collectionPageInfo(from: raw) else {
                return nil
            }
            total += page.itemsCount
            progress?(total)

            if let remaining = page.remainingItemCount {
                let predicted = total + remaining
                if predicted != total {
                    progress?(predicted)
                }
                return predicted
            }

            guard let token = page.continueToken else {
                return total
            }
            guard let next = nextRequest(token) else {
                return nil
            }
            request = next
        }

        return nil
    }

    /// Cheap total via `kubectl get --raw` + `limit=1` list (`metadata.remainingItemCount` + 1). Returns `nil` on failure or when the server omits a derivable total.
    private func collectionListTotalFromMetadataProbe(
        context: KubeContext,
        environment: [String: String],
        apiPath: String
    ) async -> Int? {
        do {
            let raw = try await restClient.rawGET(
                environment: environment,
                contextName: context.name,
                apiPath: apiPath,
                timeout: 45
            )
            return KubectlListJSON.collectionListTotal(from: raw)
        } catch {
            return nil
        }
    }

    public func clusterUsagePercent(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async -> (cpuPercent: Int?, memoryPercent: Int?) {
        guard let env = try? kubeconfigEnvironment(from: sources) else {
            return (nil, nil)
        }

        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentOperationsClient.clusterUsagePercent(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    timeout: 3
                )
            } catch {
                if isCancellationLikeError(error) {
                    return (nil, nil)
                }
                // Continue with kubectl fallback.
            }
        }

        guard let result = await runKubectlAllowFailure(
            arguments: builder.nodeTopArguments(context: context.name),
            environment: env,
            timeout: 3
        ), result.exitCode == 0 else {
            return (nil, nil)
        }

        return parser.parseNodeTopUsagePercent(from: result.stdout)
    }

    public func podLogs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> String {
        try await podLogs(
            from: sources,
            context: context,
            namespace: namespace,
            podName: podName,
            filter: filter,
            previous: previous,
            timeoutOverride: nil,
            profile: .pod
        )
    }

    private func podLogs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        filter: LogTimeFilter,
        previous: Bool,
        timeoutOverride: TimeInterval?,
        profile: LogQueryProfile = .pod
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        let timeoutBudget = timeoutOverride ?? logFetchTimeout(for: filter)
        if let agent = resolvedK8sAgentPath() {
            do {
                let logs = try await RuneK8sAgentOperationsClient.podLogs(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    podName: podName,
                    filter: filter,
                    previous: previous,
                    timeout: timeoutBudget,
                    profile: profile
                )
                if !logs.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    return logs
                }
            } catch {
                try rethrowCancellationIfNeeded(error)
                if previous, isMissingPreviousLogsError(error) {
                    return "No previous logs available for \(podName)."
                }
                // Continue with REST fallback.
            }
        }
        if let logs = try await podLogsViaREST(
            environment: env,
            context: context,
            namespace: namespace,
            podName: podName,
            filter: filter,
            previous: previous,
            timeout: timeoutBudget,
            profile: profile
        ) {
            if !logs.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                return logs
            }
        }

        let arguments = builder.podLogsArguments(
            context: context.name,
            namespace: namespace,
            podName: podName,
            container: nil,
            filter: filter,
            previous: previous,
            follow: false,
            profile: profile
        )

        // Fail fast: short first attempt surfaces wedged API servers quickly; second attempt uses the full budget.
        let budget = timeoutBudget
        let phase1 = min(20, max(12, budget / 3))
        let phase2 = min(60, max(phase1 + 5, budget))

        let result: CommandResult
        do {
            result = try await runKubectlOnce(arguments: arguments, environment: env, timeout: phase1)
        } catch {
            if previous, isMissingPreviousLogsError(error) {
                return "No previous logs available for \(podName)."
            }
            guard isProcessTimeoutError(error) else {
                throw error
            }
            result = try await runKubectlOnce(arguments: arguments, environment: env, timeout: phase2)
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

        let selectorMap: [String: String]
        if let agent = resolvedK8sAgentPath() {
            do {
                selectorMap = try await RuneK8sAgentOperationsClient.serviceSelector(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    serviceName: service.name,
                    timeout: unifiedLogsSelectorTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                if let selector = try await serviceSelectorViaREST(
                    environment: env,
                    context: context,
                    namespace: namespace,
                    serviceName: service.name
                ) {
                    selectorMap = selector
                } else {
                    let serviceJSON = try await runKubectl(
                        arguments: builder.serviceJSONArguments(context: context.name, namespace: namespace, serviceName: service.name),
                        environment: env,
                        timeout: unifiedLogsSelectorTimeout
                    )
                    do {
                        selectorMap = try parser.parseServiceSelector(from: serviceJSON.stdout)
                    } catch {
                        throw RuneError.parseError(message: "service selector could not be parsed")
                    }
                }
            }
        } else {
            if let selector = try await serviceSelectorViaREST(
                environment: env,
                context: context,
                namespace: namespace,
                serviceName: service.name
            ) {
                selectorMap = selector
            } else {
                let serviceJSON = try await runKubectl(
                    arguments: builder.serviceJSONArguments(context: context.name, namespace: namespace, serviceName: service.name),
                    environment: env,
                    timeout: unifiedLogsSelectorTimeout
                )
                do {
                    selectorMap = try parser.parseServiceSelector(from: serviceJSON.stdout)
                } catch {
                    throw RuneError.parseError(message: "service selector could not be parsed")
                }
            }
        }

        guard !selectorMap.isEmpty else {
            throw RuneError.parseError(message: "Service \(service.name) is missing a selector and cannot be used for unified logs")
        }

        let selector = selectorMap
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: ",")

        if let agent = resolvedK8sAgentPath() {
            do {
                let unified = try await RuneK8sAgentOperationsClient.unifiedLogsBySelector(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    selector: selector,
                    filter: filter,
                    previous: previous,
                    maxPods: unifiedLogsMaxPods,
                    concurrency: unifiedLogsMaxConcurrentPodFetches,
                    timeout: unifiedLogsAggregateTimeout
                )
                if unified.podNames.isEmpty {
                    return UnifiedServiceLogs(service: service, podNames: [], mergedText: "No pods found for service selector: \(selector)")
                }
                if !unified.mergedText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    return UnifiedServiceLogs(
                        service: service,
                        podNames: unified.podNames,
                        mergedText: unified.mergedText
                    )
                }
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with per-pod fallback.
            }
        }

        let pods: [PodSummary]
        if let agent = resolvedK8sAgentPath() {
            do {
                pods = try await RuneK8sAgentOperationsClient.podsBySelector(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    selector: selector,
                    timeout: unifiedLogsSelectorTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                if let restPods = try await podsBySelectorViaREST(
                    environment: env,
                    context: context,
                    namespace: namespace,
                    selector: selector
                ) {
                    pods = restPods
                } else {
                    let podResult = try await runKubectl(
                        arguments: builder.podsByLabelSelectorArguments(context: context.name, namespace: namespace, selector: selector),
                        environment: env,
                        timeout: unifiedLogsSelectorTimeout
                    )

                    let podsTrimmed = podResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                    do {
                        pods = try parser.parsePodsListJSON(namespace: namespace, from: podsTrimmed)
                    } catch {
                        let fallback = try await runKubectl(
                            arguments: builder.podsByLabelSelectorTextArguments(context: context.name, namespace: namespace, selector: selector),
                            environment: env,
                            timeout: unifiedLogsSelectorTimeout
                        )
                        pods = parser.parsePods(namespace: namespace, from: fallback.stdout)
                    }
                }
            }
        } else {
            if let restPods = try await podsBySelectorViaREST(
                environment: env,
                context: context,
                namespace: namespace,
                selector: selector
            ) {
                pods = restPods
            } else {
                let podResult = try await runKubectl(
                    arguments: builder.podsByLabelSelectorArguments(context: context.name, namespace: namespace, selector: selector),
                    environment: env,
                    timeout: unifiedLogsSelectorTimeout
                )

                let podsTrimmed = podResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                do {
                    pods = try parser.parsePodsListJSON(namespace: namespace, from: podsTrimmed)
                } catch {
                    let fallback = try await runKubectl(
                        arguments: builder.podsByLabelSelectorTextArguments(context: context.name, namespace: namespace, selector: selector),
                        environment: env,
                        timeout: unifiedLogsSelectorTimeout
                    )
                    pods = parser.parsePods(namespace: namespace, from: fallback.stdout)
                }
            }
        }
        let selectedPods = selectPodsForUnifiedLogs(pods)
        guard !selectedPods.isEmpty else {
            return UnifiedServiceLogs(service: service, podNames: [], mergedText: "No pods found for service selector: \(selector)")
        }

        let collectedLines = try await collectUnifiedPodLogLines(
            pods: selectedPods,
            sources: sources,
            context: context,
            namespace: namespace,
            filter: filter,
            previous: previous
        )

        let merged = collectedLines
            .sorted(by: Self.taggedLineSort)
            .map { line in
                "[\(line.podName)] \(line.text)"
            }
            .joined(separator: "\n")

        return UnifiedServiceLogs(
            service: service,
            podNames: selectedPods.map(\.name).sorted(),
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

        let selectorMap: [String: String]
        if let agent = resolvedK8sAgentPath() {
            do {
                selectorMap = try await RuneK8sAgentOperationsClient.deploymentSelector(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    deploymentName: deployment.name,
                    timeout: unifiedLogsSelectorTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                if let selector = try await deploymentSelectorViaREST(
                    environment: env,
                    context: context,
                    namespace: namespace,
                    deploymentName: deployment.name
                ) {
                    selectorMap = selector
                } else {
                    let deploymentJSON = try await runKubectl(
                        arguments: builder.deploymentJSONArguments(context: context.name, namespace: namespace, deploymentName: deployment.name),
                        environment: env,
                        timeout: unifiedLogsSelectorTimeout
                    )

                    do {
                        selectorMap = try parser.parseDeploymentSelector(from: deploymentJSON.stdout)
                    } catch {
                        throw RuneError.parseError(message: "deployment selector could not be parsed")
                    }
                }
            }
        } else {
            if let selector = try await deploymentSelectorViaREST(
                environment: env,
                context: context,
                namespace: namespace,
                deploymentName: deployment.name
            ) {
                selectorMap = selector
            } else {
                let deploymentJSON = try await runKubectl(
                    arguments: builder.deploymentJSONArguments(context: context.name, namespace: namespace, deploymentName: deployment.name),
                    environment: env,
                    timeout: unifiedLogsSelectorTimeout
                )

                do {
                    selectorMap = try parser.parseDeploymentSelector(from: deploymentJSON.stdout)
                } catch {
                    throw RuneError.parseError(message: "deployment selector could not be parsed")
                }
            }
        }

        guard !selectorMap.isEmpty else {
            throw RuneError.parseError(message: "Deployment \(deployment.name) is missing a matchLabels selector and cannot be used for unified logs")
        }

        let selector = selectorMap
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: ",")

        if let agent = resolvedK8sAgentPath() {
            do {
                let unified = try await RuneK8sAgentOperationsClient.unifiedLogsBySelector(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    selector: selector,
                    filter: filter,
                    previous: previous,
                    maxPods: unifiedLogsMaxPods,
                    concurrency: unifiedLogsMaxConcurrentPodFetches,
                    timeout: unifiedLogsAggregateTimeout
                )
                if unified.podNames.isEmpty {
                    return UnifiedDeploymentLogs(deployment: deployment, podNames: [], mergedText: "No pods found for deployment selector: \(selector)")
                }
                if !unified.mergedText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    return UnifiedDeploymentLogs(
                        deployment: deployment,
                        podNames: unified.podNames,
                        mergedText: unified.mergedText
                    )
                }
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with per-pod fallback.
            }
        }

        let pods: [PodSummary]
        if let agent = resolvedK8sAgentPath() {
            do {
                pods = try await RuneK8sAgentOperationsClient.podsBySelector(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    selector: selector,
                    timeout: unifiedLogsSelectorTimeout
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                if let restPods = try await podsBySelectorViaREST(
                    environment: env,
                    context: context,
                    namespace: namespace,
                    selector: selector
                ) {
                    pods = restPods
                } else {
                    let podResult = try await runKubectl(
                        arguments: builder.podsByLabelSelectorArguments(context: context.name, namespace: namespace, selector: selector),
                        environment: env,
                        timeout: unifiedLogsSelectorTimeout
                    )

                    let podsTrimmed = podResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                    do {
                        pods = try parser.parsePodsListJSON(namespace: namespace, from: podsTrimmed)
                    } catch {
                        let fallback = try await runKubectl(
                            arguments: builder.podsByLabelSelectorTextArguments(context: context.name, namespace: namespace, selector: selector),
                            environment: env,
                            timeout: unifiedLogsSelectorTimeout
                        )
                        pods = parser.parsePods(namespace: namespace, from: fallback.stdout)
                    }
                }
            }
        } else {
            if let restPods = try await podsBySelectorViaREST(
                environment: env,
                context: context,
                namespace: namespace,
                selector: selector
            ) {
                pods = restPods
            } else {
                let podResult = try await runKubectl(
                    arguments: builder.podsByLabelSelectorArguments(context: context.name, namespace: namespace, selector: selector),
                    environment: env,
                    timeout: unifiedLogsSelectorTimeout
                )

                let podsTrimmed = podResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                do {
                    pods = try parser.parsePodsListJSON(namespace: namespace, from: podsTrimmed)
                } catch {
                    let fallback = try await runKubectl(
                        arguments: builder.podsByLabelSelectorTextArguments(context: context.name, namespace: namespace, selector: selector),
                        environment: env,
                        timeout: unifiedLogsSelectorTimeout
                    )
                    pods = parser.parsePods(namespace: namespace, from: fallback.stdout)
                }
            }
        }
        let selectedPods = selectPodsForUnifiedLogs(pods)
        guard !selectedPods.isEmpty else {
            return UnifiedDeploymentLogs(deployment: deployment, podNames: [], mergedText: "No pods found for deployment selector: \(selector)")
        }

        let collectedLines = try await collectUnifiedPodLogLines(
            pods: selectedPods,
            sources: sources,
            context: context,
            namespace: namespace,
            filter: filter,
            previous: previous
        )

        let merged = collectedLines
            .sorted(by: Self.taggedLineSort)
            .map { line in
                "[\(line.podName)] \(line.text)"
            }
            .joined(separator: "\n")

        return UnifiedDeploymentLogs(
            deployment: deployment,
            podNames: selectedPods.map(\.name).sorted(),
            mergedText: merged
        )
    }

    private func selectPodsForUnifiedLogs(_ pods: [PodSummary]) -> [PodSummary] {
        guard !pods.isEmpty else { return [] }
        let active = pods.filter { pod in
            let status = pod.status.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            return status == "running" || status == "pending" || status == "unknown"
        }
        let source = active.isEmpty ? pods : active
        return Array(source.prefix(unifiedLogsMaxPods))
    }

    private func collectUnifiedPodLogLines(
        pods: [PodSummary],
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> [TaggedLogLine] {
        guard !pods.isEmpty else { return [] }

        var collectedLines: [TaggedLogLine] = []
        try await withThrowingTaskGroup(of: [TaggedLogLine].self) { group in
            var nextPodIndex = 0
            let initial = min(unifiedLogsMaxConcurrentPodFetches, pods.count)

            for _ in 0..<initial {
                let pod = pods[nextPodIndex]
                nextPodIndex += 1
                group.addTask {
                    do {
                        let logs = try await self.podLogs(
                            from: sources,
                            context: context,
                            namespace: namespace,
                            podName: pod.name,
                            filter: filter,
                            previous: previous,
                            timeoutOverride: self.unifiedLogsPerPodTimeout,
                            profile: .unifiedPerPod
                        )
                        return self.taggedLines(from: logs, podName: pod.name)
                    } catch {
                        if error is CancellationError {
                            throw error
                        }
                        // Keep unified logs responsive even when one pod log fetch fails.
                        return []
                    }
                }
            }

            while let podLines = try await group.next() {
                collectedLines.append(contentsOf: podLines)
                if nextPodIndex < pods.count {
                    let pod = pods[nextPodIndex]
                    nextPodIndex += 1
                    group.addTask {
                        do {
                            let logs = try await self.podLogs(
                                from: sources,
                                context: context,
                                namespace: namespace,
                                podName: pod.name,
                                filter: filter,
                                previous: previous,
                                timeoutOverride: self.unifiedLogsPerPodTimeout,
                                profile: .unifiedPerPod
                            )
                            return self.taggedLines(from: logs, podName: pod.name)
                        } catch {
                            if error is CancellationError {
                                throw error
                            }
                            return []
                        }
                    }
                }
            }
        }

        return collectedLines
    }

    public func resourceYAML(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        if let manifest = try await resourceYAMLViaREST(
            environment: env,
            context: context,
            namespace: namespace,
            kind: kind,
            name: name
        ) {
            return manifest
        }
        let result = try await runKubectl(
            arguments: builder.resourceYAMLArguments(context: context.name, namespace: namespace, kind: kind, name: name),
            environment: env
        )

        return result.stdout
    }

    public func resourceDescribe(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.describeResourceArguments(
                context: context.name,
                namespace: namespace,
                kind: kind,
                name: name
            ),
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
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentOperationsClient.execInPod(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    podName: podName,
                    container: container,
                    command: command,
                    timeout: 90
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with kubectl fallback.
            }
        }
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
        if let agent = resolvedK8sAgentPath() {
            do {
                try await RuneK8sAgentOperationsClient.deleteResource(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    kind: kind,
                    name: name,
                    timeout: 90
                )
                return
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with REST fallback.
            }
        }
        if try await deleteResourceViaREST(
            environment: env,
            context: context,
            namespace: namespace,
            kind: kind,
            name: name
        ) {
            return
        }
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
        if let agent = resolvedK8sAgentPath() {
            do {
                try await RuneK8sAgentOperationsClient.scaleDeployment(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    deploymentName: deploymentName,
                    replicas: replicas,
                    timeout: 90
                )
                return
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with REST fallback.
            }
        }
        if try await scaleDeploymentViaREST(
            environment: env,
            context: context,
            namespace: namespace,
            deploymentName: deploymentName,
            replicas: replicas
        ) {
            return
        }
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
        if let agent = resolvedK8sAgentPath() {
            do {
                try await RuneK8sAgentOperationsClient.restartDeploymentRollout(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    deploymentName: deploymentName,
                    timeout: 90
                )
                return
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with REST fallback.
            }
        }
        if try await restartDeploymentRolloutViaREST(
            environment: env,
            context: context,
            namespace: namespace,
            deploymentName: deploymentName
        ) {
            return
        }
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
        if let agent = resolvedK8sAgentPath() {
            do {
                return try await RuneK8sAgentOperationsClient.deploymentRolloutHistory(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    deploymentName: deploymentName,
                    timeout: 90
                )
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with kubectl fallback.
            }
        }
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
        if let agent = resolvedK8sAgentPath() {
            do {
                try await RuneK8sAgentOperationsClient.rollbackDeploymentRollout(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    deploymentName: deploymentName,
                    revision: revision,
                    timeout: 90
                )
                return
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with kubectl fallback.
            }
        }
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

        let executable: String
        let arguments: [String]
        if let agent = resolvedK8sAgentPath() {
            executable = agent
            arguments = RuneK8sAgentOperationsClient.portForwardArguments(
                contextName: context.name,
                namespace: namespace,
                targetKind: targetKind,
                targetName: targetName,
                localPort: localPort,
                remotePort: remotePort,
                address: address
            )
        } else {
            executable = kubectlPath
            arguments = ["kubectl"] + builder.portForwardArguments(
                context: context.name,
                namespace: namespace,
                targetKind: targetKind,
                targetName: targetName,
                localPort: localPort,
                remotePort: remotePort,
                address: address
            )
        }

        let handle = try longRunningRunner.start(
            executable: executable,
            arguments: arguments,
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

        if let agent = resolvedK8sAgentPath() {
            do {
                try await RuneK8sAgentOperationsClient.applyFile(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    filePath: tempURL.path,
                    timeout: 120
                )
                return
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with kubectl fallback.
            }
        }

        _ = try await runKubectl(
            arguments: builder.applyFileArguments(context: context.name, namespace: namespace, filePath: tempURL.path),
            environment: env
        )
    }

    public func patchCronJobSuspend(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        name: String,
        suspend: Bool
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                try await RuneK8sAgentOperationsClient.patchCronJobSuspend(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    name: name,
                    suspend: suspend,
                    timeout: 90
                )
                return
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with REST fallback.
            }
        }
        if try await patchCronJobSuspendViaREST(
            environment: env,
            context: context,
            namespace: namespace,
            name: name,
            suspend: suspend
        ) {
            return
        }
        _ = try await runKubectl(
            arguments: builder.patchCronJobSuspendArguments(
                context: context.name,
                namespace: namespace,
                name: name,
                suspend: suspend
            ),
            environment: env
        )
    }

    public func createJobFromCronJob(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        cronJobName: String,
        jobName: String
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                try await RuneK8sAgentOperationsClient.createJobFromCronJob(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    cronJobName: cronJobName,
                    jobName: jobName,
                    timeout: 90
                )
                return
            } catch {
                try rethrowCancellationIfNeeded(error)
                // Continue with kubectl fallback.
            }
        }
        _ = try await runKubectl(
            arguments: builder.createJobFromCronJobArguments(
                context: context.name,
                namespace: namespace,
                cronJobName: cronJobName,
                jobName: jobName
            ),
            environment: env
        )
    }

    /// Runs kubectl; when `timeout` is omitted, uses a short process timeout first, then one longer attempt if the process hit a timeout (slow API / first connection). Explicit `timeout` skips that (e.g. logs, short metrics).
    private func runKubectl(
        arguments: [String],
        environment: [String: String],
        timeout: TimeInterval? = nil
    ) async throws -> CommandResult {
        let effectiveTimeout = timeout ?? commandTimeout
        let useQuickThenRetry = timeout == nil && effectiveTimeout > quickKubectlAttemptTimeout

        if useQuickThenRetry {
            do {
                return try await runKubectlOnce(
                    arguments: arguments,
                    environment: environment,
                    timeout: quickKubectlAttemptTimeout
                )
            } catch {
                guard isProcessTimeoutError(error) else { throw error }
                return try await runKubectlOnce(
                    arguments: arguments,
                    environment: environment,
                    timeout: max(effectiveTimeout, retryTimeoutAfterQuickFailure)
                )
            }
        }

        return try await runKubectlOnce(
            arguments: arguments,
            environment: environment,
            timeout: effectiveTimeout
        )
    }

    private func runKubectlOnce(
        arguments: [String],
        environment: [String: String],
        timeout: TimeInterval
    ) async throws -> CommandResult {
        let started = Date()
        let result = try await runner.run(
            executable: kubectlPath,
            arguments: ["kubectl"] + arguments,
            environment: environment,
            timeout: timeout
        )

        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(command: "kubectl \(arguments.joined(separator: " "))", message: result.stderr)
        }

        let ms = Int(Date().timeIntervalSince(started) * 1000)
        VerboseKubeTrace.append(
            "kubectl",
            "ok ms=\(ms) stdoutBytes=\(result.stdout.utf8.count) stderrBytes=\(result.stderr.utf8.count) kubeconfig=\(VerboseKubeTrace.kubeconfigSummary(environment))"
        )

        return result
    }

    private func isProcessTimeoutError(_ error: Error) -> Bool {
        guard case let RuneError.commandFailed(_, message) = error else { return false }
        return message.localizedCaseInsensitiveContains("timed out")
    }

    private func rethrowCancellationIfNeeded(_ error: Error) throws {
        if isCancellationLikeError(error) {
            throw CancellationError()
        }
    }

    private func isCancellationLikeError(_ error: Error) -> Bool {
        if error is CancellationError {
            return true
        }
        let description = String(describing: error).lowercased()
        return description.contains("cancellationerror")
            || description.contains("command cancelled")
            || description.contains("command canceled")
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

    private func contextsViaREST(environment: [String: String]) async throws -> [KubeContext]? {
        do {
            return try await restClient.listContexts(environment: environment)
        } catch {
            try rethrowCancellationIfNeeded(error)
            return nil
        }
    }

    private func namespacesViaREST(environment: [String: String], context: KubeContext) async throws -> [String]? {
        do {
            return try await restClient.listNamespaces(
                environment: environment,
                contextName: context.name,
                timeout: commandTimeout
            )
        } catch {
            try rethrowCancellationIfNeeded(error)
            return nil
        }
    }

    private func contextNamespaceViaREST(environment: [String: String], context: KubeContext) async throws -> String?? {
        do {
            return try await restClient.contextNamespace(
                environment: environment,
                contextName: context.name
            )
        } catch {
            try rethrowCancellationIfNeeded(error)
            return nil
        }
    }

    private func listViaREST<T>(
        environment: [String: String],
        context: KubeContext,
        resource: String,
        namespace: String?,
        timeout: TimeInterval,
        parse: (String) throws -> [T]
    ) async throws -> [T]? {
        do {
            let raw = try await restClient.collection(
                environment: environment,
                contextName: context.name,
                resource: resource,
                namespace: namespace,
                timeout: timeout
            )
            return try parse(raw)
        } catch {
            try rethrowCancellationIfNeeded(error)
            return nil
        }
    }

    private func resourceViaREST<T>(
        environment: [String: String],
        context: KubeContext,
        kind: KubeResourceKind,
        namespace: String,
        name: String,
        timeout: TimeInterval,
        parse: (String) throws -> T
    ) async throws -> T? {
        do {
            let raw = try await restClient.resourceJSON(
                environment: environment,
                contextName: context.name,
                kind: kind,
                namespace: namespace,
                name: name,
                timeout: timeout
            )
            return try parse(raw)
        } catch {
            try rethrowCancellationIfNeeded(error)
            return nil
        }
    }

    private func podLogsViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        podName: String,
        filter: LogTimeFilter,
        previous: Bool,
        timeout: TimeInterval,
        profile: LogQueryProfile = .pod
    ) async throws -> String? {
        do {
            return try await restClient.podLogs(
                environment: environment,
                contextName: context.name,
                namespace: namespace,
                podName: podName,
                filter: filter,
                previous: previous,
                timeout: timeout,
                profile: profile
            )
        } catch {
            try rethrowCancellationIfNeeded(error)
            if previous, isMissingPreviousLogsError(error) {
                return "No previous logs available for \(podName)."
            }
            return nil
        }
    }

    private func serviceSelectorViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        serviceName: String
    ) async throws -> [String: String]? {
        do {
            let raw = try await restClient.serviceSelector(
                environment: environment,
                contextName: context.name,
                namespace: namespace,
                serviceName: serviceName,
                timeout: unifiedLogsSelectorTimeout
            )
            return try parser.parseServiceSelector(from: raw)
        } catch {
            try rethrowCancellationIfNeeded(error)
            return nil
        }
    }

    private func deploymentSelectorViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        deploymentName: String
    ) async throws -> [String: String]? {
        do {
            let raw = try await restClient.deploymentSelector(
                environment: environment,
                contextName: context.name,
                namespace: namespace,
                deploymentName: deploymentName,
                timeout: unifiedLogsSelectorTimeout
            )
            return try parser.parseDeploymentSelector(from: raw)
        } catch {
            try rethrowCancellationIfNeeded(error)
            return nil
        }
    }

    private func podsBySelectorViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        selector: String
    ) async throws -> [PodSummary]? {
        do {
            let raw = try await restClient.podsBySelector(
                environment: environment,
                contextName: context.name,
                namespace: namespace,
                selector: selector,
                timeout: unifiedLogsSelectorTimeout
            )
            let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            return try parser.parsePodsListJSON(namespace: namespace, from: trimmed)
        } catch {
            try rethrowCancellationIfNeeded(error)
            return nil
        }
    }

    private func resourceYAMLViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws -> String? {
        do {
            return try await restClient.resourceYAML(
                environment: environment,
                contextName: context.name,
                kind: kind,
                namespace: namespace,
                name: name,
                timeout: commandTimeout
            )
        } catch {
            try rethrowCancellationIfNeeded(error)
            return nil
        }
    }

    private func deleteResourceViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws -> Bool {
        do {
            try await restClient.deleteResource(
                environment: environment,
                contextName: context.name,
                namespace: namespace,
                kind: kind,
                name: name,
                timeout: 90
            )
            return true
        } catch {
            try rethrowCancellationIfNeeded(error)
            return false
        }
    }

    private func scaleDeploymentViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        deploymentName: String,
        replicas: Int
    ) async throws -> Bool {
        do {
            try await restClient.scaleDeployment(
                environment: environment,
                contextName: context.name,
                namespace: namespace,
                deploymentName: deploymentName,
                replicas: replicas,
                timeout: 90
            )
            return true
        } catch {
            try rethrowCancellationIfNeeded(error)
            return false
        }
    }

    private func restartDeploymentRolloutViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        deploymentName: String
    ) async throws -> Bool {
        do {
            try await restClient.restartDeploymentRollout(
                environment: environment,
                contextName: context.name,
                namespace: namespace,
                deploymentName: deploymentName,
                timeout: 90
            )
            return true
        } catch {
            try rethrowCancellationIfNeeded(error)
            return false
        }
    }

    private func patchCronJobSuspendViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        name: String,
        suspend: Bool
    ) async throws -> Bool {
        do {
            try await restClient.patchCronJobSuspend(
                environment: environment,
                contextName: context.name,
                namespace: namespace,
                name: name,
                suspend: suspend,
                timeout: 90
            )
            return true
        } catch {
            try rethrowCancellationIfNeeded(error)
            return false
        }
    }

    private func resolvedK8sAgentPath() -> String? {
        if let k8sAgentPath, !k8sAgentPath.isEmpty {
            return FileManager.default.isExecutableFile(atPath: k8sAgentPath) ? k8sAgentPath : nil
        }
        return RuneK8sAgentLocator.resolvedExecutablePath()
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

    /// Process-side ceiling for `kubectl logs` (second phase). Kept modest so the UI is not blocked for minutes.
    private func logFetchTimeout(for filter: LogTimeFilter) -> TimeInterval {
        switch filter {
        case .all:
            return 45
        case let .tailLines(lines) where lines >= 10_000:
            return 60
        case .lastDays(let days) where days >= 7:
            return 60
        case .lastHours, .lastDays:
            return 50
        default:
            return 40
        }
    }

    private func runKubectlAllowFailure(
        arguments: [String],
        environment: [String: String],
        timeout: TimeInterval? = nil
    ) async -> CommandResult? {
        try? await runKubectl(arguments: arguments, environment: environment, timeout: timeout)
    }

    private static func mergePodSummariesPreservingMetrics(base: [PodSummary], detail: [PodSummary]) -> [PodSummary] {
        let detailById = Dictionary(uniqueKeysWithValues: detail.map { ($0.id, $0) })
        return base.map { pod in
            guard let d = detailById[pod.id] else { return pod }
            return mergePodSummaryPreservingMetrics(base: pod, detail: d)
        }
    }

    private static func mergePodSummaryPreservingMetrics(base: PodSummary, detail: PodSummary) -> PodSummary {
        PodSummary(
            name: base.name,
            namespace: base.namespace,
            status: base.status,
            totalRestarts: base.totalRestarts,
            ageDescription: base.ageDescription,
            cpuUsage: base.cpuUsage,
            memoryUsage: base.memoryUsage,
            podIP: detail.podIP ?? base.podIP,
            hostIP: detail.hostIP ?? base.hostIP,
            nodeName: detail.nodeName ?? base.nodeName,
            qosClass: detail.qosClass ?? base.qosClass,
            containersReady: detail.containersReady ?? base.containersReady,
            containerNamesLine: detail.containerNamesLine ?? base.containerNamesLine
        )
    }

    private func mergePodNameMetrics(
        _ pods: [PodSummary],
        _ metrics: [String: (cpu: String, memory: String)]
    ) -> [PodSummary] {
        pods.map { pod in
            guard let m = metrics[pod.name] else { return pod }
            return PodSummary(
                name: pod.name,
                namespace: pod.namespace,
                status: pod.status,
                totalRestarts: pod.totalRestarts,
                ageDescription: pod.ageDescription,
                cpuUsage: m.cpu,
                memoryUsage: m.memory,
                podIP: pod.podIP,
                hostIP: pod.hostIP,
                nodeName: pod.nodeName,
                qosClass: pod.qosClass,
                containersReady: pod.containersReady,
                containerNamesLine: pod.containerNamesLine
            )
        }
    }

    private func mergePodNamespacedMetrics(
        _ pods: [PodSummary],
        _ metrics: [String: (cpu: String, memory: String)]
    ) -> [PodSummary] {
        pods.map { pod in
            let key = "\(pod.namespace)/\(pod.name)"
            guard let m = metrics[key] else { return pod }
            return PodSummary(
                name: pod.name,
                namespace: pod.namespace,
                status: pod.status,
                totalRestarts: pod.totalRestarts,
                ageDescription: pod.ageDescription,
                cpuUsage: m.cpu,
                memoryUsage: m.memory,
                podIP: pod.podIP,
                hostIP: pod.hostIP,
                nodeName: pod.nodeName,
                qosClass: pod.qosClass,
                containersReady: pod.containersReady,
                containerNamesLine: pod.containerNamesLine
            )
        }
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
