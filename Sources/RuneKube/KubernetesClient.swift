import Compression
import Foundation
import RuneCore
import RuneDiagnostics
import RuneSecurity

public final class KubernetesClient: ContextListingService, NamespaceListingService, PodListingService, DeploymentListingService, ServiceListingService, EventListingService, GenericResourceListingService, PodLogService, UnifiedServiceLogService, UnifiedDeploymentLogService, ManifestService, ManifestValidationService, ResourceWriteService, HelmReleaseService, @unchecked Sendable {
    private let parser: KubernetesOutputParser
    private let restClient: KubernetesRESTClient
    private let commandTimeout: TimeInterval
    private let access: SecurityScopedAccess
    private let portForwardRegistry = PortForwardRegistry()
    private let terminalSessionRegistry = TerminalSessionRegistry()

    /// Explicit ceiling for slow namespaced Kubernetes JSON lists on large / high-latency clusters.
    private let slowNamespacedJSONListTimeout: TimeInterval = 120
    /// Page size for raw list count pagination.
    private let pagedCountLimit: Int = 250
    /// Hard stop for paged counts to avoid infinite loops on broken continue tokens.
    private let pagedCountMaxPages: Int = 500
    /// Keep pod metrics merge opportunistic so `Workloads > Pods` does not stall on metrics hiccups.
    private let opportunisticPodTopTimeout: TimeInterval = 2.0
    /// Unified logs should stay responsive in large namespaces with many historical pods.
    private let unifiedLogsMaxPods: Int = 8
    private let unifiedLogsMaxConcurrentPodFetches: Int = 3
    /// Keep per-pod log fetch short so one slow pod does not block the whole merged view.
    private let unifiedLogsPerPodTimeout: TimeInterval = 8
    /// One merged backend call can use a slightly larger budget than the per-pod fallback.
    private let unifiedLogsAggregateTimeout: TimeInterval = 20
    /// Selector/pod-discovery for unified logs should fail fast; stale workloads should not block the inspector for minutes.
    private let unifiedLogsSelectorTimeout: TimeInterval = 12
    /// Validation should feel near-live while still giving the API server room on slower clusters.
    private let manifestValidationTimeout: TimeInterval = 20

    public init(
        parser: KubernetesOutputParser = KubernetesOutputParser(),
        commandTimeout: TimeInterval = 30,
        access: SecurityScopedAccess = SecurityScopedAccess()
    ) {
        self.parser = parser
        self.restClient = KubernetesRESTClient()
        self.commandTimeout = commandTimeout
        self.access = access
    }

    public func listContexts(from sources: [KubeConfigSource]) async throws -> [KubeContext] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.listContexts(environment: env)
    }

    public func listNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [String] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.listNamespaces(
            environment: env,
            contextName: context.name,
            timeout: commandTimeout
        )
    }

    public func contextNamespace(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> String? {
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.contextNamespace(environment: env, contextName: context.name)
    }

    public func listPods(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [PodSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let raw = try await restClient.collection(
            environment: env,
            contextName: context.name,
            resource: "pods",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout
        )
        let pods = try parser.parsePodsListJSON(namespace: namespace, from: raw.trimmingCharacters(in: .whitespacesAndNewlines))
        let metrics = (try? await podMetricsByNameViaREST(environment: env, context: context, namespace: namespace)) ?? [:]
        return mergePodNameMetrics(pods, metrics)
    }

    /// Full JSON list merged into `base` by pod id — keeps status/restarts/age/CPU/mem from `base`, fills IP/node/QoS/ready from JSON.
    public func enrichPodsWithJSONList(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        merging base: [PodSummary]
    ) async throws -> [PodSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let raw = try await restClient.collection(
            environment: env,
            contextName: context.name,
            resource: "pods",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout
        )
        let detailed = try parser.parsePodsListJSON(namespace: namespace, from: raw.trimmingCharacters(in: .whitespacesAndNewlines))
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
        let raw = try await restClient.resourceJSON(
            environment: env,
            contextName: context.name,
            kind: .pod,
            namespace: namespace,
            name: podName,
            timeout: slowNamespacedJSONListTimeout
        )
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        return try parser.parseSinglePodJSON(namespace: namespace, from: trimmed)
    }

    public func listPodStatuses(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [PodSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let raw = try await restClient.collection(
            environment: env,
            contextName: context.name,
            resource: "pods",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout
        )
        let pods = try parser.parsePodsListJSON(namespace: namespace, from: raw.trimmingCharacters(in: .whitespacesAndNewlines))
        return pods.map { pod in
            PodSummary(
                name: pod.name,
                namespace: pod.namespace,
                status: pod.status,
                totalRestarts: pod.totalRestarts,
                ageDescription: pod.ageDescription
            )
        }
    }

    public func listPodsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [PodSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let raw = try await restClient.collection(
            environment: env,
            contextName: context.name,
            resource: "pods",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout
        )
        let pods = try parser.parsePodsListJSONAllNamespaces(from: raw.trimmingCharacters(in: .whitespacesAndNewlines))
        let metrics = (try? await podMetricsByNamespaceAndNameViaREST(environment: env, context: context)) ?? [:]
        return mergePodNamespacedMetrics(pods, metrics)
    }

    public func listDeployments(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [DeploymentSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "deployments",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseDeployments(namespace: namespace, from: raw) }
        )
    }

    public func listDeploymentsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [DeploymentSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "deployments",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseDeployments(namespace: "", from: raw) }
        )
    }

    public func listServices(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ServiceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "services",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseServices(namespace: namespace, from: raw) }
        )
    }

    public func listServicesAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ServiceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "services",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseServices(namespace: "", from: raw) }
        )
    }

    public func listStatefulSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "statefulsets",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseStatefulSets(namespace: namespace, from: raw) }
        )
    }

    public func listDaemonSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "daemonsets",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseDaemonSets(namespace: namespace, from: raw) }
        )
    }

    public func listJobs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "jobs",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseJobs(namespace: namespace, from: raw) }
        )
    }

    public func listCronJobs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "cronjobs",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseCronJobs(namespace: namespace, from: raw) }
        )
    }

    public func listReplicaSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "replicasets",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseReplicaSets(namespace: namespace, from: raw) }
        )
    }

    public func listPersistentVolumeClaims(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "persistentvolumeclaims",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parsePersistentVolumeClaims(namespace: namespace, from: raw) }
        )
    }

    public func listPersistentVolumes(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "persistentvolumes",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parsePersistentVolumes(from: raw) }
        )
    }

    public func listStorageClasses(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "storageclasses",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseStorageClasses(from: raw) }
        )
    }

    public func listHorizontalPodAutoscalers(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "horizontalpodautoscalers",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseHorizontalPodAutoscalers(namespace: namespace, from: raw) }
        )
    }

    public func listNetworkPolicies(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "networkpolicies",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseNetworkPolicies(namespace: namespace, from: raw) }
        )
    }

    public func listIngresses(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "ingresses",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseIngresses(namespace: namespace, from: raw) }
        )
    }

    public func listIngressesAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "ingresses",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseIngresses(namespace: "", from: raw) }
        )
    }

    public func listConfigMaps(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "configmaps",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseConfigMaps(namespace: namespace, from: raw) }
        )
    }

    public func listConfigMapsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "configmaps",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseConfigMaps(namespace: "", from: raw) }
        )
    }

    public func listSecrets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "secrets",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseSecrets(namespace: namespace, from: raw) }
        )
    }

    public func listNodes(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "nodes",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseNodes(from: raw) }
        )
    }

    public func listEvents(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [EventSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "events",
            namespace: namespace,
            timeout: 20,
            parse: { raw in try self.parser.parseEvents(from: raw) }
        )
    }

    public func listEventsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [EventSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "events",
            namespace: nil,
            timeout: 20,
            parse: { raw in try self.parser.parseEvents(from: raw) }
        )
    }

    public func listRoles(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "roles",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseRoles(namespace: namespace, from: raw) }
        )
    }

    public func listRoleBindings(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "rolebindings",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseRoleBindings(namespace: namespace, from: raw) }
        )
    }

    public func listClusterRoles(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "clusterroles",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseClusterRoles(from: raw) }
        )
    }

    public func listClusterRoleBindings(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "clusterrolebindings",
            namespace: nil,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseClusterRoleBindings(from: raw) }
        )
    }

    public func countNamespacedResources(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        resource: String,
        progress: ((Int) -> Void)? = nil
    ) async throws -> Int {
        let env = try kubeconfigEnvironment(from: sources)
        if let apiPath = KubernetesRESTPath.namespacedCollectionMetadataProbe(namespace: namespace, resource: resource),
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

        throw RuneError.commandFailed(
            command: "kubernetes REST count \(resource)",
            message: "Resource count is not available from the Kubernetes API response."
        )
    }

    public func countClusterResources(
        from sources: [KubeConfigSource],
        context: KubeContext,
        resource: String,
        progress: ((Int) -> Void)? = nil
    ) async throws -> Int {
        let env = try kubeconfigEnvironment(from: sources)
        if let apiPath = KubernetesRESTPath.clusterCollectionMetadataProbe(resource: resource),
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

        throw RuneError.commandFailed(
            command: "kubernetes REST count \(resource)",
            message: "Resource count is not available from the Kubernetes API response."
        )
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

            guard let page = KubernetesListJSON.collectionPageInfo(from: raw) else {
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

    /// Cheap total via native REST `limit=1` list (`metadata.remainingItemCount` + 1). Returns `nil` on failure or when the server omits a derivable total.
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
            return KubernetesListJSON.collectionListTotal(from: raw)
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
        return (try? await clusterUsagePercentViaREST(environment: env, context: context)) ?? (nil, nil)
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
        return ""
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
        if let cachedSelector = service.selector, !cachedSelector.isEmpty {
            selectorMap = cachedSelector
        } else {
            selectorMap = try await requiredServiceSelectorViaREST(
                environment: env,
                context: context,
                namespace: namespace,
                serviceName: service.name
            )
        }

        guard !selectorMap.isEmpty else {
            throw RuneError.parseError(message: "Service \(service.name) is missing a selector and cannot be used for unified logs")
        }

        let selector = selectorMap
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: ",")

        let pods = try await requiredPodsBySelectorViaREST(
            environment: env,
            context: context,
            namespace: namespace,
            selector: selector
        )
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
        if let cachedSelector = deployment.selector, !cachedSelector.isEmpty {
            selectorMap = cachedSelector
        } else {
            selectorMap = try await requiredDeploymentSelectorViaREST(
                environment: env,
                context: context,
                namespace: namespace,
                deploymentName: deployment.name
            )
        }

        guard !selectorMap.isEmpty else {
            throw RuneError.parseError(message: "Deployment \(deployment.name) is missing a matchLabels selector and cannot be used for unified logs")
        }

        let selector = selectorMap
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: ",")

        let pods = try await requiredPodsBySelectorViaREST(
            environment: env,
            context: context,
            namespace: namespace,
            selector: selector
        )
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
        return try await restClient.resourceYAML(
            environment: env,
            contextName: context.name,
            kind: kind,
            namespace: namespace,
            name: name,
            timeout: commandTimeout
        )
    }

    public func resourceDescribe(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.resourceDescribe(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            kind: kind,
            name: name,
            timeout: commandTimeout
        )
    }

    public func listReleases(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String?,
        allNamespaces: Bool
    ) async throws -> [HelmReleaseSummary] {
        let releases = try await helmReleases(
            from: sources,
            context: context,
            namespace: allNamespaces ? nil : namespace
        )
        let latest = Dictionary(grouping: releases, by: { "\($0.namespace)/\($0.name)" })
            .compactMap { _, revisions in revisions.max { $0.revision < $1.revision } }
        return latest
            .map(\.summary)
            .sorted {
                let ns = $0.namespace.localizedCaseInsensitiveCompare($1.namespace)
                if ns != .orderedSame { return ns == .orderedAscending }
                return $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
            }
    }

    public func releaseValues(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> String {
        let release = try await latestHelmRelease(
            from: sources,
            context: context,
            namespace: namespace,
            releaseName: releaseName
        )
        return HelmValueYAMLRenderer.render(release.config)
    }

    public func releaseManifest(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> String {
        try await latestHelmRelease(
            from: sources,
            context: context,
            namespace: namespace,
            releaseName: releaseName
        ).manifest
    }

    public func releaseHistory(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> [HelmReleaseRevision] {
        try await helmReleases(from: sources, context: context, namespace: namespace)
            .filter { $0.name == releaseName }
            .sorted { $0.revision > $1.revision }
            .map(\.revisionSummary)
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
        return try await restClient.execInPod(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            podName: podName,
            container: container,
            command: command,
            timeout: 90
        )
    }

    public func startPodTerminalSession(
        id sessionID: String,
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        container: String?,
        shellCommand: [String],
        onOutput: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) -> Void
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        let handle = try await restClient.startPodTerminalSession(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            podName: podName,
            container: container,
            shellCommand: shellCommand,
            onOutput: onOutput,
            onTermination: onTermination
        )
        await terminalSessionRegistry.insert(handle: handle, id: sessionID)
    }

    public func writeToPodTerminalSession(id: String, text: String) async throws {
        guard let handle = await terminalSessionRegistry.handle(id: id) else {
            throw RuneError.commandFailed(command: "terminal session", message: "No active terminal session")
        }
        try handle.writeToStdin(Data(text.utf8))
    }

    public func stopPodTerminalSession(id: String) async {
        let handle = await terminalSessionRegistry.remove(id: id)
        handle?.terminate()
    }

    public func deleteResource(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        try await restClient.deleteResource(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            kind: kind,
            name: name,
            timeout: 90
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
        try await restClient.scaleDeployment(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            deploymentName: deploymentName,
            replicas: replicas,
            timeout: 90
        )
    }

    public func restartDeploymentRollout(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        try await restClient.restartDeploymentRollout(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            deploymentName: deploymentName,
            timeout: 90
        )
    }

    public func deploymentRolloutHistory(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.deploymentRolloutHistory(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            deploymentName: deploymentName,
            timeout: 90
        )
    }

    public func rollbackDeploymentRollout(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String,
        revision: Int?
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        try await restClient.rollbackDeploymentRollout(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            deploymentName: deploymentName,
            revision: revision,
            timeout: 90
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

        onEvent(baseSession)

        let podName: String
        switch targetKind {
        case .pod:
            podName = targetName
        case .service:
            let selectorMap = try await requiredServiceSelectorViaREST(
                environment: env,
                context: context,
                namespace: namespace,
                serviceName: targetName
            )
            guard !selectorMap.isEmpty else {
                throw RuneError.parseError(message: "Service \(targetName) is missing a selector and cannot be port-forwarded.")
            }

            let selector = selectorMap
                .sorted { $0.key < $1.key }
                .map { "\($0.key)=\($0.value)" }
                .joined(separator: ",")
            let pods = try await requiredPodsBySelectorViaREST(
                environment: env,
                context: context,
                namespace: namespace,
                selector: selector
            )
            guard let selectedPod = Self.preferredPortForwardPod(from: pods) else {
                throw RuneError.parseError(message: "No pods matched service \(targetName) selector \(selector).")
            }
            podName = selectedPod.name
        }

        let handle = try await restClient.startPodPortForward(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            podName: podName,
            localPort: localPort,
            remotePort: remotePort,
            address: address,
            onReady: {
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
                        lastMessage: "Forwarding \(address):\(localPort) to \(podName):\(remotePort)"
                    )
                )
            },
            onFailure: { message in
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
                        status: .failed,
                        lastMessage: message
                    )
                )
            }
        )
        await portForwardRegistry.insert(handle: handle, id: sessionID)
        return PortForwardSession(
            id: sessionID,
            contextName: context.name,
            namespace: namespace,
            targetKind: targetKind,
            targetName: targetName,
            localPort: localPort,
            remotePort: remotePort,
            address: address,
            status: .starting,
            lastMessage: "Starting port-forward to \(podName):\(remotePort)"
        )
    }

    static func preferredPortForwardPod(from pods: [PodSummary]) -> PodSummary? {
        pods
            .sorted { lhs, rhs in
                let lhsRunning = lhs.status.localizedCaseInsensitiveCompare("Running") == .orderedSame
                let rhsRunning = rhs.status.localizedCaseInsensitiveCompare("Running") == .orderedSame
                if lhsRunning != rhsRunning { return lhsRunning && !rhsRunning }
                return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
            }
            .first
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
        try await restClient.applyYAML(
            environment: env,
            contextName: context.name,
            defaultNamespace: namespace,
            yaml: yaml,
            dryRun: false,
            timeout: 120
        )
    }

    public func validateResourceYAML(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        yaml: String
    ) async throws -> [YAMLValidationIssue] {
        let trimmed = yaml.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return [] }

        let env = try kubeconfigEnvironment(from: sources)
        do {
            try await restClient.applyYAML(
                environment: env,
                contextName: context.name,
                defaultNamespace: namespace,
                yaml: yaml,
                dryRun: true,
                timeout: manifestValidationTimeout
            )
            return []
        } catch {
            try rethrowCancellationIfNeeded(error)
            if let issues = Self.validationIssues(from: error, yaml: yaml), !issues.isEmpty {
                return issues
            }
            return [
                YAMLValidationIssue(
                    source: .kubernetes,
                    severity: .error,
                    message: Self.normalizeValidationMessage(String(describing: error))
                )
            ]
        }
    }

    public func patchCronJobSuspend(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        name: String,
        suspend: Bool
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        try await restClient.patchCronJobSuspend(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            name: name,
            suspend: suspend,
            timeout: 90
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
        try await restClient.createJobFromCronJob(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            cronJobName: cronJobName,
            jobName: jobName,
            timeout: 90
        )
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

    private func listViaREST<T>(
        environment: [String: String],
        context: KubeContext,
        resource: String,
        namespace: String?,
        timeout: TimeInterval,
        parse: (String) throws -> [T]
    ) async throws -> [T] {
        let raw = try await restClient.collection(
            environment: environment,
            contextName: context.name,
            resource: resource,
            namespace: namespace,
            timeout: timeout
        )
        return try parse(raw)
    }

    private func helmReleases(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String?
    ) async throws -> [DecodedHelmRelease] {
        let env = try kubeconfigEnvironment(from: sources)
        var objects: [HelmStorageObject] = []
        var lastError: Error?

        for resource in ["secrets", "configmaps"] {
            do {
                let raw = try await restClient.collection(
                    environment: env,
                    contextName: context.name,
                    resource: resource,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout,
                    options: KubernetesListOptions(labelSelector: "owner=helm")
                )
                objects.append(contentsOf: try HelmStorageObject.parseList(raw, storageResource: resource))
            } catch {
                lastError = error
            }
        }

        let decoded = objects.compactMap { object in
            try? object.decodeRelease()
        }
        if decoded.isEmpty, let lastError, objects.isEmpty {
            throw lastError
        }
        return decoded
    }

    private func latestHelmRelease(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> DecodedHelmRelease {
        let matches = try await helmReleases(from: sources, context: context, namespace: namespace)
            .filter { $0.name == releaseName }
        guard let latest = matches.max(by: { $0.revision < $1.revision }) else {
            throw RuneError.invalidInput(message: "Helm release \(releaseName) was not found in namespace \(namespace).")
        }
        return latest
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
            if previous, isMissingPreviousLogsError(error) {
                return "No previous logs available for \(podName)."
            }
            throw error
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

    private func requiredServiceSelectorViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        serviceName: String
    ) async throws -> [String: String] {
        let raw = try await restClient.serviceSelector(
            environment: environment,
            contextName: context.name,
            namespace: namespace,
            serviceName: serviceName,
            timeout: unifiedLogsSelectorTimeout
        )
        do {
            return try parser.parseServiceSelector(from: raw)
        } catch {
            throw RuneError.parseError(message: "service selector could not be parsed")
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

    private func requiredDeploymentSelectorViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        deploymentName: String
    ) async throws -> [String: String] {
        let raw = try await restClient.deploymentSelector(
            environment: environment,
            contextName: context.name,
            namespace: namespace,
            deploymentName: deploymentName,
            timeout: unifiedLogsSelectorTimeout
        )
        do {
            return try parser.parseDeploymentSelector(from: raw)
        } catch {
            throw RuneError.parseError(message: "deployment selector could not be parsed")
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

    private func requiredPodsBySelectorViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String,
        selector: String
    ) async throws -> [PodSummary] {
        let raw = try await restClient.podsBySelector(
            environment: environment,
            contextName: context.name,
            namespace: namespace,
            selector: selector,
            timeout: unifiedLogsSelectorTimeout
        )
        return try parser.parsePodsListJSON(
            namespace: namespace,
            from: raw.trimmingCharacters(in: .whitespacesAndNewlines)
        )
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

    private func isMissingPreviousLogsError(_ error: Error) -> Bool {
        let text = String(describing: error).lowercased()
        return text.contains("previous terminated container")
            || text.contains("no previous terminated container")
            || text.contains("previous container not found")
    }

    /// Network-side ceiling for native log fetches. Kept modest so the UI is not blocked for minutes.
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

    static func parseValidationIssues(from output: String, yaml: String) -> [YAMLValidationIssue] {
        let trimmed = output.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return [] }

        if isLikelyTransportValidationOutput(trimmed) {
            return [
                YAMLValidationIssue(
                    source: .transport,
                    severity: .warning,
                    message: normalizeValidationMessage(trimmed)
                )
            ]
        }

        var issues = parseLineScopedValidationIssues(from: trimmed, yaml: yaml)
        issues.append(contentsOf: parseKubernetesValidationIssues(from: trimmed))

        if !issues.isEmpty {
            return deduplicatedValidationIssues(issues)
        }

        if trimmed.contains("is invalid:") || trimmed.contains("error validating data") {
            return [
                YAMLValidationIssue(
                    source: .kubernetes,
                    severity: .error,
                    message: normalizeValidationMessage(trimmed)
                )
            ]
        }

        return []
    }

    private static func parseLineScopedValidationIssues(from output: String, yaml: String) -> [YAMLValidationIssue] {
        let pattern = #"yaml:\s*line\s+(\d+)(?:,\s*column\s+(\d+))?:\s*([^\n]+)"#
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return [] }
        let nsOutput = output as NSString
        let matches = regex.matches(in: output, range: NSRange(location: 0, length: nsOutput.length))

        return matches.compactMap { match in
            guard match.numberOfRanges >= 4 else { return nil }
            guard let lineValue = Int(nsOutput.substring(with: match.range(at: 1))) else { return nil }

            let columnValue: Int? = {
                let range = match.range(at: 2)
                guard range.location != NSNotFound else { return nil }
                return Int(nsOutput.substring(with: range))
            }()

            let message = nsOutput.substring(with: match.range(at: 3)).trimmingCharacters(in: .whitespacesAndNewlines)
            let range = validationRange(in: yaml, line: lineValue)
            return YAMLValidationIssue(
                source: .syntax,
                severity: .error,
                message: message,
                line: lineValue,
                column: columnValue,
                range: range
            )
        }
    }

    private static func parseKubernetesValidationIssues(from output: String) -> [YAMLValidationIssue] {
        let pattern = #"ValidationError\(([^)]+)\):\s*([^;\n\]]+)"#
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return [] }
        let nsOutput = output as NSString
        let matches = regex.matches(in: output, range: NSRange(location: 0, length: nsOutput.length))

        if !matches.isEmpty {
            return matches.map { match in
                let path = nsOutput.substring(with: match.range(at: 1)).trimmingCharacters(in: .whitespacesAndNewlines)
                let detail = nsOutput.substring(with: match.range(at: 2)).trimmingCharacters(in: .whitespacesAndNewlines)
                return YAMLValidationIssue(
                    source: .kubernetes,
                    severity: .error,
                    message: "\(path): \(detail)"
                )
            }
        }

        if let invalidRange = output.range(of: "is invalid:") {
            let message = String(output[invalidRange.upperBound...]).trimmingCharacters(in: .whitespacesAndNewlines)
            guard !message.isEmpty else { return [] }
            return [
                YAMLValidationIssue(
                    source: .kubernetes,
                    severity: .error,
                    message: message
                )
            ]
        }

        return []
    }

    private static func validationRange(in yaml: String, line: Int) -> YAMLValidationRange? {
        guard line > 0 else { return nil }
        let nsYAML = yaml as NSString
        var currentLine = 1
        var location = 0

        while location <= nsYAML.length {
            let lineRange = nsYAML.lineRange(for: NSRange(location: location, length: 0))
            if currentLine == line {
                return YAMLValidationRange(location: lineRange.location, length: max(1, lineRange.length))
            }

            let nextLocation = NSMaxRange(lineRange)
            guard nextLocation > location else { break }
            currentLine += 1
            location = nextLocation
            if location == nsYAML.length {
                break
            }
        }

        return nil
    }

    private static func normalizeValidationMessage(_ output: String) -> String {
        var message = output.trimmingCharacters(in: .whitespacesAndNewlines)

        if let range = message.range(of: #"error parsing .*?: error converting YAML to JSON: "#, options: .regularExpression) {
            message.removeSubrange(range)
        }

        if let range = message.range(of: #"error: error validating \".*?\": error validating data: "#, options: .regularExpression) {
            message.removeSubrange(range)
        }

        message = message.replacingOccurrences(
            of: #"; if you choose to ignore these errors, turn validation off with --validate=false"#,
            with: "",
            options: .regularExpression
        )

        return message.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func transportValidationIssue(from error: Error) -> YAMLValidationIssue {
        let message: String
        if case let RuneError.commandFailed(_, detail) = error {
            message = detail.trimmingCharacters(in: .whitespacesAndNewlines)
        } else {
            message = error.localizedDescription
        }

        return YAMLValidationIssue(
            source: .transport,
            severity: .warning,
            message: normalizeValidationMessage(message)
        )
    }

    private static func validationIssues(from error: Error, yaml: String) -> [YAMLValidationIssue]? {
        guard case let RuneError.commandFailed(_, detail) = error else { return nil }
        let output = detail.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !output.isEmpty else { return nil }
        let parsed = parseValidationIssues(from: output, yaml: yaml)
        if !parsed.isEmpty {
            return parsed
        }
        return [
            YAMLValidationIssue(
                source: .kubernetes,
                severity: .error,
                message: normalizeValidationMessage(output)
            )
        ]
    }

    private static func isLikelyTransportValidationOutput(_ output: String) -> Bool {
        let lowered = output.lowercased()
        return lowered.contains("unable to connect to the server")
            || lowered.contains("timed out")
            || lowered.contains("connection refused")
            || lowered.contains("i/o timeout")
            || lowered.contains("tls handshake timeout")
            || lowered.contains("no configuration has been provided")
            || lowered.contains("the connection to the server")
    }

    private static func deduplicatedValidationIssues(_ issues: [YAMLValidationIssue]) -> [YAMLValidationIssue] {
        var seen: Set<String> = []
        return issues.filter { issue in
            seen.insert(issue.id).inserted
        }
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

    private func podMetricsByNameViaREST(
        environment: [String: String],
        context: KubeContext,
        namespace: String
    ) async throws -> [String: (cpu: String, memory: String)] {
        let raw = try await restClient.rawGET(
            environment: environment,
            contextName: context.name,
            apiPath: "/apis/metrics.k8s.io/v1beta1/namespaces/\(Self.percentEncodePathComponent(namespace))/pods",
            timeout: opportunisticPodTopTimeout
        )
        return try Self.parsePodMetrics(raw, namespaced: false)
    }

    private func podMetricsByNamespaceAndNameViaREST(
        environment: [String: String],
        context: KubeContext
    ) async throws -> [String: (cpu: String, memory: String)] {
        let raw = try await restClient.rawGET(
            environment: environment,
            contextName: context.name,
            apiPath: "/apis/metrics.k8s.io/v1beta1/pods",
            timeout: opportunisticPodTopTimeout
        )
        return try Self.parsePodMetrics(raw, namespaced: true)
    }

    private func clusterUsagePercentViaREST(
        environment: [String: String],
        context: KubeContext
    ) async throws -> (cpuPercent: Int?, memoryPercent: Int?) {
        async let metricsRaw = restClient.rawGET(
            environment: environment,
            contextName: context.name,
            apiPath: "/apis/metrics.k8s.io/v1beta1/nodes",
            timeout: 3
        )
        async let nodesRaw = restClient.collection(
            environment: environment,
            contextName: context.name,
            resource: "nodes",
            namespace: nil,
            timeout: 3
        )

        let (usage, capacity) = try await (Self.parseNodeMetrics(metricsRaw), Self.parseNodeCapacity(nodesRaw))
        guard capacity.cpuMilli > 0 || capacity.memoryBytes > 0 else {
            return (nil, nil)
        }
        let cpu = capacity.cpuMilli > 0 ? Int((Double(usage.cpuMilli) / Double(capacity.cpuMilli) * 100).rounded()) : nil
        let memory = capacity.memoryBytes > 0 ? Int((usage.memoryBytes / capacity.memoryBytes * 100).rounded()) : nil
        return (cpu, memory)
    }

    private static func parsePodMetrics(_ raw: String, namespaced: Bool) throws -> [String: (cpu: String, memory: String)] {
        guard
            let root = try JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any],
            let items = root["items"] as? [[String: Any]]
        else {
            return [:]
        }

        var result: [String: (cpu: String, memory: String)] = [:]
        for item in items {
            guard
                let metadata = item["metadata"] as? [String: Any],
                let name = metadata["name"] as? String
            else {
                continue
            }
            let namespace = metadata["namespace"] as? String ?? ""
            let containers = item["containers"] as? [[String: Any]] ?? []
            var cpuMilli = 0
            var memoryBytes = 0.0
            for container in containers {
                guard let usage = container["usage"] as? [String: Any] else { continue }
                cpuMilli += parseCPUMilli(usage["cpu"] as? String)
                memoryBytes += parseMemoryBytes(usage["memory"] as? String)
            }
            let key = namespaced ? "\(namespace)/\(name)" : name
            result[key] = (formatCPUMilli(cpuMilli), formatMemoryBytes(memoryBytes))
        }
        return result
    }

    private static func parseNodeMetrics(_ raw: String) throws -> (cpuMilli: Int, memoryBytes: Double) {
        guard
            let root = try JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any],
            let items = root["items"] as? [[String: Any]]
        else {
            return (0, 0)
        }
        var cpu = 0
        var memory = 0.0
        for item in items {
            guard let usage = item["usage"] as? [String: Any] else { continue }
            cpu += parseCPUMilli(usage["cpu"] as? String)
            memory += parseMemoryBytes(usage["memory"] as? String)
        }
        return (cpu, memory)
    }

    private static func parseNodeCapacity(_ raw: String) throws -> (cpuMilli: Int, memoryBytes: Double) {
        guard
            let root = try JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any],
            let items = root["items"] as? [[String: Any]]
        else {
            return (0, 0)
        }
        var cpu = 0
        var memory = 0.0
        for item in items {
            guard
                let status = item["status"] as? [String: Any],
                let capacity = status["capacity"] as? [String: Any]
            else {
                continue
            }
            cpu += parseCPUMilli(capacity["cpu"] as? String)
            memory += parseMemoryBytes(capacity["memory"] as? String)
        }
        return (cpu, memory)
    }

    private static func parseCPUMilli(_ value: String?) -> Int {
        guard let value else { return 0 }
        if value.hasSuffix("n"), let raw = Double(value.dropLast()) {
            return Int((raw / 1_000_000).rounded())
        }
        if value.hasSuffix("u"), let raw = Double(value.dropLast()) {
            return Int((raw / 1_000).rounded())
        }
        if value.hasSuffix("m"), let raw = Double(value.dropLast()) {
            return Int(raw.rounded())
        }
        return Int(((Double(value) ?? 0) * 1000).rounded())
    }

    private static func parseMemoryBytes(_ value: String?) -> Double {
        guard let value else { return 0 }
        let units: [(suffix: String, multiplier: Double)] = [
            ("Ki", 1024),
            ("Mi", 1024 * 1024),
            ("Gi", 1024 * 1024 * 1024),
            ("Ti", 1024 * 1024 * 1024 * 1024),
            ("K", 1000),
            ("M", 1000 * 1000),
            ("G", 1000 * 1000 * 1000),
            ("T", 1000 * 1000 * 1000 * 1000)
        ]
        for unit in units where value.hasSuffix(unit.suffix) {
            return (Double(value.dropLast(unit.suffix.count)) ?? 0) * unit.multiplier
        }
        return Double(value) ?? 0
    }

    private static func formatCPUMilli(_ value: Int) -> String {
        "\(max(0, value))m"
    }

    private static func formatMemoryBytes(_ value: Double) -> String {
        let mib = Int((value / (1024 * 1024)).rounded())
        return "\(max(0, mib))Mi"
    }

    private static func percentEncodePathComponent(_ value: String) -> String {
        value.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? value
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

private struct HelmStorageObject {
    let name: String
    let namespace: String
    let labels: [String: String]
    let releasePayload: String
    let isSecret: Bool

    static func parseList(_ raw: String, storageResource: String) throws -> [HelmStorageObject] {
        guard
            let root = try JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any],
            let items = root["items"] as? [[String: Any]]
        else { return [] }

        return items.compactMap { item in
            guard
                let metadata = item["metadata"] as? [String: Any],
                let name = metadata["name"] as? String,
                let namespace = metadata["namespace"] as? String
            else { return nil }

            let labels = (metadata["labels"] as? [String: String]) ?? [:]
            let payload: String?
            if storageResource == "secrets" {
                payload = (item["data"] as? [String: String])?["release"]
            } else {
                payload = (item["data"] as? [String: String])?["release"]
            }
            guard let payload, !payload.isEmpty else { return nil }
            return HelmStorageObject(
                name: name,
                namespace: namespace,
                labels: labels,
                releasePayload: payload,
                isSecret: storageResource == "secrets"
            )
        }
    }

    func decodeRelease() throws -> DecodedHelmRelease {
        let storedRelease: String
        if isSecret {
            guard let decoded = Data(base64Encoded: releasePayload),
                  let string = String(data: decoded, encoding: .utf8) else {
                throw RuneError.parseError(message: "Helm release Secret \(namespace)/\(name) could not be base64 decoded.")
            }
            storedRelease = string
        } else {
            storedRelease = releasePayload
        }

        let releaseJSON: Data
        if let storedData = Data(base64Encoded: storedRelease),
           let inflated = try? GzipInflator.inflate(storedData) {
            releaseJSON = inflated
        } else if let plain = storedRelease.data(using: .utf8), plain.first == UInt8(ascii: "{") {
            releaseJSON = plain
        } else {
            throw RuneError.parseError(message: "Helm release \(namespace)/\(name) uses an unsupported storage payload.")
        }

        guard let root = try JSONSerialization.jsonObject(with: releaseJSON) as? [String: Any] else {
            throw RuneError.parseError(message: "Helm release \(namespace)/\(name) JSON could not be parsed.")
        }
        return DecodedHelmRelease(raw: root, fallbackNamespace: namespace, labels: labels)
    }
}

private struct DecodedHelmRelease {
    let name: String
    let namespace: String
    let revision: Int
    let updated: String
    let status: String
    let chart: String
    let appVersion: String
    let description: String
    let config: Any
    let manifest: String

    init(raw: [String: Any], fallbackNamespace: String, labels: [String: String]) {
        let info = raw["info"] as? [String: Any] ?? [:]
        let chartRoot = raw["chart"] as? [String: Any] ?? [:]
        let metadata = chartRoot["metadata"] as? [String: Any] ?? [:]
        let chartName = metadata["name"] as? String ?? labels["name"] ?? ""
        let chartVersion = metadata["version"] as? String ?? ""

        self.name = raw["name"] as? String ?? labels["name"] ?? ""
        self.namespace = raw["namespace"] as? String ?? fallbackNamespace
        self.revision = raw["version"] as? Int ?? Int(labels["version"] ?? "") ?? 0
        self.updated = info["last_deployed"] as? String
            ?? info["lastDeployed"] as? String
            ?? info["first_deployed"] as? String
            ?? ""
        self.status = info["status"] as? String ?? labels["status"] ?? ""
        self.chart = [chartName, chartVersion].filter { !$0.isEmpty }.joined(separator: "-")
        self.appVersion = metadata["appVersion"] as? String
            ?? metadata["app_version"] as? String
            ?? ""
        self.description = info["description"] as? String ?? ""
        self.config = raw["config"] ?? [:]
        self.manifest = raw["manifest"] as? String ?? ""
    }

    var summary: HelmReleaseSummary {
        HelmReleaseSummary(
            name: name,
            namespace: namespace,
            revision: revision,
            updated: updated,
            status: status,
            chart: chart,
            appVersion: appVersion
        )
    }

    var revisionSummary: HelmReleaseRevision {
        HelmReleaseRevision(
            revision: revision,
            updated: updated,
            status: status,
            chart: chart,
            appVersion: appVersion,
            description: description
        )
    }
}

private enum HelmValueYAMLRenderer {
    static func render(_ value: Any) -> String {
        let rendered = renderValue(value, indent: 0)
        return rendered.isEmpty ? "{}\n" : rendered
    }

    private static func renderValue(_ value: Any, indent: Int) -> String {
        if let dictionary = value as? [String: Any] {
            return renderDictionary(dictionary, indent: indent)
        }
        if let array = value as? [Any] {
            return renderArray(array, indent: indent)
        }
        return "\(scalar(value))\n"
    }

    private static func renderDictionary(_ dictionary: [String: Any], indent: Int) -> String {
        guard !dictionary.isEmpty else { return "{}\n" }
        let prefix = String(repeating: " ", count: indent)
        return dictionary.keys.sorted().map { key in
            let value = dictionary[key] as Any
            if value is [String: Any] || value is [Any] {
                let nested = renderValue(value, indent: indent + 2)
                return "\(prefix)\(key):\n\(nested)"
            }
            return "\(prefix)\(key): \(scalar(value))\n"
        }.joined()
    }

    private static func renderArray(_ array: [Any], indent: Int) -> String {
        guard !array.isEmpty else { return "[]\n" }
        let prefix = String(repeating: " ", count: indent)
        return array.map { value in
            if value is [String: Any] || value is [Any] {
                let nested = renderValue(value, indent: indent + 2)
                return "\(prefix)-\n\(nested)"
            }
            return "\(prefix)- \(scalar(value))\n"
        }.joined()
    }

    private static func scalar(_ value: Any) -> String {
        switch value {
        case let string as String:
            if string.isEmpty { return "\"\"" }
            if string.range(of: #"[:#\[\]\{\},&\*\?|\-<>=!%@`]"#, options: .regularExpression) != nil
                || string.trimmingCharacters(in: .whitespacesAndNewlines) != string {
                if let encoded = try? JSONEncoder().encode(string) {
                    return String(decoding: encoded, as: UTF8.self)
                }
                return "\"\(string.replacingOccurrences(of: "\"", with: "\\\""))\""
            }
            return string
        case let number as NSNumber:
            return number.stringValue
        case is NSNull:
            return "null"
        default:
            return "\(value)"
        }
    }
}

private enum GzipInflator {
    static func inflate(_ data: Data) throws -> Data {
        let body = try deflateBody(from: data)
        let initialCapacity = max(Int(gzipISize(data) ?? 0), body.count * 4, 1024)
        return try inflateRawDeflate(body, initialCapacity: initialCapacity)
    }

    private static func deflateBody(from data: Data) throws -> Data {
        let bytes = [UInt8](data)
        guard bytes.count >= 18, bytes[0] == 0x1f, bytes[1] == 0x8b, bytes[2] == 8 else {
            throw RuneError.parseError(message: "Helm release payload is not gzip data.")
        }
        let flags = bytes[3]
        var index = 10

        if flags & 0x04 != 0 {
            guard index + 2 <= bytes.count else { throw RuneError.parseError(message: "Malformed gzip header.") }
            let extraLength = Int(bytes[index]) | (Int(bytes[index + 1]) << 8)
            index += 2 + extraLength
        }
        if flags & 0x08 != 0 {
            while index < bytes.count, bytes[index] != 0 { index += 1 }
            index += 1
        }
        if flags & 0x10 != 0 {
            while index < bytes.count, bytes[index] != 0 { index += 1 }
            index += 1
        }
        if flags & 0x02 != 0 {
            index += 2
        }

        guard index < bytes.count - 8 else {
            throw RuneError.parseError(message: "Malformed gzip body.")
        }
        return Data(bytes[index..<(bytes.count - 8)])
    }

    private static func gzipISize(_ data: Data) -> UInt32? {
        let bytes = [UInt8](data)
        guard bytes.count >= 4 else { return nil }
        let tail = bytes.suffix(4)
        return tail.enumerated().reduce(UInt32(0)) { partial, pair in
            partial | (UInt32(pair.element) << UInt32(pair.offset * 8))
        }
    }

    private static func inflateRawDeflate(_ data: Data, initialCapacity: Int) throws -> Data {
        var capacity = initialCapacity
        while capacity <= 64 * 1024 * 1024 {
            var output = [UInt8](repeating: 0, count: capacity)
            let decoded = data.withUnsafeBytes { source in
                compression_decode_buffer(
                    &output,
                    output.count,
                    source.bindMemory(to: UInt8.self).baseAddress!,
                    data.count,
                    nil,
                    COMPRESSION_ZLIB
                )
            }
            if decoded > 0 {
                return Data(output.prefix(decoded))
            }
            capacity *= 2
        }
        throw RuneError.parseError(message: "Helm release payload could not be decompressed.")
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

private actor TerminalSessionRegistry {
    private var handles: [String: any RunningCommandControlling] = [:]

    func insert(handle: any RunningCommandControlling, id: String) {
        handles[id] = handle
    }

    func handle(id: String) -> (any RunningCommandControlling)? {
        handles[id]
    }

    func remove(id: String) -> (any RunningCommandControlling)? {
        handles.removeValue(forKey: id)
    }
}
