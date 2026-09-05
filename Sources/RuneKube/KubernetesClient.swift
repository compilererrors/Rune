import Compression
import Foundation
import RuneCore
import RuneDiagnostics
import RuneSecurity

public enum DeploymentRolloutVerificationStatus: String, Sendable, Equatable {
    case ready
    case progressing
    case timedOut
    case failed
}

public struct DeploymentRolloutVerificationResult: Sendable, Equatable {
    public let status: DeploymentRolloutVerificationStatus
    public let desiredReplicas: Int
    public let readyReplicas: Int
    public let updatedReplicas: Int
    public let availableReplicas: Int
    public let message: String

    public init(
        status: DeploymentRolloutVerificationStatus,
        desiredReplicas: Int,
        readyReplicas: Int,
        updatedReplicas: Int,
        availableReplicas: Int,
        message: String
    ) {
        self.status = status
        self.desiredReplicas = desiredReplicas
        self.readyReplicas = readyReplicas
        self.updatedReplicas = updatedReplicas
        self.availableReplicas = availableReplicas
        self.message = message
    }
}

public enum KubernetesExecCredentialCacheState: String, Sendable, Equatable {
    case hit
    case miss
    case expired
}

public struct KubernetesExecCredentialCacheDiagnostic: Sendable, Equatable {
    public let state: KubernetesExecCredentialCacheState
    public let expiresAt: Date?

    public init(state: KubernetesExecCredentialCacheState, expiresAt: Date?) {
        self.state = state
        self.expiresAt = expiresAt
    }
}

public final class KubernetesClient: ContextListingService, NamespaceListingService, PodListingService, DeploymentListingService, ServiceListingService, EventListingService, GenericResourceListingService, PodLogService, UnifiedServiceLogService, UnifiedDeploymentLogService, ManifestService, ManifestValidationService, ResourceWriteService, HelmReleaseService, @unchecked Sendable {
    private let parser: KubernetesOutputParser
    private let restClient: KubernetesRESTClient
    private let requestMetricsRecorder: KubernetesRESTRequestMetricsRecorder
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
    private let unifiedLogsMaxConcurrentPodFetches: Int = 3
    /// Selector/pod-discovery for unified logs should fail fast; stale workloads should not block the inspector for minutes.
    private let unifiedLogsSelectorTimeout: TimeInterval = 12
    /// Validation should feel near-live while still giving the API server room on slower clusters.
    private let manifestValidationTimeout: TimeInterval = 20

    public init(
        parser: KubernetesOutputParser = KubernetesOutputParser(),
        commandTimeout: TimeInterval = 30,
        access: SecurityScopedAccess = SecurityScopedAccess()
    ) {
        let requestMetricsRecorder = KubernetesRESTRequestMetricsRecorder()
        self.parser = parser
        self.restClient = KubernetesRESTClient(requestMetricsRecorder: requestMetricsRecorder)
        self.requestMetricsRecorder = requestMetricsRecorder
        self.commandTimeout = commandTimeout
        self.access = access
    }

    init(
        parser: KubernetesOutputParser = KubernetesOutputParser(),
        commandTimeout: TimeInterval = 30,
        access: SecurityScopedAccess = SecurityScopedAccess(),
        restClient: KubernetesRESTClient,
        requestMetricsRecorder: KubernetesRESTRequestMetricsRecorder
    ) {
        self.parser = parser
        self.restClient = restClient
        self.requestMetricsRecorder = requestMetricsRecorder
        self.commandTimeout = commandTimeout
        self.access = access
    }

    @available(*, deprecated, message: "Use restRequestMetricsReport().metrics so metrics and summary are captured atomically.")
    public func restRequestMetricsSnapshot() async -> [KubernetesRESTRequestMetric] {
        await requestMetricsRecorder.report().metrics
    }

    @available(*, deprecated, message: "Use restRequestMetricsReport(contextName:).metrics so metrics and summary are captured atomically.")
    public func restRequestMetricsSnapshot(contextName: String) async -> [KubernetesRESTRequestMetric] {
        await requestMetricsRecorder.report(contextName: contextName).metrics
    }

    public func restRequestMetricsSummary() async -> KubernetesRESTRequestMetricsSummary {
        await requestMetricsRecorder.summary()
    }

    public func restRequestMetricsSummary(contextName: String) async -> KubernetesRESTRequestMetricsSummary {
        await requestMetricsRecorder.summary(contextName: contextName)
    }

    public func restRequestMetricsReport() async -> KubernetesRESTRequestMetricsReport {
        await requestMetricsRecorder.report()
    }

    public func restRequestMetricsReport(contextName: String) async -> KubernetesRESTRequestMetricsReport {
        await requestMetricsRecorder.report(contextName: contextName)
    }

    public func restRequestMetricsReport(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async -> KubernetesRESTRequestMetricsReport {
        guard let environment = try? kubeconfigEnvironment(from: sources),
              let scopeIdentity = try? await restClient.requestMetricsScopeIdentity(
                  environment: environment,
                  contextName: context.name
              ) else {
            return .empty
        }
        return await requestMetricsRecorder.report(
            contextName: context.name,
            scopeIdentity: scopeIdentity
        )
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

    public func canI(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String?,
        verb: String,
        resource: String,
        apiGroup: String? = nil,
        subresource: String? = nil
    ) async throws -> Bool {
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.selfSubjectAccessReview(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            verb: verb,
            resource: resource,
            apiGroup: apiGroup,
            subresource: subresource,
            timeout: commandTimeout
        )
    }

    public func execCredentialCacheDiagnostic(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> KubernetesExecCredentialCacheDiagnostic? {
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.execCredentialCacheDiagnostic(
            environment: env,
            contextName: context.name
        )
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

    public func watchResourceChanges(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        resourceVersion: String? = nil,
        timeoutSeconds: Int = 290
    ) async throws -> AsyncThrowingStream<KubernetesResourceWatchEvent, Error> {
        let environment = try kubeconfigEnvironment(from: sources)
        return try await restClient.watchCollection(
            environment: environment,
            contextName: context.name,
            resource: KubernetesRESTPath.resourceName(for: kind),
            namespace: kind.isNamespaced ? namespace : nil,
            resourceVersion: resourceVersion,
            timeoutSeconds: timeoutSeconds
        )
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
                ageDescription: pod.ageDescription,
                podIP: pod.podIP,
                hostIP: pod.hostIP,
                nodeName: pod.nodeName,
                qosClass: pod.qosClass,
                containersReady: pod.containersReady,
                containerNamesLine: pod.containerNamesLine,
                initContainerNamesLine: pod.initContainerNamesLine,
                ephemeralContainerNamesLine: pod.ephemeralContainerNamesLine,
                labels: pod.labels,
                containerImagesLine: pod.containerImagesLine,
                ownerReferencesLine: pod.ownerReferencesLine
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

    public func listOperatorResources(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [OperatorResourceSummary] {
        struct Definition {
            let family: String
            let kind: String
            let apiPath: String
        }

        let ns = namespace.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? namespace
        let definitions = [
            Definition(family: "cert-manager", kind: "Certificates", apiPath: "/apis/cert-manager.io/v1/namespaces/\(ns)/certificates"),
            Definition(family: "cert-manager", kind: "CertificateRequests", apiPath: "/apis/cert-manager.io/v1/namespaces/\(ns)/certificaterequests"),
            Definition(family: "cert-manager", kind: "Issuers", apiPath: "/apis/cert-manager.io/v1/namespaces/\(ns)/issuers"),
            Definition(family: "cert-manager", kind: "ClusterIssuers", apiPath: "/apis/cert-manager.io/v1/clusterissuers"),
            Definition(family: "Flux", kind: "GitRepositories", apiPath: "/apis/source.toolkit.fluxcd.io/v1/namespaces/\(ns)/gitrepositories"),
            Definition(family: "Flux", kind: "Kustomizations", apiPath: "/apis/kustomize.toolkit.fluxcd.io/v1/namespaces/\(ns)/kustomizations"),
            Definition(family: "Flux", kind: "HelmReleases", apiPath: "/apis/helm.toolkit.fluxcd.io/v2/namespaces/\(ns)/helmreleases"),
            Definition(family: "ArgoCD", kind: "Applications", apiPath: "/apis/argoproj.io/v1alpha1/namespaces/\(ns)/applications"),
            Definition(family: "ArgoCD", kind: "AppProjects", apiPath: "/apis/argoproj.io/v1alpha1/namespaces/\(ns)/appprojects"),
            Definition(family: "External Secrets", kind: "ExternalSecrets", apiPath: "/apis/external-secrets.io/v1beta1/namespaces/\(ns)/externalsecrets"),
            Definition(family: "External Secrets", kind: "SecretStores", apiPath: "/apis/external-secrets.io/v1beta1/namespaces/\(ns)/secretstores"),
            Definition(family: "External Secrets", kind: "ClusterSecretStores", apiPath: "/apis/external-secrets.io/v1beta1/clustersecretstores"),
            Definition(family: "Crossplane", kind: "Compositions", apiPath: "/apis/apiextensions.crossplane.io/v1/compositions"),
            Definition(family: "Crossplane", kind: "CompositeResourceDefinitions", apiPath: "/apis/apiextensions.crossplane.io/v1/compositeresourcedefinitions"),
            Definition(family: "Crossplane", kind: "Providers", apiPath: "/apis/pkg.crossplane.io/v1/providers"),
            Definition(family: "Crossplane", kind: "Configurations", apiPath: "/apis/pkg.crossplane.io/v1/configurations"),
            Definition(family: "Gateway API", kind: "GatewayClasses", apiPath: "/apis/gateway.networking.k8s.io/v1/gatewayclasses"),
            Definition(family: "Gateway API", kind: "Gateways", apiPath: "/apis/gateway.networking.k8s.io/v1/namespaces/\(ns)/gateways"),
            Definition(family: "Gateway API", kind: "HTTPRoutes", apiPath: "/apis/gateway.networking.k8s.io/v1/namespaces/\(ns)/httproutes"),
            Definition(family: "Gateway API", kind: "GRPCRoutes", apiPath: "/apis/gateway.networking.k8s.io/v1/namespaces/\(ns)/grpcroutes"),
            Definition(family: "Prometheus Operator", kind: "Prometheuses", apiPath: "/apis/monitoring.coreos.com/v1/namespaces/\(ns)/prometheuses"),
            Definition(family: "Prometheus Operator", kind: "Alertmanagers", apiPath: "/apis/monitoring.coreos.com/v1/namespaces/\(ns)/alertmanagers"),
            Definition(family: "Prometheus Operator", kind: "ServiceMonitors", apiPath: "/apis/monitoring.coreos.com/v1/namespaces/\(ns)/servicemonitors"),
            Definition(family: "Prometheus Operator", kind: "PodMonitors", apiPath: "/apis/monitoring.coreos.com/v1/namespaces/\(ns)/podmonitors"),
            Definition(family: "Prometheus Operator", kind: "PrometheusRules", apiPath: "/apis/monitoring.coreos.com/v1/namespaces/\(ns)/prometheusrules"),
            Definition(family: "Prometheus Operator", kind: "ThanosRulers", apiPath: "/apis/monitoring.coreos.com/v1/namespaces/\(ns)/thanosrulers")
        ]

        let env = try kubeconfigEnvironment(from: sources)
        let crdPrinterColumns = await crdPrinterColumnDefinitions(environment: env, contextName: context.name)
        var output: [OperatorResourceSummary] = []

        for definition in definitions {
            guard let raw = try? await restClient.customCollection(
                environment: env,
                contextName: context.name,
                apiPath: definition.apiPath,
                timeout: 20
            ) else {
                continue
            }
            output += Self.parseOperatorResources(
                raw,
                family: definition.family,
                kind: definition.kind,
                apiPath: definition.apiPath,
                printerColumnDefinitions: Self.printerColumnDefinitions(for: definition.apiPath, in: crdPrinterColumns)
            )
        }

        output += await discoverGenericCustomResources(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            alreadyListedPaths: Set(definitions.map(\.apiPath)),
            crdPrinterColumns: crdPrinterColumns
        )

        return output.sorted {
            if $0.family != $1.family { return $0.family < $1.family }
            if $0.kind != $1.kind { return $0.kind < $1.kind }
            return $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
        }
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

    public func listEndpoints(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "endpoints",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseEndpoints(namespace: namespace, from: raw) }
        )
    }

    public func listServiceAccounts(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        return try await listViaREST(
            environment: env,
            context: context,
            resource: "serviceaccounts",
            namespace: namespace,
            timeout: slowNamespacedJSONListTimeout,
            parse: { raw in try self.parser.parseServiceAccounts(namespace: namespace, from: raw) }
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
        await Self.pagedCollectionCount(
            firstRequest: firstRequest,
            nextRequest: nextRequest,
            maxPages: pagedCountMaxPages,
            progress: progress,
            fetch: { [restClient] request in
                try await restClient.rawGET(
                    environment: environment,
                    contextName: context.name,
                    apiPath: request.apiPath,
                    timeout: 45
                )
            },
            fallbackTotal: { [weak self] in
                guard let self else { return nil }
                return await self.collectionListTotalFromMetadataProbe(
                    context: context,
                    environment: environment,
                    apiPath: firstRequest.apiPath
                )
            }
        )
    }

    static func pagedCollectionCount(
        firstRequest: KubernetesRESTRequest,
        nextRequest: (String) -> KubernetesRESTRequest?,
        maxPages: Int,
        progress: ((Int) -> Void)?,
        fetch: (KubernetesRESTRequest) async throws -> String,
        fallbackTotal: (() async -> Int?)?
    ) async -> Int? {
        var request = firstRequest
        var total = 0
        var completedPages = 0

        for _ in 0..<maxPages {
            let raw: String
            do {
                raw = try await fetch(request)
            } catch {
                if completedPages > 0, isExpiredContinueTokenError(error) {
                    return await fallbackTotal?()
                }
                return nil
            }

            guard let page = KubernetesListJSON.collectionPageInfo(from: raw) else {
                return nil
            }
            total += page.itemsCount
            completedPages += 1
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

    private static func isExpiredContinueTokenError(_ error: Error) -> Bool {
        guard case let RuneError.commandFailed(_, message) = error else { return false }
        let normalized = message.lowercased()
        return normalized.contains("http 410")
            && (normalized.contains("continue") || normalized.contains("expired") || normalized.contains("too old"))
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
        container: String? = nil,
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> String {
        if let container = Self.normalizedContainerName(container) {
            return try await podLogs(
                from: sources,
                context: context,
                namespace: namespace,
                podName: podName,
                container: container,
                filter: filter,
                previous: previous,
                timeoutOverride: nil,
                profile: .pod
            )
        }

        let pod = try await fetchPodSummaryForInspector(
            from: sources,
            context: context,
            namespace: namespace,
            podName: podName
        )
        return try await podLogs(
            from: sources,
            context: context,
            namespace: namespace,
            podName: podName,
            containers: pod.logContainerNames,
            filter: filter,
            previous: previous
        )
    }

    /// Fetches one or every known container without relying on kubectl-only `allContainers` semantics.
    public func podLogs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        containers: [String],
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> String {
        let containers = Self.normalizedContainerNames(containers)
        guard containers.count > 1 else {
            return try await podLogs(
                from: sources,
                context: context,
                namespace: namespace,
                podName: podName,
                container: containers.first,
                filter: filter,
                previous: previous,
                timeoutOverride: nil,
                profile: .pod
            )
        }

        let lines = try await collectContainerPodLogLines(
            podName: podName,
            containers: containers,
            sources: sources,
            context: context,
            namespace: namespace,
            filter: filter,
            previous: previous,
            timeoutOverride: nil,
            profile: .pod,
            allowsPartialFailure: true
        )
        return lines
            .map { line in
                "[\(line.containerName ?? "container")] \(line.text)"
            }
            .joined(separator: "\n")
    }

    private static func normalizedContainerName(_ container: String?) -> String? {
        let trimmed = container?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return trimmed.isEmpty ? nil : trimmed
    }

    private static func normalizedContainerNames(_ containers: [String]) -> [String] {
        var seen: Set<String> = []
        return containers.compactMap { container in
            guard let normalized = normalizedContainerName(container), seen.insert(normalized).inserted else {
                return nil
            }
            return normalized
        }
    }

    private func collectContainerPodLogLines(
        podName: String,
        containers: [String],
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        filter: LogTimeFilter,
        previous: Bool,
        timeoutOverride: TimeInterval?,
        profile: LogQueryProfile,
        allowsPartialFailure: Bool
    ) async throws -> [TaggedLogLine] {
        let containers = Self.normalizedContainerNames(containers)
        guard containers.count > 1 else {
            let logs = try await podLogs(
                from: sources,
                context: context,
                namespace: namespace,
                podName: podName,
                container: containers.first,
                filter: filter,
                previous: previous,
                timeoutOverride: timeoutOverride,
                profile: profile
            )
            return taggedLines(from: logs, podName: podName, containerName: nil)
        }

        var collectedLines: [TaggedLogLine] = []
        var failures: [(container: String, error: Error)] = []
        var successfulContainerFetches = 0
        for container in containers {
            do {
                let logs = try await podLogs(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    podName: podName,
                    container: container,
                    filter: filter,
                    previous: previous,
                    timeoutOverride: timeoutOverride,
                    profile: profile
                )
                successfulContainerFetches += 1
                collectedLines.append(
                    contentsOf: taggedLines(
                        from: logs,
                        podName: podName,
                        containerName: container
                    )
                )
            } catch {
                if error is CancellationError || !allowsPartialFailure {
                    throw error
                }
                failures.append((container, error))
            }
        }

        if successfulContainerFetches == 0, let firstFailure = failures.first {
            throw firstFailure.error
        }
        for failure in failures {
            collectedLines.append(TaggedLogLine(
                podName: podName,
                containerName: failure.container,
                text: "⚠ Logs unavailable for container \(failure.container): \(failure.error.localizedDescription)",
                timestamp: nil,
                sequence: Int.max
            ))
        }
        return collectedLines
    }

    private func podLogs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        container: String?,
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
            container: container,
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
                let containerTag = line.containerName.map { " [\($0)]" } ?? ""
                return "[\(line.podName)]\(containerTag) \(line.text)"
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
                let containerTag = line.containerName.map { " [\($0)]" } ?? ""
                return "[\(line.podName)]\(containerTag) \(line.text)"
            }
            .joined(separator: "\n")

        return UnifiedDeploymentLogs(
            deployment: deployment,
            podNames: selectedPods.map(\.name).sorted(),
            mergedText: merged
        )
    }

    public func podsForDeployment(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deployment: DeploymentSummary
    ) async throws -> [PodSummary] {
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
            throw RuneError.parseError(message: "Deployment \(deployment.name) is missing a matchLabels selector.")
        }

        let selector = selectorMap
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: ",")

        return try await requiredPodsBySelectorViaREST(
            environment: env,
            context: context,
            namespace: namespace,
            selector: selector
        )
    }

    private func selectPodsForUnifiedLogs(_ pods: [PodSummary]) -> [PodSummary] {
        pods
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
        var failures: [Error] = []
        var successfulPodFetches = 0
        try await withThrowingTaskGroup(of: UnifiedPodLogFetchResult.self) { group in
            var nextPodIndex = 0
            let initial = min(unifiedLogsMaxConcurrentPodFetches, pods.count)

            for _ in 0..<initial {
                let pod = pods[nextPodIndex]
                nextPodIndex += 1
                group.addTask {
                    do {
                        let lines = try await self.collectContainerPodLogLines(
                            podName: pod.name,
                            containers: pod.logContainerNames,
                            sources: sources,
                            context: context,
                            namespace: namespace,
                            filter: filter,
                            previous: previous,
                            timeoutOverride: nil,
                            profile: .unifiedPerPod,
                            allowsPartialFailure: true
                        )
                        return .success(lines)
                    } catch {
                        if error is CancellationError {
                            throw error
                        }
                        return .failure(podName: pod.name, error: error)
                    }
                }
            }

            while let result = try await group.next() {
                switch result {
                case let .success(podLines):
                    successfulPodFetches += 1
                    collectedLines.append(contentsOf: podLines)
                case let .failure(podName, error):
                    failures.append(error)
                    collectedLines.append(TaggedLogLine(
                        podName: podName,
                        containerName: nil,
                        text: "⚠ Logs unavailable for pod \(podName): \(error.localizedDescription)",
                        timestamp: nil,
                        sequence: Int.max
                    ))
                }
                if nextPodIndex < pods.count {
                    let pod = pods[nextPodIndex]
                    nextPodIndex += 1
                    group.addTask {
                        do {
                            let lines = try await self.collectContainerPodLogLines(
                                podName: pod.name,
                                containers: pod.logContainerNames,
                                sources: sources,
                                context: context,
                                namespace: namespace,
                                filter: filter,
                                previous: previous,
                                timeoutOverride: nil,
                                profile: .unifiedPerPod,
                                allowsPartialFailure: true
                            )
                            return .success(lines)
                        } catch {
                            if error is CancellationError {
                                throw error
                            }
                            return .failure(podName: pod.name, error: error)
                        }
                    }
                }
            }
        }

        if successfulPodFetches == 0, let firstFailure = failures.first {
            throw firstFailure
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

    public func operatorResourceYAML(
        from sources: [KubeConfigSource],
        context: KubeContext,
        resource: OperatorResourceSummary
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.customResourceYAML(
            environment: env,
            contextName: context.name,
            collectionAPIPath: resource.apiPath,
            name: resource.name,
            timeout: commandTimeout
        )
    }

    public func operatorResourceDescribe(
        from sources: [KubeConfigSource],
        context: KubeContext,
        resource: OperatorResourceSummary
    ) async throws -> String {
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.customResourceDescribe(
            environment: env,
            contextName: context.name,
            resource: resource,
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
        let registrationGeneration = await terminalSessionRegistry.beginStart(id: sessionID)
        do {
            try Task.checkCancellation()
            if await terminalSessionRegistry.isStopRequested(
                id: sessionID,
                generation: registrationGeneration
            ) {
                _ = await terminalSessionRegistry.finishStart(
                    id: sessionID,
                    generation: registrationGeneration
                )
                throw CancellationError()
            }
            let handle = try await restClient.startPodTerminalSession(
                environment: env,
                contextName: context.name,
                namespace: namespace,
                podName: podName,
                container: container,
                shellCommand: shellCommand,
                onOutput: onOutput,
                onTermination: { [terminalSessionRegistry] exitCode in
                    Task {
                        let shouldNotify = await terminalSessionRegistry.complete(
                            id: sessionID,
                            generation: registrationGeneration
                        )
                        guard shouldNotify else { return }
                        onTermination(exitCode)
                    }
                }
            )
            if Task.isCancelled {
                handle.terminate()
                throw CancellationError()
            }
            let didInsert = await terminalSessionRegistry.insert(
                handle: handle,
                id: sessionID,
                generation: registrationGeneration
            )
            guard didInsert else {
                throw CancellationError()
            }
        } catch {
            _ = await terminalSessionRegistry.finishStart(
                id: sessionID,
                generation: registrationGeneration
            )
            throw error
        }
    }

    public func writeToPodTerminalSession(id: String, text: String) async throws {
        guard let handle = await terminalSessionRegistry.handle(id: id) else {
            throw RuneError.commandFailed(command: "terminal session", message: "No active terminal session")
        }
        try handle.writeToStdin(Data(text.utf8))
    }

    public func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws {
        guard let handle = await terminalSessionRegistry.handle(id: id) else {
            throw RuneError.commandFailed(command: "terminal session", message: "No active terminal session")
        }
        try handle.resizeTerminal(columns: columns, rows: rows)
    }

    public func stopPodTerminalSession(id: String) async {
        let handle = await terminalSessionRegistry.remove(
            id: id,
            rememberIfNotStarted: true
        )
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

    public func scaleStatefulSet(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        statefulSetName: String,
        replicas: Int
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        try await restClient.scaleStatefulSet(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            statefulSetName: statefulSetName,
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

    public func restartStatefulSetRollout(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        statefulSetName: String
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
        try await restClient.restartStatefulSetRollout(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            statefulSetName: statefulSetName,
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

    public func dryRunRollbackDeploymentRollout(
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
            timeout: 90,
            dryRun: true
        )
    }

    public func verifyDeploymentRollout(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String,
        timeout: TimeInterval? = nil
    ) async throws -> DeploymentRolloutVerificationResult {
        let timeoutBudget = max(0.05, timeout ?? commandTimeout)
        let env = try kubeconfigEnvironment(from: sources)
        return try await restClient.verifyDeploymentRollout(
            environment: env,
            contextName: context.name,
            namespace: namespace,
            deploymentName: deploymentName,
            timeout: timeoutBudget,
            pollInterval: min(1, max(0.05, timeoutBudget / 4))
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

        let registrationGeneration = await portForwardRegistry.beginStart(id: sessionID)
        onEvent(baseSession)

        func stoppedSession(message: String) -> PortForwardSession {
            PortForwardSession(
                id: sessionID,
                contextName: context.name,
                namespace: namespace,
                targetKind: targetKind,
                targetName: targetName,
                localPort: localPort,
                remotePort: remotePort,
                address: address,
                status: .stopped,
                lastMessage: message
            )
        }

        func failedSession(message: String) -> PortForwardSession {
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
        }

        do {
            let podName: String
            switch targetKind {
            case .pod:
                podName = targetName
            case .service:
                try Task.checkCancellation()
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

            try Task.checkCancellation()
            if await portForwardRegistry.isStopRequested(
                id: sessionID,
                generation: registrationGeneration
            ) {
                _ = await portForwardRegistry.finishStart(
                    id: sessionID,
                    generation: registrationGeneration
                )
                let session = stoppedSession(message: "Port-forward stopped before it connected.")
                onEvent(session)
                return session
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
                    Task {
                        guard await self.portForwardRegistry.shouldDeliverReady(
                            id: sessionID,
                            generation: registrationGeneration
                        ) else {
                            return
                        }
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
                    }
                },
                onFailure: { message in
                    Task {
                        let disposition = await self.portForwardRegistry.recordFailure(
                            message: message,
                            id: sessionID,
                            generation: registrationGeneration
                        )
                        guard case let .active(handle) = disposition else {
                            return
                        }
                        handle.terminate()
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
                }
            )
            if Task.isCancelled {
                handle.terminate()
                throw CancellationError()
            }
            let registrationResult = await portForwardRegistry.register(
                handle: handle,
                id: sessionID,
                generation: registrationGeneration
            )
            switch registrationResult {
            case .inserted:
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
            case .failed(let message):
                let session = failedSession(message: message)
                onEvent(session)
                return session
            case .stopped, .stale:
                let session = stoppedSession(message: "Port-forward stopped before it became ready.")
                onEvent(session)
                return session
            }
        } catch {
            let wasStopRequested = await portForwardRegistry.finishStart(
                id: sessionID,
                generation: registrationGeneration
            )
            guard wasStopRequested || error is CancellationError else {
                onEvent(failedSession(message: error.localizedDescription))
                throw error
            }
            let session = stoppedSession(message: "Port-forward stopped before it connected.")
            onEvent(session)
            return session
        }
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
            access.retainAccess(to: url)
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
        container: String?,
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
                container: container,
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

    private func taggedLines(
        from logs: String,
        podName: String,
        containerName: String? = nil
    ) -> [TaggedLogLine] {
        logs
            .split(whereSeparator: \.isNewline)
            .map { String($0) }
            .filter { !$0.isEmpty }
            .enumerated()
            .map { index, line in
                TaggedLogLine(
                    podName: podName,
                    containerName: containerName,
                    text: line,
                    timestamp: parseTimestamp(line),
                    sequence: index
                )
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

        let lhsContainer = lhs.containerName ?? ""
        let rhsContainer = rhs.containerName ?? ""
        if lhsContainer != rhsContainer {
            return lhsContainer < rhsContainer
        }

        if lhs.sequence != rhs.sequence {
            return lhs.sequence < rhs.sequence
        }

        return lhs.text < rhs.text
    }

    private struct TaggedLogLine: Sendable {
        let podName: String
        let containerName: String?
        let text: String
        let timestamp: Date?
        let sequence: Int
    }

    private enum UnifiedPodLogFetchResult: Sendable {
        case success([TaggedLogLine])
        case failure(podName: String, error: Error)
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

        var issues = parseLineScopedValidationIssues(from: trimmed, yaml: yaml)
        issues.append(contentsOf: parseKubernetesValidationIssues(from: trimmed, yaml: yaml))

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

        if isLikelyTransportValidationOutput(trimmed) {
            return [
                YAMLValidationIssue(
                    source: .transport,
                    severity: .warning,
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

    private static func parseKubernetesValidationIssues(
        from output: String,
        yaml: String
    ) -> [YAMLValidationIssue] {
        if let typedPatchIssue = parseTypedPatchValidationIssue(from: output, yaml: yaml) {
            return [typedPatchIssue]
        }

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

    private static func parseTypedPatchValidationIssue(
        from output: String,
        yaml: String
    ) -> YAMLValidationIssue? {
        let pattern = #"\.([A-Za-z0-9_.\-\[\]]+):\s*expected numeric \(int or float\),\s*got\s+[A-Za-z0-9_-]+"#
        guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
            return nil
        }

        let nsOutput = output as NSString
        guard let match = regex.firstMatch(
            in: output,
            range: NSRange(location: 0, length: nsOutput.length)
        ), match.numberOfRanges >= 2 else {
            return nil
        }

        let fieldPath = nsOutput.substring(with: match.range(at: 1))
        let location = validationLocation(forFieldPath: fieldPath, in: yaml)
        return YAMLValidationIssue(
            source: .kubernetes,
            severity: .error,
            message: "`\(fieldPath)` must be a number. Fix or remove the field before applying.",
            line: location?.line,
            column: location?.column,
            range: location?.range
        )
    }

    private static func validationLocation(
        forFieldPath fieldPath: String,
        in yaml: String
    ) -> (line: Int, column: Int, range: YAMLValidationRange?)? {
        guard let key = fieldPath.split(separator: ".").last.map(String.init),
              !key.isEmpty
        else {
            return nil
        }

        let pattern = #"^([ \t]*)"# + NSRegularExpression.escapedPattern(for: key) + #"\s*:"#
        guard let regex = try? NSRegularExpression(pattern: pattern) else {
            return nil
        }

        let lines = yaml.split(
            omittingEmptySubsequences: false,
            whereSeparator: \.isNewline
        ).map(String.init)
        for (index, line) in lines.enumerated() {
            let nsLine = line as NSString
            guard let match = regex.firstMatch(
                in: line,
                range: NSRange(location: 0, length: nsLine.length)
            ) else {
                continue
            }

            let lineNumber = index + 1
            return (
                line: lineNumber,
                column: match.range(at: 1).length + 1,
                range: validationRange(in: yaml, line: lineNumber)
            )
        }

        return nil
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
        let isServerFailureStatus = lowered.range(
            of: #"\bhttp\s+5\d\d\b"#,
            options: .regularExpression
        ) != nil
        return isServerFailureStatus
            || lowered.contains("unable to connect to the server")
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
            containerNamesLine: detail.containerNamesLine ?? base.containerNamesLine,
            initContainerNamesLine: detail.initContainerNamesLine ?? base.initContainerNamesLine,
            ephemeralContainerNamesLine: detail.ephemeralContainerNamesLine ?? base.ephemeralContainerNamesLine,
            labels: detail.labels.isEmpty ? base.labels : detail.labels,
            containerImagesLine: detail.containerImagesLine ?? base.containerImagesLine,
            ownerReferencesLine: detail.ownerReferencesLine ?? base.ownerReferencesLine
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
                containerNamesLine: pod.containerNamesLine,
                initContainerNamesLine: pod.initContainerNamesLine,
                ephemeralContainerNamesLine: pod.ephemeralContainerNamesLine,
                labels: pod.labels,
                containerImagesLine: pod.containerImagesLine,
                ownerReferencesLine: pod.ownerReferencesLine
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
                containerNamesLine: pod.containerNamesLine,
                initContainerNamesLine: pod.initContainerNamesLine,
                ephemeralContainerNamesLine: pod.ephemeralContainerNamesLine,
                labels: pod.labels,
                containerImagesLine: pod.containerImagesLine,
                ownerReferencesLine: pod.ownerReferencesLine
            )
        }
    }

    private static func parseOperatorResources(
        _ raw: String,
        family: String,
        kind: String,
        apiPath: String,
        printerColumnDefinitions: [CRDPrinterColumnDefinition] = []
    ) -> [OperatorResourceSummary] {
        guard
            let root = try? JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any],
            let items = root["items"] as? [[String: Any]]
        else {
            return []
        }

        return items.compactMap { item in
            guard let metadata = item["metadata"] as? [String: Any],
                  let name = metadata["name"] as? String,
                  !name.isEmpty
            else { return nil }

            let namespace = metadata["namespace"] as? String
            let statusObject = item["status"] as? [String: Any]
            let conditionSummary = Self.operatorConditionSummary(statusObject?["conditions"] as? [[String: Any]])

            return OperatorResourceSummary(
                family: family,
                kind: kind,
                apiPath: apiPath,
                name: name,
                namespace: namespace,
                status: conditionSummary.status,
                message: conditionSummary.message,
                printerColumns: Self.operatorPrinterColumns(item, definitions: printerColumnDefinitions)
            )
        }
    }

    private static func operatorPrinterColumns(
        _ item: [String: Any],
        definitions: [CRDPrinterColumnDefinition]
    ) -> [OperatorResourceSummary.PrinterColumn] {
        let fromDefinitions = definitions.compactMap { definition -> OperatorResourceSummary.PrinterColumn? in
            guard let rawValue = value(atJSONPath: definition.jsonPath, in: item) else { return nil }
            let title = boundedPrinterColumnText(definition.name, limit: 24)
            let value = boundedPrinterColumnText(rawValue, limit: 80)
            guard !title.isEmpty, !value.isEmpty else { return nil }
            return OperatorResourceSummary.PrinterColumn(title: title, value: value)
        }
        if !fromDefinitions.isEmpty {
            return Array(fromDefinitions.prefix(3))
        }

        guard let rawColumns = item["additionalPrinterColumns"] as? [[String: Any]] else { return [] }
        return rawColumns.compactMap { column in
            guard let rawTitle = column["name"] as? String else { return nil }
            let title = boundedPrinterColumnText(rawTitle, limit: 24)
            guard !title.isEmpty else { return nil }

            let rawValue = column["value"] as? String
                ?? column["displayValue"] as? String
                ?? column["text"] as? String
                ?? ""
            let value = boundedPrinterColumnText(rawValue, limit: 80)
            guard !value.isEmpty else { return nil }
            return OperatorResourceSummary.PrinterColumn(title: title, value: value)
        }
        .prefix(3)
        .map { $0 }
    }

    private static func value(atJSONPath jsonPath: String, in object: [String: Any]) -> String? {
        let trimmed = jsonPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.hasPrefix(".") else { return nil }
        let tokens = trimmed.dropFirst().split(separator: ".").map(String.init)
        guard !tokens.isEmpty else { return nil }

        var current: Any = object
        for token in tokens {
            let (key, index) = jsonPathToken(token)
            guard !key.isEmpty,
                  let dictionary = current as? [String: Any],
                  let next = dictionary[key]
            else { return nil }

            if let index {
                guard let array = next as? [Any], array.indices.contains(index) else { return nil }
                current = array[index]
            } else {
                current = next
            }
        }

        switch current {
        case let value as String:
            return value
        case let value as NSNumber:
            return value.stringValue
        case let value as Bool:
            return value ? "true" : "false"
        default:
            return nil
        }
    }

    private static func jsonPathToken(_ token: String) -> (key: String, index: Int?) {
        guard let open = token.firstIndex(of: "["),
              let close = token.firstIndex(of: "]"),
              open < close
        else { return (token, nil) }
        let key = String(token[..<open])
        let rawIndex = token[token.index(after: open)..<close]
        return (key, Int(rawIndex))
    }

    private static func boundedPrinterColumnText(_ value: String, limit: Int) -> String {
        let normalized = value
            .replacingOccurrences(of: "\n", with: " ")
            .replacingOccurrences(of: "\t", with: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard normalized.count > limit else { return normalized }
        return String(normalized.prefix(max(0, limit - 1))) + "…"
    }

    private static func operatorConditionSummary(_ conditions: [[String: Any]]?) -> (status: String, message: String) {
        let conditions = conditions ?? []
        let priority = [
            "Ready",
            "Synced",
            "Healthy",
            "Reconciled",
            "Available",
            "Established",
            "Accepted",
            "Programmed",
            "Reconciling",
            "Stalled"
        ]
        let selected = priority.compactMap { wanted in
            conditions.first { condition in
                (condition["type"] as? String)?.caseInsensitiveCompare(wanted) == .orderedSame
            }
        }.first ?? conditions.last

        let conditionType = selected?["type"] as? String
        let conditionStatus = selected?["status"] as? String
        let reason = selected?["reason"] as? String
        let message = selected?["message"] as? String
        let status = [conditionType, conditionStatus]
            .compactMap { $0?.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .joined(separator: " ")
        return (
            status: status.isEmpty ? (reason ?? "Found") : status,
            message: message ?? reason ?? ""
        )
    }

    private func discoverGenericCustomResources(
        environment: [String: String],
        contextName: String,
        namespace: String,
        alreadyListedPaths: Set<String>,
        crdPrinterColumns: [CRDPrinterColumnKey: [CRDPrinterColumnDefinition]]
    ) async -> [OperatorResourceSummary] {
        guard let groupsRaw = try? await restClient.rawGET(
            environment: environment,
            contextName: contextName,
            apiPath: "/apis",
            timeout: 15
        ) else { return [] }

        let preferredVersions = Self.parsePreferredAPIGroupVersions(groupsRaw)
            .filter { version in
                !version.group.starts(with: "metrics.k8s.io")
            }
            .prefix(48)

        let ns = namespace.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? namespace
        var output: [OperatorResourceSummary] = []
        var listedKinds = Set<String>()

        for version in preferredVersions {
            guard let resourceRaw = try? await restClient.rawGET(
                environment: environment,
                contextName: contextName,
                apiPath: "/apis/\(version.groupVersion)",
                timeout: 15
            ) else { continue }

            let resources = Self.parseNamespacedAPIResources(resourceRaw)
                .filter { resource in
                    !resource.name.contains("/")
                        && !Self.isLikelyBuiltInAPIGroup(version.group)
                        && listedKinds.insert("\(version.groupVersion)/\(resource.name)").inserted
                }
                .prefix(16)

            for resource in resources {
                let path = "/apis/\(version.groupVersion)/namespaces/\(ns)/\(resource.name)"
                guard !alreadyListedPaths.contains(path),
                      let raw = try? await restClient.customCollection(
                        environment: environment,
                        contextName: contextName,
                        apiPath: path,
                        timeout: 12
                      )
                else { continue }
                output += Self.parseOperatorResources(
                    raw,
                    family: "Custom Resources",
                    kind: resource.kind,
                    apiPath: path,
                    printerColumnDefinitions: Self.printerColumnDefinitions(for: path, in: crdPrinterColumns)
                )
                if output.count >= 500 { return output }
            }
        }

        return output
    }

    private struct CRDPrinterColumnKey: Hashable {
        let group: String
        let version: String
        let plural: String
    }

    private struct CRDPrinterColumnDefinition {
        let name: String
        let jsonPath: String
    }

    private func crdPrinterColumnDefinitions(
        environment: [String: String],
        contextName: String
    ) async -> [CRDPrinterColumnKey: [CRDPrinterColumnDefinition]] {
        guard let raw = try? await restClient.rawGET(
            environment: environment,
            contextName: contextName,
            apiPath: "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            timeout: 15
        ) else { return [:] }
        return Self.parseCRDPrinterColumnDefinitions(raw)
    }

    private static func parseCRDPrinterColumnDefinitions(_ raw: String) -> [CRDPrinterColumnKey: [CRDPrinterColumnDefinition]] {
        guard let root = try? JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any],
              let items = root["items"] as? [[String: Any]]
        else { return [:] }

        var output: [CRDPrinterColumnKey: [CRDPrinterColumnDefinition]] = [:]
        for item in items {
            guard let spec = item["spec"] as? [String: Any],
                  let group = spec["group"] as? String,
                  let names = spec["names"] as? [String: Any],
                  let plural = names["plural"] as? String,
                  let versions = spec["versions"] as? [[String: Any]]
            else { continue }

            for version in versions {
                guard let versionName = version["name"] as? String else { continue }
                let columns = parseCRDPrinterColumns(version["additionalPrinterColumns"] as? [[String: Any]])
                guard !columns.isEmpty else { continue }
                output[CRDPrinterColumnKey(group: group, version: versionName, plural: plural)] = columns
            }
        }
        return output
    }

    private static func parseCRDPrinterColumns(_ rawColumns: [[String: Any]]?) -> [CRDPrinterColumnDefinition] {
        (rawColumns ?? []).compactMap { column in
            guard let name = column["name"] as? String,
                  let jsonPath = column["jsonPath"] as? String
            else { return nil }
            let boundedName = boundedPrinterColumnText(name, limit: 24)
            guard !boundedName.isEmpty, jsonPath.hasPrefix(".") else { return nil }
            return CRDPrinterColumnDefinition(name: boundedName, jsonPath: jsonPath)
        }
        .prefix(3)
        .map { $0 }
    }

    private static func printerColumnDefinitions(
        for apiPath: String,
        in definitions: [CRDPrinterColumnKey: [CRDPrinterColumnDefinition]]
    ) -> [CRDPrinterColumnDefinition] {
        guard let key = crdPrinterColumnKey(for: apiPath) else { return [] }
        return definitions[key] ?? []
    }

    private static func crdPrinterColumnKey(for apiPath: String) -> CRDPrinterColumnKey? {
        let parts = apiPath.split(separator: "/").map(String.init)
        guard parts.count >= 4, parts[0] == "apis" else { return nil }
        if parts.count >= 6, parts[3] == "namespaces" {
            return CRDPrinterColumnKey(group: parts[1], version: parts[2], plural: parts[5])
        }
        return CRDPrinterColumnKey(group: parts[1], version: parts[2], plural: parts[3])
    }

    private static func isLikelyBuiltInAPIGroup(_ group: String) -> Bool {
        let builtIns = [
            "apps",
            "autoscaling",
            "batch",
            "coordination.k8s.io",
            "discovery.k8s.io",
            "events.k8s.io",
            "networking.k8s.io",
            "node.k8s.io",
            "policy",
            "rbac.authorization.k8s.io",
            "scheduling.k8s.io",
            "storage.k8s.io"
        ]
        return builtIns.contains(group)
    }

    private static func parsePreferredAPIGroupVersions(_ raw: String) -> [(group: String, groupVersion: String)] {
        guard let root = try? JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any],
              let groups = root["groups"] as? [[String: Any]]
        else { return [] }

        return groups.compactMap { group in
            guard let name = group["name"] as? String,
                  let preferred = group["preferredVersion"] as? [String: Any],
                  let groupVersion = preferred["groupVersion"] as? String
            else { return nil }
            return (name, groupVersion)
        }
    }

    private static func parseNamespacedAPIResources(_ raw: String) -> [(name: String, kind: String)] {
        guard let root = try? JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any],
              let resources = root["resources"] as? [[String: Any]]
        else { return [] }

        return resources.compactMap { resource in
            guard (resource["namespaced"] as? Bool) == true,
                  let name = resource["name"] as? String,
                  let kind = resource["kind"] as? String,
                  !name.isEmpty,
                  !kind.isEmpty
            else { return nil }
            return (name, kind)
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

struct RunningCommandRegistryMetadataSnapshot: Equatable, Sendable {
    let activeHandleCount: Int
    let pendingStartCount: Int
    let stopRequestedStartCount: Int
    let preStartStopIntentCount: Int

    init(
        activeHandleCount: Int,
        pendingStartCount: Int,
        stopRequestedStartCount: Int,
        preStartStopIntentCount: Int = 0
    ) {
        self.activeHandleCount = activeHandleCount
        self.pendingStartCount = pendingStartCount
        self.stopRequestedStartCount = stopRequestedStartCount
        self.preStartStopIntentCount = preStartStopIntentCount
    }
}

enum PortForwardRegistrationResult: Equatable, Sendable {
    case inserted
    case stopped
    case failed(String)
    case stale
}

enum PortForwardFailureDisposition: Sendable {
    case deferred
    case active(any RunningCommandControlling)
    case ignored
}

actor PortForwardRegistry {
    private struct PendingStart {
        let generation: UInt64
        var isStopRequested: Bool
        var failureMessage: String?
    }

    private struct RegisteredHandle {
        let generation: UInt64
        let handle: any RunningCommandControlling
    }

    private var handles: [String: RegisteredHandle] = [:]
    private var pendingStarts: [String: PendingStart] = [:]
    private var nextGeneration: UInt64 = 0

    func beginStart(id: String) -> UInt64 {
        handles.removeValue(forKey: id)?.handle.terminate()
        nextGeneration &+= 1
        let generation = nextGeneration
        pendingStarts[id] = PendingStart(
            generation: generation,
            isStopRequested: false,
            failureMessage: nil
        )
        return generation
    }

    func register(
        handle: any RunningCommandControlling,
        id: String,
        generation: UInt64
    ) -> PortForwardRegistrationResult {
        guard let pending = pendingStarts[id],
              pending.generation == generation else {
            handle.terminate()
            return .stale
        }
        pendingStarts.removeValue(forKey: id)
        if pending.isStopRequested {
            handle.terminate()
            return .stopped
        }
        if let failureMessage = pending.failureMessage {
            handle.terminate()
            return .failed(failureMessage)
        }
        handles.updateValue(
            RegisteredHandle(generation: generation, handle: handle),
            forKey: id
        )?.handle.terminate()
        return .inserted
    }

    func insert(
        handle: any RunningCommandControlling,
        id: String,
        generation: UInt64
    ) -> Bool {
        if case .inserted = register(
            handle: handle,
            id: id,
            generation: generation
        ) {
            return true
        }
        return false
    }

    func recordFailure(
        message: String,
        id: String,
        generation: UInt64
    ) -> PortForwardFailureDisposition {
        if var pending = pendingStarts[id],
           pending.generation == generation {
            guard !pending.isStopRequested, pending.failureMessage == nil else {
                return .ignored
            }
            pending.failureMessage = message
            pendingStarts[id] = pending
            return .deferred
        }
        if let registered = handles[id],
           registered.generation == generation {
            handles.removeValue(forKey: id)
            return .active(registered.handle)
        }
        return .ignored
    }

    func shouldDeliverReady(id: String, generation: UInt64) -> Bool {
        if let pending = pendingStarts[id],
           pending.generation == generation {
            return !pending.isStopRequested && pending.failureMessage == nil
        }
        return handles[id]?.generation == generation
    }

    func isStopRequested(id: String, generation: UInt64) -> Bool {
        guard let pending = pendingStarts[id],
              pending.generation == generation else {
            return false
        }
        return pending.isStopRequested
    }

    @discardableResult
    func finishStart(id: String, generation: UInt64) -> Bool {
        guard let pending = pendingStarts[id],
              pending.generation == generation else {
            return false
        }
        pendingStarts.removeValue(forKey: id)
        return pending.isStopRequested
    }

    func remove(id: String) -> (any RunningCommandControlling)? {
        if var pending = pendingStarts[id] {
            pending.isStopRequested = true
            pendingStarts[id] = pending
        }
        return handles.removeValue(forKey: id)?.handle
    }

    func _testMetadataSnapshot() -> RunningCommandRegistryMetadataSnapshot {
        RunningCommandRegistryMetadataSnapshot(
            activeHandleCount: handles.count,
            pendingStartCount: pendingStarts.count,
            stopRequestedStartCount: pendingStarts.values.lazy.filter(\.isStopRequested).count
        )
    }
}

actor TerminalSessionRegistry {
    private struct PendingStart {
        let generation: UInt64
        var isStopRequested: Bool
    }

    private struct RegisteredHandle {
        let generation: UInt64
        let handle: any RunningCommandControlling
    }

    private struct PreStartStopIntent {
        let expiresAt: Date
        let sequence: UInt64
    }

    private var handles: [String: RegisteredHandle] = [:]
    private var pendingStarts: [String: PendingStart] = [:]
    private var preStartStopIntents: [String: PreStartStopIntent] = [:]
    private var nextGeneration: UInt64 = 0
    private var nextStopIntentSequence: UInt64 = 0
    private let preStartStopIntentCapacity: Int
    private let preStartStopIntentTTL: TimeInterval
    private var stopIntentCleanupTask: Task<Void, Never>?

    init(
        preStartStopIntentCapacity: Int = 128,
        preStartStopIntentTTL: TimeInterval = 1
    ) {
        self.preStartStopIntentCapacity = max(1, preStartStopIntentCapacity)
        self.preStartStopIntentTTL = max(0.01, preStartStopIntentTTL)
    }

    deinit {
        stopIntentCleanupTask?.cancel()
    }

    func beginStart(id: String) -> UInt64 {
        purgeExpiredStopIntents(now: Date())
        let rememberedStop = preStartStopIntents.removeValue(forKey: id) != nil
        scheduleStopIntentCleanup()
        handles.removeValue(forKey: id)?.handle.terminate()
        nextGeneration &+= 1
        let generation = nextGeneration
        pendingStarts[id] = PendingStart(
            generation: generation,
            isStopRequested: rememberedStop
        )
        return generation
    }

    func insert(handle: any RunningCommandControlling, id: String, generation: UInt64) -> Bool {
        guard let pending = pendingStarts[id],
              pending.generation == generation else {
            handle.terminate()
            return false
        }
        pendingStarts.removeValue(forKey: id)
        guard !pending.isStopRequested else {
            handle.terminate()
            return false
        }
        handles.updateValue(
            RegisteredHandle(generation: generation, handle: handle),
            forKey: id
        )?.handle.terminate()
        return true
    }

    func handle(id: String) -> (any RunningCommandControlling)? {
        handles[id]?.handle
    }

    func isStopRequested(id: String, generation: UInt64) -> Bool {
        guard let pending = pendingStarts[id],
              pending.generation == generation else {
            return false
        }
        return pending.isStopRequested
    }

    /// Removes only the exact generation that terminated. A callback from an
    /// older handle can never clear a replacement registered under the same ID.
    /// The return value suppresses a stale callback once a newer generation exists.
    func complete(id: String, generation: UInt64) -> Bool {
        var shouldNotify = false
        if let pending = pendingStarts[id],
           pending.generation == generation {
            pendingStarts.removeValue(forKey: id)
            shouldNotify = !pending.isStopRequested
        }
        if let registered = handles[id],
           registered.generation == generation {
            handles.removeValue(forKey: id)
            shouldNotify = true
        }

        let hasNewerGeneration =
            pendingStarts[id].map { $0.generation != generation } == true
            || handles[id].map { $0.generation != generation } == true
        return shouldNotify && !hasNewerGeneration
    }

    @discardableResult
    func finishStart(id: String, generation: UInt64) -> Bool {
        guard let pending = pendingStarts[id],
              pending.generation == generation else {
            return false
        }
        pendingStarts.removeValue(forKey: id)
        return pending.isStopRequested
    }

    func remove(
        id: String,
        rememberIfNotStarted: Bool = false
    ) -> (any RunningCommandControlling)? {
        purgeExpiredStopIntents(now: Date())
        var foundKnownGeneration = false
        if var pending = pendingStarts[id] {
            pending.isStopRequested = true
            pendingStarts[id] = pending
            foundKnownGeneration = true
        }
        if let registered = handles.removeValue(forKey: id) {
            foundKnownGeneration = true
            return registered.handle
        }
        if rememberIfNotStarted, !foundKnownGeneration {
            rememberPreStartStop(id: id, now: Date())
        }
        return nil
    }

    func _testMetadataSnapshot() -> RunningCommandRegistryMetadataSnapshot {
        purgeExpiredStopIntents(now: Date())
        return RunningCommandRegistryMetadataSnapshot(
            activeHandleCount: handles.count,
            pendingStartCount: pendingStarts.count,
            stopRequestedStartCount: pendingStarts.values.lazy.filter(\.isStopRequested).count,
            preStartStopIntentCount: preStartStopIntents.count
        )
    }

    private func rememberPreStartStop(id: String, now: Date) {
        nextStopIntentSequence &+= 1
        preStartStopIntents[id] = PreStartStopIntent(
            expiresAt: now.addingTimeInterval(preStartStopIntentTTL),
            sequence: nextStopIntentSequence
        )
        if preStartStopIntents.count > preStartStopIntentCapacity,
           let oldestID = preStartStopIntents.min(
               by: { $0.value.sequence < $1.value.sequence }
           )?.key {
            preStartStopIntents.removeValue(forKey: oldestID)
        }
        scheduleStopIntentCleanup()
    }

    private func purgeExpiredStopIntents(now: Date) {
        preStartStopIntents = preStartStopIntents.filter { $0.value.expiresAt > now }
    }

    private func scheduleStopIntentCleanup() {
        stopIntentCleanupTask?.cancel()
        guard let nextExpiration = preStartStopIntents.values
            .map(\.expiresAt)
            .min() else {
            stopIntentCleanupTask = nil
            return
        }
        let delay = max(0, nextExpiration.timeIntervalSinceNow)
        stopIntentCleanupTask = Task { [weak self] in
            do {
                try await Task.sleep(for: .seconds(delay))
            } catch {
                return
            }
            await self?.expireStopIntentsAndReschedule()
        }
    }

    private func expireStopIntentsAndReschedule() {
        stopIntentCleanupTask = nil
        purgeExpiredStopIntents(now: Date())
        scheduleStopIntentCleanup()
    }
}
