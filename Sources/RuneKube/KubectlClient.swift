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
        self.k8sAgentPath = k8sAgentPath
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
        let pods: [PodSummary]
        do {
            // Fast path: lightweight table; IP/node/QoS/ready come from `enrichPodsWithJSONList` (background).
            let list = try await runKubectl(
                arguments: builder.podListTextArguments(context: context.name, namespace: namespace),
                environment: env
            )
            pods = parser.parsePodsTable(namespace: namespace, from: list.stdout)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.podListArguments(context: context.name, namespace: namespace),
                environment: env
            )
            let trimmed = fallback.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
            pods = try parser.parsePodsListJSON(namespace: namespace, from: trimmed)
        }

        var merged = pods
        if let top = await runKubectlAllowFailure(
            arguments: builder.podTopArguments(context: context.name, namespace: namespace),
            environment: env,
            timeout: 6
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
        let env = try kubeconfigEnvironment(from: sources)
        let list = try await runKubectl(
            arguments: builder.podListArguments(context: context.name, namespace: namespace),
            environment: env
        )
        let trimmed = list.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        let detailed = try parser.parsePodsListJSON(namespace: namespace, from: trimmed)
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
        let pods: [PodSummary]
        do {
            let list = try await runKubectl(
                arguments: builder.podListAllNamespacesTextArguments(context: context.name),
                environment: env
            )
            pods = parser.parsePodsAllNamespacesTable(from: list.stdout)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.podListAllNamespacesArguments(context: context.name),
                environment: env
            )
            let trimmed = fallback.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
            pods = try parser.parsePodsListJSONAllNamespaces(from: trimmed)
        }

        var merged = pods
        if let top = await runKubectlAllowFailure(
            arguments: builder.podTopAllNamespacesArguments(context: context.name),
            environment: env,
            timeout: 6
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
                let rows = try await RuneK8sAgentWorkloadClient.listDeployments(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
                if !rows.isEmpty {
                    return rows
                }
            } catch {
                // Go agent (client-go) failed; continue with kubectl.
            }
        }
        do {
            let result = try await runKubectl(
                arguments: builder.deploymentListTextArguments(context: context.name, namespace: namespace),
                environment: env
            )
            return parser.parseDeploymentsTable(namespace: namespace, from: result.stdout)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.deploymentListArguments(context: context.name, namespace: namespace),
                environment: env
            )
            return try parser.parseDeployments(namespace: namespace, from: fallback.stdout)
        }
    }

    public func listDeploymentsAllNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [DeploymentSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        do {
            let result = try await runKubectl(
                arguments: builder.deploymentListAllNamespacesTextArguments(context: context.name),
                environment: env
            )
            return parser.parseDeploymentsAllNamespacesTable(from: result.stdout)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.deploymentListAllNamespacesArguments(context: context.name),
                environment: env
            )
            return try parser.parseDeployments(namespace: "", from: fallback.stdout)
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
        if let agent = resolvedK8sAgentPath() {
            do {
                let rows = try await RuneK8sAgentWorkloadClient.listStatefulSets(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
                if !rows.isEmpty {
                    return rows
                }
            } catch {
                // Go agent (client-go) failed; continue with kubectl.
            }
        }
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
        if let agent = resolvedK8sAgentPath() {
            do {
                let rows = try await RuneK8sAgentWorkloadClient.listDaemonSets(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
                if !rows.isEmpty {
                    return rows
                }
            } catch {
                // Go agent (client-go) failed; continue with kubectl.
            }
        }
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

    public func listJobs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        if let agent = resolvedK8sAgentPath() {
            do {
                let rows = try await RuneK8sAgentWorkloadClient.listJobs(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
                if !rows.isEmpty {
                    return rows
                }
            } catch {
                // Go agent (client-go) failed; continue with kubectl.
            }
            // Agent returned [] or failed: still use kubectl so we do not hide resources (API/version quirks).
        }
        // Layered like `listPods`: small custom-columns table first; JSON fallback for edge cases / older kubectl quirks.
        do {
            let table = try await runKubectl(
                arguments: builder.jobListTextArguments(context: context.name, namespace: namespace),
                environment: env,
                timeout: slowNamespacedJSONListTimeout
            )
            return parser.parseJobsTable(namespace: namespace, from: table.stdout)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.jobListArguments(context: context.name, namespace: namespace),
                environment: env,
                timeout: slowNamespacedJSONListTimeout
            )
            do {
                return try parser.parseJobs(namespace: namespace, from: fallback.stdout)
            } catch {
                throw RuneError.parseError(message: "jobs kunde inte tolkas")
            }
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
                let rows = try await RuneK8sAgentWorkloadClient.listCronJobs(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
                if !rows.isEmpty {
                    return rows
                }
            } catch {
                // Go agent (client-go) failed; continue with kubectl.
            }
            // Agent returned [] or failed: still use kubectl so we do not hide resources (API/version quirks).
        }
        do {
            let table = try await runKubectl(
                arguments: builder.cronJobListTextArguments(context: context.name, namespace: namespace),
                environment: env,
                timeout: slowNamespacedJSONListTimeout
            )
            return parser.parseCronJobsTable(namespace: namespace, from: table.stdout)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.cronJobListArguments(context: context.name, namespace: namespace),
                environment: env,
                timeout: slowNamespacedJSONListTimeout
            )
            do {
                return try parser.parseCronJobs(namespace: namespace, from: fallback.stdout)
            } catch {
                throw RuneError.parseError(message: "cronjobs kunde inte tolkas")
            }
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
                let rows = try await RuneK8sAgentWorkloadClient.listReplicaSets(
                    executablePath: agent,
                    runner: runner,
                    environment: env,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: slowNamespacedJSONListTimeout
                )
                if !rows.isEmpty {
                    return rows
                }
            } catch {
                // Go agent (client-go) failed; continue with kubectl.
            }
        }
        let result = try await runKubectl(
            arguments: builder.replicaSetListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseReplicaSets(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "replicasets JSON kunde inte tolkas")
        }
    }

    public func listPersistentVolumeClaims(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.persistentVolumeClaimListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parsePersistentVolumeClaims(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "PVC JSON kunde inte tolkas")
        }
    }

    public func listPersistentVolumes(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.persistentVolumeListArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parsePersistentVolumes(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "PV JSON kunde inte tolkas")
        }
    }

    public func listStorageClasses(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.storageClassListArguments(context: context.name),
            environment: env
        )

        do {
            return try parser.parseStorageClasses(from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "StorageClass JSON kunde inte tolkas")
        }
    }

    public func listHorizontalPodAutoscalers(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.horizontalPodAutoscalerListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseHorizontalPodAutoscalers(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "HPA JSON kunde inte tolkas")
        }
    }

    public func listNetworkPolicies(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.networkPolicyListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseNetworkPolicies(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "NetworkPolicy JSON kunde inte tolkas")
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

    public func listRoles(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.roleListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseRoles(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "roles JSON kunde inte tolkas")
        }
    }

    public func listRoleBindings(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
        let result = try await runKubectl(
            arguments: builder.roleBindingListArguments(context: context.name, namespace: namespace),
            environment: env
        )

        do {
            return try parser.parseRoleBindings(namespace: namespace, from: result.stdout)
        } catch {
            throw RuneError.parseError(message: "rolebindings JSON kunde inte tolkas")
        }
    }

    public func listClusterRoles(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary] {
        let env = try kubeconfigEnvironment(from: sources)
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

    /// Prefer Go helper (client-go) counts where supported; return `nil` for unsupported resources or uncertain zero-row results.
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
            case "deployments", "deployment":
                let rows = try await RuneK8sAgentWorkloadClient.listDeployments(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.isEmpty ? nil : rows.count
            case "statefulsets", "statefulset":
                let rows = try await RuneK8sAgentWorkloadClient.listStatefulSets(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.isEmpty ? nil : rows.count
            case "daemonsets", "daemonset":
                let rows = try await RuneK8sAgentWorkloadClient.listDaemonSets(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.isEmpty ? nil : rows.count
            case "jobs", "job":
                let rows = try await RuneK8sAgentWorkloadClient.listJobs(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.isEmpty ? nil : rows.count
            case "cronjobs", "cronjob":
                let rows = try await RuneK8sAgentWorkloadClient.listCronJobs(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.isEmpty ? nil : rows.count
            case "replicasets", "replicaset":
                let rows = try await RuneK8sAgentWorkloadClient.listReplicaSets(
                    executablePath: agent,
                    runner: runner,
                    environment: environment,
                    contextName: context.name,
                    namespace: namespace,
                    timeout: timeout
                )
                return rows.isEmpty ? nil : rows.count
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
            let result: CommandResult
            do {
                result = try await runKubectl(
                    arguments: builder.rawGetArguments(context: context.name, request: request),
                    environment: environment,
                    timeout: 45
                )
            } catch {
                return nil
            }

            guard let page = KubectlListJSON.collectionPageInfo(from: result.stdout) else {
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
            let result = try await runKubectl(
                arguments: builder.rawGetArguments(context: context.name, apiPath: apiPath),
                environment: environment,
                timeout: 45
            )
            return KubectlListJSON.collectionListTotal(from: result.stdout)
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

        guard let result = await runKubectlAllowFailure(
            arguments: builder.nodeTopArguments(context: context.name),
            environment: env,
            timeout: 6
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

        // Fail fast: short first attempt surfaces wedged API servers quickly; second attempt uses the full budget.
        let budget = logFetchTimeout(for: filter)
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

        let podsTrimmed = podResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        let pods: [PodSummary]
        do {
            pods = try parser.parsePodsListJSON(namespace: namespace, from: podsTrimmed)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.podsByLabelSelectorTextArguments(context: context.name, namespace: namespace, selector: selector),
                environment: env
            )
            pods = parser.parsePods(namespace: namespace, from: fallback.stdout)
        }
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

        let podsTrimmed = podResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        let pods: [PodSummary]
        do {
            pods = try parser.parsePodsListJSON(namespace: namespace, from: podsTrimmed)
        } catch {
            let fallback = try await runKubectl(
                arguments: builder.podsByLabelSelectorTextArguments(context: context.name, namespace: namespace, selector: selector),
                environment: env
            )
            pods = parser.parsePods(namespace: namespace, from: fallback.stdout)
        }
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

    public func patchCronJobSuspend(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        name: String,
        suspend: Bool
    ) async throws {
        let env = try kubeconfigEnvironment(from: sources)
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
