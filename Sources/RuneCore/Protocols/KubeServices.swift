import Foundation

public protocol ContextListingService: Sendable {
    func listContexts(from sources: [KubeConfigSource]) async throws -> [KubeContext]
}

public protocol NamespaceListingService: Sendable {
    func listNamespaces(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [String]
}

public protocol PodListingService: Sendable {
    func listPods(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [PodSummary]
}

public protocol DeploymentListingService: Sendable {
    func listDeployments(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [DeploymentSummary]
}

public protocol ServiceListingService: Sendable {
    func listServices(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ServiceSummary]
}

public protocol EventListingService: Sendable {
    func listEvents(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [EventSummary]
}

public protocol GenericResourceListingService: Sendable {
    func listStatefulSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listDaemonSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listJobs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listCronJobs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listReplicaSets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listPersistentVolumeClaims(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listPersistentVolumes(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary]

    func listStorageClasses(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary]

    func listHorizontalPodAutoscalers(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listNetworkPolicies(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listIngresses(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listConfigMaps(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listSecrets(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String
    ) async throws -> [ClusterResourceSummary]

    func listNodes(
        from sources: [KubeConfigSource],
        context: KubeContext
    ) async throws -> [ClusterResourceSummary]
}

public protocol PodLogService: Sendable {
    func podLogs(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> String
}

public protocol UnifiedServiceLogService: Sendable {
    func unifiedLogsForService(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        service: ServiceSummary,
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> UnifiedServiceLogs
}

public protocol UnifiedDeploymentLogService: Sendable {
    func unifiedLogsForDeployment(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deployment: DeploymentSummary,
        filter: LogTimeFilter,
        previous: Bool
    ) async throws -> UnifiedDeploymentLogs
}

public protocol ManifestService: Sendable {
    func resourceYAML(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws -> String

    func resourceDescribe(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws -> String
}

public protocol ManifestValidationService: Sendable {
    func validateResourceYAML(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        yaml: String
    ) async throws -> [YAMLValidationIssue]
}

public protocol ResourceWriteService: Sendable {
    func deleteResource(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws

    func scaleDeployment(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String,
        replicas: Int
    ) async throws

    func restartDeploymentRollout(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        deploymentName: String
    ) async throws

    func applyYAML(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        yaml: String
    ) async throws

    func patchCronJobSuspend(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        name: String,
        suspend: Bool
    ) async throws

    func createJobFromCronJob(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        cronJobName: String,
        jobName: String
    ) async throws
}

public protocol HelmReleaseService: Sendable {
    func listReleases(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String?,
        allNamespaces: Bool
    ) async throws -> [HelmReleaseSummary]

    func releaseValues(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> String

    func releaseManifest(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> String

    func releaseHistory(
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        releaseName: String
    ) async throws -> [HelmReleaseRevision]
}
