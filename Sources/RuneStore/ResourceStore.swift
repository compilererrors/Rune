import Foundation
import RuneCore

/// In-process cache of the last fetched resource lists per `KubeContext` and namespace.
///
/// RAM only; keys `"\(context.name)::\(namespace)"`. Repopulates `RuneAppState` when revisiting the same pair without refetch. LRU eviction per context via `maxSnapshotsPerContext`. Cluster-scoped lists (`namespaces`, `nodes`, `persistentVolumes`, `storageClasses`) key on `context.name` only.
///
/// Heavier list payloads than `JSONOverviewSnapshotCacheStore` / `overviewSnapshotCache` (see `RuneAppViewModel`).
@MainActor
public final class ResourceStore {
    public struct NamespacedSnapshot: Sendable {
        public var pods: [PodSummary]
        public var deployments: [DeploymentSummary]
        public var statefulSets: [ClusterResourceSummary]
        public var daemonSets: [ClusterResourceSummary]
        public var jobs: [ClusterResourceSummary]
        public var cronJobs: [ClusterResourceSummary]
        public var replicaSets: [ClusterResourceSummary]
        public var persistentVolumeClaims: [ClusterResourceSummary]
        public var horizontalPodAutoscalers: [ClusterResourceSummary]
        public var networkPolicies: [ClusterResourceSummary]
        public var services: [ServiceSummary]
        public var ingresses: [ClusterResourceSummary]
        public var configMaps: [ClusterResourceSummary]
        public var secrets: [ClusterResourceSummary]
        public var events: [EventSummary]

        public static let empty = NamespacedSnapshot(
            pods: [],
            deployments: [],
            statefulSets: [],
            daemonSets: [],
            jobs: [],
            cronJobs: [],
            replicaSets: [],
            persistentVolumeClaims: [],
            horizontalPodAutoscalers: [],
            networkPolicies: [],
            services: [],
            ingresses: [],
            configMaps: [],
            secrets: [],
            events: []
        )
    }

    private var snapshotsByContextAndNamespace: [String: NamespacedSnapshot] = [:]
    private var snapshotKeysByContext: [String: [String]] = [:]
    private var namespacesByContext: [String: [String]] = [:]
    private var nodesByContext: [String: [ClusterResourceSummary]] = [:]
    private var persistentVolumesByContext: [String: [ClusterResourceSummary]] = [:]
    private var storageClassesByContext: [String: [ClusterResourceSummary]] = [:]
    private let maxSnapshotsPerContext = 32

    public init() {}

    public func cacheSnapshot(
        context: KubeContext,
        namespace: String,
        pods: [PodSummary],
        deployments: [DeploymentSummary],
        statefulSets: [ClusterResourceSummary],
        daemonSets: [ClusterResourceSummary],
        jobs: [ClusterResourceSummary],
        cronJobs: [ClusterResourceSummary],
        replicaSets: [ClusterResourceSummary],
        persistentVolumeClaims: [ClusterResourceSummary],
        horizontalPodAutoscalers: [ClusterResourceSummary],
        networkPolicies: [ClusterResourceSummary],
        services: [ServiceSummary],
        ingresses: [ClusterResourceSummary],
        configMaps: [ClusterResourceSummary],
        secrets: [ClusterResourceSummary],
        events: [EventSummary]
    ) {
        let cacheKey = key(context: context, namespace: namespace)
        snapshotsByContextAndNamespace[cacheKey] = NamespacedSnapshot(
            pods: pods,
            deployments: deployments,
            statefulSets: statefulSets,
            daemonSets: daemonSets,
            jobs: jobs,
            cronJobs: cronJobs,
            replicaSets: replicaSets,
            persistentVolumeClaims: persistentVolumeClaims,
            horizontalPodAutoscalers: horizontalPodAutoscalers,
            networkPolicies: networkPolicies,
            services: services,
            ingresses: ingresses,
            configMaps: configMaps,
            secrets: secrets,
            events: events
        )
        touchSnapshotKey(cacheKey, contextName: context.name)
        pruneSnapshotsIfNeeded(contextName: context.name)
    }

    public func snapshot(context: KubeContext, namespace: String) -> NamespacedSnapshot {
        let cacheKey = key(context: context, namespace: namespace)
        if snapshotsByContextAndNamespace[cacheKey] != nil {
            touchSnapshotKey(cacheKey, contextName: context.name)
        }
        return snapshotsByContextAndNamespace[cacheKey] ?? .empty
    }

    public func cacheNamespaces(_ namespaces: [String], context: KubeContext) {
        namespacesByContext[contextKey(context)] = namespaces
    }

    public func namespaces(context: KubeContext) -> [String] {
        namespacesByContext[contextKey(context)] ?? []
    }

    public func cacheNodes(_ nodes: [ClusterResourceSummary], context: KubeContext) {
        nodesByContext[contextKey(context)] = nodes
    }

    public func nodes(context: KubeContext) -> [ClusterResourceSummary] {
        nodesByContext[contextKey(context)] ?? []
    }

    public func cachePersistentVolumes(_ volumes: [ClusterResourceSummary], context: KubeContext) {
        persistentVolumesByContext[contextKey(context)] = volumes
    }

    public func persistentVolumes(context: KubeContext) -> [ClusterResourceSummary] {
        persistentVolumesByContext[contextKey(context)] ?? []
    }

    public func cacheStorageClasses(_ classes: [ClusterResourceSummary], context: KubeContext) {
        storageClassesByContext[contextKey(context)] = classes
    }

    public func storageClasses(context: KubeContext) -> [ClusterResourceSummary] {
        storageClassesByContext[contextKey(context)] ?? []
    }

    public func clearContext(_ context: KubeContext) {
        let keys = snapshotKeysByContext[context.name] ?? []
        for key in keys {
            snapshotsByContextAndNamespace.removeValue(forKey: key)
        }
        snapshotKeysByContext.removeValue(forKey: context.name)

        namespacesByContext.removeValue(forKey: context.name)
        nodesByContext.removeValue(forKey: context.name)
        persistentVolumesByContext.removeValue(forKey: context.name)
        storageClassesByContext.removeValue(forKey: context.name)
    }

    private func touchSnapshotKey(_ key: String, contextName: String) {
        var keys = snapshotKeysByContext[contextName] ?? []
        keys.removeAll(where: { $0 == key })
        keys.insert(key, at: 0)
        snapshotKeysByContext[contextName] = keys
    }

    private func pruneSnapshotsIfNeeded(contextName: String) {
        guard var keys = snapshotKeysByContext[contextName], keys.count > maxSnapshotsPerContext else { return }

        let overflow = keys.dropFirst(maxSnapshotsPerContext)
        for key in overflow {
            snapshotsByContextAndNamespace.removeValue(forKey: key)
        }
        keys = Array(keys.prefix(maxSnapshotsPerContext))
        snapshotKeysByContext[contextName] = keys
    }

    private func contextKey(_ context: KubeContext) -> String {
        context.name
    }

    private func key(context: KubeContext, namespace: String) -> String {
        "\(context.name)::\(namespace)"
    }
}
