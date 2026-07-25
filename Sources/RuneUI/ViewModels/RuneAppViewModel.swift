import AppKit
import Combine
import CryptoKit
import Darwin
import Foundation
import RuneCore
import UniformTypeIdentifiers
import RuneDiagnostics
import RuneExport
import RuneKube
import RuneSecurity
import RuneStore

public protocol PortForwardBrowserOpening {
    @MainActor
    func open(_ url: URL)
}

struct PortForwardStartScope: Equatable, Sendable {
    let kubeConfigSources: [KubeConfigSource]
    let context: KubeContext
    let namespace: String
}

private struct KubeConfigSourceFingerprint: Equatable {
    private struct Entry: Equatable {
        let path: String
        let exists: Bool
        let fileSize: Int64
        let modifiedAt: TimeInterval
    }

    private let entries: [Entry]

    init(sources: [KubeConfigSource]) {
        entries = sources
            .map { source in
                let url = source.url.standardizedFileURL
                let values = try? url.resourceValues(forKeys: [.contentModificationDateKey, .fileSizeKey])
                return Entry(
                    path: url.path,
                    exists: FileManager.default.fileExists(atPath: url.path),
                    fileSize: Int64(values?.fileSize ?? -1),
                    modifiedAt: values?.contentModificationDate?.timeIntervalSince1970 ?? -1
                )
            }
            .sorted { $0.path < $1.path }
    }
}

private struct KubernetesRequestMetricsSelection: Equatable {
    let sources: [KubeConfigSource]
    let context: KubeContext
    let sourceFingerprint: KubeConfigSourceFingerprint

    init(sources: [KubeConfigSource], context: KubeContext) {
        self.sources = sources
        self.context = context
        sourceFingerprint = KubeConfigSourceFingerprint(sources: sources)
    }
}

private struct KubeConfigImportRegistrySnapshot: Equatable {
    struct SourceEntry: Equatable {
        let path: String
        let contentDigest: Data
    }

    let names: KubeConfigNameRegistry
    let sources: [SourceEntry]
}

private enum PendingNativeCloudCredential: Sendable {
    case aks(clientSecret: String)
    case eks(credentials: AWSEKSCredentials)
    case gke(serviceAccountJSON: Data)

    var provider: KubernetesNativeAuthProviderKind {
        switch self {
        case .aks: return .azureKubelogin
        case .eks: return .awsEKS
        case .gke: return .googleGKE
        }
    }
}

private struct NativeCloudImportPresentationError: LocalizedError, Sendable {
    let message: String

    var errorDescription: String? { message }
}

public struct WorkspacePortForwardBrowserOpener: PortForwardBrowserOpening {
    public init() {}

    @MainActor
    public func open(_ url: URL) {
        NSWorkspace.shared.open(url)
    }
}

// Cluster data caching: `ResourceStore` holds full lists per (context, namespace) in RAM; `overviewSnapshotCache`
// holds lightweight overview rows with TTL; `overviewSnapshotPersistence` writes the same shape to disk
// (Application Support) for cold start and background prefetch. `namespaceListPersistence` stores the last
// namespace menu per context under `…/Rune/namespace-lists/` and hydrates before `listNamespaces` when RAM is empty.

public enum PodLogPreset: String, CaseIterable, Identifiable, Sendable {
    /// Default log preset in Rune: tail only (no time window), keeping responses small and avoiding huge transfers on busy pods.
    case recentLines
    case last5Minutes
    case last15Minutes
    case lastHour
    case last6Hours
    case last24Hours
    case last7Days
    /// Equivalent to plain Kubernetes pod logs: no tail or time filter.
    case largeTail
    case customOne
    case customTwo

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .recentLines: return "Recent (200 lines)"
        case .last5Minutes: return "Last 5m"
        case .last15Minutes: return "Last 15m"
        case .lastHour: return "Last 1h"
        case .last6Hours: return "Last 6h"
        case .last24Hours: return "Last 24h"
        case .last7Days: return "Last 7d"
        case .largeTail: return "All logs"
        case .customOne:
            return UserDefaults.standard.runeCustomLogPresetConfig(slot: .one).title(slot: .one)
        case .customTwo:
            return UserDefaults.standard.runeCustomLogPresetConfig(slot: .two).title(slot: .two)
        }
    }

    public var filter: LogTimeFilter {
        switch self {
        case .recentLines: return .tailLines(200)
        case .last5Minutes: return .lastMinutes(5)
        case .last15Minutes: return .lastMinutes(15)
        case .lastHour: return .lastHours(1)
        case .last6Hours: return .lastHours(6)
        case .last24Hours: return .lastHours(24)
        case .last7Days: return .lastDays(7)
        case .largeTail: return .all
        case .customOne:
            return UserDefaults.standard.runeCustomLogPresetConfig(slot: .one).filter
        case .customTwo:
            return UserDefaults.standard.runeCustomLogPresetConfig(slot: .two).filter
        }
    }
}

public enum PendingWriteAction: Equatable, Sendable {
    public struct BulkDeleteResource: Equatable, Sendable {
        public let kind: KubeResourceKind
        public let name: String
        public let namespace: String
    }

    case delete(kind: KubeResourceKind, name: String)
    case deleteMany([BulkDeleteResource])
    case apply(kind: KubeResourceKind, name: String, yaml: String, baseline: String)
    case scale(deploymentName: String, replicas: Int)
    case scaleStatefulSet(name: String, replicas: Int)
    case rolloutRestart(deploymentName: String)
    case rolloutRestartStatefulSet(name: String)
    case rolloutUndo(deploymentName: String, revision: Int?)
    case controllerRolloutUndo(kind: KubeResourceKind, name: String, revision: Int?)
    case helmRollback(releaseName: String, namespace: String, revision: Int, wait: Bool, timeout: String, cleanupOnFail: Bool)
    case exec(podName: String, command: [String])
    case createJobFromCronJob(cronJobName: String, jobName: String)

    var title: String {
        switch self {
        case let .delete(kind, _):
            return "Do you want to delete this \(kind.singularTypeName)?"
        case let .deleteMany(resources):
            return "Do you want to delete \(resources.count) resources?"
        case let .apply(kind, name, _, _):
            return "Apply YAML for \(kind.kubernetesResourceName) \(name)?"
        case let .scale(deploymentName, replicas):
            return "Scale deployment \(deploymentName) to \(replicas)?"
        case let .scaleStatefulSet(name, replicas):
            return "Scale statefulset \(name) to \(replicas)?"
        case let .rolloutRestart(deploymentName):
            return "Restart rollout for deployment \(deploymentName)?"
        case let .rolloutRestartStatefulSet(name):
            return "Restart rollout for statefulset \(name)?"
        case let .rolloutUndo(deploymentName, revision):
            if let revision {
                return "Rollback deployment \(deploymentName) to revision \(revision)?"
            }
            return "Rollback deployment \(deploymentName) to previous revision?"
        case let .controllerRolloutUndo(kind, name, revision):
            if let revision {
                return "Copy rollback command for \(kind.kubernetesResourceName) \(name) to revision \(revision)?"
            }
            return "Copy rollback command for \(kind.kubernetesResourceName) \(name) to previous revision?"
        case let .helmRollback(releaseName, namespace, revision, _, _, _):
            return "Rollback Helm release \(namespace)/\(releaseName) to revision \(revision)?"
        case let .exec(podName, command):
            return "Run command in pod \(podName)? (\(command.joined(separator: " ")))"
        case let .createJobFromCronJob(cronJobName, _):
            return "Create a Job from CronJob \(cronJobName)?"
        }
    }

    var message: String {
        switch self {
        case let .delete(_, name):
            return "“\(name)” will be removed from the cluster. This cannot be undone."
        case let .deleteMany(resources):
            let scope = Set(resources.map { "\($0.namespace)/\($0.kind.kubernetesResourceName)" }).sorted().joined(separator: ", ")
            let visibleTargets = resources.prefix(8).map {
                "\($0.kind.kubernetesResourceName)/\($0.name)"
            }
            let hiddenTargetCount = max(0, resources.count - visibleTargets.count)
            let targetSummary = visibleTargets.joined(separator: ", ")
                + (hiddenTargetCount > 0 ? ", and \(hiddenTargetCount) more" : "")
            return "\(resources.count) selected resources will be removed from \(scope). This cannot be undone.\n\nSelected: \(targetSummary)"
        case let .apply(_, _, yaml, baseline):
            let diff = Self.diffPreview(from: baseline, to: yaml)
            guard !diff.isEmpty else {
                return "This applies the current YAML to the active namespace/context."
            }
            return "This applies the current YAML to the active namespace/context.\n\nYAML diff preview:\n\(diff)"
        case .scale, .scaleStatefulSet:
            return "Replica count will be changed immediately."
        case .rolloutRestart:
            return "Pods in the deployment will be recreated according to rollout strategy."
        case .rolloutRestartStatefulSet:
            return "Pods in the statefulset will be recreated according to rollout strategy."
        case .rolloutUndo:
            return "Deployment rollout will be reverted to an earlier revision."
        case .controllerRolloutUndo:
            return "Rollout rollback command preview only. Rune will not run this controller rollback automatically yet."
        case let .helmRollback(_, _, revision, wait, timeout, cleanupOnFail):
            let waitText = wait ? "wait for Kubernetes resources" : "not wait for Kubernetes resources"
            let cleanupText = cleanupOnFail ? "cleanup-on-fail enabled" : "cleanup-on-fail disabled"
            return "Rune will run Helm rollback after confirmation. If Helm dry-run safety is enabled, Rune runs `helm rollback --dry-run` before the real rollback.\n\nTarget revision: \(revision)\nOptions: \(waitText), timeout \(timeout), \(cleanupText)."
        case .exec:
            return "This runs a command inside the selected pod."
        case let .createJobFromCronJob(_, jobName):
            return "This creates Job \(jobName) from the selected CronJob template."
        }
    }

    var confirmLabel: String {
        switch self {
        case .delete: return "Delete"
        case let .deleteMany(resources): return "Delete \(resources.count)"
        case .apply: return "Apply"
        case .scale, .scaleStatefulSet: return "Scale"
        case .rolloutRestart, .rolloutRestartStatefulSet: return "Restart"
        case .rolloutUndo: return "Rollback"
        case .controllerRolloutUndo: return "Copy Command"
        case .helmRollback: return "Rollback"
        case .exec: return "Run"
        case .createJobFromCronJob: return "Create Job"
        }
    }

    var isDestructive: Bool {
        switch self {
        case .delete, .deleteMany, .rolloutUndo, .controllerRolloutUndo, .helmRollback: return true
        case .apply, .scale, .scaleStatefulSet, .rolloutRestart, .rolloutRestartStatefulSet, .exec, .createJobFromCronJob: return false
        }
    }

    func kubectlCommand(contextName: String, namespace: String) -> String {
        let base = ["kubectl", "--context", contextName]
        let namespaced = namespace.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? base
            : base + ["--namespace", namespace]

        switch self {
        case let .delete(kind, name):
            return Self.shellCommand(namespaced + ["delete", kind.kubernetesResourceName, name])
        case let .deleteMany(resources):
            return resources.map { resource in
                Self.shellCommand(base + [
                    "--namespace",
                    resource.namespace,
                    "delete",
                    resource.kind.kubernetesResourceName,
                    resource.name
                ])
            }.joined(separator: "\n")
        case let .apply(kind, name, _, _):
            return Self.shellCommand(namespaced + [
                "apply",
                "--server-side",
                "--field-manager=rune",
                "--force-conflicts",
                "--dry-run=server",
                "-f",
                "\(kind.kubernetesResourceName)-\(name).yaml"
            ])
        case let .scale(deploymentName, replicas):
            return Self.shellCommand(namespaced + ["scale", "deployment", deploymentName, "--replicas", "\(replicas)"])
        case let .scaleStatefulSet(name, replicas):
            return Self.shellCommand(namespaced + ["scale", "statefulset", name, "--replicas", "\(replicas)"])
        case let .rolloutRestart(deploymentName):
            return Self.shellCommand(namespaced + ["rollout", "restart", "deployment", deploymentName])
        case let .rolloutRestartStatefulSet(name):
            return Self.shellCommand(namespaced + ["rollout", "restart", "statefulset", name])
        case let .rolloutUndo(deploymentName, revision):
            var parts = namespaced + ["rollout", "undo", "deployment", deploymentName]
            if let revision {
                parts.append("--to-revision=\(revision)")
            }
            return Self.shellCommand(parts)
        case let .controllerRolloutUndo(kind, name, revision):
            var parts = namespaced + ["rollout", "undo", kind.kubernetesResourceName, name]
            if let revision {
                parts.append("--to-revision=\(revision)")
            }
            return Self.shellCommand(parts)
        case let .helmRollback(releaseName, namespace, revision, wait, timeout, cleanupOnFail):
            var parts = ["helm", "--kube-context", contextName, "--namespace", namespace, "rollback", releaseName, "\(revision)"]
            if wait {
                parts.append("--wait")
            }
            if !timeout.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                parts.append("--timeout")
                parts.append(timeout)
            }
            if cleanupOnFail {
                parts.append("--cleanup-on-fail")
            }
            return Self.shellCommand(parts)
        case let .exec(podName, command):
            return Self.shellCommand(namespaced + ["exec", podName, "--"] + command)
        case let .createJobFromCronJob(cronJobName, jobName):
            return Self.shellCommand(namespaced + ["create", "job", jobName, "--from=cronjob/\(cronJobName)"])
        }
    }

    private static func diffPreview(from baseline: String, to edited: String) -> String {
        let oldLines = baseline.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
        let newLines = edited.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
        let maxLines = max(oldLines.count, newLines.count)
        var output: [String] = []

        for index in 0..<maxLines {
            let oldLine = index < oldLines.count ? oldLines[index] : nil
            let newLine = index < newLines.count ? newLines[index] : nil
            guard oldLine != newLine else { continue }
            if let oldLine {
                output.append("- \(oldLine)")
            }
            if let newLine {
                output.append("+ \(newLine)")
            }
            if output.count >= 24 {
                output.append("… diff truncated")
                break
            }
        }

        return output.joined(separator: "\n")
    }

    private static func shellCommand(_ parts: [String]) -> String {
        ShellCommandFormatting.shellCommand(parts)
    }
}

public struct CommandPaletteItem: Identifiable {
    public enum Action {
        case section(RuneSection)
        case context(KubeContext)
        case namespace(String)
        case importKubeConfig
        case reload
        case readOnly(Bool)
        case saveLogs
        case deleteSelectedResource
        case savedWorkspace(SavedWorkspaceSnapshot)
        case saveWorkspace(String)
        case toggleSavedWorkspaceFavorite(SavedWorkspaceSnapshot)
        case pod(PodSummary)
        case deployment(DeploymentSummary)
        case service(ServiceSummary)
        case event(EventSummary)
        case helmRelease(HelmReleaseSummary)
        case resourceKind(section: RuneSection, kind: KubeResourceKind)
        case clusterResource(ClusterResourceSummary)
    }

    public let id: String
    public let title: String
    public let subtitle: String
    public let symbolName: String
    public let action: Action
}

private extension CommandPaletteItem.Action {
    var recordsCompositeNavigationCheckpoint: Bool {
        switch self {
        case .pod, .deployment, .service, .event, .helmRelease, .resourceKind, .clusterResource:
            return true
        case .section, .context, .namespace, .importKubeConfig, .reload, .readOnly, .saveLogs, .deleteSelectedResource, .savedWorkspace, .saveWorkspace, .toggleSavedWorkspaceFavorite:
            return false
        }
    }
}

public enum OverviewModule: Sendable {
    case pods
    case deployments
    case services
    case ingresses
    case configMaps
    case cronJobs
    case nodes
    case events
}

public enum PodListSortColumn: String, Sendable {
    case name
    case cpu
    case memory
    case restarts
    case age
    case status
}

public enum DeploymentListSortColumn: String, Sendable {
    case name
    case replicas
}

public enum ServiceListSortColumn: String, Sendable {
    case name
    case type
    case clusterIP
}

public enum GenericResourceListSortColumn: String, Sendable {
    case name
    case primary
    case secondary
    case namespace
}

public enum HelmReleaseListSortColumn: String, Sendable {
    case name
    case status
    case namespace
    case revision
    case chart
    case appVersion
}

public enum EventListSortColumn: String, Sendable {
    case reason
    case type
    case object
    case namespace
    case lastSeen
}

public enum OperatorResourceListSortColumn: String, Sendable {
    case family
    case kind
    case name
    case namespace
    case status
    case apiPath
}

public enum OperatorResourceFocus: String, CaseIterable, Identifiable, Sendable {
    case all
    case gitOps
    case flux
    case argoCD
    case unhealthy

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .all: return "All"
        case .gitOps: return "GitOps"
        case .flux: return "Flux"
        case .argoCD: return "ArgoCD"
        case .unhealthy: return "Unhealthy"
        }
    }
}

private struct NavigationCheckpoint: Equatable, Sendable {
    let contextName: String?
    let namespace: String
    let section: RuneSection
    let workloadKind: KubeResourceKind
    let selectedPodName: String?
    let selectedDeploymentName: String?
    let selectedServiceName: String?
    let selectedEventID: String?
    let selectedStatefulSetName: String?
    let selectedDaemonSetName: String?
    let selectedJobName: String?
    let selectedCronJobName: String?
    let selectedReplicaSetName: String?
    let selectedPersistentVolumeClaimName: String?
    let selectedPersistentVolumeName: String?
    let selectedStorageClassName: String?
    let selectedHorizontalPodAutoscalerName: String?
    let selectedNetworkPolicyName: String?
    let selectedEndpointName: String?
    let selectedIngressName: String?
    let selectedConfigMapName: String?
    let selectedSecretName: String?
    let selectedNodeName: String?
    let selectedRBACResourceID: String?
}

public struct SavedWorkspaceInspectorRestoreRequest: Identifiable, Equatable, Sendable {
    public let id: UUID
    public let inspectorState: SavedWorkspaceInspectorState

    public init(id: UUID = UUID(), inspectorState: SavedWorkspaceInspectorState) {
        self.id = id
        self.inspectorState = inspectorState
    }
}

/// Which cluster lists Rune loads for the current section and resource kind. Drives parallel work in `loadResourceSnapshot`.
actor SnapshotRefreshConcurrencyLimiter {
    private struct Waiter {
        let id: UUID
        let continuation: CheckedContinuation<Void, Never>
    }

    private let limit: Int
    private var activeCount = 0
    private var waiters: [Waiter] = []

    init(limit: Int = 6) {
        self.limit = max(1, limit)
    }

    func withPermit<T: Sendable>(_ operation: @Sendable () async throws -> T) async throws -> T {
        try await acquire()
        defer { release() }
        try Task.checkCancellation()
        return try await operation()
    }

    private func acquire() async throws {
        try Task.checkCancellation()
        guard activeCount >= limit else {
            activeCount += 1
            return
        }

        let id = UUID()
        await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                waiters.append(Waiter(id: id, continuation: continuation))
            }
        } onCancel: {
            Task { await self.cancelWaiter(id: id) }
        }
        try Task.checkCancellation()
    }

    private func release() {
        guard waiters.isEmpty else {
            waiters.removeFirst().continuation.resume()
            return
        }
        activeCount = max(0, activeCount - 1)
    }

    private func cancelWaiter(id: UUID) {
        guard let index = waiters.firstIndex(where: { $0.id == id }) else { return }
        waiters.remove(at: index).continuation.resume()
    }
}

private struct SnapshotLoadPlan: Sendable {
    var podStatuses = false
    var pods = false
    var deployments = false
    var deploymentCount = false
    var statefulSets = false
    var daemonSets = false
    var jobs = false
    var cronJobs = false
    var replicaSets = false
    var persistentVolumeClaims = false
    var persistentVolumes = false
    var storageClasses = false
    var horizontalPodAutoscalers = false
    var networkPolicies = false
    var services = false
    var servicesCount = false
    var endpoints = false
    var ingresses = false
    var ingressesCount = false
    var configMaps = false
    var configMapsCount = false
    var cronJobsCount = false
    var secrets = false
    var nodes = false
    var nodesCount = false
    var events = false
    var rbacRoles = false
    var serviceAccounts = false
    var rbacRoleBindings = false
    var rbacClusterRoles = false
    var rbacClusterRoleBindings = false

    mutating func includeEventSource(kind rawKind: String?) {
        let kind = rawKind?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        switch kind {
        case "", "pod":
            pods = true
        case "deployment":
            deployments = true
        case "statefulset":
            statefulSets = true
        case "daemonset":
            daemonSets = true
        case "job":
            jobs = true
        case "cronjob":
            cronJobs = true
        case "replicaset":
            replicaSets = true
        case "horizontalpodautoscaler":
            horizontalPodAutoscalers = true
        case "service":
            services = true
        case "ingress":
            ingresses = true
        case "networkpolicy":
            networkPolicies = true
        case "configmap":
            configMaps = true
        case "secret":
            secrets = true
        case "node":
            nodes = true
        case "persistentvolumeclaim":
            persistentVolumeClaims = true
        case "persistentvolume":
            persistentVolumes = true
        case "storageclass":
            storageClasses = true
        default:
            break
        }
    }

    var resourceListFamilies: Set<RuneResourceListFamily> {
        var families: Set<RuneResourceListFamily> = []
        if podStatuses || pods { families.insert(.pods) }
        if deployments || deploymentCount { families.insert(.deployments) }
        if statefulSets { families.insert(.statefulSets) }
        if daemonSets { families.insert(.daemonSets) }
        if jobs { families.insert(.jobs) }
        if cronJobs || cronJobsCount { families.insert(.cronJobs) }
        if replicaSets { families.insert(.replicaSets) }
        if persistentVolumeClaims { families.insert(.persistentVolumeClaims) }
        if persistentVolumes { families.insert(.persistentVolumes) }
        if storageClasses { families.insert(.storageClasses) }
        if horizontalPodAutoscalers { families.insert(.horizontalPodAutoscalers) }
        if networkPolicies { families.insert(.networkPolicies) }
        if services || servicesCount { families.insert(.services) }
        if endpoints { families.insert(.endpoints) }
        if ingresses || ingressesCount { families.insert(.ingresses) }
        if configMaps || configMapsCount { families.insert(.configMaps) }
        if secrets { families.insert(.secrets) }
        if nodes || nodesCount { families.insert(.nodes) }
        if events { families.insert(.events) }
        if rbacRoles { families.insert(.rbacRoles) }
        if serviceAccounts { families.insert(.serviceAccounts) }
        if rbacRoleBindings { families.insert(.rbacRoleBindings) }
        if rbacClusterRoles { families.insert(.rbacClusterRoles) }
        if rbacClusterRoleBindings { families.insert(.rbacClusterRoleBindings) }
        return families
    }

    static func forSelection(section: RuneSection, kind: KubeResourceKind, simpleMode: Bool = false) -> SnapshotLoadPlan {
        var plan = SnapshotLoadPlan()
        switch section {
        case .overview:
            plan.podStatuses = true
            plan.deploymentCount = true
            plan.servicesCount = true
            plan.ingressesCount = true
            plan.configMapsCount = true
            plan.cronJobsCount = true
            plan.nodesCount = true
            if !simpleMode {
                plan.events = true
            }
        case .workloads:
            switch kind {
            case .pod:
                plan.pods = true
            case .deployment:
                plan.podStatuses = true
                plan.deployments = true
                if !simpleMode {
                    plan.replicaSets = true
                }
            case .statefulSet:
                plan.statefulSets = true
            case .daemonSet:
                plan.daemonSets = true
            case .job:
                plan.jobs = true
            case .cronJob:
                plan.cronJobs = true
            case .replicaSet:
                plan.replicaSets = true
            case .horizontalPodAutoscaler:
                plan.horizontalPodAutoscalers = true
            default:
                plan.pods = true
            }
        case .networking:
            switch kind {
            case .ingress:
                plan.ingresses = true
            case .networkPolicy:
                plan.networkPolicies = true
            case .endpoint:
                plan.endpoints = true
            default:
                plan.services = true
            }
        case .config:
            switch kind {
            case .secret:
                plan.secrets = true
            default:
                plan.configMaps = true
            }
        case .storage:
            switch kind {
            case .persistentVolumeClaim:
                plan.persistentVolumeClaims = true
            case .persistentVolume:
                plan.persistentVolumes = true
            case .storageClass:
                plan.storageClasses = true
            case .node:
                plan.nodes = true
            default:
                plan.persistentVolumeClaims = true
            }
        case .events:
            plan.events = true
        case .rbac:
            if simpleMode {
                switch kind {
                case .serviceAccount:
                    plan.serviceAccounts = true
                case .role:
                    plan.rbacRoles = true
                case .roleBinding:
                    plan.rbacRoleBindings = true
                case .clusterRole:
                    plan.rbacClusterRoles = true
                case .clusterRoleBinding:
                    plan.rbacClusterRoleBindings = true
                default:
                    plan.rbacRoles = true
                }
            } else {
                plan.serviceAccounts = true
                plan.rbacRoles = true
                plan.rbacRoleBindings = true
                plan.rbacClusterRoles = true
                plan.rbacClusterRoleBindings = true
            }
        case .terminal:
            plan.pods = true
        case .helm:
            break
        }
        return plan
    }
}

private struct PendingEventSourceNavigation {
    let event: EventSummary
    let contextName: String?
    let namespace: String

    func matches(contextName: String?, namespace: String) -> Bool {
        guard self.contextName == contextName else { return false }
        return self.namespace.isEmpty
            || self.namespace.caseInsensitiveCompare(namespace) == .orderedSame
    }
}

private struct OverviewSnapshotCacheEntry: Sendable {
    let fetchedAt: Date
    let pods: [PodSummary]
    let deploymentsCount: Int
    let servicesCount: Int
    let ingressesCount: Int
    let configMapsCount: Int
    let cronJobsCount: Int
    let nodesCount: Int
    let clusterCPUPercent: Int?
    let clusterMemoryPercent: Int?
    let events: [EventSummary]

    var hasCoreData: Bool {
        !pods.isEmpty
            || deploymentsCount > 0
            || servicesCount > 0
            || ingressesCount > 0
            || configMapsCount > 0
            || cronJobsCount > 0
            || nodesCount > 0
    }
}

public enum RBACCanIScope: String, CaseIterable, Identifiable, Sendable {
    case namespace
    case cluster

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .namespace: return "Namespace"
        case .cluster: return "Cluster"
        }
    }
}

public struct RBACCanIRequest: Equatable, Sendable {
    public let namespace: String?
    public let verb: String
    public let resource: String
    public let apiGroup: String?
    public let subresource: String?
    public let scope: RBACCanIScope

    public init(
        namespace: String?,
        verb: String,
        resource: String,
        apiGroup: String?,
        subresource: String?,
        scope: RBACCanIScope
    ) {
        self.namespace = Self.nilIfEmpty(namespace?.trimmingCharacters(in: .whitespacesAndNewlines))
        self.verb = verb.trimmingCharacters(in: .whitespacesAndNewlines)
        self.resource = resource.trimmingCharacters(in: .whitespacesAndNewlines)
        self.apiGroup = Self.nilIfEmpty(apiGroup?.trimmingCharacters(in: .whitespacesAndNewlines))
        self.subresource = Self.nilIfEmpty(subresource?.trimmingCharacters(in: .whitespacesAndNewlines))
        self.scope = scope
    }

    private static func nilIfEmpty(_ value: String?) -> String? {
        guard let value, !value.isEmpty else { return nil }
        return value
    }

    public var reviewNamespace: String? {
        scope == .namespace ? namespace : nil
    }

    public var summary: String {
        let group = apiGroup.map { " apiGroup=\($0)" } ?? ""
        let sub = subresource.map { "/\($0)" } ?? ""
        let target = scope == .namespace ? "namespace \(namespace ?? "-")" : "cluster scope"
        return "\(verb) \(resource)\(sub)\(group) in \(target)"
    }
}

public struct RBACCanIResult: Equatable, Sendable {
    public let request: RBACCanIRequest
    public let allowed: Bool?
    public let errorMessage: String?
    public let checkedAt: Date

    public init(request: RBACCanIRequest, allowed: Bool?, errorMessage: String?, checkedAt: Date = Date()) {
        self.request = request
        self.allowed = allowed
        self.errorMessage = errorMessage
        self.checkedAt = checkedAt
    }

    public var statusText: String {
        if let allowed {
            return allowed ? "Allowed" : "Denied"
        }
        return "Could not verify"
    }
}

public typealias RBACCanIChecking = @MainActor (
    _ sources: [KubeConfigSource],
    _ context: KubeContext,
    _ namespace: String?,
    _ verb: String,
    _ resource: String,
    _ apiGroup: String?,
    _ subresource: String?
) async throws -> Bool

public typealias HelmReleaseListing = @MainActor @Sendable (
    _ sources: [KubeConfigSource],
    _ context: KubeContext,
    _ namespace: String?,
    _ allNamespaces: Bool
) async throws -> [HelmReleaseSummary]

public struct HPAScaleTargetReference: Identifiable, Equatable, Sendable {
    public let kind: KubeResourceKind
    public let namespace: String?
    public let name: String
    public let subtitle: String
    public let symbol: String

    public init(kind: KubeResourceKind, namespace: String?, name: String, subtitle: String, symbol: String) {
        self.kind = kind
        self.namespace = namespace
        self.name = name
        self.subtitle = subtitle
        self.symbol = symbol
    }

    public var id: String {
        "\(kind.rawValue)|\(namespace ?? "_cluster")|\(name)"
    }
}

@MainActor
public final class RuneAppViewModel: ObservableObject {
    private struct HelmReleaseListScope: Equatable {
        let kubeConfigSources: [KubeConfigSource]
        let context: KubeContext
        let namespace: String
        let allNamespaces: Bool
    }

    private struct RBACDataScope: Equatable {
        let kubeConfigSources: [KubeConfigSource]
        let context: KubeContext
        let namespace: String
    }

    private struct PendingWriteScopeSnapshot: Equatable, Sendable {
        let id: UInt64
        let kubeConfigSources: [KubeConfigSource]
        let context: KubeContext
        let namespace: String
        let isProduction: Bool
    }

    private struct AuthDoctorScope: Equatable {
        let kubeConfigSources: [KubeConfigSource]
        let selectedContext: KubeContext?
        let selectedNamespace: String
    }

    private static let productionContextNameMarkers = ["prod", "production", "live", "critical"]

    @Published public private(set) var state: RuneAppState
    @Published public var selectedLogPreset: PodLogPreset = .recentLines
    @Published public var includePreviousLogs: Bool = false
    @Published public var selectedLogContainer: String = ""
    @Published public private(set) var pendingWriteDryRunStatus: String?
    @Published public var writeAuditSearchQuery: String = ""
    @Published public var isLogTailModeEnabled: Bool = false {
        didSet {
            if isLogTailModeEnabled {
                isLogStreamPaused = false
                reloadLogsForSelection()
            } else {
                isLogStreamPaused = false
                tailLogsReloadTask?.cancel()
                tailLogsReloadTask = nil
            }
        }
    }
    @Published public var isLogStreamPaused: Bool = false {
        didSet {
            if isLogStreamPaused {
                tailLogsReloadTask?.cancel()
                tailLogsReloadTask = nil
            } else if isLogTailModeEnabled {
                scheduleNextTailLogsReload()
            }
        }
    }
    @Published public private(set) var podSortColumn: PodListSortColumn = .name
    @Published public private(set) var podSortAscending: Bool = true
    @Published public private(set) var deploymentSortColumn: DeploymentListSortColumn = .name
    @Published public private(set) var deploymentSortAscending: Bool = true
    @Published public private(set) var serviceSortColumn: ServiceListSortColumn = .name
    @Published public private(set) var serviceSortAscending: Bool = true
    @Published public private(set) var genericResourceSortColumn: GenericResourceListSortColumn = .name
    @Published public private(set) var genericResourceSortAscending: Bool = true
    @Published public private(set) var helmReleaseSortColumn: HelmReleaseListSortColumn = .name
    @Published public private(set) var helmReleaseSortAscending: Bool = true
    @Published public private(set) var eventSortColumn: EventListSortColumn = .reason
    @Published public private(set) var eventSortAscending: Bool = true
    @Published public private(set) var operatorResourceSortColumn: OperatorResourceListSortColumn = .family
    @Published public private(set) var operatorResourceSortAscending: Bool = true
    @Published public private(set) var operatorResourcePage: Int = 0
    @Published public private(set) var operatorResourceFocus: OperatorResourceFocus = .all
    @Published public private(set) var hiddenOperatorPrinterColumnFamilies: Set<String> = []
    @Published public private(set) var savedWorkspaces: [SavedWorkspaceSnapshot] = [] {
        didSet {
            invalidateCommandPaletteResultCache()
        }
    }
    @Published public private(set) var kubeConfigImportReviews: [KubeConfigImportReview] = []
    @Published public private(set) var kubeConfigImportContextMetadataDrafts: [String: ContextDisplayMetadata] = [:]
    @Published public private(set) var kubeConfigImportReviewMode: KubeConfigImportReviewMode?
    @Published public private(set) var isKubeConfigImportConfirmationPending = false
    @Published public private(set) var canConfirmKubeConfigImport = false
    @Published public private(set) var isPreparingKubeConfigImport = false
    @Published public private(set) var isCommittingKubeConfigImport = false
    @Published public var favoriteImportedKubeConfigContexts: Bool = false
    @Published public var kubeConfigDuplicateHandlingChoice: KubeConfigDuplicateHandlingChoice = .skipDuplicate {
        didSet {
            refreshPendingKubeConfigImportResolution()
        }
    }
    private var namespaceResolutionLookupCache: (namespaces: [String], lookup: [String: String])?
    @Published public var manualKubeConfigName: String = ""
    @Published public var manualKubeConfigServer: String = ""
    @Published public var manualKubeConfigNamespace: String = "default"
    @Published public var manualKubeConfigToken: String = ""
    @Published public private(set) var cloudKubeConfigImportStatus: String?
    @Published public private(set) var cloudKubeConfigImportDiagnostic: AddClusterCloudImportDiagnostic?
    @Published public private(set) var cloudKubeConfigImportOutput: String = ""
    @Published public private(set) var isRunningCloudKubeConfigImport = false
    @Published public private(set) var isRunningNativeCloudClusterImport = false
    @Published public private(set) var nativeKubernetesAuthStatus: String?
    @Published public private(set) var isConnectingNativeKubernetesAuth = false
    @Published public var pendingWriteAction: PendingWriteAction? {
        didSet {
            if let pendingWriteAction,
               let context = state.selectedContext {
                nextPendingWriteScopeID &+= 1
                pendingWriteScopeSnapshot = PendingWriteScopeSnapshot(
                    id: nextPendingWriteScopeID,
                    kubeConfigSources: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    isProduction: isProductionContext(context)
                )
                pendingProductionDestructiveConfirmation = nil
                pendingProductionDestructiveConfirmationScopeID = nil
                switch pendingWriteAction {
                case .rolloutUndo, .controllerRolloutUndo:
                    break
                default:
                    pendingRollbackPlan = nil
                }
            } else {
                pendingWriteScopeSnapshot = nil
                pendingProductionDestructiveConfirmation = nil
                pendingProductionDestructiveConfirmationScopeID = nil
            }
        }
    }
    @Published public private(set) var pendingProductionDestructiveConfirmation: PendingWriteAction?
    private var pendingWriteScopeSnapshot: PendingWriteScopeSnapshot?
    private var nextPendingWriteScopeID: UInt64 = 0
    private var pendingProductionDestructiveConfirmationScopeID: UInt64?
    @Published public var scaleReplicaInput: Int = 1
    @Published public var execCommandInput: String = "printenv"
    @Published public var terminalSessionInput: String = ""
    @Published public var portForwardLocalPortInput: String = "8080"
    @Published public var portForwardRemotePortInput: String = "8080"
    @Published public var portForwardAddressInput: String = "127.0.0.1"
    @Published public var rolloutRevisionInput: String = ""
    @Published public var helmRollbackWait: Bool = true
    @Published public var helmRollbackTimeoutInput: String = "5m"
    @Published public var helmRollbackCleanupOnFail: Bool = true
    @Published public private(set) var pendingRollbackPlan: String?
    @Published public var rbacCanIVerb: String = "list"
    @Published public var rbacCanIResource: String = "pods"
    @Published public var rbacCanIApiGroup: String = ""
    @Published public var rbacCanISubresource: String = ""
    @Published public var rbacCanIScope: RBACCanIScope = .namespace
    @Published public private(set) var rbacCanIResult: RBACCanIResult?
    @Published public private(set) var isRunningRBACCanI = false
    @Published public private(set) var kubernetesRequestMetricsSummary: KubernetesRequestMetricsDebugPresentation = .empty
    @Published public private(set) var isRefreshingKubernetesRequestMetricsSummary = false
    @Published public var isSidebarVisible: Bool = UserDefaults.standard.runeLayoutSidebarVisible
    @Published public var isDetailPaneVisible: Bool = UserDefaults.standard.runeLayoutDetailPaneVisible
    @Published public var isLiveStatusUpdatesEnabled: Bool = false {
        didSet {
            if isLiveStatusUpdatesEnabled {
                startLiveStatusUpdates()
            } else {
                stopLiveStatusUpdates()
            }
        }
    }
    @Published public private(set) var canNavigateBack = false
    @Published public private(set) var canNavigateForward = false
    @Published public private(set) var isLaunchExperienceVisible = true
    @Published public private(set) var commandPalettePrefillQuery = ""
    @Published public private(set) var savedWorkspaceInspectorRestoreRequest: SavedWorkspaceInspectorRestoreRequest?

    private let kubeClient: KubernetesClient
    private let bookmarkManager: BookmarkManager
    private let pendingKubeConfigSourceAccess = SecurityScopedAccess()
    private let picker: KubeConfigPicking
    private let gkeCredentialFilePicker: any GKECredentialFilePicking
    private let kubeConfigDiscoverer: KubeConfigDiscovering
    private let store: ResourceStore
    private let exporter: FileExporting
    private let configuredExporter: ConfiguredExporting
    private let supportBundleBuilder: any SupportBundleBuilding
    private let contextPreferences: ContextPreferencesStoring
    private let savedWorkspaceStore: SavedWorkspaceStoring
    private let kubeConfigImportValidator: KubeConfigImportValidator
    private let kubeConfigImportStore: KubeConfigImportStoring
    private let kubeConfigDuplicateResolver = KubeConfigDuplicateResolver()
    private let cloudKubeConfigImporter: CloudKubeConfigImporting
    private let nativeCloudClusterImporter: any NativeCloudClusterImporting
    private let nativeAuthConfigurator: any KubernetesNativeAuthConfiguring
    private let helmCommandRunner: HelmCommandRunning
    private let helmReleaseList: HelmReleaseListing
    private let overviewSnapshotPersistence: any OverviewSnapshotCacheStoring
    private let namespaceListPersistence: NamespaceListPersisting
    private let portForwardBrowserOpener: PortForwardBrowserOpening
    private let diagnostics: DiagnosticsRecorder
    private let rbacCanICheck: RBACCanIChecking
    private let snapshotRefreshConcurrencyLimiter = SnapshotRefreshConcurrencyLimiter(limit: 6)
    private let terminalShellCommand = ["sh"]

    private var cancellables: Set<AnyCancellable> = []
    private var hasBootstrapped = false
    private var bootstrapTask: Task<Void, Never>?
    private var kubeConfigSourceSyncTask: Task<Void, Never>?
    private var sessionImportedKubeConfigSourcePaths = Set<String>()
    private var latestKubeConfigSourceFingerprint: KubeConfigSourceFingerprint?
    private var pendingKubeConfigImport: KubeConfigImportTransaction?
    private var pendingKubeConfigImportRegistrySnapshot: KubeConfigImportRegistrySnapshot?
    private var pendingKubeConfigTemporaryDirectories: [URL] = []
    private var pendingCloudKubeConfigProvider: CloudKubeConfigProvider?
    private var pendingNativeCloudCredential: PendingNativeCloudCredential?
    private var nativeCloudClusterImportTask: Task<Void, Never>?
    private var nativeGKEImportPickerReservation: UUID?
    private var nativeGKECredentialConnectPickerReservation: UUID?
    private var authDoctorTask: Task<Void, Never>?
    private var activeAuthDoctorRunID: UUID?
    private var shouldRefreshKubernetesRequestMetricsSummaryAgain = false
    private var clusterLoadGeneration = UUID()
    private var launchExperienceStartedAt = ContinuousClock.now
    private var latestSnapshotRequestID = UUID()
    private var latestResourceDetailsRequestID = UUID()
    private var latestLogsReloadRequestID = UUID()
    private var latestYAMLValidationRequestID = UUID()
    private var latestHelmDetailsRequestID = UUID()
    private var latestHelmReleaseListRequestID = UUID()
    private var activeHelmReleaseListRequestID: UUID?
    private var navigationHistory: [NavigationCheckpoint] = []
    private var navigationIndex: Int = -1
    private var isApplyingNavigationCheckpoint = false
    private var deferredSelectionRestoreGeneration: UInt64 = 0
    private var isRunningDeferredSelectionRestore = false
    private var pendingOpenEventSource: PendingEventSourceNavigation?
    /// Bounded retries while the snapshot plan hydrates the pending event source family.
    private var navigateFromEventFetchAttempts = 0
    private var scheduledRefreshTask: Task<Void, Never>?
    private var pendingCurrentViewRefreshID: UUID?
    private var savedWorkspaceRestoreTask: Task<Void, Never>?
    private var navigationSelectionRestoreTask: Task<Void, Never>?
    private var resourceDetailsTask: Task<Void, Never>?
    private var resourceDetailsRequestPreservingVisibleDocuments: UUID?
    private var scheduledLogsReloadTask: Task<Void, Never>?
    private var logsReloadTask: Task<Void, Never>?
    private var tailLogsReloadTask: Task<Void, Never>?
    private var yamlValidationTask: Task<Void, Never>?
    private var terminalOutputFlushTask: Task<Void, Never>?
    private var liveStatusUpdatesTask: Task<Void, Never>?
    private var pendingTerminalOutputBySessionID: [String: String] = [:]
    private var pendingTerminalEscapeBySessionID: [String: String] = [:]
    private var terminalSessionAttemptByID: [String: UUID] = [:]
    private var currentSavedWorkspaceInspectorState: SavedWorkspaceInspectorState?
    private var commandPaletteContextMetadataCache: [String: ContextDisplayMetadata] = [:]
    private var commandPaletteContextsWithoutMetadata: Set<String> = []
    private var commandPaletteResultCache: [String: [CommandPaletteItem]] = [:]
    private var commandPaletteResultCacheOrder: [String] = []
    private var cachedProductionNameMarker: (contextName: String, isMatch: Bool)?
    private var requiresProductionSecondConfirmation = UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation
    private var pendingForcedNamespaceRefresh = false
    /// Set during context switch with no explicit namespace so first metadata refresh can override stale carry-over namespace.
    private var pendingNamespaceRevalidationContextName: String?
    private var rbacDataScope: RBACDataScope?
    private var namespaceMetadataRefreshedAt: [String: Date] = [:]
    /// In-memory overview rows keyed by `overviewCacheKey(contextName:namespace:)`; TTL `overviewSnapshotFreshnessTTL`. Mirrors disk where possible; merged with `ResourceStore` on apply.
    private var overviewSnapshotCache: [String: OverviewSnapshotCacheEntry] = [:]
    /// One-shot bypass after cancelled/stale snapshots so the next load for a key does not get stuck behind cooldown.
    private var bypassOverviewCooldownKeys: Set<String> = []
    /// Background task: `listPodStatuses` + count queries for sibling namespaces; cancelled on context change.
    private var overviewPrefetchTask: Task<Void, Never>?
    /// Background task: warms overview cache for non-selected contexts; cancelled on context change.
    private var contextOverviewPrefetchTask: Task<Void, Never>?
    private(set) var helmBrowserResourceFamily: RuneResourceListFamily = .helmReleases
    private var recentNamespacesByContext: [String: [String]] = [:]
    /// Recently selected contexts (most-recent first); used with favorites when selecting prefetch targets.
    private var recentContextNames: [String] = []
    private let demoContextName = "rune-demo"
    private var demoContext: KubeContext { KubeContext(name: demoContextName) }
    private static let operatorResourcePageSize = 40
    private static let commandPaletteResultCacheLimit = 24

    private let refreshDebounceNanoseconds: UInt64 = 120_000_000
    /// Coalesces rapid log preset toggles while still cancelling any in-flight fetch immediately.
    private let logsReloadDebounceNanoseconds: UInt64 = 180_000_000
    private let terminalOutputFlushNanoseconds: UInt64 = 33_000_000
    private static let maximumCloudKubeConfigImportOutputCharacters = 12_000
    private let tailLogsReloadNanoseconds: UInt64 = 3_000_000_000
    private let liveStatusUpdateNanoseconds: UInt64 = 12_000_000_000
    private let kubeConfigSourceSyncNanoseconds: UInt64 = 2_000_000_000
    private let launchExperienceMinimumNanoseconds: UInt64 = 240_000_000
    /// Keep YAML validation responsive enough for editing while still avoiding a server dry-run on every keystroke.
    private let yamlValidationDebounceNanoseconds: UInt64 = 300_000_000
    /// How long `listNamespaces` results are treated as fresh before the next snapshot refresh. Larger clusters feel snappier when we do not refetch namespaces on every navigation.
    private let namespaceMetadataTTL: TimeInterval = 120
    /// Maximum age of `overviewSnapshotCache` entries before refresh is preferred over warm paths.
    private let overviewSnapshotFreshnessTTL: TimeInterval = 60
    /// Cooldown for repeating heavy overview network calls (pod statuses + count queries).
    /// Within this window, Rune reuses warm overview cache and avoids issuing the same expensive requests.
    private let overviewHeavyRequestCooldownTTL: TimeInterval = 12
    /// Maximum age for treating `overviewSnapshotPersistence` loads as warm data when hydrating memory.
    private let overviewDiskSnapshotFreshnessTTL: TimeInterval = 60 * 5
    private let overviewSnapshotRetentionTTL: TimeInterval = 60 * 20
    private let maxOverviewSnapshotEntries = 180
    private let maxRecentNamespacesPerContext = 4
    private let maxRecentContexts = 8
    /// Cap on namespaces to prefetch per snapshot (pod status + resource counts).
    /// Disabled by default to avoid API pressure that can delay foreground pod loads.
    private let maxOverviewPrefetchNamespaces = 0
    private let overviewPrefetchThrottleNanoseconds: UInt64 = 120_000_000
    /// Max non-selected contexts to warm in background (favorites + recent first).
    private let maxOverviewPrefetchContexts = 2
    private let contextOverviewPrefetchThrottleNanoseconds: UInt64 = 250_000_000

    public init(
        state: RuneAppState = RuneAppState(),
        kubeClient: KubernetesClient = KubernetesClient(),
        bookmarkManager: BookmarkManager = BookmarkManager(store: UserDefaultsBookmarkStore()),
        picker: KubeConfigPicking = OpenPanelKubeConfigPicker(),
        gkeCredentialFilePicker: any GKECredentialFilePicking = OpenPanelGKECredentialFilePicker(),
        kubeConfigDiscoverer: KubeConfigDiscovering = KubeConfigDiscoverer(),
        store: ResourceStore = ResourceStore(),
        exporter: FileExporting = SavePanelExporter(),
        configuredExporter: ConfiguredExporting = ConfiguredFolderExporter(),
        supportBundleBuilder: any SupportBundleBuilding = JSONSupportBundleBuilder(),
        contextPreferences: ContextPreferencesStoring = UserDefaultsContextPreferencesStore(),
        savedWorkspaceStore: SavedWorkspaceStoring = JSONSavedWorkspaceStore(),
        kubeConfigImportValidator: KubeConfigImportValidator = KubeConfigImportValidator(),
        kubeConfigImportStore: KubeConfigImportStoring = AppOwnedKubeConfigImportStore(),
        cloudKubeConfigImporter: CloudKubeConfigImporting = CloudKubeConfigCLIImporter(),
        nativeCloudClusterImporter: any NativeCloudClusterImporting = NativeCloudClusterImporter(),
        nativeAuthConfigurator: any KubernetesNativeAuthConfiguring = DefaultKubernetesNativeCredentialProvider.shared,
        helmCommandRunner: HelmCommandRunning = ProcessHelmCommandRunner(),
        helmReleaseList: HelmReleaseListing? = nil,
        overviewSnapshotPersistence: any OverviewSnapshotCacheStoring = JSONOverviewSnapshotCacheStore(),
        namespaceListPersistence: NamespaceListPersisting = JSONNamespaceListPersistenceStore(),
        portForwardBrowserOpener: PortForwardBrowserOpening = WorkspacePortForwardBrowserOpener(),
        diagnostics: DiagnosticsRecorder = DiagnosticsRecorder(),
        rbacCanICheck: RBACCanIChecking? = nil
    ) {
        self.state = state
        self.kubeClient = kubeClient
        self.bookmarkManager = bookmarkManager
        self.picker = picker
        self.gkeCredentialFilePicker = gkeCredentialFilePicker
        self.kubeConfigDiscoverer = kubeConfigDiscoverer
        self.store = store
        self.exporter = exporter
        self.configuredExporter = configuredExporter
        self.supportBundleBuilder = supportBundleBuilder
        self.contextPreferences = contextPreferences
        self.savedWorkspaceStore = savedWorkspaceStore
        self.kubeConfigImportValidator = kubeConfigImportValidator
        self.kubeConfigImportStore = kubeConfigImportStore
        self.cloudKubeConfigImporter = cloudKubeConfigImporter
        self.nativeCloudClusterImporter = nativeCloudClusterImporter
        self.nativeAuthConfigurator = nativeAuthConfigurator
        self.helmCommandRunner = helmCommandRunner
        self.helmReleaseList = helmReleaseList ?? { sources, context, namespace, allNamespaces in
            try await kubeClient.listReleases(
                from: sources,
                context: context,
                namespace: namespace,
                allNamespaces: allNamespaces
            )
        }
        self.overviewSnapshotPersistence = overviewSnapshotPersistence
        self.namespaceListPersistence = namespaceListPersistence
        self.portForwardBrowserOpener = portForwardBrowserOpener
        self.diagnostics = diagnostics
        self.rbacCanICheck = rbacCanICheck ?? { sources, context, namespace, verb, resource, apiGroup, subresource in
            try await kubeClient.canI(
                from: sources,
                context: context,
                namespace: namespace,
                verb: verb,
                resource: resource,
                apiGroup: apiGroup,
                subresource: subresource
            )
        }

        self.state.setFavoriteContextNames(contextPreferences.loadFavoriteContextNames())
        self.state.setFavoriteResourceIDs(contextPreferences.loadFavoriteResourceIDs())
        self.state.setFavoriteNamespaceIDs(contextPreferences.loadFavoriteNamespaceIDs())
        self.state.setManualProductionContextIDs(contextPreferences.loadManualProductionContextIDs())
        self.hiddenOperatorPrinterColumnFamilies = contextPreferences.loadHiddenOperatorPrinterColumnFamilies()
        self.savedWorkspaces = savedWorkspaceStore.loadSavedWorkspaces()

        state.objectWillChange
            .sink { [weak self] _ in
                self?.invalidateCommandPaletteResultCache()
                self?.objectWillChange.send()
            }
            .store(in: &cancellables)

        $selectedLogPreset
            .dropFirst()
            .sink { [weak self] _ in
                self?.scheduleLogsReloadForSelection()
            }
            .store(in: &cancellables)

        $includePreviousLogs
            .dropFirst()
            .sink { [weak self] _ in
                self?.scheduleLogsReloadForSelection()
            }
            .store(in: &cancellables)

        $selectedLogContainer
            .dropFirst()
            .sink { [weak self] _ in
                self?.scheduleLogsReloadForSelection()
            }
            .store(in: &cancellables)

        state.$resourceYAML
            .sink { [weak self] _ in
                self?.scheduleResourceYAMLValidation()
            }
            .store(in: &cancellables)

        state.$resourceSearchQuery
            .removeDuplicates()
            .dropFirst()
            .sink { [weak self] _ in
                self?.operatorResourcePage = 0
            }
            .store(in: &cancellables)

        Publishers.CombineLatest3(
            state.$kubeConfigSources,
            state.$selectedContext,
            state.$selectedNamespace
        )
        .map { sources, context, namespace in
            AuthDoctorScope(
                kubeConfigSources: sources,
                selectedContext: context,
                selectedNamespace: namespace
            )
        }
        .removeDuplicates()
        .dropFirst()
        .sink { [weak self] _ in
            self?.invalidateAuthDoctorRunForScopeChange()
        }
        .store(in: &cancellables)

        NotificationCenter.default.publisher(for: .runeCachesDidClear)
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
                self?.handleCachesCleared()
            }
            .store(in: &cancellables)

        NotificationCenter.default.publisher(for: UserDefaults.didChangeNotification)
            .sink { @Sendable [weak self] _ in
                self?.receiveUserDefaultsDidChange()
            }
            .store(in: &cancellables)
    }

    private nonisolated func receiveUserDefaultsDidChange() {
        if Thread.isMainThread {
            MainActor.assumeIsolated {
                handleUserDefaultsDidChange()
            }
        } else {
            Task { @MainActor [weak self] in
                self?.handleUserDefaultsDidChange()
            }
        }
    }

    private func handleUserDefaultsDidChange() {
        requiresProductionSecondConfirmation = UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation
        commandPaletteContextMetadataCache.removeAll(keepingCapacity: true)
        commandPaletteContextsWithoutMetadata.removeAll(keepingCapacity: true)
        invalidateCommandPaletteResultCache()
    }

    public var workloadKinds: [KubeResourceKind] {
        [.pod, .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler]
    }

    public var networkingKinds: [KubeResourceKind] {
        [.service, .endpoint, .ingress, .networkPolicy]
    }

    public var configKinds: [KubeResourceKind] {
        [.configMap, .secret]
    }

    public var storageKinds: [KubeResourceKind] {
        [.persistentVolumeClaim, .persistentVolume, .storageClass, .node]
    }

    public var rbacKinds: [KubeResourceKind] {
        [.serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding]
    }

    public var visibleRBACResources: [ClusterResourceSummary] {
        let list: [ClusterResourceSummary] = {
            switch state.selectedWorkloadKind {
            case .serviceAccount: return state.serviceAccounts
            case .role: return state.rbacRoles
            case .roleBinding: return state.rbacRoleBindings
            case .clusterRole: return state.rbacClusterRoles
            case .clusterRoleBinding: return state.rbacClusterRoleBindings
            default: return []
            }
        }()
        return genericResourceSorted(filtered(list) { summaryText(for: $0) })
    }

    public var writeActionsEnabled: Bool {
        !state.isReadOnlyMode
    }

    /// Cluster mutations (apply, delete, scale, exec, rollout) — blocked while a snapshot or resource manifest is still loading so users do not act on stale YAML/lists.
    public var canApplyClusterMutations: Bool {
        guard writeActionsEnabled else { return false }
        if state.isLoading { return false }
        if state.isLoadingResourceDetails { return false }
        return true
    }

    public var namespaceOptions: [String] {
        guard state.selectedContext != nil else { return [] }
        let manual = currentContextManualNamespaces()
        // Only expose namespaces that belong to the current context.
        // Source from the active state list (cleared on context switch) so we never leak a stale
        // namespace menu from cache before the current context has loaded.
        // If no verified list is loaded yet, only expose the current in-memory selection.
        let options = state.namespaces
        if !options.isEmpty {
            return favoriteSortedNamespaces(sortedNamespaceOptions(options + manual + [state.selectedNamespace]))
        }

        let selected = state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        if !selected.isEmpty {
            return favoriteSortedNamespaces(sortedNamespaceOptions([selected] + manual))
        }

        return favoriteSortedNamespaces(sortedNamespaceOptions(manual))
    }

    public var manualNamespaceOptions: [String] {
        guard state.selectedContext != nil else { return [] }
        return favoriteSortedNamespaces(sortedNamespaceOptions(currentContextManualNamespaces()))
    }

    public var podLogContainerOptions: [String] {
        state.selectedPod?.logContainerNames ?? []
    }

    public var overviewUnhealthyItems: [OverviewSignalItem] {
        overviewInsightsProjector.unhealthyItems()
    }

    public var overviewIncidentTimelineItems: [OverviewSignalItem] {
        overviewInsightsProjector.incidentTimelineItems()
    }

    public var overviewDependencyItems: [OverviewDependencyItem] {
        overviewInsightsProjector.dependencyItems()
    }

    public var overviewGitOpsRollupItems: [OverviewGitOpsRollupItem] {
        overviewInsightsProjector.gitOpsRollupItems()
    }

    private var overviewInsightsProjector: OverviewInsightsProjector {
        OverviewInsightsProjector(
            pods: state.overviewPods,
            deployments: state.deployments,
            services: state.services,
            ingresses: state.ingresses,
            persistentVolumeClaims: state.persistentVolumeClaims,
            persistentVolumes: state.persistentVolumes,
            events: state.overviewEvents,
            jobs: state.jobs,
            nodes: state.nodes,
            operatorResources: state.operatorResources
        )
    }

    public var selectedPodCount: Int {
        state.selectedPodIDs.count
    }

    public var areAllVisiblePodsSelectedForBulkActions: Bool {
        let visibleIDs = Set(visiblePods.map(\.id))
        return !visibleIDs.isEmpty && visibleIDs.isSubset(of: state.selectedPodIDs)
    }

    public var selectedPodsForBulkActions: [PodSummary] {
        let selectedIDs = state.selectedPodIDs
        guard !selectedIDs.isEmpty else { return [] }
        return sortedPods(state.pods.filter { selectedIDs.contains($0.id) })
    }

    public var visibleGenericResourcesForBulkActions: [ClusterResourceSummary] {
        switch state.selectedWorkloadKind {
        case .statefulSet: return visibleStatefulSets
        case .daemonSet: return visibleDaemonSets
        case .job: return visibleJobs
        case .cronJob: return visibleCronJobs
        case .replicaSet: return visibleReplicaSets
        case .horizontalPodAutoscaler: return visibleHorizontalPodAutoscalers
        case .ingress: return visibleIngresses
        case .networkPolicy: return visibleNetworkPolicies
        case .endpoint: return visibleEndpoints
        case .configMap: return visibleConfigMaps
        case .secret: return visibleSecrets
        case .persistentVolumeClaim: return visiblePersistentVolumeClaims
        case .persistentVolume: return visiblePersistentVolumes
        case .storageClass: return visibleStorageClasses
        case .node: return visibleNodes
        case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding: return visibleRBACResources
        case .pod, .deployment, .service, .event:
            return []
        }
    }

    private var genericResourcesForBulkActions: [ClusterResourceSummary] {
        switch state.selectedWorkloadKind {
        case .statefulSet: return state.statefulSets
        case .daemonSet: return state.daemonSets
        case .job: return state.jobs
        case .cronJob: return state.cronJobs
        case .replicaSet: return state.replicaSets
        case .horizontalPodAutoscaler: return state.horizontalPodAutoscalers
        case .ingress: return state.ingresses
        case .networkPolicy: return state.networkPolicies
        case .endpoint: return state.endpoints
        case .configMap: return state.configMaps
        case .secret: return state.secrets
        case .persistentVolumeClaim: return state.persistentVolumeClaims
        case .persistentVolume: return state.persistentVolumes
        case .storageClass: return state.storageClasses
        case .node: return state.nodes
        case .serviceAccount: return state.serviceAccounts
        case .role: return state.rbacRoles
        case .roleBinding: return state.rbacRoleBindings
        case .clusterRole: return state.rbacClusterRoles
        case .clusterRoleBinding: return state.rbacClusterRoleBindings
        case .pod, .deployment, .service, .event: return []
        }
    }

    public var selectedGenericResourceCount: Int {
        selectedGenericResourcesForBulkActions.count
    }

    public var areAllVisibleGenericResourcesSelectedForBulkActions: Bool {
        let visibleIDs = Set(visibleGenericResourcesForBulkActions.map(\.id))
        return !visibleIDs.isEmpty && visibleIDs.isSubset(of: state.selectedGenericResourceIDs)
    }

    public var selectedGenericResourcesForBulkActions: [ClusterResourceSummary] {
        let selectedIDs = state.selectedGenericResourceIDs
        guard !selectedIDs.isEmpty else { return [] }
        return genericResourceSorted(genericResourcesForBulkActions.filter { selectedIDs.contains($0.id) })
    }

    public var canCopySelectedGenericResourceComparison: Bool {
        selectedGenericResourcesForBulkActions.count >= 2
    }

    public var selectedGenericResourceComparisonText: String {
        let resources = selectedGenericResourcesForBulkActions
        guard !resources.isEmpty else { return "" }

        var lines: [String] = [
            "Selected \(state.selectedWorkloadKind.title) Compare",
            "Context: \(state.selectedContext?.name ?? "No context")",
            "Namespace: \(state.selectedNamespace.isEmpty ? "Cluster" : state.selectedNamespace)",
            "Count: \(resources.count)",
            ""
        ]

        for resource in resources {
            lines.append("- \(resource.namespace.map { "\($0)/" } ?? "")\(resource.name)")
            lines.append("  Kind: \(resource.kind.title)")
            lines.append("  Primary: \(resource.primaryText)")
            lines.append("  Secondary: \(resource.secondaryText)")
            lines.append("")
        }

        return lines.joined(separator: "\n").trimmingCharacters(in: .whitespacesAndNewlines)
    }

    public func isPodSelectedForBulkAction(_ pod: PodSummary) -> Bool {
        state.selectedPodIDs.contains(pod.id)
    }

    public func togglePodBulkSelection(_ pod: PodSummary) {
        state.toggleSelectedPodID(pod.id)
    }

    public func selectAllVisiblePodsForBulkActions() {
        state.setSelectedPodIDs(state.selectedPodIDs.union(visiblePods.map(\.id)))
    }

    public func toggleAllVisiblePodsForBulkActions() {
        let visibleIDs = Set(visiblePods.map(\.id))
        if areAllVisiblePodsSelectedForBulkActions {
            state.setSelectedPodIDs(state.selectedPodIDs.subtracting(visibleIDs))
        } else {
            selectAllVisiblePodsForBulkActions()
        }
    }

    public func clearPodBulkSelection() {
        state.clearSelectedPodIDs()
    }

    public func isGenericResourceSelectedForBulkAction(_ resource: ClusterResourceSummary) -> Bool {
        state.selectedGenericResourceIDs.contains(resource.id)
    }

    public func toggleGenericResourceBulkSelection(_ resource: ClusterResourceSummary) {
        state.toggleSelectedGenericResourceID(
            resource.id,
            validIDs: Set(genericResourcesForBulkActions.map(\.id))
        )
    }

    public func toggleAllVisibleGenericResourcesForBulkActions() {
        let visibleIDs = Set(visibleGenericResourcesForBulkActions.map(\.id))
        let validIDs = Set(genericResourcesForBulkActions.map(\.id))
        if areAllVisibleGenericResourcesSelectedForBulkActions {
            state.setSelectedGenericResourceIDs(
                state.selectedGenericResourceIDs.subtracting(visibleIDs),
                validIDs: validIDs
            )
        } else {
            state.setSelectedGenericResourceIDs(
                state.selectedGenericResourceIDs.union(visibleIDs),
                validIDs: validIDs
            )
        }
    }

    public func clearGenericResourceBulkSelection() {
        state.clearSelectedGenericResourceIDs()
    }

    private func clearResourceBulkSelections() {
        state.clearSelectedPodIDs()
        state.clearSelectedGenericResourceIDs()
    }

    public func copySelectedGenericResourceComparisonToClipboard(
        to pasteboard: NSPasteboard = .general
    ) {
        guard canCopySelectedGenericResourceComparison else { return }
        let comparison = selectedGenericResourceComparisonText
        guard !comparison.isEmpty else { return }
        pasteboard.clearContents()
        pasteboard.setString(comparison, forType: .string)
    }

    public func toggleFavoriteNamespace(_ namespace: String) {
        guard let id = favoriteNamespaceID(namespace) else { return }
        state.toggleFavoriteNamespace(id: id)
        contextPreferences.saveFavoriteNamespaceIDs(state.favoriteNamespaceIDs)
    }

    public func isFavoriteNamespace(_ namespace: String) -> Bool {
        guard let id = favoriteNamespaceID(namespace) else { return false }
        return state.isFavoriteNamespace(id: id)
    }

    public func saveCurrentWorkspace(named rawName: String) {
        let name = rawName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else { return }

        let snapshot = SavedWorkspaceSnapshot(
            name: name,
            contextName: state.selectedContext?.name,
            namespace: state.selectedNamespace,
            section: state.selectedSection,
            workloadKind: state.selectedWorkloadKind,
            isFavorite: existingSavedWorkspaceFavorite(named: name),
            resourceKind: selectedResourceKindLabel(),
            resourceName: selectedResourceName(),
            resourceNamespace: selectedResourceNamespace(),
            logPresetID: selectedLogPreset.rawValue,
            logContainer: selectedLogContainer,
            includePreviousLogs: includePreviousLogs,
            isLogTailModeEnabled: isLogTailModeEnabled,
            isSidebarVisible: isSidebarVisible,
            isDetailPaneVisible: isDetailPaneVisible,
            inspectorState: currentSavedWorkspaceInspectorState
        )
        savedWorkspaces = ([snapshot] + savedWorkspaces.filter {
            $0.name.caseInsensitiveCompare(name) != .orderedSame
        })
        persistSavedWorkspaces()
    }

    public func deleteSavedWorkspace(_ workspace: SavedWorkspaceSnapshot) {
        savedWorkspaces.removeAll { $0.id == workspace.id }
        persistSavedWorkspaces()
    }

    public func toggleSavedWorkspaceFavorite(_ workspace: SavedWorkspaceSnapshot) {
        savedWorkspaces = savedWorkspaces.map { current in
            guard current.id == workspace.id else { return current }
            return SavedWorkspaceSnapshot(
                id: current.id,
                name: current.name,
                contextName: current.contextName,
                namespace: current.namespace,
                section: current.section,
                workloadKind: current.workloadKind,
                isFavorite: !current.isFavorite,
                resourceKind: current.resourceKind,
                resourceName: current.resourceName,
                resourceNamespace: current.resourceNamespace,
                logPresetID: current.logPresetID,
                logContainer: current.logContainer,
                includePreviousLogs: current.includePreviousLogs,
                isLogTailModeEnabled: current.isLogTailModeEnabled,
                isSidebarVisible: current.isSidebarVisible,
                isDetailPaneVisible: current.isDetailPaneVisible,
                inspectorState: current.inspectorState
            )
        }
        persistSavedWorkspaces()
    }

    public func openSavedWorkspace(_ workspace: SavedWorkspaceSnapshot) {
        invalidateDeferredSelectionRestores()
        prepareNavigationMutation(trackHistory: true)
        if let contextName = workspace.contextName,
           let context = state.contexts.first(where: { $0.name == contextName }) {
            setContext(context, preferredNamespace: workspace.namespace, trackHistory: false, triggerReload: false)
        }

        if !workspace.namespace.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            setNamespace(workspace.namespace, trackHistory: false, triggerReload: false)
        }

        setSection(workspace.section, trackHistory: false, triggerReload: false)
        setWorkloadKind(workspace.workloadKind, trackHistory: false, triggerReload: false)

        if let context = state.selectedContext {
            applyCachedSnapshot(context: context, namespace: state.selectedNamespace)
            scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: true, debounced: false)
        }
        restoreSavedWorkspaceResourceSelection(workspace)
        applySavedWorkspacePresentationState(workspace)
        recordNavigationCheckpoint()

        invalidateDeferredSelectionRestores()
        let restoreGeneration = deferredSelectionRestoreGeneration
        savedWorkspaceRestoreTask = Task { @MainActor [weak self] in
            do {
                try await Task.sleep(nanoseconds: 250_000_000)
            } catch {
                return
            }
            guard !Task.isCancelled,
                  let self,
                  self.deferredSelectionRestoreGeneration == restoreGeneration
            else {
                return
            }
            self.isRunningDeferredSelectionRestore = true
            defer {
                self.isRunningDeferredSelectionRestore = false
                if self.deferredSelectionRestoreGeneration == restoreGeneration {
                    self.savedWorkspaceRestoreTask = nil
                }
            }
            self.restoreSavedWorkspaceResourceSelection(workspace)
            self.applySavedWorkspacePresentationState(workspace)
        }
    }

    private func applySavedWorkspacePresentationState(_ workspace: SavedWorkspaceSnapshot) {
        if let logPresetID = workspace.logPresetID,
           let preset = PodLogPreset(rawValue: logPresetID) {
            selectedLogPreset = preset
        }
        if let includePreviousLogs = workspace.includePreviousLogs {
            self.includePreviousLogs = includePreviousLogs
        }
        if let logContainer = workspace.logContainer {
            selectedLogContainer = logContainer
        }
        if let isSidebarVisible = workspace.isSidebarVisible {
            self.isSidebarVisible = isSidebarVisible
            UserDefaults.standard.runeLayoutSidebarVisible = isSidebarVisible
        }
        if let isDetailPaneVisible = workspace.isDetailPaneVisible {
            self.isDetailPaneVisible = isDetailPaneVisible
            UserDefaults.standard.runeLayoutDetailPaneVisible = isDetailPaneVisible
        }
        if let isLogTailModeEnabled = workspace.isLogTailModeEnabled {
            self.isLogTailModeEnabled = isLogTailModeEnabled
        }
        if let inspectorState = workspace.inspectorState {
            savedWorkspaceInspectorRestoreRequest = SavedWorkspaceInspectorRestoreRequest(inspectorState: inspectorState)
        }
    }

    public func updateSavedWorkspaceInspectorState(_ inspectorState: SavedWorkspaceInspectorState) {
        currentSavedWorkspaceInspectorState = inspectorState
    }

    private func persistSavedWorkspaces() {
        let normalized = savedWorkspaces
        savedWorkspaceStore.saveSavedWorkspaces(normalized)
        savedWorkspaces = savedWorkspaceStore.loadSavedWorkspaces()
        if savedWorkspaces.isEmpty, !normalized.isEmpty {
            savedWorkspaces = normalized
        }
    }

    private func existingSavedWorkspaceFavorite(named name: String) -> Bool {
        savedWorkspaces.first {
            $0.name.caseInsensitiveCompare(name) == .orderedSame
        }?.isFavorite ?? false
    }

    private var selectedLogContainerName: String? {
        let trimmed = selectedLogContainer.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        guard podLogContainerOptions.contains(trimmed) else { return nil }
        return trimmed
    }

    private func sortedNamespaceOptions(_ rawOptions: [String]) -> [String] {
        var seen = Set<String>()
        let normalized = rawOptions
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty && seen.insert($0.lowercased()).inserted }

        guard !normalized.isEmpty else { return [] }

        return normalized.sorted { lhs, rhs in
            return lhs.localizedCaseInsensitiveCompare(rhs) == .orderedAscending
        }
    }

    private func favoriteSortedNamespaces(_ namespaces: [String]) -> [String] {
        namespaces
            .map { namespace in
                (namespace: namespace, isFavorite: isFavoriteNamespace(namespace))
            }
            .sorted { lhs, rhs in
                if lhs.isFavorite != rhs.isFavorite {
                    return lhs.isFavorite && !rhs.isFavorite
                }
                return lhs.namespace.localizedCaseInsensitiveCompare(rhs.namespace) == .orderedAscending
            }
            .map(\.namespace)
    }

    private func contextsIncludingEnabledDemo(_ contexts: [KubeContext]) -> [KubeContext] {
        guard UserDefaults.standard.runeEnableDemoCluster else {
            return contexts.filter { $0.name != demoContextName }
        }
        guard !contexts.contains(where: { $0.name == demoContextName }) else { return contexts }
        return contexts + [demoContext]
    }

    public var visibleContexts: [KubeContext] {
        let query = state.contextSearchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        let filtered = contextsIncludingEnabledDemo(state.contexts).filter { context in
            guard !query.isEmpty else { return true }
            return contextSearchTokens(for: context).contains { matches($0, query: query) }
        }

        return filtered.sorted { lhs, rhs in
            let leftFavorite = state.isFavorite(lhs)
            let rightFavorite = state.isFavorite(rhs)

            if leftFavorite != rightFavorite {
                return leftFavorite && !rightFavorite
            }

            return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
        }
    }

    public var contextMenuOptions: [KubeContext] {
        contextsIncludingEnabledDemo(state.contexts).sorted { lhs, rhs in
            let leftFavorite = state.isFavorite(lhs)
            let rightFavorite = state.isFavorite(rhs)

            if leftFavorite != rightFavorite {
                return leftFavorite && !rightFavorite
            }

            return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
        }
    }

    public func contextDisplayName(for context: KubeContext) -> String {
        contextPreferences.loadContextDisplayMetadata(for: context.name)?.alias ?? context.name
    }

    public func contextSecondaryDisplayText(for context: KubeContext) -> String? {
        guard let metadata = contextPreferences.loadContextDisplayMetadata(for: context.name) else { return nil }
        let parts = [
            metadata.group,
            metadata.tags.isEmpty ? nil : metadata.tags.joined(separator: ", ")
        ].compactMap { $0 }
        return parts.isEmpty ? nil : parts.joined(separator: " • ")
    }

    public func contextDisplayIconName(for context: KubeContext) -> String? {
        contextPreferences.loadContextDisplayMetadata(for: context.name)?.iconName
    }

    private func contextSearchTokens(for context: KubeContext) -> [String] {
        guard let metadata = contextPreferences.loadContextDisplayMetadata(for: context.name) else {
            return [context.name]
        }
        return [
            context.name,
            metadata.alias,
            metadata.colorKey,
            metadata.iconName,
            metadata.group
        ].compactMap { $0 } + metadata.tags
    }

    public var visiblePods: [PodSummary] {
        let values = filtered(state.pods) { pod in
            "\(pod.name) \(pod.status) \(pod.namespace) \(pod.ageDescription) \(pod.cpuDisplay) \(pod.memoryDisplay) \(pod.totalRestarts)"
        }
        return sortedPods(values)
    }

    public var visibleDeployments: [DeploymentSummary] {
        stablySorted(filtered(state.deployments) { deployment in
            "\(deployment.name) \(deployment.namespace) \(deployment.replicaText)"
        }, by: deploymentComparator)
    }

    public var visibleServices: [ServiceSummary] {
        stablySorted(filtered(state.services) { service in
            "\(service.name) \(service.namespace) \(service.type) \(service.clusterIP)"
        }, by: serviceComparator)
    }

    public var visibleStatefulSets: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.statefulSets) { summaryText(for: $0) })
    }

    public var visibleDaemonSets: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.daemonSets) { summaryText(for: $0) })
    }

    public var visibleJobs: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.jobs) { summaryText(for: $0) })
    }

    public var visibleCronJobs: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.cronJobs) { summaryText(for: $0) })
    }

    public var visibleReplicaSets: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.replicaSets) { summaryText(for: $0) })
    }

    public var visiblePersistentVolumeClaims: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.persistentVolumeClaims) { summaryText(for: $0) })
    }

    public var visiblePersistentVolumes: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.persistentVolumes) { summaryText(for: $0) })
    }

    public var visibleStorageClasses: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.storageClasses) { summaryText(for: $0) })
    }

    public var visibleHorizontalPodAutoscalers: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.horizontalPodAutoscalers) { summaryText(for: $0) })
    }

    public var visibleNetworkPolicies: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.networkPolicies) { summaryText(for: $0) })
    }

    public var visibleEndpoints: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.endpoints) { summaryText(for: $0) })
    }

    public var visibleIngresses: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.ingresses) { summaryText(for: $0) })
    }

    public var visibleConfigMaps: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.configMaps) { summaryText(for: $0) })
    }

    public var visibleSecrets: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.secrets) { summaryText(for: $0) })
    }

    public var visibleNodes: [ClusterResourceSummary] {
        genericResourceSorted(filtered(state.nodes) { summaryText(for: $0) })
    }

    public var visibleEvents: [EventSummary] {
        stablySorted(filtered(state.events) { event in
            "\(event.type) \(event.reason) \(event.objectName) \(event.message)"
        }, by: eventComparator)
    }

    public var visibleHelmReleases: [HelmReleaseSummary] {
        stablySorted(filtered(state.helmReleases) { release in
            "\(release.name) \(release.namespace) \(release.status) \(release.chart) \(release.appVersion)"
        }, by: helmReleaseComparator)
    }

    public var visibleOperatorResources: [OperatorResourceSummary] {
        operatorResourceSorted(filtered(focusedOperatorResources) { resource in
            let printerColumnText = resource.printerColumns
                .map { "\($0.title) \($0.value)" }
                .joined(separator: " ")
            return "\(resource.family) \(resource.kind) \(resource.name) \(resource.namespace ?? "") \(resource.status) \(resource.message) \(printerColumnText)"
        })
    }

    public var focusedOperatorResources: [OperatorResourceSummary] {
        switch operatorResourceFocus {
        case .all:
            return state.operatorResources
        case .gitOps:
            return state.operatorResources.filter(Self.isGitOpsOperatorResource)
        case .flux:
            return state.operatorResources.filter(Self.isFluxOperatorResource)
        case .argoCD:
            return state.operatorResources.filter(Self.isArgoCDOperatorResource)
        case .unhealthy:
            return state.operatorResources.filter(Self.isUnhealthyGitOpsOperatorResource)
        }
    }

    public var gitOpsOperatorResourceCount: Int {
        state.operatorResources.filter(Self.isGitOpsOperatorResource).count
    }

    public var fluxOperatorResourceCount: Int {
        state.operatorResources.filter(Self.isFluxOperatorResource).count
    }

    public var argoCDOperatorResourceCount: Int {
        state.operatorResources.filter(Self.isArgoCDOperatorResource).count
    }

    public var unhealthyGitOpsOperatorResourceCount: Int {
        state.operatorResources.filter(Self.isUnhealthyGitOpsOperatorResource).count
    }

    public var operatorResourceFocusSummary: String {
        switch operatorResourceFocus {
        case .all:
            return "\(state.operatorResources.count) operator resource\(state.operatorResources.count == 1 ? "" : "s")"
        case .gitOps:
            return "\(gitOpsOperatorResourceCount) GitOps resource\(gitOpsOperatorResourceCount == 1 ? "" : "s") • Flux \(fluxOperatorResourceCount) • ArgoCD \(argoCDOperatorResourceCount) • Unhealthy \(unhealthyGitOpsOperatorResourceCount)"
        case .flux:
            return "\(fluxOperatorResourceCount) Flux resource\(fluxOperatorResourceCount == 1 ? "" : "s") • Unhealthy \(state.operatorResources.filter { Self.isFluxOperatorResource($0) && Self.isUnhealthyGitOpsOperatorResource($0) }.count)"
        case .argoCD:
            return "\(argoCDOperatorResourceCount) ArgoCD resource\(argoCDOperatorResourceCount == 1 ? "" : "s") • Unhealthy \(state.operatorResources.filter { Self.isArgoCDOperatorResource($0) && Self.isUnhealthyGitOpsOperatorResource($0) }.count)"
        case .unhealthy:
            return "\(unhealthyGitOpsOperatorResourceCount) unhealthy GitOps resource\(unhealthyGitOpsOperatorResourceCount == 1 ? "" : "s")"
        }
    }

    public var pagedOperatorResources: [OperatorResourceSummary] {
        let resources = visibleOperatorResources
        guard !resources.isEmpty else { return [] }
        let start = clampedOperatorResourcePage(forResourceCount: resources.count) * Self.operatorResourcePageSize
        return Array(resources.dropFirst(start).prefix(Self.operatorResourcePageSize))
    }

    public var operatorResourcePageSummary: String {
        let count = visibleOperatorResources.count
        guard count > 0 else { return "0 resources" }
        let start = clampedOperatorResourcePage(forResourceCount: count) * Self.operatorResourcePageSize + 1
        let end = min(start + Self.operatorResourcePageSize - 1, count)
        return "\(start)-\(end) of \(count)"
    }

    public var canPageOperatorResourcesBackward: Bool {
        clampedOperatorResourcePage(forResourceCount: visibleOperatorResources.count) > 0
    }

    public var canPageOperatorResourcesForward: Bool {
        let count = visibleOperatorResources.count
        return (clampedOperatorResourcePage(forResourceCount: count) + 1) * Self.operatorResourcePageSize < count
    }

    public var isProductionContext: Bool {
        guard let context = state.selectedContext else {
            return false
        }

        return isProductionContext(context)
    }

    public var pendingWriteActionTitle: String {
        guard let pendingWriteAction else { return "Confirm write action" }
        return pendingWriteAction.title
    }

    public var pendingWriteActionMessage: String {
        guard let pendingWriteAction else { return "" }
        var message = pendingWriteAction.message
        if let pendingWriteDryRunStatus, !pendingWriteDryRunStatus.isEmpty {
            message += "\n\nServer dry-run:\n\(pendingWriteDryRunStatus)"
        }
        if let pendingRollbackPlan, !pendingRollbackPlan.isEmpty {
            message += "\n\nRollback plan:\n\(pendingRollbackPlan)"
        }
        if state.isReadOnlyMode {
            return "READ-ONLY MODE: turn off read-only before running write actions."
        }
        if pendingWriteScopeSnapshot?.isProduction == true {
            if pendingWriteAction.isDestructive {
                guard requiresProductionSecondConfirmation else {
                    return "PRODUCTION CONTEXT: \(message)"
                }
                if pendingProductionDestructiveConfirmation == pendingWriteAction,
                   pendingProductionDestructiveConfirmationScopeID == pendingWriteScopeSnapshot?.id {
                    return "PRODUCTION CONTEXT: Final confirmation required. \(message)"
                }
                return "PRODUCTION CONTEXT: \(message)\n\nDestructive production actions require a second confirmation before Rune sends anything to Kubernetes."
            }
            return "PRODUCTION CONTEXT: \(message)"
        }
        return message
    }

    public var pendingWriteActionKubectlCommand: String {
        guard UserDefaults.standard.runeWriteSafetyRequireCopyableCommand else { return "" }
        guard let pendingWriteAction,
              let scope = pendingWriteScopeSnapshot
        else { return "" }
        return pendingWriteAction.kubectlCommand(contextName: scope.context.name, namespace: scope.namespace)
    }

    public var pendingWriteActionConfirmLabel: String {
        guard let pendingWriteAction else { return "Confirm" }
        if pendingWriteScopeSnapshot?.isProduction == true,
           pendingWriteAction.isDestructive,
           requiresProductionSecondConfirmation,
           (pendingProductionDestructiveConfirmation != pendingWriteAction
               || pendingProductionDestructiveConfirmationScopeID != pendingWriteScopeSnapshot?.id)
        {
            return "Review Production Action"
        }
        return pendingWriteAction.confirmLabel
    }

    public var pendingWriteActionIsDestructive: Bool {
        pendingWriteAction?.isDestructive ?? false
    }

    public var visibleWriteAuditEntries: [WriteAuditEntry] {
        let query = writeAuditSearchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else { return state.writeAuditLog }
        return state.writeAuditLog.filter { entry in
            matches(
                "\(entry.action) \(entry.contextName) \(entry.namespace) \(entry.resource) \(entry.status) \(entry.message)",
                query: query
            )
        }
    }

    public func copyPendingWriteActionKubectlCommand() {
        let command = pendingWriteActionKubectlCommand
        copyCommandToPasteboard(command)
    }

    public func bootstrapIfNeeded() {
        guard !hasBootstrapped else { return }
        hasBootstrapped = true
        bootstrap()
    }

    public func bootstrap() {
        bootstrapTask?.cancel()
        let generation = clusterLoadGeneration
        bootstrapTask = Task { @MainActor [weak self] in
            guard let self else { return }
            await Task.yield()
            guard self.isCurrentClusterLoad(generation), !Task.isCancelled else { return }
            do {
                let discoveredURLs = self.kubeConfigDiscoverer.discoverCandidateFiles()
                guard self.isCurrentClusterLoad(generation), !Task.isCancelled else { return }
                self.diagnostics.log("bootstrap discovered kubeconfig files: \(discoveredURLs.map(\.path).joined(separator: ", "))")
                await Task.yield()
                guard self.isCurrentClusterLoad(generation), !Task.isCancelled else { return }

                let sources = try self.resolvedKubeConfigSources(
                    fallbackURLs: discoveredURLs
                )
                guard self.isCurrentClusterLoad(generation), !Task.isCancelled else { return }

                if self.state.kubeConfigSources != sources {
                    self.state.setSources(sources)
                }
                self.latestKubeConfigSourceFingerprint = self.kubeConfigSourceFingerprint(for: sources)
                self.diagnostics.log("bootstrap resolved sources count=\(sources.count)")
                self.startKubeConfigSourceSync()

                guard !sources.isEmpty else {
                    self.clearClusterDataForEmptyBootstrapIfNeeded()
                    if !self.state.contexts.isEmpty {
                        self.state.setContexts([])
                    }
                    await self.finishLaunchExperience()
                    return
                }

                self.persistDiscoveredKubeConfigsInBackground(discoveredURLs)
                try await self.reloadContexts(
                    loadInitialSnapshotSynchronously: false,
                    showsLoadingIndicator: false,
                    expectedClusterLoadGeneration: generation
                )
            } catch {
                guard self.isCurrentClusterLoad(generation), !Task.isCancelled else { return }
                self.diagnostics.log("bootstrap failed: \(error.localizedDescription)")
                self.state.setError(error)
            }
            guard self.isCurrentClusterLoad(generation), !Task.isCancelled else { return }
            await self.finishLaunchExperience()
        }
    }

    private func isCurrentClusterLoad(_ generation: UUID) -> Bool {
        clusterLoadGeneration == generation
    }

    private func finishLaunchExperience() async {
        guard isLaunchExperienceVisible else { return }
        let elapsed = launchExperienceStartedAt.duration(to: .now)
        let elapsedNanoseconds = UInt64(elapsed.components.seconds) * 1_000_000_000
            + UInt64(elapsed.components.attoseconds / 1_000_000_000)
        if elapsedNanoseconds < launchExperienceMinimumNanoseconds {
            try? await Task.sleep(nanoseconds: launchExperienceMinimumNanoseconds - elapsedNanoseconds)
        }
        isLaunchExperienceVisible = false
    }

    private func clearClusterDataForEmptyBootstrapIfNeeded() {
        let hasClusterData = !state.contexts.isEmpty
            || !state.namespaces.isEmpty
            || !state.pods.isEmpty
            || !state.deployments.isEmpty
            || !state.statefulSets.isEmpty
            || !state.daemonSets.isEmpty
            || !state.jobs.isEmpty
            || !state.cronJobs.isEmpty
            || !state.replicaSets.isEmpty
            || !state.persistentVolumeClaims.isEmpty
            || !state.persistentVolumes.isEmpty
            || !state.storageClasses.isEmpty
            || !state.horizontalPodAutoscalers.isEmpty
            || !state.networkPolicies.isEmpty
            || !state.services.isEmpty
            || !state.endpoints.isEmpty
            || !state.ingresses.isEmpty
            || !state.configMaps.isEmpty
            || !state.secrets.isEmpty
            || !state.serviceAccounts.isEmpty
            || !state.nodes.isEmpty
            || !state.events.isEmpty
            || !state.overviewPods.isEmpty
            || state.overviewDeploymentsCount != 0
            || state.overviewServicesCount != 0
            || state.overviewIngressesCount != 0
            || state.overviewConfigMapsCount != 0
            || state.overviewCronJobsCount != 0
            || state.overviewNodesCount != 0
            || !state.overviewEvents.isEmpty

        if hasClusterData {
            state.setContexts([])
            state.setNamespaces([])
            state.setPods([])
            state.setDeployments([])
            state.setStatefulSets([])
            state.setDaemonSets([])
            state.setJobs([])
            state.setCronJobs([])
            state.setReplicaSets([])
            state.setPersistentVolumeClaims([])
            state.setPersistentVolumes([])
            state.setStorageClasses([])
            state.setHorizontalPodAutoscalers([])
            state.setNetworkPolicies([])
            state.setServices([])
            state.setEndpoints([])
            state.setIngresses([])
            state.setConfigMaps([])
            state.setSecrets([])
            state.setRBACData(roles: [], serviceAccounts: [], roleBindings: [], clusterRoles: [], clusterRoleBindings: [])
            rbacDataScope = nil
            state.setNodes([])
            state.setEvents([])
            state.clearResourceListFreshness()
            state.setOverviewSnapshot(
                pods: [],
                deploymentsCount: 0,
                servicesCount: 0,
                ingressesCount: 0,
                configMapsCount: 0,
                cronJobsCount: 0,
                nodesCount: 0,
                events: []
            )
        }

        let hasResourceDetails = !state.podLogs.isEmpty
            || !state.unifiedServiceLogs.isEmpty
            || !state.unifiedServiceLogPods.isEmpty
            || !state.resourceYAML.isEmpty
            || !state.resourceYAMLBaseline.isEmpty
            || !state.resourceYAMLValidationIssues.isEmpty
            || state.isValidatingResourceYAML
            || !state.resourceDescribe.isEmpty
            || state.lastResourceYAMLError != nil
            || state.lastResourceDescribeError != nil
            || !state.deploymentRolloutHistory.isEmpty
            || !state.helmValues.isEmpty
            || !state.helmManifest.isEmpty
            || !state.helmHistory.isEmpty
            || state.isLoadingLogs
            || state.isLoadingResourceDetails
            || state.lastLogFetchError != nil

        if hasResourceDetails {
            state.clearResourceDetails()
        }

        if state.lastError != nil {
            state.clearError()
        }
    }

    public func importKubeConfig() {
        guard beginKubeConfigImportPreparation() else { return }
        Task {
            defer { isPreparingKubeConfigImport = false }
            do {
                let files = try picker.pickFiles()
                guard !files.isEmpty else { return }
                for file in files {
                    pendingKubeConfigSourceAccess.retainAccess(to: file)
                }

                let payloads = try files.map { file in
                    (
                        raw: try String(contentsOf: file, encoding: .utf8),
                        sourceName: file.lastPathComponent,
                        sourceURL: Optional(file)
                    )
                }
                try await importKubeConfigPayloads(
                    payloads,
                    logLabel: "importKubeConfig",
                    securityScopedURLs: files
                )
            } catch {
                pendingKubeConfigSourceAccess.releaseAll()
                diagnostics.log("importKubeConfig failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func importKubeConfigFromPasteboard() {
        guard let raw = NSPasteboard.general.string(forType: .string),
              !raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            state.setError(RuneError.invalidInput(message: "Pasteboard does not contain kubeconfig YAML."))
            return
        }

        importKubeConfig(raw: raw, sourceName: "pasted-kubeconfig.yaml")
    }

    public func importKubeConfigFolder() {
        guard beginKubeConfigImportPreparation() else { return }
        Task {
            defer { isPreparingKubeConfigImport = false }
            do {
                guard let folder = try picker.pickFolder() else { return }
                pendingKubeConfigSourceAccess.retainAccess(to: folder)
                let files = try kubeConfigFiles(in: folder)
                guard !files.isEmpty else {
                    throw RuneError.invalidInput(message: "No kubeconfig files were found in \(folder.lastPathComponent).")
                }

                let payloads = try files.map { file in
                    (
                        raw: try String(contentsOf: file, encoding: .utf8),
                        sourceName: file.lastPathComponent,
                        sourceURL: Optional(file)
                    )
                }
                try await importKubeConfigPayloads(
                    payloads,
                    logLabel: "importKubeConfigFolder",
                    securityScopedURLs: [folder]
                )
            } catch {
                pendingKubeConfigSourceAccess.releaseAll()
                diagnostics.log("importKubeConfigFolder failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func importKubeConfig(raw: String, sourceName: String = "pasted-kubeconfig.yaml") {
        guard beginKubeConfigImportPreparation() else { return }
        Task {
            defer { isPreparingKubeConfigImport = false }
            do {
                try await importKubeConfigPayloads(
                    [(raw: raw, sourceName: sourceName, sourceURL: nil)],
                    logLabel: "importKubeConfigPaste"
                )
            } catch {
                pendingKubeConfigSourceAccess.releaseAll()
                diagnostics.log("importKubeConfigPaste failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func importManualTokenKubeConfig() {
        guard beginKubeConfigImportPreparation() else { return }
        let request = ManualTokenKubeConfigRequest(
            name: manualKubeConfigName,
            server: manualKubeConfigServer,
            namespace: manualKubeConfigNamespace,
            token: manualKubeConfigToken
        )
        clearManualKubeConfigSecret()
        Task {
            defer { isPreparingKubeConfigImport = false }
            do {
                let raw = try ManualTokenKubeConfigBuilder.buildYAML(for: request)
                try await importKubeConfigPayloads(
                    [(raw: raw, sourceName: "manual-token-kubeconfig.yaml", sourceURL: nil)],
                    logLabel: "importManualTokenKubeConfig"
                )
            } catch {
                pendingKubeConfigSourceAccess.releaseAll()
                diagnostics.log("importManualTokenKubeConfig failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func clearManualKubeConfigSecret() {
        manualKubeConfigToken = ""
    }

    private func beginKubeConfigImportPreparation() -> Bool {
        guard !isPreparingKubeConfigImport,
              !isCommittingKubeConfigImport,
              !isRunningCloudKubeConfigImport else {
            state.setError(RuneError.invalidInput(
                message: "Wait for the current kubeconfig import to finish before starting another import."
            ))
            return false
        }
        isPreparingKubeConfigImport = true
        return true
    }

    public func cloudKubeConfigCommandPreview(for request: CloudKubeConfigImportRequest) -> String {
        do {
            return try cloudKubeConfigImporter.commandPreview(for: request).displayCommand
        } catch {
            return error.localizedDescription
        }
    }

    public func runCloudKubeConfigImport(_ request: CloudKubeConfigImportRequest) {
        guard !isRunningCloudKubeConfigImport else { return }
        guard !isPreparingKubeConfigImport,
              !isCommittingKubeConfigImport,
              !isKubeConfigImportConfirmationPending else {
            state.setError(RuneError.invalidInput(
                message: "Finish or cancel the current kubeconfig import before running a provider import."
            ))
            return
        }
        isRunningCloudKubeConfigImport = true
        cloudKubeConfigImportDiagnostic = nil
        cloudKubeConfigImportOutput = ""
        if state.lastError != nil {
            state.clearError()
        }
        cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.runningStatus(for: request.provider)
        appendCloudKubeConfigImportOutput("$ \(cloudKubeConfigCommandPreview(for: request))\n")
        Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.isRunningCloudKubeConfigImport = false }
            var temporaryDirectory: URL?
            do {
                let isolated = try self.isolatedCloudKubeConfigImportRequest(request)
                temporaryDirectory = isolated.directory
                let result = try await self.cloudKubeConfigImporter.importCluster(
                    isolated.request,
                    onOutput: { [weak self] chunk in
                        Task { @MainActor [weak self] in
                            self?.appendCloudKubeConfigImportOutput(chunk.text)
                        }
                    }
                )
                if let failure = AddClusterCloudImportWorkflow.blockingFailure(in: result.reviews) {
                    self.discardPendingKubeConfigImport(clearReview: false)
                    self.kubeConfigImportReviews = result.reviews
                    self.kubeConfigImportReviewMode = .report
                    self.syncKubeConfigImportContextMetadataDrafts(from: result.reviews)
                    self.state.setAuthDoctorChecks(failure.checks)
                    let error = RuneError.invalidInput(message: failure.message)
                    self.cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.failedStatus()
                    self.cloudKubeConfigImportDiagnostic = nil
                    self.diagnostics.log("cloud kubeconfig import blocked by review: \(error.localizedDescription)")
                    self.state.setError(error)
                    if let temporaryDirectory {
                        try? FileManager.default.removeItem(at: temporaryDirectory)
                    }
                    return
                }

                guard !result.discoveredURLs.isEmpty else {
                    throw CloudKubeConfigImportError.noKubeconfigDiscovered(command: result.command.displayCommand)
                }
                let payloads = try result.discoveredURLs.map { url in
                    (
                        raw: try String(contentsOf: url, encoding: .utf8),
                        sourceName: url.lastPathComponent,
                        sourceURL: Optional(url)
                    )
                }
                try await self.importKubeConfigPayloads(
                    payloads,
                    logLabel: "runCloudKubeConfigImport",
                    temporaryDirectories: temporaryDirectory.map { [$0] } ?? []
                )
                temporaryDirectory = nil
                self.pendingCloudKubeConfigProvider = request.provider
                self.cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.readyForReviewStatus(for: request.provider)
                self.cloudKubeConfigImportDiagnostic = nil
            } catch {
                if let temporaryDirectory {
                    try? FileManager.default.removeItem(at: temporaryDirectory)
                }
                self.cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.failedStatus()
                self.cloudKubeConfigImportDiagnostic = AddClusterCloudImportWorkflow.diagnostic(for: error, provider: request.provider)
                self.state.setAuthDoctorChecks(AddClusterCloudImportWorkflow.cloudLoginFailureChecks(for: request.provider))
                self.diagnostics.log("cloud kubeconfig import failed: \(error.localizedDescription)")
                self.state.setError(error)
            }
        }
    }

    public func runNativeEKSClusterImport(
        clusterName: String,
        region: String,
        accessKeyID: String,
        secretAccessKey: String,
        sessionToken: String = ""
    ) {
        guard admitNativeCloudClusterImport() else { return }
        do {
            let request = AWSEKSClusterImportRequest(
                clusterName: clusterName,
                region: region
            )
            let credentials = try AWSEKSCredentials(
                accessKeyID: accessKeyID,
                secretAccessKey: secretAccessKey,
                sessionToken: sessionToken.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                    ? nil
                    : sessionToken
            )
            let importer = nativeCloudClusterImporter
            beginNativeCloudClusterImport(
                provider: .eks,
                credential: .eks(credentials: credentials)
            ) {
                try await importer.importEKS(request, credentials: credentials)
            }
        } catch {
            failNativeCloudClusterImport(error, provider: .eks)
        }
    }

    public func runNativeAKSClusterImport(
        subscriptionID: String,
        resourceGroup: String,
        clusterName: String,
        tenantID: String,
        clientID: String,
        clientSecret: String
    ) {
        guard admitNativeCloudClusterImport() else { return }
        do {
            let request = try AKSNativeClusterImportRequest(
                subscriptionID: subscriptionID,
                resourceGroup: resourceGroup,
                clusterName: clusterName,
                tenantID: tenantID,
                clientID: clientID
            )
            let importer = nativeCloudClusterImporter
            beginNativeCloudClusterImport(
                provider: .aks,
                credential: .aks(clientSecret: clientSecret)
            ) {
                try await importer.importAKS(request, clientSecret: clientSecret)
            }
        } catch {
            failNativeCloudClusterImport(error, provider: .aks)
        }
    }

    public func chooseAndRunNativeGKEClusterImport(
        projectID: String,
        location: String,
        clusterName: String
    ) {
        guard admitNativeCloudClusterImport() else { return }
        let request = GKENativeClusterImportRequest(
            projectID: projectID,
            location: location,
            clusterName: clusterName
        )

        isConnectingNativeKubernetesAuth = true
        isRunningCloudKubeConfigImport = true
        isRunningNativeCloudClusterImport = true
        cloudKubeConfigImportDiagnostic = nil
        cloudKubeConfigImportOutput = ""
        if state.lastError != nil { state.clearError() }
        cloudKubeConfigImportStatus = "Choose a Google service-account JSON file…"
        nativeKubernetesAuthStatus = "Choose a Google service-account JSON file…"
        let reservation = UUID()
        nativeGKEImportPickerReservation = reservation
        gkeCredentialFilePicker.beginSelection { [weak self] selection in
            guard let self else { return }
            guard self.releaseNativeGKEImportPickerReservation(reservation) else { return }
            switch selection {
            case .cancelled:
                self.cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.nativeCancelledStatus(for: .gke)
                self.cloudKubeConfigImportDiagnostic = nil
                self.nativeKubernetesAuthStatus = nil
            case .selected(let data):
                let importer = self.nativeCloudClusterImporter
                self.beginNativeCloudClusterImport(
                    provider: .gke,
                    credential: .gke(serviceAccountJSON: data)
                ) {
                    try await importer.importGKE(request, serviceAccountJSON: data)
                }
            case .failed(let error):
                self.failNativeCloudClusterImport(error, provider: .gke)
            }
        }
    }

    public func cancelNativeCloudClusterImport() {
        if let reservation = nativeGKEImportPickerReservation {
            guard releaseNativeGKEImportPickerReservation(reservation) else { return }
            gkeCredentialFilePicker.cancelSelection()
            cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.nativeCancelledStatus(for: .gke)
            cloudKubeConfigImportDiagnostic = nil
            nativeKubernetesAuthStatus = nil
            return
        }
        guard isRunningNativeCloudClusterImport else { return }
        cloudKubeConfigImportStatus = "Cancelling native cluster import…"
        nativeKubernetesAuthStatus = nil
        nativeCloudClusterImportTask?.cancel()
    }

    private func admitNativeCloudClusterImport() -> Bool {
        guard !isRunningCloudKubeConfigImport,
              !isRunningNativeCloudClusterImport,
              !isConnectingNativeKubernetesAuth,
              nativeGKEImportPickerReservation == nil else {
            return false
        }
        guard !isPreparingKubeConfigImport,
              !isCommittingKubeConfigImport,
              !isKubeConfigImportConfirmationPending else {
            state.setError(RuneError.invalidInput(
                message: "Finish or cancel the current kubeconfig import before importing another cluster."
            ))
            return false
        }
        return true
    }

    private func releaseNativeGKEImportPickerReservation(_ reservation: UUID) -> Bool {
        guard nativeGKEImportPickerReservation == reservation else { return false }
        nativeGKEImportPickerReservation = nil
        isConnectingNativeKubernetesAuth = false
        isRunningNativeCloudClusterImport = false
        isRunningCloudKubeConfigImport = false
        return true
    }

    private func beginNativeCloudClusterImport(
        provider: CloudKubeConfigProvider,
        credential: PendingNativeCloudCredential,
        operation: @escaping @Sendable () async throws -> NativeCloudClusterImportResult
    ) {
        guard admitNativeCloudClusterImport() else { return }
        isRunningCloudKubeConfigImport = true
        isRunningNativeCloudClusterImport = true
        pendingNativeCloudCredential = nil
        cloudKubeConfigImportDiagnostic = nil
        cloudKubeConfigImportOutput = ""
        if state.lastError != nil { state.clearError() }
        cloudKubeConfigImportStatus = "Connecting securely to the cloud provider…"

        nativeCloudClusterImportTask = Task { @MainActor [weak self] in
            guard let self else { return }
            defer {
                self.nativeCloudClusterImportTask = nil
                self.isRunningNativeCloudClusterImport = false
                self.isRunningCloudKubeConfigImport = false
            }
            do {
                let result = try await operation()
                try Task.checkCancellation()
                guard result.provider == provider else {
                    throw RuneError.invalidInput(message: "The cloud provider returned an unexpected import result.")
                }
                try await self.importKubeConfigPayloads(
                    [(raw: result.rawKubeConfig, sourceName: result.sourceName, sourceURL: nil)],
                    logLabel: "runNativeCloudClusterImport"
                )
                try Task.checkCancellation()
                self.pendingNativeCloudCredential = credential
                self.pendingCloudKubeConfigProvider = provider
                self.cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.nativeReadyForReviewStatus(for: provider)
                self.cloudKubeConfigImportDiagnostic = nil
                self.nativeKubernetesAuthStatus = "Cluster access is ready for review. Credentials remain in memory until you confirm."
            } catch is CancellationError {
                self.discardPendingKubeConfigImport(clearReview: true)
                self.cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.nativeCancelledStatus(for: provider)
                self.cloudKubeConfigImportDiagnostic = nil
                self.nativeKubernetesAuthStatus = nil
            } catch {
                self.pendingNativeCloudCredential = nil
                self.failNativeCloudClusterImport(error, provider: provider)
            }
        }
    }

    private func failNativeCloudClusterImport(
        _ error: Error,
        provider: CloudKubeConfigProvider
    ) {
        let diagnostic = AddClusterCloudImportWorkflow.nativeDiagnostic(
            for: error,
            provider: provider
        )
        cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.failedStatus()
        cloudKubeConfigImportDiagnostic = diagnostic
        nativeKubernetesAuthStatus = "Native cluster import could not be completed."
        state.setAuthDoctorChecks(
            AddClusterCloudImportWorkflow.nativeImportFailureChecks(
                for: provider,
                diagnostic: diagnostic
            )
        )
        diagnostics.log("native cloud import failed provider=\(provider.rawValue) classification=\(diagnostic.classification)")
        state.setError(NativeCloudImportPresentationError(
            message: "\(diagnostic.title). \(diagnostic.message)"
        ))
    }

    private func isolatedCloudKubeConfigImportRequest(
        _ request: CloudKubeConfigImportRequest
    ) throws -> (request: CloudKubeConfigImportRequest, directory: URL) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneCloudImport.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: false,
            attributes: [.posixPermissions: 0o700]
        )
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: directory.path)
        let target = directory.appendingPathComponent("config", isDirectory: false).path
        return (
            CloudKubeConfigImportRequest(
                provider: request.provider,
                clusterName: request.clusterName,
                regionOrLocation: request.regionOrLocation,
                resourceGroup: request.resourceGroup,
                projectID: request.projectID,
                profileOrSubscription: request.profileOrSubscription,
                roleARN: request.roleARN,
                targetKubeconfigPath: target,
                overwriteExisting: request.overwriteExisting
            ),
            directory
        )
    }

    public func clearCloudKubeConfigImportStatus() {
        guard !isRunningCloudKubeConfigImport else { return }
        cloudKubeConfigImportStatus = nil
        cloudKubeConfigImportDiagnostic = nil
        cloudKubeConfigImportOutput = ""
    }

    private var canStartNativeCredentialOperation: Bool {
        !isConnectingNativeKubernetesAuth
            && !isRunningNativeCloudClusterImport
            && nativeCloudClusterImportTask == nil
            && nativeGKEImportPickerReservation == nil
            && nativeGKECredentialConnectPickerReservation == nil
    }

    /// Binds explicit AWS IAM/session credentials to the selected imported EKS context.
    /// Secret material is written to Keychain by the native auth provider and is never
    /// written back to kubeconfig or diagnostics.
    public func connectSelectedEKSNativeAuth(
        accessKeyID: String,
        secretAccessKey: String,
        sessionToken: String = "",
        expiration: Date? = nil
    ) {
        guard canStartNativeCredentialOperation else { return }
        do {
            let request = try selectedNativeCredentialRequest(expectedProvider: .awsEKS)
            connectEKSNativeAuth(
                request: request,
                accessKeyID: accessKeyID,
                secretAccessKey: secretAccessKey,
                sessionToken: sessionToken,
                expiration: expiration
            )
        } catch {
            nativeKubernetesAuthStatus = "AWS native authentication could not be connected."
            diagnostics.log("native AWS auth context selection failed: \(error.localizedDescription)")
            state.setError(error)
        }
    }

    /// Binds AWS credentials to one explicit EKS context. The provider check happens
    /// before secret values are validated, persisted, or passed to a provider.
    public func connectEKSNativeAuth(
        request: KubernetesNativeCredentialRequest,
        accessKeyID: String,
        secretAccessKey: String,
        sessionToken: String = "",
        expiration: Date? = nil
    ) {
        guard canStartNativeCredentialOperation else { return }
        do {
            try validateNativeCredentialRequest(request, expectedProvider: .awsEKS)
        } catch {
            nativeKubernetesAuthStatus = "AWS native authentication could not be connected."
            diagnostics.log("native AWS auth request validation failed: \(error.localizedDescription)")
            state.setError(error)
            return
        }
        let accessKeyID = accessKeyID.trimmingCharacters(in: .whitespacesAndNewlines)
        let secretAccessKey = secretAccessKey.trimmingCharacters(in: .whitespacesAndNewlines)
        let sessionToken = sessionToken.trimmingCharacters(in: .whitespacesAndNewlines)
        isConnectingNativeKubernetesAuth = true
        nativeKubernetesAuthStatus = "Connecting AWS credentials…"
        if state.lastError != nil { state.clearError() }

        Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.isConnectingNativeKubernetesAuth = false }
            var profileWasSaved = false
            do {
                let credentials = try AWSEKSCredentials(
                    accessKeyID: accessKeyID,
                    secretAccessKey: secretAccessKey,
                    sessionToken: sessionToken.isEmpty ? nil : sessionToken,
                    expiration: expiration
                )
                try await self.nativeAuthConfigurator.bindAWSCredentials(
                    to: request,
                    credentials: credentials,
                    displayName: "AWS EKS"
                )
                profileWasSaved = true
                self.nativeKubernetesAuthStatus = "AWS credentials connected. Verifying the selected context…"
                try await self.reloadContexts()
                self.nativeKubernetesAuthStatus = "AWS native authentication is connected."
                if !UserDefaults.standard.runeSimpleMode {
                    self.runAuthDoctor()
                }
            } catch {
                self.nativeKubernetesAuthStatus = profileWasSaved
                    ? "AWS credentials were saved, but the selected context could not be verified. Run Auth Doctor and try again."
                    : "AWS native authentication could not be connected."
                self.diagnostics.log("native AWS auth connection failed: \(error.localizedDescription)")
                self.state.setError(error)
            }
        }
    }

    /// Binds an Azure service-principal secret to a selected kubelogin context whose
    /// non-secret tenant, audience, and client identifiers remain in kubeconfig.
    public func connectSelectedAKSNativeAuth(clientSecret: String) {
        guard canStartNativeCredentialOperation else { return }
        do {
            let request = try selectedNativeCredentialRequest(expectedProvider: .azureKubelogin)
            connectAKSNativeAuth(request: request, clientSecret: clientSecret)
        } catch {
            nativeKubernetesAuthStatus = "Azure native authentication could not be connected."
            diagnostics.log("native Azure auth context selection failed: \(error.localizedDescription)")
            state.setError(error)
        }
    }

    /// Binds an Azure service-principal secret to one explicit kubelogin context.
    public func connectAKSNativeAuth(
        request: KubernetesNativeCredentialRequest,
        clientSecret: String
    ) {
        guard canStartNativeCredentialOperation else { return }
        do {
            try validateNativeCredentialRequest(request, expectedProvider: .azureKubelogin)
        } catch {
            nativeKubernetesAuthStatus = "Azure native authentication could not be connected."
            diagnostics.log("native Azure auth request validation failed: \(error.localizedDescription)")
            state.setError(error)
            return
        }
        isConnectingNativeKubernetesAuth = true
        nativeKubernetesAuthStatus = "Connecting Azure service principal…"
        if state.lastError != nil { state.clearError() }

        Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.isConnectingNativeKubernetesAuth = false }
            var profileWasSaved = false
            do {
                try await self.nativeAuthConfigurator.bindAKSServicePrincipal(
                    to: request,
                    clientSecret: clientSecret,
                    displayName: "Azure AKS"
                )
                profileWasSaved = true
                self.nativeKubernetesAuthStatus = "Azure credentials connected. Verifying the selected context…"
                try await self.reloadContexts()
                self.nativeKubernetesAuthStatus = "Azure native authentication is connected."
                if !UserDefaults.standard.runeSimpleMode {
                    self.runAuthDoctor()
                }
            } catch {
                self.nativeKubernetesAuthStatus = profileWasSaved
                    ? "Azure credentials were saved, but the selected context could not be verified. Run Auth Doctor and try again."
                    : "Azure native authentication could not be connected."
                self.diagnostics.log("native Azure auth connection failed: \(error.localizedDescription)")
                self.state.setError(error)
            }
        }
    }

    /// Opens a user-mediated App Sandbox file panel, reads one bounded service-account
    /// document, and immediately moves it into Keychain through the native provider.
    public func chooseAndConnectSelectedGKENativeAuth() {
        guard canStartNativeCredentialOperation else { return }
        do {
            let request = try selectedNativeCredentialRequest(expectedProvider: .googleGKE)
            chooseAndConnectGKENativeAuth(request: request)
        } catch {
            nativeKubernetesAuthStatus = "Select a compatible imported Google GKE context first."
            state.setError(error)
        }
    }

    /// Opens the service-account picker for one explicit GKE request. Provider
    /// validation precedes panel construction so mismatched sheets cannot prompt.
    public func chooseAndConnectGKENativeAuth(request: KubernetesNativeCredentialRequest) {
        guard canStartNativeCredentialOperation else { return }
        do {
            try validateNativeCredentialRequest(request, expectedProvider: .googleGKE)
        } catch {
            nativeKubernetesAuthStatus = "Google native authentication could not be connected."
            diagnostics.log("native Google auth picker request validation failed: \(error.localizedDescription)")
            state.setError(error)
            return
        }
        isConnectingNativeKubernetesAuth = true
        nativeKubernetesAuthStatus = "Choose a Google service-account JSON file…"
        let reservation = UUID()
        nativeGKECredentialConnectPickerReservation = reservation
        gkeCredentialFilePicker.beginSelection { [weak self] selection in
            guard let self else { return }
            guard self.releaseNativeGKECredentialConnectPickerReservation(reservation) else {
                return
            }
            switch selection {
            case .cancelled:
                self.nativeKubernetesAuthStatus = nil
            case .selected(let data):
                self.connectGKENativeAuth(request: request, serviceAccountJSON: data)
            case .failed(let error):
                self.nativeKubernetesAuthStatus = "Google native authentication could not be connected."
                self.state.setError(error)
            }
        }
    }

    private func releaseNativeGKECredentialConnectPickerReservation(
        _ reservation: UUID
    ) -> Bool {
        guard nativeGKECredentialConnectPickerReservation == reservation else { return false }
        nativeGKECredentialConnectPickerReservation = nil
        isConnectingNativeKubernetesAuth = false
        return true
    }

    /// Validates and binds a Google service-account document to the selected imported
    /// GKE context. The document is persisted only as a Keychain secret.
    public func connectSelectedGKENativeAuth(serviceAccountJSON: Data) {
        guard canStartNativeCredentialOperation else { return }
        do {
            let request = try selectedNativeCredentialRequest(expectedProvider: .googleGKE)
            connectGKENativeAuth(request: request, serviceAccountJSON: serviceAccountJSON)
        } catch {
            nativeKubernetesAuthStatus = "Google native authentication could not be connected."
            diagnostics.log("native Google auth context selection failed: \(error.localizedDescription)")
            state.setError(error)
        }
    }

    /// Binds a Google service-account document to one explicit GKE context.
    public func connectGKENativeAuth(
        request: KubernetesNativeCredentialRequest,
        serviceAccountJSON: Data
    ) {
        guard canStartNativeCredentialOperation else { return }
        do {
            try validateNativeCredentialRequest(request, expectedProvider: .googleGKE)
        } catch {
            nativeKubernetesAuthStatus = "Google native authentication could not be connected."
            diagnostics.log("native Google auth request validation failed: \(error.localizedDescription)")
            state.setError(error)
            return
        }
        isConnectingNativeKubernetesAuth = true
        nativeKubernetesAuthStatus = "Connecting Google service account…"
        if state.lastError != nil { state.clearError() }

        Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.isConnectingNativeKubernetesAuth = false }
            var profileWasSaved = false
            do {
                try await self.nativeAuthConfigurator.bindGCPServiceAccount(
                    to: request,
                    serviceAccountJSON: serviceAccountJSON,
                    displayName: "Google GKE"
                )
                profileWasSaved = true
                self.nativeKubernetesAuthStatus = "Google credentials connected. Verifying the selected context…"
                try await self.reloadContexts()
                self.nativeKubernetesAuthStatus = "Google native authentication is connected."
                if !UserDefaults.standard.runeSimpleMode {
                    self.runAuthDoctor()
                }
            } catch {
                self.nativeKubernetesAuthStatus = profileWasSaved
                    ? "Google credentials were saved, but the selected context could not be verified. Run Auth Doctor and try again."
                    : "Google native authentication could not be connected."
                self.diagnostics.log("native Google auth connection failed: \(error.localizedDescription)")
                self.state.setError(error)
            }
        }
    }

    public func nativeAuthProfileStatus(
        for request: KubernetesNativeCredentialRequest
    ) async throws -> KubernetesNativeAuthProfileStatus {
        try await nativeAuthConfigurator.status(for: request)
    }

    public func disconnectSelectedNativeAuth() {
        guard canStartNativeCredentialOperation else { return }
        do {
            let request = try selectedNativeCredentialRequest(expectedProvider: nil)
            disconnectNativeAuth(request: request, expectedProvider: request.provider)
        } catch {
            nativeKubernetesAuthStatus = "Native authentication could not be disconnected."
            state.setError(error)
        }
    }

    /// Removes only a profile whose explicit request matches the provider represented
    /// by the calling UI. This prevents one provider sheet from removing another's binding.
    public func disconnectNativeAuth(
        request: KubernetesNativeCredentialRequest,
        expectedProvider: KubernetesNativeAuthProviderKind
    ) {
        guard canStartNativeCredentialOperation else { return }
        do {
            try validateNativeCredentialRequest(request, expectedProvider: expectedProvider)
        } catch {
            nativeKubernetesAuthStatus = "Native authentication could not be disconnected."
            diagnostics.log("native auth disconnect request validation failed: \(error.localizedDescription)")
            state.setError(error)
            return
        }
        isConnectingNativeKubernetesAuth = true
        Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.isConnectingNativeKubernetesAuth = false }
            do {
                try await self.nativeAuthConfigurator.removeProfile(for: request.bindingID)
                self.nativeKubernetesAuthStatus = "Native authentication disconnected."
            } catch {
                self.nativeKubernetesAuthStatus = "Native authentication could not be disconnected."
                self.state.setError(error)
            }
        }
    }

    public func clearNativeKubernetesAuthStatus() {
        guard !isConnectingNativeKubernetesAuth else { return }
        nativeKubernetesAuthStatus = nil
    }

    public func validateSelectedNativeAuthContext(for provider: KubernetesNativeAuthProviderKind) -> Bool {
        do {
            _ = try selectedNativeCredentialRequest(expectedProvider: provider)
            return true
        } catch {
            nativeKubernetesAuthStatus = "Select a compatible imported \(provider.displayName) context first."
            state.setError(error)
            return false
        }
    }

    private func selectedNativeCredentialRequest(
        expectedProvider: KubernetesNativeAuthProviderKind?
    ) throws -> KubernetesNativeCredentialRequest {
        guard let selectedContextName = state.selectedContext?.name else {
            throw RuneError.invalidInput(message: "Select an imported Kubernetes context first.")
        }
        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(sources: state.kubeConfigSources)
        if let descriptor = analysis.contexts.first(where: { $0.contextName == selectedContextName }),
           let request = descriptor.credentialRequest {
            if let expectedProvider, request.provider != expectedProvider {
                throw RuneError.invalidInput(
                    message: "The selected context uses \(request.provider.displayName), not \(expectedProvider.displayName)."
                )
            }
            return request
        }
        throw RuneError.invalidInput(
            message: "The selected context does not contain a supported native authentication configuration."
        )
    }

    private func validateNativeCredentialRequest(
        _ request: KubernetesNativeCredentialRequest,
        expectedProvider: KubernetesNativeAuthProviderKind
    ) throws {
        guard request.provider == expectedProvider else {
            throw RuneError.invalidInput(
                message: "The requested context uses \(request.provider.displayName), not \(expectedProvider.displayName)."
            )
        }
    }

    private func appendCloudKubeConfigImportOutput(_ text: String) {
        guard !text.isEmpty else { return }
        cloudKubeConfigImportOutput.append(text)
        if cloudKubeConfigImportOutput.count > Self.maximumCloudKubeConfigImportOutputCharacters {
            cloudKubeConfigImportOutput = "...\n" + cloudKubeConfigImportOutput.suffix(Self.maximumCloudKubeConfigImportOutputCharacters)
        }
    }

    private func kubeConfigFiles(in folder: URL) throws -> [URL] {
        let urls = try FileManager.default.contentsOfDirectory(
            at: folder,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        )

        return urls
            .filter { url in
                guard ((try? url.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) ?? false) else {
                    return false
                }

                let filename = url.lastPathComponent.lowercased()
                let ext = url.pathExtension.lowercased()
                return filename == "config" || ext == "yaml" || ext == "yml"
            }
            .sorted { $0.lastPathComponent.localizedStandardCompare($1.lastPathComponent) == .orderedAscending }
    }

    private func importKubeConfigPayloads(
        _ payloads: [(raw: String, sourceName: String, sourceURL: URL?)],
        logLabel: String,
        securityScopedURLs: [URL] = [],
        temporaryDirectories: [URL] = []
    ) async throws {
        guard !isCommittingKubeConfigImport else {
            throw RuneError.invalidInput(message: "Wait for the current kubeconfig import to finish before starting another import.")
        }
        discardPendingKubeConfigImport(clearReview: false)
        for url in securityScopedURLs {
            pendingKubeConfigSourceAccess.retainAccess(to: url)
        }
        pendingKubeConfigTemporaryDirectories = temporaryDirectories
        let registrySnapshot = try loadedKubeConfigRegistrySnapshot()
        let transaction = KubeConfigImportTransaction(
            payloads: payloads.map {
                KubeConfigImportTransaction.Payload(
                    raw: $0.raw,
                    sourceName: $0.sourceName,
                    sourceURL: $0.sourceURL
                )
            },
            logLabel: logLabel,
            existingNames: registrySnapshot.names,
            validator: kubeConfigImportValidator,
            resolver: kubeConfigDuplicateResolver
        )
        kubeConfigImportReviews = transaction.reviews
        kubeConfigImportReviewMode = .preflight
        syncKubeConfigImportContextMetadataDrafts(from: transaction.reviews)

        let resolution = try transaction.resolvingDuplicates(
            choice: kubeConfigDuplicateHandlingChoice,
            resolver: kubeConfigDuplicateResolver,
            validator: kubeConfigImportValidator
        )
        kubeConfigImportReviews = resolution.reviews
        syncKubeConfigImportContextMetadataDrafts(from: resolution.reviews)
        if let failure = AddClusterCloudImportWorkflow.blockingFailure(in: resolution.reviews) {
            state.setAuthDoctorChecks(failure.checks)
            kubeConfigImportReviewMode = .report
            throw RuneError.invalidInput(message: failure.message)
        }

        pendingKubeConfigImport = transaction
        pendingKubeConfigImportRegistrySnapshot = registrySnapshot
        isKubeConfigImportConfirmationPending = true
        canConfirmKubeConfigImport = true
        if state.lastError != nil {
            state.clearError()
        }
        diagnostics.log("\(logLabel) preflight ready payloads count=\(payloads.count)")
    }

    private func refreshPendingKubeConfigImportResolution() {
        guard let transaction = pendingKubeConfigImport, !isCommittingKubeConfigImport else { return }
        do {
            let resolution = try transaction.resolvingDuplicates(
                choice: kubeConfigDuplicateHandlingChoice,
                resolver: kubeConfigDuplicateResolver,
                validator: kubeConfigImportValidator
            )
            kubeConfigImportReviews = resolution.reviews
            syncKubeConfigImportContextMetadataDrafts(from: resolution.reviews)
            canConfirmKubeConfigImport = AddClusterCloudImportWorkflow.blockingFailure(in: resolution.reviews) == nil
        } catch {
            canConfirmKubeConfigImport = false
            diagnostics.log("kubeconfig import duplicate resolution failed: \(error.localizedDescription)")
        }
    }

    private func loadedKubeConfigRegistrySnapshot() throws -> KubeConfigImportRegistrySnapshot {
        var names = KubeConfigNameRegistry()
        var sourceEntries: [KubeConfigImportRegistrySnapshot.SourceEntry] = []
        sourceEntries.reserveCapacity(state.kubeConfigSources.count)
        for source in state.kubeConfigSources {
            let raw = try String(contentsOf: source.url, encoding: .utf8)
            names.formUnion(try kubeConfigDuplicateResolver.names(in: raw))
            sourceEntries.append(KubeConfigImportRegistrySnapshot.SourceEntry(
                path: source.url.standardizedFileURL.path,
                contentDigest: Data(SHA256.hash(data: Data(raw.utf8)))
            ))
        }
        sourceEntries.sort { lhs, rhs in
            if lhs.path != rhs.path { return lhs.path < rhs.path }
            return lhs.contentDigest.lexicographicallyPrecedes(rhs.contentDigest)
        }
        return KubeConfigImportRegistrySnapshot(names: names, sources: sourceEntries)
    }

    public func confirmKubeConfigImport() {
        guard let transaction = pendingKubeConfigImport,
              canConfirmKubeConfigImport,
              !isCommittingKubeConfigImport else { return }
        canConfirmKubeConfigImport = false
        isCommittingKubeConfigImport = true
        Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.isCommittingKubeConfigImport = false }
            do {
                let currentRegistrySnapshot = try self.loadedKubeConfigRegistrySnapshot()
                if try self.requireFreshKubeConfigImportConfirmationIfNeeded(
                    transaction: transaction,
                    currentRegistrySnapshot: currentRegistrySnapshot
                ) {
                    return
                }
                let resolution = try transaction.resolvingDuplicates(
                    choice: self.kubeConfigDuplicateHandlingChoice,
                    resolver: self.kubeConfigDuplicateResolver,
                    validator: self.kubeConfigImportValidator
                )
                if let failure = AddClusterCloudImportWorkflow.blockingFailure(in: resolution.reviews) {
                    self.state.setAuthDoctorChecks(failure.checks)
                    throw RuneError.invalidInput(message: failure.message)
                }
                let registrySnapshotAfterResolution = try self.loadedKubeConfigRegistrySnapshot()
                if registrySnapshotAfterResolution != currentRegistrySnapshot,
                   try self.requireFreshKubeConfigImportConfirmationIfNeeded(
                       transaction: transaction,
                       currentRegistrySnapshot: registrySnapshotAfterResolution
                   ) {
                    return
                }
                let sources = try self.publishKubeConfigImport(
                    resolution,
                    logLabel: transaction.logLabel
                )
                guard self.pendingKubeConfigImport?.id == transaction.id else { return }
                let pendingNativeCredential = self.pendingNativeCloudCredential
                self.pendingKubeConfigImport = nil
                self.pendingKubeConfigImportRegistrySnapshot = nil
                self.pendingNativeCloudCredential = nil
                self.isKubeConfigImportConfirmationPending = false
                self.canConfirmKubeConfigImport = false
                self.kubeConfigImportReviewMode = .report
                if let provider = self.pendingCloudKubeConfigProvider {
                    self.cloudKubeConfigImportStatus = AddClusterCloudImportWorkflow.importedStatus(for: provider)
                    self.cloudKubeConfigImportDiagnostic = nil
                    self.pendingCloudKubeConfigProvider = nil
                }
                var finalNativeCredentialStatus: String?
                var nativeCredentialBindingError: Error?
                if let pendingNativeCredential {
                    do {
                        if try await self.bindPendingNativeCloudCredential(
                            pendingNativeCredential,
                            resolution: resolution
                        ) != nil {
                            finalNativeCredentialStatus = "Cluster imported and native credentials connected."
                        } else {
                            finalNativeCredentialStatus = "No context was added. Import again and choose Update existing in the review to connect credentials."
                        }
                    } catch {
                        finalNativeCredentialStatus = "Cluster imported, but Keychain storage failed. Import again and choose Update existing in the review to retry."
                        nativeCredentialBindingError = error
                        self.diagnostics.log("native credential bind after import failed: \(error.localizedDescription)")
                        self.state.setError(error)
                    }
                }
                do {
                    try await self.activateCommittedKubeConfigImport(
                        sources: sources,
                        preferredContextName: resolution.preferredContextName
                    )
                } catch {
                    self.diagnostics.log("activateCommittedKubeConfigImport failed: \(error.localizedDescription)")
                    self.state.setError(error)
                }
                if let finalNativeCredentialStatus {
                    self.nativeKubernetesAuthStatus = finalNativeCredentialStatus
                }
                if let nativeCredentialBindingError {
                    self.state.setError(nativeCredentialBindingError)
                }
            } catch {
                guard self.pendingKubeConfigImport?.id == transaction.id else { return }
                self.canConfirmKubeConfigImport = true
                self.diagnostics.log("confirmKubeConfigImport failed: \(error.localizedDescription)")
                self.state.setError(error)
            }
        }
    }

    private func requireFreshKubeConfigImportConfirmationIfNeeded(
        transaction: KubeConfigImportTransaction,
        currentRegistrySnapshot: KubeConfigImportRegistrySnapshot
    ) throws -> Bool {
        guard pendingKubeConfigImportRegistrySnapshot != currentRegistrySnapshot else {
            return false
        }

        let refreshedTransaction = KubeConfigImportTransaction(
            payloads: transaction.payloads,
            logLabel: transaction.logLabel,
            existingNames: currentRegistrySnapshot.names,
            validator: kubeConfigImportValidator,
            resolver: kubeConfigDuplicateResolver
        )
        let resolution = try refreshedTransaction.resolvingDuplicates(
            choice: kubeConfigDuplicateHandlingChoice,
            resolver: kubeConfigDuplicateResolver,
            validator: kubeConfigImportValidator
        )
        let confirmationIssue = KubeConfigImportIssue(
            id: "loaded-registry-changed-reconfirmation-required",
            severity: .warning,
            message: "Loaded kubeconfig sources changed after preflight. Rune refreshed this review; review it again and confirm once more."
        )
        let refreshedReviews = resolution.reviews.map { review in
            KubeConfigImportReview(
                contexts: review.contexts,
                issues: review.issues + [confirmationIssue],
                redactedPreview: review.redactedPreview,
                sourceName: review.sourceName,
                hasDuplicateConflicts: review.hasDuplicateConflicts
            )
        }

        pendingKubeConfigImport = refreshedTransaction
        pendingKubeConfigImportRegistrySnapshot = currentRegistrySnapshot
        kubeConfigImportReviews = refreshedReviews
        kubeConfigImportReviewMode = .preflight
        syncKubeConfigImportContextMetadataDrafts(from: refreshedReviews)
        canConfirmKubeConfigImport = AddClusterCloudImportWorkflow.blockingFailure(in: refreshedReviews) == nil
        isCommittingKubeConfigImport = false
        diagnostics.log("kubeconfig import registry changed after preflight; explicit reconfirmation required")
        return true
    }

    private func bindPendingNativeCloudCredential(
        _ credential: PendingNativeCloudCredential,
        resolution: KubeConfigImportTransaction.Resolution
    ) async throws -> KubernetesNativeCredentialRequest? {
        guard !resolution.contextNamesForPreferences.isEmpty else { return nil }
        var candidates: [KubernetesNativeCredentialRequest] = []
        for payload in resolution.payloads {
            let analysis = try KubeConfigNativeAuthAnalyzer().analyze(raw: payload.raw)
            candidates.append(contentsOf: analysis.contexts.compactMap { descriptor in
                guard resolution.contextNamesForPreferences.contains(descriptor.contextName),
                      descriptor.provider == credential.provider else {
                    return nil
                }
                return descriptor.credentialRequest
            })
        }
        let request: KubernetesNativeCredentialRequest?
        if let preferredContextName = resolution.preferredContextName {
            request = candidates.first { $0.contextName == preferredContextName }
        } else if candidates.count == 1 {
            request = candidates[0]
        } else {
            request = nil
        }
        guard let request else {
            throw RuneError.invalidInput(
                message: "Rune could not match the imported cluster to its native credential profile."
            )
        }

        switch credential {
        case let .aks(clientSecret):
            try await nativeAuthConfigurator.bindAKSServicePrincipal(
                to: request,
                clientSecret: clientSecret,
                displayName: "Azure AKS"
            )
        case let .eks(credentials):
            try await nativeAuthConfigurator.bindAWSCredentials(
                to: request,
                credentials: credentials,
                displayName: "AWS EKS"
            )
        case let .gke(serviceAccountJSON):
            try await nativeAuthConfigurator.bindGCPServiceAccount(
                to: request,
                serviceAccountJSON: serviceAccountJSON,
                displayName: "Google GKE"
            )
        }
        return request
    }

    public func cancelKubeConfigImport() {
        guard !isCommittingKubeConfigImport else { return }
        let wasNativeImport = pendingNativeCloudCredential != nil
        discardPendingKubeConfigImport(clearReview: true)
        if wasNativeImport {
            cloudKubeConfigImportStatus = nil
            cloudKubeConfigImportDiagnostic = nil
            nativeKubernetesAuthStatus = "Import cancelled. Credentials were not stored."
        }
    }

    private func discardPendingKubeConfigImport(clearReview: Bool) {
        pendingKubeConfigSourceAccess.releaseAll()
        removePendingKubeConfigTemporaryDirectories()
        pendingKubeConfigImport = nil
        pendingKubeConfigImportRegistrySnapshot = nil
        pendingCloudKubeConfigProvider = nil
        pendingNativeCloudCredential = nil
        isKubeConfigImportConfirmationPending = false
        canConfirmKubeConfigImport = false
        if clearReview {
            kubeConfigImportReviews = []
            kubeConfigImportContextMetadataDrafts = [:]
            kubeConfigImportReviewMode = nil
        }
    }

    private func publishKubeConfigImport(
        _ resolution: KubeConfigImportTransaction.Resolution,
        logLabel: String
    ) throws -> [KubeConfigSource] {
        defer {
            pendingKubeConfigSourceAccess.releaseAll()
            removePendingKubeConfigTemporaryDirectories()
        }
        let payloads = resolution.payloads
        let reviews = resolution.reviews

        let importedFiles = try kubeConfigImportStore.saveImportedKubeConfigs(payloads.map { payload in
            KubeConfigImportStorePayload(
                raw: payload.raw,
                sourceName: payload.sourceName,
                sourceURL: payload.sourceURL
            )
        })
        let bookmarkPlacement: KubeConfigBookmarkPlacement = resolution.sourcePlacement == .prependNewestFirst
            ? .prependNewestFirst
            : .append
        do {
            try bookmarkManager.addKubeConfigs(urls: importedFiles, placement: bookmarkPlacement)
        } catch {
            try? kubeConfigImportStore.removeImportedKubeConfigs(at: importedFiles)
            throw error
        }

        kubeConfigImportReviews = reviews
        syncKubeConfigImportContextMetadataDrafts(from: reviews)
        persistKubeConfigImportContextPreferences(
            from: reviews,
            contextNames: resolution.contextNamesForPreferences
        )

        let importedSources = importedFiles.map(KubeConfigSource.init(url:))
        sessionImportedKubeConfigSourcePaths.formUnion(
            importedFiles.map { $0.standardizedFileURL.path }
        )
        let orderedSources: [KubeConfigSource]
        switch resolution.sourcePlacement {
        case .append:
            orderedSources = state.kubeConfigSources + importedSources
        case .prependNewestFirst:
            orderedSources = Array(importedSources.reversed()) + state.kubeConfigSources
        }
        var seenSourcePaths = Set<String>()
        let sources = orderedSources.filter { source in
            seenSourcePaths.insert(source.url.standardizedFileURL.path).inserted
        }
        state.setSources(sources)
        latestKubeConfigSourceFingerprint = kubeConfigSourceFingerprint(for: sources)
        startKubeConfigSourceSync()
        diagnostics.log("\(logLabel) loaded payloads count=\(importedFiles.count), sources count=\(sources.count)")
        return sources
    }

    private func removePendingKubeConfigTemporaryDirectories() {
        let directories = pendingKubeConfigTemporaryDirectories
        pendingKubeConfigTemporaryDirectories = []
        for directory in directories {
            try? FileManager.default.removeItem(at: directory)
        }
    }

    private func activateCommittedKubeConfigImport(
        sources: [KubeConfigSource],
        preferredContextName: String?
    ) async throws {
        if !RuneExternalCommandPolicy.allowsExternalCommands {
            let nativeAnalysis = try KubeConfigNativeAuthAnalyzer().analyze(sources: sources)
            if nativeAnalysis.contexts.contains(where: { $0.exec != nil }) {
                try await reloadContextDefinitionsOnly(
                    preferredContextName: preferredContextName ?? nativeAnalysis.currentContext
                )
                if let selectedName = state.selectedContext?.name,
                   let descriptor = nativeAnalysis.contexts.first(where: { $0.contextName == selectedName }),
                   descriptor.exec != nil {
                    if let request = descriptor.credentialRequest {
                        let status = try await nativeAuthConfigurator.status(for: request)
                        if status.isConnected {
                            try await reloadContexts()
                        } else {
                            nativeKubernetesAuthStatus = "Kubeconfig imported. Connect \(request.provider.displayName) credentials for the selected context."
                        }
                    } else {
                        nativeKubernetesAuthStatus = "Kubeconfig imported. Its exec plugin is not available in the App Store build; use a supported native profile or Rune's direct build."
                    }
                    return
                }
            }
        }
        try await reloadContexts(
            loadInitialSnapshotSynchronously: true,
            preferredContextName: preferredContextName
        )
    }

    private func reloadContextDefinitionsOnly(preferredContextName: String?) async throws {
        state.isLoading = true
        defer { state.isLoading = false }
        let previousContextName = state.selectedContext?.name
        let contexts = try await kubeClient.listContexts(from: state.kubeConfigSources)
        state.setContexts(contexts)
        if let preferredContextName,
           let preferred = contexts.first(where: { $0.name == preferredContextName }) {
            state.selectedContext = preferred
        }
        if let previousContextName, state.selectedContext?.name != previousContextName {
            stopAndClearTerminalSessions(contextName: previousContextName)
        }
        if let selected = state.selectedContext {
            rememberRecentContext(selected.name)
        }
    }

    private func persistKubeConfigImportContextPreferences(
        from reviews: [KubeConfigImportReview],
        contextNames: Set<String>? = nil
    ) {
        let contexts = reviews.flatMap(\.contexts).filter { context in
            contextNames?.contains(context.name) ?? true
        }
        var preferredNamespaces: [String: String] = [:]
        var displayMetadata: [String: ContextDisplayMetadata] = [:]
        for context in contexts {
            if let namespace = context.namespace?.trimmingCharacters(in: .whitespacesAndNewlines),
               !namespace.isEmpty {
                preferredNamespaces[context.name] = namespace
            }
            displayMetadata[context.name] = kubeConfigImportContextMetadataDrafts[context.name]
                ?? suggestedContextDisplayMetadata(for: context)
        }
        var favorites = state.favoriteContextNames
        if favoriteImportedKubeConfigContexts {
            favorites.formUnion(contexts.map(\.name))
        }
        contextPreferences.applyKubeConfigImportPreferenceBatch(KubeConfigImportPreferenceBatch(
            preferredNamespaces: preferredNamespaces,
            contextDisplayMetadata: displayMetadata,
            favoriteContextNames: favoriteImportedKubeConfigContexts ? favorites : nil
        ))
        commandPaletteContextMetadataCache.removeAll(keepingCapacity: true)
        commandPaletteContextsWithoutMetadata.removeAll(keepingCapacity: true)
        invalidateCommandPaletteResultCache()
        if favoriteImportedKubeConfigContexts {
            state.setFavoriteContextNames(favorites)
        }
    }

    private func syncKubeConfigImportContextMetadataDrafts(from reviews: [KubeConfigImportReview]) {
        let contexts = reviews.flatMap(\.contexts)
        let contextNames = Set(contexts.map(\.name))
        var drafts = kubeConfigImportContextMetadataDrafts.filter { contextNames.contains($0.key) }
        for context in contexts where drafts[context.name] == nil {
            drafts[context.name] = suggestedContextDisplayMetadata(for: context)
        }
        kubeConfigImportContextMetadataDrafts = drafts
    }

    private func suggestedContextDisplayMetadata(for context: KubeConfigImportContextPreview) -> ContextDisplayMetadata {
        let provider = context.providerHint?.trimmingCharacters(in: .whitespacesAndNewlines)
        let authType = context.authType.trimmingCharacters(in: .whitespacesAndNewlines)
        var tags: [String] = []
        if let provider, !provider.isEmpty {
            tags.append(provider)
        }
        if !authType.isEmpty, authType.localizedCaseInsensitiveCompare("Unknown") != .orderedSame {
            tags.append(authType)
        }

        let providerKey = provider.map(Self.normalizedContextMetadataKey)
        let isLocalCluster = provider.map(Self.isLocalClusterProvider) ?? false
        return ContextDisplayMetadata(
            colorKey: providerKey,
            iconName: isLocalCluster ? "desktopcomputer" : (provider == nil ? nil : "cloud"),
            tags: tags,
            group: isLocalCluster ? "Local clusters" : (provider == nil ? nil : "Provider clusters")
        )
    }

    private static func normalizedContextMetadataKey(_ value: String) -> String {
        value
            .lowercased()
            .map { character -> Character in
                character.isLetter || character.isNumber ? character : "-"
            }
            .reduce(into: "") { result, character in
                if character == "-", result.last == "-" {
                    return
                }
                result.append(character)
            }
            .trimmingCharacters(in: CharacterSet(charactersIn: "-"))
    }

    private static func isLocalClusterProvider(_ provider: String) -> Bool {
        ["minikube", "kind", "k3d", "docker desktop", "orbstack"]
            .contains { $0.localizedCaseInsensitiveCompare(provider) == .orderedSame }
    }

    public func addDefaultKubeConfig() {
        Task {
            do {
                let url = Self.standardKubeConfigURL()
                let selectedURL: URL
                if Self.isAppSandboxed {
                    guard let pickedURL = try picker.pickDefaultKubeConfig(at: url) else {
                        return
                    }
                    selectedURL = pickedURL
                } else {
                    guard FileManager.default.fileExists(atPath: url.path) else {
                        throw RuneError.invalidInput(message: "Default kubeconfig was not found at \(url.path)")
                    }
                    selectedURL = url
                }

                try? bookmarkManager.addKubeConfig(url: selectedURL)
                let sources = try resolvedKubeConfigSources(fallbackURLs: [selectedURL])
                state.setSources(sources)
                latestKubeConfigSourceFingerprint = kubeConfigSourceFingerprint(for: sources)
                startKubeConfigSourceSync()
                diagnostics.log("addDefaultKubeConfig loaded \(selectedURL.path), sources count=\(sources.count)")
                try await reloadContexts()
            } catch {
                diagnostics.log("addDefaultKubeConfig failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    private static var isAppSandboxed: Bool {
        ProcessInfo.processInfo.environment["APP_SANDBOX_CONTAINER_ID"] != nil
    }

    private static func standardKubeConfigURL() -> URL {
        if let home = getpwuid(getuid())?.pointee.pw_dir {
            return URL(fileURLWithPath: String(cString: home))
                .appendingPathComponent(".kube", isDirectory: true)
                .appendingPathComponent("config", isDirectory: false)
        }

        return FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".kube", isDirectory: true)
            .appendingPathComponent("config", isDirectory: false)
    }

    public func reloadContexts() async throws {
        try await reloadContexts(loadInitialSnapshotSynchronously: true)
    }

    private func reloadContexts(
        loadInitialSnapshotSynchronously: Bool,
        showsLoadingIndicator: Bool = true,
        expectedClusterLoadGeneration: UUID? = nil,
        preferredContextName: String? = nil
    ) async throws {
        if showsLoadingIndicator {
            state.isLoading = true
        }
        defer {
            if showsLoadingIndicator {
                state.isLoading = false
            }
        }

        diagnostics.log("reloadContexts start")
        let previousContextName = state.selectedContext?.name
        let previousNamespace = state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        let contexts = try await kubeClient.listContexts(from: state.kubeConfigSources)
        if let expectedClusterLoadGeneration, !isCurrentClusterLoad(expectedClusterLoadGeneration) {
            throw CancellationError()
        }
        state.setContexts(contexts)
        if let preferredContextName,
           let preferred = contexts.first(where: { $0.name == preferredContextName }) {
            state.selectedContext = preferred
        }
        if let previousContextName, state.selectedContext?.name != previousContextName {
            stopAndClearTerminalSessions(contextName: previousContextName)
        }
        diagnostics.log("reloadContexts contexts=\(contexts.count)")

        if let selected = state.selectedContext {
            rememberRecentContext(selected.name)
            // Keep current in-memory namespace only when staying on the same context.
            // If selected context changed (startup/new context list), start empty and let
            // `loadResourceSnapshot` resolve from context default + live namespace list.
            let requestedNamespace: String = selected.name == previousContextName ? previousNamespace : ""
            pendingNamespaceRevalidationContextName = selected.name == previousContextName ? nil : selected.name
            if state.selectedNamespace != requestedNamespace {
                if selected.name == previousContextName {
                    stopAndClearTerminalSessions(
                        contextName: selected.name,
                        namespace: state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
                    )
                }
                state.selectedNamespace = requestedNamespace
            }

            // Same warm path as `setContext`: hydrate lists from `ResourceStore` / disk immediately, then fetch fresh cluster data.
            // Without this, startup only matched the awaited snapshot path and could feel “cache-only” compared to switching contexts.
            applyCachedSnapshot(context: selected, namespace: state.selectedNamespace)

            let requestID = beginSnapshotRequest(
                context: selected,
                namespace: requestedNamespace,
                source: "reloadContexts"
            )
            if loadInitialSnapshotSynchronously {
                try await loadResourceSnapshot(
                    context: selected,
                    namespace: requestedNamespace,
                    requestID: requestID,
                    forceNamespaceMetadataRefresh: true
                )
            } else {
                Task { @MainActor [weak self] in
                    guard let self else { return }
                    do {
                        try await self.loadResourceSnapshot(
                            context: selected,
                            namespace: requestedNamespace,
                            requestID: requestID,
                            forceNamespaceMetadataRefresh: true
                        )
                    } catch {
                        guard !Self.isBenignCancellationError(error) else { return }
                        self.diagnostics.log("bootstrap background snapshot failed: \(error.localizedDescription)")
                        self.state.setError(error)
                    }
                }
            }
            if navigationHistory.isEmpty {
                recordNavigationCheckpoint()
            }
        } else {
            state.setNamespaces([])
        }
    }

    private func persistDiscoveredKubeConfigsInBackground(_ urls: [URL]) {
        guard !urls.isEmpty else { return }
        Task.detached(priority: .utility) { [bookmarkManager] in
            for url in urls {
                try? bookmarkManager.addKubeConfig(url: url)
            }
        }
    }

    public func refreshKubeConfigSourcesFromDiscovery() {
        Task { @MainActor [weak self] in
            guard let self else { return }
            do {
                _ = try await self.syncKubeConfigSourcesFromDiscovery(reason: "manual")
            } catch {
                self.diagnostics.log("manual kubeconfig sync failed: \(error.localizedDescription)")
                self.state.setError(error)
            }
        }
    }

    @discardableResult
    public func syncKubeConfigSourcesFromDiscovery(reason: String) async throws -> Bool {
        let discoveredURLs = kubeConfigDiscoverer.discoverCandidateFiles()
        let resolvedSources = try resolvedKubeConfigSources(fallbackURLs: discoveredURLs)
        let sources = retainingSessionImportedKubeConfigSources(in: resolvedSources)
        let fingerprint = kubeConfigSourceFingerprint(for: sources)
        let sourceSetChanged = state.kubeConfigSources != sources
        let contentChanged = latestKubeConfigSourceFingerprint.map { $0 != fingerprint } ?? sourceSetChanged
        guard sourceSetChanged || contentChanged else { return false }

        latestKubeConfigSourceFingerprint = fingerprint
        if sourceSetChanged {
            state.setSources(sources)
            persistDiscoveredKubeConfigsInBackground(discoveredURLs)
        } else if contentChanged {
            kubernetesRequestMetricsSummary = .empty
            invalidateAuthDoctorRunForScopeChange()
        }

        guard !state.resourceYAMLHasUnsavedEdits else {
            diagnostics.log("kubeconfig sync deferred reason=\(reason) because YAML has unsaved edits")
            return true
        }

        diagnostics.log("kubeconfig sync detected change reason=\(reason), sources count=\(sources.count)")
        if sources.isEmpty {
            clearClusterDataForEmptyBootstrapIfNeeded()
            state.setContexts([])
            state.setNamespaces([])
            return true
        }

        try await reloadContexts(
            loadInitialSnapshotSynchronously: false,
            showsLoadingIndicator: false
        )
        return true
    }

    private func retainingSessionImportedKubeConfigSources(
        in resolvedSources: [KubeConfigSource]
    ) -> [KubeConfigSource] {
        guard !sessionImportedKubeConfigSourcePaths.isEmpty else { return resolvedSources }

        let fileManager = FileManager.default
        let resolvedByPath = Dictionary(uniqueKeysWithValues: resolvedSources.map {
            ($0.url.standardizedFileURL.path, $0)
        })
        let missingSessionSources = state.kubeConfigSources.filter { source in
            let path = source.url.standardizedFileURL.path
            return sessionImportedKubeConfigSourcePaths.contains(path)
                && resolvedByPath[path] == nil
                && fileManager.fileExists(atPath: path)
        }
        guard !missingSessionSources.isEmpty else { return resolvedSources }

        let retainedPaths = Set(missingSessionSources.map { $0.url.standardizedFileURL.path })
        var merged: [KubeConfigSource] = []
        var seen = Set<String>()
        merged.reserveCapacity(resolvedSources.count + missingSessionSources.count)

        // Preserve the import placement chosen by the user while allowing discovery to
        // drop ordinary sources that genuinely disappeared.
        for current in state.kubeConfigSources {
            let path = current.url.standardizedFileURL.path
            let source = resolvedByPath[path] ?? (retainedPaths.contains(path) ? current : nil)
            guard let source, seen.insert(path).inserted else { continue }
            merged.append(source)
        }
        for source in resolvedSources {
            let path = source.url.standardizedFileURL.path
            guard seen.insert(path).inserted else { continue }
            merged.append(source)
        }
        return merged
    }

    private func startKubeConfigSourceSync() {
        kubeConfigSourceSyncTask?.cancel()
        kubeConfigSourceSyncTask = Task { @MainActor [weak self] in
            guard let self else { return }
            while !Task.isCancelled {
                do {
                    try await Task.sleep(nanoseconds: self.kubeConfigSourceSyncNanoseconds)
                } catch {
                    return
                }
                guard !Task.isCancelled else { return }
                guard self.state.selectedContext?.name != self.demoContextName else { continue }
                do {
                    _ = try await self.syncKubeConfigSourcesFromDiscovery(reason: "watch")
                } catch {
                    self.diagnostics.log("kubeconfig watch sync failed: \(error.localizedDescription)")
                }
            }
        }
    }

    private func stopKubeConfigSourceSync() {
        kubeConfigSourceSyncTask?.cancel()
        kubeConfigSourceSyncTask = nil
    }

    private func kubeConfigSourceFingerprint(for sources: [KubeConfigSource]) -> KubeConfigSourceFingerprint {
        KubeConfigSourceFingerprint(sources: sources)
    }

    /// - Parameter debounced: When `false`, reload runs immediately (⌘R, panel refresh). When `true`, waits briefly to coalesce rapid triggers.
    public func refreshCurrentView(debounced: Bool = true) {
        scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: false, debounced: debounced)
    }

    private func startLiveStatusUpdates() {
        liveStatusUpdatesTask?.cancel()
        liveStatusUpdatesTask = Task { @MainActor [weak self] in
            guard let self else { return }
            while !Task.isCancelled {
                do {
                    try await Task.sleep(nanoseconds: self.liveStatusUpdateNanoseconds)
                } catch {
                    return
                }
                guard !Task.isCancelled else { return }
                guard self.isLiveStatusUpdatesEnabled,
                      self.state.selectedContext != nil,
                      self.state.selectedSection != .terminal,
                      !self.state.isLoading,
                      !self.state.isLoadingResourceDetails
                else { continue }
                self.refreshCurrentView(debounced: false)
                if self.state.selectedWorkloadKind == .pod || self.state.selectedWorkloadKind == .deployment || self.state.selectedWorkloadKind == .service {
                    self.reloadLogsForSelection()
                }
            }
        }
    }

    private func stopLiveStatusUpdates() {
        liveStatusUpdatesTask?.cancel()
        liveStatusUpdatesTask = nil
    }

    /// Refetches only the right-hand inspector (YAML, describe, logs, etc.) for the current selection — **not** the center list or overview tiles.
    /// List data comes from ``refreshCurrentView`` / ``loadResourceSnapshot`` (driven by ``SnapshotLoadPlan`` per section, e.g. workloads → pods loads only pod list).
    public func refreshResourceInspectorOnly() {
        guard state.selectedContext != nil else { return }
        guard shouldLoadResourceDetailsForCurrentSection,
              hasCurrentResourceSelectionForDetails else { return }
        loadResourceDetailsForCurrentSelection(preservingVisibleDocuments: true)
    }

    /// Refetches the selected Helm release payload or read-only operator document
    /// without routing through the generic resource loader, which excludes Helm.
    public func refreshSelectedHelmInspector() {
        guard state.selectedSection == .helm else { return }
        if state.selectedOperatorResource != nil {
            loadOperatorResourceDetailsForCurrentSelection(preservingVisibleDocuments: true)
        } else if state.selectedHelmRelease != nil {
            loadHelmDetailsForCurrentSelection()
        }
    }

    private func snapshotLoadPlan(
        section: RuneSection,
        kind: KubeResourceKind,
        simpleMode: Bool
    ) -> SnapshotLoadPlan {
        var plan = SnapshotLoadPlan.forSelection(section: section, kind: kind, simpleMode: simpleMode)
        if let pendingOpenEventSource {
            plan.includeEventSource(kind: pendingOpenEventSource.event.involvedKind)
        }
        return plan
    }

    private func scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: Bool, debounced: Bool) {
        guard let context = state.selectedContext else { return }
        applyCachedSnapshot(context: context, namespace: state.selectedNamespace)
        pendingForcedNamespaceRefresh = pendingForcedNamespaceRefresh || forceNamespaceMetadataRefresh

        scheduledRefreshTask?.cancel()
        let delay = debounced ? refreshDebounceNanoseconds : 0
        let refreshID = UUID()
        pendingCurrentViewRefreshID = refreshID

        scheduledRefreshTask = Task { [weak self] in
            guard let self else { return }
            defer {
                if self.pendingCurrentViewRefreshID == refreshID {
                    self.pendingCurrentViewRefreshID = nil
                }
            }
            if delay > 0 {
                try? await Task.sleep(nanoseconds: delay)
            }
            guard !Task.isCancelled else { return }
            let forceNamespaceMetadataRefresh = self.pendingForcedNamespaceRefresh
            self.pendingForcedNamespaceRefresh = false
            await self.performRefreshCurrentView(forceNamespaceMetadataRefresh: forceNamespaceMetadataRefresh)
        }
    }

    private func performRefreshCurrentView(forceNamespaceMetadataRefresh: Bool) async {
        guard let context = state.selectedContext else { return }
        let namespace = state.selectedNamespace
        let simpleMode = UserDefaults.standard.runeSimpleMode
        var cancellationFamilies = snapshotLoadPlan(
            section: state.selectedSection,
            kind: state.selectedWorkloadKind,
            simpleMode: simpleMode
        )
            .resourceListFamilies
        if state.selectedSection == .helm {
            if simpleMode {
                cancellationFamilies.insert(helmBrowserResourceFamily)
            } else {
                cancellationFamilies.formUnion([.helmReleases, .operatorResources])
            }
        }
        if context.name == demoContextName {
            applyDemoClusterSnapshot()
            applyDemoResourceDetailsForCurrentSelection()
            return
        }

        do {
            diagnostics.trace(
                "refresh",
                "performRefreshCurrentView begin context=<redacted-context> namespace=<redacted-namespace> forceNamespaceMeta=\(forceNamespaceMetadataRefresh)"
            )
            diagnostics.log("refreshCurrentView context=\(context.name) namespace=\(namespace)")
            let requestID = beginSnapshotRequest(
                context: context,
                namespace: namespace,
                source: "refreshCurrentView"
            )
            try await loadResourceSnapshot(
                context: context,
                namespace: namespace,
                requestID: requestID,
                forceNamespaceMetadataRefresh: forceNamespaceMetadataRefresh
            )
            if state.selectedSection == .helm {
                if simpleMode {
                    try await loadSelectedHelmBrowserResource(context: context, namespace: state.selectedNamespace)
                } else {
                    try await loadHelmReleases(context: context, namespace: state.selectedNamespace)
                    await loadOperatorResources(context: context, namespace: state.selectedNamespace)
                }
            }
            let currentFreshness = state.snapshotFreshness
            state.setSnapshotFreshness(
                RuneSnapshotFreshness(
                    status: currentFreshness.status == .stale ? .stale : .live,
                    updatedAt: Date(),
                    message: currentFreshness.status == .stale ? currentFreshness.message : "Live for \(context.name) / \(state.selectedNamespace)"
                )
            )
            diagnostics.trace("refresh", "performRefreshCurrentView done context=<redacted-context>")
        } catch {
            if Self.isBenignCancellationError(error) {
                markOverviewCooldownBypass(contextName: context.name, namespace: namespace)
                state.markResourceListsReconnecting(
                    cancellationFamilies,
                    message: "Refresh cancelled; reconnecting to current view."
                )
                state.setSnapshotFreshness(
                    RuneSnapshotFreshness(
                        status: .reconnecting,
                        updatedAt: state.snapshotFreshness.updatedAt,
                        message: "Refresh cancelled; reconnecting to current view."
                    )
                )
                diagnostics.trace("refresh", "performRefreshCurrentView cancelled")
                return
            }
            diagnostics.trace("refresh", "performRefreshCurrentView error=\(error.localizedDescription)")
            diagnostics.log("refreshCurrentView failed: \(error.localizedDescription)")
            state.setSnapshotFreshness(
                RuneSnapshotFreshness(
                    status: .failed,
                    updatedAt: state.snapshotFreshness.updatedAt,
                    message: error.localizedDescription
                )
            )
            state.setError(error)
        }
    }

    public func navigateBack() {
        guard canNavigateBack else { return }
        invalidateDeferredSelectionRestores()
        cancelPendingEventSourceNavigation()
        navigationIndex -= 1
        applyNavigationCheckpoint(navigationHistory[navigationIndex])
        updateNavigationAvailability()
    }

    public func navigateForward() {
        guard canNavigateForward else { return }
        invalidateDeferredSelectionRestores()
        cancelPendingEventSourceNavigation()
        navigationIndex += 1
        applyNavigationCheckpoint(navigationHistory[navigationIndex])
        updateNavigationAvailability()
    }

    public func setSection(_ section: RuneSection) {
        setSection(section, trackHistory: true, triggerReload: true)
    }

    private func setSection(_ section: RuneSection, trackHistory: Bool, triggerReload: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        cancelPendingLogReload()
        resourceDetailsTask?.cancel()
        let previousSection = state.selectedSection
        let previousKind = state.selectedWorkloadKind
        state.selectedSection = section
        if previousSection == .helm, section != .helm {
            invalidateHelmReleaseListRequest()
        }
        diagnostics.log("setSection -> \(section.rawValue)")
        switch section {
        case .workloads where !workloadKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .pod
        case .networking where !networkingKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .service
        case .config where !configKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .configMap
        case .storage where !storageKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .persistentVolumeClaim
        case .rbac where !rbacKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .role
        default:
            break
        }
        if state.selectedSection != previousSection || state.selectedWorkloadKind != previousKind {
            clearResourceBulkSelections()
        }

        if triggerReload {
            let forceNamespaceRefresh = state.selectedContext.map { store.namespaces(context: $0).isEmpty } ?? false
            scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: forceNamespaceRefresh, debounced: false)
        }

        if section == .rbac {
            state.reconcileRBACSelection()
        }

        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func setWorkloadKind(_ kind: KubeResourceKind) {
        setWorkloadKind(kind, trackHistory: true, triggerReload: true)
    }

    public func setHelmBrowserResourceFamily(_ family: RuneResourceListFamily) {
        guard family == .helmReleases || family == .operatorResources else { return }
        guard helmBrowserResourceFamily != family else { return }
        if UserDefaults.standard.runeSimpleMode,
           helmBrowserResourceFamily == .helmReleases,
           family != .helmReleases {
            invalidateHelmReleaseListRequest()
        }
        helmBrowserResourceFamily = family
        guard UserDefaults.standard.runeSimpleMode,
              state.selectedSection == .helm else { return }
        refreshSelectedHelmBrowserResource()
    }

    private func setWorkloadKind(_ kind: KubeResourceKind, trackHistory: Bool, triggerReload: Bool) {
        guard kind != .event else { return }
        prepareNavigationMutation(trackHistory: trackHistory)
        cancelPendingLogReload()
        if kind != state.selectedWorkloadKind {
            clearResourceBulkSelections()
        }
        state.selectedWorkloadKind = kind
        if state.selectedSection == .rbac {
            state.reconcileRBACSelection()
        }
        let willReload = triggerReload && shouldReloadForWorkloadKind(kind)
        diagnostics.trace("workloadKind", "setWorkloadKind kind=\(kind.rawValue) willReload=\(willReload)")
        if willReload {
            scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: false, debounced: false)
        } else if !shouldLoadResourceDetailsForCurrentSection {
            state.clearResourceDetails()
        } else if pendingCurrentViewRefreshID == nil {
            loadResourceDetailsForCurrentSelectionIfNeeded()
        } else {
            diagnostics.trace("workloadKind", "deferred resource details until current view refresh completes")
        }
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func presentCommandPalette(prefilledQuery: String? = nil) {
        commandPalettePrefillQuery = prefilledQuery ?? ""
        state.isCommandPalettePresented = true
    }

    public func dismissCommandPalette() {
        state.isCommandPalettePresented = false
    }

    public func consumeCommandPalettePrefillQuery() -> String {
        let query = commandPalettePrefillQuery
        commandPalettePrefillQuery = ""
        return query
    }

    @discardableResult
    public func reviewKubeConfigImport(raw: String, sourceName: String? = nil) -> KubeConfigImportReview {
        discardPendingKubeConfigImport(clearReview: false)
        let review = kubeConfigImportValidator.validate(raw: raw, sourceName: sourceName)
        kubeConfigImportReviews = [review]
        kubeConfigImportReviewMode = .report
        syncKubeConfigImportContextMetadataDrafts(from: [review])
        return review
    }

    @discardableResult
    public func reviewLoadedKubeConfigSources() -> [KubeConfigImportReview] {
        discardPendingKubeConfigImport(clearReview: false)
        guard !state.kubeConfigSources.isEmpty else {
            let review = KubeConfigImportReview(
                contexts: [],
                issues: [
                    .init(
                        id: "kubeconfig-source-missing",
                        severity: .error,
                        message: "No kubeconfig sources are loaded."
                    )
                ],
                redactedPreview: ""
            )
            kubeConfigImportReviews = [review]
            kubeConfigImportReviewMode = .report
            syncKubeConfigImportContextMetadataDrafts(from: [review])
            return [review]
        }

        let reviews = state.kubeConfigSources.map { source -> KubeConfigImportReview in
            do {
                let raw = try String(contentsOf: source.url, encoding: .utf8)
                return kubeConfigImportValidator.validate(raw: raw, sourceName: source.displayName)
            } catch {
                return KubeConfigImportReview(
                    contexts: [],
                    issues: [
                        .init(
                            id: "kubeconfig-source-unreadable",
                            severity: .error,
                            message: "Could not read kubeconfig source \(source.displayName)."
                    )
                ],
                redactedPreview: "",
                sourceName: source.displayName
            )
            }
        }
        kubeConfigImportReviews = reviews
        kubeConfigImportReviewMode = .report
        syncKubeConfigImportContextMetadataDrafts(from: reviews)
        return reviews
    }

    public func clearKubeConfigImportReviews() {
        discardPendingKubeConfigImport(clearReview: true)
    }

    public func setKubeConfigImportContextMetadata(
        contextName: String,
        alias: String? = nil,
        colorKey: String? = nil,
        iconName: String? = nil,
        tags: [String] = [],
        group: String? = nil
    ) {
        let context = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !context.isEmpty else { return }
        kubeConfigImportContextMetadataDrafts[context] = ContextDisplayMetadata(
            alias: alias,
            colorKey: colorKey,
            iconName: iconName,
            tags: tags,
            group: group
        )
    }

    public func contextDisplayMetadata(for contextName: String) -> ContextDisplayMetadata? {
        contextPreferences.loadContextDisplayMetadata(for: contextName)
    }

    public func setContextSearchQuery(_ query: String) {
        state.contextSearchQuery = query
    }

    public func setResourceSearchQuery(_ query: String) {
        state.resourceSearchQuery = Self.normalizedResourceSearchQueryInput(query)
    }

    public static func normalizedResourceSearchQueryInput(_ query: String) -> String {
        let containsPasteSeparator = query.unicodeScalars.contains { scalar in
            CharacterSet.newlines.contains(scalar) || scalar.value == 9
        }
        guard containsPasteSeparator else { return query }

        return query
            .components(separatedBy: .whitespacesAndNewlines)
            .filter { !$0.isEmpty }
            .joined(separator: " ")
    }

    public func togglePodSort(_ column: PodListSortColumn) {
        if podSortColumn == column {
            podSortAscending.toggle()
        } else {
            podSortColumn = column
            switch column {
            case .cpu, .memory, .restarts:
                podSortAscending = false
            case .age:
                podSortAscending = true
            case .name, .status:
                podSortAscending = true
            }
        }
    }

    public func toggleDeploymentSort(_ column: DeploymentListSortColumn) {
        if deploymentSortColumn == column {
            deploymentSortAscending.toggle()
        } else {
            deploymentSortColumn = column
            switch column {
            case .name:
                deploymentSortAscending = true
            case .replicas:
                deploymentSortAscending = false
            }
        }
    }

    public func toggleServiceSort(_ column: ServiceListSortColumn) {
        if serviceSortColumn == column {
            serviceSortAscending.toggle()
        } else {
            serviceSortColumn = column
            serviceSortAscending = true
        }
    }

    public func toggleGenericResourceSort(_ column: GenericResourceListSortColumn) {
        if genericResourceSortColumn == column {
            genericResourceSortAscending.toggle()
        } else {
            genericResourceSortColumn = column
            genericResourceSortAscending = column == .primary ? false : true
        }
    }

    public func toggleHelmReleaseSort(_ column: HelmReleaseListSortColumn) {
        if helmReleaseSortColumn == column {
            helmReleaseSortAscending.toggle()
        } else {
            helmReleaseSortColumn = column
            helmReleaseSortAscending = column == .revision ? false : true
        }
    }

    public func toggleEventSort(_ column: EventListSortColumn) {
        if eventSortColumn == column {
            eventSortAscending.toggle()
        } else {
            eventSortColumn = column
            eventSortAscending = column == .lastSeen ? false : true
        }
    }

    public func toggleOperatorResourceSort(_ column: OperatorResourceListSortColumn) {
        if operatorResourceSortColumn == column {
            operatorResourceSortAscending.toggle()
        } else {
            operatorResourceSortColumn = column
            operatorResourceSortAscending = true
        }
        operatorResourcePage = 0
    }

    public func setOperatorResourceFocus(_ focus: OperatorResourceFocus) {
        guard operatorResourceFocus != focus else { return }
        operatorResourceFocus = focus
        operatorResourcePage = 0
    }

    public func setReadOnlyMode(_ value: Bool) {
        state.isReadOnlyMode = value
    }

    public func toggleSidebarVisibility() {
        isSidebarVisible.toggle()
        UserDefaults.standard.runeLayoutSidebarVisible = isSidebarVisible
    }

    public func toggleDetailPaneVisibility() {
        isDetailPaneVisible.toggle()
        UserDefaults.standard.runeLayoutDetailPaneVisible = isDetailPaneVisible
    }

    public func setHelmAllNamespaces(_ value: Bool) {
        invalidateHelmReleaseListRequest()
        state.isHelmAllNamespaces = value

        guard state.selectedSection == .helm, let context = state.selectedContext else {
            return
        }

        Task {
            do {
                try await loadHelmReleases(context: context, namespace: state.selectedNamespace)
            } catch {
                if Self.isBenignCancellationError(error) {
                    diagnostics.trace("helm", "setHelmAllNamespaces load cancelled")
                    return
                }
                state.setError(error)
            }
        }
    }

    public func setContext(_ context: KubeContext) {
        setContext(context, preferredNamespace: nil, trackHistory: true, triggerReload: true)
    }

    private func setContext(
        _ context: KubeContext,
        preferredNamespace: String?,
        trackHistory: Bool,
        triggerReload: Bool
    ) {
        if context.name == demoContextName {
            loadDemoCluster()
            return
        }
        prepareNavigationMutation(trackHistory: trackHistory)
        diagnostics.log("setContext -> \(context.name)")
        diagnostics.trace("context", "setContext context=<redacted-context> triggerReload=\(triggerReload)")
        let previousContextName = state.selectedContext?.name
        let previousNamespace = state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        let isChangingContext = context.name != previousContextName
        if isChangingContext {
            invalidateHelmReleaseListRequest()
        }
        if isChangingContext, let previousContextName {
            stopAndClearTerminalSessions(contextName: previousContextName)
        }
        if isChangingContext {
            clearResourceBulkSelections()
        }
        overviewPrefetchTask?.cancel()
        contextOverviewPrefetchTask?.cancel()
        cancelPendingLogReload()
        resourceDetailsTask?.cancel()
        state.selectedContext = context
        if isChangingContext {
            state.setOverviewClusterUsage(cpuPercent: nil, memoryPercent: nil)
            state.clearResourceListFreshness()
        }
        rememberRecentContext(context.name)
        // Clear immediately so the toolbar menu cannot briefly show the previous context's namespace list.
        state.setNamespaces([])
        let carriedNamespace = isChangingContext ? state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines) : ""
        let savedNamespace = contextPreferences.loadPreferredNamespace(for: context.name)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let requestedPreferredNamespace: String
        let namespaceCandidates: [String]
        if let preferredNamespace {
            requestedPreferredNamespace = preferredNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
            namespaceCandidates = [requestedPreferredNamespace, savedNamespace]
        } else if !carriedNamespace.isEmpty {
            requestedPreferredNamespace = carriedNamespace
            namespaceCandidates = [carriedNamespace, savedNamespace]
        } else {
            requestedPreferredNamespace = savedNamespace
            namespaceCandidates = [savedNamespace]
        }
        if requestedPreferredNamespace.isEmpty, context.name != previousContextName {
            pendingNamespaceRevalidationContextName = context.name
        } else {
            pendingNamespaceRevalidationContextName = nil
        }
        state.resourceSearchQuery = ""
        state.clearResourceDetails()

        let cachedNamespaces = store.namespaces(context: context)
        if context.name == previousContextName, !cachedNamespaces.isEmpty {
            state.selectedNamespace = resolvedNamespace(
                contextName: context.name,
                preferred: requestedPreferredNamespace,
                availableNamespaces: cachedNamespaces,
                contextDefaultNamespace: nil
            )
        } else if !cachedNamespaces.isEmpty {
            state.selectedNamespace = resolvedNamespace(
                contextName: context.name,
                preferredCandidates: namespaceCandidates,
                availableNamespaces: cachedNamespaces,
                contextDefaultNamespace: nil
            )
        } else if !requestedPreferredNamespace.isEmpty {
            diagnostics.trace(
                "context",
                "checkpoint namespace=<redacted-namespace> context=<redacted-context> until namespace list is loaded"
            )
            // Navigation checkpoint supplies a namespace string before `listNamespaces` has run for this context.
            state.selectedNamespace = requestedPreferredNamespace
        } else {
            state.selectedNamespace = ""
        }
        if !isChangingContext,
           state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines) != previousNamespace {
            clearResourceBulkSelections()
        }
        // Apply store-backed lists directly so we avoid flashing an empty table before cached rows appear.
        applyCachedSnapshot(context: context, namespace: state.selectedNamespace)

        if triggerReload {
            scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: true, debounced: false)
        }

        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func setNamespace(_ namespace: String) {
        setNamespace(namespace, trackHistory: true, triggerReload: true)
    }

    private func setNamespace(_ namespace: String, trackHistory: Bool, triggerReload: Bool) {
        let trimmed = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        prepareNavigationMutation(trackHistory: trackHistory)
        diagnostics.log("setNamespace -> \(trimmed)")
        let previousNamespace = state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed != previousNamespace {
            invalidateHelmReleaseListRequest()
        }
        if trimmed != previousNamespace, let contextName = state.selectedContext?.name {
            stopAndClearTerminalSessions(contextName: contextName, namespace: previousNamespace)
        }
        if trimmed != previousNamespace {
            clearResourceBulkSelections()
        }
        cancelPendingLogReload()
        resourceDetailsTask?.cancel()
        pendingNamespaceRevalidationContextName = nil
        state.selectedNamespace = trimmed
        state.clearResourceListFreshness()
        if let contextName = state.selectedContext?.name {
            contextPreferences.savePreferredNamespace(trimmed, for: contextName)
            if !state.namespaces.contains(where: { $0.caseInsensitiveCompare(trimmed) == .orderedSame }) {
                rememberManualNamespace(trimmed, for: contextName)
            }
            rememberRecentNamespace(trimmed, for: contextName)
        }
        state.clearResourceDetails()
        if let context = state.selectedContext {
            applyCachedSnapshot(context: context, namespace: trimmed)
        }
        if triggerReload {
            // Namespace switches are explicit user intent; always revalidate namespace metadata
            // for this context to avoid stale cache ordering or missing namespace rows.
            scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: true, debounced: false)
        }
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func openOverviewModule(_ module: OverviewModule) {
        cancelPendingEventSourceNavigation()
        switch module {
        case .pods:
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.pod, trackHistory: false, triggerReload: false)
        case .deployments:
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.deployment, trackHistory: false, triggerReload: false)
        case .services:
            setSection(.networking, trackHistory: false, triggerReload: false)
            setWorkloadKind(.service, trackHistory: false, triggerReload: false)
        case .ingresses:
            setSection(.networking, trackHistory: false, triggerReload: false)
            setWorkloadKind(.ingress, trackHistory: false, triggerReload: false)
        case .configMaps:
            setSection(.config, trackHistory: false, triggerReload: false)
            setWorkloadKind(.configMap, trackHistory: false, triggerReload: false)
        case .cronJobs:
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.cronJob, trackHistory: false, triggerReload: false)
        case .nodes:
            setSection(.storage, trackHistory: false, triggerReload: false)
            setWorkloadKind(.node, trackHistory: false, triggerReload: false)
        case .events:
            setSection(.events, trackHistory: false, triggerReload: false)
        }
        refreshCurrentView()
        recordNavigationCheckpoint()
    }

    public func openOverviewSignal(_ item: OverviewSignalItem) {
        cancelPendingEventSourceNavigation()
        if let operatorResourceID = item.operatorResourceID,
           let resource = state.operatorResources.first(where: { $0.id == operatorResourceID }) {
            setSection(.helm, trackHistory: false, triggerReload: false)
            setOperatorResourceFocus(Self.isGitOpsOperatorResource(resource) ? .gitOps : .all)
            selectOperatorResource(resource)
            return
        }
        guard let target = item.target else { return }
        openOverviewReference(target)
    }

    public func openOverviewDependency(_ item: OverviewDependencyItem) {
        cancelPendingEventSourceNavigation()
        guard let target = item.primaryTarget else { return }
        openOverviewReference(target)
    }

    public func openOverviewGitOpsRollup(_ item: OverviewGitOpsRollupItem) {
        cancelPendingEventSourceNavigation()
        setSection(.helm, trackHistory: false, triggerReload: false)
        switch item.controller {
        case .all:
            setOperatorResourceFocus(.gitOps)
        case .flux:
            setOperatorResourceFocus(.flux)
        case .argoCD:
            setOperatorResourceFocus(.argoCD)
        case .unhealthy:
            setOperatorResourceFocus(.unhealthy)
        }
    }

    public var selectedDeploymentRelatedPods: [PodSummary] {
        guard let deployment = state.selectedDeployment else { return [] }
        return relatedPods(for: deployment)
    }

    public var selectedDeploymentRelatedReplicaSets: [ClusterResourceSummary] {
        guard let deployment = state.selectedDeployment else { return [] }
        return relatedReplicaSets(for: deployment)
    }

    public var selectedDeploymentRelatedEvents: [EventSummary] {
        guard let deployment = state.selectedDeployment else { return [] }
        return relatedEvents(kind: .deployment, name: deployment.name, namespace: deployment.namespace)
    }

    public var selectedServiceRelatedPods: [PodSummary] {
        guard let service = state.selectedService else { return [] }
        return relatedPods(for: service)
    }

    public var selectedServiceRelatedEvents: [EventSummary] {
        guard let service = state.selectedService else { return [] }
        return relatedEvents(kind: .service, name: service.name, namespace: service.namespace)
    }

    public var selectedPodRelatedNode: ClusterResourceSummary? {
        guard let pod = state.selectedPod,
              let nodeName = pod.nodeName?.trimmingCharacters(in: .whitespacesAndNewlines),
              !nodeName.isEmpty else {
            return nil
        }
        return state.nodes.first { $0.name == nodeName }
    }

    public var selectedPodRelatedEvents: [EventSummary] {
        guard let pod = state.selectedPod else { return [] }
        return relatedEvents(kind: .pod, name: pod.name, namespace: pod.namespace)
    }

    public var selectedServiceRelatedIngresses: [ClusterResourceSummary] {
        guard let service = state.selectedService else { return [] }
        return state.ingresses.filter { ingress in
            ingress.namespace == service.namespace
                && Self.serviceNames(fromIngressSecondaryText: ingress.secondaryText).contains(service.name)
        }
    }

    public var selectedStatefulSetRelatedPods: [PodSummary] {
        guard let statefulSet = state.selectedStatefulSet else { return [] }
        return relatedPods(forWorkloadResource: statefulSet)
    }

    public var selectedStatefulSetReplicaCounts: (ready: Int, desired: Int)? {
        Self.replicaCounts(from: state.selectedStatefulSet?.primaryText)
    }

    public var selectedDaemonSetRelatedPods: [PodSummary] {
        guard let daemonSet = state.selectedDaemonSet else { return [] }
        return relatedPods(forWorkloadResource: daemonSet)
    }

    public var selectedJobRelatedPods: [PodSummary] {
        guard let job = state.selectedJob else { return [] }
        return relatedPods(forWorkloadResource: job)
    }

    public var selectedCronJobRelatedJobs: [ClusterResourceSummary] {
        guard let cronJob = state.selectedCronJob else { return [] }
        return state.jobs.filter { job in
            job.namespace == cronJob.namespace
                && (job.name == cronJob.name || job.name.hasPrefix(cronJob.name + "-"))
        }
    }

    public var selectedHorizontalPodAutoscalerScaleTarget: HPAScaleTargetReference? {
        guard let hpa = state.selectedHorizontalPodAutoscaler,
              let target = Self.hpaScaleTarget(fromSecondaryText: hpa.secondaryText, namespace: hpa.namespace) else {
            return nil
        }

        switch target.kind {
        case .deployment:
            let namespace = target.namespace ?? state.selectedNamespace
            guard let deployment = state.deployments.first(where: { $0.name == target.name && $0.namespace == namespace }) else { return nil }
            return HPAScaleTargetReference(
                kind: .deployment,
                namespace: deployment.namespace,
                name: deployment.name,
                subtitle: "\(deployment.namespace) · \(deployment.replicaText) replicas",
                symbol: "square.stack.3d.up"
            )
        case .statefulSet:
            return statefulWorkloadTarget(target, symbol: "externaldrive.connected.to.line.below")
        case .daemonSet:
            return statefulWorkloadTarget(target, symbol: "server.rack")
        case .replicaSet:
            return statefulWorkloadTarget(target, symbol: "rectangle.stack")
        default:
            return nil
        }
    }

    public var selectedNodeRelatedPods: [PodSummary] {
        guard let node = state.selectedNode else { return [] }
        return state.pods.filter { pod in
            pod.nodeName == node.name
        }
    }

    public var selectedIngressRelatedServices: [ServiceSummary] {
        guard let ingress = state.selectedIngress else { return [] }
        let names = Set(Self.serviceNames(fromIngressSecondaryText: ingress.secondaryText))
        guard !names.isEmpty else { return [] }
        return state.services.filter { service in
            service.namespace == ingress.namespace && names.contains(service.name)
        }
    }

    public var selectedReplicaSetRelatedPods: [PodSummary] {
        guard let replicaSet = state.selectedReplicaSet else { return [] }
        return relatedPods(for: replicaSet)
    }

    public var selectedPersistentVolumeClaimRelatedPersistentVolume: ClusterResourceSummary? {
        guard let pvc = state.selectedPersistentVolumeClaim,
              let volumeName = Self.persistentVolumeName(fromPVCSecondaryText: pvc.secondaryText) else {
            return nil
        }
        return state.persistentVolumes.first { $0.name == volumeName }
    }

    public var selectedPersistentVolumeRelatedPersistentVolumeClaims: [ClusterResourceSummary] {
        guard let persistentVolume = state.selectedPersistentVolume else { return [] }
        return state.persistentVolumeClaims.filter { pvc in
            Self.persistentVolumeName(fromPVCSecondaryText: pvc.secondaryText) == persistentVolume.name
        }
    }

    public var selectedRBACBindingReferencedRole: ClusterResourceSummary? {
        guard let binding = state.selectedRBACResource,
              let reference = Self.rbacRoleReference(fromPrimaryText: binding.primaryText, bindingKind: binding.kind) else {
            return nil
        }

        switch reference.kind {
        case .role:
            return state.rbacRoles.first { role in
                role.name == reference.name && role.namespace == binding.namespace
            }
        case .clusterRole:
            return state.rbacClusterRoles.first { role in
                role.name == reference.name
            }
        default:
            return nil
        }
    }

    public var selectedRBACRoleRelatedBindings: [ClusterResourceSummary] {
        guard let role = state.selectedRBACResource,
              role.kind == .role || role.kind == .clusterRole else {
            return []
        }
        let bindings = state.rbacRoleBindings + state.rbacClusterRoleBindings
        return bindings.filter { binding in
            guard let reference = Self.rbacRoleReference(fromPrimaryText: binding.primaryText, bindingKind: binding.kind),
                  reference.kind == role.kind,
                  reference.name == role.name else {
                return false
            }
            if role.kind == .role {
                return binding.namespace == role.namespace
            }
            return true
        }
    }

    public func relatedEvents(for resource: ClusterResourceSummary) -> [EventSummary] {
        relatedEvents(kind: resource.kind, name: resource.name, namespace: resource.namespace)
    }

    public func relatedEvents(kind: KubeResourceKind, name: String, namespace: String?) -> [EventSummary] {
        uniqueEvents(state.events + state.overviewEvents).filter { event in
            guard event.objectName == name else { return false }
            if let eventKind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines), !eventKind.isEmpty {
                guard eventKind.caseInsensitiveCompare(kind.singularTypeName) == .orderedSame else { return false }
            }
            guard kind.isNamespaced, let namespace, !namespace.isEmpty else { return true }
            guard let eventNamespace = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines), !eventNamespace.isEmpty else {
                return true
            }
            return eventNamespace == namespace
        }
    }

    public func openSelectedDeploymentPods() {
        guard let pod = selectedDeploymentRelatedPods.first else { return }
        openDeploymentRelatedPod(pod)
    }

    public func openDeploymentRelatedPod(_ pod: PodSummary) {
        openPodRelationship(pod)
    }

    public func openPodRelatedNode(_ node: ClusterResourceSummary) {
        setSection(.storage, trackHistory: false, triggerReload: false)
        setWorkloadKind(.node, trackHistory: false, triggerReload: false)
        selectNode(node, trackHistory: true)
    }

    public func openRelatedEvent(_ event: EventSummary) {
        setSection(.events, trackHistory: false, triggerReload: false)
        selectEvent(event, trackHistory: true)
    }

    public func openDeploymentRelatedReplicaSet(_ replicaSet: ClusterResourceSummary) {
        setSection(.workloads, trackHistory: false, triggerReload: false)
        setWorkloadKind(.replicaSet, trackHistory: false, triggerReload: false)
        selectReplicaSet(replicaSet, trackHistory: true)
    }

    public func openSelectedServicePods() {
        guard let pod = selectedServiceRelatedPods.first else { return }
        openServiceRelatedPod(pod)
    }

    public func openServiceRelatedPod(_ pod: PodSummary) {
        openPodRelationship(pod)
    }

    public func openServiceRelatedIngress(_ ingress: ClusterResourceSummary) {
        setSection(.networking, trackHistory: false, triggerReload: false)
        setWorkloadKind(.ingress, trackHistory: false, triggerReload: false)
        selectIngress(ingress, trackHistory: true)
    }

    public func openStatefulSetRelatedPod(_ pod: PodSummary) {
        openPodRelationship(pod)
    }

    public func openDaemonSetRelatedPod(_ pod: PodSummary) {
        openPodRelationship(pod)
    }

    public func openJobRelatedPod(_ pod: PodSummary) {
        openPodRelationship(pod)
    }

    public func openCronJobRelatedJob(_ job: ClusterResourceSummary) {
        setSection(.workloads, trackHistory: false, triggerReload: false)
        setWorkloadKind(.job, trackHistory: false, triggerReload: false)
        selectJob(job, trackHistory: true)
    }

    public func openHorizontalPodAutoscalerScaleTarget(_ target: HPAScaleTargetReference) {
        setSection(.workloads, trackHistory: false, triggerReload: false)
        setWorkloadKind(target.kind, trackHistory: false, triggerReload: false)
        switch target.kind {
        case .deployment:
            let namespace = target.namespace ?? state.selectedNamespace
            selectDeployment(
                state.deployments.first { $0.name == target.name && $0.namespace == namespace },
                trackHistory: true
            )
        case .statefulSet:
            selectStatefulSet(matchingTarget: target)
        case .daemonSet:
            selectDaemonSet(matchingTarget: target)
        case .replicaSet:
            selectReplicaSet(matchingTarget: target)
        default:
            recordNavigationCheckpoint()
        }
    }

    public func openNodeRelatedPod(_ pod: PodSummary) {
        openPodRelationship(pod)
    }

    public func openIngressRelatedService(_ service: ServiceSummary) {
        setSection(.networking, trackHistory: false, triggerReload: false)
        setWorkloadKind(.service, trackHistory: false, triggerReload: false)
        selectService(service, trackHistory: true)
    }

    public func openReplicaSetRelatedPod(_ pod: PodSummary) {
        openPodRelationship(pod)
    }

    public func openPersistentVolumeClaimRelatedPersistentVolume(_ persistentVolume: ClusterResourceSummary) {
        setSection(.storage, trackHistory: false, triggerReload: false)
        setWorkloadKind(.persistentVolume, trackHistory: false, triggerReload: false)
        selectPersistentVolume(persistentVolume, trackHistory: true)
    }

    public func openPersistentVolumeRelatedPersistentVolumeClaim(_ persistentVolumeClaim: ClusterResourceSummary) {
        setSection(.storage, trackHistory: false, triggerReload: false)
        setWorkloadKind(.persistentVolumeClaim, trackHistory: false, triggerReload: false)
        selectPersistentVolumeClaim(persistentVolumeClaim, trackHistory: true)
    }

    public func openRBACBindingReferencedRole(_ role: ClusterResourceSummary) {
        setSection(.rbac, trackHistory: false, triggerReload: false)
        setWorkloadKind(role.kind, trackHistory: false, triggerReload: false)
        selectRBACResource(role, trackHistory: true)
    }

    public func openRBACRoleRelatedBinding(_ binding: ClusterResourceSummary) {
        setSection(.rbac, trackHistory: false, triggerReload: false)
        setWorkloadKind(binding.kind, trackHistory: false, triggerReload: false)
        selectRBACResource(binding, trackHistory: true)
    }

    private func openPodRelationship(_ pod: PodSummary) {
        setSection(.workloads, trackHistory: false, triggerReload: false)
        setWorkloadKind(.pod, trackHistory: false, triggerReload: false)
        selectPod(pod, trackHistory: true)
    }

    private func relatedPods(for deployment: DeploymentSummary) -> [PodSummary] {
        let replicaSetPods = relatedReplicaSets(for: deployment).flatMap(relatedPods(for:))
        let prefixPods = state.pods.filter { pod in
            pod.namespace == deployment.namespace
                && (pod.name == deployment.name || pod.name.hasPrefix(deployment.name + "-"))
        }
        return uniquePods(replicaSetPods + prefixPods)
    }

    private func relatedReplicaSets(for deployment: DeploymentSummary) -> [ClusterResourceSummary] {
        state.replicaSets.filter { replicaSet in
            replicaSet.namespace == deployment.namespace
                && (
                    replicaSet.name == deployment.name
                    || replicaSet.name.hasPrefix(deployment.name + "-")
                    || replicaSet.secondaryText.localizedCaseInsensitiveContains("Deployment/\(deployment.name)")
                    || Self.ownerReferencesContain(replicaSet.ownerReferencesLine, kind: .deployment, name: deployment.name)
                )
        }
    }

    private func relatedPods(for replicaSet: ClusterResourceSummary) -> [PodSummary] {
        relatedPods(forWorkloadResource: replicaSet)
    }

    private func relatedPods(forWorkloadResource resource: ClusterResourceSummary) -> [PodSummary] {
        state.pods.filter { pod in
            pod.namespace == resource.namespace
                && (
                    pod.name == resource.name
                    || pod.name.hasPrefix(resource.name + "-")
                    || Self.ownerReferencesContain(pod.ownerReferencesLine, kind: resource.kind, name: resource.name)
                )
        }
    }

    private func relatedPods(for service: ServiceSummary) -> [PodSummary] {
        let deploymentMatches = state.deployments.filter { deployment in
            deployment.namespace == service.namespace
                && (deployment.name == service.name || Self.selectorsOverlap(service.selector, deployment.selector))
        }
        let deploymentPods = deploymentMatches.flatMap(relatedPods(for:))
        if !deploymentPods.isEmpty {
            return uniquePods(deploymentPods)
        }
        let prefixPods = state.pods.filter { pod in
            pod.namespace == service.namespace
                && (pod.name == service.name || pod.name.hasPrefix(service.name + "-"))
        }
        return uniquePods(prefixPods)
    }

    private func statefulWorkloadTarget(_ target: (kind: KubeResourceKind, namespace: String?, name: String), symbol: String) -> HPAScaleTargetReference? {
        let resources: [ClusterResourceSummary]
        switch target.kind {
        case .statefulSet:
            resources = state.statefulSets
        case .daemonSet:
            resources = state.daemonSets
        case .replicaSet:
            resources = state.replicaSets
        default:
            return nil
        }
        guard let resource = resources.first(where: { $0.name == target.name && $0.namespace == target.namespace }) else { return nil }
        return HPAScaleTargetReference(
            kind: resource.kind,
            namespace: resource.namespace,
            name: resource.name,
            subtitle: "\(resource.namespace ?? state.selectedNamespace) · \(resource.primaryText)",
            symbol: symbol
        )
    }

    private func selectStatefulSet(matchingTarget target: HPAScaleTargetReference) {
        selectStatefulSet(
            state.statefulSets.first { $0.name == target.name && $0.namespace == target.namespace },
            trackHistory: true
        )
    }

    private func selectDaemonSet(matchingTarget target: HPAScaleTargetReference) {
        selectDaemonSet(
            state.daemonSets.first { $0.name == target.name && $0.namespace == target.namespace },
            trackHistory: true
        )
    }

    private func selectReplicaSet(matchingTarget target: HPAScaleTargetReference) {
        selectReplicaSet(
            state.replicaSets.first { $0.name == target.name && $0.namespace == target.namespace },
            trackHistory: true
        )
    }

    private func uniquePods(_ pods: [PodSummary]) -> [PodSummary] {
        var seen = Set<String>()
        return pods.filter { pod in
            seen.insert(pod.id).inserted
        }
    }

    private func uniqueEvents(_ events: [EventSummary]) -> [EventSummary] {
        var seen = Set<String>()
        return events.filter { event in
            seen.insert(event.id).inserted
        }
    }

    private static func selectorsOverlap(_ lhs: [String: String]?, _ rhs: [String: String]?) -> Bool {
        guard let lhs, let rhs, !lhs.isEmpty, !rhs.isEmpty else { return false }
        return lhs.contains { key, value in
            rhs[key]?.caseInsensitiveCompare(value) == .orderedSame
        }
    }

    private static func ownerReferencesContain(_ line: String?, kind: KubeResourceKind, name: String) -> Bool {
        guard let line else { return false }
        let expected = "\(kind.singularTypeName)/\(name)"
        return line
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .contains { $0.caseInsensitiveCompare(expected) == .orderedSame }
    }

    private static func serviceNames(fromIngressSecondaryText text: String) -> [String] {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        let lowercased = trimmed.lowercased()
        guard lowercased.hasPrefix("service ") || lowercased.hasPrefix("services ") else { return [] }

        let prefixLength = lowercased.hasPrefix("services ") ? "services ".count : "service ".count
        let start = trimmed.index(trimmed.startIndex, offsetBy: prefixLength)
        return trimmed[start...]
            .split(separator: ",")
            .map { raw in
                raw
                    .split(separator: ":")
                    .first
                    .map(String.init)?
                    .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            }
            .filter { !$0.isEmpty }
    }

    private static func persistentVolumeName(fromPVCSecondaryText text: String) -> String? {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.lowercased().hasPrefix("pv ") else { return nil }
        let start = trimmed.index(trimmed.startIndex, offsetBy: "PV ".count)
        let name = trimmed[start...]
            .split(separator: "·")
            .first
            .map(String.init)?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard let name, !name.isEmpty else { return nil }
        return name
    }

    private static func hpaScaleTarget(fromSecondaryText text: String, namespace: String?) -> (kind: KubeResourceKind, namespace: String?, name: String)? {
        let parts = text
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .split(separator: "/", maxSplits: 1)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        guard parts.count == 2, !parts[1].isEmpty else { return nil }

        let kind: KubeResourceKind?
        switch parts[0].lowercased() {
        case "deployment", "deployments":
            kind = .deployment
        case "statefulset", "statefulsets":
            kind = .statefulSet
        case "daemonset", "daemonsets":
            kind = .daemonSet
        case "replicaset", "replicasets":
            kind = .replicaSet
        default:
            kind = nil
        }
        guard let kind else { return nil }
        return (kind, namespace, parts[1])
    }

    private static func replicaCounts(from text: String?) -> (ready: Int, desired: Int)? {
        guard let text else { return nil }
        let firstToken = text
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .split(whereSeparator: \.isWhitespace)
            .first
        guard let firstToken,
              let slash = firstToken.firstIndex(of: "/"),
              let ready = Int(firstToken[..<slash]),
              let desired = Int(firstToken[firstToken.index(after: slash)...])
        else {
            return nil
        }
        return (ready, desired)
    }

    private static func rbacRoleReference(fromPrimaryText text: String, bindingKind: KubeResourceKind) -> (kind: KubeResourceKind, name: String)? {
        guard bindingKind == .roleBinding || bindingKind == .clusterRoleBinding else { return nil }
        let normalized = text
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .replacingOccurrences(of: "→", with: "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalized.isEmpty else { return nil }

        let parts = normalized
            .split(separator: "/", maxSplits: 1)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        if parts.count == 2 {
            switch parts[0].lowercased() {
            case "role":
                return (.role, parts[1])
            case "clusterrole":
                return (.clusterRole, parts[1])
            default:
                return nil
            }
        }

        if bindingKind == .clusterRoleBinding {
            return (.clusterRole, normalized)
        }
        return (.role, normalized)
    }

    private func openOverviewReference(_ reference: OverviewResourceReference) {
        let namespace = reference.namespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !namespace.isEmpty,
           namespace.caseInsensitiveCompare(state.selectedNamespace) != .orderedSame {
            setNamespace(namespace, trackHistory: false, triggerReload: false)
        }

        switch reference.kind {
        case .pod:
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.pod, trackHistory: false, triggerReload: false)
            let pod = state.pods.first { $0.name == reference.name && $0.namespace == namespace }
                ?? state.overviewPods.first { $0.name == reference.name && (namespace.isEmpty || $0.namespace == namespace) }
                ?? PodSummary(name: reference.name, namespace: namespace.isEmpty ? state.selectedNamespace : namespace, status: "Unknown")
            selectPod(pod, trackHistory: true)
        case .deployment:
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.deployment, trackHistory: false, triggerReload: false)
            if let deployment = state.deployments.first(where: { $0.name == reference.name && (namespace.isEmpty || $0.namespace == namespace) }) {
                selectDeployment(deployment, trackHistory: true)
            } else {
                refreshCurrentView()
                recordNavigationCheckpoint()
            }
        case .service:
            setSection(.networking, trackHistory: false, triggerReload: false)
            setWorkloadKind(.service, trackHistory: false, triggerReload: false)
            if let service = state.services.first(where: { $0.name == reference.name && (namespace.isEmpty || $0.namespace == namespace) }) {
                selectService(service, trackHistory: true)
            } else {
                refreshCurrentView()
                recordNavigationCheckpoint()
            }
        case .event:
            if let event = (state.overviewEvents + state.events).first(where: { $0.id == reference.name }) {
                openEventSource(event)
            } else {
                openOverviewModule(.events)
            }
        case .persistentVolumeClaim:
            setSection(.storage, trackHistory: false, triggerReload: false)
            setWorkloadKind(.persistentVolumeClaim, trackHistory: false, triggerReload: false)
            if let persistentVolumeClaim = state.persistentVolumeClaims.first(where: { $0.name == reference.name && (namespace.isEmpty || $0.namespace == namespace) }) {
                selectPersistentVolumeClaim(persistentVolumeClaim, trackHistory: true)
            } else {
                refreshCurrentView()
                recordNavigationCheckpoint()
            }
        case .persistentVolume:
            setSection(.storage, trackHistory: false, triggerReload: false)
            setWorkloadKind(.persistentVolume, trackHistory: false, triggerReload: false)
            if let persistentVolume = state.persistentVolumes.first(where: { $0.name == reference.name }) {
                selectPersistentVolume(persistentVolume, trackHistory: true)
            } else {
                refreshCurrentView()
                recordNavigationCheckpoint()
            }
        default:
            openOverviewModule(.pods)
        }
    }

    /// Sets section, namespace, and selection from an event `involvedObject` (workload or namespaced resource).
    public func openEventSource(_ event: EventSummary) {
        cancelPendingEventSourceNavigation()
        let targetNs = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !targetNs.isEmpty && targetNs != state.selectedNamespace {
            stageEventSourceNavigation(event)
            setNamespace(targetNs, trackHistory: false, triggerReload: true)
            return
        }
        navigateToEventSource(event)
    }

    private func stageEventSourceNavigation(_ event: EventSummary) {
        let eventNamespace = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        pendingOpenEventSource = PendingEventSourceNavigation(
            event: event,
            contextName: state.selectedContext?.name,
            namespace: eventNamespace.isEmpty ? state.selectedNamespace : eventNamespace
        )
    }

    private func cancelPendingEventSourceNavigation() {
        pendingOpenEventSource = nil
        navigateFromEventFetchAttempts = 0
    }

    private func deferFetchOrShowEventDetail(event: EventSummary, showEventDetail: () -> Void) {
        if navigateFromEventFetchAttempts < 2 {
            navigateFromEventFetchAttempts += 1
            stageEventSourceNavigation(event)
            scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: false, debounced: false)
        } else {
            cancelPendingEventSourceNavigation()
            showEventDetail()
        }
    }

    private func navigateToEventSource(_ event: EventSummary) {
        let startedFromEvents = state.selectedSection == .events
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        let name = event.objectName.trimmingCharacters(in: .whitespacesAndNewlines)
        let namespace = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        defer {
            if startedFromEvents, state.selectedSection != .events {
                state.resourceSearchQuery = ""
            }
        }

        func matchesEventObject(candidateName: String, candidateNamespace: String?, requireNamespace: Bool = true) -> Bool {
            guard candidateName == name else { return false }
            guard requireNamespace, !namespace.isEmpty else { return true }
            return candidateNamespace == namespace
        }

        func showEventDetail() {
            setSection(.events, trackHistory: false, triggerReload: false)
            selectEvent(event, trackHistory: true)
        }

        switch kind {
        case "pod":
            if let pod = state.pods.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.pod, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectPod(pod, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "deployment":
            if let deployment = state.deployments.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.deployment, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectDeployment(deployment, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "statefulset":
            if let resource = state.statefulSets.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.statefulSet, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectStatefulSet(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "daemonset":
            if let resource = state.daemonSets.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.daemonSet, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectDaemonSet(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "job":
            if let resource = state.jobs.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.job, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectJob(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "cronjob":
            if let resource = state.cronJobs.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.cronJob, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectCronJob(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "replicaset":
            if let resource = state.replicaSets.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.replicaSet, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectReplicaSet(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "service":
            if let service = state.services.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.networking, trackHistory: false, triggerReload: false)
                setWorkloadKind(.service, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectService(service, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "ingress":
            if let resource = state.ingresses.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.networking, trackHistory: false, triggerReload: false)
                setWorkloadKind(.ingress, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectIngress(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "configmap":
            if let resource = state.configMaps.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.config, trackHistory: false, triggerReload: false)
                setWorkloadKind(.configMap, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectConfigMap(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "secret":
            if let resource = state.secrets.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.config, trackHistory: false, triggerReload: false)
                setWorkloadKind(.secret, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectSecret(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "node":
            if let resource = state.nodes.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace, requireNamespace: false) }) {
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.node, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectNode(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "persistentvolumeclaim":
            if let resource = state.persistentVolumeClaims.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.persistentVolumeClaim, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectPersistentVolumeClaim(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "persistentvolume":
            if let resource = state.persistentVolumes.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace, requireNamespace: false) }) {
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.persistentVolume, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectPersistentVolume(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "storageclass":
            if let resource = state.storageClasses.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace, requireNamespace: false) }) {
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.storageClass, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectStorageClass(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "horizontalpodautoscaler":
            if let resource = state.horizontalPodAutoscalers.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.horizontalPodAutoscaler, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectHorizontalPodAutoscaler(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "networkpolicy":
            if let resource = state.networkPolicies.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.networking, trackHistory: false, triggerReload: false)
                setWorkloadKind(.networkPolicy, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectNetworkPolicy(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "":
            if let pod = state.pods.first(where: { matchesEventObject(candidateName: $0.name, candidateNamespace: $0.namespace) }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.pod, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectPod(pod, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        default:
            navigateFromEventFetchAttempts = 0
            showEventDetail()
        }
    }

    public func toggleFavorite(for context: KubeContext) {
        state.toggleFavoriteContext(named: context.name)
        contextPreferences.saveFavoriteContextNames(state.favoriteContextNames)
    }

    public func toggleProductionMark(for context: KubeContext) {
        guard let id = productionContextID(for: context) else { return }
        state.toggleManualProductionContext(id: id)
        contextPreferences.saveManualProductionContextIDs(state.manualProductionContextIDs)
    }

    public func isManuallyMarkedProduction(_ context: KubeContext) -> Bool {
        guard let id = productionContextID(for: context) else { return false }
        return state.isManualProductionContext(id: id)
    }

    public func isProductionContext(_ context: KubeContext) -> Bool {
        // The name marker is the common path and does not require constructing the
        // normalized preference identifier used by an explicit manual mark.
        let nameMarkerMatch: Bool
        if let cachedProductionNameMarker, cachedProductionNameMarker.contextName == context.name {
            nameMarkerMatch = cachedProductionNameMarker.isMatch
        } else {
            let normalizedName = context.name.lowercased()
            nameMarkerMatch = Self.productionContextNameMarkers.contains(where: { normalizedName.contains($0) })
            cachedProductionNameMarker = (context.name, nameMarkerMatch)
        }
        if nameMarkerMatch {
            return true
        }

        return isManuallyMarkedProduction(context)
    }

    public func toggleFavoriteResource(kind: KubeResourceKind, namespace: String?, name: String) {
        guard let id = favoriteResourceID(kind: kind, namespace: namespace, name: name) else { return }
        state.toggleFavoriteResource(id: id)
        contextPreferences.saveFavoriteResourceIDs(state.favoriteResourceIDs)
    }

    public func isFavoriteResource(kind: KubeResourceKind, namespace: String?, name: String) -> Bool {
        guard let id = favoriteResourceID(kind: kind, namespace: namespace, name: name) else { return false }
        return state.isFavoriteResource(id: id)
    }

    public func toggleFavoriteOperatorResource(_ resource: OperatorResourceSummary) {
        state.toggleFavoriteResource(id: favoriteOperatorResourceID(resource))
        contextPreferences.saveFavoriteResourceIDs(state.favoriteResourceIDs)
    }

    public func isFavoriteOperatorResource(_ resource: OperatorResourceSummary) -> Bool {
        state.isFavoriteResource(id: favoriteOperatorResourceID(resource))
    }

    public var operatorPrinterColumnFamilyForCustomization: String? {
        if let selected = state.selectedOperatorResource {
            return selected.family
        }
        return pagedOperatorResources.first?.family ?? visibleOperatorResources.first?.family
    }

    public var showsOperatorPrinterColumnsForCurrentFamily: Bool {
        guard let family = operatorPrinterColumnFamilyForCustomization else { return true }
        return !hiddenOperatorPrinterColumnFamilies.contains(family)
    }

    public func toggleOperatorPrinterColumnsForCurrentFamily() {
        guard let family = operatorPrinterColumnFamilyForCustomization else { return }
        if hiddenOperatorPrinterColumnFamilies.contains(family) {
            hiddenOperatorPrinterColumnFamilies.remove(family)
        } else {
            hiddenOperatorPrinterColumnFamilies.insert(family)
        }
        contextPreferences.saveHiddenOperatorPrinterColumnFamilies(hiddenOperatorPrinterColumnFamilies)
    }

    public func pageOperatorResourcesBackward() {
        let currentPage = clampedOperatorResourcePage(forResourceCount: visibleOperatorResources.count)
        operatorResourcePage = max(0, currentPage - 1)
    }

    public func pageOperatorResourcesForward() {
        guard canPageOperatorResourcesForward else { return }
        operatorResourcePage = clampedOperatorResourcePage(
            forResourceCount: visibleOperatorResources.count
        ) + 1
    }

    private func clampedOperatorResourcePage(forResourceCount count: Int) -> Int {
        guard count > 0 else { return 0 }
        return min(operatorResourcePage, (count - 1) / Self.operatorResourcePageSize)
    }

    public func selectPod(_ pod: PodSummary?) {
        selectPod(pod, trackHistory: true)
    }

    public func focusTerminalPodInspector(_ pod: PodSummary, reloadLogs: Bool = false, loadDetails: Bool = false) {
        selectedLogContainer = reloadLogs ? defaultLogContainerName(for: pod) : ""
        selectPod(pod, trackHistory: false)
        if loadDetails {
            loadResourceDetailsForCurrentSelection()
        } else if reloadLogs {
            reloadLogsForSelection()
        }
    }

    private func defaultLogContainerName(for pod: PodSummary) -> String {
        pod.logContainerNames.first ?? ""
    }

    private func selectPod(_ pod: PodSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedPod(pod)
        if !selectedLogContainer.isEmpty,
           pod?.logContainerNames.contains(selectedLogContainer) != true {
            selectedLogContainer = ""
        }
        state.selectedWorkloadKind = .pod
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectDeployment(_ deployment: DeploymentSummary?) {
        selectDeployment(deployment, trackHistory: true)
    }

    private func selectDeployment(_ deployment: DeploymentSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedDeployment(deployment)
        state.selectedWorkloadKind = .deployment
        if let deployment {
            scaleReplicaInput = max(0, deployment.desiredReplicas)
        }
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectService(_ service: ServiceSummary?) {
        selectService(service, trackHistory: true)
    }

    private func selectService(_ service: ServiceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedService(service)
        state.selectedWorkloadKind = .service
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectEvent(_ event: EventSummary?) {
        selectEvent(event, trackHistory: true)
    }

    private func selectEvent(_ event: EventSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedEvent(event)
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectHelmRelease(_ release: HelmReleaseSummary?) {
        selectHelmRelease(release, trackHistory: true)
    }

    private func selectHelmRelease(_ release: HelmReleaseSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedOperatorResource(nil)
        state.setSelectedHelmRelease(release)
        loadHelmDetailsForCurrentSelection()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectOperatorResource(_ resource: OperatorResourceSummary?) {
        selectOperatorResource(resource, trackHistory: true)
    }

    private func selectOperatorResource(_ resource: OperatorResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedHelmRelease(nil)
        state.setSelectedOperatorResource(resource)
        loadOperatorResourceDetailsForCurrentSelection()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectStatefulSet(_ resource: ClusterResourceSummary?) {
        selectStatefulSet(resource, trackHistory: true)
    }

    private func selectStatefulSet(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedStatefulSet(resource)
        state.selectedWorkloadKind = .statefulSet
        if let replicas = Self.replicaCounts(from: resource?.primaryText)?.desired {
            scaleReplicaInput = max(0, replicas)
        }
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectDaemonSet(_ resource: ClusterResourceSummary?) {
        selectDaemonSet(resource, trackHistory: true)
    }

    private func selectDaemonSet(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedDaemonSet(resource)
        state.selectedWorkloadKind = .daemonSet
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectJob(_ resource: ClusterResourceSummary?) {
        selectJob(resource, trackHistory: true)
    }

    private func selectJob(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedJob(resource)
        state.selectedWorkloadKind = .job
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectCronJob(_ resource: ClusterResourceSummary?) {
        selectCronJob(resource, trackHistory: true)
    }

    private func selectCronJob(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedCronJob(resource)
        state.selectedWorkloadKind = .cronJob
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectReplicaSet(_ resource: ClusterResourceSummary?) {
        selectReplicaSet(resource, trackHistory: true)
    }

    private func selectReplicaSet(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedReplicaSet(resource)
        state.selectedWorkloadKind = .replicaSet
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectPersistentVolumeClaim(_ resource: ClusterResourceSummary?) {
        selectPersistentVolumeClaim(resource, trackHistory: true)
    }

    private func selectPersistentVolumeClaim(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedPersistentVolumeClaim(resource)
        state.selectedWorkloadKind = .persistentVolumeClaim
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectPersistentVolume(_ resource: ClusterResourceSummary?) {
        selectPersistentVolume(resource, trackHistory: true)
    }

    private func selectPersistentVolume(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedPersistentVolume(resource)
        state.selectedWorkloadKind = .persistentVolume
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectStorageClass(_ resource: ClusterResourceSummary?) {
        selectStorageClass(resource, trackHistory: true)
    }

    private func selectStorageClass(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedStorageClass(resource)
        state.selectedWorkloadKind = .storageClass
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectHorizontalPodAutoscaler(_ resource: ClusterResourceSummary?) {
        selectHorizontalPodAutoscaler(resource, trackHistory: true)
    }

    private func selectHorizontalPodAutoscaler(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedHorizontalPodAutoscaler(resource)
        state.selectedWorkloadKind = .horizontalPodAutoscaler
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectNetworkPolicy(_ resource: ClusterResourceSummary?) {
        selectNetworkPolicy(resource, trackHistory: true)
    }

    private func selectNetworkPolicy(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedNetworkPolicy(resource)
        state.selectedWorkloadKind = .networkPolicy
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectEndpoint(_ resource: ClusterResourceSummary?) {
        selectEndpoint(resource, trackHistory: true)
    }

    private func selectEndpoint(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedEndpoint(resource)
        state.selectedWorkloadKind = .endpoint
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func setSelectedCronJobSuspended(_ suspend: Bool) {
        guard writeActionsEnabled,
              let context = state.selectedContext,
              let cronJob = state.selectedCronJob else { return }
        Task {
            do {
                try await kubeClient.patchCronJobSuspend(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    name: cronJob.name,
                    suspend: suspend
                )
                refreshCurrentView()
            } catch {
                state.setError(error)
            }
        }
    }

    public func createManualJobFromSelectedCronJob() {
        guard writeActionsEnabled,
              let cronJob = state.selectedCronJob else { return }
        let jobName = "\(cronJob.name)-manual-\(Int(Date().timeIntervalSince1970))"
        pendingWriteAction = .createJobFromCronJob(cronJobName: cronJob.name, jobName: jobName)
    }

    private func runCreateManualJobFromCronJob(cronJobName: String, jobName: String) {
        guard let context = state.selectedContext else { return }
        Task {
            do {
                try await kubeClient.createJobFromCronJob(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    cronJobName: cronJobName,
                    jobName: jobName
                )
                setWorkloadKind(.job, trackHistory: false, triggerReload: true)
                refreshCurrentView()
            } catch {
                state.setError(error)
            }
        }
    }

    public func selectIngress(_ resource: ClusterResourceSummary?) {
        selectIngress(resource, trackHistory: true)
    }

    private func selectIngress(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedIngress(resource)
        state.selectedWorkloadKind = .ingress
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectConfigMap(_ resource: ClusterResourceSummary?) {
        selectConfigMap(resource, trackHistory: true)
    }

    private func selectConfigMap(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedConfigMap(resource)
        state.selectedWorkloadKind = .configMap
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectSecret(_ resource: ClusterResourceSummary?) {
        selectSecret(resource, trackHistory: true)
    }

    private func selectSecret(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedSecret(resource)
        state.selectedWorkloadKind = .secret
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectNode(_ resource: ClusterResourceSummary?) {
        selectNode(resource, trackHistory: true)
    }

    private func selectNode(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedNode(resource)
        state.selectedWorkloadKind = .node
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectRBACResource(_ resource: ClusterResourceSummary?) {
        selectRBACResource(resource, trackHistory: true)
    }

    private func selectRBACResource(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedRBACResource(resource)
        if let resource {
            state.selectedWorkloadKind = resource.kind
            useSelectedRBACResourceForCanI()
        }
        loadResourceDetailsForCurrentSelectionIfNeeded()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func reloadLogsForSelection() {
        tailLogsReloadTask?.cancel()
        tailLogsReloadTask = nil
        scheduledLogsReloadTask?.cancel()
        startLogsReloadForSelection()
    }

    public func toggleLogStreamPause() {
        guard isLogTailModeEnabled else { return }
        isLogStreamPaused.toggle()
    }

    private func scheduleLogsReloadForSelection() {
        resourceDetailsTask?.cancel()
        tailLogsReloadTask?.cancel()
        tailLogsReloadTask = nil
        let requestID = UUID()
        latestLogsReloadRequestID = requestID
        scheduledLogsReloadTask?.cancel()
        logsReloadTask?.cancel()

        let debounce = logsReloadDebounceNanoseconds
        scheduledLogsReloadTask = Task { [weak self] in
            do {
                try await Task.sleep(nanoseconds: debounce)
            } catch {
                return
            }
            guard !Task.isCancelled else { return }
            self?.startLogsReloadForSelection(requestID: requestID)
        }
    }

    private func startLogsReloadForSelection(requestID requestedRequestID: UUID? = nil) {
        resourceDetailsTask?.cancel()
        tailLogsReloadTask?.cancel()
        tailLogsReloadTask = nil
        logsReloadTask?.cancel()
        scheduledLogsReloadTask?.cancel()
        let requestID = requestedRequestID ?? UUID()
        latestLogsReloadRequestID = requestID

        guard let context = state.selectedContext else { return }

        if context.name == demoContextName {
            applyDemoLogsForCurrentSelection(contextName: context.name, namespace: state.selectedNamespace)
            return
        }

        let sources = state.kubeConfigSources
        let namespace = state.selectedNamespace
        let kind = state.selectedWorkloadKind
        let filter = selectedLogPreset.filter
        let previous = includePreviousLogs
        let container = selectedLogContainerName
        let pod = state.selectedPod
        let service = state.selectedService
        let deployment = state.selectedDeployment

        logsReloadTask = Task { [weak self] in
            guard let self else { return }
            do {
                switch kind {
                case .pod:
                    guard let pod else { return }
                    self.state.showCachedPodLogs(contextName: context.name, namespace: namespace, podName: pod.name)
                    self.state.setLastLogFetchError(nil)
                    self.state.isLoadingLogs = true
                    defer {
                        if self.isCurrentLogsReloadRequest(requestID) {
                            self.state.isLoadingLogs = false
                        }
                    }
                    let logs: String
                    if let container {
                        logs = try await self.kubeClient.podLogs(
                            from: sources,
                            context: context,
                            namespace: namespace,
                            podName: pod.name,
                            container: container,
                            filter: filter,
                            previous: previous
                        )
                    } else {
                        logs = try await self.kubeClient.podLogs(
                            from: sources,
                            context: context,
                            namespace: namespace,
                            podName: pod.name,
                            containers: pod.logContainerNames,
                            filter: filter,
                            previous: previous
                        )
                    }
                    guard self.isCurrentLogsReloadRequest(requestID) else { return }
                    self.commitPodLogFetch(
                        logs,
                        contextName: context.name,
                        namespace: namespace,
                        podName: pod.name
                    )
                    self.scheduleNextTailLogsReload()
                case .service:
                    guard let service else { return }
                    self.state.showCachedUnifiedLogs(contextName: context.name, namespace: namespace, kind: .service, resourceName: service.name)
                    self.state.setLastLogFetchError(nil)
                    self.state.isLoadingLogs = true
                    defer {
                        if self.isCurrentLogsReloadRequest(requestID) {
                            self.state.isLoadingLogs = false
                        }
                    }
                    let unified = try await self.kubeClient.unifiedLogsForService(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        service: service,
                        filter: filter,
                        previous: previous
                    )
                    guard self.isCurrentLogsReloadRequest(requestID) else { return }
                    let scoped = self.scopedUnifiedLogResult(
                        mergedText: unified.mergedText,
                        podNames: unified.podNames
                    )
                    self.commitUnifiedLogFetch(
                        scoped.mergedText,
                        pods: scoped.podNames,
                        contextName: context.name,
                        namespace: namespace,
                        kind: .service,
                        resourceName: service.name
                    )
                    self.scheduleNextTailLogsReload()
                case .deployment:
                    guard let deployment else { return }
                    self.state.showCachedUnifiedLogs(contextName: context.name, namespace: namespace, kind: .deployment, resourceName: deployment.name)
                    self.state.setLastLogFetchError(nil)
                    self.state.isLoadingLogs = true
                    defer {
                        if self.isCurrentLogsReloadRequest(requestID) {
                            self.state.isLoadingLogs = false
                        }
                    }
                    let unified = try await self.kubeClient.unifiedLogsForDeployment(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        deployment: deployment,
                        filter: filter,
                        previous: previous
                    )
                    guard self.isCurrentLogsReloadRequest(requestID) else { return }
                    let scoped = self.scopedUnifiedLogResult(
                        mergedText: unified.mergedText,
                        podNames: unified.podNames
                    )
                    self.commitUnifiedLogFetch(
                        scoped.mergedText,
                        pods: scoped.podNames,
                        contextName: context.name,
                        namespace: namespace,
                        kind: .deployment,
                        resourceName: deployment.name
                    )
                    self.scheduleNextTailLogsReload()
                case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                    return
                }
            } catch {
                if error is CancellationError {
                    return
                }
                guard self.isCurrentLogsReloadRequest(requestID) else { return }
                if Self.isLikelyLogFetchFailure(error) {
                    self.state.setLastLogFetchError(Self.logFetchFailureMessage(for: error))
                } else {
                    self.state.setLastLogFetchError(error.localizedDescription)
                }
                self.state.clearError()
            }
        }
    }

    public func saveCurrentLogs() {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = currentLogsExportPayload(timestamp: timestamp) else { return }
            _ = try exporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func saveCurrentLogsToExportFolder(openAfterSave: Bool) {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = currentLogsExportPayload(timestamp: timestamp) else { return }
            _ = try configuredExporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes,
                kind: .plainText,
                openAfterSave: openAfterSave
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    private struct LogExportPayload {
        let data: Data
        let suggestedName: String
        let allowedFileTypes: [String]
    }

    private func currentLogsExportPayload(timestamp: String) -> LogExportPayload? {
        switch state.selectedWorkloadKind {
        case .pod:
            guard let pod = state.selectedPod else { return nil }
            return LogExportPayload(
                data: Data(state.podLogs.utf8),
                suggestedName: "pod-\(pod.name)-logs-\(timestamp).log",
                allowedFileTypes: ["log", "txt"]
            )
        case .service:
            guard let service = state.selectedService else { return nil }
            return LogExportPayload(
                data: Data(state.unifiedServiceLogs.utf8),
                suggestedName: "service-\(service.name)-unified-logs-\(timestamp).log",
                allowedFileTypes: ["log", "txt"]
            )
        case .deployment:
            guard let deployment = state.selectedDeployment else { return nil }
            return LogExportPayload(
                data: Data(state.unifiedServiceLogs.utf8),
                suggestedName: "deployment-\(deployment.name)-unified-logs-\(timestamp).log",
                allowedFileTypes: ["log", "txt"]
            )
        case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            return nil
        }
    }

    public func saveActiveTerminalTranscript() {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = activeTerminalTranscriptExportPayload(timestamp: timestamp) else { return }
            _ = try exporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func saveActiveTerminalTranscriptToExportFolder(openAfterSave: Bool) {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = activeTerminalTranscriptExportPayload(timestamp: timestamp) else { return }
            _ = try configuredExporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes,
                kind: .plainText,
                openAfterSave: openAfterSave
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func saveAllTerminalTranscriptsZip() {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = try allTerminalTranscriptsZipExportPayload(timestamp: timestamp) else { return }
            _ = try exporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func saveAllTerminalTranscriptsZipToExportFolder(openAfterSave: Bool) {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = try allTerminalTranscriptsZipExportPayload(timestamp: timestamp) else { return }
            _ = try configuredExporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes,
                kind: .archive,
                openAfterSave: openAfterSave
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    private func activeTerminalTranscriptExportPayload(timestamp: String) -> LogExportPayload? {
        guard let session = state.terminalSession,
              !session.transcript.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        else { return nil }

        return LogExportPayload(
            data: Self.terminalTranscriptData(session: session, generatedAt: timestamp),
            suggestedName: "terminal-\(Self.filenameComponent(session.namespace))-\(Self.filenameComponent(session.podName))-transcript-\(timestamp).log",
            allowedFileTypes: ["log", "txt"]
        )
    }

    private func allTerminalTranscriptsZipExportPayload(timestamp: String) throws -> LogExportPayload? {
        let sessions = state.terminalSessions.filter {
            !$0.transcript.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        }
        guard !sessions.isEmpty else { return nil }

        return LogExportPayload(
            data: try Self.terminalTranscriptArchiveData(sessions: sessions, generatedAt: timestamp),
            suggestedName: "terminal-transcripts-\(timestamp).zip",
            allowedFileTypes: ["zip"]
        )
    }

    nonisolated public static func terminalTranscriptArchiveData(
        sessions: [PodTerminalSession],
        generatedAt: String
    ) throws -> Data {
        let nonEmptySessions = sessions.filter {
            !$0.transcript.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        }
        var entries: [ZipArchiveEntry] = [
            ZipArchiveEntry(
                path: "terminal-transcripts/README.txt",
                data: Data("Rune terminal transcript export\nGenerated: \(generatedAt)\nSessions: \(nonEmptySessions.count)\n".utf8)
            )
        ]

        for (index, session) in nonEmptySessions.enumerated() {
            let path = [
                "terminal-transcripts",
                "session-\(index + 1)-\(filenameComponent(session.namespace))-\(filenameComponent(session.podName))-\(generatedAt).log"
            ].joined(separator: "/")
            entries.append(
                ZipArchiveEntry(
                    path: path,
                    data: terminalTranscriptData(session: session, generatedAt: generatedAt)
                )
            )
        }

        return try ZipArchiveBuilder.build(entries: entries)
    }

    nonisolated private static func terminalTranscriptData(
        session: PodTerminalSession,
        generatedAt: String
    ) -> Data {
        var lines = [
            "Rune terminal transcript",
            "Generated: \(generatedAt)",
            "Context: \(session.contextName)",
            "Namespace: \(session.namespace)",
            "Pod: \(session.podName)",
            "Shell: \(session.shell)",
            "Status: \(session.status.rawValue)"
        ]
        if let lastExitCode = session.lastExitCode {
            lines.append("Exit Code: \(lastExitCode)")
        }
        lines.append("")
        lines.append("Transcript:")
        lines.append(session.transcript)
        return Data(lines.joined(separator: "\n").utf8)
    }

    nonisolated private static func filenameComponent(_ value: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_."))
        let sanitizedScalars = value.unicodeScalars.map { scalar in
            allowed.contains(scalar) ? Character(scalar) : "-"
        }
        let sanitized = String(sanitizedScalars)
            .trimmingCharacters(in: CharacterSet(charactersIn: "-_."))
        return sanitized.isEmpty ? "item" : sanitized
    }

    public func saveCurrentLogsZip() {
        saveCurrentLogsZip(limitToSelectedPods: true)
    }

    public func saveCurrentLogsZipToExportFolder(openAfterSave: Bool) {
        saveCurrentLogsZip(limitToSelectedPods: true, configuredOpenAfterSave: openAfterSave)
    }

    public func saveAllPodsLogsZip() {
        saveCurrentLogsZip(limitToSelectedPods: false)
    }

    public func saveAllPodsLogsZipToExportFolder(openAfterSave: Bool) {
        saveCurrentLogsZip(limitToSelectedPods: false, configuredOpenAfterSave: openAfterSave)
    }

    public func saveVisibleLogsZip(visibleText: String) {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = try visibleLogsZipExportPayload(visibleText: visibleText, timestamp: timestamp) else { return }
            _ = try exporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func saveVisibleLogsZipToExportFolder(visibleText: String, openAfterSave: Bool) {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = try visibleLogsZipExportPayload(visibleText: visibleText, timestamp: timestamp) else { return }
            _ = try configuredExporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes,
                kind: .archive,
                openAfterSave: openAfterSave
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    private func visibleLogsZipExportPayload(visibleText: String, timestamp: String) throws -> LogExportPayload? {
        switch state.selectedWorkloadKind {
        case .pod:
            guard let pod = state.selectedPod else { return nil }
            let data = try LogArchiveBuilder.buildZip(
                mergedText: visibleText,
                podNames: [pod.name],
                baseName: "pod-\(pod.name)-visible-logs",
                generatedAt: timestamp,
                metadata: logArchiveMetadata(
                    context: state.selectedContext,
                    namespace: state.selectedNamespace,
                    workloadKind: "pod",
                    workloadName: pod.name,
                    selectedPods: [pod.name],
                    scope: "visible",
                    generatedAt: timestamp,
                    previous: includePreviousLogs
                )
            )
            return LogExportPayload(
                data: data,
                suggestedName: "pod-\(pod.name)-visible-logs-\(timestamp).zip",
                allowedFileTypes: ["zip"]
            )
        case .service:
            guard let service = state.selectedService else { return nil }
            let data = try LogArchiveBuilder.buildZip(
                mergedText: visibleText,
                podNames: state.unifiedServiceLogPods,
                baseName: "service-\(service.name)-visible-logs",
                generatedAt: timestamp,
                metadata: logArchiveMetadata(
                    context: state.selectedContext,
                    namespace: state.selectedNamespace,
                    workloadKind: "service",
                    workloadName: service.name,
                    selectedPods: state.unifiedServiceLogPods,
                    scope: "visible",
                    generatedAt: timestamp,
                    previous: includePreviousLogs
                )
            )
            return LogExportPayload(
                data: data,
                suggestedName: "service-\(service.name)-visible-logs-\(timestamp).zip",
                allowedFileTypes: ["zip"]
            )
        case .deployment:
            guard let deployment = state.selectedDeployment else { return nil }
            let data = try LogArchiveBuilder.buildZip(
                mergedText: visibleText,
                podNames: state.unifiedServiceLogPods,
                baseName: "deployment-\(deployment.name)-visible-logs",
                generatedAt: timestamp,
                metadata: logArchiveMetadata(
                    context: state.selectedContext,
                    namespace: state.selectedNamespace,
                    workloadKind: "deployment",
                    workloadName: deployment.name,
                    selectedPods: state.unifiedServiceLogPods,
                    scope: "visible",
                    generatedAt: timestamp,
                    previous: includePreviousLogs
                )
            )
            return LogExportPayload(
                data: data,
                suggestedName: "deployment-\(deployment.name)-visible-logs-\(timestamp).zip",
                allowedFileTypes: ["zip"]
            )
        case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            return nil
        }
    }

    private func saveArchivePayload(_ payload: LogExportPayload, configuredOpenAfterSave: Bool?) throws {
        if let configuredOpenAfterSave {
            _ = try configuredExporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes,
                kind: .archive,
                openAfterSave: configuredOpenAfterSave
            )
        } else {
            _ = try exporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes
            )
        }
    }

    private func saveCurrentLogsZip(limitToSelectedPods: Bool, configuredOpenAfterSave: Bool? = nil) {
        guard let context = state.selectedContext else { return }
        let sources = state.kubeConfigSources
        let namespace = state.selectedNamespace
        let kind = state.selectedWorkloadKind
        let previous = includePreviousLogs
        let pod = state.selectedPod
        let service = state.selectedService
        let deployment = state.selectedDeployment

        state.isLoadingLogs = true
        Task { [weak self] in
            guard let self else { return }
            defer { self.state.isLoadingLogs = false }
            do {
                let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
                switch kind {
                case .pod:
                    guard let pod else { return }
                    let data = try await self.fullPodLogsZipData(
                        pods: [pod],
                        sources: sources,
                        context: context,
                        namespace: namespace,
                        baseName: "pod-\(pod.name)-full-logs",
                        generatedAt: timestamp,
                        previous: previous,
                        metadata: self.logArchiveMetadata(
                            context: context,
                            namespace: namespace,
                            workloadKind: "pod",
                            workloadName: pod.name,
                            selectedPods: [pod.name],
                            scope: "full",
                            generatedAt: timestamp,
                            previous: previous
                        )
                    )
                    try self.saveArchivePayload(
                        LogExportPayload(
                            data: data,
                            suggestedName: "pod-\(pod.name)-full-logs-\(timestamp).zip",
                            allowedFileTypes: ["zip"]
                        ),
                        configuredOpenAfterSave: configuredOpenAfterSave
                    )
                case .service:
                    guard let service else { return }
                    let unified = try await self.kubeClient.unifiedLogsForService(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        service: service,
                        filter: .all,
                        previous: previous
                    )
                    let scoped = limitToSelectedPods
                        ? self.scopedUnifiedLogResult(mergedText: unified.mergedText, podNames: unified.podNames)
                        : (mergedText: unified.mergedText, podNames: unified.podNames)
                    let data = try LogArchiveBuilder.buildZip(
                        mergedText: scoped.mergedText,
                        podNames: scoped.podNames,
                        baseName: "service-\(service.name)-full-logs",
                        generatedAt: timestamp,
                        metadata: self.logArchiveMetadata(
                            context: context,
                            namespace: namespace,
                            workloadKind: "service",
                            workloadName: service.name,
                            selectedPods: scoped.podNames,
                            scope: limitToSelectedPods ? "selected" : "full",
                            generatedAt: timestamp,
                            previous: previous
                        )
                    )
                    try self.saveArchivePayload(
                        LogExportPayload(
                            data: data,
                            suggestedName: "service-\(service.name)-full-logs-\(timestamp).zip",
                            allowedFileTypes: ["zip"]
                        ),
                        configuredOpenAfterSave: configuredOpenAfterSave
                    )
                case .deployment:
                    guard let deployment else { return }
                    let unified = try await self.kubeClient.unifiedLogsForDeployment(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        deployment: deployment,
                        filter: .all,
                        previous: previous
                    )
                    let scoped = limitToSelectedPods
                        ? self.scopedUnifiedLogResult(mergedText: unified.mergedText, podNames: unified.podNames)
                        : (mergedText: unified.mergedText, podNames: unified.podNames)
                    let data = try LogArchiveBuilder.buildZip(
                        mergedText: scoped.mergedText,
                        podNames: scoped.podNames,
                        baseName: "deployment-\(deployment.name)-full-logs",
                        generatedAt: timestamp,
                        metadata: self.logArchiveMetadata(
                            context: context,
                            namespace: namespace,
                            workloadKind: "deployment",
                            workloadName: deployment.name,
                            selectedPods: scoped.podNames,
                            scope: limitToSelectedPods ? "selected" : "full",
                            generatedAt: timestamp,
                            previous: previous
                        )
                    )
                    try self.saveArchivePayload(
                        LogExportPayload(
                            data: data,
                            suggestedName: "deployment-\(deployment.name)-full-logs-\(timestamp).zip",
                            allowedFileTypes: ["zip"]
                        ),
                        configuredOpenAfterSave: configuredOpenAfterSave
                    )
                case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                    return
                }
            } catch {
                self.setExportErrorUnlessCancelled(error)
            }
        }
    }

    public func saveSelectedPodLogsZip() {
        saveSelectedPodLogsZip(configuredOpenAfterSave: nil)
    }

    public func saveSelectedPodLogsZipToExportFolder(openAfterSave: Bool) {
        saveSelectedPodLogsZip(configuredOpenAfterSave: openAfterSave)
    }

    private func saveSelectedPodLogsZip(configuredOpenAfterSave: Bool?) {
        let pods = selectedPodsForBulkActions
        guard !pods.isEmpty, let context = state.selectedContext else { return }
        let sources = state.kubeConfigSources
        let namespace = state.selectedNamespace
        let previous = includePreviousLogs

        state.isLoadingLogs = true
        Task { [weak self] in
            guard let self else { return }
            defer { self.state.isLoadingLogs = false }
            do {
                let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
                let data = try await self.fullPodLogsZipData(
                    pods: pods,
                    sources: sources,
                    context: context,
                    namespace: namespace,
                    baseName: "selected-pod-full-logs",
                    generatedAt: timestamp,
                    previous: previous,
                    metadata: self.logArchiveMetadata(
                        context: context,
                        namespace: namespace,
                        workloadKind: "pod",
                        workloadName: "selected-pods",
                        selectedPods: pods.map(\.name),
                        scope: "selected",
                        generatedAt: timestamp,
                        previous: previous
                    )
                )
                try self.saveArchivePayload(
                    LogExportPayload(
                        data: data,
                        suggestedName: "selected-pod-full-logs-\(timestamp).zip",
                        allowedFileTypes: ["zip"]
                    ),
                    configuredOpenAfterSave: configuredOpenAfterSave
                )
            } catch {
                self.setExportErrorUnlessCancelled(error)
            }
        }
    }

    func fullPodLogsZipData(
        pods: [PodSummary],
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        baseName: String,
        generatedAt: String,
        previous: Bool,
        metadata: LogArchiveMetadata? = nil
    ) async throws -> Data {
        var records: [PodLogArchiveRecord] = []
        var failures: [(podName: String, containerName: String?, error: Error)] = []
        var successfulFetches = 0

        for pod in pods {
            let containers: [String?] = pod.logContainerNames.isEmpty ? [nil] : pod.logContainerNames.map { Optional($0) }
            for container in containers {
                do {
                    let logs = try await kubeClient.podLogs(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        podName: pod.name,
                        container: container,
                        filter: .all,
                        previous: previous
                    )
                    successfulFetches += 1
                    records.append(
                        PodLogArchiveRecord(
                            podName: pod.name,
                            containerName: container,
                            logs: logs
                        )
                    )
                } catch {
                    if error is CancellationError {
                        throw error
                    }
                    failures.append((pod.name, container, error))
                }
            }
        }

        if successfulFetches == 0, let firstFailure = failures.first {
            throw firstFailure.error
        }
        records.append(contentsOf: failures.map { failure in
            let containerName = failure.containerName ?? "default"
            return PodLogArchiveRecord(
                podName: failure.podName,
                containerName: failure.containerName,
                logs: "⚠ Logs unavailable for container \(containerName): \(failure.error.localizedDescription)"
            )
        })

        return try LogArchiveBuilder.buildPodContainerZip(
            records: records,
            baseName: baseName,
            generatedAt: generatedAt,
            metadata: metadata
        )
    }

    private func logArchiveMetadata(
        context: KubeContext?,
        namespace: String,
        workloadKind: String,
        workloadName: String,
        selectedPods: [String],
        scope: String,
        generatedAt: String,
        previous: Bool
    ) -> LogArchiveMetadata? {
        guard let context else { return nil }
        return LogArchiveMetadata(
            context: context.name,
            namespace: namespace,
            workloadKind: workloadKind,
            workloadName: workloadName,
            selectedPods: selectedPods,
            timeWindow: selectedLogPreset.id,
            previous: previous,
            tail: isLogTailModeEnabled,
            exportedAt: generatedAt,
            scope: scope
        )
    }

    private func scopedUnifiedLogResult(
        mergedText: String,
        podNames: [String]
    ) -> (mergedText: String, podNames: [String]) {
        Self.scopedUnifiedLogResult(
            mergedText: mergedText,
            podNames: podNames,
            selectedPodNames: Set(selectedPodsForBulkActions.map(\.name))
        )
    }

    public static func scopedUnifiedLogResult(
        mergedText: String,
        podNames: [String],
        selectedPodNames: Set<String>
    ) -> (mergedText: String, podNames: [String]) {
        let selectedNames = selectedPodNames
        guard !selectedNames.isEmpty else {
            return (mergedText, podNames)
        }

        let scopedPodNames = podNames.filter { selectedNames.contains($0) }.sorted()
        guard !scopedPodNames.isEmpty else {
            return (mergedText, podNames)
        }

        let scopedNames = Set(scopedPodNames)
        var output = String()
        output.reserveCapacity(min(mergedText.utf8.count, 1_048_576))
        var wroteLine = false

        for line in mergedText.split(separator: "\n", omittingEmptySubsequences: false) {
            guard line.first == "[",
                  let close = line.firstIndex(of: "]"),
                  scopedNames.contains(String(line[line.index(after: line.startIndex)..<close]))
            else { continue }

            if wroteLine {
                output.append("\n")
            }
            output.append(contentsOf: line)
            wroteLine = true
        }

        return (output, scopedPodNames)
    }

    public func saveSelectedPodYAMLZip() {
        let pods = selectedPodsForBulkActions
        guard !pods.isEmpty, let context = state.selectedContext else { return }
        let sources = state.kubeConfigSources
        let namespace = state.selectedNamespace

        state.isLoadingResourceDetails = true
        Task { [weak self] in
            guard let self else { return }
            defer { self.state.isLoadingResourceDetails = false }
            do {
                let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
                var entries: [ZipArchiveEntry] = []

                for pod in pods {
                    let yaml = try await self.kubeClient.resourceYAML(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        kind: .pod,
                        name: pod.name
                    )
                    entries.append(ZipArchiveEntry(
                        path: "selected-pod-yaml/pods/\(pod.name)-\(timestamp).yaml",
                        data: Data(yaml.utf8)
                    ))
                }

                let data = try ZipArchiveBuilder.build(entries: entries)
                _ = try self.exporter.save(
                    data: data,
                    suggestedName: "selected-pod-yaml-\(timestamp).zip",
                    allowedFileTypes: ["zip"]
                )
            } catch {
                self.setExportErrorUnlessCancelled(error)
            }
        }
    }

    public func saveDeploymentPodYAMLZip() {
        guard let deployment = state.selectedDeployment, let context = state.selectedContext else { return }
        let sources = state.kubeConfigSources
        let namespace = state.selectedNamespace

        state.isLoadingResourceDetails = true
        Task { [weak self] in
            guard let self else { return }
            defer { self.state.isLoadingResourceDetails = false }
            do {
                let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
                let pods = try await self.kubeClient.podsForDeployment(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    deployment: deployment
                )
                var entries: [ZipArchiveEntry] = []

                for pod in pods.sorted(by: { $0.name < $1.name }) {
                    let yaml = try await self.kubeClient.resourceYAML(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        kind: .pod,
                        name: pod.name
                    )
                    entries.append(
                        ZipArchiveEntry(
                            path: "deployment-\(deployment.name)-pod-yaml/pods/\(pod.name)-\(timestamp).yaml",
                            data: Data(yaml.utf8)
                        )
                    )
                }

                if entries.isEmpty {
                    entries.append(
                        ZipArchiveEntry(
                            path: "deployment-\(deployment.name)-pod-yaml/README.txt",
                            data: Data("No pods matched deployment \(deployment.name) in namespace \(namespace).\n".utf8)
                        )
                    )
                }

                let data = try ZipArchiveBuilder.build(entries: entries)
                _ = try self.exporter.save(
                    data: data,
                    suggestedName: "deployment-\(deployment.name)-pod-yaml-\(timestamp).zip",
                    allowedFileTypes: ["zip"]
                )
            } catch {
                self.setExportErrorUnlessCancelled(error)
            }
        }
    }

    public func saveDeploymentPodLogsZip() {
        saveDeploymentPodLogsZip(configuredOpenAfterSave: nil)
    }

    public func saveDeploymentPodLogsZipToExportFolder(openAfterSave: Bool) {
        saveDeploymentPodLogsZip(configuredOpenAfterSave: openAfterSave)
    }

    private func saveDeploymentPodLogsZip(configuredOpenAfterSave: Bool?) {
        guard let deployment = state.selectedDeployment, let context = state.selectedContext else { return }
        let sources = state.kubeConfigSources
        let namespace = state.selectedNamespace
        let previous = includePreviousLogs

        state.isLoadingLogs = true
        Task { [weak self] in
            guard let self else { return }
            defer { self.state.isLoadingLogs = false }
            do {
                let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
                let pods = try await self.kubeClient.podsForDeployment(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    deployment: deployment
                )
                let baseName = "deployment-\(deployment.name)-pod-logs"
                let data: Data

                if pods.isEmpty {
                    data = try ZipArchiveBuilder.build(entries: [
                        ZipArchiveEntry(
                            path: "\(baseName)/README.txt",
                            data: Data("No pods matched deployment \(deployment.name) in namespace \(namespace).\n".utf8)
                        )
                    ])
                } else {
                    data = try await self.fullPodLogsZipData(
                        pods: pods.sorted(by: { $0.name < $1.name }),
                        sources: sources,
                        context: context,
                        namespace: namespace,
                        baseName: baseName,
                        generatedAt: timestamp,
                        previous: previous,
                        metadata: self.logArchiveMetadata(
                            context: context,
                            namespace: namespace,
                            workloadKind: "deployment",
                            workloadName: deployment.name,
                            selectedPods: pods.map(\.name),
                            scope: "deployment-pods",
                            generatedAt: timestamp,
                            previous: previous
                        )
                    )
                }

                try self.saveArchivePayload(
                    LogExportPayload(
                        data: data,
                        suggestedName: "\(baseName)-\(timestamp).zip",
                        allowedFileTypes: ["zip"]
                    ),
                    configuredOpenAfterSave: configuredOpenAfterSave
                )
            } catch {
                self.setExportErrorUnlessCancelled(error)
            }
        }
    }

    public func copyCurrentLogsToClipboard() {
        let logs: String
        switch state.selectedWorkloadKind {
        case .pod:
            logs = state.podLogs
        case .service, .deployment:
            logs = state.unifiedServiceLogs
        case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            return
        }
        guard !logs.isEmpty else { return }
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(logs, forType: .string)
    }

    public func saveVisibleEvents() {
        do {
            guard !visibleEvents.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")

            let lines = visibleEvents.map { event in
                "[\(event.type)] \(event.reason) • \(event.objectName)\n\(event.message)"
            }
            let payload = lines.joined(separator: "\n\n")

            _ = try exporter.save(
                data: Data(payload.utf8),
                suggestedName: "events-\(state.selectedNamespace)-\(timestamp).txt",
                allowedFileTypes: ["txt", "log"]
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentHelmValues() {
        do {
            guard let release = state.selectedHelmRelease, !state.helmValues.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            _ = try exporter.save(
                data: Data(state.helmValues.utf8),
                suggestedName: "helm-\(release.name)-values-\(timestamp).yaml",
                allowedFileTypes: ["yaml", "yml"]
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentHelmManifest() {
        do {
            guard let release = state.selectedHelmRelease, !state.helmManifest.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            _ = try exporter.save(
                data: Data(state.helmManifest.utf8),
                suggestedName: "helm-\(release.name)-manifest-\(timestamp).yaml",
                allowedFileTypes: ["yaml", "yml"]
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentHelmHistory() {
        do {
            guard let release = state.selectedHelmRelease, !state.helmHistory.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            let payload = state.helmHistory.map { entry in
                "Revision \(entry.revision)\nStatus: \(entry.status)\nUpdated: \(entry.updated)\nChart: \(entry.chart)\nApp Version: \(entry.appVersion)\n\(entry.description)"
            }.joined(separator: "\n\n")
            _ = try exporter.save(
                data: Data(payload.utf8),
                suggestedName: "helm-\(release.name)-history-\(timestamp).txt",
                allowedFileTypes: ["txt", "log"]
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentResourceYAML() {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = currentResourceYAMLExportPayload(timestamp: timestamp) else { return }
            _ = try exporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentResourceYAMLToExportFolder(openAfterSave: Bool) {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = currentResourceYAMLExportPayload(timestamp: timestamp) else { return }
            _ = try configuredExporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes,
                kind: .plainText,
                openAfterSave: openAfterSave
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentResourceDescribe() {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = currentResourceDescribeExportPayload(timestamp: timestamp) else { return }
            _ = try exporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentResourceDescribeToExportFolder(openAfterSave: Bool) {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            guard let payload = currentResourceDescribeExportPayload(timestamp: timestamp) else { return }
            _ = try configuredExporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes,
                kind: .plainText,
                openAfterSave: openAfterSave
            )
        } catch {
            state.setError(error)
        }
    }

    private func currentResourceYAMLExportPayload(timestamp: String) -> LogExportPayload? {
        guard let identity = currentResourceDocumentExportIdentity(), !state.resourceYAML.isEmpty else { return nil }
        guard loadedResourceDetailScopeMatchesCurrentSelection() else { return nil }
        return LogExportPayload(
            data: Data(state.resourceYAML.utf8),
            suggestedName: "\(identity.kind)-\(identity.name)-\(timestamp).yaml",
            allowedFileTypes: ["yaml", "yml"]
        )
    }

    private func currentResourceDescribeExportPayload(timestamp: String) -> LogExportPayload? {
        guard let identity = currentResourceDocumentExportIdentity(), !state.resourceDescribe.isEmpty else { return nil }
        guard loadedResourceDetailScopeMatchesCurrentSelection() else { return nil }
        return LogExportPayload(
            data: Data(state.resourceDescribe.utf8),
            suggestedName: "\(identity.kind)-\(identity.name)-describe-\(timestamp).txt",
            allowedFileTypes: ["txt", "log"]
        )
    }

    private func currentResourceDocumentExportIdentity() -> (kind: String, name: String)? {
        if state.selectedSection == .helm, let resource = state.selectedOperatorResource {
            return (
                Self.filenameComponent(resource.kind.lowercased()),
                Self.filenameComponent(resource.name)
            )
        }
        guard let (kind, name) = currentWritableResource() else { return nil }
        return (kind.kubernetesResourceName, name)
    }

    /// Discards edits in the YAML editor and restores the last manifest loaded from the cluster.
    public func revertResourceYAMLDraft() {
        yamlValidationTask?.cancel()
        state.revertResourceYAMLToClusterSnapshot()
    }

    public func undoResourceYAMLEdit() {
        yamlValidationTask?.cancel()
        state.undoResourceYAMLEdit()
    }

    /// Replaces the editor contents with a YAML file from disk (UTF-8).
    public func importResourceYAMLFromFile() {
        let panel = NSOpenPanel()
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        panel.allowedContentTypes = ["yaml", "yml"].compactMap { UTType(filenameExtension: $0) }
        panel.prompt = "Import"

        guard panel.runModal() == .OK, let url = panel.url else { return }

        do {
            let text = try String(contentsOf: url, encoding: .utf8)
            state.updateResourceYAMLDraft(text)
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentRolloutHistory() {
        do {
            guard let deployment = state.selectedDeployment, !state.deploymentRolloutHistory.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")

            _ = try exporter.save(
                data: Data(state.deploymentRolloutHistory.utf8),
                suggestedName: "deployment-\(deployment.name)-rollout-history-\(timestamp).txt",
                allowedFileTypes: ["txt", "log"]
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveSupportBundle() {
        Task { @MainActor in
            do {
                let payload = try await supportBundleExportPayload()
                _ = try exporter.save(
                    data: payload.data,
                    suggestedName: payload.suggestedName,
                    allowedFileTypes: payload.allowedFileTypes
                )
            } catch {
                setExportErrorUnlessCancelled(error)
            }
        }
    }

    public func saveSupportBundleToExportFolder(openAfterSave: Bool) {
        Task { @MainActor in
            do {
                let payload = try await supportBundleExportPayload()
                _ = try configuredExporter.save(
                    data: payload.data,
                    suggestedName: payload.suggestedName,
                    allowedFileTypes: payload.allowedFileTypes,
                    kind: .plainText,
                    openAfterSave: openAfterSave
                )
            } catch {
                setExportErrorUnlessCancelled(error)
            }
        }
    }

    private func supportBundleExportPayload() async throws -> LogExportPayload {
        let metricsReport = await stableSelectedContextRequestMetricsReport()
        let restRequestMetrics = metricsReport.metrics
        let requestMetrics = KubernetesRequestMetricsSupportBundleProjector.metrics(from: restRequestMetrics)
        let requestMetricGroups = metricsReport.endpointGroups.isEmpty
            ? KubernetesRequestMetricsSupportBundleProjector.groups(from: restRequestMetrics)
            : KubernetesRequestMetricsSupportBundleProjector.groups(from: metricsReport.endpointGroups)
        let formatter = ISO8601DateFormatter()
        let generatedAt = formatter.string(from: Date())
        let exportStamp = generatedAt.replacingOccurrences(of: ":", with: "")
        let bundle = try supportBundleBuilder.buildBundle(
            from: SupportBundleRequest.snapshot(
                state: state,
                generatedAt: generatedAt,
                resourceCounts: resourceCounts(),
                selectedResourceKind: selectedResourceKindLabel(),
                selectedResourceName: selectedResourceName(),
                requestMetrics: requestMetrics,
                requestMetricsSummary: KubernetesRequestMetricsSupportBundleProjector.summary(from: metricsReport.summary),
                requestMetricGroups: requestMetricGroups,
                cloudImportDiagnostic: cloudKubeConfigImportDiagnostic.map {
                    SupportBundleCloudImportDiagnostic(
                        title: $0.title,
                        classification: $0.classification,
                        message: $0.message,
                        operationShape: $0.operationShape,
                        nextAction: $0.nextAction
                    )
                }
            )
        )
        return LogExportPayload(
            data: bundle,
            suggestedName: "support-bundle-\(exportStamp).json",
            allowedFileTypes: ["json"]
        )
    }

    private func stableSelectedContextRequestMetricsReport() async -> KubernetesRESTRequestMetricsReport {
        while true {
            guard let selection = selectedKubernetesRequestMetricsSelection() else {
                return .empty
            }
            guard !selection.sources.isEmpty else { return .empty }
            let report = await kubeClient.restRequestMetricsReport(
                from: selection.sources,
                context: selection.context
            )
            guard selectedKubernetesRequestMetricsSelection() == selection else { continue }
            return report
        }
    }

    private func selectedKubernetesRequestMetricsSelection() -> KubernetesRequestMetricsSelection? {
        guard let context = state.selectedContext else { return nil }
        return KubernetesRequestMetricsSelection(
            sources: state.kubeConfigSources,
            context: context
        )
    }

    public func runAuthDoctor() {
        guard !state.isRunningAuthDoctor else { return }
        let runID = UUID()
        let scope = AuthDoctorScope(
            kubeConfigSources: state.kubeConfigSources,
            selectedContext: state.selectedContext,
            selectedNamespace: state.selectedNamespace
        )
        activeAuthDoctorRunID = runID
        state.clearError()
        state.setAuthDoctorRunning(true)
        state.setAuthDoctorChecks([
            RuneHealthCheck(id: "start", title: "Auth Doctor", status: .running, message: "Checking kubeconfig, context, auth transport, namespace access, logs, exec, and port-forward permissions.")
        ])

        authDoctorTask = Task { @MainActor [weak self] in
            guard let self else { return }
            var checks: [RuneHealthCheck] = []

            @MainActor
            func isActiveRun() -> Bool {
                self.activeAuthDoctorRunID == runID && !Task.isCancelled
            }

            @MainActor
            func record(_ id: String, _ title: String, _ status: RuneHealthCheckStatus, _ message: String) {
                guard isActiveRun() else { return }
                checks.removeAll { $0.id == id }
                checks.append(RuneHealthCheck(id: id, title: title, status: status, message: message))
                self.state.setAuthDoctorChecks(checks)
            }

            @MainActor
            func recordProjectedFailureMessage(_ message: String) {
                for check in AuthDoctorFailureProjector.checks(for: message) {
                    record(check.id, check.title, check.status, check.message)
                }
            }

            @MainActor
            func recordProjectedFailure(_ error: Error) {
                recordProjectedFailureMessage(error.localizedDescription)
            }

            @MainActor
            func recordExecAuthCacheDiagnostic(context: KubeContext) async {
                guard isActiveRun() else { return }
                do {
                    guard let diagnostic = try await self.kubeClient.execCredentialCacheDiagnostic(
                        from: scope.kubeConfigSources,
                        context: context
                    ) else { return }
                    guard isActiveRun() else { return }
                    let check = AuthDoctorExecAuthCacheProjector.check(for: diagnostic)
                    record(check.id, check.title, check.status, check.message)
                } catch {
                    guard isActiveRun() else { return }
                    recordProjectedFailure(error)
                }
            }

            defer {
                if self.activeAuthDoctorRunID == runID {
                    self.activeAuthDoctorRunID = nil
                    self.authDoctorTask = nil
                    self.state.setAuthDoctorRunning(false)
                    self.refreshKubernetesRequestMetricsSummary()
                }
            }

            guard isActiveRun() else { return }
            if scope.selectedContext?.name == self.demoContextName {
                record("demo", "Demo cluster", .passed, "The in-memory demo cluster is active. Auth Doctor skips real Kubernetes API calls in demo mode.")
                return
            }

            guard !scope.kubeConfigSources.isEmpty else {
                record("kubeconfig", "Kubeconfig", .failed, "No kubeconfig source is loaded. Import a kubeconfig or load the demo cluster.")
                return
            }
            record("kubeconfig", "Kubeconfig", .passed, "\(scope.kubeConfigSources.count) source(s) loaded.")
            for check in AuthDoctorKubeconfigInspector().inspect(
                sources: scope.kubeConfigSources,
                activeContextName: scope.selectedContext?.name
            ) {
                record(check.id, check.title, check.status, check.message)
            }
            let contexts: [KubeContext]
            do {
                contexts = try await self.kubeClient.listContexts(from: scope.kubeConfigSources)
                guard isActiveRun() else { return }
                record("contexts", "Contexts", contexts.isEmpty ? .failed : .passed, contexts.isEmpty ? "No contexts were found." : "\(contexts.count) context(s) are readable.")
            } catch {
                guard isActiveRun() else { return }
                recordProjectedFailure(error)
                record("contexts", "Contexts", .failed, error.localizedDescription)
                return
            }

            guard let context = scope.selectedContext ?? contexts.first else {
                record("selected-context", "Selected context", .failed, "No selected context.")
                return
            }
            record("selected-context", "Selected context", .passed, context.name)
            if let request = try? self.selectedNativeCredentialRequest(expectedProvider: nil) {
                do {
                    let status = try await self.nativeAuthConfigurator.status(for: request)
                    guard isActiveRun() else { return }
                    record(
                        "native-auth-profile",
                        "Native \(request.provider.displayName) authentication",
                        status.isConnected ? .passed : .warning,
                        status.isConnected
                            ? "A Keychain-backed native authentication profile is connected for the selected context."
                            : "The selected context needs a native \(request.provider.displayName) login in this App Store build."
                    )
                } catch {
                    guard isActiveRun() else { return }
                    record("native-auth-profile", "Native authentication", .warning, error.localizedDescription)
                }
            }
            let kubeConfigSources = scope.kubeConfigSources
            let authDoctorKubeClient = self.kubeClient

            @MainActor
            func recordCanI(
                _ id: String,
                _ title: String,
                namespace: String?,
                verb: String,
                resource: String,
                apiGroup: String? = nil,
                subresource: String? = nil
            ) async -> AuthDoctorRBACCapability? {
                guard isActiveRun() else { return nil }
                do {
                    let allowed = try await self.kubeClient.canI(
                        from: kubeConfigSources,
                        context: context,
                        namespace: namespace,
                        verb: verb,
                        resource: resource,
                        apiGroup: apiGroup,
                        subresource: subresource
                    )
                    guard isActiveRun() else { return nil }
                    let capability = AuthDoctorRBACCapability(
                        id: id,
                        title: title,
                        verb: verb,
                        resource: resource,
                        apiGroup: apiGroup,
                        subresource: subresource,
                        allowed: allowed
                    )
                    let check = AuthDoctorRBACProjector.check(for: capability, namespace: namespace)
                    record(check.id, check.title, check.status, check.message)
                    return capability
                } catch {
                    guard isActiveRun() else { return nil }
                    recordProjectedFailure(error)
                    record(id, title, .warning, "Could not verify RBAC with SelfSubjectAccessReview: \(error.localizedDescription)")
                    return nil
                }
            }

            do {
                let defaultNamespace = try await self.kubeClient.contextNamespace(
                    from: scope.kubeConfigSources,
                    context: context
                )
                guard isActiveRun() else { return }
                record("context-namespace", "Context namespace", .passed, defaultNamespace?.isEmpty == false ? defaultNamespace! : "No default namespace in kubeconfig; Rune will resolve one.")
            } catch {
                guard isActiveRun() else { return }
                recordProjectedFailure(error)
                record("context-namespace", "Context namespace", .warning, error.localizedDescription)
            }

            do {
                let namespaces = try await self.kubeClient.listNamespaces(
                    from: scope.kubeConfigSources,
                    context: context
                )
                guard isActiveRun() else { return }
                record("namespace-list", "Namespace list", .passed, "\(namespaces.count) namespace(s) listed.")
                record("transport", "API transport", .passed, "API server, TLS/CA, proxy settings, and auth credentials worked for a live request.")
                record("exec-auth", "Kubernetes authentication", .passed, "The selected context authenticated successfully through static, native, or exec credentials.")
                await recordExecAuthCacheDiagnostic(context: context)
                guard isActiveRun() else { return }
            } catch {
                guard isActiveRun() else { return }
                recordProjectedFailure(error)
                record("namespace-list", "Namespace list", .warning, "Cannot list namespaces. Manual namespace mode can still work if RBAC allows access to a specific namespace. \(error.localizedDescription)")
            }

            let namespace = scope.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !namespace.isEmpty else {
                record("namespace", "Active namespace", .failed, "No namespace selected. Enter a namespace manually or select one from the menu.")
                return
            }
            record("namespace", "Active namespace", .passed, namespace)

            let podRBACCapabilities = [
                await recordCanI("rbac-pods-list", "RBAC pod list", namespace: namespace, verb: "list", resource: "pods"),
                await recordCanI("rbac-pod-logs", "RBAC pod logs", namespace: namespace, verb: "get", resource: "pods", subresource: "log"),
                await recordCanI("rbac-pod-exec", "RBAC pod exec", namespace: namespace, verb: "create", resource: "pods", subresource: "exec"),
                await recordCanI("rbac-port-forward", "RBAC port-forward", namespace: namespace, verb: "create", resource: "pods", subresource: "portforward")
            ].compactMap { $0 }
            if let rbacSummary = AuthDoctorRBACProjector.accessSummary(namespace: namespace, capabilities: podRBACCapabilities) {
                record(rbacSummary.id, rbacSummary.title, rbacSummary.status, rbacSummary.message)
            }
            let preflightResults = await AuthDoctorRBACPreflightRunner.run(
                targets: AuthDoctorRBACPreflightTarget.emptyViewTargets,
                activeNamespace: namespace,
                maxConcurrentChecks: 4
            ) { target, targetNamespace in
                try await authDoctorKubeClient.canI(
                    from: kubeConfigSources,
                    context: context,
                    namespace: targetNamespace,
                    verb: target.verb,
                    resource: target.resource,
                    apiGroup: target.apiGroup,
                    subresource: target.subresource
                )
            }
            guard isActiveRun() else { return }
            for result in preflightResults {
                if let allowed = result.allowed {
                    let capability = AuthDoctorRBACCapability(
                        id: result.target.id,
                        title: result.target.title,
                        verb: result.target.verb,
                        resource: result.target.resource,
                        apiGroup: result.target.apiGroup,
                        subresource: result.target.subresource,
                        allowed: allowed
                    )
                    let check = AuthDoctorRBACProjector.check(for: capability, namespace: result.namespace)
                    record(check.id, check.title, check.status, check.message)
                } else if let errorMessage = result.errorMessage {
                    recordProjectedFailureMessage(errorMessage)
                    record(result.target.id, result.target.title, .warning, "Could not verify RBAC with SelfSubjectAccessReview: \(errorMessage)")
                }
            }

            do {
                let pods = try await self.kubeClient.listPods(
                    from: scope.kubeConfigSources,
                    context: context,
                    namespace: namespace
                )
                guard isActiveRun() else { return }
                record("pod-list", "Pod list", .passed, "\(pods.count) pod(s) readable in namespace \(namespace).")
                record("transport", "API transport", .passed, "API server, TLS/CA, proxy settings, and auth credentials worked for a live request.")
                record("exec-auth", "Exec auth", .passed, "Any kubeconfig exec/auth plugin needed for this request completed successfully.")
                await recordExecAuthCacheDiagnostic(context: context)
                guard isActiveRun() else { return }
                if let pod = Self.authDoctorLogProbePod(from: pods) {
                    do {
                        _ = try await self.kubeClient.podLogs(
                            from: scope.kubeConfigSources,
                            context: context,
                            namespace: namespace,
                            podName: pod.name,
                            container: pod.containerNames.first,
                            filter: .tailLines(20),
                            previous: false
                        )
                        guard isActiveRun() else { return }
                        record("pod-logs", "Pod logs", .passed, "Logs endpoint is reachable for a pod in \(namespace).")
                    } catch {
                        guard isActiveRun() else { return }
                        recordProjectedFailure(error)
                        record("pod-logs", "Pod logs", .warning, error.localizedDescription)
                    }
                } else {
                    record("pod-logs", "Pod logs", .warning, "No pods found, so log access could not be verified.")
                }
            } catch {
                guard isActiveRun() else { return }
                recordProjectedFailure(error)
                record("pod-list", "Pod list", .failed, error.localizedDescription)
            }
        }
    }

    private func invalidateAuthDoctorRunForScopeChange() {
        let hadActiveRun = activeAuthDoctorRunID != nil
        activeAuthDoctorRunID = nil
        authDoctorTask?.cancel()
        authDoctorTask = nil
        if hadActiveRun {
            state.setAuthDoctorRunning(false)
        }
        state.clearAuthDoctorChecks()
        refreshKubernetesRequestMetricsSummary()
    }

    nonisolated static func authDoctorLogProbePod(from pods: [PodSummary]) -> PodSummary? {
        func normalizedStatus(_ pod: PodSummary) -> String {
            pod.status.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        }

        func hasAllContainersReady(_ pod: PodSummary) -> Bool {
            guard let ready = pod.containersReady?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !ready.isEmpty else {
                return false
            }
            let parts = ready
                .split(separator: "/", maxSplits: 1)
                .compactMap { Int($0.trimmingCharacters(in: .whitespacesAndNewlines)) }
            return parts.count == 2 && parts[0] == parts[1] && parts[1] > 0
        }

        return pods.first { normalizedStatus($0) == "running" && hasAllContainersReady($0) }
            ?? pods.first { normalizedStatus($0) == "running" }
            ?? pods.first { ["succeeded", "completed"].contains(normalizedStatus($0)) }
            ?? pods.first
    }

    public func clearAuthDoctorOutput() {
        guard !state.isRunningAuthDoctor else { return }
        state.clearAuthDoctorChecks()
    }

    public func refreshKubernetesRequestMetricsSummary() {
        guard !isRefreshingKubernetesRequestMetricsSummary else {
            shouldRefreshKubernetesRequestMetricsSummaryAgain = true
            return
        }
        isRefreshingKubernetesRequestMetricsSummary = true

        Task { @MainActor [weak self] in
            guard let self else { return }
            repeat {
                self.shouldRefreshKubernetesRequestMetricsSummaryAgain = false
                let report = await self.stableSelectedContextRequestMetricsReport()
                self.kubernetesRequestMetricsSummary = KubernetesRequestMetricsDebugPresentation(report: report)
            } while self.shouldRefreshKubernetesRequestMetricsSummaryAgain
            self.isRefreshingKubernetesRequestMetricsSummary = false
        }
    }

    public func useSelectedRBACResourceForCanI() {
        guard let resource = state.selectedRBACResource else { return }
        rbacCanIVerb = "list"
        rbacCanIResource = KubernetesRESTPath.resourceName(for: resource.kind)
        rbacCanIApiGroup = resource.kind.rbacAPIGroup ?? ""
        rbacCanISubresource = ""
        rbacCanIScope = resource.kind.isNamespaced ? .namespace : .cluster
        rbacCanIResult = nil
    }

    public func useRBACCanIPreset(
        verb: String,
        resource: String,
        apiGroup: String?,
        subresource: String?,
        scope: RBACCanIScope
    ) {
        rbacCanIVerb = verb.trimmingCharacters(in: .whitespacesAndNewlines)
        rbacCanIResource = resource.trimmingCharacters(in: .whitespacesAndNewlines)
        rbacCanIApiGroup = apiGroup?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        rbacCanISubresource = subresource?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        rbacCanIScope = scope
        rbacCanIResult = nil
    }

    public func runRBACCanISimulator() {
        guard !isRunningRBACCanI else { return }
        guard let context = state.selectedContext else {
            rbacCanIResult = RBACCanIResult(
                request: currentRBACCanIRequest(),
                allowed: nil,
                errorMessage: "Select a Kubernetes context before checking RBAC."
            )
            return
        }

        let request = currentRBACCanIRequest()
        guard !request.verb.isEmpty else {
            rbacCanIResult = RBACCanIResult(request: request, allowed: nil, errorMessage: "Enter a verb to check.")
            return
        }
        guard !request.resource.isEmpty else {
            rbacCanIResult = RBACCanIResult(request: request, allowed: nil, errorMessage: "Enter a resource to check.")
            return
        }
        if request.scope == .namespace, request.reviewNamespace == nil {
            rbacCanIResult = RBACCanIResult(request: request, allowed: nil, errorMessage: "Select or enter a namespace, or switch scope to Cluster.")
            return
        }

        state.clearError()
        rbacCanIResult = nil
        isRunningRBACCanI = true

        Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.isRunningRBACCanI = false }

            do {
                let allowed = try await self.rbacCanICheck(
                    self.state.kubeConfigSources,
                    context,
                    request.reviewNamespace,
                    request.verb,
                    request.resource,
                    request.apiGroup,
                    request.subresource
                )
                self.rbacCanIResult = RBACCanIResult(request: request, allowed: allowed, errorMessage: nil)
            } catch {
                self.rbacCanIResult = RBACCanIResult(
                    request: request,
                    allowed: nil,
                    errorMessage: error.localizedDescription
                )
            }
        }
    }

    private func currentRBACCanIRequest() -> RBACCanIRequest {
        RBACCanIRequest(
            namespace: state.selectedNamespace,
            verb: rbacCanIVerb,
            resource: rbacCanIResource,
            apiGroup: rbacCanIApiGroup,
            subresource: rbacCanISubresource,
            scope: rbacCanIScope
        )
    }

    public func loadDemoCluster() {
        guard UserDefaults.standard.runeEnableDemoCluster else {
            state.setError(RuneError.invalidInput(message: "Demo cluster is disabled in Settings."))
            return
        }

        if let previousContextName = state.selectedContext?.name,
           previousContextName != demoContextName {
            stopAndClearTerminalSessions(contextName: previousContextName)
        }

        clusterLoadGeneration = UUID()
        bootstrapTask?.cancel()
        bootstrapTask = nil
        stopKubeConfigSourceSync()
        scheduledRefreshTask?.cancel()
        pendingCurrentViewRefreshID = nil
        cancelPendingLogReload()
        resourceDetailsTask?.cancel()
        yamlValidationTask?.cancel()
        overviewPrefetchTask?.cancel()
        overviewPrefetchTask = nil
        contextOverviewPrefetchTask?.cancel()
        contextOverviewPrefetchTask = nil
        latestSnapshotRequestID = UUID()
        latestResourceDetailsRequestID = UUID()
        latestYAMLValidationRequestID = UUID()
        isLaunchExperienceVisible = false
        state.isLoading = false
        state.isLoadingLogs = false
        state.finishResourceDetailLoad()
        state.clearError()
        state.clearManualNamespaceMode()
        state.clearResourceDetails()
        clearResourceBulkSelections()

        let context = demoContext
        state.setContexts(contextsIncludingEnabledDemo(state.contexts))
        state.selectedContext = context
        state.selectedNamespace = "demo"
        state.selectedSection = .overview
        applyDemoClusterSnapshot()
        applyDemoResourceDetailsForCurrentSelection()
        state.setSnapshotFreshness(
            RuneSnapshotFreshness(
                status: .live,
                updatedAt: Date(),
                message: "In-memory demo cluster loaded. No Kubernetes API calls are made."
            )
        )
    }

    public func resetDemoCluster() {
        loadDemoCluster()
    }

    private func applyDemoClusterSnapshot() {
        let pods = [
            PodSummary(
                name: "api-6f78d9d7c9-2xkq8",
                namespace: "demo",
                status: "Running",
                totalRestarts: 0,
                ageDescription: "18m",
                cpuUsage: "42m",
                memoryUsage: "96Mi",
                podIP: "10.42.0.21",
                hostIP: "192.0.2.10",
                nodeName: "demo-node-a",
                qosClass: "Burstable",
                containersReady: "1/1",
                containerNamesLine: "api"
            ),
            PodSummary(
                name: "worker-7c9d8f6b5c-mk42q",
                namespace: "demo",
                status: "Running",
                totalRestarts: 1,
                ageDescription: "17m",
                cpuUsage: "21m",
                memoryUsage: "74Mi",
                podIP: "10.42.0.22",
                hostIP: "192.0.2.11",
                nodeName: "demo-node-b",
                qosClass: "Burstable",
                containersReady: "1/1",
                containerNamesLine: "worker"
            ),
            PodSummary(
                name: "checkout-5d79f6c8b9-vx4lp",
                namespace: "demo",
                status: "CrashLoopBackOff",
                totalRestarts: 7,
                ageDescription: "6m",
                cpuUsage: "8m",
                memoryUsage: "52Mi",
                podIP: "10.42.0.23",
                hostIP: "192.0.2.11",
                nodeName: "demo-node-b",
                qosClass: "Burstable",
                containersReady: "0/1",
                containerNamesLine: "checkout"
            )
        ]
        let deployments = [
            DeploymentSummary(name: "api", namespace: "demo", readyReplicas: 1, desiredReplicas: 1, selector: ["app": "api"]),
            DeploymentSummary(name: "worker", namespace: "demo", readyReplicas: 1, desiredReplicas: 1, selector: ["app": "worker"]),
            DeploymentSummary(name: "checkout", namespace: "demo", readyReplicas: 0, desiredReplicas: 2, selector: ["app": "checkout"])
        ]
        let statefulSets = [
            ClusterResourceSummary(kind: .statefulSet, name: "postgres", namespace: "demo", primaryText: "1/1 ready", secondaryText: "app=postgres")
        ]
        let daemonSets = [
            ClusterResourceSummary(kind: .daemonSet, name: "node-agent", namespace: "kube-system", primaryText: "2/2 ready", secondaryText: "app=node-agent")
        ]
        let jobs = [
            ClusterResourceSummary(kind: .job, name: "data-backfill", namespace: "demo", primaryText: "Complete (1)", secondaryText: "1/1 complete")
        ]
        let replicaSets = [
            ClusterResourceSummary(kind: .replicaSet, name: "api-6f78d9d7c9", namespace: "demo", primaryText: "1/1 ready", secondaryText: "app=api")
        ]
        let horizontalPodAutoscalers = [
            ClusterResourceSummary(kind: .horizontalPodAutoscaler, name: "api", namespace: "demo", primaryText: "1–5 replicas (current 3)", secondaryText: "Deployment/api")
        ]
        let services = [
            ServiceSummary(name: "api", namespace: "demo", type: "ClusterIP", clusterIP: "10.96.12.44", selector: ["app": "api"])
        ]
        let ingresses = [
            ClusterResourceSummary(kind: .ingress, name: "api", namespace: "demo", primaryText: "api.demo.invalid", secondaryText: "Service api:80")
        ]
        let networkPolicies = [
            ClusterResourceSummary(kind: .networkPolicy, name: "api-allow-web", namespace: "demo", primaryText: "Ingress", secondaryText: "app=api")
        ]
        let events = [
            EventSummary(type: "Normal", reason: "Started", objectName: "api-6f78d9d7c9-2xkq8", message: "Started container api", lastTimestamp: "2026-05-05T10:00:00Z", involvedKind: "Pod", involvedNamespace: "demo"),
            EventSummary(type: "Warning", reason: "BackOff", objectName: "checkout-5d79f6c8b9-vx4lp", message: "Back-off restarting failed demo container", lastTimestamp: "2026-05-05T10:05:00Z", involvedKind: "Pod", involvedNamespace: "demo")
        ]
        let configMaps = [
            ClusterResourceSummary(kind: .configMap, name: "api-settings", namespace: "demo", primaryText: "3 keys", secondaryText: "3 text values · 0 binary values")
        ]
        let secrets = [
            ClusterResourceSummary(kind: .secret, name: "api-token", namespace: "demo", primaryText: "Opaque", secondaryText: "2 values")
        ]
        let cronJobs = [
            ClusterResourceSummary(kind: .cronJob, name: "nightly-report", namespace: "demo", primaryText: "0 2 * * *", secondaryText: "Active")
        ]
        let persistentVolumeClaims = [
            ClusterResourceSummary(kind: .persistentVolumeClaim, name: "postgres-data", namespace: "demo", primaryText: "Bound", secondaryText: "10Gi")
        ]
        let persistentVolumes = [
            ClusterResourceSummary(kind: .persistentVolume, name: "demo-pv-postgres", namespace: nil, primaryText: "Bound", secondaryText: "10Gi demo-retain")
        ]
        let storageClasses = [
            ClusterResourceSummary(kind: .storageClass, name: "demo-retain", namespace: nil, primaryText: "no-provisioner", secondaryText: "No")
        ]
        let nodes = [
            ClusterResourceSummary(kind: .node, name: "demo-node-a", namespace: nil, primaryText: "Ready", secondaryText: "v1.32.0"),
            ClusterResourceSummary(kind: .node, name: "demo-node-b", namespace: nil, primaryText: "Ready", secondaryText: "v1.32.0")
        ]
        let roles = [
            ClusterResourceSummary(kind: .role, name: "api-reader", namespace: "demo", primaryText: "2 rules", secondaryText: "Namespaced role")
        ]
        let roleBindings = [
            ClusterResourceSummary(kind: .roleBinding, name: "api-reader-binding", namespace: "demo", primaryText: "→ Role/api-reader", secondaryText: "1 subject")
        ]
        let clusterRoles = [
            ClusterResourceSummary(kind: .clusterRole, name: "demo-view", namespace: nil, primaryText: "3 rules", secondaryText: "Cluster role")
        ]
        let clusterRoleBindings = [
            ClusterResourceSummary(kind: .clusterRoleBinding, name: "demo-view-binding", namespace: nil, primaryText: "→ ClusterRole/demo-view", secondaryText: "1 subject")
        ]
        let helmReleases = [
            HelmReleaseSummary(
                name: "api",
                namespace: "demo",
                revision: 2,
                updated: "2026-05-05 10:03:00",
                status: "deployed",
                chart: "api-1.2.0",
                appVersion: "1.2.0"
            )
        ]
        let operatorResources = [
            OperatorResourceSummary(
                family: "olm",
                kind: "Subscription",
                apiPath: "/apis/operators.coreos.com/v1alpha1/namespaces/demo/subscriptions",
                name: "demo-operator",
                namespace: "demo",
                status: "AtLatestKnown",
                message: "Demo operator subscription is healthy"
            )
        ]

        state.setNamespaces(["demo", "kube-system"])
        state.setPods(pods)
        state.setDeployments(deployments)
        state.setStatefulSets(statefulSets)
        state.setDaemonSets(daemonSets)
        state.setJobs(jobs)
        state.setServices(services)
        state.setEvents(events)
        state.setConfigMaps(configMaps)
        state.setCronJobs(cronJobs)
        state.setReplicaSets(replicaSets)
        state.setPersistentVolumeClaims(persistentVolumeClaims)
        state.setPersistentVolumes(persistentVolumes)
        state.setStorageClasses(storageClasses)
        state.setHorizontalPodAutoscalers(horizontalPodAutoscalers)
        state.setNetworkPolicies(networkPolicies)
        state.setIngresses(ingresses)
        state.setSecrets(secrets)
        state.setNodes(nodes)
        state.setRBACData(
            roles: roles,
            serviceAccounts: [],
            roleBindings: roleBindings,
            clusterRoles: clusterRoles,
            clusterRoleBindings: clusterRoleBindings
        )
        rbacDataScope = RBACDataScope(
            kubeConfigSources: state.kubeConfigSources,
            context: demoContext,
            namespace: "demo"
        )
        state.setHelmReleases(helmReleases)
        state.setOperatorResources(operatorResources)
        state.markResourceListsLive(
            RuneResourceListFamily.allCases,
            message: "In-memory demo cluster loaded. No Kubernetes API calls are made."
        )
        state.setOverviewSnapshot(
            pods: pods,
            deploymentsCount: deployments.count,
            servicesCount: services.count,
            ingressesCount: ingresses.count,
            configMapsCount: configMaps.count,
            cronJobsCount: cronJobs.count,
            nodesCount: nodes.count,
            clusterCPUPercent: 34,
            clusterMemoryPercent: 41,
            events: events
        )
        state.setPodLogs("2026-05-05T10:00:00Z started demo API\n2026-05-05T10:00:02Z handled GET /healthz\n")
        state.setUnifiedServiceLogs("[api-6f78d9d7c9-2xkq8] 2026-05-05T10:00:00Z started demo API\n[worker-7c9d8f6b5c-mk42q] 2026-05-05T10:00:04Z processed demo job", pods: pods.map(\.name))
    }

    private func applyDemoResourceDetailsForCurrentSelection() {
        guard state.selectedContext?.name == demoContextName else { return }

        if state.selectedSection == .helm {
            applyDemoHelmDetailsForCurrentSelection()
            return
        }

        guard let reference = demoCurrentResourceReference() else {
            state.setResourceYAML("")
            state.setResourceDescribe("")
            return
        }

        state.setResourceYAML(demoResourceYAML(kind: reference.kind, name: reference.name, namespace: reference.namespace))
        state.setResourceDescribe(demoResourceDescribe(kind: reference.kind, name: reference.name, namespace: reference.namespace))
        applyDemoLogsForCurrentSelection(contextName: demoContextName, namespace: state.selectedNamespace)
    }

    private func demoCurrentResourceReference() -> (kind: KubeResourceKind, name: String, namespace: String?)? {
        switch state.selectedWorkloadKind {
        case .pod:
            guard let pod = state.selectedPod else { return nil }
            return (.pod, pod.name, pod.namespace)
        case .deployment:
            guard let deployment = state.selectedDeployment else { return nil }
            return (.deployment, deployment.name, deployment.namespace)
        case .service:
            guard let service = state.selectedService else { return nil }
            return (.service, service.name, service.namespace)
        case .statefulSet:
            return state.selectedStatefulSet.map { ($0.kind, $0.name, $0.namespace) }
        case .daemonSet:
            return state.selectedDaemonSet.map { ($0.kind, $0.name, $0.namespace) }
        case .job:
            return state.selectedJob.map { ($0.kind, $0.name, $0.namespace) }
        case .cronJob:
            return state.selectedCronJob.map { ($0.kind, $0.name, $0.namespace) }
        case .replicaSet:
            return state.selectedReplicaSet.map { ($0.kind, $0.name, $0.namespace) }
        case .persistentVolumeClaim:
            return state.selectedPersistentVolumeClaim.map { ($0.kind, $0.name, $0.namespace) }
        case .persistentVolume:
            return state.selectedPersistentVolume.map { ($0.kind, $0.name, $0.namespace) }
        case .storageClass:
            return state.selectedStorageClass.map { ($0.kind, $0.name, $0.namespace) }
        case .horizontalPodAutoscaler:
            return state.selectedHorizontalPodAutoscaler.map { ($0.kind, $0.name, $0.namespace) }
        case .networkPolicy:
            return state.selectedNetworkPolicy.map { ($0.kind, $0.name, $0.namespace) }
        case .endpoint:
            return state.selectedEndpoint.map { ($0.kind, $0.name, $0.namespace) }
        case .ingress:
            return state.selectedIngress.map { ($0.kind, $0.name, $0.namespace) }
        case .configMap:
            return state.selectedConfigMap.map { ($0.kind, $0.name, $0.namespace) }
        case .secret:
            return state.selectedSecret.map { ($0.kind, $0.name, $0.namespace) }
        case .node:
            return state.selectedNode.map { ($0.kind, $0.name, $0.namespace) }
        case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            return state.selectedRBACResource.map { ($0.kind, $0.name, $0.namespace) }
        case .event:
            return nil
        }
    }

    private func demoResourceYAML(kind: KubeResourceKind, name: String, namespace: String?) -> String {
        let apiVersion: String = {
            switch kind {
            case .deployment, .statefulSet, .daemonSet, .replicaSet: return "apps/v1"
            case .job, .cronJob: return "batch/v1"
            case .horizontalPodAutoscaler: return "autoscaling/v2"
            case .networkPolicy: return "networking.k8s.io/v1"
            case .role, .roleBinding, .clusterRole, .clusterRoleBinding: return "rbac.authorization.k8s.io/v1"
            case .ingress: return "networking.k8s.io/v1"
            case .storageClass: return "storage.k8s.io/v1"
            default: return "v1"
            }
        }()
        let namespaceLine = namespace.map { "  namespace: \($0)\n" } ?? ""
        return """
        apiVersion: \(apiVersion)
        kind: \(kind.singularTypeName)
        metadata:
          name: \(name)
        \(namespaceLine)  labels:
            app.kubernetes.io/part-of: rune-demo
        spec:
          demo: true
        status:
          phase: Ready
        """
    }

    private func demoResourceDescribe(kind: KubeResourceKind, name: String, namespace: String?) -> String {
        """
        Name:           \(name)
        Namespace:      \(namespace ?? "<cluster>")
        Kind:           \(kind.singularTypeName)
        Status:         Ready
        Labels:         app.kubernetes.io/part-of=rune-demo

        Events:
          Type    Reason    Age   From        Message
          Normal  Synced    1m    rune-demo   In-memory demo resource is healthy
        """
    }

    private func applyDemoLogsForCurrentSelection(contextName: String, namespace: String) {
        state.setLastLogFetchError(nil)
        state.isLoadingLogs = false
        switch state.selectedWorkloadKind {
        case .pod:
            guard let pod = state.selectedPod else { return }
            state.appendPodLogRead(
                "2026-05-05T10:00:00Z started demo API\n2026-05-05T10:00:02Z handled GET /healthz\n",
                contextName: contextName,
                namespace: namespace,
                podName: pod.name
            )
            state.clearUnifiedServiceLogs()
        case .deployment:
            guard let deployment = state.selectedDeployment else { return }
            state.appendUnifiedServiceLogRead(
                "[api-6f78d9d7c9-2xkq8] 2026-05-05T10:03:00Z deployment rollout complete\n[api-6f78d9d7c9-2xkq8] 2026-05-05T10:03:04Z ready to serve traffic\n",
                pods: state.pods.map(\.name),
                contextName: contextName,
                namespace: namespace,
                kind: .deployment,
                resourceName: deployment.name
            )
        case .service:
            guard let service = state.selectedService else { return }
            state.appendUnifiedServiceLogRead(
                "[api-6f78d9d7c9-2xkq8] 2026-05-05T10:04:00Z service route accepted demo request\n[worker-7c9d8f6b5c-mk42q] 2026-05-05T10:04:03Z processed background job\n",
                pods: state.pods.map(\.name),
                contextName: contextName,
                namespace: namespace,
                kind: .service,
                resourceName: service.name
            )
        default:
            state.clearUnifiedServiceLogs()
        }
    }

    private func applyDemoHelmDetailsForCurrentSelection() {
        guard state.selectedHelmRelease != nil else {
            state.setHelmValues("")
            state.setHelmManifest("")
            state.setHelmHistory([])
            return
        }
        state.setHelmValues("""
        replicaCount: 1
        image:
          repository: rune-demo/api
          tag: 1.2.0
        service:
          port: 80
        """)
        state.setHelmManifest("""
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: api
          namespace: demo
        spec:
          replicas: 1
        """)
        state.setHelmHistory([
            HelmReleaseRevision(revision: 1, updated: "2026-05-05 10:00:00", status: "superseded", chart: "api-1.1.0", appVersion: "1.1.0", description: "Initial demo install"),
            HelmReleaseRevision(revision: 2, updated: "2026-05-05 10:03:00", status: "deployed", chart: "api-1.2.0", appVersion: "1.2.0", description: "Demo rollout")
        ])
    }

    private func applyDemoOperatorResourceDetailsForCurrentSelection() {
        guard let resource = state.selectedOperatorResource else {
            state.clearResourceDetails()
            return
        }
        state.setResourceYAML("""
        apiVersion: operators.coreos.com/v1alpha1
        kind: \(resource.kind)
        metadata:
          name: \(resource.name)
          namespace: \(resource.namespace ?? "demo")
        status:
          state: \(resource.status)
        """)
        state.setResourceDescribe("""
        Name:       \(resource.name)
        Namespace:  \(resource.namespace ?? "<cluster>")
        Operator:   \(resource.kind)
        Status:     \(resource.status)

        \(resource.message)
        """)
    }

    public func requestDeleteSelectedResource() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let (kind, name) = currentDeletableResource() else { return }
        pendingWriteAction = .delete(kind: kind, name: name)
    }

    public func requestDeleteResource(kind: KubeResourceKind, name: String) {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        pendingWriteAction = .delete(kind: kind, name: name)
    }

    public func requestDeleteSelectedGenericResources() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        let resources = selectedGenericResourcesForBulkActions.map { resource in
            PendingWriteAction.BulkDeleteResource(
                kind: resource.kind,
                name: resource.name,
                namespace: resource.namespace ?? state.selectedNamespace
            )
        }
        guard !resources.isEmpty else { return }
        pendingWriteAction = .deleteMany(resources)
    }

    public func requestApplySelectedResourceYAML() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let (kind, name) = currentWritableResource(), !state.resourceYAML.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        guard loadedResourceDetailScopeMatchesCurrentSelection() else { return }
        guard state.resourceYAMLHasUnsavedEdits else { return }
        guard !state.resourceYAMLValidationIssues.contains(where: { $0.severity == .error }) else {
            state.setError(RuneError.invalidInput(message: "Fix YAML errors before applying."))
            return
        }
        let action = PendingWriteAction.apply(kind: kind, name: name, yaml: state.resourceYAML, baseline: state.resourceYAMLBaseline)
        pendingWriteAction = action
        runPendingApplyDryRunPreview(yaml: state.resourceYAML, action: action)
    }

    public var canReapplyResourceYAMLBaseline: Bool {
        guard let (kind, _) = currentWritableResource() else { return false }
        guard kind != .secret else { return false }
        return state.resourceYAMLHasUnsavedEdits
            && !state.resourceYAMLBaseline.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    public func requestReapplyResourceYAMLBaseline() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let (kind, name) = currentWritableResource() else { return }
        guard loadedResourceDetailScopeMatchesCurrentSelection() else { return }
        guard kind != .secret else {
            state.setError(RuneError.invalidInput(message: "Apply last fetched YAML is disabled for Secrets. Review the diff and apply YAML explicitly."))
            return
        }
        guard canReapplyResourceYAMLBaseline else { return }

        let action = PendingWriteAction.apply(
            kind: kind,
            name: name,
            yaml: state.resourceYAMLBaseline,
            baseline: state.resourceYAML
        )
        pendingWriteAction = action
        runPendingApplyDryRunPreview(yaml: state.resourceYAMLBaseline, action: action)
    }

    public func requestScaleSelectedDeployment() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let deployment = state.selectedDeployment else { return }
        pendingWriteAction = .scale(deploymentName: deployment.name, replicas: max(0, scaleReplicaInput))
    }

    public func requestScaleSelectedStatefulSet() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let statefulSet = state.selectedStatefulSet else { return }
        pendingWriteAction = .scaleStatefulSet(name: statefulSet.name, replicas: max(0, scaleReplicaInput))
    }

    public func requestRolloutRestartSelectedDeployment() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let deployment = state.selectedDeployment else { return }
        pendingWriteAction = .rolloutRestart(deploymentName: deployment.name)
    }

    public func requestRolloutRestartSelectedStatefulSet() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let statefulSet = state.selectedStatefulSet else { return }
        pendingWriteAction = .rolloutRestartStatefulSet(name: statefulSet.name)
    }

    public func requestRolloutUndoSelectedDeployment() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let deployment = state.selectedDeployment else { return }

        do {
            let revision = try parseOptionalRevisionInput(rolloutRevisionInput)
            let action = PendingWriteAction.rolloutUndo(deploymentName: deployment.name, revision: revision)
            pendingWriteAction = action
            pendingRollbackPlan = rollbackPlan(for: deployment, revision: revision, action: action)
            runPendingRolloutDryRunPreview(deploymentName: deployment.name, revision: revision, action: action)
        } catch {
            state.setError(error)
        }
    }

    public func requestRolloutUndoSelectedController() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }

        let resource: ClusterResourceSummary?
        switch state.selectedWorkloadKind {
        case .statefulSet:
            resource = state.selectedStatefulSet
        case .daemonSet:
            resource = state.selectedDaemonSet
        default:
            resource = nil
        }
        guard let resource else { return }

        do {
            let revision = try parseOptionalRevisionInput(rolloutRevisionInput)
            let action = PendingWriteAction.controllerRolloutUndo(
                kind: resource.kind,
                name: resource.name,
                revision: revision
            )
            pendingWriteAction = action
            pendingRollbackPlan = controllerRollbackPlan(for: resource, revision: revision, action: action)
            if UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun {
                pendingWriteDryRunStatus = "\(resource.kind.singularTypeName) rollback dry-run is not available through Rune's native backend yet. Rune will not run this rollback automatically; copy the command and run it explicitly after review."
            } else {
                pendingWriteDryRunStatus = nil
            }
        } catch {
            state.setError(error)
        }
    }

    public func requestHelmRollback(revision: Int) {
        requestHelmRollback(
            revision: revision,
            wait: helmRollbackWait,
            timeout: helmRollbackTimeoutInput,
            cleanupOnFail: helmRollbackCleanupOnFail
        )
    }

    public func requestHelmRollback(
        revision: Int,
        wait: Bool,
        timeout: String,
        cleanupOnFail: Bool
    ) {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let release = state.selectedHelmRelease else { return }
        let normalizedTimeout = timeout.trimmingCharacters(in: .whitespacesAndNewlines)

        let action = PendingWriteAction.helmRollback(
            releaseName: release.name,
            namespace: release.namespace,
            revision: revision,
            wait: wait,
            timeout: normalizedTimeout,
            cleanupOnFail: cleanupOnFail
        )
        pendingWriteAction = action
        pendingRollbackPlan = helmRollbackPlan(for: release, revision: revision, action: action)
        if UserDefaults.standard.runeWriteSafetyRequireHelmDryRun {
            runPendingHelmDryRunPreview(action: action)
        } else {
            pendingWriteDryRunStatus = nil
        }
    }

    public func requestExecInSelectedPod() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let pod = state.selectedPod else { return }

        do {
            let command = try parseCommandInput(execCommandInput)
            pendingWriteAction = .exec(podName: pod.name, command: command)
        } catch {
            state.setError(error)
        }
    }

    public func startTerminalSessionInSelectedPod() {
        guard let pod = state.selectedPod else { return }
        startTerminalSession(for: pod)
    }

    public func startTerminalSession(
        for pod: PodSummary,
        container: String? = nil,
        replacingSessionID: String? = nil
    ) {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let context = state.selectedContext else { return }

        let kubeConfigSources = state.kubeConfigSources
        let namespace = state.selectedNamespace
        let containerName = Self.normalizedTerminalContainerSelection(container, pod: pod)
        if let replacingSessionID {
            guard let replaced = state.terminalSessions.first(where: { $0.id == replacingSessionID }),
                  replaced.targets(
                      contextName: context.name,
                      namespace: namespace,
                      podName: pod.name,
                      containerName: containerName
                  ) else {
                state.setError(
                    RuneError.invalidInput(
                        message: "The terminal target changed. Open a new shell tab for the current context, namespace, pod, and container."
                    )
                )
                return
            }
        }
        if replacingSessionID == nil,
           let existing = state.terminalSessions.first(where: {
               $0.targets(
                   contextName: context.name,
                   namespace: namespace,
                   podName: pod.name,
                   containerName: containerName
               )
           }) {
            state.selectTerminalSession(id: existing.id)
            terminalSessionInput = ""
            if existing.status == .disconnected || existing.status == .failed {
                startTerminalSession(for: pod, container: containerName, replacingSessionID: existing.id)
            }
            return
        }

        let sessionID = replacingSessionID ?? UUID().uuidString
        let attemptID = UUID()
        terminalSessionAttemptByID[sessionID] = attemptID
        let existingTranscript = replacingSessionID.flatMap { id in
            state.terminalSessions.first(where: { $0.id == id })?.transcript
        } ?? ""
        let initialTranscript = existingTranscript.isEmpty
            ? ""
            : existingTranscript + "\n[rune] Reconnecting to \(pod.name) in \(namespace)...\n"
        state.setTerminalSession(
            PodTerminalSession(
                id: sessionID,
                contextName: context.name,
                namespace: namespace,
                podName: pod.name,
                containerName: containerName,
                shell: terminalShellCommand.joined(separator: " "),
                transcript: initialTranscript,
                status: .connecting
            )
        )
        terminalSessionInput = ""
        state.selectedSection = .terminal

        Task {
            do {
                try await kubeClient.startPodTerminalSession(
                    id: sessionID,
                    from: kubeConfigSources,
                    context: context,
                    namespace: namespace,
                    podName: pod.name,
                    container: containerName,
                    shellCommand: terminalShellCommand,
                    onOutput: { [weak self] chunk in
                        Task { @MainActor [weak self] in
                            guard let self,
                                  self.terminalSessionAttemptByID[sessionID] == attemptID else { return }
                            self.enqueueTerminalSessionOutput(id: sessionID, text: chunk)
                        }
                    },
                    onTermination: { [weak self] exitCode in
                        Task { @MainActor [weak self] in
                            guard let self,
                                  self.terminalSessionAttemptByID[sessionID] == attemptID else { return }
                            self.terminalSessionAttemptByID.removeValue(forKey: sessionID)
                            self.flushTerminalSessionOutput()
                            let status: PodTerminalSessionStatus = exitCode == 0 ? .disconnected : .failed
                            self.state.updateTerminalSessionStatus(id: sessionID, status: status, exitCode: exitCode)
                            self.state.appendTerminalSessionOutput(
                                id: sessionID,
                                text: "\n[rune] Session ended (exit \(exitCode)).\n"
                            )
                        }
                    }
                )
                guard terminalSessionAttemptByID[sessionID] == attemptID,
                      state.terminalSessions.contains(where: {
                          $0.id == sessionID
                              && $0.targets(
                                  contextName: context.name,
                                  namespace: namespace,
                                  podName: pod.name,
                                  containerName: containerName
                              )
                      }) else {
                    await kubeClient.stopPodTerminalSession(id: sessionID)
                    return
                }
                state.updateTerminalSessionStatus(id: sessionID, status: .connected)
                state.appendTerminalSessionOutput(
                    id: sessionID,
                    text: "[rune] Connected to \(pod.name) in \(namespace).\n"
                )
            } catch {
                guard terminalSessionAttemptByID[sessionID] == attemptID else { return }
                terminalSessionAttemptByID.removeValue(forKey: sessionID)
                if Self.isBenignCancellationError(error) {
                    return
                }
                let diagnostic = PodTerminalSessionDiagnostic.classify(
                    errorMessage: error.localizedDescription,
                    podName: pod.name,
                    containerName: containerName,
                    shell: terminalShellCommand.joined(separator: " ")
                )
                state.updateTerminalSessionStatus(id: sessionID, status: .failed, diagnostic: diagnostic)
                state.appendTerminalSessionOutput(
                    id: sessionID,
                    text: "\(diagnostic.transcriptMessage)\n[rune] Details: \(error.localizedDescription)\n"
                )
                state.setError(error)
            }
        }
    }

    public nonisolated static func normalizedTerminalContainerSelection(
        _ container: String?,
        pod: PodSummary
    ) -> String? {
        let trimmed = container?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !trimmed.isEmpty else { return nil }
        let containers = pod.containerNames
        guard containers.isEmpty || containers.contains(trimmed) else { return nil }
        return trimmed
    }

    public func sendTerminalSessionInput() {
        guard let session = state.terminalSession else { return }
        guard session.status == .connected else { return }
        let command = terminalSessionInput.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !command.isEmpty else { return }
        terminalSessionInput = ""

        Task {
            do {
                try await kubeClient.writeToPodTerminalSession(id: session.id, text: command + "\n")
            } catch {
                state.setError(error)
            }
        }
    }

    public func sendTerminalControlSequence(_ text: String) {
        guard let session = state.terminalSession else { return }
        guard session.status == .connected else { return }
        guard !text.isEmpty else { return }

        Task {
            do {
                try await kubeClient.writeToPodTerminalSession(id: session.id, text: text)
            } catch {
                state.setError(error)
            }
        }
    }

    public func resizeTerminalSession(id: String, columns: Int, rows: Int) {
        guard let session = state.terminalSessions.first(where: { $0.id == id }) else { return }
        guard session.status == .connected else { return }

        Task {
            do {
                try await kubeClient.resizePodTerminalSession(id: id, columns: columns, rows: rows)
            } catch {
                state.setError(error)
            }
        }
    }

    private func enqueueTerminalSessionOutput(id: String, text: String) {
        var pendingEscape = pendingTerminalEscapeBySessionID[id] ?? ""
        let sanitized = TerminalTranscriptSanitizer.sanitize(text, pendingEscape: &pendingEscape)
        pendingTerminalEscapeBySessionID[id] = pendingEscape
        guard !sanitized.isEmpty else { return }

        pendingTerminalOutputBySessionID[id, default: ""] += sanitized
        terminalOutputFlushTask?.cancel()
        let flushDelay = terminalOutputFlushNanoseconds
        terminalOutputFlushTask = Task { @MainActor [weak self] in
            try? await Task.sleep(nanoseconds: flushDelay)
            self?.flushTerminalSessionOutput()
        }
    }

    private func flushTerminalSessionOutput() {
        guard !pendingTerminalOutputBySessionID.isEmpty else { return }
        let pending = pendingTerminalOutputBySessionID
        pendingTerminalOutputBySessionID.removeAll(keepingCapacity: true)
        for (id, text) in pending {
            state.appendTerminalSessionOutput(id: id, text: text)
        }
    }

    public func applySuggestedTerminalCommand(_ command: String, sendImmediately: Bool = false) {
        let trimmed = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        terminalSessionInput = trimmed
        if sendImmediately {
            sendTerminalSessionInput()
        }
    }

    public func stopTerminalSession(resetState: Bool = false) {
        guard let session = state.terminalSession else { return }
        let sessionID = session.id
        flushTerminalSessionOutput()
        terminalSessionAttemptByID.removeValue(forKey: sessionID)
        pendingTerminalOutputBySessionID.removeValue(forKey: sessionID)
        pendingTerminalEscapeBySessionID.removeValue(forKey: sessionID)
        terminalSessionInput = ""
        if resetState {
            state.setTerminalSession(nil)
        } else {
            guard session.status == .connected || session.status == .connecting else { return }
            state.updateTerminalSessionStatus(id: sessionID, status: .disconnected, exitCode: session.lastExitCode)
        }
        Task {
            await kubeClient.stopPodTerminalSession(id: sessionID)
        }
    }

    private func stopAndClearTerminalSessions(contextName: String, namespace: String? = nil) {
        let removed = state.removeTerminalSessions(contextName: contextName, namespace: namespace)
        guard !removed.isEmpty else { return }

        let sessionIDs = removed.map(\.id)
        for sessionID in sessionIDs {
            terminalSessionAttemptByID.removeValue(forKey: sessionID)
            pendingTerminalOutputBySessionID.removeValue(forKey: sessionID)
            pendingTerminalEscapeBySessionID.removeValue(forKey: sessionID)
        }
        terminalSessionInput = ""
        Task {
            for sessionID in sessionIDs {
                await kubeClient.stopPodTerminalSession(id: sessionID)
            }
        }
    }

    public func selectTerminalSession(id: String) {
        state.selectTerminalSession(id: id)
        terminalSessionInput = ""
    }

    public func closeTerminalSession(id: String) {
        state.selectTerminalSession(id: id)
        stopTerminalSession(resetState: true)
    }

    public func clearTerminalSessionTranscript() {
        state.clearTerminalSessionTranscript()
    }

    public func startPortForwardForSelection() {
        let target: (PortForwardTargetKind, String)
        switch state.selectedWorkloadKind {
        case .pod:
            guard let pod = state.selectedPod else { return }
            target = (.pod, pod.name)
        case .service:
            guard let service = state.selectedService else { return }
            target = (.service, service.name)
        case .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            state.setError(RuneError.invalidInput(message: "Port-forward is only supported for Pod or Service right now."))
            return
        }

        startPortForward(targetKind: target.0, targetName: target.1)
    }

    public func startPortForward(targetKind: PortForwardTargetKind, targetName: String) {
        startPortForward(
            targetKind: targetKind,
            targetName: targetName,
            localPortInput: portForwardLocalPortInput,
            remotePortInput: portForwardRemotePortInput,
            addressInput: portForwardAddressInput
        )
    }

    private func startPortForward(
        targetKind: PortForwardTargetKind,
        targetName: String,
        localPortInput: String,
        remotePortInput: String,
        addressInput: String,
        scope explicitScope: PortForwardStartScope? = nil
    ) {
        guard let scope = explicitScope ?? currentPortForwardStartScope() else {
            state.setError(RuneError.invalidInput(message: "Select a Kubernetes context before starting port-forward."))
            return
        }

        Task {
            var portForwardRowsOwnError = false
            do {
                let localPort = try parsePort(localPortInput, fieldName: "local port")
                let remotePort = try parsePort(remotePortInput, fieldName: "remote port")
                let address = normalizedPortForwardAddress(addressInput)

                if let existing = activePortForwardSession(contextName: scope.context.name, address: address, localPort: localPort) {
                    if existing.targetKind != targetKind || existing.targetName != targetName || existing.namespace != scope.namespace {
                        state.upsertPortForwardSession(
                            PortForwardSession(
                                id: UUID().uuidString,
                                contextName: scope.context.name,
                                namespace: scope.namespace,
                                targetKind: targetKind,
                                targetName: targetName,
                                localPort: localPort,
                                remotePort: remotePort,
                                address: address,
                                status: .failed,
                                lastMessage: "Local port \(address):\(localPort) is already used by \(existing.resourceLabel). Stop that row or choose another local port."
                            )
                        )
                    }
                    state.clearError()
                    state.selectedSection = .terminal
                    return
                }

                state.isStartingPortForward = true
                portForwardRowsOwnError = true
                defer { state.isStartingPortForward = false }

                let session = try await kubeClient.startPortForward(
                    from: scope.kubeConfigSources,
                    context: scope.context,
                    namespace: scope.namespace,
                    targetKind: targetKind,
                    targetName: targetName,
                    localPort: localPort,
                    remotePort: remotePort,
                    address: address
                ) { [weak self] session in
                    Task { @MainActor in
                        self?.state.upsertPortForwardSession(session)
                    }
                }

                if !state.portForwardSessions.contains(where: { $0.id == session.id }) {
                    state.upsertPortForwardSession(session)
                }
                if session.status != .stopped {
                    state.selectedSection = .terminal
                }
            } catch {
                if portForwardRowsOwnError {
                    state.clearError()
                } else {
                    state.setError(error)
                }
            }
        }
    }

    func currentPortForwardStartScope() -> PortForwardStartScope? {
        guard let context = state.selectedContext else { return nil }
        return PortForwardStartScope(
            kubeConfigSources: state.kubeConfigSources,
            context: context,
            namespace: state.selectedNamespace
        )
    }

    func portForwardRetryScope(for session: PortForwardSession) -> PortForwardStartScope? {
        guard !state.kubeConfigSources.isEmpty,
              !session.namespace.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              let context = state.selectedContext,
              context.name == session.contextName,
              state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
                  .caseInsensitiveCompare(session.namespace.trimmingCharacters(in: .whitespacesAndNewlines)) == .orderedSame,
              state.contexts.contains(where: { $0.name == context.name }) else {
            return nil
        }
        return PortForwardStartScope(
            kubeConfigSources: state.kubeConfigSources,
            context: context,
            namespace: session.namespace
        )
    }

    private func normalizedPortForwardAddress(_ rawAddress: String? = nil) -> String {
        let trimmed = (rawAddress ?? portForwardAddressInput).trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "127.0.0.1" : trimmed
    }

    private func activePortForwardSession(contextName: String, address: String, localPort: Int) -> PortForwardSession? {
        state.portForwardSessions.first {
            $0.contextName == contextName
                && $0.address == address
                && $0.localPort == localPort
                && $0.isActiveOrStarting
        }
    }

    public func retryPortForward(_ session: PortForwardSession) {
        guard let scope = portForwardRetryScope(for: session) else {
            state.setError(
                RuneError.invalidInput(
                    message: "Cannot retry port-forward outside its original active context and namespace. Switch to that scope or start a new port-forward in the intended scope."
                )
            )
            return
        }
        clearPortForwardSession(session)
        startPortForward(
            targetKind: session.targetKind,
            targetName: session.targetName,
            localPortInput: String(session.localPort),
            remotePortInput: String(session.remotePort),
            addressInput: session.address,
            scope: scope
        )
    }

    public func stopPortForward(_ session: PortForwardSession) {
        state.upsertPortForwardSession(
            PortForwardSession(
                id: session.id,
                contextName: session.contextName,
                namespace: session.namespace,
                targetKind: session.targetKind,
                targetName: session.targetName,
                localPort: session.localPort,
                remotePort: session.remotePort,
                address: session.address,
                status: .stopped,
                lastMessage: "Port-forward stopped."
            )
        )
        if !state.portForwardSessions.contains(where: { $0.status == .starting }) {
            state.isStartingPortForward = false
        }
        Task {
            await kubeClient.stopPortForward(sessionID: session.id)
        }
    }

    public func clearPortForwardSession(_ session: PortForwardSession) {
        guard session.isInactive else { return }
        state.removePortForwardSession(id: session.id)
    }

    public func clearInactivePortForwardSessions() {
        state.removeInactivePortForwardSessions()
    }

    public func clearInactivePortForwardSessions(targetKind: PortForwardTargetKind, targetName: String, namespace: String) {
        state.removeInactivePortForwardSessions(targetKind: targetKind, targetName: targetName, namespace: namespace)
    }

    public func openPortForwardInBrowser(_ session: PortForwardSession) {
        guard let url = session.browserURL else {
            state.setError(RuneError.invalidInput(message: "Port-forward is not connected yet."))
            return
        }

        portForwardBrowserOpener.open(url)
    }

    public func cancelPendingWriteAction() {
        if pendingWriteAction == nil {
            pendingProductionDestructiveConfirmation = nil
            pendingRollbackPlan = nil
        } else {
            pendingWriteAction = nil
        }
        pendingWriteDryRunStatus = nil
    }

    public func confirmPendingWriteAction() {
        guard writeActionsEnabled else {
            pendingWriteAction = nil
            pendingWriteDryRunStatus = nil
            pendingRollbackPlan = nil
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let action = pendingWriteAction else { return }
        guard let scope = pendingWriteScopeSnapshot else {
            pendingWriteAction = nil
            pendingWriteDryRunStatus = nil
            pendingRollbackPlan = nil
            state.setError(RuneError.invalidInput(message: "The write target is unavailable. Arm the action again."))
            return
        }
        if scope.isProduction,
           action.isDestructive,
           requiresProductionSecondConfirmation,
           (pendingProductionDestructiveConfirmation != action
               || pendingProductionDestructiveConfirmationScopeID != scope.id)
        {
            pendingProductionDestructiveConfirmation = action
            pendingProductionDestructiveConfirmationScopeID = scope.id
            return
        }
        let audit = auditDetails(for: action, context: scope.context, namespace: scope.namespace)
        pendingWriteAction = nil
        pendingWriteDryRunStatus = nil

        Task {
            do {
                switch action {
                case let .delete(kind, name):
                    try await kubeClient.deleteResource(
                        from: scope.kubeConfigSources,
                        context: scope.context,
                        namespace: scope.namespace,
                        kind: kind,
                        name: name
                    )
                case let .deleteMany(resources):
                    for resource in resources {
                        try await kubeClient.deleteResource(
                            from: scope.kubeConfigSources,
                            context: scope.context,
                            namespace: resource.namespace,
                            kind: resource.kind,
                            name: resource.name
                        )
                    }
                case let .apply(_, _, yaml, _):
                    if UserDefaults.standard.runeWriteSafetyRequireApplyDryRun {
                        let dryRunIssues = try await kubeClient.validateResourceYAML(
                            from: scope.kubeConfigSources,
                            context: scope.context,
                            namespace: scope.namespace,
                            yaml: yaml
                        )
                        let blockingIssues = dryRunIssues.filter { $0.severity == .error }
                        guard blockingIssues.isEmpty else {
                            throw RuneError.invalidInput(message: "Server dry-run failed: \(blockingIssues.map(\.message).joined(separator: " "))")
                        }
                    }
                    try await kubeClient.applyYAML(
                        from: scope.kubeConfigSources,
                        context: scope.context,
                        namespace: scope.namespace,
                        yaml: yaml
                    )
                case let .scale(deploymentName, replicas):
                    try await kubeClient.scaleDeployment(
                        from: scope.kubeConfigSources,
                        context: scope.context,
                        namespace: scope.namespace,
                        deploymentName: deploymentName,
                        replicas: replicas
                    )
                case let .scaleStatefulSet(name, replicas):
                    try await kubeClient.scaleStatefulSet(
                        from: scope.kubeConfigSources,
                        context: scope.context,
                        namespace: scope.namespace,
                        statefulSetName: name,
                        replicas: replicas
                    )
                case let .rolloutRestart(deploymentName):
                    try await kubeClient.restartDeploymentRollout(
                        from: scope.kubeConfigSources,
                        context: scope.context,
                        namespace: scope.namespace,
                        deploymentName: deploymentName
                    )
                case let .rolloutRestartStatefulSet(name):
                    try await kubeClient.restartStatefulSetRollout(
                        from: scope.kubeConfigSources,
                        context: scope.context,
                        namespace: scope.namespace,
                        statefulSetName: name
                    )
                case let .rolloutUndo(deploymentName, revision):
                    if UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun {
                        try await kubeClient.dryRunRollbackDeploymentRollout(
                            from: scope.kubeConfigSources,
                            context: scope.context,
                            namespace: scope.namespace,
                            deploymentName: deploymentName,
                            revision: revision
                        )
                    }
                    try await kubeClient.rollbackDeploymentRollout(
                        from: scope.kubeConfigSources,
                        context: scope.context,
                        namespace: scope.namespace,
                        deploymentName: deploymentName,
                        revision: revision
                    )
                case .controllerRolloutUndo:
                    copyCommandToPasteboard(action.kubectlCommand(contextName: scope.context.name, namespace: scope.namespace))
                    appendWriteAudit(
                        audit,
                        status: "Blocked",
                        message: "Controller rollback command copied; Rune did not run this rollback automatically"
                    )
                    return
                case let .helmRollback(releaseName, namespace, revision, wait, timeout, cleanupOnFail):
                    let request = HelmRollbackRequest(
                        sources: scope.kubeConfigSources,
                        contextName: scope.context.name,
                        namespace: namespace,
                        releaseName: releaseName,
                        revision: revision,
                        wait: wait,
                        timeout: timeout,
                        cleanupOnFail: cleanupOnFail,
                        dryRun: false
                    )
                    if UserDefaults.standard.runeWriteSafetyRequireHelmDryRun {
                        _ = try await helmCommandRunner.rollback(
                            HelmRollbackRequest(
                                sources: request.sources,
                                contextName: request.contextName,
                                namespace: request.namespace,
                                releaseName: request.releaseName,
                                revision: request.revision,
                                wait: request.wait,
                                timeout: request.timeout,
                                cleanupOnFail: request.cleanupOnFail,
                                dryRun: true
                            ),
                            timeout: 120
                        )
                    }
                    _ = try await helmCommandRunner.rollback(request, timeout: 120)
                    if pendingWriteScopeMatchesCurrentSelection(scope) {
                        do {
                            try await loadHelmReleases(
                                context: scope.context,
                                namespace: state.isHelmAllNamespaces ? state.selectedNamespace : namespace,
                                kubeConfigSourcesOverride: scope.kubeConfigSources,
                                allNamespacesOverride: state.isHelmAllNamespaces,
                                loadDetails: false
                            )
                        } catch {
                            diagnostics.trace("helm", "post-rollback release refresh error=\(error.localizedDescription)")
                        }
                    }
                    appendWriteAudit(
                        audit,
                        status: "Succeeded",
                        message: "Helm rollback completed\(UserDefaults.standard.runeWriteSafetyRequireHelmDryRun ? " after Helm dry-run" : "")"
                    )
                    return
                case let .exec(podName, command):
                    state.isExecutingCommand = true
                    defer { state.isExecutingCommand = false }

                    let result = try await kubeClient.execInPod(
                        from: scope.kubeConfigSources,
                        context: scope.context,
                        namespace: scope.namespace,
                        podName: podName,
                        container: nil,
                        command: command
                    )
                    if pendingWriteScopeMatchesCurrentSelection(scope) {
                        state.setLastExecResult(result)
                    }
                    appendWriteAudit(audit, status: "Succeeded", message: "Command exited \(result.exitCode)")
                    return
                case let .createJobFromCronJob(cronJobName, jobName):
                    try await kubeClient.createJobFromCronJob(
                        from: scope.kubeConfigSources,
                        context: scope.context,
                        namespace: scope.namespace,
                        cronJobName: cronJobName,
                        jobName: jobName
                    )
                    if pendingWriteScopeMatchesCurrentSelection(scope) {
                        setWorkloadKind(.job, trackHistory: false, triggerReload: false)
                    }
                }

                if pendingWriteScopeMatchesCurrentSelection(scope) {
                    let requestID = beginSnapshotRequest(
                        context: scope.context,
                        namespace: scope.namespace,
                        source: "confirmPendingWriteAction"
                    )
                    try await loadResourceSnapshot(
                        context: scope.context,
                        namespace: scope.namespace,
                        requestID: requestID,
                        kubeConfigSourcesOverride: scope.kubeConfigSources,
                        allowsBackgroundFollowUpReads: false
                    )
                }
                appendWriteAudit(
                    audit,
                    status: "Succeeded",
                    message: successAuditMessage(
                        for: action,
                        verificationMessage: await postActionVerificationMessage(for: action, scope: scope)
                    )
                )
            } catch {
                appendWriteAudit(audit, status: "Failed", message: error.localizedDescription)
                state.setError(error)
            }
        }
    }

    private func pendingWriteScopeMatchesCurrentSelection(_ scope: PendingWriteScopeSnapshot) -> Bool {
        guard state.kubeConfigSources == scope.kubeConfigSources,
              state.selectedContext == scope.context else {
            return false
        }
        return state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
            .caseInsensitiveCompare(scope.namespace.trimmingCharacters(in: .whitespacesAndNewlines)) == .orderedSame
    }

    private func runPendingApplyDryRunPreview(yaml: String, action: PendingWriteAction) {
        guard UserDefaults.standard.runeWriteSafetyRequireApplyDryRun else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard case .apply = action,
              let scope = pendingWriteScopeSnapshot
        else {
            pendingWriteDryRunStatus = nil
            return
        }

        pendingWriteDryRunStatus = "Checking with Kubernetes API..."

        Task { [weak self] in
            guard let self else { return }
            do {
                let issues = try await self.kubeClient.validateResourceYAML(
                    from: scope.kubeConfigSources,
                    context: scope.context,
                    namespace: scope.namespace,
                    yaml: yaml
                )
                guard self.pendingWriteAction == action,
                      self.pendingWriteScopeSnapshot?.id == scope.id else { return }
                let errors = issues.filter { $0.severity == .error }
                if errors.isEmpty {
                    self.pendingWriteDryRunStatus = "Passed. Kubernetes accepted the server-side dry-run."
                } else {
                    self.pendingWriteDryRunStatus = "Blocked: \(errors.map(\.message).joined(separator: " "))"
                }
            } catch {
                guard self.pendingWriteAction == action,
                      self.pendingWriteScopeSnapshot?.id == scope.id else { return }
                self.pendingWriteDryRunStatus = "Could not complete: \(error.localizedDescription)"
                self.diagnostics.log("pending apply dry-run failed: \(error.localizedDescription)")
            }
        }
    }

    private func runPendingRolloutDryRunPreview(deploymentName: String, revision: Int?, action: PendingWriteAction) {
        guard UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard let scope = pendingWriteScopeSnapshot else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard !scope.kubeConfigSources.isEmpty else {
            pendingWriteDryRunStatus = "Could not complete: No kubeconfig selected."
            return
        }

        pendingWriteDryRunStatus = "Checking with Kubernetes API..."
        Task {
            do {
                try await kubeClient.dryRunRollbackDeploymentRollout(
                    from: scope.kubeConfigSources,
                    context: scope.context,
                    namespace: scope.namespace,
                    deploymentName: deploymentName,
                    revision: revision
                )
                guard self.pendingWriteAction == action,
                      self.pendingWriteScopeSnapshot?.id == scope.id else { return }
                self.pendingWriteDryRunStatus = "Server accepted rollback dry-run."
            } catch {
                guard self.pendingWriteAction == action,
                      self.pendingWriteScopeSnapshot?.id == scope.id else { return }
                self.pendingWriteDryRunStatus = "Could not complete: \(error.localizedDescription)"
                self.diagnostics.log("pending rollout rollback dry-run failed: \(error.localizedDescription)")
            }
        }
    }

    private func runPendingHelmDryRunPreview(action: PendingWriteAction) {
        guard UserDefaults.standard.runeWriteSafetyRequireHelmDryRun else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard let scope = pendingWriteScopeSnapshot else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard case let .helmRollback(releaseName, namespace, revision, wait, timeout, cleanupOnFail) = action else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard !scope.kubeConfigSources.isEmpty else {
            pendingWriteDryRunStatus = "Could not complete: No kubeconfig selected."
            return
        }

        pendingWriteDryRunStatus = "Checking with Helm dry-run..."
        Task {
            do {
                _ = try await helmCommandRunner.rollback(
                    HelmRollbackRequest(
                        sources: scope.kubeConfigSources,
                        contextName: scope.context.name,
                        namespace: namespace,
                        releaseName: releaseName,
                        revision: revision,
                        wait: wait,
                        timeout: timeout,
                        cleanupOnFail: cleanupOnFail,
                        dryRun: true
                    ),
                    timeout: 120
                )
                guard self.pendingWriteAction == action,
                      self.pendingWriteScopeSnapshot?.id == scope.id else { return }
                self.pendingWriteDryRunStatus = "Helm accepted rollback dry-run."
            } catch {
                guard self.pendingWriteAction == action,
                      self.pendingWriteScopeSnapshot?.id == scope.id else { return }
                self.pendingWriteDryRunStatus = "Could not complete: \(error.localizedDescription)"
                self.diagnostics.log("pending helm rollback dry-run failed: \(error.localizedDescription)")
            }
        }
    }

    private func rollbackPlan(
        for deployment: DeploymentSummary,
        revision: Int?,
        action: PendingWriteAction
    ) -> String? {
        guard UserDefaults.standard.runeWriteSafetyShowRollbackPlan else { return nil }
        let targetRevision = revision.map(String.init) ?? "previous revision"
        let selector = deployment.selector?
            .sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: ", ")
        let command = action.kubectlCommand(
            contextName: pendingWriteScopeSnapshot?.context.name ?? "",
            namespace: pendingWriteScopeSnapshot?.namespace ?? deployment.namespace
        )

        var rows = [
            "Target resource: deployment/\(deployment.name)",
            "Namespace: \(deployment.namespace)",
            "Current revision: \(currentDeploymentRevisionText())",
            "Target revision: \(targetRevision)",
            "Affected selector/pods: \(selector?.isEmpty == false ? selector! : "unknown")",
            "Command: \(command)"
        ]
        if !deployment.replicaText.isEmpty {
            rows.insert("Current replicas: \(deployment.replicaText)", at: 3)
        }
        return rows.joined(separator: "\n")
    }

    private func helmRollbackPlan(
        for release: HelmReleaseSummary,
        revision: Int,
        action: PendingWriteAction
    ) -> String? {
        guard UserDefaults.standard.runeWriteSafetyShowRollbackPlan else { return nil }
        return [
            "Target release: \(release.namespace)/\(release.name)",
            "Current revision: \(release.revision)",
            "Target revision: \(revision)",
            "Current status: \(release.status)",
            "Chart: \(release.chart)",
            "Command: \(action.kubectlCommand(contextName: pendingWriteScopeSnapshot?.context.name ?? "", namespace: release.namespace))"
        ].joined(separator: "\n")
    }

    private func controllerRollbackPlan(
        for resource: ClusterResourceSummary,
        revision: Int?,
        action: PendingWriteAction
    ) -> String? {
        guard UserDefaults.standard.runeWriteSafetyShowRollbackPlan else { return nil }
        let namespace = resource.namespace ?? pendingWriteScopeSnapshot?.namespace ?? ""
        let command = action.kubectlCommand(
            contextName: pendingWriteScopeSnapshot?.context.name ?? "",
            namespace: namespace
        )
        return [
            "Target resource: \(resource.kind.kubernetesResourceName)/\(resource.name)",
            "Namespace: \(resource.namespace ?? pendingWriteScopeSnapshot?.namespace ?? "<unknown>")",
            "Target revision: \(revision.map(String.init) ?? "previous revision")",
            "Current status: \(resource.primaryText)",
            "Command: \(command)"
        ].joined(separator: "\n")
    }

    private func currentDeploymentRevisionText() -> String {
        let revisions = state.deploymentRolloutHistory
            .split(separator: "\n")
            .compactMap { line -> Int? in
                let first = line.split(whereSeparator: \.isWhitespace).first
                return first.flatMap { Int($0) }
            }
        guard let current = revisions.max() else { return "unknown" }
        return "\(current)"
    }

    private func auditDetails(
        for action: PendingWriteAction,
        context: KubeContext,
        namespace: String
    ) -> (action: String, resource: String, contextName: String, namespace: String) {
        let resource: String
        let actionName: String

        switch action {
        case let .delete(kind, name):
            actionName = "Delete"
            resource = "\(kind.kubernetesResourceName)/\(name)"
        case let .deleteMany(resources):
            actionName = "Bulk Delete"
            resource = "\(resources.count) resources"
        case let .apply(kind, name, _, _):
            actionName = "Apply YAML"
            resource = "\(kind.kubernetesResourceName)/\(name)"
        case let .scale(deploymentName, replicas):
            actionName = "Scale"
            resource = "deployment/\(deploymentName) replicas=\(replicas)"
        case let .scaleStatefulSet(name, replicas):
            actionName = "Scale"
            resource = "statefulset/\(name) replicas=\(replicas)"
        case let .rolloutRestart(deploymentName):
            actionName = "Rollout Restart"
            resource = "deployment/\(deploymentName)"
        case let .rolloutRestartStatefulSet(name):
            actionName = "Rollout Restart"
            resource = "statefulset/\(name)"
        case let .rolloutUndo(deploymentName, revision):
            actionName = "Rollout Undo"
            resource = revision.map { "deployment/\(deploymentName) revision=\($0)" } ?? "deployment/\(deploymentName)"
        case let .controllerRolloutUndo(kind, name, revision):
            actionName = "Controller Rollout Undo"
            resource = revision.map { "\(kind.kubernetesResourceName)/\(name) revision=\($0)" } ?? "\(kind.kubernetesResourceName)/\(name)"
        case let .helmRollback(releaseName, namespace, revision, _, timeout, cleanupOnFail):
            actionName = "Helm Rollback"
            resource = "helmrelease/\(namespace)/\(releaseName) revision=\(revision) timeout=\(timeout) cleanupOnFail=\(cleanupOnFail)"
        case let .exec(podName, command):
            actionName = "Exec"
            resource = "pod/\(podName) \(command.joined(separator: " "))"
        case let .createJobFromCronJob(cronJobName, jobName):
            actionName = "Create Job"
            resource = "cronjob/\(cronJobName) -> job/\(jobName)"
        }

        return (actionName, resource, context.name, namespace)
    }

    private func postActionVerificationMessage(
        for action: PendingWriteAction,
        scope: PendingWriteScopeSnapshot
    ) async -> String? {
        guard UserDefaults.standard.runeWriteSafetyRequirePostActionVerification else { return nil }

        switch action {
        case let .rolloutUndo(deploymentName, _):
            do {
                let result = try await kubeClient.verifyDeploymentRollout(
                    from: scope.kubeConfigSources,
                    context: scope.context,
                    namespace: scope.namespace,
                    deploymentName: deploymentName
                )
                switch result.status {
                case .ready:
                    return "Post-action verification: \(result.message)"
                case .progressing:
                    return "Post-action verification: \(result.message)"
                case .timedOut:
                    return "Post-action verification: \(result.message)"
                case .failed:
                    return "Post-action verification failed: \(result.message)"
                }
            } catch {
                diagnostics.log("post-action rollout verification failed: \(error.localizedDescription)")
            }

            guard pendingWriteScopeMatchesCurrentSelection(scope) else {
                return "Post-action verification could not refresh the original target after the active scope changed."
            }
            guard let deployment = state.deployments.first(where: {
                $0.name == deploymentName && $0.namespace == scope.namespace
            }) else {
                return "Post-action verification: deployment was not found after refresh."
            }
            if deployment.desiredReplicas == 0 {
                return "Post-action verification: deployment refreshed at 0 desired replicas."
            }
            if deployment.readyReplicas >= deployment.desiredReplicas {
                return "Post-action verification: deployment is ready \(deployment.replicaText) after refresh."
            }
            return "Post-action verification: rollout still in progress \(deployment.replicaText) ready after refresh."
        default:
            return nil
        }
    }

    private func successAuditMessage(for action: PendingWriteAction, verificationMessage: String? = nil) -> String {
        let baseMessage: String
        switch action {
        case .rolloutUndo:
            if UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun {
                baseMessage = "Rollback completed after server dry-run"
            } else {
                baseMessage = "Rollback completed"
            }
        default:
            baseMessage = "Write action completed"
        }
        guard let verificationMessage else { return baseMessage }
        return "\(baseMessage). \(verificationMessage)"
    }

    private func appendWriteAudit(
        _ audit: (action: String, resource: String, contextName: String, namespace: String),
        status: String,
        message: String
    ) {
        state.appendWriteAuditEntry(
            WriteAuditEntry(
                action: audit.action,
                contextName: audit.contextName,
                namespace: audit.namespace,
                resource: audit.resource,
                status: status,
                message: message
            )
        )
    }

    private func copyCommandToPasteboard(_ command: String) {
        guard !command.isEmpty else { return }
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(command, forType: .string)
    }

    public func saveVisibleWriteAuditLog() {
        do {
            let exportStamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            let payload = try visibleWriteAuditLogExportPayload(timestamp: exportStamp)
            _ = try exporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func saveVisibleWriteAuditLogToExportFolder(openAfterSave: Bool) {
        do {
            let exportStamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            let payload = try visibleWriteAuditLogExportPayload(timestamp: exportStamp)
            _ = try configuredExporter.save(
                data: payload.data,
                suggestedName: payload.suggestedName,
                allowedFileTypes: payload.allowedFileTypes,
                kind: .plainText,
                openAfterSave: openAfterSave
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    private func visibleWriteAuditLogExportPayload(timestamp: String) throws -> LogExportPayload {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        return LogExportPayload(
            data: try encoder.encode(visibleWriteAuditEntries),
            suggestedName: "write-audit-\(timestamp).json",
            allowedFileTypes: ["json"]
        )
    }

    public func commandPaletteItems(query: String) -> [CommandPaletteItem] {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        if let cached = commandPaletteResultCache[trimmedQuery] {
            return cached
        }

        let items: [CommandPaletteItem]
        if let commandItems = commandPaletteCommandItems(query: trimmedQuery) {
            items = commandItems
        } else if trimmedQuery.isEmpty {
            items = commandPaletteGlobalItems(query: nil)
        } else {
            items = commandPaletteGlobalItems(query: trimmedQuery)
        }

        cacheCommandPaletteItems(items, for: trimmedQuery)
        return items
    }

    private func cacheCommandPaletteItems(_ items: [CommandPaletteItem], for query: String) {
        if commandPaletteResultCache[query] == nil {
            commandPaletteResultCacheOrder.append(query)
        }
        commandPaletteResultCache[query] = items

        while commandPaletteResultCacheOrder.count > Self.commandPaletteResultCacheLimit {
            let evictedQuery = commandPaletteResultCacheOrder.removeFirst()
            commandPaletteResultCache.removeValue(forKey: evictedQuery)
        }
    }

    private func invalidateCommandPaletteResultCache() {
        commandPaletteResultCache.removeAll(keepingCapacity: true)
        commandPaletteResultCacheOrder.removeAll(keepingCapacity: true)
    }

    public func executeCommandPaletteItem(_ item: CommandPaletteItem) {
        if item.action.recordsCompositeNavigationCheckpoint {
            prepareNavigationMutation(trackHistory: true)
        }

        switch item.action {
        case let .section(section):
            setSection(section)
        case let .context(context):
            setContext(context)
        case let .namespace(namespace):
            diagnostics.log(
                "commandPalette namespace action context=\(state.selectedContext?.name ?? "none") from=\(state.selectedNamespace) to=\(namespace)"
            )
            setNamespace(namespace)
        case .importKubeConfig:
            importKubeConfig()
        case .reload:
            refreshCurrentView()
        case .saveLogs:
            saveCurrentLogs()
        case .deleteSelectedResource:
            guard UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette else { return }
            requestDeleteSelectedResource()
        case let .savedWorkspace(workspace):
            openSavedWorkspace(workspace)
        case let .saveWorkspace(name):
            saveCurrentWorkspace(named: name)
        case let .toggleSavedWorkspaceFavorite(workspace):
            toggleSavedWorkspaceFavorite(workspace)
        case let .readOnly(enabled):
            setReadOnlyMode(enabled)
        case let .pod(pod):
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.pod, trackHistory: false, triggerReload: false)
            selectPod(pod, trackHistory: false)
            refreshCurrentView()
            recordNavigationCheckpoint()
        case let .deployment(deployment):
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.deployment, trackHistory: false, triggerReload: false)
            selectDeployment(deployment, trackHistory: false)
            refreshCurrentView()
            recordNavigationCheckpoint()
        case let .service(service):
            setSection(.networking, trackHistory: false, triggerReload: false)
            setWorkloadKind(.service, trackHistory: false, triggerReload: false)
            selectService(service, trackHistory: false)
            refreshCurrentView()
            recordNavigationCheckpoint()
        case let .event(event):
            setSection(.events, trackHistory: false, triggerReload: false)
            selectEvent(event, trackHistory: false)
            refreshCurrentView()
            recordNavigationCheckpoint()
        case let .helmRelease(release):
            setSection(.helm, trackHistory: false, triggerReload: true)
            selectHelmRelease(release, trackHistory: false)
            recordNavigationCheckpoint()
        case let .resourceKind(section, kind):
            setSection(section, trackHistory: false, triggerReload: false)
            setWorkloadKind(kind, trackHistory: false, triggerReload: false)
            refreshCurrentView()
            recordNavigationCheckpoint()
        case let .clusterResource(resource):
            switch resource.kind {
            case .statefulSet:
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.statefulSet, trackHistory: false, triggerReload: false)
                selectStatefulSet(resource, trackHistory: false)
            case .daemonSet:
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.daemonSet, trackHistory: false, triggerReload: false)
                selectDaemonSet(resource, trackHistory: false)
            case .job:
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.job, trackHistory: false, triggerReload: false)
                selectJob(resource, trackHistory: false)
            case .cronJob:
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.cronJob, trackHistory: false, triggerReload: false)
                selectCronJob(resource, trackHistory: false)
            case .replicaSet:
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.replicaSet, trackHistory: false, triggerReload: false)
                selectReplicaSet(resource, trackHistory: false)
            case .ingress:
                setSection(.networking, trackHistory: false, triggerReload: false)
                setWorkloadKind(.ingress, trackHistory: false, triggerReload: false)
                selectIngress(resource, trackHistory: false)
            case .endpoint:
                setSection(.networking, trackHistory: false, triggerReload: false)
                setWorkloadKind(.endpoint, trackHistory: false, triggerReload: false)
                selectEndpoint(resource, trackHistory: false)
            case .configMap:
                setSection(.config, trackHistory: false, triggerReload: false)
                setWorkloadKind(.configMap, trackHistory: false, triggerReload: false)
                selectConfigMap(resource, trackHistory: false)
            case .secret:
                setSection(.config, trackHistory: false, triggerReload: false)
                setWorkloadKind(.secret, trackHistory: false, triggerReload: false)
                selectSecret(resource, trackHistory: false)
            case .node:
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.node, trackHistory: false, triggerReload: false)
                selectNode(resource, trackHistory: false)
            case .persistentVolumeClaim:
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.persistentVolumeClaim, trackHistory: false, triggerReload: false)
                selectPersistentVolumeClaim(resource, trackHistory: false)
            case .persistentVolume:
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.persistentVolume, trackHistory: false, triggerReload: false)
                selectPersistentVolume(resource, trackHistory: false)
            case .storageClass:
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.storageClass, trackHistory: false, triggerReload: false)
                selectStorageClass(resource, trackHistory: false)
            case .horizontalPodAutoscaler:
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.horizontalPodAutoscaler, trackHistory: false, triggerReload: false)
                selectHorizontalPodAutoscaler(resource, trackHistory: false)
            case .networkPolicy:
                setSection(.networking, trackHistory: false, triggerReload: false)
                setWorkloadKind(.networkPolicy, trackHistory: false, triggerReload: false)
                selectNetworkPolicy(resource, trackHistory: false)
            case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                setSection(.rbac, trackHistory: false, triggerReload: false)
                setWorkloadKind(resource.kind, trackHistory: false, triggerReload: false)
                selectRBACResource(resource, trackHistory: false)
            default:
                break
            }
            refreshCurrentView()
            recordNavigationCheckpoint()
        }

        dismissCommandPalette()
    }

    public func executeCommandPaletteQuery(_ query: String) {
        let normalized = query.trimmingCharacters(in: .whitespacesAndNewlines)

        if let parsedCommand = commandPaletteCommandQuery(query: normalized), !parsedCommand.remainder.isEmpty {
            switch parsedCommand.command {
            case "ns", "namespace", "namespaces":
                if let exactNamespace = namespaceOptions.first(where: {
                    $0.compare(parsedCommand.remainder, options: [.caseInsensitive, .diacriticInsensitive]) == .orderedSame
                }) {
                    diagnostics.log(
                        "commandPalette query direct namespace context=\(state.selectedContext?.name ?? "none") from=\(state.selectedNamespace) query=\(parsedCommand.remainder) matched=\(exactNamespace)"
                    )
                    setNamespace(exactNamespace)
                    dismissCommandPalette()
                    return
                }
                diagnostics.log(
                    "commandPalette query manual namespace context=\(state.selectedContext?.name ?? "none") from=\(state.selectedNamespace) query=\(parsedCommand.remainder)"
                )
                setNamespace(parsedCommand.remainder)
                dismissCommandPalette()
                return
            case "ctx", "context", "contexts":
                if let exactContext = visibleContexts.first(where: {
                    $0.name.compare(parsedCommand.remainder, options: [.caseInsensitive, .diacriticInsensitive]) == .orderedSame
                }) {
                    diagnostics.log(
                        "commandPalette query direct context from=\(state.selectedContext?.name ?? "none") query=\(parsedCommand.remainder) matched=\(exactContext.name)"
                    )
                    setContext(exactContext)
                    dismissCommandPalette()
                    return
                }
            default:
                break
            }
        }

        guard let first = commandPaletteItems(query: normalized).first else { return }
        executeCommandPaletteItem(first)
    }

    private func loadResourceSnapshot(
        context: KubeContext,
        namespace: String,
        requestID: UUID,
        forceNamespaceMetadataRefresh: Bool = false,
        kubeConfigSourcesOverride: [KubeConfigSource]? = nil,
        allowsBackgroundFollowUpReads: Bool = true
    ) async throws {
        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: namespace) else {
            markOverviewCooldownBypass(contextName: context.name, namespace: namespace)
            diagnostics.log("loadResourceSnapshot ignored stale start context=\(context.name) namespace=\(namespace)")
            diagnostics.trace(
                "snapshot.stale",
                "ignored start context=<redacted-context> namespace=<redacted-namespace> request=\(requestID.uuidString)"
            )
            return
        }

        state.isLoading = true
        defer { state.isLoading = false }

        try Task.checkCancellation()

        diagnostics.trace(
            "snapshot",
            "loadResourceSnapshot start context=<redacted-context> namespace=<redacted-namespace> forceMeta=\(forceNamespaceMetadataRefresh) request=\(requestID.uuidString)"
        )
        diagnostics.log("loadResourceSnapshot start context=\(context.name) namespace=\(namespace)")

        let kubeClient = self.kubeClient
        let kubeConfigSources = kubeConfigSourcesOverride ?? state.kubeConfigSources
        let storeWasEmpty = store.namespaces(context: context).isEmpty
        var hydratedNamespacesFromDisk = false
        if UserDefaults.standard.runePersistNamespaceListCache,
           storeWasEmpty,
           let disk = namespaceListPersistence.load(contextName: context.name), !disk.isEmpty {
            store.cacheNamespaces(disk, context: context)
            state.setNamespaces(disk)
            hydratedNamespacesFromDisk = true
            diagnostics.log("namespace list hydrated from disk context=\(context.name) count=\(disk.count)")
        }

        let cachedNamespaces = store.namespaces(context: context)
        /// Order before this snapshot’s API merge (memory or disk); used to preserve ordering when the cluster list updates.
        let orderBeforeFetch = cachedNamespaces
        let cachedNodes = store.nodes(context: context)
        let cachedPersistentVolumes = store.persistentVolumes(context: context)
        let cachedStorageClasses = store.storageClasses(context: context)
        let now = Date()
        let lastNamespaceRefresh = namespaceMetadataRefreshedAt[context.name]
        let namespaceMetadataIsStale = lastNamespaceRefresh.map { now.timeIntervalSince($0) > namespaceMetadataTTL } ?? true
        let namespaceInputIsEmpty = namespace.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        let shouldRefreshNamespaceMetadata = forceNamespaceMetadataRefresh
            || namespaceMetadataIsStale
            || namespaceInputIsEmpty
            || storeWasEmpty
            || cachedNamespaces.isEmpty

        var warnings: [String] = []
        let contextDefaultNamespace: String?
        let loadedNamespaces: [String]
        if shouldRefreshNamespaceMetadata {
            async let namespaceResult: Result<[String], Error> = captureSnapshotRead {
                try await kubeClient.listNamespaces(from: kubeConfigSources, context: context)
            }
            async let contextNamespaceResult: Result<String?, Error> = captureSnapshotRead {
                try await kubeClient.contextNamespace(from: kubeConfigSources, context: context)
            }

            switch await contextNamespaceResult {
            case let .success(value):
                contextDefaultNamespace = value
            case let .failure(error):
                diagnostics.log("context namespace fallback failed context=\(context.name): \(error.localizedDescription)")
                contextDefaultNamespace = nil
            }

            switch await namespaceResult {
            case let .success(value):
                loadedNamespaces = NamespaceListOrdering.merge(previousOrder: orderBeforeFetch, apiNames: value)
                state.clearManualNamespaceMode()
                if UserDefaults.standard.runePersistNamespaceListCache {
                    namespaceListPersistence.save(names: loadedNamespaces, contextName: context.name)
                }
                namespaceMetadataRefreshedAt[context.name] = now
            case let .failure(error):
                if Self.isBenignCancellationError(error) {
                    markOverviewCooldownBypass(contextName: context.name, namespace: namespace)
                    diagnostics.log("snapshot namespaces cancelled")
                    throw CancellationError()
                }
                diagnostics.log("snapshot namespaces failed: \(error.localizedDescription)")
                state.setManualNamespaceMode(
                    true,
                    warning: "You cannot list namespaces, but you can work in a namespace manually."
                )
                // Live namespace list is source of truth. If refresh fails, clear cached namespaces for
                // this context to avoid exposing stale/deleted namespaces in toolbar and command palette.
                loadedNamespaces = []
                store.cacheNamespaces([], context: context)
                state.setNamespaces([])
                namespaceMetadataRefreshedAt.removeValue(forKey: context.name)
                if hydratedNamespacesFromDisk {
                    diagnostics.log("discarded disk-hydrated namespaces after fetch failure context=\(context.name)")
                } else {
                    diagnostics.log("cleared cached namespaces after fetch failure context=\(context.name)")
                }
            }
        } else {
            contextDefaultNamespace = nil
            loadedNamespaces = cachedNamespaces
            diagnostics.log("loadResourceSnapshot using cached namespaces context=\(context.name) count=\(loadedNamespaces.count)")
        }

        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: namespace) else {
            markOverviewCooldownBypass(contextName: context.name, namespace: namespace)
            diagnostics.log("loadResourceSnapshot discarded stale result context=\(context.name) namespace=\(namespace)")
            diagnostics.trace(
                "snapshot.stale",
                "discarded after namespace metadata context=<redacted-context> namespace=<redacted-namespace> request=\(requestID.uuidString)"
            )
            return
        }

        let trimmedIncoming = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        let savedForContext = contextPreferences.loadPreferredNamespace(for: context.name)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let trimmedContextDefault = contextDefaultNamespace?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let shouldRevalidateNamespace = pendingNamespaceRevalidationContextName == context.name
        let preferredCandidatesForResolution: [String]
        if loadedNamespaces.isEmpty {
            // No verified namespace list yet: keep explicit UI/checkpoint input, then a persisted
            // user-entered namespace for this context. Do not invent "default" without kubeconfig proof.
            let manualNamespaces = contextPreferences.loadManualNamespaces(for: context.name)
            if !trimmedIncoming.isEmpty {
                preferredCandidatesForResolution = [trimmedIncoming, savedForContext]
            } else if !savedForContext.isEmpty,
                      manualNamespaces.contains(where: { $0.caseInsensitiveCompare(savedForContext) == .orderedSame }) {
                preferredCandidatesForResolution = [savedForContext]
            } else {
                preferredCandidatesForResolution = []
            }
        } else if shouldRevalidateNamespace {
            // Fresh context switch without an explicit namespace: ignore carried/saved namespace for this one pass
            // and let context default / namespace suffix heuristics choose from the live namespace list.
            preferredCandidatesForResolution = []
        } else if !trimmedIncoming.isEmpty {
            preferredCandidatesForResolution = [trimmedIncoming, savedForContext]
        } else if !trimmedContextDefault.isEmpty {
            // On context switch with an empty incoming namespace, prefer kubeconfig's context default
            // over any older saved namespace for this context.
            preferredCandidatesForResolution = []
        } else if !savedForContext.isEmpty {
            preferredCandidatesForResolution = [savedForContext]
        } else {
            preferredCandidatesForResolution = []
        }

        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: namespace) else {
            markOverviewCooldownBypass(contextName: context.name, namespace: namespace)
            diagnostics.log("loadResourceSnapshot discarded stale result before namespace apply context=\(context.name) namespace=\(namespace)")
            diagnostics.trace(
                "snapshot.stale",
                "discarded before namespace apply context=<redacted-context> namespace=<redacted-namespace> request=\(requestID.uuidString)"
            )
            return
        }

        let effectiveNamespace = resolvedNamespace(
            contextName: context.name,
            preferredCandidates: preferredCandidatesForResolution,
            availableNamespaces: loadedNamespaces,
            contextDefaultNamespace: contextDefaultNamespace,
            preferContextSuffixOverContextDefault: shouldRevalidateNamespace
        )
        if effectiveNamespace != trimmedIncoming {
            diagnostics.log("namespace adjusted from \(trimmedIncoming) to \(effectiveNamespace) for context=\(context.name)")
        }

        if state.selectedNamespace != effectiveNamespace {
            stopAndClearTerminalSessions(
                contextName: context.name,
                namespace: state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
            )
            state.selectedNamespace = effectiveNamespace
        }
        if shouldRevalidateNamespace {
            pendingNamespaceRevalidationContextName = nil
        }
        if !effectiveNamespace.isEmpty {
            contextPreferences.savePreferredNamespace(effectiveNamespace, for: context.name)
            if loadedNamespaces.isEmpty {
                rememberManualNamespace(effectiveNamespace, for: context.name)
            }
            rememberRecentNamespace(effectiveNamespace, for: context.name)
        }

        // Namespace list is cheap compared to workload snapshots; publish it immediately so the toolbar menu is usable while pods/counts load.
        store.cacheNamespaces(loadedNamespaces, context: context)
        state.setNamespaces(loadedNamespaces)

        guard !effectiveNamespace.isEmpty else {
            state.setManualNamespaceMode(
                true,
                warning: "Choose a namespace manually. Rune could not list namespaces and kubeconfig did not provide a default namespace."
            )
            state.clearResourceDetails()
            state.setPods([])
            state.setDeployments([])
            state.setServices([])
            state.setRBACData(
                roles: [],
                serviceAccounts: [],
                roleBindings: [],
                clusterRoles: [],
                clusterRoleBindings: []
            )
            rbacDataScope = nil
            state.setEvents([])
            state.setOverviewSnapshot(
                pods: [],
                deploymentsCount: 0,
                servicesCount: 0,
                ingressesCount: 0,
                configMapsCount: 0,
                cronJobsCount: 0,
                nodesCount: cachedNodes.count,
                clusterCPUPercent: nil,
                clusterMemoryPercent: nil,
                events: []
            )
            return
        }

        let cachedSnapshot = store.snapshot(context: context, namespace: effectiveNamespace)
        let requestedRBACScope = RBACDataScope(
            kubeConfigSources: kubeConfigSources,
            context: context,
            namespace: effectiveNamespace
        )
        if rbacDataScope != requestedRBACScope {
            state.setRBACData(
                roles: [],
                serviceAccounts: cachedSnapshot.serviceAccounts,
                roleBindings: [],
                clusterRoles: [],
                clusterRoleBindings: []
            )
            rbacDataScope = requestedRBACScope
        }
        let plan = snapshotLoadPlan(
            section: state.selectedSection,
            kind: state.selectedWorkloadKind,
            simpleMode: UserDefaults.standard.runeSimpleMode
        )
        let plannedFreshnessFamilies = plan.resourceListFamilies
        state.markResourceListsRefreshing(
            plannedFreshnessFamilies,
            message: "Refreshing \(context.name) / \(effectiveNamespace)"
        )
        let shouldHydrateDeploymentsForOverview = state.selectedSection == .overview && cachedSnapshot.deployments.isEmpty
        let shouldHydrateServicesForOverview = state.selectedSection == .overview && cachedSnapshot.services.isEmpty
        try Task.checkCancellation()

        let warmOverview = await warmOverviewSnapshot(
            contextName: context.name,
            namespace: effectiveNamespace,
            reference: now,
            allowDiskCache: !forceNamespaceMetadataRefresh && plan.podStatuses
        )
        let overviewCooldownKey = Self.overviewCacheKey(contextName: context.name, namespace: effectiveNamespace)
        let bypassOverviewCooldown = bypassOverviewCooldownKeys.remove(overviewCooldownKey) != nil
        let shouldUseWarmOverviewForHeavyRequests = Self.isOverviewCacheFresh(
            warmOverview,
            ttl: overviewHeavyRequestCooldownTTL,
            reference: now
        )
            && warmOverview?.hasCoreData == true
            && !forceNamespaceMetadataRefresh
            && !bypassOverviewCooldown
        try Task.checkCancellation()

        let canReuseRBACFallback = rbacDataScope == requestedRBACScope
        let preservedRBACRoles = canReuseRBACFallback ? state.rbacRoles : []
        let preservedServiceAccounts = canReuseRBACFallback ? state.serviceAccounts : cachedSnapshot.serviceAccounts
        let preservedRBACRoleBindings = canReuseRBACFallback ? state.rbacRoleBindings : []
        let preservedRBACClusterRoles = canReuseRBACFallback ? state.rbacClusterRoles : []
        let preservedRBACClusterRoleBindings = canReuseRBACFallback ? state.rbacClusterRoleBindings : []
        let currentOverviewClusterCPUPercent = state.overviewClusterCPUPercent
        let currentOverviewClusterMemoryPercent = state.overviewClusterMemoryPercent
        let shouldRefreshClusterUsageInline = plan.podStatuses

        async let clusterUsageResult: (cpuPercent: Int?, memoryPercent: Int?) = {
            if shouldRefreshClusterUsageInline {
                if shouldUseWarmOverviewForHeavyRequests,
                   let warmOverview,
                   warmOverview.clusterCPUPercent != nil || warmOverview.clusterMemoryPercent != nil {
                    return (warmOverview.clusterCPUPercent, warmOverview.clusterMemoryPercent)
                }
                return (try? await snapshotRefreshConcurrencyLimiter.withPermit {
                    await kubeClient.clusterUsagePercent(from: kubeConfigSources, context: context)
                }) ?? (nil, nil)
            }
            return (currentOverviewClusterCPUPercent, currentOverviewClusterMemoryPercent)
        }()

        async let podResult: Result<[PodSummary], Error> = captureSnapshotRead {
            if plan.pods {
                return try await kubeClient.listPods(
                    from: kubeConfigSources,
                    context: context,
                    namespace: effectiveNamespace
                )
            }
            if plan.podStatuses {
                // Non-empty `ResourceStore` pod list overrides warm overview cache (avoids empty overview after section changes).
                if !cachedSnapshot.pods.isEmpty {
                    return cachedSnapshot.pods
                }
                if let warmOverview {
                    if shouldUseWarmOverviewForHeavyRequests {
                        return warmOverview.pods
                    }
                    // Empty warm pod rows can come from partial (non-overview) snapshots; fetch live pod status in that case.
                    if !warmOverview.pods.isEmpty {
                        return warmOverview.pods
                    }
                }
                return try await kubeClient.listPodStatuses(
                    from: kubeConfigSources,
                    context: context,
                    namespace: effectiveNamespace
                )
            }
            return cachedSnapshot.pods
        }
        async let deploymentResult: Result<[DeploymentSummary], Error> = captureSnapshotRead {
            guard plan.deployments || shouldHydrateDeploymentsForOverview else { return cachedSnapshot.deployments }
            return try await kubeClient.listDeployments(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let deploymentCountResult: Result<Int, Error> = captureSnapshotRead {
            guard plan.deploymentCount, !shouldHydrateDeploymentsForOverview else {
                return cachedSnapshot.deployments.count
            }
            if !cachedSnapshot.deployments.isEmpty {
                return cachedSnapshot.deployments.count
            }
            let warm = warmOverview?.deploymentsCount
            if shouldUseWarmOverviewForHeavyRequests, let warm {
                return warm
            }
            do {
                return try await kubeClient.countNamespacedResources(
                    from: kubeConfigSources,
                    context: context,
                    namespace: effectiveNamespace,
                    resource: "deployments"
                )
            } catch {
                if let warm {
                    return warm
                }
                throw error
            }
        }
        async let statefulSetResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.statefulSets else { return cachedSnapshot.statefulSets }
            return try await kubeClient.listStatefulSets(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let daemonSetResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.daemonSets else { return cachedSnapshot.daemonSets }
            return try await kubeClient.listDaemonSets(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let jobResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.jobs else { return cachedSnapshot.jobs }
            return try await kubeClient.listJobs(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let cronJobResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.cronJobs else { return cachedSnapshot.cronJobs }
            return try await kubeClient.listCronJobs(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let replicaSetResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.replicaSets else { return cachedSnapshot.replicaSets }
            return try await kubeClient.listReplicaSets(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let pvcResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.persistentVolumeClaims else { return cachedSnapshot.persistentVolumeClaims }
            return try await kubeClient.listPersistentVolumeClaims(
                from: kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let pvResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.persistentVolumes else { return cachedPersistentVolumes }
            return try await kubeClient.listPersistentVolumes(from: kubeConfigSources, context: context)
        }
        async let storageClassResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.storageClasses else { return cachedStorageClasses }
            return try await kubeClient.listStorageClasses(from: kubeConfigSources, context: context)
        }
        async let hpaResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.horizontalPodAutoscalers else { return cachedSnapshot.horizontalPodAutoscalers }
            return try await kubeClient.listHorizontalPodAutoscalers(
                from: kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let networkPolicyResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.networkPolicies else { return cachedSnapshot.networkPolicies }
            return try await kubeClient.listNetworkPolicies(
                from: kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let serviceResult: Result<[ServiceSummary], Error> = captureSnapshotRead {
            guard plan.services || shouldHydrateServicesForOverview else { return cachedSnapshot.services }
            return try await kubeClient.listServices(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let endpointResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.endpoints else { return cachedSnapshot.endpoints }
            return try await kubeClient.listEndpoints(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let serviceCountResult: Result<Int, Error> = captureSnapshotRead {
            guard plan.servicesCount, !shouldHydrateServicesForOverview else {
                return cachedSnapshot.services.count
            }
            if !cachedSnapshot.services.isEmpty {
                return cachedSnapshot.services.count
            }
            let warm = warmOverview?.servicesCount
            if shouldUseWarmOverviewForHeavyRequests, let warm {
                return warm
            }
            do {
                return try await kubeClient.countNamespacedResources(
                    from: kubeConfigSources,
                    context: context,
                    namespace: effectiveNamespace,
                    resource: "services"
                )
            } catch {
                if let warm {
                    return warm
                }
                throw error
            }
        }
        async let ingressResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.ingresses else { return cachedSnapshot.ingresses }
            return try await kubeClient.listIngresses(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let ingressCountResult: Result<Int, Error> = captureSnapshotRead {
            guard plan.ingressesCount else { return cachedSnapshot.ingresses.count }
            if !cachedSnapshot.ingresses.isEmpty {
                return cachedSnapshot.ingresses.count
            }
            let warm = warmOverview?.ingressesCount
            if shouldUseWarmOverviewForHeavyRequests, let warm {
                return warm
            }
            do {
                return try await kubeClient.countNamespacedResources(
                    from: kubeConfigSources,
                    context: context,
                    namespace: effectiveNamespace,
                    resource: "ingresses"
                )
            } catch {
                if let warm {
                    return warm
                }
                throw error
            }
        }
        async let configMapResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.configMaps else { return cachedSnapshot.configMaps }
            return try await kubeClient.listConfigMaps(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let configMapCountResult: Result<Int, Error> = captureSnapshotRead {
            guard plan.configMapsCount else { return cachedSnapshot.configMaps.count }
            if !cachedSnapshot.configMaps.isEmpty {
                return cachedSnapshot.configMaps.count
            }
            let warm = warmOverview?.configMapsCount
            if shouldUseWarmOverviewForHeavyRequests, let warm {
                return warm
            }
            do {
                return try await kubeClient.countNamespacedResources(
                    from: kubeConfigSources,
                    context: context,
                    namespace: effectiveNamespace,
                    resource: "configmaps"
                )
            } catch {
                if let warm {
                    return warm
                }
                throw error
            }
        }
        async let cronJobsCountResult: Result<Int, Error> = captureSnapshotRead {
            guard plan.cronJobsCount else { return cachedSnapshot.cronJobs.count }
            if !cachedSnapshot.cronJobs.isEmpty {
                return cachedSnapshot.cronJobs.count
            }
            let warm = warmOverview?.cronJobsCount
            if shouldUseWarmOverviewForHeavyRequests, let warm {
                return warm
            }
            do {
                return try await kubeClient.countNamespacedResources(
                    from: kubeConfigSources,
                    context: context,
                    namespace: effectiveNamespace,
                    resource: "cronjobs"
                )
            } catch {
                if let warm {
                    return warm
                }
                throw error
            }
        }
        async let secretResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.secrets else { return cachedSnapshot.secrets }
            return try await kubeClient.listSecrets(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let nodeResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.nodes else { return cachedNodes }
            return try await kubeClient.listNodes(from: kubeConfigSources, context: context)
        }
        async let nodeCountResult: Result<Int, Error> = captureSnapshotRead {
            guard plan.nodesCount else { return cachedNodes.count }
            if !cachedNodes.isEmpty {
                return cachedNodes.count
            }
            let warm = warmOverview?.nodesCount
            if shouldUseWarmOverviewForHeavyRequests, let warm {
                return warm
            }
            do {
                return try await kubeClient.countClusterResources(
                    from: kubeConfigSources,
                    context: context,
                    resource: "nodes"
                )
            } catch {
                if let warm {
                    return warm
                }
                throw error
            }
        }
        async let eventResult: Result<[EventSummary], Error> = captureSnapshotRead {
            guard plan.events else { return cachedSnapshot.events }
            if !cachedSnapshot.events.isEmpty {
                return cachedSnapshot.events
            }
            if let warmOverview {
                return warmOverview.events
            }
            return try await kubeClient.listEvents(from: kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let rbacRolesResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.rbacRoles else { return preservedRBACRoles }
            return try await kubeClient.listRoles(
                from: kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let serviceAccountResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.serviceAccounts else { return preservedServiceAccounts }
            return try await kubeClient.listServiceAccounts(
                from: kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let rbacRoleBindingsResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.rbacRoleBindings else { return preservedRBACRoleBindings }
            return try await kubeClient.listRoleBindings(
                from: kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let rbacClusterRolesResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.rbacClusterRoles else { return preservedRBACClusterRoles }
            return try await kubeClient.listClusterRoles(from: kubeConfigSources, context: context)
        }
        async let rbacClusterRoleBindingsResult: Result<[ClusterResourceSummary], Error> = captureSnapshotRead {
            guard plan.rbacClusterRoleBindings else { return preservedRBACClusterRoleBindings }
            return try await kubeClient.listClusterRoleBindings(from: kubeConfigSources, context: context)
        }

        let loadedPods = unwrap(await podResult, label: "pods", fallback: cachedSnapshot.pods, warnings: &warnings)
        let loadedDeployments = unwrap(await deploymentResult, label: "deployments", fallback: cachedSnapshot.deployments, warnings: &warnings)
        let loadedStatefulSets = unwrap(await statefulSetResult, label: "statefulsets", fallback: cachedSnapshot.statefulSets, warnings: &warnings)
        let loadedDaemonSets = unwrap(await daemonSetResult, label: "daemonsets", fallback: cachedSnapshot.daemonSets, warnings: &warnings)
        let loadedJobs = unwrap(await jobResult, label: "jobs", fallback: cachedSnapshot.jobs, warnings: &warnings)
        let loadedCronJobs = unwrap(await cronJobResult, label: "cronjobs", fallback: cachedSnapshot.cronJobs, warnings: &warnings)
        let loadedReplicaSets = unwrap(await replicaSetResult, label: "replicasets", fallback: cachedSnapshot.replicaSets, warnings: &warnings)
        let loadedPVCs = unwrap(await pvcResult, label: "pvcs", fallback: cachedSnapshot.persistentVolumeClaims, warnings: &warnings)
        let loadedPVs = unwrap(await pvResult, label: "pvs", fallback: cachedPersistentVolumes, warnings: &warnings)
        let loadedStorageClasses = unwrap(await storageClassResult, label: "storageclasses", fallback: cachedStorageClasses, warnings: &warnings)
        let loadedHPAs = unwrap(await hpaResult, label: "hpas", fallback: cachedSnapshot.horizontalPodAutoscalers, warnings: &warnings)
        let loadedNetworkPolicies = unwrap(
            await networkPolicyResult,
            label: "networkpolicies",
            fallback: cachedSnapshot.networkPolicies,
            warnings: &warnings
        )
        let loadedServices = unwrap(await serviceResult, label: "services", fallback: cachedSnapshot.services, warnings: &warnings)
        let loadedEndpoints = unwrap(await endpointResult, label: "endpoints", fallback: cachedSnapshot.endpoints, warnings: &warnings)
        let loadedIngresses = unwrap(await ingressResult, label: "ingresses", fallback: cachedSnapshot.ingresses, warnings: &warnings)
        let loadedConfigMaps = unwrap(await configMapResult, label: "configmaps", fallback: cachedSnapshot.configMaps, warnings: &warnings)
        let loadedSecrets = unwrap(await secretResult, label: "secrets", fallback: cachedSnapshot.secrets, warnings: &warnings)
        let loadedNodes = unwrap(await nodeResult, label: "nodes", fallback: cachedNodes, warnings: &warnings)
        let loadedEvents = unwrap(await eventResult, label: "events", fallback: cachedSnapshot.events, warnings: &warnings)
        let loadedDeploymentCount = (plan.deploymentCount && !shouldHydrateDeploymentsForOverview)
            ? unwrap(await deploymentCountResult, label: "deployments-count", fallback: loadedDeployments.count, warnings: &warnings)
            : loadedDeployments.count
        let loadedServiceCount = (plan.servicesCount && !shouldHydrateServicesForOverview)
            ? unwrap(await serviceCountResult, label: "services-count", fallback: loadedServices.count, warnings: &warnings)
            : loadedServices.count
        let loadedIngressCount = plan.ingressesCount
            ? unwrap(await ingressCountResult, label: "ingresses-count", fallback: loadedIngresses.count, warnings: &warnings)
            : loadedIngresses.count
        let loadedConfigMapCount = plan.configMapsCount
            ? unwrap(await configMapCountResult, label: "configmaps-count", fallback: loadedConfigMaps.count, warnings: &warnings)
            : loadedConfigMaps.count
        let loadedCronJobsCount = plan.cronJobsCount
            ? unwrap(await cronJobsCountResult, label: "cronjobs-count", fallback: loadedCronJobs.count, warnings: &warnings)
            : loadedCronJobs.count
        let loadedNodeCount = plan.nodesCount
            ? unwrap(await nodeCountResult, label: "nodes-count", fallback: loadedNodes.count, warnings: &warnings)
            : loadedNodes.count
        let loadedClusterUsage = await clusterUsageResult
        let loadedClusterCPUPercent = loadedClusterUsage.cpuPercent
        let loadedClusterMemoryPercent = loadedClusterUsage.memoryPercent
        if shouldRefreshClusterUsageInline,
           loadedClusterCPUPercent == nil,
           loadedClusterMemoryPercent == nil {
            diagnostics.log("cluster usage unavailable context=\(context.name)")
        }

        let loadedRBACRoles = unwrap(await rbacRolesResult, label: "roles", fallback: preservedRBACRoles, warnings: &warnings)
        let loadedServiceAccounts = unwrap(
            await serviceAccountResult,
            label: "serviceaccounts",
            fallback: preservedServiceAccounts,
            warnings: &warnings
        )
        let loadedRBACRoleBindings = unwrap(
            await rbacRoleBindingsResult,
            label: "rolebindings",
            fallback: preservedRBACRoleBindings,
            warnings: &warnings
        )
        let loadedRBACClusterRoles = unwrap(
            await rbacClusterRolesResult,
            label: "clusterroles",
            fallback: preservedRBACClusterRoles,
            warnings: &warnings
        )
        let loadedRBACClusterRoleBindings = unwrap(
            await rbacClusterRoleBindingsResult,
            label: "clusterrolebindings",
            fallback: preservedRBACClusterRoleBindings,
            warnings: &warnings
        )

        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: effectiveNamespace) else {
            markOverviewCooldownBypass(contextName: context.name, namespace: effectiveNamespace)
            diagnostics.log("loadResourceSnapshot discarded stale resource result context=\(context.name) namespace=\(effectiveNamespace)")
            diagnostics.trace(
                "snapshot.stale",
                "discarded after core resource fetch context=<redacted-context> effectiveNamespace=<redacted-namespace> request=\(requestID.uuidString) selectedNamespace=<redacted-namespace>"
            )
            return
        }

        store.cacheNodes(loadedNodes, context: context)
        store.cachePersistentVolumes(loadedPVs, context: context)
        store.cacheStorageClasses(loadedStorageClasses, context: context)
        store.cacheSnapshot(
            context: context,
            namespace: effectiveNamespace,
            pods: loadedPods,
            deployments: loadedDeployments,
            statefulSets: loadedStatefulSets,
            daemonSets: loadedDaemonSets,
            jobs: loadedJobs,
            cronJobs: loadedCronJobs,
            replicaSets: loadedReplicaSets,
            persistentVolumeClaims: loadedPVCs,
            horizontalPodAutoscalers: loadedHPAs,
            networkPolicies: loadedNetworkPolicies,
            services: loadedServices,
            endpoints: loadedEndpoints,
            ingresses: loadedIngresses,
            configMaps: loadedConfigMaps,
            secrets: loadedSecrets,
            serviceAccounts: loadedServiceAccounts,
            events: loadedEvents
        )

        state.setPods(loadedPods)
        if plan.pods, Self.podListNeedsJSONEnrichment(loadedPods) {
            Task { [weak self] in
                await self?.applyPodsJSONEnrichmentIfCurrent(
                    requestID: requestID,
                    context: context,
                    namespace: effectiveNamespace,
                    basePods: loadedPods,
                    kubeConfigSources: kubeConfigSources
                )
            }
        }
        state.setDeployments(loadedDeployments)
        state.setStatefulSets(loadedStatefulSets)
        state.setDaemonSets(loadedDaemonSets)
        state.setJobs(loadedJobs)
        state.setCronJobs(loadedCronJobs)
        state.setReplicaSets(loadedReplicaSets)
        state.setPersistentVolumeClaims(loadedPVCs)
        state.setPersistentVolumes(loadedPVs)
        state.setStorageClasses(loadedStorageClasses)
        state.setHorizontalPodAutoscalers(loadedHPAs)
        state.setNetworkPolicies(loadedNetworkPolicies)
        state.setServices(loadedServices)
        state.setEndpoints(loadedEndpoints)
        state.setIngresses(loadedIngresses)
        state.setConfigMaps(loadedConfigMaps)
        state.setSecrets(loadedSecrets)
        state.setNodes(loadedNodes)
        state.setEvents(loadedEvents)
        if plan.serviceAccounts || plan.rbacRoles || plan.rbacRoleBindings || plan.rbacClusterRoles || plan.rbacClusterRoleBindings {
            state.setRBACData(
                roles: loadedRBACRoles,
                serviceAccounts: loadedServiceAccounts,
                roleBindings: loadedRBACRoleBindings,
                clusterRoles: loadedRBACClusterRoles,
                clusterRoleBindings: loadedRBACClusterRoleBindings
            )
            rbacDataScope = requestedRBACScope
        }

        if let deployment = state.selectedDeployment {
            scaleReplicaInput = max(0, deployment.desiredReplicas)
        }

        if warnings.isEmpty {
            state.markResourceListsLive(
                plannedFreshnessFamilies,
                message: "Live for \(context.name) / \(effectiveNamespace)"
            )
            state.clearError()
        } else {
            let warningText = warnings.joined(separator: " | ")
            let failedFamilies = Self.resourceListFamilies(forSnapshotWarnings: warnings)
            state.markResourceListsLive(
                plannedFreshnessFamilies.subtracting(failedFamilies),
                message: "Live for \(context.name) / \(effectiveNamespace)"
            )
            state.markResourceListsFailed(failedFamilies, message: "Partial load: \(warningText)")
            state.setErrorMessage("Partial load: \(warningText)")
            state.setSnapshotFreshness(
                RuneSnapshotFreshness(
                    status: .stale,
                    updatedAt: state.snapshotFreshness.updatedAt,
                    message: "Partial load: \(warningText)"
                )
            )
            diagnostics.log("loadResourceSnapshot partial warnings: \(warningText)")
            diagnostics.trace(
                "snapshot",
                "partial load warningCount=\(warnings.count) failedFamilyCount=\(failedFamilies.count)"
            )
        }

        if plan.podStatuses {
            loadOverviewSnapshot(
                context: context,
                namespace: effectiveNamespace,
                requestID: requestID,
                pods: loadedPods,
                deploymentsCount: loadedDeploymentCount,
                servicesCount: loadedServiceCount,
                ingressesCount: loadedIngressCount,
                configMapsCount: loadedConfigMapCount,
                cronJobsCount: loadedCronJobsCount,
                nodesCount: loadedNodeCount,
                clusterCPUPercent: loadedClusterCPUPercent,
                clusterMemoryPercent: loadedClusterMemoryPercent,
                events: loadedEvents
            )
            updateOverviewCache(
                contextName: context.name,
                namespace: effectiveNamespace,
                pods: loadedPods,
                deploymentsCount: loadedDeploymentCount,
                servicesCount: loadedServiceCount,
                ingressesCount: loadedIngressCount,
                configMapsCount: loadedConfigMapCount,
                cronJobsCount: loadedCronJobsCount,
                nodesCount: loadedNodeCount,
                clusterCPUPercent: loadedClusterCPUPercent,
                clusterMemoryPercent: loadedClusterMemoryPercent,
                events: loadedEvents
            )
            if allowsBackgroundFollowUpReads, !loadedNamespaces.isEmpty {
                scheduleOverviewPrefetch(
                    context: context,
                    namespaces: loadedNamespaces,
                    currentNamespace: effectiveNamespace
                )
            }
        } else {
            refreshClusterUsageForHeaderIfNeeded(
                context: context,
                namespace: effectiveNamespace,
                requestID: requestID,
                kubeConfigSources: kubeConfigSources
            )
            diagnostics.trace(
                "snapshot.overview",
                "skipped overview write section=\(state.selectedSection.rawValue) context=<redacted-context> namespace=<redacted-namespace>"
            )
        }

        // After primary snapshot work, optionally warm a few non-selected contexts so sidebar/context
        // switching can reuse overview cache immediately.
        if allowsBackgroundFollowUpReads, plan.podStatuses || forceNamespaceMetadataRefresh {
            scheduleContextOverviewPrefetch(currentContext: context)
        }

        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: effectiveNamespace) else {
            diagnostics.log("loadResourceSnapshot skipped details for stale context=\(context.name) namespace=\(namespace)")
            diagnostics.trace(
                "snapshot.stale",
                "skipped resource details context=<redacted-context> namespace=<redacted-namespace> request=\(requestID.uuidString)"
            )
            return
        }

        var handledPendingEventSource = false
        if allowsBackgroundFollowUpReads, let pending = pendingOpenEventSource {
            pendingOpenEventSource = nil
            if pending.matches(contextName: state.selectedContext?.name, namespace: effectiveNamespace) {
                handledPendingEventSource = true
                navigateToEventSource(pending.event)
            } else {
                navigateFromEventFetchAttempts = 0
            }
        }

        if handledPendingEventSource {
            diagnostics.log("loadResourceSnapshot delegated details to event source selection")
        } else if allowsBackgroundFollowUpReads, shouldLoadResourceDetailsForCurrentSection {
            let requestID = UUID()
            latestResourceDetailsRequestID = requestID
            state.beginResourceDetailLoad(scope: currentResourceDetailScope())
            await loadResourceDetailsForCurrentSelectionAsync(requestID: requestID)
        } else {
            diagnostics.log("loadResourceSnapshot skipped heavy resource details for section=\(state.selectedSection.rawValue)")
        }

        diagnostics.log("loadResourceSnapshot done context=\(context.name) namespace=\(namespace)")
        diagnostics.trace("snapshot", "loadResourceSnapshot done context=<redacted-context> namespace=<redacted-namespace>")
    }

    nonisolated static func podListNeedsJSONEnrichment(_ pods: [PodSummary]) -> Bool {
        guard !pods.isEmpty else { return false }
        return pods.contains { pod in
            pod.podIP == nil
                && pod.hostIP == nil
                && pod.nodeName == nil
                && pod.qosClass == nil
                && pod.containersReady == nil
                && pod.containerNamesLine == nil
                && pod.labels.isEmpty
                && pod.containerImagesLine == nil
                && pod.ownerReferencesLine == nil
        }
    }

    /// Second snapshot pass: merge full pod JSON so the inspector shows IP, node, QoS, and readiness while keeping CPU/mem from the first pass.
    private func applyPodsJSONEnrichmentIfCurrent(
        requestID: UUID,
        context: KubeContext,
        namespace: String,
        basePods: [PodSummary],
        kubeConfigSources: [KubeConfigSource]
    ) async {
        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: namespace) else { return }
        guard state.selectedNamespace == namespace else { return }
        do {
            let merged = try await kubeClient.enrichPodsWithJSONList(
                from: kubeConfigSources,
                context: context,
                namespace: namespace,
                merging: basePods
            )
            guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: namespace) else { return }
            guard state.selectedNamespace == namespace else { return }
            state.setPods(merged)
            let snap = store.snapshot(context: context, namespace: namespace)
            store.cacheSnapshot(
                context: context,
                namespace: namespace,
                pods: merged,
                deployments: snap.deployments,
                statefulSets: snap.statefulSets,
                daemonSets: snap.daemonSets,
                jobs: snap.jobs,
                cronJobs: snap.cronJobs,
                replicaSets: snap.replicaSets,
                persistentVolumeClaims: snap.persistentVolumeClaims,
                horizontalPodAutoscalers: snap.horizontalPodAutoscalers,
                networkPolicies: snap.networkPolicies,
                services: snap.services,
                endpoints: snap.endpoints,
                ingresses: snap.ingresses,
                configMaps: snap.configMaps,
                secrets: snap.secrets,
                serviceAccounts: snap.serviceAccounts,
                events: snap.events
            )
            state.setOverviewSnapshot(
                pods: merged,
                deploymentsCount: state.overviewDeploymentsCount,
                servicesCount: state.overviewServicesCount,
                ingressesCount: state.overviewIngressesCount,
                configMapsCount: state.overviewConfigMapsCount,
                cronJobsCount: state.overviewCronJobsCount,
                nodesCount: state.overviewNodesCount,
                clusterCPUPercent: state.overviewClusterCPUPercent,
                clusterMemoryPercent: state.overviewClusterMemoryPercent,
                events: state.overviewEvents
            )
            updateOverviewCache(
                contextName: context.name,
                namespace: namespace,
                pods: merged,
                deploymentsCount: state.overviewDeploymentsCount,
                servicesCount: state.overviewServicesCount,
                ingressesCount: state.overviewIngressesCount,
                configMapsCount: state.overviewConfigMapsCount,
                cronJobsCount: state.overviewCronJobsCount,
                nodesCount: state.overviewNodesCount,
                clusterCPUPercent: state.overviewClusterCPUPercent,
                clusterMemoryPercent: state.overviewClusterMemoryPercent,
                events: state.overviewEvents
            )
            diagnostics.log("pod list JSON enrichment applied context=\(context.name) namespace=\(namespace)")
        } catch {
            diagnostics.log("pod list JSON enrichment failed: \(error.localizedDescription)")
        }
    }

    private func loadOverviewSnapshot(
        context: KubeContext,
        namespace: String,
        requestID: UUID,
        pods: [PodSummary],
        deploymentsCount: Int,
        servicesCount: Int,
        ingressesCount: Int,
        configMapsCount: Int,
        cronJobsCount: Int,
        nodesCount: Int,
        clusterCPUPercent: Int?,
        clusterMemoryPercent: Int?,
        events: [EventSummary]
    ) {
        diagnostics.log("loadOverviewSnapshot start context=\(context.name) namespace=\(namespace)")
        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: namespace) else {
            diagnostics.log("loadOverviewSnapshot discarded stale result context=\(context.name)")
            diagnostics.trace(
                "snapshot.stale",
                "loadOverviewSnapshot discarded context=<redacted-context> namespace=<redacted-namespace> request=\(requestID.uuidString)"
            )
            return
        }

        state.setOverviewSnapshot(
            pods: pods,
            deploymentsCount: deploymentsCount,
            servicesCount: servicesCount,
            ingressesCount: ingressesCount,
            configMapsCount: configMapsCount,
            cronJobsCount: cronJobsCount,
            nodesCount: nodesCount,
            clusterCPUPercent: clusterCPUPercent,
            clusterMemoryPercent: clusterMemoryPercent,
            events: events
        )
        diagnostics.log(
            "loadOverviewSnapshot done context=\(context.name) pods=\(pods.count) deployments=\(deploymentsCount) services=\(servicesCount) cronjobs=\(cronJobsCount)"
        )
        diagnostics.trace(
            "snapshot.overview",
            "applied overview tiles namespace=<redacted-namespace> context=<redacted-context> pods=\(pods.count) deployments=\(deploymentsCount) services=\(servicesCount) ingresses=\(ingressesCount) configmaps=\(configMapsCount) cronjobs=\(cronJobsCount) nodes=\(nodesCount)"
        )
    }

    private func refreshClusterUsageForHeaderIfNeeded(
        context: KubeContext,
        namespace: String,
        requestID: UUID,
        kubeConfigSources: [KubeConfigSource]
    ) {
        guard state.selectedSection != .terminal else { return }

        let reference = Date()
        let cacheKey = Self.overviewCacheKey(contextName: context.name, namespace: namespace)
        if let warmOverview = overviewSnapshotCache[cacheKey],
           Self.isOverviewCacheFresh(warmOverview, ttl: overviewHeavyRequestCooldownTTL, reference: reference),
           warmOverview.clusterCPUPercent != nil || warmOverview.clusterMemoryPercent != nil {
            state.setOverviewClusterUsage(
                cpuPercent: warmOverview.clusterCPUPercent,
                memoryPercent: warmOverview.clusterMemoryPercent
            )
            return
        }

        Task { [weak self] in
            guard let self else { return }
            let usage = await self.kubeClient.clusterUsagePercent(from: kubeConfigSources, context: context)
            guard self.snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: namespace) else { return }
            guard self.state.selectedSection != .terminal else { return }
            self.state.setOverviewClusterUsage(
                cpuPercent: usage.cpuPercent,
                memoryPercent: usage.memoryPercent
            )
            self.updateOverviewCacheClusterUsage(
                contextName: context.name,
                namespace: namespace,
                cpuPercent: usage.cpuPercent,
                memoryPercent: usage.memoryPercent
            )
            if usage.cpuPercent == nil, usage.memoryPercent == nil {
                self.diagnostics.log("cluster usage unavailable context=\(context.name)")
            }
        }
    }

    private func resolvedKubeConfigSources(fallbackURLs: [URL]) throws -> [KubeConfigSource] {
        let bookmarked: [KubeConfigSource]
        if KubeConfigDiscoverer.isIsolatedKubeconfigActive()
            || ProcessInfo.processInfo.environment["RUNE_DISABLE_BOOKMARKED_KUBECONFIGS"] == "1" {
            bookmarked = []
        } else {
            do {
                bookmarked = try bookmarkManager.loadKubeConfigSources()
            } catch {
                diagnostics.log("bookmark load failed, falling back to direct kubeconfig paths: \(error.localizedDescription)")
                bookmarked = []
            }
        }
        let fallback = fallbackURLs.map(KubeConfigSource.init(url:))

        var seen = Set<String>()
        var merged: [KubeConfigSource] = []
        for source in bookmarked + fallback {
            let standardizedPath = source.url.standardizedFileURL.path
            guard seen.insert(standardizedPath).inserted else { continue }
            merged.append(source)
        }

        return merged
    }

    private func fetchYAMLAndDescribe(
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async -> (yaml: Result<String, Error>, describe: Result<String, Error>) {
        async let yaml = captureResult {
            try await self.kubeClient.resourceYAML(
                from: self.state.kubeConfigSources,
                context: context,
                namespace: namespace,
                kind: kind,
                name: name
            )
        }
        async let describe = captureResult {
            try await self.kubeClient.resourceDescribe(
                from: self.state.kubeConfigSources,
                context: context,
                namespace: namespace,
                kind: kind,
                name: name
            )
        }
        return (await yaml, await describe)
    }

    private func loadResourceDetailsForCurrentSelection(
        preservingVisibleDocuments: Bool = false
    ) {
        resourceDetailsTask?.cancel()
        let requestID = UUID()
        latestResourceDetailsRequestID = requestID
        let scope = currentResourceDetailScope()
        let preservesVisibleDocuments = beginResourceDetailFetch(
            scope: scope,
            preservingVisibleDocuments: preservingVisibleDocuments
        )
        resourceDetailsRequestPreservingVisibleDocuments = preservesVisibleDocuments ? requestID : nil
        diagnostics.log("resourceDetails start request=\(requestID.uuidString) section=\(state.selectedSection.rawValue) kind=\(state.selectedWorkloadKind.rawValue) namespace=\(state.selectedNamespace)")

        resourceDetailsTask = Task { [weak self] in
            guard let self else { return }
            await self.loadResourceDetailsForCurrentSelectionAsync(requestID: requestID)
            if self.resourceDetailsRequestPreservingVisibleDocuments == requestID {
                self.resourceDetailsRequestPreservingVisibleDocuments = nil
            }
            if self.isCurrentResourceDetailsRequest(requestID) {
                self.resourceDetailsTask = nil
            }
        }
    }

    private func loadResourceDetailsForCurrentSelectionIfNeeded() {
        guard shouldLoadResourceDetailsForCurrentSection else {
            state.clearResourceDetails()
            return
        }
        guard hasCurrentResourceSelectionForDetails else {
            state.clearResourceDetails()
            return
        }
        loadResourceDetailsForCurrentSelection()
    }

    private var hasCurrentResourceSelectionForDetails: Bool {
        switch state.selectedWorkloadKind {
        case .pod:
            return state.selectedPod != nil
        case .deployment:
            return state.selectedDeployment != nil
        case .service:
            return state.selectedService != nil
        case .statefulSet:
            return state.selectedStatefulSet != nil
        case .daemonSet:
            return state.selectedDaemonSet != nil
        case .job:
            return state.selectedJob != nil
        case .cronJob:
            return state.selectedCronJob != nil
        case .replicaSet:
            return state.selectedReplicaSet != nil
        case .persistentVolumeClaim:
            return state.selectedPersistentVolumeClaim != nil
        case .persistentVolume:
            return state.selectedPersistentVolume != nil
        case .storageClass:
            return state.selectedStorageClass != nil
        case .horizontalPodAutoscaler:
            return state.selectedHorizontalPodAutoscaler != nil
        case .networkPolicy:
            return state.selectedNetworkPolicy != nil
        case .endpoint:
            return state.selectedEndpoint != nil
        case .ingress:
            return state.selectedIngress != nil
        case .configMap:
            return state.selectedConfigMap != nil
        case .secret:
            return state.selectedSecret != nil
        case .node:
            return state.selectedNode != nil
        case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            return state.selectedRBACResource != nil
        case .event:
            return false
        }
    }

    private func captureResult<T>(
        _ operation: @escaping () async throws -> T
    ) async -> Result<T, Error> {
        do {
            return .success(try await operation())
        } catch {
            return .failure(error)
        }
    }

    private func normalizeLoadedResourceText(_ text: String) -> String {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "" : text
    }

    private func resourceDetailsFailureMessage(
        action: String,
        kind: KubeResourceKind,
        name: String,
        error: Error
    ) -> String {
        "Unable to \(action) \(kind.singularTypeName) \(name).\n\n\(error.localizedDescription)"
    }

    private func resourceDetailsFailureMessage(
        action: String,
        kind: String,
        name: String,
        error: Error
    ) -> String {
        "Unable to \(action) \(kind) \(name).\n\n\(error.localizedDescription)"
    }

    private func isCurrentResourceDetailsRequest(_ requestID: UUID) -> Bool {
        latestResourceDetailsRequestID == requestID
    }

    private func cancelPendingLogReload() {
        scheduledLogsReloadTask?.cancel()
        scheduledLogsReloadTask = nil
        logsReloadTask?.cancel()
        tailLogsReloadTask?.cancel()
        tailLogsReloadTask = nil
        latestLogsReloadRequestID = UUID()
    }

    private func commitPodLogFetch(_ logs: String, contextName: String, namespace: String, podName: String) {
        if isLogTailModeEnabled {
            state.appendPodLogRead(logs, contextName: contextName, namespace: namespace, podName: podName)
        } else {
            state.replacePodLogRead(logs, contextName: contextName, namespace: namespace, podName: podName)
        }
    }

    private func commitUnifiedLogFetch(
        _ mergedText: String,
        pods: [String],
        contextName: String,
        namespace: String,
        kind: KubeResourceKind,
        resourceName: String
    ) {
        if isLogTailModeEnabled {
            state.appendUnifiedServiceLogRead(
                mergedText,
                pods: pods,
                contextName: contextName,
                namespace: namespace,
                kind: kind,
                resourceName: resourceName
            )
        } else {
            state.replaceUnifiedServiceLogRead(
                mergedText,
                pods: pods,
                contextName: contextName,
                namespace: namespace,
                kind: kind,
                resourceName: resourceName
            )
        }
    }

    private func scheduleNextTailLogsReload() {
        guard isLogTailModeEnabled, !isLogStreamPaused else { return }
        tailLogsReloadTask?.cancel()
        tailLogsReloadTask = Task { [weak self] in
            guard let self else { return }
            do {
                try await Task.sleep(nanoseconds: self.tailLogsReloadNanoseconds)
            } catch {
                return
            }
            guard !Task.isCancelled else { return }
            self.startLogsReloadForSelection()
        }
    }

    private func isCurrentLogsReloadRequest(_ requestID: UUID) -> Bool {
        latestLogsReloadRequestID == requestID
    }

    private func applyResourceManifestResults(
        _ pair: (yaml: Result<String, Error>, describe: Result<String, Error>),
        kind: KubeResourceKind,
        name: String,
        requestID: UUID
    ) {
        guard isCurrentResourceDetailsRequest(requestID) else { return }

        switch pair.yaml {
        case let .success(yaml):
            state.setResourceYAML(normalizeLoadedResourceText(yaml))
        case let .failure(error):
            let message = resourceDetailsFailureMessage(action: "load YAML for", kind: kind, name: name, error: error)
            if resourceDetailsRequestPreservingVisibleDocuments == requestID {
                state.setResourceYAMLRefreshError(message)
            } else {
                state.setResourceYAMLError(message)
            }
        }

        switch pair.describe {
        case let .success(describe):
            state.setResourceDescribe(normalizeLoadedResourceText(describe))
        case let .failure(error):
            let message = resourceDetailsFailureMessage(action: "load describe for", kind: kind, name: name, error: error)
            if resourceDetailsRequestPreservingVisibleDocuments == requestID {
                state.setResourceDescribeRefreshError(message)
            } else {
                state.setResourceDescribeError(message)
            }
        }

        let yamlSummary: String = {
            switch pair.yaml {
            case let .success(yaml):
                return "ok chars=\(yaml.count)"
            case let .failure(error):
                return "error=\(error.localizedDescription)"
            }
        }()

        let describeSummary: String = {
            switch pair.describe {
            case let .success(describe):
                return "ok chars=\(describe.count)"
            case let .failure(error):
                return "error=\(error.localizedDescription)"
            }
        }()

        diagnostics.log(
            "resourceDetails manifest request=\(requestID.uuidString) kind=\(kind.rawValue) name=\(name) yaml=\(yamlSummary) describe=\(describeSummary)"
        )
    }

    private func loadResourceDetailsForCurrentSelectionAsync(requestID: UUID) async {
        defer {
            if isCurrentResourceDetailsRequest(requestID) {
                state.finishResourceDetailLoad()
            }
        }

        if Task.isCancelled {
            return
        }

        diagnostics.trace(
            "resourceDetails",
            "async begin request=\(requestID.uuidString) kind=\(state.selectedWorkloadKind.rawValue) section=\(state.selectedSection.rawValue) namespace=<redacted-namespace>"
        )

        guard let context = state.selectedContext else {
            if isCurrentResourceDetailsRequest(requestID) {
                state.clearResourceDetails()
            }
            return
        }

        if context.name == demoContextName {
            applyDemoResourceDetailsForCurrentSelection()
            return
        }

        switch state.selectedWorkloadKind {
        case .pod:
                guard let pod = state.selectedPod else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                state.showCachedPodLogs(contextName: context.name, namespace: state.selectedNamespace, podName: pod.name)
                state.isLoadingLogs = true
                defer {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.isLoadingLogs = false
                    }
                }

                async let inspectorPod = captureResult {
                    try await self.kubeClient.fetchPodSummaryForInspector(
                        from: self.state.kubeConfigSources,
                        context: context,
                        namespace: self.state.selectedNamespace,
                        podName: pod.name
                    )
                }
                let selectedContainer = selectedLogContainerName
                async let logsResult = captureResult {
                    if let selectedContainer {
                        return try await self.kubeClient.podLogs(
                            from: self.state.kubeConfigSources,
                            context: context,
                            namespace: self.state.selectedNamespace,
                            podName: pod.name,
                            container: selectedContainer,
                            filter: self.selectedLogPreset.filter,
                            previous: self.includePreviousLogs
                        )
                    }
                    return try await self.kubeClient.podLogs(
                        from: self.state.kubeConfigSources,
                        context: context,
                        namespace: self.state.selectedNamespace,
                        podName: pod.name,
                        containers: pod.logContainerNames,
                        filter: self.selectedLogPreset.filter,
                        previous: self.includePreviousLogs
                    )
                }
                async let manifests = fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .pod,
                    name: pod.name
                )

                switch await inspectorPod {
                case let .success(jsonPod):
                    if isCurrentResourceDetailsRequest(requestID), state.selectedPod?.id == pod.id {
                        state.setSelectedPod(pod.mergingInspectorDetail(jsonPod))
                    }
                case .failure:
                    break
                }

                applyResourceManifestResults(await manifests, kind: .pod, name: pod.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }

                switch await logsResult {
                case let .success(logs):
                    commitPodLogFetch(
                        logs,
                        contextName: context.name,
                        namespace: state.selectedNamespace,
                        podName: pod.name
                    )
                    state.setLastLogFetchError(nil)
                case let .failure(error):
                    if Self.isLikelyLogFetchFailure(error) {
                        state.setLastLogFetchError(Self.logFetchFailureMessage(for: error))
                    } else {
                        state.setLastLogFetchError(error.localizedDescription)
                    }
                }
                state.clearUnifiedServiceLogs()

        case .service:
                guard let service = state.selectedService else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                state.showCachedUnifiedLogs(contextName: context.name, namespace: state.selectedNamespace, kind: .service, resourceName: service.name)
                state.isLoadingLogs = true
                defer {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.isLoadingLogs = false
                    }
                }

                async let unifiedResult = captureResult {
                    try await self.kubeClient.unifiedLogsForService(
                        from: self.state.kubeConfigSources,
                        context: context,
                        namespace: self.state.selectedNamespace,
                        service: service,
                        filter: self.selectedLogPreset.filter,
                        previous: self.includePreviousLogs
                    )
                }
                async let manifests = fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .service,
                    name: service.name
                )

                applyResourceManifestResults(await manifests, kind: .service, name: service.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }

                switch await unifiedResult {
                case let .success(unified):
                    commitUnifiedLogFetch(
                        unified.mergedText,
                        pods: unified.podNames,
                        contextName: context.name,
                        namespace: state.selectedNamespace,
                        kind: .service,
                        resourceName: service.name
                    )
                    state.setLastLogFetchError(nil)
                case let .failure(error):
                    if Self.isLikelyLogFetchFailure(error) {
                        state.setLastLogFetchError(Self.logFetchFailureMessage(for: error))
                    } else {
                        state.setLastLogFetchError(error.localizedDescription)
                    }
                }
                state.setPodLogs("")

        case .deployment:
                guard let deployment = state.selectedDeployment else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                state.showCachedUnifiedLogs(contextName: context.name, namespace: state.selectedNamespace, kind: .deployment, resourceName: deployment.name)
                state.isLoadingLogs = true
                defer {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.isLoadingLogs = false
                    }
                }

                async let unifiedResult = captureResult {
                    try await self.kubeClient.unifiedLogsForDeployment(
                        from: self.state.kubeConfigSources,
                        context: context,
                        namespace: self.state.selectedNamespace,
                        deployment: deployment,
                        filter: self.selectedLogPreset.filter,
                        previous: self.includePreviousLogs
                    )
                }
                async let manifests = fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .deployment,
                    name: deployment.name
                )
                async let historyResult = captureResult {
                    try await self.kubeClient.deploymentRolloutHistory(
                        from: self.state.kubeConfigSources,
                        context: context,
                        namespace: self.state.selectedNamespace,
                        deploymentName: deployment.name
                    )
                }

                applyResourceManifestResults(await manifests, kind: .deployment, name: deployment.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }

                switch await unifiedResult {
                case let .success(unified):
                    commitUnifiedLogFetch(
                        unified.mergedText,
                        pods: unified.podNames,
                        contextName: context.name,
                        namespace: state.selectedNamespace,
                        kind: .deployment,
                        resourceName: deployment.name
                    )
                    state.setLastLogFetchError(nil)
                case let .failure(error):
                    if Self.isLikelyLogFetchFailure(error) {
                        state.setLastLogFetchError(Self.logFetchFailureMessage(for: error))
                    } else {
                        state.setLastLogFetchError(error.localizedDescription)
                    }
                }

                switch await historyResult {
                case let .success(history):
                    state.setDeploymentRolloutHistory(history)
                case let .failure(error):
                    state.setDeploymentRolloutHistory(
                        resourceDetailsFailureMessage(action: "load rollout history for", kind: .deployment, name: deployment.name, error: error)
                    )
                }
                state.setPodLogs("")

        case .statefulSet:
                guard let resource = state.selectedStatefulSet else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .statefulSet,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .statefulSet, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .daemonSet:
                guard let resource = state.selectedDaemonSet else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .daemonSet,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .daemonSet, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .job:
                guard let resource = state.selectedJob else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .job,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .job, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .cronJob:
                guard let resource = state.selectedCronJob else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .cronJob,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .cronJob, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .replicaSet:
                guard let resource = state.selectedReplicaSet else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .replicaSet,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .replicaSet, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .ingress:
                guard let resource = state.selectedIngress else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .ingress,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .ingress, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .configMap:
                guard let resource = state.selectedConfigMap else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .configMap,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .configMap, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .secret:
                guard let resource = state.selectedSecret else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .secret,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .secret, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .node:
                guard let resource = state.selectedNode else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .node,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .node, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .persistentVolumeClaim:
                guard let resource = state.selectedPersistentVolumeClaim else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .persistentVolumeClaim,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .persistentVolumeClaim, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .persistentVolume:
                guard let resource = state.selectedPersistentVolume else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .persistentVolume,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .persistentVolume, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .storageClass:
                guard let resource = state.selectedStorageClass else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .storageClass,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .storageClass, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .horizontalPodAutoscaler:
                guard let resource = state.selectedHorizontalPodAutoscaler else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .horizontalPodAutoscaler,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .horizontalPodAutoscaler, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .networkPolicy:
                guard let resource = state.selectedNetworkPolicy else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .networkPolicy,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .networkPolicy, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .endpoint:
                guard let resource = state.selectedEndpoint else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .endpoint,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: .endpoint, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                guard let resource = state.selectedRBACResource else {
                    if isCurrentResourceDetailsRequest(requestID) {
                        state.clearResourceDetails()
                    }
                    return
                }

                let pair = await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: resource.kind,
                    name: resource.name
                )

                applyResourceManifestResults(pair, kind: resource.kind, name: resource.name, requestID: requestID)
                guard isCurrentResourceDetailsRequest(requestID) else { return }
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

        case .event:
                if isCurrentResourceDetailsRequest(requestID) {
                    state.clearResourceDetails()
                }
        }
    }

    private func loadHelmReleases(
        context: KubeContext,
        namespace: String,
        kubeConfigSourcesOverride: [KubeConfigSource]? = nil,
        allNamespacesOverride: Bool? = nil,
        loadDetails: Bool = true
    ) async throws {
        let kubeConfigSources = kubeConfigSourcesOverride ?? state.kubeConfigSources
        let allNamespaces = allNamespacesOverride ?? state.isHelmAllNamespaces
        let requestScope = HelmReleaseListScope(
            kubeConfigSources: kubeConfigSources,
            context: context,
            namespace: namespace,
            allNamespaces: allNamespaces
        )
        guard helmReleaseListScopeMatchesCurrentState(requestScope) else {
            throw CancellationError()
        }

        let requestID = UUID()
        latestHelmReleaseListRequestID = requestID
        activeHelmReleaseListRequestID = requestID
        state.isLoading = true
        defer {
            if activeHelmReleaseListRequestID == requestID {
                activeHelmReleaseListRequestID = nil
                state.isLoading = false
            }
        }

        let scope = allNamespaces ? "all namespaces" : namespace
        state.markResourceListsRefreshing([.helmReleases], message: "Refreshing \(context.name) / \(scope)")
        let releases: [HelmReleaseSummary]
        do {
            releases = try await helmReleaseList(
                kubeConfigSources,
                context,
                allNamespaces ? nil : namespace,
                allNamespaces
            )
        } catch {
            guard helmReleaseListRequestIsCurrent(requestID, scope: requestScope) else {
                throw CancellationError()
            }
            guard !Self.isBenignCancellationError(error) else {
                throw error
            }
            state.markResourceListsFailed([.helmReleases], message: error.localizedDescription)
            throw error
        }

        try Task.checkCancellation()
        guard helmReleaseListRequestIsCurrent(requestID, scope: requestScope) else {
            throw CancellationError()
        }
        state.setHelmReleases(releases)
        state.markResourceListsLive(
            [.helmReleases],
            message: "Live for \(context.name) / \(scope)"
        )
        if loadDetails {
            await loadHelmDetailsForCurrentSelectionAsync()
        }
    }

    private func helmReleaseListRequestIsCurrent(
        _ requestID: UUID,
        scope: HelmReleaseListScope
    ) -> Bool {
        latestHelmReleaseListRequestID == requestID
            && helmReleaseListScopeMatchesCurrentState(scope)
    }

    private func invalidateHelmReleaseListRequest() {
        latestHelmReleaseListRequestID = UUID()
        if activeHelmReleaseListRequestID != nil {
            activeHelmReleaseListRequestID = nil
            state.isLoading = false
        }
    }

    private func helmReleaseListScopeMatchesCurrentState(_ scope: HelmReleaseListScope) -> Bool {
        state.selectedSection == .helm
            && (!UserDefaults.standard.runeSimpleMode || helmBrowserResourceFamily == .helmReleases)
            && state.kubeConfigSources == scope.kubeConfigSources
            && state.selectedContext == scope.context
            && state.selectedNamespace == scope.namespace
            && state.isHelmAllNamespaces == scope.allNamespaces
    }

    private func loadOperatorResources(context: KubeContext, namespace: String) async {
        state.markResourceListsRefreshing([.operatorResources], message: "Refreshing \(context.name) / \(namespace)")
        let resources: [OperatorResourceSummary]
        let loadedFreshly: Bool
        let loadErrorMessage: String?
        do {
            resources = try await kubeClient.listOperatorResources(
                from: state.kubeConfigSources,
                context: context,
                namespace: namespace
            )
            loadedFreshly = true
            loadErrorMessage = nil
        } catch {
            resources = []
            loadedFreshly = false
            loadErrorMessage = error.localizedDescription
        }
        guard state.selectedContext == context, state.selectedNamespace == namespace else { return }
        operatorResourcePage = 0
        state.setOperatorResources(resources)
        if loadedFreshly {
            state.markResourceListsLive([.operatorResources], message: "Live for \(context.name) / \(namespace)")
        } else if let loadErrorMessage {
            state.markResourceListsFailed([.operatorResources], message: loadErrorMessage)
        }
    }

    private func loadSelectedHelmBrowserResource(context: KubeContext, namespace: String) async throws {
        switch helmBrowserResourceFamily {
        case .operatorResources:
            await loadOperatorResources(context: context, namespace: namespace)
        default:
            try await loadHelmReleases(context: context, namespace: namespace)
        }
    }

    private func refreshSelectedHelmBrowserResource() {
        guard let context = state.selectedContext else { return }
        let namespace = state.selectedNamespace
        Task { [weak self] in
            guard let self else { return }
            do {
                try await self.loadSelectedHelmBrowserResource(context: context, namespace: namespace)
            } catch {
                if Self.isBenignCancellationError(error) { return }
                self.state.setError(error)
            }
        }
    }

    public func refreshReplicaSetsForCurrentNamespace() {
        guard let context = state.selectedContext else { return }
        let namespace = state.selectedNamespace
        let sources = state.kubeConfigSources
        guard !namespace.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        state.markResourceListsRefreshing([.replicaSets], message: "Refreshing \(context.name) / \(namespace)")

        Task { [weak self] in
            guard let self else { return }
            do {
                let replicaSets = try await self.kubeClient.listReplicaSets(
                    from: sources,
                    context: context,
                    namespace: namespace
                )
                guard self.state.selectedContext == context,
                      self.state.selectedNamespace == namespace else { return }
                let cached = self.store.snapshot(context: context, namespace: namespace)
                self.store.cacheSnapshot(
                    context: context,
                    namespace: namespace,
                    pods: cached.pods,
                    deployments: cached.deployments,
                    statefulSets: cached.statefulSets,
                    daemonSets: cached.daemonSets,
                    jobs: cached.jobs,
                    cronJobs: cached.cronJobs,
                    replicaSets: replicaSets,
                    persistentVolumeClaims: cached.persistentVolumeClaims,
                    horizontalPodAutoscalers: cached.horizontalPodAutoscalers,
                    networkPolicies: cached.networkPolicies,
                    services: cached.services,
                    endpoints: cached.endpoints,
                    ingresses: cached.ingresses,
                    configMaps: cached.configMaps,
                    secrets: cached.secrets,
                    serviceAccounts: cached.serviceAccounts,
                    events: cached.events
                )
                self.state.setReplicaSets(replicaSets)
                self.state.markResourceListsLive([.replicaSets], message: "Live for \(context.name) / \(namespace)")
            } catch {
                if Self.isBenignCancellationError(error) { return }
                self.state.markResourceListsFailed([.replicaSets], message: error.localizedDescription)
            }
        }
    }

    private func loadHelmDetailsForCurrentSelection() {
        Task {
            await loadHelmDetailsForCurrentSelectionAsync()
        }
    }

    private func loadOperatorResourceDetailsForCurrentSelection(
        preservingVisibleDocuments: Bool = false
    ) {
        resourceDetailsTask?.cancel()
        let requestID = UUID()
        latestResourceDetailsRequestID = requestID
        let scope = currentOperatorResourceDetailScope()
        let preservesVisibleDocuments = beginResourceDetailFetch(
            scope: scope,
            preservingVisibleDocuments: preservingVisibleDocuments
        )
        resourceDetailsRequestPreservingVisibleDocuments = preservesVisibleDocuments ? requestID : nil

        resourceDetailsTask = Task { [weak self] in
            guard let self else { return }
            await self.loadOperatorResourceDetailsForCurrentSelectionAsync(requestID: requestID)
            if self.resourceDetailsRequestPreservingVisibleDocuments == requestID {
                self.resourceDetailsRequestPreservingVisibleDocuments = nil
            }
            if self.isCurrentResourceDetailsRequest(requestID) {
                self.resourceDetailsTask = nil
            }
        }
    }

    private func beginResourceDetailFetch(
        scope: ResourceDetailScope?,
        preservingVisibleDocuments: Bool
    ) -> Bool {
        let canPreserveVisibleDocuments = preservingVisibleDocuments
            && (!state.resourceYAML.isEmpty || !state.resourceDescribe.isEmpty)
            && (state.resourceDetailScope == nil || state.resourceDetailScope == scope)
        if canPreserveVisibleDocuments {
            state.beginResourceDetailRefresh(scope: scope)
        } else {
            state.beginResourceDetailLoad(scope: scope)
        }
        return canPreserveVisibleDocuments
    }

    private func loadOperatorResourceDetailsForCurrentSelectionAsync(requestID: UUID) async {
        defer {
            if isCurrentResourceDetailsRequest(requestID) {
                state.finishResourceDetailLoad()
            }
        }

        guard let context = state.selectedContext,
              let resource = state.selectedOperatorResource else {
            if isCurrentResourceDetailsRequest(requestID) {
                state.clearResourceDetails()
            }
            return
        }

        if context.name == demoContextName {
            applyDemoOperatorResourceDetailsForCurrentSelection()
            return
        }

        async let yaml = captureResult {
            try await self.kubeClient.operatorResourceYAML(
                from: self.state.kubeConfigSources,
                context: context,
                resource: resource
            )
        }
        async let describe = captureResult {
            try await self.kubeClient.operatorResourceDescribe(
                from: self.state.kubeConfigSources,
                context: context,
                resource: resource
            )
        }

        applyOperatorResourceManifestResults(
            (await yaml, await describe),
            resource: resource,
            requestID: requestID
        )
    }

    private func applyOperatorResourceManifestResults(
        _ pair: (yaml: Result<String, Error>, describe: Result<String, Error>),
        resource: OperatorResourceSummary,
        requestID: UUID
    ) {
        guard isCurrentResourceDetailsRequest(requestID),
              state.selectedOperatorResource == resource else { return }

        switch pair.yaml {
        case let .success(yaml):
            state.setResourceYAML(normalizeLoadedResourceText(yaml))
        case let .failure(error):
            let message = resourceDetailsFailureMessage(action: "load YAML for", kind: resource.kind, name: resource.name, error: error)
            if resourceDetailsRequestPreservingVisibleDocuments == requestID {
                state.setResourceYAMLRefreshError(message)
            } else {
                state.setResourceYAMLError(message)
            }
        }

        switch pair.describe {
        case let .success(describe):
            state.setResourceDescribe(normalizeLoadedResourceText(describe))
        case let .failure(error):
            let message = resourceDetailsFailureMessage(action: "load describe for", kind: resource.kind, name: resource.name, error: error)
            if resourceDetailsRequestPreservingVisibleDocuments == requestID {
                state.setResourceDescribeRefreshError(message)
            } else {
                state.setResourceDescribeError(message)
            }
        }
    }

    private func loadHelmDetailsForCurrentSelectionAsync() async {
        let requestID = UUID()
        latestHelmDetailsRequestID = requestID

        do {
            guard let context = state.selectedContext, let release = state.selectedHelmRelease else {
                state.setHelmValues("")
                state.setHelmManifest("")
                state.setHelmHistory([])
                return
            }

            if context.name == demoContextName {
                applyDemoHelmDetailsForCurrentSelection()
                return
            }

            async let values = kubeClient.releaseValues(
                from: state.kubeConfigSources,
                context: context,
                namespace: release.namespace,
                releaseName: release.name
            )
            async let manifest = kubeClient.releaseManifest(
                from: state.kubeConfigSources,
                context: context,
                namespace: release.namespace,
                releaseName: release.name
            )
            async let history = kubeClient.releaseHistory(
                from: state.kubeConfigSources,
                context: context,
                namespace: release.namespace,
                releaseName: release.name
            )

            let loadedValues = try await values
            let loadedManifest = try await manifest
            let loadedHistory = try await history
            guard isCurrentHelmDetailsRequest(requestID, context: context, release: release) else {
                diagnostics.trace("helm", "discarded stale details load")
                return
            }

            state.setHelmValues(loadedValues)
            state.setHelmManifest(loadedManifest)
            state.setHelmHistory(loadedHistory)
        } catch {
            if Self.isBenignCancellationError(error) {
                diagnostics.trace("helm", "details load cancelled")
                return
            }
            guard isCurrentHelmDetailsRequest(requestID) else {
                diagnostics.trace("helm", "discarded stale details error")
                return
            }
            state.setError(error)
        }
    }

    private func isCurrentHelmDetailsRequest(
        _ requestID: UUID,
        context: KubeContext? = nil,
        release: HelmReleaseSummary? = nil
    ) -> Bool {
        guard latestHelmDetailsRequestID == requestID else { return false }
        guard state.selectedSection == .helm else { return false }
        if let context, state.selectedContext != context { return false }
        if let release, state.selectedHelmRelease != release { return false }
        return true
    }

    /// Short English message for the log pane when streaming logs failed (timeout or error output from the log fetch).
    private static func logFetchFailureMessage(for error: Error) -> String {
        if case let RuneError.commandFailed(_, message) = error {
            return message
        }
        return error.localizedDescription
    }

    /// True when the failed command was a pod log fetch (including timeout), as opposed to YAML or describe loads.
    private static func isLikelyLogFetchFailure(_ error: Error) -> Bool {
        guard case let RuneError.commandFailed(command, _) = error else { return false }
        return command.split(separator: " ").contains(Substring("logs"))
            || command.contains("/log")
            || command.localizedCaseInsensitiveContains("pod log")
    }

    private func setExportErrorUnlessCancelled(_ error: Error) {
        guard !Self.isUserCancelledExport(error) else { return }
        state.setError(error)
    }

    private static func isUserCancelledExport(_ error: Error) -> Bool {
        if let exportError = error as? FileExportError {
            return exportError == .userCancelled
        }

        let nsError = error as NSError
        return nsError.domain == NSCocoaErrorDomain
            && nsError.code == NSUserCancelledError
    }

    private func currentWritableResource() -> (KubeResourceKind, String)? {
        switch state.selectedWorkloadKind {
        case .pod:
            guard let pod = state.selectedPod else { return nil }
            return (.pod, pod.name)
        case .deployment:
            guard let deployment = state.selectedDeployment else { return nil }
            return (.deployment, deployment.name)
        case .statefulSet:
            guard let resource = state.selectedStatefulSet else { return nil }
            return (.statefulSet, resource.name)
        case .daemonSet:
            guard let resource = state.selectedDaemonSet else { return nil }
            return (.daemonSet, resource.name)
        case .job:
            guard let resource = state.selectedJob else { return nil }
            return (.job, resource.name)
        case .cronJob:
            guard let resource = state.selectedCronJob else { return nil }
            return (.cronJob, resource.name)
        case .replicaSet:
            guard let resource = state.selectedReplicaSet else { return nil }
            return (.replicaSet, resource.name)
        case .service:
            guard let service = state.selectedService else { return nil }
            return (.service, service.name)
        case .ingress:
            guard let resource = state.selectedIngress else { return nil }
            return (.ingress, resource.name)
        case .configMap:
            guard let resource = state.selectedConfigMap else { return nil }
            return (.configMap, resource.name)
        case .secret:
            guard let resource = state.selectedSecret else { return nil }
            return (.secret, resource.name)
        case .node:
            guard let resource = state.selectedNode else { return nil }
            return (.node, resource.name)
        case .persistentVolumeClaim:
            guard let resource = state.selectedPersistentVolumeClaim else { return nil }
            return (.persistentVolumeClaim, resource.name)
        case .persistentVolume:
            guard let resource = state.selectedPersistentVolume else { return nil }
            return (.persistentVolume, resource.name)
        case .storageClass:
            guard let resource = state.selectedStorageClass else { return nil }
            return (.storageClass, resource.name)
        case .horizontalPodAutoscaler:
            guard let resource = state.selectedHorizontalPodAutoscaler else { return nil }
            return (.horizontalPodAutoscaler, resource.name)
        case .networkPolicy:
            guard let resource = state.selectedNetworkPolicy else { return nil }
            return (.networkPolicy, resource.name)
        case .endpoint:
            guard let resource = state.selectedEndpoint else { return nil }
            return (.endpoint, resource.name)
        case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            guard let resource = state.selectedRBACResource else { return nil }
            return (resource.kind, resource.name)
        case .event:
            return nil
        }
    }

    private func currentResourceDetailScope() -> ResourceDetailScope? {
        guard let contextName = state.selectedContext?.name else { return nil }
        guard let reference = currentResourceReference() else { return nil }
        return ResourceDetailScope(
            contextName: contextName,
            namespace: reference.namespace,
            kind: reference.kind,
            name: reference.name
        )
    }

    private func currentOperatorResourceDetailScope() -> ResourceDetailScope? {
        guard let contextName = state.selectedContext?.name,
              let resource = state.selectedOperatorResource else { return nil }
        return ResourceDetailScope(
            contextName: contextName,
            namespace: resource.namespace,
            kind: resource.kind,
            name: resource.name
        )
    }

    private func loadedResourceDetailScopeMatchesCurrentSelection() -> Bool {
        guard let loadedScope = state.resourceDetailScope else { return true }
        let currentScope = state.selectedSection == .helm && state.selectedOperatorResource != nil
            ? currentOperatorResourceDetailScope()
            : currentResourceDetailScope()
        return loadedScope == currentScope
    }

    private func currentResourceReference() -> (kind: KubeResourceKind, name: String, namespace: String?)? {
        switch state.selectedWorkloadKind {
        case .pod:
            guard let pod = state.selectedPod else { return nil }
            return (.pod, pod.name, pod.namespace)
        case .deployment:
            guard let deployment = state.selectedDeployment else { return nil }
            return (.deployment, deployment.name, deployment.namespace)
        case .statefulSet:
            return state.selectedStatefulSet.map { ($0.kind, $0.name, $0.namespace) }
        case .daemonSet:
            return state.selectedDaemonSet.map { ($0.kind, $0.name, $0.namespace) }
        case .job:
            return state.selectedJob.map { ($0.kind, $0.name, $0.namespace) }
        case .cronJob:
            return state.selectedCronJob.map { ($0.kind, $0.name, $0.namespace) }
        case .replicaSet:
            return state.selectedReplicaSet.map { ($0.kind, $0.name, $0.namespace) }
        case .service:
            guard let service = state.selectedService else { return nil }
            return (.service, service.name, service.namespace)
        case .ingress:
            return state.selectedIngress.map { ($0.kind, $0.name, $0.namespace) }
        case .configMap:
            return state.selectedConfigMap.map { ($0.kind, $0.name, $0.namespace) }
        case .secret:
            return state.selectedSecret.map { ($0.kind, $0.name, $0.namespace) }
        case .node:
            return state.selectedNode.map { ($0.kind, $0.name, $0.namespace) }
        case .persistentVolumeClaim:
            return state.selectedPersistentVolumeClaim.map { ($0.kind, $0.name, $0.namespace) }
        case .persistentVolume:
            return state.selectedPersistentVolume.map { ($0.kind, $0.name, $0.namespace) }
        case .storageClass:
            return state.selectedStorageClass.map { ($0.kind, $0.name, $0.namespace) }
        case .horizontalPodAutoscaler:
            return state.selectedHorizontalPodAutoscaler.map { ($0.kind, $0.name, $0.namespace) }
        case .networkPolicy:
            return state.selectedNetworkPolicy.map { ($0.kind, $0.name, $0.namespace) }
        case .endpoint:
            return state.selectedEndpoint.map { ($0.kind, $0.name, $0.namespace) }
        case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            return state.selectedRBACResource.map { ($0.kind, $0.name, $0.namespace) }
        case .event:
            return nil
        }
    }

    private func scheduleResourceYAMLValidation() {
        yamlValidationTask?.cancel()

        let yaml = state.resourceYAML
        let localIssues = YAMLLanguageService.analyze(yaml).validationIssues
        state.setResourceYAMLValidationIssues(localIssues)

        let trimmed = yaml.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty,
              let context = state.selectedContext,
              let resource = currentWritableResource()
        else {
            state.finishResourceYAMLValidation()
            return
        }

        guard context.name != demoContextName else {
            state.finishResourceYAMLValidation()
            return
        }

        guard !localIssues.contains(where: { $0.severity == .error }) else {
            state.finishResourceYAMLValidation()
            return
        }

        let kubeConfigSources = state.kubeConfigSources
        let namespace = state.selectedNamespace
        let requestID = UUID()
        latestYAMLValidationRequestID = requestID
        state.beginResourceYAMLValidation()

        yamlValidationTask = Task { @MainActor [weak self] in
            guard let self else { return }

            do {
                try await Task.sleep(nanoseconds: self.yamlValidationDebounceNanoseconds)
            } catch {
                return
            }

            let remoteIssues: [YAMLValidationIssue]
            do {
                remoteIssues = try await self.kubeClient.validateResourceYAML(
                    from: kubeConfigSources,
                    context: context,
                    namespace: namespace,
                    yaml: yaml
                )
            } catch {
                if error is CancellationError {
                    return
                }

                remoteIssues = [
                    YAMLValidationIssue(
                        source: .transport,
                        severity: .warning,
                        message: error.localizedDescription
                    )
                ]
            }

            guard !Task.isCancelled else { return }
            guard self.latestYAMLValidationRequestID == requestID else { return }
            guard let currentResource = self.currentWritableResource() else { return }
            guard self.state.resourceYAML == yaml,
                  self.state.selectedContext == context,
                  self.state.selectedNamespace == namespace,
                  currentResource == resource
            else {
                return
            }

            self.state.setResourceYAMLValidationIssues(Self.deduplicatedYAMLValidationIssues(localIssues + remoteIssues))
            self.state.finishResourceYAMLValidation()
        }
    }

    private static func deduplicatedYAMLValidationIssues(_ issues: [YAMLValidationIssue]) -> [YAMLValidationIssue] {
        var seen: Set<String> = []
        return issues.filter { issue in
            seen.insert(issue.id).inserted
        }
    }

    private func currentDeletableResource() -> (KubeResourceKind, String)? {
        currentWritableResource()
    }

    private func shouldReloadForWorkloadKind(_ kind: KubeResourceKind) -> Bool {
        switch (state.selectedSection, kind) {
        case (.workloads, .pod):
            return state.pods.isEmpty
        case (.workloads, .deployment):
            return state.deployments.isEmpty
        case (.workloads, .statefulSet):
            return state.statefulSets.isEmpty
        case (.workloads, .daemonSet):
            return state.daemonSets.isEmpty
        case (.workloads, .job):
            return state.jobs.isEmpty
        case (.workloads, .cronJob):
            return state.cronJobs.isEmpty
        case (.workloads, .replicaSet):
            return state.replicaSets.isEmpty
        case (.networking, .service):
            return state.services.isEmpty
        case (.networking, .endpoint):
            return state.endpoints.isEmpty
        case (.networking, .ingress):
            return state.ingresses.isEmpty
        case (.config, .configMap):
            return state.configMaps.isEmpty
        case (.config, .secret):
            return state.secrets.isEmpty
        case (.storage, .persistentVolumeClaim):
            return state.persistentVolumeClaims.isEmpty
        case (.storage, .persistentVolume):
            return state.persistentVolumes.isEmpty
        case (.storage, .storageClass):
            return state.storageClasses.isEmpty
        case (.storage, .node):
            return state.nodes.isEmpty
        case (.workloads, .horizontalPodAutoscaler):
            return state.horizontalPodAutoscalers.isEmpty
        case (.networking, .networkPolicy):
            return state.networkPolicies.isEmpty
        case (.rbac, .serviceAccount):
            return state.serviceAccounts.isEmpty
        case (.rbac, .role):
            return state.rbacRoles.isEmpty
        case (.rbac, .roleBinding):
            return state.rbacRoleBindings.isEmpty
        case (.rbac, .clusterRole):
            return state.rbacClusterRoles.isEmpty
        case (.rbac, .clusterRoleBinding):
            return state.rbacClusterRoleBindings.isEmpty
        default:
            return false
        }
    }

    private func currentNavigationCheckpoint() -> NavigationCheckpoint {
        NavigationCheckpoint(
            contextName: state.selectedContext?.name,
            namespace: state.selectedNamespace,
            section: state.selectedSection,
            workloadKind: state.selectedWorkloadKind,
            selectedPodName: state.selectedPod?.name,
            selectedDeploymentName: state.selectedDeployment?.name,
            selectedServiceName: state.selectedService?.name,
            selectedEventID: state.selectedEvent?.id,
            selectedStatefulSetName: state.selectedStatefulSet?.name,
            selectedDaemonSetName: state.selectedDaemonSet?.name,
            selectedJobName: state.selectedJob?.name,
            selectedCronJobName: state.selectedCronJob?.name,
            selectedReplicaSetName: state.selectedReplicaSet?.name,
            selectedPersistentVolumeClaimName: state.selectedPersistentVolumeClaim?.name,
            selectedPersistentVolumeName: state.selectedPersistentVolume?.name,
            selectedStorageClassName: state.selectedStorageClass?.name,
            selectedHorizontalPodAutoscalerName: state.selectedHorizontalPodAutoscaler?.name,
            selectedNetworkPolicyName: state.selectedNetworkPolicy?.name,
            selectedEndpointName: state.selectedEndpoint?.name,
            selectedIngressName: state.selectedIngress?.name,
            selectedConfigMapName: state.selectedConfigMap?.name,
            selectedSecretName: state.selectedSecret?.name,
            selectedNodeName: state.selectedNode?.name,
            selectedRBACResourceID: state.selectedRBACResource?.id
        )
    }

    private func prepareNavigationMutation(trackHistory: Bool) {
        invalidateDeferredSelectionRestores()
        if trackHistory {
            cancelPendingEventSourceNavigation()
        }
        cancelObsoleteSelectionRequests()
        guard trackHistory, !isApplyingNavigationCheckpoint, navigationHistory.isEmpty else { return }
        navigationHistory.append(currentNavigationCheckpoint())
        navigationIndex = 0
        updateNavigationAvailability()
    }

    private func invalidateDeferredSelectionRestores() {
        guard !isRunningDeferredSelectionRestore else { return }
        deferredSelectionRestoreGeneration &+= 1
        navigationSelectionRestoreTask?.cancel()
        navigationSelectionRestoreTask = nil
        savedWorkspaceRestoreTask?.cancel()
        savedWorkspaceRestoreTask = nil
    }

    private func cancelObsoleteSelectionRequests() {
        cancelPendingLogReload()
        resourceDetailsTask?.cancel()
        resourceDetailsTask = nil
        latestResourceDetailsRequestID = UUID()
        if state.isLoadingResourceDetails {
            state.finishResourceDetailLoad()
        }
        state.isLoadingLogs = false
    }

    private func recordNavigationCheckpoint() {
        guard !isApplyingNavigationCheckpoint else { return }
        let checkpoint = currentNavigationCheckpoint()
        if navigationIndex >= 0, navigationIndex < navigationHistory.count, navigationHistory[navigationIndex] == checkpoint {
            updateNavigationAvailability()
            return
        }

        if navigationIndex < navigationHistory.count - 1 {
            navigationHistory.removeSubrange((navigationIndex + 1)..<navigationHistory.count)
        }

        navigationHistory.append(checkpoint)
        navigationIndex = navigationHistory.count - 1
        updateNavigationAvailability()
    }

    private func applyNavigationCheckpoint(_ checkpoint: NavigationCheckpoint) {
        isApplyingNavigationCheckpoint = true
        defer { isApplyingNavigationCheckpoint = false }

        if let contextName = checkpoint.contextName,
           let context = state.contexts.first(where: { $0.name == contextName }) {
            setContext(
                context,
                preferredNamespace: checkpoint.namespace,
                trackHistory: false,
                triggerReload: false
            )
        } else if checkpoint.contextName != nil {
            return
        }

        setSection(checkpoint.section, trackHistory: false, triggerReload: false)
        setWorkloadKind(checkpoint.workloadKind, trackHistory: false, triggerReload: false)
        restoreSelection(from: checkpoint)
        refreshCurrentView()

        invalidateDeferredSelectionRestores()
        let restoreGeneration = deferredSelectionRestoreGeneration
        navigationSelectionRestoreTask = Task { @MainActor [weak self] in
            do {
                try await Task.sleep(nanoseconds: 250_000_000)
            } catch {
                return
            }
            guard !Task.isCancelled,
                  let self,
                  self.deferredSelectionRestoreGeneration == restoreGeneration
            else {
                return
            }
            self.isRunningDeferredSelectionRestore = true
            defer {
                self.isRunningDeferredSelectionRestore = false
                if self.deferredSelectionRestoreGeneration == restoreGeneration {
                    self.navigationSelectionRestoreTask = nil
                }
            }
            self.restoreSelection(from: checkpoint)
        }
    }

    private func restoreSelection(from checkpoint: NavigationCheckpoint) {
        switch checkpoint.workloadKind {
        case .pod:
            selectPod(state.pods.first(where: { $0.name == checkpoint.selectedPodName }), trackHistory: false)
        case .deployment:
            selectDeployment(state.deployments.first(where: { $0.name == checkpoint.selectedDeploymentName }), trackHistory: false)
        case .service:
            selectService(state.services.first(where: { $0.name == checkpoint.selectedServiceName }), trackHistory: false)
        case .statefulSet:
            selectStatefulSet(state.statefulSets.first(where: { $0.name == checkpoint.selectedStatefulSetName }), trackHistory: false)
        case .daemonSet:
            selectDaemonSet(state.daemonSets.first(where: { $0.name == checkpoint.selectedDaemonSetName }), trackHistory: false)
        case .job:
            selectJob(state.jobs.first(where: { $0.name == checkpoint.selectedJobName }), trackHistory: false)
        case .cronJob:
            selectCronJob(state.cronJobs.first(where: { $0.name == checkpoint.selectedCronJobName }), trackHistory: false)
        case .replicaSet:
            selectReplicaSet(state.replicaSets.first(where: { $0.name == checkpoint.selectedReplicaSetName }), trackHistory: false)
        case .ingress:
            selectIngress(state.ingresses.first(where: { $0.name == checkpoint.selectedIngressName }), trackHistory: false)
        case .endpoint:
            selectEndpoint(state.endpoints.first(where: { $0.name == checkpoint.selectedEndpointName }), trackHistory: false)
        case .configMap:
            selectConfigMap(state.configMaps.first(where: { $0.name == checkpoint.selectedConfigMapName }), trackHistory: false)
        case .secret:
            selectSecret(state.secrets.first(where: { $0.name == checkpoint.selectedSecretName }), trackHistory: false)
        case .node:
            selectNode(state.nodes.first(where: { $0.name == checkpoint.selectedNodeName }), trackHistory: false)
        case .persistentVolumeClaim:
            selectPersistentVolumeClaim(
                state.persistentVolumeClaims.first(where: { $0.name == checkpoint.selectedPersistentVolumeClaimName }),
                trackHistory: false
            )
        case .persistentVolume:
            selectPersistentVolume(
                state.persistentVolumes.first(where: { $0.name == checkpoint.selectedPersistentVolumeName }),
                trackHistory: false
            )
        case .storageClass:
            selectStorageClass(
                state.storageClasses.first(where: { $0.name == checkpoint.selectedStorageClassName }),
                trackHistory: false
            )
        case .horizontalPodAutoscaler:
            selectHorizontalPodAutoscaler(
                state.horizontalPodAutoscalers.first(where: { $0.name == checkpoint.selectedHorizontalPodAutoscalerName }),
                trackHistory: false
            )
        case .networkPolicy:
            selectNetworkPolicy(
                state.networkPolicies.first(where: { $0.name == checkpoint.selectedNetworkPolicyName }),
                trackHistory: false
            )
        case .event:
            selectEvent(state.events.first(where: { $0.id == checkpoint.selectedEventID }), trackHistory: false)
        case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            let lists = state.serviceAccounts + state.rbacRoles + state.rbacRoleBindings + state.rbacClusterRoles + state.rbacClusterRoleBindings
            let match = lists.first(where: { $0.id == checkpoint.selectedRBACResourceID })
            selectRBACResource(match, trackHistory: false)
        }
    }

    private func updateNavigationAvailability() {
        canNavigateBack = navigationIndex > 0
        canNavigateForward = navigationIndex >= 0 && navigationIndex < navigationHistory.count - 1
    }

    private static func overviewCacheKey(contextName: String, namespace: String) -> String {
        "\(contextName)::\(namespace)"
    }

    private func markOverviewCooldownBypass(contextName: String, namespace: String) {
        let normalized = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalized.isEmpty else { return }
        bypassOverviewCooldownKeys.insert(Self.overviewCacheKey(contextName: contextName, namespace: normalized))
    }

    private static func isOverviewCacheFresh(
        _ entry: OverviewSnapshotCacheEntry?,
        ttl: TimeInterval,
        reference: Date
    ) -> Bool {
        guard let entry else { return false }
        return reference.timeIntervalSince(entry.fetchedAt) <= ttl
    }

    private func warmOverviewSnapshot(
        contextName: String,
        namespace: String,
        reference: Date,
        allowDiskCache: Bool
    ) async -> OverviewSnapshotCacheEntry? {
        let key = Self.overviewCacheKey(contextName: contextName, namespace: namespace)
        if let cached = overviewSnapshotCache[key],
           Self.isOverviewCacheFresh(cached, ttl: overviewSnapshotFreshnessTTL, reference: reference) {
            diagnostics.log("overview cache hit memory context=\(contextName) namespace=\(namespace)")
            return cached
        }

        guard allowDiskCache else { return nil }

        guard let persisted = await overviewSnapshotPersistence.loadSnapshot(
            contextName: contextName,
            namespace: namespace,
            maxAge: overviewDiskSnapshotFreshnessTTL
        ) else {
            return nil
        }

        diagnostics.log("overview cache hit disk context=\(contextName) namespace=\(namespace)")
        return cachePersistedOverviewSnapshot(persisted, reference: reference)
    }

    private func updateOverviewCache(
        contextName: String,
        namespace: String,
        pods: [PodSummary],
        deploymentsCount: Int,
        servicesCount: Int,
        ingressesCount: Int,
        configMapsCount: Int,
        cronJobsCount: Int,
        nodesCount: Int,
        clusterCPUPercent: Int?,
        clusterMemoryPercent: Int?,
        events: [EventSummary]
    ) {
        let fetchedAt = Date()
        let cacheKey = Self.overviewCacheKey(contextName: contextName, namespace: namespace)
        let entry = OverviewSnapshotCacheEntry(
            fetchedAt: fetchedAt,
            pods: pods,
            deploymentsCount: deploymentsCount,
            servicesCount: servicesCount,
            ingressesCount: ingressesCount,
            configMapsCount: configMapsCount,
            cronJobsCount: cronJobsCount,
            nodesCount: nodesCount,
            clusterCPUPercent: clusterCPUPercent,
            clusterMemoryPercent: clusterMemoryPercent,
            events: events
        )
        overviewSnapshotCache[cacheKey] = entry
        pruneOverviewCache(reference: fetchedAt)

        let persisted = PersistedOverviewSnapshot(
            contextName: contextName,
            namespace: namespace,
            fetchedAt: fetchedAt,
            lastAccessedAt: fetchedAt,
            pods: pods,
            deploymentsCount: deploymentsCount,
            servicesCount: servicesCount,
            ingressesCount: ingressesCount,
            configMapsCount: configMapsCount,
            cronJobsCount: cronJobsCount,
            nodesCount: nodesCount,
            clusterCPUPercent: clusterCPUPercent,
            clusterMemoryPercent: clusterMemoryPercent,
            events: events
        )

        Task(priority: .utility) { [overviewSnapshotPersistence] in
            await overviewSnapshotPersistence.saveSnapshot(persisted)
        }
    }

    private func updateOverviewCacheClusterUsage(
        contextName: String,
        namespace: String,
        cpuPercent: Int?,
        memoryPercent: Int?
    ) {
        let cacheKey = Self.overviewCacheKey(contextName: contextName, namespace: namespace)
        let entry = overviewSnapshotCache[cacheKey]
        let fetchedAt = Date()
        let updated = OverviewSnapshotCacheEntry(
            fetchedAt: fetchedAt,
            pods: entry?.pods ?? state.overviewPods,
            deploymentsCount: entry?.deploymentsCount ?? state.overviewDeploymentsCount,
            servicesCount: entry?.servicesCount ?? state.overviewServicesCount,
            ingressesCount: entry?.ingressesCount ?? state.overviewIngressesCount,
            configMapsCount: entry?.configMapsCount ?? state.overviewConfigMapsCount,
            cronJobsCount: entry?.cronJobsCount ?? state.overviewCronJobsCount,
            nodesCount: entry?.nodesCount ?? state.overviewNodesCount,
            clusterCPUPercent: cpuPercent,
            clusterMemoryPercent: memoryPercent,
            events: entry?.events ?? state.overviewEvents
        )
        overviewSnapshotCache[cacheKey] = updated
        pruneOverviewCache(reference: fetchedAt)
    }

    private func pruneOverviewCache(reference: Date) {
        overviewSnapshotCache = overviewSnapshotCache.filter { _, entry in
            reference.timeIntervalSince(entry.fetchedAt) <= overviewSnapshotRetentionTTL
        }

        guard overviewSnapshotCache.count > maxOverviewSnapshotEntries else { return }
        let keysByOldest = overviewSnapshotCache
            .sorted { lhs, rhs in
                lhs.value.fetchedAt < rhs.value.fetchedAt
            }
            .map(\.key)

        let removeCount = overviewSnapshotCache.count - maxOverviewSnapshotEntries
        for key in keysByOldest.prefix(removeCount) {
            overviewSnapshotCache.removeValue(forKey: key)
        }
    }

    @discardableResult
    private func cachePersistedOverviewSnapshot(
        _ persisted: PersistedOverviewSnapshot,
        reference: Date = Date()
    ) -> OverviewSnapshotCacheEntry {
        let entry = OverviewSnapshotCacheEntry(
            fetchedAt: persisted.fetchedAt,
            pods: persisted.pods,
            deploymentsCount: persisted.deploymentsCount,
            servicesCount: persisted.servicesCount,
            ingressesCount: persisted.ingressesCount,
            configMapsCount: persisted.configMapsCount,
            cronJobsCount: persisted.cronJobsCount ?? 0,
            nodesCount: persisted.nodesCount,
            clusterCPUPercent: persisted.clusterCPUPercent,
            clusterMemoryPercent: persisted.clusterMemoryPercent,
            events: persisted.events
        )
        overviewSnapshotCache[Self.overviewCacheKey(contextName: persisted.contextName, namespace: persisted.namespace)] = entry
        pruneOverviewCache(reference: reference)
        return entry
    }

    private func rememberRecentNamespace(_ namespace: String, for contextName: String) {
        let trimmed = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        var updated = recentNamespacesByContext[contextName] ?? []
        updated.removeAll { $0 == trimmed }
        updated.insert(trimmed, at: 0)
        if updated.count > maxRecentNamespacesPerContext {
            updated = Array(updated.prefix(maxRecentNamespacesPerContext))
        }
        recentNamespacesByContext[contextName] = updated
    }

    private func rememberManualNamespace(_ namespace: String, for contextName: String) {
        let trimmed = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedContext = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, !normalizedContext.isEmpty else { return }

        var manual = contextPreferences.loadManualNamespaces(for: normalizedContext)
        manual.removeAll { $0.caseInsensitiveCompare(trimmed) == .orderedSame }
        manual.append(trimmed)
        contextPreferences.saveManualNamespaces(manual, for: normalizedContext)
    }

    private func currentContextManualNamespaces() -> [String] {
        guard let contextName = state.selectedContext?.name else { return [] }
        return contextPreferences.loadManualNamespaces(for: contextName)
    }

    private func rememberRecentContext(_ contextName: String) {
        let trimmed = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        recentContextNames.removeAll { $0 == trimmed }
        recentContextNames.insert(trimmed, at: 0)
        if recentContextNames.count > maxRecentContexts {
            recentContextNames = Array(recentContextNames.prefix(maxRecentContexts))
        }
    }

    private var isRunningUnderTests: Bool {
        ProcessInfo.processInfo.environment["XCTestConfigurationFilePath"] != nil
            || ProcessInfo.processInfo.environment["SWIFT_TESTING_ENABLED"] != nil
            || NSClassFromString("XCTestCase") != nil
    }

    private func preferredOverviewPrefetchContexts(currentContextName: String) -> [KubeContext] {
        guard maxOverviewPrefetchContexts > 0 else { return [] }

        let recentRank = Dictionary(uniqueKeysWithValues: recentContextNames.enumerated().map { ($1, $0) })
        let favorites = state.favoriteContextNames

        let ranked = state.contexts
            .filter { $0.name != currentContextName }
            .sorted { lhs, rhs in
                let lhsFavorite = favorites.contains(lhs.name)
                let rhsFavorite = favorites.contains(rhs.name)
                if lhsFavorite != rhsFavorite {
                    return lhsFavorite && !rhsFavorite
                }

                let lhsRecent = recentRank[lhs.name] ?? Int.max
                let rhsRecent = recentRank[rhs.name] ?? Int.max
                if lhsRecent != rhsRecent {
                    return lhsRecent < rhsRecent
                }

                return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
            }

        return Array(ranked.prefix(maxOverviewPrefetchContexts))
    }

    private func resolveContextPrefetchNamespace(
        context: KubeContext,
        sources: [KubeConfigSource]
    ) async -> String {
        let preferred = contextPreferences.loadPreferredNamespace(for: context.name) ?? ""
        let cachedNamespaces = store.namespaces(context: context)
        if !cachedNamespaces.isEmpty {
            return resolvedNamespace(
                contextName: context.name,
                preferred: preferred,
                availableNamespaces: cachedNamespaces,
                contextDefaultNamespace: nil
            )
        }

        if UserDefaults.standard.runePersistNamespaceListCache,
           let disk = namespaceListPersistence.load(contextName: context.name),
           !disk.isEmpty {
            store.cacheNamespaces(disk, context: context)
            return resolvedNamespace(
                contextName: context.name,
                preferred: preferred,
                availableNamespaces: disk,
                contextDefaultNamespace: nil
            )
        }

        async let namespaceResult: Result<[String], Error> = captureSnapshotRead {
            try await self.kubeClient.listNamespaces(from: sources, context: context)
        }
        async let contextNamespaceResult: Result<String?, Error> = captureSnapshotRead {
            try await self.kubeClient.contextNamespace(from: sources, context: context)
        }

        let contextDefaultNamespace: String?
        switch await contextNamespaceResult {
        case let .success(value):
            contextDefaultNamespace = value
        case let .failure(error):
            diagnostics.trace(
                "prefetch.context",
                "context-namespace context=<redacted-context> error=\(error.localizedDescription)"
            )
            contextDefaultNamespace = nil
        }

        let mergedNamespaces: [String]
        switch await namespaceResult {
        case let .success(value):
            mergedNamespaces = NamespaceListOrdering.merge(previousOrder: [], apiNames: value)
            store.cacheNamespaces(mergedNamespaces, context: context)
            if UserDefaults.standard.runePersistNamespaceListCache {
                namespaceListPersistence.save(names: mergedNamespaces, contextName: context.name)
            }
        case let .failure(error):
            diagnostics.trace(
                "prefetch.context",
                "namespaces context=<redacted-context> error=\(error.localizedDescription)"
            )
            mergedNamespaces = []
        }

        return resolvedNamespace(
            contextName: context.name,
            preferred: preferred,
            availableNamespaces: mergedNamespaces,
            contextDefaultNamespace: contextDefaultNamespace
        )
    }

    /// Background warm-up for non-selected contexts so context switches can reuse overview cache immediately.
    private func scheduleContextOverviewPrefetch(currentContext: KubeContext) {
        guard UserDefaults.standard.runeBackgroundPrefetchOtherContexts else {
            contextOverviewPrefetchTask?.cancel()
            return
        }
        guard !isRunningUnderTests else { return }

        let targets = preferredOverviewPrefetchContexts(currentContextName: currentContext.name)
        guard !targets.isEmpty else { return }

        let sources = state.kubeConfigSources
        guard !sources.isEmpty else { return }

        contextOverviewPrefetchTask?.cancel()
        contextOverviewPrefetchTask = Task(priority: .utility) { [weak self] in
            guard let self else { return }

            for (index, targetContext) in targets.enumerated() {
                if Task.isCancelled { return }
                if index > 0 {
                    try? await Task.sleep(nanoseconds: self.contextOverviewPrefetchThrottleNanoseconds)
                }

                let stillSameSelectedContext = self.state.selectedContext?.name == currentContext.name
                guard stillSameSelectedContext else { return }

                let targetNamespace = await self.resolveContextPrefetchNamespace(
                    context: targetContext,
                    sources: sources
                )
                let normalizedNamespace = targetNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !normalizedNamespace.isEmpty else { continue }

                let reference = Date()
                let cacheKey = Self.overviewCacheKey(
                    contextName: targetContext.name,
                    namespace: normalizedNamespace
                )
                if Self.isOverviewCacheFresh(
                    self.overviewSnapshotCache[cacheKey],
                    ttl: self.overviewSnapshotFreshnessTTL,
                    reference: reference
                ) {
                    continue
                }

                if let persisted = await self.overviewSnapshotPersistence.loadSnapshot(
                    contextName: targetContext.name,
                    namespace: normalizedNamespace,
                    maxAge: self.overviewSnapshotFreshnessTTL
                ) {
                    _ = self.cachePersistedOverviewSnapshot(persisted, reference: reference)
                    continue
                }

                do {
                    async let pods = self.kubeClient.listPodStatuses(
                        from: sources,
                        context: targetContext,
                        namespace: normalizedNamespace
                    )
                    async let deploymentsCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: targetContext,
                        namespace: normalizedNamespace,
                        resource: "deployments"
                    )
                    async let servicesCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: targetContext,
                        namespace: normalizedNamespace,
                        resource: "services"
                    )
                    async let ingressesCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: targetContext,
                        namespace: normalizedNamespace,
                        resource: "ingresses"
                    )
                    async let configMapsCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: targetContext,
                        namespace: normalizedNamespace,
                        resource: "configmaps"
                    )
                    async let cronJobsCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: targetContext,
                        namespace: normalizedNamespace,
                        resource: "cronjobs"
                    )
                    async let nodesCount = self.kubeClient.countClusterResources(
                        from: sources,
                        context: targetContext,
                        resource: "nodes"
                    )

                    let prefetchedPods = try await pods
                    let prefetchedDeploymentsCount = try await deploymentsCount
                    let prefetchedServicesCount = try await servicesCount
                    let prefetchedIngressesCount = try await ingressesCount
                    let prefetchedConfigMapsCount = try await configMapsCount
                    let prefetchedCronJobsCount = try await cronJobsCount
                    let prefetchedNodesCount = try await nodesCount

                    guard self.state.selectedContext?.name == currentContext.name else { return }

                    self.updateOverviewCache(
                        contextName: targetContext.name,
                        namespace: normalizedNamespace,
                        pods: prefetchedPods,
                        deploymentsCount: prefetchedDeploymentsCount,
                        servicesCount: prefetchedServicesCount,
                        ingressesCount: prefetchedIngressesCount,
                        configMapsCount: prefetchedConfigMapsCount,
                        cronJobsCount: prefetchedCronJobsCount,
                        nodesCount: prefetchedNodesCount,
                        clusterCPUPercent: nil,
                        clusterMemoryPercent: nil,
                        events: []
                    )
                    self.diagnostics.trace(
                        "prefetch.context",
                        "warmed context=<redacted-context> namespace=<redacted-namespace>"
                    )
                } catch {
                    self.diagnostics.trace(
                        "prefetch.context",
                        "context=<redacted-context> namespace=<redacted-namespace> error=\(error.localizedDescription)"
                    )
                }
            }
        }
    }

    private func handleCachesCleared() {
        diagnostics.log("cache clear requested from settings")
        overviewPrefetchTask?.cancel()
        contextOverviewPrefetchTask?.cancel()
        scheduledRefreshTask?.cancel()
        pendingCurrentViewRefreshID = nil
        cancelPendingLogReload()
        resourceDetailsTask?.cancel()

        store.clearAll()
        overviewSnapshotCache.removeAll(keepingCapacity: false)
        bypassOverviewCooldownKeys.removeAll(keepingCapacity: false)
        namespaceMetadataRefreshedAt.removeAll(keepingCapacity: false)
        recentNamespacesByContext.removeAll(keepingCapacity: false)
        recentContextNames.removeAll(keepingCapacity: false)

        state.setNamespaces([])

        if let context = state.selectedContext {
            applyCachedSnapshot(context: context, namespace: state.selectedNamespace)
            scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: true, debounced: false)
        }
    }

    private func preferredOverviewPrefetchNamespaces(
        contextName: String,
        availableNamespaces: [String],
        currentNamespace: String
    ) -> [String] {
        var result: [String] = []

        if let recent = recentNamespacesByContext[contextName] {
            for namespace in recent where namespace != currentNamespace && availableNamespaces.contains(namespace) {
                if !result.contains(namespace) {
                    result.append(namespace)
                }
            }
        }

        if let firstUserNamespace = availableNamespaces.first(where: { namespace in
            let lowered = namespace.lowercased()
            return lowered != "default"
                && lowered != "kube-system"
                && lowered != "kube-public"
                && lowered != "kube-node-lease"
        }), firstUserNamespace != currentNamespace, !result.contains(firstUserNamespace) {
            result.append(firstUserNamespace)
        }

        if let defaultNamespace = availableNamespaces.first(where: { $0 == "default" }),
           defaultNamespace != currentNamespace,
           !result.contains(defaultNamespace) {
            result.append(defaultNamespace)
        }

        if result.count < maxOverviewPrefetchNamespaces {
            let sortedRest = availableNamespaces
                .filter { $0 != currentNamespace && !result.contains($0) }
                .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
            for ns in sortedRest {
                guard result.count < maxOverviewPrefetchNamespaces else { break }
                if Self.isLikelySystemNamespace(ns) { continue }
                result.append(ns)
            }
        }

        if result.count < maxOverviewPrefetchNamespaces {
            let sortedSystem = availableNamespaces
                .filter { $0 != currentNamespace && !result.contains($0) }
                .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
            for ns in sortedSystem {
                guard result.count < maxOverviewPrefetchNamespaces else { break }
                result.append(ns)
            }
        }

        return Array(result.prefix(maxOverviewPrefetchNamespaces))
    }

    private static func isLikelySystemNamespace(_ name: String) -> Bool {
        let l = name.lowercased()
        if l == "default" || l == "kube-public" || l == "kube-node-lease" { return true }
        return l.hasPrefix("kube-")
    }

    /// Background fetch of pod status + resource counts for namespaces in `preferredOverviewPrefetchNamespaces`; writes to `overviewSnapshotCache` and disk. Throttled; aborted when switching context or if tests run.
    private func scheduleOverviewPrefetch(
        context: KubeContext,
        namespaces: [String],
        currentNamespace: String
    ) {
        if isRunningUnderTests {
            return
        }

        let targets = preferredOverviewPrefetchNamespaces(
            contextName: context.name,
            availableNamespaces: namespaces,
            currentNamespace: currentNamespace
        )
        let reference = Date()
        let targetsToPrefetch = targets.filter { namespace in
            let key = Self.overviewCacheKey(contextName: context.name, namespace: namespace)
            return !Self.isOverviewCacheFresh(
                overviewSnapshotCache[key],
                ttl: overviewSnapshotFreshnessTTL,
                reference: reference
            )
        }
        guard !targetsToPrefetch.isEmpty else { return }

        let sources = state.kubeConfigSources
        guard !sources.isEmpty else { return }
        let contextName = context.name
        let nodeCountFallback = store.nodes(context: context).count
        var eventFallbackByNamespace: [String: [EventSummary]] = [:]
        for namespace in targetsToPrefetch {
            eventFallbackByNamespace[namespace] = store.snapshot(context: context, namespace: namespace).events
        }

        overviewPrefetchTask?.cancel()
        overviewPrefetchTask = Task(priority: .utility) { [weak self] in
            guard let self else { return }
            for (index, namespace) in targetsToPrefetch.enumerated() {
                if Task.isCancelled { return }
                if index > 0 {
                    try? await Task.sleep(nanoseconds: self.overviewPrefetchThrottleNanoseconds)
                }

                let stillThisContext = await MainActor.run { [weak self] () -> Bool in
                    guard let self else { return false }
                    return self.state.selectedContext?.name == contextName
                }
                guard stillThisContext else { return }

                if let persisted = await self.overviewSnapshotPersistence.loadSnapshot(
                    contextName: contextName,
                    namespace: namespace,
                    maxAge: self.overviewSnapshotFreshnessTTL
                ) {
                    await MainActor.run { [weak self] in
                        _ = self?.cachePersistedOverviewSnapshot(persisted)
                    }
                }

                do {
                    async let pods = self.kubeClient.listPodStatuses(
                        from: sources,
                        context: context,
                        namespace: namespace
                    )
                    async let deploymentsCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        resource: "deployments"
                    )
                    async let servicesCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        resource: "services"
                    )
                    async let ingressesCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        resource: "ingresses"
                    )
                    async let configMapsCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        resource: "configmaps"
                    )
                    async let cronJobsCount = self.kubeClient.countNamespacedResources(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        resource: "cronjobs"
                    )

                    let prefetchedPods = try await pods
                    let prefetchedDeploymentsCount = try await deploymentsCount
                    let prefetchedServicesCount = try await servicesCount
                    let prefetchedIngressesCount = try await ingressesCount
                    let prefetchedConfigMapsCount = try await configMapsCount
                    let prefetchedCronJobsCount = try await cronJobsCount

                    await MainActor.run { [weak self] in
                        guard let self else { return }
                        guard self.state.selectedContext?.name == contextName else { return }
                        self.updateOverviewCache(
                            contextName: contextName,
                            namespace: namespace,
                            pods: prefetchedPods,
                            deploymentsCount: prefetchedDeploymentsCount,
                            servicesCount: prefetchedServicesCount,
                            ingressesCount: prefetchedIngressesCount,
                            configMapsCount: prefetchedConfigMapsCount,
                            cronJobsCount: prefetchedCronJobsCount,
                            nodesCount: nodeCountFallback,
                            clusterCPUPercent: nodeCountFallback > 0 ? self.state.overviewClusterCPUPercent : nil,
                            clusterMemoryPercent: nodeCountFallback > 0 ? self.state.overviewClusterMemoryPercent : nil,
                            events: eventFallbackByNamespace[namespace] ?? []
                        )
                    }
                } catch {
                    await MainActor.run { [weak self] in
                        self?.diagnostics.log(
                            "overview prefetch failed context=\(contextName) namespace=\(namespace): \(error.localizedDescription)"
                        )
                        self?.diagnostics.trace(
                            "prefetch.overview",
                            "context=<redacted-context> namespace=<redacted-namespace> error=\(error.localizedDescription)"
                        )
                    }
                }
            }
        }
    }

    private func beginSnapshotRequest(context: KubeContext, namespace: String, source: String) -> UUID {
        // New list/snapshot work supersedes in-flight inspector fetches (YAML/describe) so stale results cannot apply after refresh.
        latestResourceDetailsRequestID = UUID()
        if state.isLoadingResourceDetails {
            state.finishResourceDetailLoad()
        }

        let requestID = UUID()
        latestSnapshotRequestID = requestID
        state.setSnapshotFreshness(
            RuneSnapshotFreshness(
                status: .refreshing,
                updatedAt: state.snapshotFreshness.updatedAt,
                message: "Refreshing \(context.name) / \(namespace.isEmpty ? "namespace" : namespace)"
            )
        )
        diagnostics.log("snapshot request=\(requestID.uuidString) source=\(source) context=\(context.name) namespace=\(namespace)")
        return requestID
    }

    /// Ensures the snapshot request is still the latest **and** the user has not switched context or namespace (avoids applying data for the wrong pair).
    private func snapshotRequestIsCurrent(
        _ requestID: UUID,
        context: KubeContext,
        expectedNamespace: String?
    ) -> Bool {
        guard latestSnapshotRequestID == requestID else { return false }
        guard state.selectedContext?.name == context.name else { return false }
        guard let expectedRaw = expectedNamespace else { return true }
        let expected = expectedRaw.trimmingCharacters(in: .whitespacesAndNewlines)
        if expected.isEmpty {
            let current = state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
            return current.isEmpty
        }
        let current = state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        return current.caseInsensitiveCompare(expected) == .orderedSame
    }

    func resolvedNamespace(
        contextName: String,
        preferred: String,
        availableNamespaces: [String],
        contextDefaultNamespace: String?,
        preferContextSuffixOverContextDefault: Bool = false
    ) -> String {
        let trimmedPreferred = preferred.trimmingCharacters(in: .whitespacesAndNewlines)
        let trimmedContextDefault = contextDefaultNamespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""

        guard !availableNamespaces.isEmpty else {
            if !trimmedPreferred.isEmpty {
                return trimmedPreferred
            }
            if !trimmedContextDefault.isEmpty {
                return trimmedContextDefault
            }
            return ""
        }

        let namespaceLookup = namespaceResolutionLookup(for: availableNamespaces)
        if !trimmedPreferred.isEmpty,
           let match = namespaceLookup[trimmedPreferred.lowercased()] {
            return match
        }

        if preferContextSuffixOverContextDefault,
           let suffixMatch = namespaceLongestSuffixOfContext(contextName, availableNamespaces: availableNamespaces) {
            return suffixMatch
        }

        if !trimmedContextDefault.isEmpty,
           let match = namespaceLookup[trimmedContextDefault.lowercased()] {
            return match
        }

        if let suffixMatch = namespaceLongestSuffixOfContext(contextName, availableNamespaces: availableNamespaces) {
            return suffixMatch
        }

        if let firstUserNamespace = availableNamespaces.first(where: { namespace in
            let lowered = namespace.lowercased()
            return lowered != "default"
                && lowered != "kube-system"
                && lowered != "kube-public"
                && lowered != "kube-node-lease"
        }) {
            return firstUserNamespace
        }

        if let defaultNamespace = availableNamespaces.first(where: { $0 == "default" }) {
            return defaultNamespace
        }

        return availableNamespaces[0]
    }

    func resolvedNamespace(
        contextName: String,
        preferredCandidates: [String],
        availableNamespaces: [String],
        contextDefaultNamespace: String?,
        preferContextSuffixOverContextDefault: Bool = false
    ) -> String {
        let candidates = preferredCandidates
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .reduce(into: [String]()) { result, namespace in
                if !result.contains(where: { $0.caseInsensitiveCompare(namespace) == .orderedSame }) {
                    result.append(namespace)
                }
            }

        guard !availableNamespaces.isEmpty else {
            if let firstCandidate = candidates.first {
                return firstCandidate
            }
            let trimmedContextDefault = contextDefaultNamespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            return trimmedContextDefault
        }

        let namespaceLookup = namespaceResolutionLookup(for: availableNamespaces)
        for candidate in candidates {
            if let match = namespaceLookup[candidate.lowercased()] {
                return match
            }
        }

        return resolvedNamespace(
            contextName: contextName,
            preferred: "",
            availableNamespaces: availableNamespaces,
            contextDefaultNamespace: contextDefaultNamespace,
            preferContextSuffixOverContextDefault: preferContextSuffixOverContextDefault
        )
    }

    private func namespaceResolutionLookup(for availableNamespaces: [String]) -> [String: String] {
        if let cached = namespaceResolutionLookupCache, cached.namespaces == availableNamespaces {
            return cached.lookup
        }

        var lookup: [String: String] = [:]
        lookup.reserveCapacity(availableNamespaces.count)
        for namespace in availableNamespaces {
            let key = namespace.lowercased()
            if lookup[key] == nil {
                lookup[key] = namespace
            }
        }
        namespaceResolutionLookupCache = (availableNamespaces, lookup)
        return lookup
    }

    /// Namespaces whose names are a case-insensitive suffix of `contextName` (e.g. `cluster-example-service` → `example-service`).
    /// Picks the longest match so `example-service` wins over `service` when both exist. Skips known cluster/system namespaces.
    private func namespaceLongestSuffixOfContext(_ contextName: String, availableNamespaces: [String]) -> String? {
        let contextLower = contextName.lowercased()
        let system = Set(["default", "kube-system", "kube-public", "kube-node-lease"])
        let candidates = availableNamespaces.filter { ns in
            let n = ns.lowercased()
            guard !system.contains(n), n.count >= 3 else { return false }
            return contextLower.hasSuffix(n)
        }
        return candidates.max(by: { $0.count < $1.count })
    }

    /// Applies `ResourceStore` and fresh `overviewSnapshotCache` entries to `RuneAppState` synchronously (e.g. after `setContext` / `setNamespace` before network refresh).
    private func applyCachedSnapshot(context: KubeContext, namespace: String) {
        let normalizedNamespace = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        let cachedNamespaces = store.namespaces(context: context)
        state.setNamespaces(cachedNamespaces)

        let cachedNodes = store.nodes(context: context)
        state.setNodes(cachedNodes)
        state.setPersistentVolumes(store.persistentVolumes(context: context))
        state.setStorageClasses(store.storageClasses(context: context))

        guard !normalizedNamespace.isEmpty else {
            state.setPods([])
            state.setDeployments([])
            state.setStatefulSets([])
            state.setDaemonSets([])
            state.setJobs([])
            state.setCronJobs([])
            state.setReplicaSets([])
            state.setPersistentVolumeClaims([])
            state.setPersistentVolumes([])
            state.setStorageClasses([])
            state.setHorizontalPodAutoscalers([])
            state.setNetworkPolicies([])
            state.setServices([])
            state.setEndpoints([])
            state.setIngresses([])
            state.setConfigMaps([])
            state.setSecrets([])
            state.setRBACData(roles: [], serviceAccounts: [], roleBindings: [], clusterRoles: [], clusterRoleBindings: [])
            rbacDataScope = nil
            state.setEvents([])
            state.setOverviewSnapshot(
                pods: [],
                deploymentsCount: 0,
                servicesCount: 0,
                ingressesCount: 0,
                configMapsCount: 0,
                cronJobsCount: 0,
                nodesCount: 0,
                clusterCPUPercent: nil,
                clusterMemoryPercent: nil,
                events: []
            )
            return
        }
        let cached = store.snapshot(context: context, namespace: normalizedNamespace)

        state.setPods(cached.pods)
        state.setDeployments(cached.deployments)
        state.setStatefulSets(cached.statefulSets)
        state.setDaemonSets(cached.daemonSets)
        state.setJobs(cached.jobs)
        state.setCronJobs(cached.cronJobs)
        state.setReplicaSets(cached.replicaSets)
        state.setPersistentVolumeClaims(cached.persistentVolumeClaims)
        state.setHorizontalPodAutoscalers(cached.horizontalPodAutoscalers)
        state.setNetworkPolicies(cached.networkPolicies)
        state.setPersistentVolumes(store.persistentVolumes(context: context))
        state.setStorageClasses(store.storageClasses(context: context))
        state.setServices(cached.services)
        state.setEndpoints(cached.endpoints)
        state.setIngresses(cached.ingresses)
        state.setConfigMaps(cached.configMaps)
        state.setSecrets(cached.secrets)
        let cachedRBACScope = RBACDataScope(
            kubeConfigSources: state.kubeConfigSources,
            context: context,
            namespace: normalizedNamespace
        )
        let canReuseScopedRBACData = rbacDataScope == cachedRBACScope
        state.setRBACData(
            roles: canReuseScopedRBACData ? state.rbacRoles : [],
            serviceAccounts: cached.serviceAccounts,
            roleBindings: canReuseScopedRBACData ? state.rbacRoleBindings : [],
            clusterRoles: canReuseScopedRBACData ? state.rbacClusterRoles : [],
            clusterRoleBindings: canReuseScopedRBACData ? state.rbacClusterRoleBindings : []
        )
        rbacDataScope = cachedRBACScope
        state.setEvents(cached.events)

        let reference = Date()
        if let cachedOverview = overviewSnapshotCache[Self.overviewCacheKey(contextName: context.name, namespace: normalizedNamespace)],
           Self.isOverviewCacheFresh(cachedOverview, ttl: overviewSnapshotFreshnessTTL, reference: reference) {
            // Merge: `cachedOverview` supplies fresh cluster CPU/MEM; non-empty `ResourceStore` lists override counts and pod rows.
            let mergedPods = cached.pods.isEmpty ? cachedOverview.pods : cached.pods
            let mergedDeploymentsCount = cached.deployments.isEmpty ? cachedOverview.deploymentsCount : cached.deployments.count
            let mergedServicesCount = cached.services.isEmpty ? cachedOverview.servicesCount : cached.services.count
            let mergedIngressesCount = cached.ingresses.isEmpty ? cachedOverview.ingressesCount : cached.ingresses.count
            let mergedConfigMapsCount = cached.configMaps.isEmpty ? cachedOverview.configMapsCount : cached.configMaps.count
            let mergedCronJobsCount = cached.cronJobs.isEmpty ? cachedOverview.cronJobsCount : cached.cronJobs.count
            // Node rows are cluster-scoped RAM cache; keep node count tied to live RAM rows while CPU/MEM can use
            // the fresh per-context overview cache.
            let mergedNodesCount = cachedNodes.isEmpty ? cachedOverview.nodesCount : cachedNodes.count
            let mergedEvents = cached.events.isEmpty ? cachedOverview.events : cached.events
            state.setOverviewSnapshot(
                pods: mergedPods,
                deploymentsCount: mergedDeploymentsCount,
                servicesCount: mergedServicesCount,
                ingressesCount: mergedIngressesCount,
                configMapsCount: mergedConfigMapsCount,
                cronJobsCount: mergedCronJobsCount,
                nodesCount: mergedNodesCount,
                clusterCPUPercent: cachedOverview.clusterCPUPercent,
                clusterMemoryPercent: cachedOverview.clusterMemoryPercent,
                events: mergedEvents
            )
            return
        }

        state.setOverviewSnapshot(
            pods: cached.pods,
            deploymentsCount: cached.deployments.count,
            servicesCount: cached.services.count,
            ingressesCount: cached.ingresses.count,
            configMapsCount: cached.configMaps.count,
            cronJobsCount: cached.cronJobs.count,
            nodesCount: cachedNodes.count,
            clusterCPUPercent: state.overviewClusterCPUPercent,
            clusterMemoryPercent: state.overviewClusterMemoryPercent,
            events: cached.events
        )

        Task { [weak self] in
            guard let self else { return }
            guard let persisted = await self.overviewSnapshotPersistence.loadSnapshot(
                contextName: context.name,
                namespace: normalizedNamespace,
                maxAge: self.overviewDiskSnapshotFreshnessTTL
            ) else {
                return
            }
            self.applyPersistedOverviewSnapshotIfCurrent(
                contextName: context.name,
                namespace: normalizedNamespace,
                persisted: persisted
            )
        }
    }

    private func applyPersistedOverviewSnapshotIfCurrent(
        contextName: String,
        namespace: String,
        persisted: PersistedOverviewSnapshot
    ) {
        guard state.selectedContext?.name == contextName else { return }
        guard state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines) == namespace else { return }

        let key = Self.overviewCacheKey(contextName: contextName, namespace: namespace)
        if let newer = overviewSnapshotCache[key], persisted.fetchedAt < newer.fetchedAt {
            diagnostics.log(
                "applyPersistedOverviewSnapshotIfCurrent skipped older disk snapshot context=\(contextName) namespace=\(namespace)"
            )
            return
        }

        let now = Date()
        let entry = cachePersistedOverviewSnapshot(persisted, reference: now)
        guard Self.isOverviewCacheFresh(entry, ttl: overviewDiskSnapshotFreshnessTTL, reference: now) else { return }
        state.setOverviewSnapshot(
            pods: entry.pods,
            deploymentsCount: entry.deploymentsCount,
            servicesCount: entry.servicesCount,
            ingressesCount: entry.ingressesCount,
            configMapsCount: entry.configMapsCount,
            cronJobsCount: entry.cronJobsCount,
            nodesCount: entry.nodesCount,
            clusterCPUPercent: entry.clusterCPUPercent,
            clusterMemoryPercent: entry.clusterMemoryPercent,
            events: entry.events
        )
    }

    private var shouldLoadResourceDetailsForCurrentSection: Bool {
        switch state.selectedSection {
        case .workloads, .networking, .config, .storage, .rbac:
            return true
        case .overview, .events, .helm, .terminal:
            return false
        }
    }

    private func unwrap<T>(
        _ result: Result<T, Error>,
        label: String,
        fallback: T,
        warnings: inout [String]
    ) -> T {
        switch result {
        case let .success(value):
            return value
        case let .failure(error):
            if Self.isBenignCancellationError(error) {
                diagnostics.log("snapshot \(label) cancelled")
                return fallback
            }
            diagnostics.log("snapshot \(label) failed: \(error.localizedDescription)")
            warnings.append("\(label): \(error.localizedDescription)")
            return fallback
        }
    }

    private func captureSnapshotRead<T: Sendable>(
        operation: @Sendable @escaping () async throws -> T
    ) async -> Result<T, Error> {
        await Self.capture {
            try await snapshotRefreshConcurrencyLimiter.withPermit(operation)
        }
    }

    private nonisolated static func resourceListFamilies(forSnapshotWarnings warnings: [String]) -> Set<RuneResourceListFamily> {
        Set(
            warnings.compactMap { warning in
                guard let label = warning.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: true).first else {
                    return nil
                }
                return RuneResourceListFamily(snapshotWarningLabel: String(label))
            }
        )
    }

    private nonisolated static func isBenignCancellationError(_ error: Error) -> Bool {
        if error is CancellationError {
            return true
        }

        if case let RuneError.commandFailed(_, message) = error,
           isBenignCancellationText(message) {
            return true
        }

        return isBenignCancellationText(error.localizedDescription)
    }

    private nonisolated static func isBenignCancellationText(_ text: String) -> Bool {
        let normalized = text.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !normalized.isEmpty else { return false }
        return normalized.contains("cancelled") || normalized.contains("canceled")
    }

    private nonisolated static func capture<T: Sendable>(
        operation: @Sendable () async throws -> T
    ) async -> Result<T, Error> {
        do {
            return .success(try await operation())
        } catch {
            return .failure(error)
        }
    }

    private struct PodSortRecord {
        let pod: PodSummary
        let isFavorite: Bool
        let cpuMilli: Int?
        let memoryBytes: Int64?
        let ageSeconds: Int?
    }

    private func sortedPods(_ values: [PodSummary]) -> [PodSummary] {
        let records = values.map { pod in
            PodSortRecord(
                pod: pod,
                isFavorite: isFavoriteResource(kind: .pod, namespace: pod.namespace, name: pod.name),
                cpuMilli: cpuMilliValue(pod.cpuUsage),
                memoryBytes: memoryByteValue(pod.memoryUsage),
                ageSeconds: ageSeconds(pod.ageDescription)
            )
        }
        return stablySorted(records, by: podSortRecordComparator).map(\.pod)
    }

    func podComparator(_ lhs: PodSummary, _ rhs: PodSummary) -> Bool {
        podSortRecordComparator(
            PodSortRecord(
                pod: lhs,
                isFavorite: isFavoriteResource(kind: .pod, namespace: lhs.namespace, name: lhs.name),
                cpuMilli: cpuMilliValue(lhs.cpuUsage),
                memoryBytes: memoryByteValue(lhs.memoryUsage),
                ageSeconds: ageSeconds(lhs.ageDescription)
            ),
            PodSortRecord(
                pod: rhs,
                isFavorite: isFavoriteResource(kind: .pod, namespace: rhs.namespace, name: rhs.name),
                cpuMilli: cpuMilliValue(rhs.cpuUsage),
                memoryBytes: memoryByteValue(rhs.memoryUsage),
                ageSeconds: ageSeconds(rhs.ageDescription)
            )
        )
    }

    private func podSortRecordComparator(_ lhs: PodSortRecord, _ rhs: PodSortRecord) -> Bool {
        if lhs.isFavorite != rhs.isFavorite {
            return lhs.isFavorite && !rhs.isFavorite
        }

        let ascending = podSortAscending
        let identityOrder = podIdentityComparison(lhs.pod, rhs.pod)

        switch podSortColumn {
        case .name:
            return orderedBefore(
                caseInsensitiveComparison(lhs.pod.name, rhs.pod.name),
                ascending: ascending,
                tieBreak: identityOrder
            )
        case .status:
            return orderedBefore(
                caseInsensitiveComparison(lhs.pod.status, rhs.pod.status),
                ascending: ascending,
                tieBreak: identityOrder
            )
        case .restarts:
            return comparePodsMetric(
                ascending: ascending,
                lhsValue: lhs.pod.totalRestarts,
                rhsValue: rhs.pod.totalRestarts,
                tieBreak: identityOrder
            )
        case .cpu:
            return comparePodsOptionalMetric(
                ascending: ascending,
                lhsValue: lhs.cpuMilli,
                rhsValue: rhs.cpuMilli,
                tieBreak: identityOrder
            )
        case .memory:
            return comparePodsOptionalMetric(
                ascending: ascending,
                lhsValue: lhs.memoryBytes,
                rhsValue: rhs.memoryBytes,
                tieBreak: identityOrder
            )
        case .age:
            return comparePodsOptionalMetric(
                ascending: ascending,
                lhsValue: lhs.ageSeconds,
                rhsValue: rhs.ageSeconds,
                tieBreak: identityOrder
            )
        }
    }

    func deploymentComparator(_ lhs: DeploymentSummary, _ rhs: DeploymentSummary) -> Bool {
        if let favoriteOrder = resourceFavoriteOrder(
            kind: .deployment,
            lhsNamespace: lhs.namespace,
            lhsName: lhs.name,
            rhsNamespace: rhs.namespace,
            rhsName: rhs.name
        ) {
            return favoriteOrder
        }

        let ascending = deploymentSortAscending
        let identityOrder = deploymentIdentityComparison(lhs, rhs)

        switch deploymentSortColumn {
        case .name:
            return orderedBefore(
                caseInsensitiveComparison(lhs.name, rhs.name),
                ascending: ascending,
                tieBreak: identityOrder
            )
        case .replicas:
            let lhsRatio = replicaReadinessRatio(lhs)
            let rhsRatio = replicaReadinessRatio(rhs)
            if lhsRatio != rhsRatio {
                return orderedBefore(comparableComparison(lhsRatio, rhsRatio), ascending: ascending)
            }
            if lhs.readyReplicas != rhs.readyReplicas {
                return orderedBefore(comparableComparison(lhs.readyReplicas, rhs.readyReplicas), ascending: ascending)
            }
            if lhs.desiredReplicas != rhs.desiredReplicas {
                return orderedBefore(comparableComparison(lhs.desiredReplicas, rhs.desiredReplicas), ascending: ascending)
            }
            return orderedBefore(identityOrder)
        }
    }

    func serviceComparator(_ lhs: ServiceSummary, _ rhs: ServiceSummary) -> Bool {
        if let favoriteOrder = resourceFavoriteOrder(
            kind: .service,
            lhsNamespace: lhs.namespace,
            lhsName: lhs.name,
            rhsNamespace: rhs.namespace,
            rhsName: rhs.name
        ) {
            return favoriteOrder
        }

        let ascending = serviceSortAscending
        let identityOrder = serviceIdentityComparison(lhs, rhs)

        let selectedOrder: ComparisonResult
        switch serviceSortColumn {
        case .name:
            selectedOrder = caseInsensitiveComparison(lhs.name, rhs.name)
        case .type:
            selectedOrder = caseInsensitiveComparison(lhs.type, rhs.type)
        case .clusterIP:
            selectedOrder = ipv4Comparison(lhs.clusterIP, rhs.clusterIP)
                ?? standardComparison(lhs.clusterIP, rhs.clusterIP)
        }

        return orderedBefore(selectedOrder, ascending: ascending, tieBreak: identityOrder)
    }

    private func ipv4Comparison(_ lhs: String, _ rhs: String) -> ComparisonResult? {
        let left = lhs.split(separator: ".").compactMap { Int($0) }
        let right = rhs.split(separator: ".").compactMap { Int($0) }
        guard left.count == 4, right.count == 4 else { return nil }
        for (leftPart, rightPart) in zip(left, right) where leftPart != rightPart {
            return leftPart < rightPart ? .orderedAscending : .orderedDescending
        }
        return .orderedSame
    }

    private func genericResourceSorted(_ values: [ClusterResourceSummary]) -> [ClusterResourceSummary] {
        let records = values.map { resource in
            (
                resource: resource,
                isFavorite: isFavoriteResource(kind: resource.kind, namespace: resource.namespace, name: resource.name)
            )
        }
        return stablySorted(records) { lhs, rhs in
            if lhs.isFavorite != rhs.isFavorite {
                return lhs.isFavorite && !rhs.isFavorite
            }
            return genericResourceComparator(lhs.resource, rhs.resource)
        }
            .map(\.resource)
    }

    func genericResourceComparator(_ lhs: ClusterResourceSummary, _ rhs: ClusterResourceSummary) -> Bool {
        let ascending = genericResourceSortAscending
        let identityOrder = clusterResourceIdentityComparison(lhs, rhs)

        let selectedOrder: ComparisonResult
        switch genericResourceSortColumn {
        case .name:
            selectedOrder = caseInsensitiveComparison(lhs.name, rhs.name)
        case .primary:
            if let primaryOrder = numericPrefixOrder(lhs.primaryText, rhs.primaryText), primaryOrder != .orderedSame {
                selectedOrder = primaryOrder
            } else {
                selectedOrder = standardComparison(lhs.primaryText, rhs.primaryText)
            }
        case .secondary:
            selectedOrder = standardComparison(lhs.secondaryText, rhs.secondaryText)
        case .namespace:
            let lhsNamespace = lhs.namespace ?? ""
            let rhsNamespace = rhs.namespace ?? ""
            selectedOrder = caseInsensitiveComparison(lhsNamespace, rhsNamespace)
        }

        return orderedBefore(selectedOrder, ascending: ascending, tieBreak: identityOrder)
    }

    func helmReleaseComparator(_ lhs: HelmReleaseSummary, _ rhs: HelmReleaseSummary) -> Bool {
        let ascending = helmReleaseSortAscending
        let identityOrder = helmReleaseIdentityComparison(lhs, rhs)

        let selectedOrder: ComparisonResult
        switch helmReleaseSortColumn {
        case .name:
            selectedOrder = caseInsensitiveComparison(lhs.name, rhs.name)
        case .status:
            selectedOrder = caseInsensitiveComparison(lhs.status, rhs.status)
        case .namespace:
            selectedOrder = caseInsensitiveComparison(lhs.namespace, rhs.namespace)
        case .revision:
            selectedOrder = comparableComparison(lhs.revision, rhs.revision)
        case .chart:
            selectedOrder = standardComparison(lhs.chart, rhs.chart)
        case .appVersion:
            selectedOrder = standardComparison(lhs.appVersion, rhs.appVersion)
        }

        return orderedBefore(selectedOrder, ascending: ascending, tieBreak: identityOrder)
    }

    func eventComparator(_ lhs: EventSummary, _ rhs: EventSummary) -> Bool {
        let ascending = eventSortAscending
        let identityOrder = eventIdentityComparison(lhs, rhs)

        let selectedOrder: ComparisonResult
        switch eventSortColumn {
        case .reason:
            selectedOrder = caseInsensitiveComparison(lhs.reason, rhs.reason)
        case .type:
            selectedOrder = caseInsensitiveComparison(lhs.type, rhs.type)
        case .object:
            selectedOrder = standardComparison(lhs.objectName, rhs.objectName)
        case .namespace:
            let lhsNamespace = lhs.involvedNamespace ?? ""
            let rhsNamespace = rhs.involvedNamespace ?? ""
            selectedOrder = caseInsensitiveComparison(lhsNamespace, rhsNamespace)
        case .lastSeen:
            let lhsTimestamp = lhs.lastTimestamp ?? ""
            let rhsTimestamp = rhs.lastTimestamp ?? ""
            selectedOrder = standardComparison(lhsTimestamp, rhsTimestamp)
        }

        return orderedBefore(selectedOrder, ascending: ascending, tieBreak: identityOrder)
    }

    private func numericPrefixOrder(_ lhs: String, _ rhs: String) -> ComparisonResult? {
        guard let lhsNumber = leadingInteger(lhs), let rhsNumber = leadingInteger(rhs) else { return nil }
        if lhsNumber == rhsNumber { return .orderedSame }
        return lhsNumber < rhsNumber ? .orderedAscending : .orderedDescending
    }

    private func leadingInteger(_ value: String) -> Int? {
        let digits = value.trimmingCharacters(in: .whitespacesAndNewlines).prefix { $0.isNumber }
        guard !digits.isEmpty else { return nil }
        return Int(digits)
    }

    private func replicaReadinessRatio(_ deployment: DeploymentSummary) -> Double {
        guard deployment.desiredReplicas > 0 else {
            return deployment.readyReplicas > 0 ? 1 : 0
        }
        return Double(deployment.readyReplicas) / Double(deployment.desiredReplicas)
    }

    private func orderedBefore(
        _ comparison: ComparisonResult,
        ascending: Bool = true,
        tieBreak: ComparisonResult = .orderedSame
    ) -> Bool {
        if comparison == .orderedSame {
            // Keep identity tie-breaks canonical so changing direction does not reshuffle equal values.
            return tieBreak == .orderedAscending
        }
        return ascending ? comparison == .orderedAscending : comparison == .orderedDescending
    }

    private func comparableComparison<T: Comparable>(_ lhs: T, _ rhs: T) -> ComparisonResult {
        if lhs == rhs { return .orderedSame }
        return lhs < rhs ? .orderedAscending : .orderedDescending
    }

    private func deterministicStringComparison(_ lhs: String, _ rhs: String) -> ComparisonResult {
        if lhs == rhs { return .orderedSame }
        return lhs < rhs ? .orderedAscending : .orderedDescending
    }

    private func caseInsensitiveComparison(_ lhs: String, _ rhs: String) -> ComparisonResult {
        let comparison = lhs.localizedCaseInsensitiveCompare(rhs)
        guard comparison == .orderedSame else { return comparison }
        return deterministicStringComparison(lhs, rhs)
    }

    private func standardComparison(_ lhs: String, _ rhs: String) -> ComparisonResult {
        let comparison = lhs.localizedStandardCompare(rhs)
        guard comparison == .orderedSame else { return comparison }
        return deterministicStringComparison(lhs, rhs)
    }

    private func firstNonMatchingComparison(_ comparisons: [ComparisonResult]) -> ComparisonResult {
        comparisons.first(where: { $0 != .orderedSame }) ?? .orderedSame
    }

    private func podIdentityComparison(_ lhs: PodSummary, _ rhs: PodSummary) -> ComparisonResult {
        firstNonMatchingComparison([
            caseInsensitiveComparison(lhs.name, rhs.name),
            caseInsensitiveComparison(lhs.namespace, rhs.namespace),
            deterministicStringComparison(lhs.id, rhs.id)
        ])
    }

    private func deploymentIdentityComparison(_ lhs: DeploymentSummary, _ rhs: DeploymentSummary) -> ComparisonResult {
        firstNonMatchingComparison([
            caseInsensitiveComparison(lhs.name, rhs.name),
            caseInsensitiveComparison(lhs.namespace, rhs.namespace),
            deterministicStringComparison(lhs.id, rhs.id)
        ])
    }

    private func serviceIdentityComparison(_ lhs: ServiceSummary, _ rhs: ServiceSummary) -> ComparisonResult {
        firstNonMatchingComparison([
            caseInsensitiveComparison(lhs.name, rhs.name),
            caseInsensitiveComparison(lhs.namespace, rhs.namespace),
            deterministicStringComparison(lhs.id, rhs.id)
        ])
    }

    private func clusterResourceIdentityComparison(
        _ lhs: ClusterResourceSummary,
        _ rhs: ClusterResourceSummary
    ) -> ComparisonResult {
        firstNonMatchingComparison([
            caseInsensitiveComparison(lhs.name, rhs.name),
            caseInsensitiveComparison(lhs.namespace ?? "", rhs.namespace ?? ""),
            deterministicStringComparison(lhs.id, rhs.id),
            deterministicStringComparison(lhs.kind.rawValue, rhs.kind.rawValue),
            standardComparison(lhs.primaryText, rhs.primaryText),
            standardComparison(lhs.secondaryText, rhs.secondaryText)
        ])
    }

    private func helmReleaseIdentityComparison(_ lhs: HelmReleaseSummary, _ rhs: HelmReleaseSummary) -> ComparisonResult {
        firstNonMatchingComparison([
            caseInsensitiveComparison(lhs.name, rhs.name),
            caseInsensitiveComparison(lhs.namespace, rhs.namespace),
            deterministicStringComparison(lhs.id, rhs.id),
            comparableComparison(lhs.revision, rhs.revision),
            caseInsensitiveComparison(lhs.status, rhs.status),
            standardComparison(lhs.chart, rhs.chart),
            standardComparison(lhs.appVersion, rhs.appVersion),
            standardComparison(lhs.updated, rhs.updated)
        ])
    }

    private func eventIdentityComparison(_ lhs: EventSummary, _ rhs: EventSummary) -> ComparisonResult {
        firstNonMatchingComparison([
            standardComparison(lhs.objectName, rhs.objectName),
            caseInsensitiveComparison(lhs.involvedNamespace ?? "", rhs.involvedNamespace ?? ""),
            caseInsensitiveComparison(lhs.involvedKind ?? "", rhs.involvedKind ?? ""),
            caseInsensitiveComparison(lhs.reason, rhs.reason),
            caseInsensitiveComparison(lhs.type, rhs.type),
            standardComparison(lhs.lastTimestamp ?? "", rhs.lastTimestamp ?? ""),
            deterministicStringComparison(lhs.message, rhs.message)
        ])
    }

    private func operatorResourceIdentityComparison(
        _ lhs: OperatorResourceSummary,
        _ rhs: OperatorResourceSummary
    ) -> ComparisonResult {
        let lhsPrinterColumns = lhs.printerColumns.map { "\($0.title)|\($0.value)" }.joined(separator: "|")
        let rhsPrinterColumns = rhs.printerColumns.map { "\($0.title)|\($0.value)" }.joined(separator: "|")
        return firstNonMatchingComparison([
            caseInsensitiveComparison(lhs.family, rhs.family),
            caseInsensitiveComparison(lhs.kind, rhs.kind),
            caseInsensitiveComparison(lhs.name, rhs.name),
            caseInsensitiveComparison(lhs.namespace ?? "", rhs.namespace ?? ""),
            deterministicStringComparison(lhs.id, rhs.id),
            standardComparison(lhs.apiPath, rhs.apiPath),
            caseInsensitiveComparison(lhs.status, rhs.status),
            deterministicStringComparison(lhs.message, rhs.message),
            deterministicStringComparison(lhsPrinterColumns, rhsPrinterColumns)
        ])
    }

    /// Missing metrics sort last regardless of ascending/descending direction.
    private func comparePodsOptionalMetric<T: Comparable>(
        ascending: Bool,
        lhsValue: T?,
        rhsValue: T?,
        tieBreak: ComparisonResult
    ) -> Bool {
        switch (lhsValue, rhsValue) {
        case let (l?, r?):
            if l != r {
                return orderedBefore(comparableComparison(l, r), ascending: ascending)
            }
            return orderedBefore(tieBreak)
        case (_?, nil):
            return true
        case (nil, _?):
            return false
        case (nil, nil):
            return orderedBefore(tieBreak)
        }
    }

    private func comparePodsMetric(
        ascending: Bool,
        lhsValue: Int,
        rhsValue: Int,
        tieBreak: ComparisonResult
    ) -> Bool {
        if lhsValue != rhsValue {
            return orderedBefore(comparableComparison(lhsValue, rhsValue), ascending: ascending)
        }
        return orderedBefore(tieBreak)
    }

    private func cpuMilliValue(_ raw: String?) -> Int? {
        guard let raw else { return nil }
        let token = raw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !token.isEmpty, token != "—", token != "-" else { return nil }
        if token.hasSuffix("m"), let milli = Int(token.dropLast()) {
            return milli
        }
        if let cores = Double(token) {
            return Int((cores * 1000.0).rounded())
        }
        return nil
    }

    private func memoryByteValue(_ raw: String?) -> Int64? {
        guard let raw else { return nil }
        let token = raw.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        guard !token.isEmpty, token != "—", token != "-" else { return nil }

        let suffixes: [(String, Double)] = [
            ("KI", 1024),
            ("MI", 1024 * 1024),
            ("GI", 1024 * 1024 * 1024),
            ("TI", 1024 * 1024 * 1024 * 1024),
            ("K", 1_000),
            ("M", 1_000_000),
            ("G", 1_000_000_000)
        ]

        for (suffix, multiplier) in suffixes {
            if token.hasSuffix(suffix) {
                let number = String(token.dropLast(suffix.count))
                guard let value = Double(number) else { return nil }
                return Int64((value * multiplier).rounded())
            }
        }

        if let rawInt = Int64(token) {
            return rawInt
        }

        return nil
    }

    private func ageSeconds(_ raw: String?) -> Int? {
        guard let raw else { return nil }
        let token = raw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !token.isEmpty, token != "—", token != "-" else { return nil }

        var total = 0
        var digits = ""
        for character in token {
            if character.isNumber {
                digits.append(character)
                continue
            }

            guard let value = Int(digits) else {
                return nil
            }
            digits.removeAll(keepingCapacity: true)

            switch character {
            case "s": total += value
            case "m": total += value * 60
            case "h": total += value * 3600
            case "d": total += value * 86_400
            case "w": total += value * 604_800
            case "y": total += value * 31_536_000
            default: return nil
            }
        }

        if !digits.isEmpty, let trailing = Int(digits) {
            total += trailing
        }

        return total == 0 ? nil : total
    }

    private func stablySorted<T>(
        _ values: [T],
        by areInIncreasingOrder: (T, T) -> Bool
    ) -> [T] {
        values.enumerated()
            .sorted { lhs, rhs in
                if areInIncreasingOrder(lhs.element, rhs.element) { return true }
                if areInIncreasingOrder(rhs.element, lhs.element) { return false }
                return lhs.offset < rhs.offset
            }
            .map(\.element)
    }

    private func filtered<T>(_ values: [T], text: (T) -> String) -> [T] {
        let query = state.resourceSearchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else {
            return values
        }

        return values.filter { value in
            matches(text(value), query: query)
        }
    }

    private func favoriteSorted(_ values: [ClusterResourceSummary]) -> [ClusterResourceSummary] {
        values.sorted { lhs, rhs in
            let lhsFavorite = isFavoriteResource(kind: lhs.kind, namespace: lhs.namespace, name: lhs.name)
            let rhsFavorite = isFavoriteResource(kind: rhs.kind, namespace: rhs.namespace, name: rhs.name)
            if lhsFavorite != rhsFavorite {
                return lhsFavorite && !rhsFavorite
            }
            return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
        }
    }

    private func favoriteSorted<T>(
        _ values: [T],
        kind: KubeResourceKind,
        name: (T) -> String
    ) -> [T] {
        values.sorted { lhs, rhs in
            let lhsFavorite = isFavoriteResource(kind: kind, namespace: state.selectedNamespace, name: name(lhs))
            let rhsFavorite = isFavoriteResource(kind: kind, namespace: state.selectedNamespace, name: name(rhs))
            if lhsFavorite != rhsFavorite {
                return lhsFavorite && !rhsFavorite
            }
            return name(lhs).localizedCaseInsensitiveCompare(name(rhs)) == .orderedAscending
        }
    }

    private func operatorResourceSorted(_ values: [OperatorResourceSummary]) -> [OperatorResourceSummary] {
        stablySorted(values, by: operatorResourceComparator)
    }

    public static func isGitOpsOperatorResource(_ resource: OperatorResourceSummary) -> Bool {
        isFluxOperatorResource(resource) || isArgoCDOperatorResource(resource)
    }

    public static func isFluxOperatorResource(_ resource: OperatorResourceSummary) -> Bool {
        let family = resource.family.lowercased()
        let kind = resource.kind.lowercased()
        return family.contains("flux")
            || kind.contains("kustomization")
            || kind.contains("helmrelease")
            || kind.contains("gitrepositor")
    }

    public static func isArgoCDOperatorResource(_ resource: OperatorResourceSummary) -> Bool {
        let family = resource.family.lowercased()
        let kind = resource.kind.lowercased()
        return family.contains("argo")
            || kind.contains("application")
            || kind.contains("appproject")
    }

    public static func isUnhealthyGitOpsOperatorResource(_ resource: OperatorResourceSummary) -> Bool {
        guard isGitOpsOperatorResource(resource) else { return false }
        let text = ([resource.status, resource.message] + resource.printerColumns.flatMap { [$0.title, $0.value] })
            .joined(separator: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
        guard !text.isEmpty else { return false }
        return text.contains("ready false")
            || text.contains("ready:false")
            || text.contains("not ready")
            || text.contains("failed")
            || text.contains("error")
            || text.contains("synced false")
            || text.contains("sync false")
            || text.contains("outofsync")
            || text.contains("out of sync")
            || text.contains("degraded")
            || (text.contains("reconcil") && text.contains("failed"))
            || text.contains("stalled")
            || text.contains("suspended")
    }

    private func resourceFavoriteOrder(
        kind: KubeResourceKind,
        lhsNamespace: String?,
        lhsName: String,
        rhsNamespace: String?,
        rhsName: String
    ) -> Bool? {
        let lhsFavorite = isFavoriteResource(kind: kind, namespace: lhsNamespace, name: lhsName)
        let rhsFavorite = isFavoriteResource(kind: kind, namespace: rhsNamespace, name: rhsName)
        guard lhsFavorite != rhsFavorite else { return nil }
        return lhsFavorite && !rhsFavorite
    }

    func operatorResourceComparator(_ lhs: OperatorResourceSummary, _ rhs: OperatorResourceSummary) -> Bool {
        let lhsFavorite = isFavoriteOperatorResource(lhs)
        let rhsFavorite = isFavoriteOperatorResource(rhs)
        if lhsFavorite != rhsFavorite {
            return lhsFavorite && !rhsFavorite
        }

        let ascending = operatorResourceSortAscending
        let identityOrder = operatorResourceIdentityComparison(lhs, rhs)

        let selectedOrder: ComparisonResult
        switch operatorResourceSortColumn {
        case .family:
            selectedOrder = caseInsensitiveComparison(lhs.family, rhs.family)
        case .kind:
            selectedOrder = caseInsensitiveComparison(lhs.kind, rhs.kind)
        case .name:
            selectedOrder = caseInsensitiveComparison(lhs.name, rhs.name)
        case .namespace:
            let lhsNamespace = lhs.namespace ?? ""
            let rhsNamespace = rhs.namespace ?? ""
            selectedOrder = caseInsensitiveComparison(lhsNamespace, rhsNamespace)
        case .status:
            selectedOrder = caseInsensitiveComparison(lhs.status, rhs.status)
        case .apiPath:
            selectedOrder = standardComparison(lhs.apiPath, rhs.apiPath)
        }

        return orderedBefore(selectedOrder, ascending: ascending, tieBreak: identityOrder)
    }

    private func favoriteResourceID(kind: KubeResourceKind, namespace: String?, name: String) -> String? {
        guard let contextName = state.selectedContext?.name.trimmingCharacters(in: .whitespacesAndNewlines),
              !contextName.isEmpty
        else { return nil }
        let normalizedName = name.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedName.isEmpty else { return nil }
        let normalizedNamespace = (namespace ?? (kind.isNamespaced ? state.selectedNamespace : "_cluster"))
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return [
            contextName,
            kind.rawValue,
            normalizedNamespace.isEmpty ? "_cluster" : normalizedNamespace,
            normalizedName
        ].joined(separator: "|")
    }

    private func favoriteNamespaceID(_ namespace: String) -> String? {
        guard let contextName = state.selectedContext?.name.trimmingCharacters(in: .whitespacesAndNewlines),
              !contextName.isEmpty
        else { return nil }
        let normalizedNamespace = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedNamespace.isEmpty else { return nil }
        return [contextName, "namespace", normalizedNamespace].joined(separator: "|")
    }

    private func productionContextID(for context: KubeContext) -> String? {
        let normalized = context.name.trimmingCharacters(in: .whitespacesAndNewlines)
        return normalized.isEmpty ? nil : normalized
    }

    private func favoriteOperatorResourceID(_ resource: OperatorResourceSummary) -> String {
        let contextName = state.selectedContext?.name.trimmingCharacters(in: .whitespacesAndNewlines) ?? "_context"
        let namespace = resource.namespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? "_cluster"
        return [
            contextName.isEmpty ? "_context" : contextName,
            "crd",
            resource.apiPath,
            resource.kind,
            namespace.isEmpty ? "_cluster" : namespace,
            resource.name
        ].joined(separator: "|")
    }

    private func matches(_ text: String, query: String) -> Bool {
        matches(text, tokens: commandPaletteMatchTokens(for: query))
    }

    private struct CommandPaletteMatchToken {
        let normalized: String
        let asciiBytes: [UInt8]?

        init(_ value: Substring) {
            normalized = String(value)
            let bytes = Array(value.utf8)
            asciiBytes = bytes.allSatisfy { $0 < 0x80 } ? bytes : nil
        }
    }

    private func commandPaletteMatchTokens(for query: String) -> [CommandPaletteMatchToken] {
        query.lowercased().split(whereSeparator: \.isWhitespace).map(CommandPaletteMatchToken.init)
    }

    private func matches(_ text: String, tokens: [CommandPaletteMatchToken]) -> Bool {
        guard !tokens.isEmpty else { return true }
        var normalizedText: String?
        return tokens.allSatisfy { token in
            if let asciiMatch = Self.asciiCaseInsensitiveContains(text, token: token) {
                return asciiMatch
            }
            if normalizedText == nil {
                normalizedText = text.lowercased()
            }
            return normalizedText?.contains(token.normalized) == true
        }
    }

    /// Returns `nil` when Unicode case folding is required. Kubernetes identifiers and
    /// aliases are overwhelmingly ASCII, so their hot search path can compare UTF-8 bytes
    /// without allocating a lowercased copy for every candidate row.
    private static func asciiCaseInsensitiveContains(_ text: String, token: CommandPaletteMatchToken) -> Bool? {
        guard let needle = token.asciiBytes else { return nil }
        guard !needle.isEmpty else { return true }

        if let contiguousResult = text.utf8.withContiguousStorageIfAvailable({ haystack in
            asciiCaseInsensitiveContains(haystack, needle: needle)
        }) {
            return contiguousResult
        }

        let haystack = Array(text.utf8)
        return haystack.withUnsafeBufferPointer {
            asciiCaseInsensitiveContains($0, needle: needle)
        }
    }

    private static func asciiCaseInsensitiveContains(
        _ haystack: UnsafeBufferPointer<UInt8>,
        needle: [UInt8]
    ) -> Bool? {
        guard haystack.allSatisfy({ $0 < 0x80 }) else { return nil }
        guard needle.count <= haystack.count else { return false }
        let finalStart = haystack.count - needle.count
        for start in 0...finalStart {
            var offset = 0
            while offset < needle.count {
                let haystackByte = lowercasedASCIIByte(haystack[start + offset])
                let needleByte = lowercasedASCIIByte(needle[offset])
                guard haystackByte == needleByte else { break }
                offset += 1
            }
            if offset == needle.count {
                return true
            }
        }
        return false
    }

    @inline(__always)
    private static func lowercasedASCIIByte(_ byte: UInt8) -> UInt8 {
        (0x41...0x5A).contains(byte) ? byte + 0x20 : byte
    }

    private func commandPaletteBaseCommandItems() -> [CommandPaletteItem] {
        var items = [
            CommandPaletteItem(
                id: "command:import",
                title: "Import kubeconfig…",
                subtitle: "Open a native file picker and add kubeconfig files",
                symbolName: "square.and.arrow.down",
                action: .importKubeConfig
            ),
            CommandPaletteItem(
                id: "command:reload",
                title: "Reload cluster data",
                subtitle: "Refresh the current context and section",
                symbolName: "arrow.clockwise",
                action: .reload
            ),
            CommandPaletteItem(
                id: "command:save-logs",
                title: "Save Logs",
                subtitle: "Save the current pod logs or unified logs",
                symbolName: "square.and.arrow.down",
                action: .saveLogs
            ),
            CommandPaletteItem(
                id: "command:readonly:on",
                title: "Enable read-only mode",
                subtitle: "Block write actions across the app",
                symbolName: "lock",
                action: .readOnly(true)
            ),
            CommandPaletteItem(
                id: "command:readonly:off",
                title: "Disable read-only mode",
                subtitle: "Allow write actions again",
                symbolName: "lock.open",
                action: .readOnly(false)
            )
        ]
        if let deleteItem = commandPaletteDeleteSelectedItem(alias: ":delete") {
            items.append(deleteItem)
        }
        return items
    }

    private func commandPaletteGlobalItems(query: String?) -> [CommandPaletteItem] {
        let limit = 160
        let queryTokens = query.map(commandPaletteMatchTokens(for:))
        var items: [CommandPaletteItem] = []
        items.reserveCapacity(limit)

        func append(_ item: CommandPaletteItem) {
            guard items.count < limit else { return }
            if let queryTokens, !matches("\(item.title) \(item.subtitle)", tokens: queryTokens) {
                return
            }
            items.append(item)
        }

        func append(contentsOf candidates: [CommandPaletteItem]) {
            for item in candidates {
                guard items.count < limit else { return }
                append(item)
            }
        }

        append(contentsOf: commandPaletteBaseCommandItems())
        append(contentsOf: savedWorkspaceCommandItems(query: ""))

        for section in RuneSection.allCases {
            guard items.count < limit else { return items }
            append(CommandPaletteItem(
                id: "section:\(section.rawValue)",
                title: section.title,
                subtitle: "Switch section",
                symbolName: section.symbolName,
                action: .section(section)
            ))
        }

        for context in contextsIncludingEnabledDemo(state.contexts) {
            guard items.count < limit else { return items }
            append(commandPaletteContextItem(context, idPrefix: "context", subtitlePrefix: "Switch context"))
        }

        for namespace in state.namespaces {
            guard items.count < limit else { return items }
            append(CommandPaletteItem(
                id: "namespace:\(namespace)",
                title: namespace,
                subtitle: "Switch namespace",
                symbolName: "square.3.layers.3d",
                action: .namespace(namespace)
            ))
        }

        for pod in state.pods.prefix(40) {
            guard items.count < limit else { return items }
            append(CommandPaletteItem(
                id: "pod:\(pod.id)",
                title: pod.name,
                subtitle: "Open pod",
                symbolName: "cube.box",
                action: .pod(pod)
            ))
        }

        for deployment in state.deployments.prefix(40) {
            guard items.count < limit else { return items }
            append(CommandPaletteItem(
                id: "deployment:\(deployment.id)",
                title: deployment.name,
                subtitle: "Open deployment",
                symbolName: "shippingbox",
                action: .deployment(deployment)
            ))
        }

        for service in state.services.prefix(40) {
            guard items.count < limit else { return items }
            append(CommandPaletteItem(
                id: "service:\(service.id)",
                title: service.name,
                subtitle: "Open service",
                symbolName: "point.3.connected.trianglepath.dotted",
                action: .service(service)
            ))
        }

        for endpoint in state.endpoints.prefix(40) {
            guard items.count < limit else { return items }
            append(CommandPaletteItem(
                id: "endpoint:\(endpoint.id)",
                title: endpoint.name,
                subtitle: "Open endpoints",
                symbolName: "point.3.connected.trianglepath.dotted",
                action: .clusterResource(endpoint)
            ))
        }

        for serviceAccount in state.serviceAccounts.prefix(40) {
            guard items.count < limit else { return items }
            append(CommandPaletteItem(
                id: "serviceaccount:\(serviceAccount.id)",
                title: serviceAccount.name,
                subtitle: "Open service account",
                symbolName: "person.crop.circle.badge.checkmark",
                action: .clusterResource(serviceAccount)
            ))
        }

        for release in state.helmReleases.prefix(40) {
            guard items.count < limit else { return items }
            append(CommandPaletteItem(
                id: "helm:\(release.id)",
                title: release.name,
                subtitle: "Open Helm release • \(release.namespace)",
                symbolName: "ferry",
                action: .helmRelease(release)
            ))
        }

        for event in state.events.prefix(40) {
            guard items.count < limit else { return items }
            append(CommandPaletteItem(
                id: "event:\(event.id)",
                title: "\(event.reason) (\(event.type))",
                subtitle: event.objectName,
                symbolName: "bolt.badge.clock",
                action: .event(event)
            ))
        }

        return items
    }

    private func summaryText(for resource: ClusterResourceSummary) -> String {
        "\(resource.name) \(resource.namespace ?? "") \(resource.primaryText) \(resource.secondaryText)"
    }

    /// When a resource search returns no rows, still show a navigation row so the user can open the target section from the command palette.
    private func commandPaletteResourceRowsOrNavigate(rows: [CommandPaletteItem], navigate: CommandPaletteItem) -> [CommandPaletteItem] {
        if rows.isEmpty {
            [navigate]
        } else {
            rows
        }
    }

    private func commandPaletteContextItem(
        _ context: KubeContext,
        idPrefix: String,
        subtitlePrefix: String
    ) -> CommandPaletteItem {
        let metadata = cachedCommandPaletteContextMetadata(for: context.name)
        let displayName = metadata?.alias ?? context.name
        let secondaryParts = [
            metadata?.group,
            metadata?.tags.isEmpty == false ? metadata?.tags.joined(separator: ", ") : nil
        ].compactMap { $0 }
        let secondary = secondaryParts.isEmpty ? nil : secondaryParts.joined(separator: " • ")
        let subtitleParts = [
            subtitlePrefix,
            displayName == context.name ? nil : context.name,
            secondary
        ].compactMap { $0 }
        return CommandPaletteItem(
            id: "\(idPrefix):\(context.id)",
            title: displayName,
            subtitle: subtitleParts.joined(separator: " • "),
            symbolName: state.isFavorite(context) ? "star.fill" : (metadata?.iconName ?? "network"),
            action: .context(context)
        )
    }

    private func cachedCommandPaletteContextMetadata(for contextName: String) -> ContextDisplayMetadata? {
        if let metadata = commandPaletteContextMetadataCache[contextName] {
            return metadata
        }
        guard !commandPaletteContextsWithoutMetadata.contains(contextName) else { return nil }

        if let metadata = contextPreferences.loadContextDisplayMetadata(for: contextName) {
            commandPaletteContextMetadataCache[contextName] = metadata
            return metadata
        }
        commandPaletteContextsWithoutMetadata.insert(contextName)
        return nil
    }

    private func savedWorkspaceCommandItems(query: String) -> [CommandPaletteItem] {
        let trimmed = query.trimmingCharacters(in: .whitespacesAndNewlines)
        return Array(
            savedWorkspaces
                .filter { workspace in
                    trimmed.isEmpty || matches(savedWorkspaceSearchText(workspace), query: trimmed)
                }
                .sorted { lhs, rhs in
                    if lhs.isFavorite != rhs.isFavorite {
                        return lhs.isFavorite && !rhs.isFavorite
                    }
                    return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
                }
                .prefix(40)
                .map { workspace in
                    CommandPaletteItem(
                        id: "workspace:\(workspace.id)",
                        title: workspace.name,
                        subtitle: savedWorkspaceSubtitle(workspace),
                        symbolName: workspace.isFavorite ? "star.fill" : "rectangle.stack.badge.play",
                        action: .savedWorkspace(workspace)
                    )
                }
        )
    }

    private func savedWorkspaceFavoriteCommandItems(query: String) -> [CommandPaletteItem] {
        let trimmed = query.trimmingCharacters(in: .whitespacesAndNewlines)
        return Array(
            savedWorkspaces
                .filter { workspace in
                    trimmed.isEmpty || matches(savedWorkspaceSearchText(workspace), query: trimmed)
                }
                .sorted { lhs, rhs in
                    if lhs.isFavorite != rhs.isFavorite {
                        return !lhs.isFavorite && rhs.isFavorite
                    }
                    return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
                }
                .prefix(40)
                .map { workspace in
                    CommandPaletteItem(
                        id: "workspace-favorite:\(workspace.id)",
                        title: workspace.isFavorite ? "Unfavorite Workspace: \(workspace.name)" : "Favorite Workspace: \(workspace.name)",
                        subtitle: savedWorkspaceSubtitle(workspace),
                        symbolName: workspace.isFavorite ? "star.slash" : "star",
                        action: .toggleSavedWorkspaceFavorite(workspace)
                    )
                }
        )
    }

    private func savedWorkspaceSubtitle(_ workspace: SavedWorkspaceSnapshot) -> String {
        var parts = [workspace.isFavorite ? "Favorite workspace" : "Workspace"]
        if let contextName = workspace.contextName, !contextName.isEmpty {
            let context = KubeContext(name: contextName)
            let displayName = contextDisplayName(for: context)
            if displayName == contextName {
                parts.append(contextName)
            } else {
                parts.append("\(displayName) (\(contextName))")
            }
            if let secondary = contextSecondaryDisplayText(for: context) {
                parts.append(secondary)
            }
        }
        parts.append(workspace.namespace)
        parts.append(workspace.section.title)
        if let resourceName = workspace.resourceName {
            parts.append(resourceName)
        } else {
            parts.append(workspace.workloadKind.title)
        }
        return parts.joined(separator: " • ")
    }

    private func savedWorkspaceSearchText(_ workspace: SavedWorkspaceSnapshot) -> String {
        var parts = [workspace.name, savedWorkspaceSubtitle(workspace)]
        if let contextName = workspace.contextName, !contextName.isEmpty {
            parts.append(contentsOf: contextSearchTokens(for: KubeContext(name: contextName)))
        }
        return parts.joined(separator: " ")
    }

    private func saveWorkspaceCommandItem(name rawName: String, alias: String) -> CommandPaletteItem {
        let trimmedName = rawName.trimmingCharacters(in: .whitespacesAndNewlines)
        let name = trimmedName.isEmpty ? suggestedSavedWorkspaceName() : trimmedName
        return CommandPaletteItem(
            id: "cmd:save-workspace:\(name)",
            title: "Save Workspace: \(name)",
            subtitle: "Save current context, namespace, view, selection, logs, and layout • `\(alias)`",
            symbolName: "rectangle.stack.badge.plus",
            action: .saveWorkspace(name)
        )
    }

    private func suggestedSavedWorkspaceName() -> String {
        var parts: [String] = []
        if let contextName = state.selectedContext?.name, !contextName.isEmpty {
            parts.append(contextName)
        }
        if !state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            parts.append(state.selectedNamespace)
        }
        parts.append(state.selectedSection.title)
        if let resourceName = selectedResourceName() {
            parts.append(resourceName)
        } else {
            parts.append(state.selectedWorkloadKind.title)
        }
        return parts.joined(separator: " / ")
    }

    private func commandPaletteSaveLogsItem(alias: String) -> CommandPaletteItem {
        CommandPaletteItem(
            id: "cmd:save-logs:\(alias)",
            title: "Save Logs",
            subtitle: "Save current pod logs or unified logs • `\(alias)`",
            symbolName: "square.and.arrow.down",
            action: .saveLogs
        )
    }

    private func commandPaletteDeleteSelectedItem(alias: String) -> CommandPaletteItem? {
        guard UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette else { return nil }
        guard let (kind, name) = currentDeletableResource() else { return nil }
        return CommandPaletteItem(
            id: "cmd:delete-selected:\(kind.rawValue):\(name)",
            title: "Delete \(kind.singularTypeName): \(name)",
            subtitle: "Destructive • opens confirmation only • `\(alias)`",
            symbolName: "trash",
            action: .deleteSelectedResource
        )
    }

    private struct CommandPaletteCommandQuery {
        let command: String
        let remainder: String
        let alias: String
    }

    private func commandPaletteCommandQuery(query: String) -> CommandPaletteCommandQuery? {
        let normalized = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalized.isEmpty else { return nil }

        let hasColonPrefix = normalized.hasPrefix(":")
        let commandText = hasColonPrefix ? String(normalized.dropFirst()) : normalized
        let tokens = commandText.split(whereSeparator: \.isWhitespace).map(String.init)
        guard let rawCommand = tokens.first else {
            return hasColonPrefix ? CommandPaletteCommandQuery(command: "", remainder: "", alias: ":") : nil
        }

        let command = rawCommand.lowercased()
        guard hasColonPrefix || isCommandPaletteBareCommandAlias(command) else { return nil }

        let remainder = tokens.dropFirst().joined(separator: " ").trimmingCharacters(in: .whitespacesAndNewlines)
        let alias = hasColonPrefix ? ":\(command)" : command
        return CommandPaletteCommandQuery(command: command, remainder: remainder, alias: alias)
    }

    private func isCommandPaletteBareCommandAlias(_ command: String) -> Bool {
        switch command {
        case "ws", "workspace", "workspaces",
             "savews", "save-workspace", "saveworkspace",
             "favws", "favoritews", "favorite-workspace", "unfavws", "unfavoritews", "unfavorite-workspace",
             "sl", "save-log", "save-logs", "savelog", "savelogs",
             "delete", "del", "rm",
             "po", "pod", "pods",
             "dp", "deploy", "deployment", "deployments",
             "svc", "service", "services",
             "ep", "endpoint", "endpoints",
             "ctx", "context", "contexts",
             "ns", "namespace", "namespaces",
             "ov", "overview", "home",
             "ev", "event", "events",
             "helm", "hr",
             "workloads", "wl",
             "network", "net",
             "config", "cfg",
             "storage", "sto",
             "sts", "statefulset", "statefulsets",
             "ds", "daemonset", "daemonsets",
             "rs", "replicaset", "replicasets",
             "ing", "ingress", "ingresses",
             "pvc", "pvcs", "persistentvolumeclaim", "persistentvolumeclaims",
             "pv", "pvs", "persistentvolume", "persistentvolumes",
             "sc", "storageclass", "storageclasses",
             "hpa", "horizontalpodautoscaler", "horizontalpodautoscalers",
             "np", "netpol", "networkpolicy", "networkpolicies",
             "cm", "configmap", "configmaps",
             "sec", "secret", "secrets",
             "rbac",
             "sa", "serviceaccount", "serviceaccounts",
             "role", "roles",
             "rb", "rolebinding", "rolebindings",
             "cr", "clusterrole", "clusterroles",
             "crb", "clusterrolebinding", "clusterrolebindings",
             "no", "node", "nodes",
             "cronjob", "cronjobs", "cj",
             "job", "jobs", "jo",
             "reload",
             "import",
             "ro", "readonly":
            return true
        default:
            return false
        }
    }

    private func commandPaletteCommandItems(query: String) -> [CommandPaletteItem]? {
        guard let parsedCommand = commandPaletteCommandQuery(query: query) else { return nil }
        guard !parsedCommand.command.isEmpty else {
            return commandPaletteCheatSheet()
        }

        let command = parsedCommand.command
        let remainder = parsedCommand.remainder
        let remainderTokens = commandPaletteMatchTokens(for: remainder)

        switch command {
        case "ws", "workspace", "workspaces":
            return savedWorkspaceCommandItems(query: remainder)
        case "savews", "save-workspace", "saveworkspace":
            return [saveWorkspaceCommandItem(name: remainder, alias: parsedCommand.alias)]
        case "favws", "favoritews", "favorite-workspace", "unfavws", "unfavoritews", "unfavorite-workspace":
            return savedWorkspaceFavoriteCommandItems(query: remainder)
        case "sl", "save-log", "save-logs", "savelog", "savelogs":
            return [commandPaletteSaveLogsItem(alias: parsedCommand.alias)]
        case "delete", "del", "rm":
            guard let item = commandPaletteDeleteSelectedItem(alias: parsedCommand.alias) else { return [] }
            return [item]
        case "po", "pod", "pods":
            if ["sl", "log", "logs", "save", "save logs", "save-logs"].contains(remainder.lowercased()) {
                return [commandPaletteSaveLogsItem(alias: "\(parsedCommand.alias) \(remainder)")]
            }
            let rows = Array(
                state.pods
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { pod in
                        CommandPaletteItem(
                            id: "cmd:pod:\(pod.id)",
                            title: pod.name,
                            subtitle: "Pods • `:po`",
                            symbolName: "cube.box",
                            action: .pod(pod)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:pod",
                title: "Pods",
                subtitle: "Open Workloads → Pods",
                symbolName: "cube.box",
                action: .resourceKind(section: .workloads, kind: .pod)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "dp", "deploy", "deployment", "deployments":
            let rows = Array(
                state.deployments
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { deployment in
                        CommandPaletteItem(
                            id: "cmd:deployment:\(deployment.id)",
                            title: deployment.name,
                            subtitle: "Deployments • `:deploy`",
                            symbolName: "shippingbox",
                            action: .deployment(deployment)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:deploy",
                title: "Deployments",
                subtitle: "Open Workloads → Deployments",
                symbolName: "shippingbox",
                action: .resourceKind(section: .workloads, kind: .deployment)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "svc", "service", "services":
            let rows = Array(
                state.services
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { service in
                        CommandPaletteItem(
                            id: "cmd:service:\(service.id)",
                            title: service.name,
                            subtitle: "Services • `:svc`",
                            symbolName: "point.3.connected.trianglepath.dotted",
                            action: .service(service)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:svc",
                title: "Services",
                subtitle: "Open Networking → Services",
                symbolName: "point.3.connected.trianglepath.dotted",
                action: .resourceKind(section: .networking, kind: .service)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "ep", "endpoint", "endpoints":
            let rows = Array(
                state.endpoints
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:ep:\(resource.id)",
                            title: resource.name,
                            subtitle: "Endpoints • `:ep`",
                            symbolName: "point.3.connected.trianglepath.dotted",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:ep",
                title: "Endpoints",
                subtitle: "Open Networking → Endpoints",
                symbolName: "point.3.connected.trianglepath.dotted",
                action: .resourceKind(section: .networking, kind: .endpoint)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "ctx", "context", "contexts":
            let rows = visibleContexts
                .filter { context in
                    remainderTokens.isEmpty || contextSearchTokens(for: context).contains { matches($0, tokens: remainderTokens) }
                }
                .map { context in
                    commandPaletteContextItem(context, idPrefix: "cmd:context", subtitlePrefix: "Contexts • `:ctx`")
                }
            let navigate = CommandPaletteItem(
                id: "nav:ctx",
                title: "Contexts",
                subtitle: "Open Overview",
                symbolName: "network",
                action: .section(.overview)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "ns", "namespace", "namespaces":
            let rows = namespaceOptions
                .filter { remainderTokens.isEmpty || matches($0, tokens: remainderTokens) }
                .map { namespace in
                    CommandPaletteItem(
                        id: "cmd:namespace:\(namespace)",
                        title: namespace,
                        subtitle: "Namespaces • `:ns`",
                        symbolName: isFavoriteNamespace(namespace) ? "star.fill" : "square.3.layers.3d",
                        action: .namespace(namespace)
                    )
                }
            let navigate = CommandPaletteItem(
                id: "nav:ns",
                title: "Namespaces",
                subtitle: "Open Overview",
                symbolName: "square.3.layers.3d",
                action: .section(.overview)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "ov", "overview", "home":
            return [
                CommandPaletteItem(
                    id: "cmd:overview",
                    title: "Overview",
                    subtitle: "Open Overview section",
                    symbolName: RuneSection.overview.symbolName,
                    action: .section(.overview)
                )
            ]
        case "ev", "event", "events":
            let rows = Array(
                state.events
                    .filter { remainderTokens.isEmpty || matches("\($0.reason) \($0.objectName) \($0.message)", tokens: remainderTokens) }
                    .prefix(40)
                    .map { event in
                        CommandPaletteItem(
                            id: "cmd:event:\(event.id)",
                            title: "\(event.reason) (\(event.type))",
                            subtitle: "Events • \(event.objectName)",
                            symbolName: "bolt.badge.clock",
                            action: .event(event)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:events",
                title: "Events",
                subtitle: "Open Events",
                symbolName: "bolt.badge.clock",
                action: .section(.events)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "helm", "hr":
            let rows = Array(
                state.helmReleases
                    .filter { remainderTokens.isEmpty || matches("\($0.name) \($0.namespace) \($0.chart)", tokens: remainderTokens) }
                    .prefix(40)
                    .map { release in
                        CommandPaletteItem(
                            id: "cmd:helm:\(release.id)",
                            title: release.name,
                            subtitle: "Helm • \(release.namespace)",
                            symbolName: "ferry",
                            action: .helmRelease(release)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:helm",
                title: "Helm releases",
                subtitle: "Open Helm",
                symbolName: "ferry",
                action: .section(.helm)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "workloads", "wl":
            return workloadKinds.map { kind in
                CommandPaletteItem(
                    id: "cmd:workloads:\(kind.rawValue)",
                    title: kind.title,
                    subtitle: "Switch workload kind",
                    symbolName: kind == .pod ? "cube.box" : "shippingbox",
                    action: .resourceKind(section: .workloads, kind: kind)
                )
            }
        case "network", "net":
            return networkingKinds.map { kind in
                CommandPaletteItem(
                    id: "cmd:network:\(kind.rawValue)",
                    title: kind.title,
                    subtitle: "Switch networking kind",
                    symbolName: "point.3.connected.trianglepath.dotted",
                    action: .resourceKind(section: .networking, kind: kind)
                )
            }
        case "config", "cfg":
            return configKinds.map { kind in
                CommandPaletteItem(
                    id: "cmd:config:\(kind.rawValue)",
                    title: kind.title,
                    subtitle: "Switch config kind",
                    symbolName: "slider.horizontal.3",
                    action: .resourceKind(section: .config, kind: kind)
                )
            }
        case "storage", "sto":
            return storageKinds.map { kind in
                CommandPaletteItem(
                    id: "cmd:storage:\(kind.rawValue)",
                    title: kind.title,
                    subtitle: "Switch storage kind",
                    symbolName: "internaldrive",
                    action: .resourceKind(section: .storage, kind: kind)
                )
            }
        case "sts", "statefulset", "statefulsets":
            let rows = Array(
                state.statefulSets
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:sts:\(resource.id)",
                            title: resource.name,
                            subtitle: "StatefulSets • `:sts`",
                            symbolName: "shippingbox",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:sts",
                title: "StatefulSets",
                subtitle: "Open Workloads → StatefulSets",
                symbolName: "shippingbox",
                action: .resourceKind(section: .workloads, kind: .statefulSet)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "ds", "daemonset", "daemonsets":
            let rows = Array(
                state.daemonSets
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:ds:\(resource.id)",
                            title: resource.name,
                            subtitle: "DaemonSets • `:ds`",
                            symbolName: "shippingbox",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:ds",
                title: "DaemonSets",
                subtitle: "Open Workloads → DaemonSets",
                symbolName: "shippingbox",
                action: .resourceKind(section: .workloads, kind: .daemonSet)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "rs", "replicaset", "replicasets":
            let rows = Array(
                state.replicaSets
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:rs:\(resource.id)",
                            title: resource.name,
                            subtitle: "ReplicaSets • `:rs`",
                            symbolName: "square.stack.3d.up",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:rs",
                title: "ReplicaSets",
                subtitle: "Open Workloads → ReplicaSets",
                symbolName: "square.stack.3d.up",
                action: .resourceKind(section: .workloads, kind: .replicaSet)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "ing", "ingress", "ingresses":
            let rows = Array(
                state.ingresses
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:ing:\(resource.id)",
                            title: resource.name,
                            subtitle: "Ingresses • `:ing`",
                            symbolName: "network",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:ing",
                title: "Ingresses",
                subtitle: "Open Networking → Ingresses",
                symbolName: "network",
                action: .resourceKind(section: .networking, kind: .ingress)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "pvc", "pvcs", "persistentvolumeclaim", "persistentvolumeclaims":
            let rows = Array(
                state.persistentVolumeClaims
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:pvc:\(resource.id)",
                            title: resource.name,
                            subtitle: "PVCs • `:pvc`",
                            symbolName: "externaldrive",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:pvc",
                title: "PVCs",
                subtitle: "Open Storage → PVCs",
                symbolName: "externaldrive",
                action: .resourceKind(section: .storage, kind: .persistentVolumeClaim)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "pv", "pvs", "persistentvolume", "persistentvolumes":
            let rows = Array(
                state.persistentVolumes
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:pv:\(resource.id)",
                            title: resource.name,
                            subtitle: "PVs • `:pv`",
                            symbolName: "externaldrive.fill",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:pv",
                title: "PersistentVolumes",
                subtitle: "Open Storage → PVs",
                symbolName: "externaldrive.fill",
                action: .resourceKind(section: .storage, kind: .persistentVolume)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "sc", "storageclass", "storageclasses":
            let rows = Array(
                state.storageClasses
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:sc:\(resource.id)",
                            title: resource.name,
                            subtitle: "StorageClasses • `:sc`",
                            symbolName: "internaldrive",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:sc",
                title: "StorageClasses",
                subtitle: "Open Storage → StorageClasses",
                symbolName: "internaldrive",
                action: .resourceKind(section: .storage, kind: .storageClass)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "hpa", "horizontalpodautoscaler", "horizontalpodautoscalers":
            let rows = Array(
                state.horizontalPodAutoscalers
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:hpa:\(resource.id)",
                            title: resource.name,
                            subtitle: "HPAs • `:hpa`",
                            symbolName: "gauge.with.dots.needle.67percent",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:hpa",
                title: "HPAs",
                subtitle: "Open Workloads → HPAs",
                symbolName: "gauge.with.dots.needle.67percent",
                action: .resourceKind(section: .workloads, kind: .horizontalPodAutoscaler)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "np", "netpol", "networkpolicy", "networkpolicies":
            let rows = Array(
                state.networkPolicies
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:np:\(resource.id)",
                            title: resource.name,
                            subtitle: "NetworkPolicies • `:np`",
                            symbolName: "shield.lefthalf.filled",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:np",
                title: "NetworkPolicies",
                subtitle: "Open Networking → NetworkPolicies",
                symbolName: "shield.lefthalf.filled",
                action: .resourceKind(section: .networking, kind: .networkPolicy)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "cm", "configmap", "configmaps":
            let rows = Array(
                state.configMaps
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:cm:\(resource.id)",
                            title: resource.name,
                            subtitle: "ConfigMaps • `:cm`",
                            symbolName: "doc.text",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:cm",
                title: "ConfigMaps",
                subtitle: "Open Config → ConfigMaps",
                symbolName: "doc.text",
                action: .resourceKind(section: .config, kind: .configMap)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "sec", "secret", "secrets":
            let rows = Array(
                state.secrets
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:sec:\(resource.id)",
                            title: resource.name,
                            subtitle: "Secrets • `:sec`",
                            symbolName: "key",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:sec",
                title: "Secrets",
                subtitle: "Open Config → Secrets",
                symbolName: "key",
                action: .resourceKind(section: .config, kind: .secret)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "rbac":
            return rbacKinds.map { kind in
                CommandPaletteItem(
                    id: "cmd:rbac:\(kind.rawValue)",
                    title: kind.title,
                    subtitle: "RBAC resource kind",
                    symbolName: "person.2.badge.gearshape",
                    action: .resourceKind(section: .rbac, kind: kind)
                )
            }
        case "sa", "serviceaccount", "serviceaccounts":
            let rows = Array(
                state.serviceAccounts
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:sa:\(resource.id)",
                            title: resource.name,
                            subtitle: "ServiceAccounts • `:sa`",
                            symbolName: "person.crop.circle.badge.checkmark",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:sa",
                title: "ServiceAccounts",
                subtitle: "Open RBAC → ServiceAccounts",
                symbolName: "person.crop.circle.badge.checkmark",
                action: .resourceKind(section: .rbac, kind: .serviceAccount)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "role", "roles":
            let rows = Array(
                state.rbacRoles
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:role:\(resource.id)",
                            title: resource.name,
                            subtitle: "Roles • namespace \(state.selectedNamespace)",
                            symbolName: "gearshape",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:role",
                title: "Roles",
                subtitle: "Open RBAC → Roles",
                symbolName: "gearshape",
                action: .resourceKind(section: .rbac, kind: .role)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "rb", "rolebinding", "rolebindings":
            let rows = Array(
                state.rbacRoleBindings
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:rb:\(resource.id)",
                            title: resource.name,
                            subtitle: "RoleBindings • namespace \(state.selectedNamespace)",
                            symbolName: "link",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:rb",
                title: "RoleBindings",
                subtitle: "Open RBAC → RoleBindings",
                symbolName: "link",
                action: .resourceKind(section: .rbac, kind: .roleBinding)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "cr", "clusterrole", "clusterroles":
            let rows = Array(
                state.rbacClusterRoles
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:cr:\(resource.id)",
                            title: resource.name,
                            subtitle: "ClusterRoles • `:cr`",
                            symbolName: "gearshape.2",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:cr",
                title: "ClusterRoles",
                subtitle: "Open RBAC → ClusterRoles",
                symbolName: "gearshape.2",
                action: .resourceKind(section: .rbac, kind: .clusterRole)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "crb", "clusterrolebinding", "clusterrolebindings":
            let rows = Array(
                state.rbacClusterRoleBindings
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:crb:\(resource.id)",
                            title: resource.name,
                            subtitle: "ClusterRoleBindings • `:crb`",
                            symbolName: "person.2.badge.gearshape",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:crb",
                title: "ClusterRoleBindings",
                subtitle: "Open RBAC → ClusterRoleBindings",
                symbolName: "person.2.badge.gearshape",
                action: .resourceKind(section: .rbac, kind: .clusterRoleBinding)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "no", "node", "nodes":
            let rows = Array(
                state.nodes
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:node:\(resource.id)",
                            title: resource.name,
                            subtitle: "Nodes • `:no`",
                            symbolName: "server.rack",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:node",
                title: "Nodes",
                subtitle: "Open Storage → Nodes",
                symbolName: "server.rack",
                action: .resourceKind(section: .storage, kind: .node)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "cronjob", "cronjobs", "cj":
            let rows = Array(
                state.cronJobs
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:cronjob:\(resource.id)",
                            title: resource.name,
                            subtitle: "CronJobs • `:cj`",
                            symbolName: "calendar.badge.clock",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:cronjob",
                title: "CronJobs",
                subtitle: "Open Workloads → CronJobs",
                symbolName: "calendar.badge.clock",
                action: .resourceKind(section: .workloads, kind: .cronJob)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "job", "jobs", "jo":
            let rows = Array(
                state.jobs
                    .filter { remainderTokens.isEmpty || matches($0.name, tokens: remainderTokens) }
                    .prefix(40)
                    .map { resource in
                        CommandPaletteItem(
                            id: "cmd:job:\(resource.id)",
                            title: resource.name,
                            subtitle: "Jobs • `:job`",
                            symbolName: "briefcase",
                            action: .clusterResource(resource)
                        )
                    }
            )
            let navigate = CommandPaletteItem(
                id: "nav:job",
                title: "Jobs",
                subtitle: "Open Workloads → Jobs",
                symbolName: "briefcase",
                action: .resourceKind(section: .workloads, kind: .job)
            )
            return commandPaletteResourceRowsOrNavigate(rows: rows, navigate: navigate)
        case "reload":
            return [
                CommandPaletteItem(
                    id: "cmd:reload",
                    title: "Reload cluster data",
                    subtitle: "Refresh current context and resources",
                    symbolName: "arrow.clockwise",
                    action: .reload
                )
            ]
        case "import":
            return [
                CommandPaletteItem(
                    id: "cmd:import",
                    title: "Import kubeconfig…",
                    subtitle: "Open native file picker",
                    symbolName: "square.and.arrow.down",
                    action: .importKubeConfig
                )
            ]
        case "ro", "readonly":
            return [
                CommandPaletteItem(
                    id: "cmd:readonly:on",
                    title: "Enable read-only mode",
                    subtitle: "Block write actions",
                    symbolName: "lock",
                    action: .readOnly(true)
                ),
                CommandPaletteItem(
                    id: "cmd:readonly:off",
                    title: "Disable read-only mode",
                    subtitle: "Allow write actions",
                    symbolName: "lock.open",
                    action: .readOnly(false)
                )
            ]
        default:
            return commandPaletteCheatSheet()
        }
    }

    private func commandPaletteCheatSheet() -> [CommandPaletteItem] {
        [
            CommandPaletteItem(id: "help:po", title: ":po <name>", subtitle: "Pods", symbolName: "cube.box", action: .resourceKind(section: .workloads, kind: .pod)),
            CommandPaletteItem(id: "help:deploy", title: ":deploy <name>", subtitle: "Deployments", symbolName: "shippingbox", action: .resourceKind(section: .workloads, kind: .deployment)),
            CommandPaletteItem(id: "help:sts", title: ":sts <name>", subtitle: "StatefulSets", symbolName: "shippingbox", action: .resourceKind(section: .workloads, kind: .statefulSet)),
            CommandPaletteItem(id: "help:ds", title: ":ds <name>", subtitle: "DaemonSets", symbolName: "shippingbox", action: .resourceKind(section: .workloads, kind: .daemonSet)),
            CommandPaletteItem(id: "help:svc", title: ":svc / :service <name>", subtitle: "Services", symbolName: "point.3.connected.trianglepath.dotted", action: .resourceKind(section: .networking, kind: .service)),
            CommandPaletteItem(id: "help:ep", title: ":ep <name>", subtitle: "Endpoints", symbolName: "point.3.connected.trianglepath.dotted", action: .resourceKind(section: .networking, kind: .endpoint)),
            CommandPaletteItem(id: "help:ing", title: ":ing <name>", subtitle: "Ingresses", symbolName: "network", action: .resourceKind(section: .networking, kind: .ingress)),
            CommandPaletteItem(id: "help:cm", title: ":cm <name>", subtitle: "ConfigMaps", symbolName: "doc.text", action: .resourceKind(section: .config, kind: .configMap)),
            CommandPaletteItem(id: "help:sec", title: ":sec <name>", subtitle: "Secrets", symbolName: "key", action: .resourceKind(section: .config, kind: .secret)),
            CommandPaletteItem(id: "help:sl", title: ":sl / :po logs", subtitle: "Save current pod or unified logs", symbolName: "square.and.arrow.down", action: .saveLogs),
            CommandPaletteItem(id: "help:no", title: ":no <name>", subtitle: "Nodes (Storage)", symbolName: "server.rack", action: .resourceKind(section: .storage, kind: .node)),
            CommandPaletteItem(id: "help:ns", title: ":ns <namespace>", subtitle: "Switch namespace", symbolName: "square.3.layers.3d", action: .section(.overview)),
            CommandPaletteItem(id: "help:ov", title: ":ov / :overview", subtitle: "Open Overview", symbolName: RuneSection.overview.symbolName, action: .section(.overview)),
            CommandPaletteItem(id: "help:ctx", title: ":ctx <context>", subtitle: "Switch context", symbolName: "network", action: .section(.overview)),
            CommandPaletteItem(id: "help:ws", title: ":ws <workspace>", subtitle: "Open saved workspace", symbolName: "rectangle.stack.badge.play", action: .section(.overview)),
            CommandPaletteItem(id: "help:savews", title: ":savews <name>", subtitle: "Save current workspace", symbolName: "rectangle.stack.badge.plus", action: .section(.overview)),
            CommandPaletteItem(id: "help:favws", title: ":favws <workspace>", subtitle: "Toggle workspace favorite", symbolName: "star", action: .section(.overview)),
            CommandPaletteItem(id: "help:sa", title: ":sa <name>", subtitle: "ServiceAccounts", symbolName: "person.crop.circle.badge.checkmark", action: .resourceKind(section: .rbac, kind: .serviceAccount)),
            CommandPaletteItem(id: "help:rbac", title: ":rbac", subtitle: "RBAC kinds", symbolName: "person.2.badge.gearshape", action: .resourceKind(section: .rbac, kind: .role)),
            CommandPaletteItem(id: "help:helm", title: ":helm <release>", subtitle: "Helm releases", symbolName: "ferry", action: .section(.helm)),
            CommandPaletteItem(id: "help:cj", title: ":cj <name>", subtitle: "CronJobs", symbolName: "calendar.badge.clock", action: .resourceKind(section: .workloads, kind: .cronJob)),
            CommandPaletteItem(id: "help:job", title: ":job <name>", subtitle: "Jobs", symbolName: "briefcase", action: .resourceKind(section: .workloads, kind: .job)),
            CommandPaletteItem(id: "help:rs", title: ":rs <name>", subtitle: "ReplicaSets", symbolName: "square.stack.3d.up", action: .resourceKind(section: .workloads, kind: .replicaSet)),
            CommandPaletteItem(id: "help:storage", title: ":pvc :pv :sc :hpa :np", subtitle: "Storage & HPA jumps", symbolName: "externaldrive", action: .resourceKind(section: .storage, kind: .persistentVolumeClaim))
        ]
    }

    private func resourceCounts() -> [String: Int] {
        [
            "pods": state.pods.count,
            "deployments": state.deployments.count,
            "statefulsets": state.statefulSets.count,
            "daemonsets": state.daemonSets.count,
            "jobs": state.jobs.count,
            "cronjobs": state.cronJobs.count,
            "replicasets": state.replicaSets.count,
            "services": state.services.count,
            "endpoints": state.endpoints.count,
            "ingresses": state.ingresses.count,
            "configmaps": state.configMaps.count,
            "secrets": state.secrets.count,
            "nodes": state.nodes.count,
            "events": state.events.count,
            "serviceAccounts": state.serviceAccounts.count,
            "roles": state.rbacRoles.count,
            "roleBindings": state.rbacRoleBindings.count,
            "clusterRoles": state.rbacClusterRoles.count,
            "clusterRoleBindings": state.rbacClusterRoleBindings.count,
            "persistentVolumeClaims": state.persistentVolumeClaims.count,
            "persistentVolumes": state.persistentVolumes.count,
            "storageClasses": state.storageClasses.count,
            "horizontalPodAutoscalers": state.horizontalPodAutoscalers.count,
            "networkPolicies": state.networkPolicies.count
        ]
    }

    private func selectedResourceKindLabel() -> String? {
        switch state.selectedSection {
        case .workloads, .networking, .config, .storage, .rbac:
            return currentWritableResource()?.0.kubernetesResourceName
        case .events:
            return state.selectedEvent == nil ? nil : "event"
        case .helm:
            if let release = state.selectedHelmRelease {
                return release.savedWorkspaceKindLabel
            }
            return state.selectedOperatorResource?.apiPath
        default:
            return nil
        }
    }

    private func selectedResourceName() -> String? {
        switch state.selectedSection {
        case .workloads, .networking, .config, .storage, .rbac:
            return currentWritableResource()?.1
        case .events:
            return state.selectedEvent?.objectName
        case .helm:
            return state.selectedHelmRelease?.name ?? state.selectedOperatorResource?.name
        default:
            return nil
        }
    }

    private func selectedResourceNamespace() -> String? {
        switch state.selectedSection {
        case .workloads:
            switch state.selectedWorkloadKind {
            case .pod: return state.selectedPod?.namespace
            case .deployment: return state.selectedDeployment?.namespace
            case .statefulSet: return state.selectedStatefulSet?.namespace
            case .daemonSet: return state.selectedDaemonSet?.namespace
            case .job: return state.selectedJob?.namespace
            case .cronJob: return state.selectedCronJob?.namespace
            case .replicaSet: return state.selectedReplicaSet?.namespace
            case .horizontalPodAutoscaler: return state.selectedHorizontalPodAutoscaler?.namespace
            default: return nil
            }
        case .networking:
            switch state.selectedWorkloadKind {
            case .service: return state.selectedService?.namespace
            case .endpoint: return state.selectedEndpoint?.namespace
            case .ingress: return state.selectedIngress?.namespace
            case .networkPolicy: return state.selectedNetworkPolicy?.namespace
            default: return nil
            }
        case .config:
            switch state.selectedWorkloadKind {
            case .configMap: return state.selectedConfigMap?.namespace
            case .secret: return state.selectedSecret?.namespace
            default: return nil
            }
        case .storage:
            switch state.selectedWorkloadKind {
            case .persistentVolumeClaim: return state.selectedPersistentVolumeClaim?.namespace
            case .persistentVolume: return state.selectedPersistentVolume?.namespace
            case .storageClass: return state.selectedStorageClass?.namespace
            case .node: return state.selectedNode?.namespace
            default: return nil
            }
        case .rbac:
            return state.selectedRBACResource?.namespace
        case .events:
            return state.selectedEvent?.involvedNamespace
        case .helm:
            return state.selectedHelmRelease?.namespace ?? state.selectedOperatorResource?.namespace
        default:
            return nil
        }
    }

    private func restoreSavedWorkspaceResourceSelection(_ workspace: SavedWorkspaceSnapshot) {
        guard workspace.resourceName != nil else { return }

        switch workspace.section {
        case .workloads, .networking, .config, .storage:
            restoreSavedWorkspaceWorkloadResource(workspace)
        case .rbac:
            restoreSavedWorkspaceRBACResource(workspace)
        case .events:
            let event = state.events.first { event in
                event.objectName == workspace.resourceName
                    && namespaceMatches(event.involvedNamespace, workspace: workspace)
            }
            selectEvent(event, trackHistory: false)
        case .helm:
            if workspace.resourceKind == HelmReleaseSummary.savedWorkspaceKindLabel {
                let release = state.helmReleases.first { release in
                    release.name == workspace.resourceName
                        && namespaceMatches(release.namespace, workspace: workspace)
                }
                selectHelmRelease(release, trackHistory: false)
            } else {
                let resource = state.operatorResources.first { resource in
                    resource.name == workspace.resourceName
                        && resource.apiPath == workspace.resourceKind
                        && namespaceMatches(resource.namespace, workspace: workspace)
                }
                selectOperatorResource(resource, trackHistory: false)
            }
        default:
            break
        }
    }

    private func restoreSavedWorkspaceWorkloadResource(_ workspace: SavedWorkspaceSnapshot) {
        switch workspace.workloadKind {
        case .pod:
            selectPod(state.pods.first { matchesSavedWorkspaceResource(name: $0.name, namespace: $0.namespace, workspace: workspace) }, trackHistory: false)
        case .deployment:
            selectDeployment(state.deployments.first { matchesSavedWorkspaceResource(name: $0.name, namespace: $0.namespace, workspace: workspace) }, trackHistory: false)
        case .service:
            selectService(state.services.first { matchesSavedWorkspaceResource(name: $0.name, namespace: $0.namespace, workspace: workspace) }, trackHistory: false)
        case .statefulSet:
            selectStatefulSet(state.statefulSets.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .daemonSet:
            selectDaemonSet(state.daemonSets.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .job:
            selectJob(state.jobs.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .cronJob:
            selectCronJob(state.cronJobs.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .replicaSet:
            selectReplicaSet(state.replicaSets.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .persistentVolumeClaim:
            selectPersistentVolumeClaim(state.persistentVolumeClaims.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .persistentVolume:
            selectPersistentVolume(state.persistentVolumes.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .storageClass:
            selectStorageClass(state.storageClasses.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .horizontalPodAutoscaler:
            selectHorizontalPodAutoscaler(state.horizontalPodAutoscalers.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .networkPolicy:
            selectNetworkPolicy(state.networkPolicies.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .endpoint:
            selectEndpoint(state.endpoints.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .ingress:
            selectIngress(state.ingresses.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .configMap:
            selectConfigMap(state.configMaps.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .secret:
            selectSecret(state.secrets.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .node:
            selectNode(state.nodes.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
        case .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            break
        }
    }

    private func restoreSavedWorkspaceRBACResource(_ workspace: SavedWorkspaceSnapshot) {
        let lists = state.serviceAccounts + state.rbacRoles + state.rbacRoleBindings + state.rbacClusterRoles + state.rbacClusterRoleBindings
        selectRBACResource(lists.first { matchesSavedWorkspaceResource($0, workspace: workspace) }, trackHistory: false)
    }

    private func matchesSavedWorkspaceResource(_ resource: ClusterResourceSummary, workspace: SavedWorkspaceSnapshot) -> Bool {
        matchesSavedWorkspaceResource(name: resource.name, namespace: resource.namespace, workspace: workspace)
    }

    private func matchesSavedWorkspaceResource(name: String, namespace: String?, workspace: SavedWorkspaceSnapshot) -> Bool {
        name == workspace.resourceName && namespaceMatches(namespace, workspace: workspace)
    }

    private func namespaceMatches(_ namespace: String?, workspace: SavedWorkspaceSnapshot) -> Bool {
        guard let resourceNamespace = workspace.resourceNamespace else { return true }
        return (namespace ?? "").caseInsensitiveCompare(resourceNamespace) == .orderedSame
    }

    private func parsePort(_ value: String, fieldName: String) throws -> Int {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let port = Int(trimmed), (1...65535).contains(port) else {
            throw RuneError.invalidInput(message: "\(fieldName) must be a number between 1 and 65535.")
        }
        return port
    }

    private func parseOptionalRevisionInput(_ value: String) throws -> Int? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return nil
        }

        guard let revision = Int(trimmed), revision > 0 else {
            throw RuneError.invalidInput(message: "Revision must be a positive integer.")
        }

        return revision
    }

    private func parseCommandInput(_ input: String) throws -> [String] {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            throw RuneError.invalidInput(message: "Exec command cannot be empty.")
        }

        var tokens: [String] = []
        var current = ""
        var activeQuote: Character?

        for character in trimmed {
            if activeQuote != nil {
                if character == activeQuote {
                    activeQuote = nil
                } else {
                    current.append(character)
                }
                continue
            }

            if character == "\"" || character == "'" {
                activeQuote = character
                continue
            }

            if character.isWhitespace {
                if !current.isEmpty {
                    tokens.append(current)
                    current = ""
                }
                continue
            }

            current.append(character)
        }

        if let activeQuote {
            throw RuneError.invalidInput(message: "missing closing quote \(activeQuote).")
        }

        if !current.isEmpty {
            tokens.append(current)
        }

        guard !tokens.isEmpty else {
            throw RuneError.invalidInput(message: "the exec command could not be parsed.")
        }

        return tokens
    }

}

private extension HelmReleaseSummary {
    static let savedWorkspaceKindLabel = "helmrelease"

    var savedWorkspaceKindLabel: String {
        Self.savedWorkspaceKindLabel
    }
}
