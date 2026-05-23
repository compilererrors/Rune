import AppKit
import Combine
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
    case rolloutRestart(deploymentName: String)
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
        case let .rolloutRestart(deploymentName):
            return "Restart rollout for deployment \(deploymentName)?"
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
            return "\(resources.count) selected resources will be removed from \(scope). This cannot be undone."
        case let .apply(_, _, yaml, baseline):
            let diff = Self.diffPreview(from: baseline, to: yaml)
            guard !diff.isEmpty else {
                return "This applies the current YAML to the active namespace/context."
            }
            return "This applies the current YAML to the active namespace/context.\n\nYAML diff preview:\n\(diff)"
        case .scale:
            return "Replica count will be changed immediately."
        case .rolloutRestart:
            return "Pods in the deployment will be recreated according to rollout strategy."
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
        case .scale: return "Scale"
        case .rolloutRestart: return "Restart"
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
        case .apply, .scale, .rolloutRestart, .exec, .createJobFromCronJob: return false
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
        case let .rolloutRestart(deploymentName):
            return Self.shellCommand(namespaced + ["rollout", "restart", "deployment", deploymentName])
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
        parts.map(shellQuote).joined(separator: " ")
    }

    private static func shellQuote(_ value: String) -> String {
        guard !value.isEmpty else { return "''" }
        if value.rangeOfCharacter(from: CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./:=+-").inverted) == nil {
            return value
        }
        return "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
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
        case .section, .context, .namespace, .importKubeConfig, .reload, .readOnly, .saveLogs:
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
    let selectedIngressName: String?
    let selectedConfigMapName: String?
    let selectedSecretName: String?
    let selectedNodeName: String?
    let selectedRBACResourceID: String?
}

/// Which cluster lists Rune loads for the current section and resource kind. Drives parallel work in `loadResourceSnapshot`.
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
    var rbacRoleBindings = false
    var rbacClusterRoles = false
    var rbacClusterRoleBindings = false

    static func forSelection(section: RuneSection, kind: KubeResourceKind) -> SnapshotLoadPlan {
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
            plan.events = true
        case .workloads:
            switch kind {
            case .pod:
                plan.pods = true
            case .deployment:
                plan.deployments = true
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
            plan.rbacRoles = true
            plan.rbacRoleBindings = true
            plan.rbacClusterRoles = true
            plan.rbacClusterRoleBindings = true
        case .terminal:
            plan.pods = true
        case .helm:
            break
        }
        return plan
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
}

@MainActor
public final class RuneAppViewModel: ObservableObject {
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
    @Published public private(set) var kubeConfigImportReviews: [KubeConfigImportReview] = []
    @Published public var favoriteImportedKubeConfigContexts: Bool = false
    private var namespaceResolutionLookupCache: (namespaces: [String], lookup: [String: String])?
    @Published public var manualKubeConfigName: String = ""
    @Published public var manualKubeConfigServer: String = ""
    @Published public var manualKubeConfigNamespace: String = "default"
    @Published public var manualKubeConfigToken: String = ""
    @Published public private(set) var cloudKubeConfigImportStatus: String?
    @Published public var pendingWriteAction: PendingWriteAction? {
        didSet {
            if pendingWriteAction != oldValue {
                pendingProductionDestructiveConfirmation = nil
                if case .rolloutUndo? = pendingWriteAction {
                } else if case .controllerRolloutUndo? = pendingWriteAction {
                } else {
                    pendingRollbackPlan = nil
                }
            }
        }
    }
    @Published public private(set) var pendingProductionDestructiveConfirmation: PendingWriteAction?
    @Published public var scaleReplicaInput: Int = 1
    @Published public var execCommandInput: String = "printenv"
    @Published public var terminalSessionInput: String = ""
    @Published public var portForwardLocalPortInput: String = "8080"
    @Published public var portForwardRemotePortInput: String = "8080"
    @Published public var portForwardAddressInput: String = "127.0.0.1"
    @Published public var rolloutRevisionInput: String = ""
    @Published public private(set) var pendingRollbackPlan: String?
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

    private let kubeClient: KubernetesClient
    private let bookmarkManager: BookmarkManager
    private let picker: KubeConfigPicking
    private let kubeConfigDiscoverer: KubeConfigDiscovering
    private let store: ResourceStore
    private let exporter: FileExporting
    private let supportBundleBuilder: any SupportBundleBuilding
    private let contextPreferences: ContextPreferencesStoring
    private let kubeConfigImportValidator: KubeConfigImportValidator
    private let kubeConfigImportStore: KubeConfigImportStoring
    private let cloudKubeConfigImporter: CloudKubeConfigImporting
    private let helmCommandRunner: HelmCommandRunning
    private let overviewSnapshotPersistence: any OverviewSnapshotCacheStoring
    private let namespaceListPersistence: NamespaceListPersisting
    private let portForwardBrowserOpener: PortForwardBrowserOpening
    private let diagnostics: DiagnosticsRecorder
    private let terminalShellCommand = ["sh"]

    private var cancellables: Set<AnyCancellable> = []
    private var hasBootstrapped = false
    private var bootstrapTask: Task<Void, Never>?
    private var kubeConfigSourceSyncTask: Task<Void, Never>?
    private var latestKubeConfigSourceFingerprint: KubeConfigSourceFingerprint?
    private var clusterLoadGeneration = UUID()
    private var launchExperienceStartedAt = ContinuousClock.now
    private var latestSnapshotRequestID = UUID()
    private var latestResourceDetailsRequestID = UUID()
    private var latestLogsReloadRequestID = UUID()
    private var latestYAMLValidationRequestID = UUID()
    private var latestHelmDetailsRequestID = UUID()
    private var navigationHistory: [NavigationCheckpoint] = []
    private var navigationIndex: Int = -1
    private var isApplyingNavigationCheckpoint = false
    private var pendingOpenEventSource: EventSummary?
    /// Retries for `navigateToEventSource` when lists were not loaded for the Events-only snapshot (e.g. pods empty until workloads refresh).
    private var navigateFromEventFetchAttempts = 0
    private var scheduledRefreshTask: Task<Void, Never>?
    private var pendingCurrentViewRefreshID: UUID?
    private var resourceDetailsTask: Task<Void, Never>?
    private var scheduledLogsReloadTask: Task<Void, Never>?
    private var logsReloadTask: Task<Void, Never>?
    private var tailLogsReloadTask: Task<Void, Never>?
    private var yamlValidationTask: Task<Void, Never>?
    private var terminalOutputFlushTask: Task<Void, Never>?
    private var liveStatusUpdatesTask: Task<Void, Never>?
    private var pendingTerminalOutputBySessionID: [String: String] = [:]
    private var pendingTerminalEscapeBySessionID: [String: String] = [:]
    private var pendingForcedNamespaceRefresh = false
    /// Set during context switch with no explicit namespace so first metadata refresh can override stale carry-over namespace.
    private var pendingNamespaceRevalidationContextName: String?
    private var namespaceMetadataRefreshedAt: [String: Date] = [:]
    /// In-memory overview rows keyed by `overviewCacheKey(contextName:namespace:)`; TTL `overviewSnapshotFreshnessTTL`. Mirrors disk where possible; merged with `ResourceStore` on apply.
    private var overviewSnapshotCache: [String: OverviewSnapshotCacheEntry] = [:]
    /// One-shot bypass after cancelled/stale snapshots so the next load for a key does not get stuck behind cooldown.
    private var bypassOverviewCooldownKeys: Set<String> = []
    /// Background task: `listPodStatuses` + count queries for sibling namespaces; cancelled on context change.
    private var overviewPrefetchTask: Task<Void, Never>?
    /// Background task: warms overview cache for non-selected contexts; cancelled on context change.
    private var contextOverviewPrefetchTask: Task<Void, Never>?
    private var recentNamespacesByContext: [String: [String]] = [:]
    /// Recently selected contexts (most-recent first); used with favorites when selecting prefetch targets.
    private var recentContextNames: [String] = []
    private let demoContextName = "rune-demo"
    private var demoContext: KubeContext { KubeContext(name: demoContextName) }
    private static let operatorResourcePageSize = 40

    private let refreshDebounceNanoseconds: UInt64 = 120_000_000
    /// Coalesces rapid log preset toggles while still cancelling any in-flight fetch immediately.
    private let logsReloadDebounceNanoseconds: UInt64 = 180_000_000
    private let terminalOutputFlushNanoseconds: UInt64 = 33_000_000
    private let tailLogsReloadNanoseconds: UInt64 = 3_000_000_000
    private let liveStatusUpdateNanoseconds: UInt64 = 12_000_000_000
    private let kubeConfigSourceSyncNanoseconds: UInt64 = 2_000_000_000
    private let launchExperienceMinimumNanoseconds: UInt64 = 320_000_000
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
        kubeConfigDiscoverer: KubeConfigDiscovering = KubeConfigDiscoverer(),
        store: ResourceStore = ResourceStore(),
        exporter: FileExporting = SavePanelExporter(),
        supportBundleBuilder: any SupportBundleBuilding = JSONSupportBundleBuilder(),
        contextPreferences: ContextPreferencesStoring = UserDefaultsContextPreferencesStore(),
        kubeConfigImportValidator: KubeConfigImportValidator = KubeConfigImportValidator(),
        kubeConfigImportStore: KubeConfigImportStoring = AppOwnedKubeConfigImportStore(),
        cloudKubeConfigImporter: CloudKubeConfigImporting = CloudKubeConfigCLIImporter(),
        helmCommandRunner: HelmCommandRunning = ProcessHelmCommandRunner(),
        overviewSnapshotPersistence: any OverviewSnapshotCacheStoring = JSONOverviewSnapshotCacheStore(),
        namespaceListPersistence: NamespaceListPersisting = JSONNamespaceListPersistenceStore(),
        portForwardBrowserOpener: PortForwardBrowserOpening = WorkspacePortForwardBrowserOpener(),
        diagnostics: DiagnosticsRecorder = DiagnosticsRecorder()
    ) {
        self.state = state
        self.kubeClient = kubeClient
        self.bookmarkManager = bookmarkManager
        self.picker = picker
        self.kubeConfigDiscoverer = kubeConfigDiscoverer
        self.store = store
        self.exporter = exporter
        self.supportBundleBuilder = supportBundleBuilder
        self.contextPreferences = contextPreferences
        self.kubeConfigImportValidator = kubeConfigImportValidator
        self.kubeConfigImportStore = kubeConfigImportStore
        self.cloudKubeConfigImporter = cloudKubeConfigImporter
        self.helmCommandRunner = helmCommandRunner
        self.overviewSnapshotPersistence = overviewSnapshotPersistence
        self.namespaceListPersistence = namespaceListPersistence
        self.portForwardBrowserOpener = portForwardBrowserOpener
        self.diagnostics = diagnostics

        self.state.setFavoriteContextNames(contextPreferences.loadFavoriteContextNames())
        self.state.setFavoriteResourceIDs(contextPreferences.loadFavoriteResourceIDs())
        self.state.setFavoriteNamespaceIDs(contextPreferences.loadFavoriteNamespaceIDs())
        self.state.setManualProductionContextIDs(contextPreferences.loadManualProductionContextIDs())

        state.objectWillChange
            .sink { [weak self] _ in
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

        NotificationCenter.default.publisher(for: .runeCachesDidClear)
            .sink { [weak self] _ in
                self?.handleCachesCleared()
            }
            .store(in: &cancellables)
    }

    public var workloadKinds: [KubeResourceKind] {
        [.pod, .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler]
    }

    public var networkingKinds: [KubeResourceKind] {
        [.service, .ingress, .networkPolicy]
    }

    public var configKinds: [KubeResourceKind] {
        [.configMap, .secret]
    }

    public var storageKinds: [KubeResourceKind] {
        [.persistentVolumeClaim, .persistentVolume, .storageClass, .node]
    }

    public var rbacKinds: [KubeResourceKind] {
        [.role, .roleBinding, .clusterRole, .clusterRoleBinding]
    }

    public var visibleRBACResources: [ClusterResourceSummary] {
        let list: [ClusterResourceSummary] = {
            switch state.selectedWorkloadKind {
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

    public var podLogContainerOptions: [String] {
        state.selectedPod?.containerNames ?? []
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

    private var overviewInsightsProjector: OverviewInsightsProjector {
        OverviewInsightsProjector(
            pods: state.overviewPods,
            deployments: state.deployments,
            services: state.services,
            events: state.overviewEvents
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
        return visiblePods.filter { selectedIDs.contains($0.id) }
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
        case .configMap: return visibleConfigMaps
        case .secret: return visibleSecrets
        case .persistentVolumeClaim: return visiblePersistentVolumeClaims
        case .persistentVolume: return visiblePersistentVolumes
        case .storageClass: return visibleStorageClasses
        case .node: return visibleNodes
        case .role, .roleBinding, .clusterRole, .clusterRoleBinding: return visibleRBACResources
        case .pod, .deployment, .service, .event:
            return []
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
        return visibleGenericResourcesForBulkActions.filter { selectedIDs.contains($0.id) }
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
        state.setSelectedPodIDs(Set(visiblePods.map(\.id)))
    }

    public func toggleAllVisiblePodsForBulkActions() {
        if areAllVisiblePodsSelectedForBulkActions {
            clearPodBulkSelection()
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
            validIDs: Set(visibleGenericResourcesForBulkActions.map(\.id))
        )
    }

    public func toggleAllVisibleGenericResourcesForBulkActions() {
        let visibleIDs = Set(visibleGenericResourcesForBulkActions.map(\.id))
        if areAllVisibleGenericResourcesSelectedForBulkActions {
            state.clearSelectedGenericResourceIDs()
        } else {
            state.setSelectedGenericResourceIDs(visibleIDs, validIDs: visibleIDs)
        }
    }

    public func clearGenericResourceBulkSelection() {
        state.clearSelectedGenericResourceIDs()
    }

    public func copySelectedGenericResourceComparisonToClipboard() {
        guard canCopySelectedGenericResourceComparison else { return }
        let comparison = selectedGenericResourceComparisonText
        guard !comparison.isEmpty else { return }
        let pasteboard = NSPasteboard.general
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
        namespaces.sorted { lhs, rhs in
            let lhsFavorite = isFavoriteNamespace(lhs)
            let rhsFavorite = isFavoriteNamespace(rhs)
            if lhsFavorite != rhsFavorite {
                return lhsFavorite && !rhsFavorite
            }
            return lhs.localizedCaseInsensitiveCompare(rhs) == .orderedAscending
        }
    }

    private func contextsIncludingEnabledDemo(_ contexts: [KubeContext]) -> [KubeContext] {
        guard UserDefaults.standard.runeEnableDemoCluster else {
            return contexts.filter { $0.name != demoContextName }
        }
        guard !contexts.contains(where: { $0.name == demoContextName }) else { return contexts }
        return contexts + [demoContext]
    }

    public var visibleContexts: [KubeContext] {
        let filtered = contextsIncludingEnabledDemo(state.contexts).filter { context in
            let query = state.contextSearchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !query.isEmpty else { return true }
            return matches(context.name, query: query)
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

    public var visiblePods: [PodSummary] {
        let values = filtered(state.pods) { pod in
            "\(pod.name) \(pod.status) \(pod.namespace) \(pod.ageDescription) \(pod.cpuDisplay) \(pod.memoryDisplay) \(pod.totalRestarts)"
        }
        return sortedPods(values)
    }

    public var visibleDeployments: [DeploymentSummary] {
        filtered(state.deployments) { deployment in
            "\(deployment.name) \(deployment.namespace) \(deployment.replicaText)"
        }
        .sorted(by: deploymentComparator)
    }

    public var visibleServices: [ServiceSummary] {
        filtered(state.services) { service in
            "\(service.name) \(service.namespace) \(service.type) \(service.clusterIP)"
        }
        .sorted(by: serviceComparator)
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
        filtered(state.events) { event in
            "\(event.type) \(event.reason) \(event.objectName) \(event.message)"
        }
        .sorted(by: eventComparator)
    }

    public var visibleHelmReleases: [HelmReleaseSummary] {
        filtered(state.helmReleases) { release in
            "\(release.name) \(release.namespace) \(release.status) \(release.chart) \(release.appVersion)"
        }
        .sorted(by: helmReleaseComparator)
    }

    public var visibleOperatorResources: [OperatorResourceSummary] {
        operatorResourceSorted(filtered(state.operatorResources) { resource in
            "\(resource.family) \(resource.kind) \(resource.name) \(resource.namespace ?? "") \(resource.status) \(resource.message)"
        })
    }

    public var pagedOperatorResources: [OperatorResourceSummary] {
        let resources = visibleOperatorResources
        guard !resources.isEmpty else { return [] }
        let start = min(operatorResourcePage * Self.operatorResourcePageSize, max(0, resources.count - 1))
        return Array(resources.dropFirst(start).prefix(Self.operatorResourcePageSize))
    }

    public var operatorResourcePageSummary: String {
        let count = visibleOperatorResources.count
        guard count > 0 else { return "0 resources" }
        let start = min(operatorResourcePage * Self.operatorResourcePageSize, max(0, count - 1)) + 1
        let end = min(start + Self.operatorResourcePageSize - 1, count)
        return "\(start)-\(end) of \(count)"
    }

    public var canPageOperatorResourcesBackward: Bool {
        operatorResourcePage > 0
    }

    public var canPageOperatorResourcesForward: Bool {
        (operatorResourcePage + 1) * Self.operatorResourcePageSize < visibleOperatorResources.count
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
        if isProductionContext {
            if pendingWriteAction.isDestructive {
                guard UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation else {
                    return "PRODUCTION CONTEXT: \(message)"
                }
                if pendingProductionDestructiveConfirmation == pendingWriteAction {
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
              let contextName = state.selectedContext?.name
        else { return "" }
        return pendingWriteAction.kubectlCommand(contextName: contextName, namespace: state.selectedNamespace)
    }

    public var pendingWriteActionConfirmLabel: String {
        guard let pendingWriteAction else { return "Confirm" }
        if isProductionContext,
           pendingWriteAction.isDestructive,
           UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation,
           pendingProductionDestructiveConfirmation != pendingWriteAction
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
            || !state.ingresses.isEmpty
            || !state.configMaps.isEmpty
            || !state.secrets.isEmpty
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
            state.setIngresses([])
            state.setConfigMaps([])
            state.setSecrets([])
            state.setNodes([])
            state.setEvents([])
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
        Task {
            do {
                let files = try picker.pickFiles()
                guard !files.isEmpty else { return }

                let payloads = try files.map { file in
                    (
                        raw: try String(contentsOf: file, encoding: .utf8),
                        sourceName: file.lastPathComponent
                    )
                }
                try await importKubeConfigPayloads(payloads, logLabel: "importKubeConfig")
            } catch {
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
        Task {
            do {
                guard let folder = try picker.pickFolder() else { return }
                let files = try kubeConfigFiles(in: folder)
                guard !files.isEmpty else {
                    throw RuneError.invalidInput(message: "No kubeconfig files were found in \(folder.lastPathComponent).")
                }

                let payloads = try files.map { file in
                    (
                        raw: try String(contentsOf: file, encoding: .utf8),
                        sourceName: file.lastPathComponent
                    )
                }
                try await importKubeConfigPayloads(payloads, logLabel: "importKubeConfigFolder")
            } catch {
                diagnostics.log("importKubeConfigFolder failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func importKubeConfig(raw: String, sourceName: String = "pasted-kubeconfig.yaml") {
        Task {
            do {
                try await importKubeConfigPayloads([(raw: raw, sourceName: sourceName)], logLabel: "importKubeConfigPaste")
            } catch {
                diagnostics.log("importKubeConfigPaste failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func importManualTokenKubeConfig() {
        Task {
            do {
                let raw = try manualTokenKubeConfigYAML()
                manualKubeConfigToken = ""
                try await importKubeConfigPayloads([(raw: raw, sourceName: "manual-token-kubeconfig.yaml")], logLabel: "importManualTokenKubeConfig")
            } catch {
                diagnostics.log("importManualTokenKubeConfig failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func cloudKubeConfigCommandPreview(for request: CloudKubeConfigImportRequest) -> String {
        do {
            return try cloudKubeConfigImporter.commandPreview(for: request).displayCommand
        } catch {
            return error.localizedDescription
        }
    }

    public func runCloudKubeConfigImport(_ request: CloudKubeConfigImportRequest) {
        cloudKubeConfigImportStatus = "Running \(request.provider.rawValue.uppercased()) import..."
        Task { @MainActor [weak self] in
            guard let self else { return }
            do {
                let result = try await self.cloudKubeConfigImporter.importCluster(request)
                self.kubeConfigImportReviews = result.reviews
                let blockingIssues = self.blockingKubeConfigImportIssues(in: result.reviews)
                guard blockingIssues.isEmpty else {
                    self.setKubeConfigImportFailureAuthDoctorChecks(for: blockingIssues)
                    throw RuneError.invalidInput(message: self.kubeConfigImportErrorMessage(for: blockingIssues))
                }

                let sources = try self.resolvedKubeConfigSources(fallbackURLs: result.discoveredURLs)
                self.state.setSources(sources)
                self.latestKubeConfigSourceFingerprint = self.kubeConfigSourceFingerprint(for: sources)
                self.startKubeConfigSourceSync()
                self.cloudKubeConfigImportStatus = "Imported \(request.provider.rawValue.uppercased()) kubeconfig context."
                try await self.reloadContexts()
                self.runAuthDoctor()
            } catch {
                self.cloudKubeConfigImportStatus = "Cloud import failed."
                self.state.setAuthDoctorChecks([
                    RuneHealthCheck(
                        id: "cloud-login-\(request.provider.rawValue)",
                        title: "\(request.provider.rawValue.uppercased()) cloud login",
                        status: .failed,
                        message: "Cloud kubeconfig import did not complete. Check the provider CLI login and required fields, then run Auth Doctor again."
                    )
                ])
                self.diagnostics.log("cloud kubeconfig import failed: \(error.localizedDescription)")
                self.state.setError(error)
            }
        }
    }

    private func manualTokenKubeConfigYAML() throws -> String {
        let server = manualKubeConfigServer.trimmingCharacters(in: .whitespacesAndNewlines)
        let token = manualKubeConfigToken.trimmingCharacters(in: .whitespacesAndNewlines)
        let namespace = manualKubeConfigNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let components = URLComponents(string: server),
              let scheme = components.scheme?.lowercased(),
              ["http", "https"].contains(scheme),
              components.host?.isEmpty == false else {
            throw RuneError.invalidInput(message: "Manual cluster server must be a valid HTTP or HTTPS URL.")
        }
        guard !token.isEmpty else {
            throw RuneError.invalidInput(message: "Manual cluster token is required.")
        }

        let fallbackName = components.host ?? "manual-cluster"
        let contextName = normalizedManualKubeConfigName(from: manualKubeConfigName, fallback: fallbackName)
        let clusterName = "\(contextName)-cluster"
        let userName = "\(contextName)-user"
        let namespaceLine = namespace.isEmpty ? "" : "\n    namespace: \(yamlQuoted(namespace))"

        return """
        apiVersion: v1
        kind: Config
        current-context: \(yamlQuoted(contextName))
        clusters:
        - name: \(yamlQuoted(clusterName))
          cluster:
            server: \(yamlQuoted(server))
        contexts:
        - name: \(yamlQuoted(contextName))
          context:
            cluster: \(yamlQuoted(clusterName))
            user: \(yamlQuoted(userName))\(namespaceLine)
        users:
        - name: \(yamlQuoted(userName))
          user:
            token: \(yamlQuoted(token))
        """
    }

    private func normalizedManualKubeConfigName(from raw: String, fallback: String) -> String {
        let candidate = raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? fallback : raw
        let normalized = candidate
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .map { character -> Character in
                if character.isLetter || character.isNumber || character == "-" || character == "_" || character == "." {
                    return character
                }
                return "-"
            }
            .reduce(into: "") { partial, character in
                if character == "-", partial.last == "-" { return }
                partial.append(character)
            }
            .trimmingCharacters(in: CharacterSet(charactersIn: "-._"))
        return normalized.isEmpty ? "manual-cluster" : normalized
    }

    private func yamlQuoted(_ value: String) -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.withoutEscapingSlashes]
        guard let data = try? encoder.encode(value),
              let encoded = String(data: data, encoding: .utf8) else {
            return "\"\""
        }
        return encoded
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
        _ payloads: [(raw: String, sourceName: String)],
        logLabel: String
    ) async throws {
        let reviews = payloads.map { payload in
            kubeConfigImportValidator.validate(raw: payload.raw, sourceName: payload.sourceName)
        }
        kubeConfigImportReviews = reviews

        let blockingIssues = blockingKubeConfigImportIssues(in: reviews)
        guard blockingIssues.isEmpty else {
            setKubeConfigImportFailureAuthDoctorChecks(for: blockingIssues)
            throw RuneError.invalidInput(message: kubeConfigImportErrorMessage(for: blockingIssues))
        }

        persistKubeConfigImportContextPreferences(from: reviews)

        var importedFiles: [URL] = []
        for payload in payloads {
            let imported = try kubeConfigImportStore.saveImportedKubeConfig(
                raw: payload.raw,
                sourceName: payload.sourceName
            )
            try? bookmarkManager.addKubeConfig(url: imported)
            importedFiles.append(imported)
        }

        let sources = try resolvedKubeConfigSources(fallbackURLs: importedFiles)
        state.setSources(sources)
        latestKubeConfigSourceFingerprint = kubeConfigSourceFingerprint(for: sources)
        startKubeConfigSourceSync()
        diagnostics.log("\(logLabel) loaded payloads count=\(importedFiles.count), sources count=\(sources.count)")
        try await reloadContexts()
    }

    private func persistKubeConfigImportContextPreferences(from reviews: [KubeConfigImportReview]) {
        let contexts = reviews.flatMap(\.contexts)

        for context in contexts {
            if let namespace = context.namespace?.trimmingCharacters(in: .whitespacesAndNewlines),
               !namespace.isEmpty {
                contextPreferences.savePreferredNamespace(namespace, for: context.name)
            }
        }

        guard favoriteImportedKubeConfigContexts else { return }

        var favorites = state.favoriteContextNames
        for context in contexts {
            favorites.insert(context.name)
        }
        state.setFavoriteContextNames(favorites)
        contextPreferences.saveFavoriteContextNames(favorites)
    }

    private func blockingKubeConfigImportIssues(in reviews: [KubeConfigImportReview]) -> [KubeConfigImportIssue] {
        reviews
            .flatMap(\.issues)
            .filter { $0.severity == .error }
    }

    private func kubeConfigImportErrorMessage(for issues: [KubeConfigImportIssue]) -> String {
        issues.prefix(3).map(\.message).joined(separator: " ")
    }

    private func setKubeConfigImportFailureAuthDoctorChecks(for issues: [KubeConfigImportIssue]) {
        var checks = [
            RuneHealthCheck(
                id: "kubeconfig-import",
                title: "Kubeconfig import",
                status: .failed,
                message: "Import was stopped before saving access because the kubeconfig review found blocking issues."
            )
        ]
        checks.append(contentsOf: issues.prefix(5).map { issue in
            RuneHealthCheck(
                id: "kubeconfig-import-\(issue.id)",
                title: "Import review",
                status: .failed,
                message: issue.message
            )
        })
        state.setAuthDoctorChecks(checks)
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
        expectedClusterLoadGeneration: UUID? = nil
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
        diagnostics.log("reloadContexts contexts=\(contexts.count)")

        if let selected = state.selectedContext {
            rememberRecentContext(selected.name)
            // Keep current in-memory namespace only when staying on the same context.
            // If selected context changed (startup/new context list), start empty and let
            // `loadResourceSnapshot` resolve from context default + live namespace list.
            let requestedNamespace: String = selected.name == previousContextName ? previousNamespace : ""
            pendingNamespaceRevalidationContextName = selected.name == previousContextName ? nil : selected.name
            if state.selectedNamespace != requestedNamespace {
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
        let sources = try resolvedKubeConfigSources(fallbackURLs: discoveredURLs)
        let fingerprint = kubeConfigSourceFingerprint(for: sources)
        let sourceSetChanged = state.kubeConfigSources != sources
        let contentChanged = latestKubeConfigSourceFingerprint.map { $0 != fingerprint } ?? sourceSetChanged
        guard sourceSetChanged || contentChanged else { return false }

        latestKubeConfigSourceFingerprint = fingerprint
        if sourceSetChanged {
            state.setSources(sources)
            persistDiscoveredKubeConfigsInBackground(discoveredURLs)
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
        loadResourceDetailsForCurrentSelectionIfNeeded()
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
        if context.name == demoContextName {
            applyDemoClusterSnapshot()
            applyDemoResourceDetailsForCurrentSelection()
            return
        }

        do {
            diagnostics.trace(
                "refresh",
                "performRefreshCurrentView begin context=\(context.name) namespace=\(namespace) forceNamespaceMeta=\(forceNamespaceMetadataRefresh)"
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
                try await loadHelmReleases(context: context, namespace: state.selectedNamespace)
                await loadOperatorResources(context: context, namespace: state.selectedNamespace)
            }
            let currentFreshness = state.snapshotFreshness
            state.setSnapshotFreshness(
                RuneSnapshotFreshness(
                    status: currentFreshness.status == .stale ? .stale : .live,
                    updatedAt: Date(),
                    message: currentFreshness.status == .stale ? currentFreshness.message : "Live for \(context.name) / \(state.selectedNamespace)"
                )
            )
            diagnostics.trace("refresh", "performRefreshCurrentView done context=\(context.name)")
        } catch {
            if Self.isBenignCancellationError(error) {
                markOverviewCooldownBypass(contextName: context.name, namespace: namespace)
                state.setSnapshotFreshness(
                    RuneSnapshotFreshness(
                        status: .stale,
                        updatedAt: state.snapshotFreshness.updatedAt,
                        message: "Refresh cancelled; showing previous data."
                    )
                )
                diagnostics.trace("refresh", "performRefreshCurrentView cancelled")
                return
            }
            diagnostics.trace("refresh", "performRefreshCurrentView failed: \(error.localizedDescription)")
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
        navigationIndex -= 1
        applyNavigationCheckpoint(navigationHistory[navigationIndex])
        updateNavigationAvailability()
    }

    public func navigateForward() {
        guard canNavigateForward else { return }
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
        state.selectedSection = section
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

    private func setWorkloadKind(_ kind: KubeResourceKind, trackHistory: Bool, triggerReload: Bool) {
        guard kind != .event else { return }
        prepareNavigationMutation(trackHistory: trackHistory)
        cancelPendingLogReload()
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

    public func presentCommandPalette() {
        state.isCommandPalettePresented = true
    }

    public func dismissCommandPalette() {
        state.isCommandPalettePresented = false
    }

    @discardableResult
    public func reviewKubeConfigImport(raw: String, sourceName: String? = nil) -> KubeConfigImportReview {
        let review = kubeConfigImportValidator.validate(raw: raw, sourceName: sourceName)
        kubeConfigImportReviews = [review]
        return review
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
        diagnostics.trace("context", "setContext name=\(context.name) triggerReload=\(triggerReload)")
        overviewPrefetchTask?.cancel()
        contextOverviewPrefetchTask?.cancel()
        stopTerminalSession(resetState: true)
        cancelPendingLogReload()
        resourceDetailsTask?.cancel()
        let previousContextName = state.selectedContext?.name
        let isChangingContext = context.name != previousContextName
        state.selectedContext = context
        if isChangingContext {
            state.setOverviewClusterUsage(cpuPercent: nil, memoryPercent: nil)
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
                "checkpoint namespace=\(requestedPreferredNamespace) for context=\(context.name) until namespace list is loaded"
            )
            // Navigation checkpoint supplies a namespace string before `listNamespaces` has run for this context.
            state.selectedNamespace = requestedPreferredNamespace
        } else {
            state.selectedNamespace = ""
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
        stopTerminalSession(resetState: true)
        cancelPendingLogReload()
        resourceDetailsTask?.cancel()
        pendingNamespaceRevalidationContextName = nil
        state.selectedNamespace = trimmed
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
        guard let target = item.target else { return }
        openOverviewReference(target)
    }

    public func openOverviewDependency(_ item: OverviewDependencyItem) {
        guard let target = item.primaryTarget else { return }
        openOverviewReference(target)
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
        default:
            openOverviewModule(.pods)
        }
    }

    /// Sets section, namespace, and selection from an event `involvedObject` (workload or namespaced resource).
    public func openEventSource(_ event: EventSummary) {
        navigateFromEventFetchAttempts = 0
        let targetNs = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !targetNs.isEmpty && targetNs != state.selectedNamespace {
            pendingOpenEventSource = event
            setNamespace(targetNs, trackHistory: false, triggerReload: true)
            return
        }
        navigateToEventSource(event)
    }

    private func deferFetchOrShowEventDetail(event: EventSummary, showEventDetail: () -> Void) {
        if navigateFromEventFetchAttempts < 2 {
            navigateFromEventFetchAttempts += 1
            pendingOpenEventSource = event
            scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: false, debounced: false)
        } else {
            navigateFromEventFetchAttempts = 0
            showEventDetail()
        }
    }

    private func navigateToEventSource(_ event: EventSummary) {
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        let name = event.objectName.trimmingCharacters(in: .whitespacesAndNewlines)

        func showEventDetail() {
            setSection(.events, trackHistory: false, triggerReload: false)
            selectEvent(event, trackHistory: true)
        }

        switch kind {
        case "pod":
            if let pod = state.pods.first(where: { $0.name == name }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.pod, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectPod(pod, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "deployment":
            if let deployment = state.deployments.first(where: { $0.name == name }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.deployment, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectDeployment(deployment, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "statefulset":
            if let resource = state.statefulSets.first(where: { $0.name == name }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.statefulSet, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectStatefulSet(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "daemonset":
            if let resource = state.daemonSets.first(where: { $0.name == name }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.daemonSet, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectDaemonSet(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "job":
            if let resource = state.jobs.first(where: { $0.name == name }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.job, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectJob(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "cronjob":
            if let resource = state.cronJobs.first(where: { $0.name == name }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.cronJob, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectCronJob(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "replicaset":
            if let resource = state.replicaSets.first(where: { $0.name == name }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.replicaSet, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectReplicaSet(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "service":
            if let service = state.services.first(where: { $0.name == name }) {
                setSection(.networking, trackHistory: false, triggerReload: false)
                setWorkloadKind(.service, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectService(service, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "ingress":
            if let resource = state.ingresses.first(where: { $0.name == name }) {
                setSection(.networking, trackHistory: false, triggerReload: false)
                setWorkloadKind(.ingress, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectIngress(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "configmap":
            if let resource = state.configMaps.first(where: { $0.name == name }) {
                setSection(.config, trackHistory: false, triggerReload: false)
                setWorkloadKind(.configMap, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectConfigMap(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "secret":
            if let resource = state.secrets.first(where: { $0.name == name }) {
                setSection(.config, trackHistory: false, triggerReload: false)
                setWorkloadKind(.secret, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectSecret(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "node":
            if let resource = state.nodes.first(where: { $0.name == name }) {
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.node, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectNode(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "persistentvolumeclaim":
            if let resource = state.persistentVolumeClaims.first(where: { $0.name == name }) {
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.persistentVolumeClaim, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectPersistentVolumeClaim(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "persistentvolume":
            if let resource = state.persistentVolumes.first(where: { $0.name == name }) {
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.persistentVolume, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectPersistentVolume(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "storageclass":
            if let resource = state.storageClasses.first(where: { $0.name == name }) {
                setSection(.storage, trackHistory: false, triggerReload: false)
                setWorkloadKind(.storageClass, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectStorageClass(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "horizontalpodautoscaler":
            if let resource = state.horizontalPodAutoscalers.first(where: { $0.name == name }) {
                setSection(.workloads, trackHistory: false, triggerReload: false)
                setWorkloadKind(.horizontalPodAutoscaler, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectHorizontalPodAutoscaler(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "networkpolicy":
            if let resource = state.networkPolicies.first(where: { $0.name == name }) {
                setSection(.networking, trackHistory: false, triggerReload: false)
                setWorkloadKind(.networkPolicy, trackHistory: false, triggerReload: false)
                navigateFromEventFetchAttempts = 0
                selectNetworkPolicy(resource, trackHistory: true)
            } else {
                deferFetchOrShowEventDetail(event: event, showEventDetail: showEventDetail)
            }
        case "":
            if let pod = state.pods.first(where: { $0.name == name }) {
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
        if isManuallyMarkedProduction(context) {
            return true
        }

        let normalized = context.name.lowercased()
        let markers = ["prod", "production", "live", "critical"]
        return markers.contains { normalized.contains($0) }
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

    public func pageOperatorResourcesBackward() {
        operatorResourcePage = max(0, operatorResourcePage - 1)
    }

    public func pageOperatorResourcesForward() {
        guard canPageOperatorResourcesForward else { return }
        operatorResourcePage += 1
    }

    public func selectPod(_ pod: PodSummary?) {
        selectPod(pod, trackHistory: true)
    }

    public func focusTerminalPodInspector(_ pod: PodSummary, reloadLogs: Bool = false, loadDetails: Bool = false) {
        selectedLogContainer = ""
        selectPod(pod, trackHistory: false)
        if loadDetails {
            loadResourceDetailsForCurrentSelection()
        } else if reloadLogs {
            reloadLogsForSelection()
        }
    }

    private func selectPod(_ pod: PodSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedPod(pod)
        if !selectedLogContainer.isEmpty,
           pod?.containerNames.contains(selectedLogContainer) != true {
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
                    let logs = try await self.kubeClient.podLogs(
                        from: sources,
                        context: context,
                        namespace: namespace,
                        podName: pod.name,
                        container: container,
                        filter: filter,
                        previous: previous
                    )
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
                case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
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

            switch state.selectedWorkloadKind {
            case .pod:
                guard let pod = state.selectedPod else { return }
                _ = try exporter.save(
                    data: Data(state.podLogs.utf8),
                    suggestedName: "pod-\(pod.name)-logs-\(timestamp).log",
                    allowedFileTypes: ["log", "txt"]
                )
            case .service:
                guard let service = state.selectedService else { return }
                _ = try exporter.save(
                    data: Data(state.unifiedServiceLogs.utf8),
                    suggestedName: "service-\(service.name)-unified-logs-\(timestamp).log",
                    allowedFileTypes: ["log", "txt"]
                )
            case .deployment:
                guard let deployment = state.selectedDeployment else { return }
                _ = try exporter.save(
                    data: Data(state.unifiedServiceLogs.utf8),
                    suggestedName: "deployment-\(deployment.name)-unified-logs-\(timestamp).log",
                    allowedFileTypes: ["log", "txt"]
                )
            case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                return
            }
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func saveActiveTerminalTranscript() {
        guard let session = state.terminalSession,
              !session.transcript.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        else { return }

        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            let data = Self.terminalTranscriptData(session: session, generatedAt: timestamp)
            _ = try exporter.save(
                data: data,
                suggestedName: "terminal-\(Self.filenameComponent(session.namespace))-\(Self.filenameComponent(session.podName))-transcript-\(timestamp).log",
                allowedFileTypes: ["log", "txt"]
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func saveAllTerminalTranscriptsZip() {
        let sessions = state.terminalSessions.filter {
            !$0.transcript.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        }
        guard !sessions.isEmpty else { return }

        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            let data = try Self.terminalTranscriptArchiveData(sessions: sessions, generatedAt: timestamp)
            _ = try exporter.save(
                data: data,
                suggestedName: "terminal-transcripts-\(timestamp).zip",
                allowedFileTypes: ["zip"]
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
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

    public func saveAllPodsLogsZip() {
        saveCurrentLogsZip(limitToSelectedPods: false)
    }

    public func saveVisibleLogsZip(visibleText: String) {
        do {
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
            switch state.selectedWorkloadKind {
            case .pod:
                guard let pod = state.selectedPod else { return }
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
                _ = try exporter.save(
                    data: data,
                    suggestedName: "pod-\(pod.name)-visible-logs-\(timestamp).zip",
                    allowedFileTypes: ["zip"]
                )
            case .service:
                guard let service = state.selectedService else { return }
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
                _ = try exporter.save(
                    data: data,
                    suggestedName: "service-\(service.name)-visible-logs-\(timestamp).zip",
                    allowedFileTypes: ["zip"]
                )
            case .deployment:
                guard let deployment = state.selectedDeployment else { return }
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
                _ = try exporter.save(
                    data: data,
                    suggestedName: "deployment-\(deployment.name)-visible-logs-\(timestamp).zip",
                    allowedFileTypes: ["zip"]
                )
            case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                return
            }
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    private func saveCurrentLogsZip(limitToSelectedPods: Bool) {
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
                    _ = try self.exporter.save(
                        data: data,
                        suggestedName: "pod-\(pod.name)-full-logs-\(timestamp).zip",
                        allowedFileTypes: ["zip"]
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
                    _ = try self.exporter.save(
                        data: data,
                        suggestedName: "service-\(service.name)-full-logs-\(timestamp).zip",
                        allowedFileTypes: ["zip"]
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
                    _ = try self.exporter.save(
                        data: data,
                        suggestedName: "deployment-\(deployment.name)-full-logs-\(timestamp).zip",
                        allowedFileTypes: ["zip"]
                    )
                case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                    return
                }
            } catch {
                self.setExportErrorUnlessCancelled(error)
            }
        }
    }

    public func saveSelectedPodLogsZip() {
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
                _ = try self.exporter.save(
                    data: data,
                    suggestedName: "selected-pod-full-logs-\(timestamp).zip",
                    allowedFileTypes: ["zip"]
                )
            } catch {
                self.setExportErrorUnlessCancelled(error)
            }
        }
    }

    private func fullPodLogsZipData(
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

        for pod in pods {
            let containers: [String?] = pod.containerNames.isEmpty ? [nil] : pod.containerNames.map { Optional($0) }
            for container in containers {
                let logs = try await kubeClient.podLogs(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    podName: pod.name,
                    container: container,
                    filter: .all,
                    previous: previous
                )
                records.append(
                    PodLogArchiveRecord(
                        podName: pod.name,
                        containerName: container,
                        logs: logs
                    )
                )
            }
        }

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

                _ = try self.exporter.save(
                    data: data,
                    suggestedName: "\(baseName)-\(timestamp).zip",
                    allowedFileTypes: ["zip"]
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
        case .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
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
            guard let (kind, name) = currentWritableResource(), !state.resourceYAML.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")

            _ = try exporter.save(
                data: Data(state.resourceYAML.utf8),
                suggestedName: "\(kind.kubernetesResourceName)-\(name)-\(timestamp).yaml",
                allowedFileTypes: ["yaml", "yml"]
            )
        } catch {
            state.setError(error)
        }
    }

    public func saveCurrentResourceDescribe() {
        do {
            guard let (kind, name) = currentWritableResource(), !state.resourceDescribe.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")

            _ = try exporter.save(
                data: Data(state.resourceDescribe.utf8),
                suggestedName: "\(kind.kubernetesResourceName)-\(name)-describe-\(timestamp).txt",
                allowedFileTypes: ["txt", "log"]
            )
        } catch {
            state.setError(error)
        }
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
        do {
            let formatter = ISO8601DateFormatter()
            let exportStamp = formatter.string(from: Date()).replacingOccurrences(of: ":", with: "")
            let bundle = try supportBundleBuilder.buildBundle(
                from: SupportBundleRequest.snapshot(
                    state: state,
                    generatedAt: formatter.string(from: Date()),
                    resourceCounts: resourceCounts(),
                    selectedResourceKind: selectedResourceKindLabel(),
                    selectedResourceName: selectedResourceName()
                )
            )

            _ = try exporter.save(
                data: bundle,
                suggestedName: "support-bundle-\(exportStamp).json",
                allowedFileTypes: ["json"]
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func runAuthDoctor() {
        guard !state.isRunningAuthDoctor else { return }
        state.clearError()
        state.setAuthDoctorRunning(true)
        state.setAuthDoctorChecks([
            RuneHealthCheck(id: "start", title: "Auth Doctor", status: .running, message: "Checking kubeconfig, context, auth transport, namespace access, logs, exec, and port-forward permissions.")
        ])

        Task { @MainActor in
            var checks: [RuneHealthCheck] = []

            @MainActor
            func record(_ id: String, _ title: String, _ status: RuneHealthCheckStatus, _ message: String) {
                checks.removeAll { $0.id == id }
                checks.append(RuneHealthCheck(id: id, title: title, status: status, message: message))
                state.setAuthDoctorChecks(checks)
            }

            defer { state.setAuthDoctorRunning(false) }

            if state.selectedContext?.name == demoContextName {
                record("demo", "Demo cluster", .passed, "The in-memory demo cluster is active. Auth Doctor skips real Kubernetes API calls in demo mode.")
                return
            }

            guard !state.kubeConfigSources.isEmpty else {
                record("kubeconfig", "Kubeconfig", .failed, "No kubeconfig source is loaded. Import a kubeconfig or load the demo cluster.")
                return
            }
            record("kubeconfig", "Kubeconfig", .passed, "\(state.kubeConfigSources.count) source(s) loaded.")
            for check in AuthDoctorKubeconfigInspector().inspect(sources: state.kubeConfigSources) {
                record(check.id, check.title, check.status, check.message)
            }
            let contexts: [KubeContext]
            do {
                contexts = try await kubeClient.listContexts(from: state.kubeConfigSources)
                record("contexts", "Contexts", contexts.isEmpty ? .failed : .passed, contexts.isEmpty ? "No contexts were found." : "\(contexts.count) context(s) are readable.")
            } catch {
                record("contexts", "Contexts", .failed, error.localizedDescription)
                return
            }

            guard let context = state.selectedContext ?? contexts.first else {
                record("selected-context", "Selected context", .failed, "No selected context.")
                return
            }
            record("selected-context", "Selected context", .passed, context.name)

            @MainActor
            func recordCanI(
                _ id: String,
                _ title: String,
                namespace: String?,
                verb: String,
                resource: String,
                subresource: String? = nil
            ) async {
                do {
                    let allowed = try await kubeClient.canI(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: namespace,
                        verb: verb,
                        resource: resource,
                        subresource: subresource
                    )
                    let target = "\(resource)\(subresource.map { "/" + $0 } ?? "")"
                    let scope = namespace ?? "cluster scope"
                    record(
                        id,
                        title,
                        allowed ? .passed : .warning,
                        allowed ? "RBAC allows \(verb) \(target) in \(scope)." : "RBAC denied \(verb) \(target) in \(scope)."
                    )
                } catch {
                    record(id, title, .warning, "Could not verify RBAC with SelfSubjectAccessReview: \(error.localizedDescription)")
                }
            }

            do {
                let defaultNamespace = try await kubeClient.contextNamespace(from: state.kubeConfigSources, context: context)
                record("context-namespace", "Context namespace", .passed, defaultNamespace?.isEmpty == false ? defaultNamespace! : "No default namespace in kubeconfig; Rune will resolve one.")
            } catch {
                record("context-namespace", "Context namespace", .warning, error.localizedDescription)
            }

            do {
                let namespaces = try await kubeClient.listNamespaces(from: state.kubeConfigSources, context: context)
                record("namespace-list", "Namespace list", .passed, "\(namespaces.count) namespace(s) listed.")
                record("transport", "API transport", .passed, "API server, TLS/CA, proxy settings, and auth credentials worked for a live request.")
                record("exec-auth", "Exec auth", .passed, "Any kubeconfig exec/auth plugin needed for this request completed successfully.")
            } catch {
                record("namespace-list", "Namespace list", .warning, "Cannot list namespaces. Manual namespace mode can still work if RBAC allows access to a specific namespace. \(error.localizedDescription)")
            }

            let namespace = state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !namespace.isEmpty else {
                record("namespace", "Active namespace", .failed, "No namespace selected. Enter a namespace manually or select one from the menu.")
                return
            }
            record("namespace", "Active namespace", .passed, namespace)

            await recordCanI("rbac-pods-list", "RBAC pod list", namespace: namespace, verb: "list", resource: "pods")
            await recordCanI("rbac-pod-logs", "RBAC pod logs", namespace: namespace, verb: "get", resource: "pods", subresource: "log")
            await recordCanI("rbac-pod-exec", "RBAC pod exec", namespace: namespace, verb: "create", resource: "pods", subresource: "exec")
            await recordCanI("rbac-port-forward", "RBAC port-forward", namespace: namespace, verb: "create", resource: "pods", subresource: "portforward")

            do {
                let pods = try await kubeClient.listPods(from: state.kubeConfigSources, context: context, namespace: namespace)
                record("pod-list", "Pod list", .passed, "\(pods.count) pod(s) readable in namespace \(namespace).")
                record("transport", "API transport", .passed, "API server, TLS/CA, proxy settings, and auth credentials worked for a live request.")
                record("exec-auth", "Exec auth", .passed, "Any kubeconfig exec/auth plugin needed for this request completed successfully.")
                if let pod = pods.first {
                    do {
                        _ = try await kubeClient.podLogs(
                            from: state.kubeConfigSources,
                            context: context,
                            namespace: namespace,
                            podName: pod.name,
                            filter: .tailLines(20),
                            previous: false
                        )
                        record("pod-logs", "Pod logs", .passed, "Logs endpoint is reachable for a pod in \(namespace).")
                    } catch {
                        record("pod-logs", "Pod logs", .warning, error.localizedDescription)
                    }
                } else {
                    record("pod-logs", "Pod logs", .warning, "No pods found, so log access could not be verified.")
                }
            } catch {
                record("pod-list", "Pod list", .failed, error.localizedDescription)
            }
        }
    }

    public func clearAuthDoctorOutput() {
        guard !state.isRunningAuthDoctor else { return }
        state.clearAuthDoctorChecks()
    }

    public func loadDemoCluster() {
        guard UserDefaults.standard.runeEnableDemoCluster else {
            state.setError(RuneError.invalidInput(message: "Demo cluster is disabled in Settings."))
            return
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
            ClusterResourceSummary(kind: .daemonSet, name: "node-agent", namespace: "kube-system", primaryText: "2/2 ready", secondaryText: "Runs on demo nodes")
        ]
        let jobs = [
            ClusterResourceSummary(kind: .job, name: "data-backfill", namespace: "demo", primaryText: "Complete", secondaryText: "1 succeeded")
        ]
        let replicaSets = [
            ClusterResourceSummary(kind: .replicaSet, name: "api-6f78d9d7c9", namespace: "demo", primaryText: "1/1 ready", secondaryText: "Owned by Deployment/api")
        ]
        let horizontalPodAutoscalers = [
            ClusterResourceSummary(kind: .horizontalPodAutoscaler, name: "api", namespace: "demo", primaryText: "42% / 70%", secondaryText: "min 1, max 5")
        ]
        let services = [
            ServiceSummary(name: "api", namespace: "demo", type: "ClusterIP", clusterIP: "10.96.12.44", selector: ["app": "api"])
        ]
        let ingresses = [
            ClusterResourceSummary(kind: .ingress, name: "api", namespace: "demo", primaryText: "api.demo.invalid", secondaryText: "Service api:80")
        ]
        let networkPolicies = [
            ClusterResourceSummary(kind: .networkPolicy, name: "api-allow-web", namespace: "demo", primaryText: "Ingress", secondaryText: "Allows web to api")
        ]
        let events = [
            EventSummary(type: "Normal", reason: "Started", objectName: "api-6f78d9d7c9-2xkq8", message: "Started container api", lastTimestamp: "2026-05-05T10:00:00Z", involvedKind: "Pod", involvedNamespace: "demo"),
            EventSummary(type: "Warning", reason: "BackOff", objectName: "checkout-5d79f6c8b9-vx4lp", message: "Back-off restarting failed demo container", lastTimestamp: "2026-05-05T10:05:00Z", involvedKind: "Pod", involvedNamespace: "demo")
        ]
        let configMaps = [
            ClusterResourceSummary(kind: .configMap, name: "api-settings", namespace: "demo", primaryText: "3 keys", secondaryText: "Demo configuration")
        ]
        let secrets = [
            ClusterResourceSummary(kind: .secret, name: "api-token", namespace: "demo", primaryText: "2 keys", secondaryText: "Opaque")
        ]
        let cronJobs = [
            ClusterResourceSummary(kind: .cronJob, name: "nightly-report", namespace: "demo", primaryText: "0 2 * * *", secondaryText: "Suspended: false")
        ]
        let persistentVolumeClaims = [
            ClusterResourceSummary(kind: .persistentVolumeClaim, name: "postgres-data", namespace: "demo", primaryText: "Bound", secondaryText: "10Gi")
        ]
        let persistentVolumes = [
            ClusterResourceSummary(kind: .persistentVolume, name: "demo-pv-postgres", namespace: nil, primaryText: "Bound", secondaryText: "10Gi demo-retain")
        ]
        let storageClasses = [
            ClusterResourceSummary(kind: .storageClass, name: "demo-retain", namespace: nil, primaryText: "no-provisioner", secondaryText: "Retain")
        ]
        let nodes = [
            ClusterResourceSummary(kind: .node, name: "demo-node-a", namespace: nil, primaryText: "Ready", secondaryText: "Synthetic demo node"),
            ClusterResourceSummary(kind: .node, name: "demo-node-b", namespace: nil, primaryText: "Ready", secondaryText: "Synthetic demo node")
        ]
        let roles = [
            ClusterResourceSummary(kind: .role, name: "api-reader", namespace: "demo", primaryText: "get,list,watch", secondaryText: "pods/services")
        ]
        let roleBindings = [
            ClusterResourceSummary(kind: .roleBinding, name: "api-reader-binding", namespace: "demo", primaryText: "api-reader", secondaryText: "ServiceAccount/api")
        ]
        let clusterRoles = [
            ClusterResourceSummary(kind: .clusterRole, name: "demo-view", namespace: nil, primaryText: "read-only", secondaryText: "cluster scoped")
        ]
        let clusterRoleBindings = [
            ClusterResourceSummary(kind: .clusterRoleBinding, name: "demo-view-binding", namespace: nil, primaryText: "demo-view", secondaryText: "Group/demo-viewers")
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
            roleBindings: roleBindings,
            clusterRoles: clusterRoles,
            clusterRoleBindings: clusterRoleBindings
        )
        state.setHelmReleases(helmReleases)
        state.setOperatorResources(operatorResources)
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
        case .ingress:
            return state.selectedIngress.map { ($0.kind, $0.name, $0.namespace) }
        case .configMap:
            return state.selectedConfigMap.map { ($0.kind, $0.name, $0.namespace) }
        case .secret:
            return state.selectedSecret.map { ($0.kind, $0.name, $0.namespace) }
        case .node:
            return state.selectedNode.map { ($0.kind, $0.name, $0.namespace) }
        case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
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

    public func requestRolloutRestartSelectedDeployment() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let deployment = state.selectedDeployment else { return }
        pendingWriteAction = .rolloutRestart(deploymentName: deployment.name)
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
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let release = state.selectedHelmRelease else { return }

        let action = PendingWriteAction.helmRollback(
            releaseName: release.name,
            namespace: release.namespace,
            revision: revision,
            wait: true,
            timeout: "5m",
            cleanupOnFail: true
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

        let namespace = state.selectedNamespace
        let containerName = Self.normalizedTerminalContainerSelection(container, pod: pod)
        if replacingSessionID == nil,
           let existing = state.terminalSessions.first(where: {
               $0.contextName == context.name
                   && $0.namespace == namespace
                   && $0.podName == pod.name
                   && $0.containerName == containerName
           }) {
            state.selectTerminalSession(id: existing.id)
            terminalSessionInput = ""
            if existing.status == .disconnected || existing.status == .failed {
                startTerminalSession(for: pod, container: containerName, replacingSessionID: existing.id)
            }
            return
        }

        let sessionID = replacingSessionID ?? UUID().uuidString
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
                if replacingSessionID != nil {
                    await kubeClient.stopPodTerminalSession(id: sessionID)
                }
                try await kubeClient.startPodTerminalSession(
                    id: sessionID,
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: namespace,
                    podName: pod.name,
                    container: containerName,
                    shellCommand: terminalShellCommand,
                    onOutput: { [weak self] chunk in
                        Task { @MainActor [weak self] in
                            self?.enqueueTerminalSessionOutput(id: sessionID, text: chunk)
                        }
                    },
                    onTermination: { [weak self] exitCode in
                        Task { @MainActor [weak self] in
                            guard let self else { return }
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
                state.updateTerminalSessionStatus(id: sessionID, status: .connected)
                state.appendTerminalSessionOutput(
                    id: sessionID,
                    text: "[rune] Connected to \(pod.name) in \(namespace).\n"
                )
            } catch {
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
        case .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
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
        addressInput: String
    ) {
        Task {
            var portForwardRowsOwnError = false
            do {
                guard let context = state.selectedContext else { return }
                let localPort = try parsePort(localPortInput, fieldName: "local port")
                let remotePort = try parsePort(remotePortInput, fieldName: "remote port")
                let address = normalizedPortForwardAddress(addressInput)

                if let existing = activePortForwardSession(contextName: context.name, address: address, localPort: localPort) {
                    if existing.targetKind != targetKind || existing.targetName != targetName || existing.namespace != state.selectedNamespace {
                        state.upsertPortForwardSession(
                            PortForwardSession(
                                id: UUID().uuidString,
                                contextName: context.name,
                                namespace: state.selectedNamespace,
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
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
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
        clearPortForwardSession(session)
        startPortForward(
            targetKind: session.targetKind,
            targetName: session.targetName,
            localPortInput: String(session.localPort),
            remotePortInput: String(session.remotePort),
            addressInput: session.address
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
        pendingProductionDestructiveConfirmation = nil
        pendingWriteAction = nil
        pendingWriteDryRunStatus = nil
        pendingRollbackPlan = nil
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
        if isProductionContext,
           action.isDestructive,
           UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation,
           pendingProductionDestructiveConfirmation != action
        {
            pendingProductionDestructiveConfirmation = action
            return
        }
        pendingWriteAction = nil
        pendingProductionDestructiveConfirmation = nil
        pendingWriteDryRunStatus = nil
        pendingRollbackPlan = nil

        Task {
            do {
                guard let context = state.selectedContext else { return }
                let audit = auditDetails(for: action, context: context, namespace: state.selectedNamespace)

                switch action {
                case let .delete(kind, name):
                    try await kubeClient.deleteResource(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        kind: kind,
                        name: name
                    )
                case let .deleteMany(resources):
                    for resource in resources {
                        try await kubeClient.deleteResource(
                            from: state.kubeConfigSources,
                            context: context,
                            namespace: resource.namespace,
                            kind: resource.kind,
                            name: resource.name
                        )
                    }
                case let .apply(_, _, yaml, _):
                    if UserDefaults.standard.runeWriteSafetyRequireApplyDryRun {
                        let dryRunIssues = try await kubeClient.validateResourceYAML(
                            from: state.kubeConfigSources,
                            context: context,
                            namespace: state.selectedNamespace,
                            yaml: yaml
                        )
                        let blockingIssues = dryRunIssues.filter { $0.severity == .error }
                        guard blockingIssues.isEmpty else {
                            throw RuneError.invalidInput(message: "Server dry-run failed: \(blockingIssues.map(\.message).joined(separator: " "))")
                        }
                    }
                    try await kubeClient.applyYAML(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        yaml: yaml
                    )
                case let .scale(deploymentName, replicas):
                    try await kubeClient.scaleDeployment(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deploymentName: deploymentName,
                        replicas: replicas
                    )
                case let .rolloutRestart(deploymentName):
                    try await kubeClient.restartDeploymentRollout(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deploymentName: deploymentName
                    )
                case let .rolloutUndo(deploymentName, revision):
                    if UserDefaults.standard.runeWriteSafetyRequireRolloutDryRun {
                        try await kubeClient.dryRunRollbackDeploymentRollout(
                            from: state.kubeConfigSources,
                            context: context,
                            namespace: state.selectedNamespace,
                            deploymentName: deploymentName,
                            revision: revision
                        )
                    }
                    try await kubeClient.rollbackDeploymentRollout(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deploymentName: deploymentName,
                        revision: revision
                    )
                case .controllerRolloutUndo:
                    copyCommandToPasteboard(action.kubectlCommand(contextName: context.name, namespace: state.selectedNamespace))
                    appendWriteAudit(
                        audit,
                        status: "Blocked",
                        message: "Controller rollback command copied; Rune did not run this rollback automatically"
                    )
                    return
                case let .helmRollback(releaseName, namespace, revision, wait, timeout, cleanupOnFail):
                    let request = HelmRollbackRequest(
                        sources: state.kubeConfigSources,
                        contextName: context.name,
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
                    do {
                        try await loadHelmReleases(context: context, namespace: state.selectedNamespace)
                    } catch {
                        diagnostics.trace("helm", "post-rollback release refresh failed: \(error.localizedDescription)")
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
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        podName: podName,
                        container: nil,
                        command: command
                    )
                    state.setLastExecResult(result)
                    appendWriteAudit(audit, status: "Succeeded", message: "Command exited \(result.exitCode)")
                    return
                case let .createJobFromCronJob(cronJobName, jobName):
                    try await kubeClient.createJobFromCronJob(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        cronJobName: cronJobName,
                        jobName: jobName
                    )
                    setWorkloadKind(.job, trackHistory: false, triggerReload: true)
                }

                let requestID = beginSnapshotRequest(
                    context: context,
                    namespace: state.selectedNamespace,
                    source: "confirmPendingWriteAction"
                )
                try await loadResourceSnapshot(
                    context: context,
                    namespace: state.selectedNamespace,
                    requestID: requestID
                )
                appendWriteAudit(
                    audit,
                    status: "Succeeded",
                    message: successAuditMessage(
                        for: action,
                        verificationMessage: await postActionVerificationMessage(for: action, context: context)
                    )
                )
            } catch {
                if let context = state.selectedContext {
                    let audit = auditDetails(for: action, context: context, namespace: state.selectedNamespace)
                    appendWriteAudit(audit, status: "Failed", message: error.localizedDescription)
                }
                state.setError(error)
            }
        }
    }

    private func runPendingApplyDryRunPreview(yaml: String, action: PendingWriteAction) {
        guard UserDefaults.standard.runeWriteSafetyRequireApplyDryRun else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard case .apply = action,
              let context = state.selectedContext
        else {
            pendingWriteDryRunStatus = nil
            return
        }

        let namespace = state.selectedNamespace
        let sources = state.kubeConfigSources
        pendingWriteDryRunStatus = "Checking with Kubernetes API..."

        Task { [weak self] in
            guard let self else { return }
            do {
                let issues = try await self.kubeClient.validateResourceYAML(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    yaml: yaml
                )
                guard self.pendingWriteAction == action else { return }
                let errors = issues.filter { $0.severity == .error }
                if errors.isEmpty {
                    self.pendingWriteDryRunStatus = "Passed. Kubernetes accepted the server-side dry-run."
                } else {
                    self.pendingWriteDryRunStatus = "Blocked: \(errors.map(\.message).joined(separator: " "))"
                }
            } catch {
                guard self.pendingWriteAction == action else { return }
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
        guard let context = state.selectedContext else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard !state.kubeConfigSources.isEmpty else {
            pendingWriteDryRunStatus = "Could not complete: No kubeconfig selected."
            return
        }

        pendingWriteDryRunStatus = "Checking with Kubernetes API..."
        Task {
            do {
                try await kubeClient.dryRunRollbackDeploymentRollout(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    deploymentName: deploymentName,
                    revision: revision
                )
                guard self.pendingWriteAction == action else { return }
                self.pendingWriteDryRunStatus = "Server accepted rollback dry-run."
            } catch {
                guard self.pendingWriteAction == action else { return }
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
        guard let context = state.selectedContext else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard case let .helmRollback(releaseName, namespace, revision, wait, timeout, cleanupOnFail) = action else {
            pendingWriteDryRunStatus = nil
            return
        }
        guard !state.kubeConfigSources.isEmpty else {
            pendingWriteDryRunStatus = "Could not complete: No kubeconfig selected."
            return
        }

        pendingWriteDryRunStatus = "Checking with Helm dry-run..."
        Task {
            do {
                _ = try await helmCommandRunner.rollback(
                    HelmRollbackRequest(
                        sources: state.kubeConfigSources,
                        contextName: context.name,
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
                guard self.pendingWriteAction == action else { return }
                self.pendingWriteDryRunStatus = "Helm accepted rollback dry-run."
            } catch {
                guard self.pendingWriteAction == action else { return }
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
        let command = action.kubectlCommand(contextName: state.selectedContext?.name ?? "", namespace: state.selectedNamespace)

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
            "Command: \(action.kubectlCommand(contextName: state.selectedContext?.name ?? "", namespace: release.namespace))"
        ].joined(separator: "\n")
    }

    private func controllerRollbackPlan(
        for resource: ClusterResourceSummary,
        revision: Int?,
        action: PendingWriteAction
    ) -> String? {
        guard UserDefaults.standard.runeWriteSafetyShowRollbackPlan else { return nil }
        return [
            "Target resource: \(resource.kind.kubernetesResourceName)/\(resource.name)",
            "Namespace: \(resource.namespace ?? state.selectedNamespace)",
            "Target revision: \(revision.map(String.init) ?? "previous revision")",
            "Current status: \(resource.primaryText)",
            "Command: \(action.kubectlCommand(contextName: state.selectedContext?.name ?? "", namespace: resource.namespace ?? state.selectedNamespace))"
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
        case let .rolloutRestart(deploymentName):
            actionName = "Rollout Restart"
            resource = "deployment/\(deploymentName)"
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

    private func postActionVerificationMessage(for action: PendingWriteAction, context: KubeContext) async -> String? {
        guard UserDefaults.standard.runeWriteSafetyRequirePostActionVerification else { return nil }

        switch action {
        case let .rolloutUndo(deploymentName, _):
            do {
                let result = try await kubeClient.verifyDeploymentRollout(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
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

            guard let deployment = state.deployments.first(where: {
                $0.name == deploymentName && $0.namespace == state.selectedNamespace
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
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
            let data = try encoder.encode(visibleWriteAuditEntries)
            _ = try exporter.save(
                data: data,
                suggestedName: "write-audit-\(exportStamp).json",
                allowedFileTypes: ["json"]
            )
        } catch {
            setExportErrorUnlessCancelled(error)
        }
    }

    public func commandPaletteItems(query: String) -> [CommandPaletteItem] {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)

        if let commandItems = commandPaletteCommandItems(query: trimmedQuery) {
            return commandItems
        }

        let sections = RuneSection.allCases.map { section in
            CommandPaletteItem(
                id: "section:\(section.rawValue)",
                title: section.title,
                subtitle: "Switch section",
                symbolName: section.symbolName,
                action: .section(section)
            )
        }

        let contexts = visibleContexts.map { context in
            CommandPaletteItem(
                id: "context:\(context.name)",
                title: context.name,
                subtitle: "Switch context",
                symbolName: state.isFavorite(context) ? "star.fill" : "network",
                action: .context(context)
            )
        }

        let namespaces = namespaceOptions
            .map { namespace in
                CommandPaletteItem(
                    id: "namespace:\(namespace)",
                    title: namespace,
                    subtitle: "Switch namespace",
                    symbolName: "square.3.layers.3d",
                    action: .namespace(namespace)
                )
            }

        let pods = visiblePods.prefix(40).map { pod in
            CommandPaletteItem(
                id: "pod:\(pod.id)",
                title: pod.name,
                subtitle: "Open pod",
                symbolName: "cube.box",
                action: .pod(pod)
            )
        }

        let deployments = visibleDeployments.prefix(40).map { deployment in
            CommandPaletteItem(
                id: "deployment:\(deployment.id)",
                title: deployment.name,
                subtitle: "Open deployment",
                symbolName: "shippingbox",
                action: .deployment(deployment)
            )
        }

        let services = visibleServices.prefix(40).map { service in
            CommandPaletteItem(
                id: "service:\(service.id)",
                title: service.name,
                subtitle: "Open service",
                symbolName: "point.3.connected.trianglepath.dotted",
                action: .service(service)
            )
        }

        let events = visibleEvents.prefix(40).map { event in
            CommandPaletteItem(
                id: "event:\(event.id)",
                title: "\(event.reason) (\(event.type))",
                subtitle: event.objectName,
                symbolName: "bolt.badge.clock",
                action: .event(event)
            )
        }

        let helmReleases = visibleHelmReleases.prefix(40).map { release in
            CommandPaletteItem(
                id: "helm:\(release.id)",
                title: release.name,
                subtitle: "Open Helm release • \(release.namespace)",
                symbolName: "ferry",
                action: .helmRelease(release)
            )
        }

        let commands: [CommandPaletteItem] = [
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

        let allItems = commands + sections + contexts + namespaces + pods + deployments + services + helmReleases + events

        guard !trimmedQuery.isEmpty else {
            return Array(allItems.prefix(160))
        }

        return allItems.filter { item in
            matches("\(item.title) \(item.subtitle)", query: trimmedQuery)
        }
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
            case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
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

        if normalized.hasPrefix(":") {
            let tokens = normalized.dropFirst().split(whereSeparator: \.isWhitespace).map(String.init)
            if let command = tokens.first?.lowercased() {
                let remainder = tokens.dropFirst().joined(separator: " ").trimmingCharacters(in: .whitespacesAndNewlines)
                if !remainder.isEmpty {
                    switch command {
                    case "ns", "namespace", "namespaces":
                        if let exactNamespace = namespaceOptions.first(where: {
                            $0.compare(remainder, options: [.caseInsensitive, .diacriticInsensitive]) == .orderedSame
                        }) {
                            diagnostics.log(
                                "commandPalette query direct namespace context=\(state.selectedContext?.name ?? "none") from=\(state.selectedNamespace) query=\(remainder) matched=\(exactNamespace)"
                            )
                            setNamespace(exactNamespace)
                            dismissCommandPalette()
                            return
                        }
                        diagnostics.log(
                            "commandPalette query manual namespace context=\(state.selectedContext?.name ?? "none") from=\(state.selectedNamespace) query=\(remainder)"
                        )
                        setNamespace(remainder)
                        dismissCommandPalette()
                        return
                    case "ctx", "context", "contexts":
                        if let exactContext = visibleContexts.first(where: {
                            $0.name.compare(remainder, options: [.caseInsensitive, .diacriticInsensitive]) == .orderedSame
                        }) {
                            diagnostics.log(
                                "commandPalette query direct context from=\(state.selectedContext?.name ?? "none") query=\(remainder) matched=\(exactContext.name)"
                            )
                            setContext(exactContext)
                            dismissCommandPalette()
                            return
                        }
                    default:
                        break
                    }
                }
            }
        }

        guard let first = commandPaletteItems(query: normalized).first else { return }
        executeCommandPaletteItem(first)
    }

    private func loadResourceSnapshot(
        context: KubeContext,
        namespace: String,
        requestID: UUID,
        forceNamespaceMetadataRefresh: Bool = false
    ) async throws {
        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: namespace) else {
            markOverviewCooldownBypass(contextName: context.name, namespace: namespace)
            diagnostics.log("loadResourceSnapshot ignored stale start context=\(context.name) namespace=\(namespace)")
            diagnostics.trace(
                "snapshot.stale",
                "ignored start context=\(context.name) namespace=\(namespace) request=\(requestID.uuidString)"
            )
            return
        }

        state.isLoading = true
        defer { state.isLoading = false }

        try Task.checkCancellation()

        diagnostics.trace(
            "snapshot",
            "loadResourceSnapshot start context=\(context.name) namespace=\(namespace) forceMeta=\(forceNamespaceMetadataRefresh) request=\(requestID.uuidString)"
        )
        diagnostics.log("loadResourceSnapshot start context=\(context.name) namespace=\(namespace)")

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
            async let namespaceResult: Result<[String], Error> = Self.capture {
                try await kubeClient.listNamespaces(from: state.kubeConfigSources, context: context)
            }
            async let contextNamespaceResult: Result<String?, Error> = Self.capture {
                try await kubeClient.contextNamespace(from: state.kubeConfigSources, context: context)
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
                "discarded after namespace metadata context=\(context.name) namespace=\(namespace) request=\(requestID.uuidString)"
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
                "discarded before namespace apply context=\(context.name) namespace=\(namespace) request=\(requestID.uuidString)"
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
        let plan = SnapshotLoadPlan.forSelection(section: state.selectedSection, kind: state.selectedWorkloadKind)
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
        ) && !forceNamespaceMetadataRefresh && !bypassOverviewCooldown
        try Task.checkCancellation()

        let preservedRBACRoles = state.rbacRoles
        let preservedRBACRoleBindings = state.rbacRoleBindings
        let preservedRBACClusterRoles = state.rbacClusterRoles
        let preservedRBACClusterRoleBindings = state.rbacClusterRoleBindings
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
                return await kubeClient.clusterUsagePercent(from: state.kubeConfigSources, context: context)
            }
            return (currentOverviewClusterCPUPercent, currentOverviewClusterMemoryPercent)
        }()

        async let podResult: Result<[PodSummary], Error> = Self.capture {
            if plan.pods {
                return try await kubeClient.listPods(
                    from: state.kubeConfigSources,
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
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: effectiveNamespace
                )
            }
            return cachedSnapshot.pods
        }
        async let deploymentResult: Result<[DeploymentSummary], Error> = Self.capture {
            guard plan.deployments || shouldHydrateDeploymentsForOverview else { return cachedSnapshot.deployments }
            return try await kubeClient.listDeployments(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let deploymentCountResult: Result<Int, Error> = Self.capture {
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
                    from: state.kubeConfigSources,
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
        async let statefulSetResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.statefulSets else { return cachedSnapshot.statefulSets }
            return try await kubeClient.listStatefulSets(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let daemonSetResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.daemonSets else { return cachedSnapshot.daemonSets }
            return try await kubeClient.listDaemonSets(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let jobResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.jobs else { return cachedSnapshot.jobs }
            return try await kubeClient.listJobs(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let cronJobResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.cronJobs else { return cachedSnapshot.cronJobs }
            return try await kubeClient.listCronJobs(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let replicaSetResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.replicaSets else { return cachedSnapshot.replicaSets }
            return try await kubeClient.listReplicaSets(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let pvcResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.persistentVolumeClaims else { return cachedSnapshot.persistentVolumeClaims }
            return try await kubeClient.listPersistentVolumeClaims(
                from: state.kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let pvResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.persistentVolumes else { return cachedPersistentVolumes }
            return try await kubeClient.listPersistentVolumes(from: state.kubeConfigSources, context: context)
        }
        async let storageClassResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.storageClasses else { return cachedStorageClasses }
            return try await kubeClient.listStorageClasses(from: state.kubeConfigSources, context: context)
        }
        async let hpaResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.horizontalPodAutoscalers else { return cachedSnapshot.horizontalPodAutoscalers }
            return try await kubeClient.listHorizontalPodAutoscalers(
                from: state.kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let networkPolicyResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.networkPolicies else { return cachedSnapshot.networkPolicies }
            return try await kubeClient.listNetworkPolicies(
                from: state.kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let serviceResult: Result<[ServiceSummary], Error> = Self.capture {
            guard plan.services || shouldHydrateServicesForOverview else { return cachedSnapshot.services }
            return try await kubeClient.listServices(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let serviceCountResult: Result<Int, Error> = Self.capture {
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
                    from: state.kubeConfigSources,
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
        async let ingressResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.ingresses else { return cachedSnapshot.ingresses }
            return try await kubeClient.listIngresses(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let ingressCountResult: Result<Int, Error> = Self.capture {
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
                    from: state.kubeConfigSources,
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
        async let configMapResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.configMaps else { return cachedSnapshot.configMaps }
            return try await kubeClient.listConfigMaps(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let configMapCountResult: Result<Int, Error> = Self.capture {
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
                    from: state.kubeConfigSources,
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
        async let cronJobsCountResult: Result<Int, Error> = Self.capture {
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
                    from: state.kubeConfigSources,
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
        async let secretResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.secrets else { return cachedSnapshot.secrets }
            return try await kubeClient.listSecrets(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let nodeResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.nodes else { return cachedNodes }
            return try await kubeClient.listNodes(from: state.kubeConfigSources, context: context)
        }
        async let nodeCountResult: Result<Int, Error> = Self.capture {
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
                    from: state.kubeConfigSources,
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
        async let eventResult: Result<[EventSummary], Error> = Self.capture {
            guard plan.events else { return cachedSnapshot.events }
            if !cachedSnapshot.events.isEmpty {
                return cachedSnapshot.events
            }
            if let warmOverview {
                return warmOverview.events
            }
            return try await kubeClient.listEvents(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let rbacRolesResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.rbacRoles else { return preservedRBACRoles }
            return try await kubeClient.listRoles(
                from: state.kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let rbacRoleBindingsResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.rbacRoleBindings else { return preservedRBACRoleBindings }
            return try await kubeClient.listRoleBindings(
                from: state.kubeConfigSources,
                context: context,
                namespace: effectiveNamespace
            )
        }
        async let rbacClusterRolesResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.rbacClusterRoles else { return preservedRBACClusterRoles }
            return try await kubeClient.listClusterRoles(from: state.kubeConfigSources, context: context)
        }
        async let rbacClusterRoleBindingsResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.rbacClusterRoleBindings else { return preservedRBACClusterRoleBindings }
            return try await kubeClient.listClusterRoleBindings(from: state.kubeConfigSources, context: context)
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
                "discarded after core resource fetch context=\(context.name) effectiveNamespace=\(effectiveNamespace) request=\(requestID.uuidString) selectedNamespace=\(state.selectedNamespace)"
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
            ingresses: loadedIngresses,
            configMaps: loadedConfigMaps,
            secrets: loadedSecrets,
            events: loadedEvents
        )

        state.setPods(loadedPods)
        if plan.pods, !loadedPods.isEmpty {
            Task { [weak self] in
                await self?.applyPodsJSONEnrichmentIfCurrent(
                    requestID: requestID,
                    context: context,
                    namespace: effectiveNamespace,
                    basePods: loadedPods
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
        state.setIngresses(loadedIngresses)
        state.setConfigMaps(loadedConfigMaps)
        state.setSecrets(loadedSecrets)
        state.setNodes(loadedNodes)
        state.setEvents(loadedEvents)
        if plan.rbacRoles {
            state.setRBACData(
                roles: loadedRBACRoles,
                roleBindings: loadedRBACRoleBindings,
                clusterRoles: loadedRBACClusterRoles,
                clusterRoleBindings: loadedRBACClusterRoleBindings
            )
        }

        if let deployment = state.selectedDeployment {
            scaleReplicaInput = max(0, deployment.desiredReplicas)
        }

        if warnings.isEmpty {
            state.clearError()
        } else {
            let warningText = warnings.joined(separator: " | ")
            state.setErrorMessage("Partial load: \(warningText)")
            state.setSnapshotFreshness(
                RuneSnapshotFreshness(
                    status: .stale,
                    updatedAt: state.snapshotFreshness.updatedAt,
                    message: "Partial load: \(warningText)"
                )
            )
            diagnostics.log("loadResourceSnapshot partial warnings: \(warningText)")
            diagnostics.trace("snapshot", "partial load warnings: \(warningText)")
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
            if !loadedNamespaces.isEmpty {
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
                requestID: requestID
            )
            diagnostics.trace(
                "snapshot.overview",
                "skipped overview write section=\(state.selectedSection.rawValue) context=\(context.name) namespace=\(effectiveNamespace)"
            )
        }

        // After primary snapshot work, optionally warm a few non-selected contexts so sidebar/context
        // switching can reuse overview cache immediately.
        if plan.podStatuses || forceNamespaceMetadataRefresh {
            scheduleContextOverviewPrefetch(currentContext: context)
        }

        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: effectiveNamespace) else {
            diagnostics.log("loadResourceSnapshot skipped details for stale context=\(context.name) namespace=\(namespace)")
            diagnostics.trace(
                "snapshot.stale",
                "skipped resource details context=\(context.name) namespace=\(effectiveNamespace) request=\(requestID.uuidString)"
            )
            return
        }

        if shouldLoadResourceDetailsForCurrentSection {
            let requestID = UUID()
            latestResourceDetailsRequestID = requestID
            state.beginResourceDetailLoad()
            await loadResourceDetailsForCurrentSelectionAsync(requestID: requestID)
        } else {
            diagnostics.log("loadResourceSnapshot skipped heavy resource details for section=\(state.selectedSection.rawValue)")
        }

        if let pending = pendingOpenEventSource {
            pendingOpenEventSource = nil
            navigateToEventSource(pending)
        }

        diagnostics.log("loadResourceSnapshot done context=\(context.name) namespace=\(namespace)")
        diagnostics.trace("snapshot", "loadResourceSnapshot done context=\(context.name) namespace=\(effectiveNamespace)")
    }

    /// Second snapshot pass: merge full pod JSON so the inspector shows IP, node, QoS, and readiness while keeping CPU/mem from the first pass.
    private func applyPodsJSONEnrichmentIfCurrent(
        requestID: UUID,
        context: KubeContext,
        namespace: String,
        basePods: [PodSummary]
    ) async {
        guard snapshotRequestIsCurrent(requestID, context: context, expectedNamespace: namespace) else { return }
        guard state.selectedNamespace == namespace else { return }
        do {
            let merged = try await kubeClient.enrichPodsWithJSONList(
                from: state.kubeConfigSources,
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
                ingresses: snap.ingresses,
                configMaps: snap.configMaps,
                secrets: snap.secrets,
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
                "loadOverviewSnapshot discarded context=\(context.name) namespace=\(namespace) request=\(requestID.uuidString)"
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
            "applied overview tiles namespace=\(namespace) context=\(context.name) pods=\(pods.count) deployments=\(deploymentsCount) services=\(servicesCount) ingresses=\(ingressesCount) configmaps=\(configMapsCount) cronjobs=\(cronJobsCount) nodes=\(nodesCount)"
        )
    }

    private func refreshClusterUsageForHeaderIfNeeded(
        context: KubeContext,
        namespace: String,
        requestID: UUID
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
            let usage = await self.kubeClient.clusterUsagePercent(from: self.state.kubeConfigSources, context: context)
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

        var merged: [String: KubeConfigSource] = [:]
        for source in bookmarked + fallback {
            let standardizedPath = source.url.standardizedFileURL.path
            merged[standardizedPath] = source
        }

        return merged.values.sorted { $0.path < $1.path }
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

    private func loadResourceDetailsForCurrentSelection() {
        resourceDetailsTask?.cancel()
        let requestID = UUID()
        latestResourceDetailsRequestID = requestID
        state.beginResourceDetailLoad()
        diagnostics.log("resourceDetails start request=\(requestID.uuidString) section=\(state.selectedSection.rawValue) kind=\(state.selectedWorkloadKind.rawValue) namespace=\(state.selectedNamespace)")

        resourceDetailsTask = Task { [weak self] in
            guard let self else { return }
            await self.loadResourceDetailsForCurrentSelectionAsync(requestID: requestID)
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
        case .ingress:
            return state.selectedIngress != nil
        case .configMap:
            return state.selectedConfigMap != nil
        case .secret:
            return state.selectedSecret != nil
        case .node:
            return state.selectedNode != nil
        case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
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
            state.setResourceYAMLError(
                resourceDetailsFailureMessage(action: "load YAML for", kind: kind, name: name, error: error)
            )
        }

        switch pair.describe {
        case let .success(describe):
            state.setResourceDescribe(normalizeLoadedResourceText(describe))
        case let .failure(error):
            state.setResourceDescribeError(
                resourceDetailsFailureMessage(action: "load describe for", kind: kind, name: name, error: error)
            )
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
            "async begin request=\(requestID.uuidString) kind=\(state.selectedWorkloadKind.rawValue) section=\(state.selectedSection.rawValue) namespace=\(state.selectedNamespace)"
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
                async let logsResult = captureResult {
                    try await self.kubeClient.podLogs(
                        from: self.state.kubeConfigSources,
                        context: context,
                        namespace: self.state.selectedNamespace,
                        podName: pod.name,
                        container: self.selectedLogContainerName,
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

        case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
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

    private func loadHelmReleases(context: KubeContext, namespace: String) async throws {
        state.isLoading = true
        defer { state.isLoading = false }

        let releases = try await kubeClient.listReleases(
            from: state.kubeConfigSources,
            context: context,
            namespace: state.isHelmAllNamespaces ? nil : namespace,
            allNamespaces: state.isHelmAllNamespaces
        )

        state.setHelmReleases(releases)
        await loadHelmDetailsForCurrentSelectionAsync()
    }

    private func loadOperatorResources(context: KubeContext, namespace: String) async {
        let resources = (try? await kubeClient.listOperatorResources(
            from: state.kubeConfigSources,
            context: context,
            namespace: namespace
        )) ?? []
        guard state.selectedContext == context, state.selectedNamespace == namespace else { return }
        operatorResourcePage = 0
        state.setOperatorResources(resources)
    }

    private func loadHelmDetailsForCurrentSelection() {
        Task {
            await loadHelmDetailsForCurrentSelectionAsync()
        }
    }

    private func loadOperatorResourceDetailsForCurrentSelection() {
        resourceDetailsTask?.cancel()
        let requestID = UUID()
        latestResourceDetailsRequestID = requestID
        state.beginResourceDetailLoad()

        resourceDetailsTask = Task { [weak self] in
            guard let self else { return }
            await self.loadOperatorResourceDetailsForCurrentSelectionAsync(requestID: requestID)
            if self.isCurrentResourceDetailsRequest(requestID) {
                self.resourceDetailsTask = nil
            }
        }
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
            state.setResourceYAMLError(
                resourceDetailsFailureMessage(action: "load YAML for", kind: resource.kind, name: resource.name, error: error)
            )
        }

        switch pair.describe {
        case let .success(describe):
            state.setResourceDescribe(normalizeLoadedResourceText(describe))
        case let .failure(error):
            state.setResourceDescribeError(
                resourceDetailsFailureMessage(action: "load describe for", kind: resource.kind, name: resource.name, error: error)
            )
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
        case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            guard let resource = state.selectedRBACResource else { return nil }
            return (resource.kind, resource.name)
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
            selectedIngressName: state.selectedIngress?.name,
            selectedConfigMapName: state.selectedConfigMap?.name,
            selectedSecretName: state.selectedSecret?.name,
            selectedNodeName: state.selectedNode?.name,
            selectedRBACResourceID: state.selectedRBACResource?.id
        )
    }

    private func prepareNavigationMutation(trackHistory: Bool) {
        cancelObsoleteSelectionRequests()
        guard trackHistory, !isApplyingNavigationCheckpoint, navigationHistory.isEmpty else { return }
        navigationHistory.append(currentNavigationCheckpoint())
        navigationIndex = 0
        updateNavigationAvailability()
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

        Task { @MainActor [weak self] in
            try? await Task.sleep(nanoseconds: 250_000_000)
            self?.restoreSelection(from: checkpoint)
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
        case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            let lists = state.rbacRoles + state.rbacRoleBindings + state.rbacClusterRoles + state.rbacClusterRoleBindings
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

        async let namespaceResult: Result<[String], Error> = Self.capture {
            try await kubeClient.listNamespaces(from: sources, context: context)
        }
        async let contextNamespaceResult: Result<String?, Error> = Self.capture {
            try await kubeClient.contextNamespace(from: sources, context: context)
        }

        let contextDefaultNamespace: String?
        switch await contextNamespaceResult {
        case let .success(value):
            contextDefaultNamespace = value
        case let .failure(error):
            diagnostics.trace(
                "prefetch.context",
                "context-namespace failed context=\(context.name): \(error.localizedDescription)"
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
                "namespaces failed context=\(context.name): \(error.localizedDescription)"
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
                        "warmed context=\(targetContext.name) namespace=\(normalizedNamespace)"
                    )
                } catch {
                    self.diagnostics.trace(
                        "prefetch.context",
                        "failed context=\(targetContext.name) namespace=\(normalizedNamespace): \(error.localizedDescription)"
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
                            "failed context=\(contextName) namespace=\(namespace): \(error.localizedDescription)"
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
            state.setIngresses([])
            state.setConfigMaps([])
            state.setSecrets([])
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
        state.setIngresses(cached.ingresses)
        state.setConfigMaps(cached.configMaps)
        state.setSecrets(cached.secrets)
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
        let ascending = podSortAscending
        let sortColumn = podSortColumn

        return records.sorted { lhs, rhs in
            if lhs.isFavorite != rhs.isFavorite {
                return lhs.isFavorite && !rhs.isFavorite
            }

            let nameOrder = lhs.pod.name.localizedCaseInsensitiveCompare(rhs.pod.name) == .orderedAscending
            switch sortColumn {
            case .name:
                return ascending ? nameOrder : !nameOrder
            case .status:
                let statusOrder: Bool = {
                    if lhs.pod.status != rhs.pod.status {
                        return lhs.pod.status.localizedCaseInsensitiveCompare(rhs.pod.status) == .orderedAscending
                    }
                    return nameOrder
                }()
                return ascending ? statusOrder : !statusOrder
            case .restarts:
                return comparePodsMetric(
                    ascending: ascending,
                    lhsValue: lhs.pod.totalRestarts,
                    rhsValue: rhs.pod.totalRestarts,
                    tieBreak: nameOrder
                )
            case .cpu:
                return comparePodsOptionalMetric(
                    ascending: ascending,
                    lhsValue: lhs.cpuMilli,
                    rhsValue: rhs.cpuMilli,
                    tieBreak: nameOrder
                )
            case .memory:
                return comparePodsOptionalMetric(
                    ascending: ascending,
                    lhsValue: lhs.memoryBytes,
                    rhsValue: rhs.memoryBytes,
                    tieBreak: nameOrder
                )
            case .age:
                return comparePodsOptionalMetric(
                    ascending: ascending,
                    lhsValue: lhs.ageSeconds,
                    rhsValue: rhs.ageSeconds,
                    tieBreak: nameOrder
                )
            }
        }.map(\.pod)
    }

    private func podComparator(_ lhs: PodSummary, _ rhs: PodSummary) -> Bool {
        if let favoriteOrder = resourceFavoriteOrder(
            kind: .pod,
            lhsNamespace: lhs.namespace,
            lhsName: lhs.name,
            rhsNamespace: rhs.namespace,
            rhsName: rhs.name
        ) {
            return favoriteOrder
        }

        let ascending = podSortAscending
        let nameOrder = lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending

        switch podSortColumn {
        case .name:
            return ascending ? nameOrder : !nameOrder
        case .status:
            let statusOrder: Bool = {
                if lhs.status != rhs.status {
                    return lhs.status.localizedCaseInsensitiveCompare(rhs.status) == .orderedAscending
                }
                return nameOrder
            }()
            return ascending ? statusOrder : !statusOrder
        case .restarts:
            return comparePodsMetric(
                ascending: ascending,
                lhsValue: lhs.totalRestarts,
                rhsValue: rhs.totalRestarts,
                tieBreak: nameOrder
            )
        case .cpu:
            return comparePodsOptionalMetric(
                ascending: ascending,
                lhsValue: cpuMilliValue(lhs.cpuUsage),
                rhsValue: cpuMilliValue(rhs.cpuUsage),
                tieBreak: nameOrder
            )
        case .memory:
            return comparePodsOptionalMetric(
                ascending: ascending,
                lhsValue: memoryByteValue(lhs.memoryUsage),
                rhsValue: memoryByteValue(rhs.memoryUsage),
                tieBreak: nameOrder
            )
        case .age:
            return comparePodsOptionalMetric(
                ascending: ascending,
                lhsValue: ageSeconds(lhs.ageDescription),
                rhsValue: ageSeconds(rhs.ageDescription),
                tieBreak: nameOrder
            )
        }
    }

    private func deploymentComparator(_ lhs: DeploymentSummary, _ rhs: DeploymentSummary) -> Bool {
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
        let nameOrder = lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending

        switch deploymentSortColumn {
        case .name:
            return ascending ? nameOrder : !nameOrder
        case .replicas:
            let lhsRatio = replicaReadinessRatio(lhs)
            let rhsRatio = replicaReadinessRatio(rhs)
            if lhsRatio != rhsRatio {
                return ascending ? (lhsRatio < rhsRatio) : (lhsRatio > rhsRatio)
            }
            if lhs.readyReplicas != rhs.readyReplicas {
                return ascending ? (lhs.readyReplicas < rhs.readyReplicas) : (lhs.readyReplicas > rhs.readyReplicas)
            }
            if lhs.desiredReplicas != rhs.desiredReplicas {
                return ascending ? (lhs.desiredReplicas < rhs.desiredReplicas) : (lhs.desiredReplicas > rhs.desiredReplicas)
            }
            return nameOrder
        }
    }

    private func serviceComparator(_ lhs: ServiceSummary, _ rhs: ServiceSummary) -> Bool {
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
        let nameOrder = lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending

        let orderedAscending: Bool
        switch serviceSortColumn {
        case .name:
            orderedAscending = nameOrder
        case .type:
            if lhs.type != rhs.type {
                orderedAscending = lhs.type.localizedCaseInsensitiveCompare(rhs.type) == .orderedAscending
            } else {
                orderedAscending = nameOrder
            }
        case .clusterIP:
            if lhs.clusterIP != rhs.clusterIP {
                orderedAscending = compareIPv4(lhs.clusterIP, rhs.clusterIP) ?? (lhs.clusterIP.localizedStandardCompare(rhs.clusterIP) == .orderedAscending)
            } else {
                orderedAscending = nameOrder
            }
        }

        return ascending ? orderedAscending : !orderedAscending
    }

    private func compareIPv4(_ lhs: String, _ rhs: String) -> Bool? {
        let left = lhs.split(separator: ".").compactMap { Int($0) }
        let right = rhs.split(separator: ".").compactMap { Int($0) }
        guard left.count == 4, right.count == 4 else { return nil }
        for (leftPart, rightPart) in zip(left, right) where leftPart != rightPart {
            return leftPart < rightPart
        }
        return false
    }

    private func genericResourceSorted(_ values: [ClusterResourceSummary]) -> [ClusterResourceSummary] {
        values
            .map { resource in
                (
                    resource: resource,
                    isFavorite: isFavoriteResource(kind: resource.kind, namespace: resource.namespace, name: resource.name)
                )
            }
            .sorted { lhs, rhs in
                if lhs.isFavorite != rhs.isFavorite {
                    return lhs.isFavorite && !rhs.isFavorite
                }
                return genericResourceComparator(lhs.resource, rhs.resource)
            }
            .map(\.resource)
    }

    private func genericResourceComparator(_ lhs: ClusterResourceSummary, _ rhs: ClusterResourceSummary) -> Bool {
        let ascending = genericResourceSortAscending
        let nameOrder = lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending

        let orderedAscending: Bool?
        switch genericResourceSortColumn {
        case .name:
            orderedAscending = nameOrder
        case .primary:
            if let primaryOrder = numericPrefixOrder(lhs.primaryText, rhs.primaryText), primaryOrder != .orderedSame {
                orderedAscending = primaryOrder == .orderedAscending
            } else if lhs.primaryText != rhs.primaryText {
                orderedAscending = lhs.primaryText.localizedStandardCompare(rhs.primaryText) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .secondary:
            if lhs.secondaryText != rhs.secondaryText {
                orderedAscending = lhs.secondaryText.localizedStandardCompare(rhs.secondaryText) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .namespace:
            let lhsNamespace = lhs.namespace ?? ""
            let rhsNamespace = rhs.namespace ?? ""
            if lhsNamespace != rhsNamespace {
                orderedAscending = lhsNamespace.localizedCaseInsensitiveCompare(rhsNamespace) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        }

        guard let orderedAscending else { return nameOrder }
        return ascending ? orderedAscending : !orderedAscending
    }

    private func helmReleaseComparator(_ lhs: HelmReleaseSummary, _ rhs: HelmReleaseSummary) -> Bool {
        let ascending = helmReleaseSortAscending
        let nameOrder = lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending

        let orderedAscending: Bool?
        switch helmReleaseSortColumn {
        case .name:
            orderedAscending = nameOrder
        case .status:
            if lhs.status != rhs.status {
                orderedAscending = lhs.status.localizedCaseInsensitiveCompare(rhs.status) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .namespace:
            if lhs.namespace != rhs.namespace {
                orderedAscending = lhs.namespace.localizedCaseInsensitiveCompare(rhs.namespace) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .revision:
            if lhs.revision != rhs.revision {
                orderedAscending = lhs.revision < rhs.revision
            } else {
                orderedAscending = nil
            }
        case .chart:
            if lhs.chart != rhs.chart {
                orderedAscending = lhs.chart.localizedStandardCompare(rhs.chart) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .appVersion:
            if lhs.appVersion != rhs.appVersion {
                orderedAscending = lhs.appVersion.localizedStandardCompare(rhs.appVersion) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        }

        guard let orderedAscending else { return nameOrder }
        return ascending ? orderedAscending : !orderedAscending
    }

    private func eventComparator(_ lhs: EventSummary, _ rhs: EventSummary) -> Bool {
        let ascending = eventSortAscending
        let reasonOrder = lhs.reason.localizedCaseInsensitiveCompare(rhs.reason) == .orderedAscending

        let orderedAscending: Bool?
        switch eventSortColumn {
        case .reason:
            orderedAscending = reasonOrder
        case .type:
            if lhs.type != rhs.type {
                orderedAscending = lhs.type.localizedCaseInsensitiveCompare(rhs.type) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .object:
            if lhs.objectName != rhs.objectName {
                orderedAscending = lhs.objectName.localizedStandardCompare(rhs.objectName) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .namespace:
            let lhsNamespace = lhs.involvedNamespace ?? ""
            let rhsNamespace = rhs.involvedNamespace ?? ""
            if lhsNamespace != rhsNamespace {
                orderedAscending = lhsNamespace.localizedCaseInsensitiveCompare(rhsNamespace) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .lastSeen:
            let lhsTimestamp = lhs.lastTimestamp ?? ""
            let rhsTimestamp = rhs.lastTimestamp ?? ""
            if lhsTimestamp != rhsTimestamp {
                orderedAscending = lhsTimestamp.localizedStandardCompare(rhsTimestamp) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        }

        guard let orderedAscending else { return reasonOrder }
        return ascending ? orderedAscending : !orderedAscending
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

    /// Missing metrics sort last regardless of ascending/descending direction.
    private func comparePodsOptionalMetric<T: Comparable>(
        ascending: Bool,
        lhsValue: T?,
        rhsValue: T?,
        tieBreak: Bool
    ) -> Bool {
        switch (lhsValue, rhsValue) {
        case let (l?, r?):
            if l != r {
                return ascending ? (l < r) : (l > r)
            }
            return tieBreak
        case (_?, nil):
            return true
        case (nil, _?):
            return false
        case (nil, nil):
            return tieBreak
        }
    }

    private func comparePodsMetric(
        ascending: Bool,
        lhsValue: Int,
        rhsValue: Int,
        tieBreak: Bool
    ) -> Bool {
        if lhsValue != rhsValue {
            return ascending ? (lhsValue < rhsValue) : (lhsValue > rhsValue)
        }
        return tieBreak
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
        values.sorted(by: operatorResourceComparator)
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

    private func operatorResourceComparator(_ lhs: OperatorResourceSummary, _ rhs: OperatorResourceSummary) -> Bool {
        let lhsFavorite = isFavoriteOperatorResource(lhs)
        let rhsFavorite = isFavoriteOperatorResource(rhs)
        if lhsFavorite != rhsFavorite {
            return lhsFavorite && !rhsFavorite
        }

        let ascending = operatorResourceSortAscending
        let defaultOrder: Bool = {
            if lhs.family != rhs.family {
                return lhs.family.localizedCaseInsensitiveCompare(rhs.family) == .orderedAscending
            }
            if lhs.kind != rhs.kind {
                return lhs.kind.localizedCaseInsensitiveCompare(rhs.kind) == .orderedAscending
            }
            return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
        }()

        let orderedAscending: Bool?
        switch operatorResourceSortColumn {
        case .family:
            orderedAscending = defaultOrder
        case .kind:
            if lhs.kind != rhs.kind {
                orderedAscending = lhs.kind.localizedCaseInsensitiveCompare(rhs.kind) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .name:
            if lhs.name != rhs.name {
                orderedAscending = lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .namespace:
            let lhsNamespace = lhs.namespace ?? ""
            let rhsNamespace = rhs.namespace ?? ""
            if lhsNamespace != rhsNamespace {
                orderedAscending = lhsNamespace.localizedCaseInsensitiveCompare(rhsNamespace) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .status:
            if lhs.status != rhs.status {
                orderedAscending = lhs.status.localizedCaseInsensitiveCompare(rhs.status) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        case .apiPath:
            if lhs.apiPath != rhs.apiPath {
                orderedAscending = lhs.apiPath.localizedStandardCompare(rhs.apiPath) == .orderedAscending
            } else {
                orderedAscending = nil
            }
        }

        guard let orderedAscending else { return defaultOrder }
        return ascending ? orderedAscending : !orderedAscending
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
        let normalizedText = text.lowercased()
        let tokens = query.lowercased().split(whereSeparator: \.isWhitespace)
        return tokens.allSatisfy { normalizedText.contains($0) }
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

    private func commandPaletteSaveLogsItem(alias: String) -> CommandPaletteItem {
        CommandPaletteItem(
            id: "cmd:save-logs:\(alias)",
            title: "Save Logs",
            subtitle: "Save current pod logs or unified logs • `\(alias)`",
            symbolName: "square.and.arrow.down",
            action: .saveLogs
        )
    }

    private func commandPaletteCommandItems(query: String) -> [CommandPaletteItem]? {
        let normalized = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard normalized.hasPrefix(":") else { return nil }

        let tokens = normalized.dropFirst().split(whereSeparator: \.isWhitespace).map(String.init)
        guard let command = tokens.first?.lowercased() else {
            return commandPaletteCheatSheet()
        }

        let remainder = tokens.dropFirst().joined(separator: " ")

        switch command {
        case "sl", "save-log", "save-logs", "savelog", "savelogs":
            return [commandPaletteSaveLogsItem(alias: ":\(command)")]
        case "po", "pod", "pods":
            if ["sl", "log", "logs", "save", "save logs", "save-logs"].contains(remainder.lowercased()) {
                return [commandPaletteSaveLogsItem(alias: ":\(command) \(remainder)")]
            }
            let rows = Array(
                visiblePods
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleDeployments
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleServices
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
        case "ctx", "context", "contexts":
            let rows = visibleContexts
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .map { context in
                    CommandPaletteItem(
                        id: "cmd:context:\(context.id)",
                        title: context.name,
                        subtitle: "Contexts • `:ctx`",
                        symbolName: state.isFavorite(context) ? "star.fill" : "network",
                        action: .context(context)
                    )
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
                .filter { remainder.isEmpty || matches($0, query: remainder) }
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
                visibleEvents
                    .filter { remainder.isEmpty || matches("\($0.reason) \($0.objectName) \($0.message)", query: remainder) }
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
                visibleHelmReleases
                    .filter { remainder.isEmpty || matches("\($0.name) \($0.namespace) \($0.chart)", query: remainder) }
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
                visibleStatefulSets
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleDaemonSets
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleReplicaSets
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleIngresses
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visiblePersistentVolumeClaims
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visiblePersistentVolumes
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleStorageClasses
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleHorizontalPodAutoscalers
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleNetworkPolicies
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleConfigMaps
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleSecrets
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
        case "role", "roles":
            let rows = Array(
                state.rbacRoles
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleNodes
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleCronJobs
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
                visibleJobs
                    .filter { remainder.isEmpty || matches($0.name, query: remainder) }
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
        case "sa", "serviceaccount", "serviceaccounts":
            return [
                CommandPaletteItem(
                    id: "stub:sa",
                    title: "ServiceAccounts",
                    subtitle: "Not in Rune yet — opened RBAC",
                    symbolName: "person.crop.circle",
                    action: .resourceKind(section: .rbac, kind: .role)
                )
            ]
        case "ep", "endpoint", "endpoints":
            return [
                CommandPaletteItem(
                    id: "stub:ep",
                    title: "Endpoints",
                    subtitle: "Not in Rune yet — opened Networking (Services)",
                    symbolName: "link",
                    action: .resourceKind(section: .networking, kind: .service)
                )
            ]
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
            CommandPaletteItem(id: "help:ing", title: ":ing <name>", subtitle: "Ingresses", symbolName: "network", action: .resourceKind(section: .networking, kind: .ingress)),
            CommandPaletteItem(id: "help:cm", title: ":cm <name>", subtitle: "ConfigMaps", symbolName: "doc.text", action: .resourceKind(section: .config, kind: .configMap)),
            CommandPaletteItem(id: "help:sec", title: ":sec <name>", subtitle: "Secrets", symbolName: "key", action: .resourceKind(section: .config, kind: .secret)),
            CommandPaletteItem(id: "help:sl", title: ":sl / :po logs", subtitle: "Save current pod or unified logs", symbolName: "square.and.arrow.down", action: .saveLogs),
            CommandPaletteItem(id: "help:no", title: ":no <name>", subtitle: "Nodes (Storage)", symbolName: "server.rack", action: .resourceKind(section: .storage, kind: .node)),
            CommandPaletteItem(id: "help:ns", title: ":ns <namespace>", subtitle: "Switch namespace", symbolName: "square.3.layers.3d", action: .section(.overview)),
            CommandPaletteItem(id: "help:ov", title: ":ov / :overview", subtitle: "Open Overview", symbolName: RuneSection.overview.symbolName, action: .section(.overview)),
            CommandPaletteItem(id: "help:ctx", title: ":ctx <context>", subtitle: "Switch context", symbolName: "network", action: .section(.overview)),
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
            "ingresses": state.ingresses.count,
            "configmaps": state.configMaps.count,
            "secrets": state.secrets.count,
            "nodes": state.nodes.count,
            "events": state.events.count,
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
        default:
            return nil
        }
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
