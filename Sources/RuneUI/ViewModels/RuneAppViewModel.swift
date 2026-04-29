import AppKit
import Combine
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

public enum PendingWriteAction: Sendable {
    case delete(kind: KubeResourceKind, name: String)
    case apply(kind: KubeResourceKind, name: String, yaml: String)
    case scale(deploymentName: String, replicas: Int)
    case rolloutRestart(deploymentName: String)
    case rolloutUndo(deploymentName: String, revision: Int?)
    case exec(podName: String, command: [String])

    var title: String {
        switch self {
        case let .delete(kind, _):
            return "Do you want to delete this \(kind.singularTypeName)?"
        case let .apply(kind, name, _):
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
        case let .exec(podName, command):
            return "Run command in pod \(podName)? (\(command.joined(separator: " ")))"
        }
    }

    var message: String {
        switch self {
        case let .delete(_, name):
            return "“\(name)” will be removed from the cluster. This cannot be undone."
        case .apply:
            return "This applies the current YAML to the active namespace/context."
        case .scale:
            return "Replica count will be changed immediately."
        case .rolloutRestart:
            return "Pods in the deployment will be recreated according to rollout strategy."
        case .rolloutUndo:
            return "Deployment rollout will be reverted to an earlier revision."
        case .exec:
            return "This runs a command inside the selected pod."
        }
    }

    var confirmLabel: String {
        switch self {
        case .delete: return "Delete"
        case .apply: return "Apply"
        case .scale: return "Scale"
        case .rolloutRestart: return "Restart"
        case .rolloutUndo: return "Rollback"
        case .exec: return "Run"
        }
    }

    var isDestructive: Bool {
        switch self {
        case .delete: return true
        case .apply, .scale, .rolloutRestart, .rolloutUndo, .exec: return false
        }
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
        case .section, .context, .namespace, .importKubeConfig, .reload, .readOnly:
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
        case .helm, .terminal:
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
    @Published public var isLogTailModeEnabled: Bool = false {
        didSet {
            if isLogTailModeEnabled {
                reloadLogsForSelection()
            } else {
                tailLogsReloadTask?.cancel()
                tailLogsReloadTask = nil
            }
        }
    }
    @Published public private(set) var podSortColumn: PodListSortColumn = .name
    @Published public private(set) var podSortAscending: Bool = true
    @Published public var pendingWriteAction: PendingWriteAction?
    @Published public var scaleReplicaInput: Int = 1
    @Published public var execCommandInput: String = "printenv"
    @Published public var terminalSessionInput: String = ""
    @Published public var portForwardLocalPortInput: String = "8080"
    @Published public var portForwardRemotePortInput: String = "8080"
    @Published public var portForwardAddressInput: String = "127.0.0.1"
    @Published public var rolloutRevisionInput: String = ""
    @Published public var isSidebarVisible: Bool = true
    @Published public var isDetailPaneVisible: Bool = true
    @Published public private(set) var canNavigateBack = false
    @Published public private(set) var canNavigateForward = false

    private let kubeClient: KubernetesClient
    private let bookmarkManager: BookmarkManager
    private let picker: KubeConfigPicking
    private let kubeConfigDiscoverer: KubeConfigDiscovering
    private let store: ResourceStore
    private let exporter: FileExporting
    private let supportBundleBuilder: any SupportBundleBuilding
    private let contextPreferences: ContextPreferencesStoring
    private let overviewSnapshotPersistence: any OverviewSnapshotCacheStoring
    private let namespaceListPersistence: NamespaceListPersisting
    private let portForwardBrowserOpener: PortForwardBrowserOpening
    private let diagnostics: DiagnosticsRecorder
    private let terminalShellCommand = ["sh"]

    private var cancellables: Set<AnyCancellable> = []
    private var hasBootstrapped = false
    private var latestSnapshotRequestID = UUID()
    private var latestResourceDetailsRequestID = UUID()
    private var latestLogsReloadRequestID = UUID()
    private var latestYAMLValidationRequestID = UUID()
    private var navigationHistory: [NavigationCheckpoint] = []
    private var navigationIndex: Int = -1
    private var isApplyingNavigationCheckpoint = false
    private var pendingOpenEventSource: EventSummary?
    /// Retries for `navigateToEventSource` when lists were not loaded for the Events-only snapshot (e.g. pods empty until workloads refresh).
    private var navigateFromEventFetchAttempts = 0
    private var scheduledRefreshTask: Task<Void, Never>?
    private var resourceDetailsTask: Task<Void, Never>?
    private var scheduledLogsReloadTask: Task<Void, Never>?
    private var logsReloadTask: Task<Void, Never>?
    private var tailLogsReloadTask: Task<Void, Never>?
    private var yamlValidationTask: Task<Void, Never>?
    private var terminalOutputFlushTask: Task<Void, Never>?
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

    private let refreshDebounceNanoseconds: UInt64 = 120_000_000
    /// Coalesces rapid log preset toggles while still cancelling any in-flight fetch immediately.
    private let logsReloadDebounceNanoseconds: UInt64 = 180_000_000
    private let terminalOutputFlushNanoseconds: UInt64 = 33_000_000
    private let tailLogsReloadNanoseconds: UInt64 = 3_000_000_000
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
        self.overviewSnapshotPersistence = overviewSnapshotPersistence
        self.namespaceListPersistence = namespaceListPersistence
        self.portForwardBrowserOpener = portForwardBrowserOpener
        self.diagnostics = diagnostics

        self.state.setFavoriteContextNames(contextPreferences.loadFavoriteContextNames())

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
        return filtered(list) { summaryText(for: $0) }
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
        // Only expose namespaces that belong to the current context.
        // Source from the active state list (cleared on context switch) so we never leak a stale
        // namespace menu from cache before the current context has loaded.
        // If no verified list is loaded yet, only expose the current in-memory selection.
        let options = state.namespaces
        if !options.isEmpty {
            return sortedNamespaceOptions(options)
        }

        let selected = state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        if !selected.isEmpty {
            return [selected]
        }

        return []
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

    public var visibleContexts: [KubeContext] {
        let filtered = state.contexts.filter { context in
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
        state.contexts.sorted { lhs, rhs in
            lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
        }
    }

    public var visiblePods: [PodSummary] {
        let values = filtered(state.pods) { pod in
            "\(pod.name) \(pod.status) \(pod.namespace) \(pod.ageDescription) \(pod.cpuDisplay) \(pod.memoryDisplay) \(pod.totalRestarts)"
        }
        return values.sorted(by: podComparator)
    }

    public var visibleDeployments: [DeploymentSummary] {
        filtered(state.deployments) { deployment in
            "\(deployment.name) \(deployment.namespace) \(deployment.replicaText)"
        }
    }

    public var visibleServices: [ServiceSummary] {
        filtered(state.services) { service in
            "\(service.name) \(service.namespace) \(service.type) \(service.clusterIP)"
        }
    }

    public var visibleStatefulSets: [ClusterResourceSummary] {
        filtered(state.statefulSets) { summaryText(for: $0) }
    }

    public var visibleDaemonSets: [ClusterResourceSummary] {
        filtered(state.daemonSets) { summaryText(for: $0) }
    }

    public var visibleJobs: [ClusterResourceSummary] {
        filtered(state.jobs) { summaryText(for: $0) }
    }

    public var visibleCronJobs: [ClusterResourceSummary] {
        filtered(state.cronJobs) { summaryText(for: $0) }
    }

    public var visibleReplicaSets: [ClusterResourceSummary] {
        filtered(state.replicaSets) { summaryText(for: $0) }
    }

    public var visiblePersistentVolumeClaims: [ClusterResourceSummary] {
        filtered(state.persistentVolumeClaims) { summaryText(for: $0) }
    }

    public var visiblePersistentVolumes: [ClusterResourceSummary] {
        filtered(state.persistentVolumes) { summaryText(for: $0) }
    }

    public var visibleStorageClasses: [ClusterResourceSummary] {
        filtered(state.storageClasses) { summaryText(for: $0) }
    }

    public var visibleHorizontalPodAutoscalers: [ClusterResourceSummary] {
        filtered(state.horizontalPodAutoscalers) { summaryText(for: $0) }
    }

    public var visibleNetworkPolicies: [ClusterResourceSummary] {
        filtered(state.networkPolicies) { summaryText(for: $0) }
    }

    public var visibleIngresses: [ClusterResourceSummary] {
        filtered(state.ingresses) { summaryText(for: $0) }
    }

    public var visibleConfigMaps: [ClusterResourceSummary] {
        filtered(state.configMaps) { summaryText(for: $0) }
    }

    public var visibleSecrets: [ClusterResourceSummary] {
        filtered(state.secrets) { summaryText(for: $0) }
    }

    public var visibleNodes: [ClusterResourceSummary] {
        filtered(state.nodes) { summaryText(for: $0) }
    }

    public var visibleEvents: [EventSummary] {
        filtered(state.events) { event in
            "\(event.type) \(event.reason) \(event.objectName) \(event.message)"
        }
    }

    public var visibleHelmReleases: [HelmReleaseSummary] {
        filtered(state.helmReleases) { release in
            "\(release.name) \(release.namespace) \(release.status) \(release.chart) \(release.appVersion)"
        }
    }

    public var isProductionContext: Bool {
        guard let context = state.selectedContext?.name.lowercased() else {
            return false
        }

        let markers = ["prod", "production", "live", "critical"]
        return markers.contains { context.contains($0) }
    }

    public var pendingWriteActionTitle: String {
        guard let pendingWriteAction else { return "Confirm write action" }
        return pendingWriteAction.title
    }

    public var pendingWriteActionMessage: String {
        guard let pendingWriteAction else { return "" }
        if state.isReadOnlyMode {
            return "READ-ONLY MODE: turn off read-only before running write actions."
        }
        if isProductionContext {
            return "PRODUCTION CONTEXT: \(pendingWriteAction.message)"
        }
        return pendingWriteAction.message
    }

    public var pendingWriteActionConfirmLabel: String {
        pendingWriteAction?.confirmLabel ?? "Confirm"
    }

    public var pendingWriteActionIsDestructive: Bool {
        pendingWriteAction?.isDestructive ?? false
    }

    public func bootstrapIfNeeded() {
        guard !hasBootstrapped else { return }
        hasBootstrapped = true
        bootstrap()
    }

    public func bootstrap() {
        Task {
            do {
                let discoveredURLs = kubeConfigDiscoverer.discoverCandidateFiles()
                diagnostics.log("bootstrap discovered kubeconfig files: \(discoveredURLs.map(\.path).joined(separator: ", "))")
                for url in discoveredURLs {
                    try? bookmarkManager.addKubeConfig(url: url)
                }

                let sources = try resolvedKubeConfigSources(
                    fallbackURLs: discoveredURLs
                )

                state.setSources(sources)
                diagnostics.log("bootstrap resolved sources count=\(sources.count)")

                guard !sources.isEmpty else {
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
                    state.clearResourceDetails()
                    state.clearError()
                    return
                }

                try await reloadContexts()
            } catch {
                diagnostics.log("bootstrap failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func importKubeConfig() {
        Task {
            do {
                let files = try picker.pickFiles()
                guard !files.isEmpty else { return }

                for file in files {
                    try? bookmarkManager.addKubeConfig(url: file)
                }

                let sources = try resolvedKubeConfigSources(fallbackURLs: files)
                state.setSources(sources)
                diagnostics.log("importKubeConfig loaded files count=\(files.count), sources count=\(sources.count)")
                try await reloadContexts()
            } catch {
                diagnostics.log("importKubeConfig failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func addDefaultKubeConfig() {
        Task {
            do {
                let url = URL(fileURLWithPath: "\(NSHomeDirectory())/.kube/config")
                guard FileManager.default.fileExists(atPath: url.path) else {
                    throw RuneError.invalidInput(message: "Default kubeconfig was not found at \(url.path)")
                }

                try? bookmarkManager.addKubeConfig(url: url)
                let sources = try resolvedKubeConfigSources(fallbackURLs: [url])
                state.setSources(sources)
                diagnostics.log("addDefaultKubeConfig loaded \(url.path), sources count=\(sources.count)")
                try await reloadContexts()
            } catch {
                diagnostics.log("addDefaultKubeConfig failed: \(error.localizedDescription)")
                state.setError(error)
            }
        }
    }

    public func reloadContexts() async throws {
        state.isLoading = true
        defer { state.isLoading = false }

        diagnostics.log("reloadContexts start")
        let previousContextName = state.selectedContext?.name
        let previousNamespace = state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        let contexts = try await kubeClient.listContexts(from: state.kubeConfigSources)
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
            try await loadResourceSnapshot(
                context: selected,
                namespace: requestedNamespace,
                requestID: requestID,
                forceNamespaceMetadataRefresh: true
            )
            if navigationHistory.isEmpty {
                recordNavigationCheckpoint()
            }
        } else {
            state.setNamespaces([])
        }
    }

    /// - Parameter debounced: When `false`, reload runs immediately (⌘R, panel refresh). When `true`, waits briefly to coalesce rapid triggers.
    public func refreshCurrentView(debounced: Bool = true) {
        scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: false, debounced: debounced)
    }

    /// Refetches only the right-hand inspector (YAML, describe, logs, etc.) for the current selection — **not** the center list or overview tiles.
    /// List data comes from ``refreshCurrentView`` / ``loadResourceSnapshot`` (driven by ``SnapshotLoadPlan`` per section, e.g. workloads → pods loads only pod list).
    public func refreshResourceInspectorOnly() {
        guard state.selectedContext != nil else { return }
        loadResourceDetailsForCurrentSelection()
    }

    private func scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: Bool, debounced: Bool) {
        guard let context = state.selectedContext else { return }
        applyCachedSnapshot(context: context, namespace: state.selectedNamespace)
        pendingForcedNamespaceRefresh = pendingForcedNamespaceRefresh || forceNamespaceMetadataRefresh

        scheduledRefreshTask?.cancel()
        let delay = debounced ? refreshDebounceNanoseconds : 0

        scheduledRefreshTask = Task { [weak self] in
            if delay > 0 {
                try? await Task.sleep(nanoseconds: delay)
            }
            guard let self else { return }
            let forceNamespaceMetadataRefresh = self.pendingForcedNamespaceRefresh
            self.pendingForcedNamespaceRefresh = false
            await self.performRefreshCurrentView(forceNamespaceMetadataRefresh: forceNamespaceMetadataRefresh)
        }
    }

    private func performRefreshCurrentView(forceNamespaceMetadataRefresh: Bool) async {
        guard let context = state.selectedContext else { return }
        let namespace = state.selectedNamespace

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
            }
            diagnostics.trace("refresh", "performRefreshCurrentView done context=\(context.name)")
        } catch {
            if error is CancellationError {
                markOverviewCooldownBypass(contextName: context.name, namespace: namespace)
                diagnostics.trace("refresh", "performRefreshCurrentView cancelled")
                return
            }
            diagnostics.trace("refresh", "performRefreshCurrentView failed: \(error.localizedDescription)")
            diagnostics.log("refreshCurrentView failed: \(error.localizedDescription)")
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
        }
        loadResourceDetailsForCurrentSelection()
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

    public func setContextSearchQuery(_ query: String) {
        state.contextSearchQuery = query
    }

    public func setResourceSearchQuery(_ query: String) {
        state.resourceSearchQuery = query
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

    public func setReadOnlyMode(_ value: Bool) {
        state.isReadOnlyMode = value
    }

    public func toggleSidebarVisibility() {
        isSidebarVisible.toggle()
    }

    public func toggleDetailPaneVisibility() {
        isDetailPaneVisible.toggle()
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
        let requestedPreferredNamespace = preferredNamespace?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
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

    public func selectPod(_ pod: PodSummary?) {
        selectPod(pod, trackHistory: true)
    }

    private func selectPod(_ pod: PodSummary?, trackHistory: Bool) {
        prepareNavigationMutation(trackHistory: trackHistory)
        state.setSelectedPod(pod)
        state.selectedWorkloadKind = .pod
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        state.setSelectedHelmRelease(release)
        loadHelmDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
              let context = state.selectedContext,
              let cronJob = state.selectedCronJob else { return }
        let jobName = "\(cronJob.name)-manual-\(Int(Date().timeIntervalSince1970))"
        Task {
            do {
                try await kubeClient.createJobFromCronJob(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    cronJobName: cronJob.name,
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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
        loadResourceDetailsForCurrentSelection()
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

        let sources = state.kubeConfigSources
        let namespace = state.selectedNamespace
        let kind = state.selectedWorkloadKind
        let filter = selectedLogPreset.filter
        let previous = includePreviousLogs
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
                        filter: filter,
                        previous: previous
                    )
                    guard self.isCurrentLogsReloadRequest(requestID) else { return }
                    self.state.appendPodLogRead(
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
                    self.state.appendUnifiedServiceLogRead(
                        unified.mergedText,
                        pods: unified.podNames,
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
                    self.state.appendUnifiedServiceLogRead(
                        unified.mergedText,
                        pods: unified.podNames,
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
            state.setError(error)
        }
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

    /// Discards edits in the YAML editor and restores the last manifest loaded from the cluster.
    public func revertResourceYAMLDraft() {
        yamlValidationTask?.cancel()
        state.revertResourceYAMLToClusterSnapshot()
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
            state.setError(error)
        }
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
        pendingWriteAction = .apply(kind: kind, name: name, yaml: state.resourceYAML)
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
            pendingWriteAction = .rolloutUndo(deploymentName: deployment.name, revision: revision)
        } catch {
            state.setError(error)
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

    public func startTerminalSession(for pod: PodSummary) {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let context = state.selectedContext else { return }
        if state.terminalSession != nil {
            stopTerminalSession(resetState: true)
        }

        let namespace = state.selectedNamespace
        let sessionID = UUID().uuidString
        state.setTerminalSession(
            PodTerminalSession(
                id: sessionID,
                contextName: context.name,
                namespace: namespace,
                podName: pod.name,
                shell: terminalShellCommand.joined(separator: " "),
                transcript: "",
                status: .connecting
            )
        )
        terminalSessionInput = ""
        state.selectedSection = .terminal

        Task {
            do {
                try await kubeClient.startPodTerminalSession(
                    id: sessionID,
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: namespace,
                    podName: pod.name,
                    container: nil,
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
                state.updateTerminalSessionStatus(id: sessionID, status: .failed)
                state.appendTerminalSessionOutput(
                    id: sessionID,
                    text: "[rune] Failed to start terminal session: \(error.localizedDescription)\n"
                )
                state.setError(error)
            }
        }
    }

    public func sendTerminalSessionInput() {
        guard let session = state.terminalSession else { return }
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
            state.updateTerminalSessionStatus(id: sessionID, status: .disconnected, exitCode: session.lastExitCode)
        }
        Task {
            await kubeClient.stopPodTerminalSession(id: sessionID)
        }
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
        Task {
            do {
                guard let context = state.selectedContext else { return }
                let localPort = try parsePort(portForwardLocalPortInput, fieldName: "local port")
                let remotePort = try parsePort(portForwardRemotePortInput, fieldName: "remote port")
                let address = normalizedPortForwardAddress()

                state.isStartingPortForward = true
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
                state.setError(error)
            }
        }
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

    public func openPortForwardInBrowser(_ session: PortForwardSession) {
        guard let url = session.browserURL else {
            state.setError(RuneError.invalidInput(message: "Port-forward is not connected yet."))
            return
        }

        portForwardBrowserOpener.open(url)
    }

    public func cancelPendingWriteAction() {
        pendingWriteAction = nil
    }

    public func confirmPendingWriteAction() {
        guard writeActionsEnabled else {
            pendingWriteAction = nil
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let action = pendingWriteAction else { return }
        pendingWriteAction = nil

        Task {
            do {
                guard let context = state.selectedContext else { return }

                let shouldReloadResourceInspectorAfterWrite: Bool

                switch action {
                case let .delete(kind, name):
                    shouldReloadResourceInspectorAfterWrite = false
                    try await kubeClient.deleteResource(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        kind: kind,
                        name: name
                    )
                case let .apply(_, _, yaml):
                    shouldReloadResourceInspectorAfterWrite = true
                    try await kubeClient.applyYAML(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        yaml: yaml
                    )
                case let .scale(deploymentName, replicas):
                    shouldReloadResourceInspectorAfterWrite = false
                    try await kubeClient.scaleDeployment(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deploymentName: deploymentName,
                        replicas: replicas
                    )
                case let .rolloutRestart(deploymentName):
                    shouldReloadResourceInspectorAfterWrite = false
                    try await kubeClient.restartDeploymentRollout(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deploymentName: deploymentName
                    )
                case let .rolloutUndo(deploymentName, revision):
                    shouldReloadResourceInspectorAfterWrite = false
                    try await kubeClient.rollbackDeploymentRollout(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deploymentName: deploymentName,
                        revision: revision
                    )
                case let .exec(podName, command):
                    shouldReloadResourceInspectorAfterWrite = false
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
                    return
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
                if shouldReloadResourceInspectorAfterWrite {
                    loadResourceDetailsForCurrentSelection()
                }
            } catch {
                state.setError(error)
            }
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
                if UserDefaults.standard.runePersistNamespaceListCache {
                    namespaceListPersistence.save(names: loadedNamespaces, contextName: context.name)
                }
                namespaceMetadataRefreshedAt[context.name] = now
            case let .failure(error):
                diagnostics.log("snapshot namespaces failed: \(error.localizedDescription)")
                warnings.append("namespaces: \(error.localizedDescription)")
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
        let preferredForResolution: String
        if loadedNamespaces.isEmpty {
            // No verified namespace list yet: never trust stale per-context saved namespace blindly.
            // Keep only the current incoming selection (explicit UI/checkpoint), otherwise fall back
            // to `contextDefaultNamespace` / "default" inside `resolvedNamespace`.
            preferredForResolution = trimmedIncoming
        } else if shouldRevalidateNamespace {
            // Fresh context switch without an explicit namespace: ignore carried/saved namespace for this one pass
            // and let context default / namespace suffix heuristics choose from the live namespace list.
            preferredForResolution = ""
        } else if !trimmedIncoming.isEmpty {
            preferredForResolution = trimmedIncoming
        } else if !trimmedContextDefault.isEmpty {
            // On context switch with an empty incoming namespace, prefer kubeconfig's context default
            // over any older saved namespace for this context.
            preferredForResolution = ""
        } else if !savedForContext.isEmpty {
            preferredForResolution = savedForContext
        } else {
            preferredForResolution = ""
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
            preferred: preferredForResolution,
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
        contextPreferences.savePreferredNamespace(effectiveNamespace, for: context.name)
        rememberRecentNamespace(effectiveNamespace, for: context.name)

        // Namespace list is cheap compared to workload snapshots; publish it immediately so the toolbar menu is usable while pods/counts load.
        store.cacheNamespaces(loadedNamespaces, context: context)
        state.setNamespaces(loadedNamespaces)

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
            let standardizedPath = URL(fileURLWithPath: source.path).standardizedFileURL.path
            merged[standardizedPath] = KubeConfigSource(url: URL(fileURLWithPath: standardizedPath))
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

    private func scheduleNextTailLogsReload() {
        guard isLogTailModeEnabled else { return }
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
                    state.appendPodLogRead(
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
                    state.appendUnifiedServiceLogRead(
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
                    state.appendUnifiedServiceLogRead(
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

    private func loadHelmDetailsForCurrentSelection() {
        Task {
            await loadHelmDetailsForCurrentSelectionAsync()
        }
    }

    private func loadHelmDetailsForCurrentSelectionAsync() async {
        do {
            guard let context = state.selectedContext, let release = state.selectedHelmRelease else {
                state.setHelmValues("")
                state.setHelmManifest("")
                state.setHelmHistory([])
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

            state.setHelmValues(try await values)
            state.setHelmManifest(try await manifest)
            state.setHelmHistory(try await history)
        } catch {
            state.setError(error)
        }
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
        guard trackHistory, !isApplyingNavigationCheckpoint, navigationHistory.isEmpty else { return }
        navigationHistory.append(currentNavigationCheckpoint())
        navigationIndex = 0
        updateNavigationAvailability()
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

    private func resolvedNamespace(
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
            return "default"
        }

        if !trimmedPreferred.isEmpty,
           let match = availableNamespaces.first(where: { $0.caseInsensitiveCompare(trimmedPreferred) == .orderedSame }) {
            return match
        }

        if preferContextSuffixOverContextDefault,
           let suffixMatch = namespaceLongestSuffixOfContext(contextName, availableNamespaces: availableNamespaces) {
            return suffixMatch
        }

        if !trimmedContextDefault.isEmpty,
           let match = availableNamespaces.first(where: { $0.caseInsensitiveCompare(trimmedContextDefault) == .orderedSame }) {
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

    /// Namespaces whose names are a case-insensitive suffix of `contextName` (e.g. `aks-example-service` → `example-service`).
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
            let mergedNodesCount = cachedNodes.isEmpty ? 0 : cachedNodes.count
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
            diagnostics.log("snapshot \(label) failed: \(error.localizedDescription)")
            warnings.append("\(label): \(error.localizedDescription)")
            return fallback
        }
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

    private func podComparator(_ lhs: PodSummary, _ rhs: PodSummary) -> Bool {
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
                lhs: lhs,
                rhs: rhs,
                ascending: ascending,
                lhsValue: lhs.totalRestarts,
                rhsValue: rhs.totalRestarts,
                tieBreak: nameOrder
            )
        case .cpu:
            return comparePodsOptionalMetric(
                lhs: lhs,
                rhs: rhs,
                ascending: ascending,
                lhsValue: cpuMilliValue(lhs.cpuUsage),
                rhsValue: cpuMilliValue(rhs.cpuUsage),
                tieBreak: nameOrder
            )
        case .memory:
            return comparePodsOptionalMetric(
                lhs: lhs,
                rhs: rhs,
                ascending: ascending,
                lhsValue: memoryByteValue(lhs.memoryUsage),
                rhsValue: memoryByteValue(rhs.memoryUsage),
                tieBreak: nameOrder
            )
        case .age:
            return comparePodsOptionalMetric(
                lhs: lhs,
                rhs: rhs,
                ascending: ascending,
                lhsValue: ageSeconds(lhs.ageDescription),
                rhsValue: ageSeconds(rhs.ageDescription),
                tieBreak: nameOrder
            )
        }
    }

    /// Missing metrics sort last regardless of ascending/descending direction.
    private func comparePodsOptionalMetric<T: Comparable>(
        lhs: PodSummary,
        rhs: PodSummary,
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
        lhs: PodSummary,
        rhs: PodSummary,
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

    private func commandPaletteCommandItems(query: String) -> [CommandPaletteItem]? {
        let normalized = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard normalized.hasPrefix(":") else { return nil }

        let tokens = normalized.dropFirst().split(whereSeparator: \.isWhitespace).map(String.init)
        guard let command = tokens.first?.lowercased() else {
            return commandPaletteCheatSheet()
        }

        let remainder = tokens.dropFirst().joined(separator: " ")

        switch command {
        case "po", "pod", "pods":
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
                        symbolName: "square.3.layers.3d",
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

    private func normalizedPortForwardAddress() -> String {
        let trimmed = portForwardAddressInput.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "127.0.0.1" : trimmed
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
