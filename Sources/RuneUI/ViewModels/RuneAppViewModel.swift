import Combine
import Foundation
import RuneCore
import RuneDiagnostics
import RuneExport
import RuneHelm
import RuneKube
import RuneSecurity
import RuneStore

// Cluster data caching: `ResourceStore` holds full lists per (context, namespace) in RAM; `overviewSnapshotCache`
// holds lightweight overview rows with TTL; `overviewSnapshotPersistence` writes the same shape to disk
// (Application Support) for cold start and background prefetch. Keys always pair context name with namespace.

public enum PodLogPreset: String, CaseIterable, Identifiable, Sendable {
    /// Default: `--tail` only (no `--since`); bounded transfer, similar to unrestricted `kubectl logs` on a quiet pod.
    case recentLines
    case last5Minutes
    case last15Minutes
    case lastHour
    case last6Hours
    case last24Hours
    case last7Days
    /// High line cap; still bounded to limit payload size and timeouts.
    case largeTail

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
        case .largeTail: return "Large tail (10k)"
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
        case .largeTail: return .tailLines(10_000)
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
    case helmRollback(releaseName: String, namespace: String, revision: Int)

    var title: String {
        switch self {
        case let .delete(kind, name):
            return "Delete \(kind.kubectlName) \(name)?"
        case let .apply(kind, name, _):
            return "Apply YAML for \(kind.kubectlName) \(name)?"
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
        case let .helmRollback(releaseName, namespace, revision):
            return "Rollback Helm release \(releaseName) in \(namespace) to revision \(revision)?"
        }
    }

    var message: String {
        switch self {
        case .delete:
            return "This operation mutates cluster state and may be irreversible."
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
        case .helmRollback:
            return "The selected Helm release will be rolled back immediately."
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
        case .helmRollback: return "Rollback"
        }
    }

    var isDestructive: Bool {
        switch self {
        case .delete: return true
        case .apply, .scale, .rolloutRestart, .rolloutUndo, .exec, .helmRollback: return false
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

public enum OverviewModule: Sendable {
    case pods
    case deployments
    case services
    case ingresses
    case configMaps
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
    let selectedIngressName: String?
    let selectedConfigMapName: String?
    let selectedSecretName: String?
    let selectedNodeName: String?
    let selectedHelmReleaseID: String?
    let selectedRBACResourceID: String?
}

/// kubectl fetch subset for the current `RuneSection` and `KubeResourceKind`. Drives parallel snapshot tasks in `loadResourceSnapshot`.
private struct SnapshotLoadPlan: Sendable {
    var podStatuses = false
    var pods = false
    var deployments = false
    var deploymentCount = false
    var statefulSets = false
    var daemonSets = false
    var services = false
    var servicesCount = false
    var ingresses = false
    var ingressesCount = false
    var configMaps = false
    var configMapsCount = false
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
            default:
                plan.pods = true
            }
        case .networking:
            switch kind {
            case .ingress:
                plan.ingresses = true
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
            plan.nodes = true
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
    @Published public private(set) var podSortColumn: PodListSortColumn = .name
    @Published public private(set) var podSortAscending: Bool = true
    @Published public var pendingWriteAction: PendingWriteAction?
    @Published public var scaleReplicaInput: Int = 1
    @Published public var execCommandInput: String = "printenv"
    @Published public var portForwardLocalPortInput: String = "8080"
    @Published public var portForwardRemotePortInput: String = "8080"
    @Published public var portForwardAddressInput: String = "127.0.0.1"
    @Published public var rolloutRevisionInput: String = ""
    @Published public var helmRollbackRevisionInput: String = ""
    @Published public private(set) var canNavigateBack = false
    @Published public private(set) var canNavigateForward = false

    private let kubeClient: KubectlClient
    private let helmClient: any HelmReleaseService
    private let bookmarkManager: BookmarkManager
    private let picker: KubeConfigPicking
    private let kubeConfigDiscoverer: KubeConfigDiscovering
    private let store: ResourceStore
    private let exporter: FileExporting
    private let supportBundleBuilder: any SupportBundleBuilding
    private let contextPreferences: ContextPreferencesStoring
    private let overviewSnapshotPersistence: any OverviewSnapshotCacheStoring
    private let diagnostics: DiagnosticsRecorder

    private var cancellables: Set<AnyCancellable> = []
    private var hasBootstrapped = false
    private var latestSnapshotRequestID = UUID()
    private var navigationHistory: [NavigationCheckpoint] = []
    private var navigationIndex: Int = -1
    private var isApplyingNavigationCheckpoint = false
    private var pendingOpenEventSource: EventSummary?
    private var scheduledRefreshTask: Task<Void, Never>?
    private var pendingForcedNamespaceRefresh = false
    private var namespaceMetadataRefreshedAt: [String: Date] = [:]
    /// In-memory overview rows keyed by `overviewCacheKey(contextName:namespace:)`; TTL `overviewSnapshotFreshnessTTL`. Mirrors disk where possible; merged with `ResourceStore` on apply.
    private var overviewSnapshotCache: [String: OverviewSnapshotCacheEntry] = [:]
    /// Background task: `listPodStatuses` + count queries for sibling namespaces; cancelled on context change.
    private var overviewPrefetchTask: Task<Void, Never>?
    private var recentNamespacesByContext: [String: [String]] = [:]

    private let refreshDebounceNanoseconds: UInt64 = 120_000_000
    private let namespaceMetadataTTL: TimeInterval = 30
    /// Maximum age of `overviewSnapshotCache` entries before refresh is preferred over warm paths.
    private let overviewSnapshotFreshnessTTL: TimeInterval = 60
    /// Maximum age for treating `overviewSnapshotPersistence` loads as warm data when hydrating memory.
    private let overviewDiskSnapshotFreshnessTTL: TimeInterval = 60 * 5
    private let overviewSnapshotRetentionTTL: TimeInterval = 60 * 20
    private let maxOverviewSnapshotEntries = 180
    private let maxRecentNamespacesPerContext = 4
    /// Cap on non-active namespaces to prefetch per snapshot (pod status + resource counts).
    private let maxOverviewPrefetchNamespaces = 8
    private let overviewPrefetchThrottleNanoseconds: UInt64 = 120_000_000

    public init(
        state: RuneAppState = RuneAppState(),
        kubeClient: KubectlClient = KubectlClient(),
        helmClient: any HelmReleaseService = HelmClient(),
        bookmarkManager: BookmarkManager = BookmarkManager(store: UserDefaultsBookmarkStore()),
        picker: KubeConfigPicking = OpenPanelKubeConfigPicker(),
        kubeConfigDiscoverer: KubeConfigDiscovering = KubeConfigDiscoverer(),
        store: ResourceStore = ResourceStore(),
        exporter: FileExporting = SavePanelExporter(),
        supportBundleBuilder: any SupportBundleBuilding = JSONSupportBundleBuilder(),
        contextPreferences: ContextPreferencesStoring = UserDefaultsContextPreferencesStore(),
        overviewSnapshotPersistence: any OverviewSnapshotCacheStoring = JSONOverviewSnapshotCacheStore(),
        diagnostics: DiagnosticsRecorder = DiagnosticsRecorder()
    ) {
        self.state = state
        self.kubeClient = kubeClient
        self.helmClient = helmClient
        self.bookmarkManager = bookmarkManager
        self.picker = picker
        self.kubeConfigDiscoverer = kubeConfigDiscoverer
        self.store = store
        self.exporter = exporter
        self.supportBundleBuilder = supportBundleBuilder
        self.contextPreferences = contextPreferences
        self.overviewSnapshotPersistence = overviewSnapshotPersistence
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
                self?.reloadLogsForSelection()
            }
            .store(in: &cancellables)

        $includePreviousLogs
            .dropFirst()
            .sink { [weak self] _ in
                self?.reloadLogsForSelection()
            }
            .store(in: &cancellables)
    }

    public var workloadKinds: [KubeResourceKind] {
        [.pod, .deployment, .statefulSet, .daemonSet]
    }

    public var networkingKinds: [KubeResourceKind] {
        [.service, .ingress]
    }

    public var configKinds: [KubeResourceKind] {
        [.configMap, .secret]
    }

    public var storageKinds: [KubeResourceKind] {
        [.node]
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

    public var namespaceOptions: [String] {
        var options = state.namespaces
        if options.isEmpty, !state.selectedNamespace.isEmpty, !options.contains(state.selectedNamespace) {
            options.append(state.selectedNamespace)
        }

        return Array(Set(options)).sorted()
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

    public var visibleHelmReleases: [HelmReleaseSummary] {
        filtered(state.helmReleases) {
            "\($0.name) \($0.namespace) \($0.status) \($0.chart) \($0.appVersion)"
        }
    }

    public var visibleEvents: [EventSummary] {
        filtered(state.events) { event in
            "\(event.type) \(event.reason) \(event.objectName) \(event.message)"
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
            return "READ-ONLY MODE: stäng av read-only innan write-actions körs."
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
                    state.setServices([])
                    state.setIngresses([])
                    state.setConfigMaps([])
                    state.setSecrets([])
                    state.setNodes([])
                    state.setHelmReleases([])
                    state.setEvents([])
                    state.setOverviewSnapshot(
                        pods: [],
                        deploymentsCount: 0,
                        servicesCount: 0,
                        ingressesCount: 0,
                        configMapsCount: 0,
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

    public func reloadContexts() async throws {
        state.isLoading = true
        defer { state.isLoading = false }

        diagnostics.log("reloadContexts start")
        let contexts = try await kubeClient.listContexts(from: state.kubeConfigSources)
        state.setContexts(contexts)
        diagnostics.log("reloadContexts contexts=\(contexts.count)")

        if let selected = state.selectedContext {
            let requestedNamespace = preferredNamespaceForContext(selected, fallback: "")
            if state.selectedNamespace != requestedNamespace {
                state.selectedNamespace = requestedNamespace
            }

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

    public func refreshCurrentView() {
        scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: false, debounced: true)
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
        } catch {
            if error is CancellationError {
                return
            }
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
            state.selectedWorkloadKind = .node
        case .rbac where !rbacKinds.contains(state.selectedWorkloadKind):
            state.selectedWorkloadKind = .role
        case .helm:
            guard let context = state.selectedContext else { break }
            Task {
                do {
                    try await loadHelmReleases(context: context, namespace: state.selectedNamespace)
                } catch {
                    state.setError(error)
                }
            }
        default:
            break
        }

        if triggerReload, section != .helm {
            refreshCurrentView()
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
        state.selectedWorkloadKind = kind
        if state.selectedSection == .rbac {
            state.reconcileRBACSelection()
        }
        if triggerReload, shouldReloadForWorkloadKind(kind) {
            refreshCurrentView()
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
        diagnostics.log("setContext -> \(context.name)")
        overviewPrefetchTask?.cancel()
        state.selectedContext = context
        let requestedPreferredNamespace = preferredNamespace?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let savedPreferredNamespace = contextPreferences.loadPreferredNamespace(for: context.name)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let candidatePreferredNamespace = !requestedPreferredNamespace.isEmpty
            ? requestedPreferredNamespace
            : savedPreferredNamespace
        state.selectedNamespace = ""
        state.resourceSearchQuery = ""
        state.setNamespaces([])
        state.setPods([])
        state.setDeployments([])
        state.setStatefulSets([])
        state.setDaemonSets([])
        state.setServices([])
        state.setIngresses([])
        state.setConfigMaps([])
        state.setSecrets([])
        state.setNodes([])
        state.setEvents([])
        state.setHelmReleases([])
        state.setOverviewSnapshot(
            pods: [],
            deploymentsCount: 0,
            servicesCount: 0,
            ingressesCount: 0,
            configMapsCount: 0,
            nodesCount: 0,
            events: []
        )
        state.clearResourceDetails()

        let cachedNamespaces = store.namespaces(context: context)
        if !cachedNamespaces.isEmpty {
            state.selectedNamespace = resolvedNamespace(
                preferred: candidatePreferredNamespace,
                availableNamespaces: cachedNamespaces,
                contextDefaultNamespace: nil
            )
        } else if !requestedPreferredNamespace.isEmpty {
            // Navigation checkpoint supplies a namespace string before `listNamespaces` has run for this context.
            state.selectedNamespace = requestedPreferredNamespace
        }
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

        diagnostics.log("setNamespace -> \(trimmed)")
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
            scheduleRefreshCurrentView(forceNamespaceMetadataRefresh: false, debounced: false)
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
        let targetNs = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !targetNs.isEmpty && targetNs != state.selectedNamespace {
            pendingOpenEventSource = event
            setNamespace(targetNs, trackHistory: false, triggerReload: true)
            return
        }
        navigateToEventSource(event)
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
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.pod, trackHistory: false, triggerReload: false)
            if let pod = state.pods.first(where: { $0.name == name }) {
                selectPod(pod, trackHistory: true)
            } else {
                showEventDetail()
            }
        case "deployment":
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.deployment, trackHistory: false, triggerReload: false)
            if let deployment = state.deployments.first(where: { $0.name == name }) {
                selectDeployment(deployment, trackHistory: true)
            } else {
                showEventDetail()
            }
        case "statefulset":
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.statefulSet, trackHistory: false, triggerReload: false)
            if let resource = state.statefulSets.first(where: { $0.name == name }) {
                selectStatefulSet(resource, trackHistory: true)
            } else {
                showEventDetail()
            }
        case "daemonset":
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.daemonSet, trackHistory: false, triggerReload: false)
            if let resource = state.daemonSets.first(where: { $0.name == name }) {
                selectDaemonSet(resource, trackHistory: true)
            } else {
                showEventDetail()
            }
        case "service":
            setSection(.networking, trackHistory: false, triggerReload: false)
            setWorkloadKind(.service, trackHistory: false, triggerReload: false)
            if let service = state.services.first(where: { $0.name == name }) {
                selectService(service, trackHistory: true)
            } else {
                showEventDetail()
            }
        case "ingress":
            setSection(.networking, trackHistory: false, triggerReload: false)
            setWorkloadKind(.ingress, trackHistory: false, triggerReload: false)
            if let resource = state.ingresses.first(where: { $0.name == name }) {
                selectIngress(resource, trackHistory: true)
            } else {
                showEventDetail()
            }
        case "configmap":
            setSection(.config, trackHistory: false, triggerReload: false)
            setWorkloadKind(.configMap, trackHistory: false, triggerReload: false)
            if let resource = state.configMaps.first(where: { $0.name == name }) {
                selectConfigMap(resource, trackHistory: true)
            } else {
                showEventDetail()
            }
        case "secret":
            setSection(.config, trackHistory: false, triggerReload: false)
            setWorkloadKind(.secret, trackHistory: false, triggerReload: false)
            if let resource = state.secrets.first(where: { $0.name == name }) {
                selectSecret(resource, trackHistory: true)
            } else {
                showEventDetail()
            }
        case "node":
            setSection(.storage, trackHistory: false, triggerReload: false)
            setWorkloadKind(.node, trackHistory: false, triggerReload: false)
            if let resource = state.nodes.first(where: { $0.name == name }) {
                selectNode(resource, trackHistory: true)
            } else {
                showEventDetail()
            }
        case "":
            setSection(.workloads, trackHistory: false, triggerReload: false)
            setWorkloadKind(.pod, trackHistory: false, triggerReload: false)
            if let pod = state.pods.first(where: { $0.name == name }) {
                selectPod(pod, trackHistory: true)
            } else {
                showEventDetail()
            }
        default:
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
        state.setSelectedEvent(event)
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectStatefulSet(_ resource: ClusterResourceSummary?) {
        selectStatefulSet(resource, trackHistory: true)
    }

    private func selectStatefulSet(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
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
        state.setSelectedDaemonSet(resource)
        state.selectedWorkloadKind = .daemonSet
        loadResourceDetailsForCurrentSelection()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectIngress(_ resource: ClusterResourceSummary?) {
        selectIngress(resource, trackHistory: true)
    }

    private func selectIngress(_ resource: ClusterResourceSummary?, trackHistory: Bool) {
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
        state.setSelectedRBACResource(resource)
        if let resource {
            state.selectedWorkloadKind = resource.kind
        }
        loadResourceDetailsForCurrentSelection()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func selectHelmRelease(_ release: HelmReleaseSummary?) {
        selectHelmRelease(release, trackHistory: true)
    }

    private func selectHelmRelease(_ release: HelmReleaseSummary?, trackHistory: Bool) {
        state.setSelectedHelmRelease(release)
        loadHelmDetailsForCurrentSelection()
        if trackHistory {
            recordNavigationCheckpoint()
        }
    }

    public func reloadLogsForSelection() {
        Task {
            do {
                guard let context = state.selectedContext else { return }
                switch state.selectedWorkloadKind {
                case .pod:
                    guard let pod = state.selectedPod else { return }
                    state.isLoadingLogs = true
                    defer { state.isLoadingLogs = false }
                    let logs = try await kubeClient.podLogs(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        podName: pod.name,
                        filter: selectedLogPreset.filter,
                        previous: includePreviousLogs
                    )
                    state.setPodLogs(logs)
                case .service:
                    guard let service = state.selectedService else { return }
                    state.isLoadingLogs = true
                    defer { state.isLoadingLogs = false }
                    let unified = try await kubeClient.unifiedLogsForService(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        service: service,
                        filter: selectedLogPreset.filter,
                        previous: includePreviousLogs
                    )
                    state.setUnifiedServiceLogs(unified.mergedText, pods: unified.podNames)
                case .deployment:
                    guard let deployment = state.selectedDeployment else { return }
                    state.isLoadingLogs = true
                    defer { state.isLoadingLogs = false }
                    let unified = try await kubeClient.unifiedLogsForDeployment(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deployment: deployment,
                        filter: selectedLogPreset.filter,
                        previous: includePreviousLogs
                    )
                    state.setUnifiedServiceLogs(unified.mergedText, pods: unified.podNames)
                case .statefulSet, .daemonSet, .ingress, .configMap, .secret, .node, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                    return
                }
            } catch {
                state.setError(error)
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
            case .statefulSet, .daemonSet, .ingress, .configMap, .secret, .node, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
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

    public func saveCurrentResourceYAML() {
        do {
            guard let (kind, name) = currentWritableResource(), !state.resourceYAML.isEmpty else { return }
            let timestamp = ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")

            _ = try exporter.save(
                data: Data(state.resourceYAML.utf8),
                suggestedName: "\(kind.kubectlName)-\(name)-\(timestamp).yaml",
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
                suggestedName: "\(kind.kubectlName)-\(name)-describe-\(timestamp).txt",
                allowedFileTypes: ["txt", "log"]
            )
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

    public func requestApplySelectedResourceYAML() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let (kind, name) = currentWritableResource(), !state.resourceYAML.isEmpty else { return }
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

    public func requestRollbackSelectedHelmRelease() {
        guard writeActionsEnabled else {
            state.setError(RuneError.readOnlyMode)
            return
        }
        guard let release = state.selectedHelmRelease else { return }

        do {
            guard let revision = try parseOptionalRevisionInput(helmRollbackRevisionInput) else {
                throw RuneError.invalidInput(message: "helm rollback kräver en revisionssiffra.")
            }
            pendingWriteAction = .helmRollback(releaseName: release.name, namespace: release.namespace, revision: revision)
        } catch {
            state.setError(error)
        }
    }

    public func startPortForwardForSelection() {
        Task {
            do {
                guard let context = state.selectedContext else { return }
                let localPort = try parsePort(portForwardLocalPortInput, fieldName: "local port")
                let remotePort = try parsePort(portForwardRemotePortInput, fieldName: "remote port")
                let address = normalizedPortForwardAddress()

                let target: (PortForwardTargetKind, String)
                switch state.selectedWorkloadKind {
                case .pod:
                    guard let pod = state.selectedPod else { return }
                    target = (.pod, pod.name)
                case .service:
                    guard let service = state.selectedService else { return }
                    target = (.service, service.name)
                case .deployment, .statefulSet, .daemonSet, .ingress, .configMap, .secret, .node, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                    throw RuneError.invalidInput(message: "Port-forward stöds just nu för pod eller service.")
                }

                state.isStartingPortForward = true
                defer { state.isStartingPortForward = false }

                let session = try await kubeClient.startPortForward(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    targetKind: target.0,
                    targetName: target.1,
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
                state.selectedSection = .terminal
            } catch {
                state.setError(error)
            }
        }
    }

    public func stopPortForward(_ session: PortForwardSession) {
        Task {
            await kubeClient.stopPortForward(sessionID: session.id)
        }
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

                switch action {
                case let .delete(kind, name):
                    try await kubeClient.deleteResource(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        kind: kind,
                        name: name
                    )
                case let .apply(_, _, yaml):
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
                    try await kubeClient.rollbackDeploymentRollout(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: state.selectedNamespace,
                        deploymentName: deploymentName,
                        revision: revision
                    )
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
                    state.selectedSection = .terminal
                    return
                case let .helmRollback(releaseName, namespace, revision):
                    try await helmClient.rollbackRelease(
                        from: state.kubeConfigSources,
                        context: context,
                        namespace: namespace,
                        releaseName: releaseName,
                        revision: revision
                    )
                    try await loadHelmReleases(context: context, namespace: state.selectedNamespace)
                    if let selected = state.selectedHelmRelease {
                        selectHelmRelease(selected)
                    }
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
        switch item.action {
        case let .section(section):
            setSection(section)
        case let .context(context):
            setContext(context)
        case let .namespace(namespace):
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
        guard let first = commandPaletteItems(query: query).first else { return }
        executeCommandPaletteItem(first)
    }

    private func loadResourceSnapshot(
        context: KubeContext,
        namespace: String,
        requestID: UUID,
        forceNamespaceMetadataRefresh: Bool = false
    ) async throws {
        guard snapshotRequestIsCurrent(requestID, context: context) else {
            diagnostics.log("loadResourceSnapshot ignored stale start context=\(context.name) namespace=\(namespace)")
            return
        }

        state.isLoading = true
        defer { state.isLoading = false }

        diagnostics.log("loadResourceSnapshot start context=\(context.name) namespace=\(namespace)")

        let cachedNamespaces = store.namespaces(context: context)
        let cachedNodes = store.nodes(context: context)
        let now = Date()
        let lastNamespaceRefresh = namespaceMetadataRefreshedAt[context.name]
        let namespaceMetadataIsStale = lastNamespaceRefresh.map { now.timeIntervalSince($0) > namespaceMetadataTTL } ?? true
        let namespaceInputIsEmpty = namespace.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        let shouldRefreshNamespaceMetadata = forceNamespaceMetadataRefresh
            || cachedNamespaces.isEmpty
            || namespaceMetadataIsStale
            || namespaceInputIsEmpty

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
                loadedNamespaces = value
                namespaceMetadataRefreshedAt[context.name] = now
            case let .failure(error):
                diagnostics.log("snapshot namespaces failed: \(error.localizedDescription)")
                warnings.append("namespaces: \(error.localizedDescription)")
                loadedNamespaces = cachedNamespaces
                if cachedNamespaces.isEmpty {
                    namespaceMetadataRefreshedAt.removeValue(forKey: context.name)
                }
            }
        } else {
            contextDefaultNamespace = nil
            loadedNamespaces = cachedNamespaces
            diagnostics.log("loadResourceSnapshot using cached namespaces context=\(context.name) count=\(loadedNamespaces.count)")
        }

        guard snapshotRequestIsCurrent(requestID, context: context) else {
            diagnostics.log("loadResourceSnapshot discarded stale result context=\(context.name) namespace=\(namespace)")
            return
        }

        let trimmedIncoming = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        let savedForContext = contextPreferences.loadPreferredNamespace(for: context.name)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let preferredForResolution: String
        if loadedNamespaces.isEmpty {
            // No cluster namespace list yet: resolve from saved preference only; ignore UI namespace if it came from another context.
            preferredForResolution = savedForContext
        } else if !trimmedIncoming.isEmpty {
            preferredForResolution = trimmedIncoming
        } else if !savedForContext.isEmpty {
            preferredForResolution = savedForContext
        } else {
            preferredForResolution = ""
        }

        let effectiveNamespace = resolvedNamespace(
            preferred: preferredForResolution,
            availableNamespaces: loadedNamespaces,
            contextDefaultNamespace: contextDefaultNamespace
        )
        if effectiveNamespace != trimmedIncoming {
            diagnostics.log("namespace adjusted from \(trimmedIncoming) to \(effectiveNamespace) for context=\(context.name)")
        }

        if state.selectedNamespace != effectiveNamespace {
            state.selectedNamespace = effectiveNamespace
        }
        contextPreferences.savePreferredNamespace(effectiveNamespace, for: context.name)
        rememberRecentNamespace(effectiveNamespace, for: context.name)

        let cachedSnapshot = store.snapshot(context: context, namespace: effectiveNamespace)
        var computedPlan = SnapshotLoadPlan.forSelection(section: state.selectedSection, kind: state.selectedWorkloadKind)
        if computedPlan.podStatuses {
            if cachedSnapshot.deployments.isEmpty {
                computedPlan.deployments = true
                computedPlan.deploymentCount = false
            }
            if cachedSnapshot.services.isEmpty {
                computedPlan.services = true
                computedPlan.servicesCount = false
            }
        }
        let plan = computedPlan
        let warmOverview = await warmOverviewSnapshot(
            contextName: context.name,
            namespace: effectiveNamespace,
            reference: now,
            allowDiskCache: !forceNamespaceMetadataRefresh && plan.podStatuses
        )

        let preservedRBACRoles = state.rbacRoles
        let preservedRBACRoleBindings = state.rbacRoleBindings
        let preservedRBACClusterRoles = state.rbacClusterRoles
        let preservedRBACClusterRoleBindings = state.rbacClusterRoleBindings
        let currentOverviewClusterCPUPercent = state.overviewClusterCPUPercent
        let currentOverviewClusterMemoryPercent = state.overviewClusterMemoryPercent
        let shouldRefreshClusterUsage = plan.podStatuses || plan.pods

        async let clusterUsageResult: (cpuPercent: Int?, memoryPercent: Int?) = {
            if shouldRefreshClusterUsage {
                if let warmOverview,
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
                    return warmOverview.pods
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
            guard plan.deployments else { return cachedSnapshot.deployments }
            return try await kubeClient.listDeployments(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let deploymentCountResult: Result<Int, Error> = Self.capture {
            guard plan.deploymentCount else { return cachedSnapshot.deployments.count }
            if !cachedSnapshot.deployments.isEmpty {
                return cachedSnapshot.deployments.count
            }
            if let warmOverview {
                return warmOverview.deploymentsCount
            }
            return try await kubeClient.countNamespacedResources(
                from: state.kubeConfigSources,
                context: context,
                namespace: effectiveNamespace,
                resource: "deployments"
            )
        }
        async let statefulSetResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.statefulSets else { return cachedSnapshot.statefulSets }
            return try await kubeClient.listStatefulSets(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let daemonSetResult: Result<[ClusterResourceSummary], Error> = Self.capture {
            guard plan.daemonSets else { return cachedSnapshot.daemonSets }
            return try await kubeClient.listDaemonSets(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let serviceResult: Result<[ServiceSummary], Error> = Self.capture {
            guard plan.services else { return cachedSnapshot.services }
            return try await kubeClient.listServices(from: state.kubeConfigSources, context: context, namespace: effectiveNamespace)
        }
        async let serviceCountResult: Result<Int, Error> = Self.capture {
            guard plan.servicesCount else { return cachedSnapshot.services.count }
            if !cachedSnapshot.services.isEmpty {
                return cachedSnapshot.services.count
            }
            if let warmOverview {
                return warmOverview.servicesCount
            }
            return try await kubeClient.countNamespacedResources(
                from: state.kubeConfigSources,
                context: context,
                namespace: effectiveNamespace,
                resource: "services"
            )
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
            if let warmOverview {
                return warmOverview.ingressesCount
            }
            return try await kubeClient.countNamespacedResources(
                from: state.kubeConfigSources,
                context: context,
                namespace: effectiveNamespace,
                resource: "ingresses"
            )
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
            if let warmOverview {
                return warmOverview.configMapsCount
            }
            return try await kubeClient.countNamespacedResources(
                from: state.kubeConfigSources,
                context: context,
                namespace: effectiveNamespace,
                resource: "configmaps"
            )
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
            if let warmOverview {
                return warmOverview.nodesCount
            }
            return try await kubeClient.countClusterResources(
                from: state.kubeConfigSources,
                context: context,
                resource: "nodes"
            )
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
        let loadedServices = unwrap(await serviceResult, label: "services", fallback: cachedSnapshot.services, warnings: &warnings)
        let loadedIngresses = unwrap(await ingressResult, label: "ingresses", fallback: cachedSnapshot.ingresses, warnings: &warnings)
        let loadedConfigMaps = unwrap(await configMapResult, label: "configmaps", fallback: cachedSnapshot.configMaps, warnings: &warnings)
        let loadedSecrets = unwrap(await secretResult, label: "secrets", fallback: cachedSnapshot.secrets, warnings: &warnings)
        let loadedNodes = unwrap(await nodeResult, label: "nodes", fallback: cachedNodes, warnings: &warnings)
        let loadedEvents = unwrap(await eventResult, label: "events", fallback: cachedSnapshot.events, warnings: &warnings)
        let loadedDeploymentCount = plan.deploymentCount
            ? unwrap(await deploymentCountResult, label: "deployments-count", fallback: loadedDeployments.count, warnings: &warnings)
            : loadedDeployments.count
        let loadedServiceCount = plan.servicesCount
            ? unwrap(await serviceCountResult, label: "services-count", fallback: loadedServices.count, warnings: &warnings)
            : loadedServices.count
        let loadedIngressCount = plan.ingressesCount
            ? unwrap(await ingressCountResult, label: "ingresses-count", fallback: loadedIngresses.count, warnings: &warnings)
            : loadedIngresses.count
        let loadedConfigMapCount = plan.configMapsCount
            ? unwrap(await configMapCountResult, label: "configmaps-count", fallback: loadedConfigMaps.count, warnings: &warnings)
            : loadedConfigMaps.count
        let loadedNodeCount = plan.nodesCount
            ? unwrap(await nodeCountResult, label: "nodes-count", fallback: loadedNodes.count, warnings: &warnings)
            : loadedNodes.count
        let loadedClusterUsage = await clusterUsageResult
        let loadedClusterCPUPercent = loadedClusterUsage.cpuPercent
        let loadedClusterMemoryPercent = loadedClusterUsage.memoryPercent
        if shouldRefreshClusterUsage,
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

        guard snapshotRequestIsCurrent(requestID, context: context) else {
            diagnostics.log("loadResourceSnapshot discarded stale resource result context=\(context.name) namespace=\(effectiveNamespace)")
            return
        }

        store.cacheNamespaces(loadedNamespaces, context: context)
        store.cacheNodes(loadedNodes, context: context)
        store.cacheSnapshot(
            context: context,
            namespace: effectiveNamespace,
            pods: loadedPods,
            deployments: loadedDeployments,
            statefulSets: loadedStatefulSets,
            daemonSets: loadedDaemonSets,
            services: loadedServices,
            ingresses: loadedIngresses,
            configMaps: loadedConfigMaps,
            secrets: loadedSecrets,
            events: loadedEvents
        )

        state.setNamespaces(loadedNamespaces)
        state.setPods(loadedPods)
        state.setDeployments(loadedDeployments)
        state.setStatefulSets(loadedStatefulSets)
        state.setDaemonSets(loadedDaemonSets)
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
            state.setErrorMessage("Delvis laddning: \(warningText)")
            diagnostics.log("loadResourceSnapshot partial warnings: \(warningText)")
        }

        loadOverviewSnapshot(
            context: context,
            namespace: effectiveNamespace,
            requestID: requestID,
            pods: loadedPods,
            deploymentsCount: loadedDeploymentCount,
            servicesCount: loadedServiceCount,
            ingressesCount: loadedIngressCount,
            configMapsCount: loadedConfigMapCount,
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

        guard snapshotRequestIsCurrent(requestID, context: context) else {
            diagnostics.log("loadResourceSnapshot skipped details for stale context=\(context.name) namespace=\(namespace)")
            return
        }

        if shouldLoadResourceDetailsForCurrentSection {
            await loadResourceDetailsForCurrentSelectionAsync()
        } else {
            diagnostics.log("loadResourceSnapshot skipped heavy resource details for section=\(state.selectedSection.rawValue)")
        }

        if let pending = pendingOpenEventSource {
            pendingOpenEventSource = nil
            navigateToEventSource(pending)
        }

        diagnostics.log("loadResourceSnapshot done context=\(context.name) namespace=\(namespace)")
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
        nodesCount: Int,
        clusterCPUPercent: Int?,
        clusterMemoryPercent: Int?,
        events: [EventSummary]
    ) {
        diagnostics.log("loadOverviewSnapshot start context=\(context.name) namespace=\(namespace)")
        guard snapshotRequestIsCurrent(requestID, context: context) else {
            diagnostics.log("loadOverviewSnapshot discarded stale result context=\(context.name)")
            return
        }

        state.setOverviewSnapshot(
            pods: pods,
            deploymentsCount: deploymentsCount,
            servicesCount: servicesCount,
            ingressesCount: ingressesCount,
            configMapsCount: configMapsCount,
            nodesCount: nodesCount,
            clusterCPUPercent: clusterCPUPercent,
            clusterMemoryPercent: clusterMemoryPercent,
            events: events
        )
        diagnostics.log(
            "loadOverviewSnapshot done context=\(context.name) pods=\(pods.count) deployments=\(deploymentsCount) services=\(servicesCount)"
        )
    }

    private func loadHelmReleases(context: KubeContext, namespace: String) async throws {
        state.isLoading = true
        defer { state.isLoading = false }

        let releases = try await helmClient.listReleases(
            from: state.kubeConfigSources,
            context: context,
            namespace: state.isHelmAllNamespaces ? nil : namespace,
            allNamespaces: state.isHelmAllNamespaces
        )

        state.setHelmReleases(releases)
        if let selected = state.selectedHelmRelease {
            state.setSelectedHelmRelease(selected)
        }
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

            async let values = helmClient.releaseValues(
                from: state.kubeConfigSources,
                context: context,
                namespace: release.namespace,
                releaseName: release.name
            )

            async let manifest = helmClient.releaseManifest(
                from: state.kubeConfigSources,
                context: context,
                namespace: release.namespace,
                releaseName: release.name
            )

            async let history = helmClient.releaseHistory(
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

    private func resolvedKubeConfigSources(fallbackURLs: [URL]) throws -> [KubeConfigSource] {
        let bookmarked = try bookmarkManager.loadKubeConfigSources()
        let fallback = fallbackURLs.map(KubeConfigSource.init(url:))

        var merged: [String: KubeConfigSource] = [:]
        for source in bookmarked + fallback {
            let standardizedPath = URL(fileURLWithPath: source.path).standardizedFileURL.path
            merged[standardizedPath] = KubeConfigSource(url: URL(fileURLWithPath: standardizedPath))
        }

        return merged.values.sorted { $0.path < $1.path }
    }

    private func loadResourceDetailsForCurrentSelection() {
        Task {
            await loadResourceDetailsForCurrentSelectionAsync()
        }
    }

    private func fetchYAMLAndDescribe(
        context: KubeContext,
        namespace: String,
        kind: KubeResourceKind,
        name: String
    ) async throws -> (yaml: String, describe: String) {
        async let yaml = kubeClient.resourceYAML(
            from: state.kubeConfigSources,
            context: context,
            namespace: namespace,
            kind: kind,
            name: name
        )
        async let describe = kubeClient.resourceDescribe(
            from: state.kubeConfigSources,
            context: context,
            namespace: namespace,
            kind: kind,
            name: name
        )
        return (try await yaml, try await describe)
    }

    private func loadResourceDetailsForCurrentSelectionAsync() async {
        do {
            guard let context = state.selectedContext else { return }

            switch state.selectedWorkloadKind {
            case .pod:
                guard let pod = state.selectedPod else {
                    state.clearResourceDetails()
                    return
                }

                state.isLoadingLogs = true
                defer { state.isLoadingLogs = false }

                async let logs = kubeClient.podLogs(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    podName: pod.name,
                    filter: selectedLogPreset.filter,
                    previous: includePreviousLogs
                )

                async let yaml = kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .pod,
                    name: pod.name
                )
                async let describe = kubeClient.resourceDescribe(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .pod,
                    name: pod.name
                )

                state.setPodLogs(try await logs)
                state.setResourceYAML(try await yaml)
                state.setResourceDescribe(try await describe)
                state.clearUnifiedServiceLogs()

            case .service:
                guard let service = state.selectedService else {
                    state.clearResourceDetails()
                    return
                }

                state.isLoadingLogs = true
                defer { state.isLoadingLogs = false }

                async let unified = kubeClient.unifiedLogsForService(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    service: service,
                    filter: selectedLogPreset.filter,
                    previous: includePreviousLogs
                )

                async let yaml = kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .service,
                    name: service.name
                )
                async let describe = kubeClient.resourceDescribe(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .service,
                    name: service.name
                )

                let unifiedResult = try await unified
                state.setUnifiedServiceLogs(unifiedResult.mergedText, pods: unifiedResult.podNames)
                state.setResourceYAML(try await yaml)
                state.setResourceDescribe(try await describe)
                state.setPodLogs("")

            case .deployment:
                guard let deployment = state.selectedDeployment else {
                    state.clearResourceDetails()
                    return
                }

                state.isLoadingLogs = true
                defer { state.isLoadingLogs = false }

                async let unified = kubeClient.unifiedLogsForDeployment(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    deployment: deployment,
                    filter: selectedLogPreset.filter,
                    previous: includePreviousLogs
                )

                async let yaml = kubeClient.resourceYAML(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .deployment,
                    name: deployment.name
                )
                async let describe = kubeClient.resourceDescribe(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .deployment,
                    name: deployment.name
                )

                async let history = kubeClient.deploymentRolloutHistory(
                    from: state.kubeConfigSources,
                    context: context,
                    namespace: state.selectedNamespace,
                    deploymentName: deployment.name
                )

                let unifiedResult = try await unified
                state.setUnifiedServiceLogs(unifiedResult.mergedText, pods: unifiedResult.podNames)
                state.setResourceYAML(try await yaml)
                state.setResourceDescribe(try await describe)
                state.setDeploymentRolloutHistory(try await history)
                state.setPodLogs("")

            case .statefulSet:
                guard let resource = state.selectedStatefulSet else {
                    state.clearResourceDetails()
                    return
                }

                let pair = try await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .statefulSet,
                    name: resource.name
                )

                state.setResourceYAML(pair.yaml)
                state.setResourceDescribe(pair.describe)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .daemonSet:
                guard let resource = state.selectedDaemonSet else {
                    state.clearResourceDetails()
                    return
                }

                let pair = try await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .daemonSet,
                    name: resource.name
                )

                state.setResourceYAML(pair.yaml)
                state.setResourceDescribe(pair.describe)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .ingress:
                guard let resource = state.selectedIngress else {
                    state.clearResourceDetails()
                    return
                }

                let pair = try await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .ingress,
                    name: resource.name
                )

                state.setResourceYAML(pair.yaml)
                state.setResourceDescribe(pair.describe)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .configMap:
                guard let resource = state.selectedConfigMap else {
                    state.clearResourceDetails()
                    return
                }

                let pair = try await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .configMap,
                    name: resource.name
                )

                state.setResourceYAML(pair.yaml)
                state.setResourceDescribe(pair.describe)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .secret:
                guard let resource = state.selectedSecret else {
                    state.clearResourceDetails()
                    return
                }

                let pair = try await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .secret,
                    name: resource.name
                )

                state.setResourceYAML(pair.yaml)
                state.setResourceDescribe(pair.describe)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .node:
                guard let resource = state.selectedNode else {
                    state.clearResourceDetails()
                    return
                }

                let pair = try await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: .node,
                    name: resource.name
                )

                state.setResourceYAML(pair.yaml)
                state.setResourceDescribe(pair.describe)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                guard let resource = state.selectedRBACResource else {
                    state.clearResourceDetails()
                    return
                }

                let pair = try await fetchYAMLAndDescribe(
                    context: context,
                    namespace: state.selectedNamespace,
                    kind: resource.kind,
                    name: resource.name
                )

                state.setResourceYAML(pair.yaml)
                state.setResourceDescribe(pair.describe)
                state.setPodLogs("")
                state.clearUnifiedServiceLogs()

            case .event:
                state.clearResourceDetails()
            }
        } catch {
            state.setError(error)
        }
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
        case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            guard let resource = state.selectedRBACResource else { return nil }
            return (resource.kind, resource.name)
        case .event:
            return nil
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
        case (.networking, .service):
            return state.services.isEmpty
        case (.networking, .ingress):
            return state.ingresses.isEmpty
        case (.config, .configMap):
            return state.configMaps.isEmpty
        case (.config, .secret):
            return state.secrets.isEmpty
        case (.storage, .node):
            return state.nodes.isEmpty
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
            selectedIngressName: state.selectedIngress?.name,
            selectedConfigMapName: state.selectedConfigMap?.name,
            selectedSecretName: state.selectedSecret?.name,
            selectedNodeName: state.selectedNode?.name,
            selectedHelmReleaseID: state.selectedHelmRelease?.id,
            selectedRBACResourceID: state.selectedRBACResource?.id
        )
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
        } else if checkpoint.contextName == nil {
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
        case .ingress:
            selectIngress(state.ingresses.first(where: { $0.name == checkpoint.selectedIngressName }), trackHistory: false)
        case .configMap:
            selectConfigMap(state.configMaps.first(where: { $0.name == checkpoint.selectedConfigMapName }), trackHistory: false)
        case .secret:
            selectSecret(state.secrets.first(where: { $0.name == checkpoint.selectedSecretName }), trackHistory: false)
        case .node:
            selectNode(state.nodes.first(where: { $0.name == checkpoint.selectedNodeName }), trackHistory: false)
        case .event:
            selectEvent(state.events.first(where: { $0.id == checkpoint.selectedEventID }), trackHistory: false)
        case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            let lists = state.rbacRoles + state.rbacRoleBindings + state.rbacClusterRoles + state.rbacClusterRoleBindings
            let match = lists.first(where: { $0.id == checkpoint.selectedRBACResourceID })
            selectRBACResource(match, trackHistory: false)
        }

        if checkpoint.section == .helm {
            let release = state.helmReleases.first(where: { $0.id == checkpoint.selectedHelmReleaseID })
            selectHelmRelease(release, trackHistory: false)
        }
    }

    private func updateNavigationAvailability() {
        canNavigateBack = navigationIndex > 0
        canNavigateForward = navigationIndex >= 0 && navigationIndex < navigationHistory.count - 1
    }

    private static func overviewCacheKey(contextName: String, namespace: String) -> String {
        "\(contextName)::\(namespace)"
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
            nodesCount: nodesCount,
            clusterCPUPercent: clusterCPUPercent,
            clusterMemoryPercent: clusterMemoryPercent,
            events: events
        )

        Task(priority: .utility) { [overviewSnapshotPersistence] in
            await overviewSnapshotPersistence.saveSnapshot(persisted)
        }
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
        if ProcessInfo.processInfo.environment["XCTestConfigurationFilePath"] != nil
            || ProcessInfo.processInfo.environment["SWIFT_TESTING_ENABLED"] != nil
            || NSClassFromString("XCTestCase") != nil {
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
        let nodeCountFallback = max(store.nodes(context: context).count, state.overviewNodesCount)
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
                    _ = await MainActor.run { [weak self] in
                        self?.cachePersistedOverviewSnapshot(persisted)
                    }
                    continue
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

                    let prefetchedPods = try await pods
                    let prefetchedDeploymentsCount = try await deploymentsCount
                    let prefetchedServicesCount = try await servicesCount
                    let prefetchedIngressesCount = try await ingressesCount
                    let prefetchedConfigMapsCount = try await configMapsCount

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
                            nodesCount: nodeCountFallback,
                            clusterCPUPercent: self.state.overviewClusterCPUPercent,
                            clusterMemoryPercent: self.state.overviewClusterMemoryPercent,
                            events: eventFallbackByNamespace[namespace] ?? []
                        )
                    }
                } catch {
                    await MainActor.run { [weak self] in
                        self?.diagnostics.log(
                            "overview prefetch failed context=\(contextName) namespace=\(namespace): \(error.localizedDescription)"
                        )
                    }
                }
            }
        }
    }

    private func beginSnapshotRequest(context: KubeContext, namespace: String, source: String) -> UUID {
        let requestID = UUID()
        latestSnapshotRequestID = requestID
        diagnostics.log("snapshot request=\(requestID.uuidString) source=\(source) context=\(context.name) namespace=\(namespace)")
        return requestID
    }

    private func snapshotRequestIsCurrent(_ requestID: UUID, context: KubeContext) -> Bool {
        guard latestSnapshotRequestID == requestID else {
            return false
        }

        return state.selectedContext?.name == context.name
    }

    private func resolvedNamespace(
        preferred: String,
        availableNamespaces: [String],
        contextDefaultNamespace: String?
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

        if !trimmedPreferred.isEmpty, availableNamespaces.contains(trimmedPreferred) {
            return trimmedPreferred
        }

        if !trimmedContextDefault.isEmpty, availableNamespaces.contains(trimmedContextDefault) {
            return trimmedContextDefault
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

    private func preferredNamespaceForContext(_ context: KubeContext, fallback: String) -> String {
        let preferred = contextPreferences.loadPreferredNamespace(for: context.name)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""

        if !preferred.isEmpty {
            return preferred
        }

        return fallback.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    /// Applies `ResourceStore` and fresh `overviewSnapshotCache` entries to `RuneAppState` synchronously (e.g. after `setContext` / `setNamespace` before network refresh).
    private func applyCachedSnapshot(context: KubeContext, namespace: String) {
        let normalizedNamespace = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        let cachedNamespaces = store.namespaces(context: context)
        if !cachedNamespaces.isEmpty {
            state.setNamespaces(cachedNamespaces)
        }

        let cachedNodes = store.nodes(context: context)
        if !cachedNodes.isEmpty {
            state.setNodes(cachedNodes)
        }

        guard !normalizedNamespace.isEmpty else { return }
        let cached = store.snapshot(context: context, namespace: normalizedNamespace)

        state.setPods(cached.pods)
        state.setDeployments(cached.deployments)
        state.setStatefulSets(cached.statefulSets)
        state.setDaemonSets(cached.daemonSets)
        state.setServices(cached.services)
        state.setIngresses(cached.ingresses)
        state.setConfigMaps(cached.configMaps)
        state.setSecrets(cached.secrets)
        state.setEvents(cached.events)

        let reference = Date()
        if let cachedOverview = overviewSnapshotCache[Self.overviewCacheKey(contextName: context.name, namespace: normalizedNamespace)],
           Self.isOverviewCacheFresh(cachedOverview, ttl: overviewSnapshotFreshnessTTL, reference: reference) {
            // Merge: `cachedOverview` supplies cluster CPU/MEM when present; non-empty `ResourceStore` lists override counts and pod rows.
            let mergedPods = cached.pods.isEmpty ? cachedOverview.pods : cached.pods
            let mergedDeploymentsCount = cached.deployments.isEmpty ? cachedOverview.deploymentsCount : cached.deployments.count
            let mergedServicesCount = cached.services.isEmpty ? cachedOverview.servicesCount : cached.services.count
            let mergedIngressesCount = cached.ingresses.isEmpty ? cachedOverview.ingressesCount : cached.ingresses.count
            let mergedConfigMapsCount = cached.configMaps.isEmpty ? cachedOverview.configMapsCount : cached.configMaps.count
            let mergedNodesCount = cachedNodes.isEmpty ? cachedOverview.nodesCount : cachedNodes.count
            let mergedEvents = cached.events.isEmpty ? cachedOverview.events : cached.events
            state.setOverviewSnapshot(
                pods: mergedPods,
                deploymentsCount: mergedDeploymentsCount,
                servicesCount: mergedServicesCount,
                ingressesCount: mergedIngressesCount,
                configMapsCount: mergedConfigMapsCount,
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
            nodesCount: cachedNodes.count,
            clusterCPUPercent: nil,
            clusterMemoryPercent: nil,
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

        let now = Date()
        let entry = cachePersistedOverviewSnapshot(persisted, reference: now)
        guard Self.isOverviewCacheFresh(entry, ttl: overviewDiskSnapshotFreshnessTTL, reference: now) else { return }
        state.setOverviewSnapshot(
            pods: entry.pods,
            deploymentsCount: entry.deploymentsCount,
            servicesCount: entry.servicesCount,
            ingressesCount: entry.ingressesCount,
            configMapsCount: entry.configMapsCount,
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
            return visiblePods
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .prefix(40)
                .map { pod in
                    CommandPaletteItem(
                        id: "cmd:pod:\(pod.id)",
                        title: pod.name,
                        subtitle: "Pods • k9s-style `:po`",
                        symbolName: "cube.box",
                        action: .pod(pod)
                    )
                }
        case "dp", "deploy", "deployment", "deployments":
            return visibleDeployments
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .prefix(40)
                .map { deployment in
                    CommandPaletteItem(
                        id: "cmd:deployment:\(deployment.id)",
                        title: deployment.name,
                        subtitle: "Deployments • k9s-style `:deploy`",
                        symbolName: "shippingbox",
                        action: .deployment(deployment)
                    )
                }
        case "svc", "service", "services":
            return visibleServices
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .prefix(40)
                .map { service in
                    CommandPaletteItem(
                        id: "cmd:service:\(service.id)",
                        title: service.name,
                        subtitle: "Services • k9s-style `:svc`",
                        symbolName: "point.3.connected.trianglepath.dotted",
                        action: .service(service)
                    )
                }
        case "ctx", "context", "contexts":
            return visibleContexts
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .map { context in
                    CommandPaletteItem(
                        id: "cmd:context:\(context.id)",
                        title: context.name,
                        subtitle: "Contexts • k9s-style `:ctx`",
                        symbolName: state.isFavorite(context) ? "star.fill" : "network",
                        action: .context(context)
                    )
                }
        case "ns", "namespace", "namespaces":
            return namespaceOptions
                .filter { remainder.isEmpty || matches($0, query: remainder) }
                .map { namespace in
                    CommandPaletteItem(
                        id: "cmd:namespace:\(namespace)",
                        title: namespace,
                        subtitle: "Namespaces • k9s-style `:ns`",
                        symbolName: "square.3.layers.3d",
                        action: .namespace(namespace)
                    )
                }
        case "ev", "event", "events":
            return visibleEvents
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
        case "helm", "hr":
            return visibleHelmReleases
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
        case "sts", "statefulset", "statefulsets":
            return visibleStatefulSets
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .prefix(40)
                .map { resource in
                    CommandPaletteItem(
                        id: "cmd:sts:\(resource.id)",
                        title: resource.name,
                        subtitle: "StatefulSets • k9s-style `:sts`",
                        symbolName: "shippingbox",
                        action: .clusterResource(resource)
                    )
                }
        case "ds", "daemonset", "daemonsets":
            return visibleDaemonSets
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .prefix(40)
                .map { resource in
                    CommandPaletteItem(
                        id: "cmd:ds:\(resource.id)",
                        title: resource.name,
                        subtitle: "DaemonSets • k9s-style `:ds`",
                        symbolName: "shippingbox",
                        action: .clusterResource(resource)
                    )
                }
        case "ing", "ingress", "ingresses":
            return visibleIngresses
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .prefix(40)
                .map { resource in
                    CommandPaletteItem(
                        id: "cmd:ing:\(resource.id)",
                        title: resource.name,
                        subtitle: "Ingresses • k9s-style `:ing`",
                        symbolName: "network",
                        action: .clusterResource(resource)
                    )
                }
        case "cm", "configmap", "configmaps":
            return visibleConfigMaps
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .prefix(40)
                .map { resource in
                    CommandPaletteItem(
                        id: "cmd:cm:\(resource.id)",
                        title: resource.name,
                        subtitle: "ConfigMaps • k9s-style `:cm`",
                        symbolName: "doc.text",
                        action: .clusterResource(resource)
                    )
                }
        case "sec", "secret", "secrets":
            return visibleSecrets
                .filter { remainder.isEmpty || matches($0.name, query: remainder) }
                .prefix(40)
                .map { resource in
                    CommandPaletteItem(
                        id: "cmd:sec:\(resource.id)",
                        title: resource.name,
                        subtitle: "Secrets • k9s-style `:sec`",
                        symbolName: "key",
                        action: .clusterResource(resource)
                    )
                }
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
            return state.rbacRoles
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
        case "rb", "rolebinding", "rolebindings":
            return state.rbacRoleBindings
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
        case "cr", "clusterrole", "clusterroles":
            return state.rbacClusterRoles
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
        case "crb", "clusterrolebinding", "clusterrolebindings":
            return state.rbacClusterRoleBindings
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
            CommandPaletteItem(id: "help:svc", title: ":svc <name>", subtitle: "Services (Networking → Services)", symbolName: "point.3.connected.trianglepath.dotted", action: .resourceKind(section: .networking, kind: .service)),
            CommandPaletteItem(id: "help:ing", title: ":ing <name>", subtitle: "Ingresses", symbolName: "network", action: .resourceKind(section: .networking, kind: .ingress)),
            CommandPaletteItem(id: "help:cm", title: ":cm <name>", subtitle: "ConfigMaps", symbolName: "doc.text", action: .resourceKind(section: .config, kind: .configMap)),
            CommandPaletteItem(id: "help:sec", title: ":sec <name>", subtitle: "Secrets", symbolName: "key", action: .resourceKind(section: .config, kind: .secret)),
            CommandPaletteItem(id: "help:ns", title: ":ns <namespace>", subtitle: "Byt namespace (välj i listan)", symbolName: "square.3.layers.3d", action: .resourceKind(section: .workloads, kind: .pod)),
            CommandPaletteItem(id: "help:ctx", title: ":ctx <context>", subtitle: "Byt kubecontext", symbolName: "network", action: .section(.overview)),
            CommandPaletteItem(id: "help:rbac", title: ":rbac", subtitle: "Roles, bindings, cluster roles", symbolName: "person.2.badge.gearshape", action: .resourceKind(section: .rbac, kind: .role)),
            CommandPaletteItem(id: "help:helm", title: ":helm <release>", subtitle: "Helm releases", symbolName: "ferry", action: .section(.helm))
        ]
    }

    private func resourceCounts() -> [String: Int] {
        [
            "pods": state.pods.count,
            "deployments": state.deployments.count,
            "statefulsets": state.statefulSets.count,
            "daemonsets": state.daemonSets.count,
            "services": state.services.count,
            "ingresses": state.ingresses.count,
            "configmaps": state.configMaps.count,
            "secrets": state.secrets.count,
            "nodes": state.nodes.count,
            "events": state.events.count,
            "helmReleases": state.helmReleases.count,
            "roles": state.rbacRoles.count,
            "roleBindings": state.rbacRoleBindings.count,
            "clusterRoles": state.rbacClusterRoles.count,
            "clusterRoleBindings": state.rbacClusterRoleBindings.count
        ]
    }

    private func selectedResourceKindLabel() -> String? {
        switch state.selectedSection {
        case .workloads, .networking, .config, .storage, .rbac:
            return currentWritableResource()?.0.kubectlName
        case .events:
            return state.selectedEvent == nil ? nil : "event"
        case .helm:
            return state.selectedHelmRelease == nil ? nil : "helm-release"
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
            return state.selectedHelmRelease?.name
        default:
            return nil
        }
    }

    private func parsePort(_ value: String, fieldName: String) throws -> Int {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let port = Int(trimmed), (1...65535).contains(port) else {
            throw RuneError.invalidInput(message: "\(fieldName) måste vara ett nummer mellan 1 och 65535.")
        }
        return port
    }

    private func parseOptionalRevisionInput(_ value: String) throws -> Int? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return nil
        }

        guard let revision = Int(trimmed), revision > 0 else {
            throw RuneError.invalidInput(message: "revision måste vara ett positivt heltal.")
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
            throw RuneError.invalidInput(message: "exec-kommandot får inte vara tomt.")
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
            throw RuneError.invalidInput(message: "saknar avslutande citationstecken \(activeQuote).")
        }

        if !current.isEmpty {
            tokens.append(current)
        }

        guard !tokens.isEmpty else {
            throw RuneError.invalidInput(message: "exec-kommandot kunde inte tolkas.")
        }

        return tokens
    }

}
