import Combine
import Foundation
import OSLog

public struct ResourceDetailScope: Hashable, Codable, Sendable {
    public let contextName: String
    public let namespace: String
    public let kind: String
    public let name: String

    public init(contextName: String, namespace: String?, kind: KubeResourceKind, name: String) {
        self.init(
            contextName: contextName,
            namespace: kind.isNamespaced ? namespace : nil,
            kind: kind.rawValue,
            name: name
        )
    }

    public init(contextName: String, namespace: String?, kind: String, name: String) {
        self.contextName = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        self.namespace = (namespace ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        self.kind = kind.trimmingCharacters(in: .whitespacesAndNewlines)
        self.name = name.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

@MainActor
public final class RuneAppState: ObservableObject {
    private let maxSessionLogCacheCharacters = 1_000_000
    private var maxSessionLogCacheEntries: Int {
        UserDefaults.standard.runeSessionLogCacheEntryLimit
    }
    private var maxResourceYAMLUndoSnapshots: Int {
        UserDefaults.standard.runeResourceYAMLUndoSnapshotLimit
    }

    @Published public private(set) var kubeConfigSources: [KubeConfigSource] = []
    @Published public private(set) var contexts: [KubeContext] = []
    @Published public private(set) var namespaces: [String] = []
    @Published public private(set) var favoriteContextNames: Set<String> = []
    @Published public private(set) var favoriteResourceIDs: Set<String> = []
    @Published public private(set) var favoriteNamespaceIDs: Set<String> = []
    @Published public private(set) var manualProductionContextIDs: Set<String> = []
    @Published public private(set) var authDoctorChecks: [RuneHealthCheck] = []
    @Published public private(set) var isRunningAuthDoctor = false
    @Published public private(set) var snapshotFreshness = RuneSnapshotFreshness()
    @Published public private(set) var resourceListFreshness: [RuneResourceListFamily: RuneResourceListFreshness] = [:]
    @Published public private(set) var isManualNamespaceMode = false
    @Published public private(set) var namespaceAccessWarning: String?

    @Published public var selectedContext: KubeContext?
    @Published public var selectedNamespace: String = "default"
    @Published public var selectedSection: RuneSection = .overview
    @Published public var selectedWorkloadKind: KubeResourceKind = .pod

    @Published public var selectedPod: PodSummary?
    @Published public private(set) var selectedPodIDs: Set<String> = []
    @Published public private(set) var selectedGenericResourceIDs: Set<String> = []
    @Published public var selectedDeployment: DeploymentSummary?
    @Published public var selectedService: ServiceSummary?
    @Published public var selectedEvent: EventSummary?
    @Published public var selectedStatefulSet: ClusterResourceSummary?
    @Published public var selectedDaemonSet: ClusterResourceSummary?
    @Published public var selectedJob: ClusterResourceSummary?
    @Published public var selectedCronJob: ClusterResourceSummary?
    @Published public var selectedReplicaSet: ClusterResourceSummary?
    @Published public var selectedPersistentVolumeClaim: ClusterResourceSummary?
    @Published public var selectedPersistentVolume: ClusterResourceSummary?
    @Published public var selectedStorageClass: ClusterResourceSummary?
    @Published public var selectedHorizontalPodAutoscaler: ClusterResourceSummary?
    @Published public var selectedNetworkPolicy: ClusterResourceSummary?
    @Published public var selectedIngress: ClusterResourceSummary?
    @Published public var selectedConfigMap: ClusterResourceSummary?
    @Published public var selectedSecret: ClusterResourceSummary?
    @Published public var selectedNode: ClusterResourceSummary?
    @Published public var selectedHelmRelease: HelmReleaseSummary?
    @Published public private(set) var selectedOperatorResource: OperatorResourceSummary?

    @Published public private(set) var pods: [PodSummary] = []
    @Published public private(set) var deployments: [DeploymentSummary] = []
    @Published public private(set) var services: [ServiceSummary] = []
    @Published public private(set) var events: [EventSummary] = []
    @Published public private(set) var statefulSets: [ClusterResourceSummary] = []
    @Published public private(set) var daemonSets: [ClusterResourceSummary] = []
    @Published public private(set) var jobs: [ClusterResourceSummary] = []
    @Published public private(set) var cronJobs: [ClusterResourceSummary] = []
    @Published public private(set) var replicaSets: [ClusterResourceSummary] = []
    @Published public private(set) var persistentVolumeClaims: [ClusterResourceSummary] = []
    @Published public private(set) var persistentVolumes: [ClusterResourceSummary] = []
    @Published public private(set) var storageClasses: [ClusterResourceSummary] = []
    @Published public private(set) var horizontalPodAutoscalers: [ClusterResourceSummary] = []
    @Published public private(set) var networkPolicies: [ClusterResourceSummary] = []
    @Published public private(set) var ingresses: [ClusterResourceSummary] = []
    @Published public private(set) var configMaps: [ClusterResourceSummary] = []
    @Published public private(set) var secrets: [ClusterResourceSummary] = []
    @Published public private(set) var nodes: [ClusterResourceSummary] = []
    @Published public private(set) var helmReleases: [HelmReleaseSummary] = []
    @Published public private(set) var operatorResources: [OperatorResourceSummary] = []
    @Published public private(set) var rbacRoles: [ClusterResourceSummary] = []
    @Published public private(set) var rbacRoleBindings: [ClusterResourceSummary] = []
    @Published public private(set) var rbacClusterRoles: [ClusterResourceSummary] = []
    @Published public private(set) var rbacClusterRoleBindings: [ClusterResourceSummary] = []
    @Published public private(set) var selectedRBACResource: ClusterResourceSummary?
    @Published public private(set) var overviewPods: [PodSummary] = []
    @Published public private(set) var overviewDeploymentsCount: Int = 0
    @Published public private(set) var overviewServicesCount: Int = 0
    @Published public private(set) var overviewIngressesCount: Int = 0
    @Published public private(set) var overviewConfigMapsCount: Int = 0
    @Published public private(set) var overviewCronJobsCount: Int = 0
    @Published public private(set) var overviewNodesCount: Int = 0
    @Published public private(set) var overviewClusterCPUPercent: Int?
    @Published public private(set) var overviewClusterMemoryPercent: Int?
    @Published public private(set) var overviewEvents: [EventSummary] = []

    @Published public private(set) var podLogs: String = ""
    @Published public private(set) var unifiedServiceLogs: String = ""
    @Published public private(set) var unifiedServiceLogPods: [String] = []
    @Published public private(set) var sessionLogCache: [String: String] = [:]
    private var sessionLogCacheKeysMostRecent: [String] = []
    /// Set when the latest log stream failed (timeout or error). Cleared on successful load or when a new fetch starts.
    @Published public private(set) var lastLogFetchError: String?
    @Published public private(set) var lastLogUpdatedAt: Date?
    @Published public private(set) var resourceYAML: String = ""
    /// Last manifest YAML Rune fetched for the selected resource. Baseline for unsaved-edit detection and Revert.
    @Published public private(set) var resourceYAMLBaseline: String = ""
    @Published public private(set) var resourceYAMLUndoSnapshot: String?
    private var resourceYAMLUndoStack: [String] = []
    @Published public private(set) var resourceYAMLValidationIssues: [YAMLValidationIssue] = []
    @Published public private(set) var isValidatingResourceYAML = false
    /// Read-only describe output Rune fetched for the selected resource (not user-editable).
    @Published public private(set) var resourceDescribe: String = ""
    @Published public private(set) var lastResourceYAMLError: String?
    @Published public private(set) var lastResourceDescribeError: String?
    @Published public private(set) var lastResourceDetailsUpdatedAt: Date?
    @Published public private(set) var resourceDetailScope: ResourceDetailScope?
    @Published public private(set) var deploymentRolloutHistory: String = ""
    @Published public private(set) var helmValues: String = ""
    @Published public private(set) var helmManifest: String = ""
    @Published public private(set) var helmHistory: [HelmReleaseRevision] = []
    @Published public private(set) var lastExecResult: PodExecResult?
    @Published public private(set) var writeAuditLog: [WriteAuditEntry] = []
    @Published public private(set) var terminalSession: PodTerminalSession?
    @Published public private(set) var terminalSessions: [PodTerminalSession] = []
    @Published public private(set) var activeTerminalSessionID: String?
    @Published public private(set) var portForwardSessions: [PortForwardSession] = []

    @Published public var contextSearchQuery: String = ""
    @Published public var resourceSearchQuery: String = ""
    @Published public var isHelmAllNamespaces: Bool = true

    @Published public var isCommandPalettePresented: Bool = false
    @Published public var isLoading: Bool = false
    @Published public var isLoadingLogs: Bool = false
    @Published public var isLoadingResourceDetails: Bool = false
    @Published public var isReadOnlyMode: Bool = false
    @Published public var isExecutingCommand: Bool = false
    @Published public var isStartingPortForward: Bool = false
    @Published public var lastError: String?
    @Published public private(set) var activeNotice: RuneUserNotice?

    public init() {}

    public func setSources(_ sources: [KubeConfigSource]) {
        kubeConfigSources = sources
    }

    public func setFavoriteContextNames(_ names: Set<String>) {
        favoriteContextNames = names
    }

    public func setFavoriteResourceIDs(_ ids: Set<String>) {
        favoriteResourceIDs = ids
    }

    public func setFavoriteNamespaceIDs(_ ids: Set<String>) {
        favoriteNamespaceIDs = ids
    }

    public func setManualProductionContextIDs(_ ids: Set<String>) {
        manualProductionContextIDs = ids
    }

    public func setAuthDoctorChecks(_ checks: [RuneHealthCheck]) {
        authDoctorChecks = checks
    }

    public func clearAuthDoctorChecks() {
        authDoctorChecks = []
    }

    public func setAuthDoctorRunning(_ running: Bool) {
        isRunningAuthDoctor = running
    }

    public func setSnapshotFreshness(_ freshness: RuneSnapshotFreshness) {
        snapshotFreshness = freshness
    }

    public func freshness(for family: RuneResourceListFamily) -> RuneResourceListFreshness {
        resourceListFreshness[family] ?? RuneResourceListFreshness()
    }

    public func markResourceListsRefreshing(
        _ families: some Sequence<RuneResourceListFamily>,
        message: String = "Refreshing"
    ) {
        for family in families {
            let current = freshness(for: family)
            resourceListFreshness[family] = RuneResourceListFreshness(
                status: .refreshing,
                updatedAt: current.updatedAt,
                message: message
            )
        }
    }

    public func markResourceListsLive(
        _ families: some Sequence<RuneResourceListFamily>,
        updatedAt: Date = Date(),
        message: String = "Live"
    ) {
        for family in families {
            resourceListFreshness[family] = RuneResourceListFreshness(
                status: .live,
                updatedAt: updatedAt,
                message: message
            )
        }
    }

    public func markResourceListsReconnecting(
        _ families: some Sequence<RuneResourceListFamily>,
        message: String = "Reconnecting"
    ) {
        for family in families {
            let current = freshness(for: family)
            resourceListFreshness[family] = RuneResourceListFreshness(
                status: .reconnecting,
                updatedAt: current.updatedAt,
                message: message
            )
        }
    }

    public func markResourceListsFailed(
        _ families: some Sequence<RuneResourceListFamily>,
        message: String
    ) {
        for family in families {
            let current = freshness(for: family)
            resourceListFreshness[family] = RuneResourceListFreshness(
                status: current.updatedAt == nil ? .failed : .stale,
                updatedAt: current.updatedAt,
                message: message
            )
        }
    }

    public func clearResourceListFreshness() {
        resourceListFreshness = [:]
    }

    public func setManualNamespaceMode(_ enabled: Bool, warning: String? = nil) {
        isManualNamespaceMode = enabled
        namespaceAccessWarning = warning
    }

    public func clearManualNamespaceMode() {
        isManualNamespaceMode = false
        namespaceAccessWarning = nil
    }

    public func toggleFavoriteContext(named contextName: String) {
        if favoriteContextNames.contains(contextName) {
            favoriteContextNames.remove(contextName)
        } else {
            favoriteContextNames.insert(contextName)
        }
    }

    public func isFavorite(_ context: KubeContext) -> Bool {
        favoriteContextNames.contains(context.name)
    }

    public func toggleManualProductionContext(id: String) {
        if manualProductionContextIDs.contains(id) {
            manualProductionContextIDs.remove(id)
        } else {
            manualProductionContextIDs.insert(id)
        }
    }

    public func isManualProductionContext(id: String) -> Bool {
        manualProductionContextIDs.contains(id)
    }

    public func toggleFavoriteResource(id: String) {
        if favoriteResourceIDs.contains(id) {
            favoriteResourceIDs.remove(id)
        } else {
            favoriteResourceIDs.insert(id)
        }
    }

    public func isFavoriteResource(id: String) -> Bool {
        favoriteResourceIDs.contains(id)
    }

    public func toggleFavoriteNamespace(id: String) {
        if favoriteNamespaceIDs.contains(id) {
            favoriteNamespaceIDs.remove(id)
        } else {
            favoriteNamespaceIDs.insert(id)
        }
    }

    public func isFavoriteNamespace(id: String) -> Bool {
        favoriteNamespaceIDs.contains(id)
    }

    public func setContexts(_ contexts: [KubeContext]) {
        self.contexts = contexts
        if selectedContext == nil || !contexts.contains(selectedContext!) {
            selectedContext = contexts.first
        }
    }

    public func setNamespaces(_ namespaces: [String]) {
        let normalized = Array(
            Set(
                namespaces.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                    .filter { !$0.isEmpty }
            )
        ).sorted()

        self.namespaces = normalized
    }

    public func setPods(_ pods: [PodSummary]) {
        self.pods = pods
        selectedPodIDs.formIntersection(Set(pods.map(\.id)))
        if let current = selectedPod,
           let match = pods.first(where: { $0.id == current.id }) {
            selectedPod = match
            return
        }
        selectedPod = pods.first
    }

    public func setDeployments(_ deployments: [DeploymentSummary]) {
        self.deployments = deployments
        if let selectedDeployment, deployments.contains(selectedDeployment) {
            return
        }
        selectedDeployment = deployments.first
    }

    public func setServices(_ services: [ServiceSummary]) {
        self.services = services
        if let selectedService, services.contains(selectedService) {
            return
        }
        selectedService = services.first
    }

    public func setEvents(_ events: [EventSummary]) {
        self.events = events
        if let selectedEvent, events.contains(selectedEvent) {
            return
        }
        selectedEvent = events.first
    }

    public func setHelmReleases(_ releases: [HelmReleaseSummary]) {
        helmReleases = releases
        if let selectedHelmRelease, releases.contains(selectedHelmRelease) { return }
        selectedHelmRelease = releases.first
    }

    public func setOperatorResources(_ resources: [OperatorResourceSummary]) {
        operatorResources = resources
        if let selectedOperatorResource, resources.contains(selectedOperatorResource) { return }
        selectedOperatorResource = nil
    }

    public func setStatefulSets(_ resources: [ClusterResourceSummary]) {
        statefulSets = resources
        if let selectedStatefulSet, resources.contains(selectedStatefulSet) { return }
        selectedStatefulSet = resources.first
    }

    public func setDaemonSets(_ resources: [ClusterResourceSummary]) {
        daemonSets = resources
        if let selectedDaemonSet, resources.contains(selectedDaemonSet) { return }
        selectedDaemonSet = resources.first
    }

    public func setJobs(_ resources: [ClusterResourceSummary]) {
        jobs = resources
        if let selectedJob, resources.contains(selectedJob) { return }
        selectedJob = resources.first
    }

    public func setCronJobs(_ resources: [ClusterResourceSummary]) {
        cronJobs = resources
        if let selectedCronJob, resources.contains(selectedCronJob) { return }
        selectedCronJob = resources.first
    }

    public func setReplicaSets(_ resources: [ClusterResourceSummary]) {
        replicaSets = resources
        if let selectedReplicaSet, resources.contains(selectedReplicaSet) { return }
        selectedReplicaSet = resources.first
    }

    public func setPersistentVolumeClaims(_ resources: [ClusterResourceSummary]) {
        persistentVolumeClaims = resources
        if let selectedPersistentVolumeClaim, resources.contains(selectedPersistentVolumeClaim) { return }
        selectedPersistentVolumeClaim = resources.first
    }

    public func setPersistentVolumes(_ resources: [ClusterResourceSummary]) {
        persistentVolumes = resources
        if let selectedPersistentVolume, resources.contains(selectedPersistentVolume) { return }
        selectedPersistentVolume = resources.first
    }

    public func setStorageClasses(_ resources: [ClusterResourceSummary]) {
        storageClasses = resources
        if let selectedStorageClass, resources.contains(selectedStorageClass) { return }
        selectedStorageClass = resources.first
    }

    public func setHorizontalPodAutoscalers(_ resources: [ClusterResourceSummary]) {
        horizontalPodAutoscalers = resources
        if let selectedHorizontalPodAutoscaler, resources.contains(selectedHorizontalPodAutoscaler) { return }
        selectedHorizontalPodAutoscaler = resources.first
    }

    public func setNetworkPolicies(_ resources: [ClusterResourceSummary]) {
        networkPolicies = resources
        if let selectedNetworkPolicy, resources.contains(selectedNetworkPolicy) { return }
        selectedNetworkPolicy = resources.first
    }

    public func setIngresses(_ resources: [ClusterResourceSummary]) {
        ingresses = resources
        if let selectedIngress, resources.contains(selectedIngress) { return }
        selectedIngress = resources.first
    }

    public func setConfigMaps(_ resources: [ClusterResourceSummary]) {
        configMaps = resources
        if let selectedConfigMap, resources.contains(selectedConfigMap) { return }
        selectedConfigMap = resources.first
    }

    public func setSecrets(_ resources: [ClusterResourceSummary]) {
        secrets = resources
        if let selectedSecret, resources.contains(selectedSecret) { return }
        selectedSecret = resources.first
    }

    public func setNodes(_ resources: [ClusterResourceSummary]) {
        nodes = resources
        if let selectedNode, resources.contains(selectedNode) { return }
        selectedNode = resources.first
    }

    public func setRBACData(
        roles: [ClusterResourceSummary],
        roleBindings: [ClusterResourceSummary],
        clusterRoles: [ClusterResourceSummary],
        clusterRoleBindings: [ClusterResourceSummary]
    ) {
        rbacRoles = roles
        rbacRoleBindings = roleBindings
        rbacClusterRoles = clusterRoles
        rbacClusterRoleBindings = clusterRoleBindings
        reconcileRBACSelection()
    }

    public func setSelectedRBACResource(_ resource: ClusterResourceSummary?) {
        selectedRBACResource = resource
    }

    public func reconcileRBACSelection() {
        let listForKind: [ClusterResourceSummary] = {
            switch selectedWorkloadKind {
            case .role: return rbacRoles
            case .roleBinding: return rbacRoleBindings
            case .clusterRole: return rbacClusterRoles
            case .clusterRoleBinding: return rbacClusterRoleBindings
            default: return []
            }
        }()

        guard !listForKind.isEmpty else {
            selectedRBACResource = nil
            return
        }

        if let current = selectedRBACResource,
           current.kind == selectedWorkloadKind,
           let match = listForKind.first(where: { $0.id == current.id }) {
            selectedRBACResource = match
            return
        }

        selectedRBACResource = listForKind.first
    }

    public func setOverviewSnapshot(
        pods: [PodSummary],
        deploymentsCount: Int,
        servicesCount: Int,
        ingressesCount: Int,
        configMapsCount: Int,
        cronJobsCount: Int,
        nodesCount: Int,
        clusterCPUPercent: Int? = nil,
        clusterMemoryPercent: Int? = nil,
        events: [EventSummary]
    ) {
        overviewPods = pods
        overviewDeploymentsCount = deploymentsCount
        overviewServicesCount = servicesCount
        overviewIngressesCount = ingressesCount
        overviewConfigMapsCount = configMapsCount
        overviewCronJobsCount = cronJobsCount
        overviewNodesCount = nodesCount
        overviewClusterCPUPercent = clusterCPUPercent
        overviewClusterMemoryPercent = clusterMemoryPercent
        overviewEvents = events
    }

    public func setOverviewClusterUsage(cpuPercent: Int?, memoryPercent: Int?) {
        overviewClusterCPUPercent = cpuPercent
        overviewClusterMemoryPercent = memoryPercent
    }

    public func setSelectedPod(_ pod: PodSummary?) {
        selectedPod = pod
    }

    public func setSelectedPodIDs(_ ids: Set<String>) {
        let validIDs = Set(pods.map(\.id))
        selectedPodIDs = ids.intersection(validIDs)
    }

    public func toggleSelectedPodID(_ id: String) {
        guard pods.contains(where: { $0.id == id }) else { return }
        if selectedPodIDs.contains(id) {
            selectedPodIDs.remove(id)
        } else {
            selectedPodIDs.insert(id)
        }
    }

    public func clearSelectedPodIDs() {
        selectedPodIDs = []
    }

    public func setSelectedGenericResourceIDs(_ ids: Set<String>, validIDs: Set<String>) {
        selectedGenericResourceIDs = ids.intersection(validIDs)
    }

    public func toggleSelectedGenericResourceID(_ id: String, validIDs: Set<String>) {
        guard validIDs.contains(id) else { return }
        if selectedGenericResourceIDs.contains(id) {
            selectedGenericResourceIDs.remove(id)
        } else {
            selectedGenericResourceIDs.insert(id)
        }
    }

    public func clearSelectedGenericResourceIDs() {
        selectedGenericResourceIDs = []
    }

    public func setSelectedDeployment(_ deployment: DeploymentSummary?) {
        selectedDeployment = deployment
    }

    public func setSelectedService(_ service: ServiceSummary?) {
        selectedService = service
    }

    public func setSelectedEvent(_ event: EventSummary?) {
        selectedEvent = event
    }

    public func setSelectedStatefulSet(_ resource: ClusterResourceSummary?) {
        selectedStatefulSet = resource
    }

    public func setSelectedDaemonSet(_ resource: ClusterResourceSummary?) {
        selectedDaemonSet = resource
    }

    public func setSelectedJob(_ resource: ClusterResourceSummary?) {
        selectedJob = resource
    }

    public func setSelectedCronJob(_ resource: ClusterResourceSummary?) {
        selectedCronJob = resource
    }

    public func setSelectedReplicaSet(_ resource: ClusterResourceSummary?) {
        selectedReplicaSet = resource
    }

    public func setSelectedPersistentVolumeClaim(_ resource: ClusterResourceSummary?) {
        selectedPersistentVolumeClaim = resource
    }

    public func setSelectedPersistentVolume(_ resource: ClusterResourceSummary?) {
        selectedPersistentVolume = resource
    }

    public func setSelectedStorageClass(_ resource: ClusterResourceSummary?) {
        selectedStorageClass = resource
    }

    public func setSelectedHorizontalPodAutoscaler(_ resource: ClusterResourceSummary?) {
        selectedHorizontalPodAutoscaler = resource
    }

    public func setSelectedNetworkPolicy(_ resource: ClusterResourceSummary?) {
        selectedNetworkPolicy = resource
    }

    public func setSelectedIngress(_ resource: ClusterResourceSummary?) {
        selectedIngress = resource
    }

    public func setSelectedConfigMap(_ resource: ClusterResourceSummary?) {
        selectedConfigMap = resource
    }

    public func setSelectedSecret(_ resource: ClusterResourceSummary?) {
        selectedSecret = resource
    }

    public func setSelectedNode(_ resource: ClusterResourceSummary?) {
        selectedNode = resource
    }

    public func setPodLogs(_ logs: String) {
        podLogs = logs
        lastLogFetchError = nil
        lastLogUpdatedAt = Date()
    }

    public func showCachedPodLogs(contextName: String, namespace: String, podName: String) {
        podLogs = cachedLogs(contextName: contextName, namespace: namespace, kind: .pod, resourceName: podName)
    }

    public func appendPodLogRead(
        _ logs: String,
        contextName: String,
        namespace: String,
        podName: String,
        loadedAt: Date = Date()
    ) {
        podLogs = appendSessionLogSegment(
            logs,
            contextName: contextName,
            namespace: namespace,
            kind: .pod,
            resourceName: podName,
            sourceNames: [podName],
            loadedAt: loadedAt
        )
        lastLogFetchError = nil
        lastLogUpdatedAt = loadedAt
    }

    /// Replaces the cached pod log session for this resource (no merge with prior fetches). Used when tail mode
    /// is off so changing the time window / line preset does not concatenate snapshots into one giant buffer.
    public func replacePodLogRead(
        _ logs: String,
        contextName: String,
        namespace: String,
        podName: String,
        loadedAt: Date = Date()
    ) {
        let key = logCacheKey(contextName: contextName, namespace: namespace, kind: .pod, resourceName: podName)
        let segment = formattedLogSegment(
            logs,
            contextName: contextName,
            namespace: namespace,
            kind: .pod,
            resourceName: podName,
            sourceNames: [podName],
            loadedAt: loadedAt
        )
        let bounded = boundedSessionLogCache(segment)
        updateSessionLogCache(key: key, value: bounded)
        podLogs = bounded
        lastLogFetchError = nil
        lastLogUpdatedAt = loadedAt
    }

    public func setUnifiedServiceLogs(_ logs: String, pods: [String]) {
        unifiedServiceLogs = logs
        unifiedServiceLogPods = pods
        lastLogFetchError = nil
        lastLogUpdatedAt = Date()
    }

    public func showCachedUnifiedLogs(contextName: String, namespace: String, kind: KubeResourceKind, resourceName: String) {
        unifiedServiceLogs = cachedLogs(contextName: contextName, namespace: namespace, kind: kind, resourceName: resourceName)
        unifiedServiceLogPods = []
    }

    public func appendUnifiedServiceLogRead(
        _ logs: String,
        pods: [String],
        contextName: String,
        namespace: String,
        kind: KubeResourceKind,
        resourceName: String,
        loadedAt: Date = Date()
    ) {
        unifiedServiceLogs = appendSessionLogSegment(
            logs,
            contextName: contextName,
            namespace: namespace,
            kind: kind,
            resourceName: resourceName,
            sourceNames: pods,
            loadedAt: loadedAt
        )
        unifiedServiceLogPods = pods
        lastLogFetchError = nil
        lastLogUpdatedAt = loadedAt
    }

    /// Same as `replacePodLogRead` but for unified service/deployment log streams.
    public func replaceUnifiedServiceLogRead(
        _ logs: String,
        pods: [String],
        contextName: String,
        namespace: String,
        kind: KubeResourceKind,
        resourceName: String,
        loadedAt: Date = Date()
    ) {
        let key = logCacheKey(contextName: contextName, namespace: namespace, kind: kind, resourceName: resourceName)
        let segment = formattedLogSegment(
            logs,
            contextName: contextName,
            namespace: namespace,
            kind: kind,
            resourceName: resourceName,
            sourceNames: pods,
            loadedAt: loadedAt
        )
        let bounded = boundedSessionLogCache(segment)
        updateSessionLogCache(key: key, value: bounded)
        unifiedServiceLogs = bounded
        unifiedServiceLogPods = pods
        lastLogFetchError = nil
        lastLogUpdatedAt = loadedAt
    }

    public func clearUnifiedServiceLogs() {
        unifiedServiceLogs = ""
        unifiedServiceLogPods = []
        lastLogFetchError = nil
    }

    public func setLastLogFetchError(_ message: String?) {
        lastLogFetchError = message
    }

    public func cachedLogs(contextName: String, namespace: String, kind: KubeResourceKind, resourceName: String) -> String {
        let key = logCacheKey(contextName: contextName, namespace: namespace, kind: kind, resourceName: resourceName)
        guard let logs = sessionLogCache[key] else { return "" }
        touchSessionLogCacheKey(key)
        pruneSessionLogCacheEntriesIfNeeded()
        return logs
    }

    public func setResourceYAML(_ yaml: String) {
        resourceYAML = yaml
        resourceYAMLBaseline = yaml
        clearResourceYAMLUndoHistory()
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
        lastResourceYAMLError = nil
        lastResourceDetailsUpdatedAt = Date()
    }

    /// Updates the in-memory YAML (user edits or import). Does not change the cluster baseline until the next fetch or successful apply + reload.
    public func updateResourceYAMLDraft(_ yaml: String) {
        pushResourceYAMLUndoSnapshotIfNeeded(for: yaml)
        resourceYAML = yaml
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
    }

    /// Discards local edits and restores the last loaded cluster YAML.
    public func revertResourceYAMLToClusterSnapshot() {
        pushResourceYAMLUndoSnapshotIfNeeded(for: resourceYAMLBaseline)
        resourceYAML = resourceYAMLBaseline
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
    }

    public var canUndoResourceYAMLEdit: Bool {
        !resourceYAMLUndoStack.isEmpty
    }

    public func undoResourceYAMLEdit() {
        guard let previous = resourceYAMLUndoStack.popLast() else { return }
        resourceYAMLUndoSnapshot = resourceYAMLUndoStack.last
        resourceYAML = previous
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
    }

    private func pushResourceYAMLUndoSnapshotIfNeeded(for nextYAML: String) {
        guard nextYAML != resourceYAML else { return }
        resourceYAMLUndoStack.append(resourceYAML)
        if resourceYAMLUndoStack.count > maxResourceYAMLUndoSnapshots {
            resourceYAMLUndoStack.removeFirst(resourceYAMLUndoStack.count - maxResourceYAMLUndoSnapshots)
        }
        resourceYAMLUndoSnapshot = resourceYAMLUndoStack.last
    }

    private func clearResourceYAMLUndoHistory() {
        resourceYAMLUndoStack = []
        resourceYAMLUndoSnapshot = nil
    }

    public func beginResourceYAMLValidation() {
        isValidatingResourceYAML = true
    }

    public func setResourceYAMLValidationIssues(_ issues: [YAMLValidationIssue]) {
        resourceYAMLValidationIssues = issues
    }

    public func finishResourceYAMLValidation() {
        isValidatingResourceYAML = false
    }

    public var resourceYAMLHasUnsavedEdits: Bool {
        resourceYAML != resourceYAMLBaseline
    }

    public func setResourceDescribe(_ text: String) {
        resourceDescribe = text
        lastResourceDescribeError = nil
        lastResourceDetailsUpdatedAt = Date()
    }

    public func beginResourceDetailLoad(scope: ResourceDetailScope? = nil) {
        resourceYAML = ""
        resourceYAMLBaseline = ""
        clearResourceYAMLUndoHistory()
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
        resourceDescribe = ""
        lastResourceYAMLError = nil
        lastResourceDescribeError = nil
        lastResourceDetailsUpdatedAt = nil
        resourceDetailScope = scope
        deploymentRolloutHistory = ""
        isLoadingResourceDetails = true
    }

    public func finishResourceDetailLoad() {
        isLoadingResourceDetails = false
    }

    public func setResourceYAMLError(_ message: String?) {
        resourceYAML = ""
        resourceYAMLBaseline = ""
        clearResourceYAMLUndoHistory()
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
        if message == nil {
            resourceDetailScope = nil
        }
        lastResourceYAMLError = message
    }

    public func setResourceDescribeError(_ message: String?) {
        resourceDescribe = ""
        lastResourceDescribeError = message
    }

    public func setDeploymentRolloutHistory(_ history: String) {
        deploymentRolloutHistory = history
    }

    public func setLastExecResult(_ result: PodExecResult?) {
        lastExecResult = result
    }

    public func appendWriteAuditEntry(_ entry: WriteAuditEntry) {
        writeAuditLog.insert(entry, at: 0)
        if writeAuditLog.count > 200 {
            writeAuditLog.removeLast(writeAuditLog.count - 200)
        }
    }

    public func setTerminalSession(_ session: PodTerminalSession?) {
        guard let session else {
            if let activeTerminalSessionID {
                terminalSessions.removeAll { $0.id == activeTerminalSessionID }
            }
            activeTerminalSessionID = terminalSessions.first?.id
            terminalSession = terminalSessions.first
            return
        }

        if let index = terminalSessions.firstIndex(where: { $0.id == session.id }) {
            terminalSessions[index] = session
        } else {
            terminalSessions.append(session)
        }
        activeTerminalSessionID = session.id
        terminalSession = session
    }

    public func selectTerminalSession(id: String) {
        guard let session = terminalSessions.first(where: { $0.id == id }) else { return }
        activeTerminalSessionID = id
        terminalSession = session
    }

    public func appendTerminalSessionOutput(id: String, text: String) {
        guard let index = terminalSessions.firstIndex(where: { $0.id == id }), !text.isEmpty else { return }
        var session = terminalSessions[index]
        let transcript = TerminalScrollbackRetention.retainingRecentLines(
            session.transcript + text,
            maxLines: UserDefaults.standard.runeTerminalScrollbackLineLimit
        )
        session = PodTerminalSession(
            id: session.id,
            contextName: session.contextName,
            namespace: session.namespace,
            podName: session.podName,
            containerName: session.containerName,
            shell: session.shell,
            transcript: transcript,
            status: session.status,
            lastExitCode: session.lastExitCode,
            lastDiagnostic: session.lastDiagnostic
        )
        terminalSessions[index] = session
        if activeTerminalSessionID == id {
            terminalSession = session
        }
    }

    public func appendTerminalSessionCommandEcho(id: String, command: String) {
        let rendered = "$ \(command)\n"
        appendTerminalSessionOutput(id: id, text: rendered)
    }

    public func updateTerminalSessionStatus(
        id: String,
        status: PodTerminalSessionStatus,
        exitCode: Int32? = nil,
        diagnostic: PodTerminalSessionDiagnostic? = nil
    ) {
        guard let index = terminalSessions.firstIndex(where: { $0.id == id }) else { return }
        var session = terminalSessions[index]
        let lastDiagnostic = diagnostic ?? (status == .failed ? session.lastDiagnostic : nil)
        session = PodTerminalSession(
            id: session.id,
            contextName: session.contextName,
            namespace: session.namespace,
            podName: session.podName,
            containerName: session.containerName,
            shell: session.shell,
            transcript: session.transcript,
            status: status,
            lastExitCode: exitCode ?? session.lastExitCode,
            lastDiagnostic: lastDiagnostic
        )
        terminalSessions[index] = session
        if activeTerminalSessionID == id {
            terminalSession = session
        }
    }

    public func clearTerminalSessionTranscript() {
        guard let id = activeTerminalSessionID,
              let index = terminalSessions.firstIndex(where: { $0.id == id })
        else { return }
        var session = terminalSessions[index]
        session = PodTerminalSession(
            id: session.id,
            contextName: session.contextName,
            namespace: session.namespace,
            podName: session.podName,
            containerName: session.containerName,
            shell: session.shell,
            transcript: "",
            status: session.status,
            lastExitCode: session.lastExitCode,
            lastDiagnostic: session.lastDiagnostic
        )
        terminalSessions[index] = session
        terminalSession = session
    }

    public func setPortForwardSessions(_ sessions: [PortForwardSession]) {
        portForwardSessions = sessions
    }

    public func upsertPortForwardSession(_ session: PortForwardSession) {
        if let index = portForwardSessions.firstIndex(where: { $0.id == session.id }) {
            portForwardSessions[index] = session
        } else {
            portForwardSessions.insert(session, at: 0)
        }
    }

    public func removePortForwardSession(id: String) {
        portForwardSessions.removeAll { $0.id == id }
    }

    public func removeInactivePortForwardSessions() {
        portForwardSessions.removeAll { $0.isInactive }
    }

    public func removeInactivePortForwardSessions(targetKind: PortForwardTargetKind, targetName: String, namespace: String) {
        portForwardSessions.removeAll {
            $0.isInactive
                && $0.targetKind == targetKind
                && $0.targetName == targetName
                && $0.namespace == namespace
        }
    }

    public func setSelectedHelmRelease(_ release: HelmReleaseSummary?) {
        selectedHelmRelease = release
    }

    public func setSelectedOperatorResource(_ resource: OperatorResourceSummary?) {
        selectedOperatorResource = resource
    }

    public func setHelmValues(_ values: String) {
        helmValues = values
    }

    public func setHelmManifest(_ manifest: String) {
        helmManifest = manifest
    }

    public func setHelmHistory(_ history: [HelmReleaseRevision]) {
        helmHistory = history
    }

    public func clearResourceDetails() {
        podLogs = ""
        unifiedServiceLogs = ""
        unifiedServiceLogPods = []
        resourceYAML = ""
        resourceYAMLBaseline = ""
        clearResourceYAMLUndoHistory()
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
        resourceDescribe = ""
        lastResourceYAMLError = nil
        lastResourceDescribeError = nil
        deploymentRolloutHistory = ""
        helmValues = ""
        helmManifest = ""
        helmHistory = []
        isLoadingLogs = false
        isLoadingResourceDetails = false
        lastLogFetchError = nil
        lastLogUpdatedAt = nil
        lastResourceDetailsUpdatedAt = nil
        resourceDetailScope = nil
    }

    public func setError(_ error: Error) {
        if Self.isUserCancelled(error) {
            logNotice("suppressed cancelled action: \(error.localizedDescription)")
            clearError()
            return
        }

        let message = error.localizedDescription
        lastError = message
        activeNotice = RuneUserNotice(
            severity: Self.noticeSeverity(for: error),
            title: Self.noticeTitle(for: error),
            message: message
        )
        logNotice("notice \(activeNotice?.severity.rawValue ?? "unknown"): \(message)")
    }

    public func clearError() {
        lastError = nil
        activeNotice = nil
    }

    public func setErrorMessage(_ message: String?) {
        lastError = message
        activeNotice = message.map {
            RuneUserNotice(
                severity: .warning,
                title: "Cluster data needs attention",
                message: $0
            )
        }
        if let message {
            logNotice("notice warning: \(message)")
        }
    }

    private static func isUserCancelled(_ error: Error) -> Bool {
        if let runeError = error as? RuneError, runeError == .userCancelled {
            return true
        }
        return error is CancellationError
    }

    private static func noticeSeverity(for error: Error) -> RuneUserNoticeSeverity {
        guard let runeError = error as? RuneError else { return .error }
        switch runeError {
        case .readOnlyMode, .invalidInput, .missingKubeConfig:
            return .warning
        case .commandFailed, .parseError:
            return .error
        case .userCancelled:
            return .info
        }
    }

    private static func noticeTitle(for error: Error) -> String {
        guard let runeError = error as? RuneError else { return "Action failed" }
        switch runeError {
        case .missingKubeConfig:
            return "Kubeconfig needed"
        case .commandFailed:
            return "Kubernetes command failed"
        case .parseError:
            return "Could not read Kubernetes data"
        case .readOnlyMode:
            return "Read-only mode"
        case .invalidInput:
            return "Check the action"
        case .userCancelled:
            return "Action cancelled"
        }
    }

    private func logNotice(_ message: String) {
        guard UserDefaults.standard.runeDiagnosticsLogging else { return }
        RuneLoggers.diagnostics.notice("\(message, privacy: .private)")
    }

    private func appendSessionLogSegment(
        _ logs: String,
        contextName: String,
        namespace: String,
        kind: KubeResourceKind,
        resourceName: String,
        sourceNames: [String],
        loadedAt: Date
    ) -> String {
        let key = logCacheKey(contextName: contextName, namespace: namespace, kind: kind, resourceName: resourceName)
        let previous = sessionLogCache[key]?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let segment = formattedLogSegment(
            logs,
            contextName: contextName,
            namespace: namespace,
            kind: kind,
            resourceName: resourceName,
            sourceNames: sourceNames,
            loadedAt: loadedAt
        )
        let combined = previous.isEmpty ? segment : "\(previous)\n\n\(segment)"
        let bounded = boundedSessionLogCache(combined)
        updateSessionLogCache(key: key, value: bounded)
        return bounded
    }

    private func formattedLogSegment(
        _ logs: String,
        contextName: String,
        namespace: String,
        kind: KubeResourceKind,
        resourceName: String,
        sourceNames: [String],
        loadedAt: Date
    ) -> String {
        let timestamp = ISO8601DateFormatter().string(from: loadedAt)
        let sourceSummary = sourceNames.isEmpty ? "" : "\nSources: \(sourceNames.joined(separator: ", "))"
        let body = logs.trimmingCharacters(in: .newlines)
        return """
        ────────────────────────────────────────────────────────────
        \(timestamp)  \(kind.singularTypeName)  \(namespace)/\(resourceName)
        Context: \(contextName)\(sourceSummary)
        ────────────────────────────────────────────────────────────
        \(body.isEmpty ? "[no log lines returned]" : body)
        """
    }

    private func boundedSessionLogCache(_ text: String) -> String {
        guard text.count > maxSessionLogCacheCharacters else { return text }
        let suffix = text.suffix(maxSessionLogCacheCharacters)
        return "[older session log cache truncated]\n\(suffix)"
    }

    private func updateSessionLogCache(key: String, value: String) {
        sessionLogCache[key] = value
        touchSessionLogCacheKey(key)
        pruneSessionLogCacheEntriesIfNeeded()
    }

    private func touchSessionLogCacheKey(_ key: String) {
        sessionLogCacheKeysMostRecent.removeAll { $0 == key }
        sessionLogCacheKeysMostRecent.insert(key, at: 0)
    }

    private func pruneSessionLogCacheEntriesIfNeeded() {
        guard sessionLogCacheKeysMostRecent.count > maxSessionLogCacheEntries else { return }
        let staleKeys = sessionLogCacheKeysMostRecent.dropFirst(maxSessionLogCacheEntries)
        for key in staleKeys {
            sessionLogCache.removeValue(forKey: key)
        }
        sessionLogCacheKeysMostRecent = Array(sessionLogCacheKeysMostRecent.prefix(maxSessionLogCacheEntries))
    }

    private func logCacheKey(contextName: String, namespace: String, kind: KubeResourceKind, resourceName: String) -> String {
        "\(contextName)|\(namespace)|\(kind.rawValue)|\(resourceName)"
    }
}
