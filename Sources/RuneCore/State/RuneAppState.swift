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

public enum RuneResourceSelectionChannel: Hashable, Sendable {
    case resource(KubeResourceKind)
    case helmRelease
    case operatorResource
}

public struct ResourceYAMLEditorSelection: Equatable, Sendable {
    public let location: Int
    public let length: Int

    public init(location: Int, length: Int) {
        self.location = location
        self.length = length
    }
}

public struct ResourceYAMLEditorPresentation: Equatable, Sendable {
    public let selections: [ResourceYAMLEditorSelection]
    public let viewportX: Double
    public let viewportY: Double
    public let hadKeyboardFocus: Bool

    public init(
        selections: [ResourceYAMLEditorSelection],
        viewportX: Double,
        viewportY: Double,
        hadKeyboardFocus: Bool
    ) {
        self.selections = selections
        self.viewportX = viewportX
        self.viewportY = viewportY
        self.hadKeyboardFocus = hadKeyboardFocus
    }
}

public struct ResourceYAMLEditorRestorationRequest: Equatable, Sendable {
    public let sequence: UInt64
    public let yaml: String
    public let presentation: ResourceYAMLEditorPresentation

    public init(
        sequence: UInt64,
        yaml: String,
        presentation: ResourceYAMLEditorPresentation
    ) {
        self.sequence = sequence
        self.yaml = yaml
        self.presentation = presentation
    }
}

@MainActor
public final class RuneAppState: ObservableObject {
    private struct ResourceYAMLUndoEntry {
        let yaml: String
        let editorPresentation: ResourceYAMLEditorPresentation?
    }

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

    @Published public var selectedPod: PodSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.pod),
                from: oldValue?.id,
                to: selectedPod?.id
            )
        }
    }
    @Published public private(set) var selectedPodIDs: Set<String> = []
    @Published public private(set) var selectedGenericResourceIDs: Set<String> = []
    @Published public var selectedDeployment: DeploymentSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.deployment),
                from: oldValue?.id,
                to: selectedDeployment?.id
            )
        }
    }
    @Published public var selectedService: ServiceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.service),
                from: oldValue?.id,
                to: selectedService?.id
            )
        }
    }
    @Published public var selectedEvent: EventSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.event),
                from: oldValue?.id,
                to: selectedEvent?.id
            )
        }
    }
    @Published public var selectedStatefulSet: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.statefulSet),
                from: oldValue?.id,
                to: selectedStatefulSet?.id
            )
        }
    }
    @Published public var selectedDaemonSet: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.daemonSet),
                from: oldValue?.id,
                to: selectedDaemonSet?.id
            )
        }
    }
    @Published public var selectedJob: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.job),
                from: oldValue?.id,
                to: selectedJob?.id
            )
        }
    }
    @Published public var selectedCronJob: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.cronJob),
                from: oldValue?.id,
                to: selectedCronJob?.id
            )
        }
    }
    @Published public var selectedReplicaSet: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.replicaSet),
                from: oldValue?.id,
                to: selectedReplicaSet?.id
            )
        }
    }
    @Published public var selectedPersistentVolumeClaim: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.persistentVolumeClaim),
                from: oldValue?.id,
                to: selectedPersistentVolumeClaim?.id
            )
        }
    }
    @Published public var selectedPersistentVolume: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.persistentVolume),
                from: oldValue?.id,
                to: selectedPersistentVolume?.id
            )
        }
    }
    @Published public var selectedStorageClass: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.storageClass),
                from: oldValue?.id,
                to: selectedStorageClass?.id
            )
        }
    }
    @Published public var selectedHorizontalPodAutoscaler: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.horizontalPodAutoscaler),
                from: oldValue?.id,
                to: selectedHorizontalPodAutoscaler?.id
            )
        }
    }
    @Published public var selectedNetworkPolicy: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.networkPolicy),
                from: oldValue?.id,
                to: selectedNetworkPolicy?.id
            )
        }
    }
    @Published public var selectedEndpoint: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.endpoint),
                from: oldValue?.id,
                to: selectedEndpoint?.id
            )
        }
    }
    @Published public var selectedIngress: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.ingress),
                from: oldValue?.id,
                to: selectedIngress?.id
            )
        }
    }
    @Published public var selectedConfigMap: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.configMap),
                from: oldValue?.id,
                to: selectedConfigMap?.id
            )
        }
    }
    @Published public var selectedSecret: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.secret),
                from: oldValue?.id,
                to: selectedSecret?.id
            )
        }
    }
    @Published public var selectedNode: ClusterResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .resource(.node),
                from: oldValue?.id,
                to: selectedNode?.id
            )
        }
    }
    @Published public var selectedHelmRelease: HelmReleaseSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .helmRelease,
                from: oldValue?.id,
                to: selectedHelmRelease?.id
            )
        }
    }
    @Published public private(set) var selectedOperatorResource: OperatorResourceSummary? {
        didSet {
            advanceResourceSelectionRevision(
                for: .operatorResource,
                from: oldValue?.id,
                to: selectedOperatorResource?.id
            )
        }
    }
    private var resourceSelectionRevisions: [RuneResourceSelectionChannel: UInt64] = [:]

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
    @Published public private(set) var endpoints: [ClusterResourceSummary] = []
    @Published public private(set) var ingresses: [ClusterResourceSummary] = []
    @Published public private(set) var configMaps: [ClusterResourceSummary] = []
    @Published public private(set) var secrets: [ClusterResourceSummary] = []
    @Published public private(set) var nodes: [ClusterResourceSummary] = []
    @Published public private(set) var helmReleases: [HelmReleaseSummary] = []
    @Published public private(set) var operatorResources: [OperatorResourceSummary] = []
    @Published public private(set) var rbacRoles: [ClusterResourceSummary] = []
    @Published public private(set) var serviceAccounts: [ClusterResourceSummary] = []
    @Published public private(set) var rbacRoleBindings: [ClusterResourceSummary] = []
    @Published public private(set) var rbacClusterRoles: [ClusterResourceSummary] = []
    @Published public private(set) var rbacClusterRoleBindings: [ClusterResourceSummary] = []
    @Published public private(set) var selectedRBACResource: ClusterResourceSummary? {
        didSet { advanceRBACSelectionRevisions(from: oldValue, to: selectedRBACResource) }
    }
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
    /// Monotonic token advanced only by local draft mutations. Async readers use it
    /// to reject server responses that started before a newer user edit.
    public private(set) var resourceYAMLDraftRevision: UInt64 = 0
    @Published public private(set) var resourceYAMLUndoSnapshot: String?
    @Published public private(set) var resourceYAMLEditorRestorationRequest:
        ResourceYAMLEditorRestorationRequest?
    private var resourceYAMLUndoStack: [ResourceYAMLUndoEntry] = []
    private var resourceYAMLEditorRestorationSequence: UInt64 = 0
    @Published public private(set) var resourceYAMLValidationIssues: [YAMLValidationIssue] = []
    /// Advances whenever a validation producer publishes a result. Queued local
    /// validation uses this to avoid replacing a newer result for the same draft.
    public private(set) var resourceYAMLValidationGeneration: UInt64 = 0
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
        guard !resourceListFreshness.isEmpty else { return }
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

    private func assignIfChanged<Value: Equatable>(_ keyPath: ReferenceWritableKeyPath<RuneAppState, Value>, _ value: Value) {
        if self[keyPath: keyPath] != value {
            self[keyPath: keyPath] = value
        }
    }

    public func resourceSelectionRevision(for channel: RuneResourceSelectionChannel) -> UInt64 {
        resourceSelectionRevisions[channel, default: 0]
    }

    private func advanceResourceSelectionRevision(
        for channel: RuneResourceSelectionChannel,
        from previousID: String?,
        to currentID: String?
    ) {
        guard previousID != currentID else { return }
        resourceSelectionRevisions[channel, default: 0] &+= 1
    }

    private func advanceRBACSelectionRevisions(
        from previous: ClusterResourceSummary?,
        to current: ClusterResourceSummary?
    ) {
        if previous?.kind == current?.kind, let kind = current?.kind ?? previous?.kind {
            advanceResourceSelectionRevision(
                for: .resource(kind),
                from: previous?.id,
                to: current?.id
            )
            return
        }

        if let previous {
            advanceResourceSelectionRevision(
                for: .resource(previous.kind),
                from: previous.id,
                to: nil
            )
        }
        if let current {
            advanceResourceSelectionRevision(
                for: .resource(current.kind),
                from: nil,
                to: current.id
            )
        }
    }

    private func assignSelectionIfChanged<Value: Identifiable & Equatable>(
        _ keyPath: ReferenceWritableKeyPath<RuneAppState, Value?>,
        _ value: Value?
    ) where Value.ID == String {
        let previous = self[keyPath: keyPath]
        guard previous != value else { return }
        self[keyPath: keyPath] = value
    }

    private func reconcileSelection<Value: Identifiable & Equatable>(
        _ keyPath: ReferenceWritableKeyPath<RuneAppState, Value?>,
        in values: [Value],
        fallback: Value?
    ) where Value.ID == String {
        let selectedID = self[keyPath: keyPath]?.id
        let refreshedSelection = selectedID.flatMap { id in
            values.first(where: { $0.id == id })
        }
        assignSelectionIfChanged(keyPath, refreshedSelection ?? fallback)
    }

    private var genericResourceIDs: Set<String> {
        let resourceLists = [
            statefulSets,
            daemonSets,
            jobs,
            cronJobs,
            replicaSets,
            persistentVolumeClaims,
            persistentVolumes,
            storageClasses,
            horizontalPodAutoscalers,
            networkPolicies,
            endpoints,
            ingresses,
            configMaps,
            secrets,
            nodes,
            rbacRoles,
            serviceAccounts,
            rbacRoleBindings,
            rbacClusterRoles,
            rbacClusterRoleBindings
        ]
        return Set(resourceLists.joined().map(\.id))
    }

    private func reconcileSelectedGenericResourceIDs() {
        guard !selectedGenericResourceIDs.isEmpty else { return }
        assignIfChanged(
            \.selectedGenericResourceIDs,
            selectedGenericResourceIDs.intersection(genericResourceIDs)
        )
    }

    private func setGenericResources(
        _ resources: [ClusterResourceSummary],
        list listKeyPath: ReferenceWritableKeyPath<RuneAppState, [ClusterResourceSummary]>,
        selection selectionKeyPath: ReferenceWritableKeyPath<RuneAppState, ClusterResourceSummary?>
    ) {
        assignIfChanged(listKeyPath, resources)
        reconcileSelection(selectionKeyPath, in: resources, fallback: resources.first)
        reconcileSelectedGenericResourceIDs()
    }

    public func setPods(_ pods: [PodSummary]) {
        assignIfChanged(\.pods, pods)
        let validSelectedPodIDs = selectedPodIDs.intersection(Set(pods.map(\.id)))
        if selectedPodIDs != validSelectedPodIDs {
            selectedPodIDs = validSelectedPodIDs
        }
        if let current = selectedPod,
           let match = pods.first(where: { $0.id == current.id }) {
            assignSelectionIfChanged(\.selectedPod, match)
            return
        }
        assignSelectionIfChanged(\.selectedPod, pods.first)
    }

    public func setDeployments(_ deployments: [DeploymentSummary]) {
        assignIfChanged(\.deployments, deployments)
        reconcileSelection(\.selectedDeployment, in: deployments, fallback: deployments.first)
    }

    public func setServices(_ services: [ServiceSummary]) {
        assignIfChanged(\.services, services)
        reconcileSelection(\.selectedService, in: services, fallback: services.first)
    }

    public func setEvents(_ events: [EventSummary]) {
        assignIfChanged(\.events, events)
        reconcileSelection(\.selectedEvent, in: events, fallback: events.first)
    }

    public func setHelmReleases(
        _ releases: [HelmReleaseSummary],
        selectFallback: Bool = true
    ) {
        assignIfChanged(\.helmReleases, releases)
        let fallback = selectFallback && selectedOperatorResource == nil
            ? releases.first
            : nil
        reconcileSelection(\.selectedHelmRelease, in: releases, fallback: fallback)
        if selectedOperatorResource != nil {
            assignSelectionIfChanged(\.selectedHelmRelease, nil)
        }
    }

    public func setOperatorResources(
        _ resources: [OperatorResourceSummary],
        selectFallback: Bool = false
    ) {
        assignIfChanged(\.operatorResources, resources)
        let fallback = selectFallback && selectedHelmRelease == nil
            ? resources.first
            : nil
        reconcileSelection(\.selectedOperatorResource, in: resources, fallback: fallback)
        if selectedHelmRelease != nil {
            assignSelectionIfChanged(\.selectedOperatorResource, nil)
        }
    }

    public func setStatefulSets(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.statefulSets, selection: \.selectedStatefulSet)
    }

    public func setDaemonSets(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.daemonSets, selection: \.selectedDaemonSet)
    }

    public func setJobs(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.jobs, selection: \.selectedJob)
    }

    public func setCronJobs(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.cronJobs, selection: \.selectedCronJob)
    }

    public func setReplicaSets(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.replicaSets, selection: \.selectedReplicaSet)
    }

    public func setPersistentVolumeClaims(_ resources: [ClusterResourceSummary]) {
        setGenericResources(
            resources,
            list: \.persistentVolumeClaims,
            selection: \.selectedPersistentVolumeClaim
        )
    }

    public func setPersistentVolumes(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.persistentVolumes, selection: \.selectedPersistentVolume)
    }

    public func setStorageClasses(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.storageClasses, selection: \.selectedStorageClass)
    }

    public func setHorizontalPodAutoscalers(_ resources: [ClusterResourceSummary]) {
        setGenericResources(
            resources,
            list: \.horizontalPodAutoscalers,
            selection: \.selectedHorizontalPodAutoscaler
        )
    }

    public func setNetworkPolicies(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.networkPolicies, selection: \.selectedNetworkPolicy)
    }

    public func setEndpoints(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.endpoints, selection: \.selectedEndpoint)
    }

    public func setIngresses(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.ingresses, selection: \.selectedIngress)
    }

    public func setConfigMaps(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.configMaps, selection: \.selectedConfigMap)
    }

    public func setSecrets(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.secrets, selection: \.selectedSecret)
    }

    public func setNodes(_ resources: [ClusterResourceSummary]) {
        setGenericResources(resources, list: \.nodes, selection: \.selectedNode)
    }

    public func setRBACData(
        roles: [ClusterResourceSummary],
        serviceAccounts: [ClusterResourceSummary],
        roleBindings: [ClusterResourceSummary],
        clusterRoles: [ClusterResourceSummary],
        clusterRoleBindings: [ClusterResourceSummary]
    ) {
        assignIfChanged(\.rbacRoles, roles)
        assignIfChanged(\.serviceAccounts, serviceAccounts)
        assignIfChanged(\.rbacRoleBindings, roleBindings)
        assignIfChanged(\.rbacClusterRoles, clusterRoles)
        assignIfChanged(\.rbacClusterRoleBindings, clusterRoleBindings)
        reconcileRBACSelection()
        reconcileSelectedGenericResourceIDs()
    }

    public func setSelectedRBACResource(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedRBACResource, resource)
    }

    public func reconcileRBACSelection() {
        let listForKind: [ClusterResourceSummary] = {
            switch selectedWorkloadKind {
            case .role: return rbacRoles
            case .serviceAccount: return serviceAccounts
            case .roleBinding: return rbacRoleBindings
            case .clusterRole: return rbacClusterRoles
            case .clusterRoleBinding: return rbacClusterRoleBindings
            default: return []
            }
        }()

        guard !listForKind.isEmpty else {
            assignSelectionIfChanged(\.selectedRBACResource, nil)
            return
        }

        if let current = selectedRBACResource,
           current.kind == selectedWorkloadKind,
           let match = listForKind.first(where: { $0.id == current.id }) {
            assignSelectionIfChanged(\.selectedRBACResource, match)
            return
        }

        assignSelectionIfChanged(\.selectedRBACResource, listForKind.first)
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
        assignIfChanged(\.overviewPods, pods)
        assignIfChanged(\.overviewDeploymentsCount, deploymentsCount)
        assignIfChanged(\.overviewServicesCount, servicesCount)
        assignIfChanged(\.overviewIngressesCount, ingressesCount)
        assignIfChanged(\.overviewConfigMapsCount, configMapsCount)
        assignIfChanged(\.overviewCronJobsCount, cronJobsCount)
        assignIfChanged(\.overviewNodesCount, nodesCount)
        assignIfChanged(\.overviewClusterCPUPercent, clusterCPUPercent)
        assignIfChanged(\.overviewClusterMemoryPercent, clusterMemoryPercent)
        assignIfChanged(\.overviewEvents, events)
    }

    public func setOverviewClusterUsage(cpuPercent: Int?, memoryPercent: Int?) {
        overviewClusterCPUPercent = cpuPercent
        overviewClusterMemoryPercent = memoryPercent
    }

    public func setSelectedPod(_ pod: PodSummary?) {
        assignSelectionIfChanged(\.selectedPod, pod)
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
        assignSelectionIfChanged(\.selectedDeployment, deployment)
    }

    public func setSelectedService(_ service: ServiceSummary?) {
        assignSelectionIfChanged(\.selectedService, service)
    }

    public func setSelectedEvent(_ event: EventSummary?) {
        assignSelectionIfChanged(\.selectedEvent, event)
    }

    public func setSelectedStatefulSet(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedStatefulSet, resource)
    }

    public func setSelectedDaemonSet(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedDaemonSet, resource)
    }

    public func setSelectedJob(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedJob, resource)
    }

    public func setSelectedCronJob(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedCronJob, resource)
    }

    public func setSelectedReplicaSet(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedReplicaSet, resource)
    }

    public func setSelectedPersistentVolumeClaim(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedPersistentVolumeClaim, resource)
    }

    public func setSelectedPersistentVolume(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedPersistentVolume, resource)
    }

    public func setSelectedStorageClass(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedStorageClass, resource)
    }

    public func setSelectedHorizontalPodAutoscaler(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedHorizontalPodAutoscaler, resource)
    }

    public func setSelectedNetworkPolicy(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedNetworkPolicy, resource)
    }

    public func setSelectedEndpoint(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedEndpoint, resource)
    }

    public func setSelectedIngress(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedIngress, resource)
    }

    public func setSelectedConfigMap(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedConfigMap, resource)
    }

    public func setSelectedSecret(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedSecret, resource)
    }

    public func setSelectedNode(_ resource: ClusterResourceSummary?) {
        assignSelectionIfChanged(\.selectedNode, resource)
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
    public func updateResourceYAMLDraft(
        _ yaml: String,
        undoPresentation: ResourceYAMLEditorPresentation? = nil
    ) {
        guard yaml != resourceYAML else { return }
        resourceYAMLEditorRestorationRequest = nil
        pushResourceYAMLUndoSnapshotIfNeeded(
            for: yaml,
            editorPresentation: undoPresentation
        )
        resourceYAML = yaml
        advanceResourceYAMLDraftRevision()
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
    }

    /// Discards local edits and restores the last loaded cluster YAML.
    public func revertResourceYAMLToClusterSnapshot() {
        guard resourceYAMLBaseline != resourceYAML else { return }
        resourceYAMLEditorRestorationRequest = nil
        pushResourceYAMLUndoSnapshotIfNeeded(for: resourceYAMLBaseline)
        resourceYAML = resourceYAMLBaseline
        advanceResourceYAMLDraftRevision()
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
    }

    public var canUndoResourceYAMLEdit: Bool {
        !resourceYAMLUndoStack.isEmpty
    }

    public func undoResourceYAMLEdit() {
        guard let previous = resourceYAMLUndoStack.popLast() else { return }
        resourceYAMLUndoSnapshot = resourceYAMLUndoStack.last?.yaml
        resourceYAML = previous.yaml
        advanceResourceYAMLDraftRevision()
        if let presentation = previous.editorPresentation {
            resourceYAMLEditorRestorationSequence &+= 1
            resourceYAMLEditorRestorationRequest = ResourceYAMLEditorRestorationRequest(
                sequence: resourceYAMLEditorRestorationSequence,
                yaml: previous.yaml,
                presentation: presentation
            )
        } else {
            resourceYAMLEditorRestorationRequest = nil
        }
        resourceYAMLValidationIssues = []
        isValidatingResourceYAML = false
    }

    private func advanceResourceYAMLDraftRevision() {
        resourceYAMLDraftRevision &+= 1
    }

    private func pushResourceYAMLUndoSnapshotIfNeeded(
        for nextYAML: String,
        editorPresentation: ResourceYAMLEditorPresentation? = nil
    ) {
        guard nextYAML != resourceYAML else { return }
        resourceYAMLUndoStack.append(ResourceYAMLUndoEntry(
            yaml: resourceYAML,
            editorPresentation: editorPresentation
        ))
        if resourceYAMLUndoStack.count > maxResourceYAMLUndoSnapshots {
            resourceYAMLUndoStack.removeFirst(resourceYAMLUndoStack.count - maxResourceYAMLUndoSnapshots)
        }
        resourceYAMLUndoSnapshot = resourceYAMLUndoStack.last?.yaml
    }

    private func clearResourceYAMLUndoHistory() {
        resourceYAMLUndoStack = []
        resourceYAMLUndoSnapshot = nil
        resourceYAMLEditorRestorationRequest = nil
    }

    public func beginResourceYAMLValidation() {
        guard !isValidatingResourceYAML else { return }
        isValidatingResourceYAML = true
    }

    public func setResourceYAMLValidationIssues(_ issues: [YAMLValidationIssue]) {
        resourceYAMLValidationGeneration &+= 1
        guard resourceYAMLValidationIssues != issues else { return }
        resourceYAMLValidationIssues = issues
    }

    public func finishResourceYAMLValidation() {
        guard isValidatingResourceYAML else { return }
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

    /// Starts a refetch for the currently visible resource without blanking the
    /// document or discarding the user's YAML baseline and undo history.
    public func beginResourceDetailRefresh(scope: ResourceDetailScope?) {
        resourceDetailScope = scope
        lastResourceYAMLError = nil
        lastResourceDescribeError = nil
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

    /// Records a same-scope refresh failure while retaining the last fetched
    /// document, its baseline, and any local undo history.
    public func setResourceYAMLRefreshError(_ message: String?) {
        lastResourceYAMLError = message
    }

    public func setResourceDescribeError(_ message: String?) {
        resourceDescribe = ""
        lastResourceDescribeError = message
    }

    /// Records a same-scope refresh failure while retaining the last visible
    /// describe output.
    public func setResourceDescribeRefreshError(_ message: String?) {
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

    @discardableResult
    public func removeTerminalSession(id: String) -> PodTerminalSession? {
        guard let index = terminalSessions.firstIndex(where: { $0.id == id }) else {
            return nil
        }
        let removed = terminalSessions.remove(at: index)

        if activeTerminalSessionID == id {
            let fallbackIndex = min(index, terminalSessions.count - 1)
            if terminalSessions.indices.contains(fallbackIndex) {
                let fallback = terminalSessions[fallbackIndex]
                activeTerminalSessionID = fallback.id
                terminalSession = fallback
            } else {
                activeTerminalSessionID = nil
                terminalSession = nil
            }
        } else if let activeTerminalSessionID,
                  let active = terminalSessions.first(where: { $0.id == activeTerminalSessionID }) {
            terminalSession = active
        } else {
            activeTerminalSessionID = terminalSessions.first?.id
            terminalSession = terminalSessions.first
        }
        return removed
    }

    @discardableResult
    public func removeTerminalSessions(contextName: String, namespace: String? = nil) -> [PodTerminalSession] {
        let removed = terminalSessions.filter {
            $0.contextName == contextName && (namespace == nil || $0.namespace == namespace)
        }
        guard !removed.isEmpty else { return [] }

        let removedIDs = Set(removed.map(\.id))
        terminalSessions.removeAll { removedIDs.contains($0.id) }
        if let activeTerminalSessionID,
           !removedIDs.contains(activeTerminalSessionID),
           let active = terminalSessions.first(where: { $0.id == activeTerminalSessionID }) {
            terminalSession = active
        } else {
            activeTerminalSessionID = terminalSessions.first?.id
            terminalSession = terminalSessions.first
        }
        return removed
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
        if release != nil {
            assignSelectionIfChanged(\.selectedOperatorResource, nil)
        }
        assignSelectionIfChanged(\.selectedHelmRelease, release)
    }

    public func setSelectedOperatorResource(_ resource: OperatorResourceSummary?) {
        if resource != nil {
            assignSelectionIfChanged(\.selectedHelmRelease, nil)
        }
        assignSelectionIfChanged(\.selectedOperatorResource, resource)
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
