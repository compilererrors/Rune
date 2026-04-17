import Combine
import Foundation

@MainActor
public final class RuneAppState: ObservableObject {
    @Published public private(set) var kubeConfigSources: [KubeConfigSource] = []
    @Published public private(set) var contexts: [KubeContext] = []
    @Published public private(set) var namespaces: [String] = []
    @Published public private(set) var favoriteContextNames: Set<String> = []

    @Published public var selectedContext: KubeContext?
    @Published public var selectedNamespace: String = "default"
    @Published public var selectedSection: RuneSection = .overview
    @Published public var selectedWorkloadKind: KubeResourceKind = .pod

    @Published public var selectedPod: PodSummary?
    @Published public var selectedDeployment: DeploymentSummary?
    @Published public var selectedService: ServiceSummary?
    @Published public var selectedEvent: EventSummary?
    @Published public var selectedStatefulSet: ClusterResourceSummary?
    @Published public var selectedDaemonSet: ClusterResourceSummary?
    @Published public var selectedIngress: ClusterResourceSummary?
    @Published public var selectedConfigMap: ClusterResourceSummary?
    @Published public var selectedSecret: ClusterResourceSummary?
    @Published public var selectedNode: ClusterResourceSummary?
    @Published public var selectedHelmRelease: HelmReleaseSummary?

    @Published public private(set) var pods: [PodSummary] = []
    @Published public private(set) var deployments: [DeploymentSummary] = []
    @Published public private(set) var services: [ServiceSummary] = []
    @Published public private(set) var events: [EventSummary] = []
    @Published public private(set) var statefulSets: [ClusterResourceSummary] = []
    @Published public private(set) var daemonSets: [ClusterResourceSummary] = []
    @Published public private(set) var ingresses: [ClusterResourceSummary] = []
    @Published public private(set) var configMaps: [ClusterResourceSummary] = []
    @Published public private(set) var secrets: [ClusterResourceSummary] = []
    @Published public private(set) var nodes: [ClusterResourceSummary] = []
    @Published public private(set) var helmReleases: [HelmReleaseSummary] = []
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
    @Published public private(set) var overviewNodesCount: Int = 0
    @Published public private(set) var overviewClusterCPUPercent: Int?
    @Published public private(set) var overviewClusterMemoryPercent: Int?
    @Published public private(set) var overviewEvents: [EventSummary] = []

    @Published public private(set) var podLogs: String = ""
    @Published public private(set) var unifiedServiceLogs: String = ""
    @Published public private(set) var unifiedServiceLogPods: [String] = []
    /// Set when the latest log fetch failed (timeout, kubectl error). Cleared on successful load or new fetch.
    @Published public private(set) var lastLogFetchError: String?
    @Published public private(set) var resourceYAML: String = ""
    /// Last YAML fetched from the cluster (`kubectl get -o yaml`). Used to detect local edits and for Revert.
    @Published public private(set) var resourceYAMLBaseline: String = ""
    @Published public private(set) var resourceDescribe: String = ""
    @Published public private(set) var deploymentRolloutHistory: String = ""
    @Published public private(set) var helmValues: String = ""
    @Published public private(set) var helmManifest: String = ""
    @Published public private(set) var helmHistory: [HelmReleaseRevision] = []
    @Published public private(set) var lastExecResult: PodExecResult?
    @Published public private(set) var portForwardSessions: [PortForwardSession] = []

    @Published public var contextSearchQuery: String = ""
    @Published public var resourceSearchQuery: String = ""
    @Published public var isHelmAllNamespaces: Bool = true

    @Published public var isCommandPalettePresented: Bool = false
    @Published public var isLoading: Bool = false
    @Published public var isLoadingLogs: Bool = false
    @Published public var isReadOnlyMode: Bool = false
    @Published public var isExecutingCommand: Bool = false
    @Published public var isStartingPortForward: Bool = false
    @Published public var lastError: String?

    public init() {}

    public func setSources(_ sources: [KubeConfigSource]) {
        kubeConfigSources = sources
    }

    public func setFavoriteContextNames(_ names: Set<String>) {
        favoriteContextNames = names
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

    public func setHelmReleases(_ releases: [HelmReleaseSummary]) {
        helmReleases = releases
        if let selectedHelmRelease, releases.contains(selectedHelmRelease) { return }
        selectedHelmRelease = releases.first
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
        overviewNodesCount = nodesCount
        overviewClusterCPUPercent = clusterCPUPercent
        overviewClusterMemoryPercent = clusterMemoryPercent
        overviewEvents = events
    }

    public func setSelectedPod(_ pod: PodSummary?) {
        selectedPod = pod
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

    public func setSelectedHelmRelease(_ release: HelmReleaseSummary?) {
        selectedHelmRelease = release
    }

    public func setPodLogs(_ logs: String) {
        podLogs = logs
        lastLogFetchError = nil
    }

    public func setUnifiedServiceLogs(_ logs: String, pods: [String]) {
        unifiedServiceLogs = logs
        unifiedServiceLogPods = pods
        lastLogFetchError = nil
    }

    public func clearUnifiedServiceLogs() {
        unifiedServiceLogs = ""
        unifiedServiceLogPods = []
        lastLogFetchError = nil
    }

    public func setLastLogFetchError(_ message: String?) {
        lastLogFetchError = message
    }

    public func setResourceYAML(_ yaml: String) {
        resourceYAML = yaml
        resourceYAMLBaseline = yaml
    }

    /// Updates the in-memory YAML (user edits or import). Does not change the cluster baseline until the next fetch or successful apply + reload.
    public func updateResourceYAMLDraft(_ yaml: String) {
        resourceYAML = yaml
    }

    /// Discards local edits and restores the last loaded cluster YAML.
    public func revertResourceYAMLToClusterSnapshot() {
        resourceYAML = resourceYAMLBaseline
    }

    public var resourceYAMLHasUnsavedEdits: Bool {
        resourceYAML != resourceYAMLBaseline
    }

    public func setResourceDescribe(_ text: String) {
        resourceDescribe = text
    }

    public func setDeploymentRolloutHistory(_ history: String) {
        deploymentRolloutHistory = history
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

    public func setLastExecResult(_ result: PodExecResult?) {
        lastExecResult = result
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

    public func clearResourceDetails() {
        podLogs = ""
        unifiedServiceLogs = ""
        unifiedServiceLogPods = []
        resourceYAML = ""
        resourceYAMLBaseline = ""
        resourceDescribe = ""
        deploymentRolloutHistory = ""
        helmValues = ""
        helmManifest = ""
        helmHistory = []
        isLoadingLogs = false
        lastLogFetchError = nil
    }

    public func setError(_ error: Error) {
        lastError = error.localizedDescription
    }

    public func clearError() {
        lastError = nil
    }

    public func setErrorMessage(_ message: String?) {
        lastError = message
    }
}
