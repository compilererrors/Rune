import Combine
import Foundation

@MainActor
public final class RuneAppState: ObservableObject {
    @Published public private(set) var kubeConfigSources: [KubeConfigSource] = []
    @Published public private(set) var contexts: [KubeContext] = []
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

    @Published public private(set) var podLogs: String = ""
    @Published public private(set) var unifiedServiceLogs: String = ""
    @Published public private(set) var unifiedServiceLogPods: [String] = []
    @Published public private(set) var resourceYAML: String = ""
    @Published public private(set) var lastExecResult: PodExecResult?
    @Published public private(set) var portForwardSessions: [PortForwardSession] = []

    @Published public var contextSearchQuery: String = ""
    @Published public var resourceSearchQuery: String = ""

    @Published public var isCommandPalettePresented: Bool = false
    @Published public var isLoading: Bool = false
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

    public func setPods(_ pods: [PodSummary]) {
        self.pods = pods
        if let selectedPod, pods.contains(selectedPod) {
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

    public func setPodLogs(_ logs: String) {
        podLogs = logs
    }

    public func setUnifiedServiceLogs(_ logs: String, pods: [String]) {
        unifiedServiceLogs = logs
        unifiedServiceLogPods = pods
    }

    public func clearUnifiedServiceLogs() {
        unifiedServiceLogs = ""
        unifiedServiceLogPods = []
    }

    public func setResourceYAML(_ yaml: String) {
        resourceYAML = yaml
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
    }

    public func setError(_ error: Error) {
        lastError = error.localizedDescription
    }

    public func clearError() {
        lastError = nil
    }
}
