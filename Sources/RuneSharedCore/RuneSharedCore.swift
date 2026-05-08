import Combine
import Foundation

public enum RuneSection: String, CaseIterable, Codable, Sendable, Identifiable {
    case overview
    case workloads
    case networking
    case storage
    case config
    case rbac
    case events
    case helm
    case terminal

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .overview: "Overview"
        case .workloads: "Workloads"
        case .networking: "Networking"
        case .storage: "Storage"
        case .config: "Config"
        case .rbac: "RBAC"
        case .events: "Events"
        case .helm: "Helm"
        case .terminal: "Terminal"
        }
    }

    public var symbolName: String {
        switch self {
        case .overview: "rectangle.grid.2x2"
        case .workloads: "shippingbox"
        case .networking: "point.3.connected.trianglepath.dotted"
        case .storage: "internaldrive"
        case .config: "slider.horizontal.3"
        case .rbac: "person.2.badge.gearshape"
        case .events: "bolt.badge.clock"
        case .helm: "ferry"
        case .terminal: "terminal"
        }
    }
}

public enum KubeResourceKind: String, CaseIterable, Codable, Sendable, Identifiable {
    case pod
    case deployment
    case statefulSet
    case daemonSet
    case job
    case cronJob
    case replicaSet
    case service
    case ingress
    case configMap
    case secret
    case node
    case event
    case role
    case roleBinding
    case clusterRole
    case clusterRoleBinding
    case persistentVolumeClaim
    case persistentVolume
    case storageClass
    case horizontalPodAutoscaler
    case networkPolicy

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .pod: "Pods"
        case .deployment: "Deployments"
        case .statefulSet: "StatefulSets"
        case .daemonSet: "DaemonSets"
        case .job: "Jobs"
        case .cronJob: "CronJobs"
        case .replicaSet: "ReplicaSets"
        case .service: "Services"
        case .ingress: "Ingresses"
        case .configMap: "ConfigMaps"
        case .secret: "Secrets"
        case .node: "Nodes"
        case .event: "Events"
        case .role: "Roles"
        case .roleBinding: "RoleBindings"
        case .clusterRole: "ClusterRoles"
        case .clusterRoleBinding: "ClusterRoleBindings"
        case .persistentVolumeClaim: "PVCs"
        case .persistentVolume: "PersistentVolumes"
        case .storageClass: "StorageClasses"
        case .horizontalPodAutoscaler: "HPAs"
        case .networkPolicy: "NetworkPolicies"
        }
    }

    public var singularTypeName: String {
        switch self {
        case .pod: "Pod"
        case .deployment: "Deployment"
        case .statefulSet: "StatefulSet"
        case .daemonSet: "DaemonSet"
        case .job: "Job"
        case .cronJob: "CronJob"
        case .replicaSet: "ReplicaSet"
        case .service: "Service"
        case .ingress: "Ingress"
        case .configMap: "ConfigMap"
        case .secret: "Secret"
        case .node: "Node"
        case .event: "Event"
        case .role: "Role"
        case .roleBinding: "RoleBinding"
        case .clusterRole: "ClusterRole"
        case .clusterRoleBinding: "ClusterRoleBinding"
        case .persistentVolumeClaim: "PersistentVolumeClaim"
        case .persistentVolume: "PersistentVolume"
        case .storageClass: "StorageClass"
        case .horizontalPodAutoscaler: "HorizontalPodAutoscaler"
        case .networkPolicy: "NetworkPolicy"
        }
    }

    public var kubernetesResourceName: String {
        switch self {
        case .pod: "pod"
        case .deployment: "deployment"
        case .statefulSet: "statefulset"
        case .daemonSet: "daemonset"
        case .job: "job"
        case .cronJob: "cronjob"
        case .replicaSet: "replicaset"
        case .service: "service"
        case .ingress: "ingress"
        case .configMap: "configmap"
        case .secret: "secret"
        case .node: "node"
        case .event: "event"
        case .role: "role"
        case .roleBinding: "rolebinding"
        case .clusterRole: "clusterrole"
        case .clusterRoleBinding: "clusterrolebinding"
        case .persistentVolumeClaim: "pvc"
        case .persistentVolume: "pv"
        case .storageClass: "storageclass"
        case .horizontalPodAutoscaler: "hpa"
        case .networkPolicy: "networkpolicy"
        }
    }

    public var isNamespaced: Bool {
        switch self {
        case .node, .clusterRole, .clusterRoleBinding, .persistentVolume, .storageClass:
            return false
        default:
            return true
        }
    }
}

public struct KubeContext: Identifiable, Hashable, Codable, Sendable {
    public let name: String

    public init(name: String) {
        self.name = name
    }

    public var id: String { name }
}

public struct PodSummary: Identifiable, Hashable, Codable, Sendable {
    public let name: String
    public let namespace: String
    public let status: String
    public let totalRestarts: Int
    public let ageDescription: String
    public let cpuUsage: String?
    public let memoryUsage: String?
    public let podIP: String?
    public let hostIP: String?
    public let nodeName: String?
    public let qosClass: String?
    public let containersReady: String?
    public let containerNamesLine: String?

    public init(
        name: String,
        namespace: String,
        status: String,
        totalRestarts: Int = 0,
        ageDescription: String = "-",
        cpuUsage: String? = nil,
        memoryUsage: String? = nil,
        podIP: String? = nil,
        hostIP: String? = nil,
        nodeName: String? = nil,
        qosClass: String? = nil,
        containersReady: String? = nil,
        containerNamesLine: String? = nil
    ) {
        self.name = name
        self.namespace = namespace
        self.status = status
        self.totalRestarts = totalRestarts
        self.ageDescription = ageDescription
        self.cpuUsage = cpuUsage
        self.memoryUsage = memoryUsage
        self.podIP = podIP
        self.hostIP = hostIP
        self.nodeName = nodeName
        self.qosClass = qosClass
        self.containersReady = containersReady
        self.containerNamesLine = containerNamesLine
    }

    public var id: String { "\(namespace)/\(name)" }
}

public struct DeploymentSummary: Identifiable, Hashable, Codable, Sendable {
    public let name: String
    public let namespace: String
    public let readyReplicas: Int
    public let desiredReplicas: Int
    public let selector: [String: String]?

    public init(
        name: String,
        namespace: String,
        readyReplicas: Int,
        desiredReplicas: Int,
        selector: [String: String]? = nil
    ) {
        self.name = name
        self.namespace = namespace
        self.readyReplicas = readyReplicas
        self.desiredReplicas = desiredReplicas
        self.selector = selector
    }

    public var id: String { "\(namespace)/\(name)" }
    public var replicaText: String { "\(readyReplicas)/\(desiredReplicas)" }
}

public struct ServiceSummary: Identifiable, Hashable, Codable, Sendable {
    public let name: String
    public let namespace: String
    public let type: String
    public let clusterIP: String
    public let selector: [String: String]?

    public init(
        name: String,
        namespace: String,
        type: String,
        clusterIP: String,
        selector: [String: String]? = nil
    ) {
        self.name = name
        self.namespace = namespace
        self.type = type
        self.clusterIP = clusterIP
        self.selector = selector
    }

    public var id: String { "\(namespace)/\(name)" }
}

public struct EventSummary: Identifiable, Hashable, Codable, Sendable {
    public let type: String
    public let reason: String
    public let objectName: String
    public let message: String
    public let lastTimestamp: String?
    public let involvedKind: String?
    public let involvedNamespace: String?

    public init(
        type: String,
        reason: String,
        objectName: String,
        message: String,
        lastTimestamp: String? = nil,
        involvedKind: String? = nil,
        involvedNamespace: String? = nil
    ) {
        self.type = type
        self.reason = reason
        self.objectName = objectName
        self.message = message
        self.lastTimestamp = lastTimestamp
        self.involvedKind = involvedKind
        self.involvedNamespace = involvedNamespace
    }

    public var id: String {
        "\(type)|\(reason)|\(objectName)|\(involvedKind ?? "")|\(involvedNamespace ?? "")|\(lastTimestamp ?? "")|\(message.hashValue)"
    }
}

public struct ClusterResourceSummary: Identifiable, Hashable, Codable, Sendable {
    public let kind: KubeResourceKind
    public let name: String
    public let namespace: String?
    public let primaryText: String
    public let secondaryText: String

    public init(
        kind: KubeResourceKind,
        name: String,
        namespace: String?,
        primaryText: String,
        secondaryText: String
    ) {
        self.kind = kind
        self.name = name
        self.namespace = namespace
        self.primaryText = primaryText
        self.secondaryText = secondaryText
    }

    public var id: String {
        "\(kind.rawValue)|\(namespace ?? "_cluster")|\(name)"
    }
}

public struct HelmReleaseSummary: Identifiable, Hashable, Codable, Sendable {
    public let name: String
    public let namespace: String
    public let revision: Int
    public let updated: String
    public let status: String
    public let chart: String
    public let appVersion: String

    public init(
        name: String,
        namespace: String,
        revision: Int,
        updated: String,
        status: String,
        chart: String,
        appVersion: String
    ) {
        self.name = name
        self.namespace = namespace
        self.revision = revision
        self.updated = updated
        self.status = status
        self.chart = chart
        self.appVersion = appVersion
    }

    public var id: String { "\(namespace)/\(name)" }
}

@MainActor
public final class RuneAppState: ObservableObject {
    @Published public private(set) var contexts: [KubeContext] = []
    @Published public private(set) var namespaces: [String] = []
    @Published public private(set) var favoriteContextNames: Set<String> = []
    @Published public private(set) var favoriteResourceIDs: Set<String> = []
    @Published public var selectedContext: KubeContext?
    @Published public var selectedNamespace: String = "default"

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
    @Published public private(set) var rbacRoles: [ClusterResourceSummary] = []
    @Published public private(set) var rbacRoleBindings: [ClusterResourceSummary] = []
    @Published public private(set) var rbacClusterRoles: [ClusterResourceSummary] = []
    @Published public private(set) var rbacClusterRoleBindings: [ClusterResourceSummary] = []
    @Published public private(set) var helmReleases: [HelmReleaseSummary] = []

    @Published public private(set) var overviewPods: [PodSummary] = []
    @Published public private(set) var overviewDeploymentsCount = 0
    @Published public private(set) var overviewServicesCount = 0
    @Published public private(set) var overviewIngressesCount = 0
    @Published public private(set) var overviewConfigMapsCount = 0
    @Published public private(set) var overviewCronJobsCount = 0
    @Published public private(set) var overviewNodesCount = 0
    @Published public private(set) var overviewClusterCPUPercent: Int?
    @Published public private(set) var overviewClusterMemoryPercent: Int?
    @Published public private(set) var overviewEvents: [EventSummary] = []

    @Published public private(set) var resourceYAML = ""
    @Published public private(set) var resourceDescribe = ""
    @Published public private(set) var podLogs = ""
    @Published public var isLoading = false
    @Published public var isReadOnlyMode = true
    @Published public var lastError: String?

    public init() {}

    public func setFavoriteContextNames(_ names: Set<String>) {
        favoriteContextNames = names
    }

    public func setFavoriteResourceIDs(_ ids: Set<String>) {
        favoriteResourceIDs = ids
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

    public func setContexts(_ contexts: [KubeContext]) {
        self.contexts = contexts
        if selectedContext == nil || !contexts.contains(selectedContext!) {
            selectedContext = contexts.first
        }
    }

    public func setNamespaces(_ namespaces: [String]) {
        self.namespaces = Array(Set(namespaces)).sorted()
    }

    public func setPods(_ pods: [PodSummary]) { self.pods = pods }
    public func setDeployments(_ deployments: [DeploymentSummary]) { self.deployments = deployments }
    public func setServices(_ services: [ServiceSummary]) { self.services = services }
    public func setEvents(_ events: [EventSummary]) { self.events = events }
    public func setStatefulSets(_ resources: [ClusterResourceSummary]) { statefulSets = resources }
    public func setDaemonSets(_ resources: [ClusterResourceSummary]) { daemonSets = resources }
    public func setJobs(_ resources: [ClusterResourceSummary]) { jobs = resources }
    public func setCronJobs(_ resources: [ClusterResourceSummary]) { cronJobs = resources }
    public func setReplicaSets(_ resources: [ClusterResourceSummary]) { replicaSets = resources }
    public func setPersistentVolumeClaims(_ resources: [ClusterResourceSummary]) { persistentVolumeClaims = resources }
    public func setPersistentVolumes(_ resources: [ClusterResourceSummary]) { persistentVolumes = resources }
    public func setStorageClasses(_ resources: [ClusterResourceSummary]) { storageClasses = resources }
    public func setHorizontalPodAutoscalers(_ resources: [ClusterResourceSummary]) { horizontalPodAutoscalers = resources }
    public func setNetworkPolicies(_ resources: [ClusterResourceSummary]) { networkPolicies = resources }
    public func setIngresses(_ resources: [ClusterResourceSummary]) { ingresses = resources }
    public func setConfigMaps(_ resources: [ClusterResourceSummary]) { configMaps = resources }
    public func setSecrets(_ resources: [ClusterResourceSummary]) { secrets = resources }
    public func setNodes(_ resources: [ClusterResourceSummary]) { nodes = resources }
    public func setHelmReleases(_ releases: [HelmReleaseSummary]) { helmReleases = releases }

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

    public func setResourceYAML(_ yaml: String) { resourceYAML = yaml }
    public func setResourceDescribe(_ describe: String) { resourceDescribe = describe }
    public func setPodLogs(_ logs: String) { podLogs = logs }
}
