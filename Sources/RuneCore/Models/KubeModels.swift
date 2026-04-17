import Foundation

public struct KubeConfigSource: Identifiable, Hashable, Codable, Sendable {
    public let id: String
    public let path: String
    public let displayName: String

    public init(url: URL) {
        self.id = url.path
        self.path = url.path
        self.displayName = url.lastPathComponent
    }

    public var url: URL {
        URL(fileURLWithPath: path)
    }
}

public struct KubeContext: Identifiable, Hashable, Codable, Sendable {
    public let name: String

    public init(name: String) {
        self.name = name
    }

    public var id: String { name }
}

public enum KubeResourceKind: String, CaseIterable, Codable, Sendable, Identifiable {
    case pod
    case deployment
    case statefulSet
    case daemonSet
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

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .pod: return "Pods"
        case .deployment: return "Deployments"
        case .statefulSet: return "StatefulSets"
        case .daemonSet: return "DaemonSets"
        case .service: return "Services"
        case .ingress: return "Ingresses"
        case .configMap: return "ConfigMaps"
        case .secret: return "Secrets"
        case .node: return "Nodes"
        case .event: return "Events"
        case .role: return "Roles"
        case .roleBinding: return "RoleBindings"
        case .clusterRole: return "ClusterRoles"
        case .clusterRoleBinding: return "ClusterRoleBindings"
        }
    }

    /// Singular label for confirmations and copy (e.g. "Pod", "StatefulSet").
    public var singularTypeName: String {
        switch self {
        case .pod: return "Pod"
        case .deployment: return "Deployment"
        case .statefulSet: return "StatefulSet"
        case .daemonSet: return "DaemonSet"
        case .service: return "Service"
        case .ingress: return "Ingress"
        case .configMap: return "ConfigMap"
        case .secret: return "Secret"
        case .node: return "Node"
        case .event: return "Event"
        case .role: return "Role"
        case .roleBinding: return "RoleBinding"
        case .clusterRole: return "ClusterRole"
        case .clusterRoleBinding: return "ClusterRoleBinding"
        }
    }

    public var kubectlName: String {
        switch self {
        case .pod: return "pod"
        case .deployment: return "deployment"
        case .statefulSet: return "statefulset"
        case .daemonSet: return "daemonset"
        case .service: return "service"
        case .ingress: return "ingress"
        case .configMap: return "configmap"
        case .secret: return "secret"
        case .node: return "node"
        case .event: return "event"
        case .role: return "role"
        case .roleBinding: return "rolebinding"
        case .clusterRole: return "clusterrole"
        case .clusterRoleBinding: return "clusterrolebinding"
        }
    }

    public var isNamespaced: Bool {
        switch self {
        case .node, .clusterRole, .clusterRoleBinding:
            return false
        default:
            return true
        }
    }
}

public struct PodSummary: Identifiable, Hashable, Codable, Sendable {
    public let name: String
    public let namespace: String
    public let status: String
    /// Sum of `restartCount` across containers (same idea as `kubectl get pods` RESTARTS).
    public let totalRestarts: Int
    /// Human-readable age from `metadata.creationTimestamp` (e.g. `5d`, `3h`).
    public let ageDescription: String
    /// From `kubectl top pods` / Metrics API when available.
    public let cpuUsage: String?
    public let memoryUsage: String?
    /// `status.podIP` when present (JSON list).
    public let podIP: String?
    /// `status.hostIP` (node IP) when present.
    public let hostIP: String?
    /// `spec.nodeName` when scheduled.
    public let nodeName: String?
    /// `status.qosClass` (e.g. Burstable, Guaranteed).
    public let qosClass: String?
    /// Ready count vs total main containers, e.g. `2/2` (from `containerStatuses`).
    public let containersReady: String?
    /// Comma-separated `spec.containers[].name` for quick reference.
    public let containerNamesLine: String?

    public init(
        name: String,
        namespace: String,
        status: String,
        totalRestarts: Int = 0,
        ageDescription: String = "—",
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

    public var cpuDisplay: String { cpuUsage ?? "—" }
    public var memoryDisplay: String { memoryUsage ?? "—" }

    /// Inspector `kubectl get pod -o json` row merged into list row: keeps list CPU/mem/age/restarts, fills IP/node/QoS/ready from JSON.
    public func mergingInspectorDetail(_ detail: PodSummary) -> PodSummary {
        PodSummary(
            name: detail.name,
            namespace: detail.namespace,
            status: status,
            totalRestarts: totalRestarts,
            ageDescription: ageDescription,
            cpuUsage: cpuUsage ?? detail.cpuUsage,
            memoryUsage: memoryUsage ?? detail.memoryUsage,
            podIP: detail.podIP ?? podIP,
            hostIP: detail.hostIP ?? hostIP,
            nodeName: detail.nodeName ?? nodeName,
            qosClass: detail.qosClass ?? qosClass,
            containersReady: detail.containersReady ?? containersReady,
            containerNamesLine: detail.containerNamesLine ?? containerNamesLine
        )
    }
}

public enum KubernetesAgeFormatting: Sendable {
    /// Compact age string (`30s`, `5m`, `2h`, `4d`, `1y`), matching common Kubernetes CLI conventions.
    public static func describe(creationISO8601: String?, reference: Date = Date()) -> String {
        guard let raw = creationISO8601?.trimmingCharacters(in: .whitespacesAndNewlines), !raw.isEmpty else {
            return "—"
        }
        guard let created = parseRFC3339(raw) else { return "—" }
        let seconds = max(0, Int(reference.timeIntervalSince(created)))
        if seconds < 60 { return "\(seconds)s" }
        let minutes = seconds / 60
        if minutes < 60 { return "\(minutes)m" }
        let hours = seconds / 3600
        if hours < 24 { return "\(hours)h" }
        let days = seconds / 86400
        if days < 365 { return "\(days)d" }
        let years = days / 365
        return "\(max(1, years))y"
    }

    private static func parseRFC3339(_ string: String) -> Date? {
        let iso = ISO8601DateFormatter()
        iso.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let d = iso.date(from: string) { return d }
        iso.formatOptions = [.withInternetDateTime]
        return iso.date(from: string)
    }
}

public struct DeploymentSummary: Identifiable, Hashable, Codable, Sendable {
    public let name: String
    public let namespace: String
    public let readyReplicas: Int
    public let desiredReplicas: Int

    public init(name: String, namespace: String, readyReplicas: Int, desiredReplicas: Int) {
        self.name = name
        self.namespace = namespace
        self.readyReplicas = readyReplicas
        self.desiredReplicas = desiredReplicas
    }

    public var id: String { "\(namespace)/\(name)" }

    public var replicaText: String {
        "\(readyReplicas)/\(desiredReplicas)"
    }
}

public struct ServiceSummary: Identifiable, Hashable, Codable, Sendable {
    public let name: String
    public let namespace: String
    public let type: String
    public let clusterIP: String

    public init(name: String, namespace: String, type: String, clusterIP: String) {
        self.name = name
        self.namespace = namespace
        self.type = type
        self.clusterIP = clusterIP
    }

    public var id: String { "\(namespace)/\(name)" }
}

public struct EventSummary: Identifiable, Hashable, Codable, Sendable {
    public let type: String
    public let reason: String
    public let objectName: String
    public let message: String
    /// RFC3339 from `lastTimestamp` / `firstTimestamp` when present.
    public let lastTimestamp: String?
    /// Kubernetes `involvedObject.kind` (e.g. Pod, Deployment), when present.
    public let involvedKind: String?
    /// Namespace of the involved object, when namespaced.
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

public struct UnifiedServiceLogs: Sendable {
    public let service: ServiceSummary
    public let podNames: [String]
    public let mergedText: String

    public init(service: ServiceSummary, podNames: [String], mergedText: String) {
        self.service = service
        self.podNames = podNames
        self.mergedText = mergedText
    }
}

public struct UnifiedDeploymentLogs: Sendable {
    public let deployment: DeploymentSummary
    public let podNames: [String]
    public let mergedText: String

    public init(deployment: DeploymentSummary, podNames: [String], mergedText: String) {
        self.deployment = deployment
        self.podNames = podNames
        self.mergedText = mergedText
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

public struct HelmReleaseRevision: Identifiable, Hashable, Codable, Sendable {
    public let revision: Int
    public let updated: String
    public let status: String
    public let chart: String
    public let appVersion: String
    public let description: String

    public init(
        revision: Int,
        updated: String,
        status: String,
        chart: String,
        appVersion: String,
        description: String
    ) {
        self.revision = revision
        self.updated = updated
        self.status = status
        self.chart = chart
        self.appVersion = appVersion
        self.description = description
    }

    public var id: String { String(revision) }
}

public struct PodExecResult: Codable, Sendable, Equatable {
    public let podName: String
    public let namespace: String
    public let command: [String]
    public let stdout: String
    public let stderr: String
    public let exitCode: Int32

    public init(
        podName: String,
        namespace: String,
        command: [String],
        stdout: String,
        stderr: String,
        exitCode: Int32
    ) {
        self.podName = podName
        self.namespace = namespace
        self.command = command
        self.stdout = stdout
        self.stderr = stderr
        self.exitCode = exitCode
    }
}

public enum PortForwardTargetKind: String, Codable, Sendable, Identifiable {
    case pod
    case service

    public var id: String { rawValue }

    public var kubectlResourceName: String {
        switch self {
        case .pod: return "pod"
        case .service: return "service"
        }
    }

    public var title: String {
        switch self {
        case .pod: return "Pod"
        case .service: return "Service"
        }
    }
}

public enum PortForwardStatus: String, Codable, Sendable {
    case starting
    case active
    case stopped
    case failed
}

public struct PortForwardSession: Identifiable, Hashable, Codable, Sendable {
    public let id: String
    public let contextName: String
    public let namespace: String
    public let targetKind: PortForwardTargetKind
    public let targetName: String
    public let localPort: Int
    public let remotePort: Int
    public let address: String
    public let status: PortForwardStatus
    public let lastMessage: String

    public init(
        id: String,
        contextName: String,
        namespace: String,
        targetKind: PortForwardTargetKind,
        targetName: String,
        localPort: Int,
        remotePort: Int,
        address: String,
        status: PortForwardStatus,
        lastMessage: String = ""
    ) {
        self.id = id
        self.contextName = contextName
        self.namespace = namespace
        self.targetKind = targetKind
        self.targetName = targetName
        self.localPort = localPort
        self.remotePort = remotePort
        self.address = address
        self.status = status
        self.lastMessage = lastMessage
    }

    public var resourceLabel: String {
        "\(targetKind.kubectlResourceName)/\(targetName)"
    }
}
