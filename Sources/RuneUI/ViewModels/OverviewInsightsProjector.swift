import Foundation
import RuneCore

public enum OverviewSignalSeverity: String, Codable, Sendable {
    case critical
    case warning
    case info
}

public struct OverviewResourceReference: Hashable, Sendable {
    public let kind: KubeResourceKind
    public let namespace: String?
    public let name: String

    public init(kind: KubeResourceKind, namespace: String?, name: String) {
        self.kind = kind
        self.namespace = namespace
        self.name = name
    }
}

public struct OverviewSignalItem: Identifiable, Hashable, Sendable {
    public let id: String
    public let title: String
    public let detail: String
    public let badge: String
    public let severity: OverviewSignalSeverity
    public let target: OverviewResourceReference?

    public init(
        id: String,
        title: String,
        detail: String,
        badge: String,
        severity: OverviewSignalSeverity,
        target: OverviewResourceReference? = nil
    ) {
        self.id = id
        self.title = title
        self.detail = detail
        self.badge = badge
        self.severity = severity
        self.target = target
    }
}

public struct OverviewDependencyItem: Identifiable, Hashable, Sendable {
    public let id: String
    public let source: String
    public let relation: String
    public let target: String
    public let detail: String
    public let primaryTarget: OverviewResourceReference?

    public init(
        id: String,
        source: String,
        relation: String,
        target: String,
        detail: String,
        primaryTarget: OverviewResourceReference? = nil
    ) {
        self.id = id
        self.source = source
        self.relation = relation
        self.target = target
        self.detail = detail
        self.primaryTarget = primaryTarget
    }
}

public struct OverviewInsightsProjector: Sendable {
    public let pods: [PodSummary]
    public let deployments: [DeploymentSummary]
    public let services: [ServiceSummary]
    public let events: [EventSummary]

    public init(
        pods: [PodSummary],
        deployments: [DeploymentSummary],
        services: [ServiceSummary],
        events: [EventSummary]
    ) {
        self.pods = pods
        self.deployments = deployments
        self.services = services
        self.events = events
    }

    public func unhealthyItems(limit: Int = 8) -> [OverviewSignalItem] {
        var critical: [OverviewSignalItem] = []
        var warning: [OverviewSignalItem] = []

        func append(_ item: OverviewSignalItem) {
            switch item.severity {
            case .critical:
                if critical.count < limit { critical.append(item) }
            case .warning, .info:
                if warning.count < limit { warning.append(item) }
            }
        }

        for pod in pods {
            let status = pod.status.trimmingCharacters(in: .whitespacesAndNewlines)
            let normalized = status.lowercased()
            let isSuccessfulTerminalPod = Self.successfulTerminalPodStatuses.contains(normalized)
            let readyWarning = isSuccessfulTerminalPod ? nil : pod.containersReady.flatMap { Self.readinessWarning($0) }
            if !["running"].contains(normalized) && !isSuccessfulTerminalPod {
                append(OverviewSignalItem(
                    id: "pod-status|\(pod.id)|\(status)",
                    title: pod.name,
                    detail: "Pod \(status.isEmpty ? "has unknown status" : "is \(status)")",
                    badge: "Pod",
                    severity: Self.criticalPodStatuses.contains(normalized) ? .critical : .warning,
                    target: OverviewResourceReference(kind: .pod, namespace: pod.namespace, name: pod.name)
                ))
            } else if let readyWarning {
                append(OverviewSignalItem(
                    id: "pod-ready|\(pod.id)|\(readyWarning)",
                    title: pod.name,
                    detail: readyWarning,
                    badge: "Pod",
                    severity: .warning,
                    target: OverviewResourceReference(kind: .pod, namespace: pod.namespace, name: pod.name)
                ))
            }

            if !isSuccessfulTerminalPod, pod.totalRestarts >= Self.restartSignalThreshold {
                append(OverviewSignalItem(
                    id: "pod-restarts|\(pod.id)|\(pod.totalRestarts)",
                    title: pod.name,
                    detail: "\(pod.totalRestarts) container restart\(pod.totalRestarts == 1 ? "" : "s")",
                    badge: "Restart",
                    severity: pod.totalRestarts >= 5 ? .critical : .warning,
                    target: OverviewResourceReference(kind: .pod, namespace: pod.namespace, name: pod.name)
                ))
            }
        }

        for deployment in deployments where deployment.desiredReplicas > deployment.readyReplicas {
            append(OverviewSignalItem(
                id: "deployment-ready|\(deployment.id)|\(deployment.readyReplicas)-\(deployment.desiredReplicas)",
                title: deployment.name,
                detail: "\(deployment.readyReplicas)/\(deployment.desiredReplicas) replicas ready",
                badge: "Deploy",
                severity: deployment.readyReplicas == 0 ? .critical : .warning,
                target: OverviewResourceReference(kind: .deployment, namespace: deployment.namespace, name: deployment.name)
            ))
        }

        return Array((critical + warning).prefix(limit))
    }

    public func incidentTimelineItems(limit: Int = 8) -> [OverviewSignalItem] {
        var critical: [OverviewSignalItem] = []
        var warning: [OverviewSignalItem] = []

        func append(_ item: OverviewSignalItem) {
            switch item.severity {
            case .critical:
                if critical.count < limit { critical.append(item) }
            case .warning, .info:
                if warning.count < limit { warning.append(item) }
            }
        }

        for event in events where event.type.caseInsensitiveCompare("Warning") == .orderedSame {
            append(OverviewSignalItem(
                id: "timeline-event|\(event.id)",
                title: event.reason + " • " + event.objectName,
                detail: event.message,
                badge: event.lastTimestamp?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty ?? "Event",
                severity: .warning,
                target: OverviewResourceReference(
                    kind: .event,
                    namespace: event.involvedNamespace,
                    name: event.id
                )
            ))
        }

        return Array((critical + warning).prefix(limit))
    }

    public func dependencyItems(limit: Int = 8) -> [OverviewDependencyItem] {
        var items: [OverviewDependencyItem] = []
        let deploymentsByName = deployments.reduce(into: [String: DeploymentSummary]()) { result, deployment in
            if result[deployment.name] == nil {
                result[deployment.name] = deployment
            }
        }

        for service in services {
            if items.count >= limit { break }
            let deploymentTargets = [
                deploymentsByName[service.name],
                deployments.first { deployment in
                    Self.selectorsOverlap(service.selector, deployment.selector)
                }
            ]
            .compactMap { $0 }
            .reduce(into: [DeploymentSummary]()) { result, deployment in
                if !result.contains(where: { $0.id == deployment.id }) {
                    result.append(deployment)
                }
            }

            for deployment in deploymentTargets {
                let pods = podsLikelyOwned(by: deployment)
                let podDetail = pods.isEmpty
                    ? "No matching pods loaded"
                    : "\(pods.count) pod\(pods.count == 1 ? "" : "s")"
                items.append(OverviewDependencyItem(
                    id: "service-deployment|\(service.id)|\(deployment.id)",
                    source: "Service/" + service.name,
                    relation: "routes to",
                    target: "Deployment/" + deployment.name,
                    detail: podDetail,
                    primaryTarget: OverviewResourceReference(kind: .deployment, namespace: deployment.namespace, name: deployment.name)
                ))
            }

            if deploymentTargets.isEmpty {
                let matchingPods = pods.filter { pod in
                    pod.name.hasPrefix(service.name + "-") || pod.name == service.name
                }
                if !matchingPods.isEmpty {
                    items.append(OverviewDependencyItem(
                        id: "service-pods|\(service.id)",
                        source: "Service/" + service.name,
                        relation: "selects",
                        target: "\(matchingPods.count) pod\(matchingPods.count == 1 ? "" : "s")",
                        detail: service.type,
                        primaryTarget: OverviewResourceReference(kind: .service, namespace: service.namespace, name: service.name)
                    ))
                }
            }
        }

        if items.isEmpty {
            for deployment in deployments.prefix(6) {
                if items.count >= limit { break }
                let matchingPods = podsLikelyOwned(by: deployment)
                guard !matchingPods.isEmpty else { continue }
                items.append(OverviewDependencyItem(
                    id: "deployment-pods|\(deployment.id)",
                    source: "Deployment/" + deployment.name,
                    relation: "owns",
                    target: "\(matchingPods.count) pod\(matchingPods.count == 1 ? "" : "s")",
                    detail: deployment.replicaText + " replicas ready",
                    primaryTarget: OverviewResourceReference(kind: .deployment, namespace: deployment.namespace, name: deployment.name)
                ))
            }
        }

        return Array(items.prefix(limit))
    }

    private static let criticalPodStatuses = Set([
        "failed",
        "error",
        "crashloopbackoff",
        "imagepullbackoff",
        "errimagepull"
    ])

    private static let successfulTerminalPodStatuses = Set([
        "succeeded",
        "completed"
    ])

    private static let restartSignalThreshold = 3

    private static func readinessWarning(_ readyText: String) -> String? {
        let parts = readyText
            .split(separator: "/", maxSplits: 1)
            .compactMap { Int($0.trimmingCharacters(in: .whitespacesAndNewlines)) }
        guard parts.count == 2, parts[0] < parts[1] else { return nil }
        return "\(parts[0])/\(parts[1]) containers ready"
    }

    private static func selectorsOverlap(_ lhs: [String: String]?, _ rhs: [String: String]?) -> Bool {
        guard let lhs, let rhs, !lhs.isEmpty, !rhs.isEmpty else { return false }
        return lhs.contains { key, value in
            rhs[key]?.caseInsensitiveCompare(value) == .orderedSame
        }
    }

    private func podsLikelyOwned(by deployment: DeploymentSummary) -> [PodSummary] {
        pods.filter { pod in
            pod.namespace == deployment.namespace
                && (pod.name == deployment.name || pod.name.hasPrefix(deployment.name + "-"))
        }
    }
}

private extension String {
    var nilIfEmpty: String? {
        isEmpty ? nil : self
    }
}
