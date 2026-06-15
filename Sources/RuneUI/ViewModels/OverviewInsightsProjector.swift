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
    public let operatorResourceID: String?

    public init(
        id: String,
        title: String,
        detail: String,
        badge: String,
        severity: OverviewSignalSeverity,
        target: OverviewResourceReference? = nil,
        operatorResourceID: String? = nil
    ) {
        self.id = id
        self.title = title
        self.detail = detail
        self.badge = badge
        self.severity = severity
        self.target = target
        self.operatorResourceID = operatorResourceID
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

public enum OverviewGitOpsController: String, Sendable {
    case all
    case flux
    case argoCD
    case unhealthy
}

public struct OverviewGitOpsRollupItem: Identifiable, Hashable, Sendable {
    public let id: String
    public let title: String
    public let detail: String
    public let badge: String
    public let severity: OverviewSignalSeverity
    public let controller: OverviewGitOpsController

    public init(
        id: String,
        title: String,
        detail: String,
        badge: String,
        severity: OverviewSignalSeverity,
        controller: OverviewGitOpsController
    ) {
        self.id = id
        self.title = title
        self.detail = detail
        self.badge = badge
        self.severity = severity
        self.controller = controller
    }
}

public struct OverviewInsightsProjector: Sendable {
    public let pods: [PodSummary]
    public let deployments: [DeploymentSummary]
    public let services: [ServiceSummary]
    public let ingresses: [ClusterResourceSummary]
    public let persistentVolumeClaims: [ClusterResourceSummary]
    public let persistentVolumes: [ClusterResourceSummary]
    public let events: [EventSummary]
    public let jobs: [ClusterResourceSummary]
    public let nodes: [ClusterResourceSummary]
    public let operatorResources: [OperatorResourceSummary]
    private let now: Date

    public init(
        pods: [PodSummary],
        deployments: [DeploymentSummary],
        services: [ServiceSummary],
        ingresses: [ClusterResourceSummary] = [],
        persistentVolumeClaims: [ClusterResourceSummary] = [],
        persistentVolumes: [ClusterResourceSummary] = [],
        events: [EventSummary],
        jobs: [ClusterResourceSummary] = [],
        nodes: [ClusterResourceSummary] = [],
        operatorResources: [OperatorResourceSummary] = [],
        now: Date = Date()
    ) {
        self.pods = pods
        self.deployments = deployments
        self.services = services
        self.ingresses = ingresses
        self.persistentVolumeClaims = persistentVolumeClaims
        self.persistentVolumes = persistentVolumes
        self.events = events
        self.jobs = jobs
        self.nodes = nodes
        self.operatorResources = operatorResources
        self.now = now
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

        for job in jobs {
            guard let item = jobSignal(for: job) else { continue }
            append(item)
        }

        for node in nodes {
            guard let item = nodeSignal(for: node) else { continue }
            append(item)
        }

        for resource in operatorResources {
            if let item = certificateSignal(for: resource) {
                append(item)
            } else if let item = gatewaySignal(for: resource) {
                append(item)
            } else if let item = gitOpsSignal(for: resource) {
                append(item)
            }
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
        let servicesByNamespaceAndName = services.reduce(into: [String: ServiceSummary]()) { result, service in
            result["\(service.namespace)/\(service.name)"] = service
        }

        for ingress in ingresses {
            if items.count >= limit { break }
            guard ingress.kind == .ingress else { continue }
            let serviceNames = Self.serviceNames(fromIngressSecondaryText: ingress.secondaryText)
            for serviceName in serviceNames {
                if items.count >= limit { break }
                guard let service = servicesByNamespaceAndName["\(ingress.namespace ?? "")/\(serviceName)"] else {
                    continue
                }
                items.append(OverviewDependencyItem(
                    id: "ingress-service|\(ingress.id)|\(service.id)",
                    source: "Ingress/" + ingress.name,
                    relation: "routes to",
                    target: "Service/" + service.name,
                    detail: ingress.primaryText.isEmpty ? service.type : ingress.primaryText,
                    primaryTarget: OverviewResourceReference(kind: .service, namespace: service.namespace, name: service.name)
                ))
            }
        }

        let persistentVolumesByName = persistentVolumes.reduce(into: [String: ClusterResourceSummary]()) { result, persistentVolume in
            result[persistentVolume.name] = persistentVolume
        }

        for persistentVolumeClaim in persistentVolumeClaims {
            if items.count >= limit { break }
            guard persistentVolumeClaim.kind == .persistentVolumeClaim,
                  let volumeName = Self.persistentVolumeName(fromPVCSecondaryText: persistentVolumeClaim.secondaryText),
                  let persistentVolume = persistentVolumesByName[volumeName] else {
                continue
            }
            items.append(OverviewDependencyItem(
                id: "pvc-pv|\(persistentVolumeClaim.id)|\(persistentVolume.id)",
                source: "PVC/" + persistentVolumeClaim.name,
                relation: "binds to",
                target: "PV/" + persistentVolume.name,
                detail: persistentVolumeClaim.primaryText.isEmpty ? persistentVolume.primaryText : persistentVolumeClaim.primaryText,
                primaryTarget: OverviewResourceReference(kind: .persistentVolume, namespace: nil, name: persistentVolume.name)
            ))
        }

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

    public func gitOpsRollupItems() -> [OverviewGitOpsRollupItem] {
        let flux = operatorResources.filter(Self.isFluxResource)
        let argo = operatorResources.filter(Self.isArgoResource)
        let gitOps = flux + argo
        guard !gitOps.isEmpty else { return [] }

        let unhealthyFlux = flux.filter(Self.isUnhealthyGitOpsResource)
        let unhealthyArgo = argo.filter(Self.isUnhealthyGitOpsResource)
        let unhealthyTotal = unhealthyFlux.count + unhealthyArgo.count

        var items: [OverviewGitOpsRollupItem] = [
            OverviewGitOpsRollupItem(
                id: "gitops-all",
                title: "GitOps resources",
                detail: "\(gitOps.count) loaded • Flux \(flux.count) • ArgoCD \(argo.count)",
                badge: "\(gitOps.count)",
                severity: unhealthyTotal > 0 ? .warning : .info,
                controller: .all
            )
        ]

        if !flux.isEmpty {
            items.append(OverviewGitOpsRollupItem(
                id: "gitops-flux",
                title: "Flux",
                detail: "\(flux.count) resource\(flux.count == 1 ? "" : "s") • \(unhealthyFlux.count) unhealthy",
                badge: "\(unhealthyFlux.count)",
                severity: unhealthyFlux.isEmpty ? .info : .critical,
                controller: .flux
            ))
        }

        if !argo.isEmpty {
            items.append(OverviewGitOpsRollupItem(
                id: "gitops-argocd",
                title: "ArgoCD",
                detail: "\(argo.count) resource\(argo.count == 1 ? "" : "s") • \(unhealthyArgo.count) unhealthy",
                badge: "\(unhealthyArgo.count)",
                severity: unhealthyArgo.isEmpty ? .info : .critical,
                controller: .argoCD
            ))
        }

        if unhealthyTotal > 0 {
            items.append(OverviewGitOpsRollupItem(
                id: "gitops-unhealthy",
                title: "Unhealthy GitOps",
                detail: "\(unhealthyTotal) resource\(unhealthyTotal == 1 ? "" : "s") need attention",
                badge: "\(unhealthyTotal)",
                severity: .critical,
                controller: .unhealthy
            ))
        }

        return items
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
    private static let certificateExpiryWarningWindow: TimeInterval = 14 * 24 * 60 * 60

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

    private static func isFluxResource(_ resource: OperatorResourceSummary) -> Bool {
        let family = resource.family.lowercased()
        let kind = resource.kind.lowercased()
        let apiPath = resource.apiPath.lowercased()
        return family.contains("flux")
            || apiPath.contains("toolkit.fluxcd.io")
            || kind.contains("kustomization")
            || kind.contains("helmrelease")
            || kind.contains("gitrepository")
    }

    private static func isArgoResource(_ resource: OperatorResourceSummary) -> Bool {
        let family = resource.family.lowercased()
        let kind = resource.kind.lowercased()
        let apiPath = resource.apiPath.lowercased()
        return family.contains("argo")
            || apiPath.contains("argoproj.io")
            || kind.contains("application")
            || kind.contains("appproject")
    }

    private static func isUnhealthyGitOpsResource(_ resource: OperatorResourceSummary) -> Bool {
        guard isFluxResource(resource) || isArgoResource(resource) else { return false }
        let statusText = combinedStatusText(resource)
        return statusLooksUnhealthy(statusText) || gitOpsStatusLooksDrifted(statusText)
    }

    private static func serviceNames(fromIngressSecondaryText text: String) -> [String] {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        let lowercased = trimmed.lowercased()
        guard lowercased.hasPrefix("service ") || lowercased.hasPrefix("services ") else { return [] }

        let prefixLength = lowercased.hasPrefix("services ") ? "services ".count : "service ".count
        let start = trimmed.index(trimmed.startIndex, offsetBy: prefixLength)
        return trimmed[start...]
            .split(separator: ",")
            .map { raw in
                raw
                    .split(separator: ":")
                    .first
                    .map(String.init)?
                    .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            }
            .filter { !$0.isEmpty }
    }

    private static func persistentVolumeName(fromPVCSecondaryText text: String) -> String? {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.lowercased().hasPrefix("pv ") else { return nil }
        let start = trimmed.index(trimmed.startIndex, offsetBy: "PV ".count)
        let name = trimmed[start...]
            .split(separator: "·")
            .first
            .map(String.init)?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard let name, !name.isEmpty else { return nil }
        return name
    }

    private func jobSignal(for resource: ClusterResourceSummary) -> OverviewSignalItem? {
        guard resource.kind == .job else { return nil }
        let text = Self.combinedResourceText(resource)
        guard text.contains("failed")
            || text.contains("backoff")
            || text.contains("error") else {
            return nil
        }
        return OverviewSignalItem(
            id: "job-failed|\(resource.id)|\(resource.primaryText)|\(resource.secondaryText)",
            title: resource.name,
            detail: Self.signalDetail(resource, fallback: "Job has failed"),
            badge: "Job",
            severity: .critical,
            target: OverviewResourceReference(kind: .job, namespace: resource.namespace, name: resource.name)
        )
    }

    private func nodeSignal(for resource: ClusterResourceSummary) -> OverviewSignalItem? {
        guard resource.kind == .node else { return nil }
        let text = Self.combinedResourceText(resource)
        guard text.contains("notready")
            || text.contains("not ready")
            || text.contains("ready false")
            || text.contains("unreachable")
            || text.contains("pressure") else {
            return nil
        }
        return OverviewSignalItem(
            id: "node-not-ready|\(resource.id)|\(resource.primaryText)|\(resource.secondaryText)",
            title: resource.name,
            detail: Self.signalDetail(resource, fallback: "Node is not ready"),
            badge: "Node",
            severity: text.contains("notready") || text.contains("not ready") ? .critical : .warning,
            target: OverviewResourceReference(kind: .node, namespace: nil, name: resource.name)
        )
    }

    private func certificateSignal(for resource: OperatorResourceSummary) -> OverviewSignalItem? {
        let family = resource.family.lowercased()
        let kind = resource.kind.lowercased()
        guard family.contains("cert-manager"), kind.contains("certificate") else { return nil }

        let statusText = Self.combinedStatusText(resource)
        if let expiry = Self.certificateExpiryDate(from: resource) {
            let remaining = expiry.timeIntervalSince(now)
            if remaining < 0 {
                return operatorSignal(
                    resource,
                    idPrefix: "certificate-expired",
                    detail: "Certificate expired \(Self.shortDate(expiry))",
                    badge: "Cert",
                    severity: .critical
                )
            }
            if remaining <= Self.certificateExpiryWarningWindow {
                return operatorSignal(
                    resource,
                    idPrefix: "certificate-expiring",
                    detail: "Certificate expires \(Self.shortDate(expiry))",
                    badge: "Cert",
                    severity: .warning
                )
            }
        }

        guard Self.statusLooksUnhealthy(statusText) else { return nil }
        return operatorSignal(
            resource,
            idPrefix: "certificate-ready",
            detail: Self.signalDetail(resource, fallback: "Certificate is not ready"),
            badge: "Cert",
            severity: .warning
        )
    }

    private func gatewaySignal(for resource: OperatorResourceSummary) -> OverviewSignalItem? {
        let family = resource.family.lowercased()
        let kind = resource.kind.lowercased()
        guard family.contains("gateway"), kind == "gateways" || kind == "gateway" else { return nil }

        let statusText = Self.combinedStatusText(resource)
        guard Self.statusLooksUnhealthy(statusText) else { return nil }
        return operatorSignal(
            resource,
            idPrefix: "gateway-ready",
            detail: Self.signalDetail(resource, fallback: "Gateway listeners are not ready"),
            badge: "Gateway",
            severity: statusText.contains("failed") ? .critical : .warning
        )
    }

    private func gitOpsSignal(for resource: OperatorResourceSummary) -> OverviewSignalItem? {
        let family = resource.family.lowercased()
        let kind = resource.kind.lowercased()
        let isFlux = family.contains("flux")
            && (kind.contains("kustomization") || kind.contains("helmrelease") || kind.contains("gitrepositor"))
        let isArgo = family.contains("argo")
            && (kind.contains("application") || kind.contains("appproject"))
        guard isFlux || isArgo else { return nil }

        let statusText = Self.combinedStatusText(resource)
        guard Self.statusLooksUnhealthy(statusText) || Self.gitOpsStatusLooksDrifted(statusText) else { return nil }
        return operatorSignal(
            resource,
            idPrefix: isFlux ? "flux-drift" : "argocd-drift",
            detail: Self.signalDetail(resource, fallback: isFlux ? "Flux resource is not reconciled" : "ArgoCD resource is not synced"),
            badge: isFlux ? "Flux" : "ArgoCD",
            severity: statusText.contains("degraded") || statusText.contains("failed") ? .critical : .warning
        )
    }

    private func operatorSignal(
        _ resource: OperatorResourceSummary,
        idPrefix: String,
        detail: String,
        badge: String,
        severity: OverviewSignalSeverity
    ) -> OverviewSignalItem {
        OverviewSignalItem(
            id: "\(idPrefix)|\(resource.id)|\(detail)",
            title: resource.name,
            detail: detail,
            badge: badge,
            severity: severity,
            operatorResourceID: resource.id
        )
    }

    private static func combinedStatusText(_ resource: OperatorResourceSummary) -> String {
        ([resource.status, resource.message] + resource.printerColumns.flatMap { [$0.title, $0.value] })
            .joined(separator: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
    }

    private static func statusLooksUnhealthy(_ text: String) -> Bool {
        guard !text.isEmpty else { return false }
        if text.contains("ready false")
            || text.contains("ready:false")
            || text.contains("not ready")
            || text.contains("programmed false")
            || text.contains("accepted false")
            || text.contains("failed")
            || text.contains("error") {
            return true
        }
        return false
    }

    private static func gitOpsStatusLooksDrifted(_ text: String) -> Bool {
        guard !text.isEmpty else { return false }
        return text.contains("synced false")
            || text.contains("sync false")
            || text.contains("outofsync")
            || text.contains("out of sync")
            || text.contains("degraded")
            || (text.contains("reconcil") && text.contains("failed"))
            || text.contains("stalled")
            || text.contains("suspended")
    }

    private static func signalDetail(_ resource: OperatorResourceSummary, fallback: String) -> String {
        let message = resource.message.trimmingCharacters(in: .whitespacesAndNewlines)
        if !message.isEmpty { return message }
        let status = resource.status.trimmingCharacters(in: .whitespacesAndNewlines)
        if !status.isEmpty { return status }
        return fallback
    }

    private static func signalDetail(_ resource: ClusterResourceSummary, fallback: String) -> String {
        let secondary = resource.secondaryText.trimmingCharacters(in: .whitespacesAndNewlines)
        if !secondary.isEmpty { return secondary }
        let primary = resource.primaryText.trimmingCharacters(in: .whitespacesAndNewlines)
        if !primary.isEmpty { return primary }
        return fallback
    }

    private static func combinedResourceText(_ resource: ClusterResourceSummary) -> String {
        [resource.primaryText, resource.secondaryText]
            .joined(separator: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
    }

    private static func certificateExpiryDate(from resource: OperatorResourceSummary) -> Date? {
        for column in resource.printerColumns {
            let title = column.title.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            guard title.contains("expiry")
                || title.contains("expires")
                || title.contains("expiration")
                || title.contains("not after")
                || title.contains("renewal") else {
                continue
            }
            if let date = parseDate(column.value) {
                return date
            }
        }
        return nil
    }

    private static func parseDate(_ value: String) -> Date? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        if let date = ISO8601DateFormatter().date(from: trimmed) {
            return date
        }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.date(from: trimmed)
    }

    private static func shortDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.string(from: date)
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
