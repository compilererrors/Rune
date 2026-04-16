import Foundation
import RuneCore

public struct KubectlOutputParser {
    public init() {}

    public func parseContexts(from raw: String) -> [KubeContext] {
        raw
            .split(whereSeparator: \.isNewline)
            .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .map(KubeContext.init(name:))
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseNamespaces(from raw: String) -> [String] {
        Array(
            Set(
                raw
                    .split(whereSeparator: \.isNewline)
                    .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
                    .filter { !$0.isEmpty }
            )
        )
        .sorted()
    }

    public func parsePods(namespace: String, from raw: String) -> [PodSummary] {
        raw
            .split(whereSeparator: \.isNewline)
            .compactMap { line in
                let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return nil }

                let parts = trimmed.split(maxSplits: 1, omittingEmptySubsequences: true, whereSeparator: \.isWhitespace)
                guard !parts.isEmpty else { return nil }

                let name = String(parts[0])
                let status = parts.count > 1 ? String(parts[1]) : "Unknown"
                return PodSummary(name: name, namespace: namespace, status: status)
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parsePodsAllNamespaces(from raw: String) -> [PodSummary] {
        raw
            .split(whereSeparator: \.isNewline)
            .compactMap { line in
                let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return nil }

                let parts = trimmed.split(whereSeparator: \.isWhitespace)
                guard parts.count >= 3 else { return nil }

                return PodSummary(
                    name: String(parts[1]),
                    namespace: String(parts[0]),
                    status: String(parts[2])
                )
            }
            .sorted {
                if $0.namespace != $1.namespace {
                    return $0.namespace.localizedCaseInsensitiveCompare($1.namespace) == .orderedAscending
                }
                return $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
            }
    }

    public func parseDeployments(namespace: String, from raw: String) throws -> [DeploymentSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeDeploymentItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                DeploymentSummary(
                    name: item.metadata.name,
                    namespace: item.metadata.namespace ?? namespace,
                    readyReplicas: item.status.readyReplicas ?? 0,
                    desiredReplicas: item.spec.replicas ?? 0
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseStatefulSets(namespace: String, from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeDeploymentItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                ClusterResourceSummary(
                    kind: .statefulSet,
                    name: item.metadata.name,
                    namespace: item.metadata.namespace ?? namespace,
                    primaryText: "\(item.status.readyReplicas ?? 0)/\(item.spec.replicas ?? 0) ready",
                    secondaryText: "Stateful workload"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseDaemonSets(namespace: String, from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeDaemonSetItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                ClusterResourceSummary(
                    kind: .daemonSet,
                    name: item.metadata.name,
                    namespace: item.metadata.namespace ?? namespace,
                    primaryText: "\(item.status.numberReady ?? 0)/\(item.status.desiredNumberScheduled ?? 0) ready",
                    secondaryText: "Daemon workload"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseServices(namespace: String, from raw: String) throws -> [ServiceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeServiceItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                ServiceSummary(
                    name: item.metadata.name,
                    namespace: item.metadata.namespace ?? namespace,
                    type: item.spec.type ?? "ClusterIP",
                    clusterIP: item.spec.clusterIP ?? "-"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseIngresses(namespace: String, from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeIngressItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                let host = item.spec.rules?.first?.host ?? "-"
                let address = item.status.loadBalancer?.ingress?.first?.hostname
                    ?? item.status.loadBalancer?.ingress?.first?.ip
                    ?? "No address"

                return ClusterResourceSummary(
                    kind: .ingress,
                    name: item.metadata.name,
                    namespace: item.metadata.namespace ?? namespace,
                    primaryText: host,
                    secondaryText: address
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseConfigMaps(namespace: String, from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeConfigMapItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                ClusterResourceSummary(
                    kind: .configMap,
                    name: item.metadata.name,
                    namespace: item.metadata.namespace ?? namespace,
                    primaryText: "\(item.data?.count ?? 0) keys",
                    secondaryText: "Config data"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseSecrets(namespace: String, from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeSecretItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                ClusterResourceSummary(
                    kind: .secret,
                    name: item.metadata.name,
                    namespace: item.metadata.namespace ?? namespace,
                    primaryText: item.type ?? "Opaque",
                    secondaryText: "\(item.data?.count ?? 0) values"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseNodes(from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeNodeItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                let ready = item.status.conditions?.last(where: { $0.type == "Ready" })?.status == "True" ? "Ready" : "Not Ready"
                return ClusterResourceSummary(
                    kind: .node,
                    name: item.metadata.name,
                    namespace: nil,
                    primaryText: ready,
                    secondaryText: item.status.nodeInfo?.kubeletVersion ?? "Unknown version"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseEvents(from raw: String) throws -> [EventSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeEventItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                EventSummary(
                    type: item.type ?? "Normal",
                    reason: item.reason ?? "Unknown",
                    objectName: item.involvedObject.name ?? "-",
                    message: item.message ?? ""
                )
            }
    }

    public func parseServiceSelector(from raw: String) throws -> [String: String] {
        let decoded = try JSONDecoder().decode(KubeServiceItem.self, from: Data(raw.utf8))
        return decoded.spec.selector ?? [:]
    }

    public func parseDeploymentSelector(from raw: String) throws -> [String: String] {
        let decoded = try JSONDecoder().decode(KubeDeploymentSelectorItem.self, from: Data(raw.utf8))
        return decoded.spec.selector?.matchLabels ?? [:]
    }

    private struct KubeList<Item: Decodable>: Decodable {
        let items: [Item]
    }

    private struct KubeMetadata: Decodable {
        let name: String
        let namespace: String?
    }

    private struct KubeDeploymentSpec: Decodable {
        let replicas: Int?
        let selector: KubeLabelSelector?
    }

    private struct KubeDeploymentStatus: Decodable {
        let readyReplicas: Int?
    }

    private struct KubeDaemonSetStatus: Decodable {
        let numberReady: Int?
        let desiredNumberScheduled: Int?
    }

    private struct KubeDeploymentItem: Decodable {
        let metadata: KubeMetadata
        let spec: KubeDeploymentSpec
        let status: KubeDeploymentStatus
    }

    private struct KubeDaemonSetItem: Decodable {
        let metadata: KubeMetadata
        let status: KubeDaemonSetStatus
    }

    private struct KubeDeploymentSelectorItem: Decodable {
        let spec: KubeDeploymentSpec
    }

    private struct KubeLabelSelector: Decodable {
        let matchLabels: [String: String]?
    }

    private struct KubeServiceSpec: Decodable {
        let type: String?
        let clusterIP: String?
        let selector: [String: String]?
    }

    private struct KubeServiceItem: Decodable {
        let metadata: KubeMetadata
        let spec: KubeServiceSpec
    }

    private struct KubeIngressRule: Decodable {
        let host: String?
    }

    private struct KubeIngressSpec: Decodable {
        let rules: [KubeIngressRule]?
    }

    private struct KubeLoadBalancerIngress: Decodable {
        let hostname: String?
        let ip: String?
    }

    private struct KubeLoadBalancerStatus: Decodable {
        let ingress: [KubeLoadBalancerIngress]?
    }

    private struct KubeIngressStatus: Decodable {
        let loadBalancer: KubeLoadBalancerStatus?
    }

    private struct KubeIngressItem: Decodable {
        let metadata: KubeMetadata
        let spec: KubeIngressSpec
        let status: KubeIngressStatus
    }

    private struct KubeConfigMapItem: Decodable {
        let metadata: KubeMetadata
        let data: [String: String]?
    }

    private struct KubeSecretItem: Decodable {
        let metadata: KubeMetadata
        let type: String?
        let data: [String: String]?
    }

    private struct KubeNodeCondition: Decodable {
        let type: String?
        let status: String?
    }

    private struct KubeNodeInfo: Decodable {
        let kubeletVersion: String?
    }

    private struct KubeNodeStatus: Decodable {
        let conditions: [KubeNodeCondition]?
        let nodeInfo: KubeNodeInfo?
    }

    private struct KubeNodeItem: Decodable {
        let metadata: KubeMetadata
        let status: KubeNodeStatus
    }

    private struct KubeEventObject: Decodable {
        let name: String?
    }

    private struct KubeEventItem: Decodable {
        let type: String?
        let reason: String?
        let message: String?
        let involvedObject: KubeEventObject
    }
}
