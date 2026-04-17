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
                let status = (parts.count > 1 ? String(parts[1]) : "Unknown")
                    .trimmingCharacters(in: .whitespacesAndNewlines)
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
                    status: String(parts[2]).trimmingCharacters(in: .whitespacesAndNewlines)
                )
            }
            .sorted {
                if $0.namespace != $1.namespace {
                    return $0.namespace.localizedCaseInsensitiveCompare($1.namespace) == .orderedAscending
                }
                return $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
            }
    }

    /// Fallback parser for `kubectl get pods ... -o custom-columns=NAME,STATUS,RESTARTS,CREATED`.
    public func parsePodsTable(namespace: String, from raw: String) -> [PodSummary] {
        raw
            .split(whereSeparator: \.isNewline)
            .compactMap { line in
                let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return nil }

                let parts = trimmed.split(whereSeparator: \.isWhitespace).map(String.init).filter { !$0.isEmpty }
                guard parts.count >= 2 else { return nil }

                let restartToken = parts.count >= 3 ? parts[2] : "0"
                let createdToken = parts.count >= 4 ? parts[3] : nil

                return PodSummary(
                    name: parts[0],
                    namespace: namespace,
                    status: parts[1],
                    totalRestarts: parseRestartCount(restartToken),
                    ageDescription: KubernetesAgeFormatting.describe(creationISO8601: createdToken),
                    cpuUsage: nil,
                    memoryUsage: nil
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    /// Fallback parser for `kubectl get pods -A ... -o custom-columns=NAMESPACE,NAME,STATUS,RESTARTS,CREATED`.
    public func parsePodsAllNamespacesTable(from raw: String) -> [PodSummary] {
        raw
            .split(whereSeparator: \.isNewline)
            .compactMap { line in
                let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return nil }

                let parts = trimmed.split(whereSeparator: \.isWhitespace).map(String.init).filter { !$0.isEmpty }
                guard parts.count >= 3 else { return nil }

                let restartToken = parts.count >= 4 ? parts[3] : "0"
                let createdToken = parts.count >= 5 ? parts[4] : nil

                return PodSummary(
                    name: parts[1],
                    namespace: parts[0],
                    status: parts[2],
                    totalRestarts: parseRestartCount(restartToken),
                    ageDescription: KubernetesAgeFormatting.describe(creationISO8601: createdToken),
                    cpuUsage: nil,
                    memoryUsage: nil
                )
            }
            .sorted {
                if $0.namespace != $1.namespace {
                    return $0.namespace.localizedCaseInsensitiveCompare($1.namespace) == .orderedAscending
                }
                return $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
            }
    }

    public func parsePodsListJSON(namespace: String, from raw: String) throws -> [PodSummary] {
        let decoded = try JSONDecoder().decode(KubePodList.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                let ns = item.metadata.namespace ?? namespace
                return podSummaryFromJSONItem(item, namespace: ns)
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parsePodsListJSONAllNamespaces(from raw: String) throws -> [PodSummary] {
        let decoded = try JSONDecoder().decode(KubePodList.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                let ns = item.metadata.namespace ?? ""
                return podSummaryFromJSONItem(item, namespace: ns)
            }
            .sorted {
                if $0.namespace != $1.namespace {
                    return $0.namespace.localizedCaseInsensitiveCompare($1.namespace) == .orderedAscending
                }
                return $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
            }
    }

    /// `kubectl top pods --no-headers` (namespaced): `NAME  CPU  MEM`.
    public func parsePodTopByName(from raw: String) -> [String: (cpu: String, memory: String)] {
        var result: [String: (cpu: String, memory: String)] = [:]
        for line in raw.split(whereSeparator: \.isNewline) {
            let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else { continue }
            let parts = trimmed.split(whereSeparator: \.isWhitespace).map(String.init).filter { !$0.isEmpty }
            guard parts.count >= 3 else { continue }
            result[parts[0]] = (cpu: parts[1], memory: parts[2])
        }
        return result
    }

    /// `kubectl top pods -A --no-headers`: `NAMESPACE  NAME  CPU  MEM`.
    public func parsePodTopByNamespaceAndName(from raw: String) -> [String: (cpu: String, memory: String)] {
        var result: [String: (cpu: String, memory: String)] = [:]
        for line in raw.split(whereSeparator: \.isNewline) {
            let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else { continue }
            let parts = trimmed.split(whereSeparator: \.isWhitespace).map(String.init).filter { !$0.isEmpty }
            guard parts.count >= 4 else { continue }
            let key = "\(parts[0])/\(parts[1])"
            result[key] = (cpu: parts[2], memory: parts[3])
        }
        return result
    }

    /// `kubectl top nodes --no-headers`: `NAME CPU(cores) CPU% MEMORY(bytes) MEMORY%`.
    /// Returns average CPU% and MEM% over all parsed nodes.
    public func parseNodeTopUsagePercent(from raw: String) -> (cpuPercent: Int?, memoryPercent: Int?) {
        var cpuValues: [Int] = []
        var memoryValues: [Int] = []

        for line in raw.split(whereSeparator: \.isNewline) {
            let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else { continue }

            let parts = trimmed.split(whereSeparator: \.isWhitespace).map(String.init).filter { !$0.isEmpty }
            guard parts.count >= 5 else { continue }

            if let cpu = parsePercent(parts[2]) {
                cpuValues.append(cpu)
            }
            if let memory = parsePercent(parts[4]) {
                memoryValues.append(memory)
            }
        }

        let cpuAverage = cpuValues.isEmpty ? nil : Int(round(Double(cpuValues.reduce(0, +)) / Double(cpuValues.count)))
        let memoryAverage = memoryValues.isEmpty ? nil : Int(round(Double(memoryValues.reduce(0, +)) / Double(memoryValues.count)))
        return (cpuAverage, memoryAverage)
    }

    private func parsePercent(_ token: String) -> Int? {
        let cleaned = token.replacingOccurrences(of: "%", with: "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return Int(cleaned)
    }

    private func parseRestartCount(_ token: String?) -> Int {
        guard let token else { return 0 }
        return token
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .compactMap(Int.init)
            .reduce(0, +)
    }

    private func parseOptionalInt(_ token: String?) -> Int? {
        guard let token else { return nil }
        let cleaned = token.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !cleaned.isEmpty, cleaned != "<none>" else { return nil }
        return Int(cleaned)
    }

    private func podSummaryFromJSONItem(_ item: KubePodItem, namespace: String) -> PodSummary {
        let phase = (item.status?.phase ?? "Unknown").trimmingCharacters(in: .whitespacesAndNewlines)
        let restarts = restartSum(from: item.status)
        let age = KubernetesAgeFormatting.describe(creationISO8601: item.metadata.creationTimestamp)
        let spec = item.spec
        let st = item.status
        let containersReady = containersReadySummary(spec: spec, status: st)
        let names = spec?.containers?.map(\.name).filter { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }
        let containerNamesLine = names.map { $0.joined(separator: ", ") }
        func nonEmpty(_ s: String?) -> String? {
            let t = s?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            return t.isEmpty ? nil : t
        }
        return PodSummary(
            name: item.metadata.name,
            namespace: namespace,
            status: phase,
            totalRestarts: restarts,
            ageDescription: age,
            cpuUsage: nil,
            memoryUsage: nil,
            podIP: nonEmpty(st?.podIP),
            hostIP: nonEmpty(st?.hostIP),
            nodeName: nonEmpty(spec?.nodeName),
            qosClass: nonEmpty(st?.qosClass),
            containersReady: containersReady,
            containerNamesLine: nonEmpty(containerNamesLine)
        )
    }

    private func containersReadySummary(spec: KubePodSpec?, status: KubePodRowStatus?) -> String? {
        guard let total = spec?.containers?.count, total > 0 else { return nil }
        let ready = status?.containerStatuses?.filter { $0.ready == true }.count ?? 0
        return "\(ready)/\(total)"
    }

    private func restartSum(from status: KubePodRowStatus?) -> Int {
        guard let status else { return 0 }
        let regular = status.containerStatuses?.reduce(0) { $0 + ($1.restartCount ?? 0) } ?? 0
        let inits = status.initContainerStatuses?.reduce(0) { $0 + ($1.restartCount ?? 0) } ?? 0
        return regular + inits
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

    /// Fallback parser for `kubectl get deployments ... -o custom-columns=NAME,READY,DESIRED`.
    public func parseDeploymentsTable(namespace: String, from raw: String) -> [DeploymentSummary] {
        raw
            .split(whereSeparator: \.isNewline)
            .compactMap { line in
                let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return nil }

                let parts = trimmed.split(whereSeparator: \.isWhitespace).map(String.init).filter { !$0.isEmpty }
                guard parts.count >= 1 else { return nil }

                let ready = parseOptionalInt(parts.count >= 2 ? parts[1] : nil) ?? 0
                let desired = parseOptionalInt(parts.count >= 3 ? parts[2] : nil) ?? 0

                return DeploymentSummary(
                    name: parts[0],
                    namespace: namespace,
                    readyReplicas: ready,
                    desiredReplicas: desired
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    /// Fallback parser for `kubectl get deployments -A ... -o custom-columns=NAMESPACE,NAME,READY,DESIRED`.
    public func parseDeploymentsAllNamespacesTable(from raw: String) -> [DeploymentSummary] {
        raw
            .split(whereSeparator: \.isNewline)
            .compactMap { line in
                let trimmed = String(line).trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return nil }

                let parts = trimmed.split(whereSeparator: \.isWhitespace).map(String.init).filter { !$0.isEmpty }
                guard parts.count >= 2 else { return nil }

                let ready = parseOptionalInt(parts.count >= 3 ? parts[2] : nil) ?? 0
                let desired = parseOptionalInt(parts.count >= 4 ? parts[3] : nil) ?? 0

                return DeploymentSummary(
                    name: parts[1],
                    namespace: parts[0],
                    readyReplicas: ready,
                    desiredReplicas: desired
                )
            }
            .sorted {
                if $0.namespace != $1.namespace {
                    return $0.namespace.localizedCaseInsensitiveCompare($1.namespace) == .orderedAscending
                }
                return $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
            }
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
                let stamp = item.lastTimestamp ?? item.firstTimestamp
                return EventSummary(
                    type: item.type ?? "Normal",
                    reason: item.reason ?? "Unknown",
                    objectName: item.involvedObject.name ?? "-",
                    message: item.message ?? "",
                    lastTimestamp: stamp,
                    involvedKind: item.involvedObject.kind,
                    involvedNamespace: item.involvedObject.namespace
                )
            }
    }

    public func parseClusterRoleNames(from raw: String) -> [ClusterResourceSummary] {
        raw
            .split(whereSeparator: \.isNewline)
            .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .map { name in
                ClusterResourceSummary(
                    kind: .clusterRole,
                    name: name,
                    namespace: nil,
                    primaryText: "—",
                    secondaryText: "Cluster role"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseClusterRoleBindingNames(from raw: String) -> [ClusterResourceSummary] {
        raw
            .split(whereSeparator: \.isNewline)
            .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .map { name in
                ClusterResourceSummary(
                    kind: .clusterRoleBinding,
                    name: name,
                    namespace: nil,
                    primaryText: "—",
                    secondaryText: "Cluster role binding"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseRoles(namespace: String, from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeRoleItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                let ruleCount = item.rules?.count ?? 0
                return ClusterResourceSummary(
                    kind: .role,
                    name: item.metadata.name,
                    namespace: item.metadata.namespace ?? namespace,
                    primaryText: "\(ruleCount) rules",
                    secondaryText: "Namespaced role"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseRoleBindings(namespace: String, from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeRoleBindingItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                let subjectCount = item.subjects?.count ?? 0
                let refName = item.roleRef.name ?? "-"
                return ClusterResourceSummary(
                    kind: .roleBinding,
                    name: item.metadata.name,
                    namespace: item.metadata.namespace ?? namespace,
                    primaryText: "→ \(refName)",
                    secondaryText: "\(subjectCount) subject(s)"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseClusterRoles(from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeClusterRoleItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                let ruleCount = item.rules?.count ?? 0
                return ClusterResourceSummary(
                    kind: .clusterRole,
                    name: item.metadata.name,
                    namespace: nil,
                    primaryText: "\(ruleCount) rules",
                    secondaryText: "Cluster role"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    public func parseClusterRoleBindings(from raw: String) throws -> [ClusterResourceSummary] {
        let decoded = try JSONDecoder().decode(KubeList<KubeClusterRoleBindingItem>.self, from: Data(raw.utf8))
        return decoded.items
            .map { item in
                let subjectCount = item.subjects?.count ?? 0
                let refName = item.roleRef.name ?? "-"
                return ClusterResourceSummary(
                    kind: .clusterRoleBinding,
                    name: item.metadata.name,
                    namespace: nil,
                    primaryText: "→ \(refName)",
                    secondaryText: "\(subjectCount) subject(s)"
                )
            }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
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

    private struct KubePodList: Decodable {
        let items: [KubePodItem]

        init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            items = try c.decodeIfPresent([KubePodItem].self, forKey: .items) ?? []
        }

        enum CodingKeys: String, CodingKey {
            case items
        }
    }

    private struct KubePodItem: Decodable {
        let metadata: KubePodRowMetadata
        let spec: KubePodSpec?
        let status: KubePodRowStatus?
    }

    private struct KubePodSpec: Decodable {
        let nodeName: String?
        let containers: [KubePodSpecContainer]?
    }

    private struct KubePodSpecContainer: Decodable {
        let name: String
    }

    private struct KubePodRowMetadata: Decodable {
        let name: String
        let namespace: String?
        let creationTimestamp: String?
    }

    private struct KubePodRowStatus: Decodable {
        let phase: String?
        let podIP: String?
        let hostIP: String?
        let qosClass: String?
        let containerStatuses: [KubePodContainerStatus]?
        let initContainerStatuses: [KubePodContainerStatus]?
    }

    private struct KubePodContainerStatus: Decodable {
        let name: String?
        let ready: Bool?
        let restartCount: Int?
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
        let kind: String?
        let name: String?
        let namespace: String?
    }

    private struct KubeEventItem: Decodable {
        let type: String?
        let reason: String?
        let message: String?
        let lastTimestamp: String?
        let firstTimestamp: String?
        let involvedObject: KubeEventObject
    }

    private struct KubePolicyRule: Decodable {}

    private struct KubeRoleRef: Decodable {
        let kind: String?
        let name: String?
    }

    private struct KubeSubject: Decodable {
        let kind: String?
        let name: String?
    }

    private struct KubeRoleItem: Decodable {
        let metadata: KubeMetadata
        let rules: [KubePolicyRule]?
    }

    private struct KubeClusterRoleItem: Decodable {
        let metadata: KubeMetadata
        let rules: [KubePolicyRule]?
    }

    private struct KubeRoleBindingItem: Decodable {
        let metadata: KubeMetadata
        let subjects: [KubeSubject]?
        let roleRef: KubeRoleRef
    }

    private struct KubeClusterRoleBindingItem: Decodable {
        let metadata: KubeMetadata
        let subjects: [KubeSubject]?
        let roleRef: KubeRoleRef
    }
}
