import RuneCore

/// Names for the two summary fields of `ClusterResourceSummary`, whose meaning
/// depends on the resource kind (for a PVC they are phase and capacity).
struct ResourceSummaryFieldLabel: Equatable, Sendable {
    let title: String
    let symbol: String
}

extension KubeResourceKind {
    var summaryFieldLabels: (primary: ResourceSummaryFieldLabel, secondary: ResourceSummaryFieldLabel) {
        switch self {
        case .statefulSet, .daemonSet, .replicaSet:
            return (.init(title: "Ready", symbol: "checkmark.circle"), .init(title: "Selector", symbol: "line.3.horizontal.decrease.circle"))
        case .job:
            return (.init(title: "Status", symbol: "checkmark.seal"), .init(title: "Progress", symbol: "chart.bar"))
        case .cronJob:
            return (.init(title: "Schedule", symbol: "calendar.badge.clock"), .init(title: "State", symbol: "pause.circle"))
        case .persistentVolumeClaim:
            return (.init(title: "Status", symbol: "checkmark.seal"), .init(title: "Capacity", symbol: "internaldrive"))
        case .persistentVolume:
            return (.init(title: "Status", symbol: "checkmark.seal"), .init(title: "Capacity", symbol: "internaldrive"))
        case .storageClass:
            return (.init(title: "Provisioner", symbol: "gearshape"), .init(title: "Default", symbol: "star"))
        case .horizontalPodAutoscaler:
            return (.init(title: "Replicas", symbol: "square.stack.3d.up"), .init(title: "Target", symbol: "scope"))
        case .networkPolicy:
            return (.init(title: "Policy types", symbol: "shield"), .init(title: "Pod selector", symbol: "line.3.horizontal.decrease.circle"))
        case .endpoint:
            return (.init(title: "Ready", symbol: "checkmark.circle"), .init(title: "Ports", symbol: "point.3.connected.trianglepath.dotted"))
        case .ingress:
            return (.init(title: "Host", symbol: "globe"), .init(title: "Backend", symbol: "arrow.triangle.branch"))
        case .configMap:
            return (.init(title: "Keys", symbol: "key"), .init(title: "Values", symbol: "doc.text"))
        case .secret:
            return (.init(title: "Type", symbol: "lock"), .init(title: "Values", symbol: "number"))
        case .node:
            return (.init(title: "Status", symbol: "checkmark.seal"), .init(title: "Kubelet", symbol: "cpu"))
        case .serviceAccount:
            return (.init(title: "Secrets", symbol: "key"), .init(title: "Pull secrets", symbol: "arrow.down.circle"))
        case .role, .clusterRole:
            return (.init(title: "Rules", symbol: "list.bullet"), .init(title: "Scope", symbol: "square.stack.3d.up"))
        case .roleBinding, .clusterRoleBinding:
            return (.init(title: "Role", symbol: "person.badge.key"), .init(title: "Subjects", symbol: "person.2"))
        case .pod, .deployment, .service, .event:
            return (.init(title: "Summary", symbol: "info.circle"), .init(title: "Details", symbol: "text.alignleft"))
        }
    }
}
