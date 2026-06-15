import Foundation
import RuneCore

public enum AuthDoctorRBACPreflightScope: Equatable, Sendable {
    case namespace
    case cluster
}

public struct AuthDoctorRBACPreflightTarget: Sendable {
    public let id: String
    public let title: String
    public let verb: String
    public let resource: String
    public let apiGroup: String?
    public let subresource: String?
    public let actionTitle: String
    public let systemImage: String
    public let help: String
    public let destination: AuthDoctorEntryDestination
    public let scope: AuthDoctorRBACPreflightScope

    public init(
        id: String,
        title: String,
        verb: String = "list",
        resource: String,
        apiGroup: String? = nil,
        subresource: String? = nil,
        actionTitle: String,
        systemImage: String,
        help: String,
        destination: AuthDoctorEntryDestination,
        scope: AuthDoctorRBACPreflightScope = .namespace
    ) {
        self.id = id
        self.title = title
        self.verb = verb
        self.resource = resource
        self.apiGroup = apiGroup
        self.subresource = subresource
        self.actionTitle = actionTitle
        self.systemImage = systemImage
        self.help = help
        self.destination = destination
        self.scope = scope
    }

    public func namespace(activeNamespace: String) -> String? {
        switch scope {
        case .namespace:
            return activeNamespace
        case .cluster:
            return nil
        }
    }

    public static let emptyViewTargets: [AuthDoctorRBACPreflightTarget] = [
        .workload(
            id: "rbac-deployments-list",
            title: "RBAC deployments",
            resource: "deployments",
            apiGroup: "apps",
            kind: .deployment,
            systemImage: "square.stack.3d.up"
        ),
        .workload(
            id: "rbac-statefulsets-list",
            title: "RBAC StatefulSets",
            resource: "statefulsets",
            apiGroup: "apps",
            kind: .statefulSet,
            systemImage: "externaldrive.connected.to.line.below"
        ),
        .workload(
            id: "rbac-daemonsets-list",
            title: "RBAC DaemonSets",
            resource: "daemonsets",
            apiGroup: "apps",
            kind: .daemonSet,
            systemImage: "server.rack"
        ),
        .workload(
            id: "rbac-jobs-list",
            title: "RBAC Jobs",
            resource: "jobs",
            apiGroup: "batch",
            kind: .job,
            systemImage: "checklist"
        ),
        .workload(
            id: "rbac-cronjobs-list",
            title: "RBAC CronJobs",
            resource: "cronjobs",
            apiGroup: "batch",
            kind: .cronJob,
            systemImage: "clock.arrow.circlepath"
        ),
        .workload(
            id: "rbac-replicasets-list",
            title: "RBAC ReplicaSets",
            resource: "replicasets",
            apiGroup: "apps",
            kind: .replicaSet,
            systemImage: "rectangle.stack"
        ),
        .workload(
            id: "rbac-hpas-list",
            title: "RBAC HPAs",
            resource: "horizontalpodautoscalers",
            apiGroup: "autoscaling",
            kind: .horizontalPodAutoscaler,
            systemImage: "speedometer"
        ),
        .makeResource(
            id: "rbac-services-list",
            title: "RBAC services",
            resource: "services",
            actionTitle: "Open Services",
            systemImage: "point.3.connected.trianglepath.dotted",
            help: "Open namespace-scoped Services.",
            destination: .resource(section: .networking, kind: .service)
        ),
        .makeResource(
            id: "rbac-ingresses-list",
            title: "RBAC Ingresses",
            resource: "ingresses",
            apiGroup: "networking.k8s.io",
            actionTitle: "Open Ingresses",
            systemImage: "point.3.filled.connected.trianglepath.dotted",
            help: "Open namespace-scoped Ingresses.",
            destination: .resource(section: .networking, kind: .ingress)
        ),
        .makeResource(
            id: "rbac-networkpolicies-list",
            title: "RBAC NetworkPolicies",
            resource: "networkpolicies",
            apiGroup: "networking.k8s.io",
            actionTitle: "Open NetworkPolicies",
            systemImage: "lock.shield",
            help: "Open namespace-scoped NetworkPolicies.",
            destination: .resource(section: .networking, kind: .networkPolicy)
        ),
        .makeResource(
            id: "rbac-configmaps-list",
            title: "RBAC ConfigMaps",
            resource: "configmaps",
            actionTitle: "Open ConfigMaps",
            systemImage: "slider.horizontal.3",
            help: "Open namespace-scoped ConfigMaps.",
            destination: .resource(section: .config, kind: .configMap)
        ),
        .makeResource(
            id: "rbac-secrets-list",
            title: "RBAC Secrets",
            resource: "secrets",
            actionTitle: "Open Secrets",
            systemImage: "key",
            help: "Open namespace-scoped Secrets metadata. Auth Doctor only checks list permission.",
            destination: .resource(section: .config, kind: .secret)
        ),
        .makeResource(
            id: "rbac-pvcs-list",
            title: "RBAC PVCs",
            resource: "persistentvolumeclaims",
            actionTitle: "Open PVCs",
            systemImage: "externaldrive",
            help: "Open namespace-scoped PersistentVolumeClaims.",
            destination: .resource(section: .storage, kind: .persistentVolumeClaim)
        ),
        .clusterResource(
            id: "rbac-nodes-list",
            title: "RBAC Nodes",
            resource: "nodes",
            actionTitle: "Open Nodes",
            systemImage: "server.rack",
            help: "Open cluster-scoped Nodes.",
            destination: .resource(section: .storage, kind: .node)
        ),
        .clusterResource(
            id: "rbac-pvs-list",
            title: "RBAC PersistentVolumes",
            resource: "persistentvolumes",
            actionTitle: "Open PersistentVolumes",
            systemImage: "externaldrive.badge.checkmark",
            help: "Open cluster-scoped PersistentVolumes.",
            destination: .resource(section: .storage, kind: .persistentVolume)
        ),
        .clusterResource(
            id: "rbac-storageclasses-list",
            title: "RBAC StorageClasses",
            resource: "storageclasses",
            apiGroup: "storage.k8s.io",
            actionTitle: "Open StorageClasses",
            systemImage: "tray.2",
            help: "Open cluster-scoped StorageClasses.",
            destination: .resource(section: .storage, kind: .storageClass)
        ),
        .makeResource(
            id: "rbac-roles-list",
            title: "RBAC Roles",
            resource: "roles",
            apiGroup: "rbac.authorization.k8s.io",
            actionTitle: "Open Roles",
            systemImage: "person.badge.key",
            help: "Open namespace-scoped Roles.",
            destination: .resource(section: .rbac, kind: .role)
        ),
        .makeResource(
            id: "rbac-rolebindings-list",
            title: "RBAC RoleBindings",
            resource: "rolebindings",
            apiGroup: "rbac.authorization.k8s.io",
            actionTitle: "Open RoleBindings",
            systemImage: "link.badge.plus",
            help: "Open namespace-scoped RoleBindings.",
            destination: .resource(section: .rbac, kind: .roleBinding)
        ),
        .clusterResource(
            id: "rbac-clusterroles-list",
            title: "RBAC ClusterRoles",
            resource: "clusterroles",
            apiGroup: "rbac.authorization.k8s.io",
            actionTitle: "Open ClusterRoles",
            systemImage: "person.2.badge.key",
            help: "Open cluster-scoped ClusterRoles.",
            destination: .resource(section: .rbac, kind: .clusterRole)
        ),
        .clusterResource(
            id: "rbac-clusterrolebindings-list",
            title: "RBAC ClusterRoleBindings",
            resource: "clusterrolebindings",
            apiGroup: "rbac.authorization.k8s.io",
            actionTitle: "Open ClusterRoleBindings",
            systemImage: "link.badge.plus",
            help: "Open cluster-scoped ClusterRoleBindings.",
            destination: .resource(section: .rbac, kind: .clusterRoleBinding)
        ),
        .makeResource(
            id: "rbac-events-list",
            title: "RBAC events",
            resource: "events",
            actionTitle: "Open Events",
            systemImage: "calendar.badge.exclamationmark",
            help: "Open namespace-scoped Kubernetes events.",
            destination: .section(.events)
        )
    ]

    private static func clusterResource(
        id: String,
        title: String,
        resource: String,
        apiGroup: String? = nil,
        actionTitle: String,
        systemImage: String,
        help: String,
        destination: AuthDoctorEntryDestination
    ) -> AuthDoctorRBACPreflightTarget {
        makeResource(
            id: id,
            title: title,
            resource: resource,
            apiGroup: apiGroup,
            actionTitle: actionTitle,
            systemImage: systemImage,
            help: help,
            destination: destination,
            scope: .cluster
        )
    }

    public static func target(forCheckID id: String) -> AuthDoctorRBACPreflightTarget? {
        emptyViewTargets.first { $0.id == id }
    }

    private static func workload(
        id: String,
        title: String,
        resource: String,
        apiGroup: String,
        kind: KubeResourceKind,
        systemImage: String
    ) -> AuthDoctorRBACPreflightTarget {
        makeResource(
            id: id,
            title: title,
            resource: resource,
            apiGroup: apiGroup,
            actionTitle: "Open \(kind.title)",
            systemImage: systemImage,
            help: "Open namespace-scoped \(kind.title).",
            destination: .resource(section: .workloads, kind: kind)
        )
    }

    private static func makeResource(
        id: String,
        title: String,
        resource: String,
        apiGroup: String? = nil,
        actionTitle: String,
        systemImage: String,
        help: String,
        destination: AuthDoctorEntryDestination,
        scope: AuthDoctorRBACPreflightScope = .namespace
    ) -> AuthDoctorRBACPreflightTarget {
        AuthDoctorRBACPreflightTarget(
            id: id,
            title: title,
            resource: resource,
            apiGroup: apiGroup,
            actionTitle: actionTitle,
            systemImage: systemImage,
            help: help,
            destination: destination,
            scope: scope
        )
    }
}
