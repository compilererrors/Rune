import Foundation
import RuneCore

// MARK: - Kubernetes REST helpers (Rune-owned; matches upstream API path layout)

/// Query parameters for Kubernetes **list** calls (`limit`, pagination, selectors). Same names as the API query keys.
/// Rune uses these paths directly through its owned REST client.
public struct KubernetesListOptions: Sendable, Hashable {
    public var limit: Int?
    public var continueToken: String?
    public var fieldSelector: String?
    public var labelSelector: String?

    public init(
        limit: Int? = nil,
        continueToken: String? = nil,
        fieldSelector: String? = nil,
        labelSelector: String? = nil
    ) {
        self.limit = limit
        self.continueToken = continueToken
        self.fieldSelector = fieldSelector
        self.labelSelector = labelSelector
    }

    /// Kubernetes resource plural segment as Rune uses in API paths (e.g. `pods`, `deployments`, `cronjobs`).
    public static func metadataProbeLimitOne() -> KubernetesListOptions {
        KubernetesListOptions(limit: 1)
    }

    public func appendingPercentEncoded(to path: String) -> String {
        guard path.hasPrefix("/") else { return path }
        var items: [URLQueryItem] = []
        if let limit {
            items.append(URLQueryItem(name: "limit", value: String(limit)))
        }
        if let continueToken, !continueToken.isEmpty {
            items.append(URLQueryItem(name: "continue", value: continueToken))
        }
        if let fieldSelector, !fieldSelector.isEmpty {
            items.append(URLQueryItem(name: "fieldSelector", value: fieldSelector))
        }
        if let labelSelector, !labelSelector.isEmpty {
            items.append(URLQueryItem(name: "labelSelector", value: labelSelector))
        }
        guard !items.isEmpty else { return path }
        var components = URLComponents()
        components.scheme = "https"
        components.host = "kubernetes.default.svc"
        components.path = path
        components.queryItems = items
        let pathOut = components.percentEncodedPath
        let query = components.percentEncodedQuery.map { "?\($0)" } ?? ""
        return pathOut + query
    }
}

/// Typed description of a Kubernetes **collection** GET (read-only). Holds the path and query string Rune passes to a raw list request.
public struct KubernetesRESTRequest: Sendable, Hashable {
    public var apiPath: String

    public init(apiPath: String) {
        self.apiPath = apiPath
    }
}

/// Builds REST paths matching the upstream Kubernetes API layout (group/version/prefix).
public enum KubernetesRESTPath {
    private static func encodedNamespaceSegment(_ namespace: String) -> String {
        namespace.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? namespace
    }

    private static func encodedNameSegment(_ name: String) -> String {
        name.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? name
    }

    private static func apiBasePath(resource: String, namespace: String?) -> String? {
        if let namespace {
            return namespacedCollectionPath(namespace: namespace, resource: resource)
        }

        if let clusterScoped = clusterCollectionPath(resource: resource) {
            return clusterScoped
        }

        switch resource {
        case "pods", "services", "configmaps", "secrets", "persistentvolumeclaims", "events":
            return "/api/v1/\(resource)"
        case "deployments", "statefulsets", "daemonsets", "replicasets":
            return "/apis/apps/v1/\(resource)"
        case "jobs", "cronjobs":
            return "/apis/batch/v1/\(resource)"
        case "ingresses", "networkpolicies":
            return "/apis/networking.k8s.io/v1/\(resource)"
        case "horizontalpodautoscalers":
            return "/apis/autoscaling/v2/\(resource)"
        case "roles", "rolebindings":
            return "/apis/rbac.authorization.k8s.io/v1/\(resource)"
        default:
            return nil
        }
    }

    /// REST collection path for a **namespaced** resource, or `nil` if Rune does not map this plural yet.
    public static func namespacedCollectionPath(namespace: String, resource: String) -> String? {
        let ns = encodedNamespaceSegment(namespace)
        guard let base = apiBasePath(resource: resource, namespace: nil) else { return nil }

        if base.hasPrefix("/api/") {
            return "/api/v1/namespaces/\(ns)/\(resource)"
        }

        guard let resourceIndex = base.lastIndex(of: "/") else { return nil }
        let prefix = String(base[..<resourceIndex])
        return "\(prefix)/namespaces/\(ns)/\(resource)"
    }

    /// Collection path without namespace scoping (all namespaces for namespaced resources, cluster scope for cluster resources).
    public static func collectionPath(resource: String, namespace: String?) -> String? {
        apiBasePath(resource: resource, namespace: namespace)
    }

    public static func resourcePath(
        namespace: String?,
        resource: String,
        name: String,
        subresource: String? = nil
    ) -> String? {
        guard let base = collectionPath(resource: resource, namespace: namespace) else { return nil }
        let encodedName = encodedNameSegment(name)
        if let subresource, !subresource.isEmpty {
            return "\(base)/\(encodedName)/\(encodedNameSegment(subresource))"
        }
        return "\(base)/\(encodedName)"
    }

    public static func resourceName(for kind: KubeResourceKind) -> String {
        switch kind {
        case .pod: return "pods"
        case .deployment: return "deployments"
        case .statefulSet: return "statefulsets"
        case .daemonSet: return "daemonsets"
        case .job: return "jobs"
        case .cronJob: return "cronjobs"
        case .replicaSet: return "replicasets"
        case .service: return "services"
        case .ingress: return "ingresses"
        case .configMap: return "configmaps"
        case .secret: return "secrets"
        case .node: return "nodes"
        case .event: return "events"
        case .role: return "roles"
        case .roleBinding: return "rolebindings"
        case .clusterRole: return "clusterroles"
        case .clusterRoleBinding: return "clusterrolebindings"
        case .persistentVolumeClaim: return "persistentvolumeclaims"
        case .persistentVolume: return "persistentvolumes"
        case .storageClass: return "storageclasses"
        case .horizontalPodAutoscaler: return "horizontalpodautoscalers"
        case .networkPolicy: return "networkpolicies"
        }
    }

    /// Cluster-scoped collection path (e.g. nodes).
    public static func clusterCollectionPath(resource: String) -> String? {
        switch resource {
        case "namespaces":
            return "/api/v1/namespaces"
        case "nodes":
            return "/api/v1/nodes"
        case "persistentvolumes":
            return "/api/v1/persistentvolumes"
        case "storageclasses":
            return "/apis/storage.k8s.io/v1/storageclasses"
        case "clusterroles", "clusterrolebindings":
            return "/apis/rbac.authorization.k8s.io/v1/\(resource)"
        default:
            return nil
        }
    }

    /// Cheap list probe: `limit=1` so `metadata.remainingItemCount` can imply total size (see ``KubernetesListJSON/collectionListTotal(from:)``).
    public static func namespacedCollectionMetadataProbe(namespace: String, resource: String) -> String? {
        guard let base = namespacedCollectionPath(namespace: namespace, resource: resource) else { return nil }
        return KubernetesListOptions.metadataProbeLimitOne().appendingPercentEncoded(to: base)
    }

    public static func clusterCollectionMetadataProbe(resource: String) -> String? {
        guard let base = clusterCollectionPath(resource: resource) else { return nil }
        return KubernetesListOptions.metadataProbeLimitOne().appendingPercentEncoded(to: base)
    }

    /// Generic list GET with arbitrary options (watch and future streaming use the same path discipline).
    public static func namespacedCollectionRequest(
        namespace: String,
        resource: String,
        options: KubernetesListOptions
    ) -> KubernetesRESTRequest? {
        guard let base = namespacedCollectionPath(namespace: namespace, resource: resource) else { return nil }
        return KubernetesRESTRequest(apiPath: options.appendingPercentEncoded(to: base))
    }

    public static func clusterCollectionRequest(resource: String, options: KubernetesListOptions) -> KubernetesRESTRequest? {
        guard let base = clusterCollectionPath(resource: resource) else { return nil }
        return KubernetesRESTRequest(apiPath: options.appendingPercentEncoded(to: base))
    }
}
