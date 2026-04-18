import Foundation

// MARK: - Kubernetes REST helpers (Rune-owned; matches upstream API path layout)

/// Query parameters for Kubernetes **list** calls (`limit`, pagination, selectors). Same names as the API query keys.
/// Rune issues these paths via the raw GET helper on ``KubectlCommandBuilder`` today; the same paths would work for a future in-process transport.
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

    fileprivate func appendingPercentEncoded(to path: String) -> String {
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

    /// REST collection path for a **namespaced** resource, or `nil` if Rune does not map this plural yet.
    public static func namespacedCollectionPath(namespace: String, resource: String) -> String? {
        let ns = encodedNamespaceSegment(namespace)
        switch resource {
        case "pods", "services", "configmaps", "secrets", "persistentvolumeclaims":
            return "/api/v1/namespaces/\(ns)/\(resource)"
        case "deployments", "statefulsets", "daemonsets", "replicasets":
            return "/apis/apps/v1/namespaces/\(ns)/\(resource)"
        case "jobs", "cronjobs":
            return "/apis/batch/v1/namespaces/\(ns)/\(resource)"
        case "ingresses":
            return "/apis/networking.k8s.io/v1/namespaces/\(ns)/ingresses"
        case "networkpolicies":
            return "/apis/networking.k8s.io/v1/namespaces/\(ns)/networkpolicies"
        case "horizontalpodautoscalers":
            // autoscaling/v2 is the common API for modern clusters; older clusters fall back via kubectl count.
            return "/apis/autoscaling/v2/namespaces/\(ns)/horizontalpodautoscalers"
        default:
            return nil
        }
    }

    /// Cluster-scoped collection path (e.g. nodes).
    public static func clusterCollectionPath(resource: String) -> String? {
        switch resource {
        case "nodes":
            return "/api/v1/nodes"
        default:
            return nil
        }
    }

    /// Cheap list probe: `limit=1` so `metadata.remainingItemCount` can imply total size (see ``KubectlListJSON/collectionListTotal(from:)``).
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
