import Foundation
import RuneCore

public enum KubernetesResourceWatchEventType: String, Sendable, Equatable {
    case added = "ADDED"
    case modified = "MODIFIED"
    case deleted = "DELETED"
    case bookmark = "BOOKMARK"
    case error = "ERROR"
    case unknown
}

public struct KubernetesResourceWatchEvent: Sendable, Equatable {
    public let type: KubernetesResourceWatchEventType
    public let resourceVersion: String?
    public let errorMessage: String?

    public init(
        type: KubernetesResourceWatchEventType,
        resourceVersion: String? = nil,
        errorMessage: String? = nil
    ) {
        self.type = type
        self.resourceVersion = resourceVersion
        self.errorMessage = errorMessage
    }

    static func decode(line: String) throws -> KubernetesResourceWatchEvent {
        guard let data = line.data(using: .utf8),
              let envelope = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
            throw RuneError.parseError(message: "Kubernetes watch returned an invalid event envelope.")
        }
        let rawType = (envelope["type"] as? String)?.uppercased() ?? ""
        let type = KubernetesResourceWatchEventType(rawValue: rawType) ?? .unknown
        let object = envelope["object"] as? [String: Any]
        let metadata = object?["metadata"] as? [String: Any]
        let statusMessage = object?["message"] as? String
        return KubernetesResourceWatchEvent(
            type: type,
            resourceVersion: metadata?["resourceVersion"] as? String,
            errorMessage: type == .error ? statusMessage ?? "Kubernetes watch reported an error." : nil
        )
    }
}
