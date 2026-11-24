import Foundation
import RuneCore

public enum KubernetesResourceWatchEventType: String, Sendable, Equatable {
    case connected
    case added = "ADDED"
    case modified = "MODIFIED"
    case deleted = "DELETED"
    case bookmark = "BOOKMARK"
    case error = "ERROR"
    case unknown
}

public enum KubernetesResourceWatchError: LocalizedError, Sendable {
    case resourceVersionExpired

    public var errorDescription: String? {
        "Kubernetes watch failed with HTTP status 410. The resource version expired."
    }
}

public struct KubernetesResourceWatchEvent: Sendable, Equatable {
    public let type: KubernetesResourceWatchEventType
    public let resourceVersion: String?
    public let errorMessage: String?
    public let errorStatusCode: Int?

    public init(
        type: KubernetesResourceWatchEventType,
        resourceVersion: String? = nil,
        errorMessage: String? = nil,
        errorStatusCode: Int? = nil
    ) {
        self.type = type
        self.resourceVersion = resourceVersion
        self.errorMessage = errorMessage
        self.errorStatusCode = errorStatusCode
    }

    /// An object may be a few megabytes with its managed fields; far past
    /// that, the stream is not a watch.
    static let maximumLineBytes = 64 * 1024 * 1024

    /// The event on one line of a watch stream; nil for a blank line.
    static func decodeLine(_ line: Data) throws -> KubernetesResourceWatchEvent? {
        let isBlank = line.allSatisfy { $0 == 0x20 || $0 == 0x09 || $0 == 0x0D }
        return isBlank ? nil : try decode(data: line)
    }

    static func decode(line: String) throws -> KubernetesResourceWatchEvent {
        try decode(data: Data(line.utf8))
    }

    static func decode(data: Data) throws -> KubernetesResourceWatchEvent {
        guard let envelope = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
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
            errorMessage: type == .error ? statusMessage ?? "Kubernetes watch reported an error." : nil,
            errorStatusCode: type == .error ? object?["code"] as? Int : nil
        )
    }
}
