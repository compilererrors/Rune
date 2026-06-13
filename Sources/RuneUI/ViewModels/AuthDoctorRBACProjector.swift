import Foundation
import RuneCore

public struct AuthDoctorRBACCapability: Hashable, Sendable {
    public let id: String
    public let title: String
    public let verb: String
    public let resource: String
    public let apiGroup: String?
    public let subresource: String?
    public let allowed: Bool

    public init(
        id: String,
        title: String,
        verb: String,
        resource: String,
        apiGroup: String? = nil,
        subresource: String? = nil,
        allowed: Bool
    ) {
        self.id = id
        self.title = title
        self.verb = verb
        self.resource = resource
        self.apiGroup = apiGroup
        self.subresource = subresource
        self.allowed = allowed
    }

    public var target: String {
        let group = apiGroup?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let groupPrefix = group.isEmpty ? "" : "\(group)/"
        return "\(groupPrefix)\(resource)\(subresource.map { "/" + $0 } ?? "")"
    }

    fileprivate var deniedCapabilityLabel: String {
        switch (resource, subresource) {
        case ("pods", nil):
            return "pod listing"
        case ("deployments", nil):
            return "deployment listing"
        case ("pods", "log"):
            return "logs"
        case ("pods", "exec"):
            return "exec"
        case ("pods", "portforward"):
            return "port-forward"
        default:
            return target
        }
    }
}

public enum AuthDoctorRBACProjector {
    public static func check(for capability: AuthDoctorRBACCapability, namespace: String?) -> RuneHealthCheck {
        let scope = namespace ?? "cluster scope"
        return RuneHealthCheck(
            id: capability.id,
            title: capability.title,
            status: capability.allowed ? .passed : .warning,
            message: capability.allowed
                ? "RBAC allows \(capability.verb) \(capability.target) in \(scope)."
                : "RBAC denied \(capability.verb) \(capability.target) in \(scope)."
        )
    }

    public static func accessSummary(namespace: String, capabilities: [AuthDoctorRBACCapability]) -> RuneHealthCheck? {
        guard let podList = capabilities.first(where: { $0.resource == "pods" && $0.subresource == nil && $0.verb == "list" }) else {
            return nil
        }

        if !podList.allowed {
            return RuneHealthCheck(
                id: "rbac-access-summary",
                title: "RBAC access summary",
                status: .failed,
                message: "RBAC denied pod listing in namespace \(namespace). Workloads can appear empty even when pods exist."
            )
        }

        let denied = capabilities
            .filter { !$0.allowed && !($0.resource == "pods" && $0.subresource == nil && $0.verb == "list") }
            .map(\.deniedCapabilityLabel)

        if denied.isEmpty, !hasCompletePodMatrix(capabilities) {
            return nil
        }

        if denied.isEmpty {
            return RuneHealthCheck(
                id: "rbac-access-summary",
                title: "RBAC access summary",
                status: .passed,
                message: "RBAC allows pod listing, logs, exec, and port-forward in namespace \(namespace)."
            )
        }

        return RuneHealthCheck(
            id: "rbac-access-summary",
            title: "RBAC access summary",
            status: .warning,
            message: "Partial pod access in namespace \(namespace): pods can be listed, but \(listPhrase(denied)) denied."
        )
    }

    private static func hasCompletePodMatrix(_ capabilities: [AuthDoctorRBACCapability]) -> Bool {
        let keys = Set(capabilities.map { "\($0.verb):\($0.resource):\($0.subresource ?? "")" })
        return keys.isSuperset(of: [
            "list:pods:",
            "get:pods:log",
            "create:pods:exec",
            "create:pods:portforward"
        ])
    }

    private static func listPhrase(_ values: [String]) -> String {
        switch values.count {
        case 0:
            return ""
        case 1:
            return "\(values[0]) is"
        case 2:
            return "\(values[0]) and \(values[1]) are"
        default:
            return "\(values.dropLast().joined(separator: ", ")), and \(values.last ?? "") are"
        }
    }
}
