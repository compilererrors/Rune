import Foundation
import RuneCore

public enum AuthDoctorEntryDestination: Equatable, Sendable {
    case section(RuneSection)
    case resource(section: RuneSection, kind: KubeResourceKind)
    case podLogs
    case podExec
    case podPortForward
    case kubeconfigReview
    case documentation(URL)
}

public struct AuthDoctorEntryResolution: Equatable, Sendable {
    public let title: String
    public let systemImage: String
    public let help: String
    public let destination: AuthDoctorEntryDestination
}

public enum AuthDoctorEntryActionResolver {
    public static func resolve(check: RuneHealthCheck, hasPodTarget: Bool) -> AuthDoctorEntryResolution? {
        switch check.id {
        case "pod-list", "rbac-pods-list":
            return .init(
                title: "Open Pods",
                systemImage: "cube.box",
                help: "Open the Pods list for the active namespace.",
                destination: .resource(section: .workloads, kind: .pod)
            )

        case "pod-logs" where hasPodTarget,
             "rbac-pod-logs" where hasPodTarget:
            return .init(
                title: "Open Logs",
                systemImage: "doc.text.magnifyingglass",
                help: "Open logs for a pod in the active namespace.",
                destination: .podLogs
            )

        case "rbac-pod-exec" where hasPodTarget:
            return .init(
                title: "Open Exec",
                systemImage: "terminal",
                help: "Open the pod exec panel for a pod in the active namespace.",
                destination: .podExec
            )

        case "rbac-port-forward" where hasPodTarget:
            return .init(
                title: "Open Port Forward",
                systemImage: "arrow.left.and.right",
                help: "Open port-forward controls for a pod in the active namespace.",
                destination: .podPortForward
            )

        case "namespace", "namespace-list":
            return .init(
                title: "Open Workloads",
                systemImage: "cube.box",
                help: "Open the namespace-scoped workloads view for the active context.",
                destination: .resource(section: .workloads, kind: .pod)
            )

        case "kubeconfig", "kubeconfig-files", "contexts", "selected-context", "context-namespace", "auth-provider-profile":
            return .init(
                title: "Review Config",
                systemImage: "doc.text.magnifyingglass",
                help: "Open the kubeconfig import review in Add Cluster.",
                destination: .kubeconfigReview
            )

        default:
            if let target = AuthDoctorRBACPreflightTarget.target(forCheckID: check.id) {
                return .init(
                    title: target.actionTitle,
                    systemImage: target.systemImage,
                    help: target.help,
                    destination: target.destination
                )
            }
            guard let url = documentationURL(for: check) else { return nil }
            return .init(
                title: "Docs",
                systemImage: "book",
                help: "Open official Kubernetes documentation for this check.",
                destination: .documentation(url)
            )
        }
    }

    public static func documentationURL(for check: RuneHealthCheck) -> URL? {
        let path: String
        switch check.id {
        case "kubeconfig", "kubeconfig-files", "contexts", "selected-context", "context-namespace", "auth-provider-profile":
            path = "https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/"
        case "exec-auth", "exec-auth-cache", "exec-auth-profile", "exec-auth-tools", "cloud-login-tools", "api-auth", "client-certificate-auth":
            path = "https://kubernetes.io/docs/reference/access-authn-authz/authentication/#client-go-credential-plugins"
        case "transport", "proxy-profile", "custom-ca-profile":
            path = "https://kubernetes.io/docs/tasks/administer-cluster/access-cluster-api/"
        case "api-authorization":
            path = "https://kubernetes.io/docs/reference/access-authn-authz/rbac/"
        case "pod-list":
            path = "https://kubernetes.io/docs/concepts/workloads/pods/"
        case "pod-logs":
            path = "https://kubernetes.io/docs/reference/kubectl/generated/kubectl_logs/"
        case let id where id.hasPrefix("rbac-") && AuthDoctorRBACPreflightTarget.target(forCheckID: id) != nil:
            path = "https://kubernetes.io/docs/reference/access-authn-authz/rbac/"
        case "rbac-access-summary", "rbac-pods-list", "rbac-pod-logs", "rbac-pod-exec", "rbac-port-forward":
            path = "https://kubernetes.io/docs/reference/access-authn-authz/rbac/"
        case "namespace", "namespace-list":
            path = "https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/"
        default:
            return nil
        }
        return URL(string: path)
    }
}
