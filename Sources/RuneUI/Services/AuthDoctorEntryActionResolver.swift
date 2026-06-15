import Foundation
import RuneCore

public enum AuthDoctorEntryDestination: Equatable, Sendable {
    case section(RuneSection)
    case resource(section: RuneSection, kind: KubeResourceKind)
    case rbacCanIPreset(
        verb: String,
        resource: String,
        apiGroup: String?,
        subresource: String?,
        scope: AuthDoctorRBACPreflightScope
    )
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
    private static let providerLoginDocumentationCheckIDs: Set<String> = [
        "cloud-login-tools",
        "eks-role-profile",
        "gke-auth-plugin-profile",
        "aks-kubelogin-profile",
        "doks-doctl-profile",
        "rancher-cli-profile",
        "openshift-cli-profile"
    ]

    private static let execAuthDocumentationCheckIDs: Set<String> = providerLoginDocumentationCheckIDs.union([
        "exec-auth",
        "exec-auth-cache",
        "exec-auth-profile",
        "exec-auth-tools"
    ])

    public static func resolve(check: RuneHealthCheck, hasPodTarget: Bool) -> AuthDoctorEntryResolution? {
        switch check.id {
        case "pod-list", "rbac-pods-list":
            return .init(
                title: "Open Pods",
                systemImage: "cube.box",
                help: "Open the Pods list for the active namespace.",
                destination: .resource(section: .workloads, kind: .pod)
            )

        case "pod-logs" where hasPodTarget:
            return .init(
                title: "Open Logs",
                systemImage: "doc.text.magnifyingglass",
                help: "Open logs for a pod in the active namespace.",
                destination: .podLogs
            )

        case "pod-logs":
            return .init(
                title: "Open Pods",
                systemImage: "cube.box",
                help: "Open the Pods list first, then choose a pod to inspect logs.",
                destination: .resource(section: .workloads, kind: .pod)
            )

        case "rbac-pod-logs" where check.status != .passed:
            return rbacCanIAction(
                verb: "get",
                resource: "pods",
                subresource: "log",
                help: "Open the RBAC Can I simulator for pod log access in the active namespace."
            )

        case "rbac-pod-logs" where hasPodTarget:
            return .init(
                title: "Open Logs",
                systemImage: "doc.text.magnifyingglass",
                help: "Open logs for a pod in the active namespace.",
                destination: .podLogs
            )

        case "rbac-pod-logs":
            return .init(
                title: "Open Pods",
                systemImage: "cube.box",
                help: "Open the Pods list first, then choose a pod to inspect logs.",
                destination: .resource(section: .workloads, kind: .pod)
            )

        case "rbac-pod-exec" where check.status != .passed:
            return rbacCanIAction(
                verb: "create",
                resource: "pods",
                subresource: "exec",
                help: "Open the RBAC Can I simulator for pod exec access in the active namespace."
            )

        case "rbac-pod-exec" where hasPodTarget:
            return .init(
                title: "Open Exec",
                systemImage: "terminal",
                help: "Open the pod exec panel for a pod in the active namespace.",
                destination: .podExec
            )

        case "rbac-pod-exec":
            return .init(
                title: "Open Pods",
                systemImage: "cube.box",
                help: "Open the Pods list first, then choose a pod for exec.",
                destination: .resource(section: .workloads, kind: .pod)
            )

        case "rbac-port-forward" where check.status != .passed:
            return rbacCanIAction(
                verb: "create",
                resource: "pods",
                subresource: "portforward",
                help: "Open the RBAC Can I simulator for pod port-forward access in the active namespace."
            )

        case "rbac-port-forward" where hasPodTarget:
            return .init(
                title: "Open Port Forward",
                systemImage: "arrow.left.and.right",
                help: "Open port-forward controls for a pod in the active namespace.",
                destination: .podPortForward
            )

        case "rbac-port-forward":
            return .init(
                title: "Open Pods",
                systemImage: "cube.box",
                help: "Open the Pods list first, then choose a pod for port-forwarding.",
                destination: .resource(section: .workloads, kind: .pod)
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
                if check.status != .passed {
                    return rbacCanIAction(for: target)
                }
                return .init(
                    title: target.actionTitle,
                    systemImage: target.systemImage,
                    help: target.help,
                    destination: target.destination
                )
            }
            guard let documentationAction = documentationAction(for: check) else { return nil }
            return .init(
                title: documentationAction.title,
                systemImage: documentationAction.systemImage,
                help: documentationAction.help,
                destination: .documentation(documentationAction.url)
            )
        }
    }

    private static func documentationAction(for check: RuneHealthCheck) -> (
        title: String,
        systemImage: String,
        help: String,
        url: URL
    )? {
        if providerLoginDocumentationCheckIDs.contains(check.id),
           let providerAction = providerLoginDocumentationAction(for: check.message) {
            return providerAction
        }

        guard let url = documentationURL(for: check) else { return nil }
        switch check.id {
        case "kubeconfig", "kubeconfig-files", "contexts", "selected-context", "context-namespace", "auth-provider-profile":
            return (
                title: "Kubeconfig Docs",
                systemImage: "doc.text.magnifyingglass",
                help: "Open official Kubernetes kubeconfig guidance for reviewing context and cluster access.",
                url: url
            )
        case let id where execAuthDocumentationCheckIDs.contains(id):
            return (
                title: "Exec Auth Docs",
                systemImage: "key.viewfinder",
                help: "Open official Kubernetes exec credential plugin guidance for provider login and token refresh issues.",
                url: url
            )
        case "oidc-token-profile":
            return (
                title: "OIDC Auth Docs",
                systemImage: "person.badge.key",
                help: "Open official Kubernetes authentication guidance for OIDC token expiry and refresh issues.",
                url: url
            )
        case "api-auth", "client-certificate-auth":
            return (
                title: "Auth Docs",
                systemImage: "person.badge.key",
                help: "Open official Kubernetes authentication guidance for credential rejection and client identity issues.",
                url: url
            )
        case "transport", "proxy-profile", "custom-ca-profile":
            return (
                title: "API Access Docs",
                systemImage: "network.badge.shield.half.filled",
                help: "Open official Kubernetes API access guidance for DNS, proxy, and TLS troubleshooting.",
                url: url
            )
        case "api-authorization",
             "rbac-access-summary",
             "rbac-pods-list",
             "rbac-pod-logs",
             "rbac-pod-exec",
             "rbac-port-forward":
            return (
                title: "RBAC Docs",
                systemImage: "person.2.badge.gearshape",
                help: "Open official Kubernetes RBAC guidance for denied or partial access.",
                url: url
            )
        case let id where id.hasPrefix("rbac-") && AuthDoctorRBACPreflightTarget.target(forCheckID: id) != nil:
            return (
                title: "RBAC Docs",
                systemImage: "person.2.badge.gearshape",
                help: "Open official Kubernetes RBAC guidance for denied list access.",
                url: url
            )
        case "namespace", "namespace-list":
            return (
                title: "Namespace Docs",
                systemImage: "square.grid.3x3",
                help: "Open official Kubernetes namespace guidance.",
                url: url
            )
        case "pod-logs":
            return (
                title: "Logs Docs",
                systemImage: "doc.text.magnifyingglass",
                help: "Open official Kubernetes log access guidance.",
                url: url
            )
        case "pod-list":
            return (
                title: "Pod Docs",
                systemImage: "cube.box",
                help: "Open official Kubernetes pod guidance.",
                url: url
            )
        default:
            return (
                title: "Docs",
                systemImage: "book",
                help: "Open official Kubernetes documentation for this check.",
                url: url
            )
        }
    }

    private static func providerLoginDocumentationAction(for message: String) -> (
        title: String,
        systemImage: String,
        help: String,
        url: URL
    )? {
        let normalized = message.lowercased()
        if normalized.contains("aws") || normalized.contains("eks") {
            return (
                title: "EKS Login Docs",
                systemImage: "key.viewfinder",
                help: "Open Amazon EKS kubeconfig and AWS CLI login guidance.",
                url: URL(string: "https://docs.aws.amazon.com/eks/latest/userguide/create-kubeconfig.html")!
            )
        }
        if normalized.contains("gcloud") || normalized.contains("gke-gcloud-auth-plugin") || normalized.contains("gke") {
            return (
                title: "GKE Login Docs",
                systemImage: "key.viewfinder",
                help: "Open Google Kubernetes Engine kubectl access and auth plugin guidance.",
                url: URL(string: "https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl")!
            )
        }
        if normalized.contains("az") || normalized.contains("aks") || normalized.contains("kubelogin") {
            return (
                title: "AKS Login Docs",
                systemImage: "key.viewfinder",
                help: "Open Azure Kubernetes Service kubelogin authentication guidance.",
                url: URL(string: "https://learn.microsoft.com/en-us/azure/aks/kubelogin-authentication")!
            )
        }
        if normalized.contains("doctl") || normalized.contains("doks") || normalized.contains("digitalocean") {
            return (
                title: "DOKS Login Docs",
                systemImage: "key.viewfinder",
                help: "Open DigitalOcean Kubernetes cluster connection guidance.",
                url: URL(string: "https://docs.digitalocean.com/products/kubernetes/how-to/connect-to-cluster/")!
            )
        }
        if normalized.contains("rancher") {
            return (
                title: "Rancher Kubeconfig Docs",
                systemImage: "key.viewfinder",
                help: "Open Rancher kubeconfig and kubectl access guidance.",
                url: URL(string: "https://ranchermanager.docs.rancher.com/how-to-guides/new-user-guides/manage-clusters/access-clusters/use-kubectl-and-kubeconfig")!
            )
        }
        if normalized.contains("openshift") || normalized.contains(" oc ") || normalized.contains("oc ") {
            return (
                title: "OpenShift CLI Docs",
                systemImage: "key.viewfinder",
                help: "Open Red Hat OpenShift CLI guidance.",
                url: URL(string: "https://docs.redhat.com/en/documentation/openshift_container_platform/4.17/html/cli_tools/openshift-cli-oc")!
            )
        }
        return nil
    }

    private static func rbacCanIAction(for target: AuthDoctorRBACPreflightTarget) -> AuthDoctorEntryResolution {
        AuthDoctorEntryResolution(
            title: "Check RBAC",
            systemImage: "person.badge.key",
            help: "Open the RBAC Can I simulator with this denied access check prefilled.",
            destination: .rbacCanIPreset(
                verb: target.verb,
                resource: target.resource,
                apiGroup: target.apiGroup,
                subresource: target.subresource,
                scope: target.scope
            )
        )
    }

    private static func rbacCanIAction(
        verb: String,
        resource: String,
        subresource: String?,
        help: String
    ) -> AuthDoctorEntryResolution {
        AuthDoctorEntryResolution(
            title: "Check RBAC",
            systemImage: "person.badge.key",
            help: help,
            destination: .rbacCanIPreset(
                verb: verb,
                resource: resource,
                apiGroup: nil,
                subresource: subresource,
                scope: .namespace
            )
        )
    }

    public static func documentationURL(for check: RuneHealthCheck) -> URL? {
        let path: String
        switch check.id {
        case "kubeconfig", "kubeconfig-files", "contexts", "selected-context", "context-namespace", "auth-provider-profile":
            path = "https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/"
        case let id where execAuthDocumentationCheckIDs.contains(id):
            path = "https://kubernetes.io/docs/reference/access-authn-authz/authentication/#client-go-credential-plugins"
        case "oidc-token-profile",
             "api-auth",
             "client-certificate-auth":
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
