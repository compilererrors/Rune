import Foundation
import RuneCore

public struct AuthDoctorKubeconfigInspector: Sendable {
    public init() {}

    public func inspect(sources: [KubeConfigSource]) -> [RuneHealthCheck] {
        guard !sources.isEmpty else { return [] }

        var combined = ""
        var unreadableCount = 0
        for source in sources {
            do {
                combined += "\n" + (try String(contentsOf: source.url, encoding: .utf8))
            } catch {
                unreadableCount += 1
            }
        }

        let lowercased = combined.lowercased()
        var checks: [RuneHealthCheck] = []

        if unreadableCount > 0 {
            checks.append(RuneHealthCheck(
                id: "kubeconfig-files",
                title: "Kubeconfig files",
                status: .warning,
                message: "\(unreadableCount) kubeconfig source(s) could not be inspected for local auth hints. Live API checks still verify the active context."
            ))
        } else {
            checks.append(RuneHealthCheck(
                id: "kubeconfig-files",
                title: "Kubeconfig files",
                status: .passed,
                message: "\(sources.count) kubeconfig source(s) were readable for local auth hints."
            ))
        }

        let providers = detectedProviderHints(in: lowercased)
        checks.append(RuneHealthCheck(
            id: "auth-provider-profile",
            title: "Auth provider profile",
            status: .passed,
            message: providers.isEmpty ? "Generic kubeconfig profile. No EKS, GKE, AKS, OIDC, or kubelogin-specific hint was detected." : providers.joined(separator: " ")
        ))

        if lowercased.contains("exec:") {
            checks.append(RuneHealthCheck(
                id: "exec-auth-profile",
                title: "Exec auth profile",
                status: .passed,
                message: "Kubeconfig uses exec auth. Rune will verify the configured plugin through live Kubernetes API checks without exporting command arguments."
            ))
        }

        if lowercased.contains("proxy-url:") || lowercased.contains("https_proxy") || lowercased.contains("http_proxy") {
            checks.append(RuneHealthCheck(
                id: "proxy-profile",
                title: "Proxy profile",
                status: .passed,
                message: "Proxy configuration was detected. API transport checks verify that proxy routing works for the selected context."
            ))
        }

        if lowercased.contains("certificate-authority-data:") || lowercased.contains("certificate-authority:") {
            checks.append(RuneHealthCheck(
                id: "custom-ca-profile",
                title: "Custom CA profile",
                status: .passed,
                message: "Custom certificate authority configuration was detected. API transport checks verify server trust for the selected context."
            ))
        }

        return checks
    }

    private func detectedProviderHints(in text: String) -> [String] {
        var hints: [String] = []
        if text.contains("aws eks") || text.contains("eks.amazonaws.com") || text.contains("aws-iam-authenticator") {
            hints.append("EKS auth hints detected.")
        }
        if text.contains("gke-gcloud-auth-plugin") || text.contains("container.googleapis.com") || text.contains("cmd-path: gcloud") {
            hints.append("GKE auth hints detected.")
        }
        if text.contains("kubelogin") || text.contains("azure") || text.contains("aks") {
            hints.append("AKS/kubelogin auth hints detected.")
        }
        if text.contains("oidc") || text.contains("id-token") || text.contains("client-id") {
            hints.append("OIDC-style token hints detected.")
        }
        return hints
    }
}
