import Foundation
import RuneCore

public struct AuthDoctorKubeconfigInspector: Sendable {
    private let fileExists: @Sendable (String) -> Bool
    private let executableSearchPaths: [String]

    public init(
        fileExists: @escaping @Sendable (String) -> Bool = { path in FileManager.default.fileExists(atPath: path) },
        executableSearchPaths: [String] = RuneExecutableSearchPath.directories()
    ) {
        self.fileExists = fileExists
        self.executableSearchPaths = executableSearchPaths
    }

    public func inspect(sources: [KubeConfigSource]) -> [RuneHealthCheck] {
        guard !sources.isEmpty else { return [] }

        var readableContents: [String] = []
        var unreadableCount = 0
        for source in sources {
            do {
                readableContents.append(try String(contentsOf: source.url, encoding: .utf8))
            } catch {
                unreadableCount += 1
            }
        }
        let combined = readableContents.joined(separator: "\n")

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
            message: providers.isEmpty ? "Generic kubeconfig profile. No cloud, OIDC, local, or vendor-specific hint was detected." : providers.joined(separator: " ")
        ))

        if lowercased.contains("exec:") {
            checks.append(RuneHealthCheck(
                id: "exec-auth-profile",
                title: "Exec auth profile",
                status: .passed,
                message: "Kubeconfig uses exec auth. Rune will verify the configured plugin through live Kubernetes API checks without exporting command arguments."
            ))
        }

        checks.append(contentsOf: providerLifecycleChecks(in: lowercased))

        let execCommands = Set(Self.execCommands(in: combined))
        if !execCommands.isEmpty {
            let missing = execCommands.filter { !commandExists($0) }.sorted()
            checks.append(RuneHealthCheck(
                id: "exec-auth-tools",
                title: "Exec auth tools",
                status: missing.isEmpty ? .passed : .warning,
                message: missing.isEmpty
                    ? "All kubeconfig exec auth commands were found on PATH."
                    : "Rune could not find \(missing.joined(separator: ", ")) on PATH. Cloud login may require installing or signing in with the matching CLI."
            ))
        }

        let cloudTools = requiredCloudTools(for: providers)
        if !cloudTools.isEmpty {
            let missing = cloudTools.filter { !commandExists($0) }.sorted()
            checks.append(RuneHealthCheck(
                id: "cloud-login-tools",
                title: "Cloud login tools",
                status: missing.isEmpty ? .passed : .warning,
                message: missing.isEmpty
                    ? "Detected cloud provider CLI tools are available on PATH."
                    : "Missing \(missing.joined(separator: ", ")) on PATH. Install or sign in with the provider CLI before running cloud login."
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

    private func providerLifecycleChecks(in text: String) -> [RuneHealthCheck] {
        var checks: [RuneHealthCheck] = []

        if text.contains("eks.amazonaws.com") || text.contains("aws eks") || text.contains("aws-iam-authenticator") {
            let hasRoleHint = text.contains("--role-arn")
                || text.contains("role_arn")
                || text.contains("role-arn")
                || text.contains("rolearn")
            checks.append(RuneHealthCheck(
                id: "eks-role-profile",
                title: "EKS role profile",
                status: hasRoleHint ? .passed : .warning,
                message: hasRoleHint
                    ? "EKS role assumption was detected without exporting the role ARN. Rune will validate the resulting credentials through live API checks."
                    : "EKS auth was detected without an explicit role assumption hint. If access fails, confirm the AWS profile or role selection outside Rune."
            ))
        }

        if text.contains("gke-gcloud-auth-plugin") || text.contains("container.googleapis.com") || text.contains("cmd-path: gcloud") {
            let hasPlugin = commandExists("gke-gcloud-auth-plugin")
            checks.append(RuneHealthCheck(
                id: "gke-auth-plugin-profile",
                title: "GKE auth plugin",
                status: hasPlugin ? .passed : .warning,
                message: hasPlugin
                    ? "GKE auth plugin was found on PATH. Rune will validate generated credentials through live API checks."
                    : "GKE auth hints were detected, but `gke-gcloud-auth-plugin` was not found on PATH."
            ))
        }

        if text.contains("kubelogin") || text.contains("azure") || text.contains("aks") {
            let hasKubelogin = commandExists("kubelogin")
            checks.append(RuneHealthCheck(
                id: "aks-kubelogin-profile",
                title: "AKS kubelogin",
                status: hasKubelogin ? .passed : .warning,
                message: hasKubelogin
                    ? "AKS kubelogin was found on PATH. Rune will validate token refresh through live API checks."
                    : "AKS/kubelogin hints were detected, but `kubelogin` was not found on PATH."
            ))
        }

        if text.contains("digitalocean") || text.contains("doctl") || text.contains("doks") {
            checks.append(providerToolCheck(
                id: "doks-doctl-profile",
                title: "DOKS doctl",
                providerName: "DOKS",
                command: "doctl"
            ))
        }

        if text.contains("rancher") {
            checks.append(providerToolCheck(
                id: "rancher-cli-profile",
                title: "Rancher CLI",
                providerName: "Rancher",
                command: "rancher"
            ))
        }

        if Self.hasOpenShiftHint(in: text) {
            checks.append(providerToolCheck(
                id: "openshift-cli-profile",
                title: "OpenShift CLI",
                providerName: "OpenShift",
                command: "oc"
            ))
        }

        if text.contains("oidc") || text.contains("id-token") || text.contains("client-id") {
            let expiry = Self.oidcExpiryState(in: text)
            checks.append(RuneHealthCheck(
                id: "oidc-token-profile",
                title: "OIDC token profile",
                status: expiry.status,
                message: expiry.message
            ))
        }

        return checks
    }

    private func providerToolCheck(
        id: String,
        title: String,
        providerName: String,
        command: String
    ) -> RuneHealthCheck {
        let hasCommand = commandExists(command)
        return RuneHealthCheck(
            id: id,
            title: title,
            status: hasCommand ? .passed : .warning,
            message: hasCommand
                ? "\(providerName) \(command) was found on PATH. Rune will validate generated credentials through live API checks."
                : "\(providerName) auth hints were detected, but `\(command)` was not found on PATH."
        )
    }

    private static func oidcExpiryState(in text: String) -> (status: RuneHealthCheckStatus, message: String) {
        guard let expiryText = firstValue(after: "expiry:", in: text) ?? firstValue(after: "expiration:", in: text) else {
            return (
                .passed,
                "OIDC-style auth was detected. No local expiry timestamp was exported; live API checks validate whether the token is still accepted."
            )
        }

        guard let expiry = parseDate(expiryText) else {
            return (
                .warning,
                "OIDC-style auth includes an expiry timestamp Rune could not parse. Refresh login if live API checks fail."
            )
        }

        if expiry <= Date() {
            return (
                .warning,
                "OIDC-style auth appears expired. Refresh provider login before retrying the Kubernetes API."
            )
        }

        return (
            .passed,
            "OIDC-style auth includes a future expiry timestamp. Rune does not export the token or timestamp value."
        )
    }

    private static func firstValue(after marker: String, in text: String) -> String? {
        for line in text.split(whereSeparator: \.isNewline) {
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard trimmed.hasPrefix(marker) else { continue }
            let value = trimmed.dropFirst(marker.count).trimmingCharacters(in: .whitespacesAndNewlines)
            let unquoted = value.trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
            guard !unquoted.isEmpty else { return nil }
            return unquoted
        }
        return nil
    }

    private static func parseDate(_ value: String) -> Date? {
        var normalized = value.hasSuffix("z") ? String(value.dropLast()) + "Z" : value
        if let separatorIndex = normalized.firstIndex(of: "t") {
            normalized.replaceSubrange(separatorIndex...separatorIndex, with: "T")
        }
        let iso = ISO8601DateFormatter()
        iso.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = iso.date(from: normalized) { return date }
        iso.formatOptions = [.withInternetDateTime]
        return iso.date(from: normalized)
    }

    private func commandExists(_ command: String) -> Bool {
        let trimmed = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return false }
        if trimmed.contains("/") {
            return fileExists(NSString(string: trimmed).expandingTildeInPath)
        }
        return executableSearchPaths.contains { directory in
            fileExists(URL(fileURLWithPath: directory).appendingPathComponent(trimmed).path)
        }
    }

    private func requiredCloudTools(for providerHints: [String]) -> Set<String> {
        var tools = Set<String>()
        let joined = providerHints.joined(separator: " ").lowercased()
        if joined.contains("eks") {
            tools.insert("aws")
        }
        if joined.contains("gke") {
            tools.insert("gcloud")
            tools.insert("gke-gcloud-auth-plugin")
        }
        if joined.contains("aks") || joined.contains("kubelogin") {
            tools.insert("az")
            tools.insert("kubelogin")
        }
        if joined.contains("doks") || joined.contains("digitalocean") {
            tools.insert("doctl")
        }
        if joined.contains("rancher") {
            tools.insert("rancher")
        }
        if joined.contains("openshift") {
            tools.insert("oc")
        }
        return tools
    }

    private static func execCommands(in text: String) -> [String] {
        text
            .split(whereSeparator: \.isNewline)
            .compactMap { rawLine -> String? in
                let trimmed = String(rawLine).trimmingCharacters(in: .whitespacesAndNewlines)
                guard trimmed.hasPrefix("command:") else { return nil }
                let value = String(trimmed.dropFirst("command:".count)).trimmingCharacters(in: .whitespacesAndNewlines)
                guard !value.isEmpty else { return nil }
                return value.trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
            }
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
        if text.contains("digitalocean") || text.contains("doctl") || text.contains("doks") {
            hints.append("DOKS auth hints detected.")
        }
        if text.contains("rancher") {
            hints.append("Rancher auth hints detected.")
        }
        if Self.hasOpenShiftHint(in: text) {
            hints.append("OpenShift auth hints detected.")
        }
        return hints
    }

    private static func hasOpenShiftHint(in text: String) -> Bool {
        text.contains("openshift")
            || text.contains("crc")
            || text.contains(" oc ")
            || text.contains("command: oc")
    }
}
