import Foundation
import RuneCore
import RuneSecurity

public struct AuthDoctorKubeconfigInspector: Sendable {
    private enum ExecutableState {
        case available
        case missing
        case notExecutable
    }

    private enum ProviderKind: CaseIterable, Hashable {
        case eks
        case gke
        case aks
        case oidc
        case doks
        case rancher
        case openShift

        var hint: String {
            switch self {
            case .eks: return "EKS auth hints detected."
            case .gke: return "GKE auth hints detected."
            case .aks: return "AKS/kubelogin auth hints detected."
            case .oidc: return "OIDC-style token hints detected."
            case .doks: return "DOKS auth hints detected."
            case .rancher: return "Rancher auth hints detected."
            case .openShift: return "OpenShift auth hints detected."
            }
        }
    }

    private struct InspectionScope {
        let users: [AuthDoctorKubeconfigProjection.User]
        let clusters: [AuthDoctorKubeconfigProjection.Cluster]
    }

    private let fileExists: @Sendable (String) -> Bool
    private let isExecutable: @Sendable (String) -> Bool
    private let executableSearchPaths: [String]
    private let externalCommandsAllowed: @Sendable () -> Bool

    /// Production initializer. Availability means the file both exists and is executable.
    public init(
        executableSearchPaths: [String] = RuneExecutableSearchPath.directories(),
        externalCommandsAllowed: @escaping @Sendable () -> Bool = { RuneExternalCommandPolicy.allowsExternalCommands }
    ) {
        self.fileExists = { path in FileManager.default.fileExists(atPath: path) }
        self.isExecutable = { path in
            var isDirectory: ObjCBool = false
            guard FileManager.default.fileExists(atPath: path, isDirectory: &isDirectory),
                  !isDirectory.boolValue else { return false }
            return FileManager.default.isExecutableFile(atPath: path)
        }
        self.executableSearchPaths = executableSearchPaths
        self.externalCommandsAllowed = externalCommandsAllowed
    }

    /// Compatibility initializer for existing deterministic callers. A reported file is treated as executable.
    public init(
        fileExists: @escaping @Sendable (String) -> Bool,
        executableSearchPaths: [String] = RuneExecutableSearchPath.directories(),
        externalCommandsAllowed: @escaping @Sendable () -> Bool = { RuneExternalCommandPolicy.allowsExternalCommands }
    ) {
        self.fileExists = fileExists
        self.isExecutable = fileExists
        self.executableSearchPaths = executableSearchPaths
        self.externalCommandsAllowed = externalCommandsAllowed
    }

    /// Deterministic initializer used when callers need to distinguish a missing file from a non-executable file.
    public init(
        fileExists: @escaping @Sendable (String) -> Bool,
        isExecutable: @escaping @Sendable (String) -> Bool,
        executableSearchPaths: [String] = RuneExecutableSearchPath.directories(),
        externalCommandsAllowed: @escaping @Sendable () -> Bool = { RuneExternalCommandPolicy.allowsExternalCommands }
    ) {
        self.fileExists = fileExists
        self.isExecutable = isExecutable
        self.executableSearchPaths = executableSearchPaths
        self.externalCommandsAllowed = externalCommandsAllowed
    }

    public func inspect(sources: [KubeConfigSource]) -> [RuneHealthCheck] {
        inspect(sources: sources, activeContextName: nil)
    }

    /// Inspects only the explicitly selected context when supplied. Otherwise kubeconfig `current-context`
    /// is used when it can be resolved across the supplied sources.
    public func inspect(
        sources: [KubeConfigSource],
        activeContextName: String?
    ) -> [RuneHealthCheck] {
        guard !sources.isEmpty else { return [] }

        var projections: [AuthDoctorKubeconfigProjection] = []
        var unreadableCount = 0
        for source in sources {
            do {
                let content = try String(contentsOf: source.url, encoding: .utf8)
                projections.append(AuthDoctorKubeconfigProjection.parse(content, sourceURL: source.url))
            } catch {
                unreadableCount += 1
            }
        }

        let scope = inspectionScope(projections: projections, activeContextName: activeContextName)
        let providers = detectedProviders(in: scope)
        let providerHints = ProviderKind.allCases.filter(providers.contains).map(\.hint)
        let execEntries = scope.users.compactMap(\.exec)
        let usesExecAuth = !execEntries.isEmpty
        let canRunExternalCommands = externalCommandsAllowed()
        let nativeSubstitutionAvailable = !canRunExternalCommands
            && hasNativeSubstitution(sources: sources, activeContextName: activeContextName)
        var checks: [RuneHealthCheck] = []

        checks.append(RuneHealthCheck(
            id: "kubeconfig-files",
            title: "Kubeconfig files",
            status: unreadableCount == 0 ? .passed : .warning,
            message: unreadableCount == 0
                ? "\(sources.count) kubeconfig source(s) were readable for local auth hints."
                : "\(unreadableCount) kubeconfig source(s) could not be inspected for local auth hints. Live API checks still verify the active context."
        ))

        checks.append(RuneHealthCheck(
            id: "auth-provider-profile",
            title: "Auth provider profile",
            status: .passed,
            message: providerHints.isEmpty
                ? "Generic kubeconfig profile. No cloud, OIDC, local, or vendor-specific hint was detected for the selected context."
                : providerHints.joined(separator: " ")
        ))

        if usesExecAuth {
            checks.append(RuneHealthCheck(
                id: "exec-auth-profile",
                title: "Exec auth profile",
                status: canRunExternalCommands || nativeSubstitutionAvailable ? .passed : .warning,
                message: nativeSubstitutionAvailable
                    ? "The selected ExecConfig has a supported in-process authentication replacement. Native profile and live API checks verify it without launching the configured command."
                    : canRunExternalCommands
                    ? "The selected kubeconfig context uses exec auth. Rune verifies the configured plugin through live Kubernetes API checks without exporting command arguments or environment values."
                    : RuneExternalCommandPolicy.disabledMessage
            ))
            if nativeSubstitutionAvailable {
                checks.append(RuneHealthCheck(
                    id: "exec-auth-tools",
                    title: "Exec auth tools",
                    status: .passed,
                    message: "No external auth executable is required for the selected native authentication profile."
                ))
            } else {
                checks.append(contentsOf: execConfigurationChecks(execEntries))
                checks.append(execToolsCheck(execEntries, externalCommandsAllowed: canRunExternalCommands))
            }
        }

        if !nativeSubstitutionAvailable {
            checks.append(contentsOf: providerLifecycleChecks(
                providers: providers,
                users: scope.users,
                externalCommandsAllowed: canRunExternalCommands
            ))
        }

        let cloudToolEntries = requiredCloudToolEntries(from: execEntries)
        if !cloudToolEntries.isEmpty, !nativeSubstitutionAvailable {
            checks.append(cloudToolsCheck(cloudToolEntries, externalCommandsAllowed: canRunExternalCommands))
        }

        if scope.clusters.contains(where: \.hasProxyURL) {
            checks.append(RuneHealthCheck(
                id: "proxy-profile",
                title: "Proxy profile",
                status: .warning,
                message: "Proxy configuration was detected. Rune's Kubernetes REST transport does not currently apply kubeconfig proxy-url, so proxy routing is not verified."
            ))
        }

        if scope.clusters.contains(where: \.hasCustomCA) {
            checks.append(RuneHealthCheck(
                id: "custom-ca-profile",
                title: "Custom CA profile",
                status: .passed,
                message: "Custom certificate authority configuration was detected. API transport checks verify server trust for the selected context."
            ))
        }

        return checks
    }

    private func hasNativeSubstitution(
        sources: [KubeConfigSource],
        activeContextName: String?
    ) -> Bool {
        guard let analysis = try? KubeConfigNativeAuthAnalyzer().analyze(sources: sources) else {
            return false
        }
        let requestedName = activeContextName?.trimmingCharacters(in: .whitespacesAndNewlines)
        let targetName = requestedName?.isEmpty == false ? requestedName : analysis.currentContext
        if let targetName {
            return analysis.contexts.first(where: { $0.contextName == targetName })?.credentialRequest != nil
        }
        return analysis.contexts.count == 1 && analysis.contexts[0].credentialRequest != nil
    }

    private func inspectionScope(
        projections: [AuthDoctorKubeconfigProjection],
        activeContextName: String?
    ) -> InspectionScope {
        var contexts: [String: AuthDoctorKubeconfigProjection.Context] = [:]
        var users: [String: AuthDoctorKubeconfigProjection.User] = [:]
        var clusters: [String: AuthDoctorKubeconfigProjection.Cluster] = [:]
        var mergedCurrentContext: String?

        for projection in projections {
            if mergedCurrentContext == nil, projection.currentContext?.isEmpty == false {
                mergedCurrentContext = projection.currentContext
            }
            for context in projection.contexts where contexts[context.name] == nil {
                contexts[context.name] = context
            }
            for user in projection.users where users[user.name] == nil {
                users[user.name] = user
            }
            for cluster in projection.clusters where clusters[cluster.name] == nil {
                clusters[cluster.name] = cluster
            }
        }

        let explicitContext = activeContextName?.trimmingCharacters(in: .whitespacesAndNewlines)
        let selectedContextName = explicitContext?.isEmpty == false ? explicitContext : mergedCurrentContext
        if let selectedContextName,
           let context = contexts[selectedContextName] {
            return InspectionScope(
                users: users[context.user].map { [$0] } ?? [],
                clusters: clusters[context.cluster].map { [$0] } ?? []
            )
        }

        return InspectionScope(
            users: Array(users.values),
            clusters: Array(clusters.values)
        )
    }

    private func execConfigurationChecks(
        _ entries: [AuthDoctorKubeconfigProjection.User.Exec]
    ) -> [RuneHealthCheck] {
        var checks: [RuneHealthCheck] = []
        let alwaysCount = entries.filter {
            $0.interactiveMode?.caseInsensitiveCompare("Always") == .orderedSame
        }.count
        if alwaysCount > 0 {
            checks.append(RuneHealthCheck(
                id: "exec-auth-interactive-mode",
                title: "Exec auth interactivity",
                status: .failed,
                message: "The selected exec auth configuration requires interactive stdin. Rune's non-interactive credential runner cannot satisfy interactiveMode Always."
            ))
        }

        let missingV1InteractiveMode = entries.contains { entry in
            entry.apiVersion == "client.authentication.k8s.io/v1"
                && entry.interactiveMode?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty != false
        }
        if missingV1InteractiveMode {
            checks.append(RuneHealthCheck(
                id: "exec-auth-v1-interactive-mode",
                title: "Exec auth v1 configuration",
                status: .failed,
                message: "A client.authentication.k8s.io/v1 exec entry is missing interactiveMode, which is required by the Kubernetes ExecConfig v1 contract."
            ))
        }

        let hasInvalidMode = entries.contains { entry in
            guard let mode = entry.interactiveMode?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !mode.isEmpty else { return false }
            return mode != "Never" && mode != "IfAvailable" && mode != "Always"
        }
        if hasInvalidMode {
            checks.append(RuneHealthCheck(
                id: "exec-auth-invalid-interactive-mode",
                title: "Exec auth interactivity",
                status: .failed,
                message: "An exec auth entry has an unsupported interactiveMode. Kubernetes accepts Never, IfAvailable, or Always."
            ))
        }
        return checks
    }

    private func execToolsCheck(
        _ entries: [AuthDoctorKubeconfigProjection.User.Exec],
        externalCommandsAllowed: Bool
    ) -> RuneHealthCheck {
        guard externalCommandsAllowed else {
            return RuneHealthCheck(
                id: "exec-auth-tools",
                title: "Exec auth tools",
                status: .warning,
                message: "Kubeconfig exec auth commands are present, but this Rune build cannot run external auth plugins."
            )
        }

        let assessments = entries.map { entry in
            (entry: entry, state: executableState(for: entry.command, sourceDirectory: entry.sourceDirectory))
        }
        let missing = uniqueSafeCommandNames(assessments.filter { $0.state == .missing }.map { $0.entry.command })
        let notExecutable = uniqueSafeCommandNames(assessments.filter { $0.state == .notExecutable }.map { $0.entry.command })
        let hints = assessments
            .filter { $0.state != .available }
            .compactMap { safeInstallHint($0.entry.installHint) }
            .uniqued()

        var messages: [String] = []
        if !missing.isEmpty {
            messages.append("Missing exec auth tool(s): \(missing.joined(separator: ", ")). Install the plugin or fix its kubeconfig command.")
        }
        if !notExecutable.isEmpty {
            messages.append("Exec auth tool(s) found but not executable: \(notExecutable.joined(separator: ", ")). Check the file's execute permission.")
        }
        if !hints.isEmpty {
            messages.append("Plugin guidance: \(hints.prefix(2).joined(separator: " "))")
        }
        if messages.isEmpty {
            messages.append("All kubeconfig exec auth commands were found on PATH. Each selected command is executable.")
        }

        return RuneHealthCheck(
            id: "exec-auth-tools",
            title: "Exec auth tools",
            status: missing.isEmpty && notExecutable.isEmpty ? .passed : .warning,
            message: messages.joined(separator: " ")
        )
    }

    private func cloudToolsCheck(
        _ entries: [AuthDoctorKubeconfigProjection.User.Exec],
        externalCommandsAllowed: Bool
    ) -> RuneHealthCheck {
        guard externalCommandsAllowed else {
            return RuneHealthCheck(
                id: "cloud-login-tools",
                title: "Cloud login tools",
                status: .warning,
                message: "Cloud provider CLI tools are detected in the selected kubeconfig context, but this Rune build cannot run external provider commands."
            )
        }

        let assessments = entries.map { entry in
            (name: safeCommandName(entry.command), state: executableState(for: entry.command, sourceDirectory: entry.sourceDirectory))
        }
        let missing = assessments.filter { $0.state == .missing }.map(\.name).uniqued().sorted()
        let notExecutable = assessments.filter { $0.state == .notExecutable }.map(\.name).uniqued().sorted()
        var messages: [String] = []
        if !missing.isEmpty {
            messages.append("Missing \(missing.joined(separator: ", ")) on PATH.")
        }
        if !notExecutable.isEmpty {
            messages.append("Found but not executable: \(notExecutable.joined(separator: ", ")).")
        }
        if messages.isEmpty {
            messages.append("Detected cloud provider CLI tools are available on PATH. Each detected tool is executable.")
        }
        return RuneHealthCheck(
            id: "cloud-login-tools",
            title: "Cloud login tools",
            status: missing.isEmpty && notExecutable.isEmpty ? .passed : .warning,
            message: messages.joined(separator: " ")
        )
    }

    private func providerLifecycleChecks(
        providers: Set<ProviderKind>,
        users: [AuthDoctorKubeconfigProjection.User],
        externalCommandsAllowed: Bool
    ) -> [RuneHealthCheck] {
        let execEntries = users.compactMap(\.exec)
        var checks: [RuneHealthCheck] = []

        if providers.contains(.eks) {
            let hasRoleHint = execEntries.contains(where: \.hasEKSRoleArgument)
            let message: String
            if !externalCommandsAllowed, !execEntries.isEmpty {
                message = "EKS exec auth hints were detected, but this Rune build cannot run external AWS auth commands."
            } else if hasRoleHint {
                message = "EKS role assumption was detected without exporting the role ARN. Rune validates the resulting credentials through live API checks."
            } else {
                message = "EKS auth was detected without an explicit role assumption hint. If access fails, confirm the AWS profile or role selection outside Rune."
            }
            checks.append(RuneHealthCheck(
                id: "eks-role-profile",
                title: "EKS role profile",
                status: hasRoleHint && (externalCommandsAllowed || execEntries.isEmpty) ? .passed : .warning,
                message: message
            ))
        }

        if providers.contains(.gke) {
            checks.append(providerToolCheck(
                id: "gke-auth-plugin-profile",
                title: "GKE auth plugin",
                providerName: "GKE",
                command: "gke-gcloud-auth-plugin",
                configuredEntries: execEntries,
                externalCommandsAllowed: externalCommandsAllowed
            ))
        }

        if providers.contains(.aks) {
            checks.append(providerToolCheck(
                id: "aks-kubelogin-profile",
                title: "AKS kubelogin",
                providerName: "AKS",
                command: "kubelogin",
                configuredEntries: execEntries,
                externalCommandsAllowed: externalCommandsAllowed
            ))
        }

        if providers.contains(.doks) {
            checks.append(providerToolCheck(
                id: "doks-doctl-profile",
                title: "DOKS doctl",
                providerName: "DOKS",
                command: "doctl",
                configuredEntries: execEntries,
                externalCommandsAllowed: externalCommandsAllowed
            ))
        }

        if providers.contains(.rancher) {
            checks.append(providerToolCheck(
                id: "rancher-cli-profile",
                title: "Rancher CLI",
                providerName: "Rancher",
                command: "rancher",
                configuredEntries: execEntries,
                externalCommandsAllowed: externalCommandsAllowed
            ))
        }

        if providers.contains(.openShift) {
            checks.append(providerToolCheck(
                id: "openshift-cli-profile",
                title: "OpenShift CLI",
                providerName: "OpenShift",
                command: "oc",
                configuredEntries: execEntries,
                externalCommandsAllowed: externalCommandsAllowed
            ))
        }

        if providers.contains(.oidc) {
            checks.append(oidcLifecycleCheck(users: users))
        }
        return checks
    }

    private func providerToolCheck(
        id: String,
        title: String,
        providerName: String,
        command: String,
        configuredEntries: [AuthDoctorKubeconfigProjection.User.Exec],
        externalCommandsAllowed: Bool
    ) -> RuneHealthCheck {
        guard externalCommandsAllowed else {
            return RuneHealthCheck(
                id: id,
                title: title,
                status: .warning,
                message: "\(providerName) auth hints were detected, but this Rune build cannot run external provider auth commands."
            )
        }
        let configuredEntry = configuredEntries.first { safeCommandName($0.command) == command }
        let state = configuredEntry.map {
            executableState(for: $0.command, sourceDirectory: $0.sourceDirectory)
        } ?? executableState(for: command, sourceDirectory: "")
        let message: String
        switch state {
        case .available:
            message = "\(providerName) \(command) was found on PATH and is executable. Rune validates generated credentials through live API checks."
        case .missing:
            message = "\(providerName) auth hints were detected, but `\(command)` was not found on PATH."
        case .notExecutable:
            message = "\(providerName) `\(command)` was found on PATH but is not executable."
        }
        return RuneHealthCheck(
            id: id,
            title: title,
            status: state == .available ? .passed : .warning,
            message: message
        )
    }

    private func oidcLifecycleCheck(users: [AuthDoctorKubeconfigProjection.User]) -> RuneHealthCheck {
        let expiryValues = users.compactMap(\.authExpiry)
        if expiryValues.contains(where: { value in
            guard let date = Self.parseDate(value) else { return false }
            return date <= Date()
        }) {
            return RuneHealthCheck(
                id: "oidc-token-profile",
                title: "OIDC token profile",
                status: .warning,
                message: "OIDC-style auth appears expired. Refresh provider login before retrying the Kubernetes API."
            )
        }
        if !expiryValues.isEmpty, expiryValues.contains(where: { Self.parseDate($0) == nil }) {
            return RuneHealthCheck(
                id: "oidc-token-profile",
                title: "OIDC token profile",
                status: .warning,
                message: "OIDC-style auth includes an expiry timestamp Rune could not parse. Refresh login if live API checks fail."
            )
        }
        return RuneHealthCheck(
            id: "oidc-token-profile",
            title: "OIDC token profile",
            status: .passed,
            message: expiryValues.isEmpty
                ? "OIDC-style auth was detected. No local expiry timestamp was exported; live API checks validate whether the token is still accepted."
                : "OIDC-style auth includes a future expiry timestamp. Rune does not export the token or timestamp value."
        )
    }

    private func detectedProviders(in scope: InspectionScope) -> Set<ProviderKind> {
        var providers = Set<ProviderKind>()
        for cluster in scope.clusters {
            let server = cluster.server.lowercased()
            if server.contains("eks.amazonaws.com") { providers.insert(.eks) }
            if server.contains("container.googleapis.com") { providers.insert(.gke) }
            if server.contains("azmk8s.io") || server.contains("azure.com") { providers.insert(.aks) }
            if server.contains("digitalocean.com") { providers.insert(.doks) }
            if server.contains("rancher") { providers.insert(.rancher) }
            if server.contains("openshift") { providers.insert(.openShift) }
        }
        for user in scope.users {
            if user.authProviderName?.lowercased() == "oidc" { providers.insert(.oidc) }
            guard let command = user.exec?.command else { continue }
            switch safeCommandName(command).lowercased() {
            case "aws", "aws-iam-authenticator": providers.insert(.eks)
            case "gcloud", "gke-gcloud-auth-plugin": providers.insert(.gke)
            case "az", "kubelogin": providers.insert(.aks)
            case "doctl": providers.insert(.doks)
            case "rancher": providers.insert(.rancher)
            case "oc": providers.insert(.openShift)
            default: break
            }
        }
        return providers
    }

    private func requiredCloudToolEntries(
        from entries: [AuthDoctorKubeconfigProjection.User.Exec]
    ) -> [AuthDoctorKubeconfigProjection.User.Exec] {
        let knownTools: Set<String> = [
            "aws", "aws-iam-authenticator", "az", "kubelogin", "gcloud",
            "gke-gcloud-auth-plugin", "doctl", "rancher", "oc"
        ]
        var seen = Set<String>()
        return entries.filter { entry in
            let name = safeCommandName(entry.command)
            return knownTools.contains(name) && seen.insert(entry.command).inserted
        }
    }

    private func executableState(for command: String, sourceDirectory: String) -> ExecutableState {
        let trimmed = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return .missing }

        let candidates: [String]
        if trimmed.contains("/") {
            let expanded = NSString(string: trimmed).expandingTildeInPath
            if expanded.hasPrefix("/") || sourceDirectory.isEmpty {
                candidates = [expanded]
            } else {
                candidates = [URL(fileURLWithPath: sourceDirectory).appendingPathComponent(expanded).standardized.path]
            }
        } else {
            candidates = executableSearchPaths.map {
                URL(fileURLWithPath: $0).appendingPathComponent(trimmed).path
            }
        }

        var foundFile = false
        for candidate in candidates where fileExists(candidate) {
            foundFile = true
            if isExecutable(candidate) { return .available }
        }
        return foundFile ? .notExecutable : .missing
    }

    private func uniqueSafeCommandNames(_ commands: [String]) -> [String] {
        commands.map(safeCommandName).uniqued().sorted()
    }

    private func safeCommandName(_ command: String) -> String {
        let trimmed = command.trimmingCharacters(in: .whitespacesAndNewlines)
        let name = trimmed.contains("/")
            ? URL(fileURLWithPath: trimmed).lastPathComponent
            : trimmed
        let printable = name.unicodeScalars
            .filter { !CharacterSet.controlCharacters.contains($0) }
            .map(String.init)
            .joined()
        guard printable.count > 80 else { return printable }
        return String(printable.prefix(77)) + "..."
    }

    private func safeInstallHint(_ value: String?) -> String? {
        guard let value else { return nil }
        let normalized = value
            .split(whereSeparator: { $0.isWhitespace })
            .joined(separator: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalized.isEmpty else { return nil }
        guard normalized.count > 240 else { return normalized }
        return String(normalized.prefix(237)) + "..."
    }

    private static func parseDate(_ value: String) -> Date? {
        var normalized = value
        if normalized.hasSuffix("z") {
            normalized = String(normalized.dropLast()) + "Z"
        }
        if let separatorIndex = normalized.firstIndex(of: "t") {
            normalized.replaceSubrange(separatorIndex...separatorIndex, with: "T")
        }
        let iso = ISO8601DateFormatter()
        iso.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = iso.date(from: normalized) { return date }
        iso.formatOptions = [.withInternetDateTime]
        return iso.date(from: normalized)
    }
}

private extension Sequence where Element == String {
    func uniqued() -> [String] {
        var seen = Set<String>()
        return filter { seen.insert($0).inserted }
    }
}
