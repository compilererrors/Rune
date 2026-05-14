import Foundation

public enum KubeConfigImportIssueSeverity: String, Sendable, Equatable {
    case warning
    case error
}

public struct KubeConfigImportIssue: Sendable, Equatable {
    public let id: String
    public let severity: KubeConfigImportIssueSeverity
    public let message: String

    public init(id: String, severity: KubeConfigImportIssueSeverity, message: String) {
        self.id = id
        self.severity = severity
        self.message = message
    }
}

public struct KubeConfigImportContextPreview: Sendable, Equatable {
    public let name: String
    public let clusterName: String?
    public let userName: String?
    public let namespace: String?
    public let serverHost: String?
    public let authType: String
    public let providerHint: String?

    public init(
        name: String,
        clusterName: String?,
        userName: String?,
        namespace: String?,
        serverHost: String?,
        authType: String,
        providerHint: String?
    ) {
        self.name = name
        self.clusterName = clusterName
        self.userName = userName
        self.namespace = namespace
        self.serverHost = serverHost
        self.authType = authType
        self.providerHint = providerHint
    }
}

public struct KubeConfigImportReview: Sendable, Equatable {
    public let contexts: [KubeConfigImportContextPreview]
    public let issues: [KubeConfigImportIssue]
    public let redactedPreview: String

    public init(
        contexts: [KubeConfigImportContextPreview],
        issues: [KubeConfigImportIssue],
        redactedPreview: String
    ) {
        self.contexts = contexts
        self.issues = issues
        self.redactedPreview = redactedPreview
    }

    public var isValid: Bool {
        !issues.contains { $0.severity == .error }
    }
}

public struct KubeConfigImportValidator: Sendable {
    private let fileExists: @Sendable (String) -> Bool
    private let executableSearchPaths: [String]

    public init(
        fileExists: @escaping @Sendable (String) -> Bool = { path in FileManager.default.fileExists(atPath: path) },
        executableSearchPaths: [String] = (ProcessInfo.processInfo.environment["PATH"] ?? "")
            .split(separator: ":")
            .map(String.init)
    ) {
        self.fileExists = fileExists
        self.executableSearchPaths = executableSearchPaths
    }

    public func validate(raw: String, sourceName: String? = nil) -> KubeConfigImportReview {
        let parsed = ParsedKubeConfig(raw: raw)
        var issues = parsed.syntaxIssues

        if parsed.currentContext.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            issues.append(.init(
                id: "missing-current-context",
                severity: .error,
                message: "Kubeconfig is missing current-context."
            ))
        }

        if parsed.contexts.isEmpty {
            issues.append(.init(
                id: "missing-contexts",
                severity: .error,
                message: "Kubeconfig does not contain any contexts."
            ))
        }

        for duplicate in parsed.duplicateContextNames {
            issues.append(.init(
                id: "duplicate-context-\(duplicate)",
                severity: .error,
                message: "Kubeconfig contains duplicate context name \(duplicate). Choose update, copy, or skip before importing."
            ))
        }
        for duplicate in parsed.duplicateClusterNames {
            issues.append(.init(
                id: "duplicate-cluster-\(duplicate)",
                severity: .error,
                message: "Kubeconfig contains duplicate cluster name \(duplicate). Choose update, copy, or skip before importing."
            ))
        }
        for duplicate in parsed.duplicateUserNames {
            issues.append(.init(
                id: "duplicate-user-\(duplicate)",
                severity: .error,
                message: "Kubeconfig contains duplicate user name \(duplicate). Choose update, copy, or skip before importing."
            ))
        }

        let clustersByName = Dictionary(parsed.clusters.map { ($0.name, $0) }, uniquingKeysWith: { first, _ in first })
        let usersByName = Dictionary(parsed.users.map { ($0.name, $0) }, uniquingKeysWith: { first, _ in first })
        var previews: [KubeConfigImportContextPreview] = []

        for context in parsed.contexts {
            let cluster = context.clusterName.flatMap { clustersByName[$0] }
            let user = context.userName.flatMap { usersByName[$0] }

            if context.clusterName == nil || cluster == nil {
                issues.append(.init(
                    id: "missing-cluster-\(context.name)",
                    severity: .error,
                    message: "Context \(context.name) references a missing cluster."
                ))
            } else if cluster?.serverHost == nil {
                issues.append(.init(
                    id: "missing-server-\(context.name)",
                    severity: .error,
                    message: "Context \(context.name) is missing a valid cluster server URL."
                ))
            }

            if let user, let command = user.execCommand, !commandExists(command) {
                issues.append(.init(
                    id: "missing-exec-plugin-\(context.name)",
                    severity: .warning,
                    message: "Context \(context.name) uses exec auth command \(command), but Rune cannot find that executable on PATH."
                ))
            }

            previews.append(.init(
                name: context.name,
                clusterName: context.clusterName,
                userName: context.userName,
                namespace: context.namespace,
                serverHost: cluster?.serverHost,
                authType: user?.authType ?? "Unknown",
                providerHint: providerHint(contextName: context.name, serverHost: cluster?.serverHost, user: user)
            ))
        }

        return KubeConfigImportReview(
            contexts: previews.sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending },
            issues: issues,
            redactedPreview: redacted(raw)
        )
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

    private func providerHint(serverHost: String?, user: ParsedKubeConfig.User?) -> String? {
        let combined = [serverHost ?? "", user?.execCommand ?? ""].joined(separator: " ").lowercased()
        if combined.contains("eks") || combined.contains("aws") { return "EKS" }
        if combined.contains("gke") || combined.contains("gcloud") || combined.contains("google") { return "GKE" }
        if combined.contains("az") || combined.contains("azure") || combined.contains("kubelogin") { return "AKS" }
        return nil
    }

    private func providerHint(contextName: String, serverHost: String?, user: ParsedKubeConfig.User?) -> String? {
        let name = contextName.lowercased()
        if name.contains("minikube") { return "Minikube" }
        if name.contains("kind-") || name == "kind" { return "kind" }
        if name.contains("k3d") { return "k3d" }
        if name.contains("docker-desktop") { return "Docker Desktop" }
        if name.contains("orbstack") { return "OrbStack" }
        return providerHint(serverHost: serverHost, user: user)
    }

    private func redacted(_ raw: String) -> String {
        let sensitiveKeys: Set<String> = [
            "token",
            "id-token",
            "refresh-token",
            "client-certificate-data",
            "client-key-data",
            "client-certificate",
            "client-key",
            "certificate-authority",
            "password",
            "username"
        ]

        return raw.split(separator: "\n", omittingEmptySubsequences: false).map { line in
            let text = String(line)
            guard let colon = text.firstIndex(of: ":") else { return text }
            let key = text[..<colon].trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            guard sensitiveKeys.contains(key) else { return text }
            return "\(text[..<text.index(after: colon)]) <redacted>"
        }
        .joined(separator: "\n")
    }
}

private struct ParsedKubeConfig {
    struct Cluster {
        var name: String
        var serverHost: String?
    }

    struct Context {
        var name: String
        var clusterName: String?
        var userName: String?
        var namespace: String?
    }

    struct User {
        var name: String
        var hasToken = false
        var hasClientCertificate = false
        var execCommand: String?

        var authType: String {
            if execCommand != nil { return "Exec plugin" }
            if hasClientCertificate { return "Client certificate" }
            if hasToken { return "Token" }
            return "Unknown"
        }
    }

    var currentContext = ""
    var clusters: [Cluster] = []
    var contexts: [Context] = []
    var users: [User] = []
    var duplicateContextNames: [String] = []
    var duplicateClusterNames: [String] = []
    var duplicateUserNames: [String] = []
    var syntaxIssues: [KubeConfigImportIssue] = []

    init(raw: String) {
        if raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            syntaxIssues.append(.init(id: "empty", severity: .error, message: "Kubeconfig is empty."))
            return
        }

        enum Section {
            case clusters
            case contexts
            case users
        }

        var section: Section?
        var nestedKey: String?
        var clusterNameCounts: [String: Int] = [:]
        var contextNameCounts: [String: Int] = [:]
        var userNameCounts: [String: Int] = [:]

        func finishCluster(_ cluster: inout Cluster?) {
            if let value = cluster {
                clusters.append(value)
                clusterNameCounts[value.name, default: 0] += 1
            }
            cluster = nil
        }

        func finishContext(_ context: inout Context?) {
            if let value = context {
                contexts.append(value)
                contextNameCounts[value.name, default: 0] += 1
            }
            context = nil
        }

        func finishUser(_ user: inout User?) {
            if let value = user {
                users.append(value)
                userNameCounts[value.name, default: 0] += 1
            }
            user = nil
        }

        var currentCluster: Cluster?
        var currentContextEntry: Context?
        var currentUser: User?

        for line in raw.split(separator: "\n", omittingEmptySubsequences: false).map(String.init) {
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("#") else { continue }

            if !line.hasPrefix(" "), !line.hasPrefix("-") {
                nestedKey = nil
                if let value = Self.scalarValue(trimmed, key: "current-context") {
                    currentContext = value
                    continue
                }
                switch trimmed {
                case "clusters:":
                    finishContext(&currentContextEntry)
                    finishUser(&currentUser)
                    section = .clusters
                    continue
                case "contexts:":
                    finishCluster(&currentCluster)
                    finishUser(&currentUser)
                    section = .contexts
                    continue
                case "users:":
                    finishCluster(&currentCluster)
                    finishContext(&currentContextEntry)
                    section = .users
                    continue
                default:
                    if !trimmed.contains(":") {
                        syntaxIssues.append(.init(id: "malformed-yaml", severity: .error, message: "Kubeconfig contains malformed YAML near: \(trimmed)"))
                    }
                    continue
                }
            }

            if trimmed.hasPrefix("- name:") {
                let name = Self.scalarValue(trimmed, key: "- name") ?? ""
                switch section {
                case .clusters:
                    finishCluster(&currentCluster)
                    currentCluster = Cluster(name: name)
                case .contexts:
                    finishContext(&currentContextEntry)
                    currentContextEntry = Context(name: name)
                case .users:
                    finishUser(&currentUser)
                    currentUser = User(name: name)
                case nil:
                    syntaxIssues.append(.init(id: "list-without-section", severity: .error, message: "Kubeconfig contains a list item outside clusters, contexts, or users."))
                }
                nestedKey = nil
                continue
            }

            if trimmed.hasSuffix(":") {
                nestedKey = String(trimmed.dropLast()).trimmingCharacters(in: .whitespacesAndNewlines)
                continue
            }

            switch (section, nestedKey) {
            case (.clusters?, "cluster"):
                if let value = Self.scalarValue(trimmed, key: "server") {
                    currentCluster?.serverHost = Self.serverHost(from: value)
                }
            case (.contexts?, "context"):
                if let value = Self.scalarValue(trimmed, key: "cluster") {
                    currentContextEntry?.clusterName = value
                } else if let value = Self.scalarValue(trimmed, key: "user") {
                    currentContextEntry?.userName = value
                } else if let value = Self.scalarValue(trimmed, key: "namespace") {
                    currentContextEntry?.namespace = value
                }
            case (.users?, "user"):
                if Self.scalarValue(trimmed, key: "token") != nil {
                    currentUser?.hasToken = true
                } else if Self.scalarValue(trimmed, key: "client-certificate-data") != nil {
                    currentUser?.hasClientCertificate = true
                }
            case (.users?, "exec"):
                if let value = Self.scalarValue(trimmed, key: "command") {
                    currentUser?.execCommand = value
                }
            default:
                continue
            }
        }

        finishCluster(&currentCluster)
        finishContext(&currentContextEntry)
        finishUser(&currentUser)
        duplicateContextNames = contextNameCounts
            .filter { $0.value > 1 }
            .map(\.key)
            .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
        duplicateClusterNames = clusterNameCounts
            .filter { $0.value > 1 }
            .map(\.key)
            .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
        duplicateUserNames = userNameCounts
            .filter { $0.value > 1 }
            .map(\.key)
            .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
    }

    private static func scalarValue(_ line: String, key: String) -> String? {
        let prefix = "\(key):"
        guard line.hasPrefix(prefix) else { return nil }
        return String(line.dropFirst(prefix.count))
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
    }

    private static func serverHost(from raw: String) -> String? {
        guard let url = URL(string: raw), let host = url.host, !host.isEmpty else { return nil }
        return host
    }
}
