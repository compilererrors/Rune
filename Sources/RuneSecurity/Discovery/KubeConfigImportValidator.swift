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
    public let sourceName: String?

    public init(
        contexts: [KubeConfigImportContextPreview],
        issues: [KubeConfigImportIssue],
        redactedPreview: String,
        sourceName: String? = nil
    ) {
        self.contexts = contexts
        self.issues = issues
        self.redactedPreview = redactedPreview
        self.sourceName = sourceName
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

        let currentContext = parsed.currentContext.trimmingCharacters(in: .whitespacesAndNewlines)
        let parsedContextNames = Set(parsed.contexts.map(\.name))
        if !currentContext.isEmpty, !parsedContextNames.contains(currentContext) {
            issues.append(.init(
                id: "missing-current-context-reference",
                severity: .error,
                message: "Kubeconfig current-context \(currentContext) does not match any context."
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
            redactedPreview: redacted(raw),
            sourceName: sourceName
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
        let combined = ([serverHost ?? "", user?.execCommand ?? "", user?.authProviderName ?? ""] + (user?.authProviderConfigValues ?? []))
            .joined(separator: " ")
            .lowercased()
        if combined.contains("eks") || combined.contains("aws") { return "EKS" }
        if combined.contains("gke") || combined.contains("gcloud") || combined.contains("google") { return "GKE" }
        if combined.contains("az") || combined.contains("azure") || combined.contains("kubelogin") { return "AKS" }
        if combined.contains("digitalocean") || combined.contains("doctl") || combined.contains("doks") { return "DOKS" }
        if combined.contains("rancher") { return "Rancher" }
        if combined.contains("openshift") || combined.contains("crc") || combined.contains("oc ") { return "OpenShift" }
        if combined.contains("oidc") || combined.contains("id-token") || combined.contains("client-id") { return "OIDC" }
        if combined.contains("k3s") { return "K3s" }
        return nil
    }

    private func providerHint(contextName: String, serverHost: String?, user: ParsedKubeConfig.User?) -> String? {
        let name = contextName.lowercased()
        if name.contains("minikube") { return "Minikube" }
        if name.contains("kind-") || name == "kind" { return "kind" }
        if name.contains("k3s") { return "K3s" }
        if name.contains("k3d") { return "k3d" }
        if name.contains("docker-desktop") { return "Docker Desktop" }
        if name.contains("orbstack") { return "OrbStack" }
        if name.contains("doks") || name.contains("digitalocean") { return "DOKS" }
        if name.contains("rancher") { return "Rancher" }
        if name.contains("openshift") || name.contains("crc") { return "OpenShift" }
        return providerHint(serverHost: serverHost, user: user)
    }

    private func redacted(_ raw: String) -> String {
        let sensitiveKeys: Set<String> = [
            "token",
            "id-token",
            "access-token",
            "refresh-token",
            "client-certificate-data",
            "client-key-data",
            "client-certificate",
            "client-key",
            "certificate-authority",
            "certificate-authority-data",
            "password",
            "username",
            "tokenfile",
            "token-file"
        ]

        return raw.split(separator: "\n", omittingEmptySubsequences: false).map { line in
            let text = String(line)
            guard let colon = text.firstIndex(of: ":") else { return text }
            let key = normalizedRedactionKey(String(text[..<colon]))
            guard sensitiveKeys.contains(key) else { return text }
            return "\(text[..<text.index(after: colon)]) <redacted>"
        }
        .joined(separator: "\n")
    }

    private func normalizedRedactionKey(_ rawKey: String) -> String {
        var key = rawKey.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if key.hasPrefix("-") {
            key = String(key.dropFirst()).trimmingCharacters(in: .whitespacesAndNewlines)
        }
        return key
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
        var hasTokenFile = false
        var hasBasicAuth = false
        var hasClientCertificate = false
        var authProviderName: String?
        var authProviderConfigValues: [String] = []
        var execCommand: String?

        var authType: String {
            if execCommand != nil { return "Exec plugin" }
            if authProviderName?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == "oidc" { return "OIDC" }
            if hasTokenFile { return "Token file" }
            if hasClientCertificate { return "Client certificate" }
            if hasToken { return "Token" }
            if hasBasicAuth { return "Basic" }
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
        let raw = Self.normalized(raw)
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
        var authProviderConfigKey: String?
        var clusterNameCounts: [String: Int] = [:]
        var contextNameCounts: [String: Int] = [:]
        var userNameCounts: [String: Int] = [:]

        func finishCluster(_ cluster: inout Cluster?) {
            if let value = cluster {
                guard !value.name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                    syntaxIssues.append(.init(
                        id: "cluster-missing-name",
                        severity: .error,
                        message: "Kubeconfig contains a cluster entry without a name."
                    ))
                    cluster = nil
                    return
                }
                clusters.append(value)
                clusterNameCounts[value.name, default: 0] += 1
            }
            cluster = nil
        }

        func finishContext(_ context: inout Context?) {
            if let value = context {
                guard !value.name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                    syntaxIssues.append(.init(
                        id: "context-missing-name",
                        severity: .error,
                        message: "Kubeconfig contains a context entry without a name."
                    ))
                    context = nil
                    return
                }
                contexts.append(value)
                contextNameCounts[value.name, default: 0] += 1
            }
            context = nil
        }

        func finishUser(_ user: inout User?) {
            if let value = user {
                guard !value.name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                    syntaxIssues.append(.init(
                        id: "user-missing-name",
                        severity: .error,
                        message: "Kubeconfig contains a user entry without a name."
                    ))
                    user = nil
                    return
                }
                users.append(value)
                userNameCounts[value.name, default: 0] += 1
            }
            user = nil
        }

        var currentCluster: Cluster?
        var currentContextEntry: Context?
        var currentUser: User?

        for line in raw.split(separator: "\n", omittingEmptySubsequences: false).map(String.init) {
            let line = Self.removingInlineComment(from: line)
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("#") else { continue }
            let indent = line.prefix { $0 == " " }.count

            if let message = Self.unsupportedYAMLFeatureMessage(in: trimmed) {
                syntaxIssues.append(.init(
                    id: "unsupported-yaml-feature",
                    severity: .error,
                    message: message
                ))
                continue
            }

            if trimmed == "---" {
                syntaxIssues.append(.init(
                    id: "unsupported-multi-document",
                    severity: .error,
                    message: "Kubeconfig must contain exactly one YAML document."
                ))
                continue
            }

            if !line.hasPrefix(" "), !line.hasPrefix("-") {
                nestedKey = nil
                if let value = Self.scalarValue(trimmed, key: "current-context") {
                    currentContext = value
                    continue
                }
                switch trimmed {
                case "clusters:", "clusters: []":
                    finishContext(&currentContextEntry)
                    finishUser(&currentUser)
                    section = .clusters
                    continue
                case "contexts:", "contexts: []":
                    finishCluster(&currentCluster)
                    finishUser(&currentUser)
                    section = .contexts
                    continue
                case "users:", "users: []":
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

            if trimmed.hasPrefix("- name:"), indent <= 2 {
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

            if let listItemKey = Self.typedListItemKey(from: trimmed), indent <= 2 {
                switch (section, listItemKey) {
                case (.clusters?, "cluster"):
                    finishCluster(&currentCluster)
                    currentCluster = Cluster(name: "")
                case (.contexts?, "context"):
                    finishContext(&currentContextEntry)
                    currentContextEntry = Context(name: "")
                case (.users?, "user"):
                    finishUser(&currentUser)
                    currentUser = User(name: "")
                case (nil, _):
                    syntaxIssues.append(.init(
                        id: "list-without-section",
                        severity: .error,
                        message: "Kubeconfig contains a list item outside clusters, contexts, or users."
                    ))
                default:
                    syntaxIssues.append(.init(
                        id: "malformed-yaml",
                        severity: .error,
                        message: "Kubeconfig contains a \(listItemKey) item in the wrong section."
                    ))
                }
                nestedKey = listItemKey
                continue
            }

            if Self.isAnchorOnlyListItem(trimmed), indent <= 2 {
                switch section {
                case .clusters:
                    finishCluster(&currentCluster)
                    currentCluster = Cluster(name: "")
                case .contexts:
                    finishContext(&currentContextEntry)
                    currentContextEntry = Context(name: "")
                case .users:
                    finishUser(&currentUser)
                    currentUser = User(name: "")
                case nil:
                    syntaxIssues.append(.init(id: "list-without-section", severity: .error, message: "Kubeconfig contains a list item outside clusters, contexts, or users."))
                }
                nestedKey = nil
                continue
            }

            if section == .users, trimmed == "auth-provider:" {
                nestedKey = "auth-provider"
                authProviderConfigKey = nil
                continue
            }

            if section == .users, nestedKey == "auth-provider", trimmed == "config:" {
                authProviderConfigKey = "config"
                continue
            }

            if trimmed.hasSuffix(":") {
                nestedKey = String(trimmed.dropLast()).trimmingCharacters(in: .whitespacesAndNewlines)
                authProviderConfigKey = nil
                continue
            }

            if indent <= 2, let value = Self.scalarValue(trimmed, key: "name") {
                switch section {
                case .clusters:
                    if currentCluster == nil {
                        currentCluster = Cluster(name: value)
                    } else {
                        currentCluster?.name = value
                    }
                case .contexts:
                    if currentContextEntry == nil {
                        currentContextEntry = Context(name: value)
                    } else {
                        currentContextEntry?.name = value
                    }
                case .users:
                    if currentUser == nil {
                        currentUser = User(name: value)
                    } else {
                        currentUser?.name = value
                    }
                case nil:
                    break
                }
                continue
            }

            switch (section, nestedKey) {
            case (.clusters?, "cluster"):
                if let value = Self.scalarValue(trimmed, key: "server") {
                    currentCluster?.serverHost = Self.serverHost(from: value)
                } else if let value = Self.scalarValue(trimmed, key: "name") {
                    currentCluster?.name = value
                }
            case (.contexts?, "context"):
                if let value = Self.scalarValue(trimmed, key: "cluster") {
                    currentContextEntry?.clusterName = value
                } else if let value = Self.scalarValue(trimmed, key: "user") {
                    currentContextEntry?.userName = value
                } else if let value = Self.scalarValue(trimmed, key: "namespace") {
                    currentContextEntry?.namespace = value
                } else if let value = Self.scalarValue(trimmed, key: "name") {
                    currentContextEntry?.name = value
                }
            case (.users?, "user"):
                if Self.scalarValue(trimmed, key: "token") != nil {
                    currentUser?.hasToken = true
                } else if Self.scalarValue(trimmed, key: "id-token") != nil {
                    currentUser?.hasToken = true
                } else if Self.scalarValue(trimmed, key: "access-token") != nil {
                    currentUser?.hasToken = true
                } else if Self.scalarValue(trimmed, key: "tokenFile") != nil {
                    currentUser?.hasTokenFile = true
                } else if Self.scalarValue(trimmed, key: "token-file") != nil {
                    currentUser?.hasTokenFile = true
                } else if Self.scalarValue(trimmed, key: "username") != nil {
                    currentUser?.hasBasicAuth = true
                } else if Self.scalarValue(trimmed, key: "password") != nil {
                    currentUser?.hasBasicAuth = true
                } else if Self.scalarValue(trimmed, key: "client-certificate-data") != nil {
                    currentUser?.hasClientCertificate = true
                } else if Self.scalarValue(trimmed, key: "client-certificate") != nil {
                    currentUser?.hasClientCertificate = true
                } else if Self.scalarValue(trimmed, key: "client-key-data") != nil {
                    currentUser?.hasClientCertificate = true
                } else if Self.scalarValue(trimmed, key: "client-key") != nil {
                    currentUser?.hasClientCertificate = true
                } else if trimmed == "auth-provider:" {
                    nestedKey = "auth-provider"
                    authProviderConfigKey = nil
                } else if let value = Self.scalarValue(trimmed, key: "name") {
                    currentUser?.name = value
                }
            case (.users?, "exec"):
                if let value = Self.scalarValue(trimmed, key: "command") {
                    currentUser?.execCommand = value
                }
            case (.users?, "auth-provider"):
                if let value = Self.scalarValue(trimmed, key: "name") {
                    currentUser?.authProviderName = value
                } else if trimmed == "config:" {
                    authProviderConfigKey = "config"
                } else if authProviderConfigKey == "config",
                          let value = Self.anyScalarValue(trimmed) {
                    currentUser?.authProviderConfigValues.append(value)
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

    private static func anyScalarValue(_ line: String) -> String? {
        guard let colon = line.firstIndex(of: ":") else { return nil }
        let value = String(line[line.index(after: colon)...])
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
        return value.isEmpty ? nil : value
    }

    private static func normalized(_ raw: String) -> String {
        var value = raw
        if value.hasPrefix("\u{FEFF}") {
            value.removeFirst()
        }
        value = value.replacingOccurrences(of: "\r\n", with: "\n")
        value = value.replacingOccurrences(of: "\r", with: "\n")
        return value
    }

    private static func removingInlineComment(from line: String) -> String {
        var result = ""
        var inSingleQuote = false
        var inDoubleQuote = false
        var previous: Character?

        for character in line {
            if character == "'", !inDoubleQuote {
                inSingleQuote.toggle()
            } else if character == "\"", !inSingleQuote, previous != "\\" {
                inDoubleQuote.toggle()
            } else if character == "#", !inSingleQuote, !inDoubleQuote {
                if let last = result.last, last.isWhitespace {
                    break
                }
            }

            result.append(character)
            previous = character
        }

        return trimmingTrailingWhitespace(result)
    }

    private static func typedListItemKey(from trimmed: String) -> String? {
        switch trimmed {
        case "- cluster:":
            return "cluster"
        case "- context:":
            return "context"
        case "- user:":
            return "user"
        default:
            return nil
        }
    }

    private static func isAnchorOnlyListItem(_ trimmed: String) -> Bool {
        guard trimmed.hasPrefix("- &") else { return false }
        let rest = trimmed.dropFirst(2)
        return !rest.isEmpty && !rest.contains(" ") && !rest.contains(":")
    }

    private static func unsupportedYAMLFeatureMessage(in trimmed: String) -> String? {
        if trimmed.hasPrefix("- *") {
            return "Kubeconfig uses YAML aliases, which are not supported by Rune's safe kubeconfig importer."
        }
        guard let colon = trimmed.firstIndex(of: ":") else { return nil }
        let key = trimmed[..<colon].trimmingCharacters(in: .whitespacesAndNewlines)
        let value = trimmed[trimmed.index(after: colon)...].trimmingCharacters(in: .whitespacesAndNewlines)
        if key == "<<" {
            return "Kubeconfig uses YAML merge keys, which are not supported by Rune's safe kubeconfig importer."
        }
        if value.hasPrefix("*") {
            return "Kubeconfig uses YAML aliases, which are not supported by Rune's safe kubeconfig importer."
        }
        if value.hasPrefix("&") {
            return "Kubeconfig uses anchored scalar values, which are not supported by Rune's safe kubeconfig importer."
        }
        return nil
    }

    private static func trimmingTrailingWhitespace(_ value: String) -> String {
        var result = value
        while let last = result.last, last.isWhitespace {
            result.removeLast()
        }
        return result
    }

    private static func serverHost(from raw: String) -> String? {
        guard let url = URL(string: raw), let host = url.host, !host.isEmpty else { return nil }
        return host
    }
}
