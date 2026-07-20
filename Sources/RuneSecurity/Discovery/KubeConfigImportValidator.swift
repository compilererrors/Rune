import Foundation
import Yams

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

public enum KubeConfigDuplicateHandlingChoice: String, CaseIterable, Sendable, Equatable {
    case updateExisting
    case importAsCopy
    case skipDuplicate

    public var title: String {
        switch self {
        case .updateExisting: return "Update existing"
        case .importAsCopy: return "Import as copy"
        case .skipDuplicate: return "Skip duplicate"
        }
    }

    public var detail: String {
        switch self {
        case .updateExisting:
            return "Keep the last definition for each duplicated name."
        case .importAsCopy:
            return "Keep every definition with deterministic copy names and rewritten references."
        case .skipDuplicate:
            return "Keep the first definition for each duplicated name. New contexts that redefine a loaded cluster or user name are blocked."
        }
    }
}

public struct KubeConfigImportReview: Sendable, Equatable {
    public let contexts: [KubeConfigImportContextPreview]
    public let issues: [KubeConfigImportIssue]
    public let redactedPreview: String
    public let sourceName: String?
    public let hasDuplicateConflicts: Bool

    public init(
        contexts: [KubeConfigImportContextPreview],
        issues: [KubeConfigImportIssue],
        redactedPreview: String,
        sourceName: String? = nil,
        hasDuplicateConflicts: Bool = false
    ) {
        self.contexts = contexts
        self.issues = issues
        self.redactedPreview = redactedPreview
        self.sourceName = sourceName
        self.hasDuplicateConflicts = hasDuplicateConflicts
    }

    public var isValid: Bool {
        !issues.contains { $0.severity == .error }
    }

    public var duplicateHandlingChoices: [KubeConfigDuplicateHandlingChoice] {
        hasDuplicateConflicts || issues.contains { $0.id.contains("duplicate") }
            ? KubeConfigDuplicateHandlingChoice.allCases
            : []
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
        var parsedContextNames = Set<String>()
        parsedContextNames.reserveCapacity(parsed.contexts.count)
        for context in parsed.contexts {
            parsedContextNames.insert(context.name)
        }
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

        var clustersByName: [String: ParsedKubeConfig.Cluster] = [:]
        clustersByName.reserveCapacity(parsed.clusters.count)
        for cluster in parsed.clusters where clustersByName[cluster.name] == nil {
            clustersByName[cluster.name] = cluster
        }
        var usersByName: [String: ParsedKubeConfig.User] = [:]
        usersByName.reserveCapacity(parsed.users.count)
        for user in parsed.users where usersByName[user.name] == nil {
            usersByName[user.name] = user
        }
        var previews: [KubeConfigImportContextPreview] = []
        previews.reserveCapacity(parsed.contexts.count)
        let nativelyHandledExecContexts = Set(
            (try? KubeConfigNativeAuthAnalyzer().analyze(raw: raw).contexts.compactMap { descriptor in
                descriptor.credentialRequest == nil ? nil : descriptor.contextName
            }) ?? []
        )

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

            if let user,
               let command = user.execCommand,
               !commandExists(command),
               !nativelyHandledExecContexts.contains(context.name) {
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

    private typealias YAMLMapping = [AnyHashable: Any]

    private static let redactedValue = "<redacted>"
    private static let unavailableRedactedPreview = "# Kubeconfig preview unavailable because it could not be safely redacted."
    private static let sensitiveRedactionKeys: Set<String> = [
        "token",
        "idtoken",
        "accesstoken",
        "refreshtoken",
        "bearertoken",
        "tokenfile",
        "clientcertificate",
        "clientcertificatedata",
        "clientkey",
        "clientkeydata",
        "certificateauthority",
        "certificateauthoritydata",
        "password",
        "username",
        "clientsecret",
        "secret",
        "secretaccesskey",
        "privatekey",
        "privatekeydata",
        "apikey",
        "credential",
        "credentials"
    ]

    /// Uses an indentation-aware fast path for ordinary block YAML and structural
    /// redaction for complex syntax. Unsafe structural parsing always fails closed.
    private func redacted(_ raw: String) -> String {
        if Self.requiresStructuralPreviewRedaction(raw) {
            return structurallyRedacted(raw)
        }
        return Self.fastBlockYAMLRedaction(raw) ?? structurallyRedacted(raw)
    }

    /// Common kubeconfigs use block mappings with scalar values. This path keeps large
    /// folder imports snappy while tracking indentation so a sensitive block or folded
    /// scalar is removed in full, not only on its key line.
    private enum FastPreviewKeyKind {
        case ordinary
        case sensitive
        case requiresStructuralRedaction
    }

    private static func fastBlockYAMLRedaction(_ raw: String) -> String? {
        let lines = raw.split(separator: "\n", omittingEmptySubsequences: false)
        var output = ""
        output.reserveCapacity(raw.utf8.count)
        var hasOutputLine = false
        var sensitiveParentIndent: Int?

        for lineSlice in lines {
            let indent = lineSlice.prefix(while: { $0 == " " }).count
            let content = lineSlice.dropFirst(indent)

            if let parentIndent = sensitiveParentIndent {
                if content.isEmpty || indent > parentIndent {
                    continue
                }
                sensitiveParentIndent = nil
            }

            guard !content.isEmpty, let colon = lineSlice.firstIndex(of: ":") else {
                appendFastPreviewLine(lineSlice, to: &output, hasOutputLine: &hasOutputLine)
                continue
            }

            switch fastPreviewKeyKind(lineSlice[..<colon]) {
            case .ordinary:
                appendFastPreviewLine(lineSlice, to: &output, hasOutputLine: &hasOutputLine)
            case .requiresStructuralRedaction:
                return nil
            case .sensitive:
                if hasOutputLine { output.append("\n") }
                output.append(contentsOf: lineSlice[..<lineSlice.index(after: colon)])
                output.append(" ")
                output.append(redactedValue)
                hasOutputLine = true
                sensitiveParentIndent = indent
            }
        }
        return output
    }

    private static func appendFastPreviewLine(
        _ line: Substring,
        to output: inout String,
        hasOutputLine: inout Bool
    ) {
        if hasOutputLine { output.append("\n") }
        output.append(contentsOf: line)
        hasOutputLine = true
    }

    private static func fastPreviewKeyKind(_ rawKey: Substring) -> FastPreviewKeyKind {
        var key = rawKey
        while let first = key.first, first == " " || first == "-" {
            key = key.dropFirst()
        }
        while let last = key.last, last == " " {
            key = key.dropLast()
        }

        switch key {
        case "env", "args":
            return .requiresStructuralRedaction
        case "token", "id-token", "access-token", "refresh-token", "bearer-token",
             "tokenfile", "token-file", "tokenFile", "client-certificate",
             "client-certificate-data", "client-key", "client-key-data",
             "certificate-authority", "certificate-authority-data", "password",
             "username", "client-secret", "secret", "secret-access-key",
             "private-key", "private-key-data", "api-key", "apikey",
             "credential", "credentials":
            return .sensitive
        default:
            break
        }

        // Ordinary kubeconfig keys are lowercase ASCII plus separators. Only unusual
        // quoting/casing needs the more general Unicode-normalized comparison.
        let isOrdinaryASCII = key.utf8.allSatisfy { byte in
            (byte >= 97 && byte <= 122) || (byte >= 48 && byte <= 57) || byte == 45 || byte == 95
        }
        guard !isOrdinaryASCII else { return .ordinary }

        let normalized = normalizedRedactionKey(String(key))
        if normalized == "env" || normalized == "args" {
            return .requiresStructuralRedaction
        }
        return isSensitiveRedactionKey(normalized) ? .sensitive : .ordinary
    }

    /// Complex syntax is delegated to Yams. Comments are included because they can
    /// contain copied credentials and the structural dump deliberately drops them.
    private static func requiresStructuralPreviewRedaction(_ raw: String) -> Bool {
        raw.utf8.contains { byte in
            switch byte {
            case 9, 33, 35, 38, 42, 63, 64, 91, 123:
                return true // tab, !, #, &, *, ?, @, [, {
            default:
                return false
            }
        }
    }

    private func structurallyRedacted(_ raw: String) -> String {
        do {
            var documents: [Any] = []
            var sequence = try load_all(yaml: raw)
            while let document = sequence.next() {
                documents.append(document)
            }
            guard sequence.error == nil, !documents.isEmpty else {
                return Self.unavailableRedactedPreview
            }

            var sensitiveScalarValues = Set<String>()
            for document in documents {
                collectSensitiveScalarValues(in: document, into: &sensitiveScalarValues)
            }
            let previews = try documents.map { document in
                try dump(
                    object: sanitizedPreviewValue(document, sensitiveScalarValues: sensitiveScalarValues),
                    indent: 2,
                    width: -1,
                    allowUnicode: true,
                    sortKeys: false
                )
                .trimmingCharacters(in: .newlines)
            }
            return previews.joined(separator: "\n---\n")
        } catch {
            return Self.unavailableRedactedPreview
        }
    }

    private func collectSensitiveScalarValues(in value: Any, into values: inout Set<String>) {
        if let mapping = value as? YAMLMapping {
            for (key, child) in mapping {
                let normalizedKey = (key.base as? String).map(Self.normalizedRedactionKey)
                if normalizedKey.map(Self.isSensitiveRedactionKey) == true {
                    collectScalarStrings(in: child, into: &values)
                } else if normalizedKey == "env" {
                    collectEnvironmentValues(in: child, into: &values)
                } else if normalizedKey == "args" {
                    collectSensitiveArgumentValues(in: child, into: &values)
                } else if (normalizedKey == "server" || normalizedKey == "proxyurl"),
                          let string = child as? String,
                          Self.isSecretBearingURL(string) {
                    values.insert(string)
                }
                collectSensitiveScalarValues(in: child, into: &values)
            }
        } else if let items = value as? [Any] {
            for item in items {
                collectSensitiveScalarValues(in: item, into: &values)
            }
        }
    }

    private func collectScalarStrings(in value: Any, into values: inout Set<String>) {
        if let string = value as? String {
            if !string.isEmpty {
                values.insert(string)
            }
        } else if let mapping = value as? YAMLMapping {
            for child in mapping.values {
                collectScalarStrings(in: child, into: &values)
            }
        } else if let items = value as? [Any] {
            for item in items {
                collectScalarStrings(in: item, into: &values)
            }
        }
    }

    private func collectEnvironmentValues(in value: Any, into values: inout Set<String>) {
        guard let entries = value as? [Any] else { return }
        for entry in entries {
            guard let mapping = entry as? YAMLMapping else { continue }
            for (key, child) in mapping where (key.base as? String).map(Self.normalizedRedactionKey) == "value" {
                collectScalarStrings(in: child, into: &values)
            }
        }
    }

    private func collectSensitiveArgumentValues(in value: Any, into values: inout Set<String>) {
        guard let arguments = value as? [Any] else { return }
        var redactNext = false
        for argument in arguments {
            guard let string = argument as? String else { continue }
            if redactNext {
                if !string.isEmpty { values.insert(string) }
                redactNext = false
                continue
            }
            let parsed = Self.parsedSensitiveArgument(string)
            if let value = parsed.inlineValue, !value.isEmpty {
                values.insert(value)
            } else if parsed.redactsFollowingValue {
                redactNext = true
            }
        }
    }

    private func sanitizedPreviewValue(
        _ value: Any,
        sensitiveScalarValues: Set<String>,
        environmentEntry: Bool = false
    ) -> Any {
        if let string = value as? String {
            return sensitiveScalarValues.contains(string) ? Self.redactedValue : string
        }
        if let mapping = value as? YAMLMapping {
            var sanitized: YAMLMapping = [:]
            sanitized.reserveCapacity(mapping.count)
            for (key, child) in mapping {
                let normalizedKey = (key.base as? String).map(Self.normalizedRedactionKey)
                if normalizedKey.map(Self.isSensitiveRedactionKey) == true
                    || (environmentEntry && normalizedKey == "value") {
                    sanitized[key] = Self.redactedValue
                } else if normalizedKey == "env" {
                    sanitized[key] = sanitizedEnvironmentValue(
                        child,
                        sensitiveScalarValues: sensitiveScalarValues
                    )
                } else if normalizedKey == "args" {
                    sanitized[key] = sanitizedArgumentValue(
                        child,
                        sensitiveScalarValues: sensitiveScalarValues
                    )
                } else if (normalizedKey == "server" || normalizedKey == "proxyurl"),
                          let string = child as? String,
                          Self.isSecretBearingURL(string) {
                    sanitized[key] = Self.redactedValue
                } else {
                    sanitized[key] = sanitizedPreviewValue(
                        child,
                        sensitiveScalarValues: sensitiveScalarValues
                    )
                }
            }
            return sanitized
        }
        if let items = value as? [Any] {
            return items.map {
                sanitizedPreviewValue($0, sensitiveScalarValues: sensitiveScalarValues)
            }
        }
        return value
    }

    private func sanitizedEnvironmentValue(_ value: Any, sensitiveScalarValues: Set<String>) -> Any {
        guard let entries = value as? [Any] else {
            return sanitizedPreviewValue(value, sensitiveScalarValues: sensitiveScalarValues)
        }
        return entries.map {
            sanitizedPreviewValue(
                $0,
                sensitiveScalarValues: sensitiveScalarValues,
                environmentEntry: true
            )
        }
    }

    private func sanitizedArgumentValue(_ value: Any, sensitiveScalarValues: Set<String>) -> Any {
        guard let arguments = value as? [Any] else {
            return sanitizedPreviewValue(value, sensitiveScalarValues: sensitiveScalarValues)
        }
        var redactNext = false
        return arguments.map { argument -> Any in
            guard let string = argument as? String else {
                return sanitizedPreviewValue(argument, sensitiveScalarValues: sensitiveScalarValues)
            }
            if redactNext {
                redactNext = false
                return Self.redactedValue
            }
            let parsed = Self.parsedSensitiveArgument(string)
            if parsed.redactsFollowingValue {
                redactNext = true
                return string
            }
            if let inlineValue = parsed.inlineValue {
                return string.replacingOccurrences(of: inlineValue, with: Self.redactedValue)
            }
            return sensitiveScalarValues.contains(string) ? Self.redactedValue : string
        }
    }

    private static func parsedSensitiveArgument(
        _ argument: String
    ) -> (redactsFollowingValue: Bool, inlineValue: String?) {
        let trimmed = argument.trimmingCharacters(in: .whitespacesAndNewlines)
        let withoutPrefix = trimmed.drop(while: { $0 == "-" })
        if let separator = withoutPrefix.firstIndex(of: "=") {
            let name = String(withoutPrefix[..<separator])
            guard isSensitiveArgumentName(name) else { return (false, nil) }
            return (false, String(withoutPrefix[withoutPrefix.index(after: separator)...]))
        }
        return (isSensitiveArgumentName(String(withoutPrefix)), nil)
    }

    private static func isSensitiveArgumentName(_ name: String) -> Bool {
        let normalized = normalizedRedactionKey(name)
        return isSensitiveRedactionKey(normalized)
            || normalized.contains("token")
            || normalized.contains("password")
            || normalized.contains("secret")
            || normalized.contains("credential")
            || normalized.contains("privatekey")
            || normalized.contains("apikey")
    }

    private static func isSensitiveRedactionKey(_ normalizedKey: String) -> Bool {
        sensitiveRedactionKeys.contains(normalizedKey)
    }

    private static func normalizedRedactionKey(_ rawKey: String) -> String {
        rawKey
            .lowercased()
            .filter { $0.isLetter || $0.isNumber }
    }

    private static func isSecretBearingURL(_ value: String) -> Bool {
        guard let components = URLComponents(string: value) else { return true }
        if components.user != nil || components.password != nil || components.fragment != nil {
            return true
        }
        return components.queryItems?.contains { item in
            isSensitiveArgumentName(item.name)
        } == true
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

        let yamlReferences = Self.mayContainYAMLReferences(raw)
            ? Self.referenceIndex(for: raw)
            : YAMLReferenceIndex()
        syntaxIssues.append(contentsOf: yamlReferences.issues)

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
        var reportedUnresolvedAliases = Set<String>()

        func unresolvedAlias(_ alias: String) {
            guard reportedUnresolvedAliases.insert(alias).inserted else { return }
            syntaxIssues.append(.init(
                id: "unresolved-yaml-alias-\(alias)",
                severity: .error,
                message: "Kubeconfig references YAML alias \(alias), but no matching anchor was found."
            ))
        }

        func resolvedScalar(_ rawValue: String) -> String? {
            let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.hasPrefix("\"") || trimmed.hasPrefix("'") {
                return trimmed.trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
            }
            let value = trimmed.trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
            if value.hasPrefix("*") {
                let alias = String(value.dropFirst()).trimmingCharacters(in: .whitespacesAndNewlines)
                guard let resolved = yamlReferences.scalarAnchors[alias] else {
                    unresolvedAlias(alias)
                    return nil
                }
                return resolved
            }
            if value.hasPrefix("&") {
                let parts = value.dropFirst().split(maxSplits: 1, whereSeparator: \.isWhitespace)
                guard parts.count == 2 else { return nil }
                return String(parts[1]).trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
            }
            return value
        }

        func scalarValue(_ line: String, key: String) -> String? {
            guard let value = Self.rawScalarValue(line, key: key) else { return nil }
            return resolvedScalar(value)
        }

        func anyScalarValue(_ line: String) -> String? {
            guard let value = Self.rawAnyScalarValue(line) else { return nil }
            return resolvedScalar(value)
        }

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

        func mappingValue(_ mapping: [String: String], keys: [String]) -> String? {
            for key in keys {
                if let value = mapping[key], let resolved = resolvedScalar(value) {
                    return resolved
                }
            }
            return nil
        }

        func applyUserMapping(_ mapping: [String: String], to user: inout User) {
            if mappingValue(mapping, keys: ["user.token", "token"]) != nil ||
                mappingValue(mapping, keys: ["user.id-token", "id-token"]) != nil ||
                mappingValue(mapping, keys: ["user.access-token", "access-token"]) != nil {
                user.hasToken = true
            }
            if mappingValue(mapping, keys: ["user.tokenFile", "tokenFile", "user.token-file", "token-file"]) != nil {
                user.hasTokenFile = true
            }
            if mappingValue(mapping, keys: ["user.username", "username"]) != nil ||
                mappingValue(mapping, keys: ["user.password", "password"]) != nil {
                user.hasBasicAuth = true
            }
            if mappingValue(mapping, keys: ["user.client-certificate-data", "client-certificate-data"]) != nil ||
                mappingValue(mapping, keys: ["user.client-certificate", "client-certificate"]) != nil ||
                mappingValue(mapping, keys: ["user.client-key-data", "client-key-data"]) != nil ||
                mappingValue(mapping, keys: ["user.client-key", "client-key"]) != nil {
                user.hasClientCertificate = true
            }
            if let value = mappingValue(mapping, keys: ["user.exec.command", "exec.command"]) {
                user.execCommand = value
            }
            if let value = mappingValue(mapping, keys: ["user.auth-provider.name", "auth-provider.name"]) {
                user.authProviderName = value
            }
            for key in mapping.keys where key.hasPrefix("user.auth-provider.config.") || key.hasPrefix("auth-provider.config.") {
                if let value = mappingValue(mapping, keys: [key]) {
                    user.authProviderConfigValues.append(value)
                }
            }
        }

        func applyMappingAlias(_ alias: String, section: Section?, nestedKey: String?) {
            guard let mapping = yamlReferences.mappingAnchors[alias] else {
                unresolvedAlias(alias)
                return
            }

            switch (section, nestedKey) {
            case (.clusters?, "cluster"):
                if let value = mappingValue(mapping, keys: ["cluster.server", "server"]) {
                    currentCluster?.serverHost = Self.serverHost(from: value)
                }
            case (.contexts?, "context"):
                if let value = mappingValue(mapping, keys: ["context.cluster", "cluster"]) {
                    currentContextEntry?.clusterName = value
                }
                if let value = mappingValue(mapping, keys: ["context.user", "user"]) {
                    currentContextEntry?.userName = value
                }
                if let value = mappingValue(mapping, keys: ["context.namespace", "namespace"]) {
                    currentContextEntry?.namespace = value
                }
            case (.users?, "user"):
                if var user = currentUser {
                    applyUserMapping(mapping, to: &user)
                    currentUser = user
                }
            default:
                break
            }
        }

        func appendAliasListItem(_ alias: String, section: Section?) {
            guard let mapping = yamlReferences.mappingAnchors[alias] else {
                unresolvedAlias(alias)
                return
            }

            switch section {
            case .clusters:
                finishCluster(&currentCluster)
                currentCluster = Cluster(
                    name: mappingValue(mapping, keys: ["name"]) ?? "",
                    serverHost: mappingValue(mapping, keys: ["cluster.server", "server"]).flatMap(Self.serverHost(from:))
                )
            case .contexts:
                finishContext(&currentContextEntry)
                currentContextEntry = Context(
                    name: mappingValue(mapping, keys: ["name"]) ?? "",
                    clusterName: mappingValue(mapping, keys: ["context.cluster", "cluster"]),
                    userName: mappingValue(mapping, keys: ["context.user", "user"]),
                    namespace: mappingValue(mapping, keys: ["context.namespace", "namespace"])
                )
            case .users:
                finishUser(&currentUser)
                var user = User(name: mappingValue(mapping, keys: ["name"]) ?? "")
                applyUserMapping(mapping, to: &user)
                currentUser = user
            case nil:
                syntaxIssues.append(.init(id: "list-without-section", severity: .error, message: "Kubeconfig contains a list item outside clusters, contexts, or users."))
            }
        }

        for lineSlice in raw.split(separator: "\n", omittingEmptySubsequences: false) {
            let line = Self.removingInlineCommentIfPresent(from: String(lineSlice))
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("#") else { continue }
            let indent = line.prefix { $0 == " " }.count

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
                if let value = scalarValue(trimmed, key: "current-context") {
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

            if let alias = Self.listAliasName(from: trimmed), indent <= 2 {
                appendAliasListItem(alias, section: section)
                nestedKey = nil
                continue
            }

            if trimmed.hasPrefix("- name:"), indent <= 2 {
                let name = scalarValue(trimmed, key: "- name") ?? ""
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

            if let aliases = Self.mergeAliasNames(from: trimmed) {
                for alias in aliases {
                    applyMappingAlias(alias, section: section, nestedKey: nestedKey)
                }
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

            if indent <= 2, let value = scalarValue(trimmed, key: "name") {
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
                if let value = scalarValue(trimmed, key: "server") {
                    currentCluster?.serverHost = Self.serverHost(from: value)
                } else if let value = scalarValue(trimmed, key: "name") {
                    currentCluster?.name = value
                }
            case (.contexts?, "context"):
                if let value = scalarValue(trimmed, key: "cluster") {
                    currentContextEntry?.clusterName = value
                } else if let value = scalarValue(trimmed, key: "user") {
                    currentContextEntry?.userName = value
                } else if let value = scalarValue(trimmed, key: "namespace") {
                    currentContextEntry?.namespace = value
                } else if let value = scalarValue(trimmed, key: "name") {
                    currentContextEntry?.name = value
                }
            case (.users?, "user"):
                if scalarValue(trimmed, key: "token") != nil {
                    currentUser?.hasToken = true
                } else if scalarValue(trimmed, key: "id-token") != nil {
                    currentUser?.hasToken = true
                } else if scalarValue(trimmed, key: "access-token") != nil {
                    currentUser?.hasToken = true
                } else if scalarValue(trimmed, key: "tokenFile") != nil {
                    currentUser?.hasTokenFile = true
                } else if scalarValue(trimmed, key: "token-file") != nil {
                    currentUser?.hasTokenFile = true
                } else if scalarValue(trimmed, key: "username") != nil {
                    currentUser?.hasBasicAuth = true
                } else if scalarValue(trimmed, key: "password") != nil {
                    currentUser?.hasBasicAuth = true
                } else if scalarValue(trimmed, key: "client-certificate-data") != nil {
                    currentUser?.hasClientCertificate = true
                } else if scalarValue(trimmed, key: "client-certificate") != nil {
                    currentUser?.hasClientCertificate = true
                } else if scalarValue(trimmed, key: "client-key-data") != nil {
                    currentUser?.hasClientCertificate = true
                } else if scalarValue(trimmed, key: "client-key") != nil {
                    currentUser?.hasClientCertificate = true
                } else if trimmed == "auth-provider:" {
                    nestedKey = "auth-provider"
                    authProviderConfigKey = nil
                } else if let value = scalarValue(trimmed, key: "name") {
                    currentUser?.name = value
                }
            case (.users?, "exec"):
                if let value = scalarValue(trimmed, key: "command") {
                    currentUser?.execCommand = value
                }
            case (.users?, "auth-provider"):
                if let value = scalarValue(trimmed, key: "name") {
                    currentUser?.authProviderName = value
                } else if trimmed == "config:" {
                    authProviderConfigKey = "config"
                } else if authProviderConfigKey == "config",
                          let value = anyScalarValue(trimmed) {
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

    private struct YAMLReferenceIndex {
        var scalarAnchors: [String: String] = [:]
        var mappingAnchors: [String: [String: String]] = [:]
        var issues: [KubeConfigImportIssue] = []
    }

    private static func referenceIndex(for raw: String) -> YAMLReferenceIndex {
        var index = YAMLReferenceIndex()
        let lines = raw.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)

        for (lineIndex, rawLine) in lines.enumerated() {
            let line = removingInlineCommentIfPresent(from: rawLine)
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("#") else { continue }

            if let scalar = scalarAnchor(in: trimmed) {
                index.scalarAnchors[scalar.name] = scalar.value
            }

            if let anchor = mappingAnchorName(in: trimmed) {
                let indent = line.prefix { $0 == " " }.count
                let mapping = collectMappingAnchor(lines: lines, startingAfter: lineIndex, baseIndent: indent)
                if !mapping.isEmpty {
                    index.mappingAnchors[anchor] = mapping
                }
            }
        }

        return index
    }

    private static func scalarAnchor(in trimmed: String) -> (name: String, value: String)? {
        guard let value = rawAnyScalarValue(trimmed)?.trimmingCharacters(in: .whitespacesAndNewlines),
              value.hasPrefix("&") else { return nil }
        let parts = value.dropFirst().split(maxSplits: 1, whereSeparator: \.isWhitespace)
        guard parts.count == 2 else { return nil }
        let name = String(parts[0])
        let scalar = String(parts[1]).trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
        guard !name.isEmpty, !scalar.isEmpty else { return nil }
        return (name, scalar)
    }

    private static func mappingAnchorName(in trimmed: String) -> String? {
        if isAnchorOnlyListItem(trimmed) {
            return String(trimmed.dropFirst(3)).trimmingCharacters(in: .whitespacesAndNewlines)
        }
        guard let value = rawAnyScalarValue(trimmed)?.trimmingCharacters(in: .whitespacesAndNewlines),
              value.hasPrefix("&") else { return nil }
        let parts = value.dropFirst().split(whereSeparator: \.isWhitespace)
        guard parts.count == 1 else { return nil }
        return String(parts[0])
    }

    private static func collectMappingAnchor(lines: [String], startingAfter lineIndex: Int, baseIndent: Int) -> [String: String] {
        var mapping: [String: String] = [:]
        var stack: [(indent: Int, key: String)] = []
        guard lineIndex + 1 < lines.count else { return mapping }

        for rawLine in lines[(lineIndex + 1)...] {
            let line = removingInlineCommentIfPresent(from: rawLine)
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("#") else { continue }

            let indent = line.prefix { $0 == " " }.count
            guard indent > baseIndent else { break }

            while let last = stack.last, last.indent >= indent {
                stack.removeLast()
            }

            guard let pair = keyValue(from: trimmed) else { continue }
            if pair.value.isEmpty || mappingAnchorName(in: trimmed) != nil {
                stack.append((indent: indent, key: pair.key))
                continue
            }

            let pathParts = stack.map(\.key) + [pair.key]
            let path = pathParts.joined(separator: ".")
            mapping[path] = pair.value
            if stack.isEmpty {
                mapping[pair.key] = pair.value
            }
        }

        return mapping
    }

    private static func keyValue(from trimmed: String) -> (key: String, value: String)? {
        guard let colon = trimmed.firstIndex(of: ":") else { return nil }
        var key = String(trimmed[..<colon]).trimmingCharacters(in: .whitespacesAndNewlines)
        if key.hasPrefix("-") {
            key = String(key.dropFirst()).trimmingCharacters(in: .whitespacesAndNewlines)
        }
        guard !key.isEmpty else { return nil }
        let value = String(trimmed[trimmed.index(after: colon)...]).trimmingCharacters(in: .whitespacesAndNewlines)
        return (key, value)
    }

    private static func mergeAliasNames(from trimmed: String) -> [String]? {
        guard let value = rawScalarValue(trimmed, key: "<<")?.trimmingCharacters(in: .whitespacesAndNewlines),
              value.hasPrefix("*") || (value.hasPrefix("[") && value.hasSuffix("]")) else { return nil }
        if value.hasPrefix("[") {
            let aliases = value
                .dropFirst()
                .dropLast()
                .split(separator: ",")
                .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                .filter { $0.hasPrefix("*") }
                .map { String($0.dropFirst()).trimmingCharacters(in: .whitespacesAndNewlines) }
                .filter { !$0.isEmpty }
            return aliases.isEmpty ? nil : aliases
        }
        return [String(value.dropFirst()).trimmingCharacters(in: .whitespacesAndNewlines)]
    }

    private static func listAliasName(from trimmed: String) -> String? {
        guard trimmed.hasPrefix("- *") else { return nil }
        let alias = String(trimmed.dropFirst(3)).trimmingCharacters(in: .whitespacesAndNewlines)
        return alias.isEmpty ? nil : alias
    }

    private static func rawScalarValue(_ line: String, key: String) -> String? {
        let prefix = "\(key):"
        guard line.hasPrefix(prefix) else { return nil }
        return String(line.dropFirst(prefix.count))
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func rawAnyScalarValue(_ line: String) -> String? {
        guard let colon = line.firstIndex(of: ":") else { return nil }
        let value = String(line[line.index(after: colon)...])
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return value.isEmpty ? nil : value
    }

    private static func scalarValue(_ line: String, key: String) -> String? {
        rawScalarValue(line, key: key)?
            .trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
    }

    private static func normalized(_ raw: String) -> String {
        var value = raw
        if value.hasPrefix("\u{FEFF}") {
            value.removeFirst()
        }
        if value.utf8.contains(13) {
            value = value.replacingOccurrences(of: "\r\n", with: "\n")
            value = value.replacingOccurrences(of: "\r", with: "\n")
        }
        return value
    }

    private static func mayContainYAMLReferences(_ raw: String) -> Bool {
        raw.utf8.contains { byte in
            byte == 38 || byte == 42 // `&` anchor or `*` alias.
        }
    }

    private static func removingInlineCommentIfPresent(from line: String) -> String {
        guard line.utf8.contains(35) else { return line } // `#`
        return removingInlineComment(from: line)
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
        for key in ["cluster", "context", "user"] {
            let prefix = "- \(key):"
            guard trimmed.hasPrefix(prefix) else { continue }
            let value = String(trimmed.dropFirst(prefix.count)).trimmingCharacters(in: .whitespacesAndNewlines)
            if value.isEmpty || value.hasPrefix("&") {
                return key
            }
        }
        return nil
    }

    private static func isAnchorOnlyListItem(_ trimmed: String) -> Bool {
        guard trimmed.hasPrefix("- &") else { return false }
        let rest = trimmed.dropFirst(2)
        return !rest.isEmpty && !rest.contains(" ") && !rest.contains(":")
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
