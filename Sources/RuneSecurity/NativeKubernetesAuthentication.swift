import CryptoKit
import Foundation

public enum KubernetesNativeAuthProviderKind: String, Codable, CaseIterable, Sendable {
    case awsEKS = "aws-eks"
    case azureKubelogin = "azure-kubelogin"
    case googleGKE = "google-gke"
    case oidc

    public var displayName: String {
        switch self {
        case .awsEKS: return "Amazon EKS"
        case .azureKubelogin: return "Microsoft AKS"
        case .googleGKE: return "Google GKE"
        case .oidc: return "OIDC"
        }
    }
}

public struct KubernetesNativeAuthEnvironmentEntry: Codable, Equatable, Sendable {
    public let name: String
    public let value: String
    public let isSensitive: Bool

    public init(name: String, value: String, isSensitive: Bool = false) {
        self.name = name
        self.value = value
        self.isSensitive = isSensitive
    }

    public var displayValue: String {
        isSensitive ? "<redacted>" : value
    }
}

public struct KubernetesNativeAuthExecDescriptor: Codable, Equatable, Sendable {
    public let apiVersion: String?
    public let command: String
    public let arguments: [String]
    public let environment: [KubernetesNativeAuthEnvironmentEntry]
    public let installHint: String?
    public let provideClusterInfo: Bool
    public let interactiveMode: String?

    public init(
        apiVersion: String? = nil,
        command: String,
        arguments: [String] = [],
        environment: [KubernetesNativeAuthEnvironmentEntry] = [],
        installHint: String? = nil,
        provideClusterInfo: Bool = false,
        interactiveMode: String? = nil
    ) {
        self.apiVersion = apiVersion
        self.command = command
        self.arguments = arguments
        self.environment = environment
        self.installHint = installHint
        self.provideClusterInfo = provideClusterInfo
        self.interactiveMode = interactiveMode
    }

    public var executableName: String {
        let trimmed = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.contains("/") else { return trimmed }
        return URL(fileURLWithPath: trimmed).lastPathComponent
    }

    public func optionValue(for names: String...) -> String? {
        KubernetesNativeAuthArgumentSet(arguments: arguments).value(for: names)
    }

    public func optionValues(for names: String...) -> [String] {
        KubernetesNativeAuthArgumentSet(arguments: arguments).values(for: names)
    }
}

public struct KubernetesNativeAuthProviderDescriptor: Codable, Equatable, Sendable {
    public let name: String
    public let configuration: [String: String]
    public let sensitiveConfigurationKeys: Set<String>

    public init(
        name: String,
        configuration: [String: String] = [:],
        sensitiveConfigurationKeys: Set<String> = []
    ) {
        self.name = name
        self.configuration = configuration
        self.sensitiveConfigurationKeys = Set(sensitiveConfigurationKeys.map(Self.normalizedKey))
    }

    public func displayValue(for key: String) -> String? {
        guard let value = configuration[key] else { return nil }
        return sensitiveConfigurationKeys.contains(Self.normalizedKey(key)) ? "<redacted>" : value
    }

    private static func normalizedKey(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }
}

public struct KubernetesNativeAuthClusterDescriptor: Codable, Equatable, Sendable {
    public let name: String
    public let server: String
    public let certificateAuthorityData: String?
    public let certificateAuthorityPath: String?
    public let insecureSkipTLSVerify: Bool
    public let tlsServerName: String?

    public init(
        name: String,
        server: String,
        certificateAuthorityData: String? = nil,
        certificateAuthorityPath: String? = nil,
        insecureSkipTLSVerify: Bool = false,
        tlsServerName: String? = nil
    ) {
        self.name = name
        self.server = server
        self.certificateAuthorityData = certificateAuthorityData
        self.certificateAuthorityPath = certificateAuthorityPath
        self.insecureSkipTLSVerify = insecureSkipTLSVerify
        self.tlsServerName = tlsServerName
    }
}

public struct KubernetesNativeAuthContextDescriptor: Codable, Equatable, Sendable {
    public let contextName: String
    public let clusterName: String
    public let userName: String?
    public let namespace: String?
    public let cluster: KubernetesNativeAuthClusterDescriptor
    public let exec: KubernetesNativeAuthExecDescriptor?
    public let authProvider: KubernetesNativeAuthProviderDescriptor?
    public let provider: KubernetesNativeAuthProviderKind?
    public let bindingID: String?

    public init(
        contextName: String,
        clusterName: String,
        userName: String?,
        namespace: String?,
        cluster: KubernetesNativeAuthClusterDescriptor,
        exec: KubernetesNativeAuthExecDescriptor?,
        authProvider: KubernetesNativeAuthProviderDescriptor?,
        provider: KubernetesNativeAuthProviderKind?,
        bindingID: String?
    ) {
        self.contextName = contextName
        self.clusterName = clusterName
        self.userName = userName
        self.namespace = namespace
        self.cluster = cluster
        self.exec = exec
        self.authProvider = authProvider
        self.provider = provider
        self.bindingID = bindingID
    }

    public var credentialRequest: KubernetesNativeCredentialRequest? {
        guard let provider, let bindingID else { return nil }
        return KubernetesNativeCredentialRequest(
            bindingID: bindingID,
            provider: provider,
            contextName: contextName,
            clusterName: clusterName,
            userName: userName,
            server: cluster.server,
            exec: exec,
            authProvider: authProvider
        )
    }
}

public struct KubernetesNativeCredentialRequest: Codable, Equatable, Sendable {
    public let bindingID: String
    public let provider: KubernetesNativeAuthProviderKind
    public let contextName: String
    public let clusterName: String
    public let userName: String?
    public let server: String
    public let exec: KubernetesNativeAuthExecDescriptor?
    public let authProvider: KubernetesNativeAuthProviderDescriptor?

    public init(
        bindingID: String,
        provider: KubernetesNativeAuthProviderKind,
        contextName: String,
        clusterName: String,
        userName: String?,
        server: String,
        exec: KubernetesNativeAuthExecDescriptor?,
        authProvider: KubernetesNativeAuthProviderDescriptor?
    ) {
        self.bindingID = bindingID
        self.provider = provider
        self.contextName = contextName
        self.clusterName = clusterName
        self.userName = userName
        self.server = server
        self.exec = exec
        self.authProvider = authProvider
    }
}

public struct KubernetesNativeCredential: Equatable, Sendable {
    public let bearerToken: String
    public let expiresAt: Date?
    public let revision: UUID

    public init(
        bearerToken: String,
        expiresAt: Date?,
        revision: UUID = UUID()
    ) {
        self.bearerToken = bearerToken
        self.expiresAt = expiresAt
        self.revision = revision
    }

    public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.bearerToken == rhs.bearerToken
            && lhs.expiresAt == rhs.expiresAt
    }
}

public protocol KubernetesNativeCredentialProviding: Sendable {
    /// Returns nil when no native profile is bound to this request. Direct builds may then use ExecConfig.
    func credential(for request: KubernetesNativeCredentialRequest) async throws -> KubernetesNativeCredential?

    /// Invalidates in-memory access credentials after authentication rejection. Persistent refresh material remains bound.
    func invalidateCredential(for bindingID: String) async

    /// Invalidates only when the rejected request used the provider's current credential revision.
    func invalidateCredential(for bindingID: String, matchingRevision revision: UUID) async
}

public extension KubernetesNativeCredentialProviding {
    func invalidateCredential(for _: String) async {}

    func invalidateCredential(for bindingID: String, matchingRevision _: UUID) async {
        await invalidateCredential(for: bindingID)
    }
}

public enum KubernetesNativeAuthProviderClassifier {
    public static func classify(
        exec: KubernetesNativeAuthExecDescriptor?,
        authProvider: KubernetesNativeAuthProviderDescriptor?,
        clusterServer _: String
    ) -> KubernetesNativeAuthProviderKind? {
        if let exec {
            let executable = exec.executableName.lowercased()
            let arguments = KubernetesNativeAuthArgumentSet(arguments: exec.arguments)

            if (executable == "aws" || executable == "aws.exe"),
               (try? AWSEKSExecDescriptor.parseIfSupported(
                   command: exec.command,
                   arguments: exec.arguments,
                   environment: exec.environment.reduce(into: [:]) { $0[$1.name] = $1.value }
               )) != nil {
                return .awsEKS
            }
            if executable == "gke-gcloud-auth-plugin" {
                return .googleGKE
            }
            if executable == "gcloud",
               arguments.positionals.contains(where: { $0.caseInsensitiveCompare("config-helper") == .orderedSame }) {
                return .googleGKE
            }
            if (executable == "kubelogin" || executable == "kubelogin.exe"),
               (try? AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
                   command: exec.command,
                   arguments: exec.arguments
               )) != nil {
                return .azureKubelogin
            }
        }

        if authProvider?.name.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == "oidc" {
            return .oidc
        }

        return nil
    }
}

public enum KubernetesNativeAuthBindingFingerprint {
    public static func make(
        contextName: String,
        cluster: KubernetesNativeAuthClusterDescriptor,
        userName: String?,
        provider: KubernetesNativeAuthProviderKind,
        exec: KubernetesNativeAuthExecDescriptor?,
        authProvider: KubernetesNativeAuthProviderDescriptor?
    ) -> String {
        var fields: [String] = [
            "schema=1",
            "provider=\(provider.rawValue)",
            "context=\(normalized(contextName))",
            "cluster=\(normalized(cluster.name))",
            "server=\(normalizedURL(cluster.server))",
            "user=\(normalized(userName ?? ""))",
            "tls-server-name=\(normalized(cluster.tlsServerName ?? ""))",
            "insecure=\(cluster.insecureSkipTLSVerify ? "true" : "false")"
        ]

        if let certificateAuthorityData = cluster.certificateAuthorityData {
            fields.append("ca-data-sha256=\(sha256Hex(certificateAuthorityData))")
        }

        if let exec {
            fields.append("exec=\(exec.executableName.lowercased())")
            let arguments = KubernetesNativeAuthArgumentSet(arguments: exec.arguments)
            for (key, values) in safeExecOptions(arguments, provider: provider).sorted(by: { $0.key < $1.key }) {
                for value in values.sorted() {
                    fields.append("exec-option:\(key)=\(normalized(value))")
                }
            }
            for entry in safeExecEnvironment(exec.environment, provider: provider) {
                fields.append("exec-env:\(entry.name.lowercased())=\(normalized(entry.value))")
            }
        }

        if let authProvider {
            fields.append("auth-provider=\(authProvider.name.lowercased())")
            for key in safeAuthProviderKeys {
                guard let value = authProvider.configuration.first(where: {
                    normalizedOptionName($0.key) == key
                })?.value else { continue }
                fields.append("auth-provider-option:\(key)=\(normalized(value))")
            }
        }

        return "native-k8s-v1:" + sha256Hex(fields.joined(separator: "\u{1f}"))
    }

    private static let safeAuthProviderKeys: Set<String> = [
        "client-id", "extra-scopes", "idp-issuer-url", "issuer-url", "tenant-id"
    ]

    private static func safeExecOptions(
        _ arguments: KubernetesNativeAuthArgumentSet,
        provider: KubernetesNativeAuthProviderKind
    ) -> [String: [String]] {
        let allowed: Set<String>
        switch provider {
        case .awsEKS:
            allowed = ["--cluster-name", "--region", "--role-arn", "--profile", "-i"]
        case .azureKubelogin:
            allowed = ["--authority-host", "--client-id", "--environment", "--login", "--server-id", "--tenant-id"]
        case .googleGKE:
            allowed = ["--project", "--use_application_default_credentials"]
        case .oidc:
            allowed = ["--client-id", "--extra-scope", "--oidc-client-id", "--oidc-extra-scope", "--oidc-issuer-url", "--issuer-url"]
        }
        return arguments.options.filter { allowed.contains($0.key) }
    }

    private static func safeExecEnvironment(
        _ environment: [KubernetesNativeAuthEnvironmentEntry],
        provider: KubernetesNativeAuthProviderKind
    ) -> [KubernetesNativeAuthEnvironmentEntry] {
        let allowedNames: Set<String>
        switch provider {
        case .awsEKS:
            allowedNames = ["aws_region", "aws_default_region", "aws_profile"]
        case .azureKubelogin, .googleGKE, .oidc:
            allowedNames = []
        }
        return environment
            .filter { !$0.isSensitive && allowedNames.contains($0.name.lowercased()) }
            .sorted {
                if $0.name.caseInsensitiveCompare($1.name) == .orderedSame { return $0.value < $1.value }
                return $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
            }
    }

    private static func normalized(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func normalizedURL(_ value: String) -> String {
        guard var components = URLComponents(string: normalized(value)) else { return normalized(value) }
        components.scheme = components.scheme?.lowercased()
        components.host = components.host?.lowercased()
        if (components.scheme == "https" && components.port == 443)
            || (components.scheme == "http" && components.port == 80) {
            components.port = nil
        }
        return components.string ?? normalized(value)
    }

    private static func normalizedOptionName(_ value: String) -> String {
        var output = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        while output.hasPrefix("-") { output.removeFirst() }
        return output
    }

    private static func sha256Hex(_ value: String) -> String {
        SHA256.hash(data: Data(value.utf8)).map { String(format: "%02x", $0) }.joined()
    }
}

public struct KubernetesNativeAuthProfile: Codable, Equatable, Identifiable, Sendable {
    public let id: UUID
    public let bindingID: String
    public let provider: KubernetesNativeAuthProviderKind
    public let displayName: String
    public let configuration: [String: String]
    public let createdAt: Date
    public let updatedAt: Date

    public init(
        id: UUID = UUID(),
        bindingID: String,
        provider: KubernetesNativeAuthProviderKind,
        displayName: String,
        configuration: [String: String] = [:],
        createdAt: Date = Date(),
        updatedAt: Date = Date()
    ) {
        self.id = id
        self.bindingID = bindingID
        self.provider = provider
        self.displayName = displayName.trimmingCharacters(in: .whitespacesAndNewlines)
        self.configuration = configuration
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }
}

public struct KubernetesNativeAuthStoredProfile: Equatable, Sendable {
    public let profile: KubernetesNativeAuthProfile
    public let secret: Data?

    public init(profile: KubernetesNativeAuthProfile, secret: Data?) {
        self.profile = profile
        self.secret = secret
    }
}

public protocol KubernetesNativeAuthProfileStoring: Sendable {
    func profiles() async throws -> [KubernetesNativeAuthProfile]
    func storedProfile(for bindingID: String) async throws -> KubernetesNativeAuthStoredProfile?
    func save(profile: KubernetesNativeAuthProfile, secret: Data?) async throws
    func removeProfile(for bindingID: String) async throws
}

public enum KubernetesNativeAuthProfileStoreError: Error, LocalizedError, Sendable, Equatable {
    case corruptedIndex

    public var errorDescription: String? {
        switch self {
        case .corruptedIndex:
            return "Native Kubernetes authentication profiles could not be decoded from Keychain."
        }
    }
}

public actor KeychainKubernetesNativeAuthProfileStore: KubernetesNativeAuthProfileStoring {
    private struct Index: Codable {
        var profiles: [KubernetesNativeAuthProfile]
    }

    private let secretStore: any SecretStore
    private let keyPrefix: String

    public init(
        secretStore: any SecretStore = KeychainStore(),
        keyPrefix: String = "rune.kubernetes-native-auth.v1"
    ) {
        self.secretStore = secretStore
        self.keyPrefix = keyPrefix
    }

    public func profiles() throws -> [KubernetesNativeAuthProfile] {
        try loadIndex().profiles.sorted {
            if $0.displayName == $1.displayName { return $0.bindingID < $1.bindingID }
            return $0.displayName.localizedCaseInsensitiveCompare($1.displayName) == .orderedAscending
        }
    }

    public func storedProfile(for bindingID: String) throws -> KubernetesNativeAuthStoredProfile? {
        let normalizedBindingID = bindingID.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let profile = try loadIndex().profiles.first(where: { $0.bindingID == normalizedBindingID }) else {
            return nil
        }
        return KubernetesNativeAuthStoredProfile(
            profile: profile,
            secret: try secretStore.get(for: secretKey(profile.id))
        )
    }

    public func save(profile: KubernetesNativeAuthProfile, secret: Data?) throws {
        var index = try loadIndex()
        let replacedProfiles = index.profiles.filter {
            $0.bindingID == profile.bindingID && $0.id != profile.id
        }
        index.profiles.removeAll {
            $0.id == profile.id || $0.bindingID == profile.bindingID
        }
        index.profiles.append(profile)

        if let secret {
            try secretStore.set(secret, for: secretKey(profile.id))
        } else {
            try secretStore.delete(for: secretKey(profile.id))
        }
        try saveIndex(index)

        for replaced in replacedProfiles {
            try secretStore.delete(for: secretKey(replaced.id))
        }
    }

    public func removeProfile(for bindingID: String) throws {
        var index = try loadIndex()
        let removed = index.profiles.filter { $0.bindingID == bindingID }
        guard !removed.isEmpty else { return }
        index.profiles.removeAll { $0.bindingID == bindingID }
        try saveIndex(index)
        for profile in removed {
            try secretStore.delete(for: secretKey(profile.id))
        }
    }

    private var indexKey: String { "\(keyPrefix).profiles" }

    private func secretKey(_ profileID: UUID) -> String {
        "\(keyPrefix).secret.\(profileID.uuidString.lowercased())"
    }

    private func loadIndex() throws -> Index {
        guard let data = try secretStore.get(for: indexKey) else {
            return Index(profiles: [])
        }
        do {
            return try JSONDecoder().decode(Index.self, from: data)
        } catch {
            throw KubernetesNativeAuthProfileStoreError.corruptedIndex
        }
    }

    private func saveIndex(_ index: Index) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        try secretStore.set(try encoder.encode(index), for: indexKey)
    }
}

private struct KubernetesNativeAuthArgumentSet {
    let options: [String: [String]]
    let positionals: [String]

    init(arguments: [String]) {
        var options: [String: [String]] = [:]
        var positionals: [String] = []
        var index = 0
        while index < arguments.count {
            let argument = arguments[index]
            guard argument.hasPrefix("-") else {
                positionals.append(argument)
                index += 1
                continue
            }

            if let separator = argument.firstIndex(of: "=") {
                let name = Self.normalizedName(String(argument[..<separator]))
                let value = String(argument[argument.index(after: separator)...])
                options[name, default: []].append(value)
                index += 1
                continue
            }

            let name = Self.normalizedName(argument)
            if index + 1 < arguments.count, !arguments[index + 1].hasPrefix("-") {
                options[name, default: []].append(arguments[index + 1])
                index += 2
            } else {
                options[name, default: []].append("true")
                index += 1
            }
        }
        self.options = options
        self.positionals = positionals
    }

    func value(for names: [String]) -> String? {
        values(for: names).first
    }

    func values(for names: [String]) -> [String] {
        for name in names {
            if let values = options[Self.normalizedName(name)] {
                return values
            }
        }
        return []
    }

    private static func normalizedName(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }
}
