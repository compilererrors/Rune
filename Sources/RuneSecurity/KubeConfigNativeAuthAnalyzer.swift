import Foundation
import RuneCore
import Yams

public struct KubeConfigNativeAuthAnalysisIssue: Codable, Equatable, Sendable {
    public let contextName: String
    public let message: String

    public init(contextName: String, message: String) {
        self.contextName = contextName
        self.message = message
    }
}

public struct KubeConfigNativeAuthAnalysis: Codable, Equatable, Sendable {
    public let currentContext: String?
    public let contexts: [KubernetesNativeAuthContextDescriptor]
    public let issues: [KubeConfigNativeAuthAnalysisIssue]

    public init(
        currentContext: String?,
        contexts: [KubernetesNativeAuthContextDescriptor],
        issues: [KubeConfigNativeAuthAnalysisIssue]
    ) {
        self.currentContext = currentContext
        self.contexts = contexts
        self.issues = issues
    }
}

public enum KubeConfigNativeAuthAnalysisError: Error, LocalizedError, Sendable, Equatable {
    case malformedKubeConfig

    public var errorDescription: String? {
        switch self {
        case .malformedKubeConfig:
            return "Kubeconfig could not be analyzed for native authentication because its YAML structure is invalid."
        }
    }
}

public struct KubeConfigNativeAuthAnalyzer: Sendable {
    public init() {}

    public func analyze(source: KubeConfigSource) throws -> KubeConfigNativeAuthAnalysis {
        try analyze(sources: [source])
    }

    /// Applies kubeconfig's first-definition-wins merge semantics before resolving
    /// context references. Each cluster/user entry retains its own source URL so
    /// relative CA and exec paths are resolved against the file that declared it.
    public func analyze(sources: [KubeConfigSource]) throws -> KubeConfigNativeAuthAnalysis {
        let documents = try sources.map { source in
            (
                document: try decode(String(contentsOf: source.url, encoding: .utf8)),
                sourceURL: Optional(source.url)
            )
        }
        return analyze(documents: documents)
    }

    public func analyze(raw: String, sourceURL: URL? = nil) throws -> KubeConfigNativeAuthAnalysis {
        analyze(documents: [(document: try decode(raw), sourceURL: sourceURL)])
    }

    private func decode(_ raw: String) throws -> Document {
        do {
            return try YAMLDecoder().decode(Document.self, from: raw)
        } catch {
            throw KubeConfigNativeAuthAnalysisError.malformedKubeConfig
        }
    }

    private func analyze(
        documents: [(document: Document, sourceURL: URL?)]
    ) -> KubeConfigNativeAuthAnalysis {
        var namedContexts: [(entry: Document.NamedContext, sourceURL: URL?)] = []
        var seenContextNames = Set<String>()
        var clusters: [String: (entry: Document.NamedCluster.ClusterEntry, sourceURL: URL?)] = [:]
        var users: [String: (entry: Document.NamedUser.UserEntry, sourceURL: URL?)] = [:]

        for item in documents {
            for context in item.document.contexts where seenContextNames.insert(context.name).inserted {
                namedContexts.append((context, item.sourceURL))
            }
            for cluster in item.document.clusters where clusters[cluster.name] == nil {
                clusters[cluster.name] = (cluster.cluster, item.sourceURL)
            }
            for user in item.document.users where users[user.name] == nil {
                users[user.name] = (user.user, item.sourceURL)
            }
        }
        var descriptors: [KubernetesNativeAuthContextDescriptor] = []
        var issues: [KubeConfigNativeAuthAnalysisIssue] = []

        for item in namedContexts {
            let namedContext = item.entry
            guard let clusterItem = clusters[namedContext.context.cluster] else {
                issues.append(KubeConfigNativeAuthAnalysisIssue(
                    contextName: namedContext.name,
                    message: "Context references a cluster entry that is missing."
                ))
                continue
            }

            let userItem = namedContext.context.user.flatMap { users[$0] }
            if namedContext.context.user != nil, userItem == nil {
                issues.append(KubeConfigNativeAuthAnalysisIssue(
                    contextName: namedContext.name,
                    message: "Context references a user entry that is missing."
                ))
            }

            let clusterEntry = clusterItem.entry
            let cluster = KubernetesNativeAuthClusterDescriptor(
                name: namedContext.context.cluster,
                server: clusterEntry.server,
                certificateAuthorityData: clusterEntry.certificateAuthorityData,
                certificateAuthorityPath: resolvedPath(
                    clusterEntry.certificateAuthority,
                    relativeTo: clusterItem.sourceURL
                ),
                insecureSkipTLSVerify: clusterEntry.insecureSkipTLSVerify ?? false,
                tlsServerName: clusterEntry.tlsServerName
            )
            let exec = userItem?.entry.exec.map {
                execDescriptor($0, sourceURL: userItem?.sourceURL)
            }
            let authProvider = userItem?.entry.authProvider.map(authProviderDescriptor)
            let provider: KubernetesNativeAuthProviderKind? = if exec != nil || authProvider != nil {
                KubernetesNativeAuthProviderClassifier.classify(
                    exec: exec,
                    authProvider: authProvider,
                    clusterServer: cluster.server
                )
            } else {
                nil
            }
            let bindingID = provider.map {
                KubernetesNativeAuthBindingFingerprint.make(
                    contextName: namedContext.name,
                    cluster: cluster,
                    userName: namedContext.context.user,
                    provider: $0,
                    exec: exec,
                    authProvider: authProvider
                )
            }

            descriptors.append(KubernetesNativeAuthContextDescriptor(
                contextName: namedContext.name,
                clusterName: namedContext.context.cluster,
                userName: namedContext.context.user,
                namespace: namedContext.context.namespace,
                cluster: cluster,
                exec: exec,
                authProvider: authProvider,
                provider: provider,
                bindingID: bindingID
            ))
        }

        return KubeConfigNativeAuthAnalysis(
            currentContext: documents.lazy.compactMap(\.document.currentContext).first,
            contexts: descriptors,
            issues: issues
        )
    }

    private func execDescriptor(_ entry: Document.NamedUser.UserEntry.Exec, sourceURL: URL?) -> KubernetesNativeAuthExecDescriptor {
        KubernetesNativeAuthExecDescriptor(
            apiVersion: entry.apiVersion,
            command: resolvedExecCommand(entry.command, relativeTo: sourceURL),
            arguments: entry.args,
            environment: entry.env.map {
                KubernetesNativeAuthEnvironmentEntry(
                    name: $0.name,
                    value: $0.value,
                    isSensitive: Self.isSensitiveKey($0.name)
                )
            },
            installHint: entry.installHint,
            provideClusterInfo: entry.provideClusterInfo ?? false,
            interactiveMode: entry.interactiveMode
        )
    }

    private func authProviderDescriptor(_ entry: Document.NamedUser.UserEntry.AuthProvider) -> KubernetesNativeAuthProviderDescriptor {
        let configuration = entry.config.mapValues(\.value)
        return KubernetesNativeAuthProviderDescriptor(
            name: entry.name,
            configuration: configuration,
            sensitiveConfigurationKeys: Set(configuration.keys.filter(Self.isSensitiveKey))
        )
    }

    private func resolvedExecCommand(_ command: String, relativeTo sourceURL: URL?) -> String {
        let expanded = NSString(string: command).expandingTildeInPath
        guard expanded.contains("/"), !NSString(string: expanded).isAbsolutePath,
              let sourceURL else {
            return expanded
        }
        return sourceURL.deletingLastPathComponent()
            .appendingPathComponent(expanded)
            .standardizedFileURL
            .path
    }

    private func resolvedPath(_ path: String?, relativeTo sourceURL: URL?) -> String? {
        guard let path, !path.isEmpty else { return path }
        let expanded = NSString(string: path).expandingTildeInPath
        guard !NSString(string: expanded).isAbsolutePath, let sourceURL else { return expanded }
        return sourceURL.deletingLastPathComponent()
            .appendingPathComponent(expanded)
            .standardizedFileURL
            .path
    }

    private static func isSensitiveKey(_ key: String) -> Bool {
        let normalized = key
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
            .replacingOccurrences(of: "_", with: "-")
        return ["access-token", "client-secret", "credential", "id-token", "password", "private-key", "refresh-token", "secret", "token"]
            .contains { normalized.contains($0) }
    }
}

private struct Document: Decodable {
    struct NamedContext: Decodable {
        struct ContextEntry: Decodable {
            let cluster: String
            let user: String?
            let namespace: String?
        }

        let name: String
        let context: ContextEntry
    }

    struct NamedCluster: Decodable {
        struct ClusterEntry: Decodable {
            let server: String
            let certificateAuthorityData: String?
            let certificateAuthority: String?
            let insecureSkipTLSVerify: Bool?
            let tlsServerName: String?

            enum CodingKeys: String, CodingKey {
                case server
                case certificateAuthorityData = "certificate-authority-data"
                case certificateAuthority = "certificate-authority"
                case insecureSkipTLSVerify = "insecure-skip-tls-verify"
                case tlsServerName = "tls-server-name"
            }
        }

        let name: String
        let cluster: ClusterEntry
    }

    struct NamedUser: Decodable {
        struct UserEntry: Decodable {
            struct Exec: Decodable {
                struct Environment: Decodable {
                    let name: String
                    let value: String
                }

                let apiVersion: String?
                let command: String
                let args: [String]
                let env: [Environment]
                let installHint: String?
                let provideClusterInfo: Bool?
                let interactiveMode: String?

                enum CodingKeys: String, CodingKey {
                    case apiVersion
                    case command
                    case args
                    case env
                    case installHint
                    case provideClusterInfo
                    case interactiveMode
                }

                init(from decoder: Decoder) throws {
                    let container = try decoder.container(keyedBy: CodingKeys.self)
                    apiVersion = try container.decodeIfPresent(String.self, forKey: .apiVersion)
                    command = try container.decode(String.self, forKey: .command)
                    args = try container.decodeIfPresent([String].self, forKey: .args) ?? []
                    env = try container.decodeIfPresent([Environment].self, forKey: .env) ?? []
                    installHint = try container.decodeIfPresent(String.self, forKey: .installHint)
                    provideClusterInfo = try container.decodeIfPresent(Bool.self, forKey: .provideClusterInfo)
                    interactiveMode = try container.decodeIfPresent(String.self, forKey: .interactiveMode)
                }
            }

            struct AuthProvider: Decodable {
                let name: String
                let config: [String: FlexibleString]

                enum CodingKeys: String, CodingKey {
                    case name
                    case config
                }

                init(from decoder: Decoder) throws {
                    let container = try decoder.container(keyedBy: CodingKeys.self)
                    name = try container.decode(String.self, forKey: .name)
                    config = try container.decodeIfPresent([String: FlexibleString].self, forKey: .config) ?? [:]
                }
            }

            let exec: Exec?
            let authProvider: AuthProvider?

            enum CodingKeys: String, CodingKey {
                case exec
                case authProvider = "auth-provider"
            }
        }

        let name: String
        let user: UserEntry
    }

    let currentContext: String?
    let contexts: [NamedContext]
    let clusters: [NamedCluster]
    let users: [NamedUser]

    enum CodingKeys: String, CodingKey {
        case currentContext = "current-context"
        case contexts
        case clusters
        case users
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        currentContext = try container.decodeIfPresent(String.self, forKey: .currentContext)
        contexts = try container.decodeIfPresent([NamedContext].self, forKey: .contexts) ?? []
        clusters = try container.decodeIfPresent([NamedCluster].self, forKey: .clusters) ?? []
        users = try container.decodeIfPresent([NamedUser].self, forKey: .users) ?? []
    }
}

private struct FlexibleString: Decodable {
    let value: String

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let value = try? container.decode(String.self) {
            self.value = value
        } else if let value = try? container.decode(Bool.self) {
            self.value = value ? "true" : "false"
        } else if let value = try? container.decode(Int.self) {
            self.value = String(value)
        } else if let value = try? container.decode(Double.self) {
            self.value = String(value)
        } else if container.decodeNil() {
            self.value = ""
        } else {
            throw DecodingError.typeMismatch(
                String.self,
                DecodingError.Context(codingPath: decoder.codingPath, debugDescription: "Expected a scalar kubeconfig value")
            )
        }
    }
}

private extension Dictionary {
    init<S: Sequence>(
        uniqueKeysWithValues values: S,
        keepingFirstValueOnDuplicate: Bool
    ) where S.Element == (Key, Value) {
        self.init()
        for (key, value) in values where self[key] == nil || !keepingFirstValueOnDuplicate {
            self[key] = value
        }
    }
}
