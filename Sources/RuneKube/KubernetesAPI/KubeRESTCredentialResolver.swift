import Foundation

/// Resolves REST credentials for the API server from merged kubeconfig (`kubectl config view`).
enum KubeRESTCredentialResolver {
    /// Returns `nil` if nothing usable is available (e.g. exec failure) or parse failure.
    static func resolve(
        view: KubeConfigViewJSON,
        contextName: String,
        kubeconfigDirectoryForRelativePaths: URL?,
        baseEnvironment: [String: String],
        runner: CommandRunning,
        execTimeout: TimeInterval
    ) async -> KubeRESTCredentials? {
        guard let ctx = view.contexts.first(where: { $0.name == contextName }) else { return nil }
        let clusterName = ctx.context.cluster
        let userName = ctx.context.user
        guard let clusterEntry = view.clusters.first(where: { $0.name == clusterName }) else { return nil }
        guard let userEntry = view.users.first(where: { $0.name == userName }) else { return nil }

        let cluster = clusterEntry.cluster
        let user = userEntry.user

        guard let serverURL = URL(string: cluster.server) else { return nil }

        let anchorDER = normalizedClusterCA(cluster: cluster, kubeconfigDirectoryForRelativePaths: kubeconfigDirectoryForRelativePaths)
        let skipTLS = cluster.insecureSkipTLSVerify == true

        let usedStaticToken = user.token.map { !$0.isEmpty } ?? false

        var bearerToken: String?
        var tokenExpiry: Date?
        var execPluginForRefresh: ExecPluginConfig?

        if usedStaticToken, let t = user.token {
            bearerToken = t
        } else if let exec = user.exec {
            do {
                let fetched = try await KubeExecCredentialRunner.fetchToken(
                    exec: exec,
                    baseEnvironment: baseEnvironment,
                    runner: runner,
                    timeout: execTimeout
                )
                bearerToken = fetched.token
                tokenExpiry = fetched.expiry
                execPluginForRefresh = exec
            } catch {
                return nil
            }
        }

        var clientCertPEM: Data?
        var clientKeyPEM: Data?
        if let c64 = user.clientCertificateData, let k64 = user.clientKeyData,
           let c = Data(base64Encoded: c64), let k = Data(base64Encoded: k64) {
            clientCertPEM = c
            clientKeyPEM = k
        } else if let cPath = user.clientCertificate, let kPath = user.clientKey,
                  !cPath.isEmpty, !kPath.isEmpty {
            clientCertPEM = try? Data(contentsOf: resolvedPath(cPath, base: kubeconfigDirectoryForRelativePaths))
            clientKeyPEM = try? Data(contentsOf: resolvedPath(kPath, base: kubeconfigDirectoryForRelativePaths))
        }

        let hasClientPair = clientCertPEM != nil && clientKeyPEM != nil
        let hasBearer = bearerToken.map { !$0.isEmpty } ?? false

        guard hasBearer || hasClientPair else { return nil }

        if hasClientPair, !hasBearer {
            execPluginForRefresh = nil
            tokenExpiry = nil
        }

        return KubeRESTCredentials(
            serverURL: serverURL,
            anchorCertificateDER: anchorDER,
            insecureSkipTLSVerify: skipTLS,
            bearerToken: bearerToken,
            tokenExpiry: tokenExpiry,
            clientCertificatePEM: clientCertPEM,
            clientPrivateKeyPEM: clientKeyPEM,
            execPluginForRefresh: execPluginForRefresh
        )
    }

    private static func resolvedPath(_ path: String, base: URL?) -> URL {
        if path.hasPrefix("/") {
            return URL(fileURLWithPath: path)
        }
        if let base {
            return URL(fileURLWithPath: path, relativeTo: base)
        }
        return URL(fileURLWithPath: path)
    }

    /// Returns DER suitable for `SecCertificateCreateWithData` / server-trust anchoring.
    private static func normalizedClusterCA(
        cluster: ClusterUserCluster,
        kubeconfigDirectoryForRelativePaths: URL?
    ) -> Data? {
        if let b64 = cluster.certificateAuthorityData, let raw = Data(base64Encoded: b64) {
            return derFromPossiblePEMAnchor(raw)
        }
        if let caPath = cluster.certificateAuthority, !caPath.isEmpty {
            let resolved = resolvedPath(caPath, base: kubeconfigDirectoryForRelativePaths)
            guard let fileData = try? Data(contentsOf: resolved) else { return nil }
            return derFromPossiblePEMAnchor(fileData)
        }
        return nil
    }

    private static func derFromPossiblePEMAnchor(_ data: Data) -> Data? {
        if data.first == 0x30 {
            return data
        }
        guard let pem = String(data: data, encoding: .utf8), pem.contains("BEGIN CERTIFICATE") else {
            return data
        }
        return KubePEM.derFromCertificatePEM(data) ?? data
    }
}

struct KubeRESTCredentials: Sendable {
    let serverURL: URL
    let anchorCertificateDER: Data?
    let insecureSkipTLSVerify: Bool
    let bearerToken: String?
    let tokenExpiry: Date?
    let clientCertificatePEM: Data?
    let clientPrivateKeyPEM: Data?
    /// When non-nil and bearer came from exec, token can be refreshed before expiry.
    let execPluginForRefresh: ExecPluginConfig?

    /// Cache entries should be dropped after this time so exec tokens are re-fetched.
    var cacheValidUntil: Date? {
        guard execPluginForRefresh != nil, let e = tokenExpiry else { return nil }
        return e.addingTimeInterval(-60)
    }
}
