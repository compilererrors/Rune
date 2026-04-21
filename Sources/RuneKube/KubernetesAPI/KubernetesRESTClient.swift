import Foundation
import RuneCore
import Security

final class KubernetesRESTClient: @unchecked Sendable {
    private let runner: CommandRunning
    private let kubectlPath: String
    private let configCache = KubernetesRESTConfigCache()
    private let normalizedConfigTimeout: TimeInterval = 2
    private let normalizedConfigFailureCooldown: TimeInterval = 300

    init(runner: CommandRunning, kubectlPath: String) {
        self.runner = runner
        self.kubectlPath = kubectlPath
    }

    func listContexts(environment: [String: String]) async throws -> [KubeContext] {
        let config = try await normalizedConfig(environment: environment)
        return config.contexts
            .map { KubeContext(name: $0.name) }
            .sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }

    func contextNamespace(environment: [String: String], contextName: String) async throws -> String? {
        let resolved = try await resolvedContext(environment: environment, contextName: contextName)
        let trimmed = resolved.namespace?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return trimmed.isEmpty ? nil : trimmed
    }

    func listNamespaces(environment: [String: String], contextName: String, timeout: TimeInterval) async throws -> [String] {
        let raw = try await collection(
            environment: environment,
            contextName: contextName,
            resource: "namespaces",
            namespace: nil,
            timeout: timeout
        )
        let decoded = try JSONDecoder().decode(NamespaceList.self, from: Data(raw.utf8))
        return decoded.items
            .map(\.metadata.name)
            .sorted()
    }

    func collection(
        environment: [String: String],
        contextName: String,
        resource: String,
        namespace: String?,
        timeout: TimeInterval,
        options: KubernetesListOptions = KubernetesListOptions()
    ) async throws -> String {
        if let namespace {
            guard let request = KubernetesRESTPath.namespacedCollectionRequest(
                namespace: namespace,
                resource: resource,
                options: options
            ) else {
                throw RuneError.invalidInput(message: "REST path saknas för resource \(resource)")
            }
            return try await rawRequest(
                environment: environment,
                contextName: contextName,
                method: "GET",
                apiPath: request.apiPath,
                headers: ["Accept": "application/json"],
                body: nil,
                timeout: timeout
            ).body
        }

        guard let path = KubernetesRESTPath.collectionPath(resource: resource, namespace: nil) else {
            throw RuneError.invalidInput(message: "REST path saknas för resource \(resource)")
        }
        return try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "GET",
            apiPath: options.appendingPercentEncoded(to: path),
            headers: ["Accept": "application/json"],
            body: nil,
            timeout: timeout
        ).body
    }

    func resourceJSON(
        environment: [String: String],
        contextName: String,
        kind: KubeResourceKind,
        namespace: String,
        name: String,
        subresource: String? = nil,
        timeout: TimeInterval
    ) async throws -> String {
        let resource = KubernetesRESTPath.resourceName(for: kind)
        let path = try resourcePath(kind: kind, namespace: namespace, resource: resource, name: name, subresource: subresource)
        return try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "GET",
            apiPath: path,
            headers: ["Accept": "application/json"],
            body: nil,
            timeout: timeout
        ).body
    }

    func resourceYAML(
        environment: [String: String],
        contextName: String,
        kind: KubeResourceKind,
        namespace: String,
        name: String,
        timeout: TimeInterval
    ) async throws -> String {
        let resource = KubernetesRESTPath.resourceName(for: kind)
        let path = try resourcePath(kind: kind, namespace: namespace, resource: resource, name: name, subresource: nil)
        let response = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "GET",
            apiPath: path,
            headers: ["Accept": "application/yaml, application/json"],
            body: nil,
            timeout: timeout
        )

        if response.contentType.localizedCaseInsensitiveContains("yaml") {
            return response.body
        }

        let json = try JSONSerialization.jsonObject(with: Data(response.body.utf8))
        let data = try JSONSerialization.data(withJSONObject: json, options: [.prettyPrinted, .sortedKeys])
        return String(decoding: data, as: UTF8.self)
    }

    func podLogs(
        environment: [String: String],
        contextName: String,
        namespace: String,
        podName: String,
        filter: LogTimeFilter,
        previous: Bool,
        timeout: TimeInterval
    ) async throws -> String {
        var items: [URLQueryItem] = []
        switch filter {
        case .all:
            items.append(URLQueryItem(name: "tailLines", value: "200"))
        case let .tailLines(lines):
            items.append(URLQueryItem(name: "tailLines", value: String(max(1, lines))))
        case .lastMinutes, .lastHours, .lastDays:
            if let since = filter.kubectlSinceArgument {
                items.append(URLQueryItem(name: "sinceSeconds", value: String(max(1, parseDurationSeconds(from: since)))))
            }
            items.append(URLQueryItem(name: "tailLines", value: "5000"))
        case let .since(date):
            items.append(URLQueryItem(name: "sinceTime", value: ISO8601DateFormatter().string(from: date)))
            items.append(URLQueryItem(name: "tailLines", value: "5000"))
        }
        if previous {
            items.append(URLQueryItem(name: "previous", value: "true"))
        }

        var components = URLComponents()
        components.path = try resourcePath(
            kind: .pod,
            namespace: namespace,
            resource: "pods",
            name: podName,
            subresource: "log"
        )
        components.queryItems = items.isEmpty ? nil : items
        let apiPath = components.percentEncodedPath + (components.percentEncodedQuery.map { "?\($0)" } ?? "")

        return try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "GET",
            apiPath: apiPath,
            headers: ["Accept": "text/plain"],
            body: nil,
            timeout: timeout
        ).body
    }

    func serviceSelector(
        environment: [String: String],
        contextName: String,
        namespace: String,
        serviceName: String,
        timeout: TimeInterval
    ) async throws -> String {
        try await resourceJSON(
            environment: environment,
            contextName: contextName,
            kind: .service,
            namespace: namespace,
            name: serviceName,
            timeout: timeout
        )
    }

    func deploymentSelector(
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        timeout: TimeInterval
    ) async throws -> String {
        try await resourceJSON(
            environment: environment,
            contextName: contextName,
            kind: .deployment,
            namespace: namespace,
            name: deploymentName,
            timeout: timeout
        )
    }

    func podsBySelector(
        environment: [String: String],
        contextName: String,
        namespace: String,
        selector: String,
        timeout: TimeInterval
    ) async throws -> String {
        try await collection(
            environment: environment,
            contextName: contextName,
            resource: "pods",
            namespace: namespace,
            timeout: timeout,
            options: KubernetesListOptions(labelSelector: selector)
        )
    }

    func rawGET(
        environment: [String: String],
        contextName: String,
        apiPath: String,
        timeout: TimeInterval
    ) async throws -> String {
        try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "GET",
            apiPath: apiPath,
            headers: ["Accept": "application/json"],
            body: nil,
            timeout: timeout
        ).body
    }

    func deleteResource(
        environment: [String: String],
        contextName: String,
        namespace: String,
        kind: KubeResourceKind,
        name: String,
        timeout: TimeInterval
    ) async throws {
        let resource = KubernetesRESTPath.resourceName(for: kind)
        let path = try resourcePath(kind: kind, namespace: namespace, resource: resource, name: name, subresource: nil)
        _ = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "DELETE",
            apiPath: path,
            headers: ["Accept": "application/json"],
            body: nil,
            timeout: timeout
        )
    }

    func scaleDeployment(
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        replicas: Int,
        timeout: TimeInterval
    ) async throws {
        let path = try resourcePath(
            kind: .deployment,
            namespace: namespace,
            resource: "deployments",
            name: deploymentName,
            subresource: "scale"
        )
        let body = """
        {"spec":{"replicas":\(replicas)}}
        """
        _ = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "PATCH",
            apiPath: path,
            headers: [
                "Accept": "application/json",
                "Content-Type": "application/merge-patch+json"
            ],
            body: body,
            timeout: timeout
        )
    }

    func restartDeploymentRollout(
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        timeout: TimeInterval
    ) async throws {
        let path = try resourcePath(
            kind: .deployment,
            namespace: namespace,
            resource: "deployments",
            name: deploymentName,
            subresource: nil
        )
        let restartedAt = ISO8601DateFormatter().string(from: Date())
        let body = """
        {"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"\(restartedAt)"}}}}}
        """
        _ = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "PATCH",
            apiPath: path,
            headers: [
                "Accept": "application/json",
                "Content-Type": "application/strategic-merge-patch+json"
            ],
            body: body,
            timeout: timeout
        )
    }

    func patchCronJobSuspend(
        environment: [String: String],
        contextName: String,
        namespace: String,
        name: String,
        suspend: Bool,
        timeout: TimeInterval
    ) async throws {
        let path = try resourcePath(
            kind: .cronJob,
            namespace: namespace,
            resource: "cronjobs",
            name: name,
            subresource: nil
        )
        let body = """
        {"spec":{"suspend":\(suspend ? "true" : "false")}}
        """
        _ = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "PATCH",
            apiPath: path,
            headers: [
                "Accept": "application/json",
                "Content-Type": "application/merge-patch+json"
            ],
            body: body,
            timeout: timeout
        )
    }

    private func resourcePath(
        kind: KubeResourceKind,
        namespace: String,
        resource: String,
        name: String,
        subresource: String?
    ) throws -> String {
        let effectiveNamespace = kind.isNamespaced ? namespace : nil
        guard let path = KubernetesRESTPath.resourcePath(
            namespace: effectiveNamespace,
            resource: resource,
            name: name,
            subresource: subresource
        ) else {
            throw RuneError.invalidInput(message: "REST path saknas för \(kind.rawValue)")
        }
        return path
    }

    private func rawRequest(
        environment: [String: String],
        contextName: String,
        method: String,
        apiPath: String,
        headers: [String: String],
        body: String?,
        timeout: TimeInterval
    ) async throws -> RESTResponse {
        let resolved = try await resolvedContext(environment: environment, contextName: contextName)
        let session = makeSession(for: resolved)
        defer { session.invalidateAndCancel() }

        guard let url = URL(string: apiPath, relativeTo: resolved.serverURL)?.absoluteURL else {
            throw RuneError.invalidInput(message: "Ogiltig Kubernetes API-path: \(apiPath)")
        }

        var request = URLRequest(url: url)
        request.httpMethod = method
        request.timeoutInterval = timeout
        for (key, value) in headers {
            request.setValue(value, forHTTPHeaderField: key)
        }

        switch resolved.authentication {
        case let .bearer(token):
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        case let .basic(username, password):
            let encoded = Data("\(username):\(password)".utf8).base64EncodedString()
            request.setValue("Basic \(encoded)", forHTTPHeaderField: "Authorization")
        case .none:
            break
        }

        if let body {
            request.httpBody = Data(body.utf8)
        }

        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw RuneError.commandFailed(command: "kubernetes REST \(method) \(apiPath)", message: "Missing HTTP response")
        }

        let responseBody = String(data: data, encoding: .utf8) ?? ""
        guard (200..<300).contains(http.statusCode) else {
            throw RuneError.commandFailed(
                command: "kubernetes REST \(method) \(apiPath)",
                message: responseBody.isEmpty ? "HTTP \(http.statusCode)" : "HTTP \(http.statusCode): \(responseBody)"
            )
        }

        return RESTResponse(
            body: responseBody,
            contentType: http.value(forHTTPHeaderField: "Content-Type") ?? ""
        )
    }

    private func makeSession(for resolved: ResolvedRESTContext) -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.waitsForConnectivity = false
        let delegate = RESTURLSessionDelegate(
            insecureSkipTLSVerify: resolved.insecureSkipTLSVerify,
            certificateAuthorityData: resolved.certificateAuthorityData,
            tlsServerName: resolved.tlsServerName
        )
        return URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
    }

    private func normalizedConfig(environment: [String: String]) async throws -> NormalizedKubeConfig {
        let cacheKey = environment["KUBECONFIG"] ?? ""
        if let cached = await configCache.config(for: cacheKey) {
            return cached
        }
        if let blockedUntil = await configCache.blockedUntil(for: cacheKey), blockedUntil > Date() {
            throw RuneError.commandFailed(
                command: "kubectl config view",
                message: "REST transport temporarily disabled after recent kubeconfig normalization failure"
            )
        }

        let result: CommandResult
        do {
            result = try await runner.run(
                executable: kubectlPath,
                arguments: ["kubectl", "config", "view", "--raw", "--flatten", "-o", "json"],
                environment: environment,
                timeout: normalizedConfigTimeout
            )
        } catch {
            await configCache.block(
                key: cacheKey,
                until: Date().addingTimeInterval(normalizedConfigFailureCooldown)
            )
            throw error
        }

        guard result.exitCode == 0 else {
            await configCache.block(
                key: cacheKey,
                until: Date().addingTimeInterval(normalizedConfigFailureCooldown)
            )
            throw RuneError.commandFailed(command: "kubectl config view", message: result.stderr)
        }

        let config: NormalizedKubeConfig
        do {
            config = try JSONDecoder().decode(NormalizedKubeConfig.self, from: Data(result.stdout.utf8))
        } catch {
            await configCache.block(
                key: cacheKey,
                until: Date().addingTimeInterval(normalizedConfigFailureCooldown)
            )
            throw error
        }
        await configCache.setConfig(config, for: cacheKey)
        return config
    }

    private func resolvedContext(environment: [String: String], contextName: String) async throws -> ResolvedRESTContext {
        let config = try await normalizedConfig(environment: environment)
        guard let namedContext = config.contexts.first(where: { $0.name == contextName }) else {
            throw RuneError.invalidInput(message: "Kubernetes context \(contextName) saknas i kubeconfig")
        }
        guard let namedCluster = config.clusters.first(where: { $0.name == namedContext.context.cluster }) else {
            throw RuneError.invalidInput(message: "Cluster \(namedContext.context.cluster) saknas i kubeconfig")
        }
        let namedUser = config.users.first(where: { $0.name == namedContext.context.user })

        guard let serverURL = URL(string: namedCluster.cluster.server) else {
            throw RuneError.invalidInput(message: "Ogiltig Kubernetes server-URL för context \(contextName)")
        }

        let authentication = try await resolveAuthentication(user: namedUser?.user, environment: environment)
        return ResolvedRESTContext(
            serverURL: serverURL,
            namespace: namedContext.context.namespace,
            authentication: authentication,
            insecureSkipTLSVerify: namedCluster.cluster.insecureSkipTLSVerify ?? false,
            certificateAuthorityData: namedCluster.cluster.certificateAuthorityData.flatMap { Data(base64Encoded: $0) },
            tlsServerName: namedCluster.cluster.tlsServerName
        )
    }

    private func resolveAuthentication(
        user: NormalizedKubeConfig.NamedUser.UserEntry?,
        environment: [String: String]
    ) async throws -> RESTAuthentication {
        guard let user else { return .none }

        if let token = user.token?.trimmingCharacters(in: .whitespacesAndNewlines), !token.isEmpty {
            return .bearer(token)
        }

        if let tokenFile = user.tokenFile?.trimmingCharacters(in: .whitespacesAndNewlines),
           !tokenFile.isEmpty,
           let token = try? String(contentsOfFile: tokenFile, encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines),
           !token.isEmpty {
            return .bearer(token)
        }

        if let exec = user.exec {
            if let token = try await execCredentialToken(exec, environment: environment) {
                return .bearer(token)
            }
        }

        if let username = user.username,
           let password = user.password,
           !username.isEmpty {
            return .basic(username: username, password: password)
        }

        if user.clientCertificateData != nil || user.clientKeyData != nil {
            throw RuneError.invalidInput(message: "Client certificate auth stöds inte ännu i Rune REST-transporten")
        }

        return .none
    }

    private func execCredentialToken(
        _ exec: NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig,
        environment: [String: String]
    ) async throws -> String? {
        let executable: String
        let arguments: [String]
        if exec.command.contains("/") {
            executable = exec.command
            arguments = exec.args ?? []
        } else {
            executable = "/usr/bin/env"
            arguments = [exec.command] + (exec.args ?? [])
        }

        var mergedEnvironment = environment
        for item in exec.env ?? [] {
            mergedEnvironment[item.name] = item.value
        }

        let result = try await runner.run(
            executable: executable,
            arguments: arguments,
            environment: mergedEnvironment,
            timeout: 30
        )

        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: exec.command,
                message: result.stderr.isEmpty ? "Exec credential command failed" : result.stderr
            )
        }

        let decoded = try JSONDecoder().decode(ExecCredentialResponse.self, from: Data(result.stdout.utf8))
        return decoded.status.token?.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func parseDurationSeconds(from token: String) -> Int {
        let trimmed = token.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard let unit = trimmed.last else { return 0 }
        let value = Int(trimmed.dropLast()) ?? 0
        switch unit {
        case "s": return value
        case "m": return value * 60
        case "h": return value * 3600
        default: return 0
        }
    }
}

private actor KubernetesRESTConfigCache {
    private var byKey: [String: NormalizedKubeConfig] = [:]
    private var blockedUntilByKey: [String: Date] = [:]

    func config(for key: String) -> NormalizedKubeConfig? {
        byKey[key]
    }

    func setConfig(_ config: NormalizedKubeConfig, for key: String) {
        byKey[key] = config
        blockedUntilByKey.removeValue(forKey: key)
    }

    func blockedUntil(for key: String) -> Date? {
        blockedUntilByKey[key]
    }

    func block(key: String, until: Date) {
        blockedUntilByKey[key] = until
    }
}

private struct NormalizedKubeConfig: Decodable {
    struct NamedContext: Decodable {
        struct ContextEntry: Decodable {
            let cluster: String
            let user: String
            let namespace: String?
        }

        let name: String
        let context: ContextEntry
    }

    struct NamedCluster: Decodable {
        struct ClusterEntry: Decodable {
            let server: String
            let insecureSkipTLSVerify: Bool?
            let certificateAuthorityData: String?
            let tlsServerName: String?

            enum CodingKeys: String, CodingKey {
                case server
                case insecureSkipTLSVerify = "insecure-skip-tls-verify"
                case certificateAuthorityData = "certificate-authority-data"
                case tlsServerName = "tls-server-name"
            }
        }

        let name: String
        let cluster: ClusterEntry
    }

    struct NamedUser: Decodable {
        struct UserEntry: Decodable {
            struct ExecConfig: Decodable {
                struct EnvironmentEntry: Decodable {
                    let name: String
                    let value: String
                }

                let command: String
                let args: [String]?
                let env: [EnvironmentEntry]?
            }

            let token: String?
            let tokenFile: String?
            let username: String?
            let password: String?
            let exec: ExecConfig?
            let clientCertificateData: String?
            let clientKeyData: String?

            enum CodingKeys: String, CodingKey {
                case token
                case tokenFile = "tokenFile"
                case username
                case password
                case exec
                case clientCertificateData = "client-certificate-data"
                case clientKeyData = "client-key-data"
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
}

private struct ExecCredentialResponse: Decodable {
    struct Status: Decodable {
        let token: String?
    }

    let status: Status
}

private struct ResolvedRESTContext {
    let serverURL: URL
    let namespace: String?
    let authentication: RESTAuthentication
    let insecureSkipTLSVerify: Bool
    let certificateAuthorityData: Data?
    let tlsServerName: String?
}

private enum RESTAuthentication {
    case none
    case bearer(String)
    case basic(username: String, password: String)
}

private struct RESTResponse {
    let body: String
    let contentType: String
}

private struct NamespaceList: Decodable {
    struct Item: Decodable {
        struct Metadata: Decodable {
            let name: String
        }

        let metadata: Metadata
    }

    let items: [Item]
}

private final class RESTURLSessionDelegate: NSObject, URLSessionDelegate {
    private let insecureSkipTLSVerify: Bool
    private let certificateAuthorityData: Data?
    private let tlsServerName: String?

    init(
        insecureSkipTLSVerify: Bool,
        certificateAuthorityData: Data?,
        tlsServerName: String?
    ) {
        self.insecureSkipTLSVerify = insecureSkipTLSVerify
        self.certificateAuthorityData = certificateAuthorityData
        self.tlsServerName = tlsServerName
    }

    func urlSession(
        _ session: URLSession,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        guard challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodServerTrust,
              let trust = challenge.protectionSpace.serverTrust else {
            completionHandler(.performDefaultHandling, nil)
            return
        }

        if insecureSkipTLSVerify {
            completionHandler(.useCredential, URLCredential(trust: trust))
            return
        }

        guard certificateAuthorityData != nil || tlsServerName != nil else {
            completionHandler(.performDefaultHandling, nil)
            return
        }

        let serverName = tlsServerName ?? challenge.protectionSpace.host
        let policy = SecPolicyCreateSSL(true, serverName as CFString)
        SecTrustSetPolicies(trust, policy)

        if let certificateAuthorityData,
           let certificate = SecCertificateCreateWithData(nil, pemOrDERCertificateData(certificateAuthorityData) as CFData) {
            SecTrustSetAnchorCertificates(trust, [certificate] as CFArray)
            SecTrustSetAnchorCertificatesOnly(trust, false)
        }

        var error: CFError?
        if SecTrustEvaluateWithError(trust, &error) {
            completionHandler(.useCredential, URLCredential(trust: trust))
        } else {
            completionHandler(.cancelAuthenticationChallenge, nil)
        }
    }

    private func pemOrDERCertificateData(_ data: Data) -> Data {
        guard let string = String(data: data, encoding: .utf8),
              string.contains("BEGIN CERTIFICATE"),
              let pem = extractPEMBody(from: string) else {
            return data
        }
        return pem
    }

    private func extractPEMBody(from string: String) -> Data? {
        let lines = string
            .split(whereSeparator: \.isNewline)
            .map(String.init)
            .filter { !$0.contains("BEGIN CERTIFICATE") && !$0.contains("END CERTIFICATE") }
        return Data(base64Encoded: lines.joined())
    }
}
