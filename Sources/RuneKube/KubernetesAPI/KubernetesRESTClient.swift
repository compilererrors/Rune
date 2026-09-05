import Foundation
import CryptoKit
import Darwin
import OSLog
import RuneCore
import RuneDiagnostics
import RuneSecurity
import Yams
@preconcurrency import Security
import Network
import zlib

@_silgen_name("SecKeychainCreate")
private func RuneSecKeychainCreate(
    _ pathName: UnsafePointer<CChar>,
    _ passwordLength: UInt32,
    _ password: UnsafeRawPointer?,
    _ promptUser: DarwinBoolean,
    _ initialAccess: SecAccess?,
    _ keychain: UnsafeMutablePointer<SecKeychain?>
) -> OSStatus

final class KubernetesRESTClient: @unchecked Sendable {
    private let configCache = KubernetesRESTConfigCache()
    private let execCredentialCache = KubernetesExecCredentialCache()
    private let requestCoalescer = KubernetesRESTRequestCoalescer()
    private let requestMetricsRecorder: KubernetesRESTRequestMetricsRecorder?
    private let nativeCredentialProvider: any KubernetesNativeCredentialProviding

    init(
        requestMetricsRecorder: KubernetesRESTRequestMetricsRecorder? = nil,
        nativeCredentialProvider: any KubernetesNativeCredentialProviding = DefaultKubernetesNativeCredentialProvider.shared
    ) {
        self.requestMetricsRecorder = requestMetricsRecorder
        self.nativeCredentialProvider = nativeCredentialProvider
    }

    static func _testCreateClientTLSIdentity(certificateData: Data, keyData: Data) throws -> Bool {
        try ClientTLSIdentity.temporaryIdentity(certificateData: certificateData, keyData: keyData) != nil
    }

    static func _testResolvedTLSDescription(environment: [String: String], contextName: String) async throws -> String {
        let resolved = try await KubernetesRESTClient().resolvedContext(environment: environment, contextName: contextName)
        return resolved.tlsDescription
    }

    static func _testRESTCredentialFingerprint(bearerToken: String) -> Data {
        RESTCredentialFingerprint.make(
            authentication: .bearer(bearerToken),
            clientTLSIdentity: nil
        ) ?? Data()
    }

    static func _testLateExecCredentialInvalidationPreservesFreshGeneration() async throws -> Bool {
        let cache = KubernetesExecCredentialCache()
        let key = "exec:synthetic"
        let stale = try await cache.resolve(for: key) {
            KubernetesExecCredential(
                authentication: .bearer("synthetic-stale"),
                clientTLSIdentity: nil,
                expiresAt: Date().addingTimeInterval(60)
            )
        }
        await cache.invalidate(for: key, generation: stale.generation)
        let fresh = try await cache.resolve(for: key) {
            KubernetesExecCredential(
                authentication: .bearer("synthetic-fresh"),
                clientTLSIdentity: nil,
                expiresAt: Date().addingTimeInterval(60)
            )
        }

        await cache.invalidate(for: key, generation: stale.generation)
        let reused = try await cache.resolve(for: key) {
            throw RuneError.invalidInput(
                message: "Late invalidation incorrectly removed the fresh synthetic credential"
            )
        }
        return stale.generation != fresh.generation
            && reused.generation == fresh.generation
    }

    static func _testBoundedCredentialCachesActivelyExpire() async throws -> Bool {
        let configCache = KubernetesRESTConfigCache(
            capacity: 2,
            retentionTTL: 0.05
        )
        let config = NormalizedKubeConfig(
            currentContext: nil,
            contexts: [],
            clusters: [],
            users: []
        )
        await configCache.setConfig(config, for: "config-a")
        await configCache.setConfig(config, for: "config-b")
        await configCache.setConfig(config, for: "config-c")

        let execCache = KubernetesExecCredentialCache(
            capacity: 2,
            retentionTTL: 0.05
        )
        for key in ["exec-a", "exec-b", "exec-c"] {
            _ = try await execCache.resolve(for: key) {
                KubernetesExecCredential(
                    authentication: .bearer("synthetic"),
                    clientTLSIdentity: nil,
                    expiresAt: Date().addingTimeInterval(60)
                )
            }
        }

        let configBounded = await configCache.retainedCount() == 2
        let execBounded = await execCache.retainedCount() == 2
        for _ in 0..<50 {
            let configExpired = await configCache.retainedCount() == 0
            let execExpired = await execCache.retainedCount() == 0
            if configExpired && execExpired {
                return configBounded && execBounded
            }
            try await Task.sleep(for: .milliseconds(20))
        }
        return false
    }

    static func _testLocalPortConflictMessage(
        port: Int,
        address: String,
        externalCommandsAllowed: Bool = true
    ) -> String? {
        localPortConflictMessage(
            port: port,
            address: address,
            externalCommandsAllowed: externalCommandsAllowed
        )
    }

    static func _testTerminalResizeFrame(columns: Int, rows: Int) throws -> Data {
        try terminalResizeFrame(columns: columns, rows: rows)
    }

    fileprivate static func terminalResizeFrame(columns: Int, rows: Int) throws -> Data {
        let normalizedColumns = min(max(columns, 1), 500)
        let normalizedRows = min(max(rows, 1), 200)
        let payload = try JSONSerialization.data(
            withJSONObject: ["Width": normalizedColumns, "Height": normalizedRows],
            options: [.sortedKeys]
        )
        var frame = Data([4])
        frame.append(payload)
        return frame
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

    func execCredentialCacheDiagnostic(
        environment: [String: String],
        contextName: String
    ) async throws -> KubernetesExecCredentialCacheDiagnostic? {
        let config = try await normalizedConfig(environment: environment)
        guard let namedContext = config.contexts.first(where: { $0.name == contextName }) else {
            throw RuneError.invalidInput(message: "Kubernetes context \(contextName) is missing from kubeconfig")
        }
        let namedUser = config.users.first(where: { $0.name == namedContext.context.user })
        guard let namedCluster = config.clusters.first(where: { $0.name == namedContext.context.cluster }) else {
            throw RuneError.invalidInput(message: "Cluster \(namedContext.context.cluster) is missing from kubeconfig")
        }
        if let user = namedUser?.user,
           let nativeRequest = nativeCredentialRequest(
               user: user,
               cluster: namedCluster.cluster,
               contextName: contextName,
               userName: namedUser?.name,
               clusterName: namedCluster.name
           ),
           let nativeCredential = try await nativeCredentialProvider.credential(for: nativeRequest) {
            return KubernetesExecCredentialCacheDiagnostic(
                state: .hit,
                expiresAt: nativeCredential.expiresAt
            )
        }
        guard let exec = namedUser?.user.exec else { return nil }
        let apiVersion = try Self.validatedExecCredentialAPIVersion(exec.apiVersion)
        try Self.validateExecInteractiveMode(exec.interactiveMode, apiVersion: apiVersion)
        let certificateAuthorityData: Data?
        if exec.provideClusterInfo == true {
            certificateAuthorityData = try namedCluster.cluster.resolvedCertificateAuthorityData()
        } else {
            certificateAuthorityData = nil
        }
        let execInfo = try execInfo(
            for: exec,
            cluster: namedCluster.cluster,
            apiVersion: apiVersion,
            certificateAuthorityData: certificateAuthorityData
        )
        let processEnvironment = exec.processEnvironment(base: environment, execInfo: execInfo)
        return await execCredentialCache.diagnostic(
            for: exec.cacheKey(processEnvironment: processEnvironment, execInfo: execInfo)
        )
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
                throw RuneError.invalidInput(message: "REST path is missing for resource \(resource)")
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
            throw RuneError.invalidInput(message: "REST path is missing for resource \(resource)")
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

    func watchCollection(
        environment: [String: String],
        contextName: String,
        resource: String,
        namespace: String?,
        resourceVersion: String?,
        timeoutSeconds: Int
    ) async throws -> AsyncThrowingStream<KubernetesResourceWatchEvent, Error> {
        let resolved = try await resolvedContext(environment: environment, contextName: contextName)
        let basePath: String?
        if let namespace {
            basePath = KubernetesRESTPath.namespacedCollectionPath(
                namespace: namespace,
                resource: resource
            )
        } else {
            basePath = KubernetesRESTPath.collectionPath(resource: resource, namespace: nil)
        }
        guard let basePath else {
            throw RuneError.invalidInput(message: "REST watch path is missing for resource \(resource)")
        }

        var components = URLComponents()
        components.path = basePath
        components.queryItems = [
            URLQueryItem(name: "watch", value: "1"),
            URLQueryItem(name: "allowWatchBookmarks", value: "true"),
            URLQueryItem(name: "timeoutSeconds", value: String(min(max(timeoutSeconds, 30), 600)))
        ] + (resourceVersion.map {
            [
                URLQueryItem(name: "resourceVersion", value: $0),
                URLQueryItem(name: "resourceVersionMatch", value: "NotOlderThan")
            ]
        } ?? [])
        let apiPath = components.percentEncodedPath
            + (components.percentEncodedQuery.map { "?\($0)" } ?? "")
        guard let url = URL(string: apiPath, relativeTo: resolved.serverURL)?.absoluteURL else {
            throw RuneError.invalidInput(message: "Invalid Kubernetes watch path.")
        }

        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.timeoutInterval = TimeInterval(min(max(timeoutSeconds, 30), 600) + 15)
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        switch resolved.authentication {
        case let .bearer(token):
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        case let .basic(username, password):
            let encoded = Data("\(username):\(password)".utf8).base64EncodedString()
            request.setValue("Basic \(encoded)", forHTTPHeaderField: "Authorization")
        case .none:
            break
        }

        let finalRequest = request
        return AsyncThrowingStream { continuation in
            let task = Task {
                let restSession = self.makeSession(for: resolved)
                defer { restSession.session.invalidateAndCancel() }
                do {
                    let (bytes, response) = try await restSession.session.bytes(for: finalRequest)
                    guard let httpResponse = response as? HTTPURLResponse,
                          (200..<300).contains(httpResponse.statusCode) else {
                        let status = (response as? HTTPURLResponse)?.statusCode ?? 0
                        throw RuneError.commandFailed(
                            command: "kubernetes REST watch",
                            message: "Kubernetes watch failed with HTTP status \(status)."
                        )
                    }
                    for try await line in bytes.lines {
                        try Task.checkCancellation()
                        guard !line.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { continue }
                        let event = try KubernetesResourceWatchEvent.decode(line: line)
                        if event.type == .error {
                            throw RuneError.commandFailed(
                                command: "kubernetes REST watch",
                                message: event.errorMessage ?? "Kubernetes watch reported an error."
                            )
                        }
                        continuation.yield(event)
                    }
                    continuation.finish()
                } catch is CancellationError {
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { @Sendable _ in
                task.cancel()
            }
        }
    }

    func customCollection(
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

    func customResourceYAML(
        environment: [String: String],
        contextName: String,
        collectionAPIPath: String,
        name: String,
        timeout: TimeInterval
    ) async throws -> String {
        let response = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "GET",
            apiPath: try customResourcePath(collectionAPIPath: collectionAPIPath, name: name),
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

    func customResourceDescribe(
        environment: [String: String],
        contextName: String,
        resource: OperatorResourceSummary,
        timeout: TimeInterval
    ) async throws -> String {
        let raw = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "GET",
            apiPath: try customResourcePath(collectionAPIPath: resource.apiPath, name: resource.name),
            headers: ["Accept": "application/json"],
            body: nil,
            timeout: timeout
        ).body
        let pretty = try prettyPrintedJSON(raw)
        return [
            "Name: \(resource.name)",
            "Namespace: \(resource.namespace ?? "<cluster>")",
            "Kind: \(resource.kind)",
            "Family: \(resource.family)",
            "API Path: \(resource.apiPath)",
            "Status: \(resource.status)",
            resource.message.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : "Message: \(resource.message)",
            "",
            "Manifest JSON:",
            pretty
        ].compactMap { $0 }.joined(separator: "\n")
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
        container: String?,
        filter: LogTimeFilter,
        previous: Bool,
        timeout: TimeInterval,
        profile: LogQueryProfile = .pod
    ) async throws -> String {
        let query = filter.resolvedLogQuery(profile: profile)
        var items: [URLQueryItem] = []
        if let sinceSeconds = query.sinceSeconds {
            items.append(URLQueryItem(name: "sinceSeconds", value: String(sinceSeconds)))
        }
        if let sinceTime = query.sinceTime {
            items.append(URLQueryItem(name: "sinceTime", value: ISO8601DateFormatter().string(from: sinceTime)))
        }
        if let tailLines = query.tailLines {
            items.append(URLQueryItem(name: "tailLines", value: String(tailLines)))
        }
        if let container = container?.trimmingCharacters(in: .whitespacesAndNewlines), !container.isEmpty {
            items.append(URLQueryItem(name: "container", value: container))
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
            headers: ["Accept": "*/*"],
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

    private func customResourcePath(collectionAPIPath: String, name: String) throws -> String {
        let trimmedPath = collectionAPIPath.trimmingCharacters(in: .whitespacesAndNewlines)
        let trimmedName = name.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmedPath.hasPrefix("/"), !trimmedPath.contains("?") else {
            throw RuneError.invalidInput(message: "Custom resource API path is invalid.")
        }
        guard !trimmedName.isEmpty else {
            throw RuneError.invalidInput(message: "Custom resource name is empty.")
        }
        return trimmedPath.trimmingCharacters(in: CharacterSet(charactersIn: "/")) == ""
            ? "/\(trimmedName.runePercentEncodedPathSegment)"
            : "\(trimmedPath.trimmingTrailingSlashes())/\(trimmedName.runePercentEncodedPathSegment)"
    }

    func selfSubjectAccessReview(
        environment: [String: String],
        contextName: String,
        namespace: String?,
        verb: String,
        resource: String,
        apiGroup: String? = nil,
        subresource: String?,
        timeout: TimeInterval
    ) async throws -> Bool {
        let body = try Self.selfSubjectAccessReviewRequestBody(
            namespace: namespace,
            verb: verb,
            resource: resource,
            apiGroup: apiGroup,
            subresource: subresource
        )
        let response = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "POST",
            apiPath: "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            headers: [
                "Accept": "application/json",
                "Content-Type": "application/json"
            ],
            body: body,
            timeout: timeout
        ).body
        return try Self.selfSubjectAccessReviewAllowed(from: response)
    }

    static func selfSubjectAccessReviewRequestBody(
        namespace: String?,
        verb: String,
        resource: String,
        apiGroup: String? = nil,
        subresource: String?
    ) throws -> String {
        try selfSubjectAccessReviewRequestBody(resourceAttributes: selfSubjectAccessReviewResourceAttributes(
            namespace: namespace,
            verb: verb,
            resource: resource,
            apiGroup: apiGroup,
            subresource: subresource
        ))
    }

    static func selfSubjectAccessReviewResourceAttributes(
        namespace: String?,
        verb: String,
        resource: String,
        apiGroup: String? = nil,
        subresource: String?
    ) -> [String: Any] {
        var attributes: [String: Any] = [
            "verb": verb,
            "resource": resource
        ]
        if let apiGroup = apiGroup?.trimmingCharacters(in: .whitespacesAndNewlines), !apiGroup.isEmpty {
            attributes["group"] = apiGroup
        }
        if let namespace = namespace?.trimmingCharacters(in: .whitespacesAndNewlines), !namespace.isEmpty {
            attributes["namespace"] = namespace
        }
        if let subresource = subresource?.trimmingCharacters(in: .whitespacesAndNewlines), !subresource.isEmpty {
            attributes["subresource"] = subresource
        }
        return attributes
    }

    static func selfSubjectAccessReviewRequestBody(resourceAttributes attributes: [String: Any]) throws -> String {
        let request: [String: Any] = [
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectAccessReview",
            "spec": ["resourceAttributes": attributes]
        ]
        let bodyData = try JSONSerialization.data(withJSONObject: request, options: [.sortedKeys])
        guard let body = String(data: bodyData, encoding: .utf8) else {
            throw RuneError.invalidInput(message: "Could not encode SelfSubjectAccessReview request")
        }
        return body
    }

    static func selfSubjectAccessReviewAllowed(from raw: String) throws -> Bool {
        guard let data = raw.data(using: .utf8),
              let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let status = object["status"] as? [String: Any],
              let allowed = status["allowed"] as? Bool
        else {
            throw RuneError.parseError(message: "SelfSubjectAccessReview response did not include status.allowed")
        }
        return allowed
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

    func scaleStatefulSet(
        environment: [String: String],
        contextName: String,
        namespace: String,
        statefulSetName: String,
        replicas: Int,
        timeout: TimeInterval
    ) async throws {
        let path = try resourcePath(
            kind: .statefulSet,
            namespace: namespace,
            resource: "statefulsets",
            name: statefulSetName,
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

    func restartStatefulSetRollout(
        environment: [String: String],
        contextName: String,
        namespace: String,
        statefulSetName: String,
        timeout: TimeInterval
    ) async throws {
        let path = try resourcePath(
            kind: .statefulSet,
            namespace: namespace,
            resource: "statefulsets",
            name: statefulSetName,
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

    func applyYAML(
        environment: [String: String],
        contextName: String,
        defaultNamespace: String,
        yaml: String,
        dryRun: Bool,
        timeout: TimeInterval
    ) async throws {
        let sanitizedYAML = Self.serverSideApplyYAML(from: yaml)
        let manifest = try KubernetesManifestIdentity.parse(
            yaml: sanitizedYAML,
            defaultNamespace: defaultNamespace
        )
        let resource = KubernetesRESTPath.resourceName(for: manifest.kind)
        let path = try resourcePath(
            kind: manifest.kind,
            namespace: manifest.namespace,
            resource: resource,
            name: manifest.name,
            subresource: nil
        )

        var components = URLComponents()
        components.path = path
        components.queryItems = [
            URLQueryItem(name: "fieldManager", value: "rune"),
            URLQueryItem(name: "force", value: "true")
        ] + (dryRun ? [URLQueryItem(name: "dryRun", value: "All")] : [])

        _ = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "PATCH",
            apiPath: components.percentEncodedPath + (components.percentEncodedQuery.map { "?\($0)" } ?? ""),
            headers: [
                "Accept": "application/json",
                "Content-Type": "application/apply-patch+yaml"
            ],
            body: sanitizedYAML,
            timeout: timeout
        )
    }

    static func _testServerSideApplyYAML(from yaml: String) -> String {
        serverSideApplyYAML(from: yaml)
    }

    private static func serverSideApplyYAML(from yaml: String) -> String {
        var output: [String] = []
        let lines = yaml.split(omittingEmptySubsequences: false, whereSeparator: \.isNewline).map(String.init)
        var metadataIndent: Int?
        var skippingMetadataBlockKeyIndent: Int?

        for line in lines {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            let indent = line.prefix { $0 == " " }.count

            if let skipIndent = skippingMetadataBlockKeyIndent {
                if trimmed.isEmpty {
                    continue
                }
                if indent <= (metadataIndent ?? 0) || (indent == skipIndent && !trimmed.hasPrefix("-")) {
                    skippingMetadataBlockKeyIndent = nil
                } else {
                    continue
                }
            }

            if indent == 0 {
                metadataIndent = trimmed == "metadata:" ? indent : nil
            }

            if metadataIndent != nil,
               indent > (metadataIndent ?? 0),
               trimmed == "managedFields:" || trimmed.hasPrefix("managedFields: ") {
                skippingMetadataBlockKeyIndent = indent
                continue
            }

            output.append(line)
        }

        return output.joined(separator: "\n")
    }

    func createJobFromCronJob(
        environment: [String: String],
        contextName: String,
        namespace: String,
        cronJobName: String,
        jobName: String,
        timeout: TimeInterval
    ) async throws {
        let cronJobRaw = try await resourceJSON(
            environment: environment,
            contextName: contextName,
            kind: .cronJob,
            namespace: namespace,
            name: cronJobName,
            timeout: timeout
        )
        guard
            let cronJob = try JSONSerialization.jsonObject(with: Data(cronJobRaw.utf8)) as? [String: Any],
            let spec = cronJob["spec"] as? [String: Any],
            let jobTemplate = spec["jobTemplate"] as? [String: Any],
            let jobSpec = jobTemplate["spec"] as? [String: Any]
        else {
            throw RuneError.parseError(message: "CronJob \(cronJobName) does not contain a job template")
        }

        let templateMetadata = jobTemplate["metadata"] as? [String: Any]
        var metadata: [String: Any] = [
            "name": jobName,
            "namespace": namespace,
            "labels": templateMetadata?["labels"] ?? [:],
            "annotations": templateMetadata?["annotations"] ?? [:]
        ]
        metadata["ownerReferences"] = nil

        let job: [String: Any] = [
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": metadata,
            "spec": jobSpec
        ]
        let body = String(decoding: try JSONSerialization.data(withJSONObject: job), as: UTF8.self)

        _ = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "POST",
            apiPath: "/apis/batch/v1/namespaces/\(namespace.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? namespace)/jobs",
            headers: [
                "Accept": "application/json",
                "Content-Type": "application/json"
            ],
            body: body,
            timeout: timeout
        )
    }

    func resourceDescribe(
        environment: [String: String],
        contextName: String,
        namespace: String,
        kind: KubeResourceKind,
        name: String,
        timeout: TimeInterval
    ) async throws -> String {
        let raw = try await resourceJSON(
            environment: environment,
            contextName: contextName,
            kind: kind,
            namespace: namespace,
            name: name,
            timeout: timeout
        )
        let pretty = try prettyPrintedJSON(raw)
        let events = try? await eventsForResource(
            environment: environment,
            contextName: contextName,
            namespace: namespace,
            kind: kind,
            name: name,
            timeout: min(timeout, 15)
        )
        return [
            "Name: \(name)",
            "Namespace: \(kind.isNamespaced ? namespace : "<cluster>")",
            "Kind: \(kind.singularTypeName)",
            "",
            "Manifest JSON:",
            pretty,
            "",
            "Events:",
            events?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false ? events! : "<none>"
        ].joined(separator: "\n")
    }

    func deploymentRolloutHistory(
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        timeout: TimeInterval
    ) async throws -> String {
        let deploymentRaw = try await resourceJSON(
            environment: environment,
            contextName: contextName,
            kind: .deployment,
            namespace: namespace,
            name: deploymentName,
            timeout: timeout
        )
        let selector = try KubernetesJSON.selectorMatchLabels(from: deploymentRaw)
        let selectorString = selector.sorted { $0.key < $1.key }.map { "\($0.key)=\($0.value)" }.joined(separator: ",")
        let replicaSets = try await collection(
            environment: environment,
            contextName: contextName,
            resource: "replicasets",
            namespace: namespace,
            timeout: timeout,
            options: KubernetesListOptions(labelSelector: selectorString)
        )
        let revisions = try KubernetesJSON.replicaSetRolloutRevisions(from: replicaSets)
        guard !revisions.isEmpty else {
            return "No rollout history found for deployment \(deploymentName)."
        }
        let header = "REVISION\tREPLICASET\tCHANGE-CAUSE"
        let rows = revisions.map { "\($0.revision)\t\($0.name)\t\($0.changeCause ?? "<none>")" }
        return ([header] + rows).joined(separator: "\n")
    }

    func rollbackDeploymentRollout(
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        revision: Int?,
        timeout: TimeInterval,
        dryRun: Bool = false
    ) async throws {
        let deploymentRaw = try await resourceJSON(
            environment: environment,
            contextName: contextName,
            kind: .deployment,
            namespace: namespace,
            name: deploymentName,
            timeout: timeout
        )
        let selector = try KubernetesJSON.selectorMatchLabels(from: deploymentRaw)
        let currentRevision = try KubernetesJSON.deploymentRevision(from: deploymentRaw)
        let selectorString = selector.sorted { $0.key < $1.key }.map { "\($0.key)=\($0.value)" }.joined(separator: ",")
        let replicaSets = try await collection(
            environment: environment,
            contextName: contextName,
            resource: "replicasets",
            namespace: namespace,
            timeout: timeout,
            options: KubernetesListOptions(labelSelector: selectorString)
        )
        let templates = try KubernetesJSON.replicaSetTemplates(from: replicaSets)
        let target: KubernetesJSON.RollbackTemplate?
        if let revision {
            target = templates.first { $0.revision == revision }
        } else if let currentRevision {
            target = templates.filter { $0.revision < currentRevision }.max { $0.revision < $1.revision }
        } else {
            target = templates.max { $0.revision < $1.revision }
        }
        guard let target else {
            throw RuneError.invalidInput(message: "No matching ReplicaSet revision was found for deployment \(deploymentName)")
        }
        let patch = String(decoding: try JSONSerialization.data(withJSONObject: ["spec": ["template": target.template]]), as: UTF8.self)
        var path = try resourcePath(kind: .deployment, namespace: namespace, resource: "deployments", name: deploymentName, subresource: nil)
        if dryRun {
            path += "?dryRun=All"
        }
        _ = try await rawRequest(
            environment: environment,
            contextName: contextName,
            method: "PATCH",
            apiPath: path,
            headers: [
                "Accept": "application/json",
                "Content-Type": "application/strategic-merge-patch+json"
            ],
            body: patch,
            timeout: timeout
        )
    }

    func verifyDeploymentRollout(
        environment: [String: String],
        contextName: String,
        namespace: String,
        deploymentName: String,
        timeout: TimeInterval,
        pollInterval: TimeInterval
    ) async throws -> DeploymentRolloutVerificationResult {
        let deadline = Date().addingTimeInterval(timeout)
        var lastSnapshot: KubernetesJSON.DeploymentRolloutSnapshot?

        while true {
            let remaining = max(0.05, deadline.timeIntervalSinceNow)
            let raw = try await resourceJSON(
                environment: environment,
                contextName: contextName,
                kind: .deployment,
                namespace: namespace,
                name: deploymentName,
                timeout: min(5, remaining)
            )
            let snapshot = try KubernetesJSON.deploymentRolloutSnapshot(from: raw, deploymentName: deploymentName)
            lastSnapshot = snapshot

            if let failureMessage = snapshot.progressFailureMessage {
                return snapshot.result(status: .failed, message: failureMessage)
            }
            if snapshot.isReady {
                return snapshot.result(
                    status: .ready,
                    message: "Deployment \(deploymentName) is ready \(snapshot.readyReplicas)/\(snapshot.desiredReplicas)."
                )
            }

            let remainingBeforeSleep = deadline.timeIntervalSinceNow
            guard remainingBeforeSleep > 0 else { break }
            let sleepSeconds = min(pollInterval, remainingBeforeSleep)
            try await Task.sleep(nanoseconds: UInt64(max(0.01, sleepSeconds) * 1_000_000_000))
        }

        let snapshot = lastSnapshot ?? KubernetesJSON.DeploymentRolloutSnapshot(
            deploymentName: deploymentName,
            desiredReplicas: 0,
            readyReplicas: 0,
            updatedReplicas: 0,
            availableReplicas: 0,
            observedGeneration: nil,
            generation: nil,
            progressFailureMessage: nil
        )
        return snapshot.result(
            status: .timedOut,
            message: "Timed out waiting for Deployment \(deploymentName) rollout readiness. Last observed readiness was \(snapshot.readyReplicas)/\(snapshot.desiredReplicas)."
        )
    }

    func execInPod(
        environment: [String: String],
        contextName: String,
        namespace: String,
        podName: String,
        container: String?,
        command: [String],
        timeout: TimeInterval
    ) async throws -> PodExecResult {
        let resolved = try await resolvedContext(environment: environment, contextName: contextName)
        let restSession = makeSession(for: resolved)
        defer { restSession.session.invalidateAndCancel() }

        var components = URLComponents()
        components.path = try resourcePath(kind: .pod, namespace: namespace, resource: "pods", name: podName, subresource: "exec")
        var queryItems = [
            URLQueryItem(name: "stdin", value: "false"),
            URLQueryItem(name: "stdout", value: "true"),
            URLQueryItem(name: "stderr", value: "true"),
            URLQueryItem(name: "tty", value: "false")
        ]
        if let container, !container.isEmpty {
            queryItems.append(URLQueryItem(name: "container", value: container))
        }
        queryItems.append(contentsOf: command.map { URLQueryItem(name: "command", value: $0) })
        components.queryItems = queryItems

        let task = try makeWebSocketTask(
            session: restSession.session,
            resolved: resolved,
            apiPath: components.percentEncodedPath + (components.percentEncodedQuery.map { "?\($0)" } ?? "")
        )
        task.resume()
        defer { task.cancel(with: .goingAway, reason: nil) }

        let result = try await receiveExecOutput(task: task, timeout: timeout)
        return PodExecResult(
            podName: podName,
            namespace: namespace,
            command: command,
            stdout: result.stdout,
            stderr: result.stderr,
            exitCode: result.exitCode
        )
    }

    func startPodTerminalSession(
        environment: [String: String],
        contextName: String,
        namespace: String,
        podName: String,
        container: String?,
        shellCommand: [String],
        onOutput: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) -> Void
    ) async throws -> any RunningCommandControlling {
        let resolved = try await resolvedContext(environment: environment, contextName: contextName)
        let restSession = makeSession(for: resolved)
        var components = URLComponents()
        components.path = try resourcePath(kind: .pod, namespace: namespace, resource: "pods", name: podName, subresource: "exec")
        var queryItems = [
            URLQueryItem(name: "stdin", value: "true"),
            URLQueryItem(name: "stdout", value: "true"),
            URLQueryItem(name: "stderr", value: "true"),
            URLQueryItem(name: "tty", value: "true")
        ]
        if let container, !container.isEmpty {
            queryItems.append(URLQueryItem(name: "container", value: container))
        }
        queryItems.append(contentsOf: (shellCommand.isEmpty ? ["sh"] : shellCommand).map {
            URLQueryItem(name: "command", value: $0)
        })
        components.queryItems = queryItems
        let task = try makeWebSocketTask(
            session: restSession.session,
            resolved: resolved,
            apiPath: components.percentEncodedPath + (components.percentEncodedQuery.map { "?\($0)" } ?? "")
        )
        let handle = KubernetesExecWebSocketHandle(
            task: task,
            session: restSession.session,
            onOutput: onOutput,
            onTermination: onTermination
        )
        task.resume()
        handle.startReceiving()
        return handle
    }

    func startPodPortForward(
        environment: [String: String],
        contextName: String,
        namespace: String,
        podName: String,
        localPort: Int,
        remotePort: Int,
        address: String,
        onReady: @escaping @Sendable () -> Void,
        onFailure: @escaping @Sendable (String) -> Void
    ) async throws -> any RunningCommandControlling {
        guard (1...65535).contains(localPort), (1...65535).contains(remotePort) else {
            throw RuneError.invalidInput(message: "Port-forward ports must be between 1 and 65535.")
        }

        if let conflictMessage = Self.localPortConflictMessage(port: localPort, address: address) {
            throw RuneError.commandFailed(command: "port-forward", message: conflictMessage)
        }

        let resolved = try await resolvedContext(environment: environment, contextName: contextName)
        if resolved.requiresLegacySPDYPortForward {
            let port = NWEndpoint.Port(rawValue: UInt16(localPort))!
            let parameters = NWParameters.tcp
            parameters.allowLocalEndpointReuse = true
            let listener: NWListener
            do {
                listener = try NWListener(using: parameters, on: port)
            } catch {
                let message = Self.localPortConflictMessage(port: localPort, address: address)
                    ?? "Could not bind local port \(address):\(localPort): \(error.localizedDescription)"
                throw RuneError.commandFailed(command: "port-forward", message: message)
            }
            let handle = LegacySPDYPortForwardHandle(
                listener: listener,
                resolved: resolved,
                namespace: namespace,
                podName: podName,
                remotePort: remotePort,
                onReady: onReady,
                onFailure: onFailure
            )
            handle.start()
            return handle
        }

        let restSession = makeSession(for: resolved)
        let port = NWEndpoint.Port(rawValue: UInt16(localPort))!
        let parameters = NWParameters.tcp
        parameters.allowLocalEndpointReuse = true

        let listener: NWListener
        do {
            listener = try NWListener(using: parameters, on: port)
        } catch {
            restSession.session.invalidateAndCancel()
            let message = Self.localPortConflictMessage(port: localPort, address: address)
                ?? "Could not bind local port \(address):\(localPort): \(error.localizedDescription)"
            throw RuneError.commandFailed(command: "port-forward", message: message)
        }

        let handle = KubernetesPortForwardHandle(
            listener: listener,
            session: restSession.session,
            resolved: resolved,
            namespace: namespace,
            podName: podName,
            remotePort: remotePort,
            makeTask: { [weak self] session, resolved in
                guard let self else {
                    throw RuneError.commandFailed(command: "port-forward", message: "Kubernetes client was released.")
                }
                return try self.makePortForwardWebSocketTask(
                    session: session,
                    resolved: resolved,
                    namespace: namespace,
                    podName: podName,
                    remotePort: remotePort
                )
            },
            onReady: onReady,
            onFailure: onFailure
        )
        handle.start()
        return handle
    }

    private static func localPortConflictMessage(
        port: Int,
        address: String,
        externalCommandsAllowed: Bool = RuneExternalCommandPolicy.allowsExternalCommands
    ) -> String? {
        guard externalCommandsAllowed else { return nil }
        guard let owner = localTCPListenerOwner(port: port) else { return nil }
        return "Port in use: \(address):\(port) is already used by \(owner.command) (pid \(owner.pid))."
    }

    private static func localTCPListenerOwner(port: Int) -> PortOwner? {
        let executable = "/usr/sbin/lsof"
        guard FileManager.default.isExecutableFile(atPath: executable) else { return nil }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = ["-nP", "-iTCP:\(port)", "-sTCP:LISTEN", "-Fpct"]
        let output = Pipe()
        process.standardOutput = output
        process.standardError = Pipe()

        do {
            try process.run()
        } catch {
            return nil
        }
        process.waitUntilExit()

        guard process.terminationStatus == 0 else { return nil }
        let raw = String(decoding: output.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
        return PortOwner.parseLsofFieldOutput(raw)
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
            throw RuneError.invalidInput(message: "REST path is missing for \(kind.rawValue)")
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
        let metricsRequestGeneration = await requestMetricsRecorder?.reserveScopeGeneration()
        let resolved = try await resolvedContext(
            environment: environment,
            contextName: contextName,
            metricsRequestGeneration: metricsRequestGeneration
        )
        let metricsScope = resolved.metricsScope
        VerboseKubeTrace.append(
            "k8s.request",
            "start method=\(method) context=<redacted-context> path=\(apiPath) server=\(resolved.serverURL.host ?? resolved.serverURL.absoluteString) tls=\(resolved.tlsDescription) auth=\(resolved.authentication.traceDescription) kubeconfigs=\(VerboseKubeTrace.kubeconfigSummary(environment))"
        )
        let restSession = makeSession(for: resolved)
        defer { restSession.session.invalidateAndCancel() }

        guard let url = URL(string: apiPath, relativeTo: resolved.serverURL)?.absoluteURL else {
            throw RuneError.invalidInput(message: "Invalid Kubernetes API path: \(apiPath)")
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

        if KubernetesRESTRequestCoalescingKey.isCoalescible(method: method, body: body),
           let credentialFingerprint = RESTCredentialFingerprint.make(
               authentication: resolved.authentication,
               clientTLSIdentity: resolved.clientTLSIdentity
           ) {
            let finalRequest = request
            let key = KubernetesRESTRequestCoalescingKey(
                method: method,
                server: resolved.serverURL.absoluteString,
                contextName: contextName,
                scopeIdentity: resolved.metricsScopeIdentity,
                credentialFingerprint: credentialFingerprint,
                apiPath: apiPath,
                headers: headers,
                timeout: timeout
            )
            return try await requestCoalescer.value(for: key) {
                try await self.performRawRequest(
                    request: finalRequest,
                    restSession: restSession,
                    method: method,
                    contextName: contextName,
                    apiPath: apiPath,
                    resolved: resolved,
                    metricsScope: metricsScope
                )
            }
        }

        return try await performRawRequest(
            request: request,
            restSession: restSession,
            method: method,
            contextName: contextName,
            apiPath: apiPath,
            resolved: resolved,
            metricsScope: metricsScope
        )
    }

    private func performRawRequest(
        request: URLRequest,
        restSession: RESTURLSession,
        method: String,
        contextName: String,
        apiPath: String,
        resolved: ResolvedRESTContext,
        metricsScope: KubernetesRESTRequestMetricsScopeToken?
    ) async throws -> RESTResponse {
        var attempt = 1
        while true {
            let attemptStarted = ContinuousClock.now
            let data: Data
            let response: URLResponse
            do {
                (data, response) = try await restSession.session.data(for: request)
            } catch {
                let retryDecision = KubernetesRequestRetryPolicy.classifyNetworkError(error)
                let tlsFailure = restSession.delegate.lastTLSFailure()
                let errorMessage = networkErrorMessage(error, resolved: resolved, tlsFailure: tlsFailure)
                let cancellationReason = requestCancellationReason(error)
                let isTrustChallengeCancellation = tlsFailure != nil && !(error is CancellationError && Task.isCancelled)
                let effectiveCancellationReason = isTrustChallengeCancellation ? nil : cancellationReason
                let outcome = effectiveCancellationReason == nil ? KubernetesRESTRequestMetricOutcome.networkError : .cancelled
                if KubernetesRequestRetryPolicy.shouldRetry(method: method, decision: retryDecision, attempt: attempt) {
                    await recordRequestMetric(
                        method: method,
                        metricsScope: metricsScope,
                        apiPath: apiPath,
                        statusCode: nil,
                        responseBytes: 0,
                        attempt: attempt,
                        outcome: outcome,
                        cancellationReason: effectiveCancellationReason,
                        started: attemptStarted
                    )
                    try await sleepBeforeKubernetesRetry(
                        method: method,
                        contextName: contextName,
                        apiPath: apiPath,
                        attempt: attempt,
                        decision: retryDecision
                    )
                    attempt += 1
                    continue
                }
                VerboseKubeTrace.append(
                    "k8s.request",
                    "failed method=\(method) context=<redacted-context> path=\(apiPath) \(retryDecision.traceDescription) error=\(errorMessage)"
                )
                await recordRequestMetric(
                    method: method,
                    metricsScope: metricsScope,
                    apiPath: apiPath,
                    statusCode: nil,
                    responseBytes: 0,
                    attempt: attempt,
                    outcome: outcome,
                    cancellationReason: effectiveCancellationReason,
                    started: attemptStarted
                )
                if effectiveCancellationReason != nil {
                    throw CancellationError()
                }
                throw RuneError.commandFailed(
                    command: "kubernetes REST \(method) \(apiPath)",
                    message: KubernetesRESTErrorMessageFormatter.appendingRetryAdvice(
                        to: errorMessage,
                        method: method,
                        decision: retryDecision
                    )
                )
            }
            guard let http = response as? HTTPURLResponse else {
                throw RuneError.commandFailed(command: "kubernetes REST \(method) \(apiPath)", message: "Missing HTTP response")
            }

            let responseBody = Self.decodeResponseBody(data)
            VerboseKubeTrace.append(
                "k8s.request",
                "response method=\(method) context=<redacted-context> path=\(apiPath) status=\(http.statusCode)"
            )
            guard (200..<300).contains(http.statusCode) else {
                if http.statusCode == 401, let key = resolved.credentialInvalidationKey {
                    switch key {
                    case .exec(let cacheKey, let generation):
                        await execCredentialCache.invalidate(
                            for: cacheKey,
                            generation: generation
                        )
                    case .native(let bindingID, let revision):
                        await nativeCredentialProvider.invalidateCredential(
                            for: bindingID,
                            matchingRevision: revision
                        )
                    }
                }
                let retryDecision = KubernetesRequestRetryPolicy.classifyHTTPStatus(
                    http.statusCode,
                    retryAfterHeader: http.value(forHTTPHeaderField: "Retry-After")
                )
                if KubernetesRequestRetryPolicy.shouldRetry(method: method, decision: retryDecision, attempt: attempt) {
                    await recordRequestMetric(
                        method: method,
                        metricsScope: metricsScope,
                        apiPath: apiPath,
                        statusCode: http.statusCode,
                        responseBytes: data.count,
                        attempt: attempt,
                        outcome: .httpError,
                        started: attemptStarted
                    )
                    try await sleepBeforeKubernetesRetry(
                        method: method,
                        contextName: contextName,
                        apiPath: apiPath,
                        attempt: attempt,
                        decision: retryDecision
                    )
                    attempt += 1
                    continue
                }
                VerboseKubeTrace.append(
                    "k8s.request",
                    "http_error method=\(method) context=<redacted-context> path=\(apiPath) status=\(http.statusCode) \(retryDecision.traceDescription)"
                )
                await recordRequestMetric(
                    method: method,
                    metricsScope: metricsScope,
                    apiPath: apiPath,
                    statusCode: http.statusCode,
                    responseBytes: data.count,
                    attempt: attempt,
                    outcome: .httpError,
                    started: attemptStarted
                )
                let message = KubernetesRESTErrorMessageFormatter.httpErrorMessage(
                    statusCode: http.statusCode,
                    responseBody: responseBody
                )
                throw RuneError.commandFailed(
                    command: "kubernetes REST \(method) \(apiPath)",
                    message: KubernetesRESTErrorMessageFormatter.appendingRetryAdvice(
                        to: message,
                        method: method,
                        decision: retryDecision
                    )
                )
            }

            await recordRequestMetric(
                method: method,
                metricsScope: metricsScope,
                apiPath: apiPath,
                statusCode: http.statusCode,
                responseBytes: data.count,
                attempt: attempt,
                outcome: .success,
                started: attemptStarted
            )
            return RESTResponse(
                body: responseBody,
                contentType: http.value(forHTTPHeaderField: "Content-Type") ?? ""
            )
        }
    }

    private func recordRequestMetric(
        method: String,
        metricsScope: KubernetesRESTRequestMetricsScopeToken?,
        apiPath: String,
        statusCode: Int?,
        responseBytes: Int,
        attempt: Int,
        outcome: KubernetesRESTRequestMetricOutcome,
        cancellationReason: String? = nil,
        started: ContinuousClock.Instant
    ) async {
        guard let requestMetricsRecorder, let metricsScope else { return }
        let elapsed = started.duration(to: .now)
        let seconds = Double(elapsed.components.seconds) + Double(elapsed.components.attoseconds) / 1e18
        await requestMetricsRecorder.record(
            KubernetesRESTRequestMetric(
                method: method,
                apiPath: apiPath,
                statusCode: statusCode,
                responseBytes: responseBytes,
                durationSeconds: seconds,
                attempt: attempt,
                outcome: outcome,
                cancellationReason: cancellationReason
            ),
            scope: metricsScope
        )
    }

    /// Kubernetes log responses are arbitrary process output and can contain malformed UTF-8.
    /// Lossy decoding preserves every valid portion instead of dropping the complete response.
    static func decodeResponseBody(_ data: Data) -> String {
        String(decoding: data, as: UTF8.self)
    }

    private func requestCancellationReason(_ error: Error) -> String? {
        if error is CancellationError || Task.isCancelled {
            return "task-cancelled"
        }
        if let urlError = error as? URLError, urlError.code == .cancelled {
            return "urlsession-cancelled"
        }
        return nil
    }

    private func makeWebSocketTask(
        session: URLSession,
        resolved: ResolvedRESTContext,
        apiPath: String,
        protocols: [String] = ["v5.channel.k8s.io", "v4.channel.k8s.io"]
    ) throws -> URLSessionWebSocketTask {
        guard var components = URLComponents(url: resolved.serverURL, resolvingAgainstBaseURL: false) else {
            throw RuneError.invalidInput(message: "Invalid Kubernetes server URL")
        }
        components.scheme = components.scheme == "http" ? "ws" : "wss"
        let pathAndQuery = apiPath.split(separator: "?", maxSplits: 1, omittingEmptySubsequences: false)
        components.path = String(pathAndQuery.first ?? "")
        if pathAndQuery.count > 1 {
            components.percentEncodedQuery = String(pathAndQuery[1])
        }
        guard let url = components.url else {
            throw RuneError.invalidInput(message: "Invalid Kubernetes websocket path: \(apiPath)")
        }
        var request = URLRequest(url: url)
        request.setValue(protocols.joined(separator: ", "), forHTTPHeaderField: "Sec-WebSocket-Protocol")
        switch resolved.authentication {
        case let .bearer(token):
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        case let .basic(username, password):
            let encoded = Data("\(username):\(password)".utf8).base64EncodedString()
            request.setValue("Basic \(encoded)", forHTTPHeaderField: "Authorization")
        case .none:
            break
        }
        return session.webSocketTask(with: request)
    }

    private func makePortForwardWebSocketTask(
        session: URLSession,
        resolved: ResolvedRESTContext,
        namespace: String,
        podName: String,
        remotePort: Int
    ) throws -> URLSessionWebSocketTask {
        var components = URLComponents()
        components.path = try resourcePath(kind: .pod, namespace: namespace, resource: "pods", name: podName, subresource: "portforward")
        components.queryItems = [URLQueryItem(name: "port", value: "\(remotePort)")]
        return try makeWebSocketTask(
            session: session,
            resolved: resolved,
            apiPath: components.percentEncodedPath + (components.percentEncodedQuery.map { "?\($0)" } ?? ""),
            protocols: ["SPDY/3.1+portforward.k8s.io"]
        )
    }

    private func receiveExecOutput(
        task: URLSessionWebSocketTask,
        timeout: TimeInterval
    ) async throws -> (stdout: String, stderr: String, exitCode: Int32) {
        try await withThrowingTaskGroup(of: (String, String, Int32).self) { group in
            group.addTask {
                var stdout = Data()
                var stderr = Data()
                var exitCode: Int32 = 0
                while true {
                    do {
                        let message = try await task.receive()
                        let data: Data
                        switch message {
                        case let .data(value):
                            data = value
                        case let .string(value):
                            data = Data(value.utf8)
                        @unknown default:
                            continue
                        }
                        guard let channel = data.first else { continue }
                        let payload = data.dropFirst()
                        switch channel {
                        case 1:
                            stdout.append(payload)
                        case 2:
                            stderr.append(payload)
                        case 3:
                            exitCode = Self.execExitCode(from: Data(payload))
                            return (
                                String(decoding: stdout, as: UTF8.self),
                                String(decoding: stderr, as: UTF8.self),
                                exitCode
                            )
                        default:
                            continue
                        }
                    } catch {
                        if stdout.isEmpty, stderr.isEmpty {
                            throw error
                        }
                        return (
                            String(decoding: stdout, as: UTF8.self),
                            String(decoding: stderr, as: UTF8.self),
                            exitCode
                        )
                    }
                }
            }
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(max(1, timeout) * 1_000_000_000))
                throw RuneError.commandFailed(command: "pod exec", message: "Timed out after \(Int(timeout)) seconds")
            }
            let value = try await group.next()!
            group.cancelAll()
            return value
        }
    }

    fileprivate static func execExitCode(from data: Data) -> Int32 {
        guard
            let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
            let status = object["status"] as? String,
            status != "Success"
        else { return 0 }
        if
            let details = object["details"] as? [String: Any],
            let causes = details["causes"] as? [[String: Any]],
            let exit = causes.first(where: { ($0["reason"] as? String) == "ExitCode" })?["message"] as? String,
            let code = Int32(exit) {
            return code
        }
        return 1
    }

    private func prettyPrintedJSON(_ raw: String) throws -> String {
        let object = try JSONSerialization.jsonObject(with: Data(raw.utf8))
        let data = try JSONSerialization.data(withJSONObject: object, options: [.prettyPrinted, .sortedKeys])
        return String(decoding: data, as: UTF8.self)
    }

    private func eventsForResource(
        environment: [String: String],
        contextName: String,
        namespace: String,
        kind: KubeResourceKind,
        name: String,
        timeout: TimeInterval
    ) async throws -> String {
        let raw = try await collection(
            environment: environment,
            contextName: contextName,
            resource: "events",
            namespace: kind.isNamespaced ? namespace : nil,
            timeout: timeout,
            options: KubernetesListOptions(fieldSelector: "involvedObject.name=\(name),involvedObject.kind=\(kind.singularTypeName)")
        )
        return try KubernetesJSON.describeEvents(from: raw)
    }

    private func makeSession(for resolved: ResolvedRESTContext) -> RESTURLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.waitsForConnectivity = false
        let delegate = RESTURLSessionDelegate(
            insecureSkipTLSVerify: resolved.insecureSkipTLSVerify,
            certificateAuthorityData: resolved.certificateAuthorityData,
            tlsServerName: resolved.tlsServerName,
            clientTLSIdentity: resolved.clientTLSIdentity
        )
        return RESTURLSession(
            session: URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil),
            delegate: delegate
        )
    }

    private func normalizedConfig(environment: [String: String]) async throws -> NormalizedKubeConfig {
        try await normalizedConfigSnapshot(environment: environment).config
    }

    func requestMetricsScopeIdentity(
        environment: [String: String],
        contextName: String
    ) async throws -> String {
        let configSnapshot = try await normalizedConfigSnapshot(environment: environment)
        let config = configSnapshot.config
        guard let namedContext = config.contexts.first(where: { $0.name == contextName }),
              let namedCluster = config.clusters.first(where: { $0.name == namedContext.context.cluster }),
              let serverURL = URL(string: namedCluster.cluster.server) else {
            throw RuneError.invalidInput(message: "The selected Kubernetes metrics scope is unavailable.")
        }
        return Self.requestMetricsScopeIdentity(
            configIdentity: configSnapshot.cacheIdentity,
            clusterName: namedContext.context.cluster,
            userName: namedContext.context.user,
            serverURL: serverURL
        )
    }

    private func normalizedConfigSnapshot(
        environment: [String: String]
    ) async throws -> NormalizedKubeConfigSnapshot {
        let input = try NormalizedKubeConfig.readInput(environment: environment)
        let config = try await configCache.resolve(for: input.cacheIdentity) {
            try NormalizedKubeConfig.loadDirectly(input: input)
        }
        return NormalizedKubeConfigSnapshot(
            config: config,
            cacheIdentity: input.cacheIdentity
        )
    }

    private func resolvedContext(
        environment: [String: String],
        contextName: String,
        metricsRequestGeneration: UInt64? = nil
    ) async throws -> ResolvedRESTContext {
        let initialConfigIdentity = NormalizedKubeConfig.unresolvedCacheIdentity(environment: environment)
        let configSnapshot: NormalizedKubeConfigSnapshot
        do {
            configSnapshot = try await normalizedConfigSnapshot(environment: environment)
        } catch {
            await activateUnresolvedMetricsScope(
                contextName: contextName,
                configIdentity: initialConfigIdentity,
                requestGeneration: metricsRequestGeneration
            )
            throw error
        }
        let config = configSnapshot.config
        guard let namedContext = config.contexts.first(where: { $0.name == contextName }) else {
            await activateUnresolvedMetricsScope(
                contextName: contextName,
                configIdentity: configSnapshot.cacheIdentity,
                requestGeneration: metricsRequestGeneration
            )
            throw RuneError.invalidInput(message: "Kubernetes context \(contextName) is missing from kubeconfig")
        }
        guard let namedCluster = config.clusters.first(where: { $0.name == namedContext.context.cluster }) else {
            await activateUnresolvedMetricsScope(
                contextName: contextName,
                configIdentity: configSnapshot.cacheIdentity,
                requestGeneration: metricsRequestGeneration
            )
            throw RuneError.invalidInput(message: "Cluster \(namedContext.context.cluster) is missing from kubeconfig")
        }
        let namedUser = config.users.first(where: { $0.name == namedContext.context.user })

        guard let serverURL = URL(string: namedCluster.cluster.server) else {
            await activateUnresolvedMetricsScope(
                contextName: contextName,
                configIdentity: configSnapshot.cacheIdentity,
                requestGeneration: metricsRequestGeneration
            )
            throw RuneError.invalidInput(message: "Invalid Kubernetes server URL for context \(contextName)")
        }

        let metricsScopeIdentity = Self.requestMetricsScopeIdentity(
            configIdentity: configSnapshot.cacheIdentity,
            clusterName: namedContext.context.cluster,
            userName: namedContext.context.user,
            serverURL: serverURL
        )
        let metricsScope: KubernetesRESTRequestMetricsScopeToken?
        if let metricsRequestGeneration, let requestMetricsRecorder {
            metricsScope = await requestMetricsRecorder.activateScope(
                contextName: contextName,
                scopeIdentity: metricsScopeIdentity,
                requestGeneration: metricsRequestGeneration
            )
        } else {
            metricsScope = nil
        }

        let certificateAuthorityData = try namedCluster.cluster.resolvedCertificateAuthorityData()
        let credentials = try await resolveCredentials(
            user: namedUser?.user,
            cluster: namedCluster.cluster,
            contextName: contextName,
            userName: namedUser?.name,
            clusterName: namedCluster.name,
            environment: environment,
            certificateAuthorityData: certificateAuthorityData
        )
        return ResolvedRESTContext(
            serverURL: serverURL,
            namespace: namedContext.context.namespace,
            metricsScopeIdentity: metricsScopeIdentity,
            metricsScope: metricsScope,
            authentication: credentials.authentication,
            insecureSkipTLSVerify: namedCluster.cluster.insecureSkipTLSVerify ?? false,
            certificateAuthorityData: certificateAuthorityData,
            tlsServerName: namedCluster.cluster.tlsServerName,
            clientTLSIdentity: credentials.clientTLSIdentity,
            credentialInvalidationKey: credentials.credentialInvalidationKey
        )
    }

    private func activateUnresolvedMetricsScope(
        contextName: String,
        configIdentity: String,
        requestGeneration: UInt64?
    ) async {
        guard let requestGeneration, let requestMetricsRecorder else { return }
        _ = await requestMetricsRecorder.activateScope(
            contextName: contextName,
            scopeIdentity: [
                configIdentity,
                "<unresolved-context>"
            ].joined(separator: "\u{1d}"),
            requestGeneration: requestGeneration
        )
    }

    private static func requestMetricsScopeIdentity(
        configIdentity: String,
        clusterName: String,
        userName: String,
        serverURL: URL
    ) -> String {
        [
            configIdentity,
            clusterName,
            userName,
            serverURL.absoluteString
        ].joined(separator: "\u{1d}")
    }

    private func resolveCredentials(
        user: NormalizedKubeConfig.NamedUser.UserEntry?,
        cluster: NormalizedKubeConfig.NamedCluster.ClusterEntry,
        contextName: String,
        userName: String?,
        clusterName: String,
        environment: [String: String],
        certificateAuthorityData: Data?
    ) async throws -> RESTCredentialResolution {
        guard let user else { return RESTCredentialResolution(authentication: .none, clientTLSIdentity: nil) }

        if let token = user.token?.trimmingCharacters(in: .whitespacesAndNewlines), !token.isEmpty {
            return RESTCredentialResolution(
                authentication: .bearer(token),
                clientTLSIdentity: nil
            )
        }

        if let tokenFile = user.tokenFile?.trimmingCharacters(in: .whitespacesAndNewlines), !tokenFile.isEmpty {
            let token: String
            do {
                token = try String(contentsOfFile: tokenFile, encoding: .utf8)
                    .trimmingCharacters(in: .whitespacesAndNewlines)
            } catch {
                throw RuneError.invalidInput(message: "Kubeconfig tokenFile could not be read")
            }
            guard !token.isEmpty else {
                throw RuneError.invalidInput(message: "Kubeconfig tokenFile is empty")
            }
            return RESTCredentialResolution(
                authentication: .bearer(token),
                clientTLSIdentity: nil
            )
        }

        if let username = user.username,
           let password = user.password,
           !username.isEmpty {
            return RESTCredentialResolution(
                authentication: .basic(username: username, password: password),
                clientTLSIdentity: nil
            )
        }

        if user.clientCertificateData != nil || user.clientCertificate != nil || user.clientKeyData != nil || user.clientKey != nil {
            return RESTCredentialResolution(
                authentication: .none,
                clientTLSIdentity: try user.resolvedClientTLSIdentityIfAvailable()
            )
        }

        if let nativeRequest = nativeCredentialRequest(
            user: user,
            cluster: cluster,
            contextName: contextName,
            userName: userName,
            clusterName: clusterName
        ) {
            if let credential = try await nativeCredentialProvider.credential(for: nativeRequest) {
                let token = credential.bearerToken.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !token.isEmpty else {
                    throw RuneError.invalidInput(message: "Native Kubernetes authentication returned an empty bearer token")
                }
                return RESTCredentialResolution(
                    authentication: .bearer(token),
                    clientTLSIdentity: nil,
                    credentialInvalidationKey: .native(
                        nativeRequest.bindingID,
                        revision: credential.revision
                    )
                )
            }
            if user.exec != nil, !RuneExternalCommandPolicy.allowsExternalCommands {
                throw KubernetesNativeCredentialProviderError.profileMissing(provider: nativeRequest.provider)
            }
        }

        if let exec = user.exec {
            return try await resolveExecCredentials(
                exec: exec,
                cluster: cluster,
                environment: environment,
                certificateAuthorityData: certificateAuthorityData
            )
        }

        return RESTCredentialResolution(
            authentication: .none,
            clientTLSIdentity: nil
        )
    }

    private func nativeCredentialRequest(
        user: NormalizedKubeConfig.NamedUser.UserEntry,
        cluster: NormalizedKubeConfig.NamedCluster.ClusterEntry,
        contextName: String,
        userName: String?,
        clusterName: String
    ) -> KubernetesNativeCredentialRequest? {
        let execDescriptor = user.exec.map { exec in
            KubernetesNativeAuthExecDescriptor(
                apiVersion: exec.apiVersion,
                command: exec.command,
                arguments: exec.args ?? [],
                environment: (exec.env ?? []).map { entry in
                    KubernetesNativeAuthEnvironmentEntry(
                        name: entry.name,
                        value: entry.value,
                        isSensitive: Self.isSensitiveNativeAuthKey(entry.name)
                    )
                },
                installHint: exec.installHint,
                provideClusterInfo: exec.provideClusterInfo ?? false,
                interactiveMode: exec.interactiveMode
            )
        }
        let authProviderDescriptor = user.authProvider.map { authProvider in
            KubernetesNativeAuthProviderDescriptor(
                name: authProvider.name,
                configuration: authProvider.config ?? [:],
                sensitiveConfigurationKeys: Set((authProvider.config ?? [:]).keys.filter(Self.isSensitiveNativeAuthKey))
            )
        }
        guard let provider = KubernetesNativeAuthProviderClassifier.classify(
            exec: execDescriptor,
            authProvider: authProviderDescriptor,
            clusterServer: cluster.server
        ) else {
            return nil
        }
        let clusterDescriptor = KubernetesNativeAuthClusterDescriptor(
            name: clusterName,
            server: cluster.server,
            certificateAuthorityData: cluster.certificateAuthorityData,
            certificateAuthorityPath: cluster.certificateAuthority,
            insecureSkipTLSVerify: cluster.insecureSkipTLSVerify ?? false,
            tlsServerName: cluster.tlsServerName
        )
        let bindingID = KubernetesNativeAuthBindingFingerprint.make(
            contextName: contextName,
            cluster: clusterDescriptor,
            userName: userName,
            provider: provider,
            exec: execDescriptor,
            authProvider: authProviderDescriptor
        )
        return KubernetesNativeCredentialRequest(
            bindingID: bindingID,
            provider: provider,
            contextName: contextName,
            clusterName: clusterName,
            userName: userName,
            server: cluster.server,
            exec: execDescriptor,
            authProvider: authProviderDescriptor
        )
    }

    private static func isSensitiveNativeAuthKey(_ key: String) -> Bool {
        let normalized = key
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
            .replacingOccurrences(of: "_", with: "-")
        return ["access-token", "client-secret", "credential", "id-token", "password", "private-key", "refresh-token", "secret", "token"]
            .contains { normalized.contains($0) }
    }

    private func resolveExecCredentials(
        exec: NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig,
        cluster: NormalizedKubeConfig.NamedCluster.ClusterEntry,
        environment: [String: String],
        certificateAuthorityData: Data?
    ) async throws -> RESTCredentialResolution {
        let apiVersion = try Self.validatedExecCredentialAPIVersion(exec.apiVersion)
        try Self.validateExecInteractiveMode(exec.interactiveMode, apiVersion: apiVersion)
        let execInfo = try execInfo(
            for: exec,
            cluster: cluster,
            apiVersion: apiVersion,
            certificateAuthorityData: certificateAuthorityData
        )
        let processEnvironment = exec.processEnvironment(base: environment, execInfo: execInfo)
        let key = exec.cacheKey(processEnvironment: processEnvironment, execInfo: execInfo)
        let lease = try await execCredentialCache.resolve(for: key) { [self] in
            let response = try await runExecCredential(
                exec: exec,
                processEnvironment: processEnvironment,
                apiVersion: apiVersion,
                timeout: 25
            )
            guard let status = response.status else {
                throw RuneError.invalidInput(message: "Kubeconfig exec auth response is missing status")
            }

            let token = status.token?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            let hasCertificate = status.clientCertificateData != nil
            let hasKey = status.clientKeyData != nil
            guard hasCertificate == hasKey else {
                throw RuneError.invalidInput(message: "Kubeconfig exec auth returned incomplete client certificate credentials")
            }
            guard !token.isEmpty || hasCertificate else {
                throw RuneError.invalidInput(message: "Kubeconfig exec auth returned missing token or client certificate credentials")
            }

            let expiresAt: Date?
            if let rawExpiration = status.expirationTimestamp {
                guard let parsed = Self.parseExecCredentialExpiration(rawExpiration) else {
                    throw RuneError.parseError(message: "Kubeconfig exec auth returned an invalid expirationTimestamp")
                }
                expiresAt = parsed
            } else {
                expiresAt = nil
            }

            let identity: ClientTLSIdentity?
            if let certificateData = status.decodedClientCertificateData,
               let keyData = status.decodedClientKeyData {
                identity = try ClientTLSIdentity.temporaryIdentity(certificateData: certificateData, keyData: keyData)
                guard identity != nil else {
                    throw RuneError.invalidInput(message: "Kubeconfig exec auth client certificate could not be loaded")
                }
            } else {
                identity = nil
            }

            return KubernetesExecCredential(
                authentication: token.isEmpty ? .none : .bearer(token),
                clientTLSIdentity: identity,
                expiresAt: expiresAt
            )
        }
        return RESTCredentialResolution(
            authentication: lease.credential.authentication,
            clientTLSIdentity: lease.credential.clientTLSIdentity,
            credentialInvalidationKey: .exec(key, generation: lease.generation)
        )
    }

    private func runExecCredential(
        exec: NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig,
        processEnvironment: [String: String],
        apiVersion: String,
        timeout: TimeInterval
    ) async throws -> ExecCredentialResponse {
        guard RuneExternalCommandPolicy.allowsExternalCommands else {
            throw RuneError.invalidInput(message: RuneExternalCommandPolicy.disabledMessage)
        }
        let output = try await runProcess(
            command: exec.command,
            arguments: exec.args ?? [],
            environment: processEnvironment,
            installHint: exec.installHint,
            timeout: timeout
        )
        do {
            let response = try JSONDecoder().decode(ExecCredentialResponse.self, from: output.stdout)
            guard response.kind == "ExecCredential" else {
                throw RuneError.parseError(message: "Kubeconfig exec auth returned kind \(response.kind ?? "<missing>"), expected ExecCredential")
            }
            guard response.apiVersion == apiVersion else {
                throw RuneError.parseError(
                    message: "Kubeconfig exec auth returned apiVersion \(response.apiVersion ?? "<missing>"), expected \(apiVersion)"
                )
            }
            return response
        } catch {
            if let runeError = error as? RuneError {
                throw runeError
            }
            let hint = Self.execCredentialDiagnosticHint(output.stderr)
            let suffix = hint.isEmpty ? "" : " Plugin diagnostic: \(hint)"
            throw RuneError.parseError(message: "Kubeconfig exec auth response is not a valid ExecCredential JSON document.\(suffix)")
        }
    }

    private static func validatedExecCredentialAPIVersion(_ raw: String?) throws -> String {
        let value = raw?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard value == "client.authentication.k8s.io/v1" || value == "client.authentication.k8s.io/v1beta1" else {
            throw RuneError.invalidInput(
                message: value.isEmpty
                    ? "Kubeconfig exec auth is missing apiVersion"
                    : "Kubeconfig exec auth uses unsupported apiVersion \(value)"
            )
        }
        return value
    }

    private static func validateExecInteractiveMode(_ raw: String?, apiVersion: String) throws {
        let value = raw?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if value.isEmpty, apiVersion == "client.authentication.k8s.io/v1" {
            throw RuneError.invalidInput(message: "Kubeconfig exec auth v1 requires interactiveMode")
        }
        let effective = value.isEmpty ? "IfAvailable" : value
        switch effective {
        case "Never", "IfAvailable":
            return
        case "Always":
            throw RuneError.invalidInput(
                message: "Kubeconfig exec auth requires interactive stdin, which is unavailable in Rune"
            )
        default:
            throw RuneError.invalidInput(message: "Kubeconfig exec auth has unknown interactiveMode \(effective)")
        }
    }

    private static func execCredentialFailureMessage(
        output: ExecCredentialProcessOutput,
        fallback: String
    ) -> String {
        let stdout = Self.execCredentialDiagnosticHint(output.stdout)
        let stderr = Self.execCredentialDiagnosticHint(output.stderr)
        if !stderr.isEmpty {
            return stderr
        }
        if !stdout.isEmpty {
            return stdout
        }
        return fallback
    }

    private static func execCredentialDiagnosticHint(_ data: Data) -> String {
        var value = TerminalTranscriptSanitizer.sanitize(String(decoding: data.prefix(2_048), as: UTF8.self))
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let patterns = [
            #"(?i)(token|password|secret|authorization|client[-_ ]?secret)\s*[:=]\s*[^\s,;]+"#,
            #"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?:\.[A-Za-z0-9_-]{8,})?"#
        ]
        for pattern in patterns {
            guard let regex = try? NSRegularExpression(pattern: pattern) else { continue }
            let range = NSRange(value.startIndex..<value.endIndex, in: value)
            value = regex.stringByReplacingMatches(in: value, range: range, withTemplate: "<redacted>")
        }
        return String(value.prefix(1_024))
    }

    private func execInfo(
        for exec: NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig,
        cluster: NormalizedKubeConfig.NamedCluster.ClusterEntry,
        apiVersion: String,
        certificateAuthorityData: Data?
    ) throws -> String {
        var spec: [String: Any] = ["interactive": false]
        if exec.provideClusterInfo == true {
            var clusterInfo: [String: Any] = ["server": cluster.server]
            if let data = certificateAuthorityData {
                clusterInfo["certificate-authority-data"] = data.base64EncodedString()
            }
            if let insecure = cluster.insecureSkipTLSVerify {
                clusterInfo["insecure-skip-tls-verify"] = insecure
            }
            if let tlsServerName = cluster.tlsServerName {
                clusterInfo["tls-server-name"] = tlsServerName
            }
            if let proxyURL = cluster.proxyURL {
                clusterInfo["proxy-url"] = proxyURL
            }
            if let disableCompression = cluster.disableCompression {
                clusterInfo["disable-compression"] = disableCompression
            }
            if let config = cluster.execExtensionConfig {
                clusterInfo["config"] = config.foundationValue
            }
            spec["cluster"] = clusterInfo
        }
        let payload: [String: Any] = [
            "apiVersion": apiVersion,
            "kind": "ExecCredential",
            "spec": spec
        ]
        let data = try JSONSerialization.data(withJSONObject: payload, options: [.sortedKeys])
        return String(decoding: data, as: UTF8.self)
    }

    private func runProcess(
        command: String,
        arguments: [String],
        environment: [String: String],
        installHint: String?,
        timeout: TimeInterval
    ) async throws -> ExecCredentialProcessOutput {
        let executionState = ExecCredentialProcessExecutionState()
        return try await withTaskCancellationHandler {
            try await Task.detached(priority: .utility) {
                let executableURL = try Self.resolveExecExecutable(
                    command: command,
                    environment: environment,
                    installHint: installHint
                )
                let process = Process()
                let stdout = Pipe()
                let stderr = Pipe()
                let outputCapture = ExecCredentialProcessOutputCapture(maxBytesPerStream: 1_048_576)

                process.executableURL = executableURL
                process.arguments = arguments
                process.environment = environment
                process.standardInput = FileHandle.nullDevice
                process.standardOutput = stdout
                process.standardError = stderr
                stdout.fileHandleForReading.readabilityHandler = { handle in
                    outputCapture.appendStdout(handle.availableData)
                }
                stderr.fileHandleForReading.readabilityHandler = { handle in
                    outputCapture.appendStderr(handle.availableData)
                }
                defer {
                    stdout.fileHandleForReading.readabilityHandler = nil
                    stderr.fileHandleForReading.readabilityHandler = nil
                    executionState.clear(process)
                }

                guard executionState.register(process) else { throw CancellationError() }
                do {
                    try process.run()
                } catch {
                    throw RuneError.commandFailed(
                        command: "kubeconfig exec auth \(command)",
                        message: TerminalTranscriptSanitizer.sanitize(error.localizedDescription)
                    )
                }

                let deadline = Date().addingTimeInterval(timeout)
                while process.isRunning, Date() < deadline, !executionState.isCancelled {
                    try? await Task.sleep(nanoseconds: 25_000_000)
                }
                let cancelled = executionState.isCancelled || Task.isCancelled
                let timedOut = process.isRunning && !cancelled
                if process.isRunning {
                    process.terminate()
                    let graceDeadline = Date().addingTimeInterval(2)
                    while process.isRunning, Date() < graceDeadline {
                        try? await Task.sleep(nanoseconds: 25_000_000)
                    }
                    if process.isRunning {
                        kill(process.processIdentifier, SIGKILL)
                        let killDeadline = Date().addingTimeInterval(2)
                        while process.isRunning, Date() < killDeadline {
                            try? await Task.sleep(nanoseconds: 25_000_000)
                        }
                    }
                }

                // Foundation's waitUntilExit() can block indefinitely when a Process uses
                // readability handlers, even after isRunning has become false. Keep this
                // authentication path fully bounded and only inspect terminationStatus after
                // Process has reported that the child exited.
                guard !process.isRunning else {
                    if cancelled { throw CancellationError() }
                    throw RuneError.commandFailed(
                        command: "kubeconfig exec auth \(command)",
                        message: "The credential process did not terminate"
                    )
                }

                outputCapture.appendStdout(stdout.fileHandleForReading.readDataToEndOfFile())
                outputCapture.appendStderr(stderr.fileHandleForReading.readDataToEndOfFile())
                let output = outputCapture.snapshot()
                if cancelled { throw CancellationError() }
                if timedOut {
                    let fallback = "Timed out after \(Int(timeout)) seconds"
                    let capturedMessage = Self.execCredentialFailureMessage(output: output, fallback: fallback)
                    let message = capturedMessage == fallback ? fallback : "\(fallback): \(capturedMessage)"
                    throw RuneError.commandFailed(command: "kubeconfig exec auth \(command)", message: message)
                }
                guard process.terminationStatus == 0 else {
                    throw RuneError.commandFailed(
                        command: "kubeconfig exec auth \(command)",
                        message: Self.execCredentialFailureMessage(
                            output: output,
                            fallback: "Exited with code \(process.terminationStatus)"
                        )
                    )
                }
                return output
            }.value
        } onCancel: {
            executionState.cancel()
        }
    }

    private static func resolveExecExecutable(
        command: String,
        environment: [String: String],
        installHint: String?
    ) throws -> URL {
        let expanded = NSString(string: command).expandingTildeInPath
        let candidates: [String]
        if expanded.contains("/") {
            candidates = [expanded]
        } else {
            candidates = RuneExecutableSearchPath.directories(from: environment).map {
                URL(fileURLWithPath: $0).appendingPathComponent(expanded).path
            }
        }
        if let executable = candidates.first(where: FileManager.default.isExecutableFile(atPath:)) {
            return URL(fileURLWithPath: executable)
        }

        let existing = candidates.first(where: FileManager.default.fileExists(atPath:))
        let reason = existing == nil ? "Executable was not found." : "Configured file is not executable."
        let hint = installHint.map {
            TerminalTranscriptSanitizer.sanitize($0).trimmingCharacters(in: .whitespacesAndNewlines)
        } ?? ""
        throw RuneError.commandFailed(
            command: "kubeconfig exec auth \(command)",
            message: hint.isEmpty ? reason : "\(reason) \(String(hint.prefix(1_024)))"
        )
    }

    private static func parseExecCredentialExpiration(_ raw: String) -> Date? {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = formatter.date(from: raw) {
            return date
        }
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: raw)
    }

}

enum BoundedExpiringLRUCacheLookup<Value> {
    case hit(Value)
    case expired(value: Value, expirationDate: Date)
    case miss
}

struct BoundedExpiringLRUCache<Key: Hashable, Value> {
    private struct Entry {
        let value: Value
        let expirationDate: Date
        var accessOrder: UInt64
    }

    let capacity: Int
    private(set) var count = 0
    private var entries: [Key: Entry] = [:]
    private var accessOrder: UInt64 = 0

    init(capacity: Int) {
        precondition(capacity > 0, "Cache capacity must be positive")
        self.capacity = capacity
    }

    var keys: Set<Key> {
        Set(entries.keys)
    }

    var earliestExpirationDate: Date? {
        entries.values.lazy.map(\.expirationDate).min()
    }

    mutating func lookup(for key: Key, now: Date = Date()) -> BoundedExpiringLRUCacheLookup<Value> {
        guard var entry = entries[key] else { return .miss }
        guard entry.expirationDate > now else {
            entries.removeValue(forKey: key)
            count = entries.count
            return .expired(value: entry.value, expirationDate: entry.expirationDate)
        }
        entry.accessOrder = nextAccessOrder()
        entries[key] = entry
        return .hit(entry.value)
    }

    mutating func insert(
        _ value: Value,
        for key: Key,
        expirationDate: Date,
        now: Date = Date()
    ) {
        _ = removeExpired(now: now)
        guard expirationDate > now else {
            entries.removeValue(forKey: key)
            count = entries.count
            return
        }

        let entryAccessOrder = nextAccessOrder()
        entries[key] = Entry(
            value: value,
            expirationDate: expirationDate,
            accessOrder: entryAccessOrder
        )
        if entries.count > capacity,
           let leastRecentlyUsedKey = entries.min(by: {
               $0.value.accessOrder < $1.value.accessOrder
           })?.key {
            entries.removeValue(forKey: leastRecentlyUsedKey)
        }
        count = entries.count
    }

    @discardableResult
    mutating func removeValue(for key: Key) -> Value? {
        let value = entries.removeValue(forKey: key)?.value
        count = entries.count
        return value
    }

    @discardableResult
    mutating func removeExpired(
        now: Date = Date()
    ) -> [(key: Key, value: Value, expirationDate: Date)] {
        let expired = entries.compactMap { key, entry in
            entry.expirationDate <= now
                ? (key: key, value: entry.value, expirationDate: entry.expirationDate)
                : nil
        }
        for entry in expired {
            entries.removeValue(forKey: entry.key)
        }
        count = entries.count
        return expired
    }

    private mutating func nextAccessOrder() -> UInt64 {
        if accessOrder == .max {
            let keysByRecency = entries
                .sorted { $0.value.accessOrder < $1.value.accessOrder }
                .map(\.key)
            for (index, key) in keysByRecency.enumerated() {
                guard var entry = entries[key] else { continue }
                entry.accessOrder = UInt64(index)
                entries[key] = entry
            }
            accessOrder = UInt64(entries.count)
        }
        accessOrder += 1
        return accessOrder
    }
}

enum KubernetesSensitiveCacheKeyComponent {
    case string(String?)
    case data(Data?)
    case strings([String])
    case keyValuePairs([(String, String)])
}

enum KubernetesSensitiveCacheKey {
    static func make(
        namespace: String,
        components: [KubernetesSensitiveCacheKeyComponent]
    ) -> String {
        var material = Data("rune-sensitive-cache-key-v2".utf8)
        append(tag: 0x01, value: Data(namespace.utf8), to: &material)
        for component in components {
            switch component {
            case .string(let value):
                appendOptional(tag: 0x02, value: value.map { Data($0.utf8) }, to: &material)
            case .data(let value):
                appendOptional(tag: 0x03, value: value, to: &material)
            case .strings(let values):
                appendCount(values.count, tag: 0x04, to: &material)
                for value in values {
                    append(tag: 0x05, value: Data(value.utf8), to: &material)
                }
            case .keyValuePairs(let pairs):
                appendCount(pairs.count, tag: 0x06, to: &material)
                for (key, value) in pairs {
                    append(tag: 0x07, value: Data(key.utf8), to: &material)
                    append(tag: 0x08, value: Data(value.utf8), to: &material)
                }
            }
        }
        let digest = SHA256.hash(data: material)
        return "\(namespace):\(Data(digest).base64EncodedString())"
    }

    static func effectiveEnvironmentPairs(
        processEnvironment: [String: String],
        baseEnvironment: [String: String],
        execEnvironment: [(String, String)]
    ) -> [(String, String)] {
        var effective = processEnvironment
        for (key, value) in baseEnvironment {
            effective[key] = value
        }
        effective["PATH"] = RuneExecutableSearchPath.pathValue(from: effective)
        for (key, value) in execEnvironment {
            effective[key] = value
        }
        effective.removeValue(forKey: "KUBERNETES_EXEC_INFO")
        return effective.sorted { lhs, rhs in
            lhs.key == rhs.key ? lhs.value < rhs.value : lhs.key < rhs.key
        }
    }

    private static func appendOptional(
        tag: UInt8,
        value: Data?,
        to material: inout Data
    ) {
        if let value {
            append(tag: tag, value: value, to: &material)
        } else {
            append(tag: tag | 0x80, value: Data(), to: &material)
        }
    }

    private static func appendCount(_ count: Int, tag: UInt8, to material: inout Data) {
        var value = UInt64(count).bigEndian
        append(
            tag: tag,
            value: withUnsafeBytes(of: &value) { Data($0) },
            to: &material
        )
    }

    private static func append(tag: UInt8, value: Data, to material: inout Data) {
        material.append(tag)
        var length = UInt64(value.count).bigEndian
        withUnsafeBytes(of: &length) { material.append(contentsOf: $0) }
        material.append(value)
    }
}

private actor KubernetesRESTConfigCache {
    private struct InFlight {
        let id: UUID
        let task: Task<NormalizedKubeConfig, Error>
    }

    private static let defaultCapacity = 32
    private static let defaultRetentionTTL: TimeInterval = 5 * 60

    private let retentionTTL: TimeInterval
    private let nowProvider: @Sendable () -> Date
    private var byKey: BoundedExpiringLRUCache<String, NormalizedKubeConfig>
    private var inFlightByKey: [String: InFlight] = [:]
    private var expirationTask: Task<Void, Never>?

    init(
        capacity: Int = defaultCapacity,
        retentionTTL: TimeInterval = defaultRetentionTTL,
        nowProvider: @escaping @Sendable () -> Date = Date.init
    ) {
        self.retentionTTL = retentionTTL
        self.nowProvider = nowProvider
        self.byKey = BoundedExpiringLRUCache(capacity: capacity)
    }

    deinit {
        expirationTask?.cancel()
        for inFlight in inFlightByKey.values {
            inFlight.task.cancel()
        }
    }

    func resolve(
        for key: String,
        loader: @escaping @Sendable () async throws -> NormalizedKubeConfig
    ) async throws -> NormalizedKubeConfig {
        if let cached = config(for: key) {
            return cached
        }

        let inFlight: InFlight
        if let existing = inFlightByKey[key] {
            inFlight = existing
        } else {
            let priority = Task.currentPriority
            let created = InFlight(
                id: UUID(),
                task: Task.detached(priority: priority) {
                    try await loader()
                }
            )
            inFlightByKey[key] = created
            inFlight = created
        }

        do {
            let config = try await inFlight.task.value
            if inFlightByKey[key]?.id == inFlight.id {
                inFlightByKey.removeValue(forKey: key)
                setConfig(config, for: key)
            }
            return config
        } catch {
            if inFlightByKey[key]?.id == inFlight.id {
                inFlightByKey.removeValue(forKey: key)
            }
            throw error
        }
    }

    func config(for key: String) -> NormalizedKubeConfig? {
        let now = nowProvider()
        switch byKey.lookup(for: key, now: now) {
        case .hit(let config):
            return config
        case .expired:
            scheduleExpiration(now: now)
            return nil
        case .miss:
            return nil
        }
    }

    func setConfig(_ config: NormalizedKubeConfig, for key: String) {
        let now = nowProvider()
        byKey.insert(
            config,
            for: key,
            expirationDate: now.addingTimeInterval(retentionTTL),
            now: now
        )
        scheduleExpiration(now: now)
    }

    func retainedCount() -> Int {
        byKey.count
    }

    private func purgeExpired() {
        let now = nowProvider()
        _ = byKey.removeExpired(now: now)
        scheduleExpiration(now: now)
    }

    private func scheduleExpiration(now: Date) {
        expirationTask?.cancel()
        guard let expirationDate = byKey.earliestExpirationDate else {
            expirationTask = nil
            return
        }
        let delay = max(0, expirationDate.timeIntervalSince(now))
        expirationTask = Task { [weak self] in
            do {
                try await Task.sleep(for: .seconds(delay))
            } catch {
                return
            }
            guard !Task.isCancelled else { return }
            await self?.purgeExpired()
        }
    }
}

private actor KubernetesExecCredentialCache {
    private struct InFlight {
        let id: UUID
        let task: Task<KubernetesExecCredential, Error>
        var waiterCount: Int
        var resolvedLease: KubernetesExecCredentialLease?
    }

    private static let defaultCapacity = 64
    private static let defaultRetentionTTL: TimeInterval = 15 * 60
    private static let expiredDiagnosticTTL: TimeInterval = 5 * 60

    private let retentionTTL: TimeInterval
    private let nowProvider: @Sendable () -> Date
    private var byKey: BoundedExpiringLRUCache<String, KubernetesExecCredentialLease>
    private var expiredAtByKey: BoundedExpiringLRUCache<String, Date>
    private var inFlightByKey: [String: InFlight] = [:]
    private var expirationTask: Task<Void, Never>?

    init(
        capacity: Int = defaultCapacity,
        retentionTTL: TimeInterval = defaultRetentionTTL,
        nowProvider: @escaping @Sendable () -> Date = Date.init
    ) {
        self.retentionTTL = retentionTTL
        self.nowProvider = nowProvider
        self.byKey = BoundedExpiringLRUCache(capacity: capacity)
        self.expiredAtByKey = BoundedExpiringLRUCache(capacity: capacity)
    }

    deinit {
        expirationTask?.cancel()
        for inFlight in inFlightByKey.values {
            inFlight.task.cancel()
        }
    }

    func resolve(
        for key: String,
        loader: @escaping @Sendable () async throws -> KubernetesExecCredential
    ) async throws -> KubernetesExecCredentialLease {
        if let credential = validCredential(for: key) {
            return credential
        }

        let inFlight: InFlight
        if var existing = inFlightByKey[key] {
            existing.waiterCount += 1
            inFlightByKey[key] = existing
            inFlight = existing
        } else {
            let created = InFlight(
                id: UUID(),
                task: Task.detached(priority: .utility) {
                    try await loader()
                },
                waiterCount: 1,
                resolvedLease: nil
            )
            inFlightByKey[key] = created
            inFlight = created
        }

        do {
            let credential = try await inFlight.task.value
            return try completeResolution(
                credential,
                for: key,
                inFlightID: inFlight.id
            )
        } catch {
            finishFailedResolution(for: key, inFlightID: inFlight.id)
            throw error
        }
    }

    private func validCredential(for key: String) -> KubernetesExecCredentialLease? {
        let now = nowProvider()
        switch byKey.lookup(for: key, now: now) {
        case .hit(let lease):
            return lease
        case .expired(let lease, let expirationDate):
            recordExpiredCredentialIfNeeded(
                lease.credential,
                cacheExpirationDate: expirationDate,
                for: key,
                now: now
            )
            scheduleExpiration(now: now)
            return nil
        case .miss:
            return nil
        }
    }

    private func completeResolution(
        _ credential: KubernetesExecCredential,
        for key: String,
        inFlightID: UUID
    ) throws -> KubernetesExecCredentialLease {
        guard var inFlight = inFlightByKey[key], inFlight.id == inFlightID else {
            if let cached = validCredential(for: key) {
                return cached
            }
            throw CancellationError()
        }

        let lease: KubernetesExecCredentialLease
        if let resolvedLease = inFlight.resolvedLease {
            lease = resolvedLease
        } else {
            lease = cache(credential, for: key)
            inFlight.resolvedLease = lease
        }
        inFlight.waiterCount -= 1
        if inFlight.waiterCount == 0 {
            inFlightByKey.removeValue(forKey: key)
        } else {
            inFlightByKey[key] = inFlight
        }
        return lease
    }

    private func finishFailedResolution(for key: String, inFlightID: UUID) {
        guard var inFlight = inFlightByKey[key], inFlight.id == inFlightID else { return }
        inFlight.waiterCount -= 1
        if inFlight.waiterCount == 0 {
            inFlightByKey.removeValue(forKey: key)
        } else {
            inFlightByKey[key] = inFlight
        }
    }

    private func cache(
        _ credential: KubernetesExecCredential,
        for key: String
    ) -> KubernetesExecCredentialLease {
        let now = nowProvider()
        let retentionLimit = now.addingTimeInterval(retentionTTL)
        let expirationDate = min(credential.expiresAt ?? retentionLimit, retentionLimit)
        let lease = KubernetesExecCredentialLease(
            credential: credential,
            generation: UUID()
        )
        if expirationDate > now {
            byKey.insert(
                lease,
                for: key,
                expirationDate: expirationDate,
                now: now
            )
            expiredAtByKey.removeValue(for: key)
        } else if let expiresAt = credential.expiresAt {
            recordExpired(expiresAt, for: key, now: now)
        }
        scheduleExpiration(now: now)
        return lease
    }

    func diagnostic(for key: String) -> KubernetesExecCredentialCacheDiagnostic {
        let now = nowProvider()
        switch byKey.lookup(for: key, now: now) {
        case .hit(let lease):
            return KubernetesExecCredentialCacheDiagnostic(
                state: .hit,
                expiresAt: lease.credential.expiresAt
            )
        case .expired(let lease, let expirationDate):
            recordExpiredCredentialIfNeeded(
                lease.credential,
                cacheExpirationDate: expirationDate,
                for: key,
                now: now
            )
        case .miss:
            break
        }

        let expiredAt: Date?
        switch expiredAtByKey.lookup(for: key, now: now) {
        case .hit(let value):
            expiredAt = value
        case .expired, .miss:
            expiredAt = nil
        }
        scheduleExpiration(now: now)
        return KubernetesExecCredentialCacheDiagnostic(
            state: expiredAt == nil ? .miss : .expired,
            expiresAt: expiredAt
        )
    }

    func retainedCount() -> Int {
        byKey.count
    }

    func invalidate(for key: String, generation: UUID) {
        let now = nowProvider()
        if inFlightByKey[key]?.resolvedLease?.generation == generation {
            inFlightByKey.removeValue(forKey: key)
        }
        switch byKey.lookup(for: key, now: now) {
        case .hit(let lease) where lease.generation == generation:
            byKey.removeValue(for: key)
            expiredAtByKey.removeValue(for: key)
            scheduleExpiration(now: now)
        case .expired(let lease, let expirationDate):
            recordExpiredCredentialIfNeeded(
                lease.credential,
                cacheExpirationDate: expirationDate,
                for: key,
                now: now
            )
            scheduleExpiration(now: now)
        case .hit, .miss:
            break
        }
    }

    private func purgeExpired() {
        let now = nowProvider()
        for entry in byKey.removeExpired(now: now) {
            recordExpiredCredentialIfNeeded(
                entry.value.credential,
                cacheExpirationDate: entry.expirationDate,
                for: entry.key,
                now: now
            )
        }
        _ = expiredAtByKey.removeExpired(now: now)
        scheduleExpiration(now: now)
    }

    private func recordExpiredCredentialIfNeeded(
        _ credential: KubernetesExecCredential,
        cacheExpirationDate: Date,
        for key: String,
        now: Date
    ) {
        guard let expiresAt = credential.expiresAt,
              expiresAt <= cacheExpirationDate else { return }
        recordExpired(expiresAt, for: key, now: now)
    }

    private func recordExpired(_ expirationDate: Date, for key: String, now: Date) {
        expiredAtByKey.insert(
            expirationDate,
            for: key,
            expirationDate: now.addingTimeInterval(Self.expiredDiagnosticTTL),
            now: now
        )
    }

    private func scheduleExpiration(now: Date) {
        expirationTask?.cancel()
        let nextExpiration = [
            byKey.earliestExpirationDate,
            expiredAtByKey.earliestExpirationDate
        ]
        .compactMap { $0 }
        .min()
        guard let nextExpiration else {
            expirationTask = nil
            return
        }
        let delay = max(0, nextExpiration.timeIntervalSince(now))
        expirationTask = Task { [weak self] in
            do {
                try await Task.sleep(for: .seconds(delay))
            } catch {
                return
            }
            guard !Task.isCancelled else { return }
            await self?.purgeExpired()
        }
    }
}

private struct ExecCredentialProcessOutput: Sendable, Equatable {
    let stdout: Data
    let stderr: Data
}

private final class ExecCredentialProcessExecutionState: @unchecked Sendable {
    private let lock = NSLock()
    private var process: Process?
    private var cancelled = false

    var isCancelled: Bool {
        lock.lock()
        defer { lock.unlock() }
        return cancelled
    }

    func register(_ process: Process) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !cancelled else { return false }
        self.process = process
        return true
    }

    func clear(_ process: Process) {
        lock.lock()
        defer { lock.unlock() }
        if self.process === process {
            self.process = nil
        }
    }

    func cancel() {
        lock.lock()
        cancelled = true
        let process = self.process
        lock.unlock()
        if process?.isRunning == true {
            process?.terminate()
        }
    }
}

private final class ExecCredentialProcessOutputCapture: @unchecked Sendable {
    private let lock = NSLock()
    private let maxBytesPerStream: Int
    private var stdout = Data()
    private var stderr = Data()

    init(maxBytesPerStream: Int) {
        self.maxBytesPerStream = max(0, maxBytesPerStream)
    }

    func appendStdout(_ data: Data) {
        append(data, to: &stdout)
    }

    func appendStderr(_ data: Data) {
        append(data, to: &stderr)
    }

    func snapshot() -> ExecCredentialProcessOutput {
        lock.lock()
        defer { lock.unlock() }
        return ExecCredentialProcessOutput(stdout: stdout, stderr: stderr)
    }

    private func append(_ data: Data, to stream: inout Data) {
        guard !data.isEmpty else { return }
        lock.lock()
        defer { lock.unlock() }
        let remaining = maxBytesPerStream - stream.count
        guard remaining > 0 else { return }
        stream.append(data.prefix(remaining))
    }
}

private struct KubernetesExecCredential: Sendable {
    let authentication: RESTAuthentication
    let clientTLSIdentity: ClientTLSIdentity?
    let expiresAt: Date?
}

private struct KubernetesExecCredentialLease: Sendable {
    let credential: KubernetesExecCredential
    let generation: UUID
}

private enum KubeConfigJSONValue: Decodable, Sendable, Equatable {
    case null
    case bool(Bool)
    case integer(Int64)
    case number(Double)
    case string(String)
    case array([KubeConfigJSONValue])
    case object([String: KubeConfigJSONValue])

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .bool(value)
        } else if let value = try? container.decode(Int64.self) {
            self = .integer(value)
        } else if let value = try? container.decode(Double.self) {
            self = .number(value)
        } else if let value = try? container.decode(String.self) {
            self = .string(value)
        } else if let value = try? container.decode([KubeConfigJSONValue].self) {
            self = .array(value)
        } else if let value = try? container.decode([String: KubeConfigJSONValue].self) {
            self = .object(value)
        } else {
            throw DecodingError.typeMismatch(
                KubeConfigJSONValue.self,
                .init(codingPath: decoder.codingPath, debugDescription: "Unsupported kubeconfig extension value")
            )
        }
    }

    var foundationValue: Any {
        switch self {
        case .null: return NSNull()
        case .bool(let value): return value
        case .integer(let value): return value
        case .number(let value): return value
        case .string(let value): return value
        case .array(let values): return values.map(\.foundationValue)
        case .object(let values): return values.mapValues(\.foundationValue)
        }
    }

    var stableDescription: String {
        guard JSONSerialization.isValidJSONObject(foundationValue),
              let data = try? JSONSerialization.data(withJSONObject: foundationValue, options: [.sortedKeys]) else {
            return String(describing: foundationValue)
        }
        return String(decoding: data, as: UTF8.self)
    }
}

private struct NormalizedKubeConfig: Decodable, Sendable {
    struct Input: Sendable {
        struct Source: Sendable {
            let url: URL
            let data: Data
        }

        let sources: [Source]
        let cacheIdentity: String
    }

    struct NamedContext: Decodable, Sendable {
        struct ContextEntry: Decodable, Sendable {
            let cluster: String
            let user: String
            let namespace: String?
        }

        let name: String
        let context: ContextEntry
    }

    struct NamedCluster: Decodable, Sendable {
        struct NamedExtension: Decodable, Sendable {
            let name: String
            let value: KubeConfigJSONValue

            enum CodingKeys: String, CodingKey {
                case name
                case value = "extension"
            }
        }

        struct ClusterEntry: Decodable, Sendable {
            let server: String
            let insecureSkipTLSVerify: Bool?
            let certificateAuthorityData: String?
            let tlsServerName: String?
            let certificateAuthority: String?
            let proxyURL: String?
            let disableCompression: Bool?
            let extensions: [NamedExtension]?

            init(
                server: String,
                insecureSkipTLSVerify: Bool? = nil,
                certificateAuthorityData: String? = nil,
                tlsServerName: String? = nil,
                certificateAuthority: String? = nil,
                proxyURL: String? = nil,
                disableCompression: Bool? = nil,
                extensions: [NamedExtension]? = nil
            ) {
                self.server = server
                self.insecureSkipTLSVerify = insecureSkipTLSVerify
                self.certificateAuthorityData = certificateAuthorityData
                self.tlsServerName = tlsServerName
                self.certificateAuthority = certificateAuthority
                self.proxyURL = proxyURL
                self.disableCompression = disableCompression
                self.extensions = extensions
            }

            enum CodingKeys: String, CodingKey {
                case server
                case insecureSkipTLSVerify = "insecure-skip-tls-verify"
                case certificateAuthorityData = "certificate-authority-data"
                case certificateAuthority = "certificate-authority"
                case tlsServerName = "tls-server-name"
                case proxyURL = "proxy-url"
                case disableCompression = "disable-compression"
                case extensions
            }

            var execExtensionConfig: KubeConfigJSONValue? {
                extensions?.first(where: { $0.name == "client.authentication.k8s.io/exec" })?.value
            }

            func resolvingRelativePaths(baseDirectory: URL) -> ClusterEntry {
                ClusterEntry(
                    server: server,
                    insecureSkipTLSVerify: insecureSkipTLSVerify,
                    certificateAuthorityData: certificateAuthorityData,
                    tlsServerName: tlsServerName,
                    certificateAuthority: Self.resolvedDataPath(certificateAuthority, baseDirectory: baseDirectory),
                    proxyURL: proxyURL,
                    disableCompression: disableCompression,
                    extensions: extensions
                )
            }

            private static func resolvedDataPath(_ path: String?, baseDirectory: URL) -> String? {
                guard let path, !path.isEmpty else { return path }
                let expanded = NSString(string: path).expandingTildeInPath
                guard !expanded.hasPrefix("/") else { return expanded }
                return baseDirectory.appendingPathComponent(expanded).standardizedFileURL.path
            }

            func resolvedCertificateAuthorityData() throws -> Data? {
                if let certificateAuthorityData {
                    return Data(base64Encoded: certificateAuthorityData, options: .ignoreUnknownCharacters)
                }
                guard let certificateAuthority, !certificateAuthority.isEmpty else {
                    return nil
                }
                return try Data(contentsOf: URL(fileURLWithPath: NSString(string: certificateAuthority).expandingTildeInPath))
            }
        }

        let name: String
        let cluster: ClusterEntry
    }

    struct NamedUser: Decodable, Sendable {
        struct UserEntry: Decodable, Sendable {
            struct AuthProviderConfig: Decodable, Sendable {
                let name: String
                let config: [String: String]?
            }

            struct ExecConfig: Decodable, Sendable {
                struct EnvironmentEntry: Decodable, Sendable {
                    let name: String
                    let value: String
                }

                let apiVersion: String?
                let command: String
                let args: [String]?
                let env: [EnvironmentEntry]?
                let installHint: String?
                let provideClusterInfo: Bool?
                let interactiveMode: String?

                init(
                    apiVersion: String?,
                    command: String,
                    args: [String]? = nil,
                    env: [EnvironmentEntry]? = nil,
                    installHint: String? = nil,
                    provideClusterInfo: Bool? = nil,
                    interactiveMode: String? = nil
                ) {
                    self.apiVersion = apiVersion
                    self.command = command
                    self.args = args
                    self.env = env
                    self.installHint = installHint
                    self.provideClusterInfo = provideClusterInfo
                    self.interactiveMode = interactiveMode
                }

                enum CodingKeys: String, CodingKey {
                    case apiVersion
                    case command
                    case args
                    case env
                    case installHint
                    case provideClusterInfo = "provideClusterInfo"
                    case interactiveMode
                }

                func resolvingCommand(baseDirectory: URL) -> ExecConfig {
                    let expanded = NSString(string: command).expandingTildeInPath
                    let resolved: String
                    if expanded.contains("/"), !expanded.hasPrefix("/") {
                        resolved = baseDirectory.appendingPathComponent(expanded).standardizedFileURL.path
                    } else {
                        resolved = expanded
                    }
                    return ExecConfig(
                        apiVersion: apiVersion,
                        command: resolved,
                        args: args,
                        env: env,
                        installHint: installHint,
                        provideClusterInfo: provideClusterInfo,
                        interactiveMode: interactiveMode
                    )
                }

                func processEnvironment(base: [String: String], execInfo: String?) -> [String: String] {
                    let pairs = KubernetesSensitiveCacheKey.effectiveEnvironmentPairs(
                        processEnvironment: ProcessInfo.processInfo.environment,
                        baseEnvironment: base,
                        execEnvironment: (env ?? []).map { ($0.name, $0.value) }
                    )
                    var output = Dictionary(uniqueKeysWithValues: pairs)
                    if let execInfo {
                        output["KUBERNETES_EXEC_INFO"] = execInfo
                    }
                    return output
                }

                func cacheKey(processEnvironment: [String: String], execInfo: String) -> String {
                    let environmentPairs: [(String, String)] = processEnvironment.map { entry in
                        (entry.key, entry.value)
                    }
                    let effectiveEnvironment = environmentPairs.sorted { lhs, rhs in
                        lhs.0 == rhs.0 ? lhs.1 < rhs.1 : lhs.0 < rhs.0
                    }
                    let components: [KubernetesSensitiveCacheKeyComponent] = [
                        .string(apiVersion),
                        .string(command),
                        .strings(args ?? []),
                        .keyValuePairs(effectiveEnvironment),
                        .string(installHint),
                        .string(provideClusterInfo == true ? "cluster" : nil),
                        .string(interactiveMode),
                        .string(execInfo)
                    ]
                    return KubernetesSensitiveCacheKey.make(
                        namespace: "exec",
                        components: components
                    )
                }
            }

            let token: String?
            let tokenFile: String?
            let username: String?
            let password: String?
            let authProvider: AuthProviderConfig?
            let exec: ExecConfig?
            let clientCertificateData: String?
            let clientKeyData: String?
            let clientCertificate: String?
            let clientKey: String?

            init(
                token: String? = nil,
                tokenFile: String? = nil,
                username: String? = nil,
                password: String? = nil,
                authProvider: AuthProviderConfig? = nil,
                exec: ExecConfig? = nil,
                clientCertificateData: String? = nil,
                clientKeyData: String? = nil,
                clientCertificate: String? = nil,
                clientKey: String? = nil
            ) {
                self.token = token
                self.tokenFile = tokenFile
                self.username = username
                self.password = password
                self.authProvider = authProvider
                self.exec = exec
                self.clientCertificateData = clientCertificateData
                self.clientKeyData = clientKeyData
                self.clientCertificate = clientCertificate
                self.clientKey = clientKey
            }

            enum CodingKeys: String, CodingKey {
                case token
                case tokenFile = "tokenFile"
                case username
                case password
                case authProvider = "auth-provider"
                case exec
                case clientCertificateData = "client-certificate-data"
                case clientKeyData = "client-key-data"
                case clientCertificate = "client-certificate"
                case clientKey = "client-key"
            }

            func resolvingRelativePaths(baseDirectory: URL) -> UserEntry {
                UserEntry(
                    token: token,
                    tokenFile: Self.resolvedDataPath(tokenFile, baseDirectory: baseDirectory),
                    username: username,
                    password: password,
                    authProvider: authProvider,
                    exec: exec?.resolvingCommand(baseDirectory: baseDirectory),
                    clientCertificateData: clientCertificateData,
                    clientKeyData: clientKeyData,
                    clientCertificate: Self.resolvedDataPath(clientCertificate, baseDirectory: baseDirectory),
                    clientKey: Self.resolvedDataPath(clientKey, baseDirectory: baseDirectory)
                )
            }

            private static func resolvedDataPath(_ path: String?, baseDirectory: URL) -> String? {
                guard let path, !path.isEmpty else { return path }
                let expanded = NSString(string: path).expandingTildeInPath
                guard !expanded.hasPrefix("/") else { return expanded }
                return baseDirectory.appendingPathComponent(expanded).standardizedFileURL.path
            }

            func resolvedClientTLSIdentityIfAvailable() throws -> ClientTLSIdentity? {
                guard let certificateMaterial = try resolvedClientCertificateData() else {
                    return nil
                }
                guard let certificateDER = certificateDERBlocks(from: certificateMaterial).first,
                      let certificate = SecCertificateCreateWithData(nil, certificateDER as CFData) else {
                    throw RuneError.invalidInput(message: "Client certificate data in kubeconfig could not be parsed")
                }
                var identity: SecIdentity?
                let status = SecIdentityCreateWithCertificate(nil, certificate, &identity)
                if status == errSecSuccess, let identity {
                    return ClientTLSIdentity(identity: identity)
                }

                guard let keyData = try resolvedClientKeyData() else {
                    return nil
                }
                return try ClientTLSIdentity.temporaryIdentity(certificateData: certificateMaterial, keyData: keyData)
            }

            private func resolvedClientCertificateData() throws -> Data? {
                if let clientCertificateData,
                   let decoded = Data(base64Encoded: clientCertificateData, options: .ignoreUnknownCharacters) {
                    return decoded
                }
                guard let clientCertificate, !clientCertificate.isEmpty else {
                    return nil
                }
                return try Data(contentsOf: URL(fileURLWithPath: NSString(string: clientCertificate).expandingTildeInPath))
            }

            private func resolvedClientKeyData() throws -> Data? {
                if let clientKeyData,
                   let decoded = Data(base64Encoded: clientKeyData, options: .ignoreUnknownCharacters) {
                    return decoded
                }
                guard let clientKey, !clientKey.isEmpty else {
                    return nil
                }
                return try Data(contentsOf: URL(fileURLWithPath: NSString(string: clientKey).expandingTildeInPath))
            }
        }

        let name: String
        let user: UserEntry
    }

    let currentContext: String?
    let contexts: [NamedContext]
    let clusters: [NamedCluster]
    let users: [NamedUser]

    init(
        currentContext: String?,
        contexts: [NamedContext],
        clusters: [NamedCluster],
        users: [NamedUser]
    ) {
        self.currentContext = currentContext
        self.contexts = contexts
        self.clusters = clusters
        self.users = users
    }

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

    func resolvingRelativePaths(baseDirectory: URL) -> NormalizedKubeConfig {
        NormalizedKubeConfig(
            currentContext: currentContext,
            contexts: contexts,
            clusters: clusters.map { named in
                NamedCluster(
                    name: named.name,
                    cluster: named.cluster.resolvingRelativePaths(baseDirectory: baseDirectory)
                )
            },
            users: users.map { named in
                NamedUser(
                    name: named.name,
                    user: named.user.resolvingRelativePaths(baseDirectory: baseDirectory)
                )
            }
        )
    }

    static func readInput(environment: [String: String]) throws -> Input {
        let paths = kubeconfigPaths(environment: environment)
        guard !paths.isEmpty else { throw RuneError.missingKubeConfig }

        var sources: [Input.Source] = []
        var identityComponents: [KubernetesSensitiveCacheKeyComponent] = []
        for path in paths {
            let expanded = NSString(string: path).expandingTildeInPath
            let url = URL(fileURLWithPath: expanded).standardizedFileURL
            identityComponents.append(.string(url.path))
            guard FileManager.default.fileExists(atPath: url.path) else {
                identityComponents.append(.data(nil))
                continue
            }
            let data = try Data(contentsOf: url)
            identityComponents.append(.data(data))
            sources.append(Input.Source(url: url, data: data))
        }
        return Input(
            sources: sources,
            cacheIdentity: KubernetesSensitiveCacheKey.make(
                namespace: "config",
                components: identityComponents
            )
        )
    }

    static func loadDirectly(input: Input) throws -> NormalizedKubeConfig {
        var mergedContexts: [NamedContext] = []
        var mergedClusters: [NamedCluster] = []
        var mergedUsers: [NamedUser] = []
        var currentContext: String?

        for source in input.sources {
            guard let raw = String(data: source.data, encoding: .utf8) else {
                throw RuneError.parseError(
                    message: "Could not parse kubeconfig \(source.url.lastPathComponent): invalid UTF-8"
                )
            }
            let baseDirectory = source.url.deletingLastPathComponent()
            let config: NormalizedKubeConfig
            do {
                config = try YAMLDecoder()
                    .decode(NormalizedKubeConfig.self, from: raw)
                    .resolvingRelativePaths(baseDirectory: baseDirectory)
            } catch {
                throw RuneError.parseError(
                    message: "Could not parse kubeconfig \(source.url.lastPathComponent): \(String(describing: error))"
                )
            }
            mergedContexts.append(contentsOf: config.contexts)
            mergedClusters.append(contentsOf: config.clusters)
            mergedUsers.append(contentsOf: config.users)
            if currentContext == nil, let current = config.currentContext, !current.isEmpty {
                currentContext = current
            }
        }

        guard !mergedContexts.isEmpty || !mergedClusters.isEmpty else {
            throw RuneError.missingKubeConfig
        }

        return NormalizedKubeConfig(
            currentContext: currentContext,
            contexts: deduplicateByName(mergedContexts, name: \.name),
            clusters: deduplicateByName(mergedClusters, name: \.name),
            users: deduplicateByName(mergedUsers, name: \.name)
        )
    }

    static func unresolvedCacheIdentity(environment: [String: String]) -> String {
        KubernetesSensitiveCacheKey.make(
            namespace: "unresolved-config",
            components: [
                .strings(
                    kubeconfigPaths(environment: environment).map {
                        URL(
                            fileURLWithPath: NSString(string: $0).expandingTildeInPath
                        )
                        .standardizedFileURL
                        .path
                    }
                )
            ]
        )
    }

    private static func kubeconfigPaths(environment: [String: String]) -> [String] {
        if let kubeconfig = environment["KUBECONFIG"], !kubeconfig.isEmpty {
            return kubeconfig.split(separator: ":").map(String.init).filter { !$0.isEmpty }
        }
        return ["~/.kube/config"]
    }

    private static func deduplicateByName<T>(_ values: [T], name: (T) -> String) -> [T] {
        var seen = Set<String>()
        var output: [T] = []
        for value in values where seen.insert(name(value)).inserted {
            output.append(value)
        }
        return output
    }
}

public struct KubernetesManifestIdentity: Equatable, Sendable {
    public let apiVersion: String
    public let kind: KubeResourceKind
    public let namespace: String
    public let name: String

    public static func parse(
        yaml: String,
        defaultNamespace: String
    ) throws -> KubernetesManifestIdentity {
        let document: ManifestDocument
        do {
            document = try YAMLDecoder().decode(ManifestDocument.self, from: yaml)
        } catch {
            throw RuneError.parseError(
                message: "Could not parse YAML manifest: \(String(describing: error))"
            )
        }

        guard let apiVersion = document.apiVersion, !apiVersion.isEmpty else {
            throw RuneError.parseError(message: "YAML manifest is missing apiVersion")
        }
        guard let rawKind = document.kind,
              let kind = KubeResourceKind(manifestKind: rawKind) else {
            throw RuneError.parseError(message: "YAML manifest kind is not supported by Rune")
        }
        guard let name = document.metadata?.name, !name.isEmpty else {
            throw RuneError.parseError(message: "YAML manifest is missing metadata.name")
        }
        let namespace = document.metadata?.namespace
        return KubernetesManifestIdentity(
            apiVersion: apiVersion,
            kind: kind,
            namespace: kind.isNamespaced ? (namespace?.isEmpty == false ? namespace! : defaultNamespace) : "",
            name: name
        )
    }

    private struct ManifestDocument: Decodable {
        let apiVersion: String?
        let kind: String?
        let metadata: Metadata?

        struct Metadata: Decodable {
            let name: String?
            let namespace: String?
        }
    }
}

private extension KubeResourceKind {
    init?(manifestKind: String) {
        switch manifestKind.lowercased() {
        case "pod": self = .pod
        case "deployment": self = .deployment
        case "statefulset": self = .statefulSet
        case "daemonset": self = .daemonSet
        case "job": self = .job
        case "cronjob": self = .cronJob
        case "replicaset": self = .replicaSet
        case "service": self = .service
        case "endpoints": self = .endpoint
        case "ingress": self = .ingress
        case "configmap": self = .configMap
        case "secret": self = .secret
        case "node": self = .node
        case "event": self = .event
        case "serviceaccount": self = .serviceAccount
        case "role": self = .role
        case "rolebinding": self = .roleBinding
        case "clusterrole": self = .clusterRole
        case "clusterrolebinding": self = .clusterRoleBinding
        case "persistentvolumeclaim": self = .persistentVolumeClaim
        case "persistentvolume": self = .persistentVolume
        case "storageclass": self = .storageClass
        case "horizontalpodautoscaler": self = .horizontalPodAutoscaler
        case "networkpolicy": self = .networkPolicy
        default: return nil
        }
    }
}

private enum KubernetesJSON {
    struct RolloutRevision {
        let revision: Int
        let name: String
        let changeCause: String?
    }

    struct RollbackTemplate {
        let revision: Int
        let template: [String: Any]
    }

    struct DeploymentRolloutSnapshot {
        let deploymentName: String
        let desiredReplicas: Int
        let readyReplicas: Int
        let updatedReplicas: Int
        let availableReplicas: Int
        let observedGeneration: Int?
        let generation: Int?
        let progressFailureMessage: String?

        var isReady: Bool {
            if desiredReplicas == 0 { return true }
            if let observedGeneration, let generation, observedGeneration < generation {
                return false
            }
            return readyReplicas >= desiredReplicas
                && updatedReplicas >= desiredReplicas
                && availableReplicas >= desiredReplicas
        }

        func result(
            status: DeploymentRolloutVerificationStatus,
            message: String
        ) -> DeploymentRolloutVerificationResult {
            DeploymentRolloutVerificationResult(
                status: status,
                desiredReplicas: desiredReplicas,
                readyReplicas: readyReplicas,
                updatedReplicas: updatedReplicas,
                availableReplicas: availableReplicas,
                message: message
            )
        }
    }

    static func selectorMatchLabels(from raw: String) throws -> [String: String] {
        let object = try objectDictionary(from: raw)
        guard
            let spec = object["spec"] as? [String: Any],
            let selector = spec["selector"] as? [String: Any],
            let labels = selector["matchLabels"] as? [String: Any]
        else { return [:] }
        return labels.compactMapValues { $0 as? String }
    }

    static func deploymentRevision(from raw: String) throws -> Int? {
        let object = try objectDictionary(from: raw)
        let metadata = object["metadata"] as? [String: Any]
        let annotations = metadata?["annotations"] as? [String: Any]
        return (annotations?["deployment.kubernetes.io/revision"] as? String).flatMap(Int.init)
    }

    static func deploymentRolloutSnapshot(from raw: String, deploymentName fallbackName: String) throws -> DeploymentRolloutSnapshot {
        let object = try objectDictionary(from: raw)
        let metadata = object["metadata"] as? [String: Any]
        let spec = object["spec"] as? [String: Any]
        let status = object["status"] as? [String: Any]
        let conditions = status?["conditions"] as? [[String: Any]] ?? []
        let progressFailure = conditions.first { condition in
            (condition["type"] as? String) == "Progressing"
                && (condition["status"] as? String) == "False"
                && (condition["reason"] as? String) == "ProgressDeadlineExceeded"
        }.flatMap { condition -> String? in
            let message = condition["message"] as? String
            return message?.isEmpty == false ? message : "Deployment \(fallbackName) exceeded its progress deadline."
        }

        return DeploymentRolloutSnapshot(
            deploymentName: metadata?["name"] as? String ?? fallbackName,
            desiredReplicas: spec?["replicas"] as? Int ?? status?["replicas"] as? Int ?? 0,
            readyReplicas: status?["readyReplicas"] as? Int ?? 0,
            updatedReplicas: status?["updatedReplicas"] as? Int ?? 0,
            availableReplicas: status?["availableReplicas"] as? Int ?? 0,
            observedGeneration: status?["observedGeneration"] as? Int,
            generation: metadata?["generation"] as? Int,
            progressFailureMessage: progressFailure
        )
    }

    static func replicaSetRolloutRevisions(from raw: String) throws -> [RolloutRevision] {
        let items = try listItems(from: raw)
        return items.compactMap { item in
            guard
                let metadata = item["metadata"] as? [String: Any],
                let name = metadata["name"] as? String,
                let annotations = metadata["annotations"] as? [String: Any],
                let revision = (annotations["deployment.kubernetes.io/revision"] as? String).flatMap(Int.init)
            else { return nil }
            return RolloutRevision(
                revision: revision,
                name: name,
                changeCause: annotations["kubernetes.io/change-cause"] as? String
            )
        }
        .sorted { $0.revision < $1.revision }
    }

    static func replicaSetTemplates(from raw: String) throws -> [RollbackTemplate] {
        let items = try listItems(from: raw)
        return items.compactMap { item in
            guard
                let metadata = item["metadata"] as? [String: Any],
                let annotations = metadata["annotations"] as? [String: Any],
                let revision = (annotations["deployment.kubernetes.io/revision"] as? String).flatMap(Int.init),
                let spec = item["spec"] as? [String: Any],
                let template = spec["template"] as? [String: Any]
            else { return nil }
            return RollbackTemplate(revision: revision, template: sanitizedPodTemplate(template))
        }
    }

    static func describeEvents(from raw: String) throws -> String {
        try listItems(from: raw).compactMap { item -> String? in
            guard let metadata = item["metadata"] as? [String: Any] else { return nil }
            let type = item["type"] as? String ?? ""
            let reason = item["reason"] as? String ?? ""
            let message = item["message"] as? String ?? ""
            let time = item["lastTimestamp"] as? String
                ?? item["eventTime"] as? String
                ?? item["firstTimestamp"] as? String
                ?? metadata["creationTimestamp"] as? String
                ?? ""
            return [time, type, reason, message].filter { !$0.isEmpty }.joined(separator: "\t")
        }.joined(separator: "\n")
    }

    private static func objectDictionary(from raw: String) throws -> [String: Any] {
        guard let object = try JSONSerialization.jsonObject(with: Data(raw.utf8)) as? [String: Any] else {
            throw RuneError.parseError(message: "Kubernetes JSON object could not be parsed")
        }
        return object
    }

    private static func listItems(from raw: String) throws -> [[String: Any]] {
        let object = try objectDictionary(from: raw)
        return object["items"] as? [[String: Any]] ?? []
    }

    private static func sanitizedPodTemplate(_ template: [String: Any]) -> [String: Any] {
        var output = template
        if var metadata = output["metadata"] as? [String: Any] {
            metadata.removeValue(forKey: "creationTimestamp")
            metadata.removeValue(forKey: "resourceVersion")
            metadata.removeValue(forKey: "uid")
            metadata.removeValue(forKey: "managedFields")
            output["metadata"] = metadata
        }
        return output
    }
}

private struct ExecCredentialResponse: Decodable, Sendable {
    struct Status: Decodable, Sendable {
        let token: String?
        let expirationTimestamp: String?
        let clientCertificateData: String?
        let clientKeyData: String?

        var decodedClientCertificateData: Data? {
            clientCertificateData.map { Data($0.utf8) }
        }

        var decodedClientKeyData: Data? {
            clientKeyData.map { Data($0.utf8) }
        }

        enum CodingKeys: String, CodingKey {
            case token
            case expirationTimestamp
            case clientCertificateData = "clientCertificateData"
            case clientKeyData = "clientKeyData"
        }
    }

    let apiVersion: String?
    let kind: String?
    let status: Status?
}

private struct ResolvedRESTContext {
    let serverURL: URL
    let namespace: String?
    let metricsScopeIdentity: String
    let metricsScope: KubernetesRESTRequestMetricsScopeToken?
    let authentication: RESTAuthentication
    let insecureSkipTLSVerify: Bool
    let certificateAuthorityData: Data?
    let tlsServerName: String?
    let clientTLSIdentity: ClientTLSIdentity?
    let credentialInvalidationKey: RESTCredentialInvalidationKey?

    var requiresLegacySPDYPortForward: Bool {
        serverURL.scheme == "http"
            || insecureSkipTLSVerify
            || certificateAuthorityData != nil
            || tlsServerName != nil
            || clientTLSIdentity != nil
    }
}

private struct NormalizedKubeConfigSnapshot {
    let config: NormalizedKubeConfig
    let cacheIdentity: String
}

private enum RESTCredentialInvalidationKey: Sendable {
    case exec(String, generation: UUID)
    case native(String, revision: UUID)
}

private enum RESTAuthentication: Sendable {
    case none
    case bearer(String)
    case basic(username: String, password: String)

    var traceDescription: String {
        switch self {
        case .none:
            return "none"
        case .bearer:
            return "bearer"
        case .basic:
            return "basic"
        }
    }
}

private enum RESTCredentialFingerprint {
    static func make(
        authentication: RESTAuthentication,
        clientTLSIdentity: ClientTLSIdentity?
    ) -> Data? {
        var material = Data("rune-rest-credential-v1".utf8)

        switch authentication {
        case .none:
            append(tag: 0x01, value: Data(), to: &material)
        case .bearer(let token):
            append(tag: 0x02, value: Data(token.utf8), to: &material)
        case .basic(let username, let password):
            append(tag: 0x03, value: Data(username.utf8), to: &material)
            append(tag: 0x04, value: Data(password.utf8), to: &material)
        }

        if let clientTLSIdentity {
            guard let certificateData = clientTLSIdentity.certificateData else {
                return nil
            }
            append(tag: 0x05, value: certificateData, to: &material)
        } else {
            append(tag: 0x06, value: Data(), to: &material)
        }

        return Data(SHA256.hash(data: material))
    }

    private static func append(tag: UInt8, value: Data, to material: inout Data) {
        material.append(tag)
        var length = UInt64(value.count).bigEndian
        withUnsafeBytes(of: &length) { material.append(contentsOf: $0) }
        material.append(value)
    }
}

private struct RESTCredentialResolution: Sendable {
    let authentication: RESTAuthentication
    let clientTLSIdentity: ClientTLSIdentity?
    let credentialInvalidationKey: RESTCredentialInvalidationKey?

    init(
        authentication: RESTAuthentication,
        clientTLSIdentity: ClientTLSIdentity?,
        credentialInvalidationKey: RESTCredentialInvalidationKey? = nil
    ) {
        self.authentication = authentication
        self.clientTLSIdentity = clientTLSIdentity
        self.credentialInvalidationKey = credentialInvalidationKey
    }
}

struct KubernetesRESTRequestCoalescingKey: Hashable, Sendable {
    let method: String
    let server: String
    let contextName: String
    let scopeIdentity: String
    let credentialFingerprint: Data
    let apiPath: String
    let headers: [String: String]
    let timeout: TimeInterval

    init(
        method: String,
        server: String,
        contextName: String,
        scopeIdentity: String,
        credentialFingerprint: Data,
        apiPath: String,
        headers: [String: String],
        timeout: TimeInterval
    ) {
        self.method = method.uppercased()
        self.server = server
        self.contextName = contextName
        self.scopeIdentity = scopeIdentity
        self.credentialFingerprint = credentialFingerprint
        self.apiPath = apiPath
        self.headers = headers
        self.timeout = timeout
    }

    static func isCoalescible(method: String, body: String?) -> Bool {
        guard body == nil else { return false }
        switch method.uppercased() {
        case "GET", "HEAD":
            return true
        default:
            return false
        }
    }
}

actor KubernetesRESTRequestCoalescer {
    private struct Entry {
        var task: Task<RESTResponse, Error>
        var waiters: Set<UUID>
    }

    private var inFlight: [KubernetesRESTRequestCoalescingKey: Entry] = [:]

    func value(
        for key: KubernetesRESTRequestCoalescingKey,
        operation: @escaping @Sendable () async throws -> RESTResponse
    ) async throws -> RESTResponse {
        let waiterID = UUID()
        let task: Task<RESTResponse, Error>

        if var existing = inFlight[key] {
            existing.waiters.insert(waiterID)
            inFlight[key] = existing
            task = existing.task
        } else {
            let newTask = Task {
                try await operation()
            }
            inFlight[key] = Entry(task: newTask, waiters: [waiterID])
            task = newTask
        }

        do {
            let value = try await withTaskCancellationHandler {
                try await task.value
            } onCancel: {
                Task {
                    await self.cancelWaiter(waiterID, for: key)
                }
            }
            finishWaiter(waiterID, for: key)
            return value
        } catch {
            finishWaiter(waiterID, for: key)
            throw error
        }
    }

    private func cancelWaiter(_ waiterID: UUID, for key: KubernetesRESTRequestCoalescingKey) {
        guard var entry = inFlight[key] else { return }
        entry.waiters.remove(waiterID)
        if entry.waiters.isEmpty {
            entry.task.cancel()
            inFlight[key] = nil
        } else {
            inFlight[key] = entry
        }
    }

    private func finishWaiter(_ waiterID: UUID, for key: KubernetesRESTRequestCoalescingKey) {
        guard var entry = inFlight[key] else { return }
        entry.waiters.remove(waiterID)
        if entry.waiters.isEmpty || entry.task.isCancelled {
            inFlight[key] = nil
        } else {
            inFlight[key] = entry
        }
    }
}

struct RESTResponse: Sendable {
    let body: String
    let contentType: String
}

enum KubernetesRESTErrorMessageFormatter {
    static func httpErrorMessage(statusCode: Int, responseBody: String) -> String {
        let trimmedBody = responseBody.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedBody.isEmpty else {
            return "HTTP \(statusCode)"
        }

        if let data = trimmedBody.data(using: .utf8),
           let status = try? JSONDecoder().decode(KubernetesStatusPayload.self, from: data),
           status.kind == "Status",
           let statusMessage = status.message?.trimmingCharacters(in: .whitespacesAndNewlines),
           !statusMessage.isEmpty {
            return "HTTP \(statusCode): \(statusMessage)"
        }

        return "HTTP \(statusCode): \(trimmedBody)"
    }

    static func appendingRetryAdvice(
        to message: String,
        method: String,
        decision: KubernetesRequestRetryDecision
    ) -> String {
        guard decision.isRetryable,
              KubernetesRequestRetryPolicy.isSafeRetryMethod(method)
        else {
            return message
        }

        let delayDescription = decision.suggestedDelayNanoseconds.map {
            " after \($0 / 1_000_000) ms"
        } ?? ""
        return "\(message) | Temporary Kubernetes API error. You can safely retry this read\(delayDescription)."
    }

    private struct KubernetesStatusPayload: Decodable {
        let kind: String?
        let message: String?
    }
}

private struct PortOwner {
    let pid: String
    let command: String

    static func parseLsofFieldOutput(_ output: String) -> PortOwner? {
        var currentPID: String?
        var currentCommand: String?

        for line in output.split(separator: "\n", omittingEmptySubsequences: true).map(String.init) {
            guard let field = line.first else { continue }
            let value = String(line.dropFirst()).trimmingCharacters(in: .whitespacesAndNewlines)
            switch field {
            case "p":
                currentPID = value
                currentCommand = nil
            case "c":
                currentCommand = value
            case "t":
                if value == "IPv4" || value == "IPv6",
                   let pid = currentPID,
                   let command = currentCommand,
                   !pid.isEmpty,
                   !command.isEmpty {
                    return PortOwner(pid: pid, command: command)
                }
            default:
                continue
            }
        }

        if let pid = currentPID, let command = currentCommand, !pid.isEmpty, !command.isEmpty {
            return PortOwner(pid: pid, command: command)
        }
        return nil
    }
}

private struct RESTURLSession {
    let session: URLSession
    let delegate: RESTURLSessionDelegate
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

private final class ClientTLSIdentity: @unchecked Sendable {
    let identity: SecIdentity
    private let keychain: SecKeychain?
    private let keychainPath: String?

    init(identity: SecIdentity, keychain: SecKeychain? = nil, keychainPath: String? = nil) {
        self.identity = identity
        self.keychain = keychain
        self.keychainPath = keychainPath
    }

    deinit {
        guard let keychainPath else { return }
        try? FileManager.default.removeItem(atPath: keychainPath)
    }

    var certificateData: Data? {
        var certificate: SecCertificate?
        guard SecIdentityCopyCertificate(identity, &certificate) == errSecSuccess,
              let certificate else {
            return nil
        }
        return SecCertificateCopyData(certificate) as Data
    }

    static func temporaryIdentity(certificateData: Data, keyData: Data) throws -> ClientTLSIdentity? {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-kube-mtls", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let keychainURL = directory.appendingPathComponent("\(UUID().uuidString).keychain-db")
        let password = UUID().uuidString

        var keychain: SecKeychain?
        let createStatus = password.withCString { passwordPointer in
            keychainURL.path.withCString { pathPointer in
                RuneSecKeychainCreate(
                    pathPointer,
                    UInt32(strlen(passwordPointer)),
                    passwordPointer,
                    false,
                    nil,
                    &keychain
                )
            }
        }
        guard createStatus == errSecSuccess, let keychain else {
            throw RuneError.invalidInput(message: "Could not create temporary keychain for Kubernetes client certificate auth: OSStatus \(createStatus)")
        }

        do {
            _ = try importSecurityItems(certificateData, into: keychain)
            _ = try importSecurityItems(keyData, into: keychain)
            guard let certificateDER = certificateDERBlocks(from: certificateData).first,
                  let certificate = SecCertificateCreateWithData(nil, certificateDER as CFData) else {
                throw RuneError.invalidInput(message: "Client certificate data in kubeconfig could not be parsed")
            }
            var identity: SecIdentity?
            let identityStatus = SecIdentityCreateWithCertificate(keychain, certificate, &identity)
            guard identityStatus == errSecSuccess, let identity else {
                throw RuneError.invalidInput(
                    message: "Client certificate and key in kubeconfig could not be paired into a TLS identity: OSStatus \(identityStatus)"
                )
            }
            return ClientTLSIdentity(identity: identity, keychain: keychain, keychainPath: keychainURL.path)
        } catch {
            try? FileManager.default.removeItem(at: keychainURL)
            throw error
        }
    }

    private static func importSecurityItems(_ data: Data, into keychain: SecKeychain) throws -> [AnyObject] {
        var format = SecExternalFormat.formatUnknown
        var itemType = SecExternalItemType.itemTypeUnknown
        var items: CFArray?
        var parameters = SecItemImportExportKeyParameters()
        parameters.version = UInt32(SEC_KEY_IMPORT_EXPORT_PARAMS_VERSION)
        parameters.flags = SecKeyImportExportFlags(rawValue: 0)
        let status = SecItemImport(
            data as CFData,
            nil,
            &format,
            &itemType,
            SecItemImportExportFlags(rawValue: 0),
            &parameters,
            keychain,
            &items
        )
        guard status == errSecSuccess else {
            throw RuneError.invalidInput(message: "Could not import Kubernetes client TLS material: OSStatus \(status)")
        }
        return (items as? [AnyObject]) ?? []
    }
}

private final class RESTURLSessionDelegate: NSObject, URLSessionDelegate, URLSessionTaskDelegate {
    private let insecureSkipTLSVerify: Bool
    private let certificateAuthorityData: Data?
    private let tlsServerName: String?
    private let clientTLSIdentity: ClientTLSIdentity?
    private let tlsFailureState = TLSFailureState()

    init(
        insecureSkipTLSVerify: Bool,
        certificateAuthorityData: Data?,
        tlsServerName: String?,
        clientTLSIdentity: ClientTLSIdentity?
    ) {
        self.insecureSkipTLSVerify = insecureSkipTLSVerify
        self.certificateAuthorityData = certificateAuthorityData
        self.tlsServerName = tlsServerName
        self.clientTLSIdentity = clientTLSIdentity
    }

    func lastTLSFailure() -> String? {
        tlsFailureState.value()
    }

    func urlSession(
        _ session: URLSession,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        handle(challenge: challenge, completionHandler: completionHandler)
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        handle(challenge: challenge, completionHandler: completionHandler)
    }

    private func handle(
        challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        VerboseKubeTrace.append(
            "k8s.tls",
            "challenge method=\(challenge.protectionSpace.authenticationMethod) host=\(challenge.protectionSpace.host) previousFailures=\(challenge.previousFailureCount) hasClientIdentity=\(clientTLSIdentity != nil) caConfigured=\(certificateAuthorityData != nil) insecureSkipTLSVerify=\(insecureSkipTLSVerify)"
        )
        if challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodClientCertificate {
            guard let clientIdentity = clientTLSIdentity?.identity else {
                VerboseKubeTrace.append(
                    "k8s.tls",
                    "client-certificate challenge default-handling host=\(challenge.protectionSpace.host) reason=no-client-identity"
                )
                completionHandler(.performDefaultHandling, nil)
                return
            }
            VerboseKubeTrace.append(
                "k8s.tls",
                "client-certificate challenge use-credential host=\(challenge.protectionSpace.host)"
            )
            completionHandler(
                .useCredential,
                URLCredential(identity: clientIdentity, certificates: nil, persistence: .forSession)
            )
            return
        }

        guard challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodServerTrust,
              let trust = challenge.protectionSpace.serverTrust else {
            completionHandler(.performDefaultHandling, nil)
            return
        }

        if insecureSkipTLSVerify {
            clearTLSFailure()
            VerboseKubeTrace.append(
                "k8s.tls",
                "server-trust accepted host=\(challenge.protectionSpace.host) mode=insecure-skip-tls-verify"
            )
            completionHandler(.useCredential, URLCredential(trust: trust))
            return
        }

        guard certificateAuthorityData != nil || tlsServerName != nil else {
            VerboseKubeTrace.append(
                "k8s.tls",
                "server-trust default-handling host=\(challenge.protectionSpace.host) mode=system-trust"
            )
            completionHandler(.performDefaultHandling, nil)
            return
        }

        let serverName = tlsServerName ?? challenge.protectionSpace.host
        let policy = SecPolicyCreateSSL(true, serverName as CFString)
        SecTrustSetPolicies(trust, policy)

        if let certificateAuthorityData {
            let certificates = certificates(from: certificateAuthorityData)
            guard !certificates.isEmpty else {
                recordTLSFailure("kubeconfig certificate-authority-data was present but did not contain a parseable certificate")
                VerboseKubeTrace.append(
                    "k8s.tls",
                    "server-trust rejected host=\(challenge.protectionSpace.host) reason=unparseable-kubeconfig-ca"
                )
                completionHandler(.cancelAuthenticationChallenge, nil)
                return
            }
            SecTrustSetAnchorCertificates(trust, certificates as CFArray)
            SecTrustSetAnchorCertificatesOnly(trust, true)
        }

        var error: CFError?
        if SecTrustEvaluateWithError(trust, &error) {
            clearTLSFailure()
            VerboseKubeTrace.append(
                "k8s.tls",
                "server-trust accepted host=\(challenge.protectionSpace.host) serverName=\(serverName) mode=kubeconfig-ca"
            )
            completionHandler(.useCredential, URLCredential(trust: trust))
        } else {
            let message = trustFailureMessage(trust: trust, error: error, serverName: serverName)
            recordTLSFailure(message)
            VerboseKubeTrace.append(
                "k8s.tls",
                "server-trust rejected host=\(challenge.protectionSpace.host) \(message)"
            )
            completionHandler(.cancelAuthenticationChallenge, nil)
        }
    }

    private func recordTLSFailure(_ message: String) {
        tlsFailureState.set(message)
    }

    private func clearTLSFailure() {
        tlsFailureState.set(nil)
    }

    private func trustFailureMessage(trust: SecTrust, error: CFError?, serverName: String) -> String {
        var parts = ["serverTrust=\(serverName)"]
        if let error {
            parts.append("trustError=\(CFErrorCopyDescription(error) as String)")
        }
        let chain = (SecTrustCopyCertificateChain(trust) as? [SecCertificate]) ?? []
        if !chain.isEmpty {
            let subjects = chain
                .prefix(4)
                .compactMap { SecCertificateCopySubjectSummary($0) as String? }
                .joined(separator: " -> ")
            parts.append("chainCount=\(chain.count)")
            if !subjects.isEmpty {
                parts.append("chain=\(subjects)")
            }
        }
        return parts.joined(separator: " | ")
    }
}

private final class TLSFailureState: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: String?

    func value() -> String? {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }

    func set(_ value: String?) {
        lock.lock()
        storage = value
        lock.unlock()
    }
}

private func certificates(from data: Data) -> [SecCertificate] {
    certificateDERBlocks(from: data).compactMap {
        SecCertificateCreateWithData(nil, $0 as CFData)
    }
}

private func networkErrorMessage(_ error: Error, resolved: ResolvedRESTContext, tlsFailure: String?) -> String {
    let nsError = error as NSError
    var details = [
        error.localizedDescription,
        "server=\(resolved.serverURL.host ?? resolved.serverURL.absoluteString)",
        "tls=\(resolved.tlsDescription)"
    ]
    if let tlsFailure {
        details.append("trust=\(tlsFailure)")
    }
    if let failingURL = nsError.userInfo[NSURLErrorFailingURLErrorKey] as? URL {
        details.append("url=\(failingURL.host ?? failingURL.absoluteString)")
    }
    if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? NSError {
        details.append("underlying=\(underlying.domain)(\(underlying.code)): \(underlying.localizedDescription)")
        if let deeper = underlying.userInfo[NSUnderlyingErrorKey] as? NSError {
            details.append("root=\(deeper.domain)(\(deeper.code)): \(deeper.localizedDescription)")
        }
    }
    return details.joined(separator: " | ")
}

private func sleepBeforeKubernetesRetry(
    method: String,
    contextName: String,
    apiPath: String,
    attempt: Int,
    decision: KubernetesRequestRetryDecision
) async throws {
    let delayNanoseconds = KubernetesRequestRetryPolicy.boundedDelayNanoseconds(for: decision, attempt: attempt)
    VerboseKubeTrace.append(
        "k8s.request",
        "retry method=\(method) context=<redacted-context> path=\(apiPath) attempt=\(attempt + 1) delayMs=\(delayNanoseconds / 1_000_000) \(decision.traceDescription)"
    )
    try await Task.sleep(nanoseconds: delayNanoseconds)
}

private extension ResolvedRESTContext {
    var tlsDescription: String {
        if insecureSkipTLSVerify {
            return "insecure-skip-tls-verify"
        }
        var parts: [String] = []
        parts.append(certificateAuthorityData == nil ? "system-trust" : "kubeconfig-ca")
        if tlsServerName != nil {
            parts.append("tls-server-name")
        }
        if clientTLSIdentity != nil {
            parts.append("client-certificate")
        }
        return parts.joined(separator: "+")
    }
}

private func certificateDERBlocks(from data: Data) -> [Data] {
    guard let string = String(data: data, encoding: .utf8),
          string.contains("BEGIN CERTIFICATE") else {
        return [data]
    }
    let begin = "-----BEGIN CERTIFICATE-----"
    let end = "-----END CERTIFICATE-----"
    var blocks: [Data] = []
    var remaining = string[...]
    while let beginRange = remaining.range(of: begin) {
        let bodyStart = beginRange.upperBound
        guard let endRange = remaining[bodyStart...].range(of: end) else { break }
        let body = remaining[bodyStart..<endRange.lowerBound]
            .split(whereSeparator: \.isWhitespace)
            .joined()
        if let decoded = Data(base64Encoded: body, options: .ignoreUnknownCharacters) {
            blocks.append(decoded)
        }
        remaining = remaining[endRange.upperBound...]
    }
    return blocks
}

private final class KubernetesExecWebSocketHandle: RunningCommandControlling, @unchecked Sendable {
    let id = UUID()
    private let task: URLSessionWebSocketTask
    private let session: URLSession
    private let onOutput: @Sendable (String) -> Void
    private let onTermination: @Sendable (Int32) -> Void
    private let state = State()

    init(
        task: URLSessionWebSocketTask,
        session: URLSession,
        onOutput: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) -> Void
    ) {
        self.task = task
        self.session = session
        self.onOutput = onOutput
        self.onTermination = onTermination
    }

    func startReceiving() {
        Task {
            var exitCode: Int32 = 0
            while await !state.isTerminated {
                do {
                    let message = try await task.receive()
                    let data: Data
                    switch message {
                    case let .data(value):
                        data = value
                    case let .string(value):
                        data = Data(value.utf8)
                    @unknown default:
                        continue
                    }
                    guard let channel = data.first else { continue }
                    let payload = Data(data.dropFirst())
                    switch channel {
                    case 1, 2:
                        if !payload.isEmpty {
                            onOutput(String(decoding: payload, as: UTF8.self))
                        }
                    case 3:
                        exitCode = KubernetesRESTClient.execExitCode(from: payload)
                        await state.markTerminated()
                    default:
                        continue
                    }
                } catch {
                    await state.markTerminated()
                }
            }
            task.cancel(with: .normalClosure, reason: nil)
            session.invalidateAndCancel()
            onTermination(exitCode)
        }
    }

    func terminate() {
        Task {
            await state.markTerminated()
            task.cancel(with: .goingAway, reason: nil)
            session.invalidateAndCancel()
        }
    }

    func writeToStdin(_ data: Data) throws {
        var framed = Data([0])
        framed.append(data)
        task.send(.data(framed)) { error in
            if let error {
                RuneLoggers.kubernetesExec.error("stdin send failed: \(String(describing: error), privacy: .private)")
            }
        }
    }

    func resizeTerminal(columns: Int, rows: Int) throws {
        let frame = try KubernetesRESTClient.terminalResizeFrame(columns: columns, rows: rows)
        task.send(.data(frame)) { error in
            if let error {
                RuneLoggers.kubernetesExec.error("resize send failed: \(String(describing: error), privacy: .private)")
            }
        }
    }

    private actor State {
        private(set) var isTerminated = false

        func markTerminated() {
            isTerminated = true
        }
    }
}

private final class KubernetesPortForwardHandle: RunningCommandControlling, @unchecked Sendable {
    let id = UUID()

    private let listener: NWListener
    private let session: URLSession
    private let resolved: ResolvedRESTContext
    private let namespace: String
    private let podName: String
    private let remotePort: Int
    private let makeTask: @Sendable (URLSession, ResolvedRESTContext) throws -> URLSessionWebSocketTask
    private let onReady: @Sendable () -> Void
    private let onFailure: @Sendable (String) -> Void
    private let queue = DispatchQueue(label: "com.rune.kubernetes-port-forward")
    private let lock = NSLock()
    private var bridges: [UUID: PortForwardConnectionBridge] = [:]
    private var listenerReady = false
    private var terminated = false

    init(
        listener: NWListener,
        session: URLSession,
        resolved: ResolvedRESTContext,
        namespace: String,
        podName: String,
        remotePort: Int,
        makeTask: @escaping @Sendable (URLSession, ResolvedRESTContext) throws -> URLSessionWebSocketTask,
        onReady: @escaping @Sendable () -> Void,
        onFailure: @escaping @Sendable (String) -> Void
    ) {
        self.listener = listener
        self.session = session
        self.resolved = resolved
        self.namespace = namespace
        self.podName = podName
        self.remotePort = remotePort
        self.makeTask = makeTask
        self.onReady = onReady
        self.onFailure = onFailure
    }

    func start() {
        listener.stateUpdateHandler = { [weak self] state in
            guard let self else { return }
            switch state {
            case .ready:
                self.markReady()
                self.onReady()
            case let .waiting(error):
                self.onFailure("Port-forward listener is waiting: \(error.localizedDescription)")
                self.terminate()
            case let .failed(error):
                self.onFailure("Port-forward listener failed: \(error.localizedDescription)")
                self.terminate()
            case .cancelled:
                self.closeAllBridges()
            default:
                break
            }
        }
        listener.newConnectionHandler = { [weak self] connection in
            self?.accept(connection)
        }
        listener.start(queue: queue)
        queue.asyncAfter(deadline: .now() + 10) { [weak self] in
            self?.failIfListenerDidNotStart()
        }
    }

    func terminate() {
        lock.lock()
        let shouldTerminate = !terminated
        terminated = true
        lock.unlock()

        guard shouldTerminate else { return }
        listener.cancel()
        closeAllBridges()
        session.invalidateAndCancel()
    }

    private func markReady() {
        lock.lock()
        listenerReady = true
        lock.unlock()
    }

    private func failIfListenerDidNotStart() {
        lock.lock()
        let shouldFail = !listenerReady && !terminated
        lock.unlock()

        guard shouldFail else { return }
        onFailure("Timed out starting local port-forward listener.")
        terminate()
    }

    func writeToStdin(_ data: Data) throws {
        throw RuneError.commandFailed(command: "port-forward", message: "Port-forward sessions do not accept stdin.")
    }

    private func accept(_ connection: NWConnection) {
        lock.lock()
        let isTerminated = terminated
        lock.unlock()
        guard !isTerminated else {
            connection.cancel()
            return
        }

        do {
            let task = try makeTask(session, resolved)
            let bridge = PortForwardConnectionBridge(
                connection: connection,
                webSocketTask: task,
                remotePort: remotePort,
                queue: queue,
                onClose: { [weak self] id in
                    self?.removeBridge(id: id)
                },
                onFailure: { [weak self] message in
                    self?.onFailure("Port-forward \(self?.namespace ?? "")/\(self?.podName ?? ""):\(self?.remotePort ?? 0) failed: \(message)")
                }
            )
            lock.lock()
            bridges[bridge.id] = bridge
            lock.unlock()
            bridge.start()
        } catch {
            connection.cancel()
            onFailure("Could not open Kubernetes port-forward stream: \(error.localizedDescription)")
        }
    }

    private func removeBridge(id: UUID) {
        lock.lock()
        bridges.removeValue(forKey: id)
        lock.unlock()
    }

    private func closeAllBridges() {
        lock.lock()
        let current = Array(bridges.values)
        bridges.removeAll()
        lock.unlock()
        for bridge in current {
            bridge.close()
        }
    }
}

private final class LegacySPDYPortForwardHandle: RunningCommandControlling, @unchecked Sendable {
    let id = UUID()

    private let listener: NWListener
    private let resolved: ResolvedRESTContext
    private let namespace: String
    private let podName: String
    private let remotePort: Int
    private let onReady: @Sendable () -> Void
    private let onFailure: @Sendable (String) -> Void
    private let queue = DispatchQueue(label: "com.rune.legacy-spdy-port-forward")
    private let lock = NSLock()
    private var bridges: [UUID: LegacySPDYConnectionBridge] = [:]
    private var listenerReady = false
    private var terminated = false

    init(
        listener: NWListener,
        resolved: ResolvedRESTContext,
        namespace: String,
        podName: String,
        remotePort: Int,
        onReady: @escaping @Sendable () -> Void,
        onFailure: @escaping @Sendable (String) -> Void
    ) {
        self.listener = listener
        self.resolved = resolved
        self.namespace = namespace
        self.podName = podName
        self.remotePort = remotePort
        self.onReady = onReady
        self.onFailure = onFailure
    }

    func start() {
        listener.stateUpdateHandler = { [weak self] state in
            guard let self else { return }
            switch state {
            case .ready:
                self.markReady()
                self.onReady()
            case let .waiting(error):
                self.onFailure("Port-forward listener is waiting: \(error.localizedDescription)")
                self.terminate()
            case let .failed(error):
                self.onFailure("Port-forward listener failed: \(error.localizedDescription)")
                self.terminate()
            case .cancelled:
                self.closeAllBridges()
            default:
                break
            }
        }
        listener.newConnectionHandler = { [weak self] connection in
            self?.accept(connection)
        }
        listener.start(queue: queue)
        queue.asyncAfter(deadline: .now() + 10) { [weak self] in
            self?.failIfListenerDidNotStart()
        }
    }

    func terminate() {
        lock.lock()
        let shouldTerminate = !terminated
        terminated = true
        lock.unlock()

        guard shouldTerminate else { return }
        listener.cancel()
        closeAllBridges()
    }

    func writeToStdin(_ data: Data) throws {
        throw RuneError.commandFailed(command: "port-forward", message: "Port-forward sessions do not accept stdin.")
    }

    private func markReady() {
        lock.lock()
        listenerReady = true
        lock.unlock()
    }

    private func failIfListenerDidNotStart() {
        lock.lock()
        let shouldFail = !listenerReady && !terminated
        lock.unlock()

        guard shouldFail else { return }
        onFailure("Timed out starting local port-forward listener.")
        terminate()
    }

    private func accept(_ connection: NWConnection) {
        lock.lock()
        let isTerminated = terminated
        lock.unlock()
        guard !isTerminated else {
            connection.cancel()
            return
        }

        do {
            let remote = try Self.makeRemoteConnection(resolved: resolved, queue: queue)
            let request = try Self.makeUpgradeRequest(
                resolved: resolved,
                namespace: namespace,
                podName: podName
            )
            let bridge = LegacySPDYConnectionBridge(
                localConnection: connection,
                remoteConnection: remote,
                upgradeRequest: request,
                remotePort: remotePort,
                queue: queue,
                onClose: { [weak self] id in
                    self?.removeBridge(id: id)
                },
                onFailure: { [weak self] message in
                    self?.onFailure("Port-forward \(self?.namespace ?? "")/\(self?.podName ?? ""):\(self?.remotePort ?? 0) failed: \(message)")
                }
            )
            lock.lock()
            bridges[bridge.id] = bridge
            lock.unlock()
            bridge.start()
        } catch {
            connection.cancel()
            onFailure("Could not open Kubernetes port-forward stream: \(error.localizedDescription)")
        }
    }

    private func removeBridge(id: UUID) {
        lock.lock()
        bridges.removeValue(forKey: id)
        lock.unlock()
    }

    private func closeAllBridges() {
        lock.lock()
        let current = Array(bridges.values)
        bridges.removeAll()
        lock.unlock()
        for bridge in current {
            bridge.close()
        }
    }

    private static func makeRemoteConnection(
        resolved: ResolvedRESTContext,
        queue: DispatchQueue
    ) throws -> NWConnection {
        guard let host = resolved.serverURL.host else {
            throw RuneError.invalidInput(message: "Kubernetes server URL is missing a host.")
        }
        let rawPort = resolved.serverURL.port ?? (resolved.serverURL.scheme == "http" ? 80 : 443)
        guard let port = NWEndpoint.Port(rawValue: UInt16(rawPort)) else {
            throw RuneError.invalidInput(message: "Kubernetes server URL has an invalid port.")
        }

        let parameters: NWParameters
        if resolved.serverURL.scheme == "http" {
            parameters = .tcp
        } else {
            let tlsOptions = NWProtocolTLS.Options()
            let tlsServerName = resolved.tlsServerName ?? host
            sec_protocol_options_set_tls_server_name(tlsOptions.securityProtocolOptions, tlsServerName)
            "http/1.1".withCString {
                sec_protocol_options_add_tls_application_protocol(tlsOptions.securityProtocolOptions, $0)
            }
            if let identity = resolved.clientTLSIdentity?.identity,
               let protocolIdentity = sec_identity_create(identity) {
                sec_protocol_options_set_local_identity(tlsOptions.securityProtocolOptions, protocolIdentity)
            }
            if resolved.insecureSkipTLSVerify || resolved.certificateAuthorityData != nil || resolved.tlsServerName != nil {
                sec_protocol_options_set_verify_block(
                    tlsOptions.securityProtocolOptions,
                    { _, trust, complete in
                        complete(Self.verifyServerTrust(trust, resolved: resolved, host: host))
                    },
                    queue
                )
            }
            parameters = NWParameters(tls: tlsOptions, tcp: NWProtocolTCP.Options())
        }

        return NWConnection(host: NWEndpoint.Host(host), port: port, using: parameters)
    }

    private static func verifyServerTrust(
        _ trust: sec_trust_t,
        resolved: ResolvedRESTContext,
        host: String
    ) -> Bool {
        if resolved.insecureSkipTLSVerify {
            return true
        }

        let trustRef = sec_trust_copy_ref(trust).takeRetainedValue()
        let serverName = resolved.tlsServerName ?? host
        SecTrustSetPolicies(trustRef, SecPolicyCreateSSL(true, serverName as CFString))
        if let certificateAuthorityData = resolved.certificateAuthorityData {
            let anchors = certificates(from: certificateAuthorityData)
            guard !anchors.isEmpty else { return false }
            SecTrustSetAnchorCertificates(trustRef, anchors as CFArray)
            SecTrustSetAnchorCertificatesOnly(trustRef, true)
        }
        return SecTrustEvaluateWithError(trustRef, nil)
    }

    private static func makeUpgradeRequest(
        resolved: ResolvedRESTContext,
        namespace: String,
        podName: String
    ) throws -> Data {
        guard let host = resolved.serverURL.host else {
            throw RuneError.invalidInput(message: "Kubernetes server URL is missing a host.")
        }
        let rawPort = resolved.serverURL.port ?? (resolved.serverURL.scheme == "http" ? 80 : 443)
        let hostHeader = (rawPort == 80 || rawPort == 443) ? host : "\(host):\(rawPort)"
        let path = "/api/v1/namespaces/\(namespace.runePercentEncodedPathSegment)/pods/\(podName.runePercentEncodedPathSegment)/portforward"

        var lines = [
            "POST \(path) HTTP/1.1",
            "Host: \(hostHeader)",
            "User-Agent: Rune",
            "Connection: Upgrade",
            "Upgrade: SPDY/3.1",
            "X-Stream-Protocol-Version: portforward.k8s.io"
        ]
        switch resolved.authentication {
        case .none:
            break
        case let .bearer(token):
            lines.append("Authorization: Bearer \(token)")
        case let .basic(username, password):
            let raw = Data("\(username):\(password)".utf8).base64EncodedString()
            lines.append("Authorization: Basic \(raw)")
        }
        lines.append("")
        lines.append("")
        return Data(lines.joined(separator: "\r\n").utf8)
    }
}

private final class LegacySPDYConnectionBridge: @unchecked Sendable {
    let id = UUID()

    private enum Constants {
        static let dataStreamID: UInt32 = 3
        static let errorStreamID: UInt32 = 1
        static let finFlag: UInt8 = 0x01
    }

    private let localConnection: NWConnection
    private let remoteConnection: NWConnection
    private let upgradeRequest: Data
    private let remotePort: Int
    private let queue: DispatchQueue
    private let onClose: @Sendable (UUID) -> Void
    private let onFailure: @Sendable (String) -> Void
    private let lock = NSLock()
    private let framer = SPDYPortForwardFramer()
    private var closed = false
    private var localReady = false
    private var remoteReady = false
    private var didSendUpgrade = false
    private var didUpgrade = false
    private var handshakeBuffer = Data()
    private var remoteBuffer = Data()
    private var errorStreamText = ""

    init(
        localConnection: NWConnection,
        remoteConnection: NWConnection,
        upgradeRequest: Data,
        remotePort: Int,
        queue: DispatchQueue,
        onClose: @escaping @Sendable (UUID) -> Void,
        onFailure: @escaping @Sendable (String) -> Void
    ) {
        self.localConnection = localConnection
        self.remoteConnection = remoteConnection
        self.upgradeRequest = upgradeRequest
        self.remotePort = remotePort
        self.queue = queue
        self.onClose = onClose
        self.onFailure = onFailure
    }

    func start() {
        localConnection.stateUpdateHandler = { [weak self] state in
            guard let self else { return }
            self.queue.async {
                switch state {
                case .ready:
                    self.localReady = true
                    self.startUpgradeIfReady()
                case let .failed(error):
                    self.onFailure(error.localizedDescription)
                    self.close()
                case .cancelled:
                    self.close()
                default:
                    break
                }
            }
        }
        remoteConnection.stateUpdateHandler = { [weak self] state in
            guard let self else { return }
            self.queue.async {
                switch state {
                case .ready:
                    self.remoteReady = true
                    self.startUpgradeIfReady()
                case let .failed(error):
                    self.onFailure(error.localizedDescription)
                    self.close()
                case .cancelled:
                    self.close()
                default:
                    break
                }
            }
        }
        localConnection.start(queue: queue)
        remoteConnection.start(queue: queue)
    }

    func close() {
        lock.lock()
        let shouldClose = !closed
        closed = true
        lock.unlock()
        guard shouldClose else { return }
        localConnection.cancel()
        remoteConnection.cancel()
        onClose(id)
    }

    private func startUpgradeIfReady() {
        guard localReady, remoteReady, !didSendUpgrade else { return }
        didSendUpgrade = true
        remoteConnection.send(content: upgradeRequest, completion: .contentProcessed { [weak self] error in
            guard let self else { return }
            self.queue.async {
                if let error {
                    self.onFailure(error.localizedDescription)
                    self.close()
                    return
                }
                self.receiveUpgradeResponse()
            }
        })
    }

    private func receiveUpgradeResponse() {
        remoteConnection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] data, _, isComplete, error in
            guard let self else { return }
            self.queue.async {
                if let error {
                    self.onFailure(error.localizedDescription)
                    self.close()
                    return
                }
                if let data, !data.isEmpty {
                    self.handshakeBuffer.append(data)
                }
                if let headerRange = self.handshakeBuffer.range(of: Data("\r\n\r\n".utf8)) {
                    let headerData = self.handshakeBuffer.subdata(in: 0..<headerRange.lowerBound)
                    let remainingStart = headerRange.upperBound
                    let remainder = self.handshakeBuffer.subdata(in: remainingStart..<self.handshakeBuffer.endIndex)
                    let response = String(decoding: headerData, as: UTF8.self)
                    guard response.hasPrefix("HTTP/1.1 101") || response.hasPrefix("HTTP/1.0 101") else {
                        let preview = response.split(separator: "\r\n", omittingEmptySubsequences: true).prefix(3).joined(separator: " ")
                        self.onFailure("Kubernetes rejected SPDY port-forward upgrade: \(preview)")
                        self.close()
                        return
                    }
                    self.didUpgrade = true
                    self.handshakeBuffer.removeAll(keepingCapacity: false)
                    do {
                        try self.openRemoteStreams()
                    } catch {
                        self.onFailure(error.localizedDescription)
                        self.close()
                        return
                    }
                    if !remainder.isEmpty {
                        self.remoteBuffer.append(remainder)
                        self.processRemoteFrames()
                    }
                    self.receiveFromLocal()
                    self.receiveFromRemote()
                } else if isComplete {
                    self.onFailure("Kubernetes closed the port-forward upgrade before sending a response.")
                    self.close()
                } else {
                    self.receiveUpgradeResponse()
                }
            }
        }
    }

    private func receiveFromLocal() {
        guard didUpgrade else { return }
        localConnection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] data, _, isComplete, error in
            guard let self else { return }
            self.queue.async {
                if let error {
                    self.onFailure(error.localizedDescription)
                    self.close()
                    return
                }
                if let data, !data.isEmpty {
                    self.sendRemoteData(self.framer.dataFrame(streamID: Constants.dataStreamID, payload: data))
                }
                if isComplete {
                    self.sendRemoteData(self.framer.dataFrame(streamID: Constants.dataStreamID, payload: Data(), flags: Constants.finFlag))
                } else {
                    self.receiveFromLocal()
                }
            }
        }
    }

    private func receiveFromRemote() {
        guard didUpgrade else { return }
        remoteConnection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] data, _, isComplete, error in
            guard let self else { return }
            self.queue.async {
                if let error {
                    self.onFailure(error.localizedDescription)
                    self.close()
                    return
                }
                if let data, !data.isEmpty {
                    self.remoteBuffer.append(data)
                    self.processRemoteFrames()
                }
                if isComplete {
                    self.close()
                } else {
                    self.receiveFromRemote()
                }
            }
        }
    }

    private func openRemoteStreams() throws {
        let requestID = "1"
        let port = String(remotePort)
        sendRemoteData(try framer.synStream(
            streamID: Constants.errorStreamID,
            headers: [
                "streamType": ["error"],
                "port": [port],
                "requestID": [requestID]
            ]
        ))
        sendRemoteData(framer.dataFrame(streamID: Constants.errorStreamID, payload: Data(), flags: Constants.finFlag))
        sendRemoteData(try framer.synStream(
            streamID: Constants.dataStreamID,
            headers: [
                "streamType": ["data"],
                "port": [port],
                "requestID": [requestID]
            ]
        ))
    }

    private func sendRemoteData(_ data: Data) {
        remoteConnection.send(content: data, completion: .contentProcessed { [weak self] error in
            guard let self else { return }
            self.queue.async {
                if let error {
                    self.onFailure(error.localizedDescription)
                    self.close()
                }
            }
        })
    }

    private func processRemoteFrames() {
        while let frame = SPDYPortForwardFrame.parse(from: &remoteBuffer) {
            switch frame {
            case let .data(streamID, flags, payload):
                handleDataFrame(streamID: streamID, flags: flags, payload: payload)
            case let .control(type, payload):
                handleControlFrame(type: type, payload: payload)
            }
        }
    }

    private func handleDataFrame(streamID: UInt32, flags: UInt8, payload: Data) {
        switch streamID {
        case Constants.dataStreamID:
            if !payload.isEmpty {
                localConnection.send(content: payload, completion: .contentProcessed { [weak self] error in
                    guard let self else { return }
                    self.queue.async {
                        if let error {
                            self.onFailure(error.localizedDescription)
                            self.close()
                        }
                    }
                })
            }
            if flags & Constants.finFlag != 0 {
                close()
            }
        case Constants.errorStreamID:
            if !payload.isEmpty {
                errorStreamText += String(decoding: payload, as: UTF8.self)
            }
            if flags & Constants.finFlag != 0, !errorStreamText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                onFailure(errorStreamText.trimmingCharacters(in: .whitespacesAndNewlines))
                close()
            }
        default:
            break
        }
    }

    private func handleControlFrame(type: UInt16, payload: Data) {
        switch type {
        case 3:
            if payload.count >= 8 {
                let streamID = payload.runeReadUInt32(at: 0) & 0x7fffffff
                let status = payload.runeReadUInt32(at: 4)
                onFailure("SPDY stream \(streamID) reset with status \(status).")
                close()
            }
        case 6:
            if payload.count == 4 {
                sendRemoteData(framer.pingFrame(id: payload.runeReadUInt32(at: 0)))
            }
        case 7:
            close()
        default:
            break
        }
    }
}

private final class PortForwardConnectionBridge: @unchecked Sendable {
    let id = UUID()

    private enum Constants {
        static let dataStreamID: UInt32 = 3
        static let errorStreamID: UInt32 = 1
        static let finFlag: UInt8 = 0x01
    }

    private let connection: NWConnection
    private let webSocketTask: URLSessionWebSocketTask
    private let remotePort: Int
    private let queue: DispatchQueue
    private let onClose: @Sendable (UUID) -> Void
    private let onFailure: @Sendable (String) -> Void
    private let lock = NSLock()
    private let framer = SPDYPortForwardFramer()
    private var closed = false
    private var remoteBuffer = Data()
    private var errorStreamText = ""

    init(
        connection: NWConnection,
        webSocketTask: URLSessionWebSocketTask,
        remotePort: Int,
        queue: DispatchQueue,
        onClose: @escaping @Sendable (UUID) -> Void,
        onFailure: @escaping @Sendable (String) -> Void
    ) {
        self.connection = connection
        self.webSocketTask = webSocketTask
        self.remotePort = remotePort
        self.queue = queue
        self.onClose = onClose
        self.onFailure = onFailure
    }

    func start() {
        connection.stateUpdateHandler = { [weak self] state in
            guard let self else { return }
            switch state {
            case .ready:
                self.queue.async {
                    do {
                        self.webSocketTask.resume()
                        try self.openRemoteStreams()
                        self.receiveFromLocal()
                        self.receiveFromRemote()
                    } catch {
                        self.onFailure(error.localizedDescription)
                        self.close()
                    }
                }
            case let .failed(error):
                self.onFailure(error.localizedDescription)
                self.close()
            case .cancelled:
                self.close()
            default:
                break
            }
        }
        connection.start(queue: queue)
    }

    func close() {
        lock.lock()
        let shouldClose = !closed
        closed = true
        lock.unlock()
        guard shouldClose else { return }
        connection.cancel()
        webSocketTask.cancel(with: .goingAway, reason: nil)
        onClose(id)
    }

    private func receiveFromLocal() {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] data, _, isComplete, error in
            guard let self else { return }
            self.queue.async {
                if let error {
                    self.onFailure(error.localizedDescription)
                    self.close()
                    return
                }
                if let data, !data.isEmpty {
                    self.sendWebSocketData(self.framer.dataFrame(streamID: Constants.dataStreamID, payload: data))
                }
                if isComplete {
                    self.sendWebSocketData(self.framer.dataFrame(streamID: Constants.dataStreamID, payload: Data(), flags: Constants.finFlag))
                } else {
                    self.receiveFromLocal()
                }
            }
        }
    }

    private func receiveFromRemote() {
        webSocketTask.receive { [weak self] result in
            guard let self else { return }
            self.queue.async {
                switch result {
                case let .success(message):
                    let data: Data
                    switch message {
                    case let .data(value):
                        data = value
                    case let .string(value):
                        data = Data(value.utf8)
                    @unknown default:
                        self.receiveFromRemote()
                        return
                    }
                    self.remoteBuffer.append(data)
                    self.processRemoteFrames()
                    self.receiveFromRemote()
                case let .failure(error):
                    self.onFailure(error.localizedDescription)
                    self.close()
                }
            }
        }
    }

    private func openRemoteStreams() throws {
        let requestID = "1"
        let port = String(remotePort)
        sendWebSocketData(try framer.synStream(
            streamID: Constants.errorStreamID,
            headers: [
                "streamType": ["error"],
                "port": [port],
                "requestID": [requestID]
            ]
        ))
        sendWebSocketData(framer.dataFrame(streamID: Constants.errorStreamID, payload: Data(), flags: Constants.finFlag))
        sendWebSocketData(try framer.synStream(
            streamID: Constants.dataStreamID,
            headers: [
                "streamType": ["data"],
                "port": [port],
                "requestID": [requestID]
            ]
        ))
    }

    private func sendWebSocketData(_ data: Data) {
        webSocketTask.send(.data(data)) { [weak self] error in
            guard let self, let error else { return }
            self.onFailure(error.localizedDescription)
            self.close()
        }
    }

    private func processRemoteFrames() {
        while let frame = SPDYPortForwardFrame.parse(from: &remoteBuffer) {
            switch frame {
            case let .data(streamID, flags, payload):
                handleDataFrame(streamID: streamID, flags: flags, payload: payload)
            case let .control(type, payload):
                handleControlFrame(type: type, payload: payload)
            }
        }
    }

    private func handleDataFrame(streamID: UInt32, flags: UInt8, payload: Data) {
        switch streamID {
        case Constants.dataStreamID:
            if !payload.isEmpty {
                connection.send(content: payload, completion: .contentProcessed { [weak self] error in
                    guard let self else { return }
                    if let error {
                        self.onFailure(error.localizedDescription)
                        self.close()
                    }
                })
            }
            if flags & Constants.finFlag != 0 {
                self.close()
            }
        case Constants.errorStreamID:
            if !payload.isEmpty {
                errorStreamText += String(decoding: payload, as: UTF8.self)
            }
            if flags & Constants.finFlag != 0, !errorStreamText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                onFailure(errorStreamText.trimmingCharacters(in: .whitespacesAndNewlines))
                close()
            }
        default:
            break
        }
    }

    private func handleControlFrame(type: UInt16, payload: Data) {
        switch type {
        case 3:
            if payload.count >= 8 {
                let streamID = payload.runeReadUInt32(at: 0) & 0x7fffffff
                let status = payload.runeReadUInt32(at: 4)
                onFailure("SPDY stream \(streamID) reset with status \(status).")
                close()
            }
        case 6:
            if payload.count == 4 {
                sendWebSocketData(framer.pingFrame(id: payload.runeReadUInt32(at: 0)))
            }
        case 7:
            close()
        default:
            break
        }
    }
}

private enum SPDYPortForwardFrame {
    case data(streamID: UInt32, flags: UInt8, payload: Data)
    case control(type: UInt16, payload: Data)

    static func parse(from buffer: inout Data) -> SPDYPortForwardFrame? {
        guard buffer.count >= 8 else { return nil }
        let first = buffer.runeReadUInt16(at: 0)
        let second = buffer.runeReadUInt16(at: 2)
        let flags = buffer[4]
        let length = buffer.runeReadUInt24(at: 5)
        guard buffer.count >= 8 + length else { return nil }

        let payload = buffer.subdata(in: 8..<(8 + length))
        buffer.removeSubrange(0..<(8 + length))

        if first & 0x8000 != 0 {
            return .control(type: second, payload: payload)
        }
        let streamID = (UInt32(first & 0x7fff) << 16) | UInt32(second)
        return .data(streamID: streamID, flags: flags, payload: payload)
    }
}

private final class SPDYPortForwardFramer {
    private let compressor = SPDYZlibCompressor()

    func synStream(streamID: UInt32, headers: [String: [String]]) throws -> Data {
        let headerBlock = try compressor.compress(headerValueBlock(headers))
        var payload = Data()
        payload.runeAppendUInt32(streamID & 0x7fffffff)
        payload.runeAppendUInt32(0)
        payload.append(0)
        payload.append(0)
        payload.append(headerBlock)

        var frame = Data()
        frame.runeAppendUInt16(0x8003)
        frame.runeAppendUInt16(1)
        frame.append(0)
        frame.runeAppendUInt24(payload.count)
        frame.append(payload)
        return frame
    }

    func dataFrame(streamID: UInt32, payload: Data, flags: UInt8 = 0) -> Data {
        var frame = Data()
        frame.runeAppendUInt32(streamID & 0x7fffffff)
        frame.append(flags)
        frame.runeAppendUInt24(payload.count)
        frame.append(payload)
        return frame
    }

    func pingFrame(id: UInt32) -> Data {
        var payload = Data()
        payload.runeAppendUInt32(id)
        var frame = Data()
        frame.runeAppendUInt16(0x8003)
        frame.runeAppendUInt16(6)
        frame.append(0)
        frame.runeAppendUInt24(payload.count)
        frame.append(payload)
        return frame
    }

    private func headerValueBlock(_ headers: [String: [String]]) -> Data {
        let normalized = headers
            .map { ($0.key.lowercased(), $0.value) }
            .sorted { $0.0 < $1.0 }
        var data = Data()
        data.runeAppendUInt32(UInt32(normalized.count))
        for (name, values) in normalized {
            let nameData = Data(name.utf8)
            let valueData = Data(values.joined(separator: "\u{0}").utf8)
            data.runeAppendUInt32(UInt32(nameData.count))
            data.append(nameData)
            data.runeAppendUInt32(UInt32(valueData.count))
            data.append(valueData)
        }
        return data
    }
}

private final class SPDYZlibCompressor {
    private var stream = z_stream()
    private var didEnd = false

    init() {
        stream.zalloc = nil
        stream.zfree = nil
        stream.opaque = nil
        deflateInit_(&stream, Z_BEST_COMPRESSION, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size))
        if let dictionary = Data(base64Encoded: Self.spdyDictionaryBase64) {
            dictionary.withUnsafeBytes { buffer in
                if let base = buffer.bindMemory(to: Bytef.self).baseAddress {
                    deflateSetDictionary(&stream, base, uInt(buffer.count))
                }
            }
        }
    }

    deinit {
        if !didEnd {
            deflateEnd(&stream)
        }
    }

    func compress(_ data: Data) throws -> Data {
        var inputData = data
        var output = Data()
        let status: Int32 = inputData.withUnsafeMutableBytes { inputBuffer in
            stream.next_in = inputBuffer.bindMemory(to: Bytef.self).baseAddress
            stream.avail_in = uInt(inputBuffer.count)

            var finalStatus: Int32 = Z_OK
            repeat {
                var chunk = [UInt8](repeating: 0, count: 4096)
                finalStatus = chunk.withUnsafeMutableBufferPointer { outputBuffer in
                    stream.next_out = outputBuffer.baseAddress
                    stream.avail_out = uInt(outputBuffer.count)
                    return deflate(&stream, Z_SYNC_FLUSH)
                }
                let produced = chunk.count - Int(stream.avail_out)
                if produced > 0 {
                    output.append(contentsOf: chunk.prefix(produced))
                }
            } while stream.avail_out == 0
            return finalStatus
        }

        guard status == Z_OK else {
            throw RuneError.commandFailed(command: "spdy zlib", message: "Could not compress SPDY headers: zlib status \(status).")
        }
        return output
    }

    private static let spdyDictionaryBase64 = """
    AAAAB29wdGlvbnMAAAAEaGVhZAAAAARwb3N0AAAAA3B1dAAAAAZkZWxldGUAAAAFdHJhY2UAAAAGYWNjZXB0AAAADmFjY2VwdC1jaGFyc2V0AAAAD2FjY2VwdC1lbmNvZGluZwAAAA9hY2NlcHQtbGFuZ3VhZ2UAAAANYWNjZXB0LXJhbmdlcwAAAANhZ2UAAAAFYWxsb3cAAAANYXV0aG9yaXphdGlvbgAAAA1jYWNoZS1jb250cm9sAAAACmNvbm5lY3Rpb24AAAAMY29udGVudC1iYXNlAAAAEGNvbnRlbnQtZW5jb2RpbmcAAAAQY29udGVudC1sYW5ndWFnZQAAAA5jb250ZW50LWxlbmd0aAAAABBjb250ZW50LWxvY2F0aW9uAAAAC2NvbnRlbnQtbWQ1AAAADWNvbnRlbnQtcmFuZ2UAAAAMY29udGVudC10eXBlAAAABGRhdGUAAAAEZXRhZwAAAAZleHBlY3QAAAAHZXhwaXJlcwAAAARmcm9tAAAABGhvc3QAAAAIaWYtbWF0Y2gAAAARaWYtbW9kaWZpZWQtc2luY2UAAAANaWYtbm9uZS1tYXRjaAAAAAhpZi1yYW5nZQAAABNpZi11bm1vZGlmaWVkLXNpbmNlAAAADWxhc3QtbW9kaWZpZWQAAAAIbG9jYXRpb24AAAAMbWF4LWZvcndhcmRzAAAABnByYWdtYQAAABJwcm94eS1hdXRoZW50aWNhdGUAAAATcHJveHktYXV0aG9yaXphdGlvbgAAAAVyYW5nZQAAAAdyZWZlcmVyAAAAC3JldHJ5LWFmdGVyAAAABnNlcnZlcgAAAAJ0ZQAAAAd0cmFpbGVyAAAAEXRyYW5zZmVyLWVuY29kaW5nAAAAB3VwZ3JhZGUAAAAKdXNlci1hZ2VudAAAAAR2YXJ5AAAAA3ZpYQAAAAd3YXJuaW5nAAAAEHd3dy1hdXRoZW50aWNhdGUAAAAGbWV0aG9kAAAAA2dldAAAAAZzdGF0dXMAAAAGMjAwIE9LAAAAB3ZlcnNpb24AAAAISFRUUC8xLjEAAAADdXJsAAAABnB1YmxpYwAAAApzZXQtY29va2llAAAACmtlZXAtYWxpdmUAAAAGb3JpZ2luMTAwMTAxMjAxMjAyMjA1MjA2MzAwMzAyMzAzMzA0MzA1MzA2MzA3NDAyNDA1NDA2NDA3NDA4NDA5NDEwNDExNDEyNDEzNDE0NDE1NDE2NDE3NTAyNTA0NTA1MjAzIE5vbi1BdXRob3JpdGF0aXZlIEluZm9ybWF0aW9uMjA0IE5vIENvbnRlbnQzMDEgTW92ZWQgUGVybWFuZW50bHk0MDAgQmFkIFJlcXVlc3Q0MDEgVW5hdXRob3JpemVkNDAzIEZvcmJpZGRlbjQwNCBOb3QgRm91bmQ1MDAgSW50ZXJuYWwgU2VydmVyIEVycm9yNTAxIE5vdCBJbXBsZW1lbnRlZDUwMyBTZXJ2aWNlIFVuYXZhaWxhYmxlSmFuIEZlYiBNYXIgQXByIE1heSBKdW4gSnVsIEF1ZyBTZXB0IE9jdCBOb3YgRGVjIDAwOjAwOjAwIE1vbiwgVHVlLCBXZWQsIFRodSwgRnJpLCBTYXQsIFN1biwgR01UY2h1bmtlZCx0ZXh0L2h0bWwsaW1hZ2UvcG5nLGltYWdlL2pwZyxpbWFnZS9naWYsYXBwbGljYXRpb24veG1sLGFwcGxpY2F0aW9uL3hodG1sK3htbCx0ZXh0L3BsYWluLHRleHQvamF2YXNjcmlwdCxwdWJsaWNwcml2YXRlbWF4LWFnZT1nemlwLGRlZmxhdGUsc2RjaGNoYXJzZXQ9dXRmLThjaGFyc2V0PWlzby04ODU5LTEsdXRmLSwqLGVucT0wLg==
    """
}

private extension Data {
    mutating func runeAppendUInt16(_ value: UInt16) {
        append(UInt8((value >> 8) & 0xff))
        append(UInt8(value & 0xff))
    }

    mutating func runeAppendUInt24(_ value: Int) {
        append(UInt8((value >> 16) & 0xff))
        append(UInt8((value >> 8) & 0xff))
        append(UInt8(value & 0xff))
    }

    mutating func runeAppendUInt32(_ value: UInt32) {
        append(UInt8((value >> 24) & 0xff))
        append(UInt8((value >> 16) & 0xff))
        append(UInt8((value >> 8) & 0xff))
        append(UInt8(value & 0xff))
    }

    func runeReadUInt16(at offset: Int) -> UInt16 {
        (UInt16(self[offset]) << 8) | UInt16(self[offset + 1])
    }

    func runeReadUInt24(at offset: Int) -> Int {
        (Int(self[offset]) << 16) | (Int(self[offset + 1]) << 8) | Int(self[offset + 2])
    }

    func runeReadUInt32(at offset: Int) -> UInt32 {
        (UInt32(self[offset]) << 24)
            | (UInt32(self[offset + 1]) << 16)
            | (UInt32(self[offset + 2]) << 8)
            | UInt32(self[offset + 3])
    }
}

private extension String {
    var runePercentEncodedPathSegment: String {
        addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? self
    }

    func trimmingTrailingSlashes() -> String {
        var value = self
        while value.count > 1, value.hasSuffix("/") {
            value.removeLast()
        }
        return value
    }
}
