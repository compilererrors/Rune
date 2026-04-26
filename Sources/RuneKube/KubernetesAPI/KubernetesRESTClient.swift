import Foundation
import RuneCore
import Security

final class KubernetesRESTClient: @unchecked Sendable {
    private let configCache = KubernetesRESTConfigCache()

    init() {}

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
        timeout: TimeInterval,
        profile: LogQueryProfile = .pod
    ) async throws -> String {
        let query = filter.resolvedLogQuery(profile: profile)
        var items: [URLQueryItem] = []
        switch filter {
        case .all:
            if let tailLines = query.tailLines {
                items.append(URLQueryItem(name: "tailLines", value: String(tailLines)))
            }
        case let .tailLines(lines):
            items.append(URLQueryItem(name: "tailLines", value: String(max(1, lines))))
        case .lastMinutes, .lastHours, .lastDays:
            if let since = query.since {
                items.append(URLQueryItem(name: "sinceSeconds", value: String(max(1, parseDurationSeconds(from: since)))))
            }
            if let tailLines = query.tailLines {
                items.append(URLQueryItem(name: "tailLines", value: String(tailLines)))
            }
        case let .since(date):
            items.append(URLQueryItem(name: "sinceTime", value: ISO8601DateFormatter().string(from: date)))
            if let tailLines = query.tailLines {
                items.append(URLQueryItem(name: "tailLines", value: String(tailLines)))
            }
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

    func applyYAML(
        environment: [String: String],
        contextName: String,
        defaultNamespace: String,
        yaml: String,
        dryRun: Bool,
        timeout: TimeInterval
    ) async throws {
        let manifest = try YAMLManifestIdentity.parse(yaml: yaml, defaultNamespace: defaultNamespace)
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
            body: yaml,
            timeout: timeout
        )
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
        timeout: TimeInterval
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
        let path = try resourcePath(kind: .deployment, namespace: namespace, resource: "deployments", name: deploymentName, subresource: nil)
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
        let session = makeSession(for: resolved)
        defer { session.invalidateAndCancel() }

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
            session: session,
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
        let session = makeSession(for: resolved)
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
            session: session,
            resolved: resolved,
            apiPath: components.percentEncodedPath + (components.percentEncodedQuery.map { "?\($0)" } ?? "")
        )
        let handle = KubernetesExecWebSocketHandle(
            task: task,
            session: session,
            onOutput: onOutput,
            onTermination: onTermination
        )
        task.resume()
        handle.startReceiving()
        return handle
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
        let resolved = try await resolvedContext(environment: environment, contextName: contextName)
        let session = makeSession(for: resolved)
        defer { session.invalidateAndCancel() }

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

    private func makeWebSocketTask(
        session: URLSession,
        resolved: ResolvedRESTContext,
        apiPath: String
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
        request.setValue("v5.channel.k8s.io, v4.channel.k8s.io", forHTTPHeaderField: "Sec-WebSocket-Protocol")
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

        let config = try NormalizedKubeConfig.loadDirectly(environment: environment)
        await configCache.setConfig(config, for: cacheKey)
        return config
    }

    private func resolvedContext(environment: [String: String], contextName: String) async throws -> ResolvedRESTContext {
        let config = try await normalizedConfig(environment: environment)
        guard let namedContext = config.contexts.first(where: { $0.name == contextName }) else {
            throw RuneError.invalidInput(message: "Kubernetes context \(contextName) is missing from kubeconfig")
        }
        guard let namedCluster = config.clusters.first(where: { $0.name == namedContext.context.cluster }) else {
            throw RuneError.invalidInput(message: "Cluster \(namedContext.context.cluster) is missing from kubeconfig")
        }
        let namedUser = config.users.first(where: { $0.name == namedContext.context.user })

        guard let serverURL = URL(string: namedCluster.cluster.server) else {
            throw RuneError.invalidInput(message: "Invalid Kubernetes server URL for context \(contextName)")
        }

        let authentication = try await resolveAuthentication(user: namedUser?.user, environment: environment)
        return ResolvedRESTContext(
            serverURL: serverURL,
            namespace: namedContext.context.namespace,
            authentication: authentication,
            insecureSkipTLSVerify: namedCluster.cluster.insecureSkipTLSVerify ?? false,
            certificateAuthorityData: try namedCluster.cluster.resolvedCertificateAuthorityData(),
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
            throw RuneError.invalidInput(
                message: "Kubeconfig exec auth is not available in the native runtime yet: \(exec.command)"
            )
        }

        if let username = user.username,
           let password = user.password,
           !username.isEmpty {
            return .basic(username: username, password: password)
        }

        if user.clientCertificateData != nil || user.clientKeyData != nil {
            throw RuneError.invalidInput(message: "Client certificate auth is not supported yet in the Rune REST transport")
        }

        return .none
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
            let certificateAuthority: String?

            enum CodingKeys: String, CodingKey {
                case server
                case insecureSkipTLSVerify = "insecure-skip-tls-verify"
                case certificateAuthorityData = "certificate-authority-data"
                case certificateAuthority = "certificate-authority"
                case tlsServerName = "tls-server-name"
            }

            func resolvedCertificateAuthorityData() throws -> Data? {
                if let certificateAuthorityData {
                    return Data(base64Encoded: certificateAuthorityData)
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

    static func loadDirectly(environment: [String: String]) throws -> NormalizedKubeConfig {
        let paths = kubeconfigPaths(environment: environment)
        guard !paths.isEmpty else { throw RuneError.missingKubeConfig }

        var mergedContexts: [NamedContext] = []
        var mergedClusters: [NamedCluster] = []
        var mergedUsers: [NamedUser] = []
        var currentContext: String?

        for path in paths {
            let expanded = NSString(string: path).expandingTildeInPath
            guard FileManager.default.fileExists(atPath: expanded) else { continue }
            let raw = try String(contentsOfFile: expanded, encoding: .utf8)
            let config = try DirectKubeConfigParser(raw: raw).parse()
            mergedContexts.append(contentsOf: config.contexts)
            mergedClusters.append(contentsOf: config.clusters)
            mergedUsers.append(contentsOf: config.users)
            if let current = config.currentContext, !current.isEmpty {
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

    private static func kubeconfigPaths(environment: [String: String]) -> [String] {
        if let kubeconfig = environment["KUBECONFIG"], !kubeconfig.isEmpty {
            return kubeconfig.split(separator: ":").map(String.init).filter { !$0.isEmpty }
        }
        return ["~/.kube/config"]
    }

    private static func deduplicateByName<T>(_ values: [T], name: (T) -> String) -> [T] {
        var seen = Set<String>()
        var output: [T] = []
        for value in values.reversed() where seen.insert(name(value)).inserted {
            output.append(value)
        }
        return output.reversed()
    }
}

private struct DirectKubeConfigParser {
    private enum Section {
        case none
        case clusters
        case contexts
        case users
    }

    private struct MutableCluster {
        var name = ""
        var server = ""
        var insecureSkipTLSVerify: Bool?
        var certificateAuthorityData: String?
        var certificateAuthority: String?
        var tlsServerName: String?

        func build() -> NormalizedKubeConfig.NamedCluster? {
            guard !name.isEmpty, !server.isEmpty else { return nil }
            return NormalizedKubeConfig.NamedCluster(
                name: name,
                cluster: NormalizedKubeConfig.NamedCluster.ClusterEntry(
                    server: server,
                    insecureSkipTLSVerify: insecureSkipTLSVerify,
                    certificateAuthorityData: certificateAuthorityData,
                    tlsServerName: tlsServerName,
                    certificateAuthority: certificateAuthority
                )
            )
        }
    }

    private struct MutableContext {
        var name = ""
        var cluster = ""
        var user = ""
        var namespace: String?

        func build() -> NormalizedKubeConfig.NamedContext? {
            guard !name.isEmpty, !cluster.isEmpty else { return nil }
            return NormalizedKubeConfig.NamedContext(
                name: name,
                context: NormalizedKubeConfig.NamedContext.ContextEntry(
                    cluster: cluster,
                    user: user,
                    namespace: namespace
                )
            )
        }
    }

    private struct MutableUser {
        var name = ""
        var token: String?
        var tokenFile: String?
        var username: String?
        var password: String?
        var clientCertificateData: String?
        var clientKeyData: String?
        var execCommand: String?
        var execArgs: [String] = []
        var execEnv: [NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig.EnvironmentEntry] = []

        func build() -> NormalizedKubeConfig.NamedUser? {
            guard !name.isEmpty else { return nil }
            let exec = execCommand.map {
                NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig(
                    command: $0,
                    args: execArgs.isEmpty ? nil : execArgs,
                    env: execEnv.isEmpty ? nil : execEnv
                )
            }
            return NormalizedKubeConfig.NamedUser(
                name: name,
                user: NormalizedKubeConfig.NamedUser.UserEntry(
                    token: token,
                    tokenFile: tokenFile,
                    username: username,
                    password: password,
                    exec: exec,
                    clientCertificateData: clientCertificateData,
                    clientKeyData: clientKeyData
                )
            )
        }
    }

    let raw: String

    func parse() throws -> NormalizedKubeConfig {
        var currentContext: String?
        var section: Section = .none
        var clusters: [NormalizedKubeConfig.NamedCluster] = []
        var contexts: [NormalizedKubeConfig.NamedContext] = []
        var users: [NormalizedKubeConfig.NamedUser] = []
        var cluster: MutableCluster?
        var context: MutableContext?
        var user: MutableUser?
        var userSubsection = ""
        var pendingEnvName: String?

        func flushCluster() {
            if let built = cluster?.build() { clusters.append(built) }
            cluster = nil
        }
        func flushContext() {
            if let built = context?.build() { contexts.append(built) }
            context = nil
        }
        func flushUser() {
            if let built = user?.build() { users.append(built) }
            user = nil
            userSubsection = ""
            pendingEnvName = nil
        }

        for originalLine in raw.split(omittingEmptySubsequences: false, whereSeparator: \.isNewline).map(String.init) {
            let line = stripInlineComment(originalLine)
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard !trimmed.isEmpty else { continue }
            let indent = line.prefix { $0 == " " }.count

            if indent == 0, !trimmed.hasPrefix("-") {
                switch trimmed {
                case "clusters:":
                    flushContext(); flushUser(); section = .clusters
                case "contexts:":
                    flushCluster(); flushUser(); section = .contexts
                case "users:":
                    flushCluster(); flushContext(); section = .users
                default:
                    if let value = scalarValue(trimmed, key: "current-context") {
                        currentContext = value
                    }
                }
                continue
            }

            switch section {
            case .clusters:
                if trimmed.hasPrefix("- ") {
                    flushCluster()
                    cluster = MutableCluster()
                    applyClusterLine(trimmed.dropFirst(2).description, to: &cluster)
                } else {
                    applyClusterLine(trimmed, to: &cluster)
                }
            case .contexts:
                if trimmed.hasPrefix("- ") {
                    flushContext()
                    context = MutableContext()
                    applyContextLine(trimmed.dropFirst(2).description, to: &context)
                } else {
                    applyContextLine(trimmed, to: &context)
                }
            case .users:
                if trimmed.hasPrefix("- ") {
                    flushUser()
                    user = MutableUser()
                    applyUserLine(trimmed.dropFirst(2).description, indent: indent, subsection: &userSubsection, pendingEnvName: &pendingEnvName, to: &user)
                } else {
                    applyUserLine(trimmed, indent: indent, subsection: &userSubsection, pendingEnvName: &pendingEnvName, to: &user)
                }
            case .none:
                continue
            }
        }

        flushCluster()
        flushContext()
        flushUser()

        return NormalizedKubeConfig(
            currentContext: currentContext,
            contexts: contexts,
            clusters: clusters,
            users: users
        )
    }

    private func applyClusterLine(_ line: String, to cluster: inout MutableCluster?) {
        if cluster == nil { cluster = MutableCluster() }
        if let value = scalarValue(line, key: "name") { cluster?.name = value }
        if let value = scalarValue(line, key: "server") { cluster?.server = value }
        if let value = scalarValue(line, key: "insecure-skip-tls-verify") { cluster?.insecureSkipTLSVerify = parseBool(value) }
        if let value = scalarValue(line, key: "certificate-authority-data") { cluster?.certificateAuthorityData = value }
        if let value = scalarValue(line, key: "certificate-authority") { cluster?.certificateAuthority = value }
        if let value = scalarValue(line, key: "tls-server-name") { cluster?.tlsServerName = value }
    }

    private func applyContextLine(_ line: String, to context: inout MutableContext?) {
        if context == nil { context = MutableContext() }
        if let value = scalarValue(line, key: "name") { context?.name = value }
        if let value = scalarValue(line, key: "cluster") { context?.cluster = value }
        if let value = scalarValue(line, key: "user") { context?.user = value }
        if let value = scalarValue(line, key: "namespace") { context?.namespace = value }
    }

    private func applyUserLine(
        _ line: String,
        indent: Int,
        subsection: inout String,
        pendingEnvName: inout String?,
        to user: inout MutableUser?
    ) {
        if user == nil { user = MutableUser() }
        if let value = scalarValue(line, key: "name"), indent <= 2 {
            user?.name = value
            return
        }
        if line == "exec:" {
            subsection = "exec"
            return
        }
        if line == "args:" {
            subsection = "exec.args"
            return
        }
        if line == "env:" {
            subsection = "exec.env"
            return
        }
        if line.hasPrefix("- ") {
            let value = parseScalar(String(line.dropFirst(2)))
            if subsection == "exec.args" {
                user?.execArgs.append(value)
            } else if subsection == "exec.env", let name = scalarValue(String(line.dropFirst(2)), key: "name") {
                pendingEnvName = name
            }
            return
        }

        if subsection == "exec.env" {
            if let name = scalarValue(line, key: "name") {
                pendingEnvName = name
                return
            }
            if let value = scalarValue(line, key: "value"), let name = pendingEnvName {
                user?.execEnv.append(.init(name: name, value: value))
                pendingEnvName = nil
                return
            }
        }

        if let value = scalarValue(line, key: "command"), subsection.hasPrefix("exec") {
            user?.execCommand = value
        } else if let value = scalarValue(line, key: "token") {
            user?.token = value
        } else if let value = scalarValue(line, key: "tokenFile") ?? scalarValue(line, key: "token-file") {
            user?.tokenFile = value
        } else if let value = scalarValue(line, key: "username") {
            user?.username = value
        } else if let value = scalarValue(line, key: "password") {
            user?.password = value
        } else if let value = scalarValue(line, key: "client-certificate-data") {
            user?.clientCertificateData = value
        } else if let value = scalarValue(line, key: "client-key-data") {
            user?.clientKeyData = value
        }
    }

    private func scalarValue(_ line: String, key: String) -> String? {
        let prefix = "\(key):"
        guard line.hasPrefix(prefix) else { return nil }
        return parseScalar(String(line.dropFirst(prefix.count)))
    }

    private func parseScalar(_ raw: String) -> String {
        let trimmed = raw.trimmingCharacters(in: .whitespaces)
        if trimmed.count >= 2,
           let first = trimmed.first,
           let last = trimmed.last,
           (first == "\"" && last == "\"") || (first == "'" && last == "'") {
            return String(trimmed.dropFirst().dropLast())
        }
        return trimmed
    }

    private func parseBool(_ raw: String) -> Bool? {
        switch raw.lowercased() {
        case "true", "yes", "1": return true
        case "false", "no", "0": return false
        default: return nil
        }
    }

    private func stripInlineComment(_ line: String) -> String {
        var inSingle = false
        var inDouble = false
        for index in line.indices {
            let char = line[index]
            if char == "'", !inDouble { inSingle.toggle() }
            if char == "\"", !inSingle { inDouble.toggle() }
            if char == "#", !inSingle, !inDouble {
                if index == line.startIndex || line[line.index(before: index)] == " " {
                    return String(line[..<index])
                }
            }
        }
        return line
    }
}

private struct YAMLManifestIdentity {
    let apiVersion: String
    let kind: KubeResourceKind
    let namespace: String
    let name: String

    static func parse(yaml: String, defaultNamespace: String) throws -> YAMLManifestIdentity {
        var apiVersion: String?
        var rawKind: String?
        var metadataIndent: Int?
        var name: String?
        var namespace: String?

        for originalLine in yaml.split(omittingEmptySubsequences: false, whereSeparator: \.isNewline).map(String.init) {
            let line = stripInlineComment(originalLine)
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard !trimmed.isEmpty, trimmed != "---" else { continue }
            let indent = line.prefix { $0 == " " }.count
            if indent == 0 {
                if let value = scalarValue(trimmed, key: "apiVersion") { apiVersion = value }
                if let value = scalarValue(trimmed, key: "kind") { rawKind = value }
                if trimmed == "metadata:" {
                    metadataIndent = indent
                } else if metadataIndent != nil, !trimmed.hasPrefix("-") {
                    metadataIndent = nil
                }
                continue
            }
            if metadataIndent != nil, indent > (metadataIndent ?? 0) {
                if let value = scalarValue(trimmed, key: "name") { name = value }
                if let value = scalarValue(trimmed, key: "namespace") { namespace = value }
            }
        }

        guard let apiVersion, !apiVersion.isEmpty else {
            throw RuneError.parseError(message: "YAML manifest is missing apiVersion")
        }
        guard let rawKind, let kind = KubeResourceKind(manifestKind: rawKind) else {
            throw RuneError.parseError(message: "YAML manifest kind is not supported by Rune")
        }
        guard let name, !name.isEmpty else {
            throw RuneError.parseError(message: "YAML manifest is missing metadata.name")
        }
        return YAMLManifestIdentity(
            apiVersion: apiVersion,
            kind: kind,
            namespace: kind.isNamespaced ? (namespace?.isEmpty == false ? namespace! : defaultNamespace) : "",
            name: name
        )
    }

    private static func scalarValue(_ line: String, key: String) -> String? {
        let prefix = "\(key):"
        guard line.hasPrefix(prefix) else { return nil }
        let trimmed = String(line.dropFirst(prefix.count)).trimmingCharacters(in: .whitespaces)
        if trimmed.count >= 2,
           let first = trimmed.first,
           let last = trimmed.last,
           (first == "\"" && last == "\"") || (first == "'" && last == "'") {
            return String(trimmed.dropFirst().dropLast())
        }
        return trimmed
    }

    private static func stripInlineComment(_ line: String) -> String {
        var inSingle = false
        var inDouble = false
        for index in line.indices {
            let char = line[index]
            if char == "'", !inDouble { inSingle.toggle() }
            if char == "\"", !inSingle { inDouble.toggle() }
            if char == "#", !inSingle, !inDouble {
                if index == line.startIndex || line[line.index(before: index)] == " " {
                    return String(line[..<index])
                }
            }
        }
        return line
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
        case "ingress": self = .ingress
        case "configmap": self = .configMap
        case "secret": self = .secret
        case "node": self = .node
        case "event": self = .event
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
                NSLog("[Rune][KubernetesExec] stdin send failed: %@", String(describing: error))
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
