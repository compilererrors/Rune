import Foundation
import RuneCore
import RuneDiagnostics
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

    init() {}

    static func _testCreateClientTLSIdentity(certificateData: Data, keyData: Data) throws -> Bool {
        try ClientTLSIdentity.temporaryIdentity(certificateData: certificateData, keyData: keyData) != nil
    }

    static func _testResolvedTLSDescription(environment: [String: String], contextName: String) async throws -> String {
        let resolved = try await KubernetesRESTClient().resolvedContext(environment: environment, contextName: contextName)
        return resolved.tlsDescription
    }

    static func _testLocalPortConflictMessage(port: Int, address: String) -> String? {
        localPortConflictMessage(port: port, address: address)
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
        items.append(URLQueryItem(name: "allContainers", value: "true"))
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
        let sanitizedYAML = Self.serverSideApplyYAML(from: yaml)
        let manifest = try YAMLManifestIdentity.parse(yaml: sanitizedYAML, defaultNamespace: defaultNamespace)
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
        if resolved.insecureSkipTLSVerify || resolved.serverURL.scheme == "http" {
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

    private static func localPortConflictMessage(port: Int, address: String) -> String? {
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
        let resolved = try await resolvedContext(environment: environment, contextName: contextName)
        VerboseKubeTrace.append(
            "k8s.request",
            "start method=\(method) context=\(contextName) path=\(apiPath) server=\(resolved.serverURL.host ?? resolved.serverURL.absoluteString) tls=\(resolved.tlsDescription) auth=\(resolved.authentication.traceDescription) kubeconfigs=\(VerboseKubeTrace.kubeconfigSummary(environment))"
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

        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await restSession.session.data(for: request)
        } catch {
            VerboseKubeTrace.append(
                "k8s.request",
                "failed method=\(method) context=\(contextName) path=\(apiPath) error=\(networkErrorMessage(error, resolved: resolved, tlsFailure: restSession.delegate.lastTLSFailure()))"
            )
            throw RuneError.commandFailed(
                command: "kubernetes REST \(method) \(apiPath)",
                message: networkErrorMessage(error, resolved: resolved, tlsFailure: restSession.delegate.lastTLSFailure())
            )
        }
        guard let http = response as? HTTPURLResponse else {
            throw RuneError.commandFailed(command: "kubernetes REST \(method) \(apiPath)", message: "Missing HTTP response")
        }

        let responseBody = String(data: data, encoding: .utf8) ?? ""
        VerboseKubeTrace.append(
            "k8s.request",
            "response method=\(method) context=\(contextName) path=\(apiPath) status=\(http.statusCode)"
        )
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
        let cacheKey = NormalizedKubeConfig.cacheKey(environment: environment)
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

        let credentials = try await resolveCredentials(
            user: namedUser?.user,
            cluster: namedCluster.cluster,
            environment: environment
        )
        return ResolvedRESTContext(
            serverURL: serverURL,
            namespace: namedContext.context.namespace,
            authentication: credentials.authentication,
            insecureSkipTLSVerify: namedCluster.cluster.insecureSkipTLSVerify ?? false,
            certificateAuthorityData: try namedCluster.cluster.resolvedCertificateAuthorityData(),
            tlsServerName: namedCluster.cluster.tlsServerName,
            clientTLSIdentity: credentials.clientTLSIdentity
        )
    }

    private func resolveCredentials(
        user: NormalizedKubeConfig.NamedUser.UserEntry?,
        cluster: NormalizedKubeConfig.NamedCluster.ClusterEntry,
        environment: [String: String]
    ) async throws -> RESTCredentialResolution {
        guard let user else { return RESTCredentialResolution(authentication: .none, clientTLSIdentity: nil) }

        if let token = user.token?.trimmingCharacters(in: .whitespacesAndNewlines), !token.isEmpty {
            return RESTCredentialResolution(
                authentication: .bearer(token),
                clientTLSIdentity: nil
            )
        }

        if let tokenFile = user.tokenFile?.trimmingCharacters(in: .whitespacesAndNewlines),
           !tokenFile.isEmpty,
           let token = try? String(contentsOfFile: tokenFile, encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines),
           !token.isEmpty {
            return RESTCredentialResolution(
                authentication: .bearer(token),
                clientTLSIdentity: nil
            )
        }

        if let exec = user.exec {
            return try await resolveExecCredentials(exec: exec, cluster: cluster, environment: environment)
        }

        if let username = user.username,
           let password = user.password,
           !username.isEmpty {
            return RESTCredentialResolution(
                authentication: .basic(username: username, password: password),
                clientTLSIdentity: nil
            )
        }

        return RESTCredentialResolution(
            authentication: .none,
            clientTLSIdentity: try user.resolvedClientTLSIdentityIfAvailable()
        )
    }

    private func resolveExecCredentials(
        exec: NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig,
        cluster: NormalizedKubeConfig.NamedCluster.ClusterEntry,
        environment: [String: String]
    ) async throws -> RESTCredentialResolution {
        let key = exec.cacheKey(environment: environment)
        if let cached = await execCredentialCache.credential(for: key) {
            return RESTCredentialResolution(authentication: .bearer(cached.token), clientTLSIdentity: nil)
        }

        let response = try await runExecCredential(exec: exec, cluster: cluster, environment: environment, timeout: 25)
        guard let status = response.status else {
            throw RuneError.invalidInput(message: "Kubeconfig exec auth response is missing status")
        }

        if let token = status.token?.trimmingCharacters(in: .whitespacesAndNewlines), !token.isEmpty {
            let expiresAt = status.expirationTimestamp.flatMap(Self.parseExecCredentialExpiration(_:))
            await execCredentialCache.setCredential(KubernetesExecCredential(token: token, expiresAt: expiresAt), for: key)
            return RESTCredentialResolution(authentication: .bearer(token), clientTLSIdentity: nil)
        }

        if let certificateData = status.decodedClientCertificateData,
           let keyData = status.decodedClientKeyData {
            let identity = try ClientTLSIdentity.temporaryIdentity(certificateData: certificateData, keyData: keyData)
            return RESTCredentialResolution(authentication: .none, clientTLSIdentity: identity)
        }

        if status.clientCertificateData != nil || status.clientKeyData != nil {
            throw RuneError.invalidInput(message: "Kubeconfig exec auth returned incomplete client certificate credentials")
        }

        return RESTCredentialResolution(authentication: .none, clientTLSIdentity: nil)
    }

    private func runExecCredential(
        exec: NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig,
        cluster: NormalizedKubeConfig.NamedCluster.ClusterEntry,
        environment: [String: String],
        timeout: TimeInterval
    ) async throws -> ExecCredentialResponse {
        let output = try await runProcess(
            command: exec.command,
            arguments: exec.args ?? [],
            environment: try exec.processEnvironment(base: environment, execInfo: execInfo(for: exec, cluster: cluster)),
            timeout: timeout
        )
        do {
            let response = try JSONDecoder().decode(ExecCredentialResponse.self, from: output)
            let expectedAPIVersion = exec.apiVersion ?? "client.authentication.k8s.io/v1"
            if let apiVersion = response.apiVersion, apiVersion != expectedAPIVersion {
                throw RuneError.parseError(
                    message: "Kubeconfig exec auth returned apiVersion \(apiVersion), expected \(expectedAPIVersion)"
                )
            }
            return response
        } catch {
            if let runeError = error as? RuneError {
                throw runeError
            }
            let preview = String(decoding: output.prefix(512), as: UTF8.self)
            throw RuneError.parseError(message: "Kubeconfig exec auth response is not a valid ExecCredential JSON document: \(preview)")
        }
    }

    private func execInfo(
        for exec: NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig,
        cluster: NormalizedKubeConfig.NamedCluster.ClusterEntry
    ) throws -> String? {
        guard exec.provideClusterInfo == true else { return nil }
        var clusterInfo: [String: Any] = ["server": cluster.server]
        if let data = try cluster.resolvedCertificateAuthorityData() {
            clusterInfo["certificate-authority-data"] = data.base64EncodedString()
        }
        if let insecure = cluster.insecureSkipTLSVerify {
            clusterInfo["insecure-skip-tls-verify"] = insecure
        }
        if let tlsServerName = cluster.tlsServerName {
            clusterInfo["tls-server-name"] = tlsServerName
        }
        let payload: [String: Any] = [
            "apiVersion": exec.apiVersion ?? "client.authentication.k8s.io/v1",
            "kind": "ExecCredential",
            "spec": [
                "interactive": false,
                "cluster": clusterInfo
            ]
        ]
        let data = try JSONSerialization.data(withJSONObject: payload, options: [.sortedKeys])
        return String(decoding: data, as: UTF8.self)
    }

    private func runProcess(
        command: String,
        arguments: [String],
        environment: [String: String],
        timeout: TimeInterval
    ) async throws -> Data {
        try await Task.detached(priority: .utility) {
            let process = Process()
            let stdout = Pipe()
            let stderr = Pipe()

            let expandedCommand = NSString(string: command).expandingTildeInPath
            if expandedCommand.contains("/") {
                process.executableURL = URL(fileURLWithPath: expandedCommand)
                process.arguments = arguments
            } else {
                process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
                process.arguments = [expandedCommand] + arguments
            }
            process.environment = environment
            process.standardOutput = stdout
            process.standardError = stderr

            do {
                try process.run()
            } catch {
                throw RuneError.commandFailed(
                    command: "kubeconfig exec auth \(command)",
                    message: error.localizedDescription
                )
            }

            let deadline = Date().addingTimeInterval(timeout)
            while process.isRunning, Date() < deadline {
                try await Task.sleep(nanoseconds: 25_000_000)
            }
            if process.isRunning {
                process.terminate()
                process.waitUntilExit()
                throw RuneError.commandFailed(
                    command: "kubeconfig exec auth \(command)",
                    message: "Timed out after \(Int(timeout)) seconds"
                )
            }

            let output = stdout.fileHandleForReading.readDataToEndOfFile()
            let errorData = stderr.fileHandleForReading.readDataToEndOfFile()
            guard process.terminationStatus == 0 else {
                let message = String(decoding: errorData.isEmpty ? output : errorData, as: UTF8.self)
                throw RuneError.commandFailed(
                    command: "kubeconfig exec auth \(command)",
                    message: message.trimmingCharacters(in: .whitespacesAndNewlines)
                )
            }
            return output
        }.value
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

private actor KubernetesExecCredentialCache {
    private var byKey: [String: KubernetesExecCredential] = [:]

    func credential(for key: String) -> KubernetesExecCredential? {
        guard let credential = byKey[key] else { return nil }
        if let expiresAt = credential.expiresAt, expiresAt <= Date().addingTimeInterval(30) {
            byKey.removeValue(forKey: key)
            return nil
        }
        return credential
    }

    func setCredential(_ credential: KubernetesExecCredential, for key: String) {
        byKey[key] = credential
    }
}

private struct KubernetesExecCredential: Sendable {
    let token: String
    let expiresAt: Date?
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

    struct NamedUser: Decodable {
        struct UserEntry: Decodable {
            struct ExecConfig: Decodable {
                struct EnvironmentEntry: Decodable {
                    let name: String
                    let value: String
                }

                let apiVersion: String?
                let command: String
                let args: [String]?
                let env: [EnvironmentEntry]?
                let provideClusterInfo: Bool?
                let interactiveMode: String?

                enum CodingKeys: String, CodingKey {
                    case apiVersion
                    case command
                    case args
                    case env
                    case provideClusterInfo = "provideClusterInfo"
                    case interactiveMode
                }

                func processEnvironment(base: [String: String], execInfo: String?) -> [String: String] {
                    var output = ProcessInfo.processInfo.environment
                    for (key, value) in base {
                        output[key] = value
                    }
                    for entry in env ?? [] {
                        output[entry.name] = entry.value
                    }
                    if let execInfo {
                        output["KUBERNETES_EXEC_INFO"] = execInfo
                    }
                    return output
                }

                func cacheKey(environment: [String: String]) -> String {
                    let envKey = (env ?? [])
                        .map { "\($0.name)=\($0.value)" }
                        .sorted()
                        .joined(separator: "\u{1f}")
                    return [
                        apiVersion ?? "",
                        command,
                        (args ?? []).joined(separator: "\u{1e}"),
                        envKey,
                        provideClusterInfo == true ? "cluster" : "",
                        interactiveMode ?? "",
                        environment["KUBECONFIG"] ?? ""
                    ].joined(separator: "\u{1d}")
                }
            }

            let token: String?
            let tokenFile: String?
            let username: String?
            let password: String?
            let exec: ExecConfig?
            let clientCertificateData: String?
            let clientKeyData: String?
            let clientCertificate: String?
            let clientKey: String?

            enum CodingKeys: String, CodingKey {
                case token
                case tokenFile = "tokenFile"
                case username
                case password
                case exec
                case clientCertificateData = "client-certificate-data"
                case clientKeyData = "client-key-data"
                case clientCertificate = "client-certificate"
                case clientKey = "client-key"
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

            func optionalClientTLSIdentity() -> ClientTLSIdentity? {
                try? resolvedClientTLSIdentityIfAvailable()
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
            let baseDirectory = URL(fileURLWithPath: expanded).deletingLastPathComponent().path
            let config = try DirectKubeConfigParser(raw: raw, baseDirectory: baseDirectory).parse()
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

    static func cacheKey(environment: [String: String]) -> String {
        kubeconfigPaths(environment: environment)
            .map { path in
                let expanded = NSString(string: path).expandingTildeInPath
                let attributes = try? FileManager.default.attributesOfItem(atPath: expanded)
                let modified = (attributes?[.modificationDate] as? Date)?.timeIntervalSince1970 ?? 0
                let size = attributes?[.size] as? NSNumber
                return "\(expanded):\(modified):\(size?.int64Value ?? -1)"
            }
            .joined(separator: "\u{1d}")
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
        var clientCertificate: String?
        var clientKey: String?
        var execCommand: String?
        var execAPIVersion: String?
        var execArgs: [String] = []
        var execEnv: [NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig.EnvironmentEntry] = []
        var execProvideClusterInfo: Bool?
        var execInteractiveMode: String?

        func build() -> NormalizedKubeConfig.NamedUser? {
            guard !name.isEmpty else { return nil }
            let exec = execCommand.map {
                NormalizedKubeConfig.NamedUser.UserEntry.ExecConfig(
                    apiVersion: execAPIVersion,
                    command: $0,
                    args: execArgs.isEmpty ? nil : execArgs,
                    env: execEnv.isEmpty ? nil : execEnv,
                    provideClusterInfo: execProvideClusterInfo,
                    interactiveMode: execInteractiveMode
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
                    clientKeyData: clientKeyData,
                    clientCertificate: clientCertificate,
                    clientKey: clientKey
                )
            )
        }
    }

    let raw: String
    let baseDirectory: String?

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
        if let value = scalarValue(line, key: "certificate-authority") { cluster?.certificateAuthority = resolvedPath(value) }
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
        } else if let value = scalarValue(line, key: "apiVersion"), subsection.hasPrefix("exec") {
            user?.execAPIVersion = value
        } else if let value = scalarValue(line, key: "provideClusterInfo"), subsection.hasPrefix("exec") {
            user?.execProvideClusterInfo = parseBool(value)
        } else if let value = scalarValue(line, key: "interactiveMode"), subsection.hasPrefix("exec") {
            user?.execInteractiveMode = value
        } else if let value = scalarValue(line, key: "token") {
            user?.token = value
        } else if let value = scalarValue(line, key: "tokenFile") ?? scalarValue(line, key: "token-file") {
            user?.tokenFile = resolvedPath(value)
        } else if let value = scalarValue(line, key: "username") {
            user?.username = value
        } else if let value = scalarValue(line, key: "password") {
            user?.password = value
        } else if let value = scalarValue(line, key: "client-certificate-data") {
            user?.clientCertificateData = value
        } else if let value = scalarValue(line, key: "client-key-data") {
            user?.clientKeyData = value
        } else if let value = scalarValue(line, key: "client-certificate") {
            user?.clientCertificate = resolvedPath(value)
        } else if let value = scalarValue(line, key: "client-key") {
            user?.clientKey = resolvedPath(value)
        }
    }

    private func resolvedPath(_ path: String) -> String {
        guard !path.isEmpty,
              !path.hasPrefix("/"),
              !path.hasPrefix("~"),
              let baseDirectory else {
            return path
        }
        return URL(fileURLWithPath: baseDirectory).appendingPathComponent(path).path
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
    let authentication: RESTAuthentication
    let insecureSkipTLSVerify: Bool
    let certificateAuthorityData: Data?
    let tlsServerName: String?
    let clientTLSIdentity: ClientTLSIdentity?
}

private enum RESTAuthentication {
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

private struct RESTCredentialResolution {
    let authentication: RESTAuthentication
    let clientTLSIdentity: ClientTLSIdentity?
}

private struct RESTResponse {
    let body: String
    let contentType: String
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
            if resolved.insecureSkipTLSVerify {
                sec_protocol_options_set_verify_block(
                    tlsOptions.securityProtocolOptions,
                    { _, _, complete in
                        complete(true)
                    },
                    queue
                )
            }
            parameters = NWParameters(tls: tlsOptions, tcp: NWProtocolTCP.Options())
        }

        return NWConnection(host: NWEndpoint.Host(host), port: port, using: parameters)
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
}
