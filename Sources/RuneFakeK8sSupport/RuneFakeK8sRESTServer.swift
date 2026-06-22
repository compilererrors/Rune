import Foundation
import Network

public final class RuneFakeK8sRESTServer: @unchecked Sendable {
    public let port: UInt16

    private let listener: NWListener
    private let fixture: RuneFakeK8sFixture
    private let contextName: String
    private let queue: DispatchQueue
    private let requestRecorder: RuneFakeK8sRequestRecorder

    private init(
        listener: NWListener,
        port: UInt16,
        fixture: RuneFakeK8sFixture,
        contextName: String,
        requestRecorder: RuneFakeK8sRequestRecorder
    ) {
        self.listener = listener
        self.port = port
        self.fixture = fixture
        self.contextName = contextName
        self.queue = DispatchQueue(label: "rune.fake-k8s.rest-server")
        self.requestRecorder = requestRecorder
    }

    public static func start(
        host: String = "127.0.0.1",
        port: UInt16 = 0,
        fixture: RuneFakeK8sFixture = RuneFakeK8sFixture(),
        contextName: String = RuneFakeK8sFixture.defaultContextName
    ) async throws -> RuneFakeK8sRESTServer {
        try startBlocking(host: host, port: port, fixture: fixture, contextName: contextName)
    }

    public static func startBlocking(
        host: String = "127.0.0.1",
        port: UInt16 = 0,
        fixture: RuneFakeK8sFixture = RuneFakeK8sFixture(),
        contextName: String = RuneFakeK8sFixture.defaultContextName
    ) throws -> RuneFakeK8sRESTServer {
        let parameters = NWParameters.tcp
        _ = host
        let listener = try NWListener(using: parameters, on: NWEndpoint.Port(rawValue: port) ?? 0)
        let requestRecorder = RuneFakeK8sRequestRecorder()
        let serverBox = ServerBox(
            listener: listener,
            fixture: fixture,
            contextName: contextName,
            requestRecorder: requestRecorder
        )
        listener.newConnectionHandler = { connection in
            serverBox.receive(connection: connection)
        }

        let resultBox = ServerStartResultBox()
        listener.stateUpdateHandler = { state in
            switch state {
            case .ready:
                guard let port = listener.port?.rawValue else {
                    resultBox.resume(.failure(URLError(.cannotConnectToHost)))
                    return
                }
                resultBox.resume(.success(RuneFakeK8sRESTServer(
                    listener: listener,
                    port: port,
                    fixture: fixture,
                    contextName: contextName,
                    requestRecorder: requestRecorder
                )))
            case let .failed(error):
                resultBox.resume(.failure(error))
            default:
                break
            }
        }
        listener.start(queue: DispatchQueue(label: "rune.fake-k8s.rest-listener"))
        return try resultBox.wait()
    }

    public func stop() {
        listener.cancel()
    }

    public func requestLines() -> [String] {
        requestRecorder.snapshot()
    }

    public func resetRequestLines() {
        requestRecorder.removeAll()
    }

    public func kubeconfigYAML(serverURL: String? = nil) -> String {
        let endpoint = serverURL ?? "http://127.0.0.1:\(port)"
        return RuneFakeK8sKubeconfig.render(
            fixture: fixture,
            currentContext: contextName,
            serverURL: endpoint
        )
    }
}

private final class RuneFakeK8sRequestRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var lines: [String] = []

    func append(_ line: String) {
        lock.lock()
        lines.append(line)
        lock.unlock()
    }

    func snapshot() -> [String] {
        lock.lock()
        let value = lines
        lock.unlock()
        return value
    }

    func count(target: String) -> Int {
        lock.lock()
        let value = lines.filter { line in
            line.split(separator: " ", maxSplits: 2).dropFirst().first.map(String.init) == target
        }.count
        lock.unlock()
        return value
    }

    func removeAll() {
        lock.lock()
        lines.removeAll(keepingCapacity: true)
        lock.unlock()
    }
}

private final class ServerStartResultBox: @unchecked Sendable {
    private let lock = NSLock()
    private var resumed = false
    private var result: Result<RuneFakeK8sRESTServer, Error>?
    private let semaphore = DispatchSemaphore(value: 0)

    func resume(_ result: Result<RuneFakeK8sRESTServer, Error>) {
        lock.lock()
        guard !resumed else {
            lock.unlock()
            return
        }
        resumed = true
        self.result = result
        lock.unlock()
        semaphore.signal()
    }

    func wait() throws -> RuneFakeK8sRESTServer {
        if semaphore.wait(timeout: .now() + 5) == .timedOut {
            throw URLError(.timedOut)
        }
        lock.lock()
        let result = self.result
        lock.unlock()
        return try result!.get()
    }
}

public enum RuneFakeK8sKubeconfig {
    public static func render(
        fixture: RuneFakeK8sFixture = RuneFakeK8sFixture(),
        currentContext: String = RuneFakeK8sFixture.defaultContextName,
        serverURL: String
    ) -> String {
        let clusters = fixture.contexts.map { cluster in
            """
            - name: \(cluster.contextName)
              cluster:
                server: \(serverURL)
            """
        }.joined(separator: "\n")
        let contexts = fixture.contexts.map { cluster in
            """
            - name: \(cluster.contextName)
              context:
                cluster: \(cluster.contextName)
                namespace: \(cluster.defaultNamespace)
                user: fake-user
            """
        }.joined(separator: "\n")
        return """
        apiVersion: v1
        kind: Config
        current-context: \(currentContext)
        preferences: {}
        clusters:
        \(clusters)
        contexts:
        \(contexts)
        users:
        - name: fake-user
          user:
            token: fake-token
        """
    }
}

private final class ServerBox: @unchecked Sendable {
    private let listener: NWListener
    private let fixture: RuneFakeK8sFixture
    private let contextName: String
    private let requestRecorder: RuneFakeK8sRequestRecorder

    init(
        listener: NWListener,
        fixture: RuneFakeK8sFixture,
        contextName: String,
        requestRecorder: RuneFakeK8sRequestRecorder
    ) {
        self.listener = listener
        self.fixture = fixture
        self.contextName = contextName
        self.requestRecorder = requestRecorder
    }

    func receive(connection: NWConnection) {
        connection.start(queue: DispatchQueue(label: "rune.fake-k8s.rest-connection"))
        connection.receive(minimumIncompleteLength: 1, maximumLength: 128 * 1024) { [fixture, contextName, requestRecorder] data, _, _, _ in
            guard
                let data,
                let request = String(data: data, encoding: .utf8),
                let line = request.split(separator: "\r\n", maxSplits: 1).first
            else {
                connection.cancel()
                return
            }
            requestRecorder.append(String(line))
            let expectedBodyLength = Self.contentLength(in: request)
            let currentBody = Self.body(in: request)

            if expectedBodyLength > currentBody.utf8.count {
                connection.receive(
                    minimumIncompleteLength: expectedBodyLength - currentBody.utf8.count,
                    maximumLength: max(1, expectedBodyLength - currentBody.utf8.count)
                ) { moreData, _, _, _ in
                    let more = moreData.flatMap { String(data: $0, encoding: .utf8) } ?? ""
                    let body = Self.body(in: request + more)
                    let response = RuneFakeK8sRouter(fixture: fixture, contextName: contextName, requestRecorder: requestRecorder)
                        .route(requestLine: String(line), body: body.isEmpty ? nil : body)
                    connection.sendHTTP(response, delayNanoseconds: Self.responseDelayNanoseconds(for: String(line), fixture: fixture))
                }
            } else {
                let body = Self.body(in: request)
                let response = RuneFakeK8sRouter(fixture: fixture, contextName: contextName, requestRecorder: requestRecorder)
                    .route(requestLine: String(line), body: body.isEmpty ? nil : body)
                connection.sendHTTP(response, delayNanoseconds: Self.responseDelayNanoseconds(for: String(line), fixture: fixture))
            }
        }
    }

    private static func responseDelayNanoseconds(for requestLine: String, fixture: RuneFakeK8sFixture) -> UInt64 {
        let parts = requestLine.split(separator: " ", maxSplits: 2).map(String.init)
        guard parts.count >= 2 else { return 0 }
        return fixture.delayedResponseTargets[parts[1]] ?? 0
    }

    private static func contentLength(in request: String) -> Int {
        request
            .components(separatedBy: "\r\n")
            .first { $0.localizedCaseInsensitiveComparePrefix("Content-Length:") }
            .flatMap { line in
                line.split(separator: ":", maxSplits: 1).dropFirst().first
            }
            .flatMap { Int($0.trimmingCharacters(in: .whitespacesAndNewlines)) } ?? 0
    }

    private static func body(in request: String) -> String {
        request.components(separatedBy: "\r\n\r\n").dropFirst().joined(separator: "\r\n\r\n")
    }
}

private extension String {
    func localizedCaseInsensitiveComparePrefix(_ prefix: String) -> Bool {
        range(of: prefix, options: [.caseInsensitive, .anchored]) != nil
    }
}

private struct RuneFakeK8sRouter {
    let fixture: RuneFakeK8sFixture
    let contextName: String
    let requestRecorder: RuneFakeK8sRequestRecorder

    func route(requestLine: String, body: String? = nil) -> RuneFakeK8sHTTPResponse {
        let parts = requestLine.split(separator: " ", maxSplits: 2).map(String.init)
        guard parts.count >= 2 else {
            return .json(status: 400, object: status(message: "Malformed HTTP request"))
        }
        let method = parts[0]
        guard method == "GET" || method == "PATCH" || method == "POST" else {
            return .json(status: 405, object: status(message: "Only GET, PATCH, and POST are supported by RuneFakeK8s REST."))
        }
        guard let cluster = fixture.cluster(named: contextName) else {
            return .json(status: 404, object: status(message: "Unknown fake context \(contextName)."))
        }

        let target = parts[1]
        if fixture.transientFailureTargets.contains(target), requestRecorder.count(target: target) == 1 {
            return .json(status: 503, object: status(message: "Synthetic transient failure for \(target)."))
        }
        guard let components = URLComponents(string: "http://fake\(target)") else {
            return .json(status: 400, object: status(message: "Invalid request target \(target)."))
        }
        let pathParts = components.path.split(separator: "/").map(String.init)
        let query = Dictionary(uniqueKeysWithValues: (components.queryItems ?? []).map { ($0.name, $0.value ?? "") })

        if components.path == "/" || components.path == "/healthz" {
            return .json(status: 200, object: ["status": "ok"])
        }

        if method == "POST",
           pathParts == ["apis", "authorization.k8s.io", "v1", "selfsubjectaccessreviews"] {
            return routeSelfSubjectAccessReview(body: body)
        }

        do {
            if pathParts == ["api", "v1", "namespaces"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "v1",
                    kind: "NamespaceList",
                    items: cluster.namespaces.map(namespaceObject),
                    query: query
                ))
            }

            if pathParts == ["api", "v1", "pods"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "v1",
                    kind: "PodList",
                    items: filteredPods(cluster.namespaces.flatMap { namespace in
                        namespace.pods.map { podObject($0, namespace: namespace.name) }
                    }, query: query),
                    query: query
                ))
            }

            if pathParts == ["apis", "apps", "v1", "deployments"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "apps/v1",
                    kind: "DeploymentList",
                    items: cluster.namespaces.flatMap { namespace in
                        namespace.deployments.map { deploymentObject($0, namespace: namespace.name) }
                    },
                    query: query
                ))
            }

            if pathParts == ["apis", "apps", "v1", "statefulsets"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "apps/v1",
                    kind: "StatefulSetList",
                    items: cluster.namespaces.flatMap { namespace in
                        namespace.statefulSets.map { statefulSetObject($0, namespace: namespace.name) }
                    },
                    query: query
                ))
            }

            if pathParts == ["api", "v1", "services"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "v1",
                    kind: "ServiceList",
                    items: cluster.namespaces.flatMap { namespace in
                        namespace.services.map { serviceObject($0, namespace: namespace.name) }
                    },
                    query: query
                ))
            }

            if pathParts == ["api", "v1", "configmaps"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "v1",
                    kind: "ConfigMapList",
                    items: cluster.namespaces.flatMap { namespace in configMapObjects(namespace) },
                    query: query
                ))
            }

            if pathParts == ["api", "v1", "events"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "v1",
                    kind: "EventList",
                    items: cluster.namespaces.flatMap { namespace in eventObjects(namespace) },
                    query: query
                ))
            }

            if pathParts == ["api", "v1", "nodes"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "v1",
                    kind: "NodeList",
                    items: cluster.nodes.map(nodeObject),
                    query: query
                ))
            }

            if pathParts == ["apis", "batch", "v1", "cronjobs"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "batch/v1",
                    kind: "CronJobList",
                    items: cluster.namespaces.flatMap { namespace in cronJobObjects(namespace) },
                    query: query
                ))
            }

            if pathParts == ["apis", "networking.k8s.io", "v1", "ingresses"] {
                return .json(status: 200, object: listObject(
                    apiVersion: "networking.k8s.io/v1",
                    kind: "IngressList",
                    items: cluster.namespaces.flatMap { namespace in ingressObjects(namespace) },
                    query: query
                ))
            }

            if pathParts == ["apis", "metrics.k8s.io", "v1beta1", "pods"] {
                return .json(status: 200, object: podMetricsList(
                    cluster.namespaces.flatMap { namespace in namespace.pods.map { ($0, namespace.name) } }
                ))
            }

            if pathParts == ["apis", "apiextensions.k8s.io", "v1", "customresourcedefinitions"] {
                return .json(status: 200, object: customResourceDefinitionListObject(cluster.operatorResources))
            }

            if pathParts.count >= 5,
               Array(pathParts[0...2]) == ["api", "v1", "namespaces"],
               let namespace = cluster.namespaces.first(where: { $0.name == pathParts[3] }) {
                return try routeCoreNamespaced(pathParts: pathParts, namespace: namespace, query: query)
            }

            if pathParts.count >= 6,
               Array(pathParts[0...3]) == ["apis", "apps", "v1", "namespaces"],
               let namespace = cluster.namespaces.first(where: { $0.name == pathParts[4] }) {
                return routeAppsNamespaced(method: method, pathParts: pathParts, namespace: namespace, query: query)
            }

            if pathParts.count >= 6,
               Array(pathParts[0...3]) == ["apis", "batch", "v1", "namespaces"],
               let namespace = cluster.namespaces.first(where: { $0.name == pathParts[4] }) {
                return routeBatchNamespaced(method: method, pathParts: pathParts, namespace: namespace, query: query)
            }

            if pathParts.count >= 6,
               Array(pathParts[0...3]) == ["apis", "networking.k8s.io", "v1", "namespaces"],
               let namespace = cluster.namespaces.first(where: { $0.name == pathParts[4] }) {
                return routeNetworkingNamespaced(pathParts: pathParts, namespace: namespace, query: query)
            }

            if pathParts.count == 7,
               pathParts[0] == "apis",
               pathParts[3] == "namespaces",
               cluster.namespaces.contains(where: { $0.name == pathParts[4] }),
               cluster.operatorResources.contains(where: {
                   $0.apiGroup == pathParts[1] &&
                       $0.apiVersion == pathParts[2] &&
                       $0.namespace == pathParts[4] &&
                       $0.plural == pathParts[5] &&
                       $0.name == pathParts[6]
               }) {
                return routeOperatorNamespacedResource(pathParts: pathParts, cluster: cluster, namespace: pathParts[4])
            }

            if pathParts.count == 6,
               pathParts[0] == "apis",
               pathParts[3] == "namespaces",
               cluster.namespaces.contains(where: { $0.name == pathParts[4] }),
               cluster.operatorResources.contains(where: {
                   $0.apiGroup == pathParts[1] &&
                       $0.apiVersion == pathParts[2] &&
                       $0.namespace == pathParts[4] &&
                       $0.plural == pathParts[5]
               }) {
                return routeOperatorNamespaced(pathParts: pathParts, cluster: cluster, namespace: pathParts[4], query: query)
            }

            if pathParts.count == 5,
               pathParts[0] == "apis",
               cluster.operatorResources.contains(where: {
                   $0.apiGroup == pathParts[1] &&
                       $0.apiVersion == pathParts[2] &&
                       $0.namespace == nil &&
                       $0.plural == pathParts[3] &&
                       $0.name == pathParts[4]
               }) {
                return routeOperatorClusterScopedResource(pathParts: pathParts, cluster: cluster)
            }

            if pathParts.count == 4,
               pathParts[0] == "apis",
               cluster.operatorResources.contains(where: {
                   $0.apiGroup == pathParts[1] &&
                       $0.apiVersion == pathParts[2] &&
                       $0.namespace == nil &&
                       $0.plural == pathParts[3]
               }) {
                return routeOperatorClusterScoped(pathParts: pathParts, cluster: cluster, query: query)
            }

            if pathParts.count >= 6,
               Array(pathParts[0...3]) == ["apis", "metrics.k8s.io", "v1beta1", "namespaces"],
               let namespace = cluster.namespaces.first(where: { $0.name == pathParts[4] }),
               pathParts[5] == "pods" {
                return .json(status: 200, object: podMetricsList(namespace.pods.map { ($0, namespace.name) }))
            }

            return .json(status: 404, object: status(message: "No fake route for \(components.path)."))
        } catch {
            return .json(status: 404, object: status(message: String(describing: error)))
        }
    }

    private func routeCoreNamespaced(
        pathParts: [String],
        namespace: RuneFakeK8sNamespace,
        query: [String: String]
    ) throws -> RuneFakeK8sHTTPResponse {
        guard pathParts.count >= 5 else {
            return .json(status: 404, object: status(message: "Missing namespaced resource."))
        }
        switch pathParts[4] {
        case "pods" where pathParts.count == 5:
            return .json(status: 200, object: listObject(
                apiVersion: "v1",
                kind: "PodList",
                items: filteredPods(namespace.pods.map { podObject($0, namespace: namespace.name) }, query: query),
                query: query
            ))
        case "pods" where pathParts.count == 6:
            guard let pod = namespace.pods.first(where: { $0.name == pathParts[5] }) else {
                return .json(status: 404, object: status(message: "Pod \(pathParts[5]) was not found."))
            }
            return .json(status: 200, object: podObject(pod, namespace: namespace.name))
        case "pods" where pathParts.count == 7 && pathParts[6] == "log":
            guard let pod = namespace.pods.first(where: { $0.name == pathParts[5] }) else {
                return .json(status: 404, object: status(message: "Pod \(pathParts[5]) was not found."))
            }
            if isDenied(namespace: namespace.name, verb: "get", resource: "pods", subresource: "log") {
                return .json(status: 403, object: status(message: "pods/log is forbidden in namespace \(namespace.name)."))
            }
            if namespace.failingLogPodNames.contains(pod.name) {
                return .json(status: 500, object: status(message: "Synthetic forced pod log failure for \(pod.name)."))
            }
            return .text(status: 200, body: logLines(for: pod, namespace: namespace.name, container: query["container"]))
        case "services" where pathParts.count == 5:
            return .json(status: 200, object: listObject(
                apiVersion: "v1",
                kind: "ServiceList",
                items: namespace.services.map { serviceObject($0, namespace: namespace.name) },
                query: query
            ))
        case "services" where pathParts.count == 6:
            guard let service = namespace.services.first(where: { $0.name == pathParts[5] }) else {
                return .json(status: 404, object: status(message: "Service \(pathParts[5]) was not found."))
            }
            return .json(status: 200, object: serviceObject(service, namespace: namespace.name))
        case "endpoints" where pathParts.count == 5:
            return .json(status: 200, object: listObject(
                apiVersion: "v1",
                kind: "EndpointsList",
                items: endpointObjects(namespace),
                query: query
            ))
        case "endpoints" where pathParts.count == 6:
            guard let endpoint = endpointObjects(namespace).first(where: { metadataName($0) == pathParts[5] }) else {
                return .json(status: 404, object: status(message: "Endpoints \(pathParts[5]) was not found."))
            }
            return .json(status: 200, object: endpoint)
        case "serviceaccounts" where pathParts.count == 5:
            return .json(status: 200, object: listObject(
                apiVersion: "v1",
                kind: "ServiceAccountList",
                items: serviceAccountObjects(namespace),
                query: query
            ))
        case "serviceaccounts" where pathParts.count == 6:
            guard let serviceAccount = serviceAccountObjects(namespace).first(where: { metadataName($0) == pathParts[5] }) else {
                return .json(status: 404, object: status(message: "ServiceAccount \(pathParts[5]) was not found."))
            }
            return .json(status: 200, object: serviceAccount)
        case "configmaps" where pathParts.count == 5:
            return .json(status: 200, object: listObject(
                apiVersion: "v1",
                kind: "ConfigMapList",
                items: configMapObjects(namespace),
                query: query
            ))
        case "configmaps" where pathParts.count == 6:
            guard let configMap = configMapObjects(namespace).first(where: { metadataName($0) == pathParts[5] }) else {
                return .json(status: 404, object: status(message: "ConfigMap \(pathParts[5]) was not found."))
            }
            return .json(status: 200, object: configMap)
        case "events" where pathParts.count == 5:
            return .json(status: 200, object: listObject(
                apiVersion: "v1",
                kind: "EventList",
                items: eventObjects(namespace),
                query: query
            ))
        default:
            return .json(status: 404, object: status(message: "Unsupported core namespaced route."))
        }
    }

    private func routeSelfSubjectAccessReview(body: String?) -> RuneFakeK8sHTTPResponse {
        guard let body,
              let data = body.data(using: .utf8),
              let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let spec = object["spec"] as? [String: Any],
              let attributes = spec["resourceAttributes"] as? [String: Any],
              let verb = attributes["verb"] as? String,
              let resource = attributes["resource"] as? String
        else {
            return .json(status: 400, object: status(message: "Malformed SelfSubjectAccessReview request."))
        }

        let namespace = normalized(attributes["namespace"] as? String)
        let apiGroup = normalized(attributes["group"] as? String)
        let subresource = normalized(attributes["subresource"] as? String)
        let allowed = !isDenied(
            namespace: namespace,
            verb: verb,
            resource: resource,
            apiGroup: apiGroup,
            subresource: subresource
        )

        return .json(status: 201, object: [
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectAccessReview",
            "status": [
                "allowed": allowed,
                "reason": allowed ? "allowed by Rune fake RBAC" : "denied by Rune fake RBAC"
            ]
        ])
    }

    private func isDenied(
        namespace: String?,
        verb: String,
        resource: String,
        apiGroup: String? = nil,
        subresource: String? = nil
    ) -> Bool {
        let rule = RuneFakeK8sRBACRule(
            namespace: normalized(namespace),
            verb: verb,
            resource: resource,
            apiGroup: normalized(apiGroup),
            subresource: normalized(subresource)
        )
        return fixture.selfSubjectAccessReviewDenials.contains(rule)
    }

    private func normalized(_ value: String?) -> String? {
        guard let trimmed = value?.trimmingCharacters(in: .whitespacesAndNewlines),
              !trimmed.isEmpty
        else {
            return nil
        }
        return trimmed
    }

    private func routeAppsNamespaced(
        method: String,
        pathParts: [String],
        namespace: RuneFakeK8sNamespace,
        query: [String: String]
    ) -> RuneFakeK8sHTTPResponse {
        switch pathParts.count {
        case 6 where pathParts[5] == "deployments":
            guard method == "GET" else {
                return .json(status: 405, object: status(message: "Only GET is supported for deployment lists."))
            }
            return .json(status: 200, object: listObject(
                apiVersion: "apps/v1",
                kind: "DeploymentList",
                items: namespace.deployments.map { deploymentObject($0, namespace: namespace.name) },
                query: query
            ))
        case 7 where pathParts[5] == "deployments":
            guard let deployment = namespace.deployments.first(where: { $0.name == pathParts[6] }) else {
                return .json(status: 404, object: status(message: "Deployment \(pathParts[6]) was not found."))
            }
            return .json(status: 200, object: deploymentObject(deployment, namespace: namespace.name))
        case 8 where pathParts[5] == "deployments" && pathParts[7] == "scale":
            guard method == "PATCH" else {
                return .json(status: 405, object: status(message: "Only PATCH is supported for Deployment scale."))
            }
            guard let deployment = namespace.deployments.first(where: { $0.name == pathParts[6] }) else {
                return .json(status: 404, object: status(message: "Deployment \(pathParts[6]) was not found."))
            }
            return .json(status: 200, object: deploymentObject(deployment, namespace: namespace.name))
        case 6 where pathParts[5] == "statefulsets":
            guard method == "GET" else {
                return .json(status: 405, object: status(message: "Only GET is supported for StatefulSet lists."))
            }
            return .json(status: 200, object: listObject(
                apiVersion: "apps/v1",
                kind: "StatefulSetList",
                items: namespace.statefulSets.map { statefulSetObject($0, namespace: namespace.name) },
                query: query
            ))
        case 7 where pathParts[5] == "statefulsets":
            guard let statefulSet = namespace.statefulSets.first(where: { $0.name == pathParts[6] }) else {
                return .json(status: 404, object: status(message: "StatefulSet \(pathParts[6]) was not found."))
            }
            return .json(status: 200, object: statefulSetObject(statefulSet, namespace: namespace.name))
        case 8 where pathParts[5] == "statefulsets" && pathParts[7] == "scale":
            guard method == "PATCH" else {
                return .json(status: 405, object: status(message: "Only PATCH is supported for StatefulSet scale."))
            }
            guard let statefulSet = namespace.statefulSets.first(where: { $0.name == pathParts[6] }) else {
                return .json(status: 404, object: status(message: "StatefulSet \(pathParts[6]) was not found."))
            }
            return .json(status: 200, object: statefulSetObject(statefulSet, namespace: namespace.name))
        case 6 where pathParts[5] == "replicasets":
            guard method == "GET" else {
                return .json(status: 405, object: status(message: "Only GET is supported for ReplicaSet lists."))
            }
            return .json(status: 200, object: listObject(
                apiVersion: "apps/v1",
                kind: "ReplicaSetList",
                items: filteredReplicaSets(replicaSetObjects(namespace), query: query),
                query: query
            ))
        default:
            return .json(status: 404, object: status(message: "Unsupported apps namespaced route."))
        }
    }

    private func routeBatchNamespaced(
        method: String,
        pathParts: [String],
        namespace: RuneFakeK8sNamespace,
        query: [String: String]
    ) -> RuneFakeK8sHTTPResponse {
        switch pathParts.count {
        case 6 where pathParts[5] == "jobs":
            if method == "POST" {
                return .json(status: 201, object: [
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": [
                        "name": "manual-job",
                        "namespace": namespace.name,
                        "creationTimestamp": "2026-04-26T10:00:00Z"
                    ],
                    "spec": [:],
                    "status": [:]
                ])
            }
            guard method == "GET" else {
                return .json(status: 405, object: status(message: "Only GET and POST are supported for Jobs."))
            }
            return .json(status: 200, object: listObject(
                apiVersion: "batch/v1",
                kind: "JobList",
                items: [],
                query: query
            ))
        case 6 where pathParts[5] == "cronjobs":
            guard method == "GET" else {
                return .json(status: 405, object: status(message: "Only GET is supported for CronJob lists."))
            }
            return .json(status: 200, object: listObject(
                apiVersion: "batch/v1",
                kind: "CronJobList",
                items: cronJobObjects(namespace),
                query: query
            ))
        case 7 where pathParts[5] == "cronjobs":
            guard method == "GET" else {
                return .json(status: 405, object: status(message: "Only GET is supported for CronJob resources."))
            }
            guard let cronJob = cronJobObjects(namespace).first(where: { metadataName($0) == pathParts[6] }) else {
                return .json(status: 404, object: status(message: "CronJob \(pathParts[6]) was not found."))
            }
            return .json(status: 200, object: cronJob)
        default:
            return .json(status: 404, object: status(message: "Unsupported batch namespaced route."))
        }
    }

    private func routeOperatorNamespaced(
        pathParts: [String],
        cluster: RuneFakeK8sCluster,
        namespace: String,
        query: [String: String]
    ) -> RuneFakeK8sHTTPResponse {
        let matches = cluster.operatorResources.filter { resource in
            resource.apiGroup == pathParts[1] &&
                resource.apiVersion == pathParts[2] &&
                resource.namespace == namespace &&
                resource.plural == pathParts[5]
        }
        guard !matches.isEmpty else {
            return .json(status: 404, object: status(message: "Unsupported operator namespaced route."))
        }
        return .json(status: 200, object: operatorResourceListObject(matches, query: query))
    }

    private func routeOperatorNamespacedResource(
        pathParts: [String],
        cluster: RuneFakeK8sCluster,
        namespace: String
    ) -> RuneFakeK8sHTTPResponse {
        guard let match = cluster.operatorResources.first(where: { resource in
            resource.apiGroup == pathParts[1] &&
                resource.apiVersion == pathParts[2] &&
                resource.namespace == namespace &&
                resource.plural == pathParts[5] &&
                resource.name == pathParts[6]
        }) else {
            return .json(status: 404, object: status(message: "Unsupported operator namespaced resource route."))
        }
        return .json(status: 200, object: operatorResourceObject(match))
    }

    private func routeOperatorClusterScoped(
        pathParts: [String],
        cluster: RuneFakeK8sCluster,
        query: [String: String]
    ) -> RuneFakeK8sHTTPResponse {
        let matches = cluster.operatorResources.filter { resource in
            resource.apiGroup == pathParts[1] &&
                resource.apiVersion == pathParts[2] &&
                resource.namespace == nil &&
                resource.plural == pathParts[3]
        }
        guard !matches.isEmpty else {
            return .json(status: 404, object: status(message: "Unsupported operator cluster route."))
        }
        return .json(status: 200, object: operatorResourceListObject(matches, query: query))
    }

    private func routeOperatorClusterScopedResource(
        pathParts: [String],
        cluster: RuneFakeK8sCluster
    ) -> RuneFakeK8sHTTPResponse {
        guard let match = cluster.operatorResources.first(where: { resource in
            resource.apiGroup == pathParts[1] &&
                resource.apiVersion == pathParts[2] &&
                resource.namespace == nil &&
                resource.plural == pathParts[3] &&
                resource.name == pathParts[4]
        }) else {
            return .json(status: 404, object: status(message: "Unsupported operator cluster resource route."))
        }
        return .json(status: 200, object: operatorResourceObject(match))
    }

    private func routeNetworkingNamespaced(
        pathParts: [String],
        namespace: RuneFakeK8sNamespace,
        query: [String: String]
    ) -> RuneFakeK8sHTTPResponse {
        switch pathParts.count {
        case 6 where pathParts[5] == "ingresses":
            return .json(status: 200, object: listObject(
                apiVersion: "networking.k8s.io/v1",
                kind: "IngressList",
                items: ingressObjects(namespace),
                query: query
            ))
        case 7 where pathParts[5] == "ingresses":
            guard let ingress = ingressObjects(namespace).first(where: { metadataName($0) == pathParts[6] }) else {
                return .json(status: 404, object: status(message: "Ingress \(pathParts[6]) was not found."))
            }
            return .json(status: 200, object: ingress)
        default:
            return .json(status: 404, object: status(message: "Unsupported networking namespaced route."))
        }
    }

    private func namespaceObject(_ namespace: RuneFakeK8sNamespace) -> [String: Any] {
        [
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": [
                "name": namespace.name,
                "creationTimestamp": "2026-04-21T00:00:00Z"
            ],
            "status": ["phase": "Active"]
        ]
    }

    private func podObject(_ pod: RuneFakeK8sPod, namespace: String) -> [String: Any] {
        [
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": [
                "name": pod.name,
                "namespace": namespace,
                "uid": "fake-\(pod.name)",
                "creationTimestamp": "2026-04-26T10:00:00Z",
                "labels": pod.labels
            ],
            "spec": [
                "nodeName": pod.nodeName,
                "containers": pod.containers.map { container in
                    ["name": container, "image": "ghcr.io/rune/\(pod.deploymentName):fake"]
                }
            ],
            "status": [
                "phase": pod.phase,
                "hostIP": "10.10.0.10",
                "podIP": pod.podIP ?? "",
                "qosClass": pod.phase == "Running" ? "Burstable" : "BestEffort",
                "containerStatuses": pod.containers.map { container in
                    [
                        "name": container,
                        "ready": pod.phase == "Running",
                        "restartCount": pod.restarts,
                        "state": pod.phase == "Running"
                            ? ["running": ["startedAt": "2026-04-26T10:00:00Z"]]
                            : ["waiting": ["reason": "ContainerCreating"]]
                    ] as [String: Any]
                }
            ]
        ]
    }

    private func deploymentObject(_ deployment: RuneFakeK8sDeployment, namespace: String) -> [String: Any] {
        [
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": [
                "name": deployment.name,
                "namespace": namespace,
                "creationTimestamp": "2026-04-25T10:00:00Z",
                "annotations": ["deployment.kubernetes.io/revision": "2"]
            ],
            "spec": [
                "replicas": deployment.replicas,
                "selector": ["matchLabels": deployment.selector],
                "template": [
                    "metadata": ["labels": deployment.selector],
                    "spec": ["containers": [["name": deployment.name, "image": "ghcr.io/rune/\(deployment.name):fake"]]]
                ]
            ],
            "status": [
                "readyReplicas": deployment.readyReplicas,
                "replicas": deployment.replicas,
                "updatedReplicas": deployment.readyReplicas,
                "availableReplicas": deployment.readyReplicas
            ]
        ]
    }

    private func statefulSetObject(_ statefulSet: RuneFakeK8sStatefulSet, namespace: String) -> [String: Any] {
        [
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": [
                "name": statefulSet.name,
                "namespace": namespace,
                "creationTimestamp": "2026-04-25T10:00:00Z"
            ],
            "spec": [
                "replicas": statefulSet.replicas,
                "selector": ["matchLabels": statefulSet.selector],
                "serviceName": statefulSet.name,
                "template": [
                    "metadata": ["labels": statefulSet.selector],
                    "spec": ["containers": [["name": statefulSet.name, "image": "ghcr.io/rune/\(statefulSet.name):fake"]]]
                ]
            ],
            "status": [
                "readyReplicas": statefulSet.readyReplicas,
                "replicas": statefulSet.replicas,
                "updatedReplicas": statefulSet.readyReplicas,
                "availableReplicas": statefulSet.readyReplicas
            ]
        ]
    }

    private func replicaSetObjects(_ namespace: RuneFakeK8sNamespace) -> [[String: Any]] {
        namespace.deployments.flatMap { deployment in
            [1, 2].map { revision in
                [
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "metadata": [
                        "name": "\(deployment.name)-rs\(revision)",
                        "namespace": namespace.name,
                        "creationTimestamp": "2026-04-25T10:0\(revision):00Z",
                        "annotations": [
                            "deployment.kubernetes.io/revision": "\(revision)",
                            "kubernetes.io/change-cause": revision == 1 ? "synthetic bootstrap" : "synthetic rollout"
                        ],
                        "labels": deployment.selector
                    ],
                    "spec": [
                        "selector": ["matchLabels": deployment.selector],
                        "template": [
                            "metadata": ["labels": deployment.selector],
                            "spec": [
                                "containers": [[
                                    "name": deployment.name,
                                    "image": "ghcr.io/rune/\(deployment.name):fake-\(revision)"
                                ]]
                            ]
                        ]
                    ]
                ] as [String: Any]
            }
        }
    }

    private func filteredReplicaSets(_ objects: [[String: Any]], query: [String: String]) -> [[String: Any]] {
        guard let selector = query["labelSelector"], !selector.isEmpty else { return objects }
        let requirements = selector.split(separator: ",").compactMap { pair -> (String, String)? in
            let parts = pair.split(separator: "=", maxSplits: 1).map(String.init)
            guard parts.count == 2 else { return nil }
            return (parts[0], parts[1])
        }
        guard !requirements.isEmpty else { return objects }
        return objects.filter { object in
            let metadata = object["metadata"] as? [String: Any]
            let labels = metadata?["labels"] as? [String: String] ?? [:]
            return requirements.allSatisfy { labels[$0.0] == $0.1 }
        }
    }

    private func serviceObject(_ service: RuneFakeK8sService, namespace: String) -> [String: Any] {
        [
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": [
                "name": service.name,
                "namespace": namespace,
                "creationTimestamp": "2026-04-25T10:00:00Z"
            ],
            "spec": [
                "type": "ClusterIP",
                "clusterIP": service.clusterIP,
                "selector": service.selector,
                "ports": [["name": "http", "port": 80, "targetPort": 8080]]
            ]
        ]
    }

    private func operatorResourceListObject(
        _ resources: [RuneFakeK8sOperatorResource],
        query: [String: String]
    ) -> [String: Any] {
        let first = resources[0]
        return listObject(
            apiVersion: "\(first.apiGroup)/\(first.apiVersion)",
            kind: "\(first.kind)List",
            items: resources.map(operatorResourceObject),
            query: query
        )
    }

    private func customResourceDefinitionListObject(_ resources: [RuneFakeK8sOperatorResource]) -> [String: Any] {
        var seen = Set<String>()
        let items = resources.compactMap { resource -> [String: Any]? in
            guard !resource.printerColumnDefinitions.isEmpty else { return nil }
            let key = "\(resource.plural).\(resource.apiGroup)"
            guard seen.insert("\(key)/\(resource.apiVersion)").inserted else { return nil }
            return customResourceDefinitionObject(resource, name: key)
        }
        return listObject(
            apiVersion: "apiextensions.k8s.io/v1",
            kind: "CustomResourceDefinitionList",
            items: items,
            query: [:]
        )
    }

    private func customResourceDefinitionObject(_ resource: RuneFakeK8sOperatorResource, name: String) -> [String: Any] {
        [
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": [
                "name": name,
                "creationTimestamp": "2026-04-20T10:00:00Z"
            ],
            "spec": [
                "group": resource.apiGroup,
                "names": [
                    "kind": resource.kind,
                    "plural": resource.plural
                ],
                "versions": [[
                    "name": resource.apiVersion,
                    "served": true,
                    "storage": true,
                    "additionalPrinterColumns": resource.printerColumnDefinitions
                        .sorted { $0.key < $1.key }
                        .map { ["name": $0.key, "jsonPath": $0.value, "type": "string"] }
                ]]
            ]
        ]
    }

    private func operatorResourceObject(_ resource: RuneFakeK8sOperatorResource) -> [String: Any] {
        var metadata: [String: Any] = [
            "name": resource.name,
            "uid": "fake-\(resource.kind.lowercased())-\(resource.name)",
            "creationTimestamp": "2026-04-26T10:00:00Z"
        ]
        if let namespace = resource.namespace {
            metadata["namespace"] = namespace
        }

        var object: [String: Any] = [
            "apiVersion": "\(resource.apiGroup)/\(resource.apiVersion)",
            "kind": resource.kind,
            "metadata": metadata,
            "status": [
                "conditions": [
                    [
                        "type": resource.conditionType,
                        "status": resource.conditionStatus,
                        "reason": resource.reason,
                        "message": resource.message
                    ]
                ]
            ]
        ]
        if !resource.printerColumns.isEmpty {
            object["additionalPrinterColumns"] = resource.printerColumns
                .sorted { $0.key < $1.key }
                .map { ["name": $0.key, "value": $0.value] }
        }
        return object
    }

    private func configMapObjects(_ namespace: RuneFakeK8sNamespace) -> [[String: Any]] {
        namespace.deployments.map { deployment in
            [
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": [
                    "name": "\(deployment.name)-settings",
                    "namespace": namespace.name,
                    "creationTimestamp": "2026-04-25T10:00:00Z"
                ],
                "data": [
                    "LOG_LEVEL": deployment.readyReplicas < deployment.replicas ? "debug" : "info",
                    "OWNER": namespace.name
                ]
            ]
        }
    }

    private func endpointObjects(_ namespace: RuneFakeK8sNamespace) -> [[String: Any]] {
        namespace.services.map { service in
            let matchingPods = namespace.pods.filter { pod in
                service.selector.allSatisfy { key, value in pod.labels[key] == value }
            }
            return [
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": [
                    "name": service.name,
                    "namespace": namespace.name,
                    "creationTimestamp": "2026-04-25T10:00:00Z"
                ],
                "subsets": [[
                    "addresses": matchingPods.map { pod in
                        [
                            "ip": pod.podIP ?? "0.0.0.0",
                            "targetRef": [
                                "kind": "Pod",
                                "name": pod.name,
                                "namespace": namespace.name
                            ]
                        ]
                    },
                    "ports": [[
                        "name": "http",
                        "port": 80,
                        "protocol": "TCP"
                    ]]
                ]]
            ]
        }
    }

    private func serviceAccountObjects(_ namespace: RuneFakeK8sNamespace) -> [[String: Any]] {
        let names = ["default"] + namespace.deployments.map { "\($0.name)-runner" }
        return names.map { name in
            [
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": [
                    "name": name,
                    "namespace": namespace.name,
                    "creationTimestamp": "2026-04-25T10:00:00Z"
                ],
                "secrets": [["name": "\(name)-token"]],
                "imagePullSecrets": name == "default" ? [] : [["name": "\(name)-pull"]],
                "automountServiceAccountToken": name == "default"
            ]
        }
    }

    private func cronJobObjects(_ namespace: RuneFakeK8sNamespace) -> [[String: Any]] {
        guard let deployment = namespace.deployments.first else { return [] }
        return [[
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": [
                "name": "\(deployment.name)-report",
                "namespace": namespace.name,
                "creationTimestamp": "2026-04-25T10:00:00Z"
            ],
            "spec": [
                "schedule": "*/20 * * * *",
                "suspend": false,
                "jobTemplate": [
                    "metadata": [
                        "labels": [
                            "app": deployment.name,
                            "rune.fake/source": "cronjob"
                        ]
                    ],
                    "spec": [
                        "template": [
                            "spec": [
                                "restartPolicy": "OnFailure",
                                "containers": [[
                                    "name": "report",
                                    "image": "example.invalid/rune/report:1.0",
                                    "command": ["sh", "-c", "echo synthetic report"]
                                ]]
                            ]
                        ]
                    ]
                ]
            ],
            "status": [
                "lastScheduleTime": "2026-04-26T09:00:00Z"
            ]
        ]]
    }

    private func ingressObjects(_ namespace: RuneFakeK8sNamespace) -> [[String: Any]] {
        namespace.services.prefix(1).map { service in
            [
                "apiVersion": "networking.k8s.io/v1",
                "kind": "Ingress",
                "metadata": [
                    "name": "\(service.name)-public",
                    "namespace": namespace.name,
                    "creationTimestamp": "2026-04-25T10:00:00Z"
                ],
                "spec": [
                    "rules": [[
                        "host": "\(service.name).\(namespace.name).fake.rune.local",
                        "http": [
                            "paths": [[
                                "path": "/",
                                "pathType": "Prefix",
                                "backend": [
                                    "service": [
                                        "name": service.name,
                                        "port": ["number": 80]
                                    ]
                                ]
                            ]]
                        ]
                    ]]
                ],
                "status": [
                    "loadBalancer": [
                        "ingress": [["hostname": "lb-\(service.name).fake.rune.local"]]
                    ]
                ]
            ]
        }
    }

    private func nodeObject(_ node: RuneFakeK8sNode) -> [String: Any] {
        [
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": [
                "name": node.name,
                "creationTimestamp": "2026-04-20T10:00:00Z"
            ],
            "status": [
                "addresses": [["type": "InternalIP", "address": node.internalIP]],
                "capacity": ["cpu": "4", "memory": "8192Mi"],
                "conditions": [["type": "Ready", "status": "True"]],
                "nodeInfo": ["kubeletVersion": "v1.30.0-fake"]
            ]
        ]
    }

    private func eventObjects(_ namespace: RuneFakeK8sNamespace) -> [[String: Any]] {
        namespace.pods.map { pod in
            [
                "apiVersion": "v1",
                "kind": "Event",
                "metadata": [
                    "name": "\(pod.name).ready",
                    "namespace": namespace.name,
                    "creationTimestamp": "2026-04-26T10:02:00Z"
                ],
                "type": pod.phase == "Running" ? "Normal" : "Warning",
                "reason": pod.phase == "Running" ? "Started" : "Scheduling",
                "message": pod.phase == "Running"
                    ? "Started container in fake pod \(pod.name)."
                    : "Fake pod \(pod.name) is waiting for scheduling.",
                "firstTimestamp": "2026-04-26T10:01:00Z",
                "lastTimestamp": "2026-04-26T10:02:00Z",
                "involvedObject": [
                    "kind": "Pod",
                    "name": pod.name,
                    "namespace": namespace.name
                ]
            ]
        }
    }

    private func podMetricsList(_ pods: [(RuneFakeK8sPod, String)]) -> [String: Any] {
        [
            "apiVersion": "metrics.k8s.io/v1beta1",
            "kind": "PodMetricsList",
            "items": pods.map { pod, namespace in
                [
                    "metadata": ["name": pod.name, "namespace": namespace],
                    "containers": pod.containers.map { container in
                        ["name": container, "usage": ["cpu": pod.cpu, "memory": pod.memory]]
                    }
                ]
            }
        ]
    }

    private func metadataName(_ object: [String: Any]) -> String? {
        (object["metadata"] as? [String: Any])?["name"] as? String
    }

    private func listObject(
        apiVersion: String,
        kind: String,
        items: [[String: Any]],
        query: [String: String]
    ) -> [String: Any] {
        let offset = Int(query["continue"] ?? "") ?? 0
        let limit = Int(query["limit"] ?? "") ?? items.count
        let start = min(max(0, offset), items.count)
        let end = min(items.count, start + max(0, limit))
        let page = Array(items[start..<end])
        let remaining = max(0, items.count - end)
        var metadata: [String: Any] = ["resourceVersion": "fake-1"]
        if query["limit"] != nil {
            if !fixture.listKindsOmittingRemainingItemCount.contains(kind) {
                metadata["remainingItemCount"] = remaining
            }
            metadata["continue"] = remaining > 0 ? String(end) : ""
        }
        return [
            "apiVersion": apiVersion,
            "kind": kind,
            "metadata": metadata,
            "items": page
        ]
    }

    private func filteredPods(_ pods: [[String: Any]], query: [String: String]) -> [[String: Any]] {
        guard let selector = query["labelSelector"], !selector.isEmpty else { return pods }
        let requirements = selector.split(separator: ",").compactMap { raw -> (String, String)? in
            let pair = raw.split(separator: "=", maxSplits: 1).map(String.init)
            guard pair.count == 2 else { return nil }
            return (pair[0], pair[1])
        }
        guard !requirements.isEmpty else { return pods }
        return pods.filter { object in
            guard
                let metadata = object["metadata"] as? [String: Any],
                let labels = metadata["labels"] as? [String: String]
            else {
                return false
            }
            return requirements.allSatisfy { labels[$0.0] == $0.1 }
        }
    }

    private func logLines(for pod: RuneFakeK8sPod, namespace: String, container selectedContainer: String? = nil) -> String {
        let containers = selectedContainer
            .map { selected in pod.containers.filter { $0 == selected } } ?? pod.containers
        return containers.enumerated().map { index, container in
            "2026-04-26T10:00:0\(index)Z \(container) namespace=\(namespace) pod=\(pod.name) synthetic REST fake log"
        }.joined(separator: "\n") + "\n"
    }

    private func status(message: String) -> [String: Any] {
        [
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": message
        ]
    }
}

private struct RuneFakeK8sHTTPResponse {
    let status: Int
    let contentType: String
    let body: Data

    static func json(status: Int, object: [String: Any]) -> RuneFakeK8sHTTPResponse {
        let data = (try? JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])) ?? Data("{}".utf8)
        return RuneFakeK8sHTTPResponse(status: status, contentType: "application/json", body: data)
    }

    static func text(status: Int, body: String) -> RuneFakeK8sHTTPResponse {
        RuneFakeK8sHTTPResponse(status: status, contentType: "text/plain; charset=utf-8", body: Data(body.utf8))
    }
}

private extension NWConnection {
    func sendHTTP(_ response: RuneFakeK8sHTTPResponse, delayNanoseconds: UInt64 = 0) {
        guard delayNanoseconds > 0 else {
            sendHTTPNow(response)
            return
        }

        let delay = DispatchTimeInterval.nanoseconds(min(Int(delayNanoseconds), Int(Int32.max)))
        DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + delay) { [weak self] in
            self?.sendHTTPNow(response)
        }
    }

    private func sendHTTPNow(_ response: RuneFakeK8sHTTPResponse) {
        let header = [
            "HTTP/1.1 \(response.status) \(reasonPhrase(response.status))",
            "Content-Type: \(response.contentType)",
            "Content-Length: \(response.body.count)",
            "Connection: close",
            "",
            ""
        ].joined(separator: "\r\n")
        var data = Data(header.utf8)
        data.append(response.body)
        send(content: data, completion: .contentProcessed { [weak self] _ in
            self?.cancel()
        })
    }

    private func reasonPhrase(_ status: Int) -> String {
        switch status {
        case 200: return "OK"
        case 400: return "Bad Request"
        case 404: return "Not Found"
        case 405: return "Method Not Allowed"
        default: return "Error"
        }
    }
}
