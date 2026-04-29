import Foundation
import Network

public final class RuneFakeK8sRESTServer: @unchecked Sendable {
    public let port: UInt16

    private let listener: NWListener
    private let fixture: RuneFakeK8sFixture
    private let contextName: String
    private let queue: DispatchQueue

    private init(
        listener: NWListener,
        port: UInt16,
        fixture: RuneFakeK8sFixture,
        contextName: String
    ) {
        self.listener = listener
        self.port = port
        self.fixture = fixture
        self.contextName = contextName
        self.queue = DispatchQueue(label: "rune.fake-k8s.rest-server")
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
        let serverBox = ServerBox(
            listener: listener,
            fixture: fixture,
            contextName: contextName
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
                    contextName: contextName
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

    public func kubeconfigYAML(serverURL: String? = nil) -> String {
        let endpoint = serverURL ?? "http://127.0.0.1:\(port)"
        return RuneFakeK8sKubeconfig.render(
            fixture: fixture,
            currentContext: contextName,
            serverURL: endpoint
        )
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

    init(listener: NWListener, fixture: RuneFakeK8sFixture, contextName: String) {
        self.listener = listener
        self.fixture = fixture
        self.contextName = contextName
    }

    func receive(connection: NWConnection) {
        connection.start(queue: DispatchQueue(label: "rune.fake-k8s.rest-connection"))
        connection.receive(minimumIncompleteLength: 1, maximumLength: 128 * 1024) { [fixture, contextName] data, _, _, _ in
            guard
                let data,
                let request = String(data: data, encoding: .utf8),
                let line = request.split(separator: "\r\n", maxSplits: 1).first
            else {
                connection.cancel()
                return
            }
            let response = RuneFakeK8sRouter(fixture: fixture, contextName: contextName)
                .route(requestLine: String(line))
            connection.sendHTTP(response)
        }
    }
}

private struct RuneFakeK8sRouter {
    let fixture: RuneFakeK8sFixture
    let contextName: String

    func route(requestLine: String) -> RuneFakeK8sHTTPResponse {
        let parts = requestLine.split(separator: " ", maxSplits: 2).map(String.init)
        guard parts.count >= 2 else {
            return .json(status: 400, object: status(message: "Malformed HTTP request"))
        }
        guard parts[0] == "GET" else {
            return .json(status: 405, object: status(message: "Only GET is supported by RuneFakeK8s REST."))
        }
        guard let cluster = fixture.cluster(named: contextName) else {
            return .json(status: 404, object: status(message: "Unknown fake context \(contextName)."))
        }

        let target = parts[1]
        guard let components = URLComponents(string: "http://fake\(target)") else {
            return .json(status: 400, object: status(message: "Invalid request target \(target)."))
        }
        let pathParts = components.path.split(separator: "/").map(String.init)
        let query = Dictionary(uniqueKeysWithValues: (components.queryItems ?? []).map { ($0.name, $0.value ?? "") })

        if components.path == "/" || components.path == "/healthz" {
            return .json(status: 200, object: ["status": "ok"])
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

            if pathParts == ["apis", "metrics.k8s.io", "v1beta1", "pods"] {
                return .json(status: 200, object: podMetricsList(
                    cluster.namespaces.flatMap { namespace in namespace.pods.map { ($0, namespace.name) } }
                ))
            }

            if pathParts.count >= 5,
               Array(pathParts[0...2]) == ["api", "v1", "namespaces"],
               let namespace = cluster.namespaces.first(where: { $0.name == pathParts[3] }) {
                return try routeCoreNamespaced(pathParts: pathParts, namespace: namespace, query: query)
            }

            if pathParts.count >= 6,
               Array(pathParts[0...3]) == ["apis", "apps", "v1", "namespaces"],
               let namespace = cluster.namespaces.first(where: { $0.name == pathParts[4] }) {
                return routeAppsNamespaced(pathParts: pathParts, namespace: namespace, query: query)
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
            return .text(status: 200, body: logLines(for: pod, namespace: namespace.name))
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
        default:
            return .json(status: 404, object: status(message: "Unsupported core namespaced route."))
        }
    }

    private func routeAppsNamespaced(
        pathParts: [String],
        namespace: RuneFakeK8sNamespace,
        query: [String: String]
    ) -> RuneFakeK8sHTTPResponse {
        switch pathParts.count {
        case 6 where pathParts[5] == "deployments":
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
        default:
            return .json(status: 404, object: status(message: "Unsupported apps namespaced route."))
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
                "creationTimestamp": "2026-04-25T10:00:00Z"
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
            metadata["remainingItemCount"] = remaining
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

    private func logLines(for pod: RuneFakeK8sPod, namespace: String) -> String {
        pod.containers.enumerated().map { index, container in
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
    func sendHTTP(_ response: RuneFakeK8sHTTPResponse) {
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
