import Foundation
import Network
import XCTest
@testable import RuneCore
@testable import RuneKube
@testable import RuneSecurity

final class NativeCredentialRoutingIntegrationTests: XCTestCase {
    func testDefaultProviderE2ERoutesBoundAWSProfileThroughRESTWithoutLaunchingExec() async throws {
        let server = try await NativeAuthTestServer.start(responseMode: .alwaysSuccess)
        defer { server.stop() }
        let fixture = try NativeAuthFixture.make(serverPort: server.port)
        defer { fixture.remove() }
        let profileStore = NativeAuthMemoryProfileStore()
        let provider = DefaultKubernetesNativeCredentialProvider(profileStore: profileStore)
        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(
            source: KubeConfigSource(url: fixture.kubeconfig)
        )
        let request = try XCTUnwrap(
            analysis.contexts.first(where: { $0.contextName == fixture.contextName })?.credentialRequest
        )
        try await provider.bindAWSCredentials(
            to: request,
            credentials: try AWSEKSCredentials(
                accessKeyID: "AKIDSYNTHETIC00000000",
                secretAccessKey: "synthetic-secret-material",
                sessionToken: "synthetic-session-material"
            ),
            displayName: "Synthetic AWS"
        )
        let client = KubernetesRESTClient(nativeCredentialProvider: provider)

        let namespaces = try await client.listNamespaces(
            environment: fixture.environment,
            contextName: fixture.contextName,
            timeout: 2
        )

        XCTAssertEqual(namespaces, ["synthetic-namespace"])
        XCTAssertEqual(server.requestCount, 1)
        XCTAssertTrue(server.authorizationHeaders.first?.hasPrefix("Bearer k8s-aws-v1.") == true)
        XCTAssertFalse(FileManager.default.fileExists(atPath: fixture.pluginMarker.path))
        let status = try await provider.status(for: request)
        XCTAssertTrue(status.isConnected)
    }

    func testOIDCAuthProviderUsesNativeBearerWithoutExecPlugin() async throws {
        let server = try await NativeAuthTestServer.start(responseMode: .alwaysSuccess)
        defer { server.stop() }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-native-oidc-routing-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        try """
        apiVersion: v1
        kind: Config
        current-context: synthetic-oidc-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: http://127.0.0.1:\(server.port)
        contexts:
        - name: synthetic-oidc-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
            auth-provider:
              name: oidc
              config:
                idp-issuer-url: https://issuer.example.invalid
                client-id: synthetic-client
                id-token: synthetic-id-token
        """.write(to: kubeconfig, atomically: true, encoding: .utf8)
        let provider = FakeNativeCredentialProvider(behavior: .fixed(token: "native-oidc-token"))
        let client = KubernetesRESTClient(nativeCredentialProvider: provider)

        let namespaces = try await client.listNamespaces(
            environment: ["KUBECONFIG": kubeconfig.path],
            contextName: "synthetic-oidc-context",
            timeout: 2
        )

        XCTAssertEqual(namespaces, ["synthetic-namespace"])
        XCTAssertEqual(server.authorizationHeaders, ["Bearer native-oidc-token"])
        let snapshot = await provider.snapshot()
        XCTAssertEqual(snapshot.requests.first?.provider, .oidc)
        XCTAssertEqual(snapshot.requests.first?.authProvider?.name, "oidc")
    }

    func testKnownAWSExecUsesNativeBearerWithoutStartingPlugin() async throws {
        let server = try await NativeAuthTestServer.start(responseMode: .alwaysSuccess)
        defer { server.stop() }
        let fixture = try NativeAuthFixture.make(serverPort: server.port)
        defer { fixture.remove() }
        let provider = FakeNativeCredentialProvider(behavior: .fixed(token: "native-aws-token"))
        let client = KubernetesRESTClient(nativeCredentialProvider: provider)

        let namespaces = try await client.listNamespaces(
            environment: fixture.environment,
            contextName: fixture.contextName,
            timeout: 2
        )

        XCTAssertEqual(namespaces, ["synthetic-namespace"])
        XCTAssertEqual(server.requestCount, 1)
        XCTAssertEqual(server.authorizationHeaders, ["Bearer native-aws-token"])
        XCTAssertFalse(FileManager.default.fileExists(atPath: fixture.pluginMarker.path))

        let snapshot = await provider.snapshot()
        let request = try XCTUnwrap(snapshot.requests.first)
        XCTAssertEqual(snapshot.requests.count, 1)
        XCTAssertEqual(snapshot.invalidatedBindingIDs, [])
        XCTAssertEqual(request.provider, .awsEKS)
        XCTAssertEqual(request.contextName, fixture.contextName)
        XCTAssertEqual(request.clusterName, "synthetic-cluster")
        XCTAssertEqual(request.userName, "synthetic-user")
        XCTAssertEqual(request.server, "http://127.0.0.1:\(server.port)")
        XCTAssertEqual(request.exec?.command, "aws")
        XCTAssertEqual(
            request.exec?.arguments,
            ["eks", "get-token", "--cluster-name", "synthetic-cluster", "--region", "eu-north-1"]
        )
        XCTAssertTrue(request.bindingID.hasPrefix("native-k8s-v1:"))
    }

    func testHTTP401InvalidatesNativeCredentialWithoutBlindReplayAndNextUserRequestRefreshes() async throws {
        let server = try await NativeAuthTestServer.start(responseMode: .unauthorizedThenSuccess)
        defer { server.stop() }
        let fixture = try NativeAuthFixture.make(serverPort: server.port)
        defer { fixture.remove() }
        let provider = FakeNativeCredentialProvider(
            behavior: .freshAfterInvalidation(staleToken: "stale-native-token", freshToken: "fresh-native-token")
        )
        let client = KubernetesRESTClient(nativeCredentialProvider: provider)

        do {
            _ = try await client.listNamespaces(
                environment: fixture.environment,
                contextName: fixture.contextName,
                timeout: 2
            )
            XCTFail("Expected the first explicit request to receive HTTP 401")
        } catch {
            XCTAssertTrue(String(describing: error).contains("HTTP 401"), String(describing: error))
        }

        XCTAssertEqual(server.requestCount, 1, "A native credential 401 must not trigger an automatic replay")
        XCTAssertEqual(server.authorizationHeaders, ["Bearer stale-native-token"])
        XCTAssertFalse(FileManager.default.fileExists(atPath: fixture.pluginMarker.path))

        let afterUnauthorized = await provider.snapshot()
        let firstRequest = try XCTUnwrap(afterUnauthorized.requests.first)
        XCTAssertEqual(afterUnauthorized.requests.count, 1)
        XCTAssertEqual(afterUnauthorized.invalidatedBindingIDs, [firstRequest.bindingID])

        let namespaces = try await client.listNamespaces(
            environment: fixture.environment,
            contextName: fixture.contextName,
            timeout: 2
        )

        XCTAssertEqual(namespaces, ["synthetic-namespace"])
        XCTAssertEqual(server.requestCount, 2, "Only the second explicit user call may issue the second HTTP request")
        XCTAssertEqual(
            server.authorizationHeaders,
            ["Bearer stale-native-token", "Bearer fresh-native-token"]
        )
        XCTAssertFalse(FileManager.default.fileExists(atPath: fixture.pluginMarker.path))

        let afterSuccess = await provider.snapshot()
        XCTAssertEqual(afterSuccess.requests.count, 2)
        XCTAssertEqual(afterSuccess.requests.map(\.bindingID), [firstRequest.bindingID, firstRequest.bindingID])
        XCTAssertEqual(afterSuccess.invalidatedBindingIDs, [firstRequest.bindingID])
    }

    func testDirectBuildFallsBackToExecWhenNativeProviderReturnsNil() async throws {
        guard RuneExternalCommandPolicy.allowsExternalCommands else {
            throw XCTSkip("Exec fallback is intentionally disabled for the active distribution policy")
        }
        let server = try await NativeAuthTestServer.start(responseMode: .alwaysSuccess)
        defer { server.stop() }
        let fixture = try NativeAuthFixture.make(serverPort: server.port, execToken: "exec-fallback-token")
        defer { fixture.remove() }
        let provider = FakeNativeCredentialProvider(behavior: .unavailable)
        let client = KubernetesRESTClient(nativeCredentialProvider: provider)

        let namespaces = try await client.listNamespaces(
            environment: fixture.environment,
            contextName: fixture.contextName,
            timeout: 2
        )

        XCTAssertEqual(namespaces, ["synthetic-namespace"])
        XCTAssertEqual(server.requestCount, 1)
        XCTAssertEqual(server.authorizationHeaders, ["Bearer exec-fallback-token"])
        XCTAssertEqual(try String(contentsOf: fixture.pluginMarker, encoding: .utf8), "started")

        let snapshot = await provider.snapshot()
        XCTAssertEqual(snapshot.requests.count, 1)
        XCTAssertEqual(snapshot.requests.first?.provider, .awsEKS)
        XCTAssertEqual(snapshot.invalidatedBindingIDs, [])
    }
}

private actor NativeAuthMemoryProfileStore: KubernetesNativeAuthProfileStoring {
    private var values: [String: KubernetesNativeAuthStoredProfile] = [:]

    func profiles() async throws -> [KubernetesNativeAuthProfile] {
        values.values.map(\.profile)
    }

    func storedProfile(for bindingID: String) async throws -> KubernetesNativeAuthStoredProfile? {
        values[bindingID]
    }

    func save(profile: KubernetesNativeAuthProfile, secret: Data?) async throws {
        values[profile.bindingID] = KubernetesNativeAuthStoredProfile(profile: profile, secret: secret)
    }

    func removeProfile(for bindingID: String) async throws {
        values.removeValue(forKey: bindingID)
    }
}

private actor FakeNativeCredentialProvider: KubernetesNativeCredentialProviding {
    enum Behavior: Sendable {
        case fixed(token: String)
        case freshAfterInvalidation(staleToken: String, freshToken: String)
        case unavailable
    }

    struct Snapshot: Sendable {
        let requests: [KubernetesNativeCredentialRequest]
        let invalidatedBindingIDs: [String]
    }

    private let behavior: Behavior
    private var requests: [KubernetesNativeCredentialRequest] = []
    private var invalidatedBindingIDs: [String] = []

    init(behavior: Behavior) {
        self.behavior = behavior
    }

    func credential(for request: KubernetesNativeCredentialRequest) async throws -> KubernetesNativeCredential? {
        requests.append(request)
        switch behavior {
        case .fixed(let token):
            return KubernetesNativeCredential(bearerToken: token, expiresAt: Date().addingTimeInterval(300))
        case .freshAfterInvalidation(let staleToken, let freshToken):
            let token = invalidatedBindingIDs.contains(request.bindingID) ? freshToken : staleToken
            return KubernetesNativeCredential(bearerToken: token, expiresAt: Date().addingTimeInterval(300))
        case .unavailable:
            return nil
        }
    }

    func invalidateCredential(for bindingID: String) async {
        invalidatedBindingIDs.append(bindingID)
    }

    func snapshot() -> Snapshot {
        Snapshot(requests: requests, invalidatedBindingIDs: invalidatedBindingIDs)
    }
}

private struct NativeAuthFixture {
    static let contextName = "synthetic-context"

    let directory: URL
    let kubeconfig: URL
    let pluginMarker: URL

    var contextName: String { Self.contextName }

    var environment: [String: String] {
        [
            "KUBECONFIG": kubeconfig.path,
            "PATH": directory.path
        ]
    }

    static func make(serverPort: UInt16, execToken: String = "unexpected-exec-token") throws -> NativeAuthFixture {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-native-auth-routing-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        let pluginMarker = directory.appendingPathComponent("plugin-started")
        let plugin = directory.appendingPathComponent("aws")
        let pluginBody = """
        #!/bin/sh
        printf '%s' 'started' > "$MARKER_FILE"
        printf '%s\\n' '{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"token":"\(execToken)"}}'
        """
        try pluginBody.write(to: plugin, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: plugin.path)

        let contents = """
        apiVersion: v1
        kind: Config
        current-context: \(contextName)
        clusters:
        - name: synthetic-cluster
          cluster:
            server: http://127.0.0.1:\(serverPort)
        contexts:
        - name: \(contextName)
          context:
            cluster: synthetic-cluster
            user: synthetic-user
            namespace: synthetic-namespace
        users:
        - name: synthetic-user
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: aws
              args:
              - eks
              - get-token
              - --cluster-name
              - synthetic-cluster
              - --region
              - eu-north-1
              env:
              - name: MARKER_FILE
                value: '\(pluginMarker.path.replacingOccurrences(of: "'", with: "''"))'
              interactiveMode: Never
        """
        try contents.write(to: kubeconfig, atomically: true, encoding: .utf8)
        return NativeAuthFixture(directory: directory, kubeconfig: kubeconfig, pluginMarker: pluginMarker)
    }

    func remove() {
        try? FileManager.default.removeItem(at: directory)
    }
}

private final class NativeAuthTestServer: @unchecked Sendable {
    enum ResponseMode: Sendable {
        case alwaysSuccess
        case unauthorizedThenSuccess
    }

    let port: UInt16

    private let listener: NWListener
    private let state: State

    private init(listener: NWListener, state: State, port: UInt16) {
        self.listener = listener
        self.state = state
        self.port = port
    }

    var requestCount: Int { state.requestCount }
    var authorizationHeaders: [String] { state.authorizationHeaders }

    static func start(responseMode: ResponseMode) async throws -> NativeAuthTestServer {
        let listener = try NWListener(using: .tcp, on: 0)
        let state = State(responseMode: responseMode)
        listener.newConnectionHandler = { connection in
            connection.start(queue: DispatchQueue(label: "rune.native-auth-routing.connection"))
            connection.receive(minimumIncompleteLength: 1, maximumLength: 65_536) { data, _, _, _ in
                guard let data, !data.isEmpty else {
                    connection.cancel()
                    return
                }
                let response = state.response(for: String(decoding: data, as: UTF8.self))
                let reason = response.status == 401 ? "Unauthorized" : "OK"
                let headers = [
                    "HTTP/1.1 \(response.status) \(reason)",
                    "Content-Type: application/json",
                    "Content-Length: \(response.body.utf8.count)",
                    "Connection: close",
                    "",
                    ""
                ].joined(separator: "\r\n")
                connection.send(content: Data((headers + response.body).utf8), completion: .contentProcessed { _ in
                    connection.cancel()
                })
            }
        }

        let port: UInt16 = try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<UInt16, Error>) in
            listener.stateUpdateHandler = { listenerState in
                switch listenerState {
                case .ready:
                    guard let port = listener.port?.rawValue else {
                        continuation.resume(throwing: URLError(.cannotConnectToHost))
                        return
                    }
                    continuation.resume(returning: port)
                case .failed(let error):
                    continuation.resume(throwing: error)
                default:
                    break
                }
            }
            listener.start(queue: DispatchQueue(label: "rune.native-auth-routing.listener"))
        }
        listener.stateUpdateHandler = nil
        return NativeAuthTestServer(listener: listener, state: state, port: port)
    }

    func stop() {
        listener.cancel()
    }

    private final class State: @unchecked Sendable {
        private let lock = NSLock()
        private let responseMode: ResponseMode
        private var requestHeaders: [String] = []

        init(responseMode: ResponseMode) {
            self.responseMode = responseMode
        }

        var requestCount: Int {
            lock.withLock { requestHeaders.count }
        }

        var authorizationHeaders: [String] {
            lock.withLock {
                requestHeaders.compactMap { request in
                    request
                        .components(separatedBy: "\r\n")
                        .first { $0.lowercased().hasPrefix("authorization:") }?
                        .dropFirst("authorization:".count)
                        .trimmingCharacters(in: .whitespaces)
                }
            }
        }

        func response(for request: String) -> (status: Int, body: String) {
            let index = lock.withLock {
                requestHeaders.append(request)
                return requestHeaders.count
            }
            if responseMode == .unauthorizedThenSuccess, index == 1 {
                return (
                    401,
                    #"{"kind":"Status","status":"Failure","message":"synthetic unauthorized"}"#
                )
            }
            return (
                200,
                #"{"items":[{"metadata":{"name":"synthetic-namespace"}}]}"#
            )
        }
    }
}
