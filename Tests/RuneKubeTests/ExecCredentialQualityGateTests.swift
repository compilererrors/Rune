import Foundation
import Network
import XCTest
@testable import RuneCore
@testable import RuneKube

final class ExecCredentialQualityGateTests: XCTestCase {
    func testExecInfoIsAlwaysSetAndOnlyIncludesClusterWhenRequested() async throws {
        let withoutCluster = try makeFixture()
        defer { withoutCluster.remove() }
        let withoutCapture = withoutCluster.directory.appendingPathComponent("exec-info-without-cluster.json")
        let withoutPlugin = try writePlugin(
            in: withoutCluster.directory,
            name: "capture-without-cluster.sh",
            payload: validCredentialPayload(),
            preflight: "printf '%s' \"${KUBERNETES_EXEC_INFO-}\" > \"$CAPTURE_FILE\""
        )
        try withoutCluster.writeKubeconfig(userYAML: execYAML(
            apiVersion: "client.authentication.k8s.io/v1",
            command: withoutPlugin.path,
            interactiveMode: "Never",
            environment: ["CAPTURE_FILE": withoutCapture.path]
        ))

        _ = try await resolvedNamespace(for: withoutCluster.kubeconfig)
        let withoutInfo = try readJSONObject(at: withoutCapture)
        XCTAssertEqual(withoutInfo["apiVersion"] as? String, "client.authentication.k8s.io/v1")
        XCTAssertEqual(withoutInfo["kind"] as? String, "ExecCredential")
        let withoutSpec = try XCTUnwrap(withoutInfo["spec"] as? [String: Any])
        XCTAssertEqual(withoutSpec["interactive"] as? Bool, false)
        XCTAssertNil(withoutSpec["cluster"], "Cluster information must be omitted unless provideClusterInfo is true")

        let withCluster = try makeFixture()
        defer { withCluster.remove() }
        let withCapture = withCluster.directory.appendingPathComponent("exec-info-with-cluster.json")
        let withPlugin = try writePlugin(
            in: withCluster.directory,
            name: "capture-with-cluster.sh",
            payload: validCredentialPayload(),
            preflight: "printf '%s' \"${KUBERNETES_EXEC_INFO-}\" > \"$CAPTURE_FILE\""
        )
        let certificateAuthorityData = Data("synthetic-ca".utf8).base64EncodedString()
        try withCluster.writeKubeconfig(
            userYAML: execYAML(
                apiVersion: "client.authentication.k8s.io/v1",
                command: withPlugin.path,
                interactiveMode: "Never",
                provideClusterInfo: true,
                environment: ["CAPTURE_FILE": withCapture.path]
            ),
            clusterYAML: """
            certificate-authority-data: \(certificateAuthorityData)
            insecure-skip-tls-verify: true
            tls-server-name: synthetic.internal
            proxy-url: http://127.0.0.1:18080
            disable-compression: true
            extensions:
            - name: client.authentication.k8s.io/exec
              extension:
                audience: synthetic-audience
                nested:
                  enabled: true
            """
        )

        _ = try await resolvedNamespace(for: withCluster.kubeconfig)
        let withInfo = try readJSONObject(at: withCapture)
        let withSpec = try XCTUnwrap(withInfo["spec"] as? [String: Any])
        let cluster = try XCTUnwrap(withSpec["cluster"] as? [String: Any])
        XCTAssertEqual(cluster["server"] as? String, Fixture.serverURL)
        XCTAssertEqual(cluster["certificate-authority-data"] as? String, certificateAuthorityData)
        XCTAssertEqual(cluster["insecure-skip-tls-verify"] as? Bool, true)
        XCTAssertEqual(cluster["tls-server-name"] as? String, "synthetic.internal")
        XCTAssertEqual(cluster["proxy-url"] as? String, "http://127.0.0.1:18080")
        XCTAssertEqual(cluster["disable-compression"] as? Bool, true)
        let config = try XCTUnwrap(cluster["config"] as? [String: Any])
        XCTAssertEqual(config["audience"] as? String, "synthetic-audience")
        let nested = try XCTUnwrap(config["nested"] as? [String: Any])
        XCTAssertEqual(nested["enabled"] as? Bool, true)
    }

    func testV1RequiresInteractiveModeBeforeStartingPlugin() async throws {
        let fixture = try makeFixture()
        defer { fixture.remove() }
        let marker = fixture.directory.appendingPathComponent("v1-plugin-started")
        let plugin = try writePlugin(
            in: fixture.directory,
            name: "v1-missing-interactive-mode.sh",
            payload: validCredentialPayload(),
            preflight: "printf started > \"$MARKER_FILE\""
        )
        try fixture.writeKubeconfig(userYAML: execYAML(
            apiVersion: "client.authentication.k8s.io/v1",
            command: plugin.path,
            environment: ["MARKER_FILE": marker.path]
        ))

        let message = await failureMessage(for: fixture.kubeconfig)
        XCTAssertTrue(message.contains("v1 requires interactiveMode"), message)
        XCTAssertFalse(FileManager.default.fileExists(atPath: marker.path))
    }

    func testV1Beta1DefaultsInteractiveModeToIfAvailable() async throws {
        let fixture = try makeFixture()
        defer { fixture.remove() }
        let marker = fixture.directory.appendingPathComponent("v1beta1-plugin-started")
        let capture = fixture.directory.appendingPathComponent("v1beta1-exec-info.json")
        let plugin = try writePlugin(
            in: fixture.directory,
            name: "v1beta1-default-interactive-mode.sh",
            payload: validCredentialPayload(apiVersion: "client.authentication.k8s.io/v1beta1"),
            preflight: """
            printf started > "$MARKER_FILE"
            printf '%s' "${KUBERNETES_EXEC_INFO-}" > "$CAPTURE_FILE"
            """
        )
        try fixture.writeKubeconfig(userYAML: execYAML(
            apiVersion: "client.authentication.k8s.io/v1beta1",
            command: plugin.path,
            environment: ["CAPTURE_FILE": capture.path, "MARKER_FILE": marker.path]
        ))

        let namespace = try await resolvedNamespace(for: fixture.kubeconfig)
        XCTAssertEqual(namespace, "synthetic-namespace")
        XCTAssertTrue(FileManager.default.fileExists(atPath: marker.path))
        let execInfo = try readJSONObject(at: capture)
        XCTAssertEqual(execInfo["apiVersion"] as? String, "client.authentication.k8s.io/v1beta1")
        let spec = try XCTUnwrap(execInfo["spec"] as? [String: Any])
        XCTAssertEqual(spec["interactive"] as? Bool, false)
    }

    func testInteractiveModeAlwaysDoesNotStartPlugin() async throws {
        let fixture = try makeFixture()
        defer { fixture.remove() }
        let marker = fixture.directory.appendingPathComponent("always-plugin-started")
        let plugin = try writePlugin(
            in: fixture.directory,
            name: "always.sh",
            payload: validCredentialPayload(),
            preflight: "printf started > \"$MARKER_FILE\""
        )
        try fixture.writeKubeconfig(userYAML: execYAML(
            apiVersion: "client.authentication.k8s.io/v1",
            command: plugin.path,
            interactiveMode: "Always",
            environment: ["MARKER_FILE": marker.path]
        ))

        let message = await failureMessage(for: fixture.kubeconfig)
        XCTAssertTrue(message.contains("requires interactive stdin"), message)
        XCTAssertFalse(FileManager.default.fileExists(atPath: marker.path))
    }

    func testRelativeSlashCommandResolvesFromKubeconfigDirectory() async throws {
        let fixture = try makeFixture()
        defer { fixture.remove() }
        let pluginDirectory = fixture.directory.appendingPathComponent("plugins", isDirectory: true)
        try FileManager.default.createDirectory(at: pluginDirectory, withIntermediateDirectories: true)
        _ = try writePlugin(
            in: pluginDirectory,
            name: "synthetic-auth.sh",
            payload: validCredentialPayload()
        )
        try fixture.writeKubeconfig(userYAML: execYAML(
            apiVersion: "client.authentication.k8s.io/v1",
            command: "plugins/synthetic-auth.sh",
            interactiveMode: "Never"
        ))

        let namespace = try await resolvedNamespace(for: fixture.kubeconfig)
        XCTAssertEqual(namespace, "synthetic-namespace")
    }

    func testInstallHintIsReportedForMissingAndNonExecutablePlugins() async throws {
        for executableState in [ExecutableState.missing, .notExecutable] {
            let fixture = try makeFixture()
            defer { fixture.remove() }
            let command = fixture.directory.appendingPathComponent("synthetic-helper").path
            if case .notExecutable = executableState {
                try "#!/bin/sh\nexit 0\n".write(
                    to: URL(fileURLWithPath: command),
                    atomically: true,
                    encoding: .utf8
                )
                try FileManager.default.setAttributes([.posixPermissions: 0o644], ofItemAtPath: command)
            }
            try fixture.writeKubeconfig(userYAML: execYAML(
                apiVersion: "client.authentication.k8s.io/v1",
                command: command,
                interactiveMode: "Never",
                installHint: "Install the synthetic authentication helper."
            ))

            let message = await failureMessage(for: fixture.kubeconfig)
            XCTAssertTrue(message.contains("Install the synthetic authentication helper."), message)
            switch executableState {
            case .missing:
                XCTAssertTrue(message.contains("Executable was not found"), message)
            case .notExecutable:
                XCTAssertTrue(message.contains("Configured file is not executable"), message)
            }
        }
    }

    func testMalformedExecCredentialKindAPIVersionAndExpirationAreRejected() async throws {
        let cases = [
            InvalidResponseCase(
                name: "wrong-kind",
                payload: #"{"apiVersion":"client.authentication.k8s.io/v1","kind":"Status","status":{"token":"synthetic-token"}}"#,
                expectedMessage: "returned kind Status, expected ExecCredential"
            ),
            InvalidResponseCase(
                name: "wrong-api-version",
                payload: #"{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential","status":{"token":"synthetic-token"}}"#,
                expectedMessage: "expected client.authentication.k8s.io/v1"
            ),
            InvalidResponseCase(
                name: "invalid-expiration",
                payload: #"{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"token":"synthetic-token","expirationTimestamp":"not-a-timestamp"}}"#,
                expectedMessage: "invalid expirationTimestamp"
            )
        ]

        for item in cases {
            let fixture = try makeFixture()
            defer { fixture.remove() }
            let plugin = try writePlugin(
                in: fixture.directory,
                name: "\(item.name).sh",
                payload: item.payload
            )
            try fixture.writeKubeconfig(userYAML: execYAML(
                apiVersion: "client.authentication.k8s.io/v1",
                command: plugin.path,
                interactiveMode: "Never"
            ))

            let message = await failureMessage(for: fixture.kubeconfig)
            XCTAssertTrue(message.contains(item.expectedMessage), "\(item.name): \(message)")
        }
    }

    func testUnreadableTokenFileDoesNotFallBackToExecPlugin() async throws {
        let fixture = try makeFixture()
        defer { fixture.remove() }
        let marker = fixture.directory.appendingPathComponent("fallback-plugin-started")
        let plugin = try writePlugin(
            in: fixture.directory,
            name: "must-not-run.sh",
            payload: validCredentialPayload(),
            preflight: "printf started > \"$MARKER_FILE\""
        )
        let exec = execYAML(
            apiVersion: "client.authentication.k8s.io/v1",
            command: plugin.path,
            interactiveMode: "Never",
            environment: ["MARKER_FILE": marker.path]
        )
        try fixture.writeKubeconfig(userYAML: """
        tokenFile: missing-token.txt
        \(exec)
        """)

        let message = await failureMessage(for: fixture.kubeconfig)
        XCTAssertTrue(message.contains("tokenFile could not be read"), message)
        XCTAssertFalse(FileManager.default.fileExists(atPath: marker.path))
    }

    func testConcurrentExecResolutionIsSingleFlightAndCachedWithinKPI() async throws {
        let fixture = try makeFixture()
        defer { fixture.remove() }
        let counter = fixture.directory.appendingPathComponent("invocation-count")
        let plugin = try writePlugin(
            in: fixture.directory,
            name: "counted-auth.sh",
            payload: validCredentialPayload(expirationTimestamp: "2099-01-02T03:04:05Z"),
            preflight: """
            while ! mkdir "${COUNTER_FILE}.lock" 2>/dev/null; do sleep 0.01; done
            count=0
            if [ -f "$COUNTER_FILE" ]; then count=$(cat "$COUNTER_FILE"); fi
            count=$((count + 1))
            printf '%s' "$count" > "$COUNTER_FILE"
            rmdir "${COUNTER_FILE}.lock"
            sleep 1
            """
        )
        try fixture.writeKubeconfig(userYAML: execYAML(
            apiVersion: "client.authentication.k8s.io/v1",
            command: plugin.path,
            interactiveMode: "Never",
            environment: ["COUNTER_FILE": counter.path]
        ))

        let client = KubernetesRESTClient()
        let environment = ["KUBECONFIG": fixture.kubeconfig.path]
        let started = Date()
        let concurrency = 24
        try await withThrowingTaskGroup(of: String?.self) { group in
            for _ in 0..<concurrency {
                group.addTask {
                    try await client.contextNamespace(
                        environment: environment,
                        contextName: Fixture.contextName
                    )
                }
            }
            for try await namespace in group {
                XCTAssertEqual(namespace, "synthetic-namespace")
            }
        }
        for _ in 0..<5 {
            _ = try await client.contextNamespace(
                environment: environment,
                contextName: Fixture.contextName
            )
        }
        let elapsed = Date().timeIntervalSince(started)

        XCTAssertEqual(try String(contentsOf: counter, encoding: .utf8), "1")
        XCTAssertLessThan(
            elapsed,
            8,
            "KPI: 24 concurrent resolutions plus 5 cache hits must complete within 8 seconds on local fixtures (actual \(elapsed)s)"
        )
        let diagnostic = try await client.execCredentialCacheDiagnostic(
            environment: environment,
            contextName: Fixture.contextName
        )
        XCTAssertEqual(diagnostic?.state, .hit)
    }

    func testExpiredExecCredentialRunsPluginAgainOnNextResolution() async throws {
        let fixture = try makeFixture()
        defer { fixture.remove() }
        let counter = fixture.directory.appendingPathComponent("expired-invocation-count")
        let plugin = try writePlugin(
            in: fixture.directory,
            name: "expired-auth.sh",
            payload: validCredentialPayload(expirationTimestamp: "2000-01-02T03:04:05Z"),
            preflight: sequentialCounterPreflight
        )
        try fixture.writeKubeconfig(userYAML: execYAML(
            apiVersion: "client.authentication.k8s.io/v1",
            command: plugin.path,
            interactiveMode: "Never",
            environment: ["COUNTER_FILE": counter.path]
        ))

        let client = KubernetesRESTClient()
        let environment = ["KUBECONFIG": fixture.kubeconfig.path]
        _ = try await client.contextNamespace(
            environment: environment,
            contextName: Fixture.contextName
        )
        _ = try await client.contextNamespace(
            environment: environment,
            contextName: Fixture.contextName
        )

        XCTAssertEqual(try String(contentsOf: counter, encoding: .utf8), "2")
        let diagnostic = try await client.execCredentialCacheDiagnostic(
            environment: environment,
            contextName: Fixture.contextName
        )
        XCTAssertEqual(diagnostic?.state, .expired)
    }

    func testHTTP401InvalidatesExecCredentialWithoutBlindReplay() async throws {
        let server = try await UnauthorizedThenSuccessServer.start()
        defer { server.stop() }
        let fixture = try makeFixture()
        defer { fixture.remove() }
        let counter = fixture.directory.appendingPathComponent("unauthorized-invocation-count")
        let plugin = try writePlugin(
            in: fixture.directory,
            name: "unauthorized-auth.sh",
            payload: validCredentialPayload(expirationTimestamp: "2099-01-02T03:04:05Z"),
            preflight: sequentialCounterPreflight
        )
        try fixture.writeKubeconfig(
            userYAML: execYAML(
                apiVersion: "client.authentication.k8s.io/v1",
                command: plugin.path,
                interactiveMode: "Never",
                environment: ["COUNTER_FILE": counter.path]
            ),
            serverURL: "http://127.0.0.1:\(server.port)"
        )

        let client = KubernetesRESTClient()
        let environment = ["KUBECONFIG": fixture.kubeconfig.path]
        do {
            _ = try await client.listNamespaces(
                environment: environment,
                contextName: Fixture.contextName,
                timeout: 2
            )
            XCTFail("Expected the first explicit request to receive HTTP 401")
        } catch {
            let message = String(describing: error)
            XCTAssertTrue(message.contains("HTTP 401"), message)
        }

        XCTAssertEqual(server.requestCount, 1, "A 401 must not cause an automatic replay")
        XCTAssertEqual(try String(contentsOf: counter, encoding: .utf8), "1")

        let namespaces = try await client.listNamespaces(
            environment: environment,
            contextName: Fixture.contextName,
            timeout: 2
        )

        XCTAssertEqual(namespaces, ["synthetic-namespace"])
        XCTAssertEqual(server.requestCount, 2, "Two user calls must produce exactly two HTTP requests")
        XCTAssertEqual(
            try String(contentsOf: counter, encoding: .utf8),
            "2",
            "The request after HTTP 401 must resolve a fresh ExecCredential"
        )
    }

    private func resolvedNamespace(for kubeconfig: URL) async throws -> String? {
        try await KubernetesRESTClient().contextNamespace(
            environment: ["KUBECONFIG": kubeconfig.path],
            contextName: Fixture.contextName
        )
    }

    private func failureMessage(for kubeconfig: URL) async -> String {
        do {
            _ = try await resolvedNamespace(for: kubeconfig)
            XCTFail("Expected ExecCredential resolution to fail")
            return ""
        } catch {
            return String(describing: error)
        }
    }

    private func makeFixture() throws -> Fixture {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-exec-quality-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return Fixture(directory: directory)
    }

    private func writePlugin(
        in directory: URL,
        name: String,
        payload: String,
        preflight: String = ""
    ) throws -> URL {
        let plugin = directory.appendingPathComponent(name)
        let script = """
        #!/bin/sh
        set -eu
        \(preflight)
        cat <<'RUNE_EXEC_CREDENTIAL_JSON'
        \(payload)
        RUNE_EXEC_CREDENTIAL_JSON
        """
        try script.write(to: plugin, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: plugin.path)
        return plugin
    }

    private func validCredentialPayload(
        apiVersion: String = "client.authentication.k8s.io/v1",
        expirationTimestamp: String? = nil
    ) -> String {
        let expiration = expirationTimestamp.map { ",\"expirationTimestamp\":\"\($0)\"" } ?? ""
        return #"{"apiVersion":"\#(apiVersion)","kind":"ExecCredential","status":{"token":"synthetic-token"\#(expiration)}}"#
    }

    private var sequentialCounterPreflight: String {
        """
        count=0
        if [ -f "$COUNTER_FILE" ]; then count=$(cat "$COUNTER_FILE"); fi
        count=$((count + 1))
        printf '%s' "$count" > "$COUNTER_FILE"
        """
    }

    private func execYAML(
        apiVersion: String,
        command: String,
        interactiveMode: String? = nil,
        installHint: String? = nil,
        provideClusterInfo: Bool? = nil,
        environment: [String: String] = [:]
    ) -> String {
        var lines = [
            "exec:",
            "  apiVersion: \(yamlString(apiVersion))",
            "  command: \(yamlString(command))"
        ]
        if let interactiveMode {
            lines.append("  interactiveMode: \(yamlString(interactiveMode))")
        }
        if let installHint {
            lines.append("  installHint: \(yamlString(installHint))")
        }
        if let provideClusterInfo {
            lines.append("  provideClusterInfo: \(provideClusterInfo)")
        }
        if !environment.isEmpty {
            lines.append("  env:")
            for key in environment.keys.sorted() {
                lines.append("  - name: \(yamlString(key))")
                lines.append("    value: \(yamlString(environment[key] ?? ""))")
            }
        }
        return lines.joined(separator: "\n")
    }

    private func yamlString(_ value: String) -> String {
        "'\(value.replacingOccurrences(of: "'", with: "''"))'"
    }

    private func readJSONObject(at url: URL) throws -> [String: Any] {
        let object = try JSONSerialization.jsonObject(with: Data(contentsOf: url))
        return try XCTUnwrap(object as? [String: Any])
    }

    private enum ExecutableState {
        case missing
        case notExecutable
    }

    private struct InvalidResponseCase {
        let name: String
        let payload: String
        let expectedMessage: String
    }

    private struct Fixture {
        static let contextName = "synthetic-context"
        static let serverURL = "https://127.0.0.1:6443"

        let directory: URL

        var kubeconfig: URL {
            directory.appendingPathComponent("config.yaml")
        }

        func writeKubeconfig(
            userYAML: String,
            clusterYAML: String = "",
            serverURL: String = Self.serverURL
        ) throws {
            let user = indent(userYAML, spaces: 4)
            let cluster = clusterYAML.isEmpty ? "" : "\n\(indent(clusterYAML, spaces: 4))"
            let contents = """
            apiVersion: v1
            kind: Config
            current-context: \(Self.contextName)
            clusters:
            - name: synthetic-cluster
              cluster:
                server: \(serverURL)\(cluster)
            contexts:
            - name: \(Self.contextName)
              context:
                cluster: synthetic-cluster
                user: synthetic-user
                namespace: synthetic-namespace
            users:
            - name: synthetic-user
              user:
            \(user)
            """
            try contents.write(to: kubeconfig, atomically: true, encoding: .utf8)
        }

        func remove() {
            try? FileManager.default.removeItem(at: directory)
        }

        private func indent(_ value: String, spaces: Int) -> String {
            let prefix = String(repeating: " ", count: spaces)
            return value
                .split(separator: "\n", omittingEmptySubsequences: false)
                .map { prefix + $0 }
                .joined(separator: "\n")
        }
    }
}

private final class UnauthorizedThenSuccessServer: @unchecked Sendable {
    let port: UInt16

    private let listener: NWListener
    private let state: RequestState

    private init(listener: NWListener, state: RequestState, port: UInt16) {
        self.listener = listener
        self.state = state
        self.port = port
    }

    var requestCount: Int {
        state.requestCount
    }

    static func start() async throws -> UnauthorizedThenSuccessServer {
        let listener = try NWListener(using: .tcp, on: 0)
        let state = RequestState()
        listener.newConnectionHandler = { connection in
            connection.start(queue: DispatchQueue(label: "rune.exec-quality.connection"))
            connection.receive(minimumIncompleteLength: 1, maximumLength: 65_536) { data, _, _, _ in
                guard data?.isEmpty == false else {
                    connection.cancel()
                    return
                }
                let response = state.nextResponse()
                let reason = response.status == 401 ? "Unauthorized" : "OK"
                let header = [
                    "HTTP/1.1 \(response.status) \(reason)",
                    "Content-Type: application/json",
                    "Content-Length: \(response.body.utf8.count)",
                    "Connection: close",
                    "",
                    ""
                ].joined(separator: "\r\n")
                connection.send(content: Data((header + response.body).utf8), completion: .contentProcessed { _ in
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
                case let .failed(error):
                    continuation.resume(throwing: error)
                default:
                    break
                }
            }
            listener.start(queue: DispatchQueue(label: "rune.exec-quality.listener"))
        }
        listener.stateUpdateHandler = nil
        return UnauthorizedThenSuccessServer(listener: listener, state: state, port: port)
    }

    func stop() {
        listener.cancel()
    }

    private final class RequestState: @unchecked Sendable {
        private let lock = NSLock()
        private var count = 0

        var requestCount: Int {
            lock.lock()
            defer { lock.unlock() }
            return count
        }

        func nextResponse() -> (status: Int, body: String) {
            lock.lock()
            count += 1
            let current = count
            lock.unlock()
            if current == 1 {
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
