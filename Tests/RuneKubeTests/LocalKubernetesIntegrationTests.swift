import Foundation
import Network
import Security
import XCTest
@testable import RuneCore
@testable import RuneKube

final class LocalKubernetesIntegrationTests: XCTestCase {
    func testNativeRESTClientReadsPodsMetricsAndLogsFromLocalFixture() async throws {
        let server = try await LocalKubernetesAPIServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(serverURL: "http://127.0.0.1:\(server.port)")
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: "local-fixture")

        let namespaces = try await client.listNamespaces(from: sources, context: context)
        XCTAssertEqual(namespaces, ["default"])

        let pods = try await client.listPods(from: sources, context: context, namespace: "default")
        XCTAssertEqual(pods.map(\.name), ["api-0"])
        XCTAssertEqual(pods.first?.status, "Running")
        XCTAssertEqual(pods.first?.cpuUsage, "42m")
        XCTAssertEqual(pods.first?.memoryUsage, "64Mi")

        let logs = try await client.podLogs(
            from: sources,
            context: context,
            namespace: "default",
            podName: "api-0",
            filter: .tailLines(200),
            previous: false
        )
        XCTAssertEqual(logs, "line one\nline two\n")

        let multiContainerLogs = try await client.podLogs(
            from: sources,
            context: context,
            namespace: "default",
            podName: "multi-0",
            filter: .tailLines(200),
            previous: false
        )
        XCTAssertEqual(multiContainerLogs, "main line\nsidecar line\n")
    }

    func testNativeRESTClientDoesNotForceTextPlainAcceptHeaderForPodLogs() throws {
        let source = try String(contentsOfFile: kubernetesRESTClientPath, encoding: .utf8)

        guard let podLogsStart = source.range(of: "func podLogs("),
              let serviceSelectorStart = source.range(of: "func serviceSelector(", range: podLogsStart.upperBound..<source.endIndex) else {
            XCTFail("Could not locate podLogs implementation in KubernetesRESTClient.swift")
            return
        }

        let podLogsBlock = String(source[podLogsStart.lowerBound..<serviceSelectorStart.lowerBound])
        XCTAssertTrue(podLogsBlock.contains(#""Accept": "*/*""#))
        XCTAssertFalse(podLogsBlock.contains(#""Accept": "text/plain""#))
    }

    func testPortForwardListenerReportsWaitingAndStartupTimeout() throws {
        let source = try String(contentsOfFile: kubernetesRESTClientPath, encoding: .utf8)

        guard let handleStart = source.range(of: "private final class KubernetesPortForwardHandle"),
              let bridgeStart = source.range(of: "private final class PortForwardConnectionBridge", range: handleStart.upperBound..<source.endIndex) else {
            XCTFail("Could not locate KubernetesPortForwardHandle implementation")
            return
        }

        let handleBlock = String(source[handleStart.lowerBound..<bridgeStart.lowerBound])
        XCTAssertTrue(handleBlock.contains("case let .waiting(error):"))
        XCTAssertTrue(handleBlock.contains("Port-forward listener is waiting:"))
        XCTAssertTrue(handleBlock.contains("failIfListenerDidNotStart()"))
        XCTAssertTrue(handleBlock.contains("Timed out starting local port-forward listener."))
    }

    func testPortForwardLocalPortConflictMessageIncludesProcessOwner() async throws {
        let server = try await LocalKubernetesAPIServer.start()
        defer { server.stop() }

        guard let message = KubernetesRESTClient._testLocalPortConflictMessage(port: Int(server.port), address: "127.0.0.1") else {
            throw XCTSkip("lsof did not report the local listener owner")
        }

        XCTAssertTrue(message.contains("Port in use: 127.0.0.1:\(server.port)"))
        XCTAssertTrue(message.contains("pid "))
    }

    func testPortForwardLocalPortConflictMarksSessionFailed() async throws {
        let server = try await LocalKubernetesAPIServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(serverURL: "http://127.0.0.1:\(server.port)")
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let recorder = PortForwardSessionRecorder()

        do {
            _ = try await client.startPortForward(
                from: [KubeConfigSource(url: kubeconfig)],
                context: KubeContext(name: "local-fixture"),
                namespace: "default",
                targetKind: .pod,
                targetName: "api-0",
                localPort: Int(server.port),
                remotePort: 80,
                address: "127.0.0.1",
                onEvent: { session in
                    recorder.append(session)
                }
            )
            XCTFail("Expected occupied local port to fail")
        } catch {
            XCTAssertTrue(error.localizedDescription.contains("Port in use"))
        }

        let sessions = recorder.sessions()
        XCTAssertEqual(sessions.map(\.status), [.starting, .failed])
        XCTAssertTrue(sessions.last?.lastMessage.contains("Port in use") == true)
        XCTAssertTrue(sessions.last?.lastMessage.contains("pid ") == true)
    }

    func testNativeHelmReleaseReaderDecodesV3StorageWithoutHelmBinary() async throws {
        let server = try await LocalKubernetesAPIServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(serverURL: "http://127.0.0.1:\(server.port)")
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: "local-fixture")

        let releases = try await client.listReleases(
            from: sources,
            context: context,
            namespace: "default",
            allNamespaces: false
        )

        XCTAssertEqual(releases.count, 1)
        XCTAssertEqual(releases.first?.name, "orbit")
        XCTAssertEqual(releases.first?.namespace, "default")
        XCTAssertEqual(releases.first?.revision, 3)
        XCTAssertEqual(releases.first?.status, "deployed")
        XCTAssertEqual(releases.first?.chart, "orbit-chart-1.2.3")
        XCTAssertEqual(releases.first?.appVersion, "4.5.6")

        let values = try await client.releaseValues(
            from: sources,
            context: context,
            namespace: "default",
            releaseName: "orbit"
        )
        XCTAssertTrue(values.contains("replicaCount: 2"))
        XCTAssertTrue(values.contains("tag: 4.5.6"))

        let manifest = try await client.releaseManifest(
            from: sources,
            context: context,
            namespace: "default",
            releaseName: "orbit"
        )
        XCTAssertTrue(manifest.contains("kind: ConfigMap"))

        let history = try await client.releaseHistory(
            from: sources,
            context: context,
            namespace: "default",
            releaseName: "orbit"
        )
        XCTAssertEqual(history.map(\.revision), [3])
    }

    func testNativeRESTClientUsesKubeconfigExecTokenAuth() async throws {
        let server = try await LocalKubernetesAPIServer.start()
        defer { server.stop() }
        let execPlugin = try writeExecCredentialPlugin(token: "exec-token", requireExecInfo: true)
        defer { try? FileManager.default.removeItem(at: execPlugin) }
        let kubeconfig = try writeKubeconfig(
            serverURL: "http://127.0.0.1:\(server.port)",
            userYAML: """
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: \(execPlugin.path)
              provideClusterInfo: true
            """
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let namespaces = try await client.listNamespaces(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: "local-fixture")
        )

        XCTAssertEqual(namespaces, ["default"])
    }

    private var kubernetesRESTClientPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneKube/KubernetesAPI/KubernetesRESTClient.swift").path
    }

    func testTokenAuthIgnoresUnusableClientCertificateFields() async throws {
        let server = try await LocalKubernetesAPIServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(
            serverURL: "http://127.0.0.1:\(server.port)",
            userYAML: """
            token: local-token
            client-certificate-data: bm90LWEtY2VydA==
            client-key-data: bm90LWEta2V5
            """
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let namespaces = try await client.listNamespaces(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: "local-fixture")
        )

        XCTAssertEqual(namespaces, ["default"])
    }

    func testNativeRESTClientBuildsTLSIdentityFromKubeconfigClientCertificateAndKey() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-mtls-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let certificate = directory.appendingPathComponent("client.crt")
        let key = directory.appendingPathComponent("client.key")
        let status = runOpenSSL([
            "req", "-x509",
            "-newkey", "rsa:2048",
            "-nodes",
            "-keyout", key.path,
            "-out", certificate.path,
            "-subj", "/CN=rune-native-mtls-test",
            "-days", "1"
        ])
        guard status == 0 else {
            throw XCTSkip("openssl is not available for local mTLS identity generation")
        }

        let identityCreated = try KubernetesRESTClient._testCreateClientTLSIdentity(
            certificateData: Data(contentsOf: certificate),
            keyData: Data(contentsOf: key)
        )

        XCTAssertTrue(identityCreated)
    }

    func testTokenAuthDoesNotAttachExtraClientCertificateByDefault() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-token-mtls-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let certificate = directory.appendingPathComponent("client.crt")
        let key = directory.appendingPathComponent("client.key")
        let status = runOpenSSL([
            "req", "-x509",
            "-newkey", "rsa:2048",
            "-nodes",
            "-keyout", key.path,
            "-out", certificate.path,
            "-subj", "/CN=rune-token-native-mtls-test",
            "-days", "1"
        ])
        guard status == 0 else {
            throw XCTSkip("openssl is not available for local mTLS identity generation")
        }

        let certificateData = try Data(contentsOf: certificate).base64EncodedString()
        let keyData = try Data(contentsOf: key).base64EncodedString()
        let kubeconfig = try writeKubeconfig(
            serverURL: "https://127.0.0.1:6443",
            userYAML: """
            token: local-token
            client-certificate-data: \(certificateData)
            client-key-data: \(keyData)
            """
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let tlsDescription = try await KubernetesRESTClient._testResolvedTLSDescription(
            environment: ["KUBECONFIG": kubeconfig.path],
            contextName: "local-fixture"
        )

        XCTAssertFalse(tlsDescription.contains("client-certificate"))
    }

    func testNativeRESTClientListsNamespacesWhenServerRequiresClientCertificateAndX509IsAuthMethod() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-mtls-required-k8s-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let serverMaterial = try makeLocalServerTLSMaterial(in: directory, prefix: "server")
        let server = try await LocalKubernetesAPIServer.start(
            tlsIdentity: serverMaterial.serverIdentity,
            requireClientCertificate: true
        )
        defer { server.stop() }

        let clientCertificate = directory.appendingPathComponent("client.crt")
        let clientKey = directory.appendingPathComponent("client.key")
        let status = runOpenSSL([
            "req", "-x509",
            "-newkey", "rsa:2048",
            "-nodes",
            "-keyout", clientKey.path,
            "-out", clientCertificate.path,
            "-subj", "/CN=rune-required-mtls-client",
            "-days", "1"
        ])
        guard status == 0 else {
            throw XCTSkip("openssl is not available for local mTLS client certificate generation")
        }

        let kubeconfig = try writeKubeconfig(
            serverURL: "https://127.0.0.1:\(server.port)",
            userYAML: """
            client-certificate-data: \(try Data(contentsOf: clientCertificate).base64EncodedString())
            client-key-data: \(try Data(contentsOf: clientKey).base64EncodedString())
            """,
            clusterYAML: "certificate-authority-data: \(serverMaterial.caCertificateData.base64EncodedString())"
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let namespaces = try await KubernetesRESTClient().listNamespaces(
            environment: ["KUBECONFIG": kubeconfig.path],
            contextName: "local-fixture",
            timeout: 5
        )

        XCTAssertEqual(namespaces, ["default"])
    }

    func testNativeRESTClientFailsWhenServerRequiresClientCertificateAndOnlyTokenExists() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-mtls-required-no-client-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let serverMaterial = try makeLocalServerTLSMaterial(in: directory, prefix: "server")
        let server = try await LocalKubernetesAPIServer.start(
            tlsIdentity: serverMaterial.serverIdentity,
            requireClientCertificate: true
        )
        defer { server.stop() }

        let kubeconfig = try writeKubeconfig(
            serverURL: "https://127.0.0.1:\(server.port)",
            userYAML: "token: local-token",
            clusterYAML: "certificate-authority-data: \(serverMaterial.caCertificateData.base64EncodedString())"
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        do {
            _ = try await KubernetesRESTClient().listNamespaces(
                environment: ["KUBECONFIG": kubeconfig.path],
                contextName: "local-fixture",
                timeout: 5
            )
            XCTFail("Expected TLS client certificate requirement to reject token-only credentials")
        } catch {
            let message = String(describing: error)
            XCTAssertTrue(
                message.localizedCaseInsensitiveContains("certificate") ||
                message.localizedCaseInsensitiveContains("secure connection") ||
                message.localizedCaseInsensitiveContains("network connection"),
                "Unexpected error: \(message)"
            )
        }
    }

    func testNativeRESTClientListsNamespacesOverHTTPSWithKubeconfigCA() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-https-k8s-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let material = try makeLocalServerTLSMaterial(in: directory, prefix: "trusted")
        let server = try await LocalKubernetesAPIServer.start(tlsIdentity: material.serverIdentity)
        defer { server.stop() }

        let kubeconfig = try writeKubeconfig(
            serverURL: "https://127.0.0.1:\(server.port)",
            clusterYAML: "certificate-authority-data: \(material.caCertificateData.base64EncodedString())"
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 3)
        let namespaces = try await client.listNamespaces(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: "local-fixture")
        )

        XCTAssertEqual(namespaces, ["default"])
    }

    func testNativeRESTClientRejectsHTTPSWithWrongKubeconfigCA() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-https-wrong-ca-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let material = try makeLocalServerTLSMaterial(in: directory, prefix: "server")
        let wrongCA = try makeLocalCA(in: directory, prefix: "wrong")
        let server = try await LocalKubernetesAPIServer.start(tlsIdentity: material.serverIdentity)
        defer { server.stop() }

        let kubeconfig = try writeKubeconfig(
            serverURL: "https://127.0.0.1:\(server.port)",
            clusterYAML: "certificate-authority-data: \(wrongCA.base64EncodedString())"
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 3)
        do {
            _ = try await client.listNamespaces(
                from: [KubeConfigSource(url: kubeconfig)],
                context: KubeContext(name: "local-fixture")
            )
            XCTFail("Expected kubeconfig CA verification to reject a server signed by another CA")
        } catch {
            let message = String(describing: error)
            XCTAssertTrue(
                message.localizedCaseInsensitiveContains("trust") ||
                message.localizedCaseInsensitiveContains("certificate") ||
                message.localizedCaseInsensitiveContains("secure connection"),
                "Unexpected error: \(message)"
            )
        }
    }

    func testExecClientCertificateDataUsesPEMData() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-exec-pem-cert-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let certificate = directory.appendingPathComponent("client.crt")
        let key = directory.appendingPathComponent("client.key")
        let status = runOpenSSL([
            "req", "-x509",
            "-newkey", "rsa:2048",
            "-nodes",
            "-keyout", key.path,
            "-out", certificate.path,
            "-subj", "/CN=rune-exec-pem-client",
            "-days", "1"
        ])
        guard status == 0 else {
            throw XCTSkip("openssl is not available for local exec client certificate generation")
        }

        let payload: [String: Any] = [
            "apiVersion": "client.authentication.k8s.io/v1",
            "kind": "ExecCredential",
            "status": [
                "clientCertificateData": String(decoding: try Data(contentsOf: certificate), as: UTF8.self),
                "clientKeyData": String(decoding: try Data(contentsOf: key), as: UTF8.self)
            ]
        ]
        let json = String(decoding: try JSONSerialization.data(withJSONObject: payload, options: [.sortedKeys]), as: UTF8.self)
        let execPlugin = try writeExecCredentialPlugin(jsonPayload: json)
        defer { try? FileManager.default.removeItem(at: execPlugin) }

        let kubeconfig = try writeKubeconfig(
            serverURL: "https://127.0.0.1:6443",
            userYAML: """
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: \(execPlugin.path)
            """
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let tlsDescription = try await KubernetesRESTClient._testResolvedTLSDescription(
            environment: ["KUBECONFIG": kubeconfig.path],
            contextName: "local-fixture"
        )

        XCTAssertTrue(tlsDescription.contains("client-certificate"))
    }

    func testExecAuthRejectsAPIVersionMismatch() async throws {
        let execPlugin = try writeExecCredentialPlugin(
            jsonPayload: #"{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential","status":{"token":"exec-token"}}"#
        )
        defer { try? FileManager.default.removeItem(at: execPlugin) }

        let kubeconfig = try writeKubeconfig(
            serverURL: "https://127.0.0.1:6443",
            userYAML: """
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: \(execPlugin.path)
            """
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        do {
            _ = try await KubernetesRESTClient._testResolvedTLSDescription(
                environment: ["KUBECONFIG": kubeconfig.path],
                contextName: "local-fixture"
            )
            XCTFail("Expected exec auth apiVersion mismatch to be rejected")
        } catch {
            XCTAssertTrue(String(describing: error).contains("expected client.authentication.k8s.io/v1"))
        }
    }

    func testLiveKubeconfigContextListsNamespacesWhenExplicitlyEnabled() async throws {
        guard let contextName = ProcessInfo.processInfo.environment["RUNE_LIVE_K8S_CONTEXT"],
              !contextName.isEmpty else {
            throw XCTSkip("Set RUNE_LIVE_K8S_CONTEXT to run this against a real kubeconfig context")
        }

        let client = KubernetesClient(commandTimeout: 10)
        let kubeconfig = ProcessInfo.processInfo.environment["RUNE_LIVE_KUBECONFIG"]
            ?? "\(NSHomeDirectory())/.kube/config"
        let sources = kubeconfig
            .split(separator: ":")
            .map(String.init)
            .filter { !$0.isEmpty }
            .map { KubeConfigSource(url: URL(fileURLWithPath: NSString(string: $0).expandingTildeInPath)) }
        let namespaces = try await client.listNamespaces(
            from: sources,
            context: KubeContext(name: contextName)
        )

        XCTAssertFalse(namespaces.isEmpty)
    }

    func testLiveKubeconfigContextsListNamespacesWhenExplicitlyEnabled() async throws {
        let rawContexts = ProcessInfo.processInfo.environment["RUNE_LIVE_K8S_CONTEXTS"] ?? ""
        let contextNames = rawContexts
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        guard !contextNames.isEmpty else {
            throw XCTSkip("Set RUNE_LIVE_K8S_CONTEXTS to a comma-separated list of real contexts")
        }

        let client = KubernetesClient(commandTimeout: 10)
        let kubeconfig = ProcessInfo.processInfo.environment["RUNE_LIVE_KUBECONFIG"]
            ?? "\(NSHomeDirectory())/.kube/config"
        let sources = kubeconfig
            .split(separator: ":")
            .map(String.init)
            .filter { !$0.isEmpty }
            .map { KubeConfigSource(url: URL(fileURLWithPath: NSString(string: $0).expandingTildeInPath)) }

        var failures: [String] = []
        for contextName in contextNames {
            do {
                let namespaces = try await client.listNamespaces(
                    from: sources,
                    context: KubeContext(name: contextName)
                )
                if namespaces.isEmpty {
                    failures.append("\(contextName): empty namespace list")
                }
            } catch {
                failures.append("\(contextName): \(error)")
            }
        }

        XCTAssertTrue(failures.isEmpty, failures.joined(separator: "\n"))
    }

    func testRuneFakeK8sEventsPointAtExistingPods() throws {
        guard ProcessInfo.processInfo.environment["RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS"] == "1" else {
            throw XCTSkip("Set RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 to run guarded fake-k8s integration tests")
        }
        guard let binary = ProcessInfo.processInfo.environment["RUNE_FAKE_K8S_BINARY"],
              !binary.isEmpty,
              FileManager.default.isExecutableFile(atPath: binary) else {
            throw XCTSkip("Set RUNE_FAKE_K8S_BINARY to the RuneFakeK8s executable")
        }

        let podsJSON = try runFakeK8s(binary: binary, arguments: ["kubectl", "get", "pods", "-A", "-o", "json"])
        let eventsJSON = try runFakeK8s(binary: binary, arguments: ["kubectl", "get", "events", "-A", "-o", "json"])
        let podKeys = try Set(jsonItems(from: podsJSON).compactMap { item -> String? in
            guard let metadata = item["metadata"] as? [String: Any],
                  let namespace = metadata["namespace"] as? String,
                  let name = metadata["name"] as? String else {
                return nil
            }
            return "\(namespace)/\(name)"
        })
        let eventTargets = try jsonItems(from: eventsJSON).compactMap { item -> String? in
            guard let involvedObject = item["involvedObject"] as? [String: Any],
                  let kind = involvedObject["kind"] as? String,
                  kind == "Pod",
                  let namespace = involvedObject["namespace"] as? String,
                  let name = involvedObject["name"] as? String else {
                return nil
            }
            return "\(namespace)/\(name)"
        }

        XCTAssertFalse(podKeys.isEmpty)
        XCTAssertFalse(eventTargets.isEmpty)
        XCTAssertTrue(eventTargets.allSatisfy { podKeys.contains($0) })
    }

    func testDockerComposeFakeK8sResourceGraphAndEventsAreLocalAndResolvable() async throws {
        let fixture = try dockerComposeFakeK8sFixture()
        let client = KubernetesClient(commandTimeout: 10)
        let context = KubeContext(name: "fake-lattice-spark")
        let namespace = "delta-zone"
        let appName = "rune-it-http-\(Self.shortTestID())"
        let localPort = try await availableLocalPort()

        defer {
            Task {
                try? await client.deleteResource(from: fixture.sources, context: context, namespace: namespace, kind: .service, name: appName)
                try? await client.deleteResource(from: fixture.sources, context: context, namespace: namespace, kind: .deployment, name: appName)
            }
        }

        let namespaces = try await client.listNamespaces(from: fixture.sources, context: context)
        XCTAssertTrue(namespaces.contains(namespace))

        try await client.applyYAML(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            yaml: Self.httpDeploymentYAML(name: appName, namespace: namespace, message: "rune docker fake http")
        )
        try await client.applyYAML(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            yaml: Self.httpServiceYAML(name: appName, namespace: namespace)
        )

        let pod = try await waitForRunningPod(
            client: client,
            sources: fixture.sources,
            context: context,
            namespace: namespace,
            namePrefix: appName,
            minimumCount: 1
        ).first!
        XCTAssertEqual(pod.status, "Running")

        let events = try await waitForPodEvent(
            client: client,
            sources: fixture.sources,
            context: context,
            namespace: namespace,
            namePrefix: appName
        )
        XCTAssertTrue(events.contains { $0.involvedKind == "Pod" && $0.objectName.hasPrefix(appName) })

        let services = try await client.listServices(from: fixture.sources, context: context, namespace: namespace)
        XCTAssertTrue(services.contains { $0.name == appName })

        let recorder = PortForwardSessionRecorder()
        let session = try await client.startPortForward(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            targetKind: .service,
            targetName: appName,
            localPort: localPort,
            remotePort: 8080,
            address: "127.0.0.1",
            onEvent: { recorder.append($0) }
        )
        _ = try await recorder.waitForStatus(.active, timeout: 10)
        defer { Task { await client.stopPortForward(sessionID: session.id) } }

        let body: String
        do {
            body = try await httpGET("http://127.0.0.1:\(localPort)/healthz")
        } catch {
            throw RuneError.commandFailed(
                command: "http get through port-forward",
                message: "\(error.localizedDescription). Port-forward sessions: \(recorder.sessions().map { "\($0.status.rawValue):\($0.lastMessage)" }.joined(separator: " | "))"
            )
        }
        XCTAssertTrue(body.contains("rune docker fake http ok"), body)
    }

    func testDockerComposeFakeK8sReadWriteOperationsAreReversible() async throws {
        let fixture = try dockerComposeFakeK8sFixture()
        let client = KubernetesClient(commandTimeout: 10)
        let context = KubeContext(name: "fake-orbit-mesh")
        let namespace = "alpha-zone"
        let configName = "rune-it-config-\(Self.shortTestID())"
        let deploymentName = "rune-it-editor-\(Self.shortTestID())"

        defer {
            Task {
                try? await client.deleteResource(from: fixture.sources, context: context, namespace: namespace, kind: .deployment, name: deploymentName)
                try? await client.deleteResource(from: fixture.sources, context: context, namespace: namespace, kind: .configMap, name: configName)
            }
        }

        let baselineConfig = Self.configMapYAML(name: configName, namespace: namespace, value: "baseline")
        let editedConfig = Self.configMapYAML(name: configName, namespace: namespace, value: "edited-from-yaml")
        try await client.applyYAML(from: fixture.sources, context: context, namespace: namespace, yaml: baselineConfig)

        let initialYAML = try await client.resourceYAML(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            kind: .configMap,
            name: configName
        )
        XCTAssertTrue(initialYAML.contains("baseline"))

        let validationIssues = try await client.validateResourceYAML(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            yaml: editedConfig
        )
        XCTAssertTrue(validationIssues.isEmpty)

        try await client.applyYAML(from: fixture.sources, context: context, namespace: namespace, yaml: editedConfig)
        let editedYAML = try await client.resourceYAML(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            kind: .configMap,
            name: configName
        )
        XCTAssertTrue(editedYAML.contains("edited-from-yaml"))

        try await client.applyYAML(from: fixture.sources, context: context, namespace: namespace, yaml: baselineConfig)
        let revertedYAML = try await client.resourceYAML(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            kind: .configMap,
            name: configName
        )
        XCTAssertTrue(revertedYAML.contains("baseline"))

        try await client.applyYAML(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            yaml: Self.httpDeploymentYAML(name: deploymentName, namespace: namespace, message: "rune editor manifest")
        )
        let pods = try await waitForRunningPod(
            client: client,
            sources: fixture.sources,
            context: context,
            namespace: namespace,
            namePrefix: deploymentName,
            minimumCount: 1
        )
        let exec = try await client.execInPod(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            podName: pods[0].name,
            container: nil,
            command: ["sh", "-c", "cat /usr/share/nginx/html/index.html"]
        )
        XCTAssertEqual(exec.exitCode, 0)
        XCTAssertTrue(exec.stdout.contains("rune editor manifest"))

        try await client.scaleDeployment(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            deploymentName: deploymentName,
            replicas: 2
        )
        _ = try await waitForRunningPod(
            client: client,
            sources: fixture.sources,
            context: context,
            namespace: namespace,
            namePrefix: deploymentName,
            minimumCount: 2
        )

        try await client.scaleDeployment(
            from: fixture.sources,
            context: context,
            namespace: namespace,
            deploymentName: deploymentName,
            replicas: 1
        )
    }

    private func writeKubeconfig(
        serverURL: String,
        userYAML: String = "token: local-token",
        clusterYAML: String = ""
    ) throws -> URL {
        let indentedUserYAML = userYAML
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map { "            \($0)" }
            .joined(separator: "\n")
        let indentedClusterYAML = clusterYAML.isEmpty
            ? ""
            : "\n" + clusterYAML
                .split(separator: "\n", omittingEmptySubsequences: false)
                .map { "            \($0)" }
                .joined(separator: "\n")
        let kubeconfig = """
        apiVersion: v1
        kind: Config
        current-context: local-fixture
        clusters:
        - name: local-cluster
          cluster:
            server: \(serverURL)
        \(indentedClusterYAML)
        contexts:
        - name: local-fixture
          context:
            cluster: local-cluster
            user: local-user
            namespace: default
        users:
        - name: local-user
          user:
        \(indentedUserYAML)
        """
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-local-k8s-\(UUID().uuidString).yaml")
        try kubeconfig.write(to: url, atomically: true, encoding: .utf8)
        return url
    }

    private struct DockerComposeFakeK8sFixture {
        let kubeconfig: URL
        let sources: [KubeConfigSource]
    }

    private func dockerComposeFakeK8sFixture() throws -> DockerComposeFakeK8sFixture {
        guard ProcessInfo.processInfo.environment["RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS"] == "1" else {
            throw XCTSkip("Set RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 to run guarded Docker fake-k8s integration tests")
        }

        let kubeconfig = repoRoot
            .appendingPathComponent("docker-compose/generated/rune-fake-kubeconfig.yaml")
        guard FileManager.default.fileExists(atPath: kubeconfig.path) else {
            throw XCTSkip("Run scripts/run-local-k8s-integration-report.sh or merge the Docker fake-k8s kubeconfig first")
        }

        let contents = try String(contentsOf: kubeconfig, encoding: .utf8)
        guard contents.contains("name: fake-orbit-mesh"),
              contents.contains("name: fake-lattice-spark"),
              contents.contains("server: https://127.0.0.1:16443"),
              contents.contains("server: https://127.0.0.1:17443") else {
            throw XCTSkip("Docker fake-k8s kubeconfig is not the expected localhost-only fixture")
        }

        return DockerComposeFakeK8sFixture(
            kubeconfig: kubeconfig,
            sources: [KubeConfigSource(url: kubeconfig)]
        )
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }

    private func waitForRunningPod(
        client: KubernetesClient,
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        namePrefix: String,
        minimumCount: Int,
        timeout: TimeInterval = 90
    ) async throws -> [PodSummary] {
        let deadline = Date().addingTimeInterval(timeout)
        var lastPods: [PodSummary] = []
        while Date() < deadline {
            let pods = try await client.listPods(from: sources, context: context, namespace: namespace)
                .filter { $0.name.hasPrefix(namePrefix) }
            lastPods = pods
            let running = pods.filter { $0.status.localizedCaseInsensitiveCompare("Running") == .orderedSame }
            if running.count >= minimumCount {
                return running
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        }
        throw RuneError.commandFailed(
            command: "wait for pods",
            message: "Timed out waiting for \(minimumCount) running pod(s) with prefix \(namePrefix). Last pods: \(lastPods.map { "\($0.name)=\($0.status)" }.joined(separator: ", "))"
        )
    }

    private func waitForPodEvent(
        client: KubernetesClient,
        sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        namePrefix: String,
        timeout: TimeInterval = 30
    ) async throws -> [EventSummary] {
        let deadline = Date().addingTimeInterval(timeout)
        var lastEvents: [EventSummary] = []
        while Date() < deadline {
            let events = try await client.listEvents(from: sources, context: context, namespace: namespace)
            lastEvents = events
            if events.contains(where: { $0.involvedKind == "Pod" && $0.objectName.hasPrefix(namePrefix) }) {
                return events
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        }
        throw RuneError.commandFailed(
            command: "wait for pod event",
            message: "Timed out waiting for a Pod event with prefix \(namePrefix). Last events: \(lastEvents.map { "\($0.involvedKind ?? "?")/\($0.objectName)" }.joined(separator: ", "))"
        )
    }

    private func availableLocalPort() async throws -> Int {
        for port in 20_000...45_000 {
            if KubernetesRESTClient._testLocalPortConflictMessage(port: port, address: "127.0.0.1") == nil {
                return port
            }
        }
        throw RuneError.commandFailed(command: "find local port", message: "No free local port found in test range.")
    }

    private func httpGET(_ rawURL: String, timeout: TimeInterval = 10) async throws -> String {
        guard let url = URL(string: rawURL) else {
            throw RuneError.invalidInput(message: "Invalid URL \(rawURL)")
        }
        let deadline = Date().addingTimeInterval(timeout)
        var lastError: Error?
        while Date() < deadline {
            do {
                let (data, _) = try await URLSession.shared.data(from: url)
                return String(decoding: data, as: UTF8.self)
            } catch {
                lastError = error
                try await Task.sleep(nanoseconds: 300_000_000)
            }
        }
        throw lastError ?? RuneError.commandFailed(command: "http get", message: "Timed out fetching \(rawURL)")
    }

    private static func shortTestID() -> String {
        String(UUID().uuidString.prefix(8)).lowercased()
    }

    private static func configMapYAML(name: String, namespace: String, value: String) -> String {
        """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: \(name)
          namespace: \(namespace)
          labels:
            app.kubernetes.io/managed-by: rune-integration-test
        data:
          editor-value: \(value)
        """
    }

    private static func httpDeploymentYAML(name: String, namespace: String, message: String) -> String {
        """
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: \(name)
          namespace: \(namespace)
          labels:
            app: \(name)
            app.kubernetes.io/managed-by: rune-integration-test
        spec:
          replicas: 1
          selector:
            matchLabels:
              app: \(name)
          template:
            metadata:
              labels:
                app: \(name)
            spec:
              containers:
                - name: http
                  image: nginx:1.27-alpine
                  ports:
                    - containerPort: 8080
                  command: ["/bin/sh", "-ec"]
                  args:
                    - |
                      cat >/usr/share/nginx/html/index.html <<'EOF'
                      \(message)
                      EOF
                      cat >/etc/nginx/conf.d/default.conf <<'EOF'
                      server {
                        listen 8080;
                        access_log /dev/stdout;
                        error_log /dev/stderr notice;
                        location / {
                          root /usr/share/nginx/html;
                        }
                        location /healthz {
                          default_type text/plain;
                          return 200 "\(message) ok\\n";
                        }
                      }
                      EOF
                      nginx -g 'daemon off;'
        """
    }

    private static func httpServiceYAML(name: String, namespace: String) -> String {
        """
        apiVersion: v1
        kind: Service
        metadata:
          name: \(name)
          namespace: \(namespace)
          labels:
            app: \(name)
            app.kubernetes.io/managed-by: rune-integration-test
        spec:
          selector:
            app: \(name)
          ports:
            - port: 8080
              targetPort: 8080
        """
    }

    private func runFakeK8s(binary: String, arguments: [String]) throws -> String {
        let outputURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-fake-k8s-stdout-\(UUID().uuidString).txt")
        let errorURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-fake-k8s-stderr-\(UUID().uuidString).txt")
        FileManager.default.createFile(atPath: outputURL.path, contents: nil)
        FileManager.default.createFile(atPath: errorURL.path, contents: nil)
        defer {
            try? FileManager.default.removeItem(at: outputURL)
            try? FileManager.default.removeItem(at: errorURL)
        }

        let output = try FileHandle(forWritingTo: outputURL)
        let error = try FileHandle(forWritingTo: errorURL)
        defer {
            try? output.close()
            try? error.close()
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: binary)
        process.arguments = arguments
        process.standardOutput = output
        process.standardError = error
        try process.run()
        process.waitUntilExit()

        let stdout = try String(contentsOf: outputURL, encoding: .utf8)
        let stderr = try String(contentsOf: errorURL, encoding: .utf8)
        guard process.terminationStatus == 0 else {
            throw RuneError.commandFailed(command: "\(binary) \(arguments.joined(separator: " "))", message: stderr)
        }
        return stdout
    }

    private func jsonItems(from json: String) throws -> [[String: Any]] {
        let data = Data(json.utf8)
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let items = object["items"] as? [[String: Any]] else {
            throw RuneError.parseError(message: "Expected Kubernetes list JSON with items.")
        }
        return items
    }

    private func writeExecCredentialPlugin(token: String, requireExecInfo: Bool = false) throws -> URL {
        let json = #"{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"token":"\#(token)"}}"#
        return try writeExecCredentialPlugin(jsonPayload: json, requireExecInfo: requireExecInfo)
    }

    private func writeExecCredentialPlugin(jsonPayload: String, requireExecInfo: Bool = false) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-exec-credential-\(UUID().uuidString).sh")
        let execInfoCheck = requireExecInfo ? """
        if [ -z "${KUBERNETES_EXEC_INFO:-}" ]; then
          echo "missing KUBERNETES_EXEC_INFO" >&2
          exit 7
        fi
        """ : ""
        let body = """
        #!/bin/sh
        \(execInfoCheck)
        cat <<'JSON'
        \(jsonPayload)
        JSON
        """
        try body.write(to: url, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: url.path)
        return url
    }

    private func runOpenSSL(_ arguments: [String]) -> Int32 {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["openssl"] + arguments
        process.standardOutput = Pipe()
        process.standardError = Pipe()
        do {
            try process.run()
            process.waitUntilExit()
            return process.terminationStatus
        } catch {
            return -1
        }
    }

    private struct LocalServerTLSMaterial {
        let caCertificateData: Data
        let serverIdentity: SecIdentity
    }

    private func makeLocalCA(in directory: URL, prefix: String) throws -> Data {
        let caCertificate = directory.appendingPathComponent("\(prefix)-ca.crt")
        let caKey = directory.appendingPathComponent("\(prefix)-ca.key")
        let status = runOpenSSL([
            "req", "-x509",
            "-newkey", "rsa:2048",
            "-nodes",
            "-keyout", caKey.path,
            "-out", caCertificate.path,
            "-subj", "/CN=Rune Local Kubernetes Test \(prefix) CA",
            "-days", "1"
        ])
        guard status == 0 else {
            throw XCTSkip("openssl is not available for local HTTPS certificate generation")
        }
        return try Data(contentsOf: caCertificate)
    }

    private func makeLocalServerTLSMaterial(in directory: URL, prefix: String) throws -> LocalServerTLSMaterial {
        let caCertificate = directory.appendingPathComponent("\(prefix)-ca.crt")
        let caKey = directory.appendingPathComponent("\(prefix)-ca.key")
        let serverKey = directory.appendingPathComponent("\(prefix)-server.key")
        let serverCSR = directory.appendingPathComponent("\(prefix)-server.csr")
        let serverCertificate = directory.appendingPathComponent("\(prefix)-server.crt")
        let serverExt = directory.appendingPathComponent("\(prefix)-server.ext")
        let serverP12 = directory.appendingPathComponent("\(prefix)-server.p12")

        let caStatus = runOpenSSL([
            "req", "-x509",
            "-newkey", "rsa:2048",
            "-nodes",
            "-keyout", caKey.path,
            "-out", caCertificate.path,
            "-subj", "/CN=Rune Local Kubernetes Test \(prefix) CA",
            "-days", "1"
        ])
        guard caStatus == 0 else {
            throw XCTSkip("openssl is not available for local HTTPS certificate generation")
        }

        let csrStatus = runOpenSSL([
            "req", "-newkey", "rsa:2048",
            "-nodes",
            "-keyout", serverKey.path,
            "-out", serverCSR.path,
            "-subj", "/CN=localhost"
        ])
        guard csrStatus == 0 else {
            throw XCTSkip("openssl could not create a local HTTPS server CSR")
        }

        try """
        subjectAltName=DNS:localhost,IP:127.0.0.1
        extendedKeyUsage=serverAuth
        keyUsage=digitalSignature,keyEncipherment
        """.write(to: serverExt, atomically: true, encoding: .utf8)

        let signStatus = runOpenSSL([
            "x509", "-req",
            "-in", serverCSR.path,
            "-CA", caCertificate.path,
            "-CAkey", caKey.path,
            "-CAcreateserial",
            "-out", serverCertificate.path,
            "-days", "1",
            "-sha256",
            "-extfile", serverExt.path
        ])
        guard signStatus == 0 else {
            throw XCTSkip("openssl could not sign the local HTTPS server certificate")
        }

        let p12Status = runOpenSSL([
            "pkcs12", "-export",
            "-out", serverP12.path,
            "-inkey", serverKey.path,
            "-in", serverCertificate.path,
            "-passout", "pass:rune-test"
        ])
        guard p12Status == 0 else {
            throw XCTSkip("openssl could not export the local HTTPS server identity")
        }

        let p12Data = try Data(contentsOf: serverP12)
        var imported: CFArray?
        let options = [kSecImportExportPassphrase as String: "rune-test"] as CFDictionary
        let importStatus = SecPKCS12Import(p12Data as CFData, options, &imported)
        guard importStatus == errSecSuccess,
              let items = imported as? [[String: Any]],
              let rawIdentity = items.first?[kSecImportItemIdentity as String] else {
            throw XCTSkip("Security framework could not import local HTTPS server identity: OSStatus \(importStatus)")
        }
        let identity = rawIdentity as! SecIdentity

        return LocalServerTLSMaterial(
            caCertificateData: try Data(contentsOf: caCertificate),
            serverIdentity: identity
        )
    }

}

private final class PortForwardSessionRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var recordedSessions: [PortForwardSession] = []

    func append(_ session: PortForwardSession) {
        lock.lock()
        recordedSessions.append(session)
        lock.unlock()
    }

    func sessions() -> [PortForwardSession] {
        lock.lock()
        let copy = recordedSessions
        lock.unlock()
        return copy
    }

    func waitForStatus(_ status: PortForwardStatus, timeout: TimeInterval) async throws -> PortForwardSession {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if let session = sessions().last(where: { $0.status == status }) {
                return session
            }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        throw RuneError.commandFailed(
            command: "wait for port-forward",
            message: "Timed out waiting for status \(status.rawValue). Seen: \(sessions().map(\.status.rawValue).joined(separator: ", "))"
        )
    }
}


private final class LocalKubernetesAPIServer: @unchecked Sendable {
    let port: UInt16
    private let listener: NWListener
    private let queue = DispatchQueue(label: "rune.local-k8s-fixture")

    private init(listener: NWListener, port: UInt16) {
        self.listener = listener
        self.port = port
    }

    static func start(
        tlsIdentity: SecIdentity? = nil,
        requireClientCertificate: Bool = false
    ) async throws -> LocalKubernetesAPIServer {
        let listener: NWListener
        if let tlsIdentity {
            let tlsOptions = NWProtocolTLS.Options()
            guard let protocolIdentity = sec_identity_create(tlsIdentity) else {
                throw URLError(.clientCertificateRejected)
            }
            sec_protocol_options_set_local_identity(tlsOptions.securityProtocolOptions, protocolIdentity)
            if requireClientCertificate {
                sec_protocol_options_set_peer_authentication_required(tlsOptions.securityProtocolOptions, true)
                sec_protocol_options_set_verify_block(
                    tlsOptions.securityProtocolOptions,
                    { _, _, complete in complete(true) },
                    DispatchQueue(label: "rune.local-k8s-fixture.client-cert-verify")
                )
            }
            let parameters = NWParameters(tls: tlsOptions)
            parameters.defaultProtocolStack.transportProtocol = NWProtocolTCP.Options()
            listener = try NWListener(using: parameters, on: 0)
        } else {
            listener = try NWListener(using: .tcp, on: 0)
        }
        listener.newConnectionHandler = { connection in
            connection.start(queue: DispatchQueue(label: "rune.local-k8s-fixture.connection"))
            connection.receive(minimumIncompleteLength: 1, maximumLength: 65_536) { data, _, _, _ in
                guard let data,
                      let request = String(data: data, encoding: .utf8),
                      let requestLine = request.split(separator: "\r\n").first else {
                    connection.cancel()
                    return
                }
                let parts = requestLine.split(separator: " ")
                let target = parts.count > 1 ? String(parts[1]) : "/"
                let (status, contentType, body) = response(for: target)
                let reason = status == 200 ? "OK" : "Not Found"
                let header = [
                    "HTTP/1.1 \(status) \(reason)",
                    "Content-Type: \(contentType)",
                    "Content-Length: \(body.utf8.count)",
                    "Connection: close",
                    "",
                    ""
                ].joined(separator: "\r\n")
                connection.send(content: Data((header + body).utf8), completion: .contentProcessed { _ in
                    connection.cancel()
                })
            }
        }

        return try await withCheckedThrowingContinuation { continuation in
            listener.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    guard let port = listener.port?.rawValue else {
                        continuation.resume(throwing: URLError(.cannotConnectToHost))
                        return
                    }
                    continuation.resume(returning: LocalKubernetesAPIServer(listener: listener, port: port))
                case let .failed(error):
                    continuation.resume(throwing: error)
                default:
                    break
                }
            }
            listener.start(queue: DispatchQueue(label: "rune.local-k8s-fixture.listener"))
        }
    }

    func stop() {
        listener.cancel()
    }

    private static func response(for target: String) -> (Int, String, String) {
        let path = target.split(separator: "?", maxSplits: 1).first.map(String.init) ?? target
        switch path {
        case "/api/v1/namespaces":
            return (200, "application/json", #"{"items":[{"metadata":{"name":"default"}}]}"#)
        case "/api/v1/namespaces/default/pods":
            return (200, "application/json", podListJSON)
        case "/apis/metrics.k8s.io/v1beta1/namespaces/default/pods":
            return (200, "application/json", podMetricsJSON)
        case "/api/v1/namespaces/default/pods/api-0/log":
            return (200, "text/plain", "line one\nline two\n")
        case "/api/v1/namespaces/default/pods/multi-0/log":
            if target.contains("allContainers=true") {
                return (200, "text/plain", "main line\nsidecar line\n")
            }
            return (400, "application/json", #"{"kind":"Status","status":"Failure","message":"a container name must be specified for pod multi-0, choose one of: [main sidecar]"}"#)
        case "/api/v1/namespaces/default/pods/api-0":
            return (200, "application/json", podJSON)
        case "/api/v1/namespaces/default/secrets", "/api/v1/secrets":
            return (200, "application/json", helmSecretListJSON)
        default:
            return (404, "application/json", #"{"kind":"Status","status":"Failure","message":"not found"}"#)
        }
    }

    private static let podJSON = """
    {"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2026-04-26T10:00:00Z"},"status":{"phase":"Running","podIP":"10.42.0.10","containerStatuses":[{"ready":true,"restartCount":1}]}}
    """

    private static let podListJSON = """
    {"items":[\(podJSON)]}
    """

    private static let podMetricsJSON = """
    {"items":[{"metadata":{"name":"api-0","namespace":"default"},"containers":[{"usage":{"cpu":"42m","memory":"64Mi"}}]}]}
    """

    private static let helmReleasePayload = "SDRzSUFBQUFBQUFDLzFXUHNXN0RJQkNHWHdYZGJGdUdwQjVZTTNkck8xU1dvcXM1dTZqMmdRQkhpaUsvZThGTnEzUTdmbjYrKzdnQjQwS2d3WVVQbTZEYWo5SGpVREpESTY1elNTOFVvblVNK2xDQjVkR0J2c0dNTVowTitkbGR5ZVMyYWxWWHQ4ZGFkUyt5MVZKcXFkN3owNWd3clhHbjNhdFZIdU1RckU4N0VsNzlGTkNRR056aVowb0VXd1hESjRaVXRpeVUwR0RDTWorcTFqK05CeldRaldvT09VSHYzLzdDWS9QVWRMQVZwT1BSVG9VVHNva2Q4T1JXemp0VS90S0NFNVdiaE5PL053dXlIU25tVnFiYU8xV0xpK3o1eTdMUjRyUkRuOUgzL0d1cWV4YWlxR3F4bS9ZTTJ6ZG1tYy95WndFQUFBPT0="

    private static var helmSecretListJSON: String {
        """
        {"items":[{"metadata":{"name":"sh.helm.release.v1.orbit.v3","namespace":"default","labels":{"owner":"helm","name":"orbit","status":"deployed","version":"3"}},"data":{"release":"\(helmReleasePayload)"}}]}
        """
    }
}
