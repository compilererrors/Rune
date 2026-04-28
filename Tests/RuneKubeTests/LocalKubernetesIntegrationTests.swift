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
