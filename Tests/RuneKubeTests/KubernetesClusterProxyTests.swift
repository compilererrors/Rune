import Foundation
import Network
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube

final class KubernetesClusterProxyTests: XCTestCase {
    func testParsesTheSchemesKubectlAccepts() throws {
        let http = try XCTUnwrap(KubernetesClusterProxy.parse("http://proxy.example.test:3128"))
        XCTAssertEqual(http.scheme, .http)
        XCTAssertEqual(http.host, "proxy.example.test")
        XCTAssertEqual(http.port, 3128)
        XCTAssertNil(http.username)

        XCTAssertEqual(try KubernetesClusterProxy.parse("https://proxy.example.test")?.port, 443)
        XCTAssertEqual(try KubernetesClusterProxy.parse("http://proxy.example.test")?.port, 80)
        XCTAssertEqual(try KubernetesClusterProxy.parse("socks5://127.0.0.1")?.port, 1080)
        XCTAssertEqual(try KubernetesClusterProxy.parse("SOCKS5H://127.0.0.1:9050")?.scheme, .socks5)
        XCTAssertEqual(try KubernetesClusterProxy.parse("http://[::1]:8080")?.host, "::1")
    }

    func testReadsCredentialsFromTheURL() throws {
        let proxy = try XCTUnwrap(KubernetesClusterProxy.parse("http://alice:p%40ss@proxy.example.test:3128"))
        XCTAssertEqual(proxy.username, "alice")
        XCTAssertEqual(proxy.password, "p@ss")
        XCTAssertEqual(try KubernetesClusterProxy.parse("http://alice@proxy.example.test")?.password, "")
    }

    func testAMissingValueMeansNoProxyAndABadOneIsAnError() throws {
        XCTAssertNil(try KubernetesClusterProxy.parse(nil))
        XCTAssertNil(try KubernetesClusterProxy.parse("  "))
        for bad in ["ftp://proxy.example.test", "proxy.example.test:3128", "http://", "http://proxy.example.test:70000"] {
            XCTAssertThrowsError(try KubernetesClusterProxy.parse(bad), bad)
        }
    }

    func testTheProxyIsPartOfTheSessionKey() throws {
        let server = try XCTUnwrap(URL(string: "https://cluster.example.test"))
        func key(_ proxy: String?) throws -> KubernetesRESTSessionKey {
            KubernetesRESTSessionKey(
                serverURL: server,
                insecureSkipTLSVerify: false,
                certificateAuthorityData: nil,
                tlsServerName: nil,
                clientCertificateData: nil,
                proxy: try KubernetesClusterProxy.parse(proxy)
            )
        }
        XCTAssertNotEqual(try key(nil), try key("http://proxy.example.test:3128"))
        XCTAssertNotEqual(try key("http://proxy.example.test:3128"), try key("socks5://proxy.example.test:3128"))
        XCTAssertEqual(try key("http://proxy.example.test:3128"), try key("http://proxy.example.test:3128"))
    }

    func testRequestsGoThroughAnHTTPConnectProxyWithItsCredentials() async throws {
        let expected = "Basic " + Data("alice:secret".utf8).base64EncodedString()
        let proxy = try await TestTunnelProxy.start(kind: .httpConnect, requiredAuthorization: expected)
        defer { proxy.stop() }
        try await assertListsPodsThrough(proxyURL: "http://alice:secret@127.0.0.1:\(proxy.port)", proxy: proxy)
        XCTAssertEqual(proxy.proxyAuthorizations().last, expected)
    }

    func testRequestsGoThroughASOCKS5Proxy() async throws {
        let proxy = try await TestTunnelProxy.start(kind: .socks5)
        defer { proxy.stop() }
        try await assertListsPodsThrough(proxyURL: "socks5://127.0.0.1:\(proxy.port)", proxy: proxy)
    }

    func testAnInvalidProxyURLFailsTheRequestInsteadOfGoingDirect() async throws {
        let server = try await RuneFakeK8sRESTServer.start(fixture: RuneFakeK8sFixture(contexts: RuneFakeK8sFixture.defaultContexts))
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server: server, proxyURL: "ftp://127.0.0.1:1")
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        do {
            try await listPods(kubeconfig: kubeconfig)
            XCTFail("A proxy-url Rune cannot use must not fall back to a direct connection.")
        } catch {
            XCTAssertTrue(String(describing: error).contains("proxy-url"), String(describing: error))
        }
        XCTAssertEqual(server.requestLines().count, 0)
    }

    private func assertListsPodsThrough(proxyURL: String, proxy: TestTunnelProxy) async throws {
        let server = try await RuneFakeK8sRESTServer.start(fixture: RuneFakeK8sFixture(contexts: RuneFakeK8sFixture.defaultContexts))
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server: server, proxyURL: proxyURL)
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        try await listPods(kubeconfig: kubeconfig)

        XCTAssertGreaterThanOrEqual(proxy.tunnelTargets().count, 1, "The request did not reach the proxy.")
        XCTAssertEqual(Set(proxy.tunnelTargets()), ["\(Self.proxyOnlyHost):\(server.port)"])
        XCTAssertGreaterThanOrEqual(server.requestLines().count, 1)
    }

    private func listPods(kubeconfig: URL) async throws {
        let base = RuneFakeK8sFixture.defaultContexts[0]
        let client = KubernetesClient(
            commandTimeout: 5,
            restClient: KubernetesRESTClient(),
            requestMetricsRecorder: KubernetesRESTRequestMetricsRecorder()
        )
        _ = try await client.listPods(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: base.contextName),
            namespace: base.defaultNamespace
        )
    }

    static let proxyOnlyHost = "cluster.rune-proxy.test"

    private func writeKubeconfig(server: RuneFakeK8sRESTServer, proxyURL: String) throws -> URL {
        // A name only the test proxy resolves: a direct connection fails on DNS,
        // and loopback addresses are never sent through a proxy.
        let yaml = server.kubeconfigYAML(serverURL: "http://\(Self.proxyOnlyHost):\(server.port)")
        let serverLine = try XCTUnwrap(
            yaml.components(separatedBy: "\n").first { $0.trimmingCharacters(in: .whitespaces).hasPrefix("server:") }
        )
        let indent = String(serverLine.prefix { $0 == " " })
        let withProxy = yaml.replacingOccurrences(
            of: serverLine,
            with: serverLine + "\n" + indent + "proxy-url: \"\(proxyURL)\""
        )
        XCTAssertTrue(withProxy.contains("proxy-url:"))
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-proxy-kubeconfig-\(UUID().uuidString).yaml")
        try withProxy.write(to: url, atomically: true, encoding: .utf8)
        return url
    }
}
