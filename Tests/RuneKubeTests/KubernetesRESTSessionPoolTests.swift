import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube

final class KubernetesRESTSessionPoolTests: XCTestCase {
    private final class Clock: @unchecked Sendable {
        private let lock = NSLock()
        private var value = Date(timeIntervalSince1970: 1_800_000_000)
        func now() -> Date { lock.lock(); defer { lock.unlock() }; return value }
        func advance(_ seconds: TimeInterval) { lock.lock(); value += seconds; lock.unlock() }
    }

    private func key(
        _ server: String = "https://api.synthetic.test:6443",
        insecure: Bool = false,
        ca: String? = "synthetic-ca",
        tlsServerName: String? = nil,
        clientCertificate: String? = nil
    ) -> KubernetesRESTSessionKey {
        KubernetesRESTSessionKey(
            serverURL: URL(string: server)!,
            insecureSkipTLSVerify: insecure,
            certificateAuthorityData: ca.map { Data($0.utf8) },
            tlsServerName: tlsServerName,
            clientCertificateData: clientCertificate.map { Data($0.utf8) }
        )
    }

    private func makePool(maxEntries: Int = 12, idleTimeout: TimeInterval = 300, clock: Clock = Clock()) -> KubernetesRESTSessionPool<URLSession> {
        KubernetesRESTSessionPool<URLSession>(
            maxEntries: maxEntries,
            idleTimeout: idleTimeout,
            now: { clock.now() },
            observeConnectionResets: false,
            urlSession: { $0 }
        )
    }

    private func newSession() -> URLSession {
        URLSession(configuration: .ephemeral)
    }

    func testSameServerAndTLSSetupReusesOneSession() {
        let pool = makePool()
        var created = 0
        let first = pool.session(for: key()) { created += 1; return newSession() }
        let second = pool.session(for: key("https://API.synthetic.test:6443/apis/extra")) { created += 1; return newSession() }
        XCTAssertTrue(first === second, "Path and host case don't change the connection target.")
        XCTAssertEqual(created, 1)
        XCTAssertEqual(pool.cachedSessionCount, 1)
    }

    func testTLSSettingsAndServerSeparateSessions() {
        let pool = makePool()
        let variants = [
            key(),
            key("https://api.synthetic.test:7443"),
            key("https://other.synthetic.test:6443"),
            key(insecure: true),
            key(ca: "another-ca"),
            key(ca: nil),
            key(tlsServerName: "kubernetes.default"),
            key(clientCertificate: "client-a"),
            key(clientCertificate: "client-b")
        ]
        let sessions = variants.map { variant in pool.session(for: variant) { newSession() } }
        XCTAssertEqual(Set(sessions.map(ObjectIdentifier.init)).count, variants.count)
        XCTAssertEqual(Set(variants).count, variants.count)
    }

    func testLeastRecentlyUsedSessionIsRetiredAtCapacity() {
        let clock = Clock()
        let pool = makePool(maxEntries: 2, clock: clock)
        let a = pool.session(for: key("https://a.synthetic.test")) { newSession() }
        clock.advance(1)
        _ = pool.session(for: key("https://b.synthetic.test")) { newSession() }
        clock.advance(1)
        _ = pool.session(for: key("https://a.synthetic.test")) { newSession() }
        clock.advance(1)
        _ = pool.session(for: key("https://c.synthetic.test")) { newSession() }

        XCTAssertEqual(pool.cachedSessionCount, 2)
        let aAgain = pool.session(for: key("https://a.synthetic.test")) { newSession() }
        XCTAssertTrue(a === aAgain, "The recently used session survives; b was the oldest.")
        var recreatedB = false
        _ = pool.session(for: key("https://b.synthetic.test")) { recreatedB = true; return newSession() }
        XCTAssertTrue(recreatedB)
    }

    func testIdleSessionsAreRetired() {
        let clock = Clock()
        let pool = makePool(idleTimeout: 60, clock: clock)
        let first = pool.session(for: key()) { newSession() }
        clock.advance(61)
        let second = pool.session(for: key()) { newSession() }
        XCTAssertFalse(first === second)
        XCTAssertEqual(pool.cachedSessionCount, 1)
    }

    func testEvictOnlyRemovesTheFailedSession() {
        let pool = makePool()
        let stale = newSession()
        let current = pool.session(for: key()) { newSession() }
        pool.evict(key(), ifCurrent: stale)
        XCTAssertTrue(pool.session(for: key()) { newSession() } === current, "A late failure from an older session keeps the newer one.")
        pool.evict(key(), ifCurrent: current)
        XCTAssertFalse(pool.session(for: key()) { newSession() } === current)
    }

    func testResetRetiresSharedSessionsAndCancelsStreams() async throws {
        let pool = makePool()
        let shared = pool.session(for: key()) { newSession() }
        let stream = newSession()
        let token = pool.registerStream(stream)
        XCTAssertEqual(pool.registeredStreamCount, 1)

        pool.resetAll()

        XCTAssertEqual(pool.cachedSessionCount, 0)
        XCTAssertEqual(pool.registeredStreamCount, 0)
        XCTAssertFalse(pool.session(for: key()) { newSession() } === shared)
        pool.unregisterStream(token)
    }

    func testConnectionResetNotificationClearsObservingPools() {
        let pool = KubernetesRESTSessionPool<URLSession>(observeConnectionResets: true, urlSession: { $0 })
        _ = pool.session(for: key()) { newSession() }
        _ = pool.registerStream(newSession())

        NotificationCenter.default.post(name: KubernetesConnectionLifecycle.resetNotification, object: nil)

        XCTAssertEqual(pool.cachedSessionCount, 0)
        XCTAssertEqual(pool.registeredStreamCount, 0)
    }

    func testConnectionLevelFailuresAreRecognized() {
        XCTAssertTrue(KubernetesRESTClient.isConnectionLevelFailure(URLError(.networkConnectionLost)))
        XCTAssertTrue(KubernetesRESTClient.isConnectionLevelFailure(URLError(.timedOut)))
        XCTAssertTrue(KubernetesRESTClient.isConnectionLevelFailure(URLError(.secureConnectionFailed)))
        XCTAssertFalse(KubernetesRESTClient.isConnectionLevelFailure(URLError(.cancelled)))
        XCTAssertFalse(KubernetesRESTClient.isConnectionLevelFailure(URLError(.badServerResponse)))
        XCTAssertFalse(KubernetesRESTClient.isConnectionLevelFailure(CancellationError()))
    }

    private func startKeepAliveServer(keepAlive: Bool) async throws -> (RuneFakeK8sRESTServer, URL) {
        var fixture = RuneFakeK8sFixture(contexts: RuneFakeK8sFixture.defaultContexts)
        fixture.keepAliveConnections = keepAlive
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-session-pool-kubeconfig-\(UUID().uuidString).yaml")
        try server.kubeconfigYAML().write(to: url, atomically: true, encoding: .utf8)
        return (server, url)
    }

    private func listPodsRepeatedly(_ client: KubernetesClient, kubeconfig: URL, times: Int) async throws {
        let base = RuneFakeK8sFixture.defaultContexts[0]
        for _ in 0..<times {
            _ = try await client.listPods(
                from: [KubeConfigSource(url: kubeconfig)],
                context: KubeContext(name: base.contextName),
                namespace: base.defaultNamespace
            )
        }
    }

    func testRepeatedListsReuseTheConnectionWhenTheServerKeepsItOpen() async throws {
        let (server, kubeconfig) = try await startKeepAliveServer(keepAlive: true)
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeconfig)
        }
        let restClient = KubernetesRESTClient()
        let client = KubernetesClient(
            commandTimeout: 5,
            restClient: restClient,
            requestMetricsRecorder: KubernetesRESTRequestMetricsRecorder()
        )

        try await listPodsRepeatedly(client, kubeconfig: kubeconfig, times: 10)

        XCTAssertGreaterThanOrEqual(server.requestLines().count, 10)
        XCTAssertLessThanOrEqual(server.acceptedConnectionCount(), 2, "Ten sequential lists should share a connection.")
        XCTAssertEqual(restClient._testCachedSessionCount, 1)
    }

    func testConnectionResetOpensAFreshConnection() async throws {
        let (server, kubeconfig) = try await startKeepAliveServer(keepAlive: true)
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeconfig)
        }
        let restClient = KubernetesRESTClient()
        let client = KubernetesClient(
            commandTimeout: 5,
            restClient: restClient,
            requestMetricsRecorder: KubernetesRESTRequestMetricsRecorder()
        )

        try await listPodsRepeatedly(client, kubeconfig: kubeconfig, times: 2)
        let before = server.acceptedConnectionCount()
        restClient._testResetConnections()
        XCTAssertEqual(restClient._testCachedSessionCount, 0)

        try await listPodsRepeatedly(client, kubeconfig: kubeconfig, times: 2)
        XCTAssertGreaterThan(server.acceptedConnectionCount(), before)
        XCTAssertEqual(restClient._testCachedSessionCount, 1)
    }

    func testServerThatClosesConnectionsStillWorks() async throws {
        let (server, kubeconfig) = try await startKeepAliveServer(keepAlive: false)
        defer {
            server.stop()
            try? FileManager.default.removeItem(at: kubeconfig)
        }
        let client = KubernetesClient(commandTimeout: 5)

        try await listPodsRepeatedly(client, kubeconfig: kubeconfig, times: 5)

        XCTAssertGreaterThanOrEqual(server.acceptedConnectionCount(), 5)
    }
}
