import CryptoKit
import Foundation
import Network
import RuneDiagnostics
#if canImport(AppKit)
import AppKit
#endif

struct KubernetesRESTSessionKey: Hashable, Sendable {
    let origin: String
    let insecureSkipTLSVerify: Bool
    let certificateAuthorityFingerprint: Data?
    let tlsServerName: String?
    let clientCertificateFingerprint: Data?
    let proxy: KubernetesClusterProxy?

    init(
        serverURL: URL,
        insecureSkipTLSVerify: Bool,
        certificateAuthorityData: Data?,
        tlsServerName: String?,
        clientCertificateData: Data?,
        proxy: KubernetesClusterProxy? = nil
    ) {
        let scheme = serverURL.scheme?.lowercased() ?? "https"
        let host = serverURL.host?.lowercased() ?? ""
        let port = serverURL.port.map { ":\($0)" } ?? ""
        origin = "\(scheme)://\(host)\(port)"
        self.insecureSkipTLSVerify = insecureSkipTLSVerify
        certificateAuthorityFingerprint = certificateAuthorityData.map { Data(SHA256.hash(data: $0)) }
        self.tlsServerName = tlsServerName
        clientCertificateFingerprint = clientCertificateData.map { Data(SHA256.hash(data: $0)) }
        self.proxy = proxy
    }
}

final class KubernetesRESTSessionPool<Session>: @unchecked Sendable {
    private struct Entry {
        let session: Session
        let urlSession: URLSession
        var lastUsed: Date
    }

    private let lock = NSLock()
    private var entries: [KubernetesRESTSessionKey: Entry] = [:]
    private var streams: [UUID: URLSession] = [:]
    private let maxEntries: Int
    private let idleTimeout: TimeInterval
    private let now: @Sendable () -> Date
    private let urlSession: (Session) -> URLSession
    private let retirementGrace: TimeInterval
    private var resetObserver: NSObjectProtocol?

    init(
        maxEntries: Int = 12,
        idleTimeout: TimeInterval = 300,
        retirementGrace: TimeInterval = 30,
        now: @escaping @Sendable () -> Date = Date.init,
        observeConnectionResets: Bool = true,
        urlSession: @escaping (Session) -> URLSession
    ) {
        self.maxEntries = max(1, maxEntries)
        self.idleTimeout = idleTimeout
        self.retirementGrace = retirementGrace
        self.now = now
        self.urlSession = urlSession
        if observeConnectionResets {
            KubernetesConnectionLifecycle.shared.start()
            resetObserver = NotificationCenter.default.addObserver(
                forName: KubernetesConnectionLifecycle.resetNotification,
                object: nil,
                queue: nil
            ) { [weak self] _ in
                self?.resetAll()
            }
        }
    }

    deinit {
        if let resetObserver {
            NotificationCenter.default.removeObserver(resetObserver)
        }
        lock.lock()
        let sessions = entries.values.map(\.urlSession)
        entries.removeAll()
        lock.unlock()
        sessions.forEach { $0.finishTasksAndInvalidate() }
    }

    func session(for key: KubernetesRESTSessionKey, make: () -> Session) -> Session {
        lock.lock()
        let current = now()
        var retired = removeIdleEntries(at: current)
        if var entry = entries[key] {
            entry.lastUsed = current
            entries[key] = entry
            lock.unlock()
            retire(retired)
            return entry.session
        }
        let session = make()
        entries[key] = Entry(session: session, urlSession: urlSession(session), lastUsed: current)
        if entries.count > maxEntries,
           let oldest = entries.min(by: { $0.value.lastUsed < $1.value.lastUsed })?.key,
           oldest != key,
           let removed = entries.removeValue(forKey: oldest) {
            retired.append(removed.urlSession)
        }
        lock.unlock()
        retire(retired)
        return session
    }

    /// Invalidates sessions the pool no longer hands out, later rather than
    /// now. A request that picked a session up a moment ago may not have
    /// created its task yet, and URLSession raises an Objective-C exception
    /// for a task created on an invalidated session, which ends the process.
    /// Requests already running finish either way, so waiting costs nothing
    /// but a little memory.
    private func retire(_ sessions: [URLSession]) {
        guard !sessions.isEmpty else { return }
        guard retirementGrace > 0 else {
            sessions.forEach { $0.finishTasksAndInvalidate() }
            return
        }
        DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + retirementGrace) {
            sessions.forEach { $0.finishTasksAndInvalidate() }
        }
    }

    func evict(_ key: KubernetesRESTSessionKey, ifCurrent session: URLSession) {
        lock.lock()
        guard let entry = entries[key], entry.urlSession === session else {
            lock.unlock()
            return
        }
        entries.removeValue(forKey: key)
        lock.unlock()
        retire([entry.urlSession])
    }

    func registerStream(_ session: URLSession) -> UUID {
        let token = UUID()
        lock.lock()
        streams[token] = session
        lock.unlock()
        return token
    }

    func unregisterStream(_ token: UUID) {
        lock.lock()
        streams.removeValue(forKey: token)
        lock.unlock()
    }

    func resetAll() {
        lock.lock()
        let shared = entries.values.map(\.urlSession)
        let streaming = Array(streams.values)
        entries.removeAll()
        streams.removeAll()
        lock.unlock()
        retire(shared)
        // A stream's owner invalidates its session when the stream ends.
        // Here only its running tasks are cancelled, so a stream that has
        // registered but not started yet is not left with a dead session.
        streaming.forEach { session in
            session.getAllTasks { tasks in tasks.forEach { $0.cancel() } }
        }
    }

    var cachedSessionCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return entries.count
    }

    var registeredStreamCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return streams.count
    }

    private func removeIdleEntries(at current: Date) -> [URLSession] {
        let idleKeys = entries.filter { current.timeIntervalSince($0.value.lastUsed) > idleTimeout }.map(\.key)
        return idleKeys.compactMap { entries.removeValue(forKey: $0)?.urlSession }
    }
}

final class KubernetesConnectionLifecycle: @unchecked Sendable {
    static let shared = KubernetesConnectionLifecycle()
    static let resetNotification = Notification.Name("RuneKubernetesConnectionsShouldReset")

    private let lock = NSLock()
    private var started = false
    private var monitor: NWPathMonitor?
    private var lastPath: PathSignature?
    private var wakeObserver: NSObjectProtocol?

    private struct PathSignature: Equatable {
        let satisfied: Bool
        let interfaces: [String]
    }

    func start() {
        lock.lock()
        guard !started else {
            lock.unlock()
            return
        }
        started = true
        lock.unlock()

        let monitor = NWPathMonitor()
        monitor.pathUpdateHandler = { [weak self] path in
            self?.pathChanged(PathSignature(
                satisfied: path.status == .satisfied,
                interfaces: path.availableInterfaces.map(\.name).sorted()
            ))
        }
        monitor.start(queue: DispatchQueue(label: "rune.kubernetes.connection-lifecycle"))
        #if canImport(AppKit)
        let observer = NSWorkspace.shared.notificationCenter.addObserver(
            forName: NSWorkspace.didWakeNotification,
            object: nil,
            queue: nil
        ) { _ in
            KubernetesConnectionLifecycle.postReset(reason: "wake")
        }
        #endif
        lock.lock()
        self.monitor = monitor
        #if canImport(AppKit)
        wakeObserver = observer
        #endif
        lock.unlock()
    }

    private func pathChanged(_ signature: PathSignature) {
        lock.lock()
        let previous = lastPath
        lastPath = signature
        lock.unlock()
        guard let previous, previous != signature else { return }
        Self.postReset(reason: "network-path")
    }

    static func postReset(reason: String) {
        VerboseKubeTrace.append("k8s.connections", "reset reason=\(reason)")
        NotificationCenter.default.post(name: resetNotification, object: nil)
    }
}
