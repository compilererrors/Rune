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

    init(
        serverURL: URL,
        insecureSkipTLSVerify: Bool,
        certificateAuthorityData: Data?,
        tlsServerName: String?,
        clientCertificateData: Data?
    ) {
        let scheme = serverURL.scheme?.lowercased() ?? "https"
        let host = serverURL.host?.lowercased() ?? ""
        let port = serverURL.port.map { ":\($0)" } ?? ""
        origin = "\(scheme)://\(host)\(port)"
        self.insecureSkipTLSVerify = insecureSkipTLSVerify
        certificateAuthorityFingerprint = certificateAuthorityData.map { Data(SHA256.hash(data: $0)) }
        self.tlsServerName = tlsServerName
        clientCertificateFingerprint = clientCertificateData.map { Data(SHA256.hash(data: $0)) }
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
    private var resetObserver: NSObjectProtocol?

    init(
        maxEntries: Int = 12,
        idleTimeout: TimeInterval = 300,
        now: @escaping @Sendable () -> Date = Date.init,
        observeConnectionResets: Bool = true,
        urlSession: @escaping (Session) -> URLSession
    ) {
        self.maxEntries = max(1, maxEntries)
        self.idleTimeout = idleTimeout
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
            retired.forEach { $0.finishTasksAndInvalidate() }
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
        retired.forEach { $0.finishTasksAndInvalidate() }
        return session
    }

    func evict(_ key: KubernetesRESTSessionKey, ifCurrent session: URLSession) {
        lock.lock()
        guard let entry = entries[key], entry.urlSession === session else {
            lock.unlock()
            return
        }
        entries.removeValue(forKey: key)
        lock.unlock()
        entry.urlSession.finishTasksAndInvalidate()
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
        shared.forEach { $0.finishTasksAndInvalidate() }
        streaming.forEach { $0.invalidateAndCancel() }
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
