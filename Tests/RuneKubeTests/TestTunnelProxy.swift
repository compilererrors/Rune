import Foundation
import Network

/// A loopback HTTP CONNECT or SOCKS5 proxy that records where each tunnel
/// goes and relays the bytes.
final class TestTunnelProxy: @unchecked Sendable {
    enum Kind { case httpConnect, socks5 }

    private let kind: Kind
    private let requiredAuthorization: String?
    private let listener: NWListener
    private let queue = DispatchQueue(label: "rune.test.tunnel-proxy")
    private let lock = NSLock()
    private var targets: [String] = []
    private var authorizations: [String] = []
    private var connections: [NWConnection] = []

    private init(kind: Kind, requiredAuthorization: String?, listener: NWListener) {
        self.kind = kind
        self.requiredAuthorization = requiredAuthorization
        self.listener = listener
    }

    var port: UInt16 { listener.port?.rawValue ?? 0 }

    static func start(kind: Kind, requiredAuthorization: String? = nil) async throws -> TestTunnelProxy {
        let parameters = NWParameters.tcp
        parameters.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
        let proxy = TestTunnelProxy(
            kind: kind,
            requiredAuthorization: requiredAuthorization,
            listener: try NWListener(using: parameters)
        )
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            let once = OnceFlag()
            proxy.listener.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    if once.claim() { continuation.resume() }
                case .failed(let error):
                    if once.claim() { continuation.resume(throwing: error) }
                default:
                    break
                }
            }
            proxy.listener.newConnectionHandler = { [weak proxy] connection in
                proxy?.accept(connection)
            }
            proxy.listener.start(queue: proxy.queue)
        }
        return proxy
    }

    func stop() {
        listener.cancel()
        lock.lock()
        let open = connections
        connections.removeAll()
        lock.unlock()
        open.forEach { $0.cancel() }
    }

    func tunnelTargets() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return targets
    }

    func proxyAuthorizations() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return authorizations
    }

    private func keep(_ connection: NWConnection) {
        lock.lock()
        connections.append(connection)
        lock.unlock()
    }

    private func accept(_ client: NWConnection) {
        keep(client)
        client.start(queue: queue)
        switch kind {
        case .httpConnect: readConnectRequest(client, buffer: Data())
        case .socks5: readSOCKSGreeting(client)
        }
    }

    private func readConnectRequest(_ client: NWConnection, buffer: Data) {
        client.receive(minimumIncompleteLength: 1, maximumLength: 16_384) { [weak self] data, _, isComplete, error in
            guard let self, error == nil else { client.cancel(); return }
            var buffer = buffer
            if let data { buffer.append(data) }
            guard let end = buffer.range(of: Data("\r\n\r\n".utf8)) else {
                if isComplete { client.cancel() } else { self.readConnectRequest(client, buffer: buffer) }
                return
            }
            let head = String(decoding: buffer[..<end.lowerBound], as: UTF8.self)
            let lines = head.components(separatedBy: "\r\n")
            let parts = lines.first?.split(separator: " ") ?? []
            guard parts.count >= 2, parts[0] == "CONNECT" else { client.cancel(); return }
            let target = String(parts[1])
            var authorization: String?
            for line in lines.dropFirst() {
                let pair = line.split(separator: ":", maxSplits: 1)
                if pair.count == 2, pair[0].lowercased() == "proxy-authorization" {
                    authorization = pair[1].trimmingCharacters(in: .whitespaces)
                    self.lock.lock()
                    self.authorizations.append(authorization ?? "")
                    self.lock.unlock()
                }
            }
            let leftover = Data(buffer[end.upperBound...])
            if let required = self.requiredAuthorization, authorization != required {
                let challenge = "HTTP/1.1 407 Proxy Authentication Required\r\n"
                    + "Proxy-Authenticate: Basic realm=\"rune-test\"\r\nContent-Length: 0\r\n\r\n"
                client.send(content: Data(challenge.utf8), completion: .contentProcessed { _ in })
                self.readConnectRequest(client, buffer: leftover)
                return
            }
            self.openTunnel(client, to: target, leftover: leftover) {
                client.send(content: Data("HTTP/1.1 200 Connection Established\r\n\r\n".utf8), completion: .contentProcessed { _ in })
            }
        }
    }

    private func readSOCKSGreeting(_ client: NWConnection) {
        client.receive(minimumIncompleteLength: 2, maximumLength: 257) { [weak self] data, _, _, error in
            guard let self, error == nil, let data, data.first == 5 else { client.cancel(); return }
            client.send(content: Data([5, 0]), completion: .contentProcessed { _ in })
            self.readSOCKSRequest(client)
        }
    }

    private func readSOCKSRequest(_ client: NWConnection) {
        client.receive(minimumIncompleteLength: 5, maximumLength: 512) { [weak self] data, _, _, error in
            guard let self, error == nil, let data, data.count >= 5, data[0] == 5, data[1] == 1 else {
                client.cancel(); return
            }
            let bytes = [UInt8](data)
            var host = ""
            var index = 4
            switch bytes[3] {
            case 1 where bytes.count >= 10:
                host = bytes[4..<8].map(String.init).joined(separator: ".")
                index = 8
            case 3:
                let length = Int(bytes[4])
                guard bytes.count >= 5 + length + 2 else { client.cancel(); return }
                host = String(decoding: bytes[5..<(5 + length)], as: UTF8.self)
                index = 5 + length
            default:
                client.cancel(); return
            }
            let port = Int(bytes[index]) << 8 | Int(bytes[index + 1])
            self.openTunnel(client, to: "\(host):\(port)", leftover: Data()) {
                client.send(content: Data([5, 0, 0, 1, 0, 0, 0, 0, 0, 0]), completion: .contentProcessed { _ in })
            }
        }
    }

    private func openTunnel(_ client: NWConnection, to target: String, leftover: Data, onReady: @escaping @Sendable () -> Void) {
        lock.lock()
        targets.append(target)
        lock.unlock()
        guard let colon = target.lastIndex(of: ":"),
              let port = NWEndpoint.Port(String(target[target.index(after: colon)...])) else {
            client.cancel(); return
        }
        // Every tunnel ends at the loopback fake server, whatever name it asked for.
        let upstream = NWConnection(host: "127.0.0.1", port: port, using: .tcp)
        keep(upstream)
        upstream.stateUpdateHandler = { [weak self] state in
            switch state {
            case .ready:
                onReady()
                if !leftover.isEmpty {
                    upstream.send(content: leftover, completion: .contentProcessed { _ in })
                }
                self?.relay(from: client, to: upstream)
                self?.relay(from: upstream, to: client)
            case .failed, .cancelled:
                client.cancel()
            default:
                break
            }
        }
        upstream.start(queue: queue)
    }

    private func relay(from source: NWConnection, to destination: NWConnection) {
        source.receive(minimumIncompleteLength: 1, maximumLength: 65_536) { [weak self] data, _, isComplete, error in
            if let data, !data.isEmpty {
                destination.send(content: data, completion: .contentProcessed { _ in })
            }
            if isComplete || error != nil {
                destination.send(content: nil, isComplete: true, completion: .contentProcessed { _ in })
                return
            }
            self?.relay(from: source, to: destination)
        }
    }
}

final class OnceFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var claimed = false

    func claim() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if claimed { return false }
        claimed = true
        return true
    }
}
