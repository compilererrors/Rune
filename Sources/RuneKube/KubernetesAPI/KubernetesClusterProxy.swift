import Foundation
import Network
import RuneCore

/// A kubeconfig cluster's `proxy-url`, read as client-go reads it: an `http`,
/// `https` or `socks5` proxy, with an optional `user:password` in the URL.
/// It replaces the system proxy settings for that cluster, as in kubectl.
struct KubernetesClusterProxy: Hashable, Sendable {
    enum Scheme: String, Hashable, Sendable {
        case http
        case https
        case socks5
    }

    let scheme: Scheme
    let host: String
    let port: UInt16
    let username: String?
    let password: String?

    /// nil for a missing or empty value; throws for one Rune cannot use.
    static func parse(_ raw: String?) throws -> KubernetesClusterProxy? {
        guard let raw = raw?.trimmingCharacters(in: .whitespacesAndNewlines), !raw.isEmpty else {
            return nil
        }
        guard let components = URLComponents(string: raw),
              let rawScheme = components.scheme?.lowercased() else {
            throw invalid("it is not a URL")
        }
        let scheme: Scheme
        switch rawScheme {
        case "http": scheme = .http
        case "https": scheme = .https
        // socks5h resolves names at the proxy; Network's SOCKS proxy does too.
        case "socks5", "socks5h": scheme = .socks5
        default: throw invalid("the scheme must be http, https or socks5")
        }
        guard let host = components.host, !host.isEmpty else {
            throw invalid("it has no host")
        }
        let defaultPort: Int
        switch scheme {
        case .http: defaultPort = 80
        case .https: defaultPort = 443
        case .socks5: defaultPort = 1080
        }
        let rawPort = components.port ?? defaultPort
        guard let port = UInt16(exactly: rawPort), port > 0 else {
            throw invalid("the port is out of range")
        }
        let username = components.user.flatMap { $0.isEmpty ? nil : $0 }
        return KubernetesClusterProxy(
            scheme: scheme,
            host: host.hasPrefix("[") && host.hasSuffix("]") ? String(host.dropFirst().dropLast()) : host,
            port: port,
            username: username,
            password: username == nil ? nil : (components.password ?? "")
        )
    }

    var proxyConfiguration: ProxyConfiguration {
        let endpoint = NWEndpoint.hostPort(
            host: NWEndpoint.Host(host),
            port: NWEndpoint.Port(rawValue: port) ?? .https
        )
        var configuration: ProxyConfiguration
        switch scheme {
        case .http:
            configuration = ProxyConfiguration(httpCONNECTProxy: endpoint)
        case .https:
            configuration = ProxyConfiguration(httpCONNECTProxy: endpoint, tlsOptions: NWProtocolTLS.Options())
        case .socks5:
            configuration = ProxyConfiguration(socksv5Proxy: endpoint)
        }
        // kubectl does not go around a proxy that fails.
        configuration.allowFailover = false
        if let username {
            configuration.applyCredential(username: username, password: password ?? "")
        }
        return configuration
    }

    private static func invalid(_ reason: String) -> RuneError {
        RuneError.invalidInput(message: "The kubeconfig proxy-url cannot be used: \(reason).")
    }
}
