import Foundation
import Network
import RuneCore

/// Where a port-forward listens on this Mac, read from the address the user
/// gave the way `kubectl port-forward --address` reads it: IP addresses or
/// `localhost`, separated by commas.
enum PortForwardBindAddress: Equatable, Sendable {
    /// One address. `isRequired` is false for the IPv6 half of `localhost`,
    /// which a Mac without IPv6 loopback does not have.
    case host(String, isRequired: Bool)
    /// Every interface, only for an explicit `*`.
    case anyInterface

    static let loopbackIPv4 = "127.0.0.1"
    static let loopbackIPv6 = "::1"

    static func parse(_ rawAddress: String) throws -> [PortForwardBindAddress] {
        var tokens = rawAddress
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        if tokens.isEmpty { tokens = [loopbackIPv4] }

        var hosts: [(host: String, isRequired: Bool)] = []
        var listensOnEveryInterface = false
        func add(_ host: String, isRequired: Bool) {
            if let index = hosts.firstIndex(where: { $0.host == host }) {
                hosts[index].isRequired = hosts[index].isRequired || isRequired
            } else {
                hosts.append((host, isRequired))
            }
        }

        for token in tokens {
            if token == "*" {
                listensOnEveryInterface = true
            } else if token.caseInsensitiveCompare("localhost") == .orderedSame {
                add(loopbackIPv4, isRequired: true)
                add(loopbackIPv6, isRequired: false)
            } else {
                var literal = token
                if literal.hasPrefix("["), literal.hasSuffix("]") {
                    literal = String(literal.dropFirst().dropLast())
                }
                guard IPv4Address(literal) != nil || IPv6Address(literal) != nil else {
                    throw RuneError.invalidInput(
                        message: "Port-forward address \(token) is not an IP address or localhost."
                    )
                }
                add(literal, isRequired: true)
            }
        }
        // Every interface already covers each single address.
        if listensOnEveryInterface { return [.anyInterface] }
        return hosts.map { .host($0.host, isRequired: $0.isRequired) }
    }
}

/// A local listener of a port-forward and whether the forward needs it.
struct PortForwardLocalListener {
    let listener: NWListener
    let isRequired: Bool

    /// One listener per address, each bound to that address only. A listener
    /// made from the port alone would accept connections on every interface.
    static func make(address: String, port: Int) throws -> [PortForwardLocalListener] {
        guard (1...65535).contains(port), let endpointPort = NWEndpoint.Port(rawValue: UInt16(port)) else {
            throw RuneError.invalidInput(message: "Port-forward ports must be between 1 and 65535.")
        }
        return try PortForwardBindAddress.parse(address).map { bindAddress in
            let parameters = NWParameters.tcp
            parameters.allowLocalEndpointReuse = true
            switch bindAddress {
            case .anyInterface:
                return PortForwardLocalListener(
                    listener: try NWListener(using: parameters, on: endpointPort),
                    isRequired: true
                )
            case let .host(host, isRequired):
                parameters.requiredLocalEndpoint = .hostPort(host: NWEndpoint.Host(host), port: endpointPort)
                return PortForwardLocalListener(
                    listener: try NWListener(using: parameters),
                    isRequired: isRequired
                )
            }
        }
    }
}
