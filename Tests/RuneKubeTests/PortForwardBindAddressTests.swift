import Darwin
import Network
import XCTest
@testable import RuneCore
@testable import RuneKube

final class PortForwardBindAddressTests: XCTestCase {
    func testAnAddressBindsOnlyThatAddress() throws {
        XCTAssertEqual(try PortForwardBindAddress.parse("127.0.0.1"), [.host("127.0.0.1", isRequired: true)])
        XCTAssertEqual(try PortForwardBindAddress.parse(" ::1 "), [.host("::1", isRequired: true)])
        XCTAssertEqual(try PortForwardBindAddress.parse("[::1]"), [.host("::1", isRequired: true)])
        XCTAssertEqual(try PortForwardBindAddress.parse("0.0.0.0"), [.host("0.0.0.0", isRequired: true)])
    }

    func testNoAddressMeansLoopback() throws {
        XCTAssertEqual(try PortForwardBindAddress.parse(""), [.host("127.0.0.1", isRequired: true)])
        XCTAssertEqual(try PortForwardBindAddress.parse(" , "), [.host("127.0.0.1", isRequired: true)])
    }

    func testLocalhostIsBothLoopbackAddressesAndIPv6IsOptional() throws {
        XCTAssertEqual(
            try PortForwardBindAddress.parse("LocalHost"),
            [.host("127.0.0.1", isRequired: true), .host("::1", isRequired: false)]
        )
        // Asked for by address as well, IPv6 loopback is needed.
        XCTAssertEqual(
            try PortForwardBindAddress.parse("localhost,::1,127.0.0.1"),
            [.host("127.0.0.1", isRequired: true), .host("::1", isRequired: true)]
        )
    }

    func testEveryInterfaceTakesAnExplicitWildcard() throws {
        XCTAssertEqual(try PortForwardBindAddress.parse("*"), [.anyInterface])
        XCTAssertEqual(try PortForwardBindAddress.parse("127.0.0.1,*"), [.anyInterface])
    }

    func testAHostNameIsRefused() {
        for address in ["example.test", "127.0.0.1,db.internal", "not an address"] {
            XCTAssertThrowsError(try PortForwardBindAddress.parse(address), address) { error in
                XCTAssertTrue(error.localizedDescription.contains("is not an IP address or localhost"))
            }
        }
    }

    func testListenersAreBoundToTheAddressNotJustThePort() throws {
        let port = try Self.freeLoopbackPort()
        let listeners = try PortForwardLocalListener.make(address: "127.0.0.1", port: port)

        XCTAssertEqual(listeners.count, 1)
        XCTAssertEqual(
            listeners.first?.listener.parameters.requiredLocalEndpoint,
            .hostPort(host: "127.0.0.1", port: NWEndpoint.Port(rawValue: UInt16(port))!),
            "a listener without a required local endpoint accepts connections on every interface"
        )
    }

    func testLocalhostListensOnBothLoopbackAddressesAtOnce() async throws {
        let port = try Self.freeLoopbackPort()
        let listeners = try PortForwardLocalListener.make(address: "localhost", port: port)
        defer { listeners.forEach { $0.listener.cancel() } }
        XCTAssertEqual(listeners.map(\.isRequired), [true, false])

        for member in listeners {
            let ready = expectation(description: "listener ready")
            ready.assertForOverFulfill = false
            let isRequired = member.isRequired
            member.listener.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    ready.fulfill()
                case .failed, .waiting:
                    // IPv6 loopback may be unavailable; only IPv4 must listen.
                    if !isRequired { ready.fulfill() }
                default:
                    break
                }
            }
            member.listener.newConnectionHandler = { $0.cancel() }
            member.listener.start(queue: .global())
            await fulfillment(of: [ready], timeout: 5)
        }

        XCTAssertTrue(Self.canConnect(to: "127.0.0.1", port: port))
    }

    func testAForwardOnAnIPv6AddressHasABrowserURL() {
        func url(_ address: String) -> String? {
            PortForwardSession(
                id: "pf", contextName: "sample-cluster", namespace: "team-alpha", targetKind: .pod,
                targetName: "api-0", localPort: 8080, remotePort: 80, address: address, status: .active
            ).browserURL?.absoluteString
        }
        XCTAssertEqual(url("::1"), "http://[::1]:8080/")
        XCTAssertEqual(url("[::1]"), "http://[::1]:8080/")
        XCTAssertEqual(url("127.0.0.1"), "http://127.0.0.1:8080/")
        XCTAssertEqual(url("localhost"), "http://localhost:8080/")
        XCTAssertEqual(url("::"), "http://127.0.0.1:8080/")
        XCTAssertEqual(url("localhost,::1"), "http://localhost:8080/")
    }

    func testInvalidPortIsRefused() {
        XCTAssertThrowsError(try PortForwardLocalListener.make(address: "127.0.0.1", port: 0))
        XCTAssertThrowsError(try PortForwardLocalListener.make(address: "127.0.0.1", port: 70_000))
    }

    private static func freeLoopbackPort() throws -> Int {
        let descriptor = socket(AF_INET, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw POSIXError(.EMFILE) }
        defer { close(descriptor) }
        var address = sockaddr_in()
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET)
        address.sin_addr.s_addr = inet_addr("127.0.0.1")
        address.sin_port = 0
        let bound = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bound == 0 else { throw POSIXError(.EADDRINUSE) }
        var length = socklen_t(MemoryLayout<sockaddr_in>.size)
        let named = withUnsafeMutablePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                getsockname(descriptor, $0, &length)
            }
        }
        guard named == 0 else { throw POSIXError(.EINVAL) }
        return Int(UInt16(bigEndian: address.sin_port))
    }

    private static func canConnect(to host: String, port: Int) -> Bool {
        let descriptor = socket(AF_INET, SOCK_STREAM, 0)
        guard descriptor >= 0 else { return false }
        defer { close(descriptor) }
        var address = sockaddr_in()
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET)
        address.sin_addr.s_addr = inet_addr(host)
        address.sin_port = UInt16(port).bigEndian
        return withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                connect(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in>.size)) == 0
            }
        }
    }
}
