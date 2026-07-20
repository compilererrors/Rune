import Foundation
import Network
@preconcurrency import Security
@testable import RuneSecurity
import XCTest

/// Exercises the production URLSession transports without permitting test-only
/// token endpoints in production. A URLProtocol relay forwards the pinned HTTPS
/// request to a synthetic loopback response server.
final class CloudOAuthLoopbackTransportIntegrationTests: XCTestCase {
    override func tearDown() {
        LoopbackOAuthURLProtocol.reset()
        super.tearDown()
    }

    func testAKSClientCredentialsTransportPostsToSyntheticLoopbackResponseServer() async throws {
        let server = try await LoopbackOAuthResponseServer.start(
            body: #"{"access_token":"synthetic-aks-loopback-token","token_type":"Bearer","expires_in":3600}"#
        )
        defer { server.stop() }
        LoopbackOAuthURLProtocol.configure(baseURL: server.baseURL)

        let descriptor = try AKSKubeloginServicePrincipalDescriptor(
            tenantID: "11111111-1111-4111-8111-111111111111",
            serverID: "22222222-2222-4222-8222-222222222222",
            clientID: "33333333-3333-4333-8333-333333333333"
        )
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [LoopbackOAuthURLProtocol.self]
        let service = AKSServicePrincipalTokenService(
            httpClient: URLSessionAKSServicePrincipalTokenHTTPClient(sessionConfiguration: configuration)
        )

        let token = try await service.token(
            descriptor: descriptor,
            credentials: try AKSServicePrincipalCredentials(
                clientID: descriptor.clientID,
                clientSecret: "synthetic-aks-secret"
            )
        )

        XCTAssertEqual(token.value, "synthetic-aks-loopback-token")
        let request = try XCTUnwrap(server.requests().only)
        XCTAssertEqual(request.method, "POST")
        XCTAssertEqual(request.path, "/\(descriptor.tenantID)/oauth2/v2.0/token")
        XCTAssertEqual(request.headers["content-type"], "application/x-www-form-urlencoded")
        XCTAssertEqual(request.headers["x-rune-original-host"], "login.microsoftonline.com")
        XCTAssertTrue(request.body.contains("grant_type=client_credentials"), request.body)
        XCTAssertTrue(request.body.contains("client_id=33333333-3333-4333-8333-333333333333"), request.body)
        XCTAssertTrue(request.body.contains("client_secret=synthetic-aks-secret"), request.body)
    }

    func testGKEServiceAccountTransportPostsSignedAssertionToSyntheticLoopbackResponseServer() async throws {
        let server = try await LoopbackOAuthResponseServer.start(
            body: #"{"access_token":"synthetic-gke-loopback-token","token_type":"Bearer","expires_in":3600}"#
        )
        defer { server.stop() }
        LoopbackOAuthURLProtocol.configure(baseURL: server.baseURL)

        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [LoopbackOAuthURLProtocol.self]
        let date = try fixedDate()
        let provider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: try syntheticServiceAccountJSON(),
            httpClient: GCPServiceAccountURLSessionHTTPClient(sessionConfiguration: configuration),
            now: { date }
        )

        let token = try await provider.accessToken()

        XCTAssertEqual(token.value, "synthetic-gke-loopback-token")
        let request = try XCTUnwrap(server.requests().only)
        XCTAssertEqual(request.method, "POST")
        XCTAssertEqual(request.path, "/token")
        XCTAssertEqual(request.headers["content-type"], "application/x-www-form-urlencoded")
        XCTAssertEqual(request.headers["x-rune-original-host"], "oauth2.googleapis.com")
        XCTAssertTrue(request.body.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"), request.body)
        XCTAssertTrue(request.body.contains("assertion="), request.body)
        XCTAssertFalse(request.body.contains("BEGIN%20PRIVATE%20KEY"))
    }

    func testEKSSigV4TokenIsVerifiedOfflineBecauseTheKubernetesAuthenticatorRedeemsIt() throws {
        let descriptor = try XCTUnwrap(AWSEKSExecDescriptor.parseIfSupported(
            command: "aws",
            arguments: [
                "eks", "get-token",
                "--cluster-name", "synthetic-eks-cluster",
                "--region", "eu-north-1"
            ]
        ))
        let artifacts = try AWSEKSTokenSigner().signingArtifacts(
            descriptor: descriptor,
            credentials: AWSEKSCredentials(
                accessKeyID: "AKIASYNTHETIC000001",
                secretAccessKey: "syntheticSecretKeyForOfflineVerifierOnly0001"
            ),
            signingDate: try fixedDate()
        )
        let url = try XCTUnwrap(URLComponents(string: artifacts.presignedURL))

        XCTAssertEqual(url.host, "sts.eu-north-1.amazonaws.com")
        XCTAssertEqual(url.queryItems?.first(where: { $0.name == "Action" })?.value, "GetCallerIdentity")
        XCTAssertEqual(url.queryItems?.first(where: { $0.name == "X-Amz-SignedHeaders" })?.value, "host;x-k8s-aws-id")
        XCTAssertTrue(artifacts.token.hasPrefix("k8s-aws-v1."))
        XCTAssertFalse(artifacts.token.contains("="))
    }

    private func fixedDate() throws -> Date {
        let components = DateComponents(
            calendar: Calendar(identifier: .gregorian),
            timeZone: TimeZone(secondsFromGMT: 0),
            year: 2025,
            month: 2,
            day: 3,
            hour: 4,
            minute: 5,
            second: 6
        )
        return try XCTUnwrap(components.date)
    }

    private func syntheticServiceAccountJSON() throws -> Data {
        let attributes: [CFString: Any] = [
            kSecAttrKeyType: kSecAttrKeyTypeRSA,
            kSecAttrKeySizeInBits: 2_048,
            kSecAttrIsPermanent: false
        ]
        var keyError: Unmanaged<CFError>?
        let privateKey = try XCTUnwrap(SecKeyCreateRandomKey(attributes as CFDictionary, &keyError))
        var exportError: Unmanaged<CFError>?
        let pkcs1 = try XCTUnwrap(SecKeyCopyExternalRepresentation(privateKey, &exportError) as Data?)
        let pkcs8 = derElement(tag: 0x30, value:
            Data([0x02, 0x01, 0x00])
            + Data([0x30, 0x0D, 0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01, 0x05, 0x00])
            + derElement(tag: 0x04, value: pkcs1)
        )
        let pem = pemEncoded(pkcs8)
        return try JSONSerialization.data(withJSONObject: [
            "type": "service_account",
            "private_key": pem,
            "client_email": "rune-test@synthetic-project.iam.gserviceaccount.com",
            "token_uri": "https://oauth2.googleapis.com/token"
        ], options: [.sortedKeys])
    }

    private func pemEncoded(_ data: Data) -> String {
        let encoded = data.base64EncodedString()
        let lines = stride(from: 0, to: encoded.count, by: 64).map { offset -> String in
            let start = encoded.index(encoded.startIndex, offsetBy: offset)
            let end = encoded.index(start, offsetBy: min(64, encoded.count - offset))
            return String(encoded[start..<end])
        }
        return (["-----BEGIN PRIVATE KEY-----"] + lines + ["-----END PRIVATE KEY-----"])
            .joined(separator: "\n")
    }

    private func derElement(tag: UInt8, value: Data) -> Data {
        var result = Data([tag])
        if value.count < 128 {
            result.append(UInt8(value.count))
        } else {
            var bytes: [UInt8] = []
            var length = value.count
            while length > 0 {
                bytes.insert(UInt8(length & 0xFF), at: 0)
                length >>= 8
            }
            result.append(0x80 | UInt8(bytes.count))
            result.append(contentsOf: bytes)
        }
        result.append(value)
        return result
    }
}

private final class LoopbackOAuthURLProtocol: URLProtocol, @unchecked Sendable {
    private static let state = State()

    override class func canInit(with request: URLRequest) -> Bool {
        guard let host = request.url?.host?.lowercased() else { return false }
        return host == "login.microsoftonline.com" || host == "oauth2.googleapis.com"
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let originalRequest = request
        Task {
            do {
                let result = try await Self.state.forward(originalRequest)
                guard let originalURL = originalRequest.url,
                      let response = HTTPURLResponse(
                        url: originalURL,
                        statusCode: result.statusCode,
                        httpVersion: "HTTP/1.1",
                        headerFields: result.headers
                      ) else {
                    throw URLError(.badServerResponse)
                }
                client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
                client?.urlProtocol(self, didLoad: result.body)
                client?.urlProtocolDidFinishLoading(self)
            } catch {
                client?.urlProtocol(self, didFailWithError: error)
            }
        }
    }

    override func stopLoading() {}

    static func configure(baseURL: URL) { state.configure(baseURL: baseURL) }
    static func reset() { state.reset() }

    private final class State: @unchecked Sendable {
        private let lock = NSLock()
        private var baseURL: URL?

        func configure(baseURL: URL) { lock.withLock { self.baseURL = baseURL } }
        func reset() { lock.withLock { baseURL = nil } }

        func forward(_ original: URLRequest) async throws -> (statusCode: Int, headers: [String: String], body: Data) {
            guard let baseURL = lock.withLock({ self.baseURL }),
                  let originalURL = original.url else {
                throw URLError(.cannotConnectToHost)
            }
            var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false)
            components?.path = originalURL.path
            components?.query = originalURL.query
            guard let targetURL = components?.url else { throw URLError(.badURL) }
            var request = URLRequest(url: targetURL)
            request.httpMethod = original.httpMethod
            let requestBody = body(from: original)
            request.httpBody = requestBody
            for (name, value) in original.allHTTPHeaderFields ?? [:] {
                request.setValue(value, forHTTPHeaderField: name)
            }
            request.setValue(String(requestBody.count), forHTTPHeaderField: "Content-Length")
            request.setValue(originalURL.host, forHTTPHeaderField: "X-Rune-Original-Host")
            let configuration = URLSessionConfiguration.ephemeral
            configuration.urlCache = nil
            let (body, response) = try await URLSession(configuration: configuration).data(for: request)
            guard let http = response as? HTTPURLResponse else { throw URLError(.badServerResponse) }
            var headers: [String: String] = [:]
            for (key, value) in http.allHeaderFields {
                if let key = key as? String, let value = value as? String { headers[key] = value }
            }
            return (http.statusCode, headers, body)
        }

        private func body(from request: URLRequest) -> Data {
            if let body = request.httpBody { return body }
            guard let stream = request.httpBodyStream else { return Data() }
            stream.open()
            defer { stream.close() }
            var result = Data()
            var buffer = [UInt8](repeating: 0, count: 4_096)
            while stream.hasBytesAvailable {
                let count = stream.read(&buffer, maxLength: buffer.count)
                guard count > 0 else { break }
                result.append(buffer, count: count)
            }
            return result
        }
    }
}

private final class LoopbackOAuthResponseServer: @unchecked Sendable {
    struct Request: Sendable {
        let method: String
        let path: String
        let headers: [String: String]
        let body: String
    }

    let baseURL: URL
    private let listener: NWListener
    private let state: State

    private init(listener: NWListener, port: UInt16, state: State) {
        self.listener = listener
        self.baseURL = URL(string: "http://127.0.0.1:\(port)/")!
        self.state = state
    }

    static func start(body: String) async throws -> LoopbackOAuthResponseServer {
        let listener = try NWListener(using: .tcp, on: 0)
        let state = State(responseBody: body)
        listener.newConnectionHandler = { connection in
            connection.start(queue: DispatchQueue(label: "rune.oauth-loopback.connection"))
            let reader = LoopbackHTTPBodyReader(connection: connection)
            state.retain(reader)
            reader.receive { raw in
                state.record(raw)
                let responseBody = state.responseBody
                let response = [
                    "HTTP/1.1 200 OK",
                    "Content-Type: application/json",
                    "Content-Length: \(responseBody.utf8.count)",
                    "Connection: close",
                    "",
                    responseBody
                ].joined(separator: "\r\n")
                connection.send(content: Data(response.utf8), completion: .contentProcessed { _ in connection.cancel() })
            }
        }
        let port: UInt16 = try await withCheckedThrowingContinuation { continuation in
            listener.stateUpdateHandler = { status in
                switch status {
                case .ready:
                    if let port = listener.port?.rawValue { continuation.resume(returning: port) }
                    else { continuation.resume(throwing: URLError(.cannotConnectToHost)) }
                case .failed(let error): continuation.resume(throwing: error)
                default: break
                }
            }
            listener.start(queue: DispatchQueue(label: "rune.oauth-loopback.listener"))
        }
        listener.stateUpdateHandler = nil
        return LoopbackOAuthResponseServer(listener: listener, port: port, state: state)
    }

    func requests() -> [Request] { state.requests() }
    func stop() { listener.cancel() }

    private final class State: @unchecked Sendable {
        let responseBody: String
        private let lock = NSLock()
        private var received: [Request] = []
        private var readers: [LoopbackHTTPBodyReader] = []

        init(responseBody: String) { self.responseBody = responseBody }

        func retain(_ reader: LoopbackHTTPBodyReader) {
            lock.withLock { readers.append(reader) }
        }

        func record(_ raw: String) {
            let parts = raw.components(separatedBy: "\r\n\r\n")
            let headerLines = parts[0].components(separatedBy: "\r\n")
            let requestLine = headerLines.first?.split(separator: " ", maxSplits: 2).map(String.init) ?? []
            let headers = Dictionary(uniqueKeysWithValues: headerLines.dropFirst().compactMap { line -> (String, String)? in
                guard let split = line.firstIndex(of: ":") else { return nil }
                return (
                    line[..<split].trimmingCharacters(in: .whitespaces).lowercased(),
                    line[line.index(after: split)...].trimmingCharacters(in: .whitespaces)
                )
            })
            lock.withLock {
                received.append(Request(
                    method: requestLine.first ?? "",
                    path: requestLine.dropFirst().first ?? "",
                    headers: headers,
                    body: parts.dropFirst().joined(separator: "\r\n\r\n")
                ))
            }
        }

        func requests() -> [Request] { lock.withLock { received } }
    }
}

private extension Array where Element == LoopbackOAuthResponseServer.Request {
    var only: Element? { count == 1 ? first : nil }
}

private final class LoopbackHTTPBodyReader: @unchecked Sendable {
    private let connection: NWConnection
    private var bytes = Data()
    private var completed = false

    init(connection: NWConnection) {
        self.connection = connection
    }

    func receive(completion: @escaping @Sendable (String) -> Void) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 65_536) { [weak self] data, _, complete, _ in
            guard let self else { return }
            if let data { self.bytes.append(data) }
            guard !self.completed else { return }
            if let raw = String(data: self.bytes, encoding: .utf8),
               let headerEnd = raw.range(of: "\r\n\r\n") {
                let headers = raw[..<headerEnd.lowerBound]
                let contentLength = headers
                    .components(separatedBy: "\r\n")
                    .first { $0.lowercased().hasPrefix("content-length:") }
                    .flatMap { Int($0.split(separator: ":", maxSplits: 1)[1].trimmingCharacters(in: .whitespaces)) }
                    ?? 0
                let bodyStart = raw.distance(from: raw.startIndex, to: headerEnd.upperBound)
                if self.bytes.count >= bodyStart + contentLength {
                    self.completed = true
                    completion(raw)
                    return
                }
            }
            if complete {
                self.completed = true
                self.connection.cancel()
                return
            }
            self.receive(completion: completion)
        }
    }
}
