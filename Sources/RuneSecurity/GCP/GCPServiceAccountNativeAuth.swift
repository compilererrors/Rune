import Foundation
@preconcurrency import Security

public enum GCPServiceAccountAuthError: Error, LocalizedError, Sendable, Equatable {
    case invalidJSON
    case unsupportedCredentialType
    case missingRequiredField(String)
    case invalidClientEmail
    case invalidPrivateKey
    case invalidTokenURI
    case signingFailed
    case networkFailure
    case invalidHTTPResponse
    case responseTooLarge
    case tokenEndpointRejected(Int)
    case invalidTokenResponse

    public var errorDescription: String? {
        switch self {
        case .invalidJSON:
            return "The Google service-account credential is not valid JSON."
        case .unsupportedCredentialType:
            return "This Google credential type is not supported by native authentication. Select a service-account JSON credential."
        case let .missingRequiredField(field):
            return "The Google service-account credential is missing required field \(Self.safeField(field))."
        case .invalidClientEmail:
            return "The Google service-account credential contains an invalid client email."
        case .invalidPrivateKey:
            return "The Google service-account private key is invalid or unsupported."
        case .invalidTokenURI:
            return "The Google service-account token endpoint is invalid or unsupported."
        case .signingFailed:
            return "Rune could not sign the Google service-account assertion."
        case .networkFailure:
            return "Rune could not reach the Google OAuth token endpoint."
        case .invalidHTTPResponse:
            return "The Google OAuth token endpoint returned an invalid HTTP response."
        case .responseTooLarge:
            return "The Google OAuth token response exceeded the supported size."
        case let .tokenEndpointRejected(statusCode):
            let safeStatus = (100...599).contains(statusCode) ? statusCode : 0
            return "Google OAuth rejected the service-account assertion (HTTP \(safeStatus))."
        case .invalidTokenResponse:
            return "Google OAuth returned an invalid access-token response."
        }
    }

    private static func safeField(_ value: String) -> String {
        let allowed = CharacterSet(charactersIn: "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
        let scalars = value.unicodeScalars.filter { allowed.contains($0) }
        let result = String(String.UnicodeScalarView(scalars)).prefix(64)
        return result.isEmpty ? "<unknown>" : String(result)
    }
}

/// Validated material from a Google ADC `service_account` JSON document.
/// The private key must only be persisted in a platform secret store.
public struct GCPServiceAccountCredential: Sendable, Equatable, CustomStringConvertible, CustomDebugStringConvertible {
    public let clientEmail: String
    public let privateKeyPEM: String
    public let tokenURI: URL

    public static let cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
    public static let userInfoEmailScope = "https://www.googleapis.com/auth/userinfo.email"
    public static let requiredScopes = [cloudPlatformScope, userInfoEmailScope]

    public init(jsonData: Data) throws {
        guard !jsonData.isEmpty, jsonData.count <= 1_048_576 else {
            throw GCPServiceAccountAuthError.invalidJSON
        }
        let document: Document
        do {
            document = try JSONDecoder().decode(Document.self, from: jsonData)
        } catch {
            throw GCPServiceAccountAuthError.invalidJSON
        }
        guard document.type == "service_account" else {
            throw GCPServiceAccountAuthError.unsupportedCredentialType
        }
        let email = try Self.required(document.clientEmail, field: "client_email")
        let privateKey = try Self.required(document.privateKey, field: "private_key")
        let tokenURIValue = try Self.required(document.tokenURI, field: "token_uri")

        guard email.utf8.count <= 320,
              email.range(
                  of: #"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+$"#,
                  options: .regularExpression
              ) != nil,
              email.unicodeScalars.allSatisfy({ !CharacterSet.controlCharacters.contains($0) }) else {
            throw GCPServiceAccountAuthError.invalidClientEmail
        }
        guard privateKey.utf8.count <= 65_536,
              privateKey.contains("-----BEGIN PRIVATE KEY-----"),
              privateKey.contains("-----END PRIVATE KEY-----") else {
            throw GCPServiceAccountAuthError.invalidPrivateKey
        }
        guard let tokenURI = URL(string: tokenURIValue), Self.isAllowedTokenURI(tokenURI) else {
            throw GCPServiceAccountAuthError.invalidTokenURI
        }
        // Parse eagerly so malformed key material never reaches a network request.
        _ = try GCPServiceAccountJWTAssertionSigner.privateKey(fromPKCS8PEM: privateKey)

        self.clientEmail = email
        self.privateKeyPEM = privateKey
        self.tokenURI = tokenURI
    }

    public var description: String { "GCPServiceAccountCredential(<redacted>)" }
    public var debugDescription: String { description }

    private static func required(_ value: String?, field: String) throws -> String {
        let normalized = value?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !normalized.isEmpty else {
            throw GCPServiceAccountAuthError.missingRequiredField(field)
        }
        return normalized
    }

    private static func isAllowedTokenURI(_ url: URL) -> Bool {
        guard url.scheme?.lowercased() == "https",
              url.user == nil,
              url.password == nil,
              url.fragment == nil,
              url.query == nil,
              url.port == nil || url.port == 443,
              url.path == "/token" else {
            return false
        }
        let allowedHosts: Set<String> = ["oauth2.googleapis.com"]
        return url.host.map { allowedHosts.contains($0.lowercased()) } ?? false
    }

    private struct Document: Decodable {
        let type: String?
        let clientEmail: String?
        let privateKey: String?
        let tokenURI: String?

        enum CodingKeys: String, CodingKey {
            case type
            case clientEmail = "client_email"
            case privateKey = "private_key"
            case tokenURI = "token_uri"
        }
    }
}

public struct GCPServiceAccountHTTPRequest: Sendable, Equatable {
    public let url: URL
    public let method: String
    public let headers: [String: String]
    public let body: Data

    public init(url: URL, method: String, headers: [String: String], body: Data) {
        self.url = url
        self.method = method
        self.headers = headers
        self.body = body
    }
}

public struct GCPServiceAccountHTTPResponse: Sendable, Equatable {
    public let statusCode: Int
    public let headers: [String: String]
    public let body: Data

    public init(statusCode: Int, headers: [String: String] = [:], body: Data) {
        self.statusCode = statusCode
        self.headers = headers
        self.body = body
    }
}

public protocol GCPServiceAccountHTTPClient: Sendable {
    func send(_ request: GCPServiceAccountHTTPRequest) async throws -> GCPServiceAccountHTTPResponse
}

/// URLSession implementation used by the production provider. Redirects are
/// rejected so a signed assertion is never forwarded to another origin.
public final class GCPServiceAccountURLSessionHTTPClient: GCPServiceAccountHTTPClient, @unchecked Sendable {
    private let sessionConfiguration: URLSessionConfiguration

    public init() {
        self.sessionConfiguration = .ephemeral
    }

    /// Test-only transport seam. Credential validation still pins requests to
    /// Google's HTTPS token endpoint; production callers use `init()`.
    init(sessionConfiguration: URLSessionConfiguration) {
        self.sessionConfiguration = sessionConfiguration
    }

    public func send(_ request: GCPServiceAccountHTTPRequest) async throws -> GCPServiceAccountHTTPResponse {
        var urlRequest = URLRequest(url: request.url)
        urlRequest.httpMethod = request.method
        urlRequest.httpBody = request.body
        urlRequest.timeoutInterval = 30
        for (name, value) in request.headers {
            urlRequest.setValue(value, forHTTPHeaderField: name)
        }

        let configuration = sessionConfiguration.copy() as! URLSessionConfiguration
        configuration.waitsForConnectivity = false
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.httpCookieStorage = nil
        configuration.httpShouldSetCookies = false
        let delegate = GCPServiceAccountNoRedirectDelegate()
        let session = URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
        defer { session.invalidateAndCancel() }
        do {
            let (bytes, response) = try await session.bytes(for: urlRequest)
            guard let httpResponse = response as? HTTPURLResponse else {
                throw GCPServiceAccountAuthError.invalidHTTPResponse
            }
            if let rawLength = httpResponse.value(forHTTPHeaderField: "Content-Length"),
               let contentLength = Int(rawLength),
               contentLength > 1_048_576 {
                throw GCPServiceAccountAuthError.responseTooLarge
            }
            var data = Data()
            data.reserveCapacity(64 * 1_024)
            for try await byte in bytes {
                try Task.checkCancellation()
                guard data.count < 1_048_576 else {
                    throw GCPServiceAccountAuthError.responseTooLarge
                }
                data.append(byte)
            }
            var headers: [String: String] = [:]
            for (key, value) in httpResponse.allHeaderFields {
                guard let key = key as? String, let value = value as? String else { continue }
                headers[key] = value
            }
            return GCPServiceAccountHTTPResponse(
                statusCode: httpResponse.statusCode,
                headers: headers,
                body: data
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as URLError where error.code == .cancelled {
            throw CancellationError()
        } catch let error as GCPServiceAccountAuthError {
            throw error
        } catch {
            throw GCPServiceAccountAuthError.networkFailure
        }
    }
}

private final class GCPServiceAccountNoRedirectDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(
        _: URLSession,
        task _: URLSessionTask,
        willPerformHTTPRedirection _: HTTPURLResponse,
        newRequest _: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}

public struct GCPServiceAccountAccessToken: Sendable, Equatable, CustomStringConvertible, CustomDebugStringConvertible {
    public let value: String
    public let expiration: Date

    public var description: String { "GCPServiceAccountAccessToken(<redacted>)" }
    public var debugDescription: String { description }
}

/// Native OAuth service-account provider with an in-memory access-token cache
/// and singleflight refresh. It never reads gcloud files or executes a process.
public actor GCPServiceAccountCredentialProvider {
    private let credential: GCPServiceAccountCredential
    private let httpClient: any GCPServiceAccountHTTPClient
    private let now: @Sendable () -> Date
    private var cachedToken: GCPServiceAccountAccessToken?
    private var inFlight: Task<GCPServiceAccountAccessToken, Error>?

    public init(
        serviceAccountJSON: Data,
        httpClient: any GCPServiceAccountHTTPClient = GCPServiceAccountURLSessionHTTPClient(),
        now: @escaping @Sendable () -> Date = { Date() }
    ) throws {
        self.credential = try GCPServiceAccountCredential(jsonData: serviceAccountJSON)
        self.httpClient = httpClient
        self.now = now
    }

    public func accessToken() async throws -> GCPServiceAccountAccessToken {
        let currentDate = now()
        if let cachedToken, cachedToken.expiration > currentDate.addingTimeInterval(60) {
            return cachedToken
        }
        if let inFlight {
            return try await inFlight.value
        }

        let credential = self.credential
        let httpClient = self.httpClient
        let now = self.now
        let task = Task<GCPServiceAccountAccessToken, Error> {
            try await Self.fetchToken(
                credential: credential,
                httpClient: httpClient,
                signingDate: now()
            )
        }
        inFlight = task
        do {
            let token = try await task.value
            cachedToken = token
            inFlight = nil
            return token
        } catch {
            inFlight = nil
            if error is CancellationError {
                throw CancellationError()
            }
            if let known = error as? GCPServiceAccountAuthError {
                throw known
            }
            throw GCPServiceAccountAuthError.networkFailure
        }
    }

    public func invalidate() {
        cachedToken = nil
        inFlight?.cancel()
        inFlight = nil
    }

    private static func fetchToken(
        credential: GCPServiceAccountCredential,
        httpClient: any GCPServiceAccountHTTPClient,
        signingDate: Date
    ) async throws -> GCPServiceAccountAccessToken {
        let assertion = try GCPServiceAccountJWTAssertionSigner.assertion(
            credential: credential,
            signingDate: signingDate
        )
        let form = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", assertion)
        ]
        .map { Self.formEncode($0.0) + "=" + Self.formEncode($0.1) }
        .joined(separator: "&")
        let request = GCPServiceAccountHTTPRequest(
            url: credential.tokenURI,
            method: "POST",
            headers: [
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            ],
            body: Data(form.utf8)
        )
        let response: GCPServiceAccountHTTPResponse
        do {
            response = try await httpClient.send(request)
        } catch let error as GCPServiceAccountAuthError {
            throw error
        } catch {
            throw GCPServiceAccountAuthError.networkFailure
        }
        guard response.body.count <= 1_048_576 else {
            throw GCPServiceAccountAuthError.responseTooLarge
        }
        guard (200...299).contains(response.statusCode) else {
            throw GCPServiceAccountAuthError.tokenEndpointRejected(response.statusCode)
        }

        let payload: TokenResponse
        do {
            payload = try JSONDecoder().decode(TokenResponse.self, from: response.body)
        } catch {
            throw GCPServiceAccountAuthError.invalidTokenResponse
        }
        let token = payload.accessToken.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !token.isEmpty,
              token.utf8.count <= 131_072,
              token.unicodeScalars.allSatisfy({ !CharacterSet.controlCharacters.contains($0) }),
              payload.expiresIn > 0,
              payload.expiresIn <= 86_400,
              payload.tokenType.map({ $0.caseInsensitiveCompare("Bearer") == .orderedSame }) ?? true else {
            throw GCPServiceAccountAuthError.invalidTokenResponse
        }
        return GCPServiceAccountAccessToken(
            value: token,
            expiration: signingDate.addingTimeInterval(TimeInterval(payload.expiresIn))
        )
    }

    private static func formEncode(_ value: String) -> String {
        var output = ""
        output.reserveCapacity(value.utf8.count)
        for byte in value.utf8 {
            switch byte {
            case 65...90, 97...122, 48...57, 45, 46, 95, 126:
                output.unicodeScalars.append(UnicodeScalar(byte))
            default:
                output.append("%")
                output.append(String(format: "%02X", byte))
            }
        }
        return output
    }

    private struct TokenResponse: Decodable {
        let accessToken: String
        let expiresIn: Int
        let tokenType: String?

        enum CodingKeys: String, CodingKey {
            case accessToken = "access_token"
            case expiresIn = "expires_in"
            case tokenType = "token_type"
        }
    }
}

enum GCPServiceAccountJWTAssertionSigner {
    static func assertion(
        credential: GCPServiceAccountCredential,
        signingDate: Date
    ) throws -> String {
        let issuedAt = Int(signingDate.timeIntervalSince1970.rounded(.down))
        let expiresAt = issuedAt + 3_600
        let header: [String: Any] = ["alg": "RS256", "typ": "JWT"]
        let claims: [String: Any] = [
            "iss": credential.clientEmail,
            "scope": GCPServiceAccountCredential.requiredScopes.joined(separator: " "),
            "aud": credential.tokenURI.absoluteString,
            "iat": issuedAt,
            "exp": expiresAt
        ]
        let headerData: Data
        let claimsData: Data
        do {
            headerData = try JSONSerialization.data(withJSONObject: header, options: [.sortedKeys])
            claimsData = try JSONSerialization.data(withJSONObject: claims, options: [.sortedKeys])
        } catch {
            throw GCPServiceAccountAuthError.signingFailed
        }
        let signingInput = rawURLBase64(headerData) + "." + rawURLBase64(claimsData)
        let key = try privateKey(fromPKCS8PEM: credential.privateKeyPEM)
        var signingError: Unmanaged<CFError>?
        guard let signature = SecKeyCreateSignature(
            key,
            .rsaSignatureMessagePKCS1v15SHA256,
            Data(signingInput.utf8) as CFData,
            &signingError
        ) as Data? else {
            throw GCPServiceAccountAuthError.signingFailed
        }
        return signingInput + "." + rawURLBase64(signature)
    }

    static func privateKey(fromPKCS8PEM pem: String) throws -> SecKey {
        let body = pem
            .replacingOccurrences(of: "-----BEGIN PRIVATE KEY-----", with: "")
            .replacingOccurrences(of: "-----END PRIVATE KEY-----", with: "")
            .components(separatedBy: .whitespacesAndNewlines)
            .joined()
        guard let pkcs8 = Data(base64Encoded: body), !pkcs8.isEmpty else {
            throw GCPServiceAccountAuthError.invalidPrivateKey
        }
        let pkcs1 = try PKCS8RSAKeyParser.privateKey(from: pkcs8)
        let attributes: [CFString: Any] = [
            kSecAttrKeyType: kSecAttrKeyTypeRSA,
            kSecAttrKeyClass: kSecAttrKeyClassPrivate
        ]
        var creationError: Unmanaged<CFError>?
        guard let key = SecKeyCreateWithData(pkcs1 as CFData, attributes as CFDictionary, &creationError),
              SecKeyGetBlockSize(key) >= 256,
              SecKeyIsAlgorithmSupported(key, .sign, .rsaSignatureMessagePKCS1v15SHA256) else {
            throw GCPServiceAccountAuthError.invalidPrivateKey
        }
        return key
    }

    private static func rawURLBase64(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}

private enum PKCS8RSAKeyParser {
    private static let rsaEncryptionOID = Data([0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01])

    static func privateKey(from data: Data) throws -> Data {
        var documentReader = DERReader(data: data)
        let document = try documentReader.readElement(expectedTag: 0x30)
        guard documentReader.isAtEnd else { throw GCPServiceAccountAuthError.invalidPrivateKey }

        var reader = DERReader(data: document)
        let version = try reader.readElement(expectedTag: 0x02)
        guard version == Data([0]) else { throw GCPServiceAccountAuthError.invalidPrivateKey }

        let algorithmData = try reader.readElement(expectedTag: 0x30)
        var algorithmReader = DERReader(data: algorithmData)
        let oid = try algorithmReader.readElement(expectedTag: 0x06)
        guard oid == rsaEncryptionOID else { throw GCPServiceAccountAuthError.invalidPrivateKey }
        if !algorithmReader.isAtEnd {
            let nullValue = try algorithmReader.readElement(expectedTag: 0x05)
            guard nullValue.isEmpty, algorithmReader.isAtEnd else {
                throw GCPServiceAccountAuthError.invalidPrivateKey
            }
        }
        let privateKey = try reader.readElement(expectedTag: 0x04)
        guard !privateKey.isEmpty else { throw GCPServiceAccountAuthError.invalidPrivateKey }
        return privateKey
    }

    private struct DERReader {
        let data: Data
        var offset = 0

        var isAtEnd: Bool { offset == data.count }

        mutating func readElement(expectedTag: UInt8) throws -> Data {
            guard offset < data.count, data[offset] == expectedTag else {
                throw GCPServiceAccountAuthError.invalidPrivateKey
            }
            offset += 1
            let length = try readLength()
            guard length >= 0, offset <= data.count, length <= data.count - offset else {
                throw GCPServiceAccountAuthError.invalidPrivateKey
            }
            let value = Data(data[offset..<(offset + length)])
            offset += length
            return value
        }

        mutating func readLength() throws -> Int {
            guard offset < data.count else { throw GCPServiceAccountAuthError.invalidPrivateKey }
            let first = data[offset]
            offset += 1
            if first & 0x80 == 0 { return Int(first) }
            let byteCount = Int(first & 0x7F)
            guard byteCount > 0, byteCount <= 4, offset <= data.count - byteCount else {
                throw GCPServiceAccountAuthError.invalidPrivateKey
            }
            var length = 0
            for _ in 0..<byteCount {
                guard length <= (Int.max >> 8) else {
                    throw GCPServiceAccountAuthError.invalidPrivateKey
                }
                length = (length << 8) | Int(data[offset])
                offset += 1
            }
            return length
        }
    }
}
