import CoreFoundation
import Foundation

// MARK: - Configuration

/// A normalized Kubernetes `auth-provider: oidc` configuration.
///
/// The type intentionally does not write refreshed credentials back to kubeconfig. Callers can
/// persist `OIDCNativeCredentialSession.consumeRefreshTokenUpdate()` in a secret store instead.
public struct OIDCAuthProviderConfiguration: Sendable, Equatable {
    public let issuerURL: URL
    public let clientID: String
    public let clientSecret: String?
    public let idToken: String?
    public let refreshToken: String?
    public let certificateAuthorityPath: String?
    public let extraScopes: [String]

    public init(
        issuerURL: URL,
        clientID: String,
        clientSecret: String? = nil,
        idToken: String? = nil,
        refreshToken: String? = nil,
        certificateAuthorityPath: String? = nil,
        extraScopes: [String] = []
    ) throws {
        self.issuerURL = try OIDCURLValidator.validatedIssuer(issuerURL)
        self.clientID = try Self.required(clientID, field: "client-id")
        self.clientSecret = Self.nonEmpty(clientSecret)
        self.idToken = Self.nonEmpty(idToken)
        self.refreshToken = Self.nonEmpty(refreshToken)
        self.certificateAuthorityPath = Self.nonEmpty(certificateAuthorityPath)
        self.extraScopes = Self.normalizedScopes(extraScopes)
    }

    /// Builds the model from the `user.auth-provider` entry used by kubeconfig v1.
    public init(authProviderName: String, config: [String: String]) throws {
        guard authProviderName.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == "oidc" else {
            throw OIDCNativeAuthError.invalidConfiguration(field: "auth-provider.name")
        }
        guard let rawIssuer = config["idp-issuer-url"], let issuerURL = URL(string: rawIssuer) else {
            throw OIDCNativeAuthError.invalidConfiguration(field: "idp-issuer-url")
        }

        let scopes = config["extra-scopes"]?
            .split(separator: ",", omittingEmptySubsequences: true)
            .map(String.init) ?? []
        try self.init(
            issuerURL: issuerURL,
            clientID: config["client-id"] ?? "",
            clientSecret: config["client-secret"],
            idToken: config["id-token"],
            refreshToken: config["refresh-token"],
            certificateAuthorityPath: config["idp-certificate-authority"],
            extraScopes: scopes
        )
    }

    private static func required(_ value: String, field: String) throws -> String {
        guard let value = nonEmpty(value) else {
            throw OIDCNativeAuthError.invalidConfiguration(field: field)
        }
        return value
    }

    private static func nonEmpty(_ value: String?) -> String? {
        guard let value else { return nil }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    private static func normalizedScopes(_ scopes: [String]) -> [String] {
        var seen = Set<String>()
        return scopes.compactMap { raw in
            let value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !value.isEmpty, seen.insert(value).inserted else { return nil }
            return value
        }
    }
}

// MARK: - JWT metadata

/// Non-verified JWT metadata used only for expiry and response consistency checks.
/// Signature validation remains the responsibility of the Kubernetes API server.
public struct OIDCJWTMetadata: Sendable, Equatable {
    public let issuer: String?
    public let audiences: [String]
    public let expiration: Date?

    public static func parse(_ token: String) throws -> OIDCJWTMetadata {
        guard token.utf8.count <= 256 * 1_024 else {
            throw OIDCNativeAuthError.invalidIDToken
        }
        let segments = token.split(separator: ".", omittingEmptySubsequences: false)
        guard segments.count == 3, segments.allSatisfy({ !$0.isEmpty }) else {
            throw OIDCNativeAuthError.invalidIDToken
        }
        guard let payload = decodeBase64URL(String(segments[1])), payload.count <= 128 * 1_024 else {
            throw OIDCNativeAuthError.invalidIDToken
        }
        guard let object = try? JSONSerialization.jsonObject(with: payload),
              let claims = object as? [String: Any] else {
            throw OIDCNativeAuthError.invalidIDToken
        }

        let issuer: String?
        if let rawIssuer = claims["iss"] {
            guard let value = rawIssuer as? String, !value.isEmpty else {
                throw OIDCNativeAuthError.invalidIDToken
            }
            issuer = value
        } else {
            issuer = nil
        }

        let audiences: [String]
        switch claims["aud"] {
        case nil:
            audiences = []
        case let value as String where !value.isEmpty:
            audiences = [value]
        case let values as [String] where !values.isEmpty && values.allSatisfy({ !$0.isEmpty }):
            audiences = values
        default:
            throw OIDCNativeAuthError.invalidIDToken
        }

        let expiration: Date?
        if let rawExpiration = claims["exp"] {
            guard let number = rawExpiration as? NSNumber,
                  CFGetTypeID(number) != CFBooleanGetTypeID() else {
                throw OIDCNativeAuthError.invalidIDToken
            }
            let seconds = number.doubleValue
            guard seconds.isFinite, seconds >= 0, seconds <= 253_402_300_799 else {
                throw OIDCNativeAuthError.invalidIDToken
            }
            expiration = Date(timeIntervalSince1970: seconds)
        } else {
            expiration = nil
        }

        return OIDCJWTMetadata(issuer: issuer, audiences: audiences, expiration: expiration)
    }

    private static func decodeBase64URL(_ value: String) -> Data? {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_"))
        guard !value.isEmpty, value.unicodeScalars.allSatisfy(allowed.contains) else { return nil }
        var base64 = value.replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        let remainder = base64.utf8.count % 4
        if remainder != 0 {
            base64.append(String(repeating: "=", count: 4 - remainder))
        }
        return Data(base64Encoded: base64)
    }
}

// MARK: - HTTP transport

public enum OIDCHTTPMethod: String, Sendable, Equatable {
    case get = "GET"
    case post = "POST"
}

public struct OIDCHTTPRequest: Sendable, Equatable {
    public let method: OIDCHTTPMethod
    public let url: URL
    public let headers: [String: String]
    public let body: Data?
    public let timeout: TimeInterval
    /// Optional kubeconfig CA path for transports that explicitly support custom trust material.
    public let customCertificateAuthorityPath: String?

    public init(
        method: OIDCHTTPMethod,
        url: URL,
        headers: [String: String] = [:],
        body: Data? = nil,
        timeout: TimeInterval = 15,
        customCertificateAuthorityPath: String? = nil
    ) {
        self.method = method
        self.url = url
        self.headers = headers
        self.body = body
        self.timeout = timeout
        self.customCertificateAuthorityPath = customCertificateAuthorityPath
    }
}

public struct OIDCHTTPResponse: Sendable, Equatable {
    public let statusCode: Int
    public let headers: [String: String]
    public let body: Data

    public init(statusCode: Int, headers: [String: String] = [:], body: Data) {
        self.statusCode = statusCode
        self.headers = headers
        self.body = body
    }
}

public protocol OIDCHTTPClient: Sendable {
    /// Custom CA support is explicit so the default transport never silently ignores kubeconfig trust settings.
    var supportsCustomCertificateAuthorities: Bool { get }
    func send(_ request: OIDCHTTPRequest) async throws -> OIDCHTTPResponse
}

public extension OIDCHTTPClient {
    var supportsCustomCertificateAuthorities: Bool { false }
}

/// Ephemeral URLSession transport that refuses HTTP redirects and uses system trust.
public final class URLSessionOIDCHTTPClient: OIDCHTTPClient, @unchecked Sendable {
    public let supportsCustomCertificateAuthorities = false

    private let session: URLSession
    private let maximumResponseBytes: Int

    public init(maximumResponseBytes: Int = 1_048_576) {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.timeoutIntervalForRequest = 15
        configuration.timeoutIntervalForResource = 30
        let delegate = OIDCNoRedirectSessionDelegate()
        self.session = URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
        self.maximumResponseBytes = max(1_024, min(maximumResponseBytes, 4_194_304))
    }

    public func send(_ request: OIDCHTTPRequest) async throws -> OIDCHTTPResponse {
        guard request.customCertificateAuthorityPath == nil else {
            throw OIDCNativeAuthError.customCertificateAuthorityUnsupported
        }
        var urlRequest = URLRequest(url: request.url)
        urlRequest.httpMethod = request.method.rawValue
        urlRequest.httpBody = request.body
        urlRequest.timeoutInterval = request.timeout
        for (name, value) in request.headers {
            urlRequest.setValue(value, forHTTPHeaderField: name)
        }
        let (bytes, response) = try await session.bytes(for: urlRequest)
        var data = Data()
        data.reserveCapacity(min(maximumResponseBytes, 64 * 1_024))
        for try await byte in bytes {
            guard data.count < maximumResponseBytes else {
                throw OIDCNativeAuthError.responseTooLarge(stage: .request)
            }
            data.append(byte)
        }
        guard let http = response as? HTTPURLResponse else {
            throw OIDCNativeAuthError.transport(stage: .request)
        }
        var headers: [String: String] = [:]
        for (name, value) in http.allHeaderFields {
            guard let name = name as? String else { continue }
            headers[name.lowercased()] = String(describing: value)
        }
        return OIDCHTTPResponse(statusCode: http.statusCode, headers: headers, body: data)
    }
}

private final class OIDCNoRedirectSessionDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}

// MARK: - Discovery and refresh

public struct OIDCDiscoveryDocument: Sendable, Equatable {
    public let issuer: URL
    public let tokenEndpoint: URL
    public let tokenEndpointAuthMethodsSupported: [String]

    public init(
        issuer: URL,
        tokenEndpoint: URL,
        tokenEndpointAuthMethodsSupported: [String] = []
    ) {
        self.issuer = issuer
        self.tokenEndpoint = tokenEndpoint
        self.tokenEndpointAuthMethodsSupported = tokenEndpointAuthMethodsSupported
    }
}

public struct OIDCNativeCredential: Sendable, Equatable {
    public let idToken: String
    public let expiration: Date

    public init(idToken: String, expiration: Date) {
        self.idToken = idToken
        self.expiration = expiration
    }
}

public struct OIDCRefreshResult: Sendable, Equatable {
    public let credential: OIDCNativeCredential
    /// The effective refresh token. It is the original value when the provider did not rotate it.
    public let refreshToken: String
    public let didRotateRefreshToken: Bool

    public init(credential: OIDCNativeCredential, refreshToken: String, didRotateRefreshToken: Bool) {
        self.credential = credential
        self.refreshToken = refreshToken
        self.didRotateRefreshToken = didRotateRefreshToken
    }
}

public protocol OIDCClock: Sendable {
    func now() -> Date
}

public struct SystemOIDCClock: OIDCClock {
    public init() {}
    public func now() -> Date { Date() }
}

public struct OIDCNativeAuthClient: Sendable {
    private let httpClient: any OIDCHTTPClient
    private let clock: any OIDCClock
    private let maximumResponseBytes: Int

    public init(
        httpClient: any OIDCHTTPClient = URLSessionOIDCHTTPClient(),
        clock: any OIDCClock = SystemOIDCClock(),
        maximumResponseBytes: Int = 1_048_576
    ) {
        self.httpClient = httpClient
        self.clock = clock
        self.maximumResponseBytes = max(1_024, min(maximumResponseBytes, 4_194_304))
    }

    public func discover(configuration: OIDCAuthProviderConfiguration) async throws -> OIDCDiscoveryDocument {
        if configuration.certificateAuthorityPath != nil,
           !httpClient.supportsCustomCertificateAuthorities {
            throw OIDCNativeAuthError.customCertificateAuthorityUnsupported
        }
        let discoveryURL = try OIDCURLValidator.discoveryURL(for: configuration.issuerURL)
        let response = try await send(
            OIDCHTTPRequest(
                method: .get,
                url: discoveryURL,
                headers: ["Accept": "application/json"],
                customCertificateAuthorityPath: configuration.certificateAuthorityPath
            ),
            stage: .discovery
        )
        guard response.body.count <= maximumResponseBytes else {
            throw OIDCNativeAuthError.responseTooLarge(stage: .discovery)
        }
        guard (200..<300).contains(response.statusCode) else {
            throw OIDCNativeAuthError.httpStatus(stage: .discovery, statusCode: response.statusCode)
        }

        let payload: DiscoveryPayload
        do {
            payload = try JSONDecoder().decode(DiscoveryPayload.self, from: response.body)
        } catch {
            throw OIDCNativeAuthError.invalidDiscoveryDocument
        }
        guard payload.issuer == configuration.issuerURL.absoluteString else {
            throw OIDCNativeAuthError.discoveryIssuerMismatch
        }
        guard let issuer = URL(string: payload.issuer),
              let endpoint = URL(string: payload.tokenEndpoint) else {
            throw OIDCNativeAuthError.invalidDiscoveryDocument
        }
        _ = try OIDCURLValidator.validatedIssuer(issuer)
        let tokenEndpoint = try OIDCURLValidator.validatedEndpoint(endpoint)
        return OIDCDiscoveryDocument(
            issuer: issuer,
            tokenEndpoint: tokenEndpoint,
            tokenEndpointAuthMethodsSupported: payload.tokenEndpointAuthMethodsSupported ?? []
        )
    }

    public func refresh(
        configuration: OIDCAuthProviderConfiguration,
        refreshToken suppliedRefreshToken: String? = nil
    ) async throws -> OIDCRefreshResult {
        let refreshToken = suppliedRefreshToken?.trimmingCharacters(in: .whitespacesAndNewlines)
            ?? configuration.refreshToken
            ?? ""
        guard !refreshToken.isEmpty else {
            throw OIDCNativeAuthError.missingRefreshToken
        }
        let discovery = try await discover(configuration: configuration)
        let authentication = try tokenEndpointAuthentication(
            configuration: configuration,
            methods: discovery.tokenEndpointAuthMethodsSupported
        )

        var fields = [
            ("grant_type", "refresh_token"),
            ("refresh_token", refreshToken)
        ]
        var headers = [
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded"
        ]
        switch authentication {
        case .none:
            fields.append(("client_id", configuration.clientID))
        case .clientSecretPost(let secret):
            fields.append(("client_id", configuration.clientID))
            fields.append(("client_secret", secret))
        case .clientSecretBasic(let secret):
            let user = Self.formEncoded(configuration.clientID)
            let password = Self.formEncoded(secret)
            let data = Data("\(user):\(password)".utf8)
            headers["Authorization"] = "Basic \(data.base64EncodedString())"
        }

        let body = Data(Self.formBody(fields).utf8)
        let response = try await send(
            OIDCHTTPRequest(
                method: .post,
                url: discovery.tokenEndpoint,
                headers: headers,
                body: body,
                customCertificateAuthorityPath: configuration.certificateAuthorityPath
            ),
            stage: .token
        )
        guard response.body.count <= maximumResponseBytes else {
            throw OIDCNativeAuthError.responseTooLarge(stage: .token)
        }

        let payload = try? JSONDecoder().decode(TokenPayload.self, from: response.body)
        if !(200..<300).contains(response.statusCode) {
            if let code = payload?.error {
                throw OIDCNativeAuthError.providerRejected(code: Self.sanitizedProviderCode(code))
            }
            throw OIDCNativeAuthError.httpStatus(stage: .token, statusCode: response.statusCode)
        }
        guard let payload else {
            throw OIDCNativeAuthError.invalidTokenResponse
        }
        if let code = payload.error {
            throw OIDCNativeAuthError.providerRejected(code: Self.sanitizedProviderCode(code))
        }
        guard let idToken = Self.nonEmpty(payload.idToken) else {
            throw OIDCNativeAuthError.tokenResponseMissingIDToken
        }
        let credential = try validatedCredential(
            idToken: idToken,
            configuration: configuration
        )
        let rotated = Self.nonEmpty(payload.refreshToken)
        return OIDCRefreshResult(
            credential: credential,
            refreshToken: rotated ?? refreshToken,
            didRotateRefreshToken: rotated != nil && rotated != refreshToken
        )
    }

    public func validatedCredential(
        idToken: String,
        configuration: OIDCAuthProviderConfiguration
    ) throws -> OIDCNativeCredential {
        let metadata = try OIDCJWTMetadata.parse(idToken)
        guard metadata.issuer == configuration.issuerURL.absoluteString else {
            throw OIDCNativeAuthError.tokenIssuerMismatch
        }
        guard metadata.audiences.contains(configuration.clientID) else {
            throw OIDCNativeAuthError.tokenAudienceMismatch
        }
        guard let expiration = metadata.expiration else {
            throw OIDCNativeAuthError.tokenMissingExpiration
        }
        guard expiration > clock.now() else {
            throw OIDCNativeAuthError.tokenExpired
        }
        return OIDCNativeCredential(idToken: idToken, expiration: expiration)
    }

    private func send(_ request: OIDCHTTPRequest, stage: OIDCNativeAuthStage) async throws -> OIDCHTTPResponse {
        do {
            return try await httpClient.send(request)
        } catch let error as OIDCNativeAuthError {
            if case .responseTooLarge = error {
                throw OIDCNativeAuthError.responseTooLarge(stage: stage)
            }
            throw error
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            // Underlying transport errors can contain query values, paths, account names, or tokens.
            throw OIDCNativeAuthError.transport(stage: stage)
        }
    }

    private func tokenEndpointAuthentication(
        configuration: OIDCAuthProviderConfiguration,
        methods: [String]
    ) throws -> TokenEndpointAuthentication {
        let supported = Set(methods.map { $0.lowercased() })
        if let secret = configuration.clientSecret {
            if supported.isEmpty || supported.contains("client_secret_basic") {
                return .clientSecretBasic(secret)
            }
            if supported.contains("client_secret_post") {
                return .clientSecretPost(secret)
            }
            throw OIDCNativeAuthError.unsupportedTokenEndpointAuthentication
        }
        if !supported.isEmpty, !supported.contains("none") {
            throw OIDCNativeAuthError.unsupportedTokenEndpointAuthentication
        }
        return .none
    }

    private static func formBody(_ fields: [(String, String)]) -> String {
        fields
            .map { "\(formEncoded($0.0))=\(formEncoded($0.1))" }
            .joined(separator: "&")
    }

    private static func formEncoded(_ value: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-._~"))
        return value.addingPercentEncoding(withAllowedCharacters: allowed) ?? ""
    }

    private static func nonEmpty(_ value: String?) -> String? {
        guard let value else { return nil }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    private static func sanitizedProviderCode(_ raw: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_."))
        let scalars = raw.unicodeScalars.prefix(64).filter(allowed.contains)
        let value = String(String.UnicodeScalarView(scalars))
        return value.isEmpty ? "provider_error" : value
    }

    private enum TokenEndpointAuthentication {
        case none
        case clientSecretBasic(String)
        case clientSecretPost(String)
    }

    private struct DiscoveryPayload: Decodable {
        let issuer: String
        let tokenEndpoint: String
        let tokenEndpointAuthMethodsSupported: [String]?

        enum CodingKeys: String, CodingKey {
            case issuer
            case tokenEndpoint = "token_endpoint"
            case tokenEndpointAuthMethodsSupported = "token_endpoint_auth_methods_supported"
        }
    }

    private struct TokenPayload: Decodable {
        let idToken: String?
        let refreshToken: String?
        let error: String?

        enum CodingKeys: String, CodingKey {
            case idToken = "id_token"
            case refreshToken = "refresh_token"
            case error
        }
    }
}

// MARK: - In-memory singleflight session

/// Holds short-lived OIDC state in memory and coalesces concurrent refreshes.
/// Persist refresh-token updates using `consumeRefreshTokenUpdate()` and a Keychain-backed store.
public actor OIDCNativeCredentialSession {
    private let configuration: OIDCAuthProviderConfiguration
    private let client: OIDCNativeAuthClient
    private let clock: any OIDCClock
    private var credential: OIDCNativeCredential?
    private var refreshToken: String?
    private var pendingRefreshTokenUpdate: String?
    private var inFlight: (id: UUID, task: Task<OIDCRefreshResult, Error>)?

    public init(
        configuration: OIDCAuthProviderConfiguration,
        client: OIDCNativeAuthClient,
        clock: any OIDCClock = SystemOIDCClock()
    ) {
        self.configuration = configuration
        self.client = client
        self.clock = clock
        self.refreshToken = configuration.refreshToken
        if let token = configuration.idToken,
           let parsed = try? client.validatedCredential(idToken: token, configuration: configuration) {
            self.credential = parsed
        }
    }

    public func credential(minimumValidity: TimeInterval = 60) async throws -> OIDCNativeCredential {
        let boundedValidity = max(0, min(minimumValidity, 300))
        if let credential, credential.expiration.timeIntervalSince(clock.now()) > boundedValidity {
            return credential
        }
        return try await refresh().credential
    }

    public func forceRefresh() async throws -> OIDCNativeCredential {
        try await refresh().credential
    }

    /// Returns a rotated refresh token once, allowing a later Keychain adapter to persist it atomically.
    public func consumeRefreshTokenUpdate() -> String? {
        defer { pendingRefreshTokenUpdate = nil }
        return pendingRefreshTokenUpdate
    }

    public func invalidateCredential() {
        credential = nil
    }

    private func refresh() async throws -> OIDCRefreshResult {
        if let inFlight {
            return try await inFlight.task.value
        }
        let id = UUID()
        let configuration = self.configuration
        let client = self.client
        let refreshToken = self.refreshToken
        let task = Task.detached(priority: .utility) {
            try await client.refresh(configuration: configuration, refreshToken: refreshToken)
        }
        inFlight = (id, task)
        do {
            let result = try await task.value
            if inFlight?.id == id {
                inFlight = nil
                credential = result.credential
                self.refreshToken = result.refreshToken
                if result.didRotateRefreshToken {
                    pendingRefreshTokenUpdate = result.refreshToken
                }
            }
            return result
        } catch {
            if inFlight?.id == id {
                inFlight = nil
            }
            throw error
        }
    }
}

// MARK: - Safe errors and URL validation

public enum OIDCNativeAuthStage: String, Sendable, Equatable {
    case discovery
    case token
    case request
}

public enum OIDCNativeAuthError: Error, LocalizedError, Sendable, Equatable {
    case invalidConfiguration(field: String)
    case invalidIssuerURL
    case invalidEndpointURL
    case customCertificateAuthorityUnsupported
    case transport(stage: OIDCNativeAuthStage)
    case httpStatus(stage: OIDCNativeAuthStage, statusCode: Int)
    case responseTooLarge(stage: OIDCNativeAuthStage)
    case invalidDiscoveryDocument
    case discoveryIssuerMismatch
    case missingRefreshToken
    case unsupportedTokenEndpointAuthentication
    case invalidTokenResponse
    case tokenResponseMissingIDToken
    case invalidIDToken
    case tokenIssuerMismatch
    case tokenAudienceMismatch
    case tokenMissingExpiration
    case tokenExpired
    case providerRejected(code: String)

    public var errorDescription: String? {
        switch self {
        case .invalidConfiguration(let field):
            return "OIDC configuration is missing or invalid: \(field)."
        case .invalidIssuerURL:
            return "OIDC issuer must be an absolute HTTPS URL without credentials, query, or fragment."
        case .invalidEndpointURL:
            return "OIDC discovery returned an invalid HTTPS endpoint."
        case .customCertificateAuthorityUnsupported:
            return "OIDC configuration requires a custom certificate authority that this transport cannot use."
        case .transport(let stage):
            return "OIDC \(stage.rawValue) request failed."
        case .httpStatus(let stage, let statusCode):
            return "OIDC \(stage.rawValue) request returned HTTP \(statusCode)."
        case .responseTooLarge(let stage):
            return "OIDC \(stage.rawValue) response exceeded the allowed size."
        case .invalidDiscoveryDocument:
            return "OIDC discovery returned an invalid document."
        case .discoveryIssuerMismatch:
            return "OIDC discovery issuer does not match the configured issuer."
        case .missingRefreshToken:
            return "OIDC authentication requires a refresh token."
        case .unsupportedTokenEndpointAuthentication:
            return "OIDC token endpoint does not support this client's authentication method."
        case .invalidTokenResponse:
            return "OIDC token endpoint returned an invalid response."
        case .tokenResponseMissingIDToken:
            return "OIDC token endpoint did not return an ID token."
        case .invalidIDToken:
            return "OIDC provider returned a malformed ID token."
        case .tokenIssuerMismatch:
            return "OIDC ID token issuer does not match the configured issuer."
        case .tokenAudienceMismatch:
            return "OIDC ID token audience does not include the configured client."
        case .tokenMissingExpiration:
            return "OIDC ID token is missing its expiration."
        case .tokenExpired:
            return "OIDC provider returned an expired ID token."
        case .providerRejected(let code):
            return "OIDC provider rejected the refresh request (\(code))."
        }
    }
}

private enum OIDCURLValidator {
    static func validatedIssuer(_ url: URL) throws -> URL {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              components.scheme?.lowercased() == "https",
              components.host?.isEmpty == false,
              components.user == nil,
              components.password == nil,
              components.query == nil,
              components.fragment == nil,
              !containsTraversal(components.percentEncodedPath) else {
            throw OIDCNativeAuthError.invalidIssuerURL
        }
        return url
    }

    static func validatedEndpoint(_ url: URL) throws -> URL {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              components.scheme?.lowercased() == "https",
              components.host?.isEmpty == false,
              components.user == nil,
              components.password == nil,
              components.fragment == nil,
              !containsTraversal(components.percentEncodedPath) else {
            throw OIDCNativeAuthError.invalidEndpointURL
        }
        return url
    }

    static func discoveryURL(for issuer: URL) throws -> URL {
        _ = try validatedIssuer(issuer)
        var components = URLComponents(url: issuer, resolvingAgainstBaseURL: false)
        let basePath = components?.percentEncodedPath ?? ""
        components?.percentEncodedPath = basePath.hasSuffix("/")
            ? "\(basePath).well-known/openid-configuration"
            : "\(basePath)/.well-known/openid-configuration"
        guard let url = components?.url else {
            throw OIDCNativeAuthError.invalidIssuerURL
        }
        return url
    }

    private static func containsTraversal(_ percentEncodedPath: String) -> Bool {
        let decoded = percentEncodedPath.removingPercentEncoding ?? percentEncodedPath
        return decoded.split(separator: "/", omittingEmptySubsequences: false).contains { segment in
            segment == "." || segment == ".."
        }
    }
}
