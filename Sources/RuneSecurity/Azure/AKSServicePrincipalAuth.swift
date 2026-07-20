import Foundation

// MARK: - kubelogin descriptor

public enum AKSAzureEnvironment: String, CaseIterable, Sendable, Equatable {
    case publicCloud = "AzurePublicCloud"
    case chinaCloud = "AzureChinaCloud"
    case usGovernment = "AzureUSGovernment"
    case germanCloud = "AzureGermanCloud"

    public var authorityHost: String {
        switch self {
        case .publicCloud: return "login.microsoftonline.com"
        case .chinaCloud: return "login.chinacloudapi.cn"
        case .usGovernment: return "login.microsoftonline.us"
        case .germanCloud: return "login.microsoftonline.de"
        }
    }
}

/// The non-secret subset of `kubelogin get-token -l spn` required for AKS token acquisition.
public struct AKSKubeloginServicePrincipalDescriptor: Sendable, Equatable {
    public let tenantID: String
    public let serverID: String
    public let clientID: String
    public let environment: AKSAzureEnvironment

    public init(
        tenantID: String,
        serverID: String,
        clientID: String,
        environment: AKSAzureEnvironment = .publicCloud
    ) throws {
        self.tenantID = try AKSServicePrincipalValidation.applicationUUID(
            tenantID,
            field: "tenant-id"
        )
        self.serverID = try AKSServicePrincipalValidation.serverAudience(serverID)
        self.clientID = try AKSServicePrincipalValidation.applicationUUID(
            clientID,
            field: "client-id"
        )
        self.environment = environment
    }

    public var tokenEndpoint: URL {
        // Every user-controlled path component has already been reduced to a canonical UUID.
        URL(string: "https://\(environment.authorityHost)/\(tenantID)/oauth2/v2.0/token")!
    }

    public var scope: String {
        "\(serverID.hasSuffix("/") ? String(serverID.dropLast()) : serverID)/.default"
    }

    /// Returns nil for commands unrelated to kubelogin. Recognized but unsafe kubelogin shapes throw.
    public static func parseIfSupported(
        command: String,
        arguments: [String]
    ) throws -> AKSKubeloginServicePrincipalDescriptor? {
        let executable = URL(fileURLWithPath: command).lastPathComponent.lowercased()
        guard executable == "kubelogin" || executable == "kubelogin.exe" else { return nil }

        let parsed = try AKSKubeloginArguments(arguments)
        guard parsed.operation == "get-token" else { return nil }
        guard parsed.loginMode == "spn" || parsed.loginMode == "serviceprincipal" else {
            throw AKSServicePrincipalAuthError.unsupportedLoginMode(parsed.loginMode)
        }
        guard let tenantID = parsed.value(for: "tenant-id") else {
            throw AKSServicePrincipalAuthError.invalidConfiguration(field: "tenant-id")
        }
        guard let serverID = parsed.value(for: "server-id") else {
            throw AKSServicePrincipalAuthError.invalidConfiguration(field: "server-id")
        }
        guard let clientID = parsed.value(for: "client-id") else {
            throw AKSServicePrincipalAuthError.invalidConfiguration(field: "client-id")
        }
        let environment: AKSAzureEnvironment
        if let rawEnvironment = parsed.value(for: "environment") {
            guard let parsedEnvironment = AKSAzureEnvironment(rawValue: rawEnvironment) else {
                throw AKSServicePrincipalAuthError.unsupportedEnvironment
            }
            environment = parsedEnvironment
        } else {
            environment = .publicCloud
        }
        return try AKSKubeloginServicePrincipalDescriptor(
            tenantID: tenantID,
            serverID: serverID,
            clientID: clientID,
            environment: environment
        )
    }
}

private struct AKSKubeloginArguments {
    private static let valueFlags: [String: String] = [
        "--tenant-id": "tenant-id",
        "--server-id": "server-id",
        "--client-id": "client-id",
        "--environment": "environment",
        "--login": "login",
        "-l": "login"
    ]
    private static let rejectedFlags: Set<String> = [
        "--legacy",
        "--pop-enabled",
        "--pop-claims",
        "--client-secret",
        "--client-certificate",
        "--client-certificate-password",
        "--federated-token-file",
        "--identity-resource-id",
        "--authority-host",
        "--use-azurerm-env-vars",
        "--azure-config-dir",
        "--token-cache-dir"
    ]

    let operation: String
    let loginMode: String
    private let values: [String: String]

    init(_ arguments: [String]) throws {
        guard arguments.count <= 64 else {
            throw AKSServicePrincipalAuthError.invalidCommandShape
        }
        var operation: String?
        var values: [String: String] = [:]
        var index = 0
        while index < arguments.count {
            let raw = arguments[index]
            guard raw.utf8.count <= 4_096 else {
                throw AKSServicePrincipalAuthError.invalidCommandShape
            }
            if !raw.hasPrefix("-") {
                guard operation == nil else {
                    throw AKSServicePrincipalAuthError.invalidCommandShape
                }
                operation = raw.lowercased()
                index += 1
                continue
            }

            let flag: String
            let inlineValue: String?
            if let equals = raw.firstIndex(of: "=") {
                flag = String(raw[..<equals])
                inlineValue = String(raw[raw.index(after: equals)...])
            } else {
                flag = raw
                inlineValue = nil
            }
            if Self.rejectedFlags.contains(flag) {
                if flag == "--legacy" {
                    throw AKSServicePrincipalAuthError.legacyModeUnsupported
                }
                if flag == "--pop-enabled" || flag == "--pop-claims" {
                    throw AKSServicePrincipalAuthError.proofOfPossessionUnsupported
                }
                if flag == "--client-secret" {
                    throw AKSServicePrincipalAuthError.inlineSecretUnsupported
                }
                throw AKSServicePrincipalAuthError.unsupportedOption
            }
            guard let canonical = Self.valueFlags[flag] else {
                throw AKSServicePrincipalAuthError.unsupportedOption
            }
            guard values[canonical] == nil else {
                throw AKSServicePrincipalAuthError.duplicateOption(canonical)
            }
            let value: String
            if let inlineValue {
                value = inlineValue
            } else {
                index += 1
                guard index < arguments.count, !arguments[index].hasPrefix("-") else {
                    throw AKSServicePrincipalAuthError.invalidConfiguration(field: canonical)
                }
                value = arguments[index]
            }
            let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else {
                throw AKSServicePrincipalAuthError.invalidConfiguration(field: canonical)
            }
            values[canonical] = trimmed
            index += 1
        }
        guard let operation else {
            throw AKSServicePrincipalAuthError.invalidCommandShape
        }
        self.operation = operation
        self.values = values
        self.loginMode = values["login"]?.lowercased() ?? "devicecode"
    }

    func value(for key: String) -> String? { values[key] }
}

// MARK: - Credentials and token

public struct AKSServicePrincipalCredentials: Sendable, Equatable, Codable, CustomStringConvertible, CustomDebugStringConvertible {
    public let clientID: String
    public let clientSecret: String

    public init(clientID: String, clientSecret: String) throws {
        self.clientID = try AKSServicePrincipalValidation.applicationUUID(clientID, field: "client-id")
        guard !clientSecret.isEmpty,
              clientSecret.utf8.count <= 4_096,
              clientSecret.rangeOfCharacter(from: .newlines) == nil,
              !clientSecret.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }) else {
            throw AKSServicePrincipalAuthError.invalidCredentials
        }
        self.clientSecret = clientSecret
    }

    public var description: String { "AKSServicePrincipalCredentials(clientID: <redacted>, clientSecret: <redacted>)" }
    public var debugDescription: String { description }

    private enum CodingKeys: String, CodingKey {
        case clientID
        case clientSecret
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        try self.init(
            clientID: container.decode(String.self, forKey: .clientID),
            clientSecret: container.decode(String.self, forKey: .clientSecret)
        )
    }
}

public struct AKSServicePrincipalAccessToken: Sendable, Equatable, CustomStringConvertible, CustomDebugStringConvertible {
    public let value: String
    public let expiration: Date

    public init(value: String, expiration: Date) throws {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, trimmed.utf8.count <= 256 * 1_024 else {
            throw AKSServicePrincipalAuthError.invalidTokenResponse
        }
        self.value = trimmed
        self.expiration = expiration
    }

    public var description: String { "AKSServicePrincipalAccessToken(value: <redacted>, expiration: \(expiration))" }
    public var debugDescription: String { description }
}

// MARK: - Injectable HTTPS transport

public struct AKSServicePrincipalTokenHTTPRequest: Sendable, Equatable {
    public let url: URL
    public let headers: [String: String]
    public let body: Data
    public let timeout: TimeInterval

    public init(url: URL, headers: [String: String], body: Data, timeout: TimeInterval = 15) {
        self.url = url
        self.headers = headers
        self.body = body
        self.timeout = timeout
    }
}

public struct AKSServicePrincipalTokenHTTPResponse: Sendable, Equatable {
    public let statusCode: Int
    public let body: Data

    public init(statusCode: Int, body: Data) {
        self.statusCode = statusCode
        self.body = body
    }
}

public protocol AKSServicePrincipalTokenHTTPClient: Sendable {
    func send(_ request: AKSServicePrincipalTokenHTTPRequest) async throws -> AKSServicePrincipalTokenHTTPResponse
}

public final class URLSessionAKSServicePrincipalTokenHTTPClient: AKSServicePrincipalTokenHTTPClient, @unchecked Sendable {
    private let session: URLSession
    private let maximumResponseBytes: Int

    public convenience init(maximumResponseBytes: Int = 1_048_576) {
        self.init(
            maximumResponseBytes: maximumResponseBytes,
            sessionConfiguration: .ephemeral
        )
    }

    /// Test-only transport seam. The request URL remains validated as HTTPS by
    /// `send(_:)`; production callers use the public initializer above.
    init(
        maximumResponseBytes: Int = 1_048_576,
        sessionConfiguration: URLSessionConfiguration
    ) {
        let configuration = sessionConfiguration
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.timeoutIntervalForRequest = 15
        configuration.timeoutIntervalForResource = 30
        let delegate = AKSNoRedirectSessionDelegate()
        self.session = URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
        self.maximumResponseBytes = max(1_024, min(maximumResponseBytes, 4_194_304))
    }

    public func send(_ request: AKSServicePrincipalTokenHTTPRequest) async throws -> AKSServicePrincipalTokenHTTPResponse {
        guard request.url.scheme?.lowercased() == "https",
              request.url.host?.isEmpty == false,
              request.url.user == nil,
              request.url.password == nil,
              request.url.fragment == nil else {
            throw AKSServicePrincipalAuthError.invalidTokenEndpoint
        }
        var urlRequest = URLRequest(url: request.url)
        urlRequest.httpMethod = "POST"
        urlRequest.httpBody = request.body
        urlRequest.timeoutInterval = request.timeout
        for (name, value) in request.headers {
            urlRequest.setValue(value, forHTTPHeaderField: name)
        }
        let (bytes, response) = try await session.bytes(for: urlRequest)
        guard let http = response as? HTTPURLResponse else {
            throw AKSServicePrincipalAuthError.transport
        }
        var body = Data()
        body.reserveCapacity(min(maximumResponseBytes, 64 * 1_024))
        for try await byte in bytes {
            guard body.count < maximumResponseBytes else {
                throw AKSServicePrincipalAuthError.responseTooLarge
            }
            body.append(byte)
        }
        return AKSServicePrincipalTokenHTTPResponse(statusCode: http.statusCode, body: body)
    }
}

private final class AKSNoRedirectSessionDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
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

// MARK: - Token service and cache

public protocol AKSServicePrincipalClock: Sendable {
    func now() -> Date
}

public struct SystemAKSServicePrincipalClock: AKSServicePrincipalClock {
    public init() {}
    public func now() -> Date { Date() }
}

public struct AKSServicePrincipalTokenService: Sendable {
    private let httpClient: any AKSServicePrincipalTokenHTTPClient
    private let clock: any AKSServicePrincipalClock
    private let maximumResponseBytes: Int

    public init(
        httpClient: any AKSServicePrincipalTokenHTTPClient = URLSessionAKSServicePrincipalTokenHTTPClient(),
        clock: any AKSServicePrincipalClock = SystemAKSServicePrincipalClock(),
        maximumResponseBytes: Int = 1_048_576
    ) {
        self.httpClient = httpClient
        self.clock = clock
        self.maximumResponseBytes = max(1_024, min(maximumResponseBytes, 4_194_304))
    }

    public func token(
        descriptor: AKSKubeloginServicePrincipalDescriptor,
        credentials: AKSServicePrincipalCredentials
    ) async throws -> AKSServicePrincipalAccessToken {
        guard credentials.clientID == descriptor.clientID else {
            throw AKSServicePrincipalAuthError.clientIDMismatch
        }
        let fields = [
            ("client_id", credentials.clientID),
            ("client_secret", credentials.clientSecret),
            ("grant_type", "client_credentials"),
            ("scope", descriptor.scope)
        ]
        let body = Data(Self.formBody(fields).utf8)
        let response: AKSServicePrincipalTokenHTTPResponse
        do {
            response = try await httpClient.send(AKSServicePrincipalTokenHTTPRequest(
                url: descriptor.tokenEndpoint,
                headers: [
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded"
                ],
                body: body
            ))
        } catch let error as AKSServicePrincipalAuthError {
            throw error
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            // Never expose transport descriptions: they may contain request bodies or account identifiers.
            throw AKSServicePrincipalAuthError.transport
        }
        guard response.body.count <= maximumResponseBytes else {
            throw AKSServicePrincipalAuthError.responseTooLarge
        }
        let payload = try? JSONDecoder().decode(TokenPayload.self, from: response.body)
        if !(200..<300).contains(response.statusCode) {
            if let code = payload?.error {
                throw AKSServicePrincipalAuthError.providerRejected(code: Self.sanitizedProviderCode(code))
            }
            throw AKSServicePrincipalAuthError.httpStatus(response.statusCode)
        }
        guard let payload,
              payload.error == nil,
              let accessToken = Self.nonEmpty(payload.accessToken),
              let expiresIn = payload.expiresIn?.seconds,
              expiresIn >= 60,
              expiresIn <= 86_400 else {
            if let code = payload?.error {
                throw AKSServicePrincipalAuthError.providerRejected(code: Self.sanitizedProviderCode(code))
            }
            throw AKSServicePrincipalAuthError.invalidTokenResponse
        }
        if let tokenType = Self.nonEmpty(payload.tokenType), tokenType.lowercased() != "bearer" {
            throw AKSServicePrincipalAuthError.invalidTokenResponse
        }
        return try AKSServicePrincipalAccessToken(
            value: accessToken,
            expiration: clock.now().addingTimeInterval(TimeInterval(expiresIn))
        )
    }

    private static func formBody(_ fields: [(String, String)]) -> String {
        fields.map { "\(formEncoded($0.0))=\(formEncoded($0.1))" }.joined(separator: "&")
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

    private struct TokenPayload: Decodable {
        let accessToken: String?
        let tokenType: String?
        let expiresIn: FlexibleSeconds?
        let error: String?

        enum CodingKeys: String, CodingKey {
            case accessToken = "access_token"
            case tokenType = "token_type"
            case expiresIn = "expires_in"
            case error
        }
    }

    private enum FlexibleSeconds: Decodable {
        case value(Int)

        var seconds: Int {
            switch self { case .value(let value): return value }
        }

        init(from decoder: Decoder) throws {
            let container = try decoder.singleValueContainer()
            if let value = try? container.decode(Int.self) {
                self = .value(value)
                return
            }
            if let string = try? container.decode(String.self), let value = Int(string) {
                self = .value(value)
                return
            }
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Invalid expires_in")
        }
    }
}

/// In-memory per-binding token cache with singleflight refresh behavior.
public actor AKSServicePrincipalCredentialSession {
    private let descriptor: AKSKubeloginServicePrincipalDescriptor
    private let credentials: AKSServicePrincipalCredentials
    private let service: AKSServicePrincipalTokenService
    private let clock: any AKSServicePrincipalClock
    private var cached: AKSServicePrincipalAccessToken?
    private var inFlight: (id: UUID, task: Task<AKSServicePrincipalAccessToken, Error>)?

    public init(
        descriptor: AKSKubeloginServicePrincipalDescriptor,
        credentials: AKSServicePrincipalCredentials,
        service: AKSServicePrincipalTokenService,
        clock: any AKSServicePrincipalClock = SystemAKSServicePrincipalClock()
    ) {
        self.descriptor = descriptor
        self.credentials = credentials
        self.service = service
        self.clock = clock
    }

    public func token(minimumValidity: TimeInterval = 60) async throws -> AKSServicePrincipalAccessToken {
        let boundedValidity = max(0, min(minimumValidity, 300))
        if let cached, cached.expiration.timeIntervalSince(clock.now()) > boundedValidity {
            return cached
        }
        return try await refresh()
    }

    public func forceRefresh() async throws -> AKSServicePrincipalAccessToken {
        try await refresh()
    }

    public func invalidate() {
        cached = nil
    }

    private func refresh() async throws -> AKSServicePrincipalAccessToken {
        if let inFlight {
            return try await inFlight.task.value
        }
        let id = UUID()
        let descriptor = self.descriptor
        let credentials = self.credentials
        let service = self.service
        let task = Task.detached(priority: .utility) {
            try await service.token(descriptor: descriptor, credentials: credentials)
        }
        inFlight = (id, task)
        do {
            let token = try await task.value
            if inFlight?.id == id {
                inFlight = nil
                cached = token
            }
            return token
        } catch {
            if inFlight?.id == id { inFlight = nil }
            throw error
        }
    }
}

// MARK: - Validation and safe errors

private enum AKSServicePrincipalValidation {
    static func applicationUUID(_ raw: String, field: String) throws -> String {
        let value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let uuid = UUID(uuidString: value) else {
            throw AKSServicePrincipalAuthError.invalidConfiguration(field: field)
        }
        return uuid.uuidString.lowercased()
    }

    static func serverAudience(_ raw: String) throws -> String {
        let value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !value.isEmpty,
              value.utf8.count <= 512,
              !value.hasPrefix("spn:"),
              value.rangeOfCharacter(from: .whitespacesAndNewlines) == nil,
              !value.contains("?"),
              !value.contains("#"),
              !value.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }) else {
            throw AKSServicePrincipalAuthError.invalidConfiguration(field: "server-id")
        }
        if let uuid = UUID(uuidString: value) {
            return uuid.uuidString.lowercased()
        }
        if value.lowercased().hasPrefix("api://"),
           let components = URLComponents(string: value),
           components.scheme?.lowercased() == "api",
           components.host?.isEmpty == false,
           components.user == nil,
           components.password == nil,
           components.query == nil,
           components.fragment == nil {
            return value
        }
        throw AKSServicePrincipalAuthError.invalidConfiguration(field: "server-id")
    }
}

public enum AKSServicePrincipalAuthError: Error, LocalizedError, Sendable, Equatable {
    case invalidCommandShape
    case invalidConfiguration(field: String)
    case duplicateOption(String)
    case unsupportedOption
    case unsupportedLoginMode(String)
    case unsupportedEnvironment
    case legacyModeUnsupported
    case proofOfPossessionUnsupported
    case inlineSecretUnsupported
    case invalidCredentials
    case clientIDMismatch
    case invalidTokenEndpoint
    case transport
    case httpStatus(Int)
    case responseTooLarge
    case invalidTokenResponse
    case providerRejected(code: String)

    public var errorDescription: String? {
        switch self {
        case .invalidCommandShape:
            return "Azure kubelogin command has an invalid service-principal shape."
        case .invalidConfiguration(let field):
            return "Azure service-principal configuration is missing or invalid: \(field)."
        case .duplicateOption(let option):
            return "Azure kubelogin command contains duplicate option: \(option)."
        case .unsupportedOption:
            return "Azure kubelogin command uses an option that native service-principal authentication does not support."
        case .unsupportedLoginMode:
            return "Azure kubelogin mode is not service-principal authentication."
        case .unsupportedEnvironment:
            return "Azure kubelogin environment is not supported."
        case .legacyModeUnsupported:
            return "Azure legacy token audiences are not supported by native service-principal authentication."
        case .proofOfPossessionUnsupported:
            return "Azure Proof-of-Possession tokens are not supported by this bearer-token flow."
        case .inlineSecretUnsupported:
            return "Azure client secrets must be supplied through Rune's secret store, not kubelogin arguments."
        case .invalidCredentials:
            return "Azure service-principal credentials are invalid."
        case .clientIDMismatch:
            return "Azure service-principal credentials do not match the kubeconfig client ID."
        case .invalidTokenEndpoint:
            return "Azure service-principal token endpoint is invalid."
        case .transport:
            return "Azure service-principal token request failed."
        case .httpStatus(let status):
            return "Azure service-principal token request returned HTTP \(status)."
        case .responseTooLarge:
            return "Azure service-principal token response exceeded the allowed size."
        case .invalidTokenResponse:
            return "Azure service-principal token endpoint returned an invalid response."
        case .providerRejected(let code):
            return "Azure service-principal token request was rejected (\(code))."
        }
    }
}
