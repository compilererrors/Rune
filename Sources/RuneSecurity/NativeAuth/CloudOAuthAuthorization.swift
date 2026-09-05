import CryptoKit
import Foundation
@preconcurrency import Security

public struct CloudOAuthSheetID: RawRepresentable, Codable, Hashable, Sendable {
    public let rawValue: UUID

    public init(rawValue: UUID = UUID()) {
        self.rawValue = rawValue
    }
}

public struct CloudOAuthAttemptID: RawRepresentable, Codable, Hashable, Sendable {
    public let rawValue: UUID

    public init(rawValue: UUID = UUID()) {
        self.rawValue = rawValue
    }
}

public struct CloudOAuthAuthorizationConfiguration: Sendable, Equatable {
    public let provider: CloudAccountProvider
    public let authorizationEndpoint: URL
    public let clientID: String
    public let redirectURI: URL
    public let scopes: [String]
    public let allowedAuthorizationHosts: Set<String>
    public let allowedRedirectSchemes: Set<String>
    public let allowedRedirectHosts: Set<String>
    public let expectedIssuer: String
    public let expectedAudience: String

    public init(
        provider: CloudAccountProvider,
        authorizationEndpoint: URL,
        clientID: String,
        redirectURI: URL,
        scopes: [String],
        allowedAuthorizationHosts: Set<String>,
        allowedRedirectSchemes: Set<String> = [],
        allowedRedirectHosts: Set<String> = [],
        expectedIssuer: String,
        expectedAudience: String
    ) throws {
        let normalizedHosts = Set(allowedAuthorizationHosts.map {
            $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        }.filter { !$0.isEmpty })
        let normalizedSchemes = Set(allowedRedirectSchemes.map {
            $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        }.filter { !$0.isEmpty })
        let normalizedRedirectHosts = Set(allowedRedirectHosts.map {
            $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        }.filter { !$0.isEmpty })
        let normalizedClientID = clientID.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedIssuer = expectedIssuer.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedAudience = expectedAudience.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedClientID.isEmpty,
              normalizedClientID.utf8.count <= 1_024,
              !normalizedIssuer.isEmpty,
              normalizedIssuer.utf8.count <= 2_048,
              !normalizedAudience.isEmpty,
              normalizedAudience.utf8.count <= 1_024,
              let issuerURL = URL(string: normalizedIssuer),
              issuerURL.scheme?.lowercased() == "https",
              issuerURL.host != nil,
              issuerURL.user == nil,
              issuerURL.password == nil,
              issuerURL.query == nil,
              issuerURL.fragment == nil else {
            throw CloudOAuthAuthorizationError.invalidConfiguration
        }
        let validatedEndpoint = try Self.validatedAuthorizationEndpoint(
            authorizationEndpoint,
            allowedHosts: normalizedHosts
        )
        let validatedRedirect = try Self.validatedRedirectURI(
            redirectURI,
            allowedSchemes: normalizedSchemes,
            allowedHosts: normalizedRedirectHosts
        )
        let normalizedScopes = try Self.normalizedScopes(scopes)

        self.provider = provider
        self.authorizationEndpoint = validatedEndpoint
        self.clientID = normalizedClientID
        self.redirectURI = validatedRedirect
        self.scopes = normalizedScopes
        self.allowedAuthorizationHosts = normalizedHosts
        self.allowedRedirectSchemes = normalizedSchemes
        self.allowedRedirectHosts = normalizedRedirectHosts
        self.expectedIssuer = normalizedIssuer
        self.expectedAudience = normalizedAudience
    }

    private static func validatedAuthorizationEndpoint(
        _ url: URL,
        allowedHosts: Set<String>
    ) throws -> URL {
        guard url.scheme?.lowercased() == "https",
              let host = url.host?.lowercased(),
              allowedHosts.contains(host),
              url.user == nil,
              url.password == nil,
              url.query == nil,
              url.fragment == nil else {
            throw CloudOAuthAuthorizationError.invalidAuthorizationEndpoint
        }
        return url
    }

    private static func validatedRedirectURI(
        _ url: URL,
        allowedSchemes: Set<String>,
        allowedHosts: Set<String>
    ) throws -> URL {
        guard let scheme = url.scheme?.lowercased(),
              url.user == nil,
              url.password == nil,
              url.query == nil,
              url.fragment == nil else {
            throw CloudOAuthAuthorizationError.invalidRedirectURI
        }
        if scheme == "http" {
            let host = url.host?.lowercased()
            guard host == "127.0.0.1" || host == "localhost" || host == "::1" else {
                throw CloudOAuthAuthorizationError.invalidRedirectURI
            }
            return url
        }
        if scheme == "https" {
            guard let host = url.host?.lowercased(), allowedHosts.contains(host) else {
                throw CloudOAuthAuthorizationError.invalidRedirectURI
            }
            return url
        }
        let forbiddenSchemes: Set<String> = ["data", "file", "javascript"]
        guard allowedSchemes.contains(scheme), !forbiddenSchemes.contains(scheme) else {
            throw CloudOAuthAuthorizationError.invalidRedirectURI
        }
        return url
    }

    private static func normalizedScopes(_ scopes: [String]) throws -> [String] {
        var seen = Set<String>()
        let result = scopes.compactMap { raw -> String? in
            let scope = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !scope.isEmpty, scope.utf8.count <= 512, seen.insert(scope).inserted else {
                return nil
            }
            return scope
        }
        guard !result.isEmpty,
              result.count <= 32,
              result.reduce(0, { $0 + $1.utf8.count }) <= 4_096 else {
            throw CloudOAuthAuthorizationError.invalidConfiguration
        }
        return result
    }
}

public struct CloudOAuthAuthorizationAttempt: Sendable, Equatable {
    public let id: CloudOAuthAttemptID
    public let sheetID: CloudOAuthSheetID
    public let provider: CloudAccountProvider
    public let generation: CloudAccountGeneration
    public let authorizationURL: URL
    public let callbackScheme: String
    public let expiresAt: Date

    public init(
        id: CloudOAuthAttemptID,
        sheetID: CloudOAuthSheetID,
        provider: CloudAccountProvider,
        generation: CloudAccountGeneration,
        authorizationURL: URL,
        callbackScheme: String,
        expiresAt: Date
    ) {
        self.id = id
        self.sheetID = sheetID
        self.provider = provider
        self.generation = generation
        self.authorizationURL = authorizationURL
        self.callbackScheme = callbackScheme
        self.expiresAt = expiresAt
    }
}

/// One-time secret material for the token exchange. It is intentionally not Codable and its
/// description never includes the authorization code, PKCE verifier, or nonce.
public struct CloudOAuthCodeExchange: Sendable, CustomStringConvertible {
    public let attemptID: CloudOAuthAttemptID
    public let provider: CloudAccountProvider
    public let generation: CloudAccountGeneration
    public let authorizationCode: String
    public let codeVerifier: String
    public let nonce: String
    public let redirectURI: URL
    public let expectedIssuer: String
    public let expectedAudience: String

    public var description: String {
        "CloudOAuthCodeExchange(provider: \(provider.rawValue), secrets: <redacted>)"
    }
}

public enum CloudOAuthAuthorizationError: Error, LocalizedError, Equatable, Sendable {
    case invalidConfiguration
    case invalidAuthorizationEndpoint
    case invalidRedirectURI
    case secureRandomUnavailable
    case attemptNotFound
    case superseded
    case expired
    case invalidCallback
    case stateMismatch
    case providerDenied

    public var errorDescription: String? {
        switch self {
        case .invalidConfiguration:
            return "The native account authorization configuration is invalid."
        case .invalidAuthorizationEndpoint:
            return "The account authorization endpoint is not an allowed HTTPS origin."
        case .invalidRedirectURI:
            return "The account authorization callback URI is not allowed."
        case .secureRandomUnavailable:
            return "Rune could not create secure account authorization state."
        case .attemptNotFound:
            return "The account authorization attempt is no longer active."
        case .superseded:
            return "A newer account authorization attempt replaced this one."
        case .expired:
            return "The account authorization attempt expired."
        case .invalidCallback:
            return "Rune rejected an invalid account authorization callback."
        case .stateMismatch:
            return "Rune rejected an account authorization callback with invalid state."
        case .providerDenied:
            return "The cloud provider did not authorize the account connection."
        }
    }
}

public actor CloudOAuthAuthorizationCoordinator {
    public typealias RandomBytes = @Sendable (_ count: Int) throws -> Data

    private struct ActiveAttempt {
        let publicValue: CloudOAuthAuthorizationAttempt
        let redirectURI: URL
        let expectedIssuer: String
        let expectedAudience: String
        let state: String
        let nonce: String
        let codeVerifier: String
    }

    private let now: @Sendable () -> Date
    private let randomBytes: RandomBytes
    private let lifetime: TimeInterval
    private var activeAttempts: [CloudOAuthSheetID: ActiveAttempt] = [:]
    private var expirationTasks: [CloudOAuthSheetID: Task<Void, Never>] = [:]
    private var generations: [CloudOAuthSheetID: UInt64] = [:]

    public init(
        lifetime: TimeInterval = 600,
        now: @escaping @Sendable () -> Date = Date.init,
        randomBytes: @escaping RandomBytes = CloudOAuthAuthorizationCoordinator.secureRandomBytes
    ) {
        if lifetime.isFinite {
            self.lifetime = max(30, min(lifetime, 900))
        } else {
            self.lifetime = 600
        }
        self.now = now
        self.randomBytes = randomBytes
    }

    public func begin(
        sheetID: CloudOAuthSheetID,
        configuration: CloudOAuthAuthorizationConfiguration
    ) throws -> CloudOAuthAuthorizationAttempt {
        activeAttempts.removeValue(forKey: sheetID)
        expirationTasks.removeValue(forKey: sheetID)?.cancel()
        let generation = nextGeneration(for: sheetID)
        let attemptID = CloudOAuthAttemptID()
        let codeVerifier = try randomURLSafeValue(byteCount: 32)
        let state = try randomURLSafeValue(byteCount: 32)
        let nonce = try randomURLSafeValue(byteCount: 32)
        let challenge = Self.base64URL(Data(SHA256.hash(data: Data(codeVerifier.utf8))))
        let authorizationURL = try Self.authorizationURL(
            configuration: configuration,
            state: state,
            nonce: nonce,
            challenge: challenge
        )
        let expiresAt = now().addingTimeInterval(lifetime)
        let publicValue = CloudOAuthAuthorizationAttempt(
            id: attemptID,
            sheetID: sheetID,
            provider: configuration.provider,
            generation: generation,
            authorizationURL: authorizationURL,
            callbackScheme: configuration.redirectURI.scheme?.lowercased() ?? "",
            expiresAt: expiresAt
        )
        activeAttempts[sheetID] = ActiveAttempt(
            publicValue: publicValue,
            redirectURI: configuration.redirectURI,
            expectedIssuer: configuration.expectedIssuer,
            expectedAudience: configuration.expectedAudience,
            state: state,
            nonce: nonce,
            codeVerifier: codeVerifier
        )
        let expirationDelay = lifetime
        expirationTasks[sheetID] = Task { [weak self] in
            let nanoseconds = UInt64(expirationDelay * 1_000_000_000)
            try? await Task<Never, Never>.sleep(nanoseconds: nanoseconds)
            guard !Task.isCancelled else { return }
            await self?.expire(sheetID: sheetID, attemptID: attemptID, generation: generation)
        }
        return publicValue
    }

    public func cancel(sheetID: CloudOAuthSheetID) {
        _ = nextGeneration(for: sheetID)
        activeAttempts.removeValue(forKey: sheetID)
        expirationTasks.removeValue(forKey: sheetID)?.cancel()
    }

    public func consumeCallback(
        _ callbackURL: URL,
        sheetID: CloudOAuthSheetID,
        attemptID: CloudOAuthAttemptID,
        generation: CloudAccountGeneration
    ) throws -> CloudOAuthCodeExchange {
        guard let attempt = activeAttempts[sheetID] else {
            throw CloudOAuthAuthorizationError.attemptNotFound
        }
        guard attempt.publicValue.id == attemptID,
              attempt.publicValue.generation == generation,
              generations[sheetID] == generation.rawValue else {
            throw CloudOAuthAuthorizationError.superseded
        }
        activeAttempts.removeValue(forKey: sheetID)
        expirationTasks.removeValue(forKey: sheetID)?.cancel()
        guard now() < attempt.publicValue.expiresAt else {
            throw CloudOAuthAuthorizationError.expired
        }
        guard Self.matchesCallbackBase(callbackURL, expected: attempt.redirectURI),
              let components = URLComponents(url: callbackURL, resolvingAgainstBaseURL: false),
              components.fragment == nil else {
            throw CloudOAuthAuthorizationError.invalidCallback
        }
        let values = try Self.uniqueQueryValues(components.queryItems ?? [])
        guard let suppliedState = values["state"],
              Self.constantTimeEquals(suppliedState, attempt.state) else {
            throw CloudOAuthAuthorizationError.stateMismatch
        }
        if values["error"] != nil {
            throw CloudOAuthAuthorizationError.providerDenied
        }
        if let callbackIssuer = values["iss"], callbackIssuer != attempt.expectedIssuer {
            throw CloudOAuthAuthorizationError.invalidCallback
        }
        guard let code = values["code"]?.trimmingCharacters(in: .whitespacesAndNewlines),
              !code.isEmpty,
              code.utf8.count <= 16_384 else {
            throw CloudOAuthAuthorizationError.invalidCallback
        }
        return CloudOAuthCodeExchange(
            attemptID: attemptID,
            provider: attempt.publicValue.provider,
            generation: generation,
            authorizationCode: code,
            codeVerifier: attempt.codeVerifier,
            nonce: attempt.nonce,
            redirectURI: attempt.redirectURI,
            expectedIssuer: attempt.expectedIssuer,
            expectedAudience: attempt.expectedAudience
        )
    }

    public func expireAttempts() {
        let currentDate = now()
        let expiredSheetIDs = activeAttempts.compactMap { sheetID, attempt in
            attempt.publicValue.expiresAt <= currentDate ? sheetID : nil
        }
        for sheetID in expiredSheetIDs {
            activeAttempts.removeValue(forKey: sheetID)
            expirationTasks.removeValue(forKey: sheetID)?.cancel()
        }
    }

    public func hasActiveAttempt(sheetID: CloudOAuthSheetID) -> Bool {
        guard let attempt = activeAttempts[sheetID] else { return false }
        guard attempt.publicValue.expiresAt > now() else {
            activeAttempts.removeValue(forKey: sheetID)
            expirationTasks.removeValue(forKey: sheetID)?.cancel()
            return false
        }
        return true
    }

    private func nextGeneration(for sheetID: CloudOAuthSheetID) -> CloudAccountGeneration {
        let generation = generations[sheetID, default: 0] &+ 1
        generations[sheetID] = generation
        return CloudAccountGeneration(rawValue: generation)
    }

    private func randomURLSafeValue(byteCount: Int) throws -> String {
        let data = try randomBytes(byteCount)
        guard data.count == byteCount else {
            throw CloudOAuthAuthorizationError.secureRandomUnavailable
        }
        return Self.base64URL(data)
    }

    private func expire(
        sheetID: CloudOAuthSheetID,
        attemptID: CloudOAuthAttemptID,
        generation: CloudAccountGeneration
    ) {
        guard let attempt = activeAttempts[sheetID],
              attempt.publicValue.id == attemptID,
              attempt.publicValue.generation == generation else {
            return
        }
        activeAttempts.removeValue(forKey: sheetID)
        expirationTasks.removeValue(forKey: sheetID)
    }

    public static func secureRandomBytes(count: Int) throws -> Data {
        guard count > 0, count <= 128 else {
            throw CloudOAuthAuthorizationError.secureRandomUnavailable
        }
        var data = Data(count: count)
        let status = data.withUnsafeMutableBytes { buffer -> Int32 in
            guard let baseAddress = buffer.baseAddress else { return errSecParam }
            return SecRandomCopyBytes(kSecRandomDefault, count, baseAddress)
        }
        guard status == errSecSuccess else {
            throw CloudOAuthAuthorizationError.secureRandomUnavailable
        }
        return data
    }
}

private extension CloudOAuthAuthorizationCoordinator {
    static func authorizationURL(
        configuration: CloudOAuthAuthorizationConfiguration,
        state: String,
        nonce: String,
        challenge: String
    ) throws -> URL {
        guard var components = URLComponents(
            url: configuration.authorizationEndpoint,
            resolvingAgainstBaseURL: false
        ) else {
            throw CloudOAuthAuthorizationError.invalidAuthorizationEndpoint
        }
        components.queryItems = [
            URLQueryItem(name: "response_type", value: "code"),
            URLQueryItem(name: "client_id", value: configuration.clientID),
            URLQueryItem(name: "redirect_uri", value: configuration.redirectURI.absoluteString),
            URLQueryItem(name: "scope", value: configuration.scopes.joined(separator: " ")),
            URLQueryItem(name: "state", value: state),
            URLQueryItem(name: "nonce", value: nonce),
            URLQueryItem(name: "code_challenge", value: challenge),
            URLQueryItem(name: "code_challenge_method", value: "S256")
        ]
        guard let url = components.url, url.absoluteString.utf8.count <= 16_384 else {
            throw CloudOAuthAuthorizationError.invalidConfiguration
        }
        return url
    }

    static func matchesCallbackBase(_ callback: URL, expected: URL) -> Bool {
        guard let callbackComponents = URLComponents(url: callback, resolvingAgainstBaseURL: false),
              let expectedComponents = URLComponents(url: expected, resolvingAgainstBaseURL: false) else {
            return false
        }
        return callbackComponents.scheme?.lowercased() == expectedComponents.scheme?.lowercased()
            && callbackComponents.host?.lowercased() == expectedComponents.host?.lowercased()
            && callbackComponents.port == expectedComponents.port
            && callbackComponents.path == expectedComponents.path
            && callbackComponents.user == nil
            && callbackComponents.password == nil
    }

    static func uniqueQueryValues(_ items: [URLQueryItem]) throws -> [String: String] {
        guard items.count <= 16 else {
            throw CloudOAuthAuthorizationError.invalidCallback
        }
        var values: [String: String] = [:]
        for item in items {
            let allowedNames: Set<String> = [
                "code", "state", "error", "error_description", "error_uri",
                "session_state", "scope", "authuser", "prompt", "iss"
            ]
            guard allowedNames.contains(item.name),
                  values[item.name] == nil,
                  let value = item.value,
                  value.utf8.count <= 16_384 else {
                throw CloudOAuthAuthorizationError.invalidCallback
            }
            values[item.name] = value
        }
        return values
    }

    static func constantTimeEquals(_ lhs: String, _ rhs: String) -> Bool {
        let left = Array(lhs.utf8)
        let right = Array(rhs.utf8)
        let count = max(left.count, right.count)
        var difference = left.count ^ right.count
        for index in 0..<count {
            let leftByte = index < left.count ? left[index] : 0
            let rightByte = index < right.count ? right[index] : 0
            difference |= Int(leftByte ^ rightByte)
        }
        return difference == 0
    }

    static func base64URL(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}
