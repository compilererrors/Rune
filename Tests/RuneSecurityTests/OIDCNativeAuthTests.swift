import Foundation
import XCTest
@testable import RuneSecurity

final class OIDCNativeAuthTests: XCTestCase {
    private let issuer = URL(string: "https://identity.example.invalid/tenant")!
    private let tokenEndpoint = URL(string: "https://tokens.example.invalid/oauth/token")!
    private let now = Date(timeIntervalSince1970: 1_700_000_000)

    func testKubeconfigAuthProviderConfigurationIsNormalized() throws {
        let configuration = try OIDCAuthProviderConfiguration(
            authProviderName: " OIDC ",
            config: [
                "idp-issuer-url": "https://identity.example.invalid/tenant",
                "client-id": " synthetic-client ",
                "client-secret": " synthetic-secret ",
                "id-token": " synthetic-id-token ",
                "refresh-token": " synthetic-refresh-token ",
                "idp-certificate-authority": " synthetic-ca.pem ",
                "extra-scopes": " groups, email,groups "
            ]
        )

        XCTAssertEqual(configuration.issuerURL, issuer)
        XCTAssertEqual(configuration.clientID, "synthetic-client")
        XCTAssertEqual(configuration.clientSecret, "synthetic-secret")
        XCTAssertEqual(configuration.idToken, "synthetic-id-token")
        XCTAssertEqual(configuration.refreshToken, "synthetic-refresh-token")
        XCTAssertEqual(configuration.certificateAuthorityPath, "synthetic-ca.pem")
        XCTAssertEqual(configuration.extraScopes, ["groups", "email"])
    }

    func testConfigurationRejectsNonHTTPSAndURLCredentials() {
        XCTAssertThrowsError(try OIDCAuthProviderConfiguration(
            issuerURL: URL(string: "http://identity.example.invalid")!,
            clientID: "synthetic-client"
        )) { error in
            XCTAssertEqual(error as? OIDCNativeAuthError, .invalidIssuerURL)
        }
        XCTAssertThrowsError(try OIDCAuthProviderConfiguration(
            issuerURL: URL(string: "https://user:password@identity.example.invalid")!,
            clientID: "synthetic-client"
        )) { error in
            XCTAssertEqual(error as? OIDCNativeAuthError, .invalidIssuerURL)
        }
    }

    func testJWTMetadataParsesExpiryIssuerAndMultipleAudiences() throws {
        let token = try jwt(
            issuer: issuer.absoluteString,
            audiences: ["synthetic-client", "secondary-client"],
            expiration: now.addingTimeInterval(600)
        )

        let metadata = try OIDCJWTMetadata.parse(token)

        XCTAssertEqual(metadata.issuer, issuer.absoluteString)
        XCTAssertEqual(metadata.audiences, ["synthetic-client", "secondary-client"])
        XCTAssertEqual(metadata.expiration, now.addingTimeInterval(600))
    }

    func testJWTMetadataRejectsMalformedAndBooleanExpiry() throws {
        XCTAssertThrowsError(try OIDCJWTMetadata.parse("not-a-jwt")) { error in
            XCTAssertEqual(error as? OIDCNativeAuthError, .invalidIDToken)
        }
        let token = try jwt(payload: [
            "iss": issuer.absoluteString,
            "aud": "synthetic-client",
            "exp": true
        ])
        XCTAssertThrowsError(try OIDCJWTMetadata.parse(token)) { error in
            XCTAssertEqual(error as? OIDCNativeAuthError, .invalidIDToken)
        }
    }

    func testDiscoveryUsesIssuerPathAndRequiresExactIssuer() async throws {
        let http = ScriptedOIDCHTTPClient { [issuer, tokenEndpoint] request, _ in
            XCTAssertEqual(
                request.url.absoluteString,
                "https://identity.example.invalid/tenant/.well-known/openid-configuration"
            )
            return Self.jsonResponse([
                "issuer": issuer.absoluteString,
                "token_endpoint": tokenEndpoint.absoluteString,
                "token_endpoint_auth_methods_supported": ["none"]
            ])
        }
        let client = OIDCNativeAuthClient(httpClient: http, clock: FixedOIDCClock(now: now))
        let configuration = try configuration()

        let document = try await client.discover(configuration: configuration)

        XCTAssertEqual(document.issuer, issuer)
        XCTAssertEqual(document.tokenEndpoint, tokenEndpoint)
        XCTAssertEqual(document.tokenEndpointAuthMethodsSupported, ["none"])
    }

    func testDiscoveryRejectsIssuerMismatchAndInsecureTokenEndpoint() async throws {
        let mismatchedHTTP = ScriptedOIDCHTTPClient { _, _ in
            Self.jsonResponse([
                "issuer": "https://other.example.invalid/tenant",
                "token_endpoint": "https://tokens.example.invalid/token"
            ])
        }
        let client = OIDCNativeAuthClient(httpClient: mismatchedHTTP, clock: FixedOIDCClock(now: now))
        do {
            _ = try await client.discover(configuration: configuration())
            XCTFail("Expected issuer mismatch")
        } catch {
            XCTAssertEqual(error as? OIDCNativeAuthError, .discoveryIssuerMismatch)
        }

        let insecureHTTP = ScriptedOIDCHTTPClient { [issuer] _, _ in
            Self.jsonResponse([
                "issuer": issuer.absoluteString,
                "token_endpoint": "http://tokens.example.invalid/token"
            ])
        }
        let insecureClient = OIDCNativeAuthClient(httpClient: insecureHTTP, clock: FixedOIDCClock(now: now))
        do {
            _ = try await insecureClient.discover(configuration: configuration())
            XCTFail("Expected invalid endpoint")
        } catch {
            XCTAssertEqual(error as? OIDCNativeAuthError, .invalidEndpointURL)
        }
    }

    func testDiscoveryRejectsOversizedResponseBeforeParsing() async throws {
        let http = ScriptedOIDCHTTPClient { _, _ in
            OIDCHTTPResponse(statusCode: 200, body: Data(repeating: 0x41, count: 2_048))
        }
        let client = OIDCNativeAuthClient(
            httpClient: http,
            clock: FixedOIDCClock(now: now),
            maximumResponseBytes: 1_024
        )

        do {
            _ = try await client.discover(configuration: configuration())
            XCTFail("Expected bounded response failure")
        } catch {
            XCTAssertEqual(error as? OIDCNativeAuthError, .responseTooLarge(stage: .discovery))
        }
    }

    func testDefaultTrustContractFailsClosedForCustomCertificateAuthority() async throws {
        let http = ScriptedOIDCHTTPClient { _, _ in
            XCTFail("HTTP must not run when required trust is unsupported")
            return OIDCHTTPResponse(statusCode: 500, body: Data())
        }
        let client = OIDCNativeAuthClient(httpClient: http, clock: FixedOIDCClock(now: now))
        let configuration = try OIDCAuthProviderConfiguration(
            issuerURL: issuer,
            clientID: "synthetic-client",
            refreshToken: "synthetic-refresh",
            certificateAuthorityPath: "synthetic-ca.pem"
        )

        do {
            _ = try await client.discover(configuration: configuration)
            XCTFail("Expected unsupported custom CA")
        } catch {
            XCTAssertEqual(error as? OIDCNativeAuthError, .customCertificateAuthorityUnsupported)
        }
        let requestCount = await http.requestCount()
        XCTAssertEqual(requestCount, 0)
    }

    func testCustomCertificateAuthorityPathIsForwardedOnlyToCapableTransport() async throws {
        let idToken = try jwt(
            issuer: issuer.absoluteString,
            audiences: ["synthetic-client"],
            expiration: now.addingTimeInterval(900)
        )
        let http = ScriptedOIDCHTTPClient(supportsCustomCertificateAuthorities: true) {
            [issuer, tokenEndpoint] request, index in
            XCTAssertEqual(request.customCertificateAuthorityPath, "synthetic-ca.pem")
            if index == 1 {
                return Self.jsonResponse([
                    "issuer": issuer.absoluteString,
                    "token_endpoint": tokenEndpoint.absoluteString,
                    "token_endpoint_auth_methods_supported": ["none"]
                ])
            }
            return Self.jsonResponse(["id_token": idToken])
        }
        let configuration = try OIDCAuthProviderConfiguration(
            issuerURL: issuer,
            clientID: "synthetic-client",
            refreshToken: "synthetic-refresh",
            certificateAuthorityPath: "synthetic-ca.pem"
        )
        let client = OIDCNativeAuthClient(httpClient: http, clock: FixedOIDCClock(now: now))

        _ = try await client.refresh(configuration: configuration)

        let requestCount = await http.requestCount()
        XCTAssertEqual(requestCount, 2)
    }

    func testRefreshReturnsIDTokenAndRotatesRefreshToken() async throws {
        let idToken = try jwt(
            issuer: issuer.absoluteString,
            audiences: ["synthetic-client"],
            expiration: now.addingTimeInterval(900)
        )
        let http = refreshHTTP(idToken: idToken, rotatedRefreshToken: "rotated-refresh")
        let client = OIDCNativeAuthClient(httpClient: http, clock: FixedOIDCClock(now: now))

        let result = try await client.refresh(configuration: configuration())

        XCTAssertEqual(result.credential.idToken, idToken)
        XCTAssertEqual(result.credential.expiration, now.addingTimeInterval(900))
        XCTAssertEqual(result.refreshToken, "rotated-refresh")
        XCTAssertTrue(result.didRotateRefreshToken)
        let requests = await http.capturedRequests()
        XCTAssertEqual(requests.count, 2)
        let tokenRequest = try XCTUnwrap(requests.last)
        let body = String(decoding: tokenRequest.body ?? Data(), as: UTF8.self)
        XCTAssertTrue(body.contains("grant_type=refresh_token"))
        XCTAssertTrue(body.contains("refresh_token=synthetic-refresh"))
        XCTAssertTrue(body.contains("client_id=synthetic-client"))
        XCTAssertFalse(body.contains("access_token"))
    }

    func testRefreshKeepsOriginalRefreshTokenWhenProviderDoesNotRotate() async throws {
        let idToken = try jwt(
            issuer: issuer.absoluteString,
            audiences: ["synthetic-client"],
            expiration: now.addingTimeInterval(900)
        )
        let http = refreshHTTP(idToken: idToken, rotatedRefreshToken: nil)
        let client = OIDCNativeAuthClient(httpClient: http, clock: FixedOIDCClock(now: now))

        let result = try await client.refresh(configuration: configuration())

        XCTAssertEqual(result.refreshToken, "synthetic-refresh")
        XCTAssertFalse(result.didRotateRefreshToken)
    }

    func testConfidentialClientUsesAdvertisedBasicAuthenticationWithoutSecretInBody() async throws {
        let idToken = try jwt(
            issuer: issuer.absoluteString,
            audiences: ["synthetic-client"],
            expiration: now.addingTimeInterval(900)
        )
        let http = ScriptedOIDCHTTPClient { [issuer, tokenEndpoint] request, index in
            if index == 1 {
                return Self.jsonResponse([
                    "issuer": issuer.absoluteString,
                    "token_endpoint": tokenEndpoint.absoluteString,
                    "token_endpoint_auth_methods_supported": ["client_secret_basic"]
                ])
            }
            XCTAssertEqual(request.headers["Authorization"], "Basic c3ludGhldGljLWNsaWVudDpzeW50aGV0aWMtc2VjcmV0")
            XCTAssertFalse(String(decoding: request.body ?? Data(), as: UTF8.self).contains("synthetic-secret"))
            return Self.jsonResponse(["id_token": idToken])
        }
        let configuration = try OIDCAuthProviderConfiguration(
            issuerURL: issuer,
            clientID: "synthetic-client",
            clientSecret: "synthetic-secret",
            refreshToken: "synthetic-refresh"
        )
        let client = OIDCNativeAuthClient(httpClient: http, clock: FixedOIDCClock(now: now))

        _ = try await client.refresh(configuration: configuration)

        let requestCount = await http.requestCount()
        XCTAssertEqual(requestCount, 2)
    }

    func testRefreshRejectsAccessTokenOnlyAndWrongIDTokenClaims() async throws {
        let accessOnly = ScriptedOIDCHTTPClient { [issuer, tokenEndpoint] _, index in
            index == 1
                ? Self.jsonResponse(["issuer": issuer.absoluteString, "token_endpoint": tokenEndpoint.absoluteString])
                : Self.jsonResponse(["access_token": "must-not-be-used"])
        }
        let client = OIDCNativeAuthClient(httpClient: accessOnly, clock: FixedOIDCClock(now: now))
        do {
            _ = try await client.refresh(configuration: configuration())
            XCTFail("Expected missing ID token")
        } catch {
            XCTAssertEqual(error as? OIDCNativeAuthError, .tokenResponseMissingIDToken)
        }

        let wrongAudience = try jwt(
            issuer: issuer.absoluteString,
            audiences: ["wrong-client"],
            expiration: now.addingTimeInterval(900)
        )
        let wrongAudienceClient = OIDCNativeAuthClient(
            httpClient: refreshHTTP(idToken: wrongAudience, rotatedRefreshToken: nil),
            clock: FixedOIDCClock(now: now)
        )
        do {
            _ = try await wrongAudienceClient.refresh(configuration: configuration())
            XCTFail("Expected audience mismatch")
        } catch {
            XCTAssertEqual(error as? OIDCNativeAuthError, .tokenAudienceMismatch)
        }
    }

    func testProviderAndTransportErrorsDoNotExposeResponseOrUnderlyingSecrets() async throws {
        let providerHTTP = ScriptedOIDCHTTPClient { [issuer, tokenEndpoint] _, index in
            if index == 1 {
                return Self.jsonResponse(["issuer": issuer.absoluteString, "token_endpoint": tokenEndpoint.absoluteString])
            }
            return Self.jsonResponse(
                [
                    "error": "invalid_grant<script>",
                    "error_description": "refresh_token=must-never-appear synthetic.user@example.invalid"
                ],
                statusCode: 400
            )
        }
        let providerClient = OIDCNativeAuthClient(httpClient: providerHTTP, clock: FixedOIDCClock(now: now))
        do {
            _ = try await providerClient.refresh(configuration: configuration())
            XCTFail("Expected provider rejection")
        } catch {
            let message = error.localizedDescription
            XCTAssertTrue(message.contains("invalid_grantscript"))
            XCTAssertFalse(message.contains("must-never-appear"))
            XCTAssertFalse(message.contains("example.invalid"))
        }

        let transportHTTP = ScriptedOIDCHTTPClient { _, _ in
            throw SyntheticTransportError.secret("refresh_token=must-never-appear")
        }
        let transportClient = OIDCNativeAuthClient(httpClient: transportHTTP, clock: FixedOIDCClock(now: now))
        do {
            _ = try await transportClient.discover(configuration: configuration())
            XCTFail("Expected transport failure")
        } catch {
            XCTAssertEqual(error as? OIDCNativeAuthError, .transport(stage: .discovery))
            XCTAssertFalse(error.localizedDescription.contains("must-never-appear"))
        }
    }

    func testCredentialSessionCoalescesConcurrentRefreshAndPublishesRotationOnce() async throws {
        let idToken = try jwt(
            issuer: issuer.absoluteString,
            audiences: ["synthetic-client"],
            expiration: now.addingTimeInterval(900)
        )
        let http = refreshHTTP(
            idToken: idToken,
            rotatedRefreshToken: "rotated-refresh",
            tokenDelayNanoseconds: 20_000_000
        )
        let clock = FixedOIDCClock(now: now)
        let client = OIDCNativeAuthClient(httpClient: http, clock: clock)
        let session = OIDCNativeCredentialSession(
            configuration: try configuration(),
            client: client,
            clock: clock
        )

        let credentials = try await withThrowingTaskGroup(of: OIDCNativeCredential.self) { group in
            for _ in 0..<32 {
                group.addTask { try await session.credential() }
            }
            var output: [OIDCNativeCredential] = []
            for try await credential in group {
                output.append(credential)
            }
            return output
        }

        XCTAssertEqual(credentials.count, 32)
        XCTAssertTrue(credentials.allSatisfy { $0.idToken == idToken })
        let requestCount = await http.requestCount()
        let refreshTokenUpdate = await session.consumeRefreshTokenUpdate()
        let consumedRefreshTokenUpdate = await session.consumeRefreshTokenUpdate()
        XCTAssertEqual(requestCount, 2, "One discovery and one token request")
        XCTAssertEqual(refreshTokenUpdate, "rotated-refresh")
        XCTAssertNil(consumedRefreshTokenUpdate)
    }

    func testCredentialSessionUsesValidImportedIDTokenWithoutNetwork() async throws {
        let idToken = try jwt(
            issuer: issuer.absoluteString,
            audiences: ["synthetic-client"],
            expiration: now.addingTimeInterval(900)
        )
        let http = ScriptedOIDCHTTPClient { _, _ in
            XCTFail("A valid cached ID token must not trigger network access")
            return OIDCHTTPResponse(statusCode: 500, body: Data())
        }
        let clock = FixedOIDCClock(now: now)
        let configuration = try OIDCAuthProviderConfiguration(
            issuerURL: issuer,
            clientID: "synthetic-client",
            idToken: idToken,
            refreshToken: "synthetic-refresh"
        )
        let client = OIDCNativeAuthClient(httpClient: http, clock: clock)
        let session = OIDCNativeCredentialSession(configuration: configuration, client: client, clock: clock)

        let credential = try await session.credential()

        XCTAssertEqual(credential.idToken, idToken)
        let requestCount = await http.requestCount()
        XCTAssertEqual(requestCount, 0)
    }

    func testJWTParsingBenchmarkKPI() throws {
        let token = try jwt(
            issuer: issuer.absoluteString,
            audiences: ["synthetic-client"],
            expiration: now.addingTimeInterval(900)
        )
        let iterations = 10_000
        let started = ContinuousClock.now
        for _ in 0..<iterations {
            _ = try OIDCJWTMetadata.parse(token)
        }
        let duration = ContinuousClock.now - started
        let seconds = Double(duration.components.seconds)
            + Double(duration.components.attoseconds) / 1_000_000_000_000_000_000

        XCTAssertLessThan(seconds, 3.0, "OIDC JWT parsing KPI: 10,000 parses must finish within 3 seconds")
    }

    private func configuration() throws -> OIDCAuthProviderConfiguration {
        try OIDCAuthProviderConfiguration(
            issuerURL: issuer,
            clientID: "synthetic-client",
            refreshToken: "synthetic-refresh"
        )
    }

    private func refreshHTTP(
        idToken: String,
        rotatedRefreshToken: String?,
        tokenDelayNanoseconds: UInt64 = 0
    ) -> ScriptedOIDCHTTPClient {
        ScriptedOIDCHTTPClient { [issuer, tokenEndpoint] request, index in
            if index == 1 {
                XCTAssertEqual(request.method, .get)
                return Self.jsonResponse([
                    "issuer": issuer.absoluteString,
                    "token_endpoint": tokenEndpoint.absoluteString,
                    "token_endpoint_auth_methods_supported": ["none"]
                ])
            }
            if tokenDelayNanoseconds > 0 {
                try await Task.sleep(nanoseconds: tokenDelayNanoseconds)
            }
            XCTAssertEqual(request.method, .post)
            var payload: [String: Any] = [
                "access_token": "access-token-that-must-be-ignored",
                "id_token": idToken,
                "token_type": "Bearer"
            ]
            if let rotatedRefreshToken {
                payload["refresh_token"] = rotatedRefreshToken
            }
            return Self.jsonResponse(payload)
        }
    }

    private func jwt(issuer: String, audiences: [String], expiration: Date) throws -> String {
        try jwt(payload: [
            "iss": issuer,
            "aud": audiences.count == 1 ? audiences[0] : audiences,
            "exp": expiration.timeIntervalSince1970
        ])
    }

    private func jwt(payload: [String: Any]) throws -> String {
        let header = try JSONSerialization.data(withJSONObject: ["alg": "RS256", "typ": "JWT"])
        let body = try JSONSerialization.data(withJSONObject: payload)
        return "\(base64URL(header)).\(base64URL(body)).synthetic-signature"
    }

    private func base64URL(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private static func jsonResponse(_ value: Any, statusCode: Int = 200) -> OIDCHTTPResponse {
        OIDCHTTPResponse(
            statusCode: statusCode,
            headers: ["content-type": "application/json"],
            body: try! JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
        )
    }
}

private struct FixedOIDCClock: OIDCClock {
    let nowValue: Date

    init(now: Date) {
        self.nowValue = now
    }

    func now() -> Date { nowValue }
}

private actor ScriptedOIDCHTTPClient: OIDCHTTPClient {
    nonisolated let supportsCustomCertificateAuthorities: Bool
    private let handler: @Sendable (OIDCHTTPRequest, Int) async throws -> OIDCHTTPResponse
    private var requests: [OIDCHTTPRequest] = []

    init(
        supportsCustomCertificateAuthorities: Bool = false,
        handler: @escaping @Sendable (OIDCHTTPRequest, Int) async throws -> OIDCHTTPResponse
    ) {
        self.supportsCustomCertificateAuthorities = supportsCustomCertificateAuthorities
        self.handler = handler
    }

    func send(_ request: OIDCHTTPRequest) async throws -> OIDCHTTPResponse {
        requests.append(request)
        return try await handler(request, requests.count)
    }

    func requestCount() -> Int { requests.count }
    func capturedRequests() -> [OIDCHTTPRequest] { requests }
}

private enum SyntheticTransportError: Error {
    case secret(String)
}
