import Foundation
import XCTest
@testable import RuneSecurity

final class AKSServicePrincipalAuthTests: XCTestCase {
    private let tenantID = "11111111-1111-4111-8111-111111111111"
    private let serverID = "22222222-2222-4222-8222-222222222222"
    private let clientID = "33333333-3333-4333-8333-333333333333"
    private let now = Date(timeIntervalSince1970: 1_700_000_000)

    func testParserRecognizesSeparateAndInlineKubeloginSPNFlags() throws {
        let separate = try XCTUnwrap(AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: "/synthetic/bin/kubelogin",
            arguments: [
                "get-token",
                "--environment", "AzurePublicCloud",
                "--server-id", serverID,
                "--client-id", clientID,
                "--tenant-id", tenantID,
                "--login", "spn"
            ]
        ))
        let inline = try XCTUnwrap(AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: "kubelogin",
            arguments: [
                "get-token",
                "--environment=AzurePublicCloud",
                "--server-id=\(serverID)",
                "--client-id=\(clientID)",
                "--tenant-id=\(tenantID)",
                "-l=spn"
            ]
        ))

        XCTAssertEqual(separate, inline)
        XCTAssertEqual(separate.tenantID, tenantID)
        XCTAssertEqual(separate.serverID, serverID)
        XCTAssertEqual(separate.clientID, clientID)
        XCTAssertEqual(separate.environment, .publicCloud)
        XCTAssertEqual(separate.scope, "\(serverID)/.default")
    }

    func testParserReturnsNilForUnrelatedCommandsAndOperations() throws {
        XCTAssertNil(try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: "aws",
            arguments: ["eks", "get-token"]
        ))
        XCTAssertNil(try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: "kubelogin",
            arguments: ["convert-kubeconfig"]
        ))
    }

    func testParserFailsClosedForInteractiveCLIAndWorkloadModes() {
        for mode in ["devicecode", "interactive", "azurecli", "azuredevelopercli", "msi", "workloadidentity"] {
            XCTAssertThrowsError(try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
                command: "kubelogin",
                arguments: baseArguments(loginMode: mode)
            )) { error in
                XCTAssertEqual(error as? AKSServicePrincipalAuthError, .unsupportedLoginMode(mode))
            }
        }
        XCTAssertThrowsError(try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: "kubelogin",
            arguments: baseArguments(loginMode: nil)
        )) { error in
            XCTAssertEqual(error as? AKSServicePrincipalAuthError, .unsupportedLoginMode("devicecode"))
        }
    }

    func testParserFailsClosedForLegacyPoPInlineSecretAndUnknownOptions() {
        let cases: [([String], AKSServicePrincipalAuthError)] = [
            (["--legacy"], .legacyModeUnsupported),
            (["--legacy=false"], .legacyModeUnsupported),
            (["--pop-enabled"], .proofOfPossessionUnsupported),
            (["--pop-claims", "u=/synthetic/resource"], .proofOfPossessionUnsupported),
            (["--client-secret", "must-not-be-read"], .inlineSecretUnsupported),
            (["--authority-host", "https://other.example.invalid"], .unsupportedOption),
            (["--unknown-option", "value"], .unsupportedOption)
        ]
        for (suffix, expected) in cases {
            XCTAssertThrowsError(try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
                command: "kubelogin",
                arguments: baseArguments() + suffix
            )) { error in
                XCTAssertEqual(error as? AKSServicePrincipalAuthError, expected)
                XCTAssertFalse(error.localizedDescription.contains("must-not-be-read"))
            }
        }
    }

    func testParserRejectsDuplicatesMissingValuesAndInvalidIdentifiers() {
        XCTAssertThrowsError(try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: "kubelogin",
            arguments: baseArguments() + ["--tenant-id", tenantID]
        )) { error in
            XCTAssertEqual(error as? AKSServicePrincipalAuthError, .duplicateOption("tenant-id"))
        }
        XCTAssertThrowsError(try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: "kubelogin",
            arguments: ["get-token", "--tenant-id"]
        ))
        XCTAssertThrowsError(try AKSKubeloginServicePrincipalDescriptor(
            tenantID: "../other",
            serverID: serverID,
            clientID: clientID
        )) { error in
            XCTAssertEqual(error as? AKSServicePrincipalAuthError, .invalidConfiguration(field: "tenant-id"))
        }
    }

    func testEnvironmentMapsOnlyToKnownHTTPSAuthorityHosts() throws {
        let expectations: [(AKSAzureEnvironment, String)] = [
            (.publicCloud, "login.microsoftonline.com"),
            (.chinaCloud, "login.chinacloudapi.cn"),
            (.usGovernment, "login.microsoftonline.us"),
            (.germanCloud, "login.microsoftonline.de")
        ]
        for (environment, host) in expectations {
            let descriptor = try AKSKubeloginServicePrincipalDescriptor(
                tenantID: tenantID,
                serverID: serverID,
                clientID: clientID,
                environment: environment
            )
            XCTAssertEqual(descriptor.tokenEndpoint.scheme, "https")
            XCTAssertEqual(descriptor.tokenEndpoint.host, host)
            XCTAssertEqual(
                descriptor.tokenEndpoint.path,
                "/\(tenantID)/oauth2/v2.0/token"
            )
        }
        XCTAssertThrowsError(try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: "kubelogin",
            arguments: baseArguments(environment: "SyntheticCloud")
        )) { error in
            XCTAssertEqual(error as? AKSServicePrincipalAuthError, .unsupportedEnvironment)
        }
    }

    func testApplicationURIServerAudienceProducesDefaultScope() throws {
        let descriptor = try AKSKubeloginServicePrincipalDescriptor(
            tenantID: tenantID,
            serverID: "api://synthetic-aks-server/",
            clientID: clientID
        )
        XCTAssertEqual(descriptor.scope, "api://synthetic-aks-server/.default")
    }

    func testCredentialAndTokenDescriptionsNeverExposeSecrets() throws {
        let credentials = try AKSServicePrincipalCredentials(
            clientID: clientID,
            clientSecret: "synthetic-secret-material"
        )
        let token = try AKSServicePrincipalAccessToken(
            value: "synthetic-access-token",
            expiration: now.addingTimeInterval(3_600)
        )

        XCTAssertFalse(credentials.description.contains("synthetic-secret-material"))
        XCTAssertFalse(credentials.debugDescription.contains(clientID))
        XCTAssertFalse(token.description.contains("synthetic-access-token"))
    }

    func testCredentialSecretModelRoundTripsOnlyThroughExplicitEncoding() throws {
        let credentials = try self.credentials(secret: "synthetic-secret-material")

        let encoded = try JSONEncoder().encode(credentials)
        let decoded = try JSONDecoder().decode(AKSServicePrincipalCredentials.self, from: encoded)

        XCTAssertEqual(decoded, credentials)
        XCTAssertThrowsError(try JSONDecoder().decode(
            AKSServicePrincipalCredentials.self,
            from: Data(#"{"clientID":"not-an-app-id","clientSecret":"synthetic"}"#.utf8)
        ))
    }

    func testTokenServicePostsClientCredentialsAndReturnsBearerToken() async throws {
        let http = ScriptedAKSTokenHTTPClient { [now] request, _ in
            XCTAssertEqual(request.url.host, "login.microsoftonline.com")
            XCTAssertEqual(request.headers["Content-Type"], "application/x-www-form-urlencoded")
            let body = String(decoding: request.body, as: UTF8.self)
            XCTAssertTrue(body.contains("client_id=33333333-3333-4333-8333-333333333333"))
            XCTAssertTrue(body.contains("client_secret=secret%2B%2F%3D%26%20value"))
            XCTAssertTrue(body.contains("grant_type=client_credentials"))
            XCTAssertTrue(body.contains("scope=22222222-2222-4222-8222-222222222222%2F.default"))
            return Self.jsonResponse([
                "access_token": "synthetic-access-token",
                "token_type": "Bearer",
                "expires_in": "3600",
                "synthetic_now": now.timeIntervalSince1970
            ])
        }
        let clock = FixedAKSClock(now: now)
        let service = AKSServicePrincipalTokenService(httpClient: http, clock: clock)

        let token = try await service.token(
            descriptor: descriptor(),
            credentials: credentials(secret: "secret+/=& value")
        )

        XCTAssertEqual(token.value, "synthetic-access-token")
        XCTAssertEqual(token.expiration, now.addingTimeInterval(3_600))
        let requestCount = await http.requestCount()
        XCTAssertEqual(requestCount, 1)
    }

    func testTokenServiceRejectsMismatchedClientBeforeNetwork() async throws {
        let http = ScriptedAKSTokenHTTPClient { _, _ in
            XCTFail("Mismatched client IDs must fail before HTTP")
            return Self.jsonResponse([:])
        }
        let service = AKSServicePrincipalTokenService(httpClient: http, clock: FixedAKSClock(now: now))
        let otherCredentials = try AKSServicePrincipalCredentials(
            clientID: "44444444-4444-4444-8444-444444444444",
            clientSecret: "synthetic-secret"
        )

        do {
            _ = try await service.token(descriptor: descriptor(), credentials: otherCredentials)
            XCTFail("Expected client ID mismatch")
        } catch {
            XCTAssertEqual(error as? AKSServicePrincipalAuthError, .clientIDMismatch)
        }
        let requestCount = await http.requestCount()
        XCTAssertEqual(requestCount, 0)
    }

    func testTokenServiceRejectsMalformedExpiredAndNonBearerResponses() async throws {
        let responses: [[String: Any]] = [
            ["expires_in": 3_600],
            ["access_token": "token", "expires_in": 0],
            ["access_token": "token", "expires_in": 3_600, "token_type": "PoP"]
        ]
        for response in responses {
            let encodedResponse = Self.jsonResponse(response)
            let http = ScriptedAKSTokenHTTPClient { _, _ in encodedResponse }
            let service = AKSServicePrincipalTokenService(httpClient: http, clock: FixedAKSClock(now: now))
            do {
                _ = try await service.token(descriptor: descriptor(), credentials: credentials())
                XCTFail("Expected invalid token response")
            } catch {
                XCTAssertEqual(error as? AKSServicePrincipalAuthError, .invalidTokenResponse)
            }
        }
    }

    func testProviderAndTransportErrorsAreSanitized() async throws {
        let providerHTTP = ScriptedAKSTokenHTTPClient { _, _ in
            Self.jsonResponse([
                "error": "invalid_client<script>",
                "error_description": "client_secret=must-never-appear synthetic.user@example.invalid"
            ], statusCode: 401)
        }
        let providerService = AKSServicePrincipalTokenService(
            httpClient: providerHTTP,
            clock: FixedAKSClock(now: now)
        )
        do {
            _ = try await providerService.token(descriptor: descriptor(), credentials: credentials())
            XCTFail("Expected provider rejection")
        } catch {
            XCTAssertEqual(
                error as? AKSServicePrincipalAuthError,
                .providerRejected(code: "invalid_clientscript")
            )
            XCTAssertFalse(error.localizedDescription.contains("must-never-appear"))
            XCTAssertFalse(error.localizedDescription.contains("example.invalid"))
        }

        let transportHTTP = ScriptedAKSTokenHTTPClient { _, _ in
            throw SyntheticAKSTransportError.secret("client_secret=must-never-appear")
        }
        let transportService = AKSServicePrincipalTokenService(
            httpClient: transportHTTP,
            clock: FixedAKSClock(now: now)
        )
        do {
            _ = try await transportService.token(descriptor: descriptor(), credentials: credentials())
            XCTFail("Expected transport failure")
        } catch {
            XCTAssertEqual(error as? AKSServicePrincipalAuthError, .transport)
            XCTAssertFalse(error.localizedDescription.contains("must-never-appear"))
        }
    }

    func testTokenServiceRejectsOversizedResponseBeforeDecoding() async throws {
        let http = ScriptedAKSTokenHTTPClient { _, _ in
            AKSServicePrincipalTokenHTTPResponse(
                statusCode: 200,
                body: Data(repeating: 0x41, count: 2_048)
            )
        }
        let service = AKSServicePrincipalTokenService(
            httpClient: http,
            clock: FixedAKSClock(now: now),
            maximumResponseBytes: 1_024
        )
        do {
            _ = try await service.token(descriptor: descriptor(), credentials: credentials())
            XCTFail("Expected response size failure")
        } catch {
            XCTAssertEqual(error as? AKSServicePrincipalAuthError, .responseTooLarge)
        }
    }

    func testCredentialSessionCoalescesConcurrentRequestsCachesAndInvalidates() async throws {
        let http = ScriptedAKSTokenHTTPClient { request, index in
            try await Task.sleep(nanoseconds: 20_000_000)
            return Self.jsonResponse([
                "access_token": "synthetic-token-\(index)",
                "token_type": "Bearer",
                "expires_in": 3_600,
                "request_size": request.body.count
            ])
        }
        let clock = MutableAKSClock(now: now)
        let service = AKSServicePrincipalTokenService(httpClient: http, clock: clock)
        let session = AKSServicePrincipalCredentialSession(
            descriptor: try descriptor(),
            credentials: try credentials(),
            service: service,
            clock: clock
        )

        let tokens = try await withThrowingTaskGroup(of: AKSServicePrincipalAccessToken.self) { group in
            for _ in 0..<40 {
                group.addTask { try await session.token() }
            }
            var output: [AKSServicePrincipalAccessToken] = []
            for try await token in group { output.append(token) }
            return output
        }
        XCTAssertEqual(tokens.count, 40)
        XCTAssertTrue(tokens.allSatisfy { $0.value == "synthetic-token-1" })
        let cached = try await session.token()
        XCTAssertEqual(cached.value, "synthetic-token-1")
        var requestCount = await http.requestCount()
        XCTAssertEqual(requestCount, 1)

        await session.invalidate()
        let refreshed = try await session.token()
        XCTAssertEqual(refreshed.value, "synthetic-token-2")
        requestCount = await http.requestCount()
        XCTAssertEqual(requestCount, 2)
    }

    func testCredentialSessionRefreshesWhenTokenEntersMinimumValidityWindow() async throws {
        let http = ScriptedAKSTokenHTTPClient { _, index in
            Self.jsonResponse([
                "access_token": "synthetic-token-\(index)",
                "token_type": "Bearer",
                "expires_in": 120
            ])
        }
        let clock = MutableAKSClock(now: now)
        let service = AKSServicePrincipalTokenService(httpClient: http, clock: clock)
        let session = AKSServicePrincipalCredentialSession(
            descriptor: try descriptor(),
            credentials: try credentials(),
            service: service,
            clock: clock
        )

        _ = try await session.token(minimumValidity: 60)
        clock.advance(by: 61)
        let refreshed = try await session.token(minimumValidity: 60)

        XCTAssertEqual(refreshed.value, "synthetic-token-2")
        let requestCount = await http.requestCount()
        XCTAssertEqual(requestCount, 2)
    }

    func testKubeloginParserBenchmarkKPI() throws {
        let arguments = baseArguments()
        let iterations = 20_000
        let started = ContinuousClock.now
        for _ in 0..<iterations {
            _ = try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
                command: "kubelogin",
                arguments: arguments
            )
        }
        let duration = ContinuousClock.now - started
        let seconds = Double(duration.components.seconds)
            + Double(duration.components.attoseconds) / 1_000_000_000_000_000_000

        XCTAssertLessThan(seconds, 3.0, "AKS kubelogin parser KPI: 20,000 parses must finish within 3 seconds")
    }

    private func baseArguments(
        loginMode: String? = "spn",
        environment: String = "AzurePublicCloud"
    ) -> [String] {
        var arguments = [
            "get-token",
            "--environment", environment,
            "--server-id", serverID,
            "--client-id", clientID,
            "--tenant-id", tenantID
        ]
        if let loginMode {
            arguments.append(contentsOf: ["--login", loginMode])
        }
        return arguments
    }

    private func descriptor() throws -> AKSKubeloginServicePrincipalDescriptor {
        try AKSKubeloginServicePrincipalDescriptor(
            tenantID: tenantID,
            serverID: serverID,
            clientID: clientID
        )
    }

    private func credentials(secret: String = "synthetic-secret") throws -> AKSServicePrincipalCredentials {
        try AKSServicePrincipalCredentials(clientID: clientID, clientSecret: secret)
    }

    private static func jsonResponse(_ value: Any, statusCode: Int = 200) -> AKSServicePrincipalTokenHTTPResponse {
        AKSServicePrincipalTokenHTTPResponse(
            statusCode: statusCode,
            body: try! JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
        )
    }
}

private struct FixedAKSClock: AKSServicePrincipalClock {
    let value: Date
    init(now: Date) { value = now }
    func now() -> Date { value }
}

private final class MutableAKSClock: AKSServicePrincipalClock, @unchecked Sendable {
    private let lock = NSLock()
    private var value: Date

    init(now: Date) { value = now }

    func now() -> Date {
        lock.withLock { value }
    }

    func advance(by interval: TimeInterval) {
        lock.withLock { value = value.addingTimeInterval(interval) }
    }
}

private actor ScriptedAKSTokenHTTPClient: AKSServicePrincipalTokenHTTPClient {
    private let handler: @Sendable (
        AKSServicePrincipalTokenHTTPRequest,
        Int
    ) async throws -> AKSServicePrincipalTokenHTTPResponse
    private var requests: [AKSServicePrincipalTokenHTTPRequest] = []

    init(handler: @escaping @Sendable (
        AKSServicePrincipalTokenHTTPRequest,
        Int
    ) async throws -> AKSServicePrincipalTokenHTTPResponse) {
        self.handler = handler
    }

    func send(_ request: AKSServicePrincipalTokenHTTPRequest) async throws -> AKSServicePrincipalTokenHTTPResponse {
        requests.append(request)
        return try await handler(request, requests.count)
    }

    func requestCount() -> Int { requests.count }
}

private enum SyntheticAKSTransportError: Error {
    case secret(String)
}
