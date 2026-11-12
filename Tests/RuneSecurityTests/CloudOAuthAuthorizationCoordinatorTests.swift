import CryptoKit
import Foundation
import XCTest
@testable import RuneSecurity

final class CloudOAuthAuthorizationCoordinatorTests: XCTestCase {
    func testAuthorizationAttemptUsesFreshPKCEStateAndNonceAndConsumesOnce() async throws {
        let random = SyntheticOAuthRandomBytes()
        let clock = SyntheticOAuthClock(Date(timeIntervalSince1970: 1_800_000_000))
        let coordinator = CloudOAuthAuthorizationCoordinator(
            now: { clock.now() },
            randomBytes: { count in try random.bytes(count: count) }
        )
        let sheetID = CloudOAuthSheetID(rawValue: uuid(1))
        let configuration = try configuration()

        let attempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
        let query = try queryValues(attempt.authorizationURL)

        XCTAssertEqual(query["response_type"], "code")
        XCTAssertEqual(query["client_id"], "synthetic-public-client")
        XCTAssertEqual(query["redirect_uri"], "rune-synthetic://oauth/callback")
        XCTAssertEqual(query["scope"], "openid offline_access cluster.read")
        XCTAssertEqual(query["code_challenge_method"], "S256")
        XCTAssertEqual(query["state"]?.count, 43)
        XCTAssertEqual(query["nonce"]?.count, 43)
        XCTAssertEqual(query["code_challenge"]?.count, 43)

        let callback = try callbackURL(
            code: "synthetic-authorization-code",
            state: try XCTUnwrap(query["state"]),
            issuer: "https://identity.example.invalid/tenant"
        )
        let exchange = try await coordinator.consumeCallback(
            callback,
            sheetID: sheetID,
            attemptID: attempt.id,
            generation: attempt.generation
        )

        let expectedChallenge = base64URL(Data(SHA256.hash(data: Data(exchange.codeVerifier.utf8))))
        XCTAssertEqual(query["code_challenge"], expectedChallenge)
        XCTAssertEqual(exchange.authorizationCode, "synthetic-authorization-code")
        XCTAssertEqual(exchange.nonce, query["nonce"])
        XCTAssertEqual(exchange.expectedIssuer, "https://identity.example.invalid/tenant")
        XCTAssertEqual(exchange.expectedAudience, "synthetic-public-client")
        XCTAssertFalse(exchange.description.contains(exchange.authorizationCode))
        XCTAssertFalse(exchange.description.contains(exchange.codeVerifier))
        XCTAssertFalse(exchange.description.contains(exchange.nonce))
        let activeAfterConsumption = await coordinator.hasActiveAttempt(sheetID: sheetID)
        XCTAssertFalse(activeAfterConsumption)

        do {
            _ = try await coordinator.consumeCallback(
                callback,
                sheetID: sheetID,
                attemptID: attempt.id,
                generation: attempt.generation
            )
            XCTFail("Expected one-time callback consumption")
        } catch {
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .attemptNotFound)
        }
    }

    func testNewAttemptSupersedesOlderCallbackWithoutConsumingCurrentAttempt() async throws {
        let random = SyntheticOAuthRandomBytes()
        let coordinator = CloudOAuthAuthorizationCoordinator(
            randomBytes: { count in try random.bytes(count: count) }
        )
        let sheetID = CloudOAuthSheetID(rawValue: uuid(2))
        let configuration = try configuration()
        let first = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
        let firstState = try XCTUnwrap(try queryValues(first.authorizationURL)["state"])
        let second = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
        let secondState = try XCTUnwrap(try queryValues(second.authorizationURL)["state"])

        XCTAssertNotEqual(first.generation, second.generation)
        XCTAssertNotEqual(firstState, secondState)
        do {
            _ = try await coordinator.consumeCallback(
                callbackURL(code: "old-code", state: firstState),
                sheetID: sheetID,
                attemptID: first.id,
                generation: first.generation
            )
            XCTFail("Expected the older attempt to be superseded")
        } catch {
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .superseded)
        }
        let currentStillActive = await coordinator.hasActiveAttempt(sheetID: sheetID)
        XCTAssertTrue(currentStillActive)

        let exchange = try await coordinator.consumeCallback(
            callbackURL(code: "current-code", state: secondState),
            sheetID: sheetID,
            attemptID: second.id,
            generation: second.generation
        )
        XCTAssertEqual(exchange.authorizationCode, "current-code")
    }

    func testAuthorizationSecretsAreRedactedFromDebugOutputAndReflection() async throws {
        let coordinator = CloudOAuthAuthorizationCoordinator()
        let sheetID = CloudOAuthSheetID(rawValue: uuid(6))
        let attempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration())
        let query = try queryValues(attempt.authorizationURL)
        let state = try XCTUnwrap(query["state"])
        let nonce = try XCTUnwrap(query["nonce"])
        let exchange = try await coordinator.consumeCallback(
            callbackURL(code: "synthetic-private-authorization-code", state: state),
            sheetID: sheetID,
            attemptID: attempt.id,
            generation: attempt.generation
        )

        for value: Any in [attempt, exchange] {
            var dumped = ""
            dump(value, to: &dumped)
            let mirrorValues = Mirror(reflecting: value).children.map { String(reflecting: $0.value) }
            let outputs = [String(describing: value), String(reflecting: value), dumped] + mirrorValues
            for output in outputs {
                for secret in [state, nonce, exchange.authorizationCode, exchange.codeVerifier] {
                    XCTAssertFalse(output.contains(secret), "Authorization diagnostics must redact secret material")
                }
            }
        }
        // Redaction must preserve the values needed by the browser and token exchange.
        XCTAssertEqual(exchange.nonce, nonce)
        XCTAssertEqual(exchange.authorizationCode, "synthetic-private-authorization-code")
    }

    func testOversizedCallbackIsRejectedAndConsumesOnlyItsAttempt() async throws {
        let coordinator = CloudOAuthAuthorizationCoordinator()
        let sheetID = CloudOAuthSheetID(rawValue: uuid(7))
        let otherSheetID = CloudOAuthSheetID(rawValue: uuid(8))
        let attempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration())
        _ = try await coordinator.begin(sheetID: otherSheetID, configuration: configuration())
        let state = try XCTUnwrap(try queryValues(attempt.authorizationURL)["state"])
        var callback = URLComponents(url: try callbackURL(code: "synthetic-code", state: state), resolvingAgainstBaseURL: false)!
        // Every individual parameter is within the existing limit; the complete input is not.
        callback.queryItems! += ["scope", "session_state", "authuser", "prompt"].map {
            URLQueryItem(name: $0, value: String(repeating: "x", count: 16_384))
        }
        do {
            _ = try await coordinator.consumeCallback(
                try XCTUnwrap(callback.url),
                sheetID: sheetID,
                attemptID: attempt.id,
                generation: attempt.generation
            )
            XCTFail("Expected an oversized callback to be rejected")
        } catch {
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .invalidCallback)
        }
        let active = await coordinator.hasActiveAttempt(sheetID: sheetID)
        let otherActive = await coordinator.hasActiveAttempt(sheetID: otherSheetID)
        XCTAssertFalse(active)
        XCTAssertTrue(otherActive)
        await coordinator.cancel(sheetID: otherSheetID)
    }

    func testCallbackBoundPreservesMaximumLengthEncodedAuthorizationCode() async throws {
        let coordinator = CloudOAuthAuthorizationCoordinator()
        let sheetID = CloudOAuthSheetID(rawValue: uuid(9))
        let attempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration())
        let state = try XCTUnwrap(try queryValues(attempt.authorizationURL)["state"])
        let code = String(repeating: "+", count: 16_384)
        // Percent encoding expands the code to three times its decoded byte count.
        let callback = try XCTUnwrap(URL(string:
            "rune-synthetic://oauth/callback?code=\(String(repeating: "%2B", count: 16_384))&state=\(state)"
        ))
        let exchange = try await coordinator.consumeCallback(
            callback,
            sheetID: sheetID,
            attemptID: attempt.id,
            generation: attempt.generation
        )
        XCTAssertEqual(exchange.authorizationCode, code)
    }

    func testCancelInvalidatesCallbackAcrossSheetReopen() async throws {
        let random = SyntheticOAuthRandomBytes()
        let coordinator = CloudOAuthAuthorizationCoordinator(
            randomBytes: { count in try random.bytes(count: count) }
        )
        let sheetID = CloudOAuthSheetID(rawValue: uuid(3))
        let configuration = try configuration()
        let closedAttempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
        let closedState = try XCTUnwrap(try queryValues(closedAttempt.authorizationURL)["state"])
        await coordinator.cancel(sheetID: sheetID)
        let activeAfterCancel = await coordinator.hasActiveAttempt(sheetID: sheetID)
        XCTAssertFalse(activeAfterCancel)

        let reopenedAttempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
        XCTAssertGreaterThan(reopenedAttempt.generation, closedAttempt.generation)
        do {
            _ = try await coordinator.consumeCallback(
                callbackURL(code: "closed-code", state: closedState),
                sheetID: sheetID,
                attemptID: closedAttempt.id,
                generation: closedAttempt.generation
            )
            XCTFail("Expected the closed sheet callback to be superseded")
        } catch {
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .superseded)
        }
        let reopenedStillActive = await coordinator.hasActiveAttempt(sheetID: sheetID)
        XCTAssertTrue(reopenedStillActive)
    }

    func testStateMismatchMalformedCallbackAndProviderDenialConsumeAttempt() async throws {
        let random = SyntheticOAuthRandomBytes()
        let coordinator = CloudOAuthAuthorizationCoordinator(
            randomBytes: { count in try random.bytes(count: count) }
        )
        let sheetID = CloudOAuthSheetID(rawValue: uuid(4))
        let configuration = try configuration()

        let wrongStateAttempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
        do {
            _ = try await coordinator.consumeCallback(
                callbackURL(code: "synthetic-code", state: "forged-state"),
                sheetID: sheetID,
                attemptID: wrongStateAttempt.id,
                generation: wrongStateAttempt.generation
            )
            XCTFail("Expected state mismatch")
        } catch {
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .stateMismatch)
        }
        let activeAfterMismatch = await coordinator.hasActiveAttempt(sheetID: sheetID)
        XCTAssertFalse(activeAfterMismatch)

        let malformedAttempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
        let malformedState = try XCTUnwrap(try queryValues(malformedAttempt.authorizationURL)["state"])
        var malformed = URLComponents(string: "rune-synthetic://other/callback")!
        malformed.queryItems = [
            URLQueryItem(name: "code", value: "synthetic-code"),
            URLQueryItem(name: "state", value: malformedState)
        ]
        do {
            _ = try await coordinator.consumeCallback(
                try XCTUnwrap(malformed.url),
                sheetID: sheetID,
                attemptID: malformedAttempt.id,
                generation: malformedAttempt.generation
            )
            XCTFail("Expected callback base mismatch")
        } catch {
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .invalidCallback)
        }
        let activeAfterMalformedCallback = await coordinator.hasActiveAttempt(sheetID: sheetID)
        XCTAssertFalse(activeAfterMalformedCallback)

        let deniedAttempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
        let deniedState = try XCTUnwrap(try queryValues(deniedAttempt.authorizationURL)["state"])
        var denied = URLComponents(string: "rune-synthetic://oauth/callback")!
        denied.queryItems = [
            URLQueryItem(name: "error", value: "access_denied"),
            URLQueryItem(name: "error_description", value: "provider payload is not retained"),
            URLQueryItem(name: "state", value: deniedState)
        ]
        do {
            _ = try await coordinator.consumeCallback(
                try XCTUnwrap(denied.url),
                sheetID: sheetID,
                attemptID: deniedAttempt.id,
                generation: deniedAttempt.generation
            )
            XCTFail("Expected provider denial")
        } catch {
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .providerDenied)
            XCTAssertFalse(error.localizedDescription.contains("provider payload"))
            XCTAssertFalse(error.localizedDescription.contains("access_denied"))
        }
    }

    func testExpiredAttemptIsRemovedBeforeCallbackCanPublish() async throws {
        let random = SyntheticOAuthRandomBytes()
        let start = Date(timeIntervalSince1970: 1_800_000_000)
        let clock = SyntheticOAuthClock(start)
        let coordinator = CloudOAuthAuthorizationCoordinator(
            lifetime: 30,
            now: { clock.now() },
            randomBytes: { count in try random.bytes(count: count) }
        )
        let sheetID = CloudOAuthSheetID(rawValue: uuid(5))
        let attempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration())
        let state = try XCTUnwrap(try queryValues(attempt.authorizationURL)["state"])
        clock.set(start.addingTimeInterval(31))

        do {
            _ = try await coordinator.consumeCallback(
                callbackURL(code: "late-code", state: state),
                sheetID: sheetID,
                attemptID: attempt.id,
                generation: attempt.generation
            )
            XCTFail("Expected expired attempt")
        } catch {
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .expired)
        }
        let active = await coordinator.hasActiveAttempt(sheetID: sheetID)
        XCTAssertFalse(active)
    }

    func testConfigurationRejectsUnlistedOriginsUnsafeRedirectsAndInvalidIssuer() throws {
        XCTAssertThrowsError(try CloudOAuthAuthorizationConfiguration(
            provider: .azure,
            authorizationEndpoint: URL(string: "https://unlisted.example.invalid/authorize")!,
            clientID: "synthetic-public-client",
            redirectURI: URL(string: "rune-synthetic://oauth/callback")!,
            scopes: ["openid"],
            allowedAuthorizationHosts: ["identity.example.invalid"],
            allowedRedirectSchemes: ["rune-synthetic"],
            expectedIssuer: "https://identity.example.invalid/tenant",
            expectedAudience: "synthetic-public-client"
        )) { error in
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .invalidAuthorizationEndpoint)
        }

        XCTAssertThrowsError(try CloudOAuthAuthorizationConfiguration(
            provider: .azure,
            authorizationEndpoint: URL(string: "https://identity.example.invalid/authorize")!,
            clientID: "synthetic-public-client",
            redirectURI: URL(string: "https://callback.example.invalid/oauth")!,
            scopes: ["openid"],
            allowedAuthorizationHosts: ["identity.example.invalid"],
            expectedIssuer: "https://identity.example.invalid/tenant",
            expectedAudience: "synthetic-public-client"
        )) { error in
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .invalidRedirectURI)
        }

        XCTAssertThrowsError(try CloudOAuthAuthorizationConfiguration(
            provider: .azure,
            authorizationEndpoint: URL(string: "https://identity.example.invalid/authorize")!,
            clientID: "synthetic-public-client",
            redirectURI: URL(string: "http://remote.example.invalid/oauth")!,
            scopes: ["openid"],
            allowedAuthorizationHosts: ["identity.example.invalid"],
            expectedIssuer: "not-an-issuer",
            expectedAudience: "synthetic-public-client"
        )) { error in
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .invalidConfiguration)
        }
    }

    func testActiveAttemptLimitPreservesOtherSheetsAndAllowsReplacementAndReleasedSlots() async throws {
        let coordinator = CloudOAuthAuthorizationCoordinator(maximumActiveAttempts: 2)
        let firstSheet = CloudOAuthSheetID(rawValue: uuid(10))
        let secondSheet = CloudOAuthSheetID(rawValue: uuid(11))
        let thirdSheet = CloudOAuthSheetID(rawValue: uuid(12))
        let configuration = try configuration()
        let first = try await coordinator.begin(sheetID: firstSheet, configuration: configuration)
        _ = try await coordinator.begin(sheetID: secondSheet, configuration: configuration)
        do {
            _ = try await coordinator.begin(sheetID: thirdSheet, configuration: configuration)
            XCTFail("Concurrent authorization must remain bounded")
        } catch {
            XCTAssertEqual(error as? CloudOAuthAuthorizationError, .tooManyActiveAttempts)
        }
        let replacement = try await coordinator.begin(sheetID: firstSheet, configuration: configuration)
        XCTAssertGreaterThan(replacement.generation, first.generation)
        let secondActive = await coordinator.hasActiveAttempt(sheetID: secondSheet)
        XCTAssertTrue(secondActive)
        let state = try XCTUnwrap(try queryValues(replacement.authorizationURL)["state"])
        _ = try await coordinator.consumeCallback(
            callbackURL(code: "synthetic-code", state: state), sheetID: firstSheet,
            attemptID: replacement.id, generation: replacement.generation
        )
        _ = try await coordinator.begin(sheetID: thirdSheet, configuration: configuration)
        await coordinator.cancel(sheetID: secondSheet)
        _ = try await coordinator.begin(sheetID: firstSheet, configuration: configuration)
        await coordinator.cancel(sheetID: thirdSheet)
        await coordinator.cancel(sheetID: firstSheet)
    }

    func testExpiredAttemptsReleaseCapacityBeforeNextBegin() async throws {
        let start = Date(timeIntervalSince1970: 1_800_000_000)
        let clock = SyntheticOAuthClock(start)
        let coordinator = CloudOAuthAuthorizationCoordinator(
            lifetime: 30, maximumActiveAttempts: 1, now: { clock.now() }
        )
        let expiredSheet = CloudOAuthSheetID(rawValue: uuid(13))
        let currentSheet = CloudOAuthSheetID(rawValue: uuid(14))
        _ = try await coordinator.begin(sheetID: expiredSheet, configuration: configuration())
        clock.set(start.addingTimeInterval(31))
        _ = try await coordinator.begin(sheetID: currentSheet, configuration: configuration())
        let expiredActive = await coordinator.hasActiveAttempt(sheetID: expiredSheet)
        let currentActive = await coordinator.hasActiveAttempt(sheetID: currentSheet)
        XCTAssertFalse(expiredActive)
        XCTAssertTrue(currentActive)
        await coordinator.cancel(sheetID: currentSheet)
    }

    func testCanceledCallerCannotReplaceActiveAttempt() async throws {
        let coordinator = CloudOAuthAuthorizationCoordinator()
        let sheetID = CloudOAuthSheetID(rawValue: uuid(15))
        let configuration = try configuration()
        let current = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
        let canceled = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            do {
                _ = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
                return false
            } catch is CancellationError {
                return true
            } catch {
                return false
            }
        }
        let rejected = await canceled.value
        XCTAssertTrue(rejected)
        let state = try XCTUnwrap(try queryValues(current.authorizationURL)["state"])
        let exchange = try await coordinator.consumeCallback(
            callbackURL(code: "synthetic-code", state: state), sheetID: sheetID,
            attemptID: current.id, generation: current.generation
        )
        XCTAssertEqual(exchange.authorizationCode, "synthetic-code")
    }

    func testSheetChurnUsesFreshGenerationsAndRejectsCrossSheetCallbacksKPI() async throws {
        let coordinator = CloudOAuthAuthorizationCoordinator(maximumActiveAttempts: 1)
        let configuration = try configuration()
        let originalSheet = CloudOAuthSheetID()
        let original = try await coordinator.begin(sheetID: originalSheet, configuration: configuration)
        let originalState = try XCTUnwrap(try queryValues(original.authorizationURL)["state"])
        let staleCallback = try callbackURL(code: "stale-synthetic-code", state: originalState)
        await coordinator.cancel(sheetID: originalSheet)
        var previousGeneration = original.generation
        var rejectedCount = 0
        for _ in 0..<128 {
            let sheetID = CloudOAuthSheetID()
            let current = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
            XCTAssertGreaterThan(current.generation, previousGeneration)
            previousGeneration = current.generation
            do {
                _ = try await coordinator.consumeCallback(
                    staleCallback, sheetID: sheetID, attemptID: original.id, generation: original.generation
                )
            } catch CloudOAuthAuthorizationError.superseded {
                rejectedCount += 1
            }
            let active = await coordinator.hasActiveAttempt(sheetID: sheetID)
            XCTAssertTrue(active)
            await coordinator.cancel(sheetID: sheetID)
        }
        XCTAssertEqual(rejectedCount, 128, "KPI: 128/128 stale cross-sheet callbacks must be rejected")
    }

    private func configuration() throws -> CloudOAuthAuthorizationConfiguration {
        try CloudOAuthAuthorizationConfiguration(
            provider: .azure,
            authorizationEndpoint: URL(string: "https://identity.example.invalid/authorize")!,
            clientID: "synthetic-public-client",
            redirectURI: URL(string: "rune-synthetic://oauth/callback")!,
            scopes: ["openid", "offline_access", "cluster.read"],
            allowedAuthorizationHosts: ["identity.example.invalid"],
            allowedRedirectSchemes: ["rune-synthetic"],
            expectedIssuer: "https://identity.example.invalid/tenant",
            expectedAudience: "synthetic-public-client"
        )
    }

    private func queryValues(_ url: URL) throws -> [String: String] {
        let items = try XCTUnwrap(URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems)
        return Dictionary(uniqueKeysWithValues: items.compactMap { item in
            item.value.map { (item.name, $0) }
        })
    }

    private func callbackURL(code: String, state: String, issuer: String? = nil) throws -> URL {
        var components = URLComponents(string: "rune-synthetic://oauth/callback")!
        components.queryItems = [
            URLQueryItem(name: "code", value: code),
            URLQueryItem(name: "state", value: state)
        ]
        if let issuer {
            components.queryItems?.append(URLQueryItem(name: "iss", value: issuer))
        }
        return try XCTUnwrap(components.url)
    }

    private func base64URL(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private func uuid(_ suffix: UInt8) -> UUID {
        var bytes = [UInt8](repeating: 0, count: 16)
        bytes[15] = suffix
        return UUID(uuid: (
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15]
        ))
    }
}

private final class SyntheticOAuthRandomBytes: @unchecked Sendable {
    private let lock = NSLock()
    private var invocation: UInt8 = 0

    func bytes(count: Int) throws -> Data {
        lock.lock()
        defer { lock.unlock() }
        invocation &+= 1
        return Data((0..<count).map { UInt8($0 & 0xFF) &+ invocation })
    }
}

private final class SyntheticOAuthClock: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Date

    init(_ value: Date) {
        self.value = value
    }

    func now() -> Date {
        lock.lock()
        defer { lock.unlock() }
        return value
    }

    func set(_ value: Date) {
        lock.lock()
        self.value = value
        lock.unlock()
    }
}
