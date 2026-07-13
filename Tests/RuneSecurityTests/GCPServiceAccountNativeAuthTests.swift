import Foundation
@testable import RuneSecurity
@preconcurrency import Security
import XCTest

final class GCPServiceAccountNativeAuthTests: XCTestCase {
    func testStrictServiceAccountModelParsesSyntheticCredential() throws {
        let fixture = try makeFixture()
        let credential = try GCPServiceAccountCredential(jsonData: fixture.json)

        XCTAssertEqual(credential.clientEmail, "rune-test@synthetic-project.iam.gserviceaccount.com")
        XCTAssertEqual(credential.tokenURI.absoluteString, "https://oauth2.googleapis.com/token")
        XCTAssertEqual(credential.description, "GCPServiceAccountCredential(<redacted>)")
        XCTAssertFalse(credential.description.contains("synthetic-project"))
        XCTAssertEqual(
            GCPServiceAccountCredential.requiredScopes,
            [
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/userinfo.email"
            ]
        )
    }

    func testOtherADCCredentialTypesFailClosed() throws {
        for type in ["authorized_user", "external_account", "impersonated_service_account"] {
            let json = try JSONSerialization.data(withJSONObject: [
                "type": type,
                "client_email": "rune-test@synthetic-project.iam.gserviceaccount.com",
                "private_key": "not-used",
                "token_uri": "https://oauth2.googleapis.com/token"
            ])
            XCTAssertThrowsError(try GCPServiceAccountCredential(jsonData: json)) {
                XCTAssertEqual($0 as? GCPServiceAccountAuthError, .unsupportedCredentialType)
            }
        }
    }

    func testCredentialValidatesRequiredFieldsKeyAndPinnedHTTPSEndpoint() throws {
        let fixture = try makeFixture()
        let document = try XCTUnwrap(JSONSerialization.jsonObject(with: fixture.json) as? [String: Any])

        var missingEmail = document
        missingEmail.removeValue(forKey: "client_email")
        XCTAssertThrowsError(try GCPServiceAccountCredential(
            jsonData: JSONSerialization.data(withJSONObject: missingEmail)
        )) {
            XCTAssertEqual($0 as? GCPServiceAccountAuthError, .missingRequiredField("client_email"))
        }

        var invalidKey = document
        invalidKey["private_key"] = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----"
        XCTAssertThrowsError(try GCPServiceAccountCredential(
            jsonData: JSONSerialization.data(withJSONObject: invalidKey)
        )) {
            XCTAssertEqual($0 as? GCPServiceAccountAuthError, .invalidPrivateKey)
        }

        for tokenURI in [
            "http://oauth2.googleapis.com/token",
            "https://synthetic.invalid/token",
            "https://oauth2.googleapis.com/other",
            "https://oauth2.googleapis.com/token?redirect=synthetic"
        ] {
            var invalidEndpoint = document
            invalidEndpoint["token_uri"] = tokenURI
            XCTAssertThrowsError(try GCPServiceAccountCredential(
                jsonData: JSONSerialization.data(withJSONObject: invalidEndpoint)
            )) {
                XCTAssertEqual($0 as? GCPServiceAccountAuthError, .invalidTokenURI)
            }
        }
    }

    func testJWTAssertionHasRequiredClaimsAndValidRS256Signature() async throws {
        let fixture = try makeFixture()
        let httpClient = RecordingGCPServiceAccountHTTPClient(response: tokenResponse())
        let date = try fixedDate()
        let provider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: fixture.json,
            httpClient: httpClient,
            now: { date }
        )

        let token = try await provider.accessToken()
        XCTAssertEqual(token.value, "synthetic-access-token")
        XCTAssertEqual(token.expiration, date.addingTimeInterval(3_600))
        XCTAssertEqual(token.description, "GCPServiceAccountAccessToken(<redacted>)")

        let requests = await httpClient.requests()
        let request = try XCTUnwrap(requests.first)
        XCTAssertEqual(request.url.absoluteString, "https://oauth2.googleapis.com/token")
        XCTAssertEqual(request.method, "POST")
        XCTAssertEqual(request.headers["Content-Type"], "application/x-www-form-urlencoded")
        let form = try decodedForm(request.body)
        XCTAssertEqual(form["grant_type"], "urn:ietf:params:oauth:grant-type:jwt-bearer")
        let assertion = try XCTUnwrap(form["assertion"])
        let parts = assertion.split(separator: ".", omittingEmptySubsequences: false).map(String.init)
        XCTAssertEqual(parts.count, 3)

        let header = try XCTUnwrap(JSONSerialization.jsonObject(with: decodeRawURLBase64(parts[0])) as? [String: Any])
        let claims = try XCTUnwrap(JSONSerialization.jsonObject(with: decodeRawURLBase64(parts[1])) as? [String: Any])
        XCTAssertEqual(header["alg"] as? String, "RS256")
        XCTAssertEqual(header["typ"] as? String, "JWT")
        XCTAssertEqual(claims["iss"] as? String, "rune-test@synthetic-project.iam.gserviceaccount.com")
        XCTAssertEqual(claims["aud"] as? String, "https://oauth2.googleapis.com/token")
        XCTAssertEqual(
            claims["scope"] as? String,
            "https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/userinfo.email"
        )
        let issuedAt = try XCTUnwrap(claims["iat"] as? NSNumber).intValue
        let expiresAt = try XCTUnwrap(claims["exp"] as? NSNumber).intValue
        XCTAssertEqual(expiresAt - issuedAt, 3_600)

        let signature = try decodeRawURLBase64(parts[2])
        let signingInput = Data((parts[0] + "." + parts[1]).utf8)
        var verificationError: Unmanaged<CFError>?
        XCTAssertTrue(SecKeyVerifySignature(
            fixture.publicKey,
            .rsaSignatureMessagePKCS1v15SHA256,
            signingInput as CFData,
            signature as CFData,
            &verificationError
        ))
    }

    func testTokenResponseValidationAndSanitizedEndpointRejection() async throws {
        let fixture = try makeFixture()
        let secretDiagnostic = "synthetic-sensitive-provider-diagnostic"
        let rejected = RecordingGCPServiceAccountHTTPClient(response: GCPServiceAccountHTTPResponse(
            statusCode: 401,
            body: Data("{\"error_description\":\"\(secretDiagnostic)\"}".utf8)
        ))
        let rejectedProvider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: fixture.json,
            httpClient: rejected
        )
        do {
            _ = try await rejectedProvider.accessToken()
            XCTFail("Expected OAuth rejection")
        } catch {
            XCTAssertEqual(error as? GCPServiceAccountAuthError, .tokenEndpointRejected(401))
            XCTAssertFalse(error.localizedDescription.contains(secretDiagnostic))
        }

        let malformed = RecordingGCPServiceAccountHTTPClient(response: GCPServiceAccountHTTPResponse(
            statusCode: 200,
            body: Data("{\"access_token\":\"\",\"expires_in\":3600}".utf8)
        ))
        let malformedProvider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: fixture.json,
            httpClient: malformed
        )
        do {
            _ = try await malformedProvider.accessToken()
            XCTFail("Expected invalid token response")
        } catch {
            XCTAssertEqual(error as? GCPServiceAccountAuthError, .invalidTokenResponse)
        }
    }

    func testResponseSizeAndTokenTypeAreValidated() async throws {
        let fixture = try makeFixture()
        let oversized = RecordingGCPServiceAccountHTTPClient(response: GCPServiceAccountHTTPResponse(
            statusCode: 200,
            body: Data(repeating: 65, count: 1_048_577)
        ))
        let oversizedProvider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: fixture.json,
            httpClient: oversized
        )
        do {
            _ = try await oversizedProvider.accessToken()
            XCTFail("Expected response size failure")
        } catch {
            XCTAssertEqual(error as? GCPServiceAccountAuthError, .responseTooLarge)
        }

        let wrongType = RecordingGCPServiceAccountHTTPClient(response: GCPServiceAccountHTTPResponse(
            statusCode: 200,
            body: Data("{\"access_token\":\"synthetic\",\"expires_in\":3600,\"token_type\":\"MAC\"}".utf8)
        ))
        let wrongTypeProvider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: fixture.json,
            httpClient: wrongType
        )
        do {
            _ = try await wrongTypeProvider.accessToken()
            XCTFail("Expected token type failure")
        } catch {
            XCTAssertEqual(error as? GCPServiceAccountAuthError, .invalidTokenResponse)
        }
    }

    func testAccessTokenCacheAndInvalidation() async throws {
        let fixture = try makeFixture()
        let date = try fixedDate()
        let httpClient = RecordingGCPServiceAccountHTTPClient(response: tokenResponse())
        let provider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: fixture.json,
            httpClient: httpClient,
            now: { date }
        )

        let first = try await provider.accessToken()
        let second = try await provider.accessToken()
        XCTAssertEqual(first, second)
        let cachedRequestCount = await httpClient.requestCount()
        XCTAssertEqual(cachedRequestCount, 1)

        await provider.invalidate()
        _ = try await provider.accessToken()
        let refreshedRequestCount = await httpClient.requestCount()
        XCTAssertEqual(refreshedRequestCount, 2)
    }

    func testConcurrentRefreshUsesSingleflight() async throws {
        let fixture = try makeFixture()
        let date = try fixedDate()
        let httpClient = RecordingGCPServiceAccountHTTPClient(
            response: tokenResponse(),
            delay: .milliseconds(30)
        )
        let provider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: fixture.json,
            httpClient: httpClient,
            now: { date }
        )

        let tokens = try await withThrowingTaskGroup(of: GCPServiceAccountAccessToken.self) { group in
            for _ in 0..<32 {
                group.addTask { try await provider.accessToken() }
            }
            var values: [GCPServiceAccountAccessToken] = []
            for try await token in group {
                values.append(token)
            }
            return values
        }

        XCTAssertEqual(tokens.count, 32)
        XCTAssertEqual(Set(tokens.map(\.value)), ["synthetic-access-token"])
        let requestCount = await httpClient.requestCount()
        XCTAssertEqual(requestCount, 1)
    }

    func testNetworkErrorsAreSanitized() async throws {
        let fixture = try makeFixture()
        let client = FailingGCPServiceAccountHTTPClient()
        let provider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: fixture.json,
            httpClient: client
        )

        do {
            _ = try await provider.accessToken()
            XCTFail("Expected network failure")
        } catch {
            XCTAssertEqual(error as? GCPServiceAccountAuthError, .networkFailure)
            XCTAssertFalse(error.localizedDescription.contains("synthetic-network-secret"))
        }
    }

    func testKPICachedTokenResolvesOneThousandTimesWithinTwoSeconds() async throws {
        let fixture = try makeFixture()
        let date = try fixedDate()
        let httpClient = RecordingGCPServiceAccountHTTPClient(response: tokenResponse())
        let provider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: fixture.json,
            httpClient: httpClient,
            now: { date }
        )
        _ = try await provider.accessToken()
        let start = ContinuousClock.now
        var totalBytes = 0

        for _ in 0..<1_000 {
            totalBytes += try await provider.accessToken().value.utf8.count
        }

        let elapsed = start.duration(to: .now)
        XCTAssertGreaterThan(totalBytes, 0)
        let requestCount = await httpClient.requestCount()
        XCTAssertEqual(requestCount, 1)
        XCTAssertLessThan(elapsed, .seconds(2), "GCP native auth cache KPI exceeded two seconds")
    }

    private func tokenResponse() -> GCPServiceAccountHTTPResponse {
        GCPServiceAccountHTTPResponse(
            statusCode: 200,
            headers: ["Content-Type": "application/json"],
            body: Data("{\"access_token\":\"synthetic-access-token\",\"expires_in\":3600,\"token_type\":\"Bearer\"}".utf8)
        )
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

    private func decodedForm(_ data: Data) throws -> [String: String] {
        let body = try XCTUnwrap(String(data: data, encoding: .utf8))
        return try Dictionary(uniqueKeysWithValues: body.split(separator: "&").map { item in
            let pieces = item.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
            let name = try XCTUnwrap(String(pieces[0]).removingPercentEncoding)
            let value = try XCTUnwrap(String(pieces[1]).removingPercentEncoding)
            return (name, value)
        })
    }

    private func decodeRawURLBase64(_ value: String) throws -> Data {
        var encoded = value
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        encoded.append(String(repeating: "=", count: (4 - encoded.count % 4) % 4))
        return try XCTUnwrap(Data(base64Encoded: encoded))
    }

    private func makeFixture() throws -> ServiceAccountFixture {
        let attributes: [CFString: Any] = [
            kSecAttrKeyType: kSecAttrKeyTypeRSA,
            kSecAttrKeySizeInBits: 2_048,
            kSecAttrIsPermanent: false
        ]
        var keyError: Unmanaged<CFError>?
        let privateKey = try XCTUnwrap(SecKeyCreateRandomKey(attributes as CFDictionary, &keyError))
        let publicKey = try XCTUnwrap(SecKeyCopyPublicKey(privateKey))
        var exportError: Unmanaged<CFError>?
        let pkcs1 = try XCTUnwrap(SecKeyCopyExternalRepresentation(privateKey, &exportError) as Data?)
        let pkcs8 = derElement(tag: 0x30, value:
            Data([0x02, 0x01, 0x00])
            + Data([0x30, 0x0D, 0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01, 0x05, 0x00])
            + derElement(tag: 0x04, value: pkcs1)
        )
        let encoded = pkcs8.base64EncodedString()
        let lines = stride(from: 0, to: encoded.count, by: 64).map { offset -> String in
            let start = encoded.index(encoded.startIndex, offsetBy: offset)
            let end = encoded.index(start, offsetBy: min(64, encoded.count - offset))
            return String(encoded[start..<end])
        }
        let pem = (["-----BEGIN PRIVATE KEY-----"] + lines + ["-----END PRIVATE KEY-----"])
            .joined(separator: "\n")
        let json = try JSONSerialization.data(withJSONObject: [
            "type": "service_account",
            "project_id": "synthetic-project",
            "private_key_id": "synthetic-key-id",
            "private_key": pem,
            "client_email": "rune-test@synthetic-project.iam.gserviceaccount.com",
            "client_id": "000000000000000000000",
            "token_uri": "https://oauth2.googleapis.com/token"
        ], options: [.sortedKeys])
        return ServiceAccountFixture(json: json, publicKey: publicKey)
    }

    private func derElement(tag: UInt8, value: Data) -> Data {
        var output = Data([tag])
        if value.count < 128 {
            output.append(UInt8(value.count))
        } else {
            var bytes: [UInt8] = []
            var length = value.count
            while length > 0 {
                bytes.insert(UInt8(length & 0xFF), at: 0)
                length >>= 8
            }
            output.append(0x80 | UInt8(bytes.count))
            output.append(contentsOf: bytes)
        }
        output.append(value)
        return output
    }
}

private struct ServiceAccountFixture {
    let json: Data
    let publicKey: SecKey
}

private actor RecordingGCPServiceAccountHTTPClient: GCPServiceAccountHTTPClient {
    private let response: GCPServiceAccountHTTPResponse
    private let delay: Duration?
    private var recordedRequests: [GCPServiceAccountHTTPRequest] = []

    init(response: GCPServiceAccountHTTPResponse, delay: Duration? = nil) {
        self.response = response
        self.delay = delay
    }

    func send(_ request: GCPServiceAccountHTTPRequest) async throws -> GCPServiceAccountHTTPResponse {
        recordedRequests.append(request)
        if let delay {
            try await Task.sleep(for: delay)
        }
        return response
    }

    func requests() -> [GCPServiceAccountHTTPRequest] { recordedRequests }
    func requestCount() -> Int { recordedRequests.count }
}

private struct FailingGCPServiceAccountHTTPClient: GCPServiceAccountHTTPClient {
    func send(_: GCPServiceAccountHTTPRequest) async throws -> GCPServiceAccountHTTPResponse {
        throw SyntheticNetworkError(message: "synthetic-network-secret")
    }

    private struct SyntheticNetworkError: Error {
        let message: String
    }
}
