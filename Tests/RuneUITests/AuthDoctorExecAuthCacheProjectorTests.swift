import Foundation
import XCTest
@testable import RuneCore
@testable import RuneKube
@testable import RuneUI

final class AuthDoctorExecAuthCacheProjectorTests: XCTestCase {
    func testProjectsCacheHitWithExpiration() throws {
        let expiresAt = try XCTUnwrap(ISO8601DateFormatter().date(from: "2030-01-02T03:04:05Z"))
        let check = AuthDoctorExecAuthCacheProjector.check(for: KubernetesExecCredentialCacheDiagnostic(
            state: .hit,
            expiresAt: expiresAt
        ))

        XCTAssertEqual(check.id, "exec-auth-cache")
        XCTAssertEqual(check.status, .passed)
        XCTAssertTrue(check.message.contains("cache hit"))
        XCTAssertTrue(check.message.contains("2030-01-02T03:04:05Z"))
    }

    func testProjectsCacheMissWithoutSensitiveDetails() {
        let check = AuthDoctorExecAuthCacheProjector.check(for: KubernetesExecCredentialCacheDiagnostic(
            state: .miss,
            expiresAt: nil
        ))

        XCTAssertEqual(check.status, .passed)
        XCTAssertTrue(check.message.contains("no reusable bearer token is cached"))
        XCTAssertFalse(check.message.contains("--"))
        XCTAssertFalse(check.message.localizedCaseInsensitiveContains("token="))
    }

    func testProjectsExpiredCacheAsWarning() throws {
        let expiresAt = try XCTUnwrap(ISO8601DateFormatter().date(from: "2029-12-31T23:59:59Z"))
        let check = AuthDoctorExecAuthCacheProjector.check(for: KubernetesExecCredentialCacheDiagnostic(
            state: .expired,
            expiresAt: expiresAt
        ))

        XCTAssertEqual(check.status, .warning)
        XCTAssertTrue(check.message.contains("rerun the exec auth plugin"))
        XCTAssertTrue(check.message.contains("2029-12-31T23:59:59Z"))
    }
}
