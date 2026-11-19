import XCTest
@testable import RuneCore

final class KubernetesRequestRetryPolicyTests: XCTestCase {
    func testSafeRetryMethodsAllowReadsButRejectMutations() {
        XCTAssertTrue(KubernetesRequestRetryPolicy.isSafeRetryMethod("GET"))
        XCTAssertTrue(KubernetesRequestRetryPolicy.isSafeRetryMethod("head"))
        XCTAssertFalse(KubernetesRequestRetryPolicy.isSafeRetryMethod("PATCH"))
        XCTAssertFalse(KubernetesRequestRetryPolicy.isSafeRetryMethod("POST"))
        XCTAssertFalse(KubernetesRequestRetryPolicy.isSafeRetryMethod("DELETE"))
    }

    func testRetryAfterFromTheServerNeverTraps() {
        // Each of these parses as a number; converting it unbounded to
        // nanoseconds ends the process.
        for header in ["inf", "infinity", "1e30", "1e400", "99999999999999999999"] {
            let decision = KubernetesRequestRetryPolicy.classifyHTTPStatus(429, retryAfterHeader: header)
            XCTAssertTrue(decision.isRetryable, header)
            XCTAssertLessThanOrEqual(
                KubernetesRequestRetryPolicy.boundedDelayNanoseconds(for: decision, attempt: 1),
                2_000_000_000,
                header
            )
        }
        for header in ["nan", "-1", "soon", "", "Wed, 21 Oct 2026 07:28:00 GMT"] {
            XCTAssertNil(
                KubernetesRequestRetryPolicy.classifyHTTPStatus(503, retryAfterHeader: header).suggestedDelayNanoseconds,
                header
            )
        }
        XCTAssertEqual(
            KubernetesRequestRetryPolicy.classifyHTTPStatus(429, retryAfterHeader: " 1.5 ").suggestedDelayNanoseconds,
            1_500_000_000
        )
    }
}
