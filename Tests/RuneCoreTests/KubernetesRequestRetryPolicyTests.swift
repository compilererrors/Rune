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
}
