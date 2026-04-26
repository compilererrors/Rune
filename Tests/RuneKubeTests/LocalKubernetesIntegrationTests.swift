import XCTest

final class LocalKubernetesIntegrationTests: XCTestCase {
    func testNativeLocalIntegrationHarnessIsPending() throws {
        throw XCTSkip("Rebuild local integration tests on native fake Kubernetes REST fixtures; do not shell out to external tools.")
    }
}
