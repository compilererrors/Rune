import XCTest
@testable import RuneCore

final class RuneCoreTests: XCTestCase {
    func testLogTimeFilterUsesSinceTimeOnlyForAbsoluteDate() {
        XCTAssertFalse(LogTimeFilter.lastMinutes(15).usesSinceTime)
        XCTAssertFalse(LogTimeFilter.lastHours(1).usesSinceTime)
        XCTAssertTrue(LogTimeFilter.since(Date(timeIntervalSince1970: 0)).usesSinceTime)
    }

    func testKubeConfigSourceUsesPathAsIdentifier() {
        let source = KubeConfigSource(url: URL(fileURLWithPath: "/tmp/kubeconfig"))

        XCTAssertEqual(source.id, "/tmp/kubeconfig")
        XCTAssertEqual(source.displayName, "kubeconfig")
    }
}
