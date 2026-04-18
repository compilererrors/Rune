import RuneStore
import XCTest

final class NamespaceListOrderingTests: XCTestCase {
    func testMergePreservesPreviousOrderThenAppendsSortedNew() {
        let merged = NamespaceListOrdering.merge(
            previousOrder: ["zoo", "apple", "middle"],
            apiNames: ["new1", "middle", "apple", "zoo", "aaa"]
        )
        XCTAssertEqual(merged, ["zoo", "apple", "middle", "aaa", "new1"])
    }

    func testMergeEmptyPreviousUsesSortedApiOnly() {
        let merged = NamespaceListOrdering.merge(previousOrder: [], apiNames: ["b", "a"])
        XCTAssertEqual(merged, ["a", "b"])
    }
}
