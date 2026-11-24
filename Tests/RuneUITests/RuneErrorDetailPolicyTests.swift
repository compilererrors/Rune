import XCTest
@testable import RuneCore
@testable import RuneUI

final class RuneErrorDetailPolicyTests: XCTestCase {
    func testShortSingleLineMessageShowsWithoutFolding() {
        XCTAssertFalse(RuneErrorDetailPolicy.isFoldable("HTTP 404: pods \"alpha\" not found"))
    }

    func testLongOrMultilineMessageFoldsToItsFirstLine() {
        let long = String(repeating: "x", count: RuneErrorDetailPolicy.foldingCharacterCount + 1)
        XCTAssertTrue(RuneErrorDetailPolicy.isFoldable(long))

        let multiline = "HTTP 500: failed to create typed patch object\nexpected string, got &value.valueUnstructured{Value:1}"
        XCTAssertTrue(RuneErrorDetailPolicy.isFoldable(multiline))
        XCTAssertEqual(RuneErrorDetailPolicy.firstLine(multiline), "HTTP 500: failed to create typed patch object")
    }

    func testAsksOnceAfterTheThirdExpand() {
        XCTAssertFalse(RuneErrorDetailPolicy.shouldAskToAlwaysExpand(expandCount: 2, alreadyAsked: false, alwaysExpanded: false))
        XCTAssertTrue(RuneErrorDetailPolicy.shouldAskToAlwaysExpand(expandCount: 3, alreadyAsked: false, alwaysExpanded: false))
        XCTAssertFalse(RuneErrorDetailPolicy.shouldAskToAlwaysExpand(expandCount: 4, alreadyAsked: true, alwaysExpanded: false))
        XCTAssertFalse(RuneErrorDetailPolicy.shouldAskToAlwaysExpand(expandCount: 3, alreadyAsked: false, alwaysExpanded: true))
    }

    func testHealthCheckDropsAnEmptyDetail() {
        let check = RuneHealthCheck(id: "x", title: "X", status: .warning, message: "Rune's line", detail: "  \n")
        XCTAssertNil(check.detail)
        XCTAssertEqual(check.withDetail(" HTTP 403: denied ").detail, "HTTP 403: denied")
    }
}

final class RuneNoticeBannerMessageTests: XCTestCase {
    func testPartialLoadPutsEachResourceErrorOnItsOwnLine() {
        XCTAssertEqual(
            RuneNoticeBanner.readableErrorMessage("Partial load: pods: HTTP 403: forbidden | events: HTTP 403: forbidden"),
            "Partial load: pods: HTTP 403: forbidden\nevents: HTTP 403: forbidden"
        )
    }
}
