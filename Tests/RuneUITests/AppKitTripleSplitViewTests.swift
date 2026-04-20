import XCTest
@testable import RuneUI

final class AppKitTripleSplitViewTests: XCTestCase {
    func testWidthStateIgnoresStaleParentValuesAfterUserResize() {
        var state = AppKitTripleSplitWidthState()

        XCTAssertTrue(
            state.registerRequestedWidths(
                sidebarWidth: 280,
                detailWidth: 440,
                actualSidebarWidth: 280,
                actualDetailWidth: 440
            )
        )

        state.noteRestoreAttempt(containerWidth: 1440)
        state.noteRestoreSettled()
        state.noteUserResize(actualSidebarWidth: 360, actualDetailWidth: 520, containerWidth: 1440)

        XCTAssertFalse(
            state.registerRequestedWidths(
                sidebarWidth: 280,
                detailWidth: 440,
                actualSidebarWidth: 360,
                actualDetailWidth: 520
            )
        )
        XCTAssertEqual(state.desiredSidebarWidth, 360, accuracy: 0.5)
        XCTAssertEqual(state.desiredDetailWidth, 520, accuracy: 0.5)
    }

    func testWidthStateReappliesAfterWindowWidthChanges() {
        var state = AppKitTripleSplitWidthState()

        _ = state.registerRequestedWidths(
            sidebarWidth: 320,
            detailWidth: 500,
            actualSidebarWidth: 320,
            actualDetailWidth: 500
        )
        state.noteRestoreAttempt(containerWidth: 1440)
        state.noteRestoreSettled()

        XCTAssertFalse(state.shouldApplyOnLayout(containerWidth: 1440))
        XCTAssertTrue(state.shouldApplyOnLayout(containerWidth: 1280))
    }
}
