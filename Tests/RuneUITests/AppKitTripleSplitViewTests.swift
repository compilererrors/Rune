import XCTest
@testable import RuneUI

@MainActor
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
        XCTAssertTrue(
            state.noteUserResize(actualSidebarWidth: 360, actualDetailWidth: 520, containerWidth: 1440)
        )

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

        XCTAssertFalse(
            state.shouldApplyOnLayout(
                containerWidth: 1440,
                actualSidebarWidth: 320,
                actualDetailWidth: 500
            )
        )
        XCTAssertTrue(
            state.shouldApplyOnLayout(
                containerWidth: 1280,
                actualSidebarWidth: 320,
                actualDetailWidth: 500
            )
        )
    }

    func testWidthStateReappliesAfterProgrammaticContentChangesDriftSplitWidths() {
        var state = AppKitTripleSplitWidthState()

        _ = state.registerRequestedWidths(
            sidebarWidth: 280,
            detailWidth: 440,
            actualSidebarWidth: 280,
            actualDetailWidth: 440
        )
        state.noteRestoreAttempt(containerWidth: 1440)
        state.noteRestoreSettled()

        state.noteProgrammaticContentUpdate()

        XCTAssertTrue(
            state.shouldApplyOnLayout(
                containerWidth: 1440,
                actualSidebarWidth: 360,
                actualDetailWidth: 440
            ),
            "Programmatic SwiftUI content updates must reapply desired split widths instead of accepting layout drift as user resize."
        )
        XCTAssertEqual(state.desiredSidebarWidth, 280, accuracy: 0.5)
        XCTAssertEqual(state.desiredDetailWidth, 440, accuracy: 0.5)
    }

    func testWidthStateDoesNotPersistWindowResizeAsPaneResize() {
        var state = AppKitTripleSplitWidthState()

        _ = state.registerRequestedWidths(
            sidebarWidth: 360,
            detailWidth: 520,
            actualSidebarWidth: 360,
            actualDetailWidth: 520
        )
        state.noteRestoreAttempt(containerWidth: 1440)
        state.noteRestoreSettled()

        XCTAssertFalse(
            state.noteUserResize(
                actualSidebarWidth: 280,
                actualDetailWidth: 440,
                containerWidth: 1200
            ),
            "Window-size changes can temporarily squeeze panes, but must not overwrite the user's saved divider widths."
        )
        XCTAssertEqual(state.desiredSidebarWidth, 360, accuracy: 0.5)
        XCTAssertEqual(state.desiredDetailWidth, 520, accuracy: 0.5)
    }
}
