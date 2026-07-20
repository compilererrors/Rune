import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RuneHeaderCapsuleTests: XCTestCase {
    func testRolesUseSemanticTruncationAndAccessibilityTitles() {
        XCTAssertTrue(RuneHeaderCapsuleRole.context.prefersMiddleTruncation)
        XCTAssertTrue(RuneHeaderCapsuleRole.value.prefersMiddleTruncation)
        XCTAssertFalse(RuneHeaderCapsuleRole.status.prefersMiddleTruncation)

        XCTAssertEqual(RuneHeaderCapsuleRole.context.accessibilityTitle, "Context")
        XCTAssertEqual(RuneHeaderCapsuleRole.status.accessibilityTitle, "Status")
        XCTAssertEqual(RuneHeaderCapsuleRole.value.accessibilityTitle, "Value")
    }

    @MainActor
    func testDefaultCapsuleMeetsMinimumWithoutForcingExtraHeight() {
        let host = NSHostingView(rootView: RuneHeaderCapsule(
            "synthetic-context",
            role: .context,
            systemImage: "globe"
        ))

        host.layoutSubtreeIfNeeded()
        let size = host.fittingSize

        XCTAssertGreaterThanOrEqual(size.height, RuneUILayoutMetrics.headerCapsuleMinimumHeight)
        XCTAssertLessThanOrEqual(size.height, RuneUILayoutMetrics.headerCapsuleMinimumHeight + 4)
        XCTAssertGreaterThan(size.width, RuneUILayoutMetrics.headerCapsuleHorizontalPadding * 2)
    }

    @MainActor
    func testAccessibilityTextCanGrowCapsuleBeyondMinimumHeight() {
        let defaultHost = NSHostingView(rootView: constrainedContextCapsule(dynamicTypeSize: .large))
        let accessibilityHost = NSHostingView(
            rootView: constrainedContextCapsule(dynamicTypeSize: .accessibility3)
        )

        defaultHost.layoutSubtreeIfNeeded()
        accessibilityHost.layoutSubtreeIfNeeded()
        let defaultSize = defaultHost.fittingSize
        let accessibilitySize = accessibilityHost.fittingSize

        XCTAssertEqual(defaultSize.width, 180, accuracy: 0.5)
        XCTAssertEqual(accessibilitySize.width, 180, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(defaultSize.height, RuneUILayoutMetrics.headerCapsuleMinimumHeight)
        XCTAssertGreaterThan(accessibilitySize.height, RuneUILayoutMetrics.headerCapsuleMinimumHeight)
        XCTAssertGreaterThan(accessibilitySize.height, defaultSize.height)
    }

    @MainActor
    private func constrainedContextCapsule(
        dynamicTypeSize: DynamicTypeSize
    ) -> some View {
        RuneHeaderCapsule(
            "synthetic-context-with-a-long-distinguishing-suffix",
            role: .context,
            systemImage: "globe"
        )
        .frame(width: 180)
        .environment(\.dynamicTypeSize, dynamicTypeSize)
    }
}
