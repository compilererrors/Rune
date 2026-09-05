import AppKit
import SwiftUI
import XCTest
@testable import RuneUI
import RuneSecurity

@MainActor
final class RuneOperationFormLayoutTests: XCTestCase {
    func testReplicaControlsWrapLongLabelWithoutHorizontalScrolling() {
        let view = WorkloadReplicaScaleControlsView(
            label: "Desired number of deployment replicas",
            isDirty: true,
            canMutate: true,
            replicas: .constant(3),
            action: {}
        )
        let compact = fittingSize(view, width: 240)
        let wide = fittingSize(view, width: 760)
        XCTAssertEqual(compact.width, 240, accuracy: 1)
        XCTAssertGreaterThan(compact.height, wide.height + 10)
    }

    func testHelmRollbackOptionsKeepTimeoutVisibleAtCompactAndAccessibilitySizes() {
        let view = HelmRollbackOptionsView(
            wait: .constant(true),
            timeout: .constant("5m"),
            cleanupOnFail: .constant(false)
        )
        let compact = fittingSize(view, width: 240)
        let wide = fittingSize(view, width: 760)
        let enlarged = fittingSize(view.environment(\.dynamicTypeSize, .accessibility3), width: 320)
        XCTAssertEqual(compact.width, 240, accuracy: 1)
        XCTAssertEqual(enlarged.width, 320, accuracy: 1)
        XCTAssertGreaterThan(compact.height, wide.height + 10)
        XCTAssertGreaterThan(enlarged.height, wide.height + 10)
    }

    func testContextRemovalKeepsLongFileReviewAndErrorWithinBoundedSheet() {
        let preview = KubeConfigContextRemovalPreview(
            contextName: "synthetic-context-with-a-long-display-name",
            affectedSourceDisplayNames: (0..<30).map { "synthetic-imported-configuration-\($0).yaml" },
            removedClusterCount: 1,
            removedUserCount: 1
        )
        for width in [420.0, 560.0] {
            for textSize in [DynamicTypeSize.large, .accessibility3] {
                let view = KubeConfigContextRemovalSheet(
                    preview: preview,
                    isRemoving: false,
                    errorMessage: "The reviewed configuration changed. Close this dialog and review the context again.",
                    onConfirm: {},
                    onCancel: {}
                )
                .environment(\.dynamicTypeSize, textSize)
                .environment(\.runeInterfaceFontSize, 16)
                let size = fittingSize(view, width: width)
                XCTAssertEqual(size.width, width, accuracy: 1)
                XCTAssertLessThanOrEqual(size.height, RuneUILayoutMetrics.providerDialogMaxHeight + 1)
            }
        }
    }

    private func fittingSize(_ view: some View, width: CGFloat) -> CGSize {
        let host = NSHostingView(rootView: view.frame(width: width))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }
}
