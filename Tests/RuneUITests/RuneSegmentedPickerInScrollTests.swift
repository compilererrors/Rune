import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RuneSegmentedPickerInScrollTests: XCTestCase {
    @MainActor
    func testLongSegmentGroupStaysInsideCompactInspectorWidth() {
        let host = NSHostingView(rootView: RuneSegmentedPickerInScroll(
            "Inspector",
            selection: .constant(0),
            labelsHidden: true
        ) {
            ForEach(0..<8, id: \.self) { index in
                Text("Long choice \(index)").tag(index)
            }
        }
        .frame(width: 320))

        host.layoutSubtreeIfNeeded()
        XCTAssertEqual(host.fittingSize.width, 320, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(host.fittingSize.height, 20)
    }

    @MainActor
    func testIntrinsicOverflowPolicyReportsFullSegmentWidthForAdaptiveFallback() {
        let host = NSHostingView(rootView: RuneSegmentedPickerInScroll(
            "Resource type",
            selection: .constant(0),
            labelsHidden: true,
            overflowBehavior: .intrinsic
        ) {
            ForEach(0..<8, id: \.self) { index in
                Text("Long resource type \(index)").tag(index)
            }
        })

        host.layoutSubtreeIfNeeded()
        XCTAssertGreaterThan(
            host.fittingSize.width,
            320,
            "Intrinsic mode must let ViewThatFits reject an overflowing segmented control and choose its compact menu fallback."
        )
    }

    @MainActor
    func testAdaptivePickerHostsSegmentsWideAndMenuAtCompactWidth() {
        let wideHost = adaptivePickerHost(width: 1_100)
        XCTAssertNotNil(
            firstVisibleSubview(of: NSSegmentedControl.self, in: wideHost),
            "A wide resource-family rail should host the full segmented control."
        )
        XCTAssertNil(
            firstVisibleSubview(of: NSPopUpButton.self, in: wideHost),
            "The compact menu should not coexist with the fitting segmented control."
        )

        let compactHost = adaptivePickerHost(width: 240)
        XCTAssertNil(
            firstVisibleSubview(of: NSSegmentedControl.self, in: compactHost),
            "An overflowing segmented control must leave the hosted hierarchy at compact width."
        )
        XCTAssertNotNil(
            firstVisibleSubview(of: NSPopUpButton.self, in: compactHost),
            "The compact resource-family rail should host a native menu picker."
        )
    }

    func testOverflowIsDiscoverableWithoutArtificialLeadingDrift() throws {
        let source = try String(contentsOfFile: sourcePath, encoding: .utf8)

        XCTAssertTrue(source.contains("ScrollView(.horizontal, showsIndicators: true)"))
        XCTAssertTrue(source.contains("Scroll horizontally to reveal additional choices"))
        XCTAssertTrue(source.contains("case .intrinsic:"))
        XCTAssertFalse(source.contains(".padding(.leading"))
        XCTAssertTrue(source.contains(".frame(maxWidth: .infinity, alignment: .leading)"))
    }

    private var sourcePath: String {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/RuneSegmentedPickerInScroll.swift")
            .path
    }

    @MainActor
    private func adaptivePickerHost(width: CGFloat) -> NSHostingView<some View> {
        let picker = RuneAdaptiveSegmentedPicker(
            "Resource type",
            selection: .constant(0),
            labelsHidden: true,
            compactMaximumWidth: 220
        ) {
            Text("Pods").tag(0)
            Text("Deployments").tag(1)
            Text("StatefulSets").tag(2)
            Text("DaemonSets").tag(3)
            Text("Jobs").tag(4)
            Text("CronJobs").tag(5)
            Text("ReplicaSets").tag(6)
        }
        .frame(width: width, height: 50, alignment: .leading)

        let host = NSHostingView(rootView: picker)
        host.frame = NSRect(x: 0, y: 0, width: width, height: 50)
        host.layoutSubtreeIfNeeded()
        return host
    }

    @MainActor
    private func firstVisibleSubview<ViewType: NSView>(
        of type: ViewType.Type,
        in root: NSView
    ) -> ViewType? {
        if let match = root as? ViewType,
           !match.isHidden,
           match.frame.width > 1,
           match.frame.height > 1 {
            return match
        }
        for subview in root.subviews {
            if let match = firstVisibleSubview(of: type, in: subview) {
                return match
            }
        }
        return nil
    }
}
