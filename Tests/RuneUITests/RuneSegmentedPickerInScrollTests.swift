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
    func testResourceFamilyPickerAlwaysHostsAllTabsWithoutMenuFallback() throws {
        for width in [CGFloat(1_100), CGFloat(240)] {
            let host = scrollingPickerHost(width: width)
            let segments = try XCTUnwrap(
                firstVisibleSubview(of: NSSegmentedControl.self, in: host),
                "Resource-family navigation must remain a segmented tab control at \(width) points."
            )

            XCTAssertEqual(segments.segmentCount, 8)
            XCTAssertNil(
                firstVisibleSubview(of: NSPopUpButton.self, in: host),
                "Resource-family tabs must never collapse into a dropdown."
            )
            XCTAssertEqual(host.frame.width, width, accuracy: 0.5)
        }
    }

    @MainActor
    func testInterfaceFontPreferenceReachesScrollableTabsAtEveryWidth() throws {
        for width in [CGFloat(1_100), CGFloat(240)] {
            let standardHost = scrollingPickerHost(width: width, configuredFontSize: 12)
            let enlargedHost = scrollingPickerHost(width: width, configuredFontSize: 15)
            let standardSegments = try XCTUnwrap(
                firstVisibleSubview(of: NSSegmentedControl.self, in: standardHost)
            )
            let enlargedSegments = try XCTUnwrap(
                firstVisibleSubview(of: NSSegmentedControl.self, in: enlargedHost)
            )

            XCTAssertGreaterThan(
                enlargedSegments.font?.pointSize ?? 0,
                standardSegments.font?.pointSize ?? 0
            )
            XCTAssertNil(firstVisibleSubview(of: NSPopUpButton.self, in: standardHost))
            XCTAssertNil(firstVisibleSubview(of: NSPopUpButton.self, in: enlargedHost))
        }
    }

    func testOverflowIsDiscoverableWithoutArtificialLeadingDrift() throws {
        let source = try String(contentsOfFile: sourcePath, encoding: .utf8)

        XCTAssertTrue(source.contains("ScrollView(.horizontal, showsIndicators: true)"))
        XCTAssertTrue(source.contains("Scroll horizontally to reveal additional choices"))
        XCTAssertTrue(source.contains(".pickerStyle(.segmented)"))
        XCTAssertFalse(source.contains(".pickerStyle(.menu)"))
        XCTAssertFalse(source.contains("RuneAdaptiveSegmentedPicker"))
        XCTAssertFalse(source.contains("ViewThatFits"))
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
    private func scrollingPickerHost(
        width: CGFloat,
        configuredFontSize: Double = 12
    ) -> NSHostingView<some View> {
        let picker = RuneSegmentedPickerInScroll(
            "Resource type",
            selection: .constant(0),
            labelsHidden: true
        ) {
            Text("Pods").tag(0)
            Text("Deployments").tag(1)
            Text("StatefulSets").tag(2)
            Text("DaemonSets").tag(3)
            Text("Jobs").tag(4)
            Text("CronJobs").tag(5)
            Text("ReplicaSets").tag(6)
            Text("HPAs").tag(7)
        }
        .runeInterfaceTypography(
            configuredFontSize: configuredFontSize,
            systemDynamicTypeSize: .large
        )
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
