import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RuneTabBarTests: XCTestCase {
    private let kinds = ["Pods", "Deployments", "StatefulSets", "DaemonSets", "Jobs", "CronJobs", "ReplicaSets", "HPAs"]

    @MainActor
    func testLongTabGroupStaysInsideCompactInspectorWidth() {
        let host = NSHostingView(rootView: RuneTabBar(
            "Inspector",
            selection: .constant(0),
            tabs: Array(0..<8)
        ) { "Long choice \($0)" }
        .frame(width: 320))

        host.layoutSubtreeIfNeeded()
        XCTAssertEqual(host.fittingSize.width, 320, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(host.fittingSize.height, 24)
    }

    @MainActor
    func testResourceTabsNeverCollapseIntoADropdown() {
        for width in [CGFloat(1_100), CGFloat(240)] {
            let host = tabBarHost(width: width)
            XCTAssertNil(
                firstSubview(of: NSPopUpButton.self, in: host),
                "Resource-family tabs must never collapse into a dropdown at \(width) points."
            )
            XCTAssertEqual(host.frame.width, width, accuracy: 0.5)
        }
    }

    @MainActor
    func testTabBarIsNativeSegmentedControlWithFittedSegments() throws {
        let host = tabBarHost(width: 1_100)
        let control = try XCTUnwrap(firstSubview(of: NSSegmentedControl.self, in: host))
        XCTAssertEqual(control.segmentCount, kinds.count, "every tab is hosted")
        XCTAssertEqual(control.segmentDistribution, .fit, "segments fit their labels, so padding is even")
        XCTAssertEqual(control.selectedSegment, 0)
        XCTAssertLessThanOrEqual(host.fittingSize.height, RuneControlHeight.toolbar + 0.5, "at most one step below the buttons")
    }

    @MainActor
    func testTabBarFollowsInterfaceFont() throws {
        let standard = try XCTUnwrap(firstSubview(of: NSSegmentedControl.self, in: tabBarHost(width: 1_100, configuredFontSize: 12)))
        let enlarged = try XCTUnwrap(firstSubview(of: NSSegmentedControl.self, in: tabBarHost(width: 1_100, configuredFontSize: 16)))
        XCTAssertGreaterThan(enlarged.font?.pointSize ?? 0, standard.font?.pointSize ?? 0)
    }

    func testOverflowScrollsInsteadOfMenuFallback() throws {
        let source = try String(contentsOfFile: sourcePath, encoding: .utf8)

        XCTAssertTrue(source.contains("ScrollView(.horizontal, showsIndicators: true)"))
        XCTAssertTrue(source.contains("Scroll horizontally to reveal additional choices"))
        XCTAssertTrue(source.contains("control.scrollToVisible("), "the selected tab is kept in view")
        XCTAssertFalse(source.contains("ScrollViewReader"), "scrolling the bar must not scroll the inspector around it")
        XCTAssertFalse(source.contains(".pickerStyle(.menu)"))
        XCTAssertFalse(source.contains("Menu {"))
        XCTAssertTrue(source.contains(".frame(maxWidth: .infinity, alignment: .leading)"))
    }

    private var sourcePath: String {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Design/Components/RuneTabBar.swift")
            .path
    }

    @MainActor
    private func tabBarHost(width: CGFloat, configuredFontSize: Double = 12) -> NSHostingView<some View> {
        let names = kinds
        let bar = RuneTabBar("Resource type", selection: .constant(0), tabs: Array(names.indices)) { names[$0] }
            .runeInterfaceTypography(configuredFontSize: configuredFontSize, systemDynamicTypeSize: .large)
            .frame(width: width, alignment: .leading)
        let host = NSHostingView(rootView: bar)
        host.frame = NSRect(x: 0, y: 0, width: width, height: 50)
        host.layoutSubtreeIfNeeded()
        return host
    }

    private func firstSubview<T: NSView>(of type: T.Type, in view: NSView) -> T? {
        if let match = view as? T { return match }
        for subview in view.subviews {
            if let match = firstSubview(of: type, in: subview) { return match }
        }
        return nil
    }
}
