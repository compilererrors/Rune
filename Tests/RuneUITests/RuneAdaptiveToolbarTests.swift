import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

final class RuneAdaptiveToolbarTests: XCTestCase {
    func testHorizontalScrollingRequiresExplicitCompactPolicy() {
        XCTAssertFalse(RuneAdaptiveToolbarCompactBehavior.stacked.usesHorizontalScrolling)
        XCTAssertTrue(RuneAdaptiveToolbarCompactBehavior.horizontalScroll.usesHorizontalScrolling)
    }

    func testSupportedInspectorAndRowMinimumsRemainUsable() {
        XCTAssertEqual(RuneAdaptiveToolbarMetrics.supportedInspectorWidth, 320)
        XCTAssertEqual(RBACCanILayoutMetrics.compactPanelWidth, 264)
        XCTAssertEqual(RBACCanILayoutMetrics.compactContentWidth, 240)
        XCTAssertGreaterThanOrEqual(RuneAdaptiveToolbarMetrics.minimumRowHeight, 30)
        XCTAssertGreaterThan(
            RuneAdaptiveToolbarMetrics.accessibilityMinimumRowHeight,
            RuneAdaptiveToolbarMetrics.minimumRowHeight
        )
        XCTAssertGreaterThanOrEqual(RuneAdaptiveToolbarMetrics.groupSpacing, 8)
        XCTAssertGreaterThanOrEqual(RuneAdaptiveToolbarMetrics.rowSpacing, 8)
    }

    @MainActor
    func testWideToolbarUsesOneRowAndCompactWidthUsesTwoRows() {
        let wide = fittingSize(width: 700, dynamicTypeSize: .large)
        let compact = fittingSize(
            width: RuneAdaptiveToolbarMetrics.supportedInspectorWidth,
            dynamicTypeSize: .large
        )

        XCTAssertEqual(wide.width, 700, accuracy: 0.5)
        XCTAssertEqual(
            compact.width,
            RuneAdaptiveToolbarMetrics.supportedInspectorWidth,
            accuracy: 0.5
        )
        XCTAssertGreaterThanOrEqual(wide.height, RuneAdaptiveToolbarMetrics.minimumRowHeight)
        XCTAssertGreaterThanOrEqual(
            compact.height,
            RuneAdaptiveToolbarMetrics.minimumRowHeight * 2
                + RuneAdaptiveToolbarMetrics.rowSpacing
        )
        XCTAssertGreaterThan(compact.height, wide.height)
    }

    @MainActor
    func testCompactToolbarExpandsForAccessibilityTextWithoutClippingRows() {
        let defaultSize = fittingSize(
            width: RuneAdaptiveToolbarMetrics.supportedInspectorWidth,
            dynamicTypeSize: .large
        )
        let accessibilitySize = fittingSize(
            width: RuneAdaptiveToolbarMetrics.supportedInspectorWidth,
            dynamicTypeSize: .accessibility3
        )

        XCTAssertEqual(accessibilitySize.width, defaultSize.width, accuracy: 0.5)
        XCTAssertGreaterThan(accessibilitySize.height, defaultSize.height)
    }

    @MainActor
    func testCompactScrollerExistsOnlyForExplicitScrollPolicy() {
        let stackedHost = hostingView(compactBehavior: .stacked)
        let scrollingHost = hostingView(compactBehavior: .horizontalScroll)

        stackedHost.layoutSubtreeIfNeeded()
        scrollingHost.layoutSubtreeIfNeeded()

        XCTAssertFalse(containsScrollView(in: stackedHost))
        XCTAssertTrue(containsScrollView(in: scrollingHost))
    }

    @MainActor
    func testRealRBACAdopterStacksInsideItsEffectiveCompactWidth() throws {
        let viewModel = RuneAppViewModel(state: RuneAppState())
        var latestSnapshot: RBACCanILayoutSnapshot?
        let host = NSHostingView(rootView: RBACCanISimulatorPanel(
            viewModel: viewModel,
            onLayoutSnapshotChange: { latestSnapshot = $0 }
        ).frame(width: RBACCanILayoutMetrics.compactPanelWidth))
        host.frame = NSRect(
            x: 0,
            y: 0,
            width: RBACCanILayoutMetrics.compactPanelWidth,
            height: 360
        )

        settle(host, until: { latestSnapshot != nil })

        let snapshot = try XCTUnwrap(latestSnapshot)
        for region in [
            RBACCanILayoutRegion.header,
            .request,
            .optionalFields,
            .result
        ] {
            let frame = try XCTUnwrap(snapshot[region], "Missing rendered RBAC region \(region)")
            XCTAssertGreaterThan(frame.width, 0)
            XCTAssertGreaterThan(frame.height, 0)
            XCTAssertGreaterThanOrEqual(
                frame.minX,
                RBACCanILayoutMetrics.panelHorizontalPadding - 0.5
            )
            XCTAssertLessThanOrEqual(
                frame.maxX,
                RBACCanILayoutMetrics.compactPanelWidth
                    - RBACCanILayoutMetrics.panelHorizontalPadding
                    + 0.5
            )
        }

        let optionalFields = try XCTUnwrap(snapshot[.optionalFields])
        XCTAssertEqual(
            optionalFields.width,
            RBACCanILayoutMetrics.compactContentWidth,
            accuracy: 0.5
        )
        XCTAssertGreaterThan(
            optionalFields.height,
            RuneAdaptiveToolbarMetrics.minimumRowHeight,
            "API group and subresource should render as two rows at the real 240pt content width."
        )
        XCTAssertFalse(
            containsHorizontalScroller(in: host),
            "The real compact RBAC panel must not hide fields or actions in horizontal scrolling."
        )
    }

    func testStandaloneAdoptersUseAdaptiveGroupsAndResponsiveEndpoints() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let rbac = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/RBACCanISimulatorPanel.swift"),
            encoding: .utf8
        )
        let portForward = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/TerminalPortForwardPanelView.swift"),
            encoding: .utf8
        )
        let portForwardControls = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/PortForwardControlComponents.swift"),
            encoding: .utf8
        )

        XCTAssertTrue(rbac.contains("RuneAdaptiveToolbar(\"RBAC access review actions\")"))
        XCTAssertTrue(rbac.contains("RuneAdaptiveToolbar(\"RBAC access review request\")"))
        XCTAssertTrue(portForward.contains("RuneAdaptiveToolbar(\"Port-forward controls\")"))
        XCTAssertTrue(portForward.contains("PortForwardEndpointFields("))
        XCTAssertTrue(portForwardControls.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertFalse(portForwardControls.contains("ScrollView(.horizontal"))
    }

    @MainActor
    private func fittingSize(
        width: CGFloat,
        dynamicTypeSize: DynamicTypeSize
    ) -> CGSize {
        let host = NSHostingView(rootView: toolbar(
            compactBehavior: .stacked,
            dynamicTypeSize: dynamicTypeSize
        ).frame(width: width))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    @MainActor
    private func hostingView(
        compactBehavior: RuneAdaptiveToolbarCompactBehavior
    ) -> NSView {
        NSHostingView(rootView: toolbar(
            compactBehavior: compactBehavior,
            dynamicTypeSize: .large
        ).frame(width: RuneAdaptiveToolbarMetrics.supportedInspectorWidth))
    }

    @MainActor
    private func toolbar(
        compactBehavior: RuneAdaptiveToolbarCompactBehavior,
        dynamicTypeSize: DynamicTypeSize
    ) -> some View {
        RuneAdaptiveToolbar(
            "Synthetic inspector controls",
            compactBehavior: compactBehavior
        ) {
            Text("Primary inspector controls")
                .font(.body)
                .padding(.vertical, 4)
                .frame(width: 220, alignment: .leading)
        } secondary: {
            Text("Secondary inspector actions")
                .font(.body)
                .padding(.vertical, 4)
                .frame(width: 220, alignment: .leading)
        }
        .environment(\.dynamicTypeSize, dynamicTypeSize)
    }

    @MainActor
    private func containsScrollView(in view: NSView) -> Bool {
        if view is NSScrollView { return true }
        return view.subviews.contains(where: containsScrollView(in:))
    }

    @MainActor
    private func containsHorizontalScroller(in view: NSView) -> Bool {
        if let scrollView = view as? NSScrollView, scrollView.hasHorizontalScroller {
            return true
        }
        return view.subviews.contains(where: containsHorizontalScroller(in:))
    }

    @MainActor
    private func settle(
        _ host: NSView,
        until condition: () -> Bool
    ) {
        for _ in 0..<50 {
            host.layoutSubtreeIfNeeded()
            if condition() { return }
            RunLoop.main.run(until: Date().addingTimeInterval(0.005))
        }
    }
}
