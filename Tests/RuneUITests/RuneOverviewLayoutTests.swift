import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RuneOverviewLayoutTests: XCTestCase {
    func testBalancedGridAvoidsOrphanedFinalCards() {
        XCTAssertEqual(
            RuneBalancedOverviewGrid.balancedRowCounts(itemCount: 8, maximumColumns: 4),
            [4, 4]
        )
        XCTAssertEqual(
            RuneBalancedOverviewGrid.balancedRowCounts(itemCount: 7, maximumColumns: 4),
            [4, 3]
        )
        XCTAssertEqual(
            RuneBalancedOverviewGrid.balancedRowCounts(itemCount: 7, maximumColumns: 3),
            [3, 2, 2]
        )
        XCTAssertEqual(
            RuneBalancedOverviewGrid.balancedRowCounts(itemCount: 3, maximumColumns: 8),
            [3]
        )
    }

    func testBalancedGridUsesStableRowsAtDefaultAndCompactWidths() {
        let defaultHost = overviewGridHost(width: 700)
        let compactHost = overviewGridHost(width: 500)

        XCTAssertEqual(defaultHost.fittingSize.width, 700, accuracy: 0.5)
        XCTAssertEqual(defaultHost.fittingSize.height, 110, accuracy: 0.5)
        XCTAssertEqual(compactHost.fittingSize.width, 500, accuracy: 0.5)
        XCTAssertEqual(compactHost.fittingSize.height, 170, accuracy: 0.5)
    }

    func testBadgeFlowWrapsWithoutOverlapAtCompactWidth() {
        let metrics = RuneBadgeFlowLayout.flowMetrics(
            sizes: [
                CGSize(width: 100, height: 22),
                CGSize(width: 100, height: 22),
                CGSize(width: 100, height: 22),
                CGSize(width: 100, height: 22)
            ],
            availableWidth: 208,
            horizontalSpacing: 8,
            verticalSpacing: 8
        )

        XCTAssertEqual(metrics.origins, [
            CGPoint(x: 0, y: 0),
            CGPoint(x: 108, y: 0),
            CGPoint(x: 0, y: 30),
            CGPoint(x: 108, y: 30)
        ])
        XCTAssertEqual(metrics.height, 52, accuracy: 0.5)
    }

    func testSharedOverviewHeadersUseOneIconTrack() throws {
        let root = repoRoot
        let design = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Layout/RuneOverviewLayouts.swift"),
            encoding: .utf8
        )
        let overview = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift"),
            encoding: .utf8
        )
        let signals = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/OverviewClusterSignalsPanelView.swift"),
            encoding: .utf8
        )
        let events = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/OverviewRecentEventsPanelView.swift"),
            encoding: .utf8
        )

        XCTAssertTrue(design.contains(".frame(width: 18, height: 18, alignment: .center)"))
        XCTAssertTrue(overview.contains("RuneSectionHeader(\n                            \"Pod Health\""))
        XCTAssertTrue(signals.contains("RuneSectionHeader(\n                \"Cluster Signals\""))
        XCTAssertTrue(events.contains("RuneSectionHeader(\n                \"Recent Events\""))
        XCTAssertTrue(overview.contains("RuneBadgeFlowLayout("))
    }

    private func overviewGridHost(width: CGFloat) -> NSView {
        let host = NSHostingView(
            rootView: RuneBalancedOverviewGrid(minimumItemWidth: 160, spacing: 10) {
                ForEach(0..<7, id: \.self) { index in
                    Color.clear
                        .frame(height: 50)
                        .accessibilityLabel("Card \(index)")
                }
            }
            .frame(width: width)
        )
        host.layoutSubtreeIfNeeded()
        return host
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
