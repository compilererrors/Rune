import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class TerminalTabLayoutTests: XCTestCase {
    @MainActor
    func testPlaceholderLabelKeepsExactEstablishedTabSizeWithLongContent() {
        let host = NSHostingView(rootView: TerminalPlaceholderTabLabel(
            primaryTitle: "1 New Shell With A Long Synthetic Name",
            secondaryTitle: "synthetic-pod-name-that-must-truncate"
        ))
        host.layoutSubtreeIfNeeded()

        XCTAssertEqual(host.fittingSize.width, TerminalTabLayoutMetrics.tabWidth, accuracy: 0.5)
        XCTAssertEqual(host.fittingSize.height, TerminalTabLayoutMetrics.tabHeight, accuracy: 0.5)
        XCTAssertEqual(TerminalTabLayoutMetrics.tabWidth, 216)
        XCTAssertEqual(TerminalTabLayoutMetrics.tabHeight, 28)
    }

    func testEmptyAndDraftTabsUseTheSharedFixedWidthPlaceholder() throws {
        let sessionSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)
        let logSource = try String(contentsOfFile: terminalLogTabBarPath, encoding: .utf8)
        let stripSource = try String(contentsOfFile: terminalTabStripPath, encoding: .utf8)

        XCTAssertTrue(sessionSource.contains("TerminalPlaceholderTabLabel("))
        XCTAssertTrue(logSource.contains("TerminalPlaceholderTabLabel("))
        XCTAssertEqual(
            stripSource.components(separatedBy: "struct TerminalPlaceholderTabLabel: View").count - 1,
            1
        )
        let padding = try XCTUnwrap(stripSource.range(
            of: ".padding(.horizontal, TerminalTabLayoutMetrics.placeholderHorizontalPadding)"
        ))
        let frame = try XCTUnwrap(stripSource.range(
            of: "width: TerminalTabLayoutMetrics.tabWidth",
            range: padding.upperBound..<stripSource.endIndex
        ))
        XCTAssertLessThan(padding.lowerBound, frame.lowerBound)
    }

    private var terminalSessionTabBarPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalSessionTabBar.swift").path
    }

    private var terminalLogTabBarPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalLogTabBar.swift").path
    }

    private var terminalTabStripPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalTabStrip.swift").path
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
