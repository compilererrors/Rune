import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

/// Shell and log tabs behave like native macOS window tabs (as Ghostty and
/// iTerm2 use them): they share the strip's full width, one tab spans it
/// all, and the strip scrolls only when tabs would drop below a minimum.
final class TerminalTabLayoutTests: XCTestCase {
    @MainActor
    func testOneTabSpansTheWholeStripAndSeveralShareItEqually() throws {
        for (count, width) in [(1, 600.0), (3, 600.0), (2, 320.0)] {
            var frames: [Int: CGRect] = [:]
            let strip = TerminalTabStrip(canAddTab: true, addHelp: "Add", addAccessibilityLabel: "Add", onAddTab: {}) {
                ForEach(0..<count, id: \.self) { index in
                    Color.clear
                        .background(GeometryReader { proxy in
                            Color.clear.onAppear { frames[index] = proxy.frame(in: .global) }
                        })
                }
            }
            .frame(width: width)
            let host = NSHostingView(rootView: strip)
            host.frame = NSRect(x: 0, y: 0, width: width, height: 40)
            host.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.1))

            XCTAssertEqual(frames.count, count)
            let available = width - TerminalTabLayoutMetrics.addSlotWidth - 1
            let expected = available / CGFloat(count)
            for index in 0..<count {
                let frame = try XCTUnwrap(frames[index])
                XCTAssertEqual(frame.width, expected, accuracy: 1, "tab \(index) of \(count) at \(width) pt")
                XCTAssertEqual(frame.height, TerminalTabLayoutMetrics.tabHeight, accuracy: 0.5)
            }
        }
    }

    @MainActor
    func testTooManyTabsScrollAtTheMinimumWidthInsteadOfShrinking() throws {
        let count = 6
        var frames: [Int: CGRect] = [:]
        let strip = TerminalTabStrip(canAddTab: true, addHelp: "Add", addAccessibilityLabel: "Add", onAddTab: {}) {
            ForEach(0..<count, id: \.self) { index in
                Color.clear
                    .background(GeometryReader { proxy in
                        Color.clear.onAppear { frames[index] = proxy.frame(in: .global) }
                    })
            }
        }
        .frame(width: 400)
        let host = NSHostingView(rootView: strip)
        host.frame = NSRect(x: 0, y: 0, width: 400, height: 40)
        host.layoutSubtreeIfNeeded()
        RunLoop.main.run(until: Date().addingTimeInterval(0.1))

        let first = try XCTUnwrap(frames[0])
        XCTAssertEqual(first.width, TerminalTabLayoutMetrics.minimumTabWidth, accuracy: 1)
        XCTAssertEqual(TerminalTabLayoutMetrics.tabHeight, 28, "native tab bar height")
    }

    func testTabsUseOneSharedComponentWithCenteredTitlesAndNoIndex() throws {
        let sessionSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)
        let logSource = try String(contentsOfFile: terminalLogTabBarPath, encoding: .utf8)
        let stripSource = try String(contentsOfFile: terminalTabStripPath, encoding: .utf8)

        for source in [sessionSource, logSource] {
            XCTAssertTrue(source.contains("TerminalTabStrip("))
            XCTAssertTrue(source.contains("TerminalTabTitle(") || source.contains("TerminalPlaceholderTabLabel("))
            XCTAssertTrue(source.contains("TerminalTabAccessoryButton("), "close (and favourite) in the tab's accessory slots")
            // The index stays in the VoiceOver label only, never in the visible title.
            XCTAssertFalse(source.contains("primaryTitle: \"\\(number)"), "titles have no index prefix")
            XCTAssertFalse(source.contains("Text(\"\\(number) "), "titles have no index prefix")
            XCTAssertFalse(source.contains(".frame(width: 216"), "no fixed tab width")
        }
        XCTAssertTrue(sessionSource.contains("primaryTitle: session.podName"))
        XCTAssertFalse(stripSource.contains("ViewThatFits(in: .horizontal)"), "one layout, no breakpoints")
        XCTAssertTrue(stripSource.contains("RuneEqualWidthTabsLayout(availableWidth: proxy.size.width)"))
        XCTAssertTrue(stripSource.contains("ScrollView(.horizontal, showsIndicators: false)"))
        XCTAssertTrue(stripSource.contains(".truncationMode(.middle)"))
        XCTAssertTrue(stripSource.contains("alignment: .center"))
        XCTAssertFalse(stripSource.contains("RuneOpenBottomTabOutline"), "the selected tab has no border")
        XCTAssertFalse(stripSource.contains("strokeBorder"))
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
