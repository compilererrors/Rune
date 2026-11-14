import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

/// Settings is a standard macOS settings window: pane tabs in the toolbar,
/// a small page header and compact grouped rows.
@MainActor
final class RuneSettingsWindowLayoutTests: XCTestCase {
    func testSettingsTabsAreVisibleAtTheTopAtStandardWidth() async throws {
        let host = NSHostingController(rootView: RunePreferencesView())
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 820, height: 640),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.setContentSize(NSSize(width: 820, height: 640))
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }
        try await Task.sleep(nanoseconds: 1_200_000_000)

        let tabs = try XCTUnwrap(firstSubview(of: NSSegmentedControl.self, in: try XCTUnwrap(window.contentView)), "visible pane tab bar")
        XCTAssertEqual(tabs.segmentCount, 7, "every settings pane is a visible tab")
        let frame = tabs.convert(tabs.bounds, to: nil)
        XCTAssertTrue(try XCTUnwrap(window.contentView).bounds.contains(frame), "the tab bar fits the standard settings width")
        XCTAssertGreaterThan(frame.minY, 640 - 80, "the tab bar sits at the top, with no empty band above it")
    }

    private func firstSubview<T: NSView>(of type: T.Type, in view: NSView) -> T? {
        if let match = view as? T { return match }
        for subview in view.subviews {
            if let found = firstSubview(of: type, in: subview) { return found }
        }
        return nil
    }

    func testSettingsTabLabelsAndRowsUseSystemSettingsProportions() throws {
        let source = try String(contentsOfFile: sourcePath, encoding: .utf8)
        XCTAssertTrue(source.contains("RuneTabBar(\"Settings\", selection: $selectedPane, tabs: PreferencesPane.allCases)"))
        XCTAssertTrue(source.contains("Text(title)\n                .runeText(.body)"), "row titles are 13 pt regular")
        XCTAssertTrue(source.contains("Text(detail)\n                    .runeText(.micro)\n                    .foregroundStyle(.runeSecondary)"), "descriptions are small and secondary")
        XCTAssertTrue(source.contains(".toggleStyle(.switch)\n                .controlSize(.small)"), "row switches are small")
    }

    private var sourcePath: String {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift")
            .path
    }
}

private extension String {
    func slice(from start: String, to end: String) -> String? {
        guard let lower = range(of: start)?.upperBound,
              let upper = range(of: end, range: lower..<endIndex)?.lowerBound else { return nil }
        return String(self[lower..<upper])
    }
}
