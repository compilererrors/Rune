import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

/// Settings is a standard macOS settings window: pane tabs in the toolbar,
/// window buttons, and each pane a grouped form.
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
        defer { RuneTestWindow.close(window) }
        try await Task.sleep(nanoseconds: 1_200_000_000)

        // SwiftUI puts a settings TabView's panes in the window toolbar.
        let toolbarItems = try XCTUnwrap(window.toolbar?.items, "the panes are in the window toolbar")
        XCTAssertTrue(toolbarItems.contains { $0.label == "Navigation Tab Bar" }, "the pane tabs are a toolbar item")
        XCTAssertNotNil(window.standardWindowButton(.closeButton), "the window keeps its close button")
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
        XCTAssertTrue(source.contains("TabView(selection: $selectedPane)"))
        XCTAssertTrue(source.contains(".tabItem { Label(pane.title(settingsString), systemImage: pane.symbol) }"))
        XCTAssertTrue(source.contains("Text(title)\n                .runeText(.body)"), "row titles are 13 pt regular")
        XCTAssertTrue(source.contains("Text(detail)\n                    .runeText(.micro)\n                    .foregroundStyle(.runeSecondary)"), "descriptions are small and secondary")
        XCTAssertTrue(source.contains(".toggleStyle(.switch)\n        .controlSize(.small)"), "row switches are small")
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
