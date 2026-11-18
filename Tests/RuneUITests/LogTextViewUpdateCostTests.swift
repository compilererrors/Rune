import AppKit
@testable import RuneUI
import SwiftUI
import XCTest

/// Pod logs render in an AppKit text view inside SwiftUI, re-evaluated on
/// every render of the window (pod refreshes, other panes, streamed shell
/// output). A render that changes neither the logs nor the theme must not
/// re-format them: re-colouring every ANSI line and invalidating the view's
/// size on each render pinned the CPU and fed AppKit's layout loop.
@MainActor
final class LogTextViewUpdateCostTests: XCTestCase {
    private final class Model: ObservableObject {
        @Published var logs: String
        @Published var unrelated = 0
        init(logs: String) { self.logs = logs }
    }

    private struct Host: View {
        @ObservedObject var model: Model
        let theme: RuneAppearanceTheme
        var body: some View {
            VStack(spacing: 0) {
                Text("renders \(model.unrelated)")
                AppKitManifestTextView(text: .constant(model.logs), isEditable: false, contentStyle: .ansiLogs)
            }
            .frame(width: 800, height: 460)
            .environment(\.runeResolvedTheme, theme.resolvedTheme)
            .environment(\.runeThemePalette, theme.palette)
        }
    }

    func testRendersWithoutNewLogsLeaveTheLogTextAlone() async throws {
        for theme in [RuneAppearanceTheme.native, .fjord, .paper] {
            let logs = (0..<3_000).map { "2026-01-01T10:00:00Z \u{1B}[32mINFO\u{1B}[0m request \($0) handled" }.joined(separator: "\n")
            let model = Model(logs: logs)
            let window = try await mount(Host(model: model, theme: theme))
            let storage = try XCTUnwrap(textView(in: window)?.textStorage)
            var edits = 0
            let observer = NotificationCenter.default.addObserver(
                forName: NSTextStorage.didProcessEditingNotification, object: storage, queue: nil
            ) { _ in edits += 1 }

            for render in 1...30 {
                model.unrelated = render
                try await Task.sleep(nanoseconds: 5_000_000)
            }
            NotificationCenter.default.removeObserver(observer)
            RuneTestWindow.close(window)
            XCTAssertEqual(edits, 0, "\(theme): renders without new logs re-formatted them \(edits) times")
        }
    }

    private func mount<V: View>(_ view: V) async throws -> NSWindow {
        let host = NSHostingController(rootView: view)
        host.sizingOptions = []
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 800, height: 460), styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.setContentSize(NSSize(width: 800, height: 460))
        window.orderFront(nil)
        try await Task.sleep(nanoseconds: 600_000_000)
        return window
    }

    private func textView(in window: NSWindow) -> NSTextView? {
        func find(_ view: NSView) -> NSTextView? {
            if let textView = view as? NSTextView { return textView }
            for subview in view.subviews { if let found = find(subview) { return found } }
            return nil
        }
        return window.contentView.flatMap(find)
    }
}
