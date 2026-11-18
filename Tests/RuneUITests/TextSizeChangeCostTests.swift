import AppKit
@testable import RuneUI
import RuneCore
import SwiftUI
import XCTest

/// Changing the text size with the − and + buttons (or ⌘− and ⌘+) must stay
/// instant with long logs and transcripts: one step may not block the main
/// thread for more than a frame or two, however much text is shown.
@MainActor
final class TextSizeChangeCostTests: XCTestCase {
    private final class Model: ObservableObject {
        @Published var fontSize: CGFloat = 12
    }

    private struct LogHost: View {
        let logs: String
        var body: some View {
            AppKitManifestTextView(text: .constant(logs), isEditable: false, contentStyle: .ansiLogs)
                .frame(width: 800, height: 460)
                .environment(\.runeResolvedTheme, RuneAppearanceTheme.fjord.resolvedTheme)
                .environment(\.runeThemePalette, RuneAppearanceTheme.fjord.palette)
        }
    }

    private struct TranscriptHost: View {
        @ObservedObject var model: Model
        let text: String
        var body: some View {
            TerminalTranscriptSurface(
                text: text, height: 440, resetID: "shell", fontSize: model.fontSize,
                onPasteText: { _ in }, onInputSequence: { _ in }, onResizeGrid: { _, _ in }
            )
            .frame(width: 800, height: 460)
            .environment(\.runeThemePalette, RuneAppearanceTheme.fjord.palette)
        }
    }

    func testStepsOnALongLogStayInstant() async throws {
        let defaults = UserDefaults.standard
        let previous = defaults.object(forKey: RuneSettingsKeys.terminalFontSize)
        defer { if let previous { defaults.set(previous, forKey: RuneSettingsKeys.terminalFontSize) } else { defaults.removeObject(forKey: RuneSettingsKeys.terminalFontSize) } }
        defaults.set(12.0, forKey: RuneSettingsKeys.terminalFontSize)

        let logs = (0..<20_000).map { "2026-01-01T10:00:00Z \u{1B}[32mINFO\u{1B}[0m component=api request \($0) handled in \($0 % 97) ms" }.joined(separator: "\n")
        let window = try await mount(LogHost(logs: logs))
        defer { RuneTestWindow.close(window) }
        let textView = try XCTUnwrap(firstTextView(in: window))

        var steps: [Double] = []
        for size in [13.0, 14.0, 15.0, 14.0, 13.0, 12.0] {
            let started = ContinuousClock.now
            defaults.set(size, forKey: RuneSettingsKeys.terminalFontSize)
            try await waitUntil { abs((textView.font?.pointSize ?? 0) - size) < 0.01 }
            window.contentView?.layoutSubtreeIfNeeded()
            window.displayIfNeeded()
            steps.append(Self.milliseconds(started.duration(to: .now)))
        }
        print("TEXT SIZE log steps ms:", steps.map { Int($0) })
        RuneBenchmarkRecord.emit("text_size_step_long_log", samplesSeconds: steps.map { $0 / 1000 })
        let storage = try XCTUnwrap(textView.textStorage)
        let firstLine = (storage.string as NSString).lineRange(for: NSRange(location: 0, length: 0))
        var colors = Set<NSColor>()
        storage.enumerateAttribute(.foregroundColor, in: firstLine) { value, _, _ in
            if let color = value as? NSColor { colors.insert(color) }
        }
        XCTAssertGreaterThan(colors.count, 1, "the ANSI colours survive size steps")
        XCTAssertEqual(storage.attribute(.font, at: 0, effectiveRange: nil) as? NSFont, NSFont.monospacedSystemFont(ofSize: 12, weight: .regular))
        XCTAssertLessThan(steps.sorted()[steps.count / 2], 50, "a size step on a 20,000-line log takes \(steps) ms")
    }

    /// Below 250 KB the transcript is an AppKit text view.
    func testStepsOnATranscriptTextViewStayInstant() async throws {
        let text = (0..<2_400).map { "$ echo \($0)\n\($0) rows of shell output in a pod" }.joined(separator: "\n")
        XCTAssertLessThan(text.utf8.count, 250_000)
        try await measureTranscriptSteps(text: text, label: "transcript text view", budget: 50) { window in
            self.firstTextView(in: window).map { $0.font?.pointSize ?? 0 }
        }
    }

    /// Above 250 KB it is a lazy surface that draws only the visible lines.
    func testStepsOnALargeTranscriptSurfaceStayInstant() async throws {
        let text = (0..<12_000).map { "$ echo \($0)\n\($0) rows of shell output that wrap past the edge of a narrow terminal pane" }.joined(separator: "\n")
        XCTAssertGreaterThan(text.utf8.count, 250_000)
        // The lazy surface re-measures its visible rows (SwiftUI text with
        // selection); this ceiling only catches a gross regression.
        try await measureTranscriptSteps(text: text, label: "large transcript surface", budget: 150) { window in
            self.firstTextField(in: window).map { $0.font?.pointSize ?? 0 }
        }
    }

    private func measureTranscriptSteps(text: String, label: String, budget: Double, shownSize: @escaping (NSWindow) -> CGFloat?) async throws {
        let model = Model()
        let window = try await mount(TranscriptHost(model: model, text: text))
        defer { RuneTestWindow.close(window) }
        XCTAssertNotNil(shownSize(window), "\(label) is on screen")

        var steps: [Double] = []
        for size in [13.0, 14.0, 15.0, 14.0, 13.0, 12.0] as [CGFloat] {
            let started = ContinuousClock.now
            model.fontSize = size
            try await waitUntil { abs((shownSize(window) ?? 0) - size) < 0.01 }
            window.contentView?.layoutSubtreeIfNeeded()
            window.displayIfNeeded()
            steps.append(Self.milliseconds(started.duration(to: .now)))
        }
        print("TEXT SIZE \(label) steps ms:", steps.map { Int($0) })
        RuneBenchmarkRecord.emit("text_size_step_\(label.replacingOccurrences(of: " ", with: "_"))", samplesSeconds: steps.map { $0 / 1000 })
        XCTAssertLessThan(steps.sorted()[steps.count / 2], budget, "a size step on the \(label) takes \(steps) ms")
    }

    private func firstTextField(in window: NSWindow) -> NSTextField? {
        func find(_ view: NSView) -> NSTextField? {
            if let field = view as? NSTextField, !field.stringValue.isEmpty { return field }
            for subview in view.subviews { if let found = find(subview) { return found } }
            return nil
        }
        return window.contentView.flatMap(find)
    }

    private static func milliseconds(_ duration: Duration) -> Double {
        Double(duration.components.seconds) * 1000 + Double(duration.components.attoseconds) / 1e15
    }

    private func waitUntil(timeout: TimeInterval = 5, _ condition: () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition() {
            if Date() > deadline { return XCTFail("timed out") }
            try await Task.sleep(nanoseconds: 1_000_000)
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
        try await Task.sleep(nanoseconds: 800_000_000)
        return window
    }

    private func firstTextView(in window: NSWindow) -> NSTextView? {
        func find(_ view: NSView) -> NSTextView? {
            if let textView = view as? NSTextView, !textView.isEditable { return textView }
            for subview in view.subviews { if let found = find(subview) { return found } }
            return nil
        }
        return window.contentView.flatMap(find)
    }
}
