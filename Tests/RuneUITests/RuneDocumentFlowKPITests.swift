import AppKit
@testable import RuneUI
import RuneCore
import SwiftUI
import XCTest

/// Release KPIs for the document views people watch and edit: logs that
/// keep growing while Follow is on, and typing in a large YAML manifest.
@MainActor
final class RuneDocumentFlowKPITests: XCTestCase {
    private final class TextModel: ObservableObject {
        @Published var text: String
        init(_ text: String) { self.text = text }
    }

    private struct LogHost: View {
        @ObservedObject var model: TextModel
        var body: some View {
            AppKitManifestTextView(text: .constant(model.text), isEditable: false, contentStyle: .ansiLogs)
                .frame(width: 900, height: 500)
                .environment(\.runeResolvedTheme, RuneAppearanceTheme.fjord.resolvedTheme)
                .environment(\.runeThemePalette, RuneAppearanceTheme.fjord.palette)
        }
    }

    private struct YAMLHost: View {
        @ObservedObject var model: TextModel
        var body: some View {
            AppKitManifestTextView(text: $model.text, isEditable: true, contentStyle: .yaml)
                .frame(width: 900, height: 500)
                .environment(\.runeResolvedTheme, RuneAppearanceTheme.fjord.resolvedTheme)
                .environment(\.runeThemePalette, RuneAppearanceTheme.fjord.palette)
        }
    }

    /// Follow mode: a 10,000-line log gets 50 new lines at a time. Each
    /// update must land within a couple of frames, however long the log.
    func testLogFollowAppendKPI() async throws {
        func lines(_ range: Range<Int>) -> String {
            range.map { "2026-01-01T10:00:00Z \u{1B}[32mINFO\u{1B}[0m request \($0) handled in \($0 % 97) ms" }.joined(separator: "\n")
        }
        let model = TextModel(lines(0..<10_000))
        let window = try await mount(LogHost(model: model))
        defer { RuneTestWindow.close(window) }
        let textView = try XCTUnwrap(firstTextView(in: window) { !$0.isEditable })

        var samples: [Double] = []
        var next = 10_000
        for _ in 0..<12 {
            let marker = "request \(next + 49) handled"
            let started = ContinuousClock.now
            model.text += "\n" + lines(next..<(next + 50))
            next += 50
            try await waitUntil { textView.string.hasSuffix(marker + " in \((next - 1) % 97) ms") }
            window.contentView?.layoutSubtreeIfNeeded()
            window.displayIfNeeded()
            samples.append(RuneBenchmarkRecord.seconds(started.duration(to: .now)))
        }
        RuneBenchmarkRecord.emit("log_follow_append", samplesSeconds: samples)
        let median = samples.sorted()[samples.count / 2] * 1000
        print("KPI log follow append ms:", samples.map { Int($0 * 1000) })
        XCTAssertLessThan(median, 50, "appending 50 lines to a 10,000-line log takes \(Int(median)) ms")
    }

    /// Appended lines look exactly as they would after a full reformat:
    /// colours, separators and text; text that is not an append still works.
    func testLogFollowAppendMatchesAFullReformat() async throws {
        let first = "10:00 \u{1B}[31mERROR\u{1B}[0m first\n────────────────\n10:01 \u{1B}[1;33mWARN\u{1B}[0m second"
        let added = "\n10:02 \u{1B}[32mINFO\u{1B}[0m third\n────────────────\n10:03 plain fourth"
        let model = TextModel(first)
        let window = try await mount(LogHost(model: model))
        defer { RuneTestWindow.close(window) }
        let textView = try XCTUnwrap(firstTextView(in: window) { !$0.isEditable })
        model.text += added
        try await waitUntil { textView.string.hasSuffix("plain fourth") }

        let reference = TextModel(first + added)
        let referenceWindow = try await mount(LogHost(model: reference))
        defer { RuneTestWindow.close(referenceWindow) }
        let referenceView = try XCTUnwrap(firstTextView(in: referenceWindow) { !$0.isEditable })
        XCTAssertEqual(textView.string, referenceView.string)
        XCTAssertTrue(try XCTUnwrap(textView.textStorage).isEqual(to: try XCTUnwrap(referenceView.textStorage)), "appended lines are styled as a full reformat styles them")

        model.text = "replaced log"
        try await waitUntil { textView.string == "replaced log" }
    }

    /// Typing in a 3,000-line manifest: each key must stay well inside a frame.
    func testYAMLEditorTypingInALargeManifestKPI() async throws {
        let manifest = (0..<600).map { index in
            "  - name: container-\(index)\n    image: registry.example/app:\(index)\n    ports:\n    - containerPort: \(8000 + index)\n    env: []"
        }.joined(separator: "\n")
        let model = TextModel("apiVersion: v1\nkind: Pod\nmetadata:\n  name: sample\nspec:\n  containers:\n" + manifest)
        let window = try await mount(YAMLHost(model: model))
        defer { RuneTestWindow.close(window) }
        let textView = try XCTUnwrap(firstTextView(in: window) { $0.isEditable })
        window.makeFirstResponder(textView)
        textView.setSelectedRange(NSRange(location: (textView.string as NSString).length, length: 0))

        var samples: [Double] = []
        for character in "\n  - name: typed-container" {
            let started = ContinuousClock.now
            textView.insertText(String(character), replacementRange: textView.selectedRange())
            window.contentView?.layoutSubtreeIfNeeded()
            window.displayIfNeeded()
            samples.append(RuneBenchmarkRecord.seconds(started.duration(to: .now)))
            await Task.yield()
        }
        RuneBenchmarkRecord.emit("yaml_editor_large_manifest_keystroke", samplesSeconds: samples)
        let median = samples.sorted()[samples.count / 2] * 1000
        let slowest = (samples.max() ?? 0) * 1000
        print("KPI YAML typing ms median:", String(format: "%.2f", median), "slowest:", String(format: "%.2f", slowest))
        XCTAssertLessThan(median, 16, "a key in a 3,000-line manifest takes \(median) ms")
        XCTAssertLessThan(slowest, 50, "the slowest key took \(slowest) ms")
    }

    private func waitUntil(timeout: TimeInterval = 10, _ condition: () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition() {
            if Date() > deadline { return XCTFail("timed out") }
            try await Task.sleep(nanoseconds: 1_000_000)
        }
    }

    private func mount<V: View>(_ view: V) async throws -> NSWindow {
        let host = NSHostingController(rootView: view)
        host.sizingOptions = []
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 900, height: 500), styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.setContentSize(NSSize(width: 900, height: 500))
        window.orderFront(nil)
        try await Task.sleep(nanoseconds: 800_000_000)
        return window
    }

    private func firstTextView(in window: NSWindow, where matches: (NSTextView) -> Bool) -> NSTextView? {
        func find(_ view: NSView) -> NSTextView? {
            if let textView = view as? NSTextView, matches(textView) { return textView }
            for subview in view.subviews { if let found = find(subview) { return found } }
            return nil
        }
        return window.contentView.flatMap(find)
    }
}
