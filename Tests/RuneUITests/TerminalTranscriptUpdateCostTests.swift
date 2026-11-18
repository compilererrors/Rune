import AppKit
@testable import RuneUI
import SwiftUI
import XCTest

/// The shell transcript is an AppKit text view inside SwiftUI, updated on
/// every render of the Terminal section (pod refreshes, streamed logs,
/// other tabs). A render that does not change the transcript must not touch
/// its text: re-colouring or re-laying out the whole transcript on each
/// render pinned the CPU, and invalidating sizes inside the update made
/// AppKit abort ("more Update Constraints passes than there are views").
@MainActor
final class TerminalTranscriptUpdateCostTests: XCTestCase {
    private final class Model: ObservableObject {
        @Published var text: String
        @Published var unrelated = 0
        init(text: String) { self.text = text }
    }

    private struct Host: View {
        @ObservedObject var model: Model
        var body: some View {
            VStack(spacing: 0) {
                Text("renders \(model.unrelated)")
                // As in the app: fresh closures and the theme's palette on every render.
                TerminalTranscriptSurface(
                    text: model.text, height: 400, resetID: "shell", fontSize: 12,
                    onPasteText: { _ in }, onInputSequence: { _ in }, onResizeGrid: { _, _ in }
                )
            }
            .frame(width: 800, height: 460)
            .environment(\.runeThemePalette, RuneAppearanceTheme.fjord.palette)
        }
    }

    func testRendersThatDoNotChangeTheTranscriptLeaveItsTextAlone() async throws {
        let transcript = (0..<4_000).map { "line \($0) of build output with some width to it\n" }.joined() + "$ "
        let model = Model(text: transcript)
        let window = try await mount(Host(model: model))
        defer { RuneTestWindow.close(window) }
        let storage = try XCTUnwrap(transcriptTextView(in: window)?.textStorage)

        var edits = 0
        let observer = NotificationCenter.default.addObserver(
            forName: NSTextStorage.didProcessEditingNotification, object: storage, queue: nil
        ) { _ in edits += 1 }
        defer { NotificationCenter.default.removeObserver(observer) }

        let started = ContinuousClock.now
        for render in 1...60 {
            model.unrelated = render
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        let elapsed = started.duration(to: .now)
        XCTAssertEqual(edits, 0, "renders without a transcript change edited its text \(edits) times")
        XCTAssertLessThan(elapsed, .milliseconds(1_500), "60 unrelated renders over a 4,000-line transcript")
    }

    func testAppendedOutputEditsOnlyTheNewText() async throws {
        let transcript = (0..<20_000).map { "line \($0)\n" }.joined() + "$ "
        let model = Model(text: transcript)
        let window = try await mount(Host(model: model))
        defer { RuneTestWindow.close(window) }
        let textView = try XCTUnwrap(transcriptTextView(in: window))
        let storage = try XCTUnwrap(textView.textStorage)

        var editedLengths: [Int] = []
        let observer = NotificationCenter.default.addObserver(
            forName: NSTextStorage.didProcessEditingNotification, object: storage, queue: nil
        ) { note in
            if let storage = note.object as? NSTextStorage { editedLengths.append(storage.editedRange.length) }
        }
        defer { NotificationCenter.default.removeObserver(observer) }

        for chunk in 0..<20 {
            model.text += "ls\nfile-\(chunk)\n$ "
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertEqual(textView.string, model.text)
        XCTAssertFalse(editedLengths.isEmpty)
        XCTAssertLessThan(editedLengths.max() ?? 0, 1_000, "an append re-styled far more than the new text: \(editedLengths.max() ?? 0) characters")
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

    private func transcriptTextView(in window: NSWindow) -> NSTextView? {
        func find(_ view: NSView) -> NSTextView? {
            if let textView = view as? NSTextView, !textView.isEditable { return textView }
            for subview in view.subviews { if let found = find(subview) { return found } }
            return nil
        }
        return window.contentView.flatMap(find)
    }
}
