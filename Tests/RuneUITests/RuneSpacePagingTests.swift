import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RuneSpacePagingTests: XCTestCase {
    func testOnlyPlainSpaceAndShiftSpacePage() throws {
        XCTAssertEqual(RuneSpacePaging.direction(for: try key(" ")), .down)
        XCTAssertEqual(RuneSpacePaging.direction(for: try key(" ", modifiers: .shift)), .up)
        XCTAssertNil(RuneSpacePaging.direction(for: try key(" ", modifiers: .command)))
        XCTAssertNil(RuneSpacePaging.direction(for: try key(" ", modifiers: .option)))
        XCTAssertNil(RuneSpacePaging.direction(for: try key(" ", modifiers: .control)))
        XCTAssertNil(RuneSpacePaging.direction(for: try key("a")))
    }

    func testPageKeepsOneLineOfOverlapAndClampsToDocument() {
        XCTAssertEqual(RuneSpacePaging.targetOffset(current: 0, visibleHeight: 400, documentHeight: 2_000, lineHeight: 16, direction: .down), 384)
        XCTAssertEqual(RuneSpacePaging.targetOffset(current: 1_500, visibleHeight: 400, documentHeight: 2_000, lineHeight: 16, direction: .down), 1_600)
        XCTAssertEqual(RuneSpacePaging.targetOffset(current: 100, visibleHeight: 400, documentHeight: 2_000, lineHeight: 16, direction: .up), 0)
        XCTAssertEqual(RuneSpacePaging.targetOffset(current: 0, visibleHeight: 400, documentHeight: 300, lineHeight: 16, direction: .down), 0)
    }

    func testSpacePagesReadOnlyInspectorDocumentAndShiftSpacePagesBack() throws {
        let harness = try Harness(isEditable: false)
        defer { harness.close() }
        XCTAssertTrue(harness.window.makeFirstResponder(harness.textView))
        let start = harness.offset

        harness.window.sendEvent(try key(" ", window: harness.window))
        try settle()
        let paged = harness.offset
        XCTAssertGreaterThan(paged - start, harness.visibleHeight * 0.8, "Space pages down about one screen")
        XCTAssertLessThan(paged - start, harness.visibleHeight, "one line of the previous page stays visible")

        harness.window.sendEvent(try key(" ", modifiers: .shift, window: harness.window))
        try settle()
        XCTAssertEqual(harness.offset, start, accuracy: 1, "Shift+Space pages back up")
        XCTAssertEqual(harness.textView.string, Harness.document, "paging never edits the document")
    }

    func testSpaceTypesInEditableYAMLEditor() throws {
        let harness = try Harness(isEditable: true)
        defer { harness.close() }
        XCTAssertTrue(harness.window.makeFirstResponder(harness.textView))
        harness.textView.setSelectedRange(NSRange(location: 0, length: 0))
        let start = harness.offset

        harness.window.sendEvent(try key(" ", window: harness.window))

        XCTAssertTrue(harness.textView.string.hasPrefix(" "), "Space types into an editable editor")
        XCTAssertEqual(harness.offset, start, accuracy: 1)
    }

    func testSpaceTypesInTextFieldNextToInspector() throws {
        let harness = try Harness(isEditable: false)
        defer { harness.close() }
        let field = NSTextField(string: "")
        field.frame = NSRect(x: 0, y: 0, width: 200, height: 24)
        harness.window.contentView?.addSubview(field)
        XCTAssertTrue(harness.window.makeFirstResponder(field))
        let start = harness.offset

        harness.window.sendEvent(try key("a", window: harness.window))
        harness.window.sendEvent(try key(" ", window: harness.window))
        harness.window.sendEvent(try key("b", window: harness.window))

        XCTAssertEqual(field.currentEditor()?.string, "a b", "Space types in a focused field")
        XCTAssertEqual(harness.offset, start, accuracy: 1, "the inspector does not page while a field has focus")
    }

    private func key(_ characters: String, modifiers: NSEvent.ModifierFlags = [], window: NSWindow? = nil) throws -> NSEvent {
        try XCTUnwrap(NSEvent.keyEvent(
            with: .keyDown,
            location: .zero,
            modifierFlags: modifiers,
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window?.windowNumber ?? 0,
            context: nil,
            characters: characters,
            charactersIgnoringModifiers: characters,
            isARepeat: false,
            keyCode: characters == " " ? 49 : 0
        ))
    }

    private func settle() throws {
        for _ in 0..<6 {
            RunLoop.main.run(until: Date().addingTimeInterval(0.06))
        }
    }

    @MainActor
    private final class Harness {
        static let document = (1...400).map { "line \($0): value" }.joined(separator: "\n")

        let window: NSWindow
        let textView: NSTextView

        init(isEditable: Bool) throws {
            let host = NSHostingView(rootView: AppKitManifestTextView(
                text: .constant(Self.document),
                isEditable: isEditable,
                contentStyle: .yaml
            ).frame(width: 480, height: 400))
            host.frame = NSRect(x: 0, y: 0, width: 480, height: 400)
            window = NSWindow(contentRect: host.frame, styleMask: [.titled], backing: .buffered, defer: false)
            window.isReleasedWhenClosed = false
            window.contentView = host
            window.makeKeyAndOrderFront(nil)
            for _ in 0..<6 {
                host.layoutSubtreeIfNeeded()
                RunLoop.main.run(until: Date().addingTimeInterval(0.03))
            }
            textView = try XCTUnwrap(Self.firstTextView(in: host))
        }

        var scrollView: NSScrollView { textView.enclosingScrollView! }
        var offset: CGFloat { scrollView.contentView.bounds.minY }
        var visibleHeight: CGFloat { scrollView.contentView.bounds.height }

        func close() {
            window.orderOut(nil)
        }

        private static func firstTextView(in view: NSView) -> NSTextView? {
            if let textView = view as? NSTextView { return textView }
            for subview in view.subviews {
                if let found = firstTextView(in: subview) { return found }
            }
            return nil
        }
    }
}
