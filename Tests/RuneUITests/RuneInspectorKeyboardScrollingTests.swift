import AppKit
import RuneCore
import SwiftUI
import XCTest
@testable import RuneUI

/// End to end through the app's own event path: a real click into the
/// inspector, then real key events through `NSApp.sendEvent`, so the window's
/// key monitor, Rune's pane focus and the responder chain all take part.
@MainActor
final class RuneInspectorKeyboardScrollingTests: XCTestCase {
    func testClickInInspectorThenKeysScrollTheYAMLDocument() async throws {
        let harness = try await Harness(initialYAMLInlineEditing: false)
        defer { harness.close() }

        harness.click(at: harness.inspectorHeaderPoint)
        try await harness.settle()
        let scrollView = try XCTUnwrap(harness.documentScrollView(), "inspector YAML document")
        let start = harness.offset(of: scrollView)

        harness.press(" ", keyCode: 49)
        try await harness.settle(0.4)
        let afterSpace = harness.offset(of: scrollView)
        XCTAssertGreaterThan(afterSpace - start, scrollView.contentView.bounds.height * 0.8, "Space pages the YAML down")

        harness.press("\u{F701}", keyCode: 125, modifiers: [.function, .numericPad])
        try await harness.settle()
        let afterDown = harness.offset(of: scrollView)
        XCTAssertGreaterThan(afterDown, afterSpace, "Down arrow scrolls one line")
        XCTAssertLessThan(afterDown - afterSpace, 40, "a line, not a page")

        harness.press("\u{F701}", keyCode: 126, modifiers: [.function, .numericPad])
        try await harness.settle()
        XCTAssertEqual(harness.offset(of: scrollView), afterSpace, accuracy: 0.5, "Up arrow scrolls back one line")

        harness.press("\u{F72B}", keyCode: 119, modifiers: [.function])
        try await harness.settle(0.4)
        let bottom = harness.offset(of: scrollView)
        XCTAssertEqual(bottom, harness.maxOffset(of: scrollView), accuracy: 1, "End jumps to the bottom")

        harness.press("\u{F729}", keyCode: 115, modifiers: [.function])
        try await harness.settle(0.4)
        XCTAssertEqual(harness.offset(of: scrollView), 0, accuracy: 0.5, "Home jumps to the top")
    }

    func testClickInInspectorThenSpaceScrollsLogs() async throws {
        let harness = try await Harness(initialYAMLInlineEditing: false, tab: .logs)
        defer { harness.close() }
        harness.click(at: harness.inspectorHeaderPoint)
        try await harness.settle()
        let scrollView = try XCTUnwrap(harness.documentScrollView(), "inspector logs document")
        let start = harness.offset(of: scrollView)

        harness.press(" ", keyCode: 49)
        try await harness.settle(0.4)
        XCTAssertGreaterThan(harness.offset(of: scrollView) - start, scrollView.contentView.bounds.height * 0.5, "Space pages the logs down")
    }

    func testSpaceTypesInEditableYAMLInsteadOfScrolling() async throws {
        let harness = try await Harness(initialYAMLInlineEditing: true)
        defer { harness.close() }
        let editor = try XCTUnwrap(harness.manifestTextView(), "editable YAML text view")
        XCTAssertTrue(editor.isEditable)
        XCTAssertTrue(harness.window.makeFirstResponder(editor))
        editor.setSelectedRange(NSRange(location: 0, length: 0))
        let scrollView = try XCTUnwrap(editor.enclosingScrollView)
        let start = harness.offset(of: scrollView)
        let before = editor.string

        harness.press(" ", keyCode: 49)
        try await harness.settle()

        XCTAssertEqual(editor.string.count, before.count + 1, "Space types into editable YAML")
        XCTAssertEqual(harness.offset(of: scrollView), start, accuracy: 0.5)
    }

    func testFocusedFieldOutsideTheInspectorKeepsSpaceForTyping() async throws {
        let harness = try await Harness(initialYAMLInlineEditing: false)
        defer { harness.close() }
        harness.click(at: harness.inspectorHeaderPoint)
        try await harness.settle()
        let scrollView = try XCTUnwrap(harness.documentScrollView())
        let start = harness.offset(of: scrollView)

        let field = NSTextField(string: "")
        field.frame = NSRect(x: 4, y: 4, width: 160, height: 24)
        harness.window.contentView?.addSubview(field)
        XCTAssertTrue(harness.window.makeFirstResponder(field))
        harness.press(" ", keyCode: 49)
        try await harness.settle()

        XCTAssertEqual(field.currentEditor()?.string, " ", "Space types in the focused field")
        XCTAssertEqual(harness.offset(of: scrollView), start, accuracy: 0.5, "the inspector does not scroll")
    }

    @MainActor
    private final class Harness {
        let window: NSWindow
        let viewModel: RuneAppViewModel
        private let host: NSHostingController<AnyView>

        init(initialYAMLInlineEditing: Bool, tab: PodInspectorTab = .yaml) async throws {
            let state = RuneAppState()
            // No kube context, so no background load replaces the document.
            state.setNamespaces(["default"])
            state.selectedNamespace = "default"
            state.selectedSection = .workloads
            state.selectedWorkloadKind = .pod
            let pod = PodSummary(name: "sample-api-0", namespace: "default", status: "Running", ageDescription: "1h")
            state.setPods([pod])
            state.selectedPod = pod
            state.setResourceYAML(Self.longYAML)
            state.setPodLogs(Self.longLogs)
            viewModel = RuneAppViewModel(state: state)
            viewModel.isSidebarVisible = true
            viewModel.isDetailPaneVisible = true
            host = NSHostingController(rootView: AnyView(RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: nil,
                debugDisableBootstrap: true,
                initialPodInspectorTab: tab,
                initialYAMLInlineEditing: initialYAMLInlineEditing
            )))
            window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 1_440, height: 900),
                styleMask: [.titled, .closable, .resizable],
                backing: .buffered,
                defer: false
            )
            window.isReleasedWhenClosed = false
            window.contentViewController = host
            window.setContentSize(NSSize(width: 1_440, height: 900))
            window.makeKeyAndOrderFront(nil)
            try await settle(0.8)
            // A failed background refresh may clear the document; restore it.
            viewModel.state.setResourceYAML(Self.longYAML)
            viewModel.state.setPodLogs(Self.longLogs)
            try await settle(0.5)
        }

        static let longLogs = (1...400).map { "2026-01-01T00:00:\($0 % 60)Z sample log line \($0)" }.joined(separator: "\n")

        static let longYAML = (["apiVersion: v1", "kind: Pod", "metadata:", "  name: sample-api-0", "data:"]
            + (1...300).map { "  key\($0): value-\($0)" }).joined(separator: "\n")

        var anchorRect: NSRect {
            RuneInspectorDocumentAnchor.AnchorView.anchor(in: window)?.paneRectInWindow ?? .zero
        }

        /// A point on the inspector's title, away from buttons and text.
        var inspectorHeaderPoint: NSPoint {
            NSPoint(x: anchorRect.minX + 120, y: anchorRect.maxY - 30)
        }

        func documentScrollView() -> NSScrollView? {
            RuneInspectorKeyboardScrolling.documentScrollView(in: window, paneRect: anchorRect)
        }

        func manifestTextView() -> NSTextView? {
            func find(_ view: NSView) -> NSTextView? {
                if let textView = view as? NSTextView, !textView.isFieldEditor,
                   let scrollView = textView.enclosingScrollView {
                    let frame = scrollView.convert(scrollView.bounds, to: nil)
                    if anchorRect.contains(NSPoint(x: frame.midX, y: frame.midY)) { return textView }
                }
                for subview in view.subviews {
                    if let found = find(subview) { return found }
                }
                return nil
            }
            return window.contentView.flatMap(find)
        }

        func offset(of scrollView: NSScrollView) -> CGFloat {
            let bounds = scrollView.contentView.bounds
            guard let document = scrollView.documentView else { return 0 }
            return document.isFlipped ? bounds.minY : document.frame.height - bounds.maxY
        }

        func maxOffset(of scrollView: NSScrollView) -> CGFloat {
            max(0, (scrollView.documentView?.frame.height ?? 0) - scrollView.contentView.bounds.height)
        }

        func click(at point: NSPoint) {
            for type in [NSEvent.EventType.leftMouseDown, .leftMouseUp] {
                if let event = NSEvent.mouseEvent(
                    with: type, location: point, modifierFlags: [], timestamp: ProcessInfo.processInfo.systemUptime,
                    windowNumber: window.windowNumber, context: nil, eventNumber: 0, clickCount: 1, pressure: 1
                ) {
                    NSApp.sendEvent(event)
                }
            }
        }

        func press(_ characters: String, keyCode: UInt16, modifiers: NSEvent.ModifierFlags = []) {
            if let event = NSEvent.keyEvent(
                with: .keyDown, location: .zero, modifierFlags: modifiers,
                timestamp: ProcessInfo.processInfo.systemUptime, windowNumber: window.windowNumber,
                context: nil, characters: characters, charactersIgnoringModifiers: characters,
                isARepeat: false, keyCode: keyCode
            ) {
                NSApp.sendEvent(event)
            }
        }

        func settle(_ seconds: TimeInterval = 0.15) async throws {
            try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            window.contentView?.layoutSubtreeIfNeeded()
        }

        func close() {
            RuneTestWindow.close(window)
        }
    }
}
