import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class YAMLFindCursorRuntimeTests: XCTestCase {
    func testNextMatchClickPreservesSearchAndDocumentCarets() async throws {
        let model = YAMLFindCursorRuntimeModel()
        model.text = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: first
          labels:
            name: second
        """
        model.query = "name"
        model.isFindPresented = true

        let host = NSHostingController(
            rootView: YAMLFindCursorRuntimeHarness(model: model)
                .frame(width: 700, height: 420)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 700, height: 420),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer {
            window.orderOut(nil)
            window.contentViewController = nil
        }

        try await settle(window: window)
        let scope = try XCTUnwrap(cursorScopes(in: host.view).first)
        let documentEditor = try XCTUnwrap(
            findManifestTextView(in: host.view),
            "Expected the editable YAML NSTextView."
        )
        let searchField = try XCTUnwrap(
            nativeTextFields(in: host.view).first,
            "Expected the native search text field."
        )

        XCTAssertTrue(window.makeFirstResponder(searchField))
        searchField.selectText(nil)
        let searchFieldEditor = try XCTUnwrap(searchField.currentEditor() as? NSTextView)
        let searchCaret = NSRange(location: 2, length: 0)
        let documentCaret = NSRange(location: 5, length: 0)
        searchFieldEditor.setSelectedRange(searchCaret)
        documentEditor.setSelectedRange(documentCaret)
        XCTAssertTrue(window.firstResponder === searchFieldEditor)
        XCTAssertFalse(window.firstResponder === documentEditor)

        let nextPoint = nextMatchButtonCenter(in: scope)
        XCTAssertEqual(scope.cursorIntent(at: nextPoint), .pointer)

        try click(nextPoint, in: scope, window: window)
        try await settle(window: window)

        XCTAssertEqual(model.selectedMatchIndex, 1, "The click must execute Next-match navigation.")
        XCTAssertEqual(
            documentEditor.selectedRange(),
            documentCaret,
            "Find navigation must scroll/highlight without moving the editable document caret."
        )
        XCTAssertTrue(
            searchField.currentEditor() === searchFieldEditor,
            "Next must not replace the native search field editor."
        )
        XCTAssertTrue(
            window.firstResponder === searchFieldEditor,
            "Next must leave keyboard focus and the insertion caret in the search field."
        )
        XCTAssertEqual(
            searchFieldEditor.selectedRange(),
            searchCaret,
            "Next must not move the search field caret."
        )
        XCTAssertFalse(window.firstResponder === documentEditor)
    }

    func testStationaryPointerOverFindNavigationRestoresArrowAfterMouseUp() async throws {
        let model = YAMLFindCursorRuntimeModel()
        model.query = "name"
        model.isFindPresented = true

        let host = NSHostingController(
            rootView: YAMLFindCursorRuntimeHarness(model: model)
                .frame(width: 700, height: 420)
        )
        let window = YAMLFindCursorTestWindow(
            contentRect: NSRect(x: 0, y: 0, width: 700, height: 420),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        let originalPointerLocation = CGEvent(source: nil)?.location
        defer {
            if let originalPointerLocation {
                CGWarpMouseCursorPosition(originalPointerLocation)
            }
            window.orderOut(nil)
            window.contentViewController = nil
        }

        try await settle(window: window)
        let scope = try XCTUnwrap(cursorScopes(in: host.view).first)
        let nextPoint = nextMatchButtonCenter(in: scope)
        XCTAssertEqual(scope.cursorIntent(at: nextPoint), .pointer)
        let nextScreenPoint = try quartzScreenPoint(
            for: nextPoint,
            in: scope,
            window: window
        )
        XCTAssertEqual(CGWarpMouseCursorPosition(nextScreenPoint), .success)
        let actualPointer = scope.convert(
            window.mouseLocationOutsideOfEventStream,
            from: nil
        )
        XCTAssertEqual(actualPointer.x, nextPoint.x, accuracy: 1)
        XCTAssertEqual(actualPointer.y, nextPoint.y, accuracy: 1)

        // Reproduce the stale editable-text I-beam that can win during the
        // same mouse-up transaction while the pointer stays over Next.
        NSCursor.iBeam.set()
        let mouseUp = try mouseEvent(
            type: .leftMouseUp,
            at: nextPoint,
            in: scope,
            window: window
        )
        NSApp.postEvent(mouseUp, atStart: true)
        let deliveredMouseUp = try XCTUnwrap(NSApp.nextEvent(
            matching: .leftMouseUp,
            until: Date().addingTimeInterval(1),
            inMode: .default,
            dequeue: true
        ))
        NSApp.sendEvent(deliveredMouseUp)
        try await settle(window: window)

        let settledPointer = scope.convert(
            window.mouseLocationOutsideOfEventStream,
            from: nil
        )
        XCTAssertEqual(settledPointer.x, nextPoint.x, accuracy: 1)
        XCTAssertEqual(settledPointer.y, nextPoint.y, accuracy: 1)
        XCTAssertEqual(scope.cursorIntent(at: settledPointer), .pointer)
        XCTAssertTrue(
            NSCursor.current === NSCursor.arrow,
            "A stationary pointer over Next must remain an arrow after mouse-up without another mouse-move."
        )
    }

    func testOpeningFindOverEditableYAMLRoutesPointerAndTextCursorsAtRuntime() async throws {
        let model = YAMLFindCursorRuntimeModel()
        let host = NSHostingController(
            rootView: YAMLFindCursorRuntimeHarness(model: model)
                .frame(width: 700, height: 420)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 700, height: 420),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer {
            NSCursor.arrow.set()
            window.orderOut(nil)
        }

        try await settle(window: window)
        let collapsedRegions = cursorRegions(in: host.view)
        XCTAssertEqual(collapsedRegions.count, 1)
        XCTAssertEqual(collapsedRegions.first?.intent, .pointer)

        model.isFindPresented = true
        try await settle(window: window)

        let scopes = cursorScopes(in: host.view)
        let scope = try XCTUnwrap(
            scopes.first,
            "The expanded find bar must install one frontmost cursor scope."
        )
        XCTAssertEqual(scopes.count, 1)
        XCTAssertTrue(
            cursorRegions(in: host.view).isEmpty,
            "Child cursor regions must collapse into the single frontmost find scope."
        )

        let editor = try XCTUnwrap(
            findManifestTextView(in: host.view),
            "Expected the YAML document NSTextView, not the focused search field editor."
        )
        let searchField = try XCTUnwrap(
            nativeTextFields(in: host.view).first,
            "Expected the native search text field inside the expanded find bar."
        )
        let textInputRects = scope.cursorRegionRects(for: .textInput)
        let textInputRect = try XCTUnwrap(
            textInputRects.first,
            "The find scope must resolve a text-input region for the search field."
        )
        XCTAssertEqual(textInputRects.count, 1)
        let nativeSearchRect = searchField.convert(searchField.bounds, to: scope)
        XCTAssertEqual(textInputRect.origin.x, nativeSearchRect.origin.x, accuracy: 0.5)
        XCTAssertEqual(textInputRect.origin.y, nativeSearchRect.origin.y, accuracy: 0.5)
        XCTAssertEqual(textInputRect.width, nativeSearchRect.width, accuracy: 0.5)
        XCTAssertEqual(textInputRect.height, nativeSearchRect.height, accuracy: 0.5)

        let searchPoint = NSPoint(x: textInputRect.midX, y: textInputRect.midY)
        let chromePoint = NSPoint(x: scope.bounds.maxX - 4, y: scope.bounds.maxY - 4)
        let searchPointInHost = scope.convert(searchPoint, to: host.view)
        let chromePointInHost = scope.convert(chromePoint, to: host.view)
        let editorRect = editor.convert(editor.bounds, to: host.view)
        let editorPointInHost = NSPoint(x: editorRect.midX, y: editorRect.midY)
        let editorPointInScope = scope.convert(editorPointInHost, from: host.view)

        XCTAssertEqual(scope.cursorIntent(at: chromePoint), .pointer)
        XCTAssertEqual(scope.cursorIntent(at: searchPoint), .textInput)
        XCTAssertNil(
            scope.cursorIntent(at: editorPointInScope),
            "The find scope must not replace the cursor over the editor below it."
        )
        XCTAssertTrue(
            editorRect.contains(chromePointInHost),
            "The regression requires find chrome layered over the editable NSTextView."
        )
        XCTAssertTrue(editorRect.contains(searchPointInHost))

        assertCursorTrackingArea(on: scope)
        assertNativeTextCursorTrackingArea(on: editor)
        XCTAssertNil(
            scope.hitTest(chromePoint),
            "The cursor scope must not consume clicks intended for find controls."
        )

        assertCursorTransitions(
            in: scope,
            window: window,
            samples: [
                (point: chromePoint, expected: .arrow),
                (point: searchPoint, expected: .iBeam),
                (point: chromePoint, expected: .arrow)
            ]
        )
    }

    private func assertCursorTrackingArea(
        on scope: RuneCursorScopeView,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        let trackingAreas = scope.trackingAreas.filter { area in
            area.owner === scope
                && area.options.contains(.cursorUpdate)
                && area.options.contains(.mouseMoved)
                && area.options.contains(.mouseEnteredAndExited)
                && area.options.contains(.activeInKeyWindow)
                && area.options.contains(.inVisibleRect)
        }
        XCTAssertEqual(
            trackingAreas.count,
            1,
            "The find scope must own one complete AppKit cursor tracking area.",
            file: file,
            line: line
        )
    }

    private func assertNativeTextCursorTrackingArea(
        on textView: NSTextView,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertTrue(textView.isEditable)
        XCTAssertTrue(textView.isSelectable)
        XCTAssertTrue(
            textView.trackingAreas.contains { $0.options.contains(.cursorUpdate) },
            "The editable YAML document must retain AppKit's native text-cursor tracking.",
            file: file,
            line: line
        )
    }

    private func assertCursorTransitions(
        in scope: RuneCursorScopeView,
        window: NSWindow,
        samples: [(point: NSPoint, expected: NSCursor)],
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        NSCursor.closedHand.set()
        for sample in samples {
            let windowPoint = scope.convert(sample.point, to: nil)
            guard let event = NSEvent.mouseEvent(
                with: .mouseMoved,
                location: windowPoint,
                modifierFlags: [],
                timestamp: ProcessInfo.processInfo.systemUptime,
                windowNumber: window.windowNumber,
                context: nil,
                eventNumber: 0,
                clickCount: 0,
                pressure: 0
            ) else {
                return XCTFail("Could not create mouse-moved event.", file: file, line: line)
            }

            scope.mouseMoved(with: event)
            XCTAssertTrue(
                NSCursor.current === sample.expected,
                """
                Expected \(cursorName(sample.expected)) at scope point \(sample.point), \
                got \(cursorName(NSCursor.current)).
                """,
                file: file,
                line: line
            )
        }
    }

    private func cursorRegions(in root: NSView) -> [RuneCursorRectView] {
        var result: [RuneCursorRectView] = []
        if let region = root as? RuneCursorRectView {
            result.append(region)
        }
        for subview in root.subviews {
            result.append(contentsOf: cursorRegions(in: subview))
        }
        return result
    }

    private func cursorScopes(in root: NSView) -> [RuneCursorScopeView] {
        var result: [RuneCursorScopeView] = []
        if let scope = root as? RuneCursorScopeView {
            result.append(scope)
        }
        for subview in root.subviews {
            result.append(contentsOf: cursorScopes(in: subview))
        }
        return result
    }

    private func nativeTextFields(in root: NSView) -> [NSTextField] {
        var result: [NSTextField] = []
        if let field = root as? NSTextField {
            result.append(field)
        }
        for subview in root.subviews {
            result.append(contentsOf: nativeTextFields(in: subview))
        }
        return result
    }

    private func nextMatchButtonCenter(in scope: RuneCursorScopeView) -> NSPoint {
        let trailingInset = RuneUILayoutMetrics.inspectorOverlayInset
            + RuneUILayoutMetrics.inspectorControlContentInset
        let trailingButtonCount = CGFloat(2)
        let distanceFromTrailingEdge = RuneUILayoutMetrics.iconButtonSize / 2
            + trailingButtonCount * (
                RuneUILayoutMetrics.iconButtonSize
                    + RuneAdaptiveToolbarMetrics.groupSpacing
            )
        return NSPoint(
            x: scope.bounds.maxX - trailingInset - distanceFromTrailingEdge,
            y: scope.bounds.midY
        )
    }

    private func click(
        _ point: NSPoint,
        in scope: RuneCursorScopeView,
        window: NSWindow
    ) throws {
        let windowPoint = scope.convert(point, to: nil)
        let down = try XCTUnwrap(NSEvent.mouseEvent(
            with: .leftMouseDown,
            location: windowPoint,
            modifierFlags: [],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            eventNumber: 1,
            clickCount: 1,
            pressure: 1
        ))
        let up = try XCTUnwrap(NSEvent.mouseEvent(
            with: .leftMouseUp,
            location: windowPoint,
            modifierFlags: [],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            eventNumber: 2,
            clickCount: 1,
            pressure: 0
        ))
        NSApp.sendEvent(down)
        NSApp.sendEvent(up)
    }

    private func quartzScreenPoint(
        for point: NSPoint,
        in scope: RuneCursorScopeView,
        window: NSWindow
    ) throws -> CGPoint {
        let appKitPoint = window.convertPoint(
            toScreen: scope.convert(point, to: nil)
        )
        let screen = try XCTUnwrap(window.screen)
        let screenNumberKey = NSDeviceDescriptionKey("NSScreenNumber")
        let screenNumber = try XCTUnwrap(
            screen.deviceDescription[screenNumberKey] as? NSNumber
        )
        let displayBounds = CGDisplayBounds(
            CGDirectDisplayID(screenNumber.uint32Value)
        )
        return CGPoint(
            x: displayBounds.minX + appKitPoint.x - screen.frame.minX,
            y: displayBounds.minY + screen.frame.maxY - appKitPoint.y
        )
    }

    private func mouseEvent(
        type: NSEvent.EventType,
        at point: NSPoint,
        in scope: RuneCursorScopeView,
        window: NSWindow
    ) throws -> NSEvent {
        try XCTUnwrap(NSEvent.mouseEvent(
            with: type,
            location: scope.convert(point, to: nil),
            modifierFlags: [],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            eventNumber: 0,
            clickCount: 0,
            pressure: 0
        ))
    }

    private func findManifestTextView(in root: NSView) -> NSTextView? {
        if let textView = root as? NSTextView,
           textView.string.contains("apiVersion: v1") {
            return textView
        }
        for subview in root.subviews {
            if let match = findManifestTextView(in: subview) {
                return match
            }
        }
        return nil
    }

    private func cursorName(_ cursor: NSCursor) -> String {
        if cursor === NSCursor.arrow { return "arrow" }
        if cursor === NSCursor.iBeam { return "iBeam" }
        if cursor === NSCursor.closedHand { return "closedHand" }
        if cursor === NSCursor.openHand { return "openHand" }
        if cursor === NSCursor.pointingHand { return "pointingHand" }
        if cursor === NSCursor.crosshair { return "crosshair" }
        if cursor === NSCursor.operationNotAllowed { return "operationNotAllowed" }
        return "unknown(\(cursor))"
    }

    private func settle(window: NSWindow) async throws {
        for _ in 0..<6 {
            window.contentView?.layoutSubtreeIfNeeded()
            await Task.yield()
            try await Task.sleep(for: .milliseconds(10))
        }
    }
}

private final class YAMLFindCursorTestWindow: NSWindow {
    override var isKeyWindow: Bool { true }
}

@MainActor
private final class YAMLFindCursorRuntimeModel: ObservableObject {
    @Published var text = """
    apiVersion: v1
    kind: Pod
    metadata:
      name: sample
    """
    @Published var query = ""
    @Published var matchCase = false
    @Published var selectedMatchIndex = 0
    @Published var isFindPresented = false
}

private struct YAMLFindCursorRuntimeHarness: View {
    @ObservedObject var model: YAMLFindCursorRuntimeModel

    var body: some View {
        FindableInspectorSurface(
            text: model.text,
            placeholder: "Find in YAML",
            query: $model.query,
            matchCase: $model.matchCase,
            selectedMatchIndex: $model.selectedMatchIndex,
            isFindPresented: $model.isFindPresented
        ) { searchIndex, searchNavigationRevision in
            AppKitManifestTextView(
                text: $model.text,
                isEditable: true,
                contentStyle: .yaml,
                showsLineNumbers: true,
                searchQuery: model.query,
                searchMatchCase: model.matchCase,
                selectedSearchMatchIndex: model.selectedMatchIndex,
                searchMatchRanges: searchIndex?.ranges ?? [],
                searchNavigationRevision: searchNavigationRevision
            )
        }
    }
}
