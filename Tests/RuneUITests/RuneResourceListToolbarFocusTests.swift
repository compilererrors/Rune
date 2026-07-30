import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RuneResourceListToolbarFocusTests: XCTestCase {
    func testResourceFilterKeepsActiveEditorAcrossResponsiveLayoutChange() async throws {
        let model = SyntheticResourceToolbarModel()
        let host = NSHostingController(
            rootView: SyntheticResourceToolbarHarness(model: model)
                .frame(width: 820, height: 150, alignment: .topLeading)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 820, height: 150),
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
        let originalField = try XCTUnwrap(
            editableTextFields(in: host.view).first { $0.placeholderString == "Filter resources" }
        )
        window.makeFirstResponder(originalField)
        try await settle(window: window)
        let originalEditor = try XCTUnwrap(originalField.currentEditor() as? NSTextView)
        originalEditor.setSelectedRange(NSRange(location: 3, length: 0))

        model.width = 520
        try await settle(window: window)

        let compactField = try XCTUnwrap(
            editableTextFields(in: host.view).first { $0.placeholderString == "Filter resources" }
        )
        XCTAssertTrue(compactField === originalField)
        XCTAssertTrue(compactField.currentEditor() === originalEditor)
        XCTAssertTrue(window.firstResponder === originalEditor)
        XCTAssertEqual(originalEditor.selectedRange(), NSRange(location: 3, length: 0))
    }

    private func editableTextFields(in view: NSView) -> [NSTextField] {
        var fields: [NSTextField] = []
        if let textField = view as? NSTextField, textField.isEditable {
            fields.append(textField)
        }
        for subview in view.subviews {
            fields.append(contentsOf: editableTextFields(in: subview))
        }
        return fields
    }

    private func settle(window: NSWindow) async throws {
        for _ in 0..<6 {
            window.contentView?.layoutSubtreeIfNeeded()
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }
}

@MainActor
private final class SyntheticResourceToolbarModel: ObservableObject {
    @Published var width: CGFloat = 820
    @Published var query = "synthetic"
}

private struct SyntheticResourceToolbarHarness: View {
    @ObservedObject var model: SyntheticResourceToolbarModel

    var body: some View {
        RuneResourceListToolbar {
            TextField("Filter resources", text: $model.query)
                .textFieldStyle(.roundedBorder)
        } actions: {
            HStack {
                Button("Refresh") {}
                Button("Columns") {}
            }
        }
        .frame(width: model.width)
    }
}
