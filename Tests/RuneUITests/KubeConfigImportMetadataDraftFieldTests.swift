import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class KubeConfigImportMetadataDraftFieldTests: XCTestCase {
    @MainActor
    func testNormalizedModelUpdatesDoNotReplaceActiveRawTextOrMoveCaret() async throws {
        let model = KubeConfigImportMetadataDraftFieldHarnessModel(
            value: "foo",
            canonicalize: KubeConfigImportMetadataDraft.canonicalText
        )
        let hosted = makeHostedField(title: "Alias", model: model)
        defer { closeTestWindow(hosted.window) }

        let textField = try XCTUnwrap(findEditableTextField(in: hosted.host.view))
        hosted.window.makeFirstResponder(textField)
        try await settle(window: hosted.window)

        let fieldEditor = try XCTUnwrap(textField.currentEditor() as? NSTextView)
        fieldEditor.setSelectedRange(NSRange(location: fieldEditor.string.utf16.count, length: 0))
        fieldEditor.insertText(" ", replacementRange: fieldEditor.selectedRange())
        try await settle(window: hosted.window)

        XCTAssertEqual(model.value, "foo")
        XCTAssertEqual(model.rawUpdates.last, "foo ")
        XCTAssertEqual(fieldEditor.string, "foo ")
        XCTAssertEqual(fieldEditor.selectedRange(), NSRange(location: 4, length: 0))
        XCTAssertTrue(textField.currentEditor() === fieldEditor)

        fieldEditor.insertText("b", replacementRange: fieldEditor.selectedRange())
        try await settle(window: hosted.window)

        XCTAssertEqual(model.value, "foo b")
        XCTAssertEqual(model.rawUpdates.last, "foo b")
        XCTAssertEqual(fieldEditor.string, "foo b")
        XCTAssertEqual(fieldEditor.selectedRange(), NSRange(location: 5, length: 0))
        XCTAssertTrue(textField.currentEditor() === fieldEditor)

        fieldEditor.insertText(" ", replacementRange: fieldEditor.selectedRange())
        try await settle(window: hosted.window)
        XCTAssertEqual(fieldEditor.string, "foo b ")

        XCTAssertTrue(textField.sendAction(textField.action, to: textField.target))
        try await settle(window: hosted.window)

        XCTAssertEqual(model.value, "foo b")
        XCTAssertEqual(fieldEditor.string, "foo b")
    }

    @MainActor
    func testTagNormalizationDoesNotReplaceMidStringEditingAndCanonicalizesOnFocusLoss() async throws {
        let model = KubeConfigImportMetadataDraftFieldHarnessModel(
            value: "redblue",
            canonicalize: KubeConfigImportMetadataDraft.canonicalTagsText
        )
        let hosted = makeHostedField(title: "Tags", model: model)
        defer { closeTestWindow(hosted.window) }

        let textField = try XCTUnwrap(findEditableTextField(in: hosted.host.view))
        hosted.window.makeFirstResponder(textField)
        try await settle(window: hosted.window)

        let fieldEditor = try XCTUnwrap(textField.currentEditor() as? NSTextView)
        fieldEditor.setSelectedRange(NSRange(location: 3, length: 0))
        fieldEditor.insertText(",", replacementRange: fieldEditor.selectedRange())
        try await settle(window: hosted.window)

        XCTAssertEqual(model.value, "blue, red")
        XCTAssertEqual(fieldEditor.string, "red,blue")
        XCTAssertEqual(fieldEditor.selectedRange(), NSRange(location: 4, length: 0))

        fieldEditor.insertText(" ", replacementRange: fieldEditor.selectedRange())
        fieldEditor.setSelectedRange(NSRange(location: fieldEditor.string.utf16.count, length: 0))
        fieldEditor.insertText(", ", replacementRange: fieldEditor.selectedRange())
        try await settle(window: hosted.window)

        XCTAssertEqual(model.value, "blue, red")
        XCTAssertEqual(model.rawUpdates.last, "red, blue, ")
        XCTAssertEqual(fieldEditor.string, "red, blue, ")
        XCTAssertEqual(fieldEditor.selectedRange(), NSRange(location: 11, length: 0))

        hosted.window.makeFirstResponder(nil)
        try await settle(window: hosted.window)

        XCTAssertEqual(model.value, "blue, red")
        XCTAssertEqual(textField.stringValue, "blue, red")
    }

    func testMetadataDraftCanonicalizationMatchesStoredMetadataRules() {
        XCTAssertEqual(KubeConfigImportMetadataDraft.canonicalText("  alias  "), "alias")
        XCTAssertEqual(
            KubeConfigImportMetadataDraft.canonicalTagsText("red, blue, red, "),
            "blue, red"
        )
    }

    @MainActor
    private func makeHostedField(
        title: String,
        model: KubeConfigImportMetadataDraftFieldHarnessModel
    ) -> (
        host: NSHostingController<AnyView>,
        window: NSWindow
    ) {
        let host = NSHostingController(
            rootView: AnyView(
                KubeConfigImportMetadataDraftFieldHarness(title: title, model: model)
                    .frame(width: 320, height: 60)
            )
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 320, height: 60),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        host.view.layoutSubtreeIfNeeded()
        return (host, window)
    }

    @MainActor
    private func findEditableTextField(in view: NSView) -> NSTextField? {
        if let textField = view as? NSTextField, textField.isEditable {
            return textField
        }
        return view.subviews.lazy.compactMap(findEditableTextField).first
    }

    @MainActor
    private func settle(window: NSWindow) async throws {
        for _ in 0..<6 {
            window.contentView?.layoutSubtreeIfNeeded()
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    @MainActor
    private func closeTestWindow(_ window: NSWindow) {
        window.orderOut(nil)
        window.contentViewController = nil
        window.contentView = nil
    }
}

@MainActor
private final class KubeConfigImportMetadataDraftFieldHarnessModel: ObservableObject {
    @Published var value: String
    private(set) var rawUpdates: [String] = []
    private let canonicalize: (String) -> String

    init(value: String, canonicalize: @escaping (String) -> String) {
        self.value = value
        self.canonicalize = canonicalize
    }

    func update(_ rawValue: String) {
        rawUpdates.append(rawValue)
        value = canonicalize(rawValue)
    }
}

private struct KubeConfigImportMetadataDraftFieldHarness: View {
    let title: String
    @ObservedObject var model: KubeConfigImportMetadataDraftFieldHarnessModel

    var body: some View {
        KubeConfigImportMetadataDraftField(
            title,
            value: model.value,
            canonicalize: model.canonicalizeForField,
            onChange: model.update
        )
        .textFieldStyle(.roundedBorder)
        .padding()
    }
}

private extension KubeConfigImportMetadataDraftFieldHarnessModel {
    func canonicalizeForField(_ value: String) -> String {
        canonicalize(value)
    }
}
