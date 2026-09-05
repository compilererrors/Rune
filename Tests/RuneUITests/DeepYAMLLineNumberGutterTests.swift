import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class DeepYAMLLineNumberGutterTests: XCTestCase {
    @MainActor
    func testDeepScrollShowsConsecutiveLineNumbersAtDocumentEnd() throws {
        let lineCount = 25_000
        let manifest = (1...lineCount)
            .map { line in "synthetic-key-\(String(format: "%05d", line)): \"värde-λ-終-🧪-\(line)\"" }
            .joined(separator: "\r\n")
        let result = try lineNumbersAtDocumentEnd(in: manifest)

        XCTAssertGreaterThan(result.maximumScrollOffset, result.viewportHeight * 100)
        XCTAssertFalse(result.lineNumbers.isEmpty)
        XCTAssertEqual(result.lineNumbers.last, lineCount)
        XCTAssertEqual(
            result.lineNumbers,
            Array((try XCTUnwrap(result.lineNumbers.first))...lineCount),
            "Deep gutter labels must remain consecutive and aligned with the final YAML line."
        )
    }

    @MainActor
    func testCarriageReturnOnlyYAMLKeepsGutterLineNumbersAligned() throws {
        let lineCount = 600
        let manifest = (1...lineCount)
            .map { line in "synthetic-key-\(line): value-\(line)" }
            .joined(separator: "\r")
        let result = try lineNumbersAtDocumentEnd(in: manifest)

        XCTAssertFalse(result.lineNumbers.isEmpty)
        XCTAssertEqual(result.lineNumbers.last, lineCount)
        XCTAssertEqual(
            result.lineNumbers,
            Array((try XCTUnwrap(result.lineNumbers.first))...lineCount)
        )
    }

    @MainActor
    private func lineNumbersAtDocumentEnd(
        in manifest: String
    ) throws -> (lineNumbers: [Int], maximumScrollOffset: CGFloat, viewportHeight: CGFloat) {
        let host = NSHostingController(
            rootView: AppKitManifestTextView(
                text: .constant(manifest),
                isEditable: true,
                contentStyle: .yaml,
                showsLineNumbers: true
            )
            .frame(width: 640, height: 420)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 640, height: 420),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.contentView?.layoutSubtreeIfNeeded()
        defer { window.orderOut(nil) }

        let scrollView = try XCTUnwrap(findManifestTextScrollView(in: host.view))
        let textView = try XCTUnwrap(scrollView.documentView as? NSTextView)
        let textContainer = try XCTUnwrap(textView.textContainer)
        textView.layoutManager?.ensureLayout(for: textContainer)
        window.contentView?.layoutSubtreeIfNeeded()

        let maximumScrollOffset = max(
            0,
            textView.bounds.height - scrollView.contentView.bounds.height
        )

        scrollView.contentView.scroll(to: NSPoint(x: 0, y: maximumScrollOffset))
        scrollView.reflectScrolledClipView(scrollView.contentView)
        scrollView.refreshLineNumberGutter()

        return (
            scrollView.lineNumberGutterView.visibleLineNumberLabels().map(\.number),
            maximumScrollOffset,
            scrollView.contentView.bounds.height
        )
    }

    @MainActor
    private func findManifestTextScrollView(in view: NSView) -> ManifestTextScrollView? {
        if let scrollView = view as? ManifestTextScrollView {
            return scrollView
        }
        for subview in view.subviews {
            if let match = findManifestTextScrollView(in: subview) {
                return match
            }
        }
        return nil
    }
}
