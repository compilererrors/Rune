import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class InspectorDocumentGridTests: XCTestCase {
    func testInspectorDocumentsShareTenPointInsets() {
        XCTAssertEqual(RuneUILayoutMetrics.inspectorDocumentHorizontalInset, 10)
        XCTAssertEqual(RuneUILayoutMetrics.inspectorDocumentVerticalInset, 10)
    }

    func testPlainAppKitDocumentUsesSharedInsets() async throws {
        let (host, window) = makeHost(
            AppKitManifestTextView(
                text: .constant("Name: synthetic-api\nStatus: Ready\n"),
                isEditable: false,
                contentStyle: .describe
            )
        )
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let textView = try XCTUnwrap(findTextView(in: host.view, containing: "synthetic-api"))
        XCTAssertEqual(
            textView.textContainerInset.width,
            RuneUILayoutMetrics.inspectorDocumentHorizontalInset,
            accuracy: 0.5
        )
        XCTAssertEqual(
            textView.textContainerInset.height,
            RuneUILayoutMetrics.inspectorDocumentVerticalInset,
            accuracy: 0.5
        )
    }

    func testTerminalAppKitDocumentUsesSharedInsets() async throws {
        let (host, window) = makeHost(
            TerminalTranscriptSurface(
                text: "synthetic terminal output\n",
                height: 180,
                resetID: "synthetic-terminal",
                fontSize: 12
            )
        )
        defer { window.orderOut(nil) }

        try await settle(window: window)

        let textView = try XCTUnwrap(findTextView(in: host.view, containing: "synthetic terminal output"))
        XCTAssertEqual(
            textView.textContainerInset.width,
            RuneUILayoutMetrics.inspectorDocumentHorizontalInset,
            accuracy: 0.5
        )
        XCTAssertEqual(
            textView.textContainerInset.height,
            RuneUILayoutMetrics.inspectorDocumentVerticalInset,
            accuracy: 0.5
        )
    }

    func testLargeDocumentLineNumbersRemainExplicitByDocumentType() throws {
        let largeTextSource = try source("Sources/RuneSharedUI/RuneSharedComponents.swift")
        let inspectorTextSource = try source("Sources/RuneUI/Views/InspectorTextViews.swift")
        let yamlSource = try source("Sources/RuneUI/Views/ResourceYAMLInspectorView.swift")
        let logsSource = try source("Sources/RuneUI/Views/ResourceLogsInspectorView.swift")
        let terminalSource = try source("Sources/RuneUI/Views/TerminalTranscriptSurface.swift")

        XCTAssertTrue(largeTextSource.contains("horizontalContentInset: CGFloat = 10"))
        XCTAssertTrue(largeTextSource.contains("verticalContentInset: CGFloat = 8"))
        XCTAssertTrue(largeTextSource.contains(".padding(.horizontal, horizontalContentInset)"))
        XCTAssertTrue(largeTextSource.contains(".padding(.vertical, verticalContentInset)"))
        XCTAssertTrue(
            inspectorTextSource.contains(
                "horizontalContentInset: RuneUILayoutMetrics.inspectorDocumentHorizontalInset"
            )
        )
        XCTAssertTrue(
            inspectorTextSource.contains(
                "verticalContentInset: RuneUILayoutMetrics.inspectorDocumentVerticalInset"
            )
        )
        XCTAssertTrue(
            terminalSource.contains(
                "horizontalContentInset: RuneUILayoutMetrics.inspectorDocumentHorizontalInset"
            )
        )
        XCTAssertTrue(
            terminalSource.contains(
                "verticalContentInset: RuneUILayoutMetrics.inspectorDocumentVerticalInset"
            )
        )
        XCTAssertTrue(inspectorTextSource.contains("var largeTextShowsLineNumbers = false"))
        XCTAssertTrue(yamlSource.contains("largeTextShowsLineNumbers: true"))
        XCTAssertTrue(logsSource.contains("largeTextShowsLineNumbers: false"))
        XCTAssertTrue(terminalSource.contains("showsLineNumbers: false"))
    }

    private func makeHost<Content: View>(
        _ content: Content
    ) -> (NSHostingController<some View>, NSWindow) {
        let rootView = content.frame(width: 520, height: 260)
        let host = NSHostingController(rootView: rootView)
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 520, height: 260),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        return (host, window)
    }

    private func settle(window: NSWindow) async throws {
        for _ in 0..<3 {
            await Task.yield()
            window.contentView?.layoutSubtreeIfNeeded()
        }
    }

    private func findTextView(in view: NSView, containing text: String) -> NSTextView? {
        if let textView = view as? NSTextView, textView.string.contains(text) {
            return textView
        }
        for subview in view.subviews {
            if let match = findTextView(in: subview, containing: text) {
                return match
            }
        }
        return nil
    }

    private func source(_ relativePath: String) throws -> String {
        let repositoryRoot = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return try String(
            contentsOf: repositoryRoot.appendingPathComponent(relativePath),
            encoding: .utf8
        )
    }
}
