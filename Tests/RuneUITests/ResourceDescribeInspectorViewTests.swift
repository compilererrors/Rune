import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class ResourceDescribeInspectorViewTests: XCTestCase {
    func testDescribePaneAllowsScrollingLongDescribeOutput() async throws {
        let describeText = makeLongDescribeText()
        let host = NSHostingController(
            rootView: ResourceDescribeInspectorPane(
                describeText: describeText,
                resourceReference: "pod api-0",
                canApplyMutations: true,
                yamlText: "apiVersion: v1\nkind: Pod\n",
                hasUnsavedEdits: true,
                validationIssues: [],
                onApply: {},
                onOpenYAMLEditor: {},
                readOnlyResetID: "describe-scroll-test"
            )
            .frame(width: 640, height: 520)
        )

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 640, height: 520),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        guard let scrollView = findTextScrollView(in: host.view),
              let textView = scrollView.documentView as? NSTextView,
              let textContainer = textView.textContainer,
              let layoutManager = textView.layoutManager
        else {
            return XCTFail("Expected describe NSTextView-backed scroll view")
        }

        layoutManager.ensureLayout(for: textContainer)
        let usedHeight = layoutManager.usedRect(for: textContainer).height + textView.textContainerInset.height * 2
        let viewportHeight = scrollView.contentView.bounds.height

        XCTAssertGreaterThan(usedHeight, viewportHeight + 200, "Describe content should overflow vertically")

        let targetOffset = min(240, max(40, usedHeight - viewportHeight - 20))
        scrollView.contentView.scroll(to: NSPoint(x: 0, y: targetOffset))
        scrollView.reflectScrolledClipView(scrollView.contentView)

        try await settle(window: window)

        XCTAssertGreaterThan(scrollView.contentView.bounds.origin.y, 0, "Describe pane should scroll downward")
    }

    private func settle(window: NSWindow) async throws {
        for _ in 0..<5 {
            window.contentView?.layoutSubtreeIfNeeded()
            hostRunLoopTick()
            try await Task.sleep(nanoseconds: 30_000_000)
        }
    }

    private func hostRunLoopTick() {
        RunLoop.current.run(until: Date().addingTimeInterval(0.01))
    }

    private func findTextScrollView(in view: NSView) -> NSScrollView? {
        if let scrollView = view as? NSScrollView,
           scrollView.documentView is NSTextView {
            return scrollView
        }

        for subview in view.subviews {
            if let match = findTextScrollView(in: subview) {
                return match
            }
        }

        return nil
    }

    private func makeLongDescribeText() -> String {
        (1...220).map { index in
            """
            Name: pod-api-\(index)
            Namespace: backend
            Status: Running
            Containers:
              api:
                Image: company/api:\(index)
                Args:
                  --feature-flag=enabled
                  --replica=\(index)
            """
        }
        .joined(separator: "\n\n")
    }
}
