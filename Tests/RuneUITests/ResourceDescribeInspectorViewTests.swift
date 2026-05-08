import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class ResourceDescribeInspectorViewTests: XCTestCase {
    func testDescribeSurfaceUsesSharedInspectorReadOnlyTextSurface() throws {
        let source = try String(contentsOfFile: describeTextViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("InspectorReadOnlyTextSurface("))
        XCTAssertTrue(source.contains("contentStyle: .describe"))
        XCTAssertFalse(source.contains("struct DescribeReadOnlyTextView"))
    }

    func testDescribeOutputHighlightsKubectlDescribeKeys() async throws {
        let describeText = """
        Name: api-0
        Namespace: default
        Containers:
          api:
            Image: example/app:1
        """
        let (host, window) = makeDescribeHost(describeText: describeText)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        guard let scrollView = findTextScrollView(in: host.view),
              let textView = scrollView.documentView as? NSTextView
        else {
            return XCTFail("Expected describe NSTextView-backed scroll view")
        }

        let source = textView.string as NSString
        let keyRange = source.range(of: "Name")
        let valueRange = source.range(of: "api-0")
        let sectionRange = source.range(of: "Containers")

        let keyColor = textView.textStorage?.attribute(.foregroundColor, at: keyRange.location, effectiveRange: nil) as? NSColor
        let valueColor = textView.textStorage?.attribute(.foregroundColor, at: valueRange.location, effectiveRange: nil) as? NSColor
        let sectionColor = textView.textStorage?.attribute(.foregroundColor, at: sectionRange.location, effectiveRange: nil) as? NSColor

        XCTAssertNotNil(keyColor)
        XCTAssertNotEqual(keyColor, valueColor, "Describe keys should be visually distinct from values, matching the k9s-style key/value scan pattern.")
        XCTAssertNotEqual(sectionColor, valueColor, "Describe section headers should stand out from plain values.")
    }

    func testDescribePaneDoesNotPlaceFooterBelowScrollableTextSurface() throws {
        let source = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)
        let sharedLayoutSource = try String(contentsOfFile: resourceManifestInspectorLayoutPath, encoding: .utf8)

        XCTAssertTrue(source.contains("ResourceManifestInspectorLayout"))
        XCTAssertTrue(sharedLayoutSource.contains(".layoutPriority(1)"))
        XCTAssertFalse(
            source.contains("The pane above is describe output from the cluster"),
            "Describe panes should behave like YAML panes: fixed controls above, one bounded scroll surface below. A footer after the scroll surface can push/clamp content outside the inspector."
        )
    }

    func testYAMLAndDescribeShareManifestInspectorLayout() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)
        let textSurfaceSource = try String(contentsOfFile: inspectorTextViewsPath, encoding: .utf8)

        XCTAssertTrue(yamlSource.contains("ResourceManifestInspectorLayout"))
        XCTAssertTrue(describeSource.contains("ResourceManifestInspectorLayout"))
        XCTAssertTrue(textSurfaceSource.contains("GeometryReader"))
        XCTAssertTrue(textSurfaceSource.contains("height: max(1, proxy.size.height)"))
    }

    func testUnsavedEditsIndicatorUsesReservedSlotToAvoidLayoutJumps() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(yamlSource.contains("ManifestUnsavedEditsSlot(isVisible: hasUnsavedEdits)"))
        XCTAssertTrue(describeSource.contains("ManifestUnsavedEditsSlot(isVisible: hasUnsavedEdits)"))
        XCTAssertTrue(yamlSource.contains("struct ManifestUnsavedEditsSlot"))
        XCTAssertTrue(yamlSource.contains(".opacity(isVisible ? 1 : 0)"))
        XCTAssertTrue(yamlSource.contains(".accessibilityHidden(!isVisible)"))
        XCTAssertTrue(yamlSource.contains(".allowsHitTesting(isVisible)"))
    }

    func testYAMLAndDescribeUseGroupedManifestToolbarAndCompactStatus() throws {
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)

        let yamlPaneSource = try XCTUnwrap(yamlSource.slice(
            from: "struct ResourceYAMLInspectorPane",
            to: "struct ManifestUnsavedEditsChip"
        ))
        let describePaneSource = describeSource

        XCTAssertTrue(yamlPaneSource.contains("ManifestToolbarScrollRow"))
        XCTAssertTrue(yamlPaneSource.contains("ManifestToolbarGroup"))
        XCTAssertTrue(yamlPaneSource.contains("ManifestStatusChip(text: statusText"))
        XCTAssertTrue(yamlPaneSource.contains("ManifestInlineNote("))

        XCTAssertTrue(describePaneSource.contains("ManifestToolbarScrollRow"))
        XCTAssertTrue(describePaneSource.contains("ManifestToolbarGroup"))
        XCTAssertTrue(describePaneSource.contains("ManifestStatusChip(text: statusText"))
        XCTAssertTrue(describePaneSource.contains("ManifestInlineNote("))

        XCTAssertFalse(yamlPaneSource.contains("Text(statusText)"))
        XCTAssertFalse(describePaneSource.contains("Text(statusText)"))
    }

    func testDescribePaneAllowsScrollingLongDescribeOutput() async throws {
        let describeText = makeLongDescribeText()
        let (host, window) = makeDescribeHost(describeText: describeText)
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
        let documentHeight = scrollView.documentView?.frame.height ?? 0

        XCTAssertGreaterThan(usedHeight, viewportHeight + 200, "Describe content should overflow vertically")
        XCTAssertGreaterThan(documentHeight, viewportHeight + 200, "Describe scroll view document should be taller than the viewport")

        let targetOffset = min(240, max(40, usedHeight - viewportHeight - 20))
        scrollView.contentView.scroll(to: NSPoint(x: 0, y: targetOffset))
        scrollView.reflectScrolledClipView(scrollView.contentView)

        try await settle(window: window)

        XCTAssertGreaterThan(scrollView.contentView.bounds.origin.y, 0, "Describe pane should scroll downward")
    }

    func testDescribeTextSurfaceStaysInsideInspectorBounds() async throws {
        let (host, window) = makeDescribeHost(describeText: makeLongDescribeText())
        defer { window.orderOut(nil) }

        try await settle(window: window)

        guard let scrollView = findTextScrollView(in: host.view) else {
            return XCTFail("Expected describe NSTextView-backed scroll view")
        }

        let scrollFrame = scrollView.convert(scrollView.bounds, to: host.view)
        let hostBounds = host.view.bounds

        XCTAssertGreaterThanOrEqual(scrollFrame.minX, hostBounds.minX - 1, "Describe text surface should not render left of its inspector")
        XCTAssertGreaterThanOrEqual(scrollFrame.minY, hostBounds.minY - 1, "Describe text surface should not render below its inspector")
        XCTAssertLessThanOrEqual(scrollFrame.maxX, hostBounds.maxX + 1, "Describe text surface should not render right of its inspector")
        XCTAssertLessThanOrEqual(scrollFrame.maxY, hostBounds.maxY + 1, "Describe text surface should not render above its inspector")
    }

    private func makeDescribeHost(describeText: String) -> (NSHostingController<some View>, NSWindow) {
        let host = NSHostingController(
            rootView: ResourceDescribeInspectorPane(
                describeText: describeText,
                resourceReference: "pod api-0",
                canApplyMutations: true,
                yamlText: "apiVersion: v1\nkind: Pod\n",
                hasUnsavedEdits: true,
                validationIssues: [],
                statusText: "Last updated 12:00:00",
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
        return (host, window)
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

    private var describeTextViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/DescribeTextView.swift").path
    }

    private var resourceDescribeInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceDescribeInspectorView.swift").path
    }

    private var resourceYAMLInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceYAMLInspectorView.swift").path
    }

    private var resourceManifestInspectorLayoutPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceManifestInspectorLayout.swift").path
    }

private var inspectorTextViewsPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/InspectorTextViews.swift").path
    }
}

private extension String {
    func slice(from start: String, to end: String) -> String? {
        guard let startRange = range(of: start),
              let endRange = range(of: end, range: startRange.upperBound..<endIndex) else {
            return nil
        }
        return String(self[startRange.lowerBound..<endRange.lowerBound])
    }
}
