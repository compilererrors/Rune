import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RuneFindBarActionRailLayoutTests: XCTestCase {
    func testFindBarsUseOneRowAtNormalInspectorWidthAndStackAtCompactWidth() {
        let inspectorIndex = InspectorFindIndex(
            text: "alpha\nbeta alpha",
            query: "alpha",
            matchCase: false
        )
        let terminalIndex = TerminalTranscriptSearchIndex(
            text: "alpha\nbeta alpha",
            query: "alpha",
            matchCase: false
        )

        let inspectorCompact = fittingSize(
            inspectorFindBar(index: inspectorIndex),
            width: RuneFindBarMetrics.supportedInspectorWidth
        )
        let inspectorNormal = fittingSize(
            inspectorFindBar(index: inspectorIndex),
            width: 464
        )
        let inspectorWide = fittingSize(
            inspectorFindBar(index: inspectorIndex),
            width: 760
        )
        let terminalCompact = fittingSize(
            terminalFindBar(index: terminalIndex),
            width: RuneFindBarMetrics.supportedInspectorWidth
        )
        let terminalNormal = fittingSize(
            terminalFindBar(index: terminalIndex),
            width: 464
        )
        let terminalWide = fittingSize(
            terminalFindBar(index: terminalIndex),
            width: 760
        )

        XCTAssertEqual(RuneFindBarMetrics.primaryMinimumWidth, 190, accuracy: 0.5)
        XCTAssertEqual(inspectorNormal.height, inspectorWide.height, accuracy: 0.5)
        XCTAssertEqual(terminalNormal.height, terminalWide.height, accuracy: 0.5)
        XCTAssertGreaterThan(inspectorCompact.height, inspectorNormal.height)
        XCTAssertGreaterThan(terminalCompact.height, terminalNormal.height)
    }

    func testFindBarStacksAtAccessibilitySizeEvenWhenNormalWidthFits() {
        let index = InspectorFindIndex(
            text: "alpha\nbeta alpha",
            query: "alpha",
            matchCase: false
        )
        let regular = fittingSize(
            inspectorFindBar(index: index),
            width: 464,
            dynamicTypeSize: .large
        )
        let accessibility = fittingSize(
            inspectorFindBar(index: index),
            width: 464,
            dynamicTypeSize: .accessibility3
        )

        XCTAssertGreaterThan(accessibility.height, regular.height)
        XCTAssertGreaterThanOrEqual(
            accessibility.height,
            RuneAdaptiveToolbarMetrics.accessibilityMinimumRowHeight * 2
                + RuneFindBarMetrics.rowSpacing
                + RuneUILayoutMetrics.inspectorControlChromeVerticalPadding * 2
        )
    }

    func testResponsiveFindBarResizePreservesFocusedFieldAndBothCarets() async throws {
        let model = SyntheticFindBarResizeModel()
        let host = NSHostingController(
            rootView: SyntheticFindBarResizeHarness(model: model)
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
        let searchField = try XCTUnwrap(findSearchField(in: host.view))
        let documentEditor = try XCTUnwrap(findManifestTextView(in: host.view))
        XCTAssertTrue(window.makeFirstResponder(searchField))
        searchField.selectText(nil)

        let fieldEditor = try XCTUnwrap(searchField.currentEditor() as? NSTextView)
        let searchCaret = NSRange(location: 2, length: 0)
        let documentCaret = NSRange(location: 5, length: 0)
        fieldEditor.setSelectedRange(searchCaret)
        documentEditor.setSelectedRange(documentCaret)

        for width in [CGFloat(464), RuneFindBarMetrics.supportedInspectorWidth, 464] {
            window.setContentSize(NSSize(width: width, height: 420))
            try await settle(window: window)

            let resizedField = try XCTUnwrap(findSearchField(in: host.view))
            XCTAssertTrue(resizedField === searchField, "Responsive layout must preserve the native search field.")
            XCTAssertTrue(
                resizedField.currentEditor() === fieldEditor,
                "Responsive layout must preserve the active field editor."
            )
            XCTAssertTrue(
                window.firstResponder === fieldEditor,
                "Responsive layout must preserve search focus."
            )
            XCTAssertEqual(
                fieldEditor.selectedRange(),
                searchCaret,
                "Responsive layout must preserve the search insertion caret."
            )
            XCTAssertEqual(
                documentEditor.selectedRange(),
                documentCaret,
                "Responsive layout must not move the document caret."
            )
        }
    }

    private func inspectorFindBar(index: InspectorFindIndex) -> some View {
        InspectorFindBar(
            placeholder: "Find in YAML",
            searchIndex: index,
            query: .constant("alpha"),
            matchCase: .constant(false),
            selectedMatchIndex: .constant(0),
            isPresented: .constant(true)
        )
    }

    private func terminalFindBar(index: TerminalTranscriptSearchIndex) -> some View {
        TerminalTranscriptSearchBar(
            searchIndex: index,
            resolvedSearchMatchIndex: 0,
            query: .constant("alpha"),
            matchCase: .constant(false),
            onPrevious: {},
            onNext: {},
            onClose: {}
        )
    }

    private func fittingSize<Content: View>(
        _ content: Content,
        width: CGFloat,
        dynamicTypeSize: DynamicTypeSize = .large
    ) -> CGSize {
        let host = NSHostingView(
            rootView: content
                .dynamicTypeSize(dynamicTypeSize)
                .frame(width: width)
        )
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    private func findSearchField(in root: NSView) -> NSTextField? {
        if let field = root as? NSTextField,
           field.isEditable,
           field.placeholderString == "Find in YAML" {
            return field
        }
        for child in root.subviews {
            if let field = findSearchField(in: child) {
                return field
            }
        }
        return nil
    }

    private func findManifestTextView(in root: NSView) -> NSTextView? {
        if let textView = root as? NSTextView,
           textView.string.contains("apiVersion: v1") {
            return textView
        }
        for child in root.subviews {
            if let textView = findManifestTextView(in: child) {
                return textView
            }
        }
        return nil
    }

    private func settle(window: NSWindow) async throws {
        for _ in 0..<6 {
            window.contentView?.layoutSubtreeIfNeeded()
            await Task.yield()
            try await Task.sleep(for: .milliseconds(10))
        }
    }
}

@MainActor
private final class SyntheticFindBarResizeModel: ObservableObject {
    @Published var text = """
    apiVersion: v1
    kind: Pod
    metadata:
      name: first
      labels:
        name: second
    """
    @Published var query = "name"
    @Published var matchCase = false
    @Published var selectedMatchIndex = 0
    @Published var isFindPresented = true
}

private struct SyntheticFindBarResizeHarness: View {
    @ObservedObject var model: SyntheticFindBarResizeModel

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
