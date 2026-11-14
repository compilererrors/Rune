import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

/// State changes happen in place: selecting rows, entering quick edit or
/// switching a title must not resize or move neighbouring controls.
@MainActor
final class RuneLayoutStabilityTests: XCTestCase {
    func testSelectionControlKeepsOneSizeInEveryState() {
        let states: [(selected: Int, allVisible: Bool)] = [(0, false), (1, false), (3, false), (14, true)]
        let sizes = states.map { state in
            fittingSize(of: RuneBulkSelectionBar(
                selectedCount: state.selected,
                visibleCount: 14,
                allVisibleSelected: state.allVisible,
                onToggleVisibleSelection: {},
                onClearSelection: {}
            ) {
                RuneToolbarMenu {
                    Button("Full Logs ZIP") {}
                } label: {
                    Label("Export", systemImage: "square.and.arrow.up")
                }
                .disabled(state.selected == 0)
            })
        }
        for size in sizes.dropFirst() {
            XCTAssertEqual(size.width, sizes[0].width, accuracy: 0.5, "the selection control must not change width")
            XCTAssertEqual(size.height, sizes[0].height, accuracy: 0.5)
        }
    }

    func testFilterAndSortStayPutWhenRowsAreSelected() throws {
        let frames = try [0, 3].map { selected in
            let probe = FrameProbe()
            host(ToolbarHarness(selected: selected, probe: probe), width: 760)
            return probe.frames
        }
        for id in ["filter", "sort"] {
            let before = try XCTUnwrap(frames[0][id], "\(id) measured")
            let after = try XCTUnwrap(frames[1][id], "\(id) measured")
            XCTAssertEqual(before.minX, after.minX, accuracy: 0.5, "\(id) must not move when rows are selected")
            XCTAssertEqual(before.width, after.width, accuracy: 0.5, "\(id) must not resize when rows are selected")
        }
        let filter = try XCTUnwrap(frames[0]["filter"])
        let sort = try XCTUnwrap(frames[0]["sort"])
        XCTAssertEqual(sort.minX - filter.maxX, RuneUILayoutMetrics.contentControlSpacing, accuracy: 0.5,
                       "the filter fills up to the actions with the standard control gap")
    }

    func testQuickEditDoesNotReflowTheYAMLPane() throws {
        let editorFrames = try [(false, false), (true, false), (true, true)].map { isEditing, hasEdits in
            let view = YAMLPaneHarness(isEditing: isEditing, hasEdits: hasEdits)
            // Inspector width, where the mode note competes with the status title.
            let hostView = host(view, width: 400, height: 640)
            let editor = try XCTUnwrap(firstScrollView(in: hostView), "YAML editor surface")
            return editor.convert(editor.bounds, to: hostView)
        }
        for frame in editorFrames.dropFirst() {
            XCTAssertEqual(frame.minY, editorFrames[0].minY, accuracy: 0.5, "entering quick edit must not push the editor")
            XCTAssertEqual(frame.height, editorFrames[0].height, accuracy: 0.5)
        }
    }

    func testStableLabelKeepsTheWidestTitlesSize() {
        let titles = ["Quick Edit", "Done"]
        let sizes = titles.map { fittingSize(of: RuneStableLabel($0, reserving: titles)) }
        XCTAssertEqual(sizes[0].width, sizes[1].width, accuracy: 0.5)
    }

    private func fittingSize<V: View>(of view: V) -> CGSize {
        let hostView = NSHostingView(rootView: view.runeAppearanceTheme(RuneAppearanceTheme.graphiteBlue.resolvedTheme))
        hostView.layoutSubtreeIfNeeded()
        return hostView.fittingSize
    }

    @discardableResult
    private func host<V: View>(_ view: V, width: CGFloat, height: CGFloat = 120) -> NSView {
        let hostView = NSHostingView(rootView: view
            .frame(width: width, height: height, alignment: .topLeading)
            .runeAppearanceTheme(RuneAppearanceTheme.graphiteBlue.resolvedTheme))
        hostView.frame = NSRect(x: 0, y: 0, width: width, height: height)
        let window = NSWindow(contentRect: hostView.frame, styleMask: [.borderless], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = hostView
        for _ in 0..<5 {
            hostView.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.03))
        }
        return hostView
    }

    private func firstScrollView(in view: NSView) -> NSScrollView? {
        if let scrollView = view as? NSScrollView, scrollView.documentView is NSTextView { return scrollView }
        for subview in view.subviews {
            if let found = firstScrollView(in: subview) { return found }
        }
        return nil
    }
}

@MainActor
private final class FrameProbe {
    var frames: [String: CGRect] = [:]
}

private struct ToolbarHarness: View {
    let selected: Int
    let probe: FrameProbe
    @State private var query = ""

    var body: some View {
        RuneResourceListToolbar {
            TextField("Filter Pods", text: $query)
                .textFieldStyle(RuneControlTextFieldStyle())
                .frame(maxWidth: .infinity)
                .probe("filter", into: probe)
        } actions: {
            RuneToolbarOverflowRow(spacing: RuneUILayoutMetrics.contentControlSpacing) {
                RuneResourceSortMenu(
                    options: [("name", "Name"), ("age", "Age")],
                    column: "name",
                    ascending: true,
                    onSelectColumn: { _ in },
                    onReverseDirection: {}
                )
                .probe("sort", into: probe)
                RuneBulkSelectionBar(
                    selectedCount: selected,
                    visibleCount: 14,
                    allVisibleSelected: false,
                    onToggleVisibleSelection: {},
                    onClearSelection: {}
                ) {
                    RuneToolbarMenu {
                        Button("Full Logs ZIP") {}
                    } label: {
                        Label("Export", systemImage: "square.and.arrow.up")
                    }
                    .disabled(selected == 0)
                }
            }
        }
        .coordinateSpace(name: "toolbar")
    }
}

private struct YAMLPaneHarness: View {
    let isEditing: Bool
    let hasEdits: Bool
    @State private var text = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: sample\n"

    var body: some View {
        ResourceYAMLInspectorPane(
            resourceReference: "pod/sample",
            documentIdentity: nil,
            yamlText: $text,
            yamlDisplayText: text,
            yamlFooterText: "",
            baseline: text,
            hasUnsavedEdits: hasEdits,
            canApplyMutations: true,
            validationIssues: [],
            isValidating: false,
            statusText: "Last updated 12:00",
            canUndoEdit: hasEdits,
            canReapplySnapshot: false,
            isInlineEditing: .constant(isEditing),
            inlineEditorImplementation: .appKitTextView,
            onApply: {},
            onReapplySnapshot: {},
            onOpenEditor: {},
            onUndoEdit: {},
            onRevert: {},
            onImport: {},
            onExport: {},
            onExportToExportFolder: {},
            onExportAndOpen: {},
            readOnlyResetID: "sample"
        )
    }
}

private extension View {
    func probe(_ id: String, into probe: FrameProbe) -> some View {
        background(GeometryReader { proxy in
            Color.clear
                .onAppear { probe.frames[id] = proxy.frame(in: .named("toolbar")) }
                .onChange(of: proxy.frame(in: .named("toolbar"))) { _, frame in probe.frames[id] = frame }
        })
    }
}
