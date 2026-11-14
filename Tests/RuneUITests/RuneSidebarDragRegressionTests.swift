import AppKit
import RuneCore
@testable import RuneUI
import SwiftUI
import XCTest

@MainActor
final class RuneSidebarDragRegressionTests: XCTestCase {
    func testSplitDividersFollowDragWhileRootRerenders() async throws {
        let defaults = UserDefaults.standard
        let keys = [RuneSettingsKeys.layoutSidebarWidth, RuneSettingsKeys.layoutDetailWidth, RuneSettingsKeys.saveLastAppState]
        let originals = keys.map { defaults.object(forKey: $0) }
        defaults.set(280.0, forKey: keys[0])
        defaults.set(440.0, forKey: keys[1])
        defaults.set(true, forKey: keys[2])
        let originalMouseState = AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown
        defer {
            AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown = originalMouseState
            for (key, original) in zip(keys, originals) {
                if let original { defaults.set(original, forKey: key) } else { defaults.removeObject(forKey: key) }
            }
        }

        let state = RuneAppState()
        let contexts = (0..<6).map { KubeContext(name: "sample-context-\($0)") }
        state.setContexts(contexts)
        state.selectedContext = contexts[0]
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        let pods = (0..<20).map { PodSummary(name: "sample-pod-\($0)", namespace: "default", status: "Running") }
        state.setPods(pods)
        state.selectedPod = pods[0]
        let viewModel = RuneAppViewModel(state: state)
        viewModel.isSidebarVisible = true
        viewModel.isDetailPaneVisible = true

        let host = NSHostingController(rootView: RuneRootView(
            viewModel: viewModel,
            onLayoutSnapshotChange: nil,
            debugDisableBootstrap: true,
            debugDisableLayoutPersistence: false,
            shellVariant: .appKitSplitView
        ))
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        let container = NSView(frame: NSRect(x: 0, y: 0, width: 1440, height: 900))
        window.contentView = container
        let hostView = host.view
        hostView.translatesAutoresizingMaskIntoConstraints = false
        container.addSubview(hostView)
        NSLayoutConstraint.activate([
            hostView.leadingAnchor.constraint(equalTo: container.leadingAnchor),
            hostView.trailingAnchor.constraint(equalTo: container.trailingAnchor),
            hostView.topAnchor.constraint(equalTo: container.topAnchor),
            hostView.bottomAnchor.constraint(equalTo: container.bottomAnchor)
        ])
        window.makeKeyAndOrderFront(nil)
        defer {
            hostView.removeFromSuperview()
            window.orderOut(nil)
        }
        try await Task.sleep(nanoseconds: 2_000_000_000)
        container.layoutSubtreeIfNeeded()

        let split = try XCTUnwrap(threePaneSplitView(in: container))
        AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown = { true }
        var sidebarTargets: [CGFloat] = stride(from: 284, through: 420, by: 4).map { CGFloat($0) }
        sidebarTargets += stride(from: 416, through: 240, by: -4).map { CGFloat($0) }
        let sidebarSnaps = try await drag(split, divider: 0, through: sidebarTargets, viewModel: viewModel) {
            split.arrangedSubviews[0].frame.width
        }
        AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown = originalMouseState
        try await Task.sleep(nanoseconds: 600_000_000)

        XCTAssertTrue(sidebarSnaps.isEmpty, "Sidebar divider snapped back during drag: \(sidebarSnaps.prefix(10))")
        XCTAssertEqual(split.arrangedSubviews[0].frame.width, 240, accuracy: 1.5)
        XCTAssertEqual(defaults.double(forKey: RuneSettingsKeys.layoutSidebarWidth), 240, accuracy: 3)

        AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown = { true }
        var detailTargets: [CGFloat] = stride(from: 444, through: 600, by: 4).map { CGFloat($0) }
        detailTargets += stride(from: 596, through: 400, by: -4).map { CGFloat($0) }
        let detailPositions = detailTargets.map { split.bounds.width - split.dividerThickness - $0 }
        let detailSnaps = try await drag(split, divider: 1, through: detailPositions, viewModel: viewModel) {
            split.arrangedSubviews[2].frame.minX - split.dividerThickness
        }
        AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown = originalMouseState
        try await Task.sleep(nanoseconds: 600_000_000)

        XCTAssertTrue(detailSnaps.isEmpty, "Inspector divider snapped back during drag: \(detailSnaps.prefix(10))")
        XCTAssertEqual(split.arrangedSubviews[2].frame.width, 400, accuracy: 1.5)
        XCTAssertEqual(defaults.double(forKey: RuneSettingsKeys.layoutDetailWidth), 400, accuracy: 3)
    }

    func testInspectorTextKeepsLeadingEdgeWhileInspectorResizes() async throws {
        let defaults = UserDefaults.standard
        let keys = [RuneSettingsKeys.layoutSidebarWidth, RuneSettingsKeys.layoutDetailWidth, RuneSettingsKeys.saveLastAppState]
        let originals = keys.map { defaults.object(forKey: $0) }
        defaults.set(260.0, forKey: keys[0])
        defaults.set(520.0, forKey: keys[1])
        defaults.set(true, forKey: keys[2])
        let originalMouseState = AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown
        defer {
            AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown = originalMouseState
            for (key, original) in zip(keys, originals) {
                if let original { defaults.set(original, forKey: key) } else { defaults.removeObject(forKey: key) }
            }
        }

        let state = RuneAppState()
        let context = KubeContext(name: "sample-context")
        state.setContexts([context])
        state.selectedContext = context
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        let pod = PodSummary(name: "sample-pod-0", namespace: "default", status: "Running")
        state.setPods([pod])
        state.selectedPod = pod
        let longValue = String(repeating: "sample-value-", count: 24)
        state.setResourceYAML((["apiVersion: v1", "kind: Pod", "metadata:", "  name: sample-pod-0", "  annotations:"]
            + (0..<4).map { "    note-\($0): \(longValue)" }).joined(separator: "\n"))
        let viewModel = RuneAppViewModel(state: state)
        viewModel.isSidebarVisible = true
        viewModel.isDetailPaneVisible = true

        let host = NSHostingController(rootView: RuneRootView(
            viewModel: viewModel,
            onLayoutSnapshotChange: nil,
            debugDisableBootstrap: true,
            debugDisableLayoutPersistence: false,
            initialPodInspectorTab: .yaml,
            shellVariant: .appKitSplitView
        ))
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1440, height: 900),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        let container = NSView(frame: NSRect(x: 0, y: 0, width: 1440, height: 900))
        window.contentView = container
        let hostView = host.view
        hostView.translatesAutoresizingMaskIntoConstraints = false
        container.addSubview(hostView)
        NSLayoutConstraint.activate([
            hostView.leadingAnchor.constraint(equalTo: container.leadingAnchor),
            hostView.trailingAnchor.constraint(equalTo: container.trailingAnchor),
            hostView.topAnchor.constraint(equalTo: container.topAnchor),
            hostView.bottomAnchor.constraint(equalTo: container.bottomAnchor)
        ])
        window.makeKeyAndOrderFront(nil)
        defer {
            hostView.removeFromSuperview()
            window.orderOut(nil)
        }
        try await Task.sleep(nanoseconds: 2_000_000_000)
        container.layoutSubtreeIfNeeded()

        viewModel.state.setResourceYAML((["apiVersion: v1", "kind: Pod", "metadata:", "  name: sample-pod-0", "  annotations:"]
            + (0..<1_200).map { "    note-\($0): sample-value-\($0)" }).joined(separator: "\n"))
        try await Task.sleep(nanoseconds: 800_000_000)

        let split = try XCTUnwrap(threePaneSplitView(in: container))
        let textScrollView = try XCTUnwrap(manifestScrollView(in: split.arrangedSubviews[2]))
        let textView = try XCTUnwrap(textScrollView.documentView as? NSTextView)
        let gutter = textScrollView.subviews.first { $0 is YAMLLineNumberGutterOverlayView && !$0.isHidden }
        XCTAssertEqual(textScrollView.contentView.bounds.origin.x, 0, accuracy: 0.5)

        AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown = { true }
        var widths: [CGFloat] = stride(from: 516, through: 360, by: -4).map { CGFloat($0) }
        widths += stride(from: 364, through: 640, by: 4).map { CGFloat($0) }
        var shifts: [String] = []
        for width in widths {
            split.setPosition(split.bounds.width - split.dividerThickness - width, ofDividerAt: 1)
            try await Task.sleep(nanoseconds: 16_000_000)
            let originX = textScrollView.contentView.bounds.origin.x
            let pane = split.arrangedSubviews[2]
            let leftEdge = textScrollView.convert(textScrollView.bounds, to: pane).minX
            let gutterWidth = gutter?.frame.width ?? 0
            if abs(originX) > 0.5 || leftEdge < 0 || leftEdge > 40 || gutterWidth > textView.textContainerInset.width + 0.5 {
                shifts.append("\(Int(width)): origin \(Int(originX)) edge \(Int(leftEdge)) gutter \(Int(gutterWidth)) inset \(Int(textView.textContainerInset.width))")
            }
        }
        AppKitTripleSplitView.Controller.isPrimaryMouseButtonDown = originalMouseState
        try await Task.sleep(nanoseconds: 400_000_000)

        XCTAssertTrue(shifts.isEmpty, "Inspector text scrolled horizontally while resizing: \(shifts.prefix(10))")
        XCTAssertEqual(textScrollView.contentView.bounds.origin.x, 0, accuracy: 0.5)
    }

    private func manifestScrollView(in view: NSView) -> NSScrollView? {
        if let scrollView = view as? NSScrollView, scrollView.documentView is NSTextView { return scrollView }
        for subview in view.subviews {
            if let found = manifestScrollView(in: subview) { return found }
        }
        return nil
    }

    private func drag(
        _ split: NSSplitView,
        divider: Int,
        through positions: [CGFloat],
        viewModel: RuneAppViewModel,
        measure: () -> CGFloat
    ) async throws -> [String] {
        var snaps: [String] = []
        for (index, position) in positions.enumerated() {
            split.setPosition(position, ofDividerAt: divider)
            if index % 3 == 1 {
                viewModel.objectWillChange.send()
            }
            try await Task.sleep(nanoseconds: 16_000_000)
            let actual = measure()
            if abs(actual - position) > 1.5 {
                snaps.append("\(Int(position))->\(Int(actual))")
            }
        }
        return snaps
    }

    private func threePaneSplitView(in view: NSView) -> NSSplitView? {
        if let split = view as? NSSplitView, split.isVertical, split.arrangedSubviews.count == 3 { return split }
        for subview in view.subviews {
            if let found = threePaneSplitView(in: subview) { return found }
        }
        return nil
    }
}
