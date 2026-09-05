import AppKit
import RuneCore
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class PortForwardControlLayoutTests: XCTestCase {
    func testEndpointFieldsUseStackedRenderedGeometryAt320Points() throws {
        let compact = fittingSize(
            endpointFields(dynamicTypeSize: .large),
            width: PortForwardControlLayoutMetrics.supportedInspectorWidth
        )
        let wide = fittingSize(endpointFields(dynamicTypeSize: .large), width: 760)
        let enlarged = fittingSize(
            endpointFields(dynamicTypeSize: .accessibility3),
            width: PortForwardControlLayoutMetrics.supportedInspectorWidth
        )

        XCTAssertEqual(compact.width, PortForwardControlLayoutMetrics.supportedInspectorWidth, accuracy: 0.5)
        XCTAssertEqual(enlarged.width, PortForwardControlLayoutMetrics.supportedInspectorWidth, accuracy: 0.5)
        XCTAssertGreaterThan(compact.height, wide.height)
        XCTAssertGreaterThan(enlarged.height, compact.height)
        XCTAssertGreaterThanOrEqual(compact.height, PortForwardControlLayoutMetrics.compactMinimumHeight)
    }

    func testExpandedPanelRendersInside320PointsWithoutHorizontalScroll() throws {
        for (index, dynamicTypeSize) in [DynamicTypeSize.large, .accessibility3].enumerated() {
            let host = renderHost(
                panel(dynamicTypeSize: dynamicTypeSize),
                width: PortForwardControlLayoutMetrics.supportedInspectorWidth
            )

            XCTAssertEqual(host.frame.width, PortForwardControlLayoutMetrics.supportedInspectorWidth, accuracy: 0.5)
            XCTAssertGreaterThan(host.frame.height, 300)
            XCTAssertFalse(
                scrollViews(in: host).contains(where: \.hasHorizontalScroller),
                "The compact Port Forward panel must not hide endpoint or primary controls in a horizontal scroller."
            )

            let png = try renderedPNG(from: host)
            XCTAssertGreaterThan(png.count, 4_000)
            if let artifactDirectory = ProcessInfo.processInfo.environment["RUNE_UI_TEST_ARTIFACT_DIR"] {
                let directory = URL(fileURLWithPath: artifactDirectory, isDirectory: true)
                try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
                try png.write(to: directory.appendingPathComponent("terminal-port-forward-\(index).png"))
            }
        }
    }

    func testCollapsedPanelTruncatesLongMetadataBeforeStackingActionRails() throws {
        let longPod = PodSummary(
            name: "synthetic-api-component-with-a-deliberately-long-pod-name-for-truncation-0",
            namespace: "synthetic-namespace-with-a-deliberately-long-name",
            status: "Running"
        )
        let normalWidth: CGFloat = 560
        let short = fittingSize(
            panel(dynamicTypeSize: .large, isExpanded: false, pod: selectedPod),
            width: normalWidth
        )
        let long = fittingSize(
            panel(dynamicTypeSize: .large, isExpanded: false, pod: longPod),
            width: normalWidth
        )
        let compact = fittingSize(
            panel(dynamicTypeSize: .large, isExpanded: false, pod: longPod),
            width: PortForwardControlLayoutMetrics.supportedInspectorWidth
        )

        XCTAssertEqual(short.height, long.height, accuracy: 0.5)
        XCTAssertGreaterThan(compact.height, long.height)

        let host = renderHost(
            panel(dynamicTypeSize: .large, isExpanded: false, pod: longPod),
            width: normalWidth
        )
        XCTAssertFalse(scrollViews(in: host).contains(where: \.hasHorizontalScroller))
        XCTAssertGreaterThan(try renderedPNG(from: host).count, 3_000)
    }

    func testCollapsedPanelKeepsAccessibilityActionRailsStacked() {
        let regular = fittingSize(
            panel(dynamicTypeSize: .large, isExpanded: false, pod: selectedPod),
            width: 560
        )
        let accessibility = fittingSize(
            panel(dynamicTypeSize: .accessibility3, isExpanded: false, pod: selectedPod),
            width: 560
        )

        XCTAssertGreaterThan(accessibility.height, regular.height)
        XCTAssertGreaterThanOrEqual(
            accessibility.height - regular.height,
            RuneAdaptiveToolbarMetrics.rowSpacing
        )
    }

    func testEndpointFieldsKeepActiveEditorAcrossResponsiveLayoutChange() async throws {
        let model = SyntheticPortForwardFieldsModel()
        let host = NSHostingController(
            rootView: SyntheticPortForwardFieldsHarness(model: model)
                .frame(width: 760, height: 180, alignment: .topLeading)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 760, height: 180),
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
            editableTextFields(in: host.view).first { $0.placeholderString == "Local" }
        )
        window.makeFirstResponder(originalField)
        try await settle(window: window)
        let originalEditor = try XCTUnwrap(originalField.currentEditor() as? NSTextView)
        originalEditor.setSelectedRange(NSRange(location: 2, length: 0))

        model.width = PortForwardControlLayoutMetrics.supportedInspectorWidth
        try await settle(window: window)

        let compactField = try XCTUnwrap(
            editableTextFields(in: host.view).first { $0.placeholderString == "Local" }
        )
        XCTAssertTrue(compactField === originalField)
        XCTAssertTrue(compactField.currentEditor() === originalEditor)
        XCTAssertTrue(window.firstResponder === originalEditor)
        XCTAssertEqual(originalEditor.selectedRange(), NSRange(location: 2, length: 0))
    }

    func testPrimaryActionDispatchesExactlyOneStartOrStopOperation() {
        var startCount = 0
        var stoppedSessionIDs: [String] = []
        let active = activeSession

        let start = PortForwardPrimaryActionButton(
            activeSession: nil,
            startTitle: "Start Port Forward",
            isStartDisabled: false,
            onStart: { startCount += 1 },
            onStop: { stoppedSessionIDs.append($0.id) }
        )
        start.performPrimaryAction()

        let stop = PortForwardPrimaryActionButton(
            activeSession: active,
            startTitle: "Start Port Forward",
            isStartDisabled: false,
            onStart: { startCount += 1 },
            onStop: { stoppedSessionIDs.append($0.id) }
        )
        stop.performPrimaryAction()

        let disabledStart = PortForwardPrimaryActionButton(
            activeSession: nil,
            startTitle: "Start Port Forward",
            isStartDisabled: true,
            onStart: { startCount += 1 },
            onStop: { stoppedSessionIDs.append($0.id) }
        )
        disabledStart.performPrimaryAction()

        XCTAssertEqual(startCount, 1)
        XCTAssertEqual(stoppedSessionIDs, [active.id])
        XCTAssertEqual(start.primaryActionTitle, "Start Port Forward")
        XCTAssertEqual(stop.primaryActionTitle, "Stop")
    }

    func testTerminalAndInspectorKeepStartStopInOnePrimaryLocation() throws {
        let terminalSource = try source("Sources/RuneUI/Views/TerminalPortForwardPanelView.swift")
        let rootSource = try source("Sources/RuneUI/Views/RuneRootView.swift")
        let controlsSource = try source("Sources/RuneUI/Views/PortForwardControlComponents.swift")
        let rootPane = try XCTUnwrap(rootSource.slice(
            from: "private func portForwardPane(targetKind: PortForwardTargetKind, targetName: String) -> some View",
            to: "private var terminalPane: some View"
        ))
        let rootSessionRow = try XCTUnwrap(rootSource.slice(
            from: "private func portForwardSessionRow(_ session: PortForwardSession) -> some View",
            to: "private func genericResourceList("
        ))

        XCTAssertEqual(terminalSource.occurrences(of: "PortForwardPrimaryActionButton("), 1)
        XCTAssertFalse(terminalSource.contains("stableHorizontalControls"))
        XCTAssertFalse(terminalSource.contains("ScrollView(.horizontal"))
        XCTAssertFalse(terminalSource.contains("stopButton(session)"))
        XCTAssertEqual(rootPane.occurrences(of: "PortForwardPrimaryActionButton("), 1)
        XCTAssertTrue(rootPane.contains("PortForwardEndpointFields("))
        XCTAssertFalse(rootSessionRow.contains("viewModel.stopPortForward(session)"))
        XCTAssertTrue(controlsSource.contains("PortForwardEndpointLayout"))
        XCTAssertFalse(controlsSource.contains("ScrollView(.horizontal"))
    }

    func testCompactPortForwardLayoutBenchmarkKPI() {
        let iterations = 24
        let start = ContinuousClock.now

        for _ in 0..<iterations {
            _ = fittingSize(
                endpointFields(dynamicTypeSize: .large),
                width: PortForwardControlLayoutMetrics.supportedInspectorWidth
            )
            _ = fittingSize(
                endpointFields(dynamicTypeSize: .accessibility3),
                width: PortForwardControlLayoutMetrics.supportedInspectorWidth
            )
        }

        let elapsed = ContinuousClock.now - start
        let elapsedSeconds = elapsed.seconds
        print(
            "KPI Port Forward compact layout: \(iterations * 2) rendered 320pt endpoint layouts in "
                + String(format: "%.3f", elapsedSeconds) + "s (target < 1.000s debug)."
        )
        XCTAssertLessThan(elapsedSeconds, 1.0)
    }

    private var selectedPod: PodSummary {
        PodSummary(
            name: "synthetic-api-0",
            namespace: "sample",
            status: "Running"
        )
    }

    private var activeSession: PortForwardSession {
        activeSession(for: selectedPod)
    }

    private func activeSession(for pod: PodSummary) -> PortForwardSession {
        PortForwardSession(
            id: "synthetic-port-forward",
            contextName: "synthetic-context",
            namespace: pod.namespace,
            targetKind: .pod,
            targetName: pod.name,
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1",
            status: .active
        )
    }

    private func endpointFields(dynamicTypeSize: DynamicTypeSize) -> some View {
        PortForwardEndpointFields(
            localPort: .constant("8080"),
            remotePort: .constant("80"),
            address: .constant("127.0.0.1")
        )
        .dynamicTypeSize(dynamicTypeSize)
    }

    private func panel(
        dynamicTypeSize: DynamicTypeSize,
        isExpanded: Bool = true,
        pod: PodSummary? = nil
    ) -> some View {
        let pod = pod ?? selectedPod
        return TerminalPortForwardPanelView(
            isExpanded: .constant(isExpanded),
            contextName: "synthetic-context",
            selectedPod: pod,
            availablePods: [pod],
            portForwardSessions: [activeSession(for: pod)],
            canApplyMutations: true,
            selectedPortForwardPodID: .constant(pod.id),
            localPort: .constant("8080"),
            remotePort: .constant("80"),
            address: .constant("127.0.0.1"),
            onStartPortForward: { _ in },
            onStopPortForward: { _ in },
            onOpenPortForwardInBrowser: { _ in },
            onRetryPortForward: { _ in },
            onClearPortForward: { _ in },
            onClearInactivePortForwards: {}
        )
        .dynamicTypeSize(dynamicTypeSize)
    }

    private func fittingSize<Content: View>(_ content: Content, width: CGFloat) -> CGSize {
        let host = NSHostingView(rootView: content.frame(width: width))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    private func renderHost<Content: View>(_ content: Content, width: CGFloat) -> NSView {
        let host = NSHostingView(rootView: content.frame(width: width))
        host.layoutSubtreeIfNeeded()
        host.frame = NSRect(origin: .zero, size: host.fittingSize)
        host.layoutSubtreeIfNeeded()
        return host
    }

    private func scrollViews(in root: NSView) -> [NSScrollView] {
        var result: [NSScrollView] = []
        if let scrollView = root as? NSScrollView {
            result.append(scrollView)
        }
        for child in root.subviews {
            result.append(contentsOf: scrollViews(in: child))
        }
        return result
    }

    private func renderedPNG(from view: NSView) throws -> Data {
        guard let bitmap = view.bitmapImageRepForCachingDisplay(in: view.bounds) else {
            throw CocoaError(.coderInvalidValue)
        }
        view.cacheDisplay(in: view.bounds, to: bitmap)
        return try XCTUnwrap(bitmap.representation(using: .png, properties: [:]))
    }

    private func source(_ relativePath: String) throws -> String {
        try String(
            contentsOf: repoRoot.appendingPathComponent(relativePath),
            encoding: .utf8
        )
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
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
private final class SyntheticPortForwardFieldsModel: ObservableObject {
    @Published var width: CGFloat = 760
    @Published var localPort = "8080"
    @Published var remotePort = "80"
    @Published var address = "127.0.0.1"
}

private struct SyntheticPortForwardFieldsHarness: View {
    @ObservedObject var model: SyntheticPortForwardFieldsModel

    var body: some View {
        PortForwardEndpointFields(
            localPort: $model.localPort,
            remotePort: $model.remotePort,
            address: $model.address
        )
        .frame(width: model.width)
    }
}

private extension Duration {
    var seconds: Double {
        let components = self.components
        return Double(components.seconds) + Double(components.attoseconds) / 1e18
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

    func occurrences(of value: String) -> Int {
        components(separatedBy: value).count - 1
    }
}
