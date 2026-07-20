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
        for dynamicTypeSize in [DynamicTypeSize.large, .accessibility3] {
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
        }
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
        XCTAssertTrue(controlsSource.contains("ViewThatFits(in: .horizontal)"))
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
        PortForwardSession(
            id: "synthetic-port-forward",
            contextName: "synthetic-context",
            namespace: selectedPod.namespace,
            targetKind: .pod,
            targetName: selectedPod.name,
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

    private func panel(dynamicTypeSize: DynamicTypeSize) -> some View {
        TerminalPortForwardPanelView(
            isExpanded: .constant(true),
            contextName: "synthetic-context",
            selectedPod: selectedPod,
            availablePods: [selectedPod],
            portForwardSessions: [activeSession],
            canApplyMutations: true,
            selectedPortForwardPodID: .constant(selectedPod.id),
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
