import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RuneContentStateViewTests: XCTestCase {
    func testStatesExposeDistinctAccessibleSemantics() {
        XCTAssertEqual(
            RuneContentState.loading(title: "Loading pods").accessibilitySummary,
            "Loading. Loading pods"
        )
        XCTAssertEqual(
            RuneContentState.retryableError(title: "Could not load", message: "Request failed").accessibilitySummary,
            "Error. Could not load. Request failed"
        )
        XCTAssertEqual(
            RuneContentState.filteredEmpty(title: "No matches", message: "Clear the filter").accessibilitySummary,
            "No filtered results. No matches. Clear the filter"
        )
        XCTAssertEqual(
            RuneContentState.empty(title: "No events", message: "Events appear here").accessibilitySummary,
            "Empty. No events. Events appear here"
        )
        XCTAssertEqual(
            RuneContentState.unselected(title: "Select a pod", message: "Choose from the list").accessibilitySummary,
            "Nothing selected. Select a pod. Choose from the list"
        )
    }

    func testExplicitActionRunsOnlyWhenInvoked() {
        var invocationCount = 0
        let action = RuneContentStateAction("Retry", systemImage: "arrow.clockwise") {
            invocationCount += 1
        }

        XCTAssertEqual(invocationCount, 0)
        action.perform()
        XCTAssertEqual(invocationCount, 1)
    }

    func testVariantsHavePurposefullyDifferentMinimumGeometry() {
        XCTAssertGreaterThan(
            RuneContentStateVariant.centered.minimumHeight,
            RuneContentStateVariant.card.minimumHeight
        )
        XCTAssertEqual(
            RuneContentStateVariant.card.minimumHeight,
            RuneContentStateVariant.pane.minimumHeight
        )
        XCTAssertGreaterThan(
            RuneContentStateVariant.card.minimumHeight,
            RuneContentStateVariant.inline.minimumHeight
        )
        XCTAssertGreaterThanOrEqual(RuneContentStateVariant.inline.minimumHeight, 36)
    }

    @MainActor
    func testCenteredCardPaneAndInlineViewsRespectMinimumHeights() {
        let centered = fittingSize(for: .centered)
        let card = fittingSize(for: .card)
        let pane = fittingSize(for: .pane)
        let inline = fittingSize(for: .inline)

        XCTAssertEqual(centered.width, 320, accuracy: 0.5)
        XCTAssertEqual(card.width, 320, accuracy: 0.5)
        XCTAssertEqual(pane.width, 320, accuracy: 0.5)
        XCTAssertEqual(inline.width, 320, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(centered.height, RuneContentStateVariant.centered.minimumHeight)
        XCTAssertGreaterThanOrEqual(card.height, RuneContentStateVariant.card.minimumHeight)
        XCTAssertGreaterThanOrEqual(pane.height, RuneContentStateVariant.pane.minimumHeight)
        XCTAssertGreaterThanOrEqual(inline.height, RuneContentStateVariant.inline.minimumHeight)
        XCTAssertLessThan(inline.height, card.height)
    }

    @MainActor
    func testPaneLayoutKeepsTransientContentAtTopAcrossWindowHeights() throws {
        for size in [CGSize(width: 360, height: 420), CGSize(width: 640, height: 820)] {
            let host = NSHostingView(rootView: RunePaneTopLayout {
                PaneTopProbe()
                    .frame(height: RuneContentStateVariant.card.minimumHeight)
            }
            .frame(width: size.width, height: size.height))
            host.frame = NSRect(origin: .zero, size: size)
            settle(host)

            let probe = try XCTUnwrap(findPaneTopProbe(in: host))
            let frame = probe.convert(probe.bounds, to: host)
            let topGap = host.isFlipped
                ? frame.minY - host.bounds.minY
                : host.bounds.maxY - frame.maxY
            XCTAssertEqual(topGap, 0, accuracy: 1)
            XCTAssertEqual(frame.width, size.width, accuracy: 1)
            XCTAssertEqual(
                frame.height,
                RuneContentStateVariant.card.minimumHeight,
                accuracy: 1
            )
        }
    }

    func testStandaloneAdoptionsUseSemanticStatesAndExplicitRetry() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let logs = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/ResourceLogsInspectorView.swift"),
            encoding: .utf8
        )
        let palette = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/CommandPaletteView.swift"),
            encoding: .utf8
        )
        let events = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/OverviewRecentEventsPanelView.swift"),
            encoding: .utf8
        )
        let rollout = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/DeploymentRolloutHistoryView.swift"),
            encoding: .utf8
        )

        XCTAssertTrue(logs.contains(".loading("))
        XCTAssertTrue(logs.contains(".retryableError(title: \"Could not load logs\""))
        XCTAssertTrue(logs.contains("perform: onReload"))
        XCTAssertTrue(logs.contains(".empty(title: title, message: message)"))
        XCTAssertTrue(palette.contains(".filteredEmpty("))
        XCTAssertTrue(palette.contains("RuneContentStateAction(\"Clear Search\""))
        XCTAssertTrue(events.contains("variant: .inline"))
        XCTAssertTrue(rollout.contains("variant: .card"))
    }

    @MainActor
    private func fittingSize(for variant: RuneContentStateVariant) -> CGSize {
        let host = NSHostingView(rootView: RuneContentStateView(
            .unselected(
                title: "Select a synthetic resource",
                message: "Choose a resource to inspect its details."
            ),
            variant: variant
        ).frame(width: 320))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    @MainActor
    private func settle(_ view: NSView) {
        for _ in 0..<3 {
            view.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.03))
        }
    }

    @MainActor
    private func findPaneTopProbe(in view: NSView) -> PaneTopProbeView? {
        if let probe = view as? PaneTopProbeView {
            return probe
        }
        for subview in view.subviews {
            if let probe = findPaneTopProbe(in: subview) {
                return probe
            }
        }
        return nil
    }
}

private struct PaneTopProbe: NSViewRepresentable {
    func makeNSView(context: Context) -> PaneTopProbeView {
        PaneTopProbeView()
    }

    func updateNSView(_ nsView: PaneTopProbeView, context: Context) {}
}

private final class PaneTopProbeView: NSView {}
