import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class LargeLogSearchVisibilityRuntimeTests: XCTestCase {
    @MainActor
    func testDeepSearchNavigationRendersSelectedMatchInsideVisibleLogViewport() async throws {
        let lineCount = 1_400
        let targetLineIndex = 1_200
        let payload = String(repeating: " synthetic-payload", count: 14)
        let text = (0..<lineCount)
            .map { index in
                let marker: String
                switch index {
                case 20:
                    marker = " marker needle FIRSTVISIBLE"
                case targetLineIndex:
                    marker = " marker needle DEEPVISIBLE"
                default:
                    marker = ""
                }
                return "line-\(index)\(marker)\(payload)"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.makeForInspector(text: text, query: "needle")
        let model = LargeLogSearchVisibilityModel(text: text, result: result)
        let host = NSHostingController(
            rootView: LargeLogSearchVisibilityHarness(model: model)
                .frame(width: 760, height: 420)
                .environment(\.colorScheme, .dark)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 760, height: 420),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer {
            window.orderOut(nil)
            window.contentViewController = nil
            window.contentView = nil
        }

        try await settle(window: window)
        let initialScrollView = try XCTUnwrap(
            scrollViews(in: host.view).max {
                ($0.documentView?.bounds.height ?? 0) < ($1.documentView?.bounds.height ?? 0)
            }
        )
        let initialDocumentHeight = try XCTUnwrap(initialScrollView.documentView?.bounds.height)
        let estimatedRowHeight = initialDocumentHeight / CGFloat(lineCount)
        let initialSignal = try activeRowRenderSignal(
            in: initialScrollView,
            estimatedRowHeight: estimatedRowHeight
        )
        XCTAssertGreaterThan(initialSignal.accentPixelCount, 500)
        XCTAssertGreaterThan(initialSignal.brightPixelCount, 100)
        XCTAssertGreaterThan(initialSignal.luminanceRange, 0.35)

        model.selectedMatchIndex = 1
        model.navigationSequence &+= 1
        try await settle(window: window, iterations: 20)

        let outputScrollView = try XCTUnwrap(
            scrollViews(in: host.view).max {
                ($0.documentView?.bounds.height ?? 0) < ($1.documentView?.bounds.height ?? 0)
            }
        )
        let targetMidY = CGFloat(targetLineIndex) * estimatedRowHeight + estimatedRowHeight / 2
        XCTAssertLessThan(
            abs(targetMidY - outputScrollView.contentView.bounds.midY),
            outputScrollView.contentView.bounds.height * 0.22,
            "The selected deep search match must be centered in the native viewport."
        )

        let deepSignal = try activeRowRenderSignal(
            in: outputScrollView,
            estimatedRowHeight: estimatedRowHeight
        )
        XCTAssertGreaterThan(
            deepSignal.accentPixelCount,
            500,
            "The exact centered match row must draw its active highlight. signal=\(deepSignal)"
        )
        XCTAssertGreaterThan(
            deepSignal.brightPixelCount,
            max(100, initialSignal.brightPixelCount / 3),
            "The centered deep viewport must contain rendered log glyphs, not a uniform blank surface. signal=\(deepSignal)"
        )
        XCTAssertGreaterThan(
            deepSignal.luminanceRange,
            0.35,
            "The centered deep viewport must retain foreground/background contrast. signal=\(deepSignal)"
        )
    }

    @MainActor
    private func settle(window: NSWindow, iterations: Int = 8) async throws {
        for _ in 0..<iterations {
            window.contentView?.layoutSubtreeIfNeeded()
            try await Task.sleep(nanoseconds: 25_000_000)
        }
    }

    @MainActor
    private func scrollViews(in view: NSView) -> [NSScrollView] {
        var matches: [NSScrollView] = []
        if let scrollView = view as? NSScrollView {
            matches.append(scrollView)
        }
        for subview in view.subviews {
            matches.append(contentsOf: scrollViews(in: subview))
        }
        return matches
    }

    @MainActor
    private func activeRowRenderSignal(
        in view: NSView,
        estimatedRowHeight: CGFloat
    ) throws -> LargeLogCentralRenderSignal {
        view.layoutSubtreeIfNeeded()
        let bitmap = try XCTUnwrap(view.bitmapImageRepForCachingDisplay(in: view.bounds))
        view.cacheDisplay(in: view.bounds, to: bitmap)

        let xRange = max(0, bitmap.pixelsWide / 20)..<max(1, bitmap.pixelsWide * 3 / 4)
        let centerY = bitmap.pixelsHigh / 2
        let pixelsPerPoint = CGFloat(bitmap.pixelsHigh) / max(1, view.bounds.height)
        let rowPixelHeight = max(2, Int(ceil(estimatedRowHeight * pixelsPerPoint)))
        let candidateYRanges = [
            max(0, centerY - rowPixelHeight)..<centerY,
            centerY..<min(bitmap.pixelsHigh, centerY + rowPixelHeight),
        ]
        return try XCTUnwrap(
            candidateYRanges
                .map { renderSignal(in: bitmap, xRange: xRange, yRange: $0) }
                .max { $0.accentPixelCount < $1.accentPixelCount }
        )
    }

    private func renderSignal(
        in bitmap: NSBitmapImageRep,
        xRange: Range<Int>,
        yRange: Range<Int>
    ) -> LargeLogCentralRenderSignal {
        var accentPixelCount = 0
        var brightPixelCount = 0
        var minimumLuminance = CGFloat.greatestFiniteMagnitude
        var maximumLuminance: CGFloat = 0

        for y in yRange {
            for x in xRange {
                guard let color = bitmap.colorAt(x: x, y: y)?.usingColorSpace(.sRGB) else {
                    continue
                }
                let luminance =
                    color.redComponent * 0.2126
                    + color.greenComponent * 0.7152
                    + color.blueComponent * 0.0722
                minimumLuminance = min(minimumLuminance, luminance)
                maximumLuminance = max(maximumLuminance, luminance)
                if color.blueComponent > color.redComponent + 0.18,
                   color.blueComponent > color.greenComponent + 0.12 {
                    accentPixelCount += 1
                }
                if color.redComponent > 0.62,
                   color.greenComponent > 0.62,
                   color.blueComponent > 0.62 {
                    brightPixelCount += 1
                }
            }
        }

        return LargeLogCentralRenderSignal(
            accentPixelCount: accentPixelCount,
            brightPixelCount: brightPixelCount,
            luminanceRange: max(0, maximumLuminance - minimumLuminance)
        )
    }
}

@MainActor
private final class LargeLogSearchVisibilityModel: ObservableObject {
    @Published var text: String
    @Published var result: ResourceLogSearchResult
    @Published var selectedMatchIndex = 0
    @Published var navigationSequence = 1

    init(text: String, result: ResourceLogSearchResult) {
        self.text = text
        self.result = result
    }
}

private struct LargeLogSearchVisibilityHarness: View {
    @ObservedObject var model: LargeLogSearchVisibilityModel

    var body: some View {
        ResourceLogsOutputSurface(
            isLoadingLogs: false,
            isLoadingResources: false,
            errorMessage: nil,
            logText: model.text,
            renderSearchResult: model.result,
            navigationSearchResult: model.result,
            selectedSearchMatchIndex: model.selectedMatchIndex,
            searchNavigationSequence: model.navigationSequence,
            emptyTitle: "No output",
            emptyMessage: "No synthetic output.",
            noMatchesMessage: "No synthetic matches.",
            readOnlyResetID: "large-log-search-visibility",
            onReload: {},
            presentationStyle: .regular
        )
        .tint(.blue)
    }
}

private struct LargeLogCentralRenderSignal: CustomStringConvertible {
    let accentPixelCount: Int
    let brightPixelCount: Int
    let luminanceRange: CGFloat

    var description: String {
        "accentPixels=\(accentPixelCount), brightPixels=\(brightPixelCount), luminanceRange=\(luminanceRange)"
    }
}
