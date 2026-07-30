import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RuneAdaptiveToolbarSingleGroupRegressionTests: XCTestCase {
    func testSingleGroupHeaderKeepsTitleInsideToolbarAtSupportedWidths() throws {
        let scenarios: [(width: CGFloat, dynamicTypeSize: DynamicTypeSize)] = [
            (320, .large),
            (700, .large),
            (320, .accessibility3),
            (700, .accessibility3)
        ]

        for (title, scenario) in product(
            ["Workloads", "Storage"],
            scenarios
        ) {
            let recorder = SingleGroupToolbarFrameRecorder()
            let host = NSHostingView(
                rootView: SingleGroupToolbarHarness(
                    title: title,
                    width: scenario.width,
                    dynamicTypeSize: scenario.dynamicTypeSize,
                    recorder: recorder
                )
            )
            host.frame = NSRect(x: 0, y: 0, width: scenario.width, height: 180)

            settle(host, until: {
                recorder.frames[.toolbar] != nil
                    && recorder.frames[.primaryTitle] != nil
            })

            let toolbar = try XCTUnwrap(
                recorder.frames[.toolbar],
                "Missing \(title) toolbar frame at \(scenario.width)pt / \(scenario.dynamicTypeSize)."
            )
            let titleFrame = try XCTUnwrap(
                recorder.frames[.primaryTitle],
                "\(title) was not placed at \(scenario.width)pt / \(scenario.dynamicTypeSize)."
            )

            let expectedMinimumHeight = scenario.dynamicTypeSize.isAccessibilitySize
                ? RuneAdaptiveToolbarMetrics.accessibilityMinimumRowHeight
                : RuneAdaptiveToolbarMetrics.minimumRowHeight
            XCTAssertGreaterThanOrEqual(
                toolbar.height,
                expectedMinimumHeight,
                "A toolbar with EmptyView secondary content must retain its full row height."
            )
            XCTAssertGreaterThan(
                titleFrame.width,
                0,
                "The primary title must retain visible width when it is the toolbar's only group."
            )
            XCTAssertGreaterThan(
                titleFrame.height,
                0,
                "The primary title must retain visible height when it is the toolbar's only group."
            )
            XCTAssertGreaterThanOrEqual(
                titleFrame.minX,
                toolbar.minX - 0.5,
                "The center-pane title escaped behind the toolbar's leading edge."
            )
            XCTAssertEqual(
                titleFrame.minX,
                toolbar.minX,
                accuracy: 0.5,
                "The center-pane title must stay aligned to its pane inset."
            )
            XCTAssertLessThanOrEqual(
                titleFrame.maxX,
                toolbar.maxX + 0.5,
                "The center-pane title escaped past the toolbar's trailing edge."
            )
            XCTAssertGreaterThanOrEqual(
                titleFrame.minY,
                toolbar.minY - 0.5,
                "The center-pane title escaped above the toolbar."
            )
            XCTAssertLessThanOrEqual(
                titleFrame.maxY,
                toolbar.maxY + 0.5,
                "The center-pane title escaped below the toolbar."
            )
        }
    }

    private func product<A, B>(_ lhs: [A], _ rhs: [B]) -> [(A, B)] {
        lhs.flatMap { left in rhs.map { right in (left, right) } }
    }

    private func settle(
        _ host: NSView,
        until condition: () -> Bool
    ) {
        for _ in 0..<60 {
            host.layoutSubtreeIfNeeded()
            if condition() { return }
            RunLoop.main.run(until: Date().addingTimeInterval(0.005))
        }
    }
}

@MainActor
private final class SingleGroupToolbarFrameRecorder: ObservableObject {
    @Published var frames: [SingleGroupToolbarRegion: CGRect] = [:]
}

private enum SingleGroupToolbarRegion: Hashable {
    case toolbar
    case primaryTitle
}

private struct SingleGroupToolbarFramePreferenceKey: PreferenceKey {
    static let defaultValue: [SingleGroupToolbarRegion: CGRect] = [:]

    static func reduce(
        value: inout [SingleGroupToolbarRegion: CGRect],
        nextValue: () -> [SingleGroupToolbarRegion: CGRect]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private struct SingleGroupToolbarFrameProbe: View {
    let region: SingleGroupToolbarRegion

    var body: some View {
        GeometryReader { proxy in
            Color.clear.preference(
                key: SingleGroupToolbarFramePreferenceKey.self,
                value: [
                    region: proxy.frame(in: .named(SingleGroupToolbarHarness.coordinateSpaceName))
                ]
            )
        }
    }
}

private struct SingleGroupToolbarHarness: View {
    static let coordinateSpaceName = "RuneAdaptiveToolbarSingleGroupRegression"

    let title: String
    let width: CGFloat
    let dynamicTypeSize: DynamicTypeSize
    @ObservedObject var recorder: SingleGroupToolbarFrameRecorder

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            RuneAdaptiveToolbar("Content header") {
                Text(title)
                    .font(.title2.weight(.bold))
                    .lineLimit(1)
                    .background(
                        SingleGroupToolbarFrameProbe(region: .primaryTitle)
                    )
            } secondary: {
                EmptyView()
            }
            .background(
                SingleGroupToolbarFrameProbe(region: .toolbar)
            )

            Spacer(minLength: 0)
        }
        .frame(width: width, height: 180, alignment: .topLeading)
        .coordinateSpace(name: Self.coordinateSpaceName)
        .environment(\.dynamicTypeSize, dynamicTypeSize)
        .onPreferenceChange(SingleGroupToolbarFramePreferenceKey.self) { frames in
            recorder.frames = frames
        }
    }
}
