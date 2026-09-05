import AppKit
import RuneCore
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class FavoritePodPickerTests: XCTestCase {
    func testSelectedAndRowFavoriteActionsRenderWithSharedHitTarget() {
        for placement in [
            FavoritePodFavoriteActionPlacement.selectedPod,
            .podRow,
        ] {
            for isFavorite in [false, true] {
                let host = NSHostingView(rootView: FavoritePodFavoriteActionButton(
                    podName: "synthetic-api-pod",
                    isFavorite: isFavorite,
                    placement: placement,
                    action: {}
                ))
                host.layoutSubtreeIfNeeded()

                XCTAssertGreaterThanOrEqual(
                    host.fittingSize.width,
                    RuneUILayoutMetrics.iconButtonSize,
                    "\(placement) favorite action is too narrow"
                )
                XCTAssertGreaterThanOrEqual(
                    host.fittingSize.height,
                    RuneUILayoutMetrics.iconButtonSize,
                    "\(placement) favorite action is too short"
                )
                XCTAssertLessThanOrEqual(host.fittingSize.width, RuneUILayoutMetrics.iconButtonSize + 1)
                XCTAssertLessThanOrEqual(host.fittingSize.height, RuneUILayoutMetrics.iconButtonSize + 1)
            }
        }
    }

    func testFavoriteActionLabelsStayDistinctAcrossPlacementAndState() {
        let actions = [
            FavoritePodFavoriteActionButton(
                podName: "synthetic-api-pod",
                isFavorite: false,
                placement: .selectedPod,
                action: {}
            ),
            FavoritePodFavoriteActionButton(
                podName: "synthetic-api-pod",
                isFavorite: true,
                placement: .selectedPod,
                action: {}
            ),
            FavoritePodFavoriteActionButton(
                podName: "synthetic-api-pod",
                isFavorite: false,
                placement: .podRow,
                action: {}
            ),
            FavoritePodFavoriteActionButton(
                podName: "synthetic-api-pod",
                isFavorite: true,
                placement: .podRow,
                action: {}
            ),
        ]

        XCTAssertEqual(Set(actions.map(\.accessibilityLabel)).count, actions.count)
        XCTAssertEqual(actions[0].accessibilityLabel, "Add selected pod synthetic-api-pod to favorites")
        XCTAssertEqual(actions[1].accessibilityLabel, "Remove selected pod synthetic-api-pod from favorites")
        XCTAssertEqual(actions[2].accessibilityLabel, "Add pod synthetic-api-pod to favorites")
        XCTAssertEqual(actions[3].accessibilityLabel, "Remove pod synthetic-api-pod from favorites")
    }

    func testPickerRoutesBothFavoritePlacementsThroughRuneIconButton() throws {
        let source = try String(contentsOfFile: favoritePodPickerPath, encoding: .utf8)

        XCTAssertEqual(
            source.components(separatedBy: "FavoritePodFavoriteActionButton(").count - 1,
            2
        )
        XCTAssertTrue(source.contains("placement: .selectedPod"))
        XCTAssertTrue(source.contains("placement: .podRow"))
        XCTAssertTrue(source.contains("isSelected: isFavorite"))
        XCTAssertTrue(source.contains("selectedTint: .yellow"))
        XCTAssertTrue(source.contains("cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius"))
        XCTAssertFalse(source.contains("cornerRadius: 7"))
        XCTAssertFalse(source.contains(".frame(width: 24, height: 24)"))
        XCTAssertFalse(source.contains(".frame(width: 30, height: 26)"))
    }

    func testTruncatedPickerNamesExposeTheirFullTextOnHover() throws {
        let source = try String(contentsOfFile: favoritePodPickerPath, encoding: .utf8)

        XCTAssertTrue(source.contains(".help(selectedPodHelpText)"))
        XCTAssertTrue(source.contains(".help(rowTitle(pod))"))
        XCTAssertTrue(source.contains(".help(detail)"))
        XCTAssertTrue(source.contains("selectedPod.map(podHelpText)"))
        XCTAssertTrue(source.contains(".frame(width: max(width, FavoritePodPickerMetrics.popoverMinimumWidth))"))
        XCTAssertEqual(FavoritePodPickerMetrics.popoverMinimumWidth, 320)
    }

    func testPickerRendersAtCompactAndWideReservedWidths() {
        let pod = PodSummary(
            name: "synthetic-api-component-with-a-long-distinguishing-suffix-0",
            namespace: "synthetic-namespace",
            status: "Running"
        )

        for width in [CGFloat(300), 360] {
            let host = NSHostingView(rootView: FavoritePodPicker(
                title: "Pod",
                pods: [pod],
                width: width,
                rowTitle: { $0.name },
                rowDetail: { "\($0.namespace) - \($0.status)" },
                isFavoritePod: { _ in false },
                onToggleFavoritePod: { _ in },
                selection: .constant(pod.id)
            ))
            host.layoutSubtreeIfNeeded()

            XCTAssertEqual(host.fittingSize.width, width, accuracy: 0.5)
            XCTAssertEqual(host.fittingSize.height, RuneUILayoutMetrics.iconButtonSize, accuracy: 0.5)
        }
    }

    private var favoritePodPickerPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/FavoritePodPicker.swift").path
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
