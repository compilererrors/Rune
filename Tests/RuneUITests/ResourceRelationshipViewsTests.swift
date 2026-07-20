import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class ResourceRelationshipViewsTests: XCTestCase {
    @MainActor
    func testLinkRowFillsAvailableWidthAndKeepsCompactMinimumHeight() {
        let host = NSHostingView(rootView: ResourceRelationshipLinkButton(
            title: "synthetic-workload",
            subtitle: "synthetic-namespace · Running",
            symbol: "cube.box",
            action: {}
        ).frame(width: 320))

        host.layoutSubtreeIfNeeded()
        let size = host.fittingSize

        XCTAssertEqual(size.width, 320, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(size.height, ResourceRelationshipLinkButton.minimumHeight)
        XCTAssertLessThanOrEqual(size.height, ResourceRelationshipLinkButton.minimumHeight + 2)
    }

    @MainActor
    func testEmptyRowUsesTheSameVisualHeightAndLeadingAlignment() {
        let host = NSHostingView(rootView: ResourceRelationshipEmptyRow(
            title: "No related resources",
            subtitle: "Related resources will appear here."
        ).frame(width: 320))

        host.layoutSubtreeIfNeeded()
        let size = host.fittingSize

        XCTAssertEqual(size.width, 320, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(size.height, ResourceRelationshipLinkButton.minimumHeight)
        XCTAssertLessThanOrEqual(size.height, ResourceRelationshipLinkButton.minimumHeight + 2)
    }

    @MainActor
    func testRelationshipSectionKeepsSmallListsCompactAndCapsHighCardinalityContent() {
        let single = relationshipSectionHost(rowCount: 1)
        let many = relationshipSectionHost(rowCount: 80)
        let singleSize = single.fittingSize
        let manySize = many.fittingSize

        XCTAssertGreaterThan(manySize.height, singleSize.height)
        XCTAssertLessThanOrEqual(
            manySize.height - singleSize.height,
            ResourceRelationshipLayoutMetrics.maximumListHeight
                - ResourceRelationshipLayoutMetrics.minimumRowHeight
                + 3
        )
        XCTAssertLessThanOrEqual(
            manySize.height,
            170,
            "KPI: 80 relationships must keep their inspector section below 170 points at compact width."
        )
        XCTAssertEqual(scrollViews(in: single).count, 0)
        XCTAssertEqual(scrollViews(in: many).count, 1)
    }

    @MainActor
    func testBoundedInnerScrollStartsOnlyAboveTheVisibleRowLimit() {
        let atLimit = relationshipSectionHost(
            rowCount: ResourceRelationshipLayoutMetrics.maximumVisibleRows
        )
        let aboveLimit = relationshipSectionHost(
            rowCount: ResourceRelationshipLayoutMetrics.maximumVisibleRows + 1
        )

        XCTAssertEqual(scrollViews(in: atLimit).count, 0)
        XCTAssertEqual(scrollViews(in: aboveLimit).count, 1)
        XCTAssertLessThanOrEqual(aboveLimit.fittingSize.height, 170)
    }

    @MainActor
    func testOuterInspectorRemainsTheOnlyScrollOwnerForSmallRelationships() {
        let small = relationshipSectionInsideOuterScrollHost(rowCount: 2)
        let many = relationshipSectionInsideOuterScrollHost(rowCount: 80)

        XCTAssertEqual(
            scrollViews(in: small).count,
            1,
            "Small relationship lists should use only the inspector's outer vertical viewport."
        )
        XCTAssertEqual(
            scrollViews(in: many).count,
            2,
            "High-cardinality relationships may add one bounded viewport inside the inspector."
        )
    }

    func testLinkRowSourceKeepsFullHitAreaAndClearAccessibilityContract() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/ResourceRelationshipViews.swift"),
            encoding: .utf8
        )

        XCTAssertTrue(source.contains("Button(action: action)"))
        XCTAssertTrue(source.contains("ScrollView(.vertical)"))
        XCTAssertTrue(source.contains("rowCount > ResourceRelationshipLayoutMetrics.maximumVisibleRows"))
        XCTAssertTrue(source.contains("guard let rowCount else { return false }"))
        XCTAssertTrue(source.contains("ResourceRelationshipSection(title: \"Related Pods\", rowCount: pods.count)"))
        XCTAssertTrue(source.contains("ResourceRelationshipSection(title: \"Related Events\", rowCount: events.count)"))
        XCTAssertTrue(source.contains("maxHeight: ResourceRelationshipLayoutMetrics.maximumListHeight"))
        XCTAssertTrue(source.contains(".frame(maxWidth: .infinity, minHeight: Self.minimumHeight"))
        XCTAssertTrue(source.contains(".contentShape(Rectangle())"))
        XCTAssertTrue(source.contains(".accessibilityLabel(title)"))
        XCTAssertTrue(source.contains(".accessibilityValue(subtitle)"))
        XCTAssertTrue(source.contains(".accessibilityHint(\"Opens the related resource in the inspector\")"))
    }

    @MainActor
    private func relationshipSectionHost(rowCount: Int) -> NSHostingView<some View> {
        let host = NSHostingView(rootView: ResourceRelationshipSection(
            title: "Synthetic relationships",
            rowCount: rowCount
        ) {
            ForEach(0..<rowCount, id: \.self) { index in
                ResourceRelationshipLinkButton(
                    title: "synthetic-related-\(index)",
                    subtitle: "synthetic-namespace · Ready",
                    symbol: "cube.box",
                    action: {}
                )
            }
        }
        .frame(width: 320))
        host.layoutSubtreeIfNeeded()
        return host
    }

    @MainActor
    private func relationshipSectionInsideOuterScrollHost(rowCount: Int) -> NSHostingView<some View> {
        let host = NSHostingView(rootView: ScrollView(.vertical) {
            ResourceRelationshipSection(
                title: "Synthetic relationships",
                rowCount: rowCount
            ) {
                ForEach(0..<rowCount, id: \.self) { index in
                    ResourceRelationshipLinkButton(
                        title: "synthetic-related-\(index)",
                        subtitle: "synthetic-namespace · Ready",
                        symbol: "cube.box",
                        action: {}
                    )
                }
            }
        }
        .frame(width: 320, height: 280))
        host.frame = NSRect(x: 0, y: 0, width: 320, height: 280)
        host.layoutSubtreeIfNeeded()
        return host
    }

    @MainActor
    private func scrollViews(in view: NSView) -> [NSScrollView] {
        var result = view is NSScrollView ? [view as! NSScrollView] : []
        for subview in view.subviews {
            result.append(contentsOf: scrollViews(in: subview))
        }
        return result
    }
}
