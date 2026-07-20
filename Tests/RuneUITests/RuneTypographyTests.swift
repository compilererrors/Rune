import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RuneTypographyTests: XCTestCase {
    func testInspectorInformationRowRemainsBoundedAtAccessibilityTextSizes() {
        let regular = fittingSize(dynamicTypeSize: .large)
        let accessibility = fittingSize(dynamicTypeSize: .accessibility3)

        for size in [regular, accessibility] {
            XCTAssertTrue(size.width.isFinite)
            XCTAssertTrue(size.height.isFinite)
            XCTAssertLessThanOrEqual(size.width, 261)
            XCTAssertGreaterThan(size.height, 0)
        }
        XCTAssertGreaterThanOrEqual(
            accessibility.height,
            regular.height,
            "The adaptive information row must preserve or grow its height instead of clipping enlarged copy."
        )
    }

    func testSecondaryCaptionAuditKeepsSupportingLabelsRegularWeight() throws {
        let designSource = try source(at: "Sources/RuneUI/Layout/RuneDesignComponents.swift")
        let importSource = try source(at: "Sources/RuneUI/Views/KubeConfigImportReviewPanel.swift")
        let nativeContextSource = try source(at: "Sources/RuneUI/Views/AddClusterNativeContextSection.swift")

        XCTAssertTrue(designSource.contains("Text(title)\n                .font(.caption)\n                .foregroundStyle(.secondary)"))
        XCTAssertTrue(importSource.contains("Text(context.name)\n                                .font(.caption2)\n                                .foregroundStyle(.secondary)"))
        XCTAssertTrue(importSource.contains("Duplicate handling requires an explicit choice before saving:"))
        XCTAssertTrue(nativeContextSource.contains("Text(\"Imported context\")\n                .font(.caption)\n                .foregroundStyle(.secondary)"))
    }

    private func fittingSize(dynamicTypeSize: DynamicTypeSize) -> CGSize {
        let host = NSHostingView(rootView: RuneInspectorInfoRow(
            "Namespace and environment",
            systemImage: "square.stack.3d.up"
        ) {
            Text("A long localized value that must remain readable")
                .fixedSize(horizontal: false, vertical: true)
        }
        .environment(\.dynamicTypeSize, dynamicTypeSize)
        .frame(width: 260))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    private func source(at relativePath: String) throws -> String {
        let current = URL(fileURLWithPath: #filePath)
        let repoRoot = current
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return try String(
            contentsOf: repoRoot.appendingPathComponent(relativePath),
            encoding: .utf8
        )
    }
}
