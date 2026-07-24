import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class RuneTypographyTests: XCTestCase {
    func testConfiguredFontPreferenceMapsToBoundedInterfacePointSizes() {
        XCTAssertEqual(
            RuneInterfaceTypography.preferredMenuFontSize(configuredFontSize: -100),
            13
        )
        XCTAssertEqual(
            RuneInterfaceTypography.preferredMenuFontSize(configuredFontSize: 12.999),
            13
        )
        XCTAssertEqual(
            RuneInterfaceTypography.preferredMenuFontSize(configuredFontSize: 13),
            14
        )
        XCTAssertEqual(
            RuneInterfaceTypography.preferredMenuFontSize(configuredFontSize: 14.999),
            14
        )
        XCTAssertEqual(
            RuneInterfaceTypography.preferredMenuFontSize(configuredFontSize: 15),
            15
        )
        XCTAssertEqual(
            RuneInterfaceTypography.preferredMenuFontSize(configuredFontSize: 100),
            15
        )
        XCTAssertEqual(
            RuneInterfaceTypography.preferredMenuFontSize(configuredFontSize: .nan),
            13,
            "NaN must fall back to Rune's default 12pt preference."
        )
        XCTAssertEqual(
            RuneInterfaceTypography.preferredMenuFontSize(configuredFontSize: -.infinity),
            13
        )
        XCTAssertEqual(
            RuneInterfaceTypography.preferredMenuFontSize(configuredFontSize: .infinity),
            15
        )
    }

    func testConfiguredFontPreferenceNeverReducesSystemInterfacePointSize() {
        let systemSizes: [(DynamicTypeSize, CGFloat)] = [
            (.small, 13),
            (.large, 13),
            (.xLarge, 14),
            (.xxLarge, 15),
            (.xxxLarge, 16),
            (.accessibility1, 16),
            (.accessibility3, 16)
        ]

        for (systemSize, expectedPointSize) in systemSizes {
            XCTAssertEqual(
                RuneInterfaceTypography.effectiveMenuFontSize(
                    configuredFontSize: RuneSettingsKeys.terminalFontSizeMinimum,
                    systemDynamicTypeSize: systemSize
                ),
                expectedPointSize
            )
        }

        XCTAssertEqual(
            RuneInterfaceTypography.effectiveMenuFontSize(
                configuredFontSize: 13,
                systemDynamicTypeSize: .large
            ),
            14
        )
        XCTAssertEqual(
            RuneInterfaceTypography.effectiveMenuFontSize(
                configuredFontSize: 15,
                systemDynamicTypeSize: .small
            ),
            15
        )
        XCTAssertEqual(
            RuneInterfaceTypography.effectiveMenuFontSize(
                configuredFontSize: 20,
                systemDynamicTypeSize: .accessibility3
            ),
            16
        )
    }

    func testAppKitMenuFontScaleIsMonotonicAndBounded() {
        let base: CGFloat = 11

        XCTAssertEqual(
            RuneInterfaceTypography.appKitMenuFontSize(
                systemSmallFontSize: base,
                interfaceMenuFontSize: 9
            ),
            base
        )
        XCTAssertEqual(
            RuneInterfaceTypography.appKitMenuFontSize(
                systemSmallFontSize: base,
                interfaceMenuFontSize: 13
            ),
            base
        )
        XCTAssertEqual(
            RuneInterfaceTypography.appKitMenuFontSize(
                systemSmallFontSize: base,
                interfaceMenuFontSize: 14
            ),
            base + 1
        )
        XCTAssertEqual(
            RuneInterfaceTypography.appKitMenuFontSize(
                systemSmallFontSize: base,
                interfaceMenuFontSize: 15
            ),
            base + 2
        )
        XCTAssertEqual(
            RuneInterfaceTypography.appKitMenuFontSize(
                systemSmallFontSize: base,
                interfaceMenuFontSize: 16
            ),
            base + 3
        )
        XCTAssertEqual(
            RuneInterfaceTypography.appKitMenuFontSize(
                systemSmallFontSize: base,
                interfaceMenuFontSize: 100
            ),
            base + 3
        )
    }

    func testNativeControlSizesGrowFromTheirIntendedBaseline() {
        XCTAssertEqual(
            RuneInterfaceTypography.controlSize(
                interfaceMenuFontSize: 13,
                compactBaseline: true
            ),
            .small
        )
        XCTAssertEqual(
            RuneInterfaceTypography.controlSize(
                interfaceMenuFontSize: 14,
                compactBaseline: true
            ),
            .regular
        )
        XCTAssertEqual(
            RuneInterfaceTypography.controlSize(
                interfaceMenuFontSize: 15,
                compactBaseline: true
            ),
            .large
        )
        XCTAssertEqual(
            RuneInterfaceTypography.controlSize(
                interfaceMenuFontSize: 13,
                compactBaseline: false
            ),
            .regular
        )
        XCTAssertEqual(
            RuneInterfaceTypography.controlSize(
                interfaceMenuFontSize: 14,
                compactBaseline: false
            ),
            .large
        )
    }

    func testSettingsMenuLabelActuallyGrowsWithInterfaceFontPreference() {
        let standard = settingsMenuLabelFittingSize(configuredFontSize: 12)
        let enlarged = settingsMenuLabelFittingSize(configuredFontSize: 15)

        XCTAssertGreaterThan(
            enlarged.width,
            standard.width,
            "The explicit Rune interface font must enlarge rendered Settings menu text."
        )
        XCTAssertGreaterThanOrEqual(enlarged.height, standard.height)
    }

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

    private func settingsMenuLabelFittingSize(configuredFontSize: Double) -> CGSize {
        let host = NSHostingView(rootView: RuneSettingsMenuLabel(
            title: "Configured export application",
            systemImage: "doc.text",
            subtitle: "Optional application"
        )
        .runeInterfaceTypography(
            configuredFontSize: configuredFontSize,
            systemDynamicTypeSize: .large
        )
        .fixedSize())
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
