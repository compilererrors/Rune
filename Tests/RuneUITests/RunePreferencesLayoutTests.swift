import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RunePreferencesLayoutTests: XCTestCase {
    func testSettingsMetricsPreserveProfessionalPageAndRowGeometry() {
        XCTAssertEqual(RuneSettingsMetrics.pageHorizontalPadding, 20)
        XCTAssertEqual(RuneSettingsMetrics.pageSpacing, 14)
        XCTAssertEqual(RuneSettingsMetrics.sectionCardPadding, 14)
        XCTAssertEqual(RuneSettingsMetrics.rowMinHeight, 38)
        XCTAssertEqual(RuneSettingsMetrics.rowControlColumnWidth, 260)
        XCTAssertGreaterThan(RuneSettingsMetrics.rowLabelMinWidth, 0)
        XCTAssertGreaterThan(RuneSettingsMetrics.stackedRowSpacing, 0)
    }

    func testSettingsRowStacksAtNarrowWidthAndAccessibilityTextSize() {
        let wide = adaptiveRowSize(width: 680, dynamicTypeSize: .large)
        let narrow = adaptiveRowSize(width: 480, dynamicTypeSize: .large)
        let enlarged = adaptiveRowSize(width: 680, dynamicTypeSize: .accessibility3)

        XCTAssertEqual(wide.width, 680, accuracy: 0.5)
        XCTAssertEqual(narrow.width, 480, accuracy: 0.5)
        XCTAssertEqual(enlarged.width, 680, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(wide.height, RuneSettingsMetrics.rowMinHeight)
        XCTAssertGreaterThan(narrow.height, wide.height)
        XCTAssertGreaterThan(enlarged.height, wide.height)
    }

    func testCustomThemeActionsStackWithoutHorizontalOverflow() {
        let wide = adaptiveActionSize(width: 520, dynamicTypeSize: .large)
        let narrow = adaptiveActionSize(width: 190, dynamicTypeSize: .large)
        let enlarged = adaptiveActionSize(width: 520, dynamicTypeSize: .accessibility3)

        XCTAssertEqual(wide.width, 520, accuracy: 0.5)
        XCTAssertEqual(narrow.width, 190, accuracy: 0.5)
        XCTAssertEqual(enlarged.width, 520, accuracy: 0.5)
        XCTAssertGreaterThan(narrow.height, wide.height)
        XCTAssertGreaterThan(enlarged.height, wide.height)
    }

    func testPreferencesSourceUsesRegularControlsAndPurposefulAdaptiveFallbacks() throws {
        let source = try String(contentsOfFile: preferencesViewPath, encoding: .utf8)

        XCTAssertFalse(source.contains(".id(interfaceLanguageRaw)\n        .controlSize(.small)"))
        XCTAssertEqual(source.components(separatedBy: ".controlSize(.small)").count - 1, 4)
        XCTAssertTrue(source.contains("Toggle(\"⌘\""))
        XCTAssertTrue(source.contains("Toggle(\"⌥\""))
        XCTAssertTrue(source.contains("Toggle(\"⌃\""))
        XCTAssertTrue(source.contains("Toggle(\"⇧\""))

        XCTAssertTrue(source.contains("RuneSettingsAdaptiveRow(label: label, control: control)"))
        XCTAssertTrue(source.contains("RuneSettingsAdaptiveActionGroup {"))
        XCTAssertTrue(source.contains("dynamicTypeSize.isAccessibilitySize"))
        XCTAssertTrue(source.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(source.contains(".frame(width: RuneSettingsMetrics.rowControlColumnWidth, alignment: .trailing)"))
        XCTAssertTrue(source.contains("maxWidth: RuneSettingsMetrics.rowControlColumnWidth"))

        XCTAssertFalse(source.contains("showsHelpIcon"))
        XCTAssertFalse(source.contains("private func helpIcon"))
        XCTAssertEqual(source.components(separatedBy: "questionmark.circle").count - 1, 1)
        XCTAssertTrue(source.contains("RuneIconButton("))
        XCTAssertTrue(source.contains("SettingsHelpButton("))

        let spacer = try XCTUnwrap(source.range(of: "Spacer(minLength: 8)"))
        let selectedCheckmark = try XCTUnwrap(
            source.range(of: "if isSelected {", range: spacer.upperBound..<source.endIndex)
        )
        XCTAssertLessThan(spacer.lowerBound, selectedCheckmark.lowerBound)

        XCTAssertTrue(source.contains("ScrollView {"))
        XCTAssertTrue(source.contains(".padding(.horizontal, RuneSettingsMetrics.pageHorizontalPadding)"))
        XCTAssertTrue(source.contains(".runeInsetCard(padding: RuneSettingsMetrics.sectionCardPadding)"))
        XCTAssertTrue(source.contains(".frame(minHeight: RuneSettingsMetrics.rowMinHeight)"))
    }

    private func adaptiveRowSize(
        width: CGFloat,
        dynamicTypeSize: DynamicTypeSize
    ) -> CGSize {
        let host = NSHostingView(rootView: RuneSettingsAdaptiveRow {
            VStack(alignment: .leading, spacing: 3) {
                Text("Synthetic setting with a deliberately longer label")
                    .font(.subheadline.weight(.semibold))
                Text("Synthetic explanatory detail remains visible and can wrap onto additional lines.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }
        } control: {
            Button("Synthetic action") {}
        }
        .dynamicTypeSize(dynamicTypeSize)
        .frame(width: width))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    private func adaptiveActionSize(
        width: CGFloat,
        dynamicTypeSize: DynamicTypeSize
    ) -> CGSize {
        let host = NSHostingView(rootView: RuneSettingsAdaptiveActionGroup {
            Button("Create Template") {}
            Button("Open Folder") {}
            Button("Reload") {}
            RuneIconButton("Theme help", systemImage: "questionmark.circle") {}
        }
        .dynamicTypeSize(dynamicTypeSize)
        .frame(width: width))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    private var preferencesViewPath: String {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift")
            .path
    }
}
