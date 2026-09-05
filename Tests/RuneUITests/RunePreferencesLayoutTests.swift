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
        XCTAssertEqual(RuneSettingsMetrics.compactMenuControlWidth, 190)
        XCTAssertGreaterThan(RuneSettingsMetrics.rowLabelMinWidth, 0)
        XCTAssertGreaterThan(RuneSettingsMetrics.stackedRowSpacing, 0)
        XCTAssertEqual(
            RuneSettingsMetrics.rowLabelMinWidth
                + RuneSettingsMetrics.rowControlSpacing
                + RuneSettingsMetrics.rowControlColumnWidth,
            512
        )
    }

    func testFixedSettingsMenuLabelHoverIncludesFullTitleAndSubtitle() {
        let label = RuneSettingsMenuLabel(
            title: "Synthetic export destination with a long suffix",
            systemImage: "folder",
            subtitle: "Synthetic application name"
        )

        XCTAssertEqual(
            label.resolvedHelpText,
            "Synthetic export destination with a long suffix — Synthetic application name"
        )
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

    func testCustomThemeActionRailKeepsPrimaryActionsVisibleWithoutVerticalWall() {
        let wide = customThemeActionRowSize(width: 680, dynamicTypeSize: .large)
        let narrow = customThemeActionRowSize(width: 480, dynamicTypeSize: .large)
        let enlarged = customThemeActionRowSize(width: 680, dynamicTypeSize: .accessibility3)

        XCTAssertEqual(wide.width, 680, accuracy: 0.5)
        XCTAssertEqual(narrow.width, 480, accuracy: 0.5)
        XCTAssertEqual(enlarged.width, 680, accuracy: 0.5)
        XCTAssertLessThanOrEqual(wide.height, 76)
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
        XCTAssertTrue(source.contains("dynamicTypeSize.isAccessibilitySize"))
        XCTAssertTrue(source.contains("private struct RuneSettingsRowLayout: Layout"))
        XCTAssertTrue(source.contains("RuneSettingsRowLayout(forceStacked: dynamicTypeSize.isAccessibilitySize)"))
        XCTAssertTrue(source.contains("forceStacked || availableWidth < minimumHorizontalWidth"))
        XCTAssertFalse(source.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(source.contains("anchor: .topTrailing"))
        XCTAssertTrue(source.contains("anchor: .topLeading"))
        XCTAssertFalse(source.contains("control\n                .frame(maxWidth: RuneSettingsMetrics.rowControlColumnWidth"))

        XCTAssertFalse(source.contains("showsHelpIcon"))
        XCTAssertFalse(source.contains("private func helpIcon"))
        XCTAssertEqual(source.components(separatedBy: "questionmark.circle").count - 1, 1)
        XCTAssertTrue(source.contains("RuneIconButton("))
        XCTAssertTrue(source.contains("SettingsHelpButton("))

        XCTAssertTrue(source.contains(".accessibilityLabel(theme.title)"))
        XCTAssertTrue(source.contains(".accessibilityValue(isSelected ? \"Selected\" : presentation.appearanceTitle)"))
        XCTAssertTrue(source.contains("if isSelected {"))

        XCTAssertTrue(source.contains("ScrollView {"))
        XCTAssertTrue(source.contains(".padding(.horizontal, RuneSettingsMetrics.pageHorizontalPadding)"))
        XCTAssertTrue(source.contains(".runeInsetCard(padding: RuneSettingsMetrics.sectionCardPadding)"))
        XCTAssertTrue(source.contains(".frame(minHeight: RuneSettingsMetrics.rowMinHeight)"))
    }

    func testThemeGridAndLogSettingsKeepPurposefulConsistentLayouts() throws {
        let source = try String(contentsOfFile: preferencesViewPath, encoding: .utf8)
        let themes = try XCTUnwrap(source.slice(
            from: "private var themesSettingsForm: some View",
            to: "private var clampedTerminalFontSize"
        ))
        let themeMenu = try XCTUnwrap(source.slice(
            from: "private var themeOverflowMenu: some View",
            to: "private var customThemeActions"
        ))
        let logs = try XCTUnwrap(source.slice(
            from: "private var logsSettingsForm: some View",
            to: "private var safetySettingsForm"
        ))

        XCTAssertTrue(themes.contains("settingsSection(\"Choose theme\")"))
        XCTAssertTrue(themes.contains("LazyVGrid("))
        XCTAssertTrue(themes.contains("columns: themeGridColumns"))
        XCTAssertTrue(themes.contains("RuneThemeSelectorCard("))
        XCTAssertTrue(themes.contains("themeOverflowMenu"))
        XCTAssertTrue(themes.contains("settingsGridRow {"))
        XCTAssertTrue(themes.contains("} control: {\n                        themeOverflowMenu"))
        XCTAssertTrue(themeMenu.contains("RuneSettingsMenuLabel("))
        XCTAssertTrue(themeMenu.contains("width: RuneSettingsMetrics.compactMenuControlWidth"))
        XCTAssertTrue(themes.contains("customThemeActions"))
        XCTAssertTrue(themes.contains("displayThemesDirectoryPath"))
        XCTAssertFalse(themes.contains("themePickerMenu"))
        XCTAssertFalse(themes.contains("customThemeManagementMenu"))
        XCTAssertFalse(themes.contains("RuneSettingsAdaptiveActionGroup"))

        XCTAssertTrue(source.contains("if dynamicTypeSize.isAccessibilitySize"))
        XCTAssertTrue(source.contains("return [GridItem(.flexible(minimum: 220), spacing: 12)]"))
        XCTAssertTrue(source.contains("createThemeTemplateButton"))
        XCTAssertTrue(source.contains("Label(\"New Theme\", systemImage: \"doc.badge.plus\")"))
        XCTAssertTrue(source.contains(".accessibilityLabel(\"Create theme template\")"))
        XCTAssertFalse(source.contains("Label(\"Create Template\", systemImage: \"doc.badge.plus\")"))
        XCTAssertTrue(source.contains("openThemesFolderButton"))
        XCTAssertTrue(source.contains("reloadThemesButton"))
        XCTAssertTrue(source.contains("customThemeMoreMenu"))
        XCTAssertTrue(source.contains("return \"~\" + String(path.dropFirst(homePath.count))"))

        XCTAssertTrue(logs.contains("exportFolderMenu"))
        XCTAssertTrue(logs.contains("exportOpenerMenu(inklineRecommendation)"))
        XCTAssertTrue(logs.contains("exportOpenerMenu(quikZipRecommendation)"))
        XCTAssertFalse(logs.contains("exportOpenerRecommendationRow"))
        XCTAssertFalse(logs.contains("Text opener bundle ID"))
        XCTAssertFalse(logs.contains("Archive opener bundle ID"))
        XCTAssertFalse(logs.contains("Button(\"Use "))
    }

    func testLanguageMenuUsesTheSharedTrailingControlRail() throws {
        let source = try String(contentsOfFile: preferencesViewPath, encoding: .utf8)
        let general = try XCTUnwrap(source.slice(
            from: "private var generalSettingsForm: some View",
            to: "private var themesSettingsForm"
        ))

        XCTAssertTrue(general.contains("Picker(\"Language\", selection: $interfaceLanguageRaw)"))
        XCTAssertTrue(general.contains(".pickerStyle(.menu)"))
        XCTAssertTrue(general.contains("width: RuneSettingsMetrics.compactMenuControlWidth"))
        XCTAssertTrue(general.contains("alignment: .trailing"))
    }

    func testPerformanceSettingsUseOneVisibleLabeledNumericControlContract() throws {
        let source = try String(contentsOfFile: preferencesViewPath, encoding: .utf8)
        let editor = try XCTUnwrap(source.slice(
            from: "struct RuneSettingsIntegerLimitEditor",
            to: "/// Settings window content."
        ))
        let performance = try XCTUnwrap(source.slice(
            from: "private var performanceSettingsForm: some View",
            to: "private func customLogPresetRow"
        ))

        XCTAssertTrue(editor.contains("RuneSettingsAdaptiveRow {"))
        XCTAssertTrue(editor.contains("TextField(placeholder, value: normalizedBinding, format: .number)"))
        XCTAssertTrue(editor.contains("Text(valueSuffix)"))
        XCTAssertTrue(editor.contains(".accessibilityLabel(\"\\(title) value\")"))
        XCTAssertTrue(editor.contains("Stepper(\"Adjust \\(title)\", value: normalizedBinding, step: step)"))
        XCTAssertTrue(editor.contains("Button(\"Reset\")"))
        XCTAssertTrue(editor.contains("Current value: \\(normalizedValue) \\(valueSuffix)"))

        XCTAssertEqual(
            performance.components(separatedBy: "RuneSettingsIntegerLimitEditor(").count - 1,
            3
        )
        XCTAssertTrue(performance.contains("title: \"Scrollback\""))
        XCTAssertTrue(performance.contains("valueSuffix: \"lines\""))
        XCTAssertTrue(performance.contains("title: \"Log cache\""))
        XCTAssertTrue(performance.contains("valueSuffix: \"resources\""))
        XCTAssertTrue(performance.contains("title: \"YAML undo\""))
        XCTAssertTrue(performance.contains("valueSuffix: \"snapshots\""))
        XCTAssertFalse(performance.contains("HStack(alignment: .firstTextBaseline"))
    }

    func testIntegerLimitEditorKeepsPartialValueAndCaretWhileTypingAboveMinimum() async throws {
        let model = SyntheticIntegerLimitModel(value: 128)
        let host = NSHostingController(
            rootView: SyntheticIntegerLimitEditorHarness(model: model)
                .frame(width: 620, height: 100)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 620, height: 100),
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
        let textField = try XCTUnwrap(editableTextFields(in: host.view).first)
        window.makeFirstResponder(textField)
        try await settle(window: window)
        let fieldEditor = try XCTUnwrap(textField.currentEditor() as? NSTextView)
        fieldEditor.setSelectedRange(NSRange(location: 0, length: fieldEditor.string.utf16.count))

        fieldEditor.insertText("2", replacementRange: fieldEditor.selectedRange())
        try await settle(window: window)

        XCTAssertEqual(model.value, 16)
        XCTAssertEqual(fieldEditor.string, "2")
        XCTAssertEqual(fieldEditor.selectedRange(), NSRange(location: 1, length: 0))

        fieldEditor.insertText("0", replacementRange: fieldEditor.selectedRange())
        fieldEditor.insertText("0", replacementRange: fieldEditor.selectedRange())
        try await settle(window: window)

        XCTAssertEqual(model.value, 200)
        XCTAssertEqual(fieldEditor.string, "200")
        XCTAssertEqual(fieldEditor.selectedRange(), NSRange(location: 3, length: 0))
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

    private func customThemeActionRowSize(
        width: CGFloat,
        dynamicTypeSize: DynamicTypeSize
    ) -> CGSize {
        let host = NSHostingView(rootView: RuneSettingsAdaptiveRow {
            VStack(alignment: .leading, spacing: 3) {
                Text("Synthetic managed files")
                    .font(.subheadline.weight(.semibold))
                Text("Create, open, or reload these files from one control.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }
        } control: {
            SyntheticCustomThemeActions()
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
private final class SyntheticIntegerLimitModel: ObservableObject {
    @Published var value: Int

    init(value: Int) {
        self.value = value
    }
}

private struct SyntheticIntegerLimitEditorHarness: View {
    @ObservedObject var model: SyntheticIntegerLimitModel

    var body: some View {
        RuneSettingsIntegerLimitEditor(
            title: "Synthetic limit",
            value: $model.value,
            valueSuffix: "items",
            step: 8,
            placeholder: "128",
            defaultValue: 128,
            detail: "Synthetic integer input.",
            normalize: { max(16, $0) }
        )
    }
}

private struct SyntheticCustomThemeActions: View {
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    var body: some View {
        if dynamicTypeSize.isAccessibilitySize {
            HStack(spacing: 8) {
                Button("New Theme") {}
                    .accessibilityLabel("Create theme template")
                Menu("More") {
                    Button("Open Folder") {}
                    Button("Reload") {}
                }
                Button("Help", systemImage: "questionmark.circle") {}
            }
        } else {
            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 8) {
                    Button("New Theme") {}
                        .accessibilityLabel("Create theme template")
                        .frame(maxWidth: .infinity)
                    Button("Open Folder") {}
                        .frame(maxWidth: .infinity)
                }
                HStack(spacing: 8) {
                    Button("Reload") {}
                    Spacer(minLength: 8)
                    Button("Help", systemImage: "questionmark.circle") {}
                }
            }
        }
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
}
