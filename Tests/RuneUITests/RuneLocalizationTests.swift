import XCTest
@testable import RuneCore
@testable import RuneUI

final class RuneLocalizationTests: XCTestCase {
    func testBuiltInLocalizationCatalogLoadsBuiltInLanguages() {
        let strings = RuneLocalizedStrings.shared

        XCTAssertEqual(strings.string(.saveLogs, language: .english), "Save Logs")
        XCTAssertEqual(strings.string(.saveLogs, language: .spanish), "Guardar logs")
        XCTAssertEqual(strings.string(.saveLogs, language: .german), "Logs speichern")
        XCTAssertEqual(strings.string(.saveLogs, language: .french), "Enregistrer les logs")
        XCTAssertEqual(strings.string(.saveLogs, language: .hindi), "Logs सेव करें")
        XCTAssertEqual(strings.string(.saveLogs, language: .simplifiedChinese), "保存日志")
        XCTAssertEqual(strings.string(.previous, language: .spanish), "Anterior")
        XCTAssertEqual(strings.string(.previous, language: .german), "Vorherige")
        XCTAssertEqual(strings.string(.previous, language: .french), "Precedent")
        XCTAssertEqual(strings.string(.previous, language: .hindi), "पिछला")
        XCTAssertEqual(strings.string(.reload, language: .simplifiedChinese), "重新加载")
        XCTAssertEqual(strings.string(.workloads, language: .spanish), "Cargas")
        XCTAssertEqual(strings.string(.terminal, language: .simplifiedChinese), "终端")
        XCTAssertEqual(strings.string(.applyYAML, language: .spanish), "Aplicar YAML")
        XCTAssertEqual(strings.string(.findInYAML, language: .simplifiedChinese), "在 YAML 中查找")
    }

    func testBuiltInLocalizationCatalogCoversEveryKnownKey() {
        let strings = RuneLocalizedStrings.shared

        for language in RuneLanguage.allCases {
            for key in RuneLocalizedStringKey.allCases {
                let localized = strings.string(key, language: language)
                XCTAssertNotEqual(
                    localized,
                    key.rawValue,
                    "Missing localization for \(key.rawValue) in \(language.rawValue)"
                )
                XCTAssertFalse(
                    localized.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
                    "Empty localization for \(key.rawValue) in \(language.rawValue)"
                )
            }
        }
    }

    func testLocalizationCatalogFallsBackToEnglishForMissingTablesAndKeys() {
        let strings = RuneLocalizedStrings(bundle: Bundle())

        XCTAssertEqual(strings.string(.saveLogs, language: .spanish), "saveLogs")
        XCTAssertEqual(RuneLanguage.resolved("unknown"), .english)
    }

    func testLocalizationBundleResolverSupportsSignedAppResourceLayout() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-localization-bundle-\(UUID().uuidString)")
        let resources = root
            .appendingPathComponent("Contents")
            .appendingPathComponent("Resources")
        let bundleURL = resources.appendingPathComponent("Rune_RuneUI.bundle")
        try FileManager.default.createDirectory(at: bundleURL, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        try #"{"saveLogs":"Signed Resource Logs"}"#
            .write(to: bundleURL.appendingPathComponent("en.json"), atomically: true, encoding: .utf8)

        let bundle = try XCTUnwrap(RuneLocalizedStrings.preferredAppResourceBundle(baseURLs: [resources]))
        let strings = RuneLocalizedStrings(bundle: bundle)

        XCTAssertEqual(bundle.bundleURL.standardizedFileURL, bundleURL.standardizedFileURL)
        XCTAssertEqual(strings.string(.saveLogs, language: .english), "Signed Resource Logs")
    }

    func testInterfaceLanguageSettingHasRegisteredDefaultAndUserDefaultsAccessor() {
        RuneSettingsKeys.registerDefaults()

        let defaults = UserDefaults.standard
        let previous = defaults.object(forKey: RuneSettingsKeys.interfaceLanguage)
        defer {
            if let previous {
                defaults.set(previous, forKey: RuneSettingsKeys.interfaceLanguage)
            } else {
                defaults.removeObject(forKey: RuneSettingsKeys.interfaceLanguage)
            }
        }

        defaults.removeObject(forKey: RuneSettingsKeys.interfaceLanguage)
        XCTAssertEqual(defaults.runeInterfaceLanguage, RuneSettingsKeys.interfaceLanguageDefault)

        defaults.runeInterfaceLanguage = RuneLanguage.spanish.rawValue
        XCTAssertEqual(defaults.runeInterfaceLanguage, RuneLanguage.spanish.rawValue)
    }

    func testResourceLogsToolbarReadsInterfaceLanguageFromSettings() throws {
        let source = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("@AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw"))
        XCTAssertTrue(source.contains("interfaceLanguageRaw: interfaceLanguageRaw"))
        XCTAssertTrue(source.contains(".id(interfaceLanguageRaw)"))
        XCTAssertTrue(source.contains("private var language: RuneLanguage"))
        XCTAssertTrue(source.contains("RuneLanguage.resolved(interfaceLanguageRaw)"))
        XCTAssertTrue(source.contains("private func t(_ key: RuneLocalizedStringKey) -> String"))
        XCTAssertTrue(source.contains("RuneLocalizedStrings.shared.string(key, language: language)"))
        XCTAssertTrue(source.contains("Label(t(.saveLogs), systemImage: \"square.and.arrow.down\")"))
        XCTAssertTrue(source.contains("toolbarIconLabel(t(.previous), systemImage: \"clock.arrow.circlepath\""))
        XCTAssertTrue(source.contains("placeholder: t(.searchLogs)"))
        XCTAssertTrue(source.contains("RuneIconButton(findHelp, systemImage: \"magnifyingglass\")"))
    }

    func testRootAndManifestChromeReadInterfaceLanguageFromSettings() throws {
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)
        let manifestPresentationSource = try String(contentsOfFile: resourceManifestPresentationPath, encoding: .utf8)

        XCTAssertTrue(rootSource.contains("@AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw"))
        XCTAssertTrue(rootSource.contains("private func appString(_ key: RuneLocalizedStringKey) -> String"))
        XCTAssertTrue(rootSource.contains("section.localizedTitle(appString)"))
        XCTAssertTrue(rootSource.contains("viewModel.state.selectedSection.localizedTitle(appString)"))
        XCTAssertTrue(rootSource.contains("kind.localizedTitle(appString)"))
        XCTAssertTrue(rootSource.contains("tab.localizedTitle(appString)"))
        XCTAssertTrue(rootSource.contains("Button(appString(.applyYAML))"))

        XCTAssertTrue(yamlSource.contains("@AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw"))
        XCTAssertTrue(yamlSource.contains("ManifestInlineNote(t(.yamlEditsStayLocal))"))
        XCTAssertTrue(yamlSource.contains("applyTitle: t(.applyYAML)"))
        XCTAssertTrue(yamlSource.contains("Label(t(.hideManaged), systemImage: \"eye.slash\")"))

        XCTAssertTrue(describeSource.contains("@AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw"))
        XCTAssertTrue(describeSource.contains("Label(\"Edit YAML…\", systemImage: \"square.and.pencil\")"))
        XCTAssertFalse(describeSource.contains(".confirmationDialog("))
        XCTAssertTrue(describeSource.contains("applyTitle: t(.apply)"))
        XCTAssertTrue(describeSource.contains("placeholder: t(.findInDescribe)"))
        XCTAssertTrue(manifestPresentationSource.contains("Button(applyTitle, action: onApply)"))
    }

    func testKubeConfigPickerUsesOneConsistentEnglishPromptFlow() throws {
        let source = try String(contentsOfFile: kubeConfigPickerPath, encoding: .utf8)

        XCTAssertTrue(source.contains("panel.prompt = \"Import\""))
        XCTAssertTrue(source.contains("panel.prompt = \"Add Folder\""))
        XCTAssertTrue(source.contains("panel.prompt = \"Use Config\""))
        XCTAssertFalse(source.contains("Importera"))
    }

    func testSettingsLanguagePickerUsesSharedRightAlignedSettingsGrid() throws {
        let current = URL(fileURLWithPath: #filePath)
        let preferencesPath = current
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift")
            .path
        let source = try String(contentsOfFile: preferencesPath, encoding: .utf8)

        XCTAssertTrue(source.contains("settingsControlRow(\n                    title: settingsString(.language)"))
        XCTAssertTrue(source.contains("detail: settingsString(.settingsLanguageDetail)"))
        XCTAssertTrue(source.contains("Picker(\"Language\", selection: $interfaceLanguageRaw)"))
        XCTAssertTrue(source.contains(".id(interfaceLanguageRaw)"))
        XCTAssertTrue(source.contains("PreferencesPane.general.title(settingsString)"))
        XCTAssertTrue(source.contains("title: settingsString(.settingsGeneral)"))
        XCTAssertTrue(source.contains("settingsSection(settingsString(.settingsAppearance))"))
        XCTAssertTrue(source.contains("static let rowControlColumnWidth: CGFloat = 260"))
        XCTAssertTrue(source.contains("static let compactMenuControlWidth: CGFloat = 190"))
        XCTAssertTrue(source.contains("private func settingsGridRow"))
        XCTAssertTrue(source.contains("RuneSettingsAdaptiveRow(label: label, control: control)"))
        XCTAssertTrue(source.contains("private struct RuneSettingsRowLayout: Layout"))
        XCTAssertTrue(source.contains("RuneSettingsRowLayout(forceStacked: dynamicTypeSize.isAccessibilitySize)"))
        XCTAssertTrue(source.contains("forceStacked || availableWidth < minimumHorizontalWidth"))
        XCTAssertTrue(source.contains("anchor: .topTrailing"))
        XCTAssertTrue(source.contains("anchor: .topLeading"))
        XCTAssertTrue(source.contains("width: RuneSettingsMetrics.compactMenuControlWidth"))
        XCTAssertTrue(source.contains("alignment: .trailing"))
        XCTAssertTrue(source.contains(".pickerStyle(.menu)"))
        XCTAssertFalse(source.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(source.contains("dynamicTypeSize.isAccessibilitySize"))
        XCTAssertFalse(source.contains("languagePickerWidth"))
        XCTAssertTrue(source.contains("Toggle(title, isOn: isOn)"))
        XCTAssertTrue(source.contains(".labelsHidden()"))
        XCTAssertTrue(source.contains("private func settingsString(_ key: RuneLocalizedStringKey) -> String"))
    }

    private var resourceLogsInspectorViewPath: String {
        let current = URL(fileURLWithPath: #filePath)
        return current
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/ResourceLogsInspectorView.swift")
            .path
    }

    private var resourceYAMLInspectorViewPath: String {
        let current = URL(fileURLWithPath: #filePath)
        return current
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/ResourceYAMLInspectorView.swift")
            .path
    }

    private var resourceDescribeInspectorViewPath: String {
        let current = URL(fileURLWithPath: #filePath)
        return current
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/ResourceDescribeInspectorView.swift")
            .path
    }

    private var resourceManifestPresentationPath: String {
        let current = URL(fileURLWithPath: #filePath)
        return current
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/ResourceManifestPresentation.swift")
            .path
    }

    private var runeRootViewPath: String {
        let current = URL(fileURLWithPath: #filePath)
        return current
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift")
            .path
    }

    private var kubeConfigPickerPath: String {
        let current = URL(fileURLWithPath: #filePath)
        return current
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Services/KubeConfigPicker.swift")
            .path
    }
}
