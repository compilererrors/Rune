import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class RuneThemeWorkflowTests: XCTestCase {
    func testThemeTemplateWritesLoadableThemeWithoutOverwritingEdits() throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let templateURL = RuneThemeCatalog.writeThemeTemplate(in: directory)
        let themes = RuneThemeCatalog.loadZedThemes(from: directory)

        XCTAssertEqual(templateURL.lastPathComponent, "rune-theme-template.json")
        XCTAssertEqual(themes.map(\.title), ["Custom Dark"])
        XCTAssertEqual(themes.first?.sourceSummary, "Custom theme")

        let editedTemplate = """
        {
          "name": "Edited",
          "themes": [
            {
              "name": "Edited Theme",
              "appearance": "dark",
              "style": {
                "background": "#101010ff",
                "text": "#f5f5f5ff",
                "text.accent": "#55aaffff"
              }
            }
          ]
        }
        """
        try editedTemplate.write(to: templateURL, atomically: true, encoding: .utf8)

        XCTAssertEqual(RuneThemeCatalog.writeThemeTemplate(in: directory), templateURL)
        let reloadedThemes = RuneThemeCatalog.loadZedThemes(from: directory)
        XCTAssertEqual(reloadedThemes.map(\.title), ["Edited Theme"])
    }

    func testThemeCatalogLoadsCompatibleThemeFilesAndFiltersInvalidInputs() throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        try writeThemeFamily(
            fileName: "alpha themes.json",
            familyName: "Synthetic Alpha",
            themes: [
                themeJSON(name: "Midnight Workflow", appearance: "dark", accent: "#67b7ffff"),
                themeJSON(name: "Day Workflow", appearance: "light", accent: "#1f73caff")
            ],
            to: directory
        )
        try writeThemeFamily(
            fileName: "zeta themes.json",
            familyName: "Synthetic Zeta",
            themes: [
                themeJSON(name: "Zeta Console", appearance: "dark", accent: "#b891ffff")
            ],
            to: directory
        )
        try "{ invalid json".write(to: directory.appendingPathComponent("broken.json"), atomically: true, encoding: .utf8)
        try writeThemeFamily(
            fileName: ".hidden.json",
            familyName: "Hidden",
            themes: [themeJSON(name: "Hidden Theme", appearance: "dark", accent: "#ff0000ff")],
            to: directory
        )
        try "not a theme".write(to: directory.appendingPathComponent("notes.txt"), atomically: true, encoding: .utf8)
        try String(repeating: "x", count: 1_500_001)
            .write(to: directory.appendingPathComponent("oversized.json"), atomically: true, encoding: .utf8)

        let themes = RuneThemeCatalog.loadZedThemes(from: directory)

        XCTAssertEqual(themes.map(\.id), [
            "zed:alpha-themes:midnight-workflow",
            "zed:alpha-themes:day-workflow",
            "zed:zeta-themes:zeta-console"
        ])
        XCTAssertEqual(themes.map(\.sourceSummary), Array(repeating: "Custom theme", count: 3))
        XCTAssertEqual(themes[0].preferredColorScheme, .dark)
        XCTAssertEqual(themes[1].preferredColorScheme, .light)
        XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(.runeHex(try XCTUnwrap(themes[0].appKitPalette?.accent)), over: NSColor(try XCTUnwrap(themes[0].palette).panel)), 4.5)
        XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(.runeHex(try XCTUnwrap(themes[1].syntaxPalette?.key)), over: NSColor(try XCTUnwrap(themes[1].palette).editor)), 4.5)
        XCTAssertNil(themes.first?.builtin)
    }

    func testThemeCatalogSanitizesTitlesAndDisambiguatesDuplicateThemeIDs() throws {
        let directory = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let longName = String(repeating: "A", count: 120)
        try writeThemeFamily(
            fileName: "edge.json",
            familyName: "Synthetic Edge",
            themes: [
                themeJSON(name: "Duplicate Theme", appearance: "dark", accent: "#67b7ffff"),
                themeJSON(name: "Duplicate Theme", appearance: "light", accent: "#1f73caff"),
                themeJSON(name: "      ", appearance: "dark", accent: "#b891ffff"),
                themeJSON(name: longName, appearance: "light", accent: "#006d8fff")
            ],
            to: directory
        )

        let themes = RuneThemeCatalog.loadZedThemes(from: directory)

        XCTAssertEqual(themes.map(\.id), [
            "zed:edge:duplicate-theme",
            "zed:edge:duplicate-theme-2",
            "zed:edge:custom-theme",
            "zed:edge:\(String(repeating: "a", count: 80))"
        ])
        XCTAssertEqual(themes[2].title, "Custom Theme")
        XCTAssertEqual(themes[3].title.count, 80)
        XCTAssertEqual(RuneThemeCatalog.displayTitle("  Console\n\tTheme  "), "Console Theme")
    }

    func testThemeCatalogUserThemeCacheRefreshesWhenDirectoryChanges() throws {
        let directory = try makeTemporaryDirectory()
        defer {
            RuneThemeCatalog.reloadUserThemes()
            try? FileManager.default.removeItem(at: directory)
        }
        RuneThemeCatalog.reloadUserThemes()
        let initialDate = Date(timeIntervalSince1970: 1_000)

        try writeThemeFamily(
            fileName: "first.json",
            familyName: "First",
            themes: [themeJSON(name: "First Theme", appearance: "dark", accent: "#67b7ffff")],
            to: directory
        )

        let firstLoad = RuneThemeCatalog.userThemes(from: directory, referenceDate: initialDate)
        let secondLoad = RuneThemeCatalog.userThemes(
            from: directory,
            referenceDate: initialDate.addingTimeInterval(0.1)
        )

        XCTAssertEqual(firstLoad.map(\.id), ["zed:first:first-theme"])
        XCTAssertEqual(secondLoad.map(\.id), firstLoad.map(\.id))

        try writeThemeFamily(
            fileName: "second.json",
            familyName: "Second",
            themes: [themeJSON(name: "Second Theme", appearance: "light", accent: "#1f73caff")],
            to: directory
        )

        let cachedBeforeRefreshWindow = RuneThemeCatalog.userThemes(
            from: directory,
            referenceDate: initialDate.addingTimeInterval(0.2)
        )
        let refreshedAfterInterval = RuneThemeCatalog.userThemes(
            from: directory,
            referenceDate: initialDate.addingTimeInterval(RuneThemeCatalog.metadataRefreshInterval + 0.2)
        )

        XCTAssertEqual(cachedBeforeRefreshWindow.map(\.id), ["zed:first:first-theme"])
        XCTAssertEqual(refreshedAfterInterval.map(\.id), ["zed:first:first-theme", "zed:second:second-theme"])

        try writeThemeFamily(
            fileName: "third.json",
            familyName: "Third",
            themes: [themeJSON(name: "Third Theme", appearance: "dark", accent: "#b891ffff")],
            to: directory
        )
        RuneThemeCatalog.reloadUserThemes()

        let reloadedExplicitly = RuneThemeCatalog.userThemes(
            from: directory,
            referenceDate: initialDate.addingTimeInterval(0.3)
        )

        XCTAssertEqual(reloadedExplicitly.map(\.id), [
            "zed:first:first-theme",
            "zed:second:second-theme",
            "zed:third:third-theme"
        ])
    }

    func testThemeDecoderFallsBackForMissingAndTransparentTokens() throws {
        let data = """
        {
          "name": "Synthetic Fallback",
          "themes": [
            {
              "name": "Sparse Dark",
              "appearance": "dark",
              "style": {
                "background": "#101820ff",
                "text": "#ffffff00",
                "text.muted": "#00000000",
                "text.accent": "#6bb7f7ff",
                "syntax": {
                  "property": { "color": "#8bd3ffff" }
                }
              }
            }
          ]
        }
        """.data(using: .utf8)!

        let theme = try XCTUnwrap(RuneZedThemeDecoder.decode(data: data, sourceID: "fallback").first)

        XCTAssertEqual(theme.id, "zed:fallback:sparse-dark")
        XCTAssertEqual(theme.appKitPalette?.window, "#101820")
        XCTAssertEqual(theme.appKitPalette?.foreground, "#eef4fb")
        XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(.runeHex(try XCTUnwrap(theme.appKitPalette?.accent)), over: NSColor(try XCTUnwrap(theme.palette).panel)), 4.5)
        XCTAssertEqual(theme.syntaxPalette?.key, "#8bd3ffff")
        XCTAssertEqual(theme.syntaxPalette?.string, "#86d59aff")
        XCTAssertEqual(theme.sourceSummary, "Custom theme")
    }

    func testThemePresentationKeepsCardAndMenuSymbolsConsistent() {
        let native = RuneThemePresentation(theme: RuneAppearanceTheme.native.resolvedTheme)
        XCTAssertEqual(native.appearanceTitle, "System")
        XCTAssertEqual(native.appearanceSymbol, "circle.lefthalf.filled")
        XCTAssertEqual(native.menuSymbol, "circle.lefthalf.filled")
        XCTAssertEqual(native.title, "Native")

        let dark = RuneThemePresentation(theme: RuneAppearanceTheme.graphiteBlue.resolvedTheme)
        XCTAssertEqual(dark.appearanceTitle, "Dark")
        XCTAssertEqual(dark.appearanceSymbol, "moon.fill")
        XCTAssertEqual(dark.menuSymbol, "moon")
        XCTAssertEqual(dark.sourceSummary, "Rune theme")

        let light = RuneThemePresentation(theme: RuneAppearanceTheme.paper.resolvedTheme)
        XCTAssertEqual(light.appearanceTitle, "Light")
        XCTAssertEqual(light.appearanceSymbol, "sun.max.fill")
        XCTAssertEqual(light.menuSymbol, "sun.max")
    }

    func testThemeWindowWorkflowSwitchesContrastLightBackToNativeImmediately() async throws {
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 220, height: 140),
            styleMask: [.titled, .closable],
            backing: .buffered,
            defer: false
        )
        let host = NSHostingController(
            rootView: AnyView(themeProbe(theme: RuneAppearanceTheme.contrastLight.resolvedTheme))
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settleThemeWindow()

        XCTAssertEqual(window.appearance?.name, .aqua)
        XCTAssertEqual(window.contentView?.appearance?.name, .aqua)

        host.rootView = AnyView(themeProbe(theme: RuneAppearanceTheme.native.resolvedTheme))

        try await settleThemeWindow()

        XCTAssertNil(window.appearance)
        XCTAssertNil(window.contentView?.appearance)
    }

    func testThemeSettingsSourceKeepsCustomThemeBrandingGeneric() throws {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let preferencesSource = try String(
            contentsOf: repoRoot.appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift"),
            encoding: .utf8
        )

        XCTAssertTrue(preferencesSource.contains("Theme JSON files"))
        XCTAssertTrue(preferencesSource.contains("compatible theme files"))
        XCTAssertFalse(preferencesSource.contains("Zed JSON themes"))
        XCTAssertFalse(preferencesSource.contains("VS Code"))
    }

    private func themeProbe(theme: RuneResolvedTheme) -> some View {
        Text("Theme Probe")
            .frame(width: 220, height: 140)
            .runeAppearanceTheme(theme)
    }

    private func settleThemeWindow() async throws {
        await Task.yield()
        try await Task.sleep(nanoseconds: 70_000_000)
        await Task.yield()
    }

    private func makeTemporaryDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneThemeWorkflowTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }

    private func writeThemeFamily(
        fileName: String,
        familyName: String,
        themes: [String],
        to directory: URL
    ) throws {
        let json = """
        {
          "name": "\(familyName)",
          "themes": [
            \(themes.joined(separator: ",\n"))
          ]
        }
        """
        try json.write(to: directory.appendingPathComponent(fileName), atomically: true, encoding: .utf8)
    }

    private func themeJSON(name: String, appearance: String, accent: String) -> String {
        """
        {
          "name": "\(name)",
          "appearance": "\(appearance)",
          "style": {
            "background": "\(appearance == "dark" ? "#101820ff" : "#fbfcffff")",
            "panel.background": "\(appearance == "dark" ? "#182431ff" : "#f1f5faff")",
            "surface.background": "\(appearance == "dark" ? "#1e2c3aff" : "#eef3f8ff")",
            "element.background": "\(appearance == "dark" ? "#223244ff" : "#e4ebf5ff")",
            "element.selected": "\(appearance == "dark" ? "#294866ff" : "#c8e4ffff")",
            "border": "\(appearance == "dark" ? "#40576eff" : "#91a2b5ff")",
            "border.variant": "\(appearance == "dark" ? "#34485cff" : "#a5b2c2ff")",
            "text": "\(appearance == "dark" ? "#f4f8fbff" : "#07111fff")",
            "text.muted": "\(appearance == "dark" ? "#b6c4d1ff" : "#26394fff")",
            "text.placeholder": "\(appearance == "dark" ? "#8ea0adff" : "#52677dff")",
            "text.accent": "\(accent)",
            "editor.background": "\(appearance == "dark" ? "#0d141cff" : "#ffffffff")",
            "success": "\(appearance == "dark" ? "#7ee6a8ff" : "#006d3bff")",
            "warning": "\(appearance == "dark" ? "#ffd166ff" : "#8a5a00ff")",
            "error": "\(appearance == "dark" ? "#ff7a9aff" : "#b00020ff")",
            "info": "#8bd3ffff",
            "syntax": {
              "property": { "color": "#8bd3ffff" },
              "string": { "color": "\(appearance == "dark" ? "#7ee6a8ff" : "#006d3bff")" },
              "number": { "color": "#ffd166ff" },
              "boolean": { "color": "\(accent)" },
              "comment": { "color": "\(appearance == "dark" ? "#8ea0adff" : "#52677dff")" },
              "keyword": { "color": "\(accent)" },
              "type": { "color": "#80e6d6ff" }
            }
          }
        }
        """
    }
}
