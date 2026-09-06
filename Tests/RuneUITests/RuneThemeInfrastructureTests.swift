import AppKit
import XCTest
@testable import RuneUI

@MainActor
final class RuneThemeInfrastructureTests: XCTestCase {
    func testSharedHexParserNormalizesAndPreservesAlpha() throws {
        XCTAssertEqual(RuneThemeColorParser.normalizedHex("  7EE6A8  "), "#7ee6a8")
        XCTAssertEqual(RuneThemeColorParser.normalizedHex("#7EE6A880"), "#7ee6a880")
        XCTAssertNil(RuneThemeColorParser.normalizedHex("#xyz"))
        XCTAssertNil(RuneThemeColorParser.visibleHex("#ffffff00"))

        let components = try XCTUnwrap(RuneThemeColorParser.components("#80402080"))
        XCTAssertEqual(components.red, 128.0 / 255.0, accuracy: 0.000_001)
        XCTAssertEqual(components.green, 64.0 / 255.0, accuracy: 0.000_001)
        XCTAssertEqual(components.blue, 32.0 / 255.0, accuracy: 0.000_001)
        XCTAssertEqual(components.alpha, 128.0 / 255.0, accuracy: 0.000_001)

        let roundTrip = RuneThemeColorParser.rgbaHex(NSColor.runeHex("#80402080"))
        XCTAssertEqual(roundTrip, "#80402080")
    }

    func testBuiltInAppKitAndSyntaxPalettesDeriveFromSharedTokens() throws {
        let theme = RuneAppearanceTheme.aurora.resolvedTheme
        let appKit = try XCTUnwrap(theme.appKitPalette)
        let syntax = try XCTUnwrap(theme.syntaxPalette)

        XCTAssertEqual(appKit.window, "#16131c")
        let palette = try XCTUnwrap(theme.palette)
        let pairs = [(appKit.success, palette.success), (appKit.warning, palette.warning),
                     (appKit.danger, palette.danger), (appKit.info, palette.info)]
        for (hex, color) in pairs {
            XCTAssertEqual(RuneThemeColorParser.rgbaHex(.runeHex(hex)), RuneThemeColorParser.rgbaHex(NSColor(color)))
        }
        for hex in [syntax.key, syntax.string, syntax.number] {
            XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(.runeHex(hex), over: NSColor(palette.editor)), 4.5)
        }
    }

    func testSharedSurfacesConsumeEnvironmentResolvedTheme() throws {
        let source = try sourceText("Sources/RuneUI/Layout/RuneDesignComponents.swift")
        let glass = try sourceText("Sources/RuneUI/Layout/RuneGlassShell.swift")
        let manifest = try sourceText("Sources/RuneUI/Views/AppKitManifestTextView.swift")
        let theme = try sourceText("Sources/RuneUI/Layout/RuneThemePalette.swift")

        XCTAssertTrue(theme.contains("var runeResolvedTheme: RuneResolvedTheme"))
        XCTAssertTrue(theme.contains(".environment(\\.runeResolvedTheme, theme)"))
        XCTAssertTrue(source.contains("@Environment(\\.runeResolvedTheme) private var resolvedTheme"))
        XCTAssertTrue(glass.contains("@Environment(\\.runeResolvedTheme) private var resolvedTheme"))
        XCTAssertTrue(manifest.contains("@Environment(\\.runeResolvedTheme) private var resolvedTheme"))
        XCTAssertFalse(source.contains("@AppStorage(RuneSettingsKeys.appearanceTheme)"))
        XCTAssertFalse(glass.contains("@AppStorage(RuneSettingsKeys.appearanceTheme)"))
        XCTAssertFalse(manifest.contains("@AppStorage(RuneSettingsKeys.appearanceTheme)"))
    }

    func testDefinitionOnlyDesignAPIsAreRemoved() throws {
        let source = try sourceText("Sources/RuneUI/Layout/RuneDesignComponents.swift")

        XCTAssertFalse(source.contains("struct RuneSelectionCheckboxButton"))
        XCTAssertFalse(source.contains("struct RuneInspectorSection"))
        XCTAssertFalse(source.contains("func runeEditorCard"))
        XCTAssertFalse(source.contains("func runeListRowCard"))
        XCTAssertTrue(source.contains("func runePanelCard"))
        XCTAssertTrue(source.contains("func runeInsetCard"))
    }

    func testBuiltInThemeProjectionBenchmarkKPI() {
        let iterations = 120
        let started = ContinuousClock.now
        var projectionCount = 0

        for _ in 0..<iterations {
            for appearance in RuneAppearanceTheme.allCases {
                let theme = appearance.resolvedTheme
                projectionCount += theme.appKitPalette == nil ? 0 : 1
                projectionCount += theme.syntaxPalette == nil ? 0 : 1
            }
        }

        let elapsed = ContinuousClock.now - started
        let seconds = Double(elapsed.components.seconds)
            + Double(elapsed.components.attoseconds) / 1_000_000_000_000_000_000

        XCTAssertEqual(projectionCount, iterations * (RuneAppearanceTheme.allCases.count - 1) * 2)
        XCTAssertLessThan(
            seconds,
            0.30,
            "KPI: 1,200 cached built-in theme projections should stay below 300ms in debug."
        )
    }

    private func sourceText(_ relativePath: String) throws -> String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return try String(contentsOf: repoRoot.appendingPathComponent(relativePath), encoding: .utf8)
    }
}
