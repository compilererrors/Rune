import AppKit
import RuneCore
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RuneThemeContrastTests: XCTestCase {
    func testCachedNativeInkFollowsAppearanceAndReturnsToTheOriginalColor() throws {
        let source = NSColor(srgbRed: 0.4, green: 0.5, blue: 0.6, alpha: 0.15)
        let ink = RuneThemeContrast.nativeInk(source)
        var colors: [String] = []
        for name in [NSAppearance.Name.aqua, .darkAqua, .aqua] {
            let appearance = try XCTUnwrap(NSAppearance(named: name))
            var capturedColor: NSColor?
            appearance.performAsCurrentDrawingAppearance {
                guard let resolved = ink.usingColorSpace(.sRGB) else { return }
                capturedColor = resolved
                for background in [NSColor.windowBackgroundColor, .controlBackgroundColor, .textBackgroundColor] {
                    XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(resolved, over: background), 4.5)
                }
            }
            colors.append(try XCTUnwrap(RuneThemeColorParser.rgbaHex(try XCTUnwrap(capturedColor))))
        }
        XCTAssertNotEqual(colors[0], colors[1])
        XCTAssertEqual(colors[0], colors[2])
    }

    func testLinearLuminanceAndAlphaCompositing() {
        XCTAssertEqual(RuneThemeContrast.ratio(.black, over: .white), 21, accuracy: 0.0001)
        XCTAssertEqual(RuneThemeContrast.ratio(.runeHex("#777777"), over: .white), 4.4781, accuracy: 0.001)
        XCTAssertEqual(RuneThemeContrast.ratio(.black.withAlphaComponent(0.5), over: .white), 3.97665, accuracy: 0.001)
        XCTAssertEqual(RuneThemeContrast.ratio(.clear, over: .white), 1, accuracy: 0.0001)
    }

    func testEveryBuiltInThemeKeepsTextReadableOnSurfacesAndControlStates() throws {
        for appearance in RuneAppearanceTheme.allCases where appearance != .native {
            let theme = appearance.resolvedTheme
            try assertContrast(theme)
            let palette = try XCTUnwrap(theme.palette)
            let again = RuneThemeContrast.normalized(palette, scheme: try XCTUnwrap(theme.preferredColorScheme))
            for (before, after) in zip(colors(palette), colors(again)) {
                XCTAssertEqual(RuneThemeColorParser.rgbaHex(NSColor(before)), RuneThemeColorParser.rgbaHex(NSColor(after)), "Normalization must be stable: \(theme.id)")
            }
        }
    }

    func testAllThemeAccentsKeepTheirAuthoredColorForFillsAndSelections() throws {
        for appearance in RuneAppearanceTheme.allCases where appearance != .native {
            let source = try XCTUnwrap(appearance.palette)
            let theme = appearance.resolvedTheme
            let palette = try XCTUnwrap(theme.palette)
            let expected = RuneThemeColorParser.rgbaHex(NSColor(source.accent))
            XCTAssertEqual(RuneThemeColorParser.rgbaHex(NSColor(palette.accentFill)), expected, appearance.title)
            XCTAssertEqual(RuneThemeColorParser.rgbaHex(.runeHex(try XCTUnwrap(theme.appKitPalette).accent)), expected, appearance.title)
            let again = RuneThemeContrast.normalized(palette, scheme: try XCTUnwrap(theme.preferredColorScheme))
            XCTAssertEqual(RuneThemeColorParser.rgbaHex(NSColor(again.accentFill)), expected, appearance.title)
        }
    }

    func testContrastCorrectionKeepsHueAndSaturationWhenBrightnessIsEnough() throws {
        for (hex, backdrop) in [("#28557a", "#10151c"), ("#dba82a", "#ffffff")] {
            let source = try XCTUnwrap(NSColor.runeHex(hex).usingColorSpace(.sRGB))
            let background = NSColor.runeHex(backdrop)
            XCTAssertLessThan(RuneThemeContrast.ratio(source, over: background), 4.5)
            let corrected = try XCTUnwrap(RuneThemeContrast.readable(source, over: [background]).usingColorSpace(.sRGB))
            XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(corrected, over: background), 4.5)
            XCTAssertEqual(corrected.hueComponent, source.hueComponent, accuracy: 0.001)
            XCTAssertEqual(corrected.saturationComponent, source.saturationComponent, accuracy: 0.001)
        }
    }

    func testUnreadableImportedThemesAreCorrectedWithoutChangingTheirIdentity() throws {
        for scheme in ["light", "dark"] {
            let data = """
            {"name":"Synthetic contrast fixture","themes":[{"name":"Low Contrast","appearance":"\(scheme)","style":{
              "background":"#80808080","panel.background":"#ffffff","editor.background":"#000000",
              "text":"#80808010","text.muted":"#88888810","text.accent":"#888888",
              "success":"#808080","warning":"#808080","error":"#808080","info":"#808080",
              "syntax":{"property":{"color":"#00000001"},"comment":{"color":"#ffffff01"}}
            }}]}
            """.data(using: .utf8)!
            let theme = try XCTUnwrap(RuneZedThemeDecoder.decode(data: data, sourceID: "synthetic").first)
            XCTAssertEqual(theme.id, "zed:synthetic:low-contrast")
            XCTAssertEqual(theme.preferredColorScheme, scheme == "dark" ? .dark : .light)
            try assertContrast(theme)
        }
    }

    func testNativeTextStylesInBothSystemAppearances() throws {
        for scheme in [ColorScheme.light, .dark] {
            let appearance = try XCTUnwrap(NSAppearance(named: scheme == .dark ? .darkAqua : .aqua))
            appearance.performAsCurrentDrawingAppearance {
                var environment = EnvironmentValues()
                environment.colorScheme = scheme
                for role in [RuneTextStyle.primary, .secondary, .tertiary, .success, .warning, .danger, .info, .accent] {
                    let color = NSColor(role.resolve(in: environment))
                    for background in [NSColor.windowBackgroundColor, .controlBackgroundColor, .textBackgroundColor] {
                        XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(color, over: background), 4.5, "Native \(scheme), \(role)")
                    }
                }
            }
        }
    }

    func testANSIColorsAndResourceCellsAcrossAllThemes() throws {
        let source = (Array(30...37) + Array(90...97) + [39, 0]).map { "\u{1B}[\($0)mSample " }.joined()
        for appearance in RuneAppearanceTheme.allCases where appearance != .native {
            let theme = appearance.resolvedTheme
            let palette = try XCTUnwrap(theme.palette)
            let editor = NSColor(palette.editor)
            let attributed = ResourceLogANSIFormatter.attributedString(from: source, font: .monospacedSystemFont(ofSize: 12, weight: .regular), foreground: NSColor(palette.foreground), backgrounds: [editor])
            XCTAssertEqual(attributed.string, String(repeating: "Sample ", count: 18))
            attributed.enumerateAttribute(.foregroundColor, in: NSRange(location: 0, length: attributed.length)) { value, _, _ in
                XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(value as! NSColor, over: editor), 4.5, "ANSI \(theme.id)")
            }
            let row = RuneThemeContrast.RGB(NSColor(palette.row))
            let selected = RuneThemeContrast.RGB(NSColor(palette.accentFill)).over(row, opacity: Double(theme.appKitPalette!.selectedAlpha))
            for requested in [NSColor.labelColor, .secondaryLabelColor, .systemGreen, .systemOrange, .systemRed, .systemBlue, .systemYellow] {
                for tint in [0.0, 0.12] {
                    let color = runeResourceCellColor(requested, theme: theme, tintOpacity: tint)
                    for base in [row, selected] {
                        let background = RuneThemeContrast.RGB(requested).over(base, opacity: tint).nsColor
                        XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(color, over: background), 4.5, "Resource cell \(theme.id)")
                    }
                }
            }
        }
    }

    func testRenderedResourceCellsEditorsAndSettingsControlsAcrossThemes() throws {
        for appearance in RuneAppearanceTheme.allCases where appearance != .native {
            let theme = appearance.resolvedTheme
            let palette = try XCTUnwrap(theme.palette)
            let host = NSHostingView(rootView: ContrastPreview().runeAppearanceTheme(theme))
            host.frame = NSRect(x: 0, y: 0, width: 960, height: 860)
            let window = NSWindow(contentRect: host.frame, styleMask: [.borderless], backing: .buffered, defer: false)
            window.isReleasedWhenClosed = false
            window.contentView = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }
            for _ in 0..<3 {
                host.layoutSubtreeIfNeeded()
                RunLoop.main.run(until: Date().addingTimeInterval(0.02))
            }
            let fields = descendants(host).compactMap { $0 as? NSTextField }.filter { !$0.stringValue.isEmpty }
            XCTAssertGreaterThan(fields.count, 10, "Expected real AppKit resource cells")
            for field in fields {
                var parent = field.superview
                var row: NSTableRowView?
                while let view = parent {
                    if let tableRow = view as? NSTableRowView { row = tableRow; break }
                    parent = view.superview
                }
                guard let row else { continue }
                var background = RuneThemeContrast.RGB(NSColor(palette.row))
                if row.isSelected { background = RuneThemeContrast.RGB(NSColor(palette.accentFill)).over(background, opacity: Double(theme.appKitPalette!.selectedAlpha)) }
                if let fill = field.superview?.layer?.backgroundColor, let color = NSColor(cgColor: fill) {
                    background = RuneThemeContrast.RGB(color).over(background)
                }
                XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(try XCTUnwrap(field.textColor), over: background.nsColor), 4.5, "Rendered \(appearance): \(field.stringValue)")
            }
            let editors = descendants(host).compactMap { $0 as? NSTextView }.filter { !$0.string.isEmpty }
            XCTAssertFalse(editors.isEmpty)
            for editor in editors {
                editor.textStorage?.enumerateAttribute(.foregroundColor, in: NSRange(location: 0, length: editor.string.utf16.count)) { value, _, _ in
                    if let color = value as? NSColor {
                        XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(color, over: NSColor(palette.editor)), 4.5, "Rendered editor \(appearance)")
                    }
                }
            }
            if let directory = ProcessInfo.processInfo.environment["RUNE_CONTRAST_SNAPSHOT_DIR"] {
                let url = URL(fileURLWithPath: directory, isDirectory: true)
                try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
                // Capture this test window alone. AppKit's cacheDisplay omits
                // some SwiftUI layers and cannot verify the control colors.
                let capture = Process()
                capture.executableURL = URL(fileURLWithPath: "/usr/sbin/screencapture")
                capture.arguments = ["-l\(window.windowNumber)", "-o", "-x", url.appendingPathComponent("contrast-\(appearance.rawValue).png").path]
                try capture.run()
                capture.waitUntilExit()
                XCTAssertEqual(capture.terminationStatus, 0)
            }
        }
    }

    func testNativeResourceRowsKeepContrastOverDifferentWindowBackdrops() throws {
        let application = NSApplication.shared
        let previousAppearance = application.appearance
        defer { application.appearance = previousAppearance }
        for name in [NSAppearance.Name.aqua, .darkAqua] {
            let appearance = try XCTUnwrap(NSAppearance(named: name))
            // Native mode follows the app/system appearance and intentionally
            // clears per-window overrides when its hosting view is attached.
            application.appearance = appearance
            let host = NSHostingView(rootView: ContrastPreview().runeAppearanceTheme(RuneAppearanceTheme.native.resolvedTheme))
            host.frame = NSRect(x: 0, y: 0, width: 960, height: 860)
            let window = NSWindow(contentRect: host.frame, styleMask: [.borderless], backing: .buffered, defer: false)
            window.isReleasedWhenClosed = false
            window.contentView = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }
            for _ in 0..<3 {
                host.layoutSubtreeIfNeeded()
                RunLoop.main.run(until: Date().addingTimeInterval(0.03))
            }
            let rows = descendants(host).compactMap { $0 as? NSTableRowView }.filter { !$0.subviews.isEmpty }
            XCTAssertGreaterThanOrEqual(rows.count, 4)
            XCTAssertTrue(rows.contains { $0.isSelected })
            for row in rows {
                XCTAssertEqual(row.effectiveAppearance.bestMatch(from: [.aqua, .darkAqua]), name)
                let bitmap = try XCTUnwrap(NSBitmapImageRep(
                    bitmapDataPlanes: nil, pixelsWide: Int(row.bounds.width), pixelsHigh: Int(row.bounds.height),
                    bitsPerSample: 8, samplesPerPixel: 4, hasAlpha: true, isPlanar: false,
                    colorSpaceName: .deviceRGB, bytesPerRow: 0, bitsPerPixel: 0
                ))
                var samples: [NSColor] = []
                for backdrop in [NSColor.black, .white] {
                    let context = try XCTUnwrap(NSGraphicsContext(bitmapImageRep: bitmap))
                    NSGraphicsContext.saveGraphicsState()
                    NSGraphicsContext.current = context
                    appearance.performAsCurrentDrawingAppearance {
                        backdrop.setFill()
                        row.bounds.fill()
                        row.drawBackground(in: row.bounds)
                    }
                    context.flushGraphics()
                    NSGraphicsContext.restoreGraphicsState()
                    samples.append(try XCTUnwrap(bitmap.colorAt(x: bitmap.pixelsWide / 2, y: bitmap.pixelsHigh / 2)))
                }
                XCTAssertEqual(RuneThemeColorParser.rgbaHex(samples[0]), RuneThemeColorParser.rgbaHex(samples[1]), "Window backdrop must not alter \(name) row contrast")
                for field in descendants(row).compactMap({ $0 as? NSTextField }).filter({ !$0.stringValue.isEmpty }) {
                    var background = RuneThemeContrast.RGB(samples[0])
                    if let fill = field.superview?.layer?.backgroundColor, let tint = NSColor(cgColor: fill) {
                        background = RuneThemeContrast.RGB(tint).over(background)
                    }
                    let foreground = try XCTUnwrap(field.textColor)
                    appearance.performAsCurrentDrawingAppearance {
                        XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(foreground, over: background.nsColor), 4.5, "Native \(name): \(field.stringValue), foreground=\(RuneThemeColorParser.rgbaHex(foreground) ?? "unknown"), background=\(RuneThemeColorParser.rgbaHex(background.nsColor) ?? "unknown"), effective=\(row.effectiveAppearance.name)")
                    }
                }
            }
        }
    }

    func testAlreadyVisibleResourceCellsRefreshWhenThemeAppearanceStaysLight() throws {
        let host = NSHostingView(rootView: ContrastPreview().runeAppearanceTheme(RuneAppearanceTheme.paper.resolvedTheme))
        host.frame = NSRect(x: 0, y: 0, width: 960, height: 860)
        let window = NSWindow(contentRect: host.frame, styleMask: [.borderless], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }
        for appearance in [RuneAppearanceTheme.paper, .daylight] {
            let theme = appearance.resolvedTheme
            host.rootView = ContrastPreview().runeAppearanceTheme(theme)
            for _ in 0..<3 {
                host.layoutSubtreeIfNeeded()
                RunLoop.main.run(until: Date().addingTimeInterval(0.03))
            }
            let label = try XCTUnwrap(descendants(host).compactMap { $0 as? NSTextField }.first { $0.stringValue == "sample-0" })
            let expected = runeResourceCellColor(.labelColor, theme: theme)
            XCTAssertEqual(RuneThemeColorParser.rgbaHex(try XCTUnwrap(label.textColor)), RuneThemeColorParser.rgbaHex(expected), "Visible cell must follow \(appearance)")
        }
    }

    private func descendants(_ view: NSView) -> [NSView] {
        view.subviews.flatMap { [$0] + descendants($0) }
    }

    private func assertContrast(_ theme: RuneResolvedTheme, file: StaticString = #filePath, line: UInt = #line) throws {
        let palette = try XCTUnwrap(theme.palette)
        let surfaces = [palette.window, palette.sidebar, palette.content, palette.panel, palette.inset, palette.editor, palette.row, palette.rowSelected].map { RuneThemeContrast.RGB(NSColor($0)) }
        let semantic = [palette.accent, palette.success, palette.warning, palette.danger, palette.info].map { RuneThemeContrast.RGB(NSColor($0)) }
        let text = [palette.foreground, palette.secondaryText, palette.mutedText]
        for surface in surfaces {
            let states = [surface, RuneThemeContrast.RGB(NSColor(palette.accent)).over(surface, opacity: 0.24), RuneThemeContrast.RGB(NSColor(palette.foreground)).over(surface, opacity: 0.14)]
            for foreground in text {
                for background in states {
                    XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(NSColor(foreground), over: background.nsColor), 4.5, "Text \(theme.id)", file: file, line: line)
                }
            }
            for color in semantic {
                XCTAssertGreaterThanOrEqual(color.contrast(over: color.over(surface, opacity: 0.16)), 4.5, "Status chip \(theme.id)", file: file, line: line)
            }
            XCTAssertGreaterThanOrEqual(semantic[0].contrast(over: semantic[0].over(surface, opacity: 0.24)), 4.5, "Accent text \(theme.id)", file: file, line: line)
        }
        let editor = RuneThemeContrast.RGB(NSColor(palette.editor))
        let syntax = try XCTUnwrap(theme.syntaxPalette)
        for token in [syntax.key, syntax.string, syntax.number, syntax.boolean, syntax.comment, syntax.directive, syntax.anchor] {
            for background in [editor, semantic[0].over(editor, opacity: 0.24), semantic[2].over(editor, opacity: 0.18)] {
                XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(.runeHex(token), over: background.nsColor), 4.5, "Syntax \(theme.id): \(token)", file: file, line: line)
            }
        }
        for fill in semantic {
            XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(NSColor(RuneThemeContrast.onFill(fill.color)), over: fill.nsColor), 4.5, "Prominent button \(theme.id)", file: file, line: line)
        }
    }

    private func colors(_ p: RuneThemePalette) -> [Color] {
        [p.window, p.sidebar, p.content, p.panel, p.inset, p.editor, p.row, p.rowSelected, p.foreground, p.secondaryText, p.mutedText, p.accent, p.success, p.warning, p.danger, p.info]
    }
}

private struct ContrastPreview: View {
    @Environment(\.runeResolvedTheme) private var theme
    private let pods = ["Running", "Pending", "Failed", "Succeeded"].enumerated().map {
        PodSummary(name: "sample-\($0.offset)", namespace: "demo", status: $0.element)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Settings and resource contrast").font(.title2.bold())
            HStack(spacing: 8) {
                Button("Reload") {}.buttonStyle(RuneToolbarButtonStyle())
                Button("Save Logs") {}.buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                Button("Unavailable") {}.buttonStyle(RuneToolbarButtonStyle()).disabled(true)
                RuneToolbarMenu { Button("Name") {} } label: { Label("Sort", systemImage: "arrow.up.arrow.down") }
            }
            HStack {
                VStack(alignment: .leading) {
                    Text("Default export folder")
                    Text("Save files with one click").foregroundStyle(.runeSecondary)
                }
                Spacer()
                RuneToolbarMenu(fillsWidth: true) { Button("Choose Folder") {} } label: {
                    RuneSettingsMenuLabel(title: "Exports", systemImage: "folder", subtitle: "Default destination")
                }.frame(width: 260)
            }.runeInsetCard(padding: 14)
            HStack(spacing: 12) {
                RuneThemeSelectorCard(theme: theme, isSelected: true) {}
                RuneThemeSelectorCard(
                    theme: (theme.preferredColorScheme == .dark ? RuneAppearanceTheme.paper : .emberGlass).resolvedTheme,
                    isSelected: false
                ) {}
            }
            AppKitPodTableView(
                pods: pods, selectedPodID: pods[1].id, selectedPodIDs: [], sortColumn: .name, sortAscending: true,
                nameColumnWidth: 280, canApplyClusterMutations: false, isFavorite: { _ in false },
                onSelectPod: { _ in }, onToggleBulkSelection: { _ in }, onToggleSort: { _ in },
                onNameColumnWidthChanged: { _ in }, onToggleFavorite: { _ in }, onOpenLogs: { _ in },
                onOpenExec: { _ in }, onOpenDescribe: { _ in }, onOpenYAML: { _ in }, onDelete: { _ in }
            ).frame(height: 180)
            InspectorReadOnlyTextSurface(text: "# Configuration comment\nreplicas: 3\nenabled: true\nname: \"sample\"", minHeight: 140, resetID: "sample", contentStyle: .yaml)
            HStack {
                Label("Healthy", systemImage: "checkmark.circle").foregroundStyle(.runeSuccess)
                Label("Warning", systemImage: "exclamationmark.triangle").foregroundStyle(.runeWarning)
                Label("Failed", systemImage: "xmark.circle").foregroundStyle(.runeDanger)
                Text("Secondary information").foregroundStyle(.runeSecondary)
                Text("Metadata").foregroundStyle(.runeTertiary)
            }
        }.padding(20)
    }
}
