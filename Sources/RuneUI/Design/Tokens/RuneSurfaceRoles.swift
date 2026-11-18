import AppKit
import SwiftUI

/// What a surface holds, so each kind reads the same everywhere and the
/// kinds read differently from each other (the design notes, "Surfaces"):
/// - `pane`: the sidebar, content and inspector backgrounds (palette tones).
/// - `raised`: control cards and status strips, lifted slightly off the pane.
/// - `document`: code and text documents (YAML, Describe, Logs), a deeper
///   tone with a crisp frame, so content reads as content.
/// - `terminal`: the shell transcript and prompt, the deepest tone, framed
///   like a console.
enum RuneSurfaceRole: Sendable, Equatable {
    case pane
    case raised
    case document
    case terminal
}

extension RuneThemePalette {
    private var windowRGB: RuneThemeContrast.RGB { RuneThemeContrast.RGB(NSColor(window)) }

    /// Dark palettes deepen documents and lift cards; light palettes do the
    /// opposite, so every theme keeps its own hue at each level.
    var isDarkPalette: Bool { windowRGB.luminance < 0.25 }

    var raisedSurface: Color {
        let panelRGB = RuneThemeContrast.RGB(NSColor(panel))
        if isDarkPalette {
            return RuneThemeContrast.RGB(NSColor(foreground)).over(panelRGB, opacity: 0.06).color
        }
        return RuneThemeContrast.RGB(NSColor.white).over(panelRGB, opacity: 0.55).color
    }

    var documentSurface: Color {
        let editorRGB = RuneThemeContrast.RGB(NSColor(editor))
        if isDarkPalette {
            return RuneThemeContrast.RGB(NSColor.black).over(editorRGB, opacity: 0.32).color
        }
        return RuneThemeContrast.RGB(NSColor.white).over(editorRGB, opacity: 0.6).color
    }

    var terminalSurface: Color {
        if isDarkPalette {
            return RuneThemeContrast.RGB(NSColor.black).over(windowRGB, opacity: 0.62).color
        }
        let editorRGB = RuneThemeContrast.RGB(NSColor(editor))
        return RuneThemeContrast.RGB(NSColor.black).over(editorRGB, opacity: 0.05).color
    }

    /// The frame around documents and the terminal: crisper than card edges.
    var documentFrame: Color {
        stroke.opacity(isDarkPalette ? 0.70 : 0.80)
    }
}

extension RuneSurfaceRole {
    func fill(palette: RuneThemePalette?, colorScheme: ColorScheme) -> Color {
        if let palette {
            switch self {
            case .pane: return palette.panel
            case .raised: return palette.raisedSurface
            case .document: return palette.documentSurface
            case .terminal: return palette.terminalSurface
            }
        }
        switch self {
        case .pane: return Color(nsColor: .windowBackgroundColor)
        case .raised: return Color(nsColor: .controlBackgroundColor)
        case .document: return Color(nsColor: .textBackgroundColor)
        case .terminal:
            return colorScheme == .dark ? Color(white: 0.07) : Color(nsColor: .textBackgroundColor).opacity(0.94)
        }
    }

    /// The recessed strip that a surface's tabs sit on: a step deeper than
    /// the raised bars, so the selected tab (in the surface's own fill) reads
    /// as lifted out of it.
    func tabStripFill(palette: RuneThemePalette?, colorScheme: ColorScheme) -> Color {
        let isDark = palette?.isDarkPalette ?? (colorScheme == .dark)
        let base = RuneSurfaceRole.pane.fill(palette: palette, colorScheme: colorScheme)
        let baseRGB = RuneThemeContrast.RGB(NSColor(base))
        return RuneThemeContrast.RGB(NSColor.black).over(baseRGB, opacity: isDark ? 0.22 : 0.06).color
    }

    func frame(palette: RuneThemePalette?) -> Color {
        if let palette { return palette.documentFrame }
        return Color(nsColor: .separatorColor).opacity(0.9)
    }
}
