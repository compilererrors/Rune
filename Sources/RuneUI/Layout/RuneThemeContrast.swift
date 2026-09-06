import AppKit
import SwiftUI

/// WCAG relative luminance in linear sRGB, including alpha compositing.
enum RuneThemeContrast {
    static let textMinimum: Double = 4.5

    struct RGB: Hashable {
        let r: Double
        let g: Double
        let b: Double
        let a: Double

        init(_ color: NSColor) {
            let color = color.usingColorSpace(.sRGB) ?? .black
            r = color.redComponent
            g = color.greenComponent
            b = color.blueComponent
            a = color.alphaComponent
        }

        init(r: Double, g: Double, b: Double, a: Double = 1) {
            self.r = r; self.g = g; self.b = b; self.a = a
        }

        static let black = RGB(r: 0, g: 0, b: 0)
        static let white = RGB(r: 1, g: 1, b: 1)
        var nsColor: NSColor { NSColor(srgbRed: r, green: g, blue: b, alpha: a) }
        var color: Color { Color(nsColor: nsColor) }
        var luminance: Double {
            func linear(_ channel: Double) -> Double {
                channel <= 0.04045 ? channel / 12.92 : pow((channel + 0.055) / 1.055, 2.4)
            }
            return 0.2126 * linear(r) + 0.7152 * linear(g) + 0.0722 * linear(b)
        }

        func over(_ background: RGB, opacity: Double = 1) -> RGB {
            let alpha = a * opacity
            return RGB(
                r: r * alpha + background.r * (1 - alpha),
                g: g * alpha + background.g * (1 - alpha),
                b: b * alpha + background.b * (1 - alpha)
            )
        }

        func mixed(with target: RGB, fraction: Double) -> RGB {
            RGB(r: r + (target.r - r) * fraction,
                g: g + (target.g - g) * fraction,
                b: b + (target.b - b) * fraction)
        }

        func contrast(over background: RGB) -> Double {
            let foregroundLuminance = over(background).luminance
            return (max(foregroundLuminance, background.luminance) + 0.05)
                / (min(foregroundLuminance, background.luminance) + 0.05)
        }
    }

    static func ratio(_ foreground: NSColor, over background: NSColor) -> Double {
        RGB(foreground).contrast(over: RGB(background))
    }

    static func readable(_ color: NSColor, over backgrounds: [NSColor], minimum: Double = textMinimum) -> NSColor {
        adjusted(RGB(color), backgrounds: backgrounds.map(RGB.init), minimum: minimum).nsColor
    }

    private struct NativeInkInput: Hashable {
        let source: RGB
        let backgrounds: [RGB]
        let isDark: Bool
    }

    /// Cache actual resolved color inputs, so appearance, accessibility and accent
    /// changes still invalidate the result. Never keep an unbounded palette history.
    private final class NativeInkCache: @unchecked Sendable {
        private let lock = NSLock()
        private var dynamicColors: [ObjectIdentifier: NSColor] = [:]
        private var resolvedColors: [NativeInkInput: NSColor] = [:]

        func dynamicColor(for source: NSColor) -> NSColor {
            // Avoid invoking a dynamic NSColor's hash/equality while holding the
            // lock. The cached provider retains its source, preventing ID reuse.
            let key = ObjectIdentifier(source)
            if let cached = lock.withLock({ dynamicColors[key] }) { return cached }
            let color = NSColor(name: nil, dynamicProvider: { appearance in
                var result = source
                appearance.performAsCurrentDrawingAppearance {
                    result = Self.resolve(source, appearance: appearance)
                }
                return result
            })
            return lock.withLock {
                if let cached = dynamicColors[key] { return cached }
                if dynamicColors.count >= 32 { dynamicColors.removeAll(keepingCapacity: true) }
                dynamicColors[key] = color
                return color
            }
        }

        private static func resolve(_ source: NSColor, appearance: NSAppearance) -> NSColor {
            let input = NativeInkInput(
                source: RGB(source),
                backgrounds: [NSColor.windowBackgroundColor, .controlBackgroundColor, .textBackgroundColor].map(RGB.init),
                isDark: appearance.bestMatch(from: [.darkAqua, .aqua]) == .darkAqua
            )
            return nativeInkCache.resolvedColor(for: input)
        }

        private func resolvedColor(for input: NativeInkInput) -> NSColor {
            if let cached = lock.withLock({ resolvedColors[input] }) { return cached }
            let ink: RGB = input.isDark ? .white : .black
            let backgrounds = input.backgrounds + input.backgrounds.map { ink.over($0, opacity: 0.24) }
            let color = adjusted(input.source, backgrounds: backgrounds, minimum: textMinimum).nsColor
            return lock.withLock {
                if resolvedColors.count >= 128 { resolvedColors.removeAll(keepingCapacity: true) }
                resolvedColors[input] = color
                return color
            }
        }
    }

    private static let nativeInkCache = NativeInkCache()

    static func nativeInk(_ source: NSColor) -> NSColor {
        nativeInkCache.dynamicColor(for: source)
    }

    static func onFill(_ fill: Color) -> Color {
        let background = RGB(NSColor(fill))
        return (RGB.black.contrast(over: background) >= RGB.white.contrast(over: background) ? RGB.black : RGB.white).color
    }

    private static func adjusted(_ color: RGB, backgrounds: [RGB], minimum: Double, tintOpacity: Double = 0) -> RGB {
        func score(_ candidate: RGB) -> Double {
            backgrounds.map { candidate.contrast(over: candidate.over($0, opacity: tintOpacity)) }.min() ?? 21
        }
        // Leave already-readable theme colors intact, including their alpha.
        guard score(color) < minimum else { return color }
        let target: RGB = score(.white) >= score(.black) ? .white : .black
        // Raise brightness at constant hue and saturation first. Mixing straight
        // toward white turns blue, green and gold accents into pale pastels.
        // Only desaturate once the brightest channel has reached the gamut edge.
        let maximum = max(color.r, color.g, color.b)
        let saturated = maximum > 0
            ? RGB(r: color.r / maximum, g: color.g / maximum, b: color.b / maximum)
            : RGB.white
        let brighter = target == .white
        let destination = brighter && score(saturated) >= minimum + 0.04 ? saturated : target
        let start = brighter && destination == .white ? saturated : RGB(r: color.r, g: color.g, b: color.b)
        var lower = 0.0
        var upper = 1.0
        for _ in 0..<20 {
            let fraction = (lower + upper) / 2
            if score(start.mixed(with: destination, fraction: fraction)) >= minimum + 0.04 {
                upper = fraction
            } else {
                lower = fraction
            }
        }
        return start.mixed(with: destination, fraction: upper)
    }

    static func normalized(_ source: RuneThemePalette, scheme: ColorScheme) -> RuneThemePalette {
        let ink: RGB = scheme == .dark ? .white : .black
        let canvas: RGB = scheme == .dark ? .black : .white
        func surface(_ color: Color) -> RGB {
            let color = RGB(NSColor(color)).over(canvas)
            // A shared text palette requires a coherent family of surfaces.
            // Reserve headroom for selection fills and status chips, adjusting
            // only backgrounds that would otherwise make readable text impossible.
            guard ink.contrast(over: color) < 10 else { return color }
            var lower = 0.0
            var upper = 1.0
            for _ in 0..<20 {
                let fraction = (lower + upper) / 2
                if ink.contrast(over: color.mixed(with: canvas, fraction: fraction)) >= 10.05 { upper = fraction }
                else { lower = fraction }
            }
            return color.mixed(with: canvas, fraction: upper)
        }
        let surfaces = [source.window, source.sidebar, source.content, source.panel,
                        source.inset, source.editor, source.row, source.rowSelected].map(surface)
        func semantic(_ color: Color) -> RGB {
            // Status chips use up to 16% tint. Toolbar states resolve their ink
            // separately against their actual fill, without altering the theme.
            adjusted(RGB(NSColor(color)), backgrounds: surfaces, minimum: textMinimum, tintOpacity: 0.16)
        }
        let accent = adjusted(RGB(NSColor(source.accent)), backgrounds: surfaces,
                              minimum: textMinimum, tintOpacity: 0.24)
        let success = semantic(source.success)
        let warning = semantic(source.warning)
        let danger = semantic(source.danger)
        let info = semantic(source.info)
        let textSurfaces = surfaces + surfaces.flatMap { background in
            [accent.over(background, opacity: 0.24), ink.over(background, opacity: 0.14)]
                + [success, warning, danger, info].map { $0.over(background, opacity: 0.16) }
        }
        func text(_ color: Color) -> Color {
            adjusted(RGB(NSColor(color)), backgrounds: textSurfaces, minimum: textMinimum).color
        }
        return RuneThemePalette(
            window: surfaces[0].color, sidebar: surfaces[1].color,
            content: surfaces[2].color, panel: surfaces[3].color,
            inset: surfaces[4].color, editor: surfaces[5].color,
            row: surfaces[6].color, rowSelected: surfaces[7].color,
            stroke: source.stroke, accent: accent.color,
            foreground: text(source.foreground), secondaryText: text(source.secondaryText), mutedText: text(source.mutedText),
            success: success.color, warning: warning.color, danger: danger.color, info: info.color,
            authoredAccent: source.accentFill
        )
    }

    static func syntax(_ source: RuneThemeSyntaxPalette, palette: RuneThemePalette) -> RuneThemeSyntaxPalette {
        let editor = RGB(NSColor(palette.editor))
        let backgrounds = [editor, RGB(NSColor(palette.accent)).over(editor, opacity: 0.24),
                           RGB(NSColor(palette.warning)).over(editor, opacity: 0.18)]
        func token(_ hex: String) -> String {
            let corrected = adjusted(RGB(.runeHex(hex)), backgrounds: backgrounds, minimum: textMinimum)
            return RuneThemeColorParser.rgbaHex(corrected.nsColor) ?? hex
        }
        return RuneThemeSyntaxPalette(key: token(source.key), string: token(source.string), number: token(source.number),
                                      boolean: token(source.boolean), comment: token(source.comment),
                                      directive: token(source.directive), anchor: token(source.anchor))
    }
}

/// Semantic text styles follow Rune's palette instead of system label colors
/// that may have been designed for a different background.
enum RuneTextStyle: ShapeStyle {
    case primary, secondary, tertiary, success, warning, danger, info, accent

    func resolve(in environment: EnvironmentValues) -> Color {
        if let palette = environment.runeThemePalette {
            switch self {
            case .primary: return palette.foreground
            case .secondary: return palette.secondaryText
            case .tertiary: return palette.mutedText
            case .success: return palette.success
            case .warning: return palette.warning
            case .danger: return palette.danger
            case .info: return palette.info
            case .accent: return palette.accent
            }
        }
        let color: NSColor
        switch self {
        case .primary: color = .labelColor
        case .secondary: color = .secondaryLabelColor
        case .tertiary: color = .tertiaryLabelColor
        case .success: color = .systemGreen
        case .warning: color = .systemOrange
        case .danger: color = .systemRed
        case .info: color = .systemBlue
        case .accent: color = .controlAccentColor
        }
        var result = Color(nsColor: color)
        NSAppearance(named: environment.colorScheme == .dark ? .darkAqua : .aqua)?.performAsCurrentDrawingAppearance {
            result = Color(nsColor: RuneThemeContrast.nativeInk(color).usingColorSpace(.sRGB) ?? color)
        }
        return result
    }
}

extension ShapeStyle where Self == RuneTextStyle {
    static var runePrimary: RuneTextStyle { .primary }
    static var runeSecondary: RuneTextStyle { .secondary }
    static var runeTertiary: RuneTextStyle { .tertiary }
    static var runeSuccess: RuneTextStyle { .success }
    static var runeWarning: RuneTextStyle { .warning }
    static var runeDanger: RuneTextStyle { .danger }
    static var runeInfo: RuneTextStyle { .info }
    static var runeAccent: RuneTextStyle { .accent }
}
