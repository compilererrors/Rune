import AppKit
import SwiftUI

struct RuneThemePresentation {
    let title: String
    let sourceSummary: String
    let appearanceTitle: String
    let appearanceSymbol: String
    let menuSymbol: String
    let palette: RuneThemePalette

    init(theme: RuneResolvedTheme) {
        title = theme.title
        sourceSummary = theme.sourceSummary
        palette = theme.palette ?? Self.nativePreviewPalette

        switch theme.preferredColorScheme {
        case .some(.dark):
            appearanceTitle = "Dark"
            appearanceSymbol = "moon.fill"
            menuSymbol = "moon"
        case .some(.light):
            appearanceTitle = "Light"
            appearanceSymbol = "sun.max.fill"
            menuSymbol = "sun.max"
        case .none, .some:
            appearanceTitle = "System"
            appearanceSymbol = "circle.lefthalf.filled"
            menuSymbol = "circle.lefthalf.filled"
        }
    }

    private static let nativePreviewPalette = RuneThemePalette(
        window: Color(nsColor: .windowBackgroundColor),
        sidebar: Color(nsColor: .controlBackgroundColor),
        content: Color(nsColor: .windowBackgroundColor),
        panel: Color(nsColor: .controlBackgroundColor),
        inset: Color(nsColor: .controlBackgroundColor),
        editor: Color(nsColor: .textBackgroundColor),
        row: Color(nsColor: .controlBackgroundColor),
        rowSelected: Color.accentColor.opacity(0.18),
        stroke: Color(nsColor: .separatorColor),
        accent: Color.accentColor,
        foreground: Color.primary,
        secondaryText: Color.secondary,
        mutedText: Color.secondary.opacity(0.72),
        success: Color.green,
        warning: Color.orange,
        danger: Color.red,
        info: Color.blue
    )
}
