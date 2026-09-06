import AppKit
import SwiftUI
import RuneSharedUI

enum RuneAppearanceTheme: String, CaseIterable, Identifiable {
    case native
    case aurora
    case graphiteBlue
    case emberGlass
    case mossTerminal
    case fjord
    case paper
    case daylight
    case contrastDark
    case contrastLight

    var id: String { rawValue }

    var title: String {
        switch self {
        case .native: return "Native"
        case .aurora: return "Aurora"
        case .graphiteBlue: return "Graphite Blue"
        case .emberGlass: return "Ember Glass"
        case .mossTerminal: return "Moss Terminal"
        case .fjord: return "Fjord"
        case .paper: return "Paper"
        case .daylight: return "Daylight"
        case .contrastDark: return "Contrast Dark"
        case .contrastLight: return "Contrast Light"
        }
    }

    var sourceSummary: String {
        switch self {
        case .native:
            return "macOS system colors"
        case .aurora, .graphiteBlue, .emberGlass, .mossTerminal, .fjord, .paper, .daylight, .contrastDark, .contrastLight:
            return "Rune theme"
        }
    }

    var preferredColorScheme: ColorScheme? {
        switch self {
        case .native:
            return nil
        case .paper, .daylight, .contrastLight:
            return .light
        case .aurora, .graphiteBlue, .emberGlass, .mossTerminal, .fjord, .contrastDark:
            return .dark
        }
    }

    var palette: RuneThemePalette? {
        switch self {
        case .native:
            return nil
        case .aurora:
            return RuneThemePalette(
                window: Color.runeHex("#16131c"),
                sidebar: Color.runeHex("#202230"),
                content: Color.runeHex("#282b39"),
                panel: Color.runeHex("#252837"),
                inset: Color.runeHex("#211b2a"),
                editor: Color.runeHex("#292d3d"),
                row: Color.runeHex("#202230"),
                rowSelected: Color.runeHex("#4d4266"),
                stroke: Color.runeHex("#493d5d"),
                accent: Color.runeHex("#c59cff"),
                foreground: Color.runeHex("#f4efff"),
                secondaryText: Color.runeHex("#b9accd"),
                mutedText: Color.runeHex("#8e819f"),
                success: Color.runeHex("#7ee6a8"),
                warning: Color.runeHex("#ffd166"),
                danger: Color.runeHex("#ff7a9a"),
                info: Color.runeHex("#8bd3ff")
            )
        case .graphiteBlue:
            return RuneThemePalette(
                window: Color.runeHex("#1b2027"),
                sidebar: Color.runeHex("#202630"),
                content: Color.runeHex("#252c36"),
                panel: Color.runeHex("#27303b"),
                inset: Color.runeHex("#1d232c"),
                editor: Color.runeHex("#222933"),
                row: Color.runeHex("#2d3541"),
                rowSelected: Color.runeHex("#3b5267"),
                stroke: Color.runeHex("#485360"),
                accent: Color.runeHex("#6bb7f7"),
                foreground: Color.runeHex("#eef4fb"),
                secondaryText: Color.runeHex("#adbac8"),
                mutedText: Color.runeHex("#8592a0"),
                success: Color.runeHex("#86d59a"),
                warning: Color.runeHex("#e9c46a"),
                danger: Color.runeHex("#ef7b86"),
                info: Color.runeHex("#7cc7ff")
            )
        case .emberGlass:
            return RuneThemePalette(
                window: Color.runeHex("#11151b"),
                sidebar: Color.runeHex("#20242b"),
                content: Color.runeHex("#24272f"),
                panel: Color.runeHex("#262a33"),
                inset: Color.runeHex("#31333a"),
                editor: Color.runeHex("#151a20"),
                row: Color.runeHex("#24272f"),
                rowSelected: Color.runeHex("#46443f"),
                stroke: Color.runeHex("#48474a"),
                accent: Color.runeHex("#ffb454"),
                foreground: Color.runeHex("#f8f1e7"),
                secondaryText: Color.runeHex("#c2b8aa"),
                mutedText: Color.runeHex("#958b7c"),
                success: Color.runeHex("#9bdc88"),
                warning: Color.runeHex("#ffcc66"),
                danger: Color.runeHex("#ff8a80"),
                info: Color.runeHex("#7ed0ff")
            )
        case .mossTerminal:
            return RuneThemePalette(
                window: Color.runeHex("#20221e"),
                sidebar: Color.runeHex("#242820"),
                content: Color.runeHex("#2c2f26"),
                panel: Color.runeHex("#2d3028"),
                inset: Color.runeHex("#22251f"),
                editor: Color.runeHex("#292c23"),
                row: Color.runeHex("#34382d"),
                rowSelected: Color.runeHex("#566246"),
                stroke: Color.runeHex("#4b5041"),
                accent: Color.runeHex("#b8db4c"),
                foreground: Color.runeHex("#f1f4df"),
                secondaryText: Color.runeHex("#bec6a8"),
                mutedText: Color.runeHex("#929b7e"),
                success: Color.runeHex("#a7e06f"),
                warning: Color.runeHex("#e5c45a"),
                danger: Color.runeHex("#ef7f74"),
                info: Color.runeHex("#8dcbd2")
            )
        case .fjord:
            return RuneThemePalette(
                window: Color.runeHex("#082b33"),
                sidebar: Color.runeHex("#103844"),
                content: Color.runeHex("#0b3039"),
                panel: Color.runeHex("#123c47"),
                inset: Color.runeHex("#092a32"),
                editor: Color.runeHex("#092d36"),
                row: Color.runeHex("#123944"),
                rowSelected: Color.runeHex("#255d63"),
                stroke: Color.runeHex("#52727c"),
                accent: Color.runeHex("#43a6d9"),
                foreground: Color.runeHex("#e6f7fb"),
                secondaryText: Color.runeHex("#a9c7cf"),
                mutedText: Color.runeHex("#7f9da6"),
                success: Color.runeHex("#77d6a0"),
                warning: Color.runeHex("#dfc36f"),
                danger: Color.runeHex("#ed7c8a"),
                info: Color.runeHex("#60c5e8")
            )
        case .paper:
            return RuneThemePalette(
                window: Color.runeHex("#f7f6f2"),
                sidebar: Color.runeHex("#eeeee9"),
                content: Color.runeHex("#fbfaf6"),
                panel: Color.runeHex("#ffffff"),
                inset: Color.runeHex("#f0efe9"),
                editor: Color.runeHex("#fffefa"),
                row: Color.runeHex("#f1f0ea"),
                rowSelected: Color.runeHex("#dce9f0"),
                stroke: Color.runeHex("#d4d2c9"),
                accent: Color.runeHex("#3d70b2"),
                foreground: Color.runeHex("#1f2933"),
                secondaryText: Color.runeHex("#52606d"),
                mutedText: Color.runeHex("#7b8794"),
                success: Color.runeHex("#2f8f62"),
                warning: Color.runeHex("#a86d00"),
                danger: Color.runeHex("#b42335"),
                info: Color.runeHex("#2f80b7")
            )
        case .daylight:
            return RuneThemePalette(
                window: Color.runeHex("#fbf5e6"),
                sidebar: Color.runeHex("#f1ead8"),
                content: Color.runeHex("#fffaf0"),
                panel: Color.runeHex("#fffdf7"),
                inset: Color.runeHex("#eee6d2"),
                editor: Color.runeHex("#fffaf0"),
                row: Color.runeHex("#f2ead9"),
                rowSelected: Color.runeHex("#d7e5e4"),
                stroke: Color.runeHex("#c9bfa9"),
                accent: Color.runeHex("#2f7e9f"),
                foreground: Color.runeHex("#24323a"),
                secondaryText: Color.runeHex("#5f6e6f"),
                mutedText: Color.runeHex("#87918e"),
                success: Color.runeHex("#4c8f47"),
                warning: Color.runeHex("#a66a00"),
                danger: Color.runeHex("#b94a48"),
                info: Color.runeHex("#2f7e9f")
            )
        case .contrastDark:
            return RuneThemePalette(
                window: Color.runeHex("#05070a"),
                sidebar: Color.runeHex("#0b0f14"),
                content: Color.runeHex("#0f141b"),
                panel: Color.runeHex("#111820"),
                inset: Color.runeHex("#080d12"),
                editor: Color.runeHex("#060a0f"),
                row: Color.runeHex("#111820"),
                rowSelected: Color.runeHex("#1f4f7a"),
                stroke: Color.runeHex("#6f879b"),
                accent: Color.runeHex("#00b7ff"),
                foreground: Color.runeHex("#f8fbff"),
                secondaryText: Color.runeHex("#d4e4f0"),
                mutedText: Color.runeHex("#9ab0c0"),
                success: Color.runeHex("#5cff9d"),
                warning: Color.runeHex("#ffd84d"),
                danger: Color.runeHex("#ff5c7a"),
                info: Color.runeHex("#4dd8ff")
            )
        case .contrastLight:
            return RuneThemePalette(
                window: Color.runeHex("#ffffff"),
                sidebar: Color.runeHex("#f4f7fb"),
                content: Color.runeHex("#ffffff"),
                panel: Color.runeHex("#ffffff"),
                inset: Color.runeHex("#eef3f8"),
                editor: Color.runeHex("#ffffff"),
                row: Color.runeHex("#f1f5fa"),
                rowSelected: Color.runeHex("#c8e4ff"),
                stroke: Color.runeHex("#6a7f93"),
                accent: Color.runeHex("#005fcc"),
                foreground: Color.runeHex("#07111f"),
                secondaryText: Color.runeHex("#26394f"),
                mutedText: Color.runeHex("#52677d"),
                success: Color.runeHex("#006d3b"),
                warning: Color.runeHex("#8a5a00"),
                danger: Color.runeHex("#b00020"),
                info: Color.runeHex("#005fae")
            )
        }
    }

    var resolvedTheme: RuneResolvedTheme {
        Self.resolvedThemeCache[self] ?? RuneAppearanceTheme.native.makeResolvedTheme()
    }

    private static let resolvedThemeCache = Dictionary(
        uniqueKeysWithValues: RuneAppearanceTheme.allCases.map { theme in
            (theme, theme.makeResolvedTheme())
        }
    )

    private func makeResolvedTheme() -> RuneResolvedTheme {
        RuneResolvedTheme(
            id: rawValue,
            title: title,
            sourceSummary: sourceSummary,
            preferredColorScheme: preferredColorScheme,
            palette: palette,
            appKitPalette: appKitPalette,
            syntaxPalette: syntaxPalette,
            builtin: self
        )
    }

    var appKitPalette: RuneThemeAppKitPalette? {
        guard let palette else { return nil }
        return RuneThemeAppKitPalette(
            window: palette.window.runeHexRGB,
            foreground: palette.foreground.runeHexRGB,
            accent: palette.accent.runeHexRGB,
            stroke: palette.stroke.runeHexRGB,
            row: palette.row.runeHexRGB,
            rowSelected: palette.rowSelected.runeHexRGB,
            success: palette.success.runeHexRGB,
            warning: palette.warning.runeHexRGB,
            danger: palette.danger.runeHexRGB,
            info: palette.info.runeHexRGB,
            selectedAlpha: appKitSelectionAlpha
        )
    }

    private var appKitSelectionAlpha: CGFloat {
        switch self {
        case .paper: return 0.14
        case .daylight: return 0.16
        case .contrastDark: return 0.24
        case .native, .aurora, .graphiteBlue, .emberGlass, .mossTerminal, .fjord, .contrastLight:
            return 0.18
        }
    }

    private var syntaxPalette: RuneThemeSyntaxPalette? {
        guard let palette else { return nil }
        let boolean: String
        let directive: String
        let anchor: String

        switch self {
        case .native:
            return nil
        case .aurora:
            (boolean, directive, anchor) = ("#d7b3ff", "#ff9ac8", "#80e6d6")
        case .graphiteBlue:
            (boolean, directive, anchor) = ("#b8a7ff", "#f09ab6", "#78d5d2")
        case .emberGlass:
            (boolean, directive, anchor) = ("#d6a3ff", "#ff9ca8", "#82d8c7")
        case .mossTerminal:
            (boolean, directive, anchor) = ("#c2a6ff", "#eba0a8", "#80d6bf")
        case .fjord:
            (boolean, directive, anchor) = ("#b5a9ff", "#ef96ad", "#75d7cf")
        case .paper:
            (boolean, directive, anchor) = ("#7a4fb3", "#b0446d", "#1c7f78")
        case .daylight:
            (boolean, directive, anchor) = ("#7b5eb8", "#b45a78", "#27817a")
        case .contrastDark:
            (boolean, directive, anchor) = ("#c58cff", "#ff8cc6", "#64ffe5")
        case .contrastLight:
            (boolean, directive, anchor) = ("#6a40b8", "#a0005a", "#00766f")
        }

        return RuneThemeSyntaxPalette(
            key: palette.info.runeHexRGB,
            string: palette.success.runeHexRGB,
            number: palette.warning.runeHexRGB,
            boolean: boolean,
            comment: palette.mutedText.runeHexRGB,
            directive: directive,
            anchor: anchor
        )
    }

    static func builtin(_ rawValue: String) -> RuneAppearanceTheme {
        RuneAppearanceTheme(rawValue: rawValue) ?? .native
    }

    static func resolved(_ rawValue: String) -> RuneResolvedTheme {
        RuneThemeCatalog.resolve(rawValue)
    }
}

struct RuneResolvedTheme: Identifiable {
    let id: String
    let title: String
    let sourceSummary: String
    let preferredColorScheme: ColorScheme?
    let palette: RuneThemePalette?
    let appKitPalette: RuneThemeAppKitPalette?
    let syntaxPalette: RuneThemeSyntaxPalette?
    let builtin: RuneAppearanceTheme?

    init(id: String, title: String, sourceSummary: String, preferredColorScheme: ColorScheme?,
         palette: RuneThemePalette?, appKitPalette: RuneThemeAppKitPalette?,
         syntaxPalette: RuneThemeSyntaxPalette?, builtin: RuneAppearanceTheme?) {
        self.id = id
        self.title = title
        self.sourceSummary = sourceSummary
        self.preferredColorScheme = preferredColorScheme
        self.builtin = builtin
        guard let palette, let preferredColorScheme else {
            self.palette = palette
            self.appKitPalette = appKitPalette
            self.syntaxPalette = syntaxPalette
            return
        }
        let corrected = RuneThemeContrast.normalized(palette, scheme: preferredColorScheme)
        self.palette = corrected
        self.appKitPalette = RuneThemeAppKitPalette(
            window: corrected.window.runeHexRGB, foreground: corrected.foreground.runeHexRGB,
            accent: corrected.accentFill.runeHexRGB, stroke: corrected.stroke.runeHexRGB,
            row: corrected.row.runeHexRGB, rowSelected: corrected.rowSelected.runeHexRGB,
            success: corrected.success.runeHexRGB, warning: corrected.warning.runeHexRGB,
            danger: corrected.danger.runeHexRGB, info: corrected.info.runeHexRGB,
            selectedAlpha: appKitPalette?.selectedAlpha ?? 0.18
        )
        self.syntaxPalette = syntaxPalette.map { RuneThemeContrast.syntax($0, palette: corrected) }
    }

    var isNative: Bool { builtin == .native }
}

struct RuneThemeAppKitPalette {
    let window: String
    let foreground: String
    let accent: String
    let stroke: String
    let row: String
    let rowSelected: String
    let success: String
    let warning: String
    let danger: String
    let info: String
    let selectedAlpha: CGFloat

    init(
        window: String,
        foreground: String,
        accent: String,
        stroke: String,
        row: String,
        rowSelected: String,
        success: String? = nil,
        warning: String? = nil,
        danger: String? = nil,
        info: String? = nil,
        selectedAlpha: CGFloat = 0.18
    ) {
        self.window = window
        self.foreground = foreground
        self.accent = accent
        self.stroke = stroke
        self.row = row
        self.rowSelected = rowSelected
        self.success = success ?? accent
        self.warning = warning ?? accent
        self.danger = danger ?? accent
        self.info = info ?? accent
        self.selectedAlpha = selectedAlpha
    }
}

struct RuneThemeSyntaxPalette {
    let key: String
    let string: String
    let number: String
    let boolean: String
    let comment: String
    let directive: String
    let anchor: String
}

struct RuneThemePalette {
    let window: Color
    let sidebar: Color
    let content: Color
    let panel: Color
    let inset: Color
    let editor: Color
    let row: Color
    let rowSelected: Color
    let stroke: Color
    let accent: Color
    let foreground: Color
    let secondaryText: Color
    let mutedText: Color
    let success: Color
    let warning: Color
    let danger: Color
    let info: Color
    // Text may need a contrast correction; fills and selections retain the
    // authored accent instead of inheriting that lighter/darker text color.
    var authoredAccent: Color? = nil

    var accentFill: Color { authoredAccent ?? accent }
    var selectionFill: Color { accentFill.opacity(0.18) }
    var selectionStroke: Color { accentFill.opacity(0.46) }
    var focusRing: Color { accentFill.opacity(0.72) }
    var chipFill: Color { secondaryText.opacity(0.13) }
    var divider: Color { stroke.opacity(0.48) }
}

enum RuneSemanticColorRole: Sendable {
    case success
    case warning
    case danger
    case info

    func color(in palette: RuneThemePalette?) -> Color {
        switch self {
        case .success: return palette?.success ?? Color(nsColor: RuneThemeContrast.nativeInk(.systemGreen))
        case .warning: return palette?.warning ?? Color(nsColor: RuneThemeContrast.nativeInk(.systemOrange))
        case .danger: return palette?.danger ?? Color(nsColor: RuneThemeContrast.nativeInk(.systemRed))
        case .info: return palette?.info ?? Color(nsColor: RuneThemeContrast.nativeInk(.systemBlue))
        }
    }
}

enum RuneThemeCatalog {
    private static let zedThemeIDPrefix = "zed:"
    private static let maxThemeFileBytes = 1_500_000
    private static let maxThemeTitleLength = 80
    static let metadataRefreshInterval: TimeInterval = 0.75
    nonisolated(unsafe) private static var cachedUserThemes: (
        directoryPath: String,
        signature: String,
        checkedAt: Date,
        themes: [RuneResolvedTheme]
    )?

    static var userThemesDirectoryURL: URL {
        let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? URL(fileURLWithPath: NSHomeDirectory()).appendingPathComponent("Library/Application Support", isDirectory: true)
        return base.appendingPathComponent("Rune/Themes", isDirectory: true)
    }

    static func ensureUserThemesDirectory() {
        ensureThemesDirectory(at: userThemesDirectoryURL)
    }

    @discardableResult
    static func writeUserThemeTemplate() -> URL {
        writeThemeTemplate(in: userThemesDirectoryURL)
    }

    @discardableResult
    static func writeThemeTemplate(in directory: URL) -> URL {
        ensureThemesDirectory(at: directory)
        let templateURL = directory.appendingPathComponent("rune-theme-template.json")
        guard !FileManager.default.fileExists(atPath: templateURL.path) else {
            return templateURL
        }

        let template = """
        {
          "name": "Rune Custom",
          "themes": [
            {
              "name": "Custom Dark",
              "appearance": "dark",
              "style": {
                "background": "#101820ff",
                "panel.background": "#182431ff",
                "surface.background": "#182431ff",
                "element.background": "#1e2c3aff",
                "element.selected": "#294866ff",
                "border": "#40576eff",
                "text": "#f4f8fbff",
                "text.muted": "#b6c4d1ff",
                "text.placeholder": "#8ea0adff",
                "text.accent": "#6bb7f7ff",
                "editor.background": "#0d141cff",
                "editor.foreground": "#e7edf3ff",
                "success": "#7ee6a8ff",
                "warning": "#ffd166ff",
                "error": "#ff7a9aff",
                "info": "#8bd3ffff",
                "syntax": {
                  "property": { "color": "#8bd3ffff" },
                  "string": { "color": "#7ee6a8ff" },
                  "number": { "color": "#ffd166ff" },
                  "boolean": { "color": "#c59cffff" },
                  "comment": { "color": "#8ea0adff" },
                  "keyword": { "color": "#ff9ac8ff" },
                  "type": { "color": "#80e6d6ff" }
                }
              }
            }
          ]
        }
        """
        try? template.write(to: templateURL, atomically: true, encoding: .utf8)
        return templateURL
    }

    private static func ensureThemesDirectory(at directory: URL) {
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    }

    static func availableThemes() -> [RuneResolvedTheme] {
        RuneAppearanceTheme.allCases.map(\.resolvedTheme) + userThemes()
    }

    static func resolve(_ rawValue: String) -> RuneResolvedTheme {
        if let builtin = RuneAppearanceTheme(rawValue: rawValue) {
            return builtin.resolvedTheme
        }
        return userThemes().first(where: { $0.id == rawValue }) ?? RuneAppearanceTheme.native.resolvedTheme
    }

    static func reloadUserThemes() {
        cachedUserThemes = nil
    }

    static func userThemes(
        from directory: URL = userThemesDirectoryURL,
        referenceDate: Date = Date()
    ) -> [RuneResolvedTheme] {
        ensureThemesDirectory(at: directory)
        let directoryPath = directory.standardizedFileURL.path

        if let cachedUserThemes,
           cachedUserThemes.directoryPath == directoryPath {
            let cacheAge = referenceDate.timeIntervalSince(cachedUserThemes.checkedAt)
            if cacheAge >= 0, cacheAge < metadataRefreshInterval {
                return cachedUserThemes.themes
            }
        }

        let signature = "\(directoryPath)|\(directorySignature(directory))"
        if let cachedUserThemes,
           cachedUserThemes.directoryPath == directoryPath,
           cachedUserThemes.signature == signature {
            Self.cachedUserThemes = (
                directoryPath: directoryPath,
                signature: signature,
                checkedAt: referenceDate,
                themes: cachedUserThemes.themes
            )
            return cachedUserThemes.themes
        }

        let themes = loadZedThemes(from: directory)
        cachedUserThemes = (
            directoryPath: directoryPath,
            signature: signature,
            checkedAt: referenceDate,
            themes: themes
        )
        return themes
    }

    static func loadZedThemes(from directory: URL) -> [RuneResolvedTheme] {
        let fileURLs = (try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: [.fileSizeKey, .isRegularFileKey],
            options: [.skipsHiddenFiles]
        )) ?? []

        let themes = fileURLs
            .filter { $0.pathExtension.lowercased() == "json" }
            .sorted { $0.lastPathComponent.localizedCaseInsensitiveCompare($1.lastPathComponent) == .orderedAscending }
            .flatMap { fileURL -> [RuneResolvedTheme] in
                guard let values = try? fileURL.resourceValues(forKeys: [.fileSizeKey, .isRegularFileKey]),
                      values.isRegularFile == true,
                      (values.fileSize ?? 0) <= maxThemeFileBytes,
                      let data = try? Data(contentsOf: fileURL) else {
                    return []
                }
                let sourceID = stableSourceID(for: fileURL.lastPathComponent)
                return RuneZedThemeDecoder.decode(data: data, sourceID: sourceID)
            }
        return disambiguatedThemes(themes)
    }

    private static func directorySignature(_ directory: URL) -> String {
        let fileURLs = (try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: [.fileSizeKey, .contentModificationDateKey, .isRegularFileKey],
            options: [.skipsHiddenFiles]
        )) ?? []

        return fileURLs
            .filter { $0.pathExtension.lowercased() == "json" }
            .compactMap { fileURL -> String? in
                guard let values = try? fileURL.resourceValues(forKeys: [.fileSizeKey, .contentModificationDateKey, .isRegularFileKey]),
                      values.isRegularFile == true else {
                    return nil
                }
                let modified = values.contentModificationDate?.timeIntervalSince1970 ?? 0
                return "\(fileURL.lastPathComponent):\(values.fileSize ?? 0):\(modified)"
            }
            .sorted()
            .joined(separator: "|")
    }

    private static func stableSourceID(for filename: String) -> String {
        let stem = URL(fileURLWithPath: filename).deletingPathExtension().lastPathComponent
        return stableIDComponent(stem)
    }

    private static func stableIDComponent(_ rawValue: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_"))
        let scalars = rawValue.unicodeScalars.map { allowed.contains($0) ? Character($0) : "-" }
        let slug = String(scalars).trimmingCharacters(in: CharacterSet(charactersIn: "-"))
        return slug.isEmpty ? "theme" : slug.lowercased()
    }

    static func externalThemeID(sourceID: String, themeName: String) -> String {
        "\(zedThemeIDPrefix)\(sourceID):\(stableIDComponent(themeName))"
    }

    static func displayTitle(_ rawValue: String) -> String {
        let folded = rawValue
            .unicodeScalars
            .map { CharacterSet.controlCharacters.contains($0) ? " " : String($0) }
            .joined()
        let title = folded
            .split(whereSeparator: \.isWhitespace)
            .joined(separator: " ")

        guard !title.isEmpty else { return "Custom Theme" }
        return String(title.prefix(maxThemeTitleLength))
    }

    private static func disambiguatedThemes(_ themes: [RuneResolvedTheme]) -> [RuneResolvedTheme] {
        var countsByID: [String: Int] = [:]
        return themes.map { theme in
            let count = countsByID[theme.id, default: 0] + 1
            countsByID[theme.id] = count
            guard count > 1 else { return theme }

            return RuneResolvedTheme(
                id: "\(theme.id)-\(count)",
                title: theme.title,
                sourceSummary: theme.sourceSummary,
                preferredColorScheme: theme.preferredColorScheme,
                palette: theme.palette,
                appKitPalette: theme.appKitPalette,
                syntaxPalette: theme.syntaxPalette,
                builtin: theme.builtin
            )
        }
    }
}

enum RuneZedThemeDecoder {
    static func decode(data: Data, sourceID: String) -> [RuneResolvedTheme] {
        guard let family = try? JSONDecoder().decode(ZedThemeFamily.self, from: data) else { return [] }
        return family.themes.compactMap { theme in
            let appearance = theme.appearance.colorScheme
            guard let tokens = ZedThemeTokens(style: theme.style, appearance: appearance) else {
                return nil
            }
            let title = RuneThemeCatalog.displayTitle(theme.name)
            return RuneResolvedTheme(
                id: RuneThemeCatalog.externalThemeID(sourceID: sourceID, themeName: title),
                title: title,
                sourceSummary: "Custom theme",
                preferredColorScheme: appearance,
                palette: tokens.palette,
                appKitPalette: tokens.appKitPalette,
                syntaxPalette: tokens.syntaxPalette,
                builtin: nil
            )
        }
    }

    private struct ZedThemeFamily: Decodable {
        let themes: [ZedTheme]
    }

    private struct ZedTheme: Decodable {
        let name: String
        let appearance: ZedAppearance
        let style: [String: JSONValue]
    }

    private enum ZedAppearance: String, Decodable {
        case light
        case dark

        var colorScheme: ColorScheme {
            switch self {
            case .light: return .light
            case .dark: return .dark
            }
        }
    }

    private struct ZedThemeTokens {
        let palette: RuneThemePalette
        let appKitPalette: RuneThemeAppKitPalette
        let syntaxPalette: RuneThemeSyntaxPalette

        init?(style: [String: JSONValue], appearance: ColorScheme) {
            let fallback = appearance == .dark ? RuneAppearanceTheme.graphiteBlue.resolvedTheme : RuneAppearanceTheme.paper.resolvedTheme
            guard let fallbackPalette = fallback.appKitPalette else { return nil }

            let foreground = style.visibleHex("text") ?? style.visibleHex("editor.foreground") ?? fallbackPalette.foreground
            let secondary = style.visibleHex("text.muted") ?? style.visibleHex("icon.muted") ?? foreground
            let muted = style.visibleHex("text.placeholder") ?? style.visibleHex("text.disabled") ?? secondary
            let accent = style.visibleHex("text.accent") ?? style.visibleHex("icon.accent") ?? style.visibleHex("link_text.hover") ?? fallbackPalette.accent
            let stroke = style.visibleHex("border.variant") ?? style.visibleHex("border") ?? fallbackPalette.stroke
            let window = style.visibleHex("title_bar.background") ?? style.visibleHex("background") ?? fallbackPalette.window
            let sidebar = style.visibleHex("panel.background") ?? style.visibleHex("surface.background") ?? window
            let content = style.visibleHex("background") ?? window
            let panel = style.visibleHex("panel.background") ?? style.visibleHex("surface.background") ?? content
            let inset = style.visibleHex("element.background") ?? panel
            let editor = style.visibleHex("editor.background") ?? style.visibleHex("terminal.background") ?? content
            let row = style.visibleHex("element.background") ?? style.visibleHex("surface.background") ?? panel
            let rowSelected = style.visibleHex("element.selected") ?? style.visibleHex("element.selection_background") ?? accent
            let success = style.visibleHex("success") ?? style.visibleHex("version_control.added") ?? (appearance == .dark ? "#86d59a" : "#2f8f62")
            let warning = style.visibleHex("warning") ?? style.visibleHex("version_control.modified") ?? (appearance == .dark ? "#e9c46a" : "#a86d00")
            let danger = style.visibleHex("error") ?? style.visibleHex("version_control.deleted") ?? (appearance == .dark ? "#ef7b86" : "#b42335")
            let info = style.visibleHex("info") ?? accent

            palette = RuneThemePalette(
                window: Color.runeHex(window),
                sidebar: Color.runeHex(sidebar),
                content: Color.runeHex(content),
                panel: Color.runeHex(panel),
                inset: Color.runeHex(inset),
                editor: Color.runeHex(editor),
                row: Color.runeHex(row),
                rowSelected: Color.runeHex(rowSelected),
                stroke: Color.runeHex(stroke),
                accent: Color.runeHex(accent),
                foreground: Color.runeHex(foreground),
                secondaryText: Color.runeHex(secondary),
                mutedText: Color.runeHex(muted),
                success: Color.runeHex(success),
                warning: Color.runeHex(warning),
                danger: Color.runeHex(danger),
                info: Color.runeHex(info)
            )
            appKitPalette = RuneThemeAppKitPalette(
                window: window,
                foreground: foreground,
                accent: accent,
                stroke: stroke,
                row: row,
                rowSelected: rowSelected,
                success: success,
                warning: warning,
                danger: danger,
                info: info,
                selectedAlpha: appearance == .dark ? 0.20 : 0.16
            )
            syntaxPalette = RuneThemeSyntaxPalette(
                key: style.syntaxHex("property") ?? style.syntaxHex("attribute") ?? info,
                string: style.syntaxHex("string") ?? style.visibleHex("terminal.ansi.green") ?? success,
                number: style.syntaxHex("number") ?? style.syntaxHex("constant") ?? warning,
                boolean: style.syntaxHex("boolean") ?? style.syntaxHex("constant") ?? accent,
                comment: style.syntaxHex("comment") ?? muted,
                directive: style.syntaxHex("keyword") ?? style.syntaxHex("function") ?? accent,
                anchor: style.syntaxHex("type") ?? style.visibleHex("terminal.ansi.cyan") ?? info
            )
        }
    }
}

private enum JSONValue: Decodable {
    case string(String)
    case object([String: JSONValue])
    case array([JSONValue])
    case bool(Bool)
    case number(Double)
    case null

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(String.self) {
            self = .string(value)
        } else if let value = try? container.decode([String: JSONValue].self) {
            self = .object(value)
        } else if let value = try? container.decode([JSONValue].self) {
            self = .array(value)
        } else if let value = try? container.decode(Bool.self) {
            self = .bool(value)
        } else if let value = try? container.decode(Double.self) {
            self = .number(value)
        } else {
            self = .null
        }
    }

    var stringValue: String? {
        guard case let .string(value) = self else { return nil }
        return value
    }

    subscript(key: String) -> JSONValue? {
        guard case let .object(values) = self else { return nil }
        return values[key]
    }
}

private extension Dictionary where Key == String, Value == JSONValue {
    func visibleHex(_ key: String) -> String? {
        RuneThemeColorParser.visibleHex(self[key]?.stringValue)
    }

    func syntaxHex(_ key: String) -> String? {
        guard let syntax = self["syntax"],
              let color = syntax[key]?["color"]?.stringValue else {
            return nil
        }
        return RuneThemeColorParser.visibleHex(color)
    }
}

enum RuneThemeColorParser {
    struct Components: Equatable {
        let red: CGFloat
        let green: CGFloat
        let blue: CGFloat
        let alpha: CGFloat
    }

    static func visibleHex(_ rawValue: String?) -> String? {
        guard let hex = normalizedHex(rawValue) else { return nil }
        if hex.count == 9, let alpha = UInt8(String(hex.suffix(2)), radix: 16), alpha < 28 {
            return nil
        }
        return hex
    }

    static func normalizedHex(_ rawValue: String?) -> String? {
        guard var value = rawValue?.trimmingCharacters(in: .whitespacesAndNewlines), !value.isEmpty else {
            return nil
        }
        if !value.hasPrefix("#") {
            value = "#\(value)"
        }
        let hex = String(value.dropFirst())
        guard (hex.count == 6 || hex.count == 8), UInt64(hex, radix: 16) != nil else {
            return nil
        }
        return "#\(hex.lowercased())"
    }

    static func components(_ rawValue: String?) -> Components? {
        guard let normalized = normalizedHex(rawValue) else { return nil }
        let hex = String(normalized.dropFirst())
        guard let value = UInt64(hex, radix: 16) else { return nil }

        if hex.count == 8 {
            return Components(
                red: CGFloat((value >> 24) & 0xff) / 255,
                green: CGFloat((value >> 16) & 0xff) / 255,
                blue: CGFloat((value >> 8) & 0xff) / 255,
                alpha: CGFloat(value & 0xff) / 255
            )
        }

        return Components(
            red: CGFloat((value >> 16) & 0xff) / 255,
            green: CGFloat((value >> 8) & 0xff) / 255,
            blue: CGFloat(value & 0xff) / 255,
            alpha: 1
        )
    }

    static func rgbaHex(_ color: NSColor) -> String? {
        guard let converted = color.usingColorSpace(.sRGB) else { return nil }
        return String(
            format: "#%02x%02x%02x%02x",
            channelByte(converted.redComponent),
            channelByte(converted.greenComponent),
            channelByte(converted.blueComponent),
            channelByte(converted.alphaComponent)
        )
    }

    private static func channelByte(_ component: CGFloat) -> Int {
        Int((min(1, max(0, component)) * 255).rounded())
    }
}

private struct RuneThemePaletteEnvironmentKey: EnvironmentKey {
    static let defaultValue: RuneThemePalette? = nil
}

private struct RuneResolvedThemeEnvironmentKey: EnvironmentKey {
    static let defaultValue = RuneAppearanceTheme.native.resolvedTheme
}

extension EnvironmentValues {
    var runeThemePalette: RuneThemePalette? {
        get { self[RuneThemePaletteEnvironmentKey.self] }
        set { self[RuneThemePaletteEnvironmentKey.self] = newValue }
    }

    var runeResolvedTheme: RuneResolvedTheme {
        get { self[RuneResolvedThemeEnvironmentKey.self] }
        set { self[RuneResolvedThemeEnvironmentKey.self] = newValue }
    }
}

private struct RuneAppearanceThemeModifier: ViewModifier {
    @Environment(\.colorScheme) private var colorScheme
    let theme: RuneResolvedTheme

    private var largeTextColors: RuneLargeTextColors {
        if let palette = theme.palette {
            return RuneLargeTextColors(foreground: palette.foreground, secondary: palette.secondaryText,
                                       accent: palette.accent, warning: palette.warning)
        }
        var environment = EnvironmentValues()
        environment.colorScheme = colorScheme
        return RuneLargeTextColors(foreground: RuneTextStyle.primary.resolve(in: environment),
                                   secondary: RuneTextStyle.secondary.resolve(in: environment),
                                   accent: RuneTextStyle.accent.resolve(in: environment),
                                   warning: RuneTextStyle.warning.resolve(in: environment))
    }

    func body(content: Content) -> some View {
        let palette = theme.palette
        content
            .foregroundStyle(palette?.foreground ?? Color.primary)
            .environment(\.runeResolvedTheme, theme)
            .environment(\.runeThemePalette, palette)
            .environment(\.runeLargeTextColors, largeTextColors)
            .tint(palette?.accentFill ?? Color.accentColor)
            .accentColor(palette?.accentFill)
            .background((palette?.window ?? Color.clear).ignoresSafeArea())
            .background(RuneAppearanceWindowConfigurator(theme: theme).id(theme.id).frame(width: 0, height: 0))
            .preferredColorScheme(theme.preferredColorScheme)
    }
}

extension View {
    func runeAppearanceTheme(_ theme: RuneResolvedTheme) -> some View {
        modifier(RuneAppearanceThemeModifier(theme: theme))
    }
}

private struct RuneAppearanceWindowConfigurator: NSViewRepresentable {
    let theme: RuneResolvedTheme

    final class TrackingView: NSView {
        var onWindowChange: ((NSWindow?) -> Void)?

        override func viewDidMoveToWindow() {
            super.viewDidMoveToWindow()
            onWindowChange?(window)
        }
    }

    final class Coordinator {
        var appliedThemeID: String?
        var configuredWindowNumber: Int?
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeNSView(context: Context) -> TrackingView {
        let view = TrackingView(frame: .zero)
        view.isHidden = true
        view.onWindowChange = { window in
            guard let window else { return }
            apply(theme: theme, to: window, coordinator: context.coordinator, force: true)
        }
        return view
    }

    func updateNSView(_ nsView: TrackingView, context: Context) {
        nsView.onWindowChange = { window in
            guard let window else { return }
            apply(theme: theme, to: window, coordinator: context.coordinator, force: true)
        }

        guard let window = nsView.window else { return }
        apply(theme: theme, to: window, coordinator: context.coordinator, force: false)
    }

    private func apply(
        theme: RuneResolvedTheme,
        to window: NSWindow,
        coordinator: Coordinator,
        force: Bool
    ) {
        let windowNumber = window.windowNumber
        guard force || coordinator.appliedThemeID != theme.id || coordinator.configuredWindowNumber != windowNumber else { return }

        coordinator.appliedThemeID = theme.id
        coordinator.configuredWindowNumber = windowNumber

        switch theme.preferredColorScheme {
        case .some(.dark):
            window.appearance = NSAppearance(named: .darkAqua)
            window.contentView?.appearance = NSAppearance(named: .darkAqua)
        case .some(.light):
            window.appearance = NSAppearance(named: .aqua)
            window.contentView?.appearance = NSAppearance(named: .aqua)
        case nil:
            window.appearance = nil
            window.contentView?.appearance = nil
        @unknown default:
            window.appearance = nil
            window.contentView?.appearance = nil
        }

        if theme.isNative {
            window.backgroundColor = .windowBackgroundColor
        } else {
            window.backgroundColor = windowBackground(for: theme)
        }
        invalidateWindowChrome(window)
    }

    private func windowBackground(for theme: RuneResolvedTheme) -> NSColor {
        guard let window = theme.appKitPalette?.window else { return .windowBackgroundColor }
        return NSColor.runeHex(window)
    }

    private func invalidateWindowChrome(_ window: NSWindow) {
        window.viewsNeedDisplay = true
        guard let contentView = window.contentView else { return }

        markNeedsDisplay(contentView)
        contentView.needsLayout = true
        contentView.layoutSubtreeIfNeeded()

        DispatchQueue.main.async {
            self.markNeedsDisplay(contentView)
            window.viewsNeedDisplay = true
        }
    }

    private func markNeedsDisplay(_ view: NSView) {
        view.needsDisplay = true
        view.needsLayout = true
        for subview in view.subviews {
            markNeedsDisplay(subview)
        }
    }
}

extension NSColor {
    static func runeHex(_ hex: String) -> NSColor {
        guard let components = RuneThemeColorParser.components(hex) else { return .black }
        return NSColor(
            srgbRed: components.red,
            green: components.green,
            blue: components.blue,
            alpha: components.alpha
        )
    }
}

extension Color {
    static func runeHex(_ hex: String) -> Color {
        guard let components = RuneThemeColorParser.components(hex) else { return .black }
        return Color(
            red: Double(components.red),
            green: Double(components.green),
            blue: Double(components.blue),
            opacity: Double(components.alpha)
        )
    }

    fileprivate var runeHexRGBA: String {
        RuneThemeColorParser.rgbaHex(NSColor(self)) ?? "#000000ff"
    }

    fileprivate var runeHexRGB: String {
        let rgba = runeHexRGBA
        return rgba.hasSuffix("ff") ? String(rgba.dropLast(2)) : rgba
    }
}
