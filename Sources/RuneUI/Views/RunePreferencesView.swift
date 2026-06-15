import AppKit
import RuneCore
import RuneDiagnostics
import RuneExport
import RuneStore
import SwiftUI
import UniformTypeIdentifiers

private struct SettingsHelpButton: View {
    let text: String
    @State private var isPopoverPresented = false

    var body: some View {
        Button {
            isPopoverPresented.toggle()
        } label: {
            Image(systemName: "questionmark.circle")
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(2)
                .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .help(text)
        .popover(isPresented: $isPopoverPresented, arrowEdge: .top) {
            Text(text)
                .font(.footnote)
                .foregroundStyle(.primary)
                .frame(width: 280, alignment: .leading)
                .padding(10)
        }
        .accessibilityLabel("More info")
        .accessibilityHint(text)
    }
}

private enum RuneSettingsMetrics {
    static let pageMaxWidth: CGFloat = 760
    static let pageHorizontalPadding: CGFloat = 20
    static let pageVerticalPadding: CGFloat = 16
    static let pageSpacing: CGFloat = 14
    static let sectionSpacing: CGFloat = 12
    static let sectionCardPadding: CGFloat = 14
    static let rowMinHeight: CGFloat = 38
    static let rowControlSpacing: CGFloat = 12
    static let rowControlColumnWidth: CGFloat = 260
    static let compactControlHeight: CGFloat = 32
    static let textFieldWidth: CGFloat = 92
}

private struct ExportOpenerRecommendation {
    let appName: String
    let kind: ConfiguredExportFileKind

    func detail(bundleIdentifier: String) -> String {
        switch kind {
        case .plainText:
            return "Works well for .log, .txt, .yaml, .json, and terminal transcript exports. Bundle ID: \(bundleIdentifier)"
        case .archive:
            return "Works well for .zip log archives and transcript archives. Bundle ID: \(bundleIdentifier)"
        }
    }
}

private struct DetectedExportOpener {
    let appName: String
    let bundleIdentifier: String
    let detail: String
}

private struct RuneSettingsTokenButtonStyle: ButtonStyle {
    let theme: RuneResolvedTheme

    func makeBody(configuration: Configuration) -> some View {
        let palette = RuneThemePresentation(theme: theme).palette
        configuration.label
            .font(.subheadline.weight(.medium))
            .foregroundStyle(palette.accent)
            .lineLimit(1)
            .padding(.horizontal, 12)
            .frame(height: RuneSettingsMetrics.compactControlHeight)
            .background(
                RoundedRectangle(cornerRadius: 7, style: .continuous)
                    .fill((configuration.isPressed ? palette.accent.opacity(0.18) : palette.inset.opacity(0.94)))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 7, style: .continuous)
                    .strokeBorder(palette.accent.opacity(configuration.isPressed ? 0.62 : 0.42), lineWidth: 1)
            )
            .contentShape(RoundedRectangle(cornerRadius: 7, style: .continuous))
    }
}

private struct RuneThemeSelectorCard: View {
    let theme: RuneResolvedTheme
    let isSelected: Bool
    let action: () -> Void

    private var presentation: RuneThemePresentation { RuneThemePresentation(theme: theme) }
    private var palette: RuneThemePalette { presentation.palette }

    var body: some View {
        Button(action: action) {
            VStack(alignment: .leading, spacing: 12) {
                HStack(alignment: .top, spacing: 10) {
                    Image(systemName: presentation.appearanceSymbol)
                        .font(.system(size: 14, weight: .semibold))
                        .foregroundStyle(palette.accent)
                        .frame(width: 26, height: 26)
                        .background(palette.accent.opacity(0.13), in: Circle())
                        .help(presentation.appearanceTitle)

                    VStack(alignment: .leading, spacing: 3) {
                        Text(presentation.title)
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(palette.foreground)
                            .lineLimit(1)
                        Text(presentation.sourceSummary)
                            .font(.caption)
                            .foregroundStyle(palette.secondaryText)
                            .lineLimit(1)
                    }

                    if isSelected {
                        Image(systemName: "checkmark.circle.fill")
                            .font(.system(size: 15, weight: .semibold))
                            .foregroundStyle(palette.accent)
                    }
                }

                swatches
            }
            .padding(12)
            .frame(maxWidth: .infinity, minHeight: 96, alignment: .leading)
            .background(
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .fill(isSelected ? palette.selectionFill : palette.inset.opacity(0.82))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .strokeBorder(isSelected ? palette.focusRing : palette.stroke.opacity(0.44), lineWidth: isSelected ? 1.5 : 1)
            )
            .contentShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
        }
        .buttonStyle(.plain)
        .accessibilityLabel(theme.title)
        .accessibilityValue(isSelected ? "Selected" : presentation.appearanceTitle)
    }

    private var swatches: some View {
        HStack(spacing: 6) {
            swatch(palette.window)
            swatch(palette.panel)
            swatch(palette.accent)
            swatch(palette.success)
            swatch(palette.warning)
            swatch(palette.danger)
        }
        .accessibilityLabel("Theme colors")
    }

    private func swatch(_ color: Color) -> some View {
        RoundedRectangle(cornerRadius: 3, style: .continuous)
            .fill(color)
            .frame(width: 22, height: 14)
            .overlay(
                RoundedRectangle(cornerRadius: 3, style: .continuous)
                    .strokeBorder(Color.primary.opacity(0.10), lineWidth: 1)
            )
    }
}

private struct RuneSettingsIntegerLimitEditor: View {
    let title: String
    let value: Binding<Int>
    let valueSuffix: String
    let step: Int
    let placeholder: String
    let defaultValue: Int
    let detail: String
    let normalize: (Int) -> Int

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(alignment: .firstTextBaseline, spacing: 10) {
                Text(title)
                    .font(.subheadline.weight(.semibold))
                Spacer(minLength: 12)
                Text("\(normalizedValue) \(valueSuffix)")
                    .font(.footnote.monospacedDigit())
                    .foregroundStyle(.secondary)
            }

            HStack(spacing: 10) {
                Stepper(title, value: normalizedBinding, step: step)
                    .labelsHidden()

                TextField(placeholder, value: normalizedBinding, format: .number)
                    .textFieldStyle(.roundedBorder)
                    .frame(width: RuneSettingsMetrics.textFieldWidth)

                Button("Reset") {
                    value.wrappedValue = defaultValue
                }
                .buttonStyle(.bordered)
            }

            Text(detail)
                .font(.footnote)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
        }
    }

    private var normalizedValue: Int {
        normalize(value.wrappedValue)
    }

    private var normalizedBinding: Binding<Int> {
        Binding(
            get: { normalizedValue },
            set: { value.wrappedValue = normalize($0) }
        )
    }
}

/// Settings window content.
public struct RunePreferencesView: View {
    private enum PreferencesPane: String, CaseIterable, Identifiable {
        case general
        case themes
        case keyBindings
        case logs
        case safety
        case diagnostics
        case performance

        var id: String { rawValue }

        func title(_ t: (RuneLocalizedStringKey) -> String) -> String {
            switch self {
            case .general: return t(.settingsGeneral)
            case .themes: return t(.settingsThemes)
            case .keyBindings: return t(.settingsKeyBindings)
            case .logs: return t(.settingsLogs)
            case .safety: return t(.settingsSafety)
            case .diagnostics: return t(.settingsDiagnostics)
            case .performance: return t(.settingsPerformance)
            }
        }

        var symbol: String {
            switch self {
            case .general: return "gearshape"
            case .themes: return "paintpalette"
            case .keyBindings: return "keyboard"
            case .logs: return "text.alignleft"
            case .safety: return "lock.shield"
            case .diagnostics: return "stethoscope"
            case .performance: return "speedometer"
            }
        }
    }

    @State private var selectedPane: PreferencesPane = .general
    @AppStorage(RuneSettingsKeys.persistNamespaceListCache) private var persistNamespaceListCache = true
    @AppStorage(RuneSettingsKeys.diagnosticsLogging) private var diagnosticsLogging = true
    @AppStorage(RuneSettingsKeys.verboseDebugTrace) private var verboseDebugTrace = false
    @AppStorage(RuneSettingsKeys.backgroundPrefetchOtherContexts) private var backgroundPrefetchOtherContexts = false
    @AppStorage(RuneSettingsKeys.enableDemoCluster) private var enableDemoCluster = true
    @AppStorage(RuneSettingsKeys.logsCustomPresetOneMode) private var customOneModeRaw = RuneCustomLogPresetMode.lines.rawValue
    @AppStorage(RuneSettingsKeys.logsCustomPresetOneLines) private var customOneLinesRaw = "5000"
    @AppStorage(RuneSettingsKeys.logsCustomPresetOneTimeValue) private var customOneTimeValueRaw = "15"
    @AppStorage(RuneSettingsKeys.logsCustomPresetOneTimeUnit) private var customOneTimeUnitRaw = RuneCustomLogPresetTimeUnit.minutes.rawValue
    @AppStorage(RuneSettingsKeys.logsCustomPresetTwoMode) private var customTwoModeRaw = RuneCustomLogPresetMode.time.rawValue
    @AppStorage(RuneSettingsKeys.logsCustomPresetTwoLines) private var customTwoLinesRaw = "99999"
    @AppStorage(RuneSettingsKeys.logsCustomPresetTwoTimeValue) private var customTwoTimeValueRaw = "6"
    @AppStorage(RuneSettingsKeys.logsCustomPresetTwoTimeUnit) private var customTwoTimeUnitRaw = RuneCustomLogPresetTimeUnit.hours.rawValue
    @AppStorage(RuneSettingsKeys.exportFolderDisplayName) private var exportFolderDisplayName = ""
    @AppStorage(RuneSettingsKeys.exportTextOpenerBundleIdentifier) private var exportTextOpenerBundleIdentifier = ""
    @AppStorage(RuneSettingsKeys.exportArchiveOpenerBundleIdentifier) private var exportArchiveOpenerBundleIdentifier = ""
    @AppStorage(RuneSettingsKeys.exportUsesPrivacySafeFilenames) private var exportUsesPrivacySafeFilenames = false
    @AppStorage(RuneSettingsKeys.terminalFontSize) private var terminalFontSize = RuneSettingsKeys.terminalFontSizeDefault
    @AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault) private var hideManagedFieldsByDefault = true
    @AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false
    @AppStorage(RuneSettingsKeys.showHoverTooltips) private var showHoverTooltips = true
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault
    @AppStorage(RuneSettingsKeys.terminalScrollbackLineLimit) private var terminalScrollbackLineLimit =
        RuneSettingsKeys.terminalScrollbackLineLimitDefault
    @AppStorage(RuneSettingsKeys.persistTerminalWorkspaceState) private var persistTerminalWorkspaceState = false
    private let inklineRecommendation = ExportOpenerRecommendation(appName: "Inkline", kind: .plainText)
    private let quikZipRecommendation = ExportOpenerRecommendation(appName: "QuikZip", kind: .archive)
    @AppStorage(RuneSettingsKeys.sessionLogCacheEntryLimit) private var sessionLogCacheEntryLimit =
        RuneSettingsKeys.sessionLogCacheEntryLimitDefault
    @AppStorage(RuneSettingsKeys.resourceYAMLUndoSnapshotLimit) private var resourceYAMLUndoSnapshotLimit =
        RuneSettingsKeys.resourceYAMLUndoSnapshotLimitDefault
    @AppStorage(RuneSettingsKeys.writeSafetyRequireApplyDryRun) private var requireApplyDryRun = true
    @AppStorage(RuneSettingsKeys.writeSafetyRequireRolloutDryRun) private var requireRolloutDryRun = true
    @AppStorage(RuneSettingsKeys.writeSafetyRequireHelmDryRun) private var requireHelmDryRun = true
    @AppStorage(RuneSettingsKeys.writeSafetyShowRollbackPlan) private var showRollbackPlan = true
    @AppStorage(RuneSettingsKeys.writeSafetyRequireCopyableCommand) private var requireCopyableCommand = true
    @AppStorage(RuneSettingsKeys.writeSafetyRequirePostActionVerification) private var requirePostActionVerification = true
    @AppStorage(RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation) private var requireProductionSecondConfirmation = true
    @State private var cacheClearStatus: String?
    @State private var themeReloadNonce = 0
    @State private var recentAppearanceThemeIDs = UserDefaults.standard.runeAppearanceRecentThemes
    @State private var keyBindingShortcuts = Self.loadKeyBindingShortcuts()

    public init() {}

    public var body: some View {
        TabView(selection: $selectedPane) {
            generalSettingsForm
                .tag(PreferencesPane.general)
                .tabItem {
                    Label(PreferencesPane.general.title(settingsString), systemImage: PreferencesPane.general.symbol)
                }

            themesSettingsForm
                .tag(PreferencesPane.themes)
                .tabItem {
                    Label(PreferencesPane.themes.title(settingsString), systemImage: PreferencesPane.themes.symbol)
                }

            keyBindingsSettingsForm
                .tag(PreferencesPane.keyBindings)
                .tabItem {
                    Label(PreferencesPane.keyBindings.title(settingsString), systemImage: PreferencesPane.keyBindings.symbol)
                }

            logsSettingsForm
                .tag(PreferencesPane.logs)
                .tabItem {
                    Label(PreferencesPane.logs.title(settingsString), systemImage: PreferencesPane.logs.symbol)
                }

            safetySettingsForm
                .tag(PreferencesPane.safety)
                .tabItem {
                    Label(PreferencesPane.safety.title(settingsString), systemImage: PreferencesPane.safety.symbol)
                }

            diagnosticsSettingsForm
                .tag(PreferencesPane.diagnostics)
                .tabItem {
                    Label(PreferencesPane.diagnostics.title(settingsString), systemImage: PreferencesPane.diagnostics.symbol)
                }

            performanceSettingsForm
                .tag(PreferencesPane.performance)
                .tabItem {
                    Label(PreferencesPane.performance.title(settingsString), systemImage: PreferencesPane.performance.symbol)
                }
        }
        .id(interfaceLanguageRaw)
        .controlSize(.small)
        .frame(minWidth: 740, idealWidth: 820, minHeight: 540)
        .runeAppearanceTheme(selectedAppearanceTheme)
        .onAppear {
            refreshRecentAppearanceThemes(recordSelectedTheme: true)
        }
        .onChange(of: appearanceThemeRaw) { _, _ in
            refreshRecentAppearanceThemes(recordSelectedTheme: true)
        }
    }

    private var generalSettingsForm: some View {
        settingsPane(
            title: settingsString(.settingsGeneral),
            subtitle: settingsString(.settingsGeneralSubtitle)
        ) {
            settingsSection(settingsString(.settingsCache)) {
                settingsToggleRow(
                    settingsString(.settingsPersistNamespaceListCache),
                    help: settingsString(.settingsPersistNamespaceListCacheHelp),
                    isOn: $persistNamespaceListCache
                )

                Divider()

                VStack(alignment: .leading, spacing: 10) {
                    Text(settingsString(.settingsMaintenance))
                        .font(.subheadline.weight(.semibold))
                    HStack(spacing: 10) {
                        Button(settingsString(.settingsClearCachedClusterData), role: .destructive) {
                            clearDiskCaches()
                        }
                        .buttonStyle(.bordered)

                        if let cacheClearStatus {
                            Text(cacheClearStatus)
                                .font(.footnote)
                                .foregroundStyle(.secondary)
                                .fixedSize(horizontal: false, vertical: true)
                        }
                    }
                }
            }

            settingsSection(settingsString(.settingsAppearance)) {
                VStack(alignment: .leading, spacing: 10) {
                    HStack(alignment: .firstTextBaseline, spacing: 10) {
                        Text(settingsString(.settingsFontSize))
                            .font(.subheadline.weight(.semibold))
                        Spacer(minLength: 12)
                        Text("\(Int(clampedTerminalFontSize.rounded())) pt")
                            .font(.footnote.monospacedDigit())
                            .foregroundStyle(.secondary)
                    }

                    HStack(spacing: 10) {
                        Slider(
                            value: Binding(
                                get: { clampedTerminalFontSize },
                                set: { terminalFontSize = RuneSettingsKeys.clampedTerminalFontSize($0) }
                            ),
                            in: RuneSettingsKeys.terminalFontSizeMinimum...RuneSettingsKeys.terminalFontSizeMaximum,
                            step: 1
                        )

                        Button(settingsString(.settingsReset)) {
                            terminalFontSize = RuneSettingsKeys.terminalFontSizeDefault
                        }
                        .buttonStyle(.bordered)
                    }

                    Text(settingsString(.settingsFontSizeDetail))
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }

                Divider()

                settingsToggleRow(
                    settingsString(.settingsSimpleMode),
                    help: settingsString(.settingsSimpleModeHelp),
                    isOn: $simpleMode
                )

                Divider()

                if simpleMode {
                    HStack(alignment: .top, spacing: 10) {
                        Image(systemName: "bolt")
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.secondary)
                            .frame(width: 18, height: 18)
                        Text(settingsString(.settingsSimpleModeManagedFieldsNote))
                            .font(.footnote)
                            .foregroundStyle(.secondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                } else {
                    settingsToggleRow(
                        settingsString(.settingsHideManagedFieldsByDefault),
                        help: settingsString(.settingsHideManagedFieldsByDefaultHelp),
                        isOn: $hideManagedFieldsByDefault
                    )
                }

                Divider()

                settingsToggleRow(
                    settingsString(.settingsShowHoverTooltips),
                    help: settingsString(.settingsShowHoverTooltipsHelp),
                    isOn: $showHoverTooltips
                )

                Divider()

                settingsControlRow(
                    title: settingsString(.language),
                    detail: settingsString(.settingsLanguageDetail)
                ) {
                    Picker("Language", selection: $interfaceLanguageRaw) {
                        ForEach(RuneLanguage.allCases) { language in
                            Text(language.displayName).tag(language.rawValue)
                        }
                    }
                    .labelsHidden()
                    .frame(width: RuneSettingsMetrics.rowControlColumnWidth)
                }
            }

            settingsSection(settingsString(.settingsDemoCluster)) {
                settingsToggleRow(
                    settingsString(.settingsShowDemoClusterContext),
                    help: settingsString(.settingsShowDemoClusterContextHelp),
                    isOn: $enableDemoCluster
                )
            }
        }
    }

    private var themesSettingsForm: some View {
        settingsPane(
            title: settingsString(.settingsThemes),
            subtitle: settingsString(.settingsThemesSubtitle)
        ) {
            settingsSection("Choose theme") {
                VStack(alignment: .leading, spacing: 16) {
                    HStack(alignment: .center, spacing: 10) {
                        VStack(alignment: .leading, spacing: 4) {
                            Text("Recent")
                                .font(.subheadline.weight(.semibold))
                            Text("Current: \(selectedAppearanceTheme.title)")
                                .font(.footnote)
                                .foregroundStyle(selectedAppearanceTheme.palette?.secondaryText ?? Color.secondary)
                        }

                        Spacer(minLength: 12)

                        themeOverflowMenu
                    }

                    LazyVGrid(
                        columns: [
                            GridItem(.flexible(minimum: 220), spacing: 12),
                            GridItem(.flexible(minimum: 220), spacing: 12)
                        ],
                        alignment: .leading,
                        spacing: 12
                    ) {
                        ForEach(recentAppearanceThemes) { theme in
                            RuneThemeSelectorCard(
                                theme: theme,
                                isSelected: theme.id == selectedAppearanceTheme.id
                            ) {
                                selectAppearanceTheme(theme.id)
                            }
                        }
                    }
                }
            }

            settingsSection("Custom themes") {
                settingsControlRow(
                    title: "Theme JSON files",
                    detail: "Add compatible theme files to Rune's Themes folder, then reload."
                ) {
                    HStack(spacing: 8) {
                        Button {
                            revealThemeTemplate()
                        } label: {
                            Label("Template", systemImage: "doc.badge.plus")
                        }
                        .buttonStyle(RuneSettingsTokenButtonStyle(theme: selectedAppearanceTheme))
                        .help("Create a starter theme JSON file in Rune's Themes folder. Existing template files are left untouched.")

                        Button("Open Folder") {
                            RuneThemeCatalog.ensureUserThemesDirectory()
                            NSWorkspace.shared.open(RuneThemeCatalog.userThemesDirectoryURL)
                        }
                        .buttonStyle(RuneSettingsTokenButtonStyle(theme: selectedAppearanceTheme))

                        Button("Reload") {
                            reloadAppearanceThemes()
                        }
                        .buttonStyle(RuneSettingsTokenButtonStyle(theme: selectedAppearanceTheme))

                        SettingsHelpButton(
                            text: "Rune can load JSON theme files using supported editor theme color fields. Start with Template, edit the colors, then press Reload."
                        )
                    }
                }

                Text(RuneThemeCatalog.userThemesDirectoryURL.path)
                    .font(.caption.monospaced())
                    .foregroundStyle(selectedAppearanceTheme.palette?.mutedText ?? Color.secondary)
                    .lineLimit(2)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
    }

    private var clampedTerminalFontSize: Double {
        RuneSettingsKeys.clampedTerminalFontSize(terminalFontSize)
    }

    private var selectedAppearanceTheme: RuneResolvedTheme {
        RuneAppearanceTheme.resolved(appearanceThemeRaw)
    }

    private var availableAppearanceThemes: [RuneResolvedTheme] {
        _ = themeReloadNonce
        return RuneThemeCatalog.availableThemes()
    }

    private var recentAppearanceThemes: [RuneResolvedTheme] {
        Self.recentThemesForDisplay(
            selectedThemeID: selectedAppearanceTheme.id,
            recentThemeIDs: recentAppearanceThemeIDs,
            availableThemes: availableAppearanceThemes,
            limit: 4
        )
    }

    private var olderAppearanceThemes: [RuneResolvedTheme] {
        let recentIDs = Set(recentAppearanceThemes.map(\.id))
        return availableAppearanceThemes.filter { !recentIDs.contains($0.id) }
    }

    private var themeOverflowMenu: some View {
        let palette = selectedAppearanceTheme.palette
        return Menu {
            if olderAppearanceThemes.isEmpty {
                Text("No More Themes")
            } else {
                ForEach(olderAppearanceThemes) { theme in
                    Button {
                        selectAppearanceTheme(theme.id)
                    } label: {
                        Label(theme.title, systemImage: RuneThemePresentation(theme: theme).menuSymbol)
                    }
                }
            }
        } label: {
            HStack(spacing: 8) {
                Image(systemName: "paintpalette")
                    .font(.system(size: 13, weight: .semibold))
                Text("More Themes")
                    .lineLimit(1)
                Spacer(minLength: 8)
                Image(systemName: "chevron.up.chevron.down")
                    .font(.caption2.weight(.bold))
                    .imageScale(.small)
                    .foregroundStyle(palette?.secondaryText ?? Color.secondary)
            }
            .font(.subheadline.weight(.medium))
            .foregroundStyle(palette?.foreground ?? Color.primary)
            .padding(.horizontal, 12)
            .frame(height: 34)
            .frame(width: 170)
            .background(
                RoundedRectangle(cornerRadius: 7, style: .continuous)
                    .fill(palette?.inset.opacity(0.96) ?? Color(nsColor: .controlBackgroundColor).opacity(0.72))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 7, style: .continuous)
                    .strokeBorder(palette?.stroke.opacity(0.50) ?? Color(nsColor: .separatorColor).opacity(0.24), lineWidth: 1)
            )
            .contentShape(RoundedRectangle(cornerRadius: 7, style: .continuous))
        }
        .buttonStyle(.plain)
        .accessibilityLabel("More Themes")
    }

    private func selectAppearanceTheme(_ themeID: String) {
        appearanceThemeRaw = themeID
        UserDefaults.standard.recordRuneAppearanceTheme(themeID)
        recentAppearanceThemeIDs = UserDefaults.standard.runeAppearanceRecentThemes
    }

    private func reloadAppearanceThemes() {
        RuneThemeCatalog.reloadUserThemes()
        themeReloadNonce += 1

        let availableIDs = Set(availableAppearanceThemes.map(\.id))
        if !availableIDs.contains(appearanceThemeRaw) {
            selectAppearanceTheme(RuneSettingsKeys.appearanceThemeDefault)
        } else {
            refreshRecentAppearanceThemes(recordSelectedTheme: true)
        }
    }

    private func revealThemeTemplate() {
        let templateURL = RuneThemeCatalog.writeUserThemeTemplate()
        NSWorkspace.shared.activateFileViewerSelecting([templateURL])
    }

    private func refreshRecentAppearanceThemes(recordSelectedTheme: Bool) {
        if recordSelectedTheme {
            UserDefaults.standard.recordRuneAppearanceTheme(selectedAppearanceTheme.id)
        }
        recentAppearanceThemeIDs = UserDefaults.standard.runeAppearanceRecentThemes
    }

    private var interfaceLanguage: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func settingsString(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: interfaceLanguage)
    }

    private static func recentThemesForDisplay(
        selectedThemeID: String,
        recentThemeIDs: [String],
        availableThemes: [RuneResolvedTheme],
        limit: Int
    ) -> [RuneResolvedTheme] {
        guard limit > 0 else { return [] }

        var availableByID: [String: RuneResolvedTheme] = [:]
        for theme in availableThemes where availableByID[theme.id] == nil {
            availableByID[theme.id] = theme
        }
        var orderedIDs = RuneSettingsKeys.normalizedAppearanceRecentThemes([selectedThemeID] + recentThemeIDs, limit: limit)
        for theme in availableThemes where orderedIDs.count < limit && !orderedIDs.contains(theme.id) {
            orderedIDs.append(theme.id)
        }

        return orderedIDs.compactMap { availableByID[$0] }
    }

    private var keyBindingsSettingsForm: some View {
        settingsPane(
            title: settingsString(.settingsKeyBindings),
            subtitle: settingsString(.settingsKeyBindingsSubtitle)
        ) {
            settingsSection("Defaults") {
                Text("These defaults mirror common k9s mnemonics where Rune has an equivalent action. `:`, `/`, `d`, `e`, `l`, `s`, `y`, `Ctrl-D`, and `Shift-F` follow k9s conventions; history actions are Rune mappings built around the same workflow.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)

                Button("Reset to default") {
                    resetKeyBindingShortcuts()
                }
                .buttonStyle(.bordered)
            }

            settingsSection("Actions") {
                ForEach(RuneKeyBindingAction.allCases) { action in
                    VStack(alignment: .leading, spacing: 8) {
                        HStack(alignment: .top, spacing: 12) {
                            VStack(alignment: .leading, spacing: 3) {
                                Text(action.title)
                                    .font(.subheadline.weight(.semibold))
                                Text(action.detail)
                                    .font(.footnote)
                                    .foregroundStyle(.secondary)
                                    .fixedSize(horizontal: false, vertical: true)
                            }

                            Spacer(minLength: 12)

                            Toggle("⌘", isOn: shortcutCommandBinding(for: action))
                                .toggleStyle(.button)
                                .controlSize(.small)
                                .help("Require Command for this action")

                            Toggle("⌥", isOn: shortcutOptionBinding(for: action))
                                .toggleStyle(.button)
                                .controlSize(.small)
                                .help("Require Option for this action")

                            Toggle("⌃", isOn: shortcutControlBinding(for: action))
                                .toggleStyle(.button)
                                .controlSize(.small)
                                .help("Require Control for this action")

                            Toggle("⇧", isOn: shortcutShiftBinding(for: action))
                                .toggleStyle(.button)
                                .controlSize(.small)
                                .help("Require Shift for this action")

                            Picker("Key", selection: shortcutKeyBinding(for: action)) {
                                ForEach(Self.availableShortcutKeys, id: \.self) { key in
                                    Text(Self.displayShortcutKey(key)).tag(key)
                                }
                            }
                            .frame(width: 96)
                        }

                        HStack(spacing: 8) {
                            Text("Current: \(shortcut(for: action).displayValue)")
                                .font(.footnote)
                                .foregroundStyle(.secondary)

                            if let conflict = conflictingAction(for: action) {
                                Text("Conflicts with \(conflict.title)")
                                    .font(.footnote)
                                    .foregroundStyle(.red)
                            }
                        }
                    }

                    if action != RuneKeyBindingAction.allCases.last {
                        Divider()
                    }
                }
            }
        }
    }

    private var logsSettingsForm: some View {
        settingsPane(
            title: settingsString(.settingsLogs),
            subtitle: settingsString(.settingsLogsSubtitle)
        ) {
            settingsSection("Custom log windows") {
                Text("Configure two custom presets shown in log dropdowns.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            customLogPresetSection(
                slot: .one,
                modeRaw: $customOneModeRaw,
                linesRaw: $customOneLinesRaw,
                timeValueRaw: $customOneTimeValueRaw,
                timeUnitRaw: $customOneTimeUnitRaw
            )

            customLogPresetSection(
                slot: .two,
                modeRaw: $customTwoModeRaw,
                linesRaw: $customTwoLinesRaw,
                timeValueRaw: $customTwoTimeValueRaw,
                timeUnitRaw: $customTwoTimeUnitRaw
            )

            settingsSection("Export destination") {
                settingsControlRow(
                    title: "Default export folder",
                    detail: "Used by Save to Export Folder and Save and Open actions."
                ) {
                    HStack(spacing: 8) {
                        Text(exportFolderDisplayName.isEmpty ? "Not configured" : exportFolderDisplayName)
                            .font(.subheadline.weight(.medium))
                            .foregroundStyle(exportFolderDisplayName.isEmpty ? .secondary : .primary)
                            .lineLimit(1)
                            .truncationMode(.middle)
                        Button("Choose…", action: chooseExportFolder)
                        Button("Clear", action: clearExportFolder)
                            .disabled(exportFolderDisplayName.isEmpty)
                    }
                }

                settingsControlRow(
                    title: "Text opener bundle ID",
                    detail: "Optional app bundle identifier for .log and .txt exports."
                ) {
                    HStack(spacing: 8) {
                        TextField("System default", text: $exportTextOpenerBundleIdentifier)
                            .textFieldStyle(.roundedBorder)
                        Button("Choose…") {
                            chooseExportOpener(for: .plainText)
                        }
                    }
                }

                settingsControlRow(
                    title: "Archive opener bundle ID",
                    detail: "Optional app bundle identifier for .zip exports."
                ) {
                    HStack(spacing: 8) {
                        TextField("System default", text: $exportArchiveOpenerBundleIdentifier)
                            .textFieldStyle(.roundedBorder)
                        Button("Choose…") {
                            chooseExportOpener(for: .archive)
                        }
                    }
                }

                exportOpenerRecommendationRow(inklineRecommendation)
                exportOpenerRecommendationRow(quikZipRecommendation)

                settingsToggleRow(
                    "Privacy-safe export filenames",
                    help: "Uses generic generated filenames for configured-folder exports instead of resource-derived names.",
                    isOn: $exportUsesPrivacySafeFilenames
                )
            }
        }
    }

    private var safetySettingsForm: some View {
        settingsPane(
            title: settingsString(.settingsSafety),
            subtitle: settingsString(.settingsSafetySubtitle)
        ) {
            settingsSection("Write safety") {
                settingsToggleRow(
                    "Require server dry-run before YAML apply",
                    help: "Runs Kubernetes server-side validation before applying edited YAML. When off, Apply uses the existing direct write flow.",
                    isOn: $requireApplyDryRun
                )

                settingsToggleRow(
                    "Require copyable command display in confirmations",
                    help: "Shows the equivalent command in write confirmations and keeps the copy command action available.",
                    isOn: $requireCopyableCommand
                )

                settingsToggleRow(
                    "Require production second confirmation",
                    help: "Destructive actions in production-detected contexts require an extra review step before Rune sends the write.",
                    isOn: $requireProductionSecondConfirmation
                )
            }

            settingsSection("Rollback safety") {
                settingsToggleRow(
                    "Require rollout rollback dry-run when supported",
                    help: "Rollout rollback workflows must validate the rollback plan with the Kubernetes API before execution when the API supports it.",
                    isOn: $requireRolloutDryRun
                )

                settingsToggleRow(
                    "Require Helm rollback dry-run when supported",
                    help: "Helm rollback workflows must run a Helm dry-run first when the installed Helm version supports it.",
                    isOn: $requireHelmDryRun
                )

                settingsToggleRow(
                    "Show rollback plan before execution",
                    help: "Displays the target resource, namespace, revision, affected pods when available, and copyable command before rollback.",
                    isOn: $showRollbackPlan
                )

                settingsToggleRow(
                    "Require post-action verification",
                    help: "After write or rollback actions, Rune refreshes and verifies the resulting resource state when supported.",
                    isOn: $requirePostActionVerification
                )
            }
        }
    }

    private var diagnosticsSettingsForm: some View {
        settingsPane(
            title: settingsString(.settingsDiagnostics),
            subtitle: settingsString(.settingsDiagnosticsSubtitle)
        ) {
            settingsSection("Diagnostics logging") {
                settingsToggleRow(
                    "Diagnostics logging",
                    help: "Writes diagnostics to system log and command runner log categories.",
                    isOn: $diagnosticsLogging
                )
            }

            settingsSection("Verbose trace") {
                settingsToggleRow(
                    "Verbose debug trace (file)",
                    help: "Appends detailed Kubernetes load traces to Application Support/Rune/Logs/debug-trace.log.",
                    isOn: $verboseDebugTrace
                )

                Divider()

                VStack(alignment: .leading, spacing: 8) {
                    Text("Log file path")
                        .font(.subheadline.weight(.semibold))
                    Text(DebugTraceWriter.logFileURL.path)
                        .font(.caption.monospaced())
                        .foregroundStyle(.secondary)
                        .textSelection(.enabled)

                    HStack(spacing: 10) {
                        Button("Reveal debug trace in Finder") {
                            revealDebugTraceLogInFinder()
                        }
                        .buttonStyle(.bordered)

                        Button("Clear debug trace log", role: .destructive) {
                            DebugTraceWriter.clear()
                        }
                        .buttonStyle(.bordered)
                    }
                }
            }
        }
    }

    private var performanceSettingsForm: some View {
        settingsPane(
            title: settingsString(.settingsPerformance),
            subtitle: settingsString(.settingsPerformanceSubtitle)
        ) {
            settingsSection("Background prefetch") {
                settingsToggleRow(
                    "Background prefetch other contexts",
                    help: "Allows bounded background warming of overview cache for non-selected contexts.",
                    isOn: $backgroundPrefetchOtherContexts
                )
            }

            settingsSection("Terminal") {
                VStack(alignment: .leading, spacing: 10) {
                    HStack(alignment: .firstTextBaseline, spacing: 10) {
                        Text("Scrollback")
                            .font(.subheadline.weight(.semibold))
                        Spacer(minLength: 12)
                        Text("\(clampedTerminalScrollbackLineLimit) lines")
                            .font(.footnote.monospacedDigit())
                            .foregroundStyle(.secondary)
                    }

                    HStack(spacing: 10) {
                        Stepper(
                            "Terminal scrollback line limit",
                            value: Binding(
                                get: { clampedTerminalScrollbackLineLimit },
                                set: {
                                    terminalScrollbackLineLimit =
                                        RuneSettingsKeys.clampedTerminalScrollbackLineLimit($0)
                                }
                            ),
                            in: RuneSettingsKeys.terminalScrollbackLineLimitMinimum...RuneSettingsKeys.terminalScrollbackLineLimitMaximum,
                            step: 5_000
                        )
                        .labelsHidden()

                        Button("Reset") {
                            terminalScrollbackLineLimit = RuneSettingsKeys.terminalScrollbackLineLimitDefault
                        }
                        .buttonStyle(.bordered)
                    }

                    Text("Keeps recent shell output bounded while preserving the latest context for long-running sessions.")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)

                    Divider()

                    settingsToggleRow(
                        "Save terminal view state",
                        help: "Restores open terminal targets and log tabs on next launch. Transcripts and shell commands are not saved.",
                        isOn: $persistTerminalWorkspaceState
                    )
                }
            }

            settingsSection("Memory") {
                RuneSettingsIntegerLimitEditor(
                    title: "Log cache",
                    value: $sessionLogCacheEntryLimit,
                    valueSuffix: "resources",
                    step: 16,
                    placeholder: "128",
                    defaultValue: RuneSettingsKeys.sessionLogCacheEntryLimitDefault,
                    detail: "Controls how many pod, deployment, and service log reads stay warm for fast switching. Type any larger value for high-memory machines.",
                    normalize: RuneSettingsKeys.clampedSessionLogCacheEntryLimit
                )

                Divider()

                RuneSettingsIntegerLimitEditor(
                    title: "YAML undo",
                    value: $resourceYAMLUndoSnapshotLimit,
                    valueSuffix: "snapshots",
                    step: 8,
                    placeholder: "64",
                    defaultValue: RuneSettingsKeys.resourceYAMLUndoSnapshotLimitDefault,
                    detail: "Controls how many manifest edit states Rune keeps for local undo in the YAML editor. Type any larger value for high-memory machines.",
                    normalize: RuneSettingsKeys.clampedResourceYAMLUndoSnapshotLimit
                )
            }
        }
    }

    private var clampedTerminalScrollbackLineLimit: Int {
        RuneSettingsKeys.clampedTerminalScrollbackLineLimit(terminalScrollbackLineLimit)
    }

    @ViewBuilder
    private func customLogPresetSection(
        slot: RuneCustomLogPresetSlot,
        modeRaw: Binding<String>,
        linesRaw: Binding<String>,
        timeValueRaw: Binding<String>,
        timeUnitRaw: Binding<String>
    ) -> some View {
        let mode = enumBinding(modeRaw, default: RuneCustomLogPresetMode.lines)
        let unit = enumBinding(timeUnitRaw, default: RuneCustomLogPresetTimeUnit.minutes)
        let lines = digitsOnlyBinding(linesRaw)
        let timeValue = digitsOnlyBinding(timeValueRaw)

        settingsSection("Custom \(slot.ordinal)") {
            Picker("Type", selection: mode) {
                ForEach(RuneCustomLogPresetMode.allCases, id: \.rawValue) { mode in
                    Text(mode.title).tag(mode)
                }
            }
            .pickerStyle(.segmented)
            .frame(maxWidth: 260)

            if mode.wrappedValue == .lines {
                LabeledContent("Lines") {
                    TextField("5000", text: lines)
                        .textFieldStyle(.roundedBorder)
                        .frame(maxWidth: 140)
                }
                Text("Digits only. `99999` = since beginning.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            } else {
                LabeledContent("Window") {
                    HStack(spacing: 8) {
                        TextField("15", text: timeValue)
                            .textFieldStyle(.roundedBorder)
                            .frame(maxWidth: 80)
                        Picker("Unit", selection: unit) {
                            ForEach(RuneCustomLogPresetTimeUnit.allCases, id: \.rawValue) { unit in
                                Text(unit.title).tag(unit)
                            }
                        }
                        .labelsHidden()
                        .pickerStyle(.menu)
                    }
                }
                Text("Example: 15 minutes, 6 hours, 2 days.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Text("Dropdown preview: \(UserDefaults.standard.runeCustomLogPresetConfig(slot: slot).title(slot: slot))")
                .font(.footnote)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            helpInline("Shown in all log-window dropdowns.")
        }
    }

    private func chooseExportFolder() {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.canCreateDirectories = true
        panel.allowsMultipleSelection = false
        panel.prompt = "Choose"

        guard panel.runModal() == .OK, let url = panel.url else { return }
        do {
            let bookmark = try url.bookmarkData(
                options: [.withSecurityScope],
                includingResourceValuesForKeys: nil,
                relativeTo: nil
            )
            UserDefaults.standard.runeExportFolderBookmarkData = bookmark
            exportFolderDisplayName = url.lastPathComponent.isEmpty ? url.path : url.lastPathComponent
        } catch {
            UserDefaults.standard.runeExportFolderBookmarkData = nil
            exportFolderDisplayName = ""
        }
    }

    private func clearExportFolder() {
        UserDefaults.standard.runeExportFolderBookmarkData = nil
        exportFolderDisplayName = ""
    }

    private func chooseExportOpener(for kind: ConfiguredExportFileKind) {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        panel.allowsMultipleSelection = false
        panel.allowedContentTypes = [.application]
        panel.prompt = "Choose"

        guard panel.runModal() == .OK,
              let url = panel.url,
              let bundleIdentifier = Bundle(url: url)?.bundleIdentifier else {
            return
        }

        switch kind {
        case .plainText:
            exportTextOpenerBundleIdentifier = bundleIdentifier
        case .archive:
            exportArchiveOpenerBundleIdentifier = bundleIdentifier
        }
    }

    @ViewBuilder
    private func exportOpenerRecommendationRow(_ recommendation: ExportOpenerRecommendation) -> some View {
        if let detected = detectedRecommendedExportOpener(recommendation) {
            settingsControlRow(
                title: "\(detected.appName) detected",
                detail: detected.detail
            ) {
                Button("Use \(detected.appName)") {
                    setExportOpenerBundleIdentifier(detected.bundleIdentifier, for: recommendation.kind)
                }
            }
        }
    }

    private func detectedRecommendedExportOpener(_ recommendation: ExportOpenerRecommendation) -> DetectedExportOpener? {
        guard let appURL = applicationURL(named: recommendation.appName),
              let bundleIdentifier = Bundle(url: appURL)?.bundleIdentifier,
              !bundleIdentifier.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        else { return nil }

        return DetectedExportOpener(
            appName: recommendation.appName,
            bundleIdentifier: bundleIdentifier,
            detail: recommendation.detail(bundleIdentifier: bundleIdentifier)
        )
    }

    private func applicationURL(named appName: String) -> URL? {
        let filename = "\(appName).app"
        let directories = FileManager.default.urls(
            for: .applicationDirectory,
            in: [.userDomainMask, .localDomainMask, .systemDomainMask]
        )
        return directories
            .map { $0.appendingPathComponent(filename, isDirectory: true) }
            .first { FileManager.default.fileExists(atPath: $0.path) }
    }

    private func setExportOpenerBundleIdentifier(_ bundleIdentifier: String, for kind: ConfiguredExportFileKind) {
        switch kind {
        case .plainText:
            exportTextOpenerBundleIdentifier = bundleIdentifier
        case .archive:
            exportArchiveOpenerBundleIdentifier = bundleIdentifier
        }
    }

    @ViewBuilder
    private func settingsPane<Content: View>(
        title: String,
        subtitle: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        ScrollView {
            VStack(alignment: .leading, spacing: RuneSettingsMetrics.pageSpacing) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(title)
                        .font(.title3.weight(.semibold))
                    Text(subtitle)
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }

                content()
            }
            .frame(maxWidth: RuneSettingsMetrics.pageMaxWidth, alignment: .leading)
            .padding(.horizontal, RuneSettingsMetrics.pageHorizontalPadding)
            .padding(.vertical, RuneSettingsMetrics.pageVerticalPadding)
        }
    }

    @ViewBuilder
    private func settingsSection<Content: View>(
        _ title: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            Text(title)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)

            VStack(alignment: .leading, spacing: RuneSettingsMetrics.sectionSpacing) {
                content()
            }
            .runeInsetCard(padding: RuneSettingsMetrics.sectionCardPadding)
        }
    }

    @ViewBuilder
    private func settingsToggleRow(_ title: String, help: String, isOn: Binding<Bool>) -> some View {
        settingsGridRow {
            settingRowLabel(title: title, detail: help, showsHelpIcon: true)
        } control: {
            Toggle(title, isOn: isOn)
                .labelsHidden()
                .toggleStyle(.switch)
        }
    }

    @ViewBuilder
    private func settingsControlRow<Control: View>(
        title: String,
        detail: String? = nil,
        @ViewBuilder control: () -> Control
    ) -> some View {
        settingsGridRow {
            settingRowLabel(title: title, detail: detail)
        } control: {
            control()
        }
    }

    @ViewBuilder
    private func settingsGridRow<LabelContent: View, Control: View>(
        @ViewBuilder label: () -> LabelContent,
        @ViewBuilder control: () -> Control
    ) -> some View {
        HStack(alignment: .top, spacing: RuneSettingsMetrics.rowControlSpacing) {
            label()
                .frame(maxWidth: .infinity, alignment: .leading)

            control()
                .frame(width: RuneSettingsMetrics.rowControlColumnWidth, alignment: .trailing)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .frame(minHeight: RuneSettingsMetrics.rowMinHeight)
    }

    @ViewBuilder
    private func settingRowLabel(title: String, detail: String?, showsHelpIcon: Bool = false) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            HStack(spacing: 6) {
                Text(title)
                    .font(.subheadline.weight(.semibold))
                if showsHelpIcon, let detail {
                    helpIcon(detail)
                }
            }

            if let detail {
                Text(detail)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }

    @ViewBuilder
    private func helpInline(_ text: String) -> some View {
        HStack(spacing: 6) {
            Image(systemName: "questionmark.circle")
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(text)
                .font(.footnote)
                .foregroundStyle(.tertiary)
                .fixedSize(horizontal: false, vertical: true)
        }
    }

    @ViewBuilder
    private func helpIcon(_ text: String) -> some View {
        SettingsHelpButton(text: text)
    }

    private func enumBinding<T: RawRepresentable>(
        _ rawBinding: Binding<String>,
        default defaultValue: T
    ) -> Binding<T> where T.RawValue == String {
        Binding<T>(
            get: {
                T(rawValue: rawBinding.wrappedValue) ?? defaultValue
            },
            set: { newValue in
                rawBinding.wrappedValue = newValue.rawValue
            }
        )
    }

    private func digitsOnlyBinding(_ rawBinding: Binding<String>) -> Binding<String> {
        Binding<String>(
            get: { rawBinding.wrappedValue },
            set: { newValue in
                rawBinding.wrappedValue = newValue.filter(\.isNumber)
            }
        )
    }

    private func revealDebugTraceLogInFinder() {
        let url = DebugTraceWriter.logFileURL
        let parent = url.deletingLastPathComponent()
        if FileManager.default.fileExists(atPath: url.path) {
            NSWorkspace.shared.activateFileViewerSelecting([url])
        } else if FileManager.default.fileExists(atPath: parent.path) {
            NSWorkspace.shared.open(parent)
        } else {
            try? FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
            NSWorkspace.shared.open(parent)
        }
    }

    private func clearDiskCaches() {
        let result = RuneCacheMaintenance.clearDiskCaches()
        NotificationCenter.default.post(name: .runeCachesDidClear, object: nil)
        if result.failedCount == 0 {
            cacheClearStatus = "Cleared \(result.removedCount) cache path\(result.removedCount == 1 ? "" : "s"). Reload view data to repopulate."
        } else {
            cacheClearStatus = "Cleared \(result.removedCount), failed \(result.failedCount). Close and reopen Rune if paths stay locked."
        }
    }

    private func shortcut(for action: RuneKeyBindingAction) -> RuneKeyboardShortcut {
        keyBindingShortcuts[action] ?? action.defaultShortcut
    }

    private func shortcutKeyBinding(for action: RuneKeyBindingAction) -> Binding<String> {
        Binding(
            get: { shortcut(for: action).key },
            set: { newValue in
                updateShortcut(for: action) { current in
                    RuneKeyboardShortcut(
                        key: newValue,
                        requiresShift: current.requiresShift,
                        requiresCommand: current.requiresCommand,
                        requiresOption: current.requiresOption,
                        requiresControl: current.requiresControl
                    ) ?? current
                }
            }
        )
    }

    private func shortcutCommandBinding(for action: RuneKeyBindingAction) -> Binding<Bool> {
        Binding(
            get: { shortcut(for: action).requiresCommand },
            set: { newValue in
                updateShortcut(for: action) { current in
                    RuneKeyboardShortcut(
                        key: current.key,
                        requiresShift: current.requiresShift,
                        requiresCommand: newValue,
                        requiresOption: current.requiresOption,
                        requiresControl: current.requiresControl
                    ) ?? current
                }
            }
        )
    }

    private func shortcutOptionBinding(for action: RuneKeyBindingAction) -> Binding<Bool> {
        Binding(
            get: { shortcut(for: action).requiresOption },
            set: { newValue in
                updateShortcut(for: action) { current in
                    RuneKeyboardShortcut(
                        key: current.key,
                        requiresShift: current.requiresShift,
                        requiresCommand: current.requiresCommand,
                        requiresOption: newValue,
                        requiresControl: current.requiresControl
                    ) ?? current
                }
            }
        )
    }

    private func shortcutControlBinding(for action: RuneKeyBindingAction) -> Binding<Bool> {
        Binding(
            get: { shortcut(for: action).requiresControl },
            set: { newValue in
                updateShortcut(for: action) { current in
                    RuneKeyboardShortcut(
                        key: current.key,
                        requiresShift: current.requiresShift,
                        requiresCommand: current.requiresCommand,
                        requiresOption: current.requiresOption,
                        requiresControl: newValue
                    ) ?? current
                }
            }
        )
    }

    private func shortcutShiftBinding(for action: RuneKeyBindingAction) -> Binding<Bool> {
        Binding(
            get: { shortcut(for: action).requiresShift },
            set: { newValue in
                updateShortcut(for: action) { current in
                    RuneKeyboardShortcut(
                        key: current.key,
                        requiresShift: newValue,
                        requiresCommand: current.requiresCommand,
                        requiresOption: current.requiresOption,
                        requiresControl: current.requiresControl
                    ) ?? current
                }
            }
        )
    }

    private func updateShortcut(
        for action: RuneKeyBindingAction,
        transform: (RuneKeyboardShortcut) -> RuneKeyboardShortcut
    ) {
        let updated = transform(shortcut(for: action))
        keyBindingShortcuts[action] = updated
        UserDefaults.standard.setRuneKeyBindingShortcut(updated, for: action)
    }

    private func conflictingAction(for action: RuneKeyBindingAction) -> RuneKeyBindingAction? {
        let currentShortcut = shortcut(for: action)
        return RuneKeyBindingAction.allCases.first {
            $0 != action && shortcut(for: $0) == currentShortcut
        }
    }

    private func resetKeyBindingShortcuts() {
        UserDefaults.standard.resetRuneKeyBindingShortcuts()
        keyBindingShortcuts = Self.loadKeyBindingShortcuts()
    }

    private static var availableShortcutKeys: [String] {
        Array("abcdefghijklmnopqrstuvwxyz0123456789").map(String.init) + ["[", "]", "/", ":", "?", "left", "right"]
    }

    private static func displayShortcutKey(_ key: String) -> String {
        key.rangeOfCharacter(from: .alphanumerics) != nil ? key.uppercased() : key
    }

    private static func loadKeyBindingShortcuts() -> [RuneKeyBindingAction: RuneKeyboardShortcut] {
        Dictionary(uniqueKeysWithValues: RuneKeyBindingAction.allCases.map {
            ($0, UserDefaults.standard.runeKeyBindingShortcut(for: $0))
        })
    }
}
