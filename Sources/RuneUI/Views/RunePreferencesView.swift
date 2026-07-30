import AppKit
import RuneCore
import RuneDiagnostics
import RuneExport
import RuneStore
import SwiftUI
import UniformTypeIdentifiers

private final class RuneSettingsWindowConfigurationView: NSView {
    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        window?.collectionBehavior.formUnion([.fullScreenAuxiliary, .moveToActiveSpace])
    }
}

private struct RuneSettingsWindowConfigurator: NSViewRepresentable {
    func makeNSView(context: Context) -> NSView {
        RuneSettingsWindowConfigurationView()
    }

    func updateNSView(_ nsView: NSView, context: Context) {}
}

private struct SettingsHelpButton: View {
    let text: String
    @State private var isPopoverPresented = false

    var body: some View {
        RuneIconButton(
            "More info",
            systemImage: "questionmark.circle",
            help: text
        ) {
            isPopoverPresented.toggle()
        }
        .popover(isPresented: $isPopoverPresented, arrowEdge: .top) {
            Text(text)
                .font(.footnote)
                .foregroundStyle(.primary)
                .frame(width: 280, alignment: .leading)
                .padding(10)
                .runePointerCursor()
        }
        .accessibilityHint(text)
    }
}

enum RuneSettingsMetrics {
    static let pageMaxWidth: CGFloat = 760
    static let pageHorizontalPadding: CGFloat = 20
    static let pageVerticalPadding: CGFloat = 16
    static let pageSpacing: CGFloat = 14
    static let sectionSpacing: CGFloat = 12
    static let sectionCardPadding: CGFloat = 14
    static let rowMinHeight: CGFloat = 38
    static let rowControlSpacing: CGFloat = 12
    static let rowControlColumnWidth: CGFloat = 260
    static let rowLabelMinWidth: CGFloat = 240
    static let stackedRowSpacing: CGFloat = 8
    static let compactControlHeight: CGFloat = 32
    static let compactMenuControlWidth: CGFloat = 190
}

private struct RuneSettingsRowLayout: Layout {
    let forceStacked: Bool

    private var minimumHorizontalWidth: CGFloat {
        RuneSettingsMetrics.rowLabelMinWidth
            + RuneSettingsMetrics.rowControlSpacing
            + RuneSettingsMetrics.rowControlColumnWidth
    }

    func sizeThatFits(
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache _: inout ()
    ) -> CGSize {
        guard subviews.count == 2 else { return .zero }
        let width = max(0, proposal.width ?? minimumHorizontalWidth)

        if usesStackedLayout(availableWidth: width) {
            let labelSize = subviews[0].sizeThatFits(
                ProposedViewSize(width: width, height: nil)
            )
            let controlWidth = min(width, RuneSettingsMetrics.rowControlColumnWidth)
            let controlSize = subviews[1].sizeThatFits(
                ProposedViewSize(width: controlWidth, height: nil)
            )
            return CGSize(
                width: width,
                height: max(
                    RuneSettingsMetrics.rowMinHeight,
                    labelSize.height + RuneSettingsMetrics.stackedRowSpacing + controlSize.height
                )
            )
        }

        let labelWidth = max(
            RuneSettingsMetrics.rowLabelMinWidth,
            width - RuneSettingsMetrics.rowControlSpacing - RuneSettingsMetrics.rowControlColumnWidth
        )
        let labelSize = subviews[0].sizeThatFits(
            ProposedViewSize(width: labelWidth, height: nil)
        )
        let controlSize = subviews[1].sizeThatFits(
            ProposedViewSize(width: RuneSettingsMetrics.rowControlColumnWidth, height: nil)
        )
        return CGSize(
            width: width,
            height: max(RuneSettingsMetrics.rowMinHeight, labelSize.height, controlSize.height)
        )
    }

    func placeSubviews(
        in bounds: CGRect,
        proposal _: ProposedViewSize,
        subviews: Subviews,
        cache _: inout ()
    ) {
        guard subviews.count == 2 else { return }

        if usesStackedLayout(availableWidth: bounds.width) {
            let labelSize = subviews[0].sizeThatFits(
                ProposedViewSize(width: bounds.width, height: nil)
            )
            subviews[0].place(
                at: bounds.origin,
                anchor: .topLeading,
                proposal: ProposedViewSize(width: bounds.width, height: labelSize.height)
            )

            let controlWidth = min(bounds.width, RuneSettingsMetrics.rowControlColumnWidth)
            subviews[1].place(
                at: CGPoint(
                    x: bounds.minX,
                    y: bounds.minY + labelSize.height + RuneSettingsMetrics.stackedRowSpacing
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(width: controlWidth, height: nil)
            )
            return
        }

        let labelWidth = max(
            RuneSettingsMetrics.rowLabelMinWidth,
            bounds.width - RuneSettingsMetrics.rowControlSpacing - RuneSettingsMetrics.rowControlColumnWidth
        )
        subviews[0].place(
            at: bounds.origin,
            anchor: .topLeading,
            proposal: ProposedViewSize(width: labelWidth, height: nil)
        )
        subviews[1].place(
            at: CGPoint(x: bounds.maxX, y: bounds.minY),
            anchor: .topTrailing,
            proposal: ProposedViewSize(width: RuneSettingsMetrics.rowControlColumnWidth, height: nil)
        )
    }

    private func usesStackedLayout(availableWidth: CGFloat) -> Bool {
        forceStacked || availableWidth < minimumHorizontalWidth
    }
}

struct RuneSettingsAdaptiveRow<LabelContent: View, Control: View>: View {
    @ViewBuilder let labelContent: LabelContent
    @ViewBuilder let control: Control
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    init(
        @ViewBuilder label: () -> LabelContent,
        @ViewBuilder control: () -> Control
    ) {
        self.labelContent = label()
        self.control = control()
    }

    var body: some View {
        RuneSettingsRowLayout(forceStacked: dynamicTypeSize.isAccessibilitySize) {
            labelContent
                .frame(
                    minWidth: RuneSettingsMetrics.rowLabelMinWidth,
                    maxWidth: .infinity,
                    alignment: .leading
                )

            control
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .frame(minHeight: RuneSettingsMetrics.rowMinHeight)
    }
}

private struct ExportOpenerRecommendation {
    let appName: String
    let kind: ConfiguredExportFileKind
}

private struct DetectedExportOpener {
    let appName: String
    let bundleIdentifier: String
}

struct RuneSettingsMenuLabel: View {
    let title: String
    let systemImage: String
    var subtitle: String? = nil

    var body: some View {
        HStack(spacing: 9) {
            Image(systemName: systemImage)
                .runeInterfaceFont(weight: .semibold)
                .foregroundStyle(.secondary)
                .frame(width: 18)

            VStack(alignment: .leading, spacing: 1) {
                Text(title)
                    .runeInterfaceFont(weight: .medium)
                    .foregroundStyle(.primary)
                    .lineLimit(1)
                    .truncationMode(.middle)

                if let subtitle {
                    Text(subtitle)
                        .runeInterfaceFont(relativeSize: -2)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
            }

            Spacer(minLength: 8)

            Image(systemName: "chevron.up.chevron.down")
                .runeInterfaceFont(relativeSize: -3, weight: .bold)
                .foregroundStyle(.secondary)
        }
        .padding(.horizontal, 10)
        .frame(minHeight: RuneSettingsMetrics.compactControlHeight)
        .frame(maxWidth: .infinity)
        .background(
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .fill(Color(nsColor: .controlBackgroundColor).opacity(0.72))
        )
        .overlay(
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.28), lineWidth: 1)
        )
        .contentShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
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
                            .lineLimit(2)
                            .fixedSize(horizontal: false, vertical: true)
                        Text(presentation.sourceSummary)
                            .font(.caption)
                            .foregroundStyle(palette.secondaryText)
                            .lineLimit(1)
                    }

                    Spacer(minLength: 8)

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
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                    .fill(isSelected ? palette.selectionFill : palette.inset.opacity(0.82))
            )
            .overlay(
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                    .strokeBorder(
                        isSelected ? palette.focusRing : palette.stroke.opacity(0.44),
                        lineWidth: isSelected ? 1.5 : 1
                    )
            )
            .contentShape(
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
            )
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

struct RuneSettingsIntegerLimitEditor: View {
    let title: String
    let value: Binding<Int>
    let valueSuffix: String
    let step: Int
    let placeholder: String
    let defaultValue: Int
    let detail: String
    let normalize: (Int) -> Int

    var body: some View {
        RuneSettingsAdaptiveRow {
            VStack(alignment: .leading, spacing: 3) {
                Text(title)
                    .runeInterfaceFont(weight: .semibold)
                Text(detail)
                    .runeInterfaceFont(relativeSize: -1)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        } control: {
            HStack(spacing: 8) {
                TextField(placeholder, value: normalizedBinding, format: .number)
                    .textFieldStyle(.roundedBorder)
                    .runeInterfaceFont(design: .monospaced)
                    .multilineTextAlignment(.trailing)
                    .frame(width: 76)
                    .accessibilityLabel("\(title) value")

                Text(valueSuffix)
                    .runeInterfaceFont(relativeSize: -1)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)

                Spacer(minLength: 0)

                Stepper("Adjust \(title)", value: normalizedBinding, step: step)
                    .labelsHidden()
                    .help("Adjust \(title) by \(step). Current value: \(normalizedValue) \(valueSuffix).")

                Button("Reset") {
                    value.wrappedValue = defaultValue
                }
                .buttonStyle(.bordered)
                .help("Restore \(title) to \(defaultValue) \(valueSuffix).")
            }
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

    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @State private var selectedPane: PreferencesPane = .general
    @AppStorage(RuneSettingsKeys.persistNamespaceListCache) private var persistNamespaceListCache = true
    @AppStorage(RuneSettingsKeys.diagnosticsLogging) private var diagnosticsLogging = true
    @AppStorage(RuneSettingsKeys.verboseDebugTrace) private var verboseDebugTrace = false
    @AppStorage(RuneSettingsKeys.backgroundPrefetchOtherContexts) private var backgroundPrefetchOtherContexts = false
    @AppStorage(RuneSettingsKeys.enableDemoCluster) private var enableDemoCluster = true
    @AppStorage(RuneSettingsKeys.skipClusterOnTabNavigationFromSections) private var skipClusterOnTabNavigationFromSections = false
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
    @AppStorage(RuneSettingsKeys.showResourceTableScrollEdgeGlow) private var showResourceTableScrollEdgeGlow = true
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
    @AppStorage(RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette) private var showDestructivePaletteCommands = false
    @State private var cacheClearStatus: String?
    @State private var themeReloadNonce = 0
    @State private var recentAppearanceThemeIDs = UserDefaults.standard.runeAppearanceRecentThemes
    @State private var keyBindingShortcuts = Self.loadKeyBindingShortcuts()
    @State private var detectedTextExportOpener: DetectedExportOpener?
    @State private var detectedArchiveExportOpener: DetectedExportOpener?

    public init() {}

    public var body: some View {
        TabView(selection: $selectedPane) {
            generalSettingsForm
                .tag(PreferencesPane.general)
                .tabItem {
                    Label(PreferencesPane.general.title(settingsString), systemImage: PreferencesPane.general.symbol)
                        .runeInterfaceFont(weight: .medium)
                }

            themesSettingsForm
                .tag(PreferencesPane.themes)
                .tabItem {
                    Label(PreferencesPane.themes.title(settingsString), systemImage: PreferencesPane.themes.symbol)
                        .runeInterfaceFont(weight: .medium)
                }

            keyBindingsSettingsForm
                .tag(PreferencesPane.keyBindings)
                .tabItem {
                    Label(PreferencesPane.keyBindings.title(settingsString), systemImage: PreferencesPane.keyBindings.symbol)
                        .runeInterfaceFont(weight: .medium)
                }

            logsSettingsForm
                .tag(PreferencesPane.logs)
                .tabItem {
                    Label(PreferencesPane.logs.title(settingsString), systemImage: PreferencesPane.logs.symbol)
                        .runeInterfaceFont(weight: .medium)
                }

            safetySettingsForm
                .tag(PreferencesPane.safety)
                .tabItem {
                    Label(PreferencesPane.safety.title(settingsString), systemImage: PreferencesPane.safety.symbol)
                        .runeInterfaceFont(weight: .medium)
                }

            diagnosticsSettingsForm
                .tag(PreferencesPane.diagnostics)
                .tabItem {
                    Label(PreferencesPane.diagnostics.title(settingsString), systemImage: PreferencesPane.diagnostics.symbol)
                        .runeInterfaceFont(weight: .medium)
                }

            performanceSettingsForm
                .tag(PreferencesPane.performance)
                .tabItem {
                    Label(PreferencesPane.performance.title(settingsString), systemImage: PreferencesPane.performance.symbol)
                        .runeInterfaceFont(weight: .medium)
                }
        }
        .id(interfaceLanguageRaw)
        .frame(minWidth: 740, idealWidth: 820, minHeight: 540)
        .background(RuneSettingsWindowConfigurator())
        .runeInterfaceTypography(
            configuredFontSize: terminalFontSize,
            systemDynamicTypeSize: dynamicTypeSize
        )
        .runeAppearanceTheme(selectedAppearanceTheme)
        .onAppear {
            refreshRecentAppearanceThemes(recordSelectedTheme: true)
        }
        .onChange(of: appearanceThemeRaw) { _, _ in
            refreshRecentAppearanceThemes(recordSelectedTheme: true)
        }
        .onChange(of: selectedPane) { _, pane in
            if pane == .logs {
                refreshDetectedExportOpeners()
            }
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

                settingsControlRow(
                    title: settingsString(.settingsMaintenance),
                    detail: cacheClearStatus ?? "Remove locally cached cluster data without changing cluster resources."
                ) {
                    Button(settingsString(.settingsClearCachedClusterData), role: .destructive) {
                        clearDiskCaches()
                    }
                    .buttonStyle(.bordered)
                }
            }

            settingsSection(settingsString(.settingsAppearance)) {
                settingsControlRow(
                    title: settingsString(.settingsFontSize),
                    detail: settingsString(.settingsFontSizeDetail)
                ) {
                    HStack(spacing: 8) {
                        Slider(
                            value: Binding(
                                get: { clampedTerminalFontSize },
                                set: { terminalFontSize = RuneSettingsKeys.clampedTerminalFontSize($0) }
                            ),
                            in: RuneSettingsKeys.terminalFontSizeMinimum...RuneSettingsKeys.terminalFontSizeMaximum,
                            step: 1
                        )
                        .accessibilityLabel(settingsString(.settingsFontSize))

                        Text("\(Int(clampedTerminalFontSize.rounded())) pt")
                            .font(.footnote.monospacedDigit())
                            .foregroundStyle(.secondary)
                            .frame(width: 38, alignment: .trailing)

                        Button(settingsString(.settingsReset)) {
                            terminalFontSize = RuneSettingsKeys.terminalFontSizeDefault
                        }
                        .buttonStyle(.bordered)
                    }
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

                settingsToggleRow(
                    settingsString(.settingsShowResourceTableScrollEdgeGlow),
                    help: settingsString(.settingsShowResourceTableScrollEdgeGlowHelp),
                    isOn: $showResourceTableScrollEdgeGlow
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
                    .pickerStyle(.menu)
                    .runeInterfaceFont(weight: .medium)
                    .runeInterfaceControlSize()
                    .frame(
                        width: RuneSettingsMetrics.compactMenuControlWidth,
                        alignment: .trailing
                    )
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
                    settingsGridRow {
                        VStack(alignment: .leading, spacing: 4) {
                            Text("Recent")
                                .runeInterfaceFont(weight: .semibold)
                            Text("Current: \(selectedAppearanceTheme.title)")
                                .runeInterfaceFont(relativeSize: -1)
                                .foregroundStyle(
                                    selectedAppearanceTheme.palette?.secondaryText ?? Color.secondary
                                )
                        }
                    } control: {
                        themeOverflowMenu
                    }

                    LazyVGrid(
                        columns: themeGridColumns,
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
                settingsGridRow {
                    VStack(alignment: .leading, spacing: 3) {
                        Text("Theme JSON files")
                            .runeInterfaceFont(weight: .semibold)
                        Text("Create or add compatible theme files, then reload.")
                            .runeInterfaceFont(relativeSize: -1)
                            .foregroundStyle(.secondary)
                            .fixedSize(horizontal: false, vertical: true)
                        Text(displayThemesDirectoryPath)
                            .runeInterfaceFont(relativeSize: -2, design: .monospaced)
                            .foregroundStyle(.tertiary)
                            .lineLimit(1)
                            .truncationMode(.middle)
                            .textSelection(.enabled)
                    }
                } control: {
                    customThemeActions
                }
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

    private var themeGridColumns: [GridItem] {
        if dynamicTypeSize.isAccessibilitySize {
            return [GridItem(.flexible(minimum: 220), spacing: 12)]
        }
        return [
            GridItem(.flexible(minimum: 220), spacing: 12),
            GridItem(.flexible(minimum: 220), spacing: 12)
        ]
    }

    private var themeOverflowMenu: some View {
        Menu {
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
            RuneSettingsMenuLabel(
                title: "More Themes",
                systemImage: "paintpalette"
            )
            .frame(width: RuneSettingsMetrics.compactMenuControlWidth)
        }
        .buttonStyle(.plain)
        .accessibilityLabel("More Themes")
    }

    @ViewBuilder
    private var customThemeActions: some View {
        if dynamicTypeSize.isAccessibilitySize {
            HStack(spacing: 8) {
                createThemeTemplateButton
                customThemeMoreMenu
                SettingsHelpButton(text: customThemesHelpText)
            }
            .frame(maxWidth: .infinity, alignment: .trailing)
        } else {
            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 8) {
                    createThemeTemplateButton
                        .frame(maxWidth: .infinity)
                    openThemesFolderButton
                        .frame(maxWidth: .infinity)
                }

                HStack(spacing: 8) {
                    Spacer(minLength: 8)
                    reloadThemesButton
                    SettingsHelpButton(text: customThemesHelpText)
                }
            }
            .frame(maxWidth: .infinity)
        }
    }

    private var createThemeTemplateButton: some View {
        Button {
            revealThemeTemplate()
        } label: {
            Label("New Theme", systemImage: "doc.badge.plus")
                .frame(maxWidth: .infinity)
        }
        .buttonStyle(.bordered)
        .accessibilityLabel("Create theme template")
        .help("Create a starter theme JSON file. Existing template files are left untouched.")
    }

    private var openThemesFolderButton: some View {
        Button {
            openThemesFolder()
        } label: {
            Label("Open Folder", systemImage: "folder")
                .frame(maxWidth: .infinity)
        }
        .buttonStyle(.bordered)
        .help("Open Rune's custom theme folder.")
    }

    private var reloadThemesButton: some View {
        Button {
            reloadAppearanceThemes()
        } label: {
            Label("Reload", systemImage: "arrow.clockwise")
        }
        .buttonStyle(.bordered)
        .help("Reload custom theme files from disk.")
    }

    private var customThemeMoreMenu: some View {
        Menu {
            Button {
                openThemesFolder()
            } label: {
                Label("Open Theme Folder", systemImage: "folder")
            }

            Button {
                reloadAppearanceThemes()
            } label: {
                Label("Reload Themes", systemImage: "arrow.clockwise")
            }
        } label: {
            Label("More", systemImage: "ellipsis.circle")
        }
        .buttonStyle(.bordered)
        .accessibilityLabel("More custom theme actions")
    }

    private var customThemesHelpText: String {
        "Rune can load JSON theme files using supported editor theme color fields. Create a template, edit the colors, then reload."
    }

    private var displayThemesDirectoryPath: String {
        let path = RuneThemeCatalog.userThemesDirectoryURL.path
        let homePath = FileManager.default.homeDirectoryForCurrentUser.path
        guard path.hasPrefix(homePath) else { return path }
        return "~" + String(path.dropFirst(homePath.count))
    }

    private func openThemesFolder() {
        RuneThemeCatalog.ensureUserThemesDirectory()
        NSWorkspace.shared.open(RuneThemeCatalog.userThemesDirectoryURL)
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
                settingsControlRow(
                    title: "Default shortcuts",
                    detail: "Mirrors common k9s mnemonics where Rune has an equivalent action. Command mode stays logical across keyboard layouts."
                ) {
                    Button("Reset to Default") {
                        resetKeyBindingShortcuts()
                    }
                    .buttonStyle(.bordered)
                }
            }

            settingsSection("Navigation") {
                settingsToggleRow(
                    "Skip Cluster on Tab navigation from Sections",
                    help: "Tab cycles Sections → middle panel → right panel → Sections without focusing the cluster list.",
                    isOn: $skipClusterOnTabNavigationFromSections
                )
            }

            settingsSection("Actions") {
                ForEach(RuneKeyBindingAction.allCases) { action in
                    settingsGridRow {
                        VStack(alignment: .leading, spacing: 3) {
                            Text(action.title)
                                .font(.subheadline.weight(.semibold))
                            Text(action.detail)
                                .font(.footnote)
                                .foregroundStyle(.secondary)
                                .fixedSize(horizontal: false, vertical: true)

                            HStack(spacing: 8) {
                                Text("Current: \(shortcut(for: action).displayValue)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)

                                if let conflict = conflictingAction(for: action) {
                                    Text("Conflicts with \(conflict.title)")
                                        .font(.caption)
                                        .foregroundStyle(.red)
                                }
                            }
                        }
                    } control: {
                        HStack(spacing: 6) {
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
                            .runeInterfaceFont(weight: .medium)
                            .runeInterfaceControlSize()
                            .frame(width: 96)
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
                customLogPresetRow(
                    slot: .one,
                    modeRaw: $customOneModeRaw,
                    linesRaw: $customOneLinesRaw,
                    timeValueRaw: $customOneTimeValueRaw,
                    timeUnitRaw: $customOneTimeUnitRaw
                )

                Divider()

                customLogPresetRow(
                    slot: .two,
                    modeRaw: $customTwoModeRaw,
                    linesRaw: $customTwoLinesRaw,
                    timeValueRaw: $customTwoTimeValueRaw,
                    timeUnitRaw: $customTwoTimeUnitRaw
                )
            }

            settingsSection("Export destination") {
                settingsControlRow(
                    title: "Default export folder",
                    detail: "Used by Save to Export Folder and Save and Open actions."
                ) {
                    exportFolderMenu
                }

                Divider()

                settingsControlRow(
                    title: "Text exports",
                    detail: "Choose which app opens .log, .txt, .yaml, .json, and terminal transcript exports."
                ) {
                    exportOpenerMenu(inklineRecommendation)
                }

                Divider()

                settingsControlRow(
                    title: "Archive exports",
                    detail: "Choose which app opens .zip log and transcript archives."
                ) {
                    exportOpenerMenu(quikZipRecommendation)
                }

                Divider()

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

                settingsToggleRow(
                    "Palette destructive commands",
                    help: "Opt-in only. Shows selected-resource destructive commands such as Delete in Command Palette. Running one only opens Rune's write confirmation; it never writes directly.",
                    isOn: $showDestructivePaletteCommands
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

                settingsGridRow {
                    VStack(alignment: .leading, spacing: 3) {
                        Text("Log file")
                            .font(.subheadline.weight(.semibold))
                        Text(DebugTraceWriter.logFileURL.path)
                            .font(.caption.monospaced())
                            .foregroundStyle(.secondary)
                            .lineLimit(2)
                            .truncationMode(.middle)
                            .textSelection(.enabled)
                    }
                } control: {
                    debugTraceManagementMenu
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
                RuneSettingsIntegerLimitEditor(
                    title: "Scrollback",
                    value: $terminalScrollbackLineLimit,
                    valueSuffix: "lines",
                    step: 5_000,
                    placeholder: "60000",
                    defaultValue: RuneSettingsKeys.terminalScrollbackLineLimitDefault,
                    detail: "Keeps recent shell output bounded while preserving the latest context for long-running sessions.",
                    normalize: RuneSettingsKeys.clampedTerminalScrollbackLineLimit
                )

                Divider()

                settingsToggleRow(
                    "Save terminal view state",
                    help: "Restores open terminal targets and log tabs on next launch. Transcripts and shell commands are not saved.",
                    isOn: $persistTerminalWorkspaceState
                )
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

    @ViewBuilder
    private func customLogPresetRow(
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
        let summary = customLogPresetSummary(
            mode: mode.wrappedValue,
            lines: Int(lines.wrappedValue) ?? 1,
            timeValue: Int(timeValue.wrappedValue) ?? 1,
            timeUnit: unit.wrappedValue
        )

        settingsGridRow {
            settingRowLabel(
                title: "Custom \(slot.ordinal)",
                detail: "\(summary). Shown in every log-window menu."
            )
        } control: {
            HStack(spacing: 8) {
                Picker("Type", selection: mode) {
                    ForEach(RuneCustomLogPresetMode.allCases, id: \.rawValue) { mode in
                        Text(mode == .lines ? "Lines" : "Time").tag(mode)
                    }
                }
                .labelsHidden()
                .pickerStyle(.menu)
                .runeInterfaceFont(weight: .medium)
                .runeInterfaceControlSize()
                .frame(width: 80)

                if mode.wrappedValue == .lines {
                    TextField("5000", text: lines)
                        .textFieldStyle(.roundedBorder)
                        .font(.body.monospacedDigit())
                        .multilineTextAlignment(.trailing)
                        .frame(width: 72)
                        .help("Digits only. Enter 99999 to load logs since the beginning.")

                    Text("lines")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                } else {
                    TextField("15", text: timeValue)
                        .textFieldStyle(.roundedBorder)
                        .font(.body.monospacedDigit())
                        .multilineTextAlignment(.trailing)
                        .frame(width: 54)

                    Picker("Unit", selection: unit) {
                        ForEach(RuneCustomLogPresetTimeUnit.allCases, id: \.rawValue) { unit in
                            Text(unit.title).tag(unit)
                        }
                    }
                    .labelsHidden()
                    .pickerStyle(.menu)
                    .runeInterfaceFont(weight: .medium)
                    .runeInterfaceControlSize()
                    .frame(maxWidth: 104)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func customLogPresetSummary(
        mode: RuneCustomLogPresetMode,
        lines: Int,
        timeValue: Int,
        timeUnit: RuneCustomLogPresetTimeUnit
    ) -> String {
        switch mode {
        case .lines:
            let normalizedLines = max(1, lines)
            return normalizedLines >= 99_999 ? "Since beginning" : "\(normalizedLines) lines"
        case .time:
            return "Last \(timeUnit.shortTitle(amount: timeValue))"
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

    private var exportFolderMenu: some View {
        Menu {
            Button {
                chooseExportFolder()
            } label: {
                Label("Choose Folder…", systemImage: "folder.badge.plus")
            }

            if !exportFolderDisplayName.isEmpty {
                Divider()
                Button("Clear Folder", role: .destructive) {
                    clearExportFolder()
                }
            }
        } label: {
            RuneSettingsMenuLabel(
                title: exportFolderDisplayName.isEmpty ? "Not configured" : exportFolderDisplayName,
                systemImage: "folder"
            )
        }
        .buttonStyle(.plain)
        .frame(maxWidth: .infinity)
        .accessibilityLabel("Default export folder")
        .accessibilityValue(exportFolderDisplayName.isEmpty ? "Not configured" : exportFolderDisplayName)
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
    private func exportOpenerMenu(_ recommendation: ExportOpenerRecommendation) -> some View {
        let selectedBundleIdentifier = exportOpenerBundleIdentifier(for: recommendation.kind)
        let detected = detectedExportOpener(for: recommendation.kind)

        Menu {
            Button {
                setExportOpenerBundleIdentifier("", for: recommendation.kind)
            } label: {
                Label(
                    "System Default",
                    systemImage: selectedBundleIdentifier.isEmpty ? "checkmark.circle.fill" : "macwindow"
                )
            }

            if let detected {
                Button {
                    setExportOpenerBundleIdentifier(detected.bundleIdentifier, for: recommendation.kind)
                } label: {
                    Label(
                        detected.appName,
                        systemImage: selectedBundleIdentifier == detected.bundleIdentifier
                            ? "checkmark.circle.fill"
                            : "app"
                    )
                }
            }

            Divider()

            Button {
                chooseExportOpener(for: recommendation.kind)
            } label: {
                Label("Choose Application…", systemImage: "plus.app")
            }
        } label: {
            RuneSettingsMenuLabel(
                title: exportOpenerDisplayName(for: recommendation),
                systemImage: recommendation.kind == .plainText ? "doc.text" : "archivebox"
            )
        }
        .buttonStyle(.plain)
        .frame(maxWidth: .infinity)
        .accessibilityLabel(recommendation.kind == .plainText ? "Text export opener" : "Archive export opener")
        .accessibilityValue(exportOpenerDisplayName(for: recommendation))
    }

    private func refreshDetectedExportOpeners() {
        detectedTextExportOpener = discoverRecommendedExportOpener(inklineRecommendation)
        detectedArchiveExportOpener = discoverRecommendedExportOpener(quikZipRecommendation)
    }

    private func discoverRecommendedExportOpener(
        _ recommendation: ExportOpenerRecommendation
    ) -> DetectedExportOpener? {
        if let appURL = applicationURL(named: recommendation.appName),
           let detectedBundleIdentifier = Bundle(url: appURL)?.bundleIdentifier,
           !detectedBundleIdentifier.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return DetectedExportOpener(
                appName: recommendation.appName,
                bundleIdentifier: detectedBundleIdentifier
            )
        }
        return nil
    }

    private func detectedExportOpener(for kind: ConfiguredExportFileKind) -> DetectedExportOpener? {
        switch kind {
        case .plainText:
            return detectedTextExportOpener
        case .archive:
            return detectedArchiveExportOpener
        }
    }

    private func exportOpenerBundleIdentifier(for kind: ConfiguredExportFileKind) -> String {
        switch kind {
        case .plainText:
            return exportTextOpenerBundleIdentifier.trimmingCharacters(in: .whitespacesAndNewlines)
        case .archive:
            return exportArchiveOpenerBundleIdentifier.trimmingCharacters(in: .whitespacesAndNewlines)
        }
    }

    private func exportOpenerDisplayName(for recommendation: ExportOpenerRecommendation) -> String {
        let bundleIdentifier = exportOpenerBundleIdentifier(for: recommendation.kind)
        guard !bundleIdentifier.isEmpty else { return "System Default" }

        if let detected = detectedExportOpener(for: recommendation.kind),
           detected.bundleIdentifier == bundleIdentifier {
            return detected.appName
        }

        if let appURL = NSWorkspace.shared.urlForApplication(withBundleIdentifier: bundleIdentifier) {
            return appURL.deletingPathExtension().lastPathComponent
        }

        return "Selected application"
    }

    private func applicationURL(named appName: String) -> URL? {
        let filename = "\(appName).app"
        let directories = FileManager.default.urls(
            for: .applicationDirectory,
            in: [.userDomainMask, .localDomainMask, .systemDomainMask]
        )
        let applicationCandidates = directories.map { $0.appendingPathComponent(filename, isDirectory: true) }
        let workspaceCandidates = workspaceApplicationURLs(named: appName, filename: filename)
        return (applicationCandidates + workspaceCandidates)
            .first { FileManager.default.fileExists(atPath: $0.path) }
    }

    private func workspaceApplicationURLs(named appName: String, filename: String) -> [URL] {
        let workspaceURL = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Workspace", isDirectory: true)
        let projectURL = workspaceURL.appendingPathComponent(appName, isDirectory: true)
        return [
            projectURL.appendingPathComponent(filename, isDirectory: true),
            projectURL
                .appendingPathComponent(".local", isDirectory: true)
                .appendingPathComponent("build", isDirectory: true)
                .appendingPathComponent(filename, isDirectory: true),
            projectURL
                .appendingPathComponent(".local", isDirectory: true)
                .appendingPathComponent("test-app", isDirectory: true)
                .appendingPathComponent(filename, isDirectory: true),
            projectURL
                .appendingPathComponent(".build", isDirectory: true)
                .appendingPathComponent("debug", isDirectory: true)
                .appendingPathComponent(filename, isDirectory: true),
            projectURL
                .appendingPathComponent(".build", isDirectory: true)
                .appendingPathComponent("arm64-apple-macosx", isDirectory: true)
                .appendingPathComponent("debug", isDirectory: true)
                .appendingPathComponent(filename, isDirectory: true)
        ]
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
                        .runeInterfaceFont(relativeSize: 4, weight: .semibold)
                    Text(subtitle)
                        .runeInterfaceFont(relativeSize: -1)
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
                .runeInterfaceFont(weight: .semibold)
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
            settingRowLabel(title: title, detail: help)
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
        RuneSettingsAdaptiveRow(label: label, control: control)
    }

    @ViewBuilder
    private func settingRowLabel(title: String, detail: String?) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(title)
                .runeInterfaceFont(weight: .semibold)

            if let detail {
                Text(detail)
                    .runeInterfaceFont(relativeSize: -1)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
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

    private var debugTraceManagementMenu: some View {
        Menu {
            Button {
                revealDebugTraceLogInFinder()
            } label: {
                Label("Reveal in Finder", systemImage: "folder")
            }

            Divider()

            Button("Clear Debug Trace", role: .destructive) {
                DebugTraceWriter.clear()
            }
        } label: {
            RuneSettingsMenuLabel(
                title: "Manage Log",
                systemImage: "doc.text.magnifyingglass"
            )
        }
        .buttonStyle(.plain)
        .frame(maxWidth: .infinity)
        .accessibilityLabel("Manage debug trace log")
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
        let currentShortcuts = Set([shortcut(for: action)] + action.alternateShortcuts)
        return RuneKeyBindingAction.allCases.first {
            guard $0 != action else { return false }
            let candidateShortcuts = Set([shortcut(for: $0)] + $0.alternateShortcuts)
            return !currentShortcuts.isDisjoint(with: candidateShortcuts)
        }
    }

    private func resetKeyBindingShortcuts() {
        UserDefaults.standard.resetRuneKeyBindingShortcuts()
        keyBindingShortcuts = Self.loadKeyBindingShortcuts()
    }

    private static var availableShortcutKeys: [String] {
        Array("abcdefghijklmnopqrstuvwxyz0123456789").map(String.init) + ["[", "]", "/", ".", ":", "?", "left", "right"]
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
