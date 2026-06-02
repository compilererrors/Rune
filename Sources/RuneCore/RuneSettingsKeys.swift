import Foundation

/// UserDefaults keys for in-app preferences (Settings, gear menu).
public enum RuneSettingsKeys {
    /// When true, Rune restores the last namespace list from disk on launch and saves it again after a successful namespace refresh.
    public static let persistNamespaceListCache = "rune.settings.persistNamespaceListCache"
    /// When false, `DiagnosticsRecorder` does not emit unified logging lines.
    public static let diagnosticsLogging = "rune.settings.diagnosticsLogging"
    /// When true, `DiagnosticsRecorder.trace` appends detailed lines to `Rune/Logs/debug-trace.log` under Application Support.
    public static let verboseDebugTrace = "rune.settings.verboseDebugTrace"
    /// When true, after the active context snapshot finishes, Rune may warm overview cache for a few other contexts in the background (bounded).
    public static let backgroundPrefetchOtherContexts = "rune.settings.backgroundPrefetchOtherContexts"
    /// When true, Rune exposes a small in-memory demo cluster context and menu action for screenshots and first-run evaluation.
    public static let enableDemoCluster = "rune.settings.enableDemoCluster"
    public static let logsCustomPresetOneMode = "rune.settings.logs.customPresetOne.mode"
    public static let logsCustomPresetOneLines = "rune.settings.logs.customPresetOne.lines"
    public static let logsCustomPresetOneTimeValue = "rune.settings.logs.customPresetOne.timeValue"
    public static let logsCustomPresetOneTimeUnit = "rune.settings.logs.customPresetOne.timeUnit"
    public static let logsCustomPresetTwoMode = "rune.settings.logs.customPresetTwo.mode"
    public static let logsCustomPresetTwoLines = "rune.settings.logs.customPresetTwo.lines"
    public static let logsCustomPresetTwoTimeValue = "rune.settings.logs.customPresetTwo.timeValue"
    public static let logsCustomPresetTwoTimeUnit = "rune.settings.logs.customPresetTwo.timeUnit"
    public static let keyBindingCommandPalette = "rune.settings.keybindings.commandPalette"
    public static let keyBindingFilterResources = "rune.settings.keybindings.filterResources"
    public static let keyBindingDescribe = "rune.settings.keybindings.describe"
    public static let keyBindingHistoryBack = "rune.settings.keybindings.historyBack"
    public static let keyBindingHistoryForward = "rune.settings.keybindings.historyForward"
    public static let keyBindingLogs = "rune.settings.keybindings.logs"
    public static let keyBindingSaveLogs = "rune.settings.keybindings.saveLogs"
    public static let keyBindingShell = "rune.settings.keybindings.shell"
    public static let keyBindingEdit = "rune.settings.keybindings.edit"
    public static let keyBindingYAML = "rune.settings.keybindings.yaml"
    public static let keyBindingDelete = "rune.settings.keybindings.delete"
    public static let keyBindingPortForward = "rune.settings.keybindings.portForward"
    public static let keyBindingRollout = "rune.settings.keybindings.rollout"
    /// Persisted sidebar width in the 3-column shell.
    public static let layoutSidebarWidth = "rune.settings.layout.sidebarWidth"
    /// Persisted detail/inspector width in the 3-column shell.
    public static let layoutDetailWidth = "rune.settings.layout.detailWidth"
    /// Persisted sidebar visibility in the 3-column shell.
    public static let layoutSidebarVisible = "rune.settings.layout.sidebarVisible"
    /// Persisted detail/inspector visibility in the 3-column shell.
    public static let layoutDetailPaneVisible = "rune.settings.layout.detailPaneVisible"
    /// Persisted pod table name column width in the resource list.
    public static let layoutPodNameColumnWidth = "rune.settings.layout.podNameColumnWidth"
    /// Base font size used by Rune's interface and monospaced terminal/editor surfaces.
    public static let terminalFontSize = "rune.settings.terminal.fontSize"
    public static let terminalFontSizeDefault = 12.0
    public static let terminalFontSizeMinimum = 10.0
    public static let terminalFontSizeMaximum = 20.0
    /// When true, describe and YAML inspector surfaces hide Kubernetes managedFields by default.
    public static let hideManagedFieldsByDefault = "rune.settings.appearance.hideManagedFieldsByDefault"
    public static let terminalScrollbackLineLimit = "rune.settings.terminal.scrollbackLineLimit"
    public static let terminalScrollbackLineLimitDefault = 60_000
    public static let terminalScrollbackLineLimitMinimum = 1_000
    public static let terminalScrollbackLineLimitMaximum = 200_000
    public static let writeSafetyRequireApplyDryRun = "rune.settings.writeSafety.requireApplyDryRun"
    public static let writeSafetyRequireRolloutDryRun = "rune.settings.writeSafety.requireRolloutDryRun"
    public static let writeSafetyRequireHelmDryRun = "rune.settings.writeSafety.requireHelmDryRun"
    public static let writeSafetyShowRollbackPlan = "rune.settings.writeSafety.showRollbackPlan"
    public static let writeSafetyRequireCopyableCommand = "rune.settings.writeSafety.requireCopyableCommand"
    public static let writeSafetyRequirePostActionVerification = "rune.settings.writeSafety.requirePostActionVerification"
    public static let writeSafetyRequireProductionSecondConfirmation = "rune.settings.writeSafety.requireProductionSecondConfirmation"

    public static func clampedTerminalFontSize(_ value: Double) -> Double {
        min(terminalFontSizeMaximum, max(terminalFontSizeMinimum, value))
    }

    public static func clampedTerminalScrollbackLineLimit(_ value: Int) -> Int {
        min(terminalScrollbackLineLimitMaximum, max(terminalScrollbackLineLimitMinimum, value))
    }

    public static func registerDefaults() {
        UserDefaults.standard.register(defaults: [
            persistNamespaceListCache: true,
            diagnosticsLogging: true,
            verboseDebugTrace: false,
            backgroundPrefetchOtherContexts: false,
            enableDemoCluster: true,
            logsCustomPresetOneMode: RuneCustomLogPresetMode.lines.rawValue,
            logsCustomPresetOneLines: "5000",
            logsCustomPresetOneTimeValue: "15",
            logsCustomPresetOneTimeUnit: RuneCustomLogPresetTimeUnit.minutes.rawValue,
            logsCustomPresetTwoMode: RuneCustomLogPresetMode.time.rawValue,
            logsCustomPresetTwoLines: "99999",
            logsCustomPresetTwoTimeValue: "6",
            logsCustomPresetTwoTimeUnit: RuneCustomLogPresetTimeUnit.hours.rawValue,
            keyBindingCommandPalette: RuneKeyBindingAction.commandPalette.defaultShortcut.storageValue,
            keyBindingFilterResources: RuneKeyBindingAction.filterResources.defaultShortcut.storageValue,
            keyBindingDescribe: RuneKeyBindingAction.describe.defaultShortcut.storageValue,
            keyBindingHistoryBack: RuneKeyBindingAction.historyBack.defaultShortcut.storageValue,
            keyBindingHistoryForward: RuneKeyBindingAction.historyForward.defaultShortcut.storageValue,
            keyBindingLogs: RuneKeyBindingAction.logs.defaultShortcut.storageValue,
            keyBindingSaveLogs: RuneKeyBindingAction.saveLogs.defaultShortcut.storageValue,
            keyBindingShell: RuneKeyBindingAction.shell.defaultShortcut.storageValue,
            keyBindingEdit: RuneKeyBindingAction.edit.defaultShortcut.storageValue,
            keyBindingYAML: RuneKeyBindingAction.yaml.defaultShortcut.storageValue,
            keyBindingDelete: RuneKeyBindingAction.delete.defaultShortcut.storageValue,
            keyBindingPortForward: RuneKeyBindingAction.portForward.defaultShortcut.storageValue,
            keyBindingRollout: RuneKeyBindingAction.rollout.defaultShortcut.storageValue,
            layoutSidebarWidth: 280.0,
            layoutDetailWidth: 440.0,
            layoutSidebarVisible: true,
            layoutDetailPaneVisible: true,
            layoutPodNameColumnWidth: 280.0,
            terminalFontSize: terminalFontSizeDefault,
            hideManagedFieldsByDefault: true,
            terminalScrollbackLineLimit: terminalScrollbackLineLimitDefault,
            writeSafetyRequireApplyDryRun: true,
            writeSafetyRequireRolloutDryRun: true,
            writeSafetyRequireHelmDryRun: true,
            writeSafetyShowRollbackPlan: true,
            writeSafetyRequireCopyableCommand: true,
            writeSafetyRequirePostActionVerification: true,
            writeSafetyRequireProductionSecondConfirmation: true
        ])
    }
}

public extension UserDefaults {
    var runePersistNamespaceListCache: Bool {
        get { (object(forKey: RuneSettingsKeys.persistNamespaceListCache) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.persistNamespaceListCache) }
    }

    var runeDiagnosticsLogging: Bool {
        get { (object(forKey: RuneSettingsKeys.diagnosticsLogging) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.diagnosticsLogging) }
    }

    var runeVerboseDebugTrace: Bool {
        get { (object(forKey: RuneSettingsKeys.verboseDebugTrace) as? Bool) ?? false }
        set { set(newValue, forKey: RuneSettingsKeys.verboseDebugTrace) }
    }

    var runeBackgroundPrefetchOtherContexts: Bool {
        get { (object(forKey: RuneSettingsKeys.backgroundPrefetchOtherContexts) as? Bool) ?? false }
        set { set(newValue, forKey: RuneSettingsKeys.backgroundPrefetchOtherContexts) }
    }

    var runeEnableDemoCluster: Bool {
        get { (object(forKey: RuneSettingsKeys.enableDemoCluster) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.enableDemoCluster) }
    }

    var runeLayoutSidebarVisible: Bool {
        get { (object(forKey: RuneSettingsKeys.layoutSidebarVisible) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.layoutSidebarVisible) }
    }

    var runeLayoutDetailPaneVisible: Bool {
        get { (object(forKey: RuneSettingsKeys.layoutDetailPaneVisible) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.layoutDetailPaneVisible) }
    }

    var runeTerminalFontSize: Double {
        get {
            let raw = (object(forKey: RuneSettingsKeys.terminalFontSize) as? Double)
                ?? RuneSettingsKeys.terminalFontSizeDefault
            return RuneSettingsKeys.clampedTerminalFontSize(raw)
        }
        set {
            set(RuneSettingsKeys.clampedTerminalFontSize(newValue), forKey: RuneSettingsKeys.terminalFontSize)
        }
    }

    var runeTerminalScrollbackLineLimit: Int {
        get {
            let raw = (object(forKey: RuneSettingsKeys.terminalScrollbackLineLimit) as? Int)
                ?? RuneSettingsKeys.terminalScrollbackLineLimitDefault
            return RuneSettingsKeys.clampedTerminalScrollbackLineLimit(raw)
        }
        set {
            set(
                RuneSettingsKeys.clampedTerminalScrollbackLineLimit(newValue),
                forKey: RuneSettingsKeys.terminalScrollbackLineLimit
            )
        }
    }

    var runeHideManagedFieldsByDefault: Bool {
        get { (object(forKey: RuneSettingsKeys.hideManagedFieldsByDefault) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.hideManagedFieldsByDefault) }
    }

    var runeWriteSafetyRequireApplyDryRun: Bool {
        get { (object(forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.writeSafetyRequireApplyDryRun) }
    }

    var runeWriteSafetyRequireRolloutDryRun: Bool {
        get { (object(forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.writeSafetyRequireRolloutDryRun) }
    }

    var runeWriteSafetyRequireHelmDryRun: Bool {
        get { (object(forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.writeSafetyRequireHelmDryRun) }
    }

    var runeWriteSafetyShowRollbackPlan: Bool {
        get { (object(forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.writeSafetyShowRollbackPlan) }
    }

    var runeWriteSafetyRequireCopyableCommand: Bool {
        get { (object(forKey: RuneSettingsKeys.writeSafetyRequireCopyableCommand) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.writeSafetyRequireCopyableCommand) }
    }

    var runeWriteSafetyRequirePostActionVerification: Bool {
        get { (object(forKey: RuneSettingsKeys.writeSafetyRequirePostActionVerification) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.writeSafetyRequirePostActionVerification) }
    }

    var runeWriteSafetyRequireProductionSecondConfirmation: Bool {
        get { (object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation) as? Bool) ?? true }
        set { set(newValue, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation) }
    }

    func runeCustomLogPresetConfig(slot: RuneCustomLogPresetSlot) -> RuneCustomLogPresetConfig {
        let keys = runeCustomLogPresetKeys(for: slot)

        let modeRaw = (object(forKey: keys.mode) as? String) ?? RuneCustomLogPresetMode.lines.rawValue
        let mode = RuneCustomLogPresetMode(rawValue: modeRaw) ?? .lines

        let linesRaw = (object(forKey: keys.lines) as? String) ?? "200"
        let lines = Int(linesRaw) ?? 200

        let timeValueRaw = (object(forKey: keys.timeValue) as? String) ?? "15"
        let timeValue = Int(timeValueRaw) ?? 15

        let unitRaw = (object(forKey: keys.timeUnit) as? String) ?? RuneCustomLogPresetTimeUnit.minutes.rawValue
        let unit = RuneCustomLogPresetTimeUnit(rawValue: unitRaw) ?? .minutes

        return RuneCustomLogPresetConfig(
            mode: mode,
            lines: lines,
            timeValue: timeValue,
            timeUnit: unit
        )
    }

    private func runeCustomLogPresetKeys(for slot: RuneCustomLogPresetSlot) -> (mode: String, lines: String, timeValue: String, timeUnit: String) {
        switch slot {
        case .one:
            return (
                mode: RuneSettingsKeys.logsCustomPresetOneMode,
                lines: RuneSettingsKeys.logsCustomPresetOneLines,
                timeValue: RuneSettingsKeys.logsCustomPresetOneTimeValue,
                timeUnit: RuneSettingsKeys.logsCustomPresetOneTimeUnit
            )
        case .two:
            return (
                mode: RuneSettingsKeys.logsCustomPresetTwoMode,
                lines: RuneSettingsKeys.logsCustomPresetTwoLines,
                timeValue: RuneSettingsKeys.logsCustomPresetTwoTimeValue,
                timeUnit: RuneSettingsKeys.logsCustomPresetTwoTimeUnit
            )
        }
    }
}
