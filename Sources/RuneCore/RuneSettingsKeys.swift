import Foundation

/// UserDefaults keys for in-app preferences (Settings, gear menu).
public enum RuneSettingsKeys {
    /// When true, Rune restores the last namespace list from disk on launch and saves it again after a successful namespace refresh.
    public static let persistNamespaceListCache = "rune.settings.persistNamespaceListCache"
    /// When false, `DiagnosticsRecorder` does not emit NSLog lines.
    public static let diagnosticsLogging = "rune.settings.diagnosticsLogging"
    /// When true, `DiagnosticsRecorder.trace` appends detailed lines to `Rune/Logs/debug-trace.log` under Application Support.
    public static let verboseDebugTrace = "rune.settings.verboseDebugTrace"
    /// When true, after the active context snapshot finishes, Rune may warm overview cache for a few other contexts in the background (bounded).
    public static let backgroundPrefetchOtherContexts = "rune.settings.backgroundPrefetchOtherContexts"
    public static let logsCustomPresetOneMode = "rune.settings.logs.customPresetOne.mode"
    public static let logsCustomPresetOneLines = "rune.settings.logs.customPresetOne.lines"
    public static let logsCustomPresetOneTimeValue = "rune.settings.logs.customPresetOne.timeValue"
    public static let logsCustomPresetOneTimeUnit = "rune.settings.logs.customPresetOne.timeUnit"
    public static let logsCustomPresetTwoMode = "rune.settings.logs.customPresetTwo.mode"
    public static let logsCustomPresetTwoLines = "rune.settings.logs.customPresetTwo.lines"
    public static let logsCustomPresetTwoTimeValue = "rune.settings.logs.customPresetTwo.timeValue"
    public static let logsCustomPresetTwoTimeUnit = "rune.settings.logs.customPresetTwo.timeUnit"
    public static let keyBindingDescribe = "rune.settings.keybindings.describe"
    public static let keyBindingHistoryBack = "rune.settings.keybindings.historyBack"
    public static let keyBindingHistoryForward = "rune.settings.keybindings.historyForward"
    public static let keyBindingLogs = "rune.settings.keybindings.logs"
    public static let keyBindingShell = "rune.settings.keybindings.shell"
    public static let keyBindingYAML = "rune.settings.keybindings.yaml"
    public static let keyBindingPortForward = "rune.settings.keybindings.portForward"
    public static let keyBindingRollout = "rune.settings.keybindings.rollout"
    public static let keyBindingHelmValues = "rune.settings.keybindings.helmValues"
    public static let keyBindingHelmManifest = "rune.settings.keybindings.helmManifest"
    public static let keyBindingHelmHistory = "rune.settings.keybindings.helmHistory"
    /// Persisted sidebar width in the 3-column shell.
    public static let layoutSidebarWidth = "rune.settings.layout.sidebarWidth"
    /// Persisted detail/inspector width in the 3-column shell.
    public static let layoutDetailWidth = "rune.settings.layout.detailWidth"

    public static func registerDefaults() {
        UserDefaults.standard.register(defaults: [
            persistNamespaceListCache: true,
            diagnosticsLogging: true,
            verboseDebugTrace: false,
            backgroundPrefetchOtherContexts: false,
            logsCustomPresetOneMode: RuneCustomLogPresetMode.lines.rawValue,
            logsCustomPresetOneLines: "5000",
            logsCustomPresetOneTimeValue: "15",
            logsCustomPresetOneTimeUnit: RuneCustomLogPresetTimeUnit.minutes.rawValue,
            logsCustomPresetTwoMode: RuneCustomLogPresetMode.time.rawValue,
            logsCustomPresetTwoLines: "99999",
            logsCustomPresetTwoTimeValue: "6",
            logsCustomPresetTwoTimeUnit: RuneCustomLogPresetTimeUnit.hours.rawValue,
            keyBindingDescribe: RuneKeyBindingAction.describe.defaultShortcut.storageValue,
            keyBindingHistoryBack: RuneKeyBindingAction.historyBack.defaultShortcut.storageValue,
            keyBindingHistoryForward: RuneKeyBindingAction.historyForward.defaultShortcut.storageValue,
            keyBindingLogs: RuneKeyBindingAction.logs.defaultShortcut.storageValue,
            keyBindingShell: RuneKeyBindingAction.shell.defaultShortcut.storageValue,
            keyBindingYAML: RuneKeyBindingAction.yaml.defaultShortcut.storageValue,
            keyBindingPortForward: RuneKeyBindingAction.portForward.defaultShortcut.storageValue,
            keyBindingRollout: RuneKeyBindingAction.rollout.defaultShortcut.storageValue,
            keyBindingHelmValues: RuneKeyBindingAction.helmValues.defaultShortcut.storageValue,
            keyBindingHelmManifest: RuneKeyBindingAction.helmManifest.defaultShortcut.storageValue,
            keyBindingHelmHistory: RuneKeyBindingAction.helmHistory.defaultShortcut.storageValue,
            layoutSidebarWidth: 280.0,
            layoutDetailWidth: 440.0
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
