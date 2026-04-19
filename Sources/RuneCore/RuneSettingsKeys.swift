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

    public static func registerDefaults() {
        UserDefaults.standard.register(defaults: [
            persistNamespaceListCache: true,
            diagnosticsLogging: true,
            verboseDebugTrace: false,
            backgroundPrefetchOtherContexts: false
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
}
