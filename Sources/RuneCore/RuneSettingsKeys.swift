import Foundation

/// UserDefaults keys for in-app preferences (Settings, gear menu).
public enum RuneSettingsKeys {
    /// When true, Rune restores the last namespace list from disk on launch and saves it again after a successful namespace refresh.
    public static let persistNamespaceListCache = "rune.settings.persistNamespaceListCache"
    /// When false, `DiagnosticsRecorder` does not emit NSLog lines.
    public static let diagnosticsLogging = "rune.settings.diagnosticsLogging"

    public static func registerDefaults() {
        UserDefaults.standard.register(defaults: [
            persistNamespaceListCache: true,
            diagnosticsLogging: true
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
}
