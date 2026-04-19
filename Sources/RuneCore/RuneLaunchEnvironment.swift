import Foundation

/// Process environment overrides applied before UserDefaults-backed settings are read.
/// Use when launching from Terminal to force logging / file trace without opening Settings.
public enum RuneLaunchEnvironment {
    /// `=1` → **Verbose debug trace (file)** (`debug-trace.log` under Application Support).
    public static let verboseDebugTraceVariable = "RUNE_VERBOSE_DEBUG_TRACE"
    /// `=1` → **Diagnostics logging** (`NSLog` / Console; same as Settings toggle).
    public static let diagnosticsLoggingVariable = "RUNE_DIAGNOSTICS_LOGGING"
    /// `=1` → mirror `[Rune]` log and trace lines to **stderr** (visible in the Terminal that launched the app).
    public static let logToStderrVariable = "RUNE_LOG_TO_STDERR"
    /// Absolute path to append JSON lines for every kubectl subprocess (A/B harness, comparisons).
    public static let k8sTraceFileVariable = "RUNE_K8S_TRACE_FILE"

    /// Call once at app startup, immediately after ``RuneSettingsKeys/registerDefaults()``.
    public static func applyProcessOverrides() {
        let env = ProcessInfo.processInfo.environment
        if env[verboseDebugTraceVariable] == "1" {
            UserDefaults.standard.set(true, forKey: RuneSettingsKeys.verboseDebugTrace)
        }
        if env[diagnosticsLoggingVariable] == "1" {
            UserDefaults.standard.set(true, forKey: RuneSettingsKeys.diagnosticsLogging)
        }
    }

    public static var isMirroringDiagnosticsToStderr: Bool {
        ProcessInfo.processInfo.environment[logToStderrVariable] == "1"
    }
}
