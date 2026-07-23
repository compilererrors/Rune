import Foundation
import OSLog
import RuneCore

public final class DiagnosticsRecorder {
    private static let stderrQueue = DispatchQueue(label: "com.rune.diagnostics.stderr")

    public init() {}

    public func log(_ message: String) {
        if RuneLaunchEnvironment.isMirroringDiagnosticsToStderr {
            Self.stderrQueue.async {
                let line = "[Rune] \(message)\n"
                if let data = line.data(using: .utf8) {
                    try? FileHandle.standardError.write(contentsOf: data)
                }
            }
        }
        guard UserDefaults.standard.runeDiagnosticsLogging else { return }
        RuneLoggers.diagnostics.notice("\(message, privacy: .private)")
    }

    /// Persistent verbose trace (timestamp, category, message). Gated by `runeVerboseDebugTrace` or `RUNE_VERBOSE_DEBUG_TRACE=1` at launch.
    public func trace(_ category: String, _ message: String) {
        guard UserDefaults.standard.runeVerboseDebugTrace else { return }
        let safeCategory = VerboseKubeTrace.privacySafeMessage(category)
        let safeMessage = VerboseKubeTrace.privacySafeMessage(message)
        if RuneLaunchEnvironment.isMirroringDiagnosticsToStderr {
            Self.stderrQueue.async {
                let line = "[Rune trace] [\(safeCategory)] \(safeMessage)\n"
                if let data = line.data(using: .utf8) {
                    try? FileHandle.standardError.write(contentsOf: data)
                }
            }
        }
        DebugTraceWriter.append(category: safeCategory, message: safeMessage)
    }
}
