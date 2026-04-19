import Foundation
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
        NSLog("[Rune] %@", message)
    }

    /// Persistent verbose trace (timestamp, category, message). Gated by `runeVerboseDebugTrace` or `RUNE_VERBOSE_DEBUG_TRACE=1` at launch.
    public func trace(_ category: String, _ message: String) {
        guard UserDefaults.standard.runeVerboseDebugTrace else { return }
        if RuneLaunchEnvironment.isMirroringDiagnosticsToStderr {
            Self.stderrQueue.async {
                let line = "[Rune trace] [\(category)] \(message)\n"
                if let data = line.data(using: .utf8) {
                    try? FileHandle.standardError.write(contentsOf: data)
                }
            }
        }
        DebugTraceWriter.append(category: category, message: message)
    }
}
