import Foundation
import RuneCore

public final class DiagnosticsRecorder {
    public init() {}

    public func log(_ message: String) {
        guard UserDefaults.standard.runeDiagnosticsLogging else { return }
        NSLog("[Rune] %@", message)
    }
}
