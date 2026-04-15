import Foundation

public final class DiagnosticsRecorder {
    public init() {}

    public func log(_ message: String) {
        NSLog("[Rune] %@", message)
    }
}
