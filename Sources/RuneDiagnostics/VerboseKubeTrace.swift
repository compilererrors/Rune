import Foundation
import RuneCore

/// File-only lines (same gate as `DiagnosticsRecorder.trace`) for Kubernetes I/O — **never** logs bearer tokens.
public enum VerboseKubeTrace {
    public static func append(_ category: String, _ message: String) {
        guard UserDefaults.standard.runeVerboseDebugTrace else { return }
        DebugTraceWriter.append(category: category, message: message)
    }

    /// Basenames of kubeconfig paths from `KUBECONFIG` (colon-separated).
    public static func kubeconfigSummary(_ environment: [String: String]) -> String {
        guard let raw = environment["KUBECONFIG"], !raw.isEmpty else {
            return "(default discovery)"
        }
        let parts = raw.split(separator: ":").map { String($0) }
        let names = parts.map { URL(fileURLWithPath: $0).lastPathComponent }
        return names.joined(separator: ", ")
    }
}
