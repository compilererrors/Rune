import Foundation

/// Optional append-only JSON log (one object per line). Enable: `RUNE_K8S_TRACE_FILE=/path/to/trace.jsonl`.
public enum K8sWireTrace {
    private static let queue = DispatchQueue(label: "com.rune.k8s-wire-trace")

    private struct KubectlLine: Encodable {
        let ts: String
        let transport: String
        let command: String
        let timeoutSeconds: Double?
        let durationMs: Int
        let exitCode: Int32?
        let stdoutBytes: Int?
        let stderrBytes: Int?
        let cancelled: Bool
        let error: String?
    }

    public static func logKubectlCompletion(
        command: String,
        timeout: TimeInterval?,
        duration: TimeInterval,
        exitCode: Int32?,
        stdoutBytes: Int?,
        stderrBytes: Int?,
        error: Error?
    ) {
        guard let url = traceFileURL() else { return }
        let ts = iso8601(Date())
        let cancelled = error is CancellationError
        let line = KubectlLine(
            ts: ts,
            transport: "kubectl",
            command: command,
            timeoutSeconds: timeout.map { Double($0) },
            durationMs: Int(duration * 1000),
            exitCode: exitCode,
            stdoutBytes: stdoutBytes,
            stderrBytes: stderrBytes,
            cancelled: cancelled,
            error: error.map { cancelled ? "cancelled" : $0.localizedDescription }
        )
        appendLine(line, to: url)
    }

    private static func appendLine(_ line: KubectlLine, to url: URL) {
        queue.async {
            do {
                let data = try JSONEncoder().encode(line)
                let payload = data + Data("\n".utf8)
                try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
                if FileManager.default.fileExists(atPath: url.path) {
                    let handle = try FileHandle(forWritingTo: url)
                    defer { try? handle.close() }
                    handle.seekToEndOfFile()
                    try handle.write(contentsOf: payload)
                } else {
                    try payload.write(to: url, options: .atomic)
                }
            } catch {
                // Best-effort; never break kubectl runs.
            }
        }
    }

    private static func traceFileURL() -> URL? {
        guard let raw = ProcessInfo.processInfo.environment["RUNE_K8S_TRACE_FILE"]?.trimmingCharacters(in: .whitespacesAndNewlines),
              !raw.isEmpty
        else {
            return nil
        }
        return URL(fileURLWithPath: (raw as NSString).expandingTildeInPath)
    }

    private static func iso8601(_ date: Date) -> String {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f.string(from: date)
    }
}
