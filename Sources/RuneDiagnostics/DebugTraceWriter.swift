import Foundation

/// Append-only debug trace file under Application Support (`Rune/Logs/debug-trace.log`).
/// Thread-safe; rotates when the file exceeds `maxBytes`.
public enum DebugTraceWriter {
    public static let maxBytes = 4_194_304 // 4 MiB
    private static let queue = DispatchQueue(label: "com.rune.debug-trace-writer")

    public static var logFileURL: URL {
        let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? FileManager.default.temporaryDirectory
        return base.appendingPathComponent("Rune/Logs/debug-trace.log", isDirectory: false)
    }

    public static func append(category: String, message: String) {
        let ts = iso8601String(Date())
        let line = "\(ts)\t[\(category)]\t\(message)\n"
        guard let data = line.data(using: .utf8) else { return }

        queue.async {
            do {
                let url = logFileURL
                try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
                if FileManager.default.fileExists(atPath: url.path) {
                    let attrs = try FileManager.default.attributesOfItem(atPath: url.path)
                    let size = (attrs[.size] as? NSNumber)?.int64Value ?? 0
                    if size + Int64(data.count) > maxBytes {
                        rotate(url: url)
                    }
                }
                if !FileManager.default.fileExists(atPath: url.path) {
                    try data.write(to: url, options: .atomic)
                    return
                }
                let handle = try FileHandle(forWritingTo: url)
                defer { try? handle.close() }
                handle.seekToEndOfFile()
                try handle.write(contentsOf: data)
            } catch {
                // Avoid NSLog loops
            }
        }
    }

    public static func clear() {
        queue.async {
            let url = logFileURL
            try? FileManager.default.removeItem(at: url)
        }
    }

    private static func rotate(url: URL) {
        let backup = url.deletingPathExtension().appendingPathExtension("log.1")
        try? FileManager.default.removeItem(at: backup)
        try? FileManager.default.moveItem(at: url, to: backup)
    }

    private static func iso8601String(_ date: Date) -> String {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime]
        return f.string(from: date)
    }
}
