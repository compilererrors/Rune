import Foundation
import Darwin

/// Append-only debug trace file under Application Support (`Rune/Logs/debug-trace.log`).
/// Thread-safe; rotates when the file exceeds `maxBytes`.
public enum DebugTraceWriter {
    public static let maxBytes = 4_194_304 // 4 MiB
    private static let maxCategoryCharacters = 96
    private static let maxMessageCharacters = 65_536
    private static let directoryPermissions = NSNumber(value: Int16(0o700))
    private static let filePermissions = NSNumber(value: Int16(0o600))
    private static let queue = DispatchQueue(label: "com.rune.debug-trace-writer")

    public static var logFileURL: URL {
        let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? FileManager.default.temporaryDirectory
        return base.appendingPathComponent("Rune/Logs/debug-trace.log", isDirectory: false)
    }

    public static func append(category: String, message: String) {
        let ts = iso8601String(Date())
        let safeCategory = singleLine(category, maximumCharacters: maxCategoryCharacters)
        let safeMessage = singleLine(message, maximumCharacters: maxMessageCharacters)
        let line = "\(ts)\t[\(safeCategory)]\t\(safeMessage)\n"
        guard let data = line.data(using: .utf8) else { return }

        queue.async {
            do {
                try write(data, to: logFileURL, maxBytes: maxBytes)
            } catch {
                // Avoid unified logging loops.
            }
        }
    }

    public static func clear() {
        queue.async {
            try? clearFiles(at: logFileURL)
        }
    }

    /// Synchronous write seam used by focused diagnostics tests.
    static func writeLine(_ line: String, to url: URL, maxBytes: Int = maxBytes) throws {
        guard let data = line.data(using: .utf8) else { return }
        try write(data, to: url, maxBytes: maxBytes)
    }

    /// Synchronous clear seam used by focused diagnostics tests.
    static func clearFiles(at url: URL) throws {
        var firstError: Error?
        for candidate in [url, rotatedFileURL(for: url)] {
            do {
                try unlinkTraceFileIfPresent(at: candidate)
            } catch {
                if firstError == nil {
                    firstError = error
                }
            }
        }
        if let firstError {
            throw firstError
        }
    }

    private static func write(
        _ data: Data,
        to url: URL,
        maxBytes: Int,
        fileManager: FileManager = .default
    ) throws {
        let directoryURL = url.deletingLastPathComponent()
        try prepareDirectory(at: directoryURL, fileManager: fileManager)

        let backupURL = rotatedFileURL(for: url)
        _ = try secureRegularFileIfPresent(at: backupURL, fileManager: fileManager)

        if try secureRegularFileIfPresent(at: url, fileManager: fileManager) {
            let attributes = try fileManager.attributesOfItem(atPath: url.path)
            let size = (attributes[.size] as? NSNumber)?.int64Value ?? 0
            if size + Int64(data.count) > Int64(max(0, maxBytes)) {
                try rotate(url: url, backupURL: backupURL, fileManager: fileManager)
            }
        }

        try appendSecurely(data, to: url)
    }

    private static func prepareDirectory(at url: URL, fileManager: FileManager) throws {
        if let mode = try fileMode(at: url) {
            guard mode & mode_t(S_IFMT) == mode_t(S_IFDIR) else {
                throw fileSystemError("Trace directory is not a regular directory.", at: url)
            }
        } else {
            try fileManager.createDirectory(
                at: url,
                withIntermediateDirectories: true,
                attributes: [.posixPermissions: directoryPermissions]
            )
        }
        try fileManager.setAttributes(
            [.posixPermissions: directoryPermissions],
            ofItemAtPath: url.path
        )
    }

    @discardableResult
    private static func secureRegularFileIfPresent(
        at url: URL,
        fileManager: FileManager
    ) throws -> Bool {
        guard let mode = try fileMode(at: url) else { return false }
        guard mode & mode_t(S_IFMT) == mode_t(S_IFREG) else {
            throw fileSystemError("Trace path is not a regular file.", at: url)
        }
        try fileManager.setAttributes(
            [.posixPermissions: filePermissions],
            ofItemAtPath: url.path
        )
        return true
    }

    private static func fileMode(at url: URL) throws -> mode_t? {
        var information = stat()
        let result = url.path.withCString { path in
            Darwin.lstat(path, &information)
        }
        if result == 0 {
            return information.st_mode
        }
        if errno == ENOENT {
            return nil
        }
        throw posixError(at: url)
    }

    private static func unlinkTraceFileIfPresent(at url: URL) throws {
        guard let mode = try fileMode(at: url) else { return }
        let fileType = mode & mode_t(S_IFMT)
        guard fileType == mode_t(S_IFREG) || fileType == mode_t(S_IFLNK) else {
            throw fileSystemError("Trace path is not a regular file or symbolic link.", at: url)
        }

        let result = url.path.withCString { path in
            Darwin.unlink(path)
        }
        if result != 0, errno != ENOENT {
            throw posixError(at: url)
        }
    }

    private static func appendSecurely(_ data: Data, to url: URL) throws {
        let descriptor = url.path.withCString { path in
            Darwin.open(
                path,
                O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC | O_NOFOLLOW,
                mode_t(0o600)
            )
        }
        guard descriptor >= 0 else {
            throw posixError(at: url)
        }

        guard Darwin.fchmod(descriptor, mode_t(0o600)) == 0 else {
            let error = posixError(at: url)
            Darwin.close(descriptor)
            throw error
        }

        let handle = FileHandle(fileDescriptor: descriptor, closeOnDealloc: true)
        do {
            try handle.write(contentsOf: data)
            try handle.close()
        } catch {
            try? handle.close()
            throw error
        }
    }

    private static func rotate(
        url: URL,
        backupURL: URL,
        fileManager: FileManager
    ) throws {
        if try secureRegularFileIfPresent(at: backupURL, fileManager: fileManager) {
            try fileManager.removeItem(at: backupURL)
        }
        try fileManager.moveItem(at: url, to: backupURL)
        _ = try secureRegularFileIfPresent(at: backupURL, fileManager: fileManager)
    }

    private static func rotatedFileURL(for url: URL) -> URL {
        url.deletingPathExtension().appendingPathExtension("log.1")
    }

    private static func singleLine(_ value: String, maximumCharacters: Int) -> String {
        let normalized = String(value.prefix(maximumCharacters))
            .replacingOccurrences(of: "\r", with: " ")
            .replacingOccurrences(of: "\n", with: " ")
            .replacingOccurrences(of: "\t", with: " ")
        return normalized
    }

    private static func fileSystemError(_ description: String, at url: URL) -> NSError {
        NSError(
            domain: NSCocoaErrorDomain,
            code: NSFileWriteUnknownError,
            userInfo: [
                NSFilePathErrorKey: url.path,
                NSLocalizedDescriptionKey: description
            ]
        )
    }

    private static func posixError(at url: URL) -> NSError {
        NSError(
            domain: NSPOSIXErrorDomain,
            code: Int(errno),
            userInfo: [NSFilePathErrorKey: url.path]
        )
    }

    private static func iso8601String(_ date: Date) -> String {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime]
        return f.string(from: date)
    }
}
