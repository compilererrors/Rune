import Foundation

// MARK: - Ordering

/// Keeps prior order for namespaces that still exist on the cluster, then appends new names (sorted).
public enum NamespaceListOrdering {
    public static func merge(previousOrder: [String], apiNames: [String]) -> [String] {
        let trimmedApi = apiNames.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }
        let apiSet = Set(trimmedApi)
        var seen = Set<String>()
        var result: [String] = []
        for raw in previousOrder {
            let n = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !n.isEmpty, apiSet.contains(n), !seen.contains(n) else { continue }
            seen.insert(n)
            result.append(n)
        }
        let additions = trimmedApi.filter { !seen.contains($0) }.sorted {
            $0.localizedCaseInsensitiveCompare($1) == .orderedAscending
        }
        result.append(contentsOf: additions)
        return result
    }
}

// MARK: - Persistence

public protocol NamespaceListPersisting: Sendable {
    func load(contextName: String) -> [String]?
    func save(names: [String], contextName: String)
}

/// Test / CI stub.
public struct NoopNamespaceListPersistenceStore: NamespaceListPersisting {
    public init() {}

    public func load(contextName: String) -> [String]? { nil }

    public func save(names: [String], contextName: String) {}
}

/// One JSON file per context under `~/Library/Application Support/Rune/namespace-lists/`.
public struct JSONNamespaceListPersistenceStore: NamespaceListPersisting {
    private struct FilePayload: Codable {
        let schemaVersion: Int
        let namespaces: [String]
    }

    private static let schemaVersion = 1

    private let directoryURL: URL

    public init(directoryURL: URL = JSONNamespaceListPersistenceStore.defaultDirectoryURL()) {
        self.directoryURL = directoryURL
    }

    public static func defaultDirectoryURL() -> URL {
        if let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first {
            return appSupport
                .appendingPathComponent("Rune", isDirectory: true)
                .appendingPathComponent("namespace-lists", isDirectory: true)
        }
        return URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent("rune-namespace-lists", isDirectory: true)
    }

    public func load(contextName: String) -> [String]? {
        let url = fileURL(for: contextName)
        guard FileManager.default.fileExists(atPath: url.path) else { return nil }
        guard let data = try? Data(contentsOf: url) else { return nil }
        guard let payload = try? JSONDecoder().decode(FilePayload.self, from: data) else { return nil }
        let names = payload.namespaces.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }
        return names.isEmpty ? nil : names
    }

    public func save(names: [String], contextName: String) {
        var seen = Set<String>()
        var ordered: [String] = []
        for raw in names {
            let n = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !n.isEmpty, !seen.contains(n) else { continue }
            seen.insert(n)
            ordered.append(n)
        }

        let url = fileURL(for: contextName)
        do {
            try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true)
            let payload = FilePayload(schemaVersion: Self.schemaVersion, namespaces: ordered)
            let data = try JSONEncoder().encode(payload)
            try data.write(to: url, options: [.atomic])
        } catch {
            // Best-effort cache; ignore write failures.
        }
    }

    private func fileURL(for contextName: String) -> URL {
        directoryURL.appendingPathComponent("\(Self.sanitizedFileName(for: contextName)).json")
    }

    private static func sanitizedFileName(for contextName: String) -> String {
        let trimmed = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty { return "unknown-context" }
        let safe = trimmed
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: ":", with: "_")
        if safe.count <= 200 { return safe }
        var hash: UInt64 = 5381
        for byte in safe.utf8 {
            hash = ((hash << 5) &+ hash) &+ UInt64(byte)
        }
        return "ctx-\(hash)-\(safe.prefix(80))"
    }
}
