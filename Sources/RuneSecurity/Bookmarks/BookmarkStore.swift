import Foundation
import RuneCore

public struct BookmarkRecord: Codable, Hashable, Sendable {
    public let path: String
    public let bookmarkData: Data

    public init(path: String, bookmarkData: Data) {
        self.path = path
        self.bookmarkData = bookmarkData
    }
}

public protocol BookmarkStore: Sendable {
    func loadRecords() throws -> [BookmarkRecord]
    func saveRecords(_ records: [BookmarkRecord]) throws
}

public final class UserDefaultsBookmarkStore: BookmarkStore, @unchecked Sendable {
    private let defaults: UserDefaults
    private let storageKey: String

    public init(defaults: UserDefaults = .standard, storageKey: String = "rune.kubeconfig.bookmarks") {
        self.defaults = defaults
        self.storageKey = storageKey
    }

    public func loadRecords() throws -> [BookmarkRecord] {
        guard let data = defaults.data(forKey: storageKey) else {
            return []
        }

        return try JSONDecoder().decode([BookmarkRecord].self, from: data)
    }

    public func saveRecords(_ records: [BookmarkRecord]) throws {
        let data = try JSONEncoder().encode(records)
        defaults.set(data, forKey: storageKey)
    }
}

public final class BookmarkManager: @unchecked Sendable {
    private let store: BookmarkStore

    public init(store: BookmarkStore) {
        self.store = store
    }

    public func addKubeConfig(url: URL) throws {
        let bookmark = try url.bookmarkData(
            options: .withSecurityScope,
            includingResourceValuesForKeys: nil,
            relativeTo: nil
        )

        var records = try store.loadRecords()
        records.removeAll { $0.path == url.path }
        records.append(BookmarkRecord(path: url.path, bookmarkData: bookmark))
        try store.saveRecords(records)
    }

    public func loadKubeConfigSources() throws -> [KubeConfigSource] {
        let records = try store.loadRecords()
        var sources: [KubeConfigSource] = []

        for record in records {
            var isStale = false
            let url = try URL(
                resolvingBookmarkData: record.bookmarkData,
                options: [.withSecurityScope],
                relativeTo: nil,
                bookmarkDataIsStale: &isStale
            )

            if isStale {
                try addKubeConfig(url: url)
            }

            sources.append(KubeConfigSource(url: url))
        }

        return sources
    }
}

public final class SecurityScopedAccess {
    private let lock = NSLock()
    private var retainedURLs: [String: URL] = [:]

    public init() {}

    deinit {
        lock.lock()
        let urls = Array(retainedURLs.values)
        retainedURLs.removeAll()
        lock.unlock()

        for url in urls {
            url.stopAccessingSecurityScopedResource()
        }
    }

    public func retainAccess(to url: URL) {
        let standardizedURL = url.standardizedFileURL
        let path = standardizedURL.path

        lock.lock()
        let isRetained = retainedURLs[path] != nil
        lock.unlock()

        guard !isRetained else {
            return
        }

        guard standardizedURL.startAccessingSecurityScopedResource() else {
            return
        }

        lock.lock()
        if retainedURLs[path] == nil {
            retainedURLs[path] = standardizedURL
            lock.unlock()
        } else {
            lock.unlock()
            standardizedURL.stopAccessingSecurityScopedResource()
        }
    }

    public func withAccess<T>(to url: URL, _ operation: () throws -> T) rethrows -> T {
        let accessed = url.startAccessingSecurityScopedResource()
        defer {
            if accessed {
                url.stopAccessingSecurityScopedResource()
            }
        }

        return try operation()
    }
}
