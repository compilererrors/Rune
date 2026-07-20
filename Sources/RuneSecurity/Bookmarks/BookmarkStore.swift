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

public enum KubeConfigBookmarkPlacement: Sendable, Equatable {
    case append
    case prependNewestFirst
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
    private let transactionLock = NSLock()
    private let createBookmarkData: (URL) throws -> Data
    private let resolveBookmarkData: (Data) throws -> (url: URL, isStale: Bool)

    public init(store: BookmarkStore) {
        self.store = store
        self.createBookmarkData = { url in
            try url.bookmarkData(
                options: .withSecurityScope,
                includingResourceValuesForKeys: nil,
                relativeTo: nil
            )
        }
        self.resolveBookmarkData = { data in
            var isStale = false
            let url = try URL(
                resolvingBookmarkData: data,
                options: [.withSecurityScope],
                relativeTo: nil,
                bookmarkDataIsStale: &isStale
            )
            return (url, isStale)
        }
    }

    init(
        store: BookmarkStore,
        createBookmarkData: @escaping (URL) throws -> Data,
        resolveBookmarkData: @escaping (Data) throws -> (url: URL, isStale: Bool)
    ) {
        self.store = store
        self.createBookmarkData = createBookmarkData
        self.resolveBookmarkData = resolveBookmarkData
    }

    public func addKubeConfig(url: URL) throws {
        try addKubeConfigs(urls: [url])
    }

    public func addKubeConfigs(
        urls: [URL],
        placement: KubeConfigBookmarkPlacement = .append
    ) throws {
        guard !urls.isEmpty else { return }
        try transactionLock.withLock {
            let newRecords = try urls.map { url in
                BookmarkRecord(
                    path: url.path,
                    bookmarkData: try createBookmarkData(url)
                )
            }

            let originalRecords = try store.loadRecords()
            var records = originalRecords
            let newPaths = Set(newRecords.map(\.path))
            records.removeAll { newPaths.contains($0.path) }
            switch placement {
            case .append:
                records.append(contentsOf: newRecords)
            case .prependNewestFirst:
                records.insert(contentsOf: newRecords.reversed(), at: 0)
            }
            try saveWithRollback(records, originalRecords: originalRecords)
        }
    }

    public func loadKubeConfigSources() throws -> [KubeConfigSource] {
        try transactionLock.withLock {
            let originalRecords = try store.loadRecords()
            var refreshedRecords = originalRecords
            var sources: [KubeConfigSource] = []
            var didRefreshStaleBookmark = false
            sources.reserveCapacity(originalRecords.count)

            for (index, record) in originalRecords.enumerated() {
                let resolved = try resolveBookmarkData(record.bookmarkData)
                if resolved.isStale {
                    refreshedRecords[index] = BookmarkRecord(
                        path: resolved.url.path,
                        bookmarkData: try createBookmarkData(resolved.url)
                    )
                    didRefreshStaleBookmark = true
                }
                sources.append(KubeConfigSource(url: resolved.url))
            }

            if didRefreshStaleBookmark {
                try saveWithRollback(refreshedRecords, originalRecords: originalRecords)
            }
            return sources
        }
    }

    private func saveWithRollback(
        _ records: [BookmarkRecord],
        originalRecords: [BookmarkRecord]
    ) throws {
        do {
            try store.saveRecords(records)
        } catch {
            // Stores should replace their value atomically. The rollback also protects
            // custom stores that mutate before reporting a failed write. It stays inside
            // the same manager transaction so another load cannot observe partial state.
            try? store.saveRecords(originalRecords)
            throw error
        }
    }
}

public final class SecurityScopedAccess {
    private let lock = NSLock()
    private var retainedURLs: [String: URL] = [:]

    public init() {}

    deinit {
        releaseAll()
    }

    public func releaseAll() {
        lock.lock()
        let urls = Array(retainedURLs.values)
        retainedURLs.removeAll()
        lock.unlock()

        for url in urls {
            url.stopAccessingSecurityScopedResource()
        }
    }

    public func releaseAccess(to url: URL) {
        let path = url.standardizedFileURL.path
        lock.lock()
        let retained = retainedURLs.removeValue(forKey: path)
        lock.unlock()
        retained?.stopAccessingSecurityScopedResource()
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
