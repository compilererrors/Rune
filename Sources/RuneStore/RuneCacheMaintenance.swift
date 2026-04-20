import Foundation

public struct RuneCacheMaintenance {
    public struct ClearResult: Sendable {
        public let removedPaths: [String]
        public let failedPaths: [String]

        public var removedCount: Int { removedPaths.count }
        public var failedCount: Int { failedPaths.count }
    }

    /// Clears Rune disk caches used for faster startup/list hydration.
    /// Keeps user kubeconfig/bookmarks/preferences intact.
    public static func clearDiskCaches(fileManager: FileManager = .default) -> ClearResult {
        var removed: [String] = []
        var failed: [String] = []

        let urls = cacheURLs()
        for url in urls {
            let path = url.path
            guard fileManager.fileExists(atPath: path) else { continue }
            do {
                try fileManager.removeItem(at: url)
                removed.append(path)
            } catch {
                failed.append(path)
            }
        }

        return ClearResult(removedPaths: removed, failedPaths: failed)
    }

    private static func cacheURLs() -> [URL] {
        var urls: [URL] = [
            JSONNamespaceListPersistenceStore.defaultDirectoryURL(),
            JSONOverviewSnapshotCacheStore.defaultCacheFileURL()
        ]

        if let legacyOverview = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first?
            .appendingPathComponent("Rune", isDirectory: true)
            .appendingPathComponent("overview-snapshot-cache.json") {
            urls.append(legacyOverview)
        }

        // Preserve ordering, remove duplicates.
        var seen = Set<String>()
        return urls.filter { seen.insert($0.path).inserted }
    }
}

