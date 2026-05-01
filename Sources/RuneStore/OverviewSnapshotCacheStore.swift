import Foundation
import RuneCore

// Overview snapshots on disk: `JSONOverviewSnapshotCacheStore` stores one JSON file per `contextName` / `namespace`.
// Older aggregate files at `~/Library/Application Support/Rune/overview-snapshot-cache.json` or `~/Library/Caches/Rune/`
// are imported into the per-snapshot directory. In-RAM resource lists live in `ResourceStore`.

/// Serialized overview row: pod status list, namespaced resource counts, optional cluster CPU/MEM, events.
public struct PersistedOverviewSnapshot: Codable, Sendable {
    public let contextName: String
    public let namespace: String
    public let fetchedAt: Date
    public var lastAccessedAt: Date
    public let pods: [PodSummary]
    public let deploymentsCount: Int
    public let servicesCount: Int
    public let ingressesCount: Int
    public let configMapsCount: Int
    /// Present in snapshots written by newer builds; `nil` for legacy JSON rows.
    public let cronJobsCount: Int?
    public let nodesCount: Int
    public let clusterCPUPercent: Int?
    public let clusterMemoryPercent: Int?
    public let events: [EventSummary]

    public init(
        contextName: String,
        namespace: String,
        fetchedAt: Date,
        lastAccessedAt: Date,
        pods: [PodSummary],
        deploymentsCount: Int,
        servicesCount: Int,
        ingressesCount: Int,
        configMapsCount: Int,
        cronJobsCount: Int? = nil,
        nodesCount: Int,
        clusterCPUPercent: Int? = nil,
        clusterMemoryPercent: Int? = nil,
        events: [EventSummary]
    ) {
        self.contextName = contextName
        self.namespace = namespace
        self.fetchedAt = fetchedAt
        self.lastAccessedAt = lastAccessedAt
        self.pods = pods
        self.deploymentsCount = deploymentsCount
        self.servicesCount = servicesCount
        self.ingressesCount = ingressesCount
        self.configMapsCount = configMapsCount
        self.cronJobsCount = cronJobsCount
        self.nodesCount = nodesCount
        self.clusterCPUPercent = clusterCPUPercent
        self.clusterMemoryPercent = clusterMemoryPercent
        self.events = events
    }
}

public protocol OverviewSnapshotCacheStoring: Sendable {
    func loadSnapshot(contextName: String, namespace: String, maxAge: TimeInterval) async -> PersistedOverviewSnapshot?
    func saveSnapshot(_ snapshot: PersistedOverviewSnapshot) async
}

/// Test stub: no persistence.
public actor NoopOverviewSnapshotCacheStore: OverviewSnapshotCacheStoring {
    public init() {}

    public func loadSnapshot(contextName: String, namespace: String, maxAge: TimeInterval) async -> PersistedOverviewSnapshot? {
        nil
    }

    public func saveSnapshot(_ snapshot: PersistedOverviewSnapshot) async {}
}

/// JSON file store for `PersistedOverviewSnapshot`; actor-serialized reads/writes; prunes by age and max entry count.
public actor JSONOverviewSnapshotCacheStore: OverviewSnapshotCacheStoring {
    private struct FilePayload: Codable {
        let schemaVersion: Int
        let entries: [PersistedOverviewSnapshot]
    }

    private static let schemaVersion = 1

    private let fileURL: URL
    private let entriesDirectoryURL: URL
    private let maxEntries: Int
    private let retentionTTL: TimeInterval
    private let nowProvider: @Sendable () -> Date

    private var entriesByKey: [String: PersistedOverviewSnapshot] = [:]
    private var loaded = false

    public init(
        fileURL: URL = JSONOverviewSnapshotCacheStore.defaultCacheFileURL(),
        maxEntries: Int = 160,
        retentionTTL: TimeInterval = 60 * 30,
        nowProvider: @escaping @Sendable () -> Date = Date.init
    ) {
        self.fileURL = fileURL
        self.entriesDirectoryURL = Self.entriesDirectoryURL(for: fileURL)
        self.maxEntries = max(8, maxEntries)
        self.retentionTTL = max(30, retentionTTL)
        self.nowProvider = nowProvider
    }

    public func loadSnapshot(contextName: String, namespace: String, maxAge: TimeInterval) async -> PersistedOverviewSnapshot? {
        await ensureLoaded()

        let key = Self.key(contextName: contextName, namespace: namespace)
        guard var entry = entriesByKey[key] else { return nil }

        let now = nowProvider()
        if now.timeIntervalSince(entry.fetchedAt) > maxAge {
            entriesByKey.removeValue(forKey: key)
            removeSnapshotFile(forKey: key)
            return nil
        }

        entry.lastAccessedAt = now
        entriesByKey[key] = entry
        persistSnapshotToDisk(entry)
        return entry
    }

    public func saveSnapshot(_ snapshot: PersistedOverviewSnapshot) async {
        await ensureLoaded()

        let now = nowProvider()
        let normalized = PersistedOverviewSnapshot(
            contextName: snapshot.contextName,
            namespace: snapshot.namespace,
            fetchedAt: snapshot.fetchedAt,
            lastAccessedAt: now,
            pods: snapshot.pods,
            deploymentsCount: snapshot.deploymentsCount,
            servicesCount: snapshot.servicesCount,
            ingressesCount: snapshot.ingressesCount,
            configMapsCount: snapshot.configMapsCount,
            cronJobsCount: snapshot.cronJobsCount,
            nodesCount: snapshot.nodesCount,
            clusterCPUPercent: snapshot.clusterCPUPercent,
            clusterMemoryPercent: snapshot.clusterMemoryPercent,
            events: snapshot.events
        )

        entriesByKey[Self.key(contextName: normalized.contextName, namespace: normalized.namespace)] = normalized
        let removedKeys = prune(reference: now)
        persistSnapshotToDisk(normalized)
        removeSnapshotFiles(forKeys: removedKeys)
    }

    private func ensureLoaded() async {
        guard !loaded else { return }
        loaded = true

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        func decodeEntries(at url: URL) -> [PersistedOverviewSnapshot] {
            guard let data = try? Data(contentsOf: url) else { return [] }
            guard let payload = try? decoder.decode(FilePayload.self, from: data),
                  payload.schemaVersion == Self.schemaVersion else { return [] }
            return payload.entries
        }

        var primaryEntries = decodeEntries(at: fileURL)
        var migratedFromLegacy = false

        if primaryEntries.isEmpty, let legacy = Self.legacyCachesOverviewFileURL(), legacy != fileURL {
            let legacyEntries = decodeEntries(at: legacy)
            if !legacyEntries.isEmpty {
                primaryEntries = legacyEntries
                migratedFromLegacy = true
                try? FileManager.default.removeItem(at: legacy)
            }
        }

        for entry in primaryEntries {
            entriesByKey[Self.key(contextName: entry.contextName, namespace: entry.namespace)] = entry
        }
        let directoryEntries = decodeDirectoryEntries()
        for entry in directoryEntries {
            entriesByKey[Self.key(contextName: entry.contextName, namespace: entry.namespace)] = entry
        }

        let removedKeys = prune(reference: nowProvider())

        if !primaryEntries.isEmpty || migratedFromLegacy {
            persistAllSnapshotsToDisk()
        }
        removeSnapshotFiles(forKeys: removedKeys)
    }

    @discardableResult
    private func prune(reference: Date) -> [String] {
        var removedKeys: [String] = []
        entriesByKey = entriesByKey.filter { _, entry in
            let shouldKeep = reference.timeIntervalSince(entry.fetchedAt) <= retentionTTL
            if !shouldKeep {
                removedKeys.append(Self.key(contextName: entry.contextName, namespace: entry.namespace))
            }
            return shouldKeep
        }

        guard entriesByKey.count > maxEntries else { return removedKeys }
        let sortedByAccessAscending = entriesByKey.values.sorted { lhs, rhs in
            lhs.lastAccessedAt < rhs.lastAccessedAt
        }

        let removeCount = entriesByKey.count - maxEntries
        for entry in sortedByAccessAscending.prefix(removeCount) {
            let key = Self.key(contextName: entry.contextName, namespace: entry.namespace)
            entriesByKey.removeValue(forKey: key)
            removedKeys.append(key)
        }
        return removedKeys
    }

    private func decodeDirectoryEntries() -> [PersistedOverviewSnapshot] {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        guard let files = try? FileManager.default.contentsOfDirectory(
            at: entriesDirectoryURL,
            includingPropertiesForKeys: nil
        ) else {
            return []
        }

        return files
            .filter { $0.pathExtension == "json" }
            .compactMap { url in
                guard let data = try? Data(contentsOf: url) else { return nil }
                return try? decoder.decode(PersistedOverviewSnapshot.self, from: data)
            }
    }

    private func persistAllSnapshotsToDisk() {
        for entry in entriesByKey.values {
            persistSnapshotToDisk(entry)
        }
    }

    private func persistSnapshotToDisk(_ snapshot: PersistedOverviewSnapshot) {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601

        guard let data = try? encoder.encode(snapshot) else { return }

        try? FileManager.default.createDirectory(at: entriesDirectoryURL, withIntermediateDirectories: true)
        let key = Self.key(contextName: snapshot.contextName, namespace: snapshot.namespace)
        try? data.write(to: snapshotFileURL(forKey: key), options: [.atomic])
    }

    private func removeSnapshotFiles(forKeys keys: [String]) {
        for key in keys {
            removeSnapshotFile(forKey: key)
        }
    }

    private func removeSnapshotFile(forKey key: String) {
        try? FileManager.default.removeItem(at: snapshotFileURL(forKey: key))
    }

    private func snapshotFileURL(forKey key: String) -> URL {
        entriesDirectoryURL.appendingPathComponent(Self.fileName(forKey: key), isDirectory: false)
    }

    private static func key(contextName: String, namespace: String) -> String {
        "\(contextName)::\(namespace)"
    }

    private static func fileName(forKey key: String) -> String {
        let encoded = Data(key.utf8)
            .base64EncodedString()
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "=", with: "")
        return "\(encoded).json"
    }

    public static func entriesDirectoryURL(for fileURL: URL) -> URL {
        fileURL.deletingLastPathComponent()
            .appendingPathComponent(fileURL.deletingPathExtension().lastPathComponent + "-entries", isDirectory: true)
    }

    /// Default legacy aggregate path; new snapshots are stored beside it in `overview-snapshot-cache-entries/`.
    public static func defaultCacheFileURL() -> URL {
        if let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first {
            return appSupport
                .appendingPathComponent("Rune", isDirectory: true)
                .appendingPathComponent("overview-snapshot-cache.json")
        }

        return URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("rune-overview-snapshot-cache.json")
    }

    /// Previous default location under `Library/Caches`; superseded by `defaultCacheFileURL()`.
    private static func legacyCachesOverviewFileURL() -> URL? {
        FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first?
            .appendingPathComponent("Rune", isDirectory: true)
            .appendingPathComponent("overview-snapshot-cache.json")
    }
}
