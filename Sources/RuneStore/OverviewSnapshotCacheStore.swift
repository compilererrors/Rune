import Foundation
import RuneCore

// Overview snapshots on disk: `JSONOverviewSnapshotCacheStore` stores rows keyed by `contextName` and `namespace`.
// Default file: `~/Library/Application Support/Rune/overview-snapshot-cache.json`. Older installs may have used
// `~/Library/Caches/Rune/`; that file is imported once then deleted. In-RAM resource lists live in `ResourceStore`.

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
            persistToDisk()
            return nil
        }

        entry.lastAccessedAt = now
        entriesByKey[key] = entry
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
            nodesCount: snapshot.nodesCount,
            clusterCPUPercent: snapshot.clusterCPUPercent,
            clusterMemoryPercent: snapshot.clusterMemoryPercent,
            events: snapshot.events
        )

        entriesByKey[Self.key(contextName: normalized.contextName, namespace: normalized.namespace)] = normalized
        prune(reference: now)
        persistToDisk()
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
        prune(reference: nowProvider())

        if migratedFromLegacy {
            persistToDisk()
        }
    }

    private func prune(reference: Date) {
        entriesByKey = entriesByKey.filter { _, entry in
            reference.timeIntervalSince(entry.fetchedAt) <= retentionTTL
        }

        guard entriesByKey.count > maxEntries else { return }
        let sortedByAccessAscending = entriesByKey.values.sorted { lhs, rhs in
            lhs.lastAccessedAt < rhs.lastAccessedAt
        }

        let removeCount = entriesByKey.count - maxEntries
        for entry in sortedByAccessAscending.prefix(removeCount) {
            entriesByKey.removeValue(forKey: Self.key(contextName: entry.contextName, namespace: entry.namespace))
        }
    }

    private func persistToDisk() {
        let entries = entriesByKey.values.sorted { lhs, rhs in
            lhs.lastAccessedAt > rhs.lastAccessedAt
        }

        let payload = FilePayload(schemaVersion: Self.schemaVersion, entries: entries)
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601

        guard let data = try? encoder.encode(payload) else { return }

        let directory = fileURL.deletingLastPathComponent()
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        try? data.write(to: fileURL, options: [.atomic])
    }

    private static func key(contextName: String, namespace: String) -> String {
        "\(contextName)::\(namespace)"
    }

    /// Default path: `~/Library/Application Support/Rune/overview-snapshot-cache.json` (durable user data on macOS).
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
