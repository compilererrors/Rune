import Foundation
import RuneSecurity

/// Kubeconfig bookmarks kept in memory, so a test never reads or writes the
/// bookmarks saved in the test runner's own preferences.
final class InMemoryBookmarkStore: BookmarkStore, @unchecked Sendable {
    private let lock = NSLock()
    private var storedRecords: [BookmarkRecord]

    init(records: [BookmarkRecord] = []) {
        storedRecords = records
    }

    var records: [BookmarkRecord] {
        lock.withLock { storedRecords }
    }

    func loadRecords() throws -> [BookmarkRecord] {
        records
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        lock.withLock { storedRecords = records }
    }

    func removeAllRecords() {
        lock.withLock { storedRecords = [] }
    }
}

extension BookmarkManager {
    /// A bookmark manager that starts empty and keeps its bookmarks in memory.
    static func inMemory() -> BookmarkManager {
        BookmarkManager(store: InMemoryBookmarkStore())
    }
}
