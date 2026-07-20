import Foundation
import XCTest
@testable import RuneSecurity

final class BookmarkStoreTransactionTests: XCTestCase {
    func testBatchBookmarkFailureRestoresOriginalRecords() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("BookmarkStoreTransactionTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let first = directory.appendingPathComponent("first.yaml")
        let second = directory.appendingPathComponent("second.yaml")
        try "contexts: []".write(to: first, atomically: true, encoding: .utf8)
        try "contexts: []".write(to: second, atomically: true, encoding: .utf8)
        let existing = BookmarkRecord(path: "/synthetic/existing.yaml", bookmarkData: Data([1]))
        let store = MutatingThenFailingBookmarkStore(records: [existing])

        XCTAssertThrowsError(
            try BookmarkManager(store: store).addKubeConfigs(urls: [first, second])
        )
        XCTAssertEqual(store.records, [existing])
        XCTAssertEqual(store.saveCallCount, 2)
    }

    func testConcurrentBookmarkAddsSerializeLoadMutateAndSave() throws {
        let store = FirstLoadBlockingBookmarkStore()
        let manager = BookmarkManager(
            store: store,
            createBookmarkData: { Data($0.path.utf8) },
            resolveBookmarkData: { data in
                (URL(fileURLWithPath: String(decoding: data, as: UTF8.self)), false)
            }
        )
        let first = URL(fileURLWithPath: "/synthetic/first.yaml")
        let second = URL(fileURLWithPath: "/synthetic/second.yaml")
        let queue = DispatchQueue(label: "BookmarkStoreTransactionTests.concurrent", attributes: .concurrent)
        let group = DispatchGroup()
        let failures = LockedFailures()
        let secondAttemptStarted = DispatchSemaphore(value: 0)

        group.enter()
        queue.async {
            defer { group.leave() }
            do {
                try manager.addKubeConfig(url: first)
            } catch {
                failures.append(error)
            }
        }
        XCTAssertEqual(store.firstLoadReached.wait(timeout: .now() + 1), .success)

        group.enter()
        queue.async {
            defer { group.leave() }
            secondAttemptStarted.signal()
            do {
                try manager.addKubeConfig(url: second)
            } catch {
                failures.append(error)
            }
        }
        XCTAssertEqual(secondAttemptStarted.wait(timeout: .now() + 1), .success)

        // Give an unprotected second transaction a deterministic opportunity to read the
        // first transaction's old snapshot. A serialized manager keeps it outside the store.
        _ = store.secondLoadReached.wait(timeout: .now() + 0.1)
        store.allowFirstLoad.signal()

        XCTAssertEqual(group.wait(timeout: .now() + 2), .success)
        XCTAssertTrue(failures.values.isEmpty)
        XCTAssertEqual(Set(store.records.map(\.path)), Set([first.path, second.path]))
        XCTAssertFalse(store.observedStaleConcurrentLoad)
    }

    func testStaleBookmarkRefreshCompletesInsideOneTransactionWithoutReentrantDeadlock() {
        let oldData = Data("old".utf8)
        let refreshedData = Data("refreshed".utf8)
        let resolvedURL = URL(fileURLWithPath: "/synthetic/refreshed.yaml")
        let store = RecordingBookmarkStore(records: [
            BookmarkRecord(path: "/synthetic/old.yaml", bookmarkData: oldData)
        ])
        let manager = BookmarkManager(
            store: store,
            createBookmarkData: { url in
                XCTAssertEqual(url, resolvedURL)
                return refreshedData
            },
            resolveBookmarkData: { data in
                XCTAssertEqual(data, oldData)
                return (resolvedURL, true)
            }
        )
        let completed = expectation(description: "stale bookmark refresh")
        let result = LockedSourceResult()

        DispatchQueue.global(qos: .userInitiated).async {
            defer { completed.fulfill() }
            do {
                result.set(.success(try manager.loadKubeConfigSources().map { $0.url.path }))
            } catch {
                result.set(.failure(error))
            }
        }

        wait(for: [completed], timeout: 1)
        guard case let .success(sources)? = result.value else {
            return XCTFail("Expected stale bookmark refresh to finish successfully")
        }
        XCTAssertEqual(sources, [resolvedURL.path])
        XCTAssertEqual(store.records, [BookmarkRecord(path: resolvedURL.path, bookmarkData: refreshedData)])
        XCTAssertEqual(store.saveCallCount, 1)
    }

    func testRollbackRemainsSerializedBeforeAConcurrentAddCanLoad() {
        let existing = BookmarkRecord(path: "/synthetic/existing.yaml", bookmarkData: Data("existing".utf8))
        let store = BlockingMutatingFailureBookmarkStore(records: [existing])
        let manager = BookmarkManager(
            store: store,
            createBookmarkData: { Data($0.path.utf8) },
            resolveBookmarkData: { data in
                (URL(fileURLWithPath: String(decoding: data, as: UTF8.self)), false)
            }
        )
        let rejected = URL(fileURLWithPath: "/synthetic/rejected.yaml")
        let accepted = URL(fileURLWithPath: "/synthetic/accepted.yaml")
        let queue = DispatchQueue(label: "BookmarkStoreTransactionTests.rollback", attributes: .concurrent)
        let group = DispatchGroup()
        let failures = LockedFailures()
        let secondAttemptStarted = DispatchSemaphore(value: 0)

        group.enter()
        queue.async {
            defer { group.leave() }
            do {
                try manager.addKubeConfig(url: rejected)
            } catch {
                failures.append(error)
            }
        }
        XCTAssertEqual(store.partialWriteReached.wait(timeout: .now() + 1), .success)

        group.enter()
        queue.async {
            defer { group.leave() }
            secondAttemptStarted.signal()
            do {
                try manager.addKubeConfig(url: accepted)
            } catch {
                failures.append(error)
            }
        }
        XCTAssertEqual(secondAttemptStarted.wait(timeout: .now() + 1), .success)
        store.allowFirstSaveToFail.signal()

        XCTAssertEqual(group.wait(timeout: .now() + 2), .success)
        XCTAssertEqual(failures.values.count, 1)
        XCTAssertFalse(store.observedLoadDuringPartialWrite)
        XCTAssertEqual(store.records, [
            existing,
            BookmarkRecord(path: accepted.path, bookmarkData: Data(accepted.path.utf8))
        ])
        XCTAssertEqual(store.saveCallCount, 3)
    }
}

private final class LockedFailures: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [Error] = []

    var values: [Error] { lock.withLock { storage } }

    func append(_ error: Error) {
        lock.withLock { storage.append(error) }
    }
}

private final class LockedSourceResult: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: Result<[String], Error>?

    var value: Result<[String], Error>? { lock.withLock { storage } }

    func set(_ value: Result<[String], Error>) {
        lock.withLock { storage = value }
    }
}

private final class FirstLoadBlockingBookmarkStore: BookmarkStore, @unchecked Sendable {
    let firstLoadReached = DispatchSemaphore(value: 0)
    let secondLoadReached = DispatchSemaphore(value: 0)
    let allowFirstLoad = DispatchSemaphore(value: 0)
    private let lock = NSLock()
    private var storedRecords: [BookmarkRecord] = []
    private var loadCallCount = 0
    private var firstSaveCompleted = false
    private(set) var observedStaleConcurrentLoad = false

    var records: [BookmarkRecord] { lock.withLock { storedRecords } }

    func loadRecords() throws -> [BookmarkRecord] {
        let snapshot: [BookmarkRecord]
        let call: Int
        lock.lock()
        loadCallCount += 1
        call = loadCallCount
        snapshot = storedRecords
        if call == 2, !firstSaveCompleted {
            observedStaleConcurrentLoad = true
        }
        lock.unlock()

        if call == 1 {
            firstLoadReached.signal()
            _ = allowFirstLoad.wait(timeout: .now() + 2)
        } else if call == 2 {
            secondLoadReached.signal()
        }
        return snapshot
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        lock.withLock {
            storedRecords = records
            if !firstSaveCompleted {
                firstSaveCompleted = true
            }
        }
    }
}

private final class RecordingBookmarkStore: BookmarkStore, @unchecked Sendable {
    private let lock = NSLock()
    private var storedRecords: [BookmarkRecord]
    private(set) var saveCallCount = 0

    init(records: [BookmarkRecord]) {
        storedRecords = records
    }

    var records: [BookmarkRecord] { lock.withLock { storedRecords } }

    func loadRecords() throws -> [BookmarkRecord] {
        lock.withLock { storedRecords }
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        lock.withLock {
            saveCallCount += 1
            storedRecords = records
        }
    }
}

private final class BlockingMutatingFailureBookmarkStore: BookmarkStore, @unchecked Sendable {
    private enum Failure: Error {
        case rejected
    }

    let partialWriteReached = DispatchSemaphore(value: 0)
    let allowFirstSaveToFail = DispatchSemaphore(value: 0)
    private let lock = NSLock()
    private var storedRecords: [BookmarkRecord]
    private var firstSaveInProgress = false
    private(set) var observedLoadDuringPartialWrite = false
    private(set) var saveCallCount = 0

    init(records: [BookmarkRecord]) {
        storedRecords = records
    }

    var records: [BookmarkRecord] { lock.withLock { storedRecords } }

    func loadRecords() throws -> [BookmarkRecord] {
        lock.withLock {
            if firstSaveInProgress {
                observedLoadDuringPartialWrite = true
            }
            return storedRecords
        }
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        let call: Int = lock.withLock {
            saveCallCount += 1
            storedRecords = records
            if saveCallCount == 1 {
                firstSaveInProgress = true
            } else if saveCallCount == 2 {
                firstSaveInProgress = false
            }
            return saveCallCount
        }
        guard call == 1 else { return }

        partialWriteReached.signal()
        _ = allowFirstSaveToFail.wait(timeout: .now() + 2)
        throw Failure.rejected
    }
}

private final class MutatingThenFailingBookmarkStore: BookmarkStore, @unchecked Sendable {
    private let lock = NSLock()
    private var storedRecords: [BookmarkRecord]
    private var shouldFail = true
    private(set) var saveCallCount = 0

    init(records: [BookmarkRecord]) {
        storedRecords = records
    }

    var records: [BookmarkRecord] {
        lock.withLock { storedRecords }
    }

    func loadRecords() throws -> [BookmarkRecord] {
        lock.withLock { storedRecords }
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {
        try lock.withLock {
            saveCallCount += 1
            storedRecords = records
            if shouldFail {
                shouldFail = false
                throw CocoaError(.fileWriteUnknown)
            }
        }
    }
}
