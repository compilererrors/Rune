import Foundation
import XCTest
@testable import RuneUI

final class RuneSingleFlightLatestPendingWorkLaneTests: XCTestCase {
    func testContinuousSubmissionsMakeProgressAndEventuallyPublishLatest() async throws {
        let lane = RuneSingleFlightLatestPendingWorkLane<SyntheticRenderSnapshot>()
        let firstWorkGate = SyntheticWorkGate()
        let producerState = SyntheticProducerState()

        let firstSubmission = Task {
            await lane.submit(priority: .userInitiated) {
                firstWorkGate.signalStarted()
                firstWorkGate.waitForRelease()
                return SyntheticRenderSnapshot(sequence: 0, text: "batch-0")
            }
        }
        XCTAssertTrue(firstWorkGate.waitUntilStarted(timeout: 1))

        let producer = Task {
            var lastSubmission: Task<SyntheticRenderSnapshot?, Never>?
            for sequence in 1...40 {
                let snapshot = SyntheticRenderSnapshot(
                    sequence: sequence,
                    text: "batch-\(sequence)"
                )
                lastSubmission = Task {
                    await lane.submit(priority: .userInitiated) {
                        snapshot
                    }
                }
                try await Task.sleep(nanoseconds: 5_000_000)
            }
            producerState.markFinished()
            return lastSubmission
        }

        try await Task.sleep(nanoseconds: 40_000_000)
        firstWorkGate.release()

        let firstPublished = await firstSubmission.value
        XCTAssertEqual(firstPublished?.sequence, 0)
        XCTAssertFalse(
            producerState.isFinished,
            "The in-flight snapshot must publish while newer appends are still arriving."
        )

        let optionalLastSubmission = try await producer.value
        let lastSubmission = try XCTUnwrap(optionalLastSubmission)
        let latestPublished = await lastSubmission.value
        XCTAssertEqual(latestPublished?.sequence, 40)
        XCTAssertEqual(latestPublished?.text, "batch-40")
    }

    func testAppendSnapshotsAreRenderableButReplacementSnapshotsAreNot() {
        let terminal = TerminalTranscriptRenderModel(
            text: "needle-1\n",
            query: "needle",
            matchCase: false,
            usesLargeTextSurface: true
        )
        XCTAssertTrue(terminal.isRenderableSnapshot(for: "needle-1\nneedle-2\n"))
        XCTAssertTrue(
            terminal.isSearchNavigableSnapshot(
                for: "needle-1\nneedle-2\n",
                query: "needle",
                matchCase: false
            ),
            "Known matches must stay navigable while append-only terminal output is still being indexed."
        )
        XCTAssertFalse(
            terminal.isSearchNavigableSnapshot(
                for: "needle-1\nneedle-2\n",
                query: "different",
                matchCase: false
            )
        )
        XCTAssertFalse(terminal.isRenderableSnapshot(for: "different-session\n"))

        let logs = ResourceLogSearchResult.make(text: "event-1\n", query: "event")
        XCTAssertTrue(logs.isRenderableSnapshot(for: "event-1\nevent-2\n"))
        XCTAssertTrue(
            logs.isSearchNavigableSnapshot(
                for: "event-1\nevent-2\n",
                query: "event",
                matchCase: false
            ),
            "Known matches must stay navigable while append-only logs are still being indexed."
        )
        XCTAssertFalse(
            logs.isSearchNavigableSnapshot(
                for: "event-1\nevent-2\n",
                query: "event",
                matchCase: true
            )
        )
        XCTAssertFalse(logs.isRenderableSnapshot(for: "different-resource\n"))
    }

    func testStreamingSurfacesSubmitTextChangesOnTheLeadingEdge() throws {
        let repositoryRoot = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let terminal = try String(
            contentsOf: repositoryRoot
                .appendingPathComponent("Sources/RuneUI/Views/TerminalTranscriptSurface.swift"),
            encoding: .utf8
        )
        let logs = try String(
            contentsOf: repositoryRoot
                .appendingPathComponent("Sources/RuneUI/Views/ResourceLogsInspectorView.swift"),
            encoding: .utf8
        )

        XCTAssertTrue(terminal.contains("textDebounceNanoseconds: UInt64 = 0"))
        XCTAssertTrue(logs.contains("streamedTextSearchDebounceNanoseconds: UInt64 = 0"))
        XCTAssertTrue(terminal.contains("RuneSingleFlightLatestPendingWorkLane<TerminalTranscriptRenderModel>()"))
        XCTAssertTrue(logs.contains("RuneSingleFlightLatestPendingWorkLane<ResourceLogSearchResult>()"))
        XCTAssertTrue(terminal.contains("model.isSearchNavigableSnapshot("))
        XCTAssertTrue(logs.contains("searchResult.isSearchNavigableSnapshot("))
        XCTAssertTrue(terminal.contains("scrollsOnTargetLineChange: false"))
        XCTAssertTrue(logs.contains("largeTextScrollsOnTargetLineChange: false"))
        XCTAssertTrue(logs.contains("if isInitialSearchNavigationPending {"))
        XCTAssertFalse(
            logs.contains("if result.hasMatches {\n                searchNavigationSequence"),
            "Append-only index publications must not create navigation intent."
        )
        XCTAssertFalse(
            terminal.contains(
                "searchNavigationRevision &* 1_000_003 &+ publishedSearchRevision"
            ),
            "Append-only terminal index publications must not create scroll intent."
        )
    }

    @MainActor
    func testLargeTerminalDefersNavigationRevisionUntilSearchPublishes() {
        XCTAssertNil(TerminalTranscriptSurface.largeTextSearchNavigationRevision(
            normalizedQuery: "needle",
            searchIndex: nil,
            navigationRevision: 7
        ))

        let noMatches = TerminalTranscriptSearchIndex(
            text: "plain output",
            query: "needle",
            matchCase: false
        )
        XCTAssertEqual(
            TerminalTranscriptSurface.largeTextSearchNavigationRevision(
                normalizedQuery: "needle",
                searchIndex: noMatches,
                navigationRevision: 7
            ),
            7,
            "A completed no-match result must consume the query's navigation intent."
        )

        let match = TerminalTranscriptSearchIndex(
            text: "needle output",
            query: "needle",
            matchCase: false
        )
        XCTAssertEqual(
            TerminalTranscriptSurface.largeTextSearchNavigationRevision(
                normalizedQuery: "needle",
                searchIndex: match,
                navigationRevision: 8
            ),
            8,
            "The first matching publication must deliver the pending navigation intent."
        )
    }
}

private struct SyntheticRenderSnapshot: Sendable {
    let sequence: Int
    let text: String
}

private final class SyntheticWorkGate: @unchecked Sendable {
    private let started = DispatchSemaphore(value: 0)
    private let releaseSignal = DispatchSemaphore(value: 0)

    func signalStarted() {
        started.signal()
    }

    func waitUntilStarted(timeout: TimeInterval) -> Bool {
        started.wait(timeout: .now() + timeout) == .success
    }

    func waitForRelease() {
        releaseSignal.wait()
    }

    func release() {
        releaseSignal.signal()
    }
}

private final class SyntheticProducerState: @unchecked Sendable {
    private let lock = NSLock()
    private var finished = false

    var isFinished: Bool {
        lock.lock()
        defer { lock.unlock() }
        return finished
    }

    func markFinished() {
        lock.lock()
        finished = true
        lock.unlock()
    }
}
