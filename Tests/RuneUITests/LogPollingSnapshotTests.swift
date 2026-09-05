import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class LogPollingSnapshotTests: XCTestCase {
    func testPodPollingReplacesIdenticalAndGrowingFullSnapshots() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        viewModel.isLogTailModeEnabled = true

        let initial = """
        seq=1 status=ready
        seq=2 status=ready
        """
        let growing = """
        seq=1 status=ready
        seq=2 status=ready
        seq=3 status=ready
        """

        viewModel.commitPodLogFetch(
            initial,
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            podName: "synthetic-pod"
        )
        viewModel.commitPodLogFetch(
            initial,
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            podName: "synthetic-pod"
        )

        XCTAssertEqual(occurrences(of: "seq=1 status=ready", in: state.podLogs), 1)
        XCTAssertEqual(occurrences(of: "seq=2 status=ready", in: state.podLogs), 1)

        viewModel.commitPodLogFetch(
            growing,
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            podName: "synthetic-pod"
        )

        XCTAssertEqual(occurrences(of: "seq=1 status=ready", in: state.podLogs), 1)
        XCTAssertEqual(occurrences(of: "seq=2 status=ready", in: state.podLogs), 1)
        XCTAssertEqual(occurrences(of: "seq=3 status=ready", in: state.podLogs), 1)
        XCTAssertEqual(occurrences(of: "Context: synthetic-context", in: state.podLogs), 1)
    }

    func testUnifiedPollingReplacesSnapshotsWhenNewLinesAppearInsideMultiplePodGroups() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        viewModel.isLogTailModeEnabled = true

        let initial = """
        [synthetic-pod-a] seq=1
        [synthetic-pod-b] seq=1
        """
        let growing = """
        [synthetic-pod-a] seq=1
        [synthetic-pod-a] seq=2
        [synthetic-pod-b] seq=1
        [synthetic-pod-b] seq=2
        """

        viewModel.commitUnifiedLogFetch(
            initial,
            pods: ["synthetic-pod-a", "synthetic-pod-b"],
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            kind: .deployment,
            resourceName: "synthetic-workload"
        )
        viewModel.commitUnifiedLogFetch(
            growing,
            pods: ["synthetic-pod-a", "synthetic-pod-b"],
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            kind: .deployment,
            resourceName: "synthetic-workload"
        )
        viewModel.commitUnifiedLogFetch(
            growing,
            pods: ["synthetic-pod-a", "synthetic-pod-b"],
            contextName: "synthetic-context",
            namespace: "synthetic-namespace",
            kind: .deployment,
            resourceName: "synthetic-workload"
        )

        XCTAssertEqual(occurrences(of: "[synthetic-pod-a] seq=1", in: state.unifiedServiceLogs), 1)
        XCTAssertEqual(occurrences(of: "[synthetic-pod-a] seq=2", in: state.unifiedServiceLogs), 1)
        XCTAssertEqual(occurrences(of: "[synthetic-pod-b] seq=1", in: state.unifiedServiceLogs), 1)
        XCTAssertEqual(occurrences(of: "[synthetic-pod-b] seq=2", in: state.unifiedServiceLogs), 1)
        XCTAssertEqual(occurrences(of: "Context: synthetic-context", in: state.unifiedServiceLogs), 1)
    }

    private func occurrences(of needle: String, in text: String) -> Int {
        text.components(separatedBy: needle).count - 1
    }
}
