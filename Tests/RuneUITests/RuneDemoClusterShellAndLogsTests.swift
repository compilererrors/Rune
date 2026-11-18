import RuneCore
import RuneKube
@testable import RuneUI
import XCTest

/// The demo cluster lives in memory: its shell must not reach for a real
/// cluster (which failed with "No kubeconfig selected"), and reloading its
/// logs must show them once, as a real cluster's reload replaces them.
@MainActor
final class RuneDemoClusterShellAndLogsTests: XCTestCase {
    private var previousDemoSetting: Any?

    override func setUp() async throws {
        previousDemoSetting = UserDefaults.standard.object(forKey: RuneSettingsKeys.enableDemoCluster)
        UserDefaults.standard.runeEnableDemoCluster = true
    }

    override func tearDown() async throws {
        if let previousDemoSetting {
            UserDefaults.standard.set(previousDemoSetting, forKey: RuneSettingsKeys.enableDemoCluster)
        } else {
            UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.enableDemoCluster)
        }
    }

    func testTheDemoShellConnectsInMemoryAndAnswersCommands() async throws {
        let realShell = RecordingShellBackend()
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, terminalBackend: realShell)
        viewModel.loadDemoCluster()
        let pod = try XCTUnwrap(state.pods.first)

        viewModel.startTerminalSession(for: pod)
        try await waitUntil { state.terminalSession?.status == .connected && state.terminalSession?.transcript.contains(":/app#") == true }
        XCTAssertNil(state.lastError, "the demo shell needs no kubeconfig")
        XCTAssertEqual(realShell.starts, 0, "the demo never opens a shell on a real cluster")
        let transcript = state.terminalSession?.transcript ?? ""
        let connected = try XCTUnwrap(transcript.range(of: "[rune] Connected"), transcript)
        let banner = try XCTUnwrap(transcript.range(of: "Rune demo shell"), "the banner says it is a demo shell")
        XCTAssertLessThan(connected.lowerBound, banner.lowerBound, "Rune's connected line comes before the shell's prompt")

        viewModel.terminalSessionInput = "hostname"
        viewModel.sendTerminalSessionInput()
        try await waitUntil { state.terminalSession?.transcript.contains(pod.name) == true && state.terminalSession?.transcript.components(separatedBy: pod.name).count ?? 0 > 2 }

        // Typing in the terminal itself: characters echo, Return runs the line.
        for key in ["p", "w", "d", "\r"] { viewModel.sendTerminalControlSequence(key) }
        try await waitUntil { state.terminalSession?.transcript.contains("/app") == true }

        viewModel.terminalSessionInput = "kubectl get nodes"
        viewModel.sendTerminalSessionInput()
        try await waitUntil { state.terminalSession?.transcript.contains("not available in the demo shell") == true }
        XCTAssertEqual(realShell.starts + realShell.writes, 0)
    }

    func testReloadingDemoPodLogsShowsThemOnce() async throws {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, terminalBackend: RecordingShellBackend())
        viewModel.loadDemoCluster()
        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.pod)
        viewModel.selectPod(state.pods.first)
        try await waitUntil { state.podLogs.contains("started demo API") }

        for _ in 0..<3 {
            viewModel.reloadLogsForSelection()
        }
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(state.podLogs.components(separatedBy: "started demo API").count - 1, 1, "reload replaces the demo logs")

        viewModel.setWorkloadKind(.deployment)
        viewModel.selectDeployment(state.deployments.first)
        try await waitUntil { state.unifiedServiceLogs.contains("deployment rollout complete") }
        for _ in 0..<3 {
            viewModel.reloadLogsForSelection()
        }
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertEqual(state.unifiedServiceLogs.components(separatedBy: "deployment rollout complete").count - 1, 1)
    }

    private func waitUntil(timeout: TimeInterval = 3, _ condition: () -> Bool, file: StaticString = #filePath, line: UInt = #line) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition() {
            if Date() > deadline { return XCTFail("timed out", file: file, line: line) }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }
}

/// Stands in for the real cluster's shell and counts every use of it.
private final class RecordingShellBackend: PodTerminalSessionBackend, @unchecked Sendable {
    private let lock = NSLock()
    private var startCount = 0
    private var writeCount = 0
    var starts: Int { lock.withLock { startCount } }
    var writes: Int { lock.withLock { writeCount } }

    func startPodTerminalSession(
        id sessionID: String, from sources: [KubeConfigSource], context: KubeContext, namespace: String,
        podName: String, container: String?, shellCommand: [String],
        onOutput: @escaping @Sendable (String) -> Void, onTermination: @escaping @Sendable (Int32) -> Void
    ) async throws {
        lock.withLock { startCount += 1 }
        throw RuneError.invalidInput(message: "No kubeconfig selected.")
    }

    func writeToPodTerminalSession(id: String, text: String) async throws { lock.withLock { writeCount += 1 } }
    func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws {}
    func stopPodTerminalSession(id: String) async {}
}
