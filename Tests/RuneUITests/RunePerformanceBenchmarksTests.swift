import Combine
import Foundation
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneSecurity
@testable import RuneUI

final class RunePerformanceBenchmarksTests: XCTestCase {
    private func seconds(_ duration: Duration) -> Double {
        Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
    }

    func testLogSearchBenchmarkKPI() {
        let text = (0..<20_000)
            .map { index in
                index.isMultiple(of: 40)
                    ? "ts=\(index) level=error component=api message=synthetic failure"
                    : "ts=\(index) level=info component=worker message=synthetic ok"
            }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = ResourceLogSearchResult.make(text: text, query: "error")
        }

        let started = ContinuousClock.now
        let result = ResourceLogSearchResult.make(text: text, query: "error")
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(result.matchingLineCount, 500)
        XCTAssertLessThan(seconds(elapsed), 0.25)
    }

    func testYAMLAnalysisBenchmarkKPI() {
        let manifest = (0..<800)
            .map { index in
                """
                ---
                apiVersion: v1
                kind: ConfigMap
                metadata:
                  name: synthetic-\(index)
                data:
                  mode: fast
                """
            }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = YAMLLanguageService.analyze(manifest)
        }

        let started = ContinuousClock.now
        let analysis = YAMLLanguageService.analyze(manifest)
        let elapsed = started.duration(to: .now)

        XCTAssertTrue(analysis.validationIssues.isEmpty)
        XCTAssertLessThan(seconds(elapsed), 0.75)
    }

    @MainActor
    func testColdStartBootstrapReturnPathKPI() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer()
        )

        measure(metrics: [XCTClockMetric()]) {
            let freshState = RuneAppState()
            let freshViewModel = RuneAppViewModel(
                state: freshState,
                kubeConfigDiscoverer: EmptyKubeConfigDiscoverer()
            )
            freshViewModel.bootstrapIfNeeded()
        }

        let started = ContinuousClock.now
        viewModel.bootstrapIfNeeded()
        let elapsed = started.duration(to: .now)

        XCTAssertLessThan(seconds(elapsed), 0.02)
    }

    @MainActor
    func testColdStartEmptyBootstrapDoesNotChurnVisibleLoadingStateKPI() async {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer()
        )
        var stateChangeCount = 0
        let cancellable = state.objectWillChange.sink {
            stateChangeCount += 1
        }
        defer { cancellable.cancel() }

        let started = ContinuousClock.now
        viewModel.bootstrapIfNeeded()
        for _ in 0..<10 {
            await Task.yield()
        }
        let elapsed = started.duration(to: .now)

        XCTAssertFalse(state.isLoading)
        XCTAssertTrue(state.contexts.isEmpty)
        XCTAssertLessThanOrEqual(stateChangeCount, 1)
        XCTAssertLessThan(seconds(elapsed), 0.02)
        XCTAssertTrue(viewModel.isLaunchExperienceVisible)
    }

    @MainActor
    func testLaunchExperienceCoversFastBootstrapThenFinishesKPI() async {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer()
        )

        XCTAssertTrue(viewModel.isLaunchExperienceVisible)

        let started = ContinuousClock.now
        viewModel.bootstrapIfNeeded()
        while viewModel.isLaunchExperienceVisible {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        let elapsed = started.duration(to: .now)

        XCTAssertGreaterThanOrEqual(seconds(elapsed), 0.10)
        XCTAssertLessThan(seconds(elapsed), 0.30)
        XCTAssertFalse(state.isLoading)
    }

    @MainActor
    func testColdStartBootstrapDefersDiscoveryUntilAfterFirstPaintKPI() async {
        let discoverer = CountingKubeConfigDiscoverer()
        let viewModel = RuneAppViewModel(kubeConfigDiscoverer: discoverer)

        viewModel.bootstrapIfNeeded()
        XCTAssertEqual(discoverer.callCount, 0)

        let started = ContinuousClock.now
        for _ in 0..<10 where discoverer.callCount == 0 {
            await Task.yield()
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(discoverer.callCount, 1)
        XCTAssertLessThan(seconds(elapsed), 0.02)
    }

    @MainActor
    func testColdStartViewModelInitializationKPI() {
        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for _ in 0..<200 {
                _ = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
            }
        }

        let started = ContinuousClock.now
        for _ in 0..<200 {
            _ = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
        }
        let elapsed = started.duration(to: .now)

        XCTAssertLessThan(seconds(elapsed), 0.20)
    }

    @MainActor
    func testColdStartRootShellConstructionKPI() {
        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for _ in 0..<200 {
                let viewModel = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
                _ = RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: nil,
                    debugDisableBootstrap: true
                )
            }
        }

        let started = ContinuousClock.now
        for _ in 0..<200 {
            let viewModel = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
            _ = RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: nil,
                debugDisableBootstrap: true
            )
        }
        let elapsed = started.duration(to: .now)

        XCTAssertLessThan(seconds(elapsed), 0.30)
    }

    @MainActor
    func testFavoriteResourceSortingBenchmarkKPI() {
        let state = RuneAppState()
        let suiteName = "RunePerformanceBenchmarksTests.favoriteSort.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let viewModel = RuneAppViewModel(
            state: state,
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: defaults)
        )
        state.selectedContext = KubeContext(name: "benchmark")
        state.selectedNamespace = "default"
        state.setConfigMaps((0..<5_000).map { index in
            ClusterResourceSummary(
                kind: .configMap,
                name: "config-\(String(format: "%04d", 4_999 - index))",
                namespace: "default",
                primaryText: "1 key",
                secondaryText: "1m"
            )
        })
        for index in stride(from: 0, to: 5_000, by: 250) {
            viewModel.toggleFavoriteResource(
                kind: .configMap,
                namespace: "default",
                name: "config-\(String(format: "%04d", index))"
            )
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = viewModel.visibleConfigMaps
        }

        let started = ContinuousClock.now
        let visible = viewModel.visibleConfigMaps
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(visible.count, 5_000)
        XCTAssertEqual(visible.prefix(3).map(\.name), ["config-0000", "config-0250", "config-0500"])
        XCTAssertLessThan(seconds(elapsed), 0.35)
    }

    @MainActor
    func testTerminalSessionAppendBenchmarkKPI() {
        let state = RuneAppState()
        state.setTerminalSession(PodTerminalSession(
            id: "shell",
            contextName: "benchmark",
            namespace: "default",
            podName: "api-0",
            shell: "sh",
            status: .connected
        ))

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let measuredState = RuneAppState()
            measuredState.setTerminalSession(PodTerminalSession(
                id: "shell",
                contextName: "benchmark",
                namespace: "default",
                podName: "api-0",
                shell: "sh",
                status: .connected
            ))
            for index in 0..<1_000 {
                measuredState.appendTerminalSessionOutput(id: "shell", text: "line \(index)\n")
            }
        }

        let started = ContinuousClock.now
        for index in 0..<1_000 {
            state.appendTerminalSessionOutput(id: "shell", text: "line \(index)\n")
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(state.terminalSessions.count, 1)
        XCTAssertTrue(state.terminalSession?.transcript.contains("line 999") == true)
        XCTAssertLessThan(seconds(elapsed), 0.20)
    }

    func testTerminalTranscriptSearchBenchmarkKPI() {
        let transcript = (0..<25_000)
            .map { index in
                index.isMultiple(of: 50)
                    ? "pod=pod-\(index) status=error path=/very/long/synthetic/path/\(index)/with/a/wide/terminal/line"
                    : "pod=pod-\(index) status=ok path=/very/long/synthetic/path/\(index)/with/a/wide/terminal/line"
            }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = TerminalTranscriptSearchIndex(text: transcript, query: "status=error", matchCase: false)
        }

        let started = ContinuousClock.now
        let index = TerminalTranscriptSearchIndex(text: transcript, query: "status=error", matchCase: false)
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(index.ranges.count, 500)
        XCTAssertEqual(index.statusText(selectedIndex: 0), "1 of 500")
        XCTAssertLessThan(seconds(elapsed), 0.25)
    }

    @MainActor
    func testTerminalShellViewCompositionBenchmarkKPI() {
        let pods = (0..<500).map { index in
            PodSummary(
                name: "pod-\(String(format: "%04d", index))",
                namespace: "default",
                status: index.isMultiple(of: 7) ? "Pending" : "Running"
            )
        }
        let sessions = (0..<64).map { index in
            PodTerminalSession(
                id: "shell-\(index)",
                contextName: "benchmark",
                namespace: "default",
                podName: "pod-\(String(format: "%04d", index))",
                shell: "sh",
                transcript: "line \(index)\n",
                status: index.isMultiple(of: 5) ? .disconnected : .connected
            )
        }
        let selectedPod = pods[10]
        let activeSession = sessions[10]

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for _ in 0..<500 {
                _ = TerminalShellPanelView(
                    session: activeSession,
                    sessions: sessions,
                    activeSessionID: activeSession.id,
                    isComposingNewSession: false,
                    selectedPod: selectedPod,
                    availablePods: pods,
                    canApplyMutations: true,
                    transcriptHeight: 320,
                    selectedShellPodID: .constant(selectedPod.id),
                    terminalInput: .constant(""),
                    onStartSession: { _ in },
                    onReconnectSession: { _, _ in },
                    onSend: {},
                    onDisconnect: {},
                    onSelectSession: { _ in },
                    onCloseSession: { _ in },
                    onComposeNewSession: {},
                    onClearTranscript: {}
                )
            }
        }

        let started = ContinuousClock.now
        for _ in 0..<500 {
            _ = TerminalShellPanelView(
                session: activeSession,
                sessions: sessions,
                activeSessionID: activeSession.id,
                isComposingNewSession: false,
                selectedPod: selectedPod,
                availablePods: pods,
                canApplyMutations: true,
                transcriptHeight: 320,
                selectedShellPodID: .constant(selectedPod.id),
                terminalInput: .constant(""),
                onStartSession: { _ in },
                onReconnectSession: { _, _ in },
                onSend: {},
                onDisconnect: {},
                onSelectSession: { _ in },
                onCloseSession: { _ in },
                onComposeNewSession: {},
                onClearTranscript: {}
            )
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(sessions.count, 64)
        XCTAssertEqual(pods.count, 500)
        XCTAssertLessThan(seconds(elapsed), 0.10)
    }

    @MainActor
    func testWriteAuditAppendAndCapBenchmarkKPI() {
        let state = RuneAppState()

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let measuredState = RuneAppState()
            for index in 0..<1_000 {
                measuredState.appendWriteAuditEntry(WriteAuditEntry(
                    action: "Apply YAML",
                    contextName: "benchmark",
                    namespace: "default",
                    resource: "configmap/settings-\(index)",
                    status: "Succeeded",
                    message: "ok"
                ))
            }
        }

        let started = ContinuousClock.now
        for index in 0..<1_000 {
            state.appendWriteAuditEntry(WriteAuditEntry(
                action: "Apply YAML",
                contextName: "benchmark",
                namespace: "default",
                resource: "configmap/settings-\(index)",
                status: "Succeeded",
                message: "ok"
            ))
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(state.writeAuditLog.count, 200)
        XCTAssertEqual(state.writeAuditLog.first?.resource, "configmap/settings-999")
        XCTAssertLessThan(seconds(elapsed), 0.20)
    }

    func testYAMLDiffPreviewBenchmarkKPI() {
        let baseline = (0..<2_000).map { index in "key\(index): old" }.joined(separator: "\n")
        let edited = (0..<2_000).map { index in "key\(index): new" }.joined(separator: "\n")
        let action = PendingWriteAction.apply(kind: .configMap, name: "settings", yaml: edited, baseline: baseline)

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = action.message
        }

        let started = ContinuousClock.now
        let message = action.message
        let elapsed = started.duration(to: .now)

        XCTAssertTrue(message.contains("YAML diff preview"))
        XCTAssertTrue(message.contains("diff truncated"))
        XCTAssertLessThan(seconds(elapsed), 0.10)
    }
}

private struct EmptyKubeConfigDiscoverer: KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL] {
        []
    }
}

private final class CountingKubeConfigDiscoverer: KubeConfigDiscovering {
    private(set) var callCount = 0

    func discoverCandidateFiles() -> [URL] {
        callCount += 1
        return []
    }
}
