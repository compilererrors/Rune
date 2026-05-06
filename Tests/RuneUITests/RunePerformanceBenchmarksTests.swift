import AppKit
import Combine
import Foundation
import SwiftUI
import XCTest
import struct RuneSharedCore.RuneLargeTextIndex
@testable import RuneCore
@testable import RuneExport
@testable import RuneFakeK8sSupport
@testable import RuneSecurity
@testable import RuneStore
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
        XCTAssertTrue(result.displayedText.contains("level=info"))
        XCTAssertLessThan(seconds(elapsed), 0.25)
    }

    func testLogSearchNavigationBenchmarkKPI() {
        let text = (0..<20_000)
            .map { index in
                index.isMultiple(of: 40)
                    ? "ts=\(index) level=error component=api message=synthetic failure"
                    : "ts=\(index) level=info component=worker message=synthetic ok"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.make(text: text, query: "error")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            var index = 0
            for _ in 0..<1_000 {
                index = result.nextMatchIndex(from: index)
                _ = result.navigationRequest(selectedIndex: index, sequence: index)
            }
        }

        let started = ContinuousClock.now
        var index = 0
        for _ in 0..<1_000 {
            index = result.nextMatchIndex(from: index)
            _ = result.navigationRequest(selectedIndex: index, sequence: index)
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(result.matchingLineCount, 500)
        XCTAssertLessThan(seconds(elapsed), 0.02)
    }

    func testLargeTextLineIndexBenchmarkKPI() {
        let text = (0..<60_000)
            .map { index in
                index.isMultiple(of: 40)
                    ? "ts=\(index) level=error component=api message=synthetic failure"
                    : "ts=\(index) level=info component=worker message=synthetic ok"
            }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let index = RuneLargeTextIndex(text: text)
            _ = index.viewport(startLine: 55_000, lineLimit: 80)
            _ = index.search(query: "error")
        }

        let started = ContinuousClock.now
        let index = RuneLargeTextIndex(text: text)
        let viewport = index.viewport(startLine: 55_000, lineLimit: 80)
        let result = index.search(query: "error")
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(index.lineCount, 60_000)
        XCTAssertEqual(viewport.lines.first?.text, "ts=54999 level=info component=worker message=synthetic ok")
        XCTAssertEqual(result.matches.count, 1_500)
        XCTAssertLessThan(seconds(elapsed), 0.35)
    }

    func testLogZipExportBenchmarkKPI() throws {
        let pods = (0..<12).map { "api-\($0)" }
        let text = (0..<24_000)
            .map { index in
                "[\(pods[index % pods.count])] 2026-05-05T10:00:\(String(format: "%02d", index % 60))Z message=\(index)"
            }
            .joined(separator: "\n")

        let started = ContinuousClock.now
        let zip = try LogArchiveBuilder.buildZip(
            mergedText: text,
            podNames: pods,
            baseName: "benchmark-logs",
            generatedAt: "20260505T100000Z"
        )
        let elapsed = started.duration(to: .now)

        XCTAssertGreaterThan(zip.count, text.utf8.count / 2)
        XCTAssertLessThan(seconds(elapsed), 0.45, "KPI: exporting a 24k-line, 12-pod log archive should stay below 450ms on local benchmark runs.")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = try? LogArchiveBuilder.buildZip(
                mergedText: text,
                podNames: pods,
                baseName: "benchmark-logs",
                generatedAt: "20260505T100000Z"
            )
        }
    }

    func testDeploymentPodLogZipExportBenchmarkKPI() throws {
        let records = (0..<12).flatMap { podIndex in
            ["app", "sidecar"].map { (containerName: String) in
                PodLogArchiveRecord(
                    podName: "api-\(podIndex)",
                    containerName: containerName,
                    logs: (0..<1_000)
                        .map { lineIndex in
                            "2026-05-06T10:00:\(String(format: "%02d", lineIndex % 60))Z pod=\(podIndex) container=\(containerName) message=\(lineIndex)"
                        }
                        .joined(separator: "\n")
                )
            }
        }

        let warmup = try LogArchiveBuilder.buildPodContainerZip(
            records: records,
            baseName: "deployment-api-pod-logs",
            generatedAt: "20260506T100000Z"
        )
        XCTAssertGreaterThan(warmup.count, 0)

        let started = ContinuousClock.now
        let zip = try LogArchiveBuilder.buildPodContainerZip(
            records: records,
            baseName: "deployment-api-pod-logs",
            generatedAt: "20260506T100000Z"
        )
        let elapsed = started.duration(to: .now)

        XCTAssertGreaterThan(zip.count, 0)
        XCTAssertLessThan(seconds(elapsed), 0.45, "KPI: exporting a 24k-line, 12-pod, 24-container deployment log archive should stay below 450ms on local benchmark runs.")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = try? LogArchiveBuilder.buildPodContainerZip(
                records: records,
                baseName: "deployment-api-pod-logs",
                generatedAt: "20260506T100000Z"
            )
        }
    }

    @MainActor
    func testLargeLogInspectorInitialMountBenchmarkKPI() {
        let text = (0..<20_000)
            .map { index in
                "INFO request-id=\(String(format: "%06d", index)) component=worker message=synthetic output"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.make(text: text, query: "")

        XCTAssertTrue(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let controller = NSHostingController(
                rootView: PodLogsInspectorPane(
                    selectedLogPreset: .constant(.recentLines),
                    includePreviousLogs: .constant(false),
                    selectedContainer: .constant(""),
                    isTailModeEnabled: .constant(false),
                    isStreamPaused: .constant(false),
                    isLoadingLogs: false,
                    isLoadingResources: false,
                    errorMessage: nil,
                    statusText: "Last updated 12:00:00",
                    containerOptions: [],
                    logText: text,
                    readOnlyResetID: "benchmark:logs",
                    onReload: {},
                    onSave: {},
                    onSaveVisibleZip: { _ in },
                    onSaveFullZip: {},
                    onSaveAllPodsZip: {},
                    onCopySelection: {},
                    onCopyAll: {},
                    onToggleStreamPause: {}
                )
            )
            controller.view.frame = NSRect(x: 0, y: 0, width: 900, height: 600)
            controller.view.layoutSubtreeIfNeeded()
        }

        let started = ContinuousClock.now
        let controller = NSHostingController(
            rootView: PodLogsInspectorPane(
                selectedLogPreset: .constant(.recentLines),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                isLoadingLogs: false,
                isLoadingResources: false,
                errorMessage: nil,
                statusText: "Last updated 12:00:00",
                containerOptions: [],
                logText: text,
                readOnlyResetID: "benchmark:logs",
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )
        )
        controller.view.frame = NSRect(x: 0, y: 0, width: 900, height: 600)
        controller.view.layoutSubtreeIfNeeded()
        let elapsed = started.duration(to: .now)

        XCTAssertLessThan(seconds(elapsed), 0.12)
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

        XCTAssertGreaterThanOrEqual(seconds(elapsed), 0.28)
        XCTAssertLessThan(seconds(elapsed), 0.50)
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
    func testColdStartLaunchShellInitialMountKPI() {
        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let viewModel = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
            let controller = NSHostingController(
                rootView: RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: nil
                )
            )
            controller.view.frame = NSRect(x: 0, y: 0, width: 1440, height: 900)
            controller.view.layoutSubtreeIfNeeded()
        }

        let started = ContinuousClock.now
        let viewModel = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
        let controller = NSHostingController(
            rootView: RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: nil
            )
        )
        controller.view.frame = NSRect(x: 0, y: 0, width: 1440, height: 900)
        controller.view.layoutSubtreeIfNeeded()
        let elapsed = started.duration(to: .now)

        XCTAssertLessThan(seconds(elapsed), 0.12)
    }

    @MainActor
    func testFakeRESTRapidViewSwitchBenchmarkKPI() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let state = RuneAppState()
        state.setSources([KubeConfigSource(url: kubeconfig)])
        let viewModel = RuneAppViewModel(
            state: state,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        try await viewModel.reloadContexts()
        server.resetRequestLines()

        let started = ContinuousClock.now
        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.deployment)
        viewModel.setSection(.networking)
        viewModel.setWorkloadKind(.service)
        viewModel.setSection(.config)
        viewModel.setWorkloadKind(.configMap)
        try await waitUntil {
            state.selectedSection == .config
                && state.selectedWorkloadKind == .configMap
                && state.configMaps.count == 2
                && !state.isLoading
                && !state.isLoadingResourceDetails
        }
        let elapsed = started.duration(to: .now)

        let resourcePath = "/api/v1/namespaces/alpha-zone/configmaps/ember-gate-settings"
        let finalResourceGETs = server.requestLines().filter { line in
            line.hasPrefix("GET \(resourcePath)") || line.hasPrefix("GET \(resourcePath)?")
        }

        XCTAssertEqual(finalResourceGETs.count, 2)
        XCTAssertNil(state.lastError)
        XCTAssertLessThan(seconds(elapsed), 0.75)
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
    func testFavoriteNamespaceSortingBenchmarkKPI() {
        let state = RuneAppState()
        let suiteName = "RunePerformanceBenchmarksTests.favoriteNamespaces.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let viewModel = RuneAppViewModel(
            state: state,
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: defaults)
        )
        state.selectedContext = KubeContext(name: "benchmark")
        state.setNamespaces((0..<2_000).map { "namespace-\(String(format: "%04d", 1_999 - $0))" })
        state.selectedNamespace = "namespace-0000"
        for index in stride(from: 0, to: 2_000, by: 200) {
            viewModel.toggleFavoriteNamespace("namespace-\(String(format: "%04d", index))")
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = viewModel.namespaceOptions
        }

        let started = ContinuousClock.now
        let visible = viewModel.namespaceOptions
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(visible.count, 2_000)
        XCTAssertEqual(visible.prefix(3), ["namespace-0000", "namespace-0200", "namespace-0400"])
        XCTAssertLessThan(seconds(elapsed), 0.2)
    }

    @MainActor
    func testPodLogContainerOptionsBenchmarkKPI() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.setSelectedPod(PodSummary(
            name: "api",
            namespace: "default",
            status: "Running",
            containerNamesLine: (0..<200).map { "container-\($0)" }.joined(separator: ", ")
        ))

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = viewModel.podLogContainerOptions
        }

        let started = ContinuousClock.now
        let options = viewModel.podLogContainerOptions
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(options.count, 200)
        XCTAssertEqual(options.first, "container-0")
        XCTAssertLessThan(seconds(elapsed), 0.02)
    }

    @MainActor
    func testPodBulkSelectionBenchmarkKPI() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "benchmark")
        state.selectedNamespace = "default"
        state.setPods((0..<5_000).map { index in
            PodSummary(
                name: "pod-\(String(format: "%04d", index))",
                namespace: "default",
                status: index.isMultiple(of: 10) ? "Pending" : "Running"
            )
        })

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            viewModel.selectAllVisiblePodsForBulkActions()
            _ = viewModel.selectedPodsForBulkActions
            viewModel.clearPodBulkSelection()
        }

        let started = ContinuousClock.now
        viewModel.selectAllVisiblePodsForBulkActions()
        let selected = viewModel.selectedPodsForBulkActions
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(selected.count, 5_000)
        XCTAssertEqual(selected.first?.name, "pod-0000")
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
                    onSendControlSequence: { _ in },
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
                onSendControlSequence: { _ in },
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

    @MainActor
    func testWriteAuditSearchBenchmarkKPI() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        for index in 0..<200 {
            state.appendWriteAuditEntry(WriteAuditEntry(
                action: index.isMultiple(of: 10) ? "Delete" : "Apply YAML",
                contextName: "benchmark",
                namespace: "default",
                resource: "configmap/settings-\(index)",
                status: index.isMultiple(of: 10) ? "Failed" : "Succeeded",
                message: index.isMultiple(of: 10) ? "forbidden" : "ok"
            ))
        }
        viewModel.writeAuditSearchQuery = "failed forbidden"

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = viewModel.visibleWriteAuditEntries
        }

        let started = ContinuousClock.now
        let visible = viewModel.visibleWriteAuditEntries
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(visible.count, 20)
        XCTAssertLessThan(seconds(elapsed), 0.01)
    }

    @MainActor
    func testUnifiedLogSelectedPodScopeBenchmarkKPI() {
        let podNames = (0..<120).map { "pod-\($0)" }
        let text = (0..<24_000)
            .map { index in
                "[pod-\(index % podNames.count)] line \(index)"
            }
            .joined(separator: "\n")
        let selected = Set((0..<12).map { "pod-\($0)" })

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = RuneAppViewModel.scopedUnifiedLogResult(
                mergedText: text,
                podNames: podNames,
                selectedPodNames: selected
            )
        }

        let started = ContinuousClock.now
        let result = RuneAppViewModel.scopedUnifiedLogResult(
            mergedText: text,
            podNames: podNames,
            selectedPodNames: selected
        )
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(result.podNames.count, 12)
        XCTAssertTrue(result.mergedText.contains("[pod-0]"))
        XCTAssertFalse(result.mergedText.contains("[pod-12]"))
        XCTAssertLessThan(seconds(elapsed), 0.08)
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

private func writeKubeconfig(_ contents: String) throws -> URL {
    let url = FileManager.default.temporaryDirectory
        .appendingPathComponent("rune-ui-performance-kubeconfig-\(UUID().uuidString).yaml")
    try contents.write(to: url, atomically: true, encoding: .utf8)
    return url
}

private func waitUntil(
    timeout: TimeInterval = 2,
    file: StaticString = #filePath,
    line: UInt = #line,
    predicate: @escaping @MainActor () -> Bool
) async throws {
    let deadline = Date().addingTimeInterval(timeout)
    while !(await predicate()) {
        if Date() >= deadline {
            XCTFail("Timed out waiting for condition", file: file, line: line)
            return
        }
        try await Task.sleep(nanoseconds: 20_000_000)
    }
}
