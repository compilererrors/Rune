import AppKit
import Combine
import Foundation
import SwiftUI
import XCTest
import struct RuneSharedCore.RuneLargeTextIndex
@testable import RuneCore
@testable import RuneExport
@testable import RuneFakeK8sSupport
@testable import RuneKube
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

    func testResourceLogRenderPolicyBenchmarkKPI() {
        let manyLineLogs = (0..<60_000)
            .map { index in
                index.isMultiple(of: 25)
                    ? "line=\(index) level=error component=api"
                    : "line=\(index) level=info component=api"
            }
            .joined(separator: "\n")
        let wideFewLinePayload = String(repeating: " payload=synthetic-wide-log-field", count: 140)
        let wideFewLineLogs = (0..<80)
            .map { index in "line=\(String(format: "%06d", index))\(wideFewLinePayload)" }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let manyLineResult = ResourceLogSearchResult.makeForInspector(text: manyLineLogs, query: "level=error")
            let wideResult = ResourceLogSearchResult.makeForInspector(text: wideFewLineLogs, query: "")
            _ = ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: manyLineResult)
            _ = ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: wideResult)
        }

        let started = ContinuousClock.now
        let manyLineResult = ResourceLogSearchResult.makeForInspector(text: manyLineLogs, query: "level=error")
        let wideResult = ResourceLogSearchResult.makeForInspector(text: wideFewLineLogs, query: "")
        let defersManyLines = ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: manyLineResult)
        let defersWideFewLines = ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: wideResult)
        let elapsed = started.duration(to: .now)

        XCTAssertFalse(defersManyLines)
        XCTAssertFalse(defersWideFewLines)
        XCTAssertEqual(manyLineResult.matchingLineCount, 2_400)
        XCTAssertEqual(wideResult.textIndex.lineCount, 80)
        XCTAssertLessThan(seconds(elapsed), 0.45)
    }

    func testKubernetesRequestRetryClassificationBenchmarkKPI() {
        let statuses = [200, 400, 401, 403, 404, 409, 429, 500, 502, 503, 504]
        let errors = [
            URLError(.timedOut),
            URLError(.networkConnectionLost),
            URLError(.cannotConnectToHost),
            URLError(.notConnectedToInternet),
            URLError(.cancelled)
        ]

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            var retryableCount = 0
            for index in 0..<25_000 {
                if KubernetesRequestRetryPolicy
                    .classifyHTTPStatus(statuses[index % statuses.count], retryAfterHeader: index.isMultiple(of: 3) ? "1" : nil)
                    .isRetryable {
                    retryableCount += 1
                }
                if KubernetesRequestRetryPolicy.classifyNetworkError(errors[index % errors.count]).isRetryable {
                    retryableCount += 1
                }
            }
            XCTAssertGreaterThan(retryableCount, 0)
        }

        let started = ContinuousClock.now
        var retryableCount = 0
        for index in 0..<25_000 {
            if KubernetesRequestRetryPolicy
                .classifyHTTPStatus(statuses[index % statuses.count], retryAfterHeader: index.isMultiple(of: 3) ? "1" : nil)
                .isRetryable {
                retryableCount += 1
            }
            if KubernetesRequestRetryPolicy.classifyNetworkError(errors[index % errors.count]).isRetryable {
                retryableCount += 1
            }
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(retryableCount, 31_362)
        XCTAssertLessThan(
            seconds(elapsed),
            0.08,
            "KPI: retry classification must be cheap enough to run on every failed Kubernetes request path."
        )
    }

    func testKubernetesRequestCoalescerBenchmarkKPI() async throws {
        let coalescer = KubernetesRESTRequestCoalescer()
        let counter = PerformanceCoalescerCounter()
        let key = KubernetesRESTRequestCoalescingKey(
            method: "GET",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"]
        )

        let started = ContinuousClock.now
        await withTaskGroup(of: RESTResponse?.self) { group in
            for _ in 0..<200 {
                group.addTask {
                    try? await coalescer.value(for: key) {
                        let count = await counter.increment()
                        try await Task.sleep(nanoseconds: 20_000_000)
                        return RESTResponse(body: "body-\(count)", contentType: "application/json")
                    }
                }
            }
            for await response in group {
                XCTAssertEqual(response?.body, "body-1")
            }
        }
        let elapsed = started.duration(to: .now)
        let operationCount = await counter.currentValue()

        XCTAssertEqual(operationCount, 1)
        XCTAssertLessThan(
            seconds(elapsed),
            0.12,
            "KPI: coalescing 200 identical in-flight reads should share one operation and keep scheduling overhead low."
        )
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

    func testStructuredJSONLAnalysisBenchmarkKPI() {
        let text = (0..<20_000)
            .map { index in
                """
                {"timestamp":"2026-05-06T10:00:\(String(format: "%02d", index % 60))Z","level":"\(index.isMultiple(of: 20) ? "error" : "info")","message":"synthetic message \(index % 500)","pod":"pod-\(index % 12)","container":"app","requestId":"req-\(index)","trace_id":"trace-\(index % 200)","namespace":"default"}
                """
            }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = ResourceStructuredLogAnalyzer.analyze(text: text)
        }

        let started = ContinuousClock.now
        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)
        let elapsed = started.duration(to: .now)

        XCTAssertTrue(summary.isStructured)
        XCTAssertEqual(summary.totalLineCount, 20_000)
        XCTAssertEqual(summary.jsonLineCount, 20_000)
        XCTAssertEqual(summary.field("level")?.nonEmptyCount, 20_000)
        XCTAssertEqual(summary.field("requestID")?.nonEmptyCount, 20_000)
        #if DEBUG
        let maximumAnalysisSeconds = 0.35
        #else
        let maximumAnalysisSeconds = 0.05
        #endif
        XCTAssertLessThan(
            seconds(elapsed),
            maximumAnalysisSeconds,
            "KPI: structured JSONL analysis should stay snappy in debug and under 50ms in release for a 20k-line sample."
        )
    }

    func testUnifiedLogDuplicateDetectionBenchmarkKPI() {
        let text = (0..<24_000)
            .map { index in
                "[pod-\(index % 24)] " + """
                {"timestamp":"2026-05-06T10:00:\(String(format: "%02d", index % 60))Z","level":"warn","message":"retrying upstream request \(index % 300)","pod":"pod-\(index % 24)","container":"app","namespace":"default"}
                """
            }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = ResourceStructuredLogAnalyzer.analyze(text: text)
        }

        let started = ContinuousClock.now
        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)
        let elapsed = started.duration(to: .now)

        XCTAssertTrue(summary.isStructured)
        XCTAssertEqual(summary.totalLineCount, 24_000)
        XCTAssertFalse(summary.duplicateLines.isEmpty)
        XCTAssertEqual(summary.duplicateLines.first?.count, 80)
        #if DEBUG
        let maximumDuplicateDetectionSeconds = 0.35
        #else
        let maximumDuplicateDetectionSeconds = 0.05
        #endif
        XCTAssertLessThan(
            seconds(elapsed),
            maximumDuplicateDetectionSeconds,
            "KPI: unified log duplicate detection should stay snappy in debug and under 50ms in release for a 24k-line sample."
        )
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

    func testLogArchiveMetadataExportBenchmarkKPI() throws {
        let pods = (0..<12).map { "api-\($0)" }
        let text = (0..<24_000)
            .map { index in
                "[api-\(index % pods.count)] 2026-05-07T10:00:\(String(format: "%02d", index % 60))Z message=\(index)"
            }
            .joined(separator: "\n")
        let metadata = LogArchiveMetadata(
            context: "demo",
            namespace: "default",
            workloadKind: "deployment",
            workloadName: "api",
            selectedPods: pods,
            timeWindow: "recentLines",
            previous: false,
            tail: false,
            exportedAt: "20260507T100000Z",
            scope: "full"
        )

        let started = ContinuousClock.now
        let zip = try LogArchiveBuilder.buildZip(
            mergedText: text,
            podNames: pods,
            baseName: "deployment-api-full-logs",
            generatedAt: "20260507T100000Z",
            metadata: metadata
        )
        let elapsed = started.duration(to: .now)

        XCTAssertGreaterThan(zip.count, text.utf8.count / 2)
        XCTAssertLessThan(seconds(elapsed), 0.45, "KPI: metadata should not push a 24k-line, 12-pod log archive above the 450ms export target.")
        XCTAssertTrue(String(decoding: zip, as: UTF8.self).contains("metadata-20260507T100000Z.json"))
    }

    @MainActor
    func testLargeLogInspectorInitialMountBenchmarkKPI() {
        let text = (0..<20_000)
            .map { index in
                "INFO request-id=\(String(format: "%06d", index)) component=worker message=synthetic output"
            }
            .joined(separator: "\n")
        let result = ResourceLogSearchResult.make(text: text, query: "")

        XCTAssertFalse(ResourceLogsDeferredRenderingPolicy.shouldDeferOutputMount(for: result))

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

    @MainActor
    func testStableLogInspectorToolbarLayoutBenchmarkKPI() {
        let widths: [CGFloat] = [420, 520, 720, 960]
        let searchSummary = ResourceLogSearchResult.make(
            text: (0..<300)
                .map { index in
                    index.isMultiple(of: 25)
                        ? "2026-05-07T10:00:00Z level=error component=api message=synthetic failure \(index)"
                        : "2026-05-07T10:00:00Z level=info component=api message=synthetic ok \(index)"
                }
                .joined(separator: "\n"),
            query: "error"
        )

        let measuredController = makeLogToolbarController(
            width: widths[0],
            searchSummary: searchSummary,
            containerOptions: ["app", "sidecar", "metrics"]
        )
        measuredController.view.layoutSubtreeIfNeeded()

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for width in widths {
                measuredController.view.frame = NSRect(x: 0, y: 0, width: width, height: 260)
                measuredController.view.layoutSubtreeIfNeeded()
            }
        }

        let controller = makeLogToolbarController(
            width: widths[0],
            searchSummary: searchSummary,
            containerOptions: ["app", "sidecar", "metrics"]
        )
        controller.view.layoutSubtreeIfNeeded()

        let started = ContinuousClock.now
        let heights = widths.map { width in
            controller.view.frame = NSRect(x: 0, y: 0, width: width, height: 260)
            controller.view.layoutSubtreeIfNeeded()
            return controller.sizeThatFits(in: CGSize(width: width, height: 260)).height
        }
        let elapsed = started.duration(to: .now)

        #if DEBUG
        let maximumLayoutSeconds = 0.08
        #else
        let maximumLayoutSeconds = 0.02
        #endif
        XCTAssertLessThan(
            seconds(elapsed),
            maximumLayoutSeconds,
            "KPI: log inspector controls should stay snappy while resizing the detail pane."
        )

        let minHeight = heights.min() ?? 0
        let maxHeight = heights.max() ?? 0
        XCTAssertLessThanOrEqual(
            maxHeight - minHeight,
            8,
            "KPI: log inspector controls should not jump vertically or wrap into extra rows when the detail pane width changes."
        )
    }

    @MainActor
    func testTerminalPodOnlyLogToolbarLayoutBenchmarkKPI() {
        let widths: [CGFloat] = [420, 520, 720, 960]
        let pods = (0..<12).map { index in
            PodSummary(
                name: "api-\(String(format: "%02d", index))",
                namespace: "default",
                status: index.isMultiple(of: 3) ? "Succeeded" : "Running",
                containerNamesLine: "app,sidecar"
            )
        }
        let searchSummary = ResourceLogSearchResult.make(
            text: (0..<300)
                .map { index in
                    "2026-05-07T10:00:00Z pod=api-\(String(format: "%02d", index % pods.count)) message=synthetic log \(index)"
                }
                .joined(separator: "\n"),
            query: "api"
        )

        let measuredController = makeLogToolbarController(
            width: widths[0],
            searchSummary: searchSummary,
            podOptions: pods,
            selectedPodID: pods[0].id,
            showsContainerPicker: false,
            containerOptions: ["app", "sidecar"]
        )
        measuredController.view.layoutSubtreeIfNeeded()

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for width in widths {
                measuredController.view.frame = NSRect(x: 0, y: 0, width: width, height: 260)
                measuredController.view.layoutSubtreeIfNeeded()
            }
        }

        let controller = makeLogToolbarController(
            width: widths[0],
            searchSummary: searchSummary,
            podOptions: pods,
            selectedPodID: pods[0].id,
            showsContainerPicker: false,
            containerOptions: ["app", "sidecar"]
        )
        controller.view.layoutSubtreeIfNeeded()

        let started = ContinuousClock.now
        let heights = widths.map { width in
            controller.view.frame = NSRect(x: 0, y: 0, width: width, height: 260)
            controller.view.layoutSubtreeIfNeeded()
            return controller.sizeThatFits(in: CGSize(width: width, height: 260)).height
        }
        let elapsed = started.duration(to: .now)

        XCTAssertLessThan(
            seconds(elapsed),
            0.08,
            "KPI: terminal log pod picker layout should stay responsive while resizing."
        )
        XCTAssertLessThanOrEqual(
            (heights.max() ?? 0) - (heights.min() ?? 0),
            8,
            "KPI: terminal log pod picker should not wrap or jump vertically across panel widths."
        )
    }

    @MainActor
    func testManifestInspectorToolbarLayoutBenchmarkKPI() {
        let widths: [CGFloat] = [360, 480, 640, 820]
        let measuredController = makeManifestToolbarController(width: widths[0])
        measuredController.view.layoutSubtreeIfNeeded()

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for width in widths {
                measuredController.view.frame = NSRect(x: 0, y: 0, width: width, height: 160)
                measuredController.view.layoutSubtreeIfNeeded()
            }
        }

        let controller = makeManifestToolbarController(width: widths[0])
        controller.view.layoutSubtreeIfNeeded()

        let started = ContinuousClock.now
        let heights = widths.map { width in
            controller.view.frame = NSRect(x: 0, y: 0, width: width, height: 160)
            controller.view.layoutSubtreeIfNeeded()
            return controller.sizeThatFits(in: CGSize(width: width, height: 160)).height
        }
        let elapsed = started.duration(to: .now)

        #if DEBUG
        let maximumLayoutSeconds = 0.08
        #else
        let maximumLayoutSeconds = 0.02
        #endif
        XCTAssertLessThan(
            seconds(elapsed),
            maximumLayoutSeconds,
            "KPI: manifest inspector controls should stay snappy while resizing the detail pane."
        )

        let minHeight = heights.min() ?? 0
        let maxHeight = heights.max() ?? 0
        XCTAssertLessThanOrEqual(
            maxHeight - minHeight,
            8,
            "KPI: manifest inspector controls should not jump vertically or wrap into extra rows when the detail pane width changes."
        )
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
    func testOperatorResourceBrowserProjectionBenchmarkKPI() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let resources = (0..<500).map { index in
            OperatorResourceSummary(
                family: index.isMultiple(of: 3) ? "Custom Resources" : "Flux",
                kind: index.isMultiple(of: 2) ? "Widgets" : "Kustomizations",
                apiPath: "/apis/example.test/v1/namespaces/default/widgets",
                name: String(format: "synthetic-resource-%03d", index),
                namespace: "default",
                status: index.isMultiple(of: 7) ? "Ready False" : "Ready True",
                message: index.isMultiple(of: 7) ? "Synthetic warning" : "Synthetic ready"
            )
        }
        state.setOperatorResources(resources)

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            viewModel.setResourceSearchQuery("synthetic")
            _ = viewModel.visibleOperatorResources
            _ = viewModel.pagedOperatorResources
            viewModel.setResourceSearchQuery("")
        }

        let started = ContinuousClock.now
        viewModel.setResourceSearchQuery("warning")
        let visible = viewModel.visibleOperatorResources
        let page = viewModel.pagedOperatorResources
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(visible.count, 72)
        XCTAssertEqual(page.count, 40)
        XCTAssertLessThan(seconds(elapsed), 0.08, "KPI: CRD/operator browser filtering and first-page projection should stay below 80ms for 500 resources in debug.")

        viewModel.selectOperatorResource(resources[250])
        XCTAssertEqual(state.selectedOperatorResource?.name, "synthetic-resource-250")
    }

    func testPodNameColumnResizeLayoutBenchmarkKPI() {
        let translations = (-1000...1000).map { CGFloat($0) * 0.75 }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            var checksum: CGFloat = 0
            for _ in 0..<80 {
                for translation in translations {
                    let width = PodTableLayout.resizePreviewWidth(
                        committedWidth: PodTableLayout.nameColumnDefaultWidth,
                        translation: translation
                    )
                    checksum += PodTableLayout.nameColumnFrameWidth(width)
                    checksum += PodTableLayout.minimumScrollableWidth(nameColumnWidth: width)
                }
            }
            XCTAssertGreaterThan(checksum, 0)
        }

        let started = ContinuousClock.now
        var widths: [CGFloat] = []
        widths.reserveCapacity(translations.count)
        for translation in translations {
            widths.append(PodTableLayout.resizePreviewWidth(
                committedWidth: PodTableLayout.nameColumnDefaultWidth,
                translation: translation
            ))
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(widths.first, PodTableLayout.nameColumnMinimumWidth)
        XCTAssertEqual(widths.last, PodTableLayout.nameColumnMaximumWidth)
        XCTAssertTrue(widths.allSatisfy { $0.rounded(.toNearestOrAwayFromZero) == $0 })
        XCTAssertEqual(PodTableLayout.headerHorizontalInset, PodTableLayout.rowHorizontalPadding)
        XCTAssertEqual(
            PodTableLayout.nameColumnFrameWidth(PodTableLayout.nameColumnDefaultWidth),
            PodTableLayout.nameColumnDefaultWidth + PodTableLayout.nameColumnResizeHandleWidth
        )
        XCTAssertLessThan(
            seconds(elapsed),
            0.01,
            "KPI: pod name column drag math must stay below 10ms for 2001 drag samples so resize remains pointer-rate cheap."
        )
    }

    func testResourceListColumnLayoutBenchmarkKPI() {
        let visibleWidths = stride(from: CGFloat(240), through: CGFloat(1800), by: CGFloat(3)).map { $0 }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            var checksum: CGFloat = 0
            for _ in 0..<400 {
                for visibleWidth in visibleWidths {
                    let widths = RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: visibleWidth)
                    checksum += widths.name + widths.replicas + widths.favorite
                    let serviceWidths = RuneAppKitResourceListLayout.serviceColumnWidths(visibleWidth: visibleWidth)
                    checksum += serviceWidths.name + serviceWidths.type + serviceWidths.clusterIP + serviceWidths.favorite
                    let genericWidths = RuneAppKitResourceListLayout.genericColumnWidths(visibleWidth: visibleWidth)
                    checksum += genericWidths.selection + genericWidths.name + genericWidths.primary + genericWidths.secondary + genericWidths.namespace + genericWidths.favorite
                    let helmWidths = RuneAppKitResourceListLayout.helmColumnWidths(visibleWidth: visibleWidth)
                    checksum += helmWidths.name + helmWidths.status + helmWidths.namespace + helmWidths.revision + helmWidths.chart + helmWidths.appVersion
                    let eventWidths = RuneAppKitResourceListLayout.eventColumnWidths(visibleWidth: visibleWidth)
                    checksum += eventWidths.reason + eventWidths.type + eventWidths.object + eventWidths.namespace + eventWidths.lastSeen + eventWidths.message
                    let operatorWidths = RuneAppKitResourceListLayout.operatorColumnWidths(visibleWidth: visibleWidth)
                    checksum += operatorWidths.name + operatorWidths.family + operatorWidths.kind + operatorWidths.namespace + operatorWidths.status + operatorWidths.apiPath + operatorWidths.favorite
                }
            }
            XCTAssertGreaterThan(checksum, 0)
        }

        var previousTotal: CGFloat = 0
        for visibleWidth in visibleWidths {
            let widths = RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: visibleWidth)
            let total = widths.name + widths.replicas + widths.favorite
            let deploymentMinimumTotal = RuneAppKitResourceListLayout.deploymentMinimumNameColumnWidth
                + RuneAppKitResourceListLayout.deploymentReplicaColumnWidth
                + RuneAppKitResourceListLayout.deploymentFavoriteColumnWidth
            let deploymentUsableWidth = min(
                RuneAppKitResourceListLayout.resourceMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.deploymentTrailingBreathingRoom
            )
            XCTAssertEqual(total, max(deploymentUsableWidth, deploymentMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(total, RuneAppKitResourceListLayout.resourceMaximumContentWidth)
            XCTAssertGreaterThanOrEqual(total, previousTotal)
            previousTotal = total

            let serviceWidths = RuneAppKitResourceListLayout.serviceColumnWidths(visibleWidth: visibleWidth)
            let serviceTotal = serviceWidths.name + serviceWidths.type + serviceWidths.clusterIP + serviceWidths.favorite
            let serviceMinimumTotal = RuneAppKitResourceListLayout.serviceMinimumNameColumnWidth
                + RuneAppKitResourceListLayout.serviceTypeColumnWidth
                + RuneAppKitResourceListLayout.serviceClusterIPColumnWidth
                + RuneAppKitResourceListLayout.serviceFavoriteColumnWidth
            let serviceUsableWidth = min(
                RuneAppKitResourceListLayout.resourceMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.serviceTrailingBreathingRoom
            )
            XCTAssertEqual(serviceTotal, max(serviceUsableWidth, serviceMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(serviceTotal, RuneAppKitResourceListLayout.resourceMaximumContentWidth)

            let genericWidths = RuneAppKitResourceListLayout.genericColumnWidths(visibleWidth: visibleWidth)
            let genericLeadingTotal = genericWidths.selection + genericWidths.name + genericWidths.primary
            let genericTrailingTotal = genericWidths.secondary + genericWidths.namespace + genericWidths.favorite
            let genericTotal = genericLeadingTotal + genericTrailingTotal
            let genericMinimumTotal = RuneAppKitResourceListLayout.genericSelectionColumnWidth
                + RuneAppKitResourceListLayout.genericMinimumNameColumnWidth
                + RuneAppKitResourceListLayout.genericPrimaryColumnWidth
                + RuneAppKitResourceListLayout.genericSecondaryColumnWidth
                + RuneAppKitResourceListLayout.genericNamespaceColumnWidth
                + RuneAppKitResourceListLayout.genericFavoriteColumnWidth
            let genericUsableWidth = min(
                RuneAppKitResourceListLayout.resourceMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.genericTrailingBreathingRoom
            )
            XCTAssertEqual(genericTotal, max(genericUsableWidth, genericMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(genericTotal, RuneAppKitResourceListLayout.resourceMaximumContentWidth)

            let helmWidths = RuneAppKitResourceListLayout.helmColumnWidths(visibleWidth: visibleWidth)
            let helmPrimaryTotal = helmWidths.name + helmWidths.status + helmWidths.namespace
            let helmMetadataTotal = helmWidths.revision + helmWidths.chart + helmWidths.appVersion
            let helmTotal = helmPrimaryTotal + helmMetadataTotal
            let helmMinimumTotal = RuneAppKitResourceListLayout.helmMinimumNameColumnWidth
                + RuneAppKitResourceListLayout.helmStatusColumnWidth
                + RuneAppKitResourceListLayout.helmNamespaceColumnWidth
                + RuneAppKitResourceListLayout.helmRevisionColumnWidth
                + RuneAppKitResourceListLayout.helmChartColumnWidth
                + RuneAppKitResourceListLayout.helmAppVersionColumnWidth
            let helmUsableWidth = min(
                RuneAppKitResourceListLayout.helmMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.helmTrailingBreathingRoom
            )
            XCTAssertEqual(helmTotal, max(helmUsableWidth, helmMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(helmTotal, RuneAppKitResourceListLayout.helmMaximumContentWidth)

            let eventWidths = RuneAppKitResourceListLayout.eventColumnWidths(visibleWidth: visibleWidth)
            let eventPrimaryTotal = eventWidths.reason + eventWidths.type + eventWidths.object
            let eventMetadataTotal = eventWidths.namespace + eventWidths.lastSeen + eventWidths.message
            let eventTotal = eventPrimaryTotal + eventMetadataTotal
            let eventMinimumTotal = RuneAppKitResourceListLayout.eventMinimumReasonColumnWidth
                + RuneAppKitResourceListLayout.eventTypeColumnWidth
                + RuneAppKitResourceListLayout.eventObjectColumnWidth
                + RuneAppKitResourceListLayout.eventNamespaceColumnWidth
                + RuneAppKitResourceListLayout.eventLastSeenColumnWidth
                + RuneAppKitResourceListLayout.eventMessageColumnWidth
            let eventUsableWidth = min(
                RuneAppKitResourceListLayout.eventMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.eventTrailingBreathingRoom
            )
            XCTAssertEqual(eventTotal, max(eventUsableWidth, eventMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(eventTotal, RuneAppKitResourceListLayout.eventMaximumContentWidth)

            let operatorWidths = RuneAppKitResourceListLayout.operatorColumnWidths(visibleWidth: visibleWidth)
            let operatorPrimaryTotal = operatorWidths.name + operatorWidths.family + operatorWidths.kind
            let operatorMetadataTotal = operatorWidths.namespace + operatorWidths.status + operatorWidths.apiPath + operatorWidths.favorite
            let operatorTotal = operatorPrimaryTotal + operatorMetadataTotal
            let operatorMinimumTotal = RuneAppKitResourceListLayout.operatorMinimumNameColumnWidth
                + RuneAppKitResourceListLayout.operatorFamilyColumnWidth
                + RuneAppKitResourceListLayout.operatorKindColumnWidth
                + RuneAppKitResourceListLayout.operatorNamespaceColumnWidth
                + RuneAppKitResourceListLayout.operatorStatusColumnWidth
                + RuneAppKitResourceListLayout.operatorAPIPathColumnWidth
                + RuneAppKitResourceListLayout.operatorFavoriteColumnWidth
            let operatorUsableWidth = min(
                RuneAppKitResourceListLayout.operatorMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.operatorTrailingBreathingRoom
            )
            XCTAssertEqual(operatorTotal, max(operatorUsableWidth, operatorMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(operatorTotal, RuneAppKitResourceListLayout.operatorMaximumContentWidth)

            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.deploymentReplicaColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Ready", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.serviceTypeColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Type", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.serviceClusterIPColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Cluster IP", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.genericPrimaryColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Detail", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.genericSecondaryColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Info", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.genericNamespaceColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Namespace", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.helmStatusColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Status", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.helmNamespaceColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Namespace", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.helmRevisionColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Rev", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.helmChartColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Chart", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.helmAppVersionColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "App", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.eventTypeColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Type", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.eventObjectColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Object", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.eventNamespaceColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Namespace", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.eventLastSeenColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Last Seen", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.operatorFamilyColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Family", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.operatorKindColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Kind", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.operatorNamespaceColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Namespace", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.operatorStatusColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Status", reservesSortIndicator: true))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.operatorAPIPathColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "API Path", reservesSortIndicator: true))
        }

        let wideDeployment = RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: 1320)
        XCTAssertGreaterThanOrEqual(wideDeployment.replicas, 88)
        XCTAssertGreaterThanOrEqual(wideDeployment.name, 900)

        let wideGeneric = RuneAppKitResourceListLayout.genericColumnWidths(visibleWidth: 1320)
        XCTAssertGreaterThanOrEqual(wideGeneric.name, 480)
        XCTAssertLessThanOrEqual(wideGeneric.primary, 144)
        XCTAssertLessThanOrEqual(wideGeneric.secondary, 164)
        XCTAssertLessThanOrEqual(wideGeneric.namespace, 132)

        let wideEvents = RuneAppKitResourceListLayout.eventColumnWidths(visibleWidth: 1320)
        XCTAssertGreaterThanOrEqual(wideEvents.reason, 300)
        XCTAssertLessThanOrEqual(wideEvents.type, 120)
        XCTAssertLessThanOrEqual(wideEvents.namespace, 140)

        let wideOperators = RuneAppKitResourceListLayout.operatorColumnWidths(visibleWidth: 1320)
        XCTAssertGreaterThanOrEqual(wideOperators.name, 280)
        XCTAssertLessThanOrEqual(wideOperators.family, 140)
        XCTAssertLessThanOrEqual(wideOperators.status, 130)

        let started = ContinuousClock.now
        var checksum: CGFloat = 0
        for _ in 0..<50 {
            for visibleWidth in visibleWidths {
                let deployment = RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: visibleWidth)
                checksum += deployment.name + deployment.replicas + deployment.favorite
                let service = RuneAppKitResourceListLayout.serviceColumnWidths(visibleWidth: visibleWidth)
                checksum += service.name + service.type + service.clusterIP + service.favorite
                let generic = RuneAppKitResourceListLayout.genericColumnWidths(visibleWidth: visibleWidth)
                checksum += generic.selection + generic.name + generic.primary + generic.secondary + generic.namespace + generic.favorite
                let helm = RuneAppKitResourceListLayout.helmColumnWidths(visibleWidth: visibleWidth)
                checksum += helm.name + helm.status + helm.namespace + helm.revision + helm.chart + helm.appVersion
                let event = RuneAppKitResourceListLayout.eventColumnWidths(visibleWidth: visibleWidth)
                checksum += event.reason + event.type + event.object + event.namespace + event.lastSeen + event.message
                let operatorResource = RuneAppKitResourceListLayout.operatorColumnWidths(visibleWidth: visibleWidth)
                checksum += operatorResource.name + operatorResource.family + operatorResource.kind + operatorResource.namespace + operatorResource.status + operatorResource.apiPath + operatorResource.favorite
            }
        }
        let elapsed = started.duration(to: .now)

        XCTAssertGreaterThan(checksum, 0)
        XCTAssertLessThan(
            seconds(elapsed),
            0.01,
            "KPI: resource column width projection should stay below 10ms for 50 passes across resize samples so the middle panel tracks the side panel."
        )
    }

    @MainActor
    func testGenericResourceQuickCompareBenchmarkKPI() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        state.selectedContext = KubeContext(name: "benchmark")
        state.selectedNamespace = "default"
        state.selectedWorkloadKind = .configMap
        state.setConfigMaps((0..<500).map { index in
            ClusterResourceSummary(
                kind: .configMap,
                name: "config-\(index)",
                namespace: "default",
                primaryText: "\(index % 8 + 1) keys",
                secondaryText: "updated \(index)m"
            )
        })
        viewModel.toggleAllVisibleGenericResourcesForBulkActions()

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = viewModel.selectedGenericResourceComparisonText
        }

        let started = ContinuousClock.now
        let comparison = viewModel.selectedGenericResourceComparisonText
        let elapsed = started.duration(to: .now)

        XCTAssertTrue(comparison.contains("Selected ConfigMaps Compare"))
        XCTAssertTrue(comparison.contains("config-499"))
        XCTAssertLessThan(seconds(elapsed), 0.05, "KPI: quick compare should stay below 50ms for 500 selected resources in debug.")
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
    func testPodMetricSortingBenchmarkKPI() {
        let state = RuneAppState()
        let suiteName = "RunePerformanceBenchmarksTests.podMetricSort.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let viewModel = RuneAppViewModel(
            state: state,
            contextPreferences: UserDefaultsContextPreferencesStore(defaults: defaults)
        )
        state.selectedContext = KubeContext(name: "benchmark")
        state.selectedNamespace = "default"
        state.setPods((0..<5_000).map { index in
            PodSummary(
                name: "pod-\(String(format: "%04d", index))",
                namespace: "default",
                status: index.isMultiple(of: 5) ? "Succeeded" : "Running",
                ageDescription: "\(5_000 - index)m",
                cpuUsage: "\(index % 250)m",
                memoryUsage: "\(128 + (index % 512))Mi"
            )
        })
        for index in stride(from: 0, to: 5_000, by: 333) {
            viewModel.toggleFavoriteResource(
                kind: .pod,
                namespace: "default",
                name: "pod-\(String(format: "%04d", index))"
            )
        }
        viewModel.togglePodSort(.cpu)

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = viewModel.visiblePods
        }

        let started = ContinuousClock.now
        let visible = viewModel.visiblePods
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(visible.count, 5_000)
        XCTAssertEqual(visible.prefix(3).map(\.name), ["pod-0999", "pod-1998", "pod-2997"])
        XCTAssertLessThan(seconds(elapsed), 0.35, "KPI: numeric pod CPU sorting should stay below 350ms for 5,000 rows in debug.")
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

    func testFileBackedContextPreferencesLoadBenchmarkKPI() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RunePerformanceBenchmarksTests.contextPreferences.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("context-preferences.json")
        let store = FileBackedContextPreferencesStore(url: url)
        store.saveFavoriteContextNames((0..<100).map { "context-\(String(format: "%03d", $0))" }.reduce(into: Set<String>()) { $0.insert($1) })
        store.saveFavoriteResourceIDs((0..<5_000).map { "context-\($0 % 10)|deployment|namespace-\($0 % 50)|resource-\(String(format: "%04d", $0))" }.reduce(into: Set<String>()) { $0.insert($1) })
        store.saveFavoriteNamespaceIDs((0..<500).map { "context-\($0 % 10)|namespace|namespace-\(String(format: "%03d", $0))" }.reduce(into: Set<String>()) { $0.insert($1) })
        store.saveManualProductionContextIDs(["context-001", "context-009"])
        for index in 0..<20 {
            store.saveManualNamespaces(["default", "namespace-\(index)"], for: "context-\(String(format: "%03d", index))")
            store.savePreferredNamespace("namespace-\(index)", for: "context-\(String(format: "%03d", index))")
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let reloaded = FileBackedContextPreferencesStore(url: url)
            _ = reloaded.loadFavoriteContextNames()
            _ = reloaded.loadFavoriteResourceIDs()
            _ = reloaded.loadFavoriteNamespaceIDs()
            _ = reloaded.loadManualProductionContextIDs()
            _ = reloaded.loadManualNamespaces(for: "context-005")
            _ = reloaded.loadPreferredNamespace(for: "context-005")
        }

        let started = ContinuousClock.now
        let reloaded = FileBackedContextPreferencesStore(url: url)
        XCTAssertEqual(reloaded.loadFavoriteResourceIDs().count, 5_000)
        XCTAssertEqual(reloaded.loadFavoriteNamespaceIDs().count, 500)
        XCTAssertEqual(reloaded.loadPreferredNamespace(for: "context-005"), "namespace-5")
        let elapsed = started.duration(to: .now)

        XCTAssertLessThan(seconds(elapsed), 0.08, "KPI: preferences load should stay below 80ms for a realistic local app-state file in debug.")
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

    func testTerminalTranscriptSanitizerVTBenchmarkKPI() {
        let chunk = (0..<10_000)
            .map { index in
                "pulling layer \(index)%\rpulling layer \(index + 1)%\u{001B}[0m\u{001B}(B\n"
            }
            .joined()

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = TerminalTranscriptSanitizer.sanitize(chunk)
        }

        let started = ContinuousClock.now
        let sanitized = TerminalTranscriptSanitizer.sanitize(chunk)
        let elapsed = started.duration(to: .now)

        XCTAssertTrue(sanitized.contains("pulling layer 10000%"))
        XCTAssertFalse(sanitized.contains("\u{001B}"))
        XCTAssertLessThan(seconds(elapsed), 0.10)
    }

    func testTerminalScrollbackRetentionBenchmarkKPI() {
        let transcript = (0..<80_000)
            .map { "line \($0) payload=synthetic-terminal-output" }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = TerminalScrollbackRetention.retainingRecentLines(transcript, maxLines: 60_000)
        }

        let started = ContinuousClock.now
        let retained = TerminalScrollbackRetention.retainingRecentLines(transcript, maxLines: 60_000)
        let elapsed = started.duration(to: .now)

        XCTAssertTrue(retained.hasPrefix(TerminalScrollbackRetention.truncationMarker))
        XCTAssertFalse(retained.contains("line 0 payload"))
        XCTAssertTrue(retained.contains("line 79999 payload"))
        XCTAssertLessThan(seconds(elapsed), 0.15)
    }

    func testTerminalTranscriptSearchBenchmarkKPI() {
        let transcript = (0..<60_000)
            .map { index in
                index.isMultiple(of: 5)
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

        XCTAssertEqual(index.ranges.count, 12_000)
        XCTAssertEqual(index.matchLineNumber(selectedIndex: 11_999), 59_996)
        XCTAssertEqual(index.statusText(selectedIndex: 0), "1 of 12000")
        XCTAssertLessThan(seconds(elapsed), 0.45)
    }

    func testTerminalTranscriptAppendWhileSearchOpenBenchmarkKPI() {
        let base = (0..<30_000)
            .map { index in
                index.isMultiple(of: 6)
                    ? "line=\(index) status=error terminal-search-open"
                    : "line=\(index) status=ok terminal-search-open"
            }
            .joined(separator: "\n")
        let appended = (30_000..<31_000)
            .map { index in
                index.isMultiple(of: 6)
                    ? "line=\(index) status=error appended"
                    : "line=\(index) status=ok appended"
            }
            .joined(separator: "\n")
        let transcript = "\(base)\n\(appended)"

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = TerminalTranscriptRenderModel(
                text: transcript,
                query: "status=error",
                matchCase: false,
                usesLargeTextSurface: true
            )
        }

        let started = ContinuousClock.now
        let model = TerminalTranscriptRenderModel(
            text: transcript,
            query: "status=error",
            matchCase: false,
            usesLargeTextSurface: true
        )
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(model.searchIndex.ranges.count, 5_167)
        XCTAssertEqual(model.scrollTargetLine(selectedIndex: 5_166), 30_997)
        XCTAssertLessThan(seconds(elapsed), 0.12)
    }

    func testTerminalPromptPasteAndSendPreparationBenchmarkKPI() {
        let largePaste = (0..<10_000)
            .map { "echo synthetic-\($0)" }
            .joined(separator: "\r\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let paste = TerminalShellPanelView.normalizedTerminalPasteForPrompt(largePaste)
            _ = TerminalShellPanelView.preparedTerminalPromptSend(
                input: paste.text,
                pendingMultilinePasteConfirmation: paste.requiresConfirmation
            )
        }

        let started = ContinuousClock.now
        let paste = TerminalShellPanelView.normalizedTerminalPasteForPrompt(largePaste)
        for _ in 0..<1_000 {
            _ = TerminalShellPanelView.preparedTerminalPromptSend(
                input: "kubectl get pods",
                pendingMultilinePasteConfirmation: false
            )
        }
        let elapsed = started.duration(to: .now)

        XCTAssertTrue(paste.requiresConfirmation)
        XCTAssertTrue(paste.text.contains("echo synthetic-9999"))
        XCTAssertLessThan(seconds(elapsed), 0.05)
    }

    func testTerminalTranscriptScrollNavigationBenchmarkKPI() {
        let transcript = (0..<80_000)
            .map { index in
                index.isMultiple(of: 8)
                    ? "line=\(index) status=error navigation-target"
                    : "line=\(index) status=ok navigation-target"
            }
            .joined(separator: "\n")
        let model = TerminalTranscriptRenderModel(
            text: transcript,
            query: "status=error",
            matchCase: false,
            usesLargeTextSurface: true
        )

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for index in stride(from: 0, to: model.searchIndex.ranges.count, by: 7) {
                _ = model.scrollTargetLine(selectedIndex: index, isPinnedToBottom: false)
            }
        }

        let started = ContinuousClock.now
        for index in 0..<model.searchIndex.ranges.count {
            _ = model.scrollTargetLine(selectedIndex: index, isPinnedToBottom: false)
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(model.searchIndex.ranges.count, 10_000)
        XCTAssertEqual(model.scrollTargetLine(selectedIndex: 9_999, isPinnedToBottom: false), 79_993)
        let noQueryModel = TerminalTranscriptRenderModel(
            text: transcript,
            query: "",
            matchCase: false,
            usesLargeTextSurface: true
        )
        XCTAssertNil(noQueryModel.scrollTargetLine(selectedIndex: 0, isPinnedToBottom: false))
        XCTAssertLessThan(seconds(elapsed), 0.02)
    }

    @MainActor
    func testLargeTerminalTranscriptInitialMountBenchmarkKPI() {
        let transcript = (0..<20_000)
            .map { index in
                "line=\(String(format: "%06d", index)) status=ok path=/synthetic/terminal/output/\(index)"
            }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let controller = NSHostingController(
                rootView: TerminalTranscriptSurface(
                    text: transcript,
                    height: 420,
                    resetID: "benchmark:terminal",
                    fontSize: 12
                )
            )
            controller.view.frame = NSRect(x: 0, y: 0, width: 900, height: 520)
            controller.view.layoutSubtreeIfNeeded()
        }

        let started = ContinuousClock.now
        let controller = NSHostingController(
            rootView: TerminalTranscriptSurface(
                text: transcript,
                height: 420,
                resetID: "benchmark:terminal",
                fontSize: 12
            )
        )
        controller.view.frame = NSRect(x: 0, y: 0, width: 900, height: 520)
        controller.view.layoutSubtreeIfNeeded()
        let elapsed = started.duration(to: .now)

        XCTAssertLessThan(seconds(elapsed), 0.12)
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
                    onStartSession: { _, _ in },
                    onReconnectSession: { _, _, _ in },
                    onSend: {},
                    onSendControlSequence: { _ in },
                    onResizeSession: { _, _, _ in },
                    onDisconnect: {},
                    onSelectSession: { _ in },
                    onCloseSession: { _ in },
                    onComposeNewSession: {},
                    onClearTranscript: {},
                    onSaveActiveTranscript: {},
                    onSaveAllTranscripts: {}
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
                onStartSession: { _, _ in },
                onReconnectSession: { _, _, _ in },
                onSend: {},
                onSendControlSequence: { _ in },
                onResizeSession: { _, _, _ in },
                onDisconnect: {},
                onSelectSession: { _ in },
                onCloseSession: { _ in },
                onComposeNewSession: {},
                onClearTranscript: {},
                onSaveActiveTranscript: {},
                onSaveAllTranscripts: {}
            )
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(sessions.count, 64)
        XCTAssertEqual(pods.count, 500)
        XCTAssertLessThan(seconds(elapsed), 0.10)
    }

    func testTerminalTranscriptExportBenchmarkKPI() throws {
        let sessions = (0..<8).map { sessionIndex in
            PodTerminalSession(
                id: "shell-\(sessionIndex)",
                contextName: "benchmark",
                namespace: "default",
                podName: "pod-\(String(format: "%02d", sessionIndex))",
                shell: "sh",
                transcript: (0..<2_500).map { lineIndex in
                    "session=\(sessionIndex) line=\(lineIndex) status=ok"
                }.joined(separator: "\n"),
                status: sessionIndex.isMultiple(of: 2) ? .connected : .disconnected,
                lastExitCode: sessionIndex.isMultiple(of: 2) ? nil : 0
            )
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = try? RuneAppViewModel.terminalTranscriptArchiveData(
                sessions: sessions,
                generatedAt: "20260508T100000Z"
            )
        }

        let started = ContinuousClock.now
        let data = try RuneAppViewModel.terminalTranscriptArchiveData(
            sessions: sessions,
            generatedAt: "20260508T100000Z"
        )
        let elapsed = started.duration(to: .now)
        let archiveBytes = String(decoding: data, as: UTF8.self)

        XCTAssertTrue(archiveBytes.contains("terminal-transcripts/session-1-default-pod-00-20260508T100000Z.log"))
        XCTAssertTrue(archiveBytes.contains("session=7 line=2499 status=ok"))
        XCTAssertLessThan(seconds(elapsed), 0.25, "KPI: terminal transcript ZIP export should stay below 250ms for 20k synthetic lines.")
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

    @MainActor
    private func makeLogToolbarController(
        width: CGFloat,
        searchSummary: ResourceLogSearchResult,
        podOptions: [PodSummary] = [],
        selectedPodID: String = "",
        showsContainerPicker: Bool = true,
        containerOptions: [String]
    ) -> NSHostingController<ResourceLogsToolbar> {
        let controller = NSHostingController(
            rootView: ResourceLogsToolbar(
                selectedLogPreset: .constant(.recentLines),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                searchQuery: .constant("error"),
                selectedSearchMatchIndex: .constant(2),
                searchPulseID: 0,
                searchSummary: searchSummary,
                statusText: "Last updated 12:00:00",
                podOptions: podOptions,
                selectedPodID: podOptions.isEmpty ? nil : .constant(selectedPodID),
                showsContainerPicker: showsContainerPicker,
                containerOptions: containerOptions,
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
        controller.view.frame = NSRect(x: 0, y: 0, width: width, height: 260)
        return controller
    }

    @MainActor
    private func makeManifestToolbarController(width: CGFloat) -> NSHostingController<some View> {
        let controller = NSHostingController(
            rootView: VStack(alignment: .leading, spacing: 9) {
                ManifestInlineNote("YAML edits stay local until Apply YAML.") {
                    ManifestUnsavedEditsSlot(isVisible: true)
                }

                ManifestToolbarScrollRow {
                    ManifestToolbarGroup {
                        Button("Apply YAML") {}
                            .buttonStyle(.borderedProminent)
                        Button("Quick Edit") {}
                            .buttonStyle(.bordered)
                        Button("Edit…") {}
                            .buttonStyle(.bordered)
                        Button("Undo") {}
                            .buttonStyle(.bordered)
                        Button("Revert") {}
                            .buttonStyle(.bordered)
                    }

                    ManifestToolbarGroup {
                        Button("Import…") {}
                            .buttonStyle(.bordered)
                        Button("Export…") {}
                            .buttonStyle(.bordered)
                    }
                }

                ManifestStatusChip(text: "Last updated 12:00:00", systemImage: "clock")
            }
            .frame(width: width, alignment: .leading)
        )
        controller.view.frame = NSRect(x: 0, y: 0, width: width, height: 160)
        return controller
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

private actor PerformanceCoalescerCounter {
    private var count = 0

    func increment() -> Int {
        count += 1
        return count
    }

    func currentValue() -> Int {
        count
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
