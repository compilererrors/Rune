import AppKit
import Combine
import Foundation
import SwiftUI
import XCTest
import struct RuneSharedCore.RuneLargeTextIndex
@testable import RuneCore
@testable import RuneDiagnostics
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

    private func minimumElapsedSeconds(repetitions: Int = 3, _ operation: () -> Void) -> Double {
        var best = Double.infinity
        for _ in 0..<max(1, repetitions) {
            let started = ContinuousClock.now
            autoreleasepool {
                operation()
            }
            best = min(best, seconds(started.duration(to: .now)))
        }
        return best
    }

    private func minimumThrowingElapsedSeconds(repetitions: Int = 3, _ operation: () throws -> Void) throws -> Double {
        var best = Double.infinity
        for _ in 0..<max(1, repetitions) {
            let started = ContinuousClock.now
            try autoreleasepool {
                try operation()
            }
            best = min(best, seconds(started.duration(to: .now)))
        }
        return best
    }

    @MainActor
    private func minimumAsyncElapsedSeconds(repetitions: Int = 3, _ operation: () async throws -> Void) async throws -> Double {
        var best = Double.infinity
        for _ in 0..<max(1, repetitions) {
            let started = ContinuousClock.now
            try await operation()
            best = min(best, seconds(started.duration(to: .now)))
        }
        return best
    }

    @MainActor
    private func benchmarkTable(columnIDs: [String]) -> NSTableView {
        let tableView = NSTableView(frame: NSRect(x: 0, y: 0, width: 1_120, height: 720))
        for columnID in columnIDs {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(columnID))
            column.width = columnID == "name" || columnID == "reason" ? 260 : 120
            tableView.addTableColumn(column)
        }
        return tableView
    }

    private func resizeNotification(for tableColumn: NSTableColumn) -> Notification {
        Notification(
            name: NSTableView.columnDidResizeNotification,
            object: nil,
            userInfo: ["NSTableColumn": tableColumn]
        )
    }

    private func benchmarkPods(count: Int) -> [PodSummary] {
        (0..<count).map { index in
            PodSummary(
                name: "pod-\(String(format: "%04d", index))",
                namespace: "default",
                status: index.isMultiple(of: 11) ? "Pending" : "Running",
                ageDescription: "\(index % 240)m",
                cpuUsage: "\(index % 500)m",
                memoryUsage: "\(128 + index % 768)Mi"
            )
        }
    }

    private func benchmarkDeployments(count: Int) -> [DeploymentSummary] {
        (0..<count).map { index in
            DeploymentSummary(
                name: "deploy-\(String(format: "%04d", index))",
                namespace: "default",
                readyReplicas: index % 4,
                desiredReplicas: 4
            )
        }
    }

    private func benchmarkServices(count: Int) -> [ServiceSummary] {
        (0..<count).map { index in
            ServiceSummary(
                name: "service-\(String(format: "%04d", index))",
                namespace: "default",
                type: index.isMultiple(of: 3) ? "ClusterIP" : "NodePort",
                clusterIP: "synthetic-ip-\(index)"
            )
        }
    }

    private func benchmarkConfigMaps(count: Int) -> [ClusterResourceSummary] {
        (0..<count).map { index in
            ClusterResourceSummary(
                kind: .configMap,
                name: "config-\(String(format: "%04d", index))",
                namespace: index.isMultiple(of: 9) ? nil : "default",
                primaryText: "\(index % 18 + 1) keys",
                secondaryText: "settings"
            )
        }
    }

    private func benchmarkClusterResources(
        kind: KubeResourceKind,
        prefix: String,
        count: Int,
        namespace: String? = "default"
    ) -> [ClusterResourceSummary] {
        (0..<count).map { index in
            ClusterResourceSummary(
                kind: kind,
                name: "\(prefix)-\(String(format: "%04d", index))",
                namespace: namespace,
                primaryText: "\(index % 18 + 1) keys",
                secondaryText: "settings"
            )
        }
    }

    private func benchmarkHelmReleases(count: Int) -> [HelmReleaseSummary] {
        (0..<count).map { index in
            HelmReleaseSummary(
                name: "release-\(String(format: "%04d", index))",
                namespace: "default",
                revision: index % 12 + 1,
                updated: "2026-05-01T00:00:00Z",
                status: index.isMultiple(of: 13) ? "failed" : "deployed",
                chart: "chart-\(index % 18)",
                appVersion: "1.\(index % 10).0"
            )
        }
    }

    private func benchmarkEvents(count: Int) -> [EventSummary] {
        (0..<count).map { index in
            EventSummary(
                type: index.isMultiple(of: 5) ? "Warning" : "Normal",
                reason: index.isMultiple(of: 5) ? "BackOff" : "Scheduled",
                objectName: "object-\(String(format: "%04d", index))",
                message: "synthetic event message \(index)",
                lastTimestamp: "2026-05-01T00:\(String(format: "%02d", index % 60)):00Z",
                involvedKind: "Pod",
                involvedNamespace: index.isMultiple(of: 11) ? nil : "default"
            )
        }
    }

    @MainActor
    private func benchmarkOverviewViewModel() -> RuneAppViewModel {
        let state = RuneAppState()
        state.setContexts([KubeContext(name: "benchmark")])
        state.selectedSection = .overview
        state.selectedNamespace = "default"

        let pods = (0..<800).map { index in
            PodSummary(
                name: "pod-\(String(format: "%04d", index))",
                namespace: "default",
                status: index.isMultiple(of: 29) ? "CrashLoopBackOff" : "Running",
                totalRestarts: index.isMultiple(of: 41) ? 4 : 0,
                containersReady: index.isMultiple(of: 53) ? "0/1" : "1/1"
            )
        }
        let deployments = benchmarkDeployments(count: 160)
        let services = benchmarkServices(count: 160)
        state.setDeployments(deployments)
        state.setServices(services)
        state.setOverviewSnapshot(
            pods: pods,
            deploymentsCount: deployments.count,
            servicesCount: services.count,
            ingressesCount: 20,
            configMapsCount: 80,
            cronJobsCount: 12,
            nodesCount: 24,
            clusterCPUPercent: 47,
            clusterMemoryPercent: 58,
            events: benchmarkEvents(count: 600)
        )

        return RuneAppViewModel(
            state: state,
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
    }

    private func benchmarkOperatorResources(count: Int) -> [OperatorResourceSummary] {
        (0..<count).map { index in
            OperatorResourceSummary(
                family: "family-\(index % 8)",
                kind: "Kind\(index % 12)",
                apiPath: "/apis/example.test/v1/resources/\(index)",
                name: "resource-\(String(format: "%04d", index))",
                namespace: index.isMultiple(of: 9) ? nil : "default",
                status: index.isMultiple(of: 10) ? "Progressing" : "Ready",
                message: "synthetic status \(index)"
            )
        }
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

        XCTAssertTrue(defersManyLines)
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

    func testRESTRequestMetricsRecordingBenchmarkKPI() async {
        let recorder = KubernetesRESTRequestMetricsRecorder(maxRetainedMetrics: 2_000)
        let started = ContinuousClock.now

        for index in 0..<2_000 {
            await recorder.record(KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/apis/apps/v1/namespaces/synthetic/deployments/deploy-\(index)/status?limit=200&continue=token-\(index)",
                statusCode: 200,
                responseBytes: 512,
                durationSeconds: 0.001,
                attempt: 1,
                outcome: .success
            ))
        }
        let summary = await recorder.summary()
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(summary.requestCount, 2_000)
        XCTAssertEqual(summary.successCount, 2_000)
        XCTAssertEqual(summary.responseBytes, 1_024_000)
        XCTAssertEqual(summary.retainedMetricCount, 2_000)
        XCTAssertLessThan(
            seconds(elapsed),
            0.25,
            "KPI: recording 2k privacy-safe REST request metrics should stay below 250ms in debug."
        )
    }

    func testRESTRequestMetricsRetentionChurnBenchmarkKPI() async {
        let recorder = KubernetesRESTRequestMetricsRecorder(maxRetainedMetrics: 512)
        let started = ContinuousClock.now

        for index in 0..<10_000 {
            await recorder.record(KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/pods/pod-\(index)?continue=token-\(index)",
                statusCode: index.isMultiple(of: 17) ? 503 : 200,
                responseBytes: index,
                durationSeconds: 0.001,
                attempt: index % 3 + 1,
                outcome: index.isMultiple(of: 17) ? .httpError : .success
            ))
        }
        let snapshot = await recorder.snapshot()
        let summary = await recorder.summary()
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(summary.requestCount, 10_000)
        XCTAssertEqual(summary.retainedMetricCount, 512)
        XCTAssertEqual(summary.responseBytes, 49_995_000)
        XCTAssertEqual(snapshot.count, 512)
        XCTAssertEqual(snapshot.first?.responseBytes, 9_488)
        XCTAssertEqual(snapshot.last?.responseBytes, 9_999)
        XCTAssertLessThan(
            seconds(elapsed),
            0.75,
            "KPI: sustained REST metrics churn should retain the latest window without O(n) eviction cost."
        )
    }

    func testRESTRequestCancelledMetricsRecordingBenchmarkKPI() async {
        let recorder = KubernetesRESTRequestMetricsRecorder(maxRetainedMetrics: 1_000)
        let started = ContinuousClock.now

        for index in 0..<5_000 {
            await recorder.record(KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/pods/pod-\(index)?continue=token-\(index)",
                statusCode: nil,
                responseBytes: 0,
                durationSeconds: 0.001,
                attempt: 1,
                outcome: .cancelled,
                cancellationReason: index.isMultiple(of: 2) ? "task-cancelled" : "urlsession-cancelled"
            ))
        }
        let summary = await recorder.summary()
        let snapshot = await recorder.snapshot()
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(summary.requestCount, 5_000)
        XCTAssertEqual(summary.cancelledCount, 5_000)
        XCTAssertEqual(summary.failureCount, 0)
        XCTAssertEqual(summary.retainedMetricCount, 1_000)
        XCTAssertTrue(snapshot.allSatisfy { !$0.apiPath.contains("synthetic") && !$0.apiPath.contains("token-") })
        XCTAssertLessThan(
            seconds(elapsed),
            0.35,
            "KPI: recording 5k cancelled REST request metrics should stay below 350ms in debug."
        )
    }

    func testRESTRequestMetricsGroupingBenchmarkKPI() {
        let metrics = (0..<4_000).map { index in
            KubernetesRESTRequestMetric(
                sourcePath: "swift-rest",
                method: "GET",
                apiPath: "/apis/apps/v1/namespaces/synthetic-\(index % 12)/deployments/deploy-\(index % 80)/status?limit=200&continue=token-\(index)",
                statusCode: index.isMultiple(of: 17) ? 503 : 200,
                responseBytes: 256 + index % 512,
                durationSeconds: Double(index % 50) / 1_000,
                attempt: index.isMultiple(of: 17) ? 2 : 1,
                outcome: index.isMultiple(of: 17) ? .httpError : .success
            )
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = KubernetesRequestMetricsSupportBundleProjector.groups(from: metrics)
        }

        let started = ContinuousClock.now
        let groups = KubernetesRequestMetricsSupportBundleProjector.groups(from: metrics)
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(groups.count, 1)
        XCTAssertEqual(groups.first?.apiPath, "/apis/apps/v1/namespaces/<namespace>/deployments/<name>/status?limit=<redacted>&continue=<redacted>")
        XCTAssertEqual(groups.first?.requestCount, 4_000)
        XCTAssertGreaterThan(groups.first?.failureCount ?? 0, 0)
        XCTAssertLessThan(
            seconds(elapsed),
            0.12,
            "KPI: grouping 4k privacy-safe REST request metrics for support bundles should stay below 120ms in debug."
        )
    }

    func testAddClusterProviderActionLayoutBenchmarkKPI() {
        let widths = stride(from: CGFloat(360), through: CGFloat(760), by: CGFloat(1)).map { $0 }
        let elapsed = minimumElapsedSeconds {
            for _ in 0..<2_000 {
                for width in widths {
                    _ = RuneAddClusterProviderActionLayout.columnCount(for: 5, dialogWidth: width)
                    _ = RuneAddClusterProviderActionLayout.rowCount(for: 5, dialogWidth: width)
                    _ = RuneAddClusterProviderActionLayout.rowCount(for: 4, dialogWidth: width)
                }
            }
        }

        XCTAssertEqual(RuneAddClusterProviderActionLayout.rowCount(for: 5), 2)
        #if DEBUG
        let maximumLayoutProjectionSeconds = 0.25
        #else
        let maximumLayoutProjectionSeconds = 0.10
        #endif
        XCTAssertLessThan(
            elapsed,
            maximumLayoutProjectionSeconds,
            "KPI: Add Cluster provider action layout projection should stay below 250ms in debug and 100ms in release for 2k width sweeps."
        )
    }

    func testCloudCredentialRequiredFieldProjectionBenchmarkKPI() {
        let drafts = (0..<10_000).map { index in
            CloudCredentialDraft(
                clusterName: index.isMultiple(of: 5) ? "" : "synthetic-\(index)",
                regionOrLocation: index.isMultiple(of: 7) ? "" : "eu-north-1",
                resourceGroup: index.isMultiple(of: 3) ? "" : "synthetic-group",
                projectID: index.isMultiple(of: 11) ? "" : "synthetic-project"
            )
        }
        let providers: [CloudKubeConfigProvider] = [.aks, .eks, .gke]
        let elapsed = minimumElapsedSeconds {
            var readyCount = 0
            for draft in drafts {
                for provider in providers where draft.hasRequiredFields(for: provider) {
                    readyCount += 1
                }
            }
            XCTAssertGreaterThan(readyCount, 0)
        }

        XCTAssertLessThan(
            elapsed,
            0.03,
            "KPI: Add Cluster required-field projection for 10k drafts should stay below 30ms in debug."
        )
    }

    func testAddClusterCloudImportWorkflowProjectionBenchmarkKPI() {
        let reviews = (0..<20_000).map { index in
            KubeConfigImportReview(
                contexts: [],
                issues: [
                    KubeConfigImportIssue(
                        id: "synthetic-warning-\(index)",
                        severity: .warning,
                        message: "Synthetic warning \(index)"
                    ),
                    KubeConfigImportIssue(
                        id: "synthetic-error-\(index)",
                        severity: index.isMultiple(of: 4) ? .error : .warning,
                        message: "Synthetic error \(index)"
                    )
                ],
                redactedPreview: ""
            )
        }

        let elapsed = minimumElapsedSeconds {
            let blocking = AddClusterCloudImportWorkflow.blockingIssues(in: reviews)
            let checks = AddClusterCloudImportWorkflow.importReviewFailureChecks(for: blocking)
            XCTAssertEqual(blocking.count, 5_000)
            XCTAssertEqual(checks.count, 6)
            XCTAssertFalse(AddClusterCloudImportWorkflow.blockingImportErrorMessage(for: blocking).contains("Synthetic error"))
        }

        XCTAssertLessThan(
            elapsed,
            0.05,
            "KPI: Add Cluster cloud-import review projection for 20k reviews should stay below 50ms in debug."
        )
    }

    func testAddClusterBoundedBlockingFailureProjectionBenchmarkKPI() {
        let reviews = (0..<20_000).map { index in
            KubeConfigImportReview(
                contexts: [],
                issues: [
                    KubeConfigImportIssue(
                        id: "synthetic-warning-\(index)",
                        severity: .warning,
                        message: "Synthetic warning \(index)"
                    ),
                    KubeConfigImportIssue(
                        id: "synthetic-error-\(index)",
                        severity: index.isMultiple(of: 4) ? .error : .warning,
                        message: "Synthetic error \(index)"
                    )
                ],
                redactedPreview: ""
            )
        }

        let elapsed = minimumElapsedSeconds {
            let failure = AddClusterCloudImportWorkflow.blockingFailure(in: reviews)
            XCTAssertEqual(failure?.checks.count, 6)
            XCTAssertFalse(failure?.message.contains("Synthetic error") == true)
            XCTAssertFalse(failure?.message.contains("Synthetic error 12") == true)
        }

        XCTAssertLessThan(
            elapsed,
            0.02,
            "KPI: bounded Add Cluster blocking-review failure projection should stay below 20ms for 20k reviews."
        )
    }

    func testAddClusterCloudImportStatusProjectionBenchmarkKPI() {
        let providers: [CloudKubeConfigProvider] = [.aks, .eks, .gke]
        let elapsed = minimumElapsedSeconds {
            var statusCharacterCount = 0
            for index in 0..<50_000 {
                let provider = providers[index % providers.count]
                statusCharacterCount += AddClusterCloudImportWorkflow.runningStatus(for: provider).count
                statusCharacterCount += AddClusterCloudImportWorkflow.importedStatus(for: provider).count
                statusCharacterCount += AddClusterCloudImportWorkflow.failedStatus().count
            }
            XCTAssertGreaterThan(statusCharacterCount, 0)
        }

        XCTAssertLessThan(
            elapsed,
            0.02,
            "KPI: Add Cluster status projection should stay below 20ms for 50k provider state refreshes."
        )
    }

    func testAddClusterCloudLoginFailureCheckProjectionBenchmarkKPI() {
        let providers: [CloudKubeConfigProvider] = [.aks, .eks, .gke]
        let elapsed = minimumElapsedSeconds {
            var idCharacterCount = 0
            for index in 0..<50_000 {
                let provider = providers[index % providers.count]
                let checks = AddClusterCloudImportWorkflow.cloudLoginFailureChecks(for: provider)
                idCharacterCount += checks.reduce(0) { $0 + $1.id.count + $1.title.count }
            }
            XCTAssertGreaterThan(idCharacterCount, 0)
        }

        XCTAssertLessThan(
            elapsed,
            0.035,
            "KPI: Add Cluster cloud-login failure check projection should stay below 35ms for 50k provider failures."
        )
    }

    func testAddClusterImportReviewFailureCheckIDSanitizationBenchmarkKPI() {
        let issues = (0..<12).map { index in
            KubeConfigImportIssue(
                id: index.isMultiple(of: 2)
                    ? "/private/tmp/token-\(index)/synthetic-context"
                    : String(repeating: "synthetic-long-id-\(index)-", count: 6),
                severity: .error,
                message: "Synthetic import issue \(index)"
            )
        }

        let elapsed = minimumElapsedSeconds {
            var idCharacterCount = 0
            for _ in 0..<5_000 {
                let checks = AddClusterCloudImportWorkflow.importReviewFailureChecks(for: issues)
                idCharacterCount += checks.reduce(0) { $0 + $1.id.count }
            }
            XCTAssertGreaterThan(idCharacterCount, 0)
        }

        XCTAssertLessThan(
            elapsed,
            0.08,
            "KPI: sanitized Add Cluster import-review health-check IDs should stay below 80ms for 5k projections."
        )
    }

    func testAddClusterCloudFailureSanitizationBenchmarkKPI() {
        let failures = (0..<5_000).map { index in
            CloudKubeConfigImportError.commandFailed(
                command: "provider get-credentials synthetic-private-cluster-\(index)",
                exitCode: 42,
                message: "synthetic-token-\(index) provider stderr"
            )
        }
        let issues = (0..<5).map { index in
            KubeConfigImportIssue(
                id: "missing-server-sensitive-\(index)",
                severity: .error,
                message: "Context synthetic-private-context-\(index) is missing server token synthetic-token-\(index)."
            )
        }

        let elapsed = minimumElapsedSeconds {
            var characterCount = 0
            for failure in failures {
                characterCount += failure.localizedDescription.count
            }
            for index in 0..<5_000 {
                characterCount += AddClusterCloudImportWorkflow.safeImportReviewIssueMessage(
                    for: issues[index % issues.count]
                ).count
            }
            XCTAssertGreaterThan(characterCount, 0)
        }

        let sample = failures[0].localizedDescription
            + AddClusterCloudImportWorkflow.blockingImportErrorMessage(for: issues)
            + AddClusterCloudImportWorkflow.importReviewFailureChecks(for: issues).map(\.message).joined(separator: " ")
        XCTAssertFalse(sample.contains("synthetic-token"))
        XCTAssertFalse(sample.contains("synthetic-private-cluster"))
        XCTAssertFalse(sample.contains("synthetic-private-context"))
        XCTAssertLessThan(elapsed, 0.02, "KPI: cloud import failure projection and sanitization should stay below 20ms for 5k synthetic failures.")
    }

    @MainActor
    func testProductionConfirmationStateTransitionBenchmarkKPI() {
        let previousProduction = UserDefaults.standard.object(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
        UserDefaults.standard.runeWriteSafetyRequireProductionSecondConfirmation = true
        defer {
            if let previousProduction {
                UserDefaults.standard.set(previousProduction, forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation)
            }
        }

        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "prod")
        state.selectedNamespace = "default"
        state.selectedSection = .workloads
        state.selectedWorkloadKind = .pod
        state.setSelectedPod(PodSummary(name: "api-0", namespace: "default", status: "Running"))
        let viewModel = RuneAppViewModel(state: state)

        let elapsed = minimumElapsedSeconds {
            for _ in 0..<1_000 {
                viewModel.requestDeleteSelectedResource()
                viewModel.confirmPendingWriteAction()
                XCTAssertEqual(viewModel.pendingProductionDestructiveConfirmation, .delete(kind: .pod, name: "api-0"))
                viewModel.cancelPendingWriteAction()
            }
        }

        #if DEBUG
        let maximumTransitionSeconds = 0.02
        #else
        let maximumTransitionSeconds = 0.01
        #endif
        XCTAssertLessThan(
            elapsed,
            maximumTransitionSeconds,
            "KPI: production confirmation state transition should stay below 20ms in debug and 10ms in release for 1k synthetic actions."
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

        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)
        let elapsedSeconds = minimumElapsedSeconds {
            _ = ResourceStructuredLogAnalyzer.analyze(text: text)
        }

        XCTAssertTrue(summary.isStructured)
        XCTAssertEqual(summary.totalLineCount, 20_000)
        XCTAssertEqual(summary.jsonLineCount, 20_000)
        XCTAssertEqual(summary.field("level")?.nonEmptyCount, 20_000)
        XCTAssertEqual(summary.field("requestID")?.nonEmptyCount, 20_000)
        #if DEBUG
        let maximumAnalysisSeconds = 0.60
        #else
        let maximumAnalysisSeconds = 0.05
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumAnalysisSeconds,
            "KPI: structured JSONL analysis should stay under 600ms in debug and 50ms in release for a 20k-line sample."
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

        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)
        let elapsedSeconds = minimumElapsedSeconds {
            _ = ResourceStructuredLogAnalyzer.analyze(text: text)
        }

        XCTAssertTrue(summary.isStructured)
        XCTAssertEqual(summary.totalLineCount, 24_000)
        XCTAssertFalse(summary.duplicateLines.isEmpty)
        XCTAssertEqual(summary.duplicateLines.first?.count, 80)
        #if DEBUG
        let maximumDuplicateDetectionSeconds = 0.60
        #else
        let maximumDuplicateDetectionSeconds = 0.05
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
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

    func testLogArchiveHighCardinalityPodSplitBenchmarkKPI() {
        let pods = (0..<240).map { "pod-\(String(format: "%03d", $0))" }
        let text = (0..<48_000)
            .map { index in
                "[\(pods[index % pods.count])] 2026-05-09T10:00:\(String(format: "%02d", index % 60))Z message=\(index)"
            }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = LogArchiveBuilder.splitMergedLogsByPod(mergedText: text, podNames: pods)
        }

        var split: [String: String] = [:]
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            split = LogArchiveBuilder.splitMergedLogsByPod(mergedText: text, podNames: pods)
        }

        XCTAssertEqual(split.count, 240)
        XCTAssertTrue(split["pod-000"]?.contains("message=0") == true)
        XCTAssertTrue(split["pod-239"]?.contains("message=239") == true)
        XCTAssertLessThan(
            elapsedSeconds,
            0.20,
            "KPI: splitting a 48k-line unified log across 240 pods should stay below 200ms in debug."
        )
    }

    func testLogZipExportBenchmarkKPI() throws {
        let pods = (0..<12).map { "api-\($0)" }
        let text = (0..<24_000)
            .map { index in
                "[\(pods[index % pods.count])] 2026-05-05T10:00:\(String(format: "%02d", index % 60))Z message=\(index)"
            }
            .joined(separator: "\n")

        let zip = try LogArchiveBuilder.buildZip(
            mergedText: text,
            podNames: pods,
            baseName: "benchmark-logs",
            generatedAt: "20260505T100000Z"
        )
        let elapsedSeconds = try minimumThrowingElapsedSeconds {
            _ = try LogArchiveBuilder.buildZip(
                mergedText: text,
                podNames: pods,
                baseName: "benchmark-logs",
                generatedAt: "20260505T100000Z"
            )
        }

        XCTAssertGreaterThan(zip.count, text.utf8.count / 2)
        XCTAssertLessThan(elapsedSeconds, 0.45, "KPI: exporting a 24k-line, 12-pod log archive should stay below 450ms on local benchmark runs.")

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

        let zip = try LogArchiveBuilder.buildPodContainerZip(
            records: records,
            baseName: "deployment-api-pod-logs",
            generatedAt: "20260506T100000Z"
        )
        let elapsedSeconds = try minimumThrowingElapsedSeconds {
            _ = try LogArchiveBuilder.buildPodContainerZip(
                records: records,
                baseName: "deployment-api-pod-logs",
                generatedAt: "20260506T100000Z"
            )
        }

        XCTAssertGreaterThan(zip.count, 0)
        XCTAssertLessThan(elapsedSeconds, 0.45, "KPI: exporting a 24k-line, 12-pod, 24-container deployment log archive should stay below 450ms on local benchmark runs.")

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

        let zip = try LogArchiveBuilder.buildZip(
            mergedText: text,
            podNames: pods,
            baseName: "deployment-api-full-logs",
            generatedAt: "20260507T100000Z",
            metadata: metadata
        )
        let elapsedSeconds = try minimumThrowingElapsedSeconds {
            _ = try LogArchiveBuilder.buildZip(
                mergedText: text,
                podNames: pods,
                baseName: "deployment-api-full-logs",
                generatedAt: "20260507T100000Z",
                metadata: metadata
            )
        }

        XCTAssertGreaterThan(zip.count, text.utf8.count / 2)
        XCTAssertLessThan(elapsedSeconds, 0.45, "KPI: metadata should not push a 24k-line, 12-pod log archive above the 450ms export target.")
        XCTAssertTrue(String(decoding: zip, as: UTF8.self).contains("metadata-20260507T100000Z.json"))
    }

    @MainActor
    func testConfiguredFolderExportCollisionBenchmarkKPI() throws {
        let payload = Data("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n".utf8)

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            guard let folderURL = try? makeBenchmarkTemporaryDirectory(prefix: "rune-configured-export-collision-measure") else {
                XCTFail("Failed to create benchmark export folder")
                return
            }
            defer { try? FileManager.default.removeItem(at: folderURL) }
            try? prefillBenchmarkExportCollisions(in: folderURL, baseName: "logs", ext: "yaml", count: 250)
            let exporter = ConfiguredFolderExporter(
                resolver: BenchmarkConfiguredExportDestinationResolver(folderURL: folderURL),
                opener: BenchmarkConfiguredExportFileOpener(),
                securityScopedAccess: BenchmarkSecurityScopedResourceAccess(startsAccessing: false)
            )
            _ = try? exporter.save(
                data: payload,
                suggestedName: "logs.yaml",
                allowedFileTypes: ["yaml"],
                kind: .plainText,
                openAfterSave: false
            )
        }

        let folderURL = try makeBenchmarkTemporaryDirectory(prefix: "rune-configured-export-collision-kpi")
        defer { try? FileManager.default.removeItem(at: folderURL) }
        try prefillBenchmarkExportCollisions(in: folderURL, baseName: "logs", ext: "yaml", count: 250)
        let exporter = ConfiguredFolderExporter(
            resolver: BenchmarkConfiguredExportDestinationResolver(folderURL: folderURL),
            opener: BenchmarkConfiguredExportFileOpener(),
            securityScopedAccess: BenchmarkSecurityScopedResourceAccess(startsAccessing: false)
        )
        var savedURL: URL?
        let elapsedSeconds = try minimumThrowingElapsedSeconds(repetitions: 5) {
            let url = try exporter.save(
                data: payload,
                suggestedName: "logs.yaml",
                allowedFileTypes: ["yaml"],
                kind: .plainText,
                openAfterSave: false
            )
            savedURL = url
            try FileManager.default.removeItem(at: url)
        }

        XCTAssertEqual(savedURL?.lastPathComponent, "logs-251.yaml")
        XCTAssertLessThan(
            elapsedSeconds,
            0.08,
            "KPI: resolving a configured export name after 250 existing collisions should stay below 80ms in debug."
        )
    }

    @MainActor
    func testConfiguredFolderSaveAndOpenHandoffBenchmarkKPI() throws {
        let payload = Data("apiVersion: v1\nkind: Pod\nmetadata:\n  name: benchmark\n".utf8)
        let batchSize = 150

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = try? runConfiguredFolderSaveAndOpenBatch(payload: payload, count: batchSize)
        }

        var result: (savedCount: Int, openedCount: Int, deferredScopeStopCount: Int)?
        let elapsedSeconds = try minimumThrowingElapsedSeconds(repetitions: 3) {
            result = try runConfiguredFolderSaveAndOpenBatch(payload: payload, count: batchSize)
        }

        XCTAssertEqual(result?.savedCount, batchSize)
        XCTAssertEqual(result?.openedCount, batchSize)
        XCTAssertEqual(result?.deferredScopeStopCount, batchSize)
        XCTAssertLessThan(
            elapsedSeconds,
            0.30,
            "KPI: configured Save and Open should write and hand off 150 small manifests below 300ms in debug."
        )
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

        let elapsed = minimumElapsedSeconds {
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

        #if DEBUG
        let maximumInitialMountSeconds = 0.55
        #else
        let maximumInitialMountSeconds = 0.12
        #endif
        XCTAssertLessThan(
            elapsed,
            maximumInitialMountSeconds,
            "KPI: a 20k-line log inspector should mount with virtualized output under 550ms in debug and 120ms in release."
        )
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
            presentationStyle: .terminalCompact,
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
            presentationStyle: .terminalCompact,
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
    func testTerminalLogTabsWorkflowBenchmarkKPI() {
        let pods = (0..<80).map { index in
            PodSummary(
                name: "pod-\(String(format: "%04d", index))",
                namespace: "default",
                status: index.isMultiple(of: 9) ? "Succeeded" : "Running",
                containerNamesLine: "app,sidecar"
            )
        }
        let favoriteNames = Set(["pod-0003", "pod-0013", "pod-0021", "pod-0034"])

        var measuredState = TerminalPodLogTabState()
        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            measuredState = TerminalPodLogTabState()
            for pod in pods.prefix(24) {
                measuredState.add(preferredPod: pod)
            }
            _ = measuredState.presentations(pods: pods) { favoriteNames.contains($0.name) }
            measuredState.reconcile(availablePods: Array(pods.dropFirst(8)), fallbackPod: pods[8])
            _ = measuredState.preferredPodForNewTab(
                pods: pods,
                fallbackPod: pods[10],
                isFavorite: { favoriteNames.contains($0.name) }
            )
        }

        let started = ContinuousClock.now
        var state = TerminalPodLogTabState()
        for _ in 0..<1_000 {
            state = TerminalPodLogTabState()
            for pod in pods.prefix(24) {
                state.add(preferredPod: pod)
            }
            _ = state.presentations(pods: pods) { favoriteNames.contains($0.name) }
            state.reconcile(availablePods: Array(pods.dropFirst(8)), fallbackPod: pods[8])
            _ = state.preferredPodForNewTab(
                pods: pods,
                fallbackPod: pods[10],
                isFavorite: { favoriteNames.contains($0.name) }
            )
        }
        let elapsed = started.duration(to: .now)

        #if DEBUG
        let maximumWorkflowSeconds = 0.45
        #else
        let maximumWorkflowSeconds = 0.16
        #endif
        XCTAssertLessThan(
            seconds(elapsed),
            maximumWorkflowSeconds,
            "KPI: log tab workflow state should stay below \(maximumWorkflowSeconds)s for 1k add/select/reconcile projection cycles."
        )

        let presentations = state.presentations(pods: pods) { favoriteNames.contains($0.name) }
        let controller = NSHostingController(
            rootView: TerminalLogTabBar(
                tabs: presentations,
                activeTabID: state.activeTabID,
                canAddTab: true,
                onSelectTab: { _ in },
                onCloseTab: { _ in },
                onToggleFavoriteTab: { _ in },
                onAddTab: {}
            )
        )
        controller.view.frame = NSRect(x: 0, y: 0, width: 900, height: 44)
        controller.view.layoutSubtreeIfNeeded()

        let layoutStarted = ContinuousClock.now
        for width in stride(from: CGFloat(480), through: CGFloat(1_080), by: CGFloat(120)) {
            controller.view.frame = NSRect(x: 0, y: 0, width: width, height: 44)
            controller.view.layoutSubtreeIfNeeded()
        }
        let layoutElapsed = layoutStarted.duration(to: .now)

        XCTAssertLessThan(
            seconds(layoutElapsed),
            0.05,
            "KPI: terminal log tab bar layout should stay below 50ms across resize samples."
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

    func testYAMLPlainKubernetesValueHighlightingBenchmarkKPI() {
        let container = """
          - name: app
            image: registry.example.invalid/app:latest
            imagePullPolicy: Always
            env:
              - name: JVM_OPTS
                value: -javaagent:agent.jar -XX:+UseContainerSupport -Xmx300M
                  -Xms120M
            volumeMounts:
              - mountPath: /mnt/secrets-store
        """
        let manifest = (0..<700)
            .map { index in
                """
                apiVersion: v1
                kind: Pod
                metadata:
                  name: synthetic-pod-\(index)
                  generateName: synthetic-job-\(index)-
                spec:
                  containers:
                \(container)
                  restartPolicy: Never
                """
            }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = YAMLLanguageService.analyze(manifest)
        }

        let started = ContinuousClock.now
        let analysis = YAMLLanguageService.analyze(manifest)
        let elapsed = started.duration(to: .now)
        let highlightedText = analysis.highlights
            .filter { $0.kind == .string }
            .map { (manifest as NSString).substring(with: $0.range) }

        XCTAssertTrue(highlightedText.contains("registry.example.invalid/app:latest"))
        XCTAssertTrue(highlightedText.contains("-javaagent:agent.jar -XX:+UseContainerSupport -Xmx300M"))
        XCTAssertTrue(highlightedText.contains("-Xms120M"))
        XCTAssertTrue(highlightedText.contains("/mnt/secrets-store"))
        XCTAssertFalse(highlightedText.contains("-"))
        XCTAssertTrue(analysis.validationIssues.isEmpty)
        XCTAssertLessThan(
            seconds(elapsed),
            0.85,
            "KPI: YAML plain Kubernetes value highlighting should stay below 850ms for large manifests in debug."
        )
    }

    @MainActor
    func testYAMLLineNumberGutterVisibleLabelsBenchmarkKPI() throws {
        let manifest = (0..<25_000)
            .map { index in "key\(String(format: "%05d", index)): value-\(index)" }
            .joined(separator: "\n")
        let host = NSHostingController(
            rootView: AppKitManifestTextView(
                text: .constant(manifest),
                isEditable: true,
                contentStyle: .yaml,
                showsLineNumbers: true
            )
            .frame(width: 640, height: 420)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 640, height: 420),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.contentView?.layoutSubtreeIfNeeded()
        defer { window.orderOut(nil) }

        let scrollView = try XCTUnwrap(findManifestTextScrollView(in: host.view))
        let textView = try XCTUnwrap(scrollView.documentView as? NSTextView)
        textView.layoutManager?.ensureLayout(for: try XCTUnwrap(textView.textContainer))

        let scrollOffsets = stride(from: CGFloat(0), through: CGFloat(20_000), by: CGFloat(160)).map { $0 }
        for offset in scrollOffsets {
            scrollView.contentView.bounds.origin.y = offset
            scrollView.refreshLineNumberGutter()
            _ = scrollView.lineNumberGutterView.visibleLineNumberLabels()
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            var checksum = 0
            for offset in scrollOffsets {
                scrollView.contentView.bounds.origin.y = offset
                scrollView.refreshLineNumberGutter()
                checksum += scrollView.lineNumberGutterView.visibleLineNumberLabels().count
            }
            XCTAssertGreaterThan(checksum, 0)
        }

        func refreshGutterAcrossViewportSamples() -> Int {
            var checksum = 0
            for offset in scrollOffsets {
                scrollView.contentView.bounds.origin.y = offset
                scrollView.refreshLineNumberGutter()
                checksum += scrollView.lineNumberGutterView.visibleLineNumberLabels().count
            }
            return checksum
        }

        let checksum = refreshGutterAcrossViewportSamples()
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 7) {
            _ = refreshGutterAcrossViewportSamples()
        }

        XCTAssertGreaterThan(checksum, 0)
        XCTAssertGreaterThanOrEqual(textView.textContainerInset.width, scrollView.lineNumberGutterView.frame.width)
        #if DEBUG
        let maximumGutterSeconds = 0.25
        #else
        let maximumGutterSeconds = 0.05
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumGutterSeconds,
            "KPI: YAML line-number gutter labels must stay below 250ms in debug and 50ms in release across 126 viewport scroll samples for a 25k-line manifest."
        )
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

        #if DEBUG
        let maximumEmptyBootstrapSeconds = 0.10
        #else
        let maximumEmptyBootstrapSeconds = 0.02
        #endif
        XCTAssertLessThan(seconds(elapsed), maximumEmptyBootstrapSeconds)
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
        #if DEBUG
        let maximumEmptyBootstrapSeconds = 0.10
        #else
        let maximumEmptyBootstrapSeconds = 0.02
        #endif
        XCTAssertLessThan(seconds(elapsed), maximumEmptyBootstrapSeconds)
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

        XCTAssertGreaterThanOrEqual(seconds(elapsed), 0.20)
        XCTAssertLessThan(seconds(elapsed), 0.38)
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
        #if DEBUG
        let maximumDiscoveryDeferralSeconds = 0.04
        #else
        let maximumDiscoveryDeferralSeconds = 0.02
        #endif
        XCTAssertLessThan(seconds(elapsed), maximumDiscoveryDeferralSeconds)
    }

    @MainActor
    func testColdStartViewModelInitializationKPI() {
        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for _ in 0..<200 {
                _ = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
            }
        }

        let elapsedSeconds = minimumElapsedSeconds {
            for _ in 0..<200 {
                _ = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
            }
        }

        #if DEBUG
        let maximumViewModelInitializationSeconds = 0.20
        #else
        let maximumViewModelInitializationSeconds = 0.10
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumViewModelInitializationSeconds,
            "KPI: cold ViewModel init should keep 200 inits below 200ms in debug."
        )
    }

    @MainActor
    func testColdStartAddClusterWorkflowComparisonKPI() {
        let baselineSeconds = minimumElapsedSeconds {
            for _ in 0..<200 {
                _ = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
            }
        }

        let request = CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1"
        )
        let review = KubeConfigImportReview(
            contexts: [
                KubeConfigImportContextPreview(
                    name: "synthetic-context",
                    clusterName: "synthetic-cluster",
                    userName: "synthetic-user",
                    namespace: "default",
                    serverHost: "example.invalid",
                    authType: "Exec",
                    providerHint: "EKS"
                )
            ],
            issues: [
                KubeConfigImportIssue(
                    id: "synthetic-warning",
                    severity: .warning,
                    message: "Synthetic warning"
                )
            ],
            redactedPreview: "apiVersion: v1\ncontexts: []\n"
        )
        let importer = BenchmarkCloudKubeConfigImporter(result: CloudKubeConfigImportResult(
            command: CloudKubeConfigCommandPreview(
                executable: "aws",
                arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
                displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
            ),
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "", stderr: ""),
            discoveredURLs: [],
            reviews: [review]
        ))

        let addClusterColdStartSeconds = minimumElapsedSeconds {
            for _ in 0..<200 {
                let viewModel = RuneAppViewModel(
                    kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
                    cloudKubeConfigImporter: importer
                )
                _ = viewModel.cloudKubeConfigCommandPreview(for: request)
                _ = AddClusterCloudImportWorkflow.runningStatus(for: request.provider)
                _ = AddClusterCloudImportWorkflow.importedStatus(for: request.provider)
                _ = AddClusterCloudImportWorkflow.blockingIssues(in: [review])
                XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
                XCTAssertNil(viewModel.cloudKubeConfigImportStatus)
            }
        }

        #if DEBUG
        let maximumAddClusterColdStartSeconds = 0.26
        let maximumAddClusterOverheadSeconds = 0.08
        #else
        let maximumAddClusterColdStartSeconds = 0.13
        let maximumAddClusterOverheadSeconds = 0.04
        #endif
        XCTAssertLessThan(
            addClusterColdStartSeconds,
            maximumAddClusterColdStartSeconds,
            "KPI: cold ViewModel init plus Add Cluster preview/workflow projection should stay below the app-start budget."
        )
        XCTAssertLessThanOrEqual(
            addClusterColdStartSeconds,
            baselineSeconds + maximumAddClusterOverheadSeconds,
            "KPI: Add Cluster projection must not materially regress cold-start initialization versus baseline."
        )
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

        #if DEBUG
        let maximumRootShellConstructionSeconds = 0.30
        #else
        let maximumRootShellConstructionSeconds = 0.18
        #endif
        XCTAssertLessThan(
            seconds(elapsed),
            maximumRootShellConstructionSeconds,
            "KPI: root shell construction should keep 200 shell constructions below 300ms in debug."
        )
    }

    @MainActor
    func testColdStartRootShellAddClusterComparisonKPI() {
        let baselineSeconds = minimumElapsedSeconds {
            for _ in 0..<200 {
                let viewModel = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
                _ = RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: nil,
                    debugDisableBootstrap: true
                )
            }
        }

        let readyDraft = CloudCredentialDraft(
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1",
            resourceGroup: "synthetic-group",
            projectID: "synthetic-project"
        )
        let providers: [CloudKubeConfigProvider] = [.aks, .eks, .gke]
        let addClusterRootShellSeconds = minimumElapsedSeconds {
            var readyProjectionCount = 0
            for _ in 0..<200 {
                let viewModel = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
                _ = RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: nil,
                    debugDisableBootstrap: true
                )
                _ = RuneAddClusterProviderActionLayout.columnCount(for: 5)
                _ = RuneAddClusterProviderActionLayout.rowCount(for: 5)
                for provider in providers where readyDraft.hasRequiredFields(for: provider) {
                    readyProjectionCount += 1
                }
                XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
            }
            XCTAssertEqual(readyProjectionCount, 600)
        }

        #if DEBUG
        let maximumRootShellWithAddClusterSeconds = 0.36
        let maximumAddClusterShellOverheadSeconds = 0.08
        #else
        let maximumRootShellWithAddClusterSeconds = 0.20
        let maximumAddClusterShellOverheadSeconds = 0.04
        #endif
        XCTAssertLessThan(
            addClusterRootShellSeconds,
            maximumRootShellWithAddClusterSeconds,
            "KPI: root shell construction plus Add Cluster layout projection should stay inside the cold-start shell budget."
        )
        XCTAssertLessThanOrEqual(
            addClusterRootShellSeconds,
            baselineSeconds + maximumAddClusterShellOverheadSeconds,
            "KPI: Add Cluster shell projection must not materially regress root shell cold-start construction."
        )
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

        let elapsedSeconds = minimumElapsedSeconds {
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

        XCTAssertLessThan(elapsedSeconds, 0.12)
    }

    @MainActor
    func testColdStartLaunchShellAddClusterComparisonKPI() {
        func mountLaunchShell(viewModel: RuneAppViewModel) {
            let controller = NSHostingController(
                rootView: RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: nil
                )
            )
            controller.view.frame = NSRect(x: 0, y: 0, width: 1440, height: 900)
            controller.view.layoutSubtreeIfNeeded()
        }

        let baselineSeconds = minimumElapsedSeconds(repetitions: 5) {
            mountLaunchShell(viewModel: RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer()))
        }

        let importer = BenchmarkCloudKubeConfigImporter(result: CloudKubeConfigImportResult(
            command: CloudKubeConfigCommandPreview(
                executable: "aws",
                arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
                displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
            ),
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "", stderr: ""),
            discoveredURLs: [],
            reviews: []
        ))
        let addClusterMountSeconds = minimumElapsedSeconds(repetitions: 5) {
            let viewModel = RuneAppViewModel(
                kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
                cloudKubeConfigImporter: importer
            )
            mountLaunchShell(viewModel: viewModel)
            XCTAssertFalse(viewModel.isRunningCloudKubeConfigImport)
            XCTAssertNil(viewModel.cloudKubeConfigImportStatus)
        }

        #if DEBUG
        let maximumLaunchMountWithAddClusterSeconds = 0.12
        let maximumAddClusterMountOverheadSeconds = 0.04
        #else
        let maximumLaunchMountWithAddClusterSeconds = 0.06
        let maximumAddClusterMountOverheadSeconds = 0.02
        #endif
        XCTAssertLessThan(
            addClusterMountSeconds,
            maximumLaunchMountWithAddClusterSeconds,
            "KPI: first launch shell mount with Add Cluster dependencies should stay inside the cold-start first-paint budget."
        )
        XCTAssertLessThanOrEqual(
            addClusterMountSeconds,
            baselineSeconds + maximumAddClusterMountOverheadSeconds,
            "KPI: Add Cluster dependencies must not materially regress launch shell initial mount versus baseline."
        )
    }

    @MainActor
    func testSimpleModeOverviewInitialMountBenchmarkKPI() {
        let defaults = UserDefaults.standard
        let oldValue = defaults.object(forKey: RuneSettingsKeys.simpleMode)
        defer {
            if let oldValue {
                defaults.set(oldValue, forKey: RuneSettingsKeys.simpleMode)
            } else {
                defaults.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        func mountOverview(simpleMode: Bool) -> Double {
            defaults.set(simpleMode, forKey: RuneSettingsKeys.simpleMode)
            return minimumElapsedSeconds(repetitions: 5) {
                let controller = NSHostingController(
                    rootView: RuneRootView(
                        viewModel: benchmarkOverviewViewModel(),
                        onLayoutSnapshotChange: nil,
                        debugDisableBootstrap: true
                    )
                )
                controller.view.frame = NSRect(x: 0, y: 0, width: 1440, height: 900)
                controller.view.layoutSubtreeIfNeeded()
            }
        }

        let fullModeSeconds = mountOverview(simpleMode: false)
        let simpleModeSeconds = mountOverview(simpleMode: true)

        XCTAssertLessThan(
            simpleModeSeconds,
            0.16,
            "KPI: simple-mode overview initial mount should stay below 160ms in debug with synthetic overview data."
        )
        XCTAssertLessThanOrEqual(
            simpleModeSeconds,
            fullModeSeconds + 0.01,
            "KPI: simple mode must not regress overview initial mount time versus full mode."
        )
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
    func testCommandPaletteGlobalSearchAndAliasBenchmarkKPI() {
        let state = RuneAppState()
        state.setContexts((0..<500).map { index in
            KubeContext(name: "synthetic-context-\(String(format: "%04d", index))")
        })
        state.setNamespaces((0..<250).map { "synthetic-namespace-\(String(format: "%04d", $0))" })
        state.setPods(benchmarkPods(count: 500))
        state.setDeployments(benchmarkDeployments(count: 500))
        state.setServices(benchmarkServices(count: 500))
        state.setStatefulSets(benchmarkClusterResources(kind: .statefulSet, prefix: "statefulset", count: 500))
        state.setEndpoints(benchmarkClusterResources(kind: .endpoint, prefix: "endpoint", count: 500))
        state.setIngresses(benchmarkClusterResources(kind: .ingress, prefix: "ingress", count: 500))
        state.setConfigMaps(benchmarkClusterResources(kind: .configMap, prefix: "configmap", count: 500))
        state.setSecrets(benchmarkClusterResources(kind: .secret, prefix: "secret", count: 500))
        state.setJobs(benchmarkClusterResources(kind: .job, prefix: "job", count: 500))
        state.setRBACData(
            roles: [],
            serviceAccounts: benchmarkClusterResources(kind: .serviceAccount, prefix: "serviceaccount", count: 500),
            roleBindings: [],
            clusterRoles: [],
            clusterRoleBindings: []
        )
        let viewModel = RuneAppViewModel(
            state: state,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )

        let globalItems = viewModel.commandPaletteItems(query: "synthetic-context")
        XCTAssertEqual(globalItems.count, 160)
        XCTAssertTrue(globalItems.first?.id.hasPrefix("context:") == true)
        let podItems = viewModel.commandPaletteItems(query: ":po pod-0499")
        XCTAssertEqual(podItems.first?.title, "pod-0499")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":cm configmap-0499").first?.title, "configmap-0499")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":ep endpoint-0499").first?.title, "endpoint-0499")
        XCTAssertEqual(viewModel.commandPaletteItems(query: ":sa serviceaccount-0499").first?.title, "serviceaccount-0499")

        let elapsedSeconds = minimumElapsedSeconds {
            for _ in 0..<200 {
                _ = viewModel.commandPaletteItems(query: "synthetic-context")
                _ = viewModel.commandPaletteItems(query: ":po pod-0499")
                _ = viewModel.commandPaletteItems(query: ":deploy deploy-0499")
                _ = viewModel.commandPaletteItems(query: ":svc service-0499")
                _ = viewModel.commandPaletteItems(query: ":sts statefulset-0499")
                _ = viewModel.commandPaletteItems(query: ":ep endpoint-0499")
                _ = viewModel.commandPaletteItems(query: ":ing ingress-0499")
                _ = viewModel.commandPaletteItems(query: ":cm configmap-0499")
                _ = viewModel.commandPaletteItems(query: ":sec secret-0499")
                _ = viewModel.commandPaletteItems(query: ":job job-0499")
                _ = viewModel.commandPaletteItems(query: ":sa serviceaccount-0499")
            }
        }

        #if DEBUG
        let maximumSeconds = 2.0
        #else
        let maximumSeconds = 0.45
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumSeconds,
            "KPI: command palette global search plus direct resource aliases should stay below \(maximumSeconds)s for 200 large-list lookups."
        )
    }

    @MainActor
    func testFakeRESTPodAndLogLoadBenchmarkKPI() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let elapsedSeconds = try await minimumAsyncElapsedSeconds {
            let state = RuneAppState()
            state.setSources([KubeConfigSource(url: kubeconfig)])
            let viewModel = RuneAppViewModel(
                state: state,
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )

            try await viewModel.reloadContexts()
            viewModel.setSection(.workloads)

            try await waitUntil {
                state.selectedSection == .workloads
                    && state.selectedWorkloadKind == .pod
                    && state.selectedPod != nil
                    && !state.isLoading
            }

            viewModel.reloadLogsForSelection()

            try await waitUntil {
                !state.isLoadingLogs
                    && state.podLogs.contains("synthetic REST fake log")
                    && state.lastLogFetchError == nil
            }
        }

        #if DEBUG
        let maximumPodAndLogLoadSeconds = 0.6
        #else
        let maximumPodAndLogLoadSeconds = 0.3
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumPodAndLogLoadSeconds,
            "KPI: fake REST workload pod load plus selected pod logs should stay below 600ms in debug and 300ms in release."
        )
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

        viewModel.setResourceSearchQuery("warning")
        let visible = viewModel.visibleOperatorResources
        let page = viewModel.pagedOperatorResources
        let elapsedSeconds = minimumElapsedSeconds {
            viewModel.setResourceSearchQuery("warning")
            _ = viewModel.visibleOperatorResources
            _ = viewModel.pagedOperatorResources
        }

        XCTAssertEqual(visible.count, 72)
        XCTAssertEqual(page.count, 40)
        XCTAssertLessThan(elapsedSeconds, 0.08, "KPI: CRD/operator browser filtering and first-page projection should stay below 80ms for 500 resources in debug.")

        viewModel.selectOperatorResource(resources[250])
        XCTAssertEqual(state.selectedOperatorResource?.name, "synthetic-resource-250")
    }

    @MainActor
    func testResourceContextMenuSelectionHighlightBenchmarkKPI() {
        let tableView = NSTableView(frame: NSRect(x: 0, y: 0, width: 520, height: 500))
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name"))
        column.width = 500
        tableView.addTableColumn(column)
        tableView.rowHeight = 28
        tableView.allowsMultipleSelection = false

        let dataSource = BenchmarkTableDataSource(rowCount: 1_000)
        tableView.dataSource = dataSource
        tableView.delegate = dataSource
        tableView.noteNumberOfRowsChanged()

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for row in 0..<100 {
                applyImmediateResourceContextMenuSelection(
                    row: row,
                    in: tableView
                )
            }
        }

        let elapsedSeconds = minimumElapsedSeconds {
            for row in 0..<200 {
                applyImmediateResourceContextMenuSelection(
                    row: row,
                    in: tableView
                )
            }
        }

        XCTAssertEqual(tableView.selectedRow, 199)
        XCTAssertLessThan(
            elapsedSeconds,
            0.080,
            "KPI: context-menu row highlight must be visual-first and stay below 80ms for 200 row selections in debug."
        )
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

        var firstWidth: CGFloat = 0
        var lastWidth: CGFloat = 0
        var roundedWidths = true
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            for (index, translation) in translations.enumerated() {
                let width = PodTableLayout.resizePreviewWidth(
                    committedWidth: PodTableLayout.nameColumnDefaultWidth,
                    translation: translation
                )
                if index == 0 {
                    firstWidth = width
                }
                if index == translations.count - 1 {
                    lastWidth = width
                }
                roundedWidths = roundedWidths && width.rounded(.toNearestOrAwayFromZero) == width
            }
        }

        XCTAssertEqual(firstWidth, PodTableLayout.nameColumnMinimumWidth)
        XCTAssertEqual(lastWidth, PodTableLayout.nameColumnMaximumWidth)
        XCTAssertTrue(roundedWidths)
        XCTAssertEqual(PodTableLayout.headerHorizontalInset, PodTableLayout.rowHorizontalPadding)
        XCTAssertEqual(
            PodTableLayout.nameColumnFrameWidth(PodTableLayout.nameColumnDefaultWidth),
            PodTableLayout.nameColumnDefaultWidth + PodTableLayout.nameColumnResizeHandleWidth
        )
        XCTAssertLessThan(
            elapsedSeconds,
            0.01,
            "KPI: pod name column drag math must stay below 10ms for 2001 drag samples so resize remains pointer-rate cheap."
        )
    }

    @MainActor
    func testAppKitPodTableVisibleCellProjectionBenchmarkKPI() {
        let pods = benchmarkPods(count: 1_000)
        let selectedPodIDs = Set(stride(from: 0, to: pods.count, by: 10).map { pods[$0].id })
        let tableView = NSTableView(frame: NSRect(x: 0, y: 0, width: 980, height: 720))
        for identifier in ["selection", "name", "cpu", "memory", "restarts", "age", "status", "favorite"] {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(identifier))
            column.width = identifier == "name" ? PodTableLayout.nameColumnDefaultWidth : 90
            tableView.addTableColumn(column)
        }
        let view = AppKitPodTableView(
            pods: pods,
            selectedPodID: pods[500].id,
            selectedPodIDs: selectedPodIDs,
            sortColumn: .name,
            sortAscending: true,
            nameColumnWidth: PodTableLayout.nameColumnDefaultWidth,
            canApplyClusterMutations: true,
            isFavorite: { pod in pod.name.hasSuffix("0") },
            onSelectPod: { _ in },
            onToggleBulkSelection: { _ in },
            onToggleSort: { _ in },
            onNameColumnWidthChanged: { _ in },
            onToggleFavorite: { _ in },
            onOpenLogs: { _ in },
            onOpenExec: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let coordinator = view.makeCoordinator()
        tableView.dataSource = coordinator
        tableView.delegate = coordinator

        let visibleRows = 32

        func projectVisibleCells() -> Int {
            var projected = 0
            for row in 0..<visibleRows {
                for column in tableView.tableColumns {
                    if coordinator.tableView(tableView, viewFor: column, row: row) != nil {
                        projected += 1
                    }
                }
            }
            return projected
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            XCTAssertEqual(projectVisibleCells(), 256)
        }

        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            _ = projectVisibleCells()
        }

        XCTAssertEqual(coordinator.numberOfRows(in: tableView), 1_000)
        #if DEBUG
        let maximumVisibleCellProjectionSeconds = 0.45
        #else
        let maximumVisibleCellProjectionSeconds = 0.25
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumVisibleCellProjectionSeconds,
            "KPI: AppKit pod table visible-cell projection should stay below 450ms in debug and 250ms in release for 32 visible rows across all columns."
        )
    }

    @MainActor
    func testAppKitGenericResourceTableVisibleCellProjectionBenchmarkKPI() {
        let resources = benchmarkConfigMaps(count: 1_000)
        let selectedResourceIDs = Set(stride(from: 0, to: resources.count, by: 12).map { resources[$0].id })
        let tableView = NSTableView(frame: NSRect(x: 0, y: 0, width: 1_080, height: 720))
        for identifier in ["selection", "name", "primary", "secondary", "namespace", "favorite"] {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(identifier))
            column.width = identifier == "name" ? 360 : 120
            tableView.addTableColumn(column)
        }
        let view = AppKitGenericResourceListView(
            resources: resources,
            selectedResourceID: resources[250].id,
            selectedResourceIDs: selectedResourceIDs,
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: true,
            isFavorite: { resource in resource.name.hasSuffix("0") },
            onSelectResource: { _ in },
            onToggleBulkSelection: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let coordinator = view.makeCoordinator()
        tableView.dataSource = coordinator
        tableView.delegate = coordinator

        let visibleRows = 32

        func projectVisibleCells() -> Int {
            var projected = 0
            for row in 0..<visibleRows {
                for column in tableView.tableColumns {
                    if coordinator.tableView(tableView, viewFor: column, row: row) != nil {
                        projected += 1
                    }
                }
            }
            return projected
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            XCTAssertEqual(projectVisibleCells(), 192)
        }

        let elapsedSeconds = minimumElapsedSeconds {
            _ = projectVisibleCells()
        }

        XCTAssertEqual(coordinator.numberOfRows(in: tableView), 1_000)
        XCTAssertLessThan(
            elapsedSeconds,
            0.50,
            "KPI: AppKit generic resource table visible-cell projection should stay below 500ms for 32 visible rows across all columns in debug."
        )
    }

    @MainActor
    func testAppKitResourceContextMenuConstructionBenchmarkKPI() {
        let pods = benchmarkPods(count: 500)
        let podView = AppKitPodTableView(
            pods: pods,
            selectedPodID: pods[0].id,
            selectedPodIDs: [],
            sortColumn: .name,
            sortAscending: true,
            nameColumnWidth: PodTableLayout.nameColumnDefaultWidth,
            canApplyClusterMutations: true,
            isFavorite: { pod in pod.name.hasSuffix("0") },
            onSelectPod: { _ in },
            onToggleBulkSelection: { _ in },
            onToggleSort: { _ in },
            onNameColumnWidthChanged: { _ in },
            onToggleFavorite: { _ in },
            onOpenLogs: { _ in },
            onOpenExec: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let podCoordinator = podView.makeCoordinator()

        let resources = benchmarkConfigMaps(count: 500)
        let resourceView = AppKitGenericResourceListView(
            resources: resources,
            selectedResourceID: resources[0].id,
            selectedResourceIDs: [],
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: true,
            isFavorite: { resource in resource.name.hasSuffix("0") },
            onSelectResource: { _ in },
            onToggleBulkSelection: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let resourceCoordinator = resourceView.makeCoordinator()

        func buildMenus() -> Int {
            var itemCount = 0
            for row in 0..<300 {
                itemCount += podCoordinator.makeMenu(forRow: row)?.items.count ?? 0
                itemCount += resourceCoordinator.makeMenu(forRow: row)?.items.count ?? 0
            }
            return itemCount
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            XCTAssertGreaterThan(buildMenus(), 0)
        }

        let elapsedSeconds = minimumElapsedSeconds {
            _ = buildMenus()
        }

        XCTAssertGreaterThan(buildMenus(), 5_000)
        XCTAssertLessThan(
            elapsedSeconds,
            0.25,
            "KPI: AppKit resource context menus should build under 250ms for 600 synthetic row menus in debug."
        )
    }

    @MainActor
    func testAppKitSecondaryResourceTablesVisibleCellProjectionBenchmarkKPI() {
        let deployments = benchmarkDeployments(count: 500)
        let deploymentTable = benchmarkTable(columnIDs: ["name", "replicas", "favorite"])
        let deploymentView = AppKitDeploymentListView(
            deployments: deployments,
            selectedDeploymentID: deployments[50].id,
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: true,
            isFavorite: { deployment in deployment.name.hasSuffix("0") },
            onSelectDeployment: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenUnifiedLogs: { _ in },
            onOpenRollout: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let deploymentCoordinator = deploymentView.makeCoordinator()
        deploymentTable.dataSource = deploymentCoordinator
        deploymentTable.delegate = deploymentCoordinator

        let services = benchmarkServices(count: 500)
        let serviceTable = benchmarkTable(columnIDs: ["name", "type", "clusterIP", "favorite"])
        let serviceView = AppKitServiceListView(
            services: services,
            selectedServiceID: services[50].id,
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: true,
            isFavorite: { service in service.name.hasSuffix("0") },
            onSelectService: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenUnifiedLogs: { _ in },
            onOpenPortForward: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let serviceCoordinator = serviceView.makeCoordinator()
        serviceTable.dataSource = serviceCoordinator
        serviceTable.delegate = serviceCoordinator

        let releases = benchmarkHelmReleases(count: 500)
        let helmTable = benchmarkTable(columnIDs: ["name", "status", "namespace", "revision", "chart", "appVersion"])
        let helmView = AppKitHelmReleaseListView(
            releases: releases,
            selectedReleaseID: releases[50].id,
            sortColumn: .name,
            sortAscending: true,
            onSelectRelease: { _ in },
            onToggleSort: { _ in }
        )
        let helmCoordinator = helmView.makeCoordinator()
        helmTable.dataSource = helmCoordinator
        helmTable.delegate = helmCoordinator

        let events = benchmarkEvents(count: 500)
        let eventTable = benchmarkTable(columnIDs: ["reason", "type", "object", "namespace", "lastSeen", "message"])
        let eventView = AppKitEventListView(
            events: events,
            selectedEventID: events[50].id,
            sortColumn: .lastSeen,
            sortAscending: false,
            onSelectEvent: { _ in },
            onToggleSort: { _ in }
        )
        let eventCoordinator = eventView.makeCoordinator()
        eventTable.dataSource = eventCoordinator
        eventTable.delegate = eventCoordinator

        let operatorResources = benchmarkOperatorResources(count: 500)
        let operatorTable = benchmarkTable(columnIDs: ["name", "family", "kind", "namespace", "status", "apiPath", "favorite"])
        let operatorView = AppKitOperatorResourceListView(
            resources: operatorResources,
            selectedResourceID: operatorResources[50].id,
            sortColumn: .name,
            sortAscending: true,
            showsPrinterColumns: true,
            isFavorite: { resource in resource.name.hasSuffix("0") },
            onSelectResource: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in }
        )
        let operatorCoordinator = operatorView.makeCoordinator()
        operatorTable.dataSource = operatorCoordinator
        operatorTable.delegate = operatorCoordinator

        let visibleRows = 32

        func projectVisibleCells() -> Int {
            var projected = 0
            for row in 0..<visibleRows {
                for column in deploymentTable.tableColumns where deploymentCoordinator.tableView(deploymentTable, viewFor: column, row: row) != nil {
                    projected += 1
                }
                for column in serviceTable.tableColumns where serviceCoordinator.tableView(serviceTable, viewFor: column, row: row) != nil {
                    projected += 1
                }
                for column in helmTable.tableColumns where helmCoordinator.tableView(helmTable, viewFor: column, row: row) != nil {
                    projected += 1
                }
                for column in eventTable.tableColumns where eventCoordinator.tableView(eventTable, viewFor: column, row: row) != nil {
                    projected += 1
                }
                for column in operatorTable.tableColumns where operatorCoordinator.tableView(operatorTable, viewFor: column, row: row) != nil {
                    projected += 1
                }
            }
            return projected
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            XCTAssertEqual(projectVisibleCells(), 832)
        }

        let elapsedSeconds = minimumElapsedSeconds {
            _ = projectVisibleCells()
        }

        XCTAssertLessThan(
            elapsedSeconds,
            0.65,
            "KPI: secondary AppKit resource tables should project 32 visible rows across deployment, service, Helm, event, and operator lists under 650ms in debug."
        )
    }

    @MainActor
    func testAppKitSecondaryResourceContextMenuConstructionBenchmarkKPI() {
        let deployments = benchmarkDeployments(count: 300)
        let deploymentView = AppKitDeploymentListView(
            deployments: deployments,
            selectedDeploymentID: deployments[0].id,
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: true,
            isFavorite: { deployment in deployment.name.hasSuffix("0") },
            onSelectDeployment: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenUnifiedLogs: { _ in },
            onOpenRollout: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let deploymentCoordinator = deploymentView.makeCoordinator()

        let services = benchmarkServices(count: 300)
        let serviceView = AppKitServiceListView(
            services: services,
            selectedServiceID: services[0].id,
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: true,
            isFavorite: { service in service.name.hasSuffix("0") },
            onSelectService: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenUnifiedLogs: { _ in },
            onOpenPortForward: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let serviceCoordinator = serviceView.makeCoordinator()

        let releases = benchmarkHelmReleases(count: 300)
        let helmView = AppKitHelmReleaseListView(
            releases: releases,
            selectedReleaseID: releases[0].id,
            sortColumn: .name,
            sortAscending: true,
            onSelectRelease: { _ in },
            onToggleSort: { _ in }
        )
        let helmCoordinator = helmView.makeCoordinator()

        let events = benchmarkEvents(count: 300)
        let eventView = AppKitEventListView(
            events: events,
            selectedEventID: events[0].id,
            sortColumn: .reason,
            sortAscending: true,
            onSelectEvent: { _ in },
            onToggleSort: { _ in }
        )
        let eventCoordinator = eventView.makeCoordinator()

        let operatorResources = benchmarkOperatorResources(count: 300)
        let operatorView = AppKitOperatorResourceListView(
            resources: operatorResources,
            selectedResourceID: operatorResources[0].id,
            sortColumn: .name,
            sortAscending: true,
            showsPrinterColumns: true,
            isFavorite: { resource in resource.name.hasSuffix("0") },
            onSelectResource: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in }
        )
        let operatorCoordinator = operatorView.makeCoordinator()

        func buildMenus() -> Int {
            var itemCount = 0
            for row in 0..<120 {
                itemCount += deploymentCoordinator.makeMenu(forRow: row)?.items.count ?? 0
                itemCount += serviceCoordinator.makeMenu(forRow: row)?.items.count ?? 0
                itemCount += helmCoordinator.makeMenu(forRow: row)?.items.count ?? 0
                itemCount += eventCoordinator.makeMenu(forRow: row)?.items.count ?? 0
                itemCount += operatorCoordinator.makeMenu(forRow: row)?.items.count ?? 0
            }
            return itemCount
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            XCTAssertGreaterThan(buildMenus(), 0)
        }

        let elapsedSeconds = minimumElapsedSeconds {
            _ = buildMenus()
        }

        XCTAssertGreaterThan(buildMenus(), 3_000)
        XCTAssertLessThan(
            elapsedSeconds,
            0.3,
            "KPI: secondary AppKit resource context menus should build under 300ms for 600 synthetic row menus in debug."
        )
    }

    @MainActor
    func testAppKitResourceColumnResizePersistenceBenchmarkKPI() {
        let touchedColumnsByTable = [
            "pods": ["name", "cpu", "memory", "restarts", "age", "status"],
            "deployments": ["name", "replicas"],
            "services": ["name", "type", "clusterIP"],
            "genericResources": ["name", "primary", "secondary", "namespace"],
            "helmReleases": ["name", "status", "namespace", "revision", "chart", "appVersion"],
            "events": ["reason", "type", "object", "namespace", "lastSeen", "message"],
            "operatorResources": ["name", "family", "kind", "namespace", "status", "apiPath"]
        ]
        let touchedKeys = touchedColumnsByTable.flatMap { tableID, columnIDs in
            columnIDs.map { columnID in "rune.settings.layout.resourceColumnWidths.\(tableID).\(columnID)" }
        }
        let savedDefaults = Dictionary(uniqueKeysWithValues: touchedKeys.map { key in
            (key, UserDefaults.standard.object(forKey: key))
        })
        func restoreTouchedColumnWidths() {
            for key in touchedKeys {
                if let savedValue = savedDefaults[key],
                   let value = savedValue {
                    UserDefaults.standard.set(value, forKey: key)
                } else {
                    UserDefaults.standard.removeObject(forKey: key)
                }
            }
        }
        for key in touchedKeys {
            UserDefaults.standard.removeObject(forKey: key)
        }
        defer { restoreTouchedColumnWidths() }

        let pods = benchmarkPods(count: 120)
        let podView = AppKitPodTableView(
            pods: pods,
            selectedPodID: pods[0].id,
            selectedPodIDs: [],
            sortColumn: .name,
            sortAscending: true,
            nameColumnWidth: PodTableLayout.nameColumnDefaultWidth,
            canApplyClusterMutations: true,
            isFavorite: { _ in false },
            onSelectPod: { _ in },
            onToggleBulkSelection: { _ in },
            onToggleSort: { _ in },
            onNameColumnWidthChanged: { _ in },
            onToggleFavorite: { _ in },
            onOpenLogs: { _ in },
            onOpenExec: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let podCoordinator = podView.makeCoordinator()
        let podTable = benchmarkTable(columnIDs: ["selection", "name", "cpu", "memory", "restarts", "age", "status", "favorite"])
        podCoordinator.tableView = podTable

        let deployments = benchmarkDeployments(count: 120)
        let deploymentView = AppKitDeploymentListView(
            deployments: deployments,
            selectedDeploymentID: deployments[0].id,
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: true,
            isFavorite: { _ in false },
            onSelectDeployment: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenUnifiedLogs: { _ in },
            onOpenRollout: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let deploymentCoordinator = deploymentView.makeCoordinator()
        let deploymentTable = benchmarkTable(columnIDs: ["name", "replicas", "favorite"])
        deploymentCoordinator.tableView = deploymentTable

        let services = benchmarkServices(count: 120)
        let serviceView = AppKitServiceListView(
            services: services,
            selectedServiceID: services[0].id,
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: true,
            isFavorite: { _ in false },
            onSelectService: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenUnifiedLogs: { _ in },
            onOpenPortForward: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let serviceCoordinator = serviceView.makeCoordinator()
        let serviceTable = benchmarkTable(columnIDs: ["name", "type", "clusterIP", "favorite"])
        serviceCoordinator.tableView = serviceTable

        let resources = benchmarkConfigMaps(count: 120)
        let genericView = AppKitGenericResourceListView(
            resources: resources,
            selectedResourceID: resources[0].id,
            selectedResourceIDs: [],
            sortColumn: .name,
            sortAscending: true,
            canApplyClusterMutations: true,
            isFavorite: { _ in false },
            onSelectResource: { _ in },
            onToggleBulkSelection: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in },
            onDelete: { _ in }
        )
        let genericCoordinator = genericView.makeCoordinator()
        let genericTable = benchmarkTable(columnIDs: ["selection", "name", "primary", "secondary", "namespace", "favorite"])
        genericCoordinator.tableView = genericTable

        let releases = benchmarkHelmReleases(count: 120)
        let helmView = AppKitHelmReleaseListView(
            releases: releases,
            selectedReleaseID: releases[0].id,
            sortColumn: .name,
            sortAscending: true,
            onSelectRelease: { _ in },
            onToggleSort: { _ in }
        )
        let helmCoordinator = helmView.makeCoordinator()
        let helmTable = benchmarkTable(columnIDs: ["name", "status", "namespace", "revision", "chart", "appVersion"])
        helmCoordinator.tableView = helmTable

        let events = benchmarkEvents(count: 120)
        let eventView = AppKitEventListView(
            events: events,
            selectedEventID: events[0].id,
            sortColumn: .reason,
            sortAscending: true,
            onSelectEvent: { _ in },
            onToggleSort: { _ in }
        )
        let eventCoordinator = eventView.makeCoordinator()
        let eventTable = benchmarkTable(columnIDs: ["reason", "type", "object", "namespace", "lastSeen", "message"])
        eventCoordinator.tableView = eventTable

        let operatorResources = benchmarkOperatorResources(count: 120)
        let operatorView = AppKitOperatorResourceListView(
            resources: operatorResources,
            selectedResourceID: operatorResources[0].id,
            sortColumn: .name,
            sortAscending: true,
            showsPrinterColumns: true,
            isFavorite: { _ in false },
            onSelectResource: { _ in },
            onToggleSort: { _ in },
            onToggleFavorite: { _ in },
            onOpenDescribe: { _ in },
            onOpenYAML: { _ in }
        )
        let operatorCoordinator = operatorView.makeCoordinator()
        let operatorTable = benchmarkTable(columnIDs: ["name", "family", "kind", "namespace", "status", "apiPath", "favorite"])
        operatorCoordinator.tableView = operatorTable

        let resizeSamples = stride(from: CGFloat(180), through: CGFloat(520), by: CGFloat(17)).map { $0 }
        func runResizePasses() {
            for sample in resizeSamples {
                for column in podTable.tableColumns where touchedColumnsByTable["pods"]?.contains(column.identifier.rawValue) == true {
                    column.width = sample
                    podCoordinator.tableViewColumnDidResize(resizeNotification(for: column))
                }
                for column in deploymentTable.tableColumns where touchedColumnsByTable["deployments"]?.contains(column.identifier.rawValue) == true {
                    column.width = sample
                    deploymentCoordinator.tableViewColumnDidResize(resizeNotification(for: column))
                }
                for column in serviceTable.tableColumns where touchedColumnsByTable["services"]?.contains(column.identifier.rawValue) == true {
                    column.width = sample
                    serviceCoordinator.tableViewColumnDidResize(resizeNotification(for: column))
                }
                for column in genericTable.tableColumns where touchedColumnsByTable["genericResources"]?.contains(column.identifier.rawValue) == true {
                    column.width = sample
                    genericCoordinator.tableViewColumnDidResize(resizeNotification(for: column))
                }
                for column in helmTable.tableColumns where touchedColumnsByTable["helmReleases"]?.contains(column.identifier.rawValue) == true {
                    column.width = sample
                    helmCoordinator.tableViewColumnDidResize(resizeNotification(for: column))
                }
                for column in eventTable.tableColumns where touchedColumnsByTable["events"]?.contains(column.identifier.rawValue) == true {
                    column.width = sample
                    eventCoordinator.tableViewColumnDidResize(resizeNotification(for: column))
                }
                for column in operatorTable.tableColumns where touchedColumnsByTable["operatorResources"]?.contains(column.identifier.rawValue) == true {
                    column.width = sample
                    operatorCoordinator.tableViewColumnDidResize(resizeNotification(for: column))
                }
            }
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            runResizePasses()
        }

        let elapsedSeconds = minimumElapsedSeconds {
            runResizePasses()
        }

        XCTAssertLessThan(
            elapsedSeconds,
            0.35,
            "KPI: AppKit resource column resize persistence should stay below 350ms for repeated resize notifications across all resource tables in debug."
        )
    }

    @MainActor
    func testAppKitResourceColumnResizePreviewBenchmarkKPI() {
        let table = benchmarkTable(columnIDs: [
            "selection", "name", "cpu", "memory", "restarts", "age", "status", "favorite",
            "primary", "secondary", "namespace", "message", "apiPath"
        ])
        table.frame = NSRect(x: 0, y: 0, width: 1_320, height: 720)
        table.headerView = NSTableHeaderView(frame: NSRect(x: 0, y: 0, width: 1_320, height: 24))
        for column in table.tableColumns {
            column.minWidth = column.identifier.rawValue == "name" ? 180 : 56
            column.maxWidth = column.identifier.rawValue == "favorite" ? 80 : 720
        }
        let scrollView = NSScrollView(frame: NSRect(x: 0, y: 0, width: 980, height: 720))
        scrollView.documentView = table

        let resizableColumns = table.tableColumns.filter {
            $0.identifier.rawValue != "selection" && $0.identifier.rawValue != "favorite"
        }
        let samples = stride(from: CGFloat(140), through: CGFloat(620), by: CGFloat(4)).map { $0 }

        func runPreviewPasses() -> CGFloat {
            var checksum: CGFloat = 0
            for _ in 0..<2 {
                for sample in samples {
                    for column in resizableColumns {
                        checksum += applySynchronizedResourceColumnResize(sample, for: column, in: table)
                    }
                }
            }
            return checksum
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            XCTAssertGreaterThan(runPreviewPasses(), 0)
        }

        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            _ = runPreviewPasses()
        }

        XCTAssertGreaterThanOrEqual(table.frame.width, table.tableColumns.reduce(CGFloat(0)) { $0 + $1.width })
        #if DEBUG
        let maximumResizePreviewSeconds = 0.35
        #else
        let maximumResizePreviewSeconds = 0.18
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumResizePreviewSeconds,
            "KPI: live AppKit resource column resize preview should stay below 350ms in debug and 180ms in release for repeated drag samples without forcing visible cell text redraw."
        )
    }

    @MainActor
    func testAppKitResourceColumnResizePreviewKeepsHeaderAndVisibleCellsAligned() {
        let table = NSTableView(frame: NSRect(x: 0, y: 0, width: 720, height: 240))
        table.headerView = NSTableHeaderView(frame: NSRect(x: 0, y: 0, width: 720, height: 24))
        table.rowHeight = 34
        table.intercellSpacing = NSSize(width: 0, height: 4)
        let dataSource = BenchmarkTableDataSource(rowCount: 8)
        table.dataSource = dataSource
        table.delegate = dataSource

        for columnID in ["name", "cpu", "memory"] {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(columnID))
            column.width = columnID == "name" ? 260 : 120
            column.minWidth = 80
            column.maxWidth = 620
            table.addTableColumn(column)
        }

        let scrollView = NSScrollView(frame: NSRect(x: 0, y: 0, width: 520, height: 240))
        scrollView.documentView = table
        table.noteNumberOfRowsChanged()
        table.layoutSubtreeIfNeeded()

        for row in 0..<dataSource.rowCount {
            for column in 0..<table.numberOfColumns {
                _ = table.view(atColumn: column, row: row, makeIfNecessary: true)
            }
        }
        table.layoutSubtreeIfNeeded()

        guard let nameColumn = table.tableColumns.first(where: { $0.identifier.rawValue == "name" }) else {
            XCTFail("Missing name column")
            return
        }

        for width in [CGFloat(420), CGFloat(180)] {
            applySynchronizedResourceColumnResize(width, for: nameColumn, in: table)

            let renderedColumnWidth = table.rect(ofColumn: 0).width
            XCTAssertEqual(nameColumn.width, width, accuracy: 1.0)
            XCTAssertEqual(table.headerView?.headerRect(ofColumn: 0).width ?? 0, renderedColumnWidth, accuracy: 1.0)
            for row in 0..<dataSource.rowCount {
                let cell = table.view(atColumn: 0, row: row, makeIfNecessary: false)
                XCTAssertEqual(cell?.frame.width ?? 0, width, accuracy: 1.0)
            }
        }
    }

    @MainActor
    func testAppKitResourceColumnResizePreviewDoesNotForceVisibleCellRedraw() {
        let table = NSTableView(frame: NSRect(x: 0, y: 0, width: 920, height: 360))
        table.headerView = NSTableHeaderView(frame: NSRect(x: 0, y: 0, width: 920, height: 24))
        table.rowHeight = 34
        table.intercellSpacing = NSSize(width: 0, height: 4)
        let dataSource = BenchmarkTableDataSource(rowCount: 14)
        table.dataSource = dataSource
        table.delegate = dataSource

        for columnID in ["name", "cpu", "memory", "status", "favorite"] {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(columnID))
            column.width = columnID == "name" ? 300 : 120
            column.minWidth = 80
            column.maxWidth = 640
            table.addTableColumn(column)
        }

        let scrollView = NSScrollView(frame: NSRect(x: 0, y: 0, width: 640, height: 360))
        scrollView.documentView = table
        table.noteNumberOfRowsChanged()
        table.layoutSubtreeIfNeeded()

        for row in 0..<dataSource.rowCount {
            for column in 0..<table.numberOfColumns {
                _ = table.view(atColumn: column, row: row, makeIfNecessary: true)
            }
        }
        table.layoutSubtreeIfNeeded()

        let visibleCells = (0..<dataSource.rowCount).flatMap { row in
            (0..<table.numberOfColumns).compactMap { column in
                table.view(atColumn: column, row: row, makeIfNecessary: false)
            }
        }
        visibleCells.forEach { $0.needsDisplay = false }

        guard let nameColumn = table.tableColumns.first(where: { $0.identifier.rawValue == "name" }) else {
            XCTFail("Missing name column")
            return
        }

        applySynchronizedResourceColumnResize(440, for: nameColumn, in: table)

        XCTAssertTrue(
            visibleCells.allSatisfy { !$0.needsDisplay },
            "Live resize preview should relayout visible cells without forcing text redraw on every drag event."
        )
    }

    @MainActor
    func testAppKitResourceColumnResizePreviewSuppressesResizeNotifications() {
        let table = NSTableView(frame: NSRect(x: 0, y: 0, width: 720, height: 240))
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("name"))
        column.width = 260
        column.minWidth = 80
        column.maxWidth = 620
        table.addTableColumn(column)

        let probe = ResizeNotificationProbe()
        NotificationCenter.default.addObserver(
            probe,
            selector: #selector(ResizeNotificationProbe.tableColumnDidResize(_:)),
            name: NSTableView.columnDidResizeNotification,
            object: table
        )
        defer { NotificationCenter.default.removeObserver(probe) }

        applySynchronizedResourceColumnResize(420, for: column, in: table)

        XCTAssertEqual(
            probe.unsuppressedNotificationCount,
            0,
            "Live resize preview should not run the full column resize persistence/render pipeline for every drag sample."
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
                    checksum += operatorWidths.name + operatorWidths.family + operatorWidths.kind + operatorWidths.namespace + operatorWidths.status + operatorWidths.printerColumns + operatorWidths.apiPath + operatorWidths.favorite
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
                RuneAppKitResourceListLayout.deploymentMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.deploymentTrailingBreathingRoom
            )
            XCTAssertEqual(total, max(deploymentUsableWidth, deploymentMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(total, max(RuneAppKitResourceListLayout.deploymentMaximumContentWidth, deploymentMinimumTotal))
            XCTAssertGreaterThanOrEqual(total, previousTotal)
            previousTotal = total

            let serviceWidths = RuneAppKitResourceListLayout.serviceColumnWidths(visibleWidth: visibleWidth)
            let serviceTotal = serviceWidths.name + serviceWidths.type + serviceWidths.clusterIP + serviceWidths.favorite
            let serviceMinimumTotal = RuneAppKitResourceListLayout.serviceMinimumNameColumnWidth
                + RuneAppKitResourceListLayout.serviceTypeColumnWidth
                + RuneAppKitResourceListLayout.serviceClusterIPColumnWidth
                + RuneAppKitResourceListLayout.serviceFavoriteColumnWidth
            let serviceUsableWidth = min(
                RuneAppKitResourceListLayout.serviceMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.serviceTrailingBreathingRoom
            )
            XCTAssertEqual(serviceTotal, max(serviceUsableWidth, serviceMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(serviceTotal, max(RuneAppKitResourceListLayout.serviceMaximumContentWidth, serviceMinimumTotal))

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
                RuneAppKitResourceListLayout.genericMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.genericTrailingBreathingRoom
            )
            XCTAssertEqual(genericTotal, max(genericUsableWidth, genericMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(genericTotal, max(RuneAppKitResourceListLayout.genericMaximumContentWidth, genericMinimumTotal))

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
            let operatorMetadataTotal = operatorWidths.namespace + operatorWidths.status + operatorWidths.printerColumns + operatorWidths.apiPath + operatorWidths.favorite
            let operatorTotal = operatorPrimaryTotal + operatorMetadataTotal
            let operatorMinimumTotal = RuneAppKitResourceListLayout.operatorMinimumNameColumnWidth
                + RuneAppKitResourceListLayout.operatorFamilyColumnWidth
                + RuneAppKitResourceListLayout.operatorKindColumnWidth
                + RuneAppKitResourceListLayout.operatorNamespaceColumnWidth
                + RuneAppKitResourceListLayout.operatorStatusColumnWidth
                + RuneAppKitResourceListLayout.operatorPrinterColumnsColumnWidth
                + RuneAppKitResourceListLayout.operatorAPIPathColumnWidth
                + RuneAppKitResourceListLayout.operatorFavoriteColumnWidth
            let operatorUsableWidth = min(
                RuneAppKitResourceListLayout.operatorMaximumContentWidth,
                visibleWidth.rounded(.toNearestOrAwayFromZero) - RuneAppKitResourceListLayout.operatorTrailingBreathingRoom
            )
            XCTAssertEqual(operatorTotal, max(operatorUsableWidth, operatorMinimumTotal), accuracy: 0.5)
            XCTAssertLessThanOrEqual(operatorTotal, max(RuneAppKitResourceListLayout.operatorMaximumContentWidth, operatorMinimumTotal))

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
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.operatorPrinterColumnsColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "Columns", reservesSortIndicator: false))
            XCTAssertGreaterThanOrEqual(RuneAppKitResourceListLayout.operatorAPIPathColumnWidth, RuneAppKitResourceListLayout.minimumHeaderColumnWidth(title: "API Path", reservesSortIndicator: true))
        }

        let wideDeployment = RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: 1320)
        XCTAssertGreaterThanOrEqual(wideDeployment.replicas, 88)
        XCTAssertLessThanOrEqual(wideDeployment.name, 510)

        let wideGeneric = RuneAppKitResourceListLayout.genericColumnWidths(visibleWidth: 1320)
        XCTAssertLessThanOrEqual(wideGeneric.name, 380)
        XCTAssertLessThanOrEqual(wideGeneric.primary, 144)
        XCTAssertLessThanOrEqual(wideGeneric.secondary, 164)
        XCTAssertLessThanOrEqual(wideGeneric.namespace, 132)

        let wideEvents = RuneAppKitResourceListLayout.eventColumnWidths(visibleWidth: 1320)
        XCTAssertLessThanOrEqual(wideEvents.reason, 204)
        XCTAssertLessThanOrEqual(wideEvents.type, 120)
        XCTAssertLessThanOrEqual(wideEvents.namespace, 140)

        let wideOperators = RuneAppKitResourceListLayout.operatorColumnWidths(visibleWidth: 1320)
        XCTAssertLessThanOrEqual(wideOperators.name, 262)
        XCTAssertLessThanOrEqual(wideOperators.family, 140)
        XCTAssertLessThanOrEqual(wideOperators.status, 130)

        func projectedChecksum() -> CGFloat {
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
                    checksum += operatorResource.name + operatorResource.family + operatorResource.kind + operatorResource.namespace + operatorResource.status + operatorResource.printerColumns + operatorResource.apiPath + operatorResource.favorite
                }
            }
            return checksum
        }

        let checksum = projectedChecksum()
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            _ = projectedChecksum()
        }

        #if DEBUG
        let maximumProjectionSeconds = 0.025
        #else
        let maximumProjectionSeconds = 0.01
        #endif

        XCTAssertGreaterThan(checksum, 0)
        XCTAssertLessThan(
            elapsedSeconds,
            maximumProjectionSeconds,
            "KPI: resource column width projection should stay below 25ms in debug and 10ms in release for 50 passes across resize samples so the middle panel tracks the side panel."
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

        let visible = viewModel.visiblePods
        let elapsedSeconds = minimumElapsedSeconds {
            _ = viewModel.visiblePods
        }

        XCTAssertEqual(visible.count, 5_000)
        XCTAssertEqual(visible.prefix(3).map(\.name), ["pod-0999", "pod-1998", "pod-2997"])
        XCTAssertLessThan(elapsedSeconds, 0.35, "KPI: numeric pod CPU sorting should stay below 350ms for 5,000 rows in debug.")
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

        var visible: [String] = []
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            visible = viewModel.namespaceOptions
        }

        XCTAssertEqual(visible.count, 2_000)
        XCTAssertEqual(visible.prefix(3), ["namespace-0000", "namespace-0200", "namespace-0400"])
        XCTAssertLessThan(elapsedSeconds, 0.2)
    }

    @MainActor
    func testContextSwitchNamespaceCarryResolutionBenchmarkKPI() {
        let viewModel = RuneAppViewModel(kubeConfigDiscoverer: EmptyKubeConfigDiscoverer())
        let namespaces = ["default", "namespace-blue", "namespace-green", "namespace-orange", "kube-system"]
            + (0..<2_000).map { "namespace-\(String(format: "%04d", $0))" }
        let contexts = (0..<100).map { "cluster-\(String(format: "%03d", $0))" }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            var checksum = 0
            for index in 0..<200 {
                let resolved = viewModel.resolvedNamespace(
                    contextName: contexts[index % contexts.count],
                    preferredCandidates: index.isMultiple(of: 2)
                        ? ["namespace-blue", "namespace-green"]
                        : ["missing", "namespace-green"],
                    availableNamespaces: namespaces,
                    contextDefaultNamespace: "default"
                )
                checksum += resolved.count
            }
            XCTAssertGreaterThan(checksum, 0)
        }

        func resolveNamespaceBatch() -> (carried: String, fallback: String) {
            var lastCarried = ""
            var lastFallback = ""
            for index in 0..<100 {
                lastCarried = viewModel.resolvedNamespace(
                    contextName: contexts[index % contexts.count],
                    preferredCandidates: ["namespace-blue", "namespace-green"],
                    availableNamespaces: namespaces,
                    contextDefaultNamespace: "default"
                )
                lastFallback = viewModel.resolvedNamespace(
                    contextName: contexts[index % contexts.count],
                    preferredCandidates: ["missing", "namespace-green"],
                    availableNamespaces: namespaces,
                    contextDefaultNamespace: "default"
                )
            }
            return (lastCarried, lastFallback)
        }
        let resolved = resolveNamespaceBatch()
        let elapsedSeconds = minimumElapsedSeconds {
            _ = resolveNamespaceBatch()
        }

        XCTAssertEqual(resolved.carried, "namespace-blue")
        XCTAssertEqual(resolved.fallback, "namespace-green")
        XCTAssertLessThan(
            elapsedSeconds,
            0.2,
            "KPI: cached namespace carry resolution should stay below 200ms for 200 lookup decisions and must not add API work."
        )
    }

    @MainActor
    func testOverviewIncidentProjectionBenchmarkKPI() {
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        let pods = (0..<5_000).map { index in
            PodSummary(
                name: "synthetic-app-\(String(format: "%04d", index))-7d8c9f6b5-\(String(format: "%05d", index))",
                namespace: "namespace-\(index % 20)",
                status: index.isMultiple(of: 37) ? "CrashLoopBackOff" : "Running",
                totalRestarts: index.isMultiple(of: 41) ? 6 : index.isMultiple(of: 17) ? 1 : 0,
                containersReady: index.isMultiple(of: 53) ? "0/1" : "1/1"
            )
        }
        let deployments = (0..<400).map { index in
            DeploymentSummary(
                name: "synthetic-app-\(String(format: "%04d", index))",
                namespace: "namespace-\(index % 20)",
                readyReplicas: index.isMultiple(of: 19) ? 0 : 2,
                desiredReplicas: 2,
                selector: ["app": "synthetic-app-\(String(format: "%04d", index))"]
            )
        }
        let services = (0..<400).map { index in
            ServiceSummary(
                name: "synthetic-app-\(String(format: "%04d", index))",
                namespace: "namespace-\(index % 20)",
                type: "ClusterIP",
                clusterIP: "10.0.\(index / 255).\(index % 255)",
                selector: ["app": "synthetic-app-\(String(format: "%04d", index))"]
            )
        }
        let events = (0..<2_000).map { index in
            EventSummary(
                type: index.isMultiple(of: 3) ? "Warning" : "Normal",
                reason: index.isMultiple(of: 3) ? "BackOff" : "Pulled",
                objectName: "synthetic-app-\(String(format: "%04d", index % 400))",
                message: "Synthetic event \(index)",
                lastTimestamp: "2026-05-13T10:\(String(format: "%02d", index % 60)):00Z",
                involvedKind: "Pod",
                involvedNamespace: "namespace-\(index % 20)"
            )
        }
        state.setDeployments(deployments)
        state.setServices(services)
        state.setOverviewSnapshot(
            pods: pods,
            deploymentsCount: deployments.count,
            servicesCount: services.count,
            ingressesCount: 0,
            configMapsCount: 0,
            cronJobsCount: 0,
            nodesCount: 50,
            events: events
        )

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = viewModel.overviewUnhealthyItems
            _ = viewModel.overviewIncidentTimelineItems
            _ = viewModel.overviewDependencyItems
        }

        let unhealthy = viewModel.overviewUnhealthyItems
        let incidents = viewModel.overviewIncidentTimelineItems
        let dependencies = viewModel.overviewDependencyItems
        let elapsedSeconds = minimumElapsedSeconds {
            _ = viewModel.overviewUnhealthyItems
            _ = viewModel.overviewIncidentTimelineItems
            _ = viewModel.overviewDependencyItems
        }

        XCTAssertFalse(unhealthy.isEmpty)
        XCTAssertFalse(incidents.isEmpty)
        XCTAssertFalse(dependencies.isEmpty)
        XCTAssertLessThan(
            elapsedSeconds,
            0.12,
            "KPI: overview unhealthy, incident, and dependency projections over 5k pods and 2k events should stay below 120ms in debug."
        )
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

        var favoriteResourceCount = 0
        var favoriteNamespaceCount = 0
        var preferredNamespace: String?
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            let reloaded = FileBackedContextPreferencesStore(url: url)
            favoriteResourceCount = reloaded.loadFavoriteResourceIDs().count
            favoriteNamespaceCount = reloaded.loadFavoriteNamespaceIDs().count
            preferredNamespace = reloaded.loadPreferredNamespace(for: "context-005")
        }

        XCTAssertEqual(favoriteResourceCount, 5_000)
        XCTAssertEqual(favoriteNamespaceCount, 500)
        XCTAssertEqual(preferredNamespace, "namespace-5")
        XCTAssertLessThan(elapsedSeconds, 0.08, "KPI: preferences load should stay below 80ms for a realistic local app-state file in debug.")
    }

    func testKubeConfigImportDuplicateDetectionBenchmarkKPI() {
        let contexts = (0..<500).map { index in
            """
            - name: context-\(String(format: "%03d", index))
              context:
                cluster: cluster-\(String(format: "%03d", index))
                user: user-\(String(format: "%03d", index))
            """
        }.joined(separator: "\n")
        let clusters = (0..<500).map { index in
            """
            - name: cluster-\(String(format: "%03d", index))
              cluster:
                server: https://cluster-\(String(format: "%03d", index)).example.invalid
            """
        }.joined(separator: "\n")
        let users = (0..<500).map { index in
            """
            - name: user-\(String(format: "%03d", index))
              user:
                token: test-token-\(index)
            """
        }.joined(separator: "\n")
        let raw = """
        apiVersion: v1
        kind: Config
        current-context: context-000
        clusters:
        \(clusters)
        contexts:
        \(contexts)
        users:
        \(users)
        """
        let validator = KubeConfigImportValidator()

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = validator.validate(raw: raw)
        }

        let review = validator.validate(raw: raw)
        let elapsedSeconds = minimumElapsedSeconds {
            _ = validator.validate(raw: raw)
        }

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.count, 500)
        #if DEBUG
        let maximumDuplicateDetectionSeconds = 0.12
        #else
        let maximumDuplicateDetectionSeconds = 0.05
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumDuplicateDetectionSeconds,
            "KPI: duplicate detection over 500 contexts should stay below 120ms in debug and 50ms in release."
        )
    }

    func testKubeConfigImportReviewProjectionBenchmarkKPI() {
        let payloads = (0..<120).map { fileIndex in
            let contexts = (0..<5).map { contextIndex in
                """
                - name: context-\(fileIndex)-\(contextIndex)
                  context:
                    cluster: cluster-\(fileIndex)-\(contextIndex)
                    user: user-\(fileIndex)-\(contextIndex)
                    namespace: namespace-\(contextIndex)
                """
            }.joined(separator: "\n")
            let clusters = (0..<5).map { contextIndex in
                """
                - name: cluster-\(fileIndex)-\(contextIndex)
                  cluster:
                    server: https://cluster-\(fileIndex)-\(contextIndex).example.invalid
                """
            }.joined(separator: "\n")
            let users = (0..<5).map { contextIndex in
                """
                - name: user-\(fileIndex)-\(contextIndex)
                  user:
                    token: synthetic-token-\(fileIndex)-\(contextIndex)
                """
            }.joined(separator: "\n")
            return """
            apiVersion: v1
            kind: Config
            current-context: context-\(fileIndex)-0
            clusters:
            \(clusters)
            contexts:
            \(contexts)
            users:
            \(users)
            """
        }
        let validator = KubeConfigImportValidator()

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = payloads.map { validator.validate(raw: $0) }
        }

        let reviews = payloads.map { validator.validate(raw: $0) }
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 7) {
            _ = payloads.map { validator.validate(raw: $0) }
        }

        XCTAssertEqual(reviews.flatMap(\.contexts).count, 600)
        XCTAssertTrue(reviews.allSatisfy(\.isValid))
        XCTAssertFalse(reviews.contains { $0.redactedPreview.contains("synthetic-token") })
        #if DEBUG
        let maximumReviewProjectionSeconds = 0.12
        #else
        let maximumReviewProjectionSeconds = 0.08
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumReviewProjectionSeconds,
            "KPI: import review projection for 120 selected/folder kubeconfig files should stay below 120ms in debug and 80ms in release."
        )
    }

    func testKubeConfigExpandedProviderAuthShapeBenchmarkKPI() {
        let contexts = (0..<180).map { index in
            """
            - context:
                cluster: cluster-\(String(format: "%03d", index))
                user: user-\(String(format: "%03d", index))
                namespace: namespace-\(index % 9)
              name: context-\(String(format: "%03d", index))
            """
        }.joined(separator: "\n")
        let clusters = (0..<180).map { index in
            let host: String
            switch index % 6 {
            case 0:
                host = "https://doks-\(index).example.invalid"
            case 1:
                host = "https://rancher-\(index).example.invalid"
            case 2:
                host = "https://openshift-\(index).example.invalid"
            case 3:
                host = "https://oidc-\(index).example.invalid"
            case 4:
                host = "https://crc-\(index).example.invalid"
            default:
                host = "https://generic-\(index).example.invalid"
            }
            return """
            - cluster:
                server: \(host)
              name: cluster-\(String(format: "%03d", index))
            """
        }.joined(separator: "\n")
        let users = (0..<180).map { index -> String in
            let userName = "user-\(String(format: "%03d", index))"
            switch index % 6 {
            case 0:
                return """
                - user:
                    exec:
                      command: doctl
                  name: \(userName)
                """
            case 1:
                return """
                - user:
                    exec:
                      command: rancher
                  name: \(userName)
                """
            case 2:
                return """
                - user:
                    exec:
                      command: oc
                  name: \(userName)
                """
            case 3:
                return """
                - user:
                    auth-provider:
                      name: oidc
                      config:
                        issuer-url: https://issuer-\(index).example.invalid
                        client-id: synthetic-client-\(index)
                        id-token: synthetic-id-token-\(index)
                  name: \(userName)
                """
            case 4:
                return """
                - user:
                    tokenFile: /synthetic/token-\(index).txt
                  name: \(userName)
                """
            default:
                return """
                - user:
                    username: synthetic-user-\(index)
                    password: synthetic-password-\(index)
                    client-certificate: /synthetic/client-\(index).crt
                    client-key: /synthetic/client-\(index).key
                  name: \(userName)
                """
            }
        }.joined(separator: "\n")
        let raw = """
        apiVersion: v1
        kind: Config
        current-context: context-000
        clusters:
        \(clusters)
        contexts:
        \(contexts)
        users:
        \(users)
        """
        let validator = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"])

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = validator.validate(raw: raw)
        }

        let review = validator.validate(raw: raw)
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 7) {
            _ = validator.validate(raw: raw)
        }
        let authTypes = Set(review.contexts.map(\.authType))
        let providerHints = Set(review.contexts.compactMap(\.providerHint))

        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.count, 180)
        XCTAssertTrue(authTypes.isSuperset(of: ["Exec plugin", "OIDC", "Token file", "Client certificate"]))
        XCTAssertTrue(providerHints.isSuperset(of: ["DOKS", "Rancher", "OpenShift", "OIDC"]))
        XCTAssertFalse(review.redactedPreview.contains("synthetic-id-token"))
        XCTAssertFalse(review.redactedPreview.contains("synthetic-password"))
        XCTAssertFalse(review.redactedPreview.contains("/synthetic/token-"))
        XCTAssertFalse(review.redactedPreview.contains("/synthetic/client-"))
        #if DEBUG
        let maximumExpandedImportSeconds = 0.12
        #else
        let maximumExpandedImportSeconds = 0.05
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumExpandedImportSeconds,
            "KPI: expanded kubeconfig provider/auth parsing should stay below 120ms in debug and 50ms in release for 180 contexts."
        )
    }

    @MainActor
    func testLoadedKubeConfigSourceReviewBenchmarkKPI() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RunePerformanceBenchmarks.loadedKubeconfigReview.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let sources = try (0..<80).map { fileIndex -> KubeConfigSource in
            let contexts = (0..<4).map { contextIndex in
                """
                - context:
                    cluster: cluster-\(fileIndex)-\(contextIndex)
                    user: user-\(fileIndex)-\(contextIndex)
                    namespace: namespace-\(contextIndex)
                  name: context-\(fileIndex)-\(contextIndex)
                """
            }.joined(separator: "\n")
            let clusters = (0..<4).map { contextIndex in
                """
                - cluster:
                    server: https://cluster-\(fileIndex)-\(contextIndex).example.invalid
                  name: cluster-\(fileIndex)-\(contextIndex)
                """
            }.joined(separator: "\n")
            let users = (0..<4).map { contextIndex in
                """
                - user:
                    token: synthetic-token-\(fileIndex)-\(contextIndex)
                  name: user-\(fileIndex)-\(contextIndex)
                """
            }.joined(separator: "\n")
            let raw = """
            apiVersion: v1
            kind: Config
            current-context: context-\(fileIndex)-0
            clusters:
            \(clusters)
            contexts:
            \(contexts)
            users:
            \(users)
            """
            let url = directory.appendingPathComponent("config-\(fileIndex).yaml")
            try raw.write(to: url, atomically: true, encoding: .utf8)
            return KubeConfigSource(url: url)
        }
        let state = RuneAppState()
        state.setSources(sources)
        let viewModel = RuneAppViewModel(
            state: state,
            kubeConfigImportValidator: KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"])
        )

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = viewModel.reviewLoadedKubeConfigSources()
        }

        let reviews = viewModel.reviewLoadedKubeConfigSources()
        let elapsedSeconds = minimumElapsedSeconds {
            _ = viewModel.reviewLoadedKubeConfigSources()
        }

        XCTAssertEqual(reviews.count, 80)
        XCTAssertEqual(reviews.flatMap(\.contexts).count, 320)
        XCTAssertTrue(reviews.allSatisfy(\.isValid))
        XCTAssertFalse(reviews.contains { $0.redactedPreview.contains("synthetic-token") })
        #if DEBUG
        let maximumLoadedSourceReviewSeconds = 0.18
        #else
        let maximumLoadedSourceReviewSeconds = 0.08
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumLoadedSourceReviewSeconds,
            "KPI: reviewing 80 loaded kubeconfig sources from disk should stay below 180ms in debug and 80ms in release."
        )
    }

    func testCloudKubeConfigCommandPreviewBenchmarkKPI() throws {
        let importer = CloudKubeConfigCLIImporter(
            runner: BenchmarkCloudCommandRunner(),
            discoverer: EmptyKubeConfigDiscoverer()
        )
        let requests = (0..<1_000).map { index in
            CloudKubeConfigImportRequest(
                provider: index.isMultiple(of: 3) ? .aks : (index.isMultiple(of: 2) ? .eks : .gke),
                clusterName: "synthetic-cluster-\(index)",
                regionOrLocation: "eu-north-1",
                resourceGroup: "synthetic-group-\(index)",
                projectID: "synthetic-project-\(index)",
                profileOrSubscription: "synthetic-profile-\(index)",
                roleARN: "arn:aws:iam::000000000000:role/synthetic-\(index)"
            )
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for request in requests {
                _ = try? importer.commandPreview(for: request)
            }
        }

        let elapsedSeconds = try minimumThrowingElapsedSeconds {
            for request in requests {
                _ = try importer.commandPreview(for: request)
            }
        }

        #if DEBUG
        let maximumPreviewSeconds = 0.05
        #else
        let maximumPreviewSeconds = 0.015
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumPreviewSeconds,
            "KPI: cloud provider command previews for 1k requests should stay below 50ms in debug and 15ms in release."
        )
    }

    func testAddClusterAutoDetectAndProviderPreviewBenchmarkKPI() throws {
        let validator = KubeConfigImportValidator(fileExists: { _ in true }, executableSearchPaths: ["/synthetic/bin"])
        let importer = CloudKubeConfigCLIImporter(
            runner: BenchmarkCloudCommandRunner(),
            discoverer: EmptyKubeConfigDiscoverer(),
            validator: validator
        )
        let payloads = (0..<80).map { fileIndex in
            """
            apiVersion: v1
            kind: Config
            current-context: context-\(fileIndex)
            clusters:
            - cluster:
                server: https://cluster-\(fileIndex).example.invalid
              name: cluster-\(fileIndex)
            users:
            - user:
                exec:
                  command: kubelogin
              name: user-\(fileIndex)
            contexts:
            - context:
                cluster: cluster-\(fileIndex)
                user: user-\(fileIndex)
                namespace: namespace-\(fileIndex % 7)
              name: context-\(fileIndex)
            """
        }
        let requests = (0..<240).map { index in
            CloudKubeConfigImportRequest(
                provider: index.isMultiple(of: 3) ? .aks : (index.isMultiple(of: 2) ? .eks : .gke),
                clusterName: "synthetic-cluster-\(index)",
                regionOrLocation: "eu-north-1",
                resourceGroup: "synthetic-group-\(index)",
                projectID: "synthetic-project-\(index)",
                profileOrSubscription: "synthetic-profile-\(index)"
            )
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = payloads.map { validator.validate(raw: $0) }
            for request in requests {
                _ = try? importer.commandPreview(for: request)
            }
        }

        let elapsedSeconds = try minimumThrowingElapsedSeconds {
            let reviews = payloads.map { validator.validate(raw: $0) }
            for request in requests {
                _ = try importer.commandPreview(for: request)
            }
            XCTAssertEqual(reviews.flatMap(\.contexts).count, payloads.count)
            XCTAssertTrue(reviews.allSatisfy(\.isValid))
        }

        #if DEBUG
        let maximumAddClusterSeconds = 0.08
        #else
        let maximumAddClusterSeconds = 0.025
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumAddClusterSeconds,
            "KPI: Add Cluster auto-detect parsing plus provider command previews should stay below 80ms in debug and 25ms in release."
        )
    }

    @MainActor
    func testMockedAddClusterFakeRESTCoreLoadBenchmarkKPI() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }

        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let rawKubeconfig = try String(contentsOf: kubeconfig, encoding: .utf8)
        let review = KubeConfigImportValidator(
            fileExists: { _ in true },
            executableSearchPaths: ["/synthetic/bin"]
        ).validate(raw: rawKubeconfig, sourceName: kubeconfig.lastPathComponent)
        let command = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )
        let importer = BenchmarkCloudKubeConfigImporter(result: CloudKubeConfigImportResult(
            command: command,
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
            discoveredURLs: [kubeconfig],
            reviews: [review]
        ))

        let elapsedSeconds = try await minimumAsyncElapsedSeconds {
            let state = RuneAppState()
            let viewModel = RuneAppViewModel(
                state: state,
                bookmarkManager: BookmarkManager(store: BenchmarkBookmarkStore()),
                kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
                cloudKubeConfigImporter: importer,
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )

            viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
                provider: .eks,
                clusterName: "synthetic-cloud",
                regionOrLocation: "eu-north-1"
            ))

            try await waitUntil {
                viewModel.cloudKubeConfigImportStatus == "Imported EKS kubeconfig context."
                    && state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                    && state.selectedNamespace == "alpha-zone"
                    && state.pods.contains { $0.name == "orbit-lens-6f58d7d89b-hx9q2" }
                    && state.deployments.contains { $0.name == "orbit-lens" }
            }
        }

        #if DEBUG
        let maximumMockedAddClusterSeconds = 0.6
        #else
        let maximumMockedAddClusterSeconds = 0.3
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumMockedAddClusterSeconds,
            "KPI: mocked Add Cluster provider import plus fake REST core load should stay below 600ms in debug and 300ms in release."
        )
    }

    @MainActor
    func testAddClusterDuplicateCloudImportRunGuardBenchmarkKPI() async throws {
        let command = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )
        let importer = BenchmarkBlockingCloudKubeConfigImporter(result: CloudKubeConfigImportResult(
            command: command,
            commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
            discoveredURLs: [],
            reviews: []
        ))
        let state = RuneAppState()
        let viewModel = RuneAppViewModel(
            state: state,
            bookmarkManager: BookmarkManager(store: BenchmarkBookmarkStore()),
            kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
            cloudKubeConfigImporter: importer,
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        let request = CloudKubeConfigImportRequest(
            provider: .eks,
            clusterName: "synthetic-cloud",
            regionOrLocation: "eu-north-1"
        )

        viewModel.runCloudKubeConfigImport(request)
        try await waitUntil {
            importer.hasSuspendedImport && viewModel.isRunningCloudKubeConfigImport
        }

        let elapsedSeconds = minimumElapsedSeconds {
            for _ in 0..<20_000 {
                viewModel.runCloudKubeConfigImport(request)
            }
        }

        importer.resume()
        try await waitUntil {
            !viewModel.isRunningCloudKubeConfigImport
        }

        XCTAssertEqual(importer.importCallCount, 1)
        XCTAssertLessThan(
            elapsedSeconds,
            0.02,
            "KPI: rejecting 20k duplicate Add Cluster cloud imports should stay below 20ms while an import is in flight."
        )
    }

    @MainActor
    func testAddClusterBlockingReviewShortCircuitBenchmarkKPI() async throws {
        let command = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )
        let review = KubeConfigImportReview(
            contexts: [],
            issues: [
                KubeConfigImportIssue(
                    id: "missing-current-context",
                    severity: .error,
                    message: "Synthetic current context is missing."
                )
            ],
            redactedPreview: "apiVersion: v1\ncontexts: []\n"
        )

        let elapsedSeconds = try await minimumAsyncElapsedSeconds {
            let state = RuneAppState()
            let viewModel = RuneAppViewModel(
                state: state,
                bookmarkManager: BookmarkManager(store: BenchmarkBookmarkStore()),
                kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
                cloudKubeConfigImporter: BenchmarkCloudKubeConfigImporter(result: CloudKubeConfigImportResult(
                    command: command,
                    commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
                    discoveredURLs: [],
                    reviews: [review]
                )),
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )

            viewModel.runCloudKubeConfigImport(CloudKubeConfigImportRequest(
                provider: .eks,
                clusterName: "synthetic-cloud",
                regionOrLocation: "eu-north-1"
            ))

            try await waitUntil {
                viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                    && !viewModel.isRunningCloudKubeConfigImport
                    && state.authDoctorChecks.contains { $0.id == "kubeconfig-import-missing-current-context" }
            }
            XCTAssertTrue(state.kubeConfigSources.isEmpty)
            XCTAssertTrue(state.contexts.isEmpty)
            XCTAssertFalse(state.authDoctorChecks.contains { $0.id == "cloud-login-eks" })
        }

        #if DEBUG
        let maximumShortCircuitSeconds = 0.12
        #else
        let maximumShortCircuitSeconds = 0.06
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumShortCircuitSeconds,
            "KPI: Add Cluster blocking import-review short-circuit should stay below 120ms in debug and 60ms in release."
        )
    }

    @MainActor
    func testAddClusterFailureRetryWorkflowBenchmarkKPI() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }

        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }
        let rawKubeconfig = try String(contentsOf: kubeconfig, encoding: .utf8)
        let review = KubeConfigImportValidator(
            fileExists: { _ in true },
            executableSearchPaths: ["/synthetic/bin"]
        ).validate(raw: rawKubeconfig, sourceName: kubeconfig.lastPathComponent)
        let command = CloudKubeConfigCommandPreview(
            executable: "aws",
            arguments: ["eks", "update-kubeconfig", "--region", "eu-north-1", "--name", "synthetic-cloud"],
            displayCommand: "aws eks update-kubeconfig --region eu-north-1 --name synthetic-cloud"
        )

        let elapsedSeconds = try await minimumAsyncElapsedSeconds {
            let importer = BenchmarkSequencedCloudKubeConfigImporter(
                preview: command,
                results: [
                    .failure(.commandFailed(
                        command: command.displayCommand,
                        exitCode: 42,
                        message: "synthetic login required"
                    )),
                    .success(CloudKubeConfigImportResult(
                        command: command,
                        commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "updated\n", stderr: ""),
                        discoveredURLs: [kubeconfig],
                        reviews: [review]
                    ))
                ]
            )
            let state = RuneAppState()
            let viewModel = RuneAppViewModel(
                state: state,
                bookmarkManager: BookmarkManager(store: BenchmarkBookmarkStore()),
                kubeConfigDiscoverer: EmptyKubeConfigDiscoverer(),
                cloudKubeConfigImporter: importer,
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )
            let request = CloudKubeConfigImportRequest(
                provider: .eks,
                clusterName: "synthetic-cloud",
                regionOrLocation: "eu-north-1"
            )

            viewModel.runCloudKubeConfigImport(request)
            try await waitUntil {
                viewModel.cloudKubeConfigImportStatus == "Cloud import failed."
                    && !viewModel.isRunningCloudKubeConfigImport
                    && state.lastError?.contains("Cloud import command failed") == true
            }

            viewModel.runCloudKubeConfigImport(request)
            XCTAssertNil(state.lastError)
            try await waitUntil {
                viewModel.cloudKubeConfigImportStatus == "Imported EKS kubeconfig context."
                    && !viewModel.isRunningCloudKubeConfigImport
                    && state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                    && state.pods.contains { $0.name == "orbit-lens-6f58d7d89b-hx9q2" }
            }
            XCTAssertEqual(importer.importCallCount, 2)
        }

        #if DEBUG
        let maximumRetryWorkflowSeconds = 0.7
        #else
        let maximumRetryWorkflowSeconds = 0.35
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumRetryWorkflowSeconds,
            "KPI: Add Cluster failed cloud import retry plus fake REST core load should stay below 700ms in debug and 350ms in release."
        )
    }

    @MainActor
    func testAddClusterRefreshContextsFakeRESTWorkflowBenchmarkKPI() async throws {
        let previousSimpleMode = UserDefaults.standard.object(forKey: RuneSettingsKeys.simpleMode)
        UserDefaults.standard.runeSimpleMode = true
        defer {
            if let previousSimpleMode {
                UserDefaults.standard.set(previousSimpleMode, forKey: RuneSettingsKeys.simpleMode)
            } else {
                UserDefaults.standard.removeObject(forKey: RuneSettingsKeys.simpleMode)
            }
        }

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }

        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let elapsedSeconds = try await minimumAsyncElapsedSeconds {
            let discoverer = MutableBenchmarkKubeConfigDiscoverer(urls: [kubeconfig])
            let state = RuneAppState()
            let viewModel = RuneAppViewModel(
                state: state,
                bookmarkManager: BookmarkManager(store: BenchmarkBookmarkStore()),
                kubeConfigDiscoverer: discoverer,
                cloudKubeConfigImporter: BenchmarkCloudKubeConfigImporter(result: CloudKubeConfigImportResult(
                    command: CloudKubeConfigCommandPreview(executable: "aws", arguments: [], displayCommand: "aws eks update-kubeconfig"),
                    commandResult: CloudKubeConfigCommandResult(exitCode: 0, stdout: "", stderr: ""),
                    discoveredURLs: [],
                    reviews: []
                )),
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )

            viewModel.refreshKubeConfigSourcesFromDiscovery()

            try await waitUntil {
                state.selectedContext?.name == RuneFakeK8sFixture.defaultContextName
                    && state.selectedNamespace == "alpha-zone"
                    && state.pods.contains { $0.name == "orbit-lens-6f58d7d89b-hx9q2" }
                    && state.deployments.contains { $0.name == "orbit-lens" }
            }
        }

        #if DEBUG
        let maximumRefreshWorkflowSeconds = 0.6
        #else
        let maximumRefreshWorkflowSeconds = 0.3
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumRefreshWorkflowSeconds,
            "KPI: Add Cluster external-CLI Refresh Contexts plus fake REST core load should stay below 600ms in debug and 300ms in release."
        )
    }

    func testAuthDoctorKubeconfigHintInspectionBenchmarkKPI() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-auth-doctor-benchmark-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let sources = try (0..<160).map { index -> KubeConfigSource in
            let providerCommand: String
            let serverHost: String
            switch index % 3 {
            case 0:
                providerCommand = "aws"
                serverHost = "https://synthetic-\(index).eks.amazonaws.com"
            case 1:
                providerCommand = "gke-gcloud-auth-plugin"
                serverHost = "https://container.googleapis.com/synthetic-\(index)"
            default:
                providerCommand = "kubelogin"
                serverHost = "https://synthetic-\(index).aks.example.invalid"
            }

            let raw = """
            apiVersion: v1
            kind: Config
            current-context: synthetic-context-\(index)
            clusters:
            - cluster:
                server: \(serverHost)
                certificate-authority-data: SYNTHETIC_CA_DATA
              name: synthetic-cluster-\(index)
            users:
            - user:
                exec:
                  apiVersion: client.authentication.k8s.io/v1
                  command: \(providerCommand)
                  args:
                  - get-token
                  - --synthetic-index=\(index)
              name: synthetic-user-\(index)
            contexts:
            - context:
                cluster: synthetic-cluster-\(index)
                user: synthetic-user-\(index)
                namespace: synthetic-namespace-\(index % 9)
              name: synthetic-context-\(index)
            """
            let url = directory.appendingPathComponent("config-\(index).yaml")
            try raw.write(to: url, atomically: true, encoding: .utf8)
            return KubeConfigSource(url: url)
        }

        let availableTools = Set(["aws", "az", "gcloud", "gke-gcloud-auth-plugin", "kubelogin"].map { "/synthetic/bin/" + $0 })
        let inspector = AuthDoctorKubeconfigInspector(
            fileExists: { availableTools.contains($0) },
            executableSearchPaths: ["/synthetic/bin"]
        )

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = inspector.inspect(sources: sources)
        }

        let checks = inspector.inspect(sources: sources)
        let elapsedSeconds = minimumElapsedSeconds {
            _ = inspector.inspect(sources: sources)
        }

        XCTAssertTrue(checks.contains { $0.id == "exec-auth-profile" })
        XCTAssertTrue(checks.contains { $0.id == "exec-auth-tools" && $0.status == .passed })
        XCTAssertTrue(checks.contains { $0.id == "cloud-login-tools" && $0.status == .passed })
        XCTAssertFalse(checks.map(\.message).joined(separator: "\n").contains(directory.path))
        #if DEBUG
        let maximumAuthDoctorInspectionSeconds = 0.12
        #else
        let maximumAuthDoctorInspectionSeconds = 0.04
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumAuthDoctorInspectionSeconds,
            "KPI: Auth Doctor local kubeconfig hint inspection for 160 exec/cloud profiles should stay below 120ms in debug and 40ms in release."
        )
    }

    func testAuthDoctorFailureProjectionBenchmarkKPI() {
        let samples = [
            "Command failed: kubeconfig exec auth provider: Timed out after 25 seconds",
            "Kubeconfig exec auth response is not a valid ExecCredential JSON document: not-json",
            "Kubeconfig exec auth returned apiVersion client.authentication.k8s.io/v1beta1, expected client.authentication.k8s.io/v1",
            "Kubeconfig exec auth response is missing status",
            "Kubeconfig exec auth returned incomplete client certificate credentials",
            "Command failed: kubeconfig exec auth provider: executable file not found",
            "HTTP status 401 Unauthorized: invalid bearer token",
            "Client certificate and key in kubeconfig could not be paired into a TLS identity: OSStatus -25300",
            "pods is forbidden: User cannot list resource pods in namespace default",
            "TLS handshake failed: x509 certificate signed by unknown authority",
            "Proxy CONNECT tunnel failed with HTTP 407",
            "DNS resolution timed out while connecting to the Kubernetes API",
            "synthetic unclassified parser failure"
        ]
        let messages = (0..<2_600).map { samples[$0 % samples.count] }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for message in messages {
                _ = AuthDoctorFailureProjector.checks(for: message)
            }
        }

        let projected = messages.reduce(into: 0) { count, message in
            count += AuthDoctorFailureProjector.checks(for: message).count
        }

        XCTAssertEqual(projected, 2_400)

        var measuredProjection = 0
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            measuredProjection = messages.reduce(into: 0) { count, message in
                count += AuthDoctorFailureProjector.checks(for: message).count
            }
        }

        XCTAssertEqual(measuredProjection, projected)
        #if DEBUG
        let maximumFailureProjectionSeconds = 0.10
        #else
        let maximumFailureProjectionSeconds = 0.04
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumFailureProjectionSeconds,
            "KPI: Auth Doctor failure projection should classify 2.6k mixed failure messages below 100ms in debug and 40ms in release."
        )
    }

    func testAuthDoctorOversizedFailureProjectionBenchmarkKPI() {
        let suffix = String(repeating: " noisy-stderr-chunk", count: 4_000)
            + " https://cluster.example.invalid/api/v1/namespaces/team-a/pods/api-0/log?token=secret-token"
        let messages = (0..<250).map { index in
            index.isMultiple(of: 2)
                ? "TLS handshake failed: x509 certificate signed by unknown authority \(suffix)"
                : "synthetic unclassified parser failure \(suffix)"
        }

        let started = ContinuousClock.now
        let projected = messages.reduce(into: 0) { count, message in
            count += AuthDoctorFailureProjector.checks(for: message).count
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(projected, 125)
        XCTAssertLessThan(
            seconds(elapsed),
            0.08,
            "KPI: Auth Doctor should bound oversized external failure messages before classification and endpoint scanning."
        )
    }

    func testAuthDoctorRBACPreflightRunnerBenchmarkKPI() async {
        let targets = AuthDoctorRBACPreflightTarget.emptyViewTargets

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            let expectation = expectation(description: "Auth Doctor RBAC preflight benchmark")
            Task {
                for _ in 0..<200 {
                    let results = await AuthDoctorRBACPreflightRunner.run(
                        targets: targets,
                        activeNamespace: "synthetic",
                        maxConcurrentChecks: 4
                    ) { target, namespace in
                        target.id.count + (namespace?.count ?? 0) > 0
                    }
                    XCTAssertEqual(results.count, targets.count)
                }
                expectation.fulfill()
            }
            wait(for: [expectation], timeout: 2.0)
        }

        let started = ContinuousClock.now
        for _ in 0..<200 {
            let results = await AuthDoctorRBACPreflightRunner.run(
                targets: targets,
                activeNamespace: "synthetic",
                maxConcurrentChecks: 4
            ) { target, namespace in
                target.id.count + (namespace?.count ?? 0) > 0
            }
            XCTAssertEqual(results.count, targets.count)
        }
        let elapsed = started.duration(to: .now)

        XCTAssertLessThan(
            seconds(elapsed),
            0.50,
            "KPI: Auth Doctor RBAC preflight scheduling should run 200 synthetic target sweeps below 500ms in debug."
        )
    }

    @MainActor
    func testSupportBundleSanitizerBenchmarkKPI() throws {
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "benchmark-sensitive-context")
        state.selectedNamespace = "benchmark-namespace"
        let yaml = (0..<2_000)
            .map { index in
                index.isMultiple(of: 40)
                    ? "token: synthetic-secret-\(index)\npath: /synthetic/home/user/config-\(index).yaml"
                    : "key\(index): value-\(index)"
            }
            .joined(separator: "\n")
        let logs = (0..<5_000)
            .map { index in
                index.isMultiple(of: 25)
                    ? "Authorization: Bearer synthetic-bearer-\(index) from /synthetic/home/user/log-\(index)"
                    : "line=\(index) context=benchmark-sensitive-context ok"
            }
            .joined(separator: "\n")
        state.setResourceYAML(yaml)
        state.setResourceDescribe("Loaded benchmark-sensitive-context from /synthetic/home/user/.kube/config")
        state.setPodLogs(logs)
        state.setUnifiedServiceLogs(logs, pods: ["api-0", "api-1"])
        state.setDeploymentRolloutHistory(yaml)
        state.setAuthDoctorChecks([
            RuneHealthCheck(
                id: "kubeconfig",
                title: "Kubeconfig",
                status: .warning,
                message: "benchmark-sensitive-context uses token synthetic-health-token from /synthetic/home/user/.kube/config"
            )
        ])

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = SupportBundleRequest.snapshot(
                state: state,
                generatedAt: "2026-05-13T00:00:00Z",
                resourceCounts: ["pods": 2],
                selectedResourceKind: "Pod",
                selectedResourceName: "api-0"
            )
        }

        let builder = JSONSupportBundleBuilder()
        var builtData: Data?
        let elapsedSeconds = try minimumThrowingElapsedSeconds(repetitions: 5) {
            let request = SupportBundleRequest.snapshot(
                state: state,
                generatedAt: "2026-05-13T00:00:00Z",
                resourceCounts: ["pods": 2],
                selectedResourceKind: "Pod",
                selectedResourceName: "api-0"
            )
            builtData = try builder.buildBundle(from: request)
        }
        let data = try XCTUnwrap(builtData)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))

        XCTAssertFalse(json.contains("synthetic-secret"))
        XCTAssertFalse(json.contains("synthetic-bearer"))
        XCTAssertFalse(json.contains("synthetic-health-token"))
        XCTAssertFalse(json.contains("benchmark-sensitive-context"))
        XCTAssertFalse(json.contains("/synthetic/home/user"))
        XCTAssertLessThan(
            elapsedSeconds,
            0.3,
            "KPI: sanitized support bundle snapshot plus JSON encode should stay below 300ms for large YAML/log payloads in debug."
        )
    }

    func testThemeCatalogImportBenchmarkKPI() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneThemeImportBenchmark-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        for index in 0..<80 {
            let darkAccent = index.isMultiple(of: 2) ? "#67b7ffff" : "#b891ffff"
            let lightAccent = index.isMultiple(of: 2) ? "#1f73caff" : "#006d8fff"
            let json = """
            {
              "name": "Synthetic Theme Family \(index)",
              "themes": [
                \(benchmarkThemeJSON(name: "Dark \(index)", appearance: "dark", accent: darkAccent)),
                \(benchmarkThemeJSON(name: "Light \(index)", appearance: "light", accent: lightAccent))
              ]
            }
            """
            try json.write(
                to: directory.appendingPathComponent("theme-\(String(format: "%03d", index)).json"),
                atomically: true,
                encoding: .utf8
            )
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = RuneThemeCatalog.loadZedThemes(from: directory)
        }

        let themes = RuneThemeCatalog.loadZedThemes(from: directory)
        let elapsedSeconds = try minimumThrowingElapsedSeconds {
            _ = RuneThemeCatalog.loadZedThemes(from: directory)
        }

        XCTAssertEqual(themes.count, 160)
        XCTAssertEqual(themes.first?.id, "zed:theme-000:dark-0")
        XCTAssertEqual(themes.last?.id, "zed:theme-079:light-79")
        XCTAssertEqual(themes.first?.sourceSummary, "Custom theme")
        #if DEBUG
        let maximumSeconds = 1.0
        #else
        let maximumSeconds = 0.35
        #endif
        XCTAssertLessThan(
            elapsedSeconds,
            maximumSeconds,
            "KPI: importing 160 synthetic custom themes should stay below \(maximumSeconds)s."
        )
    }

    @MainActor
    func testThemePresentationProjectionBenchmarkKPI() {
        let themes = RuneAppearanceTheme.allCases.map(\.resolvedTheme)
        let iterations = 4_000

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for _ in 0..<iterations {
                for theme in themes {
                    _ = RuneThemePresentation(theme: theme)
                }
            }
        }

        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            for _ in 0..<iterations {
                for theme in themes {
                    let presentation = RuneThemePresentation(theme: theme)
                    _ = presentation.title
                    _ = presentation.menuSymbol
                    _ = presentation.palette.accent
                }
            }
        }

        XCTAssertLessThan(
            elapsedSeconds,
            0.20,
            "KPI: theme presentation projection should stay below 200ms for repeated settings/menu refreshes in debug."
        )
    }

    func testThemeCatalogCachedUserThemesLookupBenchmarkKPI() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneThemeCachedLookupBenchmark-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            RuneThemeCatalog.reloadUserThemes()
            try? FileManager.default.removeItem(at: directory)
        }

        for index in 0..<40 {
            let json = """
            {
              "name": "Synthetic Theme Family \(index)",
              "themes": [
                \(benchmarkThemeJSON(name: "Cached Dark \(index)", appearance: "dark", accent: "#67b7ffff")),
                \(benchmarkThemeJSON(name: "Cached Light \(index)", appearance: "light", accent: "#1f73caff"))
              ]
            }
            """
            try json.write(
                to: directory.appendingPathComponent("cached-theme-\(String(format: "%03d", index)).json"),
                atomically: true,
                encoding: .utf8
            )
        }

        RuneThemeCatalog.reloadUserThemes()
        let referenceDate = Date(timeIntervalSince1970: 10_000)
        let themes = RuneThemeCatalog.userThemes(from: directory, referenceDate: referenceDate)

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for offset in 0..<1_000 {
                _ = RuneThemeCatalog.userThemes(
                    from: directory,
                    referenceDate: referenceDate.addingTimeInterval(Double(offset) * 0.0001)
                )
            }
        }

        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            for offset in 0..<1_000 {
                _ = RuneThemeCatalog.userThemes(
                    from: directory,
                    referenceDate: referenceDate.addingTimeInterval(Double(offset) * 0.0001)
                )
            }
        }

        XCTAssertEqual(themes.count, 80)
        XCTAssertLessThan(
            elapsedSeconds,
            0.05,
            "KPI: cached custom theme lookup should stay below 50ms for 1k settings refresh reads."
        )
    }

    private func benchmarkThemeJSON(name: String, appearance: String, accent: String) -> String {
        """
        {
          "name": "\(name)",
          "appearance": "\(appearance)",
          "style": {
            "background": "\(appearance == "dark" ? "#101820ff" : "#fbfcffff")",
            "panel.background": "\(appearance == "dark" ? "#182431ff" : "#f1f5faff")",
            "surface.background": "\(appearance == "dark" ? "#1e2c3aff" : "#eef3f8ff")",
            "element.background": "\(appearance == "dark" ? "#223244ff" : "#e4ebf5ff")",
            "element.selected": "\(appearance == "dark" ? "#294866ff" : "#c8e4ffff")",
            "border": "\(appearance == "dark" ? "#40576eff" : "#91a2b5ff")",
            "border.variant": "\(appearance == "dark" ? "#34485cff" : "#a5b2c2ff")",
            "text": "\(appearance == "dark" ? "#f4f8fbff" : "#07111fff")",
            "text.muted": "\(appearance == "dark" ? "#b6c4d1ff" : "#26394fff")",
            "text.placeholder": "\(appearance == "dark" ? "#8ea0adff" : "#52677dff")",
            "text.accent": "\(accent)",
            "editor.background": "\(appearance == "dark" ? "#0d141cff" : "#ffffffff")",
            "success": "\(appearance == "dark" ? "#7ee6a8ff" : "#006d3bff")",
            "warning": "\(appearance == "dark" ? "#ffd166ff" : "#8a5a00ff")",
            "error": "\(appearance == "dark" ? "#ff7a9aff" : "#b00020ff")",
            "info": "#8bd3ffff",
            "syntax": {
              "property": { "color": "#8bd3ffff" },
              "string": { "color": "\(appearance == "dark" ? "#7ee6a8ff" : "#006d3bff")" },
              "number": { "color": "#ffd166ff" },
              "boolean": { "color": "\(accent)" },
              "comment": { "color": "\(appearance == "dark" ? "#8ea0adff" : "#52677dff")" },
              "keyword": { "color": "\(accent)" },
              "type": { "color": "#80e6d6ff" }
            }
          }
        }
        """
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

        for index in 0..<1_000 {
            state.appendTerminalSessionOutput(id: "shell", text: "line \(index)\n")
        }
        let elapsedSeconds = minimumElapsedSeconds {
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

        XCTAssertEqual(state.terminalSessions.count, 1)
        XCTAssertTrue(state.terminalSession?.transcript.contains("line 999") == true)
        #if DEBUG
        let maximumAppendSeconds = 0.35
        #else
        let maximumAppendSeconds = 0.20
        #endif
        XCTAssertLessThan(elapsedSeconds, maximumAppendSeconds)
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

        let sanitized = TerminalTranscriptSanitizer.sanitize(chunk)
        let elapsedSeconds = minimumElapsedSeconds {
            _ = TerminalTranscriptSanitizer.sanitize(chunk)
        }

        XCTAssertTrue(sanitized.contains("pulling layer 10000%"))
        XCTAssertFalse(sanitized.contains("\u{001B}"))
        XCTAssertLessThan(elapsedSeconds, 0.12)
    }

    func testTerminalScrollbackRetentionBenchmarkKPI() {
        let transcript = (0..<80_000)
            .map { "line \($0) payload=synthetic-terminal-output" }
            .joined(separator: "\n")

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            _ = TerminalScrollbackRetention.retainingRecentLines(transcript, maxLines: 60_000)
        }

        let retained = TerminalScrollbackRetention.retainingRecentLines(transcript, maxLines: 60_000)
        let elapsedSeconds = minimumElapsedSeconds {
            _ = TerminalScrollbackRetention.retainingRecentLines(transcript, maxLines: 60_000)
        }

        XCTAssertTrue(retained.hasPrefix(TerminalScrollbackRetention.truncationMarker))
        XCTAssertFalse(retained.contains("line 0 payload"))
        XCTAssertTrue(retained.contains("line 79999 payload"))
        XCTAssertLessThan(elapsedSeconds, 0.20)
    }

    func testTerminalTranscriptSearchBenchmarkKPI() throws {
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

        var measuredIndex: TerminalTranscriptSearchIndex?
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            measuredIndex = TerminalTranscriptSearchIndex(text: transcript, query: "status=error", matchCase: false)
        }
        let index = try XCTUnwrap(measuredIndex)

        XCTAssertEqual(index.ranges.count, 12_000)
        XCTAssertEqual(index.matchLineNumber(selectedIndex: 11_999), 59_996)
        XCTAssertEqual(index.statusText(selectedIndex: 0), "1 of 12000")
        XCTAssertLessThan(elapsedSeconds, 0.45)
    }

    func testTerminalTranscriptAppendWhileSearchOpenBenchmarkKPI() throws {
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

        var measuredModel: TerminalTranscriptRenderModel?
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            measuredModel = TerminalTranscriptRenderModel(
                text: transcript,
                query: "status=error",
                matchCase: false,
                usesLargeTextSurface: true
            )
        }
        let model = try XCTUnwrap(measuredModel)

        XCTAssertEqual(model.searchIndex.ranges.count, 5_167)
        XCTAssertEqual(model.scrollTargetLine(selectedIndex: 5_166), 30_997)
        XCTAssertLessThan(elapsedSeconds, 0.12)
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

        let elapsed = minimumElapsedSeconds(repetitions: 5) {
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

        #if DEBUG
        let maximumInitialMountSeconds = 0.35
        #else
        let maximumInitialMountSeconds = 0.12
        #endif
        XCTAssertLessThan(elapsed, maximumInitialMountSeconds)
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
                    onSaveAllTranscripts: {},
                    onSaveActiveTranscriptToExportFolder: {},
                    onSaveActiveTranscriptAndOpen: {},
                    onSaveAllTranscriptsToExportFolder: {},
                    onSaveAllTranscriptsAndOpen: {},
                    isFavoritePod: { _ in false },
                    onToggleFavoritePod: { _ in }
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
                onSaveAllTranscripts: {},
                onSaveActiveTranscriptToExportFolder: {},
                onSaveActiveTranscriptAndOpen: {},
                onSaveAllTranscriptsToExportFolder: {},
                onSaveAllTranscriptsAndOpen: {},
                isFavoritePod: { _ in false },
                onToggleFavoritePod: { _ in }
            )
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(sessions.count, 64)
        XCTAssertEqual(pods.count, 500)
        XCTAssertLessThan(seconds(elapsed), 0.10)
    }

    func testTerminalSessionTabPresentationBenchmarkKPI() {
        let sessions = (0..<5_000).map { index in
            PodTerminalSession(
                id: "shell-\(index)",
                contextName: "benchmark",
                namespace: "default",
                podName: "pod-\(String(format: "%04d", index / 3))",
                containerName: index.isMultiple(of: 3) ? nil : "container-\(index % 3)",
                shell: "sh",
                status: index.isMultiple(of: 11) ? .failed : .connected,
                lastExitCode: index.isMultiple(of: 11) ? 137 : nil
            )
        }

        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()]) {
            for (index, session) in sessions.enumerated() {
                _ = TerminalSessionTabPresentation.make(session: session, number: index + 1)
            }
        }

        let started = ContinuousClock.now
        let presentations = sessions.enumerated().map { index, session in
            TerminalSessionTabPresentation.make(session: session, number: index + 1)
        }
        let elapsed = started.duration(to: .now)

        XCTAssertEqual(presentations.count, 5_000)
        XCTAssertEqual(presentations[1].secondaryTitle, "container-1")
        XCTAssertTrue(presentations[11].helpText.contains("Last exit code: 137"))
        XCTAssertLessThan(
            seconds(elapsed),
            0.05,
            "KPI: terminal tab presentation for 5k sessions should stay below 50ms in debug."
        )
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
        let entries = try ZipArchiveTestSupport.entries(from: data)

        XCTAssertNotNil(entries["terminal-transcripts/session-1-default-pod-00-20260508T100000Z.log"])
        XCTAssertTrue(entries.values.contains { entry in
            String(decoding: entry, as: UTF8.self).contains("session=7 line=2499 status=ok")
        })
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
    func testUnifiedLogSelectedPodScopeBenchmarkKPI() throws {
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

        var measuredResult: (mergedText: String, podNames: [String])?
        let elapsedSeconds = minimumElapsedSeconds(repetitions: 5) {
            measuredResult = RuneAppViewModel.scopedUnifiedLogResult(
                mergedText: text,
                podNames: podNames,
                selectedPodNames: selected
            )
        }
        let result = try XCTUnwrap(measuredResult)

        XCTAssertEqual(result.podNames.count, 12)
        XCTAssertTrue(result.mergedText.contains("[pod-0]"))
        XCTAssertFalse(result.mergedText.contains("[pod-12]"))
        XCTAssertLessThan(elapsedSeconds, 0.08)
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

    private func makeBenchmarkTemporaryDirectory(prefix: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("\(prefix)-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    private func prefillBenchmarkExportCollisions(
        in folderURL: URL,
        baseName: String,
        ext: String,
        count: Int
    ) throws {
        guard count > 0 else { return }
        for index in 0..<count {
            let name = index == 0 ? "\(baseName).\(ext)" : "\(baseName)-\(index + 1).\(ext)"
            try Data("existing".utf8).write(to: folderURL.appendingPathComponent(name))
        }
    }

    @MainActor
    private func runConfiguredFolderSaveAndOpenBatch(
        payload: Data,
        count: Int
    ) throws -> (savedCount: Int, openedCount: Int, deferredScopeStopCount: Int) {
        let folderURL = try makeBenchmarkTemporaryDirectory(prefix: "rune-configured-export-open-kpi")
        defer { try? FileManager.default.removeItem(at: folderURL) }
        let opener = BenchmarkConfiguredExportFileOpener()
        let securityScope = BenchmarkSecurityScopedResourceAccess(startsAccessing: true)
        let exporter = ConfiguredFolderExporter(
            resolver: BenchmarkConfiguredExportDestinationResolver(
                folderURL: folderURL,
                textOpenerBundleIdentifier: "com.example.TextViewer"
            ),
            opener: opener,
            securityScopedAccess: securityScope
        )

        var savedCount = 0
        for index in 0..<count {
            let url = try exporter.save(
                data: payload,
                suggestedName: "manifest-\(index).yaml",
                allowedFileTypes: ["yaml"],
                kind: .plainText,
                openAfterSave: true
            )
            if FileManager.default.fileExists(atPath: url.path) {
                savedCount += 1
            }
        }

        return (
            savedCount: savedCount,
            openedCount: opener.openedCount,
            deferredScopeStopCount: securityScope.deferredStopCount
        )
    }

    @MainActor
    private func makeLogToolbarController(
        width: CGFloat,
        searchSummary: ResourceLogSearchResult,
        podOptions: [PodSummary] = [],
        selectedPodID: String = "",
        presentationStyle: ResourceLogsPresentationStyle = .regular,
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
                searchMatchCase: .constant(false),
                selectedSearchMatchIndex: .constant(2),
                searchPulseID: 0,
                searchSummary: searchSummary,
                statusText: "Last updated 12:00:00",
                podOptions: podOptions,
                selectedPodID: podOptions.isEmpty ? nil : .constant(selectedPodID),
                presentationStyle: presentationStyle,
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
                        ManifestStatusChip(text: "Last updated 12:00:00", systemImage: "clock")
                    }
                }
            }
            .frame(width: width, alignment: .leading)
        )
        controller.view.frame = NSRect(x: 0, y: 0, width: width, height: 160)
        return controller
    }
}

private struct BenchmarkConfiguredExportDestinationResolver: ExportDestinationResolving {
    let folderURL: URL?
    var textOpenerBundleIdentifier: String?
    var archiveOpenerBundleIdentifier: String?
    var usesPrivacySafeFilenames = false

    func exportFolderURL() throws -> URL? {
        folderURL
    }

    func preferredOpenerBundleIdentifier(for kind: ConfiguredExportFileKind) -> String? {
        switch kind {
        case .plainText:
            return textOpenerBundleIdentifier
        case .archive:
            return archiveOpenerBundleIdentifier
        }
    }
}

@MainActor
private final class BenchmarkConfiguredExportFileOpener: ExportFileOpening {
    private(set) var openedCount = 0

    func open(_ url: URL, preferredApplicationBundleIdentifier: String?) throws {
        openedCount += 1
    }
}

@MainActor
private final class BenchmarkSecurityScopedResourceAccess: SecurityScopedResourceAccessing {
    let startsAccessing: Bool
    private(set) var stopCount = 0
    private(set) var deferredStopCount = 0

    init(startsAccessing: Bool) {
        self.startsAccessing = startsAccessing
    }

    func startAccessing(_ url: URL) -> Bool {
        startsAccessing
    }

    func stopAccessing(_ url: URL) {
        stopCount += 1
    }

    func stopAccessingAfterOpenHandoff(_ url: URL) {
        deferredStopCount += 1
    }
}

private struct EmptyKubeConfigDiscoverer: KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL] {
        []
    }
}

private final class MutableBenchmarkKubeConfigDiscoverer: KubeConfigDiscovering, @unchecked Sendable {
    var urls: [URL]

    init(urls: [URL]) {
        self.urls = urls
    }

    func discoverCandidateFiles() -> [URL] {
        urls
    }
}

private struct BenchmarkCloudCommandRunner: CloudKubeConfigCommandRunning {
    func run(_ command: CloudKubeConfigCommandPreview, timeout: TimeInterval) async throws -> CloudKubeConfigCommandResult {
        CloudKubeConfigCommandResult(exitCode: 0, stdout: "", stderr: "")
    }
}

private struct BenchmarkCloudKubeConfigImporter: CloudKubeConfigImporting {
    let result: CloudKubeConfigImportResult

    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        result.command
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        result
    }
}

private final class BenchmarkSequencedCloudKubeConfigImporter: CloudKubeConfigImporting, @unchecked Sendable {
    private let lock = NSLock()
    private let preview: CloudKubeConfigCommandPreview
    private var results: [Result<CloudKubeConfigImportResult, CloudKubeConfigImportError>]
    private var storedImportCallCount = 0

    var importCallCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return storedImportCallCount
    }

    init(
        preview: CloudKubeConfigCommandPreview,
        results: [Result<CloudKubeConfigImportResult, CloudKubeConfigImportError>]
    ) {
        self.preview = preview
        self.results = results
    }

    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        preview
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        let result = lock.withLock {
            storedImportCallCount += 1
            guard !results.isEmpty else {
                return Result<CloudKubeConfigImportResult, CloudKubeConfigImportError>.failure(.missingRequiredField("Result"))
            }
            return results.removeFirst()
        }
        return try result.get()
    }
}

private final class BenchmarkBlockingCloudKubeConfigImporter: CloudKubeConfigImporting, @unchecked Sendable {
    private let lock = NSLock()
    private let result: CloudKubeConfigImportResult
    private var continuation: CheckedContinuation<CloudKubeConfigImportResult, Error>?
    private var storedImportCallCount = 0

    var importCallCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return storedImportCallCount
    }

    var hasSuspendedImport: Bool {
        lock.lock()
        defer { lock.unlock() }
        return continuation != nil
    }

    init(result: CloudKubeConfigImportResult) {
        self.result = result
    }

    func commandPreview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        result.command
    }

    func importCluster(_ request: CloudKubeConfigImportRequest) async throws -> CloudKubeConfigImportResult {
        lock.withLock {
            storedImportCallCount += 1
        }

        return try await withCheckedThrowingContinuation { continuation in
            lock.withLock {
                self.continuation = continuation
            }
        }
    }

    func resume() {
        lock.lock()
        let continuation = continuation
        self.continuation = nil
        lock.unlock()
        continuation?.resume(returning: result)
    }
}

private final class BenchmarkBookmarkStore: BookmarkStore, @unchecked Sendable {
    func loadRecords() throws -> [BookmarkRecord] {
        []
    }

    func saveRecords(_ records: [BookmarkRecord]) throws {}
}

private final class CountingKubeConfigDiscoverer: KubeConfigDiscovering, @unchecked Sendable {
    private(set) var callCount = 0

    func discoverCandidateFiles() -> [URL] {
        callCount += 1
        return []
    }
}

private final class BenchmarkTableDataSource: NSObject, NSTableViewDataSource, NSTableViewDelegate {
    let rowCount: Int

    init(rowCount: Int) {
        self.rowCount = rowCount
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        rowCount
    }

    func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? {
        NSTableRowView()
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        NSTextField(labelWithString: "row-\(row)")
    }
}

@MainActor
private final class ResizeNotificationProbe: NSObject {
    private(set) var unsuppressedNotificationCount = 0

    @objc func tableColumnDidResize(_ notification: Notification) {
        if !isSuppressedSynchronizedResourceColumnResize(notification) {
            unsuppressedNotificationCount += 1
        }
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

@MainActor
private func findManifestTextScrollView(in view: NSView) -> ManifestTextScrollView? {
    if let scrollView = view as? ManifestTextScrollView {
        return scrollView
    }
    for subview in view.subviews {
        if let match = findManifestTextScrollView(in: subview) {
            return match
        }
    }
    return nil
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
