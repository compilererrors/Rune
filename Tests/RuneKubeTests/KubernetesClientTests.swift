import Foundation
import XCTest
@testable import RuneCore
@testable import RuneKube

final class KubernetesClientTests: XCTestCase {
    func testRESTRequestCoalescerSharesIdenticalInFlightReads() async throws {
        let coalescer = KubernetesRESTRequestCoalescer()
        let counter = RESTRequestCoalescerCounter()
        let key = KubernetesRESTRequestCoalescingKey(
            method: "GET",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            scopeIdentity: "synthetic-config",
            credentialFingerprint: Data([0x01]),
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"],
            timeout: 5
        )

        async let first = coalescer.value(for: key) {
            let count = await counter.increment()
            try await Task.sleep(nanoseconds: 30_000_000)
            return RESTResponse(body: "body-\(count)", contentType: "application/json")
        }
        async let second = coalescer.value(for: key) {
            let count = await counter.increment()
            try await Task.sleep(nanoseconds: 30_000_000)
            return RESTResponse(body: "body-\(count)", contentType: "application/json")
        }

        let responses = try await [first, second]
        let operationCount = await counter.currentValue()

        XCTAssertEqual(responses.map(\.body), ["body-1", "body-1"])
        XCTAssertEqual(operationCount, 1)
    }

    func testRESTRequestCoalescerSeparatesReusedContextsByScopeIdentity() async throws {
        let coalescer = KubernetesRESTRequestCoalescer()
        let counter = RESTRequestCoalescerCounter()
        let firstKey = KubernetesRESTRequestCoalescingKey(
            method: "GET",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            scopeIdentity: "synthetic-config-a",
            credentialFingerprint: Data([0x01]),
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"],
            timeout: 5
        )
        let secondKey = KubernetesRESTRequestCoalescingKey(
            method: "GET",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            scopeIdentity: "synthetic-config-b",
            credentialFingerprint: Data([0x01]),
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"],
            timeout: 5
        )

        async let first = coalescer.value(for: firstKey) {
            _ = await counter.increment()
            try await Task.sleep(nanoseconds: 30_000_000)
            return RESTResponse(body: "config-a", contentType: "application/json")
        }
        async let second = coalescer.value(for: secondKey) {
            _ = await counter.increment()
            try await Task.sleep(nanoseconds: 30_000_000)
            return RESTResponse(body: "config-b", contentType: "application/json")
        }

        let responses = try await [first, second]
        let operationCount = await counter.currentValue()

        XCTAssertEqual(Set(responses.map(\.body)), Set(["config-a", "config-b"]))
        XCTAssertEqual(operationCount, 2)
    }

    func testRESTRequestCoalescerSeparatesRotatedCredentialFingerprints() async throws {
        let coalescer = KubernetesRESTRequestCoalescer()
        let counter = RESTRequestCoalescerCounter()
        let firstKey = KubernetesRESTRequestCoalescingKey(
            method: "GET",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            scopeIdentity: "synthetic-config",
            credentialFingerprint: Data([0x01]),
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"],
            timeout: 5
        )
        let secondKey = KubernetesRESTRequestCoalescingKey(
            method: "GET",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            scopeIdentity: "synthetic-config",
            credentialFingerprint: Data([0x02]),
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"],
            timeout: 5
        )

        async let first = coalescer.value(for: firstKey) {
            _ = await counter.increment()
            try await Task.sleep(nanoseconds: 30_000_000)
            return RESTResponse(body: "credential-a", contentType: "application/json")
        }
        async let second = coalescer.value(for: secondKey) {
            _ = await counter.increment()
            try await Task.sleep(nanoseconds: 30_000_000)
            return RESTResponse(body: "credential-b", contentType: "application/json")
        }

        let responses = try await [first, second]
        let operationCount = await counter.currentValue()
        XCTAssertEqual(Set(responses.map(\.body)), Set(["credential-a", "credential-b"]))
        XCTAssertEqual(operationCount, 2)
    }

    func testRESTRequestCoalescerSeparatesDifferentTimeouts() async throws {
        let coalescer = KubernetesRESTRequestCoalescer()
        let counter = RESTRequestCoalescerCounter()
        let firstKey = KubernetesRESTRequestCoalescingKey(
            method: "HEAD",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            scopeIdentity: "synthetic-config",
            credentialFingerprint: Data([0x01]),
            apiPath: "/readyz",
            headers: [:],
            timeout: 1
        )
        let secondKey = KubernetesRESTRequestCoalescingKey(
            method: "HEAD",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            scopeIdentity: "synthetic-config",
            credentialFingerprint: Data([0x01]),
            apiPath: "/readyz",
            headers: [:],
            timeout: 5
        )

        async let first = coalescer.value(for: firstKey) {
            _ = await counter.increment()
            try await Task.sleep(nanoseconds: 30_000_000)
            return RESTResponse(body: "timeout-1", contentType: "")
        }
        async let second = coalescer.value(for: secondKey) {
            _ = await counter.increment()
            try await Task.sleep(nanoseconds: 30_000_000)
            return RESTResponse(body: "timeout-5", contentType: "")
        }

        let responses = try await [first, second]
        let operationCount = await counter.currentValue()
        XCTAssertEqual(Set(responses.map(\.body)), Set(["timeout-1", "timeout-5"]))
        XCTAssertEqual(operationCount, 2)
    }

    func testRESTCredentialFingerprintIsOpaqueStableAndRotationSensitive() {
        let token = "synthetic-token-alpha"
        let first = KubernetesRESTClient._testRESTCredentialFingerprint(bearerToken: token)
        let repeated = KubernetesRESTClient._testRESTCredentialFingerprint(bearerToken: token)
        let rotated = KubernetesRESTClient._testRESTCredentialFingerprint(
            bearerToken: "synthetic-token-beta"
        )

        XCTAssertEqual(first.count, 32)
        XCTAssertEqual(first, repeated)
        XCTAssertNotEqual(first, rotated)
        XCTAssertNotEqual(first, Data(token.utf8))
        XCTAssertFalse(String(decoding: first, as: UTF8.self).contains(token))
    }

    func testRESTRequestCoalescerCancelsUnderlyingReadWhenLastWaiterCancels() async throws {
        let coalescer = KubernetesRESTRequestCoalescer()
        let probe = RESTRequestCoalescerCancellationProbe()
        let key = KubernetesRESTRequestCoalescingKey(
            method: "GET",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            scopeIdentity: "synthetic-config",
            credentialFingerprint: Data([0x01]),
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"],
            timeout: 5
        )

        let task = Task {
            try await coalescer.value(for: key) {
                await probe.markStarted()
                do {
                    try await Task.sleep(nanoseconds: 60_000_000_000)
                    return RESTResponse(body: "late", contentType: "application/json")
                } catch {
                    await probe.markCancelled()
                    throw error
                }
            }
        }

        for _ in 0..<100 where await !probe.started {
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        let didStart = await probe.started
        XCTAssertTrue(didStart)

        task.cancel()
        do {
            _ = try await task.value
            XCTFail("Expected cancelled coalesced read to throw")
        } catch is CancellationError {
        } catch {
            XCTFail("Expected CancellationError, got \(error)")
        }

        for _ in 0..<100 where await !probe.cancelled {
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        let didCancel = await probe.cancelled
        XCTAssertTrue(didCancel)
    }

    func testRESTRequestCoalescerKeepsSharedReadAliveForRemainingWaiter() async throws {
        let coalescer = KubernetesRESTRequestCoalescer()
        let counter = RESTRequestCoalescerCounter()
        let key = KubernetesRESTRequestCoalescingKey(
            method: "GET",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            scopeIdentity: "synthetic-config",
            credentialFingerprint: Data([0x01]),
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"],
            timeout: 5
        )

        let first = Task {
            try await coalescer.value(for: key) {
                let count = await counter.increment()
                try await Task.sleep(nanoseconds: 40_000_000)
                return RESTResponse(body: "body-\(count)", contentType: "application/json")
            }
        }
        for _ in 0..<100 where await counter.currentValue() == 0 {
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        let startedOperationCount = await counter.currentValue()
        XCTAssertEqual(startedOperationCount, 1)

        let second = Task {
            try await coalescer.value(for: key) {
                let count = await counter.increment()
                try await Task.sleep(nanoseconds: 40_000_000)
                return RESTResponse(body: "body-\(count)", contentType: "application/json")
            }
        }
        try await Task.sleep(nanoseconds: 5_000_000)

        first.cancel()
        let secondResponse = try await second.value
        _ = try? await first.value
        let operationCount = await counter.currentValue()
        XCTAssertEqual(secondResponse.body, "body-1")
        XCTAssertEqual(operationCount, 1)
    }

    func testRESTRequestCoalescingOnlyAllowsBodylessSafeReads() {
        XCTAssertTrue(KubernetesRESTRequestCoalescingKey.isCoalescible(method: "GET", body: nil))
        XCTAssertTrue(KubernetesRESTRequestCoalescingKey.isCoalescible(method: "head", body: nil))
        XCTAssertFalse(KubernetesRESTRequestCoalescingKey.isCoalescible(method: "GET", body: "{}"))
        XCTAssertFalse(KubernetesRESTRequestCoalescingKey.isCoalescible(method: "POST", body: nil))
        XCTAssertFalse(KubernetesRESTRequestCoalescingKey.isCoalescible(method: "PATCH", body: "{}"))
    }

    func testRESTRequestMetricSanitizesPathAndQueryValues() {
        let metric = KubernetesRESTRequestMetric(
            method: "get",
            apiPath: "/apis/apps/v1/namespaces/synthetic/deployments/api-0/status?limit=200&continue=sensitive-token",
            statusCode: 200,
            responseBytes: 128,
            durationSeconds: 0.01,
            attempt: 1,
            outcome: .success
        )

        XCTAssertEqual(metric.method, "GET")
        XCTAssertEqual(
            metric.apiPath,
            "/apis/apps/v1/namespaces/<namespace>/deployments/<name>/status?limit=<redacted>&continue=<redacted>"
        )
        XCTAssertFalse(metric.apiPath.contains("synthetic"))
        XCTAssertFalse(metric.apiPath.contains("api-0"))
        XCTAssertFalse(metric.apiPath.contains("sensitive-token"))

        XCTAssertEqual(
            KubernetesRESTRequestMetric.sanitizedAPIPath(
                "/api/v1/pods?synthetic-secret&limit=200&synthetic-private-key=value&=orphan"
            ),
            "/api/v1/pods?<redacted>&limit=<redacted>&<redacted>&<redacted>"
        )
    }

    func testRESTRequestMetricSanitizesClusterScopedPathMatrix() {
        let cases = [
            (
                path: "/api/v1/nodes",
                expected: "/api/v1/nodes"
            ),
            (
                path: "/api/v1/nodes/synthetic-private-node",
                expected: "/api/v1/nodes/<name>"
            ),
            (
                path: "/api/v1/nodes/synthetic-private-node/status",
                expected: "/api/v1/nodes/<name>/status"
            ),
            (
                path: "/api/v1/nodes/synthetic-private-node/proxy/synthetic-private-route/health",
                expected: "/api/v1/nodes/<name>/proxy/<path>"
            ),
            (
                path: "/api/v1/watch/nodes/synthetic-private-node",
                expected: "/api/v1/watch/nodes/<name>"
            ),
            (
                path: "/apis/rbac.authorization.k8s.io/v1/clusterroles",
                expected: "/apis/rbac.authorization.k8s.io/v1/clusterroles"
            ),
            (
                path: "/apis/rbac.authorization.k8s.io/v1/clusterroles/synthetic-private-role",
                expected: "/apis/rbac.authorization.k8s.io/v1/clusterroles/<name>"
            ),
            (
                path: "/apis/rbac.authorization.k8s.io/v1/clusterroles/synthetic-private-role/status",
                expected: "/apis/rbac.authorization.k8s.io/v1/clusterroles/<name>/status"
            ),
            (
                path: "/apis/rbac.authorization.k8s.io/v1/watch/clusterroles/synthetic-private-role",
                expected: "/apis/rbac.authorization.k8s.io/v1/watch/clusterroles/<name>"
            ),
            (
                path: "/api/v1/namespaces/synthetic-private-namespace/pods/synthetic-private-pod/proxy/synthetic-private-route/health",
                expected: "/api/v1/namespaces/<namespace>/pods/<name>/proxy/<path>"
            )
        ]

        for testCase in cases {
            XCTAssertEqual(
                KubernetesRESTRequestMetric.sanitizedAPIPath(testCase.path),
                testCase.expected,
                testCase.path
            )
        }
    }

    func testRESTRequestMetricsRecorderSummarizesOutcomes() async {
        let recorder = KubernetesRESTRequestMetricsRecorder()

        await recorder.record(KubernetesRESTRequestMetric(
            method: "GET",
            apiPath: "/api/v1/namespaces/synthetic/pods",
            statusCode: 200,
            responseBytes: 256,
            durationSeconds: 0.01,
            attempt: 1,
            outcome: .success
        ))
        await recorder.record(KubernetesRESTRequestMetric(
            method: "GET",
            apiPath: "/api/v1/namespaces/synthetic/pods?continue=token",
            statusCode: 503,
            responseBytes: 64,
            durationSeconds: 0.02,
            attempt: 1,
            outcome: .httpError
        ))
        await recorder.record(KubernetesRESTRequestMetric(
            method: "GET",
            apiPath: "/api/v1/namespaces/synthetic/pods",
            statusCode: nil,
            responseBytes: 0,
            durationSeconds: 0.03,
            attempt: 2,
            outcome: .cancelled,
            cancellationReason: "task-cancelled"
        ))

        let summary = await recorder.summary()

        XCTAssertEqual(summary.requestCount, 3)
        XCTAssertEqual(summary.successCount, 1)
        XCTAssertEqual(summary.failureCount, 1)
        XCTAssertEqual(summary.cancelledCount, 1)
        XCTAssertEqual(summary.responseBytes, 320)
        XCTAssertEqual(summary.totalDurationSeconds, 0.06, accuracy: 0.001)
        XCTAssertEqual(summary.retainedMetricCount, 3)
        XCTAssertEqual(summary.omittedMetricCount, 0)
    }

    func testRESTRequestMetricsRecorderRetainsMostRecentMetricsOnly() async {
        let recorder = KubernetesRESTRequestMetricsRecorder(maxRetainedMetrics: 3)

        for index in 0..<5 {
            await recorder.record(KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/pods/pod-\(index)",
                statusCode: 200,
                responseBytes: index,
                durationSeconds: Double(index) / 100,
                attempt: 1,
                outcome: .success
            ))
        }

        let report = await recorder.report()
        let snapshot = report.metrics
        let summary = report.summary

        XCTAssertEqual(snapshot.map(\.apiPath), [
            "/api/v1/namespaces/<namespace>/pods/<name>",
            "/api/v1/namespaces/<namespace>/pods/<name>",
            "/api/v1/namespaces/<namespace>/pods/<name>"
        ])
        XCTAssertEqual(snapshot.map(\.responseBytes), [2, 3, 4])
        XCTAssertEqual(summary.requestCount, 5)
        XCTAssertEqual(summary.responseBytes, 10)
        XCTAssertEqual(summary.retainedMetricCount, 3)
        XCTAssertEqual(summary.omittedMetricCount, 2)
        XCTAssertEqual(report.endpointGroups.reduce(0) { $0 + $1.requestCount }, 5)
    }

    func testRESTRequestMetricsRecorderSeparatesReusedContextNamesByInternalScopeIdentity() async {
        let recorder = KubernetesRESTRequestMetricsRecorder()

        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/pods",
                statusCode: 200,
                responseBytes: 10,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .success
            ),
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-config-a|https://api-a.invalid"
        )
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/services",
                statusCode: 503,
                responseBytes: 20,
                durationSeconds: 0.02,
                attempt: 1,
                outcome: .httpError
            ),
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-config-b|https://api-b.invalid"
        )

        let secondScopeReport = await recorder.report(contextName: "synthetic-context")
        let firstExplicitScopeReport = await recorder.report(
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-config-a|https://api-a.invalid"
        )
        let secondExplicitScopeReport = await recorder.report(
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-config-b|https://api-b.invalid"
        )
        XCTAssertEqual(secondScopeReport.summary.requestCount, 1)
        XCTAssertEqual(secondScopeReport.summary.failureCount, 1)
        XCTAssertEqual(secondScopeReport.metrics.map(\.responseBytes), [20])
        XCTAssertEqual(firstExplicitScopeReport.metrics.map(\.responseBytes), [10])
        XCTAssertEqual(secondExplicitScopeReport.metrics.map(\.responseBytes), [20])

        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/configmaps",
                statusCode: 200,
                responseBytes: 30,
                durationSeconds: 0.03,
                attempt: 1,
                outcome: .success
            ),
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-config-a|https://api-a.invalid"
        )

        let firstScopeReport = await recorder.report(contextName: "synthetic-context")
        XCTAssertEqual(firstScopeReport.summary.requestCount, 1)
        XCTAssertEqual(firstScopeReport.summary.successCount, 1)
        XCTAssertEqual(firstScopeReport.metrics.map(\.responseBytes), [30])
        XCTAssertEqual(firstScopeReport.endpointGroups.reduce(0) { $0 + $1.requestCount }, 1)
    }

    func testOlderRequestCompletionCannotRestorePreviousMetricsScope() async {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let olderScope = await recorder.activateScope(
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-config-a|https://api-a.invalid"
        )
        let currentScope = await recorder.activateScope(
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-config-b|https://api-b.invalid"
        )

        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/services",
                statusCode: 200,
                responseBytes: 20,
                durationSeconds: 0.02,
                attempt: 1,
                outcome: .success
            ),
            scope: currentScope
        )
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/pods",
                statusCode: 503,
                responseBytes: 10,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .httpError
            ),
            scope: olderScope
        )

        let scopedReport = await recorder.report(contextName: "synthetic-context")
        let globalSummary = await recorder.summary()
        XCTAssertEqual(scopedReport.summary.requestCount, 1)
        XCTAssertEqual(scopedReport.summary.successCount, 1)
        XCTAssertEqual(scopedReport.summary.failureCount, 0)
        XCTAssertEqual(scopedReport.metrics.map(\.responseBytes), [20])
        XCTAssertEqual(globalSummary.requestCount, 2)
        XCTAssertEqual(globalSummary.failureCount, 1)
    }

    func testOlderCredentialResolutionCannotActivateOverNewerReservedScope() async {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let olderGeneration = await recorder.reserveScopeGeneration()
        let newerGeneration = await recorder.reserveScopeGeneration()
        let currentScope = await recorder.activateScope(
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-config-b|https://api-b.invalid",
            requestGeneration: newerGeneration
        )
        let olderScope = await recorder.activateScope(
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-config-a|https://api-a.invalid",
            requestGeneration: olderGeneration
        )

        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/services",
                statusCode: 200,
                responseBytes: 20,
                durationSeconds: 0.02,
                attempt: 1,
                outcome: .success
            ),
            scope: currentScope
        )
        await recorder.record(
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/pods",
                statusCode: 503,
                responseBytes: 10,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .httpError
            ),
            scope: olderScope
        )

        let scopedReport = await recorder.report(contextName: "synthetic-context")
        let globalSummary = await recorder.summary()
        let accumulatorCount = await recorder._testContextAccumulatorCount()
        XCTAssertEqual(scopedReport.summary.requestCount, 1)
        XCTAssertEqual(scopedReport.summary.successCount, 1)
        XCTAssertEqual(scopedReport.metrics.map(\.responseBytes), [20])
        XCTAssertEqual(globalSummary.requestCount, 2)
        XCTAssertEqual(globalSummary.failureCount, 1)
        XCTAssertEqual(accumulatorCount, 1)
    }

    func testRESTRequestMetricsRecorderEvictsLeastRecentlyUsedContextAccumulators() async {
        let recorder = KubernetesRESTRequestMetricsRecorder(
            maxRetainedMetrics: 20,
            maxContextAccumulators: 2
        )

        func metric(responseBytes: Int) -> KubernetesRESTRequestMetric {
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic/pods",
                statusCode: 200,
                responseBytes: responseBytes,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .success
            )
        }

        await recorder.record(
            metric(responseBytes: 1),
            contextName: "synthetic-a",
            scopeIdentity: "scope-a"
        )
        await recorder.record(
            metric(responseBytes: 2),
            contextName: "synthetic-b",
            scopeIdentity: "scope-b"
        )
        await recorder.record(
            metric(responseBytes: 3),
            contextName: "synthetic-a",
            scopeIdentity: "scope-a"
        )
        await recorder.record(
            metric(responseBytes: 4),
            contextName: "synthetic-c",
            scopeIdentity: "scope-c"
        )

        let accumulatorCountBeforeRecreation = await recorder._testContextAccumulatorCount()
        let contextAReport = await recorder.report(contextName: "synthetic-a")
        let contextBReport = await recorder.report(contextName: "synthetic-b")
        let contextCReport = await recorder.report(contextName: "synthetic-c")
        XCTAssertEqual(accumulatorCountBeforeRecreation, 2)
        XCTAssertEqual(contextAReport.summary.requestCount, 2)
        XCTAssertEqual(contextBReport, .empty)
        XCTAssertEqual(contextCReport.summary.requestCount, 1)

        await recorder.record(
            metric(responseBytes: 5),
            contextName: "synthetic-b",
            scopeIdentity: "scope-b"
        )
        let recreatedReport = await recorder.report(contextName: "synthetic-b")
        let accumulatorCountAfterRecreation = await recorder._testContextAccumulatorCount()
        XCTAssertEqual(recreatedReport.summary.requestCount, 1)
        XCTAssertEqual(recreatedReport.metrics.map(\.responseBytes), [5])
        XCTAssertEqual(accumulatorCountAfterRecreation, 2)
    }

    func testRESTRequestMetricsRecorderBoundsLifetimeEndpointGroupsWithSafeOverflow() async {
        let recorder = KubernetesRESTRequestMetricsRecorder(
            maxRetainedMetrics: 2,
            maxEndpointGroupsPerAccumulator: 3
        )
        let paths = [
            "/api/v1/namespaces/synthetic/pods",
            "/api/v1/namespaces/synthetic/services",
            "/api/v1/namespaces/synthetic/configmaps",
            "/api/v1/namespaces/synthetic/secrets",
            "/apis/apps/v1/namespaces/synthetic/deployments",
            "/apis/batch/v1/namespaces/synthetic/jobs"
        ]

        for (index, path) in paths.enumerated() {
            let outcome: KubernetesRESTRequestMetricOutcome = index == 4
                ? .httpError
                : (index == 5 ? .cancelled : .success)
            await recorder.record(
                KubernetesRESTRequestMetric(
                    method: "GET",
                    apiPath: path,
                    statusCode: outcome == .success ? 200 : nil,
                    responseBytes: index + 1,
                    durationSeconds: Double(index + 1) / 100,
                    attempt: 1,
                    outcome: outcome
                ),
                contextName: "synthetic-context",
                scopeIdentity: "synthetic-scope"
            )
        }

        let report = await recorder.report(contextName: "synthetic-context")
        XCTAssertEqual(report.summary.requestCount, paths.count)
        XCTAssertEqual(report.summary.retainedMetricCount, 2)
        XCTAssertEqual(report.summary.omittedMetricCount, 4)
        XCTAssertEqual(report.endpointGroups.count, 3)
        XCTAssertEqual(report.endpointGroups.reduce(0) { $0 + $1.requestCount }, paths.count)
        XCTAssertEqual(report.endpointGroups.reduce(0) { $0 + $1.responseBytes }, 21)
        XCTAssertEqual(report.endpointGroups.reduce(0) { $0 + $1.failureCount }, 1)
        XCTAssertEqual(report.endpointGroups.reduce(0) { $0 + $1.cancelledCount }, 1)
        let overflow = report.endpointGroups.first { $0.apiPath == "/<other-endpoints>" }
        XCTAssertNotNil(overflow)
        XCTAssertEqual(overflow?.sourcePath, "aggregated")
        XCTAssertEqual(overflow?.method, "*")
        XCTAssertEqual(overflow?.requestCount, 4)
        XCTAssertFalse(report.endpointGroups.map(\.apiPath).joined().contains("synthetic"))
    }

    func testRESTRequestMetricsReportsPartitionContextsWithinOneBoundedRetentionWindow() async {
        let recorder = KubernetesRESTRequestMetricsRecorder(maxRetainedMetrics: 10)

        for index in 0..<20 {
            let contextName = index.isMultiple(of: 2) ? "synthetic-context-a" : "synthetic-context-b"
            await recorder.record(
                KubernetesRESTRequestMetric(
                    method: "GET",
                    apiPath: "/api/v1/namespaces/synthetic/pods/pod-\(index)",
                    statusCode: index.isMultiple(of: 4) ? 503 : 200,
                    responseBytes: index,
                    durationSeconds: 0.001,
                    attempt: 1,
                    outcome: index.isMultiple(of: 4) ? .httpError : .success
                ),
                contextName: contextName
            )
        }

        let globalReport = await recorder.report()
        let contextAReport = await recorder.report(contextName: "synthetic-context-a")
        let contextBReport = await recorder.report(contextName: "synthetic-context-b")
        let missingReport = await recorder.report(contextName: "synthetic-context-missing")

        XCTAssertEqual(globalReport.summary.requestCount, 20)
        XCTAssertEqual(globalReport.summary.retainedMetricCount, 10)
        XCTAssertEqual(globalReport.summary.omittedMetricCount, 10)
        XCTAssertEqual(contextAReport.summary.requestCount, 10)
        XCTAssertEqual(contextAReport.summary.failureCount, 5)
        XCTAssertEqual(contextAReport.summary.retainedMetricCount, 5)
        XCTAssertEqual(contextAReport.summary.omittedMetricCount, 5)
        XCTAssertEqual(contextAReport.metrics.map(\.responseBytes), [10, 12, 14, 16, 18])
        XCTAssertEqual(contextBReport.summary.requestCount, 10)
        XCTAssertEqual(contextBReport.summary.failureCount, 0)
        XCTAssertEqual(contextBReport.summary.retainedMetricCount, 5)
        XCTAssertEqual(contextBReport.summary.omittedMetricCount, 5)
        XCTAssertEqual(contextBReport.metrics.map(\.responseBytes), [11, 13, 15, 17, 19])
        XCTAssertEqual(missingReport.summary.requestCount, 0)
        XCTAssertTrue(missingReport.metrics.isEmpty)
        let renderedMetrics = (contextAReport.metrics + contextBReport.metrics)
            .map { "\($0.sourcePath)|\($0.method)|\($0.apiPath)" }
            .joined(separator: "\n")
        XCTAssertFalse(renderedMetrics.contains("synthetic-context-a"))
        XCTAssertFalse(renderedMetrics.contains("synthetic-context-b"))
    }

    func testRESTRequestMetricsReportStaysInternallyConsistentDuringConcurrentRecording() async {
        let capacity = 64
        let requestCount = 500
        let recorder = KubernetesRESTRequestMetricsRecorder(maxRetainedMetrics: capacity)
        let writer = Task {
            for index in 0..<requestCount {
                await recorder.record(
                    KubernetesRESTRequestMetric(
                        method: "GET",
                        apiPath: "/api/v1/namespaces/synthetic/pods/pod-\(index)",
                        statusCode: 200,
                        responseBytes: index,
                        durationSeconds: 0.001,
                        attempt: 1,
                        outcome: .success
                    ),
                    contextName: index.isMultiple(of: 2) ? "synthetic-context-a" : "synthetic-context-b"
                )
                if index.isMultiple(of: 7) {
                    await Task.yield()
                }
            }
        }

        for _ in 0..<250 {
            let report = await recorder.report()

            XCTAssertEqual(report.metrics.count, report.summary.retainedMetricCount)
            XCTAssertEqual(
                report.summary.successCount + report.summary.failureCount + report.summary.cancelledCount,
                report.summary.requestCount
            )
            XCTAssertEqual(
                report.summary.omittedMetricCount,
                report.summary.requestCount - report.summary.retainedMetricCount
            )
            if let first = report.metrics.first, let last = report.metrics.last {
                XCTAssertEqual(first.responseBytes, max(0, report.summary.requestCount - capacity))
                XCTAssertEqual(last.responseBytes, report.summary.requestCount - 1)
            }
            let contextReport = await recorder.report(contextName: "synthetic-context-a")
            XCTAssertEqual(contextReport.metrics.count, contextReport.summary.retainedMetricCount)
            XCTAssertEqual(
                contextReport.summary.omittedMetricCount,
                contextReport.summary.requestCount - contextReport.summary.retainedMetricCount
            )
            XCTAssertTrue(contextReport.metrics.allSatisfy { $0.responseBytes.isMultiple(of: 2) })
            if let last = contextReport.metrics.last {
                XCTAssertEqual(last.responseBytes, (contextReport.summary.requestCount - 1) * 2)
            }
            await Task.yield()
        }

        await writer.value
        let finalReport = await recorder.report()
        XCTAssertEqual(finalReport.summary.requestCount, requestCount)
        XCTAssertEqual(finalReport.summary.retainedMetricCount, capacity)
        XCTAssertEqual(finalReport.metrics.first?.responseBytes, requestCount - capacity)
        XCTAssertEqual(finalReport.metrics.last?.responseBytes, requestCount - 1)
        let finalContextReport = await recorder.report(contextName: "synthetic-context-a")
        XCTAssertEqual(finalContextReport.summary.requestCount, requestCount / 2)
        XCTAssertEqual(finalContextReport.summary.retainedMetricCount, capacity / 2)
        XCTAssertEqual(finalContextReport.summary.omittedMetricCount, requestCount / 2 - capacity / 2)
    }

    func testTerminalResizeFrameUsesKubernetesExecResizeChannel() throws {
        let frame = try KubernetesRESTClient._testTerminalResizeFrame(columns: 120, rows: 32)

        XCTAssertEqual(frame.first, 4)
        let payload = try JSONSerialization.jsonObject(with: Data(frame.dropFirst())) as? [String: Int]
        XCTAssertEqual(payload?["Width"], 120)
        XCTAssertEqual(payload?["Height"], 32)
    }

    func testTerminalResizeFrameClampsInvalidDimensions() throws {
        let frame = try KubernetesRESTClient._testTerminalResizeFrame(columns: 0, rows: 900)
        let payload = try JSONSerialization.jsonObject(with: Data(frame.dropFirst())) as? [String: Int]

        XCTAssertEqual(payload?["Width"], 1)
        XCTAssertEqual(payload?["Height"], 200)
    }

    func testRESTClientLoadsContextsDirectlyFromKubeconfig() async throws {
        let kubeconfig = try writeKubeconfig(
            """
            apiVersion: v1
            kind: Config
            current-context: dev
            clusters:
            - name: dev-cluster
              cluster:
                server: http://127.0.0.1:65535
            contexts:
            - name: dev
              context:
                cluster: dev-cluster
                user: dev-user
                namespace: default
            users:
            - name: dev-user
              user:
                token: test-token
            """
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient()
        let contexts = try await client.listContexts(from: [KubeConfigSource(url: kubeconfig)])

        XCTAssertEqual(contexts, [KubeContext(name: "dev")])
    }

    func testRESTClientLoadsDefaultNamespaceDirectlyFromKubeconfig() async throws {
        let kubeconfig = try writeKubeconfig(
            """
            apiVersion: v1
            kind: Config
            current-context: prod
            clusters:
            - name: prod-cluster
              cluster:
                server: http://127.0.0.1:65535
            contexts:
            - name: prod
              context:
                cluster: prod-cluster
                user: prod-user
                namespace: platform
            users:
            - name: prod-user
              user:
                token: test-token
            """
        )
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient()
        let namespace = try await client.contextNamespace(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: "prod")
        )

        XCTAssertEqual(namespace, "platform")
    }

    func testRESTClientMergesKubeconfigFilesWithFirstFileWinningNamedEntries() async throws {
        let first = try writeKubeconfig(
            """
            apiVersion: v1
            kind: Config
            current-context: shared
            clusters:
            - name: shared-cluster
              cluster:
                server: http://127.0.0.1:65535
            contexts:
            - name: shared
              context:
                cluster: shared-cluster
                user: shared-user
                namespace: first-namespace
            users:
            - name: shared-user
              user:
                token: first-token
            """
        )
        let second = try writeKubeconfig(
            """
            apiVersion: v1
            kind: Config
            current-context: later
            clusters:
            - name: shared-cluster
              cluster:
                server: http://127.0.0.1:65534
            contexts:
            - name: shared
              context:
                cluster: shared-cluster
                user: shared-user
                namespace: second-namespace
            - name: later
              context:
                cluster: shared-cluster
                user: shared-user
                namespace: later-namespace
            users:
            - name: shared-user
              user:
                token: second-token
            """
        )
        defer {
            try? FileManager.default.removeItem(at: first)
            try? FileManager.default.removeItem(at: second)
        }

        let client = KubernetesClient()
        let namespace = try await client.contextNamespace(
            from: [KubeConfigSource(url: first), KubeConfigSource(url: second)],
            context: KubeContext(name: "shared")
        )
        let contexts = try await client.listContexts(from: [KubeConfigSource(url: first), KubeConfigSource(url: second)])

        XCTAssertEqual(namespace, "first-namespace")
        XCTAssertEqual(contexts.map(\.name), ["later", "shared"])
    }

    func testOutputParserParsesKubernetesPodJSON() throws {
        let raw = """
        {"items":[{"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2026-04-26T10:00:00Z","labels":{"app":"api","tier":"web"},"ownerReferences":[{"kind":"ReplicaSet","name":"api-7c9d8f6b5c"}]},"spec":{"containers":[{"name":"app","image":"example.test/api:v1"},{"name":"sidecar","image":"example.test/sidecar:v2"}],"initContainers":[{"name":"setup","image":"example.test/setup:v1"}],"ephemeralContainers":[{"name":"debugger","image":"example.test/debugger:v1"}]},"status":{"phase":"Running","containerStatuses":[{"restartCount":2}],"initContainerStatuses":[{"restartCount":1}],"ephemeralContainerStatuses":[{"restartCount":3}]}}]}
        """

        let pods = try KubernetesOutputParser().parsePodsListJSON(namespace: "default", from: raw)

        XCTAssertEqual(pods.count, 1)
        XCTAssertEqual(pods.first?.name, "api-0")
        XCTAssertEqual(pods.first?.status, "Running")
        XCTAssertEqual(pods.first?.totalRestarts, 6)
        XCTAssertEqual(pods.first?.labels, ["app": "api", "tier": "web"])
        XCTAssertEqual(pods.first?.containerNamesLine, "app, sidecar")
        XCTAssertEqual(pods.first?.initContainerNamesLine, "setup")
        XCTAssertEqual(pods.first?.ephemeralContainerNamesLine, "debugger")
        XCTAssertEqual(pods.first?.logContainerNames, ["app", "sidecar", "setup", "debugger"])
        XCTAssertEqual(pods.first?.containerImagesLine, "example.test/api:v1, example.test/sidecar:v2")
        XCTAssertEqual(pods.first?.ownerReferencesLine, "ReplicaSet/api-7c9d8f6b5c")
    }

    func testOutputParserProjectsIngressBackendServices() throws {
        let raw = """
        {
          "items": [
            {
              "metadata": {"name": "api-public", "namespace": "synthetic"},
              "spec": {
                "rules": [
                  {
                    "host": "api.synthetic.example",
                    "http": {
                      "paths": [
                        {"backend": {"service": {"name": "api", "port": {"number": 80}}}},
                        {"backend": {"service": {"name": "metrics", "port": {"number": 9090}}}}
                      ]
                    }
                  }
                ]
              },
              "status": {"loadBalancer": {"ingress": [{"hostname": "lb.synthetic.example"}]}}
            }
          ]
        }
        """

        let ingresses = try KubernetesOutputParser().parseIngresses(namespace: "synthetic", from: raw)

        XCTAssertEqual(ingresses.first?.name, "api-public")
        XCTAssertEqual(ingresses.first?.primaryText, "api.synthetic.example")
        XCTAssertEqual(ingresses.first?.secondaryText, "Service api, metrics")
    }

    func testOutputParserProjectsPVCBoundPersistentVolume() throws {
        let raw = """
        {
          "items": [
            {
              "metadata": {"name": "postgres-data", "namespace": "synthetic"},
              "spec": {
                "volumeName": "pv-postgres-data",
                "resources": {"requests": {"storage": "20Gi"}}
              },
              "status": {"phase": "Bound", "capacity": {"storage": "20Gi"}}
            }
          ]
        }
        """

        let pvcs = try KubernetesOutputParser().parsePersistentVolumeClaims(namespace: "synthetic", from: raw)

        XCTAssertEqual(pvcs.first?.name, "postgres-data")
        XCTAssertEqual(pvcs.first?.primaryText, "Bound")
        XCTAssertEqual(pvcs.first?.secondaryText, "PV pv-postgres-data · 20Gi")
    }

    func testOutputParserProjectsOwnerReferencesForGenericResources() throws {
        let raw = """
        {
          "items": [
            {
              "metadata": {
                "name": "api-7c9d8f6b5c",
                "namespace": "synthetic",
                "ownerReferences": [
                  {"kind": "Deployment", "name": "api"}
                ]
              },
              "spec": {"replicas": 3},
              "status": {"replicas": 3, "readyReplicas": 2}
            }
          ]
        }
        """

        let replicaSets = try KubernetesOutputParser().parseReplicaSets(namespace: "synthetic", from: raw)

        XCTAssertEqual(replicaSets.first?.name, "api-7c9d8f6b5c")
        XCTAssertEqual(replicaSets.first?.primaryText, "2/3 ready")
        XCTAssertEqual(replicaSets.first?.ownerReferencesLine, "Deployment/api")
    }

    func testOutputParserProjectsRoleBindingRoleRefKindAndName() throws {
        let raw = """
        {
          "items": [
            {
              "metadata": {"name": "api-readers", "namespace": "synthetic"},
              "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "view"},
              "subjects": [
                {"kind": "ServiceAccount", "name": "api"}
              ]
            }
          ]
        }
        """

        let bindings = try KubernetesOutputParser().parseRoleBindings(namespace: "synthetic", from: raw)

        XCTAssertEqual(bindings.first?.name, "api-readers")
        XCTAssertEqual(bindings.first?.primaryText, "→ ClusterRole/view")
        XCTAssertEqual(bindings.first?.secondaryText, "1 subject")
    }

    func testKubernetesListJSONReadsRemainingItemCount() {
        let raw = #"{"metadata":{"continue":"next","remainingItemCount":41},"items":[{"metadata":{"name":"one"}}]}"#

        XCTAssertEqual(KubernetesListJSON.collectionListTotal(from: raw), 42)
        XCTAssertEqual(KubernetesListJSON.collectionPageInfo(from: raw)?.continueToken, "next")
    }

    func testPagedCollectionCountFallsBackWhenContinueTokenExpires() async {
        let first = KubernetesRESTRequest(apiPath: "/api/v1/namespaces/default/pods?limit=250")
        let second = KubernetesRESTRequest(apiPath: "/api/v1/namespaces/default/pods?limit=250&continue=stale")
        let count = await KubernetesClient.pagedCollectionCount(
            firstRequest: first,
            nextRequest: { token in
                token == "stale" ? second : nil
            },
            maxPages: 4,
            progress: nil,
            fetch: { request in
                if request == first {
                    return #"{"metadata":{"continue":"stale"},"items":[{},{}]}"#
                }
                throw RuneError.commandFailed(
                    command: "kubernetes REST GET \(request.apiPath)",
                    message: #"HTTP 410: {"kind":"Status","reason":"Expired","message":"The provided continue parameter is too old."}"#
                )
            },
            fallbackTotal: {
                7
            }
        )

        XCTAssertEqual(count, 7)
    }

    func testPagedCollectionCountReportsProgressAndStopsAtRemainingItemCount() async {
        let first = KubernetesRESTRequest(apiPath: "/api/v1/namespaces/default/pods?limit=250")
        var progressValues: [Int] = []
        let count = await KubernetesClient.pagedCollectionCount(
            firstRequest: first,
            nextRequest: { _ in nil },
            maxPages: 4,
            progress: { progressValues.append($0) },
            fetch: { request in
                XCTAssertEqual(request, first)
                return #"{"metadata":{"remainingItemCount":3},"items":[{},{}]}"#
            },
            fallbackTotal: nil
        )

        XCTAssertEqual(count, 5)
        XCTAssertEqual(progressValues, [2, 5])
    }

    func testPagedCollectionCountReturnsNilWhenMaxPagesIsExhaustedBeforeFinalPage() async {
        let first = KubernetesRESTRequest(apiPath: "/api/v1/namespaces/default/pods?limit=250")
        let count = await KubernetesClient.pagedCollectionCount(
            firstRequest: first,
            nextRequest: { token in
                KubernetesRESTRequest(apiPath: "/api/v1/namespaces/default/pods?limit=250&continue=\(token)")
            },
            maxPages: 1,
            progress: nil,
            fetch: { _ in
                #"{"metadata":{"continue":"next"},"items":[{}]}"#
            },
            fallbackTotal: {
                XCTFail("Max page exhaustion should not use expired-token fallback")
                return 99
            }
        )

        XCTAssertNil(count)
    }

    func testPagedCollectionCountTreatsCancellationAsUnavailableWithoutFallback() async {
        let first = KubernetesRESTRequest(apiPath: "/api/v1/namespaces/default/pods?limit=250")
        let count = await KubernetesClient.pagedCollectionCount(
            firstRequest: first,
            nextRequest: { _ in nil },
            maxPages: 4,
            progress: nil,
            fetch: { _ in
                throw CancellationError()
            },
            fallbackTotal: {
                XCTFail("Cancellation should not be treated as an expired continue token")
                return 99
            }
        )

        XCTAssertNil(count)
    }

    func testResourceCountPathCoverageForEveryKnownResourceKind() {
        let namespace = "team alpha"
        let clusterScopedKinds: Set<KubeResourceKind> = [
            .node,
            .clusterRole,
            .clusterRoleBinding,
            .persistentVolume,
            .storageClass
        ]

        for kind in KubeResourceKind.allCases {
            let resource = KubernetesRESTPath.resourceName(for: kind)
            if clusterScopedKinds.contains(kind) {
                let probe = KubernetesRESTPath.clusterCollectionMetadataProbe(resource: resource)
                let request = KubernetesRESTPath.clusterCollectionRequest(
                    resource: resource,
                    options: KubernetesListOptions(limit: 250, continueToken: "next page")
                )

                XCTAssertNotNil(probe, "Missing cluster metadata probe for \(kind)")
                XCTAssertNotNil(request, "Missing cluster paged request for \(kind)")
                XCTAssertTrue(probe?.contains("limit=1") == true, "Expected limit probe for \(kind)")
                XCTAssertTrue(request?.apiPath.contains("limit=250") == true, "Expected paged limit for \(kind)")
                XCTAssertTrue(request?.apiPath.contains("continue=next%20page") == true, "Expected encoded continue token for \(kind)")
            } else {
                let probe = KubernetesRESTPath.namespacedCollectionMetadataProbe(namespace: namespace, resource: resource)
                let request = KubernetesRESTPath.namespacedCollectionRequest(
                    namespace: namespace,
                    resource: resource,
                    options: KubernetesListOptions(limit: 250, continueToken: "next page")
                )

                XCTAssertNotNil(probe, "Missing namespaced metadata probe for \(kind)")
                XCTAssertNotNil(request, "Missing namespaced paged request for \(kind)")
                XCTAssertTrue(probe?.contains("/namespaces/team%20alpha/") == true, "Expected encoded namespace for \(kind)")
                XCTAssertTrue(probe?.contains("limit=1") == true, "Expected limit probe for \(kind)")
                XCTAssertTrue(request?.apiPath.contains("limit=250") == true, "Expected paged limit for \(kind)")
                XCTAssertTrue(request?.apiPath.contains("continue=next%20page") == true, "Expected encoded continue token for \(kind)")
            }
        }
    }

    func testPreferredPortForwardPodChoosesRunningPodDeterministically() {
        let pods = [
            PodSummary(name: "api-b", namespace: "default", status: "Pending"),
            PodSummary(name: "api-c", namespace: "default", status: "Running"),
            PodSummary(name: "api-a", namespace: "default", status: "Running")
        ]

        XCTAssertEqual(KubernetesClient.preferredPortForwardPod(from: pods)?.name, "api-a")
    }

    func testTerminalSessionRegistryRejectsLateHandleAfterStopAndAllowsExplicitNewGeneration() async {
        let registry = TerminalSessionRegistry()
        let lateHandle = RecordingTerminalSessionHandle()
        let stoppedGeneration = await registry.beginStart(id: "terminal-synthetic")

        let handleBeforeRegistration = await registry.remove(id: "terminal-synthetic")
        let insertedLateHandle = await registry.insert(
            handle: lateHandle,
            id: "terminal-synthetic",
            generation: stoppedGeneration
        )

        XCTAssertNil(handleBeforeRegistration)
        XCTAssertFalse(insertedLateHandle)
        XCTAssertTrue(lateHandle.isTerminated)
        let missingLateHandle = await registry.handle(id: "terminal-synthetic")
        XCTAssertNil(missingLateHandle)
        let metadataAfterLateHandle = await registry._testMetadataSnapshot()
        XCTAssertEqual(metadataAfterLateHandle, emptyRegistryMetadata)

        let replacementHandle = RecordingTerminalSessionHandle()
        let replacementGeneration = await registry.beginStart(id: "terminal-synthetic")
        let insertedReplacement = await registry.insert(
            handle: replacementHandle,
            id: "terminal-synthetic",
            generation: replacementGeneration
        )
        let registeredReplacement = await registry.handle(id: "terminal-synthetic")

        XCTAssertTrue(insertedReplacement)
        XCTAssertEqual(registeredReplacement?.id, replacementHandle.id)
        XCTAssertFalse(replacementHandle.isTerminated)
        let activeMetadata = await registry._testMetadataSnapshot()
        XCTAssertEqual(
            activeMetadata,
            RunningCommandRegistryMetadataSnapshot(
                activeHandleCount: 1,
                pendingStartCount: 0,
                stopRequestedStartCount: 0
            )
        )

        let removedReplacement = await registry.remove(id: "terminal-synthetic")
        removedReplacement?.terminate()
        let finalMetadata = await registry._testMetadataSnapshot()
        XCTAssertEqual(finalMetadata, emptyRegistryMetadata)
    }

    func testPortForwardRegistryRejectsLateHandleAndCleansStoppedStartMetadata() async {
        let registry = PortForwardRegistry()
        let generation = await registry.beginStart(id: "forward-synthetic")
        let handleBeforeRegistration = await registry.remove(id: "forward-synthetic")
        let pendingMetadata = await registry._testMetadataSnapshot()

        XCTAssertNil(handleBeforeRegistration)
        XCTAssertEqual(
            pendingMetadata,
            RunningCommandRegistryMetadataSnapshot(
                activeHandleCount: 0,
                pendingStartCount: 1,
                stopRequestedStartCount: 1
            )
        )

        let lateHandle = RecordingTerminalSessionHandle()
        let insertedLateHandle = await registry.insert(
            handle: lateHandle,
            id: "forward-synthetic",
            generation: generation
        )
        let metadataAfterLateHandle = await registry._testMetadataSnapshot()

        XCTAssertFalse(insertedLateHandle)
        XCTAssertTrue(lateHandle.isTerminated)
        XCTAssertEqual(metadataAfterLateHandle, emptyRegistryMetadata)

        let replacementGeneration = await registry.beginStart(id: "forward-synthetic")
        let replacementHandle = RecordingTerminalSessionHandle()
        let insertedReplacement = await registry.insert(
            handle: replacementHandle,
            id: "forward-synthetic",
            generation: replacementGeneration
        )
        XCTAssertTrue(insertedReplacement)
        XCTAssertFalse(replacementHandle.isTerminated)

        let removedReplacement = await registry.remove(id: "forward-synthetic")
        XCTAssertEqual(removedReplacement?.id, replacementHandle.id)
        removedReplacement?.terminate()
        let finalMetadata = await registry._testMetadataSnapshot()

        XCTAssertTrue(replacementHandle.isTerminated)
        XCTAssertEqual(finalMetadata, emptyRegistryMetadata)
    }

    func testRegistriesCleanMetadataWhenStartFailsWithoutStop() async {
        let terminalRegistry = TerminalSessionRegistry()
        let portForwardRegistry = PortForwardRegistry()
        let terminalGeneration = await terminalRegistry.beginStart(id: "failed-terminal")
        let forwardGeneration = await portForwardRegistry.beginStart(id: "failed-forward")

        let terminalWasStopped = await terminalRegistry.finishStart(
            id: "failed-terminal",
            generation: terminalGeneration
        )
        let forwardWasStopped = await portForwardRegistry.finishStart(
            id: "failed-forward",
            generation: forwardGeneration
        )

        XCTAssertFalse(terminalWasStopped)
        XCTAssertFalse(forwardWasStopped)
        let terminalMetadata = await terminalRegistry._testMetadataSnapshot()
        let forwardMetadata = await portForwardRegistry._testMetadataSnapshot()
        XCTAssertEqual(terminalMetadata, emptyRegistryMetadata)
        XCTAssertEqual(forwardMetadata, emptyRegistryMetadata)
    }

    func testRegistryCompletionForOlderGenerationCannotEraseNewerSameIDStart() async {
        let terminalRegistry = TerminalSessionRegistry()
        let firstTerminalGeneration = await terminalRegistry.beginStart(id: "reused-terminal")
        let secondTerminalGeneration = await terminalRegistry.beginStart(id: "reused-terminal")

        let firstTerminalWasStopped = await terminalRegistry.finishStart(
            id: "reused-terminal",
            generation: firstTerminalGeneration
        )
        let terminalMetadataWithNewerStart = await terminalRegistry._testMetadataSnapshot()
        XCTAssertFalse(firstTerminalWasStopped)
        XCTAssertEqual(terminalMetadataWithNewerStart.pendingStartCount, 1)

        let terminalHandle = RecordingTerminalSessionHandle()
        let insertedTerminal = await terminalRegistry.insert(
            handle: terminalHandle,
            id: "reused-terminal",
            generation: secondTerminalGeneration
        )
        let removedTerminal = await terminalRegistry.remove(id: "reused-terminal")
        removedTerminal?.terminate()
        XCTAssertTrue(insertedTerminal)

        let portForwardRegistry = PortForwardRegistry()
        let firstForwardGeneration = await portForwardRegistry.beginStart(id: "reused-forward")
        let secondForwardGeneration = await portForwardRegistry.beginStart(id: "reused-forward")

        let firstForwardWasStopped = await portForwardRegistry.finishStart(
            id: "reused-forward",
            generation: firstForwardGeneration
        )
        let forwardMetadataWithNewerStart = await portForwardRegistry._testMetadataSnapshot()
        XCTAssertFalse(firstForwardWasStopped)
        XCTAssertEqual(forwardMetadataWithNewerStart.pendingStartCount, 1)

        let forwardHandle = RecordingTerminalSessionHandle()
        let insertedForward = await portForwardRegistry.insert(
            handle: forwardHandle,
            id: "reused-forward",
            generation: secondForwardGeneration
        )
        let removedForward = await portForwardRegistry.remove(id: "reused-forward")
        removedForward?.terminate()
        XCTAssertTrue(insertedForward)

        let finalTerminalMetadata = await terminalRegistry._testMetadataSnapshot()
        let finalForwardMetadata = await portForwardRegistry._testMetadataSnapshot()
        XCTAssertEqual(finalTerminalMetadata, emptyRegistryMetadata)
        XCTAssertEqual(finalForwardMetadata, emptyRegistryMetadata)
    }

    func testTerminalNaturalTerminationReleasesExactGenerationWithoutTouchingReplacement() async {
        let registry = TerminalSessionRegistry()
        let firstGeneration = await registry.beginStart(id: "reused-terminal")
        let firstHandle = RecordingTerminalSessionHandle()
        let insertedFirst = await registry.insert(
            handle: firstHandle,
            id: "reused-terminal",
            generation: firstGeneration
        )
        XCTAssertTrue(insertedFirst)

        let replacementGeneration = await registry.beginStart(id: "reused-terminal")
        XCTAssertTrue(firstHandle.isTerminated)
        let shouldNotifyOldTermination = await registry.complete(
            id: "reused-terminal",
            generation: firstGeneration
        )
        XCTAssertFalse(shouldNotifyOldTermination)

        let replacementHandle = RecordingTerminalSessionHandle()
        let insertedReplacement = await registry.insert(
            handle: replacementHandle,
            id: "reused-terminal",
            generation: replacementGeneration
        )
        XCTAssertTrue(insertedReplacement)

        let delayedOldCompletion = await registry.complete(
            id: "reused-terminal",
            generation: firstGeneration
        )
        let stillRegistered = await registry.handle(id: "reused-terminal")
        XCTAssertFalse(delayedOldCompletion)
        XCTAssertEqual(stillRegistered?.id, replacementHandle.id)

        let shouldNotifyReplacementTermination = await registry.complete(
            id: "reused-terminal",
            generation: replacementGeneration
        )
        XCTAssertTrue(shouldNotifyReplacementTermination)
        let finalMetadata = await registry._testMetadataSnapshot()
        XCTAssertEqual(finalMetadata, emptyRegistryMetadata)
    }

    func testTerminalPendingStopSuppressesTerminationCallbackRegardlessOfRaceOrder() async {
        let registry = TerminalSessionRegistry()
        let generation = await registry.beginStart(id: "stopped-terminal")
        _ = await registry.remove(id: "stopped-terminal")

        let shouldNotify = await registry.complete(
            id: "stopped-terminal",
            generation: generation
        )
        XCTAssertFalse(shouldNotify)

        let lateHandle = RecordingTerminalSessionHandle()
        let insertedLateHandle = await registry.insert(
            handle: lateHandle,
            id: "stopped-terminal",
            generation: generation
        )
        XCTAssertFalse(insertedLateHandle)
        XCTAssertTrue(lateHandle.isTerminated)
        let metadata = await registry._testMetadataSnapshot()
        XCTAssertEqual(metadata, emptyRegistryMetadata)
    }

    func testPortForwardFailureBeforeRegistrationIsDeliveredOnceWithoutStoppedOutcome() async {
        let registry = PortForwardRegistry()
        let generation = await registry.beginStart(id: "failing-forward")

        let failureDisposition = await registry.recordFailure(
            message: "synthetic connection failure",
            id: "failing-forward",
            generation: generation
        )
        guard case .deferred = failureDisposition else {
            return XCTFail("A failure during startup should be deferred to registration")
        }

        let handle = RecordingTerminalSessionHandle()
        let result = await registry.register(
            handle: handle,
            id: "failing-forward",
            generation: generation
        )

        XCTAssertEqual(result, .failed("synthetic connection failure"))
        XCTAssertTrue(handle.isTerminated)
        let finishedAfterFailure = await registry.finishStart(
            id: "failing-forward",
            generation: generation
        )
        XCTAssertFalse(finishedAfterFailure)
        let metadata = await registry._testMetadataSnapshot()
        XCTAssertEqual(metadata, emptyRegistryMetadata)
    }

    func testDelayedPortForwardCallbacksCannotAffectNewerGeneration() async {
        let registry = PortForwardRegistry()
        let firstGeneration = await registry.beginStart(id: "reused-forward")
        let firstHandle = RecordingTerminalSessionHandle()
        let insertedFirst = await registry.insert(
            handle: firstHandle,
            id: "reused-forward",
            generation: firstGeneration
        )
        XCTAssertTrue(insertedFirst)

        let replacementGeneration = await registry.beginStart(id: "reused-forward")
        XCTAssertTrue(firstHandle.isTerminated)
        let delayedFailureWhileReplacementPending = await registry.recordFailure(
            message: "late failure during replacement startup",
            id: "reused-forward",
            generation: firstGeneration
        )
        guard case .ignored = delayedFailureWhileReplacementPending else {
            return XCTFail("An older callback should be ignored while its replacement is pending")
        }
        let oldReadyWhileReplacementPending = await registry.shouldDeliverReady(
            id: "reused-forward",
            generation: firstGeneration
        )
        XCTAssertFalse(oldReadyWhileReplacementPending)

        let replacementHandle = RecordingTerminalSessionHandle()
        let insertedReplacement = await registry.insert(
            handle: replacementHandle,
            id: "reused-forward",
            generation: replacementGeneration
        )
        XCTAssertTrue(insertedReplacement)

        let delayedFailure = await registry.recordFailure(
            message: "late failure from replaced handle",
            id: "reused-forward",
            generation: firstGeneration
        )
        guard case .ignored = delayedFailure else {
            return XCTFail("A callback from an older generation should be ignored")
        }
        let shouldDeliverOldReady = await registry.shouldDeliverReady(
            id: "reused-forward",
            generation: firstGeneration
        )
        let shouldDeliverReplacementReady = await registry.shouldDeliverReady(
            id: "reused-forward",
            generation: replacementGeneration
        )
        XCTAssertFalse(shouldDeliverOldReady)
        XCTAssertTrue(shouldDeliverReplacementReady)
        XCTAssertFalse(replacementHandle.isTerminated)

        let removed = await registry.remove(id: "reused-forward")
        removed?.terminate()
        XCTAssertEqual(removed?.id, replacementHandle.id)
        let finalMetadata = await registry._testMetadataSnapshot()
        XCTAssertEqual(finalMetadata, emptyRegistryMetadata)
    }

    func testTerminalStopBeforeBeginUsesBoundedExpiringIntent() async throws {
        let registry = TerminalSessionRegistry(
            preStartStopIntentCapacity: 3,
            preStartStopIntentTTL: 0.05
        )
        for index in 0..<4 {
            _ = await registry.remove(
                id: "future-terminal-\(index)",
                rememberIfNotStarted: true
            )
        }

        let boundedMetadata = await registry._testMetadataSnapshot()
        XCTAssertEqual(boundedMetadata.preStartStopIntentCount, 3)

        let stoppedGeneration = await registry.beginStart(id: "future-terminal-3")
        let isStopRequested = await registry.isStopRequested(
            id: "future-terminal-3",
            generation: stoppedGeneration
        )
        let finishedStoppedStart = await registry.finishStart(
            id: "future-terminal-3",
            generation: stoppedGeneration
        )
        XCTAssertTrue(isStopRequested)
        XCTAssertTrue(finishedStoppedStart)

        try await Task.sleep(for: .milliseconds(80))
        let expiredMetadata = await registry._testMetadataSnapshot()
        XCTAssertEqual(expiredMetadata, emptyRegistryMetadata)
    }

    func testRegistriesReleaseMetadataAcrossTenThousandSyntheticStartStopLifecycles() async {
        let terminalRegistry = TerminalSessionRegistry()
        let portForwardRegistry = PortForwardRegistry()
        var rejectedTerminalHandles = 0
        var rejectedForwardHandles = 0
        var terminatedTerminalHandles = 0
        var terminatedForwardHandles = 0
        var completedTerminalStops = 0
        var completedForwardStops = 0

        for index in 0..<10_000 {
            let id = "synthetic-session-\(index)"
            let terminalGeneration = await terminalRegistry.beginStart(id: id)
            let forwardGeneration = await portForwardRegistry.beginStart(id: id)
            _ = await terminalRegistry.remove(id: id)
            _ = await portForwardRegistry.remove(id: id)

            if index.isMultiple(of: 2) {
                let terminalHandle = RecordingTerminalSessionHandle()
                let forwardHandle = RecordingTerminalSessionHandle()
                if !(await terminalRegistry.insert(
                    handle: terminalHandle,
                    id: id,
                    generation: terminalGeneration
                )) {
                    rejectedTerminalHandles += 1
                }
                if !(await portForwardRegistry.insert(
                    handle: forwardHandle,
                    id: id,
                    generation: forwardGeneration
                )) {
                    rejectedForwardHandles += 1
                }
                if terminalHandle.isTerminated {
                    terminatedTerminalHandles += 1
                }
                if forwardHandle.isTerminated {
                    terminatedForwardHandles += 1
                }
            } else {
                let terminalWasStopped = await terminalRegistry.finishStart(
                    id: id,
                    generation: terminalGeneration
                )
                let forwardWasStopped = await portForwardRegistry.finishStart(
                    id: id,
                    generation: forwardGeneration
                )
                if terminalWasStopped {
                    completedTerminalStops += 1
                }
                if forwardWasStopped {
                    completedForwardStops += 1
                }
            }

            if index.isMultiple(of: 1_000) {
                let terminalMetadata = await terminalRegistry._testMetadataSnapshot()
                let forwardMetadata = await portForwardRegistry._testMetadataSnapshot()
                XCTAssertEqual(terminalMetadata, emptyRegistryMetadata)
                XCTAssertEqual(forwardMetadata, emptyRegistryMetadata)
            }
        }

        for index in 0..<10_000 {
            let id = "unknown-session-\(index)"
            _ = await terminalRegistry.remove(id: id)
            _ = await portForwardRegistry.remove(id: id)
        }

        let terminalMetadata = await terminalRegistry._testMetadataSnapshot()
        let forwardMetadata = await portForwardRegistry._testMetadataSnapshot()
        XCTAssertEqual(rejectedTerminalHandles, 5_000)
        XCTAssertEqual(rejectedForwardHandles, 5_000)
        XCTAssertEqual(terminatedTerminalHandles, 5_000)
        XCTAssertEqual(terminatedForwardHandles, 5_000)
        XCTAssertEqual(completedTerminalStops, 5_000)
        XCTAssertEqual(completedForwardStops, 5_000)
        XCTAssertEqual(terminalMetadata, emptyRegistryMetadata)
        XCTAssertEqual(forwardMetadata, emptyRegistryMetadata)
    }

    func testServerSideApplyYAMLOmitsManagedFieldsFromFetchedManifest() {
        let yaml = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: api-0
          namespace: default
          managedFields:
          - apiVersion: v1
            fieldsType: FieldsV1
            fieldsV1:
              f:metadata:
                f:labels: {}
          labels:
            app: api
        spec:
          containers:
          - name: api
            image: api:latest
        """

        let sanitized = KubernetesRESTClient._testServerSideApplyYAML(from: yaml)

        XCTAssertFalse(sanitized.contains("managedFields"))
        XCTAssertFalse(sanitized.contains("fieldsType"))
        XCTAssertTrue(sanitized.contains("  labels:"))
        XCTAssertTrue(sanitized.contains("spec:"))
    }

    func testServerSideApplyYAMLOmitsInlineEmptyManagedFields() {
        let yaml = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: settings
          managedFields: []
          labels:
            app: settings
        data:
          key: value
        """

        let sanitized = KubernetesRESTClient._testServerSideApplyYAML(from: yaml)

        XCTAssertFalse(sanitized.contains("managedFields"))
        XCTAssertTrue(sanitized.contains("  labels:"))
        XCTAssertTrue(sanitized.contains("data:"))
    }

    private func writeKubeconfig(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-native-kubeconfig-\(UUID().uuidString).yaml")
        try contents.write(to: url, atomically: true, encoding: .utf8)
        return url
    }

    private var emptyRegistryMetadata: RunningCommandRegistryMetadataSnapshot {
        RunningCommandRegistryMetadataSnapshot(
            activeHandleCount: 0,
            pendingStartCount: 0,
            stopRequestedStartCount: 0
        )
    }
}

private final class RecordingTerminalSessionHandle: RunningCommandControlling, @unchecked Sendable {
    let id = UUID()
    private let lock = NSLock()
    private var terminated = false

    var isTerminated: Bool {
        lock.lock()
        defer { lock.unlock() }
        return terminated
    }

    func terminate() {
        lock.lock()
        terminated = true
        lock.unlock()
    }

    func writeToStdin(_: Data) throws {}
}

private actor RESTRequestCoalescerCounter {
    private var count = 0

    func increment() -> Int {
        count += 1
        return count
    }

    func currentValue() -> Int {
        count
    }
}

private actor RESTRequestCoalescerCancellationProbe {
    private(set) var started = false
    private(set) var cancelled = false

    func markStarted() {
        started = true
    }

    func markCancelled() {
        cancelled = true
    }
}
