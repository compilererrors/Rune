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
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"]
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

    func testRESTRequestCoalescerCancelsUnderlyingReadWhenLastWaiterCancels() async throws {
        let coalescer = KubernetesRESTRequestCoalescer()
        let probe = RESTRequestCoalescerCancellationProbe()
        let key = KubernetesRESTRequestCoalescingKey(
            method: "GET",
            server: "https://cluster.example.test",
            contextName: "synthetic",
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"]
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
            apiPath: "/api/v1/pods",
            headers: ["Accept": "application/json"]
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

        let snapshot = await recorder.snapshot()
        let summary = await recorder.summary()

        XCTAssertEqual(snapshot.map(\.apiPath), [
            "/api/v1/namespaces/<namespace>/pods/<name>",
            "/api/v1/namespaces/<namespace>/pods/<name>",
            "/api/v1/namespaces/<namespace>/pods/<name>"
        ])
        XCTAssertEqual(snapshot.map(\.responseBytes), [2, 3, 4])
        XCTAssertEqual(summary.requestCount, 5)
        XCTAssertEqual(summary.responseBytes, 10)
        XCTAssertEqual(summary.retainedMetricCount, 3)
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
        {"items":[{"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2026-04-26T10:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":2}]}}]}
        """

        let pods = try KubernetesOutputParser().parsePodsListJSON(namespace: "default", from: raw)

        XCTAssertEqual(pods.count, 1)
        XCTAssertEqual(pods.first?.name, "api-0")
        XCTAssertEqual(pods.first?.status, "Running")
        XCTAssertEqual(pods.first?.totalRestarts, 2)
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

    func testPreferredPortForwardPodChoosesRunningPodDeterministically() {
        let pods = [
            PodSummary(name: "api-b", namespace: "default", status: "Pending"),
            PodSummary(name: "api-c", namespace: "default", status: "Running"),
            PodSummary(name: "api-a", namespace: "default", status: "Running")
        ]

        XCTAssertEqual(KubernetesClient.preferredPortForwardPod(from: pods)?.name, "api-a")
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
