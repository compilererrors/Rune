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
        XCTAssertEqual(summary.omittedMetricCount, 2)
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
