import Foundation
import XCTest
@testable import RuneDiagnostics
@testable import RuneKube
@testable import RuneUI

final class KubernetesRequestMetricsSupportBundleProjectorTests: XCTestCase {
    func testProjectsRequestMetricsSummaryForDebugUI() {
        let presentation = KubernetesRequestMetricsDebugPresentation(summary: KubernetesRESTRequestMetricsSummary(
            requestCount: 12,
            successCount: 8,
            failureCount: 3,
            cancelledCount: 1,
            responseBytes: 4_096,
            totalDurationSeconds: 1.25,
            retainedMetricCount: 5
        ))

        XCTAssertEqual(presentation.requestCountText, "12 API attempts")
        XCTAssertEqual(presentation.outcomeText, "8 ok • 3 failed • 1 cancelled")
        XCTAssertEqual(presentation.transferText, "4.0 KB • 1.25 s total")
        XCTAssertEqual(presentation.retainedText, "5 retained • 7 omitted")
        XCTAssertTrue(presentation.hasFailures)
        XCTAssertTrue(presentation.endpointHighlights.isEmpty)
    }

    func testProjectsBoundedPrivacySafeEndpointHighlightsForDebugUI() {
        let metrics = [
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic-private/configmaps/private-config",
                statusCode: 503,
                responseBytes: 8,
                durationSeconds: 0.020,
                attempt: 1,
                outcome: .httpError
            ),
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces/synthetic-private/services/private-service?synthetic-key=synthetic-value",
                statusCode: nil,
                responseBytes: 0,
                durationSeconds: 0.030,
                attempt: 1,
                outcome: .cancelled,
                cancellationReason: "task-cancelled"
            ),
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/nodes/synthetic-slow-node",
                statusCode: 200,
                responseBytes: 16,
                durationSeconds: 0.900,
                attempt: 1,
                outcome: .success
            ),
            KubernetesRESTRequestMetric(
                method: "GET",
                apiPath: "/api/v1/namespaces",
                statusCode: 200,
                responseBytes: 16,
                durationSeconds: 0.800,
                attempt: 1,
                outcome: .success
            )
        ]
        let report = KubernetesRESTRequestMetricsReport(
            metrics: metrics,
            summary: KubernetesRESTRequestMetricsSummary(
                requestCount: metrics.count,
                successCount: 2,
                failureCount: 1,
                cancelledCount: 1,
                responseBytes: 40,
                totalDurationSeconds: 1.75,
                retainedMetricCount: metrics.count
            )
        )

        let presentation = KubernetesRequestMetricsDebugPresentation(
            report: report,
            maxEndpointHighlights: 3
        )

        XCTAssertEqual(presentation.endpointHighlights.count, 3)
        XCTAssertEqual(presentation.endpointHighlights.map(\.apiPath), [
            "/api/v1/namespaces/<namespace>/services/<name>?<redacted>",
            "/api/v1/namespaces/<namespace>/configmaps/<name>",
            "/api/v1/nodes/<name>"
        ])
        XCTAssertEqual(presentation.endpointHighlights.map(\.hasIssues), [true, true, false])
        XCTAssertEqual(presentation.endpointHighlights.first?.outcomeText, "1 attempt • 1 cancelled")
        XCTAssertEqual(presentation.endpointHighlights.first?.maxDurationText, "max 30 ms")
        let rendered = presentation.endpointHighlights
            .map { "\($0.id)|\($0.method)|\($0.apiPath)|\($0.outcomeText)" }
            .joined(separator: "\n")
        XCTAssertFalse(rendered.contains("synthetic-private"))
        XCTAssertFalse(rendered.contains("synthetic-key"))
        XCTAssertFalse(rendered.contains("synthetic-value"))
    }

    func testProjectsRequestMetricsForSupportBundle() {
        let metrics = KubernetesRequestMetricsSupportBundleProjector.metrics(from: [
            KubernetesRESTRequestMetric(
                sourcePath: "swift-rest",
                method: "get",
                apiPath: "/api/v1/namespaces/synthetic/pods/pod-0?continue=token",
                statusCode: 200,
                responseBytes: 512,
                durationSeconds: 0.012,
                attempt: 2,
                outcome: .success,
                cancellationReason: nil
            )
        ])

        XCTAssertEqual(metrics.count, 1)
        XCTAssertEqual(metrics.first?.sourcePath, "swift-rest")
        XCTAssertEqual(metrics.first?.method, "GET")
        XCTAssertEqual(metrics.first?.apiPath, "/api/v1/namespaces/<namespace>/pods/<name>?continue=<redacted>")
        XCTAssertEqual(metrics.first?.statusCode, 200)
        XCTAssertEqual(metrics.first?.responseBytes, 512)
        XCTAssertEqual(metrics.first?.durationSeconds, 0.012)
        XCTAssertEqual(metrics.first?.attempt, 2)
        XCTAssertEqual(metrics.first?.outcome, "success")
        XCTAssertNil(metrics.first?.cancellationReason)
    }

    func testProjectsRequestMetricsSummaryForSupportBundle() {
        let summary = KubernetesRequestMetricsSupportBundleProjector.summary(from: KubernetesRESTRequestMetricsSummary(
            requestCount: 12,
            successCount: 8,
            failureCount: 3,
            cancelledCount: 1,
            responseBytes: 4_096,
            totalDurationSeconds: 1.25,
            retainedMetricCount: 5
        ))

        XCTAssertEqual(summary.requestCount, 12)
        XCTAssertEqual(summary.successCount, 8)
        XCTAssertEqual(summary.failureCount, 3)
        XCTAssertEqual(summary.cancelledCount, 1)
        XCTAssertEqual(summary.responseBytes, 4_096)
        XCTAssertEqual(summary.totalDurationSeconds, 1.25)
        XCTAssertEqual(summary.retainedMetricCount, 5)
        XCTAssertEqual(summary.omittedMetricCount, 7)
    }

    func testOlderSupportBundleMetricsSummaryInfersOmittedCount() throws {
        let data = Data("""
        {
          "requestCount": 12,
          "successCount": 8,
          "failureCount": 3,
          "cancelledCount": 1,
          "responseBytes": 4096,
          "totalDurationSeconds": 1.25,
          "retainedMetricCount": 5
        }
        """.utf8)

        let summary = try JSONDecoder().decode(SupportBundleRequestMetricsSummary.self, from: data)

        XCTAssertEqual(summary.omittedMetricCount, 7)
    }

    func testGroupsRequestMetricsBySanitizedEndpointForSupportBundle() throws {
        let groups = KubernetesRequestMetricsSupportBundleProjector.groups(from: [
            KubernetesRESTRequestMetric(
                sourcePath: "swift-rest",
                method: "get",
                apiPath: "/api/v1/namespaces/team-a/pods?continue=token-1",
                statusCode: 200,
                responseBytes: 256,
                durationSeconds: 0.010,
                attempt: 1,
                outcome: .success
            ),
            KubernetesRESTRequestMetric(
                sourcePath: "swift-rest",
                method: "GET",
                apiPath: "/api/v1/namespaces/team-b/pods?continue=token-2",
                statusCode: 503,
                responseBytes: 64,
                durationSeconds: 0.040,
                attempt: 2,
                outcome: .httpError
            ),
            KubernetesRESTRequestMetric(
                sourcePath: "swift-rest",
                method: "GET",
                apiPath: "/api/v1/namespaces/team-a/services/api-0",
                statusCode: nil,
                responseBytes: 0,
                durationSeconds: 0.030,
                attempt: 1,
                outcome: .cancelled,
                cancellationReason: "task-cancelled"
            )
        ])

        XCTAssertEqual(groups.count, 2)
        let podGroup = try XCTUnwrap(groups.first)
        XCTAssertEqual(podGroup.apiPath, "/api/v1/namespaces/<namespace>/pods?continue=<redacted>")
        XCTAssertEqual(podGroup.requestCount, 2)
        XCTAssertEqual(podGroup.successCount, 1)
        XCTAssertEqual(podGroup.failureCount, 1)
        XCTAssertEqual(podGroup.cancelledCount, 0)
        XCTAssertEqual(podGroup.responseBytes, 320)
        XCTAssertEqual(podGroup.totalDurationSeconds, 0.050, accuracy: 0.001)
        XCTAssertEqual(podGroup.maxDurationSeconds, 0.040, accuracy: 0.001)
        XCTAssertEqual(podGroup.latestStatusCode, 503)
        XCTAssertEqual(podGroup.latestOutcome, "httpError")
        let serviceGroup = try XCTUnwrap(groups.last)
        XCTAssertEqual(serviceGroup.apiPath, "/api/v1/namespaces/<namespace>/services/<name>")
        XCTAssertEqual(serviceGroup.cancelledCount, 1)
    }

    func testGroupsUseDeterministicSourceAndMethodTieBreakers() {
        let metrics = [
            KubernetesRESTRequestMetric(
                sourcePath: "swift-rest",
                method: "POST",
                apiPath: "/api/v1/nodes/synthetic-node",
                statusCode: 200,
                responseBytes: 1,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .success
            ),
            KubernetesRESTRequestMetric(
                sourcePath: "cli",
                method: "GET",
                apiPath: "/api/v1/nodes/synthetic-node",
                statusCode: 200,
                responseBytes: 1,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .success
            ),
            KubernetesRESTRequestMetric(
                sourcePath: "swift-rest",
                method: "GET",
                apiPath: "/api/v1/nodes/synthetic-node",
                statusCode: 200,
                responseBytes: 1,
                durationSeconds: 0.01,
                attempt: 1,
                outcome: .success
            )
        ]

        let groups = KubernetesRequestMetricsSupportBundleProjector.groups(from: metrics)

        XCTAssertEqual(groups.map { "\($0.sourcePath) \($0.method)" }, [
            "cli GET",
            "swift-rest GET",
            "swift-rest POST"
        ])
        XCTAssertTrue(groups.allSatisfy { $0.apiPath == "/api/v1/nodes/<name>" })
    }
}
