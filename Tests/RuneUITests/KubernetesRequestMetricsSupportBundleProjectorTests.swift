import XCTest
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

        XCTAssertEqual(presentation.requestCountText, "12 requests")
        XCTAssertEqual(presentation.outcomeText, "8 ok • 3 failed • 1 cancelled")
        XCTAssertEqual(presentation.transferText, "4.0 KB • 1.25 s total")
        XCTAssertEqual(presentation.retainedText, "5 retained")
        XCTAssertTrue(presentation.hasFailures)
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
}
