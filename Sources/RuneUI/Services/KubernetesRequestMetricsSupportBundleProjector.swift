import Foundation
import RuneDiagnostics
import RuneKube

public enum KubernetesRequestMetricsSupportBundleProjector {
    public static func metrics(from metrics: [KubernetesRESTRequestMetric]) -> [SupportBundleRequestMetric] {
        metrics.map { metric in
            SupportBundleRequestMetric(
                sourcePath: metric.sourcePath,
                method: metric.method,
                apiPath: metric.apiPath,
                statusCode: metric.statusCode,
                responseBytes: metric.responseBytes,
                durationSeconds: metric.durationSeconds,
                attempt: metric.attempt,
                outcome: metric.outcome.rawValue,
                cancellationReason: metric.cancellationReason
            )
        }
    }

    public static func groups(from metrics: [KubernetesRESTRequestMetric]) -> [SupportBundleRequestMetricGroup] {
        var groups: [RequestMetricGroupKey: RequestMetricGroupAccumulator] = [:]
        groups.reserveCapacity(metrics.count)

        for metric in metrics {
            let key = RequestMetricGroupKey(
                sourcePath: metric.sourcePath,
                method: metric.method,
                apiPath: metric.apiPath
            )
            var accumulator = groups[key] ?? RequestMetricGroupAccumulator(key: key)
            accumulator.record(metric)
            groups[key] = accumulator
        }

        return groups.values
            .sorted { lhs, rhs in
                if lhs.maxDurationSeconds != rhs.maxDurationSeconds {
                    return lhs.maxDurationSeconds > rhs.maxDurationSeconds
                }
                if lhs.requestCount != rhs.requestCount {
                    return lhs.requestCount > rhs.requestCount
                }
                if lhs.key.apiPath != rhs.key.apiPath {
                    return lhs.key.apiPath < rhs.key.apiPath
                }
                if lhs.key.sourcePath != rhs.key.sourcePath {
                    return lhs.key.sourcePath < rhs.key.sourcePath
                }
                return lhs.key.method < rhs.key.method
            }
            .map(\.supportBundleGroup)
    }

    public static func groups(
        from groups: [KubernetesRESTRequestMetricGroup]
    ) -> [SupportBundleRequestMetricGroup] {
        groups.map { group in
            SupportBundleRequestMetricGroup(
                sourcePath: group.sourcePath,
                method: group.method,
                apiPath: group.apiPath,
                requestCount: group.requestCount,
                successCount: group.successCount,
                failureCount: group.failureCount,
                cancelledCount: group.cancelledCount,
                responseBytes: group.responseBytes,
                totalDurationSeconds: group.totalDurationSeconds,
                maxDurationSeconds: group.maxDurationSeconds,
                latestStatusCode: group.latestStatusCode,
                latestOutcome: group.latestOutcome.rawValue
            )
        }
    }

    public static func summary(from summary: KubernetesRESTRequestMetricsSummary) -> SupportBundleRequestMetricsSummary {
        SupportBundleRequestMetricsSummary(
            requestCount: summary.requestCount,
            successCount: summary.successCount,
            failureCount: summary.failureCount,
            cancelledCount: summary.cancelledCount,
            responseBytes: summary.responseBytes,
            totalDurationSeconds: summary.totalDurationSeconds,
            retainedMetricCount: summary.retainedMetricCount,
            omittedMetricCount: summary.omittedMetricCount
        )
    }

    private struct RequestMetricGroupKey: Hashable {
        let sourcePath: String
        let method: String
        let apiPath: String
    }

    private struct RequestMetricGroupAccumulator {
        let key: RequestMetricGroupKey
        var requestCount = 0
        var successCount = 0
        var cancelledCount = 0
        var responseBytes = 0
        var totalDurationSeconds = 0.0
        var maxDurationSeconds = 0.0
        var latestStatusCode: Int?
        var latestOutcome = ""

        mutating func record(_ metric: KubernetesRESTRequestMetric) {
            requestCount += 1
            switch metric.outcome {
            case .success:
                successCount += 1
            case .cancelled:
                cancelledCount += 1
            case .httpError, .networkError:
                break
            }
            responseBytes += metric.responseBytes
            totalDurationSeconds += metric.durationSeconds
            maxDurationSeconds = max(maxDurationSeconds, metric.durationSeconds)
            latestStatusCode = metric.statusCode
            latestOutcome = metric.outcome.rawValue
        }

        var supportBundleGroup: SupportBundleRequestMetricGroup {
            SupportBundleRequestMetricGroup(
                sourcePath: key.sourcePath,
                method: key.method,
                apiPath: key.apiPath,
                requestCount: requestCount,
                successCount: successCount,
                failureCount: requestCount - successCount - cancelledCount,
                cancelledCount: cancelledCount,
                responseBytes: responseBytes,
                totalDurationSeconds: totalDurationSeconds,
                maxDurationSeconds: maxDurationSeconds,
                latestStatusCode: latestStatusCode,
                latestOutcome: latestOutcome
            )
        }
    }
}
