import Foundation
import RuneKube

public struct KubernetesRequestMetricEndpointDebugPresentation: Equatable, Sendable, Identifiable {
    public let id: String
    public let method: String
    public let apiPath: String
    public let outcomeText: String
    public let maxDurationText: String
    public let hasIssues: Bool
}

public struct KubernetesRequestMetricsDebugPresentation: Equatable, Sendable {
    public let requestCountText: String
    public let outcomeText: String
    public let transferText: String
    public let retainedText: String
    public let hasFailures: Bool
    public let endpointHighlights: [KubernetesRequestMetricEndpointDebugPresentation]

    public static let empty = KubernetesRequestMetricsDebugPresentation(
        requestCountText: "0 API attempts",
        outcomeText: "0 ok • 0 failed • 0 cancelled",
        transferText: "0 B • 0 ms total",
        retainedText: "0 retained",
        hasFailures: false,
        endpointHighlights: []
    )

    public init(summary: KubernetesRESTRequestMetricsSummary) {
        self.init(summary: summary, endpointHighlights: [])
    }

    public init(
        report: KubernetesRESTRequestMetricsReport,
        maxEndpointHighlights: Int = 3
    ) {
        let boundedLimit = min(5, max(0, maxEndpointHighlights))
        let reportGroups = report.endpointGroups.isEmpty
            ? KubernetesRequestMetricsSupportBundleProjector.groups(from: report.metrics)
            : KubernetesRequestMetricsSupportBundleProjector.groups(from: report.endpointGroups)
        let rankedGroups = reportGroups
            .sorted { lhs, rhs in
                let lhsIssueCount = lhs.failureCount + lhs.cancelledCount
                let rhsIssueCount = rhs.failureCount + rhs.cancelledCount
                if (lhsIssueCount > 0) != (rhsIssueCount > 0) {
                    return lhsIssueCount > 0
                }
                if lhsIssueCount != rhsIssueCount {
                    return lhsIssueCount > rhsIssueCount
                }
                if lhs.maxDurationSeconds != rhs.maxDurationSeconds {
                    return lhs.maxDurationSeconds > rhs.maxDurationSeconds
                }
                if lhs.requestCount != rhs.requestCount {
                    return lhs.requestCount > rhs.requestCount
                }
                if lhs.apiPath != rhs.apiPath {
                    return lhs.apiPath < rhs.apiPath
                }
                if lhs.method != rhs.method {
                    return lhs.method < rhs.method
                }
                return lhs.sourcePath < rhs.sourcePath
            }
            .prefix(boundedLimit)

        let highlights = rankedGroups.map { group in
            let issueCount = group.failureCount + group.cancelledCount
            let attemptLabel = group.requestCount == 1 ? "attempt" : "attempts"
            let outcomeText: String
            if issueCount > 0 {
                var components = ["\(group.requestCount) \(attemptLabel)"]
                if group.failureCount > 0 {
                    components.append("\(group.failureCount) failed")
                }
                if group.cancelledCount > 0 {
                    components.append("\(group.cancelledCount) cancelled")
                }
                outcomeText = components.joined(separator: " • ")
            } else {
                outcomeText = "\(group.requestCount) \(attemptLabel) • all ok"
            }
            return KubernetesRequestMetricEndpointDebugPresentation(
                id: "\(group.sourcePath)|\(group.method)|\(group.apiPath)",
                method: group.method,
                apiPath: group.apiPath,
                outcomeText: outcomeText,
                maxDurationText: "max \(Self.formatDuration(group.maxDurationSeconds))",
                hasIssues: issueCount > 0
            )
        }

        self.init(summary: report.summary, endpointHighlights: highlights)
    }

    private init(
        summary: KubernetesRESTRequestMetricsSummary,
        endpointHighlights: [KubernetesRequestMetricEndpointDebugPresentation]
    ) {
        requestCountText = "\(summary.requestCount) API attempt\(summary.requestCount == 1 ? "" : "s")"
        outcomeText = "\(summary.successCount) ok • \(summary.failureCount) failed • \(summary.cancelledCount) cancelled"
        transferText = "\(Self.formatBytes(summary.responseBytes)) • \(Self.formatDuration(summary.totalDurationSeconds)) total"
        if summary.omittedMetricCount > 0 {
            retainedText = "\(summary.retainedMetricCount) retained • \(summary.omittedMetricCount) omitted"
        } else {
            retainedText = "\(summary.retainedMetricCount) retained"
        }
        hasFailures = summary.failureCount > 0 || summary.cancelledCount > 0
        self.endpointHighlights = endpointHighlights
    }

    private init(
        requestCountText: String,
        outcomeText: String,
        transferText: String,
        retainedText: String,
        hasFailures: Bool,
        endpointHighlights: [KubernetesRequestMetricEndpointDebugPresentation]
    ) {
        self.requestCountText = requestCountText
        self.outcomeText = outcomeText
        self.transferText = transferText
        self.retainedText = retainedText
        self.hasFailures = hasFailures
        self.endpointHighlights = endpointHighlights
    }

    private static func formatBytes(_ bytes: Int) -> String {
        let value = max(0, bytes)
        if value < 1_024 {
            return "\(value) B"
        }
        if value < 1_024 * 1_024 {
            return String(format: "%.1f KB", Double(value) / 1_024.0)
        }
        return String(format: "%.1f MB", Double(value) / (1_024.0 * 1_024.0))
    }

    private static func formatDuration(_ seconds: Double) -> String {
        let clamped = max(0, seconds)
        if clamped < 1 {
            return "\(Int((clamped * 1_000).rounded())) ms"
        }
        return String(format: "%.2f s", clamped)
    }
}
