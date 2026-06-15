import Foundation
import RuneKube

public struct KubernetesRequestMetricsDebugPresentation: Equatable, Sendable {
    public let requestCountText: String
    public let outcomeText: String
    public let transferText: String
    public let retainedText: String
    public let hasFailures: Bool

    public static let empty = KubernetesRequestMetricsDebugPresentation(
        requestCountText: "0 requests",
        outcomeText: "0 ok • 0 failed • 0 cancelled",
        transferText: "0 B • 0 ms total",
        retainedText: "0 retained",
        hasFailures: false
    )

    public init(summary: KubernetesRESTRequestMetricsSummary) {
        requestCountText = "\(summary.requestCount) request\(summary.requestCount == 1 ? "" : "s")"
        outcomeText = "\(summary.successCount) ok • \(summary.failureCount) failed • \(summary.cancelledCount) cancelled"
        transferText = "\(Self.formatBytes(summary.responseBytes)) • \(Self.formatDuration(summary.totalDurationSeconds)) total"
        retainedText = "\(summary.retainedMetricCount) retained"
        hasFailures = summary.failureCount > 0 || summary.cancelledCount > 0
    }

    private init(
        requestCountText: String,
        outcomeText: String,
        transferText: String,
        retainedText: String,
        hasFailures: Bool
    ) {
        self.requestCountText = requestCountText
        self.outcomeText = outcomeText
        self.transferText = transferText
        self.retainedText = retainedText
        self.hasFailures = hasFailures
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
