import Foundation
import RuneDiagnostics

public enum KubernetesRESTRequestMetricOutcome: String, Sendable, Equatable {
    case success
    case httpError
    case networkError
    case cancelled
}

public struct KubernetesRESTRequestMetric: Sendable, Equatable {
    public let sourcePath: String
    public let method: String
    public let apiPath: String
    public let statusCode: Int?
    public let responseBytes: Int
    public let durationSeconds: Double
    public let attempt: Int
    public let outcome: KubernetesRESTRequestMetricOutcome
    public let cancellationReason: String?

    public init(
        sourcePath: String = "swift-rest",
        method: String,
        apiPath: String,
        statusCode: Int?,
        responseBytes: Int,
        durationSeconds: Double,
        attempt: Int,
        outcome: KubernetesRESTRequestMetricOutcome,
        cancellationReason: String? = nil
    ) {
        self.sourcePath = sourcePath
        self.method = method.uppercased()
        self.apiPath = Self.sanitizedAPIPath(apiPath)
        self.statusCode = statusCode
        self.responseBytes = responseBytes
        self.durationSeconds = durationSeconds
        self.attempt = attempt
        self.outcome = outcome
        self.cancellationReason = cancellationReason
    }

    public static func sanitizedAPIPath(_ apiPath: String) -> String {
        KubernetesAPIPathSanitizer.sanitizedPath(apiPath)
    }
}

public struct KubernetesRESTRequestMetricsSummary: Sendable, Equatable {
    public let requestCount: Int
    public let successCount: Int
    public let failureCount: Int
    public let cancelledCount: Int
    public let responseBytes: Int
    public let totalDurationSeconds: Double
    public let retainedMetricCount: Int

    public var omittedMetricCount: Int {
        max(0, requestCount - retainedMetricCount)
    }
}

public struct KubernetesRESTRequestMetricsReport: Sendable, Equatable {
    public let metrics: [KubernetesRESTRequestMetric]
    public let summary: KubernetesRESTRequestMetricsSummary

    public static let empty = KubernetesRESTRequestMetricsReport(
        metrics: [],
        summary: KubernetesRESTRequestMetricsSummary(
            requestCount: 0,
            successCount: 0,
            failureCount: 0,
            cancelledCount: 0,
            responseBytes: 0,
            totalDurationSeconds: 0,
            retainedMetricCount: 0
        )
    )
}

actor KubernetesRESTRequestMetricsRecorder {
    private var retainedMetrics: RetainedRequestMetricBuffer
    private var allContextsAccumulator = RequestMetricsAccumulator()
    private var contextAccumulators: [String: RequestMetricsAccumulator] = [:]

    init(maxRetainedMetrics: Int = 1_000) {
        retainedMetrics = RetainedRequestMetricBuffer(capacity: max(1, maxRetainedMetrics))
    }

    func record(_ metric: KubernetesRESTRequestMetric) {
        record(metric, scopedContextName: nil)
    }

    func record(_ metric: KubernetesRESTRequestMetric, contextName: String) {
        record(metric, scopedContextName: contextName)
    }

    private func record(_ metric: KubernetesRESTRequestMetric, scopedContextName: String?) {
        allContextsAccumulator.record(metric)
        if let contextName = scopedContextName {
            contextAccumulators[contextName, default: RequestMetricsAccumulator()].record(metric)
        }
        retainedMetrics.append(RecordedRequestMetric(contextName: scopedContextName, metric: metric))
    }

    func snapshot() -> [KubernetesRESTRequestMetric] {
        retainedMetricsSnapshot(contextName: nil)
    }

    func snapshot(contextName: String) -> [KubernetesRESTRequestMetric] {
        retainedMetricsSnapshot(contextName: contextName)
    }

    func summary() -> KubernetesRESTRequestMetricsSummary {
        allContextsAccumulator.summary(retainedMetricCount: retainedMetrics.count)
    }

    func summary(contextName: String) -> KubernetesRESTRequestMetricsSummary {
        let retainedMetricCount = retainedMetrics.retainedCount(contextName: contextName)
        return accumulator(contextName: contextName).summary(retainedMetricCount: retainedMetricCount)
    }

    func report() -> KubernetesRESTRequestMetricsReport {
        makeReport(contextName: nil)
    }

    func report(contextName: String) -> KubernetesRESTRequestMetricsReport {
        makeReport(contextName: contextName)
    }

    private func makeReport(contextName: String?) -> KubernetesRESTRequestMetricsReport {
        let metrics = retainedMetricsSnapshot(contextName: contextName)
        return KubernetesRESTRequestMetricsReport(
            metrics: metrics,
            summary: accumulator(contextName: contextName).summary(retainedMetricCount: metrics.count)
        )
    }

    private func retainedMetricsSnapshot(contextName: String?) -> [KubernetesRESTRequestMetric] {
        retainedMetrics.elements.compactMap { entry in
            guard contextName == nil || entry.contextName == contextName else { return nil }
            return entry.metric
        }
    }

    private func accumulator(contextName: String?) -> RequestMetricsAccumulator {
        guard let contextName else { return allContextsAccumulator }
        return contextAccumulators[contextName] ?? RequestMetricsAccumulator()
    }
}

private struct RequestMetricsAccumulator {
    private var requestCount = 0
    private var successCount = 0
    private var cancelledCount = 0
    private var responseBytes = 0
    private var totalDurationSeconds = 0.0

    mutating func record(_ metric: KubernetesRESTRequestMetric) {
        requestCount += 1
        if metric.outcome == .success {
            successCount += 1
        }
        if metric.outcome == .cancelled {
            cancelledCount += 1
        }
        responseBytes += metric.responseBytes
        totalDurationSeconds += metric.durationSeconds
    }

    func summary(retainedMetricCount: Int) -> KubernetesRESTRequestMetricsSummary {
        KubernetesRESTRequestMetricsSummary(
            requestCount: requestCount,
            successCount: successCount,
            failureCount: requestCount - successCount - cancelledCount,
            cancelledCount: cancelledCount,
            responseBytes: responseBytes,
            totalDurationSeconds: totalDurationSeconds,
            retainedMetricCount: retainedMetricCount
        )
    }
}

private struct RecordedRequestMetric {
    let contextName: String?
    let metric: KubernetesRESTRequestMetric
}

private struct RetainedRequestMetricBuffer {
    private let capacity: Int
    private var storage: [RecordedRequestMetric] = []
    private var startIndex = 0

    init(capacity: Int) {
        self.capacity = max(1, capacity)
        storage.reserveCapacity(self.capacity)
    }

    var count: Int {
        storage.count
    }

    var elements: [RecordedRequestMetric] {
        guard storage.count == capacity, startIndex > 0 else { return storage }
        return Array(storage[startIndex...]) + Array(storage[..<startIndex])
    }

    func retainedCount(contextName: String?) -> Int {
        guard let contextName else { return count }
        return storage.lazy.filter { $0.contextName == contextName }.count
    }

    mutating func append(_ metric: RecordedRequestMetric) {
        guard storage.count == capacity else {
            storage.append(metric)
            return
        }

        storage[startIndex] = metric
        startIndex = (startIndex + 1) % capacity
    }
}
