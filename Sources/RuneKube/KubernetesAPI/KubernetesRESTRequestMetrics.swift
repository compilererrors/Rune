import Foundation

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
        let pieces = apiPath.split(separator: "?", maxSplits: 1, omittingEmptySubsequences: false)
        let redactedPath = redactedPathSegments(String(pieces.first ?? ""))
        guard pieces.count > 1 else { return redactedPath }

        let query = pieces[1]
            .split(separator: "&", omittingEmptySubsequences: false)
            .map { item -> String in
                let pair = item.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
                guard let name = pair.first, !name.isEmpty else { return "<redacted>" }
                return "\(name)=<redacted>"
            }
            .joined(separator: "&")
        return query.isEmpty ? redactedPath : "\(redactedPath)?\(query)"
    }

    private static func redactedPathSegments(_ path: String) -> String {
        let hasLeadingSlash = path.hasPrefix("/")
        var segments = path.split(separator: "/", omittingEmptySubsequences: true).map(String.init)

        if let namespaceIndex = segments.firstIndex(of: "namespaces"),
           namespaceIndex + 1 < segments.count {
            segments[namespaceIndex + 1] = "<namespace>"
        }

        if let nameIndex = objectNameIndex(in: segments) {
            segments[nameIndex] = "<name>"
        }

        let joined = segments.joined(separator: "/")
        return hasLeadingSlash ? "/" + joined : joined
    }

    private static func objectNameIndex(in segments: [String]) -> Int? {
        if let namespaceIndex = segments.firstIndex(of: "namespaces") {
            let resourceIndex = namespaceIndex + 2
            let nameIndex = resourceIndex + 1
            return nameIndex < segments.count ? nameIndex : nil
        }

        if segments.first == "api", segments.count >= 5 {
            return 4
        }

        if segments.first == "apis", segments.count >= 6 {
            return 5
        }

        return nil
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
}

actor KubernetesRESTRequestMetricsRecorder {
    private var retainedMetrics: RetainedRequestMetricBuffer
    private var totalRequestCount = 0
    private var totalSuccessCount = 0
    private var totalCancelledCount = 0
    private var totalResponseBytes = 0
    private var totalDurationSeconds = 0.0

    init(maxRetainedMetrics: Int = 1_000) {
        retainedMetrics = RetainedRequestMetricBuffer(capacity: max(1, maxRetainedMetrics))
    }

    func record(_ metric: KubernetesRESTRequestMetric) {
        totalRequestCount += 1
        if metric.outcome == .success {
            totalSuccessCount += 1
        }
        if metric.outcome == .cancelled {
            totalCancelledCount += 1
        }
        totalResponseBytes += metric.responseBytes
        totalDurationSeconds += metric.durationSeconds

        retainedMetrics.append(metric)
    }

    func snapshot() -> [KubernetesRESTRequestMetric] {
        retainedMetrics.elements
    }

    func summary() -> KubernetesRESTRequestMetricsSummary {
        return KubernetesRESTRequestMetricsSummary(
            requestCount: totalRequestCount,
            successCount: totalSuccessCount,
            failureCount: totalRequestCount - totalSuccessCount - totalCancelledCount,
            cancelledCount: totalCancelledCount,
            responseBytes: totalResponseBytes,
            totalDurationSeconds: totalDurationSeconds,
            retainedMetricCount: retainedMetrics.count
        )
    }
}

private struct RetainedRequestMetricBuffer {
    private let capacity: Int
    private var storage: [KubernetesRESTRequestMetric] = []
    private var startIndex = 0

    init(capacity: Int) {
        self.capacity = max(1, capacity)
        storage.reserveCapacity(self.capacity)
    }

    var count: Int {
        storage.count
    }

    var elements: [KubernetesRESTRequestMetric] {
        guard storage.count == capacity, startIndex > 0 else { return storage }
        return Array(storage[startIndex...]) + Array(storage[..<startIndex])
    }

    mutating func append(_ metric: KubernetesRESTRequestMetric) {
        guard storage.count == capacity else {
            storage.append(metric)
            return
        }

        storage[startIndex] = metric
        startIndex = (startIndex + 1) % capacity
    }
}
