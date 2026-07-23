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

public struct KubernetesRESTRequestMetricGroup: Sendable, Equatable {
    public let sourcePath: String
    public let method: String
    public let apiPath: String
    public let requestCount: Int
    public let successCount: Int
    public let failureCount: Int
    public let cancelledCount: Int
    public let responseBytes: Int
    public let totalDurationSeconds: Double
    public let maxDurationSeconds: Double
    public let latestStatusCode: Int?
    public let latestOutcome: KubernetesRESTRequestMetricOutcome

    public init(
        sourcePath: String,
        method: String,
        apiPath: String,
        requestCount: Int,
        successCount: Int,
        failureCount: Int,
        cancelledCount: Int,
        responseBytes: Int,
        totalDurationSeconds: Double,
        maxDurationSeconds: Double,
        latestStatusCode: Int?,
        latestOutcome: KubernetesRESTRequestMetricOutcome
    ) {
        self.sourcePath = sourcePath
        self.method = method.uppercased()
        self.apiPath = KubernetesRESTRequestMetric.sanitizedAPIPath(apiPath)
        self.requestCount = requestCount
        self.successCount = successCount
        self.failureCount = failureCount
        self.cancelledCount = cancelledCount
        self.responseBytes = responseBytes
        self.totalDurationSeconds = totalDurationSeconds
        self.maxDurationSeconds = maxDurationSeconds
        self.latestStatusCode = latestStatusCode
        self.latestOutcome = latestOutcome
    }
}

public struct KubernetesRESTRequestMetricsReport: Sendable, Equatable {
    public let metrics: [KubernetesRESTRequestMetric]
    public let summary: KubernetesRESTRequestMetricsSummary
    public let endpointGroups: [KubernetesRESTRequestMetricGroup]

    public init(
        metrics: [KubernetesRESTRequestMetric],
        summary: KubernetesRESTRequestMetricsSummary,
        endpointGroups: [KubernetesRESTRequestMetricGroup] = []
    ) {
        self.metrics = metrics
        self.summary = summary
        self.endpointGroups = endpointGroups
    }

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
        ),
        endpointGroups: []
    )
}

actor KubernetesRESTRequestMetricsRecorder {
    private var retainedMetrics: RetainedRequestMetricBuffer
    private var allContextsAccumulator: RequestMetricsAccumulator
    private var contextAccumulators: [KubernetesRESTRequestMetricsScopeToken: RequestMetricsAccumulator] = [:]
    private var activeScopes: [RequestMetricsScopeDescriptor: KubernetesRESTRequestMetricsScopeToken] = [:]
    private var latestScopeByContextName: [String: KubernetesRESTRequestMetricsScopeToken] = [:]
    private var scopeRecency: [KubernetesRESTRequestMetricsScopeToken] = []
    private var nextScopeGeneration: UInt64 = 0
    private let maxContextAccumulators: Int
    private let maxEndpointGroupsPerAccumulator: Int

    init(
        maxRetainedMetrics: Int = 1_000,
        maxContextAccumulators: Int = 32,
        maxEndpointGroupsPerAccumulator: Int = 64
    ) {
        let endpointGroupCapacity = max(1, maxEndpointGroupsPerAccumulator)
        retainedMetrics = RetainedRequestMetricBuffer(capacity: max(1, maxRetainedMetrics))
        allContextsAccumulator = RequestMetricsAccumulator(maxEndpointGroups: endpointGroupCapacity)
        self.maxContextAccumulators = max(1, maxContextAccumulators)
        self.maxEndpointGroupsPerAccumulator = endpointGroupCapacity
    }

    func record(_ metric: KubernetesRESTRequestMetric) {
        allContextsAccumulator.record(metric)
        retainedMetrics.append(RecordedRequestMetric(scope: nil, metric: metric))
    }

    func record(_ metric: KubernetesRESTRequestMetric, contextName: String) {
        record(metric, contextName: contextName, scopeIdentity: "context-name-only")
    }

    func record(
        _ metric: KubernetesRESTRequestMetric,
        contextName: String,
        scopeIdentity: String
    ) {
        let scope = activateScope(contextName: contextName, scopeIdentity: scopeIdentity)
        record(metric, scope: scope)
    }

    func activateScope(
        contextName: String,
        scopeIdentity: String,
        requestGeneration: UInt64? = nil
    ) -> KubernetesRESTRequestMetricsScopeToken {
        let descriptor = RequestMetricsScopeDescriptor(
            contextName: contextName,
            identity: scopeIdentity
        )
        if let latest = latestScopeByContextName[contextName],
           latest.descriptor == descriptor,
           contextAccumulators[latest] != nil {
            touchScope(latest)
            return latest
        }

        let generation: UInt64
        if let requestGeneration {
            generation = requestGeneration
            nextScopeGeneration = max(nextScopeGeneration, requestGeneration)
        } else {
            generation = reserveScopeGeneration()
        }
        let scope = KubernetesRESTRequestMetricsScopeToken(
            descriptor: descriptor,
            generation: generation
        )

        if let latest = latestScopeByContextName[contextName],
           latest.generation > generation {
            // A request that started under an older kubeconfig may finish resolving
            // credentials after a newer request. Keep its API attempt in the global
            // report without reviving the stale context scope or its LRU entry.
            return scope
        }

        activeScopes[descriptor] = scope
        contextAccumulators[scope] = RequestMetricsAccumulator(
            maxEndpointGroups: maxEndpointGroupsPerAccumulator
        )
        promoteScopeIfCurrentOrNewer(scope)
        touchScope(scope)
        evictScopesIfNeeded()
        return scope
    }

    func reserveScopeGeneration() -> UInt64 {
        nextScopeGeneration &+= 1
        return nextScopeGeneration
    }

    func record(
        _ metric: KubernetesRESTRequestMetric,
        scope: KubernetesRESTRequestMetricsScopeToken
    ) {
        allContextsAccumulator.record(metric)
        if contextAccumulators[scope] != nil {
            contextAccumulators[scope]?.record(metric)
            if latestScopeByContextName[scope.descriptor.contextName] == scope {
                touchScope(scope)
            }
        }
        retainedMetrics.append(RecordedRequestMetric(scope: scope, metric: metric))
    }

    func summary() -> KubernetesRESTRequestMetricsSummary {
        allContextsAccumulator.summary(retainedMetricCount: retainedMetrics.count)
    }

    func summary(contextName: String) -> KubernetesRESTRequestMetricsSummary {
        guard let scope = latestScopeByContextName[contextName],
              let accumulator = contextAccumulators[scope] else {
            return RequestMetricsAccumulator.emptySummary
        }
        let retainedMetricCount = retainedMetrics.retainedCount(scope: scope)
        return accumulator.summary(retainedMetricCount: retainedMetricCount)
    }

    func report() -> KubernetesRESTRequestMetricsReport {
        makeReport(contextName: nil)
    }

    func report(contextName: String) -> KubernetesRESTRequestMetricsReport {
        makeReport(contextName: contextName)
    }

    func report(
        contextName: String,
        scopeIdentity: String
    ) -> KubernetesRESTRequestMetricsReport {
        let descriptor = RequestMetricsScopeDescriptor(
            contextName: contextName,
            identity: scopeIdentity
        )
        guard let scope = activeScopes[descriptor],
              let accumulator = contextAccumulators[scope] else {
            return .empty
        }
        let metrics = retainedMetricsSnapshot(scope: scope)
        return KubernetesRESTRequestMetricsReport(
            metrics: metrics,
            summary: accumulator.summary(retainedMetricCount: metrics.count),
            endpointGroups: accumulator.endpointGroups
        )
    }

    private func makeReport(contextName: String?) -> KubernetesRESTRequestMetricsReport {
        let scope = contextName.flatMap { latestScopeByContextName[$0] }
        guard contextName == nil || scope != nil else { return .empty }
        let metrics = retainedMetricsSnapshot(scope: scope)
        let accumulator: RequestMetricsAccumulator
        if let scope {
            guard let scopedAccumulator = contextAccumulators[scope] else { return .empty }
            accumulator = scopedAccumulator
        } else {
            accumulator = allContextsAccumulator
        }
        return KubernetesRESTRequestMetricsReport(
            metrics: metrics,
            summary: accumulator.summary(retainedMetricCount: metrics.count),
            endpointGroups: accumulator.endpointGroups
        )
    }

    private func retainedMetricsSnapshot(
        scope: KubernetesRESTRequestMetricsScopeToken?
    ) -> [KubernetesRESTRequestMetric] {
        retainedMetrics.elements.compactMap { entry in
            guard scope == nil || entry.scope == scope else { return nil }
            return entry.metric
        }
    }

    private func touchScope(_ scope: KubernetesRESTRequestMetricsScopeToken) {
        scopeRecency.removeAll { $0 == scope }
        scopeRecency.append(scope)
    }

    private func promoteScopeIfCurrentOrNewer(
        _ scope: KubernetesRESTRequestMetricsScopeToken
    ) {
        let contextName = scope.descriptor.contextName
        guard let current = latestScopeByContextName[contextName] else {
            latestScopeByContextName[contextName] = scope
            return
        }
        guard scope.generation >= current.generation else { return }
        latestScopeByContextName[contextName] = scope
    }

    private func evictScopesIfNeeded() {
        while contextAccumulators.count > maxContextAccumulators,
              !scopeRecency.isEmpty {
            let evicted = scopeRecency.removeFirst()
            contextAccumulators.removeValue(forKey: evicted)
            if activeScopes[evicted.descriptor] == evicted {
                activeScopes.removeValue(forKey: evicted.descriptor)
            }
            if latestScopeByContextName[evicted.descriptor.contextName] == evicted {
                latestScopeByContextName.removeValue(forKey: evicted.descriptor.contextName)
            }
        }
    }

    func _testContextAccumulatorCount() -> Int {
        contextAccumulators.count
    }
}

private struct RequestMetricsAccumulator {
    static let emptySummary = KubernetesRESTRequestMetricsSummary(
        requestCount: 0,
        successCount: 0,
        failureCount: 0,
        cancelledCount: 0,
        responseBytes: 0,
        totalDurationSeconds: 0,
        retainedMetricCount: 0
    )

    private var requestCount = 0
    private var successCount = 0
    private var cancelledCount = 0
    private var responseBytes = 0
    private var totalDurationSeconds = 0.0
    private var groups: [RequestMetricEndpointKey: RequestMetricGroupAccumulator] = [:]
    private var overflowGroup: RequestMetricGroupAccumulator?
    private let maxEndpointGroups: Int

    init(maxEndpointGroups: Int) {
        self.maxEndpointGroups = max(1, maxEndpointGroups)
        groups.reserveCapacity(self.maxEndpointGroups)
    }

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
        recordEndpointGroup(metric)
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

    var endpointGroups: [KubernetesRESTRequestMetricGroup] {
        var result = groups.values.map(\.metricGroup)
        if let overflowGroup {
            result.append(overflowGroup.metricGroup)
        }
        return result.sorted { lhs, rhs in
            if lhs.apiPath != rhs.apiPath {
                return lhs.apiPath < rhs.apiPath
            }
            if lhs.method != rhs.method {
                return lhs.method < rhs.method
            }
            return lhs.sourcePath < rhs.sourcePath
        }
    }

    private mutating func recordEndpointGroup(_ metric: KubernetesRESTRequestMetric) {
        let key = RequestMetricEndpointKey(
            sourcePath: metric.sourcePath,
            method: metric.method,
            apiPath: metric.apiPath
        )
        if var group = groups[key] {
            group.record(metric)
            groups[key] = group
            return
        }

        let specificGroupCapacity = max(0, maxEndpointGroups - 1)
        if groups.count < specificGroupCapacity {
            var group = RequestMetricGroupAccumulator(key: key)
            group.record(metric)
            groups[key] = group
            return
        }

        var overflow = overflowGroup ?? RequestMetricGroupAccumulator(
            key: .overflow
        )
        overflow.record(metric)
        overflowGroup = overflow
    }
}

private struct RequestMetricEndpointKey: Hashable {
    let sourcePath: String
    let method: String
    let apiPath: String

    static let overflow = RequestMetricEndpointKey(
        sourcePath: "aggregated",
        method: "*",
        apiPath: "/<other-endpoints>"
    )
}

private struct RequestMetricGroupAccumulator {
    let key: RequestMetricEndpointKey
    private var requestCount = 0
    private var successCount = 0
    private var cancelledCount = 0
    private var responseBytes = 0
    private var totalDurationSeconds = 0.0
    private var maxDurationSeconds = 0.0
    private var latestStatusCode: Int?
    private var latestOutcome: KubernetesRESTRequestMetricOutcome = .success

    init(key: RequestMetricEndpointKey) {
        self.key = key
    }

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
        maxDurationSeconds = max(maxDurationSeconds, metric.durationSeconds)
        latestStatusCode = metric.statusCode
        latestOutcome = metric.outcome
    }

    var metricGroup: KubernetesRESTRequestMetricGroup {
        KubernetesRESTRequestMetricGroup(
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

struct RequestMetricsScopeDescriptor: Hashable, Sendable {
    let contextName: String
    let identity: String
}

struct KubernetesRESTRequestMetricsScopeToken: Hashable, Sendable {
    let descriptor: RequestMetricsScopeDescriptor
    let generation: UInt64
}

private struct RecordedRequestMetric {
    let scope: KubernetesRESTRequestMetricsScopeToken?
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

    func retainedCount(scope: KubernetesRESTRequestMetricsScopeToken) -> Int {
        storage.lazy.filter { $0.scope == scope }.count
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
