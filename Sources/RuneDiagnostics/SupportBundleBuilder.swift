import Foundation
import RuneCore

public struct SupportBundleRequest: Codable, Sendable {
    public let generatedAt: String
    public let contextName: String?
    public let namespace: String
    public let sectionTitle: String
    public let readOnlyMode: Bool
    public let resourceCounts: [String: Int]
    public let selectedResourceKind: String?
    public let selectedResourceName: String?
    public let resourceYAML: String
    public let resourceDescribe: String
    public let podLogs: String
    public let unifiedLogs: String
    public let unifiedLogPods: [String]
    public let deploymentRolloutHistory: String
    public let recentEvents: [EventSummary]
    public let portForwardSessions: [PortForwardSession]
    public let lastExecResult: PodExecResult?
    public let authDoctorChecks: [RuneHealthCheck]
    public let writeAuditLog: [WriteAuditEntry]
    public let requestMetrics: [SupportBundleRequestMetric]
    public let requestMetricsSummary: SupportBundleRequestMetricsSummary?
    public let requestMetricGroups: [SupportBundleRequestMetricGroup]
    public let resourceListFreshness: [SupportBundleResourceListFreshness]

    private enum CodingKeys: String, CodingKey {
        case generatedAt
        case contextName
        case namespace
        case sectionTitle
        case readOnlyMode
        case resourceCounts
        case selectedResourceKind
        case selectedResourceName
        case resourceYAML
        case resourceDescribe
        case podLogs
        case unifiedLogs
        case unifiedLogPods
        case deploymentRolloutHistory
        case recentEvents
        case portForwardSessions
        case lastExecResult
        case authDoctorChecks
        case writeAuditLog
        case requestMetrics
        case requestMetricsSummary
        case requestMetricGroups
        case resourceListFreshness
    }

    public init(
        generatedAt: String,
        contextName: String?,
        namespace: String,
        sectionTitle: String,
        readOnlyMode: Bool,
        resourceCounts: [String: Int],
        selectedResourceKind: String?,
        selectedResourceName: String?,
        resourceYAML: String,
        resourceDescribe: String,
        podLogs: String,
        unifiedLogs: String,
        unifiedLogPods: [String],
        deploymentRolloutHistory: String,
        recentEvents: [EventSummary],
        portForwardSessions: [PortForwardSession],
        lastExecResult: PodExecResult?,
        authDoctorChecks: [RuneHealthCheck],
        writeAuditLog: [WriteAuditEntry],
        requestMetrics: [SupportBundleRequestMetric] = [],
        requestMetricsSummary: SupportBundleRequestMetricsSummary? = nil,
        requestMetricGroups: [SupportBundleRequestMetricGroup] = [],
        resourceListFreshness: [SupportBundleResourceListFreshness] = []
    ) {
        self.generatedAt = generatedAt
        self.contextName = contextName
        self.namespace = namespace
        self.sectionTitle = sectionTitle
        self.readOnlyMode = readOnlyMode
        self.resourceCounts = resourceCounts
        self.selectedResourceKind = selectedResourceKind
        self.selectedResourceName = selectedResourceName
        self.resourceYAML = resourceYAML
        self.resourceDescribe = resourceDescribe
        self.podLogs = podLogs
        self.unifiedLogs = unifiedLogs
        self.unifiedLogPods = unifiedLogPods
        self.deploymentRolloutHistory = deploymentRolloutHistory
        self.recentEvents = recentEvents
        self.portForwardSessions = portForwardSessions
        self.lastExecResult = lastExecResult
        self.authDoctorChecks = authDoctorChecks
        self.writeAuditLog = writeAuditLog
        self.requestMetrics = requestMetrics
        self.requestMetricsSummary = requestMetricsSummary
        self.requestMetricGroups = requestMetricGroups
        self.resourceListFreshness = resourceListFreshness
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        generatedAt = try container.decode(String.self, forKey: .generatedAt)
        contextName = try container.decodeIfPresent(String.self, forKey: .contextName)
        namespace = try container.decode(String.self, forKey: .namespace)
        sectionTitle = try container.decode(String.self, forKey: .sectionTitle)
        readOnlyMode = try container.decode(Bool.self, forKey: .readOnlyMode)
        resourceCounts = try container.decode([String: Int].self, forKey: .resourceCounts)
        selectedResourceKind = try container.decodeIfPresent(String.self, forKey: .selectedResourceKind)
        selectedResourceName = try container.decodeIfPresent(String.self, forKey: .selectedResourceName)
        resourceYAML = try container.decode(String.self, forKey: .resourceYAML)
        resourceDescribe = try container.decode(String.self, forKey: .resourceDescribe)
        podLogs = try container.decode(String.self, forKey: .podLogs)
        unifiedLogs = try container.decode(String.self, forKey: .unifiedLogs)
        unifiedLogPods = try container.decode([String].self, forKey: .unifiedLogPods)
        deploymentRolloutHistory = try container.decode(String.self, forKey: .deploymentRolloutHistory)
        recentEvents = try container.decode([EventSummary].self, forKey: .recentEvents)
        portForwardSessions = try container.decode([PortForwardSession].self, forKey: .portForwardSessions)
        lastExecResult = try container.decodeIfPresent(PodExecResult.self, forKey: .lastExecResult)
        authDoctorChecks = try container.decode([RuneHealthCheck].self, forKey: .authDoctorChecks)
        writeAuditLog = try container.decode([WriteAuditEntry].self, forKey: .writeAuditLog)
        requestMetrics = try container.decodeIfPresent([SupportBundleRequestMetric].self, forKey: .requestMetrics) ?? []
        requestMetricsSummary = try container.decodeIfPresent(SupportBundleRequestMetricsSummary.self, forKey: .requestMetricsSummary)
        requestMetricGroups = try container.decodeIfPresent([SupportBundleRequestMetricGroup].self, forKey: .requestMetricGroups) ?? []
        resourceListFreshness = try container.decodeIfPresent([SupportBundleResourceListFreshness].self, forKey: .resourceListFreshness) ?? []
    }
}

public struct SupportBundleResourceListFreshness: Codable, Sendable, Equatable {
    public let family: String
    public let status: String
    public let updatedAt: Date?
    public let message: String

    public init(family: String, status: String, updatedAt: Date?, message: String) {
        self.family = family
        self.status = status
        self.updatedAt = updatedAt
        self.message = message
    }
}

public struct SupportBundleRequestMetricsSummary: Codable, Sendable, Equatable {
    public let requestCount: Int
    public let successCount: Int
    public let failureCount: Int
    public let cancelledCount: Int
    public let responseBytes: Int
    public let totalDurationSeconds: Double
    public let retainedMetricCount: Int

    public init(
        requestCount: Int,
        successCount: Int,
        failureCount: Int,
        cancelledCount: Int,
        responseBytes: Int,
        totalDurationSeconds: Double,
        retainedMetricCount: Int
    ) {
        self.requestCount = requestCount
        self.successCount = successCount
        self.failureCount = failureCount
        self.cancelledCount = cancelledCount
        self.responseBytes = responseBytes
        self.totalDurationSeconds = totalDurationSeconds
        self.retainedMetricCount = retainedMetricCount
    }
}

public struct SupportBundleRequestMetric: Codable, Sendable, Equatable {
    public let sourcePath: String
    public let method: String
    public let apiPath: String
    public let statusCode: Int?
    public let responseBytes: Int
    public let durationSeconds: Double
    public let attempt: Int
    public let outcome: String
    public let cancellationReason: String?

    public init(
        sourcePath: String,
        method: String,
        apiPath: String,
        statusCode: Int?,
        responseBytes: Int,
        durationSeconds: Double,
        attempt: Int,
        outcome: String,
        cancellationReason: String?
    ) {
        self.sourcePath = sourcePath
        self.method = method
        self.apiPath = apiPath
        self.statusCode = statusCode
        self.responseBytes = responseBytes
        self.durationSeconds = durationSeconds
        self.attempt = attempt
        self.outcome = outcome
        self.cancellationReason = cancellationReason
    }
}

public struct SupportBundleRequestMetricGroup: Codable, Sendable, Equatable {
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
    public let latestOutcome: String

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
        latestOutcome: String
    ) {
        self.sourcePath = sourcePath
        self.method = method
        self.apiPath = apiPath
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

public protocol SupportBundleBuilding: Sendable {
    func buildBundle(from request: SupportBundleRequest) throws -> Data
}

public struct JSONSupportBundleBuilder: SupportBundleBuilding {
    public init() {}

    public func buildBundle(from request: SupportBundleRequest) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        return try encoder.encode(request)
    }
}

public extension SupportBundleRequest {
    /// Builds a snapshot from current app state. **Add new persisted fields here** when `SupportBundleRequest` or `RuneAppState` grows, so call sites stay a single line.
    @MainActor
    static func snapshot(
        state: RuneAppState,
        generatedAt: String,
        resourceCounts: [String: Int],
        selectedResourceKind: String?,
        selectedResourceName: String?,
        requestMetrics: [SupportBundleRequestMetric] = [],
        requestMetricsSummary: SupportBundleRequestMetricsSummary? = nil,
        requestMetricGroups: [SupportBundleRequestMetricGroup] = []
    ) -> SupportBundleRequest {
        let sanitizer = SupportBundleSanitizer(redactedIdentifiers: [state.selectedContext?.name].compactMap { $0 })

        return SupportBundleRequest(
            generatedAt: generatedAt,
            contextName: state.selectedContext.map { sanitizer.sanitizedContextName($0.name) },
            namespace: sanitizer.sanitizedText(state.selectedNamespace),
            sectionTitle: state.selectedSection.title,
            readOnlyMode: state.isReadOnlyMode,
            resourceCounts: resourceCounts,
            selectedResourceKind: selectedResourceKind.map(sanitizer.sanitizedText),
            selectedResourceName: selectedResourceName.map(sanitizer.sanitizedText),
            resourceYAML: sanitizer.sanitizedText(state.resourceYAML),
            resourceDescribe: sanitizer.sanitizedText(state.resourceDescribe),
            podLogs: sanitizer.sanitizedText(state.podLogs),
            unifiedLogs: sanitizer.sanitizedText(state.unifiedServiceLogs),
            unifiedLogPods: state.unifiedServiceLogPods.map(sanitizer.sanitizedText),
            deploymentRolloutHistory: sanitizer.sanitizedText(state.deploymentRolloutHistory),
            recentEvents: Array(state.events.prefix(25)).map(sanitizer.sanitizedEvent),
            portForwardSessions: state.portForwardSessions.map(sanitizer.sanitizedPortForwardSession),
            lastExecResult: state.lastExecResult.map(sanitizer.sanitizedPodExecResult),
            authDoctorChecks: state.authDoctorChecks.map(sanitizer.sanitizedAuthDoctorCheck),
            writeAuditLog: state.writeAuditLog.map(sanitizer.sanitizedWriteAuditEntry),
            requestMetrics: requestMetrics.map(sanitizer.sanitizedRequestMetric),
            requestMetricsSummary: requestMetricsSummary,
            requestMetricGroups: requestMetricGroups.map(sanitizer.sanitizedRequestMetricGroup),
            resourceListFreshness: state.resourceListFreshness
                .sorted { $0.key.rawValue < $1.key.rawValue }
                .map { sanitizer.sanitizedResourceListFreshness($0, selectedNamespace: state.selectedNamespace) }
        )
    }

    private struct SupportBundleSanitizer {
        let redactedIdentifiers: [String]

        private static let sensitiveAssignmentRegex = try! NSRegularExpression(
            pattern: #"(?i)\b(client-certificate-data|client-key-data|refresh-token|access-token|id-token|password|token)\s*:\s*[^ \n\r\t,;]+"#
        )
        private static let sensitiveArgumentRegex = try! NSRegularExpression(
            pattern: #"(?i)\b(client-certificate-data|client-key-data|refresh-token|access-token|id-token|password|token)\s+[^ \n\r\t,;]+"#
        )
        private static let pathTrimCharacters = CharacterSet(charactersIn: ".,;:()[]{}\"'")

        private let sensitiveKeys: Set<String> = [
            "authorization",
            "token",
            "id-token",
            "refresh-token",
            "access-token",
            "password",
            "username",
            "client-certificate-data",
            "client-key-data",
            "certificate-authority-data",
            "client-certificate",
            "client-key",
            "certificate-authority"
        ]

        func sanitizedContextName(_ name: String) -> String {
            redactedIdentifiers.contains(name) ? "<context-name>" : sanitizedText(name)
        }

        func sanitizedText(_ text: String) -> String {
            guard !text.isEmpty else { return text }

            var sanitized = text
            for identifier in redactedIdentifiers where !identifier.isEmpty {
                sanitized = sanitized.replacingOccurrences(of: identifier, with: "<context-name>")
            }

            sanitized = Self.sensitiveAssignmentRegex.stringByReplacingMatches(
                in: sanitized,
                range: NSRange(sanitized.startIndex..., in: sanitized),
                withTemplate: "$1: <redacted>"
            )
            sanitized = Self.sensitiveArgumentRegex.stringByReplacingMatches(
                in: sanitized,
                range: NSRange(sanitized.startIndex..., in: sanitized),
                withTemplate: "$1 <redacted>"
            )

            return sanitized
                .split(separator: "\n", omittingEmptySubsequences: false)
                .map { sanitizedLine(String($0)) }
                .joined(separator: "\n")
        }

        func sanitizedEvent(_ event: EventSummary) -> EventSummary {
            EventSummary(
                type: sanitizedText(event.type),
                reason: sanitizedText(event.reason),
                objectName: sanitizedText(event.objectName),
                message: sanitizedText(event.message),
                lastTimestamp: event.lastTimestamp,
                involvedKind: event.involvedKind.map(sanitizedText),
                involvedNamespace: event.involvedNamespace.map(sanitizedText)
            )
        }

        func sanitizedPortForwardSession(_ session: PortForwardSession) -> PortForwardSession {
            PortForwardSession(
                id: session.id,
                contextName: sanitizedContextName(session.contextName),
                namespace: sanitizedText(session.namespace),
                targetKind: session.targetKind,
                targetName: sanitizedText(session.targetName),
                localPort: session.localPort,
                remotePort: session.remotePort,
                address: session.address,
                status: session.status,
                lastMessage: sanitizedText(session.lastMessage)
            )
        }

        func sanitizedPodExecResult(_ result: PodExecResult) -> PodExecResult {
            PodExecResult(
                podName: sanitizedText(result.podName),
                namespace: sanitizedText(result.namespace),
                command: result.command.map(sanitizedText),
                stdout: sanitizedText(result.stdout),
                stderr: sanitizedText(result.stderr),
                exitCode: result.exitCode
            )
        }

        func sanitizedAuthDoctorCheck(_ check: RuneHealthCheck) -> RuneHealthCheck {
            RuneHealthCheck(
                id: check.id,
                title: sanitizedText(check.title),
                status: check.status,
                message: sanitizedText(check.message)
            )
        }

        func sanitizedWriteAuditEntry(_ entry: WriteAuditEntry) -> WriteAuditEntry {
            WriteAuditEntry(
                id: entry.id,
                timestamp: entry.timestamp,
                action: sanitizedText(entry.action),
                contextName: sanitizedContextName(entry.contextName),
                namespace: sanitizedText(entry.namespace),
                resource: sanitizedText(entry.resource),
                status: sanitizedText(entry.status),
                message: sanitizedText(entry.message)
            )
        }

        func sanitizedRequestMetric(_ metric: SupportBundleRequestMetric) -> SupportBundleRequestMetric {
            SupportBundleRequestMetric(
                sourcePath: sanitizedText(metric.sourcePath),
                method: sanitizedText(metric.method),
                apiPath: sanitizedMetricAPIPath(metric.apiPath),
                statusCode: metric.statusCode,
                responseBytes: metric.responseBytes,
                durationSeconds: metric.durationSeconds,
                attempt: metric.attempt,
                outcome: sanitizedText(metric.outcome),
                cancellationReason: metric.cancellationReason.map(sanitizedText)
            )
        }

        func sanitizedRequestMetricGroup(_ group: SupportBundleRequestMetricGroup) -> SupportBundleRequestMetricGroup {
            SupportBundleRequestMetricGroup(
                sourcePath: sanitizedText(group.sourcePath),
                method: sanitizedText(group.method),
                apiPath: sanitizedMetricAPIPath(group.apiPath),
                requestCount: group.requestCount,
                successCount: group.successCount,
                failureCount: group.failureCount,
                cancelledCount: group.cancelledCount,
                responseBytes: group.responseBytes,
                totalDurationSeconds: group.totalDurationSeconds,
                maxDurationSeconds: group.maxDurationSeconds,
                latestStatusCode: group.latestStatusCode,
                latestOutcome: sanitizedText(group.latestOutcome)
            )
        }

        func sanitizedResourceListFreshness(
            _ entry: (key: RuneResourceListFamily, value: RuneResourceListFreshness),
            selectedNamespace: String
        ) -> SupportBundleResourceListFreshness {
            var message = entry.value.message.replacingOccurrences(of: " / ", with: " namespace ")
            let namespace = selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
            if !namespace.isEmpty {
                message = message.replacingOccurrences(of: namespace, with: "<namespace>")
            }
            message = sanitizedText(message)
            return SupportBundleResourceListFreshness(
                family: entry.key.rawValue,
                status: entry.value.status.rawValue,
                updatedAt: entry.value.updatedAt,
                message: message
            )
        }

        private func sanitizedMetricAPIPath(_ apiPath: String) -> String {
            var sanitized = apiPath
            for identifier in redactedIdentifiers where !identifier.isEmpty {
                sanitized = sanitized.replacingOccurrences(of: identifier, with: "<context-name>")
            }

            let pieces = sanitized.split(separator: "?", maxSplits: 1, omittingEmptySubsequences: false)
            let redactedPath = redactedMetricPathSegments(String(pieces.first ?? ""))
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

        private func redactedMetricPathSegments(_ path: String) -> String {
            let hasLeadingSlash = path.hasPrefix("/")
            var segments = path.split(separator: "/", omittingEmptySubsequences: true).map(String.init)

            if let namespaceIndex = segments.firstIndex(of: "namespaces"),
               namespaceIndex + 1 < segments.count {
                segments[namespaceIndex + 1] = "<namespace>"
            }

            if let nameIndex = metricObjectNameIndex(in: segments) {
                segments[nameIndex] = "<name>"
            }

            let joined = segments.joined(separator: "/")
            return hasLeadingSlash ? "/" + joined : joined
        }

        private func metricObjectNameIndex(in segments: [String]) -> Int? {
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

        private func sanitizedLine(_ line: String) -> String {
            let lineWithRedactedPath = sanitizedLocalPaths(in: line)

            guard let colonIndex = lineWithRedactedPath.firstIndex(of: ":") else {
                return lineWithRedactedPath
            }

            let key = lineWithRedactedPath[..<colonIndex]
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .trimmingCharacters(in: CharacterSet(charactersIn: "- "))
                .lowercased()

            guard sensitiveKeys.contains(key) else {
                return lineWithRedactedPath
            }

            let prefix = lineWithRedactedPath[...colonIndex]
            return "\(prefix) <redacted>"
        }

        private func sanitizedLocalPaths(in text: String) -> String {
            guard text.contains("/") || text.contains("~") else { return text }

            var result = ""
            result.reserveCapacity(text.count)
            var token = ""

            func flushToken() {
                guard !token.isEmpty else { return }
                result += sanitizedPathToken(token)
                token.removeAll(keepingCapacity: true)
            }

            for character in text {
                if character.isWhitespace {
                    flushToken()
                    result.append(character)
                } else {
                    token.append(character)
                }
            }
            flushToken()
            return result
        }

        private func sanitizedPathToken(_ token: String) -> String {
            var leading = ""
            var trailing = ""
            var core = token

            while let first = core.first, Self.pathTrimCharacters.contains(first) {
                leading.append(first)
                core.removeFirst()
            }

            while let last = core.last, Self.pathTrimCharacters.contains(last) {
                trailing.insert(last, at: trailing.startIndex)
                core.removeLast()
            }

            guard core.hasPrefix("/") || core.hasPrefix("~") else { return token }
            return leading + "<local-path>" + trailing
        }
    }
}

private extension CharacterSet {
    func contains(_ character: Character) -> Bool {
        character.unicodeScalars.allSatisfy { contains($0) }
    }
}
