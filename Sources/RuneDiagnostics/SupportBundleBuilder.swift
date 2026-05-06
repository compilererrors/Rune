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
        writeAuditLog: [WriteAuditEntry]
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
        selectedResourceName: String?
    ) -> SupportBundleRequest {
        SupportBundleRequest(
            generatedAt: generatedAt,
            contextName: state.selectedContext?.name,
            namespace: state.selectedNamespace,
            sectionTitle: state.selectedSection.title,
            readOnlyMode: state.isReadOnlyMode,
            resourceCounts: resourceCounts,
            selectedResourceKind: selectedResourceKind,
            selectedResourceName: selectedResourceName,
            resourceYAML: state.resourceYAML,
            resourceDescribe: state.resourceDescribe,
            podLogs: state.podLogs,
            unifiedLogs: state.unifiedServiceLogs,
            unifiedLogPods: state.unifiedServiceLogPods,
            deploymentRolloutHistory: state.deploymentRolloutHistory,
            recentEvents: Array(state.events.prefix(25)),
            portForwardSessions: state.portForwardSessions,
            lastExecResult: state.lastExecResult,
            authDoctorChecks: state.authDoctorChecks.map(sanitizedAuthDoctorCheck),
            writeAuditLog: state.writeAuditLog
        )
    }

    private static func sanitizedAuthDoctorCheck(_ check: RuneHealthCheck) -> RuneHealthCheck {
        RuneHealthCheck(
            id: check.id,
            title: check.title,
            status: check.status,
            message: sanitizedSupportText(check.message)
        )
    }

    private static func sanitizedSupportText(_ text: String) -> String {
        var sanitized = text
        for token in sanitized.components(separatedBy: .whitespacesAndNewlines) {
            let trimmed = token.trimmingCharacters(in: CharacterSet(charactersIn: ".,;:()[]{}\"'"))
            guard trimmed.hasPrefix("/") || trimmed.hasPrefix("~") else { continue }
            sanitized = sanitized.replacingOccurrences(of: trimmed, with: "<local-path>")
        }
        return sanitized
    }
}
