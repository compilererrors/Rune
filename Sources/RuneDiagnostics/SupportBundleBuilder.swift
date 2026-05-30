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
            writeAuditLog: state.writeAuditLog.map(sanitizer.sanitizedWriteAuditEntry)
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
