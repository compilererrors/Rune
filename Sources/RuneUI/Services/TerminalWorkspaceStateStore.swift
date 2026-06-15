import Foundation
import RuneCore

public struct TerminalWorkspaceSessionSnapshot: Codable, Equatable, Sendable {
    public let id: String
    public let contextName: String
    public let namespace: String
    public let podName: String
    public let containerName: String?
    public let shell: String

    public init(
        id: String,
        contextName: String,
        namespace: String,
        podName: String,
        containerName: String?,
        shell: String
    ) {
        self.id = id.trimmingCharacters(in: .whitespacesAndNewlines)
        self.contextName = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        self.namespace = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        self.podName = podName.trimmingCharacters(in: .whitespacesAndNewlines)
        self.containerName = containerName?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.shell = shell.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty ?? "sh"
    }

    public init(session: PodTerminalSession) {
        self.init(
            id: session.id,
            contextName: session.contextName,
            namespace: session.namespace,
            podName: session.podName,
            containerName: session.containerName,
            shell: session.shell
        )
    }

    public var restoredSession: PodTerminalSession {
        PodTerminalSession(
            id: id,
            contextName: contextName,
            namespace: namespace,
            podName: podName,
            containerName: containerName,
            shell: shell,
            transcript: "",
            status: .disconnected,
            lastExitCode: nil,
            lastDiagnostic: PodTerminalSessionDiagnostic(
                category: .transportDisconnected,
                summary: "Terminal tab restored without an active shell connection.",
                recoveryHint: "Reconnect when you need an interactive shell."
            )
        )
    }
}

public struct TerminalWorkspaceLogTabSnapshot: Codable, Equatable, Sendable {
    public let id: String
    public let podID: String
    public let namespace: String
    public let podName: String

    public init(id: String, podID: String, namespace: String, podName: String) {
        self.id = id.trimmingCharacters(in: .whitespacesAndNewlines)
        self.podID = podID.trimmingCharacters(in: .whitespacesAndNewlines)
        self.namespace = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        self.podName = podName.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

public struct TerminalWorkspaceStateSnapshot: Codable, Equatable, Sendable {
    public let sessions: [TerminalWorkspaceSessionSnapshot]
    public let activeSessionID: String?
    public let logTabs: [TerminalWorkspaceLogTabSnapshot]
    public let activeLogTabID: String?
    public let selectedLogPodID: String?
    public let shellPodID: String?
    public let portForwardPodID: String?
    public let inspectorTabID: String?

    public init(
        sessions: [TerminalWorkspaceSessionSnapshot],
        activeSessionID: String?,
        logTabs: [TerminalWorkspaceLogTabSnapshot],
        activeLogTabID: String?,
        selectedLogPodID: String?,
        shellPodID: String?,
        portForwardPodID: String?,
        inspectorTabID: String?
    ) {
        self.sessions = Self.normalizedSessions(sessions)
        self.activeSessionID = activeSessionID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.logTabs = Self.normalizedLogTabs(logTabs)
        self.activeLogTabID = activeLogTabID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.selectedLogPodID = selectedLogPodID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.shellPodID = shellPodID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.portForwardPodID = portForwardPodID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.inspectorTabID = inspectorTabID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
    }

    public var isEmpty: Bool {
        sessions.isEmpty && logTabs.isEmpty && shellPodID == nil && portForwardPodID == nil
    }

    private static func normalizedSessions(_ sessions: [TerminalWorkspaceSessionSnapshot]) -> [TerminalWorkspaceSessionSnapshot] {
        var seen = Set<String>()
        return sessions.filter {
            !$0.id.isEmpty
                && !$0.contextName.isEmpty
                && !$0.namespace.isEmpty
                && !$0.podName.isEmpty
                && seen.insert($0.id).inserted
        }
    }

    private static func normalizedLogTabs(_ tabs: [TerminalWorkspaceLogTabSnapshot]) -> [TerminalWorkspaceLogTabSnapshot] {
        var seen = Set<String>()
        return tabs.filter {
            !$0.id.isEmpty
                && !$0.podID.isEmpty
                && !$0.namespace.isEmpty
                && !$0.podName.isEmpty
                && seen.insert($0.id).inserted
        }
    }
}

public protocol TerminalWorkspaceStateStoring {
    func loadTerminalWorkspaceState() -> TerminalWorkspaceStateSnapshot?
    func saveTerminalWorkspaceState(_ snapshot: TerminalWorkspaceStateSnapshot)
    func clearTerminalWorkspaceState()
}

public final class JSONTerminalWorkspaceStateStore: TerminalWorkspaceStateStoring {
    private let url: URL
    private let encoder: JSONEncoder
    private let decoder = JSONDecoder()

    public init(url: URL = JSONTerminalWorkspaceStateStore.defaultURL()) {
        self.url = url
        self.encoder = JSONEncoder()
        self.encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
    }

    public static func defaultURL() -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent("Library/Application Support", isDirectory: true)
        return root
            .appendingPathComponent("Rune", isDirectory: true)
            .appendingPathComponent("terminal-workspace-state.json")
    }

    public func loadTerminalWorkspaceState() -> TerminalWorkspaceStateSnapshot? {
        guard let data = try? Data(contentsOf: url),
              let document = try? decoder.decode(Document.self, from: data),
              !document.snapshot.isEmpty
        else { return nil }
        return document.snapshot
    }

    public func saveTerminalWorkspaceState(_ snapshot: TerminalWorkspaceStateSnapshot) {
        if snapshot.isEmpty {
            clearTerminalWorkspaceState()
            return
        }

        do {
            try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
            let data = try encoder.encode(Document(snapshot: snapshot))
            try data.write(to: url, options: [.atomic])
        } catch {
            // Non-critical UI state persistence.
        }
    }

    public func clearTerminalWorkspaceState() {
        try? FileManager.default.removeItem(at: url)
    }

    private struct Document: Codable {
        var schemaVersion = 1
        var snapshot: TerminalWorkspaceStateSnapshot
    }
}

private extension String {
    var nilIfEmpty: String? {
        isEmpty ? nil : self
    }
}
