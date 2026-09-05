import Foundation
import RuneCore

public struct SavedWorkspaceInspectorState: Codable, Equatable, Sendable {
    public let podTabID: String?
    public let serviceTabID: String?
    public let deploymentTabID: String?
    public let genericManifestTabID: String?
    public let helmTabID: String?
    public let helmBrowserTabID: String?
    public let terminalTabID: String?
    public let isYAMLInlineEditing: Bool?
    public let showsHistoricalDeploymentReplicaSets: Bool?

    public init(
        podTabID: String? = nil,
        serviceTabID: String? = nil,
        deploymentTabID: String? = nil,
        genericManifestTabID: String? = nil,
        helmTabID: String? = nil,
        helmBrowserTabID: String? = nil,
        terminalTabID: String? = nil,
        isYAMLInlineEditing: Bool? = nil,
        showsHistoricalDeploymentReplicaSets: Bool? = nil
    ) {
        self.podTabID = podTabID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.serviceTabID = serviceTabID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.deploymentTabID = deploymentTabID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.genericManifestTabID = genericManifestTabID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.helmTabID = helmTabID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.helmBrowserTabID = helmBrowserTabID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.terminalTabID = terminalTabID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.isYAMLInlineEditing = isYAMLInlineEditing
        self.showsHistoricalDeploymentReplicaSets = showsHistoricalDeploymentReplicaSets
    }
}

public struct SavedWorkspaceSnapshot: Codable, Equatable, Identifiable, Sendable {
    public let id: String
    public let name: String
    public let contextName: String?
    public let namespace: String
    public let section: RuneSection
    public let workloadKind: KubeResourceKind
    public let isFavorite: Bool
    public let resourceKind: String?
    public let resourceName: String?
    public let resourceNamespace: String?
    public let logPresetID: String?
    public let logContainer: String?
    public let includePreviousLogs: Bool?
    public let isLogTailModeEnabled: Bool?
    public let isSidebarVisible: Bool?
    public let isDetailPaneVisible: Bool?
    public let inspectorState: SavedWorkspaceInspectorState?

    private enum CodingKeys: String, CodingKey {
        case id
        case name
        case contextName
        case namespace
        case section
        case workloadKind
        case isFavorite
        case resourceKind
        case resourceName
        case resourceNamespace
        case logPresetID
        case logContainer
        case includePreviousLogs
        case isLogTailModeEnabled
        case isSidebarVisible
        case isDetailPaneVisible
        case inspectorState
    }

    public init(
        id: String = UUID().uuidString,
        name: String,
        contextName: String?,
        namespace: String,
        section: RuneSection,
        workloadKind: KubeResourceKind,
        isFavorite: Bool = false,
        resourceKind: String?,
        resourceName: String?,
        resourceNamespace: String?,
        logPresetID: String? = nil,
        logContainer: String? = nil,
        includePreviousLogs: Bool? = nil,
        isLogTailModeEnabled: Bool? = nil,
        isSidebarVisible: Bool? = nil,
        isDetailPaneVisible: Bool? = nil,
        inspectorState: SavedWorkspaceInspectorState? = nil
    ) {
        self.id = id
        self.name = name
        self.contextName = contextName?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.namespace = namespace.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty ?? "default"
        self.section = section
        self.workloadKind = workloadKind
        self.isFavorite = isFavorite
        self.resourceKind = resourceKind?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.resourceName = resourceName?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.resourceNamespace = resourceNamespace?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.logPresetID = logPresetID?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.logContainer = logContainer?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.includePreviousLogs = includePreviousLogs
        self.isLogTailModeEnabled = isLogTailModeEnabled
        self.isSidebarVisible = isSidebarVisible
        self.isDetailPaneVisible = isDetailPaneVisible
        self.inspectorState = inspectorState
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            id: try container.decodeIfPresent(String.self, forKey: .id) ?? UUID().uuidString,
            name: try container.decode(String.self, forKey: .name),
            contextName: try container.decodeIfPresent(String.self, forKey: .contextName),
            namespace: try container.decodeIfPresent(String.self, forKey: .namespace) ?? "default",
            section: try container.decodeIfPresent(RuneSection.self, forKey: .section) ?? .overview,
            workloadKind: try container.decodeIfPresent(KubeResourceKind.self, forKey: .workloadKind) ?? .pod,
            isFavorite: try container.decodeIfPresent(Bool.self, forKey: .isFavorite) ?? false,
            resourceKind: try container.decodeIfPresent(String.self, forKey: .resourceKind),
            resourceName: try container.decodeIfPresent(String.self, forKey: .resourceName),
            resourceNamespace: try container.decodeIfPresent(String.self, forKey: .resourceNamespace),
            logPresetID: try container.decodeIfPresent(String.self, forKey: .logPresetID),
            logContainer: try container.decodeIfPresent(String.self, forKey: .logContainer),
            includePreviousLogs: try container.decodeIfPresent(Bool.self, forKey: .includePreviousLogs),
            isLogTailModeEnabled: try container.decodeIfPresent(Bool.self, forKey: .isLogTailModeEnabled),
            isSidebarVisible: try container.decodeIfPresent(Bool.self, forKey: .isSidebarVisible),
            isDetailPaneVisible: try container.decodeIfPresent(Bool.self, forKey: .isDetailPaneVisible),
            inspectorState: try container.decodeIfPresent(SavedWorkspaceInspectorState.self, forKey: .inspectorState)
        )
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(name, forKey: .name)
        try container.encodeIfPresent(contextName, forKey: .contextName)
        try container.encode(namespace, forKey: .namespace)
        try container.encode(section, forKey: .section)
        try container.encode(workloadKind, forKey: .workloadKind)
        try container.encode(isFavorite, forKey: .isFavorite)
        try container.encodeIfPresent(resourceKind, forKey: .resourceKind)
        try container.encodeIfPresent(resourceName, forKey: .resourceName)
        try container.encodeIfPresent(resourceNamespace, forKey: .resourceNamespace)
        try container.encodeIfPresent(logPresetID, forKey: .logPresetID)
        try container.encodeIfPresent(logContainer, forKey: .logContainer)
        try container.encodeIfPresent(includePreviousLogs, forKey: .includePreviousLogs)
        try container.encodeIfPresent(isLogTailModeEnabled, forKey: .isLogTailModeEnabled)
        try container.encodeIfPresent(isSidebarVisible, forKey: .isSidebarVisible)
        try container.encodeIfPresent(isDetailPaneVisible, forKey: .isDetailPaneVisible)
        try container.encodeIfPresent(inspectorState, forKey: .inspectorState)
    }
}

public protocol SavedWorkspaceStoring {
    func loadSavedWorkspaces() -> [SavedWorkspaceSnapshot]
    func saveSavedWorkspaces(_ workspaces: [SavedWorkspaceSnapshot])
}

public final class JSONSavedWorkspaceStore: SavedWorkspaceStoring {
    private let url: URL
    private let encoder: JSONEncoder
    private let decoder = JSONDecoder()

    public init(url: URL = JSONSavedWorkspaceStore.defaultURL()) {
        self.url = url
        self.encoder = JSONEncoder()
        self.encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
    }

    public static func defaultURL() -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent("Library/Application Support", isDirectory: true)
        return root
            .appendingPathComponent("Rune", isDirectory: true)
            .appendingPathComponent("saved-workspaces.json")
    }

    public func loadSavedWorkspaces() -> [SavedWorkspaceSnapshot] {
        guard let data = try? Data(contentsOf: url),
              let document = try? decoder.decode(Document.self, from: data)
        else { return [] }
        return Self.normalized(document.workspaces)
    }

    public func saveSavedWorkspaces(_ workspaces: [SavedWorkspaceSnapshot]) {
        do {
            try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
            let data = try encoder.encode(Document(workspaces: Self.normalized(workspaces)))
            try data.write(to: url, options: [.atomic])
        } catch {
            // Non-critical local preference persistence. The ViewModel keeps the in-memory snapshot.
        }
    }

    private static func normalized(_ workspaces: [SavedWorkspaceSnapshot]) -> [SavedWorkspaceSnapshot] {
        var seen = Set<String>()
        return workspaces.compactMap { workspace in
            let name = workspace.name.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !name.isEmpty else { return nil }
            guard seen.insert(workspace.id).inserted else { return nil }
            return SavedWorkspaceSnapshot(
                id: workspace.id,
                name: name,
                contextName: workspace.contextName,
                namespace: workspace.namespace,
                section: workspace.section,
                workloadKind: workspace.workloadKind,
                isFavorite: workspace.isFavorite,
                resourceKind: workspace.resourceKind,
                resourceName: workspace.resourceName,
                resourceNamespace: workspace.resourceNamespace,
                logPresetID: workspace.logPresetID,
                logContainer: workspace.logContainer,
                includePreviousLogs: workspace.includePreviousLogs,
                isLogTailModeEnabled: workspace.isLogTailModeEnabled,
                isSidebarVisible: workspace.isSidebarVisible,
                isDetailPaneVisible: workspace.isDetailPaneVisible,
                inspectorState: workspace.inspectorState
            )
        }
        .sorted { lhs, rhs in
            if lhs.isFavorite != rhs.isFavorite {
                return lhs.isFavorite && !rhs.isFavorite
            }
            return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
        }
    }

    private struct Document: Codable {
        var schemaVersion = 1
        var workspaces: [SavedWorkspaceSnapshot]
    }
}

private extension String {
    var nilIfEmpty: String? {
        isEmpty ? nil : self
    }
}
