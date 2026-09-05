import Foundation

public struct LastAppStateSnapshot: Codable, Equatable, Sendable {
    public let sourceScopeID: String?
    public let contextName: String?
    public let namespace: String?
    public let sectionID: String?
    public let workloadKindID: String?
    public let resourceID: String?
    public let resourceKind: String?
    public let resourceName: String?
    public let resourceNamespace: String?
    public let logPresetID: String?
    public let logContainer: String?
    public let includePreviousLogs: Bool?
    public let isHelmAllNamespaces: Bool?
    public let podSortColumnID: String?
    public let podSortAscending: Bool?
    public let deploymentSortColumnID: String?
    public let deploymentSortAscending: Bool?
    public let serviceSortColumnID: String?
    public let serviceSortAscending: Bool?
    public let genericResourceSortColumnID: String?
    public let genericResourceSortAscending: Bool?
    public let helmReleaseSortColumnID: String?
    public let helmReleaseSortAscending: Bool?
    public let eventSortColumnID: String?
    public let eventSortAscending: Bool?
    public let operatorResourceSortColumnID: String?
    public let operatorResourceSortAscending: Bool?
    public let operatorResourceFocusID: String?
    public let inspectorState: SavedWorkspaceInspectorState?

    public init(
        sourceScopeID: String? = nil,
        contextName: String? = nil,
        namespace: String? = nil,
        sectionID: String? = nil,
        workloadKindID: String? = nil,
        resourceID: String? = nil,
        resourceKind: String? = nil,
        resourceName: String? = nil,
        resourceNamespace: String? = nil,
        logPresetID: String? = nil,
        logContainer: String? = nil,
        includePreviousLogs: Bool? = nil,
        isHelmAllNamespaces: Bool? = nil,
        podSortColumnID: String? = nil,
        podSortAscending: Bool? = nil,
        deploymentSortColumnID: String? = nil,
        deploymentSortAscending: Bool? = nil,
        serviceSortColumnID: String? = nil,
        serviceSortAscending: Bool? = nil,
        genericResourceSortColumnID: String? = nil,
        genericResourceSortAscending: Bool? = nil,
        helmReleaseSortColumnID: String? = nil,
        helmReleaseSortAscending: Bool? = nil,
        eventSortColumnID: String? = nil,
        eventSortAscending: Bool? = nil,
        operatorResourceSortColumnID: String? = nil,
        operatorResourceSortAscending: Bool? = nil,
        operatorResourceFocusID: String? = nil,
        inspectorState: SavedWorkspaceInspectorState? = nil
    ) {
        self.sourceScopeID = Self.normalized(sourceScopeID)
        self.contextName = Self.normalized(contextName)
        self.namespace = Self.normalized(namespace)
        self.sectionID = Self.normalized(sectionID)
        self.workloadKindID = Self.normalized(workloadKindID)
        self.resourceID = Self.normalized(resourceID)
        self.resourceKind = Self.normalized(resourceKind)
        self.resourceName = Self.normalized(resourceName)
        self.resourceNamespace = Self.normalized(resourceNamespace)
        self.logPresetID = Self.normalized(logPresetID)
        self.logContainer = Self.normalized(logContainer)
        self.includePreviousLogs = includePreviousLogs
        self.isHelmAllNamespaces = isHelmAllNamespaces
        self.podSortColumnID = Self.normalized(podSortColumnID)
        self.podSortAscending = podSortAscending
        self.deploymentSortColumnID = Self.normalized(deploymentSortColumnID)
        self.deploymentSortAscending = deploymentSortAscending
        self.serviceSortColumnID = Self.normalized(serviceSortColumnID)
        self.serviceSortAscending = serviceSortAscending
        self.genericResourceSortColumnID = Self.normalized(genericResourceSortColumnID)
        self.genericResourceSortAscending = genericResourceSortAscending
        self.helmReleaseSortColumnID = Self.normalized(helmReleaseSortColumnID)
        self.helmReleaseSortAscending = helmReleaseSortAscending
        self.eventSortColumnID = Self.normalized(eventSortColumnID)
        self.eventSortAscending = eventSortAscending
        self.operatorResourceSortColumnID = Self.normalized(operatorResourceSortColumnID)
        self.operatorResourceSortAscending = operatorResourceSortAscending
        self.operatorResourceFocusID = Self.normalized(operatorResourceFocusID)
        self.inspectorState = inspectorState
    }

    private static func normalized(_ value: String?) -> String? {
        guard let trimmed = value?.trimmingCharacters(in: .whitespacesAndNewlines),
              !trimmed.isEmpty
        else { return nil }
        return trimmed
    }
}

public protocol LastAppStateStoring {
    func loadLastAppState() -> LastAppStateSnapshot?
    func saveLastAppState(_ snapshot: LastAppStateSnapshot)
    func clearLastAppState()
}

public struct NoopLastAppStateStore: LastAppStateStoring {
    public init() {}

    public func loadLastAppState() -> LastAppStateSnapshot? { nil }
    public func saveLastAppState(_ snapshot: LastAppStateSnapshot) {}
    public func clearLastAppState() {}
}

public final class JSONLastAppStateStore: LastAppStateStoring {
    private static let currentSchemaVersion = 1
    private let url: URL
    private let encoder: JSONEncoder
    private let decoder = JSONDecoder()

    public init(url: URL = JSONLastAppStateStore.defaultURL()) {
        self.url = url
        self.encoder = JSONEncoder()
        self.encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
    }

    public static func defaultURL() -> URL {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? FileManager.default.homeDirectoryForCurrentUser
                .appendingPathComponent("Library/Application Support", isDirectory: true)
        return root
            .appendingPathComponent("Rune", isDirectory: true)
            .appendingPathComponent("last-app-state.json")
    }

    public func loadLastAppState() -> LastAppStateSnapshot? {
        guard let data = try? Data(contentsOf: url),
              let document = try? decoder.decode(Document.self, from: data),
              document.schemaVersion == Self.currentSchemaVersion
        else { return nil }
        return document.snapshot
    }

    public func saveLastAppState(_ snapshot: LastAppStateSnapshot) {
        do {
            try FileManager.default.createDirectory(
                at: url.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            let data = try encoder.encode(Document(snapshot: snapshot))
            try data.write(to: url, options: [.atomic])
        } catch {
            // Non-critical UI state persistence.
        }
    }

    public func clearLastAppState() {
        try? FileManager.default.removeItem(at: url)
    }

    private struct Document: Codable {
        var schemaVersion = JSONLastAppStateStore.currentSchemaVersion
        var snapshot: LastAppStateSnapshot
    }
}
