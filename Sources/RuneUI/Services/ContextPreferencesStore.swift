import Foundation

public struct ContextDisplayMetadata: Codable, Equatable, Sendable {
    public let alias: String?
    public let colorKey: String?
    public let iconName: String?
    public let tags: [String]
    public let group: String?

    public init(
        alias: String? = nil,
        colorKey: String? = nil,
        iconName: String? = nil,
        tags: [String] = [],
        group: String? = nil
    ) {
        self.alias = Self.normalizedOptional(alias)
        self.colorKey = Self.normalizedOptional(colorKey)
        self.iconName = Self.normalizedOptional(iconName)
        self.tags = Self.normalizedStrings(tags)
        self.group = Self.normalizedOptional(group)
    }

    public var isEmpty: Bool {
        alias == nil
            && colorKey == nil
            && iconName == nil
            && tags.isEmpty
            && group == nil
    }

    public func normalized() -> ContextDisplayMetadata {
        ContextDisplayMetadata(
            alias: alias,
            colorKey: colorKey,
            iconName: iconName,
            tags: tags,
            group: group
        )
    }

    private static func normalizedOptional(_ value: String?) -> String? {
        let normalized = value?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return normalized.isEmpty ? nil : normalized
    }

    private static func normalizedStrings(_ values: [String]) -> [String] {
        var seen = Set<String>()
        return values.compactMap { value in
            let normalized = value.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !normalized.isEmpty else { return nil }
            guard seen.insert(normalized).inserted else { return nil }
            return normalized
        }
        .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
    }
}

public struct KubeConfigImportPreferenceBatch: Equatable, Sendable {
    public let preferredNamespaces: [String: String]
    public let contextDisplayMetadata: [String: ContextDisplayMetadata]
    public let favoriteContextNames: Set<String>?

    public init(
        preferredNamespaces: [String: String],
        contextDisplayMetadata: [String: ContextDisplayMetadata],
        favoriteContextNames: Set<String>?
    ) {
        self.preferredNamespaces = preferredNamespaces
        self.contextDisplayMetadata = contextDisplayMetadata
        self.favoriteContextNames = favoriteContextNames
    }
}

public protocol ContextPreferencesStoring {
    func loadFavoriteContextNames() -> Set<String>
    func saveFavoriteContextNames(_ names: Set<String>)
    func loadFavoriteResourceIDs() -> Set<String>
    func saveFavoriteResourceIDs(_ ids: Set<String>)
    func loadFavoriteNamespaceIDs() -> Set<String>
    func saveFavoriteNamespaceIDs(_ ids: Set<String>)
    func loadManualProductionContextIDs() -> Set<String>
    func saveManualProductionContextIDs(_ ids: Set<String>)
    func loadManualNamespaces(for contextName: String) -> [String]
    func saveManualNamespaces(_ namespaces: [String], for contextName: String)
    func loadPreferredNamespace(for contextName: String) -> String?
    func savePreferredNamespace(_ namespace: String, for contextName: String)
    func loadContextDisplayMetadata(for contextName: String) -> ContextDisplayMetadata?
    func saveContextDisplayMetadata(_ metadata: ContextDisplayMetadata?, for contextName: String)
    func loadHiddenOperatorPrinterColumnFamilies() -> Set<String>
    func saveHiddenOperatorPrinterColumnFamilies(_ families: Set<String>)
    func applyKubeConfigImportPreferenceBatch(_ batch: KubeConfigImportPreferenceBatch)
}

public extension ContextPreferencesStoring {
    func loadManualNamespaces(for contextName: String) -> [String] {
        []
    }

    func saveManualNamespaces(_ namespaces: [String], for contextName: String) {}

    func loadPreferredNamespace(for contextName: String) -> String? {
        nil
    }

    func savePreferredNamespace(_ namespace: String, for contextName: String) {}

    func loadContextDisplayMetadata(for contextName: String) -> ContextDisplayMetadata? {
        nil
    }

    func saveContextDisplayMetadata(_ metadata: ContextDisplayMetadata?, for contextName: String) {}

    func loadHiddenOperatorPrinterColumnFamilies() -> Set<String> {
        []
    }

    func saveHiddenOperatorPrinterColumnFamilies(_ families: Set<String>) {}

    func loadFavoriteResourceIDs() -> Set<String> {
        []
    }

    func saveFavoriteResourceIDs(_ ids: Set<String>) {}

    func loadFavoriteNamespaceIDs() -> Set<String> {
        []
    }

    func saveFavoriteNamespaceIDs(_ ids: Set<String>) {}

    func loadManualProductionContextIDs() -> Set<String> {
        []
    }

    func saveManualProductionContextIDs(_ ids: Set<String>) {}

    func applyKubeConfigImportPreferenceBatch(_ batch: KubeConfigImportPreferenceBatch) {
        for (contextName, namespace) in batch.preferredNamespaces {
            savePreferredNamespace(namespace, for: contextName)
        }
        for (contextName, metadata) in batch.contextDisplayMetadata {
            saveContextDisplayMetadata(metadata, for: contextName)
        }
        if let favoriteContextNames = batch.favoriteContextNames {
            saveFavoriteContextNames(favoriteContextNames)
        }
    }
}

public final class UserDefaultsContextPreferencesStore: ContextPreferencesStoring {
    private let defaults: UserDefaults
    private let favoriteContextsKey: String
    private let favoriteResourcesKey: String
    private let favoriteNamespacesKey: String
    private let manualProductionContextsKey: String
    private let manualNamespacesKey: String
    private let preferredNamespacesKey: String
    private let contextDisplayMetadataKey: String
    private let hiddenOperatorPrinterColumnFamiliesKey: String

    public init(
        defaults: UserDefaults = .standard,
        favoriteContextsKey: String = "rune.favorite.contexts",
        favoriteResourcesKey: String = "rune.favorite.resources",
        favoriteNamespacesKey: String = "rune.favorite.namespaces",
        manualProductionContextsKey: String = "rune.manual.production.contexts",
        manualNamespacesKey: String = "rune.manual.namespaces",
        preferredNamespacesKey: String = "rune.preferred.namespaces",
        contextDisplayMetadataKey: String = "rune.context.display.metadata",
        hiddenOperatorPrinterColumnFamiliesKey: String = "rune.hidden.operator.printer.column.families"
    ) {
        self.defaults = defaults
        self.favoriteContextsKey = favoriteContextsKey
        self.favoriteResourcesKey = favoriteResourcesKey
        self.favoriteNamespacesKey = favoriteNamespacesKey
        self.manualProductionContextsKey = manualProductionContextsKey
        self.manualNamespacesKey = manualNamespacesKey
        self.preferredNamespacesKey = preferredNamespacesKey
        self.contextDisplayMetadataKey = contextDisplayMetadataKey
        self.hiddenOperatorPrinterColumnFamiliesKey = hiddenOperatorPrinterColumnFamiliesKey
    }

    public func loadFavoriteContextNames() -> Set<String> {
        let names = defaults.stringArray(forKey: favoriteContextsKey) ?? []
        return Set(names)
    }

    public func saveFavoriteContextNames(_ names: Set<String>) {
        defaults.set(Array(names).sorted(), forKey: favoriteContextsKey)
    }

    public func loadFavoriteResourceIDs() -> Set<String> {
        Set(defaults.stringArray(forKey: favoriteResourcesKey) ?? [])
    }

    public func saveFavoriteResourceIDs(_ ids: Set<String>) {
        defaults.set(Array(ids).sorted(), forKey: favoriteResourcesKey)
    }

    public func loadFavoriteNamespaceIDs() -> Set<String> {
        Set(defaults.stringArray(forKey: favoriteNamespacesKey) ?? [])
    }

    public func saveFavoriteNamespaceIDs(_ ids: Set<String>) {
        defaults.set(Array(ids).sorted(), forKey: favoriteNamespacesKey)
    }

    public func loadManualProductionContextIDs() -> Set<String> {
        Set(
            (defaults.stringArray(forKey: manualProductionContextsKey) ?? [])
                .compactMap(Self.normalizedContextID)
        )
    }

    public func saveManualProductionContextIDs(_ ids: Set<String>) {
        let normalized = ids.compactMap(Self.normalizedContextID)
        defaults.set(Array(Set(normalized)).sorted(), forKey: manualProductionContextsKey)
    }

    public func loadManualNamespaces(for contextName: String) -> [String] {
        let map = defaults.dictionary(forKey: manualNamespacesKey) as? [String: [String]] ?? [:]
        return normalizedNamespaces(map[contextName] ?? [])
    }

    public func saveManualNamespaces(_ namespaces: [String], for contextName: String) {
        let normalizedContext = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedContext.isEmpty else { return }

        var map = defaults.dictionary(forKey: manualNamespacesKey) as? [String: [String]] ?? [:]
        let normalized = normalizedNamespaces(namespaces)
        if normalized.isEmpty {
            map.removeValue(forKey: normalizedContext)
        } else {
            map[normalizedContext] = normalized
        }
        defaults.set(map, forKey: manualNamespacesKey)
    }

    public func loadPreferredNamespace(for contextName: String) -> String? {
        let map = defaults.dictionary(forKey: preferredNamespacesKey) as? [String: String] ?? [:]
        let value = map[contextName]?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return value.isEmpty ? nil : value
    }

    public func savePreferredNamespace(_ namespace: String, for contextName: String) {
        let normalizedNamespace = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        var map = defaults.dictionary(forKey: preferredNamespacesKey) as? [String: String] ?? [:]

        if normalizedNamespace.isEmpty {
            map.removeValue(forKey: contextName)
        } else {
            map[contextName] = normalizedNamespace
        }

        defaults.set(map, forKey: preferredNamespacesKey)
    }

    public func loadContextDisplayMetadata(for contextName: String) -> ContextDisplayMetadata? {
        let context = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !context.isEmpty else { return nil }
        return Self.decodedContextDisplayMetadata(from: defaults.data(forKey: contextDisplayMetadataKey))?[context]
    }

    public func saveContextDisplayMetadata(_ metadata: ContextDisplayMetadata?, for contextName: String) {
        let context = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !context.isEmpty else { return }
        var map = Self.decodedContextDisplayMetadata(from: defaults.data(forKey: contextDisplayMetadataKey)) ?? [:]
        if let normalized = metadata?.normalized(), !normalized.isEmpty {
            map[context] = normalized
        } else {
            map.removeValue(forKey: context)
        }
        defaults.set(try? JSONEncoder().encode(map), forKey: contextDisplayMetadataKey)
    }

    public func loadHiddenOperatorPrinterColumnFamilies() -> Set<String> {
        Set(Self.normalizedStrings(defaults.stringArray(forKey: hiddenOperatorPrinterColumnFamiliesKey) ?? []))
    }

    public func saveHiddenOperatorPrinterColumnFamilies(_ families: Set<String>) {
        defaults.set(Self.normalizedStrings(Array(families)), forKey: hiddenOperatorPrinterColumnFamiliesKey)
    }

    private static func normalizedStrings(_ values: [String]) -> [String] {
        var seen = Set<String>()
        return values.compactMap { value in
            let normalized = value.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !normalized.isEmpty else { return nil }
            guard seen.insert(normalized).inserted else { return nil }
            return normalized
        }
        .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
    }

    private static func decodedContextDisplayMetadata(from data: Data?) -> [String: ContextDisplayMetadata]? {
        guard let data else { return nil }
        return try? JSONDecoder().decode([String: ContextDisplayMetadata].self, from: data)
    }

    private func normalizedNamespaces(_ namespaces: [String]) -> [String] {
        var seen = Set<String>()
        return namespaces.compactMap { namespace in
            let normalized = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !normalized.isEmpty else { return nil }
            let key = normalized.lowercased()
            guard seen.insert(key).inserted else { return nil }
            return normalized
        }
        .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
    }

    private static func normalizedContextID(_ contextName: String) -> String? {
        let normalized = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        return normalized.isEmpty ? nil : normalized
    }
}

public final class FileBackedContextPreferencesStore: ContextPreferencesStoring {
    public static let currentSchemaVersion = 2

    private let url: URL
    private let backupURL: URL
    private let corruptURL: URL
    private let legacyStore: ContextPreferencesStoring?
    private let fileManager: FileManager
    private var cachedDocument: ContextPreferencesDocument?

    public init(
        url: URL,
        backupURL: URL? = nil,
        corruptURL: URL? = nil,
        legacyStore: ContextPreferencesStoring? = nil,
        fileManager: FileManager = .default
    ) {
        self.url = url
        self.backupURL = backupURL ?? url.appendingPathExtension("bak")
        self.corruptURL = corruptURL ?? url.appendingPathExtension("corrupt")
        self.legacyStore = legacyStore
        self.fileManager = fileManager
    }

    public static func applicationSupportStore(
        legacyStore: ContextPreferencesStoring? = UserDefaultsContextPreferencesStore()
    ) -> FileBackedContextPreferencesStore {
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? FileManager.default.homeDirectoryForCurrentUser
                .appendingPathComponent("Library", isDirectory: true)
                .appendingPathComponent("Application Support", isDirectory: true)
        let directory = root.appendingPathComponent("Rune", isDirectory: true)
        return FileBackedContextPreferencesStore(
            url: directory.appendingPathComponent("context-preferences.json"),
            legacyStore: legacyStore
        )
    }

    public func loadFavoriteContextNames() -> Set<String> {
        Set(loadDocument().favoriteContextNames)
    }

    public func saveFavoriteContextNames(_ names: Set<String>) {
        updateDocument { document in
            document.favoriteContextNames = Self.normalizedStrings(Array(names))
        }
    }

    public func loadFavoriteResourceIDs() -> Set<String> {
        Set(loadDocument().favoriteResourceIDs)
    }

    public func saveFavoriteResourceIDs(_ ids: Set<String>) {
        updateDocument { document in
            document.favoriteResourceIDs = Self.normalizedStrings(Array(ids))
        }
    }

    public func loadFavoriteNamespaceIDs() -> Set<String> {
        Set(loadDocument().favoriteNamespaceIDs)
    }

    public func saveFavoriteNamespaceIDs(_ ids: Set<String>) {
        updateDocument { document in
            document.favoriteNamespaceIDs = Self.normalizedStrings(Array(ids))
        }
    }

    public func loadManualProductionContextIDs() -> Set<String> {
        Set(loadDocument().manualProductionContextIDs)
    }

    public func saveManualProductionContextIDs(_ ids: Set<String>) {
        updateDocument { document in
            document.manualProductionContextIDs = Self.normalizedStrings(Array(ids))
        }
    }

    public func loadManualNamespaces(for contextName: String) -> [String] {
        let context = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !context.isEmpty else { return [] }
        return loadDocument().manualNamespaces[context] ?? []
    }

    public func saveManualNamespaces(_ namespaces: [String], for contextName: String) {
        let context = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !context.isEmpty else { return }
        updateDocument { document in
            let normalized = Self.normalizedStrings(namespaces)
            if normalized.isEmpty {
                document.manualNamespaces.removeValue(forKey: context)
            } else {
                document.manualNamespaces[context] = normalized
            }
        }
    }

    public func loadPreferredNamespace(for contextName: String) -> String? {
        let context = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !context.isEmpty else { return nil }
        return loadDocument().preferredNamespaces[context]
    }

    public func savePreferredNamespace(_ namespace: String, for contextName: String) {
        let context = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !context.isEmpty else { return }
        updateDocument { document in
            let normalized = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
            if normalized.isEmpty {
                document.preferredNamespaces.removeValue(forKey: context)
            } else {
                document.preferredNamespaces[context] = normalized
            }
        }
    }

    public func loadContextDisplayMetadata(for contextName: String) -> ContextDisplayMetadata? {
        let context = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !context.isEmpty else { return nil }
        return loadDocument().contextDisplayMetadata[context]
    }

    public func saveContextDisplayMetadata(_ metadata: ContextDisplayMetadata?, for contextName: String) {
        let context = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !context.isEmpty else { return }
        updateDocument { document in
            if let normalized = metadata?.normalized(), !normalized.isEmpty {
                document.contextDisplayMetadata[context] = normalized
            } else {
                document.contextDisplayMetadata.removeValue(forKey: context)
            }
        }
    }

    public func loadHiddenOperatorPrinterColumnFamilies() -> Set<String> {
        Set(loadDocument().hiddenOperatorPrinterColumnFamilies)
    }

    public func saveHiddenOperatorPrinterColumnFamilies(_ families: Set<String>) {
        updateDocument { document in
            document.hiddenOperatorPrinterColumnFamilies = Self.normalizedStrings(Array(families))
        }
    }

    public func applyKubeConfigImportPreferenceBatch(_ batch: KubeConfigImportPreferenceBatch) {
        updateDocument { document in
            for (rawContext, rawNamespace) in batch.preferredNamespaces {
                let context = rawContext.trimmingCharacters(in: .whitespacesAndNewlines)
                let namespace = rawNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !context.isEmpty else { continue }
                if namespace.isEmpty {
                    document.preferredNamespaces.removeValue(forKey: context)
                } else {
                    document.preferredNamespaces[context] = namespace
                }
            }
            for (rawContext, metadata) in batch.contextDisplayMetadata {
                let context = rawContext.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !context.isEmpty else { continue }
                let normalized = metadata.normalized()
                if normalized.isEmpty {
                    document.contextDisplayMetadata.removeValue(forKey: context)
                } else {
                    document.contextDisplayMetadata[context] = normalized
                }
            }
            if let favorites = batch.favoriteContextNames {
                document.favoriteContextNames = Self.normalizedStrings(Array(favorites))
            }
        }
    }

    private func loadDocument() -> ContextPreferencesDocument {
        if let cachedDocument {
            return cachedDocument
        }

        if let document = decodedDocument(at: url) {
            let normalized = document.normalized()
            cachedDocument = normalized
            return normalized
        }

        let primaryExists = fileManager.fileExists(atPath: url.path)
        if primaryExists {
            preserveCorruptPrimary()
        }

        if let backup = decodedDocument(at: backupURL)?.normalized() {
            try? writeDocument(backup, backsUpCurrentPrimary: false)
            cachedDocument = backup
            return backup
        }

        if let legacy = legacyStore {
            let migrated = ContextPreferencesDocument(legacyStore: legacy).normalized()
            if !migrated.isEmpty {
                try? writeDocument(migrated, backsUpCurrentPrimary: false)
            }
            cachedDocument = migrated
            return migrated
        }

        let empty = ContextPreferencesDocument()
        cachedDocument = empty
        return empty
    }

    private func updateDocument(_ update: (inout ContextPreferencesDocument) -> Void) {
        var document = loadDocument()
        update(&document)
        document = document.normalized()
        try? writeDocument(document, backsUpCurrentPrimary: true)
        cachedDocument = document
    }

    private func decodedDocument(at url: URL) -> ContextPreferencesDocument? {
        guard let data = try? Data(contentsOf: url) else { return nil }
        return try? JSONDecoder().decode(ContextPreferencesDocument.self, from: data)
    }

    private func writeDocument(_ document: ContextPreferencesDocument, backsUpCurrentPrimary: Bool) throws {
        let directory = url.deletingLastPathComponent()
        try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)

        if backsUpCurrentPrimary,
           fileManager.fileExists(atPath: url.path),
           decodedDocument(at: url) != nil
        {
            try? fileManager.removeItem(at: backupURL)
            try? fileManager.copyItem(at: url, to: backupURL)
        }

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(document.normalized())
        try data.write(to: url, options: .atomic)
    }

    private func preserveCorruptPrimary() {
        do {
            try fileManager.createDirectory(at: corruptURL.deletingLastPathComponent(), withIntermediateDirectories: true)
            try? fileManager.removeItem(at: corruptURL)
            try fileManager.copyItem(at: url, to: corruptURL)
        } catch {
            // Best-effort diagnostics preservation; loading should continue through backup/defaults.
        }
    }

    private static func normalizedStrings(_ values: [String]) -> [String] {
        var seen = Set<String>()
        return values.compactMap { value in
            let normalized = value.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !normalized.isEmpty else { return nil }
            guard seen.insert(normalized).inserted else { return nil }
            return normalized
        }
        .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
    }

    private struct ContextPreferencesDocument: Codable, Equatable {
        var schemaVersion: Int
        var favoriteContextNames: [String]
        var favoriteResourceIDs: [String]
        var favoriteNamespaceIDs: [String]
        var manualProductionContextIDs: [String]
        var manualNamespaces: [String: [String]]
        var preferredNamespaces: [String: String]
        var contextDisplayMetadata: [String: ContextDisplayMetadata]
        var hiddenOperatorPrinterColumnFamilies: [String]

        init(
            schemaVersion: Int = FileBackedContextPreferencesStore.currentSchemaVersion,
            favoriteContextNames: [String] = [],
            favoriteResourceIDs: [String] = [],
            favoriteNamespaceIDs: [String] = [],
            manualProductionContextIDs: [String] = [],
            manualNamespaces: [String: [String]] = [:],
            preferredNamespaces: [String: String] = [:],
            contextDisplayMetadata: [String: ContextDisplayMetadata] = [:],
            hiddenOperatorPrinterColumnFamilies: [String] = []
        ) {
            self.schemaVersion = schemaVersion
            self.favoriteContextNames = favoriteContextNames
            self.favoriteResourceIDs = favoriteResourceIDs
            self.favoriteNamespaceIDs = favoriteNamespaceIDs
            self.manualProductionContextIDs = manualProductionContextIDs
            self.manualNamespaces = manualNamespaces
            self.preferredNamespaces = preferredNamespaces
            self.contextDisplayMetadata = contextDisplayMetadata
            self.hiddenOperatorPrinterColumnFamilies = hiddenOperatorPrinterColumnFamilies
        }

        init(legacyStore: ContextPreferencesStoring) {
            let favoriteContextNames = Array(legacyStore.loadFavoriteContextNames())
            let favoriteResourceIDs = Array(legacyStore.loadFavoriteResourceIDs())
            let favoriteNamespaceIDs = Array(legacyStore.loadFavoriteNamespaceIDs())
            let manualProductionContextIDs = Array(legacyStore.loadManualProductionContextIDs())
            let contextNames = Self.contextNames(
                favoriteContextNames: favoriteContextNames,
                favoriteResourceIDs: favoriteResourceIDs,
                favoriteNamespaceIDs: favoriteNamespaceIDs,
                manualProductionContextIDs: manualProductionContextIDs
            )
            var manualNamespaces: [String: [String]] = [:]
            var preferredNamespaces: [String: String] = [:]
            var contextDisplayMetadata: [String: ContextDisplayMetadata] = [:]
            for contextName in contextNames {
                let namespaces = legacyStore.loadManualNamespaces(for: contextName)
                if !namespaces.isEmpty {
                    manualNamespaces[contextName] = namespaces
                }
                if let preferred = legacyStore.loadPreferredNamespace(for: contextName) {
                    preferredNamespaces[contextName] = preferred
                }
                if let metadata = legacyStore.loadContextDisplayMetadata(for: contextName) {
                    contextDisplayMetadata[contextName] = metadata
                }
            }
            let hiddenOperatorPrinterColumnFamilies = Array(legacyStore.loadHiddenOperatorPrinterColumnFamilies())

            self.init(
                favoriteContextNames: favoriteContextNames,
                favoriteResourceIDs: favoriteResourceIDs,
                favoriteNamespaceIDs: favoriteNamespaceIDs,
                manualProductionContextIDs: manualProductionContextIDs,
                manualNamespaces: manualNamespaces,
                preferredNamespaces: preferredNamespaces,
                contextDisplayMetadata: contextDisplayMetadata,
                hiddenOperatorPrinterColumnFamilies: hiddenOperatorPrinterColumnFamilies
            )
        }

        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            schemaVersion = try container.decodeIfPresent(Int.self, forKey: .schemaVersion)
                ?? FileBackedContextPreferencesStore.currentSchemaVersion
            favoriteContextNames = try container.decodeIfPresent([String].self, forKey: .favoriteContextNames) ?? []
            favoriteResourceIDs = try container.decodeIfPresent([String].self, forKey: .favoriteResourceIDs) ?? []
            favoriteNamespaceIDs = try container.decodeIfPresent([String].self, forKey: .favoriteNamespaceIDs) ?? []
            manualProductionContextIDs = try container.decodeIfPresent([String].self, forKey: .manualProductionContextIDs) ?? []
            manualNamespaces = try container.decodeIfPresent([String: [String]].self, forKey: .manualNamespaces) ?? [:]
            preferredNamespaces = try container.decodeIfPresent([String: String].self, forKey: .preferredNamespaces) ?? [:]
            contextDisplayMetadata = try container.decodeIfPresent([String: ContextDisplayMetadata].self, forKey: .contextDisplayMetadata) ?? [:]
            hiddenOperatorPrinterColumnFamilies = try container.decodeIfPresent([String].self, forKey: .hiddenOperatorPrinterColumnFamilies) ?? []
        }

        var isEmpty: Bool {
            favoriteContextNames.isEmpty
                && favoriteResourceIDs.isEmpty
                && favoriteNamespaceIDs.isEmpty
                && manualProductionContextIDs.isEmpty
                && manualNamespaces.isEmpty
                && preferredNamespaces.isEmpty
                && contextDisplayMetadata.isEmpty
                && hiddenOperatorPrinterColumnFamilies.isEmpty
        }

        func normalized() -> ContextPreferencesDocument {
            var normalizedManualNamespaces: [String: [String]] = [:]
            for (context, namespaces) in manualNamespaces {
                let normalizedContext = context.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !normalizedContext.isEmpty else { continue }
                let normalizedNamespaces = FileBackedContextPreferencesStore.normalizedStrings(namespaces)
                guard !normalizedNamespaces.isEmpty else { continue }
                normalizedManualNamespaces[normalizedContext] = normalizedNamespaces
            }

            var normalizedPreferredNamespaces: [String: String] = [:]
            for (context, namespace) in preferredNamespaces {
                let normalizedContext = context.trimmingCharacters(in: .whitespacesAndNewlines)
                let normalizedNamespace = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !normalizedContext.isEmpty, !normalizedNamespace.isEmpty else { continue }
                normalizedPreferredNamespaces[normalizedContext] = normalizedNamespace
            }

            var normalizedContextDisplayMetadata: [String: ContextDisplayMetadata] = [:]
            for (context, metadata) in contextDisplayMetadata {
                let normalizedContext = context.trimmingCharacters(in: .whitespacesAndNewlines)
                let normalizedMetadata = metadata.normalized()
                guard !normalizedContext.isEmpty, !normalizedMetadata.isEmpty else { continue }
                normalizedContextDisplayMetadata[normalizedContext] = normalizedMetadata
            }

            return ContextPreferencesDocument(
                schemaVersion: FileBackedContextPreferencesStore.currentSchemaVersion,
                favoriteContextNames: FileBackedContextPreferencesStore.normalizedStrings(favoriteContextNames),
                favoriteResourceIDs: FileBackedContextPreferencesStore.normalizedStrings(favoriteResourceIDs),
                favoriteNamespaceIDs: FileBackedContextPreferencesStore.normalizedStrings(favoriteNamespaceIDs),
                manualProductionContextIDs: FileBackedContextPreferencesStore.normalizedStrings(manualProductionContextIDs),
                manualNamespaces: normalizedManualNamespaces,
                preferredNamespaces: normalizedPreferredNamespaces,
                contextDisplayMetadata: normalizedContextDisplayMetadata,
                hiddenOperatorPrinterColumnFamilies: FileBackedContextPreferencesStore.normalizedStrings(hiddenOperatorPrinterColumnFamilies)
            )
        }

        private static func contextNames(
            favoriteContextNames: [String],
            favoriteResourceIDs: [String],
            favoriteNamespaceIDs: [String],
            manualProductionContextIDs: [String]
        ) -> [String] {
            var names = Set<String>()
            for value in favoriteContextNames + manualProductionContextIDs {
                let normalized = value.trimmingCharacters(in: .whitespacesAndNewlines)
                if !normalized.isEmpty {
                    names.insert(normalized)
                }
            }
            for id in favoriteResourceIDs + favoriteNamespaceIDs {
                guard let context = id.split(separator: "|", maxSplits: 1).first else { continue }
                let normalized = String(context).trimmingCharacters(in: .whitespacesAndNewlines)
                if !normalized.isEmpty {
                    names.insert(normalized)
                }
            }
            return names.sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
        }
    }
}
