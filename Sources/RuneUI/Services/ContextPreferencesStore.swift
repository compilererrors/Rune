import Foundation

public protocol ContextPreferencesStoring {
    func loadFavoriteContextNames() -> Set<String>
    func saveFavoriteContextNames(_ names: Set<String>)
    func loadFavoriteResourceIDs() -> Set<String>
    func saveFavoriteResourceIDs(_ ids: Set<String>)
    func loadFavoriteNamespaceIDs() -> Set<String>
    func saveFavoriteNamespaceIDs(_ ids: Set<String>)
    func loadManualNamespaces(for contextName: String) -> [String]
    func saveManualNamespaces(_ namespaces: [String], for contextName: String)
    func loadPreferredNamespace(for contextName: String) -> String?
    func savePreferredNamespace(_ namespace: String, for contextName: String)
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

    func loadFavoriteResourceIDs() -> Set<String> {
        []
    }

    func saveFavoriteResourceIDs(_ ids: Set<String>) {}

    func loadFavoriteNamespaceIDs() -> Set<String> {
        []
    }

    func saveFavoriteNamespaceIDs(_ ids: Set<String>) {}
}

public final class UserDefaultsContextPreferencesStore: ContextPreferencesStoring {
    private let defaults: UserDefaults
    private let favoriteContextsKey: String
    private let favoriteResourcesKey: String
    private let favoriteNamespacesKey: String
    private let manualNamespacesKey: String
    private let preferredNamespacesKey: String

    public init(
        defaults: UserDefaults = .standard,
        favoriteContextsKey: String = "rune.favorite.contexts",
        favoriteResourcesKey: String = "rune.favorite.resources",
        favoriteNamespacesKey: String = "rune.favorite.namespaces",
        manualNamespacesKey: String = "rune.manual.namespaces",
        preferredNamespacesKey: String = "rune.preferred.namespaces"
    ) {
        self.defaults = defaults
        self.favoriteContextsKey = favoriteContextsKey
        self.favoriteResourcesKey = favoriteResourcesKey
        self.favoriteNamespacesKey = favoriteNamespacesKey
        self.manualNamespacesKey = manualNamespacesKey
        self.preferredNamespacesKey = preferredNamespacesKey
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
}
