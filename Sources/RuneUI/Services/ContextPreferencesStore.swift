import Foundation

public protocol ContextPreferencesStoring {
    func loadFavoriteContextNames() -> Set<String>
    func saveFavoriteContextNames(_ names: Set<String>)
    func loadFavoriteResourceIDs() -> Set<String>
    func saveFavoriteResourceIDs(_ ids: Set<String>)
    func loadPreferredNamespace(for contextName: String) -> String?
    func savePreferredNamespace(_ namespace: String, for contextName: String)
}

public extension ContextPreferencesStoring {
    func loadPreferredNamespace(for contextName: String) -> String? {
        nil
    }

    func savePreferredNamespace(_ namespace: String, for contextName: String) {}

    func loadFavoriteResourceIDs() -> Set<String> {
        []
    }

    func saveFavoriteResourceIDs(_ ids: Set<String>) {}
}

public final class UserDefaultsContextPreferencesStore: ContextPreferencesStoring {
    private let defaults: UserDefaults
    private let favoriteContextsKey: String
    private let favoriteResourcesKey: String
    private let preferredNamespacesKey: String

    public init(
        defaults: UserDefaults = .standard,
        favoriteContextsKey: String = "rune.favorite.contexts",
        favoriteResourcesKey: String = "rune.favorite.resources",
        preferredNamespacesKey: String = "rune.preferred.namespaces"
    ) {
        self.defaults = defaults
        self.favoriteContextsKey = favoriteContextsKey
        self.favoriteResourcesKey = favoriteResourcesKey
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
}
