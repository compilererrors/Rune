import Foundation

public protocol ContextPreferencesStoring {
    func loadFavoriteContextNames() -> Set<String>
    func saveFavoriteContextNames(_ names: Set<String>)
}

public final class UserDefaultsContextPreferencesStore: ContextPreferencesStoring {
    private let defaults: UserDefaults
    private let key: String

    public init(defaults: UserDefaults = .standard, key: String = "rune.favorite.contexts") {
        self.defaults = defaults
        self.key = key
    }

    public func loadFavoriteContextNames() -> Set<String> {
        let names = defaults.stringArray(forKey: key) ?? []
        return Set(names)
    }

    public func saveFavoriteContextNames(_ names: Set<String>) {
        defaults.set(Array(names).sorted(), forKey: key)
    }
}
