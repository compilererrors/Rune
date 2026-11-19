import Foundation

extension UserDefaults {
    /// The value saved in the test runner's own preferences, ignoring the
    /// registered defaults. A test that restores a setting from
    /// `object(forKey:)` would otherwise save a registered default as if the
    /// user had chosen it. Only meaningful on `UserDefaults.standard`.
    func persistedTestValue(forKey key: String) -> Any? {
        guard let domain = Bundle.main.bundleIdentifier else {
            return object(forKey: key)
        }
        return persistentDomain(forName: domain)?[key]
    }
}
