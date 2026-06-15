import Foundation

public enum RuneApplicationIdentifiers {
    public static let localBundleIdentifier = "app.rune.local"

    public static var bundleIdentifier: String {
        let identifier = Bundle.main.bundleIdentifier?.trimmingCharacters(in: .whitespacesAndNewlines)
        return identifier?.isEmpty == false ? identifier! : localBundleIdentifier
    }

    public static var keychainService: String { bundleIdentifier }
    public static let openActivityType = "\(localBundleIdentifier).open"
}
