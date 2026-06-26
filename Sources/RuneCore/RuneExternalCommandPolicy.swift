import Foundation

public enum RuneAppDistribution: String, Sendable, Equatable {
    case direct
    case appStore = "app-store"
}

public enum RuneExternalCommandPolicy {
    public static let distributionInfoPlistKey = "RuneDistribution"
    public static let distributionEnvironmentVariable = "RUNE_APP_DISTRIBUTION"
    public static let externalCommandsEnabledInfoPlistKey = "RuneExternalCommandsEnabled"
    public static let externalCommandsEnabledEnvironmentVariable = "RUNE_ENABLE_EXTERNAL_COMMANDS"

    public static var currentDistribution: RuneAppDistribution {
        distribution(
            infoDictionary: Bundle.main.infoDictionary ?? [:],
            environment: ProcessInfo.processInfo.environment
        )
    }

    public static var allowsExternalCommands: Bool {
        allowsExternalCommands(
            infoDictionary: Bundle.main.infoDictionary ?? [:],
            environment: ProcessInfo.processInfo.environment
        )
    }

    public static let disabledMessage = "This context requires CLI-backed auth, which is disabled in this Rune build. Use a static or native/import-guided kubeconfig where available, or use Rune's direct download build for full exec plugin compatibility."

    public static func distribution(
        infoDictionary: [String: Any],
        environment: [String: String]
    ) -> RuneAppDistribution {
        if let raw = environment[distributionEnvironmentVariable],
           let distribution = normalizedDistribution(raw) {
            return distribution
        }
        if let raw = infoDictionary[distributionInfoPlistKey] as? String,
           let distribution = normalizedDistribution(raw) {
            return distribution
        }
        return .direct
    }

    public static func externalCommandsEnabled(
        infoDictionary: [String: Any],
        environment: [String: String]
    ) -> Bool {
        if let raw = environment[externalCommandsEnabledEnvironmentVariable],
           let enabled = normalizedBoolean(raw) {
            return enabled
        }
        if let enabled = infoDictionary[externalCommandsEnabledInfoPlistKey] as? Bool {
            return enabled
        }
        if let raw = infoDictionary[externalCommandsEnabledInfoPlistKey] as? String,
           let enabled = normalizedBoolean(raw) {
            return enabled
        }
        return distribution(infoDictionary: infoDictionary, environment: environment) != .appStore
    }

    public static func allowsExternalCommands(
        infoDictionary: [String: Any],
        environment: [String: String]
    ) -> Bool {
        guard distribution(infoDictionary: infoDictionary, environment: environment) != .appStore else {
            return false
        }
        return externalCommandsEnabled(infoDictionary: infoDictionary, environment: environment)
    }

    private static func normalizedDistribution(_ raw: String) -> RuneAppDistribution? {
        let normalized = raw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        switch normalized {
        case "app-store", "appstore", "mas":
            return .appStore
        case "direct", "local", "notarized", "developer-id":
            return .direct
        default:
            return nil
        }
    }

    private static func normalizedBoolean(_ raw: String) -> Bool? {
        let normalized = raw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        switch normalized {
        case "1", "true", "yes", "on", "enabled":
            return true
        case "0", "false", "no", "off", "disabled":
            return false
        default:
            return nil
        }
    }
}
