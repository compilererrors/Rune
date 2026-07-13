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
        let bundledDistribution = (infoDictionary[distributionInfoPlistKey] as? String)
            .flatMap(normalizedDistribution)

        // A signed App Store bundle is authoritative. Environment variables remain
        // useful for direct/development builds, but may only make policy stricter.
        if bundledDistribution == .appStore {
            return .appStore
        }
        if let raw = environment[distributionEnvironmentVariable],
           let distribution = normalizedDistribution(raw) {
            return distribution
        }
        if let bundledDistribution {
            return bundledDistribution
        }
        return .direct
    }

    public static func externalCommandsEnabled(
        infoDictionary: [String: Any],
        environment: [String: String]
    ) -> Bool {
        guard distribution(infoDictionary: infoDictionary, environment: environment) != .appStore else {
            return false
        }

        let bundledSetting: Bool?
        if let enabled = infoDictionary[externalCommandsEnabledInfoPlistKey] as? Bool {
            bundledSetting = enabled
        } else if let raw = infoDictionary[externalCommandsEnabledInfoPlistKey] as? String {
            bundledSetting = normalizedBoolean(raw)
        } else {
            bundledSetting = nil
        }

        // An explicit signed false value cannot be loosened by the process environment.
        if bundledSetting == false {
            return false
        }
        if let raw = environment[externalCommandsEnabledEnvironmentVariable],
           let enabled = normalizedBoolean(raw) {
            return enabled
        }
        if let bundledSetting {
            return bundledSetting
        }
        return true
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
