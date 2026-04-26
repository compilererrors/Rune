import Foundation

public enum RuneRuntimeDependencyPolicy {
    public static var allowsKubectlFallbacks: Bool {
        boolEnvironment("RUNE_ALLOW_KUBECTL_FALLBACKS") ?? defaultExternalToolFallbackPolicy
    }

    public static var allowsPATHHelmFallback: Bool {
        boolEnvironment("RUNE_ALLOW_PATH_HELM") ?? defaultExternalToolFallbackPolicy
    }

    private static var defaultExternalToolFallbackPolicy: Bool {
        #if DEBUG
        true
        #else
        false
        #endif
    }

    private static func boolEnvironment(_ name: String) -> Bool? {
        guard let raw = ProcessInfo.processInfo.environment[name]?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(),
              !raw.isEmpty else {
            return nil
        }
        switch raw {
        case "1", "true", "yes", "on":
            return true
        case "0", "false", "no", "off":
            return false
        default:
            return nil
        }
    }
}
