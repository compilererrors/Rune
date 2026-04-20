import Foundation

public struct RuneKeyboardShortcut: Equatable, Hashable, Sendable {
    public let key: String
    public let requiresShift: Bool

    public init?(key: String, requiresShift: Bool) {
        guard let normalizedKey = Self.normalizeKey(key) else { return nil }
        self.key = normalizedKey
        self.requiresShift = requiresShift
    }

    public init?(storageValue: String) {
        guard let normalized = Self.normalizeStorageValue(storageValue) else { return nil }
        if normalized.hasPrefix("shift-") {
            self.key = String(normalized.dropFirst("shift-".count))
            self.requiresShift = true
        } else {
            self.key = normalized
            self.requiresShift = false
        }
    }

    public var storageValue: String {
        requiresShift ? "shift-\(key)" : key
    }

    public var displayValue: String {
        requiresShift ? "Shift-\(key.uppercased())" : key
    }

    public func matches(baseKey: String, requiresShift: Bool) -> Bool {
        guard let normalizedBaseKey = Self.normalizeKey(baseKey) else { return false }
        return key == normalizedBaseKey && self.requiresShift == requiresShift
    }

    public static func normalizeStorageValue(_ rawValue: String) -> String? {
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !trimmed.isEmpty else { return nil }

        if trimmed.hasPrefix("shift-") {
            let suffix = String(trimmed.dropFirst("shift-".count))
            guard let normalizedKey = normalizeKey(suffix) else { return nil }
            return "shift-\(normalizedKey)"
        }

        guard let normalizedKey = normalizeKey(trimmed) else { return nil }
        return normalizedKey
    }

    private static func normalizeKey(_ rawKey: String) -> String? {
        let trimmed = rawKey.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard trimmed.count == 1, let scalar = trimmed.unicodeScalars.first else { return nil }
        guard CharacterSet.alphanumerics.contains(scalar) else { return nil }
        return trimmed
    }
}

public enum RuneKeyBindingAction: String, CaseIterable, Identifiable, Sendable {
    case describe
    case logs
    case shell
    case yaml
    case portForward
    case rollout
    case helmValues
    case helmManifest
    case helmHistory

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .describe: return "Describe"
        case .logs: return "Logs"
        case .shell: return "Shell / Exec"
        case .yaml: return "YAML"
        case .portForward: return "Port Forward"
        case .rollout: return "Rollout"
        case .helmValues: return "Helm Values"
        case .helmManifest: return "Helm Manifest"
        case .helmHistory: return "Helm History"
        }
    }

    public var detail: String {
        switch self {
        case .describe:
            return "Open the describe inspector for the selected resource."
        case .logs:
            return "Open pod logs or unified service/deployment logs."
        case .shell:
            return "Open the pod exec pane. Mirrors the shell-style workflow where Rune supports it."
        case .yaml:
            return "Open the YAML manifest inspector for the selected resource."
        case .portForward:
            return "Open the port-forward pane for the selected pod or service."
        case .rollout:
            return "Open rollout actions/history for the selected deployment."
        case .helmValues:
            return "Open Helm release values for the selected release."
        case .helmManifest:
            return "Open Helm release manifest for the selected release."
        case .helmHistory:
            return "Open Helm release history for the selected release."
        }
    }

    public var defaultShortcut: RuneKeyboardShortcut {
        switch self {
        case .describe:
            return RuneKeyboardShortcut(key: "d", requiresShift: false)!
        case .logs:
            return RuneKeyboardShortcut(key: "l", requiresShift: false)!
        case .shell:
            return RuneKeyboardShortcut(key: "s", requiresShift: false)!
        case .yaml:
            return RuneKeyboardShortcut(key: "y", requiresShift: false)!
        case .portForward:
            return RuneKeyboardShortcut(key: "f", requiresShift: true)!
        case .rollout:
            return RuneKeyboardShortcut(key: "r", requiresShift: false)!
        case .helmValues:
            return RuneKeyboardShortcut(key: "v", requiresShift: false)!
        case .helmManifest:
            return RuneKeyboardShortcut(key: "m", requiresShift: false)!
        case .helmHistory:
            return RuneKeyboardShortcut(key: "h", requiresShift: false)!
        }
    }

    public var settingsKey: String {
        switch self {
        case .describe: return RuneSettingsKeys.keyBindingDescribe
        case .logs: return RuneSettingsKeys.keyBindingLogs
        case .shell: return RuneSettingsKeys.keyBindingShell
        case .yaml: return RuneSettingsKeys.keyBindingYAML
        case .portForward: return RuneSettingsKeys.keyBindingPortForward
        case .rollout: return RuneSettingsKeys.keyBindingRollout
        case .helmValues: return RuneSettingsKeys.keyBindingHelmValues
        case .helmManifest: return RuneSettingsKeys.keyBindingHelmManifest
        case .helmHistory: return RuneSettingsKeys.keyBindingHelmHistory
        }
    }
}

public extension UserDefaults {
    func runeKeyBindingShortcut(for action: RuneKeyBindingAction) -> RuneKeyboardShortcut {
        let rawValue = (object(forKey: action.settingsKey) as? String) ?? action.defaultShortcut.storageValue
        return RuneKeyboardShortcut(storageValue: rawValue) ?? action.defaultShortcut
    }

    func setRuneKeyBindingShortcut(_ shortcut: RuneKeyboardShortcut, for action: RuneKeyBindingAction) {
        set(shortcut.storageValue, forKey: action.settingsKey)
    }

    func resetRuneKeyBindingShortcuts() {
        for action in RuneKeyBindingAction.allCases {
            set(action.defaultShortcut.storageValue, forKey: action.settingsKey)
        }
    }
}
