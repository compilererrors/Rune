import Foundation

public struct RuneKeyboardShortcut: Equatable, Hashable, Sendable {
    public let key: String
    public let requiresCommand: Bool
    public let requiresOption: Bool
    public let requiresShift: Bool

    public init?(
        key: String,
        requiresShift: Bool,
        requiresCommand: Bool = false,
        requiresOption: Bool = false
    ) {
        guard let normalizedKey = Self.normalizeKey(key) else { return nil }
        self.key = normalizedKey
        self.requiresCommand = requiresCommand
        self.requiresOption = requiresOption
        self.requiresShift = requiresShift
    }

    public init?(storageValue: String) {
        guard let normalized = Self.normalizeStorageValue(storageValue) else { return nil }
        let parts = normalized.split(separator: "-").map(String.init)
        guard let key = parts.last, parts.count >= 1 else { return nil }
        let modifiers = Set(parts.dropLast())
        self.key = key
        self.requiresCommand = modifiers.contains("command")
        self.requiresOption = modifiers.contains("option")
        self.requiresShift = modifiers.contains("shift")
    }

    public var storageValue: String {
        modifierStorageParts.joined(separator: "-") + (modifierStorageParts.isEmpty ? "" : "-") + key
    }

    public var displayValue: String {
        let modifiers = [
            requiresCommand ? "⌘" : nil,
            requiresOption ? "⌥" : nil,
            requiresShift ? "⇧" : nil
        ].compactMap { $0 }.joined()
        return modifiers + displayKey
    }

    public func matches(
        baseKey: String,
        requiresShift: Bool,
        requiresCommand: Bool = false,
        requiresOption: Bool = false
    ) -> Bool {
        guard let normalizedBaseKey = Self.normalizeKey(baseKey) else { return false }
        return key == normalizedBaseKey
            && self.requiresShift == requiresShift
            && self.requiresCommand == requiresCommand
            && self.requiresOption == requiresOption
    }

    public static func normalizeStorageValue(_ rawValue: String) -> String? {
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !trimmed.isEmpty else { return nil }

        let parts = trimmed.split(separator: "-").map(String.init)
        guard let rawKey = parts.last, let normalizedKey = normalizeKey(rawKey) else { return nil }

        var seenModifiers = Set<String>()
        for modifier in parts.dropLast() {
            guard ["command", "option", "shift"].contains(modifier) else { return nil }
            guard seenModifiers.insert(modifier).inserted else { return nil }
        }

        let orderedModifiers = ["command", "option", "shift"].filter { seenModifiers.contains($0) }
        return orderedModifiers.joined(separator: "-") + (orderedModifiers.isEmpty ? "" : "-") + normalizedKey
    }

    private static func normalizeKey(_ rawKey: String) -> String? {
        let trimmed = rawKey.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard trimmed.count == 1, let scalar = trimmed.unicodeScalars.first else { return nil }
        guard CharacterSet.alphanumerics.contains(scalar) || ["[", "]"].contains(trimmed) else { return nil }
        return trimmed
    }

    private var modifierStorageParts: [String] {
        [
            requiresCommand ? "command" : nil,
            requiresOption ? "option" : nil,
            requiresShift ? "shift" : nil
        ].compactMap { $0 }
    }

    private var displayKey: String {
        key.rangeOfCharacter(from: .alphanumerics) != nil ? key.uppercased() : key
    }
}

public enum RuneKeyBindingAction: String, CaseIterable, Identifiable, Sendable {
    case historyBack
    case historyForward
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
        case .historyBack: return "History Back"
        case .historyForward: return "History Forward"
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
        case .historyBack:
            return "Move back in Rune's navigation history."
        case .historyForward:
            return "Move forward in Rune's navigation history."
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
        case .historyBack:
            return RuneKeyboardShortcut(key: "[", requiresShift: false, requiresCommand: true, requiresOption: true)!
        case .historyForward:
            return RuneKeyboardShortcut(key: "]", requiresShift: false, requiresCommand: true, requiresOption: true)!
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
        case .historyBack: return RuneSettingsKeys.keyBindingHistoryBack
        case .historyForward: return RuneSettingsKeys.keyBindingHistoryForward
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
