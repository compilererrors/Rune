import Foundation

public struct RuneKeyboardShortcut: Equatable, Hashable, Sendable {
    public let key: String
    public let requiresCommand: Bool
    public let requiresOption: Bool
    public let requiresControl: Bool
    public let requiresShift: Bool

    public init?(
        key: String,
        requiresShift: Bool,
        requiresCommand: Bool = false,
        requiresOption: Bool = false,
        requiresControl: Bool = false
    ) {
        guard let normalizedKey = Self.normalizeKey(key) else { return nil }
        self.key = normalizedKey
        self.requiresCommand = requiresCommand
        self.requiresOption = requiresOption
        self.requiresControl = requiresControl
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
        self.requiresControl = modifiers.contains("control")
        self.requiresShift = modifiers.contains("shift")
    }

    public var storageValue: String {
        modifierStorageParts.joined(separator: "-") + (modifierStorageParts.isEmpty ? "" : "-") + key
    }

    public var displayValue: String {
        let modifiers = [
            requiresCommand ? "⌘" : nil,
            requiresOption ? "⌥" : nil,
            requiresControl ? "⌃" : nil,
            requiresShift ? "⇧" : nil
        ].compactMap { $0 }.joined()
        return modifiers + displayKey
    }

    public func matches(
        baseKey: String,
        requiresShift: Bool,
        requiresCommand: Bool = false,
        requiresOption: Bool = false,
        requiresControl: Bool = false
    ) -> Bool {
        guard let normalizedBaseKey = Self.normalizeKey(baseKey) else { return false }
        return key == normalizedBaseKey
            && self.requiresShift == requiresShift
            && self.requiresCommand == requiresCommand
            && self.requiresOption == requiresOption
            && self.requiresControl == requiresControl
    }

    public static func normalizeStorageValue(_ rawValue: String) -> String? {
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !trimmed.isEmpty else { return nil }

        let parts = trimmed.split(separator: "-").map(String.init)
        guard let rawKey = parts.last, let normalizedKey = normalizeKey(rawKey) else { return nil }

        var seenModifiers = Set<String>()
        for modifier in parts.dropLast() {
            guard ["command", "option", "control", "shift"].contains(modifier) else { return nil }
            guard seenModifiers.insert(modifier).inserted else { return nil }
        }

        let orderedModifiers = ["command", "option", "control", "shift"].filter { seenModifiers.contains($0) }
        return orderedModifiers.joined(separator: "-") + (orderedModifiers.isEmpty ? "" : "-") + normalizedKey
    }

    private static func normalizeKey(_ rawKey: String) -> String? {
        let trimmed = rawKey.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if ["left", "right"].contains(trimmed) {
            return trimmed
        }
        guard trimmed.count == 1, let scalar = trimmed.unicodeScalars.first else { return nil }
        guard CharacterSet.alphanumerics.contains(scalar) || ["[", "]", "/", ":", "?"].contains(trimmed) else { return nil }
        return trimmed
    }

    private var modifierStorageParts: [String] {
        [
            requiresCommand ? "command" : nil,
            requiresOption ? "option" : nil,
            requiresControl ? "control" : nil,
            requiresShift ? "shift" : nil
        ].compactMap { $0 }
    }

    private var displayKey: String {
        if key == "left" {
            return "←"
        }
        if key == "right" {
            return "→"
        }
        return key.rangeOfCharacter(from: .alphanumerics) != nil ? key.uppercased() : key
    }
}

public enum RuneKeyBindingAction: String, CaseIterable, Identifiable, Sendable {
    case commandPalette
    case filterResources
    case historyBack
    case historyForward
    case focusPreviousPane
    case focusNextPane
    case describe
    case logs
    case saveLogs
    case shell
    case edit
    case yaml
    case delete
    case portForward
    case rollout

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .commandPalette: return "Command Palette"
        case .filterResources: return "Filter Resources"
        case .historyBack: return "History Back"
        case .historyForward: return "History Forward"
        case .focusPreviousPane: return "Focus Previous Pane"
        case .focusNextPane: return "Focus Next Pane"
        case .describe: return "Describe"
        case .logs: return "Logs"
        case .saveLogs: return "Save / Export"
        case .shell: return "Shell / Scale"
        case .edit: return "Edit"
        case .yaml: return "YAML"
        case .delete: return "Delete"
        case .portForward: return "Port Forward"
        case .rollout: return "Rollout"
        }
    }

    public var detail: String {
        switch self {
        case .commandPalette:
            return "Open Rune's command palette for resource, context, namespace, and action navigation."
        case .filterResources:
            return "Focus the current resource filter field."
        case .historyBack:
            return "Move back in Rune's navigation history."
        case .historyForward:
            return "Move forward in Rune's navigation history."
        case .focusPreviousPane:
            return "Move keyboard focus to the previous main pane."
        case .focusNextPane:
            return "Move keyboard focus to the next main pane."
        case .describe:
            return "Open the describe inspector for the selected resource."
        case .logs:
            return "Open pod logs or unified service/deployment logs."
        case .saveLogs:
            return "Save or export the active detail panel when supported: logs, YAML, Describe, rollout history, or Helm text."
        case .shell:
            return "Open pod exec, or deployment scale controls where Rune supports the k9s `s` workflow."
        case .edit:
            return "Open the editable YAML manifest sheet for the selected resource."
        case .yaml:
            return "Open the YAML manifest inspector for the selected resource."
        case .delete:
            return "Arm delete confirmation for the selected resource."
        case .portForward:
            return "Open the port-forward pane for the selected pod or service."
        case .rollout:
            return "Open rollout actions/history for the selected deployment."
        }
    }

    public var defaultShortcut: RuneKeyboardShortcut {
        switch self {
        case .commandPalette:
            return RuneKeyboardShortcut(key: ":", requiresShift: false)!
        case .filterResources:
            return RuneKeyboardShortcut(key: "/", requiresShift: false)!
        case .historyBack:
            return RuneKeyboardShortcut(key: "left", requiresShift: false, requiresCommand: true, requiresOption: true)!
        case .historyForward:
            return RuneKeyboardShortcut(key: "right", requiresShift: false, requiresCommand: true, requiresOption: true)!
        case .focusPreviousPane:
            return RuneKeyboardShortcut(key: "left", requiresShift: true, requiresCommand: true)!
        case .focusNextPane:
            return RuneKeyboardShortcut(key: "right", requiresShift: true, requiresCommand: true)!
        case .describe:
            return RuneKeyboardShortcut(key: "d", requiresShift: false)!
        case .logs:
            return RuneKeyboardShortcut(key: "l", requiresShift: false)!
        case .saveLogs:
            return RuneKeyboardShortcut(key: "s", requiresShift: false, requiresCommand: true)!
        case .shell:
            return RuneKeyboardShortcut(key: "s", requiresShift: false)!
        case .edit:
            return RuneKeyboardShortcut(key: "e", requiresShift: false)!
        case .yaml:
            return RuneKeyboardShortcut(key: "y", requiresShift: false)!
        case .delete:
            return RuneKeyboardShortcut(key: "d", requiresShift: false, requiresControl: true)!
        case .portForward:
            return RuneKeyboardShortcut(key: "f", requiresShift: true)!
        case .rollout:
            return RuneKeyboardShortcut(key: "r", requiresShift: false)!
        }
    }

    public var alternateShortcuts: [RuneKeyboardShortcut] {
        switch self {
        case .saveLogs:
            return [RuneKeyboardShortcut(key: "s", requiresShift: false, requiresControl: true)!]
        default:
            return []
        }
    }

    public var settingsKey: String {
        switch self {
        case .commandPalette: return RuneSettingsKeys.keyBindingCommandPalette
        case .filterResources: return RuneSettingsKeys.keyBindingFilterResources
        case .historyBack: return RuneSettingsKeys.keyBindingHistoryBack
        case .historyForward: return RuneSettingsKeys.keyBindingHistoryForward
        case .focusPreviousPane: return RuneSettingsKeys.keyBindingFocusPreviousPane
        case .focusNextPane: return RuneSettingsKeys.keyBindingFocusNextPane
        case .describe: return RuneSettingsKeys.keyBindingDescribe
        case .logs: return RuneSettingsKeys.keyBindingLogs
        case .saveLogs: return RuneSettingsKeys.keyBindingSaveLogs
        case .shell: return RuneSettingsKeys.keyBindingShell
        case .edit: return RuneSettingsKeys.keyBindingEdit
        case .yaml: return RuneSettingsKeys.keyBindingYAML
        case .delete: return RuneSettingsKeys.keyBindingDelete
        case .portForward: return RuneSettingsKeys.keyBindingPortForward
        case .rollout: return RuneSettingsKeys.keyBindingRollout
        }
    }
}

public enum RuneKeyBindingModifier: String, CaseIterable, Sendable, Hashable {
    case command
    case option
    case control
    case shift
    case function
}

public struct RuneKeyBindingInput: Sendable, Equatable {
    public let baseKey: String
    public let modifiers: Set<RuneKeyBindingModifier>
    public let isTextInputFocused: Bool
    public let isNavigationSuspended: Bool

    public init(
        baseKey: String,
        modifiers: Set<RuneKeyBindingModifier>,
        isTextInputFocused: Bool = false,
        isNavigationSuspended: Bool = false
    ) {
        self.baseKey = baseKey
        self.modifiers = modifiers
        self.isTextInputFocused = isTextInputFocused
        self.isNavigationSuspended = isNavigationSuspended
    }
}

public struct RuneKeyBindingResolver: Sendable {
    public typealias ShortcutProvider = @Sendable (RuneKeyBindingAction) -> RuneKeyboardShortcut

    private let shortcutProvider: ShortcutProvider

    public init(shortcutProvider: @escaping ShortcutProvider = { $0.defaultShortcut }) {
        self.shortcutProvider = shortcutProvider
    }

    public func action(for input: RuneKeyBindingInput) -> RuneKeyBindingAction? {
        guard !input.isNavigationSuspended else { return nil }
        guard !input.isTextInputFocused else { return nil }
        guard !input.modifiers.contains(.function) else { return nil }
        guard !input.baseKey.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return nil }

        return RuneKeyBindingAction.allCases.first { action in
            ([shortcutProvider(action)] + action.alternateShortcuts).contains { shortcut in
                shortcut.matches(
                    baseKey: input.baseKey,
                    requiresShift: input.modifiers.contains(.shift),
                    requiresCommand: input.modifiers.contains(.command),
                    requiresOption: input.modifiers.contains(.option),
                    requiresControl: input.modifiers.contains(.control)
                )
            }
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
