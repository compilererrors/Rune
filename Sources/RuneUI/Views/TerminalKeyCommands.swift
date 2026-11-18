import AppKit
import RuneCore

/// The Terminal section's keyboard commands, as in Terminal, iTerm2, Ghostty
/// and Safari. The root view routes each one to the tab strip you are in
/// (the log tabs in the inspector, else the shell tabs).
enum TerminalKeyCommand: Equatable {
    case closeTab
    case newTab
    case reopenClosedTab
    case nextTab
    case previousTab
    case clear
    case biggerText
    case smallerText
    case defaultTextSize

    private static let tabKeyCode: UInt16 = 48
    private static let rightBracketKeyCode: UInt16 = 30
    private static let leftBracketKeyCode: UInt16 = 33

    init?(_ event: NSEvent) {
        guard event.type == .keyDown else { return nil }
        let modifiers = event.modifierFlags
            .intersection(.deviceIndependentFlagsMask)
            .subtracting([.capsLock, .numericPad, .function])
        let key = event.charactersIgnoringModifiers?.lowercased() ?? ""
        switch modifiers {
        case [.command]:
            switch key {
            case "w": self = .closeTab
            case "t": self = .newTab
            case "k": self = .clear
            case "=", "+": self = .biggerText
            case "-": self = .smallerText
            case "0": self = .defaultTextSize
            default: return nil
            }
        case [.command, .shift]:
            switch (key, event.keyCode) {
            case ("t", _): self = .reopenClosedTab
            case ("+", _), ("=", _): self = .biggerText
            case (_, Self.rightBracketKeyCode): self = .nextTab
            case (_, Self.leftBracketKeyCode): self = .previousTab
            default: return nil
            }
        case [.control] where event.keyCode == Self.tabKeyCode:
            self = .nextTab
        case [.control, .shift] where event.keyCode == Self.tabKeyCode:
            self = .previousTab
        default:
            return nil
        }
    }

    /// The terminal text size after a size command, one point a step,
    /// within the size limits Settings uses.
    static func textSize(after command: TerminalKeyCommand, from size: Double) -> Double {
        switch command {
        case .biggerText: return RuneSettingsKeys.clampedTerminalFontSize(size + 1)
        case .smallerText: return RuneSettingsKeys.clampedTerminalFontSize(size - 1)
        case .defaultTextSize: return RuneSettingsKeys.terminalFontSizeDefault
        default: return RuneSettingsKeys.clampedTerminalFontSize(size)
        }
    }

    /// Applies a size command to the text size every monospaced view shares.
    static func stepSharedTextSize(_ command: TerminalKeyCommand, defaults: UserDefaults = .standard) {
        let current = defaults.object(forKey: RuneSettingsKeys.terminalFontSize) as? Double
            ?? RuneSettingsKeys.terminalFontSizeDefault
        defaults.set(textSize(after: command, from: current), forKey: RuneSettingsKeys.terminalFontSize)
    }
}

/// The tab next to the active one, wrapping at the ends; nil with fewer
/// than two tabs.
enum TerminalTabCycle {
    static func adjacentID(in ids: [String], active: String?, offset: Int) -> String? {
        guard ids.count > 1 else { return nil }
        let current = active.flatMap { ids.firstIndex(of: $0) } ?? 0
        return ids[(current + offset % ids.count + ids.count) % ids.count]
    }
}

/// A text view that belongs to the shell itself (its transcript or prompt),
/// so Cmd-K there clears the shell instead of opening the command palette.
protocol RuneTerminalInputSurface: AnyObject {}

/// What a key sends to the pod shell, as Terminal does by default: arrows,
/// Home, End and the function keys as VT sequences (xterm modifier codes for
/// Shift and Control), Option-arrows and Option-Delete as word motions, and
/// any other Option key as the character it types (@, |, [ ] and $ on many
/// layouts). Nil leaves the key to the app.
enum TerminalKeyEncoding {
    static func sequence(for event: NSEvent) -> String? {
        let modifiers = event.modifierFlags
            .intersection(.deviceIndependentFlagsMask)
            .subtracting([.capsLock, .numericPad, .function])
        if modifiers.contains(.command) { return nil }
        let option = modifiers.contains(.option)
        let shift = modifiers.contains(.shift)
        let control = modifiers.contains(.control)
        let modifierCode = 1 + (shift ? 1 : 0) + (option ? 2 : 0) + (control ? 4 : 0)

        if let final = cursorFinal[event.keyCode] {
            switch (event.keyCode, modifiers) {
            case (123, [.option]): return "\u{1B}b"
            case (124, [.option]): return "\u{1B}f"
            default:
                return modifierCode == 1 ? "\u{1B}[\(final)" : "\u{1B}[1;\(modifierCode)\(final)"
            }
        }
        if let function = functionKeys[event.keyCode] { return function }
        switch event.keyCode {
        case 36, 76: return "\r"
        case 48: return shift ? "\u{1B}[Z" : "\t"
        case 51: return option ? "\u{1B}\u{7F}" : "\u{7F}"
        case 53: return "\u{1B}"
        case 116: return "\u{1B}[5~"
        case 117: return "\u{1B}[3~"
        case 121: return "\u{1B}[6~"
        default:
            guard let characters = event.characters, !characters.isEmpty,
                  !characters.unicodeScalars.contains(where: { (0xF700...0xF8FF).contains($0.value) }) else { return nil }
            return characters
        }
    }

    /// In application cursor mode (DECCKM) unmodified cursor keys, Home and
    /// End are sent as SS3 (`ESC O A`) instead of CSI (`ESC [ A`).
    static func applyingCursorKeyMode(_ sequence: String, application: Bool) -> String {
        guard application, sequence.count == 3, sequence.hasPrefix("\u{1B}["),
              let final = sequence.last, "ABCDHF".contains(final) else { return sequence }
        return "\u{1B}O\(final)"
    }

    private static let cursorFinal: [UInt16: Character] = [
        126: "A", 125: "B", 124: "C", 123: "D", 115: "H", 119: "F"
    ]

    private static let functionKeys: [UInt16: String] = [
        122: "\u{1B}OP", 120: "\u{1B}OQ", 99: "\u{1B}OR", 118: "\u{1B}OS",
        96: "\u{1B}[15~", 97: "\u{1B}[17~", 98: "\u{1B}[18~", 100: "\u{1B}[19~",
        101: "\u{1B}[20~", 109: "\u{1B}[21~", 103: "\u{1B}[23~", 111: "\u{1B}[24~"
    ]
}
