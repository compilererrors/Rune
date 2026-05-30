import Foundation

public enum ShellCommandFormatting {
    private static let safeCharacters = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./:=+-")

    public static func displayCommand(executable: String, arguments: [String]) -> String {
        ([executable] + arguments).map(shellQuoted).joined(separator: " ")
    }

    public static func shellCommand(_ parts: [String]) -> String {
        parts.map(shellQuoted).joined(separator: " ")
    }

    public static func shellQuoted(_ value: String) -> String {
        guard !value.isEmpty else { return "''" }
        guard value.rangeOfCharacter(from: safeCharacters.inverted) == nil else {
            return "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
        }
        return value
    }
}
