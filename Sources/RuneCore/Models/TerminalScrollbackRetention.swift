import Foundation

public enum TerminalScrollbackRetention {
    public static let truncationMarker = "[rune] Scrollback trimmed; older terminal output was discarded."

    public static func retainingRecentLines(_ text: String, maxLines: Int) -> String {
        guard maxLines > 0, !text.isEmpty else { return text }

        var lineCount = text.last == "\n" ? 0 : 1
        var index = text.endIndex
        var cutoff: String.Index?

        while index > text.startIndex {
            text.formIndex(before: &index)
            guard text[index] == "\n" else { continue }
            lineCount += 1
            if lineCount > maxLines {
                cutoff = text.index(after: index)
                break
            }
        }

        guard let cutoff else { return text }
        let retained = text[cutoff...]
        return "\(truncationMarker)\n\(retained)"
    }
}
