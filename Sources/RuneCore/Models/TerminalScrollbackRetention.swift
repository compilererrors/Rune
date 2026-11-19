import Foundation

public enum TerminalScrollbackRetention {
    public static let truncationMarker = "[rune] Scrollback trimmed; older terminal output was discarded."

    public static func retainingRecentLines(_ text: String, maxLines: Int) -> String {
        guard maxLines > 0, !text.isEmpty else { return text }

        // Counted over UTF-8: this runs on the main thread for every piece
        // of shell output, and walking the transcript a character at a time
        // took tens of milliseconds for a long scrollback.
        var text = text
        let cutoffOffset: Int? = text.withUTF8 { bytes in
            var lineCount = bytes.last == 0x0A ? 0 : 1
            var index = bytes.count
            while index > 0 {
                index -= 1
                guard bytes[index] == 0x0A else { continue }
                lineCount += 1
                if lineCount > maxLines {
                    return index + 1
                }
            }
            return nil
        }

        guard let cutoffOffset else { return text }
        // A line feed is one byte, so the byte after it starts a character.
        let cutoff = text.utf8.index(text.utf8.startIndex, offsetBy: cutoffOffset)
        return "\(truncationMarker)\n\(text[cutoff...])"
    }
}
