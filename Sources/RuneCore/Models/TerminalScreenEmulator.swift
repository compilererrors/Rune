import Foundation

/// A bounded, incremental VT-style text screen used by interactive pod shells.
///
/// Rune intentionally keeps presentation attributes out of the persisted transcript,
/// but cursor movement and erase commands must still be applied for full-screen tools
/// to remain readable. The emulator accepts split control sequences and preserves the
/// primary screen while an application uses the alternate screen buffer.
public struct TerminalScreenEmulator: Sendable {
    private struct Screen: Sendable {
        var lines: [[Character]] = [[]]
        var cursorRow = 0
        var cursorColumn = 0
        var savedCursorRow = 0
        var savedCursorColumn = 0
    }

    private var screen = Screen()
    private var savedPrimaryScreen: Screen?
    private var scrollback: [String] = []
    private var scrollbackStartIndex = 0
    private var pendingControlSequence = ""
    private let maxScrollbackLines: Int
    private(set) public var columns: Int
    private(set) public var rows: Int

    public init(
        columns: Int = 80,
        rows: Int = 24,
        maxScrollbackLines: Int = 60_000,
        initialText: String = ""
    ) {
        self.columns = Self.clampedColumns(columns)
        self.rows = Self.clampedRows(rows)
        self.maxScrollbackLines = max(1, maxScrollbackLines)
        if !initialText.isEmpty {
            feed(initialText)
        }
    }

    public mutating func feed(_ text: String) {
        guard !text.isEmpty || !pendingControlSequence.isEmpty else { return }
        let input = Array(pendingControlSequence + text)
        pendingControlSequence = ""
        var index = 0

        while index < input.count {
            let character = input[index]
            if character == "\u{001B}" {
                guard consumeEscape(in: input, index: &index) else {
                    let suffix = input[index...]
                    pendingControlSequence = suffix.count <= 4_096 ? String(suffix) : ""
                    return
                }
                continue
            }

            switch character {
            case "\r\n":
                lineFeed()
                screen.cursorColumn = 0
            case "\r":
                screen.cursorColumn = 0
            case "\n":
                lineFeed()
                screen.cursorColumn = 0
            case "\u{0008}":
                screen.cursorColumn = max(0, screen.cursorColumn - 1)
            case "\t":
                screen.cursorColumn = min(columns - 1, ((screen.cursorColumn / 8) + 1) * 8)
            case "\u{0000}"..."\u{001F}", "\u{007F}":
                break
            default:
                write(character)
            }
            index += 1
        }
    }

    public mutating func resize(columns: Int, rows: Int) {
        self.columns = Self.clampedColumns(columns)
        self.rows = Self.clampedRows(rows)
        for index in screen.lines.indices where screen.lines[index].count > self.columns {
            screen.lines[index].removeSubrange(self.columns...)
        }
        screen.cursorColumn = min(screen.cursorColumn, self.columns - 1)
        while screen.lines.count > self.rows {
            let removed = screen.lines.removeFirst()
            if savedPrimaryScreen == nil {
                appendScrollback(Self.renderedLine(removed))
            }
            screen.cursorRow = max(0, screen.cursorRow - 1)
        }
        ensureCursorLine()
    }

    public mutating func reset() {
        screen = Screen()
        savedPrimaryScreen = nil
        scrollback.removeAll(keepingCapacity: true)
        scrollbackStartIndex = 0
        pendingControlSequence = ""
    }

    public var renderedText: String {
        var rendered = Array(scrollback.dropFirst(scrollbackStartIndex))
        let lastContentRow = screen.lines.lastIndex { !$0.isEmpty } ?? 0
        let lastVisibleRow = min(max(lastContentRow, screen.cursorRow), screen.lines.count - 1)
        rendered.append(contentsOf: screen.lines[0...lastVisibleRow].map(Self.renderedLine))
        while rendered.last?.isEmpty == true, rendered.count > 1,
              lastVisibleRow < screen.cursorRow {
            rendered.removeLast()
        }
        let text = rendered.joined(separator: "\n")
        let cursorIsOnEmptyLine = screen.cursorColumn == 0
            && screen.lines.indices.contains(screen.cursorRow)
            && screen.lines[screen.cursorRow].isEmpty
        return cursorIsOnEmptyLine && !text.isEmpty && !text.hasSuffix("\n") ? text + "\n" : text
    }

    private mutating func consumeEscape(in input: [Character], index: inout Int) -> Bool {
        let escapeIndex = index
        guard escapeIndex + 1 < input.count else { return false }
        let introducer = input[escapeIndex + 1]

        if introducer == "[" {
            var terminatorIndex = escapeIndex + 2
            while terminatorIndex < input.count {
                guard let scalar = input[terminatorIndex].unicodeScalars.first,
                      input[terminatorIndex].unicodeScalars.count == 1 else {
                    terminatorIndex += 1
                    continue
                }
                if (0x40...0x7E).contains(scalar.value) {
                    let parameters = String(input[(escapeIndex + 2)..<terminatorIndex])
                    applyCSI(parameters: parameters, command: input[terminatorIndex])
                    index = terminatorIndex + 1
                    return true
                }
                terminatorIndex += 1
            }
            return false
        }

        if introducer == "]" {
            var cursor = escapeIndex + 2
            while cursor < input.count {
                if input[cursor] == "\u{0007}" {
                    index = cursor + 1
                    return true
                }
                if input[cursor] == "\u{001B}" {
                    guard cursor + 1 < input.count else { return false }
                    if input[cursor + 1] == "\\" {
                        index = cursor + 2
                        return true
                    }
                }
                cursor += 1
            }
            return false
        }

        if ["P", "X", "^", "_"].contains(introducer) {
            var cursor = escapeIndex + 2
            while cursor < input.count {
                if input[cursor] == "\u{001B}" {
                    guard cursor + 1 < input.count else { return false }
                    if input[cursor + 1] == "\\" {
                        index = cursor + 2
                        return true
                    }
                }
                cursor += 1
            }
            return false
        }

        if ["(", ")", "*", "+", "-", ".", "/"].contains(introducer) {
            guard escapeIndex + 2 < input.count else { return false }
            index = escapeIndex + 3
            return true
        }

        switch introducer {
        case "7":
            saveCursor()
        case "8":
            restoreCursor()
        case "D":
            lineFeed()
        case "M":
            reverseIndex()
        case "E":
            lineFeed()
            screen.cursorColumn = 0
        case "c":
            screen = Screen()
            savedPrimaryScreen = nil
            scrollback.removeAll(keepingCapacity: true)
            scrollbackStartIndex = 0
        default:
            break
        }
        index = escapeIndex + 2
        return true
    }

    private mutating func applyCSI(parameters rawParameters: String, command: Character) {
        let isPrivate = rawParameters.first == "?"
        let normalized = rawParameters.drop(while: { $0 == "?" || $0 == ">" || $0 == "!" })
        let parameters = normalized.split(separator: ";", omittingEmptySubsequences: false).map {
            Int($0) ?? 0
        }
        let first = min(max(parameters.first ?? 0, 1), 1_000)

        switch command {
        case "A":
            screen.cursorRow = max(0, screen.cursorRow - first)
        case "B":
            screen.cursorRow = min(rows - 1, screen.cursorRow + first)
            ensureCursorLine()
        case "C":
            screen.cursorColumn = min(columns - 1, screen.cursorColumn + first)
        case "D":
            screen.cursorColumn = max(0, screen.cursorColumn - first)
        case "E":
            screen.cursorRow = min(rows - 1, screen.cursorRow + first)
            screen.cursorColumn = 0
            ensureCursorLine()
        case "F":
            screen.cursorRow = max(0, screen.cursorRow - first)
            screen.cursorColumn = 0
        case "G", "`":
            screen.cursorColumn = min(columns - 1, max(0, first - 1))
        case "d":
            screen.cursorRow = min(rows - 1, max(0, first - 1))
            ensureCursorLine()
        case "H", "f":
            let row = max(parameters.first ?? 1, 1) - 1
            let column = max(parameters.dropFirst().first ?? 1, 1) - 1
            screen.cursorRow = min(rows - 1, row)
            screen.cursorColumn = min(columns - 1, column)
            ensureCursorLine()
        case "J":
            eraseDisplay(mode: parameters.first ?? 0)
        case "K":
            eraseLine(mode: parameters.first ?? 0)
        case "P":
            deleteCharacters(first)
        case "@":
            insertCharacters(first)
        case "X":
            eraseCharacters(first)
        case "L":
            insertLines(first)
        case "M":
            deleteLines(first)
        case "S":
            scrollUp(first)
        case "T":
            scrollDown(first)
        case "s":
            saveCursor()
        case "u":
            restoreCursor()
        case "h" where isPrivate && parameters.contains(where: Self.isAlternateScreenMode):
            enterAlternateScreen()
        case "l" where isPrivate && parameters.contains(where: Self.isAlternateScreenMode):
            leaveAlternateScreen()
        default:
            // SGR, mode toggles, device queries and unsupported extensions do not
            // change the plain-text screen model.
            break
        }
    }

    private mutating func write(_ character: Character) {
        if screen.cursorColumn >= columns {
            lineFeed()
            screen.cursorColumn = 0
        }
        ensureCursorLine()
        var line = screen.lines[screen.cursorRow]
        if line.count < screen.cursorColumn {
            line.append(contentsOf: repeatElement(" ", count: screen.cursorColumn - line.count))
        }
        if line.indices.contains(screen.cursorColumn) {
            line[screen.cursorColumn] = character
        } else {
            line.append(character)
        }
        screen.lines[screen.cursorRow] = line
        screen.cursorColumn += 1
    }

    private mutating func lineFeed() {
        if screen.cursorRow >= rows - 1 {
            ensureCursorLine()
            let removed = screen.lines.removeFirst()
            if savedPrimaryScreen == nil {
                appendScrollback(Self.renderedLine(removed))
            }
            screen.lines.append([])
            screen.cursorRow = rows - 1
        } else {
            screen.cursorRow += 1
            ensureCursorLine()
        }
    }

    private mutating func reverseIndex() {
        if screen.cursorRow > 0 {
            screen.cursorRow -= 1
        } else {
            screen.lines.insert([], at: 0)
            if screen.lines.count > rows {
                screen.lines.removeLast()
            }
        }
    }

    private mutating func eraseDisplay(mode: Int) {
        ensureCursorLine()
        switch mode {
        case 1:
            for row in 0..<screen.cursorRow {
                screen.lines[row] = []
            }
            eraseLine(mode: 1)
        case 2, 3:
            screen.lines = Array(repeating: [], count: max(1, min(rows, screen.lines.count)))
            screen.cursorRow = 0
            screen.cursorColumn = 0
            if mode == 3, savedPrimaryScreen == nil {
                scrollback.removeAll(keepingCapacity: true)
                scrollbackStartIndex = 0
            }
        default:
            eraseLine(mode: 0)
            if screen.cursorRow + 1 < screen.lines.count {
                for row in (screen.cursorRow + 1)..<screen.lines.count {
                    screen.lines[row] = []
                }
            }
        }
    }

    private mutating func eraseLine(mode: Int) {
        ensureCursorLine()
        var line = screen.lines[screen.cursorRow]
        switch mode {
        case 1:
            let upperBound = min(screen.cursorColumn, max(0, line.count - 1))
            if !line.isEmpty {
                for column in 0...upperBound {
                    line[column] = " "
                }
            }
        case 2:
            line = []
        default:
            if screen.cursorColumn < line.count {
                line.removeSubrange(screen.cursorColumn...)
            }
        }
        screen.lines[screen.cursorRow] = line
    }

    private mutating func deleteCharacters(_ count: Int) {
        ensureCursorLine()
        var line = screen.lines[screen.cursorRow]
        guard screen.cursorColumn < line.count else { return }
        let end = min(line.count, screen.cursorColumn + count)
        line.removeSubrange(screen.cursorColumn..<end)
        screen.lines[screen.cursorRow] = line
    }

    private mutating func insertCharacters(_ count: Int) {
        ensureCursorLine()
        var line = screen.lines[screen.cursorRow]
        if line.count < screen.cursorColumn {
            line.append(contentsOf: repeatElement(" ", count: screen.cursorColumn - line.count))
        }
        line.insert(contentsOf: repeatElement(" ", count: count), at: screen.cursorColumn)
        if line.count > columns {
            line.removeSubrange(columns...)
        }
        screen.lines[screen.cursorRow] = line
    }

    private mutating func eraseCharacters(_ count: Int) {
        ensureCursorLine()
        var line = screen.lines[screen.cursorRow]
        if line.count < screen.cursorColumn + count {
            line.append(contentsOf: repeatElement(" ", count: screen.cursorColumn + count - line.count))
        }
        for column in screen.cursorColumn..<min(columns, screen.cursorColumn + count) {
            line[column] = " "
        }
        screen.lines[screen.cursorRow] = line
    }

    private mutating func insertLines(_ count: Int) {
        ensureCursorLine()
        for _ in 0..<min(count, rows) {
            screen.lines.insert([], at: screen.cursorRow)
            if screen.lines.count > rows {
                screen.lines.removeLast()
            }
        }
    }

    private mutating func deleteLines(_ count: Int) {
        ensureCursorLine()
        for _ in 0..<min(count, rows) where screen.lines.indices.contains(screen.cursorRow) {
            screen.lines.remove(at: screen.cursorRow)
            screen.lines.append([])
        }
    }

    private mutating func scrollUp(_ count: Int) {
        for _ in 0..<min(count, rows) {
            ensureCursorLine()
            let removed = screen.lines.removeFirst()
            if savedPrimaryScreen == nil {
                appendScrollback(Self.renderedLine(removed))
            }
            screen.lines.append([])
        }
    }

    private mutating func scrollDown(_ count: Int) {
        for _ in 0..<min(count, rows) {
            screen.lines.insert([], at: 0)
            if screen.lines.count > rows {
                screen.lines.removeLast()
            }
        }
    }

    private mutating func saveCursor() {
        screen.savedCursorRow = screen.cursorRow
        screen.savedCursorColumn = screen.cursorColumn
    }

    private mutating func restoreCursor() {
        screen.cursorRow = min(max(0, screen.savedCursorRow), rows - 1)
        screen.cursorColumn = min(max(0, screen.savedCursorColumn), columns - 1)
        ensureCursorLine()
    }

    private mutating func enterAlternateScreen() {
        guard savedPrimaryScreen == nil else { return }
        savedPrimaryScreen = screen
        screen = Screen()
    }

    private mutating func leaveAlternateScreen() {
        guard let primary = savedPrimaryScreen else { return }
        screen = primary
        savedPrimaryScreen = nil
    }

    private mutating func ensureCursorLine() {
        screen.cursorRow = min(max(0, screen.cursorRow), rows - 1)
        while screen.lines.count <= screen.cursorRow {
            screen.lines.append([])
        }
    }

    private mutating func appendScrollback(_ line: String) {
        scrollback.append(line)
        let retainedCount = scrollback.count - scrollbackStartIndex
        if retainedCount > maxScrollbackLines {
            scrollbackStartIndex += retainedCount - maxScrollbackLines
        }
        if scrollbackStartIndex >= 1_024, scrollbackStartIndex * 2 >= scrollback.count {
            scrollback.removeFirst(scrollbackStartIndex)
            scrollbackStartIndex = 0
        }
    }

    private static func renderedLine(_ line: [Character]) -> String {
        var trimmed = line
        while trimmed.last == " " {
            trimmed.removeLast()
        }
        return String(trimmed)
    }

    private static func isAlternateScreenMode(_ value: Int) -> Bool {
        value == 47 || value == 1047 || value == 1049
    }

    private static func clampedColumns(_ value: Int) -> Int {
        min(max(value, 1), 500)
    }

    private static func clampedRows(_ value: Int) -> Int {
        min(max(value, 1), 200)
    }
}
