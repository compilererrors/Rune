import Foundation
import RuneCore
import RuneKube

/// The demo cluster's pod shell. It runs in memory and never contacts a
/// cluster. Line editing works like a shell's: Left, Right, Home and End
/// move the cursor, Delete removes the character before it, Up and Down
/// walk the command history, Ctrl-C drops the line and Ctrl-D on an empty
/// line exits. It answers a few everyday commands with demo output.
final class RuneDemoPodShell: PodTerminalSessionBackend, @unchecked Sendable {
    private struct Session {
        let podName: String
        let output: @Sendable (String) -> Void
        let terminate: @Sendable (Int32) -> Void
        var line: [Character] = []
        var cursor = 0
        var history: [String] = []
        /// The history entry shown, while walking it; nil on a new line.
        var historyIndex: Int?
        /// The unsent line, kept while walking the history.
        var draft: [Character] = []
        /// Characters of an escape sequence split across writes.
        var pendingEscape = ""
        var prompt: String { "root@\(podName):/app# " }
    }

    private enum Key {
        case character(Character), enter, backspace, interrupt, endOfInput
        case left, right, home, end, up, down
    }

    private let lock = NSLock()
    private var sessions: [String: Session] = [:]

    func startPodTerminalSession(
        id sessionID: String,
        from sources: [KubeConfigSource],
        context: KubeContext,
        namespace: String,
        podName: String,
        container: String?,
        shellCommand: [String],
        onOutput: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) -> Void
    ) async throws {
        let session = Session(podName: podName, output: onOutput, terminate: onTermination)
        lock.withLock { sessions[sessionID] = session }
        // Answer after start returns, as a real shell's first output does, so
        // Rune's own "Connected" line comes before the banner and prompt.
        Task {
            try? await Task.sleep(nanoseconds: 30_000_000)
            onOutput("Rune demo shell for \(namespace)/\(podName). It runs in memory; no cluster is contacted. Type help.\r\n" + session.prompt)
        }
    }

    func writeToPodTerminalSession(id: String, text: String) async throws {
        guard var session = lock.withLock({ sessions[id] }) else { return }
        var echo = ""
        var ended = false
        for key in Self.keys(in: session.pendingEscape + text, pending: &session.pendingEscape) where !ended {
            echo += apply(key, to: &session, ended: &ended)
        }
        lock.withLock {
            if ended { sessions.removeValue(forKey: id) } else { sessions[id] = session }
        }
        if !echo.isEmpty { session.output(echo) }
        if ended { session.terminate(0) }
    }

    func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws {}

    func stopPodTerminalSession(id: String) async {
        _ = lock.withLock { sessions.removeValue(forKey: id) }
    }

    /// Keys from terminal input; an escape sequence cut off at the end is
    /// kept for the next write.
    private static func keys(in text: String, pending: inout String) -> [Key] {
        let characters = Array(text)
        var keys: [Key] = []
        var index = 0
        pending = ""
        while index < characters.count {
            let character = characters[index]
            if character == "\u{1B}" {
                guard index + 1 < characters.count else { pending = String(characters[index...]); break }
                let introducer = characters[index + 1]
                guard introducer == "[" || introducer == "O" else { index += 2; continue }
                var end = index + 2
                while end < characters.count, !(characters[end].isLetter || characters[end] == "~") { end += 1 }
                guard end < characters.count else { pending = String(characters[index...]); break }
                switch (characters[end], String(characters[(index + 2)..<end])) {
                case ("A", _): keys.append(.up)
                case ("B", _): keys.append(.down)
                case ("C", _): keys.append(.right)
                case ("D", _): keys.append(.left)
                case ("H", _), ("~", "1"), ("~", "7"): keys.append(.home)
                case ("F", _), ("~", "4"), ("~", "8"): keys.append(.end)
                default: break
                }
                index = end + 1
                continue
            }
            switch character {
            case "\r", "\n", "\r\n": keys.append(.enter)
            case "\u{7F}", "\u{8}": keys.append(.backspace)
            case "\u{3}": keys.append(.interrupt)
            case "\u{4}": keys.append(.endOfInput)
            case "\u{1}": keys.append(.home)
            case "\u{5}": keys.append(.end)
            default:
                if character.unicodeScalars.allSatisfy({ $0.value >= 0x20 }) { keys.append(.character(character)) }
            }
            index += 1
        }
        return keys
    }

    /// Applies one key and returns what the terminal should show.
    private func apply(_ key: Key, to session: inout Session, ended: inout Bool) -> String {
        switch key {
        case .character(let character):
            session.line.insert(character, at: session.cursor)
            session.cursor += 1
            let rest = session.line[session.cursor...]
            return String(character) + String(rest) + Self.left(rest.count)
        case .backspace:
            guard session.cursor > 0 else { return "" }
            session.cursor -= 1
            session.line.remove(at: session.cursor)
            let rest = session.line[session.cursor...]
            return "\u{8}" + String(rest) + " " + Self.left(rest.count + 1)
        case .left:
            guard session.cursor > 0 else { return "" }
            session.cursor -= 1
            return Self.left(1)
        case .right:
            guard session.cursor < session.line.count else { return "" }
            session.cursor += 1
            return "\u{1B}[C"
        case .home:
            defer { session.cursor = 0 }
            return Self.left(session.cursor)
        case .end:
            defer { session.cursor = session.line.count }
            let distance = session.line.count - session.cursor
            return distance > 0 ? "\u{1B}[\(distance)C" : ""
        case .up:
            guard !session.history.isEmpty else { return "" }
            if session.historyIndex == nil { session.draft = session.line }
            let index = max(0, (session.historyIndex ?? session.history.count) - 1)
            session.historyIndex = index
            return Self.replaceLine(with: Array(session.history[index]), in: &session)
        case .down:
            guard let index = session.historyIndex else { return "" }
            if index + 1 < session.history.count {
                session.historyIndex = index + 1
                return Self.replaceLine(with: Array(session.history[index + 1]), in: &session)
            }
            session.historyIndex = nil
            return Self.replaceLine(with: session.draft, in: &session)
        case .interrupt:
            session.line = []
            session.cursor = 0
            session.historyIndex = nil
            return "^C\r\n" + session.prompt
        case .endOfInput:
            guard session.line.isEmpty else { return "" }
            ended = true
            return "exit\r\n"
        case .enter:
            let line = String(session.line)
            session.line = []
            session.cursor = 0
            session.historyIndex = nil
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if !trimmed.isEmpty, session.history.last != line { session.history.append(line) }
            if trimmed == "exit" {
                ended = true
                return "\r\nexit\r\n"
            }
            return "\r\n" + Self.respond(to: line, podName: session.podName) + session.prompt
        }
    }

    private static func left(_ count: Int) -> String {
        count > 0 ? "\u{1B}[\(count)D" : ""
    }

    /// Shows another line in place of the one being edited.
    private static func replaceLine(with line: [Character], in session: inout Session) -> String {
        let output = left(session.cursor) + "\u{1B}[K" + String(line)
        session.line = line
        session.cursor = line.count
        return output
    }

    /// The output of one command line, each line ending in CR LF.
    static func respond(to line: String, podName: String) -> String {
        let words = line.split(separator: " ", omittingEmptySubsequences: true).map(String.init)
        guard let command = words.first else { return "" }
        let lines: [String]
        switch command {
        case "help":
            lines = ["Demo commands: ls, pwd, whoami, hostname, env, ps, date, uptime, echo, cat /etc/os-release, clear, exit"]
        case "ls":
            lines = words.contains(where: { $0.hasPrefix("-") && $0.contains("l") })
                ? ["total 16", "drwxr-xr-x 2 root root 4096 May  5 10:00 bin", "drwxr-xr-x 2 root root 4096 May  5 10:00 config",
                   "-rwxr-xr-x 1 root root 8192 May  5 10:00 server", "drwxr-xr-x 3 root root 4096 May  5 10:00 static"]
                : ["bin  config  server  static"]
        case "pwd":
            lines = ["/app"]
        case "whoami":
            lines = ["root"]
        case "hostname":
            lines = [podName]
        case "env", "printenv":
            lines = ["HOSTNAME=\(podName)", "APP_ENV=demo", "PORT=8080", "PATH=/usr/local/bin:/usr/bin:/bin"]
        case "ps":
            lines = ["PID   USER     TIME  COMMAND", "    1 root      0:01 /app/server", "   42 root      0:00 sh", "   57 root      0:00 ps"]
        case "date":
            let formatter = DateFormatter()
            formatter.locale = Locale(identifier: "en_US_POSIX")
            formatter.timeZone = TimeZone(identifier: "UTC")
            formatter.dateFormat = "EEE MMM d HH:mm:ss 'UTC' yyyy"
            lines = [formatter.string(from: Date())]
        case "uptime":
            lines = [" 10:18:00 up 18 min,  0 users,  load average: 0.08, 0.05, 0.01"]
        case "echo":
            lines = [words.dropFirst().joined(separator: " ")]
        case "cat" where words.dropFirst().first == "/etc/os-release":
            lines = ["NAME=\"Alpine Linux\"", "ID=alpine", "VERSION_ID=3.20.0", "PRETTY_NAME=\"Alpine Linux v3.20\""]
        case "clear":
            return "\u{1B}[2J\u{1B}[H"
        default:
            lines = ["sh: \(command): not available in the demo shell (type help)"]
        }
        return lines.map { $0 + "\r\n" }.joined()
    }
}
