import Foundation

/// Shell commands run in pod terminals, newest first, for the Terminal's
/// Commands tab. Kept on this Mac only, within the limit set in Settings.
public struct TerminalCommandHistory: Codable, Equatable, Sendable {
    public struct Entry: Codable, Equatable, Hashable, Sendable {
        public let command: String
        public let contextName: String
        public let namespace: String
        public let podName: String
        public let date: Date

        public init(command: String, contextName: String, namespace: String, podName: String, date: Date) {
            self.command = command
            self.contextName = contextName
            self.namespace = namespace
            self.podName = podName
            self.date = date
        }
    }

    public private(set) var entries: [Entry] = []

    public init(entries: [Entry] = []) {
        self.entries = entries
    }

    /// Records a command at the top. A command already kept moves up instead
    /// of repeating; a limit of 0 keeps nothing.
    public mutating func record(
        _ command: String,
        contextName: String,
        namespace: String,
        podName: String,
        at date: Date = Date(),
        limit: Int
    ) {
        let trimmed = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, limit > 0 else {
            apply(limit: limit)
            return
        }
        entries.removeAll { $0.command == trimmed }
        entries.insert(Entry(command: trimmed, contextName: contextName, namespace: namespace, podName: podName, date: date), at: 0)
        apply(limit: limit)
    }

    public mutating func apply(limit: Int) {
        let kept = max(0, limit)
        if entries.count > kept {
            entries.removeSubrange(kept...)
        }
    }

    public mutating func removeAll() {
        entries.removeAll()
    }
}
