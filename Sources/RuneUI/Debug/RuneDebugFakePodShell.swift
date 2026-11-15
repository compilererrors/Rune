import Foundation
import RuneCore
import RuneKube

/// A stand-in pod shell for reproducing terminal problems without opening
/// shells on a real cluster. Enabled only by `RUNE_DEBUG_FAKE_SHELL=1`.
/// Connects after a short delay, prints a prompt, echoes what it is sent and
/// answers "burst" with a screenful of output.
public final class RuneDebugFakePodShell: PodTerminalSessionBackend, @unchecked Sendable {
    public static func fromEnvironment(_ environment: [String: String] = ProcessInfo.processInfo.environment) -> PodTerminalSessionBackend? {
        environment["RUNE_DEBUG_FAKE_SHELL"] == "1" ? RuneDebugFakePodShell() : nil
    }

    private let lock = NSLock()
    private var outputs: [String: @Sendable (String) -> Void] = [:]

    public init() {}

    public func startPodTerminalSession(
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
        try await Task.sleep(nanoseconds: 150_000_000)
        lock.withLock { outputs[sessionID] = onOutput }
        onOutput("\(podName):/app$ ")
    }

    public func writeToPodTerminalSession(id: String, text: String) async throws {
        guard let output = lock.withLock({ outputs[id] }) else { return }
        let command = text.trimmingCharacters(in: .newlines)
        if command == "burst" {
            output((0..<300).map { "line \($0) of streamed output\r\n" }.joined() + "$ ")
        } else {
            output(text.replacingOccurrences(of: "\n", with: "\r\n") + "$ ")
        }
    }

    public func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws {}

    public func stopPodTerminalSession(id: String) async {
        _ = lock.withLock { outputs.removeValue(forKey: id) }
    }
}
