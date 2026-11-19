import RuneCore
import RuneKube
@testable import RuneUI
import XCTest

/// Release KPIs for the terminal paths that run on every key and every
/// chunk of output: keys typed in the terminal (encoding, cursor key mode,
/// command history) and the screen emulator under full-screen output
/// (scroll regions, as vim, less and top produce).
@MainActor
final class TerminalFeaturePerformanceKPITests: XCTestCase {
    func testTerminalKeystrokeHandlingKPI() async throws {
        // The history key by name, so the same KPI also builds against older code.
        let historyKey = "rune.terminal.commandHistory"
        let defaults = UserDefaults.standard
        let savedHistory = defaults.object(forKey: historyKey)
        defer { if let savedHistory { defaults.set(savedHistory, forKey: historyKey) } else { defaults.removeObject(forKey: historyKey) } }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state, terminalBackend: DiscardingShell(), bookmarkManager: .inMemory())
        state.setTerminalSession(PodTerminalSession(
            id: "shell-1", contextName: "sample-cluster", namespace: "team-alpha", podName: "api-0",
            shell: "sh", transcript: "$ ", status: .connected
        ))
        state.selectTerminalSession(id: "shell-1")
        // A typed command, an edit and Return: the keys a session sees most.
        let keys = Array("kubectl get pods -n team-alpha").map(String.init) + ["\u{7F}", "a", "\u{1B}[A", "\u{1B}[B", "\r"]

        var samples: [Double] = []
        for _ in 0..<10 {
            let started = ContinuousClock.now
            for _ in 0..<20 {
                for key in keys { viewModel.sendTerminalControlSequence(key) }
            }
            samples.append(RuneBenchmarkRecord.seconds(started.duration(to: .now)))
            await Task.yield()
        }
        RuneBenchmarkRecord.emit("terminal_keystroke_handling", samplesSeconds: samples, iterations: 20 * keys.count)
        let perKeyMicroseconds = samples.sorted()[samples.count / 2] / Double(20 * keys.count) * 1_000_000
        print("KPI terminal keystroke handling µs/key:", Int(perKeyMicroseconds))
        #if DEBUG
        XCTAssertLessThan(perKeyMicroseconds, 200, "a key typed in the terminal costs \(perKeyMicroseconds) µs")
        #else
        XCTAssertLessThan(perKeyMicroseconds, 60, "a key typed in the terminal costs \(perKeyMicroseconds) µs")
        #endif
    }

    func testTerminalEmulatorFullScreenOutputKPI() {
        // A full-screen tool redrawing inside a scroll region, then plain output.
        var stream = "\u{1B}[?1049h\u{1B}[?1h\u{1B}[2;23r"
        for line in 0..<3_000 {
            stream += "\u{1B}[23;1H\r\nrow \(line) \u{1B}[1mbold\u{1B}[0m value=\(line % 97)\u{1B}[K"
            if line % 50 == 0 { stream += "\u{1B}[1;1Hheader \(line)\u{1B}[24;1Hstatus \(line)\u{1B}[23;1H" }
        }
        stream += "\u{1B}[r\u{1B}[?1l\u{1B}[?1049l"
        for line in 0..<10_000 { stream += "2026-01-01T10:00:00Z output line \(line)\r\n" }
        let chunks = stride(from: 0, to: stream.utf16.count, by: 4_096).map { start -> String in
            let utf16 = Array(stream.utf16)
            return String(decoding: utf16[start..<min(start + 4_096, utf16.count)], as: UTF16.self)
        }

        var samples: [Double] = []
        for _ in 0..<5 {
            var emulator = TerminalScreenEmulator(columns: 120, rows: 24, maxScrollbackLines: 60_000)
            let started = ContinuousClock.now
            for chunk in chunks { emulator.feed(chunk) }
            _ = emulator.renderedText
            samples.append(RuneBenchmarkRecord.seconds(started.duration(to: .now)))
        }
        RuneBenchmarkRecord.emit("terminal_emulator_full_screen_output", samplesSeconds: samples)
        let median = samples.sorted()[samples.count / 2]
        let megabytes = Double(stream.utf8.count) / 1_000_000
        print("KPI terminal emulator: \(String(format: "%.1f", megabytes)) MB in \(Int(median * 1000)) ms")
        #if DEBUG
        XCTAssertLessThan(median, 4.0, "\(megabytes) MB of output took \(median) s")
        #else
        XCTAssertLessThan(median, 1.0, "\(megabytes) MB of output took \(median) s")
        #endif
    }
}

private final class DiscardingShell: PodTerminalSessionBackend, @unchecked Sendable {
    func startPodTerminalSession(
        id sessionID: String, from sources: [KubeConfigSource], context: KubeContext, namespace: String,
        podName: String, container: String?, shellCommand: [String],
        onOutput: @escaping @Sendable (String) -> Void, onTermination: @escaping @Sendable (Int32) -> Void
    ) async throws {}
    func writeToPodTerminalSession(id: String, text: String) async throws {}
    func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws {}
    func stopPodTerminalSession(id: String) async {}
}
