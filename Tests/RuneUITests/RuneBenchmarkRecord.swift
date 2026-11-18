import Foundation

/// Writes a `RUNE_BENCHMARK` record that `scripts/compare-kpi-benchmarks.py`
/// reads from XCTest logs, so a KPI can be compared before and after a change.
enum RuneBenchmarkRecord {
    static func emit(_ name: String, samplesSeconds: [Double], iterations: Int = 1) {
        #if DEBUG
        let configuration = "debug"
        #else
        let configuration = "release"
        #endif
        let record: [String: Any] = [
            "name": name,
            "samples_seconds": samplesSeconds.map { max($0, 1e-9) },
            "iterations": iterations,
            "configuration": configuration,
        ]
        guard let data = try? JSONSerialization.data(withJSONObject: record, options: [.sortedKeys]) else { return }
        print("RUNE_BENCHMARK \(String(decoding: data, as: UTF8.self))")
    }

    static func seconds(_ duration: Duration) -> Double {
        Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
    }
}
