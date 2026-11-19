import Foundation

/// Reads the resource quantities of the metrics and node APIs ("250m",
/// "1500000n", "2", "128Mi", "1e3"). The text comes from a server, so every
/// function is total: anything that is not a finite number counts as zero,
/// and nothing is converted to `Int` before it is known to fit.
enum KubernetesQuantity {
    /// Far above any real cluster, far below what a sum of them overflows.
    private static let largestMagnitude = 1.0e12

    static func cpuMillicores(_ value: String?) -> Int {
        guard let value = value?.trimmingCharacters(in: .whitespaces), !value.isEmpty else { return 0 }
        let millicores: Double
        if let cores = finiteNumber(value) {
            millicores = cores * 1000
        } else if value.hasSuffix("n"), let raw = finiteNumber(value.dropLast()) {
            millicores = raw / 1_000_000
        } else if value.hasSuffix("u"), let raw = finiteNumber(value.dropLast()) {
            millicores = raw / 1_000
        } else if value.hasSuffix("m"), let raw = finiteNumber(value.dropLast()) {
            millicores = raw
        } else if value.hasSuffix("k"), let raw = finiteNumber(value.dropLast()) {
            millicores = raw * 1_000_000
        } else {
            millicores = 0
        }
        return clampedInt(min(max(millicores.rounded(), 0), largestMagnitude))
    }

    static func memoryBytes(_ value: String?) -> Double {
        guard let value = value?.trimmingCharacters(in: .whitespaces), !value.isEmpty else { return 0 }
        if let bytes = finiteNumber(value) {
            return clamped(bytes)
        }
        for unit in memoryUnits where value.hasSuffix(unit.suffix) {
            guard let raw = finiteNumber(value.dropLast(unit.suffix.count)) else { return 0 }
            return clamped(raw * unit.multiplier)
        }
        return 0
    }

    /// `Int(value)` traps on NaN, infinity and anything outside `Int`.
    static func clampedInt(_ value: Double) -> Int {
        guard value.isFinite else { return 0 }
        return Int(min(max(value, -largestMagnitude * 1.0e6), largestMagnitude * 1.0e6))
    }

    private static let memoryUnits: [(suffix: String, multiplier: Double)] = [
        ("Ki", 1024),
        ("Mi", 1024 * 1024),
        ("Gi", 1024 * 1024 * 1024),
        ("Ti", 1024 * 1024 * 1024 * 1024),
        ("Pi", 1024 * 1024 * 1024 * 1024 * 1024),
        ("Ei", 1024 * 1024 * 1024 * 1024 * 1024 * 1024),
        ("k", 1000),
        ("K", 1000),
        ("M", 1000 * 1000),
        ("G", 1000 * 1000 * 1000),
        ("T", 1000 * 1000 * 1000 * 1000),
        ("P", 1.0e15),
        ("E", 1.0e18),
        ("m", 0.001)
    ]

    /// A decimal number, with an exponent if it has one. `Double` also reads
    /// "inf", "nan" and hexadecimal floats, which are not quantities.
    private static func finiteNumber(_ text: some StringProtocol) -> Double? {
        guard let first = text.unicodeScalars.first,
              first == "-" || first == "+" || first == "." || ("0"..."9").contains(first),
              !text.unicodeScalars.contains(where: { $0 == "x" || $0 == "X" || $0 == "p" || $0 == "P" }),
              let number = Double(text), number.isFinite else { return nil }
        return number
    }

    private static func clamped(_ value: Double) -> Double {
        guard value.isFinite else { return 0 }
        return min(max(value, 0), largestMagnitude * 1.0e9)
    }
}
