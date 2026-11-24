import Foundation

/// Reads the RFC 3339 timestamp a log line starts with, which is how lines
/// from several pods are put in order.
///
/// It runs once for every line of every pod, so it reads the bytes itself.
/// An `ISO8601DateFormatter` per line took 150 to 200 µs: seconds of work for
/// a deployment's logs.
struct KubernetesLogTimestamp: Comparable, Sendable {
    let seconds: Int
    let nanoseconds: Int

    static func < (lhs: Self, rhs: Self) -> Bool {
        (lhs.seconds, lhs.nanoseconds) < (rhs.seconds, rhs.nanoseconds)
    }

    /// The line without the timestamp the kubelet puts in front of it for
    /// `timestamps=true`, and the space after it.
    static func droppingLeadingTimestamp(_ line: some StringProtocol) -> String {
        let trimmed = line.drop { $0 == " " || $0 == "\t" }
        guard let space = trimmed.firstIndex(of: " ") else { return "" }
        return String(trimmed[trimmed.index(after: space)...])
    }

    /// The RFC 3339 timestamp at the start of the line, with nanosecond precision.
    static func leadingTimestamp(in line: some StringProtocol) -> Self? {
        let parsed = line.utf8.withContiguousStorageIfAvailable { parse($0) }
        if let parsed { return parsed }
        return Array(line.utf8.prefix(64)).withUnsafeBufferPointer { parse($0) }
    }

    private static func parse(_ bytes: UnsafeBufferPointer<UInt8>) -> Self? {
        var start = 0
        while start < bytes.count, bytes[start] == 0x20 || bytes[start] == 0x09 {
            start += 1
        }
        // YYYY-MM-DDTHH:MM:SS and at least a one-character zone.
        guard bytes.count - start >= 20 else { return nil }

        func digits(_ offset: Int, _ count: Int) -> Int? {
            var value = 0
            for index in (start + offset)..<(start + offset + count) {
                let digit = Int(bytes[index]) - 0x30
                guard (0...9).contains(digit) else { return nil }
                value = value * 10 + digit
            }
            return value
        }
        func byte(_ offset: Int) -> UInt8 { bytes[start + offset] }

        guard let year = digits(0, 4), byte(4) == 0x2D,
              let month = digits(5, 2), byte(7) == 0x2D,
              let day = digits(8, 2), byte(10) == 0x54,
              let hour = digits(11, 2), byte(13) == 0x3A,
              let minute = digits(14, 2), byte(16) == 0x3A,
              let second = digits(17, 2),
              (1...12).contains(month), (1...31).contains(day),
              hour <= 24, minute <= 59, second <= 59
        else { return nil }

        var index = start + 19
        var nanoseconds = 0
        if bytes[index] == 0x2E {
            index += 1
            var scale = 100_000_000
            var digitCount = 0
            while index < bytes.count {
                let digit = Int(bytes[index]) - 0x30
                guard (0...9).contains(digit) else { break }
                nanoseconds += digit * scale
                scale /= 10
                digitCount += 1
                index += 1
            }
            guard digitCount > 0 else { return nil }
        }

        guard index < bytes.count else { return nil }
        var offsetSeconds = 0
        switch bytes[index] {
        case 0x5A:
            break
        case 0x2B, 0x2D:
            let sign = bytes[index] == 0x2B ? 1 : -1
            index += 1
            func twoDigits() -> Int? {
                guard index + 1 < bytes.count else { return nil }
                let tens = Int(bytes[index]) - 0x30
                let ones = Int(bytes[index + 1]) - 0x30
                guard (0...9).contains(tens), (0...9).contains(ones) else { return nil }
                index += 2
                return tens * 10 + ones
            }
            guard let offsetHours = twoDigits() else { return nil }
            if index < bytes.count, bytes[index] == 0x3A { index += 1 }
            let offsetMinutes = twoDigits() ?? 0
            offsetSeconds = sign * (offsetHours * 3_600 + offsetMinutes * 60)
        default:
            return nil
        }

        let days = daysFromCivil(year: year, month: month, day: day)
        let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offsetSeconds
        return Self(seconds: seconds, nanoseconds: nanoseconds)
    }

    /// Days from 1970-01-01 to a date of the proleptic Gregorian calendar.
    /// A day past the month's end counts on into the next month.
    private static func daysFromCivil(year: Int, month: Int, day: Int) -> Int {
        let adjustedYear = month <= 2 ? year - 1 : year
        let era = (adjustedYear >= 0 ? adjustedYear : adjustedYear - 399) / 400
        let yearOfEra = adjustedYear - era * 400
        let dayOfYear = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1
        let dayOfEra = yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear
        return era * 146_097 + dayOfEra - 719_468
    }
}
