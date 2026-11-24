import Foundation
import XCTest
@testable import RuneKube

final class KubernetesLogTimestampTests: XCTestCase {
    /// What the previous implementation did for every line.
    private func formatterDate(_ token: String) -> Date? {
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return fractional.date(from: token) ?? ISO8601DateFormatter().date(from: token)
    }

    func testReadsTheTimestampsTheFormatterRead() throws {
        let tokens = [
            "2026-11-18T12:34:56Z",
            "2026-11-18T12:34:56.1Z",
            "2026-11-18T12:34:56.123Z",
            "2026-11-18T12:34:56+01:00",
            "2026-11-18T12:34:56-07:30",
            "2026-11-18T12:34:56+0100",
            "2026-11-18T12:34:56+01",
            "2026-11-18T12:34:56.250-00:00",
            "2000-02-29T23:59:59Z",
            "1999-12-31T00:00:00Z",
            "2026-02-30T12:34:56Z",
            "2026-11-18T24:00:00Z"
        ]
        for token in tokens {
            let expected = try XCTUnwrap(formatterDate(token), token)
            let parsed = try XCTUnwrap(KubernetesLogTimestamp.leadingTimestamp(in: "\(token) level=info started"), token)
            XCTAssertEqual(Double(parsed.seconds) + Double(parsed.nanoseconds) / 1_000_000_000, expected.timeIntervalSince1970, accuracy: 0.0005, token)
        }
    }

    func testKeepsNanosecondPrecisionWhenOrderingLogs() throws {
        let earlier = try XCTUnwrap(KubernetesLogTimestamp.leadingTimestamp(in: "2026-11-18T12:34:56.123456780Z a"))
        let later = try XCTUnwrap(KubernetesLogTimestamp.leadingTimestamp(in: "2026-11-18T12:34:56.123456789Z b"))
        XCTAssertLessThan(earlier, later)
        XCTAssertEqual(later.seconds, 1_795_005_296)
        XCTAssertEqual(later.nanoseconds, 123_456_789)
    }

    func testALineThatDoesNotStartWithATimestampHasNone() {
        let lines = [
            "",
            "INFO request handled",
            #"{"ts":"2026-11-18T12:34:56Z","msg":"json first"}"#,
            "2026-11-18 12:34:56Z space instead of T",
            "2026-11-18t12:34:56z lower case",
            "2026-11-18T12:34:56 no zone",
            "2026-11-18T12:34:56,123Z comma",
            "2026-11-18T12:34:56.Z empty fraction",
            "2026-11-18T12:34Z no seconds",
            "2026-13-18T12:34:56Z month",
            "2026-11-18T12:34:60Z leap second",
            "2026-11-18T12:61:00Z minute",
            "20261118T123456Z basic",
            "2026-11-18T12:34:5",
            "level=info 2026-11-18T12:34:56Z later in the line"
        ]
        for line in lines {
            XCTAssertNil(KubernetesLogTimestamp.leadingTimestamp(in: line), line)
        }
    }

    func testLeadingSpacesAndWhatFollowsTheTimestampDoNotMatter() {
        let expected = KubernetesLogTimestamp(seconds: 1_795_005_296, nanoseconds: 0)
        for line in ["  2026-11-18T12:34:56Z x", "\t2026-11-18T12:34:56Z", "2026-11-18T12:34:56Zjunk"] {
            XCTAssertEqual(KubernetesLogTimestamp.leadingTimestamp(in: line), expected, line)
        }
        let substring = "prefix|2026-11-18T12:34:56Z tail".dropFirst(7)
        XCTAssertEqual(KubernetesLogTimestamp.leadingTimestamp(in: substring), expected)
        let bridged = NSString(string: "2026-11-18T12:34:56Z bridged") as String
        XCTAssertEqual(KubernetesLogTimestamp.leadingTimestamp(in: bridged), expected)
    }

    func testParsingALargeLogStaysFarBelowTheFormatterCost() {
        let lines = (0..<20_000).map { index -> String in
            index.isMultiple(of: 2)
                ? "2026-11-18T12:34:\(String(format: "%02d", index % 60)).123456789Z level=info id=\(index)"
                : #"{"level":"info","id":\#(index)}"#
        }
        let started = ContinuousClock.now
        var parsed = 0
        for line in lines where KubernetesLogTimestamp.leadingTimestamp(in: line) != nil {
            parsed += 1
        }
        let elapsed = started.duration(to: .now)
        XCTAssertEqual(parsed, 10_000)
        // The formatter needed 3 to 4 seconds for these lines in a release build.
        XCTAssertLessThan(elapsed, .milliseconds(500), "20,000 lines took \(elapsed)")
    }

    func testDroppingTheKubeletTimestampKeepsTheRestOfTheLine() {
        XCTAssertEqual(
            KubernetesLogTimestamp.droppingLeadingTimestamp("2026-04-26T10:00:00.000000123Z 2026-04-26T10:00:00Z app ready"),
            "2026-04-26T10:00:00Z app ready"
        )
        XCTAssertEqual(KubernetesLogTimestamp.droppingLeadingTimestamp("2026-04-26T10:00:00.1Z   indented"), "  indented")
        XCTAssertEqual(KubernetesLogTimestamp.droppingLeadingTimestamp("2026-04-26T10:00:00Z"), "")
    }
}
