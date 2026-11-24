import XCTest

/// Settings rows look the same on every pane: a 13 pt regular title, a small
/// secondary description, and controls that stay together.
final class RuneSettingsRowStyleTests: XCTestCase {
    func testRowTitlesAreRegularOnEveryPane() throws {
        let source = try preferencesSource()
        // Hand-built row labels used a bold title and a different description size.
        XCTAssertFalse(source.contains("Text(title)\n                    .runeInterfaceFont(weight: .semibold)"), "limit rows")
        XCTAssertFalse(source.contains("Text(\"Recent\")\n                                .runeInterfaceFont(weight: .semibold)"), "theme picker row")
        XCTAssertFalse(source.contains("Text(\"Theme JSON files\")\n                            .runeInterfaceFont(weight: .semibold)"), "custom themes row")
        XCTAssertFalse(source.contains("Text(action.title)\n                                .runeText(.microEmphasis)"), "key binding rows")
        XCTAssertFalse(source.contains("Text(\"Log file\")\n                            .runeText(.microEmphasis)"), "debug trace file row")
        XCTAssertFalse(source.contains(".runeInterfaceFont(relativeSize: -1)\n                    .foregroundStyle(.runeSecondary)\n                    .fixedSize(horizontal: false, vertical: true)"), "limit row descriptions")
    }

    private func block(from start: String, to end: String, in source: String) throws -> String {
        let lower = try XCTUnwrap(source.range(of: start), "missing \(start)")
        let upper = try XCTUnwrap(source.range(of: end, range: lower.upperBound..<source.endIndex), "missing \(end)")
        return String(source[lower.lowerBound..<upper.lowerBound])
    }

    private func preferencesSource() throws -> String {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift")
        return try String(contentsOf: url, encoding: .utf8)
    }
}
