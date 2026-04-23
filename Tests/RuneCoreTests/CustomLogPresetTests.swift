import XCTest
@testable import RuneCore

final class CustomLogPresetTests: XCTestCase {
    func testLargeCustomLinePresetMapsToAllTime() {
        let config = RuneCustomLogPresetConfig(
            mode: .lines,
            lines: 999_999,
            timeValue: 15,
            timeUnit: .minutes
        )

        XCTAssertEqual(config.filter, .all)
    }

    func testLargeCustomLinePresetTitleUsesSinceBeginning() {
        let config = RuneCustomLogPresetConfig(
            mode: .lines,
            lines: 999_999,
            timeValue: 15,
            timeUnit: .minutes
        )

        XCTAssertEqual(config.title(slot: .one), "Custom 1 (Since beginning)")
    }
}
