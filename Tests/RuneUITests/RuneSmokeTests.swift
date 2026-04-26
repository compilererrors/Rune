import XCTest
@testable import RuneCore
@testable import RuneUI

final class RuneSmokeTests: XCTestCase {
    func testPrimaryNavigationContainsOnlyNativeRuntimeSections() {
        XCTAssertEqual(
            RuneSection.allCases.map(\.title),
            ["Overview", "Workloads", "Networking", "Storage", "Config", "RBAC", "Events", "Helm", "Terminal"]
        )
        XCTAssertEqual(RuneSection.allCases.map(\.commandShortcut), ["1", "2", "3", "4", "5", "6", "7", "8", "9"])
    }

    func testCommandPaletteCheatSheetExposesNativeHelmReader() async {
        let viewModel = await MainActor.run { RuneAppViewModel() }
        let titles = await MainActor.run {
            viewModel.commandPaletteItems(query: ":").map(\.title).joined(separator: "\n").lowercased()
        }

        XCTAssertTrue(titles.contains(":helm"))
        XCTAssertTrue(titles.contains(":po"))
        XCTAssertTrue(titles.contains(":svc"))
        XCTAssertTrue(titles.contains(":ctx"))
    }
}
