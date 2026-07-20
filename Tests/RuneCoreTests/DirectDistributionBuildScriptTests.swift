import Foundation
import XCTest

final class DirectDistributionBuildScriptTests: XCTestCase {
    private var repositoryRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }

    func testLocalBuildSealsCompletedBundleAndFailsWhenStrictVerificationFails() throws {
        let scriptURL = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let source = try String(contentsOf: scriptURL, encoding: .utf8)

        let plistWrite = try XCTUnwrap(source.range(of: "${APP_BUNDLE}/Contents/Info.plist\"\n\nif ! command -v codesign"))
        let signing = try XCTUnwrap(source.range(of: "codesign \"${codesign_args[@]}\" \"${APP_BUNDLE}\""))
        let verification = try XCTUnwrap(
            source.range(of: "codesign --verify --deep --strict --verbose=2 \"${APP_BUNDLE}\"")
        )

        XCTAssertLessThan(plistWrite.lowerBound, signing.lowerBound)
        XCTAssertLessThan(signing.lowerBound, verification.lowerBound)
        XCTAssertTrue(source.contains("CODE_SIGN_IDENTITY=\"${CODE_SIGN_IDENTITY:--}\""))
        XCTAssertTrue(source.contains("set -euo pipefail"), "Strict verification must stop the build on failure")
    }
}
