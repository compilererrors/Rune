import XCTest

final class RuneAppBundleInfoPlistTests: XCTestCase {
    private var repositoryRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }

    func testBuildScriptKeepsATSOpenForUserConfiguredKubernetesAPIServers() throws {
        let script = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let contents = try String(contentsOf: script, encoding: .utf8)

        XCTAssertTrue(contents.contains("<key>NSAppTransportSecurity</key>"))
        XCTAssertTrue(contents.contains("<key>NSAllowsArbitraryLoads</key>"))
        XCTAssertTrue(contents.contains("<true/>"))
        XCTAssertTrue(contents.contains("kubeconfig CA/client settings"))
    }

    func testBuildScriptUsesConfiguredBundleIdentifierByDefault() throws {
        let script = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let contents = try String(contentsOf: script, encoding: .utf8)

        XCTAssertTrue(contents.contains("BUNDLE_IDENTIFIER=\"${BUNDLE_IDENTIFIER:-com.rune.local}\""))
        XCTAssertTrue(contents.contains("<string>__BUNDLE_IDENTIFIER__</string>"))
        XCTAssertFalse(contents.contains("com.rune.desktop"))
    }

    func testBuildScriptOnlyBuildsLocalUnsignedAppBundle() throws {
        let script = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let contents = try String(contentsOf: script, encoding: .utf8)

        XCTAssertTrue(contents.contains("swift build -c \"${CONFIGURATION}\" --product \"${PRODUCT_NAME}\""))
        XCTAssertTrue(contents.contains("cp \"${BIN_PATH}\" \"${APP_BUNDLE}/Contents/MacOS/${PRODUCT_NAME}\""))
        XCTAssertFalse(contents.contains("LOCAL_BUILD_HOOK"))
        XCTAssertFalse(contents.contains("source \"${LOCAL_BUILD_HOOK}\""))

        let forbiddenDistributionFragments = [
            "code" + "sign",
            "DISTRIBUTION=",
            "product" + "build",
            "xcrun " + "altool",
            "PROVISIONING_" + "PROFILE",
            "ASC_" + "API",
        ]

        for fragment in forbiddenDistributionFragments {
            XCTAssertFalse(contents.contains(fragment))
        }
    }
}
