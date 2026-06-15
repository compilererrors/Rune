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

    func testBuildScriptDeclaresMacAppStoreCategory() throws {
        let script = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let contents = try String(contentsOf: script, encoding: .utf8)

        XCTAssertTrue(contents.contains("<key>LSApplicationCategoryType</key>"))
        XCTAssertTrue(contents.contains("<string>public.app-category.developer-tools</string>"))
    }

    func testBuildScriptDeclaresOnlyExemptEncryptionUse() throws {
        let script = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let contents = try String(contentsOf: script, encoding: .utf8)

        XCTAssertTrue(contents.contains("<key>ITSAppUsesNonExemptEncryption</key>"))
        XCTAssertTrue(contents.contains("<false/>"))
    }

    private func configuredLocalBundleIdentifier() throws -> String {
        let identifiers = repositoryRoot.appendingPathComponent("Sources/RuneCore/RuneApplicationIdentifiers.swift")
        let contents = try String(contentsOf: identifiers, encoding: .utf8)

        guard let assignment = contents.split(separator: "\n").first(where: { line in
            line.contains("static let localBundleIdentifier")
        }),
            let firstQuote = assignment.firstIndex(of: "\""),
            let lastQuote = assignment.lastIndex(of: "\""),
            firstQuote < lastQuote
        else {
            XCTFail("Could not locate configured bundle identifier")
            return ""
        }

        return String(assignment[assignment.index(after: firstQuote)..<lastQuote])
    }

    func testBuildScriptUsesLocalBundleIdentifierByDefault() throws {
        let script = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let contents = try String(contentsOf: script, encoding: .utf8)
        let bundleIdentifier = try configuredLocalBundleIdentifier()

        XCTAssertTrue(contents.contains("BUNDLE_IDENTIFIER=\"${BUNDLE_IDENTIFIER:-\(bundleIdentifier)}\""))
        XCTAssertTrue(contents.contains("<string>__BUNDLE_IDENTIFIER__</string>"))
        XCTAssertFalse(contents.contains("com.rune.app"))
        XCTAssertFalse(contents.contains("com.rune.desktop"))
        XCTAssertFalse(contents.contains("BUNDLE_IDENTIFIER=\"${BUNDLE_IDENTIFIER:-com."))
    }

    func testRuntimeApplicationIdentifierUsesBundleMetadataWithLocalFallback() throws {
        let identifiers = repositoryRoot.appendingPathComponent("Sources/RuneCore/RuneApplicationIdentifiers.swift")
        let contents = try String(contentsOf: identifiers, encoding: .utf8)

        XCTAssertTrue(contents.contains("Bundle.main.bundleIdentifier"))
        XCTAssertTrue(contents.contains("localBundleIdentifier"))
        XCTAssertFalse(contents.contains("static let bundleIdentifier = \""))
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
        ]

        for fragment in forbiddenDistributionFragments {
            XCTAssertFalse(contents.contains(fragment))
        }
    }

    func testBuildScriptCopiesSwiftPackageResourcesIntoAppBundle() throws {
        let script = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let contents = try String(contentsOf: script, encoding: .utf8)

        XCTAssertTrue(contents.contains("for resource_bundle in \"${BIN_DIR}\"/*.bundle"))
        XCTAssertTrue(contents.contains("cp -R \"${resource_bundle}\" \"${APP_BUNDLE}/Contents/Resources/\""))
        XCTAssertTrue(contents.contains("assets/rune_logo_main.png"))
        XCTAssertTrue(contents.contains("Contents/Resources/rune_logo_main.png"))
    }

    func testAppRegistersDefaultsBeforeConstructingRootViewModel() throws {
        let app = repositoryRoot.appendingPathComponent("Sources/RuneApp/RuneApp.swift")
        let contents = try String(contentsOf: app, encoding: .utf8)

        guard let initRange = contents.range(of: "init() {"),
              let bodyRange = contents.range(of: "var body: some Scene", range: initRange.upperBound..<contents.endIndex)
        else {
            XCTFail("Could not locate RuneApplication init/body")
            return
        }

        let initBlock = String(contents[initRange.lowerBound..<bodyRange.lowerBound])
        XCTAssertTrue(initBlock.contains("RuneSettingsKeys.registerDefaults()"))
        XCTAssertTrue(initBlock.contains("RuneLaunchEnvironment.applyProcessOverrides()"))
        XCTAssertTrue(initBlock.contains("_viewModel = StateObject("))
        XCTAssertTrue(initBlock.contains("contextPreferences: FileBackedContextPreferencesStore.applicationSupportStore()"))
        XCTAssertLessThan(
            initBlock.range(of: "RuneSettingsKeys.registerDefaults()")!.lowerBound,
            initBlock.range(of: "_viewModel = StateObject(")!.lowerBound
        )
    }
}
