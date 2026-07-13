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

    func testBuildScriptMarksDirectDistributionByDefault() throws {
        let script = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let contents = try String(contentsOf: script, encoding: .utf8)

        XCTAssertTrue(contents.contains("APP_DISTRIBUTION=\"${APP_DISTRIBUTION:-direct}\""))
        XCTAssertTrue(contents.contains("<key>RuneDistribution</key>"))
        XCTAssertTrue(contents.contains("<string>__APP_DISTRIBUTION__</string>"))
    }

    func testBuildScriptEnablesExternalCommandsByDefaultWithBuildFlag() throws {
        let script = repositoryRoot.appendingPathComponent("scripts/build-macos-app.sh")
        let contents = try String(contentsOf: script, encoding: .utf8)

        XCTAssertTrue(contents.contains("ENABLE_EXTERNAL_COMMANDS=\"${ENABLE_EXTERNAL_COMMANDS:-1}\""))
        XCTAssertTrue(contents.contains("<key>RuneExternalCommandsEnabled</key>"))
        XCTAssertTrue(contents.contains("<__ENABLE_EXTERNAL_COMMANDS__/>"))
    }

    func testReleasePrivacyDoesNotLinkTelemetryAnalyticsOrUpdaterSDKs() throws {
        let forbiddenSDKFragments = [
            "Firebase",
            "Crashlytics",
            "Sentry",
            "Amplitude",
            "Mixpanel",
            "Segment",
            "PostHog",
            "TelemetryDeck",
            "AppCenter",
            "Countly",
            "Instabug",
            "Sparkle"
        ]
        let forbiddenSourceFragments = [
            "import Firebase",
            "FirebaseApp.configure",
            "Crashlytics.crashlytics",
            "import Sentry",
            "SentrySDK",
            "import Amplitude",
            "Amplitude.instance",
            "import Mixpanel",
            "Mixpanel.mainInstance",
            "import Segment",
            "SEGAnalytics",
            "import PostHog",
            "PostHogSDK",
            "import TelemetryDeck",
            "TelemetryDeck.",
            "import AppCenter",
            "MSAppCenter",
            "import Countly",
            "Countly.sharedInstance",
            "import Instabug",
            "Instabug.start",
            "import Sparkle",
            "SPUUpdater"
        ]
        let packageContents = try String(contentsOf: repositoryRoot.appendingPathComponent("Package.swift"), encoding: .utf8)

        XCTAssertTrue(packageContents.contains("github.com/jpsim/Yams.git"))
        for fragment in forbiddenSDKFragments {
            XCTAssertFalse(packageContents.contains(fragment), "Package.swift must not add telemetry, analytics, crash-reporting, or updater SDK \(fragment).")
        }

        let sources = repositoryRoot.appendingPathComponent("Sources")
        let enumerator = try XCTUnwrap(FileManager.default.enumerator(
            at: sources,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ))

        for case let fileURL as URL in enumerator where fileURL.pathExtension == "swift" {
            let contents = try String(contentsOf: fileURL, encoding: .utf8)
            for fragment in forbiddenSourceFragments {
                XCTAssertFalse(
                    contents.contains(fragment),
                    "\(fileURL.path) must not reference telemetry, analytics, crash-reporting, or updater SDK symbol \(fragment)."
                )
            }
        }
    }

    func testReleaseCopyDisclosesKubeconfigYamlReferenceSupport() throws {
        let readme = try String(contentsOf: repositoryRoot.appendingPathComponent("README.md"), encoding: .utf8)

        XCTAssertTrue(readme.contains("Rune supports common kubeconfig YAML references"))
        XCTAssertTrue(readme.contains("anchors"))
        XCTAssertTrue(readme.contains("aliases"))
        XCTAssertTrue(readme.contains("merge keys"))
        XCTAssertTrue(readme.contains("redacted in the preview"))
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

    func testBuiltAppBundleLaunchSmokeWhenExplicitlyProvided() throws {
        guard let bundlePath = ProcessInfo.processInfo.environment["RUNE_APP_BUNDLE_SMOKE_PATH"],
              !bundlePath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        else {
            throw XCTSkip("Set RUNE_APP_BUNDLE_SMOKE_PATH to a built Rune.app bundle to run the release launch smoke.")
        }

        let bundleURL = URL(fileURLWithPath: bundlePath)
        let plistURL = bundleURL.appendingPathComponent("Contents/Info.plist")
        let plistData = try Data(contentsOf: plistURL)
        let plist = try XCTUnwrap(PropertyListSerialization.propertyList(
            from: plistData,
            options: [],
            format: nil
        ) as? [String: Any])
        let executableName = try XCTUnwrap(plist["CFBundleExecutable"] as? String)
        let executableURL = bundleURL.appendingPathComponent("Contents/MacOS/\(executableName)")

        XCTAssertTrue(FileManager.default.isExecutableFile(atPath: executableURL.path))
        XCTAssertNotNil(plist["CFBundleShortVersionString"] as? String)
        XCTAssertNotNil(plist["CFBundleVersion"] as? String)

        let smokeHome = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-app-bundle-smoke-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: smokeHome, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: smokeHome) }

        let process = Process()
        process.executableURL = executableURL
        process.environment = [
            "HOME": smokeHome.path,
            "RUNE_LOG_TO_STDERR": "1",
            "RUNE_DIAGNOSTICS_LOGGING": "0",
            "RUNE_VERBOSE_DEBUG_TRACE": "0"
        ]
        process.standardOutput = Pipe()
        process.standardError = Pipe()

        try process.run()
        Thread.sleep(forTimeInterval: 3)

        if process.isRunning {
            process.terminate()
            process.waitUntilExit()
        } else {
            process.waitUntilExit()
            XCTFail("Rune app bundle exited during launch smoke with status \(process.terminationStatus).")
        }
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
