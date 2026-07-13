import Foundation
import XCTest

final class AppStoreDistributionArtifactTests: XCTestCase {
    private var repositoryRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }

    func testTrackedEntitlementsEnableAppScopedBookmarks() throws {
        let url = repositoryRoot.appendingPathComponent("scripts/app-store.entitlements")
        let data = try Data(contentsOf: url)
        let plist = try XCTUnwrap(
            PropertyListSerialization.propertyList(from: data, format: nil) as? [String: Any]
        )

        XCTAssertEqual(plist["com.apple.security.app-sandbox"] as? Bool, true)
        XCTAssertEqual(plist["com.apple.security.network.client"] as? Bool, true)
        XCTAssertEqual(plist["com.apple.security.files.user-selected.read-write"] as? Bool, true)
        XCTAssertEqual(plist["com.apple.security.files.bookmarks.app-scope"] as? Bool, true)
    }

    func testArtifactGateAcceptsOnlySignedRestrictedAppStoreBundle() throws {
        let validApp = try makeSignedApp(
            distribution: "app-store",
            externalCommandsEnabled: false,
            includesAppScopedBookmarks: true
        )
        let validResult = try verify(app: validApp)
        XCTAssertEqual(validResult.status, 0, validResult.output)

        let directApp = try makeSignedApp(
            distribution: "direct",
            externalCommandsEnabled: true,
            includesAppScopedBookmarks: true
        )
        let directResult = try verify(app: directApp)
        XCTAssertNotEqual(directResult.status, 0)
        XCTAssertTrue(directResult.output.contains("RuneDistribution=direct"), directResult.output)

        let missingBookmarkApp = try makeSignedApp(
            distribution: "app-store",
            externalCommandsEnabled: false,
            includesAppScopedBookmarks: false
        )
        let missingBookmarkResult = try verify(app: missingBookmarkApp)
        XCTAssertNotEqual(missingBookmarkResult.status, 0)
        XCTAssertTrue(
            missingBookmarkResult.output.contains("com.apple.security.files.bookmarks.app-scope"),
            missingBookmarkResult.output
        )
    }

    private func makeSignedApp(
        distribution: String,
        externalCommandsEnabled: Bool,
        includesAppScopedBookmarks: Bool
    ) throws -> URL {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-artifact-gate-\(UUID().uuidString)", isDirectory: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: root) }

        let app = root.appendingPathComponent("Fixture.app", isDirectory: true)
        let contents = app.appendingPathComponent("Contents", isDirectory: true)
        let macOS = contents.appendingPathComponent("MacOS", isDirectory: true)
        try FileManager.default.createDirectory(at: macOS, withIntermediateDirectories: true)

        let executable = macOS.appendingPathComponent("Fixture")
        try FileManager.default.copyItem(
            at: URL(fileURLWithPath: "/usr/bin/true"),
            to: executable
        )

        let info: [String: Any] = [
            "CFBundleExecutable": "Fixture",
            "CFBundleIdentifier": "app.rune.artifact-fixture",
            "CFBundleName": "Fixture",
            "CFBundlePackageType": "APPL",
            "CFBundleShortVersionString": "1.0",
            "CFBundleVersion": "1",
            "RuneDistribution": distribution,
            "RuneExternalCommandsEnabled": externalCommandsEnabled
        ]
        try writePlist(info, to: contents.appendingPathComponent("Info.plist"))

        var entitlements: [String: Any] = [
            "com.apple.security.app-sandbox": true,
            "com.apple.security.network.client": true,
            "com.apple.security.files.user-selected.read-write": true
        ]
        if includesAppScopedBookmarks {
            entitlements["com.apple.security.files.bookmarks.app-scope"] = true
        }
        let entitlementsURL = root.appendingPathComponent("Entitlements.plist")
        try writePlist(entitlements, to: entitlementsURL)

        let signResult = try run(
            executable: URL(fileURLWithPath: "/usr/bin/codesign"),
            arguments: ["--force", "--sign", "-", "--entitlements", entitlementsURL.path, app.path]
        )
        XCTAssertEqual(signResult.status, 0, signResult.output)
        return app
    }

    private func verify(app: URL) throws -> (status: Int32, output: String) {
        try run(
            executable: repositoryRoot.appendingPathComponent("scripts/verify-app-store-artifact.sh"),
            arguments: [app.path]
        )
    }

    private func writePlist(_ value: Any, to url: URL) throws {
        let data = try PropertyListSerialization.data(
            fromPropertyList: value,
            format: .xml,
            options: 0
        )
        try data.write(to: url)
    }

    private func run(executable: URL, arguments: [String]) throws -> (status: Int32, output: String) {
        let process = Process()
        let pipe = Pipe()
        process.executableURL = executable
        process.arguments = arguments
        process.standardOutput = pipe
        process.standardError = pipe
        try process.run()
        process.waitUntilExit()
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        return (process.terminationStatus, String(decoding: data, as: UTF8.self))
    }
}
