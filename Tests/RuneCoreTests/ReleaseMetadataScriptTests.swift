import Foundation
import XCTest

final class ReleaseMetadataScriptTests: XCTestCase {
    private let metadata = [
        "BUNDLE_IDENTIFIER": "org.example.rune-fixture",
        "MARKETING_VERSION": "4.2",
        "BUNDLE_VERSION": "17"
    ]

    func testRepositoryMetadataOverridesAmbientValuesAndExportsToChildShell() throws {
        let fixture = try makeFixture(metadata: metadataText(metadata))
        let result = try run(in: fixture, command: """
        rune_load_release_metadata
        /bin/bash -c 'printf "EXPORTED=%s|%s|%s" "$BUNDLE_IDENTIFIER" "$MARKETING_VERSION" "$BUNDLE_VERSION"'
        """)

        XCTAssertEqual(result.status, 0, result.output)
        XCTAssertTrue(result.output.contains("EXPORTED=org.example.rune-fixture|4.2|17"), result.output)
    }

    func testMissingFileOrRequiredValueCannotFallBackToAmbientMetadata() throws {
        var cases: [String?] = [nil]
        for key in metadata.keys.sorted() {
            var missing = metadata
            missing.removeValue(forKey: key)
            cases.append(metadataText(missing))
            var empty = metadata
            empty[key] = ""
            cases.append(metadataText(empty))
        }

        for (index, contents) in cases.enumerated() {
            let fixture = try makeFixture(metadata: contents)
            let result = try run(in: fixture, command: "rune_load_release_metadata")
            XCTAssertNotEqual(result.status, 0, "Missing metadata case \(index) accepted ambient values: \(result.output)")
        }
    }

    func testMalformedMetadataIsRejected() throws {
        for (key, invalidValue) in [
            ("BUNDLE_IDENTIFIER", "org.example/rune"),
            ("BUNDLE_IDENTIFIER", "org.example.rune fixture"),
            ("MARKETING_VERSION", "4.2-beta"),
            ("MARKETING_VERSION", "4..2"),
            ("BUNDLE_VERSION", "seventeen")
        ] {
            var invalid = metadata
            invalid[key] = invalidValue
            let fixture = try makeFixture(metadata: metadataText(invalid))
            let result = try run(in: fixture, command: "rune_load_release_metadata")
            XCTAssertNotEqual(result.status, 0, "Accepted \(key)=\(invalidValue): \(result.output)")
        }
    }

    func testShellExportSyntaxAndExpressionsAreAccepted() throws {
        let fixture = try makeFixture(metadata: """
        export BUNDLE_IDENTIFIER='org.example.rune-fixture'
        export MARKETING_VERSION="4.2"
        export BUNDLE_VERSION="$((8 + 9))"
        """)
        let result = try run(in: fixture, command: """
        rune_load_release_metadata
        printf 'LOADED=%s|%s|%s' "$BUNDLE_IDENTIFIER" "$MARKETING_VERSION" "$BUNDLE_VERSION"
        """)

        XCTAssertEqual(result.status, 0, result.output)
        XCTAssertTrue(result.output.contains("LOADED=org.example.rune-fixture|4.2|17"), result.output)
    }

    func testAppVerificationRequiresAllThreeLoadedMetadataValues() throws {
        let expectedPlist = [
            "CFBundleIdentifier": metadata["BUNDLE_IDENTIFIER"]!,
            "CFBundleShortVersionString": metadata["MARKETING_VERSION"]!,
            "CFBundleVersion": metadata["BUNDLE_VERSION"]!
        ]
        let fixture = try makeFixture(metadata: metadataText(metadata))
        let appBundle = fixture.appendingPathComponent("Synthetic App.app")
        let contents = appBundle.appendingPathComponent("Contents", isDirectory: true)
        try FileManager.default.createDirectory(at: contents, withIntermediateDirectories: true)

        for changedKey in [nil] + expectedPlist.keys.sorted().map(Optional.some) {
            var plist = expectedPlist
            if let changedKey { plist[changedKey] = "mismatched" }
            try PropertyListSerialization.data(fromPropertyList: plist, format: .xml, options: 0)
                .write(to: contents.appendingPathComponent("Info.plist"))
            let result = try run(in: fixture, command: """
            rune_load_release_metadata
            rune_verify_release_metadata "$2"
            """, appBundle: appBundle)

            if let changedKey {
                XCTAssertNotEqual(result.status, 0, "Accepted mismatched \(changedKey): \(result.output)")
            } else {
                XCTAssertEqual(result.status, 0, result.output)
            }
        }
    }

    private func metadataText(_ values: [String: String]) -> String {
        values.keys.sorted().map { "\($0)='\(values[$0]!)'" }.joined(separator: "\n")
    }

    private func makeFixture(metadata: String?) throws -> URL {
        let fixture = FileManager.default.temporaryDirectory
            .appendingPathComponent("release metadata-\(UUID().uuidString)", isDirectory: true)
        addTeardownBlock { try FileManager.default.removeItem(at: fixture) }
        let scripts = fixture.appendingPathComponent("scripts", isDirectory: true)
        let local = fixture.appendingPathComponent(".local", isDirectory: true)
        try FileManager.default.createDirectory(at: scripts, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: local, withIntermediateDirectories: true)
        let repositoryRoot = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
        try FileManager.default.copyItem(
            at: repositoryRoot.appendingPathComponent("scripts/load-release-metadata.sh"),
            to: scripts.appendingPathComponent("load-release-metadata.sh")
        )
        try metadata?.write(to: local.appendingPathComponent("signing.env"), atomically: true, encoding: .utf8)
        return fixture
    }

    private func run(in fixture: URL, command: String, appBundle: URL? = nil) throws -> (status: Int32, output: String) {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/bin/bash")
        process.arguments = [
            "-c", "set -euo pipefail\nsource \"$1\"\n" + command,
            "release-metadata-test", fixture.appendingPathComponent("scripts/load-release-metadata.sh").path,
            appBundle?.path ?? ""
        ]
        process.currentDirectoryURL = fixture.deletingLastPathComponent()
        process.environment = [
            "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
            "BUNDLE_IDENTIFIER": "org.example.ambient",
            "MARKETING_VERSION": "9.9",
            "BUNDLE_VERSION": "999"
        ]
        let output = Pipe()
        process.standardOutput = output
        process.standardError = output
        try process.run()
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        return (process.terminationStatus, String(decoding: data, as: UTF8.self))
    }
}
