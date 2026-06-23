import Foundation
import XCTest
@testable import RuneExport

@MainActor
final class ConfiguredExportServiceTests: XCTestCase {
    func testSaveAndOpenWritesToConfiguredFolderAndUsesPreferredOpener() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(
                folderURL: folderURL,
                textOpenerBundleIdentifier: "com.example.TextViewer"
            ),
            opener: opener
        )

        let savedURL = try exporter.save(
            data: Data("line one\n".utf8),
            suggestedName: "pod-api-logs.log",
            allowedFileTypes: ["log", "txt"],
            kind: .plainText,
            openAfterSave: true
        )

        XCTAssertEqual(savedURL.deletingLastPathComponent(), folderURL)
        XCTAssertEqual(savedURL.lastPathComponent, "pod-api-logs.log")
        XCTAssertEqual(try String(contentsOf: savedURL, encoding: .utf8), "line one\n")
        XCTAssertEqual(opener.opened, [RecordingExportFileOpener.Opened(url: savedURL, bundleIdentifier: "com.example.TextViewer")])
    }

    func testSaveAndOpenFallsBackToSystemDefaultWhenNoPreferredOpenerIsConfigured() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: opener
        )

        let savedURL = try exporter.save(
            data: Data("archive".utf8),
            suggestedName: "logs.zip",
            allowedFileTypes: ["zip"],
            kind: .archive,
            openAfterSave: true
        )

        XCTAssertEqual(opener.opened, [RecordingExportFileOpener.Opened(url: savedURL, bundleIdentifier: nil)])
    }

    func testArchiveSaveAndOpenUsesPreferredArchiveOpenerAndUniqueDestination() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(
                folderURL: folderURL,
                archiveOpenerBundleIdentifier: "com.example.ArchiveViewer"
            ),
            opener: opener
        )
        try Data("existing".utf8).write(to: folderURL.appendingPathComponent("logs.zip"))

        let savedURL = try exporter.save(
            data: Data("archive".utf8),
            suggestedName: "logs.zip",
            allowedFileTypes: ["zip"],
            kind: .archive,
            openAfterSave: true
        )

        XCTAssertEqual(savedURL.lastPathComponent, "logs-2.zip")
        XCTAssertEqual(try Data(contentsOf: savedURL), Data("archive".utf8))
        XCTAssertEqual(opener.opened, [RecordingExportFileOpener.Opened(url: savedURL, bundleIdentifier: "com.example.ArchiveViewer")])
    }

    func testSaveUsesNextAvailableNameAfterManyExistingCollisions() throws {
        let folderURL = try makeTemporaryDirectory()
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: RecordingExportFileOpener()
        )
        for index in 0...40 {
            let name = index == 0 ? "logs.yaml" : "logs-\(index + 1).yaml"
            try Data("existing".utf8).write(to: folderURL.appendingPathComponent(name))
        }

        let savedURL = try exporter.save(
            data: Data("apiVersion: v1\n".utf8),
            suggestedName: "logs.yaml",
            allowedFileTypes: ["yaml"],
            kind: .plainText,
            openAfterSave: false
        )

        XCTAssertEqual(savedURL.lastPathComponent, "logs-42.yaml")
        XCTAssertEqual(try String(contentsOf: savedURL, encoding: .utf8), "apiVersion: v1\n")
    }

    func testSaveAndOpenRetainsSecurityScopeForOpenHandoff() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: true)
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: opener,
            securityScopedAccess: securityScope
        )

        let savedURL = try exporter.save(
            data: Data("line one\n".utf8),
            suggestedName: "logs.log",
            allowedFileTypes: ["log"],
            kind: .plainText,
            openAfterSave: true
        )

        XCTAssertEqual(opener.opened, [RecordingExportFileOpener.Opened(url: savedURL, bundleIdentifier: nil)])
        XCTAssertEqual(securityScope.started, [folderURL])
        XCTAssertTrue(securityScope.stopped.isEmpty)
        XCTAssertEqual(securityScope.stoppedAfterOpenHandoff, [folderURL])
    }

    func testSaveAndOpenDoesNotStopSecurityScopeWhenScopeWasNotStarted() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: false)
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: opener,
            securityScopedAccess: securityScope
        )

        let savedURL = try exporter.save(
            data: Data("line one\n".utf8),
            suggestedName: "logs.log",
            allowedFileTypes: ["log"],
            kind: .plainText,
            openAfterSave: true
        )

        XCTAssertEqual(opener.opened, [RecordingExportFileOpener.Opened(url: savedURL, bundleIdentifier: nil)])
        XCTAssertEqual(securityScope.started, [folderURL])
        XCTAssertTrue(securityScope.stopped.isEmpty)
        XCTAssertTrue(securityScope.stoppedAfterOpenHandoff.isEmpty)
    }

    func testSaveAndOpenStopsSecurityScopeIfOpenFails() throws {
        let folderURL = try makeTemporaryDirectory()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: true)
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: ThrowingExportFileOpener(error: RecordingExportError.openFailed),
            securityScopedAccess: securityScope
        )

        XCTAssertThrowsError(
            try exporter.save(
                data: Data("line one\n".utf8),
                suggestedName: "logs.log",
                allowedFileTypes: ["log"],
                kind: .plainText,
                openAfterSave: true
            )
        ) { error in
            XCTAssertEqual(error as? RecordingExportError, .openFailed)
        }
        XCTAssertEqual(securityScope.started, [folderURL])
        XCTAssertEqual(securityScope.stopped, [folderURL])
        XCTAssertTrue(securityScope.stoppedAfterOpenHandoff.isEmpty)
    }

    func testSaveWithoutOpenStopsSecurityScopeImmediately() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: true)
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: opener,
            securityScopedAccess: securityScope
        )

        _ = try exporter.save(
            data: Data("line one\n".utf8),
            suggestedName: "logs.log",
            allowedFileTypes: ["log"],
            kind: .plainText,
            openAfterSave: false
        )

        XCTAssertTrue(opener.opened.isEmpty)
        XCTAssertEqual(securityScope.started, [folderURL])
        XCTAssertEqual(securityScope.stopped, [folderURL])
        XCTAssertTrue(securityScope.stoppedAfterOpenHandoff.isEmpty)
    }

    func testPrivacySafeFilenamesDoNotUseSuggestedResourceNames() throws {
        let folderURL = try makeTemporaryDirectory()
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL, usesPrivacySafeFilenames: true),
            opener: RecordingExportFileOpener()
        )

        let savedURL = try exporter.save(
            data: Data("line one\n".utf8),
            suggestedName: "pod-prod-api-logs.log",
            allowedFileTypes: ["log", "txt"],
            kind: .plainText,
            openAfterSave: false
        )

        XCTAssertTrue(savedURL.lastPathComponent.hasPrefix("rune-export-"))
        XCTAssertTrue(savedURL.lastPathComponent.hasSuffix(".log"))
        XCTAssertFalse(savedURL.lastPathComponent.contains("prod"))
        XCTAssertFalse(savedURL.lastPathComponent.contains("api"))
    }

    func testMissingConfiguredFolderReturnsRecoveryError() {
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: nil),
            opener: RecordingExportFileOpener()
        )

        XCTAssertThrowsError(
            try exporter.save(
                data: Data("line one\n".utf8),
                suggestedName: "logs.log",
                allowedFileTypes: ["log"],
                kind: .plainText,
                openAfterSave: false
            )
        ) { error in
            XCTAssertEqual(error as? FileExportError, .missingConfiguredExportFolder)
        }
    }

    func testUnavailableConfiguredFolderReturnsRecoveryErrorWithoutOpeningFile() {
        let opener = RecordingExportFileOpener()
        let exporter = ConfiguredFolderExporter(
            resolver: ThrowingExportDestinationResolver(error: FileExportError.exportFolderUnavailable),
            opener: opener
        )

        XCTAssertThrowsError(
            try exporter.save(
                data: Data("line one\n".utf8),
                suggestedName: "logs.log",
                allowedFileTypes: ["log"],
                kind: .plainText,
                openAfterSave: true
            )
        ) { error in
            XCTAssertEqual(error as? FileExportError, .exportFolderUnavailable)
        }
        XCTAssertTrue(opener.opened.isEmpty)
    }

    private func makeTemporaryDirectory() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneConfiguredExportServiceTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }
}

private struct StaticExportDestinationResolver: ExportDestinationResolving {
    let folderURL: URL?
    var textOpenerBundleIdentifier: String?
    var archiveOpenerBundleIdentifier: String?
    var usesPrivacySafeFilenames = false

    func exportFolderURL() throws -> URL? {
        folderURL
    }

    func preferredOpenerBundleIdentifier(for kind: ConfiguredExportFileKind) -> String? {
        switch kind {
        case .plainText:
            return textOpenerBundleIdentifier
        case .archive:
            return archiveOpenerBundleIdentifier
        }
    }
}

private struct ThrowingExportDestinationResolver: ExportDestinationResolving {
    let error: Error
    var usesPrivacySafeFilenames = false

    func exportFolderURL() throws -> URL? {
        throw error
    }

    func preferredOpenerBundleIdentifier(for kind: ConfiguredExportFileKind) -> String? {
        nil
    }
}

private enum RecordingExportError: Error, Equatable {
    case openFailed
}

@MainActor
private final class RecordingExportFileOpener: ExportFileOpening {
    struct Opened: Equatable {
        let url: URL
        let bundleIdentifier: String?
    }

    private(set) var opened: [Opened] = []

    func open(_ url: URL, preferredApplicationBundleIdentifier: String?) throws {
        opened.append(Opened(url: url, bundleIdentifier: preferredApplicationBundleIdentifier))
    }
}

@MainActor
private final class ThrowingExportFileOpener: ExportFileOpening {
    let error: Error

    init(error: Error) {
        self.error = error
    }

    func open(_ url: URL, preferredApplicationBundleIdentifier: String?) throws {
        throw error
    }
}

@MainActor
private final class RecordingSecurityScopedResourceAccess: SecurityScopedResourceAccessing {
    let startsAccessing: Bool
    private(set) var started: [URL] = []
    private(set) var stopped: [URL] = []
    private(set) var stoppedAfterOpenHandoff: [URL] = []

    init(startsAccessing: Bool) {
        self.startsAccessing = startsAccessing
    }

    func startAccessing(_ url: URL) -> Bool {
        started.append(url)
        return startsAccessing
    }

    func stopAccessing(_ url: URL) {
        stopped.append(url)
    }

    func stopAccessingAfterOpenHandoff(_ url: URL) {
        stoppedAfterOpenHandoff.append(url)
    }
}
