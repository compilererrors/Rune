import AppKit
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

    func testWorkspaceOpenerUsesPreferredApplicationWithoutRunningSubstitution() throws {
        let fileURL = URL(fileURLWithPath: "/tmp/rune-export.log")
        let applicationURL = URL(fileURLWithPath: "/Applications/Inkline.app")
        let workspace = RecordingWorkspaceOpening(applicationsByBundleIdentifier: [
            "com.example.Inkline": applicationURL
        ])
        let opener = WorkspaceExportFileOpener(workspace: workspace)

        try opener.open(fileURL, preferredApplicationBundleIdentifier: "com.example.Inkline")

        XCTAssertTrue(workspace.defaultOpenedURLs.isEmpty)
        XCTAssertEqual(
            workspace.applicationOpenRequests,
            [
                RecordingWorkspaceOpening.ApplicationOpenRequest(
                    urls: [fileURL],
                    applicationURL: applicationURL,
                    allowsRunningApplicationSubstitution: false,
                    activates: true,
                    promptsUserIfNeeded: true
                )
            ]
        )
    }

    func testWorkspaceOpenerUsesQuikZipServiceURLForArchiveHandoff() throws {
        let fileURL = URL(fileURLWithPath: "/tmp/Rune Exports/unified logs.zip")
        let applicationURL = URL(fileURLWithPath: "/Applications/QuikZip.app")
        let workspace = RecordingWorkspaceOpening(applicationsByBundleIdentifier: [
            "com.viktornyberg.quikzip": applicationURL
        ])
        let opener = WorkspaceExportFileOpener(workspace: workspace)

        try opener.open(fileURL, preferredApplicationBundleIdentifier: "com.viktornyberg.quikzip")

        XCTAssertTrue(workspace.defaultOpenedURLs.isEmpty)
        let request = try XCTUnwrap(workspace.applicationOpenRequests.first)
        XCTAssertEqual(workspace.applicationOpenRequests.count, 1)
        XCTAssertEqual(request.applicationURL, applicationURL)
        XCTAssertEqual(request.allowsRunningApplicationSubstitution, false)
        let serviceURL = try XCTUnwrap(request.urls.first)
        let components = try XCTUnwrap(URLComponents(url: serviceURL, resolvingAgainstBaseURL: false))
        XCTAssertEqual(components.scheme, "quikzip")
        XCTAssertEqual(components.host, "service")
        XCTAssertEqual(components.queryItems?.first(where: { $0.name == "action" })?.value, "openInQuikZip")
        XCTAssertEqual(components.queryItems?.first(where: { $0.name == "path" })?.value, fileURL.path)
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

    func testSavedExportActionsKeepOriginalFolderAfterDestinationChangesOrIsCleared() throws {
        let folderURL = try makeTemporaryDirectory()
        let alternateFolderURL = try makeTemporaryDirectory()
        let resolver = MutableExportDestinationResolver(
            folderURL: folderURL,
            textOpenerBundleIdentifier: "com.example.TextViewer"
        )
        let opener = RecordingExportFileOpener()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: true)
        let exporter = ConfiguredFolderExporter(resolver: resolver, opener: opener, securityScopedAccess: securityScope)
        try Data("existing".utf8).write(to: folderURL.appendingPathComponent("synthetic.log"))

        let savedURL = try exporter.save(
            data: Data("synthetic log line\n".utf8),
            suggestedName: "synthetic.log",
            allowedFileTypes: ["log"],
            kind: .plainText,
            openAfterSave: false
        )

        for nextFolder in [alternateFolderURL, nil] {
            resolver.folderURL = nextFolder
            try exporter.openSavedFile(savedURL, kind: .plainText)
            try exporter.openSavedFolder(for: savedURL)
        }

        XCTAssertEqual(savedURL.lastPathComponent, "synthetic-2.log")
        XCTAssertEqual(opener.opened, [
            .init(url: savedURL, bundleIdentifier: "com.example.TextViewer"),
            .init(url: folderURL, bundleIdentifier: nil),
            .init(url: savedURL, bundleIdentifier: "com.example.TextViewer"),
            .init(url: folderURL, bundleIdentifier: nil)
        ])
        XCTAssertEqual(resolver.folderResolutionCount, 1)
        XCTAssertEqual(securityScope.started, Array(repeating: folderURL, count: 5))
        XCTAssertEqual(securityScope.stopped, [folderURL])
        XCTAssertEqual(securityScope.stoppedAfterOpenHandoff, Array(repeating: folderURL, count: 4))
    }

    func testSavedExportActionsDoNotStopScopeWhenAccessWasNotStarted() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: false)
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: opener,
            securityScopedAccess: securityScope
        )
        let savedURL = try exporter.save(
            data: Data("synthetic log line\n".utf8),
            suggestedName: "synthetic.log",
            allowedFileTypes: ["log"],
            kind: .plainText,
            openAfterSave: false
        )

        try exporter.openSavedFile(savedURL, kind: .plainText)
        try exporter.openSavedFolder(for: savedURL)

        XCTAssertEqual(opener.opened, [
            .init(url: savedURL, bundleIdentifier: nil),
            .init(url: folderURL, bundleIdentifier: nil)
        ])
        XCTAssertEqual(securityScope.started, Array(repeating: folderURL, count: 3))
        XCTAssertTrue(securityScope.stopped.isEmpty)
        XCTAssertTrue(securityScope.stoppedAfterOpenHandoff.isEmpty)
    }

    func testSavedExportActionsStopScopeIfOpeningFails() throws {
        let folderURL = try makeTemporaryDirectory()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: true)
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: ThrowingExportFileOpener(error: RecordingExportError.openFailed),
            securityScopedAccess: securityScope
        )
        let savedURL = try exporter.save(
            data: Data("synthetic log line\n".utf8),
            suggestedName: "synthetic.log",
            allowedFileTypes: ["log"],
            kind: .plainText,
            openAfterSave: false
        )

        XCTAssertThrowsError(try exporter.openSavedFile(savedURL, kind: .plainText)) {
            XCTAssertEqual($0 as? RecordingExportError, .openFailed)
        }
        XCTAssertThrowsError(try exporter.openSavedFolder(for: savedURL)) {
            XCTAssertEqual($0 as? RecordingExportError, .openFailed)
        }

        XCTAssertEqual(securityScope.started, Array(repeating: folderURL, count: 3))
        XCTAssertEqual(securityScope.stopped, Array(repeating: folderURL, count: 3))
        XCTAssertTrue(securityScope.stoppedAfterOpenHandoff.isEmpty)
    }

    func testSavedExportActionsRejectUnrecognizedAndNonFileURLsWithoutOpening() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: true)
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: opener,
            securityScopedAccess: securityScope
        )
        _ = try exporter.save(
            data: Data("synthetic log line\n".utf8),
            suggestedName: "synthetic.log",
            allowedFileTypes: ["log"],
            kind: .plainText,
            openAfterSave: false
        )
        let unrelatedURL = folderURL.appendingPathComponent("unrelated.log")
        try Data("unrelated synthetic file\n".utf8).write(to: unrelatedURL)

        for url in [unrelatedURL, try XCTUnwrap(URL(string: "https://example.invalid/synthetic.log"))] {
            XCTAssertThrowsError(try exporter.openSavedFile(url, kind: .plainText)) {
                XCTAssertEqual($0 as? FileExportError, .savedExportUnavailable)
            }
            XCTAssertThrowsError(try exporter.openSavedFolder(for: url)) {
                XCTAssertEqual($0 as? FileExportError, .savedExportUnavailable)
            }
        }

        XCTAssertTrue(opener.opened.isEmpty)
        XCTAssertEqual(securityScope.started, [folderURL])
        XCTAssertEqual(securityScope.stopped, [folderURL])
        XCTAssertTrue(securityScope.stoppedAfterOpenHandoff.isEmpty)
    }

    func testSavedExportFolderStillOpensWhenSavedFileWasRemoved() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: true)
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: opener,
            securityScopedAccess: securityScope
        )
        let savedURL = try exporter.save(
            data: Data("synthetic log line\n".utf8),
            suggestedName: "synthetic.log",
            allowedFileTypes: ["log"],
            kind: .plainText,
            openAfterSave: false
        )
        try FileManager.default.removeItem(at: savedURL)

        XCTAssertThrowsError(try exporter.openSavedFile(savedURL, kind: .plainText)) {
            XCTAssertEqual($0 as? FileExportError, .savedExportUnavailable)
        }
        try exporter.openSavedFolder(for: savedURL)

        XCTAssertEqual(opener.opened, [.init(url: folderURL, bundleIdentifier: nil)])
        XCTAssertEqual(securityScope.started, Array(repeating: folderURL, count: 3))
        XCTAssertEqual(securityScope.stopped, Array(repeating: folderURL, count: 2))
        XCTAssertEqual(securityScope.stoppedAfterOpenHandoff, [folderURL])
    }

    func testSavedExportActionsStopScopeWhenOriginalFolderWasRemoved() throws {
        let folderURL = try makeTemporaryDirectory()
        let opener = RecordingExportFileOpener()
        let securityScope = RecordingSecurityScopedResourceAccess(startsAccessing: true)
        let exporter = ConfiguredFolderExporter(
            resolver: StaticExportDestinationResolver(folderURL: folderURL),
            opener: opener,
            securityScopedAccess: securityScope
        )
        let savedURL = try exporter.save(
            data: Data("synthetic log line\n".utf8),
            suggestedName: "synthetic.log",
            allowedFileTypes: ["log"],
            kind: .plainText,
            openAfterSave: false
        )
        try FileManager.default.removeItem(at: folderURL)

        XCTAssertThrowsError(try exporter.openSavedFile(savedURL, kind: .plainText)) {
            XCTAssertEqual($0 as? FileExportError, .savedExportUnavailable)
        }
        XCTAssertThrowsError(try exporter.openSavedFolder(for: savedURL)) {
            XCTAssertEqual($0 as? FileExportError, .savedExportUnavailable)
        }

        XCTAssertTrue(opener.opened.isEmpty)
        XCTAssertEqual(securityScope.started, Array(repeating: folderURL, count: 3))
        XCTAssertEqual(securityScope.stopped, Array(repeating: folderURL, count: 3))
        XCTAssertTrue(securityScope.stoppedAfterOpenHandoff.isEmpty)
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

private final class MutableExportDestinationResolver: ExportDestinationResolving {
    var folderURL: URL?
    let textOpenerBundleIdentifier: String?
    var usesPrivacySafeFilenames = false
    private(set) var folderResolutionCount = 0

    init(folderURL: URL?, textOpenerBundleIdentifier: String? = nil) {
        self.folderURL = folderURL
        self.textOpenerBundleIdentifier = textOpenerBundleIdentifier
    }

    func exportFolderURL() throws -> URL? {
        folderResolutionCount += 1
        return folderURL
    }

    func preferredOpenerBundleIdentifier(for kind: ConfiguredExportFileKind) -> String? {
        kind == .plainText ? textOpenerBundleIdentifier : nil
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
private final class RecordingWorkspaceOpening: WorkspaceOpening {
    struct ApplicationOpenRequest: Equatable {
        let urls: [URL]
        let applicationURL: URL
        let allowsRunningApplicationSubstitution: Bool
        let activates: Bool
        let promptsUserIfNeeded: Bool
    }

    let applicationsByBundleIdentifier: [String: URL]
    private(set) var defaultOpenedURLs: [URL] = []
    private(set) var applicationOpenRequests: [ApplicationOpenRequest] = []

    init(applicationsByBundleIdentifier: [String: URL]) {
        self.applicationsByBundleIdentifier = applicationsByBundleIdentifier
    }

    func applicationURL(forBundleIdentifier bundleIdentifier: String) -> URL? {
        applicationsByBundleIdentifier[bundleIdentifier]
    }

    func openDefault(_ url: URL) {
        defaultOpenedURLs.append(url)
    }

    func open(
        _ urls: [URL],
        withApplicationAt applicationURL: URL,
        configuration: NSWorkspace.OpenConfiguration
    ) {
        applicationOpenRequests.append(
            ApplicationOpenRequest(
                urls: urls,
                applicationURL: applicationURL,
                allowsRunningApplicationSubstitution: configuration.allowsRunningApplicationSubstitution,
                activates: configuration.activates,
                promptsUserIfNeeded: configuration.promptsUserIfNeeded
            )
        )
    }
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
