import AppKit
import Foundation
import RuneCore
import UniformTypeIdentifiers

public protocol FileExporting {
    @MainActor
    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL
}

public enum ConfiguredExportFileKind: Sendable {
    case plainText
    case archive
}

public protocol ExportDestinationResolving {
    func exportFolderURL() throws -> URL?
    func preferredOpenerBundleIdentifier(for kind: ConfiguredExportFileKind) -> String?
    var usesPrivacySafeFilenames: Bool { get }
}

public protocol ExportFileOpening {
    @MainActor
    func open(_ url: URL, preferredApplicationBundleIdentifier: String?) throws
}

public protocol SecurityScopedResourceAccessing {
    @MainActor
    func startAccessing(_ url: URL) -> Bool

    @MainActor
    func stopAccessing(_ url: URL)

    @MainActor
    func stopAccessingAfterOpenHandoff(_ url: URL)
}

public protocol ConfiguredExporting {
    @MainActor
    func save(
        data: Data,
        suggestedName: String,
        allowedFileTypes: [String],
        kind: ConfiguredExportFileKind,
        openAfterSave: Bool
    ) throws -> URL
}

public enum FileExportError: LocalizedError, Equatable {
    case userCancelled
    case missingConfiguredExportFolder
    case exportFolderUnavailable

    public var errorDescription: String? {
        switch self {
        case .userCancelled:
            return "Save cancelled."
        case .missingConfiguredExportFolder:
            return "No export folder is configured. Choose an export folder in Settings > Logs."
        case .exportFolderUnavailable:
            return "Rune could not access the configured export folder. Choose the folder again in Settings."
        }
    }
}

public final class SavePanelExporter: FileExporting {
    public init() {}

    @MainActor
    public func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        let panel = NSSavePanel()
        panel.canCreateDirectories = true
        panel.nameFieldStringValue = suggestedName
        panel.allowedContentTypes = allowedFileTypes.compactMap { UTType(filenameExtension: $0) }

        let result = panel.runModal()
        guard result == .OK, let destination = panel.url else {
            throw FileExportError.userCancelled
        }

        try data.write(to: destination)
        return destination
    }
}

public struct UserDefaultsExportDestinationResolver: ExportDestinationResolving {
    private let defaults: UserDefaults

    public init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
    }

    public var usesPrivacySafeFilenames: Bool {
        defaults.runeExportUsesPrivacySafeFilenames
    }

    public func exportFolderURL() throws -> URL? {
        guard let bookmark = defaults.runeExportFolderBookmarkData else { return nil }
        var stale = false
        let url = try URL(
            resolvingBookmarkData: bookmark,
            options: [.withSecurityScope],
            relativeTo: nil,
            bookmarkDataIsStale: &stale
        )
        guard !stale else { throw FileExportError.exportFolderUnavailable }
        return url
    }

    public func preferredOpenerBundleIdentifier(for kind: ConfiguredExportFileKind) -> String? {
        switch kind {
        case .plainText:
            return defaults.runeExportTextOpenerBundleIdentifier
        case .archive:
            return defaults.runeExportArchiveOpenerBundleIdentifier
        }
    }
}

public struct WorkspaceExportFileOpener: ExportFileOpening {
    public init() {}

    @MainActor
    public func open(_ url: URL, preferredApplicationBundleIdentifier: String?) throws {
        guard let bundleIdentifier = preferredApplicationBundleIdentifier,
              let applicationURL = NSWorkspace.shared.urlForApplication(withBundleIdentifier: bundleIdentifier) else {
            NSWorkspace.shared.open(url)
            return
        }

        let configuration = NSWorkspace.OpenConfiguration()
        NSWorkspace.shared.open(
            [url],
            withApplicationAt: applicationURL,
            configuration: configuration,
            completionHandler: nil
        )
    }
}

public struct DefaultSecurityScopedResourceAccess: SecurityScopedResourceAccessing {
    private let openHandoffRetentionNanoseconds: UInt64

    public init(openHandoffRetentionNanoseconds: UInt64 = 15_000_000_000) {
        self.openHandoffRetentionNanoseconds = openHandoffRetentionNanoseconds
    }

    @MainActor
    public func startAccessing(_ url: URL) -> Bool {
        url.startAccessingSecurityScopedResource()
    }

    @MainActor
    public func stopAccessing(_ url: URL) {
        url.stopAccessingSecurityScopedResource()
    }

    @MainActor
    public func stopAccessingAfterOpenHandoff(_ url: URL) {
        let delay = openHandoffRetentionNanoseconds
        Task { @MainActor in
            try? await Task.sleep(nanoseconds: delay)
            url.stopAccessingSecurityScopedResource()
        }
    }
}

public final class ConfiguredFolderExporter: ConfiguredExporting {
    private let resolver: ExportDestinationResolving
    private let opener: ExportFileOpening
    private let securityScopedAccess: any SecurityScopedResourceAccessing

    public init(
        resolver: ExportDestinationResolving = UserDefaultsExportDestinationResolver(),
        opener: ExportFileOpening = WorkspaceExportFileOpener(),
        securityScopedAccess: any SecurityScopedResourceAccessing = DefaultSecurityScopedResourceAccess()
    ) {
        self.resolver = resolver
        self.opener = opener
        self.securityScopedAccess = securityScopedAccess
    }

    @MainActor
    public func save(
        data: Data,
        suggestedName: String,
        allowedFileTypes: [String],
        kind: ConfiguredExportFileKind,
        openAfterSave: Bool
    ) throws -> URL {
        guard let folderURL = try resolver.exportFolderURL() else {
            throw FileExportError.missingConfiguredExportFolder
        }

        let didStartSecurityScope = securityScopedAccess.startAccessing(folderURL)
        var shouldStopSecurityScope = didStartSecurityScope
        defer {
            if shouldStopSecurityScope {
                securityScopedAccess.stopAccessing(folderURL)
            }
        }

        try FileManager.default.createDirectory(at: folderURL, withIntermediateDirectories: true)
        let destination = uniqueDestinationURL(
            in: folderURL,
            suggestedName: resolver.usesPrivacySafeFilenames
                ? privacySafeFilename(allowedFileTypes: allowedFileTypes)
                : suggestedName
        )
        try data.write(to: destination, options: .atomic)

        if openAfterSave {
            try opener.open(destination, preferredApplicationBundleIdentifier: resolver.preferredOpenerBundleIdentifier(for: kind))
            if didStartSecurityScope {
                shouldStopSecurityScope = false
                securityScopedAccess.stopAccessingAfterOpenHandoff(folderURL)
            }
        }

        return destination
    }

    private func uniqueDestinationURL(in folderURL: URL, suggestedName: String) -> URL {
        let sanitized = sanitizedFilename(suggestedName)
        let base = (sanitized as NSString).deletingPathExtension
        let ext = (sanitized as NSString).pathExtension
        var candidate = folderURL.appendingPathComponent(sanitized, isDirectory: false)
        guard FileManager.default.fileExists(atPath: candidate.path) else {
            return candidate
        }

        if let existingNames = try? FileManager.default.contentsOfDirectory(atPath: folderURL.path) {
            let existingNameSet = Set(existingNames)
            var index = 2
            while true {
                let nextName = ext.isEmpty ? "\(base)-\(index)" : "\(base)-\(index).\(ext)"
                if !existingNameSet.contains(nextName) {
                    return folderURL.appendingPathComponent(nextName, isDirectory: false)
                }
                index += 1
            }
        }

        var index = 2
        while FileManager.default.fileExists(atPath: candidate.path) {
            let nextName = ext.isEmpty ? "\(base)-\(index)" : "\(base)-\(index).\(ext)"
            candidate = folderURL.appendingPathComponent(nextName, isDirectory: false)
            index += 1
        }
        return candidate
    }

    private func privacySafeFilename(allowedFileTypes: [String]) -> String {
        let ext = allowedFileTypes.first?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let suffix = ext.map { ".\($0)" } ?? ""
        return "rune-export-\(Self.timestamp())\(suffix)"
    }

    private func sanitizedFilename(_ value: String) -> String {
        let fallback = "rune-export"
        let disallowed = CharacterSet(charactersIn: "/:")
        let pieces = value
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .components(separatedBy: disallowed)
            .filter { !$0.isEmpty }
        let sanitized = pieces.joined(separator: "-")
        return sanitized.isEmpty ? fallback : sanitized
    }

    private static func timestamp() -> String {
        ISO8601DateFormatter().string(from: Date()).replacingOccurrences(of: ":", with: "")
    }
}
