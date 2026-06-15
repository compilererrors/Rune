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

public final class ConfiguredFolderExporter: ConfiguredExporting {
    private let resolver: ExportDestinationResolving
    private let opener: ExportFileOpening

    public init(
        resolver: ExportDestinationResolving = UserDefaultsExportDestinationResolver(),
        opener: ExportFileOpening = WorkspaceExportFileOpener()
    ) {
        self.resolver = resolver
        self.opener = opener
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

        let didStartSecurityScope = folderURL.startAccessingSecurityScopedResource()
        defer {
            if didStartSecurityScope {
                folderURL.stopAccessingSecurityScopedResource()
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
        }

        return destination
    }

    private func uniqueDestinationURL(in folderURL: URL, suggestedName: String) -> URL {
        let sanitized = sanitizedFilename(suggestedName)
        let base = (sanitized as NSString).deletingPathExtension
        let ext = (sanitized as NSString).pathExtension
        var candidate = folderURL.appendingPathComponent(sanitized, isDirectory: false)
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
