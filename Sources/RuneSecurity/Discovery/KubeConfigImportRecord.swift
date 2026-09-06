import CryptoKit
import Foundation

public struct KubeConfigImportOrigin: Codable, Equatable, Sendable {
    public let source: CloudConnectionSource
    public let provider: CloudAccountProvider?
    public let accountID: CloudAccountID?
    public let candidateID: CloudClusterCandidateID?

    public init(
        source: CloudConnectionSource = .importedFile,
        provider: CloudAccountProvider? = nil,
        accountID: CloudAccountID? = nil,
        candidateID: CloudClusterCandidateID? = nil
    ) {
        self.source = source
        self.provider = provider
        self.accountID = accountID
        self.candidateID = candidateID
    }

    var isValid: Bool {
        if source == .nativeAccount { return provider != nil && accountID != nil }
        return accountID == nil && candidateID == nil
    }
}

/// Describes one immutable Rune-owned copy, not ownership of its original file or cloud cluster.
/// Identity uses opaque IDs. The record retains the local filename and content fingerprint,
/// but never the original path, provider resource identifier, or credential bytes.
public struct KubeConfigImportRecord: Codable, Equatable, Sendable {
    public let version: Int
    public let id: UUID
    public let revision: UUID
    public let origin: KubeConfigImportOrigin
    public let configurationFilename: String
    public let contentDigest: String

    init(origin: KubeConfigImportOrigin, configurationFilename: String, contentDigest: String) {
        version = 1
        id = UUID()
        revision = UUID()
        self.origin = origin
        self.configurationFilename = configurationFilename
        self.contentDigest = contentDigest
    }
}

public struct KubeConfigImportPublication: Sendable {
    public let urls: [URL]
    /// Only files created by this call may be removed when bookmark publication fails.
    public let createdURLs: [URL]
    public let reusedCount: Int

    public init(urls: [URL], createdURLs: [URL], reusedCount: Int = 0) {
        self.urls = urls
        self.createdURLs = createdURLs
        self.reusedCount = reusedCount
    }
}

public enum KubeConfigImportOwnershipError: Error, LocalizedError, Equatable, Sendable {
    case unverifiedOwnership
    case changedContents
    case storageLimitExceeded

    public var errorDescription: String? {
        switch self {
        case .unverifiedOwnership: "Rune could not verify ownership of this imported copy. Its files were left in place."
        case .changedContents: "The imported copy changed. Review it again before removing its files."
        case .storageLimitExceeded: "The kubeconfig import exceeds Rune's storage verification limits. Import fewer or smaller files."
        }
    }
}

enum KubeConfigImportContents {
    static let recordFilename = ".rune-import.json"
    static let maximumRecordBytes = 16_384
    static let maximumFiles = 257
    static let maximumBytes = 128 * 1_024 * 1_024

    static func readRecord(in directory: URL) throws -> KubeConfigImportRecord {
        let url = directory.appendingPathComponent(recordFilename)
        try requireRegularFile(url)
        let values = try url.resourceValues(forKeys: [.fileSizeKey])
        guard (values.fileSize ?? Int.max) <= maximumRecordBytes else {
            throw KubeConfigImportOwnershipError.unverifiedOwnership
        }
        let data = try Data(contentsOf: url)
        guard data.count <= maximumRecordBytes,
              let record = try? JSONDecoder().decode(KubeConfigImportRecord.self, from: data),
              record.version == 1,
              record.origin.isValid,
              !record.configurationFilename.isEmpty,
              record.configurationFilename == URL(fileURLWithPath: record.configurationFilename).lastPathComponent,
              !record.configurationFilename.hasPrefix("."),
              record.configurationFilename != "assets",
              record.contentDigest.count == 64,
              record.contentDigest.utf8.allSatisfy({ (48...57).contains($0) || (97...102).contains($0) }) else {
            throw KubeConfigImportOwnershipError.unverifiedOwnership
        }
        return record
    }

    static func writeRecord(_ record: KubeConfigImportRecord, in directory: URL) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let destination = directory.appendingPathComponent(recordFilename)
        try encoder.encode(record).write(to: destination, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: destination.path)
    }

    /// Includes referenced asset bytes and relative paths. A credential/certificate file change
    /// must prevent reuse even when the kubeconfig text itself is unchanged.
    static func digest(in directory: URL, configurationFilename: String) throws -> String {
        let fileManager = FileManager.default
        let names = try fileManager.contentsOfDirectory(atPath: directory.path)
        guard Set(names).isSubset(of: [configurationFilename, recordFilename, "assets"]) else {
            throw KubeConfigImportOwnershipError.unverifiedOwnership
        }
        var files: [(name: String, url: URL)] = [
            ("configuration", directory.appendingPathComponent(configurationFilename))
        ]
        if names.contains("assets") {
            let assets = directory.appendingPathComponent("assets", isDirectory: true)
            try requireDirectory(assets)
            let assetNames = try fileManager.contentsOfDirectory(atPath: assets.path).sorted()
            guard assetNames.count < maximumFiles else { throw KubeConfigImportOwnershipError.storageLimitExceeded }
            files += assetNames.map { ("assets/" + $0, assets.appendingPathComponent($0)) }
        }
        var digest = SHA256()
        var totalBytes = 0
        for file in files {
            try requireRegularFile(file.url)
            let size = try file.url.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? Int.max
            guard size <= maximumBytes - totalBytes else { throw KubeConfigImportOwnershipError.storageLimitExceeded }
            // Hash file contents independently, then length-frame each relative name to avoid
            // ambiguous concatenations. Configuration filenames do not affect copy identity.
            var contentDigest = SHA256()
            let handle = try FileHandle(forReadingFrom: file.url)
            defer { try? handle.close() }
            while let chunk = try handle.read(upToCount: 65_536), !chunk.isEmpty {
                totalBytes += chunk.count
                guard totalBytes <= maximumBytes else { throw KubeConfigImportOwnershipError.storageLimitExceeded }
                contentDigest.update(data: chunk)
            }
            let name = Data(file.name.utf8)
            digest.update(data: Data("\(name.count):".utf8))
            digest.update(data: name)
            digest.update(data: Data(contentDigest.finalize()))
        }
        return digest.finalize().map { String(format: "%02x", $0) }.joined()
    }

    static func requireDirectory(_ url: URL) throws {
        try requireType(.typeDirectory, at: url)
    }

    private static func requireRegularFile(_ url: URL) throws {
        try requireType(.typeRegular, at: url)
    }

    private static func requireType(_ type: FileAttributeType, at url: URL) throws {
        guard url.standardizedFileURL.path == url.resolvingSymlinksInPath().path,
              try FileManager.default.attributesOfItem(atPath: url.path)[.type] as? FileAttributeType == type else {
            throw KubeConfigImportOwnershipError.unverifiedOwnership
        }
    }
}
