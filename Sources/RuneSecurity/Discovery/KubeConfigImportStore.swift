import Foundation

public struct KubeConfigImportStorePayload: Sendable {
    public let raw: String
    public let sourceName: String
    public let sourceURL: URL?
    public let origin: KubeConfigImportOrigin

    public init(raw: String, sourceName: String, sourceURL: URL?, origin: KubeConfigImportOrigin = .init()) {
        self.raw = raw
        self.sourceName = sourceName
        self.sourceURL = sourceURL
        self.origin = origin
    }
}

public protocol KubeConfigImportStoring: Sendable {
    func saveImportedKubeConfigs(_ payloads: [KubeConfigImportStorePayload]) throws -> [URL]
    func removeImportedKubeConfigs(at urls: [URL]) throws
    func publishImportedKubeConfigs(_ payloads: [KubeConfigImportStorePayload], reusing urls: [URL]) throws -> KubeConfigImportPublication
}

public extension KubeConfigImportStoring {
    func publishImportedKubeConfigs(_ payloads: [KubeConfigImportStorePayload], reusing urls: [URL]) throws -> KubeConfigImportPublication {
        let created = try saveImportedKubeConfigs(payloads)
        return KubeConfigImportPublication(urls: created, createdURLs: created)
    }

    func saveImportedKubeConfig(raw: String, sourceName: String, sourceURL: URL?) throws -> URL {
        guard let imported = try saveImportedKubeConfigs([
            KubeConfigImportStorePayload(raw: raw, sourceName: sourceName, sourceURL: sourceURL)
        ]).first else {
            throw CocoaError(.fileWriteUnknown)
        }
        return imported
    }

    func saveImportedKubeConfig(raw: String, sourceName: String) throws -> URL {
        try saveImportedKubeConfig(raw: raw, sourceName: sourceName, sourceURL: nil)
    }
}

public struct AppOwnedKubeConfigImportStore: KubeConfigImportStoring {
    private let rootDirectory: URL?
    private static let transactionLock = NSLock()

    public init(rootDirectory: URL? = nil) {
        self.rootDirectory = rootDirectory
    }

    public func saveImportedKubeConfigs(_ payloads: [KubeConfigImportStorePayload]) throws -> [URL] {
        try Self.transactionLock.withLock {
            try publish(payloads, reusing: nil).urls
        }
    }

    public func publishImportedKubeConfigs(_ payloads: [KubeConfigImportStorePayload], reusing urls: [URL]) throws -> KubeConfigImportPublication {
        try Self.transactionLock.withLock {
            try publish(payloads, reusing: urls)
        }
    }

    public func record(forImportedKubeConfigAt url: URL) throws -> KubeConfigImportRecord {
        try Self.transactionLock.withLock { try ownedEntry(at: url, verifyContents: true).record }
    }

    private func publish(_ payloads: [KubeConfigImportStorePayload], reusing urls: [URL]?) throws -> KubeConfigImportPublication {
        guard !payloads.isEmpty else { return .init(urls: [], createdURLs: []) }
        guard payloads.count <= 256 else { throw KubeConfigImportOwnershipError.storageLimitExceeded }
        guard payloads.allSatisfy({ $0.origin.isValid }) else { throw KubeConfigImportOwnershipError.unverifiedOwnership }
        let fileManager = FileManager.default
        let imports = try importsDirectory()
        try fileManager.createDirectory(
            at: imports,
            withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700]
        )
        try fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: imports.path)

        let importID = UUID().uuidString
        let stagingDirectory = imports.appendingPathComponent(".\(importID).staging", isDirectory: true)
        let publishedDirectory = imports.appendingPathComponent(importID, isDirectory: true)
        do {
            try fileManager.createDirectory(
                at: stagingDirectory,
                withIntermediateDirectories: false,
                attributes: [.posixPermissions: 0o700]
            )
            try fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: stagingDirectory.path)

            let physicalStaging = stagingDirectory.resolvingSymlinksInPath()
            // Reuse is restricted to explicitly supplied, registered Rune-owned sources. External
            // files and older imports without ownership records are never inferred to be owned.
            let candidates = (urls ?? []).prefix(1_024).compactMap { url -> (URL, KubeConfigImportRecord)? in
                guard let entry = try? ownedEntry(at: url, verifyContents: false) else { return nil }
                return (url, entry.record)
            }
            var verifiedCandidates: [Int: Bool] = [:]
            var stagedRecords: [(relativePath: String, record: KubeConfigImportRecord)] = []
            var destinations: [URL] = []
            var reusedCount = 0
            var relativeDestinations: [String] = []
            relativeDestinations.reserveCapacity(payloads.count)
            let materializer = KubeConfigImportMaterializer(fileManager: fileManager)
            // Keep only one parsed document, local to this transaction. Referenced
            // files are still validated and copied afresh for every destination.
            var previousDocument: KubeConfigImportMaterializer.ParsedDocument?
            for (index, payload) in payloads.enumerated() {
                let itemDirectoryName = String(format: "%03d", index)
                let itemDirectory = physicalStaging.appendingPathComponent(itemDirectoryName, isDirectory: true)
                try fileManager.createDirectory(
                    at: itemDirectory,
                    withIntermediateDirectories: false,
                    attributes: [.posixPermissions: 0o700]
                )
                try fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: itemDirectory.path)
                let document: KubeConfigImportMaterializer.ParsedDocument
                if let previousDocument, previousDocument.raw == payload.raw {
                    document = previousDocument
                } else {
                    document = try materializer.parse(payload.raw)
                    previousDocument = document
                }
                let materialized = try materializer.materialize(
                    document: document,
                    sourceURL: payload.sourceURL,
                    importDirectory: itemDirectory
                )
                let filename = sanitizedFilename(from: payload.sourceName)
                let destination = itemDirectory.appendingPathComponent(filename, isDirectory: false)
                try materialized.write(to: destination, atomically: true, encoding: .utf8)
                try fileManager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: destination.path)
                let digest = try KubeConfigImportContents.digest(in: itemDirectory, configurationFilename: filename)
                let reusable = candidates.enumerated().first { candidateIndex, candidate in
                    let (url, record) = candidate
                    guard record.origin == payload.origin, record.contentDigest == digest else { return false }
                    if let verified = verifiedCandidates[candidateIndex] { return verified }
                    let verified = (try? ownedEntry(at: url, verifyContents: true)) != nil
                    verifiedCandidates[candidateIndex] = verified
                    return verified
                }?.element.0
                let staged = urls == nil ? nil : stagedRecords.first { $0.record.origin == payload.origin && $0.record.contentDigest == digest }
                if let reusable = reusable ?? staged.map({ publishedDirectory.appendingPathComponent($0.relativePath) }) {
                    try fileManager.removeItem(at: itemDirectory)
                    destinations.append(reusable)
                    reusedCount += 1
                    continue
                }
                let record = KubeConfigImportRecord(origin: payload.origin, configurationFilename: filename, contentDigest: digest)
                try KubeConfigImportContents.writeRecord(record, in: itemDirectory)
                let relativePath = "\(itemDirectoryName)/\(filename)"
                relativeDestinations.append(relativePath)
                stagedRecords.append((relativePath, record))
                destinations.append(publishedDirectory.appendingPathComponent(relativePath))
            }
            if relativeDestinations.isEmpty {
                try fileManager.removeItem(at: stagingDirectory)
            } else {
                try fileManager.moveItem(at: stagingDirectory, to: publishedDirectory)
            }
            let created = relativeDestinations.map { relativePath in
                publishedDirectory.appendingPathComponent(relativePath, isDirectory: false)
            }
            var seenDestinations = Set<URL>()
            return .init(
                // Update existing prepends sources newest-first. If A, B, A reuse one A copy,
                // retain B, A here so publication still gives the final A definition precedence.
                urls: Array(destinations.reversed().filter { seenDestinations.insert($0).inserted }.reversed()),
                createdURLs: created,
                reusedCount: reusedCount
            )
        } catch {
            try? fileManager.removeItem(at: stagingDirectory)
            try? fileManager.removeItem(at: publishedDirectory)
            throw error
        }
    }

    public func removeImportedKubeConfigs(at urls: [URL]) throws {
        guard !urls.isEmpty else { return }
        try Self.transactionLock.withLock {
            let fileManager = FileManager.default
            // Validate the complete request before deleting anything. A URL prefix alone is not
            // proof of ownership, and deleting one entry must preserve its batch siblings.
            guard urls.allSatisfy(\.isFileURL) else { throw KubeConfigImportOwnershipError.unverifiedOwnership }
            let entries = try Set(urls)
                .filter { fileManager.fileExists(atPath: $0.deletingLastPathComponent().path) }
                .map { try ownedEntry(at: $0, verifyContents: true) }
            for directory in Set(entries.map(\.directory)) {
                try fileManager.removeItem(at: directory)
            }
            for transaction in Set(entries.map { $0.directory.deletingLastPathComponent() }) {
                if try fileManager.contentsOfDirectory(atPath: transaction.path).isEmpty {
                    try fileManager.removeItem(at: transaction)
                }
            }
        }
    }

    private func ownedEntry(at url: URL, verifyContents: Bool) throws -> (directory: URL, record: KubeConfigImportRecord) {
        guard url.isFileURL else { throw KubeConfigImportOwnershipError.unverifiedOwnership }
        let root = try importsDirectory().standardizedFileURL
        let physicalRoot = root.resolvingSymlinksInPath()
        let path = url.standardizedFileURL.path
        let prefixes = [root.path + "/", physicalRoot.path + "/"]
        guard let prefix = prefixes.first(where: { path.hasPrefix($0) }) else {
            throw KubeConfigImportOwnershipError.unverifiedOwnership
        }
        let components = path.dropFirst(prefix.count).split(separator: "/").map(String.init)
        guard components.count == 3, UUID(uuidString: components[0]) != nil,
              components[1].count == 3, components[1].utf8.allSatisfy({ (48...57).contains($0) }) else {
            throw KubeConfigImportOwnershipError.unverifiedOwnership
        }
        let transaction = physicalRoot.appendingPathComponent(components[0], isDirectory: true)
        let directory = transaction.appendingPathComponent(components[1], isDirectory: true)
        try KubeConfigImportContents.requireDirectory(transaction)
        try KubeConfigImportContents.requireDirectory(directory)
        let record = try KubeConfigImportContents.readRecord(in: directory)
        guard record.configurationFilename == components[2] else { throw KubeConfigImportOwnershipError.unverifiedOwnership }
        if verifyContents {
            guard try KubeConfigImportContents.digest(in: directory, configurationFilename: components[2]) == record.contentDigest else {
                throw KubeConfigImportOwnershipError.changedContents
            }
        }
        return (directory, record)
    }

    private func importsDirectory() throws -> URL {
        if let rootDirectory {
            return rootDirectory
        }

        let base = try FileManager.default.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        return base
            .appendingPathComponent("Rune", isDirectory: true)
            .appendingPathComponent("kubeconfigs", isDirectory: true)
            .appendingPathComponent("imports", isDirectory: true)
    }

    private func sanitizedFilename(from sourceName: String) -> String {
        let trimmed = sourceName.trimmingCharacters(in: .whitespacesAndNewlines)
        let fallback = trimmed.isEmpty ? "kubeconfig.yaml" : trimmed
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: ".-_"))
        let sanitized = fallback.unicodeScalars.map { scalar in
            allowed.contains(scalar) ? Character(scalar) : "_"
        }
        let name = String(sanitized).trimmingCharacters(in: CharacterSet(charactersIn: ".-_"))
        let nonEmptyName = name.isEmpty ? "kubeconfig" : name
        return nonEmptyName.contains(".") ? nonEmptyName : "\(nonEmptyName).yaml"
    }
}
