import Foundation

public struct KubeConfigImportStorePayload: Sendable {
    public let raw: String
    public let sourceName: String
    public let sourceURL: URL?

    public init(raw: String, sourceName: String, sourceURL: URL?) {
        self.raw = raw
        self.sourceName = sourceName
        self.sourceURL = sourceURL
    }
}

public protocol KubeConfigImportStoring: Sendable {
    func saveImportedKubeConfigs(_ payloads: [KubeConfigImportStorePayload]) throws -> [URL]
    func removeImportedKubeConfigs(at urls: [URL]) throws
}

public extension KubeConfigImportStoring {
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

    public init(rootDirectory: URL? = nil) {
        self.rootDirectory = rootDirectory
    }

    public func saveImportedKubeConfigs(_ payloads: [KubeConfigImportStorePayload]) throws -> [URL] {
        guard !payloads.isEmpty else { return [] }
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

            var relativeDestinations: [String] = []
            relativeDestinations.reserveCapacity(payloads.count)
            for (index, payload) in payloads.enumerated() {
                let itemDirectoryName = String(format: "%03d", index)
                let itemDirectory = stagingDirectory.appendingPathComponent(itemDirectoryName, isDirectory: true)
                try fileManager.createDirectory(
                    at: itemDirectory,
                    withIntermediateDirectories: false,
                    attributes: [.posixPermissions: 0o700]
                )
                try fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: itemDirectory.path)
                let materialized = try KubeConfigImportMaterializer(fileManager: fileManager).materialize(
                    raw: payload.raw,
                    sourceURL: payload.sourceURL,
                    importDirectory: itemDirectory
                )
                let filename = sanitizedFilename(from: payload.sourceName)
                let destination = itemDirectory.appendingPathComponent(filename, isDirectory: false)
                try materialized.write(to: destination, atomically: true, encoding: .utf8)
                try fileManager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: destination.path)
                relativeDestinations.append("\(itemDirectoryName)/\(filename)")
            }
            try fileManager.moveItem(at: stagingDirectory, to: publishedDirectory)
            return relativeDestinations.map { relativePath in
                publishedDirectory.appendingPathComponent(relativePath, isDirectory: false)
            }
        } catch {
            try? fileManager.removeItem(at: stagingDirectory)
            try? fileManager.removeItem(at: publishedDirectory)
            throw error
        }
    }

    public func removeImportedKubeConfigs(at urls: [URL]) throws {
        guard !urls.isEmpty else { return }
        let fileManager = FileManager.default
        let imports = try importsDirectory().standardizedFileURL
        let importsPrefix = imports.path.hasSuffix("/") ? imports.path : imports.path + "/"
        var transactionDirectories = Set<URL>()

        for url in urls {
            let path = url.standardizedFileURL.path
            guard path.hasPrefix(importsPrefix) else { continue }
            let relative = String(path.dropFirst(importsPrefix.count))
            guard let transaction = relative.split(separator: "/").first, !transaction.isEmpty else { continue }
            transactionDirectories.insert(imports.appendingPathComponent(String(transaction), isDirectory: true))
        }
        for directory in transactionDirectories where fileManager.fileExists(atPath: directory.path) {
            try fileManager.removeItem(at: directory)
        }
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
