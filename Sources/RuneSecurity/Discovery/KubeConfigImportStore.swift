import Foundation

public protocol KubeConfigImportStoring: Sendable {
    func saveImportedKubeConfig(raw: String, sourceName: String) throws -> URL
}

public struct AppOwnedKubeConfigImportStore: KubeConfigImportStoring {
    private let rootDirectory: URL?

    public init(rootDirectory: URL? = nil) {
        self.rootDirectory = rootDirectory
    }

    public func saveImportedKubeConfig(raw: String, sourceName: String) throws -> URL {
        let fileManager = FileManager.default
        let directory = try importsDirectory()
        try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        try? fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: directory.path)

        let filename = "\(UUID().uuidString)-\(sanitizedFilename(from: sourceName))"
        let destination = directory.appendingPathComponent(filename, isDirectory: false)
        try raw.write(to: destination, atomically: true, encoding: .utf8)
        try? fileManager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: destination.path)
        return destination
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
