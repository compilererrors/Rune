import Foundation
import Yams

public enum KubeConfigImportMaterializationError: Error, LocalizedError, Sendable, Equatable {
    case malformedKubeConfig
    case multipleDocuments
    case sourceLocationRequired(reference: String)
    case emptyReference(reference: String)
    case missingReference(reference: String)
    case unreadableReference(reference: String)
    case nonRegularReference(reference: String)
    case referenceTooLarge(reference: String, maximumBytes: Int)

    public var errorDescription: String? {
        switch self {
        case .malformedKubeConfig:
            return "Kubeconfig could not be materialized because its YAML structure is invalid."
        case .multipleDocuments:
            return "Kubeconfig must contain exactly one YAML document."
        case .sourceLocationRequired(let reference):
            return "Kubeconfig uses a relative \(reference). Import the kubeconfig file or its containing folder so Rune can preserve that reference."
        case .emptyReference(let reference):
            return "Kubeconfig contains an empty \(reference) reference."
        case .missingReference(let reference):
            return "Kubeconfig \(reference) could not be found or accessed. Confirm the file exists, then import a folder containing the kubeconfig and its credential files."
        case .unreadableReference(let reference):
            return "Kubeconfig \(reference) could not be read. Import a folder containing the kubeconfig and its credential files to grant Rune access."
        case .nonRegularReference(let reference):
            return "Kubeconfig \(reference) must point to a regular file."
        case .referenceTooLarge(let reference, let maximumBytes):
            return "Kubeconfig \(reference) exceeds Rune's \(maximumBytes)-byte import safety limit."
        }
    }
}

struct KubeConfigImportMaterializer {
    static let maximumReferenceBytes = 16 * 1_024 * 1_024

    private typealias YAMLMapping = [AnyHashable: Any]

    private enum ReferenceKind: String {
        case certificateAuthority = "certificate-authority"
        case clientCertificate = "client-certificate"
        case clientKey = "client-key"
        case tokenFile = "token file"
        case execCommand = "exec command"

        var assetFilename: String {
            switch self {
            case .certificateAuthority: return "certificate-authority.pem"
            case .clientCertificate: return "client-certificate.pem"
            case .clientKey: return "client-key.pem"
            case .tokenFile: return "token"
            case .execCommand: return "exec"
            }
        }
    }

    private let fileManager: FileManager
    private let homeDirectoryProvider: () -> URL

    init(
        fileManager: FileManager = .default,
        homeDirectoryProvider: @escaping () -> URL = { FileManager.default.homeDirectoryForCurrentUser }
    ) {
        self.fileManager = fileManager
        self.homeDirectoryProvider = homeDirectoryProvider
    }

    func materialize(raw: String, sourceURL: URL?, importDirectory: URL) throws -> String {
        var documents: [Any] = []
        do {
            var sequence = try load_all(yaml: raw)
            while let document = sequence.next() {
                documents.append(document)
            }
            if sequence.error != nil {
                throw KubeConfigImportMaterializationError.malformedKubeConfig
            }
        } catch let error as KubeConfigImportMaterializationError {
            throw error
        } catch {
            throw KubeConfigImportMaterializationError.malformedKubeConfig
        }
        guard documents.count == 1 else {
            throw KubeConfigImportMaterializationError.multipleDocuments
        }
        guard var root = documents[0] as? YAMLMapping else {
            throw KubeConfigImportMaterializationError.malformedKubeConfig
        }

        let sourceDirectory = sourceURL?.standardizedFileURL.deletingLastPathComponent()
        let assetsDirectory = importDirectory.appendingPathComponent("assets", isDirectory: true)
        var copiedAssets: [String: String] = [:]
        var assetIndex = 0
        var changed = false

        if var clusters = Self.sequence(in: root, key: "clusters") {
            for index in clusters.indices {
                guard var namedCluster = clusters[index] as? YAMLMapping,
                      var cluster = Self.mapping(in: namedCluster, key: "cluster") else {
                    continue
                }
                if let rewritten = try materializedDataReference(
                    in: cluster,
                    key: "certificate-authority",
                    kind: .certificateAuthority,
                    sourceDirectory: sourceDirectory,
                    assetsDirectory: assetsDirectory,
                    copiedAssets: &copiedAssets,
                    assetIndex: &assetIndex
                ) {
                    cluster[AnyHashable("certificate-authority")] = rewritten
                    changed = true
                }
                namedCluster[AnyHashable("cluster")] = cluster
                clusters[index] = namedCluster
            }
            root[AnyHashable("clusters")] = clusters
        }

        if var users = Self.sequence(in: root, key: "users") {
            for index in users.indices {
                guard var namedUser = users[index] as? YAMLMapping,
                      var user = Self.mapping(in: namedUser, key: "user") else {
                    continue
                }

                let dataReferences: [(String, ReferenceKind)] = [
                    ("client-certificate", .clientCertificate),
                    ("client-key", .clientKey),
                    ("tokenFile", .tokenFile),
                    ("token-file", .tokenFile)
                ]
                for (key, kind) in dataReferences {
                    if let rewritten = try materializedDataReference(
                        in: user,
                        key: key,
                        kind: kind,
                        sourceDirectory: sourceDirectory,
                        assetsDirectory: assetsDirectory,
                        copiedAssets: &copiedAssets,
                        assetIndex: &assetIndex
                    ) {
                        user[AnyHashable(key)] = rewritten
                        changed = true
                    }
                }

                if var exec = Self.mapping(in: user, key: "exec"),
                   let command = exec[AnyHashable("command")] as? String,
                   Self.isSlashRelativePath(command) {
                    guard let sourceDirectory else {
                        throw KubeConfigImportMaterializationError.sourceLocationRequired(
                            reference: ReferenceKind.execCommand.rawValue
                        )
                    }
                    exec[AnyHashable("command")] = sourceDirectory
                        .appendingPathComponent(command)
                        .standardizedFileURL
                        .path
                    user[AnyHashable("exec")] = exec
                    changed = true
                }

                namedUser[AnyHashable("user")] = user
                users[index] = namedUser
            }
            root[AnyHashable("users")] = users
        }

        guard changed else { return raw }
        do {
            return try dump(
                object: root,
                indent: 2,
                width: -1,
                allowUnicode: true,
                sortKeys: false
            )
        } catch {
            throw KubeConfigImportMaterializationError.malformedKubeConfig
        }
    }

    private func materializedDataReference(
        in mapping: YAMLMapping,
        key: String,
        kind: ReferenceKind,
        sourceDirectory: URL?,
        assetsDirectory: URL,
        copiedAssets: inout [String: String],
        assetIndex: inout Int
    ) throws -> String? {
        guard let value = mapping[AnyHashable(key)] else { return nil }
        guard let rawReference = value as? String else {
            throw KubeConfigImportMaterializationError.emptyReference(reference: kind.rawValue)
        }
        guard !rawReference.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw KubeConfigImportMaterializationError.emptyReference(reference: kind.rawValue)
        }
        let expanded = expandedDataPath(rawReference)
        let source: URL
        if NSString(string: expanded).isAbsolutePath {
            source = URL(fileURLWithPath: expanded)
                .standardizedFileURL
                .resolvingSymlinksInPath()
        } else {
            guard let sourceDirectory else {
                throw KubeConfigImportMaterializationError.sourceLocationRequired(reference: kind.rawValue)
            }
            source = sourceDirectory
                .appendingPathComponent(expanded)
                .standardizedFileURL
                .resolvingSymlinksInPath()
        }
        if let existing = copiedAssets[source.path] {
            return existing
        }
        let relativeAsset = try copyReference(
            from: source,
            kind: kind,
            index: assetIndex,
            assetsDirectory: assetsDirectory
        )
        copiedAssets[source.path] = relativeAsset
        assetIndex += 1
        return relativeAsset
    }

    private func copyReference(
        from source: URL,
        kind: ReferenceKind,
        index: Int,
        assetsDirectory: URL
    ) throws -> String {
        let values: URLResourceValues
        do {
            values = try source.resourceValues(forKeys: [.isRegularFileKey, .fileSizeKey])
        } catch {
            if !fileManager.fileExists(atPath: source.path) {
                throw KubeConfigImportMaterializationError.missingReference(reference: kind.rawValue)
            }
            throw KubeConfigImportMaterializationError.unreadableReference(reference: kind.rawValue)
        }
        guard values.isRegularFile == true else {
            if !fileManager.fileExists(atPath: source.path) {
                throw KubeConfigImportMaterializationError.missingReference(reference: kind.rawValue)
            }
            throw KubeConfigImportMaterializationError.nonRegularReference(reference: kind.rawValue)
        }
        if let fileSize = values.fileSize, fileSize > Self.maximumReferenceBytes {
            throw KubeConfigImportMaterializationError.referenceTooLarge(
                reference: kind.rawValue,
                maximumBytes: Self.maximumReferenceBytes
            )
        }

        let data: Data
        do {
            data = try Data(contentsOf: source, options: [.mappedIfSafe])
        } catch {
            throw KubeConfigImportMaterializationError.unreadableReference(reference: kind.rawValue)
        }
        guard data.count <= Self.maximumReferenceBytes else {
            throw KubeConfigImportMaterializationError.referenceTooLarge(
                reference: kind.rawValue,
                maximumBytes: Self.maximumReferenceBytes
            )
        }

        do {
            if !fileManager.fileExists(atPath: assetsDirectory.path) {
                try fileManager.createDirectory(
                    at: assetsDirectory,
                    withIntermediateDirectories: false,
                    attributes: [.posixPermissions: 0o700]
                )
            }
            try fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: assetsDirectory.path)
            let filename = String(format: "%03d", index) + "-" + kind.assetFilename
            let destination = assetsDirectory.appendingPathComponent(filename, isDirectory: false)
            try data.write(to: destination, options: .atomic)
            try fileManager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: destination.path)
            return "assets/\(filename)"
        } catch let error as KubeConfigImportMaterializationError {
            throw error
        } catch {
            throw KubeConfigImportMaterializationError.unreadableReference(reference: kind.rawValue)
        }
    }

    private static func mapping(in mapping: YAMLMapping, key: String) -> YAMLMapping? {
        mapping[AnyHashable(key)] as? YAMLMapping
    }

    private func expandedDataPath(_ path: String) -> String {
        if path == "~" {
            return homeDirectoryProvider().path
        }
        if path.hasPrefix("~/") {
            return homeDirectoryProvider()
                .appendingPathComponent(String(path.dropFirst(2)))
                .standardizedFileURL
                .path
        }
        return NSString(string: path).expandingTildeInPath
    }

    private static func sequence(in mapping: YAMLMapping, key: String) -> [Any]? {
        mapping[AnyHashable(key)] as? [Any]
    }

    private static func isAbsolutePath(_ path: String) -> Bool {
        let expanded = NSString(string: path).expandingTildeInPath
        return NSString(string: expanded).isAbsolutePath
    }

    private static func isSlashRelativePath(_ path: String) -> Bool {
        !isAbsolutePath(path) && path.contains("/")
    }
}
