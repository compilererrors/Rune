import Foundation
import RuneCore
import Yams

public struct KubeConfigContextRemovalPreview: Sendable, Equatable {
    public let contextName: String
    public let affectedSourceDisplayNames: [String]
    public let removedClusterCount: Int
    public let removedUserCount: Int

    public init(
        contextName: String,
        affectedSourceDisplayNames: [String],
        removedClusterCount: Int,
        removedUserCount: Int
    ) {
        self.contextName = contextName
        self.affectedSourceDisplayNames = affectedSourceDisplayNames
        self.removedClusterCount = removedClusterCount
        self.removedUserCount = removedUserCount
    }
}

public enum KubeConfigContextRemovalError: Error, LocalizedError, Sendable, Equatable {
    case invalidContextName
    case malformedKubeConfig(String)
    case multipleDocuments(String)
    case contextNotFound(String)

    public var errorDescription: String? {
        switch self {
        case .invalidContextName:
            return "Choose a valid Kubernetes context to remove."
        case let .malformedKubeConfig(source):
            return "Rune could not safely update \(source) because its YAML structure is invalid."
        case let .multipleDocuments(source):
            return "Rune could not safely update \(source) because it contains multiple YAML documents."
        case let .contextNotFound(name):
            return "The context \(name) is no longer present in the loaded kubeconfig files."
        }
    }
}

public protocol KubeConfigContextRemoving: Sendable {
    func previewRemoval(
        of contextName: String,
        from sources: [KubeConfigSource]
    ) throws -> KubeConfigContextRemovalPreview

    @discardableResult
    func removeContext(
        named contextName: String,
        from sources: [KubeConfigSource]
    ) throws -> KubeConfigContextRemovalPreview
}

/// Removes a context from local kubeconfig files without making any request to the cluster.
/// Cluster and user records are removed only when no remaining context references them.
public struct KubeConfigContextRemover: KubeConfigContextRemoving {
    private typealias YAMLMapping = [AnyHashable: Any]

    private struct Mutation {
        let url: URL
        let original: Data
        let replacement: Data
        let permissions: Any?
        let sourceDisplayName: String
        let removedClusterCount: Int
        let removedUserCount: Int
    }

    private let backupRootDirectory: URL?

    public init(backupRootDirectory: URL? = nil) {
        self.backupRootDirectory = backupRootDirectory
    }

    public func previewRemoval(
        of contextName: String,
        from sources: [KubeConfigSource]
    ) throws -> KubeConfigContextRemovalPreview {
        try makePreview(contextName: contextName, mutations: mutations(contextName: contextName, sources: sources))
    }

    @discardableResult
    public func removeContext(
        named contextName: String,
        from sources: [KubeConfigSource]
    ) throws -> KubeConfigContextRemovalPreview {
        let mutations = try mutations(contextName: contextName, sources: sources)
        let preview = try makePreview(contextName: contextName, mutations: mutations)
        _ = try createBackups(for: mutations)
        var written: [Mutation] = []

        do {
            for mutation in mutations {
                try mutation.replacement.write(to: mutation.url, options: .atomic)
                written.append(mutation)
                if let permissions = mutation.permissions {
                    try FileManager.default.setAttributes(
                        [.posixPermissions: permissions],
                        ofItemAtPath: mutation.url.path
                    )
                }
            }
        } catch {
            for mutation in written.reversed() {
                try? mutation.original.write(to: mutation.url, options: .atomic)
                if let permissions = mutation.permissions {
                    try? FileManager.default.setAttributes(
                        [.posixPermissions: permissions],
                        ofItemAtPath: mutation.url.path
                    )
                }
            }
            throw error
        }

        return preview
    }

    private func createBackups(for mutations: [Mutation]) throws -> URL {
        let fileManager = FileManager.default
        let root: URL
        if let backupRootDirectory {
            root = backupRootDirectory
        } else {
            root = try fileManager.url(
                for: .applicationSupportDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            )
            .appendingPathComponent("Rune", isDirectory: true)
            .appendingPathComponent("kubeconfigs", isDirectory: true)
            .appendingPathComponent("backups", isDirectory: true)
        }
        try fileManager.createDirectory(
            at: root,
            withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700]
        )
        try fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: root.path)

        let transaction = root.appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fileManager.createDirectory(
            at: transaction,
            withIntermediateDirectories: false,
            attributes: [.posixPermissions: 0o700]
        )
        do {
            for (index, mutation) in mutations.enumerated() {
                let backup = transaction.appendingPathComponent(
                    String(format: "%03d.kubeconfig", index),
                    isDirectory: false
                )
                try mutation.original.write(to: backup, options: [.atomic])
                try fileManager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: backup.path)
            }
            return transaction
        } catch {
            try? fileManager.removeItem(at: transaction)
            throw error
        }
    }

    private func makePreview(
        contextName: String,
        mutations: [Mutation]
    ) throws -> KubeConfigContextRemovalPreview {
        guard !mutations.isEmpty else {
            throw KubeConfigContextRemovalError.contextNotFound(contextName)
        }
        return KubeConfigContextRemovalPreview(
            contextName: contextName,
            affectedSourceDisplayNames: mutations.map(\.sourceDisplayName),
            removedClusterCount: mutations.reduce(0) { $0 + $1.removedClusterCount },
            removedUserCount: mutations.reduce(0) { $0 + $1.removedUserCount }
        )
    }

    private func mutations(
        contextName: String,
        sources: [KubeConfigSource]
    ) throws -> [Mutation] {
        let trimmedName = contextName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedName.isEmpty else {
            throw KubeConfigContextRemovalError.invalidContextName
        }

        var seenPaths = Set<String>()
        var result: [Mutation] = []
        for source in sources {
            let url = source.url.resolvingSymlinksInPath().standardizedFileURL
            guard seenPaths.insert(url.path).inserted else { continue }
            let original = try Data(contentsOf: url)
            guard let raw = String(data: original, encoding: .utf8) else {
                throw KubeConfigContextRemovalError.malformedKubeConfig(source.displayName)
            }
            guard let transformed = try transformedKubeConfig(
                raw: raw,
                contextName: trimmedName,
                sourceDisplayName: source.displayName
            ) else { continue }
            let attributes = try? FileManager.default.attributesOfItem(atPath: url.path)
            result.append(Mutation(
                url: url,
                original: original,
                replacement: Data(transformed.raw.utf8),
                permissions: attributes?[.posixPermissions],
                sourceDisplayName: source.displayName,
                removedClusterCount: transformed.removedClusterCount,
                removedUserCount: transformed.removedUserCount
            ))
        }
        return result
    }

    private func transformedKubeConfig(
        raw: String,
        contextName: String,
        sourceDisplayName: String
    ) throws -> (raw: String, removedClusterCount: Int, removedUserCount: Int)? {
        var root = try rootMapping(raw: raw, sourceDisplayName: sourceDisplayName)
        let contexts = sequence(in: root, key: "contexts")
        let removedContexts = contexts.filter { name(in: $0) == contextName }
        guard !removedContexts.isEmpty else { return nil }

        let remainingContexts = contexts.filter { name(in: $0) != contextName }
        let remainingClusterNames = Set(remainingContexts.compactMap { contextReference(in: $0, key: "cluster") })
        let remainingUserNames = Set(remainingContexts.compactMap { contextReference(in: $0, key: "user") })
        let removedClusterNames = Set(removedContexts.compactMap { contextReference(in: $0, key: "cluster") })
            .subtracting(remainingClusterNames)
        let removedUserNames = Set(removedContexts.compactMap { contextReference(in: $0, key: "user") })
            .subtracting(remainingUserNames)

        let clusters = sequence(in: root, key: "clusters")
        let users = sequence(in: root, key: "users")
        let remainingClusters = clusters.filter { item in
            guard let itemName = name(in: item) else { return true }
            return !removedClusterNames.contains(itemName)
        }
        let remainingUsers = users.filter { item in
            guard let itemName = name(in: item) else { return true }
            return !removedUserNames.contains(itemName)
        }

        root[AnyHashable("contexts")] = remainingContexts
        root[AnyHashable("clusters")] = remainingClusters
        root[AnyHashable("users")] = remainingUsers
        if (root[AnyHashable("current-context")] as? String) == contextName {
            root[AnyHashable("current-context")] = remainingContexts.compactMap(name(in:)).first ?? ""
        }

        do {
            let output = try dump(
                object: root,
                indent: 2,
                width: -1,
                allowUnicode: true,
                sortKeys: false
            )
            let validated = try rootMapping(raw: output, sourceDisplayName: sourceDisplayName)
            let validatedContexts = sequence(in: validated, key: "contexts")
            guard validatedContexts.count == remainingContexts.count,
                  !validatedContexts.contains(where: { name(in: $0) == contextName }),
                  referencesResolve(in: validated) else {
                throw KubeConfigContextRemovalError.malformedKubeConfig(sourceDisplayName)
            }
            return (
                raw: output,
                removedClusterCount: clusters.count - remainingClusters.count,
                removedUserCount: users.count - remainingUsers.count
            )
        } catch {
            throw KubeConfigContextRemovalError.malformedKubeConfig(sourceDisplayName)
        }
    }

    private func rootMapping(raw: String, sourceDisplayName: String) throws -> YAMLMapping {
        do {
            var documents: [Any] = []
            var sequence = try load_all(yaml: raw)
            while let document = sequence.next() {
                documents.append(document)
            }
            if sequence.error != nil {
                throw KubeConfigContextRemovalError.malformedKubeConfig(sourceDisplayName)
            }
            guard documents.count == 1 else {
                throw KubeConfigContextRemovalError.multipleDocuments(sourceDisplayName)
            }
            guard let root = documents[0] as? YAMLMapping else {
                throw KubeConfigContextRemovalError.malformedKubeConfig(sourceDisplayName)
            }
            return root
        } catch let error as KubeConfigContextRemovalError {
            throw error
        } catch {
            throw KubeConfigContextRemovalError.malformedKubeConfig(sourceDisplayName)
        }
    }

    private func sequence(in root: YAMLMapping, key: String) -> [Any] {
        root[AnyHashable(key)] as? [Any] ?? []
    }

    private func name(in item: Any) -> String? {
        guard let mapping = item as? YAMLMapping,
              let value = mapping[AnyHashable("name")] as? String else { return nil }
        return value
    }

    private func contextReference(in item: Any, key: String) -> String? {
        guard let mapping = item as? YAMLMapping,
              let context = mapping[AnyHashable("context")] as? YAMLMapping,
              let value = context[AnyHashable(key)] as? String else { return nil }
        return value
    }

    private func referencesResolve(in root: YAMLMapping) -> Bool {
        let clusterNames = Set(sequence(in: root, key: "clusters").compactMap(name(in:)))
        let userNames = Set(sequence(in: root, key: "users").compactMap(name(in:)))
        for item in sequence(in: root, key: "contexts") {
            guard let cluster = contextReference(in: item, key: "cluster"),
                  clusterNames.contains(cluster) else { return false }
            if let user = contextReference(in: item, key: "user"),
               !user.isEmpty,
               !userNames.contains(user) {
                return false
            }
        }
        return true
    }
}
