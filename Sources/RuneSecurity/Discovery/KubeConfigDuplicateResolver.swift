import Foundation
import Yams

public enum KubeConfigDuplicateResolutionError: Error, LocalizedError, Sendable, Equatable {
    case malformedKubeConfig
    case multipleDocuments

    public var errorDescription: String? {
        switch self {
        case .malformedKubeConfig:
            return "Kubeconfig duplicate handling failed because its YAML structure is invalid."
        case .multipleDocuments:
            return "Kubeconfig duplicate handling requires exactly one YAML document."
        }
    }
}

public struct KubeConfigNameRegistry: Sendable, Equatable {
    public var contextNames: Set<String>
    public var clusterNames: Set<String>
    public var userNames: Set<String>

    public init(
        contextNames: Set<String> = [],
        clusterNames: Set<String> = [],
        userNames: Set<String> = []
    ) {
        self.contextNames = contextNames
        self.clusterNames = clusterNames
        self.userNames = userNames
    }

    public mutating func formUnion(_ other: KubeConfigNameRegistry) {
        contextNames.formUnion(other.contextNames)
        clusterNames.formUnion(other.clusterNames)
        userNames.formUnion(other.userNames)
    }
}

public struct KubeConfigDuplicateResolution: Sendable, Equatable {
    public let raw: String
    public let names: KubeConfigNameRegistry
    public let contextNameVariants: [String: [String]]
    public let currentContextName: String?

    public init(
        raw: String,
        names: KubeConfigNameRegistry,
        contextNameVariants: [String: [String]],
        currentContextName: String? = nil
    ) {
        self.raw = raw
        self.names = names
        self.contextNameVariants = contextNameVariants
        self.currentContextName = currentContextName
    }
}

/// Applies an explicit, deterministic policy to duplicate named kubeconfig entries.
/// Payloads without duplicates are returned byte-for-byte to avoid needless rewrites.
public struct KubeConfigDuplicateResolver: Sendable {
    private typealias YAMLMapping = [AnyHashable: Any]

    public init() {}

    public func resolve(raw: String, choice: KubeConfigDuplicateHandlingChoice) throws -> String {
        try resolve(raw: raw, choice: choice, reservedNames: KubeConfigNameRegistry()).raw
    }

    public func names(in raw: String) throws -> KubeConfigNameRegistry {
        Self.registry(from: try rootMapping(raw: raw))
    }

    public func resolve(
        raw: String,
        choice: KubeConfigDuplicateHandlingChoice,
        reservedNames: KubeConfigNameRegistry
    ) throws -> KubeConfigDuplicateResolution {
        var root = try rootMapping(raw: raw)

        let clusters = Self.sequence(in: root, key: "clusters") ?? []
        let users = Self.sequence(in: root, key: "users") ?? []
        let contexts = Self.sequence(in: root, key: "contexts") ?? []
        let hasInternalDuplicates = Self.containsDuplicateName(in: clusters)
            || Self.containsDuplicateName(in: users)
            || Self.containsDuplicateName(in: contexts)
        let incomingNames = Self.registry(from: root)
        let hasReservedCopyConflict = choice == .importAsCopy && (
            !incomingNames.contextNames.isDisjoint(with: reservedNames.contextNames)
                || !incomingNames.clusterNames.isDisjoint(with: reservedNames.clusterNames)
                || !incomingNames.userNames.isDisjoint(with: reservedNames.userNames)
        )
        guard hasInternalDuplicates || hasReservedCopyConflict else {
            return KubeConfigDuplicateResolution(
                raw: raw,
                names: incomingNames,
                contextNameVariants: Dictionary(uniqueKeysWithValues: incomingNames.contextNames.map { ($0, [$0]) }),
                currentContextName: Self.currentContextName(in: root)
            )
        }

        var contextNameVariants: [String: [String]] = [:]

        switch choice {
        case .skipDuplicate:
            root[AnyHashable("clusters")] = Self.deduplicated(clusters, keepingLast: false)
            root[AnyHashable("users")] = Self.deduplicated(users, keepingLast: false)
            root[AnyHashable("contexts")] = Self.deduplicated(contexts, keepingLast: false)
            contextNameVariants = Self.identityVariants(in: Self.sequence(in: root, key: "contexts") ?? [])

        case .updateExisting:
            root[AnyHashable("clusters")] = Self.deduplicated(clusters, keepingLast: true)
            root[AnyHashable("users")] = Self.deduplicated(users, keepingLast: true)
            root[AnyHashable("contexts")] = Self.deduplicated(contexts, keepingLast: true)
            contextNameVariants = Self.identityVariants(in: Self.sequence(in: root, key: "contexts") ?? [])

        case .importAsCopy:
            let renamedClusters = Self.renamedCopies(in: clusters, reservedNames: reservedNames.clusterNames)
            let renamedUsers = Self.renamedCopies(in: users, reservedNames: reservedNames.userNames)
            let renamedContexts = Self.renamedCopies(in: contexts, reservedNames: reservedNames.contextNames)
            root[AnyHashable("clusters")] = renamedClusters.items
            root[AnyHashable("users")] = renamedUsers.items
            root[AnyHashable("contexts")] = Self.rewritingContextReferences(
                in: renamedContexts.items,
                clusterNames: renamedClusters.variants,
                userNames: renamedUsers.variants
            )
            contextNameVariants = renamedContexts.variants
            if let currentContext = root[AnyHashable("current-context")] as? String,
               let resolvedCurrentContext = renamedContexts.variants[currentContext]?.first {
                root[AnyHashable("current-context")] = resolvedCurrentContext
            }
        }

        do {
            let resolvedRaw = try dump(
                object: root,
                indent: 2,
                width: -1,
                allowUnicode: true,
                sortKeys: false
            )
            return KubeConfigDuplicateResolution(
                raw: resolvedRaw,
                names: Self.registry(from: root),
                contextNameVariants: contextNameVariants,
                currentContextName: Self.currentContextName(in: root)
            )
        } catch {
            throw KubeConfigDuplicateResolutionError.malformedKubeConfig
        }
    }

    private func rootMapping(raw: String) throws -> YAMLMapping {
        var documents: [Any] = []
        do {
            var sequence = try load_all(yaml: raw)
            while let document = sequence.next() {
                documents.append(document)
            }
            if sequence.error != nil {
                throw KubeConfigDuplicateResolutionError.malformedKubeConfig
            }
        } catch let error as KubeConfigDuplicateResolutionError {
            throw error
        } catch {
            throw KubeConfigDuplicateResolutionError.malformedKubeConfig
        }
        guard documents.count == 1 else {
            throw KubeConfigDuplicateResolutionError.multipleDocuments
        }
        guard let root = documents[0] as? YAMLMapping else {
            throw KubeConfigDuplicateResolutionError.malformedKubeConfig
        }
        return root
    }

    private static func sequence(in mapping: YAMLMapping, key: String) -> [Any]? {
        mapping[AnyHashable(key)] as? [Any]
    }

    private static func registry(from root: YAMLMapping) -> KubeConfigNameRegistry {
        KubeConfigNameRegistry(
            contextNames: Set((sequence(in: root, key: "contexts") ?? []).compactMap(name(in:))),
            clusterNames: Set((sequence(in: root, key: "clusters") ?? []).compactMap(name(in:))),
            userNames: Set((sequence(in: root, key: "users") ?? []).compactMap(name(in:)))
        )
    }

    private static func currentContextName(in root: YAMLMapping) -> String? {
        guard let raw = root[AnyHashable("current-context")] as? String else { return nil }
        let value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        return value.isEmpty ? nil : value
    }

    private static func identityVariants(in items: [Any]) -> [String: [String]] {
        Dictionary(uniqueKeysWithValues: items.compactMap(name(in:)).map { ($0, [$0]) })
    }

    private static func name(in item: Any) -> String? {
        guard let mapping = item as? YAMLMapping,
              let rawName = mapping[AnyHashable("name")] as? String else {
            return nil
        }
        let name = rawName.trimmingCharacters(in: .whitespacesAndNewlines)
        return name.isEmpty ? nil : name
    }

    private static func containsDuplicateName(in items: [Any]) -> Bool {
        var seen = Set<String>()
        for item in items {
            guard let name = name(in: item) else { continue }
            if !seen.insert(name).inserted {
                return true
            }
        }
        return false
    }

    private static func deduplicated(_ items: [Any], keepingLast: Bool) -> [Any] {
        var seen = Set<String>()
        if keepingLast {
            return items.reversed().filter { item in
                guard let name = name(in: item) else { return true }
                return seen.insert(name).inserted
            }.reversed()
        }
        return items.filter { item in
            guard let name = name(in: item) else { return true }
            return seen.insert(name).inserted
        }
    }

    private static func renamedCopies(
        in items: [Any],
        reservedNames: Set<String> = []
    ) -> (items: [Any], variants: [String: [String]]) {
        var usedNames = reservedNames.union(items.compactMap(name(in:)))
        var occurrences: [String: Int] = [:]
        var variants: [String: [String]] = [:]
        var renamed: [Any] = []
        renamed.reserveCapacity(items.count)

        for item in items {
            guard var mapping = item as? YAMLMapping,
                  let originalName = name(in: item) else {
                renamed.append(item)
                continue
            }
            let occurrence = occurrences[originalName, default: 0] + 1
            occurrences[originalName] = occurrence

            let resolvedName: String
            if occurrence == 1, !reservedNames.contains(originalName) {
                resolvedName = originalName
            } else {
                var suffix = max(2, occurrence)
                var candidate = "\(originalName)-copy-\(suffix)"
                while usedNames.contains(candidate) {
                    suffix += 1
                    candidate = "\(originalName)-copy-\(suffix)"
                }
                resolvedName = candidate
                usedNames.insert(candidate)
                mapping[AnyHashable("name")] = resolvedName
            }
            variants[originalName, default: []].append(resolvedName)
            renamed.append(mapping)
        }
        return (renamed, variants)
    }

    private static func rewritingContextReferences(
        in items: [Any],
        clusterNames: [String: [String]],
        userNames: [String: [String]]
    ) -> [Any] {
        var clusterOffsets: [String: Int] = [:]
        var userOffsets: [String: Int] = [:]

        return items.map { item in
            guard var namedContext = item as? YAMLMapping,
                  var context = namedContext[AnyHashable("context")] as? YAMLMapping else {
                return item
            }
            rewriteReference(
                key: "cluster",
                in: &context,
                variants: clusterNames,
                offsets: &clusterOffsets
            )
            rewriteReference(
                key: "user",
                in: &context,
                variants: userNames,
                offsets: &userOffsets
            )
            namedContext[AnyHashable("context")] = context
            return namedContext
        }
    }

    private static func rewriteReference(
        key: String,
        in mapping: inout YAMLMapping,
        variants: [String: [String]],
        offsets: inout [String: Int]
    ) {
        guard let original = mapping[AnyHashable(key)] as? String,
              let names = variants[original], !names.isEmpty else {
            return
        }
        let offset = offsets[original, default: 0]
        mapping[AnyHashable(key)] = names[min(offset, names.count - 1)]
        offsets[original] = offset + 1
    }
}
