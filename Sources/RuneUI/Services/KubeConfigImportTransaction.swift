import Foundation
import RuneSecurity

public enum KubeConfigImportReviewMode: Sendable, Equatable {
    case preflight
    case report
}

enum KubeConfigImportSourcePlacement: Equatable {
    case append
    case prependNewestFirst
}

struct KubeConfigImportTransaction {
    struct Payload {
        let raw: String
        let sourceName: String
        let sourceURL: URL?
        let origin: KubeConfigImportOrigin

        init(raw: String, sourceName: String, sourceURL: URL?, origin: KubeConfigImportOrigin = .init()) {
            self.raw = raw
            self.sourceName = sourceName
            self.sourceURL = sourceURL
            self.origin = origin
        }
    }

    struct Resolution {
        let payloads: [Payload]
        let reviews: [KubeConfigImportReview]
        let contextNamesForPreferences: Set<String>
        let sourcePlacement: KubeConfigImportSourcePlacement
        let preferredContextName: String?
    }

    let payloads: [Payload]
    let reviews: [KubeConfigImportReview]
    let logLabel: String
    let id = UUID()
    private let existingNames: KubeConfigNameRegistry

    init(
        payloads: [Payload],
        logLabel: String,
        existingNames: KubeConfigNameRegistry,
        validator: KubeConfigImportValidator,
        resolver: KubeConfigDuplicateResolver
    ) {
        self.payloads = payloads
        self.logLabel = logLabel
        self.existingNames = existingNames

        var seenNames = existingNames
        reviews = payloads.map { payload in
            let review = validator.validate(raw: payload.raw, sourceName: payload.sourceName)
            guard let names = try? resolver.names(in: payload.raw) else { return review }
            let conflicts = Self.conflicts(in: names, comparedWith: seenNames)
            seenNames.formUnion(names)
            guard !conflicts.isEmpty else { return review }
            return KubeConfigImportReview(
                contexts: review.contexts,
                issues: review.issues + Self.conflictIssues(conflicts),
                redactedPreview: review.redactedPreview,
                sourceName: review.sourceName,
                hasDuplicateConflicts: true
            )
        }
    }

    func resolvingDuplicates(
        choice: KubeConfigDuplicateHandlingChoice,
        resolver: KubeConfigDuplicateResolver,
        validator: KubeConfigImportValidator
    ) throws -> Resolution {
        var seenNames = existingNames
        var resolvedPayloads: [Payload] = []
        var resolvedReviews: [KubeConfigImportReview] = []
        var contextNamesForPreferences = Set<String>()
        var preferredContextName: String?
        resolvedPayloads.reserveCapacity(payloads.count)
        resolvedReviews.reserveCapacity(payloads.count)

        for (payload, originalReview) in zip(payloads, reviews) {
            let originalNames = try resolver.names(in: payload.raw)
            let crossSourceConflicts = Self.conflicts(in: originalNames, comparedWith: seenNames)
            let hadDuplicateConflicts = originalReview.hasDuplicateConflicts
                || !originalReview.duplicateHandlingChoices.isEmpty
                || !crossSourceConflicts.isEmpty
            let duplicateResolution = try resolver.resolve(
                raw: payload.raw,
                choice: choice,
                reservedNames: choice == .importAsCopy ? seenNames : KubeConfigNameRegistry()
            )
            let resolvedPayload = Payload(
                raw: duplicateResolution.raw,
                sourceName: payload.sourceName,
                sourceURL: payload.sourceURL,
                origin: payload.origin
            )
            let validated = validator.validate(
                raw: resolvedPayload.raw,
                sourceName: resolvedPayload.sourceName
            )

            let committedContexts: [KubeConfigImportContextPreview]
            if choice == .skipDuplicate {
                committedContexts = validated.contexts.filter { !seenNames.contextNames.contains($0.name) }
            } else {
                committedContexts = validated.contexts
            }
            let unsafeSkipDuplicateContextCount: Int
            if choice == .skipDuplicate {
                let conflictingClusterNames = originalNames.clusterNames.intersection(seenNames.clusterNames)
                let conflictingUserNames = originalNames.userNames.intersection(seenNames.userNames)
                unsafeSkipDuplicateContextCount = committedContexts.filter { context in
                    context.clusterName.map(conflictingClusterNames.contains) == true
                        || context.userName.map(conflictingUserNames.contains) == true
                }.count
            } else {
                unsafeSkipDuplicateContextCount = 0
            }
            contextNamesForPreferences.formUnion(committedContexts.map(\.name))
            if let currentContextName = duplicateResolution.currentContextName,
               committedContexts.contains(where: { $0.name == currentContextName }) {
                preferredContextName = currentContextName
            }

            var issues = validated.issues
            if unsafeSkipDuplicateContextCount > 0 {
                let contextLabel = unsafeSkipDuplicateContextCount == 1 ? "context" : "contexts"
                issues.append(KubeConfigImportIssue(
                    id: "skip-duplicate-dependent-name-conflict",
                    severity: .error,
                    message: "Skip duplicate would bind \(unsafeSkipDuplicateContextCount) new \(contextLabel) to earlier loaded cluster or user definitions. Choose Update existing or Import as copy."
                ))
            }
            if hadDuplicateConflicts {
                issues.append(KubeConfigImportIssue(
                    id: "duplicate-policy-applied",
                    severity: .warning,
                    message: "\(choice.title) will be applied when this import is confirmed."
                ))
            }
            resolvedReviews.append(KubeConfigImportReview(
                contexts: committedContexts,
                issues: issues,
                redactedPreview: validated.redactedPreview,
                sourceName: validated.sourceName,
                hasDuplicateConflicts: hadDuplicateConflicts
            ))
            resolvedPayloads.append(resolvedPayload)
            seenNames.formUnion(choice == .importAsCopy ? duplicateResolution.names : originalNames)
        }

        return Resolution(
            payloads: resolvedPayloads,
            reviews: resolvedReviews,
            contextNamesForPreferences: contextNamesForPreferences,
            sourcePlacement: choice == .updateExisting ? .prependNewestFirst : .append,
            preferredContextName: preferredContextName
        )
    }

    private struct Conflicts {
        let contexts: Int
        let clusters: Int
        let users: Int

        var isEmpty: Bool {
            contexts == 0 && clusters == 0 && users == 0
        }
    }

    private static func conflicts(
        in incoming: KubeConfigNameRegistry,
        comparedWith existing: KubeConfigNameRegistry
    ) -> Conflicts {
        Conflicts(
            contexts: incoming.contextNames.intersection(existing.contextNames).count,
            clusters: incoming.clusterNames.intersection(existing.clusterNames).count,
            users: incoming.userNames.intersection(existing.userNames).count
        )
    }

    private static func conflictIssues(_ conflicts: Conflicts) -> [KubeConfigImportIssue] {
        var issues: [KubeConfigImportIssue] = []
        if conflicts.contexts > 0 {
            issues.append(KubeConfigImportIssue(
                id: "duplicate-existing-contexts",
                severity: .error,
                message: countMessage(conflicts.contexts, singular: "context name", plural: "context names")
            ))
        }
        if conflicts.clusters > 0 {
            issues.append(KubeConfigImportIssue(
                id: "duplicate-existing-clusters",
                severity: .error,
                message: countMessage(conflicts.clusters, singular: "cluster name", plural: "cluster names")
            ))
        }
        if conflicts.users > 0 {
            issues.append(KubeConfigImportIssue(
                id: "duplicate-existing-users",
                severity: .error,
                message: countMessage(conflicts.users, singular: "user name", plural: "user names")
            ))
        }
        return issues
    }

    private static func countMessage(_ count: Int, singular: String, plural: String) -> String {
        let label = count == 1 ? singular : plural
        return "\(count) \(label) conflicts with another loaded or selected kubeconfig. Choose how Rune should resolve it."
    }
}
