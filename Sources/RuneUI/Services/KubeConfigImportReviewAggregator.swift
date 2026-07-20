import RuneSecurity

enum KubeConfigImportReviewAggregator {
    static func aggregate(_ reviews: [KubeConfigImportReview]) -> KubeConfigImportReview? {
        guard !reviews.isEmpty else { return nil }
        guard reviews.count > 1 else { return reviews[0] }

        return KubeConfigImportReview(
            contexts: reviews.flatMap(\.contexts),
            issues: reviews.flatMap(\.issues),
            redactedPreview: reviews.map { review in
                let source = review.sourceName ?? "Kubeconfig"
                return "# \(source)\n\(review.redactedPreview)"
            }.joined(separator: "\n---\n"),
            sourceName: "\(reviews.count) kubeconfig files",
            hasDuplicateConflicts: reviews.contains { $0.hasDuplicateConflicts }
                || reviews.contains { !$0.duplicateHandlingChoices.isEmpty }
        )
    }
}
