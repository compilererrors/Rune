import RuneSecurity
import SwiftUI

struct KubeConfigImportReviewPanel: View {
    let review: KubeConfigImportReview
    let onClear: () -> Void
    let onRunAuthDoctor: () -> Void

    private var statusColor: Color {
        review.isValid ? .green : .orange
    }

    private var statusText: String {
        if review.isValid {
            return review.contexts.count == 1 ? "1 context ready to import" : "\(review.contexts.count) contexts ready to import"
        }
        return review.issues.contains { $0.severity == .error } ? "Import review needs attention" : "Import review has warnings"
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            header
            sourceSummary
            contextSummary
            issueSummary
            duplicateNameGuidance
            fullReviewDisclosure
            runAuthDoctorButton
        }
        .padding(10)
        .background(RuneSurfaceBackground(kind: .inset))
    }

    private var header: some View {
        HStack(spacing: 8) {
            Circle()
                .fill(statusColor)
                .frame(width: 8, height: 8)
            Text("Import Review")
                .font(.caption.weight(.semibold))
            Spacer(minLength: 0)
            Text(statusText)
                .font(.caption2.weight(.medium))
                .foregroundStyle(.secondary)
                .lineLimit(1)

            Button {
                onClear()
            } label: {
                Label("Clear", systemImage: "xmark.circle")
            }
            .buttonStyle(.bordered)
            .controlSize(.mini)
            .help("Clear the current kubeconfig import review.")
        }
    }

    @ViewBuilder
    private var sourceSummary: some View {
        if let sourceName = review.sourceName, !sourceName.isEmpty {
            Label(sourceName, systemImage: "doc.text.magnifyingglass")
                .font(.caption2)
                .foregroundStyle(.secondary)
                .lineLimit(1)
        }
    }

    @ViewBuilder
    private var contextSummary: some View {
        if review.contexts.isEmpty {
            Text("No contexts were found in the selected kubeconfig.")
                .font(.caption)
                .foregroundStyle(.secondary)
        } else {
            VStack(alignment: .leading, spacing: 6) {
                ForEach(Array(review.contexts.prefix(3).enumerated()), id: \.offset) { _, context in
                    VStack(alignment: .leading, spacing: 2) {
                        Text(context.name)
                            .font(.caption.weight(.semibold))
                            .lineLimit(1)
                        Text(contextDetailText(context))
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                            .lineLimit(2)
                    }
                }

                if review.contexts.count > 3 {
                    Text("\(review.contexts.count - 3) more contexts")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }
        }
    }

    @ViewBuilder
    private var issueSummary: some View {
        if !review.issues.isEmpty {
            VStack(alignment: .leading, spacing: 5) {
                ForEach(Array(review.issues.prefix(3).enumerated()), id: \.offset) { _, issue in
                    Label(issue.message, systemImage: issue.severity == .error ? "xmark.octagon.fill" : "exclamationmark.triangle.fill")
                        .font(.caption2)
                        .foregroundStyle(issue.severity == .error ? .red : .orange)
                        .lineLimit(2)
                }
            }
        }
    }

    @ViewBuilder
    private var duplicateNameGuidance: some View {
        if review.issues.contains(where: { $0.id.contains("duplicate") }) {
            Text("Duplicate handling will require an explicit choice: update existing, import as copy, or skip.")
                .font(.caption2)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
        }
    }

    private var fullReviewDisclosure: some View {
        DisclosureGroup {
            VStack(alignment: .leading, spacing: 10) {
                reviewContextsSection
                reviewIssuesSection
                reviewRedactedPreviewSection
            }
        } label: {
            Text("Full review")
                .font(.caption.weight(.semibold))
        }
    }

    private var runAuthDoctorButton: some View {
        HStack(spacing: 8) {
            Button {
                onRunAuthDoctor()
            } label: {
                Label("Run Auth Doctor", systemImage: "stethoscope")
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
        }
    }

    @ViewBuilder
    private var reviewContextsSection: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Contexts (\(review.contexts.count))")
                .font(.caption2.weight(.semibold))
                .foregroundStyle(.secondary)

            if review.contexts.isEmpty {
                Text("No contexts found.")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            } else {
                ScrollView {
                    VStack(alignment: .leading, spacing: 6) {
                        ForEach(Array(review.contexts.enumerated()), id: \.offset) { _, context in
                            VStack(alignment: .leading, spacing: 2) {
                                Text(context.name)
                                    .font(.caption.weight(.semibold))
                                    .lineLimit(1)
                                Text(contextDetailText(context))
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(2)
                            }
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
                .frame(maxHeight: 120)
            }
        }
    }

    @ViewBuilder
    private var reviewIssuesSection: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Issues (\(review.issues.count))")
                .font(.caption2.weight(.semibold))
                .foregroundStyle(.secondary)

            if review.issues.isEmpty {
                Label("No issues found.", systemImage: "checkmark.circle.fill")
                    .font(.caption2)
                    .foregroundStyle(.green)
            } else {
                ScrollView {
                    VStack(alignment: .leading, spacing: 5) {
                        ForEach(Array(review.issues.enumerated()), id: \.offset) { _, issue in
                            Label(issue.message, systemImage: issue.severity == .error ? "xmark.octagon.fill" : "exclamationmark.triangle.fill")
                                .font(.caption2)
                                .foregroundStyle(issue.severity == .error ? .red : .orange)
                                .lineLimit(3)
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
                .frame(maxHeight: 120)
            }
        }
    }

    @ViewBuilder
    private var reviewRedactedPreviewSection: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Redacted kubeconfig")
                .font(.caption2.weight(.semibold))
                .foregroundStyle(.secondary)

            ScrollView {
                Text(review.redactedPreview.isEmpty ? "No redacted preview is available." : review.redactedPreview)
                    .font(.system(.caption2, design: .monospaced))
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(maxHeight: 180)
        }
    }

    private func contextDetailText(_ context: KubeConfigImportContextPreview) -> String {
        [
            context.providerHint,
            context.authType,
            context.namespace.map { "namespace \($0)" },
            context.serverHost
        ]
        .compactMap { $0 }
        .joined(separator: " • ")
    }
}
