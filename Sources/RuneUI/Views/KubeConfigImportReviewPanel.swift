import RuneSecurity
import SwiftUI

struct KubeConfigImportReviewPanel: View {
    let review: KubeConfigImportReview
    @Binding var duplicateHandlingChoice: KubeConfigDuplicateHandlingChoice
    let metadataDrafts: [String: ContextDisplayMetadata]
    let onUpdateMetadata: (String, ContextDisplayMetadata) -> Void
    let onClear: () -> Void
    let showsAuthDoctorAction: Bool
    let onRunAuthDoctor: () -> Void

    init(
        review: KubeConfigImportReview,
        duplicateHandlingChoice: Binding<KubeConfigDuplicateHandlingChoice>,
        metadataDrafts: [String: ContextDisplayMetadata],
        onUpdateMetadata: @escaping (String, ContextDisplayMetadata) -> Void,
        onClear: @escaping () -> Void,
        showsAuthDoctorAction: Bool = true,
        onRunAuthDoctor: @escaping () -> Void
    ) {
        self.review = review
        _duplicateHandlingChoice = duplicateHandlingChoice
        self.metadataDrafts = metadataDrafts
        self.onUpdateMetadata = onUpdateMetadata
        self.onClear = onClear
        self.showsAuthDoctorAction = showsAuthDoctorAction
        self.onRunAuthDoctor = onRunAuthDoctor
    }

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
            importMetadataSection
            issueSummary
            duplicateNameGuidance
            fullReviewDisclosure
            if showsAuthDoctorAction {
                runAuthDoctorButton
            }
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
    private var importMetadataSection: some View {
        if !review.contexts.isEmpty {
            DisclosureGroup {
                VStack(alignment: .leading, spacing: 8) {
                    ForEach(Array(review.contexts.prefix(3).enumerated()), id: \.offset) { _, context in
                        let metadata = metadataDrafts[context.name] ?? ContextDisplayMetadata()
                        VStack(alignment: .leading, spacing: 5) {
                            Text(context.name)
                                .font(.caption2.weight(.semibold))
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                            HStack(spacing: 6) {
                                TextField("Alias", text: metadataAliasBinding(for: context, metadata: metadata))
                                    .textFieldStyle(.roundedBorder)
                                TextField("Group", text: metadataGroupBinding(for: context, metadata: metadata))
                                    .textFieldStyle(.roundedBorder)
                            }
                            HStack(spacing: 6) {
                                TextField("Color key", text: metadataColorBinding(for: context, metadata: metadata))
                                    .textFieldStyle(.roundedBorder)
                                TextField("Icon", text: metadataIconBinding(for: context, metadata: metadata))
                                    .textFieldStyle(.roundedBorder)
                            }
                            TextField("Tags", text: metadataTagsBinding(for: context, metadata: metadata))
                                .textFieldStyle(.roundedBorder)
                        }
                    }

                    if review.contexts.count > 3 {
                        Text("\(review.contexts.count - 3) more contexts can keep generated metadata during import.")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                }
            } label: {
                Text("Import metadata")
                    .font(.caption.weight(.semibold))
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
        if !review.duplicateHandlingChoices.isEmpty {
            VStack(alignment: .leading, spacing: 5) {
                Text("Duplicate handling requires an explicit choice before saving:")
                    .font(.caption2.weight(.semibold))
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)

                Picker("Duplicate handling", selection: $duplicateHandlingChoice) {
                    ForEach(review.duplicateHandlingChoices, id: \.rawValue) { choice in
                        Text(choice.title).tag(choice)
                    }
                }
                .pickerStyle(.segmented)
                .labelsHidden()

                ForEach(review.duplicateHandlingChoices, id: \.rawValue) { choice in
                    Label {
                        Text("\(choice.title): \(choice.detail)")
                            .font(.caption2)
                            .foregroundStyle(choice == duplicateHandlingChoice ? .primary : .secondary)
                            .fixedSize(horizontal: false, vertical: true)
                    } icon: {
                        Image(systemName: duplicateHandlingChoiceIcon(choice))
                    }
                }
            }
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

    private func duplicateHandlingChoiceIcon(_ choice: KubeConfigDuplicateHandlingChoice) -> String {
        switch choice {
        case .updateExisting: return "arrow.triangle.2.circlepath"
        case .importAsCopy: return "doc.on.doc"
        case .skipDuplicate: return "minus.circle"
        }
    }

    private func metadataAliasBinding(for context: KubeConfigImportContextPreview, metadata: ContextDisplayMetadata) -> Binding<String> {
        Binding(
            get: { metadata.alias ?? "" },
            set: { value in
                updateMetadata(for: context, metadata: metadata, alias: value)
            }
        )
    }

    private func metadataColorBinding(for context: KubeConfigImportContextPreview, metadata: ContextDisplayMetadata) -> Binding<String> {
        Binding(
            get: { metadata.colorKey ?? "" },
            set: { value in
                updateMetadata(for: context, metadata: metadata, colorKey: value)
            }
        )
    }

    private func metadataIconBinding(for context: KubeConfigImportContextPreview, metadata: ContextDisplayMetadata) -> Binding<String> {
        Binding(
            get: { metadata.iconName ?? "" },
            set: { value in
                updateMetadata(for: context, metadata: metadata, iconName: value)
            }
        )
    }

    private func metadataTagsBinding(for context: KubeConfigImportContextPreview, metadata: ContextDisplayMetadata) -> Binding<String> {
        Binding(
            get: { metadata.tags.joined(separator: ", ") },
            set: { value in
                updateMetadata(
                    for: context,
                    metadata: metadata,
                    tags: value.split(separator: ",").map(String.init)
                )
            }
        )
    }

    private func metadataGroupBinding(for context: KubeConfigImportContextPreview, metadata: ContextDisplayMetadata) -> Binding<String> {
        Binding(
            get: { metadata.group ?? "" },
            set: { value in
                updateMetadata(for: context, metadata: metadata, group: value)
            }
        )
    }

    private func updateMetadata(
        for context: KubeConfigImportContextPreview,
        metadata: ContextDisplayMetadata,
        alias: String? = nil,
        colorKey: String? = nil,
        iconName: String? = nil,
        tags: [String]? = nil,
        group: String? = nil
    ) {
        onUpdateMetadata(
            context.name,
            ContextDisplayMetadata(
                alias: alias ?? metadata.alias,
                colorKey: colorKey ?? metadata.colorKey,
                iconName: iconName ?? metadata.iconName,
                tags: tags ?? metadata.tags,
                group: group ?? metadata.group
            )
        )
    }
}
