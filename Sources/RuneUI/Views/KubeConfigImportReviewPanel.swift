import RuneSecurity
import SwiftUI

enum KubeConfigImportReviewLayoutRegion: Hashable, Sendable {
    case header
    case contentViewport
    case actions
}

struct KubeConfigImportReviewLayoutSnapshot: Equatable, Sendable {
    let frames: [KubeConfigImportReviewLayoutRegion: CGRect]

    subscript(_ region: KubeConfigImportReviewLayoutRegion) -> CGRect? {
        frames[region]
    }
}

struct KubeConfigImportReviewPanel: View {
    let review: KubeConfigImportReview
    @Binding var duplicateHandlingChoice: KubeConfigDuplicateHandlingChoice
    let metadataDrafts: [String: ContextDisplayMetadata]
    let onUpdateMetadata: (String, ContextDisplayMetadata) -> Void
    let reviewMode: KubeConfigImportReviewMode
    let isConfirmationPending: Bool
    let canConfirm: Bool
    let isCommitInProgress: Bool
    let onConfirm: () -> Void
    let onCancel: () -> Void
    let onClear: () -> Void
    let showsAuthDoctorAction: Bool
    let onRunAuthDoctor: () -> Void
    let onLayoutSnapshotChange: ((KubeConfigImportReviewLayoutSnapshot) -> Void)?
    @Environment(\.runeThemePalette) private var runeThemePalette

    init(
        review: KubeConfigImportReview,
        duplicateHandlingChoice: Binding<KubeConfigDuplicateHandlingChoice>,
        metadataDrafts: [String: ContextDisplayMetadata],
        onUpdateMetadata: @escaping (String, ContextDisplayMetadata) -> Void,
        reviewMode: KubeConfigImportReviewMode = .report,
        isConfirmationPending: Bool = false,
        canConfirm: Bool = false,
        isCommitInProgress: Bool = false,
        onConfirm: @escaping () -> Void = {},
        onCancel: @escaping () -> Void = {},
        onClear: @escaping () -> Void,
        showsAuthDoctorAction: Bool = true,
        onRunAuthDoctor: @escaping () -> Void,
        onLayoutSnapshotChange: ((KubeConfigImportReviewLayoutSnapshot) -> Void)? = nil
    ) {
        self.review = review
        _duplicateHandlingChoice = duplicateHandlingChoice
        self.metadataDrafts = metadataDrafts
        self.onUpdateMetadata = onUpdateMetadata
        self.reviewMode = reviewMode
        self.isConfirmationPending = isConfirmationPending
        self.canConfirm = canConfirm
        self.isCommitInProgress = isCommitInProgress
        self.onConfirm = onConfirm
        self.onCancel = onCancel
        self.onClear = onClear
        self.showsAuthDoctorAction = showsAuthDoctorAction
        self.onRunAuthDoctor = onRunAuthDoctor
        self.onLayoutSnapshotChange = onLayoutSnapshotChange
    }

    private var statusColor: Color {
        canConfirm || review.isValid
            ? RuneSemanticColorRole.success.color(in: runeThemePalette)
            : RuneSemanticColorRole.warning.color(in: runeThemePalette)
    }

    private var statusText: String {
        if isCommitInProgress {
            return "Saving import…"
        }
        if isConfirmationPending, canConfirm {
            return review.contexts.count == 1 ? "1 context ready to confirm" : "\(review.contexts.count) contexts ready to confirm"
        }
        if review.isValid {
            if reviewMode == .report {
                return review.contexts.count == 1 ? "1 context imported" : "\(review.contexts.count) contexts imported"
            }
            return review.contexts.count == 1 ? "1 context ready to import" : "\(review.contexts.count) contexts ready to import"
        }
        return review.issues.contains { $0.severity == .error } ? "Import review needs attention" : "Import review has warnings"
    }

    var body: some View {
        Group {
            if isConfirmationPending {
                confirmationLayout
            } else {
                reportLayout
            }
        }
        .background(RuneSurfaceBackground(kind: .inset))
        .accessibilityElement(children: .contain)
        .accessibilityIdentifier("rune.kubeconfig-import.review")
        .kubeConfigImportReviewLayoutReporting(onLayoutSnapshotChange)
    }

    private var confirmationLayout: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
                .fixedSize(horizontal: false, vertical: true)
                .padding(.horizontal, 12)
                .padding(.vertical, 10)
                .kubeConfigImportReviewLayoutProbe(.header, enabled: onLayoutSnapshotChange != nil)

            Divider()

            ScrollView(.vertical) {
                reviewContent
                    .padding(12)
                    .frame(maxWidth: .infinity, alignment: .topLeading)
            }
            .scrollContentBackground(.hidden)
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .layoutPriority(1)
            .kubeConfigImportReviewLayoutProbe(.contentViewport, enabled: onLayoutSnapshotChange != nil)

            confirmationActions
                .fixedSize(horizontal: false, vertical: true)
                .padding(.horizontal, 12)
                .padding(.bottom, 10)
                .kubeConfigImportReviewLayoutProbe(.actions, enabled: onLayoutSnapshotChange != nil)
        }
    }

    private var reportLayout: some View {
        VStack(alignment: .leading, spacing: 10) {
            header
                .kubeConfigImportReviewLayoutProbe(.header, enabled: onLayoutSnapshotChange != nil)
            reviewContent
                .kubeConfigImportReviewLayoutProbe(.contentViewport, enabled: onLayoutSnapshotChange != nil)
            if showsAuthDoctorAction, reviewMode == .report {
                runAuthDoctorButton
            }
        }
        .padding(10)
    }

    private var reviewContent: some View {
        VStack(alignment: .leading, spacing: 10) {
            sourceSummary
            contextSummary
            importMetadataSection
            issueSummary
            duplicateNameGuidance
            fullReviewDisclosure
        }
        .frame(maxWidth: .infinity, alignment: .topLeading)
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
                .accessibilityLabel(statusText)
                .accessibilityIdentifier("rune.kubeconfig-import.status")

            if isCommitInProgress {
                ProgressView()
                    .controlSize(.small)
                    .accessibilityLabel("Saving kubeconfig import")
            } else {
                RuneDialogCloseButton(isConfirmationPending ? "Cancel kubeconfig import" : "Clear kubeconfig import report") {
                    if isConfirmationPending {
                        onCancel()
                    } else {
                        onClear()
                    }
                }
            }
        }
    }

    private var confirmationActions: some View {
        RuneDialogActionBar {
            Button("Cancel", action: onCancel)
                .keyboardShortcut(.cancelAction)
                .disabled(isCommitInProgress)
            Button(action: onConfirm) {
                RuneDialogButtonLabel(isCommitInProgress ? "Importing…" : "Import")
            }
            .accessibilityIdentifier("rune.kubeconfig-import.confirm")
            .buttonStyle(.borderedProminent)
            .keyboardShortcut(.defaultAction)
            .disabled(!canConfirm || isCommitInProgress)
        }
    }

    @ViewBuilder
    private var sourceSummary: some View {
        if let sourceName = review.sourceName, !sourceName.isEmpty {
            Label(sourceName, systemImage: "doc.text.magnifyingglass")
                .font(.caption2)
                .foregroundStyle(.secondary)
                .lineLimit(1)
                .help(sourceName)
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
                            .help(context.name)
                        Text(contextDetailText(context))
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                            .lineLimit(2)
                            .help(contextDetailText(context))
                    }
                    .accessibilityElement(children: .combine)
                    .accessibilityIdentifier("rune.kubeconfig-import.context.\(context.name)")
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
            RuneDisclosureSection("Import metadata") {
                VStack(alignment: .leading, spacing: 8) {
                    ForEach(Array(review.contexts.prefix(3).enumerated()), id: \.offset) { _, context in
                        let metadata = metadataDrafts[context.name] ?? ContextDisplayMetadata()
                        VStack(alignment: .leading, spacing: 5) {
                            Text(context.name)
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                                .help(context.name)
                            HStack(spacing: 6) {
                                KubeConfigImportMetadataDraftField(
                                    "Alias",
                                    value: metadata.alias ?? "",
                                    canonicalize: KubeConfigImportMetadataDraft.canonicalText
                                ) { value in
                                    updateMetadata(for: context, metadata: metadata, alias: value)
                                }
                                    .textFieldStyle(.roundedBorder)
                                KubeConfigImportMetadataDraftField(
                                    "Group",
                                    value: metadata.group ?? "",
                                    canonicalize: KubeConfigImportMetadataDraft.canonicalText
                                ) { value in
                                    updateMetadata(for: context, metadata: metadata, group: value)
                                }
                                    .textFieldStyle(.roundedBorder)
                            }
                            HStack(spacing: 6) {
                                KubeConfigImportMetadataDraftField(
                                    "Color key",
                                    value: metadata.colorKey ?? "",
                                    canonicalize: KubeConfigImportMetadataDraft.canonicalText
                                ) { value in
                                    updateMetadata(for: context, metadata: metadata, colorKey: value)
                                }
                                    .textFieldStyle(.roundedBorder)
                                KubeConfigImportMetadataDraftField(
                                    "Icon",
                                    value: metadata.iconName ?? "",
                                    canonicalize: KubeConfigImportMetadataDraft.canonicalText
                                ) { value in
                                    updateMetadata(for: context, metadata: metadata, iconName: value)
                                }
                                    .textFieldStyle(.roundedBorder)
                            }
                            KubeConfigImportMetadataDraftField(
                                "Tags",
                                value: metadata.tags.joined(separator: ", "),
                                canonicalize: KubeConfigImportMetadataDraft.canonicalTagsText
                            ) { value in
                                updateMetadata(
                                    for: context,
                                    metadata: metadata,
                                    tags: KubeConfigImportMetadataDraft.tags(from: value)
                                )
                            }
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
                    Label(issue.message, systemImage: issueIcon(issue))
                        .font(.caption2)
                        .foregroundStyle(issueColor(issue))
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
                    .font(.caption2)
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
        RuneDisclosureSection("Full review") {
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
                reviewDetailViewport(maxHeight: 120) {
                    VStack(alignment: .leading, spacing: 6) {
                        ForEach(Array(review.contexts.enumerated()), id: \.offset) { _, context in
                            VStack(alignment: .leading, spacing: 2) {
                                Text(context.name)
                                    .font(.caption.weight(.semibold))
                                    .lineLimit(1)
                                    .help(context.name)
                                Text(contextDetailText(context))
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(2)
                                    .help(contextDetailText(context))
                            }
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
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
                    .foregroundStyle(RuneSemanticColorRole.success.color(in: runeThemePalette))
            } else {
                reviewDetailViewport(maxHeight: 120) {
                    VStack(alignment: .leading, spacing: 5) {
                        ForEach(Array(review.issues.enumerated()), id: \.offset) { _, issue in
                            Label(issue.message, systemImage: issueIcon(issue))
                                .font(.caption2)
                                .foregroundStyle(issueColor(issue))
                                .lineLimit(3)
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
        }
    }

    @ViewBuilder
    private var reviewRedactedPreviewSection: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Redacted kubeconfig")
                .font(.caption2.weight(.semibold))
                .foregroundStyle(.secondary)

            reviewDetailViewport(maxHeight: 180) {
                Text(review.redactedPreview.isEmpty ? "No redacted preview is available." : review.redactedPreview)
                    .font(.system(.caption2, design: .monospaced))
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
    }

    @ViewBuilder
    private func reviewDetailViewport<Content: View>(
        maxHeight: CGFloat,
        @ViewBuilder content: () -> Content
    ) -> some View {
        if isConfirmationPending {
            content()
        } else {
            ScrollView(.vertical) {
                content()
            }
            .frame(maxHeight: maxHeight)
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

    private func issueIcon(_ issue: KubeConfigImportIssue) -> String {
        if isConfirmationPending, canConfirm, issue.id.contains("duplicate") {
            return "checkmark.circle.fill"
        }
        return issue.severity == .error ? "xmark.octagon.fill" : "exclamationmark.triangle.fill"
    }

    private func issueColor(_ issue: KubeConfigImportIssue) -> Color {
        if isConfirmationPending, canConfirm, issue.id.contains("duplicate") {
            return RuneSemanticColorRole.success.color(in: runeThemePalette)
        }
        return issue.severity == .error
            ? RuneSemanticColorRole.danger.color(in: runeThemePalette)
            : RuneSemanticColorRole.warning.color(in: runeThemePalette)
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

enum KubeConfigImportMetadataDraft {
    static func canonicalText(_ value: String) -> String {
        ContextDisplayMetadata(alias: value).alias ?? ""
    }

    static func tags(from value: String) -> [String] {
        value.components(separatedBy: ",")
    }

    static func canonicalTagsText(_ value: String) -> String {
        ContextDisplayMetadata(tags: tags(from: value))
            .tags
            .joined(separator: ", ")
    }
}

struct KubeConfigImportMetadataDraftField: View {
    private let title: String
    private let value: String
    private let canonicalize: (String) -> String
    private let onChange: (String) -> Void
    @State private var draftText: String
    @FocusState private var isFocused: Bool

    init(
        _ title: String,
        value: String,
        canonicalize: @escaping (String) -> String,
        onChange: @escaping (String) -> Void
    ) {
        self.title = title
        self.value = value
        self.canonicalize = canonicalize
        self.onChange = onChange
        _draftText = State(initialValue: value)
    }

    var body: some View {
        TextField(
            title,
            text: Binding(
                get: { draftText },
                set: { newValue in
                    draftText = newValue
                    onChange(newValue)
                }
            )
        )
        .runeTextInputCursor()
        .focused($isFocused)
        .onSubmit(commitDraft)
        .onChange(of: isFocused) { wasFocused, isFocused in
            guard wasFocused, !isFocused else { return }
            commitDraft()
        }
        .onChange(of: value) { _, newValue in
            guard !isFocused, draftText != newValue else { return }
            draftText = newValue
        }
    }

    private func commitDraft() {
        let canonicalValue = canonicalize(draftText)
        if draftText != canonicalValue {
            draftText = canonicalValue
        }
        if value != canonicalValue {
            onChange(canonicalValue)
        }
    }
}

private enum KubeConfigImportReviewLayoutCoordinateSpace {
    static let name = "KubeConfigImportReviewLayoutCoordinateSpace"
}

private struct KubeConfigImportReviewLayoutFramePreferenceKey: PreferenceKey {
    static let defaultValue: [KubeConfigImportReviewLayoutRegion: CGRect] = [:]

    static func reduce(
        value: inout [KubeConfigImportReviewLayoutRegion: CGRect],
        nextValue: () -> [KubeConfigImportReviewLayoutRegion: CGRect]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private extension View {
    @ViewBuilder
    func kubeConfigImportReviewLayoutProbe(
        _ region: KubeConfigImportReviewLayoutRegion,
        enabled: Bool
    ) -> some View {
        if enabled {
            overlay {
                GeometryReader { proxy in
                    Color.clear.preference(
                        key: KubeConfigImportReviewLayoutFramePreferenceKey.self,
                        value: [
                            region: proxy.frame(in: .named(KubeConfigImportReviewLayoutCoordinateSpace.name))
                        ]
                    )
                }
                .allowsHitTesting(false)
                .accessibilityHidden(true)
            }
        } else {
            self
        }
    }

    @ViewBuilder
    func kubeConfigImportReviewLayoutReporting(
        _ onChange: ((KubeConfigImportReviewLayoutSnapshot) -> Void)?
    ) -> some View {
        if let onChange {
            coordinateSpace(name: KubeConfigImportReviewLayoutCoordinateSpace.name)
                .onPreferenceChange(KubeConfigImportReviewLayoutFramePreferenceKey.self) { frames in
                    onChange(KubeConfigImportReviewLayoutSnapshot(frames: frames))
                }
        } else {
            self
        }
    }
}
