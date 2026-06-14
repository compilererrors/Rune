import SwiftUI
import RuneCore

struct ResourceYAMLEditorSurface: View {
    @Binding var text: String
    let displayText: String
    let readOnlyResetID: String
    let inlineEditing: Bool
    let implementation: ManifestInlineEditorImplementation
    let validationIssues: [YAMLValidationIssue]
    let navigationRequest: YAMLTextNavigationRequest?
    let searchQuery: String
    let searchMatchCase: Bool
    let selectedSearchMatchIndex: Int

    var body: some View {
        let activeImplementation = inlineEditing ? implementation : .readOnlyScroll

        InspectorTextSurface(minHeight: 280) {
            Group {
                switch activeImplementation {
                case .readOnlyScroll:
                    InspectorReadOnlyTextView(
                        text: displayText,
                        resetID: readOnlyResetID,
                        contentStyle: .yaml,
                        externalValidationIssues: validationIssues,
                        navigationRequest: navigationRequest,
                        showsLineNumbers: true,
                        searchQuery: searchQuery,
                        searchMatchCase: searchMatchCase,
                        selectedSearchMatchIndex: selectedSearchMatchIndex
                    )
                case .swiftUITextEditor:
                    TextEditor(text: $text)
                        .font(.system(size: 12, weight: .regular, design: .monospaced))
                        .scrollContentBackground(.hidden)
                        .padding(6)
                case .appKitTextView:
                    AppKitManifestTextView(
                        text: $text,
                        isEditable: true,
                        contentStyle: .yaml,
                        externalValidationIssues: validationIssues,
                        navigationRequest: navigationRequest,
                        showsLineNumbers: true,
                        searchQuery: searchQuery,
                        searchMatchCase: searchMatchCase,
                        selectedSearchMatchIndex: selectedSearchMatchIndex
                    )
                }
            }
        }
    }
}

struct ResourceYAMLInspectorPane: View {
    let resourceReference: String
    @Binding var yamlText: String
    let yamlDisplayText: String
    let yamlFooterText: String
    let baseline: String
    let hasUnsavedEdits: Bool
    let canApplyMutations: Bool
    let validationIssues: [YAMLValidationIssue]
    let isValidating: Bool
    let statusText: String
    let canUndoEdit: Bool
    let canReapplySnapshot: Bool
    @Binding var isInlineEditing: Bool
    let inlineEditorImplementation: ManifestInlineEditorImplementation
    let onApply: () -> Void
    let onReapplySnapshot: () -> Void
    let onOpenEditor: () -> Void
    let onUndoEdit: () -> Void
    let onRevert: () -> Void
    let onImport: () -> Void
    let onExport: () -> Void
    let readOnlyResetID: String
    @AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault) private var hidesManagedFields = true
    @AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault
    @State private var issueNavigationRequest: YAMLTextNavigationRequest?
    @State private var issueNavigationSequence = 0
    @State private var isFindPresented = false
    @State private var findQuery = ""
    @State private var findMatchCase = false
    @State private var selectedFindMatchIndex = 0

    var body: some View {
        let filteredYAML = KubernetesManagedFieldsDisplayFilter.removingManagedFields(from: yamlDisplayText)
        let canHideManagedFields = filteredYAML.removedBlockCount > 0 && !isInlineEditing
        let effectiveHidesManagedFields = simpleMode || hidesManagedFields
        let displayedYAML = effectiveHidesManagedFields && canHideManagedFields ? filteredYAML.text : yamlDisplayText
        let presentedIssues = YAMLIssuePresentation.presentedIssues(
            text: yamlText,
            externalIssues: validationIssues
        )
        let surfaceIssues = effectiveHidesManagedFields && canHideManagedFields ? [] : presentedIssues
        let canApplyYAML = canApplyMutations
            && hasUnsavedEdits
            && !yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !presentedIssues.contains(where: { $0.severity == .error })

        ResourceManifestInspectorLayout {
            if isInlineEditing {
                ManifestInlineNote(t(.yamlEditsStayLocal)) {
                    ManifestUnsavedEditsSlot(isVisible: hasUnsavedEdits)
                }
            } else if hasUnsavedEdits {
                ManifestUnsavedEditsChip()
            }
        } toolbar: {
            ManifestToolbarScrollRow {
                ManifestToolbarGroup {
                    Button(t(.applyYAML), action: onApply)
                        .buttonStyle(.borderedProminent)
                        .disabled(!canApplyYAML)
                        .help(hasUnsavedEdits ? "Sends the manifest to the cluster. Closing the editor or this tab does not." : "No local YAML changes to apply.")

                    if inlineEditorImplementation.supportsInlineEditing {
                        Button(isInlineEditing ? t(.done) : t(.quickEdit)) {
                            isInlineEditing.toggle()
                        }
                        .buttonStyle(.bordered)
                        .disabled(yamlText.isEmpty)
                    }

                    Button("\(t(.edit))...", action: onOpenEditor)
                        .buttonStyle(.bordered)
                        .disabled(yamlText.isEmpty)
                }

                ManifestToolbarGroup {
                    Menu {
                        Button("Apply Last Fetched YAML", action: onReapplySnapshot)
                            .disabled(!canReapplySnapshot)
                            .help("Apply the last YAML fetched for this resource again. Rune shows a confirmation and diff before sending it.")

                        Divider()

                        Button("Undo Draft Edit") {
                            onUndoEdit()
                        }
                        .disabled(!canUndoEdit)
                        .help("Restore the previous local YAML draft.")

                        Button("Revert Draft") {
                            onRevert()
                            isInlineEditing = false
                        }
                        .disabled(!hasUnsavedEdits)
                        .help("Discard local YAML edits and return to the current loaded draft.")
                    } label: {
                        Label(t(.draft), systemImage: "clock.arrow.circlepath")
                    }

                    Menu {
                        Button("Import YAML…") {
                            onImport()
                            onOpenEditor()
                        }
                        .help("Replace the editor with the contents of a YAML file.")

                        Button("Export YAML…", action: onExport)
                            .disabled(yamlText.isEmpty)
                            .help("Export the current YAML text to a file.")
                    } label: {
                        Label(t(.file), systemImage: "doc")
                    }

                    ManifestStatusChip(text: statusText, systemImage: "clock")
                }

                if !simpleMode, filteredYAML.removedBlockCount > 0 {
                    ManifestToolbarGroup {
                        ManifestManagedFieldsToggle(
                            hidesManagedFields: $hidesManagedFields,
                            isDisabled: isInlineEditing
                        )
                    }
                }
            }
        } status: {
            YAMLValidationSummaryView(
                issues: presentedIssues,
                isValidating: isValidating,
                onSelectIssue: navigateToIssue
            )
        } surface: {
            FindableInspectorSurface(
                text: isInlineEditing ? yamlText : displayedYAML,
                placeholder: t(.findInYAML),
                query: $findQuery,
                matchCase: $findMatchCase,
                selectedMatchIndex: $selectedFindMatchIndex,
                isFindPresented: $isFindPresented
            ) {
                ResourceYAMLEditorSurface(
                    text: $yamlText,
                    displayText: displayedYAML,
                    readOnlyResetID: readOnlyResetID,
                    inlineEditing: isInlineEditing,
                    implementation: inlineEditorImplementation,
                    validationIssues: surfaceIssues,
                    navigationRequest: issueNavigationRequest,
                    searchQuery: findQuery,
                    searchMatchCase: findMatchCase,
                    selectedSearchMatchIndex: selectedFindMatchIndex
                )
            }
        } footer: {
            if yamlText.isEmpty {
                Text(yamlFooterText)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .onChange(of: baseline) { _, _ in
            isInlineEditing = false
        }
    }

    private var language: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func t(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: language)
    }

    private func navigateToIssue(_ issue: YAMLValidationIssue) {
        if inlineEditorImplementation.supportsInlineEditing {
            isInlineEditing = true
        }
        issueNavigationSequence += 1
        issueNavigationRequest = YAMLTextNavigationRequest(issue: issue, sequence: issueNavigationSequence)
    }
}

struct ManifestUnsavedEditsChip: View {
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault

    var body: some View {
        RuneChip(
            horizontalPadding: 8,
            verticalPadding: 3,
            fill: Color.orange.opacity(0.14),
            cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius
        ) {
            Label(t(.unsavedEdits), systemImage: "circle.fill")
                .font(.caption.weight(.semibold))
                .labelStyle(.titleAndIcon)
                .imageScale(.small)
                .foregroundStyle(.orange)
        }
        .frame(height: RuneUILayoutMetrics.headerChipHeight)
        .help("The YAML draft has local changes that have not been applied to the cluster.")
        .accessibilityLabel("Unsaved YAML edits")
    }

    private var language: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func t(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: language)
    }
}

struct ManifestUnsavedEditsSlot: View {
    let isVisible: Bool

    var body: some View {
        ManifestUnsavedEditsChip()
            .opacity(isVisible ? 1 : 0)
            .accessibilityHidden(!isVisible)
            .allowsHitTesting(isVisible)
    }
}

struct ManifestInlineNote<Accessory: View>: View {
    private let text: String
    @ViewBuilder private let accessory: Accessory

    init(_ text: String, @ViewBuilder accessory: () -> Accessory) {
        self.text = text
        self.accessory = accessory()
    }

    var body: some View {
        HStack(alignment: .center, spacing: 8) {
            Text(text)
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(1)
                .truncationMode(.tail)

            Spacer(minLength: 0)

            accessory
        }
        .frame(minHeight: RuneUILayoutMetrics.headerChipHeight, alignment: .center)
    }
}

struct ManifestToolbarScrollRow<Content: View>: View {
    @ViewBuilder let content: Content

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarGroupSpacing) {
                content
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .controlSize(.small)
    }
}

struct ManifestToolbarGroup<Content: View>: View {
    @ViewBuilder let content: Content

    var body: some View {
        HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarControlSpacing) {
            content
        }
        .padding(.horizontal, RuneUILayoutMetrics.inspectorToolbarGroupHorizontalPadding)
        .padding(.vertical, RuneUILayoutMetrics.inspectorToolbarGroupVerticalPadding)
        .frame(minHeight: RuneUILayoutMetrics.inspectorToolbarGroupMinHeight)
        .background {
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .fill(Color.primary.opacity(0.04))
        }
        .overlay {
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.14), lineWidth: 1)
        }
        .fixedSize(horizontal: true, vertical: false)
    }
}

struct ManifestManagedFieldsToggle: View {
    @Binding var hidesManagedFields: Bool
    let isDisabled: Bool
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault

    var body: some View {
        Toggle(isOn: $hidesManagedFields) {
            Label(t(.hideManaged), systemImage: "eye.slash")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .lineLimit(1)
        }
        .toggleStyle(.checkbox)
        .controlSize(.small)
        .disabled(isDisabled)
        .help(isDisabled ? "Managed fields are shown while editing so line numbers and validation ranges stay exact." : "Hide Kubernetes metadata.managedFields in this read-only view.")
        .padding(.horizontal, 7)
        .padding(.vertical, 3)
        .background {
            RoundedRectangle(cornerRadius: 7, style: .continuous)
                .fill(Color(nsColor: .controlBackgroundColor).opacity(0.62))
        }
        .overlay {
            RoundedRectangle(cornerRadius: 7, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var language: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func t(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: language)
    }
}

struct ManifestStatusChip: View {
    let text: String
    var systemImage = "clock"

    var body: some View {
        Label(text, systemImage: systemImage)
            .font(.caption.weight(.semibold))
            .foregroundStyle(.secondary)
            .lineLimit(1)
            .truncationMode(.middle)
            .monospacedDigit()
            .padding(.horizontal, 9)
            .padding(.vertical, 5)
            .background {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .fill(Color.primary.opacity(0.035))
            }
            .overlay {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .strokeBorder(Color(nsColor: .separatorColor).opacity(0.14), lineWidth: 1)
            }
            .fixedSize(horizontal: false, vertical: true)
    }
}

struct ResourceYAMLEditorSheetView: View {
    let resourceReference: String
    @Binding var yamlText: String
    let yamlFooterText: String
    let canApplyMutations: Bool
    let hasUnsavedEdits: Bool
    let validationIssues: [YAMLValidationIssue]
    let isValidating: Bool
    let canUndoEdit: Bool
    let canReapplySnapshot: Bool
    let onApply: () -> Void
    let onReapplySnapshot: () -> Void
    let onUndoEdit: () -> Void
    let onRevert: () -> Void
    let onImport: () -> Void
    let onExport: () -> Void
    let onClose: () -> Void
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault
    @State private var issueNavigationRequest: YAMLTextNavigationRequest?
    @State private var issueNavigationSequence = 0
    @State private var isFindPresented = false
    @State private var findQuery = ""
    @State private var findMatchCase = false
    @State private var selectedFindMatchIndex = 0

    var body: some View {
        let presentedIssues = YAMLIssuePresentation.presentedIssues(
            text: yamlText,
            externalIssues: validationIssues
        )
        let canApplyYAML = canApplyMutations
            && hasUnsavedEdits
            && !yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !presentedIssues.contains(where: { $0.severity == .error })

        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(t(.yamlManifest))
                        .font(.title2.weight(.bold))
                    Text(resourceReference)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button("Close", action: onClose)
            }

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    Button(t(.applyYAML), action: onApply)
                        .buttonStyle(.borderedProminent)
                        .disabled(!canApplyYAML)
                        .help(hasUnsavedEdits ? "Sends the manifest to the cluster. Closing this sheet does not." : "No local YAML changes to apply.")

                    Menu {
                        Button("Apply Last Fetched YAML", action: onReapplySnapshot)
                            .disabled(!canReapplySnapshot)
                            .help("Apply the last YAML fetched for this resource again. Rune shows a confirmation and diff before sending it.")

                        Divider()

                        Button("Undo Draft Edit", action: onUndoEdit)
                            .disabled(!canUndoEdit)

                        Button("Revert Draft", action: onRevert)
                            .disabled(!hasUnsavedEdits)
                    } label: {
                        Label(t(.draft), systemImage: "clock.arrow.circlepath")
                    }

                    Menu {
                        Button("Import YAML…", action: onImport)

                        Button("Export YAML…", action: onExport)
                            .disabled(yamlText.isEmpty)
                    } label: {
                        Label(t(.file), systemImage: "doc")
                    }

                    Spacer(minLength: 0)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            YAMLValidationSummaryView(
                issues: presentedIssues,
                isValidating: isValidating,
                onSelectIssue: navigateToIssue
            )

            FindableInspectorSurface(
                text: yamlText,
                placeholder: t(.findInYAML),
                query: $findQuery,
                matchCase: $findMatchCase,
                selectedMatchIndex: $selectedFindMatchIndex,
                isFindPresented: $isFindPresented
            ) {
                ResourceYAMLEditorSurface(
                    text: $yamlText,
                    displayText: yamlText,
                    readOnlyResetID: "yaml-sheet:\(resourceReference)",
                    inlineEditing: true,
                    implementation: .appKitTextView,
                    validationIssues: presentedIssues,
                    navigationRequest: issueNavigationRequest,
                    searchQuery: findQuery,
                    searchMatchCase: findMatchCase,
                    selectedSearchMatchIndex: selectedFindMatchIndex
                )
            }

            if yamlText.isEmpty {
                Text(yamlFooterText)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Text("Close dismisses this sheet only. Nothing is sent to the cluster until you tap Apply YAML or Apply on the Describe tab.")
                .font(.footnote)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
        }
        .padding(16)
        .frame(minWidth: 760, minHeight: 560)
        .background(.regularMaterial)
    }

    private func navigateToIssue(_ issue: YAMLValidationIssue) {
        issueNavigationSequence += 1
        issueNavigationRequest = YAMLTextNavigationRequest(issue: issue, sequence: issueNavigationSequence)
    }

    private var language: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func t(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: language)
    }
}

private struct YAMLValidationSummaryView: View {
    let issues: [YAMLValidationIssue]
    let isValidating: Bool
    let onSelectIssue: (YAMLValidationIssue) -> Void
    private let maxVisibleIssues = 6
    @State private var isExpanded = false

    private var visibleIssues: [YAMLValidationIssue] {
        Array(issues.prefix(maxVisibleIssues))
    }

    private var errorCount: Int {
        issues.filter { $0.severity == .error }.count
    }

    private var warningCount: Int {
        issues.filter { $0.severity == .warning }.count
    }

    private var issueSignature: String {
        issues.map(\.id).joined(separator: "|")
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Button {
                if !issues.isEmpty {
                    isExpanded.toggle()
                }
            } label: {
                HStack(spacing: 8) {
                    HStack(spacing: 5) {
                        statusLight(color: .red, isActive: errorCount > 0)
                        statusLight(color: .orange, isActive: warningCount > 0)
                        statusLight(color: .green, isActive: !isValidating && issues.isEmpty)
                    }

                    Text(summaryTitle)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)

                    Spacer(minLength: 0)

                    if isValidating {
                        ProgressView()
                            .controlSize(.small)
                    }

                    if !issues.isEmpty {
                        Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                            .font(.caption2.weight(.semibold))
                            .foregroundStyle(.tertiary)
                    }
                }
            }
            .buttonStyle(.plain)

            if isExpanded {
                VStack(alignment: .leading, spacing: 8) {
                    ForEach(visibleIssues) { issue in
                        Button {
                            onSelectIssue(issue)
                        } label: {
                            HStack(alignment: .top, spacing: 8) {
                                Image(systemName: symbolName(for: issue))
                                    .font(.caption)
                                    .foregroundStyle(color(for: issue))
                                    .frame(width: 14, alignment: .center)

                                VStack(alignment: .leading, spacing: 2) {
                                    Text(issue.message)
                                        .font(.caption)
                                        .foregroundStyle(.primary)
                                    Text(locationText(for: issue))
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }

                                Spacer(minLength: 0)
                            }
                        }
                        .buttonStyle(.plain)
                        .contentShape(Rectangle())
                        .help("Jump to this YAML problem")
                    }

                    if issues.count > maxVisibleIssues {
                        Text("\(issues.count - maxVisibleIssues) more issues not shown")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                }
                .padding(.top, 2)
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .background {
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .fill(Color.primary.opacity(0.035))
        }
        .overlay {
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
        }
        .onChange(of: issueSignature) { _, _ in
            if issues.isEmpty {
                isExpanded = false
            }
        }
    }

    private var summaryTitle: String {
        if isValidating, issues.isEmpty {
            return "Validating YAML against Kubernetes…"
        }

        if errorCount > 0, warningCount > 0 {
            return "\(errorCount) errors, \(warningCount) warnings"
        }
        if errorCount > 0 {
            return errorCount == 1 ? "1 YAML error" : "\(errorCount) YAML errors"
        }
        if warningCount > 0 {
            return warningCount == 1 ? "1 validation warning" : "\(warningCount) validation warnings"
        }
        return "No YAML problems"
    }

    private func symbolName(for issue: YAMLValidationIssue) -> String {
        issue.severity == .error ? "xmark.octagon.fill" : "exclamationmark.triangle.fill"
    }

    private func color(for issue: YAMLValidationIssue) -> Color {
        issue.severity == .error ? .red : .orange
    }

    private func locationText(for issue: YAMLValidationIssue) -> String {
        let source = issue.source.rawValue.capitalized

        switch (issue.line, issue.column) {
        case let (line?, column?):
            return "\(source) • line \(line), column \(column)"
        case let (line?, nil):
            return "\(source) • line \(line)"
        default:
            return source
        }
    }

    private func statusLight(color: Color, isActive: Bool) -> some View {
        Circle()
            .fill(color.opacity(isActive ? 0.95 : 0.2))
            .overlay {
                Circle()
                    .strokeBorder(color.opacity(isActive ? 0.35 : 0.16), lineWidth: 1)
            }
            .frame(width: 9, height: 9)
    }
}

private enum YAMLIssuePresentation {
    static func presentedIssues(text: String, externalIssues: [YAMLValidationIssue]) -> [YAMLValidationIssue] {
        let localIssues = YAMLLanguageService.analyze(text).validationIssues
        let remoteIssues = externalIssues.filter { $0.source != .syntax }
        var seen: Set<String> = []
        return (localIssues + remoteIssues).filter { issue in
            seen.insert(issue.id).inserted
        }
    }
}

struct KubernetesManagedFieldsDisplayFilter {
    let text: String
    let removedBlockCount: Int

    static func removingManagedFields(from source: String) -> KubernetesManagedFieldsDisplayFilter {
        let lines = source.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
        var output: [String] = []
        var removedBlocks = 0
        var skippingIndent: Int?

        for line in lines {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            let indent = line.prefix { $0 == " " }.count

            if let blockIndent = skippingIndent {
                if trimmed.isEmpty {
                    continue
                }
                if indent > blockIndent || (indent == blockIndent && trimmed.hasPrefix("-")) {
                    continue
                }
                skippingIndent = nil
            }

            if trimmed == "managedFields:" || trimmed.hasPrefix("managedFields: ") {
                removedBlocks += 1
                if trimmed == "managedFields:" {
                    skippingIndent = indent
                }
                continue
            }

            output.append(line)
        }

        return KubernetesManagedFieldsDisplayFilter(
            text: output.joined(separator: "\n"),
            removedBlockCount: removedBlocks
        )
    }
}
