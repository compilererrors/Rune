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
    var searchIndex: InspectorFindIndex?
    var searchNavigationRevision = 0
    var editorRestorationRequest: ResourceYAMLEditorRestorationRequest?
    var onEditorEdit: ((String, ResourceYAMLEditorPresentation?) -> Void)?
    var onUndoCommand: (() -> Void)?

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
                        largeTextShowsLineNumbers: true,
                        showsLineNumbers: true,
                        searchQuery: searchQuery,
                        searchMatchCase: searchMatchCase,
                        selectedSearchMatchIndex: selectedSearchMatchIndex,
                        searchIndex: searchIndex,
                        searchMatchRanges: searchIndex?.ranges ?? [],
                        searchNavigationRevision: searchNavigationRevision
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
                        selectedSearchMatchIndex: selectedSearchMatchIndex,
                        searchMatchRanges: searchIndex?.ranges ?? [],
                        searchNavigationRevision: searchNavigationRevision,
                        editorRestorationRequest: editorRestorationRequest,
                        onEditorEdit: onEditorEdit,
                        onUndoCommand: onUndoCommand
                    )
                }
            }
        }
    }
}

struct ResourceYAMLInspectorPane: View {
    let resourceReference: String
    let documentIdentity: ResourceDetailScope?
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
    let onExportToExportFolder: () -> Void
    let onExportAndOpen: () -> Void
    let readOnlyResetID: String
    var documentState: ManifestDocumentState? = nil
    var editorRestorationRequest: ResourceYAMLEditorRestorationRequest?
    var onEditorEdit: ((String, ResourceYAMLEditorPresentation?) -> Void)?
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
        let documentText = yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            ? ""
            : yamlDisplayText
        let resolvedDocumentState = documentState ?? (
            documentText.isEmpty
                ? .empty(title: "No YAML available", message: yamlFooterText)
                : .ready
        )
        let filteredYAML = KubernetesManagedFieldsDisplayFilter.removingManagedFields(from: documentText)
        let canHideManagedFields = filteredYAML.removedBlockCount > 0 && !isInlineEditing
        let effectiveHidesManagedFields = simpleMode || hidesManagedFields
        let displayedYAML = effectiveHidesManagedFields && canHideManagedFields ? filteredYAML.text : documentText
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
            ManifestActionToolbar(
                applyTitle: t(.applyYAML),
                canApply: canApplyYAML,
                applyHelp: hasUnsavedEdits
                    ? "Sends the manifest to the cluster. Closing the editor or this tab does not."
                    : "No local YAML changes to apply.",
                statusText: statusText,
                onApply: onApply
            ) {
                if isInlineEditing {
                    ManifestEditorUndoButton(
                        canUndo: canUndoEdit,
                        onUndo: onUndoEdit
                    )
                }

                if inlineEditorImplementation.supportsInlineEditing {
                    Button(isInlineEditing ? t(.done) : t(.quickEdit)) {
                        isInlineEditing.toggle()
                    }
                    .buttonStyle(RuneToolbarButtonStyle())
                    .disabled(yamlText.isEmpty)
                }

                Button("\(t(.edit))...", action: onOpenEditor)
                    .buttonStyle(RuneToolbarButtonStyle())
                    .disabled(yamlText.isEmpty)
            } secondaryActions: {
                ManifestYAMLActionMenus(
                    draftTitle: t(.draft),
                    fileTitle: t(.file),
                    yamlTextIsEmpty: yamlText.isEmpty,
                    hasUnsavedEdits: hasUnsavedEdits,
                    canReapplySnapshot: canReapplySnapshot,
                    onReapplySnapshot: onReapplySnapshot,
                    onRevert: {
                        onRevert()
                        isInlineEditing = false
                    },
                    onImport: {
                        onImport()
                        onOpenEditor()
                    },
                    onExport: onExport,
                    onExportToExportFolder: onExportToExportFolder,
                    onExportAndOpen: onExportAndOpen
                )

                if !simpleMode, filteredYAML.removedBlockCount > 0 {
                    ManifestManagedFieldsToggle(
                        hidesManagedFields: $hidesManagedFields,
                        isDisabled: isInlineEditing
                    )
                }
            }
        } status: {
            VStack(alignment: .leading, spacing: 8) {
                YAMLValidationSummaryView(
                    issues: presentedIssues,
                    isValidating: isValidating,
                    onSelectIssue: navigateToIssue
                )
                if let staleContentState = resolvedDocumentState.staleContentState {
                    RuneContentStateView(staleContentState, variant: .inline)
                }
            }
        } surface: {
            FindableInspectorSurface(
                text: isInlineEditing ? yamlText : displayedYAML,
                placeholder: t(.findInYAML),
                query: $findQuery,
                matchCase: $findMatchCase,
                selectedMatchIndex: $selectedFindMatchIndex,
                isFindPresented: $isFindPresented
            ) { searchIndex, searchNavigationRevision in
                ManifestDocumentSurface(state: resolvedDocumentState) {
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
                        selectedSearchMatchIndex: selectedFindMatchIndex,
                        searchIndex: searchIndex,
                        searchNavigationRevision: searchNavigationRevision,
                        editorRestorationRequest: editorRestorationRequest,
                        onEditorEdit: onEditorEdit,
                        onUndoCommand: onUndoEdit
                    )
                }
            }
        } footer: {
            EmptyView()
        }
        .onChange(of: documentIdentity) { _, _ in
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
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        let warning = RuneSemanticColorRole.warning.color(in: runeThemePalette)
        RuneHeaderCapsule(
            t(.unsavedEdits),
            role: .status,
            indicatorColor: warning,
            foregroundColor: warning,
            fill: warning.opacity(0.14),
            helpText: "The YAML draft has local changes that have not been applied to the cluster.",
            accessibilityLabel: "Unsaved YAML edits"
        )
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
                .foregroundStyle(.runeSecondary)
                .lineLimit(1)
                .truncationMode(.tail)
                .help(text)

            Spacer(minLength: 0)

            accessory
        }
        .frame(minHeight: RuneUILayoutMetrics.headerChipHeight, alignment: .center)
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
                .lineLimit(1)
        }
        .toggleStyle(RuneToolbarToggleStyle())
        .disabled(isDisabled)
        .help(isDisabled ? "Managed fields are shown while editing so line numbers and validation ranges stay exact." : "Hide Kubernetes metadata.managedFields in this read-only view.")
        .fixedSize(horizontal: true, vertical: false)
    }

    private var language: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func t(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: language)
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
    let canDryRun: Bool
    let isRunningDryRun: Bool
    let dryRunStatus: String?
    let onApply: () -> Void
    let onDryRun: () -> Void
    let onReapplySnapshot: () -> Void
    let onUndoEdit: () -> Void
    let onRevert: () -> Void
    let onImport: () -> Void
    let onExport: () -> Void
    let onExportToExportFolder: () -> Void
    let onExportAndOpen: () -> Void
    let onClose: () -> Void
    var notice: RuneUserNotice? = nil
    var onDismissNotice: () -> Void = {}
    var onOpenSavedFolder: ((URL) -> Void)? = nil
    var onOpenSavedFile: ((URL) -> Void)? = nil
    var documentState: ManifestDocumentState? = nil
    var editorRestorationRequest: ResourceYAMLEditorRestorationRequest?
    var onEditorEdit: ((String, ResourceYAMLEditorPresentation?) -> Void)?
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
        let resolvedDocumentState = documentState ?? (
            yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? .empty(title: "No YAML available", message: yamlFooterText)
                : .ready
        )

        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(t(.yamlManifest))
                        .font(.title2.weight(.bold))
                        .accessibilityAddTraits(.isHeader)
                    Text(resourceReference)
                        .font(.subheadline)
                        .foregroundStyle(.runeSecondary)
                }
                Spacer()
                Button(action: onClose) {
                    RuneDialogButtonLabel("Close")
                }
                .buttonStyle(RuneToolbarButtonStyle())
                .controlSize(.regular)
                .keyboardShortcut(.cancelAction)
            }

            if let notice {
                RuneNoticeBanner(
                    notice: notice,
                    onOpenFolder: onOpenSavedFolder,
                    onOpenFile: onOpenSavedFile,
                    onDismiss: onDismissNotice
                )
                    .accessibilityIdentifier("rune.yaml-editor.notice")
            }

            ManifestActionToolbar(
                applyTitle: t(.applyYAML),
                canApply: canApplyYAML,
                applyHelp: hasUnsavedEdits
                    ? "Sends the manifest to the cluster. Closing this sheet does not."
                    : "No local YAML changes to apply.",
                statusText: dryRunStatus,
                onApply: onApply
            ) {
                Button(action: onDryRun) {
                    Label("Dry Run", systemImage: "checkmark.shield")
                }
                .buttonStyle(RuneToolbarButtonStyle())
                .disabled(!canDryRun || isRunningDryRun)
                .help(
                    isRunningDryRun
                        ? "Kubernetes is checking this draft."
                        : "Checks this draft with Kubernetes. Nothing is applied."
                )
                .accessibilityIdentifier("resource-yaml-server-dry-run")

                ManifestEditorUndoButton(
                    canUndo: canUndoEdit,
                    onUndo: onUndoEdit
                )
            } secondaryActions: {
                ManifestYAMLActionMenus(
                    draftTitle: t(.draft),
                    fileTitle: t(.file),
                    yamlTextIsEmpty: yamlText.isEmpty,
                    hasUnsavedEdits: hasUnsavedEdits,
                    canReapplySnapshot: canReapplySnapshot,
                    onReapplySnapshot: onReapplySnapshot,
                    onRevert: onRevert,
                    onImport: onImport,
                    onExport: onExport,
                    onExportToExportFolder: onExportToExportFolder,
                    onExportAndOpen: onExportAndOpen
                )
            }

            YAMLValidationSummaryView(
                issues: presentedIssues,
                isValidating: isValidating,
                expandedListMaxHeight: RuneUILayoutMetrics.yamlSheetValidationListMaxHeight,
                onSelectIssue: navigateToIssue
            )

            FindableInspectorSurface(
                text: yamlText,
                placeholder: t(.findInYAML),
                query: $findQuery,
                matchCase: $findMatchCase,
                selectedMatchIndex: $selectedFindMatchIndex,
                isFindPresented: $isFindPresented
            ) { searchIndex, searchNavigationRevision in
                ManifestDocumentSurface(state: resolvedDocumentState) {
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
                        selectedSearchMatchIndex: selectedFindMatchIndex,
                        searchIndex: searchIndex,
                        searchNavigationRevision: searchNavigationRevision,
                        editorRestorationRequest: editorRestorationRequest,
                        onEditorEdit: onEditorEdit,
                        onUndoCommand: onUndoEdit
                    )
                }
            }

            Text("Dry Run sends this draft to Kubernetes for validation but never applies it. Apply YAML can write only after confirmation.")
                .font(.footnote)
                .foregroundStyle(.runeSecondary)
                .fixedSize(horizontal: false, vertical: true)
        }
        .padding(RuneUILayoutMetrics.dialogContentPadding)
        .frame(
            width: RuneUILayoutMetrics.wideDialogWidth,
            height: RuneUILayoutMetrics.wideDialogHeight
        )
        .background(.regularMaterial)
        .runePointerCursor()
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

struct YAMLValidationSummaryView: View {
    let issues: [YAMLValidationIssue]
    let isValidating: Bool
    let expandedListMaxHeight: CGFloat?
    let onSelectIssue: (YAMLValidationIssue) -> Void
    private let maxVisibleIssues = 6
    private let inlineExpandedIssueLimit = 3
    @State private var isExpanded = false

    init(
        issues: [YAMLValidationIssue],
        isValidating: Bool,
        expandedListMaxHeight: CGFloat? = nil,
        initiallyExpanded: Bool = false,
        onSelectIssue: @escaping (YAMLValidationIssue) -> Void
    ) {
        self.issues = issues
        self.isValidating = isValidating
        self.expandedListMaxHeight = expandedListMaxHeight
        self.onSelectIssue = onSelectIssue
        _isExpanded = State(initialValue: initiallyExpanded)
    }

    private var visibleIssues: [YAMLValidationIssue] {
        expandedListMaxHeight == nil ? Array(issues.prefix(maxVisibleIssues)) : issues
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
            if issues.isEmpty {
                validationSummaryHeader
                    .frame(
                        maxWidth: .infinity,
                        minHeight: RuneDisclosureMetrics.headerMinimumHeight,
                        alignment: .leading
                    )
                    .accessibilityElement(children: .combine)
                    .accessibilityLabel(summaryTitle)
            } else {
                RuneDisclosureRow(
                    summaryTitle,
                    isExpanded: isExpanded,
                    help: isExpanded ? "Collapse validation issues" : "Expand validation issues",
                    action: {
                        isExpanded.toggle()
                    }
                ) {
                    validationSummaryHeader
                }
            }

            if isExpanded {
                expandedIssues
            }
        }
        .padding(.horizontal, RuneUILayoutMetrics.inspectorControlContentInset)
        .padding(.vertical, RuneUILayoutMetrics.inspectorControlChromeVerticalPadding)
        .background {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .fill(Color.primary.opacity(0.035))
        }
        .overlay {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
        }
        .onChange(of: issueSignature) { _, _ in
            if issues.isEmpty {
                isExpanded = false
            }
        }
    }

    private var validationSummaryHeader: some View {
        HStack(spacing: 8) {
            HStack(spacing: 5) {
                statusLight(color: .red, isActive: errorCount > 0)
                statusLight(color: .orange, isActive: warningCount > 0)
                statusLight(color: .green, isActive: !isValidating && issues.isEmpty)
            }

            Text(summaryTitle)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.runeSecondary)

            Spacer(minLength: 0)

            if isValidating {
                ProgressView()
                    .controlSize(.small)
            }
        }
    }

    @ViewBuilder
    private var expandedIssues: some View {
        if let expandedListMaxHeight, issues.count > inlineExpandedIssueLimit {
            ScrollView(.vertical, showsIndicators: true) {
                issueList
            }
            .frame(maxHeight: expandedListMaxHeight)
        } else {
            issueList
        }
    }

    private var issueList: some View {
        LazyVStack(alignment: .leading, spacing: 8) {
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
                                .foregroundStyle(.runePrimary)
                            Text(locationText(for: issue))
                                .font(.caption2)
                                .foregroundStyle(.runeSecondary)
                        }

                        Spacer(minLength: 0)
                    }
                }
                .buttonStyle(.plain)
                .contentShape(Rectangle())
                .help("Jump to this YAML problem")
            }

            if expandedListMaxHeight == nil, issues.count > maxVisibleIssues {
                Text("\(issues.count - maxVisibleIssues) more issues not shown")
                    .font(.caption2)
                    .foregroundStyle(.runeSecondary)
            }
        }
        .padding(.top, 2)
    }

    private var summaryTitle: String {
        Self.summaryTitle(
            errorCount: errorCount,
            warningCount: warningCount,
            isValidating: isValidating
        )
    }

    static func summaryTitle(
        errorCount: Int,
        warningCount: Int,
        isValidating: Bool
    ) -> String {
        if isValidating, errorCount == 0, warningCount == 0 {
            return "Checking local YAML…"
        }

        if errorCount > 0, warningCount > 0 {
            let errors = errorCount == 1 ? "error" : "errors"
            let warnings = warningCount == 1 ? "warning" : "warnings"
            return "\(errorCount) \(errors), \(warningCount) \(warnings)"
        }
        if errorCount > 0 {
            return errorCount == 1 ? "1 YAML error" : "\(errorCount) YAML errors"
        }
        if warningCount > 0 {
            return warningCount == 1 ? "1 validation warning" : "\(warningCount) validation warnings"
        }
        return "No local YAML problems"
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
