import SwiftUI
import RuneCore

struct ResourceDescribeInspectorPane: View {
    let describeText: String
    let resourceReference: String
    let canApplyMutations: Bool
    let yamlText: String
    let hasUnsavedEdits: Bool
    let validationIssues: [YAMLValidationIssue]
    let statusText: String
    let onApply: () -> Void
    let onOpenYAMLEditor: () -> Void
    let onExport: () -> Void
    let readOnlyResetID: String
    @AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault) private var hidesManagedFields = true
    @AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault
    @State private var isFindPresented = false
    @State private var findQuery = ""
    @State private var findMatchCase = false
    @State private var selectedFindMatchIndex = 0

    var body: some View {
        let canApplyYAML = canApplyMutations
            && hasUnsavedEdits
            && !yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !validationIssues.contains(where: { $0.severity == .error })
        let managedFieldsFilter = DescribeManagedFieldsDisplayFilter.removingManagedFields(from: describeText)
        let effectiveHidesManagedFields = simpleMode || hidesManagedFields
        let presentedDescribeText = effectiveHidesManagedFields ? managedFieldsFilter.text : describeText

        ResourceManifestInspectorLayout {
            ManifestInlineNote(t(.describeReadOnlyNote)) {
                ManifestUnsavedEditsSlot(isVisible: hasUnsavedEdits)
            }
        } toolbar: {
            ManifestToolbarScrollRow {
                if !simpleMode {
                    ManifestToolbarGroup {
                        ManifestManagedFieldsToggle(
                            hidesManagedFields: $hidesManagedFields,
                            isDisabled: managedFieldsFilter.removedBlockCount == 0
                        )
                    }
                }

                ManifestToolbarGroup {
                    ManifestStatusChip(text: statusText, systemImage: "clock")

                    Button(t(.apply), action: onApply)
                        .buttonStyle(.borderedProminent)
                        .disabled(!canApplyYAML)
                        .help(hasUnsavedEdits ? "Sends the manifest to the cluster. Closing the editor or this tab does not." : "No local YAML changes to apply.")

                    Button("\(t(.yamlManifest))...", action: onOpenYAMLEditor)
                        .buttonStyle(.bordered)
                        .disabled(yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        .help("Opens the YAML manifest for this resource—the same buffer as the YAML tab. Use Apply to push changes to the cluster.")

                    Button("\(t(.exportDescribe))...", action: onExport)
                        .buttonStyle(.bordered)
                        .disabled(describeText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        .help("Export the current describe output to a file.")
                }
            }
        } status: {
            EmptyView()
        } surface: {
            FindableInspectorSurface(
                text: presentedDescribeText,
                placeholder: t(.findInDescribe),
                query: $findQuery,
                matchCase: $findMatchCase,
                selectedMatchIndex: $selectedFindMatchIndex,
                isFindPresented: $isFindPresented
            ) {
                DescribeTextSurface(
                    text: presentedDescribeText,
                    minHeight: 280,
                    resetID: readOnlyResetID,
                    searchQuery: findQuery,
                    searchMatchCase: findMatchCase,
                    selectedSearchMatchIndex: selectedFindMatchIndex
                )
            }
        } footer: {
            EmptyView()
        }
    }

    private var language: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func t(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: language)
    }
}

struct DescribeManagedFieldsDisplayFilter {
    let text: String
    let removedBlockCount: Int

    static func removingManagedFields(from source: String) -> DescribeManagedFieldsDisplayFilter {
        let lines = source.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
        var output: [String] = []
        var removedBlocks = 0
        var isSkippingManagedFields = false

        for line in lines {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            let indent = line.prefix { $0 == " " || $0 == "\t" }.count

            if isSkippingManagedFields {
                if trimmed.isEmpty {
                    continue
                }
                if indent > 0 {
                    continue
                }
                isSkippingManagedFields = false
            }

            if trimmed.caseInsensitiveCompare("Managed Fields:") == .orderedSame {
                removedBlocks += 1
                isSkippingManagedFields = true
                continue
            }

            output.append(line)
        }

        return DescribeManagedFieldsDisplayFilter(
            text: output.joined(separator: "\n"),
            removedBlockCount: removedBlocks
        )
    }
}
