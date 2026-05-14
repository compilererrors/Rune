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
    let readOnlyResetID: String
    @State private var hidesManagedFields = true

    var body: some View {
        let canApplyYAML = canApplyMutations
            && hasUnsavedEdits
            && !yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !validationIssues.contains(where: { $0.severity == .error })
        let managedFieldsFilter = DescribeManagedFieldsDisplayFilter.removingManagedFields(from: describeText)
        let presentedDescribeText = hidesManagedFields ? managedFieldsFilter.text : describeText

        ResourceManifestInspectorLayout {
            ManifestInlineNote("Describe output is read-only. Edit YAML, then Apply.") {
                ManifestUnsavedEditsSlot(isVisible: hasUnsavedEdits)
            }
        } toolbar: {
            ManifestToolbarScrollRow {
                if managedFieldsFilter.removedBlockCount > 0 {
                    ManifestToolbarGroup {
                        ManifestManagedFieldsToggle(
                            hidesManagedFields: $hidesManagedFields,
                            isDisabled: false
                        )
                    }
                }

                ManifestToolbarGroup {
                    Button("Apply", action: onApply)
                        .buttonStyle(.borderedProminent)
                        .disabled(!canApplyYAML)
                        .help(hasUnsavedEdits ? "Sends the manifest to the cluster. Closing the editor or this tab does not." : "No local YAML changes to apply.")

                    Button("YAML manifest…", action: onOpenYAMLEditor)
                        .buttonStyle(.bordered)
                        .disabled(yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        .help("Opens the YAML manifest for this resource—the same buffer as the YAML tab. Use Apply to push changes to the cluster.")
                }
            }
        } status: {
            ManifestStatusChip(text: statusText, systemImage: "clock")
        } surface: {
            DescribeTextSurface(
                text: presentedDescribeText,
                minHeight: 280,
                resetID: readOnlyResetID
            )
        } footer: {
            EmptyView()
        }
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
