import SwiftUI
import RuneCore

struct ResourceDescribeInspectorPane: View {
    let describeText: String
    let resourceReference: String
    let canApplyMutations: Bool
    let yamlText: String
    let hasUnsavedEdits: Bool
    let validationIssues: [YAMLValidationIssue]
    let onApply: () -> Void
    let onOpenYAMLEditor: () -> Void
    let readOnlyResetID: String

    var body: some View {
        let canApplyYAML = canApplyMutations
            && hasUnsavedEdits
            && !yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !validationIssues.contains(where: { $0.severity == .error })

        ResourceManifestInspectorLayout {
            Text("Describe output is read-only. Edit the YAML manifest to change the resource, then Apply.")
                .font(.caption2)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
        } toolbar: {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
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
            EmptyView()
        } surface: {
            DescribeTextSurface(
                text: describeText,
                minHeight: 280,
                resetID: readOnlyResetID
            )
        } footer: {
            EmptyView()
        }
    }
}
