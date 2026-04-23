import SwiftUI

struct ResourceDescribeInspectorPane: View {
    let describeText: String
    let resourceReference: String
    let canApplyMutations: Bool
    let yamlText: String
    let onApply: () -> Void
    let onOpenYAMLEditor: () -> Void
    let readOnlyResetID: String

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Describe output is read-only. Edit the YAML manifest to change the resource, then Apply.")
                .font(.caption2)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    Button("Apply", action: onApply)
                        .buttonStyle(.borderedProminent)
                        .disabled(!canApplyMutations || yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        .help("Sends the manifest to the cluster. Closing the editor or this tab does not.")

                    Button("YAML manifest…", action: onOpenYAMLEditor)
                        .buttonStyle(.bordered)
                        .disabled(yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        .help("Opens the YAML manifest for this resource—the same buffer as the YAML tab. Use Apply to push changes to the cluster.")
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            DescribeTextSurface(
                text: describeText,
                minHeight: 280,
                resetID: readOnlyResetID
            )

            Text("The pane above is describe output from the cluster. To update the cluster, open YAML manifest (or the YAML tab), edit, then Apply.")
                .font(.footnote)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
        }
    }
}
