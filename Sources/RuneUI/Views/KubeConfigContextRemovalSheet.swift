import RuneSecurity
import SwiftUI

struct KubeConfigContextRemovalSheet: View {
    let preview: KubeConfigContextRemovalPreview
    let isRemoving: Bool
    let onConfirm: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            Divider()

            VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
                HStack(alignment: .top, spacing: 10) {
                    Image(systemName: "checkmark.shield.fill")
                        .font(.system(size: 17, weight: .semibold))
                        .foregroundStyle(.green)
                        .frame(width: 20, height: 20)
                    VStack(alignment: .leading, spacing: 3) {
                        Text("The cluster stays untouched")
                            .font(.subheadline.weight(.semibold))
                        Text("Only local kubeconfig entries are removed. Workloads, cloud resources, and Kubernetes API objects are never changed.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
                .runeInsetCard(padding: 12)

                VStack(alignment: .leading, spacing: 12) {
                    safetyRow(
                        "Local configuration only",
                        detail: "Rune makes no Kubernetes or cloud-provider API calls.",
                        systemImage: "externaldrive"
                    )
                    safetyRow(
                        "Private backup first",
                        detail: "The original kubeconfig is saved with owner-only permissions before any change.",
                        systemImage: "lock.shield"
                    )
                    safetyRow(
                        "Atomic update",
                        detail: "Rune validates references first and restores every file if the update cannot finish safely.",
                        systemImage: "checkmark.shield"
                    )
                }
                .runeInsetCard(padding: 14)

                VStack(alignment: .leading, spacing: 5) {
                    Text(affectedFilesTitle)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Text(preview.affectedSourceDisplayNames.joined(separator: ", "))
                        .font(.system(.caption, design: .monospaced))
                        .textSelection(.enabled)
                        .lineLimit(3)
                }

            }
            .padding(RuneUILayoutMetrics.dialogContentPadding)

            RuneDialogActionBar {
                Button(action: onCancel) {
                    RuneDialogButtonLabel("Cancel")
                }
                .buttonStyle(.bordered)
                .keyboardShortcut(.cancelAction)
                .disabled(isRemoving)

                Button(role: .destructive, action: onConfirm) {
                    HStack(spacing: 7) {
                        if isRemoving {
                            ProgressView()
                                .controlSize(.small)
                        }
                        RuneDialogButtonLabel(isRemoving ? "Removing…" : "Remove Local Config")
                    }
                }
                .buttonStyle(.borderedProminent)
                .keyboardShortcut(.defaultAction)
                .disabled(isRemoving)
                .accessibilityIdentifier("rune.context-removal.confirm")
            }
            .padding(.horizontal, RuneUILayoutMetrics.dialogContentPadding)
            .padding(.bottom, RuneUILayoutMetrics.dialogContentPadding)
        }
        .frame(width: RuneUILayoutMetrics.standardDialogWidth)
        .runePointerCursor()
        .interactiveDismissDisabled(isRemoving)
    }

    private var header: some View {
        HStack(spacing: 12) {
            Image(systemName: "minus.circle.fill")
                .font(.system(size: 24, weight: .semibold))
                .foregroundStyle(.red)
                .frame(width: 34, height: 34)
                .background(Circle().fill(Color.red.opacity(0.12)))

            VStack(alignment: .leading, spacing: 3) {
                Text("Remove Local Context")
                    .font(.title3.weight(.semibold))
                    .accessibilityAddTraits(.isHeader)
                Text(preview.contextName)
                    .font(.system(.subheadline, design: .monospaced))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
            }
            Spacer(minLength: 0)
            RuneDialogCloseButton("Cancel context removal", action: onCancel)
                .disabled(isRemoving)
        }
        .padding(RuneUILayoutMetrics.dialogContentPadding)
    }

    private var affectedFilesTitle: String {
        let count = preview.affectedSourceDisplayNames.count
        return count == 1 ? "Kubeconfig file" : "\(count) kubeconfig files"
    }

    private func safetyRow(_ title: String, detail: String, systemImage: String) -> some View {
        HStack(alignment: .top, spacing: 10) {
            Image(systemName: systemImage)
                .foregroundStyle(.secondary)
                .frame(width: 18, height: 18)
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.caption.weight(.semibold))
                Text(detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }
}
