import RuneSecurity
import SwiftUI

struct KubeConfigContextRemovalSheet: View {
    let preview: KubeConfigContextRemovalPreview
    let isRemoving: Bool
    var errorMessage: String? = nil
    let onConfirm: () -> Void
    let onCancel: () -> Void
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            Divider()

            ScrollViewReader { proxy in
                ScrollView {
                    VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
                        if let errorMessage {
                            Label(errorMessage, systemImage: "exclamationmark.triangle.fill")
                                .runeInterfaceFont()
                                .foregroundStyle(RuneSemanticColorRole.danger.color(in: runeThemePalette))
                                .fixedSize(horizontal: false, vertical: true)
                                .textSelection(.enabled)
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .runeInsetCard(padding: 12)
                                .accessibilityIdentifier("rune.context-removal.error")
                        }
                        HStack(alignment: .top, spacing: 10) {
                            Image(systemName: "checkmark.shield.fill")
                                .font(.system(size: 17, weight: .semibold))
                                .foregroundStyle(RuneSemanticColorRole.success.color(in: runeThemePalette))
                                .frame(width: 20, height: 20)
                            VStack(alignment: .leading, spacing: 3) {
                                Text("The cluster stays untouched")
                                    .runeInterfaceFont(weight: .semibold)
                                Text("Only local kubeconfig entries are removed. Workloads, cloud resources, and Kubernetes API objects are never changed.")
                                    .runeInterfaceFont(relativeSize: -1)
                                    .foregroundStyle(.runeSecondary)
                                    .fixedSize(horizontal: false, vertical: true)
                            }
                        }
                        .runeInsetCard(padding: 12)

                        VStack(alignment: .leading, spacing: 5) {
                            Text(affectedFilesTitle)
                                .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                                .foregroundStyle(.runeSecondary)
                            ForEach(Array(preview.affectedSourceDisplayNames.enumerated()), id: \.offset) { _, name in
                                Text(name)
                                    .runeInterfaceFont(relativeSize: -1, design: .monospaced)
                                    .textSelection(.enabled)
                                    .fixedSize(horizontal: false, vertical: true)
                            }
                        }

                        safetyRow(
                            "Private backup first",
                            detail: "Rune saves a private copy of the original configuration before removing these entries.",
                            systemImage: "lock.shield"
                        )
                    }
                    .padding(RuneUILayoutMetrics.dialogContentPadding)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .id("rune.context-removal.body-top")
                }
                .onChange(of: errorMessage) { _, error in
                    guard error != nil else { return }
                    proxy.scrollTo("rune.context-removal.body-top", anchor: .top)
                }
            }
            .frame(minHeight: 160, idealHeight: 300, maxHeight: 380)

            RuneDialogActionBar {
                Button(action: onCancel) {
                    RuneDialogButtonLabel("Cancel")
                }
                .buttonStyle(RuneToolbarButtonStyle())
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
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                .keyboardShortcut(.defaultAction)
                .disabled(isRemoving)
                .accessibilityIdentifier("rune.context-removal.confirm")
            }
            .padding(.horizontal, RuneUILayoutMetrics.dialogContentPadding)
            .padding(.bottom, RuneUILayoutMetrics.dialogContentPadding)
        }
        .frame(minWidth: RuneUILayoutMetrics.compactDialogWidth, idealWidth: RuneUILayoutMetrics.standardDialogWidth,
               maxWidth: RuneUILayoutMetrics.standardDialogWidth)
        .frame(maxHeight: RuneUILayoutMetrics.providerDialogMaxHeight)
        .runePointerCursor()
        .interactiveDismissDisabled(isRemoving)
    }

    private var header: some View {
        HStack(spacing: 12) {
            Image(systemName: "minus.circle.fill")
                .font(.system(size: 24, weight: .semibold))
                .foregroundStyle(RuneSemanticColorRole.danger.color(in: runeThemePalette))
                .frame(width: 34, height: 34)
                .background(Circle().fill(Color.red.opacity(0.12)))

            VStack(alignment: .leading, spacing: 3) {
                Text("Remove Local Context")
                    .runeInterfaceFont(relativeSize: 2, weight: .semibold)
                    .accessibilityAddTraits(.isHeader)
                Text(preview.contextName)
                    .runeInterfaceFont(relativeSize: -1, design: .monospaced)
                    .foregroundStyle(.runeSecondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .help(preview.contextName)
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
                .foregroundStyle(.runeSecondary)
                .frame(width: 18, height: 18)
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .runeInterfaceFont(relativeSize: -1, weight: .semibold)
                Text(detail)
                    .runeInterfaceFont(relativeSize: -1)
                    .foregroundStyle(.runeSecondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }
}
