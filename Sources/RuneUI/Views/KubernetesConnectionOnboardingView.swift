import SwiftUI

/// The single first-run connection surface. The sidebar keeps its compact Add Cluster
/// affordance, while this view owns the explanatory copy and primary import action.
struct KubernetesConnectionOnboardingView: View {
    @Binding var favoriteImportedContexts: Bool

    let onImportFile: () -> Void
    let onPaste: () -> Void
    let onImportFolder: () -> Void
    let onUseDefault: () -> Void
    let onShowMoreOptions: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack(alignment: .top, spacing: 12) {
                Image(systemName: "point.3.connected.trianglepath.dotted")
                    .font(.system(size: 19, weight: .semibold))
                    .foregroundStyle(.runeAccent)
                    .frame(width: 36, height: 36)
                    .background(Circle().fill(Color.accentColor.opacity(0.13)))
                    .accessibilityHidden(true)

                VStack(alignment: .leading, spacing: 4) {
                    Text("Connect Kubernetes")
                        .font(.title3.weight(.bold))
                        .accessibilityAddTraits(.isHeader)
                    Text("Import a kubeconfig to load contexts, namespaces, and resources. Rune reviews the file before saving anything.")
                        .font(.subheadline)
                        .foregroundStyle(.runeSecondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }

            Toggle("Favorite imported contexts", isOn: $favoriteImportedContexts)
                .toggleStyle(.checkbox)
                .controlSize(.small)
                .font(.caption)

            ViewThatFits(in: .horizontal) {
                HStack(spacing: 10) {
                    importButton
                    connectionMethodsMenu
                    Spacer(minLength: 0)
                }

                VStack(alignment: .leading, spacing: 8) {
                    importButton
                        .frame(maxWidth: .infinity, alignment: .leading)
                    connectionMethodsMenu
                }
            }
        }
        .runePanelCard(padding: 18)
        .frame(maxWidth: 680, alignment: .leading)
        .accessibilityElement(children: .contain)
        .accessibilityIdentifier("rune.onboarding.connect-kubernetes")
    }

    private var importButton: some View {
        Button(action: onImportFile) {
            Label("Import Kubeconfig…", systemImage: "doc.badge.plus")
                .runeMinimumInteractiveTarget()
        }
        .accessibilityIdentifier("rune.onboarding.import-kubeconfig")
        .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
        .keyboardShortcut(.defaultAction)
        .help("Choose one or more kubeconfig files and review them before import")
    }

    private var connectionMethodsMenu: some View {
        Menu {
            Button(action: onPaste) {
                Label("Paste Kubeconfig", systemImage: "doc.on.clipboard")
            }
            .accessibilityIdentifier("rune.onboarding.paste-kubeconfig")
            Button(action: onImportFolder) {
                Label("Import Folder…", systemImage: "folder.badge.plus")
            }
            Button(action: onUseDefault) {
                Label("Use Default Kubeconfig", systemImage: "folder.badge.gearshape")
            }
            Divider()
            Button(action: onShowMoreOptions) {
                Label("Provider or Manual Setup…", systemImage: "ellipsis.circle")
            }
        } label: {
            Label("Other Connection Methods", systemImage: "chevron.down")
                .runeMinimumInteractiveTarget()
        }
        .accessibilityIdentifier("rune.onboarding.connection-methods")
        .menuStyle(.borderlessButton)
        .fixedSize(horizontal: true, vertical: false)
        .help("Paste, import a folder, use the default kubeconfig, or configure a provider")
    }
}
