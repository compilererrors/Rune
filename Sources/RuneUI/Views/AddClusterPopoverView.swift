import AppKit
import SwiftUI

extension AddClusterProviderIdentifier {
    var accent: Color {
        switch self {
        case .aks: return .blue
        case .eks: return .orange
        case .gke: return .indigo
        case .local: return .green
        }
    }
}

enum AddClusterPopoverStatePresentation {
    static func canImportManualToken(
        contextName: String,
        serverURL: String,
        bearerToken: String
    ) -> Bool {
        hasValue(contextName) && hasValue(serverURL) && hasValue(bearerToken)
    }

    static func discoveryStatusText(
        kubeConfigSourceCount: Int,
        contextCount: Int,
        isLoading: Bool
    ) -> String {
        if isLoading {
            return "Reading cluster contexts..."
        }
        if contextCount > 0 {
            return "\(contextCount) context\(contextCount == 1 ? "" : "s") available from \(kubeConfigSourceCount) source\(kubeConfigSourceCount == 1 ? "" : "s")"
        }
        if kubeConfigSourceCount > 0 {
            return "\(kubeConfigSourceCount) kubeconfig source\(kubeConfigSourceCount == 1 ? "" : "s") loaded"
        }
        return "Watching for kubeconfig sources"
    }

    private static func hasValue(_ value: String) -> Bool {
        !value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }
}

/// Presentation-only Add Cluster surface. RuneRootView remains responsible for
/// navigation, sheet/popover state, and all kubeconfig side effects.
struct AddClusterPopoverView: View {
    let kubeConfigSourceCount: Int
    let contextCount: Int
    let isLoading: Bool
    let externalCommandsAllowed: Bool

    @Binding var favoriteImportedContexts: Bool
    @Binding var isManualTokenExpanded: Bool
    @Binding var manualContextName: String
    @Binding var manualServerURL: String
    @Binding var manualNamespace: String
    @Binding var manualBearerToken: String

    let onRefresh: () -> Void
    let onImportFile: () -> Void
    let onPasteKubeconfig: () -> Void
    let onImportFolder: () -> Void
    let onUseDefaultKubeconfig: () -> Void
    let onSelectProvider: (AddClusterProviderIdentifier) -> Void
    let onImportManualToken: () -> Void
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeThemePalette) private var runeThemePalette

    var body: some View {
        ZStack {
            RuneGlassPaneSurface(role: .content)

            ScrollView {
                VStack(alignment: .leading, spacing: 13) {
                    header
                    discoveryStatus
                    standardSection
                    providerToolsSection
                    manualTokenSection
                }
                .padding(RuneUILayoutMetrics.addClusterPopoverPadding)
            }
        }
        .frame(width: RuneUILayoutMetrics.addClusterPopoverWidth)
        .frame(maxHeight: RuneUILayoutMetrics.addClusterPopoverMaxHeight)
        .animation(.snappy(duration: 0.18), value: isManualTokenExpanded)
        .clipShape(
            RoundedRectangle(
                cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius,
                style: .continuous
            )
        )
        .overlay {
            RoundedRectangle(
                cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius,
                style: .continuous
            )
            .strokeBorder(Color(nsColor: .separatorColor).opacity(0.24), lineWidth: 1)
        }
        .onAppear(perform: onRefresh)
    }

    private var header: some View {
        HStack(spacing: 9) {
            Image(systemName: "plus.circle.fill")
                .font(.body.weight(.semibold))
                .foregroundStyle(Color.accentColor)
                .frame(width: 24, height: 24)
                .background(Circle().fill(Color.accentColor.opacity(0.12)))
            Text("Add Cluster")
                .font(.headline)
                .accessibilityAddTraits(.isHeader)
            Spacer(minLength: 0)
        }
    }

    private var standardSection: some View {
        VStack(alignment: .leading, spacing: 8) {
            sectionLabel("Standard")

            LazyVGrid(columns: gridColumns, spacing: 8) {
                quickAction(
                    title: "Import File",
                    subtitle: "Choose kubeconfig",
                    symbolName: "doc.badge.plus",
                    action: onImportFile
                )

                quickAction(
                    title: "Paste Kubeconfig",
                    subtitle: "From clipboard",
                    symbolName: "doc.on.clipboard",
                    action: onPasteKubeconfig
                )

                quickAction(
                    title: "Import Folder",
                    subtitle: "Direct YAML files",
                    symbolName: "folder.badge.plus",
                    action: onImportFolder
                )

                quickAction(
                    title: "Use Default",
                    subtitle: "~/.kube/config",
                    symbolName: "folder.badge.gearshape",
                    action: onUseDefaultKubeconfig
                )
            }

            Toggle("Favorite imported contexts", isOn: $favoriteImportedContexts)
                .toggleStyle(.checkbox)
                .controlSize(.small)
        }
    }

    private var providerToolsSection: some View {
        VStack(alignment: .leading, spacing: 8) {
            sectionLabel("Providers & local tools")

            LazyVGrid(columns: gridColumns, spacing: 8) {
                ForEach(AddClusterProviderIdentifier.allCases) { provider in
                    providerTile(provider)
                }
            }
        }
    }

    private var manualTokenSection: some View {
        RuneDisclosureSection(
            "Manual Token Server",
            isExpanded: $isManualTokenExpanded
        ) {
            VStack(alignment: .leading, spacing: 10) {
                AddClusterProviderCredentialField(
                    title: "Context name",
                    isRequired: true,
                    accessibilityIdentifier: "rune.add-cluster.manual-field.context-name"
                ) {
                    TextField("Example: development", text: $manualContextName)
                        .textFieldStyle(.roundedBorder)
                }
                AddClusterProviderCredentialField(
                    title: "Server URL",
                    isRequired: true,
                    accessibilityIdentifier: "rune.add-cluster.manual-field.server-url"
                ) {
                    TextField("https://cluster.example.invalid", text: $manualServerURL)
                        .textFieldStyle(.roundedBorder)
                }
                AddClusterProviderCredentialField(
                    title: "Namespace",
                    isRequired: false,
                    accessibilityIdentifier: "rune.add-cluster.manual-field.namespace"
                ) {
                    TextField("Optional namespace", text: $manualNamespace)
                        .textFieldStyle(.roundedBorder)
                }
                AddClusterProviderCredentialField(
                    title: "Bearer token",
                    isRequired: true,
                    accessibilityIdentifier: "rune.add-cluster.manual-field.bearer-token"
                ) {
                    SecureField("Required token", text: $manualBearerToken)
                        .textFieldStyle(.roundedBorder)
                        .accessibilityLabel("Bearer token")
                        .accessibilityHint("Required field")
                        .accessibilityIdentifier("rune.add-cluster.manual-field.bearer-token")
                }

                Button(action: onImportManualToken) {
                    Label("Add Manual Token Cluster", systemImage: "key")
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.regular)
                .disabled(!canImportManualToken)
            }
            .padding(.top, 8)
        } label: {
            Label("Manual Token Server", systemImage: "key")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
        }
        .padding(10)
        .background(RuneSurfaceBackground(kind: .inset))
    }

    private var gridColumns: [GridItem] {
        if dynamicTypeSize.isAccessibilitySize {
            return [GridItem(.flexible(), spacing: 8)]
        }
        return [GridItem(.adaptive(minimum: 170), spacing: 8)]
    }

    private func sectionLabel(_ text: String) -> some View {
        Text(text)
            .font(.caption.weight(.semibold))
            .foregroundStyle(.secondary)
    }

    private var canImportManualToken: Bool {
        AddClusterPopoverStatePresentation.canImportManualToken(
            contextName: manualContextName,
            serverURL: manualServerURL,
            bearerToken: manualBearerToken
        )
    }

    private var discoveryStatus: some View {
        HStack(spacing: 8) {
            if isLoading {
                ProgressView()
                    .controlSize(.small)
            } else {
                Image(systemName: hasDiscoveredConfiguration ? "checkmark.circle.fill" : "dot.radiowaves.left.and.right")
                    .foregroundStyle(
                        hasDiscoveredConfiguration
                            ? RuneSemanticColorRole.success.color(in: runeThemePalette)
                            : (runeThemePalette?.accent ?? Color.accentColor)
                    )
            }

            Text(discoveryStatusText)
                .font(.caption.weight(.medium))
                .foregroundStyle(.secondary)
                .lineLimit(1)

            Spacer(minLength: 0)

            RuneIconButton(
                "Refresh detected contexts",
                systemImage: "arrow.clockwise",
                action: onRefresh
            )
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 7)
        .background(RuneSurfaceBackground(kind: .inset))
        .overlay {
            RoundedRectangle(
                cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius,
                style: .continuous
            )
            .strokeBorder(Color(nsColor: .separatorColor).opacity(0.14), lineWidth: 1)
        }
    }

    private var hasDiscoveredConfiguration: Bool {
        contextCount > 0 || kubeConfigSourceCount > 0
    }

    private var discoveryStatusText: String {
        AddClusterPopoverStatePresentation.discoveryStatusText(
            kubeConfigSourceCount: kubeConfigSourceCount,
            contextCount: contextCount,
            isLoading: isLoading
        )
    }

    private func quickAction(
        title: String,
        subtitle: String,
        symbolName: String,
        action: @escaping () -> Void
    ) -> some View {
        Button(action: action) {
            actionContent(
                title: title,
                subtitle: subtitle,
                symbolName: symbolName
            )
        }
        .buttonStyle(.plain)
    }

    private func actionContent(
        title: String,
        subtitle: String,
        symbolName: String
    ) -> some View {
        HStack(spacing: 10) {
            Image(systemName: symbolName)
                .font(.body.weight(.semibold))
                .frame(width: 24, height: 24)
                .foregroundStyle(Color.accentColor)
                .background(Circle().fill(Color.accentColor.opacity(0.11)))
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.subheadline.weight(.semibold))
                Text(subtitle)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer(minLength: 0)
        }
        .padding(10)
        .frame(
            maxWidth: .infinity,
            minHeight: RuneUILayoutMetrics.addClusterActionCardMinHeight,
            alignment: .leading
        )
        .background(RuneSurfaceBackground(kind: .inset))
        .overlay {
            RoundedRectangle(
                cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius,
                style: .continuous
            )
            .strokeBorder(Color(nsColor: .separatorColor).opacity(0.14), lineWidth: 1)
        }
    }

    private func providerTile(_ provider: AddClusterProviderIdentifier) -> some View {
        let presentation = AddClusterProviderPresentation.resolve(
            provider: provider,
            externalCommandsAllowed: externalCommandsAllowed
        )
        let accent = provider.accent

        return Button {
            onSelectProvider(provider)
        } label: {
            HStack(alignment: .center, spacing: 9) {
                Image(systemName: provider.symbolName)
                    .font(.body.weight(.semibold))
                    .foregroundStyle(accent)
                    .frame(width: 22, height: 22)
                    .background(Circle().fill(accent.opacity(0.12)))

                VStack(alignment: .leading, spacing: 2) {
                    Text(provider.shortTitle)
                        .font(.subheadline.weight(.semibold))
                    Text(presentation.subtitle)
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }

                Spacer(minLength: 0)

                Image(systemName: "chevron.right")
                    .font(.caption2.weight(.bold))
                    .foregroundStyle(.tertiary)
            }
            .padding(10)
            .frame(
                maxWidth: .infinity,
                minHeight: RuneUILayoutMetrics.addClusterActionCardMinHeight,
                alignment: .leading
            )
            .background {
                ZStack {
                    RuneSurfaceBackground(kind: .inset)
                    RoundedRectangle(
                        cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius,
                        style: .continuous
                    )
                    .fill(accent.opacity(0.045))
                }
            }
            .overlay {
                RoundedRectangle(
                    cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius,
                    style: .continuous
                )
                .strokeBorder(accent.opacity(0.20), lineWidth: 1)
            }
        }
        .buttonStyle(.plain)
        .help(provider.title)
    }

}
