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
                VStack(alignment: .leading, spacing: RuneSpacing.sm) {
                    header
                    discoveryStatus
                    providerToolsSection
                    standardSection
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
        .runePointerCursor()
    }

    private var header: some View {
        HStack(spacing: RuneSpacing.xs) {
            Image(systemName: "plus.circle.fill")
                .runeText(.body, weight: .semibold)
                .foregroundStyle(.runeAccent)
                .frame(width: 24, height: 24)
                .background(Circle().fill(Color.accentColor.opacity(0.12)))
            Text("Add Cluster")
                .runeText(.headline)
                .accessibilityAddTraits(.isHeader)
            Spacer(minLength: 0)
        }
    }

    private var standardSection: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.xs) {
            sectionLabel("Kubeconfig files")

            LazyVGrid(columns: gridColumns, spacing: RuneSpacing.xs) {
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
        VStack(alignment: .leading, spacing: RuneSpacing.xs) {
            sectionLabel("Cloud providers & local clusters")

            LazyVGrid(columns: gridColumns, spacing: RuneSpacing.xs) {
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
            VStack(alignment: .leading, spacing: RuneSpacing.inset) {
                AddClusterProviderCredentialField(
                    title: "Context name",
                    isRequired: true,
                    accessibilityIdentifier: "rune.add-cluster.manual-field.context-name"
                ) {
                    TextField("Example: development", text: $manualContextName)
                        .textFieldStyle(RuneControlTextFieldStyle())
                        .runeTextInputCursor()
                }
                AddClusterProviderCredentialField(
                    title: "Server URL",
                    isRequired: true,
                    accessibilityIdentifier: "rune.add-cluster.manual-field.server-url"
                ) {
                    TextField("https://cluster.example.invalid", text: $manualServerURL)
                        .textFieldStyle(RuneControlTextFieldStyle())
                        .runeTextInputCursor()
                }
                AddClusterProviderCredentialField(
                    title: "Namespace",
                    isRequired: false,
                    accessibilityIdentifier: "rune.add-cluster.manual-field.namespace"
                ) {
                    TextField("Optional namespace", text: $manualNamespace)
                        .textFieldStyle(RuneControlTextFieldStyle())
                        .runeTextInputCursor()
                }
                AddClusterProviderCredentialField(
                    title: "Bearer token",
                    isRequired: true,
                    accessibilityIdentifier: "rune.add-cluster.manual-field.bearer-token"
                ) {
                    SecureField("Required token", text: $manualBearerToken)
                        .textFieldStyle(RuneControlTextFieldStyle())
                        .accessibilityLabel("Bearer token")
                        .accessibilityHint("Required field")
                        .accessibilityIdentifier("rune.add-cluster.manual-field.bearer-token")
                        .runeTextInputCursor()
                }

                Button(action: onImportManualToken) {
                    Label("Add Manual Token Cluster", systemImage: "key")
                }
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                .controlSize(.regular)
                .disabled(!canImportManualToken)
            }
            .padding(.top, RuneSpacing.xs)
        } label: {
            Label("Manual Token Server", systemImage: "key")
                .runeText(.microEmphasis)
                .foregroundStyle(.runeSecondary)
        }
        .padding(RuneSpacing.inset)
        .background(RuneSurfaceBackground(kind: .inset))
    }

    private var gridColumns: [GridItem] {
        if dynamicTypeSize.isAccessibilitySize {
            return [GridItem(.flexible(), spacing: RuneSpacing.xs)]
        }
        return [GridItem(.adaptive(minimum: 170), spacing: RuneSpacing.xs)]
    }

    private func sectionLabel(_ text: String) -> some View {
        Text(text)
            .runeText(.microEmphasis)
            .foregroundStyle(.runeSecondary)
    }

    private var canImportManualToken: Bool {
        AddClusterPopoverStatePresentation.canImportManualToken(
            contextName: manualContextName,
            serverURL: manualServerURL,
            bearerToken: manualBearerToken
        )
    }

    private var discoveryStatus: some View {
        HStack(spacing: RuneSpacing.xs) {
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
                .runeText(.micro, weight: .medium)
                .foregroundStyle(.runeSecondary)
                .lineLimit(1)
                .help(discoveryStatusText)

            Spacer(minLength: 0)

            RuneIconButton(
                "Refresh detected contexts",
                systemImage: "arrow.clockwise",
                action: onRefresh
            )
        }
        .padding(.horizontal, RuneSpacing.inset)
        .padding(.vertical, RuneSpacing.tight)
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
        .help("\(title) — \(subtitle)")
    }

    private func actionContent(
        title: String,
        subtitle: String,
        symbolName: String
    ) -> some View {
        HStack(spacing: RuneSpacing.inset) {
            Image(systemName: symbolName)
                .runeText(.body, weight: .semibold)
                .frame(width: 24, height: 24)
                .foregroundStyle(.runeAccent)
                .background(Circle().fill(Color.accentColor.opacity(0.11)))
            VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                Text(title)
                    .runeText(.microEmphasis)
                Text(subtitle)
                    .runeText(.micro)
                    .foregroundStyle(.runeSecondary)
                    .lineLimit(1)
            }
            Spacer(minLength: 0)
        }
        .padding(RuneSpacing.inset)
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
            mode: provider == .local
                ? AddClusterProviderExecutionMode(externalCommandsAllowed: externalCommandsAllowed)
                : .externalCLI
        )
        let accent = provider.accent

        return Button {
            onSelectProvider(provider)
        } label: {
            HStack(alignment: .center, spacing: RuneSpacing.xs) {
                Image(systemName: provider.symbolName)
                    .runeText(.body, weight: .semibold)
                    .foregroundStyle(accent)
                    .frame(width: 22, height: 22)
                    .background(Circle().fill(accent.opacity(0.12)))

                VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                    Text(provider.shortTitle)
                        .runeText(.microEmphasis)
                    Text(presentation.compactSubtitle)
                        .runeText(.micro)
                        .foregroundStyle(.runeSecondary)
                        .lineLimit(1)
                }

                Spacer(minLength: 0)

                Image(systemName: "chevron.right")
                    .runeText(.micro, weight: .bold)
                    .foregroundStyle(.runeTertiary)
            }
            .padding(RuneSpacing.inset)
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
        .help("\(provider.title) — \(presentation.compactSubtitle)")
    }

}
