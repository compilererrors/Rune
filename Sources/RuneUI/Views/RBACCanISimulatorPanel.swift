import SwiftUI

enum RBACCanILayoutMetrics {
    static let panelHorizontalPadding: CGFloat = 12
    static let compactPanelWidth: CGFloat = 264

    static var compactContentWidth: CGFloat {
        compactPanelWidth - panelHorizontalPadding * 2
    }
}

enum RBACCanILayoutRegion: Hashable, Sendable {
    case header
    case request
    case optionalFields
    case result
}

struct RBACCanILayoutSnapshot: Equatable, Sendable {
    let frames: [RBACCanILayoutRegion: CGRect]

    subscript(_ region: RBACCanILayoutRegion) -> CGRect? {
        frames[region]
    }
}

struct RBACCanISimulatorPanel: View {
    @ObservedObject var viewModel: RuneAppViewModel
    let onLayoutSnapshotChange: ((RBACCanILayoutSnapshot) -> Void)?
    @Environment(\.runeThemePalette) private var runeThemePalette

    init(
        viewModel: RuneAppViewModel,
        onLayoutSnapshotChange: ((RBACCanILayoutSnapshot) -> Void)? = nil
    ) {
        self.viewModel = viewModel
        self.onLayoutSnapshotChange = onLayoutSnapshotChange
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            header
                .rbacCanILayoutProbe(.header, enabled: onLayoutSnapshotChange != nil)
            fields
            result
                .rbacCanILayoutProbe(.result, enabled: onLayoutSnapshotChange != nil)
        }
        .padding(RBACCanILayoutMetrics.panelHorizontalPadding)
        .background(RuneSurfaceBackground(kind: .panel))
        .rbacCanILayoutReporting(onLayoutSnapshotChange)
    }

    private var header: some View {
        RuneAdaptiveToolbar("RBAC access review actions") {
            Label("Can I?", systemImage: "checkmark.shield")
                .font(.subheadline.weight(.semibold))
        } secondary: {
            HStack(spacing: 8) {
                Button("Use Selected") {
                    viewModel.useSelectedRBACResourceForCanI()
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
                .disabled(viewModel.state.selectedRBACResource == nil)
                Button(viewModel.isRunningRBACCanI ? "Checking..." : "Check") {
                    viewModel.runRBACCanISimulator()
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.small)
                .disabled(viewModel.isRunningRBACCanI)
            }
        }
    }

    private var fields: some View {
        VStack(alignment: .leading, spacing: 8) {
            RuneAdaptiveToolbar("RBAC access review request") {
                HStack(alignment: .center, spacing: 8) {
                    TextField("Verb", text: $viewModel.rbacCanIVerb)
                        .textFieldStyle(.roundedBorder)
                        .frame(width: 86)
                    TextField("Resource", text: $viewModel.rbacCanIResource)
                        .textFieldStyle(.roundedBorder)
                        .frame(minWidth: 130)
                }
            } secondary: {
                Picker("Scope", selection: $viewModel.rbacCanIScope) {
                    ForEach(RBACCanIScope.allCases) { scope in
                        Text(scope.title).tag(scope)
                    }
                }
                .pickerStyle(.segmented)
                .labelsHidden()
                .frame(width: 170)
            }
            .rbacCanILayoutProbe(.request, enabled: onLayoutSnapshotChange != nil)

            ViewThatFits(in: .horizontal) {
                HStack(alignment: .center, spacing: 8) {
                    apiGroupField
                        .frame(minWidth: 150)
                    subresourceField
                        .frame(minWidth: 120)
                }

                VStack(alignment: .leading, spacing: 8) {
                    apiGroupField
                    subresourceField
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .rbacCanILayoutProbe(.optionalFields, enabled: onLayoutSnapshotChange != nil)
        }
        .controlSize(.small)
    }

    private var apiGroupField: some View {
        TextField("API group", text: $viewModel.rbacCanIApiGroup)
            .textFieldStyle(.roundedBorder)
            .frame(maxWidth: .infinity)
    }

    private var subresourceField: some View {
        TextField("Subresource", text: $viewModel.rbacCanISubresource)
            .textFieldStyle(.roundedBorder)
            .frame(maxWidth: .infinity)
    }

    @ViewBuilder
    private var result: some View {
        if let result = viewModel.rbacCanIResult {
            HStack(alignment: .top, spacing: 8) {
                Image(systemName: resultSymbol(result))
                    .foregroundStyle(resultColor(result))
                    .frame(width: 16)
                VStack(alignment: .leading, spacing: 2) {
                    Text(result.statusText)
                        .font(.caption.weight(.semibold))
                    Text(result.errorMessage ?? result.request.summary)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
        } else {
            Text("Runs a read-only SelfSubjectAccessReview for the selected verb, resource, and scope.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    private func resultSymbol(_ result: RBACCanIResult) -> String {
        if let allowed = result.allowed {
            return allowed ? "checkmark.circle.fill" : "xmark.octagon.fill"
        }
        return "exclamationmark.triangle.fill"
    }

    private func resultColor(_ result: RBACCanIResult) -> Color {
        if let allowed = result.allowed {
            return allowed
                ? RuneSemanticColorRole.success.color(in: runeThemePalette)
                : RuneSemanticColorRole.danger.color(in: runeThemePalette)
        }
        return RuneSemanticColorRole.warning.color(in: runeThemePalette)
    }
}

private enum RBACCanILayoutCoordinateSpace {
    static let name = "RBACCanILayoutCoordinateSpace"
}

private struct RBACCanILayoutFramePreferenceKey: PreferenceKey {
    static let defaultValue: [RBACCanILayoutRegion: CGRect] = [:]

    static func reduce(
        value: inout [RBACCanILayoutRegion: CGRect],
        nextValue: () -> [RBACCanILayoutRegion: CGRect]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private extension View {
    @ViewBuilder
    func rbacCanILayoutProbe(
        _ region: RBACCanILayoutRegion,
        enabled: Bool
    ) -> some View {
        if enabled {
            overlay {
                GeometryReader { proxy in
                    Color.clear.preference(
                        key: RBACCanILayoutFramePreferenceKey.self,
                        value: [
                            region: proxy.frame(in: .named(RBACCanILayoutCoordinateSpace.name))
                        ]
                    )
                }
                .allowsHitTesting(false)
                .accessibilityHidden(true)
            }
        } else {
            self
        }
    }

    @ViewBuilder
    func rbacCanILayoutReporting(
        _ onChange: ((RBACCanILayoutSnapshot) -> Void)?
    ) -> some View {
        if let onChange {
            coordinateSpace(name: RBACCanILayoutCoordinateSpace.name)
                .onPreferenceChange(RBACCanILayoutFramePreferenceKey.self) { frames in
                    onChange(RBACCanILayoutSnapshot(frames: frames))
                }
        } else {
            self
        }
    }
}
