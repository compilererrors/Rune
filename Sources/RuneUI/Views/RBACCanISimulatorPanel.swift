import SwiftUI

enum RBACCanILayoutMetrics {
    static let panelHorizontalPadding: CGFloat = 12
    static let compactPanelWidth: CGFloat = 264
    static let optionalFieldSpacing: CGFloat = 8
    static let apiGroupMinimumWidth: CGFloat = 150
    static let subresourceMinimumWidth: CGFloat = 120

    static var compactContentWidth: CGFloat {
        compactPanelWidth - panelHorizontalPadding * 2
    }
}

private struct RBACOptionalFieldsLayout: Layout {
    func sizeThatFits(
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) -> CGSize {
        guard subviews.count == 2 else { return .zero }
        let width = proposal.width ?? horizontalMinimumWidth
        let measurements = fieldMeasurements(width: width, subviews: subviews)
        return CGSize(width: width, height: measurements.height)
    }

    func placeSubviews(
        in bounds: CGRect,
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) {
        guard subviews.count == 2 else { return }
        let measurements = fieldMeasurements(width: bounds.width, subviews: subviews)

        if measurements.isHorizontal {
            subviews[0].place(
                at: CGPoint(x: bounds.minX, y: bounds.minY),
                anchor: .topLeading,
                proposal: ProposedViewSize(
                    width: measurements.apiGroupWidth,
                    height: measurements.apiGroupSize.height
                )
            )
            subviews[1].place(
                at: CGPoint(
                    x: bounds.minX
                        + measurements.apiGroupWidth
                        + RBACCanILayoutMetrics.optionalFieldSpacing,
                    y: bounds.minY
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(
                    width: measurements.subresourceWidth,
                    height: measurements.subresourceSize.height
                )
            )
            return
        }

        subviews[0].place(
            at: bounds.origin,
            anchor: .topLeading,
            proposal: ProposedViewSize(
                width: bounds.width,
                height: measurements.apiGroupSize.height
            )
        )
        subviews[1].place(
            at: CGPoint(
                x: bounds.minX,
                y: bounds.minY
                    + measurements.apiGroupSize.height
                    + RBACCanILayoutMetrics.optionalFieldSpacing
            ),
            anchor: .topLeading,
            proposal: ProposedViewSize(
                width: bounds.width,
                height: measurements.subresourceSize.height
            )
        )
    }

    private var horizontalMinimumWidth: CGFloat {
        RBACCanILayoutMetrics.apiGroupMinimumWidth
            + RBACCanILayoutMetrics.optionalFieldSpacing
            + RBACCanILayoutMetrics.subresourceMinimumWidth
    }

    private func fieldMeasurements(width: CGFloat, subviews: Subviews) -> (
        isHorizontal: Bool,
        apiGroupWidth: CGFloat,
        subresourceWidth: CGFloat,
        apiGroupSize: CGSize,
        subresourceSize: CGSize,
        height: CGFloat
    ) {
        let isHorizontal = width >= horizontalMinimumWidth
        if isHorizontal {
            let extraWidth = width - horizontalMinimumWidth
            let apiGroupWidth = RBACCanILayoutMetrics.apiGroupMinimumWidth + extraWidth / 2
            let subresourceWidth = RBACCanILayoutMetrics.subresourceMinimumWidth + extraWidth / 2
            let apiGroupSize = subviews[0].sizeThatFits(
                ProposedViewSize(width: apiGroupWidth, height: nil)
            )
            let subresourceSize = subviews[1].sizeThatFits(
                ProposedViewSize(width: subresourceWidth, height: nil)
            )
            return (
                true,
                apiGroupWidth,
                subresourceWidth,
                apiGroupSize,
                subresourceSize,
                max(apiGroupSize.height, subresourceSize.height)
            )
        }

        let fieldProposal = ProposedViewSize(width: width, height: nil)
        let apiGroupSize = subviews[0].sizeThatFits(fieldProposal)
        let subresourceSize = subviews[1].sizeThatFits(fieldProposal)
        return (
            false,
            width,
            width,
            apiGroupSize,
            subresourceSize,
            apiGroupSize.height
                + RBACCanILayoutMetrics.optionalFieldSpacing
                + subresourceSize.height
        )
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
                .buttonStyle(RuneToolbarButtonStyle())
                .controlSize(.small)
                .disabled(viewModel.state.selectedRBACResource == nil)
                Button(viewModel.isRunningRBACCanI ? "Checking..." : "Check") {
                    viewModel.runRBACCanISimulator()
                }
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
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

            RBACOptionalFieldsLayout {
                apiGroupField
                subresourceField
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
                        .foregroundStyle(.runeSecondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
        } else {
            Text("Runs a read-only SelfSubjectAccessReview for the selected verb, resource, and scope.")
                .font(.caption)
                .foregroundStyle(.runeSecondary)
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
