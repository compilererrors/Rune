import RuneCore
import SwiftUI

enum PortForwardControlLayoutMetrics {
    static let supportedInspectorWidth: CGFloat = 320
    static let spacing: CGFloat = 8
    static let compactMinimumHeight: CGFloat = 94
    static let widePortFieldWidth: CGFloat = 104
    static let wideAddressFieldWidth: CGFloat = 168
    static let minimumControlHeight: CGFloat = 28
    static let accessibilityMinimumControlHeight: CGFloat = 44
    static let accessibilityCompactMinimumHeight: CGFloat = 126
}

struct PortForwardEndpointLayout: Layout {
    let compactMinimumHeight: CGFloat

    private var spacing: CGFloat {
        PortForwardControlLayoutMetrics.spacing
    }

    func sizeThatFits(
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) -> CGSize {
        guard subviews.count == 4 else { return .zero }
        let width = resolvedWidth(for: proposal, subviews: subviews)
        if usesWideLayout(width: width, subviews: subviews) {
            return CGSize(
                width: width,
                height: wideMeasurements(subviews: subviews).height
            )
        }

        let compact = compactMeasurements(width: width, subviews: subviews)
        return CGSize(
            width: width,
            height: max(compactMinimumHeight, compact.firstRowHeight + spacing + compact.addressSize.height)
        )
    }

    func placeSubviews(
        in bounds: CGRect,
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) {
        guard subviews.count == 4 else { return }
        if usesWideLayout(width: bounds.width, subviews: subviews) {
            placeWideSubviews(in: bounds, subviews: subviews)
        } else {
            placeCompactSubviews(in: bounds, subviews: subviews)
        }
    }

    private func resolvedWidth(for proposal: ProposedViewSize, subviews: Subviews) -> CGFloat {
        proposal.width ?? wideMinimumWidth(subviews: subviews)
    }

    private func usesWideLayout(width: CGFloat, subviews: Subviews) -> Bool {
        width >= wideMinimumWidth(subviews: subviews)
    }

    private func wideMinimumWidth(subviews: Subviews) -> CGFloat {
        PortForwardControlLayoutMetrics.widePortFieldWidth * 2
            + PortForwardControlLayoutMetrics.wideAddressFieldWidth
            + subviews[1].sizeThatFits(.unspecified).width
            + spacing * 3
    }

    private func wideMeasurements(subviews: Subviews) -> (
        sizes: [CGSize],
        height: CGFloat
    ) {
        let widths = [
            PortForwardControlLayoutMetrics.widePortFieldWidth,
            subviews[1].sizeThatFits(.unspecified).width,
            PortForwardControlLayoutMetrics.widePortFieldWidth,
            PortForwardControlLayoutMetrics.wideAddressFieldWidth
        ]
        let sizes = zip(subviews, widths).map { subview, width in
            subview.sizeThatFits(ProposedViewSize(width: width, height: nil))
        }
        return (sizes, sizes.map(\.height).max() ?? 0)
    }

    private func compactMeasurements(width: CGFloat, subviews: Subviews) -> (
        portWidth: CGFloat,
        directionSize: CGSize,
        localSize: CGSize,
        remoteSize: CGSize,
        addressSize: CGSize,
        firstRowHeight: CGFloat
    ) {
        let directionSize = subviews[1].sizeThatFits(.unspecified)
        let portWidth = max(0, (width - directionSize.width - spacing * 2) / 2)
        let portProposal = ProposedViewSize(width: portWidth, height: nil)
        let localSize = subviews[0].sizeThatFits(portProposal)
        let remoteSize = subviews[2].sizeThatFits(portProposal)
        let addressSize = subviews[3].sizeThatFits(
            ProposedViewSize(width: width, height: nil)
        )
        return (
            portWidth,
            directionSize,
            localSize,
            remoteSize,
            addressSize,
            max(localSize.height, max(directionSize.height, remoteSize.height))
        )
    }

    private func placeWideSubviews(in bounds: CGRect, subviews: Subviews) {
        let measurements = wideMeasurements(subviews: subviews)
        var x = bounds.minX
        for (subview, size) in zip(subviews, measurements.sizes) {
            subview.place(
                at: CGPoint(x: x, y: bounds.minY + measurements.height - size.height),
                anchor: .topLeading,
                proposal: ProposedViewSize(width: size.width, height: size.height)
            )
            x += size.width + spacing
        }
    }

    private func placeCompactSubviews(in bounds: CGRect, subviews: Subviews) {
        let measurements = compactMeasurements(width: bounds.width, subviews: subviews)
        let firstRowBottom = bounds.minY + measurements.firstRowHeight

        subviews[0].place(
            at: CGPoint(x: bounds.minX, y: firstRowBottom - measurements.localSize.height),
            anchor: .topLeading,
            proposal: ProposedViewSize(
                width: measurements.portWidth,
                height: measurements.localSize.height
            )
        )
        subviews[1].place(
            at: CGPoint(
                x: bounds.minX + measurements.portWidth + spacing,
                y: firstRowBottom - measurements.directionSize.height
            ),
            anchor: .topLeading,
            proposal: ProposedViewSize(
                width: measurements.directionSize.width,
                height: measurements.directionSize.height
            )
        )
        subviews[2].place(
            at: CGPoint(
                x: bounds.minX
                    + measurements.portWidth
                    + spacing
                    + measurements.directionSize.width
                    + spacing,
                y: firstRowBottom - measurements.remoteSize.height
            ),
            anchor: .topLeading,
            proposal: ProposedViewSize(
                width: measurements.portWidth,
                height: measurements.remoteSize.height
            )
        )
        subviews[3].place(
            at: CGPoint(
                x: bounds.minX,
                y: bounds.minY + measurements.firstRowHeight + spacing
            ),
            anchor: .topLeading,
            proposal: ProposedViewSize(
                width: bounds.width,
                height: measurements.addressSize.height
            )
        )
    }
}

/// The shared endpoint form used by terminal and resource inspectors. Its
/// compact fallback keeps every field visible instead of relying on a hidden
/// horizontal scroller.
struct PortForwardEndpointFields: View {
    @Binding var localPort: String
    @Binding var remotePort: String
    @Binding var address: String
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    var body: some View {
        PortForwardEndpointLayout(compactMinimumHeight: compactMinimumHeight) {
            endpointField("Local port", placeholder: "Local", text: $localPort)
            directionIndicator
            endpointField("Remote port", placeholder: "Remote", text: $remotePort)
            endpointField("Address", placeholder: "Address", text: $address)
        }
        .controlSize(dynamicTypeSize.isAccessibilitySize ? .large : .regular)
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Port-forward endpoint")
    }

    private func endpointField(
        _ title: String,
        placeholder: String,
        text: Binding<String>
    ) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.runeSecondary)
            TextField(placeholder, text: text)
                .textFieldStyle(.roundedBorder)
                .font(.callout.monospaced())
                .frame(minHeight: minimumControlHeight)
                .accessibilityLabel(title)
        }
    }

    private var directionIndicator: some View {
        Text("→")
            .font(.caption.weight(.semibold))
            .foregroundStyle(.runeSecondary)
            .frame(minHeight: minimumControlHeight)
            .accessibilityHidden(true)
    }

    private var minimumControlHeight: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? PortForwardControlLayoutMetrics.accessibilityMinimumControlHeight
            : PortForwardControlLayoutMetrics.minimumControlHeight
    }

    private var compactMinimumHeight: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? PortForwardControlLayoutMetrics.accessibilityCompactMinimumHeight
            : PortForwardControlLayoutMetrics.compactMinimumHeight
    }
}

/// A single state-aware Start/Cancel/Stop control. Both Port Forward surfaces
/// use this control so an active session cannot leave a second competing Stop
/// action in its status row.
struct PortForwardPrimaryActionButton: View {
    let activeSession: PortForwardSession?
    let startTitle: String
    let isStartDisabled: Bool
    let usesUniformWidth: Bool
    let onStart: () -> Void
    let onStop: (PortForwardSession) -> Void

    init(
        activeSession: PortForwardSession?,
        startTitle: String,
        isStartDisabled: Bool,
        usesUniformWidth: Bool = false,
        onStart: @escaping () -> Void,
        onStop: @escaping (PortForwardSession) -> Void
    ) {
        self.activeSession = activeSession
        self.startTitle = startTitle
        self.isStartDisabled = isStartDisabled
        self.usesUniformWidth = usesUniformWidth
        self.onStart = onStart
        self.onStop = onStop
    }

    var primaryActionTitle: String {
        guard let activeSession else { return startTitle }
        return activeSession.status == .starting ? "Cancel" : "Stop"
    }

    private var primaryActionSystemImage: String {
        activeSession == nil ? "play.fill" : "stop.fill"
    }

    var body: some View {
        Button(action: performPrimaryAction) {
            TerminalActionButtonLabel(
                primaryActionTitle,
                systemImage: primaryActionSystemImage,
                density: .regular,
                usesUniformWidth: usesUniformWidth
            )
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .terminalActionControl(.regular, usesUniformWidth: usesUniformWidth)
        .disabled(activeSession == nil && isStartDisabled)
        .help(primaryActionTitle)
        .accessibilityIdentifier("rune.port-forward.primary-action")
    }

    func performPrimaryAction() {
        if let activeSession {
            onStop(activeSession)
        } else if !isStartDisabled {
            onStart()
        }
    }
}

/// Keeps the state-changing action visually primary. Utilities stay inline
/// when they fit and move below it as a group at compact/enlarged sizes.
struct PortForwardPrimaryActionLayout<Primary: View, Utilities: View>: View {
    @ViewBuilder let primary: Primary
    @ViewBuilder let utilities: Utilities

    init(
        @ViewBuilder primary: () -> Primary,
        @ViewBuilder utilities: () -> Utilities
    ) {
        self.primary = primary()
        self.utilities = utilities()
    }

    var body: some View {
        ViewThatFits(in: .horizontal) {
            HStack(spacing: PortForwardControlLayoutMetrics.spacing) {
                primary
                utilities
            }
            .fixedSize(horizontal: true, vertical: false)

            VStack(alignment: .leading, spacing: PortForwardControlLayoutMetrics.spacing) {
                primary
                utilities
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Port-forward actions")
    }
}
