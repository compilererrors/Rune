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

/// The shared endpoint form used by terminal and resource inspectors. Its
/// compact fallback keeps every field visible instead of relying on a hidden
/// horizontal scroller.
struct PortForwardEndpointFields: View {
    @Binding var localPort: String
    @Binding var remotePort: String
    @Binding var address: String
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    var body: some View {
        ViewThatFits(in: .horizontal) {
            wideFields
            compactFields
        }
        .controlSize(dynamicTypeSize.isAccessibilitySize ? .large : .regular)
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Port-forward endpoint")
    }

    private var wideFields: some View {
        HStack(alignment: .bottom, spacing: PortForwardControlLayoutMetrics.spacing) {
            endpointField("Local port", placeholder: "Local", text: $localPort)
                .frame(width: PortForwardControlLayoutMetrics.widePortFieldWidth)
            directionIndicator
            endpointField("Remote port", placeholder: "Remote", text: $remotePort)
                .frame(width: PortForwardControlLayoutMetrics.widePortFieldWidth)
            endpointField("Address", placeholder: "Address", text: $address)
                .frame(width: PortForwardControlLayoutMetrics.wideAddressFieldWidth)
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var compactFields: some View {
        VStack(alignment: .leading, spacing: PortForwardControlLayoutMetrics.spacing) {
            HStack(alignment: .bottom, spacing: PortForwardControlLayoutMetrics.spacing) {
                endpointField("Local port", placeholder: "Local", text: $localPort)
                    .frame(maxWidth: .infinity)
                directionIndicator
                endpointField("Remote port", placeholder: "Remote", text: $remotePort)
                    .frame(maxWidth: .infinity)
            }

            endpointField("Address", placeholder: "Address", text: $address)
                .frame(maxWidth: .infinity)
        }
        .frame(
            maxWidth: .infinity,
            minHeight: compactMinimumHeight,
            alignment: .leading
        )
    }

    private func endpointField(
        _ title: String,
        placeholder: String,
        text: Binding<String>
    ) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
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
            .foregroundStyle(.secondary)
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
    let onStart: () -> Void
    let onStop: (PortForwardSession) -> Void
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    var primaryActionTitle: String {
        guard let activeSession else { return startTitle }
        return activeSession.status == .starting ? "Cancel" : "Stop"
    }

    private var primaryActionSystemImage: String {
        activeSession == nil ? "play.fill" : "stop.fill"
    }

    var body: some View {
        Button(action: performPrimaryAction) {
            Label(primaryActionTitle, systemImage: primaryActionSystemImage)
                .fixedSize(horizontal: false, vertical: true)
        }
        .buttonStyle(.borderedProminent)
        .controlSize(dynamicTypeSize.isAccessibilitySize ? .large : .regular)
        .frame(minHeight: minimumControlHeight)
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

    private var minimumControlHeight: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? PortForwardControlLayoutMetrics.accessibilityMinimumControlHeight
            : PortForwardControlLayoutMetrics.minimumControlHeight
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
