import SwiftUI

enum RuneHeaderCapsuleRole: Sendable, Equatable {
    case context
    case status
    case value

    var prefersMiddleTruncation: Bool {
        switch self {
        case .context, .value:
            return true
        case .status:
            return false
        }
    }

    var accessibilityTitle: String {
        switch self {
        case .context: return "Context"
        case .status: return "Status"
        case .value: return "Value"
        }
    }

    var fontWeight: Font.Weight {
        switch self {
        case .context: return .medium
        case .status: return .semibold
        case .value: return .bold
        }
    }
}

/// Compact, non-interactive metadata for pane headers. Its height is a minimum,
/// allowing the capsule to grow with accessibility text instead of clipping it.
struct RuneHeaderCapsule: View {
    let text: String
    let role: RuneHeaderCapsuleRole
    let systemImage: String?
    let indicatorColor: Color?
    let tint: Color?
    let foregroundColor: Color?
    let fill: Color?
    let stroke: Color?
    let helpText: String?
    let accessibilityLabel: String?

    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeThemePalette) private var runeThemePalette
    @ScaledMetric(relativeTo: .caption) private var indicatorDiameter: CGFloat = 6
    @ScaledMetric(relativeTo: .caption) private var symbolWidth: CGFloat = 11

    init(
        _ text: String,
        role: RuneHeaderCapsuleRole,
        systemImage: String? = nil,
        indicatorColor: Color? = nil,
        tint: Color? = nil,
        foregroundColor: Color? = nil,
        fill: Color? = nil,
        stroke: Color? = nil,
        helpText: String? = nil,
        accessibilityLabel: String? = nil
    ) {
        self.text = text
        self.role = role
        self.systemImage = systemImage
        self.indicatorColor = indicatorColor
        self.tint = tint
        self.foregroundColor = foregroundColor
        self.fill = fill
        self.stroke = stroke
        self.helpText = helpText
        self.accessibilityLabel = accessibilityLabel
    }

    var body: some View {
        HStack(spacing: RuneSpacing.xxs) {
            if let indicatorColor {
                Circle()
                    .fill(indicatorColor)
                    .frame(width: indicatorDiameter, height: indicatorDiameter)
                    .accessibilityHidden(true)
            } else if let systemImage {
                Image(systemName: systemImage)
                    .foregroundStyle(tint ?? foregroundColor ?? runeThemePalette?.secondaryText ?? Color.secondary)
                    .frame(width: symbolWidth)
                    .accessibilityHidden(true)
            }

            Text(text)
                .truncationMode(role.prefersMiddleTruncation ? .middle : .tail)
                .fixedSize(horizontal: false, vertical: true)
        }
        .modifier(RuneBadgeSurface(
            fill: fill,
            foreground: foregroundColor,
            stroke: stroke
        ))
        .help(resolvedHelpText)
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(accessibilityLabel ?? "\(role.accessibilityTitle): \(text)")
    }

    var resolvedHelpText: String {
        helpText ?? text
    }
}
