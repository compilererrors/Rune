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
    let accessibilityLabel: String?

    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeThemePalette) private var runeThemePalette
    @ScaledMetric(relativeTo: .caption) private var indicatorDiameter: CGFloat = 7
    @ScaledMetric(relativeTo: .caption) private var symbolWidth: CGFloat = 12

    init(
        _ text: String,
        role: RuneHeaderCapsuleRole,
        systemImage: String? = nil,
        indicatorColor: Color? = nil,
        tint: Color? = nil,
        foregroundColor: Color? = nil,
        fill: Color? = nil,
        stroke: Color? = nil,
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
        self.accessibilityLabel = accessibilityLabel
    }

    var body: some View {
        HStack(spacing: 6) {
            if let indicatorColor {
                Circle()
                    .fill(indicatorColor)
                    .frame(width: indicatorDiameter, height: indicatorDiameter)
                    .accessibilityHidden(true)
            } else if let systemImage {
                Image(systemName: systemImage)
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(tint ?? foregroundColor ?? Color.secondary)
                    .frame(width: symbolWidth)
                    .accessibilityHidden(true)
            }

            Text(text)
                .font(.caption.weight(role.fontWeight))
                .foregroundStyle(foregroundColor ?? Color.secondary)
                .lineLimit(1)
                .truncationMode(role.prefersMiddleTruncation ? .middle : .tail)
                .fixedSize(horizontal: false, vertical: true)
        }
        .padding(.horizontal, RuneUILayoutMetrics.headerCapsuleHorizontalPadding)
        .padding(.vertical, verticalPadding)
        .frame(minHeight: RuneUILayoutMetrics.headerCapsuleMinimumHeight, alignment: .center)
        .background {
            Capsule()
                .fill(fill ?? runeThemePalette?.chipFill ?? Color.secondary.opacity(0.12))
        }
        .overlay {
            if let stroke {
                Capsule()
                    .strokeBorder(stroke, lineWidth: 1)
            }
        }
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(accessibilityLabel ?? "\(role.accessibilityTitle): \(text)")
    }

    private var verticalPadding: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? RuneUILayoutMetrics.headerCapsuleAccessibilityVerticalPadding
            : RuneUILayoutMetrics.headerCapsuleVerticalPadding
    }
}
