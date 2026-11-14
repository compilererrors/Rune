import SwiftUI

/// Tone of a non-interactive badge. Neutral carries metadata, accent marks
/// the user's current selection, the semantic tones carry health.
enum RuneBadgeTone: Sendable, Equatable {
    case neutral
    case accent
    case success
    case warning
    case danger

    func tint(in palette: RuneThemePalette?) -> Color? {
        switch self {
        case .neutral: return nil
        case .accent: return palette?.accentFill ?? Color.accentColor
        case .success: return RuneSemanticColorRole.success.color(in: palette)
        case .warning: return RuneSemanticColorRole.warning.color(in: palette)
        case .danger: return RuneSemanticColorRole.danger.color(in: palette)
        }
    }
}

/// The one badge surface: small control height (20 pt at the 13 pt
/// interface size), 11 pt medium text, capsule shape. Header metadata, the
/// selection count and timestamps all use it, one step below the 24 pt
/// fields and two below the 28 pt buttons beside them.
struct RuneBadgeSurface: ViewModifier {
    var tone: RuneBadgeTone = .neutral
    var fill: Color? = nil
    var foreground: Color? = nil
    var stroke: Color? = nil
    /// A badge inside a row of 28 pt controls takes the row's height, radius
    /// and 13 pt text (one cluster, one look); elsewhere it is small.
    var inControlRow = false
    @Environment(\.runeThemePalette) private var palette
    @Environment(\.runeInterfaceFontSize) private var fontSize
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    func body(content: Content) -> some View {
        let tint = tone.tint(in: palette)
        let shape = RoundedRectangle(cornerRadius: inControlRow ? RuneRadius.control : height / 2, style: .continuous)
        content
            .runeInterfaceFont(
                relativeSize: inControlRow ? RuneControlSize.large.labelRelativeSize : RuneControlSize.small.labelRelativeSize,
                weight: inControlRow ? .medium : .semibold
            )
            .imageScale(inControlRow ? .medium : .small)
            .lineLimit(1)
            .foregroundStyle(foreground ?? tint ?? palette?.secondaryText ?? Color.secondary)
            .padding(.horizontal, inControlRow ? RuneSpacing.inset : RuneUILayoutMetrics.headerCapsuleHorizontalPadding)
            .padding(.vertical, dynamicTypeSize.isAccessibilitySize
                ? RuneUILayoutMetrics.headerCapsuleAccessibilityVerticalPadding
                : RuneUILayoutMetrics.headerCapsuleVerticalPadding)
            .frame(minHeight: height)
            .background(shape.fill(fill ?? tint?.opacity(0.16) ?? palette?.chipFill ?? Color.secondary.opacity(0.12)))
            .overlay {
                if let stroke {
                    shape.strokeBorder(stroke, lineWidth: 1)
                } else if inControlRow {
                    shape.strokeBorder((palette?.stroke ?? Color(nsColor: .separatorColor)).opacity(0.45), lineWidth: 1)
                }
            }
            .fixedSize(horizontal: false, vertical: true)
    }

    private var height: CGFloat {
        if inControlRow {
            return dynamicTypeSize.isAccessibilitySize
                ? RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight
                : RuneControlSize.large.height(interfaceFontSize: fontSize)
        }
        // Accessibility sizes move badges up to the large control size.
        return dynamicTypeSize.isAccessibilitySize
            ? RuneControlSize.large.baseHeight
            : RuneControlSize.small.height(interfaceFontSize: fontSize)
    }
}

/// Text and optional symbol on the shared badge surface.
struct RuneBadge: View {
    let text: String
    var systemImage: String? = nil
    var tone: RuneBadgeTone = .neutral
    var inControlRow = false

    init(_ text: String, systemImage: String? = nil, tone: RuneBadgeTone = .neutral, inControlRow: Bool = false) {
        self.text = text
        self.systemImage = systemImage
        self.tone = tone
        self.inControlRow = inControlRow
    }

    var body: some View {
        HStack(spacing: RuneSpacing.xxs) {
            if let systemImage {
                Image(systemName: systemImage)
                    .accessibilityHidden(true)
            }
            Text(text)
        }
        .modifier(RuneBadgeSurface(tone: tone, inControlRow: inControlRow))
    }
}

extension View {
    func runeBadgeSurface(_ tone: RuneBadgeTone = .neutral) -> some View {
        modifier(RuneBadgeSurface(tone: tone))
    }
}

/// The one status dot: health next to a label, row or badge. Colour comes
/// from `RuneSemanticColorRole` (or the theme accent for selection).
struct RuneStatusDot: View {
    static let diameter: CGFloat = 7
    let color: Color

    init(_ color: Color) {
        self.color = color
    }

    var body: some View {
        Circle()
            .fill(color)
            .frame(width: Self.diameter, height: Self.diameter)
            .accessibilityHidden(true)
    }
}
