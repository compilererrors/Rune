import SwiftUI

/// 4-point spacing grid. `hairline`, `tight` and `inset` are the only half
/// steps: glyph gaps and the text inset inside fields, tabs and documents,
/// which keeps compact 320-point inspectors from clipping.
enum RuneSpacing {
    static let hairline: CGFloat = 2
    static let xxs: CGFloat = 4
    static let tight: CGFloat = 6
    static let xs: CGFloat = 8
    static let inset: CGFloat = 10
    static let sm: CGFloat = 12
    static let md: CGFloat = 16
    static let lg: CGFloat = 20
    static let xl: CGFloat = 24
    static let xxl: CGFloat = 32

    static let scale: [CGFloat] = [hairline, xxs, tight, xs, inset, sm, md, lg, xl, xxl]
}

/// Continuous corner radii, from window-level panels down to inline chips.
enum RuneRadius {
    static let panel: CGFloat = 14
    static let card: CGFloat = 10
    static let control: CGFloat = 8
    static let tab: CGFloat = 7
    static let chip: CGFloat = 6
    static let indicator: CGFloat = 2

    static let scale: [CGFloat] = [indicator, chip, tab, control, card, panel]
}

/// Control sizes follow the AppKit scale as rendered on current macOS
/// (mini 16, small 20, regular 24, large 28 pt): a 4-point step, about 1.17x
/// per step, with 13 pt labels at regular and large. Heights grow 2 pt for
/// each point the interface font grows. Controls that share a row differ by
/// at most one step.
///
/// Component sizes:
/// - Buttons, toggles and menus (`RuneToolbarButtonStyle`,
///   `RuneToolbarMenu`): large, 13 pt medium labels, one of four roles
///   (`RuneButtonRole`: primary, secondary, plain, destructive).
/// - Tab bars (`RuneTabBar`): regular segments in a 2 pt track, so the bar is
///   as tall as the large buttons.
/// - Every editable text input (`RuneFormTextFieldStyle`): native regular,
///   the largest AppKit rounded field, one step below toolbar buttons.
/// - Settings rows (`RuneControlTextFieldStyle`): large, themed.
/// - Badges (`RuneBadge`, `RuneHeaderCapsule`): small, 11 pt medium text.
/// - Table status pills: small.
/// - Rows that can run out of width use `RuneToolbarOverflowRow`: icons
///   only, then wrapping, never clipping.
enum RuneControlSize: Sendable, CaseIterable {
    case small
    case regular
    case large

    var baseHeight: CGFloat {
        switch self {
        case .small: return 20
        case .regular: return 24
        case .large: return 28
        }
    }

    /// Label size relative to the interface font. Regular and large share the
    /// 13 pt label AppKit draws in regular and large segmented controls, so a
    /// row of tabs, fields and buttons reads as one size.
    var labelRelativeSize: CGFloat {
        switch self {
        case .small: return -2
        case .regular, .large: return 0
        }
    }

    func height(interfaceFontSize: CGFloat) -> CGFloat {
        baseHeight + max(0, interfaceFontSize - RuneInterfaceTypography.standardMenuFontSize) * 2
    }

    func nativeControlSize(interfaceFontSize: CGFloat, isAccessibilitySize: Bool = false) -> ControlSize {
        if isAccessibilitySize { return .large }
        let grows = interfaceFontSize > RuneInterfaceTypography.standardMenuFontSize
        switch self {
        case .small: return grows ? .regular : .small
        case .regular: return grows ? .large : .regular
        case .large: return .large
        }
    }
}

/// Fixed control and row heights shared by sidebars, toolbars and inspectors.
enum RuneControlHeight {
    static let compact: CGFloat = RuneControlSize.regular.baseHeight
    static let regular: CGFloat = RuneControlSize.large.baseHeight
    static let toolbar: CGFloat = RuneControlSize.large.baseHeight
    static let row: CGFloat = 32
    static let accessibility: CGFloat = 44
}

/// Semantic type roles on top of the configurable interface font size
/// (13 pt by default). Every text style in the app maps to one of these, so
/// text follows the user's font size setting. See Design/README.md.
enum RuneTextRole: Sendable, Equatable, CaseIterable {
    case pageTitle
    case title
    case sectionTitle
    case headline
    case groupLabel
    case body
    case bodyEmphasis
    case caption
    case captionEmphasis
    case micro
    case microEmphasis
    case code

    var relativeSize: CGFloat {
        switch self {
        case .pageTitle: return 7
        case .title: return 2
        case .sectionTitle: return 1
        case .headline, .body, .bodyEmphasis: return 0
        case .caption, .captionEmphasis, .code: return -1
        case .groupLabel, .micro, .microEmphasis: return -2
        }
    }

    var weight: Font.Weight {
        switch self {
        case .pageTitle: return .bold
        case .title, .sectionTitle, .headline, .groupLabel, .captionEmphasis, .microEmphasis: return .semibold
        case .bodyEmphasis: return .medium
        case .body, .caption, .micro, .code: return .regular
        }
    }

    var design: Font.Design {
        self == .code ? .monospaced : .default
    }

    var tracking: CGFloat {
        self == .groupLabel ? 0.6 : 0
    }

    var isUppercased: Bool {
        self == .groupLabel
    }
}

/// Symbol sizes that are not tied to text: tiles, sheet headers and empty
/// states. Inline symbols next to text take the text's role instead.
enum RuneGlyphSize {
    static let tile: CGFloat = 15
    static let sheetHeader: CGFloat = 17
    static let onboarding: CGFloat = 19
    static let dialog: CGFloat = 24
    static let hero: CGFloat = 58
}

private struct RuneTextModifier: ViewModifier {
    let role: RuneTextRole
    let weight: Font.Weight?
    let design: Font.Design?

    func body(content: Content) -> some View {
        let styled = content.runeInterfaceFont(
            relativeSize: role.relativeSize,
            weight: weight ?? role.weight,
            design: design ?? role.design
        )
        // Only roles that need tracking or case set them, so a role never
        // resets a text case chosen by a container.
        if role.isUppercased {
            styled.tracking(role.tracking).textCase(.uppercase)
        } else {
            styled
        }
    }
}

extension View {
    /// Sets text to a semantic role. `weight` and `design` override the
    /// role for one-off emphasis or monospaced values.
    func runeText(_ role: RuneTextRole, weight: Font.Weight? = nil, design: Font.Design? = nil) -> some View {
        modifier(RuneTextModifier(role: role, weight: weight, design: design))
    }
}

/// Categorical hues for the overview resource cards. They distinguish kinds,
/// not state; state always uses `RuneSemanticColorRole`.
enum RuneCategoryColor {
    static let pods = Color.cyan
    static let deployments = Color.blue
    static let services = Color.purple
    static let ingresses = Color.indigo
    static let configMaps = Color.teal
    static let cronJobs = Color.mint
    static let nodes = Color.gray
    static let events = Color.orange
}
