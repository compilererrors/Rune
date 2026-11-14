import AppKit
import SwiftUI

/// The one button family in Rune. Every bordered control (buttons, toggles,
/// menus) picks a role; size, radius, type and spacing are shared.
///
/// - `primary`: the single main action of a group, filled with the accent.
/// - `secondary`: everything else, a light accent tint with an accent label.
/// - `plain`: low-emphasis icon actions (copy, refresh, clear); no chrome
///   until hovered, same hit target as the others.
/// - `destructive`: deletes and other irreversible actions, danger tint.
///
/// A selected or "on" control (toggles, the active filter) is a secondary
/// control with an accent tint, the same treatment selected tabs and sidebar
/// rows use.
enum RuneButtonRole: Sendable, Equatable {
    case primary
    case secondary
    case plain
    case destructive
}

struct RuneToolbarControlColors {
    let foreground: Color
    let background: Color
    let stroke: Color
    /// The opaque colour the label is read against (the fill, or the inset
    /// surface under a resting plain control).
    let contrastSurface: Color

    init(palette: RuneThemePalette, tint: Color? = nil, isEnabled: Bool = true,
         isSelected: Bool = false, isPressed: Bool = false, isHovered: Bool = false,
         isProminent: Bool = false, role: RuneButtonRole? = nil) {
        let role = role ?? (isProminent ? .primary : tint == nil ? .secondary : .destructive)
        let base = RuneThemeContrast.RGB(NSColor(palette.inset))
        let accent = tint ?? palette.accentFill
        if role == .primary && isEnabled {
            let fill = RuneThemeContrast.RGB(NSColor(accent)).over(base)
            let pressed = isPressed ? RuneThemeContrast.RGB(NSColor.black).over(fill, opacity: 0.12) : fill
            background = pressed.color
            contrastSurface = background
            foreground = RuneThemeContrast.onFill(background)
            stroke = Color.clear
            return
        }
        // Secondary controls carry the accent as a light tint, the same family
        // as primary fills and selection, so every action reads as a control.
        let isTinted = isEnabled && (isSelected || role == .destructive || role == .secondary || (tint != nil && role != .plain))
        let fillSource = isTinted ? accent : palette.foreground
        let opacity: Double
        if !isEnabled {
            opacity = role == .plain ? 0 : 0.035
        } else if role == .plain {
            opacity = isPressed ? 0.14 : isSelected ? 0.14 : isHovered ? 0.08 : 0
        } else if isTinted {
            opacity = isPressed ? 0.20 : isSelected ? 0.16 : isHovered ? 0.11 : 0.07
        } else {
            opacity = isPressed ? 0.15 : isHovered ? 0.10 : 0.06
        }
        let blended = RuneThemeContrast.RGB(NSColor(fillSource)).over(base, opacity: opacity).color
        // A resting plain control has no chrome at all; its ink is still
        // checked against the surface it sits on.
        background = opacity == 0 ? Color.clear : blended
        contrastSurface = blended
        let ink: Color
        if !isEnabled {
            ink = palette.mutedText
        } else if isTinted {
            ink = accent
        } else if role == .plain {
            ink = isHovered || isPressed ? palette.foreground : palette.secondaryText
        } else {
            ink = palette.foreground
        }
        // Evaluate the actual control fill, so the theme's accent doesn't
        // get washed out to satisfy unrelated editor/selection backgrounds.
        foreground = Color(nsColor: RuneThemeContrast.readable(NSColor(ink), over: [NSColor(blended)]))
        if role == .plain || !isEnabled {
            stroke = role == .plain ? Color.clear : palette.stroke.opacity(0.30)
        } else if isTinted {
            stroke = accent.opacity(isSelected ? 0.55 : isHovered ? 0.40 : 0.24)
        } else {
            stroke = palette.stroke.opacity(isHovered ? 0.95 : 0.65)
        }
    }
}

/// Toolbar labels own their visible surface, so buttons, toggles and menus
/// use the same height rather than only sharing an outer layout frame.
struct RuneToolbarControlSurface: ViewModifier {
    var isSelected = false
    var isPressed = false
    var isProminent = false
    var isIconOnly = false
    var fillsWidth = false
    var tint: Color? = nil
    var role: RuneButtonRole? = nil
    @Environment(\.isEnabled) private var isEnabled
    @Environment(\.runeThemePalette) private var palette
    @Environment(\.runeInterfaceFontSize) private var fontSize
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @State private var isHovered = false

    private var resolvedRole: RuneButtonRole {
        role ?? (isProminent ? .primary : tint == nil ? .secondary : .destructive)
    }
    private var accent: Color { tint ?? palette?.accentFill ?? Color(nsColor: RuneThemeContrast.nativeInk(.controlAccentColor)) }
    private var themedColors: RuneToolbarControlColors? {
        palette.map {
            RuneToolbarControlColors(palette: $0, tint: tint, isEnabled: isEnabled,
                                     isSelected: isSelected, isPressed: isPressed,
                                     isHovered: isHovered, isProminent: isProminent, role: resolvedRole)
        }
    }
    private var height: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight
            : RuneControlSize.large.height(interfaceFontSize: fontSize)
    }

    func body(content: Content) -> some View {
        content
            .runeInterfaceFont(relativeSize: RuneControlSize.large.labelRelativeSize, weight: .medium)
            .imageScale(.medium)
            .lineLimit(1)
            .fixedSize(horizontal: !fillsWidth, vertical: true)
            .frame(width: isIconOnly ? height - 2 * RuneUILayoutMetrics.inspectorControlContentInset : nil)
            .padding(.horizontal, RuneUILayoutMetrics.inspectorControlContentInset)
            .frame(maxWidth: fillsWidth ? .infinity : nil)
            .frame(minWidth: height, minHeight: height)
            .foregroundStyle(foreground)
            .tint(foreground)
            // Menus draw their label through AppKit, which takes its font
            // from the control size; keep it on the same 13 pt label size.
            .controlSize(RuneControlSize.regular.nativeControlSize(
                interfaceFontSize: fontSize,
                isAccessibilitySize: dynamicTypeSize.isAccessibilitySize
            ))
            .background {
                RoundedRectangle(cornerRadius: RuneRadius.control, style: .continuous)
                    .fill(background)
            }
            .overlay {
                RoundedRectangle(cornerRadius: RuneRadius.control, style: .continuous)
                    .strokeBorder(stroke, lineWidth: 1)
            }
            .contentShape(RoundedRectangle(cornerRadius: RuneRadius.control, style: .continuous))
            .onHover { isHovered = $0 }
            // Colour-only transitions: hover and press fade instead of
            // snapping, and nothing moves.
            .animation(.easeOut(duration: 0.12), value: isHovered)
            .animation(.easeOut(duration: 0.08), value: isPressed)
    }

    private var foreground: Color {
        if let themedColors { return themedColors.foreground }
        if !isEnabled { return Color(nsColor: RuneThemeContrast.nativeInk(.tertiaryLabelColor)) }
        switch resolvedRole {
        case .primary: return RuneThemeContrast.onFill(background)
        case .destructive: return accent
        case .plain: return isHovered ? Color.primary : Color.secondary
        case .secondary: return accent
        }
    }

    private var background: Color {
        if let themedColors { return themedColors.background }
        if resolvedRole == .primary && isEnabled { return accent }
        if isSelected || resolvedRole == .destructive || resolvedRole == .secondary {
            return accent.opacity(isPressed ? 0.20 : isSelected ? 0.16 : isHovered ? 0.11 : 0.07)
        }
        if resolvedRole == .plain { return Color.primary.opacity(isPressed ? 0.14 : isHovered ? 0.08 : 0) }
        return Color.primary.opacity(isEnabled && isPressed ? 0.14 : isEnabled && isHovered ? 0.09 : 0.055)
    }

    private var stroke: Color {
        if let themedColors { return themedColors.stroke }
        switch resolvedRole {
        case .primary, .plain: return Color.clear
        case .destructive: return accent.opacity(0.35)
        case .secondary:
            return accent.opacity(isSelected ? 0.55 : isHovered ? 0.40 : 0.24)
        }
    }
}

struct RuneToolbarButtonStyle: ButtonStyle {
    var isSelected = false
    var isProminent = false
    var isIconOnly = false
    var tint: Color? = nil
    var role: RuneButtonRole? = nil
    @Environment(\.runeThemePalette) private var palette

    func makeBody(configuration: Configuration) -> some View {
        let isDestructive = configuration.role == .destructive || role == .destructive
        return configuration.label.modifier(RuneToolbarControlSurface(
            isSelected: isSelected,
            isPressed: configuration.isPressed,
            isProminent: isProminent,
            isIconOnly: isIconOnly,
            tint: isDestructive ? RuneSemanticColorRole.danger.color(in: palette) : tint,
            role: isDestructive ? .destructive : isProminent ? .primary : role
        ))
    }
}

/// The plain role at the regular (24 pt) size, for dense places: header and
/// row accessories, find bars, tab strips and section headers. Same colours
/// and hover chrome as `RuneToolbarButtonStyle(role: .plain)`; selected
/// controls get the shared accent tint.
struct RuneCompactButtonStyle: ButtonStyle {
    var isSelected = false
    var tint: Color? = nil
    /// `.plain` for accessories; `.secondary` for a compact action that should
    /// read as a button (the sidebar's Add Cluster).
    var role: RuneButtonRole = .plain

    func makeBody(configuration: Configuration) -> some View {
        RuneCompactButtonBody(configuration: configuration, isSelected: isSelected, tint: tint, role: role)
    }
}

private struct RuneCompactButtonBody: View {
    let configuration: ButtonStyleConfiguration
    let isSelected: Bool
    let tint: Color?
    let role: RuneButtonRole
    @Environment(\.isEnabled) private var isEnabled
    @Environment(\.runeThemePalette) private var palette
    @Environment(\.runeInterfaceFontSize) private var fontSize
    @State private var isHovered = false

    var body: some View {
        let colors = palette.map {
            RuneToolbarControlColors(palette: $0, tint: tint, isEnabled: isEnabled, isSelected: isSelected,
                                     isPressed: configuration.isPressed, isHovered: isHovered, role: role)
        }
        let height = RuneControlSize.regular.height(interfaceFontSize: fontSize)
        configuration.label
            .runeInterfaceFont(relativeSize: -1, weight: .medium)
            .lineLimit(1)
            .frame(minWidth: height, minHeight: height)
            .foregroundStyle(colors?.foreground ?? fallbackForeground)
            .background {
                RoundedRectangle(cornerRadius: RuneRadius.chip, style: .continuous)
                    .fill(colors?.background ?? fallbackBackground)
            }
            .contentShape(RoundedRectangle(cornerRadius: RuneRadius.chip, style: .continuous))
            .onHover { isHovered = $0 }
            .animation(.easeOut(duration: 0.12), value: isHovered)
    }

    private var fallbackForeground: Color {
        if !isEnabled { return Color(nsColor: .tertiaryLabelColor) }
        if isSelected || role == .secondary { return tint ?? Color.accentColor }
        return isHovered ? Color.primary : Color.secondary
    }

    private var fallbackBackground: Color {
        if isSelected || role == .secondary {
            return (tint ?? Color.accentColor).opacity(configuration.isPressed ? 0.24 : isSelected ? 0.16 : 0.12)
        }
        return Color.primary.opacity(configuration.isPressed ? 0.14 : isHovered ? 0.08 : 0)
    }
}

/// Related actions joined into one 28 pt control, like AppKit's segmented
/// buttons: one surface, thin dividers, and each segment highlights on hover.
/// The bulk selection control ("3 selected | Deselect Visible | ×") uses it.
struct RuneToolbarButtonGroup<Content: View>: View {
    var isSelected = false
    @ViewBuilder let content: Content
    @Environment(\.runeThemePalette) private var palette
    @Environment(\.runeInterfaceFontSize) private var fontSize
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    var body: some View {
        let colors = palette.map { RuneToolbarControlColors(palette: $0, isSelected: isSelected) }
        let accent = palette?.accentFill ?? Color.accentColor
        let shape = RoundedRectangle(cornerRadius: RuneRadius.control, style: .continuous)
        HStack(spacing: 0) {
            content
        }
        .runeInterfaceFont(relativeSize: RuneControlSize.large.labelRelativeSize, weight: .medium)
        .lineLimit(1)
        .fixedSize()
        .frame(minHeight: height)
        .foregroundStyle(colors?.foreground ?? accent)
        .background(shape.fill(colors?.background ?? accent.opacity(isSelected ? 0.16 : 0.07)))
        .clipShape(shape)
        .overlay(shape.strokeBorder(colors?.stroke ?? accent.opacity(isSelected ? 0.55 : 0.24), lineWidth: 1))
        .environment(\.runeToolbarGroupHeight, height)
    }

    private var height: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight
            : RuneControlSize.large.height(interfaceFontSize: fontSize)
    }
}

/// A non-interactive segment of a `RuneToolbarButtonGroup`, such as a count.
struct RuneToolbarGroupLabel<Label: View>: View {
    @ViewBuilder let label: Label

    var body: some View {
        label.padding(.horizontal, RuneSpacing.xs)
    }
}

/// The thin divider between segments of a `RuneToolbarButtonGroup`.
struct RuneToolbarGroupDivider: View {
    @Environment(\.runeThemePalette) private var palette

    var body: some View {
        Rectangle()
            .fill((palette?.accentFill ?? Color.accentColor).opacity(0.30))
            .frame(width: 1, height: RuneSpacing.md)
            .accessibilityHidden(true)
    }
}

/// A button segment inside a `RuneToolbarButtonGroup`.
struct RuneToolbarSegmentButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        RuneToolbarSegmentButtonBody(configuration: configuration)
    }
}

private struct RuneToolbarSegmentButtonBody: View {
    let configuration: ButtonStyleConfiguration
    @Environment(\.isEnabled) private var isEnabled
    @Environment(\.runeThemePalette) private var palette
    @Environment(\.runeToolbarGroupHeight) private var height
    @State private var isHovered = false

    var body: some View {
        let accent = palette?.accentFill ?? Color.accentColor
        configuration.label
            .padding(.horizontal, RuneSpacing.inset)
            .frame(minWidth: height, minHeight: height)
            .opacity(isEnabled ? 1 : 0.45)
            .background(accent.opacity(configuration.isPressed ? 0.20 : isHovered && isEnabled ? 0.11 : 0))
            .contentShape(Rectangle())
            .onHover { isHovered = $0 }
            .animation(.easeOut(duration: 0.12), value: isHovered)
    }
}

private struct RuneToolbarGroupHeightKey: EnvironmentKey {
    static let defaultValue: CGFloat = RuneControlSize.large.baseHeight
}

extension EnvironmentValues {
    var runeToolbarGroupHeight: CGFloat {
        get { self[RuneToolbarGroupHeightKey.self] }
        set { self[RuneToolbarGroupHeightKey.self] = newValue }
    }
}

/// A label that always takes the size of its widest alternative, so a
/// control that switches title (Quick Edit and Done, Select and Deselect
/// Visible, Start and Stop) keeps its frame and never moves its neighbours.
struct RuneStableLabel: View {
    let title: String
    var systemImage: String? = nil
    let reserving: [String]

    init(_ title: String, systemImage: String? = nil, reserving: [String]) {
        self.title = title
        self.systemImage = systemImage
        self.reserving = reserving
    }

    var body: some View {
        ZStack {
            ForEach(reserving.filter { $0 != title }, id: \.self) { alternative in
                label(alternative)
                    .hidden()
                    .accessibilityHidden(true)
            }
            label(title)
        }
    }

    @ViewBuilder
    private func label(_ text: String) -> some View {
        if let systemImage {
            Label(text, systemImage: systemImage)
        } else {
            Text(text)
        }
    }
}

struct RuneToolbarToggleStyle: ToggleStyle {
    var isIconOnly = false

    func makeBody(configuration: Configuration) -> some View {
        Button { configuration.isOn.toggle() } label: { configuration.label }
            .buttonStyle(RuneToolbarButtonStyle(isSelected: configuration.isOn, isIconOnly: isIconOnly))
            .accessibilityValue(configuration.isOn ? "On" : "Off")
            .accessibilityAddTraits(configuration.isOn ? .isSelected : [])
    }
}

struct RuneToolbarMenu<Content: View, Label: View>: View {
    var fillsWidth = false
    @ViewBuilder let content: Content
    @ViewBuilder let label: Label
    @Environment(\.runeThemePalette) private var palette

    var body: some View {
        Menu {
            content
                .buttonStyle(.automatic)
                .toggleStyle(.automatic)
        } label: {
            label
        }
        .menuStyle(.borderlessButton)
        .menuIndicator(.visible)
        .frame(maxWidth: fillsWidth ? .infinity : nil)
        .modifier(RuneToolbarControlSurface(fillsWidth: fillsWidth))
        .fixedSize(horizontal: !fillsWidth, vertical: true)
    }
}

struct RuneToolbarPicker<Value: Hashable>: View {
    let title: String
    @Binding var selection: Value
    let options: [(value: Value, title: String)]

    private var selectedTitle: String { options.first { $0.value == selection }?.title ?? title }

    var body: some View {
        RuneToolbarMenu(fillsWidth: true) {
            Picker(title, selection: $selection) {
                ForEach(options.indices, id: \.self) { index in
                    Text(options[index].title).tag(options[index].value)
                }
            }.pickerStyle(.inline)
        } label: {
            Text(selectedTitle).frame(maxWidth: .infinity, alignment: .leading)
        }
        .accessibilityLabel(title)
        .accessibilityValue(selectedTitle)
    }
}

/// Native rounded field, used only where its row is made of 24 pt compact
/// controls: the inspector find bars, whose cursor regions are tied to the
/// native field.
struct RuneFormTextFieldStyle: TextFieldStyle {
    @Environment(\.runeInterfaceFontSize) private var fontSize
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    func _body(configuration: TextField<Self._Label>) -> some View {
        configuration
            .textFieldStyle(.roundedBorder)
            .font(.system(size: fontSize + RuneControlSize.regular.labelRelativeSize))
            .controlSize(RuneControlSize.regular.nativeControlSize(
                interfaceFontSize: fontSize,
                isAccessibilitySize: dynamicTypeSize.isAccessibilitySize
            ))
    }
}

/// The field for every toolbar, panel, form and Settings row. It has the
/// same chrome as the buttons beside it: 28 pt high, 8 pt radius, a 1 pt
/// accent border (stronger while focused) and the 13 pt label size, on the
/// inset surface. A click anywhere on it focuses the field.
struct RuneControlTextFieldStyle: TextFieldStyle {
    /// Room kept free at the trailing edge for an accessory drawn inside the
    /// field, such as a clear button.
    var trailingAccessoryWidth: CGFloat = 0

    func _body(configuration: TextField<Self._Label>) -> some View {
        configuration
            .textFieldStyle(.plain)
            .modifier(RuneControlFieldChrome(trailingAccessoryWidth: trailingAccessoryWidth))
    }
}

private struct RuneControlFieldChrome: ViewModifier {
    let trailingAccessoryWidth: CGFloat
    @FocusState private var isFocused: Bool
    @Environment(\.runeThemePalette) private var palette
    @Environment(\.runeInterfaceFontSize) private var fontSize
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.isEnabled) private var isEnabled

    func body(content: Content) -> some View {
        let shape = RoundedRectangle(cornerRadius: RuneRadius.control, style: .continuous)
        let accent = palette?.accentFill ?? Color.accentColor
        content
            .focused($isFocused)
            .runeInterfaceFont(relativeSize: RuneControlSize.large.labelRelativeSize)
            .foregroundStyle(.runePrimary)
            .padding(.leading, RuneSpacing.inset)
            .padding(.trailing, RuneSpacing.inset + trailingAccessoryWidth)
            .frame(maxWidth: .infinity, minHeight: height, alignment: .leading)
            .background(shape.fill(palette?.inset ?? Color(nsColor: .textBackgroundColor)))
            .overlay {
                shape
                    .strokeBorder(
                        isEnabled ? accent.opacity(isFocused ? 0.70 : 0.24) : (palette?.stroke ?? Color(nsColor: .separatorColor)).opacity(0.35),
                        lineWidth: isFocused ? 1.5 : 1
                    )
                    .allowsHitTesting(false)
            }
            .contentShape(shape)
            // A click in the padding focuses the field too; the field's own
            // clicks and accessibility are untouched.
            .simultaneousGesture(TapGesture().onEnded { isFocused = true })
    }

    private var height: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight
            : RuneControlSize.large.height(interfaceFontSize: fontSize)
    }
}
