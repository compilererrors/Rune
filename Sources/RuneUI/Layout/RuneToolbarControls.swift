import AppKit
import SwiftUI

struct RuneToolbarControlColors {
    let foreground: Color
    let background: Color

    init(palette: RuneThemePalette, tint: Color? = nil, isEnabled: Bool = true,
         isSelected: Bool = false, isPressed: Bool = false, isHovered: Bool = false,
         isProminent: Bool = false) {
        let accent = tint ?? palette.accentFill
        let base = RuneThemeContrast.RGB(NSColor(palette.inset))
        let opacity = isPressed ? 0.20 : isSelected ? 0.16 : isHovered ? 0.11 : 0.07
        if isProminent && isEnabled {
            background = RuneThemeContrast.RGB(NSColor(accent)).over(base).color
            foreground = RuneThemeContrast.onFill(background)
        } else {
            let fill = isEnabled ? accent : palette.foreground
            background = RuneThemeContrast.RGB(NSColor(fill))
                .over(base, opacity: isEnabled ? opacity : 0.045).color
            // Evaluate the actual control fill, so the theme's accent doesn't
            // get washed out to satisfy unrelated editor/selection backgrounds.
            foreground = Color(nsColor: RuneThemeContrast.readable(
                NSColor(isEnabled ? accent : palette.mutedText), over: [NSColor(background)]
            ))
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
    @Environment(\.isEnabled) private var isEnabled
    @Environment(\.runeThemePalette) private var palette
    @Environment(\.runeInterfaceFontSize) private var fontSize
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @State private var isHovered = false

    private var accent: Color { tint ?? palette?.accentFill ?? Color(nsColor: RuneThemeContrast.nativeInk(.controlAccentColor)) }
    private var themedColors: RuneToolbarControlColors? {
        palette.map {
            RuneToolbarControlColors(palette: $0, tint: tint, isEnabled: isEnabled,
                                     isSelected: isSelected, isPressed: isPressed,
                                     isHovered: isHovered, isProminent: isProminent)
        }
    }
    private var height: CGFloat {
        max(
            dynamicTypeSize.isAccessibilitySize
                ? RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight
                : RuneUILayoutMetrics.inspectorToolbarControlMinHeight,
            fontSize + 16
        )
    }

    func body(content: Content) -> some View {
        content
            .runeInterfaceFont(relativeSize: -1, weight: .medium)
            .lineLimit(1)
            .fixedSize(horizontal: !fillsWidth, vertical: true)
            .frame(width: isIconOnly ? height - 2 * RuneUILayoutMetrics.inspectorControlContentInset : nil)
            .padding(.horizontal, RuneUILayoutMetrics.inspectorControlContentInset)
            .frame(maxWidth: fillsWidth ? .infinity : nil)
            .frame(minWidth: height, minHeight: height)
            .foregroundStyle(foreground)
            .tint(foreground)
            .background {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius)
                    .fill(background)
            }
            .overlay {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius)
                    .strokeBorder(
                        isEnabled && palette != nil ? accent.opacity(isSelected ? 0.55 : isHovered ? 0.40 : 0.24)
                            : (palette?.stroke ?? Color(nsColor: .separatorColor)).opacity(0.35),
                        lineWidth: 1
                    )
            }
            .contentShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius))
            .onHover { isHovered = $0 }
    }

    private var foreground: Color {
        if let themedColors { return themedColors.foreground }
        if !isEnabled { return palette?.mutedText ?? Color(nsColor: RuneThemeContrast.nativeInk(.secondaryLabelColor)) }
        if isProminent {
            return RuneThemeContrast.onFill(background)
        }
        return accent
    }

    private var background: Color {
        if let themedColors { return themedColors.background }
        if isProminent && isEnabled { return accent }
        if isSelected { return accent.opacity(isPressed ? 0.24 : 0.12) }
        return (palette?.foreground ?? Color.primary)
            .opacity(isEnabled && isPressed ? 0.14 : isEnabled && isHovered ? 0.09 : 0.055)
    }
}

struct RuneToolbarButtonStyle: ButtonStyle {
    var isSelected = false
    var isProminent = false
    var isIconOnly = false
    var tint: Color? = nil
    @Environment(\.runeThemePalette) private var palette

    func makeBody(configuration: Configuration) -> some View {
        configuration.label.modifier(RuneToolbarControlSurface(
            isSelected: isSelected,
            isPressed: configuration.isPressed,
            isProminent: isProminent,
            isIconOnly: isIconOnly,
            tint: configuration.role == .destructive ? RuneSemanticColorRole.danger.color(in: palette) : tint
        ))
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
        .tint(palette?.accentFill ?? .accentColor)
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

struct RuneControlTextFieldStyle: TextFieldStyle {
    @Environment(\.runeThemePalette) private var palette
    @Environment(\.runeInterfaceFontSize) private var fontSize
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    func _body(configuration: TextField<Self._Label>) -> some View {
        configuration
            .textFieldStyle(.plain)
            .foregroundStyle(.runePrimary)
            .padding(.horizontal, RuneUILayoutMetrics.inspectorControlContentInset)
            .frame(minHeight: max(dynamicTypeSize.isAccessibilitySize
                ? RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight
                : RuneUILayoutMetrics.inspectorToolbarControlMinHeight, fontSize + 16))
            .background {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius)
                    .fill(palette?.inset ?? Color(nsColor: .textBackgroundColor))
            }
            .overlay {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius)
                    .strokeBorder((palette?.stroke ?? Color(nsColor: .separatorColor)).opacity(0.5), lineWidth: 1)
                    .allowsHitTesting(false)
            }
    }
}
