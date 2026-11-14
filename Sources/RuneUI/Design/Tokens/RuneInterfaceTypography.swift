import RuneCore
import SwiftUI

/// Maps Rune's existing font-size preference onto bounded macOS interface sizes.
/// Editor and terminal surfaces continue using the exact configured point size.
enum RuneInterfaceTypography {
    static let standardMenuFontSize: CGFloat = 13

    static func preferredMenuFontSize(configuredFontSize: Double) -> CGFloat {
        let normalizedFontSize = normalizedConfiguredFontSize(configuredFontSize)

        switch normalizedFontSize {
        case ..<13:
            return standardMenuFontSize
        case ..<15:
            return standardMenuFontSize + 1
        default:
            return standardMenuFontSize + 2
        }
    }

    static func effectiveMenuFontSize(
        configuredFontSize: Double,
        systemDynamicTypeSize: DynamicTypeSize
    ) -> CGFloat {
        max(
            preferredMenuFontSize(configuredFontSize: configuredFontSize),
            systemMenuFontSize(dynamicTypeSize: systemDynamicTypeSize)
        )
    }

    static func appKitMenuFontSize(
        systemSmallFontSize: CGFloat,
        interfaceMenuFontSize: CGFloat
    ) -> CGFloat {
        let increase = min(3, max(0, interfaceMenuFontSize - standardMenuFontSize))
        return systemSmallFontSize + increase
    }

    static func controlSize(
        interfaceMenuFontSize: CGFloat,
        compactBaseline: Bool
    ) -> ControlSize {
        if !compactBaseline {
            return interfaceMenuFontSize > standardMenuFontSize ? .large : .regular
        }
        if interfaceMenuFontSize > standardMenuFontSize + 1 {
            return .large
        }
        if interfaceMenuFontSize > standardMenuFontSize {
            return .regular
        }
        return .small
    }

    private static func systemMenuFontSize(dynamicTypeSize: DynamicTypeSize) -> CGFloat {
        if dynamicTypeSize.isAccessibilitySize || dynamicTypeSize >= .xxxLarge {
            return standardMenuFontSize + 3
        }
        if dynamicTypeSize >= .xxLarge {
            return standardMenuFontSize + 2
        }
        if dynamicTypeSize >= .xLarge {
            return standardMenuFontSize + 1
        }
        return standardMenuFontSize
    }

    private static func normalizedConfiguredFontSize(_ value: Double) -> Double {
        if value.isNaN {
            return RuneSettingsKeys.terminalFontSizeDefault
        }
        if value == .infinity {
            return RuneSettingsKeys.terminalFontSizeMaximum
        }
        if value == -.infinity {
            return RuneSettingsKeys.terminalFontSizeMinimum
        }
        return RuneSettingsKeys.clampedTerminalFontSize(value)
    }
}

private struct RuneInterfaceFontSizeKey: EnvironmentKey {
    static let defaultValue = RuneInterfaceTypography.standardMenuFontSize
}

extension EnvironmentValues {
    var runeInterfaceFontSize: CGFloat {
        get { self[RuneInterfaceFontSizeKey.self] }
        set { self[RuneInterfaceFontSizeKey.self] = newValue }
    }
}

private struct RuneInterfaceTypographyModifier: ViewModifier {
    let pointSize: CGFloat

    func body(content: Content) -> some View {
        content
            .environment(\.runeInterfaceFontSize, pointSize)
            .font(.system(size: pointSize))
    }
}

private struct RuneInterfaceFontModifier: ViewModifier {
    @Environment(\.runeInterfaceFontSize) private var pointSize
    let relativeSize: CGFloat
    let weight: Font.Weight
    let design: Font.Design

    func body(content: Content) -> some View {
        content.font(
            .system(
                size: max(9, pointSize + relativeSize),
                weight: weight,
                design: design
            )
        )
    }
}

private struct RuneInterfaceControlSizeModifier: ViewModifier {
    @Environment(\.runeInterfaceFontSize) private var pointSize
    let compactBaseline: Bool

    func body(content: Content) -> some View {
        content.controlSize(
            RuneInterfaceTypography.controlSize(
                interfaceMenuFontSize: pointSize,
                compactBaseline: compactBaseline
            )
        )
    }
}

extension View {
    func runeInterfaceTypography(
        configuredFontSize: Double,
        systemDynamicTypeSize: DynamicTypeSize
    ) -> some View {
        modifier(
            RuneInterfaceTypographyModifier(
                pointSize: RuneInterfaceTypography.effectiveMenuFontSize(
                    configuredFontSize: configuredFontSize,
                    systemDynamicTypeSize: systemDynamicTypeSize
                )
            )
        )
    }

    func runeInterfaceFont(
        relativeSize: CGFloat = 0,
        weight: Font.Weight = .regular,
        design: Font.Design = .default
    ) -> some View {
        modifier(
            RuneInterfaceFontModifier(
                relativeSize: relativeSize,
                weight: weight,
                design: design
            )
        )
    }

    func runeInterfaceControlSize(compactBaseline: Bool = false) -> some View {
        modifier(
            RuneInterfaceControlSizeModifier(
                compactBaseline: compactBaseline
            )
        )
    }
}
