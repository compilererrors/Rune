import SwiftUI

enum RuneDisclosureMetrics {
    static let headerMinimumHeight: CGFloat = 32
    static let chevronLayoutWidth: CGFloat = 14
    static let chevronGlyphSize: CGFloat = 12
    static let headerSpacing: CGFloat = 6
}

enum RuneDisclosureNavigation {
    static func shouldToggle(
        for direction: MoveCommandDirection,
        isExpanded: Bool,
        isEnabled: Bool = true
    ) -> Bool {
        guard isEnabled else { return false }
        switch direction {
        case .left:
            return isExpanded
        case .right:
            return !isExpanded
        case .up, .down:
            return false
        @unknown default:
            return false
        }
    }
}

/// A full-width disclosure control with one predictable hit target and accessibility state.
struct RuneDisclosureRow<Label: View>: View {
    let accessibilityLabel: String
    let isExpanded: Bool
    let isEnabled: Bool
    let fillsAvailableWidth: Bool
    let helpText: String?
    let accessibilityIdentifier: String?
    let action: () -> Void
    @ViewBuilder let label: Label

    @State private var isHovering = false

    init(
        _ accessibilityLabel: String,
        isExpanded: Bool,
        isEnabled: Bool = true,
        fillsAvailableWidth: Bool = true,
        help: String? = nil,
        accessibilityIdentifier: String? = nil,
        action: @escaping () -> Void,
        @ViewBuilder label: () -> Label
    ) {
        self.accessibilityLabel = accessibilityLabel
        self.isExpanded = isExpanded
        self.isEnabled = isEnabled
        self.fillsAvailableWidth = fillsAvailableWidth
        helpText = help
        self.accessibilityIdentifier = accessibilityIdentifier
        self.action = action
        self.label = label()
    }

    var body: some View {
        Button(action: action) {
            HStack(alignment: .center, spacing: RuneDisclosureMetrics.headerSpacing) {
                Image(systemName: isExpanded ? "chevron.down" : "chevron.right")
                    .font(.system(size: RuneDisclosureMetrics.chevronGlyphSize, weight: .semibold))
                    .foregroundStyle(.secondary)
                    .frame(
                        width: RuneDisclosureMetrics.chevronLayoutWidth,
                        height: RuneDisclosureMetrics.headerMinimumHeight
                    )
                    .accessibilityHidden(true)

                label
            }
            .frame(
                maxWidth: fillsAvailableWidth ? .infinity : nil,
                minHeight: RuneDisclosureMetrics.headerMinimumHeight,
                alignment: .leading
            )
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .background {
            RoundedRectangle(
                cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius,
                style: .continuous
            )
            .fill(isHovering && isEnabled ? Color.primary.opacity(0.055) : Color.clear)
        }
        .contentShape(Rectangle())
        .disabled(!isEnabled)
        .opacity(isEnabled ? 1 : 0.55)
        .onHover { isHovering = $0 }
        .onKeyPress(.leftArrow) {
            handleArrowKey(.left)
        }
        .onKeyPress(.rightArrow) {
            handleArrowKey(.right)
        }
        .accessibilityLabel(accessibilityLabel)
        .accessibilityValue(isExpanded ? "Expanded" : "Collapsed")
        .accessibilityHint(isExpanded ? "Press to collapse this section" : "Press to expand this section")
        .modifier(RuneDisclosureOptionalHelpModifier(helpText: helpText))
        .modifier(RuneDisclosureOptionalIdentifierModifier(identifier: accessibilityIdentifier))
    }

    private func handleArrowKey(_ direction: MoveCommandDirection) -> KeyPress.Result {
        guard RuneDisclosureNavigation.shouldToggle(
            for: direction,
            isExpanded: isExpanded,
            isEnabled: isEnabled
        ) else {
            return .ignored
        }
        action()
        return .handled
    }
}

/// A lightweight disclosure container. Callers retain ownership of surfaces and side effects.
struct RuneDisclosureSection<Label: View, Content: View>: View {
    let accessibilityLabel: String
    let helpText: String?
    let accessibilityIdentifier: String?
    @ViewBuilder let label: Label
    @ViewBuilder let content: Content

    private let externalExpansion: Binding<Bool>?
    @State private var internalExpansion: Bool
    @Environment(\.accessibilityReduceMotion) private var accessibilityReduceMotion

    init(
        _ accessibilityLabel: String,
        isExpanded: Binding<Bool>? = nil,
        initiallyExpanded: Bool = false,
        help: String? = nil,
        accessibilityIdentifier: String? = nil,
        @ViewBuilder content: () -> Content,
        @ViewBuilder label: () -> Label
    ) {
        self.accessibilityLabel = accessibilityLabel
        helpText = help
        self.accessibilityIdentifier = accessibilityIdentifier
        externalExpansion = isExpanded
        _internalExpansion = State(initialValue: initiallyExpanded)
        self.content = content()
        self.label = label()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            RuneDisclosureRow(
                accessibilityLabel,
                isExpanded: isExpanded,
                help: helpText ?? defaultHelp,
                accessibilityIdentifier: accessibilityIdentifier,
                action: toggleExpansion
            ) {
                label
            }

            if isExpanded {
                content
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var isExpanded: Bool {
        externalExpansion?.wrappedValue ?? internalExpansion
    }

    private var defaultHelp: String {
        isExpanded ? "Collapse \(accessibilityLabel)" : "Expand \(accessibilityLabel)"
    }

    private func toggleExpansion() {
        let update = {
            if let externalExpansion {
                externalExpansion.wrappedValue.toggle()
            } else {
                internalExpansion.toggle()
            }
        }

        if accessibilityReduceMotion {
            update()
        } else {
            withAnimation(.snappy(duration: 0.16), update)
        }
    }
}

private struct RuneDisclosureOptionalHelpModifier: ViewModifier {
    let helpText: String?

    @ViewBuilder
    func body(content: Content) -> some View {
        if let helpText {
            content.help(helpText)
        } else {
            content
        }
    }
}

private struct RuneDisclosureOptionalIdentifierModifier: ViewModifier {
    let identifier: String?

    @ViewBuilder
    func body(content: Content) -> some View {
        if let identifier {
            content.accessibilityIdentifier(identifier)
        } else {
            content
        }
    }
}
