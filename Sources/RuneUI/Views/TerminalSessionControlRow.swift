import SwiftUI
import RuneCore

enum TerminalActionLayoutMetrics {
    static let buttonWidth: CGFloat = 112
    static let spacing: CGFloat = RuneUILayoutMetrics.inspectorToolbarControlSpacing
    static let compactMinimumHeight: CGFloat = RuneUILayoutMetrics.inspectorToolbarControlMinHeight
    static let regularMinimumHeight: CGFloat = RuneUILayoutMetrics.inspectorToolbarControlMinHeight
    static let accessibilityMinimumHeight: CGFloat = RuneAdaptiveToolbarMetrics.accessibilityMinimumRowHeight
}

enum TerminalActionControlDensity {
    case compact
    case regular

    var controlSize: ControlSize {
        switch self {
        case .compact:
            return .small
        case .regular:
            return .regular
        }
    }

    var labelMinimumHeight: CGFloat {
        switch self {
        case .compact:
            return 22
        case .regular:
            return 22
        }
    }

    var minimumHeight: CGFloat {
        switch self {
        case .compact:
            return TerminalActionLayoutMetrics.compactMinimumHeight
        case .regular:
            return TerminalActionLayoutMetrics.regularMinimumHeight
        }
    }
}

struct TerminalActionButtonLabel: View {
    let title: String
    let systemImage: String
    let density: TerminalActionControlDensity
    let usesUniformWidth: Bool
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    init(
        _ title: String,
        systemImage: String,
        density: TerminalActionControlDensity,
        usesUniformWidth: Bool = true
    ) {
        self.title = title
        self.systemImage = systemImage
        self.density = density
        self.usesUniformWidth = usesUniformWidth
    }

    @ViewBuilder
    var body: some View {
        if dynamicTypeSize.isAccessibilitySize, usesUniformWidth {
            label
                .frame(
                    minWidth: TerminalActionLayoutMetrics.buttonWidth,
                    minHeight: TerminalActionLayoutMetrics.accessibilityMinimumHeight
                )
        } else if usesUniformWidth {
            label
                .frame(
                    minWidth: TerminalActionLayoutMetrics.buttonWidth - 2 * RuneUILayoutMetrics.inspectorControlContentInset,
                    minHeight: density.labelMinimumHeight
                )
        } else {
            label
        }
    }

    private var label: some View {
        Label(title, systemImage: systemImage)
            .lineLimit(1)
    }
}

private struct TerminalActionControlModifier: ViewModifier {
    let density: TerminalActionControlDensity
    let usesUniformWidth: Bool
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    @ViewBuilder
    func body(content: Content) -> some View {
        if dynamicTypeSize.isAccessibilitySize {
            content
                .controlSize(.large)
                .frame(minHeight: TerminalActionLayoutMetrics.accessibilityMinimumHeight)
        } else if usesUniformWidth {
            content
                .controlSize(density.controlSize)
                .frame(minWidth: TerminalActionLayoutMetrics.buttonWidth)
                .frame(minHeight: density.minimumHeight)
        } else {
            content
                .controlSize(density.controlSize)
                .frame(minHeight: density.minimumHeight)
        }
    }
}

extension View {
    func terminalActionControl(
        _ density: TerminalActionControlDensity,
        usesUniformWidth: Bool = true
    ) -> some View {
        modifier(TerminalActionControlModifier(
            density: density,
            usesUniformWidth: usesUniformWidth
        ))
    }
}

struct TerminalSessionControlRow: View {
    let title: String
    let systemImage: String
    let pods: [PodSummary]
    let terminalSessions: [PodTerminalSession]
    let primaryActionTitle: String
    let primaryActionSystemImage: String
    let isPrimaryActionDisabled: Bool
    let isClearDisabled: Bool
    let isFavoritePod: (PodSummary) -> Bool
    let onToggleFavoritePod: (PodSummary) -> Void
    let onPrimaryAction: () -> Void
    let onClear: () -> Void
    @Binding var selection: String

    var body: some View {
        TerminalPodControlLayout(
            accessibilityLabel: "\(title) controls",
            title: title,
            systemImage: systemImage
        ) { width in
            TerminalPodSelectorControl(
                title: title,
                pods: pods,
                terminalSessions: terminalSessions,
                width: width,
                isFavoritePod: isFavoritePod,
                onToggleFavoritePod: onToggleFavoritePod,
                selection: $selection
            )
        } actions: {
            actionButtons
        }
    }

    private var actionButtons: some View {
        HStack(spacing: TerminalActionLayoutMetrics.spacing) {
            Button(action: onPrimaryAction) {
                TerminalActionButtonLabel(
                    primaryActionTitle,
                    systemImage: primaryActionSystemImage,
                    density: .compact
                )
            }
            .buttonStyle(RuneToolbarButtonStyle())
            .terminalActionControl(.compact)
            .disabled(isPrimaryActionDisabled)
            .help(primaryActionTitle)
            .accessibilityIdentifier("terminal-session-primary-action")

            Button(action: onClear) {
                TerminalActionButtonLabel(
                    "Clear",
                    systemImage: "xmark.circle",
                    density: .compact
                )
            }
            .buttonStyle(RuneToolbarButtonStyle())
            .terminalActionControl(.compact)
            .disabled(isClearDisabled)
            .help("Clear active terminal output")
            .keyboardShortcut("k", modifiers: [.command])
            .accessibilityIdentifier("terminal-session-clear")
        }
        .fixedSize(horizontal: true, vertical: false)
    }
}
