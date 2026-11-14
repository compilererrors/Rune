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
    /// Titles this button switches between; the label keeps the widest one's
    /// size so a state change never moves the buttons beside it.
    let reservedTitles: [String]
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    init(
        _ title: String,
        systemImage: String,
        density: TerminalActionControlDensity,
        usesUniformWidth: Bool = true,
        reservedTitles: [String] = []
    ) {
        self.title = title
        self.systemImage = systemImage
        self.density = density
        self.usesUniformWidth = usesUniformWidth
        self.reservedTitles = reservedTitles
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

    /// The terminal cluster keeps fixed-width buttons, so it uses the small
    /// label size for every button in the row.
    private var label: some View {
        ZStack {
            ForEach(reservedTitles.filter { $0 != title }, id: \.self) { alternative in
                Label(alternative, systemImage: systemImage)
                    .hidden()
                    .accessibilityHidden(true)
            }
            Label(title, systemImage: systemImage)
        }
        .runeInterfaceFont(relativeSize: RuneControlSize.large.labelRelativeSize - 1, weight: .medium)
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
    /// Off where Clear lives in the surrounding overflow menu (Pod Shell).
    var showsClear = true
    /// Off where Connect sits on the terminal surface itself (Pod Shell).
    var showsPrimaryAction = true

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
            if showsPrimaryAction {
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
            }

            if showsClear {
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
        }
        .fixedSize(horizontal: true, vertical: false)
    }
}
