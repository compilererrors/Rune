import SwiftUI

enum RuneContentState: Sendable, Equatable {
    case loading(title: String, message: String? = nil)
    case retryableError(title: String, message: String)
    case filteredEmpty(title: String, message: String)
    case empty(title: String, message: String)
    case unselected(title: String, message: String)

    var title: String {
        switch self {
        case let .loading(title, _),
             let .retryableError(title, _),
             let .filteredEmpty(title, _),
             let .empty(title, _),
             let .unselected(title, _):
            return title
        }
    }

    var message: String? {
        switch self {
        case let .loading(_, message):
            return message
        case let .retryableError(_, message),
             let .filteredEmpty(_, message),
             let .empty(_, message),
             let .unselected(_, message):
            return message
        }
    }

    var systemImage: String? {
        switch self {
        case .loading: return nil
        case .retryableError: return "exclamationmark.triangle"
        case .filteredEmpty: return "line.3.horizontal.decrease.circle"
        case .empty: return "tray"
        case .unselected: return "cursorarrow.click.2"
        }
    }

    var accessibilitySummary: String {
        let prefix: String
        switch self {
        case .loading: prefix = "Loading"
        case .retryableError: prefix = "Error"
        case .filteredEmpty: prefix = "No filtered results"
        case .empty: prefix = "Empty"
        case .unselected: prefix = "Nothing selected"
        }
        return [prefix, title, message]
            .compactMap { $0?.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .joined(separator: ". ")
    }

    var isLoading: Bool {
        if case .loading = self { return true }
        return false
    }
}

enum RuneContentStateVariant: Sendable, Equatable {
    case centered
    case card
    case pane
    case inline

    var minimumHeight: CGFloat {
        switch self {
        case .centered: return 160
        case .card, .pane: return 120
        case .inline: return 36
        }
    }
}

enum RunePaneContentStateStyle: Sendable, Equatable {
    case card
    case plain

    var variant: RuneContentStateVariant {
        switch self {
        case .card: return .card
        case .plain: return .pane
        }
    }
}

struct RuneContentStateAction {
    let title: String
    let systemImage: String?
    let perform: () -> Void

    init(
        _ title: String,
        systemImage: String? = nil,
        perform: @escaping () -> Void
    ) {
        self.title = title
        self.systemImage = systemImage
        self.perform = perform
    }
}

struct RuneContentStateView: View {
    let state: RuneContentState
    let variant: RuneContentStateVariant
    let action: RuneContentStateAction?
    let graphicSystemImage: String?
    @Environment(\.runeThemePalette) private var runeThemePalette

    init(
        _ state: RuneContentState,
        variant: RuneContentStateVariant = .centered,
        graphicSystemImage: String? = nil,
        action: RuneContentStateAction? = nil
    ) {
        self.state = state
        self.variant = variant
        self.graphicSystemImage = graphicSystemImage
        self.action = action
    }

    @ViewBuilder
    var body: some View {
        switch variant {
        case .centered:
            verticalContent
                .padding(20)
                .frame(
                    maxWidth: .infinity,
                    minHeight: variant.minimumHeight,
                    maxHeight: .infinity,
                    alignment: .center
                )
        case .card:
            verticalContent
                .padding(20)
                .frame(maxWidth: .infinity, minHeight: variant.minimumHeight, alignment: .center)
                .background(RuneSurfaceBackground(kind: .inset))
        case .pane:
            verticalContent
                .padding(20)
                .frame(maxWidth: .infinity, minHeight: variant.minimumHeight, alignment: .center)
        case .inline:
            inlineContent
                .padding(.vertical, 6)
                .frame(maxWidth: .infinity, minHeight: variant.minimumHeight, alignment: .leading)
        }
    }

    private var verticalContent: some View {
        VStack(spacing: 10) {
            stateGraphic(size: 22)

            stateText(alignment: .center)

            if let action {
                actionButton(action)
            }
        }
        .multilineTextAlignment(.center)
        .accessibilityElement(children: .contain)
    }

    private var inlineContent: some View {
        HStack(alignment: .center, spacing: 10) {
            stateGraphic(size: 16)

            stateText(alignment: .leading)

            Spacer(minLength: 8)

            if let action {
                actionButton(action)
            }
        }
        .accessibilityElement(children: .contain)
    }

    @ViewBuilder
    private func stateGraphic(size: CGFloat) -> some View {
        if state.isLoading {
            ProgressView()
                .controlSize(variant == .inline ? .small : .regular)
                .accessibilityHidden(true)
        } else if let systemImage = graphicSystemImage ?? state.systemImage {
            Image(systemName: systemImage)
                .font(.system(size: size, weight: .semibold))
                .foregroundStyle(graphicColor)
                .frame(width: size, height: size)
                .accessibilityHidden(true)
        }
    }

    private func stateText(alignment: TextAlignment) -> some View {
        VStack(alignment: alignment == .center ? .center : .leading, spacing: 4) {
            Text(state.title)
                .font(variant == .inline ? .caption.weight(.semibold) : .body.weight(.semibold))
                .foregroundStyle(.runePrimary)

            if let message = state.message,
               !message.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                Text(message)
                    .font(variant == .inline ? .caption2 : .footnote)
                    .foregroundStyle(.runeSecondary)
                    .fixedSize(horizontal: false, vertical: true)
                    .textSelection(.enabled)
            }
        }
        .multilineTextAlignment(alignment)
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(state.accessibilitySummary)
    }

    private func actionButton(_ action: RuneContentStateAction) -> some View {
        Button(action: action.perform) {
            if let systemImage = action.systemImage {
                Label(action.title, systemImage: systemImage)
            } else {
                Text(action.title)
            }
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .controlSize(variant == .inline ? .small : .regular)
    }

    private var graphicColor: Color {
        switch state {
        case .retryableError: return RuneSemanticColorRole.danger.color(in: runeThemePalette)
        case .loading: return runeThemePalette?.accent ?? .accentColor
        case .filteredEmpty, .empty, .unselected: return .secondary
        }
    }
}

/// Keeps transient pane states close to the controls they explain instead of centering them
/// in the full window height. Editor and log replacement states continue using `.centered`.
struct RunePaneTopLayout<Content: View>: View {
    @ViewBuilder let content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    var body: some View {
        VStack(spacing: 0) {
            content
            Spacer(minLength: 0)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    }
}

struct RunePaneContentStateView: View {
    let state: RuneContentState
    let style: RunePaneContentStateStyle
    let graphicSystemImage: String?
    let action: RuneContentStateAction?

    init(
        _ state: RuneContentState,
        style: RunePaneContentStateStyle = .card,
        graphicSystemImage: String? = nil,
        action: RuneContentStateAction? = nil
    ) {
        self.state = state
        self.style = style
        self.graphicSystemImage = graphicSystemImage
        self.action = action
    }

    var body: some View {
        RunePaneTopLayout {
            RuneContentStateView(
                state,
                variant: style.variant,
                graphicSystemImage: graphicSystemImage,
                action: action
            )
        }
        .accessibilityIdentifier("rune.pane.content-state")
    }
}
