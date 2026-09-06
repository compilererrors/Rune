import SwiftUI

enum RuneGlassPaneRole {
    case window
    case sidebar
    case content
    case inspector

    var material: Material {
        switch self {
        case .window:
            return .ultraThinMaterial
        case .sidebar:
            return .thinMaterial
        case .content, .inspector:
            return .regularMaterial
        }
    }

    var tint: Color {
        switch self {
        case .window:
            return Color(nsColor: .windowBackgroundColor).opacity(0.18)
        case .sidebar, .content, .inspector:
            return Color(nsColor: .windowBackgroundColor).opacity(0.16)
        }
    }

    var highlightOpacity: CGFloat {
        switch self {
        case .window:
            return 0.08
        case .sidebar, .content, .inspector:
            return 0.10
        }
    }

    var borderColor: Color {
        switch self {
        case .window:
            return Color(nsColor: .separatorColor).opacity(0.18)
        case .sidebar, .content, .inspector:
            return Color(nsColor: .separatorColor).opacity(0.22)
        }
    }
}

struct RuneGlassPaneSurface: View {
    let role: RuneGlassPaneRole
    @Environment(\.runeResolvedTheme) private var resolvedTheme

    var body: some View {
        ZStack {
            Rectangle()
                .fill(role.material)

            Rectangle()
                .fill(tint(theme: resolvedTheme))

            if resolvedTheme.isNative {
                LinearGradient(
                    colors: [
                        Color.white.opacity(role.highlightOpacity),
                        Color.white.opacity(role.highlightOpacity * 0.35),
                        Color.clear
                    ],
                    startPoint: .topLeading,
                    endPoint: .bottomTrailing
                )
            }
        }
    }

    private func tint(theme: RuneResolvedTheme) -> Color {
        guard let palette = theme.palette else { return role.tint }
        switch role {
        case .window:
            return palette.window
        case .sidebar:
            return palette.sidebar
        case .content:
            return palette.content
        case .inspector:
            return palette.panel
        }
    }
}

struct RuneGlassPaneBorder: View {
    let role: RuneGlassPaneRole
    @Environment(\.runeResolvedTheme) private var resolvedTheme

    var body: some View {
        let borderColor = resolvedTheme.palette?.stroke.opacity(0.42) ?? role.borderColor
        Rectangle()
            .fill(
                LinearGradient(
                    colors: [
                        borderColor,
                        borderColor.opacity(0.35)
                    ],
                    startPoint: .top,
                    endPoint: .bottom
                )
            )
            .frame(width: 1)
    }
}
