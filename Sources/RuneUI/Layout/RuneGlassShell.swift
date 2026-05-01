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

    var body: some View {
        ZStack {
            Rectangle()
                .fill(role.material)

            Rectangle()
                .fill(role.tint)

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

struct RuneGlassPaneBorder: View {
    let role: RuneGlassPaneRole

    var body: some View {
        Rectangle()
            .fill(
                LinearGradient(
                    colors: [
                        role.borderColor,
                        role.borderColor.opacity(0.35)
                    ],
                    startPoint: .top,
                    endPoint: .bottom
                )
            )
            .frame(width: 1)
    }
}
