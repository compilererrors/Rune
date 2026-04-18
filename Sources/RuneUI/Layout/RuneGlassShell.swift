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
            return Color(nsColor: .windowBackgroundColor).opacity(0.22)
        case .sidebar:
            return Color.black.opacity(0.10)
        case .content:
            return Color.white.opacity(0.05)
        case .inspector:
            return Color.white.opacity(0.03)
        }
    }

    var highlightOpacity: CGFloat {
        switch self {
        case .window:
            return 0.10
        case .sidebar:
            return 0.12
        case .content:
            return 0.14
        case .inspector:
            return 0.11
        }
    }

    var borderColor: Color {
        switch self {
        case .window:
            return Color.white.opacity(0.10)
        case .sidebar:
            return Color.white.opacity(0.12)
        case .content, .inspector:
            return Color.white.opacity(0.08)
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
