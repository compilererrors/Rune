import SwiftUI
import RuneCore

enum TerminalStatusStyling {
    static func title(_ status: PodTerminalSessionStatus) -> String {
        switch status {
        case .connecting: return "Connecting"
        case .connected: return "Connected"
        case .disconnected: return "Disconnected"
        case .failed: return "Failed"
        }
    }

    static func color(_ status: PodTerminalSessionStatus, palette: RuneThemePalette? = nil) -> Color {
        switch status {
        case .connecting: return RuneSemanticColorRole.warning.color(in: palette)
        case .connected: return RuneSemanticColorRole.success.color(in: palette)
        case .disconnected: return .secondary
        case .failed: return RuneSemanticColorRole.danger.color(in: palette)
        }
    }

    static func color(_ status: PortForwardStatus, palette: RuneThemePalette? = nil) -> Color {
        switch status {
        case .starting: return RuneSemanticColorRole.warning.color(in: palette)
        case .active: return RuneSemanticColorRole.success.color(in: palette)
        case .stopped: return .secondary
        case .failed: return RuneSemanticColorRole.danger.color(in: palette)
        }
    }
}

struct TerminalStatusDot: View {
    let color: Color
    var topPadding: CGFloat = 0

    var body: some View {
        Circle()
            .fill(color)
            .frame(width: 8, height: 8)
            .padding(.top, topPadding)
    }
}
