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

    static func color(_ status: PodTerminalSessionStatus) -> Color {
        switch status {
        case .connecting: return .orange
        case .connected: return .green
        case .disconnected: return .secondary
        case .failed: return .red
        }
    }

    static func color(_ status: PortForwardStatus) -> Color {
        switch status {
        case .starting: return .orange
        case .active: return .green
        case .stopped: return .secondary
        case .failed: return .red
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
