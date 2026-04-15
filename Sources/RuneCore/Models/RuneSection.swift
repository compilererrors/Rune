import Foundation

public enum RuneSection: String, CaseIterable, Codable, Sendable, Identifiable {
    case overview
    case workloads
    case networking
    case storage
    case config
    case rbac
    case events
    case helm
    case terminal

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .overview: return "Overview"
        case .workloads: return "Workloads"
        case .networking: return "Networking"
        case .storage: return "Storage"
        case .config: return "Config"
        case .rbac: return "RBAC"
        case .events: return "Events"
        case .helm: return "Helm"
        case .terminal: return "Terminal"
        }
    }

    public var symbolName: String {
        switch self {
        case .overview: return "rectangle.grid.2x2"
        case .workloads: return "shippingbox"
        case .networking: return "point.3.connected.trianglepath.dotted"
        case .storage: return "internaldrive"
        case .config: return "slider.horizontal.3"
        case .rbac: return "person.2.badge.gearshape"
        case .events: return "bolt.badge.clock"
        case .helm: return "ferry"
        case .terminal: return "terminal"
        }
    }

    public var commandShortcut: Character {
        switch self {
        case .overview: return "1"
        case .workloads: return "2"
        case .networking: return "3"
        case .storage: return "4"
        case .config: return "5"
        case .rbac: return "6"
        case .events: return "7"
        case .helm: return "8"
        case .terminal: return "9"
        }
    }
}
