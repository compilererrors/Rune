import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

enum PodInspectorTab: String, CaseIterable, Identifiable {
    case overview
    case logs
    case exec
    case portForward
    case describe
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .logs: return "Logs"
        case .exec: return "Exec"
        case .portForward: return "Port Forward"
        case .describe: return "Describe"
        case .yaml: return "YAML"
        }
    }
}

enum TerminalInspectorTab: String, CaseIterable, Identifiable {
    case logs
    case commands
    case yaml

    static let defaultTab: TerminalInspectorTab = .logs

    var id: String { rawValue }

    var title: String {
        switch self {
        case .commands: return "Commands"
        case .logs: return "Logs"
        case .yaml: return "YAML"
        }
    }
}

enum ServiceInspectorTab: String, CaseIterable, Identifiable {
    case overview
    case unifiedLogs
    case portForward
    case describe
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .unifiedLogs: return "Unified Logs"
        case .portForward: return "Port Forward"
        case .describe: return "Describe"
        case .yaml: return "YAML"
        }
    }
}

enum DeploymentInspectorTab: String, CaseIterable, Identifiable {
    case overview
    case unifiedLogs
    case rollout
    case describe
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .unifiedLogs: return "Unified Logs"
        case .rollout: return "Rollout"
        case .describe: return "Describe"
        case .yaml: return "YAML"
        }
    }
}

enum GenericResourceManifestTab: String, CaseIterable, Identifiable {
    case overview
    case describe
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .describe: return "Describe"
        case .yaml: return "YAML"
        }
    }
}
