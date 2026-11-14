import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

enum HelmInspectorTab: String, CaseIterable, Identifiable, Sendable {
    case overview
    case values
    case manifest
    case history

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .values: return "Values"
        case .manifest: return "Manifest"
        case .history: return "History"
        }
    }
}

enum HelmBrowserTab: String, CaseIterable, Identifiable, Sendable {
    case releases
    case operatorResources

    var id: String { rawValue }

    var resourceListFamily: RuneResourceListFamily {
        switch self {
        case .releases: return .helmReleases
        case .operatorResources: return .operatorResources
        }
    }

    var title: String {
        switch self {
        case .releases: return "Releases"
        case .operatorResources: return "Operator resources"
        }
    }
}

enum RuneHelmInspectorMode: Equatable, Sendable {
    case none
    case release
    case operatorResource

    static func resolve(hasRelease: Bool, hasOperatorResource: Bool) -> RuneHelmInspectorMode {
        if hasOperatorResource {
            return .operatorResource
        }
        if hasRelease {
            return .release
        }
        return .none
    }

    var browserTab: HelmBrowserTab? {
        switch self {
        case .none: return nil
        case .release: return .releases
        case .operatorResource: return .operatorResources
        }
    }
}
