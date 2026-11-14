import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

enum RuneInspectorRefreshRoute: Equatable, Sendable {
    case currentView
    case resourceInspector
    case logs
    case helmInspector
}

struct RuneInspectorRefreshRouting {
    static func route(
        section: RuneSection,
        workloadKind: KubeResourceKind,
        podTab: PodInspectorTab,
        deploymentTab: DeploymentInspectorTab,
        serviceTab: ServiceInspectorTab,
        genericTab: GenericResourceManifestTab,
        helmTab: HelmInspectorTab,
        helmMode: RuneHelmInspectorMode
    ) -> RuneInspectorRefreshRoute {
        switch section {
        case .workloads:
            switch workloadKind {
            case .pod:
                switch podTab {
                case .logs: return .logs
                case .describe, .yaml: return .resourceInspector
                case .overview, .exec, .portForward: return .currentView
                }
            case .deployment:
                switch deploymentTab {
                case .unifiedLogs: return .logs
                case .describe, .yaml: return .resourceInspector
                case .overview, .rollout: return .currentView
                }
            default:
                return genericRoute(for: genericTab)
            }

        case .networking:
            if workloadKind == .service {
                switch serviceTab {
                case .unifiedLogs: return .logs
                case .describe, .yaml: return .resourceInspector
                case .overview, .portForward: return .currentView
                }
            }
            return genericRoute(for: genericTab)

        case .config, .storage, .rbac:
            return genericRoute(for: genericTab)

        case .helm:
            switch helmMode {
            case .operatorResource:
                return genericTab == .overview ? .currentView : .helmInspector
            case .release:
                return helmTab == .overview ? .currentView : .helmInspector
            case .none:
                return .currentView
            }

        case .overview, .events, .terminal:
            return .currentView
        }
    }

    private static func genericRoute(
        for tab: GenericResourceManifestTab
    ) -> RuneInspectorRefreshRoute {
        tab == .overview ? .currentView : .resourceInspector
    }
}

struct RuneHelmSelectionIdentity: Equatable {
    let releaseID: String?
    let operatorResourceID: String?
}

struct RuneInspectorStateSyncIdentity: Equatable {
    let genericManifestTab: GenericResourceManifestTab
    let helmInspectorTab: HelmInspectorTab
    let helmBrowserTab: HelmBrowserTab
    let terminalInspectorTab: TerminalInspectorTab
    let isYAMLInlineEditing: Bool
    let showsHistoricalDeploymentReplicaSets: Bool
}

struct RuneHelmSelectionSyncModifier: ViewModifier {
    let restoreRequest: SavedWorkspaceInspectorRestoreRequest?
    let selectionIdentity: RuneHelmSelectionIdentity
    let applyRestore: (SavedWorkspaceInspectorState?) -> Void
    let syncBrowserSelection: () -> Void

    func body(content: Content) -> some View {
        content
            .onChange(of: restoreRequest) { _, request in
                applyRestore(request?.inspectorState)
                syncBrowserSelection()
            }
            .onChange(of: selectionIdentity) { _, _ in
                syncBrowserSelection()
            }
    }
}
