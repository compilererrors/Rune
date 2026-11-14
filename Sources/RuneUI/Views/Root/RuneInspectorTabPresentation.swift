import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneSection {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .workloads: return string(.workloads)
        case .networking: return string(.networking)
        case .storage: return string(.storage)
        case .config: return string(.config)
        case .rbac: return string(.rbac)
        case .events: return string(.events)
        case .helm: return string(.helm)
        case .terminal: return string(.terminal)
        }
    }
}

extension KubeResourceKind {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .pod: return string(.pods)
        case .deployment: return string(.deployments)
        case .statefulSet: return string(.statefulSets)
        case .daemonSet: return string(.daemonSets)
        case .job: return string(.jobs)
        case .cronJob: return string(.cronJobs)
        case .replicaSet: return string(.replicaSets)
        case .service: return string(.services)
        case .endpoint: return string(.endpoints)
        case .ingress: return string(.ingresses)
        case .configMap: return string(.configMaps)
        case .secret: return string(.secrets)
        case .node: return string(.nodes)
        case .event: return string(.events)
        case .serviceAccount: return string(.serviceAccounts)
        case .role: return string(.roles)
        case .roleBinding: return string(.roleBindings)
        case .clusterRole: return string(.clusterRoles)
        case .clusterRoleBinding: return string(.clusterRoleBindings)
        case .persistentVolumeClaim: return string(.persistentVolumeClaims)
        case .persistentVolume: return string(.persistentVolumes)
        case .storageClass: return string(.storageClasses)
        case .horizontalPodAutoscaler: return string(.horizontalPodAutoscalers)
        case .networkPolicy: return string(.networkPolicies)
        }
    }
}

extension PodInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .logs: return string(.logs)
        case .exec: return string(.exec)
        case .portForward: return string(.portForward)
        case .describe: return string(.describe)
        case .yaml: return string(.yaml)
        }
    }
}

extension TerminalInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .commands: return string(.commands)
        case .logs: return string(.logs)
        case .yaml: return string(.yaml)
        }
    }
}

extension ServiceInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .unifiedLogs: return string(.unifiedLogs)
        case .portForward: return string(.portForward)
        case .describe: return string(.describe)
        case .yaml: return string(.yaml)
        }
    }
}

extension DeploymentInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .unifiedLogs: return string(.unifiedLogs)
        case .rollout: return string(.rollout)
        case .describe: return string(.describe)
        case .yaml: return string(.yaml)
        }
    }
}

extension GenericResourceManifestTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .describe: return string(.describe)
        case .yaml: return string(.yaml)
        }
    }
}

extension HelmInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .values: return string(.values)
        case .manifest: return string(.manifest)
        case .history: return string(.history)
        }
    }
}

extension HelmBrowserTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .releases: return string(.releases)
        case .operatorResources: return string(.operatorResources)
        }
    }
}
