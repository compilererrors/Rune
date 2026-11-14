import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    func openShellOrScaleInspectorForSelection() -> Bool {
        guard viewModel.state.selectedSection == .workloads else {
            return false
        }

        switch viewModel.state.selectedWorkloadKind {
        case .pod:
            guard viewModel.state.selectedPod != nil else { return false }
            podInspectorTab = .exec
        case .deployment:
            guard viewModel.state.selectedDeployment != nil else { return false }
            deploymentInspectorTab = .overview
        default:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    func openPortForwardInspectorForSelection() -> Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            guard viewModel.state.selectedWorkloadKind == .pod, viewModel.state.selectedPod != nil else { return false }
            podInspectorTab = .portForward
        case .networking:
            guard viewModel.state.selectedWorkloadKind == .service, viewModel.state.selectedService != nil else { return false }
            serviceInspectorTab = .portForward
        case .overview, .config, .storage, .rbac, .events, .helm, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    func openRolloutInspectorForSelection() -> Bool {
        guard viewModel.state.selectedSection == .workloads,
              viewModel.state.selectedWorkloadKind == .deployment,
              viewModel.state.selectedDeployment != nil else {
            return false
        }
        deploymentInspectorTab = .rollout
        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    var hasGenericManifestSelection: Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .statefulSet:
                return viewModel.state.selectedStatefulSet != nil
            case .daemonSet:
                return viewModel.state.selectedDaemonSet != nil
            case .job:
                return viewModel.state.selectedJob != nil
            case .cronJob:
                return viewModel.state.selectedCronJob != nil
            case .replicaSet:
                return viewModel.state.selectedReplicaSet != nil
            case .horizontalPodAutoscaler:
                return viewModel.state.selectedHorizontalPodAutoscaler != nil
            default:
                return false
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .endpoint:
                return viewModel.state.selectedEndpoint != nil
            case .ingress:
                return viewModel.state.selectedIngress != nil
            case .networkPolicy:
                return viewModel.state.selectedNetworkPolicy != nil
            default:
                return false
            }
        case .config:
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                return viewModel.state.selectedConfigMap != nil
            case .secret:
                return viewModel.state.selectedSecret != nil
            default:
                return false
            }
        case .storage:
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                return viewModel.state.selectedPersistentVolumeClaim != nil
            case .persistentVolume:
                return viewModel.state.selectedPersistentVolume != nil
            case .storageClass:
                return viewModel.state.selectedStorageClass != nil
            case .node:
                return viewModel.state.selectedNode != nil
            default:
                return false
            }
        case .rbac:
            return viewModel.state.selectedRBACResource != nil
        case .overview, .events, .helm, .terminal:
            return false
        }
    }

    var hasDeletableSelection: Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                return viewModel.state.selectedPod != nil
            case .deployment:
                return viewModel.state.selectedDeployment != nil
            default:
                return hasGenericManifestSelection
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                return viewModel.state.selectedService != nil
            default:
                return hasGenericManifestSelection
            }
        case .config, .storage, .rbac:
            return hasGenericManifestSelection
        case .overview, .events, .helm, .terminal:
            return false
        }
    }

    func moveDetailInspectorTab(_ direction: MoveCommandDirection) {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return }
                podInspectorTab = advancedTab(current: podInspectorTab, direction: direction)
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return }
                deploymentInspectorTab = advancedTab(current: deploymentInspectorTab, direction: direction)
            case .cronJob:
                guard viewModel.state.selectedCronJob != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .statefulSet:
                guard viewModel.state.selectedStatefulSet != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .daemonSet:
                guard viewModel.state.selectedDaemonSet != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .job:
                guard viewModel.state.selectedJob != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .replicaSet:
                guard viewModel.state.selectedReplicaSet != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .horizontalPodAutoscaler:
                guard viewModel.state.selectedHorizontalPodAutoscaler != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            default:
                break
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                guard viewModel.state.selectedService != nil else { return }
                serviceInspectorTab = advancedTab(current: serviceInspectorTab, direction: direction)
            case .ingress:
                guard viewModel.state.selectedIngress != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .endpoint:
                guard viewModel.state.selectedEndpoint != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .networkPolicy:
                guard viewModel.state.selectedNetworkPolicy != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            default:
                break
            }
        case .config:
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                guard viewModel.state.selectedConfigMap != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .secret:
                guard viewModel.state.selectedSecret != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            default:
                break
            }
        case .storage:
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                guard viewModel.state.selectedPersistentVolumeClaim != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .persistentVolume:
                guard viewModel.state.selectedPersistentVolume != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .storageClass:
                guard viewModel.state.selectedStorageClass != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .node:
                guard viewModel.state.selectedNode != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            default:
                break
            }
        case .rbac:
            guard viewModel.state.selectedRBACResource != nil else { return }
            genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
        case .helm:
            switch selectedHelmInspectorMode {
            case .operatorResource:
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .release:
                helmInspectorTab = advancedTab(current: helmInspectorTab, direction: direction)
            case .none:
                return
            }
        case .terminal:
            terminalInspectorTab = advancedTab(current: terminalInspectorTab, direction: direction)
        case .overview, .events:
            break
        }
    }

    func advancedTab<T: CaseIterable & Equatable>(current: T, direction: MoveCommandDirection) -> T {
        let all = Array(T.allCases)
        guard let index = all.firstIndex(of: current), !all.isEmpty else { return current }
        switch direction {
        case .right, .down:
            return all[(index + 1) % all.count]
        case .left, .up:
            return all[(index + all.count - 1) % all.count]
        @unknown default:
            return current
        }
    }

    func steppedIndex(count: Int, current: Int, direction: MoveCommandDirection) -> Int? {
        guard count > 0 else { return nil }
        switch direction {
        case .down, .right:
            return min(current + 1, count - 1)
        case .up, .left:
            return max(current - 1, 0)
        @unknown default:
            return current
        }
    }

    func steppedItem<T: Identifiable>(
        items: [T],
        currentID: T.ID?,
        direction: MoveCommandDirection
    ) -> T? where T.ID: Equatable {
        guard !items.isEmpty else { return nil }
        guard let currentID,
              let currentIndex = items.firstIndex(where: { $0.id == currentID }),
              let nextIndex = steppedIndex(count: items.count, current: currentIndex, direction: direction) else {
            return items.first
        }
        return items[nextIndex]
    }
}
