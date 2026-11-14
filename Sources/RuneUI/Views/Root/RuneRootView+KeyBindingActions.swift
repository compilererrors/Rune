import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    func moveDetailSelection(_ direction: MoveCommandDirection) {
        switch direction {
        case .left, .right:
            moveDetailInspectorTab(direction)
        case .up, .down:
            break
        @unknown default:
            break
        }
    }

    func performConfiguredAction(_ action: RuneKeyBindingAction) -> Bool {
        switch action {
        case .commandPalette:
            viewModel.presentCommandPalette()
            return true
        case .filterResources:
            return focusResourceFilterFromKeyBinding()
        case .historyBack:
            guard viewModel.canNavigateBack else { return false }
            viewModel.navigateBack()
            return true
        case .historyForward:
            guard viewModel.canNavigateForward else { return false }
            viewModel.navigateForward()
            return true
        case .focusPreviousPane:
            focusPreviousKeyboardPane()
            return true
        case .focusNextPane:
            focusNextKeyboardPane()
            return true
        case .describe:
            return openDescribeInspectorForSelection()
        case .logs:
            return openLogsInspectorForSelection()
        case .saveLogs:
            return saveCurrentDetailFromKeyBinding()
        case .saveToExportFolder:
            return saveCurrentDetailToExportFolder(openAfterSave: false)
        case .saveAndOpen:
            return saveCurrentDetailToExportFolder(openAfterSave: true)
        case .shell:
            return openShellOrScaleInspectorForSelection()
        case .edit:
            return openYAMLEditorForSelection()
        case .yaml:
            return openYAMLInspectorForSelection()
        case .delete:
            return deleteSelectionFromKeyBinding()
        case .portForward:
            return openPortForwardInspectorForSelection()
        case .rollout:
            return openRolloutInspectorForSelection()
        }
    }

    func focusResourceFilterFromKeyBinding() -> Bool {
        guard showsResourceFilterControls else { return false }
        textInputFocus = .resourceFilter
        keyboardPaneFocus = .content
        return true
    }

    func openDescribeInspectorForSelection() -> Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return false }
                podInspectorTab = .describe
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return false }
                deploymentInspectorTab = .describe
            default:
                guard hasGenericManifestSelection else { return false }
                genericResourceManifestTab = .describe
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                guard viewModel.state.selectedService != nil else { return false }
                serviceInspectorTab = .describe
            default:
                guard hasGenericManifestSelection else { return false }
                genericResourceManifestTab = .describe
            }
        case .config, .storage, .rbac:
            guard hasGenericManifestSelection else { return false }
            genericResourceManifestTab = .describe
        case .helm:
            guard selectedHelmInspectorMode == .operatorResource else { return false }
            genericResourceManifestTab = .describe
        case .overview, .events, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        viewModel.isDetailPaneVisible = true
        keyboardPaneFocus = .detail
        return true
    }

    func openYAMLInspectorForSelection() -> Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return false }
                podInspectorTab = .yaml
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return false }
                deploymentInspectorTab = .yaml
            default:
                guard hasGenericManifestSelection else { return false }
                genericResourceManifestTab = .yaml
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                guard viewModel.state.selectedService != nil else { return false }
                serviceInspectorTab = .yaml
            default:
                guard hasGenericManifestSelection else { return false }
                genericResourceManifestTab = .yaml
            }
        case .config, .storage, .rbac:
            guard hasGenericManifestSelection else { return false }
            genericResourceManifestTab = .yaml
        case .helm:
            guard selectedHelmInspectorMode == .operatorResource else { return false }
            genericResourceManifestTab = .yaml
        case .overview, .events, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        viewModel.isDetailPaneVisible = true
        keyboardPaneFocus = .detail
        return true
    }

    func openYAMLEditorForSelection() -> Bool {
        guard openYAMLInspectorForSelection() else { return false }
        openYAMLEditorSheet()
        return true
    }

    func deleteSelectionFromKeyBinding() -> Bool {
        guard hasDeletableSelection else { return false }
        viewModel.requestDeleteSelectedResource()
        return true
    }

    func openLogsInspectorForSelection() -> Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return false }
                podInspectorTab = .logs
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return false }
                deploymentInspectorTab = .unifiedLogs
            default:
                return false
            }
        case .networking:
            guard viewModel.state.selectedWorkloadKind == .service, viewModel.state.selectedService != nil else { return false }
            serviceInspectorTab = .unifiedLogs
        case .terminal:
            guard terminalLogActivePod != nil || terminalInitialLogPod != nil else { return false }
            let wasShowingLogs = terminalInspectorTab == .logs
            terminalInspectorTab = .logs
            if wasShowingLogs {
                refreshTerminalInspectorSelectionIfNeeded()
            }
        case .overview, .config, .storage, .rbac, .events, .helm:
            return false
        }

        yamlManifestIsEditing = false
        viewModel.isDetailPaneVisible = true
        keyboardPaneFocus = .detail
        return true
    }

    func saveCurrentDetailFromKeyBinding() -> Bool {
        guard keyboardPaneFocus == .content || keyboardPaneFocus == .detail else { return false }

        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return false }
                switch podInspectorTab {
                case .logs:
                    viewModel.saveCurrentLogs()
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview, .exec, .portForward:
                    return false
                }
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return false }
                switch deploymentInspectorTab {
                case .unifiedLogs:
                    viewModel.saveCurrentLogs()
                case .rollout:
                    viewModel.saveCurrentRolloutHistory()
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview:
                    return false
                }
            default:
                guard hasGenericManifestSelection else { return false }
                switch genericResourceManifestTab {
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview:
                    return false
                }
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                guard viewModel.state.selectedService != nil else { return false }
                switch serviceInspectorTab {
                case .unifiedLogs:
                    viewModel.saveCurrentLogs()
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview, .portForward:
                    return false
                }
            default:
                guard hasGenericManifestSelection else { return false }
                switch genericResourceManifestTab {
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview:
                    return false
                }
            }
        case .config, .storage, .rbac:
            guard hasGenericManifestSelection else { return false }
            switch genericResourceManifestTab {
            case .describe:
                viewModel.saveCurrentResourceDescribe()
            case .yaml:
                viewModel.saveCurrentResourceYAML()
            case .overview:
                return false
            }
        case .helm:
            switch selectedHelmInspectorMode {
            case .operatorResource:
                switch genericResourceManifestTab {
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview:
                    return false
                }
            case .release:
                switch helmInspectorTab {
                case .values:
                    viewModel.saveCurrentHelmValues()
                case .manifest:
                    viewModel.saveCurrentHelmManifest()
                case .history:
                    viewModel.saveCurrentHelmHistory()
                case .overview:
                    return false
                }
            case .none:
                return false
            }
        case .terminal:
            if keyboardPaneFocus == .content {
                viewModel.saveActiveTerminalTranscript()
                return true
            }
            switch terminalInspectorTab {
            case .logs:
                guard terminalLogActivePod != nil else { return false }
                viewModel.saveCurrentLogs()
            case .yaml:
                guard terminalInspectorPod != nil else { return false }
                viewModel.saveCurrentResourceYAML()
            case .commands:
                return false
            }
        case .overview, .events:
            return false
        }
        return true
    }
}
