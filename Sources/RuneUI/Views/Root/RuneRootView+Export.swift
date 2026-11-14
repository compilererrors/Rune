import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    func handleWorkspaceCommand(_ command: WorkspaceCommand) {
        switch command {
        case .openLogs:
            _ = openLogsInspectorForSelection()
        case .saveCurrentDetail:
            ensureWorkspaceSaveFocus()
            _ = saveCurrentDetailFromKeyBinding()
        case .saveCurrentDetailToExportFolder:
            ensureWorkspaceSaveFocus()
            _ = saveCurrentDetailToExportFolder(openAfterSave: false)
        case .saveAndOpenCurrentDetail:
            ensureWorkspaceSaveFocus()
            _ = saveCurrentDetailToExportFolder(openAfterSave: true)
        }
    }

    func ensureWorkspaceSaveFocus() {
        guard keyboardPaneFocus != .content, keyboardPaneFocus != .detail else { return }
        keyboardPaneFocus = .detail
    }

    func saveCurrentDetailToExportFolder(openAfterSave: Bool) -> Bool {
        guard keyboardPaneFocus == .content || keyboardPaneFocus == .detail else { return false }

        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return false }
                switch podInspectorTab {
                case .logs:
                    viewModel.saveCurrentLogsToExportFolder(openAfterSave: openAfterSave)
                case .describe:
                    viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: openAfterSave)
                case .yaml:
                    viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: openAfterSave)
                case .overview, .exec, .portForward:
                    return false
                }
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return false }
                switch deploymentInspectorTab {
                case .unifiedLogs:
                    viewModel.saveCurrentLogsToExportFolder(openAfterSave: openAfterSave)
                case .rollout:
                    viewModel.saveCurrentRolloutHistoryToExportFolder(openAfterSave: openAfterSave)
                case .describe:
                    viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: openAfterSave)
                case .yaml:
                    viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: openAfterSave)
                case .overview:
                    return false
                }
            default:
                guard hasGenericManifestSelection else { return false }
                switch genericResourceManifestTab {
                case .describe:
                    viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: openAfterSave)
                case .yaml:
                    viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: openAfterSave)
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
                    viewModel.saveCurrentLogsToExportFolder(openAfterSave: openAfterSave)
                case .describe:
                    viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: openAfterSave)
                case .yaml:
                    viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: openAfterSave)
                case .overview, .portForward:
                    return false
                }
            default:
                guard hasGenericManifestSelection else { return false }
                switch genericResourceManifestTab {
                case .describe:
                    viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: openAfterSave)
                case .yaml:
                    viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: openAfterSave)
                case .overview:
                    return false
                }
            }
        case .config, .storage, .rbac:
            guard hasGenericManifestSelection else { return false }
            switch genericResourceManifestTab {
            case .describe:
                viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: openAfterSave)
            case .yaml:
                viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: openAfterSave)
            case .overview:
                return false
            }
        case .helm:
            switch selectedHelmInspectorMode {
            case .operatorResource:
                switch genericResourceManifestTab {
                case .describe:
                    viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: openAfterSave)
                case .yaml:
                    viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: openAfterSave)
                case .overview:
                    return false
                }
            case .release:
                switch helmInspectorTab {
                case .values:
                    viewModel.saveCurrentHelmValuesToExportFolder(openAfterSave: openAfterSave)
                case .manifest:
                    viewModel.saveCurrentHelmManifestToExportFolder(openAfterSave: openAfterSave)
                case .history:
                    viewModel.saveCurrentHelmHistoryToExportFolder(openAfterSave: openAfterSave)
                case .overview:
                    return false
                }
            case .none:
                return false
            }
        case .terminal:
            if keyboardPaneFocus == .content {
                viewModel.saveActiveTerminalTranscriptToExportFolder(openAfterSave: openAfterSave)
                return true
            }
            switch terminalInspectorTab {
            case .logs:
                guard terminalLogActivePod != nil else { return false }
                viewModel.saveCurrentLogsToExportFolder(openAfterSave: openAfterSave)
            case .yaml:
                guard terminalInspectorPod != nil else { return false }
                viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: openAfterSave)
            case .commands:
                return false
            }
        case .overview, .events:
            return false
        }
        return true
    }
}
