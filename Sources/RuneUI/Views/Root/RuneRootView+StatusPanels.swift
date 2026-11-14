import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var productionBanner: some View {
        RuneHeaderCapsule(
            "Production context active",
            role: .status,
            systemImage: "exclamationmark.triangle.fill",
            tint: RuneSemanticColorRole.danger.color(in: activeAppearanceTheme.palette),
            foregroundColor: RuneSemanticColorRole.danger.color(in: activeAppearanceTheme.palette),
            fill: RuneSemanticColorRole.danger.color(in: activeAppearanceTheme.palette).opacity(0.16),
            helpText: "Production context active",
            accessibilityLabel: "Production context active"
        )
    }

    var commandPalettePresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.state.isCommandPalettePresented },
            set: { value in
                if value {
                    viewModel.presentCommandPalette()
                } else {
                    viewModel.dismissCommandPalette()
                }
            }
        )
    }

    var pendingWriteActionPresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.pendingWriteAction != nil },
            set: { value in
                if !value {
                    guard !isConfirmingPendingWriteActionFromDialog else { return }
                    viewModel.cancelPendingWriteAction()
                }
            }
        )
    }

    var rootPendingWriteActionPresentedBinding: Binding<Bool> {
        Binding(
            get: { !isYAMLEditorSheetPresented && pendingWriteActionPresentedBinding.wrappedValue },
            set: { pendingWriteActionPresentedBinding.wrappedValue = $0 }
        )
    }

    func confirmPendingWriteActionFromDialog() {
        isConfirmingPendingWriteActionFromDialog = true
        viewModel.confirmPendingWriteAction()
        DispatchQueue.main.async {
            isConfirmingPendingWriteActionFromDialog = false
        }
    }

    func cancelPendingWriteActionFromDialog() {
        isConfirmingPendingWriteActionFromDialog = false
        viewModel.cancelPendingWriteAction()
    }

    var overviewStatusBanner: some View {
        HStack(spacing: RuneSpacing.xs) {
            Image(systemName: viewModel.isProductionContext ? "exclamationmark.shield.fill" : "checkmark.shield.fill")
                .foregroundStyle(viewModel.isProductionContext ? RuneTextStyle.danger : .success)
            Text(viewModel.isProductionContext ? "Production context active" : "Non-production context")
                .runeText(.microEmphasis)
            Spacer()
            HStack(spacing: RuneSpacing.tight) {
                RuneStatusDot(snapshotFreshnessColor)
                Text(snapshotFreshnessText)
                    .runeText(.microEmphasis)
                    .lineLimit(1)
            }
            .help(viewModel.state.snapshotFreshness.message)
            Text(viewModel.state.isReadOnlyMode ? "Read-only" : "Read/Write")
                .runeText(.micro, weight: .bold)
                .padding(.horizontal, RuneSpacing.xs)
                .frame(height: 28)
                .background((viewModel.state.isReadOnlyMode ? RuneSemanticColorRole.warning : .success).color(in: activeAppearanceTheme.palette).opacity(0.14), in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
        }
        .padding(RuneSpacing.sm)
        .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
    }

    @ViewBuilder
    var manualNamespaceBanner: some View {
        if viewModel.state.isManualNamespaceMode {
            HStack(alignment: .top, spacing: RuneSpacing.xs) {
                Image(systemName: "square.and.pencil")
                    .foregroundStyle(.runeWarning)
                VStack(alignment: .leading, spacing: RuneSpacing.xxs) {
                    Text("Manual namespace mode")
                        .runeText(.microEmphasis)
                    Text(viewModel.state.namespaceAccessWarning ?? "You cannot list namespaces, but you can work in a namespace manually.")
                        .runeText(.micro)
                        .foregroundStyle(.runeSecondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                Spacer()
                Button("Enter Namespace...") {
                    manualNamespaceInput = viewModel.state.selectedNamespace
                    isManualNamespaceSheetPresented = true
                }
                .buttonStyle(RuneToolbarButtonStyle())
            }
            .padding(RuneSpacing.sm)
            .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
        }
    }

    @ViewBuilder
    var authDoctorPanel: some View {
        if shouldReserveAuthDoctorPanel {
            let hasChecks = !viewModel.state.authDoctorChecks.isEmpty
            let runLabel = viewModel.state.isRunningAuthDoctor
                ? "Running..."
                : (hasChecks ? "Run Again" : "Run Auth Doctor")

            VStack(alignment: .leading, spacing: RuneSpacing.inset) {
                authDoctorHeader(runLabel: runLabel)

                if isAuthDoctorPanelExpanded {
                    requestMetricsSummaryRow

                    if viewModel.state.authDoctorChecks.isEmpty, !viewModel.state.isRunningAuthDoctor {
                        HStack(alignment: .top, spacing: RuneSpacing.xs) {
                            Image(systemName: "checkmark.shield")
                                .foregroundStyle(.runeSecondary)
                                .frame(width: 16)
                            Text("Run Auth Doctor to check kubeconfig, context auth, namespace access, logs, and RBAC before troubleshooting workloads.")
                                .runeText(.micro)
                                .foregroundStyle(.runeSecondary)
                                .fixedSize(horizontal: false, vertical: true)
                        }
                    } else {
                        ForEach(viewModel.state.authDoctorChecks) { check in
                            authDoctorCheckRow(check)
                        }
                    }
                }
            }
            .padding(RuneSpacing.sm)
            .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
            .task(id: viewModel.state.selectedContext?.name) {
                viewModel.refreshKubernetesRequestMetricsSummary()
            }
        }
    }

    func authDoctorHeader(runLabel: String) -> some View {
        HStack(alignment: .center, spacing: RuneSpacing.xs) {
            authDoctorDisclosureButton
            authDoctorSummaryChip

            ScrollView(.horizontal, showsIndicators: false) {
                authDoctorActions(runLabel: runLabel)
            }
            .scrollClipDisabled()
            .frame(maxWidth: .infinity, minHeight: 32, alignment: .trailing)
        }
    }

    var authDoctorDisclosureButton: some View {
        RuneDisclosureRow(
            "Auth Doctor",
            isExpanded: isAuthDoctorPanelExpanded,
            fillsAvailableWidth: false,
            help: isAuthDoctorPanelExpanded ? "Collapse Auth Doctor output" : "Expand Auth Doctor output",
            action: {
                withAnimation(.snappy(duration: 0.16)) {
                    isAuthDoctorPanelExpanded.toggle()
                }
            }
        ) {
            Label("Auth Doctor", systemImage: "stethoscope")
                .runeText(.microEmphasis)
                .labelStyle(.titleAndIcon)
        }
        .frame(minWidth: 154, alignment: .leading)
    }

    func authDoctorActions(runLabel: String) -> some View {
        HStack(alignment: .center, spacing: RuneSpacing.tight) {
            Button {
                isAuthDoctorPanelExpanded = true
                viewModel.runAuthDoctor()
            } label: {
                Label(runLabel, systemImage: "stethoscope")
            }
            .disabled(viewModel.state.isRunningAuthDoctor)
            .help("Run kubeconfig, auth, RBAC, logs, exec, and port-forward checks.")

            RuneToolbarMenu {
                Button("Save Bundle") {
                    viewModel.saveSupportBundle()
                }

                Button("Save Bundle to Export Folder") {
                    viewModel.saveSupportBundleToExportFolder(openAfterSave: false)
                }

                Button("Save Bundle and Open") {
                    viewModel.saveSupportBundleToExportFolder(openAfterSave: true)
                }
            } label: {
                Label("Save Bundle", systemImage: "square.and.arrow.down")
            }
            .help("Save Auth Doctor diagnostics and support data.")

            Button {
                withAnimation(.snappy(duration: 0.16)) {
                    viewModel.clearAuthDoctorOutput()
                    isAuthDoctorPanelExpanded = false
                }
            } label: {
                Label("Clear", systemImage: "xmark.circle")
            }
            .disabled(viewModel.state.isRunningAuthDoctor || viewModel.state.authDoctorChecks.isEmpty)
            .help("Clear Auth Doctor results.")
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .controlSize(.small)
        .fixedSize(horizontal: true, vertical: false)
    }

    var requestMetricsSummaryRow: some View {
        let summary = viewModel.kubernetesRequestMetricsSummary

        return HStack(alignment: .top, spacing: RuneSpacing.xs) {
            Image(systemName: summary.hasFailures ? "chart.bar.xaxis.ascending.badge.exclamationmark" : "chart.bar.xaxis.ascending")
                .foregroundStyle(summary.hasFailures ? RuneTextStyle.warning : .secondary)
                .frame(width: 16)

            VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                Text("API Attempts")
                    .runeText(.microEmphasis)
                Text("\(summary.requestCountText) • \(summary.outcomeText)")
                    .runeText(.micro)
                    .foregroundStyle(.runeSecondary)
                    .fixedSize(horizontal: false, vertical: true)
                Text("\(summary.transferText) • \(summary.retainedText)")
                    .runeText(.micro)
                    .foregroundStyle(.runeTertiary)
                    .fixedSize(horizontal: false, vertical: true)

                if !summary.endpointHighlights.isEmpty {
                    Text("Retained endpoint highlights")
                        .runeText(.microEmphasis)
                        .foregroundStyle(.runeSecondary)
                        .padding(.top, RuneSpacing.xxs)

                    ForEach(summary.endpointHighlights) { endpoint in
                        HStack(alignment: .firstTextBaseline, spacing: RuneSpacing.tight) {
                            Image(systemName: endpoint.hasIssues ? "exclamationmark.triangle.fill" : "clock")
                                .runeText(.micro)
                                .foregroundStyle(endpoint.hasIssues ? RuneTextStyle.warning : .secondary)
                            VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                                Text("\(endpoint.method) \(endpoint.apiPath)")
                                    .runeText(.micro, design: .monospaced)
                                    .lineLimit(1)
                                    .truncationMode(.middle)
                                    .help("\(endpoint.method) \(endpoint.apiPath)")
                                Text("\(endpoint.outcomeText) • \(endpoint.maxDurationText)")
                                    .runeText(.micro)
                                    .foregroundStyle(.runeTertiary)
                            }
                        }
                    }
                }
            }

            Spacer(minLength: 8)

            Button {
                viewModel.refreshKubernetesRequestMetricsSummary()
            } label: {
                Label("Refresh", systemImage: "arrow.clockwise")
            }
            .buttonStyle(RuneToolbarButtonStyle())
            .controlSize(.small)
            .disabled(viewModel.isRefreshingKubernetesRequestMetricsSummary)
            .help("Refresh API request metrics")
        }
    }

    func authDoctorCheckRow(_ check: RuneHealthCheck) -> some View {
        HStack(alignment: .top, spacing: RuneSpacing.xs) {
            Image(systemName: authDoctorSymbol(for: check.status))
                .foregroundStyle(authDoctorColor(for: check.status))
                .frame(width: 16)

            VStack(alignment: .leading, spacing: RuneSpacing.hairline) {
                Text(check.title)
                    .runeText(.microEmphasis)
                Text(check.message)
                    .runeText(.micro)
                    .foregroundStyle(.runeSecondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Spacer(minLength: 8)

            if let action = AuthDoctorEntryActionResolver.resolve(check: check, hasPodTarget: authDoctorTargetPod != nil) {
                Button {
                    performAuthDoctorEntryAction(action)
                } label: {
                    Label(action.title, systemImage: action.systemImage)
                }
                .buttonStyle(RuneToolbarButtonStyle())
                .controlSize(.small)
                .help(action.help)
            }
        }
    }

    func performAuthDoctorEntryAction(_ action: AuthDoctorEntryResolution) {
        switch action.destination {
        case let .section(section):
            viewModel.setSection(section)
        case let .resource(section, kind):
            viewModel.setSection(section)
            viewModel.setWorkloadKind(kind)
        case let .rbacCanIPreset(verb, resource, apiGroup, subresource, scope):
            viewModel.setSection(.rbac)
            viewModel.useRBACCanIPreset(
                verb: verb,
                resource: resource,
                apiGroup: apiGroup,
                subresource: subresource,
                scope: scope == .cluster ? .cluster : .namespace
            )
        case .podLogs:
            guard let pod = authDoctorTargetPod else { return }
            viewModel.setSection(.workloads)
            viewModel.selectPod(pod)
            podInspectorTab = .logs
            viewModel.reloadLogsForSelection()
        case .podExec:
            guard let pod = authDoctorTargetPod else { return }
            viewModel.setSection(.workloads)
            viewModel.selectPod(pod)
            podInspectorTab = .exec
        case .podPortForward:
            guard let pod = authDoctorTargetPod else { return }
            viewModel.setSection(.workloads)
            viewModel.selectPod(pod)
            podInspectorTab = .portForward
        case .kubeconfigReview:
            viewModel.reviewLoadedKubeConfigSources()
            isManualAddClusterExpanded = false
            addClusterPopoverPresented = true
        case let .documentation(url):
            NSWorkspace.shared.open(url)
        }
    }

    var authDoctorTargetPod: PodSummary? {
        viewModel.state.selectedPod
            ?? viewModel.visiblePods.first
            ?? viewModel.state.pods.first
            ?? viewModel.state.overviewPods.first
    }

    var shouldReserveAuthDoctorPanel: Bool {
        guard !simpleMode else { return false }
        switch viewModel.state.selectedSection {
        case .overview:
            return true
        default:
            return false
        }
    }

    @ViewBuilder
    var authDoctorSummaryChip: some View {
        let checks = viewModel.state.authDoctorChecks
        let failed = checks.filter { $0.status == .failed }.count
        let warnings = checks.filter { $0.status == .warning }.count
        let running = viewModel.state.isRunningAuthDoctor || checks.contains { $0.status == .running }
        let text = running ? "Running" : (checks.isEmpty ? "Ready" : "\(checks.count) checks")
        let detail = failed > 0 ? "\(failed) failed" : (warnings > 0 ? "\(warnings) warnings" : "OK")
        HStack(spacing: RuneSpacing.tight) {
            RuneStatusDot((running ? RuneSemanticColorRole.info : failed > 0 ? .danger : warnings > 0 ? .warning : .success).color(in: activeAppearanceTheme.palette))
            Text(text)
            if !checks.isEmpty {
                Text(detail)
                    .foregroundStyle(.runeSecondary)
            }
        }
        .runeText(.microEmphasis)
        .padding(.horizontal, RuneSpacing.xs)
        .frame(minHeight: RuneUILayoutMetrics.headerChipHeight)
        .background(panelFill, in: Capsule())
    }

    func authDoctorSymbol(for status: RuneHealthCheckStatus) -> String {
        switch status {
        case .running: return "arrow.triangle.2.circlepath"
        case .passed: return "checkmark.circle.fill"
        case .warning: return "exclamationmark.triangle.fill"
        case .failed: return "xmark.octagon.fill"
        }
    }

    func authDoctorColor(for status: RuneHealthCheckStatus) -> Color {
        switch status {
        case .running: return RuneSemanticColorRole.info.color(in: activeAppearanceTheme.palette)
        case .passed: return RuneSemanticColorRole.success.color(in: activeAppearanceTheme.palette)
        case .warning: return RuneSemanticColorRole.warning.color(in: activeAppearanceTheme.palette)
        case .failed: return RuneSemanticColorRole.danger.color(in: activeAppearanceTheme.palette)
        }
    }

    var snapshotFreshnessText: String {
        switch viewModel.state.snapshotFreshness.status {
        case .idle: return "No data"
        case .refreshing: return "Refreshing"
        case .reconnecting: return "Reconnecting"
        case .live: return "Live"
        case .stale: return "Stale"
        case .failed: return "Failed"
        }
    }

    var snapshotFreshnessColor: Color {
        switch viewModel.state.snapshotFreshness.status {
        case .idle: return .secondary
        case .refreshing: return RuneSemanticColorRole.info.color(in: activeAppearanceTheme.palette)
        case .reconnecting: return RuneSemanticColorRole.info.color(in: activeAppearanceTheme.palette)
        case .live: return RuneSemanticColorRole.success.color(in: activeAppearanceTheme.palette)
        case .stale: return RuneSemanticColorRole.warning.color(in: activeAppearanceTheme.palette)
        case .failed: return RuneSemanticColorRole.danger.color(in: activeAppearanceTheme.palette)
        }
    }

    var currentResourceListFreshness: RuneResourceListFreshness? {
        guard let family = currentResourceListFamily else { return nil }
        return viewModel.state.freshness(for: family)
    }

    var currentResourceListFamily: RuneResourceListFamily? {
        switch viewModel.state.selectedSection {
        case .overview:
            return nil
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .deployment: return .deployments
            case .statefulSet: return .statefulSets
            case .daemonSet: return .daemonSets
            case .job: return .jobs
            case .cronJob: return .cronJobs
            case .replicaSet: return .replicaSets
            case .horizontalPodAutoscaler: return .horizontalPodAutoscalers
            default: return .pods
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .endpoint: return .endpoints
            case .ingress: return .ingresses
            case .networkPolicy: return .networkPolicies
            default: return .services
            }
        case .storage:
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolume: return .persistentVolumes
            case .storageClass: return .storageClasses
            case .node: return .nodes
            default: return .persistentVolumeClaims
            }
        case .config:
            return viewModel.state.selectedWorkloadKind == .secret ? .secrets : .configMaps
        case .rbac:
            switch viewModel.state.selectedWorkloadKind {
            case .serviceAccount: return .serviceAccounts
            case .roleBinding: return .rbacRoleBindings
            case .clusterRole: return .rbacClusterRoles
            case .clusterRoleBinding: return .rbacClusterRoleBindings
            default: return .rbacRoles
            }
        case .events:
            return .events
        case .helm:
            return effectiveHelmBrowserTab == .operatorResources ? .operatorResources : .helmReleases
        case .terminal:
            return .pods
        }
    }
}
