import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var activeAppearanceTheme: RuneResolvedTheme {
        RuneAppearanceTheme.resolved(appearanceThemeRaw)
    }

    var helmSelectionSyncModifier: RuneHelmSelectionSyncModifier {
        RuneHelmSelectionSyncModifier(
            restoreRequest: viewModel.savedWorkspaceInspectorRestoreRequest,
            selectionIdentity: RuneHelmSelectionIdentity(
                releaseID: viewModel.state.selectedHelmRelease?.id,
                operatorResourceID: viewModel.state.selectedOperatorResource?.id
            ),
            applyRestore: applySavedWorkspaceInspectorState,
            syncBrowserSelection: syncHelmBrowserTabWithSelection
        )
    }

    var inspectorStateSyncIdentity: RuneInspectorStateSyncIdentity {
        RuneInspectorStateSyncIdentity(
            genericManifestTab: genericResourceManifestTab,
            helmInspectorTab: helmInspectorTab,
            helmBrowserTab: helmBrowserTab,
            terminalInspectorTab: terminalInspectorTab,
            isYAMLInlineEditing: yamlManifestIsEditing,
            showsHistoricalDeploymentReplicaSets: showsHistoricalDeploymentReplicaSets
        )
    }

    var kubeConfigImportReviewPresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.isKubeConfigImportConfirmationPending },
            set: { isPresented in
                if !isPresented {
                    viewModel.cancelKubeConfigImport()
                }
            }
        )
    }

    var kubeConfigContextRemovalPresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.pendingKubeConfigContextRemoval != nil },
            set: { isPresented in
                if !isPresented {
                    viewModel.cancelKubeConfigContextRemoval()
                }
            }
        )
    }

    var pendingKubeConfigImportReview: KubeConfigImportReview? {
        KubeConfigImportReviewAggregator.aggregate(viewModel.kubeConfigImportReviews)
    }

    var shouldShowLaunchExperience: Bool {
        !debugDisableBootstrap && viewModel.isLaunchExperienceVisible
    }

    var shouldMountWorkspaceChrome: Bool {
        debugDisableBootstrap || hasMountedWorkspaceChrome || !shouldShowLaunchExperience
    }

    func handleRootAppear() {
        keyboardPaneFocus = .sidebarSections
        textInputFocus = nil
        DispatchQueue.main.async {
            NSApp.keyWindow?.makeFirstResponder(nil)
        }
        installLocalKeyboardMonitorIfNeeded()
        installLocalMouseMonitorIfNeeded()
        scheduleWorkspaceChromeMount()
        if RuneRootLayoutDebug.isEnabled {
            RuneLoggers.layout.debug(
                "configured shell=\(resolvedShellVariant.debugLabel, privacy: .public) editor=\(resolvedManifestInlineEditorImplementation.debugLabel, privacy: .public)"
            )
        }
        #if DEBUG
        startLiveDebugScenarioIfNeeded()
        #endif
        applySavedWorkspaceInspectorState(
            viewModel.savedWorkspaceInspectorRestoreRequest?.inspectorState
        )
        syncSavedWorkspaceInspectorState()
        syncHelmBrowserTabWithSelection()
        restoreTerminalWorkspaceStateIfNeeded()
        handlePendingLaunchActionIfNeeded()
        guard !debugDisableBootstrap else { return }
        viewModel.bootstrapIfNeeded()
    }

    func restoreTerminalWorkspaceStateIfNeeded() {
        guard !debugDisableLayoutPersistence,
              saveLastAppState,
              !hasRestoredTerminalWorkspaceState,
              viewModel.terminalWorkspaceRestoreScopeID != nil
        else { return }
        hasRestoredTerminalWorkspaceState = true
        guard let snapshot = viewModel.loadPersistedTerminalWorkspaceState() else { return }

        terminalShellPodID = snapshot.shellPodID ?? terminalShellPodID
        terminalPortForwardPodID = snapshot.portForwardPodID ?? terminalPortForwardPodID
        if let inspectorTabID = snapshot.inspectorTabID,
           let tab = TerminalInspectorTab(rawValue: inspectorTabID) {
            terminalInspectorTab = tab
        }
        terminalLogTabState.restore(
            tabs: snapshot.logTabs,
            activeTabID: snapshot.activeLogTabID,
            selectedPodID: snapshot.selectedLogPodID
        )

        if viewModel.state.terminalSessions.isEmpty {
            for session in snapshot.sessions.map(\.restoredSession) {
                viewModel.state.setTerminalSession(session)
            }
            if let activeID = snapshot.activeSessionID {
                viewModel.state.selectTerminalSession(id: activeID)
            }
        }
    }

    func clearTerminalWorkspaceScopeSelections() {
        terminalShellPodID = ""
        terminalPortForwardPodID = ""
        terminalLogTabState = TerminalPodLogTabState()
    }

    func persistTerminalWorkspaceStateIfNeeded() {
        guard !debugDisableLayoutPersistence,
              saveLastAppState,
              hasRestoredTerminalWorkspaceState
        else { return }
        viewModel.persistTerminalWorkspaceState(currentTerminalWorkspaceStateSnapshot)
    }

    var terminalWorkspacePersistenceLifecycleModifier: TerminalWorkspacePersistenceLifecycleModifier {
        TerminalWorkspacePersistenceLifecycleModifier(
            isEnabled: saveLastAppState,
            terminalInspectorTab: terminalInspectorTab,
            terminalShellPodID: terminalShellPodID,
            terminalPortForwardPodID: terminalPortForwardPodID,
            terminalLogTabState: terminalLogTabState,
            terminalSessions: viewModel.state.terminalSessions,
            activeTerminalSessionID: viewModel.state.activeTerminalSessionID,
            onEnable: {
                restoreTerminalWorkspaceStateIfNeeded()
                persistTerminalWorkspaceStateIfNeeded()
            },
            onDisable: {
                guard !debugDisableLayoutPersistence else { return }
                viewModel.clearPersistedTerminalWorkspaceState()
            },
            onPersist: persistTerminalWorkspaceStateIfNeeded
        )
    }

    var currentTerminalWorkspaceStateSnapshot: TerminalWorkspaceStateSnapshot {
        TerminalWorkspaceStateSnapshot(
            sessions: viewModel.state.terminalSessions.map(TerminalWorkspaceSessionSnapshot.init(session:)),
            activeSessionID: viewModel.state.activeTerminalSessionID,
            logTabs: terminalLogTabState.snapshotTabs,
            activeLogTabID: terminalLogTabState.activeTabID,
            selectedLogPodID: terminalLogTabState.selectedPodID,
            shellPodID: terminalShellPodID,
            portForwardPodID: terminalPortForwardPodID,
            inspectorTabID: terminalInspectorTab.rawValue
        )
    }

    var currentSavedWorkspaceInspectorState: SavedWorkspaceInspectorState {
        SavedWorkspaceInspectorState(
            podTabID: podInspectorTab.rawValue,
            serviceTabID: serviceInspectorTab.rawValue,
            deploymentTabID: deploymentInspectorTab.rawValue,
            genericManifestTabID: genericResourceManifestTab.rawValue,
            helmTabID: helmInspectorTab.rawValue,
            helmBrowserTabID: helmBrowserTab.rawValue,
            terminalTabID: terminalInspectorTab.rawValue,
            isYAMLInlineEditing: yamlManifestIsEditing,
            showsHistoricalDeploymentReplicaSets: showsHistoricalDeploymentReplicaSets
        )
    }

    var selectedHelmInspectorMode: RuneHelmInspectorMode {
        RuneHelmInspectorMode.resolve(
            hasRelease: viewModel.state.selectedHelmRelease != nil,
            hasOperatorResource: viewModel.state.selectedOperatorResource != nil
        )
    }

    func syncHelmBrowserTabWithSelection() {
        guard let browserTab = selectedHelmInspectorMode.browserTab else { return }
        helmBrowserTab = browserTab
        viewModel.setHelmBrowserResourceFamily(browserTab.resourceListFamily)
    }

    var effectiveHelmBrowserTab: HelmBrowserTab {
        if viewModel.state.selectedOperatorResource != nil {
            return .operatorResources
        }
        if viewModel.state.selectedHelmRelease != nil {
            return .releases
        }
        return viewModel.helmBrowserResourceFamily == .operatorResources
            ? .operatorResources
            : .releases
    }

    func syncSavedWorkspaceInspectorState() {
        viewModel.updateSavedWorkspaceInspectorState(currentSavedWorkspaceInspectorState)
    }

    func applySavedWorkspaceInspectorState(_ inspectorState: SavedWorkspaceInspectorState?) {
        guard let inspectorState else { return }

        if let podTabID = inspectorState.podTabID,
           let tab = PodInspectorTab(rawValue: podTabID) {
            podInspectorTab = tab
        }
        if let serviceTabID = inspectorState.serviceTabID,
           let tab = ServiceInspectorTab(rawValue: serviceTabID) {
            serviceInspectorTab = tab
        }
        if let deploymentTabID = inspectorState.deploymentTabID,
           let tab = DeploymentInspectorTab(rawValue: deploymentTabID) {
            deploymentInspectorTab = tab
        }
        if let genericManifestTabID = inspectorState.genericManifestTabID,
           let tab = GenericResourceManifestTab(rawValue: genericManifestTabID) {
            genericResourceManifestTab = tab
        }
        if let helmTabID = inspectorState.helmTabID,
           let tab = HelmInspectorTab(rawValue: helmTabID) {
            helmInspectorTab = tab
        }
        if let helmBrowserTabID = inspectorState.helmBrowserTabID,
           let tab = HelmBrowserTab(rawValue: helmBrowserTabID) {
            helmBrowserTab = tab
            viewModel.setHelmBrowserResourceFamily(tab.resourceListFamily)
        }
        if let terminalTabID = inspectorState.terminalTabID,
           let tab = TerminalInspectorTab(rawValue: terminalTabID) {
            terminalInspectorTab = tab
        }
        if let isYAMLInlineEditing = inspectorState.isYAMLInlineEditing {
            yamlManifestIsEditing = isYAMLInlineEditing && resolvedManifestInlineEditorImplementation.supportsInlineEditing
        }
        if let showsHistoricalDeploymentReplicaSets = inspectorState.showsHistoricalDeploymentReplicaSets {
            self.showsHistoricalDeploymentReplicaSets = showsHistoricalDeploymentReplicaSets
        }
        syncSavedWorkspaceInspectorState()
    }

    func handlePendingLaunchActionIfNeeded() {
        guard let request = UserDefaults.standard.consumeRunePendingLaunchRequest() else { return }

        DispatchQueue.main.async {
            switch request.action {
            case .authDoctor:
                guard !simpleMode else { return }
                isAuthDoctorPanelExpanded = true
                viewModel.runAuthDoctor()
            case .commandPalette:
                viewModel.presentCommandPalette(prefilledQuery: request.query)
            case .recentContexts:
                viewModel.presentCommandPalette(prefilledQuery: Self.launchCommandPaletteQuery(prefix: ":ctx", query: request.query))
            case .savedWorkspaces:
                viewModel.presentCommandPalette(prefilledQuery: Self.launchCommandPaletteQuery(prefix: ":ws", query: request.query))
            }
        }
    }

    static func launchCommandPaletteQuery(prefix: String, query: String?) -> String {
        let trimmed = query?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return trimmed.isEmpty ? "\(prefix) " : "\(prefix) \(trimmed)"
    }

    func handleRootDisappear() {
        viewModel.persistLastAppStateNow()
        persistTerminalWorkspaceStateIfNeeded()
        removeLocalKeyboardMonitor()
        removeLocalMouseMonitor()
    }

    func scheduleWorkspaceChromeMount() {
        guard !debugDisableBootstrap else {
            hasMountedWorkspaceChrome = true
            return
        }
        guard !hasMountedWorkspaceChrome else { return }
        Task { @MainActor in
            try? await Task.sleep(nanoseconds: workspaceChromeMountDelayNanoseconds)
            hasMountedWorkspaceChrome = true
        }
    }

    var launchExperienceOverlay: some View {
        ZStack {
            RuneGlassPaneSurface(role: .window)
                .ignoresSafeArea()

            VStack(spacing: RuneSpacing.md) {
                launchLogo

                VStack(spacing: RuneSpacing.tight) {
                    Text("Loading workspace")
                        .runeText(.micro)
                        .foregroundStyle(.runeSecondary)
                }

                ProgressView()
                    .controlSize(.small)
                    .padding(.top, RuneSpacing.hairline)
            }
            .padding(RuneSpacing.xl)
        }
        .allowsHitTesting(false)
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Rune loading workspace")
    }

    @ViewBuilder
    var launchLogo: some View {
        if let image = launchLogoImage {
            Image(nsImage: image)
                .resizable()
                .scaledToFit()
                .frame(width: 112, height: 112)
                .accessibilityHidden(true)
        } else {
            Image(systemName: "hexagon")
                .font(.system(size: RuneGlyphSize.hero, weight: .semibold))
                .foregroundStyle(.runeAccent)
                .frame(width: 112, height: 112)
                .accessibilityHidden(true)
        }
    }

    var launchLogoImage: NSImage? {
        for bundle in launchLogoCandidateBundles {
            if let url = bundle.url(forResource: "rune_logo_main", withExtension: "png"),
               let image = NSImage(contentsOf: url) {
                return image
            }
        }
        if let applicationIcon = NSApp.applicationIconImage,
           applicationIcon.size.width > 0,
           applicationIcon.size.height > 0 {
            return applicationIcon
        }
        return NSImage(named: "rune_logo_main")
    }

    var launchLogoCandidateBundles: [Bundle] {
        var bundles = [Bundle.main]
        guard let resourceURL = Bundle.main.resourceURL else { return bundles }

        let explicitBundleNames = [
            "Rune_RuneUI.bundle",
            "RuneUI_RuneUI.bundle",
            "RuneUI.bundle"
        ]
        for bundleName in explicitBundleNames {
            if let bundle = Bundle(url: resourceURL.appendingPathComponent(bundleName)) {
                bundles.append(bundle)
            }
        }

        guard let resourceBundles = try? FileManager.default.contentsOfDirectory(
            at: resourceURL,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else {
            return bundles
        }

        for bundleURL in resourceBundles where bundleURL.pathExtension == "bundle" {
            if let bundle = Bundle(url: bundleURL), !bundles.contains(where: { $0.bundleURL == bundle.bundleURL }) {
                bundles.append(bundle)
            }
        }
        return bundles
    }
}

@MainActor
struct TerminalWorkspacePersistenceLifecycleModifier: ViewModifier {
    let isEnabled: Bool
    let terminalInspectorTab: TerminalInspectorTab
    let terminalShellPodID: String
    let terminalPortForwardPodID: String
    let terminalLogTabState: TerminalPodLogTabState
    let terminalSessions: [PodTerminalSession]
    let activeTerminalSessionID: String?
    let onEnable: @MainActor () -> Void
    let onDisable: @MainActor () -> Void
    let onPersist: @MainActor () -> Void

    static func persistenceKey(for sessions: [PodTerminalSession]) -> [TerminalWorkspaceSessionSnapshot] {
        sessions.map(TerminalWorkspaceSessionSnapshot.init(session:))
    }

    func body(content: Content) -> some View {
        content
            .onChange(of: terminalInspectorTab) { _, _ in
                onPersist()
            }
            .onChange(of: terminalShellPodID) { _, _ in
                onPersist()
            }
            .onChange(of: terminalPortForwardPodID) { _, _ in
                onPersist()
            }
            .onChange(of: terminalLogTabState) { _, _ in
                onPersist()
            }
            // Only what is saved (the tabs), never output or status: those
            // change many times a second while a shell runs.
            .onChange(of: Self.persistenceKey(for: terminalSessions)) { _, _ in
                onPersist()
            }
            .onChange(of: activeTerminalSessionID) { _, _ in
                onPersist()
            }
            .onChange(of: isEnabled) { _, enabled in
                if enabled {
                    onEnable()
                } else {
                    onDisable()
                }
            }
    }
}
