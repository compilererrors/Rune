import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

public struct RuneRootView: View {
    @Environment(\.openSettings) var openSettings
    @Environment(\.dynamicTypeSize) var dynamicTypeSize
    @ObservedObject var viewModel: RuneAppViewModel

    let onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)?
    let debugDisableBootstrap: Bool
    let debugDisableLayoutPersistence: Bool
    let forcedShellVariant: RuneRootShellVariant?
    let forcedManifestInlineEditorImplementation: ManifestInlineEditorImplementation?
    let forcedInitialSidebarWidth: Double?
    let forcedInitialDetailWidth: Double?
    let workspaceChromeMountDelayNanoseconds: UInt64 = 80_000_000

    @AppStorage(RuneSettingsKeys.layoutSidebarWidth) var persistedSidebarWidth = 280.0
    @AppStorage(RuneSettingsKeys.layoutDetailWidth) var persistedDetailWidth = 440.0
    @AppStorage(RuneSettingsKeys.layoutPodNameColumnWidth) var persistedPodNameColumnWidth = Double(PodTableLayout.nameColumnDefaultWidth)
    @AppStorage(RuneSettingsKeys.saveLastAppState) var saveLastAppState = true
    @AppStorage(RuneSettingsKeys.showHoverTooltips) var showHoverTooltips = true
    @AppStorage(RuneSettingsKeys.showSplitResizeHandles) var showSplitResizeHandles = false
    @AppStorage(RuneSettingsKeys.simpleMode) var simpleMode = false
    @AppStorage(RuneSettingsKeys.skipClusterOnTabNavigationFromSections) var skipClusterOnTabNavigationFromSections = false
    @AppStorage(RuneSettingsKeys.appearanceTheme) var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault
    @AppStorage(RuneSettingsKeys.interfaceLanguage) var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault
    @AppStorage(RuneSettingsKeys.terminalFontSize) var configuredAppFontSize =
        RuneSettingsKeys.terminalFontSizeDefault
    @State var layoutGeneration = 0
    @State var layoutProbeFrames: [RuneRootLayoutProbeKind: CGRect] = [:]
    @State var lastLayoutSnapshot: RuneRootLayoutSnapshot?
    @State var podInspectorTab: PodInspectorTab = .overview
    @State var serviceInspectorTab: ServiceInspectorTab = .overview
    @State var deploymentInspectorTab: DeploymentInspectorTab = .overview
    @State var showsHistoricalDeploymentReplicaSets = false
    @State var helmInspectorTab: HelmInspectorTab = .overview
    @State var helmBrowserTab: HelmBrowserTab = .releases
    @State var genericResourceManifestTab: GenericResourceManifestTab = .overview
    @State var yamlManifestIsEditing = false
    @State var isYAMLEditorSheetPresented = false
    @State var terminalShellPodID = ""
    @State var terminalPortForwardPodID = ""
    @State var terminalLogTabState = TerminalPodLogTabState()
    @State var terminalInspectorTab: TerminalInspectorTab = .defaultTab
    @State var hasRestoredTerminalWorkspaceState = false
    @State var liveDebugScenarioStarted = false
    @State var keyboardPaneFocus: RuneRootKeyboardPane = .sidebarSections
    @State var isGenericResourceComparisonPresented = false
    @State var didCopyGenericResourceComparison = false
    @State var overviewCardSelectionIndex = 0
    @State var expandedOverviewInsightPanels: Set<OverviewInsightPanelID> = []
    @State var localKeyEventMonitor: Any?
    @State var localMouseEventMonitor: Any?
    @State var workspaceWindowReference = RuneWorkspaceWindowReference()
    @State var addClusterPopoverPresented = false
    @State var selectedAddClusterProvider: RuneAddClusterProvider?
    @State var cloudCredentialDraft = CloudCredentialDraft()
    @State var addClusterNativeContextOptions: [AddClusterNativeContextOption] = []
    @State var selectedAddClusterNativeContextBindingID: String?
    @State var connectedAddClusterNativeContextBindingIDs: Set<String> = []
    @State var isCheckingAddClusterNativeProfiles = false
    @State var addClusterNativeContextAnalysisMessage: String?
    @State var addClusterNativeContextRefreshGate = RuneScopedLatestRequestGate()
    @State var isManualAddClusterExpanded = false
    @State var isAddClusterProviderAdvancedImportExpanded = false
    @State var isAddClusterProviderToolsExpanded = false
    @State var isAddClusterProviderCommandDetailsExpanded = false
    @State var isAddClusterProviderLoginOutputExpanded = false
    @State var hasMountedWorkspaceChrome = false
    @State var isManualNamespaceSheetPresented = false
    @State var manualNamespaceInput = ""
    @State var isAuthDoctorPanelExpanded = false
    @State var isConfirmingPendingWriteActionFromDialog = false
    @FocusState var textInputFocus: RuneRootTextInputFocus?

    var interfaceLanguage: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    func appString(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: interfaceLanguage)
    }

    public init(
        viewModel: RuneAppViewModel = RuneAppViewModel(),
        onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)? = nil
    ) {
        self.init(
            viewModel: viewModel,
            onLayoutSnapshotChange: onLayoutSnapshotChange,
            debugDisableBootstrap: false,
            initialPodInspectorTab: .overview,
            initialServiceInspectorTab: .overview,
            initialDeploymentInspectorTab: .overview,
            initialGenericResourceManifestTab: .overview,
            shellVariant: nil,
            manifestInlineEditorImplementation: nil
        )
    }

    init(
        viewModel: RuneAppViewModel,
        onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)?,
        debugDisableBootstrap: Bool,
        debugDisableLayoutPersistence: Bool? = nil,
        initialPodInspectorTab: PodInspectorTab = .overview,
        initialServiceInspectorTab: ServiceInspectorTab = .overview,
        initialDeploymentInspectorTab: DeploymentInspectorTab = .overview,
        initialGenericResourceManifestTab: GenericResourceManifestTab = .overview,
        shellVariant: RuneRootShellVariant? = nil,
        manifestInlineEditorImplementation: ManifestInlineEditorImplementation? = nil,
        initialYAMLInlineEditing: Bool = false,
        initialSidebarWidthOverride: Double? = nil,
        initialDetailWidthOverride: Double? = nil
    ) {
        self.viewModel = viewModel
        self.onLayoutSnapshotChange = onLayoutSnapshotChange
        self.debugDisableBootstrap = debugDisableBootstrap
        self.debugDisableLayoutPersistence = debugDisableLayoutPersistence ?? debugDisableBootstrap
        self.forcedShellVariant = shellVariant
        self.forcedManifestInlineEditorImplementation = manifestInlineEditorImplementation
        self.forcedInitialSidebarWidth = initialSidebarWidthOverride
        self.forcedInitialDetailWidth = initialDetailWidthOverride
        _podInspectorTab = State(initialValue: initialPodInspectorTab)
        _serviceInspectorTab = State(initialValue: initialServiceInspectorTab)
        _deploymentInspectorTab = State(initialValue: initialDeploymentInspectorTab)
        _genericResourceManifestTab = State(initialValue: initialGenericResourceManifestTab)
        _yamlManifestIsEditing = State(initialValue: initialYAMLInlineEditing)
    }

    public var body: some View {
        ZStack(alignment: .topLeading) {
            background
            WindowChromeConfigurator(
                onMeasuredTopInsetChange: { inset in
                    workspaceWindowReference.measuredTopInset = inset
                },
                onOwningWindowChange: { windowNumber in
                    workspaceWindowReference.windowNumber = windowNumber
                }
            )
                .frame(width: 0, height: 0)
            keyboardNavigationBridge

            GeometryReader { geometry in
                let resolvedTopInset = RuneUILayoutMetrics.resolvedWindowContentTopInset(
                    measuredInset: workspaceWindowReference.measuredTopInset
                )
                let viewportHeight = max(0, geometry.size.height - resolvedTopInset)

                if shouldMountWorkspaceChrome {
                    configuredMainSplitContainer
                        .frame(width: geometry.size.width, height: viewportHeight, alignment: .topLeading)
                        .offset(y: resolvedTopInset)
                        .clipped()
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                }
            }

            if shouldShowLaunchExperience {
                launchExperienceOverlay
                    .zIndex(10)
            }
        }
        .coordinateSpace(name: RuneRootLayoutDebug.coordinateSpaceName)
        .onAppear {
            handleRootAppear()
        }
        .onDisappear {
            handleRootDisappear()
        }
        .sheet(isPresented: kubeConfigImportReviewPresentedBinding) {
            if let review = pendingKubeConfigImportReview {
                KubeConfigImportReviewSheet(
                    review: review,
                    duplicateHandlingChoice: $viewModel.kubeConfigDuplicateHandlingChoice,
                    metadataDrafts: viewModel.kubeConfigImportContextMetadataDrafts,
                    isCommitInProgress: viewModel.isCommittingKubeConfigImport,
                    canConfirm: viewModel.canConfirmKubeConfigImport,
                    onUpdateMetadata: { contextName, metadata in
                        viewModel.setKubeConfigImportContextMetadata(
                            contextName: contextName,
                            alias: metadata.alias,
                            colorKey: metadata.colorKey,
                            iconName: metadata.iconName,
                            tags: metadata.tags,
                            group: metadata.group
                        )
                    },
                    onConfirm: viewModel.confirmKubeConfigImport,
                    onCancel: viewModel.cancelKubeConfigImport
                )
            }
        }
        .sheet(isPresented: kubeConfigContextRemovalPresentedBinding) {
            if let preview = viewModel.pendingKubeConfigContextRemoval {
                KubeConfigContextRemovalSheet(
                    preview: preview,
                    isRemoving: viewModel.isRemovingKubeConfigContext,
                    errorMessage: viewModel.kubeConfigContextRemovalError,
                    onConfirm: viewModel.confirmKubeConfigContextRemoval,
                    onCancel: viewModel.cancelKubeConfigContextRemoval
                )
            }
        }
        .onPreferenceChange(RuneRootLayoutFramePreferenceKey.self) { frames in
            guard onLayoutSnapshotChange != nil
                || RuneRootLayoutDebug.isEnabled
                || RuneRootLayoutDebug.liveScenarioEnabled else { return }
            layoutProbeFrames = frames.compactMapValues { frame in
                guard frame.generation == layoutGeneration else { return nil }
                return frame.rect
            }
            emitLayoutSnapshotIfNeeded()
        }
        .onChange(of: viewModel.state.selectedSection) { _, section in
            advanceLayoutGeneration()
            guard section == .overview, !overviewCardModules.isEmpty else { return }
            overviewCardSelectionIndex = min(overviewCardSelectionIndex, overviewCardModules.count - 1)
        }
        .onChange(of: viewModel.state.selectedWorkloadKind) { _, _ in
            advanceLayoutGeneration()
        }
        .modifier(helmSelectionSyncModifier)
        .onChange(of: podInspectorTab) { _, tab in
            syncSavedWorkspaceInspectorState()
            if tab == .logs {
                viewModel.reloadLogsForSelection()
            }
        }
        .onChange(of: deploymentInspectorTab) { _, tab in
            syncSavedWorkspaceInspectorState()
            if tab == .unifiedLogs {
                viewModel.reloadLogsForSelection()
            } else if tab == .rollout, simpleMode {
                viewModel.refreshReplicaSetsForCurrentNamespace()
            }
        }
        .onChange(of: serviceInspectorTab) { _, tab in
            syncSavedWorkspaceInspectorState()
            if tab == .unifiedLogs {
                viewModel.reloadLogsForSelection()
            }
        }
        .onChange(of: inspectorStateSyncIdentity) { _, _ in
            syncSavedWorkspaceInspectorState()
        }
        .onChange(of: viewModel.workspaceCommandRequest) { _, request in
            guard let request else { return }
            handleWorkspaceCommand(request.command)
        }
        .modifier(terminalWorkspacePersistenceLifecycleModifier)
        .onChange(of: viewModel.terminalWorkspaceRestoreScopeID) { oldScope, newScope in
            guard oldScope != newScope else { return }
            hasRestoredTerminalWorkspaceState = false
            if oldScope != nil {
                clearTerminalWorkspaceScopeSelections()
            }
            restoreTerminalWorkspaceStateIfNeeded()
        }
        .onChange(of: viewModel.isSidebarVisible) { _, isVisible in
            if !isVisible, keyboardPaneFocus == .sidebarSections || keyboardPaneFocus == .sidebarContexts {
                keyboardPaneFocus = .content
            }
        }
        .onChange(of: viewModel.isDetailPaneVisible) { _, isVisible in
            if !isVisible, keyboardPaneFocus == .detail {
                keyboardPaneFocus = .content
            }
        }
        .onChange(of: skipClusterOnTabNavigationFromSections) { _, shouldSkip in
            if shouldSkip, keyboardPaneFocus == .sidebarContexts {
                keyboardPaneFocus = .sidebarSections
            }
        }
        .onPreferenceChange(RuneRootPaneWidthPreferenceKey.self) { paneWidths in
            persistPaneWidthsIfNeeded(paneWidths)
        }
        .animation(.easeInOut(duration: 0.2), value: viewModel.isProductionContext)
        .animation(.easeOut(duration: 0.16), value: shouldShowLaunchExperience)
        .runeInterfaceTypography(
            configuredFontSize: configuredAppFontSize,
            systemDynamicTypeSize: dynamicTypeSize
        )
        .runeAppearanceTheme(activeAppearanceTheme)
    }
}
