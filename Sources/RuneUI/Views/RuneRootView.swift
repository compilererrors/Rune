import AppKit
import RuneCore
import SwiftUI

/// Layout metrics for regression tests (`RuneRootViewLayoutRegressionTests`) and optional `RUNE_DEBUG_LAYOUT` logging — not the removed step-by-step scenario runner.
public struct RuneRootLayoutSnapshot: Equatable, Sendable {
    public let section: RuneSection
    public let workloadKind: KubeResourceKind
    public let measuredWindowTopInset: CGFloat?
    public let resolvedWindowTopInset: CGFloat
    public let contentMinY: CGFloat?
    public let headerMinY: CGFloat?
    public let detailMinY: CGFloat?
    /// Leading edges of layout probes (window space). Used by UI tests to catch horizontal drift / “offset” when swapping inspectors or editors.
    public let contentMinX: CGFloat?
    public let headerMinX: CGFloat?
    public let detailMinX: CGFloat?

    public init(
        section: RuneSection,
        workloadKind: KubeResourceKind,
        measuredWindowTopInset: CGFloat?,
        resolvedWindowTopInset: CGFloat,
        contentMinY: CGFloat?,
        headerMinY: CGFloat?,
        detailMinY: CGFloat?,
        contentMinX: CGFloat? = nil,
        headerMinX: CGFloat? = nil,
        detailMinX: CGFloat? = nil
    ) {
        self.section = section
        self.workloadKind = workloadKind
        self.measuredWindowTopInset = measuredWindowTopInset
        self.resolvedWindowTopInset = resolvedWindowTopInset
        self.contentMinY = contentMinY
        self.headerMinY = headerMinY
        self.detailMinY = detailMinY
        self.contentMinX = contentMinX
        self.headerMinX = headerMinX
        self.detailMinX = detailMinX
    }
}

private enum RuneRootLayoutProbeKind: Hashable {
    case content
    case header
    case detail
}

private enum RuneRootPaneWidthKind: Hashable {
    case sidebar
    case detail
}

private enum RuneRootLiveDebugScenarioStep: String, CaseIterable {
    case overview
    case workloadPodOverview
    case workloadPodYAML
    case workloadPodDescribe
    case workloadDeploymentOverview
    case workloadDeploymentYAML
    case workloadDeploymentDescribe
    case networkingServiceOverview
    case networkingServiceYAML
    case networkingServiceDescribe
    case configConfigMapPrepare
    case configConfigMapYAML
    case configConfigMapDescribe
    case rbacRole
    case terminal
}

private struct RuneRootLayoutProbeFrame: Equatable {
    let generation: Int
    let rect: CGRect
}

private struct RuneRootPaneWidthPreferenceKey: PreferenceKey {
    static let defaultValue: [RuneRootPaneWidthKind: CGFloat] = [:]

    static func reduce(
        value: inout [RuneRootPaneWidthKind: CGFloat],
        nextValue: () -> [RuneRootPaneWidthKind: CGFloat]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private struct RuneRootPaneWidthReporter: View {
    let kind: RuneRootPaneWidthKind

    var body: some View {
        GeometryReader { proxy in
            Color.clear.preference(
                key: RuneRootPaneWidthPreferenceKey.self,
                value: [kind: proxy.size.width]
            )
        }
    }
}

private enum RuneRootLayoutDebug {
    static let coordinateSpaceName = "RuneRootLayoutSpace"
    static let isEnabled = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT"] == "1"
    static let liveScenarioEnabled = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_LIVE_SCENARIO"] == "1"
    static let liveScenarioExitWhenDone = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_LIVE_SCENARIO_EXIT"] == "1"
    static let liveScenarioContextName = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_CONTEXT"]?
        .trimmingCharacters(in: .whitespacesAndNewlines)
    static let liveScenarioNamespace = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_NAMESPACE"]?
        .trimmingCharacters(in: .whitespacesAndNewlines)
    static let liveScenarioPodDwellNanoseconds: UInt64 = {
        guard let rawValue = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_POD_DWELL_MS"]?
            .trimmingCharacters(in: .whitespacesAndNewlines),
              let milliseconds = UInt64(rawValue),
              milliseconds > 0 else {
            return 3_500_000_000
        }
        return milliseconds * 1_000_000
    }()

    static func log(
        _ snapshot: RuneRootLayoutSnapshot,
        shellVariant: RuneRootShellVariant,
        inlineEditorImplementation: ManifestInlineEditorImplementation
    ) {
        guard isEnabled else { return }

        NSLog(
            "[Rune][Layout] shell=%@ editor=%@ section=%@ kind=%@ measuredTopInset=%@ resolvedTopInset=%.1f content=(%.1f,%.1f) header=(%.1f,%.1f) detail=(%.1f,%.1f)",
            shellVariant.debugLabel,
            inlineEditorImplementation.debugLabel,
            snapshot.section.rawValue,
            snapshot.workloadKind.kubectlName,
            snapshot.measuredWindowTopInset.map { String(format: "%.1f", $0) } ?? "nil",
            snapshot.resolvedWindowTopInset,
            snapshot.contentMinX ?? -1,
            snapshot.contentMinY ?? -1,
            snapshot.headerMinX ?? -1,
            snapshot.headerMinY ?? -1,
            snapshot.detailMinX ?? -1,
            snapshot.detailMinY ?? -1
        )
    }

    static func logScenario(_ step: RuneRootLiveDebugScenarioStep, status: String, detail: String = "") {
        guard isEnabled || liveScenarioEnabled else { return }
        NSLog("[Rune][LayoutScenario] step=%@ status=%@ %@", step.rawValue, status, detail)
    }

}

private struct RuneRootLayoutFramePreferenceKey: PreferenceKey {
    static let defaultValue: [RuneRootLayoutProbeKind: RuneRootLayoutProbeFrame] = [:]

    static func reduce(
        value: inout [RuneRootLayoutProbeKind: RuneRootLayoutProbeFrame],
        nextValue: () -> [RuneRootLayoutProbeKind: RuneRootLayoutProbeFrame]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private struct RuneRootLayoutProbe: View {
    let kind: RuneRootLayoutProbeKind
    let generation: Int

    var body: some View {
        GeometryReader { proxy in
            Color.clear.preference(
                key: RuneRootLayoutFramePreferenceKey.self,
                value: [kind: RuneRootLayoutProbeFrame(
                    generation: generation,
                    rect: proxy.frame(in: .named(RuneRootLayoutDebug.coordinateSpaceName))
                )]
            )
        }
    }
}

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
    case describe
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .describe: return "Describe"
        case .yaml: return "YAML"
        }
    }
}

private enum PodTableLayout {
    static let metricsSpacing: CGFloat = 10
    static let rowHorizontalPadding: CGFloat = 10
    static let rowVerticalPadding: CGFloat = 5
    static let listRowEdgeInset: CGFloat = 4
    static let cpuWidth: CGFloat = 44
    static let memoryWidth: CGFloat = 56
    static let restartsWidth: CGFloat = 56
    static let ageWidth: CGFloat = 44
    static let statusTextWidth: CGFloat = 120
    static let statusHorizontalPadding: CGFloat = 8
    static let statusTotalWidth: CGFloat = statusTextWidth + (statusHorizontalPadding * 2)
    static let headerHorizontalInset: CGFloat = rowHorizontalPadding + listRowEdgeInset
    /// Space between column headers and first row — enough to avoid a cramped look without excess air.
    static let headerBottomSpacing: CGFloat = 10
}

private enum HelmInspectorTab: String, CaseIterable, Identifiable {
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

private enum RuneRootKeyboardPane: CaseIterable {
    case sidebarSections
    case sidebarContexts
    case content
    case detail

    func advanced(forward: Bool) -> RuneRootKeyboardPane {
        let all = Self.allCases
        guard let index = all.firstIndex(of: self) else { return self }
        if forward {
            return all[(index + 1) % all.count]
        }
        return all[(index + all.count - 1) % all.count]
    }
}

private enum RuneRootTextInputFocus: Hashable {
    case contextSearch
    case resourceFilter
}

public struct RuneRootView: View {
    @Environment(\.openSettings) private var openSettings
    @ObservedObject private var viewModel: RuneAppViewModel

    private let onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)?
    private let debugDisableBootstrap: Bool
    private let forcedShellVariant: RuneRootShellVariant?
    private let forcedManifestInlineEditorImplementation: ManifestInlineEditorImplementation?
    private let forcedInitialSidebarWidth: Double?
    private let forcedInitialDetailWidth: Double?

    @AppStorage(RuneSettingsKeys.layoutSidebarWidth) private var persistedSidebarWidth = 280.0
    @AppStorage(RuneSettingsKeys.layoutDetailWidth) private var persistedDetailWidth = 440.0
    @State private var measuredWindowContentTopInset: CGFloat?
    @State private var layoutGeneration = 0
    @State private var layoutProbeFrames: [RuneRootLayoutProbeKind: CGRect] = [:]
    @State private var lastLayoutSnapshot: RuneRootLayoutSnapshot?
    @State private var podInspectorTab: PodInspectorTab = .overview
    @State private var serviceInspectorTab: ServiceInspectorTab = .overview
    @State private var deploymentInspectorTab: DeploymentInspectorTab = .overview
    @State private var helmInspectorTab: HelmInspectorTab = .overview
    @State private var genericResourceManifestTab: GenericResourceManifestTab = .describe
    @State private var yamlManifestIsEditing = false
    @State private var isYAMLEditorSheetPresented = false
    @State private var liveDebugScenarioStarted = false
    @State private var keyboardPaneFocus: RuneRootKeyboardPane = .sidebarSections
    @State private var overviewCardSelectionIndex = 0
    @State private var localKeyEventMonitor: Any?
    @FocusState private var textInputFocus: RuneRootTextInputFocus?

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
            initialGenericResourceManifestTab: .describe,
            shellVariant: nil,
            manifestInlineEditorImplementation: nil
        )
    }

    init(
        viewModel: RuneAppViewModel,
        onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)?,
        debugDisableBootstrap: Bool,
        initialPodInspectorTab: PodInspectorTab = .overview,
        initialServiceInspectorTab: ServiceInspectorTab = .overview,
        initialDeploymentInspectorTab: DeploymentInspectorTab = .overview,
        initialGenericResourceManifestTab: GenericResourceManifestTab = .describe,
        shellVariant: RuneRootShellVariant? = nil,
        manifestInlineEditorImplementation: ManifestInlineEditorImplementation? = nil,
        initialYAMLInlineEditing: Bool = false,
        initialSidebarWidthOverride: Double? = nil,
        initialDetailWidthOverride: Double? = nil
    ) {
        self.viewModel = viewModel
        self.onLayoutSnapshotChange = onLayoutSnapshotChange
        self.debugDisableBootstrap = debugDisableBootstrap
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
            WindowChromeConfigurator(measuredTopInset: $measuredWindowContentTopInset)
                .frame(width: 0, height: 0)
            keyboardNavigationBridge

            GeometryReader { geometry in
                let resolvedTopInset = RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: measuredWindowContentTopInset)
                let viewportHeight = max(0, geometry.size.height - resolvedTopInset)

                configuredMainSplitContainer
                    .frame(width: geometry.size.width, height: viewportHeight, alignment: .topLeading)
                    .offset(y: resolvedTopInset)
                    .clipped()
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            }
        }
        .coordinateSpace(name: RuneRootLayoutDebug.coordinateSpaceName)
        .onPreferenceChange(RuneRootLayoutFramePreferenceKey.self) { frames in
            layoutProbeFrames = frames.compactMapValues { frame in
                guard frame.generation == layoutGeneration else { return nil }
                return frame.rect
            }
            emitLayoutSnapshotIfNeeded()
        }
        .onChange(of: measuredWindowContentTopInset) { _, _ in
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
        .onPreferenceChange(RuneRootPaneWidthPreferenceKey.self) { paneWidths in
            persistPaneWidthsIfNeeded(paneWidths)
        }
        .animation(.easeInOut(duration: 0.2), value: viewModel.isProductionContext)
    }

    private var configuredMainSplitContainer: some View {
        mainSplitContainer
            .toolbar {
                ToolbarItemGroup(placement: .navigation) {
                    Button {
                        viewModel.navigateBack()
                    } label: {
                        Image(systemName: "chevron.left")
                    }
                    .help("Back")
                    .disabled(!viewModel.canNavigateBack)
                    .keyboardShortcut("[", modifiers: [.command, .option])

                    Button {
                        viewModel.navigateForward()
                    } label: {
                        Image(systemName: "chevron.right")
                    }
                    .help("Forward")
                    .disabled(!viewModel.canNavigateForward)
                    .keyboardShortcut("]", modifiers: [.command, .option])

                    Menu(viewModel.state.selectedContext?.name ?? "No Context") {
                        ForEach(viewModel.visibleContexts) { context in
                            Button(context.name) {
                                viewModel.setContext(context)
                            }
                        }
                    }

                    Menu(namespaceMenuTitle) {
                        ForEach(namespaceSuggestions, id: \.self) { namespace in
                            Button(namespace) {
                                viewModel.setNamespace(namespace)
                            }
                        }
                    }
                }

                ToolbarItemGroup(placement: .primaryAction) {
                    Button {
                        openSettingsWindow()
                    } label: {
                        Image(systemName: "gearshape")
                    }
                    .help("Settings")

                    Button("Palette") {
                        viewModel.presentCommandPalette()
                    }
                    .keyboardShortcut("k", modifiers: .command)

                    Button("Reload") {
                        viewModel.refreshCurrentView(debounced: false)
                    }
                    .keyboardShortcut("r", modifiers: .command)
                }
            }
            .toolbarBackground(.visible, for: .windowToolbar)
            .sheet(isPresented: commandPalettePresentedBinding) {
                CommandPaletteView(viewModel: viewModel)
            }
            .sheet(isPresented: $isYAMLEditorSheetPresented) {
                yamlManifestEditorSheet()
            }
            .confirmationDialog(
                viewModel.pendingWriteActionTitle,
                isPresented: pendingWriteActionPresentedBinding,
                titleVisibility: .visible
            ) {
                if viewModel.pendingWriteActionIsDestructive {
                    Button(viewModel.pendingWriteActionConfirmLabel, role: .destructive) {
                        viewModel.confirmPendingWriteAction()
                    }
                } else {
                    Button(viewModel.pendingWriteActionConfirmLabel) {
                        viewModel.confirmPendingWriteAction()
                    }
                }

                Button("Cancel", role: .cancel) {
                    viewModel.cancelPendingWriteAction()
                }
            } message: {
                Text(viewModel.pendingWriteActionMessage)
            }
            .onAppear {
                keyboardPaneFocus = .sidebarSections
                textInputFocus = nil
                DispatchQueue.main.async {
                    NSApp.keyWindow?.makeFirstResponder(nil)
                }
                installLocalKeyboardMonitorIfNeeded()
                if RuneRootLayoutDebug.isEnabled {
                    NSLog(
                        "[Rune][Layout] configured shell=%@ editor=%@",
                        resolvedShellVariant.debugLabel,
                        resolvedManifestInlineEditorImplementation.debugLabel
                    )
                }
                startLiveDebugScenarioIfNeeded()
                guard !debugDisableBootstrap else { return }
                viewModel.bootstrapIfNeeded()
            }
            .onDisappear {
                removeLocalKeyboardMonitor()
            }
    }

    private var resolvedShellVariant: RuneRootShellVariant {
        RuneRootShellVariant.resolved(override: forcedShellVariant)
    }

    private var resolvedManifestInlineEditorImplementation: ManifestInlineEditorImplementation {
        ManifestInlineEditorImplementation.resolved(override: forcedManifestInlineEditorImplementation)
    }

    private func startLiveDebugScenarioIfNeeded() {
        guard RuneRootLayoutDebug.liveScenarioEnabled else { return }
        guard !liveDebugScenarioStarted else { return }
        liveDebugScenarioStarted = true

        Task { @MainActor in
            await runLiveDebugScenario()
        }
    }

    @MainActor
    private func runLiveDebugScenario() async {
        RuneRootLayoutDebug.logScenario(
            .overview,
            status: "start",
            detail: "shell=\(resolvedShellVariant.debugLabel) editor=\(resolvedManifestInlineEditorImplementation.debugLabel)"
        )

        await waitForLiveScenarioReady()

        for step in RuneRootLiveDebugScenarioStep.allCases {
            await performLiveDebugScenarioStep(step)
        }

        RuneRootLayoutDebug.logScenario(.overview, status: "finished")

        if RuneRootLayoutDebug.liveScenarioExitWhenDone {
            NSApp.terminate(nil)
        }
    }

    @MainActor
    private func waitForLiveScenarioReady() async {
        let timeout = Date().addingTimeInterval(20)
        while Date() < timeout {
            if viewModel.state.selectedContext != nil, !viewModel.visibleContexts.isEmpty {
                var appliedOverride = false

                if let contextName = RuneRootLayoutDebug.liveScenarioContextName, !contextName.isEmpty,
                   let context = viewModel.visibleContexts.first(where: { $0.name == contextName }),
                   viewModel.state.selectedContext != context {
                    viewModel.setContext(context)
                    appliedOverride = true
                }
                if let namespace = RuneRootLayoutDebug.liveScenarioNamespace, !namespace.isEmpty,
                   viewModel.state.selectedNamespace != namespace {
                    viewModel.setNamespace(namespace)
                    appliedOverride = true
                }

                let contextSettled =
                    RuneRootLayoutDebug.liveScenarioContextName == nil
                    || RuneRootLayoutDebug.liveScenarioContextName?.isEmpty == true
                    || viewModel.state.selectedContext?.name == RuneRootLayoutDebug.liveScenarioContextName
                let namespaceSettled =
                    RuneRootLayoutDebug.liveScenarioNamespace == nil
                    || RuneRootLayoutDebug.liveScenarioNamespace?.isEmpty == true
                    || viewModel.state.selectedNamespace == RuneRootLayoutDebug.liveScenarioNamespace

                if appliedOverride, contextSettled, namespaceSettled {
                    viewModel.refreshCurrentView()
                }

                if contextSettled && namespaceSettled {
                    try? await Task.sleep(nanoseconds: 700_000_000)
                    return
                }
            }
            try? await Task.sleep(nanoseconds: 200_000_000)
        }
    }

    @MainActor
    private func performLiveDebugScenarioStep(_ step: RuneRootLiveDebugScenarioStep) async {
        RuneRootLayoutDebug.logScenario(step, status: "begin")

        let applied = applyLiveDebugScenarioStep(step)
        if !applied {
            RuneRootLayoutDebug.logScenario(step, status: "skip")
            return
        }

        let dwellNanoseconds = liveDebugScenarioDwellNanoseconds(for: step)
        RuneRootLayoutDebug.logScenario(step, status: "settling", detail: "dwellMs=\(dwellNanoseconds / 1_000_000)")
        try? await Task.sleep(nanoseconds: dwellNanoseconds)

        if let snapshot = lastLayoutSnapshot {
            RuneRootLayoutDebug.logScenario(
                step,
                status: "snapshot",
                detail: "content=(\(snapshot.contentMinX ?? -1),\(snapshot.contentMinY ?? -1)) detail=(\(snapshot.detailMinX ?? -1),\(snapshot.detailMinY ?? -1))"
            )
        } else {
            RuneRootLayoutDebug.logScenario(step, status: "snapshot-missing")
        }
    }

    private func liveDebugScenarioDwellNanoseconds(for step: RuneRootLiveDebugScenarioStep) -> UInt64 {
        switch step {
        case .workloadPodOverview, .workloadPodYAML, .workloadPodDescribe:
            return RuneRootLayoutDebug.liveScenarioPodDwellNanoseconds
        default:
            return 900_000_000
        }
    }

    @MainActor
    private func applyLiveDebugScenarioStep(_ step: RuneRootLiveDebugScenarioStep) -> Bool {
        switch step {
        case .overview:
            viewModel.setSection(.overview)
            return true
        case .workloadPodOverview:
            guard let pod = viewModel.visiblePods.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.selectPod(pod)
            podInspectorTab = .overview
            yamlManifestIsEditing = false
            return true
        case .workloadPodYAML:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .workloadPodDescribe:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .describe
            return true
        case .workloadDeploymentOverview:
            guard let deployment = viewModel.visibleDeployments.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.selectDeployment(deployment)
            deploymentInspectorTab = .overview
            yamlManifestIsEditing = false
            return true
        case .workloadDeploymentYAML:
            guard viewModel.state.selectedDeployment != nil || viewModel.visibleDeployments.first != nil else { return false }
            if viewModel.state.selectedDeployment == nil, let deployment = viewModel.visibleDeployments.first {
                viewModel.setSection(.workloads)
                viewModel.selectDeployment(deployment)
            }
            deploymentInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .workloadDeploymentDescribe:
            guard viewModel.state.selectedDeployment != nil || viewModel.visibleDeployments.first != nil else { return false }
            if viewModel.state.selectedDeployment == nil, let deployment = viewModel.visibleDeployments.first {
                viewModel.setSection(.workloads)
                viewModel.selectDeployment(deployment)
            }
            deploymentInspectorTab = .describe
            return true
        case .networkingServiceOverview:
            guard let service = viewModel.visibleServices.first else { return false }
            viewModel.setSection(.networking)
            viewModel.selectService(service)
            serviceInspectorTab = .overview
            yamlManifestIsEditing = false
            return true
        case .networkingServiceYAML:
            guard viewModel.state.selectedService != nil || viewModel.visibleServices.first != nil else { return false }
            if viewModel.state.selectedService == nil, let service = viewModel.visibleServices.first {
                viewModel.setSection(.networking)
                viewModel.selectService(service)
            }
            serviceInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .networkingServiceDescribe:
            guard viewModel.state.selectedService != nil || viewModel.visibleServices.first != nil else { return false }
            if viewModel.state.selectedService == nil, let service = viewModel.visibleServices.first {
                viewModel.setSection(.networking)
                viewModel.selectService(service)
            }
            serviceInspectorTab = .describe
            return true
        case .configConfigMapPrepare:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            if let configMap = viewModel.visibleConfigMaps.first {
                viewModel.selectConfigMap(configMap)
            } else {
                viewModel.refreshCurrentView()
            }
            yamlManifestIsEditing = false
            return true
        case .configConfigMapYAML:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            guard let configMap = viewModel.visibleConfigMaps.first else { return false }
            viewModel.selectConfigMap(configMap)
            genericResourceManifestTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .configConfigMapDescribe:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            guard viewModel.state.selectedConfigMap != nil || viewModel.visibleConfigMaps.first != nil else { return false }
            if viewModel.state.selectedConfigMap == nil, let configMap = viewModel.visibleConfigMaps.first {
                viewModel.selectConfigMap(configMap)
            }
            genericResourceManifestTab = .describe
            return true
        case .rbacRole:
            guard let resource = viewModel.visibleRBACResources.first else { return false }
            viewModel.setSection(.rbac)
            viewModel.selectRBACResource(resource)
            genericResourceManifestTab = .describe
            return true
        case .terminal:
            viewModel.setSection(.terminal)
            yamlManifestIsEditing = false
            return true
        }
    }

    /// Three-column workspace — shell can be tested under both native `NavigationSplitView` and AppKit-backed split behavior.
    @ViewBuilder
    private var mainSplitContainer: some View {
        switch resolvedShellVariant {
        case .navigationSplitView:
            NavigationSplitView {
                sidebar
                    .runeAppKitFrameReporter("sidebar")
                    .background(RuneRootPaneWidthReporter(kind: .sidebar))
                    .overlay(alignment: .trailing) {
                        splitColumnResizeHandle
                            .offset(x: 7)
                    }
                    .navigationSplitViewColumnWidth(
                        min: RuneUILayoutMetrics.splitSidebarMinWidth,
                        ideal: resolvedSidebarWidth,
                        max: RuneUILayoutMetrics.splitSidebarMaxWidth
                    )
            } content: {
                contentPane
                    .runeAppKitFrameReporter("content")
                    .overlay(alignment: .trailing) {
                        splitColumnResizeHandle
                            .offset(x: 7)
                    }
                    .navigationSplitViewColumnWidth(
                        min: RuneUILayoutMetrics.splitContentColumnMinWidth,
                        ideal: 760,
                        max: RuneUILayoutMetrics.splitContentColumnMaxWidth
                    )
            } detail: {
                detailPane
                    .runeAppKitFrameReporter("detail")
                    .background(RuneRootPaneWidthReporter(kind: .detail))
                    .navigationSplitViewColumnWidth(
                        min: RuneUILayoutMetrics.splitDetailColumnMinWidth,
                        ideal: resolvedDetailWidth,
                        max: RuneUILayoutMetrics.splitDetailColumnMaxWidth
                    )
            }
            .navigationSplitViewStyle(.balanced)
        case .appKitSplitView:
            AppKitTripleSplitView(
                sidebar: AnyView(
                    sidebar
                        .runeAppKitFrameReporter("sidebar")
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                ),
                content: AnyView(
                    contentPane
                        .runeAppKitFrameReporter("content")
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                ),
                detail: AnyView(
                    detailPane
                        .runeAppKitFrameReporter("detail")
                ),
                sidebarWidth: resolvedSidebarWidth,
                detailWidth: resolvedDetailWidth,
                onSidebarWidthChange: { width in
                    persistSidebarWidthIfNeeded(width)
                },
                onDetailWidthChange: { width in
                    persistDetailWidthIfNeeded(width)
                }
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }

    private var background: some View {
        RuneGlassPaneSurface(role: .window)
            .ignoresSafeArea()
    }

    private var sidebar: some View {
        VStack(alignment: .leading, spacing: 14) {
            TextField("Search contexts", text: Binding(get: {
                viewModel.state.contextSearchQuery
            }, set: { newValue in
                viewModel.setContextSearchQuery(newValue)
            }))
            .textFieldStyle(.roundedBorder)
            .focused($textInputFocus, equals: .contextSearch)

            Text("Sections")
                .font(.headline)
                .foregroundStyle(keyboardPaneFocus == .sidebarSections ? Color.accentColor : .secondary)

            ForEach(RuneSection.allCases) { section in
                sectionRow(section)
            }

            Divider()
                .overlay(Color(nsColor: .separatorColor))

            Text("Contexts")
                .font(.headline)
                .foregroundStyle(keyboardPaneFocus == .sidebarContexts ? Color.accentColor : .secondary)

            ScrollView {
                VStack(alignment: .leading, spacing: 8) {
                    if viewModel.visibleContexts.isEmpty {
                        VStack(alignment: .leading, spacing: 10) {
                            Text("No kubeconfigs loaded")
                                .font(.subheadline.weight(.semibold))
                            Text("Rune discovers kubeconfig files automatically. You can also import files with the system file picker.")
                                .font(.footnote)
                                .foregroundStyle(.secondary)

                            Button("Import Kubeconfig…") {
                                viewModel.importKubeConfig()
                            }
                            .buttonStyle(.borderedProminent)
                        }
                        .runePanelCard(padding: 10)
                    } else {
                        ForEach(viewModel.visibleContexts) { context in
                            contextRow(context)
                        }
                    }
                }
            }

            Toggle(isOn: Binding(get: {
                viewModel.state.isReadOnlyMode
            }, set: { value in
                viewModel.setReadOnlyMode(value)
            })) {
                Text("Read-only mode")
                    .font(.headline)
            }

            Spacer(minLength: 0)
        }
        .padding(.horizontal, RuneUILayoutMetrics.sidebarPadding)
        .padding(.bottom, RuneUILayoutMetrics.sidebarPadding)
        .padding(.top, 8)
        .frame(minWidth: 0, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background {
            ZStack(alignment: .trailing) {
                RuneGlassPaneSurface(role: .sidebar)
                RuneGlassPaneBorder(role: .sidebar)
                paneFocusOutline(isFocused: keyboardPaneFocus == .sidebarSections || keyboardPaneFocus == .sidebarContexts)
            }
        }
    }

    private func sectionRow(_ section: RuneSection) -> some View {
        Button {
            viewModel.setSection(section)
        } label: {
            HStack(spacing: 8) {
                Image(systemName: section.symbolName)
                    .frame(width: 16)
                Text(section.title + "    ⌘" + String(section.commandShortcut))
                    .font(.body.weight(.medium))
                Spacer()
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 7)
            .runeSidebarSelection(isSelected: viewModel.state.selectedSection == section)
        }
        .buttonStyle(.plain)
    }

    private func contextRow(_ context: KubeContext) -> some View {
        HStack(spacing: 8) {
            Button {
                viewModel.setContext(context)
            } label: {
                HStack(spacing: 8) {
                    Circle()
                        .fill(viewModel.state.selectedContext == context ? Color.accentColor : Color.gray.opacity(0.6))
                        .frame(width: 7, height: 7)
                    Text(context.name)
                        .font(.body.weight(.medium))
                        .lineLimit(1)
                        .help(context.name)
                    Spacer()
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 6)
                .runeSidebarSelection(isSelected: viewModel.state.selectedContext == context)
            }
            .buttonStyle(.plain)

            Button {
                viewModel.toggleFavorite(for: context)
            } label: {
                Image(systemName: viewModel.state.isFavorite(context) ? "star.fill" : "star")
                    .foregroundStyle(viewModel.state.isFavorite(context) ? Color.yellow : Color.gray)
            }
            .buttonStyle(.plain)
        }
    }

    private var contentPane: some View {
        VStack(alignment: .leading, spacing: 12) {
            if viewModel.isProductionContext {
                HStack {
                    productionBanner
                    Spacer(minLength: 0)
                }
                .transition(.move(edge: .top).combined(with: .opacity))
            }

            contentHeader

            switch viewModel.state.selectedSection {
            case .overview:
                overviewPane
            case .workloads:
                workloadsPane
            case .networking:
                networkingPane
            case .config:
                configPane
            case .storage:
                storagePane
            case .helm:
                helmPane
            case .events:
                eventsPane
            case .terminal:
                terminalPane
            case .rbac:
                rbacPane
            }
        }
        .padding(RuneUILayoutMetrics.paneOuterPadding)
        .frame(minWidth: 0, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background {
            ZStack(alignment: .trailing) {
                RuneGlassPaneSurface(role: .content)
                RuneGlassPaneBorder(role: .content)
                RuneRootLayoutProbe(kind: .content, generation: layoutGeneration)
                paneFocusOutline(isFocused: keyboardPaneFocus == .content)
            }
        }
    }

    private var contentHeader: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text(viewModel.state.selectedSection.title)
                    .font(.title2.weight(.bold))

                if viewModel.visibleContexts.isEmpty == false, viewModel.state.selectedSection != .terminal {
                    Button {
                        viewModel.refreshCurrentView(debounced: false)
                    } label: {
                        if viewModel.state.isLoading {
                            ProgressView()
                                .scaleEffect(0.75)
                                .frame(width: 22, height: 22)
                        } else {
                            Image(systemName: "arrow.clockwise.circle")
                                .font(.title3)
                                .symbolRenderingMode(.hierarchical)
                        }
                    }
                    .buttonStyle(.plain)
                    .help("Refresh data for this section (same as ⌘R)")
                    .accessibilityLabel("Refresh section")
                }

                Spacer()

                if let context = viewModel.state.selectedContext {
                    Label(context.name, systemImage: "network")
                        .font(.caption.weight(.semibold))
                        .labelStyle(.titleAndIcon)
                        .imageScale(.small)
                        .lineLimit(1)
                        .padding(.horizontal, RuneUILayoutMetrics.headerChipHorizontalPadding)
                        .frame(height: 36)
                        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 10, style: .continuous))
                        .help(context.name)
                }

                contextUsageBadge(label: "CPU", value: contextUsageValue(viewModel.state.overviewClusterCPUPercent))
                contextUsageBadge(label: "MEM", value: contextUsageValue(viewModel.state.overviewClusterMemoryPercent))

                if viewModel.state.isReadOnlyMode {
                    Label("Read-only", systemImage: "lock.fill")
                        .font(.caption.weight(.semibold))
                        .labelStyle(.titleAndIcon)
                        .imageScale(.small)
                        .padding(.horizontal, RuneUILayoutMetrics.headerChipHorizontalPadding)
                        .frame(height: 36)
                        .background(Color.orange.opacity(0.16), in: RoundedRectangle(cornerRadius: 10, style: .continuous))
                }

                if viewModel.state.selectedSection == .events {
                    Button("Save Events") {
                        viewModel.saveVisibleEvents()
                    }
                    .buttonStyle(.bordered)
                }
            }

            if viewModel.visibleContexts.isEmpty {
                HStack(spacing: 10) {
                    Button("Import Kubeconfig…") {
                        viewModel.importKubeConfig()
                    }
                    .buttonStyle(.borderedProminent)

                    Button("Command Palette") {
                        viewModel.presentCommandPalette()
                    }
                    .buttonStyle(.bordered)

                    Spacer()
                }
                .controlSize(.large)
            } else if showsNamespaceAndFilterControls {
                namespaceAndFilterControls
            }

            sectionSpecificControls
        }
        .background(RuneRootLayoutProbe(kind: .header, generation: layoutGeneration))
    }

    private var showsNamespaceAndFilterControls: Bool {
        switch viewModel.state.selectedSection {
        case .overview, .terminal:
            return false
        default:
            return true
        }
    }

    private var namespaceAndFilterControls: some View {
        HStack(spacing: 10) {
            Label(viewModel.state.selectedNamespace.isEmpty ? "Namespace" : viewModel.state.selectedNamespace, systemImage: "square.stack.3d.up")
                .font(.caption.weight(.semibold))
                .labelStyle(.titleAndIcon)
                .imageScale(.small)
                .lineLimit(1)
                .padding(.horizontal, RuneUILayoutMetrics.headerChipHorizontalPadding)
                .frame(height: RuneUILayoutMetrics.headerChipHeight)
                .background(.thinMaterial, in: Capsule())
                .help("Change namespace from the toolbar menu or Command Palette (:ns).")

            TextField("/ filter resources", text: Binding(get: {
                viewModel.state.resourceSearchQuery
            }, set: { newValue in
                viewModel.setResourceSearchQuery(newValue)
            }))
            .textFieldStyle(.roundedBorder)
            .font(.caption)
            .controlSize(.small)
            .frame(maxWidth: 280)
            .focused($textInputFocus, equals: .resourceFilter)

            if viewModel.state.isLoading {
                ProgressView()
                    .scaleEffect(0.8)
                    .frame(height: RuneUILayoutMetrics.headerChipHeight)
            }
        }
    }

    @ViewBuilder
    private var sectionSpecificControls: some View {
        switch viewModel.state.selectedSection {
        case .workloads:
            resourceKindPicker(kinds: viewModel.workloadKinds)
        case .networking:
            resourceKindPicker(kinds: viewModel.networkingKinds)
        case .config:
            resourceKindPicker(kinds: viewModel.configKinds)
        case .storage:
            resourceKindPicker(kinds: viewModel.storageKinds)
        case .helm:
            Toggle("All namespaces", isOn: Binding(get: {
                viewModel.state.isHelmAllNamespaces
            }, set: { value in
                viewModel.setHelmAllNamespaces(value)
            }))
            .toggleStyle(.switch)
        case .rbac:
            resourceKindPicker(kinds: viewModel.rbacKinds)
        default:
            EmptyView()
        }
    }

    private func resourceKindPicker(kinds: [KubeResourceKind]) -> some View {
        RuneSegmentedPickerInScroll(
            "Kind",
            selection: Binding(get: {
                viewModel.state.selectedWorkloadKind
            }, set: { kind in
                viewModel.setWorkloadKind(kind)
            })
        ) {
            ForEach(kinds) { kind in
                Text(kind.title).tag(kind)
            }
        }
        .accessibilityLabel("Resource kind")
    }

    private var overviewCardModules: [OverviewModule] {
        [.pods, .deployments, .services, .ingresses, .configMaps, .cronJobs, .nodes, .events]
    }

    private func isOverviewCardKeyboardFocused(_ index: Int) -> Bool {
        keyboardPaneFocus == .content
            && viewModel.state.selectedSection == .overview
            && overviewCardSelectionIndex == index
    }

    private var overviewPane: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                if viewModel.visibleContexts.isEmpty {
                    VStack(alignment: .leading, spacing: 10) {
                        Text("Connect Kubernetes")
                            .font(.title3.weight(.bold))
                        Text("Rune is GUI-first. After a kubeconfig is loaded, contexts, namespaces, and resources appear here.")
                            .foregroundStyle(.secondary)

                        HStack(spacing: 10) {
                            Button("Import Kubeconfig…") {
                                viewModel.importKubeConfig()
                            }
                            .buttonStyle(.borderedProminent)

                            Button("Open Command Palette") {
                                viewModel.presentCommandPalette()
                            }
                        }
                    }
                    .runePanelCard(padding: 14)
                }

                overviewStatusBanner

                LazyVGrid(columns: [GridItem(.adaptive(minimum: 160), spacing: 10)], spacing: 10) {
                    overviewStatCard(
                        title: "Pods",
                        count: viewModel.state.overviewPods.count,
                        symbol: "cube.box.fill",
                        tint: .cyan,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(0)
                    ) {
                        overviewCardSelectionIndex = 0
                        viewModel.openOverviewModule(.pods)
                    }
                    overviewStatCard(
                        title: "Deployments",
                        count: viewModel.state.overviewDeploymentsCount,
                        symbol: "shippingbox.fill",
                        tint: .blue,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(1)
                    ) {
                        overviewCardSelectionIndex = 1
                        viewModel.openOverviewModule(.deployments)
                    }
                    overviewStatCard(
                        title: "Services",
                        count: viewModel.state.overviewServicesCount,
                        symbol: "point.3.connected.trianglepath.dotted",
                        tint: .purple,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(2)
                    ) {
                        overviewCardSelectionIndex = 2
                        viewModel.openOverviewModule(.services)
                    }
                    overviewStatCard(
                        title: "Ingresses",
                        count: viewModel.state.overviewIngressesCount,
                        symbol: "network",
                        tint: .indigo,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(3)
                    ) {
                        overviewCardSelectionIndex = 3
                        viewModel.openOverviewModule(.ingresses)
                    }
                    overviewStatCard(
                        title: "ConfigMaps",
                        count: viewModel.state.overviewConfigMapsCount,
                        symbol: "doc.text.fill",
                        tint: .teal,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(4)
                    ) {
                        overviewCardSelectionIndex = 4
                        viewModel.openOverviewModule(.configMaps)
                    }
                    overviewStatCard(
                        title: "CronJobs",
                        count: viewModel.state.overviewCronJobsCount,
                        symbol: "calendar.badge.clock",
                        tint: .mint,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(5)
                    ) {
                        overviewCardSelectionIndex = 5
                        viewModel.openOverviewModule(.cronJobs)
                    }
                    overviewStatCard(
                        title: "Nodes",
                        count: viewModel.state.overviewNodesCount,
                        symbol: "server.rack",
                        tint: .gray,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(6)
                    ) {
                        overviewCardSelectionIndex = 6
                        viewModel.openOverviewModule(.nodes)
                    }
                    overviewStatCard(
                        title: "Events",
                        count: viewModel.state.overviewEvents.count,
                        symbol: "bolt.badge.clock.fill",
                        tint: .orange,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(7)
                    ) {
                        overviewCardSelectionIndex = 7
                        viewModel.openOverviewModule(.events)
                    }
                }

                VStack(alignment: .leading, spacing: 8) {
                    Text("Pod Health")
                        .font(.headline)

                    HStack(spacing: 8) {
                        healthBadge(label: "Running", value: podStatusCount("running"), color: .green)
                        healthBadge(label: "Pending", value: podStatusCount("pending"), color: .orange)
                        healthBadge(label: "Failed", value: podStatusCount("failed"), color: .red)
                        healthBadge(label: "Other", value: max(0, viewModel.state.overviewPods.count - podStatusCount("running") - podStatusCount("pending") - podStatusCount("failed")), color: .gray)
                    }
                }
                .runePanelCard()

                VStack(alignment: .leading, spacing: 8) {
                    Text("Recent Events")
                        .font(.headline)

                    if viewModel.state.overviewEvents.isEmpty {
                        Text("No events loaded")
                            .foregroundStyle(.secondary)
                            .font(.subheadline)
                    } else {
                        ForEach(Array(viewModel.state.overviewEvents.prefix(8))) { event in
                            Button {
                                viewModel.openEventSource(event)
                            } label: {
                                HStack(alignment: .top, spacing: 8) {
                                    Image(systemName: event.type.lowercased() == "warning" ? "exclamationmark.triangle.fill" : "checkmark.circle.fill")
                                        .foregroundStyle(event.type.lowercased() == "warning" ? .orange : .green)
                                        .frame(width: 16)

                                    VStack(alignment: .leading, spacing: 2) {
                                        if let ts = event.lastTimestamp?.trimmingCharacters(in: .whitespacesAndNewlines), !ts.isEmpty {
                                            Text(ts)
                                                .font(.caption2.monospaced())
                                                .foregroundStyle(.tertiary)
                                        }
                                        Text(event.reason + " • " + event.objectName)
                                            .font(.subheadline.weight(.semibold))
                                            .foregroundStyle(.primary)
                                            .multilineTextAlignment(.leading)
                                        Text(event.message)
                                            .font(.footnote)
                                            .foregroundStyle(.secondary)
                                            .multilineTextAlignment(.leading)
                                            .lineLimit(2)
                                    }

                                    Spacer(minLength: 0)

                                    Image(systemName: "chevron.right")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(.tertiary)
                                        .padding(.top, 2)
                                }
                                .padding(.vertical, 4)
                                .padding(.horizontal, 2)
                                .contentShape(Rectangle())
                            }
                            .buttonStyle(.plain)
                            .help(eventHint(for: event))
                        }
                    }
                }
                .runePanelCard()
            }
        }
        .id("overview")
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var workloadsPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                List {
                    Section {
                        ForEach(viewModel.visiblePods) { pod in
                            Button {
                                viewModel.selectPod(pod)
                            } label: {
                                HStack(spacing: PodTableLayout.metricsSpacing) {
                                    Text(pod.name)
                                        .font(.body.weight(.medium))
                                        .lineLimit(1)
                                        .frame(maxWidth: .infinity, alignment: .leading)

                                    HStack(spacing: PodTableLayout.metricsSpacing) {
                                        Text(pod.cpuDisplay)
                                            .frame(width: PodTableLayout.cpuWidth, alignment: .trailing)
                                        Text(pod.memoryDisplay)
                                            .frame(width: PodTableLayout.memoryWidth, alignment: .trailing)
                                        Text("\(pod.totalRestarts)")
                                            .frame(width: PodTableLayout.restartsWidth, alignment: .trailing)
                                        Text(pod.ageDescription)
                                            .frame(width: PodTableLayout.ageWidth, alignment: .trailing)
                                    }
                                    .font(.system(size: 11, weight: .regular, design: .monospaced))
                                    .foregroundStyle(.secondary)

                                    Text(pod.status)
                                        .font(.caption.weight(.semibold))
                                        .lineLimit(1)
                                        .minimumScaleFactor(0.78)
                                        .multilineTextAlignment(.center)
                                        .frame(width: PodTableLayout.statusTextWidth, alignment: .center)
                                        .padding(.horizontal, PodTableLayout.statusHorizontalPadding)
                                        .padding(.vertical, 2)
                                        .background(statusColor(for: pod.status).opacity(0.22), in: Capsule())
                                        .foregroundStyle(statusColor(for: pod.status))
                                        .help("Pod phase from the cluster")
                                }
                                .runeListRowCard(
                                    isSelected: viewModel.state.selectedPod?.id == pod.id,
                                    horizontalPadding: PodTableLayout.rowHorizontalPadding,
                                    verticalPadding: PodTableLayout.rowVerticalPadding
                                )
                            }
                            .buttonStyle(.plain)
                            .listRowInsets(EdgeInsets(
                                top: 2,
                                leading: PodTableLayout.listRowEdgeInset,
                                bottom: 2,
                                trailing: PodTableLayout.listRowEdgeInset
                            ))
                            .listRowBackground(Color.clear)
                        }
                    } header: {
                        podTableHeader
                    }
                }
                .listStyle(.plain)

            case .deployment:
                List(viewModel.visibleDeployments) { deployment in
                    Button {
                        viewModel.selectDeployment(deployment)
                    } label: {
                        HStack {
                            Text(deployment.name)
                                .font(.body.weight(.medium))
                            Spacer()
                            Text(deployment.replicaText)
                                .font(.caption.weight(.semibold))
                                .padding(.horizontal, 8)
                                .padding(.vertical, 2)
                                .background(Color.blue.opacity(0.22), in: Capsule())
                                .foregroundStyle(.blue)
                        }
                        .runeListRowCard(isSelected: viewModel.state.selectedDeployment == deployment, verticalPadding: 5)
                    }
                    .buttonStyle(.plain)
                    .listRowInsets(EdgeInsets(top: 4, leading: 6, bottom: 4, trailing: 6))
                    .listRowBackground(Color.clear)
                }
                .listStyle(.plain)

            case .statefulSet:
                genericResourceList(viewModel.visibleStatefulSets, selection: viewModel.state.selectedStatefulSet, action: viewModel.selectStatefulSet)

            case .daemonSet:
                genericResourceList(viewModel.visibleDaemonSets, selection: viewModel.state.selectedDaemonSet, action: viewModel.selectDaemonSet)

            case .job:
                genericResourceList(viewModel.visibleJobs, selection: viewModel.state.selectedJob, action: viewModel.selectJob)

            case .cronJob:
                genericResourceList(viewModel.visibleCronJobs, selection: viewModel.state.selectedCronJob, action: viewModel.selectCronJob)

            case .replicaSet:
                genericResourceList(viewModel.visibleReplicaSets, selection: viewModel.state.selectedReplicaSet, action: viewModel.selectReplicaSet)

            case .horizontalPodAutoscaler:
                genericResourceList(
                    viewModel.visibleHorizontalPodAutoscalers,
                    selection: viewModel.state.selectedHorizontalPodAutoscaler,
                    action: viewModel.selectHorizontalPodAutoscaler
                )

            case .service, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()

            case .event:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var networkingPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 8) {
                        ForEach(viewModel.visibleServices) { service in
                            Button {
                                viewModel.selectService(service)
                            } label: {
                                HStack {
                                    Text(service.name)
                                        .font(.body.weight(.medium))
                                        .help(service.name)
                                    Spacer()
                                    Text(service.type)
                                        .font(.caption.weight(.semibold))
                                        .padding(.horizontal, 8)
                                        .padding(.vertical, 2)
                                        .background(Color.purple.opacity(0.22), in: Capsule())
                                        .foregroundStyle(.purple)
                                }
                                .runeListRowCard(isSelected: viewModel.state.selectedService == service, verticalPadding: 5)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding(.horizontal, 6)
                    .padding(.vertical, 4)
                }
                .id("networking:service")

            case .ingress:
                genericResourceList(viewModel.visibleIngresses, selection: viewModel.state.selectedIngress, action: viewModel.selectIngress)

            case .networkPolicy:
                genericResourceList(
                    viewModel.visibleNetworkPolicies,
                    selection: viewModel.state.selectedNetworkPolicy,
                    action: viewModel.selectNetworkPolicy
                )

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var configPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                genericResourceList(viewModel.visibleConfigMaps, selection: viewModel.state.selectedConfigMap, action: viewModel.selectConfigMap)

            case .secret:
                genericResourceList(viewModel.visibleSecrets, selection: viewModel.state.selectedSecret, action: viewModel.selectSecret)

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var storagePane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                genericResourceList(
                    viewModel.visiblePersistentVolumeClaims,
                    selection: viewModel.state.selectedPersistentVolumeClaim,
                    action: viewModel.selectPersistentVolumeClaim
                )

            case .persistentVolume:
                genericResourceList(
                    viewModel.visiblePersistentVolumes,
                    selection: viewModel.state.selectedPersistentVolume,
                    action: viewModel.selectPersistentVolume
                )

            case .storageClass:
                genericResourceList(
                    viewModel.visibleStorageClasses,
                    selection: viewModel.state.selectedStorageClass,
                    action: viewModel.selectStorageClass
                )

            case .node:
                genericResourceList(viewModel.visibleNodes, selection: viewModel.state.selectedNode, action: viewModel.selectNode)

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var rbacPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                genericResourceList(
                    viewModel.visibleRBACResources,
                    selection: viewModel.state.selectedRBACResource,
                    action: viewModel.selectRBACResource
                )
            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var helmPane: some View {
        List(viewModel.visibleHelmReleases) { release in
            Button {
                viewModel.selectHelmRelease(release)
            } label: {
                VStack(alignment: .leading, spacing: 6) {
                    HStack {
                        Text(release.name)
                            .font(.body.weight(.medium))
                            .help(release.name)
                        Spacer()
                        Text(release.status.capitalized)
                            .font(.caption.weight(.semibold))
                            .padding(.horizontal, 8)
                            .padding(.vertical, 2)
                            .background(statusColor(for: release.status).opacity(0.22), in: Capsule())
                            .foregroundStyle(statusColor(for: release.status))
                    }

                    HStack(spacing: 8) {
                        if shouldShowResourceNamespaceLabel(release.namespace) {
                            Text(release.namespace)
                        }
                        Text("Rev \(release.revision)")
                        Text(release.chart)
                    }
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                }
                .runeListRowCard(isSelected: viewModel.state.selectedHelmRelease == release)
            }
            .buttonStyle(.plain)
            .listRowInsets(EdgeInsets(top: 4, leading: 6, bottom: 4, trailing: 6))
            .listRowBackground(Color.clear)
        }
        .listStyle(.plain)
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var eventsPane: some View {
        List(viewModel.visibleEvents) { event in
            Button {
                viewModel.selectEvent(event)
            } label: {
                HStack(alignment: .top, spacing: 8) {
                    VStack(alignment: .leading, spacing: 4) {
                        HStack {
                            Text(event.reason)
                                .font(.subheadline.weight(.bold))
                            Spacer()
                            Text(event.type)
                                .font(.caption.weight(.semibold))
                                .foregroundStyle(event.type.lowercased() == "warning" ? .orange : .green)
                        }

                        Text(event.objectName)
                            .font(.caption.weight(.medium))
                            .foregroundStyle(.secondary)

                        Text(event.message)
                            .font(.footnote)
                            .foregroundStyle(.secondary)
                            .lineLimit(2)
                    }
                }
                .runeListRowCard(isSelected: viewModel.state.selectedEvent == event)
            }
            .buttonStyle(.plain)
            .help(eventHint(for: event))
            .listRowInsets(EdgeInsets(top: 4, leading: 6, bottom: 4, trailing: 6))
            .listRowBackground(Color.clear)
        }
        .listStyle(.plain)
        .scrollContentBackground(.hidden)
    }

    private var sectionPlaceholder: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(viewModel.state.selectedSection.title + " is being implemented")
                .font(.title3.weight(.bold))
            Text("Flow and shortcuts are already wired so section switching feels instant.")
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(20)
        .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous))
    }

    private var detailPane: some View {
        VStack(alignment: .leading, spacing: 12) {
            Group {
                switch viewModel.state.selectedSection {
                case .overview:
                    overviewDetails
                case .workloads:
                    workloadDetails
                case .networking:
                    networkingDetails
                case .config:
                    configDetails
                case .storage:
                    storageDetails
                case .helm:
                    helmDetails
                case .events:
                    eventDetails
                case .rbac:
                    rbacDetails
                case .terminal:
                    terminalDetails
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(RuneUILayoutMetrics.paneInnerPadding)
            .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous))

            if let error = viewModel.state.lastError {
                Text(error)
                    .font(.footnote)
                    .foregroundStyle(.red)
                    .padding(.top, 8)
            }
        }
        // `minWidth: 0` lets split columns shrink correctly; without it, nested scroll views can force odd horizontal alignment.
        .frame(minWidth: 0, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(RuneUILayoutMetrics.paneOuterPadding)
        .background {
            ZStack(alignment: .leading) {
                RuneGlassPaneSurface(role: .inspector)
                RuneGlassPaneBorder(role: .inspector)
                RuneRootLayoutProbe(kind: .detail, generation: layoutGeneration)
                paneFocusOutline(isFocused: keyboardPaneFocus == .detail)
            }
        }
    }

    private var overviewDetails: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Overview")
                .font(.title2.weight(.bold))

            VStack(alignment: .leading, spacing: 8) {
                Label("Context: \(viewModel.state.selectedContext?.name ?? "-")", systemImage: "network")
                Label("Namespace: \(viewModel.state.selectedNamespace)", systemImage: "square.stack.3d.up")
                Label("Mode: \(viewModel.state.isReadOnlyMode ? "Read-only" : "Read/Write")", systemImage: "lock.shield")
            }
            .font(.subheadline.weight(.medium))

            HStack(spacing: 10) {
                Button("Open Workloads") {
                    viewModel.setSection(.workloads)
                }

                Button("Open Events") {
                    viewModel.setSection(.events)
                }

                Button("Open Helm") {
                    viewModel.setSection(.helm)
                }

                Button("Reload") {
                    viewModel.refreshCurrentView(debounced: false)
                }

                Button("Save Bundle") {
                    viewModel.saveSupportBundle()
                }
            }

            Spacer(minLength: 0)
        }
    }

    private var workloadDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                podDetails
            case .deployment:
                deploymentDetails
            case .statefulSet:
                genericResourceDetails(resource: viewModel.state.selectedStatefulSet)
            case .daemonSet:
                genericResourceDetails(resource: viewModel.state.selectedDaemonSet)
            case .job:
                genericResourceDetails(resource: viewModel.state.selectedJob)
            case .cronJob:
                cronJobInspectorContent
            case .replicaSet:
                genericResourceDetails(resource: viewModel.state.selectedReplicaSet)
            case .horizontalPodAutoscaler:
                genericResourceDetails(resource: viewModel.state.selectedHorizontalPodAutoscaler)
            case .service, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()
            case .event:
                EmptyView()
            }
        }
    }

    private var cronJobInspectorContent: some View {
        VStack(alignment: .leading, spacing: 12) {
            if viewModel.state.selectedCronJob != nil {
                HStack(spacing: 10) {
                    if viewModel.state.selectedCronJob?.secondaryText == "Suspended" {
                        Button("Resume") {
                            viewModel.setSelectedCronJobSuspended(false)
                        }
                        .disabled(!viewModel.canApplyClusterMutations)
                    } else {
                        Button("Suspend") {
                            viewModel.setSelectedCronJobSuspended(true)
                        }
                        .disabled(!viewModel.canApplyClusterMutations)
                    }
                    Button("Create job now") {
                        viewModel.createManualJobFromSelectedCronJob()
                    }
                    .disabled(!viewModel.canApplyClusterMutations)
                }
            }
            genericResourceDetails(resource: viewModel.state.selectedCronJob)
        }
    }

    private var networkingDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                serviceDetails
            case .ingress:
                genericResourceDetails(resource: viewModel.state.selectedIngress)
            case .networkPolicy:
                genericResourceDetails(resource: viewModel.state.selectedNetworkPolicy)
            case .pod, .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()
            }
        }
    }

    private var configDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                genericResourceDetails(resource: viewModel.state.selectedConfigMap)
            case .secret:
                genericResourceDetails(resource: viewModel.state.selectedSecret)
            case .pod, .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .service, .ingress, .networkPolicy, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .event, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()
            }
        }
    }

    private var storageDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                genericResourceDetails(resource: viewModel.state.selectedPersistentVolumeClaim)
            case .persistentVolume:
                genericResourceDetails(resource: viewModel.state.selectedPersistentVolume)
            case .storageClass:
                genericResourceDetails(resource: viewModel.state.selectedStorageClass)
            case .node:
                genericResourceDetails(resource: viewModel.state.selectedNode)
            default:
                EmptyView()
            }
        }
    }

    private var rbacDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedRBACResource)
    }

    private var helmDetails: some View {
        Group {
            if let release = viewModel.state.selectedHelmRelease {
                VStack(alignment: .leading, spacing: 12) {
                    Text(release.name)
                        .font(.title2.weight(.bold))
                        .help(release.name)

                    RuneSegmentedPickerInScroll(
                        "",
                        selection: $helmInspectorTab,
                        labelsHidden: true
                    ) {
                        ForEach(HelmInspectorTab.allCases) { tab in
                            Text(tab.title).tag(tab)
                        }
                    }
                    .accessibilityLabel("Inspector")

                    switch helmInspectorTab {
                    case .overview:
                        VStack(alignment: .leading, spacing: 10) {
                            if shouldShowResourceNamespaceLabel(release.namespace) {
                                Label("Namespace: \(release.namespace)", systemImage: "square.stack.3d.up")
                            }
                            Label("Status: \(release.status.capitalized)", systemImage: "checkmark.seal")
                            Label("Chart: \(release.chart)", systemImage: "shippingbox")
                            Label("App Version: \(release.appVersion)", systemImage: "tag")
                            Label("Revision: \(release.revision)", systemImage: "clock.arrow.trianglehead.counterclockwise.rotate.90")
                            Text(release.updated)
                                .font(.footnote)
                                .foregroundStyle(.secondary)

                            HStack(spacing: 10) {
                                TextField("Rollback revision", text: $viewModel.helmRollbackRevisionInput)
                                    .textFieldStyle(.roundedBorder)
                                    .frame(maxWidth: 140)

                                Button("Rollback") {
                                    viewModel.requestRollbackSelectedHelmRelease()
                                }
                                .disabled(!viewModel.canApplyClusterMutations)
                            }
                        }

                    case .values:
                        exportableTextPane(
                            text: viewModel.state.helmValues,
                            emptyText: "No values loaded",
                            saveAction: viewModel.saveCurrentHelmValues
                        )

                    case .manifest:
                        exportableTextPane(
                            text: viewModel.state.helmManifest,
                            emptyText: "No manifest loaded",
                            saveAction: viewModel.saveCurrentHelmManifest
                        )

                    case .history:
                        VStack(alignment: .leading, spacing: 10) {
                            HStack {
                                Button("Save History") {
                                    viewModel.saveCurrentHelmHistory()
                                }
                                Spacer()
                            }

                            if viewModel.state.helmHistory.isEmpty {
                                Text("No history loaded")
                                    .foregroundStyle(.secondary)
                            } else {
                                ScrollView {
                                    VStack(alignment: .leading, spacing: 10) {
                                        ForEach(viewModel.state.helmHistory) { entry in
                                            VStack(alignment: .leading, spacing: 4) {
                                                HStack {
                                                    Text("Revision \(entry.revision)")
                                                        .font(.subheadline.weight(.semibold))
                                                    Spacer()
                                                    Text(entry.status.capitalized)
                                                        .font(.caption.weight(.semibold))
                                                        .foregroundStyle(statusColor(for: entry.status))
                                                }
                                                Text(entry.chart + " • " + entry.appVersion)
                                                    .font(.caption)
                                                    .foregroundStyle(.secondary)
                                                Text(entry.description)
                                                    .font(.footnote)
                                                Text(entry.updated)
                                                    .font(.caption)
                                                    .foregroundStyle(.secondary)
                                            }
                                            .padding(10)
                                            .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            } else {
                inspectorEmptyState("Select a Helm release", symbol: "ferry")
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var podDetails: some View {
        Group {
            if let pod = viewModel.state.selectedPod {
                VStack(alignment: .leading, spacing: 12) {
                    Text(pod.name)
                        .font(.title2.weight(.bold))

                    RuneSegmentedPickerInScroll(
                        "",
                        selection: $podInspectorTab,
                        labelsHidden: true
                    ) {
                        ForEach(PodInspectorTab.allCases) { tab in
                            Text(tab.title).tag(tab)
                        }
                    }
                    .accessibilityLabel("Inspector")

                    Group {
                        switch podInspectorTab {
                        case .overview:
                            podOverviewSection(pod: pod)

                        case .logs:
                            PodLogsInspectorPane(
                                selectedLogPreset: $viewModel.selectedLogPreset,
                                includePreviousLogs: $viewModel.includePreviousLogs,
                                isLoadingLogs: viewModel.state.isLoadingLogs,
                                isLoadingResources: viewModel.state.isLoading,
                                errorMessage: viewModel.state.lastLogFetchError,
                                logText: viewModel.state.podLogs,
                                readOnlyResetID: "podlogs:\(viewModel.state.selectedPod?.name ?? ""):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs)",
                                onReload: { viewModel.reloadLogsForSelection() },
                                onSave: { viewModel.saveCurrentLogs() }
                            )

                        case .exec:
                            execPane(for: pod)

                        case .portForward:
                            portForwardPane(targetKind: .pod, targetName: pod.name)

                        case .describe:
                            manifestInspectorPane(activeTab: .describe)

                        case .yaml:
                            manifestInspectorPane(activeTab: .yaml)
                        }
                    }
                    .transaction { transaction in
                        transaction.animation = nil
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            } else {
                inspectorEmptyState("Select a pod", symbol: "cube.box")
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var deploymentDetails: some View {
        Group {
            if let deployment = viewModel.state.selectedDeployment {
                VStack(alignment: .leading, spacing: 12) {
                    Text(deployment.name)
                        .font(.title2.weight(.bold))

                    RuneSegmentedPickerInScroll(
                        "",
                        selection: $deploymentInspectorTab,
                        labelsHidden: true
                    ) {
                        ForEach(DeploymentInspectorTab.allCases) { tab in
                            Text(tab.title).tag(tab)
                        }
                    }
                    .accessibilityLabel("Inspector")

                    Group {
                        switch deploymentInspectorTab {
                        case .overview:
                            deploymentOverviewSection(deployment: deployment)

                        case .unifiedLogs:
                            UnifiedResourceLogsInspectorPane(
                                selectedLogPreset: $viewModel.selectedLogPreset,
                                includePreviousLogs: $viewModel.includePreviousLogs,
                                isLoadingLogs: viewModel.state.isLoadingLogs,
                                isLoadingResources: viewModel.state.isLoading,
                                errorMessage: viewModel.state.lastLogFetchError,
                                podNames: viewModel.state.unifiedServiceLogPods,
                                logText: viewModel.state.unifiedServiceLogs,
                                readOnlyResetID: "unifiedlogs:\(viewModel.state.selectedDeployment?.name ?? ""):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs)",
                                onReload: { viewModel.reloadLogsForSelection() },
                                onSave: { viewModel.saveCurrentLogs() }
                            )

                        case .rollout:
                            VStack(alignment: .leading, spacing: 10) {
                                HStack(spacing: 10) {
                                    TextField("Revision (optional)", text: $viewModel.rolloutRevisionInput)
                                        .textFieldStyle(.roundedBorder)
                                        .frame(maxWidth: 160)

                                    Button("Rollback") {
                                        viewModel.requestRolloutUndoSelectedDeployment()
                                    }
                                    .disabled(!viewModel.canApplyClusterMutations)

                                    Button("Save History") {
                                        viewModel.saveCurrentRolloutHistory()
                                    }

                                    Spacer()
                                }

                                ScrollView {
                                    Text(viewModel.state.deploymentRolloutHistory.isEmpty ? "No rollout history loaded" : viewModel.state.deploymentRolloutHistory)
                                        .font(.system(size: 12, weight: .regular, design: .monospaced))
                                        .frame(maxWidth: .infinity, alignment: .leading)
                                        .textSelection(.enabled)
                                        .padding(10)
                                        .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                                }
                            }

                        case .describe:
                            manifestInspectorPane(activeTab: .describe)

                        case .yaml:
                            manifestInspectorPane(activeTab: .yaml)
                        }
                    }
                    .transaction { transaction in
                        transaction.animation = nil
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            } else {
                inspectorEmptyState("Select a deployment", symbol: "shippingbox")
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var serviceDetails: some View {
        Group {
            if let service = viewModel.state.selectedService {
                VStack(alignment: .leading, spacing: 12) {
                    Text(service.name)
                        .font(.title2.weight(.bold))

                    RuneSegmentedPickerInScroll(
                        "",
                        selection: $serviceInspectorTab,
                        labelsHidden: true
                    ) {
                        ForEach(ServiceInspectorTab.allCases) { tab in
                            Text(tab.title).tag(tab)
                        }
                    }
                    .accessibilityLabel("Inspector")

                    Group {
                        switch serviceInspectorTab {
                        case .overview:
                            VStack(alignment: .leading, spacing: 12) {
                                if shouldShowResourceNamespaceLabel(service.namespace) {
                                    inspectorInsetCard {
                                        Label("Namespace: \(service.namespace)", systemImage: "square.stack.3d.up")
                                            .font(.subheadline.weight(.medium))
                                            .foregroundStyle(.secondary)
                                    }
                                }
                                inspectorInsetCard {
                                    VStack(alignment: .leading, spacing: 10) {
                                        Label("Type: \(service.type)", systemImage: "point.3.connected.trianglepath.dotted")
                                            .font(.body.weight(.medium))
                                        Label("Cluster IP: \(service.clusterIP)", systemImage: "network")
                                            .font(.subheadline)
                                            .foregroundStyle(.secondary)
                                        Divider().opacity(0.45)
                                        inspectorActionButtonRow {
                                            Button("Apply YAML") { viewModel.requestApplySelectedResourceYAML() }
                                                .buttonStyle(.bordered)
                                                .disabled(!viewModel.canApplyClusterMutations)
                                            Button("Export…") { viewModel.saveCurrentResourceYAML() }
                                                .buttonStyle(.bordered)
                                            Spacer(minLength: 0)
                                        }
                                        Button("Delete", role: .destructive) {
                                            viewModel.requestDeleteSelectedResource()
                                        }
                                        .disabled(!viewModel.canApplyClusterMutations)
                                    }
                                }
                            }

                        case .unifiedLogs:
                            UnifiedResourceLogsInspectorPane(
                                selectedLogPreset: $viewModel.selectedLogPreset,
                                includePreviousLogs: $viewModel.includePreviousLogs,
                                isLoadingLogs: viewModel.state.isLoadingLogs,
                                isLoadingResources: viewModel.state.isLoading,
                                errorMessage: viewModel.state.lastLogFetchError,
                                podNames: viewModel.state.unifiedServiceLogPods,
                                logText: viewModel.state.unifiedServiceLogs,
                                readOnlyResetID: "unifiedlogs:\(viewModel.state.selectedService?.name ?? ""):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs)",
                                onReload: { viewModel.reloadLogsForSelection() },
                                onSave: { viewModel.saveCurrentLogs() }
                            )

                        case .portForward:
                            portForwardPane(targetKind: .service, targetName: service.name)

                        case .describe:
                            manifestInspectorPane(activeTab: .describe)

                        case .yaml:
                            manifestInspectorPane(activeTab: .yaml)
                        }
                    }
                    .transaction { transaction in
                        transaction.animation = nil
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            } else {
                inspectorEmptyState("Select a service", symbol: "point.3.connected.trianglepath.dotted")
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var eventDetails: some View {
        Group {
            if let event = viewModel.state.selectedEvent {
                VStack(alignment: .leading, spacing: 12) {
                    Text(event.reason)
                        .font(.title2.weight(.bold))

                    if let ts = event.lastTimestamp?.trimmingCharacters(in: .whitespacesAndNewlines), !ts.isEmpty {
                        Text("Time: \(ts)")
                            .font(.subheadline.monospaced())
                            .foregroundStyle(.secondary)
                    }

                    Text("Type: \(event.type)")

                    Group {
                        if let k = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines), !k.isEmpty {
                            Text("Object: \(k) / \(event.objectName)")
                                .font(.body.weight(.medium))
                        } else {
                            Text("Object: \(event.objectName)")
                                .font(.body.weight(.medium))
                        }
                    }

                    if let ns = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines), !ns.isEmpty {
                        Label("Namespace: \(ns)", systemImage: "square.stack.3d.up")
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }

                    if !event.objectName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                        Button(eventGoToResourceButtonTitle(for: event)) {
                            viewModel.openEventSource(event)
                        }
                        .buttonStyle(.borderedProminent)
                        .help("Switches section and selects the involved object when it appears in the list.")
                    }

                    ScrollView {
                        Text(event.message)
                            .font(.system(size: 12, weight: .regular, design: .monospaced))
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(10)
                            .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            } else {
                inspectorEmptyState("Select an event", symbol: "bolt.badge.clock")
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private func eventGoToResourceButtonTitle(for event: EventSummary) -> String {
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        switch kind {
        case "pod": return "Go to Pod"
        case "deployment": return "Go to Deployment"
        case "statefulset": return "Go to StatefulSet"
        case "daemonset": return "Go to DaemonSet"
        case "service": return "Go to Service"
        case "ingress": return "Go to Ingress"
        case "configmap": return "Go to ConfigMap"
        case "secret": return "Go to Secret"
        case "node": return "Go to Node"
        case "": return "Go to resource"
        default: return "Go to resource"
        }
    }

    private func genericResourceDetails(resource: ClusterResourceSummary?) -> some View {
        Group {
            if let resource {
                VStack(alignment: .leading, spacing: 12) {
                    Text(resource.name)
                        .font(.title2.weight(.bold))
                        .help(resource.name)

                    VStack(alignment: .leading, spacing: 8) {
                        if let namespace = resource.namespace, shouldShowResourceNamespaceLabel(namespace) {
                            Label("Namespace: \(namespace)", systemImage: "square.stack.3d.up")
                        }
                        Label(resource.primaryText, systemImage: "info.circle")
                        Label(resource.secondaryText, systemImage: "text.alignleft")
                    }
                    .font(.subheadline)

                    RuneSegmentedPickerInScroll("Manifest", selection: $genericResourceManifestTab) {
                        ForEach(GenericResourceManifestTab.allCases) { tab in
                            Text(tab.title).tag(tab)
                        }
                    }

                    manifestInspectorPane(activeTab: genericResourceManifestTab)
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            } else {
                genericResourceEmptyState
            }
        }
    }

    private var yamlDisplayText: String {
        if !viewModel.state.resourceYAML.isEmpty {
            return viewModel.state.resourceYAML
        }
        if viewModel.state.isLoadingResourceDetails {
            return "Loading YAML for \(manifestResourceReference)…"
        }
        if let error = viewModel.state.lastResourceYAMLError?.trimmingCharacters(in: .whitespacesAndNewlines), !error.isEmpty {
            return error
        }
        return "No YAML available for \(manifestResourceReference).\n\nThe resource may still be loading, may not exist in the active namespace, or the cluster returned an empty manifest."
    }

    private var yamlFooterText: String {
        if viewModel.state.isLoadingResourceDetails {
            return "Loading resource YAML from the cluster."
        }
        if viewModel.state.lastResourceYAMLError != nil {
            return "YAML could not be loaded for the current selection. Check context, namespace, and cluster access in Settings."
        }
        return "No YAML was returned for the current selection yet. You can also use Import… to paste YAML from a file."
    }

    private var describeDisplayText: String {
        if !viewModel.state.resourceDescribe.isEmpty {
            return viewModel.state.resourceDescribe
        }
        if viewModel.state.isLoadingResourceDetails {
            return "Loading describe output for \(manifestResourceReference)…"
        }
        if let error = viewModel.state.lastResourceDescribeError?.trimmingCharacters(in: .whitespacesAndNewlines), !error.isEmpty {
            return error
        }
        return "No describe output available for \(manifestResourceReference).\n\nThe resource may still be loading, may not exist in the active namespace, or describe returned no output."
    }

    private var yamlDraftBinding: Binding<String> {
        Binding(
            get: { viewModel.state.resourceYAML },
            set: { viewModel.state.updateResourceYAMLDraft($0) }
        )
    }

    private var manifestResourceReference: String {
        switch viewModel.state.selectedWorkloadKind {
        case .pod:
            return viewModel.state.selectedPod.map { "pod \($0.name)" } ?? "the selected pod"
        case .deployment:
            return viewModel.state.selectedDeployment.map { "deployment \($0.name)" } ?? "the selected deployment"
        case .service:
            return viewModel.state.selectedService.map { "service \($0.name)" } ?? "the selected service"
        case .statefulSet:
            return viewModel.state.selectedStatefulSet.map { "statefulset \($0.name)" } ?? "the selected statefulset"
        case .daemonSet:
            return viewModel.state.selectedDaemonSet.map { "daemonset \($0.name)" } ?? "the selected daemonset"
        case .job:
            return viewModel.state.selectedJob.map { "job \($0.name)" } ?? "the selected job"
        case .cronJob:
            return viewModel.state.selectedCronJob.map { "cronjob \($0.name)" } ?? "the selected cronjob"
        case .replicaSet:
            return viewModel.state.selectedReplicaSet.map { "replicaset \($0.name)" } ?? "the selected replicaset"
        case .ingress:
            return viewModel.state.selectedIngress.map { "ingress \($0.name)" } ?? "the selected ingress"
        case .configMap:
            return viewModel.state.selectedConfigMap.map { "configmap \($0.name)" } ?? "the selected configmap"
        case .secret:
            return viewModel.state.selectedSecret.map { "secret \($0.name)" } ?? "the selected secret"
        case .node:
            return viewModel.state.selectedNode.map { "node \($0.name)" } ?? "the selected node"
        case .persistentVolumeClaim:
            return viewModel.state.selectedPersistentVolumeClaim.map { "pvc \($0.name)" } ?? "the selected PVC"
        case .persistentVolume:
            return viewModel.state.selectedPersistentVolume.map { "pv \($0.name)" } ?? "the selected PV"
        case .storageClass:
            return viewModel.state.selectedStorageClass.map { "storageclass \($0.name)" } ?? "the selected StorageClass"
        case .horizontalPodAutoscaler:
            return viewModel.state.selectedHorizontalPodAutoscaler.map { "hpa \($0.name)" } ?? "the selected HPA"
        case .networkPolicy:
            return viewModel.state.selectedNetworkPolicy.map { "networkpolicy \($0.name)" } ?? "the selected NetworkPolicy"
        case .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            return viewModel.state.selectedRBACResource.map { "\($0.kind.kubectlName) \($0.name)" } ?? "the selected RBAC resource"
        case .event:
            return "the selected event"
        }
    }

    private var genericResourceEmptyState: some View {
        if viewModel.state.isLoading || viewModel.state.isLoadingResourceDetails {
            return AnyView(
                inspectorEmptyState(
                    "Loading \(viewModel.state.selectedWorkloadKind.title)",
                    symbol: "hourglass",
                    detail: "Loading resources and manifest details for the active namespace."
                )
            )
        }

        let availableCount: Int = {
            switch viewModel.state.selectedWorkloadKind {
            case .statefulSet: return viewModel.visibleStatefulSets.count
            case .daemonSet: return viewModel.visibleDaemonSets.count
            case .job: return viewModel.visibleJobs.count
            case .cronJob: return viewModel.visibleCronJobs.count
            case .replicaSet: return viewModel.visibleReplicaSets.count
            case .horizontalPodAutoscaler: return viewModel.visibleHorizontalPodAutoscalers.count
            case .ingress: return viewModel.visibleIngresses.count
            case .networkPolicy: return viewModel.visibleNetworkPolicies.count
            case .persistentVolumeClaim: return viewModel.visiblePersistentVolumeClaims.count
            case .persistentVolume: return viewModel.visiblePersistentVolumes.count
            case .storageClass: return viewModel.visibleStorageClasses.count
            case .configMap: return viewModel.visibleConfigMaps.count
            case .secret: return viewModel.visibleSecrets.count
            case .node: return viewModel.visibleNodes.count
            case .role, .roleBinding, .clusterRole, .clusterRoleBinding: return viewModel.visibleRBACResources.count
            default: return 0
            }
        }()

        if availableCount == 0 {
            return AnyView(
                inspectorEmptyState(
                    "No \(viewModel.state.selectedWorkloadKind.title.lowercased()) found",
                    symbol: "tray",
                    detail: "Nothing is available in the current namespace yet. YAML and describe will appear here when a resource is selected."
                )
            )
        }

        return AnyView(inspectorEmptyState("Select a resource", symbol: "list.bullet.rectangle"))
    }

    private func openYAMLEditorSheet() {
        yamlManifestIsEditing = false
        isYAMLEditorSheetPresented = true
    }

    private func yamlManifestEditorSheet() -> some View {
        ResourceYAMLEditorSheetView(
            resourceReference: manifestResourceReference,
            yamlText: yamlDraftBinding,
            yamlFooterText: yamlFooterText,
            canApplyMutations: viewModel.canApplyClusterMutations,
            hasUnsavedEdits: viewModel.state.resourceYAMLHasUnsavedEdits,
            validationIssues: viewModel.state.resourceYAMLValidationIssues,
            isValidating: viewModel.state.isValidatingResourceYAML,
            onApply: { viewModel.requestApplySelectedResourceYAML() },
            onRevert: { viewModel.revertResourceYAMLDraft() },
            onImport: { viewModel.importResourceYAMLFromFile() },
            onExport: { viewModel.saveCurrentResourceYAML() },
            onClose: { isYAMLEditorSheetPresented = false }
        )
    }

    private var yamlBlock: some View {
        ResourceYAMLInspectorPane(
            resourceReference: manifestResourceReference,
            yamlText: yamlDraftBinding,
            yamlDisplayText: yamlDisplayText,
            yamlFooterText: yamlFooterText,
            baseline: viewModel.state.resourceYAMLBaseline,
            hasUnsavedEdits: viewModel.state.resourceYAMLHasUnsavedEdits,
            canApplyMutations: viewModel.canApplyClusterMutations,
            validationIssues: viewModel.state.resourceYAMLValidationIssues,
            isValidating: viewModel.state.isValidatingResourceYAML,
            isInlineEditing: $yamlManifestIsEditing,
            inlineEditorImplementation: resolvedManifestInlineEditorImplementation,
            onApply: { viewModel.requestApplySelectedResourceYAML() },
            onOpenEditor: { openYAMLEditorSheet() },
            onRevert: { viewModel.revertResourceYAMLDraft() },
            onImport: { viewModel.importResourceYAMLFromFile() },
            onExport: { viewModel.saveCurrentResourceYAML() },
            readOnlyResetID: "yaml:\(manifestResourceReference):\(viewModel.state.selectedSection.rawValue):\(viewModel.state.selectedWorkloadKind.kubectlName)"
        )
    }

    /// One pane at a time — avoids `ZStack` + opacity (both branches still participated in layout, causing width drift and editor jumping when switching YAML/Describe).
    @ViewBuilder
    private func manifestInspectorPane(activeTab: GenericResourceManifestTab) -> some View {
        switch activeTab {
        case .yaml:
            yamlBlock
        case .describe:
            describeBlock
        }
    }

    /// Describe tab: read-only describe output; cluster updates use the YAML manifest (same buffer as the YAML tab) and Apply.
    private var describeBlock: some View {
        ResourceDescribeInspectorPane(
            describeText: describeDisplayText,
            resourceReference: manifestResourceReference,
            canApplyMutations: viewModel.canApplyClusterMutations,
            yamlText: viewModel.state.resourceYAML,
            onApply: { viewModel.requestApplySelectedResourceYAML() },
            onOpenYAMLEditor: { openYAMLEditorSheet() },
            readOnlyResetID: "describe:\(manifestResourceReference):\(viewModel.state.selectedSection.rawValue):\(viewModel.state.selectedWorkloadKind.kubectlName)"
        )
    }

    private func exportableTextPane(text: String, emptyText: String, saveAction: @escaping () -> Void) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Button("Save…") {
                    saveAction()
                }
                Spacer()
            }

            ScrollView {
                InspectorReadOnlyTextView(
                    text: text.isEmpty ? emptyText : text,
                    resetID: "export:\(emptyText):\((text.isEmpty ? emptyText : text).count)"
                )
                .padding(10)
                .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
            }
        }
    }

    private func execPane(for pod: PodSummary) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            TextField("Command", text: $viewModel.execCommandInput)
                .textFieldStyle(.roundedBorder)

            HStack(spacing: 10) {
                Button("Run in Pod") {
                    viewModel.requestExecInSelectedPod()
                }
                .disabled(viewModel.state.isExecutingCommand || !viewModel.canApplyClusterMutations)

                if viewModel.state.isExecutingCommand {
                    ProgressView()
                        .controlSize(.small)
                }

                Spacer()

                if let result = viewModel.state.lastExecResult, result.podName == pod.name {
                    Text("Exit code: \(result.exitCode)")
                        .font(.footnote.weight(.semibold))
                        .foregroundStyle(result.exitCode == 0 ? Color.secondary : Color.red)
                }
            }

            if let result = viewModel.state.lastExecResult, result.podName == pod.name {
                VStack(alignment: .leading, spacing: 8) {
                    Text(result.command.joined(separator: " "))
                        .font(.footnote.weight(.semibold))
                        .foregroundStyle(.secondary)

                    ScrollView {
                        InspectorReadOnlyTextView(
                            text: execOutputText(for: result),
                            resetID: "exec:\(result.podName):\(result.command.joined(separator: " ")):\(result.exitCode)"
                        )
                        .padding(10)
                        .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                    }
                }
            } else {
                Text("Run a command to see stdout/stderr here.")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private func portForwardPane(targetKind: PortForwardTargetKind, targetName: String) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 10) {
                TextField("Local Port", text: $viewModel.portForwardLocalPortInput)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 120)

                Text("->")
                    .foregroundStyle(.secondary)

                TextField("Remote Port", text: $viewModel.portForwardRemotePortInput)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 120)

                TextField("Address", text: $viewModel.portForwardAddressInput)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 180)
            }

            HStack(spacing: 10) {
                Button("Start Port Forward") {
                    viewModel.startPortForwardForSelection()
                }
                .disabled(viewModel.state.isStartingPortForward)

                if viewModel.state.isStartingPortForward {
                    ProgressView()
                        .controlSize(.small)
                }

                Spacer()

                Button("Open Terminal") {
                    viewModel.setSection(.terminal)
                }
            }

            let matchingSessions = viewModel.state.portForwardSessions.filter {
                $0.targetKind == targetKind && $0.targetName == targetName
            }

            if matchingSessions.isEmpty {
                Text("No active or recent port-forward sessions for this \(targetKind.title.lowercased()).")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(matchingSessions) { session in
                    portForwardSessionRow(session)
                }
            }
        }
    }

    private var terminalPane: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Active Port Forwards")
                        .font(.headline)

                    if viewModel.state.portForwardSessions.isEmpty {
                        Text("No port-forward sessions started yet.")
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(viewModel.state.portForwardSessions) { session in
                            portForwardSessionRow(session)
                        }
                    }
                }
                .padding(12)
                .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))

                VStack(alignment: .leading, spacing: 8) {
                    Text("Last Exec Result")
                        .font(.headline)

                    if let result = viewModel.state.lastExecResult {
                        Text("\(result.podName) • \(result.command.joined(separator: " "))")
                            .font(.footnote.weight(.semibold))
                            .foregroundStyle(.secondary)

                        ScrollView {
                            InspectorReadOnlyTextView(
                                text: execOutputText(for: result),
                                resetID: "terminal-exec:\(result.podName):\(result.command.joined(separator: " ")):\(result.exitCode)"
                            )
                            .padding(10)
                            .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                        }
                        .frame(minHeight: 180)
                    } else {
                        Text("No exec command has been run yet.")
                            .foregroundStyle(.secondary)
                    }
                }
                .padding(12)
                .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
            }
        }
        .id("terminal")
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var terminalDetails: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Terminal")
                .font(.title2.weight(.bold))

            Text("Use pod exec for one-shot commands and port-forward sessions for local tunneling.")
                .foregroundStyle(.secondary)

            if let result = viewModel.state.lastExecResult {
                Label("Last exec: \(result.podName)", systemImage: "terminal")
                    .font(.subheadline.weight(.medium))
            }

            if let active = viewModel.state.portForwardSessions.first(where: { $0.status == .active || $0.status == .starting }) {
                Label("\(active.resourceLabel) \(active.localPort):\(active.remotePort)", systemImage: "point.3.connected.trianglepath.dotted")
                    .font(.subheadline.weight(.medium))
            }

            Spacer()
        }
    }

    private func portForwardSessionRow(_ session: PortForwardSession) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text("\(session.resourceLabel)  \(session.localPort):\(session.remotePort)")
                    .font(.subheadline.weight(.semibold))
                Spacer()
                Text(session.status.rawValue.capitalized)
                    .font(.caption.weight(.semibold))
                    .padding(.horizontal, 8)
                    .padding(.vertical, 3)
                    .background(portForwardStatusColor(session.status).opacity(0.16), in: Capsule())
                    .foregroundStyle(portForwardStatusColor(session.status))
            }

            Text("\(session.contextName) • \(session.namespace) • \(session.address)")
                .font(.caption)
                .foregroundStyle(.secondary)

            if !session.lastMessage.isEmpty {
                Text(session.lastMessage)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
            }

            if session.status == .starting || session.status == .active {
                Button("Stop") {
                    viewModel.stopPortForward(session)
                }
            }
        }
        .padding(10)
        .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
    }

    private func genericResourceList(
        _ resources: [ClusterResourceSummary],
        selection: ClusterResourceSummary?,
        action: @escaping (ClusterResourceSummary?) -> Void
    ) -> some View {
        ScrollView {
            LazyVStack(alignment: .leading, spacing: 8) {
                ForEach(resources) { resource in
                    Button {
                        action(resource)
                    } label: {
                        HStack(alignment: .top, spacing: 12) {
                            VStack(alignment: .leading, spacing: 4) {
                                Text(resource.name)
                                    .font(.body.weight(.medium))
                                    .lineLimit(1)
                                    .help(resource.name)
                                Text(resource.primaryText)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(.secondary)
                                Text(resource.secondaryText)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                            Spacer()
                            if let namespace = resource.namespace, shouldShowResourceNamespaceLabel(namespace) {
                                RuneChip {
                                    Text(namespace)
                                        .font(.caption2.weight(.semibold))
                                        .foregroundStyle(.secondary)
                                }
                            }
                        }
                        .runeListRowCard(isSelected: selection == resource)
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(.vertical, 2)
        }
        .padding(.horizontal, 2)
        .id("\(viewModel.state.selectedSection.rawValue):\(viewModel.state.selectedWorkloadKind.kubectlName):\(genericResourceListIdentity(resources))")
        .transaction { transaction in
            transaction.animation = nil
        }
    }

    @ViewBuilder
    private var keyboardNavigationBridge: some View {
        VStack(spacing: 0) {
            Button("") {
                focusNextKeyboardPane()
            }
            .keyboardShortcut(.tab, modifiers: [])

            Button("") {
                focusPreviousKeyboardPane()
            }
            .keyboardShortcut(.tab, modifiers: [.shift])

            Button("") {
                moveKeyboardSelection(.up)
            }
            .keyboardShortcut(.upArrow, modifiers: [])

            Button("") {
                moveKeyboardSelection(.down)
            }
            .keyboardShortcut(.downArrow, modifiers: [])

            Button("") {
                moveKeyboardSelection(.left)
            }
            .keyboardShortcut(.leftArrow, modifiers: [])

            Button("") {
                moveKeyboardSelection(.right)
            }
            .keyboardShortcut(.rightArrow, modifiers: [])

            Button("") {
                activateKeyboardSelection()
            }
            .keyboardShortcut(.return, modifiers: [])

            // k9s-style alternates for keyboard-only navigation.
            Button("") {
                focusPreviousKeyboardPane()
            }
            .keyboardShortcut("h", modifiers: [.control])

            Button("") {
                moveKeyboardSelection(.down)
            }
            .keyboardShortcut("j", modifiers: [.control])

            Button("") {
                moveKeyboardSelection(.up)
            }
            .keyboardShortcut("k", modifiers: [.control])

            Button("") {
                focusNextKeyboardPane()
            }
            .keyboardShortcut("l", modifiers: [.control])
        }
        .frame(width: 0, height: 0)
        .opacity(0)
        .accessibilityHidden(true)
    }

    @ViewBuilder
    private func paneFocusOutline(isFocused: Bool) -> some View {
        if isFocused {
            Rectangle()
                .stroke(Color.accentColor.opacity(0.45), lineWidth: 1)
                .padding(2)
                .allowsHitTesting(false)
        }
    }

    private var keyboardNavigationSuspended: Bool {
        if viewModel.state.isCommandPalettePresented || isYAMLEditorSheetPresented || yamlManifestIsEditing {
            return true
        }
        if let responder = NSApp.keyWindow?.firstResponder, responder is NSTextView {
            return true
        }
        return false
    }

    private func focusNextKeyboardPane() {
        guard !viewModel.state.isCommandPalettePresented, !isYAMLEditorSheetPresented, !yamlManifestIsEditing else { return }
        if textInputFocus != nil {
            textInputFocus = nil
            keyboardPaneFocus = keyboardPaneFocus.advanced(forward: true)
            return
        }
        guard !keyboardNavigationSuspended else { return }
        keyboardPaneFocus = keyboardPaneFocus.advanced(forward: true)
    }

    private func focusPreviousKeyboardPane() {
        guard !viewModel.state.isCommandPalettePresented, !isYAMLEditorSheetPresented, !yamlManifestIsEditing else { return }
        if textInputFocus != nil {
            textInputFocus = nil
            keyboardPaneFocus = keyboardPaneFocus.advanced(forward: false)
            return
        }
        guard !keyboardNavigationSuspended else { return }
        keyboardPaneFocus = keyboardPaneFocus.advanced(forward: false)
    }

    private func moveKeyboardSelection(_ direction: MoveCommandDirection) {
        guard !keyboardNavigationSuspended else { return }
        switch keyboardPaneFocus {
        case .sidebarSections:
            moveSectionSelection(direction)
        case .sidebarContexts:
            moveContextSelection(direction)
        case .content:
            moveContentSelection(direction)
        case .detail:
            moveDetailSelection(direction)
        }
    }

    private func activateKeyboardSelection() {
        guard !keyboardNavigationSuspended else { return }
        switch keyboardPaneFocus {
        case .sidebarSections, .sidebarContexts:
            keyboardPaneFocus = .content
        case .content:
            if viewModel.state.selectedSection == .overview {
                openSelectedOverviewCard()
            } else {
                keyboardPaneFocus = .detail
            }
        case .detail:
            break
        }
    }

    private func moveSectionSelection(_ direction: MoveCommandDirection) {
        let sections = RuneSection.allCases
        guard let currentIndex = sections.firstIndex(of: viewModel.state.selectedSection),
              let nextIndex = steppedIndex(count: sections.count, current: currentIndex, direction: direction) else {
            return
        }
        viewModel.setSection(sections[nextIndex])
    }

    private func moveContextSelection(_ direction: MoveCommandDirection) {
        let contexts = viewModel.visibleContexts
        guard !contexts.isEmpty else { return }
        let currentID = viewModel.state.selectedContext?.id
        guard let next = steppedItem(items: contexts, currentID: currentID, direction: direction) else { return }
        viewModel.setContext(next)
    }

    private func moveContentSelection(_ direction: MoveCommandDirection) {
        if moveContentKindIfNeeded(direction) {
            return
        }

        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                if let next = steppedItem(items: viewModel.visiblePods, currentID: viewModel.state.selectedPod?.id, direction: direction) {
                    viewModel.selectPod(next)
                }
            case .deployment:
                if let next = steppedItem(items: viewModel.visibleDeployments, currentID: viewModel.state.selectedDeployment?.id, direction: direction) {
                    viewModel.selectDeployment(next)
                }
            case .statefulSet:
                if let next = steppedItem(items: viewModel.visibleStatefulSets, currentID: viewModel.state.selectedStatefulSet?.id, direction: direction) {
                    viewModel.selectStatefulSet(next)
                }
            case .daemonSet:
                if let next = steppedItem(items: viewModel.visibleDaemonSets, currentID: viewModel.state.selectedDaemonSet?.id, direction: direction) {
                    viewModel.selectDaemonSet(next)
                }
            case .job:
                if let next = steppedItem(items: viewModel.visibleJobs, currentID: viewModel.state.selectedJob?.id, direction: direction) {
                    viewModel.selectJob(next)
                }
            case .cronJob:
                if let next = steppedItem(items: viewModel.visibleCronJobs, currentID: viewModel.state.selectedCronJob?.id, direction: direction) {
                    viewModel.selectCronJob(next)
                }
            case .replicaSet:
                if let next = steppedItem(items: viewModel.visibleReplicaSets, currentID: viewModel.state.selectedReplicaSet?.id, direction: direction) {
                    viewModel.selectReplicaSet(next)
                }
            case .horizontalPodAutoscaler:
                if let next = steppedItem(items: viewModel.visibleHorizontalPodAutoscalers, currentID: viewModel.state.selectedHorizontalPodAutoscaler?.id, direction: direction) {
                    viewModel.selectHorizontalPodAutoscaler(next)
                }
            default:
                break
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                if let next = steppedItem(items: viewModel.visibleServices, currentID: viewModel.state.selectedService?.id, direction: direction) {
                    viewModel.selectService(next)
                }
            case .ingress:
                if let next = steppedItem(items: viewModel.visibleIngresses, currentID: viewModel.state.selectedIngress?.id, direction: direction) {
                    viewModel.selectIngress(next)
                }
            case .networkPolicy:
                if let next = steppedItem(items: viewModel.visibleNetworkPolicies, currentID: viewModel.state.selectedNetworkPolicy?.id, direction: direction) {
                    viewModel.selectNetworkPolicy(next)
                }
            default:
                break
            }
        case .config:
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                if let next = steppedItem(items: viewModel.visibleConfigMaps, currentID: viewModel.state.selectedConfigMap?.id, direction: direction) {
                    viewModel.selectConfigMap(next)
                }
            case .secret:
                if let next = steppedItem(items: viewModel.visibleSecrets, currentID: viewModel.state.selectedSecret?.id, direction: direction) {
                    viewModel.selectSecret(next)
                }
            default:
                break
            }
        case .storage:
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                if let next = steppedItem(items: viewModel.visiblePersistentVolumeClaims, currentID: viewModel.state.selectedPersistentVolumeClaim?.id, direction: direction) {
                    viewModel.selectPersistentVolumeClaim(next)
                }
            case .persistentVolume:
                if let next = steppedItem(items: viewModel.visiblePersistentVolumes, currentID: viewModel.state.selectedPersistentVolume?.id, direction: direction) {
                    viewModel.selectPersistentVolume(next)
                }
            case .storageClass:
                if let next = steppedItem(items: viewModel.visibleStorageClasses, currentID: viewModel.state.selectedStorageClass?.id, direction: direction) {
                    viewModel.selectStorageClass(next)
                }
            case .node:
                if let next = steppedItem(items: viewModel.visibleNodes, currentID: viewModel.state.selectedNode?.id, direction: direction) {
                    viewModel.selectNode(next)
                }
            default:
                break
            }
        case .rbac:
            if let next = steppedItem(items: viewModel.visibleRBACResources, currentID: viewModel.state.selectedRBACResource?.id, direction: direction) {
                viewModel.selectRBACResource(next)
            }
        case .events:
            if let next = steppedItem(items: viewModel.visibleEvents, currentID: viewModel.state.selectedEvent?.id, direction: direction) {
                viewModel.selectEvent(next)
            }
        case .helm:
            if let next = steppedItem(items: viewModel.visibleHelmReleases, currentID: viewModel.state.selectedHelmRelease?.id, direction: direction) {
                viewModel.selectHelmRelease(next)
            }
        case .overview:
            moveOverviewCardSelection(direction)
        case .terminal:
            break
        }
    }

    private func moveOverviewCardSelection(_ direction: MoveCommandDirection) {
        guard !overviewCardModules.isEmpty else { return }
        let current = min(max(overviewCardSelectionIndex, 0), overviewCardModules.count - 1)
        guard let next = steppedIndex(count: overviewCardModules.count, current: current, direction: direction) else {
            return
        }
        overviewCardSelectionIndex = next
    }

    private func openSelectedOverviewCard() {
        guard !overviewCardModules.isEmpty else { return }
        let index = min(max(overviewCardSelectionIndex, 0), overviewCardModules.count - 1)
        overviewCardSelectionIndex = index
        viewModel.openOverviewModule(overviewCardModules[index])
    }

    /// In the middle/content pane, use left/right to switch between kind tabs
    /// (for sections that expose segmented kinds), and leave up/down for row stepping.
    private func moveContentKindIfNeeded(_ direction: MoveCommandDirection) -> Bool {
        guard direction == .left || direction == .right else { return false }
        guard let kinds = contentKindsForSelectedSection(), !kinds.isEmpty else { return false }
        guard let currentIndex = kinds.firstIndex(of: viewModel.state.selectedWorkloadKind) else { return false }

        let nextIndex: Int
        switch direction {
        case .right:
            nextIndex = (currentIndex + 1) % kinds.count
        case .left:
            nextIndex = (currentIndex + kinds.count - 1) % kinds.count
        default:
            return false
        }

        viewModel.setWorkloadKind(kinds[nextIndex])
        return true
    }

    private func contentKindsForSelectedSection() -> [KubeResourceKind]? {
        switch viewModel.state.selectedSection {
        case .workloads:
            return viewModel.workloadKinds
        case .networking:
            return viewModel.networkingKinds
        case .config:
            return viewModel.configKinds
        case .storage:
            return viewModel.storageKinds
        case .rbac:
            return viewModel.rbacKinds
        case .overview, .events, .helm, .terminal:
            return nil
        }
    }

    private func installLocalKeyboardMonitorIfNeeded() {
        guard localKeyEventMonitor == nil else { return }
        localKeyEventMonitor = NSEvent.addLocalMonitorForEvents(matching: [.keyDown]) { event in
            if let handledEvent = handleLocalKeyEvent(event) {
                return handledEvent
            }
            return nil
        }
    }

    private func handleLocalKeyEvent(_ event: NSEvent) -> NSEvent? {
        if shouldHandleTabNavigation(event) {
            if event.modifierFlags.contains(.shift) {
                focusPreviousKeyboardPane()
            } else {
                focusNextKeyboardPane()
            }
            return nil
        }

        guard shouldHandleConfiguredActionKey(event) else { return event }
        guard let action = configuredAction(for: event) else { return event }
        return performConfiguredAction(action) ? nil : event
    }

    private func shouldHandleTabNavigation(_ event: NSEvent) -> Bool {
        guard event.keyCode == 48 else { return false }
        let disallowedModifiers: NSEvent.ModifierFlags = [.command, .option, .control, .function]
        guard event.modifierFlags.isDisjoint(with: disallowedModifiers) else { return false }
        return textInputFocus == .contextSearch || textInputFocus == .resourceFilter
    }

    private func shouldHandleConfiguredActionKey(_ event: NSEvent) -> Bool {
        guard keyboardPaneFocus == .content || keyboardPaneFocus == .detail else { return false }
        guard !keyboardNavigationSuspended else { return false }
        let disallowedModifiers: NSEvent.ModifierFlags = [.command, .option, .control, .function]
        return event.modifierFlags.isDisjoint(with: disallowedModifiers)
    }

    private func configuredAction(for event: NSEvent) -> RuneKeyBindingAction? {
        guard let baseKey = event.charactersIgnoringModifiers?.lowercased(), baseKey.count == 1 else {
            return nil
        }

        let requiresShift = event.modifierFlags.contains(.shift)
        return RuneKeyBindingAction.allCases.first {
            UserDefaults.standard.runeKeyBindingShortcut(for: $0).matches(baseKey: baseKey, requiresShift: requiresShift)
        }
    }

    private func removeLocalKeyboardMonitor() {
        guard let localKeyEventMonitor else { return }
        NSEvent.removeMonitor(localKeyEventMonitor)
        self.localKeyEventMonitor = nil
    }

    private func moveDetailSelection(_ direction: MoveCommandDirection) {
        switch direction {
        case .left, .right:
            moveDetailInspectorTab(direction)
        case .up, .down:
            break
        @unknown default:
            break
        }
    }

    private func performConfiguredAction(_ action: RuneKeyBindingAction) -> Bool {
        switch action {
        case .describe:
            return openDescribeInspectorForSelection()
        case .logs:
            return openLogsInspectorForSelection()
        case .shell:
            return openShellInspectorForSelection()
        case .yaml:
            return openYAMLInspectorForSelection()
        case .portForward:
            return openPortForwardInspectorForSelection()
        case .rollout:
            return openRolloutInspectorForSelection()
        case .helmValues:
            return openHelmInspectorTab(.values)
        case .helmManifest:
            return openHelmInspectorTab(.manifest)
        case .helmHistory:
            return openHelmInspectorTab(.history)
        }
    }

    private func openDescribeInspectorForSelection() -> Bool {
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
        case .overview, .events, .helm, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private func openYAMLInspectorForSelection() -> Bool {
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
        case .overview, .events, .helm, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private func openLogsInspectorForSelection() -> Bool {
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
        case .overview, .config, .storage, .rbac, .events, .helm, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private func openShellInspectorForSelection() -> Bool {
        guard viewModel.state.selectedSection == .workloads,
              viewModel.state.selectedWorkloadKind == .pod,
              viewModel.state.selectedPod != nil else {
            return false
        }
        podInspectorTab = .exec
        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private func openPortForwardInspectorForSelection() -> Bool {
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

    private func openRolloutInspectorForSelection() -> Bool {
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

    private func openHelmInspectorTab(_ tab: HelmInspectorTab) -> Bool {
        guard viewModel.state.selectedSection == .helm,
              viewModel.state.selectedHelmRelease != nil else {
            return false
        }
        helmInspectorTab = tab
        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private var hasGenericManifestSelection: Bool {
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

    private func moveDetailInspectorTab(_ direction: MoveCommandDirection) {
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
            guard viewModel.state.selectedHelmRelease != nil else { return }
            helmInspectorTab = advancedTab(current: helmInspectorTab, direction: direction)
        case .overview, .events, .terminal:
            break
        }
    }

    private func advancedTab<T: CaseIterable & Equatable>(current: T, direction: MoveCommandDirection) -> T {
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

    private func steppedIndex(count: Int, current: Int, direction: MoveCommandDirection) -> Int? {
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

    private func steppedItem<T: Identifiable>(
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

    private func emitLayoutSnapshotIfNeeded() {
        let snapshot = RuneRootLayoutSnapshot(
            section: viewModel.state.selectedSection,
            workloadKind: viewModel.state.selectedWorkloadKind,
            measuredWindowTopInset: measuredWindowContentTopInset,
            resolvedWindowTopInset: RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: measuredWindowContentTopInset),
            contentMinY: layoutProbeFrames[.content]?.minY,
            headerMinY: layoutProbeFrames[.header]?.minY,
            detailMinY: layoutProbeFrames[.detail]?.minY,
            contentMinX: layoutProbeFrames[.content]?.minX,
            headerMinX: layoutProbeFrames[.header]?.minX,
            detailMinX: layoutProbeFrames[.detail]?.minX
        )

        guard snapshot != lastLayoutSnapshot else { return }

        lastLayoutSnapshot = snapshot
        RuneRootLayoutDebug.log(
            snapshot,
            shellVariant: resolvedShellVariant,
            inlineEditorImplementation: resolvedManifestInlineEditorImplementation
        )
        onLayoutSnapshotChange?(snapshot)
    }

    private func genericResourceListIdentity(_ resources: [ClusterResourceSummary]) -> String {
        resources.map(\.id).joined(separator: "|")
    }

    private func advanceLayoutGeneration() {
        layoutGeneration += 1
        layoutProbeFrames = [:]
        lastLayoutSnapshot = nil
    }

    private var resolvedSidebarWidth: CGFloat {
        clampedSidebarWidth(CGFloat(forcedInitialSidebarWidth ?? persistedSidebarWidth))
    }

    private var resolvedDetailWidth: CGFloat {
        clampedDetailWidth(CGFloat(forcedInitialDetailWidth ?? persistedDetailWidth))
    }

    private func clampedSidebarWidth(_ width: CGFloat) -> CGFloat {
        min(max(width, RuneUILayoutMetrics.splitSidebarMinWidth), RuneUILayoutMetrics.splitSidebarMaxWidth)
    }

    private func clampedDetailWidth(_ width: CGFloat) -> CGFloat {
        min(max(width, RuneUILayoutMetrics.splitDetailColumnMinWidth), RuneUILayoutMetrics.splitDetailColumnMaxWidth)
    }

    private func persistPaneWidthsIfNeeded(_ paneWidths: [RuneRootPaneWidthKind: CGFloat]) {
        if let sidebarWidth = paneWidths[.sidebar], sidebarWidth > 1 {
            persistSidebarWidthIfNeeded(sidebarWidth)
        }

        if let detailWidth = paneWidths[.detail], detailWidth > 1 {
            persistDetailWidthIfNeeded(detailWidth)
        }
    }

    private func persistSidebarWidthIfNeeded(_ width: CGFloat) {
        let clamped = clampedSidebarWidth(width)
        if abs(clamped - CGFloat(persistedSidebarWidth)) >= 1 {
            persistedSidebarWidth = Double(clamped)
            UserDefaults.standard.set(persistedSidebarWidth, forKey: RuneSettingsKeys.layoutSidebarWidth)
        }
    }

    private func persistDetailWidthIfNeeded(_ width: CGFloat) {
        let clamped = clampedDetailWidth(width)
        if abs(clamped - CGFloat(persistedDetailWidth)) >= 1 {
            persistedDetailWidth = Double(clamped)
            UserDefaults.standard.set(persistedDetailWidth, forKey: RuneSettingsKeys.layoutDetailWidth)
        }
    }

    /// Visual resize affordance on column edges (`49c6517`); hit testing stays on the system split divider.
    private var splitColumnResizeHandle: some View {
        VStack {
            Spacer(minLength: 0)
            RoundedRectangle(cornerRadius: 999, style: .continuous)
                .fill(Color.secondary.opacity(0.42))
                .frame(width: 4, height: 44)
                .overlay {
                    VStack(spacing: 4) {
                        Circle().fill(Color.primary.opacity(0.18)).frame(width: 2, height: 2)
                        Circle().fill(Color.primary.opacity(0.18)).frame(width: 2, height: 2)
                        Circle().fill(Color.primary.opacity(0.18)).frame(width: 2, height: 2)
                    }
                }
                .allowsHitTesting(false)
            Spacer(minLength: 0)
        }
        .frame(width: 14)
    }

    private var productionBanner: some View {
        HStack(spacing: 8) {
            Image(systemName: "exclamationmark.triangle.fill")
            Text("Production context active")
                .font(.subheadline.weight(.bold))
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 7)
        .background(Color.red.opacity(0.87), in: Capsule())
    }

    private var commandPalettePresentedBinding: Binding<Bool> {
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

    private var pendingWriteActionPresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.pendingWriteAction != nil },
            set: { value in
                if !value {
                    viewModel.cancelPendingWriteAction()
                }
            }
        )
    }

    private var overviewStatusBanner: some View {
        HStack(spacing: 8) {
            Image(systemName: viewModel.isProductionContext ? "exclamationmark.shield.fill" : "checkmark.shield.fill")
                .foregroundStyle(viewModel.isProductionContext ? .red : .green)
            Text(viewModel.isProductionContext ? "Production context active" : "Non-production context")
                .font(.subheadline.weight(.semibold))
            Spacer()
            Text(viewModel.state.isReadOnlyMode ? "Read-only" : "Read/Write")
                .font(.caption.weight(.bold))
                .padding(.horizontal, 8)
                .frame(height: 28)
                .background((viewModel.state.isReadOnlyMode ? Color.orange : Color.green).opacity(0.24), in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
        }
        .padding(12)
        .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
    }

    private func contextUsageBadge(label: String, value: String) -> some View {
        HStack(spacing: 4) {
            Text(label + ":")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            Text(value)
                .font(.caption.weight(.bold))
        }
        .padding(.horizontal, RuneUILayoutMetrics.headerChipHorizontalPadding)
        .frame(height: RuneUILayoutMetrics.headerChipHeight)
        .background(Color.secondary.opacity(0.14), in: Capsule())
    }

    private var podTableHeader: some View {
        HStack(spacing: PodTableLayout.metricsSpacing) {
            podSortHeaderButton(title: "Name", column: .name)
                .frame(maxWidth: .infinity, alignment: .leading)
            HStack(spacing: PodTableLayout.metricsSpacing) {
                podSortHeaderButton(title: "CPU", column: .cpu, width: PodTableLayout.cpuWidth, alignment: .trailing)
                    .help("CPU in millicores (1000m = 1 core), from the metrics snapshot Rune loaded when available.")
                podSortHeaderButton(title: "MEM", column: .memory, width: PodTableLayout.memoryWidth, alignment: .trailing)
                podSortHeaderButton(title: "Restarts", column: .restarts, width: PodTableLayout.restartsWidth, alignment: .trailing)
                podSortHeaderButton(title: "Age", column: .age, width: PodTableLayout.ageWidth, alignment: .trailing)
            }
            podSortHeaderButton(title: "Status", column: .status, width: PodTableLayout.statusTotalWidth, alignment: .center)
        }
        .padding(.horizontal, PodTableLayout.headerHorizontalInset)
        .padding(.top, 4)
        .padding(.bottom, PodTableLayout.headerBottomSpacing)
        .textCase(nil)
    }

    private func podSortHeaderButton(
        title: String,
        column: PodListSortColumn,
        width: CGFloat? = nil,
        alignment: Alignment = .leading
    ) -> some View {
        Button {
            viewModel.togglePodSort(column)
        } label: {
            HStack(spacing: 4) {
                Text(title)
                if viewModel.podSortColumn == column {
                    Image(systemName: viewModel.podSortAscending ? "arrow.up" : "arrow.down")
                        .font(.system(size: 10, weight: .bold))
                }
            }
            .font(.caption.weight(.semibold))
            .foregroundStyle(.secondary)
            .frame(width: width, alignment: alignment)
        }
        .buttonStyle(.plain)
    }

    private func contextUsageValue(_ value: Int?) -> String {
        if let value {
            return "\(value)%"
        }
        return viewModel.state.isLoading ? "..." : "n/a"
    }

    private func overviewStatCard(
        title: String,
        count: Int,
        symbol: String,
        tint: Color,
        isLoading: Bool = false,
        isKeyboardFocused: Bool = false,
        action: @escaping () -> Void
    ) -> some View {
        Button(action: action) {
            VStack(alignment: .leading, spacing: 7) {
                HStack {
                    Image(systemName: symbol)
                        .foregroundStyle(tint)
                    Spacer()
                    Image(systemName: "arrow.up.right.square")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                if isLoading && count == 0 {
                    ProgressView()
                        .controlSize(.small)
                        .scaleEffect(0.85)
                        .padding(.vertical, 5)
                } else {
                    HStack(spacing: 6) {
                        Text("\(count)")
                            .font(.title2.weight(.bold))
                        if isLoading {
                            ProgressView()
                                .controlSize(.small)
                                .scaleEffect(0.75)
                        }
                    }
                }
                Text(title)
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)
            }
            .padding(12)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous)
                    .stroke(isKeyboardFocused ? Color.accentColor.opacity(0.8) : Color.clear, lineWidth: 1.5)
            }
        }
        .buttonStyle(.plain)
    }

    private func healthBadge(label: String, value: Int, color: Color) -> some View {
        HStack(spacing: 6) {
            Circle()
                .fill(color)
                .frame(width: 7, height: 7)
            Text(label + ": \(value)")
                .font(.caption.weight(.semibold))
        }
        .padding(.horizontal, 9)
        .padding(.vertical, 5)
        .background(color.opacity(0.14), in: Capsule())
    }

    private func inspectorInsetCard<Content: View>(@ViewBuilder content: () -> Content) -> some View {
        content()
            .runeInsetCard()
    }

    @ViewBuilder
    private func inspectorActionButtonRow<Content: View>(@ViewBuilder content: () -> Content) -> some View {
        ViewThatFits(in: .horizontal) {
            HStack(alignment: .center, spacing: 8) {
                content()
            }
            VStack(alignment: .leading, spacing: 8) {
                content()
            }
        }
        .controlSize(.regular)
    }

    private func deploymentReplicaStatusColor(_ deployment: DeploymentSummary) -> Color {
        if deployment.desiredReplicas == 0 { return .secondary }
        if deployment.readyReplicas >= deployment.desiredReplicas { return .green }
        if deployment.readyReplicas > 0 { return .orange }
        return .red
    }

    private func deploymentReplicaStatusText(_ deployment: DeploymentSummary) -> String {
        if deployment.desiredReplicas == 0 {
            return "Scaled to zero"
        }
        return "\(deployment.readyReplicas) of \(deployment.desiredReplicas) ready"
    }

    private func deploymentScaleIsDirty(_ deployment: DeploymentSummary) -> Bool {
        viewModel.scaleReplicaInput != deployment.desiredReplicas
    }

    @ViewBuilder
    private func deploymentScaleButton(deployment: DeploymentSummary) -> some View {
        Group {
            if deploymentScaleIsDirty(deployment), viewModel.canApplyClusterMutations {
                Button("Scale") {
                    viewModel.requestScaleSelectedDeployment()
                }
                .buttonStyle(.borderedProminent)
            } else {
                Button("Scale") {
                    viewModel.requestScaleSelectedDeployment()
                }
                .buttonStyle(.bordered)
            }
        }
        .disabled(!viewModel.canApplyClusterMutations || !deploymentScaleIsDirty(deployment))
    }

    private func podOverviewSection(pod: PodSummary) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            if shouldShowResourceNamespaceLabel(pod.namespace) {
                inspectorInsetCard {
                    Label("Namespace: \(pod.namespace)", systemImage: "square.stack.3d.up")
                        .font(.subheadline.weight(.medium))
                        .foregroundStyle(.secondary)
                }
            }

            inspectorInsetCard {
                VStack(alignment: .leading, spacing: 10) {
                    podOverviewRow(title: "Status", value: pod.status, symbol: "waveform.path.ecg")
                    Divider()
                        .opacity(0.45)
                    podOverviewRow(title: "Ready", value: pod.containersReady ?? "—", symbol: "checkmark.circle")
                    podOverviewRow(title: "Restarts", value: "\(pod.totalRestarts)", symbol: "arrow.clockwise")
                    podOverviewRow(title: "Age", value: pod.ageDescription, symbol: "clock")
                    podOverviewRow(title: "CPU", value: pod.cpuDisplay, symbol: "cpu")
                    podOverviewRow(title: "Memory", value: pod.memoryDisplay, symbol: "memorychip")
                    Divider()
                        .opacity(0.45)
                    podOverviewRow(title: "Node", value: pod.nodeName ?? "—", symbol: "server.rack")
                    podOverviewRow(title: "Pod IP", value: pod.podIP ?? "—", symbol: "network")
                    podOverviewRow(title: "Host IP", value: pod.hostIP ?? "—", symbol: "cable.connector")
                    podOverviewRow(title: "QoS class", value: pod.qosClass ?? "—", symbol: "slider.horizontal.3")
                    if let containers = pod.containerNamesLine, !containers.isEmpty {
                        VStack(alignment: .leading, spacing: 6) {
                            HStack(spacing: 6) {
                                Image(systemName: "square.stack.3d.forward.dottedline")
                                    .foregroundStyle(.secondary)
                                    .frame(width: 14, alignment: .center)
                                Text("Containers")
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(.secondary)
                            }
                            Text(containers)
                                .font(.system(size: 12, weight: .regular, design: .monospaced))
                                .textSelection(.enabled)
                                .frame(maxWidth: .infinity, alignment: .leading)
                        }
                    }

                    Divider()
                        .opacity(0.45)

                    Button("Delete", role: .destructive) {
                        viewModel.requestDeleteSelectedResource()
                    }
                    .disabled(!viewModel.canApplyClusterMutations)
                }
            }
        }
    }

    private func podOverviewRow(title: String, value: String, symbol: String) -> some View {
        ViewThatFits(in: .horizontal) {
            HStack(alignment: .firstTextBaseline, spacing: 10) {
                podOverviewRowLabel(title: title, symbol: symbol, fixedWidth: true)
                Text(value)
                    .font(.body.weight(.medium))
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .fixedSize(horizontal: false, vertical: true)
            }
            VStack(alignment: .leading, spacing: 4) {
                podOverviewRowLabel(title: title, symbol: symbol, fixedWidth: false)
                Text(value)
                    .font(.body.weight(.medium))
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }

    private func podOverviewRowLabel(title: String, symbol: String, fixedWidth: Bool) -> some View {
        HStack(spacing: 6) {
            Image(systemName: symbol)
                .foregroundStyle(.secondary)
                .frame(width: 14, alignment: .center)
            Text(title)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
        }
        .frame(width: fixedWidth ? 118 : nil, alignment: .leading)
    }

    private func deploymentOverviewSection(deployment: DeploymentSummary) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            if shouldShowResourceNamespaceLabel(deployment.namespace) {
                inspectorInsetCard {
                    Label("Namespace: \(deployment.namespace)", systemImage: "square.stack.3d.up")
                        .font(.subheadline.weight(.medium))
                        .foregroundStyle(.secondary)
                }
            }

            inspectorInsetCard {
                VStack(alignment: .leading, spacing: 12) {
                    HStack(spacing: 8) {
                        Circle()
                            .fill(deploymentReplicaStatusColor(deployment))
                            .frame(width: 8, height: 8)
                        Text(deploymentReplicaStatusText(deployment))
                            .font(.body.weight(.semibold))
                    }

                    ViewThatFits(in: .horizontal) {
                        HStack(alignment: .center, spacing: 8) {
                            Text("Replicas")
                                .font(.subheadline.weight(.medium))
                                .foregroundStyle(.secondary)
                            Stepper(value: $viewModel.scaleReplicaInput, in: 0...500) {
                                Text("\(viewModel.scaleReplicaInput)")
                                    .monospacedDigit()
                                    .font(.body.weight(.medium))
                                    .frame(minWidth: 32, alignment: .trailing)
                            }
                            deploymentScaleButton(deployment: deployment)
                            Spacer(minLength: 0)
                        }
                        VStack(alignment: .leading, spacing: 10) {
                            HStack(alignment: .center, spacing: 8) {
                                Text("Replicas")
                                    .font(.subheadline.weight(.medium))
                                    .foregroundStyle(.secondary)
                                Stepper(value: $viewModel.scaleReplicaInput, in: 0...500) {
                                    Text("\(viewModel.scaleReplicaInput)")
                                        .monospacedDigit()
                                        .font(.body.weight(.medium))
                                        .frame(minWidth: 32, alignment: .trailing)
                                }
                                Spacer(minLength: 0)
                            }
                            deploymentScaleButton(deployment: deployment)
                                .frame(maxWidth: .infinity, alignment: .leading)
                        }
                    }

                    Divider()
                        .opacity(0.45)

                    inspectorActionButtonRow {
                        Button("Restart Rollout") {
                            viewModel.requestRolloutRestartSelectedDeployment()
                        }
                        .buttonStyle(.bordered)
                        .disabled(!viewModel.canApplyClusterMutations)

                        Button("Apply YAML") {
                            viewModel.requestApplySelectedResourceYAML()
                        }
                        .buttonStyle(.bordered)
                        .disabled(!viewModel.canApplyClusterMutations)

                        Spacer(minLength: 0)
                    }

                    Button("Delete", role: .destructive) {
                        viewModel.requestDeleteSelectedResource()
                    }
                    .disabled(!viewModel.canApplyClusterMutations)
                }
            }
        }
    }

    private func inspectorEmptyState(_ message: String, symbol: String, detail: String = "Select an item in the center list to inspect details and actions here.") -> some View {
        VStack(spacing: 10) {
            Image(systemName: symbol)
                .font(.system(size: 18, weight: .semibold))
                .foregroundStyle(.secondary)
            Text(message)
                .font(.subheadline.weight(.semibold))
            Text(detail)
                .font(.footnote)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)
    }

    private var namespaceSuggestions: [String] {
        viewModel.namespaceOptions
    }

    private var namespaceMenuTitle: String {
        let trimmed = viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "Namespace" : trimmed
    }

    private func openSettingsWindow() {
        openSettings()
    }

    /// When the toolbar already scopes to a namespace, omit duplicate namespace chips/labels for resources in that namespace.
    private func shouldShowResourceNamespaceLabel(_ resourceNamespace: String?) -> Bool {
        guard let ns = resourceNamespace?.trimmingCharacters(in: .whitespacesAndNewlines), !ns.isEmpty else {
            return false
        }
        let selected = viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        if selected.isEmpty { return true }
        return ns.caseInsensitiveCompare(selected) != .orderedSame
    }

    private var panelFill: some ShapeStyle {
        RuneSurfaceKind.panel.fill
    }

    private var editorFill: Color {
        RuneSurfaceKind.editor.fill
    }

    private func contentListRowChrome(isSelected: Bool) -> some View {
        RuneSurfaceBackground(kind: .listRow(isSelected: isSelected))
    }

    private func eventHint(for event: EventSummary) -> String {
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if kind.isEmpty {
            return "Shows details in the inspector. Use the action button to open the involved resource when listed."
        }
        return "Shows details in the inspector. \"Go to …\" switches section and selects \(kind) \(event.objectName) when present."
    }

    private func podStatusCount(_ status: String) -> Int {
        viewModel.state.overviewPods.filter { $0.status.lowercased() == status }.count
    }

    private func statusColor(for status: String) -> Color {
        switch status.lowercased() {
        case "running": return .green
        case "pending": return .orange
        case "failed": return .red
        case "succeeded": return .blue
        default: return .gray
        }
    }

    private func portForwardStatusColor(_ status: PortForwardStatus) -> Color {
        switch status {
        case .starting: return .orange
        case .active: return .green
        case .stopped: return .secondary
        case .failed: return .red
        }
    }

    private func execOutputText(for result: PodExecResult) -> String {
        let stdout = result.stdout.isEmpty ? "" : result.stdout
        let stderr = result.stderr.isEmpty ? "" : "\n[stderr]\n\(result.stderr)"
        let merged = stdout + stderr
        return merged.isEmpty ? "No output" : merged
    }
}
