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

    public init(
        section: RuneSection,
        workloadKind: KubeResourceKind,
        measuredWindowTopInset: CGFloat?,
        resolvedWindowTopInset: CGFloat,
        contentMinY: CGFloat?,
        headerMinY: CGFloat?,
        detailMinY: CGFloat?
    ) {
        self.section = section
        self.workloadKind = workloadKind
        self.measuredWindowTopInset = measuredWindowTopInset
        self.resolvedWindowTopInset = resolvedWindowTopInset
        self.contentMinY = contentMinY
        self.headerMinY = headerMinY
        self.detailMinY = detailMinY
    }
}

private enum RuneRootLayoutProbeKind: Hashable {
    case content
    case header
    case detail
}

private struct RuneRootLayoutProbeFrame: Equatable {
    let generation: Int
    let rect: CGRect
}

private enum RuneRootLayoutDebug {
    static let coordinateSpaceName = "RuneRootLayoutSpace"
    static let isEnabled = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT"] == "1"

    static func log(_ snapshot: RuneRootLayoutSnapshot) {
        guard isEnabled else { return }

        NSLog(
            "[Rune][Layout] section=%@ kind=%@ measuredTopInset=%@ resolvedTopInset=%.1f contentMinY=%@ headerMinY=%@ detailMinY=%@",
            snapshot.section.rawValue,
            snapshot.workloadKind.kubectlName,
            snapshot.measuredWindowTopInset.map { String(format: "%.1f", $0) } ?? "nil",
            snapshot.resolvedWindowTopInset,
            snapshot.contentMinY.map { String(format: "%.1f", $0) } ?? "nil",
            snapshot.headerMinY.map { String(format: "%.1f", $0) } ?? "nil",
            snapshot.detailMinY.map { String(format: "%.1f", $0) } ?? "nil"
        )
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

public struct RuneRootView: View {
    @ObservedObject private var viewModel: RuneAppViewModel

    private let onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)?
    private let debugDisableBootstrap: Bool

    @State private var measuredWindowContentTopInset: CGFloat?
    @State private var layoutGeneration = 0
    @State private var layoutProbeFrames: [RuneRootLayoutProbeKind: CGRect] = [:]
    @State private var lastLayoutSnapshot: RuneRootLayoutSnapshot?
    @State private var podInspectorTab: PodInspectorTab = .overview
    @State private var serviceInspectorTab: ServiceInspectorTab = .overview
    @State private var deploymentInspectorTab: DeploymentInspectorTab = .overview
    @State private var helmInspectorTab: HelmInspectorTab = .overview
    @State private var genericResourceManifestTab: GenericResourceManifestTab = .describe
    /// Toggles YAML inspector between read-only `ScrollView` and `TextEditor`.
    @State private var yamlManifestIsEditing = false
    /// Toggles describe inspector between read-only `ScrollView` and `TextEditor`.
    @State private var describePaneIsEditing = false

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
            initialGenericResourceManifestTab: .describe
        )
    }

    init(
        viewModel: RuneAppViewModel,
        onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)?,
        debugDisableBootstrap: Bool,
        initialPodInspectorTab: PodInspectorTab = .overview,
        initialServiceInspectorTab: ServiceInspectorTab = .overview,
        initialDeploymentInspectorTab: DeploymentInspectorTab = .overview,
        initialGenericResourceManifestTab: GenericResourceManifestTab = .describe
    ) {
        self.viewModel = viewModel
        self.onLayoutSnapshotChange = onLayoutSnapshotChange
        self.debugDisableBootstrap = debugDisableBootstrap
        _podInspectorTab = State(initialValue: initialPodInspectorTab)
        _serviceInspectorTab = State(initialValue: initialServiceInspectorTab)
        _deploymentInspectorTab = State(initialValue: initialDeploymentInspectorTab)
        _genericResourceManifestTab = State(initialValue: initialGenericResourceManifestTab)
    }

    public var body: some View {
        ZStack(alignment: .topLeading) {
            background
            WindowChromeConfigurator(measuredTopInset: $measuredWindowContentTopInset)
                .frame(width: 0, height: 0)

            mainSplitContainer
            .padding(.top, RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: measuredWindowContentTopInset))
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
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
                    Button("Palette") {
                        viewModel.presentCommandPalette()
                    }
                    .keyboardShortcut("k", modifiers: .command)

                    Button("Reload") {
                        viewModel.refreshCurrentView()
                    }
                    .keyboardShortcut("r", modifiers: .command)
                }
            }
            .toolbarBackground(.visible, for: .windowToolbar)
            .sheet(isPresented: commandPalettePresentedBinding) {
                CommandPaletteView(viewModel: viewModel)
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
                guard !debugDisableBootstrap else { return }
                viewModel.bootstrapIfNeeded()
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
        .onChange(of: viewModel.state.selectedSection) { _, _ in
            advanceLayoutGeneration()
        }
        .onChange(of: viewModel.state.selectedWorkloadKind) { _, _ in
            advanceLayoutGeneration()
        }
        .animation(.easeInOut(duration: 0.2), value: viewModel.isProductionContext)
    }

    private var mainSplitContainer: some View {
        HSplitView {
            sidebar
                .padding(.trailing, RuneUILayoutMetrics.splitColumnGutter / 2)
                .runeAppKitFrameReporter("sidebar")
                .frame(minWidth: 220, idealWidth: 280, maxWidth: 480)
                .overlay(alignment: .trailing) {
                    splitColumnTrailingChrome()
                }
            contentPane
                .padding(.horizontal, RuneUILayoutMetrics.splitColumnGutter / 2)
                .runeAppKitFrameReporter("content")
                .frame(
                    minWidth: RuneUILayoutMetrics.splitContentColumnMinWidth,
                    idealWidth: 760,
                    maxWidth: .infinity
                )
                .layoutPriority(2)
                .overlay(alignment: .trailing) {
                    splitColumnTrailingChrome()
                }
            detailPane
                .padding(.leading, RuneUILayoutMetrics.splitColumnGutter / 2)
                .runeAppKitFrameReporter("detail")
                .frame(
                    minWidth: RuneUILayoutMetrics.splitDetailColumnMinWidth,
                    idealWidth: 540,
                    maxWidth: .infinity
                )
                .layoutPriority(2)
        }
    }

    private var background: some View {
        Color(nsColor: .windowBackgroundColor)
            .ignoresSafeArea()
    }

    private var sidebar: some View {
        VStack(alignment: .leading, spacing: 14) {
            // Sidebar filter: flat fill only — `.docs/rune-design-plan.md` §6.2 (avoid heavy bordered fields).
            TextField("Search contexts", text: Binding(get: {
                viewModel.state.contextSearchQuery
            }, set: { newValue in
                viewModel.setContextSearchQuery(newValue)
            }))
            .textFieldStyle(.plain)
            .font(.body)
            .padding(.horizontal, 10)
            .padding(.vertical, 7)
            .background(
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                    .fill(Color.primary.opacity(0.07))
            )

            Text("Sections")
                .font(.headline)
                .foregroundStyle(.secondary)

            ForEach(RuneSection.allCases) { section in
                sectionRow(section)
            }

            Divider()
                .overlay(Color(nsColor: .separatorColor))

            Text("Contexts")
                .font(.headline)
                .foregroundStyle(.secondary)

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
                        .padding(10)
                        .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
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
        .padding(RuneUILayoutMetrics.sidebarPadding)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        // Navigation rail — quieter than the main list (`.docs/rune-design-plan.md` §4.3 / §7.2).
        .background {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)
                .fill(.ultraThinMaterial)
        }
        .compositingGroup()
        .clipShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous))
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
            .background(
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                    .fill(viewModel.state.selectedSection == section ? Color.accentColor.opacity(0.16) : Color.clear)
            )
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
                .background(
                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                        .fill(viewModel.state.selectedContext == context ? Color.accentColor.opacity(0.12) : Color.clear)
                )
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
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        // Primary workspace surface — brighter than sidebar, calmer than inspector (`windowBackgroundColor` semantic fill).
        .background {
            ZStack {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)
                    .fill(Color(nsColor: .windowBackgroundColor))
                RuneRootLayoutProbe(kind: .content, generation: layoutGeneration)
            }
        }
        .compositingGroup()
        .clipShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous))
    }

    private var contentHeader: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text(viewModel.state.selectedSection.title)
                    .font(.title2.weight(.bold))

                Spacer()

                if let context = viewModel.state.selectedContext {
                    Label(context.name, systemImage: "network")
                        .font(.caption.weight(.semibold))
                        .labelStyle(.titleAndIcon)
                        .imageScale(.small)
                        .lineLimit(1)
                        .padding(.horizontal, RuneUILayoutMetrics.headerChipHorizontalPadding)
                        .frame(height: RuneUILayoutMetrics.headerChipHeight)
                        .background(.thinMaterial, in: Capsule())
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
                        .frame(height: RuneUILayoutMetrics.headerChipHeight)
                        .background(Color.orange.opacity(0.16), in: Capsule())
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
        runeSegmentedControlInScroll(
            title: "Kind",
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

    /// `NSSegmentedControl` uses intrinsic width; many segments otherwise extend past the `HSplitView` column and draw under the sidebar. Scroll instead of clipping.
    private func runeSegmentedControlInScroll<SelectionValue: Hashable, Content: View>(
        title: LocalizedStringKey,
        selection: Binding<SelectionValue>,
        labelsHidden: Bool = false,
        @ViewBuilder content: () -> Content
    ) -> some View {
        ScrollView(.horizontal, showsIndicators: false) {
            Group {
                if labelsHidden {
                    Picker(title, selection: selection, content: content)
                        .pickerStyle(.segmented)
                        .labelsHidden()
                        .fixedSize(horizontal: true, vertical: false)
                } else {
                    Picker(title, selection: selection, content: content)
                        .pickerStyle(.segmented)
                        .fixedSize(horizontal: true, vertical: false)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
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
                    .padding(14)
                    .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
                }

                overviewStatusBanner

                LazyVGrid(columns: [GridItem(.adaptive(minimum: 160), spacing: 10)], spacing: 10) {
                    overviewStatCard(title: "Pods", value: "\(viewModel.state.overviewPods.count)", symbol: "cube.box.fill", tint: .cyan) {
                        viewModel.openOverviewModule(.pods)
                    }
                    overviewStatCard(title: "Deployments", value: "\(viewModel.state.overviewDeploymentsCount)", symbol: "shippingbox.fill", tint: .blue) {
                        viewModel.openOverviewModule(.deployments)
                    }
                    overviewStatCard(title: "Services", value: "\(viewModel.state.overviewServicesCount)", symbol: "point.3.connected.trianglepath.dotted", tint: .purple) {
                        viewModel.openOverviewModule(.services)
                    }
                    overviewStatCard(title: "Ingresses", value: "\(viewModel.state.overviewIngressesCount)", symbol: "network", tint: .indigo) {
                        viewModel.openOverviewModule(.ingresses)
                    }
                    overviewStatCard(title: "ConfigMaps", value: "\(viewModel.state.overviewConfigMapsCount)", symbol: "doc.text.fill", tint: .teal) {
                        viewModel.openOverviewModule(.configMaps)
                    }
                    overviewStatCard(title: "Nodes", value: "\(viewModel.state.overviewNodesCount)", symbol: "server.rack", tint: .gray) {
                        viewModel.openOverviewModule(.nodes)
                    }
                    overviewStatCard(title: "Events", value: "\(viewModel.state.overviewEvents.count)", symbol: "bolt.badge.clock.fill", tint: .orange) {
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
                .padding(12)
                .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))

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
                .padding(12)
                .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
            }
        }
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
                                HStack(spacing: 10) {
                                    Text(pod.name)
                                        .font(.body.weight(.medium))
                                        .lineLimit(1)
                                        .frame(maxWidth: .infinity, alignment: .leading)

                                    HStack(spacing: 10) {
                                        Text(pod.cpuDisplay)
                                            .frame(width: 44, alignment: .trailing)
                                        Text(pod.memoryDisplay)
                                            .frame(width: 56, alignment: .trailing)
                                        Text("\(pod.totalRestarts)")
                                            .frame(width: 56, alignment: .trailing)
                                        Text(pod.ageDescription)
                                            .frame(width: 44, alignment: .trailing)
                                    }
                                    .font(.system(size: 11, weight: .regular, design: .monospaced))
                                    .foregroundStyle(.secondary)

                                    Text(pod.status)
                                        .font(.caption.weight(.semibold))
                                        .lineLimit(1)
                                        .minimumScaleFactor(0.78)
                                        .multilineTextAlignment(.center)
                                        .frame(width: 120, alignment: .center)
                                        .padding(.horizontal, 8)
                                        .padding(.vertical, 2)
                                        .background(statusColor(for: pod.status).opacity(0.22), in: Capsule())
                                        .foregroundStyle(statusColor(for: pod.status))
                                        .help("Phase (kubectl)")
                                }
                                .padding(.vertical, 3)
                                .background(
                                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                                        .fill(viewModel.state.selectedPod?.id == pod.id ? Color.accentColor.opacity(0.12) : Color.clear)
                                )
                            }
                            .buttonStyle(.plain)
                        }
                    } header: {
                        podTableHeader
                    }
                }

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
                        .padding(.vertical, 3)
                        .background(
                            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                                .fill(viewModel.state.selectedDeployment == deployment ? Color.accentColor.opacity(0.12) : Color.clear)
                        )
                    }
                    .buttonStyle(.plain)
                }

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
                    LazyVStack(alignment: .leading, spacing: 4) {
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
                                .padding(.vertical, 3)
                                .padding(.horizontal, 2)
                                .contentShape(Rectangle())
                                .background(
                                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                                        .fill(viewModel.state.selectedService == service ? Color.accentColor.opacity(0.12) : Color.clear)
                                )
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding(.vertical, 2)
                }

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
                .padding(.vertical, 4)
                .background(
                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                        .fill(viewModel.state.selectedHelmRelease == release ? Color.accentColor.opacity(0.12) : Color.clear)
                )
            }
            .buttonStyle(.plain)
        }
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
                .padding(.vertical, 4)
                .background(
                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                        .fill(viewModel.state.selectedEvent == event ? Color.accentColor.opacity(0.12) : Color.clear)
                )
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .help(eventHint(for: event))
        }
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
            .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))

            if let error = viewModel.state.lastError {
                Text(error)
                    .font(.footnote)
                    .foregroundStyle(.red)
                    .padding(.top, 8)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(.top, RuneUILayoutMetrics.paneOuterPadding)
        .padding(.bottom, RuneUILayoutMetrics.paneOuterPadding)
        .padding(.trailing, RuneUILayoutMetrics.paneOuterPadding)
        .padding(.leading, RuneUILayoutMetrics.paneOuterPadding + RuneUILayoutMetrics.inspectorLeadingInset)
        // Elevated inspector column; same shell radius as sidebar/content.
        .background {
            ZStack {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)
                    .fill(.regularMaterial)
                RuneRootLayoutProbe(kind: .detail, generation: layoutGeneration)
            }
        }
        .compositingGroup()
        .clipShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous))
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
                    viewModel.refreshCurrentView()
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
                        .disabled(!viewModel.writeActionsEnabled)
                    } else {
                        Button("Suspend") {
                            viewModel.setSelectedCronJobSuspended(true)
                        }
                        .disabled(!viewModel.writeActionsEnabled)
                    }
                    Button("Create job now") {
                        viewModel.createManualJobFromSelectedCronJob()
                    }
                    .disabled(!viewModel.writeActionsEnabled)
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

                    runeSegmentedControlInScroll(
                        title: "",
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
                                .disabled(!viewModel.writeActionsEnabled)
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

                    runeSegmentedControlInScroll(
                        title: "",
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
                            VStack(alignment: .leading, spacing: 10) {
                                logsToolbar
                                podLogsOutputScroll()
                            }

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

                    runeSegmentedControlInScroll(
                        title: "",
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
                            VStack(alignment: .leading, spacing: 10) {
                                logsToolbar

                                if !viewModel.state.unifiedServiceLogPods.isEmpty {
                                    Text("Pods: " + viewModel.state.unifiedServiceLogPods.joined(separator: ", "))
                                        .font(.footnote.weight(.medium))
                                        .foregroundStyle(.secondary)
                                }

                                unifiedLogsOutputScroll()
                            }

                        case .rollout:
                            VStack(alignment: .leading, spacing: 10) {
                                HStack(spacing: 10) {
                                    TextField("Revision (optional)", text: $viewModel.rolloutRevisionInput)
                                        .textFieldStyle(.roundedBorder)
                                        .frame(maxWidth: 160)

                                    Button("Rollback") {
                                        viewModel.requestRolloutUndoSelectedDeployment()
                                    }
                                    .disabled(!viewModel.writeActionsEnabled)

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

                    runeSegmentedControlInScroll(
                        title: "",
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
                                                .disabled(!viewModel.writeActionsEnabled)
                                            Button("Export…") { viewModel.saveCurrentResourceYAML() }
                                                .buttonStyle(.bordered)
                                            Spacer(minLength: 0)
                                        }
                                        Button("Delete", role: .destructive) {
                                            viewModel.requestDeleteSelectedResource()
                                        }
                                        .disabled(!viewModel.writeActionsEnabled)
                                    }
                                }
                            }

                        case .unifiedLogs:
                            VStack(alignment: .leading, spacing: 10) {
                                logsToolbar

                                if !viewModel.state.unifiedServiceLogPods.isEmpty {
                                    Text("Pods: " + viewModel.state.unifiedServiceLogPods.joined(separator: ", "))
                                        .font(.footnote.weight(.medium))
                                        .foregroundStyle(.secondary)
                                }

                                unifiedLogsOutputScroll()
                            }

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

                    runeSegmentedControlInScroll(title: "Manifest", selection: $genericResourceManifestTab) {
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

    private var yamlDraftBinding: Binding<String> {
        Binding(
            get: { viewModel.state.resourceYAML },
            set: { viewModel.state.updateResourceYAMLDraft($0) }
        )
    }

    private var describeDraftBinding: Binding<String> {
        Binding(
            get: { viewModel.state.resourceDescribe },
            set: { viewModel.state.updateResourceDescribeDraft($0) }
        )
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
        return "No YAML available for \(manifestResourceReference).\n\nThe resource may still be loading, may not exist in the active namespace, or kubectl returned an empty manifest."
    }

    private var yamlFooterText: String {
        if viewModel.state.isLoadingResourceDetails {
            return "Loading resource YAML from the cluster."
        }
        if viewModel.state.lastResourceYAMLError != nil {
            return "YAML could not be loaded for the current selection. Check context, namespace and kubectl access."
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
        return "No describe output available for \(manifestResourceReference).\n\nThe resource may still be loading, may not exist in the active namespace, or kubectl describe returned no output."
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

    private var yamlBlock: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 8) {
                if viewModel.state.resourceYAMLHasUnsavedEdits {
                    Text("Unsaved edits")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.orange)
                }
                Spacer(minLength: 0)
            }

            ViewThatFits(in: .horizontal) {
                HStack(spacing: 8) {
                    Button("Apply YAML") {
                        viewModel.requestApplySelectedResourceYAML()
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(
                        !viewModel.writeActionsEnabled
                            || viewModel.state.resourceYAML.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                    )

                    Button(yamlManifestIsEditing ? "Done" : "Edit") {
                        yamlManifestIsEditing.toggle()
                    }
                    .buttonStyle(.bordered)
                    .disabled(!viewModel.writeActionsEnabled || viewModel.state.resourceYAML.isEmpty)

                    Button("Revert") {
                        viewModel.revertResourceYAMLDraft()
                        yamlManifestIsEditing = false
                    }
                    .buttonStyle(.bordered)
                    .disabled(!viewModel.state.resourceYAMLHasUnsavedEdits)

                    Divider()
                        .frame(height: 16)

                    Button("Import…") {
                        viewModel.importResourceYAMLFromFile()
                        if viewModel.writeActionsEnabled {
                            yamlManifestIsEditing = true
                        }
                    }
                    .buttonStyle(.bordered)
                    .help("Replace the editor with the contents of a YAML file")

                    Button("Export…") {
                        viewModel.saveCurrentResourceYAML()
                    }
                    .buttonStyle(.bordered)
                    .disabled(viewModel.state.resourceYAML.isEmpty)

                    Spacer(minLength: 0)
                }
                .fixedSize(horizontal: true, vertical: false)

                VStack(alignment: .leading, spacing: 8) {
                    HStack(spacing: 8) {
                        Button("Apply YAML") {
                            viewModel.requestApplySelectedResourceYAML()
                        }
                        .buttonStyle(.borderedProminent)
                        .disabled(
                            !viewModel.writeActionsEnabled
                                || viewModel.state.resourceYAML.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                        )

                        Button(yamlManifestIsEditing ? "Done" : "Edit") {
                            yamlManifestIsEditing.toggle()
                        }
                        .buttonStyle(.bordered)
                        .disabled(!viewModel.writeActionsEnabled || viewModel.state.resourceYAML.isEmpty)

                        Button("Revert") {
                            viewModel.revertResourceYAMLDraft()
                            yamlManifestIsEditing = false
                        }
                        .buttonStyle(.bordered)
                        .disabled(!viewModel.state.resourceYAMLHasUnsavedEdits)

                        Divider()
                            .frame(height: 16)

                        Button("Import…") {
                            viewModel.importResourceYAMLFromFile()
                            if viewModel.writeActionsEnabled {
                                yamlManifestIsEditing = true
                            }
                        }
                        .buttonStyle(.bordered)
                        .help("Replace the editor with the contents of a YAML file")
                    }

                    HStack(spacing: 8) {
                        Button("Export…") {
                            viewModel.saveCurrentResourceYAML()
                        }
                        .buttonStyle(.bordered)
                        .disabled(viewModel.state.resourceYAML.isEmpty)

                        Spacer(minLength: 0)
                    }
                }
            }

            Group {
                if yamlManifestIsEditing, viewModel.writeActionsEnabled {
                    TextEditor(text: yamlDraftBinding)
                        .font(.system(size: 12, weight: .regular, design: .monospaced))
                        .scrollContentBackground(.hidden)
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                        .padding(10)
                        .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                } else {
                    ScrollView([.horizontal, .vertical]) {
                        Text(yamlDisplayText)
                            .font(.system(size: 12, weight: .regular, design: .monospaced))
                            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                            .textSelection(.enabled)
                            .padding(10)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                    .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                }
            }
            .frame(minHeight: 280, maxHeight: .infinity, alignment: .topLeading)

            if viewModel.state.resourceYAML.isEmpty {
                Text(yamlFooterText)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .onChange(of: viewModel.state.resourceYAMLBaseline) { _, _ in
            yamlManifestIsEditing = false
        }
    }

    private func manifestInspectorPane(activeTab: GenericResourceManifestTab) -> some View {
        ZStack(alignment: .topLeading) {
            yamlBlock
                .opacity(activeTab == .yaml ? 1 : 0)
                .allowsHitTesting(activeTab == .yaml)
                .accessibilityHidden(activeTab != .yaml)

            describeBlock
                .opacity(activeTab == .describe ? 1 : 0)
                .allowsHitTesting(activeTab == .describe)
                .accessibilityHidden(activeTab != .describe)
        }
        .transaction { transaction in
            transaction.animation = nil
        }
    }

    /// `kubectl describe` output in memory; Save exports. No cluster write from this pane.
    private var describeBlock: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 8) {
                if viewModel.state.resourceDescribeHasUnsavedEdits {
                    Text("Unsaved edits")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.orange)
                }
                Spacer(minLength: 0)
            }

            ViewThatFits(in: .horizontal) {
                HStack(spacing: 8) {
                    Button(describePaneIsEditing ? "Done" : "Edit") {
                        describePaneIsEditing.toggle()
                    }
                    .buttonStyle(.bordered)
                    .disabled(viewModel.state.resourceDescribe.isEmpty || viewModel.state.isReadOnlyMode)
                    .help("Edit describe text locally (export only). Unavailable while read-only mode is on.")

                    Button("Revert") {
                        viewModel.revertResourceDescribeDraft()
                        describePaneIsEditing = false
                    }
                    .buttonStyle(.bordered)
                    .disabled(!viewModel.state.resourceDescribeHasUnsavedEdits)

                    Button("Save…") {
                        viewModel.saveCurrentResourceDescribe()
                    }
                    .buttonStyle(.bordered)
                    .disabled(viewModel.state.resourceDescribe.isEmpty)

                    Spacer(minLength: 0)
                }
                .fixedSize(horizontal: true, vertical: false)

                VStack(alignment: .leading, spacing: 8) {
                    HStack(spacing: 8) {
                        Button(describePaneIsEditing ? "Done" : "Edit") {
                            describePaneIsEditing.toggle()
                        }
                        .buttonStyle(.bordered)
                        .disabled(viewModel.state.resourceDescribe.isEmpty || viewModel.state.isReadOnlyMode)
                        .help("Edit describe text locally (export only). Unavailable while read-only mode is on.")

                        Button("Revert") {
                            viewModel.revertResourceDescribeDraft()
                            describePaneIsEditing = false
                        }
                        .buttonStyle(.bordered)
                        .disabled(!viewModel.state.resourceDescribeHasUnsavedEdits)
                    }
                    HStack(spacing: 8) {
                        Button("Save…") {
                            viewModel.saveCurrentResourceDescribe()
                        }
                        .buttonStyle(.bordered)
                        .disabled(viewModel.state.resourceDescribe.isEmpty)
                        Spacer(minLength: 0)
                    }
                }
            }

            Group {
                if describePaneIsEditing {
                    TextEditor(text: describeDraftBinding)
                        .font(.system(size: 12, weight: .regular, design: .monospaced))
                        .scrollContentBackground(.hidden)
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                        .padding(10)
                        .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                } else {
                    ScrollView([.horizontal, .vertical]) {
                        Text(describeDisplayText)
                            .font(.system(size: 12, weight: .regular, design: .monospaced))
                            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                            .textSelection(.enabled)
                            .padding(10)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                    .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                }
            }
            .frame(minHeight: 280, maxHeight: .infinity, alignment: .topLeading)

            Text(
                "Describe shows the resource’s current state from the cluster. Editing here does not apply changes. Use the YAML tab to modify resources."
            )
            .font(.footnote)
            .foregroundStyle(.secondary)
            .fixedSize(horizontal: false, vertical: true)
        }
        .onChange(of: viewModel.state.resourceDescribeBaseline) { _, _ in
            describePaneIsEditing = false
        }
        .onChange(of: viewModel.state.isReadOnlyMode) { _, isReadOnly in
            if isReadOnly {
                describePaneIsEditing = false
            }
        }
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
                Text(text.isEmpty ? emptyText : text)
                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .textSelection(.enabled)
                    .padding(10)
                    .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
            }
        }
    }

    private var logsToolbar: some View {
        HStack {
            Picker("Log window", selection: $viewModel.selectedLogPreset) {
                ForEach(PodLogPreset.allCases) { preset in
                    Text(preset.title).tag(preset)
                }
            }
            .frame(maxWidth: 220)

            Toggle("Previous", isOn: $viewModel.includePreviousLogs)

            Spacer()

            Button("Reload") {
                viewModel.reloadLogsForSelection()
            }

            Button("Save Logs") {
                viewModel.saveCurrentLogs()
            }
        }
    }

    // MARK: - Log panes (pod + unified workloads)

    /// Shown while kubectl log fetch is in flight (`isLoadingLogs` or global `isLoading`).
    private func logLoadingPlaceholder() -> some View {
        HStack(spacing: 10) {
            ProgressView()
                .controlSize(.small)
            Text("Loading logs…")
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
    }

    /// Error banner for failed `kubectl logs` (timeout or non-zero exit).
    private func logFetchErrorView(message: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Could not load logs")
                .font(.body.weight(.semibold))
            Text(message)
                .font(.system(size: 12, weight: .regular, design: .monospaced))
                .foregroundStyle(.red)
                .textSelection(.enabled)
                .fixedSize(horizontal: false, vertical: true)
            Button("Retry") {
                viewModel.reloadLogsForSelection()
            }
            .buttonStyle(.borderedProminent)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
    }

    private func logTextScrollContent(_ text: String) -> some View {
        Text(text)
            .font(.system(size: 12, weight: .regular, design: .monospaced))
            .frame(maxWidth: .infinity, alignment: .leading)
            .textSelection(.enabled)
            .padding(10)
    }

    /// Single-pod Logs tab: loading spinner, error, empty state, or lines.
    private func podLogsOutputScroll() -> some View {
        ScrollView {
            Group {
                if viewModel.state.isLoadingLogs || viewModel.state.isLoading {
                    logLoadingPlaceholder()
                } else if let err = viewModel.state.lastLogFetchError {
                    logFetchErrorView(message: err)
                } else if viewModel.state.podLogs.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    podLogsEmptyPlaceholder()
                } else {
                    logTextScrollContent(viewModel.state.podLogs)
                }
            }
            .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
        }
    }

    private func podLogsEmptyPlaceholder() -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("No log output")
                .font(.body.weight(.medium))
                .foregroundStyle(.secondary)
            Text("The pod may be idle, or the current filter returned no lines.")
                .font(.footnote)
                .foregroundStyle(.tertiary)
                .fixedSize(horizontal: false, vertical: true)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
    }

    /// Deployment / Service unified logs: loading, error, empty, or merged lines.
    private func unifiedLogsOutputScroll() -> some View {
        ScrollView {
            Group {
                if viewModel.state.isLoadingLogs || viewModel.state.isLoading {
                    logLoadingPlaceholder()
                } else if let err = viewModel.state.lastLogFetchError {
                    logFetchErrorView(message: err)
                } else if viewModel.state.unifiedServiceLogs.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    unifiedLogsEmptyPlaceholder()
                } else {
                    logTextScrollContent(viewModel.state.unifiedServiceLogs)
                }
            }
            .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
        }
    }

    private func unifiedLogsEmptyPlaceholder() -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("No log output")
                .font(.body.weight(.medium))
                .foregroundStyle(.secondary)
            Text("No lines were returned for the selected pods and the current filter. Pods may be idle or produce no output for this time window.")
                .font(.footnote)
                .foregroundStyle(.tertiary)
                .fixedSize(horizontal: false, vertical: true)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
    }

    private func execPane(for pod: PodSummary) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            TextField("Command", text: $viewModel.execCommandInput)
                .textFieldStyle(.roundedBorder)

            HStack(spacing: 10) {
                Button("Run in Pod") {
                    viewModel.requestExecInSelectedPod()
                }
                .disabled(viewModel.state.isExecutingCommand || !viewModel.writeActionsEnabled)

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
                        Text(execOutputText(for: result))
                            .font(.system(size: 12, weight: .regular, design: .monospaced))
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .textSelection(.enabled)
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
                            Text(execOutputText(for: result))
                                .font(.system(size: 12, weight: .regular, design: .monospaced))
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .textSelection(.enabled)
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
                                Text(namespace)
                                    .font(.caption2.weight(.semibold))
                                    .padding(.horizontal, 6)
                                    .padding(.vertical, 3)
                                    .background(Color.secondary.opacity(0.12), in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous))
                                    .foregroundStyle(.secondary)
                            }
                        }
                        .padding(.horizontal, 12)
                        .padding(.vertical, 10)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .background(
                            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous)
                                .fill(panelFill)
                        )
                        .overlay(
                            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous)
                                .fill(selection == resource ? Color.accentColor.opacity(0.12) : Color.clear)
                        )
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(.vertical, 2)
        }
        .padding(.horizontal, 2)
        .id(genericResourceListIdentity(resources))
        .transaction { transaction in
            transaction.animation = nil
        }
    }

    private func emitLayoutSnapshotIfNeeded() {
        let snapshot = RuneRootLayoutSnapshot(
            section: viewModel.state.selectedSection,
            workloadKind: viewModel.state.selectedWorkloadKind,
            measuredWindowTopInset: measuredWindowContentTopInset,
            resolvedWindowTopInset: RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: measuredWindowContentTopInset),
            contentMinY: layoutProbeFrames[.content]?.minY,
            headerMinY: layoutProbeFrames[.header]?.minY,
            detailMinY: layoutProbeFrames[.detail]?.minY
        )

        guard snapshot != lastLayoutSnapshot else { return }

        lastLayoutSnapshot = snapshot
        RuneRootLayoutDebug.log(snapshot)
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

    /// Grabber only — `NSSplitView` draws the divider; no overlay masks (they fight materials and read as clipped chrome).
    private func splitColumnTrailingChrome() -> some View {
        splitColumnSeparatorOverlay()
            .allowsHitTesting(false)
            .accessibilityHidden(true)
    }

    /// Resize affordance only — `NSSplitView` draws the actual divider (see `.docs/rune-design-plan.md` §6.2).
    private func splitColumnSeparatorOverlay() -> some View {
        GeometryReader { geo in
            let inset = RuneUILayoutMetrics.splitDividerVerticalInset
            let innerH = max(52, geo.size.height - 2 * inset)
            VStack(spacing: 0) {
                Color.clear.frame(height: inset)
                ZStack {
                    splitColumnGrabberPill()
                }
                .frame(height: innerH)
                Color.clear.frame(height: inset)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
        .frame(width: 12)
    }

    private func splitColumnGrabberPill() -> some View {
        RoundedRectangle(cornerRadius: 999, style: .continuous)
            .fill(Color.secondary.opacity(0.38))
            .frame(width: 5, height: 40)
            .overlay {
                VStack(spacing: 4) {
                    Circle().fill(Color.primary.opacity(0.22)).frame(width: 2, height: 2)
                    Circle().fill(Color.primary.opacity(0.22)).frame(width: 2, height: 2)
                    Circle().fill(Color.primary.opacity(0.22)).frame(width: 2, height: 2)
                }
            }
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
            contextUsageBadge(label: "CPU", value: contextUsageValue(viewModel.state.overviewClusterCPUPercent))
            contextUsageBadge(label: "MEM", value: contextUsageValue(viewModel.state.overviewClusterMemoryPercent))
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
        HStack(spacing: 10) {
            podSortHeaderButton(title: "Name", column: .name)
                .frame(maxWidth: .infinity, alignment: .leading)
            HStack(spacing: 10) {
                podSortHeaderButton(title: "CPU", column: .cpu, width: 44, alignment: .trailing)
                    .help("CPU in millicores (1000m = 1 core), from kubectl top.")
                podSortHeaderButton(title: "MEM", column: .memory, width: 56, alignment: .trailing)
                podSortHeaderButton(title: "Restarts", column: .restarts, width: 56, alignment: .trailing)
                podSortHeaderButton(title: "Age", column: .age, width: 44, alignment: .trailing)
            }
            podSortHeaderButton(title: "Status", column: .status, width: 120, alignment: .center)
        }
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
        value: String,
        symbol: String,
        tint: Color,
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
                Text(value)
                    .font(.title2.weight(.bold))
                Text(title)
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)
            }
            .padding(12)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
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
            .padding(14)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous)
                    .fill(Color(nsColor: .controlBackgroundColor))
            )
            .overlay(
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous)
                    .strokeBorder(Color(nsColor: .separatorColor).opacity(0.45), lineWidth: 1)
            )
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
            if deploymentScaleIsDirty(deployment), viewModel.writeActionsEnabled {
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
        .disabled(!viewModel.writeActionsEnabled || !deploymentScaleIsDirty(deployment))
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
                    .disabled(!viewModel.writeActionsEnabled)
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
                        .disabled(!viewModel.writeActionsEnabled)

                        Button("Apply YAML") {
                            viewModel.requestApplySelectedResourceYAML()
                        }
                        .buttonStyle(.bordered)
                        .disabled(!viewModel.writeActionsEnabled)

                        Spacer(minLength: 0)
                    }

                    Button("Delete", role: .destructive) {
                        viewModel.requestDeleteSelectedResource()
                    }
                    .disabled(!viewModel.writeActionsEnabled)
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
        .regularMaterial
    }

    private var editorFill: Color {
        Color(nsColor: .textBackgroundColor)
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
