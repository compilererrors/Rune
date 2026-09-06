#if DEBUG
import RuneCore
import RuneSecurity
import RuneStore
import SwiftUI

enum RuneUIComponentGalleryScenario: String, CaseIterable, Sendable {
    case compactMatrix
    case minimumWindow
    case workflowSurfaces
    case authenticationSurfaces
    case dialogSurfaces

    var requiredRegions: Set<RuneUIComponentGalleryRegion> {
        let componentRegions: Set<RuneUIComponentGalleryRegion> = [
            .root,
            .iconTargets,
            .iconRefresh,
            .iconFavorite,
            .iconDisabled,
            .capsules,
            .contextCapsule,
            .productionCapsule,
            .readOnlyCapsule,
            .contentStates,
            .adaptiveForm,
            .tabs,
            .dialog,
            .dialogClose,
            .terminal,
            .settings,
            .inspectorScaffold,
        ]

        switch self {
        case .compactMatrix:
            return componentRegions
        case .minimumWindow:
            return componentRegions.union([.sidebarPane, .contentPane, .inspectorPane])
        case .workflowSurfaces:
            return [.root, .resourceTable, .addClusterPopover, .portForwardPanel]
        case .authenticationSurfaces:
            return [.root, .directProvider, .nativeOnlyProvider]
        case .dialogSurfaces:
            return [.root, .commandPalette, .importReview]
        }
    }
}

enum RuneUIComponentGalleryRegion: String, CaseIterable, Hashable, Sendable {
    case root
    case sidebarPane
    case contentPane
    case inspectorPane
    case iconTargets
    case iconRefresh
    case iconFavorite
    case iconDisabled
    case capsules
    case contextCapsule
    case productionCapsule
    case readOnlyCapsule
    case contentStates
    case adaptiveForm
    case tabs
    case dialog
    case dialogClose
    case terminal
    case settings
    case inspectorScaffold
    case resourceTable
    case addClusterPopover
    case portForwardPanel
    case directProvider
    case nativeOnlyProvider
    case commandPalette
    case importReview
}

struct RuneUIComponentGalleryLayoutSnapshot: Equatable {
    let frames: [RuneUIComponentGalleryRegion: CGRect]

    subscript(region: RuneUIComponentGalleryRegion) -> CGRect? {
        frames[region]
    }

    func containsRequiredRegions(for scenario: RuneUIComponentGalleryScenario) -> Bool {
        scenario.requiredRegions.allSatisfy { region in
            guard let frame = frames[region] else { return false }
            return frame.width > 0
                && frame.height > 0
                && frame.width.isFinite
                && frame.height.isFinite
        }
    }
}

enum RuneUIComponentGalleryFixtures {
    static let longContext = "synthetic-context-with-a-deliberately-long-name-for-middle-truncation"
    static let longNamespace = "synthetic-namespace-with-a-long-localized-label"
    static let longResource = "synthetic-workload-with-a-deliberately-long-resource-name"
    static let longSettingTitle = "Synthetic localized preference label that wraps safely at compact width"
    static let failureMessage = "The synthetic request failed without exposing credentials or infrastructure details."
    static let emptyCommandPaletteQuery = "synthetic-query-with-no-results"

    static let refreshActionLabel = "Refresh synthetic gallery"
    static let favoriteActionLabel = "Favorite synthetic context"
    static let disabledActionLabel = "Unavailable synthetic action"
    static let closeDialogActionLabel = "Close synthetic dialog"

    static let pods = [
        PodSummary(
            name: "synthetic-api-pod-with-a-long-name",
            namespace: "synthetic-namespace",
            status: "Running"
        ),
        PodSummary(
            name: "synthetic-worker-pod",
            namespace: "synthetic-namespace",
            status: "Pending"
        ),
    ]

    static let logTabs = [
        TerminalLogTabPresentation(
            id: "synthetic-log-tab",
            podID: pods[0].id,
            title: pods[0].name,
            subtitle: pods[0].namespace,
            isFavorite: true,
            accessibilityLabel: "Synthetic API pod logs, selected and favorite",
            helpText: "Synthetic logs used only by the debug component gallery"
        )
    ]

    static let portForwardSession = PortForwardSession(
        id: "synthetic-port-forward-session",
        contextName: "synthetic-context",
        namespace: pods[0].namespace,
        targetKind: .pod,
        targetName: pods[0].name,
        localPort: 18_080,
        remotePort: 8_080,
        address: "127.0.0.1",
        status: .active,
        lastMessage: "Synthetic port-forward is active."
    )

    static let importReview = KubeConfigImportReview(
        contexts: [
            KubeConfigImportContextPreview(
                name: "synthetic-context",
                clusterName: "synthetic-cluster",
                userName: "synthetic-user",
                namespace: "synthetic-namespace",
                serverHost: "cluster.example.invalid",
                authType: "exec (redacted)",
                providerHint: "Synthetic provider"
            )
        ],
        issues: [
            KubeConfigImportIssue(
                id: "synthetic-duplicate-context",
                severity: .warning,
                message: "A synthetic context name already exists; choose how Rune should handle it."
            )
        ],
        redactedPreview: """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        contexts:
          - name: synthetic-context
        """,
        sourceName: "synthetic-kubeconfig.yaml",
        hasDuplicateConflicts: true
    )

    static let directProviderPresentation = AddClusterProviderPresentation.resolve(
        provider: .eks,
        mode: .externalCLI
    )

    static let nativeOnlyProviderPresentation = AddClusterProviderPresentation.resolve(
        provider: .eks,
        mode: .nativeOnly
    )

    @MainActor
    static func makeEmptyCommandPaletteViewModel() -> RuneAppViewModel {
        let viewModel = RuneAppViewModel(
            state: RuneAppState(),
            contextPreferences: RuneUIComponentGalleryEmptyContextPreferencesStore(),
            savedWorkspaceStore: RuneUIComponentGalleryEmptySavedWorkspaceStore(),
            overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
            namespaceListPersistence: NoopNamespaceListPersistenceStore()
        )
        viewModel.presentCommandPalette(prefilledQuery: emptyCommandPaletteQuery)
        return viewModel
    }

    static let independentActionLabels = [
        refreshActionLabel,
        favoriteActionLabel,
        disabledActionLabel,
        closeDialogActionLabel,
        "Retry synthetic request",
        "Remove Log Target Favorite",
        "Close log tab for \(pods[0].name)",
        "New Log Tab",
    ]

    static let selectedActionLabels: Set<String> = [
        favoriteActionLabel,
        "Remove Log Target Favorite",
    ]

    static let disabledActionLabels: Set<String> = [
        disabledActionLabel,
    ]
}

/// Debug-only, synthetic coverage for Rune's existing shared UI components.
/// The gallery intentionally composes production primitives instead of defining
/// a second set of visual tokens or feature-specific controls.
struct RuneUIComponentGallery: View {
    let scenario: RuneUIComponentGalleryScenario
    let onLayoutSnapshotChange: ((RuneUIComponentGalleryLayoutSnapshot) -> Void)?

    @State private var selectedInspectorTab = 0
    @State private var terminalPodSelection: String
    @State private var isSyntheticSettingEnabled = true
    @State private var syntheticSettingLimit = 128
    @State private var favoriteImportedContexts = true
    @State private var isManualTokenExpanded = false
    @State private var manualContextName = "synthetic-context"
    @State private var manualServerURL = "https://cluster.example.invalid"
    @State private var manualNamespace = "synthetic-namespace"
    @State private var manualBearerToken = "synthetic-token"

    init(
        scenario: RuneUIComponentGalleryScenario = .compactMatrix,
        onLayoutSnapshotChange: ((RuneUIComponentGalleryLayoutSnapshot) -> Void)? = nil
    ) {
        self.scenario = scenario
        self.onLayoutSnapshotChange = onLayoutSnapshotChange
        _terminalPodSelection = State(initialValue: RuneUIComponentGalleryFixtures.pods[0].id)
    }

    var body: some View {
        Group {
            switch scenario {
            case .compactMatrix:
                compactMatrix
            case .minimumWindow:
                minimumWindowMatrix
            case .workflowSurfaces:
                workflowSurfaceMatrix
            case .authenticationSurfaces:
                authenticationSurfaceMatrix
            case .dialogSurfaces:
                dialogSurfaceMatrix
            }
        }
        .coordinateSpace(name: RuneUIComponentGalleryLayout.coordinateSpaceName)
        .runeGalleryLayoutProbe(.root)
        .onPreferenceChange(RuneUIComponentGalleryFramePreferenceKey.self) { frames in
            onLayoutSnapshotChange?(RuneUIComponentGalleryLayoutSnapshot(frames: frames))
        }
        .accessibilityElement(children: .contain)
        .accessibilityIdentifier("rune.debug.component-gallery.\(scenario.rawValue)")
    }

    private var compactMatrix: some View {
        ScrollView(.vertical) {
            VStack(alignment: .leading, spacing: 12) {
                gallerySection("Status capsules") {
                    capsuleStrip
                }

                gallerySection("Buttons and independent targets") {
                    iconTargetRow
                }

                gallerySection("Loading, empty, and failure states") {
                    contentStateMatrix
                }

                gallerySection("Adaptive toolbar and form") {
                    compactAdaptiveForm
                }

                gallerySection("Scrollable tabs") {
                    inspectorTabs
                }

                gallerySection("Dialog content") {
                    dialogContent
                }

                gallerySection("Terminal compact pieces") {
                    terminalCompactContent
                }

                gallerySection("Settings adaptive row") {
                    settingsContent
                }

                gallerySection("Inspector scaffold") {
                    inspectorScaffold
                        .frame(height: 430)
                }
            }
            .padding(.vertical, 12)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .scrollContentBackground(.hidden)
        .background(RuneGlassPaneSurface(role: .content))
        .frame(minWidth: RuneInspectorScaffoldMetrics.supportedMinimumWidth)
    }

    private var minimumWindowMatrix: some View {
        HStack(spacing: 0) {
            minimumWindowSidebar
                .frame(width: RuneUILayoutMetrics.splitSidebarMinWidth)
                .runeGalleryLayoutProbe(.sidebarPane)

            RuneGlassPaneBorder(role: .sidebar)

            minimumWindowContent
                .frame(minWidth: 0, maxWidth: .infinity, maxHeight: .infinity)
                .runeGalleryLayoutProbe(.contentPane)

            RuneGlassPaneBorder(role: .inspector)

            minimumWindowInspector
                .frame(width: RuneUILayoutMetrics.splitDetailColumnMinWidth)
                .runeGalleryLayoutProbe(.inspectorPane)
        }
        .frame(
            minWidth: RuneWindowLayoutDefaults.minimumWidth,
            minHeight: RuneWindowLayoutDefaults.minimumHeight
        )
        .background(RuneGlassPaneSurface(role: .window))
    }

    private var workflowSurfaceMatrix: some View {
        ScrollView(.vertical) {
            ViewThatFits(in: .horizontal) {
                HStack(alignment: .top, spacing: 12) {
                    VStack(alignment: .leading, spacing: 12) {
                        resourceTableSurface
                        portForwardSurface
                    }
                    .frame(minWidth: 600, maxWidth: .infinity, alignment: .topLeading)

                    addClusterSurface
                        .frame(width: RuneUILayoutMetrics.addClusterPopoverWidth)
                }

                VStack(alignment: .leading, spacing: 12) {
                    resourceTableSurface
                    portForwardSurface
                    addClusterSurface
                }
            }
            .padding(RuneUILayoutMetrics.paneOuterPadding)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .scrollContentBackground(.hidden)
        .background(RuneGlassPaneSurface(role: .content))
        .frame(minWidth: 440)
    }

    private var resourceTableSurface: some View {
        gallerySection("AppKit resource table") {
            AppKitPodTableView(
                pods: RuneUIComponentGalleryFixtures.pods,
                selectedPodID: RuneUIComponentGalleryFixtures.pods[0].id,
                selectedPodIDs: [RuneUIComponentGalleryFixtures.pods[0].id],
                sortColumn: .name,
                sortAscending: true,
                nameColumnWidth: 190,
                canApplyClusterMutations: false,
                isFavorite: { $0.id == RuneUIComponentGalleryFixtures.pods[0].id },
                onSelectPod: { _ in },
                onToggleBulkSelection: { _ in },
                onToggleSort: { _ in },
                onNameColumnWidthChanged: { _ in },
                onToggleFavorite: { _ in },
                onOpenLogs: { _ in },
                onOpenExec: { _ in },
                onOpenDescribe: { _ in },
                onOpenYAML: { _ in },
                onDelete: { _ in }
            )
            .frame(maxWidth: .infinity, minHeight: 150, idealHeight: 180, maxHeight: 180)
            .runeGalleryLayoutProbe(.resourceTable)
        }
    }

    private var portForwardSurface: some View {
        gallerySection("Port Forward panel") {
            RuneUIPortForwardGallerySurface()
                .runeGalleryLayoutProbe(.portForwardPanel)
        }
    }

    private var addClusterSurface: some View {
        gallerySection("Add Cluster popover") {
            AddClusterPopoverView(
                kubeConfigSourceCount: 2,
                contextCount: 4,
                isLoading: false,
                externalCommandsAllowed: false,
                favoriteImportedContexts: $favoriteImportedContexts,
                isManualTokenExpanded: $isManualTokenExpanded,
                manualContextName: $manualContextName,
                manualServerURL: $manualServerURL,
                manualNamespace: $manualNamespace,
                manualBearerToken: $manualBearerToken,
                onRefresh: {},
                onImportFile: {},
                onPasteKubeconfig: {},
                onImportFolder: {},
                onUseDefaultKubeconfig: {},
                onSelectProvider: { _ in },
                onImportManualToken: {}
            )
            .runeGalleryLayoutProbe(.addClusterPopover)
        }
    }

    private var authenticationSurfaceMatrix: some View {
        ScrollView(.vertical) {
            ViewThatFits(in: .horizontal) {
                HStack(alignment: .top, spacing: 12) {
                    directProviderSurface
                    nativeOnlyProviderSurface
                }

                VStack(alignment: .leading, spacing: 12) {
                    directProviderSurface
                    nativeOnlyProviderSurface
                }
            }
            .padding(RuneUILayoutMetrics.paneOuterPadding)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .scrollContentBackground(.hidden)
        .background(RuneGlassPaneSurface(role: .content))
    }

    private var directProviderSurface: some View {
        gallerySection("Direct build provider") {
            RuneUIProviderPresentationGallerySurface(
                presentation: RuneUIComponentGalleryFixtures.directProviderPresentation,
                modeTitle: "Direct build"
            )
            .runeGalleryLayoutProbe(.directProvider)
        }
        .frame(minWidth: AddClusterProviderCredentialFieldMetrics.supportedCompactWidth, maxWidth: .infinity)
    }

    private var nativeOnlyProviderSurface: some View {
        gallerySection("App Store provider") {
            RuneUIProviderPresentationGallerySurface(
                presentation: RuneUIComponentGalleryFixtures.nativeOnlyProviderPresentation,
                modeTitle: "Native-only build"
            )
            .runeGalleryLayoutProbe(.nativeOnlyProvider)
        }
        .frame(minWidth: AddClusterProviderCredentialFieldMetrics.supportedCompactWidth, maxWidth: .infinity)
    }

    private var dialogSurfaceMatrix: some View {
        ScrollView(.vertical) {
            ViewThatFits(in: .horizontal) {
                HStack(alignment: .top, spacing: 12) {
                    commandPaletteSurface
                    importReviewSurface
                }

                VStack(alignment: .leading, spacing: 12) {
                    commandPaletteSurface
                    importReviewSurface
                }
            }
            .padding(RuneUILayoutMetrics.paneOuterPadding)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .scrollContentBackground(.hidden)
        .background(RuneGlassPaneSurface(role: .content))
    }

    private var commandPaletteSurface: some View {
        gallerySection("Empty Command Palette") {
            RuneUICommandPaletteGallerySurface()
                .frame(
                    width: RuneUILayoutMetrics.commandPaletteIdealWidth,
                    height: RuneUILayoutMetrics.commandPaletteIdealHeight
                )
                .runeGalleryLayoutProbe(.commandPalette)
        }
    }

    private var importReviewSurface: some View {
        gallerySection("Kubeconfig Import Review") {
            RuneUIImportReviewGallerySurface()
                .frame(width: 620, height: RuneUILayoutMetrics.commandPaletteIdealHeight, alignment: .top)
                .runeGalleryLayoutProbe(.importReview)
        }
    }

    private var minimumWindowSidebar: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Rune UI Gallery")
                .font(.headline)

            Text("Synthetic debug fixtures")
                .font(.caption)
                .foregroundStyle(.runeSecondary)

            Divider()

            VStack(alignment: .leading, spacing: 3) {
                sidebarRow("Overview", systemImage: "square.grid.2x2", shortcut: "⌘1", selected: true)
                sidebarRow("Workloads", systemImage: "shippingbox", shortcut: "⌘2")
                sidebarRow("Networking", systemImage: "point.3.connected.trianglepath.dotted", shortcut: "⌘3")
                sidebarRow("Settings", systemImage: "gearshape", shortcut: "⌘,")
            }

            Divider()

            Text(RuneUIComponentGalleryFixtures.longContext)
                .font(.caption.weight(.semibold))
                .lineLimit(2)
                .truncationMode(.middle)

            Text(RuneUIComponentGalleryFixtures.longNamespace)
                .font(.caption)
                .foregroundStyle(.runeSecondary)
                .lineLimit(2)
                .truncationMode(.middle)

            Spacer(minLength: 8)

            Text("Debug-only · synthetic data")
                .font(.caption2)
                .foregroundStyle(.runeSecondary)
        }
        .padding(RuneUILayoutMetrics.sidebarPadding)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(RuneGlassPaneSurface(role: .sidebar))
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Synthetic gallery sidebar")
    }

    private var minimumWindowContent: some View {
        ScrollView(.vertical) {
            VStack(alignment: .leading, spacing: 12) {
                capsuleStrip
                iconTargetRow
                contentStateMatrix
                compactAdaptiveForm
                inspectorTabs
                terminalCompactContent
                settingsContent
                dialogContent
            }
            .padding(RuneUILayoutMetrics.paneOuterPadding)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .scrollContentBackground(.hidden)
        .background(RuneGlassPaneSurface(role: .content))
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Synthetic gallery content")
    }

    private var minimumWindowInspector: some View {
        inspectorScaffold
            .padding(RuneUILayoutMetrics.paneInnerPadding)
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .background(RuneGlassPaneSurface(role: .inspector))
            .accessibilityElement(children: .contain)
            .accessibilityLabel("Synthetic gallery inspector")
    }

    private var capsuleStrip: some View {
        ScrollView(.horizontal, showsIndicators: true) {
            HStack(spacing: 8) {
                RuneHeaderCapsule(
                    RuneUIComponentGalleryFixtures.longContext,
                    role: .context,
                    systemImage: "network",
                    accessibilityLabel: "Synthetic context"
                )
                .frame(maxWidth: 230)
                .runeGalleryLayoutProbe(.contextCapsule)

                RuneHeaderCapsule(
                    "Production",
                    role: .status,
                    indicatorColor: .red,
                    tint: .red,
                    foregroundColor: .red,
                    fill: Color.red.opacity(0.12),
                    accessibilityLabel: "Synthetic production state"
                )
                .runeGalleryLayoutProbe(.productionCapsule)

                RuneHeaderCapsule(
                    "Read-only",
                    role: .status,
                    systemImage: "lock.fill",
                    tint: .orange,
                    foregroundColor: .orange,
                    fill: Color.orange.opacity(0.12),
                    accessibilityLabel: "Synthetic read-only state"
                )
                .runeGalleryLayoutProbe(.readOnlyCapsule)
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Synthetic context and safety states")
        .runeGalleryLayoutProbe(.capsules)
    }

    private var iconTargetRow: some View {
        RuneAdaptiveToolbar("Synthetic independent actions", compactBehavior: .horizontalScroll) {
            HStack(spacing: 8) {
                RuneIconButton(
                    RuneUIComponentGalleryFixtures.refreshActionLabel,
                    systemImage: "arrow.clockwise",
                    action: {}
                )
                .runeGalleryLayoutProbe(.iconRefresh)

                RuneIconButton(
                    RuneUIComponentGalleryFixtures.favoriteActionLabel,
                    systemImage: "star.fill",
                    isSelected: true,
                    selectedTint: .yellow,
                    action: {}
                )
                .runeGalleryLayoutProbe(.iconFavorite)

                RuneIconButton(
                    RuneUIComponentGalleryFixtures.disabledActionLabel,
                    systemImage: "bolt.slash",
                    isDisabled: true,
                    action: {}
                )
                .runeGalleryLayoutProbe(.iconDisabled)
            }
        } secondary: {
            Button("Primary Action") {}
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                .disabled(true)
                .help("Disabled while synthetic read-only mode is active")
        }
        .runeGalleryLayoutProbe(.iconTargets)
    }

    private var contentStateMatrix: some View {
        VStack(alignment: .leading, spacing: 8) {
            RuneContentStateView(
                .loading(
                    title: "Loading synthetic resources",
                    message: "Waiting for a deterministic debug response."
                ),
                variant: .inline
            )

            RuneContentStateView(
                .filteredEmpty(
                    title: "No synthetic matches",
                    message: "Clear the long filter to show all fixtures."
                ),
                variant: .inline,
                action: RuneContentStateAction("Clear Filter", systemImage: "xmark.circle", perform: {})
            )

            RuneContentStateView(
                .empty(
                    title: "No synthetic events",
                    message: "New demo events will appear here."
                ),
                variant: .inline
            )

            RuneContentStateView(
                .retryableError(
                    title: "Synthetic request failed",
                    message: RuneUIComponentGalleryFixtures.failureMessage
                ),
                variant: .card,
                action: RuneContentStateAction(
                    "Retry synthetic request",
                    systemImage: "arrow.clockwise",
                    perform: {}
                )
            )
        }
        .accessibilityElement(children: .contain)
        .runeGalleryLayoutProbe(.contentStates)
    }

    private var compactAdaptiveForm: some View {
        RuneAdaptiveToolbar("Synthetic namespace form") {
            VStack(alignment: .leading, spacing: 4) {
                Text("Namespace")
                    .font(.caption.weight(.semibold))
                TextField("Required", text: .constant(RuneUIComponentGalleryFixtures.longNamespace))
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 210)
                    .accessibilityLabel("Synthetic namespace, required")
            }
        } secondary: {
            VStack(alignment: .leading, spacing: 4) {
                Text("Actions")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.runeSecondary)
                HStack(spacing: 8) {
                    Button("Validate") {}
                    Button("Apply") {}
                        .disabled(true)
                }
                .buttonStyle(RuneToolbarButtonStyle())
            }
        }
        .runeInsetCard(padding: 10)
        .runeGalleryLayoutProbe(.adaptiveForm)
    }

    private var inspectorTabs: some View {
        RuneSegmentedPickerInScroll(
            "Synthetic inspector tabs",
            selection: $selectedInspectorTab,
            labelsHidden: true
        ) {
            Text("Overview").tag(0)
            Text("Manifest with long localized label").tag(1)
            Text("Describe").tag(2)
            Text("Events").tag(3)
        }
        .runeGalleryLayoutProbe(.tabs)
    }

    private var dialogContent: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
            HStack(alignment: .center, spacing: 8) {
                VStack(alignment: .leading, spacing: 3) {
                    Text("Synthetic Authentication")
                        .font(.headline)
                    Text("Persistent labels and bounded output")
                        .font(.caption)
                        .foregroundStyle(.runeSecondary)
                }

                Spacer(minLength: 8)

                RuneDialogCloseButton(RuneUIComponentGalleryFixtures.closeDialogActionLabel, action: {})
                    .runeGalleryLayoutProbe(.dialogClose)
            }

            VStack(alignment: .leading, spacing: 4) {
                Text("Context name · Required")
                    .font(.caption.weight(.semibold))
                TextField("Context name", text: .constant(RuneUIComponentGalleryFixtures.longContext))
                    .textFieldStyle(.roundedBorder)
                    .accessibilityLabel("Synthetic context name, required")
            }

            RuneContentStateView(
                .unselected(
                    title: "No provider selected",
                    message: "Choose a synthetic provider to inspect the bounded diagnostic state."
                ),
                variant: .inline
            )

            RuneDialogActionBar {
                Button(action: {}) {
                    RuneDialogButtonLabel("Cancel")
                }
                Button(action: {}) {
                    RuneDialogButtonLabel("Continue")
                }
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
            }
        }
        .padding(RuneUILayoutMetrics.dialogContentPadding)
        .background(RuneSurfaceBackground(kind: .panel))
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Synthetic dialog content")
        .runeGalleryLayoutProbe(.dialog)
    }

    private var terminalCompactContent: some View {
        VStack(alignment: .leading, spacing: 8) {
            TerminalPodSelectorRow(
                title: "Pod",
                systemImage: "shippingbox",
                pods: RuneUIComponentGalleryFixtures.pods,
                actionTitle: "Connect",
                actionSystemImage: "terminal",
                isActionDisabled: true,
                selection: $terminalPodSelection
            )

            TerminalLogTabBar(
                tabs: RuneUIComponentGalleryFixtures.logTabs,
                activeTabID: RuneUIComponentGalleryFixtures.logTabs[0].id,
                canAddTab: true,
                onSelectTab: { _ in },
                onCloseTab: { _ in },
                onToggleFavoriteTab: { _ in },
                onAddTab: {}
            )
            .accessibilityHint("Log selection, favorite, close, and add remain independent actions")
        }
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Synthetic compact terminal controls")
        .runeGalleryLayoutProbe(.terminal)
    }

    private var settingsContent: some View {
        VStack(alignment: .leading, spacing: 10) {
            RuneSettingsAdaptiveRow {
                VStack(alignment: .leading, spacing: 3) {
                    Text(RuneUIComponentGalleryFixtures.longSettingTitle)
                        .runeInterfaceFont(weight: .semibold)
                    Text("The explanatory detail remains visible when the control moves below the label.")
                        .runeInterfaceFont(relativeSize: -1)
                        .foregroundStyle(.runeSecondary)
                }
            } control: {
                Toggle("Enabled", isOn: $isSyntheticSettingEnabled)
                    .toggleStyle(.switch)
            }

            Divider()

            RuneSettingsAdaptiveRow {
                VStack(alignment: .leading, spacing: 3) {
                    Text("Synthetic managed files")
                        .runeInterfaceFont(weight: .semibold)
                    Text("Primary and follow-up actions keep a visible, compact hierarchy.")
                        .runeInterfaceFont(relativeSize: -1)
                        .foregroundStyle(.runeSecondary)
                }
            } control: {
                VStack(alignment: .leading, spacing: 6) {
                    HStack(spacing: 8) {
                        Button {
                        } label: {
                            Label("New File", systemImage: "doc.badge.plus")
                        }
                        .accessibilityLabel("Create template file")
                        .frame(maxWidth: .infinity)

                        Button {
                        } label: {
                            Label("Open Folder", systemImage: "folder")
                        }
                        .frame(maxWidth: .infinity)
                    }

                    HStack(spacing: 8) {
                        Button {
                        } label: {
                            Label("Reload", systemImage: "arrow.clockwise")
                        }
                        Spacer(minLength: 8)
                        RuneIconButton(
                            "Synthetic settings help",
                            systemImage: "questionmark.circle",
                            action: {}
                        )
                    }
                }
                .buttonStyle(RuneToolbarButtonStyle())
            }

            Divider()

            RuneSettingsIntegerLimitEditor(
                title: "Synthetic cache",
                value: $syntheticSettingLimit,
                valueSuffix: "items",
                step: 16,
                placeholder: "128",
                defaultValue: 128,
                detail: "Value, unit, stepper, and reset stay together in the control rail.",
                normalize: { max(16, $0) }
            )
        }
        .runeInsetCard(padding: RuneSettingsMetrics.sectionCardPadding)
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Synthetic Settings adaptive controls")
        .runeGalleryLayoutProbe(.settings)
    }

    private var inspectorScaffold: some View {
        RuneInspectorScaffold(
            title: RuneUIComponentGalleryFixtures.longResource,
            copyAccessibilityLabel: "Copy synthetic resource name",
            bodyScrollBehavior: .vertical,
            onCopy: {},
            onRefresh: {},
            info: {
                VStack(alignment: .leading, spacing: 8) {
                    RuneInspectorInfoRow("Namespace", systemImage: "square.stack.3d.up") {
                        Text(RuneUIComponentGalleryFixtures.longNamespace)
                            .lineLimit(2)
                            .truncationMode(.middle)
                    }
                    RuneInspectorInfoRow("Status", systemImage: "checkmark.circle") {
                        Text("Synthetic Ready")
                    }
                }
            },
            tabs: {
                RuneSegmentedPickerInScroll(
                    "Synthetic inspector tabs",
                    selection: $selectedInspectorTab,
                    labelsHidden: true
                ) {
                    Text("Overview").tag(0)
                    Text("Manifest with long localized label").tag(1)
                    Text("Describe").tag(2)
                    Text("Events").tag(3)
                }
            },
            actions: {
                Button("Inspect") {}
                Button("Apply") {}
                    .disabled(true)
                Button("Delete", role: .destructive) {}
                    .disabled(true)
            },
            content: {
                VStack(alignment: .leading, spacing: 10) {
                    RuneHeaderCapsule(
                        "Read-only production preview",
                        role: .status,
                        systemImage: "lock.shield",
                        tint: .orange,
                        accessibilityLabel: "Synthetic inspector safety state"
                    )

                    RuneContentStateView(
                        .empty(
                            title: "No synthetic relationships",
                            message: "Related resources will appear here after a deterministic refresh."
                        ),
                        variant: .inline
                    )

                    ForEach(1...8, id: \.self) { index in
                        RuneInspectorInfoRow("Field \(index)", systemImage: "circle.fill") {
                            Text("Synthetic value \(index)")
                        }
                    }
                }
            }
        )
        .runeGalleryLayoutProbe(.inspectorScaffold)
    }

    private func sidebarRow(
        _ title: String,
        systemImage: String,
        shortcut: String,
        selected: Bool = false
    ) -> some View {
        HStack(spacing: 8) {
            Image(systemName: systemImage)
                .frame(width: 16)
            Text(title)
            Spacer(minLength: 8)
            Text(shortcut)
                .font(.caption.monospaced())
                .foregroundStyle(.runeSecondary)
        }
        .font(.subheadline.weight(selected ? .semibold : .regular))
        .padding(.horizontal, 8)
        .frame(minHeight: 30)
        .runeSidebarSelection(isSelected: selected)
        .accessibilityElement(children: .combine)
    }

    private func gallerySection<Content: View>(
        _ title: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.runeSecondary)
            content()
        }
        .runeInsetCard(padding: 10)
    }
}

/// Stateful debug harness around the production Port Forward panel. All
/// identifiers are synthetic and callbacks are deliberately side-effect free.
private struct RuneUIPortForwardGallerySurface: View {
    @State private var isExpanded = true
    @State private var selectedPodID = RuneUIComponentGalleryFixtures.pods[0].id
    @State private var localPort = "18080"
    @State private var remotePort = "8080"
    @State private var address = "127.0.0.1"

    var body: some View {
        TerminalPortForwardPanelView(
            isExpanded: $isExpanded,
            contextName: "synthetic-context",
            selectedPod: RuneUIComponentGalleryFixtures.pods[0],
            availablePods: RuneUIComponentGalleryFixtures.pods,
            portForwardSessions: [RuneUIComponentGalleryFixtures.portForwardSession],
            canApplyMutations: true,
            selectedPortForwardPodID: $selectedPodID,
            localPort: $localPort,
            remotePort: $remotePort,
            address: $address,
            isFavoritePod: { $0.id == RuneUIComponentGalleryFixtures.pods[0].id },
            onToggleFavoritePod: { _ in },
            onStartPortForward: { _ in },
            onStopPortForward: { _ in },
            onOpenPortForwardInBrowser: { _ in },
            onRetryPortForward: { _ in },
            onClearPortForward: { _ in },
            onClearInactivePortForwards: {}
        )
    }
}

/// Exercises the provider sheet's real presentation model and credential-field
/// renderer without copying provider rules into the debug gallery.
private struct RuneUIProviderPresentationGallerySurface: View {
    let presentation: AddClusterProviderPresentation
    let modeTitle: String

    var body: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
            HStack(alignment: .top, spacing: 10) {
                Image(systemName: presentation.symbolName)
                    .font(.title2.weight(.semibold))
                    .foregroundStyle(presentation.provider.accent)
                    .frame(width: 32, height: 32)

                VStack(alignment: .leading, spacing: 3) {
                    Text(presentation.title)
                        .font(.headline)
                    Text(presentation.subtitle)
                        .font(.caption)
                        .foregroundStyle(.runeSecondary)
                }

                Spacer(minLength: 8)

                RuneHeaderCapsule(
                    modeTitle,
                    role: .status,
                    systemImage: presentation.executionMode == .externalCLI ? "terminal" : "key.fill",
                    accessibilityLabel: "Provider execution mode: \(modeTitle)"
                )
            }

            Text(presentation.note)
                .font(.footnote)
                .foregroundStyle(.runeSecondary)
                .fixedSize(horizontal: false, vertical: true)

            VStack(alignment: .leading, spacing: 10) {
                ForEach(presentation.fields) { field in
                    providerField(field)
                }
            }

            RuneDialogActionBar {
                Button(action: {}) {
                    Label(
                        presentation.primaryAction.title,
                        systemImage: presentation.primaryAction.systemImage
                    )
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
                }
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))

                ForEach(presentation.utilityActions.prefix(2)) { action in
                    Button(action: {}) {
                        Label(action.title, systemImage: action.systemImage)
                            .frame(
                                minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                                minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                            )
                    }
                    .buttonStyle(RuneToolbarButtonStyle())
                }
            }
        }
        .padding(RuneUILayoutMetrics.dialogContentPadding)
        .background(RuneSurfaceBackground(kind: .panel))
        .accessibilityElement(children: .contain)
        .accessibilityLabel("\(presentation.title), \(modeTitle)")
    }

    @ViewBuilder
    private func providerField(_ field: AddClusterProviderField) -> some View {
        if field.input == .sensitiveJSONFile {
            AddClusterProviderCredentialField(field: field) {
                Button("Choose JSON…", action: {})
                    .buttonStyle(RuneToolbarButtonStyle())
            }
        } else {
            AddClusterProviderCredentialTextInput(field: field, text: .constant(""))
        }
    }
}

private struct RuneUIImportReviewGallerySurface: View {
    @State private var duplicateHandlingChoice: KubeConfigDuplicateHandlingChoice = .skipDuplicate

    var body: some View {
        KubeConfigImportReviewPanel(
            review: RuneUIComponentGalleryFixtures.importReview,
            duplicateHandlingChoice: $duplicateHandlingChoice,
            metadataDrafts: [
                "synthetic-context": ContextDisplayMetadata(
                    alias: "Synthetic context",
                    colorKey: "blue",
                    iconName: "network",
                    tags: ["synthetic"],
                    group: "Gallery"
                )
            ],
            onUpdateMetadata: { _, _ in },
            reviewMode: .preflight,
            isConfirmationPending: true,
            canConfirm: true,
            isCommitInProgress: false,
            onConfirm: {},
            onCancel: {},
            onClear: {},
            showsAuthDoctorAction: false,
            onRunAuthDoctor: {}
        )
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    }
}

@MainActor
private struct RuneUICommandPaletteGallerySurface: View {
    @StateObject private var viewModel: RuneAppViewModel

    init() {
        _viewModel = StateObject(
            wrappedValue: RuneUIComponentGalleryFixtures.makeEmptyCommandPaletteViewModel()
        )
    }

    var body: some View {
        CommandPaletteView(viewModel: viewModel)
    }
}

private struct RuneUIComponentGalleryEmptyContextPreferencesStore: ContextPreferencesStoring {
    func loadFavoriteContextNames() -> Set<String> { [] }
    func saveFavoriteContextNames(_ names: Set<String>) {}
}

private struct RuneUIComponentGalleryEmptySavedWorkspaceStore: SavedWorkspaceStoring {
    func loadSavedWorkspaces() -> [SavedWorkspaceSnapshot] { [] }
    func saveSavedWorkspaces(_ workspaces: [SavedWorkspaceSnapshot]) {}
}

private enum RuneUIComponentGalleryLayout {
    static let coordinateSpaceName = "RuneUIComponentGalleryLayoutSpace"
}

private struct RuneUIComponentGalleryFramePreferenceKey: PreferenceKey {
    static let defaultValue: [RuneUIComponentGalleryRegion: CGRect] = [:]

    static func reduce(
        value: inout [RuneUIComponentGalleryRegion: CGRect],
        nextValue: () -> [RuneUIComponentGalleryRegion: CGRect]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private extension View {
    func runeGalleryLayoutProbe(_ region: RuneUIComponentGalleryRegion) -> some View {
        overlay {
            GeometryReader { proxy in
                Color.clear.preference(
                    key: RuneUIComponentGalleryFramePreferenceKey.self,
                    value: [
                        region: proxy.frame(
                            in: .named(RuneUIComponentGalleryLayout.coordinateSpaceName)
                        )
                    ]
                )
            }
            .allowsHitTesting(false)
            .accessibilityHidden(true)
        }
    }
}

private struct RuneUIComponentGalleryPreviews: PreviewProvider {
    static var previews: some View {
        Group {
            RuneUIComponentGallery(scenario: .compactMatrix)
                .frame(width: 320, height: 1_200)
                .previewDisplayName("Compact 320")

            RuneUIComponentGallery(scenario: .minimumWindow)
                .frame(width: 980, height: 640)
                .previewDisplayName("Minimum Window 980 × 640")

            RuneUIComponentGallery(scenario: .workflowSurfaces)
                .frame(width: 1_360, height: 820)
                .previewDisplayName("Workflows 1360 × 820")

            RuneUIComponentGallery(scenario: .authenticationSurfaces)
                .dynamicTypeSize(.accessibility2)
                .preferredColorScheme(.dark)
                .frame(width: 1_360, height: 820)
                .previewDisplayName("Provider Modes · Enlarged Dark")

            RuneUIComponentGallery(scenario: .dialogSurfaces)
                .runeAppearanceTheme(RuneAppearanceTheme.fjord.resolvedTheme)
                .frame(width: 1_680, height: 900)
                .previewDisplayName("Dialogs · Fjord Wide")
        }
    }
}
#endif
