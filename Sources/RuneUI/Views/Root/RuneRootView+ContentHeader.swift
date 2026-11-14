import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var contentPane: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.contentModuleSpacing) {
            if let notice = viewModel.state.activeNotice {
                RuneNoticeBanner(
                    notice: notice,
                    onOpenFolder: viewModel.openSavedLogFolder,
                    onOpenFile: viewModel.openSavedLogFile
                ) {
                    viewModel.state.clearError()
                }
                .transition(.move(edge: .top).combined(with: .opacity))
            }

            contentHeader

            Group {
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
                case .events:
                    eventsPane
                case .helm:
                    helmPane
                case .terminal:
                    terminalPane
                case .rbac:
                    rbacPane
                }
            }
            .background(RuneRootLayoutProbe(kind: .resourceTableSurface, generation: layoutGeneration))
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

    var contentHeader: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.contentControlSpacing) {
            RuneAdaptiveToolbar("Content header") {
                HStack(spacing: RuneSpacing.sm) {
                    RuneSymbolTile(
                        viewModel.state.selectedSection.symbolName,
                        tint: Color.accentColor,
                        size: RuneControlHeight.row
                    )
                    Text(viewModel.state.selectedSection.localizedTitle(appString))
                        .runeText(.title, weight: .bold)
                        .lineLimit(1)

                    // A fixed slot that fades, so loading never shifts the header.
                    ProgressView()
                        .controlSize(.small)
                        .opacity(hasAvailableKubernetesContexts && viewModel.state.isLoading ? 1 : 0)
                        .accessibilityHidden(!(hasAvailableKubernetesContexts && viewModel.state.isLoading))
                        .accessibilityLabel("Loading section")
                }
            } secondary: {
                EmptyView()
            }

            if hasAvailableKubernetesContexts {
                contentHeaderStatusStrip
            }

            if hasAvailableKubernetesContexts, showsResourceFilterControls {
                // Events has one kind, so it has no kind picker and no label in its place.
                if viewModel.state.selectedSection != .events {
                    resourceFamilyControlRow
                }
                resourceListToolbar
            } else {
                sectionSpecificControls
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(RuneRootLayoutProbe(kind: .header, generation: layoutGeneration))
    }

    var contentHeaderStatusStrip: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: RuneSpacing.xs) {
                if viewModel.isProductionContext {
                    productionBanner
                }

                if viewModel.state.isReadOnlyMode {
                    RuneHeaderCapsule(
                        "Read-only",
                        role: .status,
                        systemImage: "lock.fill",
                        tint: RuneSemanticColorRole.warning.color(in: activeAppearanceTheme.palette),
                        foregroundColor: RuneSemanticColorRole.warning.color(in: activeAppearanceTheme.palette),
                        fill: RuneSemanticColorRole.warning.color(in: activeAppearanceTheme.palette).opacity(0.16),
                        accessibilityLabel: "Mode: Read-only"
                    )
                }

                if viewModel.state.selectedSection == .overview {
                    contextUsageBadge(label: "CPU", value: contextUsageValue(viewModel.state.overviewClusterCPUPercent))
                    contextUsageBadge(label: "MEM", value: contextUsageValue(viewModel.state.overviewClusterMemoryPercent))
                }

                if let freshness = currentResourceListFreshness {
                    ResourceListFreshnessBadge(freshness: freshness)
                }

                if let counts = currentResourceListCounts {
                    RuneHeaderCapsule(
                        resourceListCountText(visible: counts.visible, total: counts.total),
                        role: .value,
                        systemImage: "list.bullet",
                        accessibilityLabel: "Resource count: \(counts.visible) visible of \(counts.total) total"
                    )
                    // Scope shows in one place: the toolbar's namespace menu (or
                    // Helm's scope menu). The strip only notes lists that the
                    // namespace menu does not apply to.
                    if let note = currentResourceListScopeNote {
                        RuneHeaderCapsule(note, role: .value, systemImage: "globe")
                    }
                }
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .accessibilityLabel("Section status")
    }

    var showsResourceFilterControls: Bool {
        switch viewModel.state.selectedSection {
        case .overview, .terminal:
            return false
        default:
            return true
        }
    }

    /// The filter fills the row up to the actions; its clear button sits
    /// inside the field, so there is only the standard gap after it.
    var resourceFilterControls: some View {
        TextField(resourceFilterPrompt, text: Binding(get: {
            viewModel.state.resourceSearchQuery
        }, set: { newValue in
            viewModel.setResourceSearchQuery(newValue)
        }))
        .textFieldStyle(RuneControlTextFieldStyle(trailingAccessoryWidth: RuneUILayoutMetrics.iconButtonSize))
        .frame(minWidth: RuneUILayoutMetrics.resourceFilterFieldMinimumWidth, maxWidth: .infinity)
        .focused($textInputFocus, equals: .resourceFilter)
        .accessibilityLabel(resourceFilterPrompt)
        .overlay(alignment: .trailing) {
            RuneIconButton(
                "Clear resource filter",
                systemImage: "xmark.circle.fill",
                isDisabled: viewModel.state.resourceSearchQuery.isEmpty
            ) {
                viewModel.setResourceSearchQuery("")
                textInputFocus = .resourceFilter
            }
            .opacity(viewModel.state.resourceSearchQuery.isEmpty ? 0 : 1)
            .padding(.trailing, RuneSpacing.xxs)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .runeHelp("Filter the visible resource table. Press slash to focus the filter.", enabled: showHoverTooltips)
    }

    var resourceFilterPrompt: String {
        switch viewModel.state.selectedSection {
        case .events:
            return "Filter events"
        case .helm:
            return effectiveHelmBrowserTab == .releases ? "Filter releases" : "Filter operator resources"
        default:
            return "Filter \(viewModel.state.selectedWorkloadKind.localizedTitle(appString))"
        }
    }

    @ViewBuilder
    var sectionSpecificControls: some View {
        switch viewModel.state.selectedSection {
        case .workloads:
            resourceKindPicker(kinds: viewModel.workloadKinds)
        case .networking:
            resourceKindPicker(kinds: viewModel.networkingKinds)
        case .config:
            resourceKindPicker(kinds: viewModel.configKinds)
        case .storage:
            resourceKindPicker(kinds: viewModel.storageKinds)
        case .rbac:
            resourceKindPicker(kinds: viewModel.rbacKinds)
        case .helm:
            helmBrowserPicker
        default:
            EmptyView()
        }
    }

    var resourceFamilyControlRow: some View {
        sectionSpecificControls
        .frame(
            maxWidth: .infinity,
            minHeight: resourceListControlRowMinimumHeight,
            alignment: .leading
        )
        .background(RuneRootLayoutProbe(kind: .resourceFamily, generation: layoutGeneration))
        .accessibilityIdentifier("resource-family-control-row")
    }

    func resourceKindPicker(kinds: [KubeResourceKind]) -> some View {
        let selection = Binding<KubeResourceKind>(get: {
            viewModel.state.selectedWorkloadKind
        }, set: { kind in
            viewModel.setWorkloadKind(kind)
        })

        return RuneTabBar("Resource type", selection: selection, tabs: kinds) { $0.localizedTitle(appString) }
        .controlSize(resourceListControlSize)
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityLabel("Resource type")
        .runeHelp("Choose which Kubernetes resource type appears in the table.", enabled: showHoverTooltips)
    }

    var helmBrowserPicker: some View {
        let selection = Binding<HelmBrowserTab>(
            get: { effectiveHelmBrowserTab },
            set: { tab in
                helmBrowserTab = tab
                viewModel.setHelmBrowserResourceFamily(tab.resourceListFamily)
            }
        )

        return RuneTabBar("Helm browser", selection: selection, tabs: HelmBrowserTab.allCases) { $0.localizedTitle(appString) }
        .controlSize(resourceListControlSize)
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityLabel("Helm browser")
    }

    var resourceListControlSize: ControlSize {
        if dynamicTypeSize.isAccessibilitySize
            || effectiveInterfaceFontSize > RuneInterfaceTypography.standardMenuFontSize + 1 {
            return .regular
        }
        return .small
    }

    var resourceListControlRowMinimumHeight: CGFloat {
        resourceListControlSize == .regular
            ? RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight
            : RuneUILayoutMetrics.resourceListToolbarMinimumHeight
    }

    var effectiveInterfaceFontSize: CGFloat {
        RuneInterfaceTypography.effectiveMenuFontSize(
            configuredFontSize: configuredAppFontSize,
            systemDynamicTypeSize: dynamicTypeSize
        )
    }

    var resourceListToolbar: some View {
        RuneResourceListToolbar {
            resourceFilterControls
                .frame(
                    maxWidth: .infinity,
                    minHeight: resourceListControlRowMinimumHeight,
                    alignment: .leading
                )
                .background(RuneRootLayoutProbe(kind: .resourceFilterRail, generation: layoutGeneration))
        } actions: {
            ZStack(alignment: .leading) {
                Color.clear
                    .frame(height: resourceListControlRowMinimumHeight)
                    .allowsHitTesting(false)
                    .accessibilityHidden(true)
                resourceListToolbarActions
            }
                .frame(
                    maxWidth: .infinity,
                    minHeight: resourceListControlRowMinimumHeight,
                    alignment: .leading
                )
                .background(RuneRootLayoutProbe(kind: .resourceActionsRail, generation: layoutGeneration))
        }
        .background(RuneRootLayoutProbe(kind: .resourceToolbar, generation: layoutGeneration))
    }

    @ViewBuilder
    var resourceListToolbarActions: some View {
        RuneToolbarOverflowRow(spacing: RuneUILayoutMetrics.contentControlSpacing) {
            resourceSortMenu
            contextualResourceListToolbarActions
        }
    }

    @ViewBuilder
    var resourceSortMenu: some View {
        switch viewModel.state.selectedSection {
        case .workloads where viewModel.state.selectedWorkloadKind == .pod:
            RuneResourceSortMenu(
                options: [(.name, "Name"), (.status, "Status"), (.age, "Age"), (.cpu, "CPU"), (.memory, "Memory"), (.restarts, "Restarts")],
                column: viewModel.podSortColumn,
                ascending: viewModel.podSortAscending,
                ascendingTitle: viewModel.podSortColumn == .age ? "Newest First" : "Ascending",
                descendingTitle: viewModel.podSortColumn == .age ? "Oldest First" : "Descending",
                onSelectColumn: viewModel.togglePodSort,
                onReverseDirection: { viewModel.togglePodSort(viewModel.podSortColumn) }
            )
        case .workloads where viewModel.state.selectedWorkloadKind == .deployment:
            RuneResourceSortMenu(
                options: [(.name, "Name"), (.replicas, "Replicas")],
                column: viewModel.deploymentSortColumn, ascending: viewModel.deploymentSortAscending,
                onSelectColumn: viewModel.toggleDeploymentSort,
                onReverseDirection: { viewModel.toggleDeploymentSort(viewModel.deploymentSortColumn) }
            )
        case .networking where viewModel.state.selectedWorkloadKind == .service:
            RuneResourceSortMenu(
                options: [(.name, "Name"), (.type, "Type"), (.clusterIP, "Cluster IP")],
                column: viewModel.serviceSortColumn, ascending: viewModel.serviceSortAscending,
                onSelectColumn: viewModel.toggleServiceSort,
                onReverseDirection: { viewModel.toggleServiceSort(viewModel.serviceSortColumn) }
            )
        case .workloads, .networking, .config, .storage, .rbac:
            let presentation = RuneGenericResourceColumnPresentation.resolve(for: viewModel.state.selectedWorkloadKind)
            RuneResourceSortMenu(
                options: [(.name, "Name"), (.namespace, "Namespace"), (.primary, presentation.primaryTitle), (.secondary, presentation.secondaryTitle)],
                column: viewModel.genericResourceSortColumn, ascending: viewModel.genericResourceSortAscending,
                onSelectColumn: viewModel.toggleGenericResourceSort,
                onReverseDirection: { viewModel.toggleGenericResourceSort(viewModel.genericResourceSortColumn) }
            )
        case .events:
            RuneResourceSortMenu(
                options: [(.lastSeen, "Last Seen"), (.reason, "Reason"), (.type, "Type"), (.object, "Object"), (.namespace, "Namespace")],
                column: viewModel.eventSortColumn, ascending: viewModel.eventSortAscending,
                ascendingTitle: viewModel.eventSortColumn == .lastSeen ? "Oldest First" : "Ascending",
                descendingTitle: viewModel.eventSortColumn == .lastSeen ? "Newest First" : "Descending",
                onSelectColumn: viewModel.toggleEventSort,
                onReverseDirection: { viewModel.toggleEventSort(viewModel.eventSortColumn) }
            )
        case .helm:
            if effectiveHelmBrowserTab == .releases {
                RuneResourceSortMenu(
                    options: [(.name, "Name"), (.namespace, "Namespace"), (.status, "Status"), (.revision, "Revision"), (.chart, "Chart"), (.appVersion, "App Version")],
                    column: viewModel.helmReleaseSortColumn, ascending: viewModel.helmReleaseSortAscending,
                    onSelectColumn: viewModel.toggleHelmReleaseSort,
                    onReverseDirection: { viewModel.toggleHelmReleaseSort(viewModel.helmReleaseSortColumn) }
                )
            } else {
                RuneResourceSortMenu(
                    options: [(.name, "Name"), (.namespace, "Namespace"), (.family, "Family"), (.kind, "Kind"), (.status, "Status"), (.apiPath, "API Path")],
                    column: viewModel.operatorResourceSortColumn, ascending: viewModel.operatorResourceSortAscending,
                    onSelectColumn: viewModel.toggleOperatorResourceSort,
                    onReverseDirection: { viewModel.toggleOperatorResourceSort(viewModel.operatorResourceSortColumn) }
                )
            }
        case .overview, .terminal:
            EmptyView()
        }
    }

    @ViewBuilder
    var contextualResourceListToolbarActions: some View {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                podBulkSelectionControls
            case .deployment:
                EmptyView()
            default:
                genericResourceBulkSelectionControls
            }
        case .networking:
            if viewModel.state.selectedWorkloadKind == .service {
                EmptyView()
            } else {
                genericResourceBulkSelectionControls
            }
        case .config, .storage, .rbac:
            genericResourceBulkSelectionControls
        case .events:
            Button {
                viewModel.saveVisibleEvents()
            } label: {
                Label("Export Events…", systemImage: "square.and.arrow.up")
            }
            .buttonStyle(RuneToolbarButtonStyle())
            .runeMinimumInteractiveTarget()
            .help("Export the events visible in the current scope")
        case .helm:
            helmResourceListToolbarActions
        case .overview, .terminal:
            EmptyView()
        }
    }

    @ViewBuilder
    var helmResourceListToolbarActions: some View {
        switch effectiveHelmBrowserTab {
        case .releases:
            helmNamespaceScopeMenu(compact: false)
        case .operatorResources:
            operatorResourceToolbarActions
        }
    }

    var operatorResourceToolbarActions: some View {
        ViewThatFits(in: .horizontal) {
            HStack(spacing: RuneUILayoutMetrics.contentControlSpacing) {
                operatorResourceFocusMenu(compact: false)
                operatorResourcePageControls
                operatorPrinterColumnsMenu(compact: false)
                helmNamespaceScopeMenu(compact: false)
            }
            .fixedSize(horizontal: true, vertical: false)

            HStack(spacing: RuneUILayoutMetrics.resourceListCompactRowSpacing) {
                operatorResourceFocusMenu(compact: true)
                operatorResourcePageControls
                operatorPrinterColumnsMenu(compact: true)
                helmNamespaceScopeMenu(compact: true)
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Operator resource controls")
    }

    func operatorResourceFocusMenu(compact: Bool) -> some View {
        RuneToolbarMenu {
            ForEach(OperatorResourceFocus.allCases) { focus in
                Button {
                    viewModel.setOperatorResourceFocus(focus)
                } label: {
                    if focus == viewModel.operatorResourceFocus {
                        Label(focus.title, systemImage: "checkmark")
                    } else {
                        Text(focus.title)
                    }
                }
            }
        } label: {
            if compact {
                Image(systemName: "scope")
            } else {
                Label("Focus: \(viewModel.operatorResourceFocus.title)", systemImage: "scope")
            }
        }
        .runeMinimumInteractiveTarget()
        .help(viewModel.operatorResourceFocusSummary)
        .accessibilityLabel("Operator resource focus")
        .accessibilityValue(viewModel.operatorResourceFocus.title)
    }

    var operatorResourcePageControls: some View {
        HStack(spacing: RuneSpacing.hairline) {
            Text(viewModel.operatorResourcePageSummary)
                .runeText(.micro, weight: .medium)
                .foregroundStyle(.runeSecondary)
                .monospacedDigit()
                .lineLimit(1)
                .fixedSize(horizontal: true, vertical: false)
                .accessibilityLabel("Operator resource page")
                .accessibilityValue(viewModel.operatorResourcePageSummary)

            RuneIconButton(
                "Previous operator resource page",
                systemImage: "chevron.left",
                isDisabled: !viewModel.canPageOperatorResourcesBackward
            ) {
                viewModel.pageOperatorResourcesBackward()
            }

            RuneIconButton(
                "Next operator resource page",
                systemImage: "chevron.right",
                isDisabled: !viewModel.canPageOperatorResourcesForward
            ) {
                viewModel.pageOperatorResourcesForward()
            }
        }
        .accessibilityElement(children: .contain)
    }

    func operatorPrinterColumnsMenu(compact: Bool) -> some View {
        let family = viewModel.operatorPrinterColumnFamilyForCustomization
        let showsColumns = viewModel.showsOperatorPrinterColumnsForCurrentFamily

        return RuneToolbarMenu {
            if let family {
                Button {
                    viewModel.toggleOperatorPrinterColumnsForCurrentFamily()
                } label: {
                    Label(
                        showsColumns ? "Hide Columns for \(family)" : "Show Columns for \(family)",
                        systemImage: showsColumns ? "eye.slash" : "eye"
                    )
                }
            } else {
                Text("No printer columns available")
            }
        } label: {
            if compact {
                Image(systemName: "tablecolumns")
            } else {
                Label("Columns", systemImage: "tablecolumns")
            }
        }
        .runeMinimumInteractiveTarget()
        .disabled(family == nil)
        .help(family.map { "Choose whether printer columns are shown for \($0)." } ?? "No printer columns are available for this page.")
        .accessibilityLabel("Operator printer columns")
        .accessibilityValue(showsColumns ? "Shown" : "Hidden")
    }

    func helmNamespaceScopeMenu(compact: Bool) -> some View {
        RuneToolbarMenu {
            Button {
                viewModel.setHelmAllNamespaces(false)
            } label: {
                if viewModel.state.isHelmAllNamespaces {
                    Text("Current namespace")
                } else {
                    Label("Current namespace", systemImage: "checkmark")
                }
            }

            Button {
                viewModel.setHelmAllNamespaces(true)
            } label: {
                if viewModel.state.isHelmAllNamespaces {
                    Label("All namespaces", systemImage: "checkmark")
                } else {
                    Text("All namespaces")
                }
            }
        } label: {
            if compact {
                Image(systemName: viewModel.state.isHelmAllNamespaces ? "globe" : "square.stack.3d.up")
            } else {
                Label(
                    viewModel.state.isHelmAllNamespaces ? "All namespaces" : "Current namespace",
                    systemImage: viewModel.state.isHelmAllNamespaces ? "globe" : "square.stack.3d.up"
                )
            }
        }
        .runeMinimumInteractiveTarget()
        .help("Choose whether Helm resources are loaded from the current namespace or every namespace.")
        .accessibilityLabel("Helm namespace scope")
        .accessibilityValue(viewModel.state.isHelmAllNamespaces ? "All namespaces" : currentResourceListScopeLabel)
    }

    var podBulkSelectionControls: some View {
        RuneBulkSelectionBar(
            selectedCount: viewModel.selectedPodCount,
            visibleCount: viewModel.visiblePods.count,
            allVisibleSelected: viewModel.areAllVisiblePodsSelectedForBulkActions,
            onToggleVisibleSelection: viewModel.toggleAllVisiblePodsForBulkActions,
            onClearSelection: viewModel.clearPodBulkSelection
        ) {
            // Always present, dimmed until rows are selected, so selecting
            // never moves the toolbar.
            Group {
                RuneToolbarMenu {
                    Button {
                        viewModel.saveSelectedPodLogsZip()
                    } label: {
                        Label("Full Logs ZIP", systemImage: "archivebox")
                    }
                    .disabled(viewModel.state.isLoadingLogs)

                    Button {
                        viewModel.saveSelectedPodLogsZipToExportFolder(openAfterSave: false)
                    } label: {
                        Label("Full Logs ZIP to Export Folder", systemImage: "folder.badge.plus")
                    }
                    .disabled(viewModel.state.isLoadingLogs)

                    Button {
                        viewModel.saveSelectedPodLogsZipToExportFolder(openAfterSave: true)
                    } label: {
                        Label("Full Logs ZIP and Open", systemImage: "archivebox")
                    }
                    .disabled(viewModel.state.isLoadingLogs)

                    Button {
                        viewModel.saveSelectedPodYAMLZip()
                    } label: {
                        Label("YAML ZIP", systemImage: "doc.zipper")
                    }
                    .disabled(viewModel.state.isLoadingResourceDetails)
                } label: {
                    Label("Export", systemImage: "square.and.arrow.up")
                }
                .runeMinimumInteractiveTarget()
                .help(viewModel.selectedPodCount == 0
                    ? "Select pods to export their logs or YAML"
                    : "Export \(viewModel.selectedPodCount) selected pod\(viewModel.selectedPodCount == 1 ? "" : "s")")
            }
            .disabled(viewModel.selectedPodCount == 0)
        }
    }

    var genericResourceBulkSelectionControls: some View {
        RuneBulkSelectionBar(
            selectedCount: viewModel.selectedGenericResourceCount,
            visibleCount: viewModel.visibleGenericResourcesForBulkActions.count,
            allVisibleSelected: viewModel.areAllVisibleGenericResourcesSelectedForBulkActions,
            onToggleVisibleSelection: viewModel.toggleAllVisibleGenericResourcesForBulkActions,
            onClearSelection: viewModel.clearGenericResourceBulkSelection
        ) {
            Group {
                RuneToolbarMenu {
                    Button {
                        didCopyGenericResourceComparison = false
                        isGenericResourceComparisonPresented = true
                    } label: {
                        Label("Compare Selected", systemImage: "rectangle.split.2x1")
                    }
                    .disabled(!viewModel.canCopySelectedGenericResourceComparison)

                    Divider()

                    Button(role: .destructive) {
                        viewModel.requestDeleteSelectedGenericResources()
                    } label: {
                        Label("Delete Selected", systemImage: "trash")
                    }
                    .disabled(!viewModel.canApplyClusterMutations)
                } label: {
                    Label("Actions", systemImage: "ellipsis.circle")
                }
                .runeMinimumInteractiveTarget()
                .help(genericSelectionActionsHelp)
                .popover(isPresented: $isGenericResourceComparisonPresented, arrowEdge: .bottom) {
                    genericResourceComparisonPopover
                }
            }
            .disabled(viewModel.selectedGenericResourceCount == 0)
        }
    }

    var genericSelectionActionsHelp: String {
        let count = viewModel.selectedGenericResourceCount
        if count < 2 {
            return "Actions for the selected resource. Select one more resource to compare."
        }
        return "Compare or delete the \(count) selected resources."
    }

    var genericResourceComparisonPopover: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.sm) {
            HStack(spacing: RuneSpacing.xs) {
                Image(systemName: "rectangle.split.2x1")
                    .foregroundStyle(.runeAccent)
                Text("Compare selected resources")
                    .runeText(.headline)
            }

            Text("Review key fields for the selected resources in one place. Copy the summary when you want to share it.")
                .runeText(.micro)
                .foregroundStyle(.runeSecondary)
                .fixedSize(horizontal: false, vertical: true)

            ScrollView([.vertical, .horizontal]) {
                Text(viewModel.selectedGenericResourceComparisonText)
                    .runeText(.code)
                    .textSelection(.enabled)
                    .fixedSize(horizontal: true, vertical: true)
                    .frame(maxWidth: .infinity, alignment: .topLeading)
                    .padding(RuneSpacing.inset)
            }
            .background(RuneSurfaceBackground(kind: .editor))

            HStack(spacing: RuneSpacing.xs) {
                if didCopyGenericResourceComparison {
                    Label("Copied", systemImage: "checkmark")
                        .runeText(.micro, weight: .medium)
                        .foregroundStyle(.runeSecondary)
                }

                Spacer()

                Button("Done") {
                    isGenericResourceComparisonPresented = false
                }
                .keyboardShortcut(.cancelAction)

                Button("Copy Summary") {
                    viewModel.copySelectedGenericResourceComparisonToClipboard()
                    didCopyGenericResourceComparison = true
                }
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
            }
        }
        .padding(RuneSpacing.md)
        .frame(width: 430, height: 340)
        .runePointerCursor()
    }
}
