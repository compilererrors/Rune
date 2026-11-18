import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    var terminalPane: some View {
        ResourceTerminalWorkspaceView(
            session: viewModel.state.terminalSession,
            sessions: viewModel.state.terminalSessions,
            activeSessionID: viewModel.state.activeTerminalSessionID,
            contextName: viewModel.state.selectedContext?.name,
            namespace: viewModel.state.selectedNamespace,
            selectedPod: viewModel.state.selectedPod,
            availablePods: viewModel.state.pods,
            portForwardSessions: viewModel.state.portForwardSessions,
            canApplyMutations: viewModel.canApplyClusterMutations,
            selectedShellPodID: $terminalShellPodID,
            selectedPortForwardPodID: $terminalPortForwardPodID,
            terminalInput: $viewModel.terminalSessionInput,
            portForwardLocalPort: $viewModel.portForwardLocalPortInput,
            portForwardRemotePort: $viewModel.portForwardRemotePortInput,
            portForwardAddress: $viewModel.portForwardAddressInput,
            onStartSession: { pod, containerName in viewModel.startTerminalSession(for: pod, container: containerName) },
            onReconnectSession: { session, pod, containerName in
                viewModel.startTerminalSession(for: pod, container: containerName, replacingSessionID: session.id)
            },
            onStartPortForward: { pod in
                viewModel.startPortForward(targetKind: .pod, targetName: pod.name)
            },
            onStopPortForward: { session in
                viewModel.stopPortForward(session)
            },
            onOpenPortForwardInBrowser: { session in
                viewModel.openPortForwardInBrowser(session)
            },
            onRetryPortForward: { session in
                viewModel.retryPortForward(session)
            },
            onClearPortForward: { session in
                viewModel.clearPortForwardSession(session)
            },
            onClearInactivePortForwards: {
                viewModel.clearInactivePortForwardSessions()
            },
            onSend: { viewModel.sendTerminalSessionInput() },
            onSendControlSequence: { text in viewModel.sendTerminalControlSequence(text) },
            onResizeSession: { id, columns, rows in viewModel.resizeTerminalSession(id: id, columns: columns, rows: rows) },
            onDisconnect: { viewModel.stopTerminalSession() },
            onSelectSession: { id in viewModel.selectTerminalSession(id: id) },
            onCloseSession: { id in viewModel.closeTerminalSession(id: id) },
            onMoveSession: { id, targetID in viewModel.moveTerminalSession(id: id, onto: targetID) },
            onClearTranscript: { viewModel.clearTerminalSessionTranscript() },
            onSaveActiveTerminalTranscript: { viewModel.saveActiveTerminalTranscript() },
            onSaveAllTerminalTranscripts: { viewModel.saveAllTerminalTranscriptsZip() },
            onSaveActiveTerminalTranscriptToExportFolder: { viewModel.saveActiveTerminalTranscriptToExportFolder(openAfterSave: false) },
            onSaveActiveTerminalTranscriptAndOpen: { viewModel.saveActiveTerminalTranscriptToExportFolder(openAfterSave: true) },
            onSaveAllTerminalTranscriptsToExportFolder: { viewModel.saveAllTerminalTranscriptsZipToExportFolder(openAfterSave: false) },
            onSaveAllTerminalTranscriptsAndOpen: { viewModel.saveAllTerminalTranscriptsZipToExportFolder(openAfterSave: true) },
            isFavoritePod: isFavoritePod,
            onToggleFavoritePod: toggleFavoritePod,
            onOpenSelectedPodLogs: openTerminalLogs,
            autoConnectsShell: TerminalShellConnectPolicy.autoConnects(
                isReadOnly: viewModel.state.isReadOnlyMode,
                isProduction: viewModel.isProductionContext,
                canApplyMutations: viewModel.canApplyClusterMutations
            ),
            composingNewShellTab: $isComposingTerminalShellTab
        )
        .id("terminal")
        .onAppear(perform: reconcileTerminalPodSelections)
        .onChange(of: viewModel.state.pods.map(\.id)) { _, _ in
            reconcileTerminalPodSelections()
        }
        .onChange(of: viewModel.state.selectedPod?.id) { _, _ in
            reconcileTerminalPodSelections()
        }
    }

    func reconcileTerminalPodSelections() {
        let availableIDs = Set(viewModel.state.pods.map(\.id))
        let fallbackID = viewModel.state.selectedPod?.id ?? viewModel.state.pods.first?.id ?? ""

        if terminalShellPodID.isEmpty || !availableIDs.contains(terminalShellPodID) {
            terminalShellPodID = fallbackID
        }
        if terminalPortForwardPodID.isEmpty || !availableIDs.contains(terminalPortForwardPodID) {
            terminalPortForwardPodID = fallbackID
        }
        if terminalLogTabState.selectedPodID.isEmpty || !availableIDs.contains(terminalLogTabState.selectedPodID) {
            terminalLogTabState.selectedPodID = fallbackID
        }
        reconcileTerminalLogTabs()
    }

    /// The inspector names what it shows: the session's target pod, in the
    /// same header block as every other inspector (name, "Pod · namespace",
    /// status pill, copy, refresh).
    var terminalDetails: some View {
        Group {
            if let pod = terminalInspectorPod {
                RuneInspectorScaffold(
                    title: pod.name,
                    subtitle: inspectorSubtitle("Pod", namespace: pod.namespace),
                    status: RuneInspectorStatus(pod.displayStatus),
                    copyAccessibilityLabel: "Copy pod name",
                    bodyScrollBehavior: .selfManaged,
                    // The logs hand-off lives in the log viewer's More menu.
                    showsActions: false,
                    onCopy: { copyToClipboard(pod.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        EmptyView()
                    },
                    tabs: {
                        RuneTabBar("", selection: $terminalInspectorTab, tabs: TerminalInspectorTab.allCases) { $0.localizedTitle(appString) }
                            .accessibilityLabel("Terminal Inspector")
                    },
                    actions: {
                        EmptyView()
                    },
                    content: {
                        Group {
                            switch terminalInspectorTab {
                            case .commands:
                                ResourceTerminalDetailsView(
                                    session: viewModel.state.terminalSession,
                                    selectedPod: terminalInspectorPod,
                                    portForwardSessions: viewModel.state.portForwardSessions,
                                    onFillCommand: { command in
                                        viewModel.applySuggestedTerminalCommand(command, sendImmediately: false)
                                    },
                                    commandHistory: viewModel.terminalCommandHistory.entries,
                                    onClearCommandHistory: { viewModel.clearTerminalCommandHistory() }
                                )

                            case .logs:
                                terminalPodLogsDetails

                            case .yaml:
                                terminalPodYAMLDetails
                            }
                        }
                        .transaction { transaction in
                            transaction.animation = nil
                        }
                    }
                )
            } else {
                RuneContentStateView(.empty(
                    title: "No pod targeted",
                    message: "Choose a pod for the shell or its logs to inspect it here."
                ))
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .onAppear(perform: refreshTerminalInspectorSelectionIfNeeded)
        .onChange(of: terminalInspectorTab) { _, _ in
            refreshTerminalInspectorSelectionIfNeeded()
        }
        .onChange(of: terminalShellPodID) { _, _ in
            refreshTerminalInspectorForShellPodChangeIfNeeded()
        }
    }

    var terminalInspectorPod: PodSummary? {
        viewModel.state.pods.first { $0.id == terminalShellPodID }
            ?? viewModel.state.selectedPod
            ?? viewModel.state.pods.first
    }

    var terminalLogActivePod: PodSummary? {
        terminalLogTabState.activePod(in: viewModel.state.pods, fallback: terminalInitialLogPod)
    }

    var terminalInitialLogPod: PodSummary? {
        viewModel.state.pods.first { $0.id == terminalLogTabState.selectedPodID }
            ?? viewModel.state.selectedPod
            ?? viewModel.state.pods.first
    }

    @ViewBuilder
    var terminalPodLogsDetails: some View {
        if let pod = terminalLogActivePod {
            let podOptions = terminalLogPodOptions()
            // The pod tabs switch the log document, so they sit on its top edge.
            VStack(alignment: .leading, spacing: RuneSpacing.sm) {
                PodLogsInspectorPane(
                    selectedLogPreset: $viewModel.selectedLogPreset,
                    includePreviousLogs: $viewModel.includePreviousLogs,
                    selectedContainer: $viewModel.selectedLogContainer,
                    isTailModeEnabled: $viewModel.isLogTailModeEnabled,
                    isStreamPaused: $viewModel.isLogStreamPaused,
                    isLoadingLogs: viewModel.state.isLoadingLogs,
                    isLoadingResources: viewModel.state.isLoading,
                    errorMessage: viewModel.state.lastLogFetchError,
                    statusText: logStatusText,
                    podOptions: podOptions,
                    selectedPodID: terminalLogPodSelectionBinding(currentPod: pod, podOptions: podOptions),
                    isFavoritePod: isFavoritePod,
                    onToggleFavoritePod: toggleFavoritePod,
                    presentationStyle: .terminalCompact,
                    showsContainerPicker: false,
                    containerOptions: viewModel.podLogContainerOptions,
                    logText: viewModel.state.podLogs,
                    readOnlyResetID: "terminal-podlogs:\(pod.name):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs):\(viewModel.selectedLogContainer)",
                    onReload: { reloadActiveTerminalLogPod() },
                    onSave: { viewModel.saveCurrentLogs() },
                    onSaveToExportFolder: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: false) },
                    onSaveAndOpen: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: true) },
                    onSaveVisibleZip: { viewModel.saveVisibleLogsZip(visibleText: $0) },
                    onSaveVisibleZipToExportFolder: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: false) },
                    onSaveVisibleZipAndOpen: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: true) },
                    onSaveFullZip: { viewModel.saveCurrentLogsZip() },
                    onSaveFullZipToExportFolder: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: false) },
                    onSaveFullZipAndOpen: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: true) },
                    onSaveAllPodsZip: { viewModel.saveAllPodsLogsZip() },
                    onSaveAllPodsZipToExportFolder: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: false) },
                    onSaveAllPodsZipAndOpen: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: true) },
                    onCopySelection: { copySelectedTextFromFocusedTextView() },
                    onCopyAll: { viewModel.copyCurrentLogsToClipboard() },
                    onToggleStreamPause: { viewModel.toggleLogStreamPause() }
                )
                .environment(\.runeSurfaceTabs, AnyView(terminalLogTabBar))
                .environment(\.runeLogExtraActions, [
                    RuneLogExtraAction(
                        id: "terminal-logs-use-pod-in-shell",
                        title: "Use in Pod Shell",
                        systemImage: "terminal",
                        help: "Use \(pod.namespace)/\(pod.name) in Pod Shell",
                        action: { openTerminalShell(for: pod) }
                    )
                ])
                .onAppear {
                    ensureTerminalLogTabs(for: pod)
                    reloadActiveTerminalLogPod()
                }
            }
        } else {
            VStack(alignment: .leading, spacing: RuneSpacing.sm) {
                TerminalLogTabBar(
                    tabs: terminalLogTabPresentations,
                    activeTabID: terminalLogTabState.activeTabID,
                    canAddTab: !viewModel.state.pods.isEmpty,
                    onSelectTab: selectTerminalLogTab,
                    onCloseTab: closeTerminalLogTab,
                    onToggleFavoriteTab: toggleFavoriteTerminalLogTab,
                    onAddTab: addTerminalLogTab,
                    onMoveTab: moveTerminalLogTab
                )
                inspectorEmptyState(
                    .unselected(
                        title: "Select a pod for logs",
                        message: "Choose a pod from the center list before opening its logs."
                    ),
                    symbol: "doc.text.magnifyingglass"
                )
            }
        }
    }

    var terminalLogTabBar: some View {
        TerminalLogTabBar(
            tabs: terminalLogTabPresentations,
            activeTabID: terminalLogTabState.activeTabID,
            canAddTab: !viewModel.state.pods.isEmpty,
            onSelectTab: selectTerminalLogTab,
            onCloseTab: closeTerminalLogTab,
            onToggleFavoriteTab: toggleFavoriteTerminalLogTab,
            onAddTab: addTerminalLogTab,
            onMoveTab: moveTerminalLogTab
        )
    }

    func terminalLogPodOptions() -> [PodSummary] {
        viewModel.state.pods
            .sorted { lhs, rhs in
                let lhsFavorite = isFavoritePod(lhs)
                let rhsFavorite = isFavoritePod(rhs)
                if lhsFavorite != rhsFavorite {
                    return lhsFavorite && !rhsFavorite
                }
                let namespaceOrder = lhs.namespace.localizedCaseInsensitiveCompare(rhs.namespace)
                if namespaceOrder != .orderedSame {
                    return namespaceOrder == .orderedAscending
                }
                return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
            }
    }

    func terminalLogPodSelectionBinding(currentPod: PodSummary, podOptions: [PodSummary]) -> Binding<String> {
        Binding(
            get: {
                let availableIDs = Set(podOptions.map(\.id))
                if availableIDs.contains(terminalLogTabState.selectedPodID) {
                    return terminalLogTabState.selectedPodID
                }
                return currentPod.id
            },
            set: { podID in
                guard let pod = podOptions.first(where: { $0.id == podID }) else { return }
                terminalLogTabState.selectedPodID = pod.id
                updateActiveTerminalLogTab(to: pod)
                viewModel.focusTerminalPodInspector(pod, reloadLogs: true)
            }
        )
    }

    var terminalLogTabPresentations: [TerminalLogTabPresentation] {
        terminalLogTabState.presentations(pods: viewModel.state.pods, isFavorite: isFavoritePod)
    }

    func ensureTerminalLogTabs(for pod: PodSummary) {
        terminalLogTabState.ensureTab(for: pod)
    }

    func openTerminalLogs(for pod: PodSummary) {
        terminalInspectorTab = .logs
        terminalLogTabState.activateOrRetarget(for: pod)
        viewModel.focusTerminalPodInspector(
            pod,
            reloadLogs: shouldReloadTerminalPodLogs(for: pod)
        )
        viewModel.isDetailPaneVisible = true
        keyboardPaneFocus = .detail
    }

    func openTerminalShell(for pod: PodSummary) {
        if let sessionID = TerminalShellPodSelectionPolicy.preferredSessionIDForHandoff(
            pod: pod,
            sessions: viewModel.state.terminalSessions,
            contextName: viewModel.state.selectedContext?.name,
            preferredContainer: viewModel.selectedLogContainer
        ) {
            viewModel.selectTerminalSession(id: sessionID)
            keyboardPaneFocus = .content
            return
        }
        terminalShellPodID = pod.id
        keyboardPaneFocus = .content
    }

    func reconcileTerminalLogTabs() {
        terminalLogTabState.reconcile(availablePods: viewModel.state.pods, fallbackPod: terminalInitialLogPod)
    }

    func addTerminalLogTab() {
        guard let pod = preferredPodForNewTerminalLogTab() else { return }
        terminalLogTabState.add(preferredPod: pod)
        viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
    }

    func selectTerminalLogTab(_ id: String) {
        guard let pod = terminalLogTabState.select(id: id, availablePods: viewModel.state.pods) else { return }
        viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
    }

    /// A Terminal keyboard command (see `TerminalKeyCommand`), acted on in
    /// the strip you are in. False leaves the key to the window and menus:
    /// Cmd-W closes the window with one tab left, Cmd-K opens the palette
    /// outside the shell, and Cmd-T in the shell is the tab bar's own.
    func handleTerminalKeyCommand(_ command: TerminalKeyCommand, in window: NSWindow?) -> Bool {
        // Text size applies to every monospaced view, in any section.
        if command == .biggerText || command == .smallerText || command == .defaultTextSize {
            TerminalKeyCommand.stepSharedTextSize(command)
            return true
        }
        guard viewModel.state.selectedSection == .terminal else { return false }
        let inLogs = terminalLogsHaveFocus(in: window)
        switch command {
        case .closeTab:
            guard let target = terminalCloseTabTarget(in: window) else { return false }
            closeTerminalTab(target)
            return true
        case .newTab:
            guard inLogs else { return false }
            addTerminalLogTab()
            return true
        case .reopenClosedTab:
            if inLogs {
                guard let pod = terminalLogTabState.reopenLastClosed(availablePods: viewModel.state.pods) else { return false }
                viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
                return true
            }
            // With nothing to reopen, a tab being composed stays as it is.
            guard viewModel.reopenLastClosedTerminalSession() else { return false }
            isComposingTerminalShellTab = false
            return true
        case .nextTab, .previousTab:
            let offset = command == .nextTab ? 1 : -1
            if inLogs {
                guard let pod = terminalLogTabState.selectAdjacent(offset: offset, availablePods: viewModel.state.pods) else { return false }
                viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
                return true
            }
            guard let id = TerminalTabCycle.adjacentID(
                in: scopedTerminalSessions.map(\.id),
                active: viewModel.state.activeTerminalSessionID,
                offset: offset
            ) else { return false }
            isComposingTerminalShellTab = false
            viewModel.selectTerminalSession(id: id)
            return true
        case .clear:
            guard (window?.firstResponder as? RuneTerminalInputSurface) != nil else { return false }
            viewModel.clearTerminalSessionTranscript()
            return true
        case .biggerText, .smallerText, .defaultTextSize:
            return false
        }
    }

    private var scopedTerminalSessions: [PodTerminalSession] {
        TerminalShellPodSelectionPolicy.sessions(
            in: viewModel.state.terminalSessions,
            contextName: viewModel.state.selectedContext?.name,
            namespace: viewModel.state.selectedNamespace
        )
    }

    /// Whether keys are about the log tabs: the inspector shows Logs and the
    /// focus is there (see `TerminalCloseTabPolicy.logsHaveFocus`).
    private func terminalLogsHaveFocus(in window: NSWindow?) -> Bool {
        viewModel.isDetailPaneVisible
            && terminalInspectorTab == .logs
            && TerminalCloseTabPolicy.logsHaveFocus(
                responderIsInInspector: responderIsInInspector(of: window),
                paneFocusIsInspector: keyboardPaneFocus == .detail
            )
    }

    /// Cmd-W in the Terminal section: the tab to close, if any (see
    /// `TerminalCloseTabPolicy`); nil leaves Cmd-W to close the window.
    func terminalCloseTabTarget(in window: NSWindow?) -> TerminalCloseTabPolicy.Target? {
        guard viewModel.state.selectedSection == .terminal else { return nil }
        let strips = TerminalCloseTabPolicy.Strips(
            logTabIDs: terminalLogTabState.tabs.map(\.id),
            activeLogTabID: terminalLogTabState.activeTabID,
            shellSessionIDs: scopedTerminalSessions.map(\.id),
            activeShellSessionID: viewModel.state.activeTerminalSessionID,
            isComposingShellDraft: isComposingTerminalShellTab
        )
        let logsFocused = TerminalCloseTabPolicy.logsHaveFocus(
            responderIsInInspector: responderIsInInspector(of: window),
            paneFocusIsInspector: keyboardPaneFocus == .detail
        )
        return TerminalCloseTabPolicy.target(
            logsFocused: logsFocused,
            logsVisible: viewModel.isDetailPaneVisible && terminalInspectorTab == .logs,
            strips: strips
        )
    }

    /// Whether the focused view is in the inspector; nil when no view has
    /// focus or it spans both panes.
    private func responderIsInInspector(of window: NSWindow?) -> Bool? {
        guard let window,
              let responder = window.firstResponder as? NSView,
              let anchor = RuneInspectorDocumentAnchor.AnchorView.anchor(in: window) else { return nil }
        let frame = responder.convert(responder.bounds, to: nil)
        let pane = anchor.paneRectInWindow
        if frame.contains(NSPoint(x: pane.midX, y: pane.midY)) { return nil }
        return pane.contains(NSPoint(x: frame.midX, y: frame.midY))
    }

    func closeTerminalTab(_ target: TerminalCloseTabPolicy.Target) {
        switch target {
        case .logTab(let id):
            closeTerminalLogTab(id)
        case .shellSession(let id):
            viewModel.closeTerminalSession(id: id)
        case .shellDraft:
            isComposingTerminalShellTab = false
        }
    }

    func closeTerminalLogTab(_ id: String) {
        if let pod = terminalLogTabState.close(id: id, availablePods: viewModel.state.pods, fallbackPod: terminalInitialLogPod) {
            viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
        }
    }

    func toggleFavoriteTerminalLogTab(_ id: String) {
        guard let tab = terminalLogTabState.tabs.first(where: { $0.id == id }) else { return }
        if let pod = viewModel.state.pods.first(where: { $0.id == tab.podID }) {
            toggleFavoritePod(pod)
        } else {
            viewModel.toggleFavoriteResource(kind: .pod, namespace: tab.namespace, name: tab.podName)
        }
        terminalLogTabState.groupPinnedFirst(isPinned: isPinnedTerminalLogTab)
    }

    func isPinnedTerminalLogTab(_ tab: TerminalPodLogTab) -> Bool {
        viewModel.state.pods.first(where: { $0.id == tab.podID }).map(isFavoritePod) ?? false
    }

    func moveTerminalLogTab(_ id: String, onto targetID: String) {
        terminalLogTabState.move(id: id, onto: targetID, isPinned: isPinnedTerminalLogTab)
    }

    func updateActiveTerminalLogTab(to pod: PodSummary) {
        terminalLogTabState.updateActive(to: pod)
    }

    func reloadActiveTerminalLogPod() {
        guard let pod = terminalLogActivePod else { return }
        viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
    }

    func preferredPodForNewTerminalLogTab() -> PodSummary? {
        terminalLogTabState.preferredPodForNewTab(
            pods: viewModel.state.pods,
            fallbackPod: terminalInitialLogPod,
            isFavorite: isFavoritePod
        )
    }

    func isFavoritePod(_ pod: PodSummary) -> Bool {
        viewModel.isFavoriteResource(kind: .pod, namespace: pod.namespace, name: pod.name)
    }

    func toggleFavoritePod(_ pod: PodSummary) {
        viewModel.toggleFavoriteResource(kind: .pod, namespace: pod.namespace, name: pod.name)
    }

    @ViewBuilder
    var terminalPodYAMLDetails: some View {
        if let pod = terminalInspectorPod {
            manifestInspectorPane(activeTab: .yaml)
                .onAppear {
                    viewModel.focusTerminalPodInspector(pod, loadDetails: shouldReloadTerminalPodDetails(for: pod))
                }
        } else {
            inspectorEmptyState(
                .unselected(
                    title: "Select a pod for YAML",
                    message: "Choose a pod from the center list before opening its manifest."
                ),
                symbol: "curlybraces"
            )
        }
    }

    func refreshTerminalInspectorSelectionIfNeeded() {
        switch terminalInspectorTab {
        case .commands:
            break
        case .logs:
            guard let pod = terminalLogActivePod else { return }
            ensureTerminalLogTabs(for: pod)
            viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
        case .yaml:
            guard let pod = terminalInspectorPod else { return }
            viewModel.focusTerminalPodInspector(pod, loadDetails: shouldReloadTerminalPodDetails(for: pod))
        }
    }

    func refreshTerminalInspectorForShellPodChangeIfNeeded() {
        guard terminalInspectorTab == .yaml else { return }
        refreshTerminalInspectorSelectionIfNeeded()
    }

    func shouldReloadTerminalPodLogs(for pod: PodSummary) -> Bool {
        viewModel.state.selectedPod?.id != pod.id || viewModel.state.podLogs.isEmpty
    }

    func shouldReloadTerminalPodDetails(for pod: PodSummary) -> Bool {
        viewModel.state.selectedPod?.id != pod.id || viewModel.state.resourceYAML.isEmpty
    }

    func portForwardSessionRow(_ session: PortForwardSession) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
            HStack {
                Text("\(session.resourceLabel)  \(session.localPort):\(session.remotePort)")
                    .runeText(.microEmphasis)
                Spacer()
                Text(session.status.rawValue.capitalized)
                    .runeText(.microEmphasis)
                    .padding(.horizontal, RuneSpacing.xs)
                    .padding(.vertical, RuneSpacing.xxs)
                    .background(portForwardStatusColor(session.status).opacity(0.16), in: Capsule())
                    .foregroundStyle(portForwardStatusColor(session.status))
            }

            Text("\(session.contextName) • \(session.namespace) • \(session.address)")
                .runeText(.micro)
                .foregroundStyle(.runeSecondary)

            if !session.lastMessage.isEmpty {
                Text(session.lastMessage)
                    .runeText(.micro)
                    .foregroundStyle(.runeSecondary)
                    .textSelection(.enabled)
            }

            if (session.status == .active && session.browserURL != nil) || session.isInactive {
                HStack(spacing: RuneSpacing.xs) {
                    if session.status == .active, session.browserURL != nil {
                        Button {
                            viewModel.openPortForwardInBrowser(session)
                        } label: {
                            Label("Open in Browser", systemImage: "safari")
                        }
                        .help(session.browserURL.map { "Open \($0.absoluteString)" } ?? "Open local port-forward URL")
                    }

                    if session.status == .failed {
                        Button {
                            viewModel.retryPortForward(session)
                        } label: {
                            Label("Retry", systemImage: "arrow.clockwise")
                        }
                        .help("Try this port-forward again")
                    }

                    if session.isInactive {
                        Button {
                            viewModel.clearPortForwardSession(session)
                        } label: {
                            Label("Clear", systemImage: "xmark.circle")
                        }
                        .help("Remove this inactive port-forward row")
                    }
                }
            }
        }
        .padding(RuneSpacing.inset)
        .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
    }
}
