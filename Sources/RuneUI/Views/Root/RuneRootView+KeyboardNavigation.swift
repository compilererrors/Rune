import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    @ViewBuilder
    var keyboardNavigationBridge: some View {
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
    func paneFocusOutline(isFocused: Bool) -> some View {
        if isFocused {
            RoundedRectangle(cornerRadius: RuneRadius.card, style: .continuous)
                .strokeBorder(Color.accentColor.opacity(0.35), lineWidth: 1.5)
                .padding(RuneSpacing.xxs)
                .allowsHitTesting(false)
        }
    }

    var keyboardNavigationContext: RuneRootKeyboardNavigationContext {
        let keyWindow = NSApp.keyWindow
        let editableTextResponderActive = (keyWindow?.firstResponder as? NSTextView)?.isEditable == true
        return RuneRootKeyboardNavigationContext(
            commandPalettePresented: viewModel.state.isCommandPalettePresented,
            yamlEditorSheetPresented: isYAMLEditorSheetPresented,
            yamlManifestIsEditing: yamlManifestIsEditing,
            kubeConfigImportReviewPresented: viewModel.isKubeConfigImportConfirmationPending,
            addClusterPopoverPresented: addClusterPopoverPresented,
            addClusterProviderPresented: selectedAddClusterProvider != nil,
            manualNamespaceSheetPresented: isManualNamespaceSheetPresented,
            pendingWriteConfirmationPresented: viewModel.pendingWriteAction != nil,
            launchExperiencePresented: shouldShowLaunchExperience,
            appSheetAttached: keyWindow?.sheetParent != nil || keyWindow?.attachedSheet != nil,
            editableTextResponderActive: editableTextResponderActive
        )
    }

    var keyboardNavigationSuspended: Bool {
        keyboardNavigationContext.isSuspended
    }

    func focusNextKeyboardPane() {
        let navigationContext = keyboardNavigationContext
        guard !navigationContext.hasBlockingPresentation else { return }
        if textInputFocus != nil {
            textInputFocus = nil
            keyboardPaneFocus = advancedKeyboardPane(from: keyboardPaneFocus, forward: true)
            NSApp.keyWindow?.makeFirstResponder(nil)
            return
        }
        guard !navigationContext.isSuspended else { return }
        keyboardPaneFocus = advancedKeyboardPane(from: keyboardPaneFocus, forward: true)
        NSApp.keyWindow?.makeFirstResponder(nil)
    }

    func focusPreviousKeyboardPane() {
        let navigationContext = keyboardNavigationContext
        guard !navigationContext.hasBlockingPresentation else { return }
        if textInputFocus != nil {
            textInputFocus = nil
            keyboardPaneFocus = advancedKeyboardPane(from: keyboardPaneFocus, forward: false)
            NSApp.keyWindow?.makeFirstResponder(nil)
            return
        }
        guard !navigationContext.isSuspended else { return }
        keyboardPaneFocus = advancedKeyboardPane(from: keyboardPaneFocus, forward: false)
        NSApp.keyWindow?.makeFirstResponder(nil)
    }

    func moveKeyboardSelection(_ direction: MoveCommandDirection) {
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

    func activateKeyboardSelection() {
        guard !keyboardNavigationSuspended else { return }
        switch keyboardPaneFocus {
        case .sidebarSections, .sidebarContexts:
            keyboardPaneFocus = .content
        case .content:
            if viewModel.state.selectedSection == .overview {
                openSelectedOverviewCard()
            } else if viewModel.isDetailPaneVisible {
                keyboardPaneFocus = .detail
            }
        case .detail:
            break
        }
    }

    var availableKeyboardPanes: [RuneRootKeyboardPane] {
        // The installed AppKit monitor retains its original view value. Read the
        // current preference so changes made in Settings apply without a restart.
        RuneRootKeyboardPaneNavigation.availablePanes(
            sidebarVisible: viewModel.isSidebarVisible,
            detailVisible: viewModel.isDetailPaneVisible,
            skipsClusterPane: UserDefaults.standard.bool(forKey: RuneSettingsKeys.skipClusterOnTabNavigationFromSections)
        )
    }

    func advancedKeyboardPane(from current: RuneRootKeyboardPane, forward: Bool) -> RuneRootKeyboardPane {
        RuneRootKeyboardPaneNavigation.advanced(
            from: current,
            forward: forward,
            in: availableKeyboardPanes
        )
    }

    func moveSectionSelection(_ direction: MoveCommandDirection) {
        let sections = RuneSection.allCases
        guard let currentIndex = sections.firstIndex(of: viewModel.state.selectedSection),
              let nextIndex = steppedIndex(count: sections.count, current: currentIndex, direction: direction) else {
            return
        }
        viewModel.setSection(sections[nextIndex])
    }

    func moveContextSelection(_ direction: MoveCommandDirection) {
        let contexts = viewModel.visibleContexts
        guard !contexts.isEmpty else { return }
        let currentID = viewModel.state.selectedContext?.id
        guard let next = steppedItem(items: contexts, currentID: currentID, direction: direction) else { return }
        viewModel.setContext(next)
    }

    func moveContentSelection(_ direction: MoveCommandDirection) {
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
            case .endpoint:
                if let next = steppedItem(items: viewModel.visibleEndpoints, currentID: viewModel.state.selectedEndpoint?.id, direction: direction) {
                    viewModel.selectEndpoint(next)
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
            switch effectiveHelmBrowserTab {
            case .releases:
                if let next = steppedItem(
                    items: viewModel.visibleHelmReleases,
                    currentID: viewModel.state.selectedHelmRelease?.id,
                    direction: direction
                ) {
                    viewModel.selectHelmRelease(next)
                }
            case .operatorResources:
                if let next = steppedItem(
                    items: viewModel.pagedOperatorResources,
                    currentID: viewModel.state.selectedOperatorResource?.id,
                    direction: direction
                ) {
                    viewModel.selectOperatorResource(next)
                }
            }
        case .overview:
            moveOverviewCardSelection(direction)
        case .terminal:
            break
        }
    }

    func moveOverviewCardSelection(_ direction: MoveCommandDirection) {
        guard !overviewCardModules.isEmpty else { return }
        let current = min(max(overviewCardSelectionIndex, 0), overviewCardModules.count - 1)
        guard let next = steppedIndex(count: overviewCardModules.count, current: current, direction: direction) else {
            return
        }
        overviewCardSelectionIndex = next
    }

    func openSelectedOverviewCard() {
        guard !overviewCardModules.isEmpty else { return }
        let index = min(max(overviewCardSelectionIndex, 0), overviewCardModules.count - 1)
        overviewCardSelectionIndex = index
        viewModel.openOverviewModule(overviewCardModules[index])
    }

    /// In the middle/content pane, use left/right to switch between kind tabs
    /// (for sections that expose segmented kinds), and leave up/down for row stepping.
    func moveContentKindIfNeeded(_ direction: MoveCommandDirection) -> Bool {
        guard direction == .left || direction == .right else { return false }
        if viewModel.state.selectedSection == .helm {
            let nextTab = advancedTab(current: effectiveHelmBrowserTab, direction: direction)
            helmBrowserTab = nextTab
            viewModel.setHelmBrowserResourceFamily(nextTab.resourceListFamily)
            return true
        }

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

    func contentKindsForSelectedSection() -> [KubeResourceKind]? {
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

    /// A click anywhere in the inspector gives it keyboard focus, so Space,
    /// arrows, Page Up/Down and Home/End scroll its document. A field outside
    /// the inspector stops editing; a control the click lands on inside the
    /// inspector still takes focus as usual (the event continues).
    func installLocalMouseMonitorIfNeeded() {
        guard localMouseEventMonitor == nil else { return }
        localMouseEventMonitor = NSEvent.addLocalMonitorForEvents(matching: [.leftMouseDown]) { event in
            focusInspectorPaneIfClicked(event)
            return event
        }
    }

    func removeLocalMouseMonitor() {
        guard let localMouseEventMonitor else { return }
        NSEvent.removeMonitor(localMouseEventMonitor)
        self.localMouseEventMonitor = nil
    }

    func focusInspectorPaneIfClicked(_ event: NSEvent) {
        guard let window = event.window,
              window.windowNumber == workspaceWindowReference.windowNumber || workspaceWindowReference.windowNumber == nil,
              let anchor = RuneInspectorDocumentAnchor.AnchorView.anchor(in: window),
              anchor.paneRectInWindow.contains(event.locationInWindow) else { return }
        keyboardPaneFocus = .detail
        textInputFocus = nil
        if let responder = window.firstResponder as? NSView {
            let frame = responder.convert(responder.bounds, to: nil)
            let paneRect = anchor.paneRectInWindow
            let containsPane = frame.contains(NSPoint(x: paneRect.midX, y: paneRect.midY))
            let isInsidePane = paneRect.contains(NSPoint(x: frame.midX, y: frame.midY))
            if !containsPane, !isInsidePane {
                window.makeFirstResponder(nil)
            }
        }
    }

    func installLocalKeyboardMonitorIfNeeded() {
        guard localKeyEventMonitor == nil else { return }
        localKeyEventMonitor = NSEvent.addLocalMonitorForEvents(matching: [.keyDown]) { event in
            if let handledEvent = handleLocalKeyEvent(event) {
                return handledEvent
            }
            return nil
        }
    }

    func handleLocalKeyEvent(_ event: NSEvent) -> NSEvent? {
        guard RuneRootKeyboardWindowScope.owns(
            eventWindowNumber: event.windowNumber,
            workspaceWindowNumber: workspaceWindowReference.windowNumber,
            keyWindowNumber: NSApp.keyWindow?.windowNumber
        ) else { return event }

        if shouldHandleTabNavigation(event) {
            if event.modifierFlags.contains(.shift) {
                focusPreviousKeyboardPane()
            } else {
                focusNextKeyboardPane()
            }
            return nil
        }

        // The inspector pane has keyboard focus: Space, arrows, Page Up/Down
        // and Home/End scroll its document (Logs, YAML, Describe).
        if keyboardPaneFocus == .detail,
           !keyboardNavigationContext.isSuspended,
           let window = event.window ?? NSApp.keyWindow,
           RuneInspectorKeyboardScrolling.handle(event, in: window) {
            return nil
        }

        if shouldHandlePaneArrowNavigation(event) {
            if event.keyCode == 123 {
                moveKeyboardSelection(.left)
            } else {
                moveKeyboardSelection(.right)
            }
            return nil
        }

        guard shouldHandleConfiguredActionKey(event) else { return event }
        guard let action = configuredAction(for: event) else { return event }
        return performConfiguredAction(action) ? nil : event
    }

    func shouldHandleTabNavigation(_ event: NSEvent) -> Bool {
        guard event.keyCode == 48 else { return false }
        guard RuneRootKeyboardWindowScope.owns(
            eventWindowNumber: event.windowNumber,
            workspaceWindowNumber: workspaceWindowReference.windowNumber,
            keyWindowNumber: NSApp.keyWindow?.windowNumber
        ) else { return false }
        let navigationContext = keyboardNavigationContext
        guard !navigationContext.hasBlockingPresentation else { return false }
        let disallowedModifiers: NSEvent.ModifierFlags = [.command, .option, .control, .function]
        guard event.modifierFlags.isDisjoint(with: disallowedModifiers) else { return false }
        return textInputFocus != nil || !navigationContext.editableTextResponderActive
    }

    func shouldHandleConfiguredActionKey(_ event: NSEvent) -> Bool {
        guard !keyboardNavigationSuspended else { return false }
        guard textInputFocus == nil else { return false }
        let disallowedModifiers: NSEvent.ModifierFlags = [.function]
        return event.modifierFlags.isDisjoint(with: disallowedModifiers)
    }

    func shouldHandlePaneArrowNavigation(_ event: NSEvent) -> Bool {
        guard event.keyCode == 123 || event.keyCode == 124 else { return false }
        guard keyboardPaneFocus == .detail || (keyboardPaneFocus == .content && viewModel.state.selectedSection != .terminal) else {
            return false
        }
        guard !keyboardNavigationSuspended else { return false }
        guard textInputFocus == nil else { return false }
        let disallowedModifiers: NSEvent.ModifierFlags = [.command, .option, .control, .shift, .function]
        return event.modifierFlags.isDisjoint(with: disallowedModifiers)
    }

    func configuredAction(for event: NSEvent) -> RuneKeyBindingAction? {
        guard let baseKey = configuredActionBaseKey(for: event) else { return nil }

        let resolver = RuneKeyBindingResolver { action in
            UserDefaults.standard.runeKeyBindingShortcut(for: action)
        }
        let modifiers = runeKeyBindingModifiers(for: event)
        if let action = resolver.action(
            for: RuneKeyBindingInput(
                baseKey: baseKey,
                modifiers: modifiers,
                isTextInputFocused: textInputFocus != nil,
                isNavigationSuspended: keyboardNavigationSuspended
            )
        ) {
            return action
        }

        guard modifiers.contains(.shift),
              let shiftedSymbol = configuredActionShiftedSymbolKey(for: event) else {
            return nil
        }
        var symbolModifiers = modifiers
        symbolModifiers.remove(.shift)
        return resolver.action(
            for: RuneKeyBindingInput(
                baseKey: shiftedSymbol,
                modifiers: symbolModifiers,
                isTextInputFocused: textInputFocus != nil,
                isNavigationSuspended: keyboardNavigationSuspended
            )
        )
    }

    func runeKeyBindingModifiers(for event: NSEvent) -> Set<RuneKeyBindingModifier> {
        var modifiers: Set<RuneKeyBindingModifier> = []
        if event.modifierFlags.contains(.command) {
            modifiers.insert(.command)
        }
        if event.modifierFlags.contains(.option) {
            modifiers.insert(.option)
        }
        if event.modifierFlags.contains(.control) {
            modifiers.insert(.control)
        }
        if event.modifierFlags.contains(.shift) {
            modifiers.insert(.shift)
        }
        if event.modifierFlags.contains(.function) {
            modifiers.insert(.function)
        }
        return modifiers
    }

    func configuredActionBaseKey(for event: NSEvent) -> String? {
        switch event.keyCode {
        case 123:
            return "left"
        case 124:
            return "right"
        default:
            guard let baseKey = event.charactersIgnoringModifiers?.lowercased(), baseKey.count == 1 else {
                return nil
            }
            return baseKey
        }
    }

    func configuredActionShiftedSymbolKey(for event: NSEvent) -> String? {
        guard let key = event.characters?.lowercased(), key.count == 1 else { return nil }
        guard ["[", "]", "/", ":", "?"].contains(key) else { return nil }
        return key
    }

    func removeLocalKeyboardMonitor() {
        guard let localKeyEventMonitor else { return }
        NSEvent.removeMonitor(localKeyEventMonitor)
        self.localKeyEventMonitor = nil
    }
}
