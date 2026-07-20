import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class RuneSidebarChromeContractTests: XCTestCase {
    func testSidebarUsesGlassPaneSurfaceInsteadOfRoundedMaterialCard() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        guard let sidebarStart = source.range(of: "private var sidebar: some View {"),
              let sectionRowStart = source.range(of: "private func sectionRow", range: sidebarStart.upperBound..<source.endIndex) else {
            XCTFail("Could not locate sidebar block in RuneRootView.swift")
            return
        }

        let sidebarBlock = String(source[sidebarStart.lowerBound..<sectionRowStart.lowerBound])
        XCTAssertFalse(sidebarBlock.contains("sidebarBrandHeader"))
        XCTAssertFalse(sidebarBlock.contains("rune_logo_main"))
        XCTAssertFalse(sidebarBlock.contains(".frame(width: 104, height: 104)"))
        XCTAssertTrue(sidebarBlock.contains("TextField(\"Search contexts\""))
        XCTAssertTrue(sidebarBlock.contains("RuneGlassPaneSurface(role: .sidebar)"))
        XCTAssertTrue(sidebarBlock.contains("RuneGlassPaneBorder(role: .sidebar)"))
        XCTAssertFalse(sidebarBlock.contains("RoundedRectangle(cornerRadius:"))
        XCTAssertFalse(sidebarBlock.contains(".thinMaterial"))
        XCTAssertFalse(sidebarBlock.contains(".clipShape("))
    }

    func testWorkspacePanesShareNeutralGlassChrome() throws {
        let source = try String(contentsOfFile: runeGlassShellPath, encoding: .utf8)

        XCTAssertTrue(source.contains("case .sidebar, .content, .inspector:"))
        XCTAssertFalse(source.contains("Color.black.opacity(0.10)"))
        XCTAssertFalse(source.contains("Color.white.opacity(0.05)"))
        XCTAssertFalse(source.contains("Color.white.opacity(0.03)"))
    }

    func testTerminalWorkspaceUsesTransparentScrollSurface() throws {
        let source = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)

        guard let bodyStart = source.range(of: "var body: some View {"),
              let heightStart = source.range(of: "private func terminalHeight", range: bodyStart.upperBound..<source.endIndex) else {
            XCTFail("Could not locate terminal workspace body")
            return
        }

        let bodyBlock = String(source[bodyStart.lowerBound..<heightStart.lowerBound])
        XCTAssertTrue(bodyBlock.contains(".scrollContentBackground(.hidden)"))
        XCTAssertTrue(bodyBlock.contains(".background(Color.clear)"))
    }

    func testTerminalWorkspaceRendersAcrossCompactAndWideViewports() async throws {
        let pods = [
            PodSummary(name: "pod-0", namespace: "default", status: "Running"),
            PodSummary(name: "pod-1", namespace: "default", status: "Running")
        ]
        let session = PodTerminalSession(
            id: "shell-a",
            contextName: "benchmark",
            namespace: "default",
            podName: "pod-0",
            shell: "sh",
            transcript: "wide terminal line " + String(repeating: "x", count: 240),
            status: .connected
        )

        for size in [NSSize(width: 760, height: 520), NSSize(width: 1440, height: 900)] {
            let host = NSHostingController(
                rootView: ResourceTerminalWorkspaceView(
                    session: session,
                    sessions: [session],
                    activeSessionID: session.id,
                    contextName: "benchmark",
                    namespace: "default",
                    selectedPod: pods[0],
                    availablePods: pods,
                    portForwardSessions: [],
                    canApplyMutations: true,
                    selectedShellPodID: .constant(pods[0].id),
                    selectedPortForwardPodID: .constant(pods[0].id),
                    terminalInput: .constant(""),
                    portForwardLocalPort: .constant("8080"),
                    portForwardRemotePort: .constant("80"),
                    portForwardAddress: .constant("127.0.0.1"),
                    onStartSession: { _, _ in },
                    onReconnectSession: { _, _, _ in },
                    onStartPortForward: { _ in },
                    onStopPortForward: { _ in },
                    onOpenPortForwardInBrowser: { _ in },
                    onRetryPortForward: { _ in },
                    onClearPortForward: { _ in },
                    onClearInactivePortForwards: {},
                    onSend: {},
                    onSendControlSequence: { _ in },
                    onResizeSession: { _, _, _ in },
                    onDisconnect: {},
                    onSelectSession: { _ in },
                    onCloseSession: { _ in },
                    onClearTranscript: {},
                    onSaveActiveTerminalTranscript: {},
                    onSaveAllTerminalTranscripts: {},
                    onSaveActiveTerminalTranscriptToExportFolder: {},
                    onSaveActiveTerminalTranscriptAndOpen: {},
                    onSaveAllTerminalTranscriptsToExportFolder: {},
                    onSaveAllTerminalTranscriptsAndOpen: {},
                    isFavoritePod: { _ in false },
                    onToggleFavoritePod: { _ in }
                )
                .frame(width: size.width, height: size.height)
            )
            let window = NSWindow(
                contentRect: NSRect(origin: .zero, size: size),
                styleMask: [.titled, .closable, .resizable],
                backing: .buffered,
                defer: false
            )
            window.contentViewController = host
            window.makeKeyAndOrderFront(nil)
            try await settle(window: window)

            guard let terminalScrollView = findTerminalTranscriptScrollView(in: host.view) else {
                window.orderOut(nil)
                return XCTFail("Expected terminal transcript scroll view at \(size.width)x\(size.height)")
            }

            let frame = host.view.convert(terminalScrollView.bounds, from: terminalScrollView)
            XCTAssertGreaterThan(frame.width, 240)
            XCTAssertGreaterThan(frame.height, 200)
            XCTAssertLessThanOrEqual(frame.maxX, host.view.bounds.maxX + 1)
            XCTAssertTrue(terminalScrollView.hasHorizontalScroller)

            window.orderOut(nil)
        }
    }

    func testTerminalPromptInputTextStaysVisibleInDarkTerminalChrome() async throws {
        let pod = PodSummary(name: "pod-0", namespace: "default", status: "Running")
        let session = PodTerminalSession(
            id: "shell-a",
            contextName: "demo",
            namespace: "default",
            podName: pod.name,
            shell: "sh",
            transcript: "[rune] connected\n",
            status: .connected
        )
        var terminalInput = ""
        let host = NSHostingController(
            rootView: TerminalShellPanelView(
                session: session,
                sessions: [session],
                activeSessionID: session.id,
                isComposingNewSession: false,
                selectedPod: pod,
                availablePods: [pod],
                canApplyMutations: true,
                transcriptHeight: 260,
                selectedShellPodID: .constant(pod.id),
                terminalInput: Binding(
                    get: { terminalInput },
                    set: { terminalInput = $0 }
                ),
                onStartSession: { _, _ in },
                onReconnectSession: { _, _, _ in },
                onSend: {},
                onSendControlSequence: { _ in },
                onResizeSession: { _, _, _ in },
                onDisconnect: {},
                onSelectSession: { _ in },
                onCloseSession: { _ in },
                onComposeNewSession: {},
                onClearTranscript: {},
                onSaveActiveTranscript: {},
                onSaveAllTranscripts: {},
                onSaveActiveTranscriptToExportFolder: {},
                onSaveActiveTranscriptAndOpen: {},
                onSaveAllTranscriptsToExportFolder: {},
                onSaveAllTranscriptsAndOpen: {},
                isFavoritePod: { _ in false },
                onToggleFavoritePod: { _ in }
            )
            .frame(width: 720, height: 420)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 720, height: 420),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.appearance = NSAppearance(named: .darkAqua)
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        try await settle(window: window)
        defer { window.orderOut(nil) }

        guard let promptTextView = allTextViews(in: host.view).first(where: \.isEditable) else {
            XCTFail("Expected editable terminal prompt text view.")
            return
        }

        promptTextView.window?.makeFirstResponder(promptTextView)
        promptTextView.insertText("kubectl get pods", replacementRange: promptTextView.selectedRange())
        try await settle(window: window)

        XCTAssertEqual(terminalInput, "kubectl get pods")
        XCTAssertEqual(promptTextView.string, "kubectl get pods")
        XCTAssertGreaterThan(promptTextView.bounds.width, 200)

        guard let layoutManager = promptTextView.layoutManager,
              let textContainer = promptTextView.textContainer else {
            XCTFail("Expected terminal prompt text view to have a layout manager and text container.")
            return
        }
        layoutManager.ensureLayout(for: textContainer)
        let glyphRange = layoutManager.glyphRange(for: textContainer)
        let glyphRect = layoutManager
            .boundingRect(forGlyphRange: glyphRange, in: textContainer)
            .offsetBy(dx: promptTextView.textContainerOrigin.x, dy: promptTextView.textContainerOrigin.y)

        XCTAssertGreaterThan(glyphRange.length, 0)
        XCTAssertGreaterThan(glyphRect.width, 40)
        XCTAssertTrue(
            promptTextView.bounds.insetBy(dx: -1, dy: -1).intersects(glyphRect),
            "Expected typed terminal prompt glyphs to be inside the prompt bounds. bounds=\(promptTextView.bounds) glyph=\(glyphRect)"
        )
    }

    func testTerminalPromptHandlesRealAppKitKeyEventsForSendAndControlSequences() async throws {
        let pod = PodSummary(name: "pod-0", namespace: "default", status: "Running")
        let session = PodTerminalSession(
            id: "shell-a",
            contextName: "demo",
            namespace: "default",
            podName: pod.name,
            shell: "sh",
            transcript: "[rune] connected\n",
            status: .connected
        )
        var terminalInput = ""
        var sendCount = 0
        var controlSequences: [String] = []
        let host = NSHostingController(
            rootView: TerminalShellPanelView(
                session: session,
                sessions: [session],
                activeSessionID: session.id,
                isComposingNewSession: false,
                selectedPod: pod,
                availablePods: [pod],
                canApplyMutations: true,
                transcriptHeight: 260,
                selectedShellPodID: .constant(pod.id),
                terminalInput: Binding(
                    get: { terminalInput },
                    set: { terminalInput = $0 }
                ),
                onStartSession: { _, _ in },
                onReconnectSession: { _, _, _ in },
                onSend: { sendCount += 1 },
                onSendControlSequence: { controlSequences.append($0) },
                onResizeSession: { _, _, _ in },
                onDisconnect: {},
                onSelectSession: { _ in },
                onCloseSession: { _ in },
                onComposeNewSession: {},
                onClearTranscript: {},
                onSaveActiveTranscript: {},
                onSaveAllTranscripts: {},
                onSaveActiveTranscriptToExportFolder: {},
                onSaveActiveTranscriptAndOpen: {},
                onSaveAllTranscriptsToExportFolder: {},
                onSaveAllTranscriptsAndOpen: {},
                isFavoritePod: { _ in false },
                onToggleFavoritePod: { _ in }
            )
            .frame(width: 720, height: 420)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 720, height: 420),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        try await settle(window: window)
        defer { window.orderOut(nil) }

        guard let promptTextView = allTextViews(in: host.view).first(where: \.isEditable) else {
            XCTFail("Expected editable terminal prompt text view.")
            return
        }

        promptTextView.window?.makeFirstResponder(promptTextView)
        promptTextView.insertText("kubectl get pods", replacementRange: promptTextView.selectedRange())
        try await settle(window: window)

        promptTextView.keyDown(with: keyDownEvent(
            window: window,
            characters: "\r",
            charactersIgnoringModifiers: "\r",
            keyCode: 36
        ))
        promptTextView.insertText("running command", replacementRange: promptTextView.selectedRange())
        try await settle(window: window)
        promptTextView.keyDown(with: keyDownEvent(
            window: window,
            modifierFlags: .control,
            characters: "\u{3}",
            charactersIgnoringModifiers: "c",
            keyCode: 8
        ))

        XCTAssertEqual(sendCount, 1)
        XCTAssertEqual(controlSequences, ["\u{3}"])
        XCTAssertEqual(terminalInput, "")
        XCTAssertEqual(promptTextView.string, "")
    }

    func testTerminalWorkspaceComposesIndependentPanelsInStableOrder() throws {
        let workspaceSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let shellSource = try String(contentsOfFile: terminalShellPanelViewPath, encoding: .utf8)
        let portForwardSource = try String(contentsOfFile: terminalPortForwardPanelViewPath, encoding: .utf8)
        let tabBarSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)
        let tabStripSource = try String(contentsOfFile: terminalTabStripPath, encoding: .utf8)
        let sessionControlSource = try String(contentsOfFile: terminalSessionControlRowPath, encoding: .utf8)
        let transcriptSource = try String(contentsOfFile: terminalTranscriptSurfacePath, encoding: .utf8)

        XCTAssertTrue(workspaceSource.contains("TerminalPortForwardPanelView("))
        XCTAssertTrue(workspaceSource.contains("TerminalShellPanelView("))
        XCTAssertLessThan(
            workspaceSource.range(of: "TerminalPortForwardPanelView(")!.lowerBound,
            workspaceSource.range(of: "TerminalShellPanelView(")!.lowerBound,
            "Port-forward must stay above the shell panel in the Terminal workspace."
        )
        XCTAssertTrue(workspaceSource.contains("private func terminalHeight"))
        XCTAssertTrue(shellSource.contains("TerminalSessionTabBar("))
        XCTAssertTrue(shellSource.contains("TerminalTranscriptSurface("))
        XCTAssertTrue(transcriptSource.contains("struct TerminalTranscriptSearchBar"))
        XCTAssertTrue(transcriptSource.contains("RuneFindBarChrome(\"Terminal find controls\")"))
        XCTAssertTrue(transcriptSource.contains("RuneIconButton(\"Close find\", systemImage: \"xmark\", action: onClose)"))
        XCTAssertTrue(portForwardSource.contains("@Binding var isExpanded"))
        XCTAssertTrue(portForwardSource.contains("compactStatus"))
        XCTAssertTrue(portForwardSource.contains("expandedControls"))
        XCTAssertTrue(rootViewSource.contains("enum TerminalInspectorTab"))
        XCTAssertTrue(rootViewSource.contains("case commands"))
        XCTAssertTrue(rootViewSource.contains("case logs"))
        XCTAssertTrue(rootViewSource.contains("case yaml"))
        XCTAssertTrue(rootViewSource.contains("selection: $terminalInspectorTab"))
        XCTAssertTrue(rootViewSource.contains("PodLogsInspectorPane("))
        XCTAssertTrue(rootViewSource.contains("TerminalLogTabBar("))
        XCTAssertTrue(rootViewSource.contains("@State private var terminalLogTabState = TerminalPodLogTabState()"))
        XCTAssertTrue(rootViewSource.contains("manifestInspectorPane(activeTab: .yaml)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.focusTerminalPodInspector"))
        XCTAssertTrue(tabBarSource.contains("TerminalTabStrip("))
        XCTAssertTrue(tabBarSource.contains("addAccessibilityLabel: \"New Shell\""))
        XCTAssertTrue(tabStripSource.contains("ScrollView(.horizontal"))
        XCTAssertTrue(sessionControlSource.contains("struct TerminalSessionControlRow"))
        XCTAssertTrue(transcriptSource.contains("struct TerminalTranscriptSurface"))
    }

    func testTerminalShellAndLogTabBarsShareNativeChrome() throws {
        let shellTabSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)
        let logTabSource = try String(contentsOfFile: terminalLogTabBarPath, encoding: .utf8)
        let sharedSource = try String(contentsOfFile: terminalTabStripPath, encoding: .utf8)

        for source in [shellTabSource, logTabSource] {
            XCTAssertTrue(source.contains("TerminalTabStrip("))
            XCTAssertTrue(source.contains(".terminalTabChrome(isActive:"))
            XCTAssertFalse(source.contains("RuneSurfaceBackground(kind: .editor)"))
            XCTAssertFalse(source.contains("private func tabBackground"))
            XCTAssertFalse(source.contains("private func tabBorder"))
        }
        XCTAssertTrue(sharedSource.contains("RuneSurfaceBackground(kind: .inset)"))
        XCTAssertTrue(sharedSource.contains("RuneSurfaceBackground(kind: .listRow(isSelected: isActive))"))
        XCTAssertTrue(sharedSource.contains("RoundedRectangle(cornerRadius: RuneUILayoutMetrics.tabCornerRadius, style: .continuous)"))
        XCTAssertTrue(sharedSource.contains("Capsule()"))
        XCTAssertTrue(sharedSource.contains(".frame(width: 3, height: 16)"))
        XCTAssertTrue(sharedSource.contains(".frame(height: 38)"))
    }

    func testTerminalTranscriptShowsPersistentTrimmedScrollbackIndicator() throws {
        let source = try String(contentsOfFile: terminalTranscriptSurfacePath, encoding: .utf8)

        XCTAssertTrue(TerminalTranscriptSurface.hasTrimmedScrollback(
            TerminalScrollbackRetention.truncationMarker + "\nrecent output"
        ))
        XCTAssertFalse(TerminalTranscriptSurface.hasTrimmedScrollback("recent output"))
        XCTAssertTrue(source.contains("Label(\"Scrollback trimmed\", systemImage: \"scissors\")"))
        XCTAssertTrue(source.contains("Older terminal output was discarded by the scrollback limit"))
        XCTAssertTrue(source.contains("Self.hasTrimmedScrollback(text)"))
    }

    func testTerminalLogsPanelIsExplicitlyPodScoped() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let terminalLogsBlock = try functionBlock(
            named: "private var terminalPodLogsDetails: some View",
            endingBefore: "private var terminalPodYAMLDetails",
            in: rootViewSource
        )

        XCTAssertFalse(terminalLogsBlock.contains("Label(\"Pod logs: \\(pod.namespace)/\\(pod.name)\", systemImage: \"shippingbox\")"))
        XCTAssertFalse(terminalLogsBlock.contains("Terminal logs are scoped to the selected pod, not the deployment."))
        XCTAssertTrue(terminalLogsBlock.contains("PodLogsInspectorPane("))
        XCTAssertTrue(terminalLogsBlock.contains("TerminalLogTabBar("))
        XCTAssertTrue(terminalLogsBlock.contains("activeTabID: terminalLogTabState.activeTabID"))
        XCTAssertTrue(terminalLogsBlock.contains("let podOptions = terminalLogPodOptions()"))
        XCTAssertTrue(terminalLogsBlock.contains("podOptions: podOptions"))
        XCTAssertTrue(terminalLogsBlock.contains("selectedPodID: terminalLogPodSelectionBinding(currentPod: pod, podOptions: podOptions)"))
        XCTAssertTrue(terminalLogsBlock.contains("isFavoritePod: isFavoritePod"))
        XCTAssertTrue(terminalLogsBlock.contains("onToggleFavoritePod: toggleFavoritePod"))
        XCTAssertTrue(terminalLogsBlock.contains("presentationStyle: .terminalCompact"))
        XCTAssertTrue(terminalLogsBlock.contains("showsContainerPicker: false"))
        XCTAssertTrue(terminalLogsBlock.contains("onReload: { reloadActiveTerminalLogPod() }"))
        XCTAssertFalse(terminalLogsBlock.contains("UnifiedResourceLogsInspectorPane"))

        XCTAssertTrue(rootViewSource.contains("private func terminalLogPodOptions() -> [PodSummary]"))
        XCTAssertFalse(rootViewSource.contains(".filter { $0.namespace == namespace }"))
        XCTAssertTrue(rootViewSource.contains("let namespaceOrder = lhs.namespace.localizedCaseInsensitiveCompare(rhs.namespace)"))
        XCTAssertTrue(rootViewSource.contains("private var terminalInitialLogPod: PodSummary?"))
        XCTAssertTrue(rootViewSource.contains("terminalLogTabState.activePod(in: viewModel.state.pods, fallback: terminalInitialLogPod)"))
        XCTAssertTrue(rootViewSource.contains("terminalLogTabState.reconcile(availablePods: viewModel.state.pods, fallbackPod: terminalInitialLogPod)"))
        XCTAssertTrue(rootViewSource.contains("terminalLogTabState.close(id: id, availablePods: viewModel.state.pods, fallbackPod: terminalInitialLogPod)"))
        XCTAssertTrue(rootViewSource.contains("fallbackPod: terminalInitialLogPod"))
        XCTAssertFalse(rootViewSource.contains("fallbackPod: terminalInspectorPod"))
        XCTAssertTrue(rootViewSource.contains("private func terminalLogPodSelectionBinding(currentPod: PodSummary, podOptions: [PodSummary]) -> Binding<String>"))
        XCTAssertTrue(rootViewSource.contains("if availableIDs.contains(terminalLogTabState.selectedPodID)"))
        XCTAssertTrue(rootViewSource.contains("terminalLogTabState.selectedPodID = pod.id"))
        XCTAssertTrue(rootViewSource.contains(".onChange(of: terminalShellPodID) { _, _ in\n            refreshTerminalInspectorForShellPodChangeIfNeeded()"))
        XCTAssertTrue(rootViewSource.contains("private func refreshTerminalInspectorForShellPodChangeIfNeeded()"))
        XCTAssertTrue(rootViewSource.contains("guard terminalInspectorTab == .yaml else { return }"))
        XCTAssertTrue(rootViewSource.contains("private func reloadActiveTerminalLogPod()"))
        XCTAssertTrue(rootViewSource.contains("private func addTerminalLogTab()"))
        XCTAssertTrue(rootViewSource.contains("private func selectTerminalLogTab(_ id: String)"))
        XCTAssertTrue(rootViewSource.contains("private func closeTerminalLogTab(_ id: String)"))
        XCTAssertTrue(rootViewSource.contains("private func isFavoritePod(_ pod: PodSummary) -> Bool"))
        XCTAssertTrue(rootViewSource.contains("viewModel.toggleFavoriteResource(kind: .pod"))
        XCTAssertTrue(rootViewSource.contains("viewModel.focusTerminalPodInspector(pod, reloadLogs: true)"))

        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let terminalFocusBlock = try functionBlock(
            named: "public func focusTerminalPodInspector",
            endingBefore: "private func selectPod",
            in: viewModelSource
        )
        XCTAssertTrue(terminalFocusBlock.contains("selectedLogContainer = reloadLogs ? defaultLogContainerName(for: pod) : \"\""))
        XCTAssertTrue(viewModelSource.contains("private func defaultLogContainerName(for pod: PodSummary) -> String"))
        XCTAssertTrue(viewModelSource.contains("pod.logContainerNames.first ?? \"\""))
    }

    func testPodLogsInspectorPodPickerShowsAllNamespacePods() async throws {
        let pods = [
            PodSummary(name: "api-0", namespace: "default", status: "Running", containerNamesLine: "app"),
            PodSummary(name: "worker-0", namespace: "default", status: "Running", containerNamesLine: "worker"),
            PodSummary(name: "job-0", namespace: "default", status: "Succeeded", containerNamesLine: "job")
        ]
        var selectedPodID = pods[0].id
        let host = NSHostingController(
            rootView: PodLogsInspectorPane(
                selectedLogPreset: .constant(.recentLines),
                includePreviousLogs: .constant(false),
                selectedContainer: .constant(""),
                isTailModeEnabled: .constant(false),
                isStreamPaused: .constant(false),
                isLoadingLogs: false,
                isLoadingResources: false,
                errorMessage: nil,
                statusText: "Tail off - last updated 12:00:00",
                podOptions: pods,
                selectedPodID: Binding(
                    get: { selectedPodID },
                    set: { selectedPodID = $0 }
                ),
                showsContainerPicker: false,
                containerOptions: ["app"],
                logText: "line one\nline two",
                readOnlyResetID: "test:podlogs",
                onReload: {},
                onSave: {},
                onSaveVisibleZip: { _ in },
                onSaveFullZip: {},
                onSaveAllPodsZip: {},
                onCopySelection: {},
                onCopyAll: {},
                onToggleStreamPause: {}
            )
            .frame(width: 760, height: 420)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 760, height: 420),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        try await settle(window: window)
        defer { window.orderOut(nil) }

        XCTAssertFalse(
            allPopUpButtons(in: host.view).contains { popup in
                let titles = Set(popup.itemArray.map(\.title))
                return titles.contains("All containers") || titles.contains("app")
            },
            "Terminal logs should show pod selection only, not a second container dropdown."
        )

        let logsInspectorSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let favoritePodPickerSource = try String(contentsOfFile: favoritePodPickerPath, encoding: .utf8)
        XCTAssertTrue(logsInspectorSource.contains("LogToolbarPickerField(title: t(.pod), role: .pod)"))
        XCTAssertTrue(logsInspectorSource.contains("FavoritePodPicker("))
        XCTAssertTrue(favoritePodPickerSource.contains("ForEach(sortedPods) { pod in"))
        XCTAssertTrue(favoritePodPickerSource.contains("podRow(pod)"))
        XCTAssertTrue(favoritePodPickerSource.contains("selection = pod.id"))
        XCTAssertTrue(favoritePodPickerSource.contains("onToggleFavoritePod(pod)"))
    }

    func testTerminalTabsUseFullTabHitAreaForSelection() throws {
        let tabBarSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)

        XCTAssertTrue(tabBarSource.contains(".frame(width: 216, height: 28"))
        XCTAssertTrue(tabBarSource.contains(".contentShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.tabCornerRadius"))
        XCTAssertTrue(tabBarSource.contains("Button {\n                select(session)"))
        XCTAssertFalse(tabBarSource.contains(".onTapGesture"))
        XCTAssertTrue(tabBarSource.contains("RuneIconButton(\"Close terminal tab\""))
    }

    func testStartupUsesStableLaunchExperienceOverHydratingRootView() throws {
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(rootSource.contains("private var shouldShowLaunchExperience"))
        XCTAssertTrue(rootSource.contains("viewModel.isLaunchExperienceVisible"))
        XCTAssertTrue(rootSource.contains("private var launchExperienceOverlay"))
        XCTAssertTrue(rootSource.contains("private var launchLogo: some View"))
        XCTAssertFalse(rootSource.contains("Bundle.module"))
        XCTAssertTrue(rootSource.contains("private var launchLogoCandidateBundles: [Bundle]"))
        XCTAssertTrue(rootSource.contains("\"Rune_RuneUI.bundle\""))
        XCTAssertTrue(rootSource.contains("bundle.url(forResource: \"rune_logo_main\", withExtension: \"png\")"))
        XCTAssertTrue(rootSource.contains("NSApp.applicationIconImage"))
        XCTAssertTrue(rootSource.contains("Image(nsImage: image)"))
        XCTAssertTrue(rootSource.contains(".frame(width: 112, height: 112)"))
        XCTAssertTrue(rootSource.contains("workspaceChromeMountDelayNanoseconds: UInt64 = 80_000_000"))
        XCTAssertTrue(rootSource.contains(".allowsHitTesting(false)"))
        XCTAssertTrue(rootSource.contains("viewModel.bootstrapIfNeeded()"))
        let launchBlock = try functionBlock(
            named: "private var launchExperienceOverlay",
            endingBefore: "private var configuredMainSplitContainer",
            in: rootSource
        )
        XCTAssertFalse(launchBlock.contains("Text(\"Rune\")"))

        XCTAssertTrue(viewModelSource.contains("@Published public private(set) var isLaunchExperienceVisible = true"))
        XCTAssertTrue(viewModelSource.contains("launchExperienceMinimumNanoseconds: UInt64 = 240_000_000"))
        XCTAssertTrue(viewModelSource.contains("finishLaunchExperience()"))
        XCTAssertLessThan(
            viewModelSource.range(of: "await Task.yield()")!.lowerBound,
            viewModelSource.range(of: "discoverCandidateFiles()")!.lowerBound
        )
        XCTAssertFalse(viewModelSource.contains("isInitialBootstrapRunning"))
    }

    func testAppMenuExposesExplicitDemoResetAction() throws {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let appPath = repoRoot.appendingPathComponent("Sources/RuneApp/RuneApp.swift").path
        let appSource = try String(contentsOfFile: appPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(appSource.contains("Button(\"Reset Demo Cluster\")"))
        XCTAssertTrue(appSource.contains("viewModel.resetDemoCluster()"))
        XCTAssertTrue(viewModelSource.contains("public func resetDemoCluster()"))
    }

    func testTerminalControlsAreScopedToActiveTabAndReserveStableSpace() throws {
        let workspaceSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
        let shellSource = try String(contentsOfFile: terminalShellPanelViewPath, encoding: .utf8)
        let tabBarSource = try String(contentsOfFile: terminalSessionTabBarPath, encoding: .utf8)
        let tabStripSource = try String(contentsOfFile: terminalTabStripPath, encoding: .utf8)
        let portForwardSource = try String(contentsOfFile: terminalPortForwardPanelViewPath, encoding: .utf8)
        let portForwardControlsSource = try String(
            contentsOf: URL(fileURLWithPath: terminalPortForwardPanelViewPath)
                .deletingLastPathComponent()
                .appendingPathComponent("PortForwardControlComponents.swift"),
            encoding: .utf8
        )
        let sessionControlSource = try String(contentsOfFile: terminalSessionControlRowPath, encoding: .utf8)
        let podSelectorSource = try String(contentsOfFile: terminalPodSelectorRowPath, encoding: .utf8)
        let favoritePodPickerSource = try String(contentsOfFile: favoritePodPickerPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(shellSource.contains("private var primaryActionTitle"))
        XCTAssertTrue(shellSource.contains("return \"Disconnect\""))
        XCTAssertTrue(shellSource.contains("return \"Reconnect\""))
        XCTAssertTrue(shellSource.contains("private var primaryActionDisabled"))
        XCTAssertTrue(shellSource.contains("private var primaryActionSystemImage"))
        XCTAssertTrue(shellSource.contains("onReconnectSession(session, selectedPod, containerName)"))
        XCTAssertTrue(shellSource.contains("private var selectedTerminalContainerName"))
        XCTAssertTrue(shellSource.contains("private var containerOptions"))
        XCTAssertTrue(shellSource.contains("containerPicker"))
        XCTAssertTrue(shellSource.contains("onStartSession(pod, selectedTerminalContainerName.isEmpty ? nil : selectedTerminalContainerName)"))
        XCTAssertTrue(shellSource.contains("session.containerName"))
        XCTAssertTrue(viewModelSource.contains("PodTerminalSessionDiagnostic.classify"))
        XCTAssertTrue(viewModelSource.contains("diagnostic.transcriptMessage"))
        XCTAssertTrue(shellSource.contains("let isComposingNewSession: Bool"))
        XCTAssertTrue(shellSource.contains("onComposeNewSession()"))
        XCTAssertTrue(shellSource.contains("handleShellPodSelectionChange"))
        XCTAssertTrue(shellSource.contains("TerminalSessionControlRow("))
        XCTAssertFalse(shellSource.contains("private var actions: some View"))
        XCTAssertFalse(shellSource.contains("actionTitle:"))
        XCTAssertTrue(shellSource.contains("if let matchingSession = sessions.first"))

        XCTAssertTrue(tabBarSource.contains("draftTab(number: sessions.count + 1)"))
        XCTAssertTrue(tabBarSource.contains("primaryTitle: \"\\(number) New Shell\""))
        XCTAssertTrue(tabStripSource.contains("Text(primaryTitle)"))
        XCTAssertTrue(tabBarSource.contains("addAccessibilityLabel: \"New Shell\""))
        XCTAssertTrue(tabBarSource.contains(".terminalTabChrome(isActive: true, emphasis: .draft)"))
        XCTAssertTrue(tabStripSource.contains(".frame(width: 38, height: 28)"))
        XCTAssertTrue(tabStripSource.contains(".contentShape(Rectangle())"))
        XCTAssertTrue(tabStripSource.contains(".frame(height: 38)"))
        XCTAssertTrue(tabStripSource.contains("RuneSurfaceBackground(kind: .listRow(isSelected: isActive))"))

        XCTAssertTrue(sessionControlSource.contains("Label(primaryActionTitle, systemImage: primaryActionSystemImage)"))
        XCTAssertTrue(sessionControlSource.contains(".frame(width: 112)"))
        XCTAssertTrue(sessionControlSource.contains("Button(\"Clear\", action: onClear)"))
        XCTAssertTrue(sessionControlSource.contains(".frame(width: 72)"))
        XCTAssertTrue(sessionControlSource.contains("TerminalPodControlLayout("))
        XCTAssertTrue(sessionControlSource.contains("TerminalPodSelectorControl("))
        XCTAssertFalse(sessionControlSource.contains("private var favoriteButton"))
        XCTAssertFalse(sessionControlSource.contains("\"★ \""))
        XCTAssertFalse(sessionControlSource.contains("ScrollView(.horizontal, showsIndicators: false)"))
        XCTAssertTrue(shellSource.contains("onSaveActiveTranscript"))
        XCTAssertTrue(shellSource.contains("onSaveAllTranscripts"))
        XCTAssertTrue(shellSource.contains("Label(\"Export\", systemImage: \"square.and.arrow.down\")"))
        XCTAssertTrue(shellSource.contains("Button(\"Save Active Transcript\""))
        XCTAssertTrue(shellSource.contains("Button(\"Save Active to Export Folder\""))
        XCTAssertTrue(shellSource.contains("Button(\"Save Active and Open\""))
        XCTAssertTrue(shellSource.contains("Button(\"Save All Transcripts ZIP\""))
        XCTAssertTrue(shellSource.contains("Button(\"Save All ZIP to Export Folder\""))
        XCTAssertTrue(shellSource.contains("Button(\"Save All ZIP and Open\""))
        XCTAssertTrue(workspaceSource.contains("onSaveActiveTerminalTranscript"))
        XCTAssertTrue(workspaceSource.contains("onSaveAllTerminalTranscripts"))
        XCTAssertTrue(workspaceSource.contains("onSaveActiveTerminalTranscriptToExportFolder"))
        XCTAssertTrue(workspaceSource.contains("onSaveAllTerminalTranscriptsToExportFolder"))
        XCTAssertTrue(podSelectorSource.contains("struct TerminalPodControlLayout"))
        XCTAssertTrue(podSelectorSource.contains("RuneAdaptiveToolbar(accessibilityLabel)"))
        XCTAssertTrue(podSelectorSource.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(podSelectorSource.contains("FavoritePodPicker("))
        XCTAssertTrue(podSelectorSource.contains("static let widePickerWidth: CGFloat = 320"))
        XCTAssertTrue(podSelectorSource.contains("static let compactPickerWidth: CGFloat = 280"))
        XCTAssertTrue(podSelectorSource.contains(".frame(width: 104)"))
        XCTAssertFalse(podSelectorSource.contains("private var favoriteButton"))
        XCTAssertFalse(podSelectorSource.contains("\"★ \""))
        XCTAssertTrue(favoritePodPickerSource.contains("struct FavoritePodPickerPresentation"))
        XCTAssertTrue(favoritePodPickerSource.contains("struct FavoritePodPicker"))
        XCTAssertTrue(favoritePodPickerSource.contains("RuneSurfaceBackground(kind: .listRow(isSelected: false))"))
        XCTAssertTrue(favoritePodPickerSource.contains("@State private var isPopoverPresented = false"))
        XCTAssertTrue(favoritePodPickerSource.contains(".popover(isPresented: $isPopoverPresented, arrowEdge: .bottom)"))
        XCTAssertTrue(favoritePodPickerSource.contains("private func podRow(_ pod: PodSummary) -> some View"))
        XCTAssertTrue(favoritePodPickerSource.contains("selection = pod.id"))
        XCTAssertTrue(favoritePodPickerSource.contains("isPopoverPresented = false"))
        XCTAssertTrue(favoritePodPickerSource.contains("onToggleFavoritePod(pod)"))
        XCTAssertFalse(favoritePodPickerSource.contains("Picker(title, selection: $selection)"))
        XCTAssertTrue(favoritePodPickerSource.contains("FavoritePodPickerPresentation.selectedPod(in: pods, selection: selection)"))
        XCTAssertTrue(favoritePodPickerSource.contains("if pod.id == selection { return \"checkmark\" }"))
        XCTAssertTrue(favoritePodPickerSource.contains("pods.first { $0.id == selection }"))
        XCTAssertTrue(favoritePodPickerSource.contains("FavoritePodPickerPresentation.selectedFavoriteIcon("))
        XCTAssertTrue(favoritePodPickerSource.contains("static func selectedFavoriteIcon("))
        XCTAssertTrue(favoritePodPickerSource.contains("let lhsFavorite = isFavoritePod(lhs)"))
        XCTAssertTrue(favoritePodPickerSource.contains("return lhsFavorite && !rhsFavorite"))
        XCTAssertTrue(favoritePodPickerSource.contains("localizedCaseInsensitiveCompare(rhs.name)"))

        XCTAssertTrue(workspaceSource.contains("@State private var isComposingNewShellTab"))
        XCTAssertTrue(workspaceSource.contains("private var isShowingNewShellDraft"))
        XCTAssertTrue(workspaceSource.contains("private func composeNewShellTab()"))

        XCTAssertTrue(workspaceSource.contains("private func terminalHeight(availableHeight: CGFloat, portForwardExpanded: Bool)"))
        XCTAssertFalse(workspaceSource.contains("tabPressure"))
        XCTAssertFalse(workspaceSource.contains("sessionCount"))

        XCTAssertTrue(portForwardSource.contains("compactStatusHeight"))
        XCTAssertTrue(portForwardSource.contains("activeSessionListHeight"))
        XCTAssertTrue(portForwardSource.contains(".frame(minHeight: compactStatusHeight)"))
        XCTAssertTrue(portForwardSource.contains(".frame(height: activeSessionListHeight)"))
        XCTAssertTrue(portForwardSource.contains("PortForwardEndpointFields("))
        XCTAssertTrue(portForwardSource.contains("PortForwardPrimaryActionButton("))
        XCTAssertFalse(portForwardSource.contains("stopButton(session)"))
        XCTAssertTrue(portForwardSource.contains("isFavoritePod: isFavoritePod"))
        XCTAssertTrue(portForwardSource.contains("onToggleFavoritePod: onToggleFavoritePod"))
        XCTAssertTrue(portForwardControlsSource.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertFalse(portForwardControlsSource.contains("ScrollView(.horizontal"))

        XCTAssertTrue(viewModelSource.contains("replacingSessionID: String? = nil"))
        XCTAssertTrue(viewModelSource.contains("guard session.status == .connected else { return }"))
    }

    func testSidePanelViewsAvoidBreakpointWrappingThatCausesVerticalJumps() throws {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let viewsRoot = repoRoot.appendingPathComponent("Sources/RuneUI/Views")
        let enumerator = FileManager.default.enumerator(
            at: viewsRoot,
            includingPropertiesForKeys: nil
        )

        var offenders: [String] = []
        while let url = enumerator?.nextObject() as? URL {
            guard url.pathExtension == "swift" else { continue }
            // These bounded dialogs and explicitly supported compact forms have
            // focused render tests for their fallback geometry.
            let adaptiveStandaloneViews = [
                "AddClusterProviderCredentialField.swift",
                "DeploymentRolloutHistoryView.swift",
                "KubernetesConnectionOnboardingView.swift",
                "RBACCanISimulatorPanel.swift",
                "ResourceLogsInspectorView.swift",
                "RunePreferencesView.swift",
                "TerminalPodSelectorRow.swift",
                "TerminalPortForwardPanelView.swift",
                "PortForwardControlComponents.swift"
            ]
            guard !adaptiveStandaloneViews.contains(url.lastPathComponent) else { continue }
            let source = try String(contentsOf: url, encoding: .utf8)
            if source.contains("ViewThatFits(in: .horizontal)") {
                offenders.append(url.lastPathComponent)
            }
        }

        XCTAssertTrue(
            offenders.isEmpty,
            "Horizontal ViewThatFits creates breakpoint-driven row wrapping in side panels. Use stable fixed rows with horizontal scrolling instead. Offenders: \(offenders.joined(separator: ", "))"
        )
    }

    func testInspectorActionRowsAvoidBreakpointWrappingThatCausesVerticalJumps() throws {
        let source = try String(contentsOfFile: runeDesignComponentsPath, encoding: .utf8)
        let actionRow = try functionBlock(
            named: "struct RuneInspectorActionRow<Content: View>: View {",
            endingBefore: "extension View",
            in: source
        )

        XCTAssertTrue(actionRow.contains("ScrollView(.horizontal, showsIndicators: false)"))
        XCTAssertTrue(actionRow.contains("HStack(alignment: .center"))
        XCTAssertFalse(actionRow.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertFalse(actionRow.contains("VStack(alignment: .leading"))
    }

    func testCustomDialogsUseSharedRegularSizedActions() throws {
        let designSource = try String(contentsOfFile: runeDesignComponentsPath, encoding: .utf8)
        let commandPaletteSource = try String(contentsOfFile: commandPaletteViewPath, encoding: .utf8)
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(designSource.contains("struct RuneDialogActionBar<Actions: View>: View"))
        XCTAssertTrue(designSource.contains(".controlSize(.regular)"))
        XCTAssertTrue(designSource.contains("minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight"))
        XCTAssertTrue(designSource.contains("width: RuneUILayoutMetrics.iconButtonSize"))
        XCTAssertTrue(designSource.contains("height: RuneUILayoutMetrics.iconButtonSize"))
        XCTAssertTrue(designSource.contains(".contentShape(Rectangle())"))
        XCTAssertTrue(designSource.contains(".keyboardShortcut(.cancelAction)"))
        XCTAssertTrue(designSource.contains(".help(helpText)"))
        XCTAssertTrue(designSource.contains(".accessibilityLabel(accessibilityLabel)"))
        XCTAssertTrue(designSource.contains("RuneIconButton(accessibilityLabel, systemImage: \"xmark\", action: action)"))

        XCTAssertTrue(commandPaletteSource.contains("RuneDialogCloseButton(\"Close Command Palette\")"))
        XCTAssertTrue(commandPaletteSource.contains("minWidth: RuneUILayoutMetrics.commandPaletteMinWidth"))
        XCTAssertTrue(commandPaletteSource.contains("idealWidth: RuneUILayoutMetrics.commandPaletteIdealWidth"))
        XCTAssertTrue(commandPaletteSource.contains("maxWidth: RuneUILayoutMetrics.commandPaletteMaxWidth"))
        XCTAssertTrue(commandPaletteSource.contains("minHeight: RuneUILayoutMetrics.commandPaletteMinHeight"))
        XCTAssertTrue(commandPaletteSource.contains("idealHeight: RuneUILayoutMetrics.commandPaletteIdealHeight"))
        XCTAssertTrue(commandPaletteSource.contains("maxHeight: RuneUILayoutMetrics.commandPaletteMaxHeight"))

        XCTAssertTrue(yamlSource.contains("RuneDialogButtonLabel(\"Close\")"))
        XCTAssertTrue(yamlSource.contains(".controlSize(.regular)"))
        XCTAssertTrue(yamlSource.contains("width: RuneUILayoutMetrics.wideDialogWidth"))
        XCTAssertTrue(yamlSource.contains("height: RuneUILayoutMetrics.wideDialogHeight"))
    }

    func testBulkSelectionBarsAvoidBreakpointWrappingThatCausesVerticalJumps() throws {
        let source = try String(contentsOfFile: runeDesignComponentsPath, encoding: .utf8)
        let selectionBar = try functionBlock(
            named: "struct RuneBulkSelectionBar<Actions: View>: View {",
            endingBefore: "private var selectionCountChip",
            in: source
        )

        XCTAssertTrue(selectionBar.contains("ScrollView(.horizontal, showsIndicators: false)"))
        XCTAssertTrue(selectionBar.contains("HStack(spacing: 8)"))
        XCTAssertFalse(selectionBar.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertFalse(selectionBar.contains("VStack(alignment: .leading"))
    }

    func testDeploymentOverviewKeepsPrimaryActionsInOneStableActionRow() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let deploymentActions = try functionBlock(
            named: "private var deploymentInspectorActions: some View",
            endingBefore: "private func isHistoricalDeploymentReplicaSet",
            in: source
        )

        XCTAssertTrue(source.contains("actions: {\n                        deploymentInspectorActions"))
        XCTAssertTrue(deploymentActions.contains("Button(\"Delete\", role: .destructive)"))
        XCTAssertTrue(deploymentActions.contains("Button(\"Restart Rollout\")"))
        XCTAssertTrue(deploymentActions.contains("Button(\"Export Pod YAML ZIP\")"))
    }

    func testServiceOverviewKeepsPrimaryActionsInOneStableActionRow() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let serviceActions = try functionBlock(
            named: "private var serviceInspectorActions: some View",
            endingBefore: "private var eventDetails: some View",
            in: source
        )

        XCTAssertTrue(source.contains("actions: {\n                        serviceInspectorActions"))
        XCTAssertTrue(serviceActions.contains("Button(\"Delete\", role: .destructive)"))
        XCTAssertTrue(serviceActions.contains("Button(appString(.applyYAML))"))
        XCTAssertTrue(serviceActions.contains("Button(\"Export…\")"))
    }

    func testResourceRelationshipNavigationExposesClickableRows() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let deploymentOverview = try functionBlock(
            named: "private func deploymentOverviewSection",
            endingBefore: "private func inspectorEmptyState",
            in: source
        )
        let genericResourceDetails = try functionBlock(
            named: "private func genericResourceDetails",
            endingBefore: "private var yamlManifestDocumentState",
            in: source
        )
        let podOverview = try functionBlock(
            named: "private func podOverviewSection",
            endingBefore: "private func podOverviewRow",
            in: source
        )
        let serviceOverview = try functionBlock(
            named: "private var serviceDetails: some View {",
            endingBefore: "private var eventDetails: some View {",
            in: source
        )
        let replicaSetDetails = try functionBlock(
            named: "private var replicaSetDetails: some View {",
            endingBefore: "private var cronJobInspectorContent: some View {",
            in: source
        )
        let statefulSetDetails = try functionBlock(
            named: "private var statefulSetDetails: some View {",
            endingBefore: "private var daemonSetDetails: some View {",
            in: source
        )
        let daemonSetDetails = try functionBlock(
            named: "private var daemonSetDetails: some View {",
            endingBefore: "private var cronJobInspectorContent: some View {",
            in: source
        )
        let cronJobDetails = try functionBlock(
            named: "private var cronJobInspectorContent: some View {",
            endingBefore: "private var jobDetails: some View {",
            in: source
        )
        let jobDetails = try functionBlock(
            named: "private var jobDetails: some View {",
            endingBefore: "private var horizontalPodAutoscalerDetails: some View {",
            in: source
        )
        let horizontalPodAutoscalerDetails = try functionBlock(
            named: "private var horizontalPodAutoscalerDetails: some View {",
            endingBefore: "private var networkingDetails: some View {",
            in: source
        )
        let ingressDetails = try functionBlock(
            named: "private var ingressDetails: some View {",
            endingBefore: "private var configDetails: some View {",
            in: source
        )
        let pvcDetails = try functionBlock(
            named: "private var persistentVolumeClaimDetails: some View {",
            endingBefore: "private var persistentVolumeDetails: some View {",
            in: source
        )
        let pvDetails = try functionBlock(
            named: "private var persistentVolumeDetails: some View {",
            endingBefore: "private var nodeDetails: some View {",
            in: source
        )
        let nodeDetails = try functionBlock(
            named: "private var nodeDetails: some View {",
            endingBefore: "private var rbacDetails: some View {",
            in: source
        )
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let relationshipSource = try String(contentsOfFile: resourceRelationshipViewsPath, encoding: .utf8)
        let rolloutHistorySource = try String(contentsOfFile: deploymentRolloutHistoryViewPath, encoding: .utf8)

        XCTAssertTrue(deploymentOverview.contains("title: \"Related ReplicaSets\","))
        XCTAssertTrue(deploymentOverview.contains("rowCount: (shouldOfferReplicaSetHistoryLoad ? 1 : visibleReplicaSets.count)"))
        XCTAssertTrue(deploymentOverview.contains("viewModel.openDeploymentRelatedReplicaSet(replicaSet)"))
        XCTAssertTrue(deploymentOverview.contains("showsHistoricalDeploymentReplicaSets"))
        XCTAssertTrue(deploymentOverview.contains("isHistoricalDeploymentReplicaSet"))
        XCTAssertTrue(deploymentOverview.contains("\"Show history\""))
        XCTAssertTrue(deploymentOverview.contains("\"Hide history\""))
        XCTAssertTrue(deploymentOverview.contains("ResourceRelationshipSection(title: \"Related Pods\", rowCount: max(relatedPods.count, 1))"))
        XCTAssertTrue(deploymentOverview.contains("ResourceRelationshipEmptyRow("))
        XCTAssertTrue(deploymentOverview.contains("viewModel.openDeploymentRelatedPod(pod)"))
        XCTAssertTrue(deploymentOverview.contains("RelatedEventsRelationshipSection(events: relatedEvents"))
        XCTAssertTrue(genericResourceDetails.contains("RelatedEventsRelationshipSection(events: relatedEvents"))
        XCTAssertTrue(podOverview.contains("ResourceRelationshipSection(title: \"Scheduled Node\")"))
        XCTAssertTrue(podOverview.contains("viewModel.openPodRelatedNode(node)"))
        XCTAssertTrue(podOverview.contains("RelatedEventsRelationshipSection(events: relatedEvents"))
        XCTAssertTrue(serviceOverview.contains("RelatedPodsRelationshipSection(pods: relatedPods"))
        XCTAssertTrue(serviceOverview.contains("viewModel.openServiceRelatedPod"))
        XCTAssertTrue(serviceOverview.contains("RelatedEventsRelationshipSection(events: relatedEvents"))
        XCTAssertTrue(replicaSetDetails.contains("RelatedPodsRelationshipSection(pods: relatedPods"))
        XCTAssertTrue(replicaSetDetails.contains("viewModel.openReplicaSetRelatedPod"))
        XCTAssertTrue(statefulSetDetails.contains("RelatedPodsRelationshipSection(pods: relatedPods"))
        XCTAssertTrue(statefulSetDetails.contains("viewModel.openStatefulSetRelatedPod"))
        XCTAssertTrue(daemonSetDetails.contains("RelatedPodsRelationshipSection(pods: relatedPods"))
        XCTAssertTrue(daemonSetDetails.contains("viewModel.openDaemonSetRelatedPod"))
        XCTAssertTrue(cronJobDetails.contains("ResourceRelationshipSection(title: \"Related Jobs\", rowCount: relatedJobs.count)"))
        XCTAssertTrue(cronJobDetails.contains("viewModel.openCronJobRelatedJob(job)"))
        XCTAssertTrue(jobDetails.contains("RelatedPodsRelationshipSection(pods: relatedPods"))
        XCTAssertTrue(jobDetails.contains("viewModel.openJobRelatedPod"))
        XCTAssertTrue(horizontalPodAutoscalerDetails.contains("ResourceRelationshipSection(title: \"Scale Target\")"))
        XCTAssertTrue(horizontalPodAutoscalerDetails.contains("viewModel.openHorizontalPodAutoscalerScaleTarget(target)"))
        XCTAssertTrue(ingressDetails.contains("ResourceRelationshipSection(title: \"Related Services\", rowCount: relatedServices.count)"))
        XCTAssertTrue(ingressDetails.contains("viewModel.openIngressRelatedService(service)"))
        XCTAssertTrue(serviceOverview.contains("ResourceRelationshipSection(title: \"Related Ingresses\", rowCount: relatedIngresses.count)"))
        XCTAssertTrue(serviceOverview.contains("viewModel.openServiceRelatedIngress(ingress)"))
        XCTAssertTrue(pvcDetails.contains("ResourceRelationshipSection(title: \"Related PersistentVolume\")"))
        XCTAssertTrue(pvcDetails.contains("viewModel.openPersistentVolumeClaimRelatedPersistentVolume(persistentVolume)"))
        XCTAssertTrue(pvDetails.contains("ResourceRelationshipSection(title: \"Related PVCs\", rowCount: relatedClaims.count)"))
        XCTAssertTrue(pvDetails.contains("viewModel.openPersistentVolumeRelatedPersistentVolumeClaim(pvc)"))
        XCTAssertTrue(nodeDetails.contains("RelatedPodsRelationshipSection(pods: relatedPods"))
        XCTAssertTrue(nodeDetails.contains("viewModel.openNodeRelatedPod"))
        XCTAssertTrue(relationshipSource.contains("struct ResourceRelationshipSection"))
        XCTAssertTrue(relationshipSource.contains("struct RelatedPodsRelationshipSection"))
        XCTAssertTrue(relationshipSource.contains("struct RelatedEventsRelationshipSection"))
        XCTAssertTrue(relationshipSource.contains("private func subtitle(for event: EventSummary) -> String"))
        XCTAssertTrue(relationshipSource.contains("struct ResourceRelationshipLinkButton"))
        XCTAssertTrue(relationshipSource.contains("struct ResourceRelationshipEmptyRow"))
        XCTAssertTrue(source.contains("DeploymentRolloutHistoryView(history: viewModel.state.deploymentRolloutHistory)"))
        XCTAssertTrue(rolloutHistorySource.contains("struct DeploymentRolloutHistoryView"))
        XCTAssertTrue(rolloutHistorySource.contains("DeploymentRolloutHistoryPresentation.rows(from: history)"))
        XCTAssertTrue(rolloutHistorySource.contains("private var header: some View"))
        XCTAssertTrue(rolloutHistorySource.contains("ScrollView(.vertical)"))
        XCTAssertFalse(rolloutHistorySource.contains("ScrollView([.vertical, .horizontal])"))
        XCTAssertTrue(rolloutHistorySource.contains(".frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)"))
        XCTAssertTrue(viewModelSource.contains("public func openDeploymentRelatedReplicaSet"))
        XCTAssertTrue(viewModelSource.contains("public func openPodRelatedNode"))
        XCTAssertTrue(viewModelSource.contains("public var selectedDeploymentRelatedEvents"))
        XCTAssertTrue(viewModelSource.contains("public var selectedServiceRelatedEvents"))
        XCTAssertTrue(viewModelSource.contains("public func relatedEvents(for resource: ClusterResourceSummary)"))
        XCTAssertTrue(viewModelSource.contains("public func openRelatedEvent"))
        XCTAssertTrue(viewModelSource.contains("public func openServiceRelatedPod"))
        XCTAssertTrue(viewModelSource.contains("public func openServiceRelatedIngress"))
        XCTAssertTrue(viewModelSource.contains("public func openReplicaSetRelatedPod"))
        XCTAssertTrue(viewModelSource.contains("public func openStatefulSetRelatedPod"))
        XCTAssertTrue(viewModelSource.contains("public func openDaemonSetRelatedPod"))
        XCTAssertTrue(viewModelSource.contains("public func openJobRelatedPod"))
        XCTAssertTrue(viewModelSource.contains("public func openCronJobRelatedJob"))
        XCTAssertTrue(viewModelSource.contains("public func openHorizontalPodAutoscalerScaleTarget"))
        XCTAssertTrue(viewModelSource.contains("public func openNodeRelatedPod"))
        XCTAssertTrue(viewModelSource.contains("public func openIngressRelatedService"))
        XCTAssertTrue(viewModelSource.contains("public func openPersistentVolumeClaimRelatedPersistentVolume"))
        XCTAssertTrue(viewModelSource.contains("public func openPersistentVolumeRelatedPersistentVolumeClaim"))
    }

    func testResourceListFamiliesShareTopAnchoredLoadingFailureAndEmptyPresentationGate() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let gate = try functionBlock(
            named: "private func resourceListGate<Content: View>(",
            endingBefore: "private var resourceListScopeDescription",
            in: source
        )

        XCTAssertEqual(source.components(separatedBy: "resourceListGate(").count - 1, 7)
        XCTAssertTrue(gate.contains("ResourceListPresentation.project("))
        XCTAssertTrue(gate.contains("isLoading: viewModel.state.isLoading"))
        XCTAssertTrue(gate.contains("freshness: currentResourceListFreshness"))
        XCTAssertTrue(gate.contains("RunePaneContentStateView("))
        XCTAssertTrue(gate.contains("style: .card"))
        XCTAssertTrue(gate.contains("RuneContentStateAction(\"Clear Filter\""))
        XCTAssertTrue(gate.contains("case .retry:"))
        XCTAssertTrue(gate.contains("RuneContentStateAction(\"Retry\""))
        XCTAssertTrue(gate.contains("viewModel.refreshCurrentView(debounced: false)"))
        XCTAssertFalse(gate.contains("alignment: .center"))
    }

    func testInspectorTransientAndEmptyStatesUseTopAnchoredPlainPaneSurface() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let helper = try functionBlock(
            named: "private func inspectorEmptyState(_ state: RuneContentState, symbol: String)",
            endingBefore: "private var namespaceSuggestions",
            in: source
        )

        XCTAssertTrue(helper.contains("RunePaneContentStateView("))
        XCTAssertTrue(helper.contains("style: .plain"))
        XCTAssertTrue(helper.contains("graphicSystemImage: symbol"))
        XCTAssertFalse(helper.contains("alignment: .center"))
    }

    func testResourceFilterClearButtonIsReservedOutsideTextField() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let controls = try functionBlock(
            named: "private var resourceFilterControls: some View {",
            endingBefore: "@ViewBuilder\n    private var sectionSpecificControls",
            in: source
        )

        XCTAssertTrue(controls.contains("RuneIconButton("))
        XCTAssertTrue(controls.contains("systemImage: \"xmark.circle.fill\""))
        XCTAssertTrue(controls.contains("viewModel.setResourceSearchQuery(\"\")"))
        XCTAssertTrue(controls.contains("textInputFocus = .resourceFilter"))
        XCTAssertTrue(controls.contains(".frame(maxWidth: 312, alignment: .leading)"))
        XCTAssertTrue(controls.contains(".opacity(viewModel.state.resourceSearchQuery.isEmpty ? 0 : 1)"))
        XCTAssertTrue(controls.contains("isDisabled: viewModel.state.resourceSearchQuery.isEmpty"))
        XCTAssertFalse(controls.contains(".overlay"))
    }

    func testContentHeaderUsesAdaptiveSemanticMetadataWithoutToolbarDuplication() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let header = try functionBlock(
            named: "private var contentHeader: some View {",
            endingBefore: "private var showsResourceFilterControls",
            in: source
        )

        XCTAssertTrue(header.contains("RuneAdaptiveToolbar(\"Content header\")"))
        XCTAssertTrue(header.contains("contentHeaderStatusStrip"))
        XCTAssertTrue(header.contains("RuneHeaderCapsule("))
        XCTAssertTrue(header.contains("if let freshness = currentResourceListFreshness"))
        XCTAssertTrue(header.contains("ResourceListFreshnessBadge(freshness: freshness)"))
        XCTAssertTrue(header.contains("accessibilityLabel: \"Mode: Read-only\""))
        XCTAssertFalse(header.contains("viewModel.state.selectedContext"))
        XCTAssertFalse(header.contains("viewModel.state.selectedNamespace"))
        XCTAssertFalse(header.contains("refreshCurrentView"))
        XCTAssertFalse(header.contains("Refresh section"))

        let testFile = URL(fileURLWithPath: #filePath)
        let repoRootURL = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let freshnessBadgePath = repoRootURL.appendingPathComponent("Sources/RuneUI/Views/ResourceListFreshnessBadge.swift").path
        let freshnessBadgeSource = try String(contentsOfFile: freshnessBadgePath, encoding: .utf8)
        XCTAssertTrue(freshnessBadgeSource.contains("struct ResourceListFreshnessBadge: View"))
        XCTAssertTrue(freshnessBadgeSource.contains("ResourceListFreshnessPresentation.text(for: freshness.status)"))
        XCTAssertTrue(freshnessBadgeSource.contains("case .reconnecting: return \"Reconnecting\""))
    }

    func testLocalK8sIntegrationReportScriptUsesSandboxSafeSwiftHarness() throws {
        let source = try String(contentsOfFile: localK8sIntegrationReportScriptPath, encoding: .utf8)

        XCTAssertTrue(source.contains("MODULE_CACHE_DIR="))
        XCTAssertTrue(source.contains("CLANG_MODULE_CACHE_PATH"))
        XCTAssertTrue(source.contains("SWIFTPM_MODULECACHE_OVERRIDE"))
        XCTAssertTrue(source.contains("swift build --disable-sandbox --product RuneFakeK8s"))
        XCTAssertTrue(source.contains("swift test --disable-sandbox --filter LocalKubernetesIntegrationTests/testRuneFakeK8sEventsPointAtExistingPods"))
        XCTAssertTrue(source.contains("swift test --disable-sandbox --filter RuneFakeK8sRESTServerTests"))
        XCTAssertTrue(source.contains("docker_compose_terminal_smoke_test"))
        XCTAssertTrue(source.contains("docker_compose_single_context_namespace_test"))
        XCTAssertTrue(source.contains("docker_compose_multi_context_namespace_test"))
        XCTAssertTrue(source.contains("RUNE_LIVE_KUBECONFIG=\"$MERGED_KUBECONFIG\""))
        XCTAssertTrue(source.contains("RUNE_LIVE_K8S_CONTEXT=fake-orbit-mesh"))
        XCTAssertTrue(source.contains("RUNE_LIVE_K8S_CONTEXTS=fake-orbit-mesh,fake-lattice-spark"))
        XCTAssertTrue(source.contains("LocalKubernetesIntegrationTests/testLiveKubeconfigContextListsNamespacesWhenExplicitlyEnabled"))
        XCTAssertTrue(source.contains("LocalKubernetesIntegrationTests/testLiveKubeconfigContextsListNamespacesWhenExplicitlyEnabled"))
        XCTAssertTrue(source.contains("swift test --disable-sandbox --filter RuneDockerComposeViewModelIntegrationTests/testDockerComposeTerminalRightPanelLogWorkflowDoesNotFollowShellPodFallback"))
        XCTAssertTrue(source.contains("swift test --disable-sandbox --filter RuneDockerComposeKubeConfigImportIntegrationTests/testAddClusterImportPublishesAndActivatesBothDockerComposeContexts"))
        XCTAssertTrue(source.contains("swift test --disable-sandbox --filter RuneDockerComposeViewModelIntegrationTests"))
        XCTAssertTrue(source.contains("can_bind_loopback_socket"))
        XCTAssertTrue(source.contains("Skipped because this environment cannot bind local loopback sockets."))
        XCTAssertTrue(source.contains("return \"$exit_code\""))
        XCTAssertTrue(source.contains("DOCKER_READY=1"))
        XCTAssertTrue(source.contains("Skipped because Docker Compose stack did not start."))
        XCTAssertTrue(source.contains("Skipped because Docker Compose stack or kubeconfig safety gate did not pass."))
    }

    func testLocalK8sIntegrationReportScriptForcesFakeClusterOnlyMode() throws {
        let source = try String(contentsOfFile: localK8sIntegrationReportScriptPath, encoding: .utf8)

        XCTAssertTrue(source.contains("unset RUNE_LIVE_K8S_CONTEXT"))
        XCTAssertTrue(source.contains("unset RUNE_LIVE_K8S_CONTEXTS"))
        XCTAssertTrue(source.contains("unset RUNE_LIVE_KUBECONFIG"))
        XCTAssertTrue(source.contains("unset RUNE_LIVE_CLOUD_PROVIDER"))
        XCTAssertTrue(source.contains("export RUNE_ALLOW_LIVE_K8S_TESTS=0"))
        XCTAssertTrue(source.contains("export RUNE_ALLOW_LIVE_CLOUD_TESTS=0"))
    }

    func testLocalK8sIntegrationReportScriptKeepsDockerComposeOptInAndLocalOnly() throws {
        let source = try String(contentsOfFile: localK8sIntegrationReportScriptPath, encoding: .utf8)
        let readme = try String(contentsOfFile: dockerComposeReadmePath, encoding: .utf8)

        XCTAssertTrue(source.contains("SKIP_DOCKER=\"${RUNE_SKIP_DOCKER_FAKE_K8S:-1}\""))
        XCTAssertTrue(source.contains("if [[ \"$SKIP_DOCKER\" == \"1\" ]]"))
        XCTAssertTrue(source.contains("Skipped because RUNE_SKIP_DOCKER_FAKE_K8S defaults to 1."))
        XCTAssertTrue(source.contains("RERUN_COMMAND=\"RUNE_SKIP_DOCKER_FAKE_K8S=1 scripts/run-local-k8s-integration-report.sh\""))
        XCTAssertTrue(source.contains("RERUN_COMMAND=\"RUNE_SKIP_DOCKER_FAKE_K8S=0 RUNE_RESET_DOCKER_FAKE_K8S=$RERUN_RESET_DOCKER scripts/run-local-k8s-integration-report.sh\""))
        XCTAssertTrue(source.contains("$RERUN_COMMAND"))
        XCTAssertTrue(source.contains("safe_docker_kubeconfig_check"))
        XCTAssertTrue(source.contains("grep -q 'name: fake-orbit-mesh'"))
        XCTAssertTrue(source.contains("grep -q 'name: fake-lattice-spark'"))
        XCTAssertTrue(source.contains("grep -q 'server: https://127.0.0.1:16443'"))
        XCTAssertTrue(source.contains("grep -q 'server: https://127.0.0.1:17443'"))
        XCTAssertTrue(readme.contains("The default report run exercises the script and REST fake-cluster suites without starting Docker Compose."))
        XCTAssertTrue(readme.contains("RUNE_SKIP_DOCKER_FAKE_K8S=0 RUNE_RESET_DOCKER_FAKE_K8S=1 scripts/run-local-k8s-integration-report.sh"))
        XCTAssertFalse(readme.contains("resets only the local `rune-fake-k8s` Docker Compose project by default"))
    }

    func testDockerComposeLogMatrixPodEmitsFastDeterministicLogs() throws {
        let source = try String(contentsOfFile: dockerComposeOrbitBootstrapManifestPath, encoding: .utf8)

        XCTAssertTrue(source.contains("name: alpha-log-matrix"))
        XCTAssertTrue(source.contains("kubectl.kubernetes.io/default-container: main"))
        XCTAssertTrue(source.contains("echo \"alpha-log-matrix main tick index=${index}\""))
        XCTAssertTrue(source.contains("echo \"alpha-log-matrix sidecar tick index=${index}\""))
        XCTAssertTrue(source.contains("sleep 2"))
        XCTAssertFalse(source.contains("echo \"alpha-log-matrix main tick\"\n            sleep 20"))
        XCTAssertFalse(source.contains("echo \"alpha-log-matrix sidecar tick\"\n            sleep 20"))
    }

    func testLocalK8sIntegrationReportWritesSectionedMarkdownWithoutTables() throws {
        let source = try String(contentsOfFile: localK8sIntegrationReportScriptPath, encoding: .utf8)

        XCTAssertFalse(source.contains("| Step |"))
        XCTAssertFalse(source.contains("| --- |"))
        XCTAssertFalse(source.contains("printf '| `%s`"))
        XCTAssertTrue(source.contains("### `%s`"))
    }

    func testTerminalTranscriptSupportsHorizontalScrollAndFindOverlay() throws {
        let source = try String(contentsOfFile: terminalTranscriptSurfacePath, encoding: .utf8)

        XCTAssertTrue(source.contains("scrollView.hasHorizontalScroller = true"))
        XCTAssertTrue(source.contains("textView.isHorizontallyResizable = true"))
        XCTAssertTrue(source.contains("container.widthTracksTextView = false"))
        XCTAssertTrue(source.contains("CGFloat.greatestFiniteMagnitude"))

        XCTAssertTrue(source.contains("@State private var isSearchVisible"))
        XCTAssertTrue(source.contains("TextField(\"Find in terminal\""))
        XCTAssertTrue(source.contains("TerminalTranscriptSearchIndex"))
        XCTAssertTrue(source.contains("selectedSearchMatchIndex"))
        XCTAssertTrue(source.contains("let fontSize: CGFloat"))
        XCTAssertTrue(source.contains("NSFont.monospacedSystemFont(ofSize: fontSize"))
        XCTAssertTrue(source.contains("updateFontIfNeeded"))
        XCTAssertTrue(source.contains("RuneIconButton(\"Find in terminal\", systemImage: \"magnifyingglass\")"))
        XCTAssertTrue(source.contains("NSColor.systemYellow.withAlphaComponent"))
        XCTAssertTrue(source.contains("textView.scrollRangeToVisible(activeRange)"))
        XCTAssertTrue(source.contains("TerminalSearchCursorModifier"))
        XCTAssertTrue(source.contains(".terminalSearchCursor(.arrow)"))
        XCTAssertFalse(source.contains(".terminalSearchCursor(.iBeam)"))
        XCTAssertTrue(source.contains("cursor.push()"))
    }

    func testTerminalTranscriptSupportsNativeClipboardAndContextMenu() throws {
        let source = try String(contentsOfFile: terminalTranscriptSurfacePath, encoding: .utf8)

        XCTAssertTrue(source.contains("final class TerminalTranscriptInteractionTextView: NSTextView"))
        XCTAssertTrue(source.contains("override func performKeyEquivalent(with event: NSEvent) -> Bool"))
        XCTAssertTrue(source.contains("case \"a\":"))
        XCTAssertTrue(source.contains("case \"c\":"))
        XCTAssertTrue(source.contains("case \"v\":"))
        XCTAssertTrue(source.contains("NSPasteboard.general.string(forType: .string)"))
        XCTAssertTrue(source.contains("onPasteText?(paste)"))
        XCTAssertTrue(source.contains("override func paste(_ sender: Any?)"))
        XCTAssertTrue(source.contains("override func menu(for event: NSEvent) -> NSMenu?"))
        XCTAssertTrue(source.contains("NSMenuItem(title: \"Copy\""))
        XCTAssertTrue(source.contains("NSMenuItem(title: \"Paste to Prompt\""))
        XCTAssertTrue(source.contains("NSMenuItem(title: \"Select All\""))
        XCTAssertTrue(source.contains("NSMenuItem(title: \"Clear Selection\""))
        XCTAssertTrue(source.contains(".keyboardShortcut(\"f\", modifiers: [.command])"))
    }

    func testTerminalPromptUsesNativeFocusPasteAndSendHistory() throws {
        let shellSource = try String(contentsOfFile: terminalShellPanelViewPath, encoding: .utf8)
        let sessionControlSource = try String(contentsOfFile: terminalSessionControlRowPath, encoding: .utf8)

        XCTAssertTrue(shellSource.contains("@State private var isInputFocused = false"))
        XCTAssertTrue(shellSource.contains("@State private var commandHistory: [String] = []"))
        XCTAssertTrue(shellSource.contains("onPasteText: pasteIntoPrompt"))
        XCTAssertTrue(shellSource.contains("TerminalPromptTextEditor("))
        XCTAssertTrue(shellSource.contains("enum TerminalPromptPalette"))
        XCTAssertTrue(shellSource.contains("TerminalPromptPalette.inputTextColor"))
        XCTAssertTrue(shellSource.contains("TerminalPromptPalette.disabledInputTextColor"))
        XCTAssertTrue(shellSource.contains("NSColor { .labelColor }"))
        XCTAssertTrue(shellSource.contains("NSColor { .disabledControlTextColor }"))
        XCTAssertTrue(shellSource.contains("NSColor { .selectedTextColor }"))
        XCTAssertTrue(shellSource.contains("NSColor { .selectedTextBackgroundColor }"))
        XCTAssertTrue(shellSource.contains("TerminalPromptLayoutMetrics(fontSize: fontSize)"))
        XCTAssertTrue(shellSource.contains("height: terminalPromptLayout.controlHeight"))
        XCTAssertTrue(shellSource.contains("height: layout.textContainerHeight"))
        XCTAssertTrue(shellSource.contains("textView.typingAttributes"))
        XCTAssertTrue(shellSource.contains("textView.selectedTextAttributes"))
        XCTAssertTrue(shellSource.contains("onSubmit: sendPrompt"))
        XCTAssertTrue(shellSource.contains("onHistoryUp: recallPreviousCommand"))
        XCTAssertTrue(shellSource.contains("onHistoryDown: recallNextCommand"))
        XCTAssertTrue(shellSource.contains("private func pasteIntoPrompt(_ text: String)"))
        XCTAssertTrue(shellSource.contains("TerminalShellPanelView.normalizedTerminalPasteForPrompt(text)"))
        XCTAssertTrue(shellSource.contains("pendingMultilinePasteConfirmation = paste.requiresConfirmation"))
        XCTAssertTrue(shellSource.contains("pendingMultilinePasteConfirmation = false"))
        XCTAssertTrue(shellSource.contains("multilinePasteConfirmationNote"))
        XCTAssertTrue(shellSource.contains("private func rememberCommand(_ command: String)"))
        XCTAssertTrue(shellSource.contains("private func recallPreviousCommand()"))
        XCTAssertTrue(shellSource.contains("private func recallNextCommand()"))
        XCTAssertTrue(sessionControlSource.contains(".keyboardShortcut(\"k\", modifiers: [.command])"))
    }

    func testTerminalMultilinePasteRequiresExplicitSecondSend() {
        let singleLine = TerminalShellPanelView.normalizedTerminalPasteForPrompt("kubectl version\r\n")
        XCTAssertEqual(singleLine.text, "kubectl version")
        XCTAssertFalse(singleLine.requiresConfirmation)

        let multiLine = TerminalShellPanelView.normalizedTerminalPasteForPrompt("echo one\r\necho two\n")
        XCTAssertEqual(multiLine.text, "echo one\necho two")
        XCTAssertTrue(multiLine.requiresConfirmation)
    }

    func testTerminalPromptSendPreparationStagesMultilinePasteAndTrimsInput() {
        let pendingMultiline = TerminalShellPanelView.preparedTerminalPromptSend(
            input: " echo one\necho two\n",
            pendingMultilinePasteConfirmation: true
        )
        XCTAssertEqual(pendingMultiline.command, "echo one\necho two")
        XCTAssertFalse(pendingMultiline.shouldSend)
        XCTAssertTrue(pendingMultiline.shouldClearConfirmation)
        XCTAssertTrue(pendingMultiline.shouldRefocusPrompt)

        let command = TerminalShellPanelView.preparedTerminalPromptSend(
            input: " kubectl get pods \n",
            pendingMultilinePasteConfirmation: false
        )
        XCTAssertEqual(command.command, "kubectl get pods")
        XCTAssertTrue(command.shouldSend)
        XCTAssertTrue(command.shouldClearConfirmation)
        XCTAssertFalse(command.shouldRefocusPrompt)

        let empty = TerminalShellPanelView.preparedTerminalPromptSend(
            input: " \n",
            pendingMultilinePasteConfirmation: false
        )
        XCTAssertEqual(empty.command, "")
        XCTAssertFalse(empty.shouldSend)
    }

    func testTerminalPromptHandlesStandardControlSequences() throws {
        let shellSource = try String(contentsOfFile: terminalShellPanelViewPath, encoding: .utf8)
        let workspaceSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(shellSource.contains("private final class TerminalPromptTextView: NSTextView"))
        XCTAssertTrue(shellSource.contains("override func keyDown(with event: NSEvent)"))
        XCTAssertTrue(shellSource.contains("override func performKeyEquivalent(with event: NSEvent) -> Bool"))
        XCTAssertTrue(shellSource.contains("case \"c\":"))
        XCTAssertTrue(shellSource.contains("onSendControlSequence?(\"\\u{3}\")"))
        XCTAssertTrue(shellSource.contains("case \"d\":"))
        XCTAssertTrue(shellSource.contains("onSendControlSequence?(\"\\u{4}\")"))
        XCTAssertTrue(shellSource.contains("case \"l\":"))
        XCTAssertTrue(shellSource.contains("onSendControlSequence?(\"\\u{c}\")"))
        XCTAssertTrue(shellSource.contains("case \"u\":"))
        XCTAssertTrue(shellSource.contains("case \"w\":"))
        XCTAssertTrue(shellSource.contains("case \"a\":"))
        XCTAssertTrue(shellSource.contains("case \"e\":"))
        XCTAssertTrue(shellSource.contains("case 126:"))
        XCTAssertTrue(shellSource.contains("case 125:"))
        XCTAssertTrue(workspaceSource.contains("let onSendControlSequence: (String) -> Void"))
        XCTAssertTrue(rootViewSource.contains("viewModel.sendTerminalControlSequence(text)"))
        XCTAssertTrue(viewModelSource.contains("public func sendTerminalControlSequence(_ text: String)"))
        XCTAssertTrue(viewModelSource.contains("try await kubeClient.writeToPodTerminalSession(id: session.id, text: text)"))
    }

    func testTerminalSurfaceReportsStableGridSizeForResizePropagation() throws {
        let shellSource = try String(contentsOfFile: terminalShellPanelViewPath, encoding: .utf8)
        let workspaceSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let kubeClientSource = try String(contentsOfFile: kubernetesClientPath, encoding: .utf8)

        let grid = TerminalTranscriptSurface.terminalGridSize(
            surfaceSize: CGSize(width: 824, height: 342),
            fontSize: 13
        )

        XCTAssertEqual(grid.columns, 99)
        XCTAssertEqual(grid.rows, 18)
        XCTAssertTrue(shellSource.contains("onResizeSession(session.id, columns, rows)"))
        XCTAssertTrue(workspaceSource.contains("let onResizeSession: (String, Int, Int) -> Void"))
        XCTAssertTrue(rootViewSource.contains("viewModel.resizeTerminalSession(id: id, columns: columns, rows: rows)"))
        XCTAssertTrue(viewModelSource.contains("public func resizeTerminalSession(id: String, columns: Int, rows: Int)"))
        XCTAssertTrue(kubeClientSource.contains("public func resizePodTerminalSession(id: String, columns: Int, rows: Int) async throws"))
    }

    func testTerminalAndPortForwardExposeCopyKubectlCommands() throws {
        let terminalViewSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
        let portForwardSource = try String(contentsOfFile: terminalPortForwardPanelViewPath, encoding: .utf8)
        let builderSource = try String(contentsOfFile: terminalKubectlCommandBuilderPath, encoding: .utf8)

        XCTAssertTrue(terminalViewSource.contains("copySuggestedCommand(command)"))
        XCTAssertTrue(terminalViewSource.contains("TerminalKubectlCommandBuilder.exec("))
        XCTAssertTrue(portForwardSource.contains("copyDraftKubectlButton"))
        XCTAssertTrue(portForwardSource.contains("copyPortForwardCommandButton(session)"))
        XCTAssertTrue(portForwardSource.contains("TerminalKubectlCommandBuilder.portForward("))
        XCTAssertTrue(builderSource.contains("enum TerminalKubectlCommandBuilder"))
        XCTAssertTrue(builderSource.contains("static func copyToPasteboard"))
        XCTAssertTrue(builderSource.contains("kubectl"))
        XCTAssertTrue(builderSource.contains("port-forward"))
        XCTAssertTrue(builderSource.contains("exec"))
        XCTAssertTrue(builderSource.contains("ShellCommandFormatting.shellCommand(parts)"))

        XCTAssertEqual(
            TerminalKubectlCommandBuilder.exec(
                contextName: "dev",
                namespace: "default",
                podName: "api",
                command: "printenv | sort"
            ),
            "kubectl --context dev --namespace default exec -it api -- sh -lc 'printenv | sort'"
        )
        XCTAssertEqual(
            TerminalKubectlCommandBuilder.exec(
                contextName: "dev",
                namespace: "default",
                podName: "api",
                containerName: "sidecar",
                command: "printenv | sort"
            ),
            "kubectl --context dev --namespace default exec -it api --container sidecar -- sh -lc 'printenv | sort'"
        )
        XCTAssertEqual(
            TerminalKubectlCommandBuilder.portForward(
                contextName: "dev",
                namespace: "default",
                targetKind: .pod,
                targetName: "api",
                localPort: 8080,
                remotePort: 80,
                address: "127.0.0.1"
            ),
            "kubectl --context dev --namespace default port-forward --address 127.0.0.1 pod/api 8080:80"
        )
        XCTAssertEqual(
            TerminalKubectlCommandBuilder.portForward(
                contextName: "dev",
                namespace: "default",
                targetKind: .pod,
                targetName: "api;debug",
                localPort: 8080,
                remotePort: 80,
                address: "127.0.0.1"
            ),
            "kubectl --context dev --namespace default port-forward --address 127.0.0.1 'pod/api;debug' 8080:80"
        )
        XCTAssertEqual(
            TerminalKubectlCommandBuilder.exec(
                contextName: " prod west ",
                namespace: " team alpha ",
                podName: "api;debug",
                containerName: "side car",
                command: "printf 'hello'; env | sort"
            ),
            "kubectl --context 'prod west' --namespace 'team alpha' exec -it 'api;debug' --container 'side car' -- sh -lc 'printf '\\''hello'\\''; env | sort'"
        )
        XCTAssertEqual(
            TerminalKubectlCommandBuilder.portForward(
                contextName: nil,
                namespace: " ",
                targetKind: .service,
                targetName: "api service",
                localPort: "127.0.0.1:18080",
                remotePort: "http",
                address: "::1"
            ),
            "kubectl port-forward --address ::1 'service/api service' 127.0.0.1:18080:http"
        )
    }

    func testTerminalAndPortForwardLifetimeStopsBackingSessions() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let kubeClientSource = try String(contentsOfFile: kubernetesClientPath, encoding: .utf8)

        XCTAssertTrue(viewModelSource.contains("public func closeTerminalSession(id: String)"))
        XCTAssertTrue(viewModelSource.contains("state.selectTerminalSession(id: id)"))
        XCTAssertTrue(viewModelSource.contains("stopTerminalSession(resetState: true)"))
        XCTAssertTrue(viewModelSource.contains("await kubeClient.stopPodTerminalSession(id: sessionID)"))
        XCTAssertTrue(viewModelSource.contains("public func stopPortForward(_ session: PortForwardSession)"))
        XCTAssertTrue(viewModelSource.contains("await kubeClient.stopPortForward(sessionID: session.id)"))
        XCTAssertTrue(viewModelSource.contains("public func clearPortForwardSession(_ session: PortForwardSession)"))
        XCTAssertTrue(viewModelSource.contains("public func clearInactivePortForwardSessions()"))
        XCTAssertTrue(viewModelSource.contains("public func clearInactivePortForwardSessions(targetKind: PortForwardTargetKind, targetName: String, namespace: String)"))
        XCTAssertTrue(viewModelSource.contains("let existing = state.terminalSessions.first(where:"))
        XCTAssertTrue(viewModelSource.contains("state.selectTerminalSession(id: existing.id)"))

        XCTAssertTrue(kubeClientSource.contains("public func stopPodTerminalSession(id: String) async"))
        XCTAssertTrue(kubeClientSource.contains("terminalSessionRegistry.remove(id: id)"))
        XCTAssertTrue(kubeClientSource.contains("handle?.terminate()"))
        XCTAssertTrue(kubeClientSource.contains("public func stopPortForward(sessionID: String) async"))
        XCTAssertTrue(kubeClientSource.contains("portForwardRegistry.remove(id: sessionID)"))
    }

    func testTerminalNewShellDefaultsAvoidDuplicatePodSessions() throws {
        let workspaceSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
        let shellSource = try String(contentsOfFile: terminalShellPanelViewPath, encoding: .utf8)

        XCTAssertTrue(workspaceSource.contains("preferredPodIDForNewShell"))
        XCTAssertTrue(workspaceSource.contains("TerminalShellPodSelectionPolicy.preferredPodIDForNewShell("))
        XCTAssertTrue(workspaceSource.contains("!hasShellSession(for: current, in: sessions)"))
        XCTAssertTrue(workspaceSource.contains("static func hasShellSession(for pod: PodSummary, in sessions: [PodTerminalSession]) -> Bool"))

        XCTAssertTrue(shellSource.contains("private var selectedPodExistingSession"))
        XCTAssertTrue(shellSource.contains("return selectedPodExistingSession == nil ? \"Connect\" : \"Open Tab\""))
        XCTAssertTrue(shellSource.contains("onSelectSession(existing.id)"))
    }

    func testPreferencesExposeTerminalFontZoomSetting() throws {
        let testFile = URL(fileURLWithPath: #filePath)
        let root = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let preferencesPath = root.appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift").path
        let settingsPath = root.appendingPathComponent("Sources/RuneCore/RuneSettingsKeys.swift").path
        let preferencesSource = try String(contentsOfFile: preferencesPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: settingsPath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("terminalFontSize"))
        XCTAssertTrue(settingsSource.contains("terminalFontSizeDefault = 12.0"))
        XCTAssertTrue(settingsSource.contains("terminalFontSizeMinimum = 10.0"))
        XCTAssertTrue(settingsSource.contains("terminalFontSizeMaximum = 20.0"))
        XCTAssertTrue(settingsSource.contains("clampedTerminalFontSize"))

        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.terminalFontSize)"))
        XCTAssertTrue(preferencesSource.contains("settingsSection(settingsString(.settingsAppearance))"))
        XCTAssertTrue(preferencesSource.contains("Text(settingsString(.settingsFontSize))"))
        XCTAssertTrue(preferencesSource.contains("Slider("))
        XCTAssertTrue(preferencesSource.contains("Button(settingsString(.settingsReset))"))
        XCTAssertTrue(preferencesSource.contains("terminalFontSize = RuneSettingsKeys.terminalFontSizeDefault"))
    }

    func testPreferencesExposeMemoryCacheLimits() throws {
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)
        let stateSource = try String(contentsOfFile: runeAppStatePath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("sessionLogCacheEntryLimit"))
        XCTAssertTrue(settingsSource.contains("sessionLogCacheEntryLimitDefault = 128"))
        XCTAssertTrue(settingsSource.contains("resourceYAMLUndoSnapshotLimit"))
        XCTAssertTrue(settingsSource.contains("resourceYAMLUndoSnapshotLimitDefault = 64"))
        XCTAssertTrue(settingsSource.contains("clampedSessionLogCacheEntryLimit"))
        XCTAssertTrue(settingsSource.contains("clampedResourceYAMLUndoSnapshotLimit"))
        XCTAssertFalse(settingsSource.contains("sessionLogCacheEntryLimitMaximum"))
        XCTAssertFalse(settingsSource.contains("resourceYAMLUndoSnapshotLimitMaximum"))

        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.sessionLogCacheEntryLimit)"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.resourceYAMLUndoSnapshotLimit)"))
        XCTAssertTrue(preferencesSource.contains("settingsSection(\"Memory\")"))
        XCTAssertTrue(preferencesSource.contains("private struct RuneSettingsIntegerLimitEditor"))
        XCTAssertTrue(preferencesSource.contains("title: \"Log cache\""))
        XCTAssertTrue(preferencesSource.contains("title: \"YAML undo\""))
        XCTAssertTrue(preferencesSource.contains("TextField("))
        XCTAssertTrue(preferencesSource.contains("Stepper(title, value: normalizedBinding, step: step)"))
        XCTAssertTrue(preferencesSource.contains("Type any larger value for high-memory machines."))
        XCTAssertTrue(preferencesSource.contains("defaultValue: RuneSettingsKeys.sessionLogCacheEntryLimitDefault"))
        XCTAssertTrue(preferencesSource.contains("defaultValue: RuneSettingsKeys.resourceYAMLUndoSnapshotLimitDefault"))
        XCTAssertTrue(preferencesSource.contains("normalize: RuneSettingsKeys.clampedSessionLogCacheEntryLimit"))
        XCTAssertTrue(preferencesSource.contains("normalize: RuneSettingsKeys.clampedResourceYAMLUndoSnapshotLimit"))
        XCTAssertTrue(preferencesSource.contains("value.wrappedValue = defaultValue"))
        XCTAssertFalse(preferencesSource.contains("sessionLogCacheEntryLimitMinimum...RuneSettingsKeys.sessionLogCacheEntryLimitMaximum"))
        XCTAssertFalse(preferencesSource.contains("resourceYAMLUndoSnapshotLimitMinimum...RuneSettingsKeys.resourceYAMLUndoSnapshotLimitMaximum"))

        XCTAssertTrue(stateSource.contains("UserDefaults.standard.runeSessionLogCacheEntryLimit"))
        XCTAssertTrue(stateSource.contains("UserDefaults.standard.runeResourceYAMLUndoSnapshotLimit"))
    }

    func testPreferencesExposeAppearanceThemes() throws {
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let designSource = try String(contentsOfFile: runeDesignComponentsPath, encoding: .utf8)
        let glassSource = try String(contentsOfFile: runeGlassShellPath, encoding: .utf8)
        let themeSource = try String(contentsOfFile: runeThemePalettePath, encoding: .utf8)
        let themePresentationSource = try String(contentsOfFile: runeThemePresentationPath, encoding: .utf8)
        let manifestTextSource = try String(contentsOfFile: appKitManifestTextViewPath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("appearanceTheme"))
        XCTAssertTrue(settingsSource.contains("appearanceThemeDefault = \"native\""))
        XCTAssertTrue(settingsSource.contains("appearanceRecentThemes"))
        XCTAssertTrue(settingsSource.contains("appearanceRecentThemeLimit = 12"))
        XCTAssertTrue(settingsSource.contains("recordRuneAppearanceTheme"))
        XCTAssertTrue(settingsSource.contains("normalizedAppearanceRecentThemes"))

        XCTAssertTrue(themeSource.contains("enum RuneAppearanceTheme"))
        XCTAssertTrue(themeSource.contains("case aurora"))
        XCTAssertTrue(themeSource.contains("case graphiteBlue"))
        XCTAssertTrue(themeSource.contains("case emberGlass"))
        XCTAssertTrue(themeSource.contains("case mossTerminal"))
        XCTAssertTrue(themeSource.contains("case fjord"))
        XCTAssertTrue(themeSource.contains("case paper"))
        XCTAssertTrue(themeSource.contains("case daylight"))
        XCTAssertTrue(themeSource.contains("case contrastDark"))
        XCTAssertTrue(themeSource.contains("case contrastLight"))
        XCTAssertTrue(themeSource.contains("Contrast Dark"))
        XCTAssertTrue(themeSource.contains("Contrast Light"))
        XCTAssertTrue(themeSource.contains("Rune theme"))
        XCTAssertTrue(themeSource.contains("RuneZedThemeDecoder"))
        XCTAssertTrue(themeSource.contains("Custom theme"))
        XCTAssertFalse(themeSource.contains("Zed-format JSON"))
        XCTAssertFalse(themeSource.contains("VS Code"))
        XCTAssertTrue(preferencesSource.contains("case themes"))
        XCTAssertTrue(preferencesSource.contains("Label(PreferencesPane.themes.title(settingsString), systemImage: PreferencesPane.themes.symbol)"))
        XCTAssertTrue(preferencesSource.contains("private var themesSettingsForm: some View"))
        XCTAssertTrue(preferencesSource.contains("RuneThemeSelectorCard"))
        XCTAssertTrue(preferencesSource.contains("settingsSection(\"Choose theme\")"))
        XCTAssertTrue(preferencesSource.contains("Text(\"Recent\")"))
        XCTAssertTrue(preferencesSource.contains("Text(\"Current: \\(selectedAppearanceTheme.title)\")"))
        XCTAssertTrue(preferencesSource.contains("private var recentAppearanceThemes: [RuneResolvedTheme]"))
        XCTAssertTrue(preferencesSource.contains("private var olderAppearanceThemes: [RuneResolvedTheme]"))
        XCTAssertTrue(preferencesSource.contains("private var themeOverflowMenu: some View"))
        XCTAssertTrue(preferencesSource.contains("Text(\"More Themes\")"))
        XCTAssertTrue(preferencesSource.contains("private var swatches: some View"))
        XCTAssertTrue(preferencesSource.contains("swatch(palette.accent)"))
        XCTAssertTrue(preferencesSource.contains("selectAppearanceTheme(theme.id)"))
        XCTAssertTrue(preferencesSource.contains("private func reloadAppearanceThemes()"))
        XCTAssertTrue(preferencesSource.contains("if !availableIDs.contains(appearanceThemeRaw)"))
        XCTAssertTrue(preferencesSource.contains("selectAppearanceTheme(RuneSettingsKeys.appearanceThemeDefault)"))
        XCTAssertTrue(preferencesSource.contains("RuneThemePresentation(theme: theme)"))
        XCTAssertTrue(preferencesSource.contains("RuneThemePresentation(theme: theme).menuSymbol"))
        XCTAssertTrue(themePresentationSource.contains("struct RuneThemePresentation"))
        XCTAssertTrue(themePresentationSource.contains("appearanceSymbol = \"circle.lefthalf.filled\""))
        XCTAssertTrue(themePresentationSource.contains("menuSymbol = \"circle.lefthalf.filled\""))
        XCTAssertTrue(preferencesSource.contains("UserDefaults.standard.recordRuneAppearanceTheme"))
        XCTAssertTrue(preferencesSource.contains("refreshRecentAppearanceThemes(recordSelectedTheme: true)"))
        XCTAssertFalse(preferencesSource.contains("RuneThemePreview"))
        XCTAssertTrue(preferencesSource.contains("Open Folder"))
        XCTAssertTrue(preferencesSource.contains("Reload"))
        XCTAssertTrue(preferencesSource.contains("Label(\"Template\", systemImage: \"doc.badge.plus\")"))
        XCTAssertTrue(preferencesSource.contains("SettingsHelpButton("))
        XCTAssertTrue(preferencesSource.contains("private func revealThemeTemplate()"))
        XCTAssertTrue(preferencesSource.contains("RuneThemeCatalog.writeUserThemeTemplate()"))
        XCTAssertTrue(themeSource.contains("static func writeUserThemeTemplate() -> URL"))
        XCTAssertTrue(themeSource.contains("rune-theme-template.json"))
        XCTAssertTrue(preferencesSource.contains("RuneThemeCatalog.availableThemes()"))
        XCTAssertTrue(preferencesSource.contains("RuneSettingsMetrics.rowMinHeight"))
        XCTAssertFalse(preferencesSource.contains("Drop compatible theme files"))
        XCTAssertFalse(preferencesSource.contains("Zed JSON themes"))
        XCTAssertFalse(preferencesSource.contains("Zed-format custom themes"))
        XCTAssertFalse(preferencesSource.contains("VS Code"))

        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.appearanceTheme)"))
        XCTAssertTrue(preferencesSource.contains("Menu {"))
        XCTAssertTrue(preferencesSource.contains("appearanceThemeRaw = themeID"))
        XCTAssertTrue(preferencesSource.contains("chevron.up.chevron.down"))
        XCTAssertTrue(themeSource.contains("RuneAppearanceTheme.allCases.map(\\.resolvedTheme) + userThemes()"))
        XCTAssertTrue(preferencesSource.contains(".runeAppearanceTheme(selectedAppearanceTheme)"))

        XCTAssertTrue(rootSource.contains("@AppStorage(RuneSettingsKeys.appearanceTheme)"))
        XCTAssertTrue(rootSource.contains(".runeAppearanceTheme(activeAppearanceTheme)"))

        XCTAssertTrue(themeSource.contains("RuneThemePaletteEnvironmentKey"))
        XCTAssertTrue(themeSource.contains("RuneResolvedThemeEnvironmentKey"))
        XCTAssertTrue(themeSource.contains("runeThemePalette"))
        XCTAssertTrue(themeSource.contains("runeResolvedTheme"))
        XCTAssertTrue(themeSource.contains("RuneAppearanceWindowConfigurator"))
        XCTAssertTrue(themeSource.contains("RuneAppearanceWindowConfigurator(theme: theme).id(theme.id)"))
        XCTAssertTrue(themeSource.contains("invalidateWindowChrome(window)"))
        XCTAssertTrue(themeSource.contains("window.viewsNeedDisplay = true"))
        XCTAssertTrue(themeSource.contains("markNeedsDisplay(contentView)"))
        XCTAssertTrue(themeSource.contains("DispatchQueue.main.async"))
        XCTAssertTrue(themeSource.contains("window.appearance = nil"))
        XCTAssertTrue(themeSource.contains("window.contentView?.appearance = nil"))
        XCTAssertTrue(themeSource.contains("window.backgroundColor = .windowBackgroundColor"))
        XCTAssertTrue(themeSource.contains(".preferredColorScheme(theme.preferredColorScheme)"))
        XCTAssertFalse(themeSource.contains(".foregroundStyle(palette?.foreground"))
        XCTAssertTrue(themeSource.contains("selectionFill"))
        XCTAssertTrue(themeSource.contains("secondaryText"))
        XCTAssertTrue(designSource.contains("kind.fill(theme: resolvedTheme)"))
        XCTAssertTrue(designSource.contains("@Environment(\\.runeThemePalette)"))
        XCTAssertTrue(designSource.contains("@Environment(\\.runeResolvedTheme)"))
        XCTAssertTrue(glassSource.contains("@Environment(\\.runeResolvedTheme)"))
        XCTAssertTrue(manifestTextSource.contains("@Environment(\\.runeResolvedTheme)"))
        XCTAssertTrue(manifestTextSource.contains("ManifestPalette.resolved(manifestTheme)"))
        XCTAssertTrue(manifestTextSource.contains("storage.addAttributes(issue.attributes(palette: manifestPalette), range: range)"))
    }

    func testZedThemeJSONCanBeLoadedAsRuneTheme() throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneThemeCatalogTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDirectory) }

        let themeJSON = """
        {
          "$schema": "https://zed.dev/schema/themes/v0.2.0.json",
          "name": "Example Family",
          "author": "Example",
          "themes": [
            {
              "name": "Example Dark",
              "appearance": "dark",
              "style": {
                "background": "#101820ff",
                "panel.background": "#182431ff",
                "surface.background": "#182431ff",
                "element.background": "#1e2c3aff",
                "element.selected": "#294866ff",
                "border": "#40576eff",
                "border.variant": "#34485cff",
                "text": "#f4f8fbff",
                "text.muted": "#b6c4d1ff",
                "text.placeholder": "#8ea0adff",
                "text.accent": "#6bb7f7ff",
                "editor.background": "#0d141cff",
                "editor.foreground": "#e7edf3ff",
                "success": "#7ee6a8ff",
                "warning": "#ffd166ff",
                "error": "#ff7a9aff",
                "info": "#8bd3ffff",
                "syntax": {
                  "property": { "color": "#8bd3ffff" },
                  "string": { "color": "#7ee6a8ff" },
                  "number": { "color": "#ffd166ff" },
                  "boolean": { "color": "#c59cffff" },
                  "comment": { "color": "#8ea0adff" },
                  "keyword": { "color": "#ff9ac8ff" },
                  "type": { "color": "#80e6d6ff" }
                }
              }
            }
          ]
        }
        """
        try themeJSON.write(to: tempDirectory.appendingPathComponent("example.json"), atomically: true, encoding: .utf8)

        let themes = RuneThemeCatalog.loadZedThemes(from: tempDirectory)
        XCTAssertEqual(themes.count, 1)
        let theme = try XCTUnwrap(themes.first)
        XCTAssertEqual(theme.id, "zed:example:example-dark")
        XCTAssertEqual(theme.title, "Example Dark")
        XCTAssertEqual(theme.sourceSummary, "Custom theme")
        XCTAssertEqual(theme.preferredColorScheme, .dark)
        XCTAssertNotNil(theme.palette)
        XCTAssertEqual(theme.appKitPalette?.accent, "#6bb7f7ff")
        XCTAssertEqual(theme.appKitPalette?.danger, "#ff7a9aff")
        XCTAssertEqual(theme.syntaxPalette?.key, "#8bd3ffff")
        XCTAssertNil(theme.builtin)
    }

    func testPreferencesExposeWriteAndRollbackSafetySettings() throws {
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let kubeClientSource = try String(contentsOfFile: kubernetesClientPath, encoding: .utf8)
        let restClientSource = try String(contentsOfFile: kubernetesRESTClientPath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("writeSafetyRequireApplyDryRun"))
        XCTAssertTrue(settingsSource.contains("writeSafetyRequireRolloutDryRun"))
        XCTAssertTrue(settingsSource.contains("writeSafetyRequireHelmDryRun"))
        XCTAssertTrue(settingsSource.contains("writeSafetyShowRollbackPlan"))
        XCTAssertTrue(settingsSource.contains("writeSafetyRequireCopyableCommand"))
        XCTAssertTrue(settingsSource.contains("writeSafetyRequirePostActionVerification"))
        XCTAssertTrue(settingsSource.contains("writeSafetyRequireProductionSecondConfirmation"))
        XCTAssertTrue(settingsSource.contains("writeSafetyShowDestructiveCommandsInCommandPalette"))
        XCTAssertTrue(settingsSource.contains("writeSafetyShowDestructiveCommandsInCommandPalette: false"))
        XCTAssertTrue(preferencesSource.contains("case safety"))
        XCTAssertTrue(preferencesSource.contains("settingsSection(\"Write safety\")"))
        XCTAssertTrue(preferencesSource.contains("settingsSection(\"Rollback safety\")"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.writeSafetyRequireApplyDryRun)"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.writeSafetyShowDestructiveCommandsInCommandPalette)"))
        XCTAssertTrue(preferencesSource.contains("Palette destructive commands"))
        XCTAssertTrue(preferencesSource.contains("opens Rune's write confirmation; it never writes directly"))
        XCTAssertTrue(rootViewSource.contains("showsCopyCommandAction: !viewModel.pendingWriteActionKubectlCommand.isEmpty"))
        XCTAssertTrue(viewModelSource.contains("runeWriteSafetyRequireRolloutDryRun"))
        XCTAssertTrue(viewModelSource.contains("case deleteSelectedResource"))
        XCTAssertTrue(viewModelSource.contains("guard UserDefaults.standard.runeWriteSafetyShowDestructiveCommandsInCommandPalette else { return }"))
        XCTAssertTrue(viewModelSource.contains("requestDeleteSelectedResource()"))
        XCTAssertTrue(viewModelSource.contains("pendingRollbackPlan"))
        XCTAssertTrue(viewModelSource.contains("requestHelmRollback(revision:"))
        XCTAssertTrue(viewModelSource.contains("helmCommandRunner.rollback"))
        XCTAssertTrue(viewModelSource.contains("Helm accepted rollback dry-run"))
        XCTAssertTrue(rootViewSource.contains("viewModel.requestHelmRollback(revision: entry.revision)"))
        XCTAssertTrue(kubeClientSource.contains("dryRunRollbackDeploymentRollout"))
        XCTAssertTrue(restClientSource.contains("dryRun: Bool = false"))
        XCTAssertTrue(restClientSource.contains("dryRun=All"))
    }

    func testPreferencesExposeGlobalManagedFieldsDisplaySetting() throws {
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("hideManagedFieldsByDefault"))
        XCTAssertTrue(settingsSource.contains("runeHideManagedFieldsByDefault"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault)"))
        XCTAssertTrue(preferencesSource.contains("settingsString(.settingsHideManagedFieldsByDefault)"))
        XCTAssertTrue(describeSource.contains("@AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault)"))
        XCTAssertTrue(yamlSource.contains("@AppStorage(RuneSettingsKeys.hideManagedFieldsByDefault)"))
    }

    func testPreferencesExposeSimpleModeForLightweightInterface() throws {
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)
        let describeSource = try String(contentsOfFile: resourceDescribeInspectorViewPath, encoding: .utf8)
        let yamlSource = try String(contentsOfFile: resourceYAMLInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let compactRootSource = rootViewSource.filter { !$0.isWhitespace }

        XCTAssertTrue(settingsSource.contains("simpleMode"))
        XCTAssertTrue(settingsSource.contains("runeSimpleMode"))
        XCTAssertTrue(settingsSource.contains("simpleMode: false"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false"))
        XCTAssertTrue(preferencesSource.contains("settingsString(.settingsSimpleMode)"))
        XCTAssertTrue(preferencesSource.contains("settingsString(.settingsSimpleModeManagedFieldsNote)"))
        XCTAssertTrue(preferencesSource.contains("if simpleMode"))
        XCTAssertTrue(describeSource.contains("@AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false"))
        XCTAssertTrue(describeSource.contains("let effectiveHidesManagedFields = simpleMode || hidesManagedFields"))
        XCTAssertTrue(describeSource.contains("if !simpleMode"))
        XCTAssertTrue(yamlSource.contains("@AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false"))
        XCTAssertTrue(yamlSource.contains("let effectiveHidesManagedFields = simpleMode || hidesManagedFields"))
        XCTAssertTrue(yamlSource.contains("if !simpleMode, filteredYAML.removedBlockCount > 0"))
        XCTAssertTrue(rootViewSource.contains("@AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false"))
        XCTAssertTrue(compactRootSource.contains("if!simpleMode{OverviewClusterSignalsPanelView("))
        XCTAssertTrue(compactRootSource.contains("if!simpleMode{OverviewRecentEventsPanelView("))
        XCTAssertTrue(compactRootSource.contains("if!simpleMode{OverviewStatCard(title:\"Events\""))
        XCTAssertTrue(rootViewSource.contains("if !simpleMode {\n            modules.append(.events)"))
        XCTAssertTrue(compactRootSource.contains("if!simpleMode{authDoctorPanel"))
        XCTAssertTrue(rootViewSource.contains("guard !simpleMode else { return false }"))
        XCTAssertTrue(rootViewSource.contains("guard !simpleMode else { return }"))
        XCTAssertTrue(rootViewSource.contains("if !simpleMode {\n                    Menu {\n                        Button(\"Save Bundle\")"))
        XCTAssertTrue(rootViewSource.contains("if !simpleMode {") && rootViewSource.contains("viewModel.runAuthDoctor()"))
        XCTAssertTrue(rootViewSource.contains("if !simpleMode {\n                RBACCanISimulatorPanel(viewModel: viewModel)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.setHelmBrowserResourceFamily(tab.resourceListFamily)"))
        XCTAssertTrue(rootViewSource.contains("let shouldOfferReplicaSetHistoryLoad = simpleMode && relatedReplicaSets.isEmpty"))
        XCTAssertTrue(rootViewSource.contains("viewModel.refreshReplicaSetsForCurrentNamespace()"))
        XCTAssertTrue(rootViewSource.contains("if viewModel.state.selectedSection != .terminal"))
        XCTAssertTrue(viewModelSource.contains("if !UserDefaults.standard.runeSimpleMode {\n                    self.runAuthDoctor()"))
        XCTAssertTrue(viewModelSource.contains("static func forSelection(section: RuneSection, kind: KubeResourceKind, simpleMode: Bool = false)"))
        XCTAssertTrue(viewModelSource.contains("if !simpleMode {\n                plan.events = true"))
        XCTAssertTrue(viewModelSource.contains("if !simpleMode {\n                    plan.replicaSets = true"))
        XCTAssertTrue(viewModelSource.contains("if simpleMode {\n                switch kind"))
        XCTAssertTrue(viewModelSource.contains("} else {\n                plan.serviceAccounts = true\n                plan.rbacRoles = true"))
        XCTAssertTrue(viewModelSource.contains("if simpleMode {\n                cancellationFamilies.insert(helmBrowserResourceFamily)"))
        XCTAssertTrue(viewModelSource.contains("if simpleMode {\n                    try await loadSelectedHelmBrowserResource"))
        XCTAssertTrue(viewModelSource.contains("} else {\n                    try await loadHelmReleases(context: context, namespace: state.selectedNamespace)\n                    await loadOperatorResources(context: context, namespace: state.selectedNamespace)"))
        XCTAssertTrue(viewModelSource.contains("public func refreshReplicaSetsForCurrentNamespace()"))
    }

    func testPreferencesExposeHoverTooltipSetting() throws {
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let clusterSignalsSource = try String(contentsOfFile: overviewClusterSignalsPanelViewPath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("showHoverTooltips"))
        XCTAssertTrue(settingsSource.contains("runeShowHoverTooltips"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.showHoverTooltips)"))
        XCTAssertTrue(preferencesSource.contains("settingsString(.settingsShowHoverTooltips)"))
        XCTAssertTrue(rootViewSource.contains("@AppStorage(RuneSettingsKeys.showHoverTooltips)"))
        XCTAssertTrue(clusterSignalsSource.contains("@AppStorage(RuneSettingsKeys.showHoverTooltips)"))
        XCTAssertTrue(rootViewSource.contains(".runeHelp("))
        XCTAssertTrue(clusterSignalsSource.contains(".runeHelp("))
    }

    func testPreferencesExposeConfiguredExportDestinationSettings() throws {
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let logsSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.exportFolderDisplayName) private var exportFolderDisplayName"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.exportTextOpenerBundleIdentifier) private var exportTextOpenerBundleIdentifier"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.exportArchiveOpenerBundleIdentifier) private var exportArchiveOpenerBundleIdentifier"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.exportUsesPrivacySafeFilenames) private var exportUsesPrivacySafeFilenames"))
        XCTAssertTrue(preferencesSource.contains("settingsSection(\"Export destination\")"))
        XCTAssertTrue(preferencesSource.contains("Privacy-safe export filenames"))
        XCTAssertTrue(preferencesSource.contains("private func chooseExportFolder()"))
        XCTAssertTrue(preferencesSource.contains("url.bookmarkData("))
        XCTAssertTrue(preferencesSource.contains("options: [.withSecurityScope]"))
        XCTAssertTrue(preferencesSource.contains("private func chooseExportOpener(for kind: ConfiguredExportFileKind)"))
        XCTAssertTrue(preferencesSource.contains("Bundle(url: url)?.bundleIdentifier"))
        XCTAssertTrue(preferencesSource.contains("ExportOpenerRecommendation(appName: \"Inkline\", kind: .plainText)"))
        XCTAssertTrue(preferencesSource.contains("appName: \"QuikZip\""))
        XCTAssertTrue(preferencesSource.contains("Some apps cannot load .yaml, .log, or .zip exports from a background handoff"))
        XCTAssertTrue(preferencesSource.contains("recommendedBundleIdentifier: \"com.viktornyberg.quikzip\""))
        XCTAssertTrue(preferencesSource.contains("return \"\\(appName) text editor detected\""))
        XCTAssertTrue(preferencesSource.contains("return \"\\(appName) archive utility detected\""))
        XCTAssertTrue(preferencesSource.contains("return \"\\(appName) archive utility suggested\""))
        XCTAssertTrue(preferencesSource.contains("private func detectedRecommendedExportOpener(_ recommendation: ExportOpenerRecommendation)"))
        XCTAssertTrue(preferencesSource.contains("private func applicationURL(named appName: String) -> URL?"))
        XCTAssertTrue(preferencesSource.contains("private func workspaceApplicationURLs(named appName: String, filename: String) -> [URL]"))
        XCTAssertTrue(preferencesSource.contains("FileManager.default.urls("))
        XCTAssertTrue(preferencesSource.contains("Button(\"Use \\(detected.appName)\")"))
        XCTAssertTrue(logsSource.contains("Label(\"Current Logs\", systemImage: \"doc.text\")"))
        XCTAssertTrue(logsSource.contains("Label(\"Save to Export Folder\", systemImage: \"folder\")"))
        XCTAssertTrue(logsSource.contains("Label(\"Save and Open\", systemImage: \"arrow.up.right.square\")"))
        XCTAssertTrue(logsSource.contains("Label(\"Save As...\", systemImage: \"doc.zipper\")"))
        XCTAssertTrue(logsSource.contains("Label(\"Save As...\", systemImage: \"archivebox\")"))
        XCTAssertTrue(logsSource.contains("Label(\"Save As...\", systemImage: \"shippingbox\")"))
        XCTAssertTrue(logsSource.contains("Label(\"Save to Export Folder\", systemImage: \"folder.badge.plus\")"))
        XCTAssertTrue(logsSource.contains("Label(\"Save and Open\", systemImage: \"archivebox\")"))
        XCTAssertTrue(logsSource.contains("t(.exportVisibleResultsZip)"))
        XCTAssertTrue(logsSource.contains("t(.exportFullUnfilteredZip)"))
        XCTAssertTrue(logsSource.contains("t(.exportAllPodsFullZip)"))
        XCTAssertTrue(rootViewSource.contains("Button(\"Export JSON…\")"))
        XCTAssertTrue(rootViewSource.contains("Button(\"Save JSON to Export Folder\")"))
        XCTAssertTrue(rootViewSource.contains("Button(\"Save JSON and Open\")"))
        XCTAssertTrue(rootViewSource.contains("Label(\"Full Logs ZIP to Export Folder\", systemImage: \"folder.badge.plus\")"))
        XCTAssertTrue(rootViewSource.contains("Label(\"Full Logs ZIP and Open\", systemImage: \"archivebox\")"))
        XCTAssertTrue(rootViewSource.contains("Button(\"Save Pod Logs ZIP to Export Folder\")"))
        XCTAssertTrue(rootViewSource.contains("Button(\"Save Pod Logs ZIP and Open\")"))
        XCTAssertTrue(viewModelSource.contains("public func saveCurrentLogsToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveVisibleLogsZipToExportFolder(visibleText: String, openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveCurrentLogsZipToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveAllPodsLogsZipToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveSelectedPodLogsZipToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveDeploymentPodLogsZipToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveActiveTerminalTranscriptToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveAllTerminalTranscriptsZipToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveCurrentResourceYAMLToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveCurrentResourceDescribeToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveVisibleWriteAuditLogToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("public func saveSupportBundleToExportFolder(openAfterSave: Bool)"))
        XCTAssertTrue(viewModelSource.contains("configuredExporter.save("))
    }

    func testPreferencesExposeTerminalWorkspacePersistenceSetting() throws {
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("persistTerminalWorkspaceState"))
        XCTAssertTrue(settingsSource.contains("runePersistTerminalWorkspaceState"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.persistTerminalWorkspaceState)"))
        XCTAssertTrue(preferencesSource.contains("Save terminal view state"))
        XCTAssertTrue(rootViewSource.contains("@AppStorage(RuneSettingsKeys.persistTerminalWorkspaceState)"))
        XCTAssertTrue(rootViewSource.contains("restoreTerminalWorkspaceStateIfNeeded()"))
        XCTAssertTrue(rootViewSource.contains("persistTerminalWorkspaceStateIfNeeded()"))
    }

    func testTerminalLogTabsExposeFavoriteToggle() throws {
        let logsSource = try String(contentsOfFile: terminalLogTabBarPath, encoding: .utf8)
        let logsInspectorSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(logsSource.contains("let onToggleFavoriteTab: (String) -> Void"))
        XCTAssertTrue(logsSource.contains("onToggleFavoriteTab(tab.id)"))
        XCTAssertTrue(logsSource.contains("Favorite Log Target"))
        XCTAssertFalse(logsInspectorSource.contains("struct TerminalLogTabBar: View"))
        XCTAssertTrue(rootViewSource.contains("onToggleFavoriteTab: toggleFavoriteTerminalLogTab"))
        XCTAssertTrue(rootViewSource.contains("private func toggleFavoriteTerminalLogTab"))
        XCTAssertTrue(rootViewSource.contains("viewModel.toggleFavoriteResource(kind: .pod, namespace: tab.namespace, name: tab.podName)"))
    }

    func testTerminalFontSizePreferenceIsScopedToTerminalAndEditorSurfaces() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let textViewSource = try String(contentsOfFile: appKitManifestTextViewPath, encoding: .utf8)
        let shellSource = try String(contentsOfFile: terminalShellPanelViewPath, encoding: .utf8)

        XCTAssertFalse(rootViewSource.contains("@AppStorage(RuneSettingsKeys.terminalFontSize)"))
        XCTAssertFalse(rootViewSource.contains(".dynamicTypeSize(appDynamicTypeSize)"))
        XCTAssertFalse(rootViewSource.contains("private var appDynamicTypeSize: DynamicTypeSize"))
        XCTAssertTrue(textViewSource.contains("@AppStorage(RuneSettingsKeys.terminalFontSize)"))
        XCTAssertTrue(textViewSource.contains("fontSize: clampedFontSize"))
        XCTAssertTrue(textViewSource.contains("NSFont.monospacedSystemFont(ofSize: configuredFontSize"))
        XCTAssertTrue(shellSource.contains("@AppStorage(RuneSettingsKeys.terminalFontSize)"))
        XCTAssertTrue(shellSource.contains("fontSize: terminalFontSize"))
    }

    func testResourceInspectorsUseOverviewStyleInformationRows() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        let overviewDetails = try functionBlock(
            named: "private var overviewDetails: some View {",
            endingBefore: "private var workloadDetails",
            in: source
        )
        XCTAssertTrue(overviewDetails.contains("inspectorInfoRow(\"Context\""))
        XCTAssertTrue(overviewDetails.contains("inspectorActionButtonRow"))

        let podOverview = try functionBlock(
            named: "private func podOverviewSection",
            endingBefore: "private func podOverviewRow",
            in: source
        )
        let podCoreInfo = try functionBlock(
            named: "private func podInspectorCoreInfo",
            endingBefore: "private func podOverviewRow",
            in: source
        )
        XCTAssertTrue(podCoreInfo.contains("inspectorInfoRow(\"Namespace\""))
        XCTAssertTrue(podCoreInfo.contains("inspectorInfoRow(\"Status\""))
        XCTAssertTrue(podOverview.contains("RuneInspectorInfoRow(\"Containers\""))
        XCTAssertFalse(podOverview.contains("inspectorInsetCard"))
        XCTAssertFalse(podOverview.contains(".runeInsetCard()"))

        let deploymentOverview = try functionBlock(
            named: "private func deploymentOverviewSection",
            endingBefore: "private func inspectorEmptyState",
            in: source
        )
        let deploymentCoreInfo = try functionBlock(
            named: "private func deploymentInspectorCoreInfo",
            endingBefore: "private var deploymentInspectorActions",
            in: source
        )
        let deploymentActions = try functionBlock(
            named: "private var deploymentInspectorActions",
            endingBefore: "private func isHistoricalDeploymentReplicaSet",
            in: source
        )
        XCTAssertTrue(deploymentCoreInfo.contains("inspectorInfoRow(\"Namespace\""))
        XCTAssertTrue(deploymentCoreInfo.contains("RuneInspectorInfoRow(\"Status\""))
        XCTAssertTrue(deploymentActions.contains("Button(\"Export Pod Logs ZIP\")"))
        XCTAssertTrue(deploymentActions.contains("viewModel.saveDeploymentPodLogsZip()"))
        XCTAssertFalse(deploymentOverview.contains("inspectorInsetCard"))
        XCTAssertFalse(deploymentOverview.contains(".runeInsetCard()"))

        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        XCTAssertTrue(viewModelSource.contains("public func saveDeploymentPodLogsZip()"))
        XCTAssertTrue(viewModelSource.contains("let pods = try await self.kubeClient.podsForDeployment"))
        XCTAssertTrue(viewModelSource.contains("baseName = \"deployment-\\(deployment.name)-pod-logs\""))
        XCTAssertTrue(viewModelSource.contains("fullPodLogsZipData("))

        let serviceDetails = try functionBlock(
            named: "private var serviceDetails: some View {",
            endingBefore: "private var eventDetails",
            in: source
        )
        let serviceCoreInfo = try functionBlock(
            named: "private func serviceInspectorCoreInfo",
            endingBefore: "private var serviceInspectorActions",
            in: source
        )
        XCTAssertTrue(serviceCoreInfo.contains("inspectorInfoRow(\"Namespace\""))
        XCTAssertTrue(serviceCoreInfo.contains("inspectorInfoRow(\"Type\""))
        XCTAssertTrue(serviceCoreInfo.contains("inspectorInfoRow(\"Cluster IP\""))
        XCTAssertTrue(serviceDetails.contains("serviceInspectorActions"))
        XCTAssertFalse(serviceDetails.contains("inspectorInsetCard"))
        XCTAssertFalse(serviceDetails.contains(".runeInsetCard()"))
    }

    func testDetailPaneClipsSectionContentToRoundedShell() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let detailPane = try functionBlock(
            named: "private var detailPane: some View {",
            endingBefore: "private var overviewDetails",
            in: source
        )

        XCTAssertTrue(detailPane.contains("RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)"))
        XCTAssertTrue(detailPane.contains(".fill(panelFill)"))
        XCTAssertTrue(detailPane.contains(".clipShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous))"))
        XCTAssertTrue(detailPane.contains(".strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)"))
    }

    func testSplitResizeHandleUsesFixedAffordanceCenteredByColumnOverlay() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let handle = try functionBlock(
            named: "private var splitColumnResizeHandle: some View {",
            endingBefore: "private var productionBanner",
            in: source
        )

        XCTAssertFalse(handle.contains("Color.clear"))
        XCTAssertFalse(handle.contains(".frame(maxHeight: .infinity)"))
        XCTAssertTrue(handle.contains(".frame(width: 4, height: 44)"))
        XCTAssertTrue(handle.contains(".frame(width: 14, height: 44)"))
        XCTAssertFalse(handle.contains("Spacer(minLength: 0)"))
    }

    func testPodResourceListNameColumnIsResizableAndPersisted() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let appKitPodTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("layoutPodNameColumnWidth"))
        XCTAssertTrue(rootViewSource.contains("@AppStorage(RuneSettingsKeys.layoutPodNameColumnWidth)"))
        XCTAssertTrue(rootViewSource.contains("podNameColumnWidth"))
        XCTAssertFalse(rootViewSource.contains("setLivePodNameColumnWidth"))
        XCTAssertFalse(rootViewSource.contains("podNameColumnResizeTranslation"))
        XCTAssertFalse(rootViewSource.contains("private var podNameColumnResizeHandle"))
        XCTAssertFalse(rootViewSource.contains("isHoveringPodNameColumnResizeHandle"))
        XCTAssertTrue(rootViewSource.contains("commitPodNameColumnWidth"))
        XCTAssertTrue(rootViewSource.contains("AppKitPodTableView("))
        XCTAssertTrue(rootViewSource.contains("onNameColumnWidthChanged: commitPodNameColumnWidth"))
        XCTAssertTrue(rootViewSource.contains("canApplyClusterMutations: viewModel.canApplyClusterMutations"))
        XCTAssertTrue(appKitPodTableSource.contains("NSTableView"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableStyle.apply(to: tableView, allowsColumnResizing: true)"))
        XCTAssertTrue(appKitPodTableSource.contains("columnAutoresizingStyle = .noColumnAutoresizing"))
        XCTAssertTrue(appKitPodTableSource.contains("selectionHighlightStyle = .none"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableRowView"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableHeaderCell"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceStatusPillView"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableStyle.apply(to:"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.rowSizeStyle = .medium"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.focusRingType = .none"))
        XCTAssertTrue(appKitPodTableSource.contains("headerCell.configure("))
        XCTAssertTrue(appKitPodTableSource.contains("reservesSortIndicator: column.sortColumn != nil"))
        XCTAssertTrue(appKitPodTableSource.contains("let trailingInset = RuneAppKitResourceTableStyle.contentTrailingInset"))
        XCTAssertTrue(appKitPodTableSource.contains("+ (reservesSortIndicator ? RuneAppKitResourceListLayout.sortIndicatorSize.width + RuneAppKitResourceTableStyle.sortIndicatorGap : 0)"))
        XCTAssertTrue(appKitPodTableSource.contains("NSImage.Name(parent.sortAscending ? \"NSAscendingSortIndicator\" : \"NSDescendingSortIndicator\")"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableTheme.resolved(for: controlView).headerText"))
        XCTAssertTrue(appKitPodTableSource.contains("label.centerYAnchor.constraint(equalTo: container.centerYAnchor)"))
        XCTAssertTrue(appKitPodTableSource.contains("drawSortIndicator(indicatorImage"))
        XCTAssertTrue(appKitPodTableSource.contains("resetColumnWidth(_ columnID: String)"))
        XCTAssertTrue(appKitPodTableSource.contains("event.clickCount == 2"))
        XCTAssertTrue(appKitPodTableSource.contains("drawColumnDivider"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneUILayoutMetrics.compactGlyphCornerRadius"))
        XCTAssertTrue(appKitPodTableSource.contains("tableTheme.rowFill"))
        XCTAssertTrue(appKitPodTableSource.contains("NSColor.controlAccentColor.withAlphaComponent(0.11)"))
        XCTAssertTrue(appKitPodTableSource.contains("tableViewColumnDidResize"))
        XCTAssertTrue(appKitPodTableSource.contains("DispatchQueue.main.asyncAfter(deadline: .now() + 0.18"))
        XCTAssertTrue(appKitPodTableSource.contains("column.resizingMask = isUserResizable ? .userResizingMask : []"))
        XCTAssertTrue(appKitPodTableSource.contains("column.sortDescriptorPrototype = NSSortDescriptor(key: sortColumn.rawValue, ascending: true)"))
        XCTAssertTrue(appKitPodTableSource.contains("lineBreakMode: .byTruncatingMiddle"))
        XCTAssertTrue(appKitPodTableSource.contains("isEnabled: parent.canApplyClusterMutations"))
        XCTAssertFalse(appKitPodTableSource.contains("tableView.setIndicatorImage(parent.sortAscending"))
        XCTAssertFalse(appKitPodTableSource.contains("super.draw(dirtyRect)"))
        XCTAssertFalse(appKitPodTableSource.contains("DragGesture"))
    }

    func testResourceListColumnsUseNativeResizablePolicyAndPersistWidths() throws {
        let appKitPodTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)

        XCTAssertTrue(appKitPodTableSource.contains("private final class RuneAppKitColumnWidthStore"))
        XCTAssertTrue(appKitPodTableSource.contains("static let shared = RuneAppKitColumnWidthStore()"))
        XCTAssertTrue(appKitPodTableSource.contains("private let keyPrefix = \"rune.settings.layout.resourceColumnWidths.\""))
        XCTAssertTrue(appKitPodTableSource.contains("func width(tableID: String, columnID: String) -> CGFloat?"))
        XCTAssertTrue(appKitPodTableSource.contains("func setWidth(_ width: CGFloat, tableID: String, columnID: String)"))
        XCTAssertTrue(appKitPodTableSource.contains("func removeWidth(tableID: String, columnID: String)"))
        XCTAssertTrue(appKitPodTableSource.contains("var resizableColumnIdentifiers: Set<String> = []"))
        XCTAssertTrue(appKitPodTableSource.contains("headerView.resizableColumnIdentifiers = resizableColumnIdentifiers"))
        XCTAssertEqual(appKitPodTableSource.components(separatedBy: "resizableColumnIdentifiers: Set(").count - 1, 7)
        XCTAssertTrue(appKitPodTableSource.contains("column.resizingMask = isUserResizable ? .userResizingMask : []"))
        XCTAssertTrue(appKitPodTableSource.contains("tableViewColumnDidResize"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitColumnWidthStore.shared.setWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitColumnWidthStore.shared.removeWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("isUserResizable"))
        XCTAssertTrue(appKitPodTableSource.contains("case .selection, .favorite: return false"))
        XCTAssertTrue(appKitPodTableSource.contains("case .favorite: return false"))
        XCTAssertTrue(appKitPodTableSource.contains("case .replicas: return RuneAppKitResourceListLayout.deploymentReplicaColumnWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("case .type: return RuneAppKitResourceListLayout.serviceTypeColumnWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("genericPrimaryMinimumColumnWidth: CGFloat = 96"))
        XCTAssertTrue(appKitPodTableSource.contains("genericSecondaryMinimumColumnWidth: CGFloat = 72"))
        XCTAssertTrue(appKitPodTableSource.contains("genericNamespaceMinimumColumnWidth: CGFloat = 104"))
        XCTAssertTrue(appKitPodTableSource.contains("case .primary:\n            return RuneAppKitResourceListLayout.genericPrimaryMinimumColumnWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("case .secondary:\n            return RuneAppKitResourceListLayout.genericSecondaryMinimumColumnWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("case .namespace:\n            return RuneAppKitResourceListLayout.genericNamespaceMinimumColumnWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("private var applyGeneration = 0"))
        XCTAssertTrue(appKitPodTableSource.contains("DispatchQueue.main.async { [weak self, weak tableView] in"))
        XCTAssertTrue(appKitPodTableSource.contains("generation == self.applyGeneration"))
        XCTAssertFalse(appKitPodTableSource.contains("column.resizingMask = self == .name ? .userResizingMask : []"))
        XCTAssertFalse(appKitPodTableSource.contains("column.resizingMask = self == .name ? .autoresizingMask : []"))
        XCTAssertFalse(appKitPodTableSource.contains("resizableColumnIdentifier: String?"))
    }

    func testResourceColumnResizeUsesSingleSynchronizedDragPipeline() throws {
        let appKitPodTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)

        XCTAssertTrue(appKitPodTableSource.contains("private func trackColumnResize(_ column: NSTableColumn, from event: NSEvent)"))
        XCTAssertTrue(appKitPodTableSource.contains("applySynchronizedResourceColumnResize(proposedWidth, for: column, in: tableView)"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView, updatesVisibleCellsImmediately: false)"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceColumnResizeNotificationGate.withSuppressedNotifications(for: tableView)"))
        XCTAssertTrue(appKitPodTableSource.contains("if isSuppressedSynchronizedResourceColumnResize(notification) { return }"))
        XCTAssertTrue(appKitPodTableSource.contains("static func synchronizeVisibleResizeFrames(on tableView: NSTableView)"))
        XCTAssertTrue(appKitPodTableSource.contains("func latestResizeEvent(startingWith event: NSEvent) -> NSEvent"))
        XCTAssertTrue(appKitPodTableSource.contains("onColumnResizeCommitted?(column)"))
        XCTAssertFalse(appKitPodTableSource.contains("isPreviewingColumnResize"))
        XCTAssertFalse(appKitPodTableSource.contains("resizePreviewRenderedWidth"))
        XCTAssertFalse(appKitPodTableSource.contains("updateRenderedTableWidthPreview"))
        XCTAssertFalse(appKitPodTableSource.contains("synchronizeVisibleResizePreviewGeometry"))
        XCTAssertFalse(appKitPodTableSource.contains("applyImmediateResourceColumnResizePreviewIfNeeded"))
    }

    func testSplitPaneVisibilityIsRestoredFromDefaults() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)

        XCTAssertTrue(settingsSource.contains("layoutSidebarVisible"))
        XCTAssertTrue(settingsSource.contains("layoutDetailPaneVisible"))
        XCTAssertTrue(settingsSource.contains("runeLayoutSidebarVisible"))
        XCTAssertTrue(settingsSource.contains("runeLayoutDetailPaneVisible"))
        XCTAssertTrue(viewModelSource.contains("@Published public var isSidebarVisible: Bool"))
        XCTAssertTrue(viewModelSource.contains("@Published public var isDetailPaneVisible: Bool"))
        XCTAssertTrue(viewModelSource.contains("UserDefaults.standard.runeLayoutSidebarVisible"))
        XCTAssertTrue(viewModelSource.contains("UserDefaults.standard.runeLayoutDetailPaneVisible"))
        XCTAssertTrue(viewModelSource.contains("UserDefaults.standard.runeLayoutSidebarVisible = isSidebarVisible"))
        XCTAssertTrue(viewModelSource.contains("UserDefaults.standard.runeLayoutDetailPaneVisible = isDetailPaneVisible"))
    }

    func testAppKitResourceTablesUseSharedNativeTableStyle() throws {
        let appKitPodTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)

        XCTAssertTrue(appKitPodTableSource.contains("private enum RuneAppKitResourceTableStyle"))
        XCTAssertTrue(appKitPodTableSource.contains("static let headerHeight: CGFloat = 24"))
        XCTAssertTrue(appKitPodTableSource.contains("static let rowHeight: CGFloat = 34"))
        XCTAssertTrue(appKitPodTableSource.contains("static let rowGap: CGFloat = 4"))
        XCTAssertTrue(appKitPodTableSource.contains("static let rowHorizontalInset: CGFloat = 6"))
        XCTAssertTrue(appKitPodTableSource.contains("static let contentLeadingInset: CGFloat = 10"))
        XCTAssertTrue(appKitPodTableSource.contains("static let contentTrailingInset: CGFloat = 10"))
        XCTAssertTrue(appKitPodTableSource.contains("static let sortIndicatorGap: CGFloat = 4"))
        XCTAssertTrue(appKitPodTableSource.contains("headerView.horizontalInset = rowHorizontalInset"))
        XCTAssertTrue(appKitPodTableSource.contains("headerView.frame = NSRect(x: 0, y: 0, width: 0, height: headerHeight)"))
        XCTAssertTrue(appKitPodTableSource.contains("let headerSize = NSSize(width: tableWidth, height: headerHeight)"))
        XCTAssertTrue(appKitPodTableSource.contains("private final class RuneAppKitResourceListScrollView: NSScrollView"))
        XCTAssertTrue(appKitPodTableSource.contains("override func setFrameSize(_ newSize: NSSize)"))
        XCTAssertTrue(appKitPodTableSource.contains("override func setBoundsSize(_ newSize: NSSize)"))
        XCTAssertTrue(appKitPodTableSource.contains("override func viewWillStartLiveResize()"))
        XCTAssertTrue(appKitPodTableSource.contains("override func viewDidEndLiveResize()"))
        XCTAssertTrue(appKitPodTableSource.contains("private var isSendingVisibleWidthChange = false"))
        XCTAssertTrue(appKitPodTableSource.contains("private enum RuneAppKitResourceTableHost"))
        XCTAssertEqual(appKitPodTableSource.components(separatedBy: "RuneAppKitResourceTableHost.make(").count - 1, 7)
        XCTAssertEqual(appKitPodTableSource.components(separatedBy: "let scrollView = RuneAppKitResourceListScrollView()").count - 1, 1)
        XCTAssertEqual(
            appKitPodTableSource.components(
                separatedBy: "final class RuneAppKitResourceTableView: NSTableView"
            ).count - 1,
            1
        )
        XCTAssertEqual(appKitPodTableSource.components(separatedBy: "override func menu(for event: NSEvent)").count - 1, 1)
        XCTAssertEqual(appKitPodTableSource.components(separatedBy: "override func keyDown(with event: NSEvent)").count - 1, 1)
        XCTAssertTrue(appKitPodTableSource.contains("private struct RuneAppKitResourceTableTheme"))
        XCTAssertTrue(appKitPodTableSource.contains("let headerFill: NSColor"))
        XCTAssertTrue(appKitPodTableSource.contains("headerFill: NSColor.controlBackgroundColor.withAlphaComponent(0.42)"))
        XCTAssertTrue(appKitPodTableSource.contains("headerFill: NSColor.runeHex(row).withAlphaComponent(0.62)"))
        XCTAssertTrue(appKitPodTableSource.contains("private func drawHeaderBackground(in dirtyRect: NSRect)"))
        XCTAssertTrue(appKitPodTableSource.contains("roundedRect: rect"))
        XCTAssertTrue(appKitPodTableSource.contains("xRadius: RuneUILayoutMetrics.compactGlyphCornerRadius"))
        XCTAssertFalse(appKitPodTableSource.contains("RuneAppearanceTheme.resolved(UserDefaults.standard.string(forKey: RuneSettingsKeys.appearanceTheme)"))
        XCTAssertEqual(appKitPodTableSource.components(separatedBy: "@Environment(\\.runeResolvedTheme)").count - 1, 7)
        XCTAssertEqual(appKitPodTableSource.components(separatedBy: "RuneAppKitResourceTableHost.invalidateTheme(in: scrollView, resolvedTheme: resolvedTheme)").count - 1, 14)
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)"))
        XCTAssertEqual(appKitPodTableSource.components(separatedBy: "RuneAppKitResourceTableStyle.apply(to: tableView").count - 1, 1)
        XCTAssertFalse(appKitPodTableSource.contains("usesAlternatingRowBackgroundColors = false\n        tableView.backgroundColor = .clear\n        tableView.gridStyleMask = []\n        tableView.headerView"))
        XCTAssertTrue(appKitPodTableSource.contains("lineBreakMode: NSLineBreakMode = .byTruncatingTail"))
        XCTAssertTrue(appKitPodTableSource.contains("titleLabel.lineBreakMode = .byTruncatingMiddle"))
    }

    func testPodNameColumnWidthIsClampedToUsableRange() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let layout = try functionBlock(
            named: "enum PodTableLayout {",
            endingBefore: "enum RuneRootKeyboardPane",
            in: rootViewSource
        )

        XCTAssertTrue(layout.contains("nameColumnMinimumWidth"))
        XCTAssertTrue(layout.contains("nameColumnMaximumWidth"))
        XCTAssertTrue(layout.contains("clampedNameColumnWidth"))
        XCTAssertTrue(layout.contains("width.rounded(.toNearestOrAwayFromZero)"))
        XCTAssertTrue(layout.contains("min(nameColumnMaximumWidth, max(nameColumnMinimumWidth, pixelAlignedWidth))"))
        XCTAssertTrue(layout.contains("nameColumnDefaultWidth: CGFloat = 260"))
        XCTAssertTrue(layout.contains("nameColumnMinimumWidth: CGFloat = 104"))
        XCTAssertTrue(layout.contains("cpuWidth: CGFloat = 58"))
        XCTAssertTrue(layout.contains("memoryWidth: CGFloat = 64"))
        XCTAssertTrue(layout.contains("restartsWidth: CGFloat = 72"))
        XCTAssertTrue(layout.contains("ageWidth: CGFloat = 54"))
        XCTAssertTrue(layout.contains("statusTextWidth: CGFloat = 94"))
        XCTAssertTrue(layout.contains("favoriteColumnWidth: CGFloat = 34"))
        XCTAssertTrue(layout.contains("defaultAppKitTableWidth"))
        XCTAssertTrue(layout.contains("+ nameColumnDefaultWidth"))
    }

    func testPodNameColumnHeaderAndRowsShareSelectionGutterForResizeAlignment() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let appKitPodTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)
        let layout = try functionBlock(
            named: "enum PodTableLayout {",
            endingBefore: "enum RuneRootKeyboardPane",
            in: rootViewSource
        )
        let workloadsPane = try functionBlock(
            named: "private var workloadsPane: some View",
            endingBefore: "private var networkingPane",
            in: rootViewSource
        )

        XCTAssertTrue(layout.contains("selectionColumnWidth"))
        XCTAssertTrue(layout.contains("+ selectionColumnWidth"))
        XCTAssertTrue(layout.contains("headerHorizontalInset: CGFloat = rowHorizontalPadding"))
        XCTAssertTrue(layout.contains("nameColumnFrameWidth"))
        XCTAssertTrue(layout.contains("metricsColumnGroupWidth"))
        XCTAssertTrue(workloadsPane.contains("AppKitPodTableView("))
        XCTAssertTrue(workloadsPane.contains("nameColumnWidth: podNameColumnWidth"))
        XCTAssertFalse(workloadsPane.contains("pinnedViews: [.sectionHeaders]"))
        XCTAssertTrue(appKitPodTableSource.contains("case selection"))
        XCTAssertTrue(appKitPodTableSource.contains("case name"))
        XCTAssertTrue(appKitPodTableSource.contains("case cpu"))
        XCTAssertTrue(appKitPodTableSource.contains("case memory"))
        XCTAssertTrue(appKitPodTableSource.contains("case restarts"))
        XCTAssertTrue(appKitPodTableSource.contains("case age"))
        XCTAssertTrue(appKitPodTableSource.contains("case status"))
        XCTAssertTrue(appKitPodTableSource.contains("case favorite"))
        XCTAssertTrue(appKitPodTableSource.contains("case .selection: return PodTableLayout.selectionColumnWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("case .name: return PodTableLayout.clampedNameColumnWidth(nameColumnWidth)"))
        XCTAssertTrue(appKitPodTableSource.contains("case .cpu: return PodTableLayout.cpuWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("case .memory: return PodTableLayout.memoryWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("case .restarts: return PodTableLayout.restartsWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("case .age: return PodTableLayout.ageWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("case .status: return PodTableLayout.statusTotalWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("case .favorite: return PodTableLayout.favoriteColumnWidth"))
        XCTAssertTrue(appKitPodTableSource.contains("return statusCell(for: pod)"))
        XCTAssertTrue(appKitPodTableSource.contains("container.layer?.masksToBounds = true"))
        XCTAssertTrue(appKitPodTableSource.contains("let maxPillWidth = pill.widthAnchor.constraint(lessThanOrEqualTo: container.widthAnchor, constant: -8)"))
        XCTAssertTrue(appKitPodTableSource.contains("maxPillWidth.priority = .defaultHigh"))
        XCTAssertTrue(appKitPodTableSource.contains("case .cpu: return 42"))
        XCTAssertTrue(appKitPodTableSource.contains("case .memory: return 48"))
        XCTAssertTrue(appKitPodTableSource.contains("case .restarts: return 54"))
        XCTAssertTrue(appKitPodTableSource.contains("case .age: return 40"))
        XCTAssertTrue(appKitPodTableSource.contains("case .status: return 56"))
        XCTAssertTrue(appKitPodTableSource.contains("return favoriteCell(isFavorite: parent.isFavorite(pod), row: row)"))
        XCTAssertTrue(appKitPodTableSource.contains("@objc private func toggleFavoriteButton"))
        XCTAssertTrue(appKitPodTableSource.contains("static let rowGap: CGFloat = 4"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.intercellSpacing = NSSize(width: 0, height: rowGap)"))
        XCTAssertTrue(appKitPodTableSource.contains("x: bounds.minX + horizontalInset"))
        XCTAssertFalse(appKitPodTableSource.contains("drawInterior(withFrame: cellFrame.insetBy(dx: 4, dy: 0)"))
    }

    func testResourceContextMenusExposeOperationalCopyActions() throws {
        let appKitPodTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)

        XCTAssertTrue(appKitPodTableSource.contains("Copy pod name"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy namespace"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy container names"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy images"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy labels"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy owner reference"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy node name"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy pod IP"))

        XCTAssertTrue(appKitPodTableSource.contains("Copy deployment name"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy selector"))

        XCTAssertTrue(appKitPodTableSource.contains("Copy service name"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy cluster IP"))
        XCTAssertTrue(appKitPodTableSource.contains("copyLabelSelector"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy primary detail"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy secondary detail"))
        XCTAssertTrue(appKitPodTableSource.contains("resource.ownerReferencesLine"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy object kind"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy family"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy kind"))
        XCTAssertTrue(appKitPodTableSource.contains("Copy status"))
    }

    func testConfigMapInspectorExposesFocusedEditAction() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("Edit ConfigMap YAML"))
        XCTAssertTrue(rootViewSource.contains("resource.kind == .configMap"))
        XCTAssertTrue(rootViewSource.contains("genericResourceManifestTab = .yaml"))
        XCTAssertTrue(rootViewSource.contains("yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing"))
    }

    func testSidebarContextListIsAConstrainedScrollableRegion() async throws {
        let state = RuneAppState()
        state.setContexts((1...80).map { KubeContext(name: String(format: "cluster-%03d", $0)) })
        state.setNamespaces(["default"])
        let viewModel = RuneAppViewModel(state: state)

        let host = NSHostingController(
            rootView: RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: nil,
                debugDisableBootstrap: true
            )
            .frame(width: 980, height: 520)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 980, height: 520),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        try await settle(window: window)

        guard let scrollView = findConstrainedOverflowingScrollView(in: host.view) else {
            return XCTFail("Expected sidebar context list to render as a constrained overflowing scroll view")
        }

        let documentHeight = scrollView.documentView?.frame.height ?? 0
        let viewportHeight = scrollView.contentView.bounds.height
        XCTAssertGreaterThan(documentHeight, viewportHeight + 400, "Long context lists should overflow inside the sidebar scroll view")
        XCTAssertLessThan(viewportHeight, 360, "Context scroll view should be constrained by sidebar chrome instead of expanding to full content height")

        scrollView.contentView.scroll(to: NSPoint(x: 0, y: min(240, documentHeight - viewportHeight)))
        scrollView.reflectScrolledClipView(scrollView.contentView)
        try await settle(window: window)

        XCTAssertGreaterThan(scrollView.contentView.bounds.origin.y, 0, "Sidebar context list should scroll downward")
    }

    func testPrimaryScrollViewsDeclareFlexibleHeightFrames() throws {
        let source = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains(".accessibilityIdentifier(\"rune.sidebar.contexts.scroll\")"))
        XCTAssertTrue(source.contains(".frame(minHeight: 80, maxHeight: .infinity, alignment: .topLeading)"))
        XCTAssertTrue(source.contains("LazyVStack(alignment: .leading, spacing: 5)"))
        XCTAssertTrue(source.contains("VStack(alignment: .leading, spacing: 3)"))
        XCTAssertTrue(source.contains("Text(section.localizedTitle(appString))"))
        XCTAssertTrue(source.contains("Text(\"⌘\" + String(section.commandShortcut))"))
        XCTAssertFalse(source.contains("localizedTitle(appString) + \"    ⌘\""))
    }

    func testPortForwardRowsExposeBrowserActionOnlyWhenActive() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let terminalViewSource = try String(contentsOfFile: resourceTerminalInspectorViewPath, encoding: .utf8)
        let portForwardPanelSource = try String(contentsOfFile: terminalPortForwardPanelViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("if session.status == .active, session.browserURL != nil"))
        XCTAssertTrue(rootViewSource.contains("Label(\"Open in Browser\", systemImage: \"safari\")"))
        XCTAssertTrue(rootViewSource.contains("viewModel.openPortForwardInBrowser(session)"))
        XCTAssertTrue(rootViewSource.contains("onOpenPortForwardInBrowser: { session in"))
        XCTAssertTrue(rootViewSource.contains("onStopPortForward: { session in"))
        XCTAssertTrue(rootViewSource.contains("viewModel.stopPortForward(session)"))

        XCTAssertTrue(terminalViewSource.contains("let onOpenPortForwardInBrowser: (PortForwardSession) -> Void"))
        XCTAssertTrue(terminalViewSource.contains("let onStopPortForward: (PortForwardSession) -> Void"))
        XCTAssertTrue(terminalViewSource.contains("let onRetryPortForward: (PortForwardSession) -> Void"))
        XCTAssertTrue(terminalViewSource.contains("let onClearPortForward: (PortForwardSession) -> Void"))
        XCTAssertTrue(portForwardPanelSource.contains("Clear Inactive"))
        XCTAssertTrue(portForwardPanelSource.contains("Label(\"Retry\", systemImage: \"arrow.clockwise\")"))
        XCTAssertTrue(portForwardPanelSource.contains("onClearPortForward(session)"))
        XCTAssertTrue(portForwardPanelSource.contains("if session.status == .active, session.browserURL != nil"))
        XCTAssertTrue(portForwardPanelSource.contains("PortForwardPrimaryActionButton("))
        XCTAssertTrue(portForwardPanelSource.contains("Label(\"Open in Browser\", systemImage: \"safari\")"))
        XCTAssertTrue(portForwardPanelSource.contains("onOpenPortForwardInBrowser(session)"))
        XCTAssertTrue(portForwardPanelSource.contains("onStop: onStopPortForward"))
        XCTAssertFalse(portForwardPanelSource.contains("stopButton(session)"))
    }

    func testPodBulkSelectionUsesSharedNativeSelectionComponents() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let designSource = try String(contentsOfFile: runeDesignComponentsPath, encoding: .utf8)
        let appKitPodTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("RuneBulkSelectionBar("))
        XCTAssertTrue(appKitPodTableSource.contains("NSButton(checkboxWithTitle: \"\""))
        XCTAssertTrue(rootViewSource.contains("genericResourceBulkSelectionControls"))
        XCTAssertTrue(rootViewSource.contains("viewModel.requestDeleteSelectedGenericResources()"))
        XCTAssertTrue(rootViewSource.contains("Label(\"Export\", systemImage: \"square.and.arrow.up\")"))
        XCTAssertTrue(rootViewSource.contains("Menu {"))
        XCTAssertTrue(designSource.contains("struct RuneBulkSelectionBar"))
        XCTAssertFalse(designSource.contains("struct RuneSelectionCheckboxButton"))
        XCTAssertFalse(designSource.contains("struct RuneInspectorSection"))
        XCTAssertFalse(designSource.contains("func runeEditorCard"))
        XCTAssertFalse(designSource.contains("func runeListRowCard"))
        XCTAssertTrue(designSource.contains("allVisibleSelected ? \"Deselect All\" : \"Select All\""))
    }

    func testEmptyKubernetesWorkspaceUsesOneFocusedOnboardingBranch() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let onboardingSource = try String(contentsOfFile: kubernetesConnectionOnboardingViewPath, encoding: .utf8)
        let sidebarBlock = try functionBlock(
            named: "private var sidebar: some View",
            endingBefore: "private func sectionRow",
            in: rootViewSource
        )
        let contentHeaderBlock = try functionBlock(
            named: "private var contentHeader: some View",
            endingBefore: "private var showsResourceFilterControls",
            in: rootViewSource
        )
        let overviewBlock = try functionBlock(
            named: "private var overviewPane: some View",
            endingBefore: "private var workloadsPane",
            in: rootViewSource
        )

        XCTAssertEqual(
            rootViewSource.components(separatedBy: "KubernetesConnectionOnboardingView(").count - 1,
            1,
            "The empty workspace should have one connection entry point instead of repeating import controls."
        )
        XCTAssertFalse(sidebarBlock.contains("No kubeconfigs loaded"))
        XCTAssertFalse(sidebarBlock.contains("Rune discovers kubeconfig files automatically"))
        XCTAssertEqual(
            sidebarBlock.components(separatedBy: "addClusterButton").count - 1,
            1,
            "The sidebar should keep one compact Add Cluster affordance."
        )
        XCTAssertFalse(contentHeaderBlock.contains("Button(\"Import Kubeconfig…\")"))
        XCTAssertFalse(contentHeaderBlock.contains("Button(\"Command Palette\")"))

        guard let emptyBranchStart = overviewBlock.range(of: "if !hasAvailableKubernetesContexts"),
              let onboardingStart = overviewBlock.range(
                of: "KubernetesConnectionOnboardingView(",
                range: emptyBranchStart.upperBound..<overviewBlock.endIndex
              ),
              let connectedBranchStart = overviewBlock.range(
                of: "} else {",
                range: onboardingStart.upperBound..<overviewBlock.endIndex
              ) else {
            XCTFail("Overview should explicitly separate empty onboarding from connected-cluster content.")
            return
        }

        let emptyBranch = String(overviewBlock[emptyBranchStart.lowerBound..<connectedBranchStart.lowerBound])
        let connectedBranch = String(overviewBlock[connectedBranchStart.upperBound...])
        XCTAssertTrue(emptyBranch.contains("KubernetesConnectionOnboardingView("))
        XCTAssertFalse(emptyBranch.contains("overviewStatusBanner"))
        XCTAssertFalse(emptyBranch.contains("authDoctorPanel"))
        XCTAssertFalse(emptyBranch.contains("OverviewStatCard("))
        XCTAssertTrue(connectedBranch.contains("overviewStatusBanner"))
        XCTAssertTrue(connectedBranch.contains("authDoctorPanel"))
        XCTAssertTrue(connectedBranch.contains("OverviewStatCard("))

        XCTAssertTrue(
            onboardingSource.contains("@Binding var favoriteImportedContexts: Bool")
                || onboardingSource.contains("let favoriteImportedContexts: Binding<Bool>")
        )
        XCTAssertTrue(onboardingSource.contains("let onImportFile: () -> Void"))
        XCTAssertTrue(onboardingSource.contains("let onPaste: () -> Void"))
        XCTAssertTrue(onboardingSource.contains("let onImportFolder: () -> Void"))
        XCTAssertTrue(onboardingSource.contains("let onUseDefault: () -> Void"))
        XCTAssertTrue(onboardingSource.contains("let onShowMoreOptions: () -> Void"))
        XCTAssertEqual(
            onboardingSource.components(separatedBy: ".buttonStyle(.borderedProminent)").count - 1,
            1,
            "Import should be the single visually primary onboarding action."
        )
        XCTAssertTrue(onboardingSource.contains(".keyboardShortcut(.defaultAction)"))
        XCTAssertTrue(
            onboardingSource.contains("GridItem(.adaptive") || onboardingSource.contains("ViewThatFits(in: .horizontal)"),
            "Secondary connection paths should adapt instead of compressing at compact widths."
        )
    }

    @MainActor
    func testKubernetesConnectionOnboardingAdaptsBetweenCompactAndDefaultWidths() {
        func fittingHeight(width: CGFloat) -> CGFloat {
            let host = NSHostingView(
                rootView: KubernetesConnectionOnboardingView(
                    favoriteImportedContexts: .constant(false),
                    onImportFile: {},
                    onPaste: {},
                    onImportFolder: {},
                    onUseDefault: {},
                    onShowMoreOptions: {}
                )
                .frame(width: width)
            )
            host.layoutSubtreeIfNeeded()
            let size = host.fittingSize
            XCTAssertEqual(size.width, width, accuracy: 1)
            XCTAssertGreaterThan(size.height, 140)
            XCTAssertLessThan(size.height, 520, "Onboarding should remain focused without excessive empty space.")
            return size.height
        }

        let compactHeight = fittingHeight(width: 360)
        let defaultHeight = fittingHeight(width: 700)
        XCTAssertGreaterThan(
            compactHeight,
            defaultHeight + 20,
            "Secondary paths should wrap vertically at compact width instead of shrinking labels."
        )
    }

    func testGlobalErrorsRenderAsContentNoticesWithIndependentDismissAction() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let designSource = try String(contentsOfFile: runeDesignComponentsPath, encoding: .utf8)

        guard let contentPaneStart = rootViewSource.range(of: "private var contentPane: some View {"),
              let contentHeaderStart = rootViewSource.range(of: "private var contentHeader: some View {", range: contentPaneStart.upperBound..<rootViewSource.endIndex),
              let detailPaneStart = rootViewSource.range(of: "private var detailPane: some View {"),
              let overviewDetailsStart = rootViewSource.range(of: "private var overviewDetails: some View {", range: detailPaneStart.upperBound..<rootViewSource.endIndex)
        else {
            XCTFail("Could not locate content and detail pane blocks")
            return
        }

        let contentPaneBlock = String(rootViewSource[contentPaneStart.lowerBound..<contentHeaderStart.lowerBound])
        let detailPaneBlock = String(rootViewSource[detailPaneStart.lowerBound..<overviewDetailsStart.lowerBound])
        XCTAssertTrue(contentPaneBlock.contains("RuneNoticeBanner(notice: notice)"))
        XCTAssertTrue(contentPaneBlock.contains("viewModel.state.activeNotice"))
        XCTAssertFalse(contentPaneBlock.contains("Text(error)"))
        XCTAssertFalse(contentPaneBlock.contains(".foregroundStyle(.red)"))
        XCTAssertFalse(detailPaneBlock.contains("RuneNoticeBanner"))
        XCTAssertFalse(detailPaneBlock.contains("viewModel.state.activeNotice"))
        XCTAssertTrue(designSource.contains("struct RuneNoticeBanner"))

        let noticeBlock = try functionBlock(
            named: "struct RuneNoticeBanner: View",
            endingBefore: "private var symbolName: String",
            in: designSource
        )
        XCTAssertTrue(noticeBlock.contains("RuneIconButton("))
        XCTAssertTrue(noticeBlock.contains("\"Dismiss notice\""))
        XCTAssertTrue(noticeBlock.contains("action: onDismiss"))
        XCTAssertTrue(
            noticeBlock.contains(".accessibilityElement(children: .contain)"),
            "Combining the banner accessibility tree hides its independent Dismiss control."
        )
    }

    func testTerminalSectionRefreshLoadsPodsForShellAndPortForwardSelectors() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        guard let terminalCase = viewModelSource.range(of: "case .terminal:"),
              let helmCase = viewModelSource.range(of: "case .helm:", range: terminalCase.upperBound..<viewModelSource.endIndex)
        else {
            XCTFail("Could not locate terminal snapshot load plan in RuneAppViewModel.swift")
            return
        }

        let terminalPlanBlock = String(viewModelSource[terminalCase.lowerBound..<helmCase.lowerBound])
        XCTAssertTrue(
            terminalPlanBlock.contains("plan.pods = true"),
            "Terminal refreshes must load pods directly; otherwise context/namespace switches can leave shell and port-forward selectors empty until the user visits Workloads."
        )
    }

    func testReadOnlyTextModulesResetScrollWhenExternalContentChanges() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let textViewSource = try String(contentsOfFile: appKitManifestTextViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains(".id(\"overview\")"))
        XCTAssertTrue(rootViewSource.contains(".id(\"networking:service\")"))
        XCTAssertTrue(rootViewSource.contains(".id(\"terminal\")"))
        XCTAssertTrue(rootViewSource.contains(".id(\"\\(viewModel.state.selectedSection.rawValue):\\(viewModel.state.selectedWorkloadKind.kubernetesResourceName):\\(genericResourceListIdentity(resources))\")"))
        XCTAssertTrue(textViewSource.contains("var resetScrollOnExternalChange = false"))
        XCTAssertTrue(textViewSource.contains("if resetScrollOnExternalChange"))
        XCTAssertTrue(textViewSource.contains("textView.scrollRangeToVisible(NSRange(location: 0, length: 0))"))
    }

    func testYAMLEditorHandlesTabKeyBeforeFocusTraversal() throws {
        let textViewSource = try String(contentsOfFile: appKitManifestTextViewPath, encoding: .utf8)

        XCTAssertTrue(textViewSource.contains("override func keyDown(with event: NSEvent)"))
        XCTAssertTrue(textViewSource.contains("handleYAMLTabKey(event)"))
        XCTAssertTrue(textViewSource.contains("event.keyCode == 48"))
        XCTAssertTrue(textViewSource.contains("NSEvent.addLocalMonitorForEvents(matching: .keyDown)"))
        XCTAssertTrue(textViewSource.contains("self.window?.firstResponder === self"))
        XCTAssertTrue(textViewSource.contains("insertSoftTabOrIndentSelection()"))
        XCTAssertTrue(textViewSource.contains("outdentSelectedLines()"))
    }

    func testSidebarExposesAddClusterProviderFlow() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let importReviewPanelSource = try String(contentsOfFile: kubeConfigImportReviewPanelPath, encoding: .utf8)
        let importReviewSheetSource = try String(contentsOfFile: kubeConfigImportReviewSheetPath, encoding: .utf8)
        let contextSidebarRowSource = try String(contentsOfFile: contextSidebarRowPath, encoding: .utf8)
        let pickerSource = try String(contentsOfFile: kubeConfigPickerPath, encoding: .utf8)
        let kubernetesClientSource = try String(contentsOfFile: kubernetesClientPath, encoding: .utf8)
        let providerPresentationSource = try String(contentsOfFile: addClusterProviderPresentationPath, encoding: .utf8)
        let providerCredentialFieldSource = try String(contentsOfFile: addClusterProviderCredentialFieldPath, encoding: .utf8)
        let nativeContextSectionSource = try String(contentsOfFile: addClusterNativeContextSectionPath, encoding: .utf8)
        let addClusterPopoverSource = try String(contentsOfFile: addClusterPopoverViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("AddClusterPopoverView("))
        XCTAssertFalse(addClusterPopoverSource.contains("RuneAppViewModel"))
        XCTAssertFalse(addClusterPopoverSource.contains("viewModel"))
        XCTAssertFalse(addClusterPopoverSource.contains(".popover("))
        XCTAssertTrue(addClusterPopoverSource.contains("RuneGlassPaneSurface(role: .content)"))
        XCTAssertTrue(addClusterPopoverSource.contains("RuneSurfaceBackground(kind: .inset)"))
        XCTAssertTrue(addClusterPopoverSource.contains("RuneUILayoutMetrics.paneShellCornerRadius"))
        XCTAssertTrue(addClusterPopoverSource.contains(".frame(maxHeight: RuneUILayoutMetrics.addClusterPopoverMaxHeight)"))
        XCTAssertFalse(rootViewSource.contains(".id(addClusterPopoverLayoutID)"))
        XCTAssertTrue(rootViewSource.contains("isManualAddClusterExpanded = false"))
        XCTAssertTrue(rootViewSource.contains("viewModel.clearManualKubeConfigSecret()"))
        let popoverDismissBlock = try XCTUnwrap(
            rootViewSource.range(of: ".onChange(of: addClusterPopoverPresented)")
        )
        let providerSheetBlock = try XCTUnwrap(
            rootViewSource.range(of: "private func openAddClusterProviderSheet")
        )
        XCTAssertTrue(
            rootViewSource[popoverDismissBlock.lowerBound..<providerSheetBlock.lowerBound]
                .contains("viewModel.clearManualKubeConfigSecret()")
        )
        XCTAssertTrue(addClusterPopoverSource.contains("Standard"))
        XCTAssertTrue(addClusterPopoverSource.contains("discoveryStatus"))
        XCTAssertFalse(addClusterPopoverSource.contains("Auto-detect Clusters"))
        XCTAssertFalse(addClusterPopoverSource.contains("Import Options"))
        XCTAssertTrue(addClusterPopoverSource.contains("Import File"))
        XCTAssertEqual(addClusterPopoverSource.components(separatedBy: "title: \"Paste Kubeconfig\"").count - 1, 1)
        XCTAssertTrue(rootViewSource.contains("viewModel.importKubeConfigFromPasteboard()"))
        XCTAssertTrue(addClusterPopoverSource.contains("Import Folder"))
        XCTAssertTrue(rootViewSource.contains("viewModel.importKubeConfigFolder()"))
        XCTAssertTrue(addClusterPopoverSource.contains("Use Default"))
        XCTAssertTrue(addClusterPopoverSource.contains("~/.kube/config"))
        XCTAssertTrue(addClusterPopoverSource.contains("Favorite imported contexts"))
        XCTAssertTrue(rootViewSource.contains("$viewModel.favoriteImportedKubeConfigContexts"))
        XCTAssertTrue(addClusterPopoverSource.contains("Provider Login"))
        XCTAssertTrue(addClusterPopoverSource.contains("ForEach(AddClusterProviderIdentifier.allCases.filter { $0 != .local })"))
        XCTAssertTrue(addClusterPopoverSource.contains("Local Tools"))
        XCTAssertTrue(addClusterPopoverSource.contains("providerTile(.local)"))
        XCTAssertTrue(addClusterPopoverSource.contains("Manual Token Server"))
        XCTAssertTrue(addClusterPopoverSource.contains("manualField(\"Context name\", requirement: \"Required\")"))
        XCTAssertTrue(addClusterPopoverSource.contains("manualField(\"Namespace\", requirement: \"Optional\")"))
        XCTAssertTrue(addClusterPopoverSource.contains("manualField(\"Bearer token\", requirement: \"Required\")"))
        XCTAssertTrue(addClusterPopoverSource.contains("Add Manual Token Cluster"))
        XCTAssertTrue(rootViewSource.contains("viewModel.importManualTokenKubeConfig()"))
        XCTAssertTrue(addClusterPopoverSource.contains(".disabled(!canImportManualToken)"))
        XCTAssertFalse(addClusterPopoverSource.contains("KubeConfigImportReviewPanel("))
        XCTAssertTrue(addClusterPopoverSource.contains(".padding(RuneUILayoutMetrics.addClusterPopoverPadding)"))
        XCTAssertTrue(addClusterPopoverSource.contains(".frame(width: RuneUILayoutMetrics.addClusterPopoverWidth)"))
        XCTAssertFalse(rootViewSource.contains("private func addClusterQuickAction"))
        XCTAssertFalse(rootViewSource.contains("private func providerTileButton"))
        XCTAssertTrue(rootViewSource.contains(".sheet(isPresented: kubeConfigImportReviewPresentedBinding)"))
        XCTAssertTrue(rootViewSource.contains("KubeConfigImportReviewSheet("))
        XCTAssertTrue(rootViewSource.contains("KubeConfigImportReviewAggregator.aggregate(viewModel.kubeConfigImportReviews)"))
        XCTAssertTrue(rootViewSource.contains("get: { viewModel.isKubeConfigImportConfirmationPending }"))
        XCTAssertTrue(rootViewSource.contains("viewModel.cancelKubeConfigImport()"))
        XCTAssertTrue(viewModelSource.contains("public func clearKubeConfigImportReviews()"))
        XCTAssertTrue(importReviewPanelSource.contains("showsAuthDoctorAction: Bool = true"))
        XCTAssertTrue(importReviewPanelSource.contains("self.showsAuthDoctorAction = showsAuthDoctorAction"))
        XCTAssertTrue(importReviewPanelSource.contains("if showsAuthDoctorAction, reviewMode == .report {\n                runAuthDoctorButton"))
        XCTAssertTrue(importReviewSheetSource.contains("reviewMode: .preflight"))
        XCTAssertTrue(importReviewSheetSource.contains("showsAuthDoctorAction: false"))
        XCTAssertTrue(importReviewSheetSource.contains(".interactiveDismissDisabled(isCommitInProgress)"))
        XCTAssertTrue(rootViewSource.contains("metadataDrafts: viewModel.kubeConfigImportContextMetadataDrafts"))
        XCTAssertTrue(rootViewSource.contains("viewModel.setKubeConfigImportContextMetadata"))
        XCTAssertTrue(rootViewSource.contains("viewModel.contextDisplayName(for: context)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.contextSecondaryDisplayText(for: context)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.contextDisplayIconName(for: context)"))
        XCTAssertTrue(rootViewSource.contains("ContextSidebarRow("))
        XCTAssertTrue(contextSidebarRowSource.contains("struct ContextSidebarRow: View"))
        XCTAssertTrue(contextSidebarRowSource.contains("displayName == rawName ? secondaryText : \"\\(rawName) • \\(secondaryText)\""))
        XCTAssertTrue(contextSidebarRowSource.contains("isManuallyMarkedProduction ? \"Unmark Production\" : \"Mark as Production\""))
        XCTAssertTrue(contextSidebarRowSource.contains("isFavorite ? \"star.fill\" : \"star\""))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Import Review\")"))
        XCTAssertTrue(importReviewPanelSource.contains("RuneDialogCloseButton(isConfirmationPending ? \"Cancel kubeconfig import\" : \"Clear kubeconfig import report\")"))
        XCTAssertTrue(importReviewPanelSource.contains("RuneDialogActionBar {"))
        XCTAssertTrue(importReviewPanelSource.contains("Button(\"Cancel\", action: onCancel)"))
        XCTAssertTrue(importReviewPanelSource.contains("RuneDialogButtonLabel(isCommitInProgress ? \"Importing…\" : \"Import\")"))
        XCTAssertTrue(importReviewPanelSource.contains(".disabled(!canConfirm || isCommitInProgress)"))
        XCTAssertTrue(importReviewPanelSource.contains(".accessibilityLabel(\"Saving kubeconfig import\")"))
        XCTAssertTrue(importReviewPanelSource.contains("doc.text.magnifyingglass"))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Import metadata\")"))
        XCTAssertTrue(importReviewPanelSource.contains("TextField(\"Alias\""))
        XCTAssertTrue(importReviewPanelSource.contains("TextField(\"Tags\""))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Full review\")"))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Contexts"))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Issues"))
        XCTAssertTrue(importReviewPanelSource.contains("Text(\"Redacted kubeconfig\")"))
        XCTAssertTrue(importReviewPanelSource.contains("Run Auth Doctor"))
        XCTAssertTrue(importReviewPanelSource.contains("duplicateHandlingChoices"))
        XCTAssertTrue(importReviewPanelSource.contains("@Binding var duplicateHandlingChoice"))
        XCTAssertTrue(importReviewPanelSource.contains("Picker(\"Duplicate handling\", selection: $duplicateHandlingChoice)"))
        XCTAssertTrue(importReviewPanelSource.contains(".pickerStyle(.segmented)"))
        XCTAssertTrue(rootViewSource.contains("duplicateHandlingChoice: $viewModel.kubeConfigDuplicateHandlingChoice"))
        XCTAssertTrue(importReviewPanelSource.contains("Duplicate handling requires an explicit choice"))
        XCTAssertTrue(importReviewPanelSource.contains("choice.title"))
        XCTAssertTrue(importReviewPanelSource.contains("duplicateHandlingChoiceIcon"))
        XCTAssertTrue(providerPresentationSource.contains("Microsoft AKS"))
        XCTAssertTrue(providerPresentationSource.contains("Amazon EKS"))
        XCTAssertTrue(providerPresentationSource.contains("Google GKE"))
        XCTAssertTrue(providerPresentationSource.contains("Local Cluster"))
        XCTAssertTrue(rootViewSource.contains("az aks get-credentials"))
        XCTAssertTrue(rootViewSource.contains("aws eks update-kubeconfig"))
        XCTAssertTrue(rootViewSource.contains("gcloud container clusters get-credentials"))
        XCTAssertTrue(rootViewSource.contains("--location <location>"))
        XCTAssertTrue(providerPresentationSource.contains(".refreshContexts, title: \"Refresh\""))
        XCTAssertTrue(rootViewSource.contains("viewModel.refreshKubeConfigSourcesFromDiscovery()"))
        XCTAssertTrue(rootViewSource.contains("Text(viewModel.isRunningCloudKubeConfigImport ? \"Running\" : \"Run\")"))
        XCTAssertTrue(rootViewSource.contains("Image(systemName: \"icloud.and.arrow.down\")"))
        XCTAssertTrue(rootViewSource.contains("let canRunCredentialImport = canRunProviderCredentialImport(provider)"))
        XCTAssertTrue(rootViewSource.contains("let credentialCommand = providerCredentialCommand(provider, canRunCredentialImport: canRunCredentialImport)"))
        XCTAssertTrue(rootViewSource.contains("let runHelp = providerCredentialRunHelp(provider, canRunCredentialImport: canRunCredentialImport)"))
        XCTAssertTrue(rootViewSource.contains(".disabled(!canRunCredentialImport || viewModel.isRunningCloudKubeConfigImport)"))
        XCTAssertTrue(rootViewSource.contains(".help(runHelp)"))
        XCTAssertTrue(rootViewSource.contains("return \"Provider import is already running.\""))
        XCTAssertTrue(rootViewSource.contains("return \"Enter \\(missingFields) to run provider import.\""))
        XCTAssertTrue(providerPresentationSource.contains(".copyExternalCommand, title: \"Copy\""))
        XCTAssertTrue(providerPresentationSource.contains(".importKubeconfig, title: \"Import…\""))
        XCTAssertTrue(rootViewSource.contains("copyToPasteboard(credentialCommand)"))
        XCTAssertTrue(rootViewSource.contains("Copy the provider CLI command."))
        XCTAssertTrue(rootViewSource.contains("if provider != .local"))
        XCTAssertTrue(rootViewSource.contains("Refresh imported contexts and native credential status."))
        XCTAssertTrue(providerPresentationSource.contains(".runAuthDoctor, title: \"Doctor\""))
        XCTAssertTrue(rootViewSource.contains(".help(\"Run Auth Doctor for provider login, kubeconfig, RBAC, and API access checks.\")"))
        XCTAssertTrue(rootViewSource.contains("LazyVGrid(columns: addClusterProviderActionColumns, spacing: RuneUILayoutMetrics.dialogControlSpacing)"))
        XCTAssertTrue(rootViewSource.contains(".adaptive(minimum: RuneAddClusterProviderActionLayout.minimumButtonWidth)"))
        XCTAssertTrue(rootViewSource.contains("spacing: RuneAddClusterProviderActionLayout.columnSpacing"))
        XCTAssertTrue(rootViewSource.contains(".frame(width: RuneAddClusterProviderActionLayout.dialogWidth)"))
        XCTAssertTrue(rootViewSource.contains(".frame(maxHeight: RuneUILayoutMetrics.providerDialogMaxHeight)"))
        XCTAssertTrue(rootViewSource.contains("minHeight: RuneUILayoutMetrics.providerDialogBodyMinHeight"))
        XCTAssertTrue(rootViewSource.contains("idealHeight: RuneUILayoutMetrics.providerDialogBodyIdealHeight"))
        XCTAssertTrue(rootViewSource.contains("maxHeight: RuneUILayoutMetrics.providerDialogBodyMaxHeight"))
        XCTAssertTrue(rootViewSource.contains("RuneDialogActionBar {"))
        XCTAssertTrue(rootViewSource.contains("RuneDialogButtonLabel(\"Close\")"))
        XCTAssertTrue(rootViewSource.contains("providerPrimaryAction("))
        XCTAssertTrue(rootViewSource.contains("viewModel.runCloudKubeConfigImport"))
        XCTAssertTrue(rootViewSource.contains("openAddClusterProviderSheet(provider)"))
        XCTAssertTrue(rootViewSource.contains("private func openAddClusterProviderSheet(_ provider: RuneAddClusterProvider)"))
        XCTAssertTrue(rootViewSource.contains("presentation.executionMode == .externalCLI"))
        XCTAssertTrue(rootViewSource.contains("let diagnostic = viewModel.cloudKubeConfigImportDiagnostic"))
        XCTAssertTrue(rootViewSource.contains("addClusterCloudImportDiagnosticView(diagnostic)"))
        XCTAssertTrue(rootViewSource.contains("private func addClusterCloudImportDiagnosticView(_ diagnostic: AddClusterCloudImportDiagnostic) -> some View"))
        XCTAssertTrue(rootViewSource.contains("Text(diagnostic.commandShape)"))
        XCTAssertTrue(rootViewSource.contains("Link(destination: diagnostic.documentationURL)"))
        XCTAssertTrue(rootViewSource.contains("closeAddClusterProviderSheet(showPopover: true)"))
        XCTAssertTrue(rootViewSource.contains("Image(systemName: \"chevron.left\")"))
        XCTAssertTrue(rootViewSource.contains(".help(\"Back to Add Cluster\")"))
        XCTAssertTrue(rootViewSource.contains(".keyboardShortcut(.cancelAction)"))
        XCTAssertTrue(rootViewSource.contains("private func resetAddClusterProviderSheetStateIfIdle()"))
        XCTAssertTrue(rootViewSource.contains("guard !viewModel.isRunningCloudKubeConfigImport else { return }"))
        XCTAssertTrue(rootViewSource.contains("cloudCredentialDraft = CloudCredentialDraft()"))
        XCTAssertTrue(rootViewSource.contains("viewModel.clearCloudKubeConfigImportStatus()"))
        XCTAssertTrue(rootViewSource.contains("if presentation.showsCommandDetails"))
        XCTAssertTrue(rootViewSource.contains("Command Details"))
        XCTAssertTrue(rootViewSource.contains("AddClusterProviderPresentation.resolve("))
        XCTAssertTrue(rootViewSource.contains("let primaryAction = presentation.primaryAction("))
        XCTAssertTrue(rootViewSource.contains("let utilityActions = presentation.utilityActions("))
        XCTAssertTrue(rootViewSource.contains("ForEach(utilityActions)"))
        XCTAssertTrue(rootViewSource.contains("AddClusterNativeContextSection("))
        XCTAssertTrue(rootViewSource.contains("AddClusterNativeContextResolver.compatibleOptions("))
        XCTAssertTrue(rootViewSource.contains("viewModel.connectEKSNativeAuth("))
        XCTAssertTrue(rootViewSource.contains("viewModel.connectAKSNativeAuth("))
        XCTAssertTrue(rootViewSource.contains("viewModel.chooseAndConnectGKENativeAuth(request:"))
        XCTAssertTrue(rootViewSource.contains("viewModel.disconnectNativeAuth("))
        XCTAssertFalse(rootViewSource.contains("viewModel.disconnectSelectedNativeAuth()"))
        XCTAssertTrue(rootViewSource.contains("case .awsAccessKeyID:"))
        XCTAssertTrue(providerCredentialFieldSource.contains("if field.input == .secureText"))
        XCTAssertTrue(providerCredentialFieldSource.contains("TextField(\"\", text: $text)"))
        XCTAssertTrue(providerCredentialFieldSource.contains("SecureField(\"\", text: $text)"))
        XCTAssertTrue(providerPresentationSource.contains(".runNativeImport"))
        XCTAssertTrue(providerPresentationSource.contains("requiresCompatibleImportedContext: false"))
        XCTAssertTrue(providerPresentationSource.contains("showsCommandDetails: false"))
        XCTAssertTrue(nativeContextSectionSource.contains("Picker(\"Native authentication context\""))
        XCTAssertTrue(nativeContextSectionSource.contains("Credentials connected"))
        XCTAssertTrue(rootViewSource.contains("private func providerCredentialCommand(_ provider: RuneAddClusterProvider, canRunCredentialImport: Bool) -> String"))
        XCTAssertTrue(rootViewSource.contains("guard canRunCredentialImport, let cloudProvider = provider.cloudProvider else {"))
        XCTAssertTrue(rootViewSource.contains("private func canRunProviderCredentialImport(_ provider: RuneAddClusterProvider) -> Bool"))
        XCTAssertTrue(rootViewSource.contains("providerCredentialFields(presentation.fields)"))
        XCTAssertTrue(providerPresentationSource.contains("title: \"Cluster name\", isRequired: true"))
        XCTAssertTrue(providerPresentationSource.contains("title: \"Role ARN\", isRequired: false"))
        XCTAssertTrue(providerPresentationSource.contains("title: \"Subscription ID or name\", isRequired: false"))
        XCTAssertTrue(providerPresentationSource.contains("title: \"Project ID\", isRequired: true"))
        XCTAssertTrue(providerCredentialFieldSource.contains("Text(field.requirementTitle)"))
        XCTAssertTrue(providerPresentationSource.contains("kind, k3s, k3d, minikube"))
        XCTAssertTrue(rootViewSource.contains("kind get clusters && minikube status && k3d cluster list && k3s kubectl config current-context"))
        XCTAssertTrue(rootViewSource.contains("sudo cat /etc/rancher/k3s/k3s.yaml"))
        XCTAssertTrue(rootViewSource.contains("k3d kubeconfig get <cluster-name>"))
        XCTAssertTrue(rootViewSource.contains("kind get kubeconfig --name <cluster-name>"))
        XCTAssertTrue(rootViewSource.contains("minikube start"))
        XCTAssertTrue(rootViewSource.contains("minikube stop"))
        XCTAssertTrue(viewModelSource.contains("func addDefaultKubeConfig()"))
        XCTAssertTrue(viewModelSource.contains("func syncKubeConfigSourcesFromDiscovery(reason: String)"))
        XCTAssertTrue(viewModelSource.contains("func runCloudKubeConfigImport"))
        XCTAssertTrue(viewModelSource.contains("func clearCloudKubeConfigImportStatus()"))
        XCTAssertTrue(viewModelSource.contains("isRunningCloudKubeConfigImport"))
        XCTAssertTrue(viewModelSource.contains("guard !isRunningCloudKubeConfigImport else { return }"))
        XCTAssertTrue(viewModelSource.contains("cloudKubeConfigImportDiagnostic"))
        XCTAssertTrue(viewModelSource.contains("AddClusterCloudImportWorkflow.diagnostic(for: error, provider: request.provider)"))
        XCTAssertTrue(viewModelSource.contains("state.clearError()"))
        XCTAssertTrue(viewModelSource.contains("AddClusterCloudImportWorkflow.blockingFailure"))
        XCTAssertTrue(viewModelSource.contains("state.setAuthDoctorChecks(failure.checks)"))
        XCTAssertTrue(viewModelSource.contains("AddClusterCloudImportWorkflow.cloudLoginFailureChecks"))
        XCTAssertTrue(viewModelSource.contains("self.runAuthDoctor()"))
        XCTAssertTrue(viewModelSource.contains("CloudKubeConfigImporting"))
        XCTAssertTrue(viewModelSource.contains("kubeConfigSourceSyncNanoseconds"))
        XCTAssertTrue(viewModelSource.contains("state.resourceYAMLHasUnsavedEdits"))
        XCTAssertTrue(viewModelSource.contains("APP_SANDBOX_CONTAINER_ID"))
        XCTAssertTrue(viewModelSource.contains("pickDefaultKubeConfig(at: url)"))
        XCTAssertTrue(viewModelSource.contains("getpwuid(getuid())"))
        XCTAssertTrue(viewModelSource.contains("seen.insert(standardizedPath).inserted"))
        XCTAssertTrue(viewModelSource.contains("merged.append(source)"))
        XCTAssertTrue(pickerSource.contains("func pickDefaultKubeConfig(at defaultURL: URL) throws -> URL?"))
        XCTAssertTrue(pickerSource.contains("panel.showsHiddenFiles = true"))
        XCTAssertFalse(pickerSource.contains("allowedContentTypes"))
        XCTAssertTrue(kubernetesClientSource.contains("access.retainAccess(to: url)"))
    }

    func testAddClusterProviderSheetDefaultsToPrimaryActionInsteadOfClose() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let sheetBlock = try functionBlock(
            named: "private func addClusterProviderSheet(_ provider: RuneAddClusterProvider) -> some View",
            endingBefore: "private var addClusterProviderActionColumns",
            in: rootViewSource
        )
        let primaryActionBlock = try functionBlock(
            named: "private func providerPrimaryAction(",
            endingBefore: "private func nativeCloudAuthConnectButton",
            in: rootViewSource
        )
        let nativeActionBlock = try functionBlock(
            named: "private func nativeCloudAuthConnectButton",
            endingBefore: "private func providerCredentialCommand",
            in: rootViewSource
        )

        XCTAssertTrue(
            primaryActionBlock.contains("case .importKubeconfig:")
                && primaryActionBlock.contains("Label(action.title, systemImage: action.systemImage)")
                && primaryActionBlock.contains(".buttonStyle(.borderedProminent)\n            .keyboardShortcut(.defaultAction)"),
            "Local and native Add Cluster should default Return to Import when a kubeconfig is needed."
        )
        XCTAssertTrue(
            primaryActionBlock.contains("Image(systemName: \"icloud.and.arrow.down\")")
                && primaryActionBlock.contains(".disabled(!canRunCredentialImport || viewModel.isRunningCloudKubeConfigImport)"),
            "Cloud Add Cluster should default Return to the primary Run action once required fields are valid."
        )
        XCTAssertTrue(
            primaryActionBlock.contains("case .connectNativeCredentials, .chooseServiceAccountJSON:")
                && primaryActionBlock.contains("nativeCloudAuthConnectButton(provider)"),
            "Native credential actions should only render after the presentation selects them."
        )
        XCTAssertEqual(
            nativeActionBlock.components(separatedBy: "selectedAddClusterNativeContextOption == nil").count - 1,
            3,
            "Every native provider must require an explicit imported-context selection before connecting credentials."
        )
        XCTAssertTrue(sheetBlock.contains("RuneDialogButtonLabel(\"Close\")"))
        XCTAssertTrue(sheetBlock.contains(".keyboardShortcut(.cancelAction)"))
        XCTAssertFalse(sheetBlock.contains("RuneDialogCloseButton"))
        XCTAssertTrue(sheetBlock.contains("providerPrimaryAction("))
    }

    func testAuthDoctorPanelExposesSupportBundleQuickAction() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let authDoctorBlock = try functionBlock(
            named: "private var authDoctorPanel: some View",
            endingBefore: "private func authDoctorSymbol",
            in: rootViewSource
        )

        XCTAssertTrue(authDoctorBlock.contains("Label(\"Auth Doctor\", systemImage: \"stethoscope\")"))
        XCTAssertTrue(authDoctorBlock.contains("isAuthDoctorPanelExpanded.toggle()"))
        XCTAssertTrue(authDoctorBlock.contains("private func authDoctorHeader(runLabel: String) -> some View"))
        XCTAssertTrue(authDoctorBlock.contains("ScrollView(.horizontal, showsIndicators: false)"))
        XCTAssertTrue(authDoctorBlock.contains(".frame(maxWidth: .infinity, minHeight: 32, alignment: .trailing)"))
        XCTAssertTrue(authDoctorBlock.contains("private var authDoctorDisclosureButton: some View"))
        XCTAssertTrue(authDoctorBlock.contains("RuneDisclosureRow("))
        XCTAssertTrue(authDoctorBlock.contains(".frame(minWidth: 154, alignment: .leading)"))
        XCTAssertTrue(authDoctorBlock.contains("private func authDoctorActions(runLabel: String) -> some View"))
        XCTAssertTrue(authDoctorBlock.contains(".buttonStyle(.bordered)"))
        XCTAssertTrue(authDoctorBlock.contains(".controlSize(.small)"))
        XCTAssertTrue(authDoctorBlock.contains(".fixedSize(horizontal: true, vertical: false)"))
        XCTAssertTrue(authDoctorBlock.contains("authDoctorSummaryChip"))
        XCTAssertTrue(authDoctorBlock.contains("requestMetricsSummaryRow"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.refreshKubernetesRequestMetricsSummary()"))
        XCTAssertTrue(authDoctorBlock.contains("shouldReserveAuthDoctorPanel"))
        XCTAssertTrue(authDoctorBlock.contains("\"Run Auth Doctor\""))
        XCTAssertTrue(authDoctorBlock.contains("\"Ready\""))
        XCTAssertTrue(authDoctorBlock.contains("Button(\"Save Bundle\")"))
        XCTAssertTrue(authDoctorBlock.contains("Button(\"Save Bundle to Export Folder\")"))
        XCTAssertTrue(authDoctorBlock.contains("Button(\"Save Bundle and Open\")"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.saveSupportBundle()"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.saveSupportBundleToExportFolder(openAfterSave: false)"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.saveSupportBundleToExportFolder(openAfterSave: true)"))
        XCTAssertTrue(authDoctorBlock.contains("Label(\"Clear\", systemImage: \"xmark.circle\")"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.clearAuthDoctorOutput()"))
        XCTAssertTrue(authDoctorBlock.contains("if isAuthDoctorPanelExpanded"))
        XCTAssertTrue(authDoctorBlock.contains("authDoctorCheckRow(check)"))
        XCTAssertTrue(authDoctorBlock.contains("AuthDoctorEntryActionResolver.resolve"))
        XCTAssertTrue(authDoctorBlock.contains("performAuthDoctorEntryAction(action)"))
        XCTAssertTrue(authDoctorBlock.contains("private func performAuthDoctorEntryAction(_ action: AuthDoctorEntryResolution)"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.setSection(.workloads)"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.selectPod(pod)"))
        XCTAssertTrue(authDoctorBlock.contains("podInspectorTab = .logs"))
        XCTAssertTrue(authDoctorBlock.contains("podInspectorTab = .exec"))
        XCTAssertTrue(authDoctorBlock.contains("podInspectorTab = .portForward"))
        XCTAssertTrue(authDoctorBlock.contains("case let .section(section):"))
        XCTAssertTrue(authDoctorBlock.contains("case let .resource(section, kind):"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.setSection(section)"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.setWorkloadKind(kind)"))
        XCTAssertTrue(authDoctorBlock.contains("case let .rbacCanIPreset(verb, resource, apiGroup, subresource, scope):"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.useRBACCanIPreset"))
        XCTAssertTrue(authDoctorBlock.contains("viewModel.reviewLoadedKubeConfigSources()"))
        XCTAssertTrue(authDoctorBlock.contains("addClusterPopoverPresented = true"))
        XCTAssertTrue(authDoctorBlock.contains("NSWorkspace.shared.open(url)"))

        let authDoctorVisibilityBlock = try functionBlock(
            named: "private var shouldReserveAuthDoctorPanel: Bool",
            endingBefore: "@ViewBuilder\n    private var authDoctorSummaryChip",
            in: rootViewSource
        )
        XCTAssertTrue(authDoctorVisibilityBlock.contains("guard !simpleMode else { return false }"))
        XCTAssertTrue(authDoctorVisibilityBlock.contains("case .overview"))
        XCTAssertFalse(authDoctorVisibilityBlock.contains("case .overview, .workloads"))

        let workloadsBlock = try functionBlock(
            named: "private var workloadsPane: some View",
            endingBefore: "private var networkingPane",
            in: rootViewSource
        )
        XCTAssertFalse(workloadsBlock.contains("authDoctorPanel"))
    }

    func testAuthDoctorPanelExposesRequestMetricsDebugSummary() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let requestMetricsBlock = try functionBlock(
            named: "private var requestMetricsSummaryRow: some View",
            endingBefore: "private func authDoctorCheckRow",
            in: rootViewSource
        )
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(requestMetricsBlock.contains("Text(\"API Requests\")"))
        XCTAssertTrue(requestMetricsBlock.contains("summary.requestCountText"))
        XCTAssertTrue(requestMetricsBlock.contains("summary.outcomeText"))
        XCTAssertTrue(requestMetricsBlock.contains("summary.transferText"))
        XCTAssertTrue(requestMetricsBlock.contains("summary.retainedText"))
        XCTAssertTrue(requestMetricsBlock.contains("Label(\"Refresh\", systemImage: \"arrow.clockwise\")"))
        XCTAssertTrue(requestMetricsBlock.contains("viewModel.refreshKubernetesRequestMetricsSummary()"))
        XCTAssertTrue(requestMetricsBlock.contains("viewModel.isRefreshingKubernetesRequestMetricsSummary"))
        XCTAssertTrue(viewModelSource.contains("KubernetesRequestMetricsDebugPresentation"))
        XCTAssertTrue(viewModelSource.contains("public func refreshKubernetesRequestMetricsSummary()"))
        XCTAssertTrue(viewModelSource.contains("kubeClient.restRequestMetricsSummary()"))
    }

    func testAuthDoctorStaysReadOnlyAndDoesNotRunMutatingActions() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let authDoctorBlock = try functionBlock(
            named: "public func runAuthDoctor()",
            endingBefore: "public func loadDemoCluster()",
            in: viewModelSource
        )

        XCTAssertTrue(authDoctorBlock.contains("listContexts"))
        XCTAssertTrue(authDoctorBlock.contains("listNamespaces"))
        XCTAssertTrue(authDoctorBlock.contains("canI"))
        XCTAssertTrue(authDoctorBlock.contains("AuthDoctorRBACProjector.accessSummary"))
        XCTAssertTrue(authDoctorBlock.contains("AuthDoctorFailureProjector.checks"))
        XCTAssertTrue(authDoctorBlock.contains("execCredentialCacheDiagnostic"))
        XCTAssertTrue(authDoctorBlock.contains("AuthDoctorExecAuthCacheProjector.check"))
        XCTAssertTrue(authDoctorBlock.contains("AuthDoctorRBACPreflightTarget.emptyViewTargets"))
        XCTAssertTrue(authDoctorBlock.contains("listPods"))
        XCTAssertTrue(authDoctorBlock.contains("podLogs"))
        XCTAssertFalse(authDoctorBlock.contains("applyYAML"))
        XCTAssertFalse(authDoctorBlock.contains("delete"))
        XCTAssertFalse(authDoctorBlock.contains("rollback"))
        XCTAssertFalse(authDoctorBlock.contains("execInPod"))
        XCTAssertFalse(authDoctorBlock.contains("helmCommandRunner"))
        XCTAssertFalse(authDoctorBlock.contains("confirmPendingWriteAction("))
        XCTAssertFalse(authDoctorBlock.contains("patchCronJobSuspend"))
        XCTAssertFalse(authDoctorBlock.contains("startTerminalSession"))
        XCTAssertFalse(authDoctorBlock.contains("startPortForward"))
        XCTAssertFalse(authDoctorBlock.contains("stopPortForward"))
        XCTAssertFalse(authDoctorBlock.contains("dryRunRollbackDeploymentRollout"))
        XCTAssertFalse(authDoctorBlock.contains("runDeploymentRollback"))
    }

    func testRBACInspectorExposesReadOnlyCanISimulator() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let rbacDetailsBlock = try functionBlock(
            named: "private var rbacDetails: some View",
            endingBefore: "private var helmDetails: some View",
            in: rootViewSource
        )
        let simulatorSource = try String(contentsOfFile: rbacCanISimulatorPanelPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let simulatorActionBlock = try functionBlock(
            named: "public func runRBACCanISimulator()",
            endingBefore: "private func currentRBACCanIRequest()",
            in: viewModelSource
        )

        XCTAssertTrue(rbacDetailsBlock.contains("if !simpleMode {\n                RBACCanISimulatorPanel(viewModel: viewModel)"))
        XCTAssertTrue(rbacDetailsBlock.contains("ResourceRelationshipSection(title: \"Referenced Role\")"))
        XCTAssertTrue(rbacDetailsBlock.contains("viewModel.openRBACBindingReferencedRole(role)"))
        XCTAssertTrue(rbacDetailsBlock.contains("ResourceRelationshipSection(title: \"Related Bindings\", rowCount: relatedBindings.count)"))
        XCTAssertTrue(rbacDetailsBlock.contains("viewModel.openRBACRoleRelatedBinding(binding)"))
        XCTAssertTrue(simulatorSource.contains("Can I?"))
        XCTAssertTrue(simulatorSource.contains("viewModel.runRBACCanISimulator()"))
        XCTAssertTrue(simulatorSource.contains("viewModel.useSelectedRBACResourceForCanI()"))
        XCTAssertTrue(simulatorSource.contains("RBACCanIScope.allCases"))
        XCTAssertTrue(simulatorSource.contains("RuneSurfaceBackground(kind: .panel)"))
        XCTAssertTrue(simulatorActionBlock.contains("rbacCanICheck"))
        XCTAssertTrue(viewModelSource.contains("public var selectedRBACBindingReferencedRole"))
        XCTAssertTrue(viewModelSource.contains("public var selectedRBACRoleRelatedBindings"))
        XCTAssertTrue(viewModelSource.contains("public func openRBACBindingReferencedRole"))
        XCTAssertTrue(viewModelSource.contains("public func openRBACRoleRelatedBinding"))
        XCTAssertFalse(simulatorActionBlock.contains("applyYAML"))
        XCTAssertFalse(simulatorActionBlock.contains("delete"))
        XCTAssertFalse(simulatorActionBlock.contains("rollback"))
    }

    func testLogInspectorTabsReloadWhenSelectedDirectly() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains(".onChange(of: podInspectorTab)"))
        XCTAssertTrue(rootViewSource.contains("if tab == .logs"))
        XCTAssertTrue(rootViewSource.contains(".onChange(of: deploymentInspectorTab)"))
        XCTAssertTrue(rootViewSource.contains("if tab == .unifiedLogs"))
        XCTAssertTrue(rootViewSource.contains(".onChange(of: serviceInspectorTab)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.reloadLogsForSelection()"))
    }

    func testLogReloadErrorsStayInLogInspectorInsteadOfGlobalErrorBanner() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        guard let reloadStart = viewModelSource.range(of: "private func startLogsReloadForSelection"),
              let saveLogsStart = viewModelSource.range(of: "public func saveCurrentLogs", range: reloadStart.upperBound..<viewModelSource.endIndex) else {
            XCTFail("Could not locate log reload implementation in RuneAppViewModel.swift")
            return
        }

        let reloadBlock = String(viewModelSource[reloadStart.lowerBound..<saveLogsStart.lowerBound])
        XCTAssertTrue(reloadBlock.contains("state.setLastLogFetchError"))
        XCTAssertTrue(reloadBlock.contains("state.clearError()"))
        XCTAssertFalse(reloadBlock.contains("state.setError(error)"))
    }

    func testCancelledSnapshotWorkDoesNotSurfaceAsGlobalPartialLoadError() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        let scheduleBlock = try functionBlock(
            named: "private func scheduleRefreshCurrentView",
            endingBefore: "private func performRefreshCurrentView",
            in: viewModelSource
        )
        XCTAssertTrue(scheduleBlock.contains("guard !Task.isCancelled else { return }"))

        let unwrapBlock = try functionBlock(
            named: "private func unwrap<T>",
            endingBefore: "private nonisolated static func capture",
            in: viewModelSource
        )
        XCTAssertTrue(unwrapBlock.contains("Self.isBenignCancellationError(error)"))
        XCTAssertTrue(unwrapBlock.contains("return fallback"))
        XCTAssertTrue(unwrapBlock.contains("warnings.append"))

        guard let namespaceFailure = viewModelSource.range(of: "case let .failure(error):\n                if Self.isBenignCancellationError(error)"),
              let manualNamespaceMode = viewModelSource.range(of: "state.setManualNamespaceMode")
        else {
            XCTFail("Namespace snapshot cancellation handling was not found")
            return
        }
        XCTAssertLessThan(namespaceFailure.lowerBound, manualNamespaceMode.lowerBound)
        XCTAssertFalse(viewModelSource.contains("warnings.append(\"namespaces: \\(error.localizedDescription)\")"))

        XCTAssertTrue(viewModelSource.contains("private nonisolated static func isBenignCancellationError"))
        XCTAssertTrue(viewModelSource.contains("normalized.contains(\"cancelled\") || normalized.contains(\"canceled\")"))
    }

    func testNamespaceMenuExposesManualNamespaceAffordance() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(viewModelSource.contains("public var manualNamespaceOptions"))
        XCTAssertTrue(viewModelSource.contains("currentContextManualNamespaces()"))
        XCTAssertTrue(rootViewSource.contains("private var manualNamespaceMenuOptions"))
        XCTAssertTrue(rootViewSource.contains("Section(\"Manual namespaces\")"))
        XCTAssertTrue(rootViewSource.contains("Label(namespace, systemImage: \"square.and.pencil\")"))
    }

    func testHelmDetailsDiscardStaleLoadsBeforeWritingGlobalErrors() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let helmDetailsBlock = try functionBlock(
            named: "private func loadHelmDetailsForCurrentSelectionAsync() async",
            endingBefore: "private func isCurrentHelmDetailsRequest",
            in: viewModelSource
        )

        XCTAssertTrue(helmDetailsBlock.contains("latestHelmDetailsRequestID = requestID"))
        XCTAssertTrue(helmDetailsBlock.contains("isCurrentHelmDetailsRequest(requestID, context: context, release: release)"))
        XCTAssertTrue(helmDetailsBlock.contains("isCurrentHelmDetailsRequest(requestID)"))
        XCTAssertTrue(helmDetailsBlock.contains("Self.isBenignCancellationError(error)"))
        XCTAssertTrue(helmDetailsBlock.contains("state.setError(error)"))
    }

    func testLogsExposeTailModeAndSessionCache() throws {
        let logsViewSource = try String(contentsOfFile: resourceLogsInspectorViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let stateSource = try String(contentsOfFile: runeAppStatePath, encoding: .utf8)

        XCTAssertTrue(logsViewSource.contains("private var tailControl: some View"))
        XCTAssertTrue(logsViewSource.contains("toolbarIconLabel(t(.resume), systemImage: \"play.fill\""))
        XCTAssertTrue(logsViewSource.contains("toolbarIconLabel(t(.pause), systemImage: \"pause.fill\""))
        XCTAssertTrue(logsViewSource.contains("Button(t(.stopTail))"))
        XCTAssertFalse(logsViewSource.contains("Toggle(\"Tail\""))
        XCTAssertFalse(logsViewSource.contains("Button(isStreamPaused ? \"Resume\" : \"Pause\""))
        XCTAssertTrue(logsViewSource.contains("contentStyle: .ansiLogs"))
        XCTAssertTrue(logsViewSource.contains("Label(t(.saveLogs), systemImage: \"square.and.arrow.down\")"))
        XCTAssertTrue(logsViewSource.contains("Label(t(.more), systemImage: \"ellipsis.circle\")"))
        XCTAssertTrue(logsViewSource.contains("Label(t(.exportVisibleResultsZip), systemImage: \"doc.zipper\")"))
        XCTAssertTrue(logsViewSource.contains("Label(t(.exportFullUnfilteredZip), systemImage: \"archivebox\")"))
        XCTAssertTrue(logsViewSource.contains("Label(t(.exportAllPodsFullZip), systemImage: \"shippingbox\")"))
        XCTAssertTrue(logsViewSource.contains("allowsAutomaticLargeTextSurface: false"))
        XCTAssertTrue(logsViewSource.contains("deferredOutputThreshold"))
        XCTAssertTrue(logsViewSource.contains("ResourceLogSearchResult.makeForInspector"))
        XCTAssertTrue(viewModelSource.contains("isLogTailModeEnabled"))
        XCTAssertTrue(viewModelSource.contains("isLogStreamPaused"))
        XCTAssertTrue(viewModelSource.contains("tailLogsReloadNanoseconds"))
        XCTAssertTrue(viewModelSource.contains("guard isLogTailModeEnabled, !isLogStreamPaused else { return }"))
        XCTAssertTrue(viewModelSource.contains("private func logArchiveMetadata("))
        XCTAssertTrue(viewModelSource.contains("metadata: logArchiveMetadata("))
        XCTAssertTrue(viewModelSource.contains("metadata: self.logArchiveMetadata("))
        XCTAssertTrue(stateSource.contains("sessionLogCache"))
        XCTAssertTrue(stateSource.contains("appendPodLogRead"))
        XCTAssertTrue(stateSource.contains("replacePodLogRead"))
        XCTAssertTrue(stateSource.contains("appendUnifiedServiceLogRead"))
        XCTAssertTrue(stateSource.contains("replaceUnifiedServiceLogRead"))
        XCTAssertTrue(viewModelSource.contains("commitPodLogFetch"))
        XCTAssertTrue(viewModelSource.contains("commitUnifiedLogFetch"))
    }

    func testLaunchExperienceDefersWorkspaceChromeUntilAfterFirstRender() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("@State private var hasMountedWorkspaceChrome = false"))
        XCTAssertTrue(rootViewSource.contains("if shouldMountWorkspaceChrome"))
        XCTAssertTrue(rootViewSource.contains("private var shouldMountWorkspaceChrome"))
        XCTAssertTrue(rootViewSource.contains("private func scheduleWorkspaceChromeMount()"))
        XCTAssertTrue(rootViewSource.contains("try? await Task.sleep(nanoseconds: workspaceChromeMountDelayNanoseconds)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.bootstrapIfNeeded()"))
    }

    func testCenterResourceRowsExposeOperationalContextMenus() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let appKitPodTableSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)

        XCTAssertTrue(appKitPodTableSource.contains("menu.addItem(menuItem(\"Open Logs\""))
        XCTAssertTrue(appKitPodTableSource.contains("menu.addItem(menuItem(\"Describe\""))
        XCTAssertTrue(appKitPodTableSource.contains("menu.addItem(menuItem(\"Open YAML\""))
        XCTAssertTrue(rootViewSource.contains("AppKitDeploymentListView("))
        XCTAssertTrue(appKitPodTableSource.contains("struct AppKitDeploymentListView"))
        XCTAssertTrue(appKitPodTableSource.contains("private enum DeploymentColumn"))
        XCTAssertTrue(appKitPodTableSource.contains("DeploymentListSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortColumn: viewModel.deploymentSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortAscending: viewModel.deploymentSortAscending"))
        XCTAssertTrue(rootViewSource.contains("onToggleSort: viewModel.toggleDeploymentSort"))
        XCTAssertTrue(appKitPodTableSource.contains("let headerView = RuneAppKitResourceTableHeaderView()"))
        XCTAssertTrue(appKitPodTableSource.contains("updateDeploymentSortIndicator"))
        XCTAssertTrue(appKitPodTableSource.contains("func tableView(_ tableView: NSTableView, didClick tableColumn: NSTableColumn)"))
        XCTAssertTrue(appKitPodTableSource.contains("case replicas"))
        XCTAssertTrue(appKitPodTableSource.contains("case favorite"))
        XCTAssertTrue(appKitPodTableSource.contains("deploymentTrailingBreathingRoom: CGFloat = 14"))
        XCTAssertTrue(appKitPodTableSource.contains("resourceMaximumContentWidth: CGFloat = 860"))
        XCTAssertTrue(appKitPodTableSource.contains("deploymentMaximumContentWidth: CGFloat = 620"))
        XCTAssertTrue(appKitPodTableSource.contains("serviceMaximumContentWidth: CGFloat = 740"))
        XCTAssertTrue(appKitPodTableSource.contains("genericMaximumContentWidth: CGFloat = 820"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceListLayout.deploymentColumnWidths(visibleWidth: visibleWidth)"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.enclosingScrollView?.contentView.bounds.width"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.setFrameSize(NSSize(width: tableWidth, height: tableView.frame.height))"))
        XCTAssertTrue(appKitPodTableSource.contains("static func renderedTableWidth(in tableView: NSTableView) -> CGFloat"))
        XCTAssertTrue(appKitPodTableSource.contains("actionColumnTrailingPadding: CGFloat = 18"))
        XCTAssertTrue(appKitPodTableSource.contains("columnContentWidth(in: tableView) + (rowHorizontalInset * 2) + actionColumnTrailingPadding"))
        XCTAssertTrue(appKitPodTableSource.contains("static func updateRenderedTableWidth(on tableView: NSTableView?, updatesVisibleCellsImmediately: Bool = true)"))
        XCTAssertTrue(appKitPodTableSource.contains("headerView.setFrameSize(headerSize)"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.headerView?.needsDisplay = true"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.needsLayout = true"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.layoutSubtreeIfNeeded()"))
        XCTAssertTrue(appKitPodTableSource.contains("scrollView.reflectScrolledClipView(scrollView.contentView)"))
        XCTAssertTrue(appKitPodTableSource.contains("let visibleRows = tableView.rows(in: tableView.visibleRect)"))
        XCTAssertTrue(appKitPodTableSource.contains("rowView.layoutSubtreeIfNeeded()"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.view(atColumn: column, row: row, makeIfNecessary: false)"))
        XCTAssertTrue(appKitPodTableSource.contains("cellView.layoutSubtreeIfNeeded()"))
        XCTAssertTrue(appKitPodTableSource.contains("tableView.displayIfNeeded()"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableStyle.updateRenderedTableWidth(on: tableView)"))
        XCTAssertFalse(appKitPodTableSource.contains("let tableWidth = RuneAppKitResourceTableStyle.renderedTableWidth(in: tableView)\n            if abs(tableView.frame.width - tableWidth) >= 1"))
        XCTAssertTrue(appKitPodTableSource.contains("applyImmediateResourceContextMenuSelection"))
        XCTAssertTrue(appKitPodTableSource.contains("coordinator?.selectRowForContextMenu(row, in: tableView)"))
        XCTAssertTrue(appKitPodTableSource.contains("return onContextMenu?(row, self)"))
        XCTAssertTrue(appKitPodTableSource.contains("DispatchQueue.main.async { [weak self]"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceListScrollView"))
        XCTAssertTrue(appKitPodTableSource.contains("onVisibleWidthChanged"))
        XCTAssertTrue(appKitPodTableSource.contains("override func layout()"))
        XCTAssertTrue(appKitPodTableSource.contains("coordinator?.updateColumnWidths()"))
        XCTAssertTrue(appKitPodTableSource.contains("let renderedWidth = tableView.map(RuneAppKitResourceTableStyle.renderedTableWidth(in:))"))
        XCTAssertTrue(appKitPodTableSource.contains("let contentWidth = (superview as? NSTableView)"))
        XCTAssertTrue(appKitPodTableSource.contains(".map(RuneAppKitResourceTableStyle.renderedTableWidth(in:))"))
        XCTAssertTrue(appKitPodTableSource.contains(".tableColumns"))
        XCTAssertTrue(appKitPodTableSource.contains("min(bounds.width, contentWidth)"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitFavoriteButtonCell"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceTableRowView(horizontalInset: RuneAppKitResourceTableStyle.rowHorizontalInset)"))
        XCTAssertFalse(rootViewSource.contains("List(viewModel.visibleDeployments)"))
        XCTAssertTrue(appKitPodTableSource.contains("menu.addItem(menuItem(\"Open Unified Logs\""))
        XCTAssertTrue(appKitPodTableSource.contains("menu.addItem(menuItem(\"Open Rollout\""))
        XCTAssertTrue(rootViewSource.contains("AppKitServiceListView("))
        XCTAssertTrue(appKitPodTableSource.contains("struct AppKitServiceListView"))
        XCTAssertTrue(appKitPodTableSource.contains("private enum ServiceColumn"))
        XCTAssertTrue(appKitPodTableSource.contains("ServiceListSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortColumn: viewModel.serviceSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortAscending: viewModel.serviceSortAscending"))
        XCTAssertTrue(rootViewSource.contains("onToggleSort: viewModel.toggleServiceSort"))
        XCTAssertTrue(appKitPodTableSource.contains("updateServiceSortIndicator"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceListLayout.serviceColumnWidths(visibleWidth: visibleWidth)"))
        XCTAssertFalse(rootViewSource.contains("serviceResourceContextMenu(service)"))
        XCTAssertTrue(appKitPodTableSource.contains("menu.addItem(menuItem(\"Port Forward\""))
        XCTAssertTrue(rootViewSource.contains("AppKitGenericResourceListView("))
        XCTAssertTrue(appKitPodTableSource.contains("struct AppKitGenericResourceListView"))
        XCTAssertTrue(appKitPodTableSource.contains("private enum GenericResourceColumn"))
        XCTAssertTrue(appKitPodTableSource.contains("GenericResourceListSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortColumn: viewModel.genericResourceSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortAscending: viewModel.genericResourceSortAscending"))
        XCTAssertTrue(rootViewSource.contains("onToggleSort: viewModel.toggleGenericResourceSort"))
        XCTAssertTrue(appKitPodTableSource.contains("updateGenericSortIndicator"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceListLayout.genericColumnWidths(visibleWidth: visibleWidth)"))
        XCTAssertFalse(rootViewSource.contains("genericResourceContextMenu(resource, action: action)"))
        XCTAssertTrue(appKitPodTableSource.contains("menu.addItem(menuItem(\"Open YAML\""))
        XCTAssertTrue(appKitPodTableSource.contains("menu.addItem(menuItem(\"Describe\""))
        XCTAssertTrue(rootViewSource.contains("AppKitHelmReleaseListView("))
        XCTAssertTrue(rootViewSource.contains("enum HelmBrowserTab"))
        XCTAssertTrue(rootViewSource.contains("@State private var helmBrowserTab: HelmBrowserTab = .releases"))
        XCTAssertTrue(rootViewSource.contains("RuneSegmentedPickerInScroll("))
        XCTAssertTrue(rootViewSource.contains("get: { helmBrowserTab }"))
        XCTAssertTrue(rootViewSource.contains("viewModel.setHelmBrowserResourceFamily(tab.resourceListFamily)"))
        XCTAssertTrue(rootViewSource.contains("case .releases:"))
        XCTAssertTrue(rootViewSource.contains("helmReleaseBrowser"))
        XCTAssertTrue(rootViewSource.contains("case .operatorResources:"))
        XCTAssertFalse(rootViewSource.contains("Text(\"Operator Views\")"))
        XCTAssertTrue(appKitPodTableSource.contains("struct AppKitHelmReleaseListView"))
        XCTAssertTrue(appKitPodTableSource.contains("private enum HelmReleaseColumn"))
        XCTAssertTrue(appKitPodTableSource.contains("HelmReleaseListSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortColumn: viewModel.helmReleaseSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortAscending: viewModel.helmReleaseSortAscending"))
        XCTAssertTrue(rootViewSource.contains("onToggleSort: viewModel.toggleHelmReleaseSort"))
        XCTAssertTrue(appKitPodTableSource.contains("updateHelmSortIndicator"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceListLayout.helmColumnWidths(visibleWidth: visibleWidth)"))
        XCTAssertFalse(rootViewSource.contains("ForEach(viewModel.visibleHelmReleases)"))
        XCTAssertTrue(rootViewSource.contains("AppKitEventListView("))
        XCTAssertTrue(appKitPodTableSource.contains("struct AppKitEventListView"))
        XCTAssertTrue(appKitPodTableSource.contains("private enum EventColumn"))
        XCTAssertTrue(appKitPodTableSource.contains("EventListSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortColumn: viewModel.eventSortColumn"))
        XCTAssertTrue(rootViewSource.contains("sortAscending: viewModel.eventSortAscending"))
        XCTAssertTrue(rootViewSource.contains("onToggleSort: viewModel.toggleEventSort"))
        XCTAssertTrue(appKitPodTableSource.contains("updateEventSortIndicator"))
        XCTAssertTrue(appKitPodTableSource.contains("RuneAppKitResourceListLayout.eventColumnWidths(visibleWidth: visibleWidth)"))
        XCTAssertFalse(rootViewSource.contains("List(viewModel.visibleEvents)"))
        XCTAssertTrue(appKitPodTableSource.contains("Open Unified Logs"))
        XCTAssertTrue(rootViewSource.contains("Open YAML"))
        XCTAssertTrue(rootViewSource.contains("Describe"))
    }

    func testAppKitFavoriteResourceListsExposeKeyboardToggle() throws {
        let source = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)

        XCTAssertTrue(source.contains("private func runeAppKitEventIsFavoriteToggle(_ event: NSEvent) -> Bool"))
        XCTAssertTrue(source.contains("let disallowedModifiers: NSEvent.ModifierFlags = [.command, .control, .option]"))
        XCTAssertTrue(source.contains("event.charactersIgnoringModifiers?.lowercased() == \"f\""))

        XCTAssertEqual(
            source.components(
                separatedBy: "final class RuneAppKitResourceTableView: NSTableView"
            ).count - 1,
            1
        )
        XCTAssertEqual(source.components(separatedBy: "override func keyDown(with event: NSEvent)").count - 1, 1)
        XCTAssertEqual(source.components(separatedBy: "runeAppKitEventIsFavoriteToggle(event)").count - 1, 1)
        XCTAssertEqual(source.components(separatedBy: "onFavoriteToggle: {").count - 1, 5)
        XCTAssertTrue(source.contains("onFavoriteToggle()"))
        XCTAssertEqual(source.components(separatedBy: "func toggleFavoriteForSelectedRow(in tableView: NSTableView)").count - 1, 5)
    }

    func testGenericResourceBulkSelectionExposesQuickCompareAction() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("Label(\"Compare\", systemImage: \"rectangle.split.2x1\")"))
        XCTAssertTrue(rootViewSource.contains("isGenericResourceComparisonPresented = true"))
        XCTAssertTrue(rootViewSource.contains("genericResourceComparisonPopover"))
        XCTAssertTrue(rootViewSource.contains("Button(\"Copy Summary\")"))
        XCTAssertTrue(rootViewSource.contains("viewModel.copySelectedGenericResourceComparisonToClipboard()"))
        XCTAssertTrue(rootViewSource.contains("viewModel.canCopySelectedGenericResourceComparison"))
        XCTAssertTrue(viewModelSource.contains("selectedGenericResourceComparisonText"))
        XCTAssertTrue(viewModelSource.contains("copySelectedGenericResourceComparisonToClipboard"))
    }

    func testOverviewClusterSignalsExposeHoverExplanations() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let clusterSignalsSource = try String(contentsOfFile: overviewClusterSignalsPanelViewPath, encoding: .utf8)
        let recentEventsSource = try String(contentsOfFile: overviewRecentEventsPanelViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("OverviewClusterSignalsPanelView("))
        XCTAssertTrue(rootViewSource.contains("OverviewRecentEventsPanelView("))
        XCTAssertTrue(rootViewSource.contains("overviewEventsCardHelp"))
        XCTAssertTrue(rootViewSource.contains(".runeHelp(viewModel.state.selectedEvent.map(eventHint(for:)) ?? \"\", enabled: showHoverTooltips)"))
        XCTAssertTrue(clusterSignalsSource.contains("overviewClusterSignalsHelp"))
        XCTAssertTrue(clusterSignalsSource.contains("overviewInsightHelp(for:"))
        XCTAssertTrue(clusterSignalsSource.contains("overviewSignalRowHelp("))
        XCTAssertTrue(clusterSignalsSource.contains("gitOpsSection(id: .gitOps, items: gitOpsRollups)"))
        XCTAssertTrue(clusterSignalsSource.contains("overviewGitOpsRowHelp("))
        XCTAssertTrue(clusterSignalsSource.contains("onOpenGitOpsRollup(item)"))
        XCTAssertTrue(clusterSignalsSource.contains("title: \"GitOps\""))
        XCTAssertTrue(clusterSignalsSource.contains("overviewDependencyRowHelp("))
        XCTAssertTrue(clusterSignalsSource.contains("Flux and ArgoCD rollups from loaded custom resources"))
        XCTAssertTrue(clusterSignalsSource.contains("Warning Kubernetes Events promoted into a short incident timeline"))
        XCTAssertTrue(clusterSignalsSource.contains(".runeHelp(overviewClusterSignalsHelp, enabled: showHoverTooltips)"))
        XCTAssertTrue(clusterSignalsSource.contains(".runeHelp(help, enabled: showHoverTooltips)"))
        XCTAssertTrue(rootViewSource.contains("gitOpsRollups: viewModel.overviewGitOpsRollupItems"))
        XCTAssertTrue(rootViewSource.contains("onOpenGitOpsRollup: viewModel.openOverviewGitOpsRollup"))
        XCTAssertTrue(recentEventsSource.contains("Recent Events shows raw Kubernetes Event objects"))
        XCTAssertTrue(recentEventsSource.contains("overviewEventRowHelp("))
        XCTAssertTrue(recentEventsSource.contains("@AppStorage(RuneSettingsKeys.showHoverTooltips)"))
        XCTAssertTrue(recentEventsSource.contains(".runeHelp(overviewRecentEventsHelp, enabled: showHoverTooltips)"))
        XCTAssertTrue(recentEventsSource.contains(".runeHelp(overviewEventRowHelp(event), enabled: showHoverTooltips)"))
    }

    func testToolbarUsesNavigationPaneTogglesAndIconActions() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        guard let toolbarStart = rootViewSource.range(of: ".toolbar {"),
              let toolbarEnd = rootViewSource.range(of: ".toolbarBackground", range: toolbarStart.upperBound..<rootViewSource.endIndex) else {
            XCTFail("Could not locate main toolbar block in RuneRootView.swift")
            return
        }

        let toolbarBlock = String(rootViewSource[toolbarStart.lowerBound..<toolbarEnd.lowerBound])
        XCTAssertTrue(toolbarBlock.contains("Image(systemName: \"sidebar.left\")"))
        XCTAssertTrue(toolbarBlock.contains("Image(systemName: \"sidebar.right\")"))
        XCTAssertTrue(toolbarBlock.contains("Image(systemName: \"arrow.clockwise\")"))
        XCTAssertTrue(toolbarBlock.contains("Image(systemName: \"command\")"))
        XCTAssertTrue(toolbarBlock.contains("Image(systemName: \"gearshape\")"))
        XCTAssertTrue(toolbarBlock.contains(".keyboardShortcut(.leftArrow, modifiers: [.command, .option])"))
        XCTAssertTrue(toolbarBlock.contains(".keyboardShortcut(.rightArrow, modifiers: [.command, .option])"))
        XCTAssertFalse(toolbarBlock.contains("Button(\"Palette\")"))
        XCTAssertFalse(toolbarBlock.contains("Button(\"Reload\")"))
    }

    func testHistoryKeyboardMonitorAcceptsCommandOptionArrowKeys() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(rootViewSource.contains("configuredActionBaseKey(for event: NSEvent)"))
        XCTAssertTrue(rootViewSource.contains("configuredActionShiftedSymbolKey(for event: NSEvent)"))
        XCTAssertTrue(rootViewSource.contains("symbolModifiers.remove(.shift)"))
        XCTAssertTrue(rootViewSource.contains("case 123:"))
        XCTAssertTrue(rootViewSource.contains("return \"left\""))
        XCTAssertTrue(rootViewSource.contains("case 124:"))
        XCTAssertTrue(rootViewSource.contains("return \"right\""))
        XCTAssertTrue(rootViewSource.contains("let disallowedModifiers: NSEvent.ModifierFlags = [.function]"))
    }

    func testTabNavigationCanSkipClusterPaneFromSections() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let preferencesSource = try String(contentsOfFile: runePreferencesViewPath, encoding: .utf8)
        let settingsSource = try String(contentsOfFile: runeSettingsKeysPath, encoding: .utf8)
        let tabHandlingBlock = try functionBlock(
            named: "private func shouldHandleTabNavigation(_ event: NSEvent) -> Bool",
            endingBefore: "private func shouldHandleConfiguredActionKey",
            in: rootViewSource
        )

        XCTAssertTrue(settingsSource.contains("skipClusterOnTabNavigationFromSections"))
        XCTAssertTrue(settingsSource.contains("skipClusterOnTabNavigationFromSections: false"))
        XCTAssertTrue(preferencesSource.contains("@AppStorage(RuneSettingsKeys.skipClusterOnTabNavigationFromSections)"))
        XCTAssertTrue(preferencesSource.contains("Skip Cluster on Tab navigation from Sections"))
        XCTAssertTrue(rootViewSource.contains("@AppStorage(RuneSettingsKeys.skipClusterOnTabNavigationFromSections)"))
        XCTAssertTrue(rootViewSource.contains("RuneRootKeyboardPaneNavigation.availablePanes("))
        XCTAssertTrue(tabHandlingBlock.contains("RuneRootKeyboardWindowScope.owns("))
        XCTAssertTrue(tabHandlingBlock.contains("workspaceWindowReference.windowNumber"))
        XCTAssertTrue(tabHandlingBlock.contains("keyWindowNumber: NSApp.keyWindow?.windowNumber"))
        XCTAssertTrue(tabHandlingBlock.contains("textInputFocus != nil || !navigationContext.editableTextResponderActive"))
        XCTAssertTrue(rootViewSource.contains("onOwningWindowChange: { windowNumber in"))
        XCTAssertTrue(rootViewSource.contains("workspaceWindowReference.windowNumber = windowNumber"))
        XCTAssertFalse(rootViewSource.contains("@State private var workspaceWindowNumber"))
        XCTAssertFalse(rootViewSource.contains("workspaceWindowNumber = window?.windowNumber"))
        XCTAssertFalse(rootViewSource.contains("workspaceWindowReference.windowNumber = nil"))
        XCTAssertTrue(rootViewSource.contains("NSApp.keyWindow?.makeFirstResponder(nil)"))
    }

    func testTabNavigationPaneSequenceIncludesOrSkipsClusterAccordingToSetting() {
        let includingCluster = RuneRootKeyboardPaneNavigation.availablePanes(
            sidebarVisible: true,
            detailVisible: true,
            skipsClusterPane: false
        )
        let skippingCluster = RuneRootKeyboardPaneNavigation.availablePanes(
            sidebarVisible: true,
            detailVisible: true,
            skipsClusterPane: true
        )

        XCTAssertEqual(includingCluster, [.sidebarSections, .sidebarContexts, .content, .detail])
        XCTAssertEqual(skippingCluster, [.sidebarSections, .content, .detail])
        XCTAssertEqual(
            RuneRootKeyboardPaneNavigation.advanced(from: .sidebarSections, forward: true, in: skippingCluster),
            .content
        )
        XCTAssertEqual(
            RuneRootKeyboardPaneNavigation.advanced(from: .content, forward: true, in: skippingCluster),
            .detail
        )
        XCTAssertEqual(
            RuneRootKeyboardPaneNavigation.advanced(from: .detail, forward: true, in: skippingCluster),
            .sidebarSections
        )
        XCTAssertEqual(
            RuneRootKeyboardPaneNavigation.advanced(from: .sidebarSections, forward: false, in: skippingCluster),
            .detail
        )
    }

    func testKeyboardWindowScopeRejectsMissingAndForeignWindows() {
        XCTAssertFalse(RuneRootKeyboardWindowScope.owns(
            eventWindowNumber: 41,
            workspaceWindowNumber: nil,
            keyWindowNumber: 41
        ))
        XCTAssertFalse(RuneRootKeyboardWindowScope.owns(
            eventWindowNumber: 41,
            workspaceWindowNumber: 42,
            keyWindowNumber: 42
        ))
        XCTAssertTrue(RuneRootKeyboardWindowScope.owns(
            eventWindowNumber: 42,
            workspaceWindowNumber: 42,
            keyWindowNumber: nil
        ))
        XCTAssertTrue(RuneRootKeyboardWindowScope.owns(
            eventWindowNumber: 0,
            workspaceWindowNumber: 42,
            keyWindowNumber: 42
        ))
        XCTAssertFalse(RuneRootKeyboardWindowScope.owns(
            eventWindowNumber: 0,
            workspaceWindowNumber: 42,
            keyWindowNumber: 43
        ))
    }

    func testTabNavigationSkipsClusterThroughRealAppKitWindowEvents() async throws {
        let settingKey = RuneSettingsKeys.skipClusterOnTabNavigationFromSections
        let previousSetting = UserDefaults.standard.object(forKey: settingKey)
        UserDefaults.standard.set(false, forKey: settingKey)
        defer {
            if let previousSetting {
                UserDefaults.standard.set(previousSetting, forKey: settingKey)
            } else {
                UserDefaults.standard.removeObject(forKey: settingKey)
            }
        }

        let state = RuneAppState()
        let viewModel = RuneAppViewModel(state: state)
        viewModel.loadDemoCluster()
        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.pod)

        let host = NSHostingController(
            rootView: RuneRootView(
                viewModel: viewModel,
                onLayoutSnapshotChange: nil,
                debugDisableBootstrap: true
            )
            .frame(width: 1_280, height: 800)
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1_280, height: 800),
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentViewController = host
        window.makeKeyAndOrderFront(nil)
        defer {
            window.contentViewController = nil
            window.orderOut(nil)
        }
        try await settle(window: window)
        XCTAssertEqual(state.selectedSection, .workloads)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)

        UserDefaults.standard.set(true, forKey: settingKey)
        try await settle(window: window)

        NSApp.sendEvent(keyDownEvent(
            window: window,
            characters: "\t",
            charactersIgnoringModifiers: "\t",
            keyCode: 48
        ))
        try await settle(window: window)
        NSApp.sendEvent(keyDownEvent(
            window: window,
            characters: "\u{F703}",
            charactersIgnoringModifiers: "\u{F703}",
            keyCode: 124
        ))
        try await settle(window: window)

        XCTAssertEqual(
            state.selectedWorkloadKind,
            .deployment,
            "Tab from Sections must skip Contexts and put Right-arrow navigation in the content pane."
        )

        NSApp.sendEvent(keyDownEvent(
            window: window,
            characters: "\u{F702}",
            charactersIgnoringModifiers: "\u{F702}",
            keyCode: 123
        ))
        try await settle(window: window)
        XCTAssertEqual(state.selectedWorkloadKind, .pod)

        NSApp.sendEvent(keyDownEvent(
            window: window,
            modifierFlags: .shift,
            characters: "\t",
            charactersIgnoringModifiers: "\t",
            keyCode: 48
        ))
        try await settle(window: window)
        NSApp.sendEvent(keyDownEvent(
            window: window,
            characters: "\t",
            charactersIgnoringModifiers: "\t",
            keyCode: 48
        ))
        try await settle(window: window)
        NSApp.sendEvent(keyDownEvent(
            window: window,
            characters: "\u{F703}",
            charactersIgnoringModifiers: "\u{F703}",
            keyCode: 124
        ))
        try await settle(window: window)

        XCTAssertEqual(
            state.selectedWorkloadKind,
            .deployment,
            "Shift-Tab from content must return to Sections, so the next Tab reaches content instead of the skipped Contexts pane."
        )
    }

    func testKeyboardNavigationIsScopedToItsOwningAppKitWindow() async throws {
        let settingKey = RuneSettingsKeys.skipClusterOnTabNavigationFromSections
        let previousSetting = UserDefaults.standard.object(forKey: settingKey)
        UserDefaults.standard.set(true, forKey: settingKey)
        defer {
            if let previousSetting {
                UserDefaults.standard.set(previousSetting, forKey: settingKey)
            } else {
                UserDefaults.standard.removeObject(forKey: settingKey)
            }
        }

        func makeWorkspace() -> (RuneAppState, NSHostingController<AnyView>, NSWindow) {
            let state = RuneAppState()
            let viewModel = RuneAppViewModel(state: state)
            viewModel.loadDemoCluster()
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.pod)
            let host = NSHostingController(
                rootView: AnyView(RuneRootView(
                    viewModel: viewModel,
                    onLayoutSnapshotChange: nil,
                    debugDisableBootstrap: true
                )
                .frame(width: 1_280, height: 800))
            )
            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 1_280, height: 800),
                styleMask: [.titled, .closable, .resizable],
                backing: .buffered,
                defer: false
            )
            window.contentViewController = host
            return (state, host, window)
        }

        let first = makeWorkspace()
        let second = makeWorkspace()
        first.2.makeKeyAndOrderFront(nil)
        try await settle(window: first.2)
        second.2.makeKeyAndOrderFront(nil)
        try await settle(window: second.2)
        first.2.makeKeyAndOrderFront(nil)
        try await settle(window: first.2)
        defer {
            first.2.contentViewController = nil
            second.2.contentViewController = nil
            first.2.orderOut(nil)
            second.2.orderOut(nil)
            _ = first.1
            _ = second.1
        }
        NSApp.sendEvent(keyDownEvent(
            window: first.2,
            characters: "\t",
            charactersIgnoringModifiers: "\t",
            keyCode: 48
        ))
        NSApp.sendEvent(keyDownEvent(
            window: first.2,
            characters: "\u{F703}",
            charactersIgnoringModifiers: "\u{F703}",
            keyCode: 124
        ))
        try await settle(window: first.2)

        XCTAssertEqual(first.0.selectedWorkloadKind, .deployment)
        XCTAssertEqual(second.0.selectedWorkloadKind, .pod, "The inactive second window must remain unchanged.")
    }

    func testDetailPaneArrowKeysCycleInspectorTabsAcrossSections() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let navigationContextBlock = try functionBlock(
            named: "private var keyboardNavigationContext: RuneRootKeyboardNavigationContext",
            endingBefore: "private var keyboardNavigationSuspended: Bool",
            in: rootViewSource
        )
        let localKeyBlock = try functionBlock(
            named: "private func handleLocalKeyEvent(_ event: NSEvent) -> NSEvent?",
            endingBefore: "private func shouldHandleTabNavigation",
            in: rootViewSource
        )
        let arrowGuardBlock = try functionBlock(
            named: "private func shouldHandlePaneArrowNavigation(_ event: NSEvent) -> Bool",
            endingBefore: "private func configuredAction(for event: NSEvent) -> RuneKeyBindingAction?",
            in: rootViewSource
        )
        let detailMoveBlock = try functionBlock(
            named: "private func moveDetailInspectorTab(_ direction: MoveCommandDirection)",
            endingBefore: "private func advancedTab",
            in: rootViewSource
        )

        XCTAssertTrue(navigationContextBlock.contains("RuneRootKeyboardNavigationContext("))
        XCTAssertTrue(navigationContextBlock.contains("kubeConfigImportReviewPresented:"))
        XCTAssertTrue(navigationContextBlock.contains("addClusterPopoverPresented:"))
        XCTAssertTrue(navigationContextBlock.contains("addClusterProviderPresented:"))
        XCTAssertTrue(navigationContextBlock.contains("pendingWriteConfirmationPresented:"))
        XCTAssertTrue(navigationContextBlock.contains("editableTextResponderActive:"))
        XCTAssertTrue(localKeyBlock.contains("shouldHandlePaneArrowNavigation(event)"))
        XCTAssertTrue(localKeyBlock.contains("moveKeyboardSelection(.left)"))
        XCTAssertTrue(localKeyBlock.contains("moveKeyboardSelection(.right)"))
        XCTAssertTrue(arrowGuardBlock.contains("event.keyCode == 123 || event.keyCode == 124"))
        XCTAssertTrue(arrowGuardBlock.contains("keyboardPaneFocus == .detail || (keyboardPaneFocus == .content && viewModel.state.selectedSection != .terminal)"))
        XCTAssertTrue(arrowGuardBlock.contains("[.command, .option, .control, .shift, .function]"))
        XCTAssertTrue(rootViewSource.contains("helmBrowserTab = advancedTab(current: helmBrowserTab, direction: direction)"))
        XCTAssertTrue(detailMoveBlock.contains("case .operatorResource:"))
        XCTAssertTrue(detailMoveBlock.contains("genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)"))
        XCTAssertTrue(detailMoveBlock.contains("case .release:"))
        XCTAssertTrue(detailMoveBlock.contains("helmInspectorTab = advancedTab(current: helmInspectorTab, direction: direction)"))
        XCTAssertTrue(detailMoveBlock.contains("case .terminal:"))
        XCTAssertTrue(detailMoveBlock.contains("terminalInspectorTab = advancedTab(current: terminalInspectorTab, direction: direction)"))
    }

    func testOperatorResourcesSupportDescribeAndYAMLKeyboardNavigation() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let describeBlock = try functionBlock(
            named: "private func openDescribeInspectorForSelection() -> Bool",
            endingBefore: "private func openYAMLInspectorForSelection",
            in: rootViewSource
        )
        let yamlBlock = try functionBlock(
            named: "private func openYAMLInspectorForSelection() -> Bool",
            endingBefore: "private func openYAMLEditorForSelection",
            in: rootViewSource
        )

        for block in [describeBlock, yamlBlock] {
            XCTAssertTrue(block.contains("case .helm:"))
            XCTAssertTrue(block.contains("guard selectedHelmInspectorMode == .operatorResource else { return false }"))
            XCTAssertTrue(block.contains("genericResourceManifestTab ="))
        }
        XCTAssertTrue(describeBlock.contains("genericResourceManifestTab = .describe"))
        XCTAssertTrue(yamlBlock.contains("genericResourceManifestTab = .yaml"))
    }

    func testPreferencesExposeArrowKeysForHistoryBindings() throws {
        let testFile = URL(fileURLWithPath: #filePath)
        let root = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let preferencesPath = root.appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift").path
        let preferencesSource = try String(contentsOfFile: preferencesPath, encoding: .utf8)

        XCTAssertTrue(preferencesSource.contains("[\"[\", \"]\", \"/\", \".\", \":\", \"?\", \"left\", \"right\"]"))
    }

    func testPreferencesExposeSupportedK9sStyleActionBindings() throws {
        let testFile = URL(fileURLWithPath: #filePath)
        let root = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let preferencesPath = root.appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift").path
        let keyBindingsPath = root.appendingPathComponent("Sources/RuneCore/Models/RuneKeyBindings.swift").path
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let preferencesSource = try String(contentsOfFile: preferencesPath, encoding: .utf8)
        let keyBindingsSource = try String(contentsOfFile: keyBindingsPath, encoding: .utf8)

        XCTAssertTrue(preferencesSource.contains("Button(\"Reset to default\")"))
        XCTAssertFalse(preferencesSource.contains("Reset to k9s-style defaults"))
        XCTAssertTrue(preferencesSource.contains("Swedish keyboards can also bind Command Palette to `Shift-.`"))
        XCTAssertTrue(preferencesSource.contains("Toggle(\"⌃\""))
        XCTAssertTrue(keyBindingsSource.contains("case commandPalette"))
        XCTAssertTrue(keyBindingsSource.contains("case filterResources"))
        XCTAssertTrue(keyBindingsSource.contains("case edit"))
        XCTAssertTrue(keyBindingsSource.contains("case delete"))
        XCTAssertTrue(keyBindingsSource.contains("case saveLogs"))
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \":\", requiresShift: false)"))
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \"/\", requiresShift: false)"))
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \"e\", requiresShift: false)"))
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \"d\", requiresShift: false, requiresControl: true)"))
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \"s\", requiresShift: false, requiresCommand: true)"))
        XCTAssertTrue(keyBindingsSource.contains("RuneKeyboardShortcut(key: \"s\", requiresShift: false, requiresControl: true)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.presentCommandPalette()"))
        XCTAssertTrue(rootViewSource.contains("return focusResourceFilterFromKeyBinding()"))
        XCTAssertTrue(rootViewSource.contains("return openYAMLEditorForSelection()"))
        XCTAssertTrue(rootViewSource.contains("return saveCurrentDetailFromKeyBinding()"))
        XCTAssertTrue(rootViewSource.contains("return deleteSelectionFromKeyBinding()"))
    }

    func testSaveShortcutExportsFocusedDetailPanel() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let saveBlock = try functionBlock(
            named: "private func saveCurrentDetailFromKeyBinding() -> Bool",
            endingBefore: "private func openShellOrScaleInspectorForSelection",
            in: rootViewSource
        )

        XCTAssertTrue(saveBlock.contains("guard keyboardPaneFocus == .content || keyboardPaneFocus == .detail else { return false }"))
        XCTAssertTrue(saveBlock.contains("podInspectorTab"))
        XCTAssertTrue(saveBlock.contains("deploymentInspectorTab"))
        XCTAssertTrue(saveBlock.contains("serviceInspectorTab"))
        XCTAssertTrue(saveBlock.contains("genericResourceManifestTab"))
        XCTAssertTrue(saveBlock.contains("helmInspectorTab"))
        XCTAssertTrue(saveBlock.contains("switch selectedHelmInspectorMode"))
        XCTAssertTrue(saveBlock.contains("case .operatorResource:"))
        XCTAssertTrue(saveBlock.contains("viewModel.saveCurrentLogs()"))
        XCTAssertTrue(saveBlock.contains("viewModel.saveCurrentResourceYAML()"))
        XCTAssertTrue(saveBlock.contains("viewModel.saveCurrentResourceDescribe()"))
        XCTAssertTrue(saveBlock.contains("viewModel.saveCurrentRolloutHistory()"))
        XCTAssertTrue(saveBlock.contains("viewModel.saveCurrentHelmValues()"))
        XCTAssertTrue(saveBlock.contains("viewModel.saveCurrentHelmManifest()"))
        XCTAssertTrue(saveBlock.contains("viewModel.saveCurrentHelmHistory()"))
    }

    func testAppCommandsExposeHistoryArrowShortcuts() throws {
        let testFile = URL(fileURLWithPath: #filePath)
        let root = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let appPath = root.appendingPathComponent("Sources/RuneApp/RuneApp.swift").path
        let appSource = try String(contentsOfFile: appPath, encoding: .utf8)

        XCTAssertTrue(appSource.contains("Button(\"Back\")"))
        XCTAssertTrue(appSource.contains("Button(\"Forward\")"))
        XCTAssertTrue(appSource.contains(".keyboardShortcut(.leftArrow, modifiers: [.command, .option])"))
        XCTAssertTrue(appSource.contains(".keyboardShortcut(.rightArrow, modifiers: [.command, .option])"))
    }

    func testAppShortcutsExposeParameterizedContextAndWorkspaceLaunches() throws {
        let shortcutsSource = try String(contentsOfFile: runeAppShortcutsPath, encoding: .utf8)
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)

        XCTAssertTrue(shortcutsSource.contains("struct OpenRuneContextIntent: AppIntent"))
        XCTAssertTrue(shortcutsSource.contains("@Parameter(title: \"Context Name\")"))
        XCTAssertTrue(shortcutsSource.contains("RunePendingLaunchRequest(action: .recentContexts, query: contextName)"))
        XCTAssertTrue(shortcutsSource.contains("struct OpenRuneWorkspaceIntent: AppIntent"))
        XCTAssertTrue(shortcutsSource.contains("@Parameter(title: \"Workspace Name\")"))
        XCTAssertTrue(shortcutsSource.contains("RunePendingLaunchRequest(action: .savedWorkspaces, query: workspaceName)"))
        XCTAssertTrue(rootSource.contains("consumeRunePendingLaunchRequest()"))
        XCTAssertTrue(rootSource.contains("launchCommandPaletteQuery(prefix: \":ctx\", query: request.query)"))
        XCTAssertTrue(rootSource.contains("launchCommandPaletteQuery(prefix: \":ws\", query: request.query)"))
    }

    func testContextMenuDeleteOnlyArmsConfirmationWithoutSelectingOrReloading() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)

        XCTAssertTrue(viewModelSource.contains("func requestDeleteResource(kind: KubeResourceKind, name: String)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.requestDeleteResource(kind: .pod, name: pod.name)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.requestDeleteResource(kind: .deployment, name: deployment.name)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.requestDeleteResource(kind: .service, name: service.name)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.requestDeleteResource(kind: resource.kind, name: resource.name)"))
        XCTAssertFalse(rootViewSource.contains("viewModel.selectPod(pod)\n            viewModel.requestDeleteSelectedResource()"))
        XCTAssertFalse(rootViewSource.contains("viewModel.selectDeployment(deployment)\n            viewModel.requestDeleteSelectedResource()"))
        XCTAssertFalse(rootViewSource.contains("viewModel.selectService(service)\n            viewModel.requestDeleteSelectedResource()"))
        XCTAssertFalse(rootViewSource.contains("action(resource)\n            viewModel.requestDeleteSelectedResource()"))
    }

    func testWriteRefreshDoesNotDuplicateInspectorDetailsLoad() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let confirmBlock = try functionBlock(
            named: "public func confirmPendingWriteAction()",
            endingBefore: "private func auditDetails",
            in: viewModelSource
        )

        XCTAssertTrue(confirmBlock.contains("try await loadResourceSnapshot("))
        XCTAssertFalse(confirmBlock.contains("shouldReloadResourceInspectorAfterWrite"))
        XCTAssertFalse(confirmBlock.contains("loadResourceDetailsForCurrentSelection()"))
    }

    func testProductionDestructiveWriteHasSecondConfirmationGate() throws {
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let confirmBlock = try functionBlock(
            named: "public func confirmPendingWriteAction()",
            endingBefore: "private func auditDetails",
            in: viewModelSource
        )

        XCTAssertTrue(viewModelSource.contains("@Published public private(set) var pendingProductionDestructiveConfirmation"))
        XCTAssertTrue(viewModelSource.contains("Destructive production actions require a second confirmation"))
        XCTAssertTrue(viewModelSource.contains("private struct PendingWriteScopeSnapshot"))
        XCTAssertTrue(viewModelSource.contains("let kubeConfigSources: [KubeConfigSource]"))
        XCTAssertTrue(viewModelSource.contains("let namespace: String"))
        XCTAssertTrue(viewModelSource.contains("let isProduction: Bool"))
        XCTAssertTrue(confirmBlock.contains("scope.isProduction"))
        XCTAssertTrue(confirmBlock.contains("action.isDestructive"))
        XCTAssertTrue(confirmBlock.contains("requiresProductionSecondConfirmation"))
        XCTAssertTrue(confirmBlock.contains("pendingProductionDestructiveConfirmation != action"))
        XCTAssertTrue(confirmBlock.contains("pendingProductionDestructiveConfirmationScopeID != scope.id"))
        XCTAssertTrue(confirmBlock.contains("pendingProductionDestructiveConfirmation = action"))
        XCTAssertTrue(confirmBlock.contains("from: scope.kubeConfigSources"))
        XCTAssertTrue(confirmBlock.contains("context: scope.context"))
        XCTAssertTrue(confirmBlock.contains("namespace: scope.namespace"))
    }

    func testPendingWriteDialogDismissDoesNotCancelProductionReviewStep() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let bindingBlock = try functionBlock(
            named: "private var pendingWriteActionPresentedBinding",
            endingBefore: "private func confirmPendingWriteActionFromDialog",
            in: rootViewSource
        )
        let confirmBlock = try functionBlock(
            named: "private func confirmPendingWriteActionFromDialog()",
            endingBefore: "private func cancelPendingWriteActionFromDialog",
            in: rootViewSource
        )

        XCTAssertTrue(rootViewSource.contains("@State private var isConfirmingPendingWriteActionFromDialog = false"))
        XCTAssertTrue(bindingBlock.contains("get: { viewModel.pendingWriteAction != nil }"))
        XCTAssertTrue(bindingBlock.contains("guard !isConfirmingPendingWriteActionFromDialog else { return }"))
        XCTAssertTrue(confirmBlock.contains("isConfirmingPendingWriteActionFromDialog = true"))
        XCTAssertTrue(confirmBlock.contains("viewModel.confirmPendingWriteAction()"))
        XCTAssertTrue(confirmBlock.contains("isConfirmingPendingWriteActionFromDialog = false"))
        XCTAssertTrue(rootViewSource.contains("private func cancelPendingWriteActionFromDialog()"))
    }

    func testContextMenuExposesManualProductionMarking() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let stateSource = try String(contentsOfFile: runeAppStatePath, encoding: .utf8)
        let preferencesSource = try String(contentsOfFile: contextPreferencesStorePath, encoding: .utf8)

        XCTAssertTrue(preferencesSource.contains("loadManualProductionContextIDs"))
        XCTAssertTrue(preferencesSource.contains("rune.manual.production.contexts"))
        XCTAssertTrue(stateSource.contains("manualProductionContextIDs"))
        XCTAssertTrue(viewModelSource.contains("public func toggleProductionMark(for context: KubeContext)"))
        XCTAssertTrue(viewModelSource.contains("public func isManuallyMarkedProduction(_ context: KubeContext)"))
        XCTAssertTrue(rootViewSource.contains("Mark as Production"))
        XCTAssertTrue(rootViewSource.contains("Unmark Production"))
        XCTAssertTrue(rootViewSource.contains("viewModel.isProductionContext(context)"))
        XCTAssertTrue(rootViewSource.contains("Production context active"))
        XCTAssertTrue(rootViewSource.contains("Non-production context"))
    }

    func testCustomResourceBrowserUsesPaginationAndPinnedColumns() throws {
        let rootViewSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let viewModelSource = try String(contentsOfFile: runeAppViewModelPath, encoding: .utf8)
        let kubeClientSource = try String(contentsOfFile: kubernetesClientPath, encoding: .utf8)

        XCTAssertTrue(viewModelSource.contains("private static let operatorResourcePageSize = 40"))
        XCTAssertTrue(viewModelSource.contains("OperatorResourceFocus"))
        XCTAssertTrue(viewModelSource.contains("public func setOperatorResourceFocus(_ focus: OperatorResourceFocus)"))
        XCTAssertTrue(viewModelSource.contains("public static func isGitOpsOperatorResource"))
        XCTAssertTrue(viewModelSource.contains("public var pagedOperatorResources"))
        XCTAssertTrue(viewModelSource.contains("OperatorResourceListSortColumn"))
        XCTAssertTrue(viewModelSource.contains("toggleOperatorResourceSort"))
        XCTAssertTrue(rootViewSource.contains("Operator resource focus"))
        XCTAssertTrue(rootViewSource.contains("ForEach(OperatorResourceFocus.allCases)"))
        XCTAssertTrue(rootViewSource.contains("viewModel.setOperatorResourceFocus($0)"))
        XCTAssertTrue(rootViewSource.contains("scopeDescription: \"the selected operator focus\""))
        XCTAssertTrue(rootViewSource.contains("AppKitOperatorResourceListView("))
        XCTAssertTrue(rootViewSource.contains("resources: viewModel.pagedOperatorResources"))
        XCTAssertTrue(rootViewSource.contains("sortColumn: viewModel.operatorResourceSortColumn"))
        XCTAssertTrue(rootViewSource.contains("onToggleSort: viewModel.toggleOperatorResourceSort"))
        XCTAssertTrue(rootViewSource.contains("viewModel.toggleOperatorPrinterColumnsForCurrentFamily()"))
        XCTAssertTrue(rootViewSource.contains("showsPrinterColumns: viewModel.showsOperatorPrinterColumnsForCurrentFamily"))
        XCTAssertTrue(viewModelSource.contains("resource.printerColumns"))
        XCTAssertTrue(viewModelSource.contains("hiddenOperatorPrinterColumnFamilies"))
        XCTAssertTrue(viewModelSource.contains("contextPreferences.saveHiddenOperatorPrinterColumnFamilies"))
        XCTAssertTrue(kubeClientSource.contains(".prefix(48)"))
        XCTAssertTrue(kubeClientSource.contains(".prefix(16)"))
        XCTAssertTrue(kubeClientSource.contains("if output.count >= 500 { return output }"))
        XCTAssertTrue(kubeClientSource.contains("/apis/apiextensions.k8s.io/v1/customresourcedefinitions"))
        XCTAssertTrue(kubeClientSource.contains("parseCRDPrinterColumnDefinitions"))
        XCTAssertTrue(kubeClientSource.contains("value(atJSONPath: definition.jsonPath"))
        let appKitSource = try String(contentsOfFile: appKitPodTableViewPath, encoding: .utf8)
        XCTAssertTrue(appKitSource.contains("case printerColumns"))
        XCTAssertTrue(appKitSource.contains("return \"Columns\""))
        XCTAssertTrue(appKitSource.contains("operatorPrinterColumnsColumnWidth"))
        XCTAssertTrue(appKitSource.contains("tableColumn.isHidden = !parent.showsPrinterColumns"))
    }

    private var runeRootViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
    }

    private var kubernetesConnectionOnboardingViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent(
            "Sources/RuneUI/Views/KubernetesConnectionOnboardingView.swift"
        ).path
    }

    private var runeAppShortcutsPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneApp/RuneAppShortcuts.swift").path
    }

    private var rbacCanISimulatorPanelPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/RBACCanISimulatorPanel.swift").path
    }

    private var resourceRelationshipViewsPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceRelationshipViews.swift").path
    }

    private var deploymentRolloutHistoryViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/DeploymentRolloutHistoryView.swift").path
    }

    private var kubeConfigImportReviewPanelPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/KubeConfigImportReviewPanel.swift").path
    }

    private var kubeConfigImportReviewSheetPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/KubeConfigImportReviewSheet.swift").path
    }

    private var addClusterProviderPresentationPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent(
            "Sources/RuneUI/Presentation/AddClusterProviderPresentation.swift"
        ).path
    }

    private var addClusterNativeContextSectionPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent(
            "Sources/RuneUI/Views/AddClusterNativeContextSection.swift"
        ).path
    }

    private var addClusterProviderCredentialFieldPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent(
            "Sources/RuneUI/Views/AddClusterProviderCredentialField.swift"
        ).path
    }

    private var addClusterPopoverViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/AddClusterPopoverView.swift").path
    }

    private var contextSidebarRowPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ContextSidebarRow.swift").path
    }

    private var appKitPodTableViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/AppKitPodTableView.swift").path
    }

    private var overviewClusterSignalsPanelViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/OverviewClusterSignalsPanelView.swift").path
    }

    private var overviewRecentEventsPanelViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/OverviewRecentEventsPanelView.swift").path
    }

    private func functionBlock(named startMarker: String, endingBefore endMarker: String, in source: String) throws -> String {
        guard let start = source.range(of: startMarker),
              let end = source.range(of: endMarker, range: start.upperBound..<source.endIndex) else {
            throw XCTSkip("Could not locate source block between \(startMarker) and \(endMarker)")
        }
        return String(source[start.lowerBound..<end.lowerBound])
    }

    private func settle(window: NSWindow) async throws {
        for _ in 0..<8 {
            window.contentView?.layoutSubtreeIfNeeded()
            try await Task.sleep(nanoseconds: 30_000_000)
        }
    }

    private func keyDownEvent(
        window: NSWindow,
        modifierFlags: NSEvent.ModifierFlags = [],
        characters: String,
        charactersIgnoringModifiers: String,
        keyCode: UInt16
    ) -> NSEvent {
        NSEvent.keyEvent(
            with: .keyDown,
            location: .zero,
            modifierFlags: modifierFlags,
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            characters: characters,
            charactersIgnoringModifiers: charactersIgnoringModifiers,
            isARepeat: false,
            keyCode: keyCode
        )!
    }

    private func findConstrainedOverflowingScrollView(in view: NSView) -> NSScrollView? {
        allScrollViews(in: view)
            .filter { scrollView in
                let documentHeight = scrollView.documentView?.frame.height ?? 0
                let viewportHeight = scrollView.contentView.bounds.height
                return documentHeight > viewportHeight + 400
                    && viewportHeight >= 80
                    && viewportHeight < 360
            }
            .min { lhs, rhs in
                lhs.frame.minX < rhs.frame.minX
            }
    }

    private func allScrollViews(in view: NSView) -> [NSScrollView] {
        var scrollViews: [NSScrollView] = []
        if let scrollView = view as? NSScrollView {
            scrollViews.append(scrollView)
        }

        for subview in view.subviews {
            scrollViews.append(contentsOf: allScrollViews(in: subview))
        }

        return scrollViews
    }

    private func allTextViews(in view: NSView) -> [NSTextView] {
        var textViews: [NSTextView] = []
        if let textView = view as? NSTextView {
            textViews.append(textView)
        }

        for subview in view.subviews {
            textViews.append(contentsOf: allTextViews(in: subview))
        }

        return textViews
    }

    private func allPopUpButtons(in view: NSView) -> [NSPopUpButton] {
        var popUpButtons: [NSPopUpButton] = []
        if let popUpButton = view as? NSPopUpButton {
            popUpButtons.append(popUpButton)
        }

        for subview in view.subviews {
            popUpButtons.append(contentsOf: allPopUpButtons(in: subview))
        }

        return popUpButtons
    }

    private func findTerminalTranscriptScrollView(in view: NSView) -> NSScrollView? {
        allScrollViews(in: view).first { scrollView in
            guard let textView = scrollView.documentView as? NSTextView else { return false }
            return textView.string.contains("wide terminal line")
        }
    }

    private var runeAppViewModelPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/ViewModels/RuneAppViewModel.swift").path
    }

    private var kubeConfigPickerPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Services/KubeConfigPicker.swift").path
    }

    private var resourceTerminalInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceTerminalInspectorView.swift").path
    }

    private var terminalShellPanelViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalShellPanelView.swift").path
    }

    private var terminalPortForwardPanelViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalPortForwardPanelView.swift").path
    }

    private var terminalSessionTabBarPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalSessionTabBar.swift").path
    }

    private var terminalLogTabBarPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalLogTabBar.swift").path
    }

    private var terminalTabStripPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalTabStrip.swift").path
    }

    private var terminalSessionControlRowPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalSessionControlRow.swift").path
    }

    private var terminalPodSelectorRowPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalPodSelectorRow.swift").path
    }

    private var favoritePodPickerPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/FavoritePodPicker.swift").path
    }

    private var terminalTranscriptSurfacePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalTranscriptSurface.swift").path
    }

    private var terminalKubectlCommandBuilderPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalKubectlCommandBuilder.swift").path
    }

    private var runePreferencesViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/RunePreferencesView.swift").path
    }

    private var kubernetesClientPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneKube/KubernetesClient.swift").path
    }

    private var kubernetesRESTClientPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneKube/KubernetesAPI/KubernetesRESTClient.swift").path
    }

    private var runeGlassShellPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Layout/RuneGlassShell.swift").path
    }

    private var runeDesignComponentsPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Layout/RuneDesignComponents.swift").path
    }

    private var runeThemePalettePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Layout/RuneThemePalette.swift").path
    }

    private var runeThemePresentationPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Layout/RuneThemePresentation.swift").path
    }

    private var appKitManifestTextViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/AppKitManifestTextView.swift").path
    }

    private var resourceLogsInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceLogsInspectorView.swift").path
    }

    private var resourceDescribeInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceDescribeInspectorView.swift").path
    }

    private var resourceYAMLInspectorViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/ResourceYAMLInspectorView.swift").path
    }

    private var commandPaletteViewPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Views/CommandPaletteView.swift").path
    }

    private var runeAppStatePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneCore/State/RuneAppState.swift").path
    }

    private var contextPreferencesStorePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneUI/Services/ContextPreferencesStore.swift").path
    }

    private var runeSettingsKeysPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("Sources/RuneCore/RuneSettingsKeys.swift").path
    }

    private var localK8sIntegrationReportScriptPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("scripts/run-local-k8s-integration-report.sh").path
    }

    private var dockerComposeReadmePath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("docker-compose/README.md").path
    }

    private var dockerComposeOrbitBootstrapManifestPath: String {
        let testFile = URL(fileURLWithPath: #filePath)
        let repoRoot = testFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        return repoRoot.appendingPathComponent("docker-compose/manifests/orbit/bootstrap.yaml").path
    }
}
